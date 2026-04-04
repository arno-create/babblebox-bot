from __future__ import annotations

import contextlib
import secrets
from typing import Any
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.confessions_service import ConfessionSubmissionResult, ConfessionsService


DOMAIN_BUCKET_CHOICES = [
    app_commands.Choice(name="Allowlist", value="allow"),
    app_commands.Choice(name="Blocklist", value="block"),
]
DOMAIN_MODE_CHOICES = [
    app_commands.Choice(name="Add", value="add"),
    app_commands.Choice(name="Remove", value="remove"),
]
STAFF_ACTION_CHOICES = [
    app_commands.Choice(name="Approve", value="approve"),
    app_commands.Choice(name="Deny", value="deny"),
    app_commands.Choice(name="Delete", value="delete"),
    app_commands.Choice(name="Restrict Images", value="restrict_images"),
    app_commands.Choice(name="Pause 24h", value="pause_24h"),
    app_commands.Choice(name="Pause 7d", value="pause_7d"),
    app_commands.Choice(name="Pause 30d", value="pause_30d"),
    app_commands.Choice(name="Permanent Ban", value="perm_ban"),
    app_commands.Choice(name="Clear Restriction", value="clear"),
    app_commands.Choice(name="False Positive", value="false_positive"),
]


def _moderation_action_payload(action: str) -> tuple[str, int | None, bool]:
    if action == "pause_24h":
        return "suspend", 24 * 3600, False
    if action == "pause_7d":
        return "temp_ban", 7 * 24 * 3600, False
    if action == "pause_30d":
        return "temp_ban", 30 * 24 * 3600, False
    if action == "override":
        return "approve", None, True
    return action, None, False


RISKY_POLICY_WARNINGS: dict[str, str] = {
    "allow_images": "Images increase moderation burden and can reveal someone through faces, screenshots, or files. Enabled images always stay bounded and reviewed.",
    "allow_replies": "Anonymous replies can increase abuse, drama, and moderation complexity. Babblebox keeps them text-only, depth-1, and always reviewed.",
    "allow_self_edit": "Editing can create bait-and-switch moderation problems. Babblebox limits it to pending submissions only.",
}


class ConfessionComposerModal(discord.ui.Modal, title="Anonymous Confession"):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        config = self.cog.service.get_config(guild_id)
        self.body_input = discord.ui.TextInput(
            label="What do you want to share?",
            style=discord.TextStyle.paragraph,
            placeholder="Keep it clear. Babblebox blocks mentions, unsafe links, and private details.",
            required=False,
            max_length=1800,
        )
        self.link_input = discord.ui.TextInput(
            label="Trusted link (optional)",
            style=discord.TextStyle.short,
            placeholder="One trusted link only. Avoid links that reveal you.",
            required=False,
            max_length=500,
        )
        self.add_item(self.body_input)
        self.add_item(self.link_input)
        self.upload_input: discord.ui.FileUpload | None = None
        if config["allow_images"]:
            self.upload_input = discord.ui.FileUpload(
                custom_id="bb-confession-modal:files",
                required=False,
                min_values=0,
                max_values=int(config["max_images"]),
            )
            self.add_item(self.upload_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        try:
            result = await self.cog.service.submit_confession(
                interaction.guild,
                author_id=interaction.user.id,
                content=self.body_input.value,
                link=self.link_input.value,
                attachments=list(self.upload_input.values) if self.upload_input is not None else [],
            )
            embed = self.cog.service.build_member_result_embed(result)
            view = self.cog.build_member_result_view(result=result, guild_id=interaction.guild.id)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        except Exception:
            embed = ge.make_status_embed(
                "Confessions Unavailable",
                "Babblebox could not process that confession safely right now. Please try again in a moment.",
                tone="warning",
                footer="Babblebox Confessions",
            )
            if interaction.response.is_done():
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, ephemeral=True)


class ReplyComposerModal(discord.ui.Modal, title="Anonymous Reply"):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.target_input = discord.ui.TextInput(
            label="Confession ID",
            style=discord.TextStyle.short,
            placeholder="Reply to a published confession like CF-XXXXXX",
            required=True,
            max_length=32,
            default=default_target or None,
        )
        self.body_input = discord.ui.TextInput(
            label="Reply",
            style=discord.TextStyle.paragraph,
            placeholder="Replies are text-only and always reviewed before posting.",
            required=True,
            max_length=1800,
        )
        self.add_item(self.target_input)
        self.add_item(self.body_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        result = await self.cog.service.submit_confession(
            interaction.guild,
            author_id=interaction.user.id,
            content=self.body_input.value,
            submission_kind="reply",
            parent_confession_id=self.target_input.value,
        )
        await interaction.response.send_message(
            embed=self.cog.service.build_member_result_embed(result),
            view=self.cog.build_member_result_view(result=result, guild_id=interaction.guild.id),
            ephemeral=True,
        )


class ManageConfessionModal(discord.ui.Modal, title="Manage My Confession"):
    def __init__(self, cog: "ConfessionsCog", *, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.target_input = discord.ui.TextInput(
            label="Confession or Case ID",
            style=discord.TextStyle.short,
            placeholder="Use a confession ID like CF-XXXXXX or a case ID like CS-XXXXXX",
            required=True,
            max_length=32,
            default=default_target or None,
        )
        self.add_item(self.target_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private owner tools only work inside a server.", ephemeral=True)
            return
        context, error = await self.cog.service.get_owned_submission_context(
            interaction.guild.id,
            author_id=interaction.user.id,
            target_id=self.target_input.value,
        )
        if context is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Cannot Open Owner Tools", error or "That confession could not be verified.", tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return
        submission = context["submission"]
        await interaction.response.send_message(
            embed=self.cog.service.build_member_manage_embed(context),
            view=MemberManageActionView(
                self.cog,
                guild_id=interaction.guild.id,
                target_id=submission["confession_id"],
                can_delete=context["can_delete"],
                can_edit=context["can_edit"],
            ),
            ephemeral=True,
        )


class EditConfessionModal(discord.ui.Modal):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, target_id: str, submission: dict[str, Any]):
        title = "Edit Anonymous Reply" if submission.get("submission_kind") == "reply" else "Edit Anonymous Confession"
        super().__init__(title=title, timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.target_id = target_id
        self.body_input = discord.ui.TextInput(
            label="Updated Text",
            style=discord.TextStyle.paragraph,
            placeholder="Keep it clear. Babblebox re-checks safety before saving.",
            required=False,
            max_length=1800,
            default=submission.get("content_body") or None,
        )
        self.add_item(self.body_input)
        self.link_input: discord.ui.TextInput | None = None
        if submission.get("submission_kind") != "reply":
            self.link_input = discord.ui.TextInput(
                label="Trusted link (optional)",
                style=discord.TextStyle.short,
                placeholder="One trusted link only.",
                required=False,
                max_length=500,
                default=submission.get("shared_link_url") or None,
            )
            self.add_item(self.link_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private owner tools only work inside a server.", ephemeral=True)
            return
        result = await self.cog.service.self_edit_confession(
            interaction.guild,
            author_id=interaction.user.id,
            target_id=self.target_id,
            content=self.body_input.value,
            link=self.link_input.value if self.link_input is not None else None,
        )
        await interaction.response.send_message(
            embed=self.cog.service.build_member_result_embed(result),
            view=self.cog.build_member_result_view(result=result, guild_id=interaction.guild.id),
            ephemeral=True,
        )


class AppealModal(discord.ui.Modal, title="Anonymous Appeal"):
    def __init__(self, cog: "ConfessionsCog", *, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.target_input = discord.ui.TextInput(
            label="Confession or Case ID (optional)",
            style=discord.TextStyle.short,
            placeholder="Use your own confession or case ID if you have one",
            required=False,
            max_length=32,
            default=default_target or None,
        )
        self.details_input = discord.ui.TextInput(
            label="What should staff know?",
            style=discord.TextStyle.paragraph,
            placeholder="Explain the false positive, restriction, or moderation issue.",
            required=True,
            max_length=1800,
        )
        self.add_item(self.target_input)
        self.add_item(self.details_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private support only works inside a server.", ephemeral=True)
            return
        ok, message = await self.cog.service.submit_support_request(
            interaction.guild,
            author_id=interaction.user.id,
            kind="appeal",
            target_id=self.target_input.value,
            details=self.details_input.value,
        )
        await interaction.response.send_message(
            embed=ge.make_status_embed("Anonymous Appeal", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )


class ReportModal(discord.ui.Modal, title="Anonymous Report"):
    def __init__(self, cog: "ConfessionsCog", *, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.target_input = discord.ui.TextInput(
            label="Confession or Case ID",
            style=discord.TextStyle.short,
            placeholder="Report a confession ID like CF-XXXXXX or case ID like CS-XXXXXX",
            required=True,
            max_length=32,
            default=default_target or None,
        )
        self.details_input = discord.ui.TextInput(
            label="What is the problem?",
            style=discord.TextStyle.paragraph,
            placeholder="Explain the issue clearly without mentioning users directly.",
            required=True,
            max_length=1800,
        )
        self.add_item(self.target_input)
        self.add_item(self.details_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private support only works inside a server.", ephemeral=True)
            return
        ok, message = await self.cog.service.submit_support_request(
            interaction.guild,
            author_id=interaction.user.id,
            kind="report",
            target_id=self.target_input.value,
            details=self.details_input.value,
        )
        await interaction.response.send_message(
            embed=ge.make_status_embed("Anonymous Report", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )


class MemberSupportView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.default_target = default_target

    @discord.ui.button(label="Appeal Restriction", style=discord.ButtonStyle.secondary, row=0)
    async def appeal_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AppealModal(self.cog, default_target=self.default_target))

    @discord.ui.button(label="Report Problem", style=discord.ButtonStyle.secondary, row=0)
    async def report_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ReportModal(self.cog, default_target=self.default_target))


class MemberManageActionView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, target_id: str, can_delete: bool, can_edit: bool):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.target_id = target_id
        self.delete_button.disabled = not can_delete
        self.edit_button.disabled = not can_edit

    @discord.ui.button(label="Delete Privately", style=discord.ButtonStyle.danger, row=0)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private owner tools only work inside a server.", ephemeral=True)
            return
        ok, message = await self.cog.service.self_delete_confession(interaction.guild, author_id=interaction.user.id, target_id=self.target_id)
        await interaction.response.send_message(
            embed=ge.make_status_embed("Private Delete", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )

    @discord.ui.button(label="Edit Pending Submission", style=discord.ButtonStyle.primary, row=0)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Private owner tools only work inside a server.", ephemeral=True)
            return
        context, error = await self.cog.service.get_owned_submission_context(
            interaction.guild.id,
            author_id=interaction.user.id,
            target_id=self.target_id,
        )
        if context is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Cannot Edit", error or "That confession could not be verified.", tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(
            EditConfessionModal(self.cog, guild_id=interaction.guild.id, target_id=self.target_id, submission=context["submission"])
        )

    @discord.ui.button(label="Appeal / Report", style=discord.ButtonStyle.secondary, row=1)
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Private Support",
                "Choose whether you want to appeal a restriction or report a problem without exposing your account to staff.",
                tone="info",
                footer="Babblebox Confessions",
            ),
            view=MemberSupportView(self.cog, default_target=self.target_id),
            ephemeral=True,
        )


class MemberResultActionView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, result: ConfessionSubmissionResult):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.result = result
        if result.jump_url:
            self.add_item(discord.ui.Button(label="Open Post", style=discord.ButtonStyle.link, url=result.jump_url))

    @discord.ui.button(label="Manage My Confession", style=discord.ButtonStyle.secondary, row=0)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_target = self.result.confession_id or self.result.case_id
        await interaction.response.send_modal(ManageConfessionModal(self.cog, default_target=default_target))

    @discord.ui.button(label="Reply", style=discord.ButtonStyle.primary, row=0)
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        config = self.cog.service.get_config(interaction.guild.id)
        if not config["allow_anonymous_replies"]:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Replies Are Off",
                    "Anonymous replies are off by default in this server unless admins explicitly enable them.",
                    tone="info",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return
        default_target = self.result.confession_id if self.result.submission_kind == "confession" else self.result.parent_confession_id
        await interaction.response.send_modal(ReplyComposerModal(self.cog, guild_id=self.guild_id, default_target=default_target))

    @discord.ui.button(label="Appeal / Report", style=discord.ButtonStyle.secondary, row=1)
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_target = self.result.case_id or self.result.confession_id
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Private Support",
                "Choose whether you want to appeal a restriction or report a problem without exposing your account to staff.",
                tone="info",
                footer="Babblebox Confessions",
            ),
            view=MemberSupportView(self.cog, default_target=default_target),
            ephemeral=True,
        )


class RiskyConfigConfirmView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, token: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.token = token

    def _claim(self) -> dict[str, Any] | None:
        return self.cog._pending_policy_updates.get(self.token)

    @discord.ui.button(label="Enable With Warning", style=discord.ButtonStyle.danger, row=0)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        payload = self._claim()
        if payload is None:
            await interaction.response.send_message("That pending confirmation expired. Run the command again.", ephemeral=True)
            return
        if interaction.user.id != payload["author_id"]:
            await interaction.response.send_message("Run the command yourself to confirm that policy change.", ephemeral=True)
            return
        ok, message = await self.cog.service.configure_guild(payload["guild_id"], **payload["updates"])
        if ok and interaction.guild is not None:
            await self.cog._sync_runtime_surfaces(interaction.guild)
        self.cog._pending_policy_updates.pop(self.token, None)
        await interaction.response.edit_message(
            embed=ge.make_status_embed("Confessions Policy", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            view=None,
        )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=0)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.cog._pending_policy_updates.pop(self.token, None)
        await interaction.response.edit_message(
            embed=ge.make_status_embed(
                "Confessions Policy",
                "That risky policy change was cancelled. Babblebox left the current safety settings in place.",
                tone="info",
                footer="Babblebox Confessions",
            ),
            view=None,
        )


class ConfessionMemberPanelView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        ready = self.cog.service.operability_message(guild_id) == "Confessions are ready."
        self.send_button.disabled = not ready

    @discord.ui.button(
        label="Send Confession",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-panel:compose",
        row=0,
    )
    async def send_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        ready_message = self.cog.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            await interaction.response.send_message(
                embed=ge.make_status_embed("Confessions Unavailable", ready_message, tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(ConfessionComposerModal(self.cog, guild_id=interaction.guild.id))

    @discord.ui.button(
        label="Manage My Confession",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:manage",
        row=0,
    )
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ManageConfessionModal(self.cog))

    @discord.ui.button(
        label="Reply to Confession",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:reply",
        row=1,
    )
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        config = self.cog.service.get_config(interaction.guild.id)
        if not config["allow_anonymous_replies"]:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Replies Are Off",
                    "Anonymous replies are off by default in this server unless admins explicitly enable them.",
                    tone="info",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(ReplyComposerModal(self.cog, guild_id=interaction.guild.id))

    @discord.ui.button(
        label="Appeal / Report",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:support",
        row=1,
    )
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Private Support",
                "Choose whether you want to appeal a restriction or report a problem without exposing your account to staff.",
                tone="info",
                footer="Babblebox Confessions",
            ),
            view=MemberSupportView(self.cog),
            ephemeral=True,
        )

    @discord.ui.button(
        label="How It Works",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:help",
        row=1,
    )
    async def help_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        await interaction.response.send_message(embed=self.cog.service.build_member_panel_help_embed(interaction.guild), ephemeral=True)


class ConfessionReviewView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, case_id: str, version: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.case_id = case_id
        self.version = version
        self.add_item(self._make_button("approve", "Approve", discord.ButtonStyle.success, row=0))
        self.add_item(self._make_button("deny", "Deny", discord.ButtonStyle.secondary, row=0))
        self.add_item(self._make_button("pause_24h", "Pause 24h", discord.ButtonStyle.secondary, row=0))
        self.add_item(self._make_button("pause_7d", "Pause 7d", discord.ButtonStyle.secondary, row=1))
        self.add_item(self._make_button("pause_30d", "Pause 30d", discord.ButtonStyle.secondary, row=1))
        self.add_item(self._make_button("perm_ban", "Perm Ban", discord.ButtonStyle.danger, row=1))
        self.add_item(self._make_button("override", "Override", discord.ButtonStyle.primary, row=2))
        self.add_item(self._make_button("details", "Details", discord.ButtonStyle.secondary, row=2))
        self.add_item(self._make_button("refresh", "Refresh", discord.ButtonStyle.secondary, row=2))

    def _make_button(self, action: str, label: str, style: discord.ButtonStyle, *, row: int) -> discord.ui.Button:
        button = discord.ui.Button(
            label=label,
            style=style,
            row=row,
            custom_id=f"bb-confession-review:{action}:{self.case_id}:{self.version}",
        )

        async def _callback(interaction: discord.Interaction):
            try:
                await self._handle_action(interaction, action)
            except Exception:
                embed = ge.make_status_embed(
                    "Review Action Failed",
                    "Babblebox could not finish that review action safely. Refresh the queue and try again.",
                    tone="warning",
                    footer="Babblebox Confessions",
                )
                if interaction.response.is_done():
                    await interaction.followup.send(embed=embed, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, ephemeral=True)

        button.callback = _callback
        return button

    async def _refresh_queue_message(self, interaction: discord.Interaction, *, note: str | None = None):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This review action only works inside a server.", ephemeral=True)
            return
        pending = await self.cog.service.list_review_targets(guild.id, limit=25)
        if not pending:
            await interaction.response.edit_message(embed=self.cog.service.build_review_queue_embed(guild, [], note=note), view=None)
            return
        current = pending[0]
        view = self.cog.build_review_view(case_id=current["case_id"], version=current["review_version"])
        await interaction.response.edit_message(embed=self.cog.service.build_review_queue_embed(guild, pending, note=note), view=view)

    async def _handle_action(self, interaction: discord.Interaction, action: str):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("This review action only works inside a server.", ephemeral=True)
            return
        if not self.cog._is_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to use confession review actions.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return
        if action == "details":
            await interaction.response.send_message(
                embed=await self.cog.service.build_target_status_embed(interaction.guild, self.case_id),
                ephemeral=True,
            )
            return
        if action == "refresh":
            await self._refresh_queue_message(interaction, note="Confession review queue refreshed.")
            return
        service_action, duration_seconds, clear_strikes = _moderation_action_payload(action)
        ok, message = await self.cog.service.handle_case_action(
            interaction.guild,
            case_id=self.case_id,
            action=service_action,
            actor=interaction.user,
            version=self.version,
            duration_seconds=duration_seconds,
            clear_strikes=clear_strikes,
        )
        if not ok:
            if "stale" in message.lower() or "closed" in message.lower():
                await self._refresh_queue_message(interaction, note=message)
                return
            await interaction.response.send_message(message, ephemeral=True)
            return
        await self._refresh_queue_message(interaction, note=message)


class ConfessionsAdminPanelView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, author_id: int, section: str = "overview"):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.section = section
        self.message: discord.Message | None = None
        self._refresh_buttons()

    async def current_embed(self) -> discord.Embed:
        guild = self.cog.bot.get_guild(self.guild_id)
        if guild is None:
            return ge.make_status_embed("Confessions Unavailable", "That server is no longer available.", tone="warning", footer="Babblebox Confessions")
        return await self.cog.service.build_dashboard_embed(guild, section=self.section)

    def _refresh_buttons(self):
        statuses = {
            "overview": self.overview_button,
            "policy": self.policy_button,
            "review": self.review_button,
            "launch": self.launch_button,
        }
        for name, button in statuses.items():
            button.style = discord.ButtonStyle.primary if self.section == name else discord.ButtonStyle.secondary
        current = self.cog.service.get_config(self.guild_id)
        self.toggle_button.label = "Disable" if current["enabled"] else "Enable"
        self.toggle_button.style = discord.ButtonStyle.danger if current["enabled"] else discord.ButtonStyle.success

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/confessions` to open your own private confessions panel.",
                    tone="info",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return False
        if not self.cog._is_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure anonymous confessions.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return False
        return True

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException):
                await self.message.edit(view=self)

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None):
        self._refresh_buttons()
        await interaction.response.edit_message(embed=await self.current_embed(), view=self)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    async def _switch_section(self, interaction: discord.Interaction, section: str):
        self.section = section
        await self._rerender(interaction)

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary, row=0)
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "overview")

    @discord.ui.button(label="Policy", style=discord.ButtonStyle.secondary, row=0)
    async def policy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "policy")

    @discord.ui.button(label="Review", style=discord.ButtonStyle.secondary, row=0)
    async def review_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "review")

    @discord.ui.button(label="Launch", style=discord.ButtonStyle.secondary, row=0)
    async def launch_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "launch")

    @discord.ui.button(label="Publish Panel", style=discord.ButtonStyle.success, row=1)
    async def publish_panel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        ok, message = await self.cog.service.sync_member_panel(guild)
        await self._rerender(interaction, note=message if ok else message)

    @discord.ui.button(label="Refresh Queue", style=discord.ButtonStyle.secondary, row=1)
    async def refresh_queue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        await self.cog.service._sync_review_queue(guild, note="Confession review queue refreshed.")
        await self._rerender(interaction, note="Confession review queue refreshed.")

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._rerender(interaction, note="Confessions panel refreshed.")

    @discord.ui.button(label="Enable", style=discord.ButtonStyle.success, row=1)
    async def toggle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        current = self.cog.service.get_config(self.guild_id)
        ok, message = await self.cog.service.configure_guild(self.guild_id, enabled=not current["enabled"])
        if ok and (current.get("panel_message_id") or current.get("panel_channel_id")):
            await self.cog.service.sync_member_panel(guild)
        await self.cog.service._sync_review_queue(guild, note="Confessions runtime refreshed.")
        await self._rerender(interaction, note=message)


class ConfessionsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ConfessionsService(bot)
        self._pending_policy_updates: dict[str, dict[str, Any]] = {}

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "confessions_service", self.service)
        if self.service.storage_ready:
            await self.service.resume_member_panels()
            await self.service.resume_review_queues()

    def cog_unload(self):
        if getattr(self.bot, "confessions_service", None) is self.service:
            delattr(self.bot, "confessions_service")
        self.bot.loop.create_task(self.service.close())

    def _is_admin(self, member: object) -> bool:
        perms = getattr(member, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    def build_review_view(self, *, case_id: str, version: int) -> ConfessionReviewView:
        return ConfessionReviewView(self, case_id=case_id, version=version)

    def build_member_panel_view(self, *, guild_id: int) -> ConfessionMemberPanelView:
        return ConfessionMemberPanelView(self, guild_id=guild_id)

    def build_member_result_view(self, *, result: ConfessionSubmissionResult, guild_id: int) -> discord.ui.View | None:
        if result.state not in {"published", "queued", "blocked", "restricted"}:
            return self.service.build_member_result_view(result)
        return MemberResultActionView(self, guild_id=guild_id, result=result)

    async def _send_policy_warning(
        self,
        ctx: commands.Context,
        *,
        updates: dict[str, Any],
        warning_fields: list[tuple[str, str]],
    ):
        token = secrets.token_urlsafe(12)
        self._pending_policy_updates[token] = {
            "guild_id": ctx.guild.id,
            "author_id": ctx.author.id,
            "updates": updates,
        }
        embed = discord.Embed(
            title="Confirm Risky Policy Change",
            description="These features stay off by default because they expand abuse surface or moderation complexity. Review the impact before enabling them.",
            color=ge.EMBED_THEME["warning"],
        )
        for name, value in warning_fields:
            embed.add_field(name=name, value=value, inline=False)
        embed = ge.style_embed(embed, footer="Babblebox Confessions | Admin warning")
        await send_hybrid_response(ctx, embed=embed, view=RiskyConfigConfirmView(self, token=token), ephemeral=True)

    async def _delete_stored_panel_message(self, guild: discord.Guild, config: dict[str, object]):
        channel_id = config.get("panel_channel_id")
        message_id = config.get("panel_message_id")
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return
        channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
        if channel is None:
            return
        message = await self.service._queue_message(channel, message_id=message_id)
        if message is not None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException, Exception):
                await message.delete()

    async def _send_admin_panel(self, ctx: commands.Context, *, section: str = "overview"):
        view = ConfessionsAdminPanelView(self, guild_id=ctx.guild.id, author_id=ctx.author.id, section=section)
        message = await send_hybrid_response(ctx, embed=await view.current_embed(), view=view, ephemeral=True)
        if message is not None:
            view.message = message

    async def _require_admin(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "Confessions can only be configured inside a server.", tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return False
        if not self._is_admin(ctx.author):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure anonymous confessions.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
                ephemeral=True,
            )
            return False
        if not self.service.storage_ready:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Confessions Unavailable", self.service.storage_message("Confessions"), tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return False
        return True

    async def _sync_runtime_surfaces(self, guild: discord.Guild):
        config = self.service.get_config(guild.id)
        if config.get("panel_channel_id") or config.get("panel_message_id"):
            await self.service.sync_member_panel(guild)
        await self.service._sync_review_queue(guild)

    @app_commands.command(name="confess", description="Open the private confession composer when a server has Confessions enabled")
    async def confess_command(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        ready_message = self.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            unavailable = ConfessionSubmissionResult(False, "unavailable", ready_message)
            await interaction.response.send_message(embed=self.service.build_member_result_embed(unavailable), ephemeral=True)
            return
        await interaction.response.send_modal(ConfessionComposerModal(self, guild_id=interaction.guild.id))

    @commands.hybrid_group(name="confessions", with_app_command=True, description="Admin controls for the optional Confessions feature", invoke_without_command=True)
    @app_commands.default_permissions(manage_guild=True)
    async def confessions_group(self, ctx: commands.Context):
        if not await self._require_admin(ctx):
            return
        await self._send_admin_panel(ctx, section="overview")

    @confessions_group.command(name="status", description="Open the Confessions dashboard or inspect one confession/case")
    async def confessions_status_command(self, ctx: commands.Context, target_id: Optional[str] = None):
        if not await self._require_admin(ctx):
            return
        if not target_id:
            await self._send_admin_panel(ctx, section="overview")
            return
        await send_hybrid_response(ctx, embed=await self.service.build_target_status_embed(ctx.guild, target_id), ephemeral=True)

    @app_commands.describe(
        enabled="Turn confessions on or off",
        confession_channel="Public channel for approved confessions",
        panel_channel="Channel where the public confession panel should live",
        review_channel="Private review queue channel",
        appeals_channel="Private channel for anonymous appeals and reports",
        review_mode="Queue even safe confessions for review before posting",
        clear_confession_channel="Clear the public confession channel",
        clear_panel="Clear the stored public panel location",
        clear_review_channel="Clear the private review channel",
        clear_appeals_channel="Clear the appeals/report channel",
    )
    @confessions_group.command(name="setup", description="Enable or configure the optional Confessions feature")
    async def confessions_setup_command(
        self,
        ctx: commands.Context,
        enabled: Optional[bool] = None,
        confession_channel: Optional[discord.TextChannel] = None,
        panel_channel: Optional[discord.TextChannel] = None,
        review_channel: Optional[discord.TextChannel] = None,
        appeals_channel: Optional[discord.TextChannel] = None,
        review_mode: Optional[bool] = None,
        clear_confession_channel: bool = False,
        clear_panel: bool = False,
        clear_review_channel: bool = False,
        clear_appeals_channel: bool = False,
    ):
        if not await self._require_admin(ctx):
            return
        previous_config = self.service.get_config(ctx.guild.id)
        ok, message = await self.service.configure_guild(
            ctx.guild.id,
            enabled=enabled,
            confession_channel_id=getattr(confession_channel, "id", None),
            panel_channel_id=getattr(panel_channel, "id", None),
            review_channel_id=getattr(review_channel, "id", None),
            appeals_channel_id=getattr(appeals_channel, "id", None),
            review_mode=review_mode,
            clear_confession_channel=clear_confession_channel,
            clear_panel=clear_panel,
            clear_review_channel=clear_review_channel,
            clear_appeals_channel=clear_appeals_channel,
        )
        if ok:
            if clear_panel:
                await self._delete_stored_panel_message(ctx.guild, previous_config)
            await self._sync_runtime_surfaces(ctx.guild)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Confessions Setup", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )

    @app_commands.describe(
        block_adult_language="Block adult or 18+ language",
        allow_trusted_links="Allow Babblebox's trusted link families",
        allow_images="Enable image attachments for confessions",
        allow_replies="Enable anonymous replies",
        allow_self_edit="Enable member self-edit for pending submissions",
        max_images="Maximum images per confession",
        cooldown_seconds="Minimum gap between submissions",
        burst_limit="Max submissions before auto-suspend",
        burst_window_seconds="Burst window length",
        auto_suspend_hours="Automatic suspension length",
        strike_temp_ban_threshold="Strike count for temporary ban",
        temp_ban_days="Temporary ban length",
        strike_perm_ban_threshold="Strike count for permanent ban",
    )
    @confessions_group.command(name="policy", description="Adjust Confessions safety, link, image, and flood controls")
    async def confessions_policy_command(
        self,
        ctx: commands.Context,
        block_adult_language: Optional[bool] = None,
        allow_trusted_links: Optional[bool] = None,
        allow_images: Optional[bool] = None,
        allow_replies: Optional[bool] = None,
        allow_self_edit: Optional[bool] = None,
        max_images: Optional[int] = None,
        cooldown_seconds: Optional[int] = None,
        burst_limit: Optional[int] = None,
        burst_window_seconds: Optional[int] = None,
        auto_suspend_hours: Optional[int] = None,
        strike_temp_ban_threshold: Optional[int] = None,
        temp_ban_days: Optional[int] = None,
        strike_perm_ban_threshold: Optional[int] = None,
    ):
        if not await self._require_admin(ctx):
            return
        current = self.service.get_config(ctx.guild.id)
        updates = {
            "block_adult_language": block_adult_language,
            "allow_trusted_mainstream_links": allow_trusted_links,
            "allow_images": allow_images,
            "allow_anonymous_replies": allow_replies,
            "allow_self_edit": allow_self_edit,
            "max_images": max_images,
            "cooldown_seconds": cooldown_seconds,
            "burst_limit": burst_limit,
            "burst_window_seconds": burst_window_seconds,
            "auto_suspend_hours": auto_suspend_hours,
            "strike_temp_ban_threshold": strike_temp_ban_threshold,
            "temp_ban_days": temp_ban_days,
            "strike_perm_ban_threshold": strike_perm_ban_threshold,
        }
        warning_fields: list[tuple[str, str]] = []
        if allow_images and not current["allow_images"]:
            warning_fields.append(("Images", RISKY_POLICY_WARNINGS["allow_images"]))
        if allow_replies and not current["allow_anonymous_replies"]:
            warning_fields.append(("Anonymous Replies", RISKY_POLICY_WARNINGS["allow_replies"]))
        if allow_self_edit and not current["allow_self_edit"]:
            warning_fields.append(("Self-Edit", RISKY_POLICY_WARNINGS["allow_self_edit"]))
        if warning_fields:
            await self._send_policy_warning(ctx, updates=updates, warning_fields=warning_fields)
            return
        ok, message = await self.service.configure_guild(ctx.guild.id, **updates)
        if ok:
            await self._sync_runtime_surfaces(ctx.guild)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Confessions Policy", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )

    @app_commands.choices(bucket=DOMAIN_BUCKET_CHOICES, mode=DOMAIN_MODE_CHOICES)
    @confessions_group.command(name="domains", description="Update the Confessions domain allowlist or blocklist")
    async def confessions_domains_command(self, ctx: commands.Context, bucket: str, mode: str, domain: str):
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_domain_policy(ctx.guild.id, bucket=bucket, domain=domain, enabled=mode == "add")
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Confessions Domains", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )

    @confessions_group.command(name="panel", description="Publish or refresh the public Confessions panel")
    async def confessions_panel_command(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.sync_member_panel(ctx.guild, channel_id=getattr(channel, "id", None))
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Confessions Panel", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )

    @app_commands.choices(action=STAFF_ACTION_CHOICES)
    @app_commands.describe(
        target_id="A confession ID like CF-XXXXXX or a case ID like CS-XXXXXX",
        clear_strikes="Clear stored strikes when using the clear action",
    )
    @confessions_group.command(name="moderate", description="Moderate a confession or case by ID without seeing the author")
    async def confessions_moderate_command(
        self,
        ctx: commands.Context,
        target_id: str,
        action: str,
        clear_strikes: bool = False,
    ):
        if not await self._require_admin(ctx):
            return
        service_action, duration_seconds, action_clears_strikes = _moderation_action_payload(action)
        ok, message = await self.service.handle_staff_action(
            ctx.guild,
            target_id=target_id,
            action=service_action,
            actor=ctx.author,
            duration_seconds=duration_seconds,
            clear_strikes=clear_strikes or action_clears_strikes,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Confessions Moderation", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ConfessionsCog(bot))
