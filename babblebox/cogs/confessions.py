from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
from typing import Any
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox.app_command_hardening import harden_admin_root_group
from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.confessions_service import ConfessionSubmissionResult, ConfessionsService


LOGGER = logging.getLogger(__name__)

DOMAIN_BUCKET_CHOICES = [
    app_commands.Choice(name="Allowlist", value="allow"),
    app_commands.Choice(name="Blocklist", value="block"),
]
DOMAIN_MODE_CHOICES = [
    app_commands.Choice(name="Add", value="add"),
    app_commands.Choice(name="Remove", value="remove"),
]
ROLE_STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
ROLE_RESET_CHOICES = [
    app_commands.Choice(name="Allowlist", value="allowlist"),
    app_commands.Choice(name="Blacklist", value="blacklist"),
    app_commands.Choice(name="All", value="all"),
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
CONFESSIONS_ADMIN_PANEL_EXPIRED_MESSAGE = "This private confessions panel expired. Run `/confessions` again to open a fresh one."


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


def _resolve_live_confessions_cog(interaction: discord.Interaction, fallback: object | None = None):
    client = getattr(interaction, "client", None)
    get_cog = getattr(client, "get_cog", None)
    if callable(get_cog):
        current = get_cog("ConfessionsCog")
        if current is not None:
            return current
    return fallback


async def _send_confessions_runtime_unavailable(interaction: discord.Interaction):
    embed = ge.make_status_embed(
        "Confessions Unavailable",
        "Babblebox could not continue that Confessions interaction right now. Try `/confess create` again in a moment.",
        tone="warning",
        footer="Babblebox Confessions",
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ConfessionComposerModal(discord.ui.Modal, title="Anonymous Confession"):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        config = self.cog.service.get_config(guild_id)
        self.image_upload_requested = bool(config["allow_images"])
        self.image_upload_available = False
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
        if self.image_upload_requested:
            if not self.cog.modal_file_upload_available():
                self._apply_image_upload_fallback(guild_id, code="confession_modal_upload_runtime_unavailable")
            else:
                try:
                    self.upload_input = discord.ui.FileUpload(
                        custom_id="bb-confession-modal:files",
                        required=False,
                        min_values=0,
                        max_values=int(config["max_images"]),
                    )
                    self.add_item(self.upload_input)
                    self.image_upload_available = True
                except Exception as exc:
                    self._apply_image_upload_fallback(
                        guild_id,
                        code="confession_modal_upload_construct_failed",
                        exc=exc,
                    )

    def _apply_image_upload_fallback(self, guild_id: int, *, code: str, exc: Exception | None = None):
        self.body_input.placeholder = "Image upload is temporarily unavailable right now. You can still send text and one trusted link."
        self.link_input.placeholder = "Text and one trusted link only right now."
        self.cog.log_modal_diagnostic(
            code=code,
            stage="construct_upload",
            modal_kind="confession",
            guild_id=guild_id,
            allow_images=self.image_upload_requested,
            upload_present=False,
            attachment_count=0,
            storage_ready=self.cog.service.storage_ready,
            operability_ready=self.cog.service.operability_message(guild_id) == "Confessions are ready.",
            exc=exc,
        )

    def _collect_attachments(self) -> list[Any]:
        if self.upload_input is None:
            return []
        return list(self.upload_input.values or [])

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        ready_message = self.cog.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            await self.cog._send_confession_result_response(
                interaction,
                guild_id=interaction.guild.id,
                modal_kind="confession",
                result=ConfessionSubmissionResult(False, "unavailable", ready_message),
                allow_images=self.image_upload_requested,
                upload_present=self.upload_input is not None,
                attachment_count=0,
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="confession",
            guild_id=interaction.guild.id,
            allow_images=self.image_upload_requested,
            upload_present=self.upload_input is not None,
            attachment_count=0,
            failure_message="Babblebox could not continue that private confession flow right now. Please try again in a moment.",
        ):
            return
        try:
            attachments = self._collect_attachments()
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="confession_modal_attachment_extract_failed",
                stage="extract_attachments",
                modal_kind="confession",
                guild_id=interaction.guild.id,
                allow_images=self.image_upload_requested,
                upload_present=self.upload_input is not None,
                attachment_count=0,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=True,
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Image Upload Unavailable",
                    "Babblebox could not safely read that uploaded image. Try again in a moment or send the confession without images.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        try:
            result = await self.cog.service.submit_confession(
                interaction.guild,
                author_id=interaction.user.id,
                member=interaction.user,
                content=self.body_input.value,
                link=self.link_input.value,
                attachments=attachments,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="confession_modal_submit_failed",
                stage="submit",
                modal_kind="confession",
                guild_id=interaction.guild.id,
                allow_images=self.image_upload_requested,
                upload_present=self.upload_input is not None,
                attachment_count=len(attachments),
                storage_ready=self.cog.service.storage_ready,
                operability_ready=True,
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(
                    "Babblebox could not process that confession safely right now. Please try again in a moment."
                ),
            )
            return
        await self.cog._send_confession_result_response(
            interaction,
            guild_id=interaction.guild.id,
            modal_kind="confession",
            result=result,
            allow_images=self.image_upload_requested,
            upload_present=self.upload_input is not None,
            attachment_count=len(attachments),
        )


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
            label="Anonymous reply",
            style=discord.TextStyle.paragraph,
            placeholder="Your anonymous reply stays anonymous. Babblebox may send it through private approval before posting.",
            required=True,
            max_length=1800,
        )
        self.add_item(self.target_input)
        self.add_item(self.body_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        ready_message = self.cog.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            await self.cog._send_confession_result_response(
                interaction,
                guild_id=interaction.guild.id,
                modal_kind="reply",
                result=ConfessionSubmissionResult(False, "unavailable", ready_message, submission_kind="reply"),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="reply",
            guild_id=interaction.guild.id,
            failure_message="Babblebox could not continue that private reply flow right now. Please try again in a moment.",
        ):
            return
        try:
            result = await self.cog.service.submit_confession(
                interaction.guild,
                author_id=interaction.user.id,
                member=interaction.user,
                content=self.body_input.value,
                submission_kind="reply",
                parent_confession_id=self.target_input.value,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="reply_modal_submit_failed",
                stage="submit",
                modal_kind="reply",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=True,
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(
                    "Babblebox could not process that anonymous reply safely right now. Please try again in a moment."
                ),
            )
            return
        await self.cog._send_confession_result_response(
            interaction,
            guild_id=interaction.guild.id,
            modal_kind="reply",
            result=result,
        )


class OwnerReplyComposerModal(discord.ui.Modal, title="Reply to Member Anonymously"):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, opportunity_id: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.opportunity_id = opportunity_id
        self.body_input = discord.ui.TextInput(
            label="Anonymous owner reply",
            style=discord.TextStyle.paragraph,
            placeholder="Your reply posts publicly as an Anonymous Owner Reply. Babblebox keeps your identity hidden and only queues it if this server enables owner-reply review.",
            required=True,
            max_length=1800,
        )
        self.add_item(self.body_input)

    async def on_submit(self, interaction: discord.Interaction):
        guild = self.cog.bot.get_guild(self.guild_id)
        if guild is None or interaction.user is None:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Owner Reply Unavailable",
                    "Babblebox could not reach that server to continue the owner-reply flow.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        context, error = await self.cog.service.get_owner_reply_opportunity_context(
            guild,
            author_id=interaction.user.id,
            opportunity_id=self.opportunity_id,
        )
        if context is None:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Owner Reply Unavailable",
                    error or "That owner-reply prompt is no longer available.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="owner_reply",
            guild_id=guild.id,
            failure_message="Babblebox could not continue that private owner-reply flow right now. Please try again in a moment.",
        ):
            return
        try:
            member = guild.get_member(interaction.user.id) or interaction.user
            result = await self.cog.service.submit_owner_reply(
                guild,
                author_id=interaction.user.id,
                member=member,
                opportunity_id=self.opportunity_id,
                content=self.body_input.value,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="owner_reply_modal_submit_failed",
                stage="submit",
                modal_kind="owner_reply",
                guild_id=guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=True,
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(
                    "Babblebox could not process that anonymous owner reply safely right now. Please try again in a moment."
                ),
            )
            return
        embed = self.cog.service.build_member_result_embed(result)
        view = self.cog.service.build_member_result_view(result)
        await self.cog._send_private_interaction(interaction, embed=embed, view=view)


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
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="manage",
            guild_id=interaction.guild.id,
            failure_message="Babblebox could not open that private owner flow right now. Please try again in a moment.",
        ):
            return
        try:
            context, error = await self.cog.service.get_owned_submission_context(
                interaction.guild.id,
                author_id=interaction.user.id,
                target_id=self.target_input.value,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="manage_modal_context_failed",
                stage="load_context",
                modal_kind="manage",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Owner Tools Unavailable",
                    "Babblebox could not open that private owner flow safely right now. Please try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        if context is None:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed("Cannot Open Owner Tools", error or "That confession could not be verified.", tone="warning", footer="Babblebox Confessions"),
            )
            return
        submission = context["submission"]
        try:
            embed = self.cog.service.build_member_manage_embed(context)
            view = MemberManageActionView(
                self.cog,
                guild_id=interaction.guild.id,
                target_id=submission["confession_id"],
                can_delete=context["can_delete"],
                can_edit=context["can_edit"],
            )
            await self.cog._send_private_interaction(interaction, embed=embed, view=view)
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="manage_modal_render_failed",
                stage="render",
                modal_kind="manage",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Owner Tools Unavailable",
                    "Babblebox could not finish that private owner response safely right now. Please try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
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
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="edit",
            guild_id=interaction.guild.id,
            failure_message="Babblebox could not continue that private edit flow right now. Please try again in a moment.",
        ):
            return
        try:
            result = await self.cog.service.self_edit_confession(
                interaction.guild,
                author_id=interaction.user.id,
                target_id=self.target_id,
                content=self.body_input.value,
                link=self.link_input.value if self.link_input is not None else None,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="edit_modal_submit_failed",
                stage="submit",
                modal_kind="edit",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Edit Unavailable",
                    "Babblebox could not process that private edit safely right now. Please try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        await self.cog._send_confession_result_response(
            interaction,
            guild_id=interaction.guild.id,
            modal_kind="edit",
            result=result,
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
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="appeal",
            guild_id=interaction.guild.id,
            failure_message="Babblebox could not continue that private appeal flow right now. Please try again in a moment.",
        ):
            return
        try:
            ok, message = await self.cog.service.submit_support_request(
                interaction.guild,
                author_id=interaction.user.id,
                kind="appeal",
                target_id=self.target_input.value,
                details=self.details_input.value,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="appeal_modal_submit_failed",
                stage="submit",
                modal_kind="appeal",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Anonymous Appeal",
                    "Babblebox could not send that anonymous appeal safely right now. Please try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        try:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed("Anonymous Appeal", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="appeal_modal_send_failed",
                stage="send_result",
                modal_kind="appeal",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Anonymous Appeal",
                    "Babblebox received that appeal, but could not finish the private confirmation safely. Please check your appeals channel or try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
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
        if not self.cog.service.storage_ready:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._modal_unavailable_embed(self.cog.service.storage_message("Confessions")),
            )
            return
        if not await self.cog._acknowledge_modal_submit(
            interaction,
            modal_kind="report",
            guild_id=interaction.guild.id,
            failure_message="Babblebox could not continue that private report flow right now. Please try again in a moment.",
        ):
            return
        try:
            ok, message = await self.cog.service.submit_support_request(
                interaction.guild,
                author_id=interaction.user.id,
                kind="report",
                target_id=self.target_input.value,
                details=self.details_input.value,
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="report_modal_submit_failed",
                stage="submit",
                modal_kind="report",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Anonymous Report",
                    "Babblebox could not send that anonymous report safely right now. Please try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        try:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed("Anonymous Report", message, tone="success" if ok else "warning", footer="Babblebox Confessions"),
            )
        except Exception as exc:
            self.cog.log_modal_diagnostic(
                code="report_modal_send_failed",
                stage="send_result",
                modal_kind="report",
                guild_id=interaction.guild.id,
                storage_ready=self.cog.service.storage_ready,
                operability_ready=self.cog.service.operability_message(interaction.guild.id) == "Confessions are ready.",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Anonymous Report",
                    "Babblebox received that report, but could not finish the private confirmation safely. Please check your appeals channel or try again in a moment.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )


class MemberSupportView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, default_target: str | None = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.default_target = default_target
        support_ready = self.cog._support_channel_ready_for_guild_id(guild_id)
        self.appeal_button.disabled = not support_ready
        self.report_button.disabled = not support_ready

    @discord.ui.button(label="Appeal Restriction", style=discord.ButtonStyle.secondary, row=0)
    async def appeal_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._open_appeal_modal(interaction, default_target=self.default_target)

    @discord.ui.button(label="Report Problem", style=discord.ButtonStyle.secondary, row=0)
    async def report_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._open_report_modal(interaction, default_target=self.default_target)


class MemberManageActionView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, target_id: str, can_delete: bool, can_edit: bool):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.target_id = target_id
        self.delete_button.disabled = not can_delete
        self.edit_button.disabled = not can_edit
        self.support_button.disabled = not self.cog._support_channel_ready_for_guild_id(guild_id)

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
        await self.cog._send_support_entry(interaction, default_target=self.target_id)


class MemberResultActionView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, result: ConfessionSubmissionResult):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.result = result
        self.support_button.disabled = not self.cog._support_channel_ready_for_guild_id(guild_id)
        if result.jump_url:
            self.add_item(discord.ui.Button(label="Open Post", style=discord.ButtonStyle.link, url=result.jump_url))

    @discord.ui.button(label="Manage My Confession", style=discord.ButtonStyle.secondary, row=0)
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_target = self.result.confession_id or self.result.case_id
        await self.cog._open_manage_modal(interaction, default_target=default_target)

    @discord.ui.button(label="Reply to confession anonymously", style=discord.ButtonStyle.primary, row=0)
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_target = self.result.confession_id if self.result.submission_kind == "confession" else self.result.parent_confession_id
        await self.cog._open_reply_modal(interaction, default_target=default_target)

    @discord.ui.button(label="Appeal / Report", style=discord.ButtonStyle.secondary, row=1)
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_target = self.result.case_id or self.result.confession_id
        await self.cog._send_support_entry(interaction, default_target=default_target)


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
        try:
            ok, message = await self.cog.service.configure_guild(payload["guild_id"], **payload["updates"])
            runtime_issues: tuple[str, ...] = ()
            if ok and interaction.guild is not None:
                runtime_result = await self.cog.service.sync_runtime_surfaces(interaction.guild, stage_prefix="policy_confirm")
                runtime_issues = runtime_result.issues
            self.cog._pending_policy_updates.pop(self.token, None)
            result_message = self.cog._compose_admin_result(message, list(runtime_issues))
            await interaction.response.edit_message(
                embed=self.cog._admin_status_embed("Confessions Policy", result_message, ok=ok and not runtime_issues),
                view=None,
            )
        except Exception as exc:
            self.cog.log_admin_diagnostic(
                code="policy_confirm_failed",
                stage="policy_confirm",
                guild_id=payload["guild_id"],
                exc=exc,
            )
            self.cog._pending_policy_updates.pop(self.token, None)
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(
                    "Confessions Policy",
                    "Babblebox could not finish that Confessions policy update safely. Run the command again and review the warning before retrying.",
                    ok=False,
                ),
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
        self.support_button.disabled = not self.cog._support_channel_ready_for_guild_id(guild_id)

    async def _resolve_cog(self, interaction: discord.Interaction):
        cog = _resolve_live_confessions_cog(interaction, self.cog)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return None
        return cog

    @discord.ui.button(
        label="Send Confession",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-panel:compose",
        row=0,
    )
    async def send_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._open_confession_modal(interaction)

    @discord.ui.button(
        label="Manage My Confession",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:manage",
        row=0,
    )
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._open_manage_modal(interaction)

    @discord.ui.button(
        label="Appeal / Report",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:support",
        row=1,
    )
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._send_support_entry(interaction)

    @discord.ui.button(
        label="How It Works",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:help",
        row=1,
    )
    async def help_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._send_confession_about(interaction)


class PublishedConfessionReplyView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    async def _resolve_cog(self, interaction: discord.Interaction):
        cog = _resolve_live_confessions_cog(interaction, self.cog)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return None
        return cog

    @discord.ui.button(
        label="Reply to confession anonymously",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-post:reply",
        row=0,
    )
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._handle_published_reply_button(interaction)


class StatelessConfessionMemberPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _resolve_cog(self, interaction: discord.Interaction):
        cog = _resolve_live_confessions_cog(interaction)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return None
        return cog

    @discord.ui.button(
        label="Send Confession",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-panel:compose",
        row=0,
    )
    async def send_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._open_confession_modal(interaction)

    @discord.ui.button(
        label="Manage My Confession",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:manage",
        row=0,
    )
    async def manage_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._open_manage_modal(interaction)

    @discord.ui.button(
        label="Appeal / Report",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:support",
        row=1,
    )
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._send_support_entry(interaction)

    @discord.ui.button(
        label="How It Works",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-panel:help",
        row=1,
    )
    async def help_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = await self._resolve_cog(interaction)
        if cog is None:
            return
        await cog._send_confession_about(interaction)


class StatelessPublishedConfessionReplyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Reply to confession anonymously",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-post:reply",
        row=0,
    )
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = _resolve_live_confessions_cog(interaction)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return
        await cog._handle_published_reply_button(interaction)


class OwnerReplyOpportunitySelect(discord.ui.Select):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, author_id: int, contexts: list[dict[str, Any]]):
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.contexts = contexts
        options = []
        for context in contexts[:5]:
            opportunity = context["opportunity"]
            options.append(
                discord.SelectOption(
                    label=ge.safe_field_text(opportunity["source_author_name"], limit=100),
                    value=opportunity["opportunity_id"],
                    description=ge.safe_field_text(opportunity["source_preview"], limit=100).replace("\n", " "),
                )
            )
        super().__init__(
            placeholder="Choose a member response to review privately",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None or interaction.guild.id != self.guild_id:
            await self.cog._send_private_interaction(interaction, content="That owner-reply flow is no longer available.")
            return
        if interaction.user.id != self.author_id:
            await self.cog._send_private_interaction(interaction, content="Run the command yourself to open your private owner-reply inbox.")
            return
        context, error = await self.cog.service.get_owner_reply_opportunity_context(
            interaction.guild,
            author_id=interaction.user.id,
            opportunity_id=self.values[0],
        )
        if context is None:
            await interaction.response.edit_message(
                embed=ge.make_status_embed(
                    "Owner Reply Unavailable",
                    error or "That owner-reply prompt is no longer available.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
                view=None,
            )
            return
        await interaction.response.edit_message(
            embed=self.cog.service.build_owner_reply_detail_embed(interaction.guild, context),
            view=OwnerReplyOpportunityActionView(
                self.cog,
                guild_id=interaction.guild.id,
                author_id=interaction.user.id,
                opportunity_id=context["opportunity"]["opportunity_id"],
            ),
        )


class OwnerReplyInboxView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, author_id: int, contexts: list[dict[str, Any]]):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        if contexts:
            self.add_item(OwnerReplyOpportunitySelect(cog, guild_id=guild_id, author_id=author_id, contexts=contexts))


class OwnerReplyOpportunityActionView(discord.ui.View):
    def __init__(self, cog: "ConfessionsCog", *, guild_id: int, author_id: int, opportunity_id: str):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.opportunity_id = opportunity_id

    @discord.ui.button(label="Reply anonymously", style=discord.ButtonStyle.primary, row=0)
    async def reply_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None or interaction.guild.id != self.guild_id:
            await self.cog._send_private_interaction(interaction, content="That owner-reply flow is no longer available.")
            return
        if interaction.user.id != self.author_id:
            await self.cog._send_private_interaction(interaction, content="Run the command yourself to open your private owner-reply flow.")
            return
        context, error = await self.cog.service.get_owner_reply_opportunity_context(
            interaction.guild,
            author_id=interaction.user.id,
            opportunity_id=self.opportunity_id,
        )
        if context is None:
            await interaction.response.edit_message(
                embed=ge.make_status_embed(
                    "Owner Reply Unavailable",
                    error or "That owner-reply prompt is no longer available.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
                view=None,
            )
            return
        await interaction.response.send_modal(
            OwnerReplyComposerModal(
                self.cog,
                guild_id=interaction.guild.id,
                opportunity_id=context["opportunity"]["opportunity_id"],
            )
        )

    @discord.ui.button(label="Dismiss", style=discord.ButtonStyle.secondary, row=0)
    async def dismiss_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None or interaction.guild.id != self.guild_id:
            await self.cog._send_private_interaction(interaction, content="That owner-reply flow is no longer available.")
            return
        if interaction.user.id != self.author_id:
            await self.cog._send_private_interaction(interaction, content="Run the command yourself to manage your private owner-reply flow.")
            return
        ok, message = await self.cog.service.dismiss_owner_reply_opportunity(
            interaction.guild,
            author_id=interaction.user.id,
            opportunity_id=self.opportunity_id,
        )
        await interaction.response.edit_message(
            embed=ge.make_status_embed(
                "Owner Reply Inbox",
                message,
                tone="info" if ok else "warning",
                footer="Babblebox Confessions",
            ),
            view=None,
        )

    @discord.ui.button(label="Back to Inbox", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.guild is None or interaction.user is None or interaction.guild.id != self.guild_id:
            await self.cog._send_private_interaction(interaction, content="That owner-reply flow is no longer available.")
            return
        await self.cog._send_owner_reply_inbox(interaction, edit_existing=True)


class StatelessOwnerReplyPromptView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Reply anonymously",
        style=discord.ButtonStyle.primary,
        custom_id="bb-confession-owner-reply:open",
        row=0,
    )
    async def open_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = _resolve_live_confessions_cog(interaction)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return
        await cog._handle_owner_reply_prompt_open(interaction)

    @discord.ui.button(
        label="Dismiss",
        style=discord.ButtonStyle.secondary,
        custom_id="bb-confession-owner-reply:dismiss",
        row=0,
    )
    async def dismiss_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = _resolve_live_confessions_cog(interaction)
        if cog is None:
            await _send_confessions_runtime_unavailable(interaction)
            return
        await cog._handle_owner_reply_prompt_dismiss(interaction)


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
        super().__init__(timeout=None)
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

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_buttons()
        await interaction.response.edit_message(embed=await self.current_embed(), view=self)
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(
                    "Confessions Panel",
                    note,
                    ok=note_ok,
                ),
            )

    async def _safe_action(
        self,
        interaction: discord.Interaction,
        *,
        stage: str,
        failure_message: str,
        action,
    ):
        try:
            return await action()
        except Exception as exc:
            self.cog.log_admin_diagnostic(
                code=f"{stage}_failed",
                stage=stage,
                guild_id=self.guild_id,
                note=f"section={self.section}",
                exc=exc,
            )
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(
                    "Confessions Panel",
                    failure_message,
                    ok=False,
                ),
            )
            return None

    async def _switch_section(self, interaction: discord.Interaction, section: str):
        async def _action():
            self.section = section
            await self._rerender(interaction)

        await self._safe_action(
            interaction,
            stage="panel_switch_section",
            failure_message="Babblebox could not refresh that private confessions panel. Run `/confessions` again to open a fresh one.",
            action=_action,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item[Any]):
        self.cog.log_admin_diagnostic(
            code="panel_callback_unhandled",
            stage="panel_callback",
            guild_id=self.guild_id,
            note=getattr(item, "custom_id", None),
            exc=error,
        )
        await self.cog._send_private_interaction(
            interaction,
            embed=self.cog._admin_status_embed(
                "Confessions Panel",
                "Babblebox could not finish that panel action safely. Run `/confessions` again to open a fresh one.",
                ok=False,
            ),
        )

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary, row=0, custom_id="bb-confession-admin:overview")
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "overview")

    @discord.ui.button(label="Policy", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:policy")
    async def policy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "policy")

    @discord.ui.button(label="Review", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:review")
    async def review_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "review")

    @discord.ui.button(label="Launch", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:launch")
    async def launch_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "launch")

    @discord.ui.button(label="Publish Panel", style=discord.ButtonStyle.success, row=1, custom_id="bb-confession-admin:publish")
    async def publish_panel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        async def _action():
            ok, message = await self.cog.service.sync_member_panel(guild)
            await self._rerender(interaction, note=message, note_ok=ok)

        await self._safe_action(
            interaction,
            stage="panel_publish",
            failure_message="Babblebox could not publish the public confessions panel right now. Check the panel channel and try again.",
            action=_action,
        )

    @discord.ui.button(label="Refresh Queue", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:refresh-queue")
    async def refresh_queue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        async def _action():
            ok, message = await self.cog.service.sync_review_queue(guild, note="Confession review queue refreshed.")
            await self._rerender(interaction, note=message, note_ok=ok)

        await self._safe_action(
            interaction,
            stage="panel_refresh_queue",
            failure_message="Babblebox could not refresh the confession review queue right now. Check the review channel and try again.",
            action=_action,
        )

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:refresh")
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._safe_action(
            interaction,
            stage="panel_refresh",
            failure_message="Babblebox could not refresh that private confessions panel. Run `/confessions` again to open a fresh one.",
            action=lambda: self._rerender(interaction, note="Confessions panel refreshed.", note_ok=True),
        )

    @discord.ui.button(label="Enable", style=discord.ButtonStyle.success, row=1, custom_id="bb-confession-admin:toggle")
    async def toggle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This panel only works inside a server.", ephemeral=True)
            return
        async def _action():
            current = self.cog.service.get_config(self.guild_id)
            ok, message = await self.cog.service.configure_guild(self.guild_id, enabled=not current["enabled"])
            runtime_issues: tuple[str, ...] = ()
            if ok:
                runtime_result = await self.cog.service.sync_runtime_surfaces(
                    guild,
                    stage_prefix="panel_toggle",
                    review_note="Confessions runtime refreshed.",
                )
                runtime_issues = runtime_result.issues
            result_message = self.cog._compose_admin_result(message, list(runtime_issues))
            await self._rerender(interaction, note=result_message, note_ok=ok and not runtime_issues)

        await self._safe_action(
            interaction,
            stage="panel_toggle",
            failure_message="Babblebox could not update those Confessions settings right now. Run `/confessions` again and try once more.",
            action=_action,
        )


class StatelessConfessionsAdminPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _send_expired_notice(self, interaction: discord.Interaction):
        embed = ge.make_status_embed(
            "Confessions Panel Expired",
            CONFESSIONS_ADMIN_PANEL_EXPIRED_MESSAGE,
            tone="info",
            footer="Babblebox Confessions",
        )
        cog = _resolve_live_confessions_cog(interaction)
        if cog is not None:
            await cog._send_private_interaction(interaction, embed=embed)
            return
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:overview")
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Policy", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:policy")
    async def policy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Review", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:review")
    async def review_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Launch", style=discord.ButtonStyle.secondary, row=0, custom_id="bb-confession-admin:launch")
    async def launch_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Publish Panel", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:publish")
    async def publish_panel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Refresh Queue", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:refresh-queue")
    async def refresh_queue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:refresh")
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)

    @discord.ui.button(label="Enable / Disable", style=discord.ButtonStyle.secondary, row=1, custom_id="bb-confession-admin:toggle")
    async def toggle_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._send_expired_notice(interaction)


class ConfessionsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ConfessionsService(bot)
        self._pending_policy_updates: dict[str, dict[str, Any]] = {}
        self._persistent_restore_lock = asyncio.Lock()
        self._persistent_views_restored = False
        harden_admin_root_group(self.confessions_group)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "confessions_service", self.service)
        self._register_global_persistent_views()
        if self.service.storage_ready and self._bot_is_ready():
            await self._restore_runtime_surfaces_once()

    def cog_unload(self):
        if getattr(self.bot, "confessions_service", None) is self.service:
            delattr(self.bot, "confessions_service")
        self.bot.loop.create_task(self.service.close())

    def _bot_is_ready(self) -> bool:
        is_ready = getattr(self.bot, "is_ready", None)
        return bool(callable(is_ready) and is_ready())

    def _register_global_persistent_views(self):
        with contextlib.suppress(Exception):
            self.bot.add_view(StatelessConfessionMemberPanelView())
        with contextlib.suppress(Exception):
            self.bot.add_view(StatelessPublishedConfessionReplyView())
        with contextlib.suppress(Exception):
            self.bot.add_view(StatelessOwnerReplyPromptView())
        with contextlib.suppress(Exception):
            self.bot.add_view(StatelessConfessionsAdminPanelView())

    async def _restore_runtime_surfaces_once(self):
        if not self.service.storage_ready:
            return
        async with self._persistent_restore_lock:
            if self._persistent_views_restored:
                return
            await self.service.resume_member_panels()
            await self.service.resume_public_confession_views()
            await self.service.resume_review_queues()
            self._persistent_views_restored = True

    @commands.Cog.listener()
    async def on_ready(self):
        await self._restore_runtime_surfaces_once()

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

    def build_public_confession_view(self, *, guild_id: int) -> PublishedConfessionReplyView:
        return PublishedConfessionReplyView(self, guild_id=guild_id)

    def build_owner_reply_prompt_view(self) -> StatelessOwnerReplyPromptView:
        return StatelessOwnerReplyPromptView()

    def build_admin_panel_view(self, *, guild_id: int, author_id: int, section: str = "overview") -> ConfessionsAdminPanelView:
        return ConfessionsAdminPanelView(self, guild_id=guild_id, author_id=author_id, section=section)

    def _admin_attention_needed(self, ok: bool, message: str | None) -> bool:
        if not ok:
            return True
        if not message:
            return False
        lowered = message.casefold()
        return (
            "could not" in lowered
            or "unavailable" in lowered
            or "disabled until" in lowered
            or "still needs attention" in lowered
            or "rerun `/confessions`" in lowered
        )

    def _admin_status_embed(
        self,
        title: str,
        message: str,
        *,
        ok: bool,
        tone: str | None = None,
    ) -> discord.Embed:
        resolved_tone = tone or ("warning" if self._admin_attention_needed(ok, message) else "success")
        return ge.make_status_embed(title, message, tone=resolved_tone, footer="Babblebox Confessions")

    def _compose_admin_result(self, base_message: str, issues: list[str]) -> str:
        cleaned_issues: list[str] = []
        for issue in issues:
            text = str(issue).strip()
            if text and text not in cleaned_issues:
                cleaned_issues.append(text)
        if not cleaned_issues:
            return base_message
        return f"{base_message} Runtime follow-up still needs attention: {' '.join(cleaned_issues)}"

    def log_admin_diagnostic(
        self,
        *,
        code: str,
        stage: str,
        guild_id: int | None,
        note: str | None = None,
        exc: Exception | None = None,
    ):
        parts = [
            f"code={code}",
            f"stage={stage}",
            f"guild_id={guild_id if guild_id is not None else 'none'}",
        ]
        if note:
            parts.append(f"note={str(note)[:160]}")
        if exc is not None:
            parts.append(f"exception={type(exc).__name__}")
        parts.append(f"backend={getattr(self.service.store, 'backend_name', 'unknown')}")
        message = f"Confessions admin diagnostic: {', '.join(parts)}"
        if exc is not None:
            LOGGER.exception(message)
            return
        LOGGER.warning(message)

    def modal_file_upload_available(self) -> bool:
        file_upload = getattr(discord.ui, "FileUpload", None)
        return file_upload is not None and callable(getattr(file_upload, "to_component_dict", None))

    def _is_default_role(self, role: discord.Role) -> bool:
        is_default = getattr(role, "is_default", None)
        if callable(is_default):
            with contextlib.suppress(Exception):
                return bool(is_default())
        guild = getattr(role, "guild", None)
        return getattr(role, "id", None) == getattr(guild, "id", None)

    def _support_channel_ready_for_guild_id(self, guild_id: int) -> bool:
        guild = self.bot.get_guild(guild_id)
        return bool(guild is not None and self.service.support_channel_snapshot(guild)["ok"])

    async def _ensure_support_channel_ready(self, interaction: discord.Interaction) -> dict[str, object] | None:
        if interaction.guild is None:
            await interaction.response.send_message("Private support only works inside a server.", ephemeral=True)
            return None
        snapshot = self.service.support_channel_snapshot(interaction.guild)
        if snapshot["ok"]:
            return snapshot
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Private Support Unavailable",
                str(snapshot["message"]),
                tone="warning",
                footer="Babblebox Confessions",
            ),
            ephemeral=True,
        )
        return None

    async def _send_slash_only_notice(self, ctx: commands.Context, message: str):
        await ctx.send(content=message, delete_after=15)

    async def _open_confession_modal(self, interaction: discord.Interaction):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        ready_message = self.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            unavailable = ConfessionSubmissionResult(False, "unavailable", ready_message)
            await interaction.response.send_message(embed=self.service.build_member_result_embed(unavailable), ephemeral=True)
            return
        gate_message = self.service.member_submission_gate_message(
            interaction.guild,
            submission_kind="confession",
            author_id=interaction.user.id,
            member=interaction.user,
        )
        if gate_message is not None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Confession Access", gate_message, tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(ConfessionComposerModal(self, guild_id=interaction.guild.id))

    async def _open_reply_modal(self, interaction: discord.Interaction, *, default_target: str | None = None):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        ready_message = self.service.operability_message(interaction.guild.id)
        if ready_message != "Confessions are ready.":
            unavailable = ConfessionSubmissionResult(False, "unavailable", ready_message, submission_kind="reply")
            await interaction.response.send_message(embed=self.service.build_member_result_embed(unavailable), ephemeral=True)
            return
        config = self.service.get_config(interaction.guild.id)
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
        gate_message = self.service.member_submission_gate_message(
            interaction.guild,
            submission_kind="reply",
            author_id=interaction.user.id,
            member=interaction.user,
        )
        if gate_message is not None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Reply Access", gate_message, tone="warning", footer="Babblebox Confessions"),
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(ReplyComposerModal(self, guild_id=interaction.guild.id, default_target=default_target))

    async def _handle_published_reply_button(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Anonymous replies only work inside a server.", ephemeral=True)
            return
        message = getattr(interaction, "message", None)
        message_id = getattr(message, "id", None)
        if not isinstance(message_id, int):
            await interaction.response.send_message("That confession is not available for anonymous replies.", ephemeral=True)
            return
        submission = await self.service.store.fetch_submission_by_message_id(interaction.guild.id, message_id)
        if submission is None or submission.get("status") != "published" or submission.get("submission_kind") != "confession":
            await interaction.response.send_message("That confession is not available for anonymous replies.", ephemeral=True)
            return
        await self._open_reply_modal(interaction, default_target=submission["confession_id"])

    async def _send_owner_reply_inbox(self, interaction: discord.Interaction, *, edit_existing: bool = False):
        if interaction.guild is None or interaction.user is None:
            await self._send_private_interaction(interaction, content="Private owner replies only work inside a server.")
            return
        contexts = await self.service.list_pending_owner_reply_contexts(
            interaction.guild,
            author_id=interaction.user.id,
            limit=5,
        )
        embed = self.service.build_owner_reply_inbox_embed(interaction.guild, contexts)
        view = OwnerReplyInboxView(self, guild_id=interaction.guild.id, author_id=interaction.user.id, contexts=contexts) if contexts else None
        if edit_existing:
            await interaction.response.edit_message(embed=embed, view=view)
            return
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _handle_owner_reply_prompt_open(self, interaction: discord.Interaction):
        message = getattr(interaction, "message", None)
        message_id = getattr(message, "id", None)
        author = getattr(interaction, "user", None)
        if not isinstance(message_id, int) or author is None:
            await self._send_private_interaction(interaction, content="That owner-reply prompt is no longer available.")
            return
        context, error = await self.service.get_owner_reply_opportunity_context_from_notification_message(
            notification_message_id=message_id,
            author_id=author.id,
        )
        if context is None:
            await self._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Owner Reply Unavailable",
                    error or "That owner-reply prompt is no longer available.",
                    tone="warning",
                    footer="Babblebox Confessions",
                ),
            )
            return
        await interaction.response.send_modal(
            OwnerReplyComposerModal(
                self,
                guild_id=context["guild"].id,
                opportunity_id=context["opportunity"]["opportunity_id"],
            )
        )

    async def _handle_owner_reply_prompt_dismiss(self, interaction: discord.Interaction):
        message = getattr(interaction, "message", None)
        message_id = getattr(message, "id", None)
        author = getattr(interaction, "user", None)
        if not isinstance(message_id, int) or author is None:
            await self._send_private_interaction(interaction, content="That owner-reply prompt is no longer available.")
            return
        ok, note = await self.service.dismiss_owner_reply_opportunity_from_notification(
            notification_message_id=message_id,
            author_id=author.id,
        )
        await interaction.response.edit_message(
            embed=ge.make_status_embed(
                "Owner Reply Prompt",
                note,
                tone="info" if ok else "warning",
                footer="Babblebox Confessions",
            ),
            view=None,
        )

    async def _open_manage_modal(self, interaction: discord.Interaction, *, default_target: str | None = None):
        if interaction.guild is None:
            await interaction.response.send_message("Private owner tools only work inside a server.", ephemeral=True)
            return
        await interaction.response.send_modal(ManageConfessionModal(self, default_target=default_target))

    async def _send_support_entry(self, interaction: discord.Interaction, *, default_target: str | None = None):
        snapshot = await self._ensure_support_channel_ready(interaction)
        if snapshot is None:
            return
        await interaction.response.send_message(
            embed=ge.make_status_embed(
                "Private Support",
                "Choose whether you want to appeal a restriction or report a problem without exposing your account to staff.",
                tone="info",
                footer="Babblebox Confessions",
            ),
            view=MemberSupportView(self, guild_id=interaction.guild.id, default_target=default_target),
            ephemeral=True,
        )

    async def _open_appeal_modal(self, interaction: discord.Interaction, *, default_target: str | None = None):
        snapshot = await self._ensure_support_channel_ready(interaction)
        if snapshot is None:
            return
        await interaction.response.send_modal(AppealModal(self, default_target=default_target))

    async def _open_report_modal(self, interaction: discord.Interaction, *, default_target: str | None = None):
        snapshot = await self._ensure_support_channel_ready(interaction)
        if snapshot is None:
            return
        await interaction.response.send_modal(ReportModal(self, default_target=default_target))

    async def _send_confession_about(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Anonymous confessions only work inside a server.", ephemeral=True)
            return
        await interaction.response.send_message(embed=self.service.build_member_panel_help_embed(interaction.guild), ephemeral=True)

    def _modal_unavailable_embed(self, message: str) -> discord.Embed:
        return ge.make_status_embed("Confessions Unavailable", message, tone="warning", footer="Babblebox Confessions")

    def _minimal_member_result_embed(self, result: ConfessionSubmissionResult) -> discord.Embed:
        noun_title = "Reply" if result.submission_kind == "reply" else "Confession"
        title_map = {
            "published": f"{noun_title} Posted",
            "queued": f"{noun_title} Received",
            "blocked": f"{noun_title} Not Sent",
            "restricted": "Confessions Paused",
            "unavailable": "Confessions Unavailable",
        }
        tone_map = {
            "published": "success",
            "queued": "info",
            "blocked": "warning",
            "restricted": "warning",
            "unavailable": "warning",
        }
        embed = ge.make_status_embed(
            title_map.get(result.state, "Anonymous Confession"),
            result.message,
            tone=tone_map.get(result.state, "info"),
            footer="Babblebox Confessions",
        )
        if result.confession_id is not None:
            embed.add_field(name="Confession ID", value=f"`{result.confession_id}`", inline=True)
        if result.parent_confession_id is not None:
            embed.add_field(name="Replying To", value=f"`{result.parent_confession_id}`", inline=True)
        if result.state == "queued":
            embed.add_field(name="Status", value="Private review", inline=True)
        elif result.state == "published":
            embed.add_field(name="Status", value="Live", inline=True)
        return embed

    def log_modal_diagnostic(
        self,
        *,
        code: str,
        stage: str,
        modal_kind: str,
        guild_id: int | None,
        allow_images: bool | None = None,
        upload_present: bool | None = None,
        attachment_count: int | None = None,
        storage_ready: bool | None = None,
        operability_ready: bool | None = None,
        result_state: str | None = None,
        exc: Exception | None = None,
    ):
        parts = [
            f"code={code}",
            f"stage={stage}",
            f"modal={modal_kind}",
            f"guild_id={guild_id if guild_id is not None else 'none'}",
        ]
        if storage_ready is not None:
            parts.append(f"storage_ready={bool(storage_ready)}")
        if operability_ready is not None:
            parts.append(f"operability_ready={bool(operability_ready)}")
        if allow_images is not None:
            parts.append(f"allow_images={bool(allow_images)}")
        if upload_present is not None:
            parts.append(f"upload_present={bool(upload_present)}")
        if attachment_count is not None:
            parts.append(f"attachment_count={int(max(0, attachment_count))}")
        if result_state:
            parts.append(f"result_state={result_state}")
        if exc is not None:
            parts.append(f"exception={type(exc).__name__}")
        parts.append(f"backend={getattr(self.service.store, 'backend_name', 'unknown')}")
        message = f"Confessions modal diagnostic: {', '.join(parts)}"
        if exc is not None:
            LOGGER.exception(message)
            return
        LOGGER.warning(message)

    async def _send_private_interaction(self, interaction: discord.Interaction, **kwargs):
        if interaction.guild is not None:
            kwargs["ephemeral"] = True
        else:
            kwargs.pop("ephemeral", None)
        try:
            if interaction.response.is_done():
                return await interaction.followup.send(**kwargs)
            return await interaction.response.send_message(**kwargs)
        except discord.InteractionResponded:
            with contextlib.suppress(discord.NotFound, discord.HTTPException):
                return await interaction.followup.send(**kwargs)
            return None
        except (discord.NotFound, discord.HTTPException):
            return None

    async def _acknowledge_modal_submit(
        self,
        interaction: discord.Interaction,
        *,
        modal_kind: str,
        guild_id: int,
        failure_message: str,
        allow_images: bool | None = None,
        upload_present: bool | None = None,
        attachment_count: int | None = None,
    ) -> bool:
        try:
            await interaction.response.defer(ephemeral=interaction.guild is not None, thinking=True)
            return True
        except Exception as exc:
            self.log_modal_diagnostic(
                code=f"{modal_kind}_modal_defer_failed",
                stage="defer",
                modal_kind=modal_kind,
                guild_id=guild_id,
                allow_images=allow_images,
                upload_present=upload_present,
                attachment_count=attachment_count,
                storage_ready=self.service.storage_ready,
                operability_ready=self.service.operability_message(guild_id) == "Confessions are ready.",
                exc=exc,
            )
            await self._send_private_interaction(interaction, embed=self._modal_unavailable_embed(failure_message))
            return False

    async def _send_confession_result_response(
        self,
        interaction: discord.Interaction,
        *,
        guild_id: int,
        modal_kind: str,
        result: ConfessionSubmissionResult,
        allow_images: bool | None = None,
        upload_present: bool | None = None,
        attachment_count: int | None = None,
    ):
        try:
            embed = self.service.build_member_result_embed(result)
        except Exception as exc:
            self.log_modal_diagnostic(
                code=f"{modal_kind}_modal_build_embed_failed",
                stage="build_embed",
                modal_kind=modal_kind,
                guild_id=guild_id,
                allow_images=allow_images,
                upload_present=upload_present,
                attachment_count=attachment_count,
                storage_ready=self.service.storage_ready,
                operability_ready=self.service.operability_message(guild_id) == "Confessions are ready.",
                result_state=result.state,
                exc=exc,
            )
            embed = self._minimal_member_result_embed(result)
        try:
            view = self.build_member_result_view(result=result, guild_id=guild_id)
        except Exception as exc:
            self.log_modal_diagnostic(
                code=f"{modal_kind}_modal_build_view_failed",
                stage="build_view",
                modal_kind=modal_kind,
                guild_id=guild_id,
                allow_images=allow_images,
                upload_present=upload_present,
                attachment_count=attachment_count,
                storage_ready=self.service.storage_ready,
                operability_ready=self.service.operability_message(guild_id) == "Confessions are ready.",
                result_state=result.state,
                exc=exc,
            )
            view = None
        try:
            await self._send_private_interaction(interaction, embed=embed, view=view)
            return
        except Exception as exc:
            self.log_modal_diagnostic(
                code=f"{modal_kind}_modal_send_result_failed",
                stage="send_result",
                modal_kind=modal_kind,
                guild_id=guild_id,
                allow_images=allow_images,
                upload_present=upload_present,
                attachment_count=attachment_count,
                storage_ready=self.service.storage_ready,
                operability_ready=self.service.operability_message(guild_id) == "Confessions are ready.",
                result_state=result.state,
                exc=exc,
            )
        with contextlib.suppress(Exception):
            await self._send_private_interaction(interaction, embed=embed)

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

    async def _delete_stored_panel_message(self, guild: discord.Guild, config: dict[str, object]) -> str | None:
        channel_id = config.get("panel_channel_id")
        message_id = config.get("panel_message_id")
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return None
        channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
        if channel is None:
            return None
        fetch_message = getattr(channel, "fetch_message", None)
        if not callable(fetch_message):
            return None
        try:
            message = await fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return None
        except Exception as exc:
            self.log_admin_diagnostic(
                code="setup_fetch_stale_panel_failed",
                stage="setup_fetch_stale_panel",
                guild_id=guild.id,
                note=f"message_id={message_id}",
                exc=exc,
            )
            return "Babblebox could not inspect the previous public confessions panel message."
        if message is not None:
            try:
                await message.delete()
            except (discord.Forbidden, discord.HTTPException):
                return "Babblebox could not remove the previous public confessions panel message."
            except Exception as exc:
                self.log_admin_diagnostic(
                    code="setup_delete_stale_panel_failed",
                    stage="setup_delete_stale_panel",
                    guild_id=guild.id,
                    note=f"message_id={message_id}",
                    exc=exc,
                )
                return "Babblebox could not remove the previous public confessions panel message."
        return None

    async def _send_admin_panel(self, ctx: commands.Context, *, section: str = "overview"):
        view = self.build_admin_panel_view(guild_id=ctx.guild.id, author_id=ctx.author.id, section=section)
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

    async def _run_admin_command(
        self,
        ctx: commands.Context,
        *,
        title: str,
        stage: str,
        failure_message: str,
        action,
    ):
        try:
            if not await self._require_admin(ctx):
                return None
            return await action()
        except Exception as exc:
            self.log_admin_diagnostic(
                code=f"{stage}_failed",
                stage=stage,
                guild_id=getattr(getattr(ctx, "guild", None), "id", None),
                exc=exc,
            )
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed(title, failure_message, ok=False),
                ephemeral=True,
            )
            return None

    @commands.hybrid_group(
        name="confess",
        with_app_command=True,
        description="Private anonymous confession and support flows",
        fallback="create",
        invoke_without_command=True,
    )
    async def confess_group(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            await self._send_slash_only_notice(ctx, "Use `/confess create` in a server to open the private confession composer.")
            return
        await self._open_confession_modal(interaction)

    @confess_group.command(name="manage", description="Open the private manage-my-confession flow")
    async def confess_manage_command(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            await self._send_slash_only_notice(ctx, "Use `/confess manage` in a server to open the private manage flow.")
            return
        await self._open_manage_modal(interaction)

    @confess_group.command(name="appeal", description="Open the private anonymous appeal flow")
    async def confess_appeal_command(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            await self._send_slash_only_notice(ctx, "Use `/confess appeal` in a server to open the private appeal flow.")
            return
        await self._open_appeal_modal(interaction)

    @confess_group.command(name="report", description="Open the private anonymous report flow")
    async def confess_report_command(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            await self._send_slash_only_notice(ctx, "Use `/confess report` in a server to open the private report flow.")
            return
        await self._open_report_modal(interaction)

    @confess_group.command(name="reply-to-user", description="Review member responses to your confession and post an anonymous owner reply")
    async def confess_reply_to_user_command(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            await self._send_slash_only_notice(ctx, "Use `/confess reply-to-user` in a server to review private owner-reply opportunities.")
            return
        await self._send_owner_reply_inbox(interaction)

    @confess_group.command(name="about", description="Learn how anonymous confessions work in this server")
    async def confess_about_command(self, ctx: commands.Context):
        interaction = getattr(ctx, "interaction", None)
        if interaction is None:
            if ctx.guild is None:
                await ctx.send(content="Anonymous confessions only work inside a server.", delete_after=15)
                return
            await ctx.send(embed=self.service.build_member_panel_help_embed(ctx.guild))
            return
        await self._send_confession_about(interaction)

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    @commands.hybrid_group(name="confessions", with_app_command=True, description="Admin controls for the optional Confessions feature", invoke_without_command=True)
    async def confessions_group(self, ctx: commands.Context):
        async def _action():
            await self._send_admin_panel(ctx, section="overview")

        await self._run_admin_command(
            ctx,
            title="Confessions Panel",
            stage="panel_open",
            failure_message="Babblebox could not open the private confessions panel right now. Run `/confessions` again in a moment.",
            action=_action,
        )

    @confessions_group.command(name="status", description="Open the Confessions dashboard or inspect one confession/case")
    async def confessions_status_command(self, ctx: commands.Context, target_id: Optional[str] = None):
        async def _action():
            if not target_id:
                await self._send_admin_panel(ctx, section="overview")
                return
            await send_hybrid_response(ctx, embed=await self.service.build_target_status_embed(ctx.guild, target_id), ephemeral=True)

        await self._run_admin_command(
            ctx,
            title="Confessions Status",
            stage="status_command",
            failure_message="Babblebox could not open that private Confessions status view right now. Run `/confessions` again in a moment.",
            action=_action,
        )

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
        async def _action():
            if appeals_channel is not None:
                support_snapshot = self.service.support_channel_snapshot(ctx.guild, channel_id=appeals_channel.id)
                if not support_snapshot["ok"]:
                    self.log_admin_diagnostic(
                        code="setup_validate_rejected",
                        stage="setup_validate",
                        guild_id=ctx.guild.id,
                        note=str(support_snapshot["message"]),
                    )
                    await send_hybrid_response(
                        ctx,
                        embed=self._admin_status_embed("Confessions Setup", str(support_snapshot["message"]), ok=False),
                        ephemeral=True,
                    )
                    return
            previous_config = self.service.get_config(ctx.guild.id)
            try:
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
            except Exception as exc:
                self.log_admin_diagnostic(
                    code="setup_configure_failed",
                    stage="setup_configure",
                    guild_id=ctx.guild.id,
                    exc=exc,
                )
                raise
            issues: list[str] = []
            if ok:
                if clear_panel:
                    stale_panel_issue = await self._delete_stored_panel_message(ctx.guild, previous_config)
                    if stale_panel_issue:
                        issues.append(stale_panel_issue)
                runtime_result = await self.service.sync_runtime_surfaces(
                    ctx.guild,
                    stage_prefix="setup_sync",
                    review_note="Confessions runtime refreshed.",
                )
                issues.extend(runtime_result.issues)
                current_config = self.service.get_config(ctx.guild.id)
                previous_panel_channel_id = previous_config.get("panel_channel_id")
                previous_panel_message_id = previous_config.get("panel_message_id")
                if (
                    not clear_panel
                    and isinstance(previous_panel_channel_id, int)
                    and isinstance(previous_panel_message_id, int)
                    and previous_panel_channel_id != current_config.get("panel_channel_id")
                    and current_config.get("panel_message_id") != previous_panel_message_id
                ):
                    stale_panel_issue = await self._delete_stored_panel_message(ctx.guild, previous_config)
                    if stale_panel_issue:
                        issues.append(stale_panel_issue)
            result_message = self._compose_admin_result(message, issues)
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Setup", result_message, ok=ok and not issues),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Setup",
            stage="setup_command",
            failure_message="Babblebox could not finish that Confessions setup update safely. Review the selected channels and run `/confessions setup` again.",
            action=_action,
        )

    @app_commands.describe(
        block_adult_language="Block adult or 18+ language",
        allow_trusted_links="Allow Babblebox's trusted link families",
        allow_images="Enable image attachments for confessions",
        allow_replies="Enable anonymous replies",
        allow_owner_replies="Enable owner-bound anonymous owner replies when members respond to a confession",
        owner_reply_review="Send owner replies through private review before posting",
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
        allow_owner_replies: Optional[bool] = None,
        owner_reply_review: Optional[bool] = None,
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
        async def _action():
            current = self.service.get_config(ctx.guild.id)
            updates = {
                "block_adult_language": block_adult_language,
                "allow_trusted_mainstream_links": allow_trusted_links,
                "allow_images": allow_images,
                "allow_anonymous_replies": allow_replies,
                "allow_owner_replies": allow_owner_replies,
                "owner_reply_review_mode": owner_reply_review,
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
            runtime_issues: list[str] = []
            if ok:
                runtime_result = await self.service.sync_runtime_surfaces(ctx.guild, stage_prefix="policy_update")
                runtime_issues = list(runtime_result.issues)
            result_message = self._compose_admin_result(message, runtime_issues)
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Policy", result_message, ok=ok and not runtime_issues),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Policy",
            stage="policy_command",
            failure_message="Babblebox could not finish that Confessions policy update safely. Review the policy values and try again.",
            action=_action,
        )

    @app_commands.choices(bucket=DOMAIN_BUCKET_CHOICES, mode=DOMAIN_MODE_CHOICES)
    @confessions_group.command(name="domains", description="Update the Confessions domain allowlist or blocklist")
    async def confessions_domains_command(self, ctx: commands.Context, bucket: str, mode: str, domain: str):
        async def _action():
            ok, message = await self.service.update_domain_policy(ctx.guild.id, bucket=bucket, domain=domain, enabled=mode == "add")
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Domains", message, ok=ok),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Domains",
            stage="domains_command",
            failure_message="Babblebox could not update that Confessions domain rule right now. Try again in a moment.",
            action=_action,
        )

    @confessions_group.group(
        name="role",
        with_app_command=True,
        invoke_without_command=True,
        description="Manage which roles can submit anonymous confessions",
    )
    async def confessions_role_group(self, ctx: commands.Context):
        async def _action():
            await send_hybrid_response(ctx, embed=self.service.build_role_policy_embed(ctx.guild), ephemeral=True)

        await self._run_admin_command(
            ctx,
            title="Confessions Role Eligibility",
            stage="role_group",
            failure_message="Babblebox could not open the Confessions role policy view right now. Try `/confessions role` again in a moment.",
            action=_action,
        )

    @app_commands.describe(role="Role to add or remove from the Confessions allowlist", state="Turn this allowlist entry on or off")
    @app_commands.choices(state=ROLE_STATE_CHOICES)
    @confessions_role_group.command(name="allowlist", description="Add or remove a role from the Confessions allowlist")
    async def confessions_role_allowlist_command(self, ctx: commands.Context, role: discord.Role, state: str = "on"):
        async def _action():
            if self._is_default_role(role):
                await send_hybrid_response(
                    ctx,
                    embed=self._admin_status_embed(
                        "Confessions Role Eligibility",
                        "Babblebox does not allow `@everyone` in the Confessions role allowlist.",
                        ok=False,
                    ),
                    ephemeral=True,
                )
                return
            ok, message = await self.service.update_role_policy(ctx.guild.id, bucket="allow", role_id=role.id, enabled=state == "on")
            runtime_issues: list[str] = []
            if ok:
                runtime_result = await self.service.sync_runtime_surfaces(ctx.guild, stage_prefix="role_allowlist")
                runtime_issues = list(runtime_result.issues)
            result_message = self._compose_admin_result(message, runtime_issues)
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Role Eligibility", result_message, ok=ok and not runtime_issues),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Role Eligibility",
            stage="role_allowlist_command",
            failure_message="Babblebox could not update that Confessions allowlist entry right now. Try again in a moment.",
            action=_action,
        )

    @app_commands.describe(role="Role to add or remove from the Confessions blacklist", state="Turn this blacklist entry on or off")
    @app_commands.choices(state=ROLE_STATE_CHOICES)
    @confessions_role_group.command(name="blacklist", description="Add or remove a role from the Confessions blacklist")
    async def confessions_role_blacklist_command(self, ctx: commands.Context, role: discord.Role, state: str = "on"):
        async def _action():
            if self._is_default_role(role):
                await send_hybrid_response(
                    ctx,
                    embed=self._admin_status_embed(
                        "Confessions Role Eligibility",
                        "Babblebox does not allow `@everyone` in the Confessions role blacklist.",
                        ok=False,
                    ),
                    ephemeral=True,
                )
                return
            ok, message = await self.service.update_role_policy(ctx.guild.id, bucket="block", role_id=role.id, enabled=state == "on")
            runtime_issues: list[str] = []
            if ok:
                runtime_result = await self.service.sync_runtime_surfaces(ctx.guild, stage_prefix="role_blacklist")
                runtime_issues = list(runtime_result.issues)
            result_message = self._compose_admin_result(message, runtime_issues)
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Role Eligibility", result_message, ok=ok and not runtime_issues),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Role Eligibility",
            stage="role_blacklist_command",
            failure_message="Babblebox could not update that Confessions blacklist entry right now. Try again in a moment.",
            action=_action,
        )

    @app_commands.describe(target="Reset the allowlist, blacklist, or both")
    @app_commands.choices(target=ROLE_RESET_CHOICES)
    @confessions_role_group.command(name="reset", description="Reset Confessions role allowlist or blacklist state")
    async def confessions_role_reset_command(self, ctx: commands.Context, target: str):
        async def _action():
            ok, message = await self.service.reset_role_policy(ctx.guild.id, target=target)
            runtime_issues: list[str] = []
            if ok:
                runtime_result = await self.service.sync_runtime_surfaces(ctx.guild, stage_prefix="role_reset")
                runtime_issues = list(runtime_result.issues)
            result_message = self._compose_admin_result(message, runtime_issues)
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Role Eligibility", result_message, ok=ok and not runtime_issues),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Role Eligibility",
            stage="role_reset_command",
            failure_message="Babblebox could not reset that Confessions role policy right now. Try again in a moment.",
            action=_action,
        )

    @confessions_group.command(name="panel", description="Publish or refresh the public Confessions panel")
    async def confessions_panel_command(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        async def _action():
            ok, message = await self.service.sync_member_panel(ctx.guild, channel_id=getattr(channel, "id", None))
            await send_hybrid_response(
                ctx,
                embed=self._admin_status_embed("Confessions Panel", message, ok=ok),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Panel",
            stage="panel_command",
            failure_message="Babblebox could not publish the Confessions panel right now. Check the panel channel and try again.",
            action=_action,
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
        async def _action():
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
                embed=self._admin_status_embed("Confessions Moderation", message, ok=ok),
                ephemeral=True,
            )

        await self._run_admin_command(
            ctx,
            title="Confessions Moderation",
            stage="moderate_command",
            failure_message="Babblebox could not finish that Confessions moderation action right now. Refresh the status view and try again.",
            action=_action,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(ConfessionsCog(bot))
