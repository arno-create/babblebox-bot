from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.admin_service import (
    FOLLOWUP_MODE_LABELS,
    REVIEW_ACTION_LABELS,
    VERIFICATION_DEADLINE_ACTION_LABELS,
    VERIFICATION_REVIEW_ACTION_LABELS,
    VERIFICATION_LOGIC_LABELS,
    AdminService,
    VerificationPrecheck,
    VerificationSyncPreview,
    VerificationSyncSession,
    VerificationSyncSummary,
    _followup_duration_label,
)
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.utility_helpers import deserialize_datetime, format_duration_brief


FOLLOWUP_MODE_CHOICES = [
    app_commands.Choice(name="Auto remove", value="auto_remove"),
    app_commands.Choice(name="Moderator review", value="review"),
]
VERIFICATION_LOGIC_CHOICES = [
    app_commands.Choice(name="Unverified if member DOES NOT have this role", value="must_have_role"),
    app_commands.Choice(name="Unverified if member DOES have this role", value="must_not_have_role"),
]
VERIFICATION_DEADLINE_ACTION_CHOICES = [
    app_commands.Choice(name="Kick automatically", value="auto_kick"),
    app_commands.Choice(name="Moderator review", value="review"),
]
STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
EXCLUSION_TARGET_CHOICES = [
    app_commands.Choice(name="Exclude member", value="excluded_user_ids"),
    app_commands.Choice(name="Exclude role", value="excluded_role_ids"),
    app_commands.Choice(name="Trusted role", value="trusted_role_ids"),
]
ADMIN_TEST_CHOICES = [
    app_commands.Choice(name="Warning DM", value="warning_dm"),
    app_commands.Choice(name="Final kick DM", value="kick_dm"),
    app_commands.Choice(name="Logs channel", value="logs"),
]


class FollowupReviewView(discord.ui.View):
    def __init__(self, *, guild_id: int, user_id: int, version: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id
        self.version = version
        self.add_item(self._make_button("remove", discord.ButtonStyle.danger))
        self.add_item(self._make_button("delay_week", discord.ButtonStyle.secondary))
        self.add_item(self._make_button("delay_month", discord.ButtonStyle.secondary))
        self.add_item(self._make_button("keep", discord.ButtonStyle.success))

    def _make_button(self, action: str, style: discord.ButtonStyle) -> discord.ui.Button:
        button = discord.ui.Button(
            label=REVIEW_ACTION_LABELS[action],
            style=style,
            custom_id=f"bb-admin-followup:{action}:{self.guild_id}:{self.user_id}:{self.version}",
        )
        async def _callback(interaction: discord.Interaction):
            await self._handle_action(interaction, action)

        button.callback = _callback
        return button

    async def _handle_action(self, interaction: discord.Interaction, action: str):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("This review action only works inside a server.", ephemeral=True)
            return
        perms = getattr(interaction.user, "guild_permissions", None)
        if not (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to use follow-up review actions.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return
        service = getattr(interaction.client, "admin_service", None)
        if service is None:
            await interaction.response.send_message("Babblebox admin systems are unavailable right now.", ephemeral=True)
            return
        ok, message, record = await service.handle_review_action(
            guild_id=self.guild_id,
            user_id=self.user_id,
            version=self.version,
            action=action,
            actor=interaction.user,
        )
        if not ok:
            if "stale" in message.lower() or "closed" in message.lower():
                for child in self.children:
                    child.disabled = True
                embed = service.build_followup_resolution_embed(
                    record or {"user_id": self.user_id, "role_id": 0},
                    message=message,
                    success=False,
                )
                await interaction.response.edit_message(embed=embed, view=self)
                return
            await interaction.response.send_message(message, ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        embed = service.build_followup_resolution_embed(record or {"user_id": self.user_id, "role_id": 0}, message=message, success=True)
        await interaction.response.edit_message(embed=embed, view=self)


class VerificationDeadlineReviewView(discord.ui.View):
    def __init__(self, *, guild_id: int, user_id: int, version: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id
        self.user_id = user_id
        self.version = version
        self.add_item(self._make_button("kick", discord.ButtonStyle.danger))
        self.add_item(self._make_button("delay", discord.ButtonStyle.secondary))
        self.add_item(self._make_button("ignore", discord.ButtonStyle.success))

    def _make_button(self, action: str, style: discord.ButtonStyle) -> discord.ui.Button:
        button = discord.ui.Button(
            label=VERIFICATION_REVIEW_ACTION_LABELS[action],
            style=style,
            custom_id=f"bb-admin-verification-review:{action}:{self.guild_id}:{self.user_id}:{self.version}",
        )

        async def _callback(interaction: discord.Interaction):
            await self._handle_action(interaction, action)

        button.callback = _callback
        return button

    async def _handle_action(self, interaction: discord.Interaction, action: str):
        if interaction.guild is None or interaction.user is None:
            await interaction.response.send_message("This verification review action only works inside a server.", ephemeral=True)
            return
        perms = getattr(interaction.user, "guild_permissions", None)
        if not (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to use verification review actions.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return
        service = getattr(interaction.client, "admin_service", None)
        if service is None:
            await interaction.response.send_message("Babblebox admin systems are unavailable right now.", ephemeral=True)
            return
        ok, message, record = await service.handle_verification_review_action(
            guild_id=self.guild_id,
            user_id=self.user_id,
            version=self.version,
            action=action,
            actor=interaction.user,
        )
        if not ok:
            if "stale" in message.lower() or "closed" in message.lower():
                for child in self.children:
                    child.disabled = True
                embed = service.build_verification_review_resolution_embed(
                    record or {"user_id": self.user_id},
                    message=message,
                    success=False,
                )
                await interaction.response.edit_message(embed=embed, view=self)
                return
            await interaction.response.send_message(message, ephemeral=True)
            return
        for child in self.children:
            child.disabled = True
        embed = service.build_verification_review_resolution_embed(
            record or {"user_id": self.user_id},
            message=message,
            success=True,
        )
        await interaction.response.edit_message(embed=embed, view=self)


class AdminPanelView(discord.ui.View):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, section: str = "overview"):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.section = section
        self.message: discord.Message | None = None
        self._refresh_buttons()

    async def current_embed(self) -> discord.Embed:
        return await self.cog.build_panel_embed(self.guild_id, self.section)

    def _refresh_buttons(self):
        mapping = {
            "overview": self.overview_button,
            "followup": self.followup_button,
            "verification": self.verification_button,
            "exclusions": self.exclusions_button,
            "logs": self.logs_button,
            "templates": self.templates_button,
        }
        for name, button in mapping.items():
            button.style = discord.ButtonStyle.primary if self.section == name else discord.ButtonStyle.secondary

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/admin panel` to open your own admin panel.",
                    tone="info",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        if not self.cog.user_can_manage_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure these admin systems.",
                    tone="warning",
                    footer="Babblebox Admin",
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

    async def _render(self, interaction: discord.Interaction, *, note: str | None = None):
        self._refresh_buttons()
        await interaction.response.edit_message(embed=await self.current_embed(), view=self)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary, row=0)
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "overview"
        await self._render(interaction)

    @discord.ui.button(label="Follow-up", style=discord.ButtonStyle.secondary, row=0)
    async def followup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "followup"
        await self._render(interaction)

    @discord.ui.button(label="Verification", style=discord.ButtonStyle.secondary, row=0)
    async def verification_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "verification"
        await self._render(interaction)

    @discord.ui.button(label="Exclusions", style=discord.ButtonStyle.secondary, row=0)
    async def exclusions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "exclusions"
        await self._render(interaction)

    @discord.ui.button(label="Logs", style=discord.ButtonStyle.secondary, row=0)
    async def logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "logs"
        await self._render(interaction)

    @discord.ui.button(label="Templates", style=discord.ButtonStyle.secondary, row=1)
    async def templates_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.section = "templates"
        await self._render(interaction)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._render(interaction, note="Admin panel refreshed.")


class VerificationSyncView(discord.ui.View):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.message: discord.Message | None = None
        self.static_embed: discord.Embed | None = None
        self._last_edit_at = 0.0
        self._refresh_buttons()

    def _session(self) -> VerificationSyncSession | None:
        return self.cog.service.get_verification_sync_session(self.guild_id)

    def _disable_all(self):
        for child in self.children:
            child.disabled = True

    def _refresh_buttons(self):
        session = self._session()
        running = session is not None and session.running
        stopping = running and session.stop_requested
        finished = self.static_embed is not None
        self.start_button.disabled = running or finished
        self.refresh_button.disabled = finished
        self.stop_button.label = "Stopping" if stopping else ("Stop" if running else "Cancel")
        self.stop_button.disabled = finished or stopping

    async def current_embed(self) -> discord.Embed:
        if self.static_embed is not None:
            return self.static_embed
        guild = self.cog._guild(self.guild_id)
        if guild is None:
            return ge.make_status_embed(
                "Verification Sync Unavailable",
                "This server is no longer available to Babblebox, so the sync panel cannot load.",
                tone="warning",
                footer="Babblebox Admin | Verification cleanup",
            )
        session = self._session()
        if session is not None:
            return self.cog.build_sync_session_embed(guild, session)
        preview = await self.cog.service.build_verification_sync_preview(guild)
        return self.cog.build_sync_preview_embed(guild, preview)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "This Sync Panel Is Locked",
                    "Use `/admin sync` to open your own verification sync panel.",
                    tone="info",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        if not self.cog.user_can_manage_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to run verification sync tools.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        return True

    async def _safe_edit(self, *, force: bool = False):
        if self.message is None:
            return
        now = asyncio.get_running_loop().time()
        if not force and now - self._last_edit_at < 1.5:
            return
        self._last_edit_at = now
        self._refresh_buttons()
        with contextlib.suppress(discord.HTTPException, AttributeError):
            await self.message.edit(embed=await self.current_embed(), view=self)

    async def _handle_progress(self, session: VerificationSyncSession, force: bool):
        await self._safe_edit(force=force)

    async def _run_session(self, guild: discord.Guild, session: VerificationSyncSession):
        summary = await self.cog.service.run_verification_sync_session(
            guild,
            session,
            progress_callback=self._handle_progress,
        )
        self.static_embed = self.cog.build_sync_summary_embed(summary)
        self._disable_all()
        await self._safe_edit(force=True)

    @discord.ui.button(label="Start Sync", style=discord.ButtonStyle.success)
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This verification sync only works inside a server.", ephemeral=True)
            return
        self.message = interaction.message or self.message
        preview = await self.cog.service.build_verification_sync_preview(guild)
        if preview.blocking_prechecks:
            await interaction.response.edit_message(embed=self.cog.build_sync_preview_embed(guild, preview), view=self)
            return
        created, session = await self.cog.service.create_verification_sync_session(
            guild,
            actor_id=interaction.user.id,
            preview=preview,
        )
        self._refresh_buttons()
        await interaction.response.edit_message(embed=self.cog.build_sync_session_embed(guild, session), view=self)
        if created:
            asyncio.create_task(self._run_session(guild, session), name=f"babblebox-admin-sync-{guild.id}")

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.message = interaction.message or self.message
        await interaction.response.edit_message(embed=await self.current_embed(), view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.message = interaction.message or self.message
        session = self._session()
        if session is None:
            self.static_embed = ge.make_status_embed(
                "Verification Sync Cancelled",
                "No verification sync was started, and no member state was changed.",
                tone="info",
                footer="Babblebox Admin | Verification cleanup",
            )
            self._disable_all()
            await interaction.response.edit_message(embed=self.static_embed, view=self)
            return
        await self.cog.service.request_verification_sync_stop(self.guild_id)
        self._refresh_buttons()
        await interaction.response.edit_message(embed=self.cog.build_sync_session_embed(interaction.guild, session), view=self)

class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = AdminService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "admin_service", self.service)
        for record in await self.service.list_review_views():
            message_id = record.get("review_message_id")
            if not isinstance(message_id, int):
                continue
            with contextlib.suppress(Exception):
                self.bot.add_view(
                    FollowupReviewView(
                        guild_id=int(record["guild_id"]),
                        user_id=int(record["user_id"]),
                        version=int(record.get("review_version", 0) or 0),
                    ),
                    message_id=message_id,
                )
        for record in await self.service.list_verification_review_views():
            message_id = record.get("review_message_id")
            if not isinstance(message_id, int):
                continue
            with contextlib.suppress(Exception):
                self.bot.add_view(
                    VerificationDeadlineReviewView(
                        guild_id=int(record["guild_id"]),
                        user_id=int(record["user_id"]),
                        version=int(record.get("review_version", 0) or 0),
                    ),
                    message_id=message_id,
                )

    def cog_unload(self):
        if getattr(self.bot, "admin_service", None) is self.service:
            delattr(self.bot, "admin_service")
        self.bot.loop.create_task(self.service.close())

    def user_can_manage_admin(self, actor: object) -> bool:
        perms = getattr(actor, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    async def _guard(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "These admin systems can only be configured inside a server.", tone="warning", footer="Babblebox Admin"),
                ephemeral=True,
            )
            return False
        if not self.user_can_manage_admin(ctx.author):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure these admin systems.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        if not self.service.storage_ready:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Admin Systems Unavailable", self.service.storage_message(), tone="warning", footer="Babblebox Admin"),
                ephemeral=True,
            )
            return False
        return True

    def _format_mentions(self, ids: list[int], *, kind: str) -> str:
        if not ids:
            return "None"
        prefix = {"user": "<@", "role": "<@&"}[kind]
        rendered = [f"{prefix}{value}>" for value in ids[:6]]
        if len(ids) > 6:
            rendered.append(f"+{len(ids) - 6} more")
        return ", ".join(rendered)

    def _guild(self, guild_id: int) -> discord.Guild | None:
        get_guild = getattr(self.bot, "get_guild", None)
        return get_guild(guild_id) if callable(get_guild) else None

    def _role_mention(self, role_id: int | None) -> str:
        return f"<@&{role_id}>" if role_id else "Not set"

    def _channel_mention(self, channel_id: int | None) -> str:
        return f"<#{channel_id}>" if channel_id else "Not set"

    def _verification_rule_details(self, guild_id: int) -> dict[str, object]:
        config = self.service.get_config(guild_id)
        guild = self._guild(guild_id)
        role = self.service._guild_role(guild, config["verification_role_id"]) if guild is not None else None
        role_text = role.mention if role is not None else self._role_mention(config["verification_role_id"])
        role_name = str(getattr(role, "name", "") or "").strip()
        warning_after = format_duration_brief(config["verification_kick_after_seconds"] - config["verification_warning_lead_seconds"])
        kick_after = format_duration_brief(config["verification_kick_after_seconds"])
        deadline_action_label = VERIFICATION_DEADLINE_ACTION_LABELS[config["verification_deadline_action"]]
        deadline_after = (
            f"sent for moderator review after {kick_after}"
            if config["verification_deadline_action"] == "review"
            else f"kicked after {kick_after}"
        )
        if config["verification_role_id"] is None:
            verified_sentence = "Members cannot be evaluated until a verification role is configured."
            unverified_sentence = "Babblebox will not enforce verification deadlines until the verification role is configured."
            preview_sentence = "Current rule: incomplete, because no verification role is configured yet."
        elif config["verification_logic"] == "must_have_role":
            verified_sentence = f"Members are considered verified only if they HAVE {role_text}."
            unverified_sentence = f"Users WITHOUT {role_text} are treated as unverified."
            preview_sentence = f"Current rule: users who do NOT have {role_text} will be warned after {warning_after} and {deadline_after}."
        else:
            verified_sentence = f"Members are considered verified only if they DO NOT HAVE {role_text}."
            unverified_sentence = f"Users WITH {role_text} are treated as unverified."
            preview_sentence = f"Current rule: users who still have {role_text} will be warned after {warning_after} and {deadline_after}."

        exempt_parts: list[str] = []
        if config["excluded_user_ids"] or config["excluded_role_ids"]:
            exempt_parts.append("listed exclusions")
        if config["verification_exempt_staff"]:
            exempt_parts.append("staff or trusted roles")
        if config["verification_exempt_bots"]:
            exempt_parts.append("bots")
        if exempt_parts:
            exempt_sentence = f"Exempt from warning/kick: {', '.join(exempt_parts)}."
        else:
            exempt_sentence = "Exempt from warning/kick: only members Babblebox cannot safely moderate."

        review_lines: list[str] = []
        role_name_lower = role_name.lower()
        negative_tokens = ("not verified", "unverified", "guest", "pending", "visitor")
        positive_tokens = ("verified", "approved", "member")
        has_negative_name = any(token in role_name_lower for token in negative_tokens)
        has_positive_name = any(token in role_name_lower for token in positive_tokens)
        if role_name and has_negative_name:
            if config["verification_logic"] == "must_have_role":
                review_lines.append(
                    f"Please review carefully: `{role_name}` sounds like an unverified-state role, but users WITHOUT {role_text} are currently targeted."
                )
            else:
                review_lines.append(
                    f"Please review carefully: `{role_name}` sounds like an unverified-state role. Confirm that users WITH {role_text} should be warned and kicked."
                )
        elif role_name and has_positive_name and config["verification_logic"] == "must_not_have_role":
            review_lines.append(
                f"Please review carefully: `{role_name}` sounds like a verified-state role, but users WITH {role_text} are currently treated as unverified."
            )

        return {
            "role_text": role_text,
            "verified_sentence": verified_sentence,
            "unverified_sentence": unverified_sentence,
            "preview_sentence": preview_sentence,
            "deadline_action_label": deadline_action_label,
            "exempt_sentence": exempt_sentence,
            "review_lines": review_lines,
        }

    def _operability_lines(self, guild_id: int) -> list[str]:
        guild = self._guild(guild_id)
        if guild is None:
            return []
        compiled = self.service.get_compiled_config(guild_id)
        config = self.service.get_config(guild_id)
        me = self.service._bot_member(guild)
        if me is None:
            return ["Warning: Babblebox could not resolve its own server member for admin automations."]
        lines: list[str] = []
        seen: set[str] = set()

        def add(line: str):
            if line not in seen:
                seen.add(line)
                lines.append(line)

        if compiled.followup_enabled:
            role = self.service._guild_role(guild, compiled.followup_role_id)
            if compiled.followup_role_id is None:
                add("Warning: Punishment follow-up is enabled but no follow-up role is configured.")
            elif role is None:
                add("Warning: The configured follow-up role no longer exists.")
            else:
                perms = getattr(me, "guild_permissions", None)
                if perms is None or not getattr(perms, "manage_roles", False):
                    add("Warning: Punishment follow-up cannot manage roles because Babblebox is missing Manage Roles.")
                elif getattr(role, "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
                    add(f"Warning: Babblebox cannot manage {role.mention} because it is at or above Babblebox's top role.")
            if compiled.followup_mode == "review" and compiled.admin_log_channel_id is None:
                add("Warning: Review-mode follow-ups need an admin log channel so Babblebox can send review alerts.")

        if compiled.verification_enabled:
            if compiled.verification_role_id is None:
                add("Warning: Verification cleanup is enabled but no verification role is configured.")
            elif self.service._guild_role(guild, compiled.verification_role_id) is None:
                add("Warning: The configured verification role no longer exists.")
            perms = getattr(me, "guild_permissions", None)
            if perms is None or not getattr(perms, "kick_members", False):
                if compiled.verification_deadline_action == "review":
                    add("Warning: Verification review mode can still warn members, but Kick Members is required for the Kick button.")
                else:
                    add("Warning: Verification cleanup cannot kick members because Babblebox is missing Kick Members.")
            if compiled.verification_deadline_action == "review" and compiled.admin_log_channel_id is None:
                add("Warning: Review-mode verification cleanup needs an admin log channel so Babblebox can send Kick, Delay, and Ignore buttons.")
            add("Note: Verification cleanup still cannot kick administrators or members whose top role is at or above Babblebox.")

        if compiled.admin_log_channel_id is not None:
            channel = self.service._guild_channel(guild, compiled.admin_log_channel_id)
            if channel is None:
                add("Warning: The configured admin log channel is missing or inaccessible.")
            else:
                perms = channel.permissions_for(me)
                if not getattr(perms, "view_channel", False):
                    add(f"Warning: Babblebox cannot see {channel.mention}.")
                if not getattr(perms, "send_messages", False):
                    add(f"Warning: Babblebox cannot send admin alerts in {channel.mention}.")
                if not getattr(perms, "embed_links", False):
                    add(f"Warning: Babblebox cannot embed admin alerts in {channel.mention}.")
        if compiled.admin_alert_role_id is not None and not self.service.can_ping_alert_role(guild, compiled):
            add("Warning: Babblebox may not be able to ping the configured admin alert role.")
        return lines

    def _format_precheck_lines(self, checks: tuple[VerificationPrecheck, ...], *, include_notes: bool = True) -> str:
        labels = {
            "blocked": "Blocked",
            "warning": "Warning",
            "note": "Note",
        }
        lines = [
            f"{labels.get(check.severity, 'Note')}: {check.message}"
            for check in checks
            if include_notes or check.severity != "note"
        ]
        return ge.join_limited_lines(lines[:6], limit=1024, empty="No preflight issues detected.")

    def build_sync_preview_embed(self, guild: discord.Guild, preview: VerificationSyncPreview) -> discord.Embed:
        blocked = bool(preview.blocking_prechecks)
        rule = self._verification_rule_details(guild.id)
        embed = discord.Embed(
            title="Verification Sync Review",
            description=(
                "Review this one-time bulk action before Babblebox touches current verification rows or sends any due warning DMs."
                if not blocked
                else "Start is blocked until the blocking items below are fixed."
            ),
            color=ge.EMBED_THEME["warning"] if blocked else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Bulk Action",
            value=(
                "This sync scans the current member list against the active verification rule.\n"
                "It updates compact verification state, clears stale rows, and sends warning DMs that are already due.\n"
                f"{rule['preview_sentence']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Dry Run",
            value=(
                f"Currently **{preview.matched_unverified}** members match this rule.\n"
                f"New rows to track: **{preview.newly_tracked}**\n"
                f"Already tracked: **{preview.already_tracked}**\n"
                f"Rows to clear: **{preview.stale_rows_to_clear}**\n"
                f"Warning DMs due now: **{preview.warnings_due_now}**\n"
                f"Member scan: **{'Exact' if preview.exact_member_scan else 'Cached only'}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="DM Template",
            value=(
                f"Warning DM: **{preview.warning_template_label}**\n"
                f"Snippet: {preview.warning_template_preview}\n"
                "Internal state updates: **compact verification rows only**\n"
                "Stop control: **Babblebox finishes the current member and then halts the remaining batch**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Preflight",
            value=self._format_precheck_lines(preview.prechecks),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | /admin sync")

    def build_sync_session_embed(self, guild: discord.Guild | None, session: VerificationSyncSession) -> discord.Embed:
        stopping = session.stop_requested
        title = "Verification Sync Stopping" if stopping else "Verification Sync Running"
        description = (
            "Stop requested. Babblebox will finish the current member and then halt the remaining batch."
            if stopping
            else "Babblebox is scanning current members, updating compact verification state, and sending warning DMs that are already due."
        )
        embed = discord.Embed(
            title=title,
            description=description,
            color=ge.EMBED_THEME["warning"] if stopping else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Progress",
            value=(
                f"Scanned members: **{session.scanned_members}**\n"
                f"Matched unverified: **{session.matched_unverified}**\n"
                f"Newly tracked: **{session.tracked_count}**\n"
                f"Stale rows cleared: **{session.cleared_count}**\n"
                f"Warnings processed: **{session.warned_count}**\n"
                f"Failed DMs: **{session.failed_dm_count}**\n"
                f"Skipped without change: **{session.skipped_count}**"
            ),
            inline=False,
        )
        current_member = f"<@{session.current_member_id}>" if session.current_member_id is not None else "Between members"
        state_lines = [
            f"Started: {ge.format_timestamp(session.created_at, 'R')}",
            f"Current member: {current_member}",
            f"Stop requested: {'Yes' if session.stop_requested else 'No'}",
        ]
        if session.preview.warnings_due_now:
            state_lines.append(f"Warnings due at start: **{session.preview.warnings_due_now}**")
        embed.add_field(name="State", value="\n".join(state_lines), inline=False)
        preflight = tuple(check for check in session.preview.prechecks if check.severity != "note")
        if preflight:
            embed.add_field(name="Preflight", value=self._format_precheck_lines(preflight, include_notes=False), inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | /admin sync")

    def build_sync_summary_embed(self, summary: VerificationSyncSummary) -> discord.Embed:
        return self.service.build_verification_sync_summary_embed(summary)

    async def _overview_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        counts = await self.service.get_counts(guild_id)
        verification_rule = self._verification_rule_details(guild_id)
        embed = discord.Embed(
            title="Admin Systems Overview",
            description="Compact server-lifecycle helpers for returned-after-ban follow-up roles and long-unverified cleanup.",
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Punishment Follow-up",
            value=(
                f"Enabled: **{'Yes' if config['followup_enabled'] else 'No'}**\n"
                f"Role: {self._role_mention(config['followup_role_id'])}\n"
                f"Mode: {FOLLOWUP_MODE_LABELS[config['followup_mode']]}\n"
                f"Duration: {_followup_duration_label(config['followup_duration_value'], config['followup_duration_unit'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Verification Cleanup",
            value=(
                f"Enabled: **{'Yes' if config['verification_enabled'] else 'No'}**\n"
                f"Rule: {VERIFICATION_LOGIC_LABELS[config['verification_logic']]}\n"
                f"Deadline action: **{verification_rule['deadline_action_label']}**\n"
                f"{verification_rule['unverified_sentence']}\n"
                f"Deadline timer: {format_duration_brief(config['verification_kick_after_seconds'])}\n"
                f"Warn lead: {format_duration_brief(config['verification_warning_lead_seconds'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Live Counts",
            value=(
                f"Pending ban-return candidates: **{counts['ban_candidates']}**\n"
                f"Active follow-up roles: **{counts['active_followups']}**\n"
                f"Pending follow-up reviews: **{counts['pending_reviews']}**\n"
                f"Tracked unverified members: **{counts['verification_pending']}**\n"
                f"Warned and waiting: **{counts['verification_warned']}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Logs",
            value=(
                f"Channel: {self._channel_mention(config['admin_log_channel_id'])}\n"
                f"Alert role: {self._role_mention(config['admin_alert_role_id'])}"
            ),
            inline=False,
        )
        return embed

    async def _followup_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Punishment Follow-up",
            description="When someone returns within 30 days of a ban event, Babblebox can apply one configured follow-up role.",
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(
            name="Policy",
            value=(
                f"Enabled: **{'Yes' if config['followup_enabled'] else 'No'}**\n"
                f"Role: {self._role_mention(config['followup_role_id'])}\n"
                f"Return window: **30 days after a ban event**\n"
                f"Mode: {FOLLOWUP_MODE_LABELS[config['followup_mode']]}\n"
                f"Duration: {_followup_duration_label(config['followup_duration_value'], config['followup_duration_unit'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Behavior",
            value=(
                "Babblebox does not know the original ban length.\n"
                "It only reacts when a member returns within 30 days of a ban event.\n"
                "Auto-remove removes the role on expiry.\n"
                "Review mode sends moderator buttons to the admin log channel."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/admin followup enabled:true role:@Probation mode:review duration:30d`",
            inline=False,
        )
        return embed

    async def _verification_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        rule = self._verification_rule_details(guild_id)
        embed = discord.Embed(
            title="Verification Cleanup",
            description="Warn members who stay unverified too long, then either kick automatically or send a moderator review with a verification-help extension path.",
            color=ge.EMBED_THEME["danger"],
        )
        embed.add_field(
            name="Current Rule",
            value=(
                f"Enabled: **{'Yes' if config['verification_enabled'] else 'No'}**\n"
                f"Role: {self._role_mention(config['verification_role_id'])}\n"
                f"Logic label: {VERIFICATION_LOGIC_LABELS[config['verification_logic']]}\n"
                f"Deadline action: **{rule['deadline_action_label']}**\n"
                f"{rule['verified_sentence']}\n"
                f"{rule['unverified_sentence']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Deadline Path",
            value=(
                f"{rule['preview_sentence']}\n"
                f"{rule['exempt_sentence']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Help Channel Path",
            value=(
                f"Help channel: {self._channel_mention(config['verification_help_channel_id'])}\n"
                f"Extension: {format_duration_brief(config['verification_help_extension_seconds'])}\n"
                f"Max extensions: **{config['verification_max_extensions']}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Behavior",
            value=(
                "Babblebox tracks compact pending member state only.\n"
                "Any non-trivial message in the verification-help channel can extend the deadline.\n"
                "If someone is already deep into the timer when tracking starts, Babblebox gives them a fresh warning window instead of enforcing the deadline instantly.\n"
                "Review mode sends one mod-facing message with Kick, Delay, and Ignore buttons."
            ),
            inline=False,
        )
        if rule["review_lines"]:
            embed.add_field(name="Please Review Carefully", value="\n".join(rule["review_lines"]), inline=False)
        embed.add_field(
            name="Examples",
            value=(
                "`@Verified + must_have_role` -> users WITHOUT `@Verified` follow the configured deadline action.\n"
                "`@Not Verified + must_not_have_role` -> users WITH `@Not Verified` follow the configured deadline action."
            ),
            inline=False,
        )
        return embed

    async def _exclusions_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Exclusions And Trusted Roles",
            description="Shared exclusions keep these automations compact and predictable instead of layering many per-feature lists.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Shared Buckets",
            value=(
                f"Excluded members: {self._format_mentions(config['excluded_user_ids'], kind='user')}\n"
                f"Excluded roles: {self._format_mentions(config['excluded_role_ids'], kind='role')}\n"
                f"Trusted roles: {self._format_mentions(config['trusted_role_ids'], kind='role')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Toggles",
            value=(
                f"Follow-up exempts staff/trusted: **{'Yes' if config['followup_exempt_staff'] else 'No'}**\n"
                f"Verification exempts staff/trusted: **{'Yes' if config['verification_exempt_staff'] else 'No'}**\n"
                f"Verification exempts bots: **{'Yes' if config['verification_exempt_bots'] else 'No'}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/admin exclusions target:trusted_role_ids state:on role:@Mods`\n`/admin exclusions verification_exempt_bots:false`",
            inline=False,
        )
        return embed

    async def _logs_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Logs And Alerts",
            description="Both admin systems share one compact mod-facing log path instead of keeping a big internal archive.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Channel: {self._channel_mention(config['admin_log_channel_id'])}\n"
                f"Alert role: {self._role_mention(config['admin_alert_role_id'])}\n"
                "Logs cover follow-up role assignments, review deadlines, verification warnings, kicks, help extensions, and clear operability failures."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/admin logs channel:#admin-log role:@Mods`",
            inline=False,
        )
        return embed

    async def _templates_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Templates And Messages",
            description="Verification DMs stay configurable, but Babblebox only supports a small safe placeholder set.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Current Settings",
            value=(
                f"Warning template: {'Custom' if config['warning_template'] else 'Default'}\n"
                f"Kick template: {'Custom' if config['kick_template'] else 'Default'}\n"
                f"Invite link: {config['invite_link'] or 'None'}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Placeholders",
            value="`{guild}`  `{deadline}`  `{deadline_relative}`  `{help_channel}`  `{invite_link}`",
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/admin templates invite_link:https://discord.gg/example`",
            inline=False,
        )
        return embed

    async def build_panel_embed(self, guild_id: int, section: str) -> discord.Embed:
        if section == "followup":
            embed = await self._followup_embed(guild_id)
        elif section == "verification":
            embed = await self._verification_embed(guild_id)
        elif section == "exclusions":
            embed = await self._exclusions_embed(guild_id)
        elif section == "logs":
            embed = await self._logs_embed(guild_id)
        elif section == "templates":
            embed = await self._templates_embed(guild_id)
        else:
            embed = await self._overview_embed(guild_id)
        operability = self._operability_lines(guild_id)
        if operability:
            embed.add_field(name="Operability", value="\n".join(operability[:6]), inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | /admin panel, status, followup, verification, logs, exclusions, templates, sync, or test")

    async def _send_result(self, ctx: commands.Context, title: str, message: str, *, ok: bool):
        embed = ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Admin")
        operability = self._operability_lines(ctx.guild.id)
        if operability:
            embed.add_field(name="Operability", value="\n".join(operability[:6]), inline=False)
        await send_hybrid_response(ctx, embed=embed, ephemeral=True)

    async def _send_panel(self, ctx: commands.Context, *, section: str = "overview"):
        view = AdminPanelView(self, guild_id=ctx.guild.id, author_id=ctx.author.id, section=section)
        message = await send_hybrid_response(ctx, embed=await view.current_embed(), view=view, ephemeral=True)
        if message is not None:
            view.message = message

    async def _send_sync_panel(self, ctx: commands.Context):
        view = VerificationSyncView(self, guild_id=ctx.guild.id, author_id=ctx.author.id)
        message = await send_hybrid_response(ctx, embed=await view.current_embed(), view=view, ephemeral=True)
        if message is not None:
            view.message = message

    async def _member_status_embed(self, member: discord.Member) -> discord.Embed:
        status = await self.service.get_member_status(member)
        rule = self._verification_rule_details(member.guild.id)
        followup_role_text = f"<@&{status['followup']['role_id']}>" if status["followup"] else "None"
        embed = discord.Embed(
            title="Admin Member Status",
            description=f"Current automation state for {member.mention}.",
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Follow-up",
            value=(
                f"Exempt: {status['followup_exempt_reason'] or 'No'}\n"
                f"Ban-return candidate: {'Yes' if status['candidate'] else 'No'}\n"
                f"Active follow-up role: {followup_role_text}"
            ),
            inline=False,
        )
        verification_state = status["verification"]
        verification_lines = [
            f"{rule['preview_sentence']}",
            f"This member currently counts as: {status['verified_state'].title()}",
            f"Why: {status['verified_reason']}",
        ]
        exempt_reason = status.get("verification_exempt_reason")
        if exempt_reason:
            verification_lines.append(f"Exempt: {exempt_reason}")
        if verification_state is None:
            verification_lines.append("Tracked deadline: None")
        else:
            verification_lines.extend(
                [
                    f"Warn at: {ge.format_timestamp(deserialize_datetime(verification_state.get('warning_at')), 'R')}",
                    f"Kick at: {ge.format_timestamp(deserialize_datetime(verification_state.get('kick_at')), 'R')}",
                    f"Extensions used: {verification_state.get('extension_count', 0)}",
                ]
            )
        embed.add_field(name="Verification", value="\n".join(verification_lines), inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Member automation status")

    def _copy_embed(self, embed: discord.Embed) -> discord.Embed:
        return discord.Embed.from_dict(embed.to_dict())

    def _placeholder_lines(self, placeholders: dict[str, str]) -> str:
        labels = {
            "{member}": "Member",
            "{guild}": "Guild",
            "{deadline}": "Deadline",
            "{deadline_relative}": "Deadline (relative)",
            "{help_channel}": "Help channel",
            "{invite_link}": "Invite link",
        }
        return ge.join_limited_lines(
            [f"{labels.get(name, name)}: {value}" for name, value in placeholders.items()],
            limit=1024,
            empty="No placeholders were rendered.",
        )

    @commands.hybrid_group(
        name="admin",
        with_app_command=True,
        description="Configure Babblebox admin lifecycle automations",
        invoke_without_command=True,
    )
    async def admin_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @admin_group.command(name="status", with_app_command=True, description="View the admin overview or inspect one member's state")
    async def admin_status_command(self, ctx: commands.Context, member: Optional[discord.Member] = None):
        if not await self._guard(ctx):
            return
        if member is None:
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "overview"), ephemeral=True)
            return
        await send_hybrid_response(ctx, embed=await self._member_status_embed(member), ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="panel", with_app_command=True, description="Open the private admin panel")
    async def admin_panel_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="followup", with_app_command=True, description="Configure returned-after-ban follow-up roles")
    @app_commands.describe(
        enabled="Turn punishment follow-up on or off",
        role="Role to assign when someone returns within 30 days of a ban event",
        mode="Remove automatically later or send a moderator review alert",
        duration="How long the follow-up role should stay, like 30d, 6w, or 3mo",
        clear_role="Clear the configured follow-up role",
    )
    @app_commands.choices(mode=FOLLOWUP_MODE_CHOICES)
    async def admin_followup_command(
        self,
        ctx: commands.Context,
        enabled: Optional[bool] = None,
        role: Optional[discord.Role] = None,
        mode: Optional[str] = None,
        duration: Optional[str] = None,
        clear_role: bool = False,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (enabled, role, mode, duration)) and not clear_role:
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "followup"), ephemeral=True)
            return
        current = self.service.get_config(ctx.guild.id)
        resolved_role_id = None if clear_role else (role.id if role is not None else current["followup_role_id"])
        ok, message = await self.service.set_followup_config(
            ctx.guild.id,
            enabled=enabled,
            role_id=resolved_role_id,
            mode=mode,
            duration_text=duration,
        )
        await self._send_result(ctx, "Punishment Follow-up", message, ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="verification", with_app_command=True, description="Configure verification retention and cleanup")
    @app_commands.describe(
        enabled="Turn verification cleanup on or off",
        role="Verification role to evaluate",
        logic="Choose whether users missing this role or users still holding this role are treated as unverified",
        deadline_action="Kick automatically at the deadline or send a moderator review message instead",
        kick_after="Full time before kick, like 7d",
        warning_lead="How long before the kick Babblebox should warn, like 2d",
        help_channel="Verification-help channel where messages can extend the deadline",
        help_extension="How much time a help message adds, like 2d",
        max_extensions="How many help-channel extensions each member can use",
        clear_role="Clear the configured verification role",
        clear_help_channel="Clear the verification-help channel",
    )
    @app_commands.choices(logic=VERIFICATION_LOGIC_CHOICES, deadline_action=VERIFICATION_DEADLINE_ACTION_CHOICES)
    async def admin_verification_command(
        self,
        ctx: commands.Context,
        enabled: Optional[bool] = None,
        role: Optional[discord.Role] = None,
        logic: Optional[str] = None,
        deadline_action: Optional[str] = None,
        kick_after: Optional[str] = None,
        warning_lead: Optional[str] = None,
        help_channel: Optional[discord.TextChannel] = None,
        help_extension: Optional[str] = None,
        max_extensions: Optional[int] = None,
        clear_role: bool = False,
        clear_help_channel: bool = False,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (enabled, role, logic, deadline_action, kick_after, warning_lead, help_channel, help_extension, max_extensions)) and not clear_role and not clear_help_channel:
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "verification"), ephemeral=True)
            return
        current = self.service.get_config(ctx.guild.id)
        resolved_role_id = None if clear_role else (role.id if role is not None else current["verification_role_id"])
        resolved_help_channel_id = None if clear_help_channel else (help_channel.id if help_channel is not None else current["verification_help_channel_id"])
        ok, message = await self.service.set_verification_config(
            ctx.guild.id,
            enabled=enabled,
            role_id=resolved_role_id,
            logic=logic,
            deadline_action=deadline_action,
            kick_after_text=kick_after,
            warning_lead_text=warning_lead,
            help_channel_id=resolved_help_channel_id,
            help_extension_text=help_extension,
            max_extensions=max_extensions,
        )
        await self._send_result(ctx, "Verification Cleanup", message, ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="logs", with_app_command=True, description="Configure admin log delivery and alert pings")
    async def admin_logs_command(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
        clear_channel: bool = False,
        clear_role: bool = False,
    ):
        if not await self._guard(ctx):
            return
        if channel is None and role is None and not clear_channel and not clear_role:
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "logs"), ephemeral=True)
            return
        current = self.service.get_config(ctx.guild.id)
        resolved_channel_id = None if clear_channel else (channel.id if channel is not None else current["admin_log_channel_id"])
        resolved_role_id = None if clear_role else (role.id if role is not None else current["admin_alert_role_id"])
        ok, message = await self.service.set_logs_config(ctx.guild.id, channel_id=resolved_channel_id, alert_role_id=resolved_role_id)
        await self._send_result(ctx, "Admin Logs", message, ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="exclusions", with_app_command=True, description="Configure shared exclusions and trusted-role behavior")
    @app_commands.choices(target=EXCLUSION_TARGET_CHOICES, state=STATE_CHOICES)
    async def admin_exclusions_command(
        self,
        ctx: commands.Context,
        target: Optional[str] = None,
        state: Optional[str] = None,
        member: Optional[discord.Member] = None,
        role: Optional[discord.Role] = None,
        followup_exempt_staff: Optional[bool] = None,
        verification_exempt_staff: Optional[bool] = None,
        verification_exempt_bots: Optional[bool] = None,
    ):
        if not await self._guard(ctx):
            return
        if (
            target is None
            and state is None
            and followup_exempt_staff is None
            and verification_exempt_staff is None
            and verification_exempt_bots is None
        ):
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "exclusions"), ephemeral=True)
            return
        messages: list[str] = []
        ok = True
        if target is not None or state is not None:
            if target is None or state is None:
                ok = False
                messages.append("Choose both a target bucket and on/off state when updating exclusions.")
            else:
                if target == "excluded_user_ids":
                    target_id = getattr(member, "id", None)
                    if not isinstance(target_id, int):
                        ok = False
                        messages.append("Select a member for member exclusions.")
                    else:
                        part_ok, part_message = await self.service.set_exclusion_target(ctx.guild.id, target, target_id, state == "on")
                        ok = ok and part_ok
                        messages.append(part_message)
                else:
                    target_id = getattr(role, "id", None)
                    if not isinstance(target_id, int):
                        ok = False
                        messages.append("Select a role for role exclusions or trusted roles.")
                    else:
                        part_ok, part_message = await self.service.set_exclusion_target(ctx.guild.id, target, target_id, state == "on")
                        ok = ok and part_ok
                        messages.append(part_message)
        for field, value in (
            ("followup_exempt_staff", followup_exempt_staff),
            ("verification_exempt_staff", verification_exempt_staff),
            ("verification_exempt_bots", verification_exempt_bots),
        ):
            if value is None:
                continue
            part_ok, part_message = await self.service.set_exemption_toggle(ctx.guild.id, field, value)
            ok = ok and part_ok
            messages.append(part_message)
        await self._send_result(ctx, "Admin Exclusions", "\n".join(messages), ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="templates", with_app_command=True, description="Configure verification DM templates and invite link")
    async def admin_templates_command(
        self,
        ctx: commands.Context,
        warning_template: Optional[str] = None,
        kick_template: Optional[str] = None,
        invite_link: Optional[str] = None,
        clear_warning: bool = False,
        clear_kick: bool = False,
        clear_invite: bool = False,
    ):
        if not await self._guard(ctx):
            return
        if warning_template is None and kick_template is None and invite_link is None and not clear_warning and not clear_kick and not clear_invite:
            await send_hybrid_response(ctx, embed=await self.build_panel_embed(ctx.guild.id, "templates"), ephemeral=True)
            return
        ok, message = await self.service.set_templates(
            ctx.guild.id,
            warning_template=None if clear_warning else (warning_template if warning_template is not None else ...),
            kick_template=None if clear_kick else (kick_template if kick_template is not None else ...),
            invite_link=None if clear_invite else (invite_link if invite_link is not None else ...),
        )
        await self._send_result(ctx, "Admin Templates", message, ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="test", with_app_command=True, description="Safely preview verification templates and log delivery")
    @app_commands.describe(
        kind="Choose whether to preview the warning DM, final kick DM, or logs channel output",
        member="Optional member to use for placeholder rendering in the preview",
        dm_self="DM the selected preview to you as a self-test",
        post_log="For log tests, also post the test embed to the configured admin log channel",
    )
    @app_commands.choices(kind=ADMIN_TEST_CHOICES)
    async def admin_test_command(
        self,
        ctx: commands.Context,
        kind: Optional[str] = None,
        member: Optional[discord.Member] = None,
        dm_self: bool = False,
        post_log: bool = False,
    ):
        if not await self._guard(ctx):
            return
        if kind is None:
            embed = discord.Embed(
                title="Verification Test Tools",
                description="Safely preview verification DMs and logs without changing any member state.",
                color=ge.EMBED_THEME["info"],
            )
            embed.add_field(
                name="Available Tests",
                value=(
                    "`warning_dm` previews the warning template.\n"
                    "`kick_dm` previews the final removal template.\n"
                    "`logs` previews or posts a safe log message to the configured admin log channel."
                ),
                inline=False,
            )
            embed.add_field(
                name="Safety",
                value="These tests do not mark members as warned, do not start a sync, and do not kick or mass-DM anyone.",
                inline=False,
            )
            await send_hybrid_response(ctx, embed=ge.style_embed(embed, footer="Babblebox Admin | /admin test"), ephemeral=True)
            return

        compiled = self.service.get_compiled_config(ctx.guild.id)
        target = member or ctx.author
        help_channel = self.service._guild_channel(ctx.guild, compiled.verification_help_channel_id)
        preview_deadline = ge.now_utc() + timedelta(seconds=max(compiled.verification_warning_lead_seconds, 3600))
        log_result = "Preview only"
        dm_result = "Not requested"
        checks = self.service.get_verification_prechecks(
            ctx.guild,
            blocked_kick_matches=0,
            exact_member_scan=bool(getattr(ctx.guild, "chunked", True)),
        )

        if kind == "logs":
            preview_embed = ge.make_status_embed(
                "Verification Log Test",
                f"This is a safe verification log test requested by {ctx.author.mention}. No member state was changed.",
                tone="info",
                footer="Babblebox Admin | Verification cleanup test",
            )
            preview_embed.add_field(
                name="Current Templates",
                value=(
                    f"Warning template: **{'Custom' if compiled.warning_template else 'Default'}**\n"
                    f"Final kick template: **{'Custom' if compiled.kick_template else 'Default'}**\n"
                    f"Log channel: {self._channel_mention(compiled.admin_log_channel_id)}"
                ),
                inline=False,
            )
            if post_log:
                sent = await self.service.send_log(ctx.guild, compiled, embed=self._copy_embed(preview_embed), alert=False)
                log_result = (
                    f"Posted to {self._channel_mention(compiled.admin_log_channel_id)}."
                    if sent
                    else "Could not post to the configured admin log channel."
                )
            elif dm_self:
                dm_result = "DM self is only used for warning and final kick previews."
            preview_embed.add_field(
                name="Delivery",
                value=(
                    f"Private preview: **Shown here**\n"
                    f"Log channel test: **{log_result}**\n"
                    "Member side effects: **None**"
                ),
                inline=False,
            )
            preview_embed.add_field(name="Prechecks", value=self._format_precheck_lines(checks), inline=False)
            await send_hybrid_response(ctx, embed=preview_embed, ephemeral=True)
            return

        final = kind == "kick_dm"
        preview_embed = (
            self.service.build_kick_embed(target, guild=ctx.guild, deadline=preview_deadline, compiled=compiled)
            if final
            else self.service.build_warning_embed(target, guild=ctx.guild, deadline=preview_deadline, compiled=compiled)
        )
        placeholders = self.service.verification_template_placeholders(
            target,
            guild=ctx.guild,
            deadline=preview_deadline,
            help_channel=help_channel,
            invite_link=compiled.invite_link,
            preview=True,
        )
        if dm_self:
            if hasattr(ctx.author, "send"):
                try:
                    await ctx.author.send(embed=self._copy_embed(preview_embed))
                    dm_result = "Sent to your DMs"
                except (discord.Forbidden, discord.HTTPException):
                    dm_result = "DM failed"
            else:
                dm_result = "DM route unavailable for this test context"
        preview_embed = self._copy_embed(preview_embed)
        preview_embed.add_field(
            name="Test Mode",
            value=(
                f"Template: **{'Final kick DM' if final else 'Warning DM'}**\n"
                f"Rendered member: {getattr(target, 'mention', ge.display_name_of(target))}\n"
                "Member side effects: **None**"
            ),
            inline=False,
        )
        preview_embed.add_field(name="Resolved Placeholders", value=self._placeholder_lines(placeholders), inline=False)
        preview_embed.add_field(
            name="Delivery",
            value=(
                "Private preview: **Shown here**\n"
                f"DM self-test: **{dm_result}**\n"
                "Bulk sends started: **No**"
            ),
            inline=False,
        )
        preview_embed.add_field(name="Prechecks", value=self._format_precheck_lines(checks), inline=False)
        await send_hybrid_response(ctx, embed=preview_embed, ephemeral=True)

    @app_commands.default_permissions(manage_guild=True)
    @admin_group.command(name="sync", with_app_command=True, description="Review, preview, and safely run a verification catch-up sync")
    async def admin_sync_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        if not getattr(ctx.guild, "chunked", True) and hasattr(ctx.guild, "chunk"):
            with contextlib.suppress(discord.HTTPException):
                await ctx.guild.chunk(cache=True)
        await self._send_sync_panel(ctx)

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.abc.User):
        await self.service.handle_member_ban(guild, user)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.service.handle_member_join(member)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await self.service.handle_member_remove(member)

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        await self.service.handle_member_update(before, after)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is None or message.author.bot or message.webhook_id is not None:
            return
        from babblebox.command_utils import is_command_message

        if await is_command_message(self.bot, message):
            return
        await self.service.handle_message(message)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
