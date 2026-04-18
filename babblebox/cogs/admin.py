from __future__ import annotations

import asyncio
import contextlib
from datetime import timedelta
from typing import Any, Awaitable, Callable, Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox.app_command_hardening import harden_admin_root_group, harden_lock_root_group, harden_timeout_root_group
from babblebox import game_engine as ge
from babblebox.admin_service import (
    FOLLOWUP_MODE_LABELS,
    CONFIG_UNCHANGED,
    LOCK_MODERATOR_PERMISSION_NAMES,
    REVIEW_ACTION_LABELS,
    AdminService,
    _followup_duration_label,
)
from babblebox.command_utils import (
    HybridPanelSendResult,
    defer_hybrid_response,
    send_hybrid_panel_response,
)
from babblebox.admin_panel_views import AdminPanelView
from babblebox.utility_helpers import deserialize_datetime, format_duration_brief, parse_duration_string


FOLLOWUP_MODE_CHOICES = [
    app_commands.Choice(name="Auto remove", value="auto_remove"),
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

PERMISSION_DIAGNOSTIC_RULES: tuple[tuple[str, str, str], ...] = (
    (
        "manage_channels",
        "Manage Channels",
        "Emergency locks cannot change the `@everyone` overwrite, so `/lock channel` and `/lock remove` will fail.",
    ),
    (
        "manage_roles",
        "Manage Roles",
        "Returned-after-ban follow-up roles and Question Drops role grants or removals cannot manage roles.",
    ),
    (
        "moderate_members",
        "Moderate Members / Timeout Members",
        "Shield timeout actions and `/timeout remove` cannot run.",
    ),
    (
        "manage_messages",
        "Manage Messages",
        "Shield delete and delete + timeout actions cannot remove matched messages.",
    ),
    (
        "view_channel",
        "View Channels",
        "Babblebox may miss help cards, log channels, lock notices, or moderation surfaces in channels it cannot see.",
    ),
    (
        "send_messages",
        "Send Messages",
        "Babblebox cannot post lock notices, moderation logs, or status cards where sending is blocked.",
    ),
    (
        "embed_links",
        "Embed Links",
        "Rich help, support, Shield, and admin embeds may fail or fall back.",
    ),
)

def _preset_select_options(
    presets: tuple[tuple[str, str], ...],
    *,
    current_label: str,
    current_value: str,
) -> list[discord.SelectOption]:
    options = [discord.SelectOption(label=label, value=value, default=value == current_value) for value, label in presets]
    if any(option.default for option in options):
        return options
    return [discord.SelectOption(label=f"Current: {current_label}", value="__current__", default=True)] + options


def _match_duration_preset_value(seconds: int, presets: tuple[tuple[str, str], ...]) -> str:
    for value, _label in presets:
        if parse_duration_string(value) == seconds:
            return value
    return "__current__"


def _best_duration_input(seconds: int) -> str:
    if seconds % (24 * 3600) == 0:
        return f"{seconds // (24 * 3600)}d"
    if seconds % 3600 == 0:
        return f"{seconds // 3600}h"
    return f"{max(seconds // 60, 1)}m"


def _followup_duration_input(value: int, unit: str) -> str:
    suffix = {"days": "d", "weeks": "w", "months": "mo"}.get(unit, "d")
    return f"{value}{suffix}"


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


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = AdminService(bot)
        harden_admin_root_group(self.admin_group)
        harden_lock_root_group(self.lock_group)
        harden_timeout_root_group(self.timeout_group)

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

    def cog_unload(self):
        if getattr(self.bot, "admin_service", None) is self.service:
            delattr(self.bot, "admin_service")
        self.bot.loop.create_task(self.service.close())

    def user_can_manage_admin(self, actor: object) -> bool:
        perms = getattr(actor, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    def _user_has_lock_moderator_permission(self, actor: object) -> bool:
        perms = getattr(actor, "guild_permissions", None)
        return any(bool(getattr(perms, permission_name, False)) for permission_name in LOCK_MODERATOR_PERMISSION_NAMES)

    def user_can_manage_lock(self, actor: object, guild_id: int) -> bool:
        if self.user_can_manage_admin(actor):
            return True
        if self.service.get_compiled_config(guild_id).lock_admin_only:
            return False
        return self._user_has_lock_moderator_permission(actor)

    def user_can_manage_timeout(self, actor: object) -> bool:
        if self.user_can_manage_admin(actor):
            return True
        perms = getattr(actor, "guild_permissions", None)
        return bool(getattr(perms, "moderate_members", False))

    async def _guard(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "These admin systems can only be configured inside a server.", tone="warning", footer="Babblebox Admin"),
                recovery_title="Admin Systems Unavailable",
                recovery_description="Babblebox could not complete the admin permission check just now. Please try again inside a server.",
            )
            return False
        if not self.user_can_manage_admin(ctx.author):
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure these admin systems.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                recovery_title="Admin Access Check Unavailable",
                recovery_description="Babblebox could not finish the admin access check just now. Please try the command again in a moment.",
            )
            return False
        if not self.service.storage_ready:
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed("Admin Systems Unavailable", self.service.storage_message(), tone="warning", footer="Babblebox Admin"),
                recovery_title="Admin Systems Unavailable",
                recovery_description="Babblebox could not complete the admin storage check just now. Please try again in a moment.",
            )
            return False
        return True

    async def _lock_guard(self, ctx: commands.Context, *, settings: bool = False) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Emergency lock tools only work inside a server.",
                    tone="warning",
                    footer="Babblebox Lock",
                ),
                recovery_title="Emergency Lock Unavailable",
                recovery_description="Babblebox could not complete the emergency lock permission check just now. Please try again inside a server.",
                recovery_footer="Babblebox Lock",
            )
            return False
        if not self.service.storage_ready:
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed(
                    "Emergency Lock Unavailable",
                    self.service.storage_message("Emergency lock tools"),
                    tone="warning",
                    footer="Babblebox Lock",
                ),
                recovery_title="Emergency Lock Unavailable",
                recovery_description="Babblebox could not complete the emergency lock storage check just now. Please try again in a moment.",
                recovery_footer="Babblebox Lock",
            )
            return False
        if settings:
            if self.user_can_manage_admin(ctx.author):
                return True
            description = "You need **Manage Server** or administrator access to change emergency lock settings."
            title = "Admin Only"
        else:
            if self.user_can_manage_lock(ctx.author, ctx.guild.id):
                return True
            if self.service.get_compiled_config(ctx.guild.id).lock_admin_only:
                description = "This server has limited emergency locks to **Manage Server** or administrator users."
            else:
                description = (
                    "You need a moderator permission such as **Manage Channels**, **Manage Messages**, "
                    "**Timeout Members**, **Kick Members**, or **Ban Members** to use emergency locks in this server."
                )
            title = "Lock Access Denied"
        await self._send_admin_response(
            ctx,
            embed=ge.make_status_embed(title, description, tone="warning", footer="Babblebox Lock"),
            recovery_title="Emergency Lock Access Check Unavailable",
            recovery_description="Babblebox could not finish the emergency lock access check just now. Please try again in a moment.",
            recovery_footer="Babblebox Lock",
        )
        return False

    async def _timeout_guard(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await self._send_admin_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Timeout removal tools only work inside a server.",
                    tone="warning",
                    footer="Babblebox Timeout",
                ),
                recovery_title="Timeout Removal Unavailable",
                recovery_description="Babblebox could not complete the timeout removal permission check just now. Please try again inside a server.",
                recovery_footer="Babblebox Timeout",
            )
            return False
        if self.user_can_manage_timeout(ctx.author):
            return True
        await self._send_admin_response(
            ctx,
            embed=ge.make_status_embed(
                "Timeout Access Denied",
                "You need **Timeout Members**, **Manage Server**, or administrator access to remove timeouts in this server.",
                tone="warning",
                footer="Babblebox Timeout",
            ),
            recovery_title="Timeout Access Check Unavailable",
            recovery_description="Babblebox could not finish the timeout access check just now. Please try again in a moment.",
            recovery_footer="Babblebox Timeout",
        )
        return False

    def _lock_target_channel(self, ctx: commands.Context, channel: discord.TextChannel | None):
        return channel or ctx.channel

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

    def _operability_lines(self, guild_id: int) -> list[str]:
        guild = self._guild(guild_id)
        if guild is None:
            return []
        me = self.service._bot_member(guild)
        if me is None:
            return ["Babblebox could not resolve its own server member for operability checks."]

        compiled = self.service.get_compiled_config(guild_id)
        lines: list[str] = []

        if compiled.followup_enabled:
            if compiled.followup_role_id is None:
                lines.append("Follow-up is enabled but no follow-up role is configured.")
            else:
                role = self.service._guild_role(guild, compiled.followup_role_id)
                if role is None:
                    lines.append("The configured follow-up role is missing or no longer accessible.")
                else:
                    perms = getattr(me, "guild_permissions", None)
                    if perms is None or not getattr(perms, "manage_roles", False):
                        lines.append("Manage Roles is missing, so follow-up role assignment will fail.")
                    elif getattr(role, "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
                        lines.append(f"{role.mention} is at or above Babblebox's top role.")

        if compiled.admin_log_channel_id is not None:
            channel = self.service._guild_channel(guild, compiled.admin_log_channel_id)
            if channel is None:
                lines.append("Babblebox cannot see the configured admin log channel.")
            else:
                perms = channel.permissions_for(me)
                if not getattr(perms, "view_channel", False):
                    lines.append("Babblebox cannot see the configured admin log channel.")
                if not getattr(perms, "send_messages", False):
                    lines.append("Babblebox cannot send messages in the configured admin log channel.")
                if not getattr(perms, "embed_links", False):
                    lines.append("Babblebox cannot embed messages in the configured admin log channel.")

        if compiled.admin_alert_role_id is not None:
            role = self.service._guild_role(guild, compiled.admin_alert_role_id)
            if role is None:
                lines.append("The configured admin alert role is missing or no longer accessible.")
            elif not self.service.can_ping_alert_role(guild, compiled):
                lines.append("Babblebox cannot ping the configured admin alert role with current permissions.")

        return lines

    def _permission_diagnostic_rows(self, guild_id: int) -> list[tuple[str, str]]:
        guild = self._guild(guild_id)
        if guild is None:
            return []
        me = self.service._bot_member(guild)
        perms = getattr(me, "guild_permissions", None)
        rows: list[tuple[str, str]] = []
        for permission_name, label, impact in PERMISSION_DIAGNOSTIC_RULES:
            if perms is not None and getattr(perms, permission_name, False):
                continue
            rows.append((label, impact))
        return rows

    async def _permission_diagnostics_embed(self, guild_id: int) -> discord.Embed:
        guild = self._guild(guild_id)
        if guild is None:
            return ge.make_status_embed(
                "Permission Diagnostics Unavailable",
                "Babblebox could not resolve that server, so permission diagnostics are unavailable right now.",
                tone="warning",
                footer="Babblebox Admin",
            )
        me = self.service._bot_member(guild)
        if me is None:
            return ge.make_status_embed(
                "Permission Diagnostics Unavailable",
                "Babblebox could not resolve its own server member, so permission diagnostics are unavailable right now.",
                tone="warning",
                footer="Babblebox Admin",
            )
        rows = self._permission_diagnostic_rows(guild_id)
        compiled = self.service.get_compiled_config(guild_id)
        embed = discord.Embed(
            title="Babblebox Permission Health",
            description="Checks Babblebox's current server permissions and maps any missing permissions to the feature lanes they directly affect.",
            color=ge.EMBED_THEME["info"],
        )
        if rows:
            embed.add_field(
                name="Missing Server Permissions",
                value=ge.join_limited_lines(
                    [f"**{label}**: {impact}" for label, impact in rows],
                    limit=1024,
                    empty="None.",
                ),
                inline=False,
            )
            embed.add_field(
                name="Summary",
                value=f"Babblebox is currently missing **{len(rows)}** audited server permissions or permission-dependent capabilities in this guild.",
                inline=False,
            )
        else:
            embed.add_field(
                name="Missing Server Permissions",
                value="None from the audited moderation and admin feature set.",
                inline=False,
            )
            embed.add_field(
                name="Summary",
                value="Babblebox has the audited moderation and admin permissions at the server level.",
                inline=False,
            )

        admin_log_lines: list[str] = []
        if compiled.admin_log_channel_id is None:
            admin_log_lines.append("Admin log channel: **Not configured**")
        else:
            channel = self.service._guild_channel(guild, compiled.admin_log_channel_id)
            if channel is None:
                admin_log_lines.append("Admin log channel: configured, but missing or inaccessible.")
            else:
                channel_perms = channel.permissions_for(me)
                admin_log_lines.append(f"Admin log channel: {channel.mention}")
                if not getattr(channel_perms, "view_channel", False):
                    admin_log_lines.append("Babblebox cannot view that admin log channel.")
                if not getattr(channel_perms, "send_messages", False):
                    admin_log_lines.append("Babblebox cannot send messages in that admin log channel.")
                if not getattr(channel_perms, "embed_links", False):
                    admin_log_lines.append("Babblebox cannot embed messages in that admin log channel.")
        embed.add_field(name="Log Delivery", value="\n".join(admin_log_lines), inline=False)
        embed.add_field(
            name="Emergency Lock Lane",
            value=(
                f"Access model: **{self.service.lock_access_summary(guild_id)}**\n"
                "Direct locks only target normal text channels and intentionally refuse category-synced channels."
            ),
            inline=False,
        )
        operability = self._operability_lines(guild_id)
        if operability:
            embed.add_field(
                name="Current Blockers Or Warnings",
                value=ge.join_limited_lines(operability[:6], limit=1024, empty="No additional blockers detected."),
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Admin | /admin permissions")

    async def _overview_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        counts = await self.service.get_counts(guild_id)
        embed = discord.Embed(
            title="Admin Control Panel",
            description="Operator-first control surface for follow-up, exclusions, logs, permissions, and channel locks.",
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Follow-up Lane",
            value=(
                f"Enabled: **{'Yes' if config['followup_enabled'] else 'No'}**\n"
                f"Role: {self._role_mention(config['followup_role_id'])}\n"
                f"Mode: {FOLLOWUP_MODE_LABELS[config['followup_mode']]}\n"
                f"Duration: {_followup_duration_label(config['followup_duration_value'], config['followup_duration_unit'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Current Backlog",
            value=(
                f"Pending ban-return candidates: **{counts['ban_candidates']}**\n"
                f"Active follow-up roles: **{counts['active_followups']}**\n"
                f"Pending follow-up reviews: **{counts['pending_reviews']}**\n"
                f"Active channel locks: **{counts['active_channel_locks']}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Admin Delivery",
            value=(
                f"Channel: {self._channel_mention(config['admin_log_channel_id'])}\n"
                f"Alert role: {self._role_mention(config['admin_alert_role_id'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Configure",
            value=(
                "Use **Edit Follow-up**, **Edit Exclusions**, and **Edit Logs** for the most common changes.\n"
                "Use **Run Permission Check** when automations feel blocked or incomplete."
            ),
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin followup`, `/admin logs`, `/admin exclusions`, `/admin permissions`",
            inline=False,
        )
        return embed

    async def _followup_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Punishment Follow-up",
            description="Manage the returned-after-ban follow-up lane directly from the panel, with commands still available for exact overrides.",
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(
            name="Current Policy",
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
            name="What This Controls",
            value=(
                "Babblebox does not know the original ban length.\n"
                "It only reacts when a member returns within 30 days of a ban event.\n"
                "Auto-remove removes the role on expiry.\n"
                "Review mode posts one moderator review item to the admin log channel."
            ),
            inline=False,
        )
        embed.add_field(
            name="Panel Actions",
            value="Use **Edit Follow-up** to change enabled state, follow-up role, mode, and duration directly from the panel.",
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin followup enabled:true role:@Probation mode:review duration:30d`",
            inline=False,
        )
        return embed

    async def _exclusions_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Exclusions And Trusted Roles",
            description="Shared exclusions keep follow-up explicit, predictable, and easy to audit.",
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
                f"Follow-up exempts staff/trusted: **{'Yes' if config['followup_exempt_staff'] else 'No'}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Panel Actions",
            value="Use **Edit Exclusions** to replace the shared lists directly from the panel and toggle the follow-up staff exemption without leaving the UI.",
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin exclusions target:trusted_role_ids state:on role:@Mods`\n`/admin exclusions followup_exempt_staff:false`",
            inline=False,
        )
        return embed

    async def _logs_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Logs And Alerts",
            description="Choose one calm staff-facing delivery lane for admin automation output and operability warnings.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Channel: {self._channel_mention(config['admin_log_channel_id'])}\n"
                f"Alert role: {self._role_mention(config['admin_alert_role_id'])}\n"
                "Logs cover follow-up role assignments, review deadlines, lock actions, timeout removals, and clear operability failures."
            ),
            inline=False,
        )
        embed.add_field(
            name="Panel Actions",
            value="Use **Edit Logs** to change the admin log channel or alert role directly from the panel.",
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin logs channel:#admin-log role:@Mods`",
            inline=False,
        )
        return embed

    async def build_panel_embed(self, guild_id: int, section: str) -> discord.Embed:
        if section == "followup":
            embed = await self._followup_embed(guild_id)
        elif section == "exclusions":
            embed = await self._exclusions_embed(guild_id)
        elif section == "logs":
            embed = await self._logs_embed(guild_id)
        else:
            embed = await self._overview_embed(guild_id)
        operability = self._operability_lines(guild_id)
        if operability:
            embed.add_field(name="Operability", value="\n".join(operability[:6]), inline=False)
        return ge.style_embed(
            embed,
            footer="Babblebox Admin | /admin panel, status, followup, logs, exclusions, or permissions",
        )

    async def _send_admin_response(
        self,
        ctx: commands.Context,
        *,
        embed: discord.Embed,
        view: discord.ui.View | None = None,
        retry_without_view: bool = False,
        recovery_title: str = "Admin Response Unavailable",
        recovery_description: str = "Babblebox could not finish this admin response just now. Please try the command again in a moment.",
        recovery_footer: str = "Babblebox Admin",
    ) -> HybridPanelSendResult:
        result = await send_hybrid_panel_response(ctx, embed=embed, view=view, ephemeral=True)
        if not result.delivered and retry_without_view and view is not None:
            fallback = await send_hybrid_panel_response(ctx, embed=embed, ephemeral=True)
            if fallback.delivered:
                return fallback
            result = HybridPanelSendResult(delivered=False, path=fallback.path, error=fallback.error or result.error)
        if result.delivered:
            return result

        recovery_embed = ge.make_status_embed(recovery_title, recovery_description, tone="warning", footer=recovery_footer)
        with contextlib.suppress(discord.ClientException, discord.HTTPException, discord.NotFound, TypeError, ValueError):
            recovery = await send_hybrid_panel_response(ctx, embed=recovery_embed, ephemeral=True)
            if recovery.delivered:
                return recovery
        return result

    async def _send_result(
        self,
        ctx: commands.Context,
        title: str,
        message: str,
        *,
        ok: bool,
        footer: str = "Babblebox Admin",
        recovery_footer: str = "Babblebox Admin",
    ):
        embed = ge.make_status_embed(title, message, tone="success" if ok else "warning", footer=footer)
        operability = self._operability_lines(ctx.guild.id)
        if operability:
            embed.add_field(name="Operability", value="\n".join(operability[:6]), inline=False)
        await self._send_admin_response(
            ctx,
            embed=embed,
            recovery_title=f"{title} Unavailable",
            recovery_description="Babblebox could not finish the admin update response just now. No additional admin changes were applied after this point.",
            recovery_footer=recovery_footer,
        )

    async def _run_result_action(
        self,
        ctx: commands.Context,
        *,
        title: str,
        footer: str,
        recovery_footer: str,
        unexpected_message: str,
        action,
    ):
        try:
            ok, message = await action()
        except Exception as exc:
            print(f"{title} action failed: {exc}")
            ok, message = False, unexpected_message
        await self._send_result(
            ctx,
            title,
            message,
            ok=ok,
            footer=footer,
            recovery_footer=recovery_footer,
        )

    async def _send_panel(self, ctx: commands.Context, *, section: str = "overview"):
        view = AdminPanelView(self, guild_id=ctx.guild.id, author_id=ctx.author.id, section=section)
        result = await self._send_admin_response(
            ctx,
            embed=await view.current_embed(),
            view=view,
            retry_without_view=True,
            recovery_title="Admin Panel Unavailable",
            recovery_description="Babblebox could not open the admin panel just now. Please try `/admin panel` again in a moment.",
        )
        if result.message is not None:
            view.message = result.message

    async def _member_status_embed(self, member: discord.Member) -> discord.Embed:
        status = await self.service.get_member_status(member)
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
        embed.add_field(
            name="Shared Exclusions",
            value=(
                f"Excluded member: {'Yes' if member.id in self.service.get_config(member.guild.id)['excluded_user_ids'] else 'No'}\n"
                f"Trusted or staff exempt from follow-up: {status['followup_exempt_reason'] or 'No'}"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Member automation status")

    def _admin_status_embed(self, title: str, message: str, *, ok: bool) -> discord.Embed:
        return ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Admin")

    async def _resolve_interaction_message(self, interaction: discord.Interaction, response=None) -> discord.Message | None:
        resource = getattr(response, "resource", None)
        if resource is not None and hasattr(resource, "edit"):
            return resource
        message_id = getattr(response, "message_id", None)
        channel = getattr(interaction, "channel", None)
        if isinstance(message_id, int) and channel is not None and hasattr(channel, "get_partial_message"):
            with contextlib.suppress(Exception):
                return channel.get_partial_message(message_id)
        original_response = getattr(interaction, "original_response", None)
        if callable(original_response):
            with contextlib.suppress(discord.NotFound, discord.HTTPException, discord.ClientException):
                return await original_response()
        return None

    async def _send_private_interaction(self, interaction: discord.Interaction, **kwargs):
        if interaction.guild is not None:
            kwargs["ephemeral"] = True
        else:
            kwargs.pop("ephemeral", None)
        try:
            if interaction.response.is_done():
                kwargs.setdefault("wait", True)
                return await interaction.followup.send(**kwargs)
            response = await interaction.response.send_message(**kwargs)
            return await self._resolve_interaction_message(interaction, response=response)
        except discord.InteractionResponded:
            with contextlib.suppress(discord.NotFound, discord.HTTPException):
                kwargs.setdefault("wait", True)
                return await interaction.followup.send(**kwargs)
            return None
        except (discord.NotFound, discord.HTTPException):
            return None

    async def _edit_interaction_message(self, interaction: discord.Interaction, **kwargs) -> bool:
        if not interaction.response.is_done():
            await interaction.response.edit_message(**kwargs)
            return True
        edit_original_response = getattr(interaction, "edit_original_response", None)
        if callable(edit_original_response):
            with contextlib.suppress(discord.NotFound, discord.HTTPException, discord.ClientException):
                await edit_original_response(**kwargs)
                return True
        message = getattr(interaction, "message", None)
        edit_message = getattr(message, "edit", None)
        if callable(edit_message):
            with contextlib.suppress(discord.NotFound, discord.HTTPException, AttributeError):
                await edit_message(**kwargs)
                return True
        return False

    async def _defer_component_interaction(
        self,
        interaction: discord.Interaction,
        *,
        stage: str,
        failure_title: str,
        failure_message: str,
        guild_id: int | None = None,
    ) -> bool:
        if interaction.response.is_done():
            return True
        defer = getattr(interaction.response, "defer", None)
        if not callable(defer):
            return True
        try:
            await defer(ephemeral=interaction.guild is not None, thinking=False)
            return True
        except Exception:
            await self._send_private_interaction(
                interaction,
                embed=self._admin_status_embed(failure_title, failure_message, ok=False),
            )
            return False

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @commands.hybrid_group(
        name="lock",
        with_app_command=True,
        description="Emergency channel lock tools",
        invoke_without_command=True,
    )
    async def lock_group(self, ctx: commands.Context):
        if not await self._lock_guard(ctx):
            return
        await self._send_admin_response(
            ctx,
            embed=await self._lock_embed(ctx.guild.id),
            recovery_title="Emergency Lock Panel Unavailable",
            recovery_description="Babblebox could not open the emergency lock panel just now. Please try `/lock` again in a moment.",
            recovery_footer="Babblebox Lock",
        )

    @lock_group.command(name="channel", with_app_command=True, description="Lock one text channel safely")
    @app_commands.describe(
        channel="Which text channel to lock. Leave blank to lock the current channel",
        duration="Optional timer like 30m, 2h, or 1d",
        notice_message="Optional one-off notice to post in the locked channel",
        post_notice="Post the configured or custom notice in the channel after locking it",
    )
    async def lock_channel_command(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        duration: Optional[str] = None,
        notice_message: Optional[str] = None,
        post_notice: bool = True,
    ):
        if not await self._lock_guard(ctx):
            return
        target_channel = self._lock_target_channel(ctx, channel)
        if target_channel is None:
            await self._send_result(
                ctx,
                "Channel Lock",
                "Choose a text channel or run the command inside the channel you want to lock.",
                ok=False,
                footer="Babblebox Lock",
                recovery_footer="Babblebox Lock",
            )
            return
        await self._run_result_action(
            ctx,
            title="Channel Lock",
            footer="Babblebox Lock",
            recovery_footer="Babblebox Lock",
            unexpected_message="Babblebox could not finish the emergency lock action right now. Review the channel before retrying.",
            action=lambda: self.service.lock_channel(
                ctx.guild,
                target_channel,
                actor=ctx.author,
                duration_text=duration,
                notice_message=notice_message,
                post_notice=post_notice,
            ),
        )

    @lock_group.command(name="remove", with_app_command=True, description="Remove a Babblebox emergency lock safely")
    @app_commands.describe(channel="Which text channel to unlock. Leave blank to unlock the current channel")
    async def lock_remove_command(self, ctx: commands.Context, channel: Optional[discord.TextChannel] = None):
        if not await self._lock_guard(ctx):
            return
        target_channel = self._lock_target_channel(ctx, channel)
        if target_channel is None:
            await self._send_result(
                ctx,
                "Channel Unlock",
                "Choose a text channel or run the command inside the channel you want to unlock.",
                ok=False,
                footer="Babblebox Lock",
                recovery_footer="Babblebox Lock",
            )
            return
        await self._run_result_action(
            ctx,
            title="Channel Unlock",
            footer="Babblebox Lock",
            recovery_footer="Babblebox Lock",
            unexpected_message="Babblebox could not finish the emergency unlock action right now. Review the channel before retrying.",
            action=lambda: self.service.remove_channel_lock(ctx.guild, target_channel, actor=ctx.author, automatic=False),
        )

    @lock_group.command(name="settings", with_app_command=True, description="Review or change emergency lock defaults")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        default_notice="Default lock notice to post when a moderator does not supply a one-off notice",
        clear_notice="Clear the custom default notice and return to Babblebox's built-in notice",
        admin_only="Limit `/lock` use to Manage Server/admin users instead of moderators who can manage channels or messages, timeout, kick, or ban members",
    )
    async def lock_settings_command(
        self,
        ctx: commands.Context,
        default_notice: Optional[str] = None,
        clear_notice: bool = False,
        admin_only: Optional[bool] = None,
    ):
        if not await self._lock_guard(ctx, settings=True):
            return
        if default_notice is None and not clear_notice and admin_only is None:
            await self._send_admin_response(
                ctx,
                embed=await self._lock_embed(ctx.guild.id),
                recovery_title="Emergency Lock Settings Unavailable",
                recovery_description="Babblebox could not open the emergency lock settings just now. Please try `/lock settings` again in a moment.",
                recovery_footer="Babblebox Lock",
            )
            return
        if clear_notice and default_notice is not None:
            await self._send_result(
                ctx,
                "Emergency Lock Settings",
                "Choose either a new default notice or `clear_notice:true`, not both in the same update.",
                ok=False,
                footer="Babblebox Lock",
                recovery_footer="Babblebox Lock",
            )
            return
        await self._run_result_action(
            ctx,
            title="Emergency Lock Settings",
            footer="Babblebox Lock",
            recovery_footer="Babblebox Lock",
            unexpected_message="Babblebox could not update the emergency lock settings right now. No settings changed after the failure point.",
            action=lambda: self.service.set_lock_config(
                ctx.guild.id,
                notice_template=None if clear_notice else (default_notice if default_notice is not None else ...),
                admin_only=admin_only,
            ),
        )

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @commands.hybrid_group(
        name="timeout",
        with_app_command=True,
        description="Moderator timeout removal tools",
        invoke_without_command=True,
    )
    async def timeout_group(self, ctx: commands.Context):
        if not await self._timeout_guard(ctx):
            return
        await self._send_admin_response(
            ctx,
            embed=await self._timeout_embed(ctx.guild.id),
            recovery_title="Timeout Removal Panel Unavailable",
            recovery_description="Babblebox could not open the timeout removal panel just now. Please try `/timeout` again in a moment.",
            recovery_footer="Babblebox Timeout",
        )

    @timeout_group.command(name="remove", with_app_command=True, description="Remove one active member timeout safely")
    @app_commands.describe(
        member="Which member's timeout to remove",
        reason="Optional moderation note for the audit log",
    )
    async def timeout_remove_command(
        self,
        ctx: commands.Context,
        member: discord.Member,
        reason: Optional[str] = None,
    ):
        if not await self._timeout_guard(ctx):
            return
        await self._run_result_action(
            ctx,
            title="Timeout Removal",
            footer="Babblebox Timeout",
            recovery_footer="Babblebox Timeout",
            unexpected_message="Babblebox could not finish the timeout removal right now. Check whether the member is still timed out before retrying.",
            action=lambda: self.service.remove_timeout(
                ctx.guild,
                member,
                actor=ctx.author,
                reason_text=reason,
            ),
        )

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
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
            await self._send_admin_response(
                ctx,
                embed=await self.build_panel_embed(ctx.guild.id, "overview"),
                recovery_title="Admin Overview Unavailable",
                recovery_description="Babblebox could not open the admin overview just now. Please try `/admin status` again in a moment.",
            )
            return
        await self._send_admin_response(
            ctx,
            embed=await self._member_status_embed(member),
            recovery_title="Admin Member Status Unavailable",
            recovery_description="Babblebox could not finish the member status response just now. Please try `/admin status` again in a moment.",
        )

    @admin_group.command(name="panel", with_app_command=True, description="Open the private admin panel")
    async def admin_panel_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @admin_group.command(name="permissions", with_app_command=True, description="See which bot permissions are missing and which features they affect")
    async def admin_permissions_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_admin_response(
            ctx,
            embed=await self._permission_diagnostics_embed(ctx.guild.id),
            recovery_title="Permission Diagnostics Unavailable",
            recovery_description="Babblebox could not open permission diagnostics just now. Please try `/admin permissions` again in a moment.",
        )

    @admin_group.command(name="followup", with_app_command=True, description="Configure returned-after-ban follow-up roles")
    @app_commands.describe(
        enabled="Turn punishment follow-up on or off",
        role="Role to assign when someone returns within 30 days of a ban event",
        mode="Remove automatically later or open a moderator review item",
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
            await self._send_admin_response(
                ctx,
                embed=await self.build_panel_embed(ctx.guild.id, "followup"),
                recovery_title="Punishment Follow-up Unavailable",
                recovery_description="Babblebox could not open the follow-up settings just now. Please try `/admin followup` again in a moment.",
            )
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

    @admin_group.command(name="logs", with_app_command=True, description="Configure the admin log channel and alert role")
    @app_commands.describe(
        channel="Channel where Babblebox should post admin automation logs",
        role="Optional staff role to mention for important admin alerts",
        clear_channel="Clear the configured admin log channel",
        clear_role="Clear the configured admin alert role",
    )
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
            await self._send_admin_response(
                ctx,
                embed=await self.build_panel_embed(ctx.guild.id, "logs"),
                recovery_title="Admin Logs Unavailable",
                recovery_description="Babblebox could not open the admin log settings just now. Please try `/admin logs` again in a moment.",
            )
            return
        current = self.service.get_config(ctx.guild.id)
        resolved_channel_id = None if clear_channel else (channel.id if channel is not None else current["admin_log_channel_id"])
        resolved_role_id = None if clear_role else (role.id if role is not None else current["admin_alert_role_id"])
        ok, message = await self.service.set_logs_config(ctx.guild.id, channel_id=resolved_channel_id, alert_role_id=resolved_role_id)
        await self._send_result(ctx, "Admin Logs", message, ok=ok)

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
    ):
        if not await self._guard(ctx):
            return
        if (
            target is None
            and state is None
            and followup_exempt_staff is None
        ):
            await self._send_admin_response(
                ctx,
                embed=await self.build_panel_embed(ctx.guild.id, "exclusions"),
                recovery_title="Admin Exclusions Unavailable",
                recovery_description="Babblebox could not open the exclusions settings just now. Please try `/admin exclusions` again in a moment.",
            )
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
        if followup_exempt_staff is not None:
            part_ok, part_message = await self.service.set_exemption_toggle(ctx.guild.id, "followup_exempt_staff", followup_exempt_staff)
            ok = ok and part_ok
            messages.append(part_message)
        await self._send_result(ctx, "Admin Exclusions", "\n".join(messages), ok=ok)

    @commands.Cog.listener()
    async def on_member_ban(self, guild: discord.Guild, user: discord.abc.User):
        await self.service.handle_member_ban(guild, user)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        await self.service.handle_member_join(member)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await self.service.handle_member_remove(member)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))

