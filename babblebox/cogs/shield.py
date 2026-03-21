from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.shield_service import (
    ACTION_LABELS,
    CUSTOM_PATTERN_LIMIT,
    PACK_LABELS,
    SENSITIVITY_LABELS,
    ShieldService,
)


PACK_CHOICES = [
    app_commands.Choice(name="Privacy Leak", value="privacy"),
    app_commands.Choice(name="Promo / Invite", value="promo"),
    app_commands.Choice(name="Scam Heuristic", value="scam"),
]
ACTION_CHOICES = [
    app_commands.Choice(name="Detect only", value="detect"),
    app_commands.Choice(name="Log only", value="log"),
    app_commands.Choice(name="Delete + log", value="delete_log"),
    app_commands.Choice(name="Delete + log + escalate", value="delete_escalate"),
    app_commands.Choice(name="Timeout + log", value="timeout_log"),
]
SENSITIVITY_CHOICES = [
    app_commands.Choice(name="Low", value="low"),
    app_commands.Choice(name="Normal", value="normal"),
    app_commands.Choice(name="High", value="high"),
]
STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
SCAN_MODE_CHOICES = [
    app_commands.Choice(name="All eligible messages", value="all"),
    app_commands.Choice(name="Only included scope", value="only_included"),
]
PATTERN_MODE_CHOICES = [
    app_commands.Choice(name="Contains text", value="contains"),
    app_commands.Choice(name="Whole word", value="word"),
    app_commands.Choice(name="Safe wildcard", value="wildcard"),
]


class ShieldCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ShieldService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "shield_service", self.service)

    def cog_unload(self):
        if hasattr(self.bot, "shield_service"):
            delattr(self.bot, "shield_service")
        self.bot.loop.create_task(self.service.close())

    async def _guard(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "Shield can only be configured inside a server.", tone="warning", footer="Babblebox Shield"),
                ephemeral=True,
            )
            return False
        perms = getattr(ctx.author, "guild_permissions", None)
        is_admin = bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))
        if not is_admin:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure Babblebox Shield.",
                    tone="warning",
                    footer="Babblebox Shield",
                ),
                ephemeral=True,
            )
            return False
        if not self.service.storage_ready:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Shield Unavailable", self.service.storage_message(), tone="warning", footer="Babblebox Shield"),
                ephemeral=True,
            )
            return False
        return True

    def _format_mentions(self, ids: list[int], *, kind: str) -> str:
        if not ids:
            return "None"
        prefix = {"channel": "<#", "user": "<@", "role": "<@&"}[kind]
        rendered = [f"{prefix}{value}>" for value in ids[:6]]
        if len(ids) > 6:
            rendered.append(f"+{len(ids) - 6} more")
        return ", ".join(rendered)

    def _status_embed(self, guild: discord.Guild) -> discord.Embed:
        config = self.service.get_config(guild.id)
        embed = discord.Embed(
            title="Shield Control Panel",
            description="Conservative, log-first safety controls for privacy leaks, promo spam, and heuristic scam bait.",
            color=ge.EMBED_THEME["warning"] if config["module_enabled"] else ge.EMBED_THEME["info"],
        )
        log_channel = f"<#{config['log_channel_id']}>" if config.get("log_channel_id") else "Not set"
        alert_role = f"<@&{config['alert_role_id']}>" if config.get("alert_role_id") else "None"
        embed.add_field(
            name="Module",
            value=(
                f"Enabled: **{'Yes' if config['module_enabled'] else 'No'}**\n"
                f"Scan mode: `{config['scan_mode']}`\n"
                f"Log channel: {log_channel}\n"
                f"Alert role: {alert_role}"
            ),
            inline=False,
        )
        pack_lines = []
        for pack in ("privacy", "promo", "scam"):
            pack_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | "
                f"Action: `{config[f'{pack}_action']}` | "
                f"Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}"
            )
        embed.add_field(name="Protection Packs", value="\n\n".join(pack_lines), inline=False)
        embed.add_field(
            name="Scope",
            value=(
                f"Include channels: {self._format_mentions(config['included_channel_ids'], kind='channel')}\n"
                f"Exclude channels: {self._format_mentions(config['excluded_channel_ids'], kind='channel')}\n"
                f"Include roles: {self._format_mentions(config['included_role_ids'], kind='role')}\n"
                f"Exclude roles: {self._format_mentions(config['excluded_role_ids'], kind='role')}\n"
                f"Trusted roles: {self._format_mentions(config['trusted_role_ids'], kind='role')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Targeted Bypass",
            value=(
                f"Include users: {self._format_mentions(config['included_user_ids'], kind='user')}\n"
                f"Exclude users: {self._format_mentions(config['excluded_user_ids'], kind='user')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Allowlists",
            value=(
                f"Domains: {', '.join(config['allow_domains'][:6]) or 'None'}\n"
                f"Invite codes: {', '.join(config['allow_invite_codes'][:6]) or 'None'}\n"
                f"Phrases: {', '.join(config['allow_phrases'][:4]) or 'None'}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Escalation",
            value=(
                f"Threshold: `{config['escalation_threshold']}` hits\n"
                f"Window: `{config['escalation_window_minutes']}` minutes\n"
                f"Timeout: `{config['timeout_minutes']}` minutes"
            ),
            inline=True,
        )
        embed.add_field(
            name="Advanced Mode",
            value=(
                f"{len(config['custom_patterns'])}/{CUSTOM_PATTERN_LIMIT} safe custom patterns\n"
                "Raw user regex is intentionally unsupported."
            ),
            inline=True,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Start with log-only and tune before deletes")

    async def _send_result(self, ctx: commands.Context, title: str, message: str, *, ok: bool):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Shield"),
            ephemeral=True,
        )

    @commands.hybrid_group(
        name="shield",
        with_app_command=True,
        description="Configure Babblebox Shield moderation and safety",
        invoke_without_command=True,
    )
    async def shield_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_group.command(name="status", with_app_command=True, description="View the Babblebox Shield control panel")
    async def shield_status_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_group.command(name="module", with_app_command=True, description="Enable or disable Shield for this server")
    @app_commands.describe(enabled="Turn the Babblebox Shield module on or off")
    async def shield_module_command(self, ctx: commands.Context, enabled: bool):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_module_enabled(ctx.guild.id, enabled)
        await self._send_result(ctx, "Shield Module", message, ok=ok)

    @shield_group.command(name="pack", with_app_command=True, description="Configure one Shield protection pack")
    @app_commands.describe(pack="Which protection pack to adjust", enabled="Turn this pack on or off", action="What Shield should do when it matches", sensitivity="How cautious or broad the pack should be")
    @app_commands.choices(pack=PACK_CHOICES, action=ACTION_CHOICES, sensitivity=SENSITIVITY_CHOICES)
    async def shield_pack_command(
        self,
        ctx: commands.Context,
        pack: str,
        enabled: Optional[bool] = None,
        action: Optional[str] = None,
        sensitivity: Optional[str] = None,
    ):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_pack_config(ctx.guild.id, pack, enabled=enabled, action=action, sensitivity=sensitivity)
        await self._send_result(ctx, "Shield Pack Updated", message, ok=ok)

    @shield_group.command(name="log", with_app_command=True, description="Set the private Shield log channel and optional alert role")
    @app_commands.describe(channel="Channel for Shield alerts", role="Optional mod role to ping", clear_channel="Clear the current log channel", clear_role="Clear the current alert role")
    async def shield_log_command(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
        clear_channel: bool = False,
        clear_role: bool = False,
    ):
        if not await self._guard(ctx):
            return
        messages: list[str] = []
        ok = True
        if channel is not None or clear_channel:
            current_channel = None if clear_channel else channel.id
            channel_ok, channel_message = await self.service.set_log_channel(ctx.guild.id, current_channel)
            ok = ok and channel_ok
            messages.append(channel_message)
        if role is not None or clear_role:
            current_role = None if clear_role else role.id
            role_ok, role_message = await self.service.set_alert_role(ctx.guild.id, current_role)
            ok = ok and role_ok
            messages.append(role_message)
        if not messages:
            messages.append("Nothing changed. Provide a channel or role, or use one of the clear toggles.")
            ok = False
        await self._send_result(ctx, "Shield Logging", "\n".join(messages), ok=ok)

    @shield_group.command(name="scope", with_app_command=True, description="Choose whether Shield scans everything or only explicitly included scope")
    @app_commands.describe(mode="Scan all eligible messages or only explicitly included channels/users/roles")
    @app_commands.choices(mode=SCAN_MODE_CHOICES)
    async def shield_scope_command(self, ctx: commands.Context, mode: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_scan_mode(ctx.guild.id, mode)
        await self._send_result(ctx, "Shield Scope", message, ok=ok)

    @shield_group.group(
        name="include",
        with_app_command=True,
        invoke_without_command=True,
        description="Include channels, users, or roles when Shield is in only-included mode",
    )
    async def shield_include_app_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_include_app_group.command(name="channel", description="Include or remove a channel from Shield scope")
    @app_commands.describe(state="Turn this include on or off", channel="Channel to include. Defaults to the current channel.")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_include_channel_command(self, ctx: commands.Context, state: str, channel: Optional[discord.TextChannel] = None):
        if not await self._guard(ctx):
            return
        target = channel.id if channel is not None else ctx.channel.id
        ok, message = await self.service.set_filter_target(ctx.guild.id, "included_channel_ids", target, state == "on")
        await self._send_result(ctx, "Shield Include Channel", message, ok=ok)

    @shield_include_app_group.command(name="role", description="Include or remove a role from Shield scope")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_include_role_command(self, ctx: commands.Context, state: str, role: discord.Role):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_filter_target(ctx.guild.id, "included_role_ids", role.id, state == "on")
        await self._send_result(ctx, "Shield Include Role", message, ok=ok)

    @shield_include_app_group.command(name="user", description="Include or remove one user from Shield scope")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_include_user_command(self, ctx: commands.Context, state: str, user: discord.Member):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_filter_target(ctx.guild.id, "included_user_ids", user.id, state == "on")
        await self._send_result(ctx, "Shield Include User", message, ok=ok)

    @shield_group.group(
        name="exclude",
        with_app_command=True,
        invoke_without_command=True,
        description="Exclude channels, users, or roles from Shield scanning",
    )
    async def shield_exclude_app_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_exclude_app_group.command(name="channel", description="Exclude or un-exclude a channel from Shield scanning")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_exclude_channel_command(self, ctx: commands.Context, state: str, channel: Optional[discord.TextChannel] = None):
        if not await self._guard(ctx):
            return
        target = channel.id if channel is not None else ctx.channel.id
        ok, message = await self.service.set_filter_target(ctx.guild.id, "excluded_channel_ids", target, state == "on")
        await self._send_result(ctx, "Shield Exclude Channel", message, ok=ok)

    @shield_exclude_app_group.command(name="role", description="Exclude or un-exclude a role from Shield scanning")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_exclude_role_command(self, ctx: commands.Context, state: str, role: discord.Role):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_filter_target(ctx.guild.id, "excluded_role_ids", role.id, state == "on")
        await self._send_result(ctx, "Shield Exclude Role", message, ok=ok)

    @shield_exclude_app_group.command(name="user", description="Exclude or un-exclude one user from Shield scanning")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_exclude_user_command(self, ctx: commands.Context, state: str, user: discord.Member):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_filter_target(ctx.guild.id, "excluded_user_ids", user.id, state == "on")
        await self._send_result(ctx, "Shield Exclude User", message, ok=ok)

    @shield_group.group(
        name="trust",
        with_app_command=True,
        invoke_without_command=True,
        description="Bypass Shield for trusted roles",
    )
    async def shield_trust_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_trust_group.command(name="role", description="Trust or untrust a role so Shield skips it")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_trust_role_command(self, ctx: commands.Context, state: str, role: discord.Role):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_filter_target(ctx.guild.id, "trusted_role_ids", role.id, state == "on")
        await self._send_result(ctx, "Shield Trusted Role", message, ok=ok)

    @shield_group.group(
        name="allow",
        with_app_command=True,
        invoke_without_command=True,
        description="Add or remove allowlisted domains, invite codes, and phrases",
    )
    async def shield_allow_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_allow_group.command(name="domain", description="Add or remove an allowlisted domain")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_allow_domain_command(self, ctx: commands.Context, state: str, value: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_allow_entry(ctx.guild.id, "allow_domains", value, state == "on")
        await self._send_result(ctx, "Shield Allowlist Domain", message, ok=ok)

    @shield_allow_group.command(name="invite", description="Add or remove an allowlisted Discord invite code")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_allow_invite_command(self, ctx: commands.Context, state: str, value: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_allow_entry(ctx.guild.id, "allow_invite_codes", value, state == "on")
        await self._send_result(ctx, "Shield Allowlist Invite", message, ok=ok)

    @shield_allow_group.command(name="phrase", description="Add or remove an allowlisted phrase that bypasses Shield matching")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_allow_phrase_command(self, ctx: commands.Context, state: str, value: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_allow_entry(ctx.guild.id, "allow_phrases", value, state == "on")
        await self._send_result(ctx, "Shield Allowlist Phrase", message, ok=ok)

    @shield_group.command(name="escalation", with_app_command=True, description="Tune repeated-hit escalation thresholds")
    async def shield_escalation_command(
        self,
        ctx: commands.Context,
        threshold: Optional[int] = None,
        window_minutes: Optional[int] = None,
        timeout_minutes: Optional[int] = None,
    ):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_escalation(
            ctx.guild.id,
            threshold=threshold,
            window_minutes=window_minutes,
            timeout_minutes=timeout_minutes,
        )
        await self._send_result(ctx, "Shield Escalation", message, ok=ok)

    @shield_group.group(
        name="advanced",
        with_app_command=True,
        invoke_without_command=True,
        description="Safe advanced matching with contains, whole-word, and wildcard patterns",
    )
    async def shield_advanced_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._status_embed(ctx.guild), ephemeral=True)

    @shield_advanced_group.command(name="add", description="Add a safe advanced Shield pattern")
    @app_commands.choices(mode=PATTERN_MODE_CHOICES, action=ACTION_CHOICES)
    async def shield_advanced_add_command(self, ctx: commands.Context, label: str, pattern: str, mode: str, action: str = "log"):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.add_custom_pattern(ctx.guild.id, label=label, pattern=pattern, mode=mode, action=action)
        await self._send_result(ctx, "Shield Advanced Pattern", message, ok=ok)

    @shield_advanced_group.command(name="remove", description="Remove an advanced Shield pattern by ID")
    async def shield_advanced_remove_command(self, ctx: commands.Context, pattern_id: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.remove_custom_pattern(ctx.guild.id, pattern_id)
        await self._send_result(ctx, "Shield Advanced Pattern", message, ok=ok)

    @shield_advanced_group.command(name="list", description="List the current advanced Shield patterns")
    async def shield_advanced_list_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        patterns = self.service.get_config(ctx.guild.id).get("custom_patterns", [])
        embed = discord.Embed(title="Shield Advanced Patterns", color=ge.EMBED_THEME["info"])
        if not patterns:
            embed.description = "No advanced patterns are configured."
        else:
            lines = [
                f"`{item['pattern_id']}` | **{item['label']}** | `{item['mode']}` | `{item['action']}` | `{item['pattern']}`"
                for item in patterns[:10]
            ]
            embed.description = "\n".join(lines)
        await send_hybrid_response(ctx, embed=ge.style_embed(embed, footer="Babblebox Shield | Safe patterns only"), ephemeral=True)

    @shield_group.command(name="test", with_app_command=True, description="Dry-run a message through the current Shield rules")
    async def shield_test_command(self, ctx: commands.Context, text: str):
        if not await self._guard(ctx):
            return
        matches = self.service.test_message(ctx.guild.id, text)
        embed = discord.Embed(title="Shield Test", description="Dry-run results for the current configuration.", color=ge.EMBED_THEME["info"])
        if not matches:
            embed.add_field(name="Result", value="No Shield pack matched that sample.", inline=False)
        else:
            embed.add_field(
                name="Matches",
                value="\n".join(
                    f"**{PACK_LABELS.get(item.pack, item.pack.title())}** | {item.label} | `{item.action}` | {item.confidence}"
                    for item in matches[:5]
                ),
                inline=False,
            )
        await send_hybrid_response(ctx, embed=ge.style_embed(embed, footer="Babblebox Shield | Dry run only"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ShieldCog(bot))
