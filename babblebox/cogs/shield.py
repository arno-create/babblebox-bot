from __future__ import annotations

import contextlib
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
FILTER_TARGET_CHOICES = [
    app_commands.Choice(name="Include current/selected channel", value="included_channel_ids"),
    app_commands.Choice(name="Exclude current/selected channel", value="excluded_channel_ids"),
    app_commands.Choice(name="Include member", value="included_user_ids"),
    app_commands.Choice(name="Exclude member", value="excluded_user_ids"),
    app_commands.Choice(name="Include role", value="included_role_ids"),
    app_commands.Choice(name="Exclude role", value="excluded_role_ids"),
    app_commands.Choice(name="Trust role", value="trusted_role_ids"),
]
ALLOWLIST_BUCKET_CHOICES = [
    app_commands.Choice(name="Domain", value="allow_domains"),
    app_commands.Choice(name="Invite code", value="allow_invite_codes"),
    app_commands.Choice(name="Phrase", value="allow_phrases"),
]
AI_CONFIDENCE_CHOICES = [
    app_commands.Choice(name="Low", value="low"),
    app_commands.Choice(name="Medium", value="medium"),
    app_commands.Choice(name="High", value="high"),
]


class ShieldPanelView(discord.ui.View):
    def __init__(self, cog: "ShieldCog", *, guild_id: int, author_id: int, section: str = "overview"):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.section = section
        self.message: discord.Message | None = None
        self._refresh_buttons()

    def current_embed(self) -> discord.Embed:
        return self.cog.build_panel_embed(self.guild_id, self.section)

    def _refresh_buttons(self):
        statuses = {
            "overview": self.overview_button,
            "rules": self.rules_button,
            "scope": self.scope_button,
            "ai": self.ai_button,
            "logs": self.logs_button,
        }
        for name, button in statuses.items():
            button.style = discord.ButtonStyle.primary if self.section == name else discord.ButtonStyle.secondary
        config = self.cog.service.get_config(self.guild_id)
        ai_status = self.cog.service.get_ai_status(self.guild_id)
        self.toggle_shield_button.label = "Disable Shield" if config["module_enabled"] else "Enable Shield"
        self.toggle_ai_button.label = "Disable AI" if ai_status["enabled"] else "Enable AI"
        self.toggle_ai_button.disabled = not ai_status["supported"] or (not ai_status["provider_available"] and not ai_status["enabled"])

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/shield panel` to open your own Shield admin panel.",
                    tone="info",
                    footer="Babblebox Shield",
                ),
                ephemeral=True,
            )
            return False
        if not self.cog.user_can_manage_shield(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure Babblebox Shield.",
                    tone="warning",
                    footer="Babblebox Shield",
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

    async def _rerender(self, interaction: discord.Interaction, note: str | None = None):
        self._refresh_buttons()
        await interaction.response.edit_message(embed=self.current_embed(), view=self)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    async def _switch_section(self, interaction: discord.Interaction, section: str):
        self.section = section
        await self._rerender(interaction)

    @discord.ui.button(label="Overview", style=discord.ButtonStyle.primary, row=0)
    async def overview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "overview")

    @discord.ui.button(label="Rules", style=discord.ButtonStyle.secondary, row=0)
    async def rules_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "rules")

    @discord.ui.button(label="Scope", style=discord.ButtonStyle.secondary, row=0)
    async def scope_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "scope")

    @discord.ui.button(label="AI", style=discord.ButtonStyle.secondary, row=0)
    async def ai_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "ai")

    @discord.ui.button(label="Logs", style=discord.ButtonStyle.secondary, row=0)
    async def logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch_section(interaction, "logs")

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._rerender(interaction, note="Shield panel refreshed.")

    @discord.ui.button(label="Enable Shield", style=discord.ButtonStyle.success, row=1)
    async def toggle_shield_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        current = self.cog.service.get_config(self.guild_id)
        ok, message = await self.cog.service.set_module_enabled(self.guild_id, not current["module_enabled"])
        await self._rerender(interaction, note=message if ok else message)

    @discord.ui.button(label="Enable AI", style=discord.ButtonStyle.success, row=1)
    async def toggle_ai_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        current = self.cog.service.get_ai_status(self.guild_id)
        if not current["supported"]:
            await interaction.response.send_message("AI review is not available in this server yet.", ephemeral=True)
            return
        if not current["provider_available"] and not current["enabled"]:
            await interaction.response.send_message(
                "AI review cannot be enabled until `OPENAI_API_KEY` is configured.",
                ephemeral=True,
            )
            return
        ok, message = await self.cog.service.set_ai_config(self.guild_id, enabled=not current["enabled"])
        await self._rerender(interaction, note=message if ok else message)


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

    def user_can_manage_shield(self, actor: object) -> bool:
        perms = getattr(actor, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    async def _guard(self, ctx: commands.Context) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "Shield can only be configured inside a server.", tone="warning", footer="Babblebox Shield"),
                ephemeral=True,
            )
            return False
        if not self.user_can_manage_shield(ctx.author):
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

    def _format_text_list(self, values: list[str], *, limit: int) -> str:
        if not values:
            return "None"
        visible = values[:limit]
        suffix = f", +{len(values) - limit} more" if len(values) > limit else ""
        return ", ".join(visible) + suffix

    def _format_ai_pack_summary(self, enabled_packs: list[str]) -> str:
        if not enabled_packs:
            return "None selected"
        return ", ".join(PACK_LABELS.get(pack, pack.title()) for pack in enabled_packs)

    def _overview_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        ai_status = self.service.get_ai_status(guild_id)
        embed = discord.Embed(
            title="Shield Control Panel",
            description="Layer 1 stays local. Layer 2 AI review is optional, second-pass only, and never decides punishment on its own.",
            color=ge.EMBED_THEME["warning"] if config["module_enabled"] else ge.EMBED_THEME["info"],
        )
        log_channel = f"<#{config['log_channel_id']}>" if config.get("log_channel_id") else "Not set"
        alert_role = f"<@&{config['alert_role_id']}>" if config.get("alert_role_id") else "None"
        embed.add_field(
            name="Core Shield",
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
            name="AI Assist",
            value=(
                f"Status: {ai_status['status']}\n"
                f"Enabled: **{'Yes' if ai_status['enabled'] else 'No'}**\n"
                f"Local-confidence threshold: `{ai_status['min_confidence']}`\n"
                f"Packs: {self._format_ai_pack_summary(ai_status['enabled_packs'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Storage Discipline",
            value=(
                "Shield stores config and compact pattern metadata only.\n"
                "Moderator context is delivered to the log channel instead of a heavy moderation archive."
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Use /shield panel, rules, filters, logs, allowlist, ai, or test")

    def _rules_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Shield Rules",
            description="Primary local detections, actions, and repeated-hit escalation.",
            color=ge.EMBED_THEME["info"],
        )
        pack_lines = []
        for pack in ("privacy", "promo", "scam"):
            pack_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | "
                f"Action: `{config[f'{pack}_action']}` | "
                f"Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}"
            )
        embed.add_field(name="Pack Rules", value="\n\n".join(pack_lines), inline=False)
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
            name="Advanced Patterns",
            value=(
                f"{len(config['custom_patterns'])}/{CUSTOM_PATTERN_LIMIT} configured\n"
                "Advanced patterns stay safe-text only. Raw user regex is intentionally unsupported."
            ),
            inline=True,
        )
        embed.add_field(
            name="Quick Use",
            value=(
                "`/shield rules pack:promo enabled:true action:log sensitivity:high`\n"
                "`/shield rules module:true escalation_threshold:3 timeout_minutes:10`\n"
                "`bb!shield advanced list` for safe custom patterns"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Local rules stay authoritative")

    def _scope_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Shield Scope and Allowlists",
            description="Control where Shield scans, who it skips, and what it should not flag.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Scan Scope",
            value=(
                f"Mode: `{config['scan_mode']}`\n"
                f"Include channels: {self._format_mentions(config['included_channel_ids'], kind='channel')}\n"
                f"Exclude channels: {self._format_mentions(config['excluded_channel_ids'], kind='channel')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Members and Roles",
            value=(
                f"Include users: {self._format_mentions(config['included_user_ids'], kind='user')}\n"
                f"Exclude users: {self._format_mentions(config['excluded_user_ids'], kind='user')}\n"
                f"Include roles: {self._format_mentions(config['included_role_ids'], kind='role')}\n"
                f"Exclude roles: {self._format_mentions(config['excluded_role_ids'], kind='role')}\n"
                f"Trusted roles: {self._format_mentions(config['trusted_role_ids'], kind='role')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Allowlists",
            value=(
                f"Domains: {self._format_text_list(config['allow_domains'], limit=6)}\n"
                f"Invite codes: {self._format_text_list(config['allow_invite_codes'], limit=6)}\n"
                f"Phrases: {self._format_text_list(config['allow_phrases'], limit=4)}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value=(
                "`/shield filters mode:only_included`\n"
                "`/shield filters target:trusted_role_ids state:on role:@Mods`\n"
                "`/shield allowlist bucket:allow_domains state:on value:example.com`"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Tune scope before moving beyond log-only")

    def _ai_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        ai_status = self.service.get_ai_status(guild_id)
        embed = discord.Embed(
            title="Shield AI Assist",
            description="Optional second-pass review for already-flagged messages only.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Availability",
            value=(
                f"Server access: {'Allowed' if ai_status['supported'] else 'Not available in this server yet'}\n"
                f"Provider: {ai_status['provider'] or 'Not configured'}\n"
                f"Provider ready: {'Yes' if ai_status['provider_available'] else 'No'}\n"
                f"Status: {ai_status['status']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Runtime Policy",
            value=(
                f"Enabled: **{'Yes' if config['ai_enabled'] else 'No'}**\n"
                f"Local-confidence threshold: `{config['ai_min_confidence']}`\n"
                f"Eligible packs: {self._format_ai_pack_summary(ai_status['enabled_packs'])}\n"
                f"Model: `{ai_status['model'] or 'Not configured'}`"
            ),
            inline=False,
        )
        embed.add_field(
            name="Privacy Boundaries",
            value=(
                "Only already-flagged messages are eligible.\n"
                "Babblebox redacts obvious private patterns, truncates content, and avoids sending broad history or attachment bodies.\n"
                "AI output only enriches moderator alerts. It never directly deletes, times out, or punishes users."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/shield ai enabled:true min_confidence:high privacy:true promo:false scam:true`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield AI | Optional, admin-only, and support-server limited")

    def _logs_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        log_channel = f"<#{config['log_channel_id']}>" if config.get("log_channel_id") else "Not set"
        alert_role = f"<@&{config['alert_role_id']}>" if config.get("alert_role_id") else "None"
        embed = discord.Embed(
            title="Shield Logs",
            description="Moderator delivery stays compact and channel-based.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Log channel: {log_channel}\n"
                f"Alert role: {alert_role}\n"
                "Alerts are deduped so one message does not spam repeated mod notices."
            ),
            inline=False,
        )
        embed.add_field(
            name="What Alerts Include",
            value=(
                "Detection summary, action summary, compact preview, optional attachment summary, and optional AI second-pass note.\n"
                "Babblebox does not keep a heavy deleted-message archive in Shield storage."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/shield logs channel:#shield-log role:@Mods`\n`/shield logs clear_channel:true clear_role:true`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Log-first and compact by design")

    def build_panel_embed(self, guild_id: int, section: str) -> discord.Embed:
        if section == "rules":
            return self._rules_embed(guild_id)
        if section == "scope":
            return self._scope_embed(guild_id)
        if section == "ai":
            return self._ai_embed(guild_id)
        if section == "logs":
            return self._logs_embed(guild_id)
        return self._overview_embed(guild_id)

    async def _send_result(self, ctx: commands.Context, title: str, message: str, *, ok: bool):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Shield"),
            ephemeral=True,
        )

    async def _send_panel(self, ctx: commands.Context, *, section: str = "overview"):
        view = ShieldPanelView(self, guild_id=ctx.guild.id, author_id=ctx.author.id, section=section)
        message = await send_hybrid_response(ctx, embed=view.current_embed(), view=view, ephemeral=True)
        if message is not None:
            view.message = message

    def _resolve_filter_target_id(
        self,
        field: str,
        *,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel],
        role: Optional[discord.Role],
        user: Optional[discord.Member],
    ) -> tuple[bool, int | str]:
        if field.endswith("channel_ids"):
            target_channel = channel or ctx.channel
            channel_id = getattr(target_channel, "id", None)
            return (True, int(channel_id)) if isinstance(channel_id, int) else (False, "Select a channel for that filter target.")
        if field.endswith("role_ids"):
            role_id = getattr(role, "id", None)
            return (True, int(role_id)) if isinstance(role_id, int) else (False, "Select a role for that filter target.")
        user_id = getattr(user, "id", None)
        return (True, int(user_id)) if isinstance(user_id, int) else (False, "Select a member for that filter target.")

    @commands.hybrid_group(
        name="shield",
        with_app_command=True,
        description="Configure Babblebox Shield moderation and safety",
        invoke_without_command=True,
    )
    async def shield_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @shield_group.command(name="status", with_app_command=False)
    async def shield_status_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="panel", with_app_command=True, description="Open the Shield admin panel")
    async def shield_panel_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="rules", with_app_command=True, description="Configure core Shield rules, actions, and escalation")
    @app_commands.describe(
        module="Turn the Shield module on or off",
        pack="Which protection pack to adjust",
        enabled="Turn that pack on or off",
        action="What Shield should do when the pack matches",
        sensitivity="How broad or cautious the pack should be",
        escalation_threshold="Repeated-hit threshold for delete_escalate",
        escalation_window_minutes="Strike window used for delete_escalate",
        timeout_minutes="Timeout length used when escalation or timeout actions fire",
    )
    @app_commands.choices(pack=PACK_CHOICES, action=ACTION_CHOICES, sensitivity=SENSITIVITY_CHOICES)
    async def shield_rules_command(
        self,
        ctx: commands.Context,
        module: Optional[bool] = None,
        pack: Optional[str] = None,
        enabled: Optional[bool] = None,
        action: Optional[str] = None,
        sensitivity: Optional[str] = None,
        escalation_threshold: Optional[int] = None,
        escalation_window_minutes: Optional[int] = None,
        timeout_minutes: Optional[int] = None,
    ):
        if not await self._guard(ctx):
            return
        messages: list[str] = []
        ok = True
        if module is not None:
            module_ok, module_message = await self.service.set_module_enabled(ctx.guild.id, module)
            ok = ok and module_ok
            messages.append(module_message)
        pack_fields_used = any(value is not None for value in (enabled, action, sensitivity))
        if pack_fields_used and pack is None:
            ok = False
            messages.append("Choose a pack when changing pack enabled/action/sensitivity.")
        elif pack is not None:
            pack_ok, pack_message = await self.service.set_pack_config(
                ctx.guild.id,
                pack,
                enabled=enabled,
                action=action,
                sensitivity=sensitivity,
            )
            ok = ok and pack_ok
            messages.append(pack_message)
        if any(value is not None for value in (escalation_threshold, escalation_window_minutes, timeout_minutes)):
            escalation_ok, escalation_message = await self.service.set_escalation(
                ctx.guild.id,
                threshold=escalation_threshold,
                window_minutes=escalation_window_minutes,
                timeout_minutes=timeout_minutes,
            )
            ok = ok and escalation_ok
            messages.append(escalation_message)
        if not messages:
            await send_hybrid_response(ctx, embed=self._rules_embed(ctx.guild.id), ephemeral=True)
            return
        await self._send_result(ctx, "Shield Rules", "\n".join(messages), ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="logs", with_app_command=True, description="Configure Shield log delivery")
    @app_commands.describe(channel="Channel for Shield alerts", role="Optional role to ping for alerts", clear_channel="Clear the current log channel", clear_role="Clear the current alert role")
    async def shield_logs_command(
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
            channel_ok, channel_message = await self.service.set_log_channel(ctx.guild.id, None if clear_channel else channel.id)
            ok = ok and channel_ok
            messages.append(channel_message)
        if role is not None or clear_role:
            role_ok, role_message = await self.service.set_alert_role(ctx.guild.id, None if clear_role else role.id)
            ok = ok and role_ok
            messages.append(role_message)
        if not messages:
            await send_hybrid_response(ctx, embed=self._logs_embed(ctx.guild.id), ephemeral=True)
            return
        await self._send_result(ctx, "Shield Logs", "\n".join(messages), ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="filters", with_app_command=True, description="Configure Shield scope, includes, excludes, and trusted roles")
    @app_commands.describe(
        mode="Scan everything eligible or only explicitly included scope",
        target="Which include/exclude/trust bucket to change",
        state="Turn that filter on or off",
        channel="Channel target for channel-based filters",
        role="Role target for role-based filters",
        user="Member target for user-based filters",
    )
    @app_commands.choices(mode=SCAN_MODE_CHOICES, target=FILTER_TARGET_CHOICES, state=STATE_CHOICES)
    async def shield_filters_command(
        self,
        ctx: commands.Context,
        mode: Optional[str] = None,
        target: Optional[str] = None,
        state: Optional[str] = None,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
        user: Optional[discord.Member] = None,
    ):
        if not await self._guard(ctx):
            return
        messages: list[str] = []
        ok = True
        if mode is not None:
            mode_ok, mode_message = await self.service.set_scan_mode(ctx.guild.id, mode)
            ok = ok and mode_ok
            messages.append(mode_message)
        if target is not None or state is not None:
            if target is None or state is None:
                ok = False
                messages.append("Choose both a filter target and on/off state when changing scope filters.")
            else:
                resolved, target_id_or_message = self._resolve_filter_target_id(
                    target,
                    ctx=ctx,
                    channel=channel,
                    role=role,
                    user=user,
                )
                if not resolved:
                    ok = False
                    messages.append(str(target_id_or_message))
                else:
                    filter_ok, filter_message = await self.service.set_filter_target(
                        ctx.guild.id,
                        target,
                        int(target_id_or_message),
                        state == "on",
                    )
                    ok = ok and filter_ok
                    messages.append(filter_message)
        if not messages:
            await send_hybrid_response(ctx, embed=self._scope_embed(ctx.guild.id), ephemeral=True)
            return
        await self._send_result(ctx, "Shield Filters", "\n".join(messages), ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="allowlist", with_app_command=True, description="Configure Shield allowlists for domains, invite codes, and phrases")
    @app_commands.describe(bucket="Which allowlist bucket to change", state="Turn this allowlist entry on or off", value="The domain, invite code, or phrase to change")
    @app_commands.choices(bucket=ALLOWLIST_BUCKET_CHOICES, state=STATE_CHOICES)
    async def shield_allowlist_command(
        self,
        ctx: commands.Context,
        bucket: Optional[str] = None,
        state: Optional[str] = None,
        value: Optional[str] = None,
    ):
        if not await self._guard(ctx):
            return
        if bucket is None and state is None and value is None:
            await send_hybrid_response(ctx, embed=self._scope_embed(ctx.guild.id), ephemeral=True)
            return
        if bucket is None or state is None or value is None:
            await self._send_result(
                ctx,
                "Shield Allowlists",
                "Choose a bucket, on/off state, and value when changing allowlists.",
                ok=False,
            )
            return
        ok, message = await self.service.set_allow_entry(ctx.guild.id, bucket, value, state == "on")
        await self._send_result(ctx, "Shield Allowlists", message, ok=ok)

    @app_commands.default_permissions(manage_guild=True)
    @shield_group.command(name="ai", with_app_command=True, description="Configure optional Shield AI second-pass review")
    @app_commands.describe(
        enabled="Turn Shield AI second-pass review on or off",
        min_confidence="Minimum local Shield confidence needed before AI review is attempted",
        privacy="Allow AI review for privacy-pack hits",
        promo="Allow AI review for promo-pack hits",
        scam="Allow AI review for scam-pack hits",
    )
    @app_commands.choices(min_confidence=AI_CONFIDENCE_CHOICES)
    async def shield_ai_command(
        self,
        ctx: commands.Context,
        enabled: Optional[bool] = None,
        min_confidence: Optional[str] = None,
        privacy: Optional[bool] = None,
        promo: Optional[bool] = None,
        scam: Optional[bool] = None,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (enabled, min_confidence, privacy, promo, scam)):
            await send_hybrid_response(ctx, embed=self._ai_embed(ctx.guild.id), ephemeral=True)
            return
        if not self.service.is_ai_supported_guild(ctx.guild.id):
            await self._send_result(ctx, "Shield AI", "AI review is not available in this server yet.", ok=False)
            return
        current = self.service.get_config(ctx.guild.id)
        next_packs = list(current.get("ai_enabled_packs", []))
        for pack, state in (("privacy", privacy), ("promo", promo), ("scam", scam)):
            if state is None:
                continue
            if state and pack not in next_packs:
                next_packs.append(pack)
            if not state and pack in next_packs:
                next_packs.remove(pack)
        ok, message = await self.service.set_ai_config(
            ctx.guild.id,
            enabled=enabled,
            min_confidence=min_confidence,
            enabled_packs=next_packs if any(value is not None for value in (privacy, promo, scam)) else None,
        )
        await self._send_result(ctx, "Shield AI", message, ok=ok)

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

    @shield_group.group(
        name="advanced",
        with_app_command=False,
        invoke_without_command=True,
        description="Safe advanced matching with contains, whole-word, and wildcard patterns",
    )
    async def shield_advanced_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._rules_embed(ctx.guild.id), ephemeral=True)

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
            embed.description = "\n".join(
                f"`{item['pattern_id']}` | **{item['label']}** | `{item['mode']}` | `{item['action']}` | `{item['pattern']}`"
                for item in patterns[:10]
            )
        await send_hybrid_response(ctx, embed=ge.style_embed(embed, footer="Babblebox Shield | Safe patterns only"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ShieldCog(bot))
