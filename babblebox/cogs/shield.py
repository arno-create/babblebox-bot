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
    MATCH_CLASS_LABELS,
    PACK_LABELS,
    SENSITIVITY_LABELS,
    ShieldService,
)


PACK_CHOICES = [
    app_commands.Choice(name="Privacy Leak", value="privacy"),
    app_commands.Choice(name="Promo / Invite", value="promo"),
    app_commands.Choice(name="Scam / Malicious Links", value="scam"),
    app_commands.Choice(name="Adult / 18+ Links", value="adult"),
]
ACTION_CHOICES = [
    app_commands.Choice(name="Detect only", value="detect"),
    app_commands.Choice(name="Log only", value="log"),
    app_commands.Choice(name="Delete + log", value="delete_log"),
    app_commands.Choice(name="Delete + log + escalate", value="delete_escalate"),
    app_commands.Choice(name="Timeout + log", value="timeout_log"),
]
LOW_ACTION_CHOICES = [
    app_commands.Choice(name="Detect only", value="detect"),
    app_commands.Choice(name="Log only", value="log"),
]
MEDIUM_ACTION_CHOICES = [
    app_commands.Choice(name="Detect only", value="detect"),
    app_commands.Choice(name="Log only", value="log"),
    app_commands.Choice(name="Delete + log", value="delete_log"),
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
SHIELD_AI_OVERRIDE_OWNER_IDS = {1266444952779620413, 1345860619836063754}


class ShieldPanelView(discord.ui.View):
    def __init__(self, cog: "ShieldCog", *, guild_id: int, author_id: int, channel_id: int | None = None, section: str = "overview"):
        super().__init__(timeout=180)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.channel_id = channel_id
        self.section = section
        self.message: discord.Message | None = None
        self._refresh_buttons()

    def current_embed(self) -> discord.Embed:
        return self.cog.build_panel_embed(self.guild_id, self.section, channel_id=self.channel_id)

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

    def _pack_policy_actions(self, config: dict[str, object], pack: str) -> tuple[str, str, str]:
        return (
            str(config.get(f"{pack}_low_action", "log")),
            str(config.get(f"{pack}_medium_action", "log")),
            str(config.get(f"{pack}_high_action", "log")),
        )

    def _pack_policy_compact(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        return f"Low / Medium / High: `{low_action}` / `{medium_action}` / `{high_action}`"

    def _pack_policy_detail(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        return (
            f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | "
            f"Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}\n"
            f"Low action: `{low_action}`\n"
            f"Medium action: `{medium_action}`\n"
            f"High action: `{high_action}`"
        )

    def _link_assessment_label(self, assessment) -> str:
        if assessment.category == "malicious":
            return "malicious | matched local intel"
        if assessment.category == "adult":
            return "adult | matched local intel"
        if assessment.category == "unknown_suspicious":
            if assessment.provider_lookup_warranted:
                return "unknown suspicious | lookup candidate only, no action"
            return "unknown suspicious | local caution only, no action"
        if assessment.category == "unknown":
            return "unknown | no action"
        return "safe | allowlisted or safe family"

    def _is_override_owner(self, user_id: int) -> bool:
        return user_id in SHIELD_AI_OVERRIDE_OWNER_IDS

    def _build_ai_override_embed(self, *, title: str, note: str) -> discord.Embed:
        meta = self.service.get_meta()
        state_label = "On" if meta["global_ai_override_enabled"] else "Off"
        updated_by = meta["global_ai_override_updated_by"]
        updated_at = meta["global_ai_override_updated_at"] or "Never"
        updated_by_label = f"`{updated_by}`" if updated_by is not None else "`None`"
        embed = discord.Embed(
            title=title,
            description=note,
            color=ge.EMBED_THEME["accent"] if meta["global_ai_override_enabled"] else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Global Override",
            value=(
                f"State: **{state_label}**\n"
                f"Support server baseline: Always available there\n"
                f"Last updated by: {updated_by_label}\n"
                f"Last updated at: `{updated_at}`"
            ),
            inline=False,
        )
        embed.set_footer(text="Babblebox Shield AI | DM-only maintainer control")
        return embed

    def _guild_from_bot(self, guild_id: int) -> discord.Guild | None:
        get_guild = getattr(self.bot, "get_guild", None)
        if callable(get_guild):
            return get_guild(guild_id)
        return None

    def _bot_member(self, guild: discord.Guild | None):
        if guild is None:
            return None
        me = getattr(guild, "me", None)
        if me is not None:
            return me
        bot_user = getattr(self.bot, "user", None)
        get_member = getattr(guild, "get_member", None)
        if bot_user is not None and callable(get_member):
            return get_member(getattr(bot_user, "id", 0))
        return None

    def _channel_from_guild(self, guild: discord.Guild | None, channel_id: int | None):
        if guild is None or channel_id is None:
            return None
        get_channel = getattr(guild, "get_channel", None)
        if callable(get_channel):
            channel = get_channel(channel_id)
            if channel is not None:
                return channel
        bot_get_channel = getattr(self.bot, "get_channel", None)
        if callable(bot_get_channel):
            return bot_get_channel(channel_id)
        return None

    def _shield_operability_lines(self, guild_id: int, *, channel_id: int | None = None) -> list[str]:
        guild = self._guild_from_bot(guild_id)
        me = self._bot_member(guild)
        if guild is None or me is None:
            return []

        config = self.service.get_config(guild_id)
        seen: set[str] = set()
        lines: list[str] = []

        def add(line: str):
            if line not in seen:
                seen.add(line)
                lines.append(line)

        delete_actions_enabled = any(
            config.get(f"{pack}_enabled")
            and bool(set(self._pack_policy_actions(config, pack)).intersection({"delete_log", "delete_escalate"}))
            for pack in ("privacy", "promo", "scam", "adult")
        )
        timeout_actions_enabled = any(
            config.get(f"{pack}_enabled")
            and bool(set(self._pack_policy_actions(config, pack)).intersection({"timeout_log", "delete_escalate"}))
            for pack in ("privacy", "promo", "scam", "adult")
        )

        focus_channel = self._channel_from_guild(guild, channel_id)
        if focus_channel is not None:
            permissions = focus_channel.permissions_for(me)
            channel_label = getattr(focus_channel, "mention", "#this-channel")
            if delete_actions_enabled and not getattr(permissions, "manage_messages", False):
                add(f"Warning: Shield can't delete messages in {channel_label} because I'm missing Manage Messages.")
            if timeout_actions_enabled and not getattr(permissions, "moderate_members", False):
                add(f"Warning: Timeout actions can't run in {channel_label} because I'm missing Moderate Members.")

        if timeout_actions_enabled:
            add("Note: Timeout actions still cannot affect administrators or members whose top role is at or above mine.")

        log_channel_id = config.get("log_channel_id")
        if isinstance(log_channel_id, int):
            log_channel = self._channel_from_guild(guild, log_channel_id)
            if log_channel is None:
                add("Warning: Shield logging won't work because the configured log channel is missing or I can't access it.")
            else:
                log_permissions = log_channel.permissions_for(me)
                log_label = getattr(log_channel, "mention", f"<#{log_channel_id}>")
                if not getattr(log_permissions, "view_channel", False):
                    add(f"Warning: Shield logging won't work in {log_label} because I'm missing View Channel.")
                if not getattr(log_permissions, "send_messages", False):
                    add(f"Warning: Shield logging won't work in {log_label} because I'm missing Send Messages.")
                if not getattr(log_permissions, "embed_links", False):
                    add(f"Warning: Shield logging won't work in {log_label} because I'm missing Embed Links.")

        return lines

    def _add_operability_field(self, embed: discord.Embed, guild_id: int, *, channel_id: int | None = None) -> discord.Embed:
        lines = self._shield_operability_lines(guild_id, channel_id=channel_id)
        if lines:
            embed.add_field(name="Operability", value="\n".join(lines[:6]), inline=False)
        return embed

    def _overview_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        ai_status = self.service.get_ai_status(guild_id)
        embed = discord.Embed(
            title="Shield Control Panel",
            description="Shield stays local-first. Bundled intel handles malicious/scam and optional adult / 18+ domains, safe mainstream families bypass suspicion, and unknown suspicious links stay non-enforcing in this phase.",
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
        protection_lines = []
        for pack in ("privacy", "promo"):
            protection_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}\n"
                f"{self._pack_policy_compact(config, pack)}"
            )
        embed.add_field(name="Protection Packs", value="\n\n".join(protection_lines), inline=False)
        link_safety_lines = []
        for pack in ("scam", "adult"):
            link_safety_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}\n"
                f"{self._pack_policy_compact(config, pack)}"
            )
        embed.add_field(name="Link Safety", value="\n\n".join(link_safety_lines), inline=False)
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
            description="Confidence-tier local policy. Local malicious and adult matches can act hard; unknown suspicious links remain advisory in phase 1.",
            color=ge.EMBED_THEME["info"],
        )
        pack_lines = []
        for pack in ("privacy", "promo"):
            pack_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"{self._pack_policy_detail(config, pack)}"
            )
        embed.add_field(name="Low / Medium / High Action Policy", value="\n\n".join(pack_lines), inline=False)
        link_safety_lines = []
        for pack in ("scam", "adult"):
            link_safety_lines.append(
                f"**{PACK_LABELS[pack]}**\n"
                f"{self._pack_policy_detail(config, pack)}"
            )
        embed.add_field(name="Link Safety Policy", value="\n\n".join(link_safety_lines), inline=False)
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
                "`/shield rules pack:promo enabled:true low_action:log medium_action:delete_log high_action:delete_escalate sensitivity:high`\n"
                "`/shield rules pack:adult enabled:true low_action:log medium_action:delete_log high_action:delete_log`\n"
                "`/shield rules module:true escalation_threshold:3 timeout_minutes:10`\n"
                "`bb!shield advanced list` for safe custom patterns"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Shield | Low / Medium / High policy stays local and explicit")

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
                "Default rollout: Support server\n"
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
                "Detection class, confidence, resolved action, compact preview, optional attachment summary, and optional AI second-pass note.\n"
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

    def build_panel_embed(self, guild_id: int, section: str, *, channel_id: int | None = None) -> discord.Embed:
        if section == "rules":
            embed = self._rules_embed(guild_id)
        elif section == "scope":
            embed = self._scope_embed(guild_id)
        elif section == "ai":
            embed = self._ai_embed(guild_id)
        elif section == "logs":
            embed = self._logs_embed(guild_id)
        else:
            embed = self._overview_embed(guild_id)
        return self._add_operability_field(embed, guild_id, channel_id=channel_id)

    async def _send_result(self, ctx: commands.Context, title: str, message: str, *, ok: bool):
        embed = ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Shield")
        channel_id = getattr(ctx.channel, "id", None) if ctx.guild is not None else None
        self._add_operability_field(embed, ctx.guild.id, channel_id=channel_id)
        await send_hybrid_response(ctx, embed=embed, ephemeral=True)

    async def _send_panel(self, ctx: commands.Context, *, section: str = "overview"):
        channel_id = getattr(ctx.channel, "id", None) if ctx.guild is not None else None
        view = ShieldPanelView(self, guild_id=ctx.guild.id, author_id=ctx.author.id, channel_id=channel_id, section=section)
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

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
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

    @shield_group.command(name="panel", with_app_command=True, description="Open the Shield admin panel")
    async def shield_panel_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await self._send_panel(ctx, section="overview")

    @shield_group.command(name="rules", with_app_command=True, description="Configure core Shield rules, actions, and escalation")
    @app_commands.describe(
        module="Turn the Shield module on or off",
        pack="Which protection pack to adjust",
        enabled="Turn that pack on or off",
        action="Shorthand to use one graduated policy derived from a single action",
        low_action="Action for broad or uncertain low-confidence matches",
        medium_action="Action for medium-confidence matches",
        high_action="Action for high-confidence matches",
        sensitivity="How broad or cautious the pack should be",
        escalation_threshold="Repeated-hit threshold for delete_escalate",
        escalation_window_minutes="Strike window used for delete_escalate",
        timeout_minutes="Timeout length used when escalation or timeout actions fire",
    )
    @app_commands.choices(
        pack=PACK_CHOICES,
        action=ACTION_CHOICES,
        low_action=LOW_ACTION_CHOICES,
        medium_action=MEDIUM_ACTION_CHOICES,
        high_action=ACTION_CHOICES,
        sensitivity=SENSITIVITY_CHOICES,
    )
    async def shield_rules_command(
        self,
        ctx: commands.Context,
        module: Optional[bool] = None,
        pack: Optional[str] = None,
        enabled: Optional[bool] = None,
        action: Optional[str] = None,
        low_action: Optional[str] = None,
        medium_action: Optional[str] = None,
        high_action: Optional[str] = None,
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
        pack_fields_used = any(value is not None for value in (enabled, action, low_action, medium_action, high_action, sensitivity))
        if pack_fields_used and pack is None:
            ok = False
            messages.append("Choose a pack when changing pack enabled, action policy, or sensitivity.")
        elif pack is not None:
            pack_ok, pack_message = await self.service.set_pack_config(
                ctx.guild.id,
                pack,
                enabled=enabled,
                action=action,
                low_action=low_action,
                medium_action=medium_action,
                high_action=high_action,
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

    @commands.command(name="shieldaiglobal", hidden=True)
    async def shield_ai_global_override_command(self, ctx: commands.Context, mode: str = "status"):
        if ctx.guild is not None:
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_override_owner(author_id):
            print(f"Shield AI override denied: unauthorized_dm_user_id={author_id}")
            await ctx.send(content="That command is unavailable.")
            return
        normalized_mode = str(mode or "status").strip().lower()
        if normalized_mode not in {"status", "on", "off"}:
            await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Override", note="Use `status`, `on`, or `off`."))
            return
        if normalized_mode == "status":
            await ctx.send(
                embed=self._build_ai_override_embed(
                    title="Shield AI Override",
                    note="Private maintainer status for the global Shield AI rollout override.",
                )
            )
            return
        ok, message = await self.service.set_global_ai_override(normalized_mode == "on", actor_id=author_id)
        await ctx.send(
            embed=self._build_ai_override_embed(
                title="Shield AI Override",
                note=message if ok else f"Override update failed: {message}",
            )
        )

    @shield_group.command(name="test", with_app_command=True, description="Dry-run a message through the current Shield rules")
    async def shield_test_command(self, ctx: commands.Context, text: str):
        if not await self._guard(ctx):
            return
        result = self.service.test_message_details(ctx.guild.id, text)
        embed = discord.Embed(title="Shield Test", description="Dry-run results for the current configuration.", color=ge.EMBED_THEME["info"])
        if result.bypass_reason:
            embed.add_field(name="Bypass", value=result.bypass_reason, inline=False)
        if not result.matches:
            embed.add_field(name="Result", value="No Shield pack matched that sample.", inline=False)
        else:
            embed.add_field(
                name="Matches",
                value="\n".join(
                    f"**{PACK_LABELS.get(item.pack, item.pack.title())}** | {item.label} | "
                    f"{MATCH_CLASS_LABELS.get(item.match_class, item.match_class.replace('_', ' ').title() if item.match_class else 'Match')} | "
                    f"`{item.action}` | {item.confidence}"
                    for item in result.matches[:5]
                ),
                inline=False,
            )
        if result.link_assessments:
            embed.add_field(
                name="Link Safety",
                value="\n".join(
                    f"`{item.normalized_domain}` | {self._link_assessment_label(item)} | "
                    f"signals: {', '.join(item.matched_signals[:4]) if item.matched_signals else 'none'}"
                    for item in result.link_assessments[:5]
                ),
                inline=False,
            )
        channel_id = getattr(ctx.channel, "id", None) if ctx.guild is not None else None
        self._add_operability_field(embed, ctx.guild.id, channel_id=channel_id)
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
