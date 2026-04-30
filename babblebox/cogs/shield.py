from __future__ import annotations

import contextlib
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox.app_command_hardening import harden_admin_root_group
from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.premium_models import SYSTEM_PREMIUM_OWNER_USER_IDS
from babblebox.premium_service import format_saved_state_status, preserved_over_limit_note
from babblebox.runtime_health import bind_started_service
from babblebox.shield_ai import SHIELD_AI_SUPPORT_GUILD_ID, format_shield_ai_model_list
from babblebox.shield_service import (
    ACTION_LABELS,
    CONFIDENCE_LABELS,
    MATCH_CLASS_LABELS,
    PACK_LABELS,
    SEVERE_CATEGORY_LABELS,
    SENSITIVITY_LABELS,
    ShieldService,
)
from babblebox.shield_store import SHIELD_NUMERIC_CONFIG_SPECS


LOGGER = logging.getLogger(__name__)

PACK_CHOICES = [
    app_commands.Choice(name="Privacy Leak", value="privacy"),
    app_commands.Choice(name="Promo / Invite", value="promo"),
    app_commands.Choice(name="Scam / Malicious Links", value="scam"),
    app_commands.Choice(name="Anti-Spam", value="spam"),
    app_commands.Choice(name="GIF Flood / Media Pressure", value="gif"),
    app_commands.Choice(name="Adult Links + Solicitation", value="adult"),
    app_commands.Choice(name="Severe Harm / Hate", value="severe"),
]
ACTION_CHOICES = [
    app_commands.Choice(name="Detect only", value="detect"),
    app_commands.Choice(name="Log only", value="log"),
    app_commands.Choice(name="Delete + log", value="delete_log"),
    app_commands.Choice(name="Delete + Timeout + log", value="delete_timeout_log"),
    app_commands.Choice(name="Delete + log + escalate", value="delete_escalate"),
    app_commands.Choice(name="Timeout + log (keep message)", value="timeout_log"),
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
    app_commands.Choice(name="Relax solicitation in current/selected channel", value="adult_solicitation_excluded_channel_ids"),
    app_commands.Choice(name="Include member", value="included_user_ids"),
    app_commands.Choice(name="Exclude member", value="excluded_user_ids"),
    app_commands.Choice(name="Include role", value="included_role_ids"),
    app_commands.Choice(name="Exclude role", value="excluded_role_ids"),
    app_commands.Choice(name="Trust role", value="trusted_role_ids"),
]
PACK_EXEMPTION_TARGET_CHOICES = [
    app_commands.Choice(name="Channel", value="channel"),
    app_commands.Choice(name="Role", value="role"),
    app_commands.Choice(name="Member", value="user"),
]
SPAM_MODERATOR_POLICY_CHOICES = [
    app_commands.Choice(name="Exempt moderators", value="exempt"),
    app_commands.Choice(name="Delete only", value="delete_only"),
    app_commands.Choice(name="Full policy", value="full"),
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
LINK_POLICY_MODE_CHOICES = [
    app_commands.Choice(name="Default", value="default"),
    app_commands.Choice(name="Trusted Links Only", value="trusted_only"),
]
LOG_STYLE_CHOICES = [
    app_commands.Choice(name="Adaptive", value="adaptive"),
    app_commands.Choice(name="Compact", value="compact"),
]
LOG_PING_MODE_CHOICES = [
    app_commands.Choice(name="Smart", value="smart"),
    app_commands.Choice(name="Never Ping", value="never"),
]
PACK_LOG_STYLE_CHOICES = [
    app_commands.Choice(name="Inherit", value="inherit"),
    app_commands.Choice(name="Adaptive", value="adaptive"),
    app_commands.Choice(name="Compact", value="compact"),
]
PACK_LOG_PING_CHOICES = [
    app_commands.Choice(name="Inherit", value="inherit"),
    app_commands.Choice(name="Smart", value="smart"),
    app_commands.Choice(name="Never Ping", value="never"),
]
SEVERE_CATEGORY_CHOICES = [
    app_commands.Choice(name="Sexual Exploitation", value="sexual_exploitation"),
    app_commands.Choice(name="Self-Harm Encouragement", value="self_harm_encouragement"),
    app_commands.Choice(name="Eliminationist Hate", value="eliminationist_hate"),
    app_commands.Choice(name="Severe Slur Abuse", value="severe_slur_abuse"),
]
SEVERE_TERM_ACTION_CHOICES = [
    app_commands.Choice(name="Add custom", value="add"),
    app_commands.Choice(name="Disable bundled", value="remove_default"),
    app_commands.Choice(name="Restore bundled", value="restore_default"),
    app_commands.Choice(name="Remove custom", value="remove_custom"),
]
RULE_PANEL_PACKS = ("privacy", "promo", "scam", "spam", "gif", "adult", "severe")
CORE_RULE_PANEL_PACKS = ("privacy", "promo", "spam", "gif")
HIGH_RISK_RULE_PANEL_PACKS = ("scam", "adult", "severe")
PACK_PANEL_DESCRIPTIONS = {
    "privacy": "Catches phone numbers, email drops, payment handles, and similar private-info leaks.",
    "promo": "Handles invite spam, promo blasts, and repetitive self-promotion without overfiring on normal info links.",
    "scam": "Catches malicious domains, impersonation hosts, and suspicious lure patterns, including no-link money, wins, and picks bait routed into private follow-up.",
    "spam": "Handles rate spam, duplicate floods, optional emoji clutter, capitals spam, low-value chatter, and moderator handling for live-message raids.",
    "gif": "Controls one-user GIF floods, channel-wide streaks, pressure slices, and lightweight meaningful-text balance without polluting unrelated packs.",
    "adult": "Blocks adult domains and can optionally catch DM-gated adult solicitation text with a bounded carve-out lane.",
    "severe": "Targets explicit severe harm, self-harm encouragement, eliminationist or dehumanizing hate, and server-specific severe phrase tuning with reference-aware suppressors.",
    "link_policy": "Controls the separate trusted-link policy lane for broad link posting without weakening hard malicious intel.",
}
TIMEOUT_PRESET_CHOICES = (
    ("inherit", "Inherit global timeout"),
    ("5", "5 minutes"),
    ("10", "10 minutes"),
    ("15", "15 minutes"),
    ("30", "30 minutes"),
    ("60", "60 minutes"),
)
def _threshold_values(field: str, *, include_max: bool = False) -> tuple[int, ...]:
    minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS[field]
    stop = maximum + 1 if include_max else maximum
    return tuple(range(minimum, stop))


SPAM_RATE_THRESHOLDS = _threshold_values("spam_message_threshold", include_max=True)
SPAM_RATE_WINDOWS = (3, 4, 5, 6, 8, 10, 12, 15, 20, 30)
SPAM_BURST_THRESHOLDS = _threshold_values("spam_burst_threshold", include_max=True)
SPAM_BURST_WINDOWS = (5, 6, 8, 10, 12, 15, 20, 25, 30)
SPAM_DUPLICATE_THRESHOLDS = _threshold_values("spam_near_duplicate_threshold", include_max=True)
SPAM_DUPLICATE_WINDOWS = (5, 6, 8, 10, 12, 15, 20, 25, 30, 45)
SPAM_EMOTE_THRESHOLDS = (8, 10, 12, 15, 18, 24, 30, 36, 40)
SPAM_CAPS_THRESHOLDS = (12, 16, 20, 24, 28, 36, 48, 60, 80)
SPAM_LOW_VALUE_THRESHOLDS = _threshold_values("spam_low_value_threshold", include_max=True)
SPAM_LOW_VALUE_WINDOWS = (20, 30, 45, 60, 90, 120)
GIF_RATE_THRESHOLDS = _threshold_values("gif_message_threshold", include_max=True)
GIF_RATE_WINDOWS = (3, 5, 8, 10, 12, 15, 20, 25, 30, 45)
GIF_REPEAT_THRESHOLDS = _threshold_values("gif_repeat_threshold", include_max=True)
GIF_SAME_ASSET_THRESHOLDS = _threshold_values("gif_same_asset_threshold", include_max=True)
GIF_RATIO_THRESHOLDS = (50, 55, 60, 65, 70, 75, 80, 85, 90, 95)
GIF_CONSECUTIVE_THRESHOLDS = _threshold_values("gif_consecutive_threshold", include_max=True)

SPAM_OPTION_LANES = (
    {
        "key": "rate",
        "label": "Rate",
        "enabled_field": "spam_message_enabled",
        "enabled_arg": "message_enabled",
        "threshold_field": "spam_message_threshold",
        "threshold_arg": "message_threshold",
        "threshold_values": SPAM_RATE_THRESHOLDS,
        "threshold_placeholder": "Rate message count",
        "secondary_field": "spam_message_window_seconds",
        "secondary_arg": "window_seconds",
        "secondary_values": SPAM_RATE_WINDOWS,
        "secondary_placeholder": "Rate window",
    },
    {
        "key": "burst",
        "label": "Burst",
        "enabled_field": "spam_burst_enabled",
        "enabled_arg": "burst_enabled",
        "threshold_field": "spam_burst_threshold",
        "threshold_arg": "burst_threshold",
        "threshold_values": SPAM_BURST_THRESHOLDS,
        "threshold_placeholder": "Burst message count",
        "secondary_field": "spam_burst_window_seconds",
        "secondary_arg": "burst_window_seconds",
        "secondary_values": SPAM_BURST_WINDOWS,
        "secondary_placeholder": "Burst window",
    },
    {
        "key": "near_duplicate",
        "label": "Near-duplicate",
        "enabled_field": "spam_near_duplicate_enabled",
        "enabled_arg": "near_duplicate_enabled",
        "threshold_field": "spam_near_duplicate_threshold",
        "threshold_arg": "duplicate_threshold",
        "threshold_values": SPAM_DUPLICATE_THRESHOLDS,
        "threshold_placeholder": "Near-duplicate count",
        "secondary_field": "spam_near_duplicate_window_seconds",
        "secondary_arg": "duplicate_window_seconds",
        "secondary_values": SPAM_DUPLICATE_WINDOWS,
        "secondary_placeholder": "Near-duplicate window",
    },
    {
        "key": "emote",
        "label": "Emoji / emote",
        "enabled_field": "spam_emote_enabled",
        "enabled_arg": "emote_enabled",
        "threshold_field": "spam_emote_threshold",
        "threshold_arg": "emote_threshold",
        "threshold_values": SPAM_EMOTE_THRESHOLDS,
        "threshold_placeholder": "Emoji / emote threshold",
    },
    {
        "key": "caps",
        "label": "Capitals",
        "enabled_field": "spam_caps_enabled",
        "enabled_arg": "caps_enabled",
        "threshold_field": "spam_caps_threshold",
        "threshold_arg": "caps_threshold",
        "threshold_values": SPAM_CAPS_THRESHOLDS,
        "threshold_placeholder": "Capitals threshold",
    },
    {
        "key": "low_value",
        "label": "Low-value chatter",
        "enabled_field": "spam_low_value_enabled",
        "enabled_arg": "low_value_enabled",
        "threshold_field": "spam_low_value_threshold",
        "threshold_arg": "low_value_threshold",
        "threshold_values": SPAM_LOW_VALUE_THRESHOLDS,
        "threshold_placeholder": "Low-value message count",
        "secondary_field": "spam_low_value_window_seconds",
        "secondary_arg": "low_value_window_seconds",
        "secondary_values": SPAM_LOW_VALUE_WINDOWS,
        "secondary_placeholder": "Low-value window",
    },
)

GIF_OPTION_LANES = (
    {
        "key": "rate",
        "label": "GIF-heavy rate",
        "enabled_field": "gif_message_enabled",
        "enabled_arg": "message_enabled",
        "threshold_field": "gif_message_threshold",
        "threshold_arg": "message_threshold",
        "threshold_values": GIF_RATE_THRESHOLDS,
        "threshold_placeholder": "GIF-heavy rate count",
        "secondary_field": "gif_window_seconds",
        "secondary_arg": "window_seconds",
        "secondary_values": GIF_RATE_WINDOWS,
        "secondary_placeholder": "GIF-heavy rate window",
    },
    {
        "key": "consecutive",
        "label": "True channel streak",
        "enabled_field": "gif_consecutive_enabled",
        "enabled_arg": "consecutive_enabled",
        "threshold_field": "gif_consecutive_threshold",
        "threshold_arg": "consecutive_threshold",
        "threshold_values": GIF_CONSECUTIVE_THRESHOLDS,
        "threshold_placeholder": "Consecutive GIF streak threshold",
    },
    {
        "key": "repeat",
        "label": "Low-text repeat",
        "enabled_field": "gif_repeat_enabled",
        "enabled_arg": "repeat_enabled",
        "threshold_field": "gif_repeat_threshold",
        "threshold_arg": "repeat_threshold",
        "threshold_values": GIF_REPEAT_THRESHOLDS,
        "threshold_placeholder": "Low-text repeat threshold",
    },
    {
        "key": "same_asset",
        "label": "Same asset",
        "enabled_field": "gif_same_asset_enabled",
        "enabled_arg": "same_asset_enabled",
        "threshold_field": "gif_same_asset_threshold",
        "threshold_arg": "same_asset_threshold",
        "threshold_values": GIF_SAME_ASSET_THRESHOLDS,
        "threshold_placeholder": "Same-asset threshold",
    },
)


class ShieldManagedView(discord.ui.View):
    panel_title = "Shield Panel"
    stale_message = "That Shield panel expired. Run `/shield panel` again to open a fresh one."

    def __init__(self, cog: "ShieldCog", *, guild_id: int, author_id: int, timeout: float | None = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.message: discord.Message | None = None
        self._expired = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self._expired or interaction.is_expired():
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return False
        if interaction.user.id != self.author_id:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/shield panel` to open your own Shield admin panel.",
                    tone="info",
                    footer="Babblebox Shield",
                ),
            )
            return False
        if not self.cog.user_can_manage_shield(interaction.user):
            _allowed, reason = self.cog.shield_access_reason(interaction.user, self.guild_id)
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Admin Only",
                    reason,
                    tone="warning",
                    footer="Babblebox Shield",
                ),
            )
            return False
        return True

    async def on_timeout(self):
        self._expired = True
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException):
                await self.message.edit(view=self)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await self.cog._send_private_interaction(
            interaction,
            embed=self.cog._shield_status_embed(
                self.panel_title,
                "Babblebox could not finish that Shield panel action. Run `/shield panel` again if this panel feels stale.",
                ok=False,
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
            if not await self.cog._defer_component_interaction(
                interaction,
                stage=stage,
                failure_title=self.panel_title,
                failure_message=failure_message,
                guild_id=self.guild_id,
            ):
                return None
            return await action()
        except Exception:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, failure_message, ok=False),
            )
            return None


class ShieldPanelView(ShieldManagedView):
    panel_title = "Shield Panel"
    stale_message = "That Shield panel expired. Run `/shield panel` again to open a fresh one."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        channel_id: int | None = None,
        section: str = "overview",
        selected_pack: str | None = None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.channel_id = channel_id
        self.section = section
        self.selected_pack = selected_pack or self.cog._default_rule_pack(guild_id)
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog.build_panel_embed(
            self.guild_id,
            self.section,
            channel_id=self.channel_id,
            selected_pack=self.selected_pack,
        )

    async def refresh_message(self):
        self._refresh_items()
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException, discord.NotFound):
                await self.message.edit(embed=self.current_embed(), view=self)

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    async def _switch_section(self, interaction: discord.Interaction, section: str):
        async def action():
            self.section = section
            await self._rerender(interaction)

        await self._safe_action(
            interaction,
            stage=f"shield_panel_{section}",
            failure_message="Babblebox could not refresh that Shield panel section right now.",
            action=action,
        )

    async def _open_pack_editor(self, interaction: discord.Interaction, editor_kind: str):
        pack_label = PACK_LABELS.get(self.selected_pack, self.selected_pack.replace("_", " ").title())

        async def action():
            if editor_kind == "actions":
                view: ShieldManagedView = ShieldPackActionEditorView(
                    self.cog,
                    guild_id=self.guild_id,
                    author_id=self.author_id,
                    pack=self.selected_pack,
                    panel_view=self,
                )
            elif editor_kind == "options":
                view = ShieldPackOptionsEditorView(
                    self.cog,
                    guild_id=self.guild_id,
                    author_id=self.author_id,
                    pack=self.selected_pack,
                    panel_view=self,
                )
            else:
                view = ShieldPackExemptionsEditorView(
                    self.cog,
                    guild_id=self.guild_id,
                    author_id=self.author_id,
                    pack=self.selected_pack,
                    panel_view=self,
                )
            sent = await self.cog._send_private_interaction(interaction, embed=view.current_embed(), view=view)
            if sent is not None:
                view.message = sent

        await self._safe_action(
            interaction,
            stage=f"shield_panel_open_{editor_kind}",
            failure_message=f"Babblebox could not open the {pack_label} editor right now.",
            action=action,
        )

    async def _open_link_policy_editor(self, interaction: discord.Interaction):
        async def action():
            view = ShieldLinkPolicyEditorView(
                self.cog,
                guild_id=self.guild_id,
                author_id=self.author_id,
                panel_view=self,
            )
            sent = await self.cog._send_private_interaction(interaction, embed=view.current_embed(), view=view)
            if sent is not None:
                view.message = sent

        await self._safe_action(
            interaction,
            stage="shield_panel_open_link_policy",
            failure_message="Babblebox could not open the trusted-link editor right now.",
            action=action,
        )

    async def _open_logs_editor(self, interaction: discord.Interaction):
        async def action():
            view = ShieldLogsEditorView(
                self.cog,
                guild_id=self.guild_id,
                author_id=self.author_id,
                panel_view=self,
            )
            sent = await self.cog._send_private_interaction(interaction, embed=view.current_embed(), view=view)
            if sent is not None:
                view.message = sent

        await self._safe_action(
            interaction,
            stage="shield_panel_open_logs",
            failure_message="Babblebox could not open the Shield log-delivery editor right now.",
            action=action,
        )

    def _refresh_items(self):
        self.clear_items()

        def add_button(*, label: str, style: discord.ButtonStyle, row: int, callback):
            button = discord.ui.Button(label=label, style=style, row=row)
            button.callback = callback
            self.add_item(button)

        for label, section, row in (
            ("Overview", "overview", 0),
            ("Rules", "rules", 0),
            ("Links", "links", 0),
            ("Scope", "scope", 0),
            ("AI", "ai", 0),
            ("Logs", "logs", 1),
        ):
            async def nav_callback(interaction: discord.Interaction, *, target: str = section):
                await self._switch_section(interaction, target)

            add_button(
                label=label,
                style=discord.ButtonStyle.primary if self.section == section else discord.ButtonStyle.secondary,
                row=row,
                callback=nav_callback,
            )

        async def refresh_callback(interaction: discord.Interaction):
            await self._switch_section(interaction, self.section)

        add_button(label="Refresh", style=discord.ButtonStyle.secondary, row=1, callback=refresh_callback)

        config = self.cog.service.get_config(self.guild_id)

        async def toggle_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_module_enabled(self.guild_id, not bool(config.get("module_enabled")))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage="shield_panel_toggle_module",
                failure_message="Babblebox could not update live moderation right now.",
                action=action,
            )

        add_button(
            label="Disable Live Moderation" if config["module_enabled"] else "Enable Live Moderation",
            style=discord.ButtonStyle.danger if config["module_enabled"] else discord.ButtonStyle.success,
            row=1,
            callback=toggle_callback,
        )

        if self.section == "rules":
            pack_select = discord.ui.Select(
                placeholder="Select a Shield pack",
                row=2,
                options=[
                    discord.SelectOption(
                        label=PACK_LABELS[pack],
                        value=pack,
                        description=PACK_PANEL_DESCRIPTIONS[pack][:100],
                        default=pack == self.selected_pack,
                    )
                    for pack in RULE_PANEL_PACKS
                ],
            )

            async def pack_callback(interaction: discord.Interaction):
                async def action():
                    self.selected_pack = pack_select.values[0]
                    await self._rerender(interaction)

                await self._safe_action(
                    interaction,
                    stage="shield_panel_pack_select",
                    failure_message="Babblebox could not switch that Shield pack right now.",
                    action=action,
                )

            pack_select.callback = pack_callback
            self.add_item(pack_select)

            for label, editor_kind in (("Actions", "actions"), ("Options", "options"), ("Exemptions", "exemptions")):
                async def editor_callback(interaction: discord.Interaction, *, kind: str = editor_kind):
                    await self._open_pack_editor(interaction, kind)

                add_button(label=label, style=discord.ButtonStyle.secondary, row=3, callback=editor_callback)

        if self.section == "links":
            async def link_editor_callback(interaction: discord.Interaction):
                await self._open_link_policy_editor(interaction)

            add_button(label="Edit Link Policy", style=discord.ButtonStyle.secondary, row=2, callback=link_editor_callback)

        if self.section == "logs":
            async def logs_editor_callback(interaction: discord.Interaction):
                await self._open_logs_editor(interaction)

            add_button(label="Edit Log Delivery", style=discord.ButtonStyle.secondary, row=2, callback=logs_editor_callback)


class ShieldPackActionEditorView(ShieldManagedView):
    panel_title = "Shield Pack Actions"
    stale_message = "That pack editor expired. Open it again from `/shield panel`."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        pack: str,
        panel_view: ShieldPanelView | None = None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.pack = pack
        self.panel_view = panel_view
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog._pack_action_editor_embed(self.guild_id, self.pack)

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        pack_label = PACK_LABELS.get(self.pack, self.pack.replace("_", " ").title())

        enabled_value = bool(config.get(f"{self.pack}_enabled", True))
        sensitivity_value = str(config.get(f"{self.pack}_sensitivity", "normal"))
        status_select = discord.ui.Select(
            placeholder=f"{pack_label}: enable state and sensitivity",
            row=0,
            options=[
                discord.SelectOption(
                    label=f"{'On' if enabled else 'Off'} - {SENSITIVITY_LABELS[sensitivity]}",
                    value=f"{'on' if enabled else 'off'}:{sensitivity}",
                    default=enabled == enabled_value and sensitivity == sensitivity_value,
                )
                for enabled in (True, False)
                for sensitivity in ("low", "normal", "high")
            ],
        )

        async def status_callback(interaction: discord.Interaction):
            async def action():
                enabled_token, sensitivity_token = status_select.values[0].split(":", 1)
                ok, message = await self.cog.service.set_pack_config(
                    self.guild_id,
                    self.pack,
                    enabled=enabled_token == "on",
                    sensitivity=sensitivity_token,
                )
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage=f"shield_{self.pack}_status",
                failure_message=f"Babblebox could not update {pack_label} state right now.",
                action=action,
            )

        status_select.callback = status_callback
        self.add_item(status_select)

        for row, lane, choices in (
            (1, "low_action", LOW_ACTION_CHOICES),
            (2, "medium_action", MEDIUM_ACTION_CHOICES),
            (3, "high_action", ACTION_CHOICES),
        ):
            current_value = str(config.get(f"{self.pack}_{lane}", config.get(f"{self.pack}_action", "log")))
            select = discord.ui.Select(
                placeholder=f"{pack_label}: {lane.replace('_', ' ')}",
                row=row,
                options=[
                    discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == current_value)
                    for choice in choices
                ],
            )

            async def lane_callback(interaction: discord.Interaction, *, field: str = lane, component: discord.ui.Select = select):
                async def action():
                    ok, message = await self.cog.service.set_pack_config(self.guild_id, self.pack, **{field: component.values[0]})
                    await self._rerender(interaction, note=message, note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage=f"shield_{self.pack}_{field}",
                    failure_message=f"Babblebox could not update {pack_label} actions right now.",
                    action=action,
                )

            select.callback = lane_callback
            self.add_item(select)

        pack_timeout_minutes = config.get("pack_timeout_minutes", {})
        timeout_override = pack_timeout_minutes.get(self.pack) if isinstance(pack_timeout_minutes, dict) else None
        timeout_select = discord.ui.Select(
            placeholder=f"{pack_label}: timeout profile",
            row=4,
            options=[
                discord.SelectOption(
                    label=label,
                    value=value,
                    default=(value == "inherit" and not isinstance(timeout_override, int))
                    or (isinstance(timeout_override, int) and value == str(timeout_override)),
                )
                for value, label in TIMEOUT_PRESET_CHOICES
            ],
        )

        async def timeout_callback(interaction: discord.Interaction):
            async def action():
                selected = timeout_select.values[0]
                timeout_value = None if selected == "inherit" else int(selected)
                ok, message = await self.cog.service.set_pack_timeout_override(self.guild_id, self.pack, timeout_value)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage=f"shield_{self.pack}_timeout",
                failure_message=f"Babblebox could not update the {pack_label} timeout profile right now.",
                action=action,
            )

        timeout_select.callback = timeout_callback
        self.add_item(timeout_select)


class ShieldPackOptionsEditorView(ShieldManagedView):
    panel_title = "Shield Pack Options"
    stale_message = "That pack-options editor expired. Open it again from `/shield panel`."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        pack: str,
        panel_view: ShieldPanelView | None = None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.pack = pack
        self.panel_view = panel_view
        self.selected_lane = "rate" if pack in {"spam", "gif"} else None
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog._pack_options_editor_embed(self.guild_id, self.pack, selected_lane=self.selected_lane)

    def _lane_definitions(self) -> tuple[dict[str, object], ...]:
        if self.pack == "spam":
            return SPAM_OPTION_LANES
        if self.pack == "gif":
            return GIF_OPTION_LANES
        return ()

    def _current_lane(self) -> dict[str, object] | None:
        lane_map = {str(item["key"]): item for item in self._lane_definitions()}
        if self.selected_lane not in lane_map:
            self.selected_lane = next(iter(lane_map), None)
        return lane_map.get(self.selected_lane)

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        pack_label = PACK_LABELS.get(self.pack, self.pack.replace("_", " ").title())

        if self.pack in {"spam", "gif"}:
            lane = self._current_lane()
            if lane is None:
                return
            lane_definitions = self._lane_definitions()
            lane_select = discord.ui.Select(
                placeholder="Anti-Spam lane" if self.pack == "spam" else "GIF lane",
                row=0,
                options=[
                    discord.SelectOption(
                        label=str(item["label"]),
                        value=str(item["key"]),
                        default=str(item["key"]) == self.selected_lane,
                    )
                    for item in lane_definitions
                ],
            )

            async def lane_select_callback(interaction: discord.Interaction):
                async def action():
                    self.selected_lane = lane_select.values[0]
                    await self._rerender(interaction)

                await self._safe_action(
                    interaction,
                    stage=f"shield_{self.pack}_lane_select",
                    failure_message=f"Babblebox could not switch the {pack_label} lane editor right now.",
                    action=action,
                )

            lane_select.callback = lane_select_callback
            self.add_item(lane_select)

            current_enabled = bool(config.get(str(lane["enabled_field"])))
            state_select = discord.ui.Select(
                placeholder=f"{lane['label']} lane state",
                row=1,
                options=[
                    discord.SelectOption(label="On", value="on", default=current_enabled),
                    discord.SelectOption(label="Off", value="off", default=not current_enabled),
                ],
            )

            async def state_callback(
                interaction: discord.Interaction,
                *,
                component: discord.ui.Select = state_select,
                enabled_arg: str = str(lane["enabled_arg"]),
            ):
                async def action():
                    ok, message = await self.cog.service.set_pack_config(
                        self.guild_id,
                        self.pack,
                        **{enabled_arg: component.values[0] == "on"},
                    )
                    await self._rerender(interaction, note=message, note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage=f"shield_{self.pack}_{self.selected_lane}_state",
                    failure_message=f"Babblebox could not update that {pack_label} lane right now.",
                    action=action,
                )

            state_select.callback = state_callback
            self.add_item(state_select)

            threshold_values = tuple(int(value) for value in lane["threshold_values"])
            current_threshold = int(config.get(str(lane["threshold_field"]), threshold_values[0]))
            threshold_select = discord.ui.Select(
                placeholder=str(lane["threshold_placeholder"]),
                row=2,
                options=[
                    discord.SelectOption(label=str(value), value=str(value), default=value == current_threshold)
                    for value in threshold_values
                ],
            )

            async def threshold_callback(
                interaction: discord.Interaction,
                *,
                component: discord.ui.Select = threshold_select,
                threshold_arg: str = str(lane["threshold_arg"]),
            ):
                async def action():
                    ok, message = await self.cog.service.set_pack_config(
                        self.guild_id,
                        self.pack,
                        **{threshold_arg: int(component.values[0])},
                    )
                    await self._rerender(interaction, note=message, note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage=f"shield_{self.pack}_{self.selected_lane}_threshold",
                    failure_message=f"Babblebox could not update that {pack_label} threshold right now.",
                    action=action,
                )

            threshold_select.callback = threshold_callback
            self.add_item(threshold_select)

            secondary_field = lane.get("secondary_field")
            secondary_values = lane.get("secondary_values")
            if isinstance(secondary_field, str) and isinstance(secondary_values, tuple):
                current_secondary = int(config.get(secondary_field, secondary_values[0]))
                secondary_select = discord.ui.Select(
                    placeholder=str(lane["secondary_placeholder"]),
                    row=3,
                    options=[
                        discord.SelectOption(label=str(value), value=str(value), default=value == current_secondary)
                        for value in secondary_values
                    ],
                )

                async def secondary_callback(
                    interaction: discord.Interaction,
                    *,
                    component: discord.ui.Select = secondary_select,
                    secondary_arg: str = str(lane["secondary_arg"]),
                ):
                    async def action():
                        ok, message = await self.cog.service.set_pack_config(
                            self.guild_id,
                            self.pack,
                            **{secondary_arg: int(component.values[0])},
                        )
                        await self._rerender(interaction, note=message, note_ok=ok)

                    await self._safe_action(
                        interaction,
                        stage=f"shield_{self.pack}_{self.selected_lane}_secondary",
                        failure_message=f"Babblebox could not update that {pack_label} window right now.",
                        action=action,
                    )

                secondary_select.callback = secondary_callback
                self.add_item(secondary_select)

            if self.pack == "spam":
                moderator_policy = str(config.get("spam_moderator_policy", "exempt"))
                moderator_select = discord.ui.Select(
                    placeholder="Moderator anti-spam policy",
                    row=4,
                    options=[
                        discord.SelectOption(
                            label=self.cog._moderator_policy_label(policy),
                            value=policy,
                            default=policy == moderator_policy,
                        )
                        for policy in ("exempt", "delete_only", "full")
                    ],
                )

                async def moderator_callback(interaction: discord.Interaction):
                    async def action():
                        ok, message = await self.cog.service.set_pack_config(
                            self.guild_id,
                            "spam",
                            moderator_policy=moderator_select.values[0],
                        )
                        await self._rerender(interaction, note=message, note_ok=ok)

                    await self._safe_action(
                        interaction,
                        stage="shield_spam_moderator_policy",
                        failure_message="Babblebox could not update moderator anti-spam handling right now.",
                        action=action,
                    )

                moderator_select.callback = moderator_callback
                self.add_item(moderator_select)
            else:
                current_ratio = int(config.get("gif_min_ratio_percent", GIF_RATIO_THRESHOLDS[0]))
                ratio_select = discord.ui.Select(
                    placeholder="Minimum GIF ratio",
                    row=4,
                    options=[
                        discord.SelectOption(label=str(value), value=str(value), default=value == current_ratio)
                        for value in GIF_RATIO_THRESHOLDS
                    ],
                )

                async def ratio_callback(interaction: discord.Interaction):
                    async def action():
                        ok, message = await self.cog.service.set_pack_config(
                            self.guild_id,
                            "gif",
                            ratio_percent=int(ratio_select.values[0]),
                        )
                        await self._rerender(interaction, note=message, note_ok=ok)

                    await self._safe_action(
                        interaction,
                        stage="shield_gif_ratio",
                        failure_message=f"Babblebox could not update {pack_label} ratio settings right now.",
                        action=action,
                    )

                ratio_select.callback = ratio_callback
                self.add_item(ratio_select)
            return

        if self.pack == "adult":
            current = bool(config.get("adult_solicitation_enabled"))
            select = discord.ui.Select(
                placeholder="Adult solicitation text detector",
                row=0,
                options=[
                    discord.SelectOption(label="On", value="on", default=current),
                    discord.SelectOption(label="Off", value="off", default=not current),
                ],
            )

            async def adult_callback(interaction: discord.Interaction):
                async def action():
                    ok, message = await self.cog.service.set_pack_config(self.guild_id, "adult", adult_solicitation=select.values[0] == "on")
                    await self._rerender(interaction, note=message, note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage="shield_adult_solicitation",
                    failure_message="Babblebox could not update the adult solicitation lane right now.",
                    action=action,
                )

            select.callback = adult_callback
            self.add_item(select)
            return

        if self.pack == "severe":
            current_categories = set(config.get("severe_enabled_categories", []))
            select = discord.ui.Select(
                placeholder="Active severe categories",
                min_values=0,
                max_values=len(SEVERE_CATEGORY_LABELS),
                row=0,
                options=[
                    discord.SelectOption(label=label, value=category, default=category in current_categories)
                    for category, label in SEVERE_CATEGORY_LABELS.items()
                ],
            )

            async def severe_callback(interaction: discord.Interaction):
                async def action():
                    target = set(select.values)
                    messages: list[str] = []
                    ok = True
                    for category in SEVERE_CATEGORY_LABELS:
                        should_enable = category in target
                        if should_enable == (category in current_categories):
                            continue
                        change_ok, change_message = await self.cog.service.set_severe_category(self.guild_id, category, should_enable)
                        ok = ok and change_ok
                        messages.append(change_message)
                    if not messages:
                        messages.append("Severe categories already matched that selection.")
                    await self._rerender(interaction, note="\n".join(messages), note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage="shield_severe_categories",
                    failure_message="Babblebox could not update severe categories right now.",
                    action=action,
                )

            select.callback = severe_callback
            self.add_item(select)
            return

        self.add_item(discord.ui.Button(label="No Extra Pack Options", style=discord.ButtonStyle.secondary, disabled=True, row=0))


class ShieldPackExemptionsEditorView(ShieldManagedView):
    panel_title = "Shield Pack Exemptions"
    stale_message = "That pack-exemptions editor expired. Open it again from `/shield panel`."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        pack: str,
        panel_view: ShieldPanelView | None = None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.pack = pack
        self.panel_view = panel_view
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog._pack_exemptions_editor_embed(self.guild_id, self.pack)

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    def _refresh_items(self):
        self.clear_items()
        pack_label = PACK_LABELS.get(self.pack, self.pack.replace("_", " ").title())

        async def replace_targets(interaction: discord.Interaction, target_kind: str, target_ids: list[int], *, stage: str):
            async def action():
                ok, message = await self.cog.service.replace_pack_exemptions(self.guild_id, self.pack, target_kind, target_ids)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage=stage,
                failure_message=f"Babblebox could not update {pack_label} {target_kind} exemptions right now.",
                action=action,
            )

        channel_select = discord.ui.ChannelSelect(
            placeholder=f"{pack_label}: exempt channels",
            min_values=0,
            max_values=25,
            row=0,
        )

        async def channel_callback(interaction: discord.Interaction):
            await replace_targets(interaction, "channel", [int(item.id) for item in channel_select.values], stage=f"shield_{self.pack}_channel_exemptions")

        channel_select.callback = channel_callback
        self.add_item(channel_select)

        role_select = discord.ui.RoleSelect(
            placeholder=f"{pack_label}: exempt roles",
            min_values=0,
            max_values=25,
            row=1,
        )

        async def role_callback(interaction: discord.Interaction):
            await replace_targets(interaction, "role", [int(item.id) for item in role_select.values], stage=f"shield_{self.pack}_role_exemptions")

        role_select.callback = role_callback
        self.add_item(role_select)

        user_select = discord.ui.UserSelect(
            placeholder=f"{pack_label}: exempt members",
            min_values=0,
            max_values=25,
            row=2,
        )

        async def user_callback(interaction: discord.Interaction):
            await replace_targets(interaction, "user", [int(item.id) for item in user_select.values], stage=f"shield_{self.pack}_user_exemptions")

        user_select.callback = user_callback
        self.add_item(user_select)

        for row, label, target_kind in (
            (3, "Clear Channels", "channel"),
            (3, "Clear Roles", "role"),
            (3, "Clear Members", "user"),
        ):
            button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, row=row)

            async def button_callback(interaction: discord.Interaction, *, kind: str = target_kind):
                await replace_targets(interaction, kind, [], stage=f"shield_{self.pack}_{kind}_clear")

            button.callback = button_callback
            self.add_item(button)


class ShieldLinkPolicyEditorView(ShieldManagedView):
    panel_title = "Shield Link Policy"
    stale_message = "That link-policy editor expired. Open it again from `/shield panel`."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        panel_view: ShieldPanelView | None = None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.panel_view = panel_view
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog._link_policy_editor_embed(self.guild_id)

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        mode_value = str(config.get("link_policy_mode", "default"))

        mode_select = discord.ui.Select(
            placeholder="Trusted-link mode",
            row=0,
            options=[
                discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == mode_value)
                for choice in LINK_POLICY_MODE_CHOICES
            ],
        )

        async def mode_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_link_policy_config(self.guild_id, mode=mode_select.values[0])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage="shield_link_policy_mode",
                failure_message="Babblebox could not update the trusted-link mode right now.",
                action=action,
            )

        mode_select.callback = mode_callback
        self.add_item(mode_select)

        for row, lane, choices in (
            (1, "low_action", LOW_ACTION_CHOICES),
            (2, "medium_action", MEDIUM_ACTION_CHOICES),
            (3, "high_action", ACTION_CHOICES),
        ):
            current_value = str(config.get(f"link_policy_{lane}", "log"))
            select = discord.ui.Select(
                placeholder=f"Trusted-link {lane.replace('_', ' ')}",
                row=row,
                options=[
                    discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == current_value)
                    for choice in choices
                ],
            )

            async def callback(interaction: discord.Interaction, *, field: str = lane, component: discord.ui.Select = select):
                async def action():
                    ok, message = await self.cog.service.set_link_policy_config(self.guild_id, **{field: component.values[0]})
                    await self._rerender(interaction, note=message, note_ok=ok)

                await self._safe_action(
                    interaction,
                    stage=f"shield_link_policy_{field}",
                    failure_message="Babblebox could not update the trusted-link action ladder right now.",
                    action=action,
                )

            select.callback = callback
            self.add_item(select)

        pack_timeout_minutes = config.get("pack_timeout_minutes", {})
        timeout_override = pack_timeout_minutes.get("link_policy") if isinstance(pack_timeout_minutes, dict) else None
        timeout_select = discord.ui.Select(
            placeholder="Trusted-link timeout profile",
            row=4,
            options=[
                discord.SelectOption(
                    label=label,
                    value=value,
                    default=(value == "inherit" and not isinstance(timeout_override, int))
                    or (isinstance(timeout_override, int) and value == str(timeout_override)),
                )
                for value, label in TIMEOUT_PRESET_CHOICES
            ],
        )

        async def timeout_callback(interaction: discord.Interaction):
            async def action():
                selected = timeout_select.values[0]
                timeout_value = None if selected == "inherit" else int(selected)
                ok, message = await self.cog.service.set_link_policy_timeout_override(self.guild_id, timeout_value)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage="shield_link_policy_timeout",
                failure_message="Babblebox could not update the trusted-link timeout profile right now.",
                action=action,
            )

        timeout_select.callback = timeout_callback
        self.add_item(timeout_select)


class ShieldLogsEditorView(ShieldManagedView):
    panel_title = "Shield Log Delivery"
    stale_message = "That log-delivery editor expired. Open it again from `/shield panel`."

    def __init__(
        self,
        cog: "ShieldCog",
        *,
        guild_id: int,
        author_id: int,
        panel_view: ShieldPanelView | None = None,
        selected_pack: str = "gif",
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.panel_view = panel_view
        self.selected_pack = selected_pack if selected_pack in RULE_PANEL_PACKS else "gif"
        self._refresh_items()

    def current_embed(self) -> discord.Embed:
        return self.cog._logs_editor_embed(self.guild_id, selected_pack=self.selected_pack)

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._shield_status_embed(self.panel_title, note, ok=note_ok),
            )

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        pack_overrides = config.get("pack_log_overrides", {})
        selected_override = pack_overrides.get(self.selected_pack, {}) if isinstance(pack_overrides, dict) else {}
        current_style = str(config.get("log_style", "adaptive"))
        current_ping_mode = str(config.get("log_ping_mode", "smart"))
        override_style = str(selected_override.get("style", "inherit"))
        override_ping_mode = str(selected_override.get("ping_mode", "inherit"))

        global_style_select = discord.ui.Select(
            placeholder="Global log style",
            row=0,
            options=[
                discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == current_style)
                for choice in LOG_STYLE_CHOICES
            ],
        )

        async def global_style_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_log_delivery(self.guild_id, style=global_style_select.values[0])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage="shield_log_style",
                failure_message="Babblebox could not update the global Shield log style right now.",
                action=action,
            )

        global_style_select.callback = global_style_callback
        self.add_item(global_style_select)

        global_ping_select = discord.ui.Select(
            placeholder="Global ping mode",
            row=1,
            options=[
                discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == current_ping_mode)
                for choice in LOG_PING_MODE_CHOICES
            ],
        )

        async def global_ping_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_log_delivery(self.guild_id, ping_mode=global_ping_select.values[0])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage="shield_log_ping_mode",
                failure_message="Babblebox could not update the global Shield log ping mode right now.",
                action=action,
            )

        global_ping_select.callback = global_ping_callback
        self.add_item(global_ping_select)

        pack_select = discord.ui.Select(
            placeholder="Pack override target",
            row=2,
            options=[
                discord.SelectOption(
                    label=PACK_LABELS[pack],
                    value=pack,
                    default=pack == self.selected_pack,
                )
                for pack in RULE_PANEL_PACKS
            ],
        )

        async def pack_callback(interaction: discord.Interaction):
            async def action():
                self.selected_pack = pack_select.values[0]
                await self._rerender(interaction)

            await self._safe_action(
                interaction,
                stage="shield_log_override_pack",
                failure_message="Babblebox could not switch the pack log override target right now.",
                action=action,
            )

        pack_select.callback = pack_callback
        self.add_item(pack_select)

        override_style_select = discord.ui.Select(
            placeholder=f"{PACK_LABELS[self.selected_pack]} style override",
            row=3,
            options=[
                discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == override_style)
                for choice in PACK_LOG_STYLE_CHOICES
            ],
        )

        async def override_style_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_pack_log_override(
                    self.guild_id,
                    self.selected_pack,
                    style=override_style_select.values[0],
                )
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage=f"shield_log_override_style_{self.selected_pack}",
                failure_message="Babblebox could not update that pack log style override right now.",
                action=action,
            )

        override_style_select.callback = override_style_callback
        self.add_item(override_style_select)

        override_ping_select = discord.ui.Select(
            placeholder=f"{PACK_LABELS[self.selected_pack]} ping override",
            row=4,
            options=[
                discord.SelectOption(label=choice.name, value=str(choice.value), default=str(choice.value) == override_ping_mode)
                for choice in PACK_LOG_PING_CHOICES
            ],
        )

        async def override_ping_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_pack_log_override(
                    self.guild_id,
                    self.selected_pack,
                    ping_mode=override_ping_select.values[0],
                )
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(
                interaction,
                stage=f"shield_log_override_ping_{self.selected_pack}",
                failure_message="Babblebox could not update that pack log ping override right now.",
                action=action,
            )

        override_ping_select.callback = override_ping_callback
        self.add_item(override_ping_select)


class ShieldCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ShieldService(bot)
        harden_admin_root_group(self.shield_group)

    async def cog_load(self):
        await bind_started_service(self.bot, attr_name="shield_service", service=self.service, label="Shield")
        add_view = getattr(self.bot, "add_view", None)
        if callable(add_view):
            for record in self.service.active_alert_action_records():
                message_id = record.get("alert_message_id")
                if isinstance(message_id, int) and message_id > 0:
                    add_view(self.service.build_alert_action_view(record), message_id=message_id)

    def cog_unload(self):
        if hasattr(self.bot, "shield_service"):
            delattr(self.bot, "shield_service")
        self.bot.loop.create_task(self.service.close())

    def shield_access_reason(self, actor: object, guild_id: int | None = None) -> tuple[bool, str]:
        perms = getattr(actor, "guild_permissions", None)
        if bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
            return True, "Manage Server or administrator access."
        return False, "You need **Manage Server** or administrator access to configure Babblebox Shield."

    def user_can_manage_shield(self, actor: object) -> bool:
        allowed, _reason = self.shield_access_reason(actor)
        return allowed

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
            _allowed, reason = self.shield_access_reason(ctx.author, ctx.guild.id)
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Admin Only",
                    reason,
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

    def _shield_status_embed(self, title: str, message: str, *, ok: bool) -> discord.Embed:
        return ge.make_status_embed(title, message, tone="success" if ok else "warning", footer="Babblebox Shield")

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
            with contextlib.suppress(discord.NotFound, discord.HTTPException):
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
                embed=self._shield_status_embed(failure_title, failure_message, ok=False),
            )
            return False

    def _default_rule_pack(self, guild_id: int) -> str:
        config = self.service.get_config(guild_id)
        for pack in RULE_PANEL_PACKS:
            if config.get(f"{pack}_enabled"):
                return pack
        return "spam"

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

    def _format_ai_models(self, models: list[str]) -> str:
        return format_shield_ai_model_list(models)

    def _ai_policy_source_label(self, source: str) -> str:
        labels = {
            "ordinary_global": "Global owner default",
            "guild_override": "Per-guild owner override",
        }
        return labels.get(source, source.replace("_", " ").title())

    def _action_label(self, action: str) -> str:
        return ACTION_LABELS.get(str(action), str(action).replace("_", " ").title())

    def _moderator_policy_label(self, policy: str) -> str:
        labels = {
            "exempt": "Exempt moderators",
            "delete_only": "Delete only",
            "full": "Full anti-spam policy",
        }
        return labels.get(str(policy), str(policy).replace("_", " ").title())

    def _log_style_label(self, style: str) -> str:
        labels = {
            "adaptive": "Adaptive",
            "compact": "Compact",
            "inherit": "Inherit",
        }
        return labels.get(str(style), str(style).replace("_", " ").title())

    def _log_ping_mode_label(self, mode: str) -> str:
        labels = {
            "smart": "Smart",
            "never": "Never ping",
            "inherit": "Inherit",
        }
        return labels.get(str(mode), str(mode).replace("_", " ").title())

    def _resolved_pack_log_delivery(self, config: dict[str, object], pack: str) -> tuple[str, str, str, str]:
        pack_overrides = config.get("pack_log_overrides", {})
        entry = pack_overrides.get(pack, {}) if isinstance(pack_overrides, dict) else {}
        local_style = str(entry.get("style", "inherit"))
        local_ping_mode = str(entry.get("ping_mode", "inherit"))
        global_style = str(config.get("log_style", "adaptive"))
        global_ping_mode = str(config.get("log_ping_mode", "smart"))
        effective_style = global_style if local_style == "inherit" else local_style
        effective_ping_mode = global_ping_mode if local_ping_mode == "inherit" else local_ping_mode
        return (local_style, local_ping_mode, effective_style, effective_ping_mode)

    def _pack_log_override_summary(self, config: dict[str, object], pack: str) -> str:
        local_style, local_ping_mode, effective_style, effective_ping_mode = self._resolved_pack_log_delivery(config, pack)
        return (
            f"Local style: {self._log_style_label(local_style)} | "
            f"Local ping: {self._log_ping_mode_label(local_ping_mode)} | "
            f"Effective: {self._log_style_label(effective_style)} + {self._log_ping_mode_label(effective_ping_mode)}"
        )

    def _pack_exemption_summary(self, config: dict[str, object], pack: str) -> str:
        pack_exemptions = config.get("pack_exemptions", {})
        if not isinstance(pack_exemptions, dict):
            return "Channels: None | Roles: None | Members: None"
        entry = pack_exemptions.get(pack, {})
        if not isinstance(entry, dict):
            return "Channels: None | Roles: None | Members: None"
        return (
            f"Channels: {self._format_mentions(entry.get('channel_ids', []), kind='channel')} | "
            f"Roles: {self._format_mentions(entry.get('role_ids', []), kind='role')} | "
            f"Members: {self._format_mentions(entry.get('user_ids', []), kind='user')}"
        )

    def _pack_policy_actions(self, config: dict[str, object], pack: str) -> tuple[str, str, str]:
        return (
            str(config.get(f"{pack}_low_action", "log")),
            str(config.get(f"{pack}_medium_action", "log")),
            str(config.get(f"{pack}_high_action", "log")),
        )

    def _pack_rule_summary(self, config: dict[str, object], pack: str) -> str:
        if pack == "spam":
            return (
                "Rate lane: "
                + (
                    f"On at {config.get('spam_message_threshold', 7)} messages in {config.get('spam_message_window_seconds', 5)}s"
                    if config.get("spam_message_enabled", True)
                    else "Off"
                )
                + "\n"
                + "Burst lane: "
                + (
                    f"On at {config.get('spam_burst_threshold', 5)} messages in {config.get('spam_burst_window_seconds', 10)}s"
                    if config.get("spam_burst_enabled", True)
                    else "Off"
                )
                + "\n"
                + "Near-duplicate lane: "
                + (
                    f"On at {config.get('spam_near_duplicate_threshold', 5)} in {config.get('spam_near_duplicate_window_seconds', 10)}s"
                    if config.get("spam_near_duplicate_enabled", True)
                    else "Off"
                )
                + "\n"
                f"Emote spam: {'On' if config.get('spam_emote_enabled') else 'Off'}"
                + (f" at {config.get('spam_emote_threshold', 18)}+" if config.get('spam_emote_enabled') else "")
                + "\n"
                + f"Capitals spam: {'On' if config.get('spam_caps_enabled') else 'Off'}"
                + (f" at {config.get('spam_caps_threshold', 28)}+ uppercase letters" if config.get('spam_caps_enabled') else "")
                + "\n"
                + f"Low-value chatter: {'On' if config.get('spam_low_value_enabled') else 'Off'}"
                + (
                    f" at {config.get('spam_low_value_threshold', 5)} messages in {config.get('spam_low_value_window_seconds', 60)}s"
                    if config.get("spam_low_value_enabled")
                    else ""
                )
                + "\n"
                + f"Moderator anti-spam: {self._moderator_policy_label(str(config.get('spam_moderator_policy', 'exempt')))}"
            )
        if pack == "gif":
            return (
                "GIF-heavy rate lane: "
                + (
                    f"On at {config.get('gif_message_threshold', 4)} posts in {config.get('gif_window_seconds', 20)}s"
                    if config.get("gif_message_enabled", True)
                    else "Off"
                )
                + "\n"
                + "True channel streak lane: "
                + (
                    f"On at {config.get('gif_consecutive_threshold', 5)} consecutive GIF-heavy messages across members"
                    if config.get("gif_consecutive_enabled", True)
                    else "Off"
                )
                + "\n"
                + "Low-text repeat lane: "
                + (
                    f"On at {config.get('gif_repeat_threshold', 3)}+ repeats with {config.get('gif_min_ratio_percent', 70)}% effective GIF pressure"
                    if config.get("gif_repeat_enabled", True)
                    else "Off"
                )
                + "\n"
                + "Same asset lane: "
                + (
                    f"On at {config.get('gif_same_asset_threshold', 3)}+ repeats"
                    if config.get("gif_same_asset_enabled", True)
                    else "Off"
                )
                + "\n"
                "Delete actions remove bounded GIF bursts, not just the last message. Collective cleanup removes the exact streak or trims only the newest contributing GIFs from the active pressure slice, personal abuse can still enforce one member, and healthy text stays untouched. Tight low-end settings are stricter and can be noisy in meme-heavy rooms."
            )
        return ""

    def _pack_policy_overview(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        lines = [
            f"Enabled: {'Yes' if config.get(f'{pack}_enabled') else 'No'} | Sensitivity: {SENSITIVITY_LABELS[config.get(f'{pack}_sensitivity', 'normal')]} | Timeout: {self._pack_timeout_badge(config, pack)}",
            f"Actions: {self._action_label(low_action)} / {self._action_label(medium_action)} / {self._action_label(high_action)}",
        ]
        if pack == "spam":
            lines.append(
                "Rate "
                + (
                    f"On at {config.get('spam_message_threshold', 7)} in {config.get('spam_message_window_seconds', 5)}s"
                    if config.get("spam_message_enabled", True)
                    else "Off"
                )
                + " | Burst "
                + (
                    f"On at {config.get('spam_burst_threshold', 5)} in {config.get('spam_burst_window_seconds', 10)}s"
                    if config.get("spam_burst_enabled", True)
                    else "Off"
                )
                + " | Duplicates "
                + (
                    f"On at {config.get('spam_near_duplicate_threshold', 5)} in {config.get('spam_near_duplicate_window_seconds', 10)}s"
                    if config.get("spam_near_duplicate_enabled", True)
                    else "Off"
                )
            )
            lines.append(
                "Emotes: "
                + (f"On at {config.get('spam_emote_threshold', 18)}+" if config.get("spam_emote_enabled") else "Off")
                + " | Caps: "
                + (f"On at {config.get('spam_caps_threshold', 28)}+" if config.get("spam_caps_enabled") else "Off")
                + " | Low-value: "
                + (
                    f"On at {config.get('spam_low_value_threshold', 5)} in {config.get('spam_low_value_window_seconds', 60)}s"
                    if config.get("spam_low_value_enabled")
                    else "Off"
                )
            )
        elif pack == "gif":
            lines.append(
                "One-member rate "
                + (
                    f"On at {config.get('gif_message_threshold', 4)} in {config.get('gif_window_seconds', 20)}s"
                    if config.get("gif_message_enabled", True)
                    else "Off"
                )
                + " | True streak "
                + (
                    f"On at {config.get('gif_consecutive_threshold', 5)}"
                    if config.get("gif_consecutive_enabled", True)
                    else "Off"
                )
                + " | Low-text repeat "
                + (
                    f"On at {config.get('gif_repeat_threshold', 3)} with {config.get('gif_min_ratio_percent', 70)}% effective pressure"
                    if config.get("gif_repeat_enabled", True)
                    else "Off"
                )
                + " | Same asset "
                + (
                    f"On at {config.get('gif_same_asset_threshold', 3)}"
                    if config.get("gif_same_asset_enabled", True)
                    else "Off"
                )
            )
            lines.append(
                "Delete lane removes bounded GIF bursts; collective cleanup uses the exact streak or trims the newest contributing GIFs inside the active pressure slice while personal abuse still targets one member."
            )
        return "\n".join(lines)

    def _pack_status_line(self, config: dict[str, object], pack: str) -> str:
        _, _, high_action = self._pack_policy_actions(config, pack)
        status = "On" if config.get(f"{pack}_enabled") else "Off"
        sensitivity = SENSITIVITY_LABELS[config.get(f"{pack}_sensitivity", "normal")]
        return f"**{PACK_LABELS[pack]}**: {status} | {sensitivity} | high {self._action_label(high_action)}"

    def _pack_policy_compact(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        rule_summary = self._pack_rule_summary(config, pack)
        if not rule_summary:
            return (
                f"Low confidence: {self._action_label(low_action)}\n"
                f"Medium confidence: {self._action_label(medium_action)}\n"
                f"High confidence: {self._action_label(high_action)}"
            )
        return (
            f"Low confidence: {self._action_label(low_action)}\n"
            f"Medium confidence: {self._action_label(medium_action)}\n"
            f"High confidence: {self._action_label(high_action)}\n"
            f"{rule_summary}"
        )

    def _pack_policy_detail(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        rule_summary = self._pack_rule_summary(config, pack)
        rule_line = f"\n{rule_summary}" if rule_summary else ""
        exemption_line = f"\nPack-specific exemptions: {self._pack_exemption_summary(config, pack)}"
        adult_solicit_line = ""
        if pack == "adult":
            adult_solicit_line = (
                f"\nOptional solicitation detector: {'On' if config.get('adult_solicitation_enabled') else 'Off'}\n"
                f"Solicitation carve-out channels: {self._format_mentions(config.get('adult_solicitation_excluded_channel_ids', []), kind='channel')}"
            )
        severe_line = ""
        if pack == "severe":
            category_labels = [
                SEVERE_CATEGORY_LABELS.get(str(value), str(value).replace("_", " ").title())
                for value in config.get("severe_enabled_categories", [])
            ]
            severe_line = (
                f"\nCategories: {', '.join(category_labels) if category_labels else 'None'}\n"
                f"Custom terms: {self._format_text_list(config.get('severe_custom_terms', []), limit=4)}\n"
                f"Removed bundled terms: {self._format_text_list(config.get('severe_removed_terms', []), limit=4)}"
            )
        return (
            f"Enabled: {'Yes' if config[f'{pack}_enabled'] else 'No'} | "
            f"Sensitivity: {SENSITIVITY_LABELS[config[f'{pack}_sensitivity']]}\n"
            f"Low confidence action: {self._action_label(low_action)}\n"
            f"Medium confidence action: {self._action_label(medium_action)}\n"
            f"High confidence action: {self._action_label(high_action)}"
            f"{rule_line}"
            f"{exemption_line}"
            f"{adult_solicit_line}"
            f"{severe_line}"
        )

    def _link_policy_actions(self, config: dict[str, object]) -> tuple[str, str, str]:
        return (
            str(config.get("link_policy_low_action", "log")),
            str(config.get("link_policy_medium_action", "log")),
            str(config.get("link_policy_high_action", "log")),
        )

    def _link_policy_label(self, config: dict[str, object]) -> str:
        return "Trusted Links Only" if config.get("link_policy_mode") == "trusted_only" else "Default"

    def _link_policy_detail(self, config: dict[str, object]) -> str:
        low_action, medium_action, high_action = self._link_policy_actions(config)
        if config.get("link_policy_mode") == "trusted_only":
            detail = (
                "Shield allows the built-in trusted pack plus admin allowlisted domains and invite codes as bounded "
                "policy exceptions. Malicious, impersonation, adult, and suspicious-link intel still wins."
            )
        else:
            detail = (
                "Shield keeps the normal broad link posture here. Trusted-only policy is inactive, but malicious, "
                "impersonation, adult, and suspicious-link intel still feeds the specialized packs."
            )
        return (
            f"Mode: **{self._link_policy_label(config)}**\n"
            f"Low confidence: {self._action_label(low_action)}\n"
            f"Medium confidence: {self._action_label(medium_action)}\n"
            f"High confidence: {self._action_label(high_action)}\n"
            f"Timeout profile: {self._pack_timeout_summary(config, 'link_policy')}\n"
            f"{detail}"
        )

    def _truncate_text(self, value: object, limit: int) -> str:
        text = str(value or "")
        if not text:
            return "None"
        if len(text) <= limit:
            return text
        suffix = "..."
        return text[: max(1, limit - len(suffix))].rstrip() + suffix

    def _embed_char_count(self, embed: discord.Embed) -> int:
        total = len(embed.title or "") + len(embed.description or "")
        total += len(getattr(embed.footer, "text", "") or "")
        for field in embed.fields:
            total += len(field.name or "") + len(field.value or "")
        return total

    def _finalize_shield_embed(self, embed: discord.Embed, *, footer: str) -> discord.Embed:
        embed = ge.style_embed(embed, footer=footer)
        if embed.title:
            embed.title = self._truncate_text(embed.title, 256)
        if embed.description:
            embed.description = self._truncate_text(embed.description, 4096)
        if getattr(embed.footer, "text", None):
            embed.set_footer(text=self._truncate_text(embed.footer.text, 2048))
        for index, field in enumerate(list(embed.fields)):
            embed.set_field_at(
                index,
                name=self._truncate_text(field.name, 256),
                value=self._truncate_text(field.value, 1024),
                inline=field.inline,
            )
        total = self._embed_char_count(embed)
        if total > 6000 and embed.description:
            embed.description = self._truncate_text(embed.description, max(64, len(embed.description) - (total - 6000)))
            total = self._embed_char_count(embed)
        if total > 6000:
            for index in range(len(embed.fields) - 1, -1, -1):
                if total <= 6000:
                    break
                field = embed.fields[index]
                embed.set_field_at(
                    index,
                    name=field.name,
                    value=self._truncate_text(field.value, max(64, len(field.value) - (total - 6000))),
                    inline=field.inline,
                )
                total = self._embed_char_count(embed)
        return embed

    def _pack_timeout_summary(self, config: dict[str, object], pack: str) -> str:
        pack_timeout_minutes = config.get("pack_timeout_minutes", {})
        override = pack_timeout_minutes.get(pack) if isinstance(pack_timeout_minutes, dict) else None
        global_timeout = int(config.get("timeout_minutes", 10))
        if isinstance(override, int) and override >= 1:
            return f"Dedicated `{override}` minute timeout"
        return f"Inherits global `{global_timeout}` minute timeout"

    def _pack_timeout_badge(self, config: dict[str, object], pack: str) -> str:
        pack_timeout_minutes = config.get("pack_timeout_minutes", {})
        override = pack_timeout_minutes.get(pack) if isinstance(pack_timeout_minutes, dict) else None
        if isinstance(override, int) and override >= 1:
            return f"{override}m dedicated"
        return f"{int(config.get('timeout_minutes', 10))}m global"

    def _ai_routing_label(self, value: object) -> str:
        labels = {
            "single_model_override": "Single model override",
            "routed_fast_complex": "Fast + complex routing",
            "routed_fast_complex_frontier": "Fast + complex + frontier routing",
        }
        return labels.get(str(value or ""), str(value or "disabled"))

    def _ai_setup_blocker_summary(self, blockers: list[str]) -> str:
        return "; ".join(blockers) if blockers else "None"

    def _pack_exemption_counts(self, config: dict[str, object], pack: str) -> tuple[int, int, int]:
        pack_exemptions = config.get("pack_exemptions", {})
        if not isinstance(pack_exemptions, dict):
            return (0, 0, 0)
        entry = pack_exemptions.get(pack, {})
        if not isinstance(entry, dict):
            return (0, 0, 0)
        return (
            len(entry.get("channel_ids", [])),
            len(entry.get("role_ids", [])),
            len(entry.get("user_ids", [])),
        )

    def _shield_ai_entitlement_lines(self, ai_status: dict[str, object], *, include_plan_allowed: bool = False) -> list[str]:
        lines = [
            f"Entitlement: {ai_status.get('premium_summary') or 'Guild Pro status is unavailable right now.'}",
            f"Configured models: {self._format_ai_models(ai_status.get('configured_allowed_models', []))}",
            f"Effective models right now: {self._format_ai_models(ai_status.get('effective_allowed_models', ai_status.get('allowed_models', [])))}",
            "Upgrade path: Babblebox Guild Pro can make gpt-5.4-mini plus gpt-5.4 available when owner policy and provider/runtime readiness allow review.",
        ]
        if include_plan_allowed:
            lines.append(f"Plan-allowed models: {self._format_ai_models(ai_status.get('plan_allowed_models', []))}")
        if ai_status.get("configured_models_capped"):
            lines.append("Stored higher-tier Shield AI settings stay configured, but the effective lane is capped until Guild Pro returns.")
        return lines

    def _shield_ai_entitlement_text(self, ai_status: dict[str, object], *, include_plan_allowed: bool = False) -> str:
        return "\n".join(self._shield_ai_entitlement_lines(ai_status, include_plan_allowed=include_plan_allowed))

    def _shield_ai_scope_update_text(self, ai_status: dict[str, object], *, pack_summary: str) -> str:
        lines = [
            f"Shield AI review scope now uses `{ai_status.get('min_confidence', 'high')}` minimum local confidence for {pack_summary}.",
            self._shield_ai_entitlement_text(ai_status),
            f"Provider diagnostics: {ai_status.get('provider_status') or 'Unavailable.'}",
            f"Local blockers: {self._ai_setup_blocker_summary(list(ai_status.get('setup_blockers', [])))}",
        ]
        return "\n".join(lines)

    def _custom_pattern_limit_text(self, guild_id: int, config: dict[str, object]) -> str:
        limit_value = self.service.custom_pattern_limit(guild_id)
        saved_count = len(config.get("custom_patterns", []))
        active_count = min(saved_count, limit_value)
        line = f"Advanced patterns: {format_saved_state_status(saved_count=saved_count, active_count=active_count, limit_value=limit_value)}"
        note = preserved_over_limit_note(saved_count=saved_count, active_count=active_count)
        if note:
            line += f"\n{note}"
        return line

    def _saved_pack_exemption_lines(self, guild_id: int, config: dict[str, object]) -> list[str]:
        limit_value = self.service.pack_exemption_limit(guild_id)
        lines: list[str] = []
        for pack in RULE_PANEL_PACKS:
            channel_count, role_count, user_count = self._pack_exemption_counts(config, pack)
            for noun, saved_count in (("channels", channel_count), ("roles", role_count), ("members", user_count)):
                active_count = min(saved_count, limit_value)
                if saved_count <= active_count:
                    continue
                lines.append(
                    f"{PACK_LABELS[pack]} {noun}: {format_saved_state_status(saved_count=saved_count, active_count=active_count, limit_value=limit_value)}"
                )
        if lines:
            lines.append(preserved_over_limit_note(saved_count=1, active_count=0) or "")
        return lines

    def _pack_option_lines(self, config: dict[str, object], pack: str) -> list[str]:
        if pack == "spam":
            return [
                "Rate lane: "
                + (
                    f"On at {config.get('spam_message_threshold', 7)} messages in {config.get('spam_message_window_seconds', 5)}s"
                    if config.get("spam_message_enabled", True)
                    else "Off"
                ),
                "Burst lane: "
                + (
                    f"On at {config.get('spam_burst_threshold', 5)} messages in {config.get('spam_burst_window_seconds', 10)}s"
                    if config.get("spam_burst_enabled", True)
                    else "Off"
                ),
                "Near-duplicate lane: "
                + (
                    f"On at {config.get('spam_near_duplicate_threshold', 5)} in {config.get('spam_near_duplicate_window_seconds', 10)}s"
                    if config.get("spam_near_duplicate_enabled", True)
                    else "Off"
                ),
                f"Emoji / emote lane: {'On' if config.get('spam_emote_enabled') else 'Off'}"
                + (f" at {config.get('spam_emote_threshold', 18)}+" if config.get('spam_emote_enabled') else ""),
                f"Capitals lane: {'On' if config.get('spam_caps_enabled') else 'Off'}"
                + (f" at {config.get('spam_caps_threshold', 28)}+" if config.get('spam_caps_enabled') else ""),
                "Low-value chatter lane: "
                + (
                    f"On at {config.get('spam_low_value_threshold', 5)} messages in {config.get('spam_low_value_window_seconds', 60)}s"
                    if config.get("spam_low_value_enabled")
                    else "Off"
                ),
                f"Moderator handling: {self._moderator_policy_label(str(config.get('spam_moderator_policy', 'exempt')))}",
            ]
        if pack == "gif":
            return [
                "GIF-heavy rate lane: "
                + (
                    f"On at {config.get('gif_message_threshold', 4)} posts in {config.get('gif_window_seconds', 20)}s"
                    if config.get("gif_message_enabled", True)
                    else "Off"
                ),
                "True channel streak lane: "
                + (
                    f"On at {config.get('gif_consecutive_threshold', 5)} consecutive GIF-heavy messages"
                    if config.get("gif_consecutive_enabled", True)
                    else "Off"
                ),
                "Low-text repeat lane: "
                + (
                    f"On at {config.get('gif_repeat_threshold', 3)}+ repeats with {config.get('gif_min_ratio_percent', 70)}% effective GIF pressure"
                    if config.get("gif_repeat_enabled", True)
                    else "Off"
                ),
                "Same asset lane: "
                + (
                    f"On at {config.get('gif_same_asset_threshold', 3)}+ repeats"
                    if config.get("gif_same_asset_enabled", True)
                    else "Off"
                ),
                "Delete actions remove bounded GIF bursts, not healthy text. Collective cleanup uses the exact streak or newest contributing GIFs; personal abuse can still target one member.",
                "Tighter low-end values are stricter and best for rooms that want faster GIF cleanup.",
            ]
        if pack == "adult":
            return [
                f"Solicitation text detector: {'On' if config.get('adult_solicitation_enabled') else 'Off'}",
                f"Solicitation carve-out channels: {self._format_mentions(config.get('adult_solicitation_excluded_channel_ids', []), kind='channel')}",
            ]
        if pack == "severe":
            category_labels = [
                SEVERE_CATEGORY_LABELS.get(str(value), str(value).replace("_", " ").title())
                for value in config.get("severe_enabled_categories", [])
            ]
            return [
                f"Categories: {', '.join(category_labels) if category_labels else 'None'}",
                f"Custom terms: {self._format_text_list(config.get('severe_custom_terms', []), limit=4)}",
                f"Removed bundled terms: {self._format_text_list(config.get('severe_removed_terms', []), limit=4)}",
            ]
        return ["No extra pack-local thresholds on this pack."]

    def _pack_overview_line(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        status = "On" if config.get(f"{pack}_enabled") else "Off"
        sensitivity = SENSITIVITY_LABELS[config.get(f"{pack}_sensitivity", "normal")]
        return (
            f"**{PACK_LABELS[pack]}**\n"
            f"{status} | {sensitivity} | "
            f"{self._action_label(low_action)} / {self._action_label(medium_action)} / {self._action_label(high_action)}\n"
            f"Timeout: {self._pack_timeout_badge(config, pack)}"
        )

    def _pack_detail_text(self, config: dict[str, object], pack: str) -> str:
        low_action, medium_action, high_action = self._pack_policy_actions(config, pack)
        channels, roles, users = self._pack_exemption_counts(config, pack)
        lines = [
            PACK_PANEL_DESCRIPTIONS.get(pack, PACK_LABELS.get(pack, pack.title())),
            f"Enabled: {'Yes' if config.get(f'{pack}_enabled') else 'No'} | Sensitivity: {SENSITIVITY_LABELS[config.get(f'{pack}_sensitivity', 'normal')]}",
            f"Low confidence: {self._action_label(low_action)}",
            f"Medium confidence: {self._action_label(medium_action)}",
            f"High confidence: {self._action_label(high_action)}",
            f"Timeout profile: {self._pack_timeout_summary(config, pack)}",
            *self._pack_option_lines(config, pack),
            f"Pack exemptions: {channels} channel(s), {roles} role(s), {users} member(s)",
        ]
        return "\n".join(lines)

    def _pack_action_editor_embed(self, guild_id: int, pack: str) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title=f"{PACK_LABELS.get(pack, pack.title())} Actions",
            description="Only action lanes, sensitivity, enable state, and this pack's timeout profile live here.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Profile", value=self._pack_detail_text(config, pack), inline=False)
        embed.add_field(
            name="Why This Is Separate",
            value="Action tuning stays compact here so unrelated spam or GIF thresholds do not crowd every pack.",
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Pack-local action editor")

    def _pack_options_editor_embed(self, guild_id: int, pack: str, *, selected_lane: str | None = None) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title=f"{PACK_LABELS.get(pack, pack.title())} Options",
            description="Only the controls relevant to this pack are shown here. Use the lane selector first, then adjust that lane's state and thresholds.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Options", value="\n".join(self._pack_option_lines(config, pack)), inline=False)
        if pack in {"spam", "gif"} and selected_lane:
            lane_label = {
                **{str(item["key"]): str(item["label"]) for item in SPAM_OPTION_LANES},
                **{str(item["key"]): str(item["label"]) for item in GIF_OPTION_LANES},
            }.get(selected_lane, selected_lane.replace("_", " ").title())
            embed.add_field(name="Editing Lane", value=lane_label, inline=False)
        if pack == "severe":
            embed.add_field(
                name="Term Editing",
                value="Bundled and custom severe phrases stay under `/shield severe term` so this panel can stay compact.",
                inline=False,
            )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Pack-local options editor")

    def _pack_exemptions_editor_embed(self, guild_id: int, pack: str) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title=f"{PACK_LABELS.get(pack, pack.title())} Exemptions",
            description="Channel, role, and member exemptions here affect only this pack. Global filters stay under Scope.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Exemptions", value=self._pack_exemption_summary(config, pack), inline=False)
        embed.add_field(
            name="Editing Model",
            value="Each selector replaces that pack's saved set for channels, roles, or members so the state stays predictable.",
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Pack-local exemptions editor")

    def _link_policy_editor_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Shield Link Policy",
            description="Trusted-link policy is edited separately from the rule packs so it stays obvious and bounded.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Policy", value=self._link_policy_detail(config), inline=False)
        embed.add_field(
            name="Precedence",
            value="Built-in trusted pack -> local trusted overrides -> admin allowlists. Hard malicious, impersonation, adult, and suspicious-link intel still wins.",
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Trusted-link policy editor")

    def _format_trusted_family_lines(self, families: list[dict[str, object]], *, limit: int) -> str:
        if not families:
            return "None"
        lines = []
        for item in families[:limit]:
            examples = ", ".join(str(value) for value in item.get("examples", [])[:3]) or "no sample domains"
            state = "off here" if item.get("disabled") else "on"
            lines.append(f"`{item['name']}` ({item['count']}) | {state} | {examples}")
        if len(families) > limit:
            lines.append(f"+{len(families) - limit} more families")
        return "\n".join(lines)

    def _format_trusted_domain_lines(self, domains: list[dict[str, object]], *, limit: int) -> str:
        if not domains:
            return "None"
        lines = []
        for item in domains[:limit]:
            state = "off here" if item.get("disabled") else "on"
            lines.append(f"`{item['domain']}` | {state}")
        if len(domains) > limit:
            lines.append(f"+{len(domains) - limit} more domains")
        return "\n".join(lines)

    def _is_override_owner(self, user_id: int) -> bool:
        return user_id in SYSTEM_PREMIUM_OWNER_USER_IDS

    def _build_ai_override_embed(self, *, title: str, note: str, guild_id: int | None = None) -> discord.Embed:
        meta = self.service.get_meta()
        updated_by = meta["ordinary_ai_updated_by"]
        updated_at = meta["ordinary_ai_updated_at"] or "Never"
        updated_by_label = f"`{updated_by}`" if updated_by is not None else "`None`"
        embed = discord.Embed(
            title=title,
            description=note,
            color=ge.EMBED_THEME["accent"] if meta["ordinary_ai_enabled"] else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="AI Model Tiers",
            value=(
                f"Baseline tier: {self._format_ai_models(['gpt-5.4-nano'])}\n"
                f"Guild Pro tiers: {self._format_ai_models(['gpt-5.4-mini', 'gpt-5.4'])}\n"
                "Owner policy can list all three models by default, but provider/runtime readiness still gates frontier routing. "
                "AI stays second-pass only and owner policy controls whether review runs at all."
            ),
            inline=False,
        )
        embed.add_field(
            name="Default Owner Policy",
            value=(
                f"Enabled: **{'Yes' if meta['ordinary_ai_enabled'] else 'No'}**\n"
                f"Allowed models: {self._format_ai_models(list(meta['ordinary_ai_allowed_models']))}\n"
                f"Last updated by: {updated_by_label}\n"
                f"Last updated at: `{updated_at}`"
            ),
            inline=False,
        )
        if guild_id is not None:
            guild = self._guild_from_bot(guild_id)
            ai_status = self.service.get_ai_status(guild_id)
            guild_label = f"{getattr(guild, 'name', 'Unknown Guild')} (`{guild_id}`)"
            embed.add_field(
                name="Guild Policy",
                value=(
                    f"Guild: {guild_label}\n"
                    f"Enabled: **{'Yes' if ai_status['enabled'] else 'No'}**\n"
                    f"Source: {self._ai_policy_source_label(ai_status['policy_source'])}\n"
                    f"{self._shield_ai_entitlement_text(ai_status)}\n"
                    f"Guild access mode: `{ai_status['guild_access_mode']}`\n"
                    f"Guild model override: {self._format_ai_models(ai_status['guild_allowed_models_override'])}"
                ),
                inline=False,
            )
            embed.add_field(
                name="Review Scope",
                value=(
                    f"Minimum local confidence: `{ai_status['min_confidence']}`\n"
                    f"Eligible packs: {self._format_ai_pack_summary(ai_status['enabled_packs'])}\n"
                    f"Routing: {self._ai_routing_label(ai_status['routing_strategy'])}\n"
                    f"Provider ready: {'Yes' if ai_status['provider_available'] else 'No'}\n"
                    f"Provider diagnostics: {ai_status['provider_status']}\n"
                    f"Local blockers: {self._ai_setup_blocker_summary(ai_status['setup_blockers'])}"
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
            and bool(set(self._pack_policy_actions(config, pack)).intersection({"delete_log", "delete_escalate", "delete_timeout_log"}))
            for pack in ("privacy", "promo", "scam", "spam", "gif", "adult", "severe")
        )
        timeout_actions_enabled = any(
            config.get(f"{pack}_enabled")
            and bool(set(self._pack_policy_actions(config, pack)).intersection({"timeout_log", "delete_escalate", "delete_timeout_log"}))
            for pack in ("privacy", "promo", "scam", "spam", "gif", "adult", "severe")
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
            description="Essential Shield state. Use the tabs for rules, scope, links, AI, and logs.",
            color=ge.EMBED_THEME["warning"] if config["module_enabled"] else ge.EMBED_THEME["info"],
        )
        log_channel = f"<#{config['log_channel_id']}>" if config.get("log_channel_id") else "Not set"
        alert_role = f"<@&{config['alert_role_id']}>" if config.get("alert_role_id") else "None"
        embed.add_field(
            name="Live Status",
            value=(
                f"Enabled: **{'Yes' if config['module_enabled'] else 'No'}**\n"
                f"Scan mode: `{config['scan_mode']}`\n"
                f"Log channel: {log_channel}\n"
                f"Alert role: {alert_role}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Pack Status",
            value="\n".join(self._pack_status_line(config, pack) for pack in RULE_PANEL_PACKS),
            inline=False,
        )
        _, _, link_high_action = self._link_policy_actions(config)
        embed.add_field(
            name="Link Policy",
            value=(
                f"Mode: **{self._link_policy_label(config)}**\n"
                f"Strongest action: {self._action_label(link_high_action)}\n"
                f"Timeout: {self._pack_timeout_badge(config, 'link_policy')}"
            ),
            inline=False,
        )
        embed.add_field(
            name="AI Assist",
            value=(
                f"Readiness: {ai_status['status']}\n"
                f"Effective models right now: {self._format_ai_models(ai_status.get('effective_allowed_models', ai_status.get('allowed_models', [])))}\n"
                f"Policy source: {self._ai_policy_source_label(ai_status['policy_source'])}"
            ),
            inline=False,
        )
        return self._finalize_shield_embed(
            embed,
            footer="Babblebox Shield | Use /shield panel, module, escalation, rules, exemptions, links, trusted, filters, logs, allowlist, ai, or test",
        )

    def _rules_embed(self, guild_id: int, *, selected_pack: str | None = None) -> discord.Embed:
        config = self.service.get_config(guild_id)
        active_pack = selected_pack if selected_pack in RULE_PANEL_PACKS else self._default_rule_pack(guild_id)
        embed = discord.Embed(
            title="Shield Rules",
            description="Select a pack below in `/shield panel` and Babblebox only shows the controls relevant to that pack. `/shield module` owns the live toggle, `/shield escalation` owns repeated-hit fallbacks, and `/shield rules` stays pack-local.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Core Packs",
            value="\n\n".join(self._pack_overview_line(config, pack) for pack in CORE_RULE_PANEL_PACKS),
            inline=False,
        )
        embed.add_field(
            name="High-Risk Packs",
            value="\n\n".join(self._pack_overview_line(config, pack) for pack in HIGH_RISK_RULE_PANEL_PACKS),
            inline=False,
        )
        embed.add_field(name=f"{PACK_LABELS[active_pack]} Details", value=self._pack_detail_text(config, active_pack), inline=False)
        embed.add_field(
            name="Panel Flow",
            value=(
                "Use **Actions** for enable state, sensitivity, low/medium/high actions, and this pack's timeout profile.\n"
                "Use **Options** for pack-local thresholds only when that pack supports them.\n"
                "Use **Exemptions** for channels, roles, or members that should bypass only this pack."
            ),
            inline=False,
        )
        embed.add_field(
            name="Global Fallbacks",
            value=(
                f"Repeated-hit escalation: `{config['escalation_threshold']}` hits in `{config['escalation_window_minutes']}` minutes\n"
                f"Global timeout fallback: `{config['timeout_minutes']}` minutes\n"
                f"{self._custom_pattern_limit_text(guild_id, config)}\n"
                "Advanced patterns stay safe-text only. Raw user regex is intentionally unsupported."
            ),
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Pack-aware rules editor")

    def _links_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        trusted_state = self.service.trusted_pack_state(guild_id)
        embed = discord.Embed(
            title="Shield Link Policy",
            description="Trusted-link policy is a separate live-message lane, separate from Confessions link mode, and hard malicious, impersonation, adult, or suspicious-link evidence still wins over policy exceptions.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Policy", value=self._link_policy_detail(config), inline=False)
        embed.add_field(
            name="Built-In Trusted Pack",
            value=(
                f"Families:\n{self._format_trusted_family_lines(trusted_state['families'], limit=5)}\n\n"
                f"Direct domains:\n{self._format_trusted_domain_lines(trusted_state['direct_domains'], limit=5)}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Local Overrides",
            value=(
                f"Disabled built-in families: {self._format_text_list(trusted_state['disabled_families'], limit=6)}\n"
                f"Disabled built-in domains: {self._format_text_list(trusted_state['disabled_domains'], limit=6)}\n"
                f"Admin allowlisted domains: {self._format_text_list(trusted_state['allow_domains'], limit=6)}\n"
                f"Admin allowlisted invites: {self._format_text_list(trusted_state['allow_invite_codes'], limit=6)}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Precedence",
            value=(
                "1. Hard malicious, impersonation, adult, and strong suspicious-link evidence wins.\n"
                "2. Admin allowlisted domains or invites can add bounded trusted-only exceptions.\n"
                "3. Built-in trusted families and direct domains apply after subtracting any local built-in disables.\n"
                "4. Phrase allowlists do not change link trust. They only suppress targeted promo or adult-solicitation text matches."
            ),
            inline=False,
        )
        embed.add_field(
            name="Panel Flow",
            value="Use **Edit Link Policy** in `/shield panel` to tune mode, action ladder, and timeout profile. Built-in trusted families and domains still live under `/shield trusted`.",
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Trust stays visible, bounded, and override-aware")

    def _scope_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        embed = discord.Embed(
            title="Shield Scope and Allowlists",
            description="Control where Shield scans, who it skips globally, which entries are allowlisted, and which members, roles, or channels are exempted from a specific protection pack.",
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
            name="Allowlists and Targeted Carve-Outs",
            value=(
                f"Domains: {self._format_text_list(config['allow_domains'], limit=6)}\n"
                f"Invite codes: {self._format_text_list(config['allow_invite_codes'], limit=6)}\n"
                f"Phrases: {self._format_text_list(config['allow_phrases'], limit=4)}\n"
                "Domains / invites: trusted-link policy exceptions only\n"
                "Phrases: suppress promo or adult-solicitation text matches only\n"
                f"Solicitation carve-out channels: {self._format_mentions(config.get('adult_solicitation_excluded_channel_ids', []), kind='channel')}\n"
                f"Trusted-link mode: **{self._link_policy_label(config)}**\n"
                "Built-in trusted families or domains are managed under `/shield trusted`.\n"
                "Malicious, impersonation, suspicious-link, adult-domain, and scam protections still run."
            ),
            inline=False,
        )
        embed.add_field(
            name="Feature Surface Checks",
            value=(
                "AFK and reminders use privacy, adult, and severe checks.\n"
                "Watch stays privacy-only.\n"
                "Confessions shares link checks.\n"
                "Spam and GIF moderation stay live-message only."
            ),
            inline=False,
        )
        embed.add_field(
            name="Pack-Specific Exemptions",
            value="\n".join(
                f"**{PACK_LABELS[pack]}** | "
                f"{self._pack_exemption_counts(config, pack)[0]} channel(s), "
                f"{self._pack_exemption_counts(config, pack)[1]} role(s), "
                f"{self._pack_exemption_counts(config, pack)[2]} member(s)"
                for pack in RULE_PANEL_PACKS
            ),
            inline=False,
        )
        saved_exemption_lines = self._saved_pack_exemption_lines(guild_id, config)
        if saved_exemption_lines:
            embed.add_field(name="Saved Above Current Plan", value="\n".join(saved_exemption_lines), inline=False)
        embed.add_field(
            name="Editing Model",
            value=(
                "Global includes, excludes, trusted roles, and allowlists stay here.\n"
                "Pack-local exemptions now live in Rules -> Exemptions so unrelated scope settings do not clutter every pack."
            ),
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Global filters stay separate from pack-specific exemptions")

    def _ai_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        ai_status = self.service.get_ai_status(guild_id)
        log_channel = self._format_mentions([int(config["log_channel_id"])], kind="channel") if config.get("log_channel_id") else "Not set"
        embed = discord.Embed(
            title="Shield AI Assist",
            description="Second-pass review for already-flagged live messages only. Owner policy controls availability, the default model policy can list nano, mini, and full, and effective routing still depends on Guild Pro plus provider/runtime readiness.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Access Policy",
            value=(
                f"Enabled: **{'Yes' if ai_status['enabled'] else 'No'}**\n"
                f"Readiness: {ai_status['status']}\n"
                f"Policy source: {self._ai_policy_source_label(ai_status['policy_source'])}\n"
                f"{self._shield_ai_entitlement_text(ai_status, include_plan_allowed=True)}\n"
                f"Ordinary-guild default: {'Enabled' if ai_status['ordinary_global_enabled'] else 'Disabled'}\n"
                f"Guild model override: {self._format_ai_models(ai_status['guild_allowed_models_override'])}\n"
                f"Provider diagnostics: {ai_status['provider_status']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Provider and Routing",
            value=(
                f"Provider: {ai_status['provider'] or 'Not configured'}\n"
                f"Provider ready: {'Yes' if ai_status['provider_available'] else 'No'}\n"
                f"Routing mode: {self._ai_routing_label(ai_status['routing_strategy'])}\n"
                f"Fast tier: `{ai_status['fast_model'] or 'Not configured'}`\n"
                f"Complex tier: `{ai_status['complex_model'] or 'Not configured'}`\n"
                f"Frontier tier: `{ai_status['top_model'] or 'Not configured'}`\n"
                f"Frontier enabled: {'Yes' if ai_status['top_tier_enabled'] else 'No'}\n"
                f"Provider model gate: {ai_status.get('provider_model_note') or 'None'}\n"
                f"Ignored invalid model settings: {self._format_text_list(ai_status['ignored_model_settings'], limit=4)}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Runtime Policy",
            value=(
                f"Live moderation: {'On' if config['module_enabled'] else 'Off'}\n"
                f"Shield log channel: {log_channel}\n"
                f"Local-confidence threshold: `{config['ai_min_confidence']}`\n"
                f"Eligible packs: {self._format_ai_pack_summary(ai_status['enabled_packs'])}\n"
                f"Local blockers: {self._ai_setup_blocker_summary(ai_status['setup_blockers'])}\n"
                "Live-message only: Yes\n"
                "Punishment engine: Never\n"
                "Spam and GIF moderation stay local and non-AI"
            ),
            inline=False,
        )
        embed.add_field(
            name="Privacy Boundaries",
            value=(
                "Only already-flagged messages are eligible.\n"
                "Babblebox redacts obvious private patterns, truncates content, and avoids sending broad history or attachment bodies.\n"
                "AI output only enriches moderator alerts. It never directly deletes, times out, or punishes users.\n"
                "AFK, reminders, watch keywords, and Confessions feature-surface checks stay local and AI-free."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value="`/shield ai min_confidence:high privacy:true promo:false scam:true adult:true severe:true`",
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield AI | Review scope is admin-configurable; baseline nano stays available and Guild Pro can make mini/full available when policy and provider/runtime readiness allow review")

    def _logs_embed(self, guild_id: int) -> discord.Embed:
        config = self.service.get_config(guild_id)
        log_channel = f"<#{config['log_channel_id']}>" if config.get("log_channel_id") else "Not set"
        alert_role = f"<@&{config['alert_role_id']}>" if config.get("alert_role_id") else "None"
        embed = discord.Embed(
            title="Shield Logs",
            description="Calm, pack-aware delivery controls keep Shield readable without hiding serious incidents.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Global Delivery",
            value=(
                f"Log channel: {log_channel}\n"
                f"Alert role: {alert_role}\n"
                f"Log style: {self._log_style_label(str(config.get('log_style', 'adaptive')))}\n"
                f"Ping mode: {self._log_ping_mode_label(str(config.get('log_ping_mode', 'smart')))}\n"
                "Alerts are deduped so one message does not spray repeated moderator notices.\n"
                "Low-confidence repeated-link notes still stay compact and no-ping in adaptive mode.\n"
                "Collective GIF pressure always stays channel-level and no-ping, even when GIF high action is stronger."
            ),
            inline=False,
        )
        embed.add_field(
            name="Per-Pack Overrides",
            value=(
                "\n".join(
                    f"**{PACK_LABELS[pack]}** | {self._pack_log_override_summary(config, pack)}"
                    for pack in RULE_PANEL_PACKS
                )
            ),
            inline=False,
        )
        embed.add_field(
            name="Behavior Notes",
            value=(
                "Compact mode keeps the calmer note layout across Shield packs while preserving detection, reason, preview, and jump context.\n"
                "Smart ping mode keeps current high-signal ping behavior for serious or actioned incidents.\n"
                "Never ping mode still logs everything to the configured channel but suppresses alert-role mentions."
            ),
            inline=False,
        )
        embed.add_field(
            name="Quick Use",
            value=(
                "`/shield logs channel:#shield-log role:@Mods style:compact ping_mode:never`\n"
                "`/shield logs override_pack:gif override_style:compact override_ping_mode:never`\n"
                "`/shield logs clear_channel:true clear_role:true`"
            ),
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Calm delivery, compact by policy when you want it")

    def _logs_editor_embed(self, guild_id: int, *, selected_pack: str) -> discord.Embed:
        config = self.service.get_config(guild_id)
        local_style, local_ping_mode, effective_style, effective_ping_mode = self._resolved_pack_log_delivery(config, selected_pack)
        embed = discord.Embed(
            title="Shield Log Delivery",
            description="Global defaults live at the top. One selected-pack override lives underneath so the editor stays compact.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Global Defaults",
            value=(
                f"Style: {self._log_style_label(str(config.get('log_style', 'adaptive')))}\n"
                f"Ping mode: {self._log_ping_mode_label(str(config.get('log_ping_mode', 'smart')))}\n"
                "Adaptive keeps today's smart behavior. Compact forces the calmer note layout.\n"
                "Smart ping preserves current high-signal mentions; Never ping suppresses all alert-role pings."
            ),
            inline=False,
        )
        embed.add_field(
            name=f"{PACK_LABELS[selected_pack]} Override",
            value=(
                f"Local style: {self._log_style_label(local_style)}\n"
                f"Local ping mode: {self._log_ping_mode_label(local_ping_mode)}\n"
                f"Effective style: {self._log_style_label(effective_style)}\n"
                f"Effective ping mode: {self._log_ping_mode_label(effective_ping_mode)}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Reminder",
            value=(
                "Collective GIF pressure still stays channel-level and no-ping even if GIF override ping mode is smart.\n"
                "Log channel and alert role remain under `/shield logs` so selector-based delivery tuning stays uncluttered here."
            ),
            inline=False,
        )
        return self._finalize_shield_embed(embed, footer="Babblebox Shield | Pack-aware log delivery editor")

    def build_panel_embed(
        self,
        guild_id: int,
        section: str,
        *,
        channel_id: int | None = None,
        selected_pack: str | None = None,
    ) -> discord.Embed:
        if section == "rules":
            embed = self._rules_embed(guild_id, selected_pack=selected_pack)
        elif section == "links":
            embed = self._links_embed(guild_id)
        elif section == "scope":
            embed = self._scope_embed(guild_id)
        elif section == "ai":
            embed = self._ai_embed(guild_id)
        elif section == "logs":
            embed = self._logs_embed(guild_id)
        else:
            embed = self._overview_embed(guild_id)
        return self._finalize_shield_embed(
            self._add_operability_field(embed, guild_id, channel_id=channel_id),
            footer=getattr(embed.footer, "text", None) or "Babblebox Shield",
        )

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

    @shield_group.command(name="module", with_app_command=True, description="Turn Shield live moderation on or off")
    @app_commands.describe(enabled="Turn the live Shield moderation module on or off")
    async def shield_module_command(self, ctx: commands.Context, enabled: bool):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_module_enabled(ctx.guild.id, enabled)
        await self._send_result(ctx, "Shield Module", message, ok=ok)

    @shield_group.command(
        name="escalation",
        with_app_command=True,
        description="Configure repeated-hit escalation and the global Shield timeout fallback",
    )
    @app_commands.describe(
        threshold="Repeated-hit threshold used by delete_escalate",
        window_minutes="Strike window used by delete_escalate",
        timeout_minutes="Global timeout length used when escalation or timeout actions fire",
    )
    async def shield_escalation_command(
        self,
        ctx: commands.Context,
        threshold: Optional[int] = None,
        window_minutes: Optional[int] = None,
        timeout_minutes: Optional[int] = None,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (threshold, window_minutes, timeout_minutes)):
            await send_hybrid_response(ctx, embed=self._rules_embed(ctx.guild.id), ephemeral=True)
            return
        ok, message = await self.service.set_escalation(
            ctx.guild.id,
            threshold=threshold,
            window_minutes=window_minutes,
            timeout_minutes=timeout_minutes,
        )
        await self._send_result(ctx, "Shield Escalation", message, ok=ok)

    @shield_group.command(name="rules", with_app_command=True, description="Configure one Shield pack's actions and thresholds")
    @app_commands.describe(
        pack="Which protection pack to adjust",
        enabled="Turn that pack on or off",
        action="Shorthand to use one graduated policy derived from a single action",
        low_action="Action for broad or uncertain low-confidence matches",
        medium_action="Action for medium-confidence matches",
        high_action="Action for high-confidence matches",
        sensitivity="How broad or cautious the pack should be",
        adult_solicitation="Enable the adult pack's optional solicitation / DM-ad text detector",
        message_threshold="Spam or GIF message count threshold inside the configured window",
        window_seconds="Window size in seconds for spam or GIF threshold checks",
        burst_threshold="Fast-burst message count threshold for the spam pack",
        burst_window_seconds="Fast-burst window in seconds for the spam pack",
        duplicate_threshold="Near-duplicate threshold for the spam pack",
        duplicate_window_seconds="Near-duplicate window in seconds for the spam pack",
        emote_enabled="Enable or disable the spam pack's optional emote clutter lane",
        emote_threshold="Emoji or emote token threshold for the spam pack",
        caps_enabled="Enable or disable the spam pack's optional excessive-capitals lane",
        caps_threshold="Uppercase-letter threshold for the spam pack",
        low_value_enabled="Enable or disable the spam pack's optional low-value chatter lane",
        moderator_policy="How the spam pack should treat moderators by default",
        consecutive_threshold="Channel-level consecutive GIF threshold for the GIF pack",
        repeat_threshold="Low-text GIF repeat threshold for the GIF pack",
        same_asset_threshold="Same-GIF asset repeat threshold for the GIF pack",
        ratio_percent="Minimum effective GIF-share ratio for the GIF pack",
        timeout_minutes="Pack-specific timeout override. Use `/shield escalation timeout_minutes:...` for the global Shield timeout fallback.",
    )
    @app_commands.choices(
        pack=PACK_CHOICES,
        action=ACTION_CHOICES,
        low_action=LOW_ACTION_CHOICES,
        medium_action=MEDIUM_ACTION_CHOICES,
        high_action=ACTION_CHOICES,
        sensitivity=SENSITIVITY_CHOICES,
        moderator_policy=SPAM_MODERATOR_POLICY_CHOICES,
    )
    async def shield_rules_command(
        self,
        ctx: commands.Context,
        pack: Optional[str] = None,
        enabled: Optional[bool] = None,
        action: Optional[str] = None,
        low_action: Optional[str] = None,
        medium_action: Optional[str] = None,
        high_action: Optional[str] = None,
        sensitivity: Optional[str] = None,
        adult_solicitation: Optional[bool] = None,
        message_threshold: Optional[int] = None,
        window_seconds: Optional[int] = None,
        burst_threshold: Optional[int] = None,
        burst_window_seconds: Optional[int] = None,
        duplicate_threshold: Optional[int] = None,
        duplicate_window_seconds: Optional[int] = None,
        emote_enabled: Optional[bool] = None,
        emote_threshold: Optional[int] = None,
        caps_enabled: Optional[bool] = None,
        caps_threshold: Optional[int] = None,
        low_value_enabled: Optional[bool] = None,
        moderator_policy: Optional[str] = None,
        consecutive_threshold: Optional[int] = None,
        repeat_threshold: Optional[int] = None,
        same_asset_threshold: Optional[int] = None,
        ratio_percent: Optional[int] = None,
        timeout_minutes: Optional[int] = None,
    ):
        if not await self._guard(ctx):
            return
        messages: list[str] = []
        ok = True
        pack_fields_used = any(
            value is not None
            for value in (
                enabled,
                action,
                low_action,
                medium_action,
                high_action,
                sensitivity,
                adult_solicitation,
                message_threshold,
                window_seconds,
                burst_threshold,
                burst_window_seconds,
                duplicate_threshold,
                duplicate_window_seconds,
                emote_enabled,
                emote_threshold,
                caps_enabled,
                caps_threshold,
                low_value_enabled,
                moderator_policy,
                consecutive_threshold,
                repeat_threshold,
                same_asset_threshold,
                ratio_percent,
            )
        )
        if pack_fields_used and pack is None:
            ok = False
            messages.append("Choose a pack when changing pack enabled, action policy, sensitivity, or explicit thresholds.")
        elif pack is not None and pack_fields_used:
            pack_ok, pack_message = await self.service.set_pack_config(
                ctx.guild.id,
                pack,
                enabled=enabled,
                action=action,
                low_action=low_action,
                medium_action=medium_action,
                high_action=high_action,
                sensitivity=sensitivity,
                adult_solicitation=adult_solicitation,
                message_threshold=message_threshold,
                window_seconds=window_seconds,
                burst_threshold=burst_threshold,
                burst_window_seconds=burst_window_seconds,
                duplicate_threshold=duplicate_threshold,
                duplicate_window_seconds=duplicate_window_seconds,
                emote_enabled=emote_enabled,
                emote_threshold=emote_threshold,
                caps_enabled=caps_enabled,
                caps_threshold=caps_threshold,
                low_value_enabled=low_value_enabled,
                moderator_policy=moderator_policy,
                consecutive_threshold=consecutive_threshold,
                repeat_threshold=repeat_threshold,
                same_asset_threshold=same_asset_threshold,
                ratio_percent=ratio_percent,
            )
            ok = ok and pack_ok
            messages.append(pack_message)
        if timeout_minutes is not None and pack is None:
            ok = False
            messages.append(
                "Use `/shield escalation timeout_minutes:...` to change the global Shield timeout fallback. "
                "`/shield rules timeout_minutes` only applies when `pack` is selected."
            )
        if pack is not None and timeout_minutes is not None:
            timeout_ok, timeout_message = await self.service.set_pack_timeout_override(ctx.guild.id, pack, timeout_minutes)
            ok = ok and timeout_ok
            messages.append(timeout_message)
        if not messages:
            await send_hybrid_response(ctx, embed=self._rules_embed(ctx.guild.id, selected_pack=pack), ephemeral=True)
            return
        await self._send_result(ctx, "Shield Rules", "\n".join(messages), ok=ok)

    @shield_group.command(name="links", with_app_command=True, description="Configure Shield's trusted-link policy lane")
    @app_commands.describe(
        mode="Use the current broad behavior or require trusted mainstream destinations plus bounded domain or invite policy exceptions",
        action="Shorthand to derive the trusted-link policy action ladder from one action",
        low_action="Action for safe-but-untrusted low-confidence policy matches",
        medium_action="Action for medium-confidence policy matches such as invites or link hubs",
        high_action="Action for dangerous high-confidence policy matches that still fall through to the policy lane",
        timeout_minutes="Optional dedicated timeout override for the trusted-link lane; omit it to keep using the global Shield timeout.",
    )
    @app_commands.choices(
        mode=LINK_POLICY_MODE_CHOICES,
        action=ACTION_CHOICES,
        low_action=LOW_ACTION_CHOICES,
        medium_action=MEDIUM_ACTION_CHOICES,
        high_action=ACTION_CHOICES,
    )
    async def shield_links_command(
        self,
        ctx: commands.Context,
        mode: Optional[str] = None,
        action: Optional[str] = None,
        low_action: Optional[str] = None,
        medium_action: Optional[str] = None,
        high_action: Optional[str] = None,
        timeout_minutes: Optional[int] = None,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (mode, action, low_action, medium_action, high_action, timeout_minutes)):
            await send_hybrid_response(ctx, embed=self._links_embed(ctx.guild.id), ephemeral=True)
            return
        messages: list[str] = []
        ok, message = await self.service.set_link_policy_config(
            ctx.guild.id,
            mode=mode,
            action=action,
            low_action=low_action,
            medium_action=medium_action,
            high_action=high_action,
        )
        messages.append(message)
        if timeout_minutes is not None:
            timeout_ok, timeout_message = await self.service.set_link_policy_timeout_override(ctx.guild.id, timeout_minutes)
            ok = ok and timeout_ok
            messages.append(timeout_message)
        await self._send_result(ctx, "Shield Link Policy", "\n".join(messages), ok=ok)

    @shield_group.group(
        name="trusted",
        with_app_command=True,
        invoke_without_command=True,
        description="Inspect or tune Shield's built-in trusted families and domains",
    )
    async def shield_trusted_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._links_embed(ctx.guild.id), ephemeral=True)

    @shield_trusted_group.command(name="view", description="Show Shield's built-in trusted pack and local overrides")
    async def shield_trusted_view_command(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._links_embed(ctx.guild.id), ephemeral=True)

    @shield_trusted_group.command(name="family", description="Turn one built-in trusted family on or off for this server")
    @app_commands.describe(family="Built-in trusted family name, such as docs or dev", state="Turn that family on or off")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_trusted_family_command(self, ctx: commands.Context, family: str, state: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_trusted_builtin_family_enabled(ctx.guild.id, family, state == "on")
        await self._send_result(ctx, "Shield Trusted Families", message, ok=ok)

    @shield_trusted_group.command(name="domain", description="Turn one built-in trusted domain on or off for this server")
    @app_commands.describe(domain="Built-in trusted domain, such as google.com", state="Turn that domain on or off")
    @app_commands.choices(state=STATE_CHOICES)
    async def shield_trusted_domain_command(self, ctx: commands.Context, domain: str, state: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_trusted_builtin_domain_enabled(ctx.guild.id, domain, state == "on")
        await self._send_result(ctx, "Shield Trusted Domains", message, ok=ok)

    @shield_group.group(
        name="severe",
        with_app_command=True,
        invoke_without_command=True,
        description="Configure severe-harm categories and bundled or custom terms",
    )
    async def shield_severe_group(self, ctx: commands.Context):
        if not await self._guard(ctx):
            return
        await send_hybrid_response(ctx, embed=self._rules_embed(ctx.guild.id, selected_pack="severe"), ephemeral=True)

    @shield_severe_group.command(name="category", description="Turn a severe-harm category on or off")
    @app_commands.describe(category="Which severe-harm category to change", state="Turn that category on or off")
    @app_commands.choices(category=SEVERE_CATEGORY_CHOICES, state=STATE_CHOICES)
    async def shield_severe_category_command(self, ctx: commands.Context, category: str, state: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.set_severe_category(ctx.guild.id, category, state == "on")
        await self._send_result(ctx, "Shield Severe Categories", message, ok=ok)

    @shield_severe_group.command(name="term", description="Manage bundled and custom severe-harm terms")
    @app_commands.describe(action="What to do with the phrase", phrase="The exact bundled or custom severe phrase")
    @app_commands.choices(action=SEVERE_TERM_ACTION_CHOICES)
    async def shield_severe_term_command(self, ctx: commands.Context, action: str, phrase: str):
        if not await self._guard(ctx):
            return
        ok, message = await self.service.update_severe_term(ctx.guild.id, action, phrase)
        await self._send_result(ctx, "Shield Severe Terms", message, ok=ok)

    @shield_group.command(name="logs", with_app_command=True, description="Configure Shield log delivery")
    @app_commands.describe(
        channel="Channel for Shield alerts",
        role="Optional role to ping for alerts",
        style="Global default log layout",
        ping_mode="Global default alert-role ping behavior",
        override_pack="Optional per-pack override target",
        override_style="Override log style for the selected pack",
        override_ping_mode="Override ping behavior for the selected pack",
        clear_channel="Clear the current log channel",
        clear_role="Clear the current alert role",
    )
    @app_commands.choices(
        style=LOG_STYLE_CHOICES,
        ping_mode=LOG_PING_MODE_CHOICES,
        override_pack=PACK_CHOICES,
        override_style=PACK_LOG_STYLE_CHOICES,
        override_ping_mode=PACK_LOG_PING_CHOICES,
    )
    async def shield_logs_command(
        self,
        ctx: commands.Context,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
        style: Optional[str] = None,
        ping_mode: Optional[str] = None,
        override_pack: Optional[str] = None,
        override_style: Optional[str] = None,
        override_ping_mode: Optional[str] = None,
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
        if style is not None or ping_mode is not None:
            delivery_ok, delivery_message = await self.service.set_log_delivery(ctx.guild.id, style=style, ping_mode=ping_mode)
            ok = ok and delivery_ok
            messages.append(delivery_message)
        if override_pack is not None or override_style is not None or override_ping_mode is not None:
            if override_pack is None:
                ok = False
                messages.append("Choose `override_pack` when saving a per-pack Shield log override.")
            else:
                override_ok, override_message = await self.service.set_pack_log_override(
                    ctx.guild.id,
                    override_pack,
                    style=override_style,
                    ping_mode=override_ping_mode,
                )
                ok = ok and override_ok
                messages.append(override_message)
        if not messages:
            await send_hybrid_response(ctx, embed=self._logs_embed(ctx.guild.id), ephemeral=True)
            return
        await self._send_result(ctx, "Shield Logs", "\n".join(messages), ok=ok)

    @shield_group.command(name="filters", with_app_command=True, description="Configure Shield scope, includes, excludes, trusted roles, and solicitation carve-outs")
    @app_commands.describe(
        mode="Scan everything eligible or only explicitly included scope",
        target="Which include/exclude/trust bucket or solicitation carve-out to change",
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

    @shield_group.command(name="exemptions", with_app_command=True, description="Configure pack-specific member, role, or channel exemptions")
    @app_commands.describe(
        pack="Which Shield pack should ignore this target",
        target="What kind of target to exempt on that pack",
        state="Turn that pack exemption on or off",
        channel="Channel target for channel exemptions",
        role="Role target for role exemptions",
        user="Member target for member exemptions",
    )
    @app_commands.choices(pack=PACK_CHOICES, target=PACK_EXEMPTION_TARGET_CHOICES, state=STATE_CHOICES)
    async def shield_exemptions_command(
        self,
        ctx: commands.Context,
        pack: str,
        target: str,
        state: str,
        channel: Optional[discord.TextChannel] = None,
        role: Optional[discord.Role] = None,
        user: Optional[discord.Member] = None,
    ):
        if not await self._guard(ctx):
            return
        if target == "channel":
            resolved_target = channel or ctx.channel
        elif target == "role":
            resolved_target = role
        else:
            resolved_target = user
        target_id = getattr(resolved_target, "id", None)
        if not isinstance(target_id, int):
            await self._send_result(
                ctx,
                "Shield Exemptions",
                "Select the member, role, or channel that should be exempted on that pack.",
                ok=False,
            )
            return
        ok, message = await self.service.set_pack_exemption(ctx.guild.id, pack, target, target_id, state == "on")
        await self._send_result(ctx, "Shield Exemptions", message, ok=ok)

    @shield_group.command(name="allowlist", with_app_command=True, description="Configure Shield's bounded domain, invite, and phrase carve-outs")
    @app_commands.describe(
        bucket="Which bounded allowlist bucket to change",
        state="Turn this allowlist entry on or off",
        value="The domain, invite code, or phrase to change",
    )
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

    @shield_group.command(name="ai", with_app_command=True, description="Configure Shield AI review scope for already-flagged live messages")
    @app_commands.describe(
        min_confidence="Minimum local Shield confidence needed before AI review is attempted",
        privacy="Allow AI review for privacy-pack hits",
        promo="Allow AI review for promo-pack hits",
        scam="Allow AI review for scam-pack hits",
        adult="Allow AI review for adult-pack hits",
        severe="Allow AI review for severe-pack hits",
    )
    @app_commands.choices(min_confidence=AI_CONFIDENCE_CHOICES)
    async def shield_ai_command(
        self,
        ctx: commands.Context,
        min_confidence: Optional[str] = None,
        privacy: Optional[bool] = None,
        promo: Optional[bool] = None,
        scam: Optional[bool] = None,
        adult: Optional[bool] = None,
        severe: Optional[bool] = None,
    ):
        if not await self._guard(ctx):
            return
        if all(value is None for value in (min_confidence, privacy, promo, scam, adult, severe)):
            await send_hybrid_response(ctx, embed=self._ai_embed(ctx.guild.id), ephemeral=True)
            return
        current = self.service.get_config(ctx.guild.id)
        next_packs = list(current.get("ai_enabled_packs", []))
        for pack, state in (("privacy", privacy), ("promo", promo), ("scam", scam), ("adult", adult), ("severe", severe)):
            if state is None:
                continue
            if state and pack not in next_packs:
                next_packs.append(pack)
            if not state and pack in next_packs:
                next_packs.remove(pack)
        ok, message = await self.service.set_ai_config(
            ctx.guild.id,
            min_confidence=min_confidence,
            enabled_packs=next_packs if any(value is not None for value in (privacy, promo, scam, adult, severe)) else None,
        )
        if ok:
            ai_status = self.service.get_ai_status(ctx.guild.id)
            message = self._shield_ai_scope_update_text(
                ai_status,
                pack_summary=self._format_ai_pack_summary(ai_status["enabled_packs"]),
            )
        await self._send_result(ctx, "Shield AI", message, ok=ok)

    @commands.command(name="shieldai", hidden=True)
    async def shield_ai_owner_command(self, ctx: commands.Context, *parts: str):
        if ctx.guild is not None:
            await ctx.send(content="That command is only available in DM.")
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_override_owner(author_id):
            LOGGER.warning(
                "Shield AI owner command denied: unauthorized_dm_user_id=%s",
                author_id,
            )
            await ctx.send(content="That command is unavailable.")
            return

        tokens = [str(part).strip() for part in parts if str(part).strip()]
        if not tokens:
            tokens = ["status"]

        root = tokens[0].casefold()
        usage = (
            "Use `status`, `global status|enable [models]|disable|models <csv>`, "
            "`guild <id> status|enable [models]|disable|models <csv>|inherit`, or `support status|defaults`."
        )

        if root == "status":
            await ctx.send(
                embed=self._build_ai_override_embed(
                    title="Shield AI Owner Policy",
                    note="Private maintainer status for support defaults, global ordinary-guild defaults, and guild overrides.",
                )
            )
            return

        if root == "global":
            if len(tokens) == 1 or tokens[1].casefold() == "status":
                await ctx.send(
                    embed=self._build_ai_override_embed(
                        title="Shield AI Global Policy",
                        note="Ordinary-guild default policy for Shield AI access.",
                    )
                )
                return
            subcommand = tokens[1].casefold()
            if subcommand == "enable":
                models = ",".join(tokens[2:]) if len(tokens) > 2 else None
                ok, message = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models=models, actor_id=author_id)
            elif subcommand == "disable":
                ok, message = await self.service.set_ordinary_ai_policy(enabled=False, actor_id=author_id)
            elif subcommand == "models":
                if len(tokens) < 3:
                    await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Global Policy", note=usage))
                    return
                ok, message = await self.service.set_ordinary_ai_policy(enabled=None, allowed_models=",".join(tokens[2:]), actor_id=author_id)
            else:
                await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Global Policy", note=usage))
                return
            await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Global Policy", note=message if ok else f"Update failed: {message}"))
            return

        if root == "guild":
            if len(tokens) < 3:
                await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Guild Policy", note=usage))
                return
            try:
                guild_id = int(tokens[1])
            except ValueError:
                await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Guild Policy", note="Guild IDs must be numeric."))
                return
            subcommand = tokens[2].casefold()
            if subcommand == "status":
                await ctx.send(
                    embed=self._build_ai_override_embed(
                        title="Shield AI Guild Policy",
                        note="Private maintainer status for this guild's resolved Shield AI policy.",
                        guild_id=guild_id,
                    )
                )
                return
            if subcommand == "enable":
                models = ",".join(tokens[3:]) if len(tokens) > 3 else None
                ok, message = await self.service.set_guild_ai_access_policy(
                    guild_id,
                    mode="enabled",
                    allowed_models=models,
                    actor_id=author_id,
                )
            elif subcommand == "disable":
                ok, message = await self.service.set_guild_ai_access_policy(guild_id, mode="disabled", actor_id=author_id)
            elif subcommand == "models":
                if len(tokens) < 4:
                    await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Guild Policy", note=usage, guild_id=guild_id))
                    return
                ok, message = await self.service.set_guild_ai_access_policy(
                    guild_id,
                    allowed_models=",".join(tokens[3:]),
                    actor_id=author_id,
                )
            elif subcommand == "inherit":
                ok, message = await self.service.inherit_guild_ai_access_policy(guild_id, actor_id=author_id)
            else:
                await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Guild Policy", note=usage, guild_id=guild_id))
                return
            await ctx.send(
                embed=self._build_ai_override_embed(
                    title="Shield AI Guild Policy",
                    note=message if ok else f"Update failed: {message}",
                    guild_id=guild_id,
                )
            )
            return

        if root == "support":
            if len(tokens) == 1 or tokens[1].casefold() == "status":
                await ctx.send(
                    embed=self._build_ai_override_embed(
                        title="Shield AI Support Policy",
                        note="Private maintainer status for the support server's inherited Shield AI policy.",
                        guild_id=SHIELD_AI_SUPPORT_GUILD_ID,
                    )
                )
                return
            if tokens[1].casefold() == "defaults":
                ok, message = await self.service.restore_support_ai_defaults(actor_id=author_id)
                await ctx.send(
                    embed=self._build_ai_override_embed(
                        title="Shield AI Support Policy",
                        note=message if ok else f"Update failed: {message}",
                        guild_id=SHIELD_AI_SUPPORT_GUILD_ID,
                    )
                )
                return

        await ctx.send(embed=self._build_ai_override_embed(title="Shield AI Owner Policy", note=usage))

    @shield_group.command(name="test", with_app_command=True, description="Dry-run a message through the current Shield rules")
    async def shield_test_command(self, ctx: commands.Context, text: str):
        if not await self._guard(ctx):
            return
        channel_id = getattr(ctx.channel, "id", None) if ctx.guild is not None else None
        result = self.service.test_message_details(ctx.guild.id, text, channel_id=channel_id)
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
                    f"{self._action_label(item.action)} | {CONFIDENCE_LABELS.get(item.confidence, item.confidence)}"
                    for item in result.matches[:5]
                ),
                inline=False,
            )
        if result.link_explanations:
            embed.add_field(
                name="Link Decisions",
                value=self.service.format_link_decision_lines(result.link_explanations, limit=5),
                inline=False,
            )
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

