from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Awaitable, Callable

import discord

from babblebox import game_engine as ge
from babblebox.admin_service import (
    FOLLOWUP_MODE_LABELS,
    VERIFICATION_DEADLINE_ACTION_LABELS,
    VERIFICATION_LOGIC_LABELS,
    VerificationSyncSession,
    _followup_duration_label,
)
from babblebox.utility_helpers import format_duration_brief, parse_duration_string


if TYPE_CHECKING:
    from babblebox.cogs.admin import AdminCog


FOLLOWUP_DURATION_PRESETS: tuple[tuple[str, str], ...] = (
    ("14d", "2 weeks"),
    ("30d", "30 days"),
    ("6w", "6 weeks"),
    ("3mo", "3 months"),
    ("6mo", "6 months"),
)
VERIFICATION_KICK_AFTER_PRESETS: tuple[tuple[str, str], ...] = (
    ("3d", "3 days"),
    ("7d", "1 week"),
    ("14d", "2 weeks"),
    ("30d", "30 days"),
)
VERIFICATION_WARNING_LEAD_PRESETS: tuple[tuple[str, str], ...] = (
    ("12h", "12 hours"),
    ("1d", "1 day"),
    ("2d", "2 days"),
    ("3d", "3 days"),
    ("7d", "1 week"),
)
VERIFICATION_HELP_EXTENSION_PRESETS: tuple[tuple[str, str], ...] = (
    ("12h", "12 hours"),
    ("1d", "1 day"),
    ("2d", "2 days"),
    ("3d", "3 days"),
    ("7d", "1 week"),
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


class AdminManagedView(discord.ui.View):
    panel_title = "Admin Panel"
    stale_message = "That admin panel expired. Run `/admin panel` again to open a fresh one."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, timeout: float | None = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.message: discord.Message | None = None
        self._expired = False

    async def current_embed(self) -> discord.Embed:
        raise NotImplementedError

    def _refresh_items(self):
        return

    async def _sync_parent_panel(self):
        return

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self._expired or interaction.is_expired():
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return False
        if interaction.user.id != self.author_id:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/admin panel` to open your own admin panel.",
                    tone="info",
                    footer="Babblebox Admin",
                ),
            )
            return False
        if not self.cog.user_can_manage_admin(interaction.user):
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure these admin systems.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
            )
            return False
        return True

    async def on_timeout(self):
        self._expired = True
        for child in self.children:
            child.disabled = True
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException, discord.NotFound, AttributeError):
                await self.message.edit(view=self)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        await self.cog._send_private_interaction(
            interaction,
            embed=self.cog._admin_status_embed(
                self.panel_title,
                f"Babblebox could not finish that {self.panel_title.lower()} action. Run `/admin panel` again if this view feels stale.",
                ok=False,
            ),
        )

    async def refresh_message(self):
        self._refresh_items()
        if self.message is not None:
            with contextlib.suppress(discord.HTTPException, discord.NotFound, AttributeError):
                await self.message.edit(embed=await self.current_embed(), view=self)

    async def _rerender(self, interaction: discord.Interaction, *, note: str | None = None, note_ok: bool = True):
        self._refresh_items()
        updated = await self.cog._edit_interaction_message(interaction, embed=await self.current_embed(), view=self)
        await self._sync_parent_panel()
        if not updated:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.panel_title, self.stale_message, ok=False),
            )
            return
        if note:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.panel_title, note, ok=note_ok),
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
                embed=self.cog._admin_status_embed(self.panel_title, failure_message, ok=False),
            )
            return None

    async def _open_modal(self, interaction: discord.Interaction, modal: discord.ui.Modal, *, failure_message: str):
        try:
            await interaction.response.send_modal(modal)
        except Exception:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.panel_title, failure_message, ok=False),
            )


class AdminManagedModal(discord.ui.Modal):
    def __init__(self, managed_view: AdminManagedView, *, title: str):
        super().__init__(title=title)
        self.managed_view = managed_view
        self.cog = managed_view.cog

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.managed_view._expired or interaction.is_expired():
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.managed_view.panel_title, self.managed_view.stale_message, ok=False),
            )
            return False
        if interaction.user.id != self.managed_view.author_id:
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/admin panel` to open your own admin panel.",
                    tone="info",
                    footer="Babblebox Admin",
                ),
            )
            return False
        if not self.cog.user_can_manage_admin(interaction.user):
            await self.cog._send_private_interaction(
                interaction,
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure these admin systems.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
            )
            return False
        return True

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await self.cog._send_private_interaction(
            interaction,
            embed=self.cog._admin_status_embed(
                self.managed_view.panel_title,
                f"Babblebox could not finish that {self.managed_view.panel_title.lower()} update.",
                ok=False,
            ),
        )


class AdminTextInputModal(AdminManagedModal):
    def __init__(
        self,
        managed_view: AdminManagedView,
        *,
        title: str,
        label: str,
        placeholder: str,
        default_value: str = "",
        multiline: bool = False,
        submit_handler: Callable[[str], Awaitable[tuple[bool, str]]],
        failure_message: str,
    ):
        super().__init__(managed_view, title=title)
        self.submit_handler = submit_handler
        self.failure_message = failure_message
        self.value_input = discord.ui.TextInput(
            label=label,
            placeholder=placeholder,
            default=default_value,
            required=False,
            style=discord.TextStyle.paragraph if multiline else discord.TextStyle.short,
        )
        self.add_item(self.value_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            ok, message = await self.submit_handler(str(self.value_input.value or ""))
        except Exception:
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog._admin_status_embed(self.managed_view.panel_title, self.failure_message, ok=False),
            )
            return
        await self.managed_view.refresh_message()
        await self.managed_view._sync_parent_panel()
        await self.cog._send_private_interaction(
            interaction,
            embed=self.cog._admin_status_embed(self.managed_view.panel_title, message, ok=ok),
        )


class AdminPanelChildView(AdminManagedView):
    def __init__(
        self,
        cog: "AdminCog",
        *,
        guild_id: int,
        author_id: int,
        panel_view: "AdminPanelView | None" = None,
        timeout: float | None = 180,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, timeout=timeout)
        self.panel_view = panel_view

    async def _sync_parent_panel(self):
        if self.panel_view is not None:
            await self.panel_view.refresh_message()


class AdminPanelView(AdminManagedView):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, section: str = "overview"):
        super().__init__(cog, guild_id=guild_id, author_id=author_id)
        self.section = section
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        return await self.cog.build_panel_embed(self.guild_id, self.section)

    async def _switch_section(self, interaction: discord.Interaction, section: str):
        async def action():
            self.section = section
            await self._rerender(interaction)

        await self._safe_action(
            interaction,
            stage=f"admin_panel_{section}",
            failure_message="Babblebox could not refresh that admin panel section right now.",
            action=action,
        )

    async def _open_child_view(
        self,
        interaction: discord.Interaction,
        *,
        stage: str,
        failure_message: str,
        view_factory: Callable[[], AdminManagedView],
    ):
        async def action():
            view = view_factory()
            sent = await self.cog._send_private_interaction(interaction, embed=await view.current_embed(), view=view)
            if sent is not None:
                view.message = sent

        await self._safe_action(interaction, stage=stage, failure_message=failure_message, action=action)

    async def _open_permission_diagnostics(self, interaction: discord.Interaction):
        async def action():
            await self.cog._send_private_interaction(
                interaction,
                embed=await self.cog._permission_diagnostics_embed(self.guild_id),
            )

        await self._safe_action(
            interaction,
            stage="admin_panel_permissions",
            failure_message="Babblebox could not open permission diagnostics right now.",
            action=action,
        )

    async def _open_sync_review(self, interaction: discord.Interaction):
        await self._open_child_view(
            interaction,
            stage="admin_panel_sync",
            failure_message="Babblebox could not open the verification sync review right now.",
            view_factory=lambda: VerificationSyncView(self.cog, guild_id=self.guild_id, author_id=self.author_id),
        )

    async def _open_preview(self, interaction: discord.Interaction, *, final: bool):
        async def action():
            await self.cog._send_private_interaction(
                interaction,
                embed=self.cog.build_verification_preview_embed(interaction.guild, interaction.user, final=final),
            )

        await self._safe_action(
            interaction,
            stage="admin_panel_preview_kick" if final else "admin_panel_preview_warning",
            failure_message="Babblebox could not open that verification preview right now.",
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
            ("Follow-up", "followup", 0),
            ("Verification", "verification", 0),
            ("Exclusions", "exclusions", 1),
            ("Logs", "logs", 1),
            ("Templates", "templates", 1),
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

        add_button(label="Refresh", style=discord.ButtonStyle.secondary, row=2, callback=refresh_callback)

        if self.section == "overview":
            add_button(label="Open Sync Review", style=discord.ButtonStyle.secondary, row=3, callback=self._open_sync_review)
            add_button(label="Run Permission Check", style=discord.ButtonStyle.secondary, row=3, callback=self._open_permission_diagnostics)
        elif self.section == "followup":
            add_button(
                label="Edit Follow-up",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_followup_editor",
                    failure_message="Babblebox could not open the follow-up editor right now.",
                    view_factory=lambda: FollowupEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
        elif self.section == "verification":
            add_button(
                label="Edit Policy",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_verification_policy",
                    failure_message="Babblebox could not open the verification policy editor right now.",
                    view_factory=lambda: VerificationPolicyEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
            add_button(
                label="Edit Timing",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_verification_timing",
                    failure_message="Babblebox could not open the verification timing editor right now.",
                    view_factory=lambda: VerificationTimingEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
            add_button(
                label="Edit Help Path",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_verification_help",
                    failure_message="Babblebox could not open the verification help-path editor right now.",
                    view_factory=lambda: VerificationHelpEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
            add_button(label="Preview Warning", style=discord.ButtonStyle.secondary, row=4, callback=lambda interaction: self._open_preview(interaction, final=False))
            add_button(label="Preview Final Kick", style=discord.ButtonStyle.secondary, row=4, callback=lambda interaction: self._open_preview(interaction, final=True))
            add_button(label="Open Sync Review", style=discord.ButtonStyle.secondary, row=4, callback=self._open_sync_review)
        elif self.section == "exclusions":
            add_button(
                label="Edit Exclusions",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_exclusions_editor",
                    failure_message="Babblebox could not open the exclusions editor right now.",
                    view_factory=lambda: ExclusionsEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
        elif self.section == "logs":
            add_button(
                label="Edit Logs",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_logs_editor",
                    failure_message="Babblebox could not open the logs editor right now.",
                    view_factory=lambda: LogsEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )
            add_button(label="Run Permission Check", style=discord.ButtonStyle.secondary, row=3, callback=self._open_permission_diagnostics)
        elif self.section == "templates":
            add_button(
                label="Edit Templates",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=lambda interaction: self._open_child_view(
                    interaction,
                    stage="admin_panel_templates_editor",
                    failure_message="Babblebox could not open the templates editor right now.",
                    view_factory=lambda: TemplatesEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
                ),
            )


class FollowupEditorView(AdminPanelChildView):
    panel_title = "Follow-up Editor"
    stale_message = "That follow-up editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Follow-up Editor",
            description="Set the returned-after-ban follow-up lane directly from the panel.",
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(
            name="Current Policy",
            value=(
                f"Enabled: **{'Yes' if config['followup_enabled'] else 'No'}**\n"
                f"Role: {self.cog._role_mention(config['followup_role_id'])}\n"
                f"Mode: {FOLLOWUP_MODE_LABELS[config['followup_mode']]}\n"
                f"Duration: {_followup_duration_label(config['followup_duration_value'], config['followup_duration_unit'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="What This Controls",
            value=(
                "Babblebox can assign one role when someone returns within 30 days of a ban event.\n"
                "Auto-remove clears it on expiry. Review mode posts one moderator review item to the admin log lane."
            ),
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin followup enabled:true role:@Probation mode:review duration:30d`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Follow-up editor")

    async def _submit_custom_duration(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_followup_config(self.guild_id, duration_text=raw_value)

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        current_duration_label = _followup_duration_label(config["followup_duration_value"], config["followup_duration_unit"])
        current_duration_value = _followup_duration_input(config["followup_duration_value"], config["followup_duration_unit"])

        status_select = discord.ui.Select(
            placeholder="Follow-up enabled state + mode",
            row=0,
            options=[
                discord.SelectOption(
                    label=f"{'On' if enabled else 'Off'} - {FOLLOWUP_MODE_LABELS[mode]}",
                    value=f"{'on' if enabled else 'off'}:{mode}",
                    default=bool(config["followup_enabled"]) == enabled and config["followup_mode"] == mode,
                )
                for enabled in (True, False)
                for mode in FOLLOWUP_MODE_LABELS
            ],
        )

        async def status_callback(interaction: discord.Interaction):
            async def action():
                enabled_token, mode_token = status_select.values[0].split(":", 1)
                ok, message = await self.cog.service.set_followup_config(self.guild_id, enabled=enabled_token == "on", mode=mode_token)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_followup_status", failure_message="Babblebox could not update the follow-up policy right now.", action=action)

        status_select.callback = status_callback
        self.add_item(status_select)

        role_select = discord.ui.RoleSelect(placeholder="Follow-up role", min_values=1, max_values=1, row=1)

        async def role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_followup_config(self.guild_id, role_id=int(role_select.values[0].id))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_followup_role", failure_message="Babblebox could not update the follow-up role right now.", action=action)

        role_select.callback = role_callback
        self.add_item(role_select)

        clear_role = discord.ui.Button(label="Clear Role", style=discord.ButtonStyle.secondary, row=2)
        clear_role.disabled = config["followup_role_id"] is None

        async def clear_role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_followup_config(self.guild_id, role_id=None)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_followup_role_clear", failure_message="Babblebox could not clear the follow-up role right now.", action=action)

        clear_role.callback = clear_role_callback
        self.add_item(clear_role)

        duration_select = discord.ui.Select(
            placeholder="Follow-up duration presets",
            row=3,
            options=_preset_select_options(FOLLOWUP_DURATION_PRESETS, current_label=current_duration_label, current_value=current_duration_value),
        )

        async def duration_callback(interaction: discord.Interaction):
            selected = duration_select.values[0]
            if selected == "__current__":
                await self._rerender(interaction, note=f"Already using {current_duration_label}.", note_ok=True)
                return

            async def action():
                ok, message = await self.cog.service.set_followup_config(self.guild_id, duration_text=selected)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_followup_duration", failure_message="Babblebox could not update the follow-up duration right now.", action=action)

        duration_select.callback = duration_callback
        self.add_item(duration_select)

        custom_duration = discord.ui.Button(label="Custom Duration", style=discord.ButtonStyle.secondary, row=4)

        async def custom_duration_callback(interaction: discord.Interaction):
            await self._open_modal(
                interaction,
                AdminTextInputModal(
                    self,
                    title="Custom Follow-up Duration",
                    label="Duration",
                    placeholder="Examples: 30d, 6w, 3mo",
                    default_value=current_duration_value,
                    submit_handler=self._submit_custom_duration,
                    failure_message="Babblebox could not update the follow-up duration right now.",
                ),
                failure_message="Babblebox could not open the follow-up duration editor right now.",
            )

        custom_duration.callback = custom_duration_callback
        self.add_item(custom_duration)


class VerificationPolicyEditorView(AdminPanelChildView):
    panel_title = "Verification Policy"
    stale_message = "That verification policy editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        rule = self.cog._verification_rule_details(self.guild_id)
        embed = discord.Embed(
            title="Verification Policy Editor",
            description="Set who counts as unverified and what Babblebox should do at the deadline.",
            color=ge.EMBED_THEME["danger"],
        )
        embed.add_field(
            name="Current Policy",
            value=(
                f"Enabled: **{'Yes' if config['verification_enabled'] else 'No'}**\n"
                f"Role: {self.cog._role_mention(config['verification_role_id'])}\n"
                f"Logic: {VERIFICATION_LOGIC_LABELS[config['verification_logic']]}\n"
                f"Deadline action: **{rule['deadline_action_label']}**"
            ),
            inline=False,
        )
        embed.add_field(name="What Happens Next", value=f"{rule['preview_sentence']}\n{rule['exempt_sentence']}", inline=False)
        embed.add_field(
            name="Command Fallback",
            value="`/admin verification enabled:true role:@Verified logic:must_have_role deadline_action:review`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Verification policy editor")

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)

        toggle_button = discord.ui.Button(
            label="Disable Cleanup" if config["verification_enabled"] else "Enable Cleanup",
            style=discord.ButtonStyle.danger if config["verification_enabled"] else discord.ButtonStyle.success,
            row=0,
        )

        async def toggle_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, enabled=not bool(config["verification_enabled"]))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_toggle", failure_message="Babblebox could not update verification cleanup right now.", action=action)

        toggle_button.callback = toggle_callback
        self.add_item(toggle_button)

        role_select = discord.ui.RoleSelect(placeholder="Verification role", min_values=1, max_values=1, row=1)

        async def role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, role_id=int(role_select.values[0].id))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_role", failure_message="Babblebox could not update the verification role right now.", action=action)

        role_select.callback = role_callback
        self.add_item(role_select)

        clear_role = discord.ui.Button(label="Clear Role", style=discord.ButtonStyle.secondary, row=2)
        clear_role.disabled = config["verification_role_id"] is None

        async def clear_role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, role_id=None)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_role_clear", failure_message="Babblebox could not clear the verification role right now.", action=action)

        clear_role.callback = clear_role_callback
        self.add_item(clear_role)

        logic_select = discord.ui.Select(
            placeholder="Who counts as unverified",
            row=3,
            options=[
                discord.SelectOption(label="Unverified if member DOES NOT have this role", value="must_have_role", default=config["verification_logic"] == "must_have_role"),
                discord.SelectOption(label="Unverified if member DOES have this role", value="must_not_have_role", default=config["verification_logic"] == "must_not_have_role"),
            ],
        )

        async def logic_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, logic=logic_select.values[0])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_logic", failure_message="Babblebox could not update the verification rule right now.", action=action)

        logic_select.callback = logic_callback
        self.add_item(logic_select)

        action_select = discord.ui.Select(
            placeholder="Deadline action",
            row=4,
            options=[
                discord.SelectOption(label="Kick automatically", value="auto_kick", default=config["verification_deadline_action"] == "auto_kick"),
                discord.SelectOption(label="Moderator review", value="review", default=config["verification_deadline_action"] == "review"),
            ],
        )

        async def action_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, deadline_action=action_select.values[0])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_deadline_action", failure_message="Babblebox could not update the verification deadline action right now.", action=action)

        action_select.callback = action_callback
        self.add_item(action_select)


class VerificationTimingEditorView(AdminPanelChildView):
    panel_title = "Verification Timing"
    stale_message = "That verification timing editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Verification Timing Editor",
            description="Tune the warning lead and final deadline without leaving the panel.",
            color=ge.EMBED_THEME["danger"],
        )
        embed.add_field(
            name="Current Timing",
            value=(
                f"Kick after: **{format_duration_brief(config['verification_kick_after_seconds'])}**\n"
                f"Warning lead: **{format_duration_brief(config['verification_warning_lead_seconds'])}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="What This Controls",
            value="Babblebox warns after the lead window and then either kicks or queues moderator review at the final deadline.",
            inline=False,
        )
        embed.add_field(name="Command Fallback", value="`/admin verification kick_after:7d warning_lead:2d`", inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Verification timing editor")

    async def _submit_kick_after(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_verification_config(self.guild_id, kick_after_text=raw_value)

    async def _submit_warning_lead(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_verification_config(self.guild_id, warning_lead_text=raw_value)

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        current_kick_label = format_duration_brief(config["verification_kick_after_seconds"])
        current_warning_label = format_duration_brief(config["verification_warning_lead_seconds"])
        current_kick_value = _match_duration_preset_value(config["verification_kick_after_seconds"], VERIFICATION_KICK_AFTER_PRESETS)
        current_warning_value = _match_duration_preset_value(config["verification_warning_lead_seconds"], VERIFICATION_WARNING_LEAD_PRESETS)

        kick_select = discord.ui.Select(
            placeholder="Kick-after presets",
            row=0,
            options=_preset_select_options(VERIFICATION_KICK_AFTER_PRESETS, current_label=current_kick_label, current_value=current_kick_value),
        )

        async def kick_callback(interaction: discord.Interaction):
            selected = kick_select.values[0]
            if selected == "__current__":
                await self._rerender(interaction, note=f"Already using {current_kick_label}.", note_ok=True)
                return

            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, kick_after_text=selected)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_kick_after", failure_message="Babblebox could not update the verification deadline right now.", action=action)

        kick_select.callback = kick_callback
        self.add_item(kick_select)

        warning_select = discord.ui.Select(
            placeholder="Warning-lead presets",
            row=1,
            options=_preset_select_options(VERIFICATION_WARNING_LEAD_PRESETS, current_label=current_warning_label, current_value=current_warning_value),
        )

        async def warning_callback(interaction: discord.Interaction):
            selected = warning_select.values[0]
            if selected == "__current__":
                await self._rerender(interaction, note=f"Already using {current_warning_label}.", note_ok=True)
                return

            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, warning_lead_text=selected)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_warning_lead", failure_message="Babblebox could not update the warning lead right now.", action=action)

        warning_select.callback = warning_callback
        self.add_item(warning_select)

        custom_kick = discord.ui.Button(label="Custom Deadline", style=discord.ButtonStyle.secondary, row=2)

        async def custom_kick_callback(interaction: discord.Interaction):
            await self._open_modal(
                interaction,
                AdminTextInputModal(
                    self,
                    title="Custom Verification Deadline",
                    label="Kick-after timer",
                    placeholder="Examples: 7d, 14d, 24h",
                    default_value=_best_duration_input(config["verification_kick_after_seconds"]),
                    submit_handler=self._submit_kick_after,
                    failure_message="Babblebox could not update the verification deadline right now.",
                ),
                failure_message="Babblebox could not open the verification deadline editor right now.",
            )

        custom_kick.callback = custom_kick_callback
        self.add_item(custom_kick)

        custom_warning = discord.ui.Button(label="Custom Warning Lead", style=discord.ButtonStyle.secondary, row=2)

        async def custom_warning_callback(interaction: discord.Interaction):
            await self._open_modal(
                interaction,
                AdminTextInputModal(
                    self,
                    title="Custom Warning Lead",
                    label="Warning lead",
                    placeholder="Examples: 12h, 1d, 2d",
                    default_value=_best_duration_input(config["verification_warning_lead_seconds"]),
                    submit_handler=self._submit_warning_lead,
                    failure_message="Babblebox could not update the warning lead right now.",
                ),
                failure_message="Babblebox could not open the warning-lead editor right now.",
            )

        custom_warning.callback = custom_warning_callback
        self.add_item(custom_warning)


class VerificationHelpEditorView(AdminPanelChildView):
    panel_title = "Verification Help Path"
    stale_message = "That verification help-path editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Verification Help Path Editor",
            description="Choose where members can extend their deadline and how much extra time they get.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Current Path",
            value=(
                f"Help channel: {self.cog._channel_mention(config['verification_help_channel_id'])}\n"
                f"Extension: **{format_duration_brief(config['verification_help_extension_seconds'])}**\n"
                f"Max extensions: **{config['verification_max_extensions']}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="What This Controls",
            value="Any non-trivial message in the help channel can extend the deadline until the member reaches the configured cap.",
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin verification help_channel:#verify-help help_extension:2d max_extensions:1`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Verification help-path editor")

    async def _submit_help_extension(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_verification_config(self.guild_id, help_extension_text=raw_value)

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)
        current_extension_label = format_duration_brief(config["verification_help_extension_seconds"])
        current_extension_value = _match_duration_preset_value(config["verification_help_extension_seconds"], VERIFICATION_HELP_EXTENSION_PRESETS)

        channel_select = discord.ui.ChannelSelect(placeholder="Verification-help channel", min_values=1, max_values=1, row=0)

        async def channel_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, help_channel_id=int(channel_select.values[0].id))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_help_channel", failure_message="Babblebox could not update the verification-help channel right now.", action=action)

        channel_select.callback = channel_callback
        self.add_item(channel_select)

        clear_channel = discord.ui.Button(label="Clear Channel", style=discord.ButtonStyle.secondary, row=1)
        clear_channel.disabled = config["verification_help_channel_id"] is None

        async def clear_channel_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, help_channel_id=None)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_help_channel_clear", failure_message="Babblebox could not clear the verification-help channel right now.", action=action)

        clear_channel.callback = clear_channel_callback
        self.add_item(clear_channel)

        extension_select = discord.ui.Select(
            placeholder="Help-extension presets",
            row=2,
            options=_preset_select_options(VERIFICATION_HELP_EXTENSION_PRESETS, current_label=current_extension_label, current_value=current_extension_value),
        )

        async def extension_callback(interaction: discord.Interaction):
            selected = extension_select.values[0]
            if selected == "__current__":
                await self._rerender(interaction, note=f"Already using {current_extension_label}.", note_ok=True)
                return

            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, help_extension_text=selected)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_help_extension", failure_message="Babblebox could not update the verification help extension right now.", action=action)

        extension_select.callback = extension_callback
        self.add_item(extension_select)

        max_extensions_select = discord.ui.Select(
            placeholder="Max extensions per member",
            row=3,
            options=[
                discord.SelectOption(label="0 - no extensions" if value == 0 else f"{value} extension{'s' if value != 1 else ''}", value=str(value), default=value == config["verification_max_extensions"])
                for value in range(0, 6)
            ],
        )

        async def max_extensions_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_verification_config(self.guild_id, max_extensions=int(max_extensions_select.values[0]))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_verification_max_extensions", failure_message="Babblebox could not update the verification extension cap right now.", action=action)

        max_extensions_select.callback = max_extensions_callback
        self.add_item(max_extensions_select)

        custom_extension = discord.ui.Button(label="Custom Extension", style=discord.ButtonStyle.secondary, row=4)

        async def custom_extension_callback(interaction: discord.Interaction):
            await self._open_modal(
                interaction,
                AdminTextInputModal(
                    self,
                    title="Custom Help Extension",
                    label="Extension",
                    placeholder="Examples: 12h, 2d, 7d",
                    default_value=_best_duration_input(config["verification_help_extension_seconds"]),
                    submit_handler=self._submit_help_extension,
                    failure_message="Babblebox could not update the verification help extension right now.",
                ),
                failure_message="Babblebox could not open the help-extension editor right now.",
            )

        custom_extension.callback = custom_extension_callback
        self.add_item(custom_extension)


class LogsEditorView(AdminPanelChildView):
    panel_title = "Admin Logs"
    stale_message = "That logs editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Logs Editor",
            description="Route admin automation output to one calm moderator-facing lane.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Current Delivery",
            value=(
                f"Channel: {self.cog._channel_mention(config['admin_log_channel_id'])}\n"
                f"Alert role: {self.cog._role_mention(config['admin_alert_role_id'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="What This Controls",
            value="The shared admin lane carries follow-up alerts, verification warnings, review actions, help extensions, and operability failures.",
            inline=False,
        )
        embed.add_field(name="Command Fallback", value="`/admin logs channel:#admin-log role:@Mods`", inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Logs editor")

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)

        channel_select = discord.ui.ChannelSelect(placeholder="Admin log channel", min_values=1, max_values=1, row=0)

        async def channel_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_logs_config(self.guild_id, channel_id=int(channel_select.values[0].id), alert_role_id=config["admin_alert_role_id"])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_logs_channel", failure_message="Babblebox could not update the admin log channel right now.", action=action)

        channel_select.callback = channel_callback
        self.add_item(channel_select)

        role_select = discord.ui.RoleSelect(placeholder="Admin alert role", min_values=1, max_values=1, row=1)

        async def role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_logs_config(self.guild_id, channel_id=config["admin_log_channel_id"], alert_role_id=int(role_select.values[0].id))
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_logs_role", failure_message="Babblebox could not update the admin alert role right now.", action=action)

        role_select.callback = role_callback
        self.add_item(role_select)

        clear_channel = discord.ui.Button(label="Clear Channel", style=discord.ButtonStyle.secondary, row=2)
        clear_channel.disabled = config["admin_log_channel_id"] is None

        async def clear_channel_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_logs_config(self.guild_id, channel_id=None, alert_role_id=config["admin_alert_role_id"])
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_logs_channel_clear", failure_message="Babblebox could not clear the admin log channel right now.", action=action)

        clear_channel.callback = clear_channel_callback
        self.add_item(clear_channel)

        clear_role = discord.ui.Button(label="Clear Alert Role", style=discord.ButtonStyle.secondary, row=2)
        clear_role.disabled = config["admin_alert_role_id"] is None

        async def clear_role_callback(interaction: discord.Interaction):
            async def action():
                ok, message = await self.cog.service.set_logs_config(self.guild_id, channel_id=config["admin_log_channel_id"], alert_role_id=None)
                await self._rerender(interaction, note=message, note_ok=ok)

            await self._safe_action(interaction, stage="admin_logs_role_clear", failure_message="Babblebox could not clear the admin alert role right now.", action=action)

        clear_role.callback = clear_role_callback
        self.add_item(clear_role)


class ExclusionsEditorView(AdminPanelChildView):
    panel_title = "Exclusions Editor"
    stale_message = "That exclusions editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Exclusions Editor",
            description="Keep shared exclusions tight so follow-up and verification stay predictable.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Shared Buckets",
            value=(
                f"Excluded members: {self.cog._format_mentions(config['excluded_user_ids'], kind='user')}\n"
                f"Excluded roles: {self.cog._format_mentions(config['excluded_role_ids'], kind='role')}\n"
                f"Trusted roles: {self.cog._format_mentions(config['trusted_role_ids'], kind='role')}"
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
        embed.add_field(name="Command Fallback", value="`/admin exclusions target:trusted_role_ids state:on role:@Mods`", inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Exclusions editor")

    async def _replace_targets(self, field: str, values: list[int]) -> tuple[bool, str]:
        return await self.cog.service.replace_exclusion_targets(self.guild_id, field, values)

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)

        user_select = discord.ui.UserSelect(placeholder="Excluded members", min_values=0, max_values=20, row=0)
        user_select.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_exclusions_users",
            failure_message="Babblebox could not update the excluded-member list right now.",
            action=lambda: self._rerender_after_replace(interaction, "excluded_user_ids", [int(item.id) for item in user_select.values]),
        )
        self.add_item(user_select)

        excluded_role_select = discord.ui.RoleSelect(placeholder="Excluded roles", min_values=0, max_values=20, row=1)
        excluded_role_select.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_exclusions_roles",
            failure_message="Babblebox could not update the excluded-role list right now.",
            action=lambda: self._rerender_after_replace(interaction, "excluded_role_ids", [int(item.id) for item in excluded_role_select.values]),
        )
        self.add_item(excluded_role_select)

        trusted_role_select = discord.ui.RoleSelect(placeholder="Trusted roles", min_values=0, max_values=20, row=2)
        trusted_role_select.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_exclusions_trusted_roles",
            failure_message="Babblebox could not update the trusted-role list right now.",
            action=lambda: self._rerender_after_replace(interaction, "trusted_role_ids", [int(item.id) for item in trusted_role_select.values]),
        )
        self.add_item(trusted_role_select)

        for label, field in (("Clear Members", "excluded_user_ids"), ("Clear Roles", "excluded_role_ids"), ("Clear Trusted", "trusted_role_ids")):
            button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary, row=3)
            button.disabled = not bool(config[field])
            button.callback = lambda interaction, *, target_field=field: self._safe_action(
                interaction,
                stage=f"admin_exclusions_clear_{target_field}",
                failure_message="Babblebox could not clear that exclusions list right now.",
                action=lambda: self._rerender_after_replace(interaction, target_field, []),
            )
            self.add_item(button)

        for label, field in (("Follow-up Staff", "followup_exempt_staff"), ("Verification Staff", "verification_exempt_staff"), ("Verification Bots", "verification_exempt_bots")):
            enabled = bool(config[field])
            button = discord.ui.Button(label=f"{label}: {'On' if enabled else 'Off'}", style=discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary, row=4)
            button.callback = lambda interaction, *, target_field=field, current=enabled: self._safe_action(
                interaction,
                stage=f"admin_exclusion_toggle_{target_field}",
                failure_message="Babblebox could not update that exclusions toggle right now.",
                action=lambda: self._rerender_after_toggle(interaction, target_field, not current),
            )
            self.add_item(button)

    async def _rerender_after_replace(self, interaction: discord.Interaction, field: str, values: list[int]):
        ok, message = await self._replace_targets(field, values)
        await self._rerender(interaction, note=message, note_ok=ok)

    async def _rerender_after_toggle(self, interaction: discord.Interaction, field: str, enabled: bool):
        ok, message = await self.cog.service.set_exemption_toggle(self.guild_id, field, enabled)
        await self._rerender(interaction, note=message, note_ok=ok)


class TemplatesEditorView(AdminPanelChildView):
    panel_title = "Templates Editor"
    stale_message = "That templates editor expired. Open it again from `/admin panel`."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, panel_view: AdminPanelView | None = None):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, panel_view=panel_view)
        self._refresh_items()

    async def current_embed(self) -> discord.Embed:
        config = self.cog.service.get_config(self.guild_id)
        embed = discord.Embed(
            title="Templates Editor",
            description="Edit the verification DM copy and optional invite link without leaving the panel.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Current Settings",
            value=(
                f"Warning template: **{'Custom' if config['warning_template'] else 'Default'}**\n"
                f"Final kick template: **{'Custom' if config['kick_template'] else 'Default'}**\n"
                f"Invite link: {config['invite_link'] or 'None'}"
            ),
            inline=False,
        )
        embed.add_field(
            name="What This Controls",
            value="Babblebox supports `{guild}`, `{deadline}`, `{deadline_relative}`, `{help_channel}`, and `{invite_link}`. Preview renders against your own member profile.",
            inline=False,
        )
        embed.add_field(
            name="Command Fallback",
            value="`/admin templates warning_template:... kick_template:... invite_link:https://discord.gg/example`",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Templates editor")

    async def _submit_warning_template(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_templates(self.guild_id, warning_template=raw_value)

    async def _submit_kick_template(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_templates(self.guild_id, kick_template=raw_value)

    async def _submit_invite_link(self, raw_value: str) -> tuple[bool, str]:
        return await self.cog.service.set_templates(self.guild_id, invite_link=raw_value)

    def _refresh_items(self):
        self.clear_items()
        config = self.cog.service.get_config(self.guild_id)

        async def preview(interaction: discord.Interaction, *, final: bool):
            async def action():
                await self.cog._send_private_interaction(interaction, embed=self.cog.build_verification_preview_embed(interaction.guild, interaction.user, final=final))

            await self._safe_action(interaction, stage="admin_templates_preview_kick" if final else "admin_templates_preview_warning", failure_message="Babblebox could not open that verification preview right now.", action=action)

        edit_warning = discord.ui.Button(label="Edit Warning", style=discord.ButtonStyle.secondary, row=0)
        edit_warning.callback = lambda interaction: self._open_modal(
            interaction,
            AdminTextInputModal(
                self,
                title="Edit Warning Template",
                label="Warning template",
                placeholder="Use {deadline_relative}, {help_channel}, and {invite_link} where needed.",
                default_value=config["warning_template"] or "",
                multiline=True,
                submit_handler=self._submit_warning_template,
                failure_message="Babblebox could not update the warning template right now.",
            ),
            failure_message="Babblebox could not open the warning-template editor right now.",
        )
        self.add_item(edit_warning)

        clear_warning = discord.ui.Button(label="Clear Warning", style=discord.ButtonStyle.secondary, row=0)
        clear_warning.disabled = config["warning_template"] is None
        clear_warning.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_templates_clear_warning",
            failure_message="Babblebox could not clear the warning template right now.",
            action=lambda: self._rerender_after_templates(interaction, warning_template=None),
        )
        self.add_item(clear_warning)

        edit_kick = discord.ui.Button(label="Edit Final Kick", style=discord.ButtonStyle.secondary, row=1)
        edit_kick.callback = lambda interaction: self._open_modal(
            interaction,
            AdminTextInputModal(
                self,
                title="Edit Final Kick Template",
                label="Final kick template",
                placeholder="Use {deadline}, {help_channel}, and {invite_link} where needed.",
                default_value=config["kick_template"] or "",
                multiline=True,
                submit_handler=self._submit_kick_template,
                failure_message="Babblebox could not update the final kick template right now.",
            ),
            failure_message="Babblebox could not open the final-kick template editor right now.",
        )
        self.add_item(edit_kick)

        clear_kick = discord.ui.Button(label="Clear Final Kick", style=discord.ButtonStyle.secondary, row=1)
        clear_kick.disabled = config["kick_template"] is None
        clear_kick.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_templates_clear_kick",
            failure_message="Babblebox could not clear the final kick template right now.",
            action=lambda: self._rerender_after_templates(interaction, kick_template=None),
        )
        self.add_item(clear_kick)

        edit_invite = discord.ui.Button(label="Edit Invite Link", style=discord.ButtonStyle.secondary, row=2)
        edit_invite.callback = lambda interaction: self._open_modal(
            interaction,
            AdminTextInputModal(
                self,
                title="Edit Invite Link",
                label="Invite link",
                placeholder="https://discord.gg/example",
                default_value=config["invite_link"] or "",
                submit_handler=self._submit_invite_link,
                failure_message="Babblebox could not update the invite link right now.",
            ),
            failure_message="Babblebox could not open the invite-link editor right now.",
        )
        self.add_item(edit_invite)

        clear_invite = discord.ui.Button(label="Clear Invite", style=discord.ButtonStyle.secondary, row=2)
        clear_invite.disabled = config["invite_link"] is None
        clear_invite.callback = lambda interaction: self._safe_action(
            interaction,
            stage="admin_templates_clear_invite",
            failure_message="Babblebox could not clear the invite link right now.",
            action=lambda: self._rerender_after_templates(interaction, invite_link=None),
        )
        self.add_item(clear_invite)

        preview_warning = discord.ui.Button(label="Preview Warning", style=discord.ButtonStyle.secondary, row=3)
        preview_warning.callback = lambda interaction: preview(interaction, final=False)
        self.add_item(preview_warning)

        preview_kick = discord.ui.Button(label="Preview Final Kick", style=discord.ButtonStyle.secondary, row=3)
        preview_kick.callback = lambda interaction: preview(interaction, final=True)
        self.add_item(preview_kick)

    async def _rerender_after_templates(self, interaction: discord.Interaction, **kwargs):
        ok, message = await self.cog.service.set_templates(self.guild_id, **kwargs)
        await self._rerender(interaction, note=message, note_ok=ok)


class VerificationSyncView(AdminManagedView):
    panel_title = "Verification Sync"
    stale_message = "That verification sync panel expired. Run `/admin sync` again to open a fresh one."

    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, timeout=None)
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
            return ge.make_status_embed("Verification Sync Unavailable", "This server is no longer available to Babblebox, so the sync panel cannot load.", tone="warning", footer="Babblebox Admin | Verification cleanup")
        session = self._session()
        if session is not None:
            return self.cog.build_sync_session_embed(guild, session)
        preview = await self.cog.service.build_verification_sync_preview(guild)
        return self.cog.build_sync_preview_embed(guild, preview)

    async def _safe_edit(self, *, force: bool = False):
        if self.message is None:
            return
        now = asyncio.get_running_loop().time()
        if not force and now - self._last_edit_at < 1.5:
            return
        self._last_edit_at = now
        self._refresh_buttons()
        with contextlib.suppress(discord.HTTPException, discord.NotFound, AttributeError):
            await self.message.edit(embed=await self.current_embed(), view=self)

    async def _handle_progress(self, session: VerificationSyncSession, force: bool):
        await self._safe_edit(force=force)

    async def _run_session(self, guild: discord.Guild, session: VerificationSyncSession):
        summary = await self.cog.service.run_verification_sync_session(guild, session, progress_callback=self._handle_progress)
        self.static_embed = self.cog.build_sync_summary_embed(summary)
        self._disable_all()
        await self._safe_edit(force=True)

    @discord.ui.button(label="Start Sync", style=discord.ButtonStyle.success)
    async def start_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def action():
            guild = interaction.guild
            if guild is None:
                await self.cog._send_private_interaction(interaction, content="This verification sync only works inside a server.")
                return
            self.message = interaction.message or self.message
            preview = await self.cog.service.build_verification_sync_preview(guild)
            if preview.blocking_prechecks:
                await self.cog._edit_interaction_message(interaction, embed=self.cog.build_sync_preview_embed(guild, preview), view=self)
                return
            created, session = await self.cog.service.create_verification_sync_session(guild, actor_id=interaction.user.id, preview=preview)
            self._refresh_buttons()
            updated = await self.cog._edit_interaction_message(interaction, embed=self.cog.build_sync_session_embed(guild, session), view=self)
            if not updated:
                await self.cog._send_private_interaction(interaction, embed=self.cog._admin_status_embed(self.panel_title, self.stale_message, ok=False))
                return
            if created:
                asyncio.create_task(self._run_session(guild, session), name=f"babblebox-admin-sync-{guild.id}")

        await self._safe_action(interaction, stage="admin_sync_start", failure_message="Babblebox could not start the verification sync right now.", action=action)

    @discord.ui.button(label="Refresh", style=discord.ButtonStyle.secondary)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def action():
            self.message = interaction.message or self.message
            await self._rerender(interaction, note="Verification sync refreshed.", note_ok=True)

        await self._safe_action(interaction, stage="admin_sync_refresh", failure_message="Babblebox could not refresh the verification sync right now.", action=action)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def action():
            self.message = interaction.message or self.message
            session = self._session()
            if session is None:
                self.static_embed = ge.make_status_embed("Verification Sync Cancelled", "No verification sync was started, and no member state was changed.", tone="info", footer="Babblebox Admin | Verification cleanup")
                self._disable_all()
                await self.cog._edit_interaction_message(interaction, embed=self.static_embed, view=self)
                return
            await self.cog.service.request_verification_sync_stop(self.guild_id)
            self._refresh_buttons()
            await self.cog._edit_interaction_message(interaction, embed=self.cog.build_sync_session_embed(interaction.guild, session), view=self)

        await self._safe_action(interaction, stage="admin_sync_stop", failure_message="Babblebox could not update the verification sync stop request right now.", action=action)


__all__ = [
    "AdminPanelView",
    "VerificationSyncView",
]
