from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Awaitable, Callable

import discord

from babblebox import game_engine as ge
from babblebox.admin_service import (
    FOLLOWUP_MODE_LABELS,
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

    async def _open_followup_editor(self, interaction: discord.Interaction):
        await self._open_child_view(
            interaction,
            stage="admin_panel_followup_editor",
            failure_message="Babblebox could not open the follow-up editor right now.",
            view_factory=lambda: FollowupEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
        )

    async def _open_logs_editor(self, interaction: discord.Interaction):
        await self._open_child_view(
            interaction,
            stage="admin_panel_logs_editor",
            failure_message="Babblebox could not open the logs editor right now.",
            view_factory=lambda: LogsEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, panel_view=self),
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
            ("Exclusions", "exclusions", 1),
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

        add_button(label="Refresh", style=discord.ButtonStyle.secondary, row=2, callback=refresh_callback)

        if self.section == "overview":
            add_button(label="Edit Follow-up", style=discord.ButtonStyle.secondary, row=3, callback=self._open_followup_editor)
            add_button(label="Edit Logs", style=discord.ButtonStyle.secondary, row=3, callback=self._open_logs_editor)
            add_button(label="Run Permission Check", style=discord.ButtonStyle.secondary, row=4, callback=self._open_permission_diagnostics)
        elif self.section == "followup":
            add_button(
                label="Edit Follow-up",
                style=discord.ButtonStyle.secondary,
                row=3,
                callback=self._open_followup_editor,
            )
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
                callback=self._open_logs_editor,
            )
            add_button(label="Run Permission Check", style=discord.ButtonStyle.secondary, row=3, callback=self._open_permission_diagnostics)


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
            value="The shared admin lane carries follow-up alerts, lock notices, and operability failures.",
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
            description="Keep shared exclusions tight so follow-up stays predictable.",
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
                f"Follow-up exempts staff/trusted: **{'Yes' if config['followup_exempt_staff'] else 'No'}**"
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

        for label, field in (("Follow-up Staff", "followup_exempt_staff"),):
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


__all__ = [
    "AdminPanelView",
    ]

