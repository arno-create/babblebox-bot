from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import discord

from babblebox import game_engine as ge
from babblebox.admin_service import (
    PERMISSION_ORCHESTRATION_PREVIEW_LIMIT,
    PermissionOrchestrationPreview,
    PermissionOrchestrationResult,
    permission_future_rule_action_label,
)
from babblebox.permission_orchestration import (
    PERMISSION_SYNC_APPLY_BOTH,
    PERMISSION_SYNC_APPLY_EXISTING,
    PERMISSION_SYNC_APPLY_FUTURE,
    PERMISSION_SYNC_PRESETS,
    PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS,
    PERMISSION_SYNC_RULE_SCOPE_SELECTED_CATEGORIES,
    PERMISSION_SYNC_SCOPE_ALL_CHANNELS,
    PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN,
    PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES,
    PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS,
    VALID_PERMISSION_SYNC_CHANNEL_TYPES,
    VALID_PERMISSION_SYNC_FLAGS,
    default_permission_sync_channel_types,
    permission_apply_target_label,
    permission_channel_type_label,
    permission_flag_label,
    permission_scope_label,
    summarize_permission_map,
)

if TYPE_CHECKING:
    from babblebox.cogs.admin import AdminCog


EDITOR_FOOTER = "Babblebox Admin | /admin permissions"


@dataclass
class PermissionDraftState:
    role_id: int | None = None
    preset_key: str | None = None
    permission_map: dict[str, str] = field(default_factory=dict)
    scope_mode: str = PERMISSION_SYNC_SCOPE_ALL_CHANNELS
    apply_target: str = PERMISSION_SYNC_APPLY_EXISTING
    channel_ids: list[int] = field(default_factory=list)
    category_ids: list[int] = field(default_factory=list)
    future_channel_type_filters: list[str] = field(default_factory=default_permission_sync_channel_types)
    disable_future_rule: bool = False
    selected_permission_flags: list[str] = field(default_factory=list)
    preview: PermissionOrchestrationPreview | None = None

    def clear_preview(self):
        self.preview = None

    def clear_targets(self):
        self.channel_ids = []
        self.category_ids = []
        self.clear_preview()

    def clear_permission_changes(self):
        self.permission_map = {}
        self.preset_key = None
        self.selected_permission_flags = []
        self.clear_preview()

    def sync_scope_targets(self):
        if self.scope_mode == PERMISSION_SYNC_SCOPE_ALL_CHANNELS:
            self.channel_ids = []
            self.category_ids = []
        elif self.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS:
            self.category_ids = []
        else:
            self.channel_ids = []
        self.clear_preview()

    def service_kwargs(self) -> dict[str, object]:
        return {
            "role_id": int(self.role_id or 0),
            "permission_map": dict(self.permission_map),
            "scope_mode": self.scope_mode,
            "apply_target": self.apply_target,
            "channel_ids": list(self.channel_ids),
            "category_ids": list(self.category_ids),
            "future_channel_type_filters": list(self.future_channel_type_filters),
            "preset_key": self.preset_key,
            "disable_future_rule": self.disable_future_rule,
        }


def _sorted_unique(values: list[int]) -> list[int]:
    return sorted({int(value) for value in values if isinstance(value, int) and value > 0})


def _mention_list(guild: discord.Guild | None, ids: list[int], *, limit: int = 6) -> str:
    if not ids:
        return "None selected"
    rendered: list[str] = []
    for value in ids[:limit]:
        channel = guild.get_channel(value) if guild is not None else None
        rendered.append(getattr(channel, "mention", f"<#{value}>"))
    if len(ids) > limit:
        rendered.append(f"+{len(ids) - limit} more")
    return ", ".join(rendered)


def _future_scope_summary(rule: object | None) -> str:
    if rule is None:
        return "No saved future-channel rule."
    scope_mode = getattr(rule, "scope_mode", PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS)
    if scope_mode == PERMISSION_SYNC_RULE_SCOPE_SELECTED_CATEGORIES:
        category_ids = sorted(int(value) for value in getattr(rule, "category_ids", []))
        categories = ", ".join(f"<#{value}>" for value in category_ids[:4]) if category_ids else "selected categories"
        if len(category_ids) > 4:
            categories += f", +{len(category_ids) - 4} more"
        scope_label = f"New channels inside {categories}"
    else:
        scope_label = "All new supported channels"
    channel_types = sorted(str(value) for value in getattr(rule, "channel_type_filters", [])) or sorted(VALID_PERMISSION_SYNC_CHANNEL_TYPES)
    type_summary = ", ".join(permission_channel_type_label(value) for value in channel_types)
    preset_key = getattr(rule, "preset_key", None)
    preset_label = PERMISSION_SYNC_PRESETS[preset_key].name if preset_key in PERMISSION_SYNC_PRESETS else "Custom"
    return f"{scope_label}\nPreset: **{preset_label}**\nTypes: {type_summary}"


def _preview_action_label(action: str) -> str:
    mapping = {
        "change": "Will change",
        "unchanged": "Already matches",
        "inherit": "Will inherit",
        "skip": "Will stay untouched",
    }
    return mapping.get(action, action.replace("_", " ").title())


def _future_rule_status_text(action: str, summary: str) -> str:
    return f"Status: **{permission_future_rule_action_label(action)}**\n{summary}"


def _future_rule_reason_text(action: str) -> str:
    return {
        "create": "This draft will create a saved future-channel rule.",
        "replace": "This draft will replace the saved future-channel rule.",
        "disable": "This draft will disable the saved future-channel rule.",
        "unchanged": "This draft keeps the saved future-channel rule unchanged.",
        "none": "This draft does not change any saved future-channel rule.",
    }.get(action, "This draft touches saved future-channel automation.")


class PermissionViewBase(discord.ui.View):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, state: PermissionDraftState):
        super().__init__(timeout=300)
        self.cog = cog
        self.guild_id = guild_id
        self.author_id = author_id
        self.state = state
        self.message: discord.Message | None = None

    def _guild(self) -> discord.Guild | None:
        return self.cog._guild(self.guild_id)

    def _saved_rule(self):
        if not self.state.role_id:
            return None
        return self.cog.service.permission_sync_rule_for_role(self.guild_id, self.state.role_id)

    async def current_embed(self) -> discord.Embed:
        raise NotImplementedError

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if int(getattr(interaction.user, "id", 0) or 0) != self.author_id:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "This Panel Is Locked",
                    "Use `/admin permissions` to open your own permission orchestration panel.",
                    tone="info",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Server Only",
                    "These permission controls only work inside a server.",
                    tone="warning",
                    footer="Babblebox Admin",
                ),
                ephemeral=True,
            )
            return False
        allowed, reason = self.cog.service.can_manage_control_plane(interaction.user, self.guild_id, operation="manage")
        if not allowed:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Permission Orchestration Restricted",
                    reason,
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

    async def _swap(
        self,
        interaction: discord.Interaction,
        next_view: "PermissionViewBase",
        *,
        embed: discord.Embed | None = None,
        note: str | None = None,
    ):
        next_view.message = interaction.message or self.message
        resolved_embed = embed or await next_view.current_embed()
        if interaction.response.is_done():
            if interaction.message is not None:
                await interaction.message.edit(embed=resolved_embed, view=next_view)
            if note:
                await interaction.followup.send(note, ephemeral=True)
            return
        await interaction.response.edit_message(embed=resolved_embed, view=next_view)
        if note:
            await interaction.followup.send(note, ephemeral=True)

    async def _show_result(
        self,
        interaction: discord.Interaction,
        *,
        ok: bool,
        message: str,
        result: PermissionOrchestrationResult | None,
    ):
        self.state.clear_preview()
        self.state.disable_future_rule = False
        view = PermissionResultView(
            self.cog,
            guild_id=self.guild_id,
            author_id=self.author_id,
            state=self.state,
            ok=ok and result is not None,
            message=message,
            result=result,
        )
        await self._swap(interaction, view)

    async def _apply_preview(self, interaction: discord.Interaction, preview: PermissionOrchestrationPreview):
        guild = interaction.guild or self._guild()
        if guild is None:
            await interaction.response.send_message("This permission panel is no longer attached to a server.", ephemeral=True)
            return
        ok, message, result = await self.cog.service.apply_permission_orchestration(
            guild,
            actor=interaction.user,
            expected_signature=preview.signature,
            **self.state.service_kwargs(),
        )
        await self._show_result(interaction, ok=ok, message=message, result=result)

    async def _close(self, interaction: discord.Interaction, *, title: str, message: str):
        for child in self.children:
            child.disabled = True
        embed = ge.make_status_embed(title, message, tone="info", footer=EDITOR_FOOTER)
        await self._swap(interaction, self, embed=embed)


class PermissionOrchestrationView(PermissionViewBase):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, state: PermissionDraftState):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self._build_items()

    def _build_items(self):
        role_select = discord.ui.RoleSelect(
            placeholder="Choose the role Babblebox should orchestrate",
            min_values=1,
            max_values=1,
            row=0,
        )

        async def _role_callback(interaction: discord.Interaction):
            role = role_select.values[0]
            self.state.role_id = int(role.id)
            self.state.disable_future_rule = False
            self.state.clear_preview()
            saved_rule = self._saved_rule()
            note = (
                f"Target role set to {role.mention}. A saved future rule already exists for this role; open **Future Rule** to review, load, or disable it."
                if saved_rule is not None
                else f"Target role set to {role.mention}."
            )
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=note,
            )

        role_select.callback = _role_callback
        self.add_item(role_select)

        preset_options = [discord.SelectOption(label="Custom Draft", value="custom", default=self.state.preset_key is None)]
        for key, preset in PERMISSION_SYNC_PRESETS.items():
            preset_options.append(
                discord.SelectOption(
                    label=preset.name,
                    value=key,
                    description=preset.description[:100],
                    default=self.state.preset_key == key,
                )
            )
        preset_select = discord.ui.Select(
            placeholder="Choose a starter preset or stay custom",
            min_values=1,
            max_values=1,
            options=preset_options,
            row=1,
        )

        async def _preset_callback(interaction: discord.Interaction):
            value = preset_select.values[0]
            if value == "custom":
                self.state.preset_key = None
                self.state.clear_preview()
                note = "Preset selection cleared. Your current custom permission draft stays in place."
            else:
                preset = PERMISSION_SYNC_PRESETS[value]
                self.state.preset_key = value
                self.state.permission_map = dict(preset.permission_map)
                self.state.selected_permission_flags = sorted(preset.permission_map)
                self.state.disable_future_rule = False
                self.state.clear_preview()
                note = f"Loaded the **{preset.name}** starter preset. You can still edit the permission states before previewing."
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=note,
            )

        preset_select.callback = _preset_callback
        self.add_item(preset_select)

        scope_select = discord.ui.Select(
            placeholder="Choose how wide the existing-channel scope should be",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=permission_scope_label(PERMISSION_SYNC_SCOPE_ALL_CHANNELS),
                    value=PERMISSION_SYNC_SCOPE_ALL_CHANNELS,
                    description="Evaluate every category first, then direct child channels as needed.",
                    default=self.state.scope_mode == PERMISSION_SYNC_SCOPE_ALL_CHANNELS,
                ),
                discord.SelectOption(
                    label=permission_scope_label(PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS),
                    value=PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS,
                    description="Only the chosen current channels are edited directly.",
                    default=self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS,
                ),
                discord.SelectOption(
                    label=permission_scope_label(PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES),
                    value=PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES,
                    description="Edit the categories only; synced child channels inherit.",
                    default=self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES,
                ),
                discord.SelectOption(
                    label=permission_scope_label(PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN),
                    value=PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN,
                    description="Edit the child channels inside the chosen categories directly.",
                    default=self.state.scope_mode == PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN,
                ),
            ],
            row=2,
        )

        async def _scope_callback(interaction: discord.Interaction):
            self.state.scope_mode = scope_select.values[0]
            if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS and self.state.apply_target in {
                PERMISSION_SYNC_APPLY_FUTURE,
                PERMISSION_SYNC_APPLY_BOTH,
            }:
                self.state.apply_target = PERMISSION_SYNC_APPLY_EXISTING
                note = "Selected-channel scope cannot create future automation, so Babblebox switched this draft to **Existing channels only**."
            else:
                note = f"Scope updated to **{permission_scope_label(self.state.scope_mode)}**."
            self.state.sync_scope_targets()
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=note,
            )

        scope_select.callback = _scope_callback
        self.add_item(scope_select)

        target_select = discord.ui.Select(
            placeholder="Choose whether this changes current channels, future automation, or both",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=permission_apply_target_label(PERMISSION_SYNC_APPLY_EXISTING),
                    value=PERMISSION_SYNC_APPLY_EXISTING,
                    description="Only current channels are touched.",
                    default=self.state.apply_target == PERMISSION_SYNC_APPLY_EXISTING,
                ),
                discord.SelectOption(
                    label=permission_apply_target_label(PERMISSION_SYNC_APPLY_FUTURE),
                    value=PERMISSION_SYNC_APPLY_FUTURE,
                    description="Only new channels and saved automation are touched.",
                    default=self.state.apply_target == PERMISSION_SYNC_APPLY_FUTURE,
                ),
                discord.SelectOption(
                    label=permission_apply_target_label(PERMISSION_SYNC_APPLY_BOTH),
                    value=PERMISSION_SYNC_APPLY_BOTH,
                    description="Update current channels now and save the matching future rule.",
                    default=self.state.apply_target == PERMISSION_SYNC_APPLY_BOTH,
                ),
            ],
            row=3,
        )

        async def _target_callback(interaction: discord.Interaction):
            self.state.apply_target = target_select.values[0]
            self.state.clear_preview()
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=f"Apply target updated to **{permission_apply_target_label(self.state.apply_target)}**.",
            )

        target_select.callback = _target_callback
        self.add_item(target_select)

        targets_button = discord.ui.Button(label="Targets", style=discord.ButtonStyle.secondary, row=4)
        permissions_button = discord.ui.Button(label="Permissions", style=discord.ButtonStyle.secondary, row=4)
        future_button = discord.ui.Button(label="Future Rule", style=discord.ButtonStyle.secondary, row=4)
        preview_button = discord.ui.Button(label="Preview", style=discord.ButtonStyle.primary, row=4)
        close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.secondary, row=4)

        async def _targets_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionTargetEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        async def _permissions_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionFlagEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        async def _future_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionFutureRuleView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        async def _preview_callback(interaction: discord.Interaction):
            guild = interaction.guild or self._guild()
            if guild is None:
                await interaction.response.send_message("This permission panel is no longer attached to a server.", ephemeral=True)
                return
            preview = await self.cog.service.build_permission_orchestration_preview(
                guild,
                actor=interaction.user,
                **self.state.service_kwargs(),
            )
            self.state.preview = preview
            await self._swap(
                interaction,
                PermissionPreviewView(
                    self.cog,
                    guild_id=self.guild_id,
                    author_id=self.author_id,
                    state=self.state,
                    preview=preview,
                ),
            )

        async def _close_callback(interaction: discord.Interaction):
            await self._close(
                interaction,
                title="Permission Orchestrator Closed",
                message="No channel overwrites or saved future rules were changed.",
            )

        targets_button.callback = _targets_callback
        permissions_button.callback = _permissions_callback
        future_button.callback = _future_callback
        preview_button.callback = _preview_callback
        close_button.callback = _close_callback
        for item in (targets_button, permissions_button, future_button, preview_button, close_button):
            self.add_item(item)

    async def current_embed(self) -> discord.Embed:
        guild = self._guild()
        role = guild.get_role(self.state.role_id) if guild is not None and self.state.role_id else None
        role_label = getattr(role, "mention", "Choose a role")
        preset_label = PERMISSION_SYNC_PRESETS[self.state.preset_key].name if self.state.preset_key in PERMISSION_SYNC_PRESETS else "Custom"
        type_summary = ", ".join(permission_channel_type_label(value) for value in self.state.future_channel_type_filters)
        embed = discord.Embed(
            title="Role Permission Orchestrator",
            description="Draft one role-scoped overwrite change, preview the exact channel impact, and optionally save the same rule for future channels.",
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Draft",
            value=(
                f"Role: {role_label}\n"
                f"Preset: **{preset_label}**\n"
                f"Scope: **{permission_scope_label(self.state.scope_mode)}**\n"
                f"Apply target: **{permission_apply_target_label(self.state.apply_target)}**\n"
                f"Future channel types: {type_summary}\n"
                f"Disable saved future rule on apply: **{'Yes' if self.state.disable_future_rule else 'No'}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Permission Changes",
            value=ge.join_limited_lines(summarize_permission_map(self.state.permission_map), limit=1024, empty="No permission changes drafted yet."),
            inline=False,
        )
        if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS:
            scope_targets = _mention_list(guild, self.state.channel_ids)
        elif self.state.scope_mode == PERMISSION_SYNC_SCOPE_ALL_CHANNELS:
            scope_targets = "No explicit selection needed. Babblebox evaluates all categories and channels."
        else:
            scope_targets = _mention_list(guild, self.state.category_ids)
        embed.add_field(name="Scope Targets", value=scope_targets, inline=False)
        embed.add_field(name="Saved Future Rule", value=_future_scope_summary(self._saved_rule()), inline=False)
        safety_lines = [
            "Babblebox blocks @everyone and managed roles here.",
            "You can only configure roles strictly below your own highest role.",
            "Only the selected permission flags are edited; unrelated overwrite bits stay untouched.",
            "Preview first. Nothing changes until you confirm the dry run.",
        ]
        if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS and self.state.apply_target in {
            PERMISSION_SYNC_APPLY_FUTURE,
            PERMISSION_SYNC_APPLY_BOTH,
        }:
            safety_lines.append("Selected-channel scope cannot be used for future automation. Switch to all channels or categories before previewing.")
        embed.add_field(name="Safety", value="\n".join(safety_lines), inline=False)
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionTargetEditorView(PermissionViewBase):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, state: PermissionDraftState):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self._build_items()

    def _build_items(self):
        if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS:
            select = discord.ui.ChannelSelect(
                placeholder="Add channels to the current target list",
                channel_types=[
                    discord.ChannelType.text,
                    discord.ChannelType.news,
                    discord.ChannelType.voice,
                    discord.ChannelType.stage_voice,
                    discord.ChannelType.forum,
                ],
                min_values=1,
                max_values=25,
                row=0,
            )

            async def _select_callback(interaction: discord.Interaction):
                new_ids = [int(channel.id) for channel in select.values]
                self.state.channel_ids = _sorted_unique(self.state.channel_ids + new_ids)
                self.state.clear_preview()
                await self._swap(
                    interaction,
                    PermissionTargetEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                    note=f"Added {len(new_ids)} channel{'s' if len(new_ids) != 1 else ''}. Total selected: **{len(self.state.channel_ids)}**.",
                )

            select.callback = _select_callback
            self.add_item(select)
        elif self.state.scope_mode in {PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES, PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN}:
            select = discord.ui.ChannelSelect(
                placeholder="Add categories to the current target list",
                channel_types=[discord.ChannelType.category],
                min_values=1,
                max_values=25,
                row=0,
            )

            async def _category_callback(interaction: discord.Interaction):
                new_ids = [int(channel.id) for channel in select.values]
                self.state.category_ids = _sorted_unique(self.state.category_ids + new_ids)
                self.state.clear_preview()
                await self._swap(
                    interaction,
                    PermissionTargetEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                    note=f"Added {len(new_ids)} categor{'y' if len(new_ids) == 1 else 'ies'}. Total selected: **{len(self.state.category_ids)}**.",
                )

            select.callback = _category_callback
            self.add_item(select)

        clear_button = discord.ui.Button(label="Clear Targets", style=discord.ButtonStyle.secondary, row=4)
        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, row=4)

        async def _clear_callback(interaction: discord.Interaction):
            self.state.clear_targets()
            await self._swap(
                interaction,
                PermissionTargetEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note="Target selections cleared.",
            )

        async def _back_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        clear_button.callback = _clear_callback
        back_button.callback = _back_callback
        self.add_item(clear_button)
        self.add_item(back_button)

    async def current_embed(self) -> discord.Embed:
        guild = self._guild()
        if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS:
            title = "Channel Targets"
            description = "Only the selected current channels are edited directly."
            current_targets = _mention_list(guild, self.state.channel_ids, limit=10)
        elif self.state.scope_mode in {PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES, PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN}:
            title = "Category Targets"
            description = "Choose the categories Babblebox should use for category-only edits or direct child-channel edits."
            current_targets = _mention_list(guild, self.state.category_ids, limit=10)
        else:
            title = "All Channels Scope"
            description = "This scope does not need manual target selection."
            current_targets = "Every current category and supported channel will be evaluated."
        embed = discord.Embed(title=title, description=description, color=ge.EMBED_THEME["info"])
        embed.add_field(name="Current Targets", value=current_targets, inline=False)
        if self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES:
            behavior = "Babblebox edits the chosen categories directly. Synced child channels inherit those changes; unsynced children stay untouched."
        elif self.state.scope_mode == PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN:
            behavior = "Babblebox edits the child channels inside the chosen categories directly and leaves the category overwrites alone."
        elif self.state.scope_mode == PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS:
            behavior = "Future automation cannot use this scope. It is only for current channels."
        else:
            behavior = "Babblebox evaluates categories first and preserves synced child inheritance where possible."
        embed.add_field(name="Behavior", value=behavior, inline=False)
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionFlagEditorView(PermissionViewBase):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, state: PermissionDraftState):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self._build_items()

    def _build_items(self):
        options = []
        selected = set(self.state.selected_permission_flags)
        for flag in sorted(VALID_PERMISSION_SYNC_FLAGS):
            current_state = self.state.permission_map.get(flag)
            description = f"Current draft: {current_state.title()}" if current_state else "Current draft: Unchanged"
            options.append(
                discord.SelectOption(
                    label=permission_flag_label(flag),
                    value=flag,
                    description=description[:100],
                    default=flag in selected,
                )
            )
        select = discord.ui.Select(
            placeholder="Choose one or more permission flags to edit",
            min_values=1,
            max_values=len(options),
            options=options,
            row=0,
        )

        async def _select_callback(interaction: discord.Interaction):
            self.state.selected_permission_flags = sorted(select.values)
            await self._swap(
                interaction,
                PermissionFlagEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=f"Selected {len(self.state.selected_permission_flags)} permission flag{'s' if len(self.state.selected_permission_flags) != 1 else ''}.",
            )

        select.callback = _select_callback
        self.add_item(select)

        for label, state_value, style in (
            ("Allow Selected", "allow", discord.ButtonStyle.success),
            ("Deny Selected", "deny", discord.ButtonStyle.danger),
            ("Clear Selected", "clear", discord.ButtonStyle.secondary),
        ):
            button = discord.ui.Button(label=label, style=style, row=1)

            async def _apply_state(interaction: discord.Interaction, *, resolved_state: str = state_value):
                if not self.state.selected_permission_flags:
                    await interaction.response.send_message("Choose one or more permission flags first.", ephemeral=True)
                    return
                for flag in self.state.selected_permission_flags:
                    self.state.permission_map[flag] = resolved_state
                self.state.clear_preview()
                await self._swap(
                    interaction,
                    PermissionFlagEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                    note=(
                        f"Updated {len(self.state.selected_permission_flags)} flag{'s' if len(self.state.selected_permission_flags) != 1 else ''} "
                        f"to **{resolved_state.title()}** in the draft."
                    ),
                )

            button.callback = _apply_state
            self.add_item(button)

        clear_button = discord.ui.Button(label="Clear Draft", style=discord.ButtonStyle.secondary, row=2)
        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, row=2)

        async def _clear_callback(interaction: discord.Interaction):
            self.state.clear_permission_changes()
            await self._swap(
                interaction,
                PermissionFlagEditorView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note="All drafted permission changes were cleared.",
            )

        async def _back_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        clear_button.callback = _clear_callback
        back_button.callback = _back_callback
        self.add_item(clear_button)
        self.add_item(back_button)

    async def current_embed(self) -> discord.Embed:
        preset_name = PERMISSION_SYNC_PRESETS[self.state.preset_key].name if self.state.preset_key in PERMISSION_SYNC_PRESETS else "Custom"
        selected = ", ".join(permission_flag_label(flag) for flag in self.state.selected_permission_flags) or "Nothing selected yet"
        embed = discord.Embed(
            title="Permission Flag Editor",
            description="Choose the flags you want to edit, then stamp them as Allow, Deny, or Clear without touching unrelated overwrite bits.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Current Draft Preset", value=f"**{preset_name}**", inline=False)
        embed.add_field(name="Selected Flags", value=selected, inline=False)
        embed.add_field(
            name="Draft Permission Map",
            value=ge.join_limited_lines(summarize_permission_map(self.state.permission_map), limit=1024, empty="No permission changes drafted yet."),
            inline=False,
        )
        embed.add_field(
            name="Behavior",
            value="`Clear` makes a flag neutral for this role on the targeted channels. It does not remove unrelated allow or deny bits for other flags.",
            inline=False,
        )
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionFutureRuleView(PermissionViewBase):
    def __init__(self, cog: "AdminCog", *, guild_id: int, author_id: int, state: PermissionDraftState):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self._build_items()

    def _build_items(self):
        options = [
            discord.SelectOption(
                label=permission_channel_type_label(channel_type),
                value=channel_type,
                default=channel_type in self.state.future_channel_type_filters,
            )
            for channel_type in sorted(VALID_PERMISSION_SYNC_CHANNEL_TYPES)
        ]
        select = discord.ui.Select(
            placeholder="Choose which new channel types Babblebox should automate",
            min_values=1,
            max_values=len(options),
            options=options,
            row=0,
        )

        async def _type_callback(interaction: discord.Interaction):
            self.state.future_channel_type_filters = sorted(select.values)
            self.state.clear_preview()
            await self._swap(
                interaction,
                PermissionFutureRuleView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note="Future-channel type filters updated.",
            )

        select.callback = _type_callback
        self.add_item(select)

        load_button = discord.ui.Button(label="Load Saved Rule", style=discord.ButtonStyle.secondary, row=1)
        disable_label = "Undo Disable Draft" if self.state.disable_future_rule else "Disable Saved Rule"
        disable_style = discord.ButtonStyle.secondary if self.state.disable_future_rule else discord.ButtonStyle.danger
        disable_button = discord.ui.Button(label=disable_label, style=disable_style, row=1)
        reset_button = discord.ui.Button(label="Reset Types", style=discord.ButtonStyle.secondary, row=1)
        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, row=1)

        async def _load_callback(interaction: discord.Interaction):
            rule = self._saved_rule()
            if rule is None:
                await interaction.response.send_message("Choose a role with an existing saved future rule first.", ephemeral=True)
                return
            self.state.preset_key = getattr(rule, "preset_key", None)
            self.state.permission_map = dict(rule.permission_map_dict())
            self.state.scope_mode = (
                PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES
                if getattr(rule, "scope_mode", PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS) == PERMISSION_SYNC_RULE_SCOPE_SELECTED_CATEGORIES
                else PERMISSION_SYNC_SCOPE_ALL_CHANNELS
            )
            self.state.channel_ids = []
            self.state.category_ids = sorted(int(value) for value in getattr(rule, "category_ids", []))
            self.state.future_channel_type_filters = sorted(str(value) for value in getattr(rule, "channel_type_filters", [])) or default_permission_sync_channel_types()
            self.state.apply_target = PERMISSION_SYNC_APPLY_FUTURE
            self.state.disable_future_rule = False
            self.state.selected_permission_flags = sorted(self.state.permission_map)
            self.state.clear_preview()
            await self._swap(
                interaction,
                PermissionFutureRuleView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note="Loaded the saved future rule into the draft. Review the preview carefully before applying it.",
            )

        async def _disable_callback(interaction: discord.Interaction):
            rule = self._saved_rule()
            if rule is None:
                await interaction.response.send_message("No saved future rule exists for the current role.", ephemeral=True)
                return
            self.state.disable_future_rule = not self.state.disable_future_rule
            if self.state.disable_future_rule and self.state.apply_target == PERMISSION_SYNC_APPLY_EXISTING:
                self.state.apply_target = PERMISSION_SYNC_APPLY_FUTURE
            self.state.clear_preview()
            await self._swap(
                interaction,
                PermissionFutureRuleView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note=(
                    "This draft will disable the saved future rule."
                    if self.state.disable_future_rule
                    else "Future-rule disable mode removed from the draft."
                ),
            )

        async def _reset_callback(interaction: discord.Interaction):
            self.state.future_channel_type_filters = default_permission_sync_channel_types()
            self.state.clear_preview()
            await self._swap(
                interaction,
                PermissionFutureRuleView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
                note="Future-channel type filters reset to all supported channel types.",
            )

        async def _back_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        load_button.callback = _load_callback
        disable_button.callback = _disable_callback
        reset_button.callback = _reset_callback
        back_button.callback = _back_callback
        for item in (load_button, disable_button, reset_button, back_button):
            self.add_item(item)

    async def current_embed(self) -> discord.Embed:
        guild = self._guild()
        role = guild.get_role(self.state.role_id) if guild is not None and self.state.role_id else None
        role_label = getattr(role, "mention", "Choose a role first")
        types_summary = ", ".join(permission_channel_type_label(value) for value in self.state.future_channel_type_filters)
        embed = discord.Embed(
            title="Future-Channel Automation",
            description="Review or tune the deterministic rule Babblebox can apply when matching new channels are created.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Current Draft",
            value=(
                f"Role: {role_label}\n"
                f"Apply target: **{permission_apply_target_label(self.state.apply_target)}**\n"
                f"Disable saved rule on apply: **{'Yes' if self.state.disable_future_rule else 'No'}**"
            ),
            inline=False,
        )
        embed.add_field(name="Draft Channel Types", value=types_summary, inline=False)
        embed.add_field(name="Saved Rule", value=_future_scope_summary(self._saved_rule()), inline=False)
        embed.add_field(
            name="Behavior",
            value=(
                "Babblebox keeps one saved future rule per role in this release. Saving a new rule replaces the previous one for the same role.\n"
                "New-channel automation is deterministic and only edits the targeted permission flags."
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionPreviewView(PermissionViewBase):
    def __init__(
        self,
        cog: "AdminCog",
        *,
        guild_id: int,
        author_id: int,
        state: PermissionDraftState,
        preview: PermissionOrchestrationPreview,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self.preview = preview
        self._build_items()

    def _build_items(self):
        if not self.preview.blocked_reasons:
            confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.success, row=4)

            async def _confirm_callback(interaction: discord.Interaction):
                if self.preview.requires_heightened_confirmation:
                    await self._swap(
                        interaction,
                        PermissionConfirmView(
                            self.cog,
                            guild_id=self.guild_id,
                            author_id=self.author_id,
                            state=self.state,
                            preview=self.preview,
                        ),
                    )
                    return
                await self._apply_preview(interaction, self.preview)

            confirm_button.callback = _confirm_callback
            self.add_item(confirm_button)

        revise_button = discord.ui.Button(label="Revise", style=discord.ButtonStyle.primary, row=4)
        close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.secondary, row=4)

        async def _revise_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        async def _close_callback(interaction: discord.Interaction):
            await self._close(
                interaction,
                title="Permission Preview Closed",
                message="No channel overwrites or saved future rules were changed.",
            )

        revise_button.callback = _revise_callback
        close_button.callback = _close_callback
        self.add_item(revise_button)
        self.add_item(close_button)

    async def current_embed(self) -> discord.Embed:
        guild = self._guild()
        preview = self.preview
        description = (
            "Dry run only. Nothing has changed yet."
            if not preview.blocked_reasons
            else "Babblebox blocked this draft. Fix the issues below, then preview again."
        )
        embed = discord.Embed(
            title="Permission Orchestration Preview",
            description=description,
            color=ge.EMBED_THEME["warning"] if preview.blocked_reasons else ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Draft Summary",
            value=(
                f"Role: {preview.role_mention}\n"
                f"Preset: **{preview.preset_name or 'Custom'}{' (edited)' if preview.preset_edited else ''}**\n"
                f"Scope: **{permission_scope_label(preview.request.scope_mode)}**\n"
                f"Apply target: **{permission_apply_target_label(preview.request.apply_target)}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Existing Impact",
            value=(
                f"Will change: **{preview.changed_count}**\n"
                f"Already matched: **{preview.unchanged_count}**\n"
                f"Will inherit category result: **{preview.inherited_count}**\n"
                f"Will stay untouched: **{preview.skipped_count}**\n"
                f"{preview.existing_scope_summary}"
            ),
            inline=False,
        )
        sample_lines: list[str] = []
        for row in preview.results[:PERMISSION_ORCHESTRATION_PREVIEW_LIMIT]:
            channel = guild.get_channel(row.channel_id) if guild is not None else None
            label = getattr(channel, "mention", f"<#{row.channel_id}>")
            prefix = "Category" if row.target_kind == "category" else "Channel"
            line = f"{prefix} {label}: **{_preview_action_label(row.action)}**"
            if row.reason:
                line += f" - {row.reason}"
            sample_lines.append(line)
        embed.add_field(
            name="Sample Existing Channels",
            value=ge.join_limited_lines(sample_lines, limit=1024, empty="No existing channel changes are part of this draft."),
            inline=False,
        )
        embed.add_field(
            name="Future Automation",
            value=_future_rule_status_text(preview.future_rule_action, preview.future_rule_summary),
            inline=False,
        )
        permission_lines = summarize_permission_map(preview.request.permission_map_dict())
        if permission_lines:
            embed.add_field(
                name="Permission Changes",
                value=ge.join_limited_lines(permission_lines, limit=1024, empty="No permission changes drafted."),
                inline=False,
            )
        if preview.warnings:
            embed.add_field(
                name="Warnings",
                value=ge.join_limited_lines(list(preview.warnings), limit=1024, empty="None"),
                inline=False,
            )
        if preview.blocked_reasons:
            embed.add_field(
                name="Blocked",
                value=ge.join_limited_lines(list(preview.blocked_reasons), limit=1024, empty="None"),
                inline=False,
            )
        else:
            confirm_message = (
                "This draft is broad enough that Babblebox requires one more explicit confirmation."
                if preview.requires_heightened_confirmation
                else "If this looks right, press **Confirm** to apply it."
            )
            embed.add_field(name="Confirmation", value=confirm_message, inline=False)
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionConfirmView(PermissionViewBase):
    def __init__(
        self,
        cog: "AdminCog",
        *,
        guild_id: int,
        author_id: int,
        state: PermissionDraftState,
        preview: PermissionOrchestrationPreview,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self.preview = preview
        self._build_items()

    def _build_items(self):
        apply_button = discord.ui.Button(label="Apply Now", style=discord.ButtonStyle.danger, row=4)
        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, row=4)
        close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.secondary, row=4)

        async def _apply_callback(interaction: discord.Interaction):
            await self._apply_preview(interaction, self.preview)

        async def _back_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionPreviewView(
                    self.cog,
                    guild_id=self.guild_id,
                    author_id=self.author_id,
                    state=self.state,
                    preview=self.preview,
                ),
            )

        async def _close_callback(interaction: discord.Interaction):
            await self._close(
                interaction,
                title="Permission Confirmation Closed",
                message="No channel overwrites or saved future rules were changed.",
            )

        apply_button.callback = _apply_callback
        back_button.callback = _back_callback
        close_button.callback = _close_callback
        self.add_item(apply_button)
        self.add_item(back_button)
        self.add_item(close_button)

    async def current_embed(self) -> discord.Embed:
        reasons: list[str] = []
        if self.preview.request.scope_mode == PERMISSION_SYNC_SCOPE_ALL_CHANNELS:
            reasons.append("This draft evaluates all current channels.")
        if self.preview.request.apply_target in {PERMISSION_SYNC_APPLY_FUTURE, PERMISSION_SYNC_APPLY_BOTH}:
            reasons.append(_future_rule_reason_text(self.preview.future_rule_action))
        if self.preview.existing_direct_targets > 25 or self.preview.changed_count > 25:
            reasons.append("This draft touches a large current-channel set.")
        embed = discord.Embed(
            title="Confirm Broad Permission Change",
            description="Babblebox requires one extra explicit confirmation for wide-scope or future-channel permission drafts.",
            color=ge.EMBED_THEME["danger"],
        )
        embed.add_field(
            name="Why This Needs Extra Confirmation",
            value=ge.join_limited_lines(reasons, limit=1024, empty="This draft is broad."),
            inline=False,
        )
        embed.add_field(
            name="Draft Summary",
            value=(
                f"Role: {self.preview.role_mention}\n"
                f"Scope: **{permission_scope_label(self.preview.request.scope_mode)}**\n"
                f"Apply target: **{permission_apply_target_label(self.preview.request.apply_target)}**\n"
                f"Existing channels to change now: **{self.preview.changed_count}**\n"
                f"Future automation: **{permission_future_rule_action_label(self.preview.future_rule_action)}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Future Automation",
            value=self.preview.future_rule_summary,
            inline=False,
        )
        embed.add_field(
            name="Safety",
            value=(
                "Unrelated overwrite flags stay preserved.\n"
                "Babblebox still revalidates hierarchy and stale-preview safety before it applies anything."
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer=EDITOR_FOOTER)


class PermissionResultView(PermissionViewBase):
    def __init__(
        self,
        cog: "AdminCog",
        *,
        guild_id: int,
        author_id: int,
        state: PermissionDraftState,
        ok: bool,
        message: str,
        result: PermissionOrchestrationResult | None,
    ):
        super().__init__(cog, guild_id=guild_id, author_id=author_id, state=state)
        self.ok = ok
        self.result_message = message
        self.result = result
        self._build_items()

    def _build_items(self):
        edit_button = discord.ui.Button(label="Edit Again", style=discord.ButtonStyle.primary, row=4)
        close_button = discord.ui.Button(label="Close", style=discord.ButtonStyle.secondary, row=4)

        async def _edit_callback(interaction: discord.Interaction):
            await self._swap(
                interaction,
                PermissionOrchestrationView(self.cog, guild_id=self.guild_id, author_id=self.author_id, state=self.state),
            )

        async def _close_callback(interaction: discord.Interaction):
            await self._close(
                interaction,
                title="Permission Result Closed",
                message="The orchestration result stays logged in the admin log if delivery is configured.",
            )

        edit_button.callback = _edit_callback
        close_button.callback = _close_callback
        self.add_item(edit_button)
        self.add_item(close_button)

    async def current_embed(self) -> discord.Embed:
        if not self.ok or self.result is None:
            embed = ge.make_status_embed(
                "Permission Orchestration Not Applied",
                self.result_message,
                tone="warning",
                footer=EDITOR_FOOTER,
            )
            embed.add_field(
                name="Safety",
                value="No new overwrite batch was applied from this failed confirmation. Reopen the draft, refresh the preview, and try again.",
                inline=False,
            )
            return embed

        result = self.result
        preview = result.preview
        tone = "success" if result.failed_count == 0 else "warning"
        title = "Permission Orchestration Applied" if result.failed_count == 0 else "Permission Orchestration Applied With Issues"
        embed = ge.make_status_embed(title, self.result_message, tone=tone, footer=EDITOR_FOOTER)
        embed.add_field(
            name="Result",
            value=(
                f"Changed: **{result.changed_count}**\n"
                f"Already matched: **{result.unchanged_count}**\n"
                f"Inherited: **{result.inherited_count}**\n"
                f"Skipped: **{result.skipped_count}**\n"
                f"Failed: **{result.failed_count}**"
            ),
            inline=False,
        )
        permission_lines = summarize_permission_map(preview.request.permission_map_dict())
        if permission_lines:
            embed.add_field(
                name="Permission Changes",
                value=ge.join_limited_lines(permission_lines, limit=1024, empty="No permission changes."),
                inline=False,
            )
        embed.add_field(name="Existing Scope", value=preview.existing_scope_summary, inline=False)
        embed.add_field(
            name="Future Automation",
            value=_future_rule_status_text(preview.future_rule_action, preview.future_rule_summary),
            inline=False,
        )
        failed_lines: list[str] = []
        guild = self._guild()
        for row in result.results:
            if row.action != "failed" or not row.reason:
                continue
            channel = guild.get_channel(row.channel_id) if guild is not None else None
            label = getattr(channel, "mention", f"<#{row.channel_id}>")
            failed_lines.append(f"{label}: {row.reason}")
        if failed_lines:
            embed.add_field(
                name="Failures",
                value=ge.join_limited_lines(failed_lines[:PERMISSION_ORCHESTRATION_PREVIEW_LIMIT], limit=1024, empty="None"),
                inline=False,
            )
            embed.add_field(
                name="Operator Note",
                value=(
                    "Some current channels rejected the overwrite update. Review the failures above before assuming the role is fully aligned. "
                    "The future-automation status shown here still reflects what Babblebox saved for newly created channels."
                ),
                inline=False,
            )
        return embed
