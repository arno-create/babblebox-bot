from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.question_drops_content import QUESTION_DROP_CATEGORIES, QUESTION_DROP_DIFFICULTY_PROFILES, QUESTION_DROP_TONES
from babblebox.question_drops_service import QuestionDropsService


OWNER_AI_OVERRIDE_IDS = {1266444952779620413, 1345860619836063754}

STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
TONE_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_TONES]
DIFFICULTY_PROFILE_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_DIFFICULTY_PROFILES]
ACTIVITY_GATE_CHOICES = [
    app_commands.Choice(name="Light", value="light"),
    app_commands.Choice(name="Off", value="off"),
]
CATEGORY_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_CATEGORIES]
CHANNEL_ACTION_CHOICES = [
    app_commands.Choice(name="Add", value="add"),
    app_commands.Choice(name="Remove", value="remove"),
    app_commands.Choice(name="Clear", value="clear"),
]
CATEGORY_ACTION_CHOICES = [
    app_commands.Choice(name="Enable", value="enable"),
    app_commands.Choice(name="Disable", value="disable"),
    app_commands.Choice(name="Reset", value="reset"),
]
VISIBILITY_CHOICES = [
    app_commands.Choice(name="Public", value="public"),
    app_commands.Choice(name="Only me", value="private"),
]
TIER_CHOICES = [
    app_commands.Choice(name="Tier I", value=1),
    app_commands.Choice(name="Tier II", value=2),
    app_commands.Choice(name="Tier III", value=3),
]
RECALC_MODE_CHOICES = [
    app_commands.Choice(name="Preview", value="preview"),
    app_commands.Choice(name="Execute", value="execute"),
]
DIGEST_MENTION_CHOICES = [
    app_commands.Choice(name="No pings", value="none"),
    app_commands.Choice(name="@here", value="here"),
]
TEMPLATE_ACTION_CHOICES = [
    app_commands.Choice(name="Status", value="status"),
    app_commands.Choice(name="Edit", value="edit"),
    app_commands.Choice(name="Clear", value="clear"),
]
ROLE_PREFERENCE_MODE_CHOICES = [
    app_commands.Choice(name="Stop future grants", value="stop"),
    app_commands.Choice(name="Receive roles again", value="receive"),
]


class CategoryAnnouncementTemplateModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        cog: "QuestionDropsCog",
        category: str,
        tier: int | None = None,
        current_template: str | None = None,
        placeholder_tokens: tuple[str, ...] = (),
    ):
        super().__init__(title=f"Edit {cog.service._announcement_title(scope_type='category', scope_key=category, tier=tier)}")
        self.cog = cog
        self.category = str(category or "").strip().casefold()
        self.tier = int(tier) if isinstance(tier, int) else None
        self.template_input = discord.ui.TextInput(
            label="Tier Override Template" if self.tier is not None else "Template",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=220,
            default=current_template or "",
            placeholder=" ".join(str(token) for token in placeholder_tokens) or "Use approved placeholders only.",
        )
        self.add_item(self.template_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Server Only", "This editor only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not self.cog._is_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure Question Drops.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        ok, message = await self.cog.service.save_category_mastery_announcement_template(
            interaction.guild.id,
            category=self.category,
            template=str(self.template_input.value),
            tier=self.tier,
        )
        if not ok:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    self.cog.service._announcement_title(scope_type="category", scope_key=self.category, tier=self.tier),
                    message,
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        payload = await self.cog.service.get_category_mastery_announcement_status(
            interaction.guild,
            category=self.category,
            tier=self.tier,
        )
        await interaction.response.send_message(
            embed=self.cog.service.build_mastery_announcement_status_embed(payload, note=message),
            ephemeral=True,
        )


class ScholarAnnouncementTemplateModal(discord.ui.Modal):
    def __init__(
        self,
        *,
        cog: "QuestionDropsCog",
        tier: int | None = None,
        current_template: str | None = None,
        placeholder_tokens: tuple[str, ...] = (),
    ):
        super().__init__(title=f"Edit {cog.service._announcement_title(scope_type='scholar', scope_key='global', tier=tier)}")
        self.cog = cog
        self.tier = int(tier) if isinstance(tier, int) else None
        self.template_input = discord.ui.TextInput(
            label="Tier Override Template" if self.tier is not None else "Template",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=220,
            default=current_template or "",
            placeholder=" ".join(str(token) for token in placeholder_tokens) or "Use approved placeholders only.",
        )
        self.add_item(self.template_input)

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message(
                embed=ge.make_status_embed("Server Only", "This editor only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not self.cog._is_admin(interaction.user):
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    "Admin Only",
                    "You need **Manage Server** or administrator access to configure Question Drops.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        ok, message = await self.cog.service.save_scholar_announcement_template(
            interaction.guild.id,
            template=str(self.template_input.value),
            tier=self.tier,
        )
        if not ok:
            await interaction.response.send_message(
                embed=ge.make_status_embed(
                    self.cog.service._announcement_title(scope_type="scholar", scope_key="global", tier=self.tier),
                    message,
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        payload = await self.cog.service.get_scholar_announcement_status(interaction.guild, tier=self.tier)
        await interaction.response.send_message(
            embed=self.cog.service.build_mastery_announcement_status_embed(payload, note=message),
            ephemeral=True,
        )


class QuestionDropsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = QuestionDropsService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "question_drops_service", self.service)

    def cog_unload(self):
        if getattr(self.bot, "question_drops_service", None) is self.service:
            delattr(self.bot, "question_drops_service")
        self.bot.loop.create_task(self.service.close())

    async def _require_storage(self, ctx: commands.Context, feature_name: str, *, defer_response: bool = True) -> bool:
        if defer_response:
            await defer_hybrid_response(ctx, ephemeral=True)
        if self.service.storage_ready:
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                f"{feature_name} Unavailable",
                self.service.storage_message(feature_name),
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )
        return False

    def _profile_service(self):
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return None
        return profile_service

    async def _require_profile_storage(self, ctx: commands.Context, feature_name: str) -> bool:
        if self._profile_service() is not None:
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Profile Unavailable",
                f"{feature_name} need the Babblebox profile store.",
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )
        return False

    def _is_admin(self, member) -> bool:
        perms = getattr(member, "guild_permissions", None)
        return bool(getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False))

    async def _require_admin(self, ctx: commands.Context) -> bool:
        if self._is_admin(ctx.author):
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Admin Only",
                "You need **Manage Server** or administrator access to configure Question Drops.",
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )
        return False

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    def _is_override_owner(self, user_id: int) -> bool:
        return int(user_id or 0) in OWNER_AI_OVERRIDE_IDS

    async def _send_modal_only_notice(self, ctx: commands.Context, *, title: str):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                title,
                "Use the slash form of this command to open the Babblebox template editor modal.",
                tone="info",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    def _normalize_template_action(self, *, template_action: str | None, enabled: str | None) -> tuple[str | None, str | None]:
        normalized_action = str(template_action or "").strip().casefold() or None
        normalized_enabled = enabled
        if normalized_action is None and isinstance(enabled, str):
            maybe_action = enabled.strip().casefold()
            if maybe_action in {"status", "edit", "clear"}:
                normalized_action = maybe_action
                normalized_enabled = None
        return normalized_action, normalized_enabled

    def _template_mode_conflict_message(
        self,
        *,
        template_action: str | None,
        enabled: str | None,
        role: discord.Role | None,
        threshold: int | None,
        announcement_channel: discord.TextChannel | None,
        clear_announcement: bool,
        silent_grant: str | None,
    ) -> str | None:
        if template_action is None:
            return None
        if any(
            (
                enabled is not None,
                role is not None,
                threshold is not None,
                announcement_channel is not None,
                bool(clear_announcement),
                silent_grant is not None,
            )
        ):
            return (
                "Template mode only uses the scope, optional tier, and `template_action`. "
                "Leave enabled, role, threshold, announcement channel, clear announcement, and silent grant empty."
            )
        return None

    async def _send_server_only_notice(self, ctx: commands.Context, *, message: str = "This command only works inside a server."):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Server Only",
                message,
                tone="warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    async def _send_drops_status_overview(self, ctx: commands.Context):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx, message="Question Drops only work inside a server.")
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    async def _send_drops_admin_overview(self, ctx: commands.Context):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_admin(ctx):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    async def _send_drops_digest_status(self, ctx: commands.Context):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_admin(ctx):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_digest_status_embed(ctx.guild, snapshot), ephemeral=True)

    async def _handle_drops_config(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        drops_per_day: Optional[int] = None,
        timezone_name: Optional[str] = None,
        answer_window_seconds: Optional[int] = None,
        tone_mode: Optional[str] = None,
        difficulty_profile: Optional[str] = None,
        activity_gate: Optional[str] = None,
        active_start_hour: Optional[int] = None,
        active_end_hour: Optional[int] = None,
        ai_celebrations: Optional[str] = None,
    ):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_config(
            ctx.guild.id,
            enabled={"on": True, "off": False}.get(enabled) if enabled is not None else None,
            drops_per_day=drops_per_day,
            timezone_name=timezone_name,
            answer_window_seconds=answer_window_seconds,
            tone_mode=tone_mode,
            difficulty_profile=difficulty_profile,
            activity_gate=activity_gate,
            active_start_hour=active_start_hour,
            active_end_hour=active_end_hour,
            ai_celebrations_enabled={"on": True, "off": False}.get(ai_celebrations) if ai_celebrations is not None else None,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Config",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    async def _handle_drops_channels(self, ctx: commands.Context, action: str, channel: Optional[discord.TextChannel] = None):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_channels(ctx.guild.id, action=action, channel_id=channel.id if channel is not None else None)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Channels",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    async def _handle_drops_categories(self, ctx: commands.Context, action: str, category: Optional[str] = None):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_categories(ctx.guild.id, action=action, category=category)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Question Drops Categories",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    async def _handle_drops_digest_config(
        self,
        ctx: commands.Context,
        *,
        weekly: Optional[str] = None,
        monthly: Optional[str] = None,
        timezone_name: Optional[str] = None,
        shared_channel: Optional[discord.TextChannel] = None,
        weekly_channel: Optional[discord.TextChannel] = None,
        monthly_channel: Optional[discord.TextChannel] = None,
        skip_low_activity: Optional[str] = None,
        mention_mode: Optional[str] = None,
    ):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        ok, message = await self.service.update_digest_config(
            ctx.guild,
            weekly_enabled={"on": True, "off": False}.get(weekly) if weekly is not None else None,
            monthly_enabled={"on": True, "off": False}.get(monthly) if monthly is not None else None,
            timezone_name=timezone_name,
            shared_channel_id=shared_channel.id if shared_channel is not None else None,
            weekly_channel_id=weekly_channel.id if weekly_channel is not None else None,
            monthly_channel_id=monthly_channel.id if monthly_channel is not None else None,
            skip_low_activity={"on": True, "off": False}.get(skip_low_activity) if skip_low_activity is not None else None,
            mention_mode=mention_mode,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Knowledge Digests",
                message,
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            ),
            ephemeral=True,
        )

    async def _handle_drops_mastery_category(
        self,
        ctx: commands.Context,
        *,
        category: str,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        normalized_template_action, enabled = self._normalize_template_action(template_action=template_action, enabled=enabled)
        if not await self._require_storage(ctx, "Question Drops", defer_response=normalized_template_action != "edit"):
            return
        if not await self._require_admin(ctx):
            return
        template_conflict = self._template_mode_conflict_message(
            template_action=normalized_template_action,
            enabled=enabled,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
        )
        if template_conflict is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    self.service._announcement_title(scope_type="category", scope_key=category, tier=tier),
                    template_conflict,
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        if normalized_template_action is not None:
            if normalized_template_action == "edit":
                interaction = getattr(ctx, "interaction", None)
                if interaction is None:
                    await self._send_modal_only_notice(
                        ctx,
                        title=self.service._announcement_title(scope_type="category", scope_key=category, tier=tier),
                    )
                    return
                payload = await self.service.get_category_mastery_announcement_status(ctx.guild, category=category, tier=tier)
                if payload.get("status") != "ok":
                    await send_hybrid_response(
                        ctx,
                        embed=self.service.build_mastery_announcement_status_embed(payload),
                        ephemeral=True,
                    )
                    return
                await interaction.response.send_modal(
                    CategoryAnnouncementTemplateModal(
                        cog=self,
                        category=category,
                        tier=tier,
                        current_template=payload.get("announcement_template"),
                        placeholder_tokens=tuple(payload.get("placeholder_tokens", ())),
                    )
                )
                return
            if normalized_template_action == "clear":
                ok, message = await self.service.clear_category_mastery_announcement_template(
                    ctx.guild.id,
                    category=category,
                    tier=tier,
                )
                if not ok:
                    await send_hybrid_response(
                        ctx,
                        embed=ge.make_status_embed(
                            self.service._announcement_title(scope_type="category", scope_key=category, tier=tier),
                            message,
                            tone="warning",
                            footer="Babblebox Question Drops",
                        ),
                        ephemeral=True,
                    )
                    return
                payload = await self.service.get_category_mastery_announcement_status(ctx.guild, category=category, tier=tier)
                await send_hybrid_response(
                    ctx,
                    embed=self.service.build_mastery_announcement_status_embed(payload, note=message),
                    ephemeral=True,
                )
                return
            payload = await self.service.get_category_mastery_announcement_status(ctx.guild, category=category, tier=tier)
            await send_hybrid_response(
                ctx,
                embed=self.service.build_mastery_announcement_status_embed(payload),
                ephemeral=True,
            )
            return
        ok, message = await self.service.update_category_mastery(
            ctx.guild.id,
            category=category,
            enabled={"on": True, "off": False}.get(enabled) if enabled is not None else None,
            tier=tier,
            role_id=role.id if role is not None else None,
            threshold=threshold,
            announcement_channel_id=announcement_channel.id if announcement_channel is not None else None,
            clear_announcement_channel=clear_announcement,
            silent_grant={"on": True, "off": False}.get(silent_grant) if silent_grant is not None else None,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Category Mastery", message, tone="success" if ok else "warning", footer="Babblebox Question Drops"),
            ephemeral=True,
        )

    async def _handle_drops_mastery_scholar(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        normalized_template_action, enabled = self._normalize_template_action(template_action=template_action, enabled=enabled)
        if not await self._require_storage(ctx, "Question Drops", defer_response=normalized_template_action != "edit"):
            return
        if not await self._require_admin(ctx):
            return
        template_conflict = self._template_mode_conflict_message(
            template_action=normalized_template_action,
            enabled=enabled,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
        )
        if template_conflict is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    self.service._announcement_title(scope_type="scholar", scope_key="global", tier=tier),
                    template_conflict,
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        if normalized_template_action is not None:
            if normalized_template_action == "edit":
                interaction = getattr(ctx, "interaction", None)
                if interaction is None:
                    await self._send_modal_only_notice(
                        ctx,
                        title=self.service._announcement_title(scope_type="scholar", scope_key="global", tier=tier),
                    )
                    return
                payload = await self.service.get_scholar_announcement_status(ctx.guild, tier=tier)
                if payload.get("status") != "ok":
                    await send_hybrid_response(
                        ctx,
                        embed=self.service.build_mastery_announcement_status_embed(payload),
                        ephemeral=True,
                    )
                    return
                await interaction.response.send_modal(
                    ScholarAnnouncementTemplateModal(
                        cog=self,
                        tier=tier,
                        current_template=payload.get("announcement_template"),
                        placeholder_tokens=tuple(payload.get("placeholder_tokens", ())),
                    )
                )
                return
            if normalized_template_action == "clear":
                ok, message = await self.service.clear_scholar_announcement_template(ctx.guild.id, tier=tier)
                if not ok:
                    await send_hybrid_response(
                        ctx,
                        embed=ge.make_status_embed(
                            self.service._announcement_title(scope_type="scholar", scope_key="global", tier=tier),
                            message,
                            tone="warning",
                            footer="Babblebox Question Drops",
                        ),
                        ephemeral=True,
                    )
                    return
                payload = await self.service.get_scholar_announcement_status(ctx.guild, tier=tier)
                await send_hybrid_response(
                    ctx,
                    embed=self.service.build_mastery_announcement_status_embed(payload, note=message),
                    ephemeral=True,
                )
                return
            payload = await self.service.get_scholar_announcement_status(ctx.guild, tier=tier)
            await send_hybrid_response(
                ctx,
                embed=self.service.build_mastery_announcement_status_embed(payload),
                ephemeral=True,
            )
            return
        ok, message = await self.service.update_scholar_ladder(
            ctx.guild.id,
            enabled={"on": True, "off": False}.get(enabled) if enabled is not None else None,
            tier=tier,
            role_id=role.id if role is not None else None,
            threshold=threshold,
            announcement_channel_id=announcement_channel.id if announcement_channel is not None else None,
            clear_announcement_channel=clear_announcement,
            silent_grant={"on": True, "off": False}.get(silent_grant) if silent_grant is not None else None,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Scholar Ladder", message, tone="success" if ok else "warning", footer="Babblebox Question Drops"),
            ephemeral=True,
        )

    async def _handle_drops_mastery_recalc(self, ctx: commands.Context, member: Optional[discord.Member] = None, mode: str = "preview"):
        if ctx.guild is None:
            await self._send_server_only_notice(ctx)
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        preview = mode != "execute"
        summary = await self.service.recalculate_mastery_roles(ctx.guild, member=member, preview=preview)
        skipped_line = f" Skipped opted-out members: **{summary['skipped_opted_out']}**."
        if preview:
            body = f"Scanned **{summary['scanned']}** member(s). Pending role grants: **{summary['pending']}**.{skipped_line}"
            title = "Mastery Recalc Preview"
        else:
            body = f"Scanned **{summary['scanned']}** member(s). Granted **{summary['granted']}** missing role(s).{skipped_line}"
            title = "Mastery Recalc"
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(title, body, tone="success", footer="Babblebox Question Drops"),
            ephemeral=True,
        )

    @commands.hybrid_group(
        name="drops",
        with_app_command=True,
        description="Knowledge mastery lane for scheduled Question Drops",
        invoke_without_command=True,
    )
    async def drops_group(self, ctx: commands.Context):
        await self._send_drops_status_overview(ctx)

    @drops_group.command(name="status", with_app_command=True, description="Show Question Drops mastery status for this server")
    async def drops_status_command(self, ctx: commands.Context):
        await self._send_drops_status_overview(ctx)

    @drops_group.command(name="config", with_app_command=False, description="Update Question Drops schedule, tone, and AI opt-in")
    @app_commands.describe(
        enabled="Turn scheduled drops on or off",
        drops_per_day="How many drops per day to schedule (1-10)",
        timezone_name="Server timezone like Asia/Yerevan or UTC+04:00",
        answer_window_seconds="How many seconds answers stay open",
        tone_mode="Wrong-answer tone",
        activity_gate="Require recent chat activity before a drop can post",
        active_start_hour="Local hour when drops may start",
        active_end_hour="Local hour when drops stop",
        ai_celebrations="Let this guild opt into rare AI celebration copy",
    )
    @app_commands.choices(enabled=STATE_CHOICES, tone_mode=TONE_CHOICES, activity_gate=ACTIVITY_GATE_CHOICES, ai_celebrations=STATE_CHOICES)
    async def drops_config_command(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        drops_per_day: Optional[int] = None,
        timezone_name: Optional[str] = None,
        answer_window_seconds: Optional[int] = None,
        tone_mode: Optional[str] = None,
        activity_gate: Optional[str] = None,
        active_start_hour: Optional[int] = None,
        active_end_hour: Optional[int] = None,
        ai_celebrations: Optional[str] = None,
    ):
        await self._handle_drops_config(
            ctx,
            enabled=enabled,
            drops_per_day=drops_per_day,
            timezone_name=timezone_name,
            answer_window_seconds=answer_window_seconds,
            tone_mode=tone_mode,
            activity_gate=activity_gate,
            active_start_hour=active_start_hour,
            active_end_hour=active_end_hour,
            ai_celebrations=ai_celebrations,
        )

    @drops_group.command(name="channels", with_app_command=False, description="Add or remove Question Drops channels")
    @app_commands.describe(action="Add, remove, or clear channels", channel="Channel to update")
    @app_commands.choices(action=CHANNEL_ACTION_CHOICES)
    async def drops_channels_command(self, ctx: commands.Context, action: str, channel: Optional[discord.TextChannel] = None):
        await self._handle_drops_channels(ctx, action, channel)

    @drops_group.command(name="categories", with_app_command=False, description="Enable or disable Question Drops categories")
    @app_commands.describe(action="Enable, disable, or reset categories", category="Category to update")
    @app_commands.choices(action=CATEGORY_ACTION_CHOICES, category=CATEGORY_CHOICES)
    async def drops_categories_command(self, ctx: commands.Context, action: str, category: Optional[str] = None):
        await self._handle_drops_categories(ctx, action, category)

    @drops_group.command(name="stats", with_app_command=True, description="View guild-first Question Drops mastery progress")
    @app_commands.describe(user="Whose Question Drops stats to view", visibility="Show the card publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def drops_stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None, visibility: str = "public"):
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        profile_service = self._profile_service()
        if profile_service is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Profile Unavailable",
                    "Question Drops stats need the Babblebox profile store.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        target = user or ctx.author
        guild_id = ctx.guild.id if ctx.guild is not None else None
        summary = await profile_service.get_question_drop_summary(target.id, guild_id=guild_id)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_stats_embed(target, summary),
            ephemeral=self._is_private(visibility),
        )

    @drops_group.group(name="roles", with_app_command=True, invoke_without_command=True, description="Manage your Babblebox Question Drops roles")
    async def drops_roles_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_profile_storage(ctx, "Question Drops role preferences"):
            return
        payload = await self.service.get_member_roles_status(ctx.guild, ctx.author)
        await send_hybrid_response(ctx, embed=self.service.build_member_roles_status_embed(ctx.guild, ctx.author, payload), ephemeral=True)

    @drops_roles_group.command(name="status", with_app_command=True, description="View your Babblebox Question Drops role state")
    async def drops_roles_status_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_profile_storage(ctx, "Question Drops role preferences"):
            return
        payload = await self.service.get_member_roles_status(ctx.guild, ctx.author)
        await send_hybrid_response(ctx, embed=self.service.build_member_roles_status_embed(ctx.guild, ctx.author, payload), ephemeral=True)

    @drops_roles_group.command(name="remove", with_app_command=True, description="Remove current Babblebox-managed Question Drops roles")
    @app_commands.describe(role="One Babblebox-managed Question Drops role to remove; leave blank to remove all current Babblebox roles")
    async def drops_roles_remove_command(self, ctx: commands.Context, role: Optional[discord.Role] = None):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_profile_storage(ctx, "Question Drops role preferences"):
            return
        payload = await self.service.remove_member_managed_roles(ctx.guild, ctx.author, role_id=role.id if role is not None else None)
        await send_hybrid_response(ctx, embed=self.service.build_member_roles_remove_embed(payload), ephemeral=True)

    @drops_roles_group.command(name="preference", with_app_command=True, description="Stop or resume future Babblebox Question Drops role grants")
    @app_commands.describe(
        mode="Stop future Babblebox role grants or receive them again",
        remove_current_roles="When stopping future grants, also remove your current Babblebox Question Drops roles now",
        restore_current_roles="When receiving roles again, also restore currently eligible Babblebox roles now",
    )
    @app_commands.choices(mode=ROLE_PREFERENCE_MODE_CHOICES)
    async def drops_roles_preference_command(
        self,
        ctx: commands.Context,
        *,
        mode: str,
        remove_current_roles: bool = False,
        restore_current_roles: bool = False,
    ):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_profile_storage(ctx, "Question Drops role preferences"):
            return
        payload = await self.service.update_member_role_preference(
            ctx.guild,
            ctx.author,
            mode=mode,
            remove_current_roles=remove_current_roles,
            restore_current_roles=restore_current_roles,
        )
        await send_hybrid_response(ctx, embed=self.service.build_member_role_preference_embed(payload), ephemeral=True)

    @drops_group.command(name="leaderboard", with_app_command=True, description="View the guild knowledge leaderboard")
    @app_commands.describe(category="Optional category-specific leaderboard")
    @app_commands.choices(category=CATEGORY_CHOICES)
    async def drops_leaderboard_command(self, ctx: commands.Context, category: Optional[str] = None):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Profile Unavailable", "Knowledge leaderboards need the Babblebox profile store.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        entries = await profile_service.get_question_drop_leaderboard(guild_id=ctx.guild.id, category=category)
        await send_hybrid_response(ctx, embed=self.service.build_leaderboard_embed(ctx.guild, entries, category=category), ephemeral=False)

    @drops_group.group(name="digest", with_app_command=False, invoke_without_command=True, description="Configure weekly and monthly knowledge digests")
    async def drops_digest_group(self, ctx: commands.Context):
        await self._send_drops_digest_status(ctx)

    @drops_digest_group.command(name="config", with_app_command=False, description="Update digest cadence, channels, timezone, and ping behavior")
    @app_commands.describe(
        weekly="Turn the weekly digest on or off",
        monthly="Turn the monthly digest on or off",
        timezone_name="Digest timezone like Asia/Yerevan or UTC+04:00",
        shared_channel="Use one channel for both weekly and monthly digests",
        weekly_channel="Weekly digest channel when you want it separate",
        monthly_channel="Monthly digest channel when you want it separate",
        skip_low_activity="Skip quiet periods instead of posting thin digests",
        mention_mode="Digest ping behavior",
    )
    @app_commands.choices(weekly=STATE_CHOICES, monthly=STATE_CHOICES, skip_low_activity=STATE_CHOICES, mention_mode=DIGEST_MENTION_CHOICES)
    async def drops_digest_config_command(
        self,
        ctx: commands.Context,
        *,
        weekly: Optional[str] = None,
        monthly: Optional[str] = None,
        timezone_name: Optional[str] = None,
        shared_channel: Optional[discord.TextChannel] = None,
        weekly_channel: Optional[discord.TextChannel] = None,
        monthly_channel: Optional[discord.TextChannel] = None,
        skip_low_activity: Optional[str] = None,
        mention_mode: Optional[str] = None,
    ):
        await self._handle_drops_digest_config(
            ctx,
            weekly=weekly,
            monthly=monthly,
            timezone_name=timezone_name,
            shared_channel=shared_channel,
            weekly_channel=weekly_channel,
            monthly_channel=monthly_channel,
            skip_low_activity=skip_low_activity,
            mention_mode=mention_mode,
        )

    @drops_group.group(name="mastery", with_app_command=False, invoke_without_command=True, description="Configure mastery roles, scholar ranks, and recalculation")
    async def drops_mastery_group(self, ctx: commands.Context):
        await self._send_drops_admin_overview(ctx)

    @drops_mastery_group.command(name="category", with_app_command=False, description="Configure category mastery roles")
    @app_commands.describe(
        category="Which knowledge category to configure",
        enabled="Turn category mastery on or off",
        tier="Tier I, II, or III. Also selects a tier override when template_action is used",
        role="Discord role to grant at that tier",
        threshold="Knowledge points required for that tier",
        announcement_channel="Optional channel for role milestone notices",
        clear_announcement="Clear the category announcement channel",
        silent_grant="Grant the role quietly without posting a notice",
        template_action="Preview, edit, or clear the default or selected tier announcement template",
    )
    @app_commands.choices(
        category=CATEGORY_CHOICES,
        enabled=STATE_CHOICES,
        tier=TIER_CHOICES,
        silent_grant=STATE_CHOICES,
        template_action=TEMPLATE_ACTION_CHOICES,
    )
    async def drops_mastery_category_command(
        self,
        ctx: commands.Context,
        *,
        category: str,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        await self._handle_drops_mastery_category(
            ctx,
            category=category,
            enabled=enabled,
            tier=tier,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
            template_action=template_action,
        )

    @drops_mastery_group.command(name="scholar", with_app_command=False, description="Configure the guild scholar ladder")
    @app_commands.describe(
        enabled="Turn the scholar ladder on or off",
        tier="Scholar tier to configure. Also selects a tier override when template_action is used",
        role="Discord role to grant at that tier",
        threshold="Guild knowledge points required for that tier",
        announcement_channel="Optional channel for scholar notices",
        clear_announcement="Clear the scholar announcement channel",
        silent_grant="Grant scholar roles quietly without posting a notice",
        template_action="Preview, edit, or clear the default or selected tier announcement template",
    )
    @app_commands.choices(enabled=STATE_CHOICES, tier=TIER_CHOICES, silent_grant=STATE_CHOICES, template_action=TEMPLATE_ACTION_CHOICES)
    async def drops_mastery_scholar_command(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        await self._handle_drops_mastery_scholar(
            ctx,
            enabled=enabled,
            tier=tier,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
            template_action=template_action,
        )

    @drops_mastery_group.command(name="recalc", with_app_command=False, description="Preview or execute a grant-only mastery role recalculation")
    @app_commands.describe(member="Optional single member to recalculate", mode="Preview first, then execute when ready")
    @app_commands.choices(mode=RECALC_MODE_CHOICES)
    async def drops_mastery_recalc_command(self, ctx: commands.Context, member: Optional[discord.Member] = None, mode: str = "preview"):
        await self._handle_drops_mastery_recalc(ctx, member=member, mode=mode)

    @app_commands.allowed_installs(guilds=True, users=False)
    @app_commands.guild_only()
    @app_commands.default_permissions(manage_guild=True)
    @commands.hybrid_group(
        name="dropsadmin",
        with_app_command=True,
        description="Configure Question Drops schedules, digests, and mastery",
        invoke_without_command=True,
    )
    async def dropsadmin_group(self, ctx: commands.Context):
        await self._send_drops_admin_overview(ctx)

    @dropsadmin_group.command(name="config", with_app_command=True, description="Update Question Drops schedule, tone, and AI opt-in")
    @app_commands.describe(
        enabled="Turn scheduled drops on or off",
        drops_per_day="How many drops per day to schedule (1-10)",
        timezone_name="Server timezone like Asia/Yerevan or UTC+04:00",
        answer_window_seconds="How many seconds answers stay open",
        tone_mode="Wrong-answer tone",
        difficulty_profile="Welcoming, smart, or hard difficulty mix",
        activity_gate="Require recent chat activity before a drop can post",
        active_start_hour="Local hour when drops may start",
        active_end_hour="Local hour when drops stop",
        ai_celebrations="Let this guild opt into rare AI celebration copy",
    )
    @app_commands.choices(
        enabled=STATE_CHOICES,
        tone_mode=TONE_CHOICES,
        difficulty_profile=DIFFICULTY_PROFILE_CHOICES,
        activity_gate=ACTIVITY_GATE_CHOICES,
        ai_celebrations=STATE_CHOICES,
    )
    async def dropsadmin_config_command(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        drops_per_day: Optional[int] = None,
        timezone_name: Optional[str] = None,
        answer_window_seconds: Optional[int] = None,
        tone_mode: Optional[str] = None,
        difficulty_profile: Optional[str] = None,
        activity_gate: Optional[str] = None,
        active_start_hour: Optional[int] = None,
        active_end_hour: Optional[int] = None,
        ai_celebrations: Optional[str] = None,
    ):
        await self._handle_drops_config(
            ctx,
            enabled=enabled,
            drops_per_day=drops_per_day,
            timezone_name=timezone_name,
            answer_window_seconds=answer_window_seconds,
            tone_mode=tone_mode,
            difficulty_profile=difficulty_profile,
            activity_gate=activity_gate,
            active_start_hour=active_start_hour,
            active_end_hour=active_end_hour,
            ai_celebrations=ai_celebrations,
        )

    @dropsadmin_group.command(name="channels", with_app_command=True, description="Add or remove Question Drops channels")
    @app_commands.describe(action="Add, remove, or clear channels", channel="Channel to update")
    @app_commands.choices(action=CHANNEL_ACTION_CHOICES)
    async def dropsadmin_channels_command(self, ctx: commands.Context, action: str, channel: Optional[discord.TextChannel] = None):
        await self._handle_drops_channels(ctx, action, channel)

    @dropsadmin_group.command(name="categories", with_app_command=True, description="Enable or disable Question Drops categories")
    @app_commands.describe(action="Enable, disable, or reset categories", category="Category to update")
    @app_commands.choices(action=CATEGORY_ACTION_CHOICES, category=CATEGORY_CHOICES)
    async def dropsadmin_categories_command(self, ctx: commands.Context, action: str, category: Optional[str] = None):
        await self._handle_drops_categories(ctx, action, category)

    @dropsadmin_group.group(name="digest", with_app_command=True, invoke_without_command=True, description="Configure weekly and monthly knowledge digests")
    async def dropsadmin_digest_group(self, ctx: commands.Context):
        await self._send_drops_digest_status(ctx)

    @dropsadmin_digest_group.command(name="config", with_app_command=True, description="Update digest cadence, channels, timezone, and ping behavior")
    @app_commands.describe(
        weekly="Turn the weekly digest on or off",
        monthly="Turn the monthly digest on or off",
        timezone_name="Digest timezone like Asia/Yerevan or UTC+04:00",
        shared_channel="Use one channel for both weekly and monthly digests",
        weekly_channel="Weekly digest channel when you want it separate",
        monthly_channel="Monthly digest channel when you want it separate",
        skip_low_activity="Skip quiet periods instead of posting thin digests",
        mention_mode="Digest ping behavior",
    )
    @app_commands.choices(weekly=STATE_CHOICES, monthly=STATE_CHOICES, skip_low_activity=STATE_CHOICES, mention_mode=DIGEST_MENTION_CHOICES)
    async def dropsadmin_digest_config_command(
        self,
        ctx: commands.Context,
        *,
        weekly: Optional[str] = None,
        monthly: Optional[str] = None,
        timezone_name: Optional[str] = None,
        shared_channel: Optional[discord.TextChannel] = None,
        weekly_channel: Optional[discord.TextChannel] = None,
        monthly_channel: Optional[discord.TextChannel] = None,
        skip_low_activity: Optional[str] = None,
        mention_mode: Optional[str] = None,
    ):
        await self._handle_drops_digest_config(
            ctx,
            weekly=weekly,
            monthly=monthly,
            timezone_name=timezone_name,
            shared_channel=shared_channel,
            weekly_channel=weekly_channel,
            monthly_channel=monthly_channel,
            skip_low_activity=skip_low_activity,
            mention_mode=mention_mode,
        )

    @dropsadmin_group.group(name="mastery", with_app_command=True, invoke_without_command=True, description="Configure mastery roles, scholar ranks, and recalculation")
    async def dropsadmin_mastery_group(self, ctx: commands.Context):
        await self._send_drops_admin_overview(ctx)

    @dropsadmin_mastery_group.command(name="category", with_app_command=True, description="Configure category mastery roles")
    @app_commands.describe(
        category="Which knowledge category to configure",
        enabled="Turn category mastery on or off",
        tier="Tier I, II, or III. Also selects a tier override when template_action is used",
        role="Discord role to grant at that tier",
        threshold="Knowledge points required for that tier",
        announcement_channel="Optional channel for role milestone notices",
        clear_announcement="Clear the category announcement channel",
        silent_grant="Grant the role quietly without posting a notice",
        template_action="Preview, edit, or clear the default or selected tier announcement template",
    )
    @app_commands.choices(
        category=CATEGORY_CHOICES,
        enabled=STATE_CHOICES,
        tier=TIER_CHOICES,
        silent_grant=STATE_CHOICES,
        template_action=TEMPLATE_ACTION_CHOICES,
    )
    async def dropsadmin_mastery_category_command(
        self,
        ctx: commands.Context,
        *,
        category: str,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        await self._handle_drops_mastery_category(
            ctx,
            category=category,
            enabled=enabled,
            tier=tier,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
            template_action=template_action,
        )

    @dropsadmin_mastery_group.command(name="scholar", with_app_command=True, description="Configure the guild scholar ladder")
    @app_commands.describe(
        enabled="Turn the scholar ladder on or off",
        tier="Scholar tier to configure. Also selects a tier override when template_action is used",
        role="Discord role to grant at that tier",
        threshold="Guild knowledge points required for that tier",
        announcement_channel="Optional channel for scholar notices",
        clear_announcement="Clear the scholar announcement channel",
        silent_grant="Grant scholar roles quietly without posting a notice",
        template_action="Preview, edit, or clear the default or selected tier announcement template",
    )
    @app_commands.choices(enabled=STATE_CHOICES, tier=TIER_CHOICES, silent_grant=STATE_CHOICES, template_action=TEMPLATE_ACTION_CHOICES)
    async def dropsadmin_mastery_scholar_command(
        self,
        ctx: commands.Context,
        *,
        enabled: Optional[str] = None,
        tier: Optional[int] = None,
        role: Optional[discord.Role] = None,
        threshold: Optional[int] = None,
        announcement_channel: Optional[discord.TextChannel] = None,
        clear_announcement: bool = False,
        silent_grant: Optional[str] = None,
        template_action: Optional[str] = None,
    ):
        await self._handle_drops_mastery_scholar(
            ctx,
            enabled=enabled,
            tier=tier,
            role=role,
            threshold=threshold,
            announcement_channel=announcement_channel,
            clear_announcement=clear_announcement,
            silent_grant=silent_grant,
            template_action=template_action,
        )

    @dropsadmin_mastery_group.command(name="recalc", with_app_command=True, description="Preview or execute a grant-only mastery role recalculation")
    @app_commands.describe(member="Optional single member to recalculate", mode="Preview first, then execute when ready")
    @app_commands.choices(mode=RECALC_MODE_CHOICES)
    async def dropsadmin_mastery_recalc_command(self, ctx: commands.Context, member: Optional[discord.Member] = None, mode: str = "preview"):
        await self._handle_drops_mastery_recalc(ctx, member=member, mode=mode)

    @commands.command(name="dropscelebaiglobal", hidden=True)
    async def drops_celebration_ai_global_override_command(self, ctx: commands.Context, mode: str = "status"):
        if ctx.guild is not None:
            await ctx.send(content="That command is only available in DM.")
            return
        author_id = getattr(ctx.author, "id", 0)
        if not self._is_override_owner(author_id):
            print(f"Question Drops AI override denied: unauthorized_dm_user_id={author_id}")
            await ctx.send(content="That command is unavailable.")
            return
        normalized_mode = str(mode or "status").strip().lower()
        if normalized_mode not in {"status", "off", "rare", "event_only"}:
            await ctx.send(
                embed=ge.make_status_embed(
                    "Question Drops AI Override",
                    "Use `status`, `off`, `rare`, or `event_only`.",
                    tone="info",
                    footer="Babblebox Question Drops",
                )
            )
            return
        if normalized_mode == "status":
            meta = self.service.get_meta()
            await ctx.send(
                embed=ge.make_status_embed(
                    "Question Drops AI Override",
                    f"Private maintainer status for rare AI celebration copy.\nCurrent mode: **{meta.get('ai_celebration_mode', 'off')}**",
                    tone="info",
                    footer="Babblebox Question Drops",
                )
            )
            return
        ok, message = await self.service.set_global_ai_celebration_mode(normalized_mode, actor_id=author_id)
        await ctx.send(
            embed=ge.make_status_embed(
                "Question Drops AI Override",
                message if ok else f"Override update failed: {message}",
                tone="success" if ok else "warning",
                footer="Babblebox Question Drops",
            )
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(QuestionDropsCog(bot))
