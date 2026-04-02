from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.question_drops_content import QUESTION_DROP_CATEGORIES, QUESTION_DROP_TONES
from babblebox.question_drops_service import QuestionDropsService


OWNER_AI_OVERRIDE_IDS = {1266444952779620413, 1345860619836063754}

STATE_CHOICES = [
    app_commands.Choice(name="On", value="on"),
    app_commands.Choice(name="Off", value="off"),
]
TONE_CHOICES = [app_commands.Choice(name=value.title(), value=value) for value in QUESTION_DROP_TONES]
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

    @commands.hybrid_group(
        name="drops",
        with_app_command=True,
        description="Knowledge mastery lane for scheduled Question Drops",
        invoke_without_command=True,
    )
    async def drops_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "Question Drops only work inside a server.",
                    tone="warning",
                    footer="Babblebox Question Drops",
                ),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_group.command(name="status", with_app_command=True, description="Show Question Drops mastery status for this server")
    async def drops_status_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_group.command(name="config", with_app_command=True, description="Update Question Drops schedule, tone, and AI opt-in")
    @app_commands.default_permissions(manage_guild=True)
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
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
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

    @drops_group.command(name="channels", with_app_command=True, description="Add or remove Question Drops channels")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(action="Add, remove, or clear channels", channel="Channel to update")
    @app_commands.choices(action=CHANNEL_ACTION_CHOICES)
    async def drops_channels_command(self, ctx: commands.Context, action: str, channel: Optional[discord.TextChannel] = None):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
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

    @drops_group.command(name="categories", with_app_command=True, description="Enable or disable Question Drops categories")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(action="Enable, disable, or reset categories", category="Category to update")
    @app_commands.choices(action=CATEGORY_ACTION_CHOICES, category=CATEGORY_CHOICES)
    async def drops_categories_command(self, ctx: commands.Context, action: str, category: Optional[str] = None):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
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

    @drops_group.command(name="stats", with_app_command=True, description="View guild-first Question Drops mastery progress")
    @app_commands.describe(user="Whose Question Drops stats to view", visibility="Show the card publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def drops_stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None, visibility: str = "public"):
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
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

    @drops_group.group(name="digest", with_app_command=True, invoke_without_command=True, description="Configure weekly and monthly knowledge digests")
    async def drops_digest_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_admin(ctx):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_digest_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_digest_group.command(name="config", with_app_command=True, description="Update digest cadence, channels, timezone, and ping behavior")
    @app_commands.default_permissions(manage_guild=True)
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
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
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

    @drops_group.group(name="mastery", with_app_command=True, invoke_without_command=True, description="Configure mastery roles, scholar ranks, and recalculation")
    async def drops_mastery_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops", defer_response=False):
            return
        if not await self._require_admin(ctx):
            return
        snapshot = await self.service.get_status_snapshot(ctx.guild)
        await send_hybrid_response(ctx, embed=self.service.build_status_embed(ctx.guild, snapshot), ephemeral=True)

    @drops_mastery_group.command(name="category", with_app_command=True, description="Configure category mastery roles")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        category="Which knowledge category to configure",
        enabled="Turn category mastery on or off",
        tier="Tier I, II, or III",
        role="Discord role to grant at that tier",
        threshold="Knowledge points required for that tier",
        announcement_channel="Optional channel for role milestone notices",
        clear_announcement="Clear the category announcement channel",
        silent_grant="Grant the role quietly without posting a notice",
    )
    @app_commands.choices(category=CATEGORY_CHOICES, enabled=STATE_CHOICES, tier=TIER_CHOICES, silent_grant=STATE_CHOICES)
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
    ):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
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

    @drops_mastery_group.command(name="scholar", with_app_command=True, description="Configure the guild scholar ladder")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(
        enabled="Turn the scholar ladder on or off",
        tier="Scholar tier to configure",
        role="Discord role to grant at that tier",
        threshold="Guild knowledge points required for that tier",
        announcement_channel="Optional channel for scholar notices",
        clear_announcement="Clear the scholar announcement channel",
        silent_grant="Grant scholar roles quietly without posting a notice",
    )
    @app_commands.choices(enabled=STATE_CHOICES, tier=TIER_CHOICES, silent_grant=STATE_CHOICES)
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
    ):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
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

    @drops_mastery_group.command(name="recalc", with_app_command=True, description="Preview or execute a grant-only mastery role recalculation")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(member="Optional single member to recalculate", mode="Preview first, then execute when ready")
    @app_commands.choices(mode=RECALC_MODE_CHOICES)
    async def drops_mastery_recalc_command(self, ctx: commands.Context, member: Optional[discord.Member] = None, mode: str = "preview"):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "This command only works inside a server.", tone="warning", footer="Babblebox Question Drops"),
                ephemeral=True,
            )
            return
        if not await self._require_storage(ctx, "Question Drops"):
            return
        if not await self._require_admin(ctx):
            return
        preview = mode != "execute"
        summary = await self.service.recalculate_mastery_roles(ctx.guild, member=member, preview=preview)
        if preview:
            body = f"Scanned **{summary['scanned']}** member(s). Pending role grants: **{summary['pending']}**."
            title = "Mastery Recalc Preview"
        else:
            body = f"Scanned **{summary['scanned']}** member(s). Granted **{summary['granted']}** missing role(s)."
            title = "Mastery Recalc"
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(title, body, tone="success", footer="Babblebox Question Drops"),
            ephemeral=True,
        )

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
