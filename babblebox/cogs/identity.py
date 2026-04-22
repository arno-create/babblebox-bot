from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.daily_challenges import DAILY_DEFAULT_MODE
from babblebox.profile_service import BUDDY_STYLES, ProfileService
from babblebox.runtime_health import bind_started_service


DAILY_LEADERBOARD_CHOICES = [
    app_commands.Choice(name="Total clears", value="clears"),
    app_commands.Choice(name="Current streak", value="streak"),
]
DAILY_MODE_CHOICES = [
    app_commands.Choice(name="Shuffle Booth", value="shuffle"),
    app_commands.Choice(name="Emoji Booth", value="emoji"),
    app_commands.Choice(name="Signal Booth", value="signal"),
]
VISIBILITY_CHOICES = [
    app_commands.Choice(name="Public", value="public"),
    app_commands.Choice(name="Only me", value="private"),
]

BUDDY_STYLE_CHOICES = [
    app_commands.Choice(name=meta["label"], value=style_id)
    for style_id, meta in BUDDY_STYLES.items()
]


class IdentityCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ProfileService(bot)
        self._public_user_cooldowns: dict[tuple[str, int], float] = {}
        self._public_channel_cooldowns: dict[tuple[str, int], float] = {}

    async def cog_load(self):
        await bind_started_service(self.bot, attr_name="profile_service", service=self.service, label="Profile")

    def cog_unload(self):
        if getattr(self.bot, "profile_service", None) is self.service:
            delattr(self.bot, "profile_service")
        self.bot.loop.create_task(self.service.close())

    async def _require_storage(
        self,
        ctx: commands.Context,
        feature_name: str,
        *,
        defer_response: bool = True,
        defer_ephemeral: bool = True,
    ) -> bool:
        if defer_response:
            await defer_hybrid_response(ctx, ephemeral=defer_ephemeral)
        if self.service.storage_ready:
            return True
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                f"{feature_name} Unavailable",
                self.service.storage_message(feature_name),
                tone="warning",
                footer="Babblebox Identity",
            ),
            ephemeral=True,
        )
        return False

    def _utility_summary_for(self, *, user_id: int, guild_id: int | None) -> dict | None:
        utility_service = getattr(self.bot, "utility_service", None)
        if utility_service is None or not getattr(utility_service, "storage_ready", False):
            return None
        watch_summary = utility_service.get_watch_summary(user_id, guild_id=guild_id)
        return {
            "watch_enabled": bool(watch_summary["mention_global"] or watch_summary["mention_server_enabled"] or watch_summary["total_keywords"]),
            "active_later_markers": len(utility_service.list_later_markers(user_id)),
            "active_reminders": len(utility_service.list_reminders(user_id)),
        }

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    def _mark_public_cooldown(self, ctx: commands.Context, *, bucket: str, visibility: str):
        if self._is_private(visibility):
            return
        now = self.bot.loop.time()
        user_key = (bucket, ctx.author.id)
        channel_key = (bucket, ctx.channel.id if ctx.channel is not None else 0)
        self._public_user_cooldowns[user_key] = now
        if channel_key[1]:
            self._public_channel_cooldowns[channel_key] = now

    def _public_cooldown_error(
        self,
        ctx: commands.Context,
        *,
        bucket: str,
        visibility: str,
        user_seconds: float,
        channel_seconds: float,
    ) -> str | None:
        if self._is_private(visibility):
            return None
        now = self.bot.loop.time()
        user_key = (bucket, ctx.author.id)
        channel_key = (bucket, ctx.channel.id if ctx.channel is not None else 0)
        user_remaining = user_seconds - (now - self._public_user_cooldowns.get(user_key, 0.0))
        channel_remaining = channel_seconds - (now - self._public_channel_cooldowns.get(channel_key, 0.0))
        if user_remaining > 0 or channel_remaining > 0:
            wait_for = int(max(user_remaining, channel_remaining)) + 1
            return f"That public card is on cooldown. Try again in about {wait_for} seconds, or switch visibility to private."
        return None

    def _coerce_daily_root_args(self, mode: Optional[str], visibility: str) -> tuple[Optional[str], str]:
        if mode in {"public", "private"}:
            return None, mode
        return mode, visibility

    def _coerce_daily_play_args(
        self,
        ctx: commands.Context,
        mode: Optional[str],
        guess: Optional[str],
        visibility: str,
    ) -> tuple[Optional[str], Optional[str], str]:
        if getattr(ctx, "interaction", None) is not None:
            return mode, guess, visibility
        if guess is None:
            if mode in {"public", "private"}:
                return None, None, mode
            if mode is not None and mode not in {"shuffle", "emoji", "signal"}:
                return DAILY_DEFAULT_MODE, mode, visibility
            return mode, guess, visibility
        if guess in {"public", "private"}:
            if mode in {None, "shuffle", "emoji", "signal"}:
                return mode, None, guess
            return DAILY_DEFAULT_MODE, mode, guess
        if mode is not None and mode not in {"shuffle", "emoji", "signal"}:
            return DAILY_DEFAULT_MODE, f"{mode} {guess}", visibility
        return mode, guess, visibility

    @commands.hybrid_group(
        name="daily",
        with_app_command=True,
        description="Step into today's Babblebox Daily Arcade",
        invoke_without_command=True,
    )
    @app_commands.describe(mode="Optional arcade booth to open first", visibility="Show the arcade publicly or only to you")
    @app_commands.choices(mode=DAILY_MODE_CHOICES, visibility=VISIBILITY_CHOICES)
    async def daily_group(self, ctx: commands.Context, mode: Optional[str] = None, visibility: str = "public"):
        mode, visibility = self._coerce_daily_root_args(mode, visibility)
        if not await self._require_storage(ctx, "Daily", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="daily_panel",
            visibility=visibility,
            user_seconds=12.0,
            channel_seconds=6.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Cooldown", cooldown_error, tone="warning", footer="Babblebox Daily Arcade"),
                ephemeral=True,
            )
            return
        payload = await self.service.get_daily_status(ctx.author.id, mode=mode)
        self._mark_public_cooldown(ctx, bucket="daily_panel", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_daily_embed(ctx.author, payload, public=not self._is_private(visibility)),
            ephemeral=self._is_private(visibility),
        )

    @daily_group.command(name="play", with_app_command=True, description="Open a booth or submit a guess")
    @app_commands.describe(mode="Shuffle, Emoji, or Signal", guess="Your answer guess", visibility="Show the card publicly or only to you")
    @app_commands.choices(mode=DAILY_MODE_CHOICES, visibility=VISIBILITY_CHOICES)
    async def daily_play_command(
        self,
        ctx: commands.Context,
        mode: Optional[str] = None,
        *,
        guess: Optional[str] = None,
        visibility: str = "public",
    ):
        mode, guess, visibility = self._coerce_daily_play_args(ctx, mode, guess, visibility)
        if not await self._require_storage(ctx, "Daily", defer_response=False):
            return
        resolved_mode = mode or DAILY_DEFAULT_MODE
        if guess is None:
            cooldown_error = self._public_cooldown_error(
                ctx,
                bucket="daily_panel",
                visibility=visibility,
                user_seconds=12.0,
                channel_seconds=6.0,
            )
            if cooldown_error is not None:
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed("Daily Cooldown", cooldown_error, tone="warning", footer="Babblebox Daily Arcade"),
                    ephemeral=True,
                )
                return
            payload = await self.service.get_daily_status(ctx.author.id, mode=resolved_mode)
            self._mark_public_cooldown(ctx, bucket="daily_panel", visibility=visibility)
            await send_hybrid_response(
                ctx,
                embed=self.service.build_daily_embed(ctx.author, payload, public=not self._is_private(visibility)),
                ephemeral=self._is_private(visibility),
            )
            return
        ok, payload_or_message = await self.service.submit_daily_guess(ctx.author.id, guess, mode=resolved_mode)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Update", payload_or_message, tone="warning", footer="Babblebox Daily"),
                ephemeral=True,
            )
            return
        result_private = self._is_private(visibility) or payload_or_message["status"] == "retry"
        await send_hybrid_response(
            ctx,
            embed=self.service.build_daily_result_embed(ctx.author, payload_or_message, public=not result_private),
            ephemeral=result_private,
        )

    @daily_group.command(name="stats", with_app_command=True, description="View Daily streaks and recent Daily runs")
    @app_commands.describe(user="Whose Daily stats to view", visibility="Show the stats publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def daily_stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None, *, visibility: str = "public"):
        if not await self._require_storage(ctx, "Daily", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="daily_stats",
            visibility=visibility,
            user_seconds=15.0,
            channel_seconds=7.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Stats Cooldown", cooldown_error, tone="warning", footer="Babblebox Daily Arcade"),
                ephemeral=True,
            )
            return
        target = user or ctx.author
        payload = await self.service.get_daily_stats(target.id)
        if payload is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Stats", "Daily stats are not available right now.", tone="warning", footer="Babblebox Daily"),
                ephemeral=True,
            )
            return
        self._mark_public_cooldown(ctx, bucket="daily_stats", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_daily_stats_embed(target, payload, public=not self._is_private(visibility)),
            ephemeral=self._is_private(visibility),
        )

    @daily_group.command(name="share", with_app_command=True, description="Share a completed Daily Arcade booth")
    @app_commands.describe(mode="Which booth result to share", visibility="Post publicly or keep the result private")
    @app_commands.choices(mode=DAILY_MODE_CHOICES, visibility=VISIBILITY_CHOICES)
    async def daily_share_command(self, ctx: commands.Context, mode: str = DAILY_DEFAULT_MODE, visibility: str = "public"):
        if not await self._require_storage(ctx, "Daily", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="daily_share",
            visibility=visibility,
            user_seconds=18.0,
            channel_seconds=8.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Share Cooldown", cooldown_error, tone="warning", footer="Babblebox Daily Arcade"),
                ephemeral=True,
            )
            return
        ok, share_text = await self.service.build_daily_share(ctx.author.id, mode=mode)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Share", share_text, tone="warning", footer="Babblebox Daily"),
                ephemeral=True,
            )
            return
        self._mark_public_cooldown(ctx, bucket="daily_share", visibility=visibility)
        embed = discord.Embed(
            title="Daily Arcade Share",
            description=share_text,
            color=ge.EMBED_THEME["accent"],
        )
        await send_hybrid_response(
            ctx,
            embed=ge.style_embed(embed, footer="Babblebox Daily Arcade | Shared result"),
            ephemeral=self._is_private(visibility),
        )

    @daily_group.command(name="leaderboard", with_app_command=True, description="View the Babblebox Daily leaderboard")
    @app_commands.describe(metric="Rank by total clears or current streak")
    @app_commands.choices(metric=DAILY_LEADERBOARD_CHOICES)
    async def daily_leaderboard_command(self, ctx: commands.Context, metric: str = "clears"):
        if not await self._require_storage(ctx, "Daily", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="daily_leaderboard",
            visibility="public",
            user_seconds=12.0,
            channel_seconds=6.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Leaderboard Cooldown", cooldown_error, tone="warning", footer="Babblebox Daily Arcade"),
                ephemeral=True,
            )
            return
        entries = await self.service.get_daily_leaderboard(metric=metric)
        self._mark_public_cooldown(ctx, bucket="daily_leaderboard", visibility="public")
        await send_hybrid_response(ctx, embed=self.service.build_daily_leaderboard_embed(entries, metric=metric), ephemeral=False)

    @commands.hybrid_group(
        name="buddy",
        with_app_command=True,
        description="View and customize your Babblebox Buddy",
        invoke_without_command=True,
    )
    @app_commands.describe(visibility="Show your Buddy publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def buddy_group(self, ctx: commands.Context, visibility: str = "public"):
        if not await self._require_storage(ctx, "Buddy", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="buddy",
            visibility=visibility,
            user_seconds=15.0,
            channel_seconds=7.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Buddy Cooldown", cooldown_error, tone="warning", footer="Babblebox Buddy"),
                ephemeral=True,
            )
            return
        profile = await self.service.get_profile(ctx.author.id)
        knowledge_summary = await self.service.get_question_drop_summary(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild is not None else None,
        )
        self._mark_public_cooldown(ctx, bucket="buddy", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_buddy_embed(ctx.author, profile, knowledge_summary=knowledge_summary),
            ephemeral=self._is_private(visibility),
        )

    @buddy_group.command(name="profile", with_app_command=True, description="Open your Buddy card")
    @app_commands.describe(visibility="Show your Buddy publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def buddy_profile_command(self, ctx: commands.Context, visibility: str = "public"):
        if not await self._require_storage(ctx, "Buddy", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="buddy_profile",
            visibility=visibility,
            user_seconds=15.0,
            channel_seconds=7.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Buddy Cooldown", cooldown_error, tone="warning", footer="Babblebox Buddy"),
                ephemeral=True,
            )
            return
        profile = await self.service.get_profile(ctx.author.id)
        knowledge_summary = await self.service.get_question_drop_summary(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild is not None else None,
        )
        self._mark_public_cooldown(ctx, bucket="buddy_profile", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_buddy_embed(ctx.author, profile, knowledge_summary=knowledge_summary),
            ephemeral=self._is_private(visibility),
        )

    @buddy_group.command(name="rename", with_app_command=True, description="Rename your Babblebox Buddy")
    @app_commands.describe(nickname="A short safe name for your buddy")
    async def buddy_rename_command(self, ctx: commands.Context, *, nickname: str):
        if not await self._require_storage(ctx, "Buddy"):
            return
        ok, message = await self.service.rename_buddy(ctx.author.id, nickname)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Buddy Updated", message, tone="success" if ok else "warning", footer="Babblebox Buddy"),
            ephemeral=True,
        )

    @buddy_group.command(name="style", with_app_command=True, description="Change your buddy's style palette")
    @app_commands.describe(style="Pick a buddy style")
    @app_commands.choices(style=BUDDY_STYLE_CHOICES)
    async def buddy_style_command(self, ctx: commands.Context, style: str):
        if not await self._require_storage(ctx, "Buddy"):
            return
        ok, message = await self.service.set_buddy_style(ctx.author.id, style)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed("Buddy Style", message, tone="success" if ok else "warning", footer="Babblebox Buddy"),
            ephemeral=True,
        )

    @buddy_group.command(name="stats", with_app_command=True, description="See buddy XP, badges, and progression")
    @app_commands.describe(visibility="Show your Buddy stats publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def buddy_stats_command(self, ctx: commands.Context, visibility: str = "public"):
        if not await self._require_storage(ctx, "Buddy", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="buddy_stats",
            visibility=visibility,
            user_seconds=15.0,
            channel_seconds=7.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Buddy Cooldown", cooldown_error, tone="warning", footer="Babblebox Buddy"),
                ephemeral=True,
            )
            return
        profile = await self.service.get_profile(ctx.author.id)
        knowledge_summary = await self.service.get_question_drop_summary(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild is not None else None,
        )
        self._mark_public_cooldown(ctx, bucket="buddy_stats", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_buddy_stats_embed(ctx.author, profile, knowledge_summary=knowledge_summary),
            ephemeral=self._is_private(visibility),
        )

    @commands.hybrid_command(name="profile", with_app_command=True, description="View a Babblebox profile with Daily, Buddy, utilities, and game stats")
    @app_commands.describe(user="Whose profile to view", visibility="Show the profile publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def profile_command(self, ctx: commands.Context, user: Optional[discord.User] = None, visibility: str = "public"):
        if not await self._require_storage(ctx, "Profile", defer_response=False):
            return
        cooldown_error = self._public_cooldown_error(
            ctx,
            bucket="profile",
            visibility=visibility,
            user_seconds=15.0,
            channel_seconds=7.0,
        )
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Profile Cooldown", cooldown_error, tone="warning", footer="Babblebox Profile"),
                ephemeral=True,
            )
            return
        target = user or ctx.author
        profile = await self.service.get_profile(target.id)
        knowledge_summary = await self.service.get_question_drop_summary(
            target.id,
            guild_id=ctx.guild.id if ctx.guild is not None else None,
        )
        utility_summary = None
        if target.id == ctx.author.id and self._is_private(visibility):
            utility_summary = self._utility_summary_for(user_id=target.id, guild_id=ctx.guild.id if ctx.guild else None)
        session_stats = ge.session_stats.get(target.id)
        self._mark_public_cooldown(ctx, bucket="profile", visibility=visibility)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_profile_embed(
                target,
                profile,
                knowledge_summary=knowledge_summary,
                utility_summary=utility_summary,
                session_stats=session_stats,
                title="Babblebox Profile",
            ),
            ephemeral=self._is_private(visibility),
        )

    @commands.hybrid_command(name="vault", with_app_command=True, description="Open your personal Babblebox vault view")
    async def vault_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Vault"):
            return
        profile = await self.service.get_profile(ctx.author.id)
        knowledge_summary = await self.service.get_question_drop_summary(
            ctx.author.id,
            guild_id=ctx.guild.id if ctx.guild is not None else None,
        )
        utility_summary = self._utility_summary_for(user_id=ctx.author.id, guild_id=ctx.guild.id if ctx.guild else None)
        session_stats = ge.session_stats.get(ctx.author.id)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_profile_embed(
                ctx.author,
                profile,
                knowledge_summary=knowledge_summary,
                utility_summary=utility_summary,
                session_stats=session_stats,
                title="Babblebox Vault",
            ),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(IdentityCog(bot))
