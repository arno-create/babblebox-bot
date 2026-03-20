from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, send_hybrid_response
from babblebox.profile_service import BUDDY_STYLES, ProfileService


DAILY_LEADERBOARD_CHOICES = [
    app_commands.Choice(name="Total clears", value="clears"),
    app_commands.Choice(name="Current streak", value="streak"),
]

BUDDY_STYLE_CHOICES = [
    app_commands.Choice(name=meta["label"], value=style_id)
    for style_id, meta in BUDDY_STYLES.items()
]


class IdentityCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.service = ProfileService(bot)

    async def cog_load(self):
        await self.service.start()
        setattr(self.bot, "profile_service", self.service)

    def cog_unload(self):
        if getattr(self.bot, "profile_service", None) is self.service:
            delattr(self.bot, "profile_service")
        self.bot.loop.create_task(self.service.close())

    async def _require_storage(self, ctx: commands.Context, feature_name: str) -> bool:
        await defer_hybrid_response(ctx, ephemeral=True)
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

    @commands.hybrid_group(
        name="daily",
        with_app_command=True,
        description="Play today's shared Babblebox Daily challenge",
        invoke_without_command=True,
    )
    async def daily_group(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Daily"):
            return
        payload = await self.service.get_daily_status(ctx.author.id)
        await send_hybrid_response(ctx, embed=self.service.build_daily_embed(ctx.author, payload), ephemeral=True)

    @daily_group.command(name="play", with_app_command=True, description="View today's Daily or submit a guess")
    @app_commands.describe(guess="Your Daily answer guess")
    async def daily_play_command(self, ctx: commands.Context, *, guess: Optional[str] = None):
        if not await self._require_storage(ctx, "Daily"):
            return
        if guess is None:
            payload = await self.service.get_daily_status(ctx.author.id)
            await send_hybrid_response(ctx, embed=self.service.build_daily_embed(ctx.author, payload), ephemeral=True)
            return
        ok, payload_or_message = await self.service.submit_daily_guess(ctx.author.id, guess)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Update", payload_or_message, tone="warning", footer="Babblebox Daily"),
                ephemeral=True,
            )
            return
        await send_hybrid_response(ctx, embed=self.service.build_daily_result_embed(ctx.author, payload_or_message), ephemeral=True)

    @daily_group.command(name="stats", with_app_command=True, description="View Daily streaks and recent Daily runs")
    @app_commands.describe(user="Whose Daily stats to view")
    async def daily_stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None):
        if not await self._require_storage(ctx, "Daily"):
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
        await send_hybrid_response(ctx, embed=self.service.build_daily_stats_embed(target, payload), ephemeral=True)

    @daily_group.command(name="share", with_app_command=True, description="Share your completed Daily result")
    async def daily_share_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Daily"):
            return
        ok, share_text = await self.service.build_daily_share(ctx.author.id)
        if not ok:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Daily Share", share_text, tone="warning", footer="Babblebox Daily"),
                ephemeral=True,
            )
            return
        embed = discord.Embed(
            title="Babblebox Daily Share",
            description=share_text,
            color=ge.EMBED_THEME["accent"],
        )
        await send_hybrid_response(ctx, embed=ge.style_embed(embed, footer="Babblebox Daily | Shared result"), ephemeral=False)

    @daily_group.command(name="leaderboard", with_app_command=True, description="View the Babblebox Daily leaderboard")
    @app_commands.describe(metric="Rank by total clears or current streak")
    @app_commands.choices(metric=DAILY_LEADERBOARD_CHOICES)
    async def daily_leaderboard_command(self, ctx: commands.Context, metric: str = "clears"):
        if not await self._require_storage(ctx, "Daily"):
            return
        entries = await self.service.get_daily_leaderboard(metric=metric)
        await send_hybrid_response(ctx, embed=self.service.build_daily_leaderboard_embed(entries, metric=metric), ephemeral=False)

    @commands.hybrid_group(
        name="buddy",
        with_app_command=True,
        description="View and customize your Babblebox Buddy",
        invoke_without_command=True,
    )
    async def buddy_group(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Buddy"):
            return
        profile = await self.service.get_profile(ctx.author.id)
        await send_hybrid_response(ctx, embed=self.service.build_buddy_embed(ctx.author, profile), ephemeral=True)

    @buddy_group.command(name="profile", with_app_command=True, description="Open your Buddy card")
    async def buddy_profile_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Buddy"):
            return
        profile = await self.service.get_profile(ctx.author.id)
        await send_hybrid_response(ctx, embed=self.service.build_buddy_embed(ctx.author, profile), ephemeral=True)

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
    async def buddy_stats_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Buddy"):
            return
        profile = await self.service.get_profile(ctx.author.id)
        await send_hybrid_response(ctx, embed=self.service.build_buddy_stats_embed(ctx.author, profile), ephemeral=True)

    @commands.hybrid_command(name="profile", with_app_command=True, description="View a Babblebox profile with Daily, Buddy, utilities, and game stats")
    @app_commands.describe(user="Whose profile to view")
    async def profile_command(self, ctx: commands.Context, user: Optional[discord.User] = None):
        if not await self._require_storage(ctx, "Profile"):
            return
        target = user or ctx.author
        profile = await self.service.get_profile(target.id)
        utility_summary = None
        if target.id == ctx.author.id:
            utility_summary = self._utility_summary_for(user_id=target.id, guild_id=ctx.guild.id if ctx.guild else None)
        session_stats = ge.session_stats.get(target.id)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_profile_embed(
                target,
                profile,
                utility_summary=utility_summary,
                session_stats=session_stats,
                title="Babblebox Profile",
            ),
            ephemeral=True,
        )

    @commands.hybrid_command(name="vault", with_app_command=True, description="Open your personal Babblebox vault view")
    async def vault_command(self, ctx: commands.Context):
        if not await self._require_storage(ctx, "Vault"):
            return
        profile = await self.service.get_profile(ctx.author.id)
        utility_summary = self._utility_summary_for(user_id=ctx.author.id, guild_id=ctx.guild.id if ctx.guild else None)
        session_stats = ge.session_stats.get(ctx.author.id)
        await send_hybrid_response(
            ctx,
            embed=self.service.build_profile_embed(
                ctx.author,
                profile,
                utility_summary=utility_summary,
                session_stats=session_stats,
                title="Babblebox Vault",
            ),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(IdentityCog(bot))
