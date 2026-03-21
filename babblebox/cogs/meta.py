from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import require_channel_permissions, send_hybrid_response


LEADERBOARD_LABELS = {
    "wins": "Wins",
    "bomb_wins": "Bomb Wins",
    "bomb_words": "Bomb Words",
    "spy_wins": "Spy Wins",
}
VISIBILITY_CHOICES = [
    app_commands.Choice(name="Public", value="public"),
    app_commands.Choice(name="Only me", value="private"),
]


class MetaCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._help_user_cooldowns: dict[int, float] = {}
        self._help_channel_cooldowns: dict[int, float] = {}

    def _is_private(self, visibility: str) -> bool:
        return visibility == "private"

    def _help_cooldown_error(self, ctx: commands.Context, *, visibility: str) -> str | None:
        if self._is_private(visibility):
            return None
        now = self.bot.loop.time()
        user_remaining = 15.0 - (now - self._help_user_cooldowns.get(ctx.author.id, 0.0))
        channel_key = ctx.channel.id if ctx.channel is not None else 0
        channel_remaining = 8.0 - (now - self._help_channel_cooldowns.get(channel_key, 0.0))
        if user_remaining > 0 or channel_remaining > 0:
            wait_for = int(max(user_remaining, channel_remaining)) + 1
            return f"The public manual is on cooldown. Try again in about {wait_for} seconds, or switch visibility to private."
        self._help_user_cooldowns[ctx.author.id] = now
        if channel_key:
            self._help_channel_cooldowns[channel_key] = now
        return None

    @commands.hybrid_command(name="help", with_app_command=True, description="View the Babblebox manual, categories, and command guide")
    @app_commands.describe(visibility="Show the manual publicly or only to you")
    @app_commands.choices(visibility=VISIBILITY_CHOICES)
    async def help_command(self, ctx: commands.Context, visibility: str = "public"):
        if not await require_channel_permissions(ctx, ge.HELP_REQUIRED_PERMS, "/help"):
            return
        cooldown_error = self._help_cooldown_error(ctx, visibility=visibility)
        if cooldown_error is not None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Help Cooldown", cooldown_error, tone="warning", footer="Babblebox Manual"),
                ephemeral=True,
            )
            return
        await send_hybrid_response(ctx, embed=ge.build_help_embed(), ephemeral=self._is_private(visibility))

    @commands.hybrid_command(name="ping", with_app_command=True, description="Check if the bot is online and responsive")
    async def ping_command(self, ctx: commands.Context):
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Pong!",
                "Babblebox is online, responsive, and ready for games, utilities, Daily, and Buddy commands.",
                tone="success",
            ),
            ephemeral=True,
        )

    @commands.hybrid_command(name="stats", with_app_command=True, description="View Babblebox session stats")
    @app_commands.describe(user="Whose session stats to view")
    async def stats_command(self, ctx: commands.Context, user: Optional[discord.User] = None):
        target = user or ctx.author
        stats = ge.session_stats.get(target.id)
        if not stats:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Stats Yet",
                    "No session stats were found for that player yet. Finish a game first.",
                    tone="warning",
                    footer="Babblebox Session Stats",
                ),
                ephemeral=True,
            )
            return

        await send_hybrid_response(ctx, embed=ge.build_stats_embed(target, stats), ephemeral=True)

    @commands.hybrid_command(name="leaderboard", with_app_command=True, description="View the Babblebox session leaderboard")
    @app_commands.describe(metric="What to rank players by")
    @app_commands.choices(
        metric=[
            app_commands.Choice(name="Wins", value="wins"),
            app_commands.Choice(name="Bomb Wins", value="bomb_wins"),
            app_commands.Choice(name="Bomb Words", value="bomb_words"),
            app_commands.Choice(name="Spy Wins", value="spy_wins"),
        ]
    )
    async def leaderboard_command(self, ctx: commands.Context, metric: str = "wins"):
        if metric not in LEADERBOARD_LABELS:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Unknown Metric",
                    f"Try one of: {', '.join(LEADERBOARD_LABELS)}.",
                    tone="warning",
                    footer="Babblebox Leaderboard",
                ),
                ephemeral=True,
            )
            return

        entries = [value for value in ge.session_stats.values() if value.get(metric, 0) > 0]
        if not entries:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Leaderboard Data",
                    "Nobody has any stats in that category yet. Finish a few games first.",
                    tone="warning",
                    footer="Babblebox Leaderboard",
                ),
                ephemeral=True,
            )
            return

        entries.sort(
            key=lambda item: (item.get(metric, 0), item.get("wins", 0), item.get("games_played", 0)),
            reverse=True,
        )
        await send_hybrid_response(
            ctx,
            embed=ge.build_leaderboard_embed(metric, LEADERBOARD_LABELS[metric], entries),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(MetaCog(bot))
