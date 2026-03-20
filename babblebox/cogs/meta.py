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


class MetaCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.hybrid_command(name="help", with_app_command=True, description="View the Babblebox manual, categories, and command guide")
    async def help_command(self, ctx: commands.Context):
        if not await require_channel_permissions(ctx, ge.HELP_REQUIRED_PERMS, "/help"):
            return
        await send_hybrid_response(ctx, embed=ge.build_help_embed(), ephemeral=True)

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
