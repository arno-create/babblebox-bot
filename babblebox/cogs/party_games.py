from __future__ import annotations

import discord
from discord import app_commands
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import send_hybrid_response
from babblebox.pattern_hunt_game import (
    build_pattern_hunt_status_embed,
    submit_pattern_theory_locked,
)


class PartyGamesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.hybrid_group(name="hunt", with_app_command=True, description="Private Pattern Hunt card and private rule guesses", invoke_without_command=True)
    async def hunt_group(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("Server Only", "Pattern Hunt only works inside a server.", tone="warning", footer="Babblebox Pattern Hunt"),
                ephemeral=True,
            )
            return
        game = ge.games.get(ctx.guild.id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed("No Active Hunt", "There is no live Pattern Hunt round right now.", tone="info", footer="Babblebox Pattern Hunt"),
                ephemeral=True,
            )
            return
        await send_hybrid_response(ctx, embed=build_pattern_hunt_status_embed(game, public=False, viewer=ctx.author), ephemeral=True)

    @hunt_group.command(name="status", with_app_command=True, description="Show the live Pattern Hunt card just for you")
    async def hunt_status_command(self, ctx: commands.Context):
        await PartyGamesCog.hunt_group.callback(self, ctx)

    @hunt_group.command(name="guess", with_app_command=True, description="Privately submit a natural Pattern Hunt theory")
    @app_commands.describe(
        theory="Your rule theory, like 'contains a number' or 'starts with b and has 3 words'",
    )
    async def hunt_guess_command(
        self,
        ctx: commands.Context,
        *,
        theory: str,
    ):
        if ctx.guild is None:
            await send_hybrid_response(ctx, embed=ge.make_status_embed("Server Only", "Pattern Hunt only works inside a server.", tone="warning", footer="Babblebox Pattern Hunt"), ephemeral=True)
            return
        game = ge.games.get(ctx.guild.id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            await send_hybrid_response(ctx, embed=ge.make_status_embed("No Active Hunt", "There is no live Pattern Hunt round right now.", tone="info", footer="Babblebox Pattern Hunt"), ephemeral=True)
            return
        async with game["lock"]:
            game = ge.games.get(ctx.guild.id)
            if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
                await send_hybrid_response(ctx, embed=ge.make_status_embed("No Active Hunt", "That Pattern Hunt round is already closed.", tone="warning", footer="Babblebox Pattern Hunt"), ephemeral=True)
                return
            ok, message = await submit_pattern_theory_locked(ctx.guild.id, game, ctx.author, theory)
        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Pattern Guess",
                message,
                tone="success" if ok and message == "You cracked it." else "warning",
                footer="Babblebox Pattern Hunt",
            ),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(PartyGamesCog(bot))
