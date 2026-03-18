from __future__ import annotations

import asyncio
import contextlib

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import defer_hybrid_response, require_channel_permissions, send_hybrid_response


class GameplayCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.hybrid_command(name="play", with_app_command=True, description="Open the Babblebox menu and host a game")
    async def play_command(self, ctx: commands.Context):
        if ctx.guild is None or ctx.channel is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "This command only works inside a server.",
                    tone="warning",
                    footer="Babblebox Lobby",
                ),
                ephemeral=True,
            )
            return

        await defer_hybrid_response(ctx)
        if not await require_channel_permissions(ctx, ge.PLAY_REQUIRED_PERMS, "/play"):
            return

        guild_id = ctx.guild.id
        async with ge.games_guard:
            if guild_id in ge.games:
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "Game Room Is Busy",
                        "A lobby or game is already active on this server. Use `/stop` or `bb!stop` to cancel it.",
                        tone="danger",
                        footer="Babblebox Lobby",
                    ),
                    ephemeral=True,
                )
                return

            ge.games[guild_id] = ge.create_game_state(ctx.author, ctx.channel)

        view = ge.LobbyView(guild_id)
        try:
            message = await send_hybrid_response(ctx, embed=ge.get_lobby_embed(guild_id), view=view)
            if message is not None:
                ge.register_view(guild_id, view, message)
        except Exception:
            await ge.cleanup_game(guild_id)
            raise

    @commands.hybrid_command(name="vote", with_app_command=True, description="Trigger a Spyfall vote")
    async def vote_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "This command only works inside a server.",
                    tone="warning",
                    footer="Babblebox Spyfall",
                ),
                ephemeral=True,
            )
            return

        await defer_hybrid_response(ctx)
        if not await require_channel_permissions(ctx, ge.VOTE_REQUIRED_PERMS, "/vote"):
            return

        if getattr(ctx, "interaction", None) is not None:
            await ge.trigger_spyfall_vote(ctx.interaction)
            return

        await self._trigger_spyfall_vote_prefix(ctx)

    @commands.hybrid_command(name="stop", with_app_command=True, description="Host/Admin Only: Force stop the current game and clear the lobby")
    async def stop_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "This command only works inside a server.",
                    tone="warning",
                    footer="Babblebox Lobby",
                ),
                ephemeral=True,
            )
            return

        await defer_hybrid_response(ctx)
        if not await require_channel_permissions(ctx, ge.STOP_REQUIRED_PERMS, "/stop"):
            return

        game = ge.games.get(ctx.guild.id)
        if not game:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Nothing To Stop",
                    "There is no active game or lobby to stop.",
                    tone="info",
                    footer="Babblebox Lobby",
                ),
                ephemeral=True,
            )
            return

        if ctx.author.id != game["host"].id and not ctx.author.guild_permissions.administrator:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Permission Denied",
                    "Only the host or an administrator can stop the game.",
                    tone="warning",
                    footer="Babblebox Lobby",
                ),
                ephemeral=True,
            )
            return

        async with game["lock"]:
            await ge.cleanup_game(ctx.guild.id)

        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Game Stopped",
                "The game has been forcibly shut down by the host. The lobby is now open for a new `/play` or `bb!play`.",
                tone="danger",
                footer="Babblebox Lobby",
            ),
            ephemeral=False,
        )

    @commands.hybrid_command(name="chaoscard", with_app_command=True, description="Cycle the lobby Chaos Card or show the active one")
    async def chaoscard_command(self, ctx: commands.Context):
        if ctx.guild is None:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Server Only",
                    "This command only works inside a server.",
                    tone="warning",
                    footer="Babblebox Chaos Cards",
                ),
                ephemeral=True,
            )
            return

        await defer_hybrid_response(ctx, ephemeral=True)
        game = ge.games.get(ctx.guild.id)
        if not game:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Active Lobby",
                    "There is no active lobby right now. Start one with `/play` or `bb!play`.",
                    tone="info",
                    footer="Babblebox Chaos Cards",
                ),
                ephemeral=True,
            )
            return

        card = ge.get_chaos_card_config(game.get("chaos_card", "none"))
        if game.get("active"):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Active Chaos Card",
                    f"**{card['label']}** - {card['description']}",
                    tone="accent",
                    footer="Babblebox Chaos Cards",
                ),
                ephemeral=True,
            )
            return

        if ctx.author.id != game["host"].id:
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "Host Only",
                    f"Only the host can cycle the Chaos Card. Current card: **{card['label']}** - {card['description']}",
                    tone="warning",
                    footer="Babblebox Chaos Cards",
                ),
                ephemeral=True,
            )
            return

        async with game["lock"]:
            game = ge.games.get(ctx.guild.id)
            if not game or game.get("closing") or game.get("active"):
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "Lobby Closed",
                        "This lobby is already closed.",
                        tone="warning",
                        footer="Babblebox Chaos Cards",
                    ),
                    ephemeral=True,
                )
                return

            game["chaos_card"] = ge.get_next_chaos_card(game.get("chaos_card", "none"))
            card = ge.get_chaos_card_config(game["chaos_card"])

            lobby_view = next((view for view in game["views"] if isinstance(view, ge.LobbyView)), None)
            if lobby_view and lobby_view.message is not None:
                lobby_view.refresh_components()
                with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                    await lobby_view.message.edit(embed=ge.get_lobby_embed(ctx.guild.id), view=lobby_view)

        await send_hybrid_response(
            ctx,
            embed=ge.make_status_embed(
                "Chaos Card Updated",
                f"Chaos Card set to **{card['label']}** - {card['description']}",
                tone="accent",
                footer="Babblebox Chaos Cards",
            ),
            ephemeral=True,
        )

    async def _trigger_spyfall_vote_prefix(self, ctx: commands.Context):
        guild_id = ctx.guild.id
        game = ge.games.get(guild_id)
        if not game or game.get("closing"):
            await send_hybrid_response(
                ctx,
                embed=ge.make_status_embed(
                    "No Active Vote",
                    "No active Spyfall game is available to vote on.",
                    tone="info",
                    footer="Babblebox Spyfall",
                ),
                ephemeral=True,
            )
            return

        async with game["lock"]:
            game = ge.games.get(guild_id)
            if not game or game.get("closing"):
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "No Active Vote",
                        "No active Spyfall game is available to vote on.",
                        tone="info",
                        footer="Babblebox Spyfall",
                    ),
                    ephemeral=True,
                )
                return
            if game.get("game_type") != "spyfall" or not game.get("active"):
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "No Active Vote",
                        "No active Spyfall game is available to vote on.",
                        tone="info",
                        footer="Babblebox Spyfall",
                    ),
                    ephemeral=True,
                )
                return
            if not ge.is_player_in_game(game, ctx.author.id):
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "Not In Match",
                        "You are not part of the current Spyfall match.",
                        tone="warning",
                        footer="Babblebox Spyfall",
                    ),
                    ephemeral=True,
                )
                return
            if game.get("voting_active"):
                await send_hybrid_response(
                    ctx,
                    embed=ge.make_status_embed(
                        "Vote Already Open",
                        "A Spyfall vote is already in progress.",
                        tone="warning",
                        footer="Babblebox Spyfall",
                    ),
                    ephemeral=True,
                )
                return

            game["voting_active"] = True
            game["votes"] = {}
            vote_token = ge.bump_token(game, "vote_token")
            ge.reset_idle_timer(guild_id)

            dashboard = ge.get_live_view(game, ge.SpyfallDashboard)
            if dashboard is not None:
                for child in dashboard.children:
                    child.disabled = True
                if dashboard.message is not None:
                    with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                        await dashboard.message.edit(view=dashboard)
                ge.unregister_view(guild_id, dashboard)
                dashboard.stop()

            vote_view = ge.SpyfallVoteView(guild_id)
            vote_timeout = game.get("spyfall_vote_timeout", ge.SPYFALL_VOTE_TIMEOUT_SECONDS)
            embed = discord.Embed(
                title="Emergency Meeting",
                description=(
                    f"{ctx.author.mention} called a vote!\n"
                    f"Select who you think the spy is. You have **{vote_timeout} seconds**."
                ),
                color=discord.Color.red(),
            )
            ge.style_embed(embed, footer="Babblebox Spyfall Vote | Cast one vote")
            content = f"Attention {ge.build_ping_string(game['players'])}!"
            vote_message = await ctx.send(content=content, embed=embed, view=vote_view)
            ge.register_view(guild_id, vote_view, vote_message)
            await ge.cancel_task(game.get("vote_task"))
            game["vote_task"] = asyncio.create_task(ge.spyfall_vote_timeout(guild_id, vote_token, game))


async def setup(bot: commands.Bot):
    await bot.add_cog(GameplayCog(bot))
