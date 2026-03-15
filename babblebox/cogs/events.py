from __future__ import annotations

import contextlib

import discord
from discord.ext import commands

from babblebox import game_engine as ge


class EventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        author_afk = ge.afk_records.get(message.author.id)
        if ge.is_active_afk_record(author_afk):
            ge.clear_afk_state(message.author.id)
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Welcome Back",
                        f"{message.author.mention}, I removed your AFK status.",
                        tone="success",
                        footer="Babblebox AFK",
                    ),
                    delete_after=5.0,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )

        active_afk_mentions = [
            (mention, ge.afk_records.get(mention.id))
            for mention in message.mentions
            if mention.id != message.author.id and ge.is_active_afk_record(ge.afk_records.get(mention.id))
        ]
        if active_afk_mentions:
            lines_to_send = [ge.build_afk_brief_line(user, record) for user, record in active_afk_mentions[:5]]
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "AFK Notice",
                        "\n".join(lines_to_send),
                        tone="info",
                        footer="Babblebox AFK | Mentions are muted to avoid ping spam",
                    ),
                    delete_after=12.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        prefix = self.bot.command_prefix
        if isinstance(prefix, str) and message.content.startswith(prefix):
            return

        if isinstance(message.channel, discord.DMChannel):
            guild_id = ge.dm_routes.get(message.author.id)
            if guild_id is not None:
                game = ge.games.get(guild_id)
                if not game or game.get("closing") or not game.get("active"):
                    ge.release_dm_route(message.author.id, guild_id)
                else:
                    async with game["lock"]:
                        game = ge.games.get(guild_id)
                        if not game or game.get("closing") or not game.get("active"):
                            ge.release_dm_route(message.author.id, guild_id)
                        else:
                            current_player = ge.get_current_player(game)
                            if current_player and current_player.id == message.author.id:
                                if game["game_type"] == "corpse":
                                    await ge.handle_corpse_turn_locked(message, guild_id, game)
                                    return
                                if game["game_type"] == "telephone":
                                    await ge.handle_telephone_turn_locked(message, guild_id, game)
                                    return

        if message.guild:
            guild_id = message.guild.id
            game = ge.games.get(guild_id)
            if (
                game
                and not game.get("closing")
                and game.get("active")
                and game.get("game_type") == "bomb"
                and message.channel.id == game["channel"].id
            ):
                async with game["lock"]:
                    game = ge.games.get(guild_id)
                    if (
                        game
                        and not game.get("closing")
                        and game.get("active")
                        and game.get("game_type") == "bomb"
                    ):
                        current_player = ge.get_current_player(game)
                        if current_player and current_player.id == message.author.id:
                            await ge.handle_bomb_turn_locked(message, guild_id, game)
                            return

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        game = ge.games.get(member.guild.id)
        if not game or game.get("closing"):
            return

        involved = ge.is_player_in_game(game, member.id) or game["host"].id == member.id
        if not involved:
            return

        async with game["lock"]:
            game = ge.games.get(member.guild.id)
            if not game or game.get("closing"):
                return

            was_host = game["host"].id == member.id
            was_player = ge.is_player_in_game(game, member.id)
            if was_player:
                game["players"] = [player for player in game["players"] if player.id != member.id]

            if game.get("active"):
                with contextlib.suppress(discord.HTTPException):
                    await game["channel"].send(
                        embed=ge.make_status_embed(
                            "Player Left Mid-Game",
                            f"{member.display_name} left the server during the game. The match was cancelled to avoid a broken state.",
                            tone="danger",
                            footer="Babblebox Safety Shutdown",
                        )
                    )
                await ge.cleanup_game(member.guild.id)
                return

            if was_host:
                with contextlib.suppress(discord.HTTPException):
                    await game["channel"].send(
                        embed=ge.make_status_embed(
                            "Lobby Closed",
                            "The host left the server, so the lobby has been closed.",
                            tone="warning",
                            footer="Babblebox Lobby",
                        )
                    )
                await ge.cleanup_game(member.guild.id)
                return

            lobby_view = next((view for view in game["views"] if isinstance(view, ge.LobbyView)), None)
            if lobby_view and lobby_view.message:
                lobby_view.refresh_components()
                with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                    await lobby_view.message.edit(embed=ge.get_lobby_embed(member.guild.id), view=lobby_view)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return

        game = ge.games.get(payload.guild_id)
        if not game or game.get("closing"):
            return

        tracked_message_ids = {
            view.message.id
            for view in game.get("views", [])
            if getattr(view, "message", None) is not None
        }
        if payload.message_id not in tracked_message_ids:
            return

        async with game["lock"]:
            game = ge.games.get(payload.guild_id)
            if not game or game.get("closing"):
                return
            with contextlib.suppress(discord.HTTPException):
                await game["channel"].send(
                    embed=ge.make_status_embed(
                        "Panel Deleted",
                        "A live game panel was deleted, so Babblebox closed the game to prevent a broken state.",
                        tone="danger",
                        footer="Babblebox Safety Shutdown",
                    )
                )
            await ge.cleanup_game(payload.guild_id)

    @commands.Cog.listener()
    async def on_ready(self):
        print(f"Bot {self.bot.user} is ready.")


async def setup(bot: commands.Bot):
    await bot.add_cog(EventsCog(bot))
