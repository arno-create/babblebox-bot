from __future__ import annotations

import contextlib

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import is_command_message


def _collect_away_targets(message: discord.Message) -> list[discord.abc.User]:
    targets: list[discord.abc.User] = []
    seen: set[int] = set()

    for member in message.mentions:
        if member.id in seen:
            continue
        targets.append(member)
        seen.add(member.id)

    reference = message.reference
    resolved = getattr(reference, "resolved", None)
    cached_message = getattr(reference, "cached_message", None)
    reply_message = resolved if isinstance(resolved, discord.Message) else cached_message
    reply_author = getattr(reply_message, "author", None)
    if reply_author is not None and getattr(reply_author, "id", None) not in seen:
        targets.append(reply_author)

    return targets


class EventsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.webhook_id is not None:
            return

        if await is_command_message(self.bot, message):
            return

        utility_service = getattr(self.bot, "utility_service", None)
        if utility_service is not None:
            await utility_service.clear_brb_on_activity(message.author.id)

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

        away_targets = _collect_away_targets(message)
        active_afk_mentions = [
            (target, ge.afk_records.get(target.id))
            for target in away_targets
            if target.id != message.author.id and ge.is_active_afk_record(ge.afk_records.get(target.id))
        ]
        notice_lines = [ge.build_afk_brief_line(user, record) for user, record in active_afk_mentions[:5]]
        if utility_service is not None and len(notice_lines) < 5:
            remaining = 5 - len(notice_lines)
            notice_lines.extend(
                utility_service.build_brb_notice_lines_for_targets(
                    channel_id=message.channel.id,
                    author_id=message.author.id,
                    targets=away_targets,
                )[:remaining]
            )
        if notice_lines:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Away Notice",
                        "\n".join(notice_lines),
                        tone="info",
                        footer="Babblebox AFK/BRB | Mentions are muted. Try /watch for private mention alerts.",
                    ),
                    delete_after=12.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

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
