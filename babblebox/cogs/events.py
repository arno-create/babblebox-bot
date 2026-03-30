from __future__ import annotations

import contextlib

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.command_utils import is_command_message
from babblebox.utility_helpers import build_afk_notice_line


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
        shield_service = getattr(self.bot, "shield_service", None)
        question_drops_service = getattr(self.bot, "question_drops_service", None)
        author_afk = None
        if utility_service is not None:
            author_afk = await utility_service.clear_afk_on_activity(message.author.id)

        if author_afk is not None:
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

        if shield_service is not None:
            shield_decision = await shield_service.handle_message(message)
            if shield_decision is not None and shield_decision.matched:
                return

        away_targets = _collect_away_targets(message)
        afk_notice_targets = []
        if utility_service is not None:
            afk_notice_targets = utility_service.collect_afk_notice_targets(
                channel_id=message.channel.id,
                author_id=message.author.id,
                targets=away_targets,
            )
        notice_lines = [build_afk_notice_line(member, record) for member, record in afk_notice_targets]
        if notice_lines:
            view = None
            utilities_cog = self.bot.get_cog("UtilityCog")
            if len(afk_notice_targets) == 1 and utilities_cog is not None and message.guild is not None:
                target_member, _ = afk_notice_targets[0]
                build_view = getattr(utilities_cog, "build_afk_return_watch_view", None)
                if callable(build_view):
                    view = build_view(guild_id=message.guild.id, target_user_id=target_member.id)
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Away Notice",
                        "\n".join(notice_lines),
                        tone="info",
                        footer="Babblebox AFK | Mentions are muted. Try /watch for private mention alerts.",
                    ),
                    view=view,
                    delete_after=12.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        if utility_service is not None:
            await utility_service.handle_watch_message(message)
            await utility_service.handle_return_watch_message(message)
        if question_drops_service is not None:
            question_drops_service.observe_message_activity(message)
            handled_drop = await question_drops_service.handle_message(message)
            if handled_drop:
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
                and game.get("game_type") in {"only16", "pattern_hunt"}
                and message.channel.id == game["channel"].id
            ):
                async with game["lock"]:
                    game = ge.games.get(guild_id)
                    if not game or game.get("closing") or not game.get("active") or message.channel.id != game["channel"].id:
                        return
                    if game.get("game_type") == "only16":
                        from babblebox.only16_game import handle_only16_message_locked

                        handled = await handle_only16_message_locked(message, guild_id, game)
                        if handled:
                            return
                    if game.get("game_type") == "pattern_hunt":
                        from babblebox.pattern_hunt_game import handle_pattern_hunt_message_locked

                        handled = await handle_pattern_hunt_message_locked(message, guild_id, game)
                        if handled:
                            return
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

        question_drops_service = getattr(self.bot, "question_drops_service", None)
        if question_drops_service is not None:
            await question_drops_service.handle_raw_message_delete(payload)

        game = ge.games.get(payload.guild_id)
        if game and not game.get("closing") and game.get("active") and game.get("game_type") == "only16":
            async with game["lock"]:
                game = ge.games.get(payload.guild_id)
                if game and not game.get("closing") and game.get("active") and game.get("game_type") == "only16":
                    from babblebox.only16_game import handle_only16_message_delete_locked

                    handled = await handle_only16_message_delete_locked(payload.message_id, payload.guild_id, game)
                    if handled:
                        return
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
