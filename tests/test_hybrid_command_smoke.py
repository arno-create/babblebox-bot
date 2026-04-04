import asyncio
import types
import unittest
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.cogs.admin import AdminCog
from babblebox.cogs.confessions import ConfessionsCog
from babblebox.cogs.gameplay import GameplayCog
from babblebox.cogs.identity import IdentityCog
from babblebox.cogs.meta import HELP_PAGES, MetaCog
from babblebox.cogs.question_drops import QuestionDropsCog
from babblebox.cogs.shield import ShieldCog
from babblebox.cogs.utilities import AfkReturnWatchDurationSelect, UtilityCog
from babblebox.profile_service import ProfileService
from babblebox.profile_store import ProfileStore


class FakeMessage:
    pass


class FakeResponse:
    def __init__(self):
        self._done = False
        self.send_calls = []
        self.edit_calls = []
        self.modal_calls = []

    def is_done(self):
        return self._done

    async def send_message(self, *args, **kwargs):
        self._done = True
        self.send_calls.append((args, kwargs))

    async def edit_message(self, *args, **kwargs):
        self._done = True
        self.edit_calls.append((args, kwargs))

    async def send_modal(self, modal):
        self._done = True
        self.modal_calls.append(modal)


class FakeInteraction:
    def __init__(self, *, expired: bool = False, user=None, guild=None, client=None):
        self.response = FakeResponse()
        self._expired = expired
        self.user = user or FakeAuthor()
        self.guild = guild
        self.client = client or types.SimpleNamespace(get_guild=lambda guild_id: guild)

    def is_expired(self):
        return self._expired


class FakeGuildPermissions:
    administrator = False
    manage_guild = False


class FakeAuthor:
    def __init__(self, user_id: int = 1, *, manage_guild: bool = False):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions()
        self.guild_permissions.manage_guild = manage_guild


class FakeGuild:
    def __init__(self, guild_id: int = 10, *, members=None):
        self.id = guild_id
        self.name = "Guild"
        self._members = {member.id: member for member in (members or [])}

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class FakeChannel:
    def __init__(self, channel_id: int = 20, *, allowed_user_ids=None):
        self.id = channel_id
        self.name = "general"
        self.mention = "#general"
        self._allowed_user_ids = set(allowed_user_ids or set())

    def permissions_for(self, member):
        allowed = not self._allowed_user_ids or getattr(member, "id", None) in self._allowed_user_ids
        return types.SimpleNamespace(view_channel=allowed, read_message_history=allowed)


class ShieldPermissionSnapshot:
    def __init__(self, **overrides):
        defaults = {
            "view_channel": True,
            "send_messages": True,
            "embed_links": True,
            "manage_messages": True,
            "moderate_members": True,
        }
        defaults.update(overrides)
        for name, value in defaults.items():
            setattr(self, name, value)


class ShieldRole:
    def __init__(self, *, position: int):
        self.position = position

    def __ge__(self, other):
        return self.position >= getattr(other, "position", 0)


class ShieldAwareChannel(FakeChannel):
    def __init__(self, channel_id: int, *, name: str = "general", permissions: Optional[ShieldPermissionSnapshot] = None):
        super().__init__(channel_id)
        self.name = name
        self.mention = f"<#{channel_id}>"
        self._permissions = permissions or ShieldPermissionSnapshot()

    def permissions_for(self, member):
        return self._permissions


class ShieldAwareGuild(FakeGuild):
    def __init__(self, guild_id: int = 10, *, channels=None):
        super().__init__(guild_id)
        self.me = types.SimpleNamespace(id=999, top_role=ShieldRole(position=50))
        self._channels = {channel.id: channel for channel in (channels or [])}

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return None


class ShieldAwareBot:
    def __init__(self, guild: ShieldAwareGuild):
        self.loop = asyncio.get_running_loop()
        self.user = types.SimpleNamespace(id=999)
        self._guild = guild

    def get_guild(self, guild_id: int):
        if guild_id == self._guild.id:
            return self._guild
        return None

    def get_channel(self, channel_id: int):
        return self._guild.get_channel(channel_id)


class FakeContext:
    def __init__(self, *, interaction=None, author=None, guild=None, channel=None, message=None):
        self.interaction = interaction
        self.author = author or FakeAuthor()
        self.guild = guild
        self.channel = channel
        self.message = message
        self.send_calls = []
        self.defer_calls = []

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage()

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        if self.interaction is not None:
            self.interaction.response._done = True


class FakeLobbyView:
    def __init__(self, guild_id):
        self.guild_id = guild_id
        self.message = None


class HybridCommandSmokeTests(unittest.IsolatedAsyncioTestCase):
    def test_help_pages_reflect_hardened_only16_and_pattern_hunt_copy(self):
        party_page = next(page for page in HELP_PAGES if page["title"] == "Party Games")
        self.assertIn("ask one clean number question, then wait for the first clear answer", party_page["body"])
        self.assertIn("Strict = reply to the armed question only", party_page["body"])
        self.assertIn("private guesses with `/hunt guess`", party_page["body"])
        self.assertIn("Coders need server DMs open before start", party_page["body"])
        self.assertIn("digits `0-9` only", party_page["body"])

    def test_help_pages_reflect_question_drop_option_copy(self):
        question_drops_page = next(page for page in HELP_PAGES if page["title"] == "Question Drops")
        self.assertIn("/drops status", question_drops_page["body"])
        self.assertIn("/drops roles status", question_drops_page["body"])
        self.assertNotIn("/drops panel", question_drops_page["body"])
        self.assertIn("/dropsadmin mastery category", question_drops_page["body"])
        self.assertNotIn("/drops mastery category", question_drops_page["body"])
        self.assertIn("template_action", question_drops_page["body"])
        self.assertIn("{user.mention}", question_drops_page["body"])
        self.assertIn("{category.name}", question_drops_page["body"])
        self.assertNotIn("category-template", question_drops_page["body"])
        self.assertIn("scholar ladder", question_drops_page["body"])
        daily_page = next(page for page in HELP_PAGES if page["title"] == "Daily Arcade")
        self.assertIn("Question Drops stay separate as the guild knowledge lane", daily_page["body"])

    def test_question_drops_slash_tree_splits_public_and_admin_surfaces(self):
        cog = QuestionDropsCog(types.SimpleNamespace(loop=None))
        public_slash_names = {command.name for command in cog.drops_group.app_command.commands}
        prefix_alias_names = {command.name for command in cog.drops_group.commands}
        admin_slash_names = {command.name for command in cog.dropsadmin_group.app_command.commands}
        prefix_mastery_names = {command.name for command in cog.drops_mastery_group.commands}
        admin_slash_mastery_names = {command.name for command in cog.dropsadmin_mastery_group.app_command.commands}

        self.assertEqual(public_slash_names, {"leaderboard", "roles", "stats", "status"})
        self.assertTrue({"config", "channels", "categories", "digest", "mastery"}.issubset(prefix_alias_names))
        self.assertEqual(admin_slash_names, {"categories", "channels", "config", "digest", "mastery"})
        self.assertEqual(prefix_mastery_names, {"category", "recalc", "scholar"})
        self.assertEqual(admin_slash_mastery_names, {"category", "recalc", "scholar"})

    def test_admin_only_roots_emit_hidden_guild_only_metadata(self):
        bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
        expected = {
            "admin": AdminCog.admin_group.app_command,
            "shield": ShieldCog.shield_group.app_command,
            "confessions": ConfessionsCog.confessions_group.app_command,
            "dropsadmin": QuestionDropsCog.dropsadmin_group.app_command,
        }

        for name, command in expected.items():
            with self.subTest(command=name):
                payload = command.to_dict(bot.tree)

                self.assertEqual(payload["default_member_permissions"], 32)
                self.assertEqual(payload["contexts"], [0])
                self.assertEqual(payload["integration_types"], [0])
                self.assertFalse(payload["dm_permission"])
                self.assertTrue(command.guild_only)
                self.assertTrue(command.allowed_contexts.guild)
                self.assertFalse(command.allowed_contexts.dm_channel)
                self.assertFalse(command.allowed_contexts.private_channel)
                self.assertTrue(command.allowed_installs.guild)
                self.assertFalse(command.allowed_installs.user)

    def test_public_member_roots_remain_visible(self):
        drops_names = {command.name for command in QuestionDropsCog.drops_group.app_command.commands}
        confess_names = {command.name for command in ConfessionsCog.confess_group.app_command.commands}

        self.assertEqual(drops_names, {"leaderboard", "roles", "stats", "status"})
        self.assertEqual(confess_names, {"about", "appeal", "manage", "report"})

    def test_only16_lobby_copy_stays_aligned_with_manual(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            55: {
                "host": host,
                "players": [host, FakeAuthor(2)],
                "game_type": "only16",
                "only16_mode": "smart",
            }
        }
        try:
            embed = ge.get_lobby_embed(55)
        finally:
            ge.games = saved_games

        mode_field = next(field.value for field in embed.fields if field.name == "Only 16 Mode")
        self.assertIn("Strict = reply to the armed question only. Best for first-time rooms.", mode_field)
        self.assertIn("Smart = also counts one clean standalone answer like `16!`.", mode_field)
        self.assertIn("Start with Strict, then switch to Smart if the room wants extra chaos.", mode_field)

    def test_pattern_hunt_lobby_copy_surfaces_dm_requirement_and_private_guess_flow(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            77: {
                "host": host,
                "players": [host, FakeAuthor(2), FakeAuthor(3)],
                "game_type": "pattern_hunt",
            }
        }
        try:
            embed = ge.get_lobby_embed(77)
        finally:
            ge.games = saved_games

        setup_field = next(field.value for field in embed.fields if field.name == "Pattern Hunt Setup")
        self.assertIn("Coders need server DMs open before the room starts.", setup_field)
        self.assertIn("private rule theories stay in `/hunt guess`", setup_field)

    async def test_ping_command_responds_through_context_send(self):
        cog = MetaCog(object())
        ctx = FakeContext(interaction=FakeInteraction())

        await MetaCog.ping_command.callback(cog, ctx)

        self.assertEqual(ctx.defer_calls, [])
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_play_command_defers_before_sending_lobby(self):
        saved_games = ge.games
        ge.games = {}
        try:
            cog = GameplayCog(object())
            ctx = FakeContext(
                interaction=FakeInteraction(),
                author=FakeAuthor(),
                guild=FakeGuild(),
                channel=FakeChannel(),
            )

            with patch("babblebox.cogs.gameplay.require_channel_permissions", new=AsyncMock(return_value=True)), patch.object(
                ge,
                "create_game_state",
                return_value={"host": ctx.author, "channel": ctx.channel, "views": []},
            ), patch.object(ge, "LobbyView", FakeLobbyView), patch.object(ge, "get_lobby_embed", return_value=object()), patch.object(
                ge,
                "register_view",
            ) as register_view, patch.object(ge, "cleanup_game", new=AsyncMock()):
                await GameplayCog.play_command.callback(cog, ctx)

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            register_view.assert_called_once()
        finally:
            ge.games = saved_games

    async def test_play_command_blocks_same_channel_when_question_drop_is_live(self):
        saved_games = ge.games
        ge.games = {}
        try:
            bot = types.SimpleNamespace(
                question_drops_service=types.SimpleNamespace(storage_ready=True, has_live_drop=lambda guild_id, channel_id: True)
            )
            cog = GameplayCog(bot)
            ctx = FakeContext(
                interaction=FakeInteraction(),
                author=FakeAuthor(),
                guild=FakeGuild(),
                channel=FakeChannel(),
            )

            with patch("babblebox.cogs.gameplay.require_channel_permissions", new=AsyncMock(return_value=True)):
                await GameplayCog.play_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Question Drop", ctx.send_calls[0]["embed"].description)
        finally:
            ge.games = saved_games

    async def test_watch_mentions_storage_unavailable_still_responds(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = False
            cog.service.storage_error = "db down"
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await UtilityCog.watch_mentions_command.callback(cog, ctx, state="on", scope="server")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_watch_user_rejects_self_watch_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            author = FakeAuthor(user_id=7)
            guild = FakeGuild(members=[author])
            ctx = FakeContext(interaction=FakeInteraction(user=author, guild=guild), guild=guild, channel=FakeChannel(), author=author)

            await UtilityCog.watch_user_command.callback(cog, ctx, author, "6h")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("someone else", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_watch_user_rejects_bot_target_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            author = FakeAuthor(user_id=7)
            bot_target = types.SimpleNamespace(id=8, bot=True, display_name="Reminder Bot", mention="<@8>")
            guild = FakeGuild(members=[author])
            ctx = FakeContext(interaction=FakeInteraction(user=author, guild=guild), guild=guild, channel=FakeChannel(), author=author)

            await UtilityCog.watch_user_command.callback(cog, ctx, bot_target, "6h")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("bots", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_afk_return_duration_select_creates_user_watch(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            watcher = FakeAuthor(user_id=11)
            target = FakeAuthor(user_id=12)
            target.bot = False
            guild = FakeGuild(10, members=[watcher, target])
            interaction = FakeInteraction(
                user=watcher,
                guild=guild,
                client=types.SimpleNamespace(get_guild=lambda guild_id: guild),
            )
            select = AfkReturnWatchDurationSelect(cog, guild_id=guild.id, target_user_id=target.id, target_name=target.display_name)
            select._values = ["6h"]

            await select.callback(interaction)

            self.assertEqual(len(interaction.response.edit_calls), 1)
            self.assertEqual(len(cog.service.store.state["return_watches"]), 1)
            record = next(iter(cog.service.store.state["return_watches"].values()))
            self.assertEqual(record["watcher_user_id"], watcher.id)
            self.assertEqual(record["target_type"], "user")
            self.assertEqual(record["target_id"], target.id)
        finally:
            await cog.service.close()

    async def test_remind_set_storage_unavailable_still_responds(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = False
            cog.service.storage_error = "db down"
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await UtilityCog.remind_set_command.callback(cog, ctx, "10m", "dm", text="take a break")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_renders_with_memory_profile_service(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx)

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_private_stays_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx, mode=None, visibility="private")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_storage_unavailable_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        try:
            cog.service.storage_ready = False
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx, mode=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_open_state_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="emoji", guess=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_open_state_private_is_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="emoji", guess=None, visibility="private")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_success_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzles"]["shuffle"].answer
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess=answer, visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_prefix_guess_still_defaults_to_shuffle(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzles"]["shuffle"].answer
            ctx = FakeContext(guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode=answer, guess=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_retry_warning_stays_private_even_when_public_requested(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong", visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_final_failed_result_can_be_public_without_spoiler(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzle"].answer.upper()
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong one", visibility="public")
            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong two", visibility="public")
            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong three", visibility="public")

            self.assertEqual(len(ctx.send_calls), 3)
            self.assertFalse(ctx.send_calls[2]["ephemeral"])
            self.assertNotIn(answer, ctx.send_calls[2]["embed"].description)
        finally:
            await cog.service.close()

    async def test_daily_stats_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_stats_command.callback(cog, ctx, user=None, visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_stats_private_is_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_stats_command.callback(cog, ctx, user=None, visibility="private")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_public_panel_cooldown_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx_one = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
            ctx_two = FakeContext(interaction=FakeInteraction(), guild=ctx_one.guild, channel=ctx_one.channel, author=ctx_one.author)

            await IdentityCog.daily_group.callback(cog, ctx_one, mode=None, visibility="public")
            await IdentityCog.daily_group.callback(cog, ctx_two, mode=None, visibility="public")

            self.assertFalse(ctx_one.send_calls[0]["ephemeral"])
            self.assertTrue(ctx_two.send_calls[0]["ephemeral"])
            self.assertIn("cooldown", ctx_two.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_daily_share_invalid_public_request_does_not_consume_public_cooldown(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
            second_ctx = FakeContext(interaction=FakeInteraction(), guild=first_ctx.guild, channel=first_ctx.channel, author=first_ctx.author)
            status = await cog.service.get_daily_status(1)
            await cog.service.submit_daily_guess(1, status["puzzles"]["shuffle"].answer, mode="shuffle")

            await IdentityCog.daily_share_command.callback(cog, first_ctx, mode="emoji", visibility="public")
            await IdentityCog.daily_share_command.callback(cog, second_ctx, mode="shuffle", visibility="public")

            self.assertTrue(first_ctx.send_calls[0]["ephemeral"])
            self.assertFalse(second_ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_profile_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.profile_command.callback(cog, ctx, user=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_share_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            await cog.service.submit_daily_guess(1, status["puzzles"]["shuffle"].answer, mode="shuffle")
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_share_command.callback(cog, ctx, mode="shuffle", visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_buddy_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.buddy_group.callback(cog, ctx, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_help_public_uses_view_and_public_visibility(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, ctx, visibility="public")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertFalse(ctx.send_calls[0]["ephemeral"])
        self.assertIsNotNone(ctx.send_calls[0]["view"])

    async def test_shield_status_is_private_for_admins(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_status_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_shield_status_denies_members_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await ShieldCog.shield_status_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_shield_ai_command_reports_guild_restriction_cleanly(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_ai_command.callback(cog, ctx, enabled=True, min_confidence=None, privacy=None, promo=None, scam=None)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("not available", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_moment_public_card_uses_public_visibility(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            source_author = types.SimpleNamespace(
                id=5,
                display_name="Mira",
                color=discord.Color.blue(),
                display_avatar=types.SimpleNamespace(url="https://cdn.example/avatar.png"),
            )
            source_message = types.SimpleNamespace(
                content="That one line deserved a card.",
                attachments=[],
                author=source_author,
                channel=types.SimpleNamespace(mention="#general"),
                guild=types.SimpleNamespace(name="Guild"),
                created_at=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
                jump_url="https://discord.com/channels/10/20/30",
            )
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            with patch.object(cog, "_resolve_moment_source", new=AsyncMock(return_value=(source_message, None))):
                await UtilityCog.moment_create_command.callback(cog, ctx, message_link=None, title="Best Line", visibility="public")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertFalse(ctx.defer_calls[0]["ephemeral"])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_shield_panel_overview_reflects_legacy_pack_state(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            guild_id = 10
            cog.service.store.state["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "packs": {
                    "privacy": {"enabled": True, "action": "delete_log", "sensitivity": "high"},
                    "promo": {"tracking": True},
                },
                "scam_enabled": True,
            }

            embed = cog.build_panel_embed(guild_id, "overview")
            protection_field = next(field for field in embed.fields if field.name == "Protection Packs")
            link_safety_field = next(field for field in embed.fields if field.name == "Link Safety")

            self.assertIn("**Privacy Leak**", protection_field.value)
            self.assertIn("Enabled: Yes | Sensitivity: High", protection_field.value)
            self.assertIn("Low / Medium / High: `log` / `delete_log` / `delete_log`", protection_field.value)
            self.assertIn("**Promo / Invite**", protection_field.value)
            self.assertIn("Enabled: Yes | Sensitivity: Normal", protection_field.value)
            self.assertIn("Low / Medium / High: `log` / `log` / `log`", protection_field.value)
            self.assertIn("**Scam / Malicious Links**", link_safety_field.value)
            self.assertIn("**Adult / 18+ Links**", link_safety_field.value)
        finally:
            await cog.service.close()

    async def test_shield_panel_warns_when_operability_is_missing(self):
        current_channel = ShieldAwareChannel(20, permissions=ShieldPermissionSnapshot(manage_messages=False, moderate_members=False))
        log_channel = ShieldAwareChannel(
            30,
            name="mod-logs",
            permissions=ShieldPermissionSnapshot(view_channel=False, send_messages=False, embed_links=False),
        )
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
                "scam_enabled": True,
                "scam_action": "timeout_log",
            }

            embed = cog.build_panel_embed(guild.id, "overview", channel_id=current_channel.id)
            operability = next(field for field in embed.fields if field.name == "Operability")

            self.assertIn("Manage Messages", operability.value)
            self.assertIn("Moderate Members", operability.value)
            self.assertIn("View Channel", operability.value)
            self.assertIn("Send Messages", operability.value)
            self.assertIn("Embed Links", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_logs_command_surfaces_log_channel_permission_warning(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs", permissions=ShieldPermissionSnapshot(send_messages=False))
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_logs_command.callback(cog, ctx, channel=log_channel, role=None, clear_channel=False, clear_role=False)

            self.assertEqual(len(ctx.send_calls), 1)
            operability = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Operability")
            self.assertIn("Send Messages", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_stays_quiet_when_permissions_are_available(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs")
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="Email me at friend@example.com")

            self.assertEqual(len(ctx.send_calls), 1)
            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertNotIn("Operability", field_names)
        finally:
            await cog.service.close()

    async def test_shield_rules_command_supports_adult_pack(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_rules_command.callback(
                cog,
                ctx,
                module=None,
                pack="adult",
                enabled=True,
                action="delete_log",
                low_action=None,
                medium_action=None,
                high_action=None,
                sensitivity="normal",
                escalation_threshold=None,
                escalation_window_minutes=None,
                timeout_minutes=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Adult / 18+ Links", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_shield_test_command_includes_link_safety_assessments(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="Free nitro https://dlscord-gift.com/claim")

            self.assertEqual(len(ctx.send_calls), 1)
            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertIn("Link Safety", field_names)
            link_safety_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Safety")
            self.assertIn("dlscord-gift.com", link_safety_field.value)
            self.assertIn("matched local intel", link_safety_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_marks_lookup_candidates_as_no_action(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(
                cog,
                ctx,
                text="Visit https://wallet-bonus-drop.click/account?redirect=%2Flogin%2Fauth%2Ftoken%2Fseed to claim access.",
            )

            link_safety_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Safety")
            self.assertIn("lookup candidate only, no action", link_safety_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_surfaces_allow_phrase_bypass(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"]["10"] = {
                "guild_id": 10,
                "allow_phrases": ["scam example"],
            }
            ctx = FakeContext(guild=FakeGuild(10), channel=FakeChannel(), author=FakeAuthor(manage_guild=True))

            await cog.shield_test_command.callback(cog, ctx, text="scam example https://dlscord-gift.com/claim")

            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertIn("Bypass", field_names)
            bypass_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Bypass")
            self.assertIn("allow phrase", bypass_field.value.lower())
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_override_ignores_guild_invocation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=1266444952779620413, manage_guild=True),
            )

            await ShieldCog.shield_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(ctx.send_calls, [])
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_override_rejects_unauthorized_dm(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=None,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=777),
            )

            await ShieldCog.shield_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is unavailable.")
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_override_status_and_toggle_work_in_dm_for_owner(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await ShieldCog.shield_ai_global_override_command.callback(cog, ctx, "status")
            await ShieldCog.shield_ai_global_override_command.callback(cog, ctx, "on")
            await ShieldCog.shield_ai_global_override_command.callback(cog, ctx, "off")

            self.assertEqual(len(ctx.send_calls), 3)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Shield AI Override")
            self.assertIn("Private maintainer status", ctx.send_calls[0]["embed"].description)
            self.assertIn("now on", ctx.send_calls[1]["embed"].description.lower())
            self.assertIn("now off", ctx.send_calls[2]["embed"].description.lower())
            self.assertFalse(cog.service.get_meta()["global_ai_override_enabled"])
        finally:
            await cog.service.close()

    async def test_drops_mastery_group_denies_non_admins_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await QuestionDropsCog.drops_mastery_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_dropsadmin_group_denies_non_admins_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await QuestionDropsCog.dropsadmin_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_drops_roles_group_rejects_dm_invocation_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(interaction=FakeInteraction(), guild=None, channel=FakeChannel(), author=FakeAuthor())

            await QuestionDropsCog.drops_roles_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("only works inside a server", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_drops_roles_group_sends_private_member_status(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_member_roles_status = AsyncMock(return_value={"preference": {"role_grants_enabled": True}, "held_records": []})
            cog.service.build_member_roles_status_embed = lambda guild, member, payload: discord.Embed(title="Question Drops Roles")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=7)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=7),
            )

            await QuestionDropsCog.drops_roles_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops Roles")
            cog.service.get_member_roles_status.assert_awaited_once()
        finally:
            await cog.service.close()

    async def test_drops_roles_preference_command_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.update_member_role_preference = AsyncMock(return_value={"mode": "stop", "before": {}, "after": {}})
            cog.service.build_member_role_preference_embed = lambda payload: discord.Embed(title="Question Drops Role Grants Off")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=8)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=8),
            )

            await QuestionDropsCog.drops_roles_preference_command.callback(
                cog,
                ctx,
                mode="stop",
                remove_current_roles=False,
                restore_current_roles=False,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops Role Grants Off")
            cog.service.update_member_role_preference.assert_awaited_once()
        finally:
            await cog.service.close()

    async def test_drops_mastery_category_template_action_edit_opens_modal_for_slash(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_category_mastery_announcement_status = AsyncMock(
                return_value={
                    "status": "ok",
                    "announcement_template": "Hello {user.mention}",
                    "placeholder_tokens": ("{user.mention}", "{user.name}", "{user.display_name}", "{role.name}", "{tier.label}", "{threshold}", "{category.name}"),
                }
            )
            guild = FakeGuild(10, members=[FakeAuthor(user_id=9, manage_guild=True)])
            interaction = FakeInteraction(guild=guild, user=FakeAuthor(user_id=9, manage_guild=True))
            ctx = FakeContext(
                interaction=interaction,
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=9, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                template_action="edit",
            )

            self.assertEqual(len(interaction.response.modal_calls), 1)
            modal = interaction.response.modal_calls[0]
            self.assertEqual(modal.title, "Edit Science Mastery Announcement")
            self.assertIn("{user.mention}", modal.template_input.placeholder)
            self.assertIn("{category.name}", modal.template_input.placeholder)
            self.assertNotIn("Plain text only", modal.template_input.placeholder)
            self.assertEqual(ctx.send_calls, [])
        finally:
            await cog.service.close()

    async def test_drops_mastery_scholar_template_action_edit_opens_tier_modal_for_slash(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_scholar_announcement_status = AsyncMock(
                return_value={
                    "status": "ok",
                    "announcement_template": "Hello {user.mention}",
                    "placeholder_tokens": ("{user.mention}", "{user.name}", "{user.display_name}", "{role.name}", "{tier.label}", "{threshold}"),
                }
            )
            guild = FakeGuild(10, members=[FakeAuthor(user_id=10, manage_guild=True)])
            interaction = FakeInteraction(guild=guild, user=FakeAuthor(user_id=10, manage_guild=True))
            ctx = FakeContext(
                interaction=interaction,
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=10, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_scholar_command.callback(
                cog,
                ctx,
                tier=2,
                template_action="edit",
            )

            self.assertEqual(len(interaction.response.modal_calls), 1)
            modal = interaction.response.modal_calls[0]
            self.assertEqual(modal.title, "Edit Scholar II Announcement")
            self.assertIn("{user.mention}", modal.template_input.placeholder)
            self.assertIn("{threshold}", modal.template_input.placeholder)
            self.assertNotIn("{category.name}", modal.template_input.placeholder)
        finally:
            await cog.service.close()

    async def test_drops_mastery_scholar_template_edit_requires_slash_for_prefix(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=10, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_scholar_command.callback(
                cog,
                ctx,
                enabled="edit",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("slash form", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_drops_mastery_category_template_clear_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.clear_category_mastery_announcement_template = AsyncMock(return_value=(True, "Science Tier II announcement override cleared."))
            cog.service.get_category_mastery_announcement_status = AsyncMock(
                return_value={"status": "ok", "title": "Science Tier II Announcement"}
            )
            cog.service.build_mastery_announcement_status_embed = lambda payload, note=None: discord.Embed(title="Science Tier II Announcement")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=11, manage_guild=True)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild, user=FakeAuthor(user_id=11, manage_guild=True)),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=11, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                tier=2,
                template_action="clear",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Science Tier II Announcement")
            cog.service.clear_category_mastery_announcement_template.assert_awaited_once_with(guild.id, category="science", tier=2)
        finally:
            await cog.service.close()

    async def test_drops_mastery_template_mode_rejects_mixed_role_fields(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            guild = FakeGuild(10, members=[FakeAuthor(user_id=12, manage_guild=True)])
            role = types.SimpleNamespace(id=222, mention="<@&222>")
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild, user=FakeAuthor(user_id=12, manage_guild=True)),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=12, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                template_action="status",
                role=role,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("template mode only uses", ctx.send_calls[0]["embed"].description.casefold())
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_rejects_guild_invocation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=1266444952779620413, manage_guild=True),
            )

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is only available in DM.")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_rejects_unauthorized_dm(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=None,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=777),
            )

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is unavailable.")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_status_and_toggle_work_in_dm_for_owner(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "rare")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "event_only")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "off")

            self.assertEqual(len(ctx.send_calls), 4)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops AI Override")
            self.assertIn("Private maintainer status", ctx.send_calls[0]["embed"].description)
            self.assertIn("now `rare`", ctx.send_calls[1]["embed"].description.lower())
            self.assertIn("now `event_only`", ctx.send_calls[2]["embed"].description.lower())
            self.assertIn("now `off`", ctx.send_calls[3]["embed"].description.lower())
            self.assertEqual(cog.service.get_meta()["ai_celebration_mode"], "off")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_invalid_mode_in_dm_shows_usage(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "loud")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops AI Override")
            self.assertIn("Use `status`, `off`, `rare`, or `event_only`.", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    def test_hidden_override_command_is_not_in_public_help_pages(self):
        serialized_help = " ".join(page["body"] + " " + page.get("try", "") for page in HELP_PAGES).casefold()

        self.assertNotIn("shieldaiglobal", serialized_help)
        self.assertNotIn("dropscelebaiglobal", serialized_help)
