import asyncio
import types
import unittest
from unittest.mock import AsyncMock, patch

from babblebox import game_engine as ge
from babblebox.cogs.gameplay import GameplayCog
from babblebox.cogs.identity import IdentityCog
from babblebox.cogs.meta import MetaCog
from babblebox.cogs.shield import ShieldCog
from babblebox.cogs.utilities import UtilityCog
from babblebox.profile_service import ProfileService
from babblebox.profile_store import ProfileStore


class FakeMessage:
    pass


class FakeResponse:
    def __init__(self):
        self._done = False

    def is_done(self):
        return self._done


class FakeInteraction:
    def __init__(self, *, expired: bool = False):
        self.response = FakeResponse()
        self._expired = expired
        self.user = FakeAuthor()

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
    def __init__(self, guild_id: int = 10):
        self.id = guild_id
        self.name = "Guild"


class FakeChannel:
    def __init__(self, channel_id: int = 20):
        self.id = channel_id
        self.name = "general"
        self.mention = "#general"


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

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
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
