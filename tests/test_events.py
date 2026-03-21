import types
import unittest
from unittest.mock import AsyncMock, patch

from babblebox.cogs.events import EventsCog
from babblebox.shield_service import ShieldDecision


class FakeAuthor:
    def __init__(self, user_id: int = 1):
        self.id = user_id
        self.bot = False
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"


class FakeChannel:
    def __init__(self, channel_id: int = 20):
        self.id = channel_id
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))


class FakeGuild:
    def __init__(self, guild_id: int = 10):
        self.id = guild_id


class FakeMessage:
    def __init__(self):
        self.author = FakeAuthor()
        self.webhook_id = None
        self.content = "hello there"
        self.channel = FakeChannel()
        self.guild = FakeGuild()
        self.mentions = []
        self.reference = None


class EventsCogTests(unittest.IsolatedAsyncioTestCase):
    async def test_shield_match_short_circuits_watch_and_game_paths(self):
        utility_service = types.SimpleNamespace(
            clear_afk_on_activity=AsyncMock(return_value=None),
            build_afk_notice_lines_for_targets=lambda **kwargs: [],
            handle_watch_message=AsyncMock(),
        )
        shield_service = types.SimpleNamespace(
            handle_message=AsyncMock(return_value=ShieldDecision(matched=True, action="log", pack="promo", reasons=()))
        )
        bot = types.SimpleNamespace(utility_service=utility_service, shield_service=shield_service)
        cog = EventsCog(bot)
        message = FakeMessage()

        with patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)):
            await cog.on_message(message)

        utility_service.handle_watch_message.assert_not_awaited()
