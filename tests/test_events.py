import types
import unittest
import asyncio
from unittest.mock import AsyncMock, Mock, patch

from babblebox import game_engine as ge
from babblebox.cogs.events import EventsCog
from babblebox.shield_service import ShieldDecision
from babblebox.utility_helpers import serialize_datetime


class FakeAuthor:
    def __init__(self, user_id: int = 1):
        self.id = user_id
        self.bot = False
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"


class FakeTarget(FakeAuthor):
    pass


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
            collect_afk_notice_targets=lambda **kwargs: [],
            handle_watch_message=AsyncMock(),
            handle_return_watch_message=AsyncMock(),
        )
        shield_service = types.SimpleNamespace(
            handle_message=AsyncMock(return_value=ShieldDecision(matched=True, action="log", pack="promo", reasons=()))
        )
        confessions_service = types.SimpleNamespace(handle_member_response_message=AsyncMock())
        bot = types.SimpleNamespace(
            utility_service=utility_service,
            shield_service=shield_service,
            confessions_service=confessions_service,
            get_cog=lambda name: None,
        )
        cog = EventsCog(bot)
        message = FakeMessage()

        with patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)):
            await cog.on_message(message)

        utility_service.handle_watch_message.assert_not_awaited()
        utility_service.handle_return_watch_message.assert_not_awaited()
        confessions_service.handle_member_response_message.assert_not_awaited()

    async def test_single_afk_notice_attaches_return_ping_button(self):
        target = FakeTarget(5)
        record = {
            "created_at": serialize_datetime(ge.now_utc()),
            "set_at": serialize_datetime(ge.now_utc()),
            "ends_at": None,
            "reason": "Stepped away",
        }
        utility_service = types.SimpleNamespace(
            clear_afk_on_activity=AsyncMock(return_value=None),
            collect_afk_notice_targets=lambda **kwargs: [(target, record)],
            handle_watch_message=AsyncMock(),
            handle_return_watch_message=AsyncMock(),
        )
        sentinel_view = object()
        utilities_cog = types.SimpleNamespace(build_afk_return_watch_view=lambda **kwargs: sentinel_view)
        bot = types.SimpleNamespace(
            utility_service=utility_service,
            shield_service=None,
            get_cog=lambda name: utilities_cog if name == "UtilityCog" else None,
        )
        cog = EventsCog(bot)
        message = FakeMessage()
        message.mentions = [target]

        with patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)):
            await cog.on_message(message)

        self.assertEqual(len(message.channel.sent), 1)
        self.assertIs(message.channel.sent[0][1]["view"], sentinel_view)

    async def test_multi_target_afk_notice_stays_unambiguous(self):
        first = FakeTarget(5)
        second = FakeTarget(6)
        record = {
            "created_at": serialize_datetime(ge.now_utc()),
            "set_at": serialize_datetime(ge.now_utc()),
            "ends_at": None,
            "reason": "Away",
        }
        utility_service = types.SimpleNamespace(
            clear_afk_on_activity=AsyncMock(return_value=None),
            collect_afk_notice_targets=lambda **kwargs: [(first, record), (second, record)],
            handle_watch_message=AsyncMock(),
            handle_return_watch_message=AsyncMock(),
        )
        utilities_cog = types.SimpleNamespace(build_afk_return_watch_view=lambda **kwargs: object())
        bot = types.SimpleNamespace(
            utility_service=utility_service,
            shield_service=None,
            get_cog=lambda name: utilities_cog if name == "UtilityCog" else None,
        )
        cog = EventsCog(bot)
        message = FakeMessage()
        message.mentions = [first, second]

        with patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)):
            await cog.on_message(message)

        self.assertEqual(len(message.channel.sent), 1)
        self.assertIsNone(message.channel.sent[0][1]["view"])

    async def test_party_game_channel_routes_before_question_drops(self):
        utility_service = types.SimpleNamespace(
            clear_afk_on_activity=AsyncMock(return_value=None),
            collect_afk_notice_targets=lambda **kwargs: [],
            handle_watch_message=AsyncMock(),
            handle_return_watch_message=AsyncMock(),
        )
        question_drops_service = types.SimpleNamespace(
            observe_message_activity=Mock(),
            retire_drop_for_party_game=AsyncMock(),
            handle_message=AsyncMock(return_value=False),
        )
        bot = types.SimpleNamespace(
            utility_service=utility_service,
            shield_service=None,
            question_drops_service=question_drops_service,
            get_cog=lambda name: None,
        )
        cog = EventsCog(bot)
        message = FakeMessage()
        saved_games = ge.games
        ge.games = {
            message.guild.id: {
                "closing": False,
                "active": True,
                "game_type": "pattern_hunt",
                "channel": message.channel,
                "lock": asyncio.Lock(),
            }
        }
        try:
            with (
                patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)),
                patch("babblebox.pattern_hunt_game.handle_pattern_hunt_message_locked", new=AsyncMock(return_value=True)) as handle_hunt,
            ):
                await cog.on_message(message)
        finally:
            ge.games = saved_games

        question_drops_service.observe_message_activity.assert_called_once_with(message)
        question_drops_service.retire_drop_for_party_game.assert_awaited_once_with(message.guild.id, message.channel.id)
        handle_hunt.assert_awaited_once()
        question_drops_service.handle_message.assert_not_awaited()

    async def test_raw_delete_notifies_confessions_service_before_other_handlers(self):
        confessions_service = types.SimpleNamespace(handle_raw_message_delete=AsyncMock())
        question_drops_service = types.SimpleNamespace(handle_raw_message_delete=AsyncMock())
        bot = types.SimpleNamespace(confessions_service=confessions_service, question_drops_service=question_drops_service)
        cog = EventsCog(bot)
        payload = types.SimpleNamespace(guild_id=10, message_id=99)

        await cog.on_raw_message_delete(payload)

        confessions_service.handle_raw_message_delete.assert_awaited_once_with(payload)
        question_drops_service.handle_raw_message_delete.assert_awaited_once_with(payload)

    async def test_guild_messages_forward_to_confession_response_detection(self):
        utility_service = types.SimpleNamespace(
            clear_afk_on_activity=AsyncMock(return_value=None),
            collect_afk_notice_targets=lambda **kwargs: [],
            handle_watch_message=AsyncMock(),
            handle_return_watch_message=AsyncMock(),
        )
        confessions_service = types.SimpleNamespace(handle_member_response_message=AsyncMock())
        question_drops_service = types.SimpleNamespace(observe_message_activity=Mock(), handle_message=AsyncMock(return_value=False))
        bot = types.SimpleNamespace(
            utility_service=utility_service,
            shield_service=None,
            confessions_service=confessions_service,
            question_drops_service=question_drops_service,
            get_cog=lambda name: None,
        )
        cog = EventsCog(bot)
        message = FakeMessage()

        with patch("babblebox.cogs.events.is_command_message", new=AsyncMock(return_value=False)):
            await cog.on_message(message)

        confessions_service.handle_member_response_message.assert_awaited_once_with(message)
