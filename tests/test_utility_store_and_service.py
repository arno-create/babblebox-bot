import os
import json
import types
import unittest
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from unittest.mock import AsyncMock

import discord

from babblebox import game_engine as ge
from babblebox.cogs.utilities import UtilityCog
from babblebox.shield_service import (
    FEATURE_SURFACE_AFK_REASON,
    FEATURE_SURFACE_AFK_SCHEDULE_REASON,
    FEATURE_SURFACE_REMINDER_CREATE,
    FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY,
    FEATURE_SURFACE_WATCH_KEYWORD,
    ShieldFeatureDecision,
    ShieldFeatureSafetyGateway,
)
from babblebox.utility_helpers import build_afk_reason_text, compute_next_afk_schedule_start, deserialize_datetime, serialize_datetime
from babblebox.utility_service import UtilityService
from babblebox.utility_store import UtilityStateStore, UtilityStorageUnavailable, _PostgresUtilityStore


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id


class DummyTarget(DummyUser):
    def __init__(self, user_id: int, *, bot: bool = False, display_name: Optional[str] = None):
        super().__init__(user_id)
        self.bot = bot
        self.display_name = display_name or f"User {user_id}"


class DummyMember(DummyTarget):
    def __init__(self, user_id: int, *, bot: bool = False, display_name: Optional[str] = None):
        super().__init__(user_id, bot=bot, display_name=display_name)
        self.mention = f"<@{user_id}>"
        self.send = AsyncMock()


class DummyChannel:
    def __init__(self, channel_id: int, *, guild, name: str = "general", visible_user_ids: Optional[set[int]] = None):
        self.id = channel_id
        self.guild = guild
        self.name = name
        self.mention = f"#{name}"
        self._visible_user_ids = set(visible_user_ids or set())
        self.sent = []

    def permissions_for(self, member):
        allowed = getattr(member, "id", None) in self._visible_user_ids
        return types.SimpleNamespace(view_channel=allowed, read_message_history=allowed)

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))


class DummyGuild:
    def __init__(self, guild_id: int, *, name: str = "Guild", members: Optional[list[DummyMember]] = None):
        self.id = guild_id
        self.name = name
        self._members = {member.id: member for member in (members or [])}

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class DummyMessage:
    def __init__(
        self,
        *,
        message_id: int,
        author,
        guild,
        channel,
        content: str,
        created_at,
        message_type=discord.MessageType.default,
        raw_mentions: Optional[list[int]] = None,
    ):
        self.id = message_id
        self.author = author
        self.guild = guild
        self.channel = channel
        self.content = content
        self.attachments = []
        self.created_at = created_at
        self.jump_url = f"https://discord.com/channels/{guild.id}/{channel.id}/{message_id}"
        self.mentions = []
        self.reference = None
        self.type = message_type
        self.raw_mentions = list(raw_mentions or [])


class DummyBot:
    def __init__(self):
        self._users = {}
        self._channels = {}

    def add_user(self, user):
        self._users[user.id] = user

    def get_user(self, user_id: int):
        return self._users.get(user_id)

    async def fetch_user(self, user_id: int):
        return self.get_user(user_id)

    def add_channel(self, channel):
        self._channels[channel.id] = channel

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    async def fetch_channel(self, channel_id: int):
        return self.get_channel(channel_id)


class FeatureGatewaySpy(ShieldFeatureSafetyGateway):
    def __init__(self, *, blocked_surfaces: Optional[dict[str, str]] = None):
        self.blocked_surfaces = dict(blocked_surfaces or {})
        self.evaluations: list[tuple[str, Optional[str]]] = []

    def evaluate(self, surface: str, text: Optional[str], *, attachments=None, channel_id=None) -> ShieldFeatureDecision:
        self.evaluations.append((surface, text))
        if surface in self.blocked_surfaces:
            return ShieldFeatureDecision(
                allowed=False,
                surface=surface,
                reason_code="spy_block",
                user_message=self.blocked_surfaces[surface],
            )
        return ShieldFeatureDecision(
            allowed=True,
            surface=surface,
            reason_code=None,
            user_message=None,
        )


class FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return FakeAcquire(self.connection)


class FakeReloadConnection:
    def __init__(self, *, watch_rows=None, later_rows=None, reminder_rows=None):
        self._watch_rows = watch_rows or []
        self._later_rows = later_rows or []
        self._reminder_rows = reminder_rows or []

    async def fetch(self, query):
        if "FROM utility_watch_configs" in query:
            return self._watch_rows
        if "FROM utility_watch_keywords" in query:
            return []
        if "FROM utility_return_watches" in query:
            return []
        if "FROM utility_later_markers" in query:
            return self._later_rows
        if "FROM utility_reminders" in query:
            return self._reminder_rows
        if "FROM utility_afk_settings" in query:
            return []
        if "FROM utility_afk_schedules" in query:
            return []
        if "FROM utility_afk" in query:
            return []
        raise AssertionError(f"Unexpected fetch query: {query}")


class UtilityStoreAndServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = UtilityStateStore(backend="memory")
        await self.store.load()
        self.bot = DummyBot()
        self.service = UtilityService(self.bot, store=self.store)
        self.service.storage_ready = True

    async def test_memory_store_loads_clean_state(self):
        self.assertEqual(self.store.backend_name, "memory")
        self.assertEqual(self.store.state["watch"], {})
        self.assertEqual(self.store.state["return_watches"], {})
        self.assertEqual(self.store.state["afk"], {})
        self.assertEqual(self.store.state["afk_settings"], {})
        self.assertEqual(self.store.state["afk_schedules"], {})

    async def test_postgres_backend_requires_database_url(self):
        with self.assertRaises(UtilityStorageUnavailable):
            UtilityStateStore(backend="postgres", database_url="")

    async def test_database_url_env_precedence_prefers_utility_database_url(self):
        original_backend = os.environ.get("UTILITY_STORAGE_BACKEND")
        original_db = os.environ.get("UTILITY_DATABASE_URL")
        original_supabase = os.environ.get("SUPABASE_DB_URL")
        original_database_url = os.environ.get("DATABASE_URL")
        try:
            os.environ["UTILITY_STORAGE_BACKEND"] = "memory"
            os.environ["UTILITY_DATABASE_URL"] = "postgresql://utility-user:secret@utility.example.com:5432/app"
            os.environ["SUPABASE_DB_URL"] = "postgresql://supabase-user:secret@supabase.example.com:5432/app"
            os.environ["DATABASE_URL"] = "postgresql://database-user:secret@database.example.com:5432/app"
            store = UtilityStateStore()
            self.assertEqual(store.database_url_source, "UTILITY_DATABASE_URL")
            self.assertIn("utility.example.com", store.database_url)
        finally:
            if original_backend is not None:
                os.environ["UTILITY_STORAGE_BACKEND"] = original_backend
            else:
                os.environ.pop("UTILITY_STORAGE_BACKEND", None)
            if original_db is not None:
                os.environ["UTILITY_DATABASE_URL"] = original_db
            else:
                os.environ.pop("UTILITY_DATABASE_URL", None)
            if original_supabase is not None:
                os.environ["SUPABASE_DB_URL"] = original_supabase
            else:
                os.environ.pop("SUPABASE_DB_URL", None)
            if original_database_url is not None:
                os.environ["DATABASE_URL"] = original_database_url
            else:
                os.environ.pop("DATABASE_URL", None)

    async def test_redacted_database_url_hides_password(self):
        store = UtilityStateStore(
            backend="memory",
            database_url="postgresql://utility-user:super-secret@db.example.com:5432/appdb?sslmode=require",
        )
        self.assertEqual(
            store.redacted_database_url(),
            "postgresql://utility-user:***@db.example.com:5432/appdb",
        )

    async def test_service_degrades_cleanly_without_database_url(self):
        original_backend = os.environ.get("UTILITY_STORAGE_BACKEND")
        original_db = os.environ.get("UTILITY_DATABASE_URL")
        original_supabase = os.environ.get("SUPABASE_DB_URL")
        original_database_url = os.environ.get("DATABASE_URL")
        try:
            os.environ.pop("UTILITY_STORAGE_BACKEND", None)
            os.environ.pop("UTILITY_DATABASE_URL", None)
            os.environ.pop("SUPABASE_DB_URL", None)
            os.environ.pop("DATABASE_URL", None)

            service = UtilityService(object())
            started = await service.start()
            self.assertFalse(started)
            self.assertFalse(service.storage_ready)
            self.assertIsNotNone(service.storage_error)
            await service.close()
        finally:
            if original_backend is not None:
                os.environ["UTILITY_STORAGE_BACKEND"] = original_backend
            else:
                os.environ.pop("UTILITY_STORAGE_BACKEND", None)
            if original_db is not None:
                os.environ["UTILITY_DATABASE_URL"] = original_db
            else:
                os.environ.pop("UTILITY_DATABASE_URL", None)
            if original_supabase is not None:
                os.environ["SUPABASE_DB_URL"] = original_supabase
            else:
                os.environ.pop("SUPABASE_DB_URL", None)
            if original_database_url is not None:
                os.environ["DATABASE_URL"] = original_database_url
            else:
                os.environ.pop("DATABASE_URL", None)

    async def test_add_watch_keyword_updates_summary(self):
        ok, _ = await self.service.add_watch_keyword(
            42,
            guild_id=100,
            channel_id=None,
            phrase="hello world",
            scope="server",
            mode="contains",
        )
        self.assertTrue(ok)
        summary = self.service.get_watch_summary(42, guild_id=100)
        self.assertEqual(len(summary["server_keywords"]), 1)
        self.assertEqual(summary["server_keywords"][0]["phrase"], "hello world")

    async def test_feature_gateway_routes_utility_surfaces_with_stable_labels(self):
        gateway = FeatureGatewaySpy()
        self.bot.shield_service = types.SimpleNamespace(feature_gateway=gateway)

        valid, cleaned = self.service.validate_watch_keyword("camera")
        self.assertTrue(valid)
        self.assertEqual(cleaned, "camera")

        ok, reminder = await self.service.create_reminder(
            user=DummyUser(700),
            text="Check the thread.",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )
        self.assertTrue(ok)
        self.assertIsInstance(reminder, dict)

        ok, afk_record = await self.service.set_afk(
            user=DummyUser(701),
            reason="Stepped away",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        self.assertIsInstance(afk_record, dict)

        ok, schedule = await self.service.create_afk_schedule(
            user=DummyUser(702),
            repeat="daily",
            timezone_name="UTC+04:00",
            local_hour=9,
            local_minute=0,
            weekday=None,
            reason="Office hours",
            preset=None,
            duration_seconds=8 * 3600,
        )
        self.assertTrue(ok)
        self.assertIsInstance(schedule, dict)
        self.assertEqual(
            [surface for surface, _ in gateway.evaluations],
            [
                FEATURE_SURFACE_WATCH_KEYWORD,
                FEATURE_SURFACE_REMINDER_CREATE,
                FEATURE_SURFACE_AFK_REASON,
                FEATURE_SURFACE_AFK_SCHEDULE_REASON,
            ],
        )

    async def test_real_feature_gateway_allows_health_context_but_blocks_adult_and_severe_utility_text(self):
        ok, afk_record = await self.service.set_afk(
            user=DummyUser(710),
            reason="Sexual health workshop tomorrow",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        self.assertIsInstance(afk_record, dict)

        ok, adult_error = await self.service.create_reminder(
            user=DummyUser(711),
            text="DM me for nudes",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )
        self.assertFalse(ok)
        self.assertIn("adult", adult_error.lower())

        ok, severe_error = await self.service.set_afk(
            user=DummyUser(712),
            reason="kill yourself",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertFalse(ok)
        self.assertIn("severe", severe_error.lower())

    async def test_watch_keyword_stays_privacy_only_under_real_gateway(self):
        valid, cleaned = self.service.validate_watch_keyword("dm me for nudes")

        self.assertTrue(valid)
        self.assertEqual(cleaned, "dm me for nudes")

    async def test_watch_summary_distinguishes_mentions_replies_and_channel_keywords(self):
        ok, _ = await self.service.set_watch_mentions(42, guild_id=100, channel_id=200, scope="channel", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_watch_replies(42, guild_id=100, channel_id=200, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.add_watch_keyword(
            42,
            guild_id=100,
            channel_id=200,
            phrase="camera",
            scope="channel",
            mode="contains",
        )
        self.assertTrue(ok)
        summary = self.service.get_watch_summary(42, guild_id=100, channel_id=200)
        self.assertTrue(summary["mention_channel_enabled"])
        self.assertTrue(summary["reply_server_enabled"])
        self.assertEqual(len(summary["channel_keywords"]), 1)

    def _watch_embed_field(self, member: DummyMember, name: str) -> str:
        embed = member.send.await_args.kwargs["embed"]
        return next(field.value for field in embed.fields if field.name == name)

    async def test_watch_explicit_mention_alert_stays_distinct_from_replies(self):
        watcher = DummyMember(200, display_name="Mira")
        author = DummyMember(201, display_name="Nina")
        guild = DummyGuild(20, members=[watcher, author])
        channel = DummyChannel(21, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        message = DummyMessage(
            message_id=40,
            author=author,
            guild=guild,
            channel=channel,
            content="hey <@200>",
            created_at=ge.now_utc(),
            raw_mentions=[watcher.id],
        )
        message.mentions = [watcher]

        await self.service.handle_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertEqual(self._watch_embed_field(watcher, "Why"), "Mention")

    async def test_watch_reply_alert_ignores_reply_generated_mention_metadata(self):
        watcher = DummyMember(210, display_name="Mira")
        author = DummyMember(211, display_name="Nina")
        guild = DummyGuild(22, members=[watcher, author])
        channel = DummyChannel(23, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_watch_replies(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        source = DummyMessage(
            message_id=41,
            author=watcher,
            guild=guild,
            channel=channel,
            content="original",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )
        message = DummyMessage(
            message_id=42,
            author=author,
            guild=guild,
            channel=channel,
            content="reply without explicit ping",
            created_at=ge.now_utc(),
            message_type=discord.MessageType.reply,
            raw_mentions=[],
        )
        message.mentions = [watcher]
        message.reference = types.SimpleNamespace(resolved=None, cached_message=source)

        await self.service.handle_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertEqual(self._watch_embed_field(watcher, "Why"), "Reply")

    async def test_watch_combines_reply_and_explicit_mention_for_same_user_once(self):
        watcher = DummyMember(220, display_name="Mira")
        author = DummyMember(221, display_name="Nina")
        guild = DummyGuild(24, members=[watcher, author])
        channel = DummyChannel(25, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_watch_replies(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        source = DummyMessage(
            message_id=43,
            author=watcher,
            guild=guild,
            channel=channel,
            content="original",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )
        message = DummyMessage(
            message_id=44,
            author=author,
            guild=guild,
            channel=channel,
            content="reply and ping <@220>",
            created_at=ge.now_utc(),
            message_type=discord.MessageType.reply,
            raw_mentions=[watcher.id],
        )
        message.mentions = [watcher]
        message.reference = types.SimpleNamespace(resolved=None, cached_message=source)

        await self.service.handle_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertEqual(self._watch_embed_field(watcher, "Why"), "Mention, Reply")

    async def test_watch_keeps_reply_and_explicit_mention_targets_separate(self):
        reply_watcher = DummyMember(230, display_name="ReplyWatcher")
        mention_watcher = DummyMember(231, display_name="MentionWatcher")
        author = DummyMember(232, display_name="Nina")
        guild = DummyGuild(26, members=[reply_watcher, mention_watcher, author])
        channel = DummyChannel(27, guild=guild, visible_user_ids={reply_watcher.id, mention_watcher.id, author.id})
        self.bot.add_user(reply_watcher)
        self.bot.add_user(mention_watcher)
        ok, _ = await self.service.set_watch_replies(reply_watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_watch_mentions(mention_watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        source = DummyMessage(
            message_id=45,
            author=reply_watcher,
            guild=guild,
            channel=channel,
            content="original",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )
        message = DummyMessage(
            message_id=46,
            author=author,
            guild=guild,
            channel=channel,
            content="reply to one person and tag another <@231>",
            created_at=ge.now_utc(),
            message_type=discord.MessageType.reply,
            raw_mentions=[mention_watcher.id],
        )
        message.mentions = [reply_watcher, mention_watcher]
        message.reference = types.SimpleNamespace(resolved=None, cached_message=source)

        await self.service.handle_watch_message(message)

        reply_watcher.send.assert_awaited_once()
        mention_watcher.send.assert_awaited_once()
        self.assertEqual(self._watch_embed_field(reply_watcher, "Why"), "Reply")
        self.assertEqual(self._watch_embed_field(mention_watcher, "Why"), "Mention")

    async def test_watch_reply_metadata_noise_does_not_trigger_false_mention_alert(self):
        watcher = DummyMember(240, display_name="Mira")
        author = DummyMember(241, display_name="Nina")
        guild = DummyGuild(28, members=[watcher, author])
        channel = DummyChannel(29, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        source = DummyMessage(
            message_id=47,
            author=watcher,
            guild=guild,
            channel=channel,
            content="original",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )
        message = DummyMessage(
            message_id=48,
            author=author,
            guild=guild,
            channel=channel,
            content="reply without explicit ping",
            created_at=ge.now_utc(),
            message_type=discord.MessageType.reply,
            raw_mentions=[],
        )
        message.mentions = [watcher]
        message.reference = types.SimpleNamespace(resolved=None, cached_message=source)

        await self.service.handle_watch_message(message)

        watcher.send.assert_not_awaited()

    async def test_watch_keyword_alerts_can_stack_with_reply_alerts(self):
        watcher = DummyMember(250, display_name="Mira")
        author = DummyMember(251, display_name="Nina")
        guild = DummyGuild(30, members=[watcher, author])
        channel = DummyChannel(31, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_replies(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.add_watch_keyword(
            watcher.id,
            guild_id=guild.id,
            channel_id=channel.id,
            phrase="camera",
            scope="server",
            mode="contains",
        )
        self.assertTrue(ok)
        source = DummyMessage(
            message_id=49,
            author=watcher,
            guild=guild,
            channel=channel,
            content="original",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )
        message = DummyMessage(
            message_id=50,
            author=author,
            guild=guild,
            channel=channel,
            content="replying about the camera",
            created_at=ge.now_utc(),
            message_type=discord.MessageType.reply,
            raw_mentions=[],
        )
        message.reference = types.SimpleNamespace(resolved=None, cached_message=source)

        await self.service.handle_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertEqual(self._watch_embed_field(watcher, "Why"), "Keyword, Reply")
        self.assertEqual(self._watch_embed_field(watcher, "Matched Keywords"), "`camera`")

    async def test_watch_dedupes_same_message_for_same_user(self):
        watcher = DummyMember(260, display_name="Mira")
        author = DummyMember(261, display_name="Nina")
        guild = DummyGuild(32, members=[watcher, author])
        channel = DummyChannel(33, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        message = DummyMessage(
            message_id=51,
            author=author,
            guild=guild,
            channel=channel,
            content="hey <@260>",
            created_at=ge.now_utc(),
            raw_mentions=[watcher.id],
        )
        message.mentions = [watcher]

        await self.service.handle_watch_message(message)
        await self.service.handle_watch_message(message)

        watcher.send.assert_awaited_once()

    async def test_watch_dm_cooldown_suppresses_second_message_briefly(self):
        watcher = DummyMember(270, display_name="Mira")
        author = DummyMember(271, display_name="Nina")
        guild = DummyGuild(34, members=[watcher, author])
        channel = DummyChannel(35, guild=guild, visible_user_ids={watcher.id, author.id})
        self.bot.add_user(watcher)
        ok, _ = await self.service.set_watch_mentions(watcher.id, guild_id=guild.id, channel_id=channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        first = DummyMessage(
            message_id=52,
            author=author,
            guild=guild,
            channel=channel,
            content="hey <@270>",
            created_at=ge.now_utc(),
            raw_mentions=[watcher.id],
        )
        second = DummyMessage(
            message_id=53,
            author=author,
            guild=guild,
            channel=channel,
            content="again <@270>",
            created_at=ge.now_utc() + timedelta(seconds=1),
            raw_mentions=[watcher.id],
        )
        first.mentions = [watcher]
        second.mentions = [watcher]

        await self.service.handle_watch_message(first)
        await self.service.handle_watch_message(second)

        watcher.send.assert_awaited_once()

    async def test_watch_filters_and_channel_access_still_block_alerts(self):
        watched_user = DummyMember(280, display_name="Mira")
        blocked_user = DummyMember(281, display_name="Blocked")
        author = DummyMember(282, display_name="Nina")
        guild = DummyGuild(36, members=[watched_user, blocked_user, author])
        visible_channel = DummyChannel(37, guild=guild, visible_user_ids={watched_user.id, blocked_user.id, author.id})
        hidden_channel = DummyChannel(38, guild=guild, visible_user_ids={author.id})
        self.bot.add_user(watched_user)
        self.bot.add_user(blocked_user)
        ok, _ = await self.service.set_watch_mentions(watched_user.id, guild_id=guild.id, channel_id=visible_channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_watch_mentions(blocked_user.id, guild_id=guild.id, channel_id=visible_channel.id, scope="server", enabled=True)
        self.assertTrue(ok)
        ok, _ = await self.service.add_watch_ignored_user(watched_user.id, ignored_user_id=author.id)
        self.assertTrue(ok)

        visible_message = DummyMessage(
            message_id=54,
            author=author,
            guild=guild,
            channel=visible_channel,
            content="hey <@280>",
            created_at=ge.now_utc(),
            raw_mentions=[watched_user.id],
        )
        visible_message.mentions = [watched_user]
        hidden_message = DummyMessage(
            message_id=55,
            author=author,
            guild=guild,
            channel=hidden_channel,
            content="hey <@281>",
            created_at=ge.now_utc() + timedelta(seconds=1),
            raw_mentions=[blocked_user.id],
        )
        hidden_message.mentions = [blocked_user]

        await self.service.handle_watch_message(visible_message)
        await self.service.handle_watch_message(hidden_message)

        watched_user.send.assert_not_awaited()
        blocked_user.send.assert_not_awaited()

    async def test_channel_reminders_are_strictly_limited(self):
        user = DummyUser(55)
        ok, _ = await self.service.create_reminder(
            user=user,
            text="Check the thread.",
            delay_seconds=20 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": 1, "name": "Guild"})(),
            channel=type("Channel", (), {"id": 2, "name": "general"})(),
            origin_jump_url=None,
        )
        self.assertTrue(ok)
        self.service._reminder_cooldowns[user.id] = 0.0
        ok, message = await self.service.create_reminder(
            user=user,
            text="Second public reminder.",
            delay_seconds=25 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": 1, "name": "Guild"})(),
            channel=type("Channel", (), {"id": 2, "name": "general"})(),
            origin_jump_url=None,
        )
        self.assertFalse(ok)
        self.assertIn("channel reminder", message)

    async def test_channel_reminders_require_longer_delay(self):
        user = DummyUser(56)
        ok, message = await self.service.create_reminder(
            user=user,
            text="Too soon.",
            delay_seconds=5 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": 1, "name": "Guild"})(),
            channel=type("Channel", (), {"id": 2, "name": "general"})(),
            origin_jump_url=None,
        )
        self.assertFalse(ok)
        self.assertIn("at least", message)

    async def test_public_reminder_delivery_is_rechecked_and_withheld_privately_when_blocked(self):
        user = DummyMember(560, display_name="Mira")
        guild = DummyGuild(1, members=[user])
        channel = DummyChannel(2, guild=guild, visible_user_ids={user.id})
        gateway = FeatureGatewaySpy(blocked_surfaces={FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY: "Babblebox withheld that public reminder."})
        self.bot.shield_service = types.SimpleNamespace(feature_gateway=gateway)
        self.bot.add_user(user)
        self.bot.add_channel(channel)
        record = {
            "id": "blocked-reminder",
            "user_id": user.id,
            "text": "Safe reminder text",
            "delivery": "here",
            "created_at": serialize_datetime(ge.now_utc()),
            "due_at": serialize_datetime(ge.now_utc()),
            "guild_id": guild.id,
            "guild_name": guild.name,
            "channel_id": channel.id,
            "channel_name": channel.name,
            "origin_jump_url": None,
            "delivery_attempts": 0,
            "last_attempt_at": None,
            "retry_after": None,
        }

        delivered = await self.service._deliver_single_reminder(record)

        self.assertTrue(delivered)
        self.assertEqual(channel.sent, [])
        user.send.assert_awaited_once()
        self.assertIn("withheld", user.send.await_args.args[0].lower())
        self.assertEqual(gateway.evaluations, [(FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY, "Safe reminder text")])

    async def test_public_reminder_delivery_real_gateway_withholds_severe_text(self):
        user = DummyMember(562, display_name="Ari")
        guild = DummyGuild(1, members=[user])
        channel = DummyChannel(2, guild=guild, visible_user_ids={user.id})
        self.bot.add_user(user)
        self.bot.add_channel(channel)
        record = {
            "id": "rem-real-severe",
            "user_id": user.id,
            "text": "kill yourself",
            "delivery": "here",
            "created_at": serialize_datetime(ge.now_utc()),
            "due_at": serialize_datetime(ge.now_utc()),
            "guild_id": guild.id,
            "guild_name": guild.name,
            "channel_id": channel.id,
            "channel_name": channel.name,
            "origin_jump_url": None,
            "delivery_attempts": 0,
            "last_attempt_at": None,
            "retry_after": None,
        }

        removed = await self.service._deliver_single_reminder(record)

        self.assertTrue(removed)
        user.send.assert_awaited_once()
        self.assertIn("withheld", user.send.await_args.args[0].lower())
        self.assertFalse(channel.sent)

    async def test_public_reminder_delivery_posts_when_feature_gateway_allows_it(self):
        user = DummyMember(561, display_name="Ari")
        guild = DummyGuild(1, members=[user])
        channel = DummyChannel(2, guild=guild, visible_user_ids={user.id})
        self.bot.add_user(user)
        self.bot.add_channel(channel)
        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Check the thread.",
            delay_seconds=20 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": guild.id, "name": guild.name})(),
            channel=type("Channel", (), {"id": channel.id, "name": channel.name})(),
            origin_jump_url=None,
        )
        self.assertTrue(ok)

        delivered = await self.service._deliver_single_reminder(reminder)

        self.assertTrue(delivered)
        self.assertEqual(len(channel.sent), 1)
        user.send.assert_not_awaited()

    async def test_failed_reminder_delivery_is_retried_instead_of_removed(self):
        user = DummyMember(57, display_name="Mira")
        guild = DummyGuild(1, members=[user])
        channel = DummyChannel(2, guild=guild, visible_user_ids={user.id})
        response = types.SimpleNamespace(status=403, reason="Forbidden", text="Forbidden")
        channel.send = AsyncMock(side_effect=discord.Forbidden(response=response, message="missing perms"))
        user.send = AsyncMock(side_effect=discord.Forbidden(response=response, message="closed dms"))
        self.bot.add_user(user)
        self.bot.add_channel(channel)

        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Retry me later.",
            delay_seconds=20 * 60,
            delivery="here",
            guild=guild,
            channel=channel,
            origin_jump_url=None,
        )
        self.assertTrue(ok)
        reminder["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        due_reminders, _, _, _, _ = self.service._collect_due_records()
        await self.service._deliver_due_reminders(due_reminders)

        stored = self.service.store.state["reminders"][reminder["id"]]
        self.assertEqual(stored["delivery_attempts"], 1)
        self.assertIsNotNone(stored["last_attempt_at"])
        self.assertGreater(deserialize_datetime(stored["retry_after"]), ge.now_utc())

    async def test_retrying_reminder_waits_until_retry_after(self):
        user = DummyUser(58)
        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Wait to retry.",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )
        self.assertTrue(ok)
        reminder["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        retry_after = ge.now_utc() + timedelta(minutes=15)
        reminder["retry_after"] = serialize_datetime(retry_after)
        reminder["delivery_attempts"] = 1

        due_reminders, _, _, _, next_due = self.service._collect_due_records()

        self.assertEqual(due_reminders, [])
        self.assertEqual(next_due, retry_after)

    async def test_successful_retry_delivery_removes_reminder(self):
        user = DummyMember(59, display_name="Ari")
        self.bot.add_user(user)
        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Deliver on retry.",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url="https://discord.com/channels/1/2/3",
        )
        self.assertTrue(ok)
        reminder["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=5))
        reminder["retry_after"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        reminder["delivery_attempts"] = 2

        due_reminders, _, _, _, _ = self.service._collect_due_records()
        await self.service._deliver_due_reminders(due_reminders)

        user.send.assert_awaited_once()
        self.assertNotIn(reminder["id"], self.service.store.state["reminders"])

    async def test_afk_notice_lines_cover_reply_targets(self):
        user = DummyUser(77)
        ok, _ = await self.service.set_afk(
            user=user,
            reason="Stepped away",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        lines = self.service.build_afk_notice_lines_for_targets(
            channel_id=12,
            author_id=99,
            targets=[DummyTarget(77, display_name="CoffeeUser")],
        )
        self.assertEqual(len(lines), 1)
        self.assertIn("CoffeeUser", lines[0])
        self.assertIn("Stepped away", lines[0])

    async def test_afk_reason_rejects_private_contact_details(self):
        ok, message = await self.service.set_afk(
            user=DummyUser(780),
            reason="Call me at +1 (212) 555-0189.",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )

        self.assertFalse(ok)
        self.assertIn("phone numbers", message.lower())

    async def test_reminder_text_blocks_adult_dm_solicitation_via_feature_gateway(self):
        ok, message = await self.service.create_reminder(
            user=DummyUser(781),
            text="DM me for adult content.",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )

        self.assertFalse(ok)
        self.assertIn("adult", message.lower())

    async def test_scheduled_afk_activates_when_due(self):
        user = DummyUser(88)
        ok, _ = await self.service.set_afk(
            user=user,
            reason=build_afk_reason_text(preset="sleeping", custom_reason=None),
            preset="sleeping",
            duration_seconds=30 * 60,
            start_in_seconds=10 * 60,
        )
        self.assertTrue(ok)
        record = self.service.store.state["afk"][str(user.id)]
        now = ge.now_utc()
        record["starts_at"] = serialize_datetime(now - timedelta(minutes=1))
        record["ends_at"] = serialize_datetime(now + timedelta(minutes=29))

        _, afk_to_activate, _, _, _ = self.service._collect_due_records()
        self.assertEqual(len(afk_to_activate), 1)

        await self.service._activate_due_afk(afk_to_activate)

        self.assertEqual(self.service.store.state["afk"][str(user.id)]["status"], "active")

    async def test_active_afk_expires_when_due(self):
        user = DummyUser(89)
        ok, _ = await self.service.set_afk(
            user=user,
            reason=build_afk_reason_text(preset="studying", custom_reason=None),
            preset="studying",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        record = self.service.store.state["afk"][str(user.id)]
        record["ends_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        _, _, afk_to_expire, _, _ = self.service._collect_due_records()
        self.assertEqual(len(afk_to_expire), 1)

        await self.service._expire_due_afk(afk_to_expire)

        self.assertNotIn(str(user.id), self.service.store.state["afk"])

    async def test_afk_timezone_can_be_saved_and_cleared(self):
        ok, timezone_name = await self.service.set_afk_timezone(90, "utc+4")
        self.assertTrue(ok)
        self.assertEqual(timezone_name, "UTC+04:00")
        self.assertEqual(self.service.get_afk_timezone(90), "UTC+04:00")

        ok, message = await self.service.clear_afk_timezone(90)
        self.assertTrue(ok)
        self.assertIn("cleared", message.lower())
        self.assertIsNone(self.service.get_afk_timezone(90))

    async def test_recurring_afk_schedule_can_be_created_and_removed(self):
        user = DummyUser(91)
        ok, schedule = await self.service.create_afk_schedule(
            user=user,
            repeat="weekly",
            timezone_name="UTC+04:00",
            local_hour=9,
            local_minute=0,
            weekday=0,
            reason=build_afk_reason_text(preset="working", custom_reason="Office hours"),
            preset="working",
            duration_seconds=8 * 3600,
        )
        self.assertTrue(ok)
        self.assertEqual(len(self.service.list_afk_schedules(user.id)), 1)
        self.assertEqual(schedule["repeat"], "weekly")
        self.assertEqual(schedule["timezone"], "UTC+04:00")

        ok, message = await self.service.remove_afk_schedule(user.id, schedule["id"][:8])
        self.assertTrue(ok)
        self.assertIn("removed", message.lower())
        self.assertEqual(self.service.list_afk_schedules(user.id), [])

    async def test_recurring_afk_schedule_activates_and_advances_next_start(self):
        user = DummyUser(92)
        now = ge.now_utc().replace(second=0, microsecond=0)
        local_start = now - timedelta(minutes=30)
        schedule = {
            "id": "sched1234",
            "user_id": user.id,
            "reason": build_afk_reason_text(preset="gaming", custom_reason="Raid night"),
            "preset": "gaming",
            "timezone": "UTC+00:00",
            "repeat": "daily",
            "weekday_mask": 127,
            "local_hour": local_start.hour,
            "local_minute": local_start.minute,
            "duration_seconds": 2 * 3600,
            "created_at": serialize_datetime(now - timedelta(days=1)),
            "next_start_at": serialize_datetime(now - timedelta(minutes=30)),
        }
        self.service.store.state["afk_schedules"][schedule["id"]] = dict(schedule)

        _, _, _, afk_schedule_candidates, _ = self.service._collect_due_records()
        self.assertEqual(len(afk_schedule_candidates), 1)

        await self.service._activate_due_afk_schedules(afk_schedule_candidates)

        afk_record = self.service.store.state["afk"][str(user.id)]
        self.assertEqual(afk_record["schedule_id"], schedule["id"])
        self.assertEqual(afk_record["preset"], "gaming")
        self.assertEqual(
            self.service.store.state["afk_schedules"][schedule["id"]]["next_start_at"],
            serialize_datetime(compute_next_afk_schedule_start(schedule, after=now)),
        )

    async def test_legacy_json_path_is_import_source_only(self):
        with TemporaryDirectory() as temp_dir:
            legacy_path = Path(temp_dir) / "utility_state.json"
            legacy_path.write_text('{"version": 1, "watch": {}, "later": {}, "reminders": {}, "brb": {}}', encoding="utf-8")
            store = UtilityStateStore(legacy_path, backend="memory")
            await store.load()
            self.assertEqual(store.backend_name, "memory")
            self.assertFalse(store.state["reminders"])

    async def test_create_user_return_watch_dedupes_and_refreshes(self):
        ok, record, refreshed = await self.service.upsert_return_watch(
            watcher_user_id=11,
            guild_id=22,
            target_type="user",
            target_id=33,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)
        self.assertFalse(refreshed)
        watch_id = record["id"]
        self.service.store.state["return_watches"][watch_id]["created_at"] = serialize_datetime(ge.now_utc() - timedelta(hours=3))
        self.service.store.state["return_watches"][watch_id]["expires_at"] = serialize_datetime(ge.now_utc() + timedelta(minutes=30))
        self.service._rebuild_return_watch_indexes()

        ok, updated, refreshed = await self.service.upsert_return_watch(
            watcher_user_id=11,
            guild_id=22,
            target_type="user",
            target_id=33,
            duration_seconds=6 * 3600,
            created_from="afk_button",
        )

        self.assertTrue(ok)
        self.assertTrue(refreshed)
        self.assertEqual(updated["id"], watch_id)
        self.assertEqual(len(self.service.store.state["return_watches"]), 1)
        self.assertEqual(updated["created_from"], "afk_button")

    async def test_create_channel_return_watch(self):
        ok, record, refreshed = await self.service.upsert_return_watch(
            watcher_user_id=90,
            guild_id=100,
            target_type="channel",
            target_id=200,
            duration_seconds=24 * 3600,
            created_from="slash_command",
        )

        self.assertTrue(ok)
        self.assertFalse(refreshed)
        self.assertEqual(record["target_type"], "channel")
        self.assertEqual(record["target_id"], 200)

    async def test_return_watch_normalization_is_restart_safe(self):
        payload = {
            "version": 5,
            "return_watches": {
                "watch1": {
                    "watcher_user_id": 5,
                    "guild_id": 10,
                    "target_type": "user",
                    "target_id": 15,
                    "created_at": serialize_datetime(ge.now_utc()),
                    "expires_at": serialize_datetime(ge.now_utc() + timedelta(hours=1)),
                    "created_from": "slash_command",
                }
            },
        }
        normalized = self.store._store.normalize_state(payload)
        self.assertIn("watch1", normalized["return_watches"])

        reloaded_service = UtilityService(self.bot, store=self.store)
        reloaded_service.store.state = normalized
        reloaded_service.storage_ready = True
        reloaded_service._rebuild_return_watch_indexes()
        self.assertIn((10, 15), reloaded_service._return_user_watch_ids_by_target)

    async def test_postgres_reload_preserves_later_attachments_and_reminder_retry_fields(self):
        now = ge.now_utc()
        connection = FakeReloadConnection(
            watch_rows=[
                {
                    "user_id": 1,
                    "mention_global": True,
                    "mention_guild_ids": json.dumps([10, 10, 11]),
                    "mention_channel_ids": json.dumps([20]),
                    "reply_global": False,
                    "reply_guild_ids": json.dumps([12]),
                    "reply_channel_ids": json.dumps([21, 22]),
                    "excluded_channel_ids": json.dumps([99]),
                    "ignored_user_ids": json.dumps([5, 5]),
                }
            ],
            later_rows=[
                {
                    "user_id": 1,
                    "guild_id": 10,
                    "guild_name": "Guild",
                    "channel_id": 20,
                    "channel_name": "clips",
                    "message_id": 30,
                    "message_jump_url": "https://discord.com/channels/10/20/30",
                    "message_created_at": now,
                    "saved_at": now,
                    "author_name": "Mira",
                    "author_id": 1,
                    "preview": "Saved message",
                    "attachment_labels": json.dumps(["image.png", "clip.mp4"]),
                }
            ],
            reminder_rows=[
                {
                    "id": "reminder1",
                    "user_id": 1,
                    "text": "Retry later",
                    "delivery": "dm",
                    "created_at": now,
                    "due_at": now,
                    "guild_id": 10,
                    "guild_name": "Guild",
                    "channel_id": None,
                    "channel_name": None,
                    "origin_jump_url": "https://discord.com/channels/10/20/30",
                    "delivery_attempts": 2,
                    "last_attempt_at": now - timedelta(minutes=5),
                    "retry_after": now + timedelta(minutes=10),
                }
            ],
        )
        store = _PostgresUtilityStore("postgresql://utility-user:secret@db.example.com:5432/app")
        store._pool = FakePool(connection)

        await store._reload_from_db()

        watch = store.state["watch"]["1"]
        marker = store.state["later"]["1"]["20"]
        reminder = store.state["reminders"]["reminder1"]
        self.assertTrue(watch["mention_global"])
        self.assertEqual(watch["mention_guild_ids"], [10, 11])
        self.assertEqual(watch["mention_channel_ids"], [20])
        self.assertEqual(watch["reply_guild_ids"], [12])
        self.assertEqual(watch["reply_channel_ids"], [21, 22])
        self.assertEqual(watch["excluded_channel_ids"], [99])
        self.assertEqual(watch["ignored_user_ids"], [5])
        self.assertEqual(marker["attachment_labels"], ["image.png", "clip.mp4"])
        self.assertEqual(reminder["delivery_attempts"], 2)
        self.assertEqual(reminder["retry_after"], serialize_datetime(now + timedelta(minutes=10)))

    def test_reminder_list_embed_marks_retrying_delivery(self):
        cog = object.__new__(UtilityCog)
        user = DummyTarget(60, display_name="Mira")
        embed = UtilityCog._reminder_list_embed(
            cog,
            user,
            [
                {
                    "id": "abcdef123456",
                    "delivery": "dm",
                    "due_at": serialize_datetime(ge.now_utc() - timedelta(minutes=2)),
                    "retry_after": serialize_datetime(ge.now_utc() + timedelta(minutes=8)),
                    "text": "Retry status visible",
                }
            ],
        )

        self.assertIn("Retrying delivery", embed.fields[0].value)

    async def test_user_return_watch_triggers_on_next_message_after_creation(self):
        watcher = DummyMember(41, display_name="Mira")
        target = DummyMember(42, display_name="Alex")
        guild = DummyGuild(10, members=[watcher, target])
        channel = DummyChannel(20, guild=guild, visible_user_ids={watcher.id, target.id})
        message = DummyMessage(
            message_id=30,
            author=target,
            guild=guild,
            channel=channel,
            content="I'm back.",
            created_at=ge.now_utc() + timedelta(seconds=5),
        )
        self.bot.add_user(watcher)
        ok, _, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="afk_button",
        )
        self.assertTrue(ok)

        await self.service.handle_return_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertEqual(self.service.store.state["return_watches"], {})

    async def test_user_return_watch_does_not_trigger_on_message_before_creation(self):
        watcher = DummyMember(51, display_name="Mira")
        target = DummyMember(52, display_name="Alex")
        guild = DummyGuild(11, members=[watcher, target])
        channel = DummyChannel(21, guild=guild, visible_user_ids={watcher.id, target.id})
        message_time = ge.now_utc()
        message = DummyMessage(
            message_id=31,
            author=target,
            guild=guild,
            channel=channel,
            content="Almost back.",
            created_at=message_time,
        )
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)
        self.service.store.state["return_watches"][record["id"]]["created_at"] = serialize_datetime(message_time + timedelta(seconds=1))
        self.service._rebuild_return_watch_indexes()

        await self.service.handle_return_watch_message(message)

        watcher.send.assert_not_awaited()
        self.assertIn(record["id"], self.service.store.state["return_watches"])

    async def test_channel_return_watch_triggers_once_and_deletes(self):
        watcher = DummyMember(61, display_name="Mira")
        author = DummyMember(62, display_name="Nina")
        guild = DummyGuild(12, members=[watcher, author])
        channel = DummyChannel(22, guild=guild, visible_user_ids={watcher.id, author.id})
        message = DummyMessage(
            message_id=32,
            author=author,
            guild=guild,
            channel=channel,
            content="Fresh message.",
            created_at=ge.now_utc() + timedelta(seconds=5),
        )
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="channel",
            target_id=channel.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)

        await self.service.handle_return_watch_message(message)
        await self.service.handle_return_watch_message(message)

        watcher.send.assert_awaited_once()
        self.assertNotIn(record["id"], self.service.store.state["return_watches"])

    async def test_expired_return_watch_does_not_trigger(self):
        watcher = DummyMember(71, display_name="Mira")
        target = DummyMember(72, display_name="Alex")
        guild = DummyGuild(13, members=[watcher, target])
        channel = DummyChannel(23, guild=guild, visible_user_ids={watcher.id, target.id})
        message = DummyMessage(
            message_id=33,
            author=target,
            guild=guild,
            channel=channel,
            content="Too late.",
            created_at=ge.now_utc(),
        )
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)
        self.service.store.state["return_watches"][record["id"]]["expires_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        self.service._rebuild_return_watch_indexes()

        await self.service.handle_return_watch_message(message)

        watcher.send.assert_not_awaited()
        self.assertNotIn(record["id"], self.service.store.state["return_watches"])

    async def test_dm_failure_cleans_up_return_watch(self):
        watcher = DummyMember(81, display_name="Mira")
        target = DummyMember(82, display_name="Alex")
        guild = DummyGuild(14, members=[watcher, target])
        channel = DummyChannel(24, guild=guild, visible_user_ids={watcher.id, target.id})
        message = DummyMessage(
            message_id=34,
            author=target,
            guild=guild,
            channel=channel,
            content="Back again.",
            created_at=ge.now_utc() + timedelta(seconds=5),
        )
        response = types.SimpleNamespace(status=403, reason="Forbidden", text="Forbidden")
        watcher.send = AsyncMock(side_effect=discord.Forbidden(response=response, message="closed"))
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)

        await self.service.handle_return_watch_message(message)

        self.assertNotIn(record["id"], self.service.store.state["return_watches"])

    async def test_channel_return_watch_respects_channel_access(self):
        watcher = DummyMember(91, display_name="Mira")
        author = DummyMember(92, display_name="Nina")
        guild = DummyGuild(15, members=[watcher, author])
        channel = DummyChannel(25, guild=guild, visible_user_ids={author.id})
        message = DummyMessage(
            message_id=35,
            author=author,
            guild=guild,
            channel=channel,
            content="Private update.",
            created_at=ge.now_utc() + timedelta(seconds=5),
        )
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=guild.id,
            target_type="channel",
            target_id=channel.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)

        await self.service.handle_return_watch_message(message)

        watcher.send.assert_not_awaited()
        self.assertNotIn(record["id"], self.service.store.state["return_watches"])

    async def test_user_return_watch_is_guild_scoped(self):
        watcher = DummyMember(101, display_name="Mira")
        target = DummyMember(102, display_name="Alex")
        watched_guild = DummyGuild(16, members=[watcher, target])
        other_guild = DummyGuild(17, members=[watcher, target])
        watched_channel = DummyChannel(26, guild=watched_guild, visible_user_ids={watcher.id, target.id})
        other_channel = DummyChannel(27, guild=other_guild, visible_user_ids={watcher.id, target.id})
        message = DummyMessage(
            message_id=36,
            author=target,
            guild=other_guild,
            channel=other_channel,
            content="Different server.",
            created_at=ge.now_utc() + timedelta(seconds=5),
        )
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=watched_guild.id,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="slash_command",
        )
        self.assertTrue(ok)

        await self.service.handle_return_watch_message(message)

        watcher.send.assert_not_awaited()
        self.assertIn(record["id"], self.service.store.state["return_watches"])

    async def test_clearing_afk_does_not_trigger_return_watch(self):
        watcher = DummyMember(111, display_name="Mira")
        target = DummyMember(112, display_name="Alex")
        ok, _ = await self.service.set_afk(
            user=target,
            reason="Stepped away",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        ok, record, _ = await self.service.upsert_return_watch(
            watcher_user_id=watcher.id,
            guild_id=18,
            target_type="user",
            target_id=target.id,
            duration_seconds=3600,
            created_from="afk_button",
        )
        self.assertTrue(ok)

        removed = await self.service.clear_afk_on_activity(target.id)

        self.assertIsNotNone(removed)
        watcher.send.assert_not_awaited()
        self.assertIn(record["id"], self.service.store.state["return_watches"])
