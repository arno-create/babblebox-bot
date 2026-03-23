import os
import types
import unittest
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional
from unittest.mock import AsyncMock

import discord

from babblebox import game_engine as ge
from babblebox.utility_helpers import build_afk_reason_text, compute_next_afk_schedule_start, serialize_datetime
from babblebox.utility_service import UtilityService
from babblebox.utility_store import UtilityStateStore, UtilityStorageUnavailable


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

    def permissions_for(self, member):
        allowed = getattr(member, "id", None) in self._visible_user_ids
        return types.SimpleNamespace(view_channel=allowed, read_message_history=allowed)


class DummyGuild:
    def __init__(self, guild_id: int, *, name: str = "Guild", members: Optional[list[DummyMember]] = None):
        self.id = guild_id
        self.name = name
        self._members = {member.id: member for member in (members or [])}

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class DummyMessage:
    def __init__(self, *, message_id: int, author, guild, channel, content: str, created_at):
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


class DummyBot:
    def __init__(self):
        self._users = {}

    def add_user(self, user):
        self._users[user.id] = user

    def get_user(self, user_id: int):
        return self._users.get(user_id)


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
