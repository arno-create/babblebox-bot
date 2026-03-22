import unittest
import os
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from babblebox import game_engine as ge
from babblebox.utility_service import UtilityService
from babblebox.utility_helpers import serialize_datetime
from babblebox.utility_store import UtilityStateStore, UtilityStorageUnavailable


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id


class DummyTarget(DummyUser):
    def __init__(self, user_id: int, *, bot: bool = False, display_name: Optional[str] = None):
        super().__init__(user_id)
        self.bot = bot
        self.display_name = display_name or f"User {user_id}"


class UtilityStoreAndServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = UtilityStateStore(backend="memory")
        await self.store.load()
        self.service = UtilityService(object(), store=self.store)
        self.service.storage_ready = True

    async def test_memory_store_loads_clean_state(self):
        self.assertEqual(self.store.backend_name, "memory")
        self.assertEqual(self.store.state["watch"], {})
        self.assertEqual(self.store.state["afk"], {})

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
            reason="💤 Sleeping",
            duration_seconds=30 * 60,
            start_in_seconds=10 * 60,
        )
        self.assertTrue(ok)
        record = self.service.store.state["afk"][str(user.id)]
        now = ge.now_utc()
        record["starts_at"] = serialize_datetime(now - timedelta(minutes=1))
        record["ends_at"] = serialize_datetime(now + timedelta(minutes=29))

        _, afk_to_activate, _, _ = self.service._collect_due_records()
        self.assertEqual(len(afk_to_activate), 1)

        await self.service._activate_due_afk(afk_to_activate)

        self.assertEqual(self.service.store.state["afk"][str(user.id)]["status"], "active")

    async def test_active_afk_expires_when_due(self):
        user = DummyUser(89)
        ok, _ = await self.service.set_afk(
            user=user,
            reason="📚 Studying",
            duration_seconds=30 * 60,
            start_in_seconds=None,
        )
        self.assertTrue(ok)
        record = self.service.store.state["afk"][str(user.id)]
        record["ends_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        _, _, afk_to_expire, _ = self.service._collect_due_records()
        self.assertEqual(len(afk_to_expire), 1)

        await self.service._expire_due_afk(afk_to_expire)

        self.assertNotIn(str(user.id), self.service.store.state["afk"])

    async def test_legacy_json_path_is_import_source_only(self):
        with TemporaryDirectory() as temp_dir:
            legacy_path = Path(temp_dir) / "utility_state.json"
            legacy_path.write_text('{"version": 1, "watch": {}, "later": {}, "reminders": {}, "brb": {}}', encoding="utf-8")
            store = UtilityStateStore(legacy_path, backend="memory")
            await store.load()
            self.assertEqual(store.backend_name, "memory")
            self.assertFalse(store.state["reminders"])
