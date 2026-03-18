import unittest
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

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
            phrase="hello world",
            scope="server",
            mode="contains",
        )
        self.assertTrue(ok)
        summary = self.service.get_watch_summary(42, guild_id=100)
        self.assertEqual(len(summary["server_keywords"]), 1)
        self.assertEqual(summary["server_keywords"][0]["phrase"], "hello world")

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
            duration_minutes=30,
            start_in_minutes=None,
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

    async def test_legacy_json_path_is_import_source_only(self):
        with TemporaryDirectory() as temp_dir:
            legacy_path = Path(temp_dir) / "utility_state.json"
            legacy_path.write_text('{"version": 1, "watch": {}, "later": {}, "reminders": {}, "brb": {}}', encoding="utf-8")
            store = UtilityStateStore(legacy_path, backend="memory")
            await store.load()
            self.assertEqual(store.backend_name, "memory")
            self.assertFalse(store.state["reminders"])
