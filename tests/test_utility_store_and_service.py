import tempfile
import unittest
from pathlib import Path

from babblebox.utility_service import UtilityService
from babblebox.utility_store import UtilityStateStore


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id


class UtilityStoreAndServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_path = Path(self.temp_dir.name) / "utility_state.json"
        self.store = UtilityStateStore(self.store_path)
        await self.store.load()
        self.service = UtilityService(object(), store=self.store)

    async def asyncTearDown(self):
        self.temp_dir.cleanup()

    async def test_corrupt_store_falls_back_to_defaults(self):
        self.store_path.write_text("{bad json", encoding="utf-8")
        store = UtilityStateStore(self.store_path)
        state = await store.load()
        self.assertEqual(state["version"], 1)
        self.assertEqual(state["watch"], {})

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

    async def test_set_brb_enforces_cooldown(self):
        user = DummyUser(55)
        ok, _ = await self.service.set_brb(user=user, delay_seconds=600, reason="Coffee", guild=None)
        self.assertTrue(ok)
        ok, message = await self.service.set_brb(user=user, delay_seconds=600, reason="Coffee", guild=None)
        self.assertFalse(ok)
        self.assertIn("cooldown", message)
