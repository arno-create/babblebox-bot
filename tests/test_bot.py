import os
import unittest
from unittest.mock import AsyncMock, patch

import discord

from babblebox.bot import BabbleBot, DISCORD_MAX_COMMAND_CHOICES, DISCORD_MAX_COMMAND_OPTIONS


MEMORY_STORAGE_ENV = {
    "UTILITY_STORAGE_BACKEND": "memory",
    "ADMIN_STORAGE_BACKEND": "memory",
    "SHIELD_STORAGE_BACKEND": "memory",
    "PROFILE_STORAGE_BACKEND": "memory",
    "QUESTION_DROPS_STORAGE_BACKEND": "memory",
    "CONFESSIONS_STORAGE_BACKEND": "memory",
}


async def _fake_load_dictionary(self):
    self.dictionary_ready = True


class BabbleBotSetupHookTests(unittest.IsolatedAsyncioTestCase):
    def _iter_command_schema_paths(self, payload: dict, *, path: str):
        yield path, payload
        for option in payload.get("options", []):
            if not isinstance(option, dict):
                continue
            option_name = option.get("name")
            next_path = f"{path} {option_name}" if option_name else path
            yield from self._iter_command_schema_paths(option, path=next_path)

    async def _close_bot(self, bot: BabbleBot) -> None:
        for loaded in list(bot.cogs.values()):
            service = getattr(loaded, "service", None)
            if service is not None:
                await service.close()
        await bot.close()

    def _env(self, **extra: str):
        values = dict(MEMORY_STORAGE_ENV)
        values.update(extra)
        return patch.dict(os.environ, values, clear=False)

    async def test_setup_hook_syncs_and_verifies_lock_root(self):
        with self._env():
            bot = BabbleBot()
            sync_targets: list[int | None] = []

            async def fake_sync(*, guild=None):
                sync_targets.append(guild.id if guild else None)
                return bot.tree.get_commands(guild=guild)

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    await bot.setup_hook()

                lock_root = next(command for command in bot.tree.get_commands() if command.name == "lock")
                timeout_root = next(command for command in bot.tree.get_commands() if command.name == "timeout")
                self.assertEqual(sync_targets, [None])
                self.assertEqual({command.name for command in lock_root.commands}, {"channel", "remove", "settings"})
                self.assertEqual({command.name for command in timeout_root.commands}, {"remove"})
                self.assertIsNone(lock_root.default_permissions)
                self.assertIsNone(timeout_root.default_permissions)
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_syncs_dev_guild_before_global_when_configured(self):
        with self._env(DEV_GUILD_ID="42"):
            bot = BabbleBot()
            sync_targets: list[int | None] = []
            copy_targets: list[int] = []
            original_copy_global_to = bot.tree.copy_global_to

            async def fake_sync(*, guild=None):
                sync_targets.append(guild.id if guild else None)
                return bot.tree.get_commands(guild=guild)

            def record_copy_global_to(*, guild):
                copy_targets.append(guild.id)
                return original_copy_global_to(guild=guild)

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ), patch.object(bot.tree, "copy_global_to", new=record_copy_global_to):
                    await bot.setup_hook()

                self.assertEqual(copy_targets, [42])
                self.assertEqual(sync_targets, [42, None])
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_when_synced_result_is_missing_lock_root(self):
        with self._env():
            bot = BabbleBot()

            async def fake_sync(*, guild=None):
                return [command for command in bot.tree.get_commands(guild=guild) if command.name not in {"lock", "timeout"}]

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn("missing `/lock`", str(error_context.exception))
                self.assertIn("missing `/timeout`", str(error_context.exception))
                self.assertIn("global commands sync result", str(error_context.exception))
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_when_synced_timeout_root_keeps_old_visibility_bitset(self):
        with self._env():
            bot = BabbleBot()

            synced_lock_root = type(
                "FakeSyncedLockCommand",
                (),
                {
                    "name": "lock",
                    "options": [type("FakeOption", (), {"name": name})() for name in ("channel", "remove", "settings")],
                    "default_member_permissions": None,
                },
            )()
            stale_timeout_root = type(
                "FakeSyncedTimeoutCommand",
                (),
                {
                    "name": "timeout",
                    "options": [type("FakeOption", (), {"name": "remove"})()],
                    "default_member_permissions": discord.Permissions(moderate_members=True),
                },
            )()

            async def fake_sync(*, guild=None):
                return [synced_lock_root, stale_timeout_root]

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                message = str(error_context.exception)
                self.assertIn("default_member_permissions", message)
                self.assertIn("`/timeout`", message)
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_when_synced_lock_root_keeps_old_visibility_bitset(self):
        with self._env():
            bot = BabbleBot()

            stale_lock_root = type(
                "FakeSyncedCommand",
                (),
                {
                    "name": "lock",
                    "options": [type("FakeOption", (), {"name": name})() for name in ("channel", "remove", "settings")],
                    "default_member_permissions": discord.Permissions(manage_channels=True),
                },
            )()

            async def fake_sync(*, guild=None):
                return [stale_lock_root]

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                message = str(error_context.exception)
                self.assertIn("default_member_permissions", message)
                self.assertIn("`/lock`", message)
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_before_sync_when_local_tree_contract_drifts(self):
        with self._env():
            bot = BabbleBot()
            sync_mock = AsyncMock()

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot,
                    "_required_slash_contract",
                    return_value={
                        "lock": {
                            "children": frozenset({"channel", "remove", "missing"}),
                            "default_member_permissions": None,
                        }
                    },
                ), patch.object(bot.tree, "sync", new=sync_mock):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn("`/lock missing`", str(error_context.exception))
                sync_mock.assert_not_awaited()
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_keeps_loaded_tree_within_discord_schema_limits(self):
        with self._env():
            bot = BabbleBot()

            async def fake_sync(*, guild=None):
                return bot.tree.get_commands(guild=guild)

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    await bot.setup_hook()

                for command in bot.tree.get_commands():
                    payload = bot._command_schema_payload(command)
                    for path, node in self._iter_command_schema_paths(payload, path=f"/{command.name}"):
                        self.assertLessEqual(
                            len(node.get("options", [])),
                            DISCORD_MAX_COMMAND_OPTIONS,
                            msg=f"{path} exceeded the Discord option cap",
                        )
                        self.assertLessEqual(
                            len(node.get("choices", [])),
                            DISCORD_MAX_COMMAND_CHOICES,
                            msg=f"{path} exceeded the Discord choice cap",
                        )
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_before_sync_when_local_tree_exposes_too_many_options(self):
        with self._env():
            bot = BabbleBot()
            sync_mock = AsyncMock()
            original_payload = bot._command_schema_payload

            def overflow_payload(command):
                payload = original_payload(command)
                if getattr(command, "name", None) != "shield":
                    return payload
                mutated = dict(payload)
                mutated["options"] = [
                    dict(option)
                    if not (isinstance(option, dict) and option.get("name") == "rules")
                    else {
                        **option,
                        "options": [{"name": f"overflow_{index}"} for index in range(DISCORD_MAX_COMMAND_OPTIONS + 2)],
                    }
                    for option in payload.get("options", [])
                ]
                return mutated

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot,
                    "_command_schema_payload",
                    side_effect=overflow_payload,
                ), patch.object(bot.tree, "sync", new=sync_mock):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn("loaded global tree exposes 27 options on `/shield rules`", str(error_context.exception))
                sync_mock.assert_not_awaited()
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_before_sync_when_local_tree_exposes_too_many_choices(self):
        with self._env():
            bot = BabbleBot()
            sync_mock = AsyncMock()
            original_payload = bot._command_schema_payload

            def overflow_payload(command):
                payload = original_payload(command)
                if getattr(command, "name", None) != "shield":
                    return payload
                mutated = dict(payload)
                mutated["options"] = [
                    dict(option)
                    if not (isinstance(option, dict) and option.get("name") == "rules")
                    else {
                        **option,
                        "options": [
                            {
                                **dict(parameter),
                                "choices": [{"name": f"choice_{index}", "value": f"choice_{index}"} for index in range(DISCORD_MAX_COMMAND_CHOICES + 1)],
                            }
                            if isinstance(parameter, dict) and parameter.get("name") == "pack"
                            else parameter
                            for parameter in option.get("options", [])
                        ],
                    }
                    for option in payload.get("options", [])
                ]
                return mutated

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot,
                    "_command_schema_payload",
                    side_effect=overflow_payload,
                ), patch.object(bot.tree, "sync", new=sync_mock):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn(f"loaded global tree exposes {DISCORD_MAX_COMMAND_CHOICES + 1} choices on `/shield rules pack`", str(error_context.exception))
                sync_mock.assert_not_awaited()
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_when_sync_call_fails(self):
        with self._env():
            bot = BabbleBot()

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=RuntimeError("boom")),
                ):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn("Command sync failed for global commands", str(error_context.exception))
                self.assertIn("boom", str(error_context.exception))
            finally:
                await self._close_bot(bot)
