import os
import unittest
from unittest.mock import AsyncMock, patch

import discord
from discord import app_commands

from babblebox.bot import BabbleBot, DISCORD_MAX_COMMAND_CHOICES, DISCORD_MAX_COMMAND_OPTIONS


MEMORY_STORAGE_ENV = {
    "UTILITY_STORAGE_BACKEND": "memory",
    "ADMIN_STORAGE_BACKEND": "memory",
    "SHIELD_STORAGE_BACKEND": "memory",
    "PROFILE_STORAGE_BACKEND": "memory",
    "QUESTION_DROPS_STORAGE_BACKEND": "memory",
    "CONFESSIONS_STORAGE_BACKEND": "memory",
    "PREMIUM_STORAGE_BACKEND": "memory",
}


async def _fake_load_dictionary(self):
    self.dictionary_ready = True


def _fake_synced_root(name: str, children: tuple[str, ...], default_member_permissions=None):
    return type(
        f"FakeSynced{name.title()}Command",
        (),
        {
            "name": name,
            "options": [type("FakeOption", (), {"name": child})() for child in children],
            "default_member_permissions": default_member_permissions,
        },
    )()


class _FakeInteractionResponse:
    def __init__(self, *, done: bool):
        self._done = done
        self.send_message = AsyncMock()

    def is_done(self) -> bool:
        return self._done


class _FakeInteraction:
    def __init__(self, *, done: bool):
        self.command = type("FakeCommand", (), {"qualified_name": "premium link"})()
        self.guild = type("FakeGuild", (), {"id": 123})()
        self.channel = type("FakeChannel", (), {"id": 456})()
        self.user = type("FakeUser", (), {"id": 789})()
        self.response = _FakeInteractionResponse(done=done)
        self.followup = type("FakeFollowup", (), {"send": AsyncMock()})()


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

    async def test_setup_hook_syncs_and_verifies_required_roots(self):
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

                premium_root = next(command for command in bot.tree.get_commands() if command.name == "premium")
                lock_root = next(command for command in bot.tree.get_commands() if command.name == "lock")
                timeout_root = next(command for command in bot.tree.get_commands() if command.name == "timeout")
                self.assertEqual(sync_targets, [None])
                self.assertEqual({command.name for command in premium_root.commands}, {"status", "plans", "subscribe", "link", "refresh", "unlink", "guild"})
                self.assertEqual({command.name for command in lock_root.commands}, {"channel", "remove", "settings"})
                self.assertEqual({command.name for command in timeout_root.commands}, {"remove"})
                self.assertIsNone(premium_root.default_permissions)
                self.assertIsNone(lock_root.default_permissions)
                self.assertIsNone(timeout_root.default_permissions)
            finally:
                await self._close_bot(bot)


class BabbleBotCommandErrorTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_invalid_dev_guild_id_logs_warning(self):
        with self._env(DEV_GUILD_ID="not-a-number"), patch("babblebox.bot.LOGGER.warning") as warning_log:
            bot = BabbleBot()
            try:
                self.assertIsNone(bot.dev_guild_id)
                warning_log.assert_called_once_with(
                    "Invalid DEV_GUILD_ID '%s'. Ignoring dev guild sync.",
                    "not-a-number",
                )
            finally:
                await bot.close()

    async def test_check_failures_log_metadata_and_reply_privately(self):
        bot = BabbleBot()
        interaction = _FakeInteraction(done=False)

        try:
            with patch("babblebox.bot.LOGGER.info") as info_log:
                await bot.on_app_command_error(interaction, app_commands.CheckFailure())

            info_log.assert_called_once_with(
                "App command failure: command=%s guild_id=%s channel_id=%s user_id=%s error_type=%s",
                "premium link",
                123,
                456,
                789,
                "CheckFailure",
            )
            interaction.response.send_message.assert_awaited_once_with(
                "This command can only be used in a server.",
                ephemeral=True,
            )
            interaction.followup.send.assert_not_awaited()
        finally:
            await bot.close()

    async def test_unexpected_failures_log_type_only_and_use_followup_when_response_started(self):
        class FakeAppCommandError(app_commands.AppCommandError):
            pass

        bot = BabbleBot()
        interaction = _FakeInteraction(done=True)

        try:
            with patch("babblebox.bot.LOGGER.error") as error_log:
                await bot.on_app_command_error(interaction, FakeAppCommandError("sensitive details"))

            error_log.assert_called_once_with(
                "App command failure: command=%s guild_id=%s channel_id=%s user_id=%s error_type=%s",
                "premium link",
                123,
                456,
                789,
                "FakeAppCommandError",
            )
            interaction.followup.send.assert_awaited_once_with(
                "Something went wrong while running that command.",
                ephemeral=True,
            )
            interaction.response.send_message.assert_not_awaited()
        finally:
            await bot.close()

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

    async def test_setup_hook_raises_when_synced_result_is_missing_required_roots(self):
        with self._env():
            bot = BabbleBot()

            async def fake_sync(*, guild=None):
                return [command for command in bot.tree.get_commands(guild=guild) if command.name not in {"premium", "lock", "timeout"}]

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary), patch.object(
                    bot.tree,
                    "sync",
                    new=AsyncMock(side_effect=fake_sync),
                ):
                    with self.assertRaises(RuntimeError) as error_context:
                        await bot.setup_hook()

                self.assertIn("missing `/premium`", str(error_context.exception))
                self.assertIn("missing `/lock`", str(error_context.exception))
                self.assertIn("missing `/timeout`", str(error_context.exception))
                self.assertIn("global commands sync result", str(error_context.exception))
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_raises_when_synced_timeout_root_keeps_old_visibility_bitset(self):
        with self._env():
            bot = BabbleBot()

            synced_premium_root = _fake_synced_root("premium", ("status", "plans", "subscribe", "link", "refresh", "unlink", "guild"))
            synced_lock_root = _fake_synced_root("lock", ("channel", "remove", "settings"))
            stale_timeout_root = _fake_synced_root("timeout", ("remove",), discord.Permissions(moderate_members=True))

            async def fake_sync(*, guild=None):
                return [synced_premium_root, synced_lock_root, stale_timeout_root]

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

            synced_premium_root = _fake_synced_root("premium", ("status", "plans", "subscribe", "link", "refresh", "unlink", "guild"))
            stale_lock_root = _fake_synced_root("lock", ("channel", "remove", "settings"), discord.Permissions(manage_channels=True))
            synced_timeout_root = _fake_synced_root("timeout", ("remove",))

            async def fake_sync(*, guild=None):
                return [synced_premium_root, stale_lock_root, synced_timeout_root]

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

    async def test_setup_hook_fails_closed_when_required_service_storage_is_unavailable(self):
        failure_cases = (
            ("PREMIUM_STORAGE_BACKEND", "Premium"),
            ("CONFESSIONS_STORAGE_BACKEND", "Confessions"),
            ("SHIELD_STORAGE_BACKEND", "Shield"),
            ("ADMIN_STORAGE_BACKEND", "Admin"),
            ("UTILITY_STORAGE_BACKEND", "Utilities"),
            ("PROFILE_STORAGE_BACKEND", "Profile"),
            ("QUESTION_DROPS_STORAGE_BACKEND", "Question Drops"),
        )
        forced_missing_database_env = {
            "PREMIUM_DATABASE_URL": "",
            "QUESTION_DROPS_DATABASE_URL": "",
            "UTILITY_DATABASE_URL": "",
            "SUPABASE_DB_URL": "",
            "DATABASE_URL": "",
            "CONFESSIONS_CONTENT_KEY": "c" * 32,
            "CONFESSIONS_IDENTITY_KEY": "i" * 32,
        }

        for env_name, service_label in failure_cases:
            with self.subTest(service=service_label):
                with self._env(**forced_missing_database_env, **{env_name: "postgres"}):
                    bot = BabbleBot()

                    try:
                        with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary):
                            with self.assertRaises(Exception) as error_context:
                                await bot.setup_hook()

                        message = str(error_context.exception)
                        self.assertIn(f"{service_label} startup failed", message)
                        self.assertIn("configured backend `postgres`", message)
                    finally:
                        await self._close_bot(bot)

    async def test_setup_hook_fails_closed_when_patreon_configuration_is_partial(self):
        with self._env(
            PUBLIC_BASE_URL="https://example.test",
            PATREON_CLIENT_ID="client-only",
        ):
            bot = BabbleBot()

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary):
                    with self.assertRaises(Exception) as error_context:
                        await bot.setup_hook()

                message = str(error_context.exception)
                self.assertIn("Premium startup unsafe", message)
                self.assertIn("Patreon premium configuration is incomplete or inconsistent", message)
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_fails_closed_when_public_premium_uses_memory_storage(self):
        with self._env(
            PUBLIC_BASE_URL="https://example.test",
            PREMIUM_STORAGE_BACKEND="memory",
            PATREON_CLIENT_ID="client",
            PATREON_CLIENT_SECRET="secret",
            PATREON_REDIRECT_URI="https://example.test/premium/patreon/callback",
            PATREON_WEBHOOK_SECRET="webhook-secret",
            PATREON_CAMPAIGN_ID="1234",
            PATREON_PLUS_TIER_IDS="9876",
        ):
            bot = BabbleBot()

            try:
                with patch.object(BabbleBot, "_load_dictionary", new=_fake_load_dictionary):
                    with self.assertRaises(Exception) as error_context:
                        await bot.setup_hook()

                message = str(error_context.exception)
                self.assertIn("Premium startup unsafe", message)
                self.assertIn("Postgres-backed premium storage", message)
            finally:
                await self._close_bot(bot)

    async def test_setup_hook_allows_disabled_patreon_on_free_only_deployment(self):
        with self._env(PUBLIC_BASE_URL="https://example.test"):
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

                self.assertEqual(sync_targets, [None])
                diagnostics = bot.premium_service.provider_diagnostics()
                self.assertEqual(diagnostics["startup_state"], "disabled")
                self.assertFalse(diagnostics["patreon_configured"])
            finally:
                await self._close_bot(bot)
