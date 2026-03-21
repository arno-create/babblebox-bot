import asyncio
import types
import unittest
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import discord

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


class ShieldPermissionSnapshot:
    def __init__(self, **overrides):
        defaults = {
            "view_channel": True,
            "send_messages": True,
            "embed_links": True,
            "manage_messages": True,
            "moderate_members": True,
        }
        defaults.update(overrides)
        for name, value in defaults.items():
            setattr(self, name, value)


class ShieldRole:
    def __init__(self, *, position: int):
        self.position = position

    def __ge__(self, other):
        return self.position >= getattr(other, "position", 0)


class ShieldAwareChannel(FakeChannel):
    def __init__(self, channel_id: int, *, name: str = "general", permissions: Optional[ShieldPermissionSnapshot] = None):
        super().__init__(channel_id)
        self.name = name
        self.mention = f"<#{channel_id}>"
        self._permissions = permissions or ShieldPermissionSnapshot()

    def permissions_for(self, member):
        return self._permissions


class ShieldAwareGuild(FakeGuild):
    def __init__(self, guild_id: int = 10, *, channels=None):
        super().__init__(guild_id)
        self.me = types.SimpleNamespace(id=999, top_role=ShieldRole(position=50))
        self._channels = {channel.id: channel for channel in (channels or [])}

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return None


class ShieldAwareBot:
    def __init__(self, guild: ShieldAwareGuild):
        self.loop = asyncio.get_running_loop()
        self.user = types.SimpleNamespace(id=999)
        self._guild = guild

    def get_guild(self, guild_id: int):
        if guild_id == self._guild.id:
            return self._guild
        return None

    def get_channel(self, channel_id: int):
        return self._guild.get_channel(channel_id)


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

    async def test_buddy_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.buddy_group.callback(cog, ctx, visibility="public")

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

    async def test_shield_status_denies_members_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await ShieldCog.shield_status_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_shield_ai_command_reports_guild_restriction_cleanly(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_ai_command.callback(cog, ctx, enabled=True, min_confidence=None, privacy=None, promo=None, scam=None)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("not available", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_moment_public_card_uses_public_visibility(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            source_author = types.SimpleNamespace(
                id=5,
                display_name="Mira",
                color=discord.Color.blue(),
                display_avatar=types.SimpleNamespace(url="https://cdn.example/avatar.png"),
            )
            source_message = types.SimpleNamespace(
                content="That one line deserved a card.",
                attachments=[],
                author=source_author,
                channel=types.SimpleNamespace(mention="#general"),
                guild=types.SimpleNamespace(name="Guild"),
                created_at=datetime(2026, 3, 21, 12, 0, tzinfo=timezone.utc),
                jump_url="https://discord.com/channels/10/20/30",
            )
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            with patch.object(cog, "_resolve_moment_source", new=AsyncMock(return_value=(source_message, None))):
                await UtilityCog.moment_create_command.callback(cog, ctx, message_link=None, title="Best Line", visibility="public")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertFalse(ctx.defer_calls[0]["ephemeral"])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_shield_panel_overview_reflects_legacy_pack_state(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            guild_id = 10
            cog.service.store.state["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "packs": {
                    "privacy": {"enabled": True, "action": "delete_log", "sensitivity": "high"},
                    "promo": {"tracking": True},
                },
                "scam_enabled": True,
            }

            embed = cog.build_panel_embed(guild_id, "overview")
            protection_field = next(field for field in embed.fields if field.name == "Protection Packs")

            self.assertIn("**Privacy Leak**", protection_field.value)
            self.assertIn("Enabled: Yes | Action: `delete_log` | Sensitivity: High", protection_field.value)
            self.assertIn("**Promo / Invite**", protection_field.value)
            self.assertIn("Enabled: Yes | Action: `log` | Sensitivity: Normal", protection_field.value)
            self.assertIn("**Scam Heuristic**", protection_field.value)
        finally:
            await cog.service.close()

    async def test_shield_panel_warns_when_operability_is_missing(self):
        current_channel = ShieldAwareChannel(20, permissions=ShieldPermissionSnapshot(manage_messages=False, moderate_members=False))
        log_channel = ShieldAwareChannel(
            30,
            name="mod-logs",
            permissions=ShieldPermissionSnapshot(view_channel=False, send_messages=False, embed_links=False),
        )
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
                "scam_enabled": True,
                "scam_action": "timeout_log",
            }

            embed = cog.build_panel_embed(guild.id, "overview", channel_id=current_channel.id)
            operability = next(field for field in embed.fields if field.name == "Operability")

            self.assertIn("Manage Messages", operability.value)
            self.assertIn("Moderate Members", operability.value)
            self.assertIn("View Channel", operability.value)
            self.assertIn("Send Messages", operability.value)
            self.assertIn("Embed Links", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_logs_command_surfaces_log_channel_permission_warning(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs", permissions=ShieldPermissionSnapshot(send_messages=False))
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_logs_command.callback(cog, ctx, channel=log_channel, role=None, clear_channel=False, clear_role=False)

            self.assertEqual(len(ctx.send_calls), 1)
            operability = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Operability")
            self.assertIn("Send Messages", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_stays_quiet_when_permissions_are_available(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs")
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="Email me at friend@example.com")

            self.assertEqual(len(ctx.send_calls), 1)
            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertNotIn("Operability", field_names)
        finally:
            await cog.service.close()
