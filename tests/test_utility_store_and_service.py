from __future__ import annotations

import asyncio
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
from babblebox.premium_limits import (
    LIMIT_AFK_SCHEDULES,
    LIMIT_BUMP_DETECTION_CHANNELS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
    guild_limit as premium_guild_limit,
    user_limit as premium_user_limit,
)
from babblebox.premium_models import (
    PLAN_FREE,
    PLAN_GUILD_PRO,
    PLAN_PLUS,
    PLAN_SUPPORTER,
    SYSTEM_PREMIUM_OWNER_USER_IDS,
    SYSTEM_PREMIUM_SUPPORT_GUILD_ID,
)
from babblebox.shield_service import (
    FEATURE_SURFACE_AFK_REASON,
    FEATURE_SURFACE_AFK_SCHEDULE_REASON,
    FEATURE_SURFACE_REMINDER_CREATE,
    FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY,
    FEATURE_SURFACE_WATCH_KEYWORD,
    ShieldFeatureDecision,
    ShieldFeatureSafetyGateway,
)
from babblebox.utility_helpers import (
    build_afk_reason_text,
    build_bump_reminder_embed,
    build_bump_thanks_embed,
    compute_next_afk_schedule_start,
    deserialize_datetime,
    serialize_datetime,
)
from babblebox.utility_service import BUMP_PROVIDER_DISBOARD, UtilityService
from babblebox.utility_store import UtilityStateStore, UtilityStorageUnavailable, _PostgresUtilityStore


USER_PREMIUM_LIMIT_KEYS = {
    LIMIT_WATCH_KEYWORDS,
    LIMIT_WATCH_FILTERS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_AFK_SCHEDULES,
}


class PremiumLimitStub:
    def __init__(self, *, user_plans: Optional[dict[int, str]] = None, guild_plans: Optional[dict[int, str]] = None):
        self.user_plans = dict(user_plans or {})
        self.guild_plans = dict(guild_plans or {})

    def resolve_user_limit(self, user_id: int, limit_key: str) -> int:
        return premium_user_limit(self.user_plans.get(user_id, PLAN_FREE), limit_key)

    def resolve_guild_limit(self, guild_id: int, limit_key: str) -> int:
        return premium_guild_limit(self.guild_plans.get(guild_id, PLAN_FREE), limit_key)

    def describe_limit_error(self, *, limit_key: str, limit_value: int) -> str:
        plan_label = "Babblebox Plus" if limit_key in USER_PREMIUM_LIMIT_KEYS else "Babblebox Guild Pro"
        return (
            f"You reached this plan's active limit of {limit_value}. {plan_label} allows higher active limits. "
            "Use `/premium plans` to compare tiers. Previously saved over-limit state stays preserved."
        )


class VoteBonusStub:
    LIMITS = {
        LIMIT_WATCH_KEYWORDS: 15,
        LIMIT_WATCH_FILTERS: 12,
        LIMIT_REMINDERS_ACTIVE: 5,
        LIMIT_REMINDERS_PUBLIC_ACTIVE: 2,
        LIMIT_AFK_SCHEDULES: 10,
    }

    def __init__(self, *, active_user_ids: Optional[set[int]] = None, configured: bool = True):
        self.active_user_ids = set(active_user_ids or set())
        self.configured = configured

    def resolve_user_limit(self, *, user_id: int, plan_code: str, limit_key: str, current_limit: int) -> int:
        if plan_code not in {PLAN_FREE, "supporter"}:
            return current_limit
        if user_id not in self.active_user_ids:
            return current_limit
        return max(current_limit, self.LIMITS.get(limit_key, current_limit))

    def describe_limit_error(self, *, user_id: int, plan_code: str, limit_key: str, limit_value: int, default_message: str) -> str | None:
        if not self.configured or plan_code not in {PLAN_FREE, "supporter"} or user_id in self.active_user_ids:
            return None
        bonus_limit = self.LIMITS.get(limit_key)
        if bonus_limit is None or bonus_limit <= limit_value:
            return None
        return (
            f"{default_message} Use `/vote` to unlock a temporary Vote Bonus up to {bonus_limit}. "
            "Babblebox Plus still goes higher permanently."
        )


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id


class DummyTarget(DummyUser):
    def __init__(self, user_id: int, *, bot: bool = False, display_name: Optional[str] = None):
        super().__init__(user_id)
        self.bot = bot
        self.display_name = display_name or f"User {user_id}"


class DummyMember(DummyTarget):
    def __init__(
        self,
        user_id: int,
        *,
        bot: bool = False,
        display_name: Optional[str] = None,
        mention_everyone: bool = False,
    ):
        super().__init__(user_id, bot=bot, display_name=display_name)
        self.mention = f"<@{user_id}>"
        self.send = AsyncMock()
        self.guild_permissions = types.SimpleNamespace(mention_everyone=mention_everyone)


class DummyRole:
    def __init__(self, role_id: int, *, name: str = "Role", mentionable: bool = True):
        self.id = role_id
        self.name = name
        self.mention = f"<@&{role_id}>"
        self.mentionable = mentionable


class DummyChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        guild,
        name: str = "general",
        visible_user_ids: Optional[set[int]] = None,
        can_send: bool = True,
        can_embed: bool = True,
    ):
        self.id = channel_id
        self.guild = guild
        self.name = name
        self.mention = f"#{name}"
        self._visible_user_ids = set(visible_user_ids or set())
        self._can_send = can_send
        self._can_embed = can_embed
        self.sent = []

    def permissions_for(self, member):
        allowed = getattr(member, "id", None) in self._visible_user_ids
        return types.SimpleNamespace(
            view_channel=allowed,
            read_message_history=allowed,
            send_messages=allowed and self._can_send,
            embed_links=allowed and self._can_embed,
        )

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))


class DummyGuild:
    def __init__(
        self,
        guild_id: int,
        *,
        name: str = "Guild",
        members: Optional[list[DummyMember]] = None,
        channels: Optional[list[DummyChannel]] = None,
        roles: Optional[list[DummyRole]] = None,
    ):
        self.id = guild_id
        self.name = name
        self._members = {member.id: member for member in (members or [])}
        self._channels = {channel.id: channel for channel in (channels or [])}
        self._roles = {role.id: role for role in (roles or [])}
        self.me = None

    def get_member(self, user_id: int):
        return self._members.get(user_id)

    def add_member(self, member):
        self._members[member.id] = member
        return member

    def add_channel(self, channel):
        self._channels[channel.id] = channel
        return channel

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def add_role(self, role):
        self._roles[role.id] = role
        return role

    def get_role(self, role_id: int):
        return self._roles.get(role_id)


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
        embeds: Optional[list[discord.Embed]] = None,
        interaction_metadata=None,
        interaction=None,
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
        self.embeds = list(embeds or [])
        self.interaction_metadata = interaction_metadata
        self.interaction = interaction
        self.webhook_id = None


class DummyBot:
    def __init__(self):
        self._users = {}
        self._channels = {}
        self._guilds = {}
        self.user = DummyUser(9990)

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

    def add_guild(self, guild):
        self._guilds[guild.id] = guild

    def get_guild(self, guild_id: int):
        return self._guilds.get(guild_id)


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
    def __init__(self, *, watch_rows=None, later_rows=None, reminder_rows=None, bump_config_rows=None, bump_cycle_rows=None):
        self._watch_rows = watch_rows or []
        self._later_rows = later_rows or []
        self._reminder_rows = reminder_rows or []
        self._bump_config_rows = bump_config_rows or []
        self._bump_cycle_rows = bump_cycle_rows or []

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
        if "FROM utility_bump_configs" in query:
            return self._bump_config_rows
        if "FROM utility_bump_cycles" in query:
            return self._bump_cycle_rows
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

    def _attach_premium(self, *, user_plans: Optional[dict[int, str]] = None, guild_plans: Optional[dict[int, str]] = None):
        self.bot.premium_service = PremiumLimitStub(user_plans=user_plans, guild_plans=guild_plans)
        self.service._rebuild_watch_indexes()

    def _attach_vote_bonus(self, *, active_user_ids: Optional[set[int]] = None, configured: bool = True):
        self.bot.vote_service = VoteBonusStub(active_user_ids=active_user_ids, configured=configured)
        self.service._rebuild_watch_indexes()

    async def _configure_bump_fixture(
        self,
        *,
        thanks_mode: str = "quiet",
        reminder_role: bool = False,
        role_mentionable: bool = True,
        bot_can_ping_role: bool = False,
        detection_can_embed: bool = True,
        reminder_can_send: bool = True,
        reminder_can_embed: bool = True,
    ):
        bot_member = DummyMember(
            self.bot.user.id,
            bot=True,
            display_name="Babblebox",
            mention_everyone=bot_can_ping_role,
        )
        bumper = DummyMember(7001, display_name="Mira")
        provider = DummyMember(302050872383242240, bot=True, display_name="DISBOARD")
        guild = DummyGuild(901, members=[bot_member, bumper, provider])
        guild.me = bot_member
        detection_channel = DummyChannel(
            902,
            guild=guild,
            name="bump-here",
            visible_user_ids={bot_member.id, bumper.id, provider.id},
            can_send=True,
            can_embed=detection_can_embed,
        )
        reminder_channel = DummyChannel(
            903,
            guild=guild,
            name="bump-reminders",
            visible_user_ids={bot_member.id, bumper.id, provider.id},
            can_send=reminder_can_send,
            can_embed=reminder_can_embed,
        )
        guild.add_channel(detection_channel)
        guild.add_channel(reminder_channel)
        role = None
        if reminder_role:
            role = DummyRole(904, name="Bump Squad", mentionable=role_mentionable)
            guild.add_role(role)
        self.bot.add_user(bot_member)
        self.bot.add_user(bumper)
        self.bot.add_user(provider)
        self.bot.add_channel(detection_channel)
        self.bot.add_channel(reminder_channel)
        self.bot.add_guild(guild)
        ok, result = await self.service.configure_bump(
            guild.id,
            enabled=True,
            provider=BUMP_PROVIDER_DISBOARD,
            detection_channel_ids=[detection_channel.id],
            reminder_channel_id=reminder_channel.id,
            reminder_role_id=role.id if role is not None else None,
            thanks_mode=thanks_mode,
        )
        self.assertTrue(ok, result)
        return guild, detection_channel, reminder_channel, bumper, provider, role

    def _make_disboard_message(
        self,
        *,
        guild: DummyGuild,
        channel: DummyChannel,
        provider: DummyMember,
        bumper: Optional[DummyUser],
        message_id: int,
        content: str = "",
        embeds: Optional[list[discord.Embed]] = None,
        created_at=None,
        interaction_style: str = "metadata",
    ) -> DummyMessage:
        interaction_metadata = None
        interaction = None
        if bumper is not None and interaction_style == "metadata":
            interaction_metadata = types.SimpleNamespace(user=bumper)
        elif bumper is not None and interaction_style == "interaction":
            interaction = types.SimpleNamespace(user=bumper)
        return DummyMessage(
            message_id=message_id,
            author=provider,
            guild=guild,
            channel=channel,
            content=content,
            created_at=created_at or ge.now_utc(),
            embeds=embeds,
            interaction_metadata=interaction_metadata,
            interaction=interaction,
        )

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

    async def test_watch_keywords_upgrade_from_free_to_plus_without_resetting_saved_state(self):
        user_id = 4401
        for index in range(10):
            ok, _ = await self.service.add_watch_keyword(
                user_id,
                guild_id=None,
                channel_id=None,
                phrase=f"topic-{index}",
                scope="global",
                mode="contains",
            )
            self.assertTrue(ok)

        ok, message = await self.service.add_watch_keyword(
            user_id,
            guild_id=None,
            channel_id=None,
            phrase="topic-10",
            scope="global",
            mode="contains",
        )
        self.assertFalse(ok)
        self.assertIn("store up to 10 watch keywords", message)

        self._attach_premium(user_plans={user_id: PLAN_PLUS})
        ok, result = await self.service.add_watch_keyword(
            user_id,
            guild_id=None,
            channel_id=None,
            phrase="topic-10",
            scope="global",
            mode="contains",
        )
        self.assertTrue(ok, result)
        self.assertEqual(self.service.watch_keyword_limit(user_id), 25)
        self.assertEqual(self.service.get_watch_summary(user_id, guild_id=None)["total_keywords"], 11)

    async def test_plus_reminders_can_grow_past_free_limit(self):
        user = DummyUser(4402)
        for index in range(3):
            ok, record = await self.service.create_reminder(
                user=user,
                text=f"Reminder {index}",
                delay_seconds=20 * 60,
                delivery="dm",
                guild=None,
                channel=None,
                origin_jump_url=None,
            )
            self.assertTrue(ok, record)
            self.service._reminder_cooldowns[user.id] = 0.0

        ok, message = await self.service.create_reminder(
            user=user,
            text="Reminder 3",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )
        self.assertFalse(ok)
        self.assertIn("keep up to 3 active reminders", message)

        self._attach_premium(user_plans={user.id: PLAN_PLUS})
        ok, record = await self.service.create_reminder(
            user=user,
            text="Reminder 3",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=None,
            channel=None,
            origin_jump_url=None,
        )
        self.assertTrue(ok, record)
        self.assertEqual(self.service.reminder_limit(user.id), 15)

    async def test_vote_bonus_limits_apply_only_to_free_and_supporter(self):
        self._attach_vote_bonus(active_user_ids={8801, 8802})

        self.assertEqual(self.service.watch_keyword_limit(8801), 15)
        self.assertEqual(self.service.watch_filter_limit(8801), 12)
        self.assertEqual(self.service.reminder_limit(8801), 5)
        self.assertEqual(self.service.public_reminder_limit(8801), 2)
        self.assertEqual(self.service.afk_schedule_limit(8801), 10)

        self._attach_premium(user_plans={8802: PLAN_SUPPORTER, 8803: PLAN_PLUS})
        self.assertEqual(self.service.watch_keyword_limit(8802), 15)
        self.assertEqual(self.service.watch_keyword_limit(8803), 25)
        self.assertEqual(self.service.public_reminder_limit(8803), 5)

    async def test_vote_bonus_copy_surfaces_only_when_bonus_is_available_but_inactive(self):
        user_id = 8804
        self._attach_vote_bonus(configured=True)
        for index in range(10):
            ok, _ = await self.service.add_watch_keyword(
                user_id,
                guild_id=None,
                channel_id=None,
                phrase=f"topic-{index}",
                scope="global",
                mode="contains",
            )
            self.assertTrue(ok)

        ok, message = await self.service.add_watch_keyword(
            user_id,
            guild_id=None,
            channel_id=None,
            phrase="topic-10",
            scope="global",
            mode="contains",
        )
        self.assertFalse(ok)
        self.assertIn("/vote", message)
        self.assertIn("temporary Vote Bonus", message)
        self.assertIn("Babblebox Plus", message)

    async def test_vote_bonus_allows_second_public_reminder_but_not_third(self):
        user = DummyUser(8805)
        self._attach_vote_bonus(active_user_ids={user.id})

        for text in ("First public reminder.", "Second public reminder."):
            ok, result = await self.service.create_reminder(
                user=user,
                text=text,
                delay_seconds=20 * 60,
                delivery="here",
                guild=type("Guild", (), {"id": 1, "name": "Guild"})(),
                channel=type("Channel", (), {"id": 2, "name": "general"})(),
                origin_jump_url=None,
            )
            self.assertTrue(ok, result)
            self.service._reminder_cooldowns[user.id] = 0.0

        ok, message = await self.service.create_reminder(
            user=user,
            text="Third public reminder.",
            delay_seconds=25 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": 1, "name": "Guild"})(),
            channel=type("Channel", (), {"id": 2, "name": "general"})(),
            origin_jump_url=None,
        )
        self.assertFalse(ok)
        self.assertIn("only 2 active channel reminder", message)

    async def test_system_owner_limit_fallback_applies_without_attached_premium_runtime(self):
        owner_user_id = next(iter(SYSTEM_PREMIUM_OWNER_USER_IDS))
        self.assertEqual(self.service.watch_keyword_limit(owner_user_id), 25)
        self.assertEqual(self.service.reminder_limit(owner_user_id), 15)
        self.assertEqual(self.service.afk_schedule_limit(owner_user_id), 20)

    async def test_support_guild_limit_fallback_applies_without_attached_premium_runtime(self):
        self.assertEqual(self.service.bump_detection_channel_limit(SYSTEM_PREMIUM_SUPPORT_GUILD_ID), 15)

    async def test_watch_filters_preserve_saved_state_but_only_active_subset_blocks_runtime(self):
        user_id = 4403
        self._attach_premium(user_plans={user_id: PLAN_PLUS})
        for ignored_user_id in range(7000, 7010):
            ok, message = await self.service.add_watch_ignored_user(user_id, ignored_user_id=ignored_user_id)
            self.assertTrue(ok, message)

        self._attach_premium()
        self.service._rebuild_watch_indexes()
        summary = self.service.get_watch_summary(user_id, guild_id=None)
        self.assertEqual(len(summary["ignored_user_ids"]), 10)
        self.assertEqual(len(self.service._ignored_users_by_user[user_id]), 8)
        self.assertTrue(self.service._watch_filters_block(user_id, author_id=7000, channel_id=1))
        self.assertFalse(self.service._watch_filters_block(user_id, author_id=7009, channel_id=1))

        ok, message = await self.service.add_watch_ignored_user(user_id, ignored_user_id=7010)
        self.assertFalse(ok)
        self.assertIn("active limit of 8", message)
        self.assertIn("/premium plans", message)
        self.assertIn("stays preserved", message)
        self.assertEqual(len(self.service.get_watch_summary(user_id, guild_id=None)["ignored_user_ids"]), 10)

    async def test_downgraded_saved_reminders_keep_all_rows_but_only_due_subset_runs(self):
        user = DummyUser(4404)
        self._attach_premium(user_plans={user.id: PLAN_PLUS})
        now = ge.now_utc()
        for index in range(5):
            self.service.store.state.setdefault("reminders", {})[f"rem-{index}"] = {
                "id": f"rem-{index}",
                "user_id": user.id,
                "text": f"Reminder {index}",
                "delivery": "dm",
                "created_at": serialize_datetime(now),
                "due_at": serialize_datetime(now),
                "guild_id": None,
                "guild_name": None,
                "channel_id": None,
                "channel_name": None,
                "origin_jump_url": None,
                "delivery_attempts": 0,
                "last_attempt_at": None,
                "retry_after": None,
            }

        self._attach_premium()
        due_reminders, _due_bump_cycles, _afk_to_activate, _afk_to_expire, _afk_schedule_candidates, _next_due = self.service._collect_due_records()

        self.assertEqual(len(self.service.list_reminders(user.id)), 5)
        self.assertEqual([record["id"] for record in due_reminders], ["rem-0", "rem-1", "rem-2"])

    async def test_downgraded_saved_afk_schedules_keep_all_rows_but_only_active_subset_triggers(self):
        user = DummyUser(4405)
        self._attach_premium(user_plans={user.id: PLAN_PLUS})
        now = ge.now_utc()
        for index in range(8):
            self.service.store.state.setdefault("afk_schedules", {})[f"afk-{index}"] = {
                "id": f"afk-{index}",
                "user_id": user.id,
                "status": "scheduled",
                "reason": None,
                "preset": None,
                "repeat": "daily",
                "timezone": "UTC",
                "weekday_mask": 0,
                "local_hour": index % 24,
                "local_minute": 0,
                "duration_seconds": 3600,
                "created_at": serialize_datetime(now),
                "next_start_at": serialize_datetime(now),
            }

        self._attach_premium()
        _due_reminders, _due_bump_cycles, _afk_to_activate, _afk_to_expire, afk_schedule_candidates, _next_due = self.service._collect_due_records()

        self.assertEqual(len(self.service.list_afk_schedules(user.id)), 8)
        self.assertEqual(len(afk_schedule_candidates), 6)
        self.assertEqual([record["id"] for record in afk_schedule_candidates], [f"afk-{index}" for index in range(6)])

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
        _args, kwargs = channel.sent[0]
        self.assertIsNotNone(kwargs.get("embed"))
        self.assertIsNone(kwargs.get("view"))
        self.assertIsNotNone(kwargs.get("allowed_mentions"))

    async def test_public_reminder_persists_origin_jump_url_from_server_message(self):
        guild = type("Guild", (), {"id": 1, "name": "Guild"})()
        channel = type("Channel", (), {"id": 2, "name": "general"})()

        ok, reminder = await self.service.create_reminder(
            user=DummyUser(580),
            text="Check the thread.",
            delay_seconds=20 * 60,
            delivery="here",
            guild=guild,
            channel=channel,
            origin_jump_url="https://discord.com/channels/1/2/3",
        )

        self.assertTrue(ok)
        self.assertEqual(reminder["origin_jump_url"], "https://discord.com/channels/1/2/3")

    async def test_public_reminder_delivery_includes_jump_button_when_posted_in_channel(self):
        user = DummyMember(582, display_name="Ari")
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
            origin_jump_url="https://discord.com/channels/1/2/3",
        )

        self.assertTrue(ok)

        delivered = await self.service._deliver_single_reminder(reminder)

        self.assertTrue(delivered)
        self.assertEqual(len(channel.sent), 1)
        _args, kwargs = channel.sent[0]
        self.assertIsNotNone(kwargs.get("view"))
        self.assertEqual(kwargs["view"].children[0].label, "Jump to Message")
        user.send.assert_not_awaited()

    async def test_dm_reminder_keeps_origin_without_showing_jump_button(self):
        user = DummyMember(581, display_name="Ari")
        self.bot.add_user(user)
        guild = type("Guild", (), {"id": 1, "name": "Guild"})()

        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Deliver quietly.",
            delay_seconds=20 * 60,
            delivery="dm",
            guild=guild,
            channel=None,
            origin_jump_url="https://discord.com/channels/1/2/3",
        )

        self.assertTrue(ok)
        self.assertEqual(reminder["origin_jump_url"], "https://discord.com/channels/1/2/3")

        delivered = await self.service._deliver_single_reminder(reminder)

        self.assertTrue(delivered)
        user.send.assert_awaited_once()
        self.assertIsNone(user.send.await_args.kwargs.get("view"))

    async def test_public_reminder_fallback_dm_never_shows_jump_button(self):
        user = DummyMember(583, display_name="Ari")
        guild = DummyGuild(1, members=[user])
        channel = DummyChannel(2, guild=guild, visible_user_ids={user.id})
        response = types.SimpleNamespace(status=403, reason="Forbidden", text="Forbidden")
        channel.send = AsyncMock(side_effect=discord.Forbidden(response=response, message="missing perms"))
        self.bot.add_user(user)
        self.bot.add_channel(channel)
        ok, reminder = await self.service.create_reminder(
            user=user,
            text="Fallback to DM.",
            delay_seconds=20 * 60,
            delivery="here",
            guild=type("Guild", (), {"id": guild.id, "name": guild.name})(),
            channel=type("Channel", (), {"id": channel.id, "name": channel.name})(),
            origin_jump_url="https://discord.com/channels/1/2/3",
        )

        self.assertTrue(ok)

        delivered = await self.service._deliver_single_reminder(reminder)

        self.assertTrue(delivered)
        user.send.assert_awaited_once()
        self.assertIsNone(user.send.await_args.kwargs.get("view"))

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

        due_reminders, _, _, _, _, _ = self.service._collect_due_records()
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

        due_reminders, _, _, _, _, next_due = self.service._collect_due_records()

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

        due_reminders, _, _, _, _, _ = self.service._collect_due_records()
        await self.service._deliver_due_reminders(due_reminders)

        user.send.assert_awaited_once()
        self.assertNotIn(reminder["id"], self.service.store.state["reminders"])

    async def test_bump_success_detection_creates_cycle_from_verified_provider_message(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        event_time = ge.now_utc()
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8801,
            content="Bump done.",
            created_at=event_time,
        )

        await self.service.handle_bump_provider_message(message)

        cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["last_provider_event_kind"], "success")
        self.assertEqual(cycle["last_success_message_id"], message.id)
        self.assertEqual(cycle["last_success_channel_id"], detection_channel.id)
        self.assertEqual(cycle["last_bumper_user_id"], bumper.id)
        self.assertEqual(cycle["last_bump_at"], serialize_datetime(event_time))
        self.assertEqual(cycle["due_at"], serialize_datetime(event_time + timedelta(hours=2)))
        self.assertEqual(len(detection_channel.sent), 1)
        _args, kwargs = detection_channel.sent[0]
        self.assertEqual(kwargs["delete_after"], 15.0)
        self.assertIs(kwargs["reference"], message)
        self.assertIsNotNone(kwargs.get("embed"))

    async def test_bump_thanks_text_accepts_three_normal_sentences_within_new_limit(self):
        guild, _detection_channel, _reminder_channel, _bumper, _provider, _role = await self._configure_bump_fixture()
        text = (
            "Thanks for keeping our listing active today. "
            "That extra visibility helps new people find the server. "
            "Babblebox will quietly watch for the next verified window."
        )

        ok, result = await self.service.configure_bump(guild.id, thanks_text=text)

        self.assertTrue(ok, result)
        self.assertEqual(result["thanks_text"], text)

    async def test_bump_thanks_text_rejects_four_sentences(self):
        guild, _detection_channel, _reminder_channel, _bumper, _provider, _role = await self._configure_bump_fixture()
        text = "One sentence. Two sentence. Three sentence. Four sentence."

        ok, message = await self.service.configure_bump(guild.id, thanks_text=text)

        self.assertFalse(ok)
        self.assertIn("at most 3 short sentences", message.lower())

    async def test_bump_success_detection_accepts_embed_only_variant(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        embed = discord.Embed(title="DISBOARD", description="You can bump again right now.")
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8802,
            embeds=[embed],
        )

        await self.service.handle_bump_provider_message(message)

        cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)
        self.assertEqual(cycle["last_success_message_id"], message.id)
        self.assertEqual(cycle["last_bumper_user_id"], bumper.id)

    async def test_bump_webhook_style_success_embed_variant_creates_cycle_and_reminder(self):
        guild, detection_channel, reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        embed = discord.Embed(
            title="DISBOARD: The Public Server List",
            description="Bump done! 👍\nCheck it out on DISBOARD.",
        )
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=88021,
            content="",
            embeds=[embed],
        )
        message.webhook_id = 991

        self.assertTrue(self.service.is_bump_provider_message_candidate(message))

        await self.service.handle_bump_provider_message(message)

        cycle_id = f"{guild.id}:{BUMP_PROVIDER_DISBOARD}"
        cycle = self.service.store.state["bump_cycles"][cycle_id]
        self.assertEqual(cycle["last_provider_event_kind"], "success")
        self.assertEqual(cycle["last_success_message_id"], message.id)
        self.assertEqual(len(detection_channel.sent), 1)

        cycle["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        _due_reminders, due_bump_cycles, _, _, _, _ = self.service._collect_due_records()
        self.assertEqual(len(due_bump_cycles), 1)

        await self.service._deliver_due_bump_cycles(due_bump_cycles)

        self.assertEqual(len(reminder_channel.sent), 1)
        _args, kwargs = reminder_channel.sent[0]
        self.assertIsNotNone(kwargs.get("embed"))

    async def test_unrelated_bot_messages_do_not_start_bump_cycle(self):
        guild, detection_channel, _reminder_channel, bumper, _provider, _role = await self._configure_bump_fixture()
        other_bot = DummyMember(5555, bot=True, display_name="OtherBot")
        message = DummyMessage(
            message_id=8803,
            author=other_bot,
            guild=guild,
            channel=detection_channel,
            content="Bump done.",
            created_at=ge.now_utc(),
            interaction_metadata=types.SimpleNamespace(user=bumper),
        )

        await self.service.handle_bump_provider_message(message)

        self.assertIsNone(self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD))
        self.assertEqual(detection_channel.sent, [])

    async def test_repeated_bump_success_message_does_not_duplicate_cycle_or_thanks(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8804,
            content="Bump done.",
        )

        await self.service.handle_bump_provider_message(message)
        first_cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)
        await self.service.handle_bump_provider_message(message)
        second_cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)

        self.assertEqual(first_cycle["last_success_message_id"], second_cycle["last_success_message_id"])
        self.assertEqual(len(detection_channel.sent), 1)

    async def test_bump_cooldown_message_updates_health_without_starting_timer(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8805,
            content="Please wait another 1 hour before the next bump.",
        )

        await self.service.handle_bump_provider_message(message)

        cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)
        self.assertIsNotNone(cycle)
        self.assertEqual(cycle["last_provider_event_kind"], "cooldown")
        self.assertIsNone(cycle["last_bump_at"])
        self.assertIsNone(cycle["due_at"])
        self.assertEqual(detection_channel.sent, [])

    def test_bremind_status_embed_uses_premium_minimal_sections(self):
        bot_member = DummyMember(9990, bot=True, display_name="Babblebox")
        bumper = DummyMember(601, display_name="Mira")
        role = DummyRole(604, name="Bump Squad", mentionable=True)
        guild = DummyGuild(600, members=[bot_member, bumper], roles=[role])
        guild.me = bot_member
        detection_channel = DummyChannel(602, guild=guild, name="partnerships", visible_user_ids={bot_member.id, bumper.id})
        reminder_channel = DummyChannel(603, guild=guild, name="reminders", visible_user_ids={bot_member.id, bumper.id})
        guild.add_channel(detection_channel)
        guild.add_channel(reminder_channel)
        self.bot.add_guild(guild)
        self.bot.add_channel(detection_channel)
        self.bot.add_channel(reminder_channel)
        self.service.store.state["bump_configs"][str(guild.id)] = {
            "guild_id": guild.id,
            "enabled": True,
            "provider": BUMP_PROVIDER_DISBOARD,
            "detection_channel_ids": [detection_channel.id],
            "reminder_channel_id": reminder_channel.id,
            "reminder_role_id": role.id,
            "reminder_text": "The next Disboard window is open.",
            "thanks_text": "Thanks for the verified bump. Babblebox will keep an eye on the next window.",
            "thanks_mode": "quiet",
        }
        self.service.store.state["bump_cycles"][f"{guild.id}:{BUMP_PROVIDER_DISBOARD}"] = {
            "id": f"{guild.id}:{BUMP_PROVIDER_DISBOARD}",
            "guild_id": guild.id,
            "provider": BUMP_PROVIDER_DISBOARD,
            "last_provider_event_at": serialize_datetime(ge.now_utc() - timedelta(minutes=5)),
            "last_provider_event_kind": "success",
            "last_bump_at": serialize_datetime(ge.now_utc() - timedelta(minutes=5)),
            "last_bumper_user_id": bumper.id,
            "last_success_message_id": 7000,
            "last_success_channel_id": detection_channel.id,
            "due_at": serialize_datetime(ge.now_utc() + timedelta(hours=2)),
            "reminder_sent_at": None,
            "delivery_attempts": 0,
            "last_delivery_attempt_at": None,
            "retry_after": None,
            "last_delivery_error": None,
        }
        cog = object.__new__(UtilityCog)
        cog.bot = self.bot
        cog.service = self.service

        embed = UtilityCog._bremind_status_embed(cog, guild)

        self.assertEqual(embed.title, "Babblebox Bump Reminders")
        self.assertIn("Disboard is the only supported provider", embed.description)
        field_names = [field.name for field in embed.fields]
        self.assertEqual(field_names, ["Setup", "Copy", "Current Cycle", "Provider Health", "Delivery Notes"])
        self.assertIn("Provider: **Disboard**", embed.fields[0].value)
        self.assertIn("Reminder destination", embed.fields[0].value)
        self.assertIn("Last verified bump", embed.fields[2].value)
        self.assertIn("Last provider event", embed.fields[3].value)
        self.assertIn("No blockers detected", embed.fields[4].value)

    async def test_due_bump_cycle_delivers_once_and_marks_cycle_sent(self):
        guild, detection_channel, reminder_channel, bumper, provider, role = await self._configure_bump_fixture(reminder_role=True)
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8806,
            content="Bump done.",
        )
        await self.service.handle_bump_provider_message(message)
        detection_channel.sent.clear()
        cycle_id = f"{guild.id}:{BUMP_PROVIDER_DISBOARD}"
        cycle = self.service.store.state["bump_cycles"][cycle_id]
        cycle["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        due_reminders, due_bump_cycles, _, _, _, _ = self.service._collect_due_records()
        self.assertEqual(due_reminders, [])
        self.assertEqual(len(due_bump_cycles), 1)
        await self.service._deliver_due_bump_cycles(due_bump_cycles)

        stored = self.service.store.state["bump_cycles"][cycle_id]
        self.assertIsNotNone(stored["reminder_sent_at"])
        self.assertIsNone(stored["retry_after"])
        self.assertEqual(stored["delivery_attempts"], 1)
        self.assertEqual(len(reminder_channel.sent), 1)
        _args, kwargs = reminder_channel.sent[0]
        self.assertEqual(kwargs["content"], role.mention)
        self.assertIsNotNone(kwargs["embed"])

        due_reminders, due_bump_cycles, _, _, _, _ = self.service._collect_due_records()
        self.assertEqual(due_reminders, [])
        self.assertEqual(due_bump_cycles, [])

    async def test_due_bump_cycle_retries_when_reminder_delivery_fails(self):
        guild, detection_channel, reminder_channel, bumper, provider, _role = await self._configure_bump_fixture()
        response = types.SimpleNamespace(status=403, reason="Forbidden", text="Forbidden")
        reminder_channel.send = AsyncMock(side_effect=discord.Forbidden(response=response, message="missing perms"))
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8807,
            content="Bump done.",
        )
        await self.service.handle_bump_provider_message(message)
        cycle_id = f"{guild.id}:{BUMP_PROVIDER_DISBOARD}"
        self.service.store.state["bump_cycles"][cycle_id]["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        _due_reminders, due_bump_cycles, _, _, _, _ = self.service._collect_due_records()
        await self.service._deliver_due_bump_cycles(due_bump_cycles)

        stored = self.service.store.state["bump_cycles"][cycle_id]
        self.assertIsNone(stored["reminder_sent_at"])
        self.assertEqual(stored["delivery_attempts"], 1)
        self.assertIsNotNone(stored["last_delivery_attempt_at"])
        self.assertGreater(deserialize_datetime(stored["retry_after"]), ge.now_utc())
        self.assertIn("rejected", stored["last_delivery_error"].lower())

    async def test_due_bump_cycle_falls_back_to_plain_text_when_embed_links_missing(self):
        guild, detection_channel, reminder_channel, bumper, provider, _role = await self._configure_bump_fixture(reminder_can_embed=False)
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8808,
            content="Bump done.",
        )
        await self.service.handle_bump_provider_message(message)
        cycle_id = f"{guild.id}:{BUMP_PROVIDER_DISBOARD}"
        self.service.store.state["bump_cycles"][cycle_id]["due_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))

        _due_reminders, due_bump_cycles, _, _, _, _ = self.service._collect_due_records()
        await self.service._deliver_due_bump_cycles(due_bump_cycles)

        self.assertEqual(len(reminder_channel.sent), 1)
        _args, kwargs = reminder_channel.sent[0]
        self.assertIsNone(kwargs.get("embed"))
        self.assertIn("bump window is open", kwargs["content"].lower())

    async def test_bump_public_thanks_mode_stays_public_without_auto_delete(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture(thanks_mode="public")
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8809,
            content="Bump done.",
            interaction_style="interaction",
        )

        await self.service.handle_bump_provider_message(message)

        self.assertEqual(len(detection_channel.sent), 1)
        _args, kwargs = detection_channel.sent[0]
        self.assertNotIn("delete_after", kwargs)
        self.assertIsNotNone(kwargs.get("embed"))
        self.assertEqual(kwargs["embed"].title, "Disboard bump confirmed")

    async def test_bump_thanks_mode_off_sends_no_confirmation_message(self):
        guild, detection_channel, _reminder_channel, bumper, provider, _role = await self._configure_bump_fixture(thanks_mode="off")
        message = self._make_disboard_message(
            guild=guild,
            channel=detection_channel,
            provider=provider,
            bumper=bumper,
            message_id=8810,
            content="Bump done.",
        )

        await self.service.handle_bump_provider_message(message)

        cycle = self.service.get_bump_cycle(guild.id, provider=BUMP_PROVIDER_DISBOARD)
        self.assertEqual(cycle["last_success_message_id"], message.id)
        self.assertEqual(detection_channel.sent, [])

    async def test_bump_operability_surfaces_degraded_permissions_and_role_ping_limits(self):
        guild, _detection_channel, _reminder_channel, _bumper, _provider, _role = await self._configure_bump_fixture(
            reminder_role=True,
            role_mentionable=False,
            bot_can_ping_role=False,
            detection_can_embed=False,
            reminder_can_embed=False,
        )

        lines = self.service.get_bump_operability(guild)

        self.assertTrue(any("thank-you messages" in line.lower() and "plain text" in line.lower() for line in lines))
        self.assertTrue(any("reminders" in line.lower() and "plain text" in line.lower() for line in lines))
        self.assertTrue(any("post without that role mention" in line.lower() for line in lines))

    def test_bump_preview_helpers_use_polished_titles(self):
        cycle = {
            "due_at": serialize_datetime(ge.now_utc()),
            "last_bump_at": serialize_datetime(ge.now_utc() - timedelta(hours=2)),
            "last_bumper_user_id": 777,
        }

        reminder_embed = build_bump_reminder_embed(
            provider_label="Disboard",
            reminder_text="The next listing window is ready.",
            cycle=cycle,
            delayed=True,
        )
        thanks_embed = build_bump_thanks_embed(
            provider_label="Disboard",
            thanks_text="Thanks for keeping the server visible.",
            bumper_name="Mira",
        )

        self.assertEqual(reminder_embed.title, "Disboard bump window is open")
        self.assertEqual(reminder_embed.fields[0].name, "Provider")
        self.assertEqual(reminder_embed.fields[1].name, "Window opened")
        self.assertEqual(reminder_embed.fields[2].name, "Delivery")
        self.assertEqual(thanks_embed.title, "Disboard bump confirmed")
        self.assertEqual(thanks_embed.fields[0].name, "Provider")

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

        _, _, afk_to_activate, _, _, _ = self.service._collect_due_records()
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

        _, _, _, afk_to_expire, _, _ = self.service._collect_due_records()
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

    async def test_plus_afk_schedule_limit_expands_without_affecting_existing_free_baseline(self):
        user = DummyUser(4404)
        for index in range(6):
            ok, schedule = await self.service.create_afk_schedule(
                user=user,
                repeat="weekly",
                timezone_name="UTC+04:00",
                local_hour=9,
                local_minute=index,
                weekday=index % 7,
                reason=build_afk_reason_text(preset="working", custom_reason=f"Shift {index}"),
                preset="working",
                duration_seconds=8 * 3600,
            )
            self.assertTrue(ok, schedule)

        ok, message = await self.service.create_afk_schedule(
            user=user,
            repeat="weekly",
            timezone_name="UTC+04:00",
            local_hour=10,
            local_minute=0,
            weekday=6,
            reason=build_afk_reason_text(preset="working", custom_reason="Overflow"),
            preset="working",
            duration_seconds=8 * 3600,
        )
        self.assertFalse(ok)
        self.assertIn("keep up to 6 recurring AFK schedules", message)

        self._attach_premium(user_plans={user.id: PLAN_PLUS})
        ok, schedule = await self.service.create_afk_schedule(
            user=user,
            repeat="weekly",
            timezone_name="UTC+04:00",
            local_hour=10,
            local_minute=0,
            weekday=6,
            reason=build_afk_reason_text(preset="working", custom_reason="Overflow"),
            preset="working",
            duration_seconds=8 * 3600,
        )
        self.assertTrue(ok, schedule)
        self.assertEqual(self.service.afk_schedule_limit(user.id), 20)

    async def test_guild_pro_bump_detection_limit_allows_more_channels(self):
        guild, detection_channel, reminder_channel, _bumper, provider, _role = await self._configure_bump_fixture()
        extra_channels = []
        for channel_id in range(904, 909):
            channel = DummyChannel(
                channel_id,
                guild=guild,
                name=f"bump-{channel_id}",
                visible_user_ids={guild.me.id},
            )
            guild.add_channel(channel)
            self.bot.add_channel(channel)
            extra_channels.append(channel)

        detection_ids = [detection_channel.id, *[channel.id for channel in extra_channels]]
        ok, message = await self.service.configure_bump(
            guild.id,
            provider=BUMP_PROVIDER_DISBOARD,
            detection_channel_ids=detection_ids,
            reminder_channel_id=reminder_channel.id,
        )
        self.assertFalse(ok)
        self.assertIn("keep up to 5 bump detection channels", message)

        self._attach_premium(guild_plans={guild.id: PLAN_GUILD_PRO})
        ok, result = await self.service.configure_bump(
            guild.id,
            provider=BUMP_PROVIDER_DISBOARD,
            detection_channel_ids=detection_ids,
            reminder_channel_id=reminder_channel.id,
        )
        self.assertTrue(ok, result)
        self.assertEqual(self.service.bump_detection_channel_limit(guild.id), 15)

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

        _, _, _, _, afk_schedule_candidates, _ = self.service._collect_due_records()
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

    async def test_postgres_reload_sanitizes_later_attachment_labels_and_preserves_other_state(self):
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
                    "attachment_labels": json.dumps(
                        [
                            "image.png (https://cdn.example/image.png)",
                            "https://cdn.example/clip.mp4",
                            "notes.pdf - https://cdn.example/notes.pdf",
                        ]
                    ),
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
            bump_config_rows=[
                {
                    "guild_id": 10,
                    "enabled": True,
                    "provider": BUMP_PROVIDER_DISBOARD,
                    "detection_channel_ids": json.dumps([20, 21]),
                    "reminder_channel_id": 22,
                    "reminder_role_id": 23,
                    "reminder_text": "Window is open.",
                    "thanks_text": "Thanks for the bump.",
                    "thanks_mode": "public",
                }
            ],
            bump_cycle_rows=[
                {
                    "id": "10:disboard",
                    "guild_id": 10,
                    "provider": BUMP_PROVIDER_DISBOARD,
                    "last_provider_event_at": now - timedelta(minutes=2),
                    "last_provider_event_kind": "success",
                    "last_bump_at": now - timedelta(minutes=2),
                    "last_bumper_user_id": 7,
                    "last_success_message_id": 30,
                    "last_success_channel_id": 20,
                    "due_at": now + timedelta(hours=2),
                    "reminder_sent_at": None,
                    "delivery_attempts": 1,
                    "last_delivery_attempt_at": now - timedelta(minutes=1),
                    "retry_after": now + timedelta(minutes=9),
                    "last_delivery_error": "Discord rejected the bump reminder text delivery.",
                }
            ],
        )
        store = _PostgresUtilityStore("postgresql://utility-user:secret@db.example.com:5432/app")
        store._pool = FakePool(connection)

        await store._reload_from_db()

        watch = store.state["watch"]["1"]
        marker = store.state["later"]["1"]["20"]
        reminder = store.state["reminders"]["reminder1"]
        bump_config = store.state["bump_configs"]["10"]
        bump_cycle = store.state["bump_cycles"]["10:disboard"]
        self.assertTrue(watch["mention_global"])
        self.assertEqual(watch["mention_guild_ids"], [10, 11])
        self.assertEqual(watch["mention_channel_ids"], [20])
        self.assertEqual(watch["reply_guild_ids"], [12])
        self.assertEqual(watch["reply_channel_ids"], [21, 22])
        self.assertEqual(watch["excluded_channel_ids"], [99])
        self.assertEqual(watch["ignored_user_ids"], [5])
        self.assertEqual(marker["attachment_labels"], ["image.png", "attachment", "notes.pdf"])
        self.assertEqual(reminder["delivery_attempts"], 2)
        self.assertEqual(reminder["retry_after"], serialize_datetime(now + timedelta(minutes=10)))
        self.assertEqual(bump_config["detection_channel_ids"], [20, 21])
        self.assertEqual(bump_config["thanks_mode"], "public")
        self.assertEqual(bump_cycle["last_bumper_user_id"], 7)
        self.assertEqual(bump_cycle["retry_after"], serialize_datetime(now + timedelta(minutes=9)))

    async def test_save_later_marker_stores_compact_attachment_labels_only(self):
        user = DummyMember(64, display_name="Mira")
        guild = DummyGuild(10, members=[user])
        channel = DummyChannel(20, guild=guild, visible_user_ids={user.id})
        message = DummyMessage(
            message_id=30,
            author=user,
            guild=guild,
            channel=channel,
            content="Saved message",
            created_at=ge.now_utc(),
        )
        message.attachments = [
            types.SimpleNamespace(filename="clip.png", url="https://cdn.example/clip.png", content_type="image/png"),
            types.SimpleNamespace(filename="", url="https://cdn.example/raw.bin", content_type="application/octet-stream"),
        ]

        ok, marker = await self.service.save_later_marker(user=user, channel=channel, message=message)

        self.assertTrue(ok)
        self.assertEqual(marker["attachment_labels"], ["clip.png", "attachment"])
        self.assertNotIn("https://", " ".join(marker["attachment_labels"]))
        stored_marker = self.store.state["later"][str(user.id)][str(channel.id)]
        self.assertEqual(stored_marker["attachment_labels"], ["clip.png", "attachment"])

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

    async def test_watch_settings_embed_reports_saved_state_above_current_plan(self):
        user_id = 6601
        self._attach_premium(user_plans={user_id: PLAN_PLUS})
        for index in range(12):
            ok, message = await self.service.add_watch_keyword(
                user_id,
                guild_id=None,
                channel_id=None,
                phrase=f"camera-{index}",
                scope="global",
                mode="contains",
            )
            self.assertTrue(ok, message)
        for ignored_user_id in range(8000, 8010):
            ok, message = await self.service.add_watch_ignored_user(user_id, ignored_user_id=ignored_user_id)
            self.assertTrue(ok, message)

        self._attach_premium()
        self.service._rebuild_watch_indexes()
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda _user_id: None)
        cog = UtilityCog(bot)
        original_service = cog.service
        try:
            cog.service = self.service
            user = DummyTarget(user_id, display_name="Mira")

            embed = cog._watch_settings_embed(user, guild=None, channel=None)

            fields = {field.name: field.value for field in embed.fields}
            self.assertIn("Saved total: **12**", fields["Keyword Buckets"])
            self.assertIn("Active on this plan: **10 / 10**", fields["Keyword Buckets"])
            self.assertIn("Saved Above Current Plan", fields)
            self.assertIn("Keywords: saved **12** | active on this plan **10 / 10**", fields["Saved Above Current Plan"])
            self.assertIn("Ignored users: saved **10** | active on this plan **8 / 8**", fields["Saved Above Current Plan"])
            self.assertIn("stays preserved", fields["Saved Above Current Plan"])
        finally:
            await original_service.close()

    async def test_reminder_list_embed_reports_saved_state_above_current_plan(self):
        user = DummyUser(6602)
        now = ge.now_utc()
        self._attach_premium(user_plans={user.id: PLAN_PLUS})
        for index in range(4):
            self.service.store.state.setdefault("reminders", {})[f"rem-{index}"] = {
                "id": f"rem-{index}",
                "user_id": user.id,
                "text": f"Reminder {index}",
                "delivery": "here" if index < 2 else "dm",
                "created_at": serialize_datetime(now),
                "due_at": serialize_datetime(now + timedelta(minutes=index)),
                "guild_id": None,
                "guild_name": None,
                "channel_id": None,
                "channel_name": None,
                "origin_jump_url": None,
                "delivery_attempts": 0,
                "last_attempt_at": None,
                "retry_after": None,
            }
        self._attach_premium()

        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_channel=self.bot.get_channel)
        cog = UtilityCog(bot)
        original_service = cog.service
        try:
            cog.service = self.service
            embed = cog._reminder_list_embed(DummyTarget(user.id, display_name="Mira"), self.service.list_reminders(user.id))
            fields = {field.name: field.value for field in embed.fields}

            self.assertIn("Saved Above Current Plan", fields)
            self.assertIn("Reminders: saved **4** | active on this plan **3 / 3**", fields["Saved Above Current Plan"])
            self.assertIn("Channel reminders: saved **2** | active on this plan **1 / 1**", fields["Saved Above Current Plan"])
            self.assertIn("stays preserved", fields["Saved Above Current Plan"])
        finally:
            await original_service.close()

    async def test_bremind_status_embed_reports_saved_detection_channels_above_current_plan(self):
        bot_member = DummyMember(9991, bot=True, display_name="Babblebox")
        guild = DummyGuild(91, members=[bot_member])
        guild.me = bot_member
        for channel_id in range(201, 207):
            channel = DummyChannel(channel_id, guild=guild, visible_user_ids={guild.me.id})
            guild.add_channel(channel)
            self.bot.add_channel(channel)

        self._attach_premium(guild_plans={guild.id: PLAN_GUILD_PRO})
        ok, result = await self.service.configure_bump(
            guild.id,
            detection_channel_ids=[201, 202, 203, 204, 205, 206],
        )
        self.assertTrue(ok, result)
        self._attach_premium()

        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_channel=self.bot.get_channel)
        cog = UtilityCog(bot)
        original_service = cog.service
        try:
            cog.service = self.service
            embed = cog._bremind_status_embed(guild)
            fields = {field.name: field.value for field in embed.fields}

            self.assertIn("Saved Above Current Plan", fields)
            self.assertIn("Detection channels: saved **6** | active on this plan **5 / 5**", fields["Saved Above Current Plan"])
            self.assertIn("stays preserved", fields["Saved Above Current Plan"])
        finally:
            await original_service.close()

    def test_later_list_embed_uses_compact_marker_blocks(self):
        cog = object.__new__(UtilityCog)
        user = DummyTarget(61, display_name="Mira")
        embed = UtilityCog._later_list_embed(
            cog,
            user,
            [
                {
                    "guild_name": "Guild",
                    "channel_name": "clips",
                    "saved_at": serialize_datetime(ge.now_utc() - timedelta(minutes=5)),
                    "author_name": "Ari",
                    "preview": "Quiet note\nMedia: [video: clip.mp4]",
                }
            ],
            guild=None,
        )

        self.assertIn("**Guild / #clips** |", embed.fields[0].value)
        self.assertIn("By Ari", embed.fields[0].value)
        self.assertIn("`/later mark` refreshes this channel", embed.fields[1].value)

    async def test_send_later_marker_dm_uses_clear_saved_message_button(self):
        user = DummyMember(62, display_name="Mira")
        marker = {
            "guild_name": "Guild",
            "channel_name": "clips",
            "author_name": "Ari",
            "saved_at": serialize_datetime(ge.now_utc()),
            "message_created_at": serialize_datetime(ge.now_utc()),
            "preview": "Saved preview",
            "attachment_labels": ["clip.png"],
            "message_jump_url": "https://discord.com/channels/1/2/3",
        }

        await self.service.send_later_marker_dm(user, marker)

        user.send.assert_awaited_once()
        kwargs = user.send.await_args.kwargs
        self.assertEqual(kwargs["view"].children[0].label, "Open Saved Message")
        self.assertEqual(kwargs["embed"].fields[0].name, "Location")

    async def test_send_capture_dm_preserves_transcript_and_uses_shorter_return_button(self):
        user = DummyMember(63, display_name="Mira")
        guild = DummyGuild(10, members=[user])
        channel = DummyChannel(20, guild=guild, visible_user_ids={user.id})
        first = DummyMessage(
            message_id=30,
            author=user,
            guild=guild,
            channel=channel,
            content="First line",
            created_at=ge.now_utc() - timedelta(minutes=2),
        )
        first.attachments = [types.SimpleNamespace(filename="clip.png", url="https://cdn.example/clip.png", content_type="image/png")]
        second = DummyMessage(
            message_id=31,
            author=user,
            guild=guild,
            channel=channel,
            content="Second line",
            created_at=ge.now_utc() - timedelta(minutes=1),
        )

        await self.service.send_capture_dm(
            user=user,
            guild_name=guild.name,
            channel_name=channel.name,
            messages=[first, second],
            requested_count=10,
        )

        user.send.assert_awaited_once()
        kwargs = user.send.await_args.kwargs
        self.assertEqual(kwargs["embed"].title, "Capture Ready")
        self.assertEqual(kwargs["view"].children[0].label, "Back to Channel")
        self.assertEqual(kwargs["file"].filename, "babblebox-capture-general.txt")
        transcript = kwargs["file"].fp.getvalue().decode("utf-8")
        self.assertIn("Attachment: clip.png - https://cdn.example/clip.png", transcript)

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
