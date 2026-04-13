from __future__ import annotations

import asyncio
import json
import discord
import types
import unittest
from datetime import timedelta
from unittest.mock import patch

from babblebox import game_engine as ge
from babblebox.admin_service import AdminService
from babblebox.admin_store import (
    AdminStore,
    _PostgresAdminStore,
    _config_from_row as _admin_config_from_row,
    default_admin_config,
    normalize_admin_config,
    normalize_member_risk_state,
    normalize_verification_state,
)
from babblebox.utility_helpers import deserialize_datetime, serialize_datetime


class FakePermissions:
    def __init__(self, **overrides):
        defaults = {
            "manage_roles": False,
            "kick_members": False,
            "view_channel": True,
            "send_messages": True,
            "embed_links": True,
            "mention_everyone": False,
            "administrator": False,
            "manage_guild": False,
            "ban_members": False,
            "moderate_members": False,
        }
        defaults.update(overrides)
        for name, value in defaults.items():
            setattr(self, name, value)


class FakeRole:
    def __init__(self, role_id: int, *, position: int = 1, mentionable: bool = True):
        self.id = role_id
        self.position = position
        self.mention = f"<@&{role_id}>"
        self.mentionable = mentionable


class FakeSentMessage:
    def __init__(self, message_id: int, payload: dict[str, object]):
        self.id = message_id
        self.embed = payload.get("embed")
        self.view = payload.get("view")
        self.content = payload.get("content")
        self.edits: list[dict[str, object]] = []

    async def edit(self, **kwargs):
        self.edits.append(kwargs)
        if "embed" in kwargs:
            self.embed = kwargs["embed"]
        if "view" in kwargs:
            self.view = kwargs["view"]
        if "content" in kwargs:
            self.content = kwargs["content"]
        return self


class FakeChannel:
    def __init__(self, channel_id: int, *, permissions: FakePermissions | None = None):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self._permissions = permissions or FakePermissions()
        self._messages: dict[int, FakeSentMessage] = {}

    def permissions_for(self, member):
        return self._permissions

    async def send(self, **kwargs):
        message = FakeSentMessage(1000 + len(self.sent), kwargs)
        self.sent.append({**kwargs, "message": message})
        self._messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            raise discord.NotFound(response=None, message="missing")
        return message


class FakeMember:
    def __init__(
        self,
        user_id: int,
        guild,
        *,
        roles=None,
        top_role: FakeRole | None = None,
        bot: bool = False,
        guild_permissions: FakePermissions | None = None,
        joined_at=None,
        created_at=None,
        avatar=None,
        display_name: str | None = None,
    ):
        self.id = user_id
        self.guild = guild
        self.roles = list(roles or [])
        self.top_role = top_role or (self.roles[0] if self.roles else FakeRole(0, position=0))
        self.bot = bot
        self.guild_permissions = guild_permissions or FakePermissions()
        self.mention = f"<@{user_id}>"
        self.display_name = display_name or f"User {user_id}"
        self.joined_at = joined_at or ge.now_utc()
        self.created_at = created_at or ge.now_utc()
        self.avatar = avatar
        self.default_avatar = object()
        self.sent = []
        self.kicked = False

    async def add_roles(self, role, reason=None):
        if role not in self.roles:
            self.roles.append(role)

    async def remove_roles(self, role, reason=None):
        self.roles = [item for item in self.roles if item.id != role.id]

    async def send(self, *, embed=None):
        self.sent.append(embed)

    async def kick(self, reason=None):
        self.kicked = True
        self.guild.members.pop(self.id, None)


class FakeGuild:
    def __init__(self, guild_id: int = 10):
        self.id = guild_id
        self.name = "Guild"
        self.owner_id = 1
        self.members: dict[int, FakeMember] = {}
        self.roles: dict[int, FakeRole] = {}
        self.channels: dict[int, FakeChannel] = {}
        self.me = FakeMember(
            999,
            self,
            roles=[FakeRole(900, position=100)],
            top_role=FakeRole(900, position=100),
            guild_permissions=FakePermissions(manage_roles=True, kick_members=True, view_channel=True, send_messages=True, embed_links=True, mention_everyone=True),
        )

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return self.members.get(user_id)

    def get_role(self, role_id: int):
        return self.roles.get(role_id)

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)


class FakeBot:
    def __init__(self, guild: FakeGuild):
        self.user = types.SimpleNamespace(id=999)
        self._guild = guild

    def get_guild(self, guild_id: int):
        return self._guild if guild_id == self._guild.id else None

    def get_channel(self, channel_id: int):
        return self._guild.get_channel(channel_id)


class AdminStoreNormalizationTests(unittest.TestCase):
    def test_admin_config_defaults_verification_deadline_action_to_auto_kick(self):
        config = normalize_admin_config(10, {})
        self.assertEqual(config["verification_deadline_action"], "auto_kick")
        self.assertEqual(default_admin_config(10)["verification_deadline_action"], "auto_kick")

    def test_admin_config_defaults_member_risk_to_disabled_review(self):
        config = normalize_admin_config(10, {})
        self.assertFalse(config["member_risk_enabled"])
        self.assertEqual(config["member_risk_mode"], "review")
        self.assertEqual(default_admin_config(10)["member_risk_mode"], "review")

    def test_verification_state_normalization_keeps_review_metadata(self):
        normalized = normalize_verification_state(
            {
                "guild_id": 10,
                "user_id": 20,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=7)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=2)),
                "kick_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "warning_sent_at": serialize_datetime(ge.now_utc() - timedelta(days=1)),
                "extension_count": 1,
                "review_pending": True,
                "review_version": 3,
                "review_message_channel_id": 50,
                "review_message_id": 75,
                "last_result_code": "kick:blocked:missing_kick_members",
                "last_result_at": serialize_datetime(ge.now_utc() - timedelta(minutes=2)),
                "last_notified_code": "kick:blocked:missing_kick_members",
                "last_notified_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
            }
        )
        self.assertIsNotNone(normalized)
        self.assertTrue(normalized["review_pending"])
        self.assertEqual(normalized["review_version"], 3)
        self.assertEqual(normalized["review_message_channel_id"], 50)
        self.assertEqual(normalized["review_message_id"], 75)
        self.assertEqual(normalized["last_result_code"], "kick:blocked:missing_kick_members")
        self.assertEqual(normalized["last_notified_code"], "kick:blocked:missing_kick_members")

    def test_member_risk_state_normalization_prioritizes_high_signal_codes(self):
        normalized = normalize_member_risk_state(
            {
                "guild_id": 10,
                "user_id": 20,
                "first_seen_at": serialize_datetime(ge.now_utc() - timedelta(hours=3)),
                "last_seen_at": serialize_datetime(ge.now_utc()),
                "snooze_until": None,
                "risk_level": "review",
                "signal_codes": [
                    "name_separator_heavy",
                    "default_avatar",
                    "account_new_7d",
                    "campaign_path_shape",
                    "campaign_lure_reuse",
                    "scam_high",
                    "malicious_link",
                    "first_message_link",
                    "first_external_link",
                    "newcomer_first_messages_risky",
                    "fresh_campaign_cluster_3",
                    "name_zero_width",
                ],
                "primary_domain": "mint-pass.live",
                "review_pending": True,
                "review_version": 1,
            }
        )

        self.assertIsNotNone(normalized)
        self.assertEqual(
            normalized["signal_codes"][:6],
            [
                "malicious_link",
                "scam_high",
                "fresh_campaign_cluster_3",
                "campaign_lure_reuse",
                "campaign_path_shape",
                "newcomer_first_messages_risky",
            ],
        )
        self.assertNotIn("name_separator_heavy", normalized["signal_codes"])

    def test_postgres_config_row_decodes_json_string_id_lists(self):
        config = _admin_config_from_row(
            {
                "guild_id": 10,
                "followup_enabled": True,
                "followup_role_id": 70,
                "followup_mode": "review",
                "followup_duration_value": 30,
                "followup_duration_unit": "days",
                "verification_enabled": True,
                "verification_role_id": 80,
                "verification_logic": "must_have_role",
                "verification_deadline_action": "auto_kick",
                "verification_kick_after_seconds": 604800,
                "verification_warning_lead_seconds": 86400,
                "verification_help_channel_id": 60,
                "verification_help_extension_seconds": 86400,
                "verification_max_extensions": 1,
                "admin_log_channel_id": 50,
                "admin_alert_role_id": 90,
                "warning_template": None,
                "kick_template": None,
                "invite_link": None,
                "excluded_user_ids": json.dumps([11, 12, 12]),
                "excluded_role_ids": json.dumps([21, 22]),
                "trusted_role_ids": json.dumps([31, 31]),
                "followup_exempt_staff": True,
                "verification_exempt_staff": True,
                "verification_exempt_bots": True,
            }
        )

        self.assertEqual(config["excluded_user_ids"], [11, 12])
        self.assertEqual(config["excluded_role_ids"], [21, 22])
        self.assertEqual(config["trusted_role_ids"], [31])


class _FakeSchemaConnection:
    def __init__(self):
        self.executed: list[str] = []
        self._legacy_columns = {
            "admin_followup_roles": {
                "guild_id",
                "user_id",
                "role_id",
                "assigned_at",
                "due_at",
                "mode",
            },
            "admin_verification_states": {
                "guild_id",
                "user_id",
                "joined_at",
                "warning_at",
                "kick_at",
                "warning_sent_at",
                "extension_count",
            },
        }

    async def execute(self, statement: str, *args):
        self.executed.append(statement)
        if statement == "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE":
            self._legacy_columns["admin_followup_roles"].add("review_pending")
        elif statement == "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL":
            self._legacy_columns["admin_followup_roles"].add("review_message_id")
        elif statement == "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE":
            self._legacy_columns["admin_verification_states"].add("review_pending")
        elif statement == "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL":
            self._legacy_columns["admin_verification_states"].add("review_message_id")
        elif statement == "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_result_code TEXT NULL":
            self._legacy_columns["admin_verification_states"].add("last_result_code")
        elif statement == "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ NULL":
            self._legacy_columns["admin_verification_states"].add("last_notified_at")
        elif statement == "CREATE INDEX IF NOT EXISTS ix_admin_followup_review_pending ON admin_followup_roles (review_pending, review_message_id)":
            missing = {"review_pending", "review_message_id"} - self._legacy_columns["admin_followup_roles"]
            if missing:
                raise AssertionError(f"follow-up review index created before legacy columns were backfilled: {sorted(missing)}")
        elif statement == "CREATE INDEX IF NOT EXISTS ix_admin_verification_review_pending ON admin_verification_states (review_pending, review_message_id)":
            missing = {"review_pending", "review_message_id"} - self._legacy_columns["admin_verification_states"]
            if missing:
                raise AssertionError(f"verification review index created before legacy columns were backfilled: {sorted(missing)}")
        elif statement == "CREATE INDEX IF NOT EXISTS ix_admin_verification_last_notified ON admin_verification_states (guild_id, last_notified_at)":
            missing = {"last_notified_at"} - self._legacy_columns["admin_verification_states"]
            if missing:
                raise AssertionError(f"verification notification index created before legacy columns were backfilled: {sorted(missing)}")


class _FakeAcquireContext:
    def __init__(self, connection: _FakeSchemaConnection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSchemaPool:
    def __init__(self, connection: _FakeSchemaConnection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquireContext(self.connection)


class PostgresAdminStoreSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_schema_backfills_columns_before_creating_indexes(self):
        store = _PostgresAdminStore("postgresql://admin-user:secret@db.example.com:5432/app")
        connection = _FakeSchemaConnection()
        store._pool = _FakeSchemaPool(connection)

        await store._ensure_schema()

        executed = connection.executed
        followup_review_pending_alter = executed.index(
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE"
        )
        followup_review_message_id_alter = executed.index(
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL"
        )
        followup_review_index = executed.index(
            "CREATE INDEX IF NOT EXISTS ix_admin_followup_review_pending ON admin_followup_roles (review_pending, review_message_id)"
        )
        verification_review_pending_alter = executed.index(
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE"
        )
        verification_review_message_id_alter = executed.index(
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL"
        )
        verification_last_result_code_alter = executed.index(
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_result_code TEXT NULL"
        )
        verification_last_notified_at_alter = executed.index(
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ NULL"
        )
        verification_review_index = executed.index(
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_review_pending ON admin_verification_states (review_pending, review_message_id)"
        )
        verification_last_notified_index = executed.index(
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_last_notified ON admin_verification_states (guild_id, last_notified_at)"
        )
        first_index = next(index for index, statement in enumerate(executed) if statement.startswith("CREATE INDEX"))
        last_alter = max(index for index, statement in enumerate(executed) if statement.startswith("ALTER TABLE"))

        self.assertLess(followup_review_pending_alter, followup_review_index)
        self.assertLess(followup_review_message_id_alter, followup_review_index)
        self.assertLess(verification_review_pending_alter, verification_review_index)
        self.assertLess(verification_review_message_id_alter, verification_review_index)
        self.assertLess(verification_last_result_code_alter, verification_last_notified_index)
        self.assertLess(verification_last_notified_at_alter, verification_last_notified_index)
        self.assertGreater(first_index, last_alter)


class AdminServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.log_channel = FakeChannel(50)
        self.help_channel = FakeChannel(60)
        self.guild.channels[self.log_channel.id] = self.log_channel
        self.guild.channels[self.help_channel.id] = self.help_channel
        self.followup_role = FakeRole(70, position=10)
        self.verified_role = FakeRole(80, position=10)
        self.guild.roles[self.followup_role.id] = self.followup_role
        self.guild.roles[self.verified_role.id] = self.verified_role
        self.bot = FakeBot(self.guild)
        self.store = AdminStore(backend="memory")
        await self.store.load()
        self.service = AdminService(self.bot, store=self.store)
        self.service.storage_ready = True

    async def _configure_verification(self, *, with_logs: bool = False, deadline_action: str = "auto_kick"):
        if with_logs:
            ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
            self.assertTrue(ok)
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            deadline_action=deadline_action,
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)

    async def _configure_followup(self, *, with_logs: bool = False):
        if with_logs:
            ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
            self.assertTrue(ok)
        ok, _ = await self.service.set_followup_config(
            self.guild.id,
            enabled=True,
            role_id=self.followup_role.id,
            mode="auto_remove",
            duration_text="30d",
        )
        self.assertTrue(ok)

    async def _configure_member_risk(self, *, with_logs: bool = False, mode: str = "review"):
        if with_logs:
            ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
            self.assertTrue(ok)
        ok, _ = await self.service.set_member_risk_config(self.guild.id, enabled=True, mode=mode)
        self.assertTrue(ok)

    def _member_risk_decision(
        self,
        *codes: str,
        primary_domain: str | None = "mint-pass.live",
        message_codes: tuple[str, ...] | None = None,
        context_codes: tuple[str, ...] | None = None,
        match_class: str | None = None,
        confidence: str | None = None,
        scan_source: str = "new_message",
    ):
        evidence = types.SimpleNamespace(
            signal_codes=tuple(codes),
            message_codes=tuple(message_codes or ()),
            context_codes=tuple(context_codes or ()),
            primary_domain=primary_domain,
            message_match_class=match_class,
            message_confidence=confidence,
            scan_source=scan_source,
        )
        return types.SimpleNamespace(member_risk_evidence=evidence)

    def _dm_forbidden(self):
        return discord.Forbidden(types.SimpleNamespace(status=403, reason="Forbidden", headers={}), "DMs are closed")

    def _make_dm_fail(self, member: FakeMember):
        async def _send(*, embed=None):
            raise self._dm_forbidden()

        member.send = _send

    async def _store_warning_due_state(self, member: FakeMember):
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=8)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "kick_at": serialize_datetime(ge.now_utc() + timedelta(days=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )

    async def _store_kick_due_state(self, member: FakeMember):
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=10)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=3)),
                "kick_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "warning_sent_at": serialize_datetime(ge.now_utc() - timedelta(days=2)),
                "extension_count": 0,
            }
        )

    def _last_log_embed(self):
        return self.log_channel.sent[-1]["embed"]

    def _grouped_outcomes(self, embed) -> str:
        return next(field.value for field in embed.fields if field.name == "Grouped Outcomes")

    def _run_summary(self, embed) -> str:
        return next(field.value for field in embed.fields if field.name == "Run Summary")

    async def _create_verification_review(self, member: FakeMember) -> dict[str, object]:
        await self._configure_verification(with_logs=True, deadline_action="review")
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)
        processed = await self.service._process_due_verification_kicks(ge.now_utc())
        self.assertTrue(processed)
        record = await self.store.fetch_verification_state(self.guild.id, member.id)
        self.assertIsNotNone(record)
        self.assertTrue(record["review_pending"])
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertIsNotNone(queue)
        return record

    async def test_ban_candidate_and_return_assign_followup_role(self):
        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
        self.assertTrue(ok)
        ok, _ = await self.service.set_followup_config(
            self.guild.id,
            enabled=True,
            role_id=self.followup_role.id,
            mode="review",
            duration_text="30d",
        )
        self.assertTrue(ok)
        member = FakeMember(42, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member

        await self.service.handle_member_ban(self.guild, types.SimpleNamespace(id=member.id))
        await self.service.handle_member_join(member)

        self.assertIn(self.followup_role, member.roles)
        self.assertIsNone(await self.store.fetch_ban_candidate(self.guild.id, member.id))
        followup = await self.store.fetch_followup(self.guild.id, member.id)
        self.assertIsNotNone(followup)
        self.assertEqual(followup["role_id"], self.followup_role.id)
        self.assertEqual(len(self.log_channel.sent), 1)

    async def test_duplicate_followup_role_is_not_reassigned(self):
        ok, _ = await self.service.set_followup_config(
            self.guild.id,
            enabled=True,
            role_id=self.followup_role.id,
            mode="review",
            duration_text="30d",
        )
        self.assertTrue(ok)
        member = FakeMember(43, self.guild, roles=[self.followup_role], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_ban_candidate(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "banned_at": serialize_datetime(ge.now_utc()),
                "expires_at": serialize_datetime(ge.now_utc() + timedelta(days=30)),
            }
        )

        await self.service.handle_member_join(member)

        self.assertIsNone(await self.store.fetch_followup(self.guild.id, member.id))

    async def test_due_followup_review_records_message_without_ping(self):
        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
        self.assertTrue(ok)
        ok, _ = await self.service.set_followup_config(
            self.guild.id,
            enabled=True,
            role_id=self.followup_role.id,
            mode="review",
            duration_text="30d",
        )
        self.assertTrue(ok)
        member = FakeMember(44, self.guild, roles=[self.followup_role], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_followup(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "role_id": self.followup_role.id,
                "assigned_at": serialize_datetime(ge.now_utc() - timedelta(days=31)),
                "due_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "mode": "review",
                "review_pending": False,
                "review_version": 0,
                "review_message_channel_id": None,
                "review_message_id": None,
            }
        )

        processed = await self.service._process_due_followups(ge.now_utc())

        self.assertTrue(processed)
        updated = await self.store.fetch_followup(self.guild.id, member.id)
        self.assertTrue(updated["review_pending"])
        self.assertIsNotNone(updated["review_message_id"])
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertIsNone(self.log_channel.sent[0].get("content"))

    async def test_followup_review_remove_clears_role_and_record(self):
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(20, position=20), guild_permissions=FakePermissions(manage_guild=True))
        self.guild.members[actor.id] = actor
        member = FakeMember(45, self.guild, roles=[self.followup_role], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_followup(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "role_id": self.followup_role.id,
                "assigned_at": serialize_datetime(ge.now_utc() - timedelta(days=31)),
                "due_at": serialize_datetime(ge.now_utc()),
                "mode": "review",
                "review_pending": True,
                "review_version": 1,
                "review_message_channel_id": self.log_channel.id,
                "review_message_id": 1234,
            }
        )

        ok, message, _ = await self.service.handle_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=1,
            action="remove",
            actor=actor,
        )

        self.assertTrue(ok)
        self.assertIn("removed", message.lower())
        self.assertNotIn(self.followup_role, member.roles)
        self.assertIsNone(await self.store.fetch_followup(self.guild.id, member.id))

    async def test_verification_help_channel_extends_once(self):
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="2d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        member = FakeMember(46, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member

        await self.service.handle_member_join(member)
        state = await self.store.fetch_verification_state(self.guild.id, member.id)
        original_kick_at = deserialize_datetime(state["kick_at"])

        message = types.SimpleNamespace(
            guild=self.guild,
            author=member,
            content="I need help with verification",
            webhook_id=None,
            channel=self.help_channel,
        )
        await self.service.handle_message(message)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)
        first_kick_at = deserialize_datetime(updated["kick_at"])

        await self.service.handle_message(message)
        final_state = await self.store.fetch_verification_state(self.guild.id, member.id)
        second_kick_at = deserialize_datetime(final_state["kick_at"])

        self.assertGreater(first_kick_at, original_kick_at)
        self.assertEqual(first_kick_at, second_kick_at)
        self.assertEqual(final_state["extension_count"], 1)

    async def test_verification_warning_then_kick(self):
        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
        self.assertTrue(ok)
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        member = FakeMember(47, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=6)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "kick_at": serialize_datetime(ge.now_utc() + timedelta(days=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )

        warned = await self.service._process_due_verification_warnings(ge.now_utc())
        warning_state = await self.store.fetch_verification_state(self.guild.id, member.id)
        self.assertTrue(warned)
        self.assertIsNotNone(warning_state["warning_sent_at"])
        self.assertEqual(len(member.sent), 1)

        warning_state["kick_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        await self.store.upsert_verification_state(warning_state)
        kicked = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(kicked)
        self.assertTrue(member.kicked)
        self.assertIsNone(await self.store.fetch_verification_state(self.guild.id, member.id))
        self.assertEqual(len(member.sent), 2)

    async def test_verification_kick_without_prior_warning_delays_and_warns(self):
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        member = FakeMember(48, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=8)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=1)),
                "kick_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )

        processed = await self.service._process_due_verification_kicks(ge.now_utc())
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)

        self.assertTrue(processed)
        self.assertFalse(member.kicked)
        self.assertIsNotNone(updated["warning_sent_at"])
        self.assertGreater(deserialize_datetime(updated["kick_at"]), ge.now_utc())
        self.assertEqual(len(member.sent), 1)

    async def test_store_lists_verification_review_views(self):
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": 48,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=8)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=1)),
                "kick_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "warning_sent_at": serialize_datetime(ge.now_utc() - timedelta(days=2)),
                "extension_count": 0,
                "review_pending": True,
                "review_version": 2,
                "review_message_channel_id": self.log_channel.id,
                "review_message_id": 1234,
            }
        )

        rows = await self.store.list_verification_review_views()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["review_version"], 2)
        self.assertEqual(rows[0]["review_message_id"], 1234)

    async def test_verification_review_mode_sends_review_message_instead_of_kicking(self):
        member = FakeMember(481, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)

        self.assertFalse(member.kicked)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Verification Review Queue")
        self.assertIsNotNone(self.log_channel.sent[0]["view"])
        self.assertIsNone(self.log_channel.sent[0].get("content"))
        self.assertIsNone(record["review_message_id"])
        self.assertIsNotNone(queue["message_id"])

    async def test_verification_review_due_state_is_not_resent_while_pending(self):
        member = FakeMember(482, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc() + timedelta(minutes=5))

        self.assertFalse(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertFalse(member.kicked)

    async def test_verification_review_without_log_channel_backs_off(self):
        await self._configure_verification(deadline_action="review")
        member = FakeMember(483, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)
        before = ge.now_utc()

        processed = await self.service._process_due_verification_kicks(before)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)

        self.assertTrue(processed)
        self.assertFalse(member.kicked)
        self.assertTrue(updated["review_pending"])
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))
        self.assertLessEqual(deserialize_datetime(updated["kick_at"]), before)

    async def test_verification_review_queue_batches_many_members_into_one_message(self):
        await self._configure_verification(with_logs=True, deadline_action="review")
        for user_id in (490, 491, 492, 493, 494, 495):
            member = FakeMember(user_id, self.guild, roles=[], top_role=FakeRole(5, position=5))
            self.guild.members[member.id] = member
            await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())
        queue = await self.store.fetch_verification_review_queue(self.guild.id)

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Verification Review Queue")
        self.assertIsNone(self.log_channel.sent[0].get("content"))
        self.assertIsNotNone(queue)
        queue_field = next(field.value for field in self.log_channel.sent[0]["embed"].fields if field.name == "Queue")
        preview_field = next(field.value for field in self.log_channel.sent[0]["embed"].fields if field.name == "Backlog Preview")
        self.assertIn("Pending reviews: **6**", queue_field)
        self.assertIn("<@490> - due", preview_field)
        self.assertIn("... and 1 more queued case.", preview_field)

    async def test_verification_review_queue_startup_refresh_reuses_existing_message(self):
        member = FakeMember(496, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)

        restarted = AdminService(self.bot, store=self.store)
        restarted.storage_ready = True
        await restarted._rebuild_config_cache()
        await restarted._refresh_startup_verification_review_queues(now=ge.now_utc())

        self.assertEqual(len(self.log_channel.sent), 1)
        updated_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertEqual(updated_queue["message_id"], queue["message_id"])
        message = await self.log_channel.fetch_message(queue["message_id"])
        self.assertTrue(message.edits)

    async def test_verification_review_queue_appears_immediately_when_log_channel_added(self):
        await self._configure_verification(deadline_action="review")
        member = FakeMember(497, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(processed)
        self.assertTrue((await self.store.fetch_verification_state(self.guild.id, member.id))["review_pending"])
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))

        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)

        self.assertTrue(ok)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertIsNotNone(queue)
        self.assertEqual(queue["channel_id"], self.log_channel.id)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Verification Review Queue")

    async def test_verification_review_queue_same_channel_resave_reuses_existing_message(self):
        member = FakeMember(498, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        message = await self.log_channel.fetch_message(queue["message_id"])

        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)

        self.assertTrue(ok)
        updated_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertEqual(updated_queue["message_id"], queue["message_id"])
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertTrue(message.edits)

    async def test_verification_review_queue_moves_without_duplicate_active_message(self):
        second_channel = FakeChannel(51)
        self.guild.channels[second_channel.id] = second_channel
        member = FakeMember(499, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)
        original_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        original_message = await self.log_channel.fetch_message(original_queue["message_id"])

        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=second_channel.id, alert_role_id=None)

        self.assertTrue(ok)
        updated_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertEqual(updated_queue["channel_id"], second_channel.id)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(len(second_channel.sent), 1)
        self.assertEqual(original_message.view, None)
        self.assertEqual(original_message.embed.title, "Verification Review Queue Moved")
        self.assertEqual(second_channel.sent[0]["embed"].title, "Verification Review Queue")

    async def test_switching_into_review_mode_reconciles_existing_overdue_backlog(self):
        await self._configure_verification(with_logs=True)
        member = FakeMember(500, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)

        ok, _ = await self.service.set_verification_config(self.guild.id, deadline_action="review")

        self.assertTrue(ok)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertTrue(updated["review_pending"])
        self.assertIsNotNone(queue)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Verification Review Queue")

    async def test_switching_out_of_review_mode_retires_queue_and_closes_pending_rows(self):
        member = FakeMember(501, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        message = await self.log_channel.fetch_message(queue["message_id"])

        ok, _ = await self.service.set_verification_config(self.guild.id, deadline_action="auto_kick")

        self.assertTrue(ok)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)
        self.assertFalse(updated["review_pending"])
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))
        self.assertEqual(message.view, None)
        self.assertEqual(message.embed.title, "Verification Review Queue Updated")
        self.assertFalse(member.kicked)

    async def test_disabling_verification_hides_queue_but_reenable_restores_backlog(self):
        member = FakeMember(502, self.guild, roles=[], top_role=FakeRole(5, position=5))
        await self._create_verification_review(member)
        first_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        first_message = await self.log_channel.fetch_message(first_queue["message_id"])

        ok, _ = await self.service.set_verification_config(self.guild.id, enabled=False)

        self.assertTrue(ok)
        hidden = await self.store.fetch_verification_state(self.guild.id, member.id)
        self.assertTrue(hidden["review_pending"])
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))
        self.assertEqual(first_message.view, None)
        self.assertEqual(first_message.embed.title, "Verification Review Queue Updated")

        ok, _ = await self.service.set_verification_config(self.guild.id, enabled=True, deadline_action="review")

        self.assertTrue(ok)
        restored_queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertIsNotNone(restored_queue)
        self.assertEqual(len(self.log_channel.sent), 2)
        self.assertNotEqual(restored_queue["message_id"], first_queue["message_id"])
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Review Queue")

    async def test_verification_review_queue_appears_when_invalid_log_channel_is_fixed(self):
        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=999, alert_role_id=None)
        self.assertTrue(ok)
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            deadline_action="review",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        member = FakeMember(503, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(processed)
        self.assertTrue((await self.store.fetch_verification_state(self.guild.id, member.id))["review_pending"])
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))
        self.assertEqual(len(self.log_channel.sent), 0)

        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)

        self.assertTrue(ok)
        queue = await self.store.fetch_verification_review_queue(self.guild.id)
        self.assertIsNotNone(queue)
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Verification Review Queue")

    async def test_verification_review_kick_action_clears_state_and_logs(self):
        member = FakeMember(484, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(10, position=10), guild_permissions=FakePermissions(manage_guild=True))

        ok, message, _ = await self.service.handle_verification_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=record["review_version"],
            action="kick",
            actor=actor,
        )

        self.assertTrue(ok)
        self.assertIn("kicked", message.lower())
        self.assertTrue(member.kicked)
        self.assertIsNone(await self.store.fetch_verification_state(self.guild.id, member.id))
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Review Kick")
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))

    async def test_verification_review_kick_action_permission_failure_keeps_review_open(self):
        member = FakeMember(485, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(10, position=10), guild_permissions=FakePermissions(manage_guild=True))

        ok, message, _ = await self.service.handle_verification_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=record["review_version"],
            action="kick",
            actor=actor,
        )

        self.assertFalse(ok)
        self.assertIn("Kick Members", message)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)
        self.assertTrue(updated["review_pending"])
        self.assertFalse(member.kicked)

    async def test_verification_review_delay_action_moves_deadline_by_24_hours(self):
        member = FakeMember(486, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(10, position=10), guild_permissions=FakePermissions(manage_guild=True))
        before = ge.now_utc()

        ok, message, _ = await self.service.handle_verification_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=record["review_version"],
            action="delay",
            actor=actor,
        )
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)

        self.assertTrue(ok)
        self.assertIn("24 hours", message)
        self.assertFalse(updated["review_pending"])
        self.assertIsNone(updated["review_message_id"])
        self.assertGreaterEqual(deserialize_datetime(updated["kick_at"]), before + timedelta(hours=23, minutes=59))
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Review Delayed")
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))

    async def test_verification_review_ignore_action_clears_state(self):
        member = FakeMember(487, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(10, position=10), guild_permissions=FakePermissions(manage_guild=True))

        ok, message, _ = await self.service.handle_verification_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=record["review_version"],
            action="ignore",
            actor=actor,
        )

        self.assertTrue(ok)
        self.assertIn("ignored", message.lower())
        self.assertIsNone(await self.store.fetch_verification_state(self.guild.id, member.id))
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Review Ignored")
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))

    async def test_verification_review_stale_action_fails_safely(self):
        member = FakeMember(488, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        actor = FakeMember(2, self.guild, roles=[], top_role=FakeRole(10, position=10), guild_permissions=FakePermissions(manage_guild=True))

        ok, message, current = await self.service.handle_verification_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=record["review_version"] + 1,
            action="delay",
            actor=actor,
        )

        self.assertFalse(ok)
        self.assertIn("stale", message.lower())
        self.assertTrue(current["review_pending"])

    async def test_verification_help_extension_closes_pending_review_and_reschedules(self):
        member = FakeMember(489, self.guild, roles=[], top_role=FakeRole(5, position=5))
        record = await self._create_verification_review(member)
        old_kick_at = deserialize_datetime(record["kick_at"])

        message = types.SimpleNamespace(
            guild=self.guild,
            author=member,
            content="I still need help with verification",
            webhook_id=None,
            channel=self.help_channel,
        )
        await self.service.handle_message(message)
        updated = await self.store.fetch_verification_state(self.guild.id, member.id)

        self.assertFalse(updated["review_pending"])
        self.assertGreater(updated["review_version"], record["review_version"])
        self.assertIsNone(updated["review_message_id"])
        self.assertGreater(deserialize_datetime(updated["kick_at"]), old_kick_at)
        self.assertIsNone(await self.store.fetch_verification_review_queue(self.guild.id))

    async def test_verification_must_not_have_role_marks_role_holder_unverified(self):
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_not_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        member = FakeMember(49, self.guild, roles=[self.verified_role], top_role=FakeRole(5, position=5))
        state, reason = self.service._verification_status(member, self.service.get_compiled_config(self.guild.id))
        self.assertEqual(state, "unverified")
        self.assertIn("still has", reason.lower())

    async def test_build_verification_sync_preview_counts_matches_and_due_warnings(self):
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        unverified = FakeMember(60, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        verified = FakeMember(61, self.guild, roles=[self.verified_role], top_role=FakeRole(5, position=5))
        self.guild.members[unverified.id] = unverified
        self.guild.members[verified.id] = verified
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": verified.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=2)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=1)),
                "kick_at": serialize_datetime(ge.now_utc() + timedelta(days=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )

        preview = await self.service.build_verification_sync_preview(self.guild)

        self.assertEqual(preview.matched_unverified, 1)
        self.assertEqual(preview.newly_tracked, 1)
        self.assertEqual(preview.stale_rows_to_clear, 1)
        self.assertEqual(preview.warnings_due_now, 1)
        self.assertTrue(any("verification-help channel" in check.message for check in preview.prechecks))

    async def test_run_verification_sync_session_processes_due_warnings_and_clears_stale_rows(self):
        ok, _ = await self.service.set_logs_config(self.guild.id, channel_id=self.log_channel.id, alert_role_id=None)
        self.assertTrue(ok)
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        unverified = FakeMember(62, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        verified = FakeMember(63, self.guild, roles=[self.verified_role], top_role=FakeRole(5, position=5))
        self.guild.members[unverified.id] = unverified
        self.guild.members[verified.id] = verified
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": verified.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=2)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(days=1)),
                "kick_at": serialize_datetime(ge.now_utc() + timedelta(days=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )
        preview = await self.service.build_verification_sync_preview(self.guild)
        created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
        self.assertTrue(created)

        with patch("babblebox.admin_service.VERIFICATION_SYNC_DM_PACE_SECONDS", new=0):
            summary = await self.service.run_verification_sync_session(self.guild, session)

        self.assertEqual(summary.scanned_members, 2)
        self.assertEqual(summary.matched_unverified, 1)
        self.assertEqual(summary.tracked_count, 1)
        self.assertEqual(summary.cleared_count, 1)
        self.assertEqual(summary.warned_count, 1)
        self.assertEqual(summary.failed_dm_count, 0)
        self.assertFalse(summary.manually_stopped)
        self.assertEqual(len(unverified.sent), 1)
        updated = await self.store.fetch_verification_state(self.guild.id, unverified.id)
        self.assertIsNotNone(updated)
        self.assertIsNotNone(updated["warning_sent_at"])
        self.assertIsNone(await self.store.fetch_verification_state(self.guild.id, verified.id))
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Sync Complete")

    async def test_run_verification_sync_session_stop_requested_halts_remaining_members(self):
        ok, _ = await self.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.help_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        for user_id in (70, 71, 72):
            self.guild.members[user_id] = FakeMember(
                user_id,
                self.guild,
                roles=[],
                top_role=FakeRole(5, position=5),
                joined_at=ge.now_utc() - timedelta(days=9),
            )
        preview = await self.service.build_verification_sync_preview(self.guild)
        created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
        self.assertTrue(created)

        with patch("babblebox.admin_service.VERIFICATION_SYNC_DM_PACE_SECONDS", new=0.05):
            task = asyncio.create_task(self.service.run_verification_sync_session(self.guild, session))
            await asyncio.sleep(0.01)
            self.assertTrue(await self.service.request_verification_sync_stop(self.guild.id))
            summary = await task

        self.assertTrue(summary.manually_stopped)
        self.assertLess(summary.scanned_members, 3)
        self.assertLess(summary.warned_count, 3)

    async def test_due_warning_sweep_skips_guild_with_active_sync_session(self):
        await self._configure_verification()
        member = FakeMember(80, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self.store.upsert_verification_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "joined_at": serialize_datetime(ge.now_utc() - timedelta(days=8)),
                "warning_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                "kick_at": serialize_datetime(ge.now_utc() + timedelta(days=1)),
                "warning_sent_at": None,
                "extension_count": 0,
            }
        )
        preview = await self.service.build_verification_sync_preview(self.guild)
        created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
        self.assertTrue(created)

        processed = await self.service._process_due_verification_warnings(ge.now_utc())

        self.assertFalse(processed)
        self.assertEqual(len(member.sent), 0)
        await self.service.clear_verification_sync_session(self.guild.id, session)

    async def test_due_verification_warnings_group_failed_dms_into_one_log(self):
        await self._configure_verification(with_logs=True)
        first = FakeMember(81, self.guild, roles=[], top_role=FakeRole(5, position=5))
        second = FakeMember(82, self.guild, roles=[], top_role=FakeRole(5, position=5))
        for member in (first, second):
            self._make_dm_fail(member)
            self.guild.members[member.id] = member
            await self._store_warning_due_state(member)

        processed = await self.service._process_due_verification_warnings(ge.now_utc())

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        embed = self._last_log_embed()
        self.assertEqual(embed.title, "Verification Automation Summary")
        self.assertIn("Warnings sent: **2**", self._run_summary(embed))
        self.assertIn("Warning DMs failed for <@81> and <@82>.", self._grouped_outcomes(embed))

    async def test_due_verification_kicks_group_same_failure_reason_into_one_log(self):
        await self._configure_verification(with_logs=True)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        members = [
            FakeMember(90, self.guild, roles=[], top_role=FakeRole(5, position=5)),
            FakeMember(91, self.guild, roles=[], top_role=FakeRole(5, position=5)),
            FakeMember(92, self.guild, roles=[], top_role=FakeRole(5, position=5)),
        ]
        for member in members:
            self.guild.members[member.id] = member
            await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        embed = self._last_log_embed()
        self.assertEqual(embed.title, "Verification Automation Summary")
        self.assertIn("Kicks blocked: **3**", self._run_summary(embed))
        self.assertIn(
            "<@90>, <@91>, and <@92> were not kicked because Babblebox is missing Kick Members.",
            self._grouped_outcomes(embed),
        )

    async def test_due_verification_kicks_keep_distinct_reasons_separate(self):
        await self._configure_verification(with_logs=True)
        ok, _ = await self.service.set_exemption_toggle(self.guild.id, "verification_exempt_staff", False)
        self.assertTrue(ok)
        admin_member = FakeMember(
            93,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            guild_permissions=FakePermissions(administrator=True),
        )
        high_member = FakeMember(94, self.guild, roles=[], top_role=FakeRole(901, position=150))
        for member in (admin_member, high_member):
            self.guild.members[member.id] = member
            await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        descriptions = self._grouped_outcomes(self._last_log_embed())
        self.assertIn(
            "<@93> was not kicked because they are administrators.",
            descriptions,
        )
        self.assertIn(
            "<@94> was not kicked because their top role is at or above Babblebox's.",
            descriptions,
        )

    async def test_due_verification_kicks_truncate_large_group_member_list(self):
        await self._configure_verification(with_logs=True)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        for user_id in (95, 96, 97, 98, 99):
            member = FakeMember(user_id, self.guild, roles=[], top_role=FakeRole(5, position=5))
            self.guild.members[member.id] = member
            await self._store_kick_due_state(member)

        processed = await self.service._process_due_verification_kicks(ge.now_utc())

        self.assertTrue(processed)
        grouped = self._grouped_outcomes(self._last_log_embed())
        self.assertIn("<@95>, <@96>, <@97>, and 2 more", grouped)
        self.assertNotIn("<@98>", grouped)
        self.assertNotIn("<@99>", grouped)

    async def test_startup_resume_suppresses_identical_blocked_backlog(self):
        await self._configure_verification(with_logs=True)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        for user_id in (120, 121):
            member = FakeMember(user_id, self.guild, roles=[], top_role=FakeRole(5, position=5))
            self.guild.members[member.id] = member
            await self._store_kick_due_state(member)

        processed = await self.service._run_sweep()

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        first_embed = self._last_log_embed()
        self.assertEqual(first_embed.title, "Verification Reconciliation Resumed")
        self.assertIn("Kicks blocked: **2**", self._run_summary(first_embed))
        snapshot = await self.store.fetch_verification_notification_snapshot(
            self.guild.id,
            run_context="startup_resume",
            operation="kick",
            outcome="blocked",
            reason_code="missing_kick_members",
        )
        self.assertIsNotNone(snapshot)
        for user_id in (120, 121):
            record = await self.store.fetch_verification_state(self.guild.id, user_id)
            record["kick_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
            await self.store.upsert_verification_state(record)

        restarted = AdminService(self.bot, store=self.store)
        restarted.storage_ready = True
        await restarted._rebuild_config_cache()
        processed = await restarted._run_sweep()

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        for user_id in (120, 121):
            record = await self.store.fetch_verification_state(self.guild.id, user_id)
            self.assertEqual(record["last_notified_code"], "kick:blocked:missing_kick_members")

    async def test_startup_resume_renotifies_when_blocked_backlog_changes(self):
        await self._configure_verification(with_logs=True)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        member = FakeMember(122, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)
        await self.service._run_sweep()
        existing = await self.store.fetch_verification_state(self.guild.id, member.id)
        existing["kick_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        await self.store.upsert_verification_state(existing)

        newcomer = FakeMember(123, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[newcomer.id] = newcomer
        await self._store_kick_due_state(newcomer)

        restarted = AdminService(self.bot, store=self.store)
        restarted.storage_ready = True
        await restarted._rebuild_config_cache()
        processed = await restarted._run_sweep()

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 2)
        second_embed = self._last_log_embed()
        self.assertEqual(second_embed.title, "Verification Reconciliation Resumed")
        self.assertIn("Kicks blocked: **2**", self._run_summary(second_embed))
        self.assertIn("<@122>", self._grouped_outcomes(second_embed))
        self.assertIn("<@123>", self._grouped_outcomes(second_embed))

    async def test_startup_resume_renotifies_after_suppression_window(self):
        await self._configure_verification(with_logs=True)
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
            mention_everyone=True,
        )
        member = FakeMember(124, self.guild, roles=[], top_role=FakeRole(5, position=5))
        self.guild.members[member.id] = member
        await self._store_kick_due_state(member)
        await self.service._run_sweep()

        record = await self.store.fetch_verification_state(self.guild.id, member.id)
        record["kick_at"] = serialize_datetime(ge.now_utc() - timedelta(minutes=1))
        record["last_notified_at"] = serialize_datetime(ge.now_utc() - timedelta(hours=25))
        await self.store.upsert_verification_state(record)
        snapshot = await self.store.fetch_verification_notification_snapshot(
            self.guild.id,
            run_context="startup_resume",
            operation="kick",
            outcome="blocked",
            reason_code="missing_kick_members",
        )
        snapshot["notified_at"] = serialize_datetime(ge.now_utc() - timedelta(hours=25))
        await self.store.upsert_verification_notification_snapshot(snapshot)

        restarted = AdminService(self.bot, store=self.store)
        restarted.storage_ready = True
        await restarted._rebuild_config_cache()
        processed = await restarted._run_sweep()

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 2)
        self.assertEqual(self._last_log_embed().title, "Verification Reconciliation Resumed")

    async def test_verification_sync_summary_groups_dm_failures_and_skip_reasons(self):
        await self._configure_verification(with_logs=True)
        first = FakeMember(100, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        second = FakeMember(101, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        skipped = FakeMember(102, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        for member in (first, second):
            self._make_dm_fail(member)
        for member in (first, second, skipped):
            self.guild.members[member.id] = member

        original_status = self.service._verification_status

        def fake_status(member, compiled):
            if member.id == skipped.id:
                return "ambiguous", "the configured verification role could not be resolved for this member"
            return original_status(member, compiled)

        with patch.object(self.service, "_verification_status", side_effect=fake_status):
            preview = await self.service.build_verification_sync_preview(self.guild)
            created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
            self.assertTrue(created)
            with patch("babblebox.admin_service.VERIFICATION_SYNC_DM_PACE_SECONDS", new=0):
                summary = await self.service.run_verification_sync_session(self.guild, session)

        self.assertEqual(summary.failed_dm_count, 2)
        self.assertIn("Warning DMs failed for <@100> and <@101> during verification sync.", summary.issues)
        self.assertIn(
            "<@102> was skipped during verification sync because the configured verification role could not be resolved for this member.",
            summary.issues,
        )
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Verification Sync Complete")
        issues_field = next(field for field in self.log_channel.sent[-1]["embed"].fields if field.name == "Issues")
        self.assertIn("Warning DMs failed for <@100> and <@101> during verification sync.", issues_field.value)

    async def test_verification_sync_issue_groups_do_not_cross_merge_between_runs(self):
        await self._configure_verification(with_logs=True)
        first = FakeMember(103, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        second = FakeMember(104, self.guild, roles=[], top_role=FakeRole(5, position=5), joined_at=ge.now_utc() - timedelta(days=9))
        self._make_dm_fail(first)
        self._make_dm_fail(second)
        self.guild.members[first.id] = first

        preview = await self.service.build_verification_sync_preview(self.guild)
        created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
        self.assertTrue(created)
        with patch("babblebox.admin_service.VERIFICATION_SYNC_DM_PACE_SECONDS", new=0):
            first_summary = await self.service.run_verification_sync_session(self.guild, session)

        self.guild.members[second.id] = second
        preview = await self.service.build_verification_sync_preview(self.guild)
        created, session = await self.service.create_verification_sync_session(self.guild, actor_id=2, preview=preview)
        self.assertTrue(created)
        with patch("babblebox.admin_service.VERIFICATION_SYNC_DM_PACE_SECONDS", new=0):
            second_summary = await self.service.run_verification_sync_session(self.guild, session)

        self.assertEqual(first_summary.issues, ("Warning DMs failed for <@103> during verification sync.",))
        self.assertEqual(second_summary.issues, ("Warning DMs failed for <@104> during verification sync.",))

    async def test_due_followup_auto_remove_groups_same_outcome_into_one_log(self):
        await self._configure_followup(with_logs=True)
        first = FakeMember(110, self.guild, roles=[self.followup_role], top_role=FakeRole(5, position=5))
        second = FakeMember(111, self.guild, roles=[self.followup_role], top_role=FakeRole(5, position=5))
        for member in (first, second):
            self.guild.members[member.id] = member
            await self.store.upsert_followup(
                {
                    "guild_id": self.guild.id,
                    "user_id": member.id,
                    "role_id": self.followup_role.id,
                    "assigned_at": serialize_datetime(ge.now_utc() - timedelta(days=31)),
                    "due_at": serialize_datetime(ge.now_utc() - timedelta(minutes=1)),
                    "mode": "auto_remove",
                    "review_pending": False,
                    "review_version": 0,
                    "review_message_channel_id": None,
                    "review_message_id": None,
                }
            )

        processed = await self.service._process_due_followups(ge.now_utc())

        self.assertTrue(processed)
        self.assertEqual(len(self.log_channel.sent), 1)
        embed = self.log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Follow-up Roles Removed")
        self.assertEqual(
            embed.description,
            "Babblebox auto-removed <@&70> from <@110> and <@111> after 30 days.",
        )

    async def test_member_risk_identity_only_logs_note_without_queue(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            201,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=30),
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[member.id] = member

        await self.service.handle_member_join(member)

        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Note")

    async def test_member_risk_default_avatar_alone_does_not_punish(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            202,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(days=60),
            joined_at=ge.now_utc() - timedelta(days=30),
            avatar=None,
        )
        self.guild.members[member.id] = member

        await self.service.handle_member_join(member)

        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertEqual(self.log_channel.sent, [])

    async def test_member_risk_recent_account_alone_does_not_punish(self):
        member = FakeMember(
            2021,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=6),
            joined_at=ge.now_utc() - timedelta(days=3),
            avatar=object(),
            display_name="normal-user",
        )

        assessment = self.service._assess_member_risk(
            member,
            types.SimpleNamespace(signal_codes=(), primary_domain=None),
            now=ge.now_utc(),
        )

        self.assertEqual(assessment.level, "low")
        self.assertIn("account_new_1d", assessment.signal_codes)
        self.assertNotIn("joined_recently", assessment.signal_codes)

    async def test_member_risk_mixed_script_name_alone_does_not_punish(self):
        member = FakeMember(
            2022,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(days=90),
            joined_at=ge.now_utc() - timedelta(days=30),
            avatar=object(),
            display_name="Admіn",
        )

        assessment = self.service._assess_member_risk(
            member,
            types.SimpleNamespace(signal_codes=(), primary_domain=None),
            now=ge.now_utc(),
        )

        self.assertEqual(assessment.level, "low")
        self.assertIn("name_mixed_script", assessment.signal_codes)

    async def test_member_risk_context_only_first_link_does_not_log_or_queue(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            2023,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=4),
            joined_at=ge.now_utc() - timedelta(minutes=15),
            avatar=None,
            display_name="new-user",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("newcomer_early_message", "first_message_link", "first_external_link", "newcomer_first_messages_risky"),
        )

        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertEqual(self.log_channel.sent, [])

    async def test_member_risk_message_creates_review_for_new_account_and_risky_message(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            203,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=4),
            joined_at=ge.now_utc() - timedelta(minutes=20),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_medium", "unknown_suspicious_link", "newcomer_early_message"),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        queue = await self.store.fetch_member_risk_review_queue(self.guild.id)
        self.assertIsNotNone(record)
        self.assertTrue(record["review_pending"])
        self.assertEqual(record["risk_level"], "review")
        self.assertIsNotNone(queue)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Review Queue")

    async def test_member_risk_spam_raid_context_reuses_private_review_queue(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            2035,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=4),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "spam_high",
                "raid_join_wave",
                "raid_fresh_join_wave",
                "raid_pattern_cluster",
                "newcomer_early_message",
                message_codes=("spam_high",),
                context_codes=("raid_join_wave", "raid_fresh_join_wave", "raid_pattern_cluster", "newcomer_early_message"),
                match_class="spam_mention_flood",
                confidence="high",
            ),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        queue = await self.store.fetch_member_risk_review_queue(self.guild.id)
        self.assertIsNotNone(record)
        self.assertEqual(record["latest_message_basis"], "Mention flood")
        self.assertEqual(record["latest_message_confidence"], "high")
        self.assertTrue(record["review_pending"])
        self.assertIsNotNone(queue)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Review Queue")
        self.assertIn("message, join-wave, and account signals", self.log_channel.sent[0]["embed"].description)

    async def test_member_risk_review_persists_latest_message_metadata_and_updates_event_count(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            20301,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=4),
            joined_at=ge.now_utc() - timedelta(minutes=20),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "scam_medium",
                "unknown_suspicious_link",
                "newcomer_early_message",
                primary_domain="mint-pass.live",
                message_codes=("scam_medium", "unknown_suspicious_link"),
                context_codes=("newcomer_early_message",),
                match_class="scam_brand_impersonation",
                confidence="medium",
                scan_source="message_edit",
            ),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        self.assertIsNotNone(record)
        self.assertEqual(record["message_event_count"], 1)
        self.assertEqual(record["latest_message_basis"], "Official-looking brand lure")
        self.assertEqual(record["latest_message_confidence"], "medium")
        self.assertEqual(record["latest_scan_source"], "message_edit")
        risk_field = next(field for field in self.log_channel.sent[0]["embed"].fields if field.name == "Risk")
        self.assertIn("Basis: Official-looking brand lure", risk_field.value)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "scam_high",
                "malicious_link",
                "fresh_campaign_cluster_2",
                primary_domain="secure-auth-session.click",
                message_codes=("scam_high", "malicious_link"),
                context_codes=("fresh_campaign_cluster_2",),
                match_class="known_malicious_domain",
                confidence="high",
                scan_source="new_message",
            ),
        )

        updated = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        self.assertIsNotNone(updated)
        self.assertEqual(updated["message_event_count"], 2)
        self.assertEqual(updated["latest_message_basis"], "Known malicious domain")
        self.assertEqual(updated["latest_message_confidence"], "high")
        self.assertEqual(updated["latest_scan_source"], "new_message")
        activity_field = next(field for field in self.log_channel.sent[0]["message"].embed.fields if field.name == "Activity")
        self.assertIn("Message events: 2 hits", activity_field.value)
        self.assertIn("Latest signal: New message | High confidence", activity_field.value)

    async def test_member_risk_mixed_script_name_with_risky_message_queues_review(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            2031,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=4),
            joined_at=ge.now_utc() - timedelta(minutes=20),
            avatar=None,
            display_name="Admіn Support",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "scam_medium",
                "unknown_suspicious_link",
                "first_external_link",
                "newcomer_first_messages_risky",
            ),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        self.assertIsNotNone(record)
        self.assertIn("name_mixed_script", record["signal_codes"])
        queue_embed = self.log_channel.sent[0]["embed"]
        field_names = [field.name for field in queue_embed.fields]
        self.assertIn("Message Evidence", field_names)
        self.assertIn("Identity Hints", field_names)
        self.assertIn("Escalation Context", field_names)
        self.assertIn("Activity", field_names)

    async def test_member_risk_trusted_role_bypasses_message_lane(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        trusted_role = FakeRole(91, position=12)
        self.guild.roles[trusted_role.id] = trusted_role
        ok, _ = await self.service.set_exclusion_target(self.guild.id, "trusted_role_ids", trusted_role.id, True)
        self.assertTrue(ok)
        member = FakeMember(
            204,
            self.guild,
            roles=[trusted_role],
            top_role=trusted_role,
            created_at=ge.now_utc() - timedelta(hours=3),
            joined_at=ge.now_utc() - timedelta(minutes=15),
            avatar=None,
            display_name="Official Mod",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_high", "malicious_link", "newcomer_early_message"),
        )

        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertIsNone(await self.store.fetch_member_risk_review_queue(self.guild.id))
        self.assertEqual(self.log_channel.sent, [])

    async def test_member_risk_review_or_kick_removes_member_and_includes_rejoin_link(self):
        ok, _ = await self.service.set_templates(self.guild.id, invite_link="https://discord.gg/rejoin")
        self.assertTrue(ok)
        await self._configure_member_risk(with_logs=True, mode="review_or_kick")
        member = FakeMember(
            205,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_high", "malicious_link", "newcomer_early_message", "fresh_campaign_cluster_2"),
        )

        self.assertTrue(member.kicked)
        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertIn("Rejoin: https://discord.gg/rejoin", member.sent[0].description)
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Member Risk Kick")

    async def test_member_risk_review_or_kick_falls_back_to_review_when_dm_fails(self):
        await self._configure_member_risk(with_logs=True, mode="review_or_kick")
        member = FakeMember(
            206,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Official Support",
        )
        self._make_dm_fail(member)
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_high", "malicious_link", "newcomer_early_message", "fresh_campaign_cluster_2"),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        self.assertFalse(member.kicked)
        self.assertIsNotNone(record)
        self.assertTrue(record["review_pending"])
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Review Queue")

    async def test_member_risk_review_or_kick_permission_failure_falls_back_to_review(self):
        await self._configure_member_risk(with_logs=True, mode="review_or_kick")
        self.guild.me.guild_permissions = FakePermissions(
            manage_roles=True,
            kick_members=False,
            view_channel=True,
            send_messages=True,
            embed_links=True,
        )
        member = FakeMember(
            2061,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_high", "malicious_link", "first_external_link", "fresh_campaign_cluster_2"),
        )

        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        self.assertFalse(member.kicked)
        self.assertIsNotNone(record)
        self.assertTrue(record["review_pending"])
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Review Queue")

    async def test_member_risk_log_mode_keeps_strong_cases_private_and_non_destructive(self):
        await self._configure_member_risk(with_logs=True, mode="log")
        member = FakeMember(
            2062,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_high", "malicious_link", "first_external_link", "fresh_campaign_cluster_2"),
        )

        self.assertFalse(member.kicked)
        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))
        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Member Risk Note")
        self.assertIn("Known malicious-link intel", self.log_channel.sent[-1]["embed"].description)

    async def test_member_risk_log_mode_note_uses_precise_message_basis_label(self):
        await self._configure_member_risk(with_logs=True, mode="log")
        member = FakeMember(
            2063,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "scam_medium",
                "unknown_suspicious_link",
                "first_external_link",
                message_codes=("scam_medium", "unknown_suspicious_link"),
                context_codes=("first_external_link",),
                match_class="scam_brand_impersonation",
                confidence="medium",
            ),
        )

        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Member Risk Note")
        self.assertIn("Basis: Official-looking brand lure", self.log_channel.sent[-1]["embed"].description)

    async def test_member_risk_log_mode_note_uses_attachment_only_scam_basis_label(self):
        await self._configure_member_risk(with_logs=True, mode="log")
        member = FakeMember(
            2064,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)

        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision(
                "scam_high",
                "suspicious_attachment",
                message_codes=("scam_high", "suspicious_attachment"),
                match_class="scam_bait_attachment",
                confidence="high",
            ),
        )

        self.assertEqual(self.log_channel.sent[-1]["embed"].title, "Member Risk Note")
        self.assertIn("Basis: Scam bait + suspicious file", self.log_channel.sent[-1]["embed"].description)

    async def test_member_risk_review_action_delay_and_ignore(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            207,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=3),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        self.guild.members[member.id] = member
        message = types.SimpleNamespace(guild=self.guild, author=member, webhook_id=None)
        await self.service.handle_member_risk_message(
            message,
            self._member_risk_decision("scam_medium", "unknown_suspicious_link", "newcomer_early_message"),
        )
        record = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        actor = FakeMember(1, self.guild, guild_permissions=FakePermissions(manage_guild=True))
        self.guild.members[actor.id] = actor

        ok, _message, updated = await self.service.handle_member_risk_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=int(record["review_version"]),
            action="delay",
            actor=actor,
        )
        self.assertTrue(ok)
        self.assertFalse(updated["review_pending"])
        self.assertIsNotNone(updated["snooze_until"])

        delayed = await self.store.fetch_member_risk_state(self.guild.id, member.id)
        delayed["review_pending"] = True
        delayed["review_version"] = int(delayed["review_version"]) + 1
        delayed["snooze_until"] = None
        await self.store.upsert_member_risk_state(delayed)
        ok, _message, _record = await self.service.handle_member_risk_review_action(
            guild_id=self.guild.id,
            user_id=member.id,
            version=int(delayed["review_version"]),
            action="ignore",
            actor=actor,
        )
        self.assertTrue(ok)
        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))

    async def test_member_risk_join_tolerates_missing_avatar_attrs(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            208,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(days=20),
            joined_at=ge.now_utc() - timedelta(days=2),
            display_name="normal-user",
        )
        delattr(member, "avatar")
        delattr(member, "default_avatar")
        self.guild.members[member.id] = member

        await self.service.handle_member_join(member)

        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, member.id))

    async def test_member_risk_note_dedupes_repeated_identity_only_noise(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        member = FakeMember(
            209,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=30),
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[member.id] = member

        await self.service.handle_member_join(member)
        await self.service.handle_member_join(member)

        self.assertEqual(len(self.log_channel.sent), 1)
        field_names = [field.name for field in self.log_channel.sent[0]["embed"].fields]
        self.assertIn("Message Evidence", field_names)
        self.assertIn("Identity Hints", field_names)

    async def test_member_update_identity_only_rechecks_member_risk_without_queueing(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        before = FakeMember(
            211,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=30),
            avatar=object(),
            display_name="normal-user",
        )
        after = FakeMember(
            before.id,
            self.guild,
            roles=[],
            top_role=before.top_role,
            created_at=before.created_at,
            joined_at=before.joined_at,
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[after.id] = after

        await self.service.handle_member_update(before, after)

        self.assertEqual(len(self.log_channel.sent), 1)
        self.assertEqual(self.log_channel.sent[0]["embed"].title, "Member Risk Note")
        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, after.id))

    async def test_member_update_identity_only_noop_does_not_log_noise(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        before = FakeMember(
            212,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(days=90),
            joined_at=ge.now_utc() - timedelta(days=30),
            avatar=object(),
            display_name="normal-user",
        )
        after = FakeMember(
            before.id,
            self.guild,
            roles=[],
            top_role=before.top_role,
            created_at=before.created_at,
            joined_at=before.joined_at,
            avatar=object(),
            display_name="trusted-helper",
        )
        self.guild.members[after.id] = after

        await self.service.handle_member_update(before, after)

        self.assertEqual(self.log_channel.sent, [])
        self.assertIsNone(await self.store.fetch_member_risk_state(self.guild.id, after.id))

    async def test_member_update_identity_only_note_dedupes_repeat_updates(self):
        await self._configure_member_risk(with_logs=True, mode="review")
        before = FakeMember(
            213,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=30),
            avatar=object(),
            display_name="normal-user",
        )
        after = FakeMember(
            before.id,
            self.guild,
            roles=[],
            top_role=before.top_role,
            created_at=before.created_at,
            joined_at=before.joined_at,
            avatar=None,
            display_name="Official Support",
        )
        self.guild.members[after.id] = after

        await self.service.handle_member_update(before, after)
        await self.service.handle_member_update(before, after)

        self.assertEqual(len(self.log_channel.sent), 1)

    def test_member_risk_signal_summary_labels_host_pattern_cleanly(self):
        summary = self.service._member_risk_signal_summary(["campaign_host_family", "campaign_path_shape", "campaign_lure_reuse"])

        self.assertIn("shared risky host pattern", summary)
        self.assertIn("shared risky link shape", summary)
        self.assertIn("reused lure wording", summary)

    async def test_member_risk_does_not_touch_profile_bio_surfaces(self):
        class NoBioMember(FakeMember):
            @property
            def bio(self):
                raise AssertionError("bio should not be inspected")

            @property
            def about_me(self):
                raise AssertionError("about_me should not be inspected")

        member = NoBioMember(
            210,
            self.guild,
            roles=[],
            top_role=FakeRole(5, position=5),
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=30),
            avatar=None,
            display_name="Official Support",
        )

        assessment = self.service._assess_member_risk(
            member,
            types.SimpleNamespace(signal_codes=(), primary_domain=None),
            now=ge.now_utc(),
        )

        self.assertEqual(assessment.level, "note")
