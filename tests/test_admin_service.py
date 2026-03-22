from __future__ import annotations

import asyncio
import types
import unittest
from datetime import timedelta
from unittest.mock import patch

from babblebox import game_engine as ge
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore
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


class FakeChannel:
    def __init__(self, channel_id: int, *, permissions: FakePermissions | None = None):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self._permissions = permissions or FakePermissions()

    def permissions_for(self, member):
        return self._permissions

    async def send(self, **kwargs):
        self.sent.append(kwargs)
        return types.SimpleNamespace(id=1000 + len(self.sent))


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
    ):
        self.id = user_id
        self.guild = guild
        self.roles = list(roles or [])
        self.top_role = top_role or (self.roles[0] if self.roles else FakeRole(0, position=0))
        self.bot = bot
        self.guild_permissions = guild_permissions or FakePermissions()
        self.mention = f"<@{user_id}>"
        self.display_name = f"User {user_id}"
        self.joined_at = joined_at or ge.now_utc()
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

    async def test_due_followup_review_sends_alert_and_records_message(self):
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
