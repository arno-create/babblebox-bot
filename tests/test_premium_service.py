import asyncio
import json
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from babblebox.premium_limits import CAPABILITY_SHIELD_AI_REVIEW, LIMIT_BUMP_DETECTION_CHANNELS, LIMIT_WATCH_KEYWORDS
from babblebox.premium_models import (
    LINK_STATUS_BROKEN,
    LINK_STATUS_REVOKED,
    MANUAL_KIND_BLOCK,
    MANUAL_KIND_GRANT,
    PLAN_FREE,
    PLAN_GUILD_PRO,
    PLAN_PLUS,
    PLAN_SUPPORTER,
    PROVIDER_PATREON,
    PatreonIdentity,
    SCOPE_GUILD,
    SCOPE_USER,
    SYSTEM_PREMIUM_OWNER_USER_IDS,
    SYSTEM_PREMIUM_SUPPORT_GUILD_ID,
)
from babblebox.premium_crypto import PremiumCryptoError
from babblebox.premium_provider_patreon import PatreonPremiumProvider
from babblebox.premium_provider import PremiumProviderError, WebhookVerificationError
from babblebox.premium_service import PatreonWebhookResult, PremiumService
from babblebox.premium_store import PremiumStorageUnavailable, PremiumStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


class FakePatreonProvider:
    def __init__(self):
        self.identity = PatreonIdentity(
            provider_user_id="patreon-user-1",
            email="premium@example.com",
            display_name="Premium Patron",
            member_id="member-1",
            plan_codes=(PLAN_PLUS,),
            patron_status="active_patron",
            tier_ids=("tier-plus",),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-1"},
            raw_member={"id": "member-1"},
            raw_tiers=(),
        )
        self.fetch_access_tokens: list[str] = []
        self.refresh_tokens: list[str] = []

    def configured(self) -> bool:
        return True

    def automation_ready(self) -> bool:
        return True

    def configuration_errors(self) -> tuple[str, ...]:
        return ()

    def configuration_message(self) -> str:
        return "Patreon premium is configured."

    def build_authorize_url(self, *, state_token: str) -> str:
        return f"https://patreon.test/oauth?state={state_token}"

    async def exchange_code(self, *, code: str) -> dict:
        return {
            "access_token": f"access-{code}",
            "refresh_token": f"refresh-{code}",
            "expires_in": 3600,
            "scope": "identity identity[email] identity.memberships",
        }

    async def refresh_access_token(self, *, refresh_token: str) -> dict:
        self.refresh_tokens.append(refresh_token)
        return {
            "access_token": f"refreshed-{refresh_token}",
            "refresh_token": refresh_token,
            "expires_in": 3600,
            "scope": "identity identity[email] identity.memberships",
        }

    async def fetch_identity(self, *, access_token: str) -> PatreonIdentity:
        self.fetch_access_tokens.append(access_token)
        return self.identity

    def entitlement_timestamps(self, *, identity: PatreonIdentity):
        now = _utcnow()
        return now + timedelta(hours=24), now + timedelta(days=7), identity.next_charge_date

    def verify_webhook(self, *, body: bytes, signature: str, secret: str) -> None:
        if signature != "ok" or secret != "secret":
            raise WebhookVerificationError("bad signature")

    def scopes_from_token_payload(self, payload: dict) -> tuple[str, ...]:
        raw = str(payload.get("scope") or "").strip()
        return tuple(sorted(part for part in raw.split() if part))

    async def close(self):
        return None


class FakeGuildPermissions:
    def __init__(self, *, administrator: bool = False, manage_guild: bool = False):
        self.administrator = administrator
        self.manage_guild = manage_guild


class FakeGuildMember:
    def __init__(self, user_id: int, *, guild=None, administrator: bool = False, manage_guild: bool = False):
        self.id = user_id
        self.guild = guild
        self.guild_permissions = FakeGuildPermissions(administrator=administrator, manage_guild=manage_guild)


class FakeGuild:
    def __init__(self, guild_id: int, *, members: Optional[list[FakeGuildMember]] = None):
        self.id = guild_id
        self._members: dict[int, FakeGuildMember] = {}
        for member in members or []:
            self.add_member(member)

    def add_member(self, member: FakeGuildMember):
        member.guild = self
        self._members[member.id] = member
        return member

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class PremiumServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.provider = FakePatreonProvider()
        self.store = PremiumStore(backend="memory")
        self.bot = types.SimpleNamespace(loop=None)
        self.service = PremiumService(self.bot, store=self.store, provider=self.provider)
        started = await self.service.start()
        self.assertTrue(started)

    async def asyncTearDown(self):
        await self.service.close()

    async def _create_state(self, *, user_id: int, state_token: str):
        now = _utcnow()
        await self.service.store.create_oauth_state(
            {
                "provider": PROVIDER_PATREON,
                "state_token": self.service._hash_oauth_state_token(state_token),
                "discord_user_id": user_id,
                "action": "link",
                "created_at": _serialize_datetime(now),
                "expires_at": _serialize_datetime(now + timedelta(minutes=15)),
                "consumed_at": None,
                "metadata": {},
            }
        )

    async def _link_identity(self, *, user_id: int, plan_codes: tuple[str, ...]):
        self.provider.identity = PatreonIdentity(
            provider_user_id=f"patreon-user-{user_id}",
            email=f"user-{user_id}@example.com",
            display_name=f"Patron {user_id}",
            member_id=f"member-{user_id}",
            plan_codes=plan_codes,
            patron_status="active_patron",
            tier_ids=tuple(f"tier-{plan}" for plan in plan_codes),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": f"patreon-user-{user_id}"},
            raw_member={"id": f"member-{user_id}"},
            raw_tiers=(),
        )
        await self.service._sync_identity(
            discord_user_id=user_id,
            identity=self.provider.identity,
            access_token=f"access-{user_id}",
            refresh_token=f"refresh-{user_id}",
            token_expires_at=_utcnow() + timedelta(hours=1),
        )

    async def test_provider_diagnostics_redacts_storage_error_credentials(self):
        self.service.storage_ready = False
        self.service.storage_error = "could not connect to postgresql://user:secret@db.example/babblebox"

        diagnostics = self.service.provider_diagnostics()

        self.assertNotIn("user:secret", str(diagnostics["storage_error"]))
        self.assertIn("postgresql://[redacted]@db.example/babblebox", str(diagnostics["storage_error"]))

    async def test_manual_grant_block_and_clear_follow_precedence_rules(self):
        self.assertEqual(self.service.get_user_snapshot(10)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.resolve_user_limit(10, LIMIT_WATCH_KEYWORDS), 10)

        grant = await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=10,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_PLUS,
            actor_user_id=999,
            reason="support",
        )
        self.assertEqual(self.service.get_user_snapshot(10)["plan_code"], PLAN_PLUS)
        self.assertEqual(self.service.resolve_user_limit(10, LIMIT_WATCH_KEYWORDS), 25)

        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=10,
            kind=MANUAL_KIND_BLOCK,
            plan_code=None,
            actor_user_id=999,
            reason="fraud hold",
        )
        blocked = self.service.get_user_snapshot(10)
        self.assertTrue(blocked["blocked"])
        self.assertEqual(blocked["plan_code"], PLAN_FREE)

        ok, message = await self.service.clear_block_overrides(target_type=SCOPE_USER, target_id=10, actor_user_id=999)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_user_snapshot(10)["plan_code"], PLAN_PLUS)

        ok, message = await self.service.deactivate_override(grant["override_id"], actor_user_id=999)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_user_snapshot(10)["plan_code"], PLAN_FREE)

    async def test_supporter_keeps_free_limits_while_resolving_as_paid_support_tier(self):
        await self._link_identity(user_id=11, plan_codes=(PLAN_SUPPORTER,))

        snapshot = self.service.get_user_snapshot(11)

        self.assertEqual(snapshot["plan_code"], PLAN_SUPPORTER)
        self.assertEqual(snapshot["active_plans"], (PLAN_SUPPORTER,))
        self.assertEqual(snapshot["claimable_sources"], ())
        self.assertEqual(self.service.resolve_user_limit(11, LIMIT_WATCH_KEYWORDS), 10)

    async def test_manual_plus_can_stack_with_provider_guild_pro_without_merging_personal_and_claim_lanes(self):
        await self._link_identity(user_id=12, plan_codes=(PLAN_GUILD_PRO,))
        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=12,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_PLUS,
            actor_user_id=999,
            reason="personal utility grant",
        )

        snapshot = self.service.get_user_snapshot(12)

        self.assertEqual(snapshot["plan_code"], PLAN_PLUS)
        self.assertEqual(snapshot["active_plans"], (PLAN_PLUS,))
        self.assertEqual(len(snapshot["claimable_sources"]), 1)
        self.assertEqual(snapshot["claimable_sources"][0]["source_kind"], "entitlement")
        self.assertEqual(self.service.resolve_user_limit(12, LIMIT_WATCH_KEYWORDS), 25)

    async def test_system_owner_keeps_full_access_and_can_claim_multiple_guilds_without_manual_grants(self):
        owner_user_id = next(iter(SYSTEM_PREMIUM_OWNER_USER_IDS))
        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=owner_user_id,
            kind=MANUAL_KIND_BLOCK,
            plan_code=None,
            actor_user_id=999,
            reason="should not suppress operator premium",
        )

        snapshot = self.service.get_user_snapshot(owner_user_id)
        self.assertEqual(snapshot["plan_code"], PLAN_PLUS)
        self.assertEqual(snapshot["active_plans"], (PLAN_PLUS, PLAN_GUILD_PRO))
        self.assertFalse(snapshot["blocked"])
        self.assertEqual(snapshot["system_guild_claims"], "unlimited")
        self.assertEqual(self.service.resolve_user_limit(owner_user_id, LIMIT_WATCH_KEYWORDS), 25)

        guild_one = FakeGuild(770)
        guild_one.add_member(FakeGuildMember(owner_user_id, manage_guild=True))
        guild_two = FakeGuild(771)
        guild_two.add_member(FakeGuildMember(owner_user_id, manage_guild=True))

        ok, message = await self.service.claim_guild(guild=guild_one, actor=guild_one.get_member(owner_user_id))
        self.assertTrue(ok, message)
        ok, message = await self.service.claim_guild(guild=guild_two, actor=guild_two.get_member(owner_user_id))
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(770)["plan_code"], PLAN_GUILD_PRO)
        self.assertEqual(self.service.get_guild_snapshot(771)["plan_code"], PLAN_GUILD_PRO)

    async def test_support_guild_keeps_permanent_guild_pro_and_rejects_claims(self):
        await self.service.create_manual_override(
            target_type=SCOPE_GUILD,
            target_id=SYSTEM_PREMIUM_SUPPORT_GUILD_ID,
            kind=MANUAL_KIND_BLOCK,
            plan_code=None,
            actor_user_id=999,
            reason="should not suppress support-guild premium",
        )
        snapshot = self.service.get_guild_snapshot(SYSTEM_PREMIUM_SUPPORT_GUILD_ID)
        self.assertEqual(snapshot["plan_code"], PLAN_GUILD_PRO)
        self.assertTrue(snapshot["system_access"])
        self.assertEqual(self.service.resolve_guild_limit(SYSTEM_PREMIUM_SUPPORT_GUILD_ID, LIMIT_BUMP_DETECTION_CHANNELS), 15)
        self.assertTrue(self.service.guild_has_capability(SYSTEM_PREMIUM_SUPPORT_GUILD_ID, CAPABILITY_SHIELD_AI_REVIEW))

        owner_user_id = next(iter(SYSTEM_PREMIUM_OWNER_USER_IDS))
        support_guild = FakeGuild(SYSTEM_PREMIUM_SUPPORT_GUILD_ID)
        support_guild.add_member(FakeGuildMember(owner_user_id, manage_guild=True))

        ok, message = await self.service.claim_guild(guild=support_guild, actor=support_guild.get_member(owner_user_id))
        self.assertFalse(ok)
        self.assertIn("already has permanent full-access premium", message)

    async def test_guild_claim_release_and_transfer_rules_are_non_duplicating(self):
        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=41,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="staff grant",
        )
        guild_one = FakeGuild(700)
        owner = guild_one.add_member(FakeGuildMember(41, manage_guild=True))
        outsider = guild_one.add_member(FakeGuildMember(99, manage_guild=True))
        guild_two = FakeGuild(701)
        guild_two.add_member(FakeGuildMember(41, manage_guild=True))

        ok, message = await self.service.claim_guild(guild=guild_one, actor=owner)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(700)["plan_code"], PLAN_GUILD_PRO)

        ok, message = await self.service.claim_guild(guild=guild_two, actor=guild_two.get_member(41))
        self.assertFalse(ok)
        self.assertIn("No unclaimed Guild Pro entitlement", message)

        ok, message = await self.service.release_guild(guild=guild_one, actor=outsider)
        self.assertFalse(ok)
        self.assertIn("claim owner", message)

        ok, message = await self.service.release_guild(guild=guild_one, actor=owner)
        self.assertTrue(ok, message)

        ok, message = await self.service.claim_guild(guild=guild_two, actor=guild_two.get_member(41))
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(701)["plan_code"], PLAN_GUILD_PRO)

    async def test_concurrent_guild_claim_race_only_allows_one_active_source(self):
        await self._link_identity(user_id=411, plan_codes=(PLAN_GUILD_PRO,))
        guild_one = FakeGuild(7411)
        guild_one.add_member(FakeGuildMember(411, manage_guild=True))
        guild_two = FakeGuild(7412)
        guild_two.add_member(FakeGuildMember(411, manage_guild=True))

        first, second = await asyncio.gather(
            self.service.claim_guild(guild=guild_one, actor=guild_one.get_member(411)),
            self.service.claim_guild(guild=guild_two, actor=guild_two.get_member(411)),
        )

        self.assertEqual(sum(1 for ok, _message in (first, second) if ok), 1)
        self.assertEqual(sum(1 for ok, _message in (first, second) if not ok), 1)
        active_claims = await self.store.list_active_claims()
        self.assertEqual(len(active_claims), 1)
        self.assertEqual(active_claims[0]["owner_user_id"], 411)
        self.assertIn(active_claims[0]["guild_id"], {7411, 7412})
        self.assertTrue(any("No unclaimed Guild Pro entitlement" in message for ok, message in (first, second) if not ok))

    async def test_claim_and_release_require_live_manage_guild_context(self):
        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=42,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="staff grant",
        )
        guild = FakeGuild(710)
        unauthorized = guild.add_member(FakeGuildMember(42, manage_guild=False))
        ok, message = await self.service.claim_guild(guild=guild, actor=unauthorized)
        self.assertFalse(ok)
        self.assertIn("Manage Server", message)

        authorized = guild.add_member(FakeGuildMember(42, manage_guild=True))
        other_guild = FakeGuild(711)
        ok, message = await self.service.claim_guild(guild=other_guild, actor=authorized)
        self.assertFalse(ok)
        self.assertIn("this server", message)

        ok, message = await self.service.claim_guild(guild=guild, actor=authorized)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(710)["plan_code"], PLAN_GUILD_PRO)

    async def test_stale_entitlement_keeps_plan_through_grace_then_expires_to_free(self):
        now = _utcnow()
        await self.service.store.upsert_entitlement(
            {
                "entitlement_id": "ent-1",
                "provider": PROVIDER_PATREON,
                "source_ref": "member-1:plus",
                "discord_user_id": 51,
                "plan_code": PLAN_PLUS,
                "status": "active",
                "linked_provider_user_id": "patreon-user-51",
                "last_verified_at": _serialize_datetime(now - timedelta(days=2)),
                "stale_after": _serialize_datetime(now - timedelta(hours=1)),
                "grace_until": _serialize_datetime(now + timedelta(days=1)),
                "current_period_end": _serialize_datetime(now + timedelta(days=1)),
                "metadata": {},
            }
        )
        await self.service._reload_cache()

        stale_snapshot = self.service.get_user_snapshot(51)
        self.assertEqual(stale_snapshot["plan_code"], PLAN_PLUS)
        self.assertTrue(stale_snapshot["stale"])
        self.assertTrue(stale_snapshot["in_grace"])

        await self.service.store.upsert_entitlement(
            {
                "entitlement_id": "ent-1",
                "provider": PROVIDER_PATREON,
                "source_ref": "member-1:plus",
                "discord_user_id": 51,
                "plan_code": PLAN_PLUS,
                "status": "active",
                "linked_provider_user_id": "patreon-user-51",
                "last_verified_at": _serialize_datetime(now - timedelta(days=2)),
                "stale_after": _serialize_datetime(now - timedelta(hours=2)),
                "grace_until": _serialize_datetime(now - timedelta(minutes=1)),
                "current_period_end": _serialize_datetime(now - timedelta(minutes=1)),
                "metadata": {},
            }
        )
        await self.service._reload_cache()

        expired_snapshot = self.service.get_user_snapshot(51)
        self.assertEqual(expired_snapshot["plan_code"], PLAN_FREE)
        self.assertTrue(expired_snapshot["stale"])
        self.assertFalse(expired_snapshot["in_grace"])

    async def test_create_link_url_hashes_state_and_invalidates_previous_link_session(self):
        ok, first_url = await self.service.create_link_url(60)
        self.assertTrue(ok, first_url)
        first_state = parse_qs(urlsplit(first_url).query)["state"][0]

        ok, second_url = await self.service.create_link_url(60)
        self.assertTrue(ok, second_url)
        second_state = parse_qs(urlsplit(second_url).query)["state"][0]

        self.assertNotEqual(first_state, second_state)
        oauth_states = self.store._store.oauth_states
        self.assertNotIn((PROVIDER_PATREON, first_state), oauth_states)
        self.assertIn((PROVIDER_PATREON, self.service._hash_oauth_state_token(first_state)), oauth_states)
        self.assertIsNone(await self.service.store.consume_oauth_state(PROVIDER_PATREON, self.service._hash_oauth_state_token(first_state), action="link", now=_utcnow()))
        reused = await self.service.complete_link_callback(state_token=first_state, code="code-old")
        self.assertEqual(reused["title"], "Link expired")

    async def test_complete_link_callback_links_identity_and_rejects_reuse(self):
        await self._create_state(user_id=61, state_token="state-1")

        result = await self.service.complete_link_callback(state_token="state-1", code="code-1")

        self.assertEqual(result["title"], "Patreon linked")
        self.assertIsNotNone(self.service.get_link(61))
        self.assertEqual(
            self.service.get_link(61)["scopes"],
            ("identity", "identity.memberships", "identity[email]"),
        )
        self.assertEqual(self.service.get_user_snapshot(61)["plan_code"], PLAN_PLUS)

        reused = await self.service.complete_link_callback(state_token="state-1", code="code-1")
        self.assertEqual(reused["title"], "Link expired")

    async def test_complete_link_callback_rejects_wrong_state_action(self):
        now = _utcnow()
        await self.service.store.create_oauth_state(
            {
                "provider": PROVIDER_PATREON,
                "state_token": self.service._hash_oauth_state_token("state-wrong"),
                "discord_user_id": 611,
                "action": "unexpected",
                "created_at": _serialize_datetime(now),
                "expires_at": _serialize_datetime(now + timedelta(minutes=15)),
                "consumed_at": None,
                "metadata": {},
            }
        )
        result = await self.service.complete_link_callback(state_token="state-wrong", code="code-1")
        self.assertEqual(result["title"], "Link expired")

    async def test_complete_link_callback_rejects_expired_state_without_consuming_it(self):
        now = _utcnow()
        hashed_state = self.service._hash_oauth_state_token("state-expired")
        await self.service.store.create_oauth_state(
            {
                "provider": PROVIDER_PATREON,
                "state_token": hashed_state,
                "discord_user_id": 612,
                "action": "link",
                "created_at": _serialize_datetime(now - timedelta(minutes=20)),
                "expires_at": _serialize_datetime(now - timedelta(minutes=5)),
                "consumed_at": None,
                "metadata": {},
            }
        )

        result = await self.service.complete_link_callback(state_token="state-expired", code="code-1")

        self.assertEqual(result["title"], "Link expired")
        stored_state = self.store._store.oauth_states[(PROVIDER_PATREON, hashed_state)]
        self.assertIsNone(stored_state["consumed_at"])

    async def test_unlink_releases_provider_backed_claims_non_destructively(self):
        await self._link_identity(user_id=62, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(801)
        guild.add_member(FakeGuildMember(62, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(62))
        self.assertTrue(ok, message)

        ok, message = await self.service.unlink_user(62)
        self.assertTrue(ok, message)
        self.assertIsNone(self.service.get_link(62))
        self.assertEqual(self.service.get_user_snapshot(62)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(801)["plan_code"], PLAN_FREE)

    async def test_unlink_rebinds_provider_backed_claim_to_manual_guild_grant(self):
        await self._link_identity(user_id=6200, plan_codes=(PLAN_GUILD_PRO,))
        grant = await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=6200,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="fallback guild grant",
        )
        guild = FakeGuild(86200)
        guild.add_member(FakeGuildMember(6200, manage_guild=True))

        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(6200))
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(86200)["claim"]["source_kind"], "entitlement")

        ok, message = await self.service.unlink_user(6200)
        self.assertTrue(ok, message)
        claim = self.service.get_guild_snapshot(86200)["claim"]
        self.assertEqual(self.service.get_guild_snapshot(86200)["plan_code"], PLAN_GUILD_PRO)
        self.assertEqual(claim["source_kind"], MANUAL_KIND_GRANT)
        self.assertEqual(claim["source_id"], grant["override_id"])
        self.assertIn("Auto-rebound", claim["note"])
        self.assertIsNone(self.service.get_link(6200))

    async def test_manual_guild_grant_deactivation_rebinds_claim_to_provider_entitlement(self):
        grant = await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=6201,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="launch fallback",
        )
        guild = FakeGuild(86201)
        guild.add_member(FakeGuildMember(6201, manage_guild=True))

        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(6201))
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(86201)["claim"]["source_kind"], MANUAL_KIND_GRANT)

        await self._link_identity(user_id=6201, plan_codes=(PLAN_GUILD_PRO,))
        ok, message = await self.service.deactivate_override(grant["override_id"], actor_user_id=999)

        self.assertTrue(ok, message)
        claim = self.service.get_guild_snapshot(86201)["claim"]
        self.assertEqual(self.service.get_guild_snapshot(86201)["plan_code"], PLAN_GUILD_PRO)
        self.assertEqual(claim["source_kind"], "entitlement")
        self.assertTrue(str(claim["entitlement_id"]).startswith("patreon:member-6201:guild_pro"))
        self.assertIn("Auto-rebound", claim["note"])

    async def test_ambiguous_patreon_link_callback_fails_closed_without_granting_access(self):
        await self._create_state(user_id=620, state_token="ambiguous-state")
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-620",
            email="user-620@example.com",
            display_name="Patron 620",
            member_id="member-620",
            plan_codes=(PLAN_PLUS, PLAN_GUILD_PRO),
            patron_status="active_patron",
            tier_ids=("tier-plus", "tier-guild-pro"),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-620"},
            raw_member={"id": "member-620"},
            raw_tiers=(),
        )

        result = await self.service.complete_link_callback(state_token="ambiguous-state", code="code-620")

        self.assertEqual(result["title"], "Link needs review")
        self.assertIn("No Patreon-backed premium was granted", result["message"])
        self.assertEqual(self.service.get_user_snapshot(620)["plan_code"], PLAN_FREE)
        self.assertIsNotNone(self.service.get_link(620))
        self.assertEqual(
            self.service._provider_state[PROVIDER_PATREON]["payload"]["recent_issues"][0]["issue_type"],
            "patreon_ambiguous_plan_mapping",
        )

    async def test_ambiguous_patreon_refresh_releases_existing_provider_backed_claim(self):
        await self._link_identity(user_id=621, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(8621)
        guild.add_member(FakeGuildMember(621, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(621))
        self.assertTrue(ok, message)
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-621",
            email="user-621@example.com",
            display_name="Patron 621",
            member_id="member-621",
            plan_codes=(PLAN_PLUS, PLAN_GUILD_PRO),
            patron_status="active_patron",
            tier_ids=("tier-plus", "tier-guild-pro"),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-621"},
            raw_member={"id": "member-621"},
            raw_tiers=(),
        )

        ok, message = await self.service.refresh_user_link(621)

        self.assertFalse(ok)
        self.assertIn("No Patreon-backed premium was granted", message)
        self.assertEqual(self.service.get_user_snapshot(621)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(8621)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_link(621)["link_status"], "active")
        self.assertEqual(await self.store.list_active_claims(), [])

    async def test_unmapped_patreon_refresh_withdraws_old_claim_without_guessing_plan(self):
        await self._link_identity(user_id=622, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(8622)
        guild.add_member(FakeGuildMember(622, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(622))
        self.assertTrue(ok, message)
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-622",
            email="user-622@example.com",
            display_name="Patron 622",
            member_id="member-622",
            plan_codes=(),
            patron_status="active_patron",
            tier_ids=("tier-legacy-622",),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-622"},
            raw_member={"id": "member-622"},
            raw_tiers=({"id": "tier-legacy-622", "type": "tier"},),
        )

        ok, message = await self.service.refresh_user_link(622)

        self.assertTrue(ok, message)
        self.assertEqual(message, "Patreon entitlements refreshed.")
        self.assertEqual(self.service.get_user_snapshot(622)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(8622)["plan_code"], PLAN_FREE)
        self.assertEqual(await self.store.list_active_claims(), [])
        link = self.service.get_link(622)
        self.assertEqual(link["link_status"], "active")
        self.assertEqual(link["metadata"]["tier_ids"], ["tier-legacy-622"])
        entitlements = self.service.list_cached_entitlements_for_user(622)
        self.assertTrue(entitlements)
        self.assertTrue(all(record["status"] == "inactive" for record in entitlements))

    async def test_inactive_entitlement_backed_claim_is_auto_released_on_reload(self):
        now = _utcnow()
        await self.store.upsert_entitlement(
            {
                "entitlement_id": "ent-guild-1",
                "provider": PROVIDER_PATREON,
                "source_ref": "member-77:guild_pro",
                "discord_user_id": 77,
                "plan_code": PLAN_GUILD_PRO,
                "status": "inactive",
                "linked_provider_user_id": "patreon-user-77",
                "last_verified_at": _serialize_datetime(now - timedelta(days=2)),
                "stale_after": _serialize_datetime(now - timedelta(days=1)),
                "grace_until": _serialize_datetime(now - timedelta(hours=1)),
                "current_period_end": _serialize_datetime(now - timedelta(hours=1)),
                "metadata": {},
            }
        )
        await self.store.claim_guild(
            {
                "claim_id": "claim-1",
                "guild_id": 770,
                "plan_code": PLAN_GUILD_PRO,
                "owner_user_id": 77,
                "source_kind": "entitlement",
                "source_id": "ent-guild-1",
                "status": "active",
                "claimed_at": _serialize_datetime(now - timedelta(hours=2)),
                "updated_at": _serialize_datetime(now - timedelta(hours=2)),
                "entitlement_id": "ent-guild-1",
                "note": None,
            }
        )

        await self.service._reload_cache()

        snapshot = self.service.get_guild_snapshot(770)
        self.assertEqual(snapshot["plan_code"], PLAN_FREE)
        self.assertIsNone(snapshot["claim"])
        self.assertEqual(await self.store.list_active_claims(), [])

    async def test_revoking_manual_guild_pro_grant_releases_attached_claim(self):
        grant = await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=78,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="manual staff grant",
        )
        guild = FakeGuild(780)
        guild.add_member(FakeGuildMember(78, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(78))
        self.assertTrue(ok, message)

        ok, message = await self.service.deactivate_override(grant["override_id"], actor_user_id=999)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(780)["plan_code"], PLAN_FREE)
        self.assertIsNone(self.service.get_guild_snapshot(780)["claim"])

    async def test_stale_entitlement_rebinds_to_manual_claim_source_and_survives_restart(self):
        await self._link_identity(user_id=7801, plan_codes=(PLAN_GUILD_PRO,))
        grant = await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=7801,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="recovery fallback",
        )
        guild = FakeGuild(87801)
        guild.add_member(FakeGuildMember(7801, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(7801))
        self.assertTrue(ok, message)
        entitlement_id = self.service.get_guild_snapshot(87801)["claim"]["entitlement_id"]
        now = _utcnow()
        await self.store.upsert_entitlement(
            {
                **self.service.list_cached_entitlements_for_user(7801)[0],
                "entitlement_id": entitlement_id,
                "status": "inactive",
                "stale_after": _serialize_datetime(now - timedelta(hours=1)),
                "grace_until": _serialize_datetime(now - timedelta(minutes=1)),
                "current_period_end": _serialize_datetime(now - timedelta(minutes=1)),
                "last_verified_at": _serialize_datetime(now - timedelta(days=1)),
            }
        )

        await self.service._reload_cache()

        rebound = self.service.get_guild_snapshot(87801)["claim"]
        self.assertEqual(rebound["source_kind"], MANUAL_KIND_GRANT)
        self.assertEqual(rebound["source_id"], grant["override_id"])
        self.assertEqual(self.service.get_guild_snapshot(87801)["plan_code"], PLAN_GUILD_PRO)

        reloaded = PremiumService(self.bot, store=self.store, provider=self.provider)
        started = await reloaded.start()
        self.assertTrue(started)
        try:
            reloaded_claim = reloaded.get_guild_snapshot(87801)["claim"]
            self.assertEqual(reloaded_claim["source_kind"], MANUAL_KIND_GRANT)
            self.assertEqual(reloaded_claim["source_id"], grant["override_id"])
            self.assertEqual(reloaded.get_guild_snapshot(87801)["plan_code"], PLAN_GUILD_PRO)
        finally:
            await reloaded.close()

    async def test_webhook_processing_is_idempotent_for_duplicate_payloads(self):
        await self._link_identity(user_id=63, plan_codes=(PLAN_PLUS,))
        body = json.dumps(
            {
                "data": {"id": "member-63", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-63"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:pledge:create", signature="ok")
            self.assertIsInstance(result, PatreonWebhookResult)
            self.assertEqual(result.outcome, "processed")
            self.assertEqual(result.message, "Patreon webhook processed.")

            result = await self.service.handle_patreon_webhook(body=body, event_type="members:pledge:create", signature="ok")
            self.assertEqual(result.outcome, "duplicate")
            self.assertEqual(result.message, "Duplicate Patreon webhook ignored.")

        self.assertEqual(len(self.provider.fetch_access_tokens), 1)

    async def test_webhook_signature_failure_is_rejected_before_processing(self):
        await self._link_identity(user_id=64, plan_codes=(PLAN_PLUS,))
        body = json.dumps({"included": [{"type": "user", "id": "patreon-user-64"}]}).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            with self.assertRaises(WebhookVerificationError):
                await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="bad")

    async def test_refresh_invalid_grant_immediately_revokes_provider_access(self):
        await self._link_identity(user_id=641, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(8641)
        guild.add_member(FakeGuildMember(641, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(641))
        self.assertTrue(ok, message)

        link = self.service.get_link(641)
        await self.store.upsert_link({**link, "token_expires_at": _serialize_datetime(_utcnow() - timedelta(minutes=1))})
        await self.service._reload_cache()

        async def _fail_refresh(*, refresh_token: str):
            raise PremiumProviderError(
                "invalid_grant",
                safe_message="Patreon rejected the saved link token. Re-link Patreon from `/premium link`.",
                provider_code="invalid_grant",
                status_code=401,
                hard_failure=True,
            )

        self.provider.refresh_access_token = _fail_refresh
        ok, message = await self.service.refresh_user_link(641)
        self.assertFalse(ok)
        self.assertIn("Re-link Patreon", message)
        self.assertEqual(self.service.get_user_snapshot(641)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(8641)["plan_code"], PLAN_FREE)
        revoked_link = self.service.get_link(641)
        self.assertEqual(revoked_link["link_status"], LINK_STATUS_REVOKED)
        self.assertIsNone(revoked_link["access_token_ciphertext"])
        self.assertIsNone(revoked_link["refresh_token_ciphertext"])

    async def test_identity_unauthorized_immediately_revokes_provider_access(self):
        await self._link_identity(user_id=642, plan_codes=(PLAN_PLUS,))

        async def _fail_identity(*, access_token: str):
            raise PremiumProviderError(
                "unauthorized",
                safe_message="Patreon rejected the saved link token. Re-link Patreon from `/premium link`.",
                provider_code="unauthorized",
                status_code=401,
                hard_failure=True,
            )

        self.provider.fetch_identity = _fail_identity
        ok, message = await self.service.refresh_user_link(642)
        self.assertFalse(ok)
        self.assertIn("Re-link Patreon", message)
        self.assertEqual(self.service.get_user_snapshot(642)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_link(642)["link_status"], LINK_STATUS_REVOKED)

    async def test_refresh_provider_identity_mismatch_revokes_provider_access(self):
        await self._link_identity(user_id=6421, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(86421)
        guild.add_member(FakeGuildMember(6421, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(6421))
        self.assertTrue(ok, message)
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-6421-changed",
            email="changed-6421@example.com",
            display_name="Changed 6421",
            member_id="member-6421-changed",
            plan_codes=(PLAN_GUILD_PRO,),
            patron_status="active_patron",
            tier_ids=("tier-guild_pro",),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-6421-changed"},
            raw_member={"id": "member-6421-changed"},
            raw_tiers=(),
        )

        ok, message = await self.service.refresh_user_link(6421)

        self.assertFalse(ok)
        self.assertIn("different account", message)
        self.assertEqual(self.service.get_user_snapshot(6421)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(86421)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_link(6421)["link_status"], LINK_STATUS_REVOKED)
        self.assertEqual(
            self.service.provider_diagnostics()["provider_state"]["payload"]["last_issue"]["issue_type"],
            "patreon_identity_mismatch",
        )

    async def test_local_token_decrypt_failure_marks_link_broken_and_withholds_runtime_access(self):
        await self._link_identity(user_id=643, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(8643)
        guild.add_member(FakeGuildMember(643, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(643))
        self.assertTrue(ok, message)

        with patch.object(
            self.store.crypto,
            "decrypt_secret",
            side_effect=PremiumCryptoError("decrypt failed"),
        ):
            ok, message = await self.service.refresh_user_link(643)

        self.assertFalse(ok)
        self.assertIn("Re-link Patreon", message)
        self.assertEqual(self.service.get_user_snapshot(643)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(8643)["plan_code"], PLAN_FREE)
        broken_link = self.service.get_link(643)
        self.assertEqual(broken_link["link_status"], LINK_STATUS_BROKEN)
        self.assertIsNone(broken_link["access_token_ciphertext"])
        self.assertIsNone(broken_link["refresh_token_ciphertext"])

    async def test_ambiguous_patreon_webhook_is_stored_for_manual_review_and_releases_claim(self):
        await self._link_identity(user_id=644, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(8644)
        guild.add_member(FakeGuildMember(644, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(644))
        self.assertTrue(ok, message)
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-644",
            email="user-644@example.com",
            display_name="Patron 644",
            member_id="member-644",
            plan_codes=(PLAN_PLUS, PLAN_GUILD_PRO),
            patron_status="active_patron",
            tier_ids=("tier-plus", "tier-guild-pro"),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-644"},
            raw_member={"id": "member-644"},
            raw_tiers=(),
        )
        body = json.dumps(
            {
                "data": {"id": "member-644", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-644"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="ok")

        self.assertEqual(result.outcome, "unresolved")
        self.assertEqual(result.message, "Patreon webhook stored for manual review.")
        self.assertEqual(self.service.get_guild_snapshot(8644)["plan_code"], PLAN_FREE)
        self.assertEqual(await self.store.list_active_claims(), [])

    async def test_unmapped_patreon_webhook_withdraws_old_claim_without_guessing_plan(self):
        await self._link_identity(user_id=6442, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(86442)
        guild.add_member(FakeGuildMember(6442, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(6442))
        self.assertTrue(ok, message)
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-6442",
            email="user-6442@example.com",
            display_name="Patron 6442",
            member_id="member-6442",
            plan_codes=(),
            patron_status="active_patron",
            tier_ids=("tier-legacy-6442",),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-6442"},
            raw_member={"id": "member-6442"},
            raw_tiers=({"id": "tier-legacy-6442", "type": "tier"},),
        )
        body = json.dumps(
            {
                "data": {"id": "member-6442", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-6442"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="ok")

        self.assertEqual(result.outcome, "processed")
        self.assertEqual(result.message, "Patreon webhook processed.")
        self.assertEqual(self.service.get_user_snapshot(6442)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(86442)["plan_code"], PLAN_FREE)
        self.assertEqual(await self.store.list_active_claims(), [])
        link = self.service.get_link(6442)
        self.assertEqual(link["link_status"], "active")
        self.assertEqual(link["metadata"]["tier_ids"], ["tier-legacy-6442"])

    async def test_webhook_identity_mismatch_degrades_safely_without_failing_webhook(self):
        await self._link_identity(user_id=6441, plan_codes=(PLAN_GUILD_PRO,))
        guild = FakeGuild(86441)
        guild.add_member(FakeGuildMember(6441, manage_guild=True))
        ok, message = await self.service.claim_guild(guild=guild, actor=guild.get_member(6441))
        self.assertTrue(ok, message)
        body = json.dumps(
            {
                "data": {"id": "member-6441-changed", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-6441"}],
            }
        ).encode("utf-8")
        self.provider.identity = PatreonIdentity(
            provider_user_id="patreon-user-6441-changed",
            email="changed-6441@example.com",
            display_name="Changed 6441",
            member_id="member-6441-changed",
            plan_codes=(PLAN_GUILD_PRO,),
            patron_status="active_patron",
            tier_ids=("tier-guild-pro",),
            next_charge_date=_utcnow() + timedelta(days=30),
            raw_user={"id": "patreon-user-6441-changed"},
            raw_member={"id": "member-6441-changed"},
            raw_tiers=(),
        )

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="ok")

        self.assertEqual(result.outcome, "processed")
        self.assertEqual(self.service.get_user_snapshot(6441)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(86441)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_link(6441)["link_status"], LINK_STATUS_REVOKED)
        self.assertEqual(self.service.public_provider_monitor_summary()["last_webhook_status"], "processed")

    async def test_linked_provider_identity_cannot_be_stolen_by_another_user(self):
        await self._link_identity(user_id=65, plan_codes=(PLAN_PLUS,))

        with self.assertRaises(PremiumProviderError):
            await self.service._sync_identity(
                discord_user_id=66,
                identity=self.provider.identity,
                access_token="access-66",
                refresh_token="refresh-66",
                token_expires_at=_utcnow() + timedelta(hours=1),
            )

    async def test_manual_override_creation_requires_ready_storage(self):
        self.service.storage_ready = False
        with self.assertRaises(PremiumStorageUnavailable):
            await self.service.create_manual_override(
                target_type=SCOPE_USER,
                target_id=90,
                kind=MANUAL_KIND_GRANT,
                plan_code=PLAN_PLUS,
                actor_user_id=999,
                reason="offline",
            )
        self.service.storage_ready = True

    async def test_patreon_linking_rejects_missing_campaign_id(self):
        env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "https://example.test",
            "PATREON_REDIRECT_URI": "https://example.test/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "secret",
            "PATREON_CAMPAIGN_ID": "",
            "PATREON_PLUS_TIER_IDS": "123",
        }
        with patch.dict(os.environ, env, clear=False):
            provider = PatreonPremiumProvider()
            service = PremiumService(self.bot, store=PremiumStore(backend="memory"), provider=provider)
            with self.assertRaises(RuntimeError) as error_context:
                await service.start()
            self.assertIn("Premium startup unsafe", str(error_context.exception))
            self.assertIn("Patreon premium configuration is incomplete or inconsistent", str(error_context.exception))

    async def test_disabled_patreon_deployment_returns_honest_message_and_state(self):
        with patch.dict(os.environ, {}, clear=False):
            provider = PatreonPremiumProvider()
            service = PremiumService(self.bot, store=PremiumStore(backend="memory"), provider=provider)
            started = await service.start()
            self.assertTrue(started)
            try:
                ok, message = await service.create_link_url(91)
                self.assertFalse(ok)
                self.assertIn("not enabled on this deployment", message)
                diagnostics = service.provider_diagnostics()
                self.assertEqual(diagnostics["startup_state"], "disabled")
                self.assertEqual(diagnostics["patreon_state"], "disabled")
                self.assertFalse(diagnostics["patreon_configured"])
            finally:
                await service.close()

    async def test_local_memory_premium_is_allowed_for_explicit_local_work(self):
        env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "http://127.0.0.1:8000",
            "PATREON_REDIRECT_URI": "http://127.0.0.1:8000/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "secret",
            "PATREON_CAMPAIGN_ID": "1234",
            "PATREON_PLUS_TIER_IDS": "123",
        }
        with patch.dict(os.environ, env, clear=False):
            provider = PatreonPremiumProvider()
            service = PremiumService(self.bot, store=PremiumStore(backend="memory"), provider=provider)
            started = await service.start()
            self.assertTrue(started)
            try:
                diagnostics = service.provider_diagnostics()
                self.assertEqual(diagnostics["startup_state"], "enabled_safe")
                ok, url = await service.create_link_url(91)
                self.assertTrue(ok)
                self.assertIn("state=", url)
            finally:
                await service.close()

    async def test_public_memory_premium_is_rejected_at_startup(self):
        env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "https://example.test",
            "PATREON_REDIRECT_URI": "https://example.test/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "secret",
            "PATREON_CAMPAIGN_ID": "1234",
            "PATREON_PLUS_TIER_IDS": "123",
        }
        with patch.dict(os.environ, env, clear=False):
            provider = PatreonPremiumProvider()
            service = PremiumService(self.bot, store=PremiumStore(backend="memory"), provider=provider)
            with self.assertRaises(RuntimeError) as error_context:
                await service.start()
            self.assertIn("Premium startup unsafe", str(error_context.exception))
            self.assertIn("Postgres-backed premium storage", str(error_context.exception))

    async def test_patreon_linking_rejects_redirect_base_mismatch(self):
        env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "https://example.test",
            "PATREON_REDIRECT_URI": "https://other.test/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "secret",
            "PATREON_CAMPAIGN_ID": "1234",
            "PATREON_PLUS_TIER_IDS": "123",
        }
        with patch.dict(os.environ, env, clear=False):
            provider = PatreonPremiumProvider()
            self.assertFalse(provider.configured())
            self.assertTrue(any("PUBLIC_BASE_URL" in error for error in provider.configuration_errors()))

    async def test_patreon_linking_rejects_overlapping_tier_mappings(self):
        env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "https://example.test",
            "PATREON_REDIRECT_URI": "https://example.test/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "secret",
            "PATREON_CAMPAIGN_ID": "1234",
            "PATREON_SUPPORTER_TIER_IDS": "999",
            "PATREON_PLUS_TIER_IDS": "999",
        }
        with patch.dict(os.environ, env, clear=False):
            provider = PatreonPremiumProvider()
            self.assertFalse(provider.configured())
            self.assertTrue(any("cannot map to both supporter and plus" in error for error in provider.configuration_errors()))

    async def test_unmatched_patreon_webhook_is_marked_unresolved_and_recorded_for_review(self):
        body = json.dumps(
            {
                "data": {"id": "member-404", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-missing"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="ok")
        self.assertEqual(result.outcome, "unresolved")
        self.assertEqual(result.message, "Patreon webhook stored for manual review.")
        provider_state = self.service.provider_diagnostics()["provider_state"]["payload"]
        self.assertEqual(provider_state["last_issue"]["reason"], "linked_user_missing")
        unresolved_events = [
            record
            for record in self.store._store.webhook_events.values()
            if record.get("status") == "unresolved"
        ]
        self.assertEqual(len(unresolved_events), 1)

    async def test_public_provider_monitor_summary_redacts_sensitive_issue_detail(self):
        await self.service.record_webhook_monitor_event(status="invalid", status_code=400, invalid_signature=True)
        await self.service.record_webhook_monitor_event(status="unavailable", status_code=503)
        await self.service.record_webhook_monitor_event(status="error", status_code=500)
        await self.service._record_provider_issue(
            provider=PROVIDER_PATREON,
            issue_type="webhook_unresolved",
            detail={
                "provider_user_id": "patreon-user-secret",
                "payload_hash": "hash-secret",
                "reason": "linked_user_missing",
            },
        )

        summary = self.service.public_provider_monitor_summary()

        self.assertEqual(summary["status"], "degraded")
        self.assertEqual(summary["invalid_signature_count"], 1)
        self.assertEqual(summary["recent_unavailable_count"], 1)
        self.assertEqual(summary["recent_server_error_count"], 1)
        self.assertEqual(summary["last_issue_type"], "webhook_unresolved")
        self.assertNotIn("provider_user_id", summary)
        self.assertNotIn("payload_hash", summary)

    async def test_public_provider_monitor_summary_survives_service_reload(self):
        await self.service.record_webhook_monitor_event(status="processed", status_code=200)
        await self.service._record_provider_issue(
            provider=PROVIDER_PATREON,
            issue_type="webhook_unresolved",
            detail={"reason": "linked_user_missing"},
        )

        reloaded = PremiumService(self.bot, store=self.store, provider=self.provider)
        started = await reloaded.start()
        self.assertTrue(started)
        try:
            summary = reloaded.public_provider_monitor_summary()
        finally:
            await reloaded.close()

        self.assertEqual(summary["status"], "ready")
        self.assertEqual(summary["last_webhook_status"], "processed")
        self.assertEqual(summary["unresolved_issue_count"], 1)
        self.assertEqual(summary["last_issue_type"], "webhook_unresolved")

    async def test_stale_provider_monitor_summary_marks_old_incidents_stale_without_changing_entitlements(self):
        await self._link_identity(user_id=811, plan_codes=(PLAN_PLUS,))
        stale_at = _serialize_datetime(_utcnow() - timedelta(days=3))
        await self.service._store_provider_state_payload(
            PROVIDER_PATREON,
            {
                "webhook_monitor": {
                    "last_status": "error",
                    "last_http_status": 500,
                    "last_event_at": stale_at,
                    "invalid_signature_count": 0,
                    "recent_unavailable_count": 1,
                    "recent_server_error_count": 1,
                },
                "last_issue": {
                    "issue_type": "webhook_unresolved",
                    "recorded_at": stale_at,
                },
                "unresolved_issue_count": 1,
            },
        )

        summary = self.service.public_provider_monitor_summary()

        self.assertEqual(summary["status"], "stale")
        self.assertTrue(summary["stale"])
        self.assertEqual(self.service.get_user_snapshot(811)["plan_code"], PLAN_PLUS)

    async def test_webhook_processing_is_idempotent_for_canonical_duplicate_json(self):
        await self._link_identity(user_id=643, plan_codes=(PLAN_PLUS,))
        first_body = json.dumps(
            {
                "included": [{"id": "patreon-user-643", "type": "user"}],
                "data": {"type": "member", "id": "member-643"},
            },
            separators=(",", ":"),
        ).encode("utf-8")
        second_body = json.dumps(
            {
                "data": {"id": "member-643", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-643"}],
            },
            separators=(",", ":"),
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            first_result = await self.service.handle_patreon_webhook(body=first_body, event_type="members:update", signature="ok")
            self.assertEqual(first_result.outcome, "processed")
            second_result = await self.service.handle_patreon_webhook(body=second_body, event_type="members:update", signature="ok")
            self.assertEqual(second_result.outcome, "duplicate")
            self.assertEqual(second_result.message, "Duplicate Patreon webhook ignored.")

    async def test_webhook_campaign_mismatch_is_marked_unresolved(self):
        await self._link_identity(user_id=644, plan_codes=(PLAN_PLUS,))
        self.provider.campaign_id = "campaign-good"
        body = json.dumps(
            {
                "data": {
                    "id": "member-644",
                    "type": "member",
                    "relationships": {"campaign": {"data": {"id": "campaign-bad"}}},
                },
                "included": [{"type": "user", "id": "patreon-user-644"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            result = await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="ok")
        self.assertEqual(result.outcome, "unresolved")
        self.assertEqual(result.message, "Patreon webhook stored for manual review.")
        unresolved_events = [
            record
            for record in self.store._store.webhook_events.values()
            if record.get("status") == "unresolved"
        ]
        self.assertEqual(len(unresolved_events), 1)
