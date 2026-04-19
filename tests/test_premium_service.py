import json
import os
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from babblebox.premium_limits import LIMIT_WATCH_KEYWORDS
from babblebox.premium_models import (
    MANUAL_KIND_BLOCK,
    MANUAL_KIND_GRANT,
    PLAN_FREE,
    PLAN_GUILD_PRO,
    PLAN_PLUS,
    PROVIDER_PATREON,
    PatreonIdentity,
    SCOPE_GUILD,
    SCOPE_USER,
)
from babblebox.premium_provider import PremiumProviderError, WebhookVerificationError
from babblebox.premium_service import PremiumService
from babblebox.premium_store import PremiumStore


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

    def build_authorize_url(self, *, state_token: str) -> str:
        return f"https://patreon.test/oauth?state={state_token}"

    async def exchange_code(self, *, code: str) -> dict:
        return {
            "access_token": f"access-{code}",
            "refresh_token": f"refresh-{code}",
            "expires_in": 3600,
        }

    async def refresh_access_token(self, *, refresh_token: str) -> dict:
        self.refresh_tokens.append(refresh_token)
        return {
            "access_token": f"refreshed-{refresh_token}",
            "refresh_token": refresh_token,
            "expires_in": 3600,
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

    async def close(self):
        return None


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
                "state_token": state_token,
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

    async def test_guild_claim_release_and_transfer_rules_are_non_duplicating(self):
        await self.service.create_manual_override(
            target_type=SCOPE_USER,
            target_id=41,
            kind=MANUAL_KIND_GRANT,
            plan_code=PLAN_GUILD_PRO,
            actor_user_id=999,
            reason="staff grant",
        )

        ok, message = await self.service.claim_guild(guild_id=700, user_id=41)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(700)["plan_code"], PLAN_GUILD_PRO)

        ok, message = await self.service.claim_guild(guild_id=701, user_id=41)
        self.assertFalse(ok)
        self.assertIn("No unclaimed Guild Pro entitlement", message)

        ok, message = await self.service.release_guild(guild_id=700, user_id=99)
        self.assertFalse(ok)
        self.assertIn("claim owner", message)

        ok, message = await self.service.release_guild(guild_id=700, user_id=41)
        self.assertTrue(ok, message)

        ok, message = await self.service.claim_guild(guild_id=701, user_id=41)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_guild_snapshot(701)["plan_code"], PLAN_GUILD_PRO)

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

    async def test_complete_link_callback_links_identity_and_rejects_reuse(self):
        await self._create_state(user_id=61, state_token="state-1")

        result = await self.service.complete_link_callback(state_token="state-1", code="code-1")

        self.assertEqual(result["title"], "Patreon linked")
        self.assertIsNotNone(self.service.get_link(61))
        self.assertEqual(self.service.get_user_snapshot(61)["plan_code"], PLAN_PLUS)

        reused = await self.service.complete_link_callback(state_token="state-1", code="code-1")
        self.assertEqual(reused["title"], "Link expired")

    async def test_unlink_releases_provider_backed_claims_non_destructively(self):
        await self._link_identity(user_id=62, plan_codes=(PLAN_PLUS, PLAN_GUILD_PRO))
        ok, message = await self.service.claim_guild(guild_id=801, user_id=62)
        self.assertTrue(ok, message)

        ok, message = await self.service.unlink_user(62)
        self.assertTrue(ok, message)
        self.assertIsNone(self.service.get_link(62))
        self.assertEqual(self.service.get_user_snapshot(62)["plan_code"], PLAN_FREE)
        self.assertEqual(self.service.get_guild_snapshot(801)["plan_code"], PLAN_FREE)

    async def test_webhook_processing_is_idempotent_for_duplicate_payloads(self):
        await self._link_identity(user_id=63, plan_codes=(PLAN_PLUS,))
        body = json.dumps(
            {
                "data": {"id": "member-63", "type": "member"},
                "included": [{"type": "user", "id": "patreon-user-63"}],
            }
        ).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            ok, message = await self.service.handle_patreon_webhook(body=body, event_type="members:pledge:create", signature="ok")
            self.assertTrue(ok, message)
            self.assertEqual(message, "Patreon webhook processed.")

            ok, message = await self.service.handle_patreon_webhook(body=body, event_type="members:pledge:create", signature="ok")
            self.assertTrue(ok, message)
            self.assertEqual(message, "Duplicate Patreon webhook ignored.")

        self.assertEqual(len(self.provider.fetch_access_tokens), 1)

    async def test_webhook_signature_failure_is_rejected_before_processing(self):
        await self._link_identity(user_id=64, plan_codes=(PLAN_PLUS,))
        body = json.dumps({"included": [{"type": "user", "id": "patreon-user-64"}]}).encode("utf-8")

        with patch.dict(os.environ, {"PATREON_WEBHOOK_SECRET": "secret"}, clear=False):
            with self.assertRaises(WebhookVerificationError):
                await self.service.handle_patreon_webhook(body=body, event_type="members:update", signature="bad")

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
