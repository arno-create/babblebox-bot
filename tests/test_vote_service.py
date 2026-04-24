import hashlib
import hmac
import json
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from babblebox.premium_limits import (
    LIMIT_AFK_SCHEDULES,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
    user_limit as premium_user_limit,
)
from babblebox.premium_models import PLAN_FREE, PLAN_PLUS, PLAN_SUPPORTER
from babblebox.premium_provider import WebhookVerificationError
from babblebox.vote_service import TopggWebhookResult, VoteService
from babblebox.vote_store import VoteStore


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _raw_signature(secret: str, timestamp: int, body: bytes) -> str:
    message = f"{timestamp}.{body.decode('utf-8')}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, "sha256").hexdigest()
    return f"t={timestamp},v1={digest}"


class VoteServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = types.SimpleNamespace(
            loop=types.SimpleNamespace(is_closed=lambda: False),
            premium_service=types.SimpleNamespace(
                get_user_snapshot=lambda user_id: {
                    "plan_code": PLAN_FREE,
                    "active_plans": (),
                }
            ),
            get_user=lambda user_id: None,
            fetch_user=AsyncMock(return_value=None),
        )
        self.secret = "whs_test_secret"
        self.env = patch.dict(
            "os.environ",
            {
                "TOPGG_ENABLED": "true",
                "TOPGG_WEBHOOK_SECRET": self.secret,
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        )
        self.env.start()
        self.service = VoteService(self.bot, store=VoteStore(backend="memory"))
        started = await self.service.start()
        self.assertTrue(started)

    async def asyncTearDown(self):
        await self.service.close()
        self.env.stop()

    async def test_handle_topgg_webhook_persists_vote_and_dedupes(self):
        created_at = _utc_now()
        expires_at = created_at + timedelta(hours=12)
        payload = {
            "type": "vote.create",
            "data": {
                "id": "vote-1",
                "weight": 2,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "project": {
                    "id": "1480903089518022739",
                    "type": "bot",
                    "platform": "discord",
                    "platform_id": "1480903089518022739",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "5511",
                    "name": "Voter",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        signature = _raw_signature(self.secret, int(created_at.timestamp()), body)

        first = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-1")
        second = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-1")

        self.assertEqual(first, TopggWebhookResult("processed", "Top.gg vote recorded."))
        self.assertEqual(second, TopggWebhookResult("duplicate", "That Top.gg vote event was already processed."))

        record = self.service.get_vote_record(5511)
        self.assertIsNotNone(record)
        self.assertEqual(record["discord_user_id"], 5511)
        self.assertEqual(record["topgg_vote_id"], "vote-1")
        self.assertEqual(record["weight"], 2)
        self.assertEqual(record["webhook_status"], "processed")
        self.assertEqual(record["webhook_trace_id"], "trace-1")

    async def test_handle_topgg_webhook_accepts_dashboard_test_event(self):
        payload = {
            "type": "webhook.test",
            "data": {
                "project": {
                    "id": "1480903089518022739",
                    "type": "bot",
                    "platform": "discord",
                    "platform_id": "1480903089518022739",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "6612",
                    "name": "Tester",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        signature = _raw_signature(self.secret, int(_utc_now().timestamp()), body)

        result = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-test")

        self.assertEqual(result, TopggWebhookResult("processed", "Top.gg webhook test received."))
        self.assertIsNone(self.service.get_vote_record(6612))

    async def test_handle_topgg_webhook_rejects_wrong_project_platform_and_signature(self):
        payload = {
            "type": "vote.create",
            "data": {
                "id": "vote-2",
                "weight": 1,
                "created_at": _utc_now().isoformat(),
                "expires_at": (_utc_now() + timedelta(hours=12)).isoformat(),
                "project": {
                    "id": "999999999999999999",
                    "type": "bot",
                    "platform": "roblox",
                    "platform_id": "999999999999999999",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "5511",
                    "name": "Voter",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        signature = _raw_signature(self.secret, int(_utc_now().timestamp()), body)

        wrong_project = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-2")
        self.assertEqual(wrong_project, TopggWebhookResult("invalid", "This Top.gg webhook does not target the configured Babblebox project."))

        with self.assertRaises(WebhookVerificationError):
            await self.service.handle_topgg_webhook(
                body=body,
                signature="t=1713864020,v1=bad",
                trace_id="trace-3",
            )

    async def test_topgg_flag_disables_vote_lane_until_explicitly_enabled(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_ENABLED": "false",
                "TOPGG_WEBHOOK_SECRET": self.secret,
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            disabled_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await disabled_service.start()
            self.assertTrue(started)
            try:
                self.assertEqual(disabled_service.configuration_state(), "disabled")
                self.assertIn("topgg_enabled=true", disabled_service.configuration_message().casefold())
                diagnostics = disabled_service.diagnostics_snapshot()
                self.assertFalse(diagnostics["enabled"])
                self.assertEqual(diagnostics["configuration_state"], "disabled")
            finally:
                await disabled_service.close()

    async def test_handle_topgg_webhook_rejects_stale_v2_signature_replay(self):
        fixed_now = datetime(2026, 4, 24, 12, 10, tzinfo=timezone.utc)
        created_at = fixed_now - timedelta(minutes=1)
        expires_at = created_at + timedelta(hours=12)
        payload = {
            "type": "vote.create",
            "data": {
                "id": "vote-stale",
                "weight": 1,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "project": {
                    "id": "1480903089518022739",
                    "type": "bot",
                    "platform": "discord",
                    "platform_id": "1480903089518022739",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "5521",
                    "name": "Voter",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        stale_timestamp = int((fixed_now - timedelta(minutes=6)).timestamp())
        signature = _raw_signature(self.secret, stale_timestamp, body)

        with patch("babblebox.vote_service._utcnow", return_value=fixed_now):
            with self.assertRaises(WebhookVerificationError):
                await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-stale")

    async def test_handle_topgg_webhook_rejects_invalid_exact_vote_window(self):
        created_at = _utc_now()
        expires_at = created_at - timedelta(minutes=1)
        payload = {
            "type": "vote.create",
            "data": {
                "id": "vote-bad-window",
                "weight": 1,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "project": {
                    "id": "1480903089518022739",
                    "type": "bot",
                    "platform": "discord",
                    "platform_id": "1480903089518022739",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "5531",
                    "name": "Voter",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        signature = _raw_signature(self.secret, int(created_at.timestamp()), body)

        result = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-window")

        self.assertEqual(result.outcome, "invalid")
        self.assertIn("vote window", result.message.casefold())

    async def test_handle_topgg_webhook_records_receipt_metadata_for_processed_vote(self):
        created_at = _utc_now()
        expires_at = created_at + timedelta(hours=12)
        payload = {
            "type": "vote.create",
            "data": {
                "id": "vote-receipt",
                "weight": 2,
                "created_at": created_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "project": {
                    "id": "1480903089518022739",
                    "type": "bot",
                    "platform": "discord",
                    "platform_id": "1480903089518022739",
                },
                "user": {
                    "id": "topgg-user",
                    "platform_id": "5541",
                    "name": "Voter",
                    "avatar_url": "https://cdn.example/avatar.png",
                },
            },
        }
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        signature_timestamp = int(created_at.timestamp())
        signature = _raw_signature(self.secret, signature_timestamp, body)

        result = await self.service.handle_topgg_webhook(body=body, signature=signature, trace_id="trace-receipt")

        self.assertEqual(result, TopggWebhookResult("processed", "Top.gg vote recorded."))
        event = self.service.store._store.webhook_events["vote-receipt"]
        self.assertEqual(event["webhook_mode"], "v2")
        self.assertEqual(event["trace_id"], "trace-receipt")
        self.assertEqual(event["payload_hash"], hashlib.sha256(body).hexdigest())
        self.assertEqual(event["signature_timestamp"], signature_timestamp)
        self.assertEqual(event["vote_created_at"], created_at.isoformat())
        self.assertEqual(event["vote_expires_at"], expires_at.isoformat())
        self.assertEqual(event["timing_source"], "exact")
        self.assertEqual(event["status"], "processed")
        self.assertIsNotNone(event["processed_at"])

    async def test_legacy_webhook_requires_token_for_configured_mode(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            self.assertEqual(legacy_service.configuration_state(), "misconfigured")
            self.assertIn("legacy", legacy_service.configuration_message().casefold())
            await legacy_service.close()

        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "read-token",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            self.assertEqual(legacy_service.configuration_state(), "configured")
            self.assertIn("legacy", legacy_service.configuration_message().casefold())
            self.assertIn("weaker replay", legacy_service.configuration_message().casefold())
            await legacy_service.close()

    async def test_handle_topgg_legacy_webhook_uses_authorization_header_and_api_status(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "read-token",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            payload = {
                "bot": "1480903089518022739",
                "user": "5511",
                "type": "upvote",
                "isWeekend": True,
                "query": {},
            }
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
            with patch.object(
                legacy_service,
                "_check_legacy_vote_status",
                new=AsyncMock(return_value=(True, None)),
            ) as refresh:
                first = await legacy_service.handle_topgg_webhook(
                    body=body,
                    signature="legacy-shared-secret",
                    trace_id="trace-legacy",
                )
                second = await legacy_service.handle_topgg_webhook(
                    body=body,
                    signature="legacy-shared-secret",
                    trace_id="trace-legacy",
                )

            self.assertEqual(
                first,
                TopggWebhookResult(
                    "processed",
                    "Top.gg legacy vote recorded using the standard 12-hour vote window.",
                ),
            )
            self.assertEqual(second, TopggWebhookResult("duplicate", "That Top.gg vote event was already processed."))
            refresh.assert_awaited()
            record = legacy_service.get_vote_record(5511)
            self.assertIsNotNone(record)
            self.assertEqual(record["weight"], 2)
            self.assertEqual(record["webhook_status"], "processed_legacy_estimated")
            self.assertTrue(str(record["topgg_vote_id"]).startswith("legacy:5511:"))
            created_at = datetime.fromisoformat(record["created_at"])
            expires_at = datetime.fromisoformat(record["expires_at"])
            self.assertAlmostEqual((expires_at - created_at).total_seconds(), 12 * 60 * 60, delta=2)
            await legacy_service.close()

    async def test_handle_topgg_legacy_webhook_test_event_and_unconfirmed_vote(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "read-token",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            test_body = json.dumps(
                {
                    "bot": "1480903089518022739",
                    "user": "6612",
                    "type": "test",
                    "isWeekend": False,
                    "query": {},
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            result = await legacy_service.handle_topgg_webhook(
                body=test_body,
                signature="legacy-shared-secret",
                trace_id="trace-test",
            )
            self.assertEqual(result, TopggWebhookResult("processed", "Top.gg legacy webhook test received."))

            vote_body = json.dumps(
                {
                    "bot": "1480903089518022739",
                    "user": "6612",
                    "type": "upvote",
                    "isWeekend": False,
                    "query": {},
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            with patch.object(
                legacy_service,
                "_check_legacy_vote_status",
                new=AsyncMock(return_value=(False, None)),
            ):
                unconfirmed = await legacy_service.handle_topgg_webhook(
                    body=vote_body,
                    signature="legacy-shared-secret",
                    trace_id="trace-missing",
                )
            self.assertEqual(
                unconfirmed,
                TopggWebhookResult(
                    "unavailable",
                    "Top.gg legacy webhook could not be confirmed yet. Top.gg should retry shortly.",
                ),
            )
            await legacy_service.close()

    async def test_handle_topgg_legacy_webhook_rejects_wrong_bot_and_bad_authorization(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "read-token",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            body = json.dumps(
                {
                    "bot": "999999999999999999",
                    "user": "5511",
                    "type": "upvote",
                    "isWeekend": False,
                    "query": {},
                },
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("utf-8")
            with patch.object(legacy_service, "_check_legacy_vote_status", new=AsyncMock(return_value=(True, None))):
                wrong_bot = await legacy_service.handle_topgg_webhook(
                    body=body,
                    signature="legacy-shared-secret",
                    trace_id="trace-bot",
                )
            self.assertEqual(wrong_bot, TopggWebhookResult("invalid", "This Top.gg webhook does not target the configured Babblebox project."))
            with self.assertRaises(WebhookVerificationError):
                await legacy_service.handle_topgg_webhook(
                    body=body,
                    signature="wrong-secret",
                    trace_id="trace-auth",
                )
            await legacy_service.close()

    async def test_refresh_user_vote_status_legacy_mode_uses_estimated_window_after_v0_confirmation(self):
        with patch.dict(
            "os.environ",
            {
                "TOPGG_WEBHOOK_SECRET": "legacy-shared-secret",
                "TOPGG_TOKEN": "legacy-token",
                "TOPGG_PROJECT_ID": "1480903089518022739",
                "TOPGG_STORAGE_BACKEND": "memory",
            },
            clear=False,
        ):
            legacy_service = VoteService(self.bot, store=VoteStore(backend="memory"))
            started = await legacy_service.start()
            self.assertTrue(started)
            with patch.object(
                legacy_service,
                "_check_legacy_vote_status",
                new=AsyncMock(return_value=(True, None)),
            ) as check_vote:
                ok, message = await legacy_service.refresh_user_vote_status(9911, force=True)
            self.assertTrue(ok)
            self.assertIn("legacy", message.casefold())
            self.assertIn("estimated", message.casefold())
            check_vote.assert_awaited()
            record = legacy_service.get_vote_record(9911)
            self.assertIsNotNone(record)
            self.assertEqual(record["webhook_status"], "api_refresh_legacy_estimated")
            created_at = datetime.fromisoformat(record["created_at"])
            expires_at = datetime.fromisoformat(record["expires_at"])
            self.assertAlmostEqual((expires_at - created_at).total_seconds(), 12 * 60 * 60, delta=2)
            await legacy_service.close()

    async def test_refresh_user_vote_status_handles_active_inactive_missing_token_and_cooldown(self):
        user_id = 7821
        now = _utc_now()
        active_payload = {
            "created_at": (now - timedelta(hours=1)).isoformat(),
            "expires_at": (now + timedelta(hours=11)).isoformat(),
            "weight": 1,
        }
        with patch.dict("os.environ", {"TOPGG_TOKEN": "token"}, clear=False):
            with patch.object(self.service, "_fetch_vote_status_payload", new=AsyncMock(return_value=(True, active_payload, None))):
                ok, message = await self.service.refresh_user_vote_status(user_id)
            self.assertTrue(ok)
            self.assertIn("refreshed", message.casefold())
            self.assertTrue(self.service.has_active_vote_bonus(user_id, plan_code=PLAN_FREE))

            with patch.object(self.service, "_fetch_vote_status_payload", new=AsyncMock(return_value=(False, None, None))):
                ok, message = await self.service.refresh_user_vote_status(user_id, force=True)
            self.assertTrue(ok)
            self.assertIn("not active", message.casefold())
            self.assertFalse(self.service.has_active_vote_bonus(user_id, plan_code=PLAN_FREE))

            with patch.object(self.service, "_fetch_vote_status_payload", new=AsyncMock(return_value=(True, active_payload, None))):
                ok, _message = await self.service.refresh_user_vote_status(user_id)
            self.assertTrue(ok)

            with patch.object(self.service, "_fetch_vote_status_payload", new=AsyncMock(return_value=(True, active_payload, None))) as refresh:
                ok, message = await self.service.refresh_user_vote_status(user_id)
            self.assertFalse(ok)
            self.assertIn("cooldown", message.casefold())
            refresh.assert_not_awaited()

        with patch.dict("os.environ", {"TOPGG_TOKEN": ""}, clear=False):
            ok, message = await self.service.refresh_user_vote_status(user_id, force=True)
        self.assertFalse(ok)
        self.assertIn("topgg_token", message.casefold())

    async def test_vote_limit_overlay_and_copy_stay_separate_from_plus(self):
        created_at = _utc_now()
        await self.service._upsert_vote_record(
            {
                "discord_user_id": 55,
                "topgg_vote_id": "vote-live",
                "created_at": created_at.isoformat(),
                "expires_at": (created_at + timedelta(hours=12)).isoformat(),
                "weight": 1,
                "reminder_opt_in": False,
                "last_reminder_sent_at": None,
                "webhook_status": "processed",
                "webhook_trace_id": "trace-limit",
                "webhook_received_at": created_at.isoformat(),
                "updated_at": created_at.isoformat(),
            }
        )

        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_FREE,
                limit_key=LIMIT_WATCH_KEYWORDS,
                current_limit=premium_user_limit(PLAN_FREE, LIMIT_WATCH_KEYWORDS),
            ),
            15,
        )
        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_SUPPORTER,
                limit_key=LIMIT_WATCH_FILTERS,
                current_limit=premium_user_limit(PLAN_SUPPORTER, LIMIT_WATCH_FILTERS),
            ),
            12,
        )
        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_FREE,
                limit_key=LIMIT_REMINDERS_ACTIVE,
                current_limit=premium_user_limit(PLAN_FREE, LIMIT_REMINDERS_ACTIVE),
            ),
            5,
        )
        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_FREE,
                limit_key=LIMIT_REMINDERS_PUBLIC_ACTIVE,
                current_limit=premium_user_limit(PLAN_FREE, LIMIT_REMINDERS_PUBLIC_ACTIVE),
            ),
            2,
        )
        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_FREE,
                limit_key=LIMIT_AFK_SCHEDULES,
                current_limit=premium_user_limit(PLAN_FREE, LIMIT_AFK_SCHEDULES),
            ),
            10,
        )
        self.assertEqual(
            self.service.resolve_user_limit(
                user_id=55,
                plan_code=PLAN_PLUS,
                limit_key=LIMIT_WATCH_KEYWORDS,
                current_limit=premium_user_limit(PLAN_PLUS, LIMIT_WATCH_KEYWORDS),
            ),
            25,
        )

        inactive_message = self.service.describe_limit_error(
            user_id=99,
            plan_code=PLAN_FREE,
            limit_key=LIMIT_WATCH_KEYWORDS,
            limit_value=10,
            default_message="You can store up to 10 watch keywords.",
        )
        self.assertIsNotNone(inactive_message)
        self.assertIn("/vote", inactive_message)
        self.assertIn("temporary", inactive_message.casefold())
        self.assertIn("Babblebox Plus", inactive_message)

        no_bonus_for_plus = self.service.describe_limit_error(
            user_id=99,
            plan_code=PLAN_PLUS,
            limit_key=LIMIT_WATCH_KEYWORDS,
            limit_value=25,
            default_message="You can store up to 25 watch keywords.",
        )
        self.assertIsNone(no_bonus_for_plus)
