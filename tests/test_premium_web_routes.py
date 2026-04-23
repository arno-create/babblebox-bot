import types
import unittest
from unittest.mock import patch
from typing import Optional

from babblebox import web
from babblebox.premium_provider import PremiumProviderError, WebhookVerificationError
from babblebox.premium_service import PatreonWebhookResult
from babblebox.vote_service import TopggWebhookResult


class _LoopStub:
    def __init__(self, *, closed: bool = False):
        self._closed = closed

    def is_closed(self):
        return self._closed


class _PatreonStub:
    def __init__(self, *, configured: bool = True):
        self._configured = configured

    def configured(self):
        return self._configured


class PremiumWebRoutesTests(unittest.TestCase):
    def setUp(self):
        self.client = web.app.test_client()
        self._original_runtime = getattr(web, "_premium_runtime", None)
        self._original_vote_runtime = getattr(web, "_vote_runtime", None)
        self._original_bot_runtime = getattr(web, "_bot_runtime", None)
        self._original_webhook_stats = web.get_patreon_webhook_stats()
        web.reset_patreon_webhook_stats()

    def tearDown(self):
        web.set_premium_runtime(self._original_runtime)
        web.set_vote_runtime(self._original_vote_runtime)
        web.set_bot_runtime(self._original_bot_runtime)
        web.reset_patreon_webhook_stats()
        web._patreon_webhook_stats.update(self._original_webhook_stats)

    def _service_stub(
        self,
        *,
        bot=None,
        configured_backend: str = "memory",
        storage_ready: bool = True,
        storage_error=None,
        patreon_configured: bool = True,
        startup_state: Optional[str] = None,
        provider_monitor=None,
        confessions_readiness=None,
    ):
        stub = types.SimpleNamespace(
            bot=bot,
            storage_ready=storage_ready,
            storage_error=storage_error if storage_error is not None else (None if storage_ready else "offline"),
            storage_backend_preference=configured_backend,
            store=types.SimpleNamespace(backend_name=configured_backend),
            patreon=_PatreonStub(configured=patreon_configured),
            complete_link_callback=lambda **kwargs: object(),
            handle_patreon_webhook=lambda **kwargs: object(),
            handle_topgg_webhook=lambda **kwargs: object(),
        )
        resolved_startup_state = startup_state or ("enabled_safe" if patreon_configured else "disabled")
        stub.provider_diagnostics = lambda: {
            "storage_ready": storage_ready,
            "storage_error": stub.storage_error,
            "storage_backend": configured_backend,
            "database_url": "not-configured",
            "crypto_source": "ephemeral",
            "crypto_ephemeral": True,
            "patreon_configured": patreon_configured,
            "patreon_sync_ready": patreon_configured,
            "patreon_config_errors": (),
            "patreon_state": "configured" if patreon_configured else "disabled",
            "startup_state": resolved_startup_state,
            "link_count": 0,
            "entitlement_count": 0,
            "active_claim_count": 0,
            "provider_monitor": dict(provider_monitor or {}),
            "provider_state": {},
        }
        if provider_monitor is not None:
            stub.public_provider_monitor_summary = lambda: dict(provider_monitor)
        if confessions_readiness is not None:
            stub.readiness_snapshot = lambda: dict(confessions_readiness)
        return stub

    def _attach_runtime(
        self,
        *,
        loop=None,
        bot_ready: bool = True,
        storage_ready: bool = True,
        patreon_configured: bool = True,
        service_overrides=None,
    ):
        bot = types.SimpleNamespace(loop=loop, is_ready=lambda: bot_ready)
        services = {
            "premium_service": self._service_stub(bot=bot, storage_ready=storage_ready, patreon_configured=patreon_configured),
            "confessions_service": self._service_stub(bot=bot),
            "shield_service": self._service_stub(bot=bot),
            "admin_service": self._service_stub(bot=bot),
            "utility_service": self._service_stub(bot=bot),
            "profile_service": self._service_stub(bot=bot),
            "question_drops_service": self._service_stub(bot=bot),
            "vote_service": self._service_stub(bot=bot),
        }
        for attr_name, service in (service_overrides or {}).items():
            services[attr_name] = service
        for attr_name, service in services.items():
            if service is not None and getattr(service, "bot", None) is None:
                service.bot = bot
            setattr(bot, attr_name, service)
        web.set_bot_runtime(bot)
        web.set_premium_runtime(services["premium_service"])
        web.set_vote_runtime(services["vote_service"])
        return bot

    def test_public_root_and_allowed_static_files_are_served_with_security_headers(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("X-Content-Type-Options"), "nosniff")
        self.assertEqual(response.headers.get("Referrer-Policy"), "no-referrer")
        self.assertEqual(response.headers.get("X-Frame-Options"), "DENY")
        response.get_data()
        response.close()

        for path in ("/help.html", "/privacy.html", "/terms.html", "/sitemap.xml", "/assets/drops_status_example.png"):
            allowed = self.client.get(path)
            self.assertEqual(allowed.status_code, 200, msg=path)
            self.assertEqual(allowed.headers.get("X-Content-Type-Options"), "nosniff", msg=path)
            allowed.get_data()
            allowed.close()

    def test_livez_is_always_ok_and_no_store(self):
        response = self.client.get("/livez")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["live"])
        self.assertEqual(payload["ingress_mode"], "embedded_waitress")
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, max-age=0")

    def test_repo_files_and_dotfiles_are_not_public_routes(self):
        for path in (
            "/.env",
            "/.git/config",
            "/.python-version",
            "/README.md",
            "/babblebox/web.py",
            "/tests/test_premium_web_routes.py",
            "/index.html",
            "/banner.png",
        ):
            response = self.client.get(path)
            self.assertEqual(response.status_code, 404, msg=path)

    def test_callback_failure_does_not_leak_internal_error_and_sets_no_store_headers(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_premium_coroutine", side_effect=RuntimeError("client_secret leaked")):
            response = self.client.get("/premium/patreon/callback?state=state-1")
        body = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 500)
        self.assertIn("could not finish Patreon linking safely right now", body)
        self.assertNotIn("client_secret leaked", body)
        self.assertEqual(response.headers.get("Cache-Control"), "no-store, max-age=0")
        self.assertEqual(response.headers.get("Pragma"), "no-cache")
        self.assertEqual(response.headers.get("Referrer-Policy"), "no-referrer")
        self.assertEqual(response.headers.get("X-Frame-Options"), "DENY")

    def test_webhook_invalid_signature_response_does_not_leak_details(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_premium_coroutine", side_effect=WebhookVerificationError("signature mismatch detail")):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{}",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "bad"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["message"], "Patreon webhook signature was invalid.")
        self.assertNotIn("detail", payload["message"])
        stats = web.get_patreon_webhook_stats()
        self.assertEqual(stats["invalid"], 1)
        self.assertEqual(stats["invalid_signature_count"], 1)
        self.assertEqual(stats["last_status"], "invalid")
        self.assertEqual(stats["last_http_status"], 400)

    def test_webhook_rejects_oversized_payload(self):
        self._attach_runtime(loop=_LoopStub())
        response = self.client.post(
            "/premium/patreon/webhook",
            data=(b"x" * (web.PREMIUM_WEBHOOK_MAX_BYTES + 1)),
            headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
        )
        payload = response.get_json()
        self.assertEqual(response.status_code, 413)
        self.assertEqual(payload["message"], "Patreon webhook payload exceeded the safe size limit.")

    def test_webhook_storage_unavailable_maps_to_503(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(
            web,
            "_run_premium_coroutine",
            return_value=PatreonWebhookResult("unavailable", "Premium is temporarily unavailable because Babblebox could not reach its premium database."),
        ):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{}",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "unavailable")
        self.assertIn("temporarily unavailable", payload["message"])
        stats = web.get_patreon_webhook_stats()
        self.assertEqual(stats["unavailable"], 1)
        self.assertEqual(stats["last_http_status"], 503)

    def test_webhook_provider_misconfiguration_maps_to_503(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(
            web,
            "_run_premium_coroutine",
            return_value=PatreonWebhookResult("unavailable", "Patreon premium linking is not configured safely on this deployment."),
        ):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{}",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "unavailable")
        self.assertIn("not configured safely", payload["message"])

    def test_webhook_missing_secret_maps_to_503(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(
            web,
            "_run_premium_coroutine",
            return_value=PatreonWebhookResult("unavailable", "Patreon webhook secret is not configured."),
        ):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{}",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "unavailable")
        self.assertEqual(payload["message"], "Patreon webhook secret is not configured.")

    def test_webhook_invalid_json_maps_to_400_without_leaking_internal_text(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(
            web,
            "_run_premium_coroutine",
            side_effect=PremiumProviderError(
                "raw decoder detail",
                safe_message="Patreon webhook payload was invalid.",
                status_code=400,
            ),
        ):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["status"], "invalid")
        self.assertEqual(payload["message"], "Patreon webhook payload was invalid.")
        self.assertNotIn("decoder detail", payload["message"])

    def test_webhook_processed_duplicate_and_unresolved_outcomes_map_to_200(self):
        self._attach_runtime(loop=_LoopStub())
        for outcome, expected_status in (
            ("processed", "processed"),
            ("duplicate", "duplicate"),
            ("unresolved", "unresolved"),
        ):
            with patch.object(
                web,
                "_run_premium_coroutine",
                return_value=PatreonWebhookResult(outcome, f"Outcome: {outcome}"),
            ):
                response = self.client.post(
                    "/premium/patreon/webhook",
                    data=b"{}",
                    headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
                )
            payload = response.get_json()
            self.assertEqual(response.status_code, 200, msg=outcome)
            self.assertEqual(payload["status"], expected_status, msg=outcome)
            self.assertEqual(payload["message"], f"Outcome: {outcome}", msg=outcome)

    def test_webhook_runtime_unavailable_maps_to_503(self):
        self._attach_runtime(loop=None)
        response = self.client.post(
            "/premium/patreon/webhook",
            data=b"{}",
            headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
        )
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "unavailable")

    def test_webhook_unexpected_failure_maps_to_500_without_detail_leak(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_premium_coroutine", side_effect=RuntimeError("stack trace detail")):
            response = self.client.post(
                "/premium/patreon/webhook",
                data=b"{}",
                headers={"X-Patreon-Event": "members:update", "X-Patreon-Signature": "ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 500)
        self.assertEqual(payload["status"], "error")
        self.assertEqual(payload["message"], "Babblebox could not process the Patreon webhook safely.")
        self.assertNotIn("stack trace detail", payload["message"])

    def test_topgg_webhook_invalid_signature_response_does_not_leak_details(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_vote_coroutine", side_effect=WebhookVerificationError("signature mismatch detail")):
            response = self.client.post(
                "/topgg/webhook",
                data=b"{}",
                headers={"x-topgg-signature": "t=1,v1=bad"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["status"], "invalid")
        self.assertEqual(payload["message"], "Top.gg webhook signature was invalid.")
        self.assertNotIn("detail", payload["message"])

    def test_topgg_webhook_accepts_legacy_authorization_header(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_vote_coroutine", return_value=TopggWebhookResult("processed", "Legacy webhook ok.")):
            response = self.client.post(
                "/topgg/webhook",
                data=b"{}",
                headers={"Authorization": "legacy-shared-secret"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "processed")
        self.assertEqual(payload["message"], "Legacy webhook ok.")

    def test_topgg_webhook_processed_duplicate_and_test_outcomes_map_to_200(self):
        self._attach_runtime(loop=_LoopStub())
        for outcome in ("processed", "duplicate"):
            with patch.object(web, "_run_vote_coroutine", return_value=TopggWebhookResult(outcome, f"Outcome: {outcome}")):
                response = self.client.post(
                    "/topgg/webhook",
                    data=b"{}",
                    headers={"x-topgg-signature": "t=1,v1=ok"},
                )
            payload = response.get_json()
            self.assertEqual(response.status_code, 200, msg=outcome)
            self.assertEqual(payload["status"], outcome, msg=outcome)
            self.assertEqual(payload["message"], f"Outcome: {outcome}", msg=outcome)

    def test_topgg_webhook_invalid_and_runtime_unavailable_map_cleanly(self):
        self._attach_runtime(loop=_LoopStub())
        with patch.object(web, "_run_vote_coroutine", return_value=TopggWebhookResult("invalid", "Bad Top.gg payload.")):
            response = self.client.post(
                "/topgg/webhook",
                data=b"{}",
                headers={"x-topgg-signature": "t=1,v1=ok"},
            )
        payload = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["status"], "invalid")
        self.assertEqual(payload["message"], "Bad Top.gg payload.")

        web.set_vote_runtime(None)
        response = self.client.post(
            "/topgg/webhook",
            data=b"{}",
            headers={"x-topgg-signature": "t=1,v1=ok"},
        )
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "unavailable")

        self._attach_runtime(loop=_LoopStub())
        response = self.client.post("/topgg/webhook", data=b"{}")
        payload = response.get_json()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["status"], "invalid")
        self.assertEqual(payload["message"], "Missing Top.gg webhook verification header.")

    def test_topgg_webhook_rejects_oversized_payload(self):
        self._attach_runtime(loop=_LoopStub())
        response = self.client.post(
            "/topgg/webhook",
            data=(b"x" * (web.TOPGG_WEBHOOK_MAX_BYTES + 1)),
            headers={"x-topgg-signature": "t=1,v1=ok"},
        )
        payload = response.get_json()
        self.assertEqual(response.status_code, 413)
        self.assertEqual(payload["message"], "Top.gg webhook payload exceeded the safe size limit.")

    def test_health_is_degraded_without_runtime_attachment(self):
        with patch.dict(web.os.environ, {}, clear=False):
            web.set_premium_runtime(None)
            web.set_bot_runtime(None)
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertFalse(payload["bot_runtime_attached"])
        self.assertFalse(payload["runtime_ready"])
        self.assertFalse(payload["public_base_url_configured"])
        self.assertTrue(payload["public_premium_routes_ready"])
        self.assertIn("runtime_missing", payload["issues"])

    def test_health_reports_ready_public_premium_surface(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            self._attach_runtime(loop=_LoopStub(), storage_ready=True, patreon_configured=True)
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["public_base_url_configured"])
        self.assertTrue(payload["public_premium_routes_ready"])
        self.assertTrue(payload["premium_runtime_attached"])
        self.assertTrue(payload["premium_bot_loop_attached"])
        self.assertTrue(payload["premium_storage_ready"])
        self.assertTrue(payload["patreon_configured"])
        self.assertTrue(payload["required_services_ready"])
        self.assertEqual(payload["required_service_failures"], [])
        self.assertEqual(payload["patreon_webhook_stats"]["total"], 0)
        self.assertTrue(payload["services"]["utility"]["storage_ready"])
        self.assertEqual(payload["premium"]["provider_monitor"]["invalid_signature_count"], 0)
        self.assertEqual(payload["premium"]["startup_state"], "enabled_safe")

    def test_health_stays_ready_when_patreon_is_intentionally_disabled(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            self._attach_runtime(
                loop=_LoopStub(),
                storage_ready=True,
                patreon_configured=False,
                service_overrides={
                    "premium_service": self._service_stub(
                        bot=None,
                        storage_ready=True,
                        patreon_configured=False,
                        startup_state="disabled",
                    )
                },
            )
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["public_premium_routes_ready"])
        self.assertEqual(payload["premium"]["startup_state"], "disabled")
        self.assertEqual(payload["premium"]["provider_monitor"]["status"], "disabled")

    def test_health_reports_degraded_public_premium_surface(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            self._attach_runtime(loop=_LoopStub(), storage_ready=False, patreon_configured=True)
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertFalse(payload["public_premium_routes_ready"])
        self.assertFalse(payload["premium_storage_ready"])

    def test_health_reports_degraded_when_required_service_is_unavailable(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            self._attach_runtime(
                loop=_LoopStub(),
                storage_ready=True,
                patreon_configured=True,
                service_overrides={
                    "utility_service": self._service_stub(
                        configured_backend="postgres",
                        storage_ready=False,
                        storage_error="offline",
                    )
                },
            )
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertTrue(payload["public_premium_routes_ready"])
        self.assertFalse(payload["required_services_ready"])
        self.assertEqual(payload["required_service_failures"], ["utility"])
        self.assertFalse(payload["services"]["utility"]["storage_ready"])
        self.assertEqual(payload["services"]["utility"]["configured_backend"], "postgres")
        self.assertIn("utility_service_storage_unavailable", payload["issues"])

    def test_health_reports_stale_provider_monitor_without_failing_readiness(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            self._attach_runtime(
                loop=_LoopStub(),
                storage_ready=True,
                patreon_configured=True,
                service_overrides={
                    "premium_service": self._service_stub(
                        bot=None,
                        storage_ready=True,
                        patreon_configured=True,
                        startup_state="enabled_safe",
                        provider_monitor={
                            "status": "stale",
                            "stale": True,
                            "last_webhook_status": "error",
                            "last_webhook_http_status": 500,
                            "last_webhook_at": "2026-04-20T00:00:00+00:00",
                            "invalid_signature_count": 0,
                            "unresolved_issue_count": 1,
                            "recent_unavailable_count": 1,
                            "recent_server_error_count": 1,
                            "last_issue_type": "webhook_unresolved",
                            "last_issue_at": "2026-04-20T00:00:00+00:00",
                        },
                    )
                },
            )
            response = self.client.get("/health")
        payload = response.get_json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["premium"]["provider_monitor"]["status"], "stale")
        self.assertTrue(payload["premium"]["provider_monitor"]["stale"])

    def test_readyz_hides_raw_storage_and_provider_details(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=False):
            confessions_service = self._service_stub(
                confessions_readiness={
                    "status": "degraded",
                    "ready": False,
                    "configured_guild_count": 2,
                    "review_required_guild_count": 2,
                    "privacy_ready": False,
                    "review_ready": False,
                    "support_ready": False,
                    "review_issue_counts": {"public": 1},
                    "support_issue_counts": {"bot_missing_permissions": 1},
                    "issue_codes": ("confessions_privacy_backfill_incomplete",),
                    "guild_id": 123456789012345678,
                    "review_channel_id": 998877665544332211,
                },
            )
            premium_service = self._service_stub(
                bot=None,
                storage_ready=True,
                patreon_configured=True,
                provider_monitor={
                    "status": "degraded",
                    "last_webhook_status": "unresolved",
                    "last_webhook_http_status": 200,
                    "last_webhook_at": "2026-04-22T00:00:00+00:00",
                    "invalid_signature_count": 1,
                    "unresolved_issue_count": 2,
                    "recent_unavailable_count": 3,
                    "recent_server_error_count": 4,
                    "last_issue_type": "webhook_unresolved",
                    "last_issue_at": "2026-04-22T00:01:00+00:00",
                    "provider_user_id": "patreon-user-secret",
                    "payload_hash": "abc123",
                },
            )
            self._attach_runtime(
                loop=_LoopStub(),
                storage_ready=True,
                patreon_configured=True,
                service_overrides={
                    "premium_service": premium_service,
                    "confessions_service": confessions_service,
                    "utility_service": self._service_stub(
                        configured_backend="postgres",
                        storage_ready=False,
                        storage_error="postgresql://user:secret@db.example/babblebox",
                    ),
                },
            )
            response = self.client.get("/readyz")
        payload = response.get_json()
        rendered = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 503)
        self.assertEqual(payload["status"], "degraded")
        self.assertIn("confessions_privacy_backfill_incomplete", payload["issues"])
        self.assertNotIn("storage_error", rendered)
        self.assertNotIn("postgresql://user:secret@db.example/babblebox", rendered)
        self.assertNotIn("patreon-user-secret", rendered)
        self.assertNotIn("abc123", rendered)
        self.assertNotIn("123456789012345678", rendered)
        self.assertNotIn("998877665544332211", rendered)

    def test_run_uses_waitress_server_contract(self):
        called = {}

        def fake_serve(app, **kwargs):
            called["kwargs"] = dict(kwargs)

        with patch.object(web, "_load_waitress_server", return_value=fake_serve), patch.dict(
            web.os.environ,
            {"PORT": "12345", "BABBLEBOX_WEB_HOST": "127.0.0.1", "BABBLEBOX_WEB_THREADS": "6"},
            clear=False,
        ):
            web.run()

        self.assertEqual(called["kwargs"]["host"], "127.0.0.1")
        self.assertEqual(called["kwargs"]["port"], 12345)
        self.assertEqual(called["kwargs"]["threads"], 6)
        self.assertEqual(called["kwargs"]["ident"], "Babblebox")

    def test_bind_host_defaults_to_local_only_without_public_base_url(self):
        with patch.dict(web.os.environ, {}, clear=True):
            self.assertEqual(web._resolve_bind_host(), "127.0.0.1")

    def test_bind_host_defaults_to_public_when_public_base_url_is_configured(self):
        with patch.dict(web.os.environ, {"PUBLIC_BASE_URL": "https://example.test"}, clear=True):
            self.assertEqual(web._resolve_bind_host(), "0.0.0.0")

    def test_bind_host_can_be_overridden_explicitly(self):
        with patch.dict(web.os.environ, {"BABBLEBOX_WEB_HOST": "0.0.0.0"}, clear=True):
            self.assertEqual(web._resolve_bind_host(), "0.0.0.0")
