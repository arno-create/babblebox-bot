import os
import unittest
from unittest.mock import AsyncMock, patch

from babblebox import shield_ai


def _request(**overrides):
    payload = {
        "guild_id": 10,
        "pack": "privacy",
        "local_confidence": "high",
        "local_action": "delete_log",
        "local_labels": ("Privacy Leak",),
        "local_reasons": ("Email address detected",),
        "sanitized_content": "email me at [EMAIL]",
        "sanitized_redaction_count": 1,
        "sanitized_truncated": False,
        "has_links": False,
        "domains": (),
        "has_suspicious_attachment": False,
        "attachment_extensions": (),
        "invite_detected": False,
        "repetitive_promo": False,
        "allowed_models": shield_ai.SHIELD_AI_MODEL_ORDER,
    }
    payload.update(overrides)
    return shield_ai.ShieldAIReviewRequest(**payload)


def _provider_payload():
    return {
        "choices": [
            {
                "message": {
                    "content": (
                        '{"classification":"privacy_leak","confidence":"high","priority":"normal",'
                        '"false_positive":false,"explanation":"Likely real contact detail."}'
                    )
                }
            }
        ]
    }


class ShieldAITests(unittest.IsolatedAsyncioTestCase):
    def test_sanitize_message_redacts_private_patterns(self):
        result = shield_ai.sanitize_message_for_ai(
            "Email me at friend@example.com, call +1 555 123 4567, and visit https://discord.gg/example",
            max_chars=200,
        )

        self.assertIn("[EMAIL]", result.text)
        self.assertIn("[PHONE]", result.text)
        self.assertIn("[LINK]", result.text)
        self.assertGreaterEqual(result.redaction_count, 3)

    def test_sanitize_message_truncates_safely(self):
        result = shield_ai.sanitize_message_for_ai("word " * 80, max_chars=80)

        self.assertTrue(result.truncated)
        self.assertLessEqual(len(result.text), 80)
        self.assertTrue(result.text.endswith("..."))

    def test_support_guild_gate_is_explicit(self):
        self.assertTrue(shield_ai.shield_ai_available_in_guild(shield_ai.SHIELD_AI_SUPPORT_GUILD_ID))
        self.assertFalse(shield_ai.shield_ai_available_in_guild(10))

    def test_parse_model_list_accepts_aliases_and_canonical_names(self):
        parsed = shield_ai.parse_shield_ai_model_list(["mini", "gpt-5", "nano", "mini"])

        self.assertEqual(parsed, ("gpt-5-nano", "gpt-5-mini", "gpt-5"))

    def test_route_defaults_to_fast_for_simple_case(self):
        provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(_request())

        self.assertEqual(route.target_tier, "fast")
        self.assertEqual(route.selected_model, "gpt-5-nano")
        self.assertFalse(route.policy_capped)

    def test_route_escalates_to_complex_for_high_risk_or_ambiguous_case(self):
        provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(
            _request(
                pack="scam",
                local_confidence="medium",
                local_action="delete_escalate",
                local_labels=("Scam", "Unknown risky link"),
                has_links=True,
                domains=("example.com", "cdn.example.com"),
                invite_detected=True,
            )
        )

        self.assertEqual(route.target_tier, "complex")
        self.assertEqual(route.selected_model, "gpt-5-mini")
        self.assertIn("high_risk_pack", route.route_reasons)
        self.assertIn("high_severity_action", route.route_reasons)

    def test_top_tier_stays_dormant_when_disabled(self):
        provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(
            _request(
                pack="severe",
                local_confidence="medium",
                local_action="timeout_log",
                local_labels=("Severe", "Escalate", "Targeted abuse"),
                sanitized_redaction_count=4,
                sanitized_truncated=True,
            )
        )

        self.assertEqual(route.target_tier, "complex")
        self.assertEqual(route.selected_model, "gpt-5-mini")

    def test_route_can_reach_frontier_when_enabled_and_justified(self):
        with patch.dict(os.environ, {"SHIELD_AI_ENABLE_TOP_TIER": "true"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(
            _request(
                pack="severe",
                local_confidence="medium",
                local_action="timeout_log",
                local_labels=("Severe", "Escalate", "Targeted abuse"),
                sanitized_redaction_count=4,
                sanitized_truncated=True,
                has_suspicious_attachment=True,
            )
        )

        self.assertEqual(route.target_tier, "frontier")
        self.assertEqual(route.selected_model, "gpt-5")

    def test_diagnostics_report_frontier_capable_routing_when_top_tier_is_enabled(self):
        with patch.dict(os.environ, {"SHIELD_AI_ENABLE_TOP_TIER": "true"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        diagnostics = provider.diagnostics()

        self.assertEqual(diagnostics["routing_strategy"], "routed_fast_complex_frontier")
        self.assertTrue(diagnostics["top_tier_enabled"])

    def test_policy_cap_uses_best_allowed_lower_model(self):
        provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(
            _request(
                pack="scam",
                local_confidence="medium",
                local_action="delete_escalate",
                local_labels=("Scam", "Unknown risky link"),
                allowed_models=("gpt-5-nano",),
            )
        )

        self.assertEqual(route.target_tier, "complex")
        self.assertEqual(route.selected_model, "gpt-5-nano")
        self.assertTrue(route.policy_capped)

    def test_single_model_override_bypasses_routing_with_supported_model(self):
        with patch.dict(os.environ, {"SHIELD_AI_MODEL": "mini"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        route = provider._route_request(_request())

        self.assertTrue(route.single_model_override)
        self.assertEqual(route.selected_model, "gpt-5-mini")
        self.assertEqual(route.route_reasons, ("single_model_override",))

    def test_invalid_single_model_override_is_ignored_and_reported_truthfully(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test", "SHIELD_AI_MODEL": "gpt-4.1-mini"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        diagnostics = provider.diagnostics()

        self.assertTrue(diagnostics["available"])
        self.assertFalse(diagnostics["single_model_override"])
        self.assertEqual(diagnostics["model"], "gpt-5-nano")
        self.assertEqual(diagnostics["status"], "Ready.")
        self.assertEqual(diagnostics["ignored_model_settings"], ["SHIELD_AI_MODEL"])
        self.assertEqual(diagnostics["model_override_state"], "invalid")
        self.assertIn("Invalid override ignored: SHIELD_AI_MODEL", diagnostics["model_override_note"])
        self.assertIn("routed defaults", diagnostics["model_override_note"])
        self.assertEqual(diagnostics["routed_default_model"], "gpt-5-nano")

    def test_invalid_tier_model_settings_are_ignored_and_reported_truthfully(self):
        with patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": "",
                "SHIELD_AI_FAST_MODEL": "gpt-4.1-mini",
                "SHIELD_AI_COMPLEX_MODEL": "bad-model",
                "SHIELD_AI_TOP_MODEL": "legacy-full",
            },
            clear=False,
        ):
            provider = shield_ai.OpenAIShieldAIProvider()

        diagnostics = provider.diagnostics()

        self.assertEqual(diagnostics["fast_model"], "gpt-5-nano")
        self.assertEqual(diagnostics["complex_model"], "gpt-5-mini")
        self.assertEqual(diagnostics["top_model"], "gpt-5")
        self.assertEqual(
            diagnostics["ignored_model_settings"],
            ["SHIELD_AI_FAST_MODEL", "SHIELD_AI_COMPLEX_MODEL", "SHIELD_AI_TOP_MODEL"],
        )
        self.assertEqual(diagnostics["status"], "OpenAI API key is not configured.")
        self.assertIn("SHIELD_AI_FAST_MODEL", diagnostics["invalid_model_settings_note"])

    async def test_retryable_failure_falls_back_once(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        provider._request_completion = AsyncMock(
            side_effect=[
                shield_ai._RetryableProviderFailure("rate_limit"),
                _provider_payload(),
            ]
        )

        result = await provider.review(
            _request(
                pack="scam",
                local_confidence="medium",
                local_action="delete_escalate",
                local_labels=("Scam", "Unknown risky link"),
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(provider._request_completion.await_count, 2)
        self.assertTrue(result.fallback_used)
        self.assertEqual(result.attempted_models, ("gpt-5-mini", "gpt-5-nano"))
        self.assertEqual(result.model, "gpt-5-nano")

    async def test_timeout_failure_does_not_retry(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        provider._request_completion = AsyncMock(side_effect=shield_ai._TimeoutProviderFailure("timeout"))

        result = await provider.review(
            _request(
                pack="scam",
                local_confidence="medium",
                local_action="delete_escalate",
                local_labels=("Scam", "Unknown risky link"),
            )
        )

        self.assertIsNone(result)
        self.assertEqual(provider._request_completion.await_count, 1)

    async def test_malformed_output_does_not_retry(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test"}, clear=False):
            provider = shield_ai.OpenAIShieldAIProvider()

        provider._request_completion = AsyncMock(return_value={"choices": [{"message": {"content": "{}"}}]})

        result = await provider.review(
            _request(
                pack="scam",
                local_confidence="medium",
                local_action="delete_escalate",
                local_labels=("Scam", "Unknown risky link"),
            )
        )

        self.assertIsNone(result)
        self.assertEqual(provider._request_completion.await_count, 1)
