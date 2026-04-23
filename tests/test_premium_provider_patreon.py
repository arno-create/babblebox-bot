import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from babblebox.premium_provider import PremiumProviderError
from babblebox.premium_provider_patreon import PatreonPremiumProvider


class _FakeResponse:
    def __init__(self, *, status: int, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        return self._payload


class _FakeSession:
    def __init__(self, *, status: int, payload):
        self._status = status
        self._payload = payload

    def get(self, *args, **kwargs):
        return _FakeResponse(status=self._status, payload=self._payload)


class PatreonPremiumProviderTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env = {
            "PATREON_CLIENT_ID": "client",
            "PATREON_CLIENT_SECRET": "secret",
            "PUBLIC_BASE_URL": "https://example.test",
            "PATREON_REDIRECT_URI": "https://example.test/premium/patreon/callback",
            "PATREON_WEBHOOK_SECRET": "webhook-secret",
            "PATREON_CAMPAIGN_ID": "1234",
            "PATREON_PLUS_TIER_IDS": "9876",
        }

    def test_build_authorize_url_requests_campaign_scope(self):
        with patch.dict("os.environ", self.env, clear=False):
            provider = PatreonPremiumProvider()

        authorize_url = provider.build_authorize_url(state_token="state-1")
        query = parse_qs(urlsplit(authorize_url).query)
        scopes = set(str(query["scope"][0]).split())

        self.assertIn("identity.memberships", scopes)
        self.assertIn("campaigns", scopes)
        self.assertIn("campaigns.members", scopes)

    async def test_fetch_identity_missing_configured_campaign_is_actionable(self):
        payload = {
            "data": {
                "id": "patreon-user-1",
                "type": "user",
                "attributes": {"email": "user@example.com", "full_name": "User"},
            },
            "included": [
                {
                    "id": "member-9999",
                    "type": "member",
                    "attributes": {"patron_status": "active_patron"},
                    "relationships": {"campaign": {"data": {"id": "9999"}}},
                }
            ],
        }
        with patch.dict("os.environ", self.env, clear=False):
            provider = PatreonPremiumProvider()

        async def _fake_get_session():
            return _FakeSession(status=200, payload=payload)

        provider._get_session = _fake_get_session

        with self.assertRaises(PremiumProviderError) as raised:
            await provider.fetch_identity(access_token="token-1")

        self.assertEqual(raised.exception.provider_code, "campaign_membership_missing")
        self.assertIn("not the creator account", raised.exception.safe_message)

    async def test_fetch_identity_keeps_unmapped_legacy_tier_ids_without_guessing_plan(self):
        payload = {
            "data": {
                "id": "patreon-user-1",
                "type": "user",
                "attributes": {"email": "user@example.com", "full_name": "User"},
            },
            "included": [
                {
                    "id": "member-1234",
                    "type": "member",
                    "attributes": {
                        "patron_status": "active_patron",
                        "next_charge_date": "2026-05-01T00:00:00+00:00",
                    },
                    "relationships": {
                        "campaign": {"data": {"id": "1234"}},
                        "currently_entitled_tiers": {"data": [{"id": "5555", "type": "tier"}]},
                    },
                },
                {
                    "id": "5555",
                    "type": "tier",
                    "attributes": {"title": "Legacy Tier", "amount_cents": 999},
                },
            ],
        }
        with patch.dict("os.environ", self.env, clear=False):
            provider = PatreonPremiumProvider()

        async def _fake_get_session():
            return _FakeSession(status=200, payload=payload)

        provider._get_session = _fake_get_session

        identity = await provider.fetch_identity(access_token="token-legacy")

        self.assertEqual(identity.provider_user_id, "patreon-user-1")
        self.assertEqual(identity.tier_ids, ("5555",))
        self.assertEqual(identity.plan_codes, ())
        self.assertEqual(len(identity.raw_tiers), 1)
        self.assertEqual(identity.raw_tiers[0]["id"], "5555")


if __name__ == "__main__":
    unittest.main()
