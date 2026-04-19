from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import aiohttp

from babblebox.premium_models import PLAN_GUILD_PRO, PLAN_PLUS, PLAN_SUPPORTER, PROVIDER_PATREON, PatreonIdentity
from babblebox.premium_provider import OAuthExchangeError, PremiumProviderAdapter, PremiumProviderError, WebhookVerificationError


PATREON_AUTHORIZE_URL = "https://www.patreon.com/oauth2/authorize"
PATREON_TOKEN_URL = "https://www.patreon.com/api/oauth2/token"
PATREON_IDENTITY_URL = "https://www.patreon.com/api/oauth2/v2/identity"
PATREON_WEBHOOKS_URL = "https://www.patreon.com/api/oauth2/v2/webhooks"
PATREON_SCOPES = (
    "identity",
    "identity[email]",
    "identity.memberships",
    "campaigns.members",
    "w:campaigns.webhook",
)
PATREON_GRACE_DAYS = 7
PATREON_STALE_HOURS = 24


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class PatreonPremiumProvider(PremiumProviderAdapter):
    provider_name = PROVIDER_PATREON

    def __init__(self):
        self.client_id = os.getenv("PATREON_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("PATREON_CLIENT_SECRET", "").strip()
        self.redirect_uri = os.getenv("PATREON_REDIRECT_URI", "").strip()
        self.campaign_id = os.getenv("PATREON_CAMPAIGN_ID", "").strip()
        self.webhook_uri = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
        self.supporter_tier_ids = self._parse_tier_env("PATREON_SUPPORTER_TIER_IDS")
        self.plus_tier_ids = self._parse_tier_env("PATREON_PLUS_TIER_IDS")
        self.guild_pro_tier_ids = self._parse_tier_env("PATREON_GUILD_PRO_TIER_IDS")
        self.creator_access_token = os.getenv("PATREON_CREATOR_ACCESS_TOKEN", "").strip()
        timeout = aiohttp.ClientTimeout(total=30)
        self._session: aiohttp.ClientSession | None = None
        self._timeout = timeout

    def _parse_tier_env(self, env_name: str) -> frozenset[str]:
        raw = os.getenv(env_name, "").strip()
        if not raw:
            return frozenset()
        return frozenset(part.strip() for part in raw.replace(";", ",").split(",") if part.strip())

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self):
        if self._session is not None:
            await self._session.close()

    def configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.redirect_uri)

    def automation_ready(self) -> bool:
        return self.configured() and bool(self.campaign_id)

    def build_authorize_url(self, *, state_token: str) -> str:
        if not self.configured():
            raise PremiumProviderError("Patreon OAuth is not configured.")
        query = urlencode(
            {
                "response_type": "code",
                "client_id": self.client_id,
                "redirect_uri": self.redirect_uri,
                "scope": " ".join(PATREON_SCOPES),
                "state": state_token,
            }
        )
        return f"{PATREON_AUTHORIZE_URL}?{query}"

    async def exchange_code(self, *, code: str) -> dict:
        session = await self._get_session()
        payload = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
        }
        async with session.post(PATREON_TOKEN_URL, data=payload) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                raise OAuthExchangeError(str(data))
            return data if isinstance(data, dict) else {}

    async def refresh_access_token(self, *, refresh_token: str) -> dict:
        session = await self._get_session()
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        async with session.post(PATREON_TOKEN_URL, data=payload) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                raise OAuthExchangeError(str(data))
            return data if isinstance(data, dict) else {}

    async def fetch_identity(self, *, access_token: str) -> PatreonIdentity:
        session = await self._get_session()
        params = {
            "include": "memberships.campaign,memberships.currently_entitled_tiers",
            "fields[user]": "email,full_name",
            "fields[member]": "patron_status,last_charge_status,last_charge_date,next_charge_date,pledge_relationship_start",
            "fields[tier]": "title,amount_cents",
            "fields[campaign]": "creation_name",
        }
        async with session.get(
            PATREON_IDENTITY_URL,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                raise PremiumProviderError(str(data))
        if not isinstance(data, dict):
            raise PremiumProviderError("Patreon identity payload was not an object.")
        user = data.get("data") or {}
        included = list(data.get("included") or [])
        memberships = [item for item in included if item.get("type") == "member"]
        tiers_by_id = {str(item.get("id")): item for item in included if item.get("type") == "tier"}
        selected_member = None
        for member in memberships:
            relationships = member.get("relationships") or {}
            campaign_ref = ((relationships.get("campaign") or {}).get("data") or {}).get("id")
            if self.campaign_id and str(campaign_ref) != self.campaign_id:
                continue
            selected_member = member
            break
        if selected_member is None and memberships:
            selected_member = memberships[0]

        raw_tiers: list[dict[str, Any]] = []
        tier_ids: list[str] = []
        if isinstance(selected_member, dict):
            tier_refs = (((selected_member.get("relationships") or {}).get("currently_entitled_tiers") or {}).get("data") or [])
            for tier_ref in tier_refs:
                tier_id = str((tier_ref or {}).get("id") or "").strip()
                if not tier_id:
                    continue
                tier_ids.append(tier_id)
                tier = tiers_by_id.get(tier_id)
                if isinstance(tier, dict):
                    raw_tiers.append(tier)
        plan_codes: list[str] = []
        if any(tier_id in self.supporter_tier_ids for tier_id in tier_ids):
            plan_codes.append(PLAN_SUPPORTER)
        if any(tier_id in self.plus_tier_ids for tier_id in tier_ids):
            plan_codes.append(PLAN_PLUS)
        if any(tier_id in self.guild_pro_tier_ids for tier_id in tier_ids):
            plan_codes.append(PLAN_GUILD_PRO)
        user_attrs = user.get("attributes") or {}
        member_attrs = (selected_member or {}).get("attributes") or {}
        return PatreonIdentity(
            provider_user_id=str(user.get("id") or ""),
            email=str(user_attrs.get("email") or "").strip() or None,
            display_name=str(user_attrs.get("full_name") or "").strip() or None,
            member_id=str((selected_member or {}).get("id") or "").strip() or None,
            plan_codes=tuple(sorted(set(plan_codes))),
            patron_status=str(member_attrs.get("patron_status") or "").strip() or None,
            tier_ids=tuple(sorted(set(tier_ids))),
            next_charge_date=_parse_datetime(member_attrs.get("next_charge_date")),
            raw_user=user,
            raw_member=selected_member if isinstance(selected_member, dict) else None,
            raw_tiers=tuple(raw_tiers),
        )

    def entitlement_timestamps(self, *, identity: PatreonIdentity) -> tuple[datetime, datetime, datetime | None]:
        now = _utcnow()
        current_period_end = identity.next_charge_date
        stale_after = now + timedelta(hours=PATREON_STALE_HOURS)
        grace_until = max(current_period_end or now, now + timedelta(days=PATREON_GRACE_DAYS))
        return stale_after, grace_until, current_period_end

    def verify_webhook(self, *, body: bytes, signature: str, secret: str) -> None:
        computed = hmac.new(secret.encode("utf-8"), body, hashlib.md5).hexdigest()
        if not hmac.compare_digest(computed, str(signature or "").strip()):
            raise WebhookVerificationError("Patreon webhook signature did not match.")

