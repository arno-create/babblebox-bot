from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode, urlsplit

import aiohttp

from babblebox.premium_models import PLAN_GUILD_PRO, PLAN_PLUS, PLAN_SUPPORTER, PROVIDER_PATREON, PatreonIdentity
from babblebox.premium_provider import OAuthExchangeError, PremiumProviderAdapter, PremiumProviderError, WebhookVerificationError


PATREON_AUTHORIZE_URL = "https://www.patreon.com/oauth2/authorize"
PATREON_TOKEN_URL = "https://www.patreon.com/api/oauth2/token"
PATREON_IDENTITY_URL = "https://www.patreon.com/api/oauth2/v2/identity"
PATREON_SCOPES = (
    "identity",
    "identity[email]",
    "identity.memberships",
    "campaigns",
    "campaigns.members",
)
PATREON_GRACE_DAYS = 7
PATREON_STALE_HOURS = 24

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1"})
_HEX_MD5_RE = re.compile(r"^[A-Fa-f0-9]{32}$")
_PATREON_ID_RE = re.compile(r"^[0-9]+$")


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


def _is_local_hostname(hostname: str | None) -> bool:
    host = str(hostname or "").strip().casefold()
    return bool(host) and (host in _LOCAL_HOSTS or host.endswith(".local"))


def _default_port(scheme: str) -> int | None:
    if scheme == "https":
        return 443
    if scheme == "http":
        return 80
    return None


def _normalized_origin(parsed) -> tuple[str, str, int | None]:
    scheme = str(parsed.scheme or "").strip().casefold()
    hostname = str(parsed.hostname or "").strip().casefold()
    port = parsed.port or _default_port(scheme)
    return scheme, hostname, port


def _provider_error_code(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("error", "code", "type"):
            value = str(data.get(key) or "").strip()
            if value:
                return value
        errors = data.get("errors")
        if isinstance(errors, list):
            for item in errors:
                if not isinstance(item, dict):
                    continue
                for key in ("code", "id", "title"):
                    value = str(item.get(key) or "").strip()
                    if value:
                        return value
    return None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


class PatreonPremiumProvider(PremiumProviderAdapter):
    provider_name = PROVIDER_PATREON

    def __init__(self):
        self.client_id = os.getenv("PATREON_CLIENT_ID", "").strip()
        self.client_secret = os.getenv("PATREON_CLIENT_SECRET", "").strip()
        self.redirect_uri = os.getenv("PATREON_REDIRECT_URI", "").strip()
        self.public_base_url = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")
        self.webhook_secret = os.getenv("PATREON_WEBHOOK_SECRET", "").strip()
        self.campaign_id = os.getenv("PATREON_CAMPAIGN_ID", "").strip()
        self.supporter_tier_ids = self._parse_tier_env("PATREON_SUPPORTER_TIER_IDS")
        self.plus_tier_ids = self._parse_tier_env("PATREON_PLUS_TIER_IDS")
        self.guild_pro_tier_ids = self._parse_tier_env("PATREON_GUILD_PRO_TIER_IDS")
        timeout = aiohttp.ClientTimeout(total=30)
        self._session: aiohttp.ClientSession | None = None
        self._timeout = timeout
        self._configuration_errors = self._validate_configuration()

    def _parse_tier_env(self, env_name: str) -> frozenset[str]:
        raw = os.getenv(env_name, "").strip()
        if not raw:
            return frozenset()
        return frozenset(part.strip() for part in raw.replace(";", ",").split(",") if part.strip())

    def _validate_url(self, raw: str, *, label: str) -> tuple[Any | None, list[str]]:
        errors: list[str] = []
        cleaned = str(raw or "").strip()
        if not cleaned:
            errors.append(f"{label} is missing.")
            return None, errors
        parsed = urlsplit(cleaned)
        if not parsed.scheme or not parsed.netloc:
            errors.append(f"{label} must be an absolute URL.")
            return None, errors
        if parsed.query or parsed.fragment:
            errors.append(f"{label} must not include a query string or fragment.")
        if not _is_local_hostname(parsed.hostname) and parsed.scheme.casefold() != "https":
            errors.append(f"{label} must use https outside local development.")
        return parsed, errors

    def _validate_configuration(self) -> tuple[str, ...]:
        errors: list[str] = []
        if not self.client_id:
            errors.append("PATREON_CLIENT_ID is missing.")
        if not self.client_secret:
            errors.append("PATREON_CLIENT_SECRET is missing.")
        if not self.campaign_id:
            errors.append("PATREON_CAMPAIGN_ID is missing.")
        if not self.webhook_secret:
            errors.append("PATREON_WEBHOOK_SECRET is missing.")

        redirect_uri, redirect_errors = self._validate_url(self.redirect_uri, label="PATREON_REDIRECT_URI")
        public_base_url, public_errors = self._validate_url(self.public_base_url, label="PUBLIC_BASE_URL")
        errors.extend(redirect_errors)
        errors.extend(public_errors)
        if redirect_uri is not None and public_base_url is not None:
            if _normalized_origin(redirect_uri) != _normalized_origin(public_base_url):
                errors.append("PATREON_REDIRECT_URI must match PUBLIC_BASE_URL host, scheme, and port.")
            expected_path = f"{public_base_url.path.rstrip('/')}/premium/patreon/callback" if public_base_url.path.rstrip("/") else "/premium/patreon/callback"
            if redirect_uri.path != expected_path:
                errors.append("PATREON_REDIRECT_URI must point exactly to /premium/patreon/callback on PUBLIC_BASE_URL.")

        tier_sets = {
            "supporter": self.supporter_tier_ids,
            "plus": self.plus_tier_ids,
            "guild_pro": self.guild_pro_tier_ids,
        }
        if not any(tier_sets.values()):
            errors.append("At least one Patreon tier id must be mapped to a Babblebox plan.")
        for label, tier_ids in tier_sets.items():
            invalid_ids = sorted(tier_id for tier_id in tier_ids if not _PATREON_ID_RE.fullmatch(tier_id))
            if invalid_ids:
                errors.append(f"PATREON_{label.upper()}_TIER_IDS must contain Patreon numeric tier ids only.")
        overlaps = (
            ("supporter", "plus", self.supporter_tier_ids & self.plus_tier_ids),
            ("supporter", "guild_pro", self.supporter_tier_ids & self.guild_pro_tier_ids),
            ("plus", "guild_pro", self.plus_tier_ids & self.guild_pro_tier_ids),
        )
        for left, right, overlap in overlaps:
            if overlap:
                errors.append(f"Patreon tier ids cannot map to both {left} and {right}.")
        return tuple(errors)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def close(self):
        if self._session is not None:
            await self._session.close()

    def configuration_errors(self) -> tuple[str, ...]:
        return self._configuration_errors

    def configuration_message(self) -> str:
        if not self._configuration_errors:
            return "Patreon premium is configured."
        return self._configuration_errors[0]

    def configured(self) -> bool:
        return not self._configuration_errors

    def automation_ready(self) -> bool:
        return self.configured()

    def build_authorize_url(self, *, state_token: str) -> str:
        if not self.configured():
            raise PremiumProviderError(
                "Patreon OAuth is not configured.",
                safe_message="Patreon linking is not fully configured on this Babblebox deployment.",
            )
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

    async def _read_response_payload(self, response: aiohttp.ClientResponse) -> dict[str, Any]:
        try:
            data = await response.json(content_type=None)
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def _token_error(self, *, status_code: int, payload: dict[str, Any], during_refresh: bool) -> OAuthExchangeError:
        provider_code = _provider_error_code(payload)
        hard_failure = during_refresh and (status_code in {401, 403} or provider_code in {"invalid_grant", "invalid_token", "access_denied"})
        retryable = status_code == 429 or status_code >= 500
        safe_message = (
            "Patreon rejected the saved link token. Re-link Patreon from `/premium link`."
            if hard_failure
            else "Babblebox could not finish the Patreon token exchange safely right now."
        )
        if during_refresh and retryable:
            safe_message = "Babblebox could not refresh Patreon right now. Try again shortly."
        return OAuthExchangeError(
            "Patreon token exchange failed.",
            safe_message=safe_message,
            provider_code=provider_code,
            status_code=status_code,
            retryable=retryable,
            hard_failure=hard_failure,
        )

    async def exchange_code(self, *, code: str) -> dict:
        session = await self._get_session()
        payload = {
            "code": code,
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
        }
        try:
            async with session.post(
                PATREON_TOKEN_URL,
                data=payload,
                allow_redirects=False,
                headers={"Accept": "application/json"},
            ) as response:
                data = await self._read_response_payload(response)
                if response.status >= 400:
                    raise self._token_error(status_code=response.status, payload=data, during_refresh=False)
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise OAuthExchangeError(
                "Patreon token exchange failed.",
                safe_message="Babblebox could not reach Patreon safely right now. Try `/premium link` again shortly.",
                retryable=True,
            ) from exc

    async def refresh_access_token(self, *, refresh_token: str) -> dict:
        session = await self._get_session()
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            async with session.post(
                PATREON_TOKEN_URL,
                data=payload,
                allow_redirects=False,
                headers={"Accept": "application/json"},
            ) as response:
                data = await self._read_response_payload(response)
                if response.status >= 400:
                    raise self._token_error(status_code=response.status, payload=data, during_refresh=True)
                return data
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise OAuthExchangeError(
                "Patreon token refresh failed.",
                safe_message="Babblebox could not refresh Patreon right now. Try again shortly.",
                retryable=True,
            ) from exc

    def scopes_from_token_payload(self, payload: dict[str, Any]) -> tuple[str, ...]:
        raw_scopes = str(payload.get("scope") or "").replace(",", " ")
        scopes = sorted({part.strip() for part in raw_scopes.split() if part.strip()})
        return tuple(scopes)

    async def fetch_identity(self, *, access_token: str) -> PatreonIdentity:
        session = await self._get_session()
        params = {
            "include": "memberships.campaign,memberships.currently_entitled_tiers",
            "fields[user]": "email,full_name",
            "fields[member]": "patron_status,last_charge_status,last_charge_date,next_charge_date,pledge_relationship_start",
            "fields[tier]": "title,amount_cents",
            "fields[campaign]": "creation_name",
        }
        try:
            async with session.get(
                PATREON_IDENTITY_URL,
                headers={"Authorization": f"Bearer {access_token}", "Accept": "application/json"},
                params=params,
                allow_redirects=False,
            ) as response:
                data = await self._read_response_payload(response)
                if response.status >= 400:
                    raise PremiumProviderError(
                        "Patreon identity fetch failed.",
                        safe_message=(
                            "Patreon rejected the saved link token. Re-link Patreon from `/premium link`."
                            if response.status in {401, 403}
                            else "Babblebox could not verify Patreon membership safely right now."
                        ),
                        provider_code=_provider_error_code(data),
                        status_code=response.status,
                        retryable=response.status == 429 or response.status >= 500,
                        hard_failure=response.status in {401, 403},
                    )
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            raise PremiumProviderError(
                "Patreon identity fetch failed.",
                safe_message="Babblebox could not verify Patreon membership safely right now.",
                retryable=True,
            ) from exc
        if not isinstance(data, dict):
            raise PremiumProviderError(
                "Patreon identity payload was not an object.",
                safe_message="Patreon returned an invalid identity response. Re-link Patreon from `/premium link`.",
                hard_failure=True,
            )
        user = _json_object(data.get("data"))
        user_id = str(user.get("id") or "").strip()
        if not user_id:
            raise PremiumProviderError(
                "Patreon identity payload was missing the user id.",
                safe_message="Patreon returned an incomplete identity response. Re-link Patreon from `/premium link`.",
                hard_failure=True,
            )
        included = [item for item in list(data.get("included") or []) if isinstance(item, dict)]
        memberships = [item for item in included if item.get("type") == "member"]
        tiers_by_id = {str(item.get("id")): item for item in included if item.get("type") == "tier"}
        selected_member = None
        for member in memberships:
            relationships = _json_object(member.get("relationships"))
            campaign_ref = ((_json_object(relationships.get("campaign")).get("data")) or {}).get("id")
            if self.campaign_id and str(campaign_ref or "").strip() != self.campaign_id:
                continue
            selected_member = member
            break
        if not isinstance(selected_member, dict):
            raise PremiumProviderError(
                "Patreon identity did not include a membership for the configured campaign.",
                safe_message=(
                    "Babblebox could not find a Patreon membership for the configured campaign. "
                    "Make sure this Patreon account is subscribed to the Babblebox campaign, not the creator account, "
                    "then start again from `/premium link`."
                ),
                provider_code="campaign_membership_missing",
                hard_failure=False,
            )

        raw_tiers: list[dict[str, Any]] = []
        tier_ids: list[str] = []
        tier_refs = (_json_object(_json_object(selected_member.get("relationships")).get("currently_entitled_tiers")).get("data") or [])
        for tier_ref in tier_refs:
            tier_id = str(_json_object(tier_ref).get("id") or "").strip()
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
        user_attrs = _json_object(user.get("attributes"))
        member_attrs = _json_object(selected_member.get("attributes"))
        return PatreonIdentity(
            provider_user_id=user_id,
            email=str(user_attrs.get("email") or "").strip() or None,
            display_name=str(user_attrs.get("full_name") or "").strip() or None,
            member_id=str(selected_member.get("id") or "").strip() or None,
            plan_codes=tuple(sorted(set(plan_codes))),
            patron_status=str(member_attrs.get("patron_status") or "").strip() or None,
            tier_ids=tuple(sorted(set(tier_ids))),
            next_charge_date=_parse_datetime(member_attrs.get("next_charge_date")),
            raw_user=user,
            raw_member=selected_member,
            raw_tiers=tuple(raw_tiers),
        )

    def entitlement_timestamps(self, *, identity: PatreonIdentity) -> tuple[datetime, datetime, datetime | None]:
        now = _utcnow()
        current_period_end = identity.next_charge_date
        stale_after = now + timedelta(hours=PATREON_STALE_HOURS)
        grace_until = max(current_period_end or now, now + timedelta(days=PATREON_GRACE_DAYS))
        return stale_after, grace_until, current_period_end

    def verify_webhook(self, *, body: bytes, signature: str, secret: str) -> None:
        cleaned_signature = str(signature or "").strip()
        if not _HEX_MD5_RE.fullmatch(cleaned_signature):
            raise WebhookVerificationError(
                "Patreon webhook signature format was invalid.",
                safe_message="Patreon webhook signature was invalid.",
            )
        computed = hmac.new(secret.encode("utf-8"), body, hashlib.md5).hexdigest()
        if not hmac.compare_digest(computed, cleaned_signature):
            raise WebhookVerificationError(
                "Patreon webhook signature did not match.",
                safe_message="Patreon webhook signature did not match.",
            )
