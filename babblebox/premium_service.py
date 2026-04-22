from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlsplit

from discord.ext import commands

from babblebox.premium_limits import (
    GUILD_LIMITS,
    LIMIT_AFK_SCHEDULES,
    LIMIT_BUMP_DETECTION_CHANNELS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_SHIELD_ALLOWLIST,
    LIMIT_SHIELD_CUSTOM_PATTERNS,
    LIMIT_SHIELD_FILTERS,
    LIMIT_SHIELD_PACK_EXEMPTIONS,
    LIMIT_SHIELD_SEVERE_TERMS,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
    guild_capabilities,
    guild_limit,
    highest_guild_plan,
    highest_user_plan,
    storage_ceiling,
    user_limit,
)
from babblebox.premium_models import (
    CLAIM_STATUS_ACTIVE,
    ENTITLEMENT_STATUS_ACTIVE,
    ENTITLEMENT_STATUS_INACTIVE,
    LINK_STATUS_ACTIVE,
    LINK_STATUS_BROKEN,
    LINK_STATUS_REVOKED,
    MANUAL_KIND_BLOCK,
    MANUAL_KIND_GRANT,
    PLAN_FREE,
    PLAN_GUILD_PRO,
    PLAN_PLUS,
    PLAN_SUPPORTER,
    PROVIDER_PATREON,
    SCOPE_GUILD,
    SCOPE_USER,
    SYSTEM_PREMIUM_CLAIM_KIND,
    SYSTEM_PREMIUM_OWNER_USER_IDS,
    SYSTEM_PREMIUM_SUPPORT_GUILD_ID,
    WEBHOOK_STATUS_FAILED,
    WEBHOOK_STATUS_PENDING,
    WEBHOOK_STATUS_PROCESSED,
    WEBHOOK_STATUS_UNRESOLVED,
)
from babblebox.premium_provider import PremiumProviderError, WebhookVerificationError
from babblebox.premium_crypto import PremiumCryptoError
from babblebox.premium_provider_patreon import PatreonPremiumProvider
from babblebox.premium_store import PremiumStorageUnavailable, PremiumStore, PremiumStoreConflict


PREMIUM_STALE_WARNING_HOURS = 24
PREMIUM_GRACE_DAYS = 7
PREMIUM_REPAIR_INTERVAL_SECONDS = 6 * 3600
PREMIUM_LINK_STATE_MINUTES = 15
PREMIUM_WEBHOOK_MAX_BYTES = 65536
PREMIUM_REPAIR_REFRESH_WINDOW_SECONDS = 10 * 60
PREMIUM_PROVIDER_MONITOR_STALE_HOURS = 48
PREMIUM_STARTUP_STATE_DISABLED = "disabled"
PREMIUM_STARTUP_STATE_ENABLED_SAFE = "enabled_safe"
PREMIUM_STARTUP_STATE_ENABLED_UNSAFE = "enabled_unsafe"
PREMIUM_PATREON_STATE_DISABLED = "disabled"
PREMIUM_PATREON_STATE_CONFIGURED = "configured"
PREMIUM_PATREON_STATE_MISCONFIGURED = "misconfigured"
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1"})

USER_LIMIT_KEYS = {
    LIMIT_WATCH_KEYWORDS,
    LIMIT_WATCH_FILTERS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_AFK_SCHEDULES,
}
GUILD_LIMIT_KEYS = set(GUILD_LIMITS[PLAN_FREE])
USER_GRANT_PLAN_CODES = frozenset({PLAN_SUPPORTER, PLAN_PLUS, PLAN_GUILD_PRO})
GUILD_GRANT_PLAN_CODES = frozenset({PLAN_GUILD_PRO})
PATREON_USER_FAMILY_PLAN_CODES = frozenset({PLAN_SUPPORTER, PLAN_PLUS})
PATREON_GUILD_FAMILY_PLAN_CODES = frozenset({PLAN_GUILD_PRO})
PATREON_AMBIGUOUS_PLAN_MESSAGE = (
    "Babblebox could not decide whether this Patreon membership should unlock personal Plus or Guild Pro. "
    "No Patreon-backed premium was granted. Contact support to review the mapped tier family."
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class PatreonWebhookResult:
    outcome: str
    message: str


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _effective_patreon_plan_codes(plan_codes: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    user_candidates = [code for code in plan_codes if code in PATREON_USER_FAMILY_PLAN_CODES]
    guild_candidates = [code for code in plan_codes if code in PATREON_GUILD_FAMILY_PLAN_CODES]
    resolved: list[str] = []
    if user_candidates:
        resolved.append(highest_user_plan(user_candidates))
    if guild_candidates:
        resolved.append(highest_guild_plan(guild_candidates))
    return tuple(resolved)


def _patreon_plan_families(plan_codes: tuple[str, ...] | list[str]) -> frozenset[str]:
    families: set[str] = set()
    if any(code in PATREON_USER_FAMILY_PLAN_CODES for code in plan_codes):
        families.add("user")
    if any(code in PATREON_GUILD_FAMILY_PLAN_CODES for code in plan_codes):
        families.add("guild")
    return frozenset(families)


class PremiumService:
    def __init__(self, bot: commands.Bot, store: PremiumStore | None = None, provider: PatreonPremiumProvider | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        self.storage_backend_preference = (
            getattr(store, "backend_preference", None)
            or (os.getenv("PREMIUM_STORAGE_BACKEND", "").strip() or "postgres")
        ).strip().lower()
        self.store: PremiumStore | None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = PremiumStore()
                self.storage_backend_preference = getattr(self.store, "backend_preference", self.storage_backend_preference)
            except PremiumStorageUnavailable as exc:
                LOGGER.warning("Premium storage constructor failed: %s", exc)
                self.store = None
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self.patreon = provider or PatreonPremiumProvider()
        self._lock = asyncio.Lock()
        self._repair_task: asyncio.Task | None = None
        self._startup_state = PREMIUM_STARTUP_STATE_DISABLED
        self._startup_validation_error: str | None = None

        self._links_by_user: dict[tuple[str, int], dict[str, Any]] = {}
        self._links_by_provider_user: dict[tuple[str, str], dict[str, Any]] = {}
        self._entitlements_by_user: dict[int, list[dict[str, Any]]] = {}
        self._entitlements_by_id: dict[str, dict[str, Any]] = {}
        self._manual_overrides_by_target: dict[tuple[str, int], list[dict[str, Any]]] = {}
        self._manual_overrides_by_id: dict[str, dict[str, Any]] = {}
        self._claims_by_guild: dict[int, dict[str, Any]] = {}
        self._claims_by_owner: dict[int, list[dict[str, Any]]] = {}
        self._provider_state: dict[str, dict[str, Any]] = {}

    def is_system_owner(self, user_id: int) -> bool:
        return int(user_id or 0) in SYSTEM_PREMIUM_OWNER_USER_IDS

    def is_support_guild(self, guild_id: int) -> bool:
        return int(guild_id or 0) == SYSTEM_PREMIUM_SUPPORT_GUILD_ID

    def _system_owner_claim_source_id(self, *, user_id: int, guild_id: int) -> str:
        return f"{SYSTEM_PREMIUM_CLAIM_KIND}:{int(user_id)}:{int(guild_id)}"

    def _patreon_configuration_state(self) -> str:
        state_getter = getattr(self.patreon, "configuration_state", None)
        if callable(state_getter):
            state = str(state_getter() or "").strip().lower()
            if state in {
                PREMIUM_PATREON_STATE_DISABLED,
                PREMIUM_PATREON_STATE_CONFIGURED,
                PREMIUM_PATREON_STATE_MISCONFIGURED,
            }:
                return state
        if self.patreon.configured():
            return PREMIUM_PATREON_STATE_CONFIGURED
        configuration_errors = tuple(getattr(self.patreon, "configuration_errors", lambda: ())() or ())
        if configuration_errors:
            return PREMIUM_PATREON_STATE_MISCONFIGURED
        return PREMIUM_PATREON_STATE_DISABLED

    def _public_base_url_is_local_only(self) -> bool:
        raw = str(os.getenv("PUBLIC_BASE_URL", "") or "").strip()
        if not raw:
            return False
        parsed = urlsplit(raw)
        hostname = str(parsed.hostname or "").strip().casefold()
        return bool(hostname) and (hostname in _LOCAL_HOSTS or hostname.endswith(".local"))

    def _startup_validation_result(self) -> tuple[str, str | None]:
        patreon_state = self._patreon_configuration_state()
        backend = getattr(self.store, "backend_name", None) or self.storage_backend_preference or "unknown"
        if patreon_state == PREMIUM_PATREON_STATE_DISABLED:
            return PREMIUM_STARTUP_STATE_DISABLED, None
        if patreon_state == PREMIUM_PATREON_STATE_MISCONFIGURED:
            return (
                PREMIUM_STARTUP_STATE_ENABLED_UNSAFE,
                (
                    "Premium startup unsafe: Patreon premium configuration is incomplete or inconsistent. "
                    f"{self._patreon_configuration_message()}"
                ),
            )
        public_base_url = str(os.getenv("PUBLIC_BASE_URL", "") or "").strip()
        if backend in {"memory", "test", "dev"} and public_base_url and not self._public_base_url_is_local_only():
            return (
                PREMIUM_STARTUP_STATE_ENABLED_UNSAFE,
                (
                    "Premium startup unsafe: Patreon-linked premium cannot use Postgres-less in-memory storage on "
                    "a non-local public deployment. Configure Postgres-backed premium storage or disable Patreon premium."
                ),
            )
        return PREMIUM_STARTUP_STATE_ENABLED_SAFE, None

    async def start(self) -> bool:
        if self._startup_storage_error is not None or self.store is None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            LOGGER.warning("Premium storage unavailable: %s", self._startup_storage_error)
            return False
        try:
            await self.store.load()
        except PremiumStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            LOGGER.warning("Premium storage unavailable: %s", exc)
            return False
        self.storage_ready = True
        self.storage_error = None
        self._startup_state, self._startup_validation_error = self._startup_validation_result()
        if self._startup_validation_error:
            self.storage_ready = False
            self.storage_error = self._startup_validation_error
            raise RuntimeError(self._startup_validation_error)
        await self._reload_cache()
        self._register_web_runtime()
        if self.patreon.configured():
            self._repair_task = asyncio.create_task(self._repair_loop(), name="babblebox-premium-repair")
        return True

    async def close(self):
        if self._repair_task is not None:
            self._repair_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._repair_task
        await self.patreon.close()
        if self.store is not None:
            await self.store.close()
        self._register_web_runtime(clear=True)

    def storage_message(self, feature_name: str = "Premium") -> str:
        return f"{feature_name} is temporarily unavailable because Babblebox could not reach its premium database."

    def _register_web_runtime(self, *, clear: bool = False):
        try:
            from babblebox import web
        except Exception:
            return
        setter = getattr(web, "set_premium_runtime", None)
        if callable(setter):
            setter(None if clear else self)

    async def _load_cache_state(self):
        if self.store is None:
            self._links_by_user = {}
            self._links_by_provider_user = {}
            self._entitlements_by_user = {}
            self._entitlements_by_id = {}
            self._manual_overrides_by_target = {}
            self._manual_overrides_by_id = {}
            self._claims_by_guild = {}
            self._claims_by_owner = {}
            self._provider_state = {}
            return
        links = await self.store.list_links()
        entitlements = await self.store.list_entitlements()
        overrides = await self.store.list_manual_overrides()
        claims = await self.store.list_active_claims()
        provider_state_rows = [await self.store.fetch_provider_state(PROVIDER_PATREON)]

        self._links_by_user = {}
        self._links_by_provider_user = {}
        for record in links:
            key = (str(record["provider"]), int(record["discord_user_id"]))
            self._links_by_user[key] = record
            provider_user_id = str(record.get("provider_user_id") or "").strip()
            if provider_user_id:
                self._links_by_provider_user[(str(record["provider"]), provider_user_id)] = record

        self._entitlements_by_user = {}
        self._entitlements_by_id = {}
        for record in entitlements:
            self._entitlements_by_id[str(record["entitlement_id"])] = record
            self._entitlements_by_user.setdefault(int(record["discord_user_id"]), []).append(record)

        self._manual_overrides_by_target = {}
        self._manual_overrides_by_id = {}
        for record in overrides:
            key = (str(record["target_type"]), int(record["target_id"]))
            self._manual_overrides_by_target.setdefault(key, []).append(record)
            self._manual_overrides_by_id[str(record["override_id"])] = record

        self._claims_by_guild = {int(record["guild_id"]): record for record in claims}
        self._claims_by_owner = {}
        for record in claims:
            self._claims_by_owner.setdefault(int(record["owner_user_id"]), []).append(record)

        self._provider_state = {}
        for row in provider_state_rows:
            if isinstance(row, dict):
                self._provider_state[str(row["provider"])] = row

    async def _reload_cache(self):
        await self._load_cache_state()
        if not self.storage_ready:
            return
        await self._reconcile_invalid_claims()
        await self._refresh_dependent_runtime()

    async def _refresh_dependent_runtime(self):
        utility_service = getattr(self.bot, "utility_service", None)
        rebuild_watch_indexes = getattr(utility_service, "_rebuild_watch_indexes", None)
        if callable(rebuild_watch_indexes):
            try:
                rebuild_watch_indexes()
            except Exception as exc:
                LOGGER.warning("Premium dependent runtime refresh failed: component=utility error_type=%s", type(exc).__name__)

        shield_service = getattr(self.bot, "shield_service", None)
        rebuild_shield_cache = getattr(shield_service, "_rebuild_config_cache", None)
        if callable(rebuild_shield_cache):
            try:
                rebuild_shield_cache()
            except Exception as exc:
                LOGGER.warning("Premium dependent runtime refresh failed: component=shield error_type=%s", type(exc).__name__)

        confessions_service = getattr(self.bot, "confessions_service", None)
        rebuild_confessions_cache = getattr(confessions_service, "_rebuild_config_cache", None)
        if callable(rebuild_confessions_cache):
            try:
                await rebuild_confessions_cache()
            except Exception as exc:
                LOGGER.warning("Premium dependent runtime refresh failed: component=confessions error_type=%s", type(exc).__name__)

    async def _audit(self, *, action: str, target_type: str, target_id: str, actor_user_id: int | None = None, detail: dict[str, Any] | None = None):
        if not self.storage_ready:
            return
        await self.store.append_audit(
            {
                "audit_id": uuid.uuid4().hex,
                "actor_user_id": actor_user_id,
                "action": action,
                "target_type": target_type,
                "target_id": target_id,
                "detail": detail or {},
                "created_at": _serialize_datetime(_utcnow()),
            }
        )

    def _manual_overrides_for(self, target_type: str, target_id: int) -> list[dict[str, Any]]:
        return list(self._manual_overrides_by_target.get((target_type, target_id), ()))

    def _active_grants_for(self, target_type: str, target_id: int) -> list[dict[str, Any]]:
        return [
            record
            for record in self._manual_overrides_for(target_type, target_id)
            if bool(record.get("active")) and record.get("kind") == MANUAL_KIND_GRANT and record.get("plan_code")
        ]

    def _manual_grant_effective(self, record: dict[str, Any] | None, *, owner_user_id: int | None = None, plan_code: str | None = None) -> bool:
        if not isinstance(record, dict):
            return False
        if not bool(record.get("active")) or record.get("kind") != MANUAL_KIND_GRANT:
            return False
        if str(record.get("target_type") or "") != SCOPE_USER:
            return False
        if owner_user_id is not None and int(record.get("target_id", 0) or 0) != owner_user_id:
            return False
        if plan_code is not None and str(record.get("plan_code") or "") != plan_code:
            return False
        return True

    def _is_blocked(self, target_type: str, target_id: int) -> bool:
        return any(
            bool(record.get("active")) and record.get("kind") == MANUAL_KIND_BLOCK
            for record in self._manual_overrides_for(target_type, target_id)
        )

    def _entitlement_state(self, record: dict[str, Any]) -> tuple[bool, bool, bool]:
        now = _utcnow()
        status = str(record.get("status") or "")
        stale_after = _parse_datetime(record.get("stale_after"))
        grace_until = _parse_datetime(record.get("grace_until"))
        effective = status == ENTITLEMENT_STATUS_ACTIVE and grace_until is not None and grace_until >= now
        stale = stale_after is not None and stale_after <= now
        in_grace = grace_until is not None and grace_until >= now
        return effective, stale, in_grace

    def _claim_source_state(self, claim: dict[str, Any]) -> tuple[bool, bool, bool, str | None]:
        if claim.get("status") != CLAIM_STATUS_ACTIVE:
            return False, False, False, "claim_inactive"
        owner_user_id = int(claim.get("owner_user_id", 0) or 0)
        source_kind = str(claim.get("source_kind") or "")
        if source_kind == SYSTEM_PREMIUM_CLAIM_KIND:
            if self.is_system_owner(owner_user_id):
                return True, False, False, None
            return False, False, False, "system_owner_access_removed"
        if source_kind == MANUAL_KIND_GRANT:
            override = self._manual_overrides_by_id.get(str(claim.get("source_id") or ""))
            if self._manual_grant_effective(override, owner_user_id=owner_user_id, plan_code=PLAN_GUILD_PRO):
                return True, False, False, None
            return False, False, False, "manual_grant_inactive"
        entitlement_id = str(claim.get("entitlement_id") or claim.get("source_id") or "")
        if not entitlement_id:
            return False, False, False, "claim_source_missing"
        record = self._entitlements_by_id.get(entitlement_id)
        if record is None:
            return False, False, False, "entitlement_missing"
        if int(record.get("discord_user_id", 0) or 0) != owner_user_id:
            return False, False, False, "entitlement_owner_mismatch"
        if str(record.get("plan_code") or "") != PLAN_GUILD_PRO:
            return False, False, False, "entitlement_plan_mismatch"
        effective, stale, in_grace = self._entitlement_state(record)
        if effective:
            return True, stale, in_grace, None
        if str(record.get("status") or "") != ENTITLEMENT_STATUS_ACTIVE:
            return False, stale, in_grace, "entitlement_inactive"
        if stale and not in_grace:
            return False, stale, in_grace, "entitlement_grace_expired"
        return False, stale, in_grace, "entitlement_not_effective"

    def _effective_claims_for_owner(self, owner_user_id: int) -> list[dict[str, Any]]:
        return [
            claim
            for claim in self._claims_by_owner.get(owner_user_id, ())
            if self._claim_source_state(claim)[0]
        ]

    def _preferred_claim_source(self, user_id: int) -> dict[str, Any] | None:
        sources = self._claimable_guild_sources(user_id)
        if not sources:
            return None
        return sorted(sources, key=lambda item: (str(item["source_kind"]), str(item["source_id"])))[0]

    async def _reconcile_invalid_claims(self):
        if not self.storage_ready or self.store is None:
            return
        releases: list[tuple[dict[str, Any], str]] = []
        rebinds: list[tuple[dict[str, Any], dict[str, Any], str]] = []
        changed_at = _utcnow()
        for claim in list(self._claims_by_guild.values()):
            if self.is_support_guild(int(claim.get("guild_id", 0) or 0)):
                released = await self.store.release_guild_claim(
                    int(claim["guild_id"]),
                    released_at=changed_at,
                    note="Auto-released because the support guild keeps permanent full-access premium.",
                )
                if released is not None:
                    releases.append((claim, "support_guild_permanent_premium"))
                continue
            effective, _stale, _in_grace, reason = self._claim_source_state(claim)
            if effective:
                continue
            rebound_source = self._preferred_claim_source(int(claim.get("owner_user_id", 0) or 0))
            if rebound_source is not None:
                rebound = await self.store.reassign_guild_claim_source(
                    int(claim["guild_id"]),
                    owner_user_id=int(claim["owner_user_id"]),
                    source_kind=str(rebound_source["source_kind"]),
                    source_id=str(rebound_source["source_id"]),
                    entitlement_id=rebound_source.get("entitlement_id"),
                    updated_at=changed_at,
                    note=f"Auto-rebound after premium source check: {reason or 'claim_source_invalid'}",
                )
                if rebound is not None:
                    rebinds.append((claim, rebound, reason or "claim_source_invalid"))
                    continue
            released = await self.store.release_guild_claim(
                int(claim["guild_id"]),
                released_at=changed_at,
                note=f"Auto-released after premium source check: {reason or 'claim_source_invalid'}",
            )
            if released is not None:
                releases.append((claim, reason or "claim_source_invalid"))
        if not releases and not rebinds:
            return
        await self._load_cache_state()
        for prior_claim, rebound_claim, reason in rebinds:
            await self._audit(
                action="guild_claim_source_rebind",
                target_type=SCOPE_GUILD,
                target_id=str(prior_claim["guild_id"]),
                detail={
                    "claim_id": prior_claim.get("claim_id"),
                    "owner_user_id": prior_claim.get("owner_user_id"),
                    "reason": reason,
                    "old_source_kind": prior_claim.get("source_kind"),
                    "old_source_id": prior_claim.get("source_id"),
                    "old_entitlement_id": prior_claim.get("entitlement_id"),
                    "new_source_kind": rebound_claim.get("source_kind"),
                    "new_source_id": rebound_claim.get("source_id"),
                    "new_entitlement_id": rebound_claim.get("entitlement_id"),
                },
            )
        for claim, reason in releases:
            await self._audit(
                action="guild_claim_auto_release",
                target_type=SCOPE_GUILD,
                target_id=str(claim["guild_id"]),
                detail={
                    "claim_id": claim.get("claim_id"),
                    "owner_user_id": claim.get("owner_user_id"),
                    "reason": reason,
                    "source_kind": claim.get("source_kind"),
                    "source_id": claim.get("source_id"),
                },
            )

    def _claimable_guild_sources(self, user_id: int) -> list[dict[str, Any]]:
        claimed_source_ids = {
            (record.get("source_kind"), record.get("source_id"))
            for record in self._effective_claims_for_owner(user_id)
        }
        sources: list[dict[str, Any]] = []
        for override in self._active_grants_for(SCOPE_USER, user_id):
            if str(override.get("plan_code")) != PLAN_GUILD_PRO:
                continue
            token = (MANUAL_KIND_GRANT, str(override["override_id"]))
            if token in claimed_source_ids:
                continue
            sources.append(
                {
                    "source_kind": MANUAL_KIND_GRANT,
                    "source_id": str(override["override_id"]),
                    "entitlement_id": None,
                }
            )
        for record in self._entitlements_by_user.get(user_id, []):
            if str(record.get("plan_code")) != PLAN_GUILD_PRO:
                continue
            effective, _stale, _grace = self._entitlement_state(record)
            if not effective:
                continue
            token = ("entitlement", str(record["entitlement_id"]))
            if token in claimed_source_ids:
                continue
            sources.append(
                {
                    "source_kind": "entitlement",
                    "source_id": str(record["entitlement_id"]),
                    "entitlement_id": str(record["entitlement_id"]),
                }
            )
        return sources

    def get_user_snapshot(self, user_id: int) -> dict[str, Any]:
        if self.is_system_owner(user_id):
            return {
                "scope": SCOPE_USER,
                "target_id": user_id,
                "plan_code": PLAN_PLUS,
                "active_plans": (PLAN_PLUS, PLAN_GUILD_PRO),
                "blocked": False,
                "stale": False,
                "in_grace": False,
                "linked": (PROVIDER_PATREON, user_id) in self._links_by_user,
                "claimable_sources": (),
                "system_access": True,
                "system_access_scope": "owner",
                "system_guild_claims": "unlimited",
            }
        if self._is_blocked(SCOPE_USER, user_id):
            return {
                "scope": SCOPE_USER,
                "target_id": user_id,
                "plan_code": PLAN_FREE,
                "active_plans": (),
                "blocked": True,
                "stale": False,
                "in_grace": False,
                "linked": (PROVIDER_PATREON, user_id) in self._links_by_user,
                "claimable_sources": (),
                "system_access": False,
                "system_access_scope": None,
                "system_guild_claims": None,
            }
        plans: set[str] = set()
        stale = False
        in_grace = False
        for override in self._active_grants_for(SCOPE_USER, user_id):
            plan_code = str(override.get("plan_code") or "")
            if plan_code and plan_code != PLAN_GUILD_PRO:
                plans.add(plan_code)
        for record in self._entitlements_by_user.get(user_id, []):
            effective, row_stale, row_grace = self._entitlement_state(record)
            stale = stale or row_stale
            in_grace = in_grace or row_grace
            if not effective:
                continue
            plan_code = str(record.get("plan_code") or "")
            if plan_code and plan_code != PLAN_GUILD_PRO:
                plans.add(plan_code)
        claimable_sources = tuple(self._claimable_guild_sources(user_id))
        return {
            "scope": SCOPE_USER,
            "target_id": user_id,
            "plan_code": highest_user_plan(plans),
            "active_plans": tuple(sorted(plans)),
            "blocked": False,
            "stale": stale,
            "in_grace": in_grace,
            "linked": (PROVIDER_PATREON, user_id) in self._links_by_user,
            "claimable_sources": claimable_sources,
            "system_access": False,
            "system_access_scope": None,
            "system_guild_claims": None,
        }

    def get_guild_snapshot(self, guild_id: int) -> dict[str, Any]:
        if self.is_support_guild(guild_id):
            claim = self._claims_by_guild.get(guild_id)
            active_claim = claim if claim is not None and claim.get("status") == CLAIM_STATUS_ACTIVE else None
            return {
                "scope": SCOPE_GUILD,
                "target_id": guild_id,
                "plan_code": PLAN_GUILD_PRO,
                "active_plans": (PLAN_GUILD_PRO,),
                "blocked": False,
                "stale": False,
                "in_grace": False,
                "claim": active_claim,
                "system_access": True,
                "system_access_scope": "support_guild",
            }
        if self._is_blocked(SCOPE_GUILD, guild_id):
            return {
                "scope": SCOPE_GUILD,
                "target_id": guild_id,
                "plan_code": PLAN_FREE,
                "active_plans": (),
                "blocked": True,
                "stale": False,
                "in_grace": False,
                "claim": None,
                "system_access": False,
                "system_access_scope": None,
            }
        plans: set[str] = set()
        stale = False
        in_grace = False
        for override in self._active_grants_for(SCOPE_GUILD, guild_id):
            plan_code = str(override.get("plan_code") or "")
            if plan_code:
                plans.add(plan_code)
        claim = self._claims_by_guild.get(guild_id)
        effective_claim = None
        if claim is not None and claim.get("status") == CLAIM_STATUS_ACTIVE:
            claim_effective, claim_stale, claim_in_grace, _claim_reason = self._claim_source_state(claim)
            stale = stale or claim_stale
            in_grace = in_grace or claim_in_grace
            if claim_effective:
                effective_claim = claim
        if effective_claim is not None:
            plans.add(str(effective_claim.get("plan_code") or PLAN_FREE))
            entitlement_id = effective_claim.get("entitlement_id")
            if entitlement_id:
                record = self._entitlements_by_id.get(str(entitlement_id))
                if record is not None:
                    _effective, row_stale, row_grace = self._entitlement_state(record)
                    stale = stale or row_stale
                    in_grace = in_grace or row_grace
        return {
            "scope": SCOPE_GUILD,
            "target_id": guild_id,
            "plan_code": highest_guild_plan(plans),
            "active_plans": tuple(sorted(plans)),
            "blocked": False,
            "stale": stale,
            "in_grace": in_grace,
            "claim": effective_claim,
            "system_access": False,
            "system_access_scope": None,
        }

    def get_link(self, user_id: int, *, provider: str = PROVIDER_PATREON) -> dict[str, Any] | None:
        record = self._links_by_user.get((provider, user_id))
        return dict(record) if isinstance(record, dict) else None

    def list_cached_entitlements_for_user(self, user_id: int) -> list[dict[str, Any]]:
        return [dict(record) for record in self._entitlements_by_user.get(user_id, ())]

    def list_cached_manual_overrides(self, *, target_type: str | None = None, target_id: int | None = None) -> list[dict[str, Any]]:
        if target_type is None and target_id is None:
            groups = self._manual_overrides_by_target.values()
            return [dict(record) for group in groups for record in group]
        if target_type is None or target_id is None:
            return []
        return [dict(record) for record in self._manual_overrides_for(target_type, target_id)]

    def list_cached_claims_for_user(self, owner_user_id: int) -> list[dict[str, Any]]:
        return [dict(record) for record in self._claims_by_owner.get(owner_user_id, ())]

    def provider_diagnostics(self) -> dict[str, Any]:
        crypto_status = getattr(self.store, "crypto", None) if self.store is not None else None
        crypto_meta = getattr(crypto_status, "status", None)
        patreon_state = self._patreon_configuration_state()
        return {
            "storage_ready": self.storage_ready,
            "storage_error": self.storage_error,
            "storage_backend": getattr(self.store, "backend_name", "unavailable") if self.store is not None else "unavailable",
            "database_url": self.store.redacted_database_url() if self.store is not None and hasattr(self.store, "redacted_database_url") else "unknown",
            "crypto_source": getattr(crypto_meta, "source", "unknown"),
            "crypto_ephemeral": bool(getattr(crypto_meta, "ephemeral", False)),
            "patreon_configured": self.patreon.configured(),
            "patreon_sync_ready": self.patreon.automation_ready(),
            "patreon_config_errors": tuple(getattr(self.patreon, "configuration_errors", lambda: ())()),
            "patreon_state": patreon_state,
            "startup_state": self._startup_state,
            "startup_validation_error": self._startup_validation_error,
            "link_count": len(self._links_by_user),
            "entitlement_count": len(self._entitlements_by_id),
            "active_claim_count": len(self._claims_by_guild),
            "provider_monitor": self.public_provider_monitor_summary(),
            "provider_state": dict(self._provider_state.get(PROVIDER_PATREON, {})),
        }

    def public_provider_monitor_summary(self, provider: str = PROVIDER_PATREON) -> dict[str, Any]:
        payload = dict((self._provider_state.get(provider, {}) or {}).get("payload") or {})
        monitor = dict(payload.get("webhook_monitor") or {})
        last_issue = dict(payload.get("last_issue") or {})
        invalid_signature_count = int(monitor.get("invalid_signature_count", 0) or 0)
        recent_unavailable_count = int(monitor.get("recent_unavailable_count", 0) or 0)
        recent_server_error_count = int(monitor.get("recent_server_error_count", 0) or 0)
        unresolved_issue_count = int(payload.get("unresolved_issue_count", 0) or 0)
        last_status = str(monitor.get("last_status") or "").strip().lower() or None
        last_webhook_at = _parse_datetime(monitor.get("last_event_at"))
        last_issue_at = _parse_datetime(last_issue.get("recorded_at"))
        stale_cutoff = _utcnow() - timedelta(hours=PREMIUM_PROVIDER_MONITOR_STALE_HOURS)
        stale = any(timestamp is not None and timestamp <= stale_cutoff for timestamp in (last_webhook_at, last_issue_at))
        status = "ready"
        if not self.storage_ready:
            status = "unavailable"
        elif provider == PROVIDER_PATREON and self._patreon_configuration_state() == PREMIUM_PATREON_STATE_DISABLED:
            status = "disabled"
        elif provider == PROVIDER_PATREON and self._patreon_configuration_state() == PREMIUM_PATREON_STATE_MISCONFIGURED:
            status = "misconfigured"
        elif stale and (monitor or last_issue):
            status = "stale"
        elif last_status in {"invalid", "unresolved", "unavailable", "error"}:
            status = "degraded"
        return {
            "status": status,
            "last_webhook_status": last_status,
            "last_webhook_http_status": monitor.get("last_http_status"),
            "last_webhook_at": monitor.get("last_event_at"),
            "invalid_signature_count": invalid_signature_count,
            "unresolved_issue_count": unresolved_issue_count,
            "recent_unavailable_count": recent_unavailable_count,
            "recent_server_error_count": recent_server_error_count,
            "last_issue_type": last_issue.get("issue_type"),
            "last_issue_at": last_issue.get("recorded_at"),
            "stale": stale,
        }

    def resolve_user_limit(self, user_id: int, limit_key: str) -> int:
        return user_limit(self.get_user_snapshot(user_id)["plan_code"], limit_key)

    def resolve_guild_limit(self, guild_id: int, limit_key: str) -> int:
        return guild_limit(self.get_guild_snapshot(guild_id)["plan_code"], limit_key)

    def guild_has_capability(self, guild_id: int, capability: str) -> bool:
        snapshot = self.get_guild_snapshot(guild_id)
        return capability in guild_capabilities(snapshot["plan_code"])

    def storage_ceiling(self, limit_key: str, fallback: int) -> int:
        return storage_ceiling(limit_key, fallback)

    def get_plan_upgrade_label_for_limit(self, limit_key: str) -> str:
        if limit_key in USER_LIMIT_KEYS:
            return "Babblebox Plus"
        if limit_key in GUILD_LIMIT_KEYS:
            return "Babblebox Guild Pro"
        return "Babblebox Premium"

    def describe_limit_error(self, *, limit_key: str, limit_value: int) -> str:
        return (
            f"You reached the current limit of {limit_value}. "
            f"{self.get_plan_upgrade_label_for_limit(limit_key)} unlocks more. "
            "Use `/premium plans` to compare tiers."
        )

    def over_limit_label(self, *, current_count: int, limit_value: int) -> str | None:
        if current_count <= limit_value:
            return None
        return f"Over current plan limit: {current_count} saved while this plan allows {limit_value}."

    def plan_catalog(self) -> tuple[dict[str, Any], ...]:
        return (
            {
                "plan_code": PLAN_SUPPORTER,
                "title": "Supporter",
                "audience": "People who want to support Babblebox without changing the product lane.",
                "summary": "Recognition-focused support tier with no power unlocks.",
                "unlocks": (
                    "Visible premium recognition in your Babblebox premium status.",
                ),
                "does_not_unlock": (
                    "Higher personal utility limits.",
                    "Guild Pro server upgrades or higher Shield AI model tiers.",
                ),
                "best_for": "Supporters who want to back the project and keep free behavior unchanged.",
            },
            {
                "plan_code": PLAN_PLUS,
                "title": "Plus",
                "audience": "People who use Babblebox as a personal utility bot inside Discord.",
                "summary": "Higher personal limits for Watch, reminders, and recurring AFK scheduling.",
                "unlocks": (
                    "Up to 25 Watch keywords and 25 Watch filters.",
                    "Up to 15 active reminders and 5 active channel reminders.",
                    "Up to 20 recurring AFK schedules.",
                ),
                "does_not_unlock": (
                    "Guild Pro server upgrades.",
                    "Server-side Shield AI model upgrades or Question Drops AI celebrations.",
                ),
                "best_for": "Members who rely on Watch, reminders, or AFK routines and want much more room.",
            },
            {
                "plan_code": PLAN_GUILD_PRO,
                "title": "Guild Pro",
                "audience": "Admins who want one server to get Babblebox's higher-cap admin lane.",
                "summary": "Server-level premium for higher bounded caps, richer admin power, and premium-only server extras.",
                "unlocks": (
                    "Up to 15 bump detection channels.",
                    "Higher bounded Shield config ceilings for patterns, filters, allowlists, exemptions, and severe terms.",
                    "Shield AI's higher gpt-5.4-mini and gpt-5.4 tiers when owner policy enables review.",
                    "Question Drops AI celebrations.",
                    "Confessions max_images up to 6.",
                ),
                "does_not_unlock": (
                    "Automatic upgrades for every server; Guild Pro still has to be claimed explicitly.",
                    "Plus utility caps for every individual user.",
                ),
                "best_for": "Servers that want one explicit premium claim with clearer admin headroom.",
            },
        )

    def plan_title(self, plan_code: str) -> str:
        return {
            PLAN_FREE: "Free",
            PLAN_SUPPORTER: "Supporter",
            PLAN_PLUS: "Plus",
            PLAN_GUILD_PRO: "Guild Pro",
        }.get(str(plan_code or PLAN_FREE), "Free")

    def _require_storage_ready(self):
        if self.storage_ready and self.store is not None:
            return
        raise PremiumStorageUnavailable(self.storage_message())

    def _validate_manual_override_request(self, *, target_type: str, kind: str, plan_code: str | None) -> str | None:
        cleaned_target = str(target_type or "").strip().lower()
        cleaned_kind = str(kind or "").strip().lower()
        cleaned_plan = str(plan_code or "").strip().lower() or None
        if cleaned_target not in {SCOPE_USER, SCOPE_GUILD}:
            raise ValueError("Premium overrides must target a user or guild.")
        if cleaned_kind not in {MANUAL_KIND_GRANT, MANUAL_KIND_BLOCK}:
            raise ValueError("Premium overrides must be a grant or block.")
        if cleaned_kind == MANUAL_KIND_BLOCK:
            if cleaned_plan is not None:
                raise ValueError("Premium suspensions cannot carry a plan code.")
            return None
        allowed_plans = USER_GRANT_PLAN_CODES if cleaned_target == SCOPE_USER else GUILD_GRANT_PLAN_CODES
        if cleaned_plan not in allowed_plans:
            if cleaned_target == SCOPE_GUILD:
                raise ValueError("Guild-level premium grants may only assign `guild_pro`.")
            raise ValueError("User-level premium grants must use `supporter`, `plus`, or `guild_pro`.")
        return cleaned_plan

    async def _store_provider_state_payload(self, provider: str, payload: dict[str, Any]):
        if not self.storage_ready or self.store is None:
            return
        await self.store.upsert_provider_state(provider, payload)
        self._provider_state[provider] = {
            "provider": provider,
            "payload": dict(payload),
            "updated_at": _serialize_datetime(_utcnow()),
        }

    async def _record_provider_issue(self, *, provider: str, issue_type: str, detail: dict[str, Any]):
        current_row = self._provider_state.get(provider, {})
        payload = dict(current_row.get("payload") or {})
        now = _serialize_datetime(_utcnow())
        recent_issues = list(payload.get("recent_issues") or [])
        requires_review = issue_type == "webhook_unresolved"
        issue_record = {"issue_type": issue_type, "recorded_at": now, "requires_review": requires_review, **detail}
        recent_issues.insert(0, issue_record)
        payload["recent_issues"] = recent_issues[:10]
        payload["last_issue"] = issue_record
        if requires_review:
            payload["unresolved_issue_count"] = int(payload.get("unresolved_issue_count", 0) or 0) + 1
        await self._store_provider_state_payload(provider, payload)

    async def record_webhook_monitor_event(self, *, status: str, status_code: int, invalid_signature: bool = False):
        if not self.storage_ready or self.store is None:
            return
        current_row = self._provider_state.get(PROVIDER_PATREON, {})
        payload = dict(current_row.get("payload") or {})
        monitor = dict(payload.get("webhook_monitor") or {})
        normalized_status = str(status or "error").strip().lower() or "error"
        now = _serialize_datetime(_utcnow())
        monitor["last_status"] = normalized_status
        monitor["last_http_status"] = int(status_code)
        monitor["last_event_at"] = now
        monitor["total_count"] = int(monitor.get("total_count", 0) or 0) + 1
        if invalid_signature:
            monitor["invalid_signature_count"] = int(monitor.get("invalid_signature_count", 0) or 0) + 1
        if normalized_status == "unavailable":
            monitor["recent_unavailable_count"] = int(monitor.get("recent_unavailable_count", 0) or 0) + 1
        if int(status_code) >= 500 and normalized_status != "unavailable":
            monitor["recent_server_error_count"] = int(monitor.get("recent_server_error_count", 0) or 0) + 1
        payload["webhook_monitor"] = monitor
        await self._store_provider_state_payload(PROVIDER_PATREON, payload)

    def _authorize_guild_actor(self, *, guild: Any, actor: Any) -> tuple[bool, int | None, int | None, str | None]:
        guild_id = int(getattr(guild, "id", 0) or 0)
        if guild_id <= 0:
            return False, None, None, "Guild Pro claims can only be changed from a live server context."
        user_id = int(getattr(actor, "id", 0) or 0)
        if user_id <= 0:
            return False, None, None, "Babblebox could not verify who is changing this Guild Pro claim."
        actor_member = actor
        actor_guild = getattr(actor, "guild", None)
        actor_guild_id = int(getattr(actor_guild, "id", 0) or 0) if actor_guild is not None else 0
        if actor_guild is not None and actor_guild_id != guild_id:
            return False, None, None, "Only members acting inside this server can change Guild Pro claims."
        if actor_guild is None:
            get_member = getattr(guild, "get_member", None)
            resolved_member = get_member(user_id) if callable(get_member) else None
            if resolved_member is None:
                return False, None, None, "Only current members of this server can change Guild Pro claims."
            actor_member = resolved_member
        perms = getattr(actor_member, "guild_permissions", None)
        if not (getattr(perms, "administrator", False) or getattr(perms, "manage_guild", False)):
            return False, None, None, "Only administrators or members with Manage Server can claim or release Guild Pro."
        return True, guild_id, user_id, None

    def _patreon_configuration_message(self) -> str:
        if self.patreon.configured():
            return "Patreon premium is configured."
        message_getter = getattr(self.patreon, "configuration_message", None)
        if callable(message_getter):
            return str(message_getter()).strip() or "Patreon linking is not fully configured on this Babblebox deployment."
        return "Patreon linking is not fully configured on this Babblebox deployment."

    def _hash_oauth_state_token(self, state_token: str) -> str:
        return hashlib.sha256(str(state_token or "").encode("utf-8")).hexdigest()

    def _safe_provider_message(self, exc: Exception, default_message: str) -> str:
        if isinstance(exc, PremiumProviderError):
            return str(exc.safe_message or default_message)
        return default_message

    def _event_error_text(self, exc: Exception, default_message: str) -> str:
        if isinstance(exc, PremiumProviderError):
            parts = [str(exc.safe_message or default_message)]
            if exc.provider_code:
                parts.append(f"code={exc.provider_code}")
            if exc.status_code is not None:
                parts.append(f"status={exc.status_code}")
            return " | ".join(parts)
        return default_message

    def _canonicalize_webhook_payload(self, payload: dict[str, Any]) -> bytes:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")

    def _extract_webhook_campaign_id(self, payload: dict[str, Any]) -> str | None:
        data = payload.get("data") or {}
        relationships = data.get("relationships") or {}
        campaign_id = ((relationships.get("campaign") or {}).get("data") or {}).get("id")
        if campaign_id:
            return str(campaign_id).strip()
        for item in list(payload.get("included") or []):
            if not isinstance(item, dict):
                continue
            if item.get("type") == "campaign":
                campaign_id = str(item.get("id") or "").strip()
                if campaign_id:
                    return campaign_id
        return None

    def _extract_webhook_tier_ids(self, payload: dict[str, Any]) -> tuple[str, ...]:
        tier_ids: set[str] = set()
        data = payload.get("data") or {}
        relationships = data.get("relationships") or {}
        tier_refs = ((relationships.get("currently_entitled_tiers") or {}).get("data") or [])
        for item in tier_refs:
            tier_id = str((item or {}).get("id") or "").strip()
            if tier_id:
                tier_ids.add(tier_id)
        return tuple(sorted(tier_ids))

    def _sanitize_webhook_payload(self, *, payload: dict[str, Any], provider_user_id: str | None, payload_hash: str) -> dict[str, Any]:
        data = payload.get("data") or {}
        return {
            "provider_user_id": provider_user_id,
            "campaign_id": self._extract_webhook_campaign_id(payload),
            "member_id": str(data.get("id") or "").strip() or None,
            "object_type": str(data.get("type") or "").strip() or None,
            "tier_ids": list(self._extract_webhook_tier_ids(payload)),
            "payload_hash": payload_hash,
        }

    async def _set_patreon_entitlements_inactive(
        self,
        *,
        user_id: int,
        now: datetime,
        stale_after: datetime,
        grace_until: datetime,
        current_period_end: datetime | None,
    ) -> set[str]:
        if self.store is None:
            return set()
        current_records = await self.store.list_entitlements_for_user(user_id, provider=PROVIDER_PATREON)
        updated_entitlement_ids: set[str] = set()
        for record in current_records:
            updated_entitlement_ids.add(str(record["entitlement_id"]))
            await self.store.upsert_entitlement(
                {
                    **record,
                    "status": ENTITLEMENT_STATUS_INACTIVE,
                    "last_verified_at": _serialize_datetime(now),
                    "stale_after": _serialize_datetime(stale_after),
                    "grace_until": _serialize_datetime(grace_until),
                    "current_period_end": _serialize_datetime(current_period_end),
                }
            )
        return updated_entitlement_ids

    async def _set_patreon_link_status(
        self,
        *,
        user_id: int,
        status: str,
        reason: str,
        scrub_tokens: bool,
    ) -> None:
        if self.store is None:
            return
        link = self._links_by_user.get((PROVIDER_PATREON, user_id))
        if link is None:
            return
        metadata = dict(link.get("metadata") or {})
        metadata["last_link_status_reason"] = reason
        metadata["last_link_status_at"] = _serialize_datetime(_utcnow())
        updated = {
            **link,
            "link_status": status,
            "updated_at": _serialize_datetime(_utcnow()),
            "access_token_ciphertext": None if scrub_tokens else link.get("access_token_ciphertext"),
            "refresh_token_ciphertext": None if scrub_tokens else link.get("refresh_token_ciphertext"),
            "token_expires_at": None if scrub_tokens else link.get("token_expires_at"),
            "metadata": metadata,
        }
        await self.store.upsert_link(updated)

    async def _revoke_patreon_access(
        self,
        *,
        user_id: int,
        reason: str,
        safe_message: str,
        action: str,
        link_status: str = LINK_STATUS_REVOKED,
    ) -> tuple[bool, str]:
        self._require_storage_ready()
        now = _utcnow()
        await self._set_patreon_entitlements_inactive(
            user_id=user_id,
            now=now,
            stale_after=now,
            grace_until=now,
            current_period_end=now,
        )
        await self._set_patreon_link_status(user_id=user_id, status=link_status, reason=reason, scrub_tokens=True)
        await self._reload_cache()
        await self._audit(
            action=action,
            target_type=SCOPE_USER,
            target_id=str(user_id),
            actor_user_id=user_id,
            detail={"reason": reason},
        )
        return False, safe_message

    async def _handle_patreon_identity_mismatch(
        self,
        *,
        user_id: int,
        stage: str,
        expected_provider_user_id: str,
        identity,
    ) -> tuple[bool, str]:
        safe_message = (
            "Patreon returned a different account than the one linked to this Discord user. "
            "Re-link Patreon from `/premium link` before Babblebox can trust provider-backed premium access."
        )
        await self._record_provider_issue(
            provider=PROVIDER_PATREON,
            issue_type="patreon_identity_mismatch",
            detail={
                "user_id": user_id,
                "stage": stage,
                "expected_provider_user_id": expected_provider_user_id,
                "observed_provider_user_id": identity.provider_user_id,
                "member_id": identity.member_id,
            },
        )
        return await self._revoke_patreon_access(
            user_id=user_id,
            reason="identity_provider_user_mismatch",
            safe_message=safe_message,
            action="patreon_identity_mismatch",
            link_status=LINK_STATUS_REVOKED,
        )

    async def _handle_patreon_sync_failure(self, *, user_id: int, stage: str, exc: Exception) -> tuple[bool, str]:
        safe_message = self._safe_provider_message(exc, "Babblebox could not verify Patreon safely right now.")
        if isinstance(exc, PremiumProviderError) and exc.provider_code == "ambiguous_plan_mapping":
            return False, safe_message
        issue_type = "patreon_sync_failure"
        if isinstance(exc, PremiumCryptoError):
            issue_type = "patreon_token_local_failure"
        elif isinstance(exc, PremiumProviderError) and exc.hard_failure:
            issue_type = "patreon_hard_auth_failure"
        await self._record_provider_issue(
            provider=PROVIDER_PATREON,
            issue_type=issue_type,
            detail={
                "user_id": user_id,
                "stage": stage,
                "message": safe_message,
                "provider_code": getattr(exc, "provider_code", None),
                "status_code": getattr(exc, "status_code", None),
            },
        )
        if isinstance(exc, PremiumCryptoError):
            return await self._revoke_patreon_access(
                user_id=user_id,
                reason=f"{stage}_token_unreadable",
                safe_message="Babblebox could not safely read the saved Patreon link token. Re-link Patreon from `/premium link`.",
                action="patreon_link_broken",
                link_status=LINK_STATUS_BROKEN,
            )
        if isinstance(exc, PremiumProviderError) and exc.hard_failure:
            return await self._revoke_patreon_access(
                user_id=user_id,
                reason=f"{stage}:{exc.provider_code or exc.status_code or 'hard_failure'}",
                safe_message=safe_message,
                action="patreon_refresh_revoke",
            )
        return False, safe_message

    async def create_link_url(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready or self.store is None:
            return False, self.storage_message()
        if not self.patreon.configured():
            return False, self._patreon_configuration_message()
        state_token = secrets.token_urlsafe(32)
        state_token_hash = self._hash_oauth_state_token(state_token)
        now = _utcnow()
        await self.store.invalidate_oauth_states(PROVIDER_PATREON, user_id, action="link")
        await self.store.create_oauth_state(
            {
                "provider": PROVIDER_PATREON,
                "state_token": state_token_hash,
                "discord_user_id": user_id,
                "action": "link",
                "created_at": _serialize_datetime(now),
                "expires_at": _serialize_datetime(now + timedelta(minutes=PREMIUM_LINK_STATE_MINUTES)),
                "consumed_at": None,
                "metadata": {},
            }
        )
        await self._audit(action="oauth_state_create", target_type="user", target_id=str(user_id), actor_user_id=user_id, detail={"provider": PROVIDER_PATREON})
        return True, self.patreon.build_authorize_url(state_token=state_token)

    def _encrypt_token(self, *, label: str, user_id: int, secret: str | None) -> str | None:
        if not secret:
            return None
        return self.store.crypto.encrypt_secret(label=label, aad_fields={"user_id": user_id}, secret=secret)

    def _decrypt_token(self, *, label: str, user_id: int, envelope: str | None) -> str | None:
        return self.store.crypto.decrypt_secret(label=label, aad_fields={"user_id": user_id}, envelope=envelope)

    async def _sync_identity(
        self,
        *,
        discord_user_id: int,
        identity,
        access_token: str,
        refresh_token: str | None,
        token_expires_at: datetime | None,
        scopes: tuple[str, ...] = (),
    ):
        existing_other = self._links_by_provider_user.get((PROVIDER_PATREON, identity.provider_user_id))
        if existing_other is not None and int(existing_other["discord_user_id"]) != discord_user_id:
            raise PremiumProviderError("That Patreon account is already linked to another Discord user.")
        now = _utcnow()
        link_record = {
            "provider": PROVIDER_PATREON,
            "discord_user_id": discord_user_id,
            "provider_user_id": identity.provider_user_id,
            "link_status": LINK_STATUS_ACTIVE,
            "linked_at": _serialize_datetime(now),
            "updated_at": _serialize_datetime(now),
            "access_token_ciphertext": self._encrypt_token(label="patreon-access", user_id=discord_user_id, secret=access_token),
            "refresh_token_ciphertext": self._encrypt_token(label="patreon-refresh", user_id=discord_user_id, secret=refresh_token),
            "token_expires_at": _serialize_datetime(token_expires_at),
            "scopes": tuple(sorted(set(scopes))),
            "email": identity.email,
            "display_name": identity.display_name,
            "metadata": {
                "member_id": identity.member_id,
                "patron_status": identity.patron_status,
                "tier_ids": list(identity.tier_ids),
            },
        }
        try:
            await self.store.upsert_link(link_record)
        except PremiumStoreConflict as exc:
            raise PremiumProviderError(
                "That Patreon account is already linked to another Discord user.",
                safe_message="That Patreon account is already linked to another Discord user.",
            ) from exc

        stale_after, grace_until, current_period_end = self.patreon.entitlement_timestamps(identity=identity)
        current = await self.store.list_entitlements_for_user(discord_user_id, provider=PROVIDER_PATREON)
        effective_plan_codes = _effective_patreon_plan_codes(identity.plan_codes)
        if _patreon_plan_families(effective_plan_codes) == frozenset({"user", "guild"}):
            await self._set_patreon_entitlements_inactive(
                user_id=discord_user_id,
                now=now,
                stale_after=now,
                grace_until=now,
                current_period_end=current_period_end,
            )
            await self._set_patreon_link_status(
                user_id=discord_user_id,
                status=LINK_STATUS_ACTIVE,
                reason="ambiguous_plan_mapping",
                scrub_tokens=False,
            )
            await self._reload_cache()
            await self._record_provider_issue(
                provider=PROVIDER_PATREON,
                issue_type="patreon_ambiguous_plan_mapping",
                detail={
                    "user_id": discord_user_id,
                    "provider_user_id": identity.provider_user_id,
                    "member_id": identity.member_id,
                    "plan_codes": list(identity.plan_codes),
                    "tier_ids": list(identity.tier_ids),
                },
            )
            await self._audit(
                action="patreon_sync_ambiguous",
                target_type="user",
                target_id=str(discord_user_id),
                actor_user_id=discord_user_id,
                detail={"plans": list(identity.plan_codes), "provider_user_id": identity.provider_user_id},
            )
            raise PremiumProviderError(
                "Patreon plan mapping is ambiguous across personal and guild premium families.",
                safe_message=PATREON_AMBIGUOUS_PLAN_MESSAGE,
                provider_code="ambiguous_plan_mapping",
            )
        seen_source_refs: set[str] = set()
        for plan_code in effective_plan_codes:
            source_ref = f"{identity.member_id or identity.provider_user_id}:{plan_code}"
            seen_source_refs.add(source_ref)
            await self.store.upsert_entitlement(
                {
                    "entitlement_id": f"patreon:{identity.member_id or identity.provider_user_id}:{plan_code}",
                    "provider": PROVIDER_PATREON,
                    "source_ref": source_ref,
                    "discord_user_id": discord_user_id,
                    "plan_code": plan_code,
                    "status": ENTITLEMENT_STATUS_ACTIVE,
                    "linked_provider_user_id": identity.provider_user_id,
                    "last_verified_at": _serialize_datetime(now),
                    "stale_after": _serialize_datetime(stale_after),
                    "grace_until": _serialize_datetime(grace_until),
                    "current_period_end": _serialize_datetime(current_period_end),
                    "metadata": {
                        "member_id": identity.member_id,
                        "tier_ids": list(identity.tier_ids),
                        "patron_status": identity.patron_status,
                    },
                }
            )
        for record in current:
            if record.get("source_ref") in seen_source_refs:
                continue
            if record.get("provider") != PROVIDER_PATREON:
                continue
            await self.store.upsert_entitlement(
                {
                    **record,
                    "status": ENTITLEMENT_STATUS_INACTIVE,
                    "last_verified_at": _serialize_datetime(now),
                    "stale_after": _serialize_datetime(now + timedelta(hours=PREMIUM_STALE_WARNING_HOURS)),
                    "grace_until": _serialize_datetime(now),
                    "current_period_end": _serialize_datetime(current_period_end),
                }
            )
        await self._reload_cache()
        await self._audit(
            action="patreon_sync",
            target_type="user",
            target_id=str(discord_user_id),
            actor_user_id=discord_user_id,
            detail={"plans": list(identity.plan_codes), "provider_user_id": identity.provider_user_id},
        )

    async def complete_link_callback(self, *, state_token: str, code: str | None, error: str | None = None) -> dict[str, str]:
        if not self.storage_ready:
            return {"title": "Premium unavailable", "message": self.storage_message()}
        if not self.patreon.configured():
            return {"title": "Premium unavailable", "message": self._patreon_configuration_message()}
        cleaned_state = str(state_token or "").strip()
        cleaned_code = str(code or "").strip() or None
        cleaned_error = str(error or "").strip() or None
        if not cleaned_state or len(cleaned_state) > 256:
            return {"title": "Link failed", "message": "Patreon did not return a valid link state. Start again from `/premium link` in Discord."}
        if cleaned_code is not None and len(cleaned_code) > 1024:
            return {"title": "Link failed", "message": "Patreon returned an invalid authorization code. Start again from `/premium link` in Discord."}
        if cleaned_error is not None and len(cleaned_error) > 256:
            cleaned_error = "authorization_failed"
        state = await self.store.consume_oauth_state(
            PROVIDER_PATREON,
            self._hash_oauth_state_token(cleaned_state),
            action="link",
            now=_utcnow(),
        )
        if state is None:
            return {"title": "Link expired", "message": "That Patreon link session is missing or already used. Run `/premium link` again from Discord."}
        if cleaned_error:
            await self._audit(action="patreon_link_denied", target_type="user", target_id=str(state["discord_user_id"]), actor_user_id=state["discord_user_id"], detail={"error": cleaned_error})
            return {"title": "Link canceled", "message": "Patreon did not authorize the link. Run `/premium link` again when you are ready."}
        if not cleaned_code:
            return {"title": "Link failed", "message": "Patreon did not return an authorization code."}
        try:
            token_payload = await self.patreon.exchange_code(code=cleaned_code)
            access_token = str(token_payload.get("access_token") or "").strip()
            refresh_token = str(token_payload.get("refresh_token") or "").strip() or None
            if not access_token:
                raise PremiumProviderError(
                    "Patreon token payload was missing an access token.",
                    safe_message="Patreon did not return a usable access token. Start again from `/premium link` in Discord.",
                )
            expires_in = int(token_payload.get("expires_in") or 0)
            token_expires_at = _utcnow() + timedelta(seconds=max(0, expires_in))
            token_scopes = getattr(self.patreon, "scopes_from_token_payload", lambda payload: ())(token_payload)
            identity = await self.patreon.fetch_identity(access_token=access_token)
            await self._sync_identity(
                discord_user_id=int(state["discord_user_id"]),
                identity=identity,
                access_token=access_token,
                refresh_token=refresh_token,
                token_expires_at=token_expires_at,
                scopes=token_scopes,
            )
        except Exception as exc:
            safe_message = self._safe_provider_message(exc, "Babblebox could not finish Patreon linking safely right now.")
            await self._audit(
                action="patreon_link_failed",
                target_type="user",
                target_id=str(state["discord_user_id"]),
                actor_user_id=state["discord_user_id"],
                detail={
                    "message": safe_message,
                    "provider_code": getattr(exc, "provider_code", None),
                    "status_code": getattr(exc, "status_code", None),
                },
            )
            title = "Link needs review" if getattr(exc, "provider_code", None) == "ambiguous_plan_mapping" else "Link failed"
            return {"title": title, "message": safe_message}
        await self._audit(action="patreon_link_success", target_type="user", target_id=str(state["discord_user_id"]), actor_user_id=state["discord_user_id"])
        return {"title": "Patreon linked", "message": "Babblebox linked your Patreon account and refreshed your premium entitlements."}

    async def refresh_user_link(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        if not self.patreon.configured():
            return False, self._patreon_configuration_message()
        link = self._links_by_user.get((PROVIDER_PATREON, user_id))
        if link is None:
            return False, "No Patreon account is linked to this Discord user."
        if str(link.get("link_status") or LINK_STATUS_ACTIVE) != LINK_STATUS_ACTIVE:
            return False, "Patreon needs to be linked again from `/premium link` before Babblebox can refresh this entitlement."
        try:
            access_token = self._decrypt_token(label="patreon-access", user_id=user_id, envelope=link.get("access_token_ciphertext"))
            refresh_token = self._decrypt_token(label="patreon-refresh", user_id=user_id, envelope=link.get("refresh_token_ciphertext"))
        except PremiumCryptoError as exc:
            return await self._handle_patreon_sync_failure(user_id=user_id, stage="decrypt", exc=exc)
        expires_at = _parse_datetime(link.get("token_expires_at"))
        token_scopes = tuple(link.get("scopes", ()))
        if expires_at is not None and expires_at <= _utcnow() and refresh_token:
            try:
                token_payload = await self.patreon.refresh_access_token(refresh_token=refresh_token)
            except Exception as exc:
                return await self._handle_patreon_sync_failure(user_id=user_id, stage="refresh", exc=exc)
            access_token = str(token_payload.get("access_token") or "").strip()
            refresh_token = str(token_payload.get("refresh_token") or "").strip() or refresh_token
            expires_in = int(token_payload.get("expires_in") or 0)
            expires_at = _utcnow() + timedelta(seconds=max(0, expires_in))
            token_scopes = getattr(self.patreon, "scopes_from_token_payload", lambda payload: ())(token_payload)
        if not access_token:
            return await self._revoke_patreon_access(
                user_id=user_id,
                reason="missing_access_token",
                safe_message="Babblebox could not refresh Patreon because the saved link token is missing. Re-link Patreon from `/premium link`.",
                action="patreon_link_broken",
                link_status=LINK_STATUS_BROKEN,
            )
        try:
            identity = await self.patreon.fetch_identity(access_token=access_token)
            expected_provider_user_id = str(link.get("provider_user_id") or "").strip()
            if expected_provider_user_id and identity.provider_user_id != expected_provider_user_id:
                return await self._handle_patreon_identity_mismatch(
                    user_id=user_id,
                    stage="identity",
                    expected_provider_user_id=expected_provider_user_id,
                    identity=identity,
                )
            await self._sync_identity(
                discord_user_id=user_id,
                identity=identity,
                access_token=access_token,
                refresh_token=refresh_token,
                token_expires_at=expires_at,
                scopes=token_scopes,
            )
        except Exception as exc:
            return await self._handle_patreon_sync_failure(user_id=user_id, stage="identity", exc=exc)
        await self._audit(action="patreon_refresh_success", target_type="user", target_id=str(user_id), actor_user_id=user_id)
        return True, "Patreon entitlements refreshed."

    async def unlink_user(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        link = self._links_by_user.get((PROVIDER_PATREON, user_id))
        if link is None:
            return False, "No Patreon account is linked."
        await self.store.delete_link(PROVIDER_PATREON, user_id)
        now = _utcnow()
        for record in await self.store.list_entitlements_for_user(user_id, provider=PROVIDER_PATREON):
            await self.store.upsert_entitlement(
                {
                    **record,
                    "status": ENTITLEMENT_STATUS_INACTIVE,
                    "last_verified_at": _serialize_datetime(now),
                    "stale_after": _serialize_datetime(now + timedelta(hours=PREMIUM_STALE_WARNING_HOURS)),
                    "grace_until": _serialize_datetime(now),
                    "current_period_end": _serialize_datetime(now),
                }
            )
        await self._reload_cache()
        await self._audit(action="patreon_unlink", target_type="user", target_id=str(user_id), actor_user_id=user_id)
        return True, "Patreon was unlinked and provider-backed entitlements were withdrawn."

    async def claim_guild(self, *, guild: Any, actor: Any) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        authorized, guild_id, user_id, error = self._authorize_guild_actor(guild=guild, actor=actor)
        if not authorized or guild_id is None or user_id is None:
            return False, error or "Babblebox could not verify this Guild Pro claim request."
        if self.is_support_guild(guild_id):
            return False, "The Babblebox support server already has permanent full-access premium and does not need a Guild Pro claim."
        current = self._claims_by_guild.get(guild_id)
        if current is not None and current.get("status") == CLAIM_STATUS_ACTIVE:
            if int(current.get("owner_user_id", 0)) == user_id:
                return False, "This server already uses one of your Guild Pro claims."
            return False, "This server already has an active Guild Pro claim."
        if self.is_system_owner(user_id):
            source = {
                "source_kind": SYSTEM_PREMIUM_CLAIM_KIND,
                "source_id": self._system_owner_claim_source_id(user_id=user_id, guild_id=guild_id),
                "entitlement_id": None,
            }
        else:
            sources = self._claimable_guild_sources(user_id)
            if not sources:
                return False, "No unclaimed Guild Pro entitlement is available for this user."
            source = self._preferred_claim_source(user_id) or sources[0]
        now = _utcnow()
        claimed = await self.store.claim_guild(
            {
                "claim_id": uuid.uuid4().hex,
                "guild_id": guild_id,
                "plan_code": PLAN_GUILD_PRO,
                "owner_user_id": user_id,
                "source_kind": source["source_kind"],
                "source_id": source["source_id"],
                "status": CLAIM_STATUS_ACTIVE,
                "claimed_at": _serialize_datetime(now),
                "updated_at": _serialize_datetime(now),
                "entitlement_id": source.get("entitlement_id"),
                "note": None,
            }
        )
        if claimed is None:
            await self._reload_cache()
            current = self._claims_by_guild.get(guild_id)
            if current is not None and current.get("status") == CLAIM_STATUS_ACTIVE:
                if int(current.get("owner_user_id", 0) or 0) == user_id:
                    return False, "This server already uses one of your Guild Pro claims."
                return False, "This server already has an active Guild Pro claim."
            if not self.is_system_owner(user_id) and not self._claimable_guild_sources(user_id):
                return False, "No unclaimed Guild Pro entitlement is available for this user."
            return False, "Babblebox could not claim Guild Pro for this server right now."
        await self._reload_cache()
        await self._audit(action="guild_claim", target_type="guild", target_id=str(guild_id), actor_user_id=user_id, detail={"source_id": source["source_id"], "source_kind": source["source_kind"]})
        return True, "Guild Pro is now assigned to this server."

    async def release_guild(self, *, guild: Any, actor: Any, force: bool = False) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        authorized, guild_id, user_id, error = self._authorize_guild_actor(guild=guild, actor=actor)
        if not authorized or guild_id is None or user_id is None:
            return False, error or "Babblebox could not verify this Guild Pro release request."
        current = self._claims_by_guild.get(guild_id)
        if self.is_support_guild(guild_id):
            if current is None or current.get("status") != CLAIM_STATUS_ACTIVE:
                return False, "The Babblebox support server keeps permanent full-access premium and does not use a Guild Pro claim."
            released = await self.store.release_guild_claim(
                guild_id,
                released_at=_utcnow(),
                note="Released stored claim from the permanent support-guild premium lane",
            )
            if released is None:
                return False, "Babblebox could not release the stored support-guild claim right now."
            await self._reload_cache()
            await self._audit(action="guild_release", target_type="guild", target_id=str(guild_id), actor_user_id=user_id)
            return True, "The stored Guild Pro claim was released. The Babblebox support server still keeps permanent full-access premium."
        if current is None or current.get("status") != CLAIM_STATUS_ACTIVE:
            return False, "This server does not have an active Guild Pro claim."
        if not force and int(current.get("owner_user_id", 0)) != user_id:
            return False, "Only the claim owner can release Guild Pro from this server."
        released = await self.store.release_guild_claim(guild_id, released_at=_utcnow(), note="Released from Discord")
        if released is None:
            return False, "Babblebox could not release Guild Pro right now."
        await self._reload_cache()
        await self._audit(action="guild_release", target_type="guild", target_id=str(guild_id), actor_user_id=user_id)
        return True, "Guild Pro was released from this server."

    async def create_manual_override(
        self,
        *,
        target_type: str,
        target_id: int,
        kind: str,
        plan_code: str | None,
        actor_user_id: int | None,
        reason: str | None,
    ) -> dict[str, Any]:
        self._require_storage_ready()
        cleaned_plan = self._validate_manual_override_request(target_type=target_type, kind=kind, plan_code=plan_code)
        now = _utcnow()
        record = {
            "override_id": uuid.uuid4().hex,
            "target_type": str(target_type).strip().lower(),
            "target_id": target_id,
            "kind": str(kind).strip().lower(),
            "plan_code": cleaned_plan,
            "active": True,
            "created_at": _serialize_datetime(now),
            "updated_at": _serialize_datetime(now),
            "actor_user_id": actor_user_id,
            "reason": reason,
            "metadata": {},
        }
        await self.store.upsert_manual_override(record)
        await self._reload_cache()
        await self._audit(action=f"manual_{record['kind']}", target_type=record["target_type"], target_id=str(target_id), actor_user_id=actor_user_id, detail={"plan_code": cleaned_plan, "override_id": record["override_id"]})
        return record

    async def deactivate_override(self, override_id: str, *, actor_user_id: int | None) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        overrides = await self.store.list_manual_overrides()
        target = next((item for item in overrides if item.get("override_id") == override_id), None)
        if target is None:
            return False, "That premium override was not found."
        if not target.get("active"):
            return False, "That premium override is already inactive."
        updated = {**target, "active": False, "updated_at": _serialize_datetime(_utcnow())}
        await self.store.upsert_manual_override(updated)
        await self._reload_cache()
        await self._audit(action="manual_override_deactivate", target_type=target["target_type"], target_id=str(target["target_id"]), actor_user_id=actor_user_id, detail={"override_id": override_id})
        return True, f"Premium override `{override_id}` is now inactive."

    async def clear_block_overrides(self, *, target_type: str, target_id: int, actor_user_id: int | None) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        active_blocks = [
            record
            for record in self._manual_overrides_for(target_type, target_id)
            if record.get("active") and record.get("kind") == MANUAL_KIND_BLOCK
        ]
        if not active_blocks:
            return False, "No active premium suspension was found."
        now = _utcnow()
        for record in active_blocks:
            await self.store.upsert_manual_override({**record, "active": False, "updated_at": _serialize_datetime(now)})
        await self._reload_cache()
        await self._audit(action="manual_block_clear", target_type=target_type, target_id=str(target_id), actor_user_id=actor_user_id)
        return True, "Premium suspension cleared."

    async def handle_patreon_webhook(self, *, body: bytes, event_type: str, signature: str) -> PatreonWebhookResult:
        if not self.storage_ready:
            return PatreonWebhookResult("unavailable", self.storage_message())
        if not self.patreon.configured():
            await self.record_webhook_monitor_event(status="unavailable", status_code=503)
            return PatreonWebhookResult("unavailable", self._patreon_configuration_message())
        if len(body) > PREMIUM_WEBHOOK_MAX_BYTES:
            await self.record_webhook_monitor_event(status="invalid", status_code=413)
            return PatreonWebhookResult("invalid", "Patreon webhook payload exceeded the safe size limit.")
        secret = os.getenv("PATREON_WEBHOOK_SECRET", "").strip()
        if not secret:
            await self.record_webhook_monitor_event(status="unavailable", status_code=503)
            return PatreonWebhookResult("unavailable", "Patreon webhook secret is not configured.")
        try:
            self.patreon.verify_webhook(body=body, signature=signature, secret=secret)
        except WebhookVerificationError:
            await self.record_webhook_monitor_event(status="invalid", status_code=400, invalid_signature=True)
            raise
        try:
            payload = json.loads(body.decode("utf-8") or "{}")
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            await self.record_webhook_monitor_event(status="invalid", status_code=400)
            raise PremiumProviderError(
                "Patreon webhook payload was not valid JSON.",
                safe_message="Patreon webhook payload was invalid.",
                status_code=400,
            ) from exc
        if not isinstance(payload, dict):
            await self.record_webhook_monitor_event(status="invalid", status_code=400)
            raise PremiumProviderError(
                "Patreon webhook payload was not an object.",
                safe_message="Patreon webhook payload was invalid.",
                status_code=400,
            )
        payload_hash = hashlib.sha256(self._canonicalize_webhook_payload(payload)).hexdigest()
        provider_user_id = self._extract_provider_user_id_from_webhook(payload)
        event_key = f"{PROVIDER_PATREON}:{event_type}:{payload_hash}"
        sanitized_payload = self._sanitize_webhook_payload(payload=payload, provider_user_id=provider_user_id, payload_hash=payload_hash)
        accepted = await self.store.record_webhook_event(
            {
                "event_key": event_key,
                "provider": PROVIDER_PATREON,
                "event_type": event_type,
                "payload_hash": payload_hash,
                "status": WEBHOOK_STATUS_PENDING,
                "received_at": _serialize_datetime(_utcnow()),
                "payload": sanitized_payload,
            }
        )
        if not accepted:
            await self.record_webhook_monitor_event(status="duplicate", status_code=200)
            return PatreonWebhookResult("duplicate", "Duplicate Patreon webhook ignored.")
        try:
            campaign_id = self._extract_webhook_campaign_id(payload)
            if campaign_id and campaign_id != self.patreon.campaign_id:
                issue = {
                    "event_key": event_key,
                    "event_type": event_type,
                    "reason": "campaign_mismatch",
                    "campaign_id": campaign_id,
                    "payload_hash": payload_hash,
                }
                await self._record_provider_issue(provider=PROVIDER_PATREON, issue_type="webhook_unresolved", detail=issue)
                await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_UNRESOLVED, error_text="Webhook campaign id did not match the configured Patreon campaign.")
                await self.record_webhook_monitor_event(status="unresolved", status_code=200)
                return PatreonWebhookResult("unresolved", "Patreon webhook stored for manual review.")
            if not provider_user_id:
                issue = {
                    "event_key": event_key,
                    "event_type": event_type,
                    "reason": "missing_provider_user_id",
                    "payload_hash": payload_hash,
                }
                await self._record_provider_issue(provider=PROVIDER_PATREON, issue_type="webhook_unresolved", detail=issue)
                await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_UNRESOLVED, error_text="Missing provider user id in webhook payload.")
                await self.record_webhook_monitor_event(status="unresolved", status_code=200)
                return PatreonWebhookResult("unresolved", "Patreon webhook stored for manual review.")
            link = self._links_by_provider_user.get((PROVIDER_PATREON, provider_user_id))
            if link is None:
                issue = {
                    "event_key": event_key,
                    "event_type": event_type,
                    "reason": "linked_user_missing",
                    "provider_user_id": provider_user_id,
                    "payload_hash": payload_hash,
                }
                await self._record_provider_issue(provider=PROVIDER_PATREON, issue_type="webhook_unresolved", detail=issue)
                await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_UNRESOLVED, error_text="Webhook did not match a linked Discord user.")
                await self.record_webhook_monitor_event(status="unresolved", status_code=200)
                return PatreonWebhookResult("unresolved", "Patreon webhook stored for manual review.")
            ok, message = await self.refresh_user_link(int(link["discord_user_id"]))
            if not ok:
                if message == PATREON_AMBIGUOUS_PLAN_MESSAGE:
                    await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_UNRESOLVED, error_text=message)
                    await self._audit(
                        action="patreon_webhook_unresolved",
                        target_type=SCOPE_USER,
                        target_id=str(link["discord_user_id"]),
                        detail={"event_type": event_type, "provider_user_id": provider_user_id, "reason": "ambiguous_plan_mapping"},
                    )
                    await self.record_webhook_monitor_event(status="unresolved", status_code=200)
                    return PatreonWebhookResult("unresolved", "Patreon webhook stored for manual review.")
                refreshed_link = self.get_link(int(link["discord_user_id"]))
                refreshed_status = str((refreshed_link or {}).get("link_status") or "").strip().lower()
                if refreshed_status in {LINK_STATUS_REVOKED, LINK_STATUS_BROKEN}:
                    await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_PROCESSED)
                    await self._audit(
                        action="patreon_webhook_processed",
                        target_type=SCOPE_USER,
                        target_id=str(link["discord_user_id"]),
                        detail={
                            "event_type": event_type,
                            "provider_user_id": provider_user_id,
                            "degraded_safely": True,
                            "link_status": refreshed_status,
                        },
                    )
                    await self.record_webhook_monitor_event(status="processed", status_code=200)
                    return PatreonWebhookResult("processed", "Patreon webhook processed.")
                raise PremiumProviderError(
                    message,
                    safe_message="Babblebox could not process the Patreon webhook safely.",
                    status_code=500,
                )
            await self.store.finish_webhook_event(event_key, status=WEBHOOK_STATUS_PROCESSED)
            await self._audit(
                action="patreon_webhook_processed",
                target_type=SCOPE_USER,
                target_id=str(link["discord_user_id"]),
                detail={"event_type": event_type, "provider_user_id": provider_user_id},
            )
            await self.record_webhook_monitor_event(status="processed", status_code=200)
            return PatreonWebhookResult("processed", "Patreon webhook processed.")
        except Exception as exc:
            await self.store.finish_webhook_event(
                event_key,
                status=WEBHOOK_STATUS_FAILED,
                error_text=self._event_error_text(exc, "Patreon webhook processing failed."),
            )
            status_code = int(getattr(exc, "status_code", 0) or 500)
            await self.record_webhook_monitor_event(status="invalid" if status_code < 500 else "error", status_code=status_code)
            raise

    def _extract_provider_user_id_from_webhook(self, payload: dict[str, Any]) -> str | None:
        included = list(payload.get("included") or [])
        for item in included:
            if item.get("type") == "user":
                value = str(item.get("id") or "").strip()
                if value:
                    return value
        data = payload.get("data") or {}
        relationships = data.get("relationships") or {}
        for key in ("user", "patron"):
            value = ((relationships.get(key) or {}).get("data") or {}).get("id")
            if value:
                return str(value).strip()
        return None

    def _links_due_for_repair(self) -> list[int]:
        now = _utcnow()
        refresh_before = now + timedelta(seconds=PREMIUM_REPAIR_REFRESH_WINDOW_SECONDS)
        due_user_ids: list[int] = []
        for (provider, user_id), link in self._links_by_user.items():
            if provider != PROVIDER_PATREON:
                continue
            if str(link.get("link_status") or LINK_STATUS_ACTIVE) != LINK_STATUS_ACTIVE:
                continue
            entitlements = self._entitlements_by_user.get(user_id, ())
            if not entitlements:
                due_user_ids.append(user_id)
                continue
            token_expires_at = _parse_datetime(link.get("token_expires_at"))
            if token_expires_at is None or token_expires_at <= refresh_before:
                due_user_ids.append(user_id)
                continue
            if any((_parse_datetime(record.get("stale_after")) or now) <= refresh_before for record in entitlements if record.get("provider") == PROVIDER_PATREON):
                due_user_ids.append(user_id)
        return due_user_ids

    async def _repair_loop(self):
        first_pass = True
        while True:
            if first_pass:
                first_pass = False
            else:
                await asyncio.sleep(PREMIUM_REPAIR_INTERVAL_SECONDS)
            if not self.storage_ready or not self.patreon.configured():
                continue
            for user_id in self._links_due_for_repair():
                ok, message = await self.refresh_user_link(user_id)
                if ok:
                    continue
                await self._audit(
                    action="patreon_repair_deferred",
                    target_type=SCOPE_USER,
                    target_id=str(user_id),
                    detail={"message": message},
                )
