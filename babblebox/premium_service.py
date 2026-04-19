from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

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
    MANUAL_KIND_BLOCK,
    MANUAL_KIND_GRANT,
    PLAN_FREE,
    PLAN_GUILD_PRO,
    PLAN_PLUS,
    PLAN_SUPPORTER,
    PROVIDER_PATREON,
    SCOPE_GUILD,
    SCOPE_USER,
)
from babblebox.premium_provider import PremiumProviderError
from babblebox.premium_provider_patreon import PatreonPremiumProvider
from babblebox.premium_store import PremiumStorageUnavailable, PremiumStore


PREMIUM_STALE_WARNING_HOURS = 24
PREMIUM_GRACE_DAYS = 7
PREMIUM_REPAIR_INTERVAL_SECONDS = 6 * 3600

USER_LIMIT_KEYS = {
    LIMIT_WATCH_KEYWORDS,
    LIMIT_WATCH_FILTERS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_AFK_SCHEDULES,
}
GUILD_LIMIT_KEYS = set(GUILD_LIMITS[PLAN_FREE])


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


class PremiumService:
    def __init__(self, bot: commands.Bot, store: PremiumStore | None = None, provider: PatreonPremiumProvider | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = PremiumStore()
            except PremiumStorageUnavailable as exc:
                print(f"Premium storage constructor failed: {exc}")
                self.store = PremiumStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self.patreon = provider or PatreonPremiumProvider()
        self._lock = asyncio.Lock()
        self._repair_task: asyncio.Task | None = None

        self._links_by_user: dict[tuple[str, int], dict[str, Any]] = {}
        self._links_by_provider_user: dict[tuple[str, str], dict[str, Any]] = {}
        self._entitlements_by_user: dict[int, list[dict[str, Any]]] = {}
        self._entitlements_by_id: dict[str, dict[str, Any]] = {}
        self._manual_overrides_by_target: dict[tuple[str, int], list[dict[str, Any]]] = {}
        self._claims_by_guild: dict[int, dict[str, Any]] = {}
        self._claims_by_owner: dict[int, list[dict[str, Any]]] = {}
        self._provider_state: dict[str, dict[str, Any]] = {}

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Premium storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except PremiumStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Premium storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
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

    async def _reload_cache(self):
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
        for record in overrides:
            key = (str(record["target_type"]), int(record["target_id"]))
            self._manual_overrides_by_target.setdefault(key, []).append(record)

        self._claims_by_guild = {int(record["guild_id"]): record for record in claims}
        self._claims_by_owner = {}
        for record in claims:
            self._claims_by_owner.setdefault(int(record["owner_user_id"]), []).append(record)

        self._provider_state = {}
        for row in provider_state_rows:
            if isinstance(row, dict):
                self._provider_state[str(row["provider"])] = row

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

    def _claimable_guild_sources(self, user_id: int) -> list[dict[str, Any]]:
        claimed_source_ids = {(record.get("source_kind"), record.get("source_id")) for record in self._claims_by_owner.get(user_id, [])}
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
        }

    def get_guild_snapshot(self, guild_id: int) -> dict[str, Any]:
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
            }
        plans: set[str] = set()
        stale = False
        in_grace = False
        for override in self._active_grants_for(SCOPE_GUILD, guild_id):
            plan_code = str(override.get("plan_code") or "")
            if plan_code:
                plans.add(plan_code)
        claim = self._claims_by_guild.get(guild_id)
        if claim is not None and claim.get("status") == CLAIM_STATUS_ACTIVE:
            plans.add(str(claim.get("plan_code") or PLAN_FREE))
            entitlement_id = claim.get("entitlement_id")
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
            "claim": claim,
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
        crypto_status = getattr(self.store, "crypto", None)
        crypto_meta = getattr(crypto_status, "status", None)
        return {
            "storage_ready": self.storage_ready,
            "storage_error": self.storage_error,
            "storage_backend": getattr(self.store, "backend_name", "unknown"),
            "database_url": self.store.redacted_database_url() if hasattr(self.store, "redacted_database_url") else "unknown",
            "crypto_source": getattr(crypto_meta, "source", "unknown"),
            "crypto_ephemeral": bool(getattr(crypto_meta, "ephemeral", False)),
            "patreon_configured": self.patreon.configured(),
            "patreon_automation_ready": self.patreon.automation_ready(),
            "link_count": len(self._links_by_user),
            "entitlement_count": len(self._entitlements_by_id),
            "active_claim_count": len(self._claims_by_guild),
            "provider_state": dict(self._provider_state.get(PROVIDER_PATREON, {})),
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
        return f"You reached the current limit of {limit_value}. {self.get_plan_upgrade_label_for_limit(limit_key)} unlocks more."

    def over_limit_label(self, *, current_count: int, limit_value: int) -> str | None:
        if current_count <= limit_value:
            return None
        return f"Over current plan limit: {current_count} saved while this plan allows {limit_value}."

    def plan_catalog(self) -> tuple[dict[str, Any], ...]:
        return (
            {
                "plan_code": PLAN_SUPPORTER,
                "title": "Supporter",
                "summary": "Status-only support tier for launch. It does not unlock product power.",
            },
            {
                "plan_code": PLAN_PLUS,
                "title": "Plus",
                "summary": "Higher personal utility limits for Watch, reminders, and AFK scheduling.",
            },
            {
                "plan_code": PLAN_GUILD_PRO,
                "title": "Guild Pro",
                "summary": "Server-level premium for larger caps, Shield AI review, and advanced admin power.",
            },
        )

    def plan_title(self, plan_code: str) -> str:
        return {
            PLAN_FREE: "Free",
            PLAN_SUPPORTER: "Supporter",
            PLAN_PLUS: "Plus",
            PLAN_GUILD_PRO: "Guild Pro",
        }.get(str(plan_code or PLAN_FREE), "Free")

    async def create_link_url(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        if not self.patreon.configured():
            return False, "Patreon linking is not configured on this Babblebox deployment."
        state_token = uuid.uuid4().hex
        now = _utcnow()
        await self.store.create_oauth_state(
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
        await self._audit(action="oauth_state_create", target_type="user", target_id=str(user_id), actor_user_id=user_id, detail={"provider": PROVIDER_PATREON})
        return True, self.patreon.build_authorize_url(state_token=state_token)

    def _encrypt_token(self, *, label: str, user_id: int, secret: str | None) -> str | None:
        if not secret:
            return None
        return self.store.crypto.encrypt_secret(label=label, aad_fields={"user_id": user_id}, secret=secret)

    def _decrypt_token(self, *, label: str, user_id: int, envelope: str | None) -> str | None:
        return self.store.crypto.decrypt_secret(label=label, aad_fields={"user_id": user_id}, envelope=envelope)

    async def _sync_identity(self, *, discord_user_id: int, identity, access_token: str, refresh_token: str | None, token_expires_at: datetime | None):
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
            "scopes": tuple(sorted(os.getenv("PATREON_SCOPES_OVERRIDE", "").split())) if os.getenv("PATREON_SCOPES_OVERRIDE", "").strip() else (),
            "email": identity.email,
            "display_name": identity.display_name,
            "metadata": {
                "member_id": identity.member_id,
                "patron_status": identity.patron_status,
                "tier_ids": list(identity.tier_ids),
            },
        }
        await self.store.upsert_link(link_record)

        stale_after, grace_until, current_period_end = self.patreon.entitlement_timestamps(identity=identity)
        current = await self.store.list_entitlements_for_user(discord_user_id, provider=PROVIDER_PATREON)
        seen_source_refs: set[str] = set()
        for plan_code in identity.plan_codes:
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
        state = await self.store.consume_oauth_state(PROVIDER_PATREON, state_token)
        if state is None:
            return {"title": "Link expired", "message": "That Patreon link session is missing or already used. Run `/premium link` again from Discord."}
        expires_at = _parse_datetime(state.get("expires_at"))
        if expires_at is not None and expires_at < _utcnow():
            return {"title": "Link expired", "message": "That Patreon link session expired. Run `/premium link` again from Discord."}
        if error:
            await self._audit(action="patreon_link_denied", target_type="user", target_id=str(state["discord_user_id"]), actor_user_id=state["discord_user_id"], detail={"error": error})
            return {"title": "Link canceled", "message": "Patreon did not authorize the link. Run `/premium link` again when you are ready."}
        if not code:
            return {"title": "Link failed", "message": "Patreon did not return an authorization code."}
        token_payload = await self.patreon.exchange_code(code=code)
        access_token = str(token_payload.get("access_token") or "").strip()
        refresh_token = str(token_payload.get("refresh_token") or "").strip() or None
        expires_in = int(token_payload.get("expires_in") or 0)
        token_expires_at = _utcnow() + timedelta(seconds=max(0, expires_in))
        identity = await self.patreon.fetch_identity(access_token=access_token)
        await self._sync_identity(
            discord_user_id=int(state["discord_user_id"]),
            identity=identity,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at,
        )
        return {"title": "Patreon linked", "message": "Babblebox linked your Patreon account and refreshed your premium entitlements."}

    async def refresh_user_link(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        link = self._links_by_user.get((PROVIDER_PATREON, user_id))
        if link is None:
            return False, "No Patreon account is linked to this Discord user."
        access_token = self._decrypt_token(label="patreon-access", user_id=user_id, envelope=link.get("access_token_ciphertext"))
        refresh_token = self._decrypt_token(label="patreon-refresh", user_id=user_id, envelope=link.get("refresh_token_ciphertext"))
        expires_at = _parse_datetime(link.get("token_expires_at"))
        if expires_at is not None and expires_at <= _utcnow() and refresh_token:
            token_payload = await self.patreon.refresh_access_token(refresh_token=refresh_token)
            access_token = str(token_payload.get("access_token") or "").strip()
            refresh_token = str(token_payload.get("refresh_token") or "").strip() or refresh_token
            expires_in = int(token_payload.get("expires_in") or 0)
            expires_at = _utcnow() + timedelta(seconds=max(0, expires_in))
        if not access_token:
            return False, "Babblebox could not refresh Patreon because the link token is missing."
        identity = await self.patreon.fetch_identity(access_token=access_token)
        await self._sync_identity(
            discord_user_id=user_id,
            identity=identity,
            access_token=access_token,
            refresh_token=refresh_token,
            token_expires_at=expires_at,
        )
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
            if record.get("plan_code") == PLAN_GUILD_PRO:
                for claim in list(self._claims_by_owner.get(user_id, ())):
                    if claim.get("entitlement_id") == record.get("entitlement_id"):
                        await self.store.release_guild_claim(int(claim["guild_id"]), released_at=now, note="Patreon link removed")
        await self._reload_cache()
        await self._audit(action="patreon_unlink", target_type="user", target_id=str(user_id), actor_user_id=user_id)
        return True, "Patreon was unlinked and provider-backed entitlements were released."

    async def claim_guild(self, *, guild_id: int, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        current = self._claims_by_guild.get(guild_id)
        if current is not None and current.get("status") == CLAIM_STATUS_ACTIVE:
            if int(current.get("owner_user_id", 0)) == user_id:
                return False, "This server already uses one of your Guild Pro claims."
            return False, "This server already has an active Guild Pro claim."
        sources = self._claimable_guild_sources(user_id)
        if not sources:
            return False, "No unclaimed Guild Pro entitlement is available for this user."
        source = sorted(sources, key=lambda item: (str(item["source_kind"]), str(item["source_id"])))[0]
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
            return False, "Babblebox could not claim Guild Pro for this server right now."
        await self._reload_cache()
        await self._audit(action="guild_claim", target_type="guild", target_id=str(guild_id), actor_user_id=user_id, detail={"source_id": source["source_id"], "source_kind": source["source_kind"]})
        return True, "Guild Pro is now assigned to this server."

    async def release_guild(self, *, guild_id: int, user_id: int, force: bool = False) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        current = self._claims_by_guild.get(guild_id)
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
        now = _utcnow()
        record = {
            "override_id": uuid.uuid4().hex,
            "target_type": target_type,
            "target_id": target_id,
            "kind": kind,
            "plan_code": plan_code,
            "active": True,
            "created_at": _serialize_datetime(now),
            "updated_at": _serialize_datetime(now),
            "actor_user_id": actor_user_id,
            "reason": reason,
            "metadata": {},
        }
        await self.store.upsert_manual_override(record)
        await self._reload_cache()
        await self._audit(action=f"manual_{kind}", target_type=target_type, target_id=str(target_id), actor_user_id=actor_user_id, detail={"plan_code": plan_code, "override_id": record["override_id"]})
        return record

    async def deactivate_override(self, override_id: str, *, actor_user_id: int | None) -> tuple[bool, str]:
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

    async def handle_patreon_webhook(self, *, body: bytes, event_type: str, signature: str) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        secret = os.getenv("PATREON_WEBHOOK_SECRET", "").strip()
        if not secret:
            return False, "Patreon webhook secret is not configured."
        self.patreon.verify_webhook(body=body, signature=signature, secret=secret)
        payload = json.loads(body.decode("utf-8") or "{}")
        payload_hash = hashlib.sha256(body).hexdigest()
        provider_user_id = self._extract_provider_user_id_from_webhook(payload)
        event_key = f"{event_type}:{provider_user_id or 'unknown'}:{payload_hash[:24]}"
        accepted = await self.store.record_webhook_event(
            {
                "event_key": event_key,
                "provider": PROVIDER_PATREON,
                "event_type": event_type,
                "payload_hash": payload_hash,
                "status": "pending",
                "received_at": _serialize_datetime(_utcnow()),
                "payload": payload,
            }
        )
        if not accepted:
            return True, "Duplicate Patreon webhook ignored."
        try:
            if provider_user_id:
                link = self._links_by_provider_user.get((PROVIDER_PATREON, provider_user_id))
                if link is not None:
                    ok, message = await self.refresh_user_link(int(link["discord_user_id"]))
                    if not ok:
                        raise PremiumProviderError(message)
            await self.store.finish_webhook_event(event_key, status="processed")
            return True, "Patreon webhook processed."
        except Exception as exc:
            await self.store.finish_webhook_event(event_key, status="failed", error_text=str(exc))
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

    async def _repair_loop(self):
        while True:
            await asyncio.sleep(PREMIUM_REPAIR_INTERVAL_SECONDS)
            if not self.storage_ready or not self.patreon.configured():
                continue
            for link in list(self._links_by_user.values()):
                if link.get("provider") != PROVIDER_PATREON:
                    continue
                with contextlib.suppress(Exception):
                    await self.refresh_user_link(int(link["discord_user_id"]))
