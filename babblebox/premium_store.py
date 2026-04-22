from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array, decode_postgres_json_object
from babblebox.premium_crypto import PremiumCrypto, PremiumKeyConfigError


DEFAULT_BACKEND = "postgres"
DEFAULT_DATABASE_URL_ENV_ORDER = ("PREMIUM_DATABASE_URL", "UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")


class PremiumStorageUnavailable(RuntimeError):
    pass


class PremiumStoreConflict(RuntimeError):
    pass


def _resolve_database_url(configured: str | None = None) -> tuple[str, str | None]:
    if configured is not None and configured.strip():
        return configured.strip(), "argument"
    for env_name in DEFAULT_DATABASE_URL_ENV_ORDER:
        value = os.getenv(env_name, "").strip()
        if value:
            return value, env_name
    return "", None


def _redact_database_url(dsn: str | None) -> str:
    if not dsn:
        return "not-configured"
    try:
        parsed = urlsplit(dsn)
    except ValueError:
        return "[configured]"
    if not parsed.scheme or not parsed.netloc:
        return "[configured]"
    safe_netloc = parsed.netloc
    if "@" in safe_netloc:
        userinfo, hostinfo = safe_netloc.rsplit("@", 1)
        if ":" in userinfo:
            username, _ = userinfo.split(":", 1)
            userinfo = f"{username}:***"
        safe_netloc = f"{userinfo}@{hostinfo}"
    return urlunsplit((parsed.scheme, safe_netloc, parsed.path, "", ""))


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


def _serialize_datetime(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _decode_json_list(value: Any, *, label: str) -> tuple[str, ...]:
    return tuple(str(item) for item in decode_postgres_json_array(value, label=label))


class _BasePremiumStore:
    backend_name = "unknown"

    async def load(self):
        return None

    async def close(self):
        return None

    async def create_oauth_state(self, record: dict[str, Any]):
        raise NotImplementedError

    async def invalidate_oauth_states(self, provider: str, discord_user_id: int, *, action: str | None = None):
        raise NotImplementedError

    async def consume_oauth_state(
        self,
        provider: str,
        state_token: str,
        *,
        action: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_link(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_link(self, provider: str, discord_user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_link_by_provider_user(self, provider: str, provider_user_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_links(self, *, provider: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def delete_link(self, provider: str, discord_user_id: int):
        raise NotImplementedError

    async def upsert_entitlement(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_entitlement(self, entitlement_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_entitlements_for_user(self, discord_user_id: int, *, provider: str | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_entitlements(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def upsert_manual_override(self, record: dict[str, Any]):
        raise NotImplementedError

    async def list_manual_overrides(self, *, target_type: str | None = None, target_id: int | None = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def claim_guild(self, record: dict[str, Any]) -> dict[str, Any] | None:
        raise NotImplementedError

    async def release_guild_claim(self, guild_id: int, *, released_at: datetime, note: str | None = None) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_guild_claim(self, guild_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_active_claims_for_user(self, owner_user_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_active_claims(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def upsert_provider_state(self, provider: str, payload: dict[str, Any]):
        raise NotImplementedError

    async def fetch_provider_state(self, provider: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        raise NotImplementedError

    async def finish_webhook_event(self, event_key: str, *, status: str, error_text: str | None = None):
        raise NotImplementedError

    async def append_audit(self, record: dict[str, Any]):
        raise NotImplementedError


class _MemoryPremiumStore(_BasePremiumStore):
    backend_name = "memory"

    def __init__(self):
        self.oauth_states: dict[tuple[str, str], dict[str, Any]] = {}
        self.links: dict[tuple[str, int], dict[str, Any]] = {}
        self.entitlements: dict[str, dict[str, Any]] = {}
        self.manual_overrides: dict[str, dict[str, Any]] = {}
        self.claims_by_guild: dict[int, dict[str, Any]] = {}
        self.provider_state: dict[str, dict[str, Any]] = {}
        self.webhook_events: dict[str, dict[str, Any]] = {}
        self.audit_log: list[dict[str, Any]] = []

    async def create_oauth_state(self, record: dict[str, Any]):
        self.oauth_states[(str(record["provider"]), str(record["state_token"]))] = deepcopy(record)

    async def invalidate_oauth_states(self, provider: str, discord_user_id: int, *, action: str | None = None):
        consumed_at = _serialize_datetime(datetime.now(timezone.utc))
        for key, record in list(self.oauth_states.items()):
            if key[0] != provider:
                continue
            if int(record.get("discord_user_id", 0) or 0) != discord_user_id:
                continue
            if action is not None and str(record.get("action") or "") != action:
                continue
            if record.get("consumed_at") is not None:
                continue
            updated = deepcopy(record)
            updated["consumed_at"] = consumed_at
            self.oauth_states[key] = updated

    async def consume_oauth_state(
        self,
        provider: str,
        state_token: str,
        *,
        action: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        key = (provider, state_token)
        record = deepcopy(self.oauth_states.get(key))
        if record is None or record.get("consumed_at") is not None:
            return None
        if action is not None and str(record.get("action") or "") != action:
            return None
        expires_at = _parse_datetime(record.get("expires_at"))
        current_time = now or datetime.now(timezone.utc)
        if expires_at is not None and expires_at <= current_time:
            return None
        record["consumed_at"] = _serialize_datetime(datetime.now(timezone.utc))
        self.oauth_states[key] = deepcopy(record)
        return record

    async def upsert_link(self, record: dict[str, Any]):
        provider = str(record["provider"])
        discord_user_id = int(record["discord_user_id"])
        provider_user_id = str(record.get("provider_user_id") or "").strip()
        for existing in self.links.values():
            if existing.get("provider") != provider:
                continue
            if str(existing.get("provider_user_id") or "") != provider_user_id:
                continue
            if int(existing.get("discord_user_id", 0) or 0) != discord_user_id:
                raise PremiumStoreConflict("That provider identity is already linked to another Discord user.")
        self.links[(str(record["provider"]), int(record["discord_user_id"]))] = deepcopy(record)

    async def fetch_link(self, provider: str, discord_user_id: int) -> dict[str, Any] | None:
        record = self.links.get((provider, discord_user_id))
        return deepcopy(record) if record is not None else None

    async def fetch_link_by_provider_user(self, provider: str, provider_user_id: str) -> dict[str, Any] | None:
        for record in self.links.values():
            if record.get("provider") == provider and record.get("provider_user_id") == provider_user_id:
                return deepcopy(record)
        return None

    async def list_links(self, *, provider: str | None = None) -> list[dict[str, Any]]:
        return [
            deepcopy(record)
            for record in self.links.values()
            if provider is None or record.get("provider") == provider
        ]

    async def delete_link(self, provider: str, discord_user_id: int):
        self.links.pop((provider, discord_user_id), None)

    async def upsert_entitlement(self, record: dict[str, Any]):
        self.entitlements[str(record["entitlement_id"])] = deepcopy(record)

    async def fetch_entitlement(self, entitlement_id: str) -> dict[str, Any] | None:
        record = self.entitlements.get(entitlement_id)
        return deepcopy(record) if record is not None else None

    async def list_entitlements_for_user(self, discord_user_id: int, *, provider: str | None = None) -> list[dict[str, Any]]:
        return [
            deepcopy(record)
            for record in self.entitlements.values()
            if int(record.get("discord_user_id", 0)) == discord_user_id and (provider is None or record.get("provider") == provider)
        ]

    async def list_entitlements(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.entitlements.values()]

    async def upsert_manual_override(self, record: dict[str, Any]):
        self.manual_overrides[str(record["override_id"])] = deepcopy(record)

    async def list_manual_overrides(self, *, target_type: str | None = None, target_id: int | None = None) -> list[dict[str, Any]]:
        rows = []
        for record in self.manual_overrides.values():
            if target_type is not None and record.get("target_type") != target_type:
                continue
            if target_id is not None and int(record.get("target_id", 0)) != target_id:
                continue
            rows.append(deepcopy(record))
        return rows

    async def claim_guild(self, record: dict[str, Any]) -> dict[str, Any] | None:
        guild_id = int(record["guild_id"])
        entitlement_id = record.get("entitlement_id")
        current = self.claims_by_guild.get(guild_id)
        if current is not None and current.get("status") == "active":
            return None
        if entitlement_id:
            for existing in self.claims_by_guild.values():
                if existing.get("status") == "active" and existing.get("entitlement_id") == entitlement_id:
                    return None
        self.claims_by_guild[guild_id] = deepcopy(record)
        return deepcopy(record)

    async def release_guild_claim(self, guild_id: int, *, released_at: datetime, note: str | None = None) -> dict[str, Any] | None:
        record = self.claims_by_guild.get(guild_id)
        if record is None or record.get("status") != "active":
            return None
        record = deepcopy(record)
        record["status"] = "released"
        record["released_at"] = _serialize_datetime(released_at)
        record["updated_at"] = _serialize_datetime(released_at)
        record["note"] = note
        self.claims_by_guild[guild_id] = deepcopy(record)
        return record

    async def fetch_guild_claim(self, guild_id: int) -> dict[str, Any] | None:
        record = self.claims_by_guild.get(guild_id)
        return deepcopy(record) if record is not None else None

    async def list_active_claims_for_user(self, owner_user_id: int) -> list[dict[str, Any]]:
        return [
            deepcopy(record)
            for record in self.claims_by_guild.values()
            if record.get("status") == "active" and int(record.get("owner_user_id", 0)) == owner_user_id
        ]

    async def list_active_claims(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.claims_by_guild.values() if record.get("status") == "active"]

    async def upsert_provider_state(self, provider: str, payload: dict[str, Any]):
        self.provider_state[provider] = deepcopy(payload)

    async def fetch_provider_state(self, provider: str) -> dict[str, Any] | None:
        record = self.provider_state.get(provider)
        if record is None:
            return None
        return {
            "provider": provider,
            "payload": deepcopy(record),
            "updated_at": _serialize_datetime(datetime.now(timezone.utc)),
        }

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        event_key = str(record["event_key"])
        if event_key in self.webhook_events:
            return False
        self.webhook_events[event_key] = deepcopy(record)
        return True

    async def finish_webhook_event(self, event_key: str, *, status: str, error_text: str | None = None):
        record = self.webhook_events.get(event_key)
        if record is None:
            return
        updated = deepcopy(record)
        updated["status"] = status
        updated["error_text"] = error_text
        updated["processed_at"] = _serialize_datetime(datetime.now(timezone.utc))
        self.webhook_events[event_key] = updated

    async def append_audit(self, record: dict[str, Any]):
        self.audit_log.append(deepcopy(record))


class _PostgresPremiumStore(_BasePremiumStore):
    backend_name = "postgres"

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._pool = None
        self._asyncpg = None
        self._io_lock = asyncio.Lock()

    async def load(self):
        if self._pool is None:
            try:
                self._asyncpg = importlib.import_module("asyncpg")
            except ImportError as exc:
                raise PremiumStorageUnavailable("Premium Postgres storage requires asyncpg.") from exc
            try:
                self._pool = await self._asyncpg.create_pool(self.database_url, min_size=1, max_size=4)
            except Exception as exc:
                raise PremiumStorageUnavailable(f"Premium storage could not connect to Postgres: {exc}") from exc
        await self._ensure_schema()

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _ensure_schema(self):
        statements = (
            "CREATE TABLE IF NOT EXISTS premium_oauth_states (provider TEXT NOT NULL, state_token TEXT NOT NULL, discord_user_id BIGINT NOT NULL, action TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, consumed_at TIMESTAMPTZ NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (provider, state_token))",
            "CREATE TABLE IF NOT EXISTS premium_links (provider TEXT NOT NULL, discord_user_id BIGINT NOT NULL, provider_user_id TEXT NOT NULL, link_status TEXT NOT NULL, linked_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, access_token_ciphertext TEXT NULL, refresh_token_ciphertext TEXT NULL, token_expires_at TIMESTAMPTZ NULL, scopes JSONB NOT NULL DEFAULT '[]'::jsonb, email TEXT NULL, display_name TEXT NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (provider, discord_user_id))",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_premium_links_provider_user ON premium_links (provider, provider_user_id)",
            "CREATE TABLE IF NOT EXISTS premium_entitlements (entitlement_id TEXT PRIMARY KEY, provider TEXT NOT NULL, source_ref TEXT NOT NULL, discord_user_id BIGINT NOT NULL, plan_code TEXT NOT NULL, status TEXT NOT NULL, linked_provider_user_id TEXT NOT NULL, last_verified_at TIMESTAMPTZ NOT NULL, stale_after TIMESTAMPTZ NOT NULL, grace_until TIMESTAMPTZ NOT NULL, current_period_end TIMESTAMPTZ NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_premium_entitlements_provider_source ON premium_entitlements (provider, source_ref)",
            "CREATE INDEX IF NOT EXISTS ix_premium_entitlements_user ON premium_entitlements (discord_user_id)",
            "CREATE TABLE IF NOT EXISTS premium_manual_overrides (override_id TEXT PRIMARY KEY, target_type TEXT NOT NULL, target_id BIGINT NOT NULL, kind TEXT NOT NULL, plan_code TEXT NULL, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, actor_user_id BIGINT NULL, reason TEXT NULL, metadata JSONB NOT NULL DEFAULT '{}'::jsonb)",
            "CREATE INDEX IF NOT EXISTS ix_premium_manual_targets ON premium_manual_overrides (target_type, target_id)",
            "CREATE TABLE IF NOT EXISTS premium_guild_claims (claim_id TEXT PRIMARY KEY, guild_id BIGINT NOT NULL UNIQUE, plan_code TEXT NOT NULL, owner_user_id BIGINT NOT NULL, source_kind TEXT NOT NULL, source_id TEXT NOT NULL, status TEXT NOT NULL, claimed_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL, entitlement_id TEXT NULL, released_at TIMESTAMPTZ NULL, note TEXT NULL)",
            "CREATE INDEX IF NOT EXISTS ix_premium_guild_claims_entitlement_active ON premium_guild_claims (entitlement_id) WHERE entitlement_id IS NOT NULL AND status = 'active'",
            "CREATE TABLE IF NOT EXISTS premium_provider_state (provider TEXT PRIMARY KEY, payload JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ NOT NULL)",
            "CREATE TABLE IF NOT EXISTS premium_webhook_events (event_key TEXT PRIMARY KEY, provider TEXT NOT NULL, event_type TEXT NOT NULL, payload_hash TEXT NOT NULL, status TEXT NOT NULL, error_text TEXT NULL, received_at TIMESTAMPTZ NOT NULL, processed_at TIMESTAMPTZ NULL, payload JSONB NOT NULL DEFAULT '{}'::jsonb)",
            "CREATE TABLE IF NOT EXISTS premium_audit_log (audit_id TEXT PRIMARY KEY, actor_user_id BIGINT NULL, action TEXT NOT NULL, target_type TEXT NOT NULL, target_id TEXT NOT NULL, detail JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMPTZ NOT NULL)",
        )
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    def _oauth_state_from_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "provider": row["provider"],
            "state_token": row["state_token"],
            "discord_user_id": int(row["discord_user_id"]),
            "action": row["action"],
            "created_at": _serialize_datetime(row["created_at"]),
            "expires_at": _serialize_datetime(row["expires_at"]),
            "consumed_at": _serialize_datetime(row["consumed_at"]),
            "metadata": decode_postgres_json_object(row["metadata"], label="premium_oauth_states.metadata"),
        }

    def _link_from_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "provider": row["provider"],
            "discord_user_id": int(row["discord_user_id"]),
            "provider_user_id": row["provider_user_id"],
            "link_status": row["link_status"],
            "linked_at": _serialize_datetime(row["linked_at"]),
            "updated_at": _serialize_datetime(row["updated_at"]),
            "access_token_ciphertext": row["access_token_ciphertext"],
            "refresh_token_ciphertext": row["refresh_token_ciphertext"],
            "token_expires_at": _serialize_datetime(row["token_expires_at"]),
            "scopes": _decode_json_list(row["scopes"], label="premium_links.scopes"),
            "email": row["email"],
            "display_name": row["display_name"],
            "metadata": decode_postgres_json_object(row["metadata"], label="premium_links.metadata"),
        }

    def _entitlement_from_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "entitlement_id": row["entitlement_id"],
            "provider": row["provider"],
            "source_ref": row["source_ref"],
            "discord_user_id": int(row["discord_user_id"]),
            "plan_code": row["plan_code"],
            "status": row["status"],
            "linked_provider_user_id": row["linked_provider_user_id"],
            "last_verified_at": _serialize_datetime(row["last_verified_at"]),
            "stale_after": _serialize_datetime(row["stale_after"]),
            "grace_until": _serialize_datetime(row["grace_until"]),
            "current_period_end": _serialize_datetime(row["current_period_end"]),
            "metadata": decode_postgres_json_object(row["metadata"], label="premium_entitlements.metadata"),
        }

    def _manual_override_from_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "override_id": row["override_id"],
            "target_type": row["target_type"],
            "target_id": int(row["target_id"]),
            "kind": row["kind"],
            "plan_code": row["plan_code"],
            "active": bool(row["active"]),
            "created_at": _serialize_datetime(row["created_at"]),
            "updated_at": _serialize_datetime(row["updated_at"]),
            "actor_user_id": int(row["actor_user_id"]) if row["actor_user_id"] is not None else None,
            "reason": row["reason"],
            "metadata": decode_postgres_json_object(row["metadata"], label="premium_manual_overrides.metadata"),
        }

    def _claim_from_row(self, row: Any) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "claim_id": row["claim_id"],
            "guild_id": int(row["guild_id"]),
            "plan_code": row["plan_code"],
            "owner_user_id": int(row["owner_user_id"]),
            "source_kind": row["source_kind"],
            "source_id": row["source_id"],
            "status": row["status"],
            "claimed_at": _serialize_datetime(row["claimed_at"]),
            "updated_at": _serialize_datetime(row["updated_at"]),
            "entitlement_id": row["entitlement_id"],
            "released_at": _serialize_datetime(row["released_at"]),
            "note": row["note"],
        }

    async def create_oauth_state(self, record: dict[str, Any]):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO premium_oauth_states (provider, state_token, discord_user_id, action, created_at, expires_at, consumed_at, metadata) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb) ON CONFLICT (provider, state_token) DO UPDATE SET discord_user_id = EXCLUDED.discord_user_id, action = EXCLUDED.action, created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at, consumed_at = EXCLUDED.consumed_at, metadata = EXCLUDED.metadata",
                record["provider"],
                record["state_token"],
                record["discord_user_id"],
                record["action"],
                _parse_datetime(record["created_at"]),
                _parse_datetime(record["expires_at"]),
                _parse_datetime(record.get("consumed_at")),
                json.dumps(record.get("metadata", {})),
            )

    async def invalidate_oauth_states(self, provider: str, discord_user_id: int, *, action: str | None = None):
        async with self._io_lock, self._pool.acquire() as conn:
            if action is None:
                await conn.execute(
                    "UPDATE premium_oauth_states SET consumed_at = timezone('utc', now()) WHERE provider = $1 AND discord_user_id = $2 AND consumed_at IS NULL",
                    provider,
                    discord_user_id,
                )
            else:
                await conn.execute(
                    "UPDATE premium_oauth_states SET consumed_at = timezone('utc', now()) WHERE provider = $1 AND discord_user_id = $2 AND action = $3 AND consumed_at IS NULL",
                    provider,
                    discord_user_id,
                    action,
                )

    async def consume_oauth_state(
        self,
        provider: str,
        state_token: str,
        *,
        action: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        current_time = now or datetime.now(timezone.utc)
        async with self._io_lock, self._pool.acquire() as conn:
            if action is None:
                row = await conn.fetchrow(
                    "UPDATE premium_oauth_states SET consumed_at = timezone('utc', now()) WHERE provider = $1 AND state_token = $2 AND consumed_at IS NULL AND expires_at > $3 RETURNING provider, state_token, discord_user_id, action, created_at, expires_at, consumed_at, metadata",
                    provider,
                    state_token,
                    current_time,
                )
            else:
                row = await conn.fetchrow(
                    "UPDATE premium_oauth_states SET consumed_at = timezone('utc', now()) WHERE provider = $1 AND state_token = $2 AND action = $3 AND consumed_at IS NULL AND expires_at > $4 RETURNING provider, state_token, discord_user_id, action, created_at, expires_at, consumed_at, metadata",
                    provider,
                    state_token,
                    action,
                    current_time,
                )
        return self._oauth_state_from_row(row)

    async def upsert_link(self, record: dict[str, Any]):
        try:
            async with self._io_lock, self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO premium_links (provider, discord_user_id, provider_user_id, link_status, linked_at, updated_at, access_token_ciphertext, refresh_token_ciphertext, token_expires_at, scopes, email, display_name, metadata) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13::jsonb) "
                        "ON CONFLICT (provider, discord_user_id) DO UPDATE SET provider_user_id = EXCLUDED.provider_user_id, link_status = EXCLUDED.link_status, linked_at = EXCLUDED.linked_at, updated_at = EXCLUDED.updated_at, access_token_ciphertext = EXCLUDED.access_token_ciphertext, refresh_token_ciphertext = EXCLUDED.refresh_token_ciphertext, token_expires_at = EXCLUDED.token_expires_at, scopes = EXCLUDED.scopes, email = EXCLUDED.email, display_name = EXCLUDED.display_name, metadata = EXCLUDED.metadata"
                    ),
                    record["provider"],
                    record["discord_user_id"],
                    record["provider_user_id"],
                    record["link_status"],
                    _parse_datetime(record["linked_at"]),
                    _parse_datetime(record["updated_at"]),
                    record.get("access_token_ciphertext"),
                    record.get("refresh_token_ciphertext"),
                    _parse_datetime(record.get("token_expires_at")),
                    json.dumps(list(record.get("scopes", ()))),
                    record.get("email"),
                    record.get("display_name"),
                    json.dumps(record.get("metadata", {})),
                )
        except Exception as exc:
            unique_violation = getattr(getattr(self._asyncpg, "exceptions", None), "UniqueViolationError", None)
            if unique_violation is not None and isinstance(exc, unique_violation):
                raise PremiumStoreConflict("That provider identity is already linked to another Discord user.") from exc
            raise

    async def fetch_link(self, provider: str, discord_user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM premium_links WHERE provider = $1 AND discord_user_id = $2", provider, discord_user_id)
        return self._link_from_row(row)

    async def fetch_link_by_provider_user(self, provider: str, provider_user_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM premium_links WHERE provider = $1 AND provider_user_id = $2", provider, provider_user_id)
        return self._link_from_row(row)

    async def list_links(self, *, provider: str | None = None) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            if provider is None:
                rows = await conn.fetch("SELECT * FROM premium_links ORDER BY provider, discord_user_id")
            else:
                rows = await conn.fetch("SELECT * FROM premium_links WHERE provider = $1 ORDER BY discord_user_id", provider)
        return [record for row in rows if (record := self._link_from_row(row)) is not None]

    async def delete_link(self, provider: str, discord_user_id: int):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute("DELETE FROM premium_links WHERE provider = $1 AND discord_user_id = $2", provider, discord_user_id)

    async def upsert_entitlement(self, record: dict[str, Any]):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                (
                    "INSERT INTO premium_entitlements (entitlement_id, provider, source_ref, discord_user_id, plan_code, status, linked_provider_user_id, last_verified_at, stale_after, grace_until, current_period_end, metadata) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb) "
                    "ON CONFLICT (entitlement_id) DO UPDATE SET provider = EXCLUDED.provider, source_ref = EXCLUDED.source_ref, discord_user_id = EXCLUDED.discord_user_id, plan_code = EXCLUDED.plan_code, status = EXCLUDED.status, linked_provider_user_id = EXCLUDED.linked_provider_user_id, last_verified_at = EXCLUDED.last_verified_at, stale_after = EXCLUDED.stale_after, grace_until = EXCLUDED.grace_until, current_period_end = EXCLUDED.current_period_end, metadata = EXCLUDED.metadata"
                ),
                record["entitlement_id"],
                record["provider"],
                record["source_ref"],
                record["discord_user_id"],
                record["plan_code"],
                record["status"],
                record["linked_provider_user_id"],
                _parse_datetime(record["last_verified_at"]),
                _parse_datetime(record["stale_after"]),
                _parse_datetime(record["grace_until"]),
                _parse_datetime(record.get("current_period_end")),
                json.dumps(record.get("metadata", {})),
            )

    async def fetch_entitlement(self, entitlement_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM premium_entitlements WHERE entitlement_id = $1", entitlement_id)
        return self._entitlement_from_row(row)

    async def list_entitlements_for_user(self, discord_user_id: int, *, provider: str | None = None) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            if provider is None:
                rows = await conn.fetch("SELECT * FROM premium_entitlements WHERE discord_user_id = $1 ORDER BY entitlement_id", discord_user_id)
            else:
                rows = await conn.fetch(
                    "SELECT * FROM premium_entitlements WHERE discord_user_id = $1 AND provider = $2 ORDER BY entitlement_id",
                    discord_user_id,
                    provider,
                )
        return [record for row in rows if (record := self._entitlement_from_row(row)) is not None]

    async def list_entitlements(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM premium_entitlements ORDER BY provider, discord_user_id, entitlement_id")
        return [record for row in rows if (record := self._entitlement_from_row(row)) is not None]

    async def upsert_manual_override(self, record: dict[str, Any]):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                (
                    "INSERT INTO premium_manual_overrides (override_id, target_type, target_id, kind, plan_code, active, created_at, updated_at, actor_user_id, reason, metadata) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb) "
                    "ON CONFLICT (override_id) DO UPDATE SET target_type = EXCLUDED.target_type, target_id = EXCLUDED.target_id, kind = EXCLUDED.kind, plan_code = EXCLUDED.plan_code, active = EXCLUDED.active, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at, actor_user_id = EXCLUDED.actor_user_id, reason = EXCLUDED.reason, metadata = EXCLUDED.metadata"
                ),
                record["override_id"],
                record["target_type"],
                record["target_id"],
                record["kind"],
                record.get("plan_code"),
                bool(record.get("active", True)),
                _parse_datetime(record["created_at"]),
                _parse_datetime(record["updated_at"]),
                record.get("actor_user_id"),
                record.get("reason"),
                json.dumps(record.get("metadata", {})),
            )

    async def list_manual_overrides(self, *, target_type: str | None = None, target_id: int | None = None) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if target_type is not None:
            clauses.append(f"target_type = ${len(params) + 1}")
            params.append(target_type)
        if target_id is not None:
            clauses.append(f"target_id = ${len(params) + 1}")
            params.append(target_id)
        where_sql = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM premium_manual_overrides{where_sql} ORDER BY created_at ASC", *params)
        return [record for row in rows if (record := self._manual_override_from_row(row)) is not None]

    async def claim_guild(self, record: dict[str, Any]) -> dict[str, Any] | None:
        async with self._io_lock, self._pool.acquire() as conn:
            async with conn.transaction():
                entitlement_id = record.get("entitlement_id")
                if entitlement_id:
                    in_use = await conn.fetchrow(
                        "SELECT claim_id FROM premium_guild_claims WHERE entitlement_id = $1 AND status = 'active'",
                        entitlement_id,
                    )
                    if in_use is not None:
                        return None
                row = await conn.fetchrow(
                    (
                        "INSERT INTO premium_guild_claims (claim_id, guild_id, plan_code, owner_user_id, source_kind, source_id, status, claimed_at, updated_at, entitlement_id, released_at, note) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11) "
                        "ON CONFLICT (guild_id) DO UPDATE SET claim_id = EXCLUDED.claim_id, plan_code = EXCLUDED.plan_code, owner_user_id = EXCLUDED.owner_user_id, source_kind = EXCLUDED.source_kind, source_id = EXCLUDED.source_id, status = EXCLUDED.status, claimed_at = EXCLUDED.claimed_at, updated_at = EXCLUDED.updated_at, entitlement_id = EXCLUDED.entitlement_id, released_at = EXCLUDED.released_at, note = EXCLUDED.note "
                        "WHERE premium_guild_claims.status <> 'active' "
                        "RETURNING *"
                    ),
                    record["claim_id"],
                    record["guild_id"],
                    record["plan_code"],
                    record["owner_user_id"],
                    record["source_kind"],
                    record["source_id"],
                    record["status"],
                    _parse_datetime(record["claimed_at"]),
                    _parse_datetime(record["updated_at"]),
                    record.get("entitlement_id"),
                    record.get("note"),
                )
        return self._claim_from_row(row)

    async def release_guild_claim(self, guild_id: int, *, released_at: datetime, note: str | None = None) -> dict[str, Any] | None:
        async with self._io_lock, self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "UPDATE premium_guild_claims SET status = 'released', released_at = $2, updated_at = $2, note = $3 WHERE guild_id = $1 AND status = 'active' RETURNING *",
                guild_id,
                released_at,
                note,
            )
        return self._claim_from_row(row)

    async def fetch_guild_claim(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM premium_guild_claims WHERE guild_id = $1", guild_id)
        return self._claim_from_row(row)

    async def list_active_claims_for_user(self, owner_user_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM premium_guild_claims WHERE owner_user_id = $1 AND status = 'active' ORDER BY guild_id", owner_user_id)
        return [record for row in rows if (record := self._claim_from_row(row)) is not None]

    async def list_active_claims(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM premium_guild_claims WHERE status = 'active' ORDER BY guild_id")
        return [record for row in rows if (record := self._claim_from_row(row)) is not None]

    async def upsert_provider_state(self, provider: str, payload: dict[str, Any]):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                (
                    "INSERT INTO premium_provider_state (provider, payload, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) "
                    "ON CONFLICT (provider) DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at"
                ),
                provider,
                json.dumps(payload),
            )

    async def fetch_provider_state(self, provider: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT provider, payload, updated_at FROM premium_provider_state WHERE provider = $1", provider)
        if row is None:
            return None
        return {
            "provider": row["provider"],
            "payload": decode_postgres_json_object(row["payload"], label="premium_provider_state.payload"),
            "updated_at": _serialize_datetime(row["updated_at"]),
        }

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        async with self._io_lock, self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "INSERT INTO premium_webhook_events (event_key, provider, event_type, payload_hash, status, error_text, received_at, processed_at, payload) "
                    "VALUES ($1, $2, $3, $4, $5, NULL, $6, NULL, $7::jsonb) "
                    "ON CONFLICT (event_key) DO NOTHING RETURNING event_key"
                ),
                record["event_key"],
                record["provider"],
                record["event_type"],
                record["payload_hash"],
                record["status"],
                _parse_datetime(record["received_at"]),
                json.dumps(record.get("payload", {})),
            )
        return row is not None

    async def finish_webhook_event(self, event_key: str, *, status: str, error_text: str | None = None):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE premium_webhook_events SET status = $2, error_text = $3, processed_at = timezone('utc', now()) WHERE event_key = $1",
                event_key,
                status,
                error_text,
            )

    async def append_audit(self, record: dict[str, Any]):
        async with self._io_lock, self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO premium_audit_log (audit_id, actor_user_id, action, target_type, target_id, detail, created_at) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)",
                record["audit_id"],
                record.get("actor_user_id"),
                record["action"],
                record["target_type"],
                record["target_id"],
                json.dumps(record.get("detail", {})),
                _parse_datetime(record["created_at"]),
            )


class PremiumStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        requested_backend = (backend or os.getenv("PREMIUM_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        try:
            self.crypto = PremiumCrypto.from_environment(backend_name=requested_backend)
        except PremiumKeyConfigError as exc:
            raise PremiumStorageUnavailable(str(exc)) from exc
        if requested_backend in {"memory", "test", "dev"}:
            self._store: _BasePremiumStore = _MemoryPremiumStore()
        elif requested_backend in {"postgres", "postgresql", "supabase", "auto"}:
            if not self.database_url:
                raise PremiumStorageUnavailable(
                    "No Postgres premium database URL is configured. Set PREMIUM_DATABASE_URL, UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL."
                )
            self._store = _PostgresPremiumStore(self.database_url)
        else:
            raise PremiumStorageUnavailable(f"Unsupported premium storage backend '{requested_backend}'.")

    @property
    def backend_name(self) -> str:
        return self._store.backend_name

    async def load(self):
        await self._store.load()

    async def close(self):
        await self._store.close()

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)

    async def create_oauth_state(self, record: dict[str, Any]):
        await self._store.create_oauth_state(record)

    async def invalidate_oauth_states(self, provider: str, discord_user_id: int, *, action: str | None = None):
        await self._store.invalidate_oauth_states(provider, discord_user_id, action=action)

    async def consume_oauth_state(
        self,
        provider: str,
        state_token: str,
        *,
        action: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, Any] | None:
        return await self._store.consume_oauth_state(provider, state_token, action=action, now=now)

    async def upsert_link(self, record: dict[str, Any]):
        await self._store.upsert_link(record)

    async def fetch_link(self, provider: str, discord_user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_link(provider, discord_user_id)

    async def fetch_link_by_provider_user(self, provider: str, provider_user_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_link_by_provider_user(provider, provider_user_id)

    async def list_links(self, *, provider: str | None = None) -> list[dict[str, Any]]:
        return await self._store.list_links(provider=provider)

    async def delete_link(self, provider: str, discord_user_id: int):
        await self._store.delete_link(provider, discord_user_id)

    async def upsert_entitlement(self, record: dict[str, Any]):
        await self._store.upsert_entitlement(record)

    async def fetch_entitlement(self, entitlement_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_entitlement(entitlement_id)

    async def list_entitlements_for_user(self, discord_user_id: int, *, provider: str | None = None) -> list[dict[str, Any]]:
        return await self._store.list_entitlements_for_user(discord_user_id, provider=provider)

    async def list_entitlements(self) -> list[dict[str, Any]]:
        return await self._store.list_entitlements()

    async def upsert_manual_override(self, record: dict[str, Any]):
        await self._store.upsert_manual_override(record)

    async def list_manual_overrides(self, *, target_type: str | None = None, target_id: int | None = None) -> list[dict[str, Any]]:
        return await self._store.list_manual_overrides(target_type=target_type, target_id=target_id)

    async def claim_guild(self, record: dict[str, Any]) -> dict[str, Any] | None:
        return await self._store.claim_guild(record)

    async def release_guild_claim(self, guild_id: int, *, released_at: datetime, note: str | None = None) -> dict[str, Any] | None:
        return await self._store.release_guild_claim(guild_id, released_at=released_at, note=note)

    async def fetch_guild_claim(self, guild_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_guild_claim(guild_id)

    async def list_active_claims_for_user(self, owner_user_id: int) -> list[dict[str, Any]]:
        return await self._store.list_active_claims_for_user(owner_user_id)

    async def list_active_claims(self) -> list[dict[str, Any]]:
        return await self._store.list_active_claims()

    async def upsert_provider_state(self, provider: str, payload: dict[str, Any]):
        await self._store.upsert_provider_state(provider, payload)

    async def fetch_provider_state(self, provider: str) -> dict[str, Any] | None:
        return await self._store.fetch_provider_state(provider)

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        return await self._store.record_webhook_event(record)

    async def finish_webhook_event(self, event_key: str, *, status: str, error_text: str | None = None):
        await self._store.finish_webhook_event(event_key, status=status, error_text=error_text)

    async def append_audit(self, record: dict[str, Any]):
        await self._store.append_audit(record)
