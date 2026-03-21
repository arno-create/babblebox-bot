from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit


DEFAULT_DATABASE_URL_ENV_ORDER = ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")
DEFAULT_BACKEND = "postgres"
DEFAULT_VERSION = 1


class ShieldStorageUnavailable(RuntimeError):
    pass


def default_guild_shield_config(guild_id: int | None = None) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "module_enabled": False,
        "log_channel_id": None,
        "alert_role_id": None,
        "scan_mode": "all",
        "included_channel_ids": [],
        "excluded_channel_ids": [],
        "included_user_ids": [],
        "excluded_user_ids": [],
        "included_role_ids": [],
        "excluded_role_ids": [],
        "trusted_role_ids": [],
        "allow_domains": [],
        "allow_invite_codes": [],
        "allow_phrases": [],
        "privacy_enabled": False,
        "privacy_action": "log",
        "privacy_sensitivity": "normal",
        "promo_enabled": False,
        "promo_action": "log",
        "promo_sensitivity": "normal",
        "scam_enabled": False,
        "scam_action": "log",
        "scam_sensitivity": "normal",
        "escalation_threshold": 3,
        "escalation_window_minutes": 15,
        "timeout_minutes": 10,
        "custom_patterns": [],
    }


def default_shield_state() -> dict[str, Any]:
    return {"version": DEFAULT_VERSION, "guilds": {}}


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


class _BaseShieldStore:
    backend_name = "unknown"

    def __init__(self):
        self.state: dict[str, Any] = default_shield_state()

    async def load(self) -> dict[str, Any]:
        raise NotImplementedError

    async def flush(self) -> bool:
        raise NotImplementedError

    async def close(self):
        return None

    def normalize_state(self, payload: Any) -> dict[str, Any]:
        normalized = default_shield_state()
        if not isinstance(payload, dict):
            return normalized
        version = payload.get("version")
        normalized["version"] = version if isinstance(version, int) and version > 0 else DEFAULT_VERSION
        guilds = payload.get("guilds")
        if not isinstance(guilds, dict):
            return normalized
        cleaned_guilds: dict[str, Any] = {}
        for guild_id_text, config in guilds.items():
            try:
                guild_id = int(guild_id_text)
            except (TypeError, ValueError):
                continue
            cleaned = self.normalize_config(guild_id, config)
            if cleaned is not None:
                cleaned_guilds[str(guild_id)] = cleaned
        normalized["guilds"] = cleaned_guilds
        return normalized

    def normalize_config(self, guild_id: int, config: Any) -> dict[str, Any] | None:
        if not isinstance(config, dict):
            return None
        cleaned = default_guild_shield_config(guild_id)
        cleaned["module_enabled"] = bool(config.get("module_enabled"))
        cleaned["log_channel_id"] = config.get("log_channel_id") if isinstance(config.get("log_channel_id"), int) else None
        cleaned["alert_role_id"] = config.get("alert_role_id") if isinstance(config.get("alert_role_id"), int) else None
        scan_mode = config.get("scan_mode", "all")
        cleaned["scan_mode"] = scan_mode if scan_mode in {"all", "only_included"} else "all"
        for field in (
            "included_channel_ids",
            "excluded_channel_ids",
            "included_user_ids",
            "excluded_user_ids",
            "included_role_ids",
            "excluded_role_ids",
            "trusted_role_ids",
        ):
            cleaned[field] = sorted({value for value in config.get(field, []) if isinstance(value, int) and value > 0})
        for field in ("allow_domains", "allow_invite_codes", "allow_phrases"):
            cleaned[field] = sorted({str(value).strip().casefold() for value in config.get(field, []) if isinstance(value, str) and str(value).strip()})
        for pack in ("privacy", "promo", "scam"):
            enabled_field = f"{pack}_enabled"
            action_field = f"{pack}_action"
            sensitivity_field = f"{pack}_sensitivity"
            cleaned[enabled_field] = bool(config.get(enabled_field))
            action = str(config.get(action_field, "log")).strip().lower()
            cleaned[action_field] = action if action in {"disabled", "detect", "log", "delete_log", "delete_escalate", "timeout_log"} else "log"
            sensitivity = str(config.get(sensitivity_field, "normal")).strip().lower()
            cleaned[sensitivity_field] = sensitivity if sensitivity in {"low", "normal", "high"} else "normal"
        for field, minimum, maximum, default in (
            ("escalation_threshold", 2, 6, 3),
            ("escalation_window_minutes", 5, 120, 15),
            ("timeout_minutes", 1, 60, 10),
        ):
            value = config.get(field)
            cleaned[field] = value if isinstance(value, int) and minimum <= value <= maximum else default
        patterns = []
        for item in config.get("custom_patterns", []):
            if not isinstance(item, dict):
                continue
            pattern_id = item.get("pattern_id")
            label = item.get("label")
            pattern = item.get("pattern")
            mode = item.get("mode", "contains")
            action = item.get("action", "log")
            if not all(isinstance(value, str) and value.strip() for value in (pattern_id, label, pattern)):
                continue
            if mode not in {"contains", "word", "wildcard"}:
                continue
            if action not in {"detect", "log", "delete_log", "delete_escalate", "timeout_log"}:
                continue
            patterns.append(
                {
                    "pattern_id": pattern_id.strip(),
                    "label": label.strip(),
                    "pattern": pattern.strip(),
                    "mode": mode,
                    "action": action,
                    "enabled": bool(item.get("enabled", True)),
                }
            )
        cleaned["custom_patterns"] = patterns[:10]
        return cleaned


class _MemoryShieldStore(_BaseShieldStore):
    backend_name = "memory"

    async def load(self) -> dict[str, Any]:
        self.state = default_shield_state()
        return self.state

    async def flush(self) -> bool:
        self.state = deepcopy(self.state)
        return True


class _PostgresShieldStore(_BaseShieldStore):
    backend_name = "postgres"

    def __init__(self, dsn: str):
        super().__init__()
        self.dsn = dsn
        self._asyncpg = None
        self._pool = None
        self._io_lock = asyncio.Lock()

    async def load(self) -> dict[str, Any]:
        await self._connect()
        await self._ensure_schema()
        await self._reload_from_db()
        return self.state

    async def flush(self) -> bool:
        snapshot = self.normalize_state(deepcopy(self.state))
        async with self._io_lock:
            try:
                await self._flush_snapshot(snapshot)
            except Exception as exc:
                print(f"Shield Postgres store flush failed: {exc}")
                return False
        self.state = snapshot
        return True

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _connect(self):
        if self._pool is not None:
            return
        try:
            self._asyncpg = importlib.import_module("asyncpg")
        except ModuleNotFoundError as exc:
            raise ShieldStorageUnavailable("asyncpg is not installed, so Postgres Shield storage is unavailable.") from exc
        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=3,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-shield-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise ShieldStorageUnavailable(f"Could not connect to Postgres Shield storage: {last_error}") from last_error

    async def _ensure_schema(self):
        statements = [
            (
                "CREATE TABLE IF NOT EXISTS shield_guild_configs ("
                "guild_id BIGINT PRIMARY KEY, "
                "module_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "log_channel_id BIGINT NULL, "
                "alert_role_id BIGINT NULL, "
                "scan_mode TEXT NOT NULL DEFAULT 'all', "
                "included_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "included_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "included_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "trusted_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_invite_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_phrases JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "privacy_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "privacy_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "promo_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "promo_action TEXT NOT NULL DEFAULT 'log', "
                "promo_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "scam_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "scam_action TEXT NOT NULL DEFAULT 'log', "
                "scam_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "escalation_threshold SMALLINT NOT NULL DEFAULT 3, "
                "escalation_window_minutes SMALLINT NOT NULL DEFAULT 15, "
                "timeout_minutes SMALLINT NOT NULL DEFAULT 10, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            (
                "CREATE TABLE IF NOT EXISTS shield_custom_patterns ("
                "pattern_id TEXT PRIMARY KEY, "
                "guild_id BIGINT NOT NULL REFERENCES shield_guild_configs(guild_id) ON DELETE CASCADE, "
                "label TEXT NOT NULL, "
                "pattern TEXT NOT NULL, "
                "mode TEXT NOT NULL, "
                "action TEXT NOT NULL, "
                "enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_shield_custom_patterns_guild ON shield_custom_patterns (guild_id)",
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def _reload_from_db(self):
        loaded = default_shield_state()
        async with self._pool.acquire() as conn:
            config_rows = await conn.fetch("SELECT * FROM shield_guild_configs")
            pattern_rows = await conn.fetch("SELECT pattern_id, guild_id, label, pattern, mode, action, enabled FROM shield_custom_patterns ORDER BY created_at ASC")
        for row in config_rows:
            guild_id = int(row["guild_id"])
            loaded["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "module_enabled": bool(row["module_enabled"]),
                "log_channel_id": row["log_channel_id"],
                "alert_role_id": row["alert_role_id"],
                "scan_mode": row["scan_mode"],
                "included_channel_ids": list(row["included_channel_ids"] or []),
                "excluded_channel_ids": list(row["excluded_channel_ids"] or []),
                "included_user_ids": list(row["included_user_ids"] or []),
                "excluded_user_ids": list(row["excluded_user_ids"] or []),
                "included_role_ids": list(row["included_role_ids"] or []),
                "excluded_role_ids": list(row["excluded_role_ids"] or []),
                "trusted_role_ids": list(row["trusted_role_ids"] or []),
                "allow_domains": list(row["allow_domains"] or []),
                "allow_invite_codes": list(row["allow_invite_codes"] or []),
                "allow_phrases": list(row["allow_phrases"] or []),
                "privacy_enabled": bool(row["privacy_enabled"]),
                "privacy_action": row["privacy_action"],
                "privacy_sensitivity": row["privacy_sensitivity"],
                "promo_enabled": bool(row["promo_enabled"]),
                "promo_action": row["promo_action"],
                "promo_sensitivity": row["promo_sensitivity"],
                "scam_enabled": bool(row["scam_enabled"]),
                "scam_action": row["scam_action"],
                "scam_sensitivity": row["scam_sensitivity"],
                "escalation_threshold": int(row["escalation_threshold"]),
                "escalation_window_minutes": int(row["escalation_window_minutes"]),
                "timeout_minutes": int(row["timeout_minutes"]),
                "custom_patterns": [],
            }
        for row in pattern_rows:
            guild = loaded["guilds"].setdefault(str(int(row["guild_id"])), default_guild_shield_config(int(row["guild_id"])))
            guild["custom_patterns"].append(
                {
                    "pattern_id": row["pattern_id"],
                    "label": row["label"],
                    "pattern": row["pattern"],
                    "mode": row["mode"],
                    "action": row["action"],
                    "enabled": bool(row["enabled"]),
                }
            )
        self.state = self.normalize_state(loaded)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("DELETE FROM shield_custom_patterns")
                await conn.execute("DELETE FROM shield_guild_configs")
                for config in snapshot.get("guilds", {}).values():
                    await conn.execute(
                        (
                            "INSERT INTO shield_guild_configs ("
                            "guild_id, module_enabled, log_channel_id, alert_role_id, scan_mode, "
                            "included_channel_ids, excluded_channel_ids, included_user_ids, excluded_user_ids, "
                            "included_role_ids, excluded_role_ids, trusted_role_ids, allow_domains, allow_invite_codes, allow_phrases, "
                            "privacy_enabled, privacy_action, privacy_sensitivity, "
                            "promo_enabled, promo_action, promo_sensitivity, "
                            "scam_enabled, scam_action, scam_sensitivity, "
                            "escalation_threshold, escalation_window_minutes, timeout_minutes, updated_at"
                            ") VALUES ("
                            "$1, $2, $3, $4, $5, "
                            "$6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, "
                            "$10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, "
                            "$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, timezone('utc', now())"
                            ")"
                        ),
                        config["guild_id"],
                        config["module_enabled"],
                        config["log_channel_id"],
                        config["alert_role_id"],
                        config["scan_mode"],
                        json.dumps(config["included_channel_ids"]),
                        json.dumps(config["excluded_channel_ids"]),
                        json.dumps(config["included_user_ids"]),
                        json.dumps(config["excluded_user_ids"]),
                        json.dumps(config["included_role_ids"]),
                        json.dumps(config["excluded_role_ids"]),
                        json.dumps(config["trusted_role_ids"]),
                        json.dumps(config["allow_domains"]),
                        json.dumps(config["allow_invite_codes"]),
                        json.dumps(config["allow_phrases"]),
                        config["privacy_enabled"],
                        config["privacy_action"],
                        config["privacy_sensitivity"],
                        config["promo_enabled"],
                        config["promo_action"],
                        config["promo_sensitivity"],
                        config["scam_enabled"],
                        config["scam_action"],
                        config["scam_sensitivity"],
                        config["escalation_threshold"],
                        config["escalation_window_minutes"],
                        config["timeout_minutes"],
                    )
                    for item in config.get("custom_patterns", []):
                        await conn.execute(
                            (
                                "INSERT INTO shield_custom_patterns (pattern_id, guild_id, label, pattern, mode, action, enabled) "
                                "VALUES ($1, $2, $3, $4, $5, $6, $7)"
                            ),
                            item["pattern_id"],
                            config["guild_id"],
                            item["label"],
                            item["pattern"],
                            item["mode"],
                            item["action"],
                            item["enabled"],
                        )


class ShieldStateStore:
    def __init__(
        self,
        *,
        backend: str | None = None,
        database_url: str | None = None,
    ):
        requested_backend = (backend or os.getenv("SHIELD_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.database_url_source = self.database_url_source or "none"
        self.backend_name = requested_backend
        self._store: _BaseShieldStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        print(
            "Shield storage init: "
            f"backend_preference={requested_backend}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source}, "
            f"database_target={_redact_database_url(self.database_url)}"
        )
        if requested_backend == "memory":
            self._store = _MemoryShieldStore()
        elif requested_backend == "postgres":
            if not self.database_url:
                raise ShieldStorageUnavailable("No Postgres Shield database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL.")
            self._store = _PostgresShieldStore(self.database_url)
        else:
            raise ShieldStorageUnavailable(f"Unsupported Shield storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        self.state = self._store.state
        print(f"Shield storage init succeeded: backend={self.backend_name}")

    async def load(self) -> dict[str, Any]:
        if self._store is None:
            raise ShieldStorageUnavailable("Shield storage was not initialized.")
        state = await self._store.load()
        self.state = state
        return state

    async def flush(self) -> bool:
        if self._store is None:
            raise ShieldStorageUnavailable("Shield storage was not initialized.")
        self._store.state = self.state
        flushed = await self._store.flush()
        self.state = self._store.state
        return flushed

    async def close(self):
        if self._store is not None:
            await self._store.close()

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
