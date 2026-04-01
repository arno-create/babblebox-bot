from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array, decode_postgres_json_object
from babblebox.shield_ai import SHIELD_AI_MIN_CONFIDENCE_CHOICES, SHIELD_AI_REVIEW_PACKS


DEFAULT_DATABASE_URL_ENV_ORDER = ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")
DEFAULT_BACKEND = "postgres"
DEFAULT_VERSION = 2
VALID_SCAN_MODES = {"all", "only_included"}
VALID_SHIELD_ACTIONS = {"disabled", "detect", "log", "delete_log", "delete_escalate", "timeout_log"}
VALID_SHIELD_SENSITIVITIES = {"low", "normal", "high"}
LOW_CONFIDENCE_ACTIONS = {"detect", "log"}
MEDIUM_CONFIDENCE_ACTIONS = {"detect", "log", "delete_log"}
HIGH_CONFIDENCE_ACTIONS = VALID_SHIELD_ACTIONS - {"disabled"}
CONFIDENCE_TIERS = ("low", "medium", "high")
SHIELD_META_GLOBAL_AI_OVERRIDE_KEY = "global_ai_override"


class ShieldStorageUnavailable(RuntimeError):
    pass


def default_shield_meta() -> dict[str, Any]:
    return {
        "global_ai_override_enabled": False,
        "global_ai_override_updated_by": None,
        "global_ai_override_updated_at": None,
    }


def _legacy_action_policy(action: str) -> tuple[str, str, str]:
    cleaned = str(action).strip().lower()
    if cleaned == "detect":
        return ("detect", "detect", "detect")
    if cleaned == "log":
        return ("log", "log", "log")
    if cleaned == "delete_log":
        return ("log", "delete_log", "delete_log")
    if cleaned == "timeout_log":
        return ("log", "delete_log", "timeout_log")
    if cleaned == "delete_escalate":
        return ("log", "delete_log", "delete_escalate")
    return ("log", "log", "log")


def _clean_action_value(value: Any, *, allowed: set[str], fallback: str) -> str:
    cleaned = str(value).strip().lower() if isinstance(value, str) else fallback
    return cleaned if cleaned in allowed else fallback


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
        "privacy_low_action": "log",
        "privacy_medium_action": "log",
        "privacy_high_action": "log",
        "privacy_sensitivity": "normal",
        "promo_enabled": False,
        "promo_action": "log",
        "promo_low_action": "log",
        "promo_medium_action": "log",
        "promo_high_action": "log",
        "promo_sensitivity": "normal",
        "scam_enabled": False,
        "scam_action": "log",
        "scam_low_action": "log",
        "scam_medium_action": "log",
        "scam_high_action": "log",
        "scam_sensitivity": "normal",
        "ai_enabled": False,
        "ai_min_confidence": "high",
        "ai_enabled_packs": list(SHIELD_AI_REVIEW_PACKS),
        "escalation_threshold": 3,
        "escalation_window_minutes": 15,
        "timeout_minutes": 10,
        "custom_patterns": [],
    }


def default_shield_state() -> dict[str, Any]:
    return {"version": DEFAULT_VERSION, "meta": default_shield_meta(), "guilds": {}}


def _clean_int_list(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _clean_text_list(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({str(value).strip().casefold() for value in values if isinstance(value, str) and str(value).strip()})


def _legacy_pack_payload(config: dict[str, Any], pack: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    sources = []
    pack_map = config.get("packs")
    if isinstance(pack_map, dict):
        sources.append(pack_map.get(pack))
    sources.append(config.get(pack))

    for candidate in sources:
        if isinstance(candidate, bool):
            payload.setdefault("enabled", candidate)
            continue
        if not isinstance(candidate, dict):
            continue
        if "enabled" in candidate:
            payload.setdefault("enabled", candidate.get("enabled"))
        elif "tracking" in candidate:
            payload.setdefault("enabled", candidate.get("tracking"))
        if "action" in candidate:
            payload.setdefault("action", candidate.get("action"))
        if "sensitivity" in candidate:
            payload.setdefault("sensitivity", candidate.get("sensitivity"))
    return payload


def normalize_guild_shield_config(guild_id: int, config: Any) -> dict[str, Any]:
    cleaned = default_guild_shield_config(guild_id)
    if not isinstance(config, dict):
        return cleaned

    cleaned["module_enabled"] = bool(config.get("module_enabled"))
    cleaned["log_channel_id"] = config.get("log_channel_id") if isinstance(config.get("log_channel_id"), int) else None
    cleaned["alert_role_id"] = config.get("alert_role_id") if isinstance(config.get("alert_role_id"), int) else None
    scan_mode = config.get("scan_mode", "all")
    cleaned["scan_mode"] = scan_mode if scan_mode in VALID_SCAN_MODES else "all"

    for field in (
        "included_channel_ids",
        "excluded_channel_ids",
        "included_user_ids",
        "excluded_user_ids",
        "included_role_ids",
        "excluded_role_ids",
        "trusted_role_ids",
    ):
        cleaned[field] = _clean_int_list(config.get(field))
    for field in ("allow_domains", "allow_invite_codes", "allow_phrases"):
        cleaned[field] = _clean_text_list(config.get(field))

    for pack in ("privacy", "promo", "scam"):
        enabled_field = f"{pack}_enabled"
        action_field = f"{pack}_action"
        low_action_field = f"{pack}_low_action"
        medium_action_field = f"{pack}_medium_action"
        high_action_field = f"{pack}_high_action"
        sensitivity_field = f"{pack}_sensitivity"
        legacy = _legacy_pack_payload(config, pack)

        if enabled_field in config:
            cleaned[enabled_field] = bool(config.get(enabled_field))
        else:
            cleaned[enabled_field] = bool(legacy.get("enabled"))

        action = str(config.get(action_field, legacy.get("action", "log"))).strip().lower()
        cleaned[action_field] = action if action in VALID_SHIELD_ACTIONS else "log"
        default_low, default_medium, default_high = _legacy_action_policy(cleaned[action_field])
        cleaned[low_action_field] = _clean_action_value(config.get(low_action_field), allowed=LOW_CONFIDENCE_ACTIONS, fallback=default_low)
        cleaned[medium_action_field] = _clean_action_value(
            config.get(medium_action_field),
            allowed=MEDIUM_CONFIDENCE_ACTIONS,
            fallback=default_medium,
        )
        cleaned[high_action_field] = _clean_action_value(config.get(high_action_field), allowed=HIGH_CONFIDENCE_ACTIONS, fallback=default_high)
        cleaned[action_field] = cleaned[high_action_field]

        sensitivity = str(config.get(sensitivity_field, legacy.get("sensitivity", "normal"))).strip().lower()
        cleaned[sensitivity_field] = sensitivity if sensitivity in VALID_SHIELD_SENSITIVITIES else "normal"

    cleaned["ai_enabled"] = bool(config.get("ai_enabled"))
    ai_min_confidence = str(config.get("ai_min_confidence", "high")).strip().lower()
    cleaned["ai_min_confidence"] = ai_min_confidence if ai_min_confidence in SHIELD_AI_MIN_CONFIDENCE_CHOICES else "high"
    raw_ai_packs = config.get("ai_enabled_packs", list(SHIELD_AI_REVIEW_PACKS))
    if isinstance(raw_ai_packs, (list, tuple, set)):
        cleaned["ai_enabled_packs"] = sorted(
            {
                str(value).strip().lower()
                for value in raw_ai_packs
                if str(value).strip().lower() in SHIELD_AI_REVIEW_PACKS
            }
        )
    else:
        cleaned["ai_enabled_packs"] = list(SHIELD_AI_REVIEW_PACKS)

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
        if action not in VALID_SHIELD_ACTIONS - {"disabled"}:
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
        meta = payload.get("meta")
        if isinstance(meta, dict):
            cleaned_meta = default_shield_meta()
            cleaned_meta["global_ai_override_enabled"] = bool(meta.get("global_ai_override_enabled"))
            updated_by = meta.get("global_ai_override_updated_by")
            cleaned_meta["global_ai_override_updated_by"] = updated_by if isinstance(updated_by, int) and updated_by > 0 else None
            updated_at = meta.get("global_ai_override_updated_at")
            cleaned_meta["global_ai_override_updated_at"] = updated_at if isinstance(updated_at, str) and updated_at.strip() else None
            normalized["meta"] = cleaned_meta
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
        return normalize_guild_shield_config(guild_id, config)


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
        self._last_flushed_state = default_shield_state()

    async def load(self) -> dict[str, Any]:
        await self._connect()
        await self._ensure_schema()
        await self._reload_from_db()
        self._last_flushed_state = deepcopy(self.state)
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
        self._last_flushed_state = deepcopy(snapshot)
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
                "privacy_low_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_medium_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_high_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "promo_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "promo_action TEXT NOT NULL DEFAULT 'log', "
                "promo_low_action TEXT NOT NULL DEFAULT 'log', "
                "promo_medium_action TEXT NOT NULL DEFAULT 'log', "
                "promo_high_action TEXT NOT NULL DEFAULT 'log', "
                "promo_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "scam_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "scam_action TEXT NOT NULL DEFAULT 'log', "
                "scam_low_action TEXT NOT NULL DEFAULT 'log', "
                "scam_medium_action TEXT NOT NULL DEFAULT 'log', "
                "scam_high_action TEXT NOT NULL DEFAULT 'log', "
                "scam_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "ai_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "ai_min_confidence TEXT NOT NULL DEFAULT 'high', "
                "ai_enabled_packs JSONB NOT NULL DEFAULT '[\"privacy\",\"promo\",\"scam\"]'::jsonb, "
                "escalation_threshold SMALLINT NOT NULL DEFAULT 3, "
                "escalation_window_minutes SMALLINT NOT NULL DEFAULT 15, "
                "timeout_minutes SMALLINT NOT NULL DEFAULT 10, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS shield_meta ("
                "key TEXT PRIMARY KEY, "
                "value JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_min_confidence TEXT NOT NULL DEFAULT 'high'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_enabled_packs JSONB NOT NULL DEFAULT '[\"privacy\",\"promo\",\"scam\"]'::jsonb",
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
            meta_rows = await conn.fetch("SELECT key, value FROM shield_meta")
        for row in config_rows:
            guild_id = int(row["guild_id"])
            loaded["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "module_enabled": bool(row["module_enabled"]),
                "log_channel_id": row["log_channel_id"],
                "alert_role_id": row["alert_role_id"],
                "scan_mode": row["scan_mode"],
                "included_channel_ids": decode_postgres_json_array(
                    row["included_channel_ids"],
                    label="shield_guild_configs.included_channel_ids",
                ),
                "excluded_channel_ids": decode_postgres_json_array(
                    row["excluded_channel_ids"],
                    label="shield_guild_configs.excluded_channel_ids",
                ),
                "included_user_ids": decode_postgres_json_array(
                    row["included_user_ids"],
                    label="shield_guild_configs.included_user_ids",
                ),
                "excluded_user_ids": decode_postgres_json_array(
                    row["excluded_user_ids"],
                    label="shield_guild_configs.excluded_user_ids",
                ),
                "included_role_ids": decode_postgres_json_array(
                    row["included_role_ids"],
                    label="shield_guild_configs.included_role_ids",
                ),
                "excluded_role_ids": decode_postgres_json_array(
                    row["excluded_role_ids"],
                    label="shield_guild_configs.excluded_role_ids",
                ),
                "trusted_role_ids": decode_postgres_json_array(
                    row["trusted_role_ids"],
                    label="shield_guild_configs.trusted_role_ids",
                ),
                "allow_domains": decode_postgres_json_array(
                    row["allow_domains"],
                    label="shield_guild_configs.allow_domains",
                ),
                "allow_invite_codes": decode_postgres_json_array(
                    row["allow_invite_codes"],
                    label="shield_guild_configs.allow_invite_codes",
                ),
                "allow_phrases": decode_postgres_json_array(
                    row["allow_phrases"],
                    label="shield_guild_configs.allow_phrases",
                ),
                "privacy_enabled": bool(row["privacy_enabled"]),
                "privacy_action": row["privacy_action"],
                "privacy_low_action": row["privacy_low_action"],
                "privacy_medium_action": row["privacy_medium_action"],
                "privacy_high_action": row["privacy_high_action"],
                "privacy_sensitivity": row["privacy_sensitivity"],
                "promo_enabled": bool(row["promo_enabled"]),
                "promo_action": row["promo_action"],
                "promo_low_action": row["promo_low_action"],
                "promo_medium_action": row["promo_medium_action"],
                "promo_high_action": row["promo_high_action"],
                "promo_sensitivity": row["promo_sensitivity"],
                "scam_enabled": bool(row["scam_enabled"]),
                "scam_action": row["scam_action"],
                "scam_low_action": row["scam_low_action"],
                "scam_medium_action": row["scam_medium_action"],
                "scam_high_action": row["scam_high_action"],
                "scam_sensitivity": row["scam_sensitivity"],
                "ai_enabled": bool(row["ai_enabled"]),
                "ai_min_confidence": row["ai_min_confidence"],
                "ai_enabled_packs": decode_postgres_json_array(
                    row["ai_enabled_packs"],
                    label="shield_guild_configs.ai_enabled_packs",
                ),
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
        for row in meta_rows:
            if row["key"] != SHIELD_META_GLOBAL_AI_OVERRIDE_KEY:
                continue
            value = decode_postgres_json_object(
                row["value"],
                label="shield_meta.value",
            )
            loaded["meta"] = {
                "global_ai_override_enabled": bool(value.get("enabled")),
                "global_ai_override_updated_by": value.get("updated_by") if isinstance(value.get("updated_by"), int) and value.get("updated_by") > 0 else None,
                "global_ai_override_updated_at": value.get("updated_at") if isinstance(value.get("updated_at"), str) and value.get("updated_at").strip() else None,
            }
        self.state = self.normalize_state(loaded)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        previous = self._last_flushed_state
        current_guilds = snapshot.get("guilds", {})
        previous_guilds = previous.get("guilds", {})
        removed_guild_ids = sorted(
            int(guild_id_text)
            for guild_id_text in previous_guilds
            if guild_id_text not in current_guilds
        )
        changed_configs = [
            config
            for guild_id_text, config in current_guilds.items()
            if previous_guilds.get(guild_id_text) != config
        ]
        meta_changed = snapshot.get("meta") != previous.get("meta")
        if not removed_guild_ids and not changed_configs and not meta_changed:
            return
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if meta_changed:
                    await conn.execute(
                        (
                            "INSERT INTO shield_meta (key, value, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) "
                            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
                        ),
                        SHIELD_META_GLOBAL_AI_OVERRIDE_KEY,
                        json.dumps(
                            {
                                "enabled": bool(snapshot["meta"]["global_ai_override_enabled"]),
                                "updated_by": snapshot["meta"]["global_ai_override_updated_by"],
                                "updated_at": snapshot["meta"]["global_ai_override_updated_at"],
                            }
                        ),
                    )
                for guild_id in removed_guild_ids:
                    await conn.execute("DELETE FROM shield_guild_configs WHERE guild_id = $1", guild_id)
                for config in changed_configs:
                    await self._upsert_guild_config(conn, config)
                    await self._replace_custom_patterns_for_guild(conn, config["guild_id"], config.get("custom_patterns", []))

    async def _upsert_guild_config(self, conn, config: dict[str, Any]):
        await conn.execute(
            (
                "INSERT INTO shield_guild_configs ("
                "guild_id, module_enabled, log_channel_id, alert_role_id, scan_mode, "
                "included_channel_ids, excluded_channel_ids, included_user_ids, excluded_user_ids, "
                "included_role_ids, excluded_role_ids, trusted_role_ids, allow_domains, allow_invite_codes, allow_phrases, "
                "privacy_enabled, privacy_action, privacy_low_action, privacy_medium_action, privacy_high_action, privacy_sensitivity, "
                "promo_enabled, promo_action, promo_low_action, promo_medium_action, promo_high_action, promo_sensitivity, "
                "scam_enabled, scam_action, scam_low_action, scam_medium_action, scam_high_action, scam_sensitivity, "
                "ai_enabled, ai_min_confidence, ai_enabled_packs, "
                "escalation_threshold, escalation_window_minutes, timeout_minutes, updated_at"
                ") VALUES ("
                "$1, $2, $3, $4, $5, "
                "$6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, "
                "$10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, "
                "$16, $17, $18, $19, $20, $21, "
                "$22, $23, $24, $25, $26, $27, "
                "$28, $29, $30, $31, $32, $33, "
                "$34, $35, $36::jsonb, $37, $38, $39, timezone('utc', now())"
                ") "
                "ON CONFLICT (guild_id) DO UPDATE SET "
                "module_enabled = EXCLUDED.module_enabled, "
                "log_channel_id = EXCLUDED.log_channel_id, "
                "alert_role_id = EXCLUDED.alert_role_id, "
                "scan_mode = EXCLUDED.scan_mode, "
                "included_channel_ids = EXCLUDED.included_channel_ids, "
                "excluded_channel_ids = EXCLUDED.excluded_channel_ids, "
                "included_user_ids = EXCLUDED.included_user_ids, "
                "excluded_user_ids = EXCLUDED.excluded_user_ids, "
                "included_role_ids = EXCLUDED.included_role_ids, "
                "excluded_role_ids = EXCLUDED.excluded_role_ids, "
                "trusted_role_ids = EXCLUDED.trusted_role_ids, "
                "allow_domains = EXCLUDED.allow_domains, "
                "allow_invite_codes = EXCLUDED.allow_invite_codes, "
                "allow_phrases = EXCLUDED.allow_phrases, "
                "privacy_enabled = EXCLUDED.privacy_enabled, "
                "privacy_action = EXCLUDED.privacy_action, "
                "privacy_low_action = EXCLUDED.privacy_low_action, "
                "privacy_medium_action = EXCLUDED.privacy_medium_action, "
                "privacy_high_action = EXCLUDED.privacy_high_action, "
                "privacy_sensitivity = EXCLUDED.privacy_sensitivity, "
                "promo_enabled = EXCLUDED.promo_enabled, "
                "promo_action = EXCLUDED.promo_action, "
                "promo_low_action = EXCLUDED.promo_low_action, "
                "promo_medium_action = EXCLUDED.promo_medium_action, "
                "promo_high_action = EXCLUDED.promo_high_action, "
                "promo_sensitivity = EXCLUDED.promo_sensitivity, "
                "scam_enabled = EXCLUDED.scam_enabled, "
                "scam_action = EXCLUDED.scam_action, "
                "scam_low_action = EXCLUDED.scam_low_action, "
                "scam_medium_action = EXCLUDED.scam_medium_action, "
                "scam_high_action = EXCLUDED.scam_high_action, "
                "scam_sensitivity = EXCLUDED.scam_sensitivity, "
                "ai_enabled = EXCLUDED.ai_enabled, "
                "ai_min_confidence = EXCLUDED.ai_min_confidence, "
                "ai_enabled_packs = EXCLUDED.ai_enabled_packs, "
                "escalation_threshold = EXCLUDED.escalation_threshold, "
                "escalation_window_minutes = EXCLUDED.escalation_window_minutes, "
                "timeout_minutes = EXCLUDED.timeout_minutes, "
                "updated_at = EXCLUDED.updated_at"
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
            config["privacy_low_action"],
            config["privacy_medium_action"],
            config["privacy_high_action"],
            config["privacy_sensitivity"],
            config["promo_enabled"],
            config["promo_action"],
            config["promo_low_action"],
            config["promo_medium_action"],
            config["promo_high_action"],
            config["promo_sensitivity"],
            config["scam_enabled"],
            config["scam_action"],
            config["scam_low_action"],
            config["scam_medium_action"],
            config["scam_high_action"],
            config["scam_sensitivity"],
            config["ai_enabled"],
            config["ai_min_confidence"],
            json.dumps(config["ai_enabled_packs"]),
            config["escalation_threshold"],
            config["escalation_window_minutes"],
            config["timeout_minutes"],
        )

    async def _replace_custom_patterns_for_guild(self, conn, guild_id: int, patterns: list[dict[str, Any]]):
        await conn.execute("DELETE FROM shield_custom_patterns WHERE guild_id = $1", guild_id)
        rows = [
            (
                item["pattern_id"],
                guild_id,
                item["label"],
                item["pattern"],
                item["mode"],
                item["action"],
                item["enabled"],
            )
            for item in patterns
            if isinstance(item, dict)
        ]
        if rows:
            await conn.executemany(
                "INSERT INTO shield_custom_patterns (pattern_id, guild_id, label, pattern, mode, action, enabled) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                rows,
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
