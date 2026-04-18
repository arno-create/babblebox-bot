from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array


DEFAULT_LEGACY_JSON_PATH = Path(__file__).resolve().parent.parent / ".cache" / "utility_state.json"


class UtilityStorageUnavailable(RuntimeError):
    pass


def default_utility_state() -> dict[str, Any]:
    return {
        "version": 6,
        "watch": {},
        "return_watches": {},
        "later": {},
        "reminders": {},
        "bump_configs": {},
        "bump_cycles": {},
        "afk": {},
        "afk_settings": {},
        "afk_schedules": {},
    }


def _default_watch_config() -> dict[str, Any]:
    return {
        "mention_global": False,
        "mention_guild_ids": [],
        "mention_channel_ids": [],
        "reply_global": False,
        "reply_guild_ids": [],
        "reply_channel_ids": [],
        "excluded_channel_ids": [],
        "ignored_user_ids": [],
        "keywords": [],
    }


def _resolve_legacy_json_path(path: Path | None = None) -> Path | None:
    if path is not None:
        return path
    configured = os.getenv("UTILITY_JSON_MIGRATION_PATH", "").strip() or os.getenv("UTILITY_JSON_PATH", "").strip()
    return Path(configured) if configured else DEFAULT_LEGACY_JSON_PATH


def _resolve_database_url(configured: str | None = None) -> tuple[str, str | None]:
    if configured is not None and configured.strip():
        return configured.strip(), "argument"
    for env_name in ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL"):
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


class _BaseUtilityStore:
    backend_name = "unknown"

    def __init__(self):
        self.state: dict[str, Any] = default_utility_state()

    async def load(self) -> dict[str, Any]:
        raise NotImplementedError

    async def flush(self) -> bool:
        raise NotImplementedError

    async def close(self):
        return None

    def normalize_state(self, payload: Any) -> dict[str, Any]:
        normalized = default_utility_state()
        if not isinstance(payload, dict):
            return normalized

        version = payload.get("version")
        normalized["version"] = version if isinstance(version, int) and version > 0 else 6

        watch = payload.get("watch")
        if isinstance(watch, dict):
            cleaned_watch: dict[str, Any] = {}
            for user_id, config in watch.items():
                if not isinstance(config, dict):
                    continue
                keywords = []
                for keyword in config.get("keywords", []):
                    if not isinstance(keyword, dict):
                        continue
                    phrase = keyword.get("phrase")
                    mode = keyword.get("mode", "contains")
                    if not isinstance(phrase, str) or not phrase.strip() or mode not in {"contains", "word"}:
                        continue
                    guild_id = keyword.get("guild_id")
                    channel_id = keyword.get("channel_id")
                    created_at = keyword.get("created_at")
                    keywords.append(
                        {
                            "phrase": phrase.strip(),
                            "mode": mode,
                            "guild_id": guild_id if isinstance(guild_id, int) else None,
                            "channel_id": channel_id if isinstance(channel_id, int) and isinstance(guild_id, int) else None,
                            "created_at": created_at if isinstance(created_at, str) else None,
                        }
                    )
                mention_guild_ids = sorted({guild_id for guild_id in config.get("mention_guild_ids", []) if isinstance(guild_id, int) and guild_id > 0})
                mention_channel_ids = sorted({channel_id for channel_id in config.get("mention_channel_ids", []) if isinstance(channel_id, int) and channel_id > 0})
                reply_guild_ids = sorted({guild_id for guild_id in config.get("reply_guild_ids", []) if isinstance(guild_id, int) and guild_id > 0})
                reply_channel_ids = sorted({channel_id for channel_id in config.get("reply_channel_ids", []) if isinstance(channel_id, int) and channel_id > 0})
                excluded_channel_ids = sorted({channel_id for channel_id in config.get("excluded_channel_ids", []) if isinstance(channel_id, int) and channel_id > 0})
                ignored_user_ids = sorted({other_user_id for other_user_id in config.get("ignored_user_ids", []) if isinstance(other_user_id, int) and other_user_id > 0})
                mention_global = bool(config.get("mention_global"))
                reply_global = bool(config.get("reply_global"))
                if (
                    mention_global
                    or mention_guild_ids
                    or mention_channel_ids
                    or reply_global
                    or reply_guild_ids
                    or reply_channel_ids
                    or excluded_channel_ids
                    or ignored_user_ids
                    or keywords
                ):
                    cleaned_watch[str(user_id)] = {
                        "mention_global": mention_global,
                        "mention_guild_ids": mention_guild_ids,
                        "mention_channel_ids": mention_channel_ids,
                        "reply_global": reply_global,
                        "reply_guild_ids": reply_guild_ids,
                        "reply_channel_ids": reply_channel_ids,
                        "excluded_channel_ids": excluded_channel_ids,
                        "ignored_user_ids": ignored_user_ids,
                        "keywords": keywords,
                    }
            normalized["watch"] = cleaned_watch

        normalized["return_watches"] = self._normalize_return_watches(payload.get("return_watches"))

        for section in ("later", "reminders"):
            value = payload.get(section)
            if isinstance(value, dict):
                normalized[section] = deepcopy(value)
        normalized["bump_configs"] = self._normalize_bump_configs(payload.get("bump_configs"))
        normalized["bump_cycles"] = self._normalize_bump_cycles(payload.get("bump_cycles"))
        normalized["afk"] = self._normalize_afk_state(payload.get("afk"))
        normalized["afk_settings"] = self._normalize_afk_settings(payload.get("afk_settings"))
        normalized["afk_schedules"] = self._normalize_afk_schedules(payload.get("afk_schedules"))
        return normalized

    def _parse_iso_datetime(self, value: Any):
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)

    def _serialize_datetime(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        return value.astimezone(timezone.utc).isoformat()

    def _normalize_afk_state(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for user_id_text, record in payload.items():
            if not isinstance(record, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            status = "scheduled" if record.get("status") == "scheduled" else "active"
            normalized_record = {
                "user_id": user_id,
                "status": status,
                "reason": record.get("reason").strip() if isinstance(record.get("reason"), str) and record.get("reason").strip() else None,
                "preset": record.get("preset").strip().casefold() if isinstance(record.get("preset"), str) and record.get("preset").strip() else None,
                "created_at": self._serialize_datetime(self._parse_iso_datetime(record.get("created_at"))),
                "set_at": self._serialize_datetime(self._parse_iso_datetime(record.get("set_at"))),
                "starts_at": self._serialize_datetime(self._parse_iso_datetime(record.get("starts_at"))),
                "ends_at": self._serialize_datetime(self._parse_iso_datetime(record.get("ends_at"))),
                "schedule_id": record.get("schedule_id").strip() if isinstance(record.get("schedule_id"), str) and record.get("schedule_id").strip() else None,
                "occurrence_at": self._serialize_datetime(self._parse_iso_datetime(record.get("occurrence_at"))),
            }
            cleaned[str(user_id)] = normalized_record
        return cleaned

    def _normalize_afk_settings(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for user_id_text, record in payload.items():
            if not isinstance(record, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            timezone_name = record.get("timezone")
            if not isinstance(timezone_name, str) or not timezone_name.strip():
                continue
            cleaned[str(user_id)] = {"timezone": timezone_name.strip()}
        return cleaned

    def _normalize_afk_schedules(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for schedule_id, record in payload.items():
            if not isinstance(record, dict) or not isinstance(schedule_id, str) or not schedule_id.strip():
                continue
            try:
                user_id = int(record.get("user_id"))
                weekday_mask = int(record.get("weekday_mask", 0))
                local_hour = int(record.get("local_hour"))
                local_minute = int(record.get("local_minute"))
            except (TypeError, ValueError):
                continue
            repeat_rule = str(record.get("repeat") or "").strip().casefold()
            timezone_name = record.get("timezone")
            if repeat_rule not in {"daily", "weekdays", "weekly", "custom"}:
                continue
            if not isinstance(timezone_name, str) or not timezone_name.strip():
                continue
            if not (1 <= weekday_mask <= 127 and 0 <= local_hour <= 23 and 0 <= local_minute <= 59):
                continue
            duration_seconds = record.get("duration_seconds")
            if not isinstance(duration_seconds, int) or duration_seconds <= 0:
                duration_seconds = None
            cleaned[schedule_id] = {
                "id": schedule_id,
                "user_id": user_id,
                "reason": record.get("reason").strip() if isinstance(record.get("reason"), str) and record.get("reason").strip() else None,
                "preset": record.get("preset").strip().casefold() if isinstance(record.get("preset"), str) and record.get("preset").strip() else None,
                "timezone": timezone_name.strip(),
                "repeat": repeat_rule,
                "weekday_mask": weekday_mask,
                "local_hour": local_hour,
                "local_minute": local_minute,
                "duration_seconds": duration_seconds,
                "created_at": self._serialize_datetime(self._parse_iso_datetime(record.get("created_at"))),
                "next_start_at": self._serialize_datetime(self._parse_iso_datetime(record.get("next_start_at"))),
            }
        return cleaned

    def _normalize_return_watches(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for watch_id, record in payload.items():
            if not isinstance(record, dict) or not isinstance(watch_id, str) or not watch_id.strip():
                continue
            try:
                watcher_user_id = int(record.get("watcher_user_id"))
                guild_id = int(record.get("guild_id"))
                target_id = int(record.get("target_id"))
            except (TypeError, ValueError):
                continue
            target_type = str(record.get("target_type") or "").strip().casefold()
            if target_type not in {"user", "channel"}:
                continue
            created_at = self._serialize_datetime(self._parse_iso_datetime(record.get("created_at")))
            expires_at = self._serialize_datetime(self._parse_iso_datetime(record.get("expires_at")))
            if created_at is None or expires_at is None:
                continue
            created_from = record.get("created_from")
            cleaned[watch_id] = {
                "id": watch_id,
                "watcher_user_id": watcher_user_id,
                "guild_id": guild_id,
                "target_type": target_type,
                "target_id": target_id,
                "created_at": created_at,
                "expires_at": expires_at,
                "created_from": created_from.strip() if isinstance(created_from, str) and created_from.strip() else None,
            }
        return cleaned

    def _normalize_bump_configs(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for guild_id_text, record in payload.items():
            if not isinstance(record, dict):
                continue
            try:
                guild_id = int(guild_id_text)
            except (TypeError, ValueError):
                continue
            provider = str(record.get("provider") or "disboard").strip().casefold() or "disboard"
            detection_channel_ids = sorted(
                {
                    channel_id
                    for channel_id in record.get("detection_channel_ids", [])
                    if isinstance(channel_id, int) and channel_id > 0
                }
            )
            reminder_channel_id = record.get("reminder_channel_id")
            reminder_role_id = record.get("reminder_role_id")
            reminder_text = record.get("reminder_text")
            thanks_text = record.get("thanks_text")
            thanks_mode = str(record.get("thanks_mode") or "quiet").strip().casefold()
            if thanks_mode not in {"quiet", "public", "off"}:
                thanks_mode = "quiet"
            cleaned[str(guild_id)] = {
                "guild_id": guild_id,
                "enabled": bool(record.get("enabled")),
                "provider": provider,
                "detection_channel_ids": detection_channel_ids,
                "reminder_channel_id": reminder_channel_id if isinstance(reminder_channel_id, int) and reminder_channel_id > 0 else None,
                "reminder_role_id": reminder_role_id if isinstance(reminder_role_id, int) and reminder_role_id > 0 else None,
                "reminder_text": reminder_text.strip() if isinstance(reminder_text, str) and reminder_text.strip() else None,
                "thanks_text": thanks_text.strip() if isinstance(thanks_text, str) and thanks_text.strip() else None,
                "thanks_mode": thanks_mode,
            }
        return cleaned

    def _normalize_bump_cycles(self, payload: Any) -> dict[str, Any]:
        cleaned: dict[str, Any] = {}
        if not isinstance(payload, dict):
            return cleaned
        for cycle_id, record in payload.items():
            if not isinstance(record, dict):
                continue
            try:
                guild_id = int(record.get("guild_id"))
            except (TypeError, ValueError):
                continue
            provider = str(record.get("provider") or "").strip().casefold()
            if not provider:
                continue
            last_bumper_user_id = record.get("last_bumper_user_id")
            last_success_message_id = record.get("last_success_message_id")
            last_success_channel_id = record.get("last_success_channel_id")
            delivery_attempts = record.get("delivery_attempts")
            if not isinstance(delivery_attempts, int) or delivery_attempts < 0:
                delivery_attempts = 0
            last_provider_event_kind = record.get("last_provider_event_kind")
            cleaned[str(cycle_id or f"{guild_id}:{provider}")] = {
                "id": str(cycle_id or f"{guild_id}:{provider}"),
                "guild_id": guild_id,
                "provider": provider,
                "last_provider_event_at": self._serialize_datetime(self._parse_iso_datetime(record.get("last_provider_event_at"))),
                "last_provider_event_kind": last_provider_event_kind.strip().casefold()
                if isinstance(last_provider_event_kind, str) and last_provider_event_kind.strip()
                else None,
                "last_bump_at": self._serialize_datetime(self._parse_iso_datetime(record.get("last_bump_at"))),
                "last_bumper_user_id": last_bumper_user_id if isinstance(last_bumper_user_id, int) and last_bumper_user_id > 0 else None,
                "last_success_message_id": last_success_message_id if isinstance(last_success_message_id, int) and last_success_message_id > 0 else None,
                "last_success_channel_id": last_success_channel_id if isinstance(last_success_channel_id, int) and last_success_channel_id > 0 else None,
                "due_at": self._serialize_datetime(self._parse_iso_datetime(record.get("due_at"))),
                "reminder_sent_at": self._serialize_datetime(self._parse_iso_datetime(record.get("reminder_sent_at"))),
                "delivery_attempts": delivery_attempts,
                "last_delivery_attempt_at": self._serialize_datetime(self._parse_iso_datetime(record.get("last_delivery_attempt_at"))),
                "retry_after": self._serialize_datetime(self._parse_iso_datetime(record.get("retry_after"))),
                "last_delivery_error": record.get("last_delivery_error").strip()
                if isinstance(record.get("last_delivery_error"), str) and record.get("last_delivery_error").strip()
                else None,
            }
        return cleaned


class _MemoryUtilityStore(_BaseUtilityStore):
    backend_name = "memory"

    async def load(self) -> dict[str, Any]:
        self.state = default_utility_state()
        return self.state

    async def flush(self) -> bool:
        self.state = deepcopy(self.state)
        return True


class _PostgresUtilityStore(_BaseUtilityStore):
    backend_name = "postgres"

    def __init__(self, dsn: str, *, legacy_json_path: Path | None = None):
        super().__init__()
        self.dsn = dsn
        self.legacy_json_path = _resolve_legacy_json_path(legacy_json_path)
        self._asyncpg = None
        self._pool = None
        self._io_lock = asyncio.Lock()
        self._last_flushed_state = default_utility_state()

    async def load(self) -> dict[str, Any]:
        await self._connect()
        await self._ensure_schema()
        await self._migrate_legacy_brb_table()
        await self._maybe_import_legacy_json_seed()
        await self._reload_from_db()
        self._last_flushed_state = deepcopy(self.state)
        print("Utility storage backend: postgres")
        return self.state

    async def flush(self) -> bool:
        snapshot = self.normalize_state(deepcopy(self.state))
        async with self._io_lock:
            try:
                await self._flush_snapshot(snapshot)
            except Exception as exc:
                print(f"Utility Postgres store flush failed: {exc}")
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
            raise UtilityStorageUnavailable("asyncpg is not installed, so Postgres utility storage is unavailable.") from exc

        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=3,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-utility-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise UtilityStorageUnavailable(f"Could not connect to Postgres utility storage: {last_error}") from last_error

    async def _ensure_schema(self):
        statements = [
            "CREATE TABLE IF NOT EXISTS utility_meta (key TEXT PRIMARY KEY, value JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            "CREATE TABLE IF NOT EXISTS utility_watch_configs (user_id BIGINT PRIMARY KEY, mention_global BOOLEAN NOT NULL DEFAULT FALSE, mention_guild_ids JSONB NOT NULL DEFAULT '[]'::jsonb, updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            "CREATE TABLE IF NOT EXISTS utility_watch_keywords (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES utility_watch_configs(user_id) ON DELETE CASCADE, guild_id BIGINT NULL, phrase TEXT NOT NULL, mode TEXT NOT NULL, created_at TIMESTAMPTZ NULL)",
            "CREATE TABLE IF NOT EXISTS utility_return_watches (id TEXT PRIMARY KEY, watcher_user_id BIGINT NOT NULL, guild_id BIGINT NOT NULL, target_type TEXT NOT NULL, target_id BIGINT NOT NULL, created_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, created_from TEXT NULL)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_utility_return_watches_dedupe ON utility_return_watches (watcher_user_id, guild_id, target_type, target_id)",
            "CREATE INDEX IF NOT EXISTS ix_utility_return_watches_target ON utility_return_watches (guild_id, target_type, target_id)",
            "CREATE INDEX IF NOT EXISTS ix_utility_return_watches_expires_at ON utility_return_watches (expires_at)",
            "CREATE TABLE IF NOT EXISTS utility_later_markers (user_id BIGINT NOT NULL, guild_id BIGINT NOT NULL, guild_name TEXT NOT NULL, channel_id BIGINT NOT NULL, channel_name TEXT NOT NULL, message_id BIGINT NOT NULL, message_jump_url TEXT NOT NULL, message_created_at TIMESTAMPTZ NULL, saved_at TIMESTAMPTZ NULL, author_name TEXT NOT NULL, author_id BIGINT NOT NULL, preview TEXT NOT NULL, attachment_labels JSONB NOT NULL DEFAULT '[]'::jsonb, PRIMARY KEY (user_id, channel_id))",
            "CREATE INDEX IF NOT EXISTS ix_utility_later_markers_user ON utility_later_markers (user_id)",
            "CREATE TABLE IF NOT EXISTS utility_reminders (id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, text TEXT NOT NULL, delivery TEXT NOT NULL, created_at TIMESTAMPTZ NULL, due_at TIMESTAMPTZ NULL, guild_id BIGINT NULL, guild_name TEXT NULL, channel_id BIGINT NULL, channel_name TEXT NULL, origin_jump_url TEXT NULL, delivery_attempts INTEGER NOT NULL DEFAULT 0, last_attempt_at TIMESTAMPTZ NULL, retry_after TIMESTAMPTZ NULL)",
            "CREATE INDEX IF NOT EXISTS ix_utility_reminders_due_at ON utility_reminders (due_at)",
            "CREATE INDEX IF NOT EXISTS ix_utility_reminders_user ON utility_reminders (user_id)",
            "CREATE TABLE IF NOT EXISTS utility_bump_configs (guild_id BIGINT PRIMARY KEY, enabled BOOLEAN NOT NULL DEFAULT FALSE, provider TEXT NOT NULL DEFAULT 'disboard', detection_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, reminder_channel_id BIGINT NULL, reminder_role_id BIGINT NULL, reminder_text TEXT NULL, thanks_text TEXT NULL, thanks_mode TEXT NOT NULL DEFAULT 'quiet', updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            "CREATE TABLE IF NOT EXISTS utility_bump_cycles (id TEXT PRIMARY KEY, guild_id BIGINT NOT NULL, provider TEXT NOT NULL, last_provider_event_at TIMESTAMPTZ NULL, last_provider_event_kind TEXT NULL, last_bump_at TIMESTAMPTZ NULL, last_bumper_user_id BIGINT NULL, last_success_message_id BIGINT NULL, last_success_channel_id BIGINT NULL, due_at TIMESTAMPTZ NULL, reminder_sent_at TIMESTAMPTZ NULL, delivery_attempts INTEGER NOT NULL DEFAULT 0, last_delivery_attempt_at TIMESTAMPTZ NULL, retry_after TIMESTAMPTZ NULL, last_delivery_error TEXT NULL)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_utility_bump_cycles_guild_provider ON utility_bump_cycles (guild_id, provider)",
            "CREATE INDEX IF NOT EXISTS ix_utility_bump_cycles_due_at ON utility_bump_cycles (due_at)",
            "CREATE TABLE IF NOT EXISTS utility_afk (user_id BIGINT PRIMARY KEY, status TEXT NOT NULL, reason TEXT NULL, created_at TIMESTAMPTZ NULL, set_at TIMESTAMPTZ NULL, starts_at TIMESTAMPTZ NULL, ends_at TIMESTAMPTZ NULL)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_status ON utility_afk (status)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_starts_at ON utility_afk (starts_at)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_ends_at ON utility_afk (ends_at)",
            "CREATE TABLE IF NOT EXISTS utility_afk_settings (user_id BIGINT PRIMARY KEY, timezone TEXT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            "CREATE TABLE IF NOT EXISTS utility_afk_schedules (id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, reason TEXT NULL, preset TEXT NULL, timezone TEXT NOT NULL, repeat_rule TEXT NOT NULL, weekday_mask INTEGER NOT NULL, local_hour SMALLINT NOT NULL, local_minute SMALLINT NOT NULL, duration_seconds INTEGER NULL, created_at TIMESTAMPTZ NULL, next_start_at TIMESTAMPTZ NULL)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_schedules_user ON utility_afk_schedules (user_id)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_schedules_next_start ON utility_afk_schedules (next_start_at)",
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS mention_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS reply_global BOOLEAN NOT NULL DEFAULT FALSE")
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS reply_guild_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS reply_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS excluded_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_watch_configs ADD COLUMN IF NOT EXISTS ignored_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_watch_keywords ADD COLUMN IF NOT EXISTS channel_id BIGINT NULL")
            await conn.execute("ALTER TABLE utility_later_markers ADD COLUMN IF NOT EXISTS attachment_labels JSONB NOT NULL DEFAULT '[]'::jsonb")
            await conn.execute("ALTER TABLE utility_reminders ADD COLUMN IF NOT EXISTS delivery_attempts INTEGER NOT NULL DEFAULT 0")
            await conn.execute("ALTER TABLE utility_reminders ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ NULL")
            await conn.execute("ALTER TABLE utility_reminders ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ NULL")
            await conn.execute("ALTER TABLE utility_afk ADD COLUMN IF NOT EXISTS preset TEXT NULL")
            await conn.execute("ALTER TABLE utility_afk ADD COLUMN IF NOT EXISTS schedule_id TEXT NULL")
            await conn.execute("ALTER TABLE utility_afk ADD COLUMN IF NOT EXISTS occurrence_at TIMESTAMPTZ NULL")
            await conn.execute("DROP INDEX IF EXISTS ux_utility_watch_keywords_scope")
            await conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_utility_watch_keywords_scope_v2 "
                "ON utility_watch_keywords (user_id, COALESCE(guild_id, 0), COALESCE(channel_id, 0), mode, phrase)"
            )

    async def _set_meta(self, key: str, value: dict[str, Any]):
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO utility_meta (key, value, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                key,
                json.dumps(value),
            )

    async def _migrate_legacy_brb_table(self):
        async with self._pool.acquire() as conn:
            if await conn.fetchval("SELECT 1 FROM utility_meta WHERE key = $1", "legacy_brb_migration_v1"):
                return
            if await conn.fetchval("SELECT to_regclass('public.utility_brb')") is None:
                await self._set_meta("legacy_brb_migration_v1", {"status": "missing"})
                return
            brb_rows = await conn.fetch("SELECT user_id, reason, created_at, ends_at FROM utility_brb")
            existing_afk = {row["user_id"] for row in await conn.fetch("SELECT user_id FROM utility_afk")}
            inserted = 0
            now = datetime.now(timezone.utc)
            async with conn.transaction():
                for row in brb_rows:
                    ends_at = row["ends_at"]
                    if ends_at is None or ends_at <= now or row["user_id"] in existing_afk:
                        continue
                    starts_at = row["created_at"] or now
                    await conn.execute(
                        "INSERT INTO utility_afk (user_id, status, reason, created_at, set_at, starts_at, ends_at) VALUES ($1, 'active', $2, $3, $3, $3, $4) ON CONFLICT (user_id) DO NOTHING",
                        row["user_id"],
                        row["reason"],
                        starts_at,
                        ends_at,
                    )
                    inserted += 1
                await conn.execute("DROP TABLE IF EXISTS utility_brb")
        await self._set_meta("legacy_brb_migration_v1", {"status": "migrated", "inserted_afk_rows": inserted})

    async def _maybe_import_legacy_json_seed(self):
        if self.legacy_json_path is None or not self.legacy_json_path.exists():
            return
        async with self._pool.acquire() as conn:
            if await conn.fetchval("SELECT 1 FROM utility_meta WHERE key = $1", "legacy_json_seed_v2"):
                return
            has_rows = await conn.fetchval(
                "SELECT EXISTS ("
                "SELECT 1 FROM utility_watch_configs "
                "UNION ALL SELECT 1 FROM utility_later_markers "
                "UNION ALL SELECT 1 FROM utility_reminders "
                "UNION ALL SELECT 1 FROM utility_bump_configs "
                "UNION ALL SELECT 1 FROM utility_bump_cycles "
                "UNION ALL SELECT 1 FROM utility_afk "
                "UNION ALL SELECT 1 FROM utility_afk_settings "
                "UNION ALL SELECT 1 FROM utility_afk_schedules "
                "UNION ALL SELECT 1 FROM utility_return_watches"
                ")"
            )
            if has_rows:
                await self._set_meta("legacy_json_seed_v2", {"status": "skipped_existing_db_state"})
                return

        seed_state, summary = await self._load_legacy_json_seed(self.legacy_json_path)
        if seed_state is None:
            await self._set_meta("legacy_json_seed_v2", {"status": "ignored_invalid_json", "source": str(self.legacy_json_path)})
            return
        if not any(bool(seed_state.get(section)) for section in ("watch", "later", "reminders", "afk")):
            await self._set_meta("legacy_json_seed_v2", {"status": "empty", "source": str(self.legacy_json_path)})
            return

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await self._replace_all_data(conn, seed_state)
                await conn.execute(
                    "INSERT INTO utility_meta (key, value, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                    "legacy_json_seed_v2",
                    json.dumps({"status": "imported", "source": str(self.legacy_json_path), "summary": summary}),
                )
        print(f"Imported legacy utility JSON seed into Postgres: {summary}")

    async def _load_legacy_json_seed(self, path: Path) -> tuple[dict[str, Any] | None, dict[str, int]]:
        try:
            raw = await asyncio.to_thread(path.read_text, encoding="utf-8")
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Legacy utility JSON import skipped: {exc}")
            return None, {"watch_users": 0, "later_markers": 0, "reminders": 0, "afk_users": 0}
        normalized = self.normalize_state(payload)
        normalized["afk"] = self._merge_legacy_brb_into_afk(payload, normalized.get("afk", {}))
        later_count = sum(len(markers) for markers in normalized.get("later", {}).values() if isinstance(markers, dict))
        summary = {"watch_users": len(normalized.get("watch", {})), "later_markers": later_count, "reminders": len(normalized.get("reminders", {})), "afk_users": len(normalized.get("afk", {}))}
        return normalized, summary

    def _merge_legacy_brb_into_afk(self, payload: Any, afk_state: dict[str, Any]) -> dict[str, Any]:
        merged = dict(afk_state)
        brb = payload.get("brb") if isinstance(payload, dict) else None
        if not isinstance(brb, dict):
            return merged
        now = datetime.now(timezone.utc)
        for user_id, record in brb.items():
            if not isinstance(record, dict) or str(user_id) in merged:
                continue
            ends_at = self._parse_iso_datetime(record.get("ends_at"))
            if ends_at is None or ends_at <= now:
                continue
            created_at = self._parse_iso_datetime(record.get("created_at")) or now
            merged[str(user_id)] = {
                "user_id": int(user_id),
                "status": "active",
                "reason": record.get("reason"),
                "created_at": self._serialize_datetime(created_at),
                "set_at": self._serialize_datetime(created_at),
                "starts_at": self._serialize_datetime(created_at),
                "ends_at": self._serialize_datetime(ends_at),
            }
        return merged

    async def _reload_from_db(self):
        loaded = default_utility_state()
        async with self._pool.acquire() as conn:
            watch_rows = await conn.fetch(
                "SELECT user_id, mention_global, mention_guild_ids, mention_channel_ids, reply_global, reply_guild_ids, "
                "reply_channel_ids, excluded_channel_ids, ignored_user_ids FROM utility_watch_configs"
            )
            keyword_rows = await conn.fetch(
                "SELECT user_id, guild_id, channel_id, phrase, mode, created_at FROM utility_watch_keywords ORDER BY id ASC"
            )
            return_watch_rows = await conn.fetch(
                "SELECT id, watcher_user_id, guild_id, target_type, target_id, created_at, expires_at, created_from FROM utility_return_watches"
            )
            later_rows = await conn.fetch("SELECT user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url, message_created_at, saved_at, author_name, author_id, preview, attachment_labels FROM utility_later_markers")
            reminder_rows = await conn.fetch("SELECT id, user_id, text, delivery, created_at, due_at, guild_id, guild_name, channel_id, channel_name, origin_jump_url, delivery_attempts, last_attempt_at, retry_after FROM utility_reminders")
            bump_config_rows = await conn.fetch(
                "SELECT guild_id, enabled, provider, detection_channel_ids, reminder_channel_id, reminder_role_id, reminder_text, thanks_text, thanks_mode FROM utility_bump_configs"
            )
            bump_cycle_rows = await conn.fetch(
                "SELECT id, guild_id, provider, last_provider_event_at, last_provider_event_kind, last_bump_at, last_bumper_user_id, last_success_message_id, last_success_channel_id, due_at, reminder_sent_at, delivery_attempts, last_delivery_attempt_at, retry_after, last_delivery_error FROM utility_bump_cycles"
            )
            afk_rows = await conn.fetch("SELECT user_id, status, reason, preset, created_at, set_at, starts_at, ends_at, schedule_id, occurrence_at FROM utility_afk")
            afk_setting_rows = await conn.fetch("SELECT user_id, timezone FROM utility_afk_settings")
            afk_schedule_rows = await conn.fetch(
                "SELECT id, user_id, reason, preset, timezone, repeat_rule, weekday_mask, local_hour, local_minute, duration_seconds, created_at, next_start_at "
                "FROM utility_afk_schedules"
            )

        for row in watch_rows:
            loaded["watch"][str(row["user_id"])] = {
                "mention_global": bool(row["mention_global"]),
                "mention_guild_ids": [
                    guild_id
                    for guild_id in decode_postgres_json_array(
                        row["mention_guild_ids"],
                        label="utility_watch_configs.mention_guild_ids",
                    )
                    if isinstance(guild_id, int)
                ],
                "mention_channel_ids": [
                    channel_id
                    for channel_id in decode_postgres_json_array(
                        row["mention_channel_ids"],
                        label="utility_watch_configs.mention_channel_ids",
                    )
                    if isinstance(channel_id, int)
                ],
                "reply_global": bool(row["reply_global"]),
                "reply_guild_ids": [
                    guild_id
                    for guild_id in decode_postgres_json_array(
                        row["reply_guild_ids"],
                        label="utility_watch_configs.reply_guild_ids",
                    )
                    if isinstance(guild_id, int)
                ],
                "reply_channel_ids": [
                    channel_id
                    for channel_id in decode_postgres_json_array(
                        row["reply_channel_ids"],
                        label="utility_watch_configs.reply_channel_ids",
                    )
                    if isinstance(channel_id, int)
                ],
                "excluded_channel_ids": [
                    channel_id
                    for channel_id in decode_postgres_json_array(
                        row["excluded_channel_ids"],
                        label="utility_watch_configs.excluded_channel_ids",
                    )
                    if isinstance(channel_id, int)
                ],
                "ignored_user_ids": [
                    ignored_user_id
                    for ignored_user_id in decode_postgres_json_array(
                        row["ignored_user_ids"],
                        label="utility_watch_configs.ignored_user_ids",
                    )
                    if isinstance(ignored_user_id, int)
                ],
                "keywords": [],
            }
        for row in keyword_rows:
            loaded["watch"].setdefault(str(row["user_id"]), _default_watch_config())["keywords"].append(
                {
                    "phrase": row["phrase"],
                    "mode": row["mode"],
                    "guild_id": row["guild_id"],
                    "channel_id": row["channel_id"],
                    "created_at": self._serialize_datetime(row["created_at"]),
                }
            )
        for row in return_watch_rows:
            loaded["return_watches"][row["id"]] = {
                "id": row["id"],
                "watcher_user_id": row["watcher_user_id"],
                "guild_id": row["guild_id"],
                "target_type": row["target_type"],
                "target_id": row["target_id"],
                "created_at": self._serialize_datetime(row["created_at"]),
                "expires_at": self._serialize_datetime(row["expires_at"]),
                "created_from": row["created_from"],
            }
        for row in later_rows:
            loaded["later"].setdefault(str(row["user_id"]), {})[str(row["channel_id"])] = {
                "user_id": row["user_id"],
                "guild_id": row["guild_id"],
                "guild_name": row["guild_name"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "message_id": row["message_id"],
                "message_jump_url": row["message_jump_url"],
                "message_created_at": self._serialize_datetime(row["message_created_at"]),
                "saved_at": self._serialize_datetime(row["saved_at"]),
                "author_name": row["author_name"],
                "author_id": row["author_id"],
                "preview": row["preview"],
                "attachment_labels": [
                    label
                    for label in decode_postgres_json_array(
                        row["attachment_labels"],
                        label="utility_later_markers.attachment_labels",
                    )
                    if isinstance(label, str) and label.strip()
                ],
            }
        for row in reminder_rows:
            loaded["reminders"][row["id"]] = {
                "id": row["id"],
                "user_id": row["user_id"],
                "text": row["text"],
                "delivery": row["delivery"],
                "created_at": self._serialize_datetime(row["created_at"]),
                "due_at": self._serialize_datetime(row["due_at"]),
                "guild_id": row["guild_id"],
                "guild_name": row["guild_name"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "origin_jump_url": row["origin_jump_url"],
                "delivery_attempts": row["delivery_attempts"] if isinstance(row["delivery_attempts"], int) and row["delivery_attempts"] >= 0 else 0,
                "last_attempt_at": self._serialize_datetime(row["last_attempt_at"]),
                "retry_after": self._serialize_datetime(row["retry_after"]),
            }
        for row in bump_config_rows:
            loaded["bump_configs"][str(row["guild_id"])] = {
                "guild_id": row["guild_id"],
                "enabled": bool(row["enabled"]),
                "provider": row["provider"],
                "detection_channel_ids": [
                    channel_id
                    for channel_id in decode_postgres_json_array(
                        row["detection_channel_ids"],
                        label="utility_bump_configs.detection_channel_ids",
                    )
                    if isinstance(channel_id, int)
                ],
                "reminder_channel_id": row["reminder_channel_id"],
                "reminder_role_id": row["reminder_role_id"],
                "reminder_text": row["reminder_text"],
                "thanks_text": row["thanks_text"],
                "thanks_mode": row["thanks_mode"],
            }
        for row in bump_cycle_rows:
            loaded["bump_cycles"][row["id"]] = {
                "id": row["id"],
                "guild_id": row["guild_id"],
                "provider": row["provider"],
                "last_provider_event_at": self._serialize_datetime(row["last_provider_event_at"]),
                "last_provider_event_kind": row["last_provider_event_kind"],
                "last_bump_at": self._serialize_datetime(row["last_bump_at"]),
                "last_bumper_user_id": row["last_bumper_user_id"],
                "last_success_message_id": row["last_success_message_id"],
                "last_success_channel_id": row["last_success_channel_id"],
                "due_at": self._serialize_datetime(row["due_at"]),
                "reminder_sent_at": self._serialize_datetime(row["reminder_sent_at"]),
                "delivery_attempts": row["delivery_attempts"] if isinstance(row["delivery_attempts"], int) and row["delivery_attempts"] >= 0 else 0,
                "last_delivery_attempt_at": self._serialize_datetime(row["last_delivery_attempt_at"]),
                "retry_after": self._serialize_datetime(row["retry_after"]),
                "last_delivery_error": row["last_delivery_error"],
            }
        for row in afk_rows:
            loaded["afk"][str(row["user_id"])] = {
                "user_id": row["user_id"],
                "status": row["status"],
                "reason": row["reason"],
                "preset": row["preset"],
                "created_at": self._serialize_datetime(row["created_at"]),
                "set_at": self._serialize_datetime(row["set_at"]),
                "starts_at": self._serialize_datetime(row["starts_at"]),
                "ends_at": self._serialize_datetime(row["ends_at"]),
                "schedule_id": row["schedule_id"],
                "occurrence_at": self._serialize_datetime(row["occurrence_at"]),
            }
        for row in afk_setting_rows:
            loaded["afk_settings"][str(row["user_id"])] = {"timezone": row["timezone"]}
        for row in afk_schedule_rows:
            loaded["afk_schedules"][row["id"]] = {
                "id": row["id"],
                "user_id": row["user_id"],
                "reason": row["reason"],
                "preset": row["preset"],
                "timezone": row["timezone"],
                "repeat": row["repeat_rule"],
                "weekday_mask": row["weekday_mask"],
                "local_hour": row["local_hour"],
                "local_minute": row["local_minute"],
                "duration_seconds": row["duration_seconds"],
                "created_at": self._serialize_datetime(row["created_at"]),
                "next_start_at": self._serialize_datetime(row["next_start_at"]),
            }
        self.state = self.normalize_state(loaded)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        changed_sections = [
            section
            for section in ("watch", "return_watches", "later", "reminders", "bump_configs", "bump_cycles", "afk", "afk_settings", "afk_schedules")
            if snapshot.get(section) != self._last_flushed_state.get(section)
        ]
        if not changed_sections:
            return
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if "watch" in changed_sections:
                    await self._replace_watch_data(conn, snapshot.get("watch", {}))
                if "return_watches" in changed_sections:
                    await self._replace_return_watches_data(conn, snapshot.get("return_watches", {}))
                if "later" in changed_sections:
                    await self._replace_later_data(conn, snapshot.get("later", {}))
                if "reminders" in changed_sections:
                    await self._replace_reminders_data(conn, snapshot.get("reminders", {}))
                if "bump_configs" in changed_sections:
                    await self._replace_bump_configs_data(conn, snapshot.get("bump_configs", {}))
                if "bump_cycles" in changed_sections:
                    await self._replace_bump_cycles_data(conn, snapshot.get("bump_cycles", {}))
                if "afk" in changed_sections:
                    await self._replace_afk_data(conn, snapshot.get("afk", {}))
                if "afk_settings" in changed_sections:
                    await self._replace_afk_settings_data(conn, snapshot.get("afk_settings", {}))
                if "afk_schedules" in changed_sections:
                    await self._replace_afk_schedules_data(conn, snapshot.get("afk_schedules", {}))

    async def _replace_all_data(self, conn, state: dict[str, Any]):
        await self._replace_watch_data(conn, state.get("watch", {}))
        await self._replace_return_watches_data(conn, state.get("return_watches", {}))
        await self._replace_later_data(conn, state.get("later", {}))
        await self._replace_reminders_data(conn, state.get("reminders", {}))
        await self._replace_bump_configs_data(conn, state.get("bump_configs", {}))
        await self._replace_bump_cycles_data(conn, state.get("bump_cycles", {}))
        await self._replace_afk_data(conn, state.get("afk", {}))
        await self._replace_afk_settings_data(conn, state.get("afk_settings", {}))
        await self._replace_afk_schedules_data(conn, state.get("afk_schedules", {}))

    async def _replace_watch_data(self, conn, watch_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_watch_keywords")
        await conn.execute("DELETE FROM utility_watch_configs")
        config_rows = []
        keyword_rows = []
        for user_id_text, config in watch_state.items():
            if not isinstance(config, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            config_rows.append(
                (
                    user_id,
                    bool(config.get("mention_global")),
                    json.dumps([guild_id for guild_id in config.get("mention_guild_ids", []) if isinstance(guild_id, int)]),
                    json.dumps([channel_id for channel_id in config.get("mention_channel_ids", []) if isinstance(channel_id, int)]),
                    bool(config.get("reply_global")),
                    json.dumps([guild_id for guild_id in config.get("reply_guild_ids", []) if isinstance(guild_id, int)]),
                    json.dumps([channel_id for channel_id in config.get("reply_channel_ids", []) if isinstance(channel_id, int)]),
                    json.dumps([channel_id for channel_id in config.get("excluded_channel_ids", []) if isinstance(channel_id, int)]),
                    json.dumps([ignored_user_id for ignored_user_id in config.get("ignored_user_ids", []) if isinstance(ignored_user_id, int)]),
                )
            )
            for keyword in config.get("keywords", []):
                if isinstance(keyword, dict):
                    guild_id = keyword.get("guild_id") if isinstance(keyword.get("guild_id"), int) else None
                    channel_id = keyword.get("channel_id") if isinstance(keyword.get("channel_id"), int) and guild_id is not None else None
                    keyword_rows.append((user_id, guild_id, channel_id, keyword.get("phrase"), keyword.get("mode", "contains"), self._parse_iso_datetime(keyword.get("created_at"))))
        if config_rows:
            await conn.executemany(
                "INSERT INTO utility_watch_configs ("
                "user_id, mention_global, mention_guild_ids, mention_channel_ids, reply_global, reply_guild_ids, "
                "reply_channel_ids, excluded_channel_ids, ignored_user_ids, updated_at"
                ") VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, timezone('utc', now()))",
                config_rows,
            )
        if keyword_rows:
            await conn.executemany(
                "INSERT INTO utility_watch_keywords (user_id, guild_id, channel_id, phrase, mode, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
                keyword_rows,
            )

    async def _replace_return_watches_data(self, conn, return_watch_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_return_watches")
        rows = []
        for watch_id, record in return_watch_state.items():
            if not isinstance(record, dict):
                continue
            rows.append(
                (
                    watch_id,
                    record.get("watcher_user_id"),
                    record.get("guild_id"),
                    record.get("target_type"),
                    record.get("target_id"),
                    self._parse_iso_datetime(record.get("created_at")),
                    self._parse_iso_datetime(record.get("expires_at")),
                    record.get("created_from"),
                )
            )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_return_watches (id, watcher_user_id, guild_id, target_type, target_id, created_at, expires_at, created_from) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                rows,
            )

    async def _replace_later_data(self, conn, later_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_later_markers")
        rows = []
        for user_id_text, markers in later_state.items():
            if not isinstance(markers, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            for marker in markers.values():
                if isinstance(marker, dict):
                    rows.append(
                        (
                            user_id,
                            marker.get("guild_id"),
                            marker.get("guild_name"),
                            marker.get("channel_id"),
                            marker.get("channel_name"),
                            marker.get("message_id"),
                            marker.get("message_jump_url"),
                            self._parse_iso_datetime(marker.get("message_created_at")),
                            self._parse_iso_datetime(marker.get("saved_at")),
                            marker.get("author_name"),
                            marker.get("author_id"),
                            marker.get("preview"),
                            json.dumps([label for label in marker.get("attachment_labels", []) if isinstance(label, str) and label.strip()]),
                        )
                    )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_later_markers (user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url, message_created_at, saved_at, author_name, author_id, preview, attachment_labels) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb)",
                rows,
            )

    async def _replace_reminders_data(self, conn, reminders_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_reminders")
        rows = []
        for reminder_id, record in reminders_state.items():
            if isinstance(record, dict):
                rows.append(
                    (
                        reminder_id,
                        record.get("user_id"),
                        record.get("text"),
                        record.get("delivery"),
                        self._parse_iso_datetime(record.get("created_at")),
                        self._parse_iso_datetime(record.get("due_at")),
                        record.get("guild_id"),
                        record.get("guild_name"),
                        record.get("channel_id"),
                        record.get("channel_name"),
                        record.get("origin_jump_url"),
                        record.get("delivery_attempts", 0),
                        self._parse_iso_datetime(record.get("last_attempt_at")),
                        self._parse_iso_datetime(record.get("retry_after")),
                    )
                )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_reminders (id, user_id, text, delivery, created_at, due_at, guild_id, guild_name, channel_id, channel_name, origin_jump_url, delivery_attempts, last_attempt_at, retry_after) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
                rows,
            )

    async def _replace_bump_configs_data(self, conn, bump_configs_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_bump_configs")
        rows = []
        for guild_id_text, record in bump_configs_state.items():
            if not isinstance(record, dict):
                continue
            try:
                guild_id = int(guild_id_text)
            except (TypeError, ValueError):
                continue
            rows.append(
                (
                    guild_id,
                    bool(record.get("enabled")),
                    record.get("provider"),
                    json.dumps([channel_id for channel_id in record.get("detection_channel_ids", []) if isinstance(channel_id, int)]),
                    record.get("reminder_channel_id"),
                    record.get("reminder_role_id"),
                    record.get("reminder_text"),
                    record.get("thanks_text"),
                    record.get("thanks_mode"),
                )
            )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_bump_configs (guild_id, enabled, provider, detection_channel_ids, reminder_channel_id, reminder_role_id, reminder_text, thanks_text, thanks_mode, updated_at) "
                "VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9, timezone('utc', now()))",
                rows,
            )

    async def _replace_bump_cycles_data(self, conn, bump_cycles_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_bump_cycles")
        rows = []
        for cycle_id, record in bump_cycles_state.items():
            if not isinstance(record, dict):
                continue
            rows.append(
                (
                    cycle_id,
                    record.get("guild_id"),
                    record.get("provider"),
                    self._parse_iso_datetime(record.get("last_provider_event_at")),
                    record.get("last_provider_event_kind"),
                    self._parse_iso_datetime(record.get("last_bump_at")),
                    record.get("last_bumper_user_id"),
                    record.get("last_success_message_id"),
                    record.get("last_success_channel_id"),
                    self._parse_iso_datetime(record.get("due_at")),
                    self._parse_iso_datetime(record.get("reminder_sent_at")),
                    record.get("delivery_attempts", 0),
                    self._parse_iso_datetime(record.get("last_delivery_attempt_at")),
                    self._parse_iso_datetime(record.get("retry_after")),
                    record.get("last_delivery_error"),
                )
            )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_bump_cycles (id, guild_id, provider, last_provider_event_at, last_provider_event_kind, last_bump_at, last_bumper_user_id, last_success_message_id, last_success_channel_id, due_at, reminder_sent_at, delivery_attempts, last_delivery_attempt_at, retry_after, last_delivery_error) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                rows,
            )

    async def _replace_afk_data(self, conn, afk_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_afk")
        rows = []
        for user_id_text, record in afk_state.items():
            if not isinstance(record, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            rows.append(
                (
                    user_id,
                    record.get("status", "active"),
                    record.get("reason"),
                    record.get("preset"),
                    self._parse_iso_datetime(record.get("created_at")),
                    self._parse_iso_datetime(record.get("set_at")),
                    self._parse_iso_datetime(record.get("starts_at")),
                    self._parse_iso_datetime(record.get("ends_at")),
                    record.get("schedule_id"),
                    self._parse_iso_datetime(record.get("occurrence_at")),
                )
            )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_afk (user_id, status, reason, preset, created_at, set_at, starts_at, ends_at, schedule_id, occurrence_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                rows,
            )

    async def _replace_afk_settings_data(self, conn, afk_settings_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_afk_settings")
        rows = []
        for user_id_text, record in afk_settings_state.items():
            if not isinstance(record, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            timezone_name = record.get("timezone")
            if not isinstance(timezone_name, str) or not timezone_name.strip():
                continue
            rows.append((user_id, timezone_name.strip()))
        if rows:
            await conn.executemany(
                "INSERT INTO utility_afk_settings (user_id, timezone, updated_at) VALUES ($1, $2, timezone('utc', now()))",
                rows,
            )

    async def _replace_afk_schedules_data(self, conn, afk_schedules_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_afk_schedules")
        rows = []
        for schedule_id, record in afk_schedules_state.items():
            if not isinstance(record, dict):
                continue
            rows.append(
                (
                    schedule_id,
                    record.get("user_id"),
                    record.get("reason"),
                    record.get("preset"),
                    record.get("timezone"),
                    record.get("repeat"),
                    record.get("weekday_mask"),
                    record.get("local_hour"),
                    record.get("local_minute"),
                    record.get("duration_seconds"),
                    self._parse_iso_datetime(record.get("created_at")),
                    self._parse_iso_datetime(record.get("next_start_at")),
                )
            )
        if rows:
            await conn.executemany(
                "INSERT INTO utility_afk_schedules (id, user_id, reason, preset, timezone, repeat_rule, weekday_mask, local_hour, local_minute, duration_seconds, created_at, next_start_at) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                rows,
            )


class UtilityStateStore:
    def __init__(self, legacy_json_path: Path | None = None, *, backend: str | None = None, database_url: str | None = None):
        self.legacy_json_path = _resolve_legacy_json_path(legacy_json_path)
        self.backend_preference = (backend or os.getenv("UTILITY_STORAGE_BACKEND", "postgres")).strip().lower() or "postgres"
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.state: dict[str, Any] = default_utility_state()
        self._memory_store = _MemoryUtilityStore()
        self._store: _BaseUtilityStore = self._build_primary_store()

    @property
    def backend_name(self) -> str:
        return getattr(self._store, "backend_name", "unknown")

    async def load(self) -> dict[str, Any]:
        print(
            "Utility storage init: "
            f"backend_preference={self.backend_preference}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source or 'none'}, "
            f"database_target={self.redacted_database_url()}"
        )
        self._store = self._build_primary_store()
        try:
            self.state = await self._store.load()
        except UtilityStorageUnavailable as exc:
            print(
                "Utility storage init failed: "
                f"backend_preference={self.backend_preference}, "
                f"database_url_configured={'yes' if self.database_url else 'no'}, "
                f"database_url_source={self.database_url_source or 'none'}, "
                f"database_target={self.redacted_database_url()}, "
                f"error={exc}"
            )
            raise
        self._store.state = self.state
        print(f"Utility storage init succeeded: backend={self.backend_name}")
        return self.state

    async def flush(self) -> bool:
        self._store.state = self.state
        return await self._store.flush()

    async def close(self):
        await self._store.close()

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)

    def _build_primary_store(self) -> _BaseUtilityStore:
        if self.backend_preference in {"memory", "test", "dev"}:
            return self._memory_store
        if self.backend_preference not in {"postgres", "postgresql", "supabase", "auto"}:
            raise UtilityStorageUnavailable(f"Unsupported utility storage backend '{self.backend_preference}'.")
        if not self.database_url:
            raise UtilityStorageUnavailable("No Postgres utility database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL.")
        return _PostgresUtilityStore(self.database_url, legacy_json_path=self.legacy_json_path)
