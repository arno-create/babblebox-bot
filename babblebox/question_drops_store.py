from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit


DEFAULT_BACKEND = "postgres"
QUESTION_DROP_MAX_DROPS_PER_DAY = 4


class QuestionDropsStorageUnavailable(RuntimeError):
    pass


def default_question_drops_config(guild_id: int | None = None) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "enabled": False,
        "drops_per_day": 2,
        "timezone": "UTC",
        "answer_window_seconds": 60,
        "tone_mode": "clean",
        "activity_gate": "light",
        "active_start_hour": 10,
        "active_end_hour": 22,
        "enabled_channel_ids": [],
        "enabled_categories": [],
    }


def normalize_question_drops_config(guild_id: int, payload: Any) -> dict[str, Any]:
    cleaned = default_question_drops_config(guild_id)
    if not isinstance(payload, dict):
        return cleaned
    cleaned["enabled"] = bool(payload.get("enabled"))
    drops_per_day = payload.get("drops_per_day")
    cleaned["drops_per_day"] = (
        drops_per_day if isinstance(drops_per_day, int) and 1 <= drops_per_day <= QUESTION_DROP_MAX_DROPS_PER_DAY else 2
    )
    timezone_name = payload.get("timezone")
    cleaned["timezone"] = timezone_name.strip() if isinstance(timezone_name, str) and timezone_name.strip() else "UTC"
    answer_window = payload.get("answer_window_seconds")
    cleaned["answer_window_seconds"] = answer_window if isinstance(answer_window, int) and 15 <= answer_window <= 180 else 60
    tone_mode = str(payload.get("tone_mode", "clean")).strip().casefold()
    cleaned["tone_mode"] = tone_mode if tone_mode in {"clean", "playful", "roast-light"} else "clean"
    activity_gate = str(payload.get("activity_gate", "light")).strip().casefold()
    cleaned["activity_gate"] = activity_gate if activity_gate in {"off", "light"} else "light"
    for field, default_value in (("active_start_hour", 10), ("active_end_hour", 22)):
        value = payload.get(field)
        cleaned[field] = value if isinstance(value, int) and 0 <= value <= 23 else default_value
    cleaned["enabled_channel_ids"] = sorted({value for value in payload.get("enabled_channel_ids", []) if isinstance(value, int) and value > 0})
    cleaned["enabled_categories"] = sorted(
        {
            str(value).strip().casefold()
            for value in payload.get("enabled_categories", [])
            if str(value).strip().casefold() in {"science", "history", "geography", "language", "logic", "math", "culture"}
        }
    )
    return cleaned


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
    if value is None:
        return None
    if isinstance(value, str):
        return value
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _normalize_participant_ids(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({int(value) for value in values if isinstance(value, int) and value > 0})


def normalize_active_drop(payload: Any, *, allow_missing_exposure_id: bool = False) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    required_ints = ("guild_id", "channel_id", "message_id", "author_user_id", "difficulty")
    for field in required_ints:
        if not isinstance(payload.get(field), int) or int(payload[field]) <= 0:
            return None
    exposure_id = payload.get("exposure_id")
    if allow_missing_exposure_id:
        if exposure_id is not None and (not isinstance(exposure_id, int) or exposure_id <= 0):
            return None
    elif not isinstance(exposure_id, int) or exposure_id <= 0:
        return None
    asked_at = _serialize_datetime(_parse_datetime(payload.get("asked_at")))
    expires_at = _serialize_datetime(_parse_datetime(payload.get("expires_at")))
    if asked_at is None or expires_at is None:
        return None
    answer_spec = payload.get("answer_spec")
    if not isinstance(answer_spec, dict):
        return None
    string_fields = ("concept_id", "variant_hash", "category", "prompt", "slot_key")
    if not all(isinstance(payload.get(field), str) and payload[field].strip() for field in string_fields):
        return None
    tone_mode = str(payload.get("tone_mode", "clean")).strip().casefold()
    normalized = {
        "guild_id": int(payload["guild_id"]),
        "channel_id": int(payload["channel_id"]),
        "message_id": int(payload["message_id"]),
        "author_user_id": int(payload["author_user_id"]),
        "exposure_id": int(exposure_id) if isinstance(exposure_id, int) and exposure_id > 0 else None,
        "concept_id": payload["concept_id"].strip(),
        "variant_hash": payload["variant_hash"].strip(),
        "category": payload["category"].strip().casefold(),
        "difficulty": int(payload["difficulty"]),
        "prompt": payload["prompt"].strip(),
        "answer_spec": deepcopy(answer_spec),
        "asked_at": asked_at,
        "expires_at": expires_at,
        "slot_key": payload["slot_key"].strip(),
        "tone_mode": tone_mode if tone_mode in {"clean", "playful", "roast-light"} else "clean",
        "participant_user_ids": _normalize_participant_ids(payload.get("participant_user_ids", [])),
    }
    if not allow_missing_exposure_id and normalized["exposure_id"] is None:
        return None
    return normalized


def normalize_exposure(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    required_ints = ("guild_id", "channel_id", "difficulty")
    for field in required_ints:
        if not isinstance(payload.get(field), int) or int(payload[field]) <= 0:
            return None
    asked_at = _serialize_datetime(_parse_datetime(payload.get("asked_at")))
    resolved_at = _serialize_datetime(_parse_datetime(payload.get("resolved_at")))
    if asked_at is None:
        return None
    string_fields = ("concept_id", "variant_hash", "category", "slot_key")
    if not all(isinstance(payload.get(field), str) and payload[field].strip() for field in string_fields):
        return None
    exposure_id = payload.get("id")
    winner_user_id = payload.get("winner_user_id")
    return {
        "id": int(exposure_id) if isinstance(exposure_id, int) and exposure_id > 0 else None,
        "guild_id": int(payload["guild_id"]),
        "channel_id": int(payload["channel_id"]),
        "concept_id": payload["concept_id"].strip(),
        "variant_hash": payload["variant_hash"].strip(),
        "category": payload["category"].strip().casefold(),
        "difficulty": int(payload["difficulty"]),
        "asked_at": asked_at,
        "resolved_at": resolved_at,
        "winner_user_id": int(winner_user_id) if isinstance(winner_user_id, int) and winner_user_id > 0 else None,
        "slot_key": payload["slot_key"].strip(),
    }


def _resolve_database_url(configured: str | None = None) -> tuple[str, str | None]:
    if configured is not None and configured.strip():
        return configured.strip(), "argument"
    for env_name in ("QUESTION_DROPS_DATABASE_URL", "UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL"):
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


class _BaseQuestionDropsStore:
    backend_name = "unknown"

    async def load(self):
        raise NotImplementedError

    async def close(self):
        return None

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        raise NotImplementedError

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_config(self, config: dict[str, Any]):
        raise NotImplementedError

    async def list_active_drops(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_active_drop(self, record: dict[str, Any]):
        raise NotImplementedError

    async def register_posted_drop(self, exposure_record: dict[str, Any], active_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        raise NotImplementedError

    async def update_active_drop_participants(self, guild_id: int, channel_id: int, participant_user_ids: list[int]):
        raise NotImplementedError

    async def delete_active_drop(self, guild_id: int, channel_id: int):
        raise NotImplementedError

    async def insert_exposure(self, record: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def delete_exposure(self, exposure_id: int):
        raise NotImplementedError

    async def resolve_exposure(self, exposure_id: int, *, resolved_at: datetime, winner_user_id: int | None):
        raise NotImplementedError

    async def list_exposures_for_guild(self, guild_id: int, *, limit: int = 400) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def prune_exposures(self, *, before: datetime, limit: int = 500) -> int:
        raise NotImplementedError


class _MemoryQuestionDropsStore(_BaseQuestionDropsStore):
    backend_name = "memory"

    def __init__(self):
        self.configs: dict[int, dict[str, Any]] = {}
        self.active_drops: dict[tuple[int, int], dict[str, Any]] = {}
        self.exposures: dict[int, dict[str, Any]] = {}
        self._next_exposure_id = 1

    async def load(self):
        self.configs = {}
        self.active_drops = {}
        self.exposures = {}
        self._next_exposure_id = 1

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return {guild_id: deepcopy(config) for guild_id, config in self.configs.items()}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        config = self.configs.get(guild_id)
        return deepcopy(config) if config is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_question_drops_config(int(config["guild_id"]), config)
        self.configs[int(config["guild_id"])] = normalized

    async def list_active_drops(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.active_drops.values()]

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        record = self.active_drops.get((guild_id, channel_id))
        return deepcopy(record) if record is not None else None

    async def upsert_active_drop(self, record: dict[str, Any]):
        normalized = normalize_active_drop(record)
        if normalized is not None:
            self.active_drops[(normalized["guild_id"], normalized["channel_id"])] = normalized

    async def register_posted_drop(self, exposure_record: dict[str, Any], active_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        normalized_exposure = normalize_exposure(exposure_record)
        normalized_active = normalize_active_drop(active_record, allow_missing_exposure_id=True)
        if normalized_exposure is None or normalized_active is None:
            raise ValueError("Invalid Question Drops record.")
        if any(
            record["guild_id"] == normalized_exposure["guild_id"] and record["slot_key"] == normalized_exposure["slot_key"]
            for record in self.exposures.values()
        ):
            raise ValueError("Question Drops slot already registered.")
        active_key = (normalized_active["guild_id"], normalized_active["channel_id"])
        if active_key in self.active_drops:
            raise ValueError("Question Drops channel already has an active drop.")
        normalized_exposure["id"] = self._next_exposure_id
        self._next_exposure_id += 1
        self.exposures[int(normalized_exposure["id"])] = deepcopy(normalized_exposure)
        normalized_active["exposure_id"] = int(normalized_exposure["id"])
        self.active_drops[active_key] = deepcopy(normalized_active)
        return deepcopy(normalized_exposure), deepcopy(normalized_active)

    async def update_active_drop_participants(self, guild_id: int, channel_id: int, participant_user_ids: list[int]):
        record = self.active_drops.get((guild_id, channel_id))
        if record is None:
            return
        record["participant_user_ids"] = _normalize_participant_ids(participant_user_ids)

    async def delete_active_drop(self, guild_id: int, channel_id: int):
        self.active_drops.pop((guild_id, channel_id), None)

    async def insert_exposure(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_exposure(record)
        if normalized is None:
            raise ValueError("Invalid Question Drops exposure record.")
        normalized["id"] = self._next_exposure_id
        self._next_exposure_id += 1
        self.exposures[int(normalized["id"])] = normalized
        return deepcopy(normalized)

    async def delete_exposure(self, exposure_id: int):
        self.exposures.pop(exposure_id, None)

    async def resolve_exposure(self, exposure_id: int, *, resolved_at: datetime, winner_user_id: int | None):
        record = self.exposures.get(exposure_id)
        if record is None:
            return
        record["resolved_at"] = _serialize_datetime(resolved_at)
        record["winner_user_id"] = winner_user_id if isinstance(winner_user_id, int) and winner_user_id > 0 else None

    async def list_exposures_for_guild(self, guild_id: int, *, limit: int = 400) -> list[dict[str, Any]]:
        rows = [record for record in self.exposures.values() if record["guild_id"] == guild_id]
        rows.sort(key=lambda item: item["asked_at"], reverse=True)
        return [deepcopy(record) for record in rows[:limit]]

    async def prune_exposures(self, *, before: datetime, limit: int = 500) -> int:
        removed = 0
        before_iso = _serialize_datetime(before)
        for exposure_id, record in list(self.exposures.items()):
            asked_at = record.get("asked_at")
            if not isinstance(asked_at, str) or asked_at >= before_iso:
                continue
            self.exposures.pop(exposure_id, None)
            removed += 1
            if removed >= limit:
                break
        return removed


def _config_from_row(row) -> dict[str, Any]:
    return normalize_question_drops_config(
        int(row["guild_id"]),
        {
            "guild_id": row["guild_id"],
            "enabled": row["enabled"],
            "drops_per_day": row["drops_per_day"],
            "timezone": row["timezone"],
            "answer_window_seconds": row["answer_window_seconds"],
            "tone_mode": row["tone_mode"],
            "activity_gate": row["activity_gate"],
            "active_start_hour": row["active_start_hour"],
            "active_end_hour": row["active_end_hour"],
            "enabled_channel_ids": row["enabled_channel_ids"] or [],
            "enabled_categories": row["enabled_categories"] or [],
        },
    )


def _active_drop_from_row(row) -> dict[str, Any] | None:
    return normalize_active_drop(
        {
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "message_id": row["message_id"],
            "author_user_id": row["author_user_id"],
            "exposure_id": row["exposure_id"],
            "concept_id": row["concept_id"],
            "variant_hash": row["variant_hash"],
            "category": row["category"],
            "difficulty": row["difficulty"],
            "prompt": row["prompt"],
            "answer_spec": dict(row["answer_spec"] or {}),
            "asked_at": _serialize_datetime(row["asked_at"]),
            "expires_at": _serialize_datetime(row["expires_at"]),
            "slot_key": row["slot_key"],
            "tone_mode": row["tone_mode"],
            "participant_user_ids": row["participant_user_ids"] or [],
        }
    )


def _exposure_from_row(row) -> dict[str, Any] | None:
    return normalize_exposure(
        {
            "id": row["id"],
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "concept_id": row["concept_id"],
            "variant_hash": row["variant_hash"],
            "category": row["category"],
            "difficulty": row["difficulty"],
            "asked_at": _serialize_datetime(row["asked_at"]),
            "resolved_at": _serialize_datetime(row["resolved_at"]),
            "winner_user_id": row["winner_user_id"],
            "slot_key": row["slot_key"],
        }
    )


class _PostgresQuestionDropsStore(_BaseQuestionDropsStore):
    backend_name = "postgres"

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._asyncpg = None
        self._pool = None
        self._io_lock = asyncio.Lock()

    async def load(self):
        await self._connect()
        await self._ensure_schema()

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
            raise QuestionDropsStorageUnavailable("asyncpg is not installed, so Question Drops Postgres storage is unavailable.") from exc
        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=3,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-question-drops-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise QuestionDropsStorageUnavailable(f"Could not connect to Postgres Question Drops storage: {last_error}") from last_error

    async def _ensure_schema(self):
        statements = [
            (
                "CREATE TABLE IF NOT EXISTS question_drop_configs ("
                "guild_id BIGINT PRIMARY KEY, "
                "enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "drops_per_day INTEGER NOT NULL DEFAULT 2, "
                "timezone TEXT NOT NULL DEFAULT 'UTC', "
                "answer_window_seconds INTEGER NOT NULL DEFAULT 60, "
                "tone_mode TEXT NOT NULL DEFAULT 'clean', "
                "activity_gate TEXT NOT NULL DEFAULT 'light', "
                "active_start_hour INTEGER NOT NULL DEFAULT 10, "
                "active_end_hour INTEGER NOT NULL DEFAULT 22, "
                "enabled_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "enabled_categories JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS question_drop_exposures ("
                "id BIGSERIAL PRIMARY KEY, "
                "guild_id BIGINT NOT NULL, "
                "channel_id BIGINT NOT NULL, "
                "concept_id TEXT NOT NULL, "
                "variant_hash TEXT NOT NULL, "
                "category TEXT NOT NULL, "
                "difficulty INTEGER NOT NULL, "
                "asked_at TIMESTAMPTZ NOT NULL, "
                "resolved_at TIMESTAMPTZ NULL, "
                "winner_user_id BIGINT NULL, "
                "slot_key TEXT NOT NULL"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_question_drop_exposures_guild_asked ON question_drop_exposures (guild_id, asked_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_question_drop_exposures_guild_concept ON question_drop_exposures (guild_id, concept_id, asked_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_question_drop_exposures_guild_variant ON question_drop_exposures (guild_id, variant_hash, asked_at DESC)",
            (
                "DELETE FROM question_drop_exposures WHERE id IN ("
                "SELECT id FROM ("
                "SELECT id, ROW_NUMBER() OVER (PARTITION BY guild_id, slot_key ORDER BY asked_at DESC, id DESC) AS rn "
                "FROM question_drop_exposures"
                ") dedupe WHERE rn > 1)"
            ),
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_question_drop_exposures_guild_slot ON question_drop_exposures (guild_id, slot_key)",
            (
                "CREATE TABLE IF NOT EXISTS question_drop_active ("
                "guild_id BIGINT NOT NULL, "
                "channel_id BIGINT NOT NULL, "
                "message_id BIGINT NOT NULL, "
                "author_user_id BIGINT NOT NULL, "
                "exposure_id BIGINT NOT NULL REFERENCES question_drop_exposures(id) ON DELETE CASCADE, "
                "concept_id TEXT NOT NULL, "
                "variant_hash TEXT NOT NULL, "
                "category TEXT NOT NULL, "
                "difficulty INTEGER NOT NULL, "
                "prompt TEXT NOT NULL, "
                "answer_spec JSONB NOT NULL, "
                "asked_at TIMESTAMPTZ NOT NULL, "
                "expires_at TIMESTAMPTZ NOT NULL, "
                "slot_key TEXT NOT NULL, "
                "tone_mode TEXT NOT NULL DEFAULT 'clean', "
                "participant_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "PRIMARY KEY (guild_id, channel_id)"
                ")"
            ),
            "ALTER TABLE question_drop_active ADD COLUMN IF NOT EXISTS participant_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "CREATE INDEX IF NOT EXISTS ix_question_drop_active_expires ON question_drop_active (expires_at)",
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, enabled, drops_per_day, timezone, answer_window_seconds, tone_mode, activity_gate, "
                    "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories "
                    "FROM question_drop_configs"
                )
            )
        return {int(row["guild_id"]): _config_from_row(row) for row in rows}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, enabled, drops_per_day, timezone, answer_window_seconds, tone_mode, activity_gate, "
                    "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories "
                    "FROM question_drop_configs WHERE guild_id = $1"
                ),
                guild_id,
            )
        return _config_from_row(row) if row is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_question_drops_config(int(config["guild_id"]), config)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO question_drop_configs ("
                        "guild_id, enabled, drops_per_day, timezone, answer_window_seconds, tone_mode, activity_gate, "
                        "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, timezone('utc', now())"
                        ") ON CONFLICT (guild_id) DO UPDATE SET "
                        "enabled = EXCLUDED.enabled, "
                        "drops_per_day = EXCLUDED.drops_per_day, "
                        "timezone = EXCLUDED.timezone, "
                        "answer_window_seconds = EXCLUDED.answer_window_seconds, "
                        "tone_mode = EXCLUDED.tone_mode, "
                        "activity_gate = EXCLUDED.activity_gate, "
                        "active_start_hour = EXCLUDED.active_start_hour, "
                        "active_end_hour = EXCLUDED.active_end_hour, "
                        "enabled_channel_ids = EXCLUDED.enabled_channel_ids, "
                        "enabled_categories = EXCLUDED.enabled_categories, "
                        "updated_at = timezone('utc', now())"
                    ),
                    normalized["guild_id"],
                    normalized["enabled"],
                    normalized["drops_per_day"],
                    normalized["timezone"],
                    normalized["answer_window_seconds"],
                    normalized["tone_mode"],
                    normalized["activity_gate"],
                    normalized["active_start_hour"],
                    normalized["active_end_hour"],
                    json.dumps(normalized["enabled_channel_ids"]),
                    json.dumps(normalized["enabled_categories"]),
                )

    async def list_active_drops(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, channel_id, message_id, author_user_id, exposure_id, concept_id, variant_hash, category, "
                    "difficulty, prompt, answer_spec, asked_at, expires_at, slot_key, tone_mode, participant_user_ids "
                    "FROM question_drop_active ORDER BY expires_at ASC"
                )
            )
        return [record for row in rows if (record := _active_drop_from_row(row)) is not None]

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, channel_id, message_id, author_user_id, exposure_id, concept_id, variant_hash, category, "
                    "difficulty, prompt, answer_spec, asked_at, expires_at, slot_key, tone_mode, participant_user_ids "
                    "FROM question_drop_active WHERE guild_id = $1 AND channel_id = $2"
                ),
                guild_id,
                channel_id,
            )
        return _active_drop_from_row(row) if row is not None else None

    async def upsert_active_drop(self, record: dict[str, Any]):
        normalized = normalize_active_drop(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await self._upsert_active_drop_with_conn(conn, normalized)

    async def _upsert_active_drop_with_conn(self, conn, normalized: dict[str, Any]):
        await conn.execute(
            (
                "INSERT INTO question_drop_active ("
                "guild_id, channel_id, message_id, author_user_id, exposure_id, concept_id, variant_hash, category, difficulty, "
                "prompt, answer_spec, asked_at, expires_at, slot_key, tone_mode, participant_user_ids"
                ") VALUES ("
                "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15, $16::jsonb"
                ") ON CONFLICT (guild_id, channel_id) DO UPDATE SET "
                "message_id = EXCLUDED.message_id, "
                "author_user_id = EXCLUDED.author_user_id, "
                "exposure_id = EXCLUDED.exposure_id, "
                "concept_id = EXCLUDED.concept_id, "
                "variant_hash = EXCLUDED.variant_hash, "
                "category = EXCLUDED.category, "
                "difficulty = EXCLUDED.difficulty, "
                "prompt = EXCLUDED.prompt, "
                "answer_spec = EXCLUDED.answer_spec, "
                "asked_at = EXCLUDED.asked_at, "
                "expires_at = EXCLUDED.expires_at, "
                "slot_key = EXCLUDED.slot_key, "
                "tone_mode = EXCLUDED.tone_mode, "
                "participant_user_ids = EXCLUDED.participant_user_ids"
            ),
            normalized["guild_id"],
            normalized["channel_id"],
            normalized["message_id"],
            normalized["author_user_id"],
            normalized["exposure_id"],
            normalized["concept_id"],
            normalized["variant_hash"],
            normalized["category"],
            normalized["difficulty"],
            normalized["prompt"],
            json.dumps(normalized["answer_spec"]),
            _parse_datetime(normalized["asked_at"]),
            _parse_datetime(normalized["expires_at"]),
            normalized["slot_key"],
            normalized["tone_mode"],
            json.dumps(normalized["participant_user_ids"]),
        )

    async def register_posted_drop(self, exposure_record: dict[str, Any], active_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        normalized_exposure = normalize_exposure(exposure_record)
        normalized_active = normalize_active_drop(active_record, allow_missing_exposure_id=True)
        if normalized_exposure is None or normalized_active is None:
            raise ValueError("Invalid Question Drops record.")
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    row = await conn.fetchrow(
                        (
                            "INSERT INTO question_drop_exposures ("
                            "guild_id, channel_id, concept_id, variant_hash, category, difficulty, asked_at, resolved_at, winner_user_id, slot_key"
                            ") VALUES ("
                            "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10"
                            ") ON CONFLICT (guild_id, slot_key) DO NOTHING RETURNING id"
                        ),
                        normalized_exposure["guild_id"],
                        normalized_exposure["channel_id"],
                        normalized_exposure["concept_id"],
                        normalized_exposure["variant_hash"],
                        normalized_exposure["category"],
                        normalized_exposure["difficulty"],
                        _parse_datetime(normalized_exposure["asked_at"]),
                        _parse_datetime(normalized_exposure["resolved_at"]),
                        normalized_exposure["winner_user_id"],
                        normalized_exposure["slot_key"],
                    )
                    if row is None:
                        raise ValueError("Question Drops slot already registered.")
                    normalized_exposure["id"] = int(row["id"])
                    normalized_active["exposure_id"] = int(row["id"])
                    if await conn.fetchval(
                        "SELECT 1 FROM question_drop_active WHERE guild_id = $1 AND channel_id = $2",
                        normalized_active["guild_id"],
                        normalized_active["channel_id"],
                    ):
                        raise ValueError("Question Drops channel already has an active drop.")
                    await self._upsert_active_drop_with_conn(conn, normalize_active_drop(normalized_active))
        return normalized_exposure, normalize_active_drop(normalized_active)

    async def update_active_drop_participants(self, guild_id: int, channel_id: int, participant_user_ids: list[int]):
        normalized_ids = _normalize_participant_ids(participant_user_ids)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE question_drop_active SET participant_user_ids = $3::jsonb WHERE guild_id = $1 AND channel_id = $2",
                    guild_id,
                    channel_id,
                    json.dumps(normalized_ids),
                )

    async def delete_active_drop(self, guild_id: int, channel_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM question_drop_active WHERE guild_id = $1 AND channel_id = $2", guild_id, channel_id)

    async def insert_exposure(self, record: dict[str, Any]) -> dict[str, Any]:
        normalized = normalize_exposure(record)
        if normalized is None:
            raise ValueError("Invalid Question Drops exposure record.")
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    (
                        "INSERT INTO question_drop_exposures ("
                        "guild_id, channel_id, concept_id, variant_hash, category, difficulty, asked_at, resolved_at, winner_user_id, slot_key"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10"
                        ") RETURNING id"
                    ),
                    normalized["guild_id"],
                    normalized["channel_id"],
                    normalized["concept_id"],
                    normalized["variant_hash"],
                    normalized["category"],
                    normalized["difficulty"],
                    _parse_datetime(normalized["asked_at"]),
                    _parse_datetime(normalized["resolved_at"]),
                    normalized["winner_user_id"],
                    normalized["slot_key"],
                )
        normalized["id"] = int(row["id"])
        return normalized

    async def delete_exposure(self, exposure_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM question_drop_exposures WHERE id = $1", exposure_id)

    async def resolve_exposure(self, exposure_id: int, *, resolved_at: datetime, winner_user_id: int | None):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE question_drop_exposures SET resolved_at = $2, winner_user_id = $3 WHERE id = $1",
                    exposure_id,
                    resolved_at,
                    winner_user_id if isinstance(winner_user_id, int) and winner_user_id > 0 else None,
                )

    async def list_exposures_for_guild(self, guild_id: int, *, limit: int = 400) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT id, guild_id, channel_id, concept_id, variant_hash, category, difficulty, asked_at, "
                    "resolved_at, winner_user_id, slot_key "
                    "FROM question_drop_exposures WHERE guild_id = $1 ORDER BY asked_at DESC LIMIT $2"
                ),
                guild_id,
                limit,
            )
        return [record for row in rows if (record := _exposure_from_row(row)) is not None]

    async def prune_exposures(self, *, before: datetime, limit: int = 500) -> int:
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT id FROM question_drop_exposures WHERE asked_at < $1 ORDER BY asked_at ASC LIMIT $2",
                    before,
                    limit,
                )
                if not rows:
                    return 0
                exposure_ids = [int(row["id"]) for row in rows]
                await conn.execute("DELETE FROM question_drop_exposures WHERE id = ANY($1::bigint[])", exposure_ids)
        return len(exposure_ids)


class QuestionDropsStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        requested_backend = (backend or os.getenv("QUESTION_DROPS_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.backend_name = requested_backend
        self._store: _BaseQuestionDropsStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        print(
            "Question Drops storage init: "
            f"backend_preference={requested_backend}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source or 'none'}, "
            f"database_target={_redact_database_url(self.database_url)}"
        )
        if requested_backend in {"memory", "test", "dev"}:
            self._store = _MemoryQuestionDropsStore()
        elif requested_backend in {"postgres", "postgresql", "supabase", "auto"}:
            if not self.database_url:
                raise QuestionDropsStorageUnavailable(
                    "No Postgres Question Drops database URL is configured. Set QUESTION_DROPS_DATABASE_URL, UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL."
                )
            self._store = _PostgresQuestionDropsStore(self.database_url)
        else:
            raise QuestionDropsStorageUnavailable(f"Unsupported Question Drops storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        print(f"Question Drops storage init succeeded: backend={self.backend_name}")

    async def load(self):
        if self._store is None:
            raise QuestionDropsStorageUnavailable("Question Drops storage was not initialized.")
        await self._store.load()

    async def close(self):
        if self._store is not None:
            await self._store.close()

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return await self._store.fetch_all_configs()

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_config(guild_id)

    async def upsert_config(self, config: dict[str, Any]):
        await self._store.upsert_config(config)

    async def list_active_drops(self) -> list[dict[str, Any]]:
        return await self._store.list_active_drops()

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_active_drop(guild_id, channel_id)

    async def upsert_active_drop(self, record: dict[str, Any]):
        await self._store.upsert_active_drop(record)

    async def register_posted_drop(self, exposure_record: dict[str, Any], active_record: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        return await self._store.register_posted_drop(exposure_record, active_record)

    async def update_active_drop_participants(self, guild_id: int, channel_id: int, participant_user_ids: list[int]):
        await self._store.update_active_drop_participants(guild_id, channel_id, participant_user_ids)

    async def delete_active_drop(self, guild_id: int, channel_id: int):
        await self._store.delete_active_drop(guild_id, channel_id)

    async def insert_exposure(self, record: dict[str, Any]) -> dict[str, Any]:
        return await self._store.insert_exposure(record)

    async def delete_exposure(self, exposure_id: int):
        await self._store.delete_exposure(exposure_id)

    async def resolve_exposure(self, exposure_id: int, *, resolved_at: datetime, winner_user_id: int | None):
        await self._store.resolve_exposure(exposure_id, resolved_at=resolved_at, winner_user_id=winner_user_id)

    async def list_exposures_for_guild(self, guild_id: int, *, limit: int = 400) -> list[dict[str, Any]]:
        return await self._store.list_exposures_for_guild(guild_id, limit=limit)

    async def prune_exposures(self, *, before: datetime, limit: int = 500) -> int:
        return await self._store.prune_exposures(before=before, limit=limit)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
