from __future__ import annotations

import importlib
import logging
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit


LOGGER = logging.getLogger(__name__)
DEFAULT_DATABASE_URL_ENV_ORDER = (
    "VOTE_DATABASE_URL",
    "UTILITY_DATABASE_URL",
    "PREMIUM_DATABASE_URL",
    "SUPABASE_DB_URL",
    "DATABASE_URL",
)


class VoteStorageUnavailable(RuntimeError):
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


class _BaseVoteStore:
    backend_name = "unknown"

    async def load(self):
        return None

    async def close(self):
        return None

    async def list_votes(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_vote(self, discord_user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_vote(self, record: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        raise NotImplementedError

    async def finish_webhook_event(
        self,
        event_id: str,
        *,
        status: str,
        error_text: str | None = None,
        processed_at: Any | None = None,
    ):
        raise NotImplementedError


class _MemoryVoteStore(_BaseVoteStore):
    backend_name = "memory"

    def __init__(self):
        self.votes: dict[int, dict[str, Any]] = {}
        self.webhook_events: dict[str, dict[str, Any]] = {}

    async def list_votes(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.votes.values()]

    async def fetch_vote(self, discord_user_id: int) -> dict[str, Any] | None:
        record = self.votes.get(int(discord_user_id))
        return deepcopy(record) if record is not None else None

    async def upsert_vote(self, record: dict[str, Any]) -> dict[str, Any]:
        user_id = int(record["discord_user_id"])
        current = deepcopy(self.votes.get(user_id) or {})
        current.update(deepcopy(record))
        current["discord_user_id"] = user_id
        self.votes[user_id] = current
        return deepcopy(current)

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        event_id = str(record["event_id"])
        if event_id in self.webhook_events:
            return False
        self.webhook_events[event_id] = deepcopy(record)
        return True

    async def finish_webhook_event(
        self,
        event_id: str,
        *,
        status: str,
        error_text: str | None = None,
        processed_at: Any | None = None,
    ):
        current = deepcopy(self.webhook_events.get(str(event_id)) or {})
        if not current:
            return
        current["status"] = str(status or "").strip() or "failed"
        current["error_text"] = str(error_text).strip() if error_text else None
        current["processed_at"] = _serialize_datetime(processed_at) or datetime.now(timezone.utc).isoformat()
        self.webhook_events[str(event_id)] = current


class _PostgresVoteStore(_BaseVoteStore):
    backend_name = "postgres"

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._asyncpg = None
        self._pool = None

    async def load(self):
        await self._ensure_pool()
        await self._ensure_schema()

    async def close(self):
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def list_votes(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT discord_user_id, topgg_vote_id, created_at, expires_at, weight,
                       reminder_opt_in, last_reminder_sent_at, webhook_status,
                       webhook_trace_id, webhook_received_at, webhook_payload_hash, updated_at
                FROM topgg_votes
                ORDER BY discord_user_id
                """
            )
        return [self._row_to_vote(row) for row in rows]

    async def fetch_vote(self, discord_user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT discord_user_id, topgg_vote_id, created_at, expires_at, weight,
                       reminder_opt_in, last_reminder_sent_at, webhook_status,
                       webhook_trace_id, webhook_received_at, webhook_payload_hash, updated_at
                FROM topgg_votes
                WHERE discord_user_id = $1
                """,
                int(discord_user_id),
            )
        return self._row_to_vote(row) if row is not None else None

    async def upsert_vote(self, record: dict[str, Any]) -> dict[str, Any]:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO topgg_votes (
                    discord_user_id,
                    topgg_vote_id,
                    created_at,
                    expires_at,
                    weight,
                    reminder_opt_in,
                    last_reminder_sent_at,
                    webhook_status,
                    webhook_trace_id,
                    webhook_received_at,
                    webhook_payload_hash,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (discord_user_id) DO UPDATE
                SET topgg_vote_id = EXCLUDED.topgg_vote_id,
                    created_at = EXCLUDED.created_at,
                    expires_at = EXCLUDED.expires_at,
                    weight = EXCLUDED.weight,
                    reminder_opt_in = EXCLUDED.reminder_opt_in,
                    last_reminder_sent_at = EXCLUDED.last_reminder_sent_at,
                    webhook_status = EXCLUDED.webhook_status,
                    webhook_trace_id = EXCLUDED.webhook_trace_id,
                    webhook_received_at = EXCLUDED.webhook_received_at,
                    webhook_payload_hash = EXCLUDED.webhook_payload_hash,
                    updated_at = EXCLUDED.updated_at
                RETURNING discord_user_id, topgg_vote_id, created_at, expires_at, weight,
                          reminder_opt_in, last_reminder_sent_at, webhook_status,
                          webhook_trace_id, webhook_received_at, webhook_payload_hash, updated_at
                """,
                int(record["discord_user_id"]),
                record.get("topgg_vote_id"),
                _parse_datetime(record.get("created_at")),
                _parse_datetime(record.get("expires_at")),
                int(record.get("weight", 1) or 1),
                bool(record.get("reminder_opt_in", False)),
                _parse_datetime(record.get("last_reminder_sent_at")),
                record.get("webhook_status"),
                record.get("webhook_trace_id"),
                _parse_datetime(record.get("webhook_received_at")),
                record.get("webhook_payload_hash"),
                _parse_datetime(record.get("updated_at")) or datetime.now(timezone.utc),
            )
        return self._row_to_vote(row)

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO topgg_webhook_events (
                    event_id,
                    discord_user_id,
                    event_type,
                    webhook_mode,
                    received_at,
                    status,
                    error_text,
                    payload_hash,
                    trace_id,
                    signature_timestamp,
                    vote_created_at,
                    vote_expires_at,
                    timing_source,
                    processed_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                str(record["event_id"]),
                int(record["discord_user_id"]) if record.get("discord_user_id") is not None else None,
                str(record.get("event_type") or "").strip() or "unknown",
                str(record.get("webhook_mode") or "").strip() or None,
                _parse_datetime(record.get("received_at")) or datetime.now(timezone.utc),
                str(record.get("status") or "").strip() or "pending",
                record.get("error_text"),
                record.get("payload_hash"),
                record.get("trace_id"),
                int(record["signature_timestamp"]) if record.get("signature_timestamp") is not None else None,
                _parse_datetime(record.get("vote_created_at")),
                _parse_datetime(record.get("vote_expires_at")),
                str(record.get("timing_source") or "").strip() or None,
                _parse_datetime(record.get("processed_at")),
            )
        return row is not None

    async def finish_webhook_event(
        self,
        event_id: str,
        *,
        status: str,
        error_text: str | None = None,
        processed_at: Any | None = None,
    ):
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE topgg_webhook_events
                SET status = $2,
                    error_text = $3,
                    processed_at = $4
                WHERE event_id = $1
                """,
                str(event_id),
                str(status or "").strip() or "failed",
                str(error_text).strip() if error_text else None,
                _parse_datetime(processed_at) or datetime.now(timezone.utc),
            )

    async def _ensure_pool(self):
        if self._pool is not None:
            return
        if not self.database_url:
            raise VoteStorageUnavailable(
                "Vote storage requires a Postgres database URL via VOTE_DATABASE_URL, UTILITY_DATABASE_URL, "
                "PREMIUM_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL."
            )
        try:
            self._asyncpg = importlib.import_module("asyncpg")
        except ModuleNotFoundError as exc:
            raise VoteStorageUnavailable("asyncpg is not installed, so Postgres vote storage is unavailable.") from exc
        try:
            self._pool = await self._asyncpg.create_pool(self.database_url, min_size=1, max_size=3, command_timeout=30)
        except Exception as exc:
            raise VoteStorageUnavailable(f"Vote storage could not connect to Postgres: {exc}") from exc

    async def _ensure_schema(self):
        async with self._pool.acquire() as connection:
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS topgg_votes (
                    discord_user_id BIGINT PRIMARY KEY,
                    topgg_vote_id TEXT,
                    created_at TIMESTAMPTZ,
                    expires_at TIMESTAMPTZ,
                    weight INTEGER NOT NULL DEFAULT 1,
                    reminder_opt_in BOOLEAN NOT NULL DEFAULT FALSE,
                    last_reminder_sent_at TIMESTAMPTZ,
                    webhook_status TEXT,
                    webhook_trace_id TEXT,
                    webhook_received_at TIMESTAMPTZ,
                    webhook_payload_hash TEXT,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            await connection.execute(
                """
                CREATE TABLE IF NOT EXISTS topgg_webhook_events (
                    event_id TEXT PRIMARY KEY,
                    discord_user_id BIGINT,
                    event_type TEXT NOT NULL,
                    webhook_mode TEXT,
                    received_at TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    error_text TEXT,
                    payload_hash TEXT,
                    trace_id TEXT,
                    signature_timestamp BIGINT,
                    vote_created_at TIMESTAMPTZ,
                    vote_expires_at TIMESTAMPTZ,
                    timing_source TEXT,
                    processed_at TIMESTAMPTZ
                )
                """
            )
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS webhook_mode TEXT")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS payload_hash TEXT")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS trace_id TEXT")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS signature_timestamp BIGINT")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS vote_created_at TIMESTAMPTZ")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS vote_expires_at TIMESTAMPTZ")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS timing_source TEXT")
            await connection.execute("ALTER TABLE topgg_webhook_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ")

    def _row_to_vote(self, row: Any) -> dict[str, Any]:
        return {
            "discord_user_id": int(row["discord_user_id"]),
            "topgg_vote_id": row["topgg_vote_id"],
            "created_at": _serialize_datetime(row["created_at"]),
            "expires_at": _serialize_datetime(row["expires_at"]),
            "weight": int(row["weight"] or 1),
            "reminder_opt_in": bool(row["reminder_opt_in"]),
            "last_reminder_sent_at": _serialize_datetime(row["last_reminder_sent_at"]),
            "webhook_status": row["webhook_status"],
            "webhook_trace_id": row["webhook_trace_id"],
            "webhook_received_at": _serialize_datetime(row["webhook_received_at"]),
            "webhook_payload_hash": row["webhook_payload_hash"],
            "updated_at": _serialize_datetime(row["updated_at"]),
        }


class VoteStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        self.backend_preference = (
            backend
            or os.getenv("TOPGG_STORAGE_BACKEND", "").strip()
            or os.getenv("VOTE_STORAGE_BACKEND", "").strip()
            or "postgres"
        ).strip().lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.database_target = _redact_database_url(self.database_url)
        self._memory_store = _MemoryVoteStore()
        self._store: _BaseVoteStore = self._memory_store

    @property
    def backend_name(self) -> str:
        return getattr(self._store, "backend_name", "unknown")

    async def load(self):
        LOGGER.info(
            "Vote storage init: backend_preference=%s database_url_configured=%s database_url_source=%s database_target=%s",
            self.backend_preference,
            bool(self.database_url),
            self.database_url_source,
            self.database_target,
        )
        self._store = self._build_store()
        await self._store.load()
        LOGGER.info("Vote storage init succeeded: backend=%s", self.backend_name)

    async def close(self):
        await self._store.close()

    async def list_votes(self) -> list[dict[str, Any]]:
        return await self._store.list_votes()

    async def fetch_vote(self, discord_user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_vote(discord_user_id)

    async def upsert_vote(self, record: dict[str, Any]) -> dict[str, Any]:
        return await self._store.upsert_vote(record)

    async def record_webhook_event(self, record: dict[str, Any]) -> bool:
        return await self._store.record_webhook_event(record)

    async def finish_webhook_event(
        self,
        event_id: str,
        *,
        status: str,
        error_text: str | None = None,
        processed_at: Any | None = None,
    ):
        await self._store.finish_webhook_event(
            event_id,
            status=status,
            error_text=error_text,
            processed_at=processed_at,
        )

    def _build_store(self) -> _BaseVoteStore:
        if self.backend_preference in {"memory", "test", "dev"}:
            return self._memory_store
        if self.backend_preference not in {"postgres", "postgresql", "supabase", "auto"}:
            raise VoteStorageUnavailable(f"Unsupported vote storage backend '{self.backend_preference}'.")
        return _PostgresVoteStore(self.database_url)
