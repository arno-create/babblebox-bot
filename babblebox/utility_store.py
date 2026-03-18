from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_UTILITY_PATH = Path(__file__).resolve().parent.parent / ".cache" / "utility_state.json"


def default_utility_state() -> dict[str, Any]:
    return {
        "version": 1,
        "watch": {},
        "later": {},
        "reminders": {},
        "brb": {},
    }


def _default_watch_config() -> dict[str, Any]:
    return {
        "mention_global": False,
        "mention_guild_ids": [],
        "keywords": [],
    }


def _resolve_utility_path(path: Path | None = None) -> Path:
    if path is not None:
        return path
    configured = os.getenv("UTILITY_JSON_PATH", "").strip()
    if configured:
        return Path(configured)
    return DEFAULT_UTILITY_PATH


def _has_any_saved_state(state: dict[str, Any]) -> bool:
    return any(bool(state.get(section)) for section in ("watch", "later", "reminders", "brb"))


def _state_counts(state: dict[str, Any]) -> dict[str, int]:
    later_count = 0
    for markers in state.get("later", {}).values():
        if isinstance(markers, dict):
            later_count += len(markers)

    return {
        "watch_users": len(state.get("watch", {})),
        "later_markers": later_count,
        "reminders": len(state.get("reminders", {})),
        "brb_users": len(state.get("brb", {})),
    }


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

    def _normalize_state(self, payload: Any) -> dict[str, Any]:
        normalized = default_utility_state()
        if not isinstance(payload, dict):
            return normalized

        version = payload.get("version")
        normalized["version"] = version if isinstance(version, int) and version > 0 else 1

        watch = payload.get("watch")
        if isinstance(watch, dict):
            normalized_watch: dict[str, Any] = {}
            for user_id, config in watch.items():
                if not isinstance(config, dict):
                    continue
                mention_global = bool(config.get("mention_global"))
                mention_guild_ids = sorted(
                    {
                        guild_id
                        for guild_id in config.get("mention_guild_ids", [])
                        if isinstance(guild_id, int) and guild_id > 0
                    }
                )
                keywords = []
                for keyword in config.get("keywords", []):
                    if not isinstance(keyword, dict):
                        continue
                    phrase = keyword.get("phrase")
                    mode = keyword.get("mode", "contains")
                    guild_id = keyword.get("guild_id")
                    created_at = keyword.get("created_at")
                    if not isinstance(phrase, str) or not phrase.strip():
                        continue
                    if mode not in {"contains", "word"}:
                        continue
                    keywords.append(
                        {
                            "phrase": phrase.strip(),
                            "mode": mode,
                            "guild_id": guild_id if isinstance(guild_id, int) else None,
                            "created_at": created_at if isinstance(created_at, str) else None,
                        }
                    )
                if mention_global or mention_guild_ids or keywords:
                    normalized_watch[str(user_id)] = {
                        "mention_global": mention_global,
                        "mention_guild_ids": mention_guild_ids,
                        "keywords": keywords,
                    }
            normalized["watch"] = normalized_watch

        later = payload.get("later")
        if isinstance(later, dict):
            normalized_later: dict[str, Any] = {}
            for user_id, markers in later.items():
                if not isinstance(markers, dict):
                    continue
                normalized_markers = {
                    str(channel_id): marker
                    for channel_id, marker in markers.items()
                    if isinstance(marker, dict)
                }
                if normalized_markers:
                    normalized_later[str(user_id)] = normalized_markers
            normalized["later"] = normalized_later

        reminders = payload.get("reminders")
        if isinstance(reminders, dict):
            normalized["reminders"] = {
                str(reminder_id): record
                for reminder_id, record in reminders.items()
                if isinstance(record, dict)
            }

        brb = payload.get("brb")
        if isinstance(brb, dict):
            normalized["brb"] = {
                str(user_id): record
                for user_id, record in brb.items()
                if isinstance(record, dict)
            }

        return normalized


class _JsonUtilityStore(_BaseUtilityStore):
    backend_name = "json"

    def __init__(self, path: Path | None = None):
        super().__init__()
        self.path = _resolve_utility_path(path)
        self._io_lock = asyncio.Lock()

    async def load(self) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        try:
            raw = await asyncio.to_thread(self.path.read_text, encoding="utf-8")
        except FileNotFoundError:
            self.state = default_utility_state()
            return self.state
        except Exception as exc:
            print(f"Utility JSON store load failed: {exc}")
            self.state = default_utility_state()
            return self.state

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            print(f"Utility JSON store is corrupt, using defaults: {exc}")
            await self._backup_corrupt_store()
            self.state = default_utility_state()
            return self.state

        self.state = self._normalize_state(payload)
        return self.state

    async def flush(self) -> bool:
        snapshot = deepcopy(self.state)
        async with self._io_lock:
            try:
                await asyncio.to_thread(self._write_snapshot, snapshot)
            except Exception as exc:
                print(f"Utility JSON store flush failed: {exc}")
                return False
        return True

    async def create_migration_backup(self, *, label: str) -> Path | None:
        if not self.path.exists():
            return None
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = self.path.with_suffix(f".{label}-{timestamp}.json")
        try:
            await asyncio.to_thread(shutil.copy2, self.path, backup_path)
        except Exception as exc:
            print(f"Failed to back up utility JSON store for migration: {exc}")
            return None
        return backup_path

    async def _backup_corrupt_store(self):
        if not self.path.exists():
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = self.path.with_suffix(f".corrupt-{timestamp}.json")

        try:
            await asyncio.to_thread(shutil.copy2, self.path, backup_path)
        except Exception as exc:
            print(f"Failed to back up corrupt utility JSON store: {exc}")

    def _write_snapshot(self, snapshot: dict[str, Any]):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        payload = json.dumps(snapshot, indent=2, ensure_ascii=True, sort_keys=True)
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(self.path)


class _PostgresUtilityStore(_BaseUtilityStore):
    backend_name = "postgres"

    def __init__(self, dsn: str, *, json_seed_path: Path | None = None):
        super().__init__()
        self.dsn = dsn
        self.json_seed_path = _resolve_utility_path(json_seed_path)
        self._pool = None
        self._asyncpg = None
        self._io_lock = asyncio.Lock()
        self._last_flushed_state = default_utility_state()

    async def load(self) -> dict[str, Any]:
        await self._connect()
        await self._ensure_schema()
        await self._maybe_migrate_json_seed()
        await self._reload_from_db()
        self._last_flushed_state = deepcopy(self.state)
        print("Utility storage backend: postgres")
        return self.state

    async def flush(self) -> bool:
        snapshot = self._normalize_state(deepcopy(self.state))
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
            raise RuntimeError("asyncpg is not installed, so Postgres utility storage is unavailable.") from exc
        self._pool = await self._asyncpg.create_pool(
            dsn=self.dsn,
            min_size=1,
            max_size=3,
            command_timeout=30,
            max_inactive_connection_lifetime=60,
            server_settings={"application_name": "babblebox-utility-store"},
        )

    async def _ensure_schema(self):
        statements = [
            """
            CREATE TABLE IF NOT EXISTS utility_meta (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS utility_watch_configs (
                user_id BIGINT PRIMARY KEY,
                mention_global BOOLEAN NOT NULL DEFAULT FALSE,
                mention_guild_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS utility_watch_keywords (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES utility_watch_configs(user_id) ON DELETE CASCADE,
                guild_id BIGINT NULL,
                phrase TEXT NOT NULL,
                mode TEXT NOT NULL,
                created_at TIMESTAMPTZ NULL
            )
            """,
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_utility_watch_keywords_scope
            ON utility_watch_keywords (user_id, COALESCE(guild_id, 0), mode, phrase)
            """,
            """
            CREATE TABLE IF NOT EXISTS utility_later_markers (
                user_id BIGINT NOT NULL,
                guild_id BIGINT NOT NULL,
                guild_name TEXT NOT NULL,
                channel_id BIGINT NOT NULL,
                channel_name TEXT NOT NULL,
                message_id BIGINT NOT NULL,
                message_jump_url TEXT NOT NULL,
                message_created_at TIMESTAMPTZ NULL,
                saved_at TIMESTAMPTZ NULL,
                author_name TEXT NOT NULL,
                author_id BIGINT NOT NULL,
                preview TEXT NOT NULL,
                PRIMARY KEY (user_id, channel_id)
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_utility_later_markers_user
            ON utility_later_markers (user_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS utility_reminders (
                id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                text TEXT NOT NULL,
                delivery TEXT NOT NULL,
                created_at TIMESTAMPTZ NULL,
                due_at TIMESTAMPTZ NULL,
                guild_id BIGINT NULL,
                guild_name TEXT NULL,
                channel_id BIGINT NULL,
                channel_name TEXT NULL,
                origin_jump_url TEXT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_utility_reminders_due_at
            ON utility_reminders (due_at)
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_utility_reminders_user
            ON utility_reminders (user_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS utility_brb (
                user_id BIGINT PRIMARY KEY,
                reason TEXT NULL,
                created_at TIMESTAMPTZ NULL,
                ends_at TIMESTAMPTZ NULL,
                guild_id BIGINT NULL,
                guild_name TEXT NULL
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS ix_utility_brb_ends_at
            ON utility_brb (ends_at)
            """,
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def _maybe_migrate_json_seed(self):
        if not self.json_seed_path.exists():
            return

        async with self._pool.acquire() as conn:
            migration_row = await conn.fetchrow(
                "SELECT value FROM utility_meta WHERE key = $1",
                "json_seed_migration_v1",
            )
            if migration_row is not None:
                return

        json_store = _JsonUtilityStore(self.json_seed_path)
        seed_state = await json_store.load()
        summary = _state_counts(seed_state)
        if not _has_any_saved_state(seed_state):
            await self._set_meta("json_seed_migration_v1", {"status": "empty"})
            return

        backup_path = await json_store.create_migration_backup(label="seed")
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await self._replace_all_data(conn, seed_state)
                await conn.execute(
                    """
                    INSERT INTO utility_meta (key, value, updated_at)
                    VALUES ($1, $2::jsonb, timezone('utc', now()))
                    ON CONFLICT (key)
                    DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                    """,
                    "json_seed_migration_v1",
                    json.dumps(
                        {
                            "status": "migrated",
                            "source": str(self.json_seed_path),
                            "backup": str(backup_path) if backup_path is not None else None,
                            "summary": summary,
                        }
                    ),
                )
        print(
            "Migrated utility JSON seed into Postgres: "
            f"{summary['watch_users']} watch users, "
            f"{summary['later_markers']} Later markers, "
            f"{summary['reminders']} reminders, "
            f"{summary['brb_users']} BRB records."
        )

    async def _set_meta(self, key: str, value: dict[str, Any]):
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO utility_meta (key, value, updated_at)
                VALUES ($1, $2::jsonb, timezone('utc', now()))
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                """,
                key,
                json.dumps(value),
            )

    async def _reload_from_db(self):
        loaded_state = default_utility_state()
        async with self._pool.acquire() as conn:
            watch_rows = await conn.fetch(
                """
                SELECT user_id, mention_global, mention_guild_ids
                FROM utility_watch_configs
                """
            )
            keyword_rows = await conn.fetch(
                """
                SELECT user_id, guild_id, phrase, mode, created_at
                FROM utility_watch_keywords
                ORDER BY id ASC
                """
            )
            later_rows = await conn.fetch(
                """
                SELECT user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url,
                       message_created_at, saved_at, author_name, author_id, preview
                FROM utility_later_markers
                """
            )
            reminder_rows = await conn.fetch(
                """
                SELECT id, user_id, text, delivery, created_at, due_at, guild_id, guild_name, channel_id,
                       channel_name, origin_jump_url
                FROM utility_reminders
                """
            )
            brb_rows = await conn.fetch(
                """
                SELECT user_id, reason, created_at, ends_at, guild_id, guild_name
                FROM utility_brb
                """
            )

        for row in watch_rows:
            loaded_state["watch"][str(row["user_id"])] = {
                "mention_global": bool(row["mention_global"]),
                "mention_guild_ids": [
                    guild_id
                    for guild_id in (row["mention_guild_ids"] or [])
                    if isinstance(guild_id, int)
                ],
                "keywords": [],
            }

        for row in keyword_rows:
            config = loaded_state["watch"].setdefault(str(row["user_id"]), _default_watch_config())
            config["keywords"].append(
                {
                    "phrase": row["phrase"],
                    "mode": row["mode"],
                    "guild_id": row["guild_id"],
                    "created_at": row["created_at"].astimezone(timezone.utc).isoformat() if row["created_at"] else None,
                }
            )

        for row in later_rows:
            loaded_state["later"].setdefault(str(row["user_id"]), {})[str(row["channel_id"])] = {
                "user_id": row["user_id"],
                "guild_id": row["guild_id"],
                "guild_name": row["guild_name"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "message_id": row["message_id"],
                "message_jump_url": row["message_jump_url"],
                "message_created_at": row["message_created_at"].astimezone(timezone.utc).isoformat()
                if row["message_created_at"]
                else None,
                "saved_at": row["saved_at"].astimezone(timezone.utc).isoformat() if row["saved_at"] else None,
                "author_name": row["author_name"],
                "author_id": row["author_id"],
                "preview": row["preview"],
            }

        for row in reminder_rows:
            loaded_state["reminders"][row["id"]] = {
                "id": row["id"],
                "user_id": row["user_id"],
                "text": row["text"],
                "delivery": row["delivery"],
                "created_at": row["created_at"].astimezone(timezone.utc).isoformat() if row["created_at"] else None,
                "due_at": row["due_at"].astimezone(timezone.utc).isoformat() if row["due_at"] else None,
                "guild_id": row["guild_id"],
                "guild_name": row["guild_name"],
                "channel_id": row["channel_id"],
                "channel_name": row["channel_name"],
                "origin_jump_url": row["origin_jump_url"],
            }

        for row in brb_rows:
            loaded_state["brb"][str(row["user_id"])] = {
                "user_id": row["user_id"],
                "reason": row["reason"],
                "created_at": row["created_at"].astimezone(timezone.utc).isoformat() if row["created_at"] else None,
                "ends_at": row["ends_at"].astimezone(timezone.utc).isoformat() if row["ends_at"] else None,
                "guild_id": row["guild_id"],
                "guild_name": row["guild_name"],
            }

        self.state = self._normalize_state(loaded_state)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        changed_sections = [
            section
            for section in ("watch", "later", "reminders", "brb")
            if snapshot.get(section) != self._last_flushed_state.get(section)
        ]
        if not changed_sections:
            return

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for section in changed_sections:
                    if section == "watch":
                        await self._replace_watch_data(conn, snapshot.get("watch", {}))
                    elif section == "later":
                        await self._replace_later_data(conn, snapshot.get("later", {}))
                    elif section == "reminders":
                        await self._replace_reminders_data(conn, snapshot.get("reminders", {}))
                    elif section == "brb":
                        await self._replace_brb_data(conn, snapshot.get("brb", {}))

    async def _replace_all_data(self, conn, state: dict[str, Any]):
        await self._replace_watch_data(conn, state.get("watch", {}))
        await self._replace_later_data(conn, state.get("later", {}))
        await self._replace_reminders_data(conn, state.get("reminders", {}))
        await self._replace_brb_data(conn, state.get("brb", {}))

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
            mention_guild_ids = [
                guild_id
                for guild_id in config.get("mention_guild_ids", [])
                if isinstance(guild_id, int)
            ]
            config_rows.append(
                (
                    user_id,
                    bool(config.get("mention_global")),
                    json.dumps(mention_guild_ids),
                )
            )
            for keyword in config.get("keywords", []):
                if not isinstance(keyword, dict):
                    continue
                keyword_rows.append(
                    (
                        user_id,
                        keyword.get("guild_id") if isinstance(keyword.get("guild_id"), int) else None,
                        keyword.get("phrase"),
                        keyword.get("mode", "contains"),
                        self._parse_iso_datetime(keyword.get("created_at")),
                    )
                )

        if config_rows:
            await conn.executemany(
                """
                INSERT INTO utility_watch_configs (user_id, mention_global, mention_guild_ids, updated_at)
                VALUES ($1, $2, $3::jsonb, timezone('utc', now()))
                """,
                config_rows,
            )
        if keyword_rows:
            await conn.executemany(
                """
                INSERT INTO utility_watch_keywords (user_id, guild_id, phrase, mode, created_at)
                VALUES ($1, $2, $3, $4, $5)
                """,
                keyword_rows,
            )

    async def _replace_later_data(self, conn, later_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_later_markers")

        marker_rows = []
        for user_id_text, markers in later_state.items():
            if not isinstance(markers, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            for marker in markers.values():
                if not isinstance(marker, dict):
                    continue
                marker_rows.append(
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
                    )
                )

        if marker_rows:
            await conn.executemany(
                """
                INSERT INTO utility_later_markers (
                    user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url,
                    message_created_at, saved_at, author_name, author_id, preview
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                marker_rows,
            )

    async def _replace_reminders_data(self, conn, reminders_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_reminders")

        reminder_rows = []
        for reminder_id, record in reminders_state.items():
            if not isinstance(record, dict):
                continue
            reminder_rows.append(
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
                )
            )

        if reminder_rows:
            await conn.executemany(
                """
                INSERT INTO utility_reminders (
                    id, user_id, text, delivery, created_at, due_at, guild_id, guild_name,
                    channel_id, channel_name, origin_jump_url
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                reminder_rows,
            )

    async def _replace_brb_data(self, conn, brb_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_brb")

        brb_rows = []
        for user_id_text, record in brb_state.items():
            if not isinstance(record, dict):
                continue
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            brb_rows.append(
                (
                    user_id,
                    record.get("reason"),
                    self._parse_iso_datetime(record.get("created_at")),
                    self._parse_iso_datetime(record.get("ends_at")),
                    record.get("guild_id"),
                    record.get("guild_name"),
                )
            )

        if brb_rows:
            await conn.executemany(
                """
                INSERT INTO utility_brb (user_id, reason, created_at, ends_at, guild_id, guild_name)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                brb_rows,
            )

    def _parse_iso_datetime(self, value: Any):
        if not isinstance(value, str) or not value:
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)


class UtilityStateStore:
    def __init__(
        self,
        path: Path | None = None,
        *,
        backend: str | None = None,
        database_url: str | None = None,
    ):
        self.path = _resolve_utility_path(path)
        self.backend_preference = (backend or os.getenv("UTILITY_STORAGE_BACKEND", "auto")).strip().lower() or "auto"
        self.database_url = (
            database_url
            or os.getenv("UTILITY_DATABASE_URL", "").strip()
            or os.getenv("SUPABASE_DB_URL", "").strip()
            or os.getenv("DATABASE_URL", "").strip()
        )
        self.state: dict[str, Any] = default_utility_state()
        self._json_store = _JsonUtilityStore(self.path)
        self._store: _BaseUtilityStore = self._build_primary_store()

    @property
    def backend_name(self) -> str:
        return getattr(self._store, "backend_name", "json")

    async def load(self) -> dict[str, Any]:
        self._store = self._build_primary_store()
        try:
            self.state = await self._store.load()
        except Exception as exc:
            if self._store is not self._json_store:
                print(f"Primary utility store failed ({exc}). Falling back to JSON storage.")
                self._store = self._json_store
                self.state = await self._store.load()
            else:
                raise
        self._store.state = self.state
        if self._store is self._json_store:
            print("Utility storage backend: json")
        return self.state

    async def flush(self) -> bool:
        self._store.state = self.state
        return await self._store.flush()

    async def close(self):
        await self._store.close()

    def _build_primary_store(self) -> _BaseUtilityStore:
        if self.backend_preference in {"json", "file", "local"}:
            return self._json_store

        wants_postgres = self.backend_preference in {"auto", "postgres", "postgresql", "supabase"}
        if wants_postgres and self.database_url:
            return _PostgresUtilityStore(self.database_url, json_seed_path=self.path)

        if self.backend_preference in {"postgres", "postgresql", "supabase"} and not self.database_url:
            print("Postgres utility storage was requested, but no database URL was configured. Falling back to JSON.")
        return self._json_store
