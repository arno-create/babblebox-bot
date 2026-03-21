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


DEFAULT_LEGACY_JSON_PATH = Path(__file__).resolve().parent.parent / ".cache" / "utility_state.json"


class UtilityStorageUnavailable(RuntimeError):
    pass


def default_utility_state() -> dict[str, Any]:
    return {"version": 3, "watch": {}, "later": {}, "reminders": {}, "afk": {}}


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
        normalized["version"] = version if isinstance(version, int) and version > 0 else 3

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

        for section in ("later", "reminders", "afk"):
            value = payload.get(section)
            if isinstance(value, dict):
                normalized[section] = deepcopy(value)
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
            "CREATE TABLE IF NOT EXISTS utility_later_markers (user_id BIGINT NOT NULL, guild_id BIGINT NOT NULL, guild_name TEXT NOT NULL, channel_id BIGINT NOT NULL, channel_name TEXT NOT NULL, message_id BIGINT NOT NULL, message_jump_url TEXT NOT NULL, message_created_at TIMESTAMPTZ NULL, saved_at TIMESTAMPTZ NULL, author_name TEXT NOT NULL, author_id BIGINT NOT NULL, preview TEXT NOT NULL, PRIMARY KEY (user_id, channel_id))",
            "CREATE INDEX IF NOT EXISTS ix_utility_later_markers_user ON utility_later_markers (user_id)",
            "CREATE TABLE IF NOT EXISTS utility_reminders (id TEXT PRIMARY KEY, user_id BIGINT NOT NULL, text TEXT NOT NULL, delivery TEXT NOT NULL, created_at TIMESTAMPTZ NULL, due_at TIMESTAMPTZ NULL, guild_id BIGINT NULL, guild_name TEXT NULL, channel_id BIGINT NULL, channel_name TEXT NULL, origin_jump_url TEXT NULL)",
            "CREATE INDEX IF NOT EXISTS ix_utility_reminders_due_at ON utility_reminders (due_at)",
            "CREATE INDEX IF NOT EXISTS ix_utility_reminders_user ON utility_reminders (user_id)",
            "CREATE TABLE IF NOT EXISTS utility_afk (user_id BIGINT PRIMARY KEY, status TEXT NOT NULL, reason TEXT NULL, created_at TIMESTAMPTZ NULL, set_at TIMESTAMPTZ NULL, starts_at TIMESTAMPTZ NULL, ends_at TIMESTAMPTZ NULL)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_status ON utility_afk (status)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_starts_at ON utility_afk (starts_at)",
            "CREATE INDEX IF NOT EXISTS ix_utility_afk_ends_at ON utility_afk (ends_at)",
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
            has_rows = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM utility_watch_configs UNION ALL SELECT 1 FROM utility_later_markers UNION ALL SELECT 1 FROM utility_reminders UNION ALL SELECT 1 FROM utility_afk)")
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
            later_rows = await conn.fetch("SELECT user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url, message_created_at, saved_at, author_name, author_id, preview FROM utility_later_markers")
            reminder_rows = await conn.fetch("SELECT id, user_id, text, delivery, created_at, due_at, guild_id, guild_name, channel_id, channel_name, origin_jump_url FROM utility_reminders")
            afk_rows = await conn.fetch("SELECT user_id, status, reason, created_at, set_at, starts_at, ends_at FROM utility_afk")

        for row in watch_rows:
            loaded["watch"][str(row["user_id"])] = {
                "mention_global": bool(row["mention_global"]),
                "mention_guild_ids": [guild_id for guild_id in (row["mention_guild_ids"] or []) if isinstance(guild_id, int)],
                "mention_channel_ids": [channel_id for channel_id in (row["mention_channel_ids"] or []) if isinstance(channel_id, int)],
                "reply_global": bool(row["reply_global"]),
                "reply_guild_ids": [guild_id for guild_id in (row["reply_guild_ids"] or []) if isinstance(guild_id, int)],
                "reply_channel_ids": [channel_id for channel_id in (row["reply_channel_ids"] or []) if isinstance(channel_id, int)],
                "excluded_channel_ids": [channel_id for channel_id in (row["excluded_channel_ids"] or []) if isinstance(channel_id, int)],
                "ignored_user_ids": [ignored_user_id for ignored_user_id in (row["ignored_user_ids"] or []) if isinstance(ignored_user_id, int)],
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
        for row in later_rows:
            loaded["later"].setdefault(str(row["user_id"]), {})[str(row["channel_id"])] = {"user_id": row["user_id"], "guild_id": row["guild_id"], "guild_name": row["guild_name"], "channel_id": row["channel_id"], "channel_name": row["channel_name"], "message_id": row["message_id"], "message_jump_url": row["message_jump_url"], "message_created_at": self._serialize_datetime(row["message_created_at"]), "saved_at": self._serialize_datetime(row["saved_at"]), "author_name": row["author_name"], "author_id": row["author_id"], "preview": row["preview"]}
        for row in reminder_rows:
            loaded["reminders"][row["id"]] = {"id": row["id"], "user_id": row["user_id"], "text": row["text"], "delivery": row["delivery"], "created_at": self._serialize_datetime(row["created_at"]), "due_at": self._serialize_datetime(row["due_at"]), "guild_id": row["guild_id"], "guild_name": row["guild_name"], "channel_id": row["channel_id"], "channel_name": row["channel_name"], "origin_jump_url": row["origin_jump_url"]}
        for row in afk_rows:
            loaded["afk"][str(row["user_id"])] = {"user_id": row["user_id"], "status": row["status"], "reason": row["reason"], "created_at": self._serialize_datetime(row["created_at"]), "set_at": self._serialize_datetime(row["set_at"]), "starts_at": self._serialize_datetime(row["starts_at"]), "ends_at": self._serialize_datetime(row["ends_at"])}
        self.state = self.normalize_state(loaded)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        changed_sections = [section for section in ("watch", "later", "reminders", "afk") if snapshot.get(section) != self._last_flushed_state.get(section)]
        if not changed_sections:
            return
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if "watch" in changed_sections:
                    await self._replace_watch_data(conn, snapshot.get("watch", {}))
                if "later" in changed_sections:
                    await self._replace_later_data(conn, snapshot.get("later", {}))
                if "reminders" in changed_sections:
                    await self._replace_reminders_data(conn, snapshot.get("reminders", {}))
                if "afk" in changed_sections:
                    await self._replace_afk_data(conn, snapshot.get("afk", {}))

    async def _replace_all_data(self, conn, state: dict[str, Any]):
        await self._replace_watch_data(conn, state.get("watch", {}))
        await self._replace_later_data(conn, state.get("later", {}))
        await self._replace_reminders_data(conn, state.get("reminders", {}))
        await self._replace_afk_data(conn, state.get("afk", {}))

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
                    rows.append((user_id, marker.get("guild_id"), marker.get("guild_name"), marker.get("channel_id"), marker.get("channel_name"), marker.get("message_id"), marker.get("message_jump_url"), self._parse_iso_datetime(marker.get("message_created_at")), self._parse_iso_datetime(marker.get("saved_at")), marker.get("author_name"), marker.get("author_id"), marker.get("preview")))
        if rows:
            await conn.executemany("INSERT INTO utility_later_markers (user_id, guild_id, guild_name, channel_id, channel_name, message_id, message_jump_url, message_created_at, saved_at, author_name, author_id, preview) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", rows)

    async def _replace_reminders_data(self, conn, reminders_state: dict[str, Any]):
        await conn.execute("DELETE FROM utility_reminders")
        rows = []
        for reminder_id, record in reminders_state.items():
            if isinstance(record, dict):
                rows.append((reminder_id, record.get("user_id"), record.get("text"), record.get("delivery"), self._parse_iso_datetime(record.get("created_at")), self._parse_iso_datetime(record.get("due_at")), record.get("guild_id"), record.get("guild_name"), record.get("channel_id"), record.get("channel_name"), record.get("origin_jump_url")))
        if rows:
            await conn.executemany("INSERT INTO utility_reminders (id, user_id, text, delivery, created_at, due_at, guild_id, guild_name, channel_id, channel_name, origin_jump_url) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", rows)

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
            rows.append((user_id, record.get("status", "active"), record.get("reason"), self._parse_iso_datetime(record.get("created_at")), self._parse_iso_datetime(record.get("set_at")), self._parse_iso_datetime(record.get("starts_at")), self._parse_iso_datetime(record.get("ends_at"))))
        if rows:
            await conn.executemany("INSERT INTO utility_afk (user_id, status, reason, created_at, set_at, starts_at, ends_at) VALUES ($1, $2, $3, $4, $5, $6, $7)", rows)


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
