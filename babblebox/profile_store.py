from __future__ import annotations

import asyncio
import copy
import importlib
import json
import os
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit


PROFILE_COLUMNS = (
    "user_id",
    "buddy_species",
    "buddy_name",
    "buddy_style",
    "selected_title",
    "featured_badge",
    "buddy_mood",
    "xp_total",
    "last_interaction_at",
    "last_daily_clear_date",
    "current_daily_streak",
    "best_daily_streak",
    "total_daily_participations",
    "total_daily_clears",
    "watch_actions",
    "later_saves",
    "capture_uses",
    "reminders_created",
    "afk_sessions",
    "games_played",
    "games_hosted",
    "games_won",
    "telephone_rounds",
    "telephone_completions",
    "corpse_rounds",
    "corpse_masterpieces",
    "spyfall_rounds",
    "spyfall_wins",
    "bomb_rounds",
    "bomb_wins",
    "xp_window_date",
    "daily_xp_actions",
    "utility_xp_actions",
    "game_xp_actions",
)

DAILY_RESULT_COLUMNS = (
    "challenge_id",
    "puzzle_date",
    "user_id",
    "attempt_count",
    "solved",
    "first_attempt_at",
    "completed_at",
    "solve_seconds",
)


class ProfileStorageUnavailable(RuntimeError):
    pass


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


def default_profile_store_state() -> dict[str, Any]:
    return {
        "profiles": {},
        "daily_results": {},
        "meta": {},
    }


def _copy_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    return copy.deepcopy(payload)


class _BaseProfileStore:
    backend_name = "unknown"

    async def load(self):
        raise NotImplementedError

    async def close(self):
        return None

    async def fetch_profile(self, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def save_profile(self, profile: dict[str, Any]):
        raise NotImplementedError

    async def fetch_daily_result(self, *, challenge_id: str, puzzle_date: date, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def save_daily_result(self, result: dict[str, Any]):
        raise NotImplementedError

    async def fetch_recent_daily_results(self, *, user_id: int, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_daily_leaderboard(self, *, metric: str, today: date, limit: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def set_meta(self, key: str, value: dict[str, Any]):
        raise NotImplementedError

    async def prune_daily_results(self, *, challenge_id: str, keep_after: date) -> int:
        raise NotImplementedError


class _MemoryProfileStore(_BaseProfileStore):
    backend_name = "memory"

    def __init__(self):
        self.state = default_profile_store_state()

    async def load(self):
        self.state = default_profile_store_state()

    async def fetch_profile(self, user_id: int) -> dict[str, Any] | None:
        return _copy_payload(self.state["profiles"].get(user_id))

    async def save_profile(self, profile: dict[str, Any]):
        self.state["profiles"][profile["user_id"]] = _copy_payload(profile)

    async def fetch_daily_result(self, *, challenge_id: str, puzzle_date: date, user_id: int) -> dict[str, Any] | None:
        return _copy_payload(self.state["daily_results"].get((challenge_id, puzzle_date, user_id)))

    async def save_daily_result(self, result: dict[str, Any]):
        key = (result["challenge_id"], result["puzzle_date"], result["user_id"])
        self.state["daily_results"][key] = _copy_payload(result)

    async def fetch_recent_daily_results(self, *, user_id: int, limit: int) -> list[dict[str, Any]]:
        rows = [
            _copy_payload(row)
            for (challenge_id, _, row_user_id), row in self.state["daily_results"].items()
            if row_user_id == user_id and challenge_id == row["challenge_id"]
        ]
        rows.sort(key=lambda item: (item["puzzle_date"], item.get("completed_at") or item.get("first_attempt_at") or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        return rows[:limit]

    async def fetch_daily_leaderboard(self, *, metric: str, today: date, limit: int) -> list[dict[str, Any]]:
        rows = []
        for profile in self.state["profiles"].values():
            last_clear = profile.get("last_daily_clear_date")
            active_streak = 0
            if isinstance(last_clear, date) and (today - last_clear).days <= 1:
                active_streak = int(profile.get("current_daily_streak", 0) or 0)
            rows.append(
                {
                    "user_id": profile["user_id"],
                    "total_daily_clears": int(profile.get("total_daily_clears", 0) or 0),
                    "best_daily_streak": int(profile.get("best_daily_streak", 0) or 0),
                    "active_streak": active_streak,
                    "xp_total": int(profile.get("xp_total", 0) or 0),
                }
            )
        if metric == "streak":
            rows = [row for row in rows if row["active_streak"] > 0 or row["best_daily_streak"] > 0]
            rows.sort(key=lambda item: (item["active_streak"], item["best_daily_streak"], item["total_daily_clears"], item["xp_total"]), reverse=True)
        else:
            rows = [row for row in rows if row["total_daily_clears"] > 0]
            rows.sort(key=lambda item: (item["total_daily_clears"], item["best_daily_streak"], item["active_streak"], item["xp_total"]), reverse=True)
        return rows[:limit]

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        return _copy_payload(self.state["meta"].get(key))

    async def set_meta(self, key: str, value: dict[str, Any]):
        self.state["meta"][key] = _copy_payload(value)

    async def prune_daily_results(self, *, challenge_id: str, keep_after: date) -> int:
        keys_to_remove = [
            key
            for key, row in self.state["daily_results"].items()
            if key[0] == challenge_id and row["puzzle_date"] < keep_after
        ]
        for key in keys_to_remove:
            self.state["daily_results"].pop(key, None)
        return len(keys_to_remove)


class _PostgresProfileStore(_BaseProfileStore):
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
            raise ProfileStorageUnavailable("asyncpg is not installed, so Babblebox profile storage is unavailable.") from exc

        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=2,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-profile-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise ProfileStorageUnavailable(f"Could not connect to Babblebox profile storage: {last_error}") from last_error

    async def _ensure_schema(self):
        statements = [
            "CREATE TABLE IF NOT EXISTS bb_identity_meta (key TEXT PRIMARY KEY, value JSONB NOT NULL DEFAULT '{}'::jsonb, updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            (
                "CREATE TABLE IF NOT EXISTS bb_user_profiles ("
                "user_id BIGINT PRIMARY KEY, "
                "buddy_species TEXT NOT NULL, "
                "buddy_name TEXT NOT NULL, "
                "buddy_style TEXT NOT NULL, "
                "selected_title TEXT NULL, "
                "featured_badge TEXT NULL, "
                "buddy_mood TEXT NOT NULL DEFAULT 'curious', "
                "xp_total INTEGER NOT NULL DEFAULT 0, "
                "last_interaction_at TIMESTAMPTZ NULL, "
                "last_daily_clear_date DATE NULL, "
                "current_daily_streak INTEGER NOT NULL DEFAULT 0, "
                "best_daily_streak INTEGER NOT NULL DEFAULT 0, "
                "total_daily_participations INTEGER NOT NULL DEFAULT 0, "
                "total_daily_clears INTEGER NOT NULL DEFAULT 0, "
                "watch_actions INTEGER NOT NULL DEFAULT 0, "
                "later_saves INTEGER NOT NULL DEFAULT 0, "
                "capture_uses INTEGER NOT NULL DEFAULT 0, "
                "reminders_created INTEGER NOT NULL DEFAULT 0, "
                "afk_sessions INTEGER NOT NULL DEFAULT 0, "
                "games_played INTEGER NOT NULL DEFAULT 0, "
                "games_hosted INTEGER NOT NULL DEFAULT 0, "
                "games_won INTEGER NOT NULL DEFAULT 0, "
                "telephone_rounds INTEGER NOT NULL DEFAULT 0, "
                "telephone_completions INTEGER NOT NULL DEFAULT 0, "
                "corpse_rounds INTEGER NOT NULL DEFAULT 0, "
                "corpse_masterpieces INTEGER NOT NULL DEFAULT 0, "
                "spyfall_rounds INTEGER NOT NULL DEFAULT 0, "
                "spyfall_wins INTEGER NOT NULL DEFAULT 0, "
                "bomb_rounds INTEGER NOT NULL DEFAULT 0, "
                "bomb_wins INTEGER NOT NULL DEFAULT 0, "
                "xp_window_date DATE NULL, "
                "daily_xp_actions SMALLINT NOT NULL DEFAULT 0, "
                "utility_xp_actions SMALLINT NOT NULL DEFAULT 0, "
                "game_xp_actions SMALLINT NOT NULL DEFAULT 0, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()), "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_bb_profiles_total_daily_clears ON bb_user_profiles (total_daily_clears DESC)",
            "CREATE INDEX IF NOT EXISTS ix_bb_profiles_best_daily_streak ON bb_user_profiles (best_daily_streak DESC)",
            "CREATE INDEX IF NOT EXISTS ix_bb_profiles_xp_total ON bb_user_profiles (xp_total DESC)",
            (
                "CREATE TABLE IF NOT EXISTS bb_daily_results ("
                "challenge_id TEXT NOT NULL, "
                "puzzle_date DATE NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "attempt_count SMALLINT NOT NULL DEFAULT 0, "
                "solved BOOLEAN NOT NULL DEFAULT FALSE, "
                "first_attempt_at TIMESTAMPTZ NULL, "
                "completed_at TIMESTAMPTZ NULL, "
                "solve_seconds INTEGER NULL, "
                "PRIMARY KEY (challenge_id, puzzle_date, user_id)"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_bb_daily_results_user_date ON bb_daily_results (user_id, puzzle_date DESC)",
            "CREATE INDEX IF NOT EXISTS ix_bb_daily_results_date ON bb_daily_results (puzzle_date DESC)",
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    def _row_to_dict(self, row) -> dict[str, Any] | None:
        if row is None:
            return None
        return dict(row)

    async def fetch_profile(self, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM bb_user_profiles WHERE user_id = $1", user_id)
        return self._row_to_dict(row)

    async def save_profile(self, profile: dict[str, Any]):
        columns_sql = ", ".join(PROFILE_COLUMNS)
        placeholders_sql = ", ".join(f"${index}" for index in range(1, len(PROFILE_COLUMNS) + 1))
        updates_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in PROFILE_COLUMNS if column != "user_id")
        values = [profile.get(column) for column in PROFILE_COLUMNS]
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO bb_user_profiles ({columns_sql}) VALUES ({placeholders_sql}) "
                    f"ON CONFLICT (user_id) DO UPDATE SET {updates_sql}, updated_at = timezone('utc', now())",
                    *values,
                )

    async def fetch_daily_result(self, *, challenge_id: str, puzzle_date: date, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM bb_daily_results WHERE challenge_id = $1 AND puzzle_date = $2 AND user_id = $3",
                challenge_id,
                puzzle_date,
                user_id,
            )
        return self._row_to_dict(row)

    async def save_daily_result(self, result: dict[str, Any]):
        columns_sql = ", ".join(DAILY_RESULT_COLUMNS)
        placeholders_sql = ", ".join(f"${index}" for index in range(1, len(DAILY_RESULT_COLUMNS) + 1))
        updates_sql = ", ".join(f"{column} = EXCLUDED.{column}" for column in DAILY_RESULT_COLUMNS if column not in {"challenge_id", "puzzle_date", "user_id"})
        values = [result.get(column) for column in DAILY_RESULT_COLUMNS]
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO bb_daily_results ({columns_sql}) VALUES ({placeholders_sql}) "
                    f"ON CONFLICT (challenge_id, puzzle_date, user_id) DO UPDATE SET {updates_sql}",
                    *values,
                )

    async def fetch_recent_daily_results(self, *, user_id: int, limit: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM bb_daily_results WHERE user_id = $1 ORDER BY puzzle_date DESC, completed_at DESC NULLS LAST LIMIT $2",
                user_id,
                limit,
            )
        return [dict(row) for row in rows]

    async def fetch_daily_leaderboard(self, *, metric: str, today: date, limit: int) -> list[dict[str, Any]]:
        active_streak_sql = "CASE WHEN last_daily_clear_date IS NOT NULL AND last_daily_clear_date >= ($1::date - 1) THEN current_daily_streak ELSE 0 END"
        if metric == "streak":
            query = (
                "SELECT user_id, total_daily_clears, best_daily_streak, xp_total, "
                f"{active_streak_sql} AS active_streak "
                "FROM bb_user_profiles "
                "WHERE best_daily_streak > 0 OR total_daily_clears > 0 "
                "ORDER BY active_streak DESC, best_daily_streak DESC, total_daily_clears DESC, xp_total DESC "
                "LIMIT $2"
            )
        else:
            query = (
                "SELECT user_id, total_daily_clears, best_daily_streak, xp_total, "
                f"{active_streak_sql} AS active_streak "
                "FROM bb_user_profiles "
                "WHERE total_daily_clears > 0 "
                "ORDER BY total_daily_clears DESC, best_daily_streak DESC, active_streak DESC, xp_total DESC "
                "LIMIT $2"
            )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, today, limit)
        return [dict(row) for row in rows]

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            value = await conn.fetchval("SELECT value FROM bb_identity_meta WHERE key = $1", key)
        if value is None:
            return None
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return None
        if isinstance(value, dict):
            return value
        return None

    async def set_meta(self, key: str, value: dict[str, Any]):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO bb_identity_meta (key, value, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) "
                    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                    key,
                    json.dumps(value),
                )

    async def prune_daily_results(self, *, challenge_id: str, keep_after: date) -> int:
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM bb_daily_results WHERE challenge_id = $1 AND puzzle_date < $2",
                    challenge_id,
                    keep_after,
                )
        try:
            return int(str(result).split()[-1])
        except (ValueError, IndexError):
            return 0


class ProfileStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        configured_backend = backend or os.getenv("PROFILE_STORAGE_BACKEND", "").strip() or os.getenv("UTILITY_STORAGE_BACKEND", "postgres")
        self.backend_preference = configured_backend.strip().lower() or "postgres"
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self._memory_store = _MemoryProfileStore()
        self._store: _BaseProfileStore = self._build_primary_store()

    @property
    def backend_name(self) -> str:
        return getattr(self._store, "backend_name", "unknown")

    async def load(self):
        print(
            "Profile storage init: "
            f"backend_preference={self.backend_preference}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source or 'none'}, "
            f"database_target={self.redacted_database_url()}"
        )
        self._store = self._build_primary_store()
        try:
            await self._store.load()
        except ProfileStorageUnavailable as exc:
            print(
                "Profile storage init failed: "
                f"backend_preference={self.backend_preference}, "
                f"database_url_configured={'yes' if self.database_url else 'no'}, "
                f"database_url_source={self.database_url_source or 'none'}, "
                f"database_target={self.redacted_database_url()}, "
                f"error={exc}"
            )
            raise
        print(f"Profile storage init succeeded: backend={self.backend_name}")

    async def close(self):
        await self._store.close()

    async def fetch_profile(self, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_profile(user_id)

    async def save_profile(self, profile: dict[str, Any]):
        await self._store.save_profile(profile)

    async def fetch_daily_result(self, *, challenge_id: str, puzzle_date: date, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_daily_result(challenge_id=challenge_id, puzzle_date=puzzle_date, user_id=user_id)

    async def save_daily_result(self, result: dict[str, Any]):
        await self._store.save_daily_result(result)

    async def fetch_recent_daily_results(self, *, user_id: int, limit: int) -> list[dict[str, Any]]:
        return await self._store.fetch_recent_daily_results(user_id=user_id, limit=limit)

    async def fetch_daily_leaderboard(self, *, metric: str, today: date, limit: int) -> list[dict[str, Any]]:
        return await self._store.fetch_daily_leaderboard(metric=metric, today=today, limit=limit)

    async def get_meta(self, key: str) -> dict[str, Any] | None:
        return await self._store.get_meta(key)

    async def set_meta(self, key: str, value: dict[str, Any]):
        await self._store.set_meta(key, value)

    async def prune_daily_results(self, *, challenge_id: str, keep_after: date) -> int:
        return await self._store.prune_daily_results(challenge_id=challenge_id, keep_after=keep_after)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)

    def _build_primary_store(self) -> _BaseProfileStore:
        if self.backend_preference in {"memory", "test", "dev"}:
            return self._memory_store
        if self.backend_preference not in {"postgres", "postgresql", "supabase", "auto"}:
            raise ProfileStorageUnavailable(f"Unsupported Babblebox profile backend '{self.backend_preference}'.")
        if not self.database_url:
            raise ProfileStorageUnavailable("No Postgres database URL is configured for Babblebox profiles.")
        return _PostgresProfileStore(self.database_url)
