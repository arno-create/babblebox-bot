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


DEFAULT_BACKEND = "postgres"
QUESTION_DROP_MIN_DROPS_PER_DAY = 1
QUESTION_DROP_MAX_DROPS_PER_DAY = 10
QUESTION_DROP_KNOWLEDGE_CATEGORIES = (
    "science",
    "history",
    "geography",
    "language",
    "logic",
    "math",
    "culture",
)
QUESTION_DROP_MASTERY_TIERS = (1, 2, 3)
QUESTION_DROP_AI_CELEBRATION_MODES = ("off", "rare", "event_only")


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
        "enabled_categories": list(QUESTION_DROP_KNOWLEDGE_CATEGORIES),
        "category_mastery": default_question_drop_category_mastery(),
        "scholar_ladder": default_question_drop_scholar_ladder(),
        "ai_celebrations_enabled": False,
    }


def default_question_drop_tier(tier: int) -> dict[str, Any]:
    return {"tier": int(tier), "role_id": None, "threshold": 0}


def default_question_drop_tiers() -> list[dict[str, Any]]:
    return [default_question_drop_tier(tier) for tier in QUESTION_DROP_MASTERY_TIERS]


def default_question_drop_category_mastery() -> dict[str, Any]:
    return {
        category: {
            "enabled": False,
            "announcement_channel_id": None,
            "silent_grant": False,
            "tiers": default_question_drop_tiers(),
        }
        for category in QUESTION_DROP_KNOWLEDGE_CATEGORIES
    }


def default_question_drop_scholar_ladder() -> dict[str, Any]:
    return {
        "enabled": False,
        "announcement_channel_id": None,
        "silent_grant": False,
        "tiers": default_question_drop_tiers(),
    }


def default_question_drops_meta() -> dict[str, Any]:
    return {
        "ai_celebration_mode": "off",
        "updated_by": None,
        "updated_at": None,
    }


def _normalize_positive_int(value: Any) -> int | None:
    return int(value) if isinstance(value, int) and value > 0 else None


def _normalize_nonnegative_int(value: Any, *, default: int = 0) -> int:
    return int(value) if isinstance(value, int) and value >= 0 else default


def _normalize_question_drop_tiers(payload: Any) -> list[dict[str, Any]]:
    normalized_by_tier = {tier: default_question_drop_tier(tier) for tier in QUESTION_DROP_MASTERY_TIERS}
    raw_items = payload if isinstance(payload, list) else []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        tier = item.get("tier")
        if not isinstance(tier, int) or tier not in QUESTION_DROP_MASTERY_TIERS:
            continue
        normalized_by_tier[tier] = {
            "tier": tier,
            "role_id": _normalize_positive_int(item.get("role_id")),
            "threshold": _normalize_nonnegative_int(item.get("threshold"), default=0),
        }
    return [normalized_by_tier[tier] for tier in QUESTION_DROP_MASTERY_TIERS]


def _normalize_category_mastery(payload: Any) -> dict[str, Any]:
    default_value = default_question_drop_category_mastery()
    if not isinstance(payload, dict):
        return default_value
    cleaned: dict[str, Any] = {}
    for category in QUESTION_DROP_KNOWLEDGE_CATEGORIES:
        raw_category = payload.get(category)
        if not isinstance(raw_category, dict):
            cleaned[category] = deepcopy(default_value[category])
            continue
        cleaned[category] = {
            "enabled": bool(raw_category.get("enabled")),
            "announcement_channel_id": _normalize_positive_int(raw_category.get("announcement_channel_id")),
            "silent_grant": bool(raw_category.get("silent_grant")),
            "tiers": _normalize_question_drop_tiers(raw_category.get("tiers")),
        }
    return cleaned


def _normalize_scholar_ladder(payload: Any) -> dict[str, Any]:
    default_value = default_question_drop_scholar_ladder()
    if not isinstance(payload, dict):
        return default_value
    return {
        "enabled": bool(payload.get("enabled")),
        "announcement_channel_id": _normalize_positive_int(payload.get("announcement_channel_id")),
        "silent_grant": bool(payload.get("silent_grant")),
        "tiers": _normalize_question_drop_tiers(payload.get("tiers")),
    }


def normalize_question_drops_meta(payload: Any) -> dict[str, Any]:
    cleaned = default_question_drops_meta()
    if not isinstance(payload, dict):
        return cleaned
    mode = str(payload.get("ai_celebration_mode", "off")).strip().casefold()
    cleaned["ai_celebration_mode"] = mode if mode in QUESTION_DROP_AI_CELEBRATION_MODES else "off"
    cleaned["updated_by"] = _normalize_positive_int(payload.get("updated_by"))
    cleaned["updated_at"] = _serialize_datetime(_parse_datetime(payload.get("updated_at")))
    return cleaned


def normalize_question_drops_config(guild_id: int, payload: Any) -> dict[str, Any]:
    cleaned = default_question_drops_config(guild_id)
    if not isinstance(payload, dict):
        return cleaned
    cleaned["enabled"] = bool(payload.get("enabled"))
    drops_per_day = payload.get("drops_per_day")
    cleaned["drops_per_day"] = (
        drops_per_day
        if isinstance(drops_per_day, int) and QUESTION_DROP_MIN_DROPS_PER_DAY <= drops_per_day <= QUESTION_DROP_MAX_DROPS_PER_DAY
        else 2
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
            if str(value).strip().casefold() in QUESTION_DROP_KNOWLEDGE_CATEGORIES
        }
    )
    cleaned["category_mastery"] = _normalize_category_mastery(payload.get("category_mastery"))
    cleaned["scholar_ladder"] = _normalize_scholar_ladder(payload.get("scholar_ladder"))
    cleaned["ai_celebrations_enabled"] = bool(payload.get("ai_celebrations_enabled"))
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


def normalize_pending_post(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    required_ints = ("guild_id", "channel_id")
    for field in required_ints:
        if not isinstance(payload.get(field), int) or int(payload[field]) <= 0:
            return None
    string_fields = ("slot_key", "concept_id", "variant_hash")
    if not all(isinstance(payload.get(field), str) and payload[field].strip() for field in string_fields):
        return None
    claimed_at = _serialize_datetime(_parse_datetime(payload.get("claimed_at")))
    lease_expires_at = _serialize_datetime(_parse_datetime(payload.get("lease_expires_at")))
    if claimed_at is None or lease_expires_at is None:
        return None
    message_id = payload.get("message_id")
    if message_id is not None and (not isinstance(message_id, int) or message_id <= 0):
        return None
    return {
        "guild_id": int(payload["guild_id"]),
        "channel_id": int(payload["channel_id"]),
        "slot_key": payload["slot_key"].strip(),
        "concept_id": payload["concept_id"].strip(),
        "variant_hash": payload["variant_hash"].strip(),
        "claimed_at": claimed_at,
        "lease_expires_at": lease_expires_at,
        "message_id": int(message_id) if isinstance(message_id, int) and message_id > 0 else None,
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

    async def fetch_meta(self) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_config(self, config: dict[str, Any]):
        raise NotImplementedError

    async def upsert_meta(self, meta: dict[str, Any]):
        raise NotImplementedError

    async def list_active_drops(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_pending_posts(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_active_drop(self, record: dict[str, Any]):
        raise NotImplementedError

    async def claim_pending_post(self, record: dict[str, Any]) -> dict[str, Any] | None:
        raise NotImplementedError

    async def attach_pending_post_message(self, guild_id: int, slot_key: str, message_id: int):
        raise NotImplementedError

    async def finalize_pending_post(
        self,
        guild_id: int,
        slot_key: str,
        *,
        exposure_record: dict[str, Any],
        active_record: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        raise NotImplementedError

    async def release_pending_post(self, guild_id: int, slot_key: str):
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
        self.meta: dict[str, Any] = default_question_drops_meta()
        self.active_drops: dict[tuple[int, int], dict[str, Any]] = {}
        self.pending_posts: dict[tuple[int, str], dict[str, Any]] = {}
        self.exposures: dict[int, dict[str, Any]] = {}
        self._next_exposure_id = 1

    async def load(self):
        self.configs = {}
        self.meta = default_question_drops_meta()
        self.active_drops = {}
        self.pending_posts = {}
        self.exposures = {}
        self._next_exposure_id = 1

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return {guild_id: deepcopy(config) for guild_id, config in self.configs.items()}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        config = self.configs.get(guild_id)
        return deepcopy(config) if config is not None else None

    async def fetch_meta(self) -> dict[str, Any] | None:
        return deepcopy(self.meta)

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_question_drops_config(int(config["guild_id"]), config)
        self.configs[int(config["guild_id"])] = normalized

    async def upsert_meta(self, meta: dict[str, Any]):
        self.meta = normalize_question_drops_meta(meta)

    async def list_active_drops(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.active_drops.values()]

    async def list_pending_posts(self) -> list[dict[str, Any]]:
        return [deepcopy(record) for record in self.pending_posts.values()]

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        record = self.active_drops.get((guild_id, channel_id))
        return deepcopy(record) if record is not None else None

    async def upsert_active_drop(self, record: dict[str, Any]):
        normalized = normalize_active_drop(record)
        if normalized is not None:
            self.active_drops[(normalized["guild_id"], normalized["channel_id"])] = normalized

    async def claim_pending_post(self, record: dict[str, Any]) -> dict[str, Any] | None:
        normalized = normalize_pending_post(record)
        if normalized is None:
            return None
        pending_key = (normalized["guild_id"], normalized["slot_key"])
        active_key = (normalized["guild_id"], normalized["channel_id"])
        if pending_key in self.pending_posts:
            return None
        if active_key in self.active_drops:
            return None
        if any(
            pending["guild_id"] == normalized["guild_id"] and pending["channel_id"] == normalized["channel_id"]
            for pending in self.pending_posts.values()
        ):
            return None
        if any(
            exposure["guild_id"] == normalized["guild_id"] and exposure["slot_key"] == normalized["slot_key"]
            for exposure in self.exposures.values()
        ):
            return None
        self.pending_posts[pending_key] = deepcopy(normalized)
        return deepcopy(normalized)

    async def attach_pending_post_message(self, guild_id: int, slot_key: str, message_id: int):
        pending = self.pending_posts.get((guild_id, slot_key))
        if pending is None:
            return
        if isinstance(message_id, int) and message_id > 0:
            pending["message_id"] = message_id

    async def finalize_pending_post(
        self,
        guild_id: int,
        slot_key: str,
        *,
        exposure_record: dict[str, Any],
        active_record: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pending = self.pending_posts.get((guild_id, slot_key))
        normalized_exposure = normalize_exposure(exposure_record)
        normalized_active = normalize_active_drop(active_record, allow_missing_exposure_id=True)
        if pending is None:
            raise ValueError("Question Drops slot was not reserved.")
        if normalized_exposure is None or normalized_active is None:
            raise ValueError("Invalid Question Drops record.")
        active_key = (normalized_active["guild_id"], normalized_active["channel_id"])
        if active_key in self.active_drops:
            raise ValueError("Question Drops channel already has an active drop.")
        if any(
            exposure["guild_id"] == normalized_exposure["guild_id"] and exposure["slot_key"] == normalized_exposure["slot_key"]
            for exposure in self.exposures.values()
        ):
            raise ValueError("Question Drops slot already registered.")
        normalized_exposure["id"] = self._next_exposure_id
        self._next_exposure_id += 1
        self.exposures[int(normalized_exposure["id"])] = deepcopy(normalized_exposure)
        normalized_active["exposure_id"] = int(normalized_exposure["id"])
        self.active_drops[active_key] = deepcopy(normalized_active)
        self.pending_posts.pop((guild_id, slot_key), None)
        return deepcopy(normalized_exposure), deepcopy(normalized_active)

    async def release_pending_post(self, guild_id: int, slot_key: str):
        self.pending_posts.pop((guild_id, slot_key), None)

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
            "enabled_channel_ids": decode_postgres_json_array(
                row["enabled_channel_ids"],
                label="question_drop_configs.enabled_channel_ids",
            ),
            "enabled_categories": decode_postgres_json_array(
                row["enabled_categories"],
                label="question_drop_configs.enabled_categories",
            ),
            "category_mastery": decode_postgres_json_object(
                row["category_mastery"],
                label="question_drop_configs.category_mastery",
            ),
            "scholar_ladder": decode_postgres_json_object(
                row["scholar_ladder"],
                label="question_drop_configs.scholar_ladder",
            ),
            "ai_celebrations_enabled": row["ai_celebrations_enabled"],
        },
    )


def _meta_from_row(row) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = decode_postgres_json_object(
        row["value"],
        label="question_drop_meta.value",
    )
    payload["updated_at"] = _serialize_datetime(row.get("updated_at"))
    return normalize_question_drops_meta(payload)


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
            "answer_spec": decode_postgres_json_object(
                row["answer_spec"],
                label="question_drop_active.answer_spec",
            ),
            "asked_at": _serialize_datetime(row["asked_at"]),
            "expires_at": _serialize_datetime(row["expires_at"]),
            "slot_key": row["slot_key"],
            "tone_mode": row["tone_mode"],
            "participant_user_ids": decode_postgres_json_array(
                row["participant_user_ids"],
                label="question_drop_active.participant_user_ids",
            ),
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


def _pending_post_from_row(row) -> dict[str, Any] | None:
    return normalize_pending_post(
        {
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "slot_key": row["slot_key"],
            "concept_id": row["concept_id"],
            "variant_hash": row["variant_hash"],
            "claimed_at": _serialize_datetime(row["claimed_at"]),
            "lease_expires_at": _serialize_datetime(row["lease_expires_at"]),
            "message_id": row["message_id"],
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
                "category_mastery JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "scholar_ladder JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "ai_celebrations_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS question_drop_meta ("
                "key TEXT PRIMARY KEY, "
                "value JSONB NOT NULL DEFAULT '{}'::jsonb, "
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
            "ALTER TABLE question_drop_configs ADD COLUMN IF NOT EXISTS category_mastery JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE question_drop_configs ADD COLUMN IF NOT EXISTS scholar_ladder JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE question_drop_configs ADD COLUMN IF NOT EXISTS ai_celebrations_enabled BOOLEAN NOT NULL DEFAULT FALSE",
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
                "CREATE TABLE IF NOT EXISTS question_drop_pending ("
                "guild_id BIGINT NOT NULL, "
                "channel_id BIGINT NOT NULL, "
                "slot_key TEXT NOT NULL, "
                "concept_id TEXT NOT NULL, "
                "variant_hash TEXT NOT NULL, "
                "claimed_at TIMESTAMPTZ NOT NULL, "
                "lease_expires_at TIMESTAMPTZ NOT NULL, "
                "message_id BIGINT NULL, "
                "PRIMARY KEY (guild_id, slot_key)"
                ")"
            ),
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_question_drop_pending_guild_channel ON question_drop_pending (guild_id, channel_id)",
            "CREATE INDEX IF NOT EXISTS ix_question_drop_pending_lease ON question_drop_pending (lease_expires_at)",
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
                    "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories, "
                    "category_mastery, scholar_ladder, ai_celebrations_enabled "
                    "FROM question_drop_configs"
                )
            )
        return {int(row["guild_id"]): _config_from_row(row) for row in rows}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, enabled, drops_per_day, timezone, answer_window_seconds, tone_mode, activity_gate, "
                    "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories, "
                    "category_mastery, scholar_ladder, ai_celebrations_enabled "
                    "FROM question_drop_configs WHERE guild_id = $1"
                ),
                guild_id,
            )
        return _config_from_row(row) if row is not None else None

    async def fetch_meta(self) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT value, updated_at FROM question_drop_meta WHERE key = 'global'"
            )
        return _meta_from_row(row) if row is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_question_drops_config(int(config["guild_id"]), config)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO question_drop_configs ("
                        "guild_id, enabled, drops_per_day, timezone, answer_window_seconds, tone_mode, activity_gate, "
                        "active_start_hour, active_end_hour, enabled_channel_ids, enabled_categories, "
                        "category_mastery, scholar_ladder, ai_celebrations_enabled, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, $14, timezone('utc', now())"
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
                        "category_mastery = EXCLUDED.category_mastery, "
                        "scholar_ladder = EXCLUDED.scholar_ladder, "
                        "ai_celebrations_enabled = EXCLUDED.ai_celebrations_enabled, "
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
                    json.dumps(normalized["category_mastery"]),
                    json.dumps(normalized["scholar_ladder"]),
                    normalized["ai_celebrations_enabled"],
                )

    async def upsert_meta(self, meta: dict[str, Any]):
        normalized = normalize_question_drops_meta(meta)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO question_drop_meta (key, value, updated_at) "
                        "VALUES ('global', $1::jsonb, timezone('utc', now())) "
                        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
                    ),
                    json.dumps(
                        {
                            "ai_celebration_mode": normalized["ai_celebration_mode"],
                            "updated_by": normalized["updated_by"],
                            "updated_at": normalized["updated_at"],
                        }
                    ),
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

    async def list_pending_posts(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, channel_id, slot_key, concept_id, variant_hash, claimed_at, lease_expires_at, message_id "
                    "FROM question_drop_pending ORDER BY claimed_at ASC"
                )
            )
        return [record for row in rows if (record := _pending_post_from_row(row)) is not None]

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

    async def claim_pending_post(self, record: dict[str, Any]) -> dict[str, Any] | None:
        normalized = normalize_pending_post(record)
        if normalized is None:
            return None
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    if await conn.fetchval(
                        "SELECT 1 FROM question_drop_exposures WHERE guild_id = $1 AND slot_key = $2",
                        normalized["guild_id"],
                        normalized["slot_key"],
                    ):
                        return None
                    if await conn.fetchval(
                        "SELECT 1 FROM question_drop_active WHERE guild_id = $1 AND channel_id = $2",
                        normalized["guild_id"],
                        normalized["channel_id"],
                    ):
                        return None
                    if await conn.fetchval(
                        "SELECT 1 FROM question_drop_pending WHERE guild_id = $1 AND channel_id = $2",
                        normalized["guild_id"],
                        normalized["channel_id"],
                    ):
                        return None
                    row = await conn.fetchrow(
                        (
                            "INSERT INTO question_drop_pending ("
                            "guild_id, channel_id, slot_key, concept_id, variant_hash, claimed_at, lease_expires_at, message_id"
                            ") VALUES ("
                            "$1, $2, $3, $4, $5, $6, $7, $8"
                            ") ON CONFLICT DO NOTHING "
                            "RETURNING guild_id, channel_id, slot_key, concept_id, variant_hash, claimed_at, lease_expires_at, message_id"
                        ),
                        normalized["guild_id"],
                        normalized["channel_id"],
                        normalized["slot_key"],
                        normalized["concept_id"],
                        normalized["variant_hash"],
                        _parse_datetime(normalized["claimed_at"]),
                        _parse_datetime(normalized["lease_expires_at"]),
                        normalized["message_id"],
                    )
        return _pending_post_from_row(row) if row is not None else None

    async def attach_pending_post_message(self, guild_id: int, slot_key: str, message_id: int):
        if not isinstance(message_id, int) or message_id <= 0:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE question_drop_pending SET message_id = $3 WHERE guild_id = $1 AND slot_key = $2",
                    guild_id,
                    slot_key,
                    message_id,
                )

    async def finalize_pending_post(
        self,
        guild_id: int,
        slot_key: str,
        *,
        exposure_record: dict[str, Any],
        active_record: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        normalized_exposure = normalize_exposure(exposure_record)
        normalized_active = normalize_active_drop(active_record, allow_missing_exposure_id=True)
        if normalized_exposure is None or normalized_active is None:
            raise ValueError("Invalid Question Drops record.")
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    pending = await conn.fetchrow(
                        (
                            "SELECT guild_id, channel_id, slot_key, concept_id, variant_hash, claimed_at, lease_expires_at, message_id "
                            "FROM question_drop_pending WHERE guild_id = $1 AND slot_key = $2"
                        ),
                        guild_id,
                        slot_key,
                    )
                    if pending is None:
                        raise ValueError("Question Drops slot was not reserved.")
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
                    await conn.execute("DELETE FROM question_drop_pending WHERE guild_id = $1 AND slot_key = $2", guild_id, slot_key)
        return normalized_exposure, normalize_active_drop(normalized_active)

    async def release_pending_post(self, guild_id: int, slot_key: str):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM question_drop_pending WHERE guild_id = $1 AND slot_key = $2", guild_id, slot_key)

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

    async def fetch_meta(self) -> dict[str, Any] | None:
        return await self._store.fetch_meta()

    async def upsert_config(self, config: dict[str, Any]):
        await self._store.upsert_config(config)

    async def upsert_meta(self, meta: dict[str, Any]):
        await self._store.upsert_meta(meta)

    async def list_active_drops(self) -> list[dict[str, Any]]:
        return await self._store.list_active_drops()

    async def list_pending_posts(self) -> list[dict[str, Any]]:
        return await self._store.list_pending_posts()

    async def fetch_active_drop(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_active_drop(guild_id, channel_id)

    async def upsert_active_drop(self, record: dict[str, Any]):
        await self._store.upsert_active_drop(record)

    async def claim_pending_post(self, record: dict[str, Any]) -> dict[str, Any] | None:
        return await self._store.claim_pending_post(record)

    async def attach_pending_post_message(self, guild_id: int, slot_key: str, message_id: int):
        await self._store.attach_pending_post_message(guild_id, slot_key, message_id)

    async def finalize_pending_post(
        self,
        guild_id: int,
        slot_key: str,
        *,
        exposure_record: dict[str, Any],
        active_record: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        return await self._store.finalize_pending_post(
            guild_id,
            slot_key,
            exposure_record=exposure_record,
            active_record=active_record,
        )

    async def release_pending_post(self, guild_id: int, slot_key: str):
        await self._store.release_pending_post(guild_id, slot_key)

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
