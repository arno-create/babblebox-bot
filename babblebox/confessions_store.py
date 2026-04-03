from __future__ import annotations

import asyncio
import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array
from babblebox.text_safety import normalize_plain_text


DEFAULT_BACKEND = "postgres"
DEFAULT_DATABASE_URL_ENV_ORDER = ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")
DISCORD_MEDIA_HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})
VALID_RESTRICTIONS = {"none", "suspended", "temp_ban", "perm_ban"}
VALID_SUBMISSION_STATUSES = {"blocked", "queued", "published", "denied", "deleted", "overridden"}
VALID_REVIEW_STATUSES = {"none", "pending", "approved", "denied", "overridden", "blocked"}
VALID_CASE_KINDS = {"review", "safety_block", "published_moderation"}
VALID_CASE_STATUSES = {"open", "resolved"}


class ConfessionsStorageUnavailable(RuntimeError):
    pass


def default_confession_config(guild_id: int | None = None) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "enabled": False,
        "confession_channel_id": None,
        "panel_channel_id": None,
        "panel_message_id": None,
        "review_channel_id": None,
        "review_mode": True,
        "block_adult_language": True,
        "allow_trusted_mainstream_links": True,
        "custom_allow_domains": [],
        "custom_block_domains": [],
        "allow_images": False,
        "max_images": 3,
        "cooldown_seconds": 5 * 60,
        "burst_limit": 3,
        "burst_window_seconds": 30 * 60,
        "auto_suspend_hours": 12,
        "strike_temp_ban_threshold": 3,
        "temp_ban_days": 7,
        "strike_perm_ban_threshold": 5,
    }


def default_enforcement_state(guild_id: int, user_id: int) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "active_restriction": "none",
        "restricted_until": None,
        "is_permanent_ban": False,
        "strike_count": 0,
        "last_strike_at": None,
        "cooldown_until": None,
        "burst_count": 0,
        "burst_window_started_at": None,
        "last_case_id": None,
        "updated_at": None,
    }


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


def _clean_int(value: Any) -> int | None:
    return value if isinstance(value, int) and value > 0 else None


def _clean_optional_text(value: Any, *, max_length: int | None = None) -> str | None:
    if value is None:
        return None
    cleaned = normalize_plain_text(str(value))
    if not cleaned:
        return None
    if max_length is not None:
        cleaned = cleaned[:max_length]
    return cleaned


def _clean_domain_list(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    cleaned = {
        normalize_plain_text(str(value)).casefold().strip().strip(".")
        for value in values
        if isinstance(value, str) and normalize_plain_text(str(value)).strip()
    }
    return sorted(value for value in cleaned if value)


def _clean_string_list(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    cleaned = {
        normalize_plain_text(str(value)).casefold()
        for value in values
        if isinstance(value, str) and normalize_plain_text(str(value))
    }
    return sorted(cleaned)


def _normalize_private_media_url(raw_value: Any) -> str | None:
    cleaned = _clean_optional_text(raw_value, max_length=500)
    if cleaned is None:
        return None
    try:
        parsed = urlsplit(cleaned)
    except ValueError:
        return None
    host = normalize_plain_text(parsed.netloc).casefold().strip()
    if parsed.scheme != "https" or host not in DISCORD_MEDIA_HOSTS or not normalize_plain_text(parsed.path):
        return None
    return urlunsplit(("https", host, parsed.path, parsed.query or "", ""))


def _clean_private_media_url_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    cleaned = []
    for value in values:
        url = _normalize_private_media_url(value)
        if url:
            cleaned.append(url)
    return cleaned[:3]


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
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _clean_attachment_meta(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    cleaned: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        kind = _clean_optional_text(item.get("kind"), max_length=24)
        if kind is None:
            continue
        size = item.get("size")
        width = item.get("width")
        height = item.get("height")
        cleaned.append(
            {
                "kind": kind,
                "size": size if isinstance(size, int) and size >= 0 else None,
                "width": width if isinstance(width, int) and width >= 0 else None,
                "height": height if isinstance(height, int) and height >= 0 else None,
                "spoiler": bool(item.get("spoiler")),
            }
        )
    return cleaned[:3]


def normalize_confession_config(guild_id: int, payload: Any) -> dict[str, Any]:
    cleaned = default_confession_config(guild_id)
    if not isinstance(payload, dict):
        return cleaned
    cleaned["enabled"] = bool(payload.get("enabled"))
    cleaned["confession_channel_id"] = _clean_int(payload.get("confession_channel_id"))
    cleaned["panel_channel_id"] = _clean_int(payload.get("panel_channel_id"))
    cleaned["panel_message_id"] = _clean_int(payload.get("panel_message_id"))
    cleaned["review_channel_id"] = _clean_int(payload.get("review_channel_id"))
    cleaned["review_mode"] = bool(payload.get("review_mode", True))
    cleaned["block_adult_language"] = bool(payload.get("block_adult_language", True))
    cleaned["allow_trusted_mainstream_links"] = bool(payload.get("allow_trusted_mainstream_links", True))
    cleaned["custom_allow_domains"] = _clean_domain_list(payload.get("custom_allow_domains"))
    cleaned["custom_block_domains"] = _clean_domain_list(payload.get("custom_block_domains"))
    cleaned["allow_images"] = bool(payload.get("allow_images", False))
    max_images = payload.get("max_images")
    cleaned["max_images"] = max_images if isinstance(max_images, int) and 1 <= max_images <= 3 else 3
    for field, default_value, minimum, maximum in (
        ("cooldown_seconds", 5 * 60, 15, 24 * 3600),
        ("burst_limit", 3, 1, 10),
        ("burst_window_seconds", 30 * 60, 60, 24 * 3600),
        ("auto_suspend_hours", 12, 1, 24 * 30),
        ("strike_temp_ban_threshold", 3, 2, 10),
        ("temp_ban_days", 7, 1, 90),
        ("strike_perm_ban_threshold", 5, 3, 20),
    ):
        value = payload.get(field)
        cleaned[field] = value if isinstance(value, int) and minimum <= value <= maximum else default_value
    if cleaned["strike_perm_ban_threshold"] < cleaned["strike_temp_ban_threshold"]:
        cleaned["strike_perm_ban_threshold"] = cleaned["strike_temp_ban_threshold"]
    if cleaned["panel_channel_id"] is None:
        cleaned["panel_message_id"] = None
    if cleaned["review_channel_id"] is None or cleaned["review_channel_id"] == cleaned["confession_channel_id"]:
        cleaned["allow_images"] = False
    return cleaned


def normalize_submission(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    submission_id = _clean_optional_text(payload.get("submission_id"), max_length=64)
    confession_id = _clean_optional_text(payload.get("confession_id"), max_length=32)
    status = str(payload.get("status", "queued")).strip().lower()
    review_status = str(payload.get("review_status", "none")).strip().lower()
    created_at = _serialize_datetime(payload.get("created_at"))
    if guild_id is None or submission_id is None or confession_id is None or created_at is None:
        return None
    if status not in VALID_SUBMISSION_STATUSES or review_status not in VALID_REVIEW_STATUSES:
        return None
    return {
        "submission_id": submission_id,
        "guild_id": guild_id,
        "confession_id": confession_id,
        "status": status,
        "review_status": review_status,
        "staff_preview": _clean_optional_text(payload.get("staff_preview"), max_length=260),
        "content_body": _clean_optional_text(payload.get("content_body"), max_length=2000),
        "shared_link_url": _clean_optional_text(payload.get("shared_link_url"), max_length=500),
        "content_fingerprint": _clean_optional_text(payload.get("content_fingerprint"), max_length=96),
        "similarity_key": _clean_optional_text(payload.get("similarity_key"), max_length=160),
        "flag_codes": _clean_string_list(payload.get("flag_codes")),
        "attachment_meta": _clean_attachment_meta(payload.get("attachment_meta")),
        "posted_channel_id": _clean_int(payload.get("posted_channel_id")),
        "posted_message_id": _clean_int(payload.get("posted_message_id")),
        "current_case_id": _clean_optional_text(payload.get("current_case_id"), max_length=32),
        "created_at": created_at,
        "published_at": _serialize_datetime(payload.get("published_at")),
        "resolved_at": _serialize_datetime(payload.get("resolved_at")),
    }


def normalize_author_link(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    submission_id = _clean_optional_text(payload.get("submission_id"), max_length=64)
    author_user_id = _clean_int(payload.get("author_user_id"))
    created_at = _serialize_datetime(payload.get("created_at"))
    if guild_id is None or submission_id is None or author_user_id is None or created_at is None:
        return None
    return {
        "guild_id": guild_id,
        "submission_id": submission_id,
        "author_user_id": author_user_id,
        "created_at": created_at,
    }


def normalize_private_media(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    submission_id = _clean_optional_text(payload.get("submission_id"), max_length=64)
    created_at = _serialize_datetime(payload.get("created_at"))
    updated_at = _serialize_datetime(payload.get("updated_at"))
    attachment_urls = _clean_private_media_url_list(payload.get("attachment_urls"))
    if guild_id is None or submission_id is None or created_at is None or updated_at is None:
        return None
    return {
        "guild_id": guild_id,
        "submission_id": submission_id,
        "attachment_urls": attachment_urls,
        "created_at": created_at,
        "updated_at": updated_at,
    }


def normalize_enforcement_state(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    user_id = _clean_int(payload.get("user_id"))
    if guild_id is None or user_id is None:
        return None
    active_restriction = str(payload.get("active_restriction", "none")).strip().lower()
    if active_restriction not in VALID_RESTRICTIONS:
        active_restriction = "none"
    strike_count = payload.get("strike_count")
    burst_count = payload.get("burst_count")
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "active_restriction": active_restriction,
        "restricted_until": _serialize_datetime(payload.get("restricted_until")),
        "is_permanent_ban": bool(payload.get("is_permanent_ban")),
        "strike_count": strike_count if isinstance(strike_count, int) and strike_count >= 0 else 0,
        "last_strike_at": _serialize_datetime(payload.get("last_strike_at")),
        "cooldown_until": _serialize_datetime(payload.get("cooldown_until")),
        "burst_count": burst_count if isinstance(burst_count, int) and burst_count >= 0 else 0,
        "burst_window_started_at": _serialize_datetime(payload.get("burst_window_started_at")),
        "last_case_id": _clean_optional_text(payload.get("last_case_id"), max_length=32),
        "updated_at": _serialize_datetime(payload.get("updated_at")),
    }


def normalize_case(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    submission_id = _clean_optional_text(payload.get("submission_id"), max_length=64)
    confession_id = _clean_optional_text(payload.get("confession_id"), max_length=32)
    case_id = _clean_optional_text(payload.get("case_id"), max_length=32)
    case_kind = str(payload.get("case_kind", "review")).strip().lower()
    status = str(payload.get("status", "open")).strip().lower()
    created_at = _serialize_datetime(payload.get("created_at"))
    if guild_id is None or submission_id is None or confession_id is None or case_id is None or created_at is None:
        return None
    if case_kind not in VALID_CASE_KINDS or status not in VALID_CASE_STATUSES:
        return None
    version = payload.get("review_version")
    return {
        "guild_id": guild_id,
        "submission_id": submission_id,
        "confession_id": confession_id,
        "case_id": case_id,
        "case_kind": case_kind,
        "status": status,
        "reason_codes": _clean_string_list(payload.get("reason_codes")),
        "review_version": version if isinstance(version, int) and version >= 0 else 0,
        "resolution_action": _clean_optional_text(payload.get("resolution_action"), max_length=48),
        "resolution_note": _clean_optional_text(payload.get("resolution_note"), max_length=240),
        "review_message_channel_id": _clean_int(payload.get("review_message_channel_id")),
        "review_message_id": _clean_int(payload.get("review_message_id")),
        "created_at": created_at,
        "resolved_at": _serialize_datetime(payload.get("resolved_at")),
    }


def normalize_review_queue(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    if guild_id is None:
        return None
    return {
        "guild_id": guild_id,
        "channel_id": _clean_int(payload.get("channel_id")),
        "message_id": _clean_int(payload.get("message_id")),
        "updated_at": _serialize_datetime(payload.get("updated_at")),
    }


def _submission_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_submission(
        {
            "submission_id": row["submission_id"],
            "guild_id": row["guild_id"],
            "confession_id": row["confession_id"],
                "status": row["status"],
                "review_status": row["review_status"],
                "staff_preview": row["staff_preview"],
                "content_body": row["content_body"],
                "shared_link_url": row.get("shared_link_url"),
                "content_fingerprint": row["content_fingerprint"],
                "similarity_key": row["similarity_key"],
            "flag_codes": decode_postgres_json_array(row["flag_codes"], label="confession_submissions.flag_codes"),
            "attachment_meta": decode_postgres_json_array(row["attachment_meta"], label="confession_submissions.attachment_meta"),
            "posted_channel_id": row["posted_channel_id"],
            "posted_message_id": row["posted_message_id"],
            "current_case_id": row["current_case_id"],
            "created_at": row["created_at"],
            "published_at": row["published_at"],
            "resolved_at": row["resolved_at"],
        }
    )


def _case_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_case(
        {
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "confession_id": row["confession_id"],
            "case_id": row["case_id"],
            "case_kind": row["case_kind"],
            "status": row["status"],
            "reason_codes": decode_postgres_json_array(row["reason_codes"], label="confession_cases.reason_codes"),
            "review_version": row["review_version"],
            "resolution_action": row["resolution_action"],
            "resolution_note": row["resolution_note"],
            "review_message_channel_id": row["review_message_channel_id"],
            "review_message_id": row["review_message_id"],
            "created_at": row["created_at"],
            "resolved_at": row["resolved_at"],
        }
    )


def _config_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_confession_config(
        int(row["guild_id"]),
        {
            "enabled": row["enabled"],
            "confession_channel_id": row["confession_channel_id"],
            "panel_channel_id": row["panel_channel_id"],
            "panel_message_id": row["panel_message_id"],
            "review_channel_id": row["review_channel_id"],
            "review_mode": row["review_mode"],
            "block_adult_language": row["block_adult_language"],
            "allow_trusted_mainstream_links": row["allow_trusted_mainstream_links"],
            "custom_allow_domains": decode_postgres_json_array(
                row["custom_allow_domains"],
                label="confession_guild_configs.custom_allow_domains",
            ),
            "custom_block_domains": decode_postgres_json_array(
                row["custom_block_domains"],
                label="confession_guild_configs.custom_block_domains",
            ),
            "allow_images": row["allow_images"],
            "max_images": row["max_images"],
            "cooldown_seconds": row["cooldown_seconds"],
            "burst_limit": row["burst_limit"],
            "burst_window_seconds": row["burst_window_seconds"],
            "auto_suspend_hours": row["auto_suspend_hours"],
            "strike_temp_ban_threshold": row["strike_temp_ban_threshold"],
            "temp_ban_days": row["temp_ban_days"],
            "strike_perm_ban_threshold": row["strike_perm_ban_threshold"],
        },
    )


def _author_link_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_author_link(
        {
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "author_user_id": row["author_user_id"],
            "created_at": row["created_at"],
        }
    )


def _enforcement_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_enforcement_state(
        {
            "guild_id": row["guild_id"],
            "user_id": row["user_id"],
            "active_restriction": row["active_restriction"],
            "restricted_until": row["restricted_until"],
            "is_permanent_ban": row["is_permanent_ban"],
            "strike_count": row["strike_count"],
            "last_strike_at": row["last_strike_at"],
            "cooldown_until": row["cooldown_until"],
            "burst_count": row["burst_count"],
            "burst_window_started_at": row["burst_window_started_at"],
            "last_case_id": row["last_case_id"],
            "updated_at": row["updated_at"],
        }
    )


def _review_queue_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_review_queue(
        {
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "message_id": row["message_id"],
            "updated_at": row["updated_at"],
        }
    )


def _private_media_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_private_media(
        {
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "attachment_urls": decode_postgres_json_array(row["attachment_urls"], label="confession_private_media.attachment_urls"),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


class _BaseConfessionsStore:
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

    async def upsert_submission(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_review_cases(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_case(self, guild_id: int, case_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_case(self, record: dict[str, Any]):
        raise NotImplementedError

    async def upsert_author_link(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_private_media(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_private_media(self, submission_id: str):
        raise NotImplementedError

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        raise NotImplementedError

    async def list_review_queues(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_review_surfaces(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_review_queue(self, record: dict[str, Any]):
        raise NotImplementedError

    async def delete_review_queue(self, guild_id: int):
        raise NotImplementedError

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        raise NotImplementedError


class _MemoryConfessionsStore(_BaseConfessionsStore):
    backend_name = "memory"

    def __init__(self):
        self.configs: dict[int, dict[str, Any]] = {}
        self.submissions: dict[str, dict[str, Any]] = {}
        self.author_links: dict[str, dict[str, Any]] = {}
        self.private_media: dict[str, dict[str, Any]] = {}
        self.enforcement_states: dict[tuple[int, int], dict[str, Any]] = {}
        self.cases: dict[tuple[int, str], dict[str, Any]] = {}
        self.review_queues: dict[int, dict[str, Any]] = {}

    async def load(self):
        self.configs = {}
        self.submissions = {}
        self.author_links = {}
        self.private_media = {}
        self.enforcement_states = {}
        self.cases = {}
        self.review_queues = {}

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return {guild_id: deepcopy(record) for guild_id, record in self.configs.items()}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        record = self.configs.get(guild_id)
        return deepcopy(record) if record is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_confession_config(int(config["guild_id"]), deepcopy(config))
        self.configs[int(normalized["guild_id"])] = normalized

    async def upsert_submission(self, record: dict[str, Any]):
        normalized = normalize_submission(record)
        if normalized is not None:
            self.submissions[normalized["submission_id"]] = normalized

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        record = self.submissions.get(submission_id)
        return deepcopy(record) if record is not None else None

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        for record in self.submissions.values():
            if record["guild_id"] == guild_id and record["confession_id"] == confession_id:
                return deepcopy(record)
        return None

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        for record in self.submissions.values():
            if record["guild_id"] == guild_id and int(record.get("posted_message_id") or 0) == message_id:
                return deepcopy(record)
        return None

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        rows = []
        for link in self.author_links.values():
            if link["guild_id"] != guild_id or link["author_user_id"] != author_user_id:
                continue
            submission = self.submissions.get(link["submission_id"])
            if submission is None:
                continue
            rows.append(deepcopy(submission))
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def list_review_cases(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        rows = []
        for record in self.cases.values():
            if record["guild_id"] != guild_id or record.get("status") != "open" or record.get("case_kind") != "review":
                continue
            submission = self.submissions.get(record["submission_id"])
            if submission is None or submission.get("status") != "queued":
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda item: item.get("created_at") or "")
        return rows[:limit]

    async def fetch_case(self, guild_id: int, case_id: str) -> dict[str, Any] | None:
        record = self.cases.get((guild_id, case_id))
        return deepcopy(record) if record is not None else None

    async def upsert_case(self, record: dict[str, Any]):
        normalized = normalize_case(record)
        if normalized is not None:
            self.cases[(normalized["guild_id"], normalized["case_id"])] = normalized

    async def upsert_author_link(self, record: dict[str, Any]):
        normalized = normalize_author_link(record)
        if normalized is not None:
            self.author_links[normalized["submission_id"]] = normalized

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        record = self.author_links.get(submission_id)
        return deepcopy(record) if record is not None else None

    async def upsert_private_media(self, record: dict[str, Any]):
        normalized = normalize_private_media(record)
        if normalized is not None:
            self.private_media[normalized["submission_id"]] = normalized

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        record = self.private_media.get(submission_id)
        return deepcopy(record) if record is not None else None

    async def delete_private_media(self, submission_id: str):
        self.private_media.pop(submission_id, None)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        record = self.enforcement_states.get((guild_id, user_id))
        return deepcopy(record) if record is not None else None

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        normalized = normalize_enforcement_state(record)
        if normalized is not None:
            self.enforcement_states[(normalized["guild_id"], normalized["user_id"])] = normalized

    async def list_review_queues(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.review_queues.values()]
        rows.sort(key=lambda item: item.get("guild_id", 0))
        return rows

    async def fetch_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        record = self.review_queues.get(guild_id)
        return deepcopy(record) if record is not None else None

    async def list_review_surfaces(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        rows = []
        for record in self.cases.values():
            if record["guild_id"] != guild_id or record.get("status") != "open" or record.get("case_kind") != "review":
                continue
            submission = self.submissions.get(record["submission_id"])
            if submission is None or submission.get("status") != "queued":
                continue
            rows.append(
                {
                    "case_id": record["case_id"],
                    "confession_id": submission["confession_id"],
                    "case_kind": record["case_kind"],
                    "status": record["status"],
                    "review_version": int(record.get("review_version") or 0),
                    "staff_preview": submission.get("staff_preview"),
                    "flag_codes": list(submission.get("flag_codes") or ()),
                    "attachment_meta": deepcopy(submission.get("attachment_meta") or []),
                    "shared_link_url": submission.get("shared_link_url"),
                    "created_at": submission.get("created_at"),
                }
            )
        rows.sort(key=lambda item: item.get("created_at") or "")
        return rows[:limit]

    async def upsert_review_queue(self, record: dict[str, Any]):
        normalized = normalize_review_queue(record)
        if normalized is not None:
            self.review_queues[int(normalized["guild_id"])] = normalized

    async def delete_review_queue(self, guild_id: int):
        self.review_queues.pop(guild_id, None)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        submissions = [record for record in self.submissions.values() if record["guild_id"] == guild_id]
        cases = [record for record in self.cases.values() if record["guild_id"] == guild_id]
        return {
            "queued_submissions": sum(record.get("status") == "queued" for record in submissions),
            "published_submissions": sum(record.get("status") == "published" for record in submissions),
            "blocked_submissions": sum(record.get("status") == "blocked" for record in submissions),
            "open_cases_total": sum(record.get("status") == "open" for record in cases),
            "open_review_cases": sum(
                record.get("status") == "open" and record.get("case_kind") == "review" for record in cases
            ),
            "open_safety_cases": sum(
                record.get("status") == "open" and record.get("case_kind") == "safety_block" for record in cases
            ),
            "open_moderation_cases": sum(
                record.get("status") == "open" and record.get("case_kind") == "published_moderation" for record in cases
            ),
        }


class _PostgresConfessionsStore(_BaseConfessionsStore):
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
            raise ConfessionsStorageUnavailable("asyncpg is not installed, so Babblebox confessions storage is unavailable.") from exc
        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=2,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-confessions-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise ConfessionsStorageUnavailable(f"Could not connect to Babblebox confessions storage: {last_error}") from last_error

    async def _ensure_schema(self):
        table_statements = [
            (
                "CREATE TABLE IF NOT EXISTS confession_guild_configs ("
                "guild_id BIGINT PRIMARY KEY, "
                "enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "confession_channel_id BIGINT NULL, "
                "panel_channel_id BIGINT NULL, "
                "panel_message_id BIGINT NULL, "
                "review_channel_id BIGINT NULL, "
                "review_mode BOOLEAN NOT NULL DEFAULT TRUE, "
                "block_adult_language BOOLEAN NOT NULL DEFAULT TRUE, "
                "allow_trusted_mainstream_links BOOLEAN NOT NULL DEFAULT TRUE, "
                "custom_allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "custom_block_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_images BOOLEAN NOT NULL DEFAULT FALSE, "
                "max_images SMALLINT NOT NULL DEFAULT 3, "
                "cooldown_seconds INTEGER NOT NULL DEFAULT 300, "
                "burst_limit SMALLINT NOT NULL DEFAULT 3, "
                "burst_window_seconds INTEGER NOT NULL DEFAULT 1800, "
                "auto_suspend_hours INTEGER NOT NULL DEFAULT 12, "
                "strike_temp_ban_threshold SMALLINT NOT NULL DEFAULT 3, "
                "temp_ban_days SMALLINT NOT NULL DEFAULT 7, "
                "strike_perm_ban_threshold SMALLINT NOT NULL DEFAULT 5, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_submissions ("
                "submission_id TEXT PRIMARY KEY, "
                "guild_id BIGINT NOT NULL, "
                "confession_id TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "review_status TEXT NOT NULL DEFAULT 'none', "
                "staff_preview TEXT NULL, "
                "content_body TEXT NULL, "
                "shared_link_url TEXT NULL, "
                "content_fingerprint TEXT NULL, "
                "similarity_key TEXT NULL, "
                "flag_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "attachment_meta JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "posted_channel_id BIGINT NULL, "
                "posted_message_id BIGINT NULL, "
                "current_case_id TEXT NULL, "
                "created_at TIMESTAMPTZ NOT NULL, "
                "published_at TIMESTAMPTZ NULL, "
                "resolved_at TIMESTAMPTZ NULL"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_author_links ("
                "submission_id TEXT PRIMARY KEY REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "guild_id BIGINT NOT NULL, "
                "author_user_id BIGINT NOT NULL, "
                "created_at TIMESTAMPTZ NOT NULL"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_private_media ("
                "submission_id TEXT PRIMARY KEY REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "guild_id BIGINT NOT NULL, "
                "attachment_urls JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "created_at TIMESTAMPTZ NOT NULL, "
                "updated_at TIMESTAMPTZ NOT NULL"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_enforcement_states ("
                "guild_id BIGINT NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "active_restriction TEXT NOT NULL DEFAULT 'none', "
                "restricted_until TIMESTAMPTZ NULL, "
                "is_permanent_ban BOOLEAN NOT NULL DEFAULT FALSE, "
                "strike_count INTEGER NOT NULL DEFAULT 0, "
                "last_strike_at TIMESTAMPTZ NULL, "
                "cooldown_until TIMESTAMPTZ NULL, "
                "burst_count INTEGER NOT NULL DEFAULT 0, "
                "burst_window_started_at TIMESTAMPTZ NULL, "
                "last_case_id TEXT NULL, "
                "updated_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_cases ("
                "case_id TEXT NOT NULL, "
                "guild_id BIGINT NOT NULL, "
                "submission_id TEXT NOT NULL REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "confession_id TEXT NOT NULL, "
                "case_kind TEXT NOT NULL, "
                "status TEXT NOT NULL, "
                "reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "review_version INTEGER NOT NULL DEFAULT 0, "
                "resolution_action TEXT NULL, "
                "resolution_note TEXT NULL, "
                "review_message_channel_id BIGINT NULL, "
                "review_message_id BIGINT NULL, "
                "created_at TIMESTAMPTZ NOT NULL, "
                "resolved_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, case_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS confession_review_queues ("
                "guild_id BIGINT PRIMARY KEY, "
                "channel_id BIGINT NULL, "
                "message_id BIGINT NULL, "
                "updated_at TIMESTAMPTZ NULL"
                ")"
            ),
        ]
        alter_statements = [
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS review_mode BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS block_adult_language BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_trusted_mainstream_links BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS custom_allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS custom_block_domains JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_images BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS max_images SMALLINT NOT NULL DEFAULT 3",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS panel_channel_id BIGINT NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS panel_message_id BIGINT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'none'",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS content_body TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS shared_link_url TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS content_fingerprint TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS similarity_key TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS flag_codes JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS attachment_meta JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS current_case_id TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS resolution_action TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS resolution_note TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL",
            "ALTER TABLE confession_guild_configs ALTER COLUMN allow_images SET DEFAULT FALSE",
            (
                "UPDATE confession_guild_configs "
                "SET allow_images = FALSE "
                "WHERE allow_images = TRUE AND (review_channel_id IS NULL OR review_channel_id = confession_channel_id)"
            ),
        ]
        index_statements = [
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_confession_submissions_confession_id ON confession_submissions (guild_id, confession_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_status_created ON confession_submissions (guild_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_message_id ON confession_submissions (guild_id, posted_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_cases_status_created ON confession_cases (guild_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_cases_submission_id ON confession_cases (submission_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_author_links_author_created ON confession_author_links (guild_id, author_user_id, created_at DESC)",
        ]
        async with self._pool.acquire() as conn:
            for statement in table_statements:
                await conn.execute(statement)
            for statement in alter_statements:
                await conn.execute(statement)
            for statement in index_statements:
                await conn.execute(statement)

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM confession_guild_configs")
        results: dict[int, dict[str, Any]] = {}
        for row in rows:
            record = _config_from_row(row)
            if record is not None:
                results[int(row["guild_id"])] = record
        return results

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM confession_guild_configs WHERE guild_id = $1", guild_id)
        return _config_from_row(row)

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_confession_config(int(config["guild_id"]), deepcopy(config))
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_guild_configs ("
                        "guild_id, enabled, confession_channel_id, panel_channel_id, panel_message_id, review_channel_id, review_mode, block_adult_language, "
                        "allow_trusted_mainstream_links, custom_allow_domains, custom_block_domains, allow_images, max_images, "
                        "cooldown_seconds, burst_limit, burst_window_seconds, auto_suspend_hours, strike_temp_ban_threshold, temp_ban_days, strike_perm_ban_threshold, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, "
                        "$9, $10::jsonb, $11::jsonb, $12, $13, "
                        "$14, $15, $16, $17, $18, $19, $20, timezone('utc', now())"
                        ") "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "enabled = EXCLUDED.enabled, "
                        "confession_channel_id = EXCLUDED.confession_channel_id, "
                        "panel_channel_id = EXCLUDED.panel_channel_id, "
                        "panel_message_id = EXCLUDED.panel_message_id, "
                        "review_channel_id = EXCLUDED.review_channel_id, "
                        "review_mode = EXCLUDED.review_mode, "
                        "block_adult_language = EXCLUDED.block_adult_language, "
                        "allow_trusted_mainstream_links = EXCLUDED.allow_trusted_mainstream_links, "
                        "custom_allow_domains = EXCLUDED.custom_allow_domains, "
                        "custom_block_domains = EXCLUDED.custom_block_domains, "
                        "allow_images = EXCLUDED.allow_images, "
                        "max_images = EXCLUDED.max_images, "
                        "cooldown_seconds = EXCLUDED.cooldown_seconds, "
                        "burst_limit = EXCLUDED.burst_limit, "
                        "burst_window_seconds = EXCLUDED.burst_window_seconds, "
                        "auto_suspend_hours = EXCLUDED.auto_suspend_hours, "
                        "strike_temp_ban_threshold = EXCLUDED.strike_temp_ban_threshold, "
                        "temp_ban_days = EXCLUDED.temp_ban_days, "
                        "strike_perm_ban_threshold = EXCLUDED.strike_perm_ban_threshold, "
                        "updated_at = timezone('utc', now())"
                    ),
                    normalized["guild_id"],
                    normalized["enabled"],
                    normalized["confession_channel_id"],
                    normalized["panel_channel_id"],
                    normalized["panel_message_id"],
                    normalized["review_channel_id"],
                    normalized["review_mode"],
                    normalized["block_adult_language"],
                    normalized["allow_trusted_mainstream_links"],
                    json.dumps(normalized["custom_allow_domains"]),
                    json.dumps(normalized["custom_block_domains"]),
                    normalized["allow_images"],
                    normalized["max_images"],
                    normalized["cooldown_seconds"],
                    normalized["burst_limit"],
                    normalized["burst_window_seconds"],
                    normalized["auto_suspend_hours"],
                    normalized["strike_temp_ban_threshold"],
                    normalized["temp_ban_days"],
                    normalized["strike_perm_ban_threshold"],
                )

    async def upsert_submission(self, record: dict[str, Any]):
        normalized = normalize_submission(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_submissions ("
                        "submission_id, guild_id, confession_id, status, review_status, staff_preview, content_body, shared_link_url, content_fingerprint, similarity_key, "
                        "flag_codes, attachment_meta, posted_channel_id, posted_message_id, current_case_id, created_at, published_at, resolved_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, "
                        "$11::jsonb, $12::jsonb, $13, $14, $15, $16, $17, $18"
                        ") "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "status = EXCLUDED.status, "
                        "review_status = EXCLUDED.review_status, "
                        "staff_preview = EXCLUDED.staff_preview, "
                        "content_body = EXCLUDED.content_body, "
                        "shared_link_url = EXCLUDED.shared_link_url, "
                        "content_fingerprint = EXCLUDED.content_fingerprint, "
                        "similarity_key = EXCLUDED.similarity_key, "
                        "flag_codes = EXCLUDED.flag_codes, "
                        "attachment_meta = EXCLUDED.attachment_meta, "
                        "posted_channel_id = EXCLUDED.posted_channel_id, "
                        "posted_message_id = EXCLUDED.posted_message_id, "
                        "current_case_id = EXCLUDED.current_case_id, "
                        "published_at = EXCLUDED.published_at, "
                        "resolved_at = EXCLUDED.resolved_at"
                    ),
                    normalized["submission_id"],
                    normalized["guild_id"],
                    normalized["confession_id"],
                    normalized["status"],
                    normalized["review_status"],
                    normalized["staff_preview"],
                    normalized["content_body"],
                    normalized["shared_link_url"],
                    normalized["content_fingerprint"],
                    normalized["similarity_key"],
                    json.dumps(normalized["flag_codes"]),
                    json.dumps(normalized["attachment_meta"]),
                    normalized["posted_channel_id"],
                    normalized["posted_message_id"],
                    normalized["current_case_id"],
                    _parse_datetime(normalized["created_at"]),
                    _parse_datetime(normalized["published_at"]),
                    _parse_datetime(normalized["resolved_at"]),
                )

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM confession_submissions WHERE submission_id = $1", submission_id)
        return _submission_from_row(row)

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_submissions WHERE guild_id = $1 AND confession_id = $2",
                guild_id,
                confession_id,
            )
        return _submission_from_row(row)

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_submissions WHERE guild_id = $1 AND posted_message_id = $2",
                guild_id,
                message_id,
            )
        return _submission_from_row(row)

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT s.* "
                    "FROM confession_author_links a "
                    "JOIN confession_submissions s ON s.submission_id = a.submission_id "
                    "WHERE a.guild_id = $1 AND a.author_user_id = $2 "
                    "ORDER BY s.created_at DESC LIMIT $3"
                ),
                guild_id,
                author_user_id,
                limit,
            )
        return [record for row in rows if (record := _submission_from_row(row)) is not None]

    async def list_review_cases(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT c.* "
                    "FROM confession_cases c "
                    "JOIN confession_submissions s ON s.submission_id = c.submission_id "
                    "WHERE c.guild_id = $1 AND c.status = 'open' AND c.case_kind = 'review' AND s.status = 'queued' "
                    "ORDER BY c.created_at ASC LIMIT $2"
                ),
                guild_id,
                limit,
            )
        return [record for row in rows if (record := _case_from_row(row)) is not None]

    async def fetch_case(self, guild_id: int, case_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM confession_cases WHERE guild_id = $1 AND case_id = $2", guild_id, case_id)
        return _case_from_row(row)

    async def upsert_case(self, record: dict[str, Any]):
        normalized = normalize_case(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_cases ("
                        "case_id, guild_id, submission_id, confession_id, case_kind, status, reason_codes, review_version, resolution_action, resolution_note, "
                        "review_message_channel_id, review_message_id, created_at, resolved_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, "
                        "$11, $12, $13, $14"
                        ") "
                        "ON CONFLICT (guild_id, case_id) DO UPDATE SET "
                        "submission_id = EXCLUDED.submission_id, "
                        "confession_id = EXCLUDED.confession_id, "
                        "case_kind = EXCLUDED.case_kind, "
                        "status = EXCLUDED.status, "
                        "reason_codes = EXCLUDED.reason_codes, "
                        "review_version = EXCLUDED.review_version, "
                        "resolution_action = EXCLUDED.resolution_action, "
                        "resolution_note = EXCLUDED.resolution_note, "
                        "review_message_channel_id = EXCLUDED.review_message_channel_id, "
                        "review_message_id = EXCLUDED.review_message_id, "
                        "resolved_at = EXCLUDED.resolved_at"
                    ),
                    normalized["case_id"],
                    normalized["guild_id"],
                    normalized["submission_id"],
                    normalized["confession_id"],
                    normalized["case_kind"],
                    normalized["status"],
                    json.dumps(normalized["reason_codes"]),
                    normalized["review_version"],
                    normalized["resolution_action"],
                    normalized["resolution_note"],
                    normalized["review_message_channel_id"],
                    normalized["review_message_id"],
                    _parse_datetime(normalized["created_at"]),
                    _parse_datetime(normalized["resolved_at"]),
                )

    async def upsert_author_link(self, record: dict[str, Any]):
        normalized = normalize_author_link(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_author_links (submission_id, guild_id, author_user_id, created_at) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "guild_id = EXCLUDED.guild_id, author_user_id = EXCLUDED.author_user_id, created_at = EXCLUDED.created_at"
                    ),
                    normalized["submission_id"],
                    normalized["guild_id"],
                    normalized["author_user_id"],
                    _parse_datetime(normalized["created_at"]),
                )

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT submission_id, guild_id, author_user_id, created_at FROM confession_author_links WHERE submission_id = $1",
                submission_id,
            )
        return _author_link_from_row(row)

    async def upsert_private_media(self, record: dict[str, Any]):
        normalized = normalize_private_media(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_private_media (submission_id, guild_id, attachment_urls, created_at, updated_at) "
                        "VALUES ($1, $2, $3::jsonb, $4, $5) "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "guild_id = EXCLUDED.guild_id, "
                        "attachment_urls = EXCLUDED.attachment_urls, "
                        "created_at = EXCLUDED.created_at, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["submission_id"],
                    normalized["guild_id"],
                    json.dumps(normalized["attachment_urls"]),
                    _parse_datetime(normalized["created_at"]),
                    _parse_datetime(normalized["updated_at"]),
                )

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT submission_id, guild_id, attachment_urls, created_at, updated_at FROM confession_private_media WHERE submission_id = $1",
                submission_id,
            )
        return _private_media_from_row(row)

    async def delete_private_media(self, submission_id: str):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM confession_private_media WHERE submission_id = $1", submission_id)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_enforcement_states WHERE guild_id = $1 AND user_id = $2",
                guild_id,
                user_id,
            )
        return _enforcement_from_row(row)

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        normalized = normalize_enforcement_state(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_enforcement_states ("
                        "guild_id, user_id, active_restriction, restricted_until, is_permanent_ban, strike_count, last_strike_at, cooldown_until, "
                        "burst_count, burst_window_started_at, last_case_id, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, "
                        "$9, $10, $11, $12"
                        ") "
                        "ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                        "active_restriction = EXCLUDED.active_restriction, "
                        "restricted_until = EXCLUDED.restricted_until, "
                        "is_permanent_ban = EXCLUDED.is_permanent_ban, "
                        "strike_count = EXCLUDED.strike_count, "
                        "last_strike_at = EXCLUDED.last_strike_at, "
                        "cooldown_until = EXCLUDED.cooldown_until, "
                        "burst_count = EXCLUDED.burst_count, "
                        "burst_window_started_at = EXCLUDED.burst_window_started_at, "
                        "last_case_id = EXCLUDED.last_case_id, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["guild_id"],
                    normalized["user_id"],
                    normalized["active_restriction"],
                    _parse_datetime(normalized["restricted_until"]),
                    normalized["is_permanent_ban"],
                    normalized["strike_count"],
                    _parse_datetime(normalized["last_strike_at"]),
                    _parse_datetime(normalized["cooldown_until"]),
                    normalized["burst_count"],
                    _parse_datetime(normalized["burst_window_started_at"]),
                    normalized["last_case_id"],
                    _parse_datetime(normalized["updated_at"]),
                )

    async def list_review_queues(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id, channel_id, message_id, updated_at FROM confession_review_queues ORDER BY guild_id ASC")
        return [record for row in rows if (record := _review_queue_from_row(row)) is not None]

    async def fetch_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT guild_id, channel_id, message_id, updated_at FROM confession_review_queues WHERE guild_id = $1",
                guild_id,
            )
        return _review_queue_from_row(row)

    async def list_review_surfaces(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT "
                    "c.case_id, c.case_kind, c.status, c.review_version, "
                    "s.confession_id, s.staff_preview, s.flag_codes, s.attachment_meta, s.shared_link_url, s.created_at "
                    "FROM confession_cases c "
                    "JOIN confession_submissions s ON s.submission_id = c.submission_id "
                    "WHERE c.guild_id = $1 AND c.status = 'open' AND c.case_kind = 'review' AND s.status = 'queued' "
                    "ORDER BY s.created_at ASC LIMIT $2"
                ),
                guild_id,
                limit,
            )
        surfaces = []
        for row in rows:
            surfaces.append(
                {
                    "case_id": row["case_id"],
                    "confession_id": row["confession_id"],
                    "case_kind": row["case_kind"],
                    "status": row["status"],
                    "review_version": int(row["review_version"] or 0),
                    "staff_preview": row["staff_preview"],
                    "flag_codes": decode_postgres_json_array(row["flag_codes"], label="confession_submissions.flag_codes"),
                    "attachment_meta": decode_postgres_json_array(row["attachment_meta"], label="confession_submissions.attachment_meta"),
                    "shared_link_url": row["shared_link_url"],
                    "created_at": _serialize_datetime(row["created_at"]),
                }
            )
        return surfaces

    async def upsert_review_queue(self, record: dict[str, Any]):
        normalized = normalize_review_queue(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_review_queues (guild_id, channel_id, message_id, updated_at) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "channel_id = EXCLUDED.channel_id, message_id = EXCLUDED.message_id, updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["guild_id"],
                    normalized["channel_id"],
                    normalized["message_id"],
                    _parse_datetime(normalized["updated_at"]),
                )

    async def delete_review_queue(self, guild_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM confession_review_queues WHERE guild_id = $1", guild_id)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            submission_rows = await conn.fetch(
                "SELECT status, COUNT(*) AS count FROM confession_submissions WHERE guild_id = $1 GROUP BY status",
                guild_id,
            )
            case_rows = await conn.fetch(
                "SELECT case_kind, status, COUNT(*) AS count FROM confession_cases WHERE guild_id = $1 GROUP BY case_kind, status",
                guild_id,
            )
        submission_counts = {str(row['status']): int(row['count']) for row in submission_rows}
        open_case_counts = {
            str(row["case_kind"]): int(row["count"])
            for row in case_rows
            if str(row["status"]) == "open"
        }
        return {
            "queued_submissions": submission_counts.get("queued", 0),
            "published_submissions": submission_counts.get("published", 0),
            "blocked_submissions": submission_counts.get("blocked", 0),
            "open_cases_total": sum(open_case_counts.values()),
            "open_review_cases": open_case_counts.get("review", 0),
            "open_safety_cases": open_case_counts.get("safety_block", 0),
            "open_moderation_cases": open_case_counts.get("published_moderation", 0),
        }


class ConfessionsStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        requested_backend = (backend or os.getenv("CONFESSIONS_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.backend_name = requested_backend
        self._store: _BaseConfessionsStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        print(
            "Confessions storage init: "
            f"backend_preference={requested_backend}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source or 'none'}"
        )
        if requested_backend in {"memory", "test", "dev"}:
            self._store = _MemoryConfessionsStore()
        elif requested_backend in {"postgres", "postgresql", "supabase", "auto"}:
            if not self.database_url:
                raise ConfessionsStorageUnavailable(
                    "No Postgres confessions database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL."
                )
            self._store = _PostgresConfessionsStore(self.database_url)
        else:
            raise ConfessionsStorageUnavailable(f"Unsupported confessions storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        print(f"Confessions storage init succeeded: backend={self.backend_name}")

    async def load(self):
        if self._store is None:
            raise ConfessionsStorageUnavailable("Confessions storage was not initialized.")
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

    async def upsert_submission(self, record: dict[str, Any]):
        await self._store.upsert_submission(record)

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_submission(submission_id)

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_submission_by_confession_id(guild_id, confession_id)

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_submission_by_message_id(guild_id, message_id)

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        return await self._store.list_recent_submissions_for_author(guild_id, author_user_id, limit=limit)

    async def list_review_cases(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        return await self._store.list_review_cases(guild_id, limit=limit)

    async def fetch_case(self, guild_id: int, case_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_case(guild_id, case_id)

    async def upsert_case(self, record: dict[str, Any]):
        await self._store.upsert_case(record)

    async def upsert_author_link(self, record: dict[str, Any]):
        await self._store.upsert_author_link(record)

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_author_link(submission_id)

    async def upsert_private_media(self, record: dict[str, Any]):
        await self._store.upsert_private_media(record)

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_private_media(submission_id)

    async def delete_private_media(self, submission_id: str):
        await self._store.delete_private_media(submission_id)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_enforcement_state(guild_id, user_id)

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        await self._store.upsert_enforcement_state(record)

    async def list_review_queues(self) -> list[dict[str, Any]]:
        return await self._store.list_review_queues()

    async def fetch_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_review_queue(guild_id)

    async def list_review_surfaces(self, guild_id: int, *, limit: int = 25) -> list[dict[str, Any]]:
        return await self._store.list_review_surfaces(guild_id, limit=limit)

    async def upsert_review_queue(self, record: dict[str, Any]):
        await self._store.upsert_review_queue(record)

    async def delete_review_queue(self, guild_id: int):
        await self._store.delete_review_queue(guild_id)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        return await self._store.fetch_guild_counts(guild_id)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
