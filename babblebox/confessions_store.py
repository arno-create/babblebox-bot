from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.confessions_crypto import ConfessionsCrypto, ConfessionsCryptoError, ConfessionsKeyConfigError
from babblebox.confessions_privacy import build_duplicate_signals
from babblebox.premium_limits import LIMIT_CONFESSIONS_MAX_IMAGES, storage_ceiling as premium_storage_ceiling
from babblebox.postgres_json import decode_postgres_json_array
from babblebox.text_safety import normalize_plain_text


LOGGER = logging.getLogger(__name__)


DEFAULT_BACKEND = "postgres"
DEFAULT_DATABASE_URL_ENV_ORDER = ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")
DISCORD_MEDIA_HOSTS = frozenset({"cdn.discordapp.com", "media.discordapp.net"})
VALID_RESTRICTIONS = {"none", "suspended", "temp_ban", "perm_ban"}
VALID_SUBMISSION_STATUSES = {"blocked", "queued", "published", "denied", "deleted", "overridden"}
VALID_REVIEW_STATUSES = {"none", "pending", "approved", "denied", "overridden", "blocked", "withdrawn"}
VALID_SUBMISSION_KINDS = {"confession", "reply"}
VALID_REPLY_FLOWS = {"reply_to_confession", "owner_reply_to_user"}
VALID_CASE_KINDS = {"review", "safety_block", "published_moderation"}
CONFESSION_IMAGE_STORAGE_LIMIT = premium_storage_ceiling(LIMIT_CONFESSIONS_MAX_IMAGES, 3)
VALID_CASE_STATUSES = {"open", "resolved"}
VALID_SUPPORT_TICKET_KINDS = {"appeal", "report"}
VALID_SUPPORT_TICKET_STATUSES = {"open", "resolved"}
VALID_OWNER_REPLY_OPPORTUNITY_STATUSES = {"pending", "locked", "used", "dismissed", "expired"}
VALID_OWNER_REPLY_NOTIFICATION_STATUSES = {"none", "sent", "failed", "cooldown"}
VALID_LINK_POLICY_MODES = {"disabled", "trusted_only", "allow_all_safe"}
DEFAULT_LINK_POLICY_MODE = "trusted_only"
PROTECTED_OWNER_REPLY_NAME = "Protected member"
PROTECTED_OWNER_REPLY_PREVIEW = "[protected]"
SECURE_AUTHOR_LINK_TABLE = "confession_author_identities"
SECURE_ENFORCEMENT_TABLE = "confession_enforcement_states_secure"
PRIVACY_CATEGORY_LABELS = {
    "plaintext_submission_content": "Plaintext submission content still exists",
    "legacy_duplicate_fields": "Legacy duplicate fields still exist",
    "plaintext_private_media": "Plaintext private-media URLs still exist",
    "legacy_author_links": "Legacy author-link rows still exist",
    "plaintext_owner_reply_rows": "Owner-reply private fields still exist in plaintext",
    "legacy_enforcement_rows": "Legacy enforcement rows still exist",
    "stale_key_rows": "Legacy key material is still required for some rows",
}
PRIVACY_CATEGORY_ORDER = tuple(PRIVACY_CATEGORY_LABELS)


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
        "appeals_channel_id": None,
        "review_mode": True,
        "block_adult_language": True,
        "link_policy_mode": DEFAULT_LINK_POLICY_MODE,
        "allow_trusted_mainstream_links": True,
        "custom_allow_domains": [],
        "custom_block_domains": [],
        "allowed_role_ids": [],
        "blocked_role_ids": [],
        "allow_images": False,
        "image_review_required": False,
        "allow_anonymous_replies": False,
        "anonymous_reply_review_required": False,
        "allow_owner_replies": True,
        "owner_reply_review_mode": False,
        "allow_self_edit": False,
        "max_images": 3,
        "cooldown_seconds": 5 * 60,
        "burst_limit": 3,
        "burst_window_seconds": 30 * 60,
        "auto_moderation_exempt_admins": True,
        "auto_moderation_exempt_role_ids": [],
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
        "image_restriction_active": False,
        "image_restricted_until": None,
        "image_restriction_case_id": None,
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


def _clean_int_list(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    cleaned = {
        int(value)
        for value in values
        if isinstance(value, int) and value > 0
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


def _normalize_link_policy_mode(value: Any, *, legacy_allow_trusted_links: Any = None) -> str:
    cleaned = normalize_plain_text(value).casefold() if value is not None else ""
    if cleaned in VALID_LINK_POLICY_MODES:
        return cleaned
    if legacy_allow_trusted_links is not None:
        return DEFAULT_LINK_POLICY_MODE if bool(legacy_allow_trusted_links) else "disabled"
    return DEFAULT_LINK_POLICY_MODE


def normalize_confession_config(guild_id: int, payload: Any) -> dict[str, Any]:
    cleaned = default_confession_config(guild_id)
    if not isinstance(payload, dict):
        return cleaned
    cleaned["enabled"] = bool(payload.get("enabled"))
    cleaned["confession_channel_id"] = _clean_int(payload.get("confession_channel_id"))
    cleaned["panel_channel_id"] = _clean_int(payload.get("panel_channel_id"))
    cleaned["panel_message_id"] = _clean_int(payload.get("panel_message_id"))
    cleaned["review_channel_id"] = _clean_int(payload.get("review_channel_id"))
    cleaned["appeals_channel_id"] = _clean_int(payload.get("appeals_channel_id"))
    cleaned["review_mode"] = bool(payload.get("review_mode", True))
    cleaned["block_adult_language"] = bool(payload.get("block_adult_language", True))
    cleaned["link_policy_mode"] = _normalize_link_policy_mode(
        payload.get("link_policy_mode"),
        legacy_allow_trusted_links=payload.get("allow_trusted_mainstream_links"),
    )
    cleaned["allow_trusted_mainstream_links"] = cleaned["link_policy_mode"] != "disabled"
    cleaned["custom_allow_domains"] = _clean_domain_list(payload.get("custom_allow_domains"))
    cleaned["custom_block_domains"] = _clean_domain_list(payload.get("custom_block_domains"))
    cleaned["allowed_role_ids"] = _clean_int_list(payload.get("allowed_role_ids"))
    cleaned["blocked_role_ids"] = _clean_int_list(payload.get("blocked_role_ids"))
    cleaned["allow_images"] = bool(payload.get("allow_images", False))
    if "image_review_required" in payload:
        cleaned["image_review_required"] = bool(payload.get("image_review_required"))
    else:
        cleaned["image_review_required"] = cleaned["allow_images"]
    cleaned["allow_anonymous_replies"] = bool(payload.get("allow_anonymous_replies", False))
    if "anonymous_reply_review_required" in payload:
        cleaned["anonymous_reply_review_required"] = bool(payload.get("anonymous_reply_review_required"))
    else:
        cleaned["anonymous_reply_review_required"] = cleaned["allow_anonymous_replies"]
    cleaned["allow_owner_replies"] = bool(payload.get("allow_owner_replies", True))
    cleaned["owner_reply_review_mode"] = bool(payload.get("owner_reply_review_mode", False))
    cleaned["allow_self_edit"] = bool(payload.get("allow_self_edit", False))
    if not cleaned["allow_images"]:
        cleaned["image_review_required"] = False
    if not cleaned["allow_anonymous_replies"]:
        cleaned["anonymous_reply_review_required"] = False
    if not cleaned["allow_owner_replies"]:
        cleaned["owner_reply_review_mode"] = False
    cleaned["auto_moderation_exempt_admins"] = bool(payload.get("auto_moderation_exempt_admins", True))
    cleaned["auto_moderation_exempt_role_ids"] = _clean_int_list(payload.get("auto_moderation_exempt_role_ids"))
    max_images = payload.get("max_images")
    cleaned["max_images"] = (
        min(max_images, CONFESSION_IMAGE_STORAGE_LIMIT)
        if isinstance(max_images, int) and max_images >= 1
        else 3
    )
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
        cleaned["image_review_required"] = False
        cleaned["anonymous_reply_review_required"] = False
        cleaned["owner_reply_review_mode"] = False
    return cleaned


def normalize_submission(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    submission_id = _clean_optional_text(payload.get("submission_id"), max_length=64)
    confession_id = _clean_optional_text(payload.get("confession_id"), max_length=32)
    submission_kind = str(payload.get("submission_kind", "confession")).strip().lower()
    reply_flow = str(payload.get("reply_flow") or "").strip().lower()
    status = str(payload.get("status", "queued")).strip().lower()
    review_status = str(payload.get("review_status", "none")).strip().lower()
    created_at = _serialize_datetime(payload.get("created_at"))
    if guild_id is None or submission_id is None or confession_id is None or created_at is None:
        return None
    if (
        status not in VALID_SUBMISSION_STATUSES
        or review_status not in VALID_REVIEW_STATUSES
        or submission_kind not in VALID_SUBMISSION_KINDS
    ):
        return None
    if submission_kind == "reply":
        if reply_flow not in VALID_REPLY_FLOWS:
            reply_flow = "reply_to_confession"
        owner_reply_generation = payload.get("owner_reply_generation")
        if reply_flow == "owner_reply_to_user":
            if not isinstance(owner_reply_generation, int) or owner_reply_generation < 1:
                owner_reply_generation = 1
            elif owner_reply_generation > 2:
                owner_reply_generation = 2
        else:
            owner_reply_generation = None
    else:
        reply_flow = None
        owner_reply_generation = None
    discussion_thread_id = _clean_int(payload.get("discussion_thread_id")) if submission_kind == "confession" else None
    return {
        "submission_id": submission_id,
        "guild_id": guild_id,
        "confession_id": confession_id,
        "submission_kind": submission_kind,
        "reply_flow": reply_flow,
        "owner_reply_generation": owner_reply_generation,
        "parent_confession_id": _clean_optional_text(payload.get("parent_confession_id"), max_length=32),
        "reply_target_label": _clean_optional_text(payload.get("reply_target_label"), max_length=160),
        "reply_target_preview": _clean_optional_text(payload.get("reply_target_preview"), max_length=260),
        "status": status,
        "review_status": review_status,
        "staff_preview": _clean_optional_text(payload.get("staff_preview"), max_length=260),
        "content_body": _clean_optional_text(payload.get("content_body"), max_length=4000),
        "shared_link_url": _clean_optional_text(payload.get("shared_link_url"), max_length=500),
        "content_fingerprint": _clean_optional_text(payload.get("content_fingerprint"), max_length=160),
        "similarity_key": _clean_optional_text(payload.get("similarity_key"), max_length=160),
        "fuzzy_signature": _clean_optional_text(payload.get("fuzzy_signature"), max_length=96),
        "flag_codes": _clean_string_list(payload.get("flag_codes")),
        "attachment_meta": _clean_attachment_meta(payload.get("attachment_meta")),
        "posted_channel_id": _clean_int(payload.get("posted_channel_id")),
        "posted_message_id": _clean_int(payload.get("posted_message_id")),
        "discussion_thread_id": discussion_thread_id,
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


def normalize_owner_reply_opportunity(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    opportunity_id = _clean_optional_text(payload.get("opportunity_id"), max_length=64)
    root_submission_id = _clean_optional_text(payload.get("root_submission_id"), max_length=64)
    root_confession_id = _clean_optional_text(payload.get("root_confession_id"), max_length=32)
    referenced_submission_id = _clean_optional_text(payload.get("referenced_submission_id"), max_length=64)
    source_channel_id = _clean_int(payload.get("source_channel_id"))
    source_message_id = _clean_int(payload.get("source_message_id"))
    created_at = _serialize_datetime(payload.get("created_at"))
    expires_at = _serialize_datetime(payload.get("expires_at"))
    if (
        guild_id is None
        or opportunity_id is None
        or root_submission_id is None
        or root_confession_id is None
        or referenced_submission_id is None
        or source_channel_id is None
        or source_message_id is None
        or created_at is None
        or expires_at is None
    ):
        return None
    status = str(payload.get("status", "pending")).strip().lower()
    if status not in VALID_OWNER_REPLY_OPPORTUNITY_STATUSES:
        status = "pending"
    notification_status = str(payload.get("notification_status", "none")).strip().lower()
    if notification_status not in VALID_OWNER_REPLY_NOTIFICATION_STATUSES:
        notification_status = "none"
    return {
        "opportunity_id": opportunity_id,
        "guild_id": guild_id,
        "root_submission_id": root_submission_id,
        "root_confession_id": root_confession_id,
        "referenced_submission_id": referenced_submission_id,
        "source_channel_id": source_channel_id,
        "source_message_id": source_message_id,
        "source_author_user_id": _clean_int(payload.get("source_author_user_id")),
        "source_author_name": _clean_optional_text(payload.get("source_author_name"), max_length=120) or "A member",
        "source_preview": _clean_optional_text(payload.get("source_preview"), max_length=320) or "[message unavailable]",
        "source_message_fingerprint": _clean_optional_text(payload.get("source_message_fingerprint"), max_length=96),
        "status": status,
        "notification_status": notification_status,
        "notification_channel_id": _clean_int(payload.get("notification_channel_id")),
        "notification_message_id": _clean_int(payload.get("notification_message_id")),
        "created_at": created_at,
        "expires_at": expires_at,
        "notified_at": _serialize_datetime(payload.get("notified_at")),
        "resolved_at": _serialize_datetime(payload.get("resolved_at")),
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
        "image_restriction_active": bool(payload.get("image_restriction_active")),
        "image_restricted_until": _serialize_datetime(payload.get("image_restricted_until")),
        "image_restriction_case_id": _clean_optional_text(payload.get("image_restriction_case_id"), max_length=32),
        "updated_at": _serialize_datetime(payload.get("updated_at")),
    }


def _enforcement_state_requires_gate_cache(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if bool(payload.get("is_permanent_ban")):
        return True
    if bool(payload.get("image_restriction_active")):
        return True
    return str(payload.get("active_restriction") or "none").strip().lower() != "none"


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


def normalize_support_ticket(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = _clean_int(payload.get("guild_id"))
    ticket_id = _clean_optional_text(payload.get("ticket_id"), max_length=32)
    kind = str(payload.get("kind") or "").strip().lower()
    status = str(payload.get("status", "open")).strip().lower()
    created_at = _serialize_datetime(payload.get("created_at"))
    if guild_id is None or ticket_id is None or kind not in VALID_SUPPORT_TICKET_KINDS or status not in VALID_SUPPORT_TICKET_STATUSES:
        return None
    if created_at is None:
        return None
    return {
        "ticket_id": ticket_id,
        "guild_id": guild_id,
        "kind": kind,
        "action_target_id": _clean_optional_text(payload.get("action_target_id"), max_length=32),
        "reference_confession_id": _clean_optional_text(payload.get("reference_confession_id"), max_length=32),
        "reference_case_id": _clean_optional_text(payload.get("reference_case_id"), max_length=32),
        "context_label": _clean_optional_text(payload.get("context_label"), max_length=240),
        "details": _clean_optional_text(payload.get("details"), max_length=1800),
        "status": status,
        "resolution_action": _clean_optional_text(payload.get("resolution_action"), max_length=48),
        "message_channel_id": _clean_int(payload.get("message_channel_id")),
        "message_id": _clean_int(payload.get("message_id")),
        "created_at": created_at,
        "resolved_at": _serialize_datetime(payload.get("resolved_at")),
    }


def _submission_requires_sensitive_payload(record: dict[str, Any]) -> bool:
    status = str(record.get("status") or "")
    review_status = str(record.get("review_status") or "")
    return status in {"queued", "blocked"} or review_status in {"pending", "blocked"}


def _raw_json_array_length(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, list):
        return len(value)
    if isinstance(value, tuple):
        return len(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned or cleaned == "[]":
            return 0
        try:
            parsed = json.loads(cleaned)
        except ValueError:
            return 0
        return len(parsed) if isinstance(parsed, list) else 0
    return 0


def _has_sensitive_submission_plaintext(row: Any) -> bool:
    return any(
        _clean_optional_text(row.get(field), max_length=4000) is not None
        for field in ("staff_preview", "content_body", "shared_link_url", "reply_target_label", "reply_target_preview")
    )


def _submission_privacy_categories(row: Any, privacy: ConfessionsCrypto) -> set[str]:
    categories: set[str] = set()
    if _has_sensitive_submission_plaintext(row):
        categories.add("plaintext_submission_content")
    content_ciphertext = _clean_optional_text(row.get("content_ciphertext"), max_length=12000)
    if content_ciphertext is not None and (
        not privacy.is_versioned_envelope(content_ciphertext)
        or not privacy.envelope_is_active(content_ciphertext, key_domain="content")
    ):
        categories.add("stale_key_rows")
    exact_hash = _clean_optional_text(row.get("content_fingerprint"), max_length=160)
    if exact_hash is not None:
        if privacy.is_keyed_exact_hash(exact_hash):
            if not privacy.exact_duplicate_hash_is_active(exact_hash):
                categories.add("stale_key_rows")
        else:
            categories.add("legacy_duplicate_fields")
    similarity_key = _clean_optional_text(row.get("similarity_key"), max_length=160)
    if similarity_key is not None:
        categories.add("legacy_duplicate_fields")
    fuzzy_signature = _clean_optional_text(row.get("fuzzy_signature"), max_length=96)
    if fuzzy_signature is not None:
        if privacy.is_keyed_fuzzy_signature(fuzzy_signature):
            if not privacy.fuzzy_signature_is_active(fuzzy_signature):
                categories.add("stale_key_rows")
        else:
            categories.add("legacy_duplicate_fields")
    return categories


def _private_media_privacy_categories(row: Any, privacy: ConfessionsCrypto) -> set[str]:
    categories: set[str] = set()
    if _raw_json_array_length(row.get("attachment_urls")) > 0:
        categories.add("plaintext_private_media")
    attachment_payload = _clean_optional_text(row.get("attachment_payload"), max_length=12000)
    if attachment_payload is not None and (
        not privacy.is_versioned_envelope(attachment_payload)
        or not privacy.envelope_is_active(attachment_payload, key_domain="content")
    ):
        categories.add("stale_key_rows")
    return categories


def _author_link_privacy_categories(row: Any, privacy: ConfessionsCrypto) -> set[str]:
    categories: set[str] = set()
    author_lookup_hash = _clean_optional_text(row.get("author_lookup_hash"), max_length=160)
    author_ciphertext = _clean_optional_text(row.get("author_identity_ciphertext"), max_length=12000)
    if author_lookup_hash is None or not privacy.is_blind_index(author_lookup_hash) or not privacy.blind_index_is_active(author_lookup_hash):
        categories.add("stale_key_rows")
    if author_ciphertext is None or not privacy.is_versioned_envelope(author_ciphertext) or not privacy.envelope_is_active(
        author_ciphertext,
        key_domain="identity",
    ):
        categories.add("stale_key_rows")
    return categories


def _owner_reply_plaintext_present(row: Any) -> bool:
    author_name = _clean_optional_text(row.get("source_author_name"), max_length=120)
    source_preview = _clean_optional_text(row.get("source_preview"), max_length=320)
    return (
        _clean_int(row.get("source_author_user_id")) is not None
        or _clean_optional_text(row.get("source_message_fingerprint"), max_length=96) is not None
        or (author_name is not None and author_name != PROTECTED_OWNER_REPLY_NAME)
        or (source_preview is not None and source_preview != PROTECTED_OWNER_REPLY_PREVIEW)
        or _clean_optional_text(row.get("private_payload"), max_length=12000) is None
    )


def _owner_reply_privacy_categories(row: Any, privacy: ConfessionsCrypto) -> set[str]:
    categories: set[str] = set()
    if _owner_reply_plaintext_present(row):
        categories.add("plaintext_owner_reply_rows")
    lookup_hash = _clean_optional_text(row.get("source_author_lookup_hash"), max_length=160)
    if lookup_hash is not None and (not privacy.is_blind_index(lookup_hash) or not privacy.blind_index_is_active(lookup_hash)):
        categories.add("stale_key_rows")
    private_payload = _clean_optional_text(row.get("private_payload"), max_length=12000)
    if private_payload is not None and (
        not privacy.is_versioned_envelope(private_payload)
        or not privacy.envelope_is_active(private_payload, key_domain="content")
    ):
        categories.add("stale_key_rows")
    return categories


def _enforcement_privacy_categories(row: Any, privacy: ConfessionsCrypto) -> set[str]:
    categories: set[str] = set()
    lookup_hash = _clean_optional_text(row.get("user_lookup_hash"), max_length=160)
    identity_ciphertext = _clean_optional_text(row.get("user_identity_ciphertext"), max_length=12000)
    if lookup_hash is None or not privacy.is_blind_index(lookup_hash) or not privacy.blind_index_is_active(lookup_hash):
        categories.add("stale_key_rows")
    if identity_ciphertext is None or not privacy.is_versioned_envelope(identity_ciphertext) or not privacy.envelope_is_active(
        identity_ciphertext,
        key_domain="identity",
    ):
        categories.add("stale_key_rows")
    return categories


def _empty_privacy_status(*, scope: str, guild_id: int | None = None) -> dict[str, Any]:
    return {
        "scope": scope,
        "guild_id": guild_id,
        "state": "ready",
        "privacy_hardened": True,
        "needs_backfill": False,
        "categories": [],
        "category_counts": {name: 0 for name in PRIVACY_CATEGORY_ORDER},
    }


def _apply_privacy_categories(status: dict[str, Any], categories: set[str]) -> None:
    for category in categories:
        if category in status["category_counts"]:
            status["category_counts"][category] += 1


def _finalize_privacy_status(status: dict[str, Any]) -> dict[str, Any]:
    categories = [name for name in PRIVACY_CATEGORY_ORDER if int(status["category_counts"].get(name) or 0) > 0]
    status["categories"] = categories
    status["privacy_hardened"] = not categories
    status["needs_backfill"] = bool(categories)
    status["state"] = "ready" if not categories else "partial"
    return status


def _backfill_submission_duplicate_fields(
    privacy: ConfessionsCrypto,
    record: dict[str, Any],
) -> dict[str, Any]:
    updated = deepcopy(record)
    guild_id = int(record["guild_id"])
    can_rebuild = bool(
        normalize_plain_text(record.get("content_body"))
        or normalize_plain_text(record.get("shared_link_url"))
        or list(record.get("attachment_meta") or [])
    )
    clear_similarity_key = False
    if can_rebuild:
        signals = build_duplicate_signals(
            privacy,
            guild_id,
            str(record.get("content_body") or ""),
            list(record.get("attachment_meta") or []),
            record.get("shared_link_url"),
        )
        if signals.exact_hash is not None:
            updated["content_fingerprint"] = signals.exact_hash
        if signals.fuzzy_signature is not None:
            updated["fuzzy_signature"] = signals.fuzzy_signature
            clear_similarity_key = True
    else:
        exact_hash = _clean_optional_text(record.get("content_fingerprint"), max_length=160)
        if exact_hash is not None and not privacy.exact_duplicate_hash_is_active(exact_hash):
            transformed_exact = privacy.transform_legacy_exact_hash(exact_hash, guild_id=guild_id)
            if transformed_exact is not None:
                updated["content_fingerprint"] = transformed_exact
        fuzzy_signature = _clean_optional_text(record.get("fuzzy_signature"), max_length=96)
        if fuzzy_signature is not None:
            if privacy.fuzzy_signature_is_active(fuzzy_signature):
                clear_similarity_key = True
            else:
                transformed_fuzzy = privacy.transform_legacy_fuzzy_signature(fuzzy_signature, guild_id=guild_id)
                if transformed_fuzzy is not None:
                    updated["fuzzy_signature"] = transformed_fuzzy
                    clear_similarity_key = True
    if clear_similarity_key:
        updated["similarity_key"] = None
    return updated


def _decrypt_payload(
    privacy: ConfessionsCrypto,
    *,
    label: str,
    domain: str,
    aad_fields: dict[str, Any],
    envelope: str | None,
    key_domain: str,
) -> dict[str, Any] | None:
    cleaned = _clean_optional_text(envelope, max_length=12000)
    if cleaned is None:
        return None
    try:
        return privacy.decrypt_payload(
            domain=domain,
            aad_fields=aad_fields,
            envelope=cleaned,
            key_domain=key_domain,
        )
    except ConfessionsCryptoError as exc:
        raise ConfessionsStorageUnavailable(f"Confessions privacy error while reading {label}.") from exc


def _submission_from_row(row: Any, privacy: ConfessionsCrypto) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _decrypt_payload(
        privacy,
        label="submission content",
        domain="submission-content",
        aad_fields={
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "confession_id": row["confession_id"],
        },
        envelope=row.get("content_ciphertext"),
        key_domain="content",
    ) or {}
    return normalize_submission(
        {
            "submission_id": row["submission_id"],
            "guild_id": row["guild_id"],
            "confession_id": row["confession_id"],
            "submission_kind": row.get("submission_kind") or "confession",
            "reply_flow": row.get("reply_flow"),
            "owner_reply_generation": row.get("owner_reply_generation"),
            "parent_confession_id": row.get("parent_confession_id"),
            "reply_target_label": payload.get("reply_target_label", row.get("reply_target_label")),
            "reply_target_preview": payload.get("reply_target_preview", row.get("reply_target_preview")),
            "status": row["status"],
            "review_status": row["review_status"],
            "staff_preview": payload.get("staff_preview", row["staff_preview"]),
            "content_body": payload.get("content_body", row["content_body"]),
            "shared_link_url": payload.get("shared_link_url", row.get("shared_link_url")),
            "content_fingerprint": row["content_fingerprint"],
            "similarity_key": row.get("similarity_key"),
            "fuzzy_signature": row.get("fuzzy_signature"),
            "flag_codes": decode_postgres_json_array(row["flag_codes"], label="confession_submissions.flag_codes"),
            "attachment_meta": decode_postgres_json_array(row["attachment_meta"], label="confession_submissions.attachment_meta"),
            "posted_channel_id": row["posted_channel_id"],
            "posted_message_id": row["posted_message_id"],
            "discussion_thread_id": row.get("discussion_thread_id"),
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
            "appeals_channel_id": row.get("appeals_channel_id"),
            "review_mode": row["review_mode"],
            "block_adult_language": row["block_adult_language"],
            "link_policy_mode": row.get("link_policy_mode"),
            "allow_trusted_mainstream_links": row.get("allow_trusted_mainstream_links"),
            "custom_allow_domains": decode_postgres_json_array(
                row["custom_allow_domains"],
                label="confession_guild_configs.custom_allow_domains",
            ),
            "custom_block_domains": decode_postgres_json_array(
                row["custom_block_domains"],
                label="confession_guild_configs.custom_block_domains",
            ),
            "allowed_role_ids": decode_postgres_json_array(
                row.get("allowed_role_ids"),
                label="confession_guild_configs.allowed_role_ids",
            ),
            "blocked_role_ids": decode_postgres_json_array(
                row.get("blocked_role_ids"),
                label="confession_guild_configs.blocked_role_ids",
            ),
            "allow_images": row["allow_images"],
            "image_review_required": row.get("image_review_required"),
            "allow_anonymous_replies": row.get("allow_anonymous_replies"),
            "anonymous_reply_review_required": row.get("anonymous_reply_review_required"),
            "allow_owner_replies": row.get("allow_owner_replies"),
            "owner_reply_review_mode": row.get("owner_reply_review_mode"),
            "allow_self_edit": row.get("allow_self_edit"),
            "auto_moderation_exempt_admins": row.get("auto_moderation_exempt_admins"),
            "auto_moderation_exempt_role_ids": decode_postgres_json_array(
                row.get("auto_moderation_exempt_role_ids"),
                label="confession_guild_configs.auto_moderation_exempt_role_ids",
            ),
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


def _author_link_from_secure_row(row: Any, privacy: ConfessionsCrypto) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _decrypt_payload(
        privacy,
        label="author link",
        domain="author-link",
        aad_fields={
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
        },
        envelope=row.get("author_identity_ciphertext"),
        key_domain="identity",
    ) or {}
    return normalize_author_link(
        {
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "author_user_id": payload.get("author_user_id"),
            "created_at": row["created_at"],
        }
    )


def _author_link_from_legacy_row(row: Any) -> dict[str, Any] | None:
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


def _enforcement_from_secure_row(row: Any, privacy: ConfessionsCrypto) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _decrypt_payload(
        privacy,
        label="enforcement state",
        domain="enforcement-state",
        aad_fields={
            "guild_id": row["guild_id"],
            "user_lookup_hash": row["user_lookup_hash"],
        },
        envelope=row.get("user_identity_ciphertext"),
        key_domain="identity",
    ) or {}
    return normalize_enforcement_state(
        {
            "guild_id": row["guild_id"],
            "user_id": payload.get("user_id"),
            "active_restriction": row["active_restriction"],
            "restricted_until": row["restricted_until"],
            "is_permanent_ban": row["is_permanent_ban"],
            "strike_count": row["strike_count"],
            "last_strike_at": row["last_strike_at"],
            "cooldown_until": row["cooldown_until"],
            "burst_count": row["burst_count"],
            "burst_window_started_at": row["burst_window_started_at"],
            "last_case_id": row["last_case_id"],
            "image_restriction_active": row.get("image_restriction_active"),
            "image_restricted_until": row.get("image_restricted_until"),
            "image_restriction_case_id": row.get("image_restriction_case_id"),
            "updated_at": row["updated_at"],
        }
    )


def _enforcement_from_legacy_row(row: Any) -> dict[str, Any] | None:
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
            "image_restriction_active": row.get("image_restriction_active"),
            "image_restricted_until": row.get("image_restricted_until"),
            "image_restriction_case_id": row.get("image_restriction_case_id"),
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


def _support_ticket_from_row(row: Any) -> dict[str, Any] | None:
    if row is None:
        return None
    return normalize_support_ticket(
        {
            "ticket_id": row["ticket_id"],
            "guild_id": row["guild_id"],
            "kind": row["kind"],
            "action_target_id": row.get("action_target_id"),
            "reference_confession_id": row.get("reference_confession_id"),
            "reference_case_id": row.get("reference_case_id"),
            "context_label": row.get("context_label"),
            "details": row.get("details"),
            "status": row.get("status"),
            "resolution_action": row.get("resolution_action"),
            "message_channel_id": row.get("message_channel_id"),
            "message_id": row.get("message_id"),
            "created_at": row.get("created_at"),
            "resolved_at": row.get("resolved_at"),
        }
    )


def _private_media_from_row(row: Any, privacy: ConfessionsCrypto) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _decrypt_payload(
        privacy,
        label="private media",
        domain="private-media",
        aad_fields={
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
        },
        envelope=row.get("attachment_payload"),
        key_domain="content",
    ) or {}
    return normalize_private_media(
        {
            "guild_id": row["guild_id"],
            "submission_id": row["submission_id"],
            "attachment_urls": payload.get(
                "attachment_urls",
                decode_postgres_json_array(row["attachment_urls"], label="confession_private_media.attachment_urls"),
            ),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }
    )


def _owner_reply_opportunity_from_row(row: Any, privacy: ConfessionsCrypto) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = _decrypt_payload(
        privacy,
        label="owner reply opportunity",
        domain="owner-reply-opportunity",
        aad_fields={
            "guild_id": row["guild_id"],
            "opportunity_id": row["opportunity_id"],
            "root_submission_id": row["root_submission_id"],
        },
        envelope=row.get("private_payload"),
        key_domain="content",
    ) or {}
    return normalize_owner_reply_opportunity(
        {
            "opportunity_id": row["opportunity_id"],
            "guild_id": row["guild_id"],
            "root_submission_id": row["root_submission_id"],
            "root_confession_id": row["root_confession_id"],
            "referenced_submission_id": row["referenced_submission_id"],
            "source_channel_id": row["source_channel_id"],
            "source_message_id": row["source_message_id"],
            "source_author_user_id": payload.get("source_author_user_id", row.get("source_author_user_id")),
            "source_author_name": payload.get("source_author_name", row["source_author_name"]),
            "source_preview": payload.get("source_preview", row["source_preview"]),
            "source_message_fingerprint": payload.get("source_message_fingerprint", row.get("source_message_fingerprint")),
            "status": row["status"],
            "notification_status": row["notification_status"],
            "notification_channel_id": row.get("notification_channel_id"),
            "notification_message_id": row["notification_message_id"],
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "notified_at": row["notified_at"],
            "resolved_at": row["resolved_at"],
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

    async def list_published_top_level_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_published_public_reply_submissions(self, guild_id: int) -> list[dict[str, Any]]:
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

    async def upsert_owner_reply_opportunity(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_owner_reply_opportunity_by_source_message_id(self, guild_id: int, source_message_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_owner_reply_opportunity_by_notification_message_id(self, notification_message_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def fetch_pending_owner_reply_opportunity_for_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_pending_owner_reply_opportunities_for_author(
        self,
        guild_id: int,
        author_user_id: int,
        *,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_owner_reply_opportunities_for_root_submission(
        self,
        root_submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_owner_reply_opportunities_for_responder_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_owner_reply_opportunities_for_source_author(
        self,
        guild_id: int,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def claim_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def release_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_owner_reply_opportunities_for_submission(
        self,
        submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        raise NotImplementedError

    async def list_active_enforcement_states(self, *, guild_id: int | None = None) -> list[dict[str, Any]]:
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

    async def fetch_support_ticket(self, guild_id: int, ticket_id: str) -> dict[str, Any] | None:
        raise NotImplementedError

    async def list_support_tickets(self, guild_id: int, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def upsert_support_ticket(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        raise NotImplementedError

    async def fetch_privacy_status(self, guild_id: int | None = None) -> dict[str, Any]:
        raise NotImplementedError

    async def run_privacy_backfill(self, *, apply: bool, batch_size: int = 100) -> dict[str, Any]:
        raise NotImplementedError


class _MemoryConfessionsStore(_BaseConfessionsStore):
    backend_name = "memory"

    def __init__(self, privacy: ConfessionsCrypto):
        self._privacy = privacy
        self.configs: dict[int, dict[str, Any]] = {}
        self.submissions: dict[str, dict[str, Any]] = {}
        self.author_links: dict[str, dict[str, Any]] = {}
        self.private_media: dict[str, dict[str, Any]] = {}
        self.owner_reply_opportunities: dict[str, dict[str, Any]] = {}
        self.enforcement_states: dict[tuple[int, int], dict[str, Any]] = {}
        self.secure_author_links: dict[str, dict[str, Any]] = {}
        self.secure_enforcement_states: dict[tuple[int, str], dict[str, Any]] = {}
        self.cases: dict[tuple[int, str], dict[str, Any]] = {}
        self.review_queues: dict[int, dict[str, Any]] = {}
        self.support_tickets: dict[tuple[int, str], dict[str, Any]] = {}

    async def load(self):
        self.configs = {}
        self.submissions = {}
        self.author_links = {}
        self.private_media = {}
        self.owner_reply_opportunities = {}
        self.enforcement_states = {}
        self.secure_author_links = {}
        self.secure_enforcement_states = {}
        self.cases = {}
        self.review_queues = {}
        self.support_tickets = {}

    def _encode_submission_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        payload = {
            "reply_target_label": normalized.get("reply_target_label"),
            "reply_target_preview": normalized.get("reply_target_preview"),
            "staff_preview": normalized.get("staff_preview"),
            "content_body": normalized.get("content_body"),
            "shared_link_url": normalized.get("shared_link_url"),
        }
        if any(value is not None for value in payload.values()):
            row["content_ciphertext"] = self._privacy.encrypt_payload(
                domain="submission-content",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                    "confession_id": normalized["confession_id"],
                },
                payload=payload,
                key_domain="content",
            )
            row["reply_target_label"] = None
            row["reply_target_preview"] = None
            row["staff_preview"] = None
            row["content_body"] = None
            row["shared_link_url"] = None
        else:
            row["content_ciphertext"] = None
        return row

    def _encode_author_link_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        return {
            "submission_id": normalized["submission_id"],
            "guild_id": normalized["guild_id"],
            "author_lookup_hash": self._privacy.blind_index(
                label="author-link",
                guild_id=normalized["guild_id"],
                value=normalized["author_user_id"],
            ),
            "author_identity_ciphertext": self._privacy.encrypt_payload(
                domain="author-link",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                },
                payload={"author_user_id": normalized["author_user_id"]},
                key_domain="identity",
            ),
            "created_at": normalized["created_at"],
        }

    def _encode_private_media_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        if normalized["attachment_urls"]:
            row["attachment_payload"] = self._privacy.encrypt_payload(
                domain="private-media",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                },
                payload={"attachment_urls": list(normalized["attachment_urls"])},
                key_domain="content",
            )
            row["attachment_urls"] = []
        else:
            row["attachment_payload"] = None
        return row

    def _encode_owner_reply_opportunity_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        payload = {
            "source_author_user_id": normalized.get("source_author_user_id"),
            "source_author_name": normalized.get("source_author_name"),
            "source_preview": normalized.get("source_preview"),
            "source_message_fingerprint": normalized.get("source_message_fingerprint"),
        }
        row["private_payload"] = self._privacy.encrypt_payload(
            domain="owner-reply-opportunity",
            aad_fields={
                "guild_id": normalized["guild_id"],
                "opportunity_id": normalized["opportunity_id"],
                "root_submission_id": normalized["root_submission_id"],
            },
            payload=payload,
            key_domain="content",
        )
        row["source_author_lookup_hash"] = (
            self._privacy.blind_index(
                label="owner-reply-source-author",
                guild_id=normalized["guild_id"],
                value=normalized["source_author_user_id"],
            )
            if normalized.get("source_author_user_id")
            else None
        )
        row["source_author_user_id"] = None
        row["source_author_name"] = PROTECTED_OWNER_REPLY_NAME
        row["source_preview"] = PROTECTED_OWNER_REPLY_PREVIEW
        row["source_message_fingerprint"] = None
        return row

    def _encode_enforcement_state_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        lookup_hash = self._privacy.blind_index(
            label="enforcement-state",
            guild_id=normalized["guild_id"],
            value=normalized["user_id"],
        )
        return {
            "guild_id": normalized["guild_id"],
            "user_lookup_hash": lookup_hash,
            "user_identity_ciphertext": self._privacy.encrypt_payload(
                domain="enforcement-state",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "user_lookup_hash": lookup_hash,
                },
                payload={"user_id": normalized["user_id"]},
                key_domain="identity",
            ),
            "active_restriction": normalized["active_restriction"],
            "restricted_until": normalized["restricted_until"],
            "is_permanent_ban": normalized["is_permanent_ban"],
            "strike_count": normalized["strike_count"],
            "last_strike_at": normalized["last_strike_at"],
            "cooldown_until": normalized["cooldown_until"],
            "burst_count": normalized["burst_count"],
            "burst_window_started_at": normalized["burst_window_started_at"],
            "last_case_id": normalized["last_case_id"],
            "image_restriction_active": normalized["image_restriction_active"],
            "image_restricted_until": normalized["image_restricted_until"],
            "image_restriction_case_id": normalized["image_restriction_case_id"],
            "updated_at": normalized["updated_at"],
        }

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
            self.submissions[normalized["submission_id"]] = self._encode_submission_row(normalized)

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        record = self.submissions.get(submission_id)
        return _submission_from_row(deepcopy(record), self._privacy) if record is not None else None

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        for record in self.submissions.values():
            if record["guild_id"] == guild_id and record["confession_id"] == confession_id:
                return _submission_from_row(deepcopy(record), self._privacy)
        return None

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        for record in self.submissions.values():
            if record["guild_id"] == guild_id and int(record.get("posted_message_id") or 0) == message_id:
                return _submission_from_row(deepcopy(record), self._privacy)
        return None

    async def list_published_top_level_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        rows = []
        for record in self.submissions.values():
            if record["guild_id"] != guild_id:
                continue
            if record.get("status") != "published" or record.get("submission_kind") != "confession":
                continue
            if not isinstance(record.get("posted_channel_id"), int) or not isinstance(record.get("posted_message_id"), int):
                continue
            rows.append(_submission_from_row(deepcopy(record), self._privacy))
        rows.sort(key=lambda item: item.get("published_at") or item.get("created_at") or "")
        return rows

    async def list_published_public_reply_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        rows = []
        for record in self.submissions.values():
            if record["guild_id"] != guild_id:
                continue
            if record.get("status") != "published" or record.get("submission_kind") != "reply":
                continue
            if record.get("reply_flow") != "reply_to_confession":
                continue
            if not isinstance(record.get("posted_channel_id"), int) or not isinstance(record.get("posted_message_id"), int):
                continue
            rows.append(_submission_from_row(deepcopy(record), self._privacy))
        rows.sort(key=lambda item: item.get("published_at") or item.get("created_at") or "")
        return rows

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        rows = []
        author_lookup_hashes = set(
            self._privacy.blind_index_candidates(label="author-link", guild_id=guild_id, value=author_user_id)
        )
        for submission_id, link in self.secure_author_links.items():
            if link["guild_id"] != guild_id or link["author_lookup_hash"] not in author_lookup_hashes:
                continue
            submission = self.submissions.get(submission_id)
            if submission is None:
                continue
            decoded = _submission_from_row(deepcopy(submission), self._privacy)
            if decoded is not None:
                rows.append(decoded)
        for link in self.author_links.values():
            if link["guild_id"] != guild_id or link["author_user_id"] != author_user_id:
                continue
            if link["submission_id"] in self.secure_author_links:
                continue
            submission = self.submissions.get(link["submission_id"])
            if submission is None:
                continue
            decoded = _submission_from_row(deepcopy(submission), self._privacy)
            if decoded is not None:
                rows.append(decoded)
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
            self.secure_author_links[normalized["submission_id"]] = self._encode_author_link_row(normalized)
            self.author_links.pop(normalized["submission_id"], None)

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        secure_record = self.secure_author_links.get(submission_id)
        if secure_record is not None:
            return _author_link_from_secure_row(deepcopy(secure_record), self._privacy)
        record = self.author_links.get(submission_id)
        return _author_link_from_legacy_row(deepcopy(record)) if record is not None else None

    async def upsert_private_media(self, record: dict[str, Any]):
        normalized = normalize_private_media(record)
        if normalized is not None:
            self.private_media[normalized["submission_id"]] = self._encode_private_media_row(normalized)

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        record = self.private_media.get(submission_id)
        return _private_media_from_row(deepcopy(record), self._privacy) if record is not None else None

    async def delete_private_media(self, submission_id: str):
        self.private_media.pop(submission_id, None)

    async def upsert_owner_reply_opportunity(self, record: dict[str, Any]):
        normalized = normalize_owner_reply_opportunity(record)
        if normalized is not None:
            self.owner_reply_opportunities[normalized["opportunity_id"]] = self._encode_owner_reply_opportunity_row(normalized)

    async def fetch_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        record = self.owner_reply_opportunities.get(opportunity_id)
        return _owner_reply_opportunity_from_row(deepcopy(record), self._privacy) if record is not None else None

    async def fetch_owner_reply_opportunity_by_source_message_id(self, guild_id: int, source_message_id: int) -> dict[str, Any] | None:
        for record in self.owner_reply_opportunities.values():
            if record["guild_id"] == guild_id and int(record.get("source_message_id") or 0) == source_message_id:
                return _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
        return None

    async def fetch_owner_reply_opportunity_by_notification_message_id(self, notification_message_id: int) -> dict[str, Any] | None:
        for record in self.owner_reply_opportunities.values():
            if int(record.get("notification_message_id") or 0) == notification_message_id:
                return _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
        return None

    async def fetch_pending_owner_reply_opportunity_for_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
    ) -> dict[str, Any] | None:
        rows = await self.list_owner_reply_opportunities_for_responder_path(
            guild_id,
            root_submission_id,
            referenced_submission_id,
            source_author_user_id,
            limit=1,
        )
        for record in rows:
            if record.get("status") == "pending":
                return deepcopy(record)
        return None

    async def list_pending_owner_reply_opportunities_for_author(
        self,
        guild_id: int,
        author_user_id: int,
        *,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        rows = []
        for record in self.owner_reply_opportunities.values():
            if record["guild_id"] != guild_id or record.get("status") != "pending":
                continue
            owner_link = await self.fetch_author_link(record["root_submission_id"])
            if owner_link is None or owner_link["author_user_id"] != author_user_id:
                continue
            decoded = _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
            if decoded is not None:
                rows.append(decoded)
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def list_owner_reply_opportunities_for_root_submission(
        self,
        root_submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        rows = [
            _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
            for record in self.owner_reply_opportunities.values()
            if record.get("root_submission_id") == root_submission_id
        ]
        rows = [record for record in rows if record is not None]
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def list_owner_reply_opportunities_for_submission(
        self,
        submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        rows = [
            _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
            for record in self.owner_reply_opportunities.values()
            if record.get("root_submission_id") == submission_id or record.get("referenced_submission_id") == submission_id
        ]
        rows = [record for record in rows if record is not None]
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def list_owner_reply_opportunities_for_responder_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        source_lookup_hashes = set(
            self._privacy.blind_index_candidates(
                label="owner-reply-source-author",
                guild_id=guild_id,
                value=source_author_user_id,
            )
        )
        rows = [
            _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
            for record in self.owner_reply_opportunities.values()
            if record["guild_id"] == guild_id
            and record.get("root_submission_id") == root_submission_id
            and record.get("referenced_submission_id") == referenced_submission_id
            and (
                record.get("source_author_lookup_hash") in source_lookup_hashes
                or int(record.get("source_author_user_id") or 0) == source_author_user_id
            )
        ]
        rows = [record for record in rows if record is not None]
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def list_owner_reply_opportunities_for_source_author(
        self,
        guild_id: int,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        source_lookup_hashes = set(
            self._privacy.blind_index_candidates(
                label="owner-reply-source-author",
                guild_id=guild_id,
                value=source_author_user_id,
            )
        )
        rows = [
            _owner_reply_opportunity_from_row(deepcopy(record), self._privacy)
            for record in self.owner_reply_opportunities.values()
            if record["guild_id"] == guild_id
            and (
                record.get("source_author_lookup_hash") in source_lookup_hashes
                or int(record.get("source_author_user_id") or 0) == source_author_user_id
            )
        ]
        rows = [record for record in rows if record is not None]
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def claim_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        record = self.owner_reply_opportunities.get(opportunity_id)
        if record is None or record.get("status") != "pending":
            return None
        updated = deepcopy(record)
        updated["status"] = "locked"
        self.owner_reply_opportunities[opportunity_id] = updated
        return _owner_reply_opportunity_from_row(deepcopy(updated), self._privacy)

    async def release_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        record = self.owner_reply_opportunities.get(opportunity_id)
        if record is None or record.get("status") != "locked":
            return _owner_reply_opportunity_from_row(deepcopy(record), self._privacy) if record is not None else None
        updated = deepcopy(record)
        updated["status"] = "pending"
        self.owner_reply_opportunities[opportunity_id] = updated
        return _owner_reply_opportunity_from_row(deepcopy(updated), self._privacy)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        for lookup_hash in self._privacy.blind_index_candidates(label="enforcement-state", guild_id=guild_id, value=user_id):
            secure_record = self.secure_enforcement_states.get((guild_id, lookup_hash))
            if secure_record is not None:
                return _enforcement_from_secure_row(deepcopy(secure_record), self._privacy)
        record = self.enforcement_states.get((guild_id, user_id))
        return _enforcement_from_legacy_row(deepcopy(record)) if record is not None else None

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        normalized = normalize_enforcement_state(record)
        if normalized is not None:
            secure_row = self._encode_enforcement_state_row(normalized)
            self.secure_enforcement_states[(normalized["guild_id"], secure_row["user_lookup_hash"])] = secure_row
            self.enforcement_states.pop((normalized["guild_id"], normalized["user_id"]), None)

    async def list_active_enforcement_states(self, *, guild_id: int | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()
        for secure_row in self.secure_enforcement_states.values():
            decoded = _enforcement_from_secure_row(deepcopy(secure_row), self._privacy)
            if decoded is None:
                continue
            if guild_id is not None and decoded["guild_id"] != guild_id:
                continue
            if not _enforcement_state_requires_gate_cache(decoded):
                continue
            key = (decoded["guild_id"], decoded["user_id"])
            seen.add(key)
            rows.append(decoded)
        for legacy_row in self.enforcement_states.values():
            decoded = _enforcement_from_legacy_row(deepcopy(legacy_row))
            if decoded is None:
                continue
            if guild_id is not None and decoded["guild_id"] != guild_id:
                continue
            key = (decoded["guild_id"], decoded["user_id"])
            if key in seen or not _enforcement_state_requires_gate_cache(decoded):
                continue
            rows.append(decoded)
        rows.sort(key=lambda item: (int(item.get("guild_id") or 0), int(item.get("user_id") or 0)))
        return rows

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
            submission_row = self.submissions.get(record["submission_id"])
            submission = _submission_from_row(deepcopy(submission_row), self._privacy) if submission_row is not None else None
            if submission is None or submission.get("status") != "queued":
                continue
            private_media = self.private_media.get(record["submission_id"])
            attachment_urls = []
            if private_media is not None:
                decoded_media = _private_media_from_row(deepcopy(private_media), self._privacy)
                attachment_urls = list((decoded_media or {}).get("attachment_urls") or [])
            rows.append(
                {
                    "case_id": record["case_id"],
                    "confession_id": submission["confession_id"],
                    "case_kind": record["case_kind"],
                    "status": record["status"],
                    "review_version": int(record.get("review_version") or 0),
                    "submission_kind": submission.get("submission_kind") or "confession",
                    "reply_flow": submission.get("reply_flow"),
                    "owner_reply_generation": submission.get("owner_reply_generation"),
                    "parent_confession_id": submission.get("parent_confession_id"),
                    "staff_preview": submission.get("staff_preview"),
                    "flag_codes": list(submission.get("flag_codes") or ()),
                    "attachment_meta": deepcopy(submission.get("attachment_meta") or []),
                    "attachment_urls": attachment_urls,
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

    async def fetch_support_ticket(self, guild_id: int, ticket_id: str) -> dict[str, Any] | None:
        record = self.support_tickets.get((guild_id, ticket_id))
        return deepcopy(record) if record is not None else None

    async def list_support_tickets(self, guild_id: int, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        cleaned_status = str(status).strip().lower() if status is not None else None
        rows = []
        for record in self.support_tickets.values():
            if record["guild_id"] != guild_id:
                continue
            if cleaned_status is not None and record.get("status") != cleaned_status:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda item: item.get("created_at") or "", reverse=True)
        return rows[:limit]

    async def upsert_support_ticket(self, record: dict[str, Any]):
        normalized = normalize_support_ticket(record)
        if normalized is not None:
            self.support_tickets[(normalized["guild_id"], normalized["ticket_id"])] = normalized

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

    async def fetch_privacy_status(self, guild_id: int | None = None) -> dict[str, Any]:
        status = _empty_privacy_status(scope="guild" if guild_id is not None else "global", guild_id=guild_id)
        for row in self.submissions.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, _submission_privacy_categories(row, self._privacy))
        for row in self.private_media.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, _private_media_privacy_categories(row, self._privacy))
        for row in self.secure_author_links.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, _author_link_privacy_categories(row, self._privacy))
        for row in self.author_links.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, {"legacy_author_links"})
        for row in self.owner_reply_opportunities.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, _owner_reply_privacy_categories(row, self._privacy))
        for row in self.secure_enforcement_states.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, _enforcement_privacy_categories(row, self._privacy))
        for row in self.enforcement_states.values():
            if guild_id is not None and int(row.get("guild_id") or 0) != guild_id:
                continue
            _apply_privacy_categories(status, {"legacy_enforcement_rows"})
        return _finalize_privacy_status(status)

    async def run_privacy_backfill(self, *, apply: bool, batch_size: int = 100) -> dict[str, Any]:
        summary = {
            "mode": "apply" if apply else "dry-run",
            "submissions": 0,
            "private_media": 0,
            "author_links": 0,
            "owner_reply_opportunities": 0,
            "enforcement_states": 0,
            "batch_size": batch_size,
        }
        seen = 0
        for submission_id, row in list(self.submissions.items()):
            if seen >= batch_size:
                break
            if not _submission_privacy_categories(row, self._privacy):
                continue
            summary["submissions"] += 1
            seen += 1
            if not apply:
                continue
            normalized = _submission_from_row(deepcopy(row), self._privacy)
            if normalized is None:
                continue
            if _submission_requires_sensitive_payload(normalized):
                self.submissions[submission_id] = self._encode_submission_row(
                    _backfill_submission_duplicate_fields(self._privacy, normalized)
                )
            else:
                terminal = _backfill_submission_duplicate_fields(self._privacy, normalized)
                terminal["reply_target_label"] = None
                terminal["reply_target_preview"] = None
                terminal["staff_preview"] = None
                terminal["content_body"] = None
                terminal["shared_link_url"] = None
                terminal["attachment_meta"] = []
                self.submissions[submission_id] = self._encode_submission_row(terminal)
                self.private_media.pop(submission_id, None)
        seen = 0
        for submission_id, row in list(self.private_media.items()):
            if seen >= batch_size:
                break
            if not _private_media_privacy_categories(row, self._privacy):
                continue
            summary["private_media"] += 1
            seen += 1
            if not apply:
                continue
            submission = self.submissions.get(submission_id)
            decoded_submission = _submission_from_row(deepcopy(submission), self._privacy) if submission is not None else None
            if decoded_submission is not None and not _submission_requires_sensitive_payload(decoded_submission):
                del self.private_media[submission_id]
                continue
            normalized = _private_media_from_row(deepcopy(row), self._privacy)
            if normalized is None:
                continue
            self.private_media[submission_id] = self._encode_private_media_row(normalized)
        seen = 0
        for submission_id, row in list(self.author_links.items()):
            if seen >= batch_size:
                break
            summary["author_links"] += 1
            seen += 1
            if not apply:
                continue
            normalized = normalize_author_link(row)
            if normalized is None:
                continue
            self.secure_author_links[submission_id] = self._encode_author_link_row(normalized)
            del self.author_links[submission_id]
        for submission_id, row in list(self.secure_author_links.items()):
            if seen >= batch_size:
                break
            if not _author_link_privacy_categories(row, self._privacy):
                continue
            summary["author_links"] += 1
            seen += 1
            if not apply:
                continue
            normalized = _author_link_from_secure_row(deepcopy(row), self._privacy)
            if normalized is None:
                continue
            self.secure_author_links[submission_id] = self._encode_author_link_row(normalized)
        seen = 0
        for opportunity_id, row in list(self.owner_reply_opportunities.items()):
            if seen >= batch_size:
                break
            if not _owner_reply_privacy_categories(row, self._privacy):
                continue
            summary["owner_reply_opportunities"] += 1
            seen += 1
            if not apply:
                continue
            normalized = _owner_reply_opportunity_from_row(deepcopy(row), self._privacy)
            if normalized is None:
                continue
            self.owner_reply_opportunities[opportunity_id] = self._encode_owner_reply_opportunity_row(normalized)
        seen = 0
        for key, row in list(self.enforcement_states.items()):
            if seen >= batch_size:
                break
            normalized = normalize_enforcement_state(row)
            if normalized is None:
                continue
            summary["enforcement_states"] += 1
            seen += 1
            if not apply:
                continue
            secure_row = self._encode_enforcement_state_row(normalized)
            self.secure_enforcement_states[(normalized["guild_id"], secure_row["user_lookup_hash"])] = secure_row
            del self.enforcement_states[key]
        for key, row in list(self.secure_enforcement_states.items()):
            if seen >= batch_size:
                break
            if not _enforcement_privacy_categories(row, self._privacy):
                continue
            summary["enforcement_states"] += 1
            seen += 1
            if not apply:
                continue
            normalized = _enforcement_from_secure_row(deepcopy(row), self._privacy)
            if normalized is None:
                continue
            secure_row = self._encode_enforcement_state_row(normalized)
            self.secure_enforcement_states[(normalized["guild_id"], secure_row["user_lookup_hash"])] = secure_row
        summary["privacy_status"] = await self.fetch_privacy_status()
        return summary


class _PostgresConfessionsStore(_BaseConfessionsStore):
    backend_name = "postgres"

    def __init__(self, dsn: str, privacy: ConfessionsCrypto):
        self.dsn = dsn
        self._privacy = privacy
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

    def _encode_submission_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        payload = {
            "reply_target_label": normalized.get("reply_target_label"),
            "reply_target_preview": normalized.get("reply_target_preview"),
            "staff_preview": normalized.get("staff_preview"),
            "content_body": normalized.get("content_body"),
            "shared_link_url": normalized.get("shared_link_url"),
        }
        if any(value is not None for value in payload.values()):
            row["content_ciphertext"] = self._privacy.encrypt_payload(
                domain="submission-content",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                    "confession_id": normalized["confession_id"],
                },
                payload=payload,
                key_domain="content",
            )
            row["reply_target_label"] = None
            row["reply_target_preview"] = None
            row["staff_preview"] = None
            row["content_body"] = None
            row["shared_link_url"] = None
        else:
            row["content_ciphertext"] = None
        return row

    def _encode_author_link_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        return {
            "submission_id": normalized["submission_id"],
            "guild_id": normalized["guild_id"],
            "author_lookup_hash": self._privacy.blind_index(
                label="author-link",
                guild_id=normalized["guild_id"],
                value=normalized["author_user_id"],
            ),
            "author_identity_ciphertext": self._privacy.encrypt_payload(
                domain="author-link",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                },
                payload={"author_user_id": normalized["author_user_id"]},
                key_domain="identity",
            ),
            "created_at": normalized["created_at"],
        }

    def _encode_private_media_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        if normalized["attachment_urls"]:
            row["attachment_payload"] = self._privacy.encrypt_payload(
                domain="private-media",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "submission_id": normalized["submission_id"],
                },
                payload={"attachment_urls": list(normalized["attachment_urls"])},
                key_domain="content",
            )
            row["attachment_urls"] = []
        else:
            row["attachment_payload"] = None
        return row

    def _encode_owner_reply_opportunity_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        row = deepcopy(normalized)
        payload = {
            "source_author_user_id": normalized.get("source_author_user_id"),
            "source_author_name": normalized.get("source_author_name"),
            "source_preview": normalized.get("source_preview"),
            "source_message_fingerprint": normalized.get("source_message_fingerprint"),
        }
        row["private_payload"] = self._privacy.encrypt_payload(
            domain="owner-reply-opportunity",
            aad_fields={
                "guild_id": normalized["guild_id"],
                "opportunity_id": normalized["opportunity_id"],
                "root_submission_id": normalized["root_submission_id"],
            },
            payload=payload,
            key_domain="content",
        )
        row["source_author_lookup_hash"] = (
            self._privacy.blind_index(
                label="owner-reply-source-author",
                guild_id=normalized["guild_id"],
                value=normalized["source_author_user_id"],
            )
            if normalized.get("source_author_user_id")
            else None
        )
        row["source_author_user_id"] = None
        row["source_author_name"] = PROTECTED_OWNER_REPLY_NAME
        row["source_preview"] = PROTECTED_OWNER_REPLY_PREVIEW
        row["source_message_fingerprint"] = None
        return row

    def _encode_enforcement_state_row(self, normalized: dict[str, Any]) -> dict[str, Any]:
        lookup_hash = self._privacy.blind_index(
            label="enforcement-state",
            guild_id=normalized["guild_id"],
            value=normalized["user_id"],
        )
        return {
            "guild_id": normalized["guild_id"],
            "user_lookup_hash": lookup_hash,
            "user_identity_ciphertext": self._privacy.encrypt_payload(
                domain="enforcement-state",
                aad_fields={
                    "guild_id": normalized["guild_id"],
                    "user_lookup_hash": lookup_hash,
                },
                payload={"user_id": normalized["user_id"]},
                key_domain="identity",
            ),
            "active_restriction": normalized["active_restriction"],
            "restricted_until": normalized["restricted_until"],
            "is_permanent_ban": normalized["is_permanent_ban"],
            "strike_count": normalized["strike_count"],
            "last_strike_at": normalized["last_strike_at"],
            "cooldown_until": normalized["cooldown_until"],
            "burst_count": normalized["burst_count"],
            "burst_window_started_at": normalized["burst_window_started_at"],
            "last_case_id": normalized["last_case_id"],
            "image_restriction_active": normalized["image_restriction_active"],
            "image_restricted_until": normalized["image_restricted_until"],
            "image_restriction_case_id": normalized["image_restriction_case_id"],
            "updated_at": normalized["updated_at"],
        }

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
                "appeals_channel_id BIGINT NULL, "
                "review_mode BOOLEAN NOT NULL DEFAULT TRUE, "
                "block_adult_language BOOLEAN NOT NULL DEFAULT TRUE, "
                "link_policy_mode TEXT NOT NULL DEFAULT 'trusted_only', "
                "allow_trusted_mainstream_links BOOLEAN NOT NULL DEFAULT TRUE, "
                "custom_allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "custom_block_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allowed_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "blocked_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_images BOOLEAN NOT NULL DEFAULT FALSE, "
                "image_review_required BOOLEAN NOT NULL DEFAULT FALSE, "
                "allow_anonymous_replies BOOLEAN NOT NULL DEFAULT FALSE, "
                "anonymous_reply_review_required BOOLEAN NOT NULL DEFAULT FALSE, "
                "allow_owner_replies BOOLEAN NOT NULL DEFAULT TRUE, "
                "owner_reply_review_mode BOOLEAN NOT NULL DEFAULT FALSE, "
                "allow_self_edit BOOLEAN NOT NULL DEFAULT FALSE, "
                "auto_moderation_exempt_admins BOOLEAN NOT NULL DEFAULT TRUE, "
                "auto_moderation_exempt_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
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
                "submission_kind TEXT NOT NULL DEFAULT 'confession', "
                "reply_flow TEXT NULL, "
                "owner_reply_generation SMALLINT NULL, "
                "parent_confession_id TEXT NULL, "
                "reply_target_label TEXT NULL, "
                "reply_target_preview TEXT NULL, "
                "status TEXT NOT NULL, "
                "review_status TEXT NOT NULL DEFAULT 'none', "
                "staff_preview TEXT NULL, "
                "content_body TEXT NULL, "
                "shared_link_url TEXT NULL, "
                "content_fingerprint TEXT NULL, "
                "similarity_key TEXT NULL, "
                "fuzzy_signature TEXT NULL, "
                "flag_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "attachment_meta JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "posted_channel_id BIGINT NULL, "
                "posted_message_id BIGINT NULL, "
                "discussion_thread_id BIGINT NULL, "
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
                f"CREATE TABLE IF NOT EXISTS {SECURE_AUTHOR_LINK_TABLE} ("
                "submission_id TEXT PRIMARY KEY REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "guild_id BIGINT NOT NULL, "
                "author_lookup_hash TEXT NOT NULL, "
                "author_identity_ciphertext TEXT NOT NULL, "
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
                "CREATE TABLE IF NOT EXISTS confession_owner_reply_opportunities ("
                "opportunity_id TEXT PRIMARY KEY, "
                "guild_id BIGINT NOT NULL, "
                "root_submission_id TEXT NOT NULL REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "root_confession_id TEXT NOT NULL, "
                "referenced_submission_id TEXT NOT NULL REFERENCES confession_submissions(submission_id) ON DELETE CASCADE, "
                "source_channel_id BIGINT NOT NULL, "
                "source_message_id BIGINT NOT NULL, "
                "source_author_user_id BIGINT NULL, "
                "source_author_name TEXT NOT NULL, "
                "source_preview TEXT NOT NULL, "
                "source_message_fingerprint TEXT NULL, "
                "status TEXT NOT NULL DEFAULT 'pending', "
                "notification_status TEXT NOT NULL DEFAULT 'none', "
                "notification_channel_id BIGINT NULL, "
                "notification_message_id BIGINT NULL, "
                "created_at TIMESTAMPTZ NOT NULL, "
                "expires_at TIMESTAMPTZ NOT NULL, "
                "notified_at TIMESTAMPTZ NULL, "
                "resolved_at TIMESTAMPTZ NULL"
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
                "image_restriction_active BOOLEAN NOT NULL DEFAULT FALSE, "
                "image_restricted_until TIMESTAMPTZ NULL, "
                "image_restriction_case_id TEXT NULL, "
                "updated_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                f"CREATE TABLE IF NOT EXISTS {SECURE_ENFORCEMENT_TABLE} ("
                "guild_id BIGINT NOT NULL, "
                "user_lookup_hash TEXT NOT NULL, "
                "user_identity_ciphertext TEXT NOT NULL, "
                "active_restriction TEXT NOT NULL DEFAULT 'none', "
                "restricted_until TIMESTAMPTZ NULL, "
                "is_permanent_ban BOOLEAN NOT NULL DEFAULT FALSE, "
                "strike_count INTEGER NOT NULL DEFAULT 0, "
                "last_strike_at TIMESTAMPTZ NULL, "
                "cooldown_until TIMESTAMPTZ NULL, "
                "burst_count INTEGER NOT NULL DEFAULT 0, "
                "burst_window_started_at TIMESTAMPTZ NULL, "
                "last_case_id TEXT NULL, "
                "image_restriction_active BOOLEAN NOT NULL DEFAULT FALSE, "
                "image_restricted_until TIMESTAMPTZ NULL, "
                "image_restriction_case_id TEXT NULL, "
                "updated_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, user_lookup_hash)"
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
            (
                "CREATE TABLE IF NOT EXISTS confession_support_tickets ("
                "ticket_id TEXT NOT NULL, "
                "guild_id BIGINT NOT NULL, "
                "kind TEXT NOT NULL, "
                "action_target_id TEXT NULL, "
                "reference_confession_id TEXT NULL, "
                "reference_case_id TEXT NULL, "
                "context_label TEXT NULL, "
                "details TEXT NULL, "
                "status TEXT NOT NULL DEFAULT 'open', "
                "resolution_action TEXT NULL, "
                "message_channel_id BIGINT NULL, "
                "message_id BIGINT NULL, "
                "created_at TIMESTAMPTZ NOT NULL, "
                "resolved_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, ticket_id)"
                ")"
            ),
        ]
        alter_statements = [
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS review_mode BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS block_adult_language BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS link_policy_mode TEXT NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_trusted_mainstream_links BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS custom_allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS custom_block_domains JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allowed_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS blocked_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_images BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS image_review_required BOOLEAN NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS max_images SMALLINT NOT NULL DEFAULT 3",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS panel_channel_id BIGINT NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS panel_message_id BIGINT NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS appeals_channel_id BIGINT NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_anonymous_replies BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS anonymous_reply_review_required BOOLEAN NULL",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_owner_replies BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS owner_reply_review_mode BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS allow_self_edit BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS auto_moderation_exempt_admins BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE confession_guild_configs ADD COLUMN IF NOT EXISTS auto_moderation_exempt_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'none'",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS submission_kind TEXT NOT NULL DEFAULT 'confession'",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS reply_flow TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS owner_reply_generation SMALLINT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS parent_confession_id TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS reply_target_label TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS reply_target_preview TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS content_body TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS shared_link_url TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS content_ciphertext TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS content_fingerprint TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS similarity_key TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS fuzzy_signature TEXT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS flag_codes JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS attachment_meta JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS discussion_thread_id BIGINT NULL",
            "ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS current_case_id TEXT NULL",
            "ALTER TABLE confession_private_media ADD COLUMN IF NOT EXISTS attachment_payload TEXT NULL",
            "ALTER TABLE confession_enforcement_states ADD COLUMN IF NOT EXISTS image_restriction_active BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE confession_enforcement_states ADD COLUMN IF NOT EXISTS image_restricted_until TIMESTAMPTZ NULL",
            "ALTER TABLE confession_enforcement_states ADD COLUMN IF NOT EXISTS image_restriction_case_id TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS resolution_action TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS resolution_note TEXT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL",
            "ALTER TABLE confession_cases ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS root_confession_id TEXT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS source_author_user_id BIGINT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS source_author_lookup_hash TEXT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS notification_status TEXT NOT NULL DEFAULT 'none'",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS source_message_fingerprint TEXT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS private_payload TEXT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS notification_channel_id BIGINT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS notification_message_id BIGINT NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS notified_at TIMESTAMPTZ NULL",
            "ALTER TABLE confession_owner_reply_opportunities ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS action_target_id TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS reference_confession_id TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS reference_case_id TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS context_label TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS details TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'open'",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS resolution_action TEXT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS message_channel_id BIGINT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS message_id BIGINT NULL",
            "ALTER TABLE confession_support_tickets ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ NULL",
            "UPDATE confession_guild_configs SET link_policy_mode = CASE WHEN allow_trusted_mainstream_links THEN 'trusted_only' ELSE 'disabled' END WHERE link_policy_mode IS NULL OR link_policy_mode = ''",
            "ALTER TABLE confession_guild_configs ALTER COLUMN link_policy_mode SET DEFAULT 'trusted_only'",
            "ALTER TABLE confession_guild_configs ALTER COLUMN link_policy_mode SET NOT NULL",
            "ALTER TABLE confession_guild_configs ALTER COLUMN allow_images SET DEFAULT FALSE",
            "UPDATE confession_guild_configs SET image_review_required = allow_images WHERE image_review_required IS NULL",
            "UPDATE confession_guild_configs SET anonymous_reply_review_required = allow_anonymous_replies WHERE anonymous_reply_review_required IS NULL",
            "ALTER TABLE confession_guild_configs ALTER COLUMN image_review_required SET DEFAULT FALSE",
            "ALTER TABLE confession_guild_configs ALTER COLUMN anonymous_reply_review_required SET DEFAULT FALSE",
            "UPDATE confession_guild_configs SET image_review_required = FALSE WHERE image_review_required IS NULL",
            "UPDATE confession_guild_configs SET anonymous_reply_review_required = FALSE WHERE anonymous_reply_review_required IS NULL",
            "ALTER TABLE confession_guild_configs ALTER COLUMN image_review_required SET NOT NULL",
            "ALTER TABLE confession_guild_configs ALTER COLUMN anonymous_reply_review_required SET NOT NULL",
            "UPDATE confession_submissions SET reply_flow = 'reply_to_confession' WHERE submission_kind = 'reply' AND (reply_flow IS NULL OR reply_flow = '')",
            "UPDATE confession_submissions SET owner_reply_generation = 1 WHERE submission_kind = 'reply' AND reply_flow = 'owner_reply_to_user' AND owner_reply_generation IS NULL",
        ]
        index_statements = [
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_confession_submissions_confession_id ON confession_submissions (guild_id, confession_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_status_created ON confession_submissions (guild_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_message_id ON confession_submissions (guild_id, posted_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_discussion_thread_id ON confession_submissions (guild_id, discussion_thread_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_parent_confession_id ON confession_submissions (guild_id, parent_confession_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_submissions_reply_flow ON confession_submissions (guild_id, reply_flow)",
            "CREATE INDEX IF NOT EXISTS ix_confession_cases_status_created ON confession_cases (guild_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_cases_submission_id ON confession_cases (submission_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_author_links_author_created ON confession_author_links (guild_id, author_user_id, created_at DESC)",
            f"CREATE INDEX IF NOT EXISTS ix_confession_author_identities_lookup_created ON {SECURE_AUTHOR_LINK_TABLE} (guild_id, author_lookup_hash, created_at DESC)",
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_confession_owner_reply_source_message ON confession_owner_reply_opportunities (guild_id, source_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_root_created ON confession_owner_reply_opportunities (root_submission_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_responder_path_status ON confession_owner_reply_opportunities (guild_id, root_submission_id, referenced_submission_id, source_author_user_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_source_author_created ON confession_owner_reply_opportunities (guild_id, source_author_user_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_responder_path_lookup_status ON confession_owner_reply_opportunities (guild_id, root_submission_id, referenced_submission_id, source_author_lookup_hash, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_source_author_lookup_created ON confession_owner_reply_opportunities (guild_id, source_author_lookup_hash, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_owner_reply_notification_message_id ON confession_owner_reply_opportunities (notification_message_id)",
            f"CREATE INDEX IF NOT EXISTS ix_confession_enforcement_states_secure_lookup ON {SECURE_ENFORCEMENT_TABLE} (guild_id, user_lookup_hash)",
            "CREATE INDEX IF NOT EXISTS ix_confession_support_tickets_status_created ON confession_support_tickets (guild_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS ix_confession_support_tickets_message_id ON confession_support_tickets (message_id)",
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
                        "guild_id, enabled, confession_channel_id, panel_channel_id, panel_message_id, review_channel_id, appeals_channel_id, review_mode, block_adult_language, "
                        "link_policy_mode, allow_trusted_mainstream_links, custom_allow_domains, custom_block_domains, allowed_role_ids, blocked_role_ids, "
                        "allow_images, image_review_required, allow_anonymous_replies, anonymous_reply_review_required, allow_owner_replies, owner_reply_review_mode, allow_self_edit, "
                        "auto_moderation_exempt_admins, auto_moderation_exempt_role_ids, max_images, cooldown_seconds, "
                        "burst_limit, burst_window_seconds, auto_suspend_hours, strike_temp_ban_threshold, temp_ban_days, strike_perm_ban_threshold, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, "
                        "$10, $11, $12::jsonb, $13::jsonb, $14::jsonb, $15::jsonb, $16, $17, $18, $19, $20, $21, $22, "
                        "$23, $24::jsonb, $25, $26, $27, $28, $29, $30, $31, $32, timezone('utc', now())"
                        ") "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "enabled = EXCLUDED.enabled, "
                        "confession_channel_id = EXCLUDED.confession_channel_id, "
                        "panel_channel_id = EXCLUDED.panel_channel_id, "
                        "panel_message_id = EXCLUDED.panel_message_id, "
                        "review_channel_id = EXCLUDED.review_channel_id, "
                        "appeals_channel_id = EXCLUDED.appeals_channel_id, "
                        "review_mode = EXCLUDED.review_mode, "
                        "block_adult_language = EXCLUDED.block_adult_language, "
                        "link_policy_mode = EXCLUDED.link_policy_mode, "
                        "allow_trusted_mainstream_links = EXCLUDED.allow_trusted_mainstream_links, "
                        "custom_allow_domains = EXCLUDED.custom_allow_domains, "
                        "custom_block_domains = EXCLUDED.custom_block_domains, "
                        "allowed_role_ids = EXCLUDED.allowed_role_ids, "
                        "blocked_role_ids = EXCLUDED.blocked_role_ids, "
                        "allow_images = EXCLUDED.allow_images, "
                        "image_review_required = EXCLUDED.image_review_required, "
                        "allow_anonymous_replies = EXCLUDED.allow_anonymous_replies, "
                        "anonymous_reply_review_required = EXCLUDED.anonymous_reply_review_required, "
                        "allow_owner_replies = EXCLUDED.allow_owner_replies, "
                        "owner_reply_review_mode = EXCLUDED.owner_reply_review_mode, "
                        "allow_self_edit = EXCLUDED.allow_self_edit, "
                        "auto_moderation_exempt_admins = EXCLUDED.auto_moderation_exempt_admins, "
                        "auto_moderation_exempt_role_ids = EXCLUDED.auto_moderation_exempt_role_ids, "
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
                    normalized["appeals_channel_id"],
                    normalized["review_mode"],
                    normalized["block_adult_language"],
                    normalized["link_policy_mode"],
                    normalized["allow_trusted_mainstream_links"],
                    json.dumps(normalized["custom_allow_domains"]),
                    json.dumps(normalized["custom_block_domains"]),
                    json.dumps(normalized["allowed_role_ids"]),
                    json.dumps(normalized["blocked_role_ids"]),
                    normalized["allow_images"],
                    normalized["image_review_required"],
                    normalized["allow_anonymous_replies"],
                    normalized["anonymous_reply_review_required"],
                    normalized["allow_owner_replies"],
                    normalized["owner_reply_review_mode"],
                    normalized["allow_self_edit"],
                    normalized["auto_moderation_exempt_admins"],
                    json.dumps(normalized["auto_moderation_exempt_role_ids"]),
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
        row = self._encode_submission_row(normalized)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_submissions ("
                        "submission_id, guild_id, confession_id, submission_kind, reply_flow, owner_reply_generation, parent_confession_id, reply_target_label, reply_target_preview, status, review_status, staff_preview, content_body, shared_link_url, content_ciphertext, "
                        "content_fingerprint, similarity_key, fuzzy_signature, flag_codes, attachment_meta, posted_channel_id, posted_message_id, discussion_thread_id, current_case_id, created_at, published_at, resolved_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, "
                        "$16, $17, $18, $19::jsonb, $20::jsonb, $21, $22, $23, $24, $25, $26, $27"
                        ") "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "submission_kind = EXCLUDED.submission_kind, "
                        "reply_flow = EXCLUDED.reply_flow, "
                        "owner_reply_generation = EXCLUDED.owner_reply_generation, "
                        "parent_confession_id = EXCLUDED.parent_confession_id, "
                        "reply_target_label = EXCLUDED.reply_target_label, "
                        "reply_target_preview = EXCLUDED.reply_target_preview, "
                        "status = EXCLUDED.status, "
                        "review_status = EXCLUDED.review_status, "
                        "staff_preview = EXCLUDED.staff_preview, "
                        "content_body = EXCLUDED.content_body, "
                        "shared_link_url = EXCLUDED.shared_link_url, "
                        "content_ciphertext = EXCLUDED.content_ciphertext, "
                        "content_fingerprint = EXCLUDED.content_fingerprint, "
                        "similarity_key = EXCLUDED.similarity_key, "
                        "fuzzy_signature = EXCLUDED.fuzzy_signature, "
                        "flag_codes = EXCLUDED.flag_codes, "
                        "attachment_meta = EXCLUDED.attachment_meta, "
                        "posted_channel_id = EXCLUDED.posted_channel_id, "
                        "posted_message_id = EXCLUDED.posted_message_id, "
                        "discussion_thread_id = EXCLUDED.discussion_thread_id, "
                        "current_case_id = EXCLUDED.current_case_id, "
                        "published_at = EXCLUDED.published_at, "
                        "resolved_at = EXCLUDED.resolved_at"
                    ),
                    row["submission_id"],
                    row["guild_id"],
                    row["confession_id"],
                    row["submission_kind"],
                    row["reply_flow"],
                    row["owner_reply_generation"],
                    row["parent_confession_id"],
                    row["reply_target_label"],
                    row["reply_target_preview"],
                    row["status"],
                    row["review_status"],
                    row["staff_preview"],
                    row["content_body"],
                    row["shared_link_url"],
                    row["content_ciphertext"],
                    row["content_fingerprint"],
                    row["similarity_key"],
                    row["fuzzy_signature"],
                    json.dumps(row["flag_codes"]),
                    json.dumps(row["attachment_meta"]),
                    row["posted_channel_id"],
                    row["posted_message_id"],
                    row["discussion_thread_id"],
                    row["current_case_id"],
                    _parse_datetime(row["created_at"]),
                    _parse_datetime(row["published_at"]),
                    _parse_datetime(row["resolved_at"]),
                )

    async def fetch_submission(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM confession_submissions WHERE submission_id = $1", submission_id)
        return _submission_from_row(row, self._privacy)

    async def fetch_submission_by_confession_id(self, guild_id: int, confession_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_submissions WHERE guild_id = $1 AND confession_id = $2",
                guild_id,
                confession_id,
            )
        return _submission_from_row(row, self._privacy)

    async def fetch_submission_by_message_id(self, guild_id: int, message_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_submissions WHERE guild_id = $1 AND posted_message_id = $2",
                guild_id,
                message_id,
            )
        return _submission_from_row(row, self._privacy)

    async def list_published_top_level_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_submissions "
                    "WHERE guild_id = $1 AND status = 'published' AND submission_kind = 'confession' "
                    "AND posted_channel_id IS NOT NULL AND posted_message_id IS NOT NULL "
                    "ORDER BY COALESCE(published_at, created_at) ASC"
                ),
                guild_id,
            )
        return [record for row in rows if (record := _submission_from_row(row, self._privacy)) is not None]

    async def list_published_public_reply_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_submissions "
                    "WHERE guild_id = $1 AND status = 'published' AND submission_kind = 'reply' "
                    "AND reply_flow = 'reply_to_confession' "
                    "AND posted_channel_id IS NOT NULL AND posted_message_id IS NOT NULL "
                    "ORDER BY COALESCE(published_at, created_at) ASC"
                ),
                guild_id,
            )
        return [record for row in rows if (record := _submission_from_row(row, self._privacy)) is not None]

    async def list_recent_submissions_for_author(self, guild_id: int, author_user_id: int, *, limit: int = 5) -> list[dict[str, Any]]:
        author_lookup_hashes = list(
            self._privacy.blind_index_candidates(label="author-link", guild_id=guild_id, value=author_user_id)
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM ("
                    "SELECT s.* "
                    f"FROM {SECURE_AUTHOR_LINK_TABLE} a "
                    "JOIN confession_submissions s ON s.submission_id = a.submission_id "
                    "WHERE a.guild_id = $1 AND a.author_lookup_hash = ANY($2::text[]) "
                    "UNION ALL "
                    "SELECT s.* "
                    "FROM confession_author_links a "
                    "JOIN confession_submissions s ON s.submission_id = a.submission_id "
                    f"WHERE a.guild_id = $1 AND a.author_user_id = $3 AND NOT EXISTS (SELECT 1 FROM {SECURE_AUTHOR_LINK_TABLE} sa WHERE sa.submission_id = a.submission_id)"
                    ") recent_rows ORDER BY created_at DESC LIMIT $4"
                ),
                guild_id,
                author_lookup_hashes,
                author_user_id,
                limit,
            )
        return [record for row in rows if (record := _submission_from_row(row, self._privacy)) is not None]

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
        row = self._encode_author_link_row(normalized)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        f"INSERT INTO {SECURE_AUTHOR_LINK_TABLE} (submission_id, guild_id, author_lookup_hash, author_identity_ciphertext, created_at) "
                        "VALUES ($1, $2, $3, $4, $5) "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "guild_id = EXCLUDED.guild_id, "
                        "author_lookup_hash = EXCLUDED.author_lookup_hash, "
                        "author_identity_ciphertext = EXCLUDED.author_identity_ciphertext, "
                        "created_at = EXCLUDED.created_at"
                    ),
                    row["submission_id"],
                    row["guild_id"],
                    row["author_lookup_hash"],
                    row["author_identity_ciphertext"],
                    _parse_datetime(row["created_at"]),
                )
                await conn.execute("DELETE FROM confession_author_links WHERE submission_id = $1", row["submission_id"])

    async def fetch_author_link(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            secure_row = await conn.fetchrow(
                f"SELECT submission_id, guild_id, author_lookup_hash, author_identity_ciphertext, created_at FROM {SECURE_AUTHOR_LINK_TABLE} WHERE submission_id = $1",
                submission_id,
            )
            if secure_row is not None:
                return _author_link_from_secure_row(secure_row, self._privacy)
            row = await conn.fetchrow(
                "SELECT submission_id, guild_id, author_user_id, created_at FROM confession_author_links WHERE submission_id = $1",
                submission_id,
            )
        return _author_link_from_legacy_row(row)

    async def upsert_private_media(self, record: dict[str, Any]):
        normalized = normalize_private_media(record)
        if normalized is None:
            return
        row = self._encode_private_media_row(normalized)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_private_media (submission_id, guild_id, attachment_urls, attachment_payload, created_at, updated_at) "
                        "VALUES ($1, $2, $3::jsonb, $4, $5, $6) "
                        "ON CONFLICT (submission_id) DO UPDATE SET "
                        "guild_id = EXCLUDED.guild_id, "
                        "attachment_urls = EXCLUDED.attachment_urls, "
                        "attachment_payload = EXCLUDED.attachment_payload, "
                        "created_at = EXCLUDED.created_at, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    row["submission_id"],
                    row["guild_id"],
                    json.dumps(row["attachment_urls"]),
                    row["attachment_payload"],
                    _parse_datetime(row["created_at"]),
                    _parse_datetime(row["updated_at"]),
                )

    async def fetch_private_media(self, submission_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT submission_id, guild_id, attachment_urls, attachment_payload, created_at, updated_at FROM confession_private_media WHERE submission_id = $1",
                submission_id,
            )
        return _private_media_from_row(row, self._privacy)

    async def delete_private_media(self, submission_id: str):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM confession_private_media WHERE submission_id = $1", submission_id)

    async def upsert_owner_reply_opportunity(self, record: dict[str, Any]):
        normalized = normalize_owner_reply_opportunity(record)
        if normalized is None:
            return
        row = self._encode_owner_reply_opportunity_row(normalized)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_owner_reply_opportunities ("
                        "opportunity_id, guild_id, root_submission_id, root_confession_id, referenced_submission_id, "
                        "source_channel_id, source_message_id, source_author_user_id, source_author_lookup_hash, source_author_name, source_preview, source_message_fingerprint, private_payload, status, notification_status, "
                        "notification_channel_id, notification_message_id, created_at, expires_at, notified_at, resolved_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, "
                        "$6, $7, $8, $9, $10, $11, $12, $13, $14, "
                        "$15, $16, $17, $18, $19, $20, $21"
                        ") "
                        "ON CONFLICT (opportunity_id) DO UPDATE SET "
                        "guild_id = EXCLUDED.guild_id, "
                        "root_submission_id = EXCLUDED.root_submission_id, "
                        "root_confession_id = EXCLUDED.root_confession_id, "
                        "referenced_submission_id = EXCLUDED.referenced_submission_id, "
                        "source_channel_id = EXCLUDED.source_channel_id, "
                        "source_message_id = EXCLUDED.source_message_id, "
                        "source_author_user_id = EXCLUDED.source_author_user_id, "
                        "source_author_lookup_hash = EXCLUDED.source_author_lookup_hash, "
                        "source_author_name = EXCLUDED.source_author_name, "
                        "source_preview = EXCLUDED.source_preview, "
                        "source_message_fingerprint = EXCLUDED.source_message_fingerprint, "
                        "private_payload = EXCLUDED.private_payload, "
                        "status = EXCLUDED.status, "
                        "notification_status = EXCLUDED.notification_status, "
                        "notification_channel_id = EXCLUDED.notification_channel_id, "
                        "notification_message_id = EXCLUDED.notification_message_id, "
                        "created_at = EXCLUDED.created_at, "
                        "expires_at = EXCLUDED.expires_at, "
                        "notified_at = EXCLUDED.notified_at, "
                        "resolved_at = EXCLUDED.resolved_at"
                    ),
                    row["opportunity_id"],
                    row["guild_id"],
                    row["root_submission_id"],
                    row["root_confession_id"],
                    row["referenced_submission_id"],
                    row["source_channel_id"],
                    row["source_message_id"],
                    row["source_author_user_id"],
                    row["source_author_lookup_hash"],
                    row["source_author_name"],
                    row["source_preview"],
                    row["source_message_fingerprint"],
                    row["private_payload"],
                    row["status"],
                    row["notification_status"],
                    row["notification_channel_id"],
                    row["notification_message_id"],
                    _parse_datetime(row["created_at"]),
                    _parse_datetime(row["expires_at"]),
                    _parse_datetime(row["notified_at"]),
                    _parse_datetime(row["resolved_at"]),
                )

    async def fetch_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_owner_reply_opportunities WHERE opportunity_id = $1",
                opportunity_id,
            )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def fetch_owner_reply_opportunity_by_source_message_id(self, guild_id: int, source_message_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_owner_reply_opportunities WHERE guild_id = $1 AND source_message_id = $2",
                guild_id,
                source_message_id,
            )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def fetch_owner_reply_opportunity_by_notification_message_id(self, notification_message_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_owner_reply_opportunities WHERE notification_message_id = $1",
                notification_message_id,
            )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def fetch_pending_owner_reply_opportunity_for_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
    ) -> dict[str, Any] | None:
        source_lookup_hashes = list(
            self._privacy.blind_index_candidates(
                label="owner-reply-source-author",
                guild_id=guild_id,
                value=source_author_user_id,
            )
        )
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE guild_id = $1 AND root_submission_id = $2 AND referenced_submission_id = $3 "
                    "AND (source_author_lookup_hash = ANY($4::text[]) OR source_author_user_id = $5) AND status = 'pending' "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                guild_id,
                root_submission_id,
                referenced_submission_id,
                source_lookup_hashes,
                source_author_user_id,
            )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def list_pending_owner_reply_opportunities_for_author(
        self,
        guild_id: int,
        author_user_id: int,
        *,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        author_lookup_hashes = list(
            self._privacy.blind_index_candidates(label="author-link", guild_id=guild_id, value=author_user_id)
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM ("
                    "SELECT o.* "
                    "FROM confession_owner_reply_opportunities o "
                    f"JOIN {SECURE_AUTHOR_LINK_TABLE} a ON a.submission_id = o.root_submission_id "
                    "WHERE o.guild_id = $1 AND a.author_lookup_hash = ANY($2::text[]) AND o.status = 'pending' "
                    "UNION ALL "
                    "SELECT o.* "
                    "FROM confession_owner_reply_opportunities o "
                    "JOIN confession_author_links a ON a.submission_id = o.root_submission_id "
                    f"WHERE o.guild_id = $1 AND a.author_user_id = $3 AND o.status = 'pending' AND NOT EXISTS (SELECT 1 FROM {SECURE_AUTHOR_LINK_TABLE} sa WHERE sa.submission_id = a.submission_id)"
                    ") owner_rows ORDER BY created_at DESC LIMIT $4"
                ),
                guild_id,
                author_lookup_hashes,
                author_user_id,
                limit,
            )
        return [record for row in rows if (record := _owner_reply_opportunity_from_row(row, self._privacy)) is not None]

    async def list_owner_reply_opportunities_for_root_submission(
        self,
        root_submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE root_submission_id = $1 "
                    "ORDER BY created_at DESC LIMIT $2"
                ),
                root_submission_id,
                limit,
            )
        return [record for row in rows if (record := _owner_reply_opportunity_from_row(row, self._privacy)) is not None]

    async def list_owner_reply_opportunities_for_submission(
        self,
        submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE root_submission_id = $1 OR referenced_submission_id = $1 "
                    "ORDER BY created_at DESC LIMIT $2"
                ),
                submission_id,
                limit,
            )
        return [record for row in rows if (record := _owner_reply_opportunity_from_row(row, self._privacy)) is not None]

    async def list_owner_reply_opportunities_for_responder_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        source_lookup_hashes = list(
            self._privacy.blind_index_candidates(
                label="owner-reply-source-author",
                guild_id=guild_id,
                value=source_author_user_id,
            )
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE guild_id = $1 AND root_submission_id = $2 AND referenced_submission_id = $3 "
                    "AND (source_author_lookup_hash = ANY($4::text[]) OR source_author_user_id = $5) "
                    "ORDER BY created_at DESC LIMIT $6"
                ),
                guild_id,
                root_submission_id,
                referenced_submission_id,
                source_lookup_hashes,
                source_author_user_id,
                limit,
            )
        return [record for row in rows if (record := _owner_reply_opportunity_from_row(row, self._privacy)) is not None]

    async def list_owner_reply_opportunities_for_source_author(
        self,
        guild_id: int,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        source_lookup_hashes = list(
            self._privacy.blind_index_candidates(
                label="owner-reply-source-author",
                guild_id=guild_id,
                value=source_author_user_id,
            )
        )
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE guild_id = $1 AND (source_author_lookup_hash = ANY($2::text[]) OR source_author_user_id = $3) "
                    "ORDER BY created_at DESC LIMIT $4"
                ),
                guild_id,
                source_lookup_hashes,
                source_author_user_id,
                limit,
            )
        return [record for row in rows if (record := _owner_reply_opportunity_from_row(row, self._privacy)) is not None]

    async def claim_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    (
                        "UPDATE confession_owner_reply_opportunities "
                        "SET status = 'locked' "
                        "WHERE opportunity_id = $1 AND status = 'pending' "
                        "RETURNING *"
                    ),
                    opportunity_id,
                )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def release_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    (
                        "UPDATE confession_owner_reply_opportunities "
                        "SET status = 'pending' "
                        "WHERE opportunity_id = $1 AND status = 'locked' "
                        "RETURNING *"
                    ),
                    opportunity_id,
                )
        return _owner_reply_opportunity_from_row(row, self._privacy)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        lookup_hashes = list(self._privacy.blind_index_candidates(label="enforcement-state", guild_id=guild_id, value=user_id))
        async with self._pool.acquire() as conn:
            secure_row = await conn.fetchrow(
                f"SELECT * FROM {SECURE_ENFORCEMENT_TABLE} WHERE guild_id = $1 AND user_lookup_hash = ANY($2::text[])",
                guild_id,
                lookup_hashes,
            )
            if secure_row is not None:
                return _enforcement_from_secure_row(secure_row, self._privacy)
            row = await conn.fetchrow(
                "SELECT * FROM confession_enforcement_states WHERE guild_id = $1 AND user_id = $2",
                guild_id,
                user_id,
            )
        return _enforcement_from_legacy_row(row)

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        normalized = normalize_enforcement_state(record)
        if normalized is None:
            return
        row = self._encode_enforcement_state_row(normalized)
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        f"INSERT INTO {SECURE_ENFORCEMENT_TABLE} ("
                        "guild_id, user_lookup_hash, user_identity_ciphertext, active_restriction, restricted_until, is_permanent_ban, strike_count, last_strike_at, cooldown_until, "
                        "burst_count, burst_window_started_at, last_case_id, image_restriction_active, image_restricted_until, image_restriction_case_id, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, "
                        "$10, $11, $12, $13, $14, $15, $16"
                        ") "
                        "ON CONFLICT (guild_id, user_lookup_hash) DO UPDATE SET "
                        "user_identity_ciphertext = EXCLUDED.user_identity_ciphertext, "
                        "active_restriction = EXCLUDED.active_restriction, "
                        "restricted_until = EXCLUDED.restricted_until, "
                        "is_permanent_ban = EXCLUDED.is_permanent_ban, "
                        "strike_count = EXCLUDED.strike_count, "
                        "last_strike_at = EXCLUDED.last_strike_at, "
                        "cooldown_until = EXCLUDED.cooldown_until, "
                        "burst_count = EXCLUDED.burst_count, "
                        "burst_window_started_at = EXCLUDED.burst_window_started_at, "
                        "last_case_id = EXCLUDED.last_case_id, "
                        "image_restriction_active = EXCLUDED.image_restriction_active, "
                        "image_restricted_until = EXCLUDED.image_restricted_until, "
                        "image_restriction_case_id = EXCLUDED.image_restriction_case_id, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    row["guild_id"],
                    row["user_lookup_hash"],
                    row["user_identity_ciphertext"],
                    row["active_restriction"],
                    _parse_datetime(row["restricted_until"]),
                    row["is_permanent_ban"],
                    row["strike_count"],
                    _parse_datetime(row["last_strike_at"]),
                    _parse_datetime(row["cooldown_until"]),
                    row["burst_count"],
                    _parse_datetime(row["burst_window_started_at"]),
                    row["last_case_id"],
                    row["image_restriction_active"],
                    _parse_datetime(row["image_restricted_until"]),
                    row["image_restriction_case_id"],
                    _parse_datetime(row["updated_at"]),
                )
                await conn.execute(
                    "DELETE FROM confession_enforcement_states WHERE guild_id = $1 AND user_id = $2",
                    normalized["guild_id"],
                    normalized["user_id"],
                )

    async def list_active_enforcement_states(self, *, guild_id: int | None = None) -> list[dict[str, Any]]:
        guild_clause = "WHERE guild_id = $1 AND " if guild_id is not None else "WHERE "
        secure_params: tuple[object, ...] = (guild_id,) if guild_id is not None else ()
        legacy_params: tuple[object, ...] = (guild_id,) if guild_id is not None else ()
        async with self._pool.acquire() as conn:
            secure_rows = await conn.fetch(
                (
                    f"SELECT * FROM {SECURE_ENFORCEMENT_TABLE} "
                    f"{guild_clause}(is_permanent_ban = TRUE OR active_restriction <> 'none' OR image_restriction_active = TRUE) "
                    "ORDER BY guild_id ASC, user_lookup_hash ASC"
                ),
                *secure_params,
            )
            legacy_rows = await conn.fetch(
                (
                    "SELECT * FROM confession_enforcement_states "
                    f"{guild_clause}(is_permanent_ban = TRUE OR active_restriction <> 'none' OR image_restriction_active = TRUE) "
                    "ORDER BY guild_id ASC, user_id ASC"
                ),
                *legacy_params,
            )
        rows: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()
        for secure_row in secure_rows:
            decoded = _enforcement_from_secure_row(secure_row, self._privacy)
            if decoded is None or not _enforcement_state_requires_gate_cache(decoded):
                continue
            key = (decoded["guild_id"], decoded["user_id"])
            seen.add(key)
            rows.append(decoded)
        for legacy_row in legacy_rows:
            decoded = _enforcement_from_legacy_row(legacy_row)
            if decoded is None:
                continue
            key = (decoded["guild_id"], decoded["user_id"])
            if key in seen or not _enforcement_state_requires_gate_cache(decoded):
                continue
            rows.append(decoded)
        rows.sort(key=lambda item: (int(item.get("guild_id") or 0), int(item.get("user_id") or 0)))
        return rows

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
                    "s.submission_id, s.guild_id, s.confession_id, s.submission_kind, s.reply_flow, s.owner_reply_generation, s.parent_confession_id, "
                    "s.staff_preview, s.content_body, s.shared_link_url, s.content_ciphertext, s.flag_codes, s.attachment_meta, s.created_at "
                    "FROM confession_cases c "
                    "JOIN confession_submissions s ON s.submission_id = c.submission_id "
                    "WHERE c.guild_id = $1 AND c.status = 'open' AND c.case_kind = 'review' AND s.status = 'queued' "
                    "ORDER BY s.created_at ASC LIMIT $2"
                ),
                guild_id,
                limit,
            )
            submission_ids = [str(row["submission_id"]) for row in rows if row.get("submission_id")]
            media_rows = []
            if submission_ids:
                media_rows = await conn.fetch(
                    "SELECT submission_id, guild_id, attachment_urls, attachment_payload, created_at, updated_at "
                    "FROM confession_private_media WHERE submission_id = ANY($1::text[])",
                    submission_ids,
                )
        media_by_submission = {
            str(record["submission_id"]): _private_media_from_row(record, self._privacy) for record in media_rows
        }
        surfaces = []
        for row in rows:
            payload = _decrypt_payload(
                self._privacy,
                label="review surface submission content",
                domain="submission-content",
                aad_fields={
                    "guild_id": row["guild_id"],
                    "submission_id": row["submission_id"],
                    "confession_id": row["confession_id"],
                },
                envelope=row.get("content_ciphertext"),
                key_domain="content",
            ) or {}
            surfaces.append(
                {
                    "case_id": row["case_id"],
                    "confession_id": row["confession_id"],
                    "case_kind": row["case_kind"],
                    "status": row["status"],
                    "review_version": int(row["review_version"] or 0),
                    "submission_kind": row["submission_kind"] or "confession",
                    "reply_flow": row.get("reply_flow"),
                    "owner_reply_generation": row.get("owner_reply_generation"),
                    "parent_confession_id": row["parent_confession_id"],
                    "staff_preview": payload.get("staff_preview", row["staff_preview"]),
                    "flag_codes": decode_postgres_json_array(row["flag_codes"], label="confession_submissions.flag_codes"),
                    "attachment_meta": decode_postgres_json_array(row["attachment_meta"], label="confession_submissions.attachment_meta"),
                    "attachment_urls": list((media_by_submission.get(str(row["submission_id"])) or {}).get("attachment_urls") or []),
                    "shared_link_url": payload.get("shared_link_url", row["shared_link_url"]),
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

    async def fetch_support_ticket(self, guild_id: int, ticket_id: str) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM confession_support_tickets WHERE guild_id = $1 AND ticket_id = $2",
                guild_id,
                ticket_id,
            )
        return _support_ticket_from_row(row)

    async def list_support_tickets(self, guild_id: int, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        cleaned_status = str(status).strip().lower() if status is not None else None
        async with self._pool.acquire() as conn:
            if cleaned_status is None:
                rows = await conn.fetch(
                    "SELECT * FROM confession_support_tickets WHERE guild_id = $1 ORDER BY created_at DESC LIMIT $2",
                    guild_id,
                    limit,
                )
            else:
                rows = await conn.fetch(
                    "SELECT * FROM confession_support_tickets WHERE guild_id = $1 AND status = $2 ORDER BY created_at DESC LIMIT $3",
                    guild_id,
                    cleaned_status,
                    limit,
                )
        return [record for row in rows if (record := _support_ticket_from_row(row)) is not None]

    async def upsert_support_ticket(self, record: dict[str, Any]):
        normalized = normalize_support_ticket(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO confession_support_tickets ("
                        "ticket_id, guild_id, kind, action_target_id, reference_confession_id, reference_case_id, context_label, details, "
                        "status, resolution_action, message_channel_id, message_id, created_at, resolved_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14"
                        ") "
                        "ON CONFLICT (guild_id, ticket_id) DO UPDATE SET "
                        "kind = EXCLUDED.kind, "
                        "action_target_id = EXCLUDED.action_target_id, "
                        "reference_confession_id = EXCLUDED.reference_confession_id, "
                        "reference_case_id = EXCLUDED.reference_case_id, "
                        "context_label = EXCLUDED.context_label, "
                        "details = EXCLUDED.details, "
                        "status = EXCLUDED.status, "
                        "resolution_action = EXCLUDED.resolution_action, "
                        "message_channel_id = EXCLUDED.message_channel_id, "
                        "message_id = EXCLUDED.message_id, "
                        "resolved_at = EXCLUDED.resolved_at"
                    ),
                    normalized["ticket_id"],
                    normalized["guild_id"],
                    normalized["kind"],
                    normalized["action_target_id"],
                    normalized["reference_confession_id"],
                    normalized["reference_case_id"],
                    normalized["context_label"],
                    normalized["details"],
                    normalized["status"],
                    normalized["resolution_action"],
                    normalized["message_channel_id"],
                    normalized["message_id"],
                    _parse_datetime(normalized["created_at"]),
                    _parse_datetime(normalized["resolved_at"]),
                )

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

    async def fetch_privacy_status(self, guild_id: int | None = None) -> dict[str, Any]:
        status = _empty_privacy_status(scope="guild" if guild_id is not None else "global", guild_id=guild_id)
        where_clause = " WHERE guild_id = $1" if guild_id is not None else ""
        params: tuple[Any, ...] = (guild_id,) if guild_id is not None else ()
        async with self._pool.acquire() as conn:
            submission_rows = await conn.fetch(
                (
                    "SELECT guild_id, staff_preview, content_body, shared_link_url, reply_target_label, reply_target_preview, content_ciphertext, "
                    "content_fingerprint, similarity_key, fuzzy_signature "
                    "FROM confession_submissions"
                    f"{where_clause}"
                ),
                *params,
            )
            private_media_rows = await conn.fetch(
                (
                    "SELECT guild_id, attachment_urls, attachment_payload "
                    "FROM confession_private_media"
                    f"{where_clause}"
                ),
                *params,
            )
            secure_author_rows = await conn.fetch(
                (
                    f"SELECT guild_id, author_lookup_hash, author_identity_ciphertext FROM {SECURE_AUTHOR_LINK_TABLE}"
                    f"{where_clause}"
                ),
                *params,
            )
            legacy_author_rows = await conn.fetch(
                f"SELECT guild_id FROM confession_author_links{where_clause}",
                *params,
            )
            owner_reply_rows = await conn.fetch(
                (
                    "SELECT guild_id, source_author_user_id, source_author_lookup_hash, source_author_name, "
                    "source_preview, source_message_fingerprint, private_payload "
                    "FROM confession_owner_reply_opportunities"
                    f"{where_clause}"
                ),
                *params,
            )
            secure_enforcement_rows = await conn.fetch(
                (
                    f"SELECT guild_id, user_lookup_hash, user_identity_ciphertext FROM {SECURE_ENFORCEMENT_TABLE}"
                    f"{where_clause}"
                ),
                *params,
            )
            legacy_enforcement_rows = await conn.fetch(
                f"SELECT guild_id FROM confession_enforcement_states{where_clause}",
                *params,
            )
        for row in submission_rows:
            _apply_privacy_categories(status, _submission_privacy_categories(row, self._privacy))
        for row in private_media_rows:
            _apply_privacy_categories(status, _private_media_privacy_categories(row, self._privacy))
        for row in secure_author_rows:
            _apply_privacy_categories(status, _author_link_privacy_categories(row, self._privacy))
        for _ in legacy_author_rows:
            _apply_privacy_categories(status, {"legacy_author_links"})
        for row in owner_reply_rows:
            _apply_privacy_categories(status, _owner_reply_privacy_categories(row, self._privacy))
        for row in secure_enforcement_rows:
            _apply_privacy_categories(status, _enforcement_privacy_categories(row, self._privacy))
        for _ in legacy_enforcement_rows:
            _apply_privacy_categories(status, {"legacy_enforcement_rows"})
        return _finalize_privacy_status(status)

    async def run_privacy_backfill(self, *, apply: bool, batch_size: int = 100) -> dict[str, Any]:
        summary = {
            "mode": "apply" if apply else "dry-run",
            "submissions": 0,
            "private_media": 0,
            "author_links": 0,
            "owner_reply_opportunities": 0,
            "enforcement_states": 0,
            "batch_size": batch_size,
        }
        active_content_prefix = self._privacy.active_envelope_prefix(key_domain="content")
        active_identity_prefix = self._privacy.active_envelope_prefix(key_domain="identity")
        active_blind_prefix = self._privacy.active_blind_index_prefix()
        active_exact_prefix = self._privacy.active_exact_duplicate_hash_prefix()
        active_fuzzy_prefix = self._privacy.active_fuzzy_signature_prefix()
        async with self._pool.acquire() as conn:
            submission_rows = await conn.fetch(
                (
                    "SELECT * FROM confession_submissions "
                    "WHERE staff_preview IS NOT NULL OR content_body IS NOT NULL OR shared_link_url IS NOT NULL "
                    "OR reply_target_label IS NOT NULL OR reply_target_preview IS NOT NULL OR similarity_key IS NOT NULL "
                    "OR (content_ciphertext IS NOT NULL AND content_ciphertext NOT LIKE $1) "
                    "OR (content_fingerprint IS NOT NULL AND content_fingerprint NOT LIKE $2) "
                    "OR (fuzzy_signature IS NOT NULL AND fuzzy_signature NOT LIKE $3) "
                    "ORDER BY created_at ASC LIMIT $4"
                ),
                active_content_prefix + "%",
                active_exact_prefix + "%",
                active_fuzzy_prefix + "%",
                batch_size,
            )
            private_media_rows = await conn.fetch(
                (
                    "SELECT submission_id, guild_id, attachment_urls, attachment_payload, created_at, updated_at "
                    "FROM confession_private_media "
                    "WHERE attachment_urls <> '[]'::jsonb "
                    "OR (attachment_payload IS NOT NULL AND attachment_payload NOT LIKE $1) "
                    "ORDER BY updated_at ASC LIMIT $2"
                ),
                active_content_prefix + "%",
                batch_size,
            )
            author_link_rows = await conn.fetch(
                (
                    "SELECT submission_id, guild_id, author_user_id, created_at "
                    "FROM confession_author_links legacy "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {SECURE_AUTHOR_LINK_TABLE} secure WHERE secure.submission_id = legacy.submission_id) "
                    "ORDER BY created_at ASC LIMIT $1"
                ),
                batch_size,
            )
            secure_author_rows = await conn.fetch(
                (
                    f"SELECT submission_id, guild_id, author_lookup_hash, author_identity_ciphertext, created_at "
                    f"FROM {SECURE_AUTHOR_LINK_TABLE} "
                    "WHERE author_lookup_hash NOT LIKE $1 OR author_identity_ciphertext NOT LIKE $2 "
                    "ORDER BY created_at ASC LIMIT $3"
                ),
                active_blind_prefix + "%",
                active_identity_prefix + "%",
                batch_size,
            )
            owner_reply_rows = await conn.fetch(
                (
                    "SELECT * FROM confession_owner_reply_opportunities "
                    "WHERE source_author_user_id IS NOT NULL "
                    "OR source_message_fingerprint IS NOT NULL "
                    "OR source_author_name IS DISTINCT FROM $1 "
                    "OR source_preview IS DISTINCT FROM $2 "
                    "OR private_payload IS NULL "
                    "OR (source_author_lookup_hash IS NOT NULL AND source_author_lookup_hash NOT LIKE $3) "
                    "OR (private_payload IS NOT NULL AND private_payload NOT LIKE $4) "
                    "ORDER BY created_at ASC LIMIT $5"
                ),
                PROTECTED_OWNER_REPLY_NAME,
                PROTECTED_OWNER_REPLY_PREVIEW,
                active_blind_prefix + "%",
                active_content_prefix + "%",
                batch_size,
            )
            enforcement_rows = await conn.fetch(
                (
                    "SELECT * FROM confession_enforcement_states "
                    "ORDER BY COALESCE(updated_at, timezone('utc', now())) ASC LIMIT $1"
                ),
                batch_size,
            )
            secure_enforcement_rows = await conn.fetch(
                (
                    f"SELECT * FROM {SECURE_ENFORCEMENT_TABLE} "
                    "WHERE user_lookup_hash NOT LIKE $1 OR user_identity_ciphertext NOT LIKE $2 "
                    "ORDER BY COALESCE(updated_at, timezone('utc', now())) ASC LIMIT $3"
                ),
                active_blind_prefix + "%",
                active_identity_prefix + "%",
                batch_size,
            )
        summary["submissions"] = len(submission_rows)
        summary["private_media"] = len(private_media_rows)
        summary["author_links"] = len(author_link_rows) + len(secure_author_rows)
        summary["owner_reply_opportunities"] = len(owner_reply_rows)
        summary["enforcement_states"] = len(enforcement_rows) + len(secure_enforcement_rows)
        if not apply:
            summary["privacy_status"] = await self.fetch_privacy_status()
            return summary

        for row in submission_rows:
            normalized = _submission_from_row(row, self._privacy)
            if normalized is None:
                continue
            if _submission_requires_sensitive_payload(normalized):
                await self.upsert_submission(_backfill_submission_duplicate_fields(self._privacy, normalized))
            else:
                terminal = _backfill_submission_duplicate_fields(self._privacy, normalized)
                terminal["reply_target_label"] = None
                terminal["reply_target_preview"] = None
                terminal["staff_preview"] = None
                terminal["content_body"] = None
                terminal["shared_link_url"] = None
                terminal["attachment_meta"] = []
                await self.upsert_submission(terminal)
                await self.delete_private_media(terminal["submission_id"])

        for row in private_media_rows:
            normalized = _private_media_from_row(row, self._privacy)
            if normalized is None:
                continue
            submission = await self.fetch_submission(normalized["submission_id"])
            if submission is not None and not _submission_requires_sensitive_payload(submission):
                await self.delete_private_media(normalized["submission_id"])
                continue
            await self.upsert_private_media(normalized)

        for row in author_link_rows:
            normalized = normalize_author_link(
                {
                    "submission_id": row["submission_id"],
                    "guild_id": row["guild_id"],
                    "author_user_id": row["author_user_id"],
                    "created_at": row["created_at"],
                }
            )
            if normalized is not None:
                await self.upsert_author_link(normalized)
        for row in secure_author_rows:
            normalized = _author_link_from_secure_row(row, self._privacy)
            if normalized is not None:
                await self.upsert_author_link(normalized)

        for row in owner_reply_rows:
            normalized = _owner_reply_opportunity_from_row(row, self._privacy)
            if normalized is not None:
                await self.upsert_owner_reply_opportunity(normalized)

        for row in enforcement_rows:
            normalized = _enforcement_from_legacy_row(row)
            if normalized is not None:
                await self.upsert_enforcement_state(normalized)
        for row in secure_enforcement_rows:
            normalized = _enforcement_from_secure_row(row, self._privacy)
            if normalized is not None:
                await self.upsert_enforcement_state(normalized)
        summary["privacy_status"] = await self.fetch_privacy_status()
        return summary


class ConfessionsStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        requested_backend = (backend or os.getenv("CONFESSIONS_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.backend_name = requested_backend
        try:
            self.privacy = ConfessionsCrypto.from_environment(backend_name=requested_backend)
        except ConfessionsKeyConfigError as exc:
            raise ConfessionsStorageUnavailable(str(exc)) from exc
        self._store: _BaseConfessionsStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        LOGGER.info(
            "Confessions storage init: backend_preference=%s, database_url_configured=%s, database_url_source=%s, privacy_source=%s",
            requested_backend,
            "yes" if self.database_url else "no",
            self.database_url_source or "none",
            "ephemeral" if self.privacy.status.ephemeral else "environment",
        )
        if requested_backend in {"memory", "test", "dev"}:
            self._store = _MemoryConfessionsStore(self.privacy)
        elif requested_backend in {"postgres", "postgresql", "supabase", "auto"}:
            if not self.database_url:
                raise ConfessionsStorageUnavailable(
                    "No Postgres confessions database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL."
                )
            self._store = _PostgresConfessionsStore(self.database_url, self.privacy)
        else:
            raise ConfessionsStorageUnavailable(f"Unsupported confessions storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        LOGGER.info("Confessions storage init succeeded: backend=%s", self.backend_name)

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

    async def list_published_top_level_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        return await self._store.list_published_top_level_submissions(guild_id)

    async def list_published_public_reply_submissions(self, guild_id: int) -> list[dict[str, Any]]:
        return await self._store.list_published_public_reply_submissions(guild_id)

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

    async def upsert_owner_reply_opportunity(self, record: dict[str, Any]):
        await self._store.upsert_owner_reply_opportunity(record)

    async def fetch_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_owner_reply_opportunity(opportunity_id)

    async def fetch_owner_reply_opportunity_by_source_message_id(self, guild_id: int, source_message_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_owner_reply_opportunity_by_source_message_id(guild_id, source_message_id)

    async def fetch_owner_reply_opportunity_by_notification_message_id(self, notification_message_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_owner_reply_opportunity_by_notification_message_id(notification_message_id)

    async def fetch_pending_owner_reply_opportunity_for_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
    ) -> dict[str, Any] | None:
        return await self._store.fetch_pending_owner_reply_opportunity_for_path(
            guild_id,
            root_submission_id,
            referenced_submission_id,
            source_author_user_id,
        )

    async def list_pending_owner_reply_opportunities_for_author(
        self,
        guild_id: int,
        author_user_id: int,
        *,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        return await self._store.list_pending_owner_reply_opportunities_for_author(guild_id, author_user_id, limit=limit)

    async def list_owner_reply_opportunities_for_root_submission(
        self,
        root_submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        return await self._store.list_owner_reply_opportunities_for_root_submission(root_submission_id, limit=limit)

    async def list_owner_reply_opportunities_for_submission(
        self,
        submission_id: str,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        return await self._store.list_owner_reply_opportunities_for_submission(submission_id, limit=limit)

    async def list_owner_reply_opportunities_for_responder_path(
        self,
        guild_id: int,
        root_submission_id: str,
        referenced_submission_id: str,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        return await self._store.list_owner_reply_opportunities_for_responder_path(
            guild_id,
            root_submission_id,
            referenced_submission_id,
            source_author_user_id,
            limit=limit,
        )

    async def list_owner_reply_opportunities_for_source_author(
        self,
        guild_id: int,
        source_author_user_id: int,
        *,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        return await self._store.list_owner_reply_opportunities_for_source_author(
            guild_id,
            source_author_user_id,
            limit=limit,
        )

    async def claim_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        return await self._store.claim_owner_reply_opportunity(opportunity_id)

    async def release_owner_reply_opportunity(self, opportunity_id: str) -> dict[str, Any] | None:
        return await self._store.release_owner_reply_opportunity(opportunity_id)

    async def fetch_enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_enforcement_state(guild_id, user_id)

    async def upsert_enforcement_state(self, record: dict[str, Any]):
        await self._store.upsert_enforcement_state(record)

    async def list_active_enforcement_states(self, *, guild_id: int | None = None) -> list[dict[str, Any]]:
        return await self._store.list_active_enforcement_states(guild_id=guild_id)

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

    async def fetch_support_ticket(self, guild_id: int, ticket_id: str) -> dict[str, Any] | None:
        return await self._store.fetch_support_ticket(guild_id, ticket_id)

    async def list_support_tickets(self, guild_id: int, *, status: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        return await self._store.list_support_tickets(guild_id, status=status, limit=limit)

    async def upsert_support_ticket(self, record: dict[str, Any]):
        await self._store.upsert_support_ticket(record)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        return await self._store.fetch_guild_counts(guild_id)

    async def fetch_privacy_status(self, guild_id: int | None = None) -> dict[str, Any]:
        return await self._store.fetch_privacy_status(guild_id)

    async def run_privacy_backfill(self, *, apply: bool, batch_size: int = 100) -> dict[str, Any]:
        return await self._store.run_privacy_backfill(apply=apply, batch_size=batch_size)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
