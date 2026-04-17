from __future__ import annotations

import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array


DEFAULT_BACKEND = "postgres"
VALID_FOLLOWUP_MODES = {"auto_remove", "review"}
VALID_FOLLOWUP_DURATION_UNITS = {"days", "weeks", "months"}
VALID_VERIFICATION_LOGIC = {"must_have_role", "must_not_have_role"}
VALID_VERIFICATION_DEADLINE_ACTIONS = {"auto_kick", "review"}
VALID_CHANNEL_LOCK_PERMISSION_NAMES = frozenset(
    {
        "send_messages",
        "create_public_threads",
        "create_private_threads",
        "send_messages_in_threads",
        "add_reactions",
    }
)


class AdminStorageUnavailable(RuntimeError):
    pass


def default_admin_config(guild_id: int | None = None) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "followup_enabled": False,
        "followup_role_id": None,
        "followup_mode": "review",
        "followup_duration_value": 30,
        "followup_duration_unit": "days",
        "verification_enabled": False,
        "verification_role_id": None,
        "verification_logic": "must_have_role",
        "verification_deadline_action": "auto_kick",
        "verification_kick_after_seconds": 7 * 24 * 3600,
        "verification_warning_lead_seconds": 24 * 3600,
        "verification_help_channel_id": None,
        "verification_help_extension_seconds": 3 * 24 * 3600,
        "verification_max_extensions": 1,
        "admin_log_channel_id": None,
        "admin_alert_role_id": None,
        "warning_template": None,
        "kick_template": None,
        "invite_link": None,
        "lock_notice_template": None,
        "lock_admin_only": False,
        "excluded_user_ids": [],
        "excluded_role_ids": [],
        "trusted_role_ids": [],
        "followup_exempt_staff": True,
        "verification_exempt_staff": True,
        "verification_exempt_bots": True,
    }


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


def _clean_int_list(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set, frozenset)):
        return []
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _clean_optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


def _clean_permission_name_list(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set, frozenset)):
        return []
    cleaned = {
        str(value).strip()
        for value in values
        if str(value).strip() in VALID_CHANNEL_LOCK_PERMISSION_NAMES
    }
    return sorted(cleaned)


def _clean_permission_state_map(value: Any) -> dict[str, bool | None]:
    if not isinstance(value, dict):
        return {}
    cleaned: dict[str, bool | None] = {}
    for key, raw_state in value.items():
        name = str(key).strip()
        if name not in VALID_CHANNEL_LOCK_PERMISSION_NAMES:
            continue
        if raw_state is None or isinstance(raw_state, bool):
            cleaned[name] = raw_state
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


def _coerce_storage_datetime(value: Any) -> datetime | None:
    return _parse_datetime(value)


def _coerce_storage_datetimes(record: dict[str, Any], *fields: str) -> dict[str, Any]:
    coerced = dict(record)
    for field in fields:
        coerced[field] = _coerce_storage_datetime(coerced.get(field))
    return coerced


def normalize_admin_config(guild_id: int, payload: Any) -> dict[str, Any]:
    cleaned = default_admin_config(guild_id)
    if not isinstance(payload, dict):
        return cleaned
    cleaned["followup_enabled"] = bool(payload.get("followup_enabled"))
    cleaned["followup_role_id"] = payload.get("followup_role_id") if isinstance(payload.get("followup_role_id"), int) else None
    followup_mode = str(payload.get("followup_mode", "review")).strip().lower()
    cleaned["followup_mode"] = followup_mode if followup_mode in VALID_FOLLOWUP_MODES else "review"
    followup_duration_value = payload.get("followup_duration_value")
    cleaned["followup_duration_value"] = followup_duration_value if isinstance(followup_duration_value, int) and 1 <= followup_duration_value <= 365 else 30
    followup_duration_unit = str(payload.get("followup_duration_unit", "days")).strip().lower()
    cleaned["followup_duration_unit"] = followup_duration_unit if followup_duration_unit in VALID_FOLLOWUP_DURATION_UNITS else "days"
    cleaned["verification_enabled"] = bool(payload.get("verification_enabled"))
    cleaned["verification_role_id"] = payload.get("verification_role_id") if isinstance(payload.get("verification_role_id"), int) else None
    verification_logic = str(payload.get("verification_logic", "must_have_role")).strip().lower()
    cleaned["verification_logic"] = verification_logic if verification_logic in VALID_VERIFICATION_LOGIC else "must_have_role"
    verification_deadline_action = str(payload.get("verification_deadline_action", "auto_kick")).strip().lower()
    cleaned["verification_deadline_action"] = verification_deadline_action if verification_deadline_action in VALID_VERIFICATION_DEADLINE_ACTIONS else "auto_kick"
    for field, default_value, minimum, maximum in (
        ("verification_kick_after_seconds", 7 * 24 * 3600, 3600, 365 * 24 * 3600),
        ("verification_warning_lead_seconds", 24 * 3600, 60, 90 * 24 * 3600),
        ("verification_help_extension_seconds", 3 * 24 * 3600, 60, 30 * 24 * 3600),
    ):
        value = payload.get(field)
        cleaned[field] = value if isinstance(value, int) and minimum <= value <= maximum else default_value
    cleaned["verification_help_channel_id"] = payload.get("verification_help_channel_id") if isinstance(payload.get("verification_help_channel_id"), int) else None
    max_extensions = payload.get("verification_max_extensions")
    cleaned["verification_max_extensions"] = max_extensions if isinstance(max_extensions, int) and 0 <= max_extensions <= 5 else 1
    cleaned["admin_log_channel_id"] = payload.get("admin_log_channel_id") if isinstance(payload.get("admin_log_channel_id"), int) else None
    cleaned["admin_alert_role_id"] = payload.get("admin_alert_role_id") if isinstance(payload.get("admin_alert_role_id"), int) else None
    cleaned["warning_template"] = _clean_optional_text(payload.get("warning_template"))
    cleaned["kick_template"] = _clean_optional_text(payload.get("kick_template"))
    cleaned["invite_link"] = _clean_optional_text(payload.get("invite_link"))
    cleaned["lock_notice_template"] = _clean_optional_text(payload.get("lock_notice_template"))
    cleaned["lock_admin_only"] = bool(payload.get("lock_admin_only", False))
    for field in ("excluded_user_ids", "excluded_role_ids", "trusted_role_ids"):
        cleaned[field] = _clean_int_list(payload.get(field))
    cleaned["followup_exempt_staff"] = bool(payload.get("followup_exempt_staff", True))
    cleaned["verification_exempt_staff"] = bool(payload.get("verification_exempt_staff", True))
    cleaned["verification_exempt_bots"] = bool(payload.get("verification_exempt_bots", True))
    return cleaned


def normalize_ban_candidate(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    user_id = payload.get("user_id")
    banned_at = _serialize_datetime(_parse_datetime(payload.get("banned_at")))
    expires_at = _serialize_datetime(_parse_datetime(payload.get("expires_at")))
    if not isinstance(guild_id, int) or guild_id <= 0 or not isinstance(user_id, int) or user_id <= 0:
        return None
    if banned_at is None or expires_at is None:
        return None
    return {"guild_id": guild_id, "user_id": user_id, "banned_at": banned_at, "expires_at": expires_at}


def normalize_followup_record(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    user_id = payload.get("user_id")
    role_id = payload.get("role_id")
    assigned_at = _serialize_datetime(_parse_datetime(payload.get("assigned_at")))
    due_at = _serialize_datetime(_parse_datetime(payload.get("due_at")))
    mode = str(payload.get("mode", "review")).strip().lower()
    review_version = payload.get("review_version")
    review_message_channel_id = payload.get("review_message_channel_id")
    review_message_id = payload.get("review_message_id")
    if not all(isinstance(value, int) and value > 0 for value in (guild_id, user_id, role_id)):
        return None
    if assigned_at is None or mode not in VALID_FOLLOWUP_MODES:
        return None
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "role_id": role_id,
        "assigned_at": assigned_at,
        "due_at": due_at,
        "mode": mode,
        "review_pending": bool(payload.get("review_pending")),
        "review_version": review_version if isinstance(review_version, int) and review_version >= 0 else 0,
        "review_message_channel_id": review_message_channel_id if isinstance(review_message_channel_id, int) and review_message_channel_id > 0 else None,
        "review_message_id": review_message_id if isinstance(review_message_id, int) and review_message_id > 0 else None,
    }

def normalize_verification_state(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    user_id = payload.get("user_id")
    joined_at = _serialize_datetime(_parse_datetime(payload.get("joined_at")))
    warning_at = _serialize_datetime(_parse_datetime(payload.get("warning_at")))
    kick_at = _serialize_datetime(_parse_datetime(payload.get("kick_at")))
    warning_sent_at = _serialize_datetime(_parse_datetime(payload.get("warning_sent_at")))
    extension_count = payload.get("extension_count")
    review_version = payload.get("review_version")
    review_message_channel_id = payload.get("review_message_channel_id")
    review_message_id = payload.get("review_message_id")
    last_result_code = _clean_optional_text(payload.get("last_result_code"))
    last_result_at = _serialize_datetime(_parse_datetime(payload.get("last_result_at")))
    last_notified_code = _clean_optional_text(payload.get("last_notified_code"))
    last_notified_at = _serialize_datetime(_parse_datetime(payload.get("last_notified_at")))
    if not all(isinstance(value, int) and value > 0 for value in (guild_id, user_id)):
        return None
    if joined_at is None or warning_at is None or kick_at is None:
        return None
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "joined_at": joined_at,
        "warning_at": warning_at,
        "kick_at": kick_at,
        "warning_sent_at": warning_sent_at,
        "extension_count": extension_count if isinstance(extension_count, int) and extension_count >= 0 else 0,
        "review_pending": bool(payload.get("review_pending")),
        "review_version": review_version if isinstance(review_version, int) and review_version >= 0 else 0,
        "review_message_channel_id": review_message_channel_id if isinstance(review_message_channel_id, int) and review_message_channel_id > 0 else None,
        "review_message_id": review_message_id if isinstance(review_message_id, int) and review_message_id > 0 else None,
        "last_result_code": last_result_code,
        "last_result_at": last_result_at,
        "last_notified_code": last_notified_code,
        "last_notified_at": last_notified_at,
    }


def normalize_verification_review_queue(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    channel_id = payload.get("channel_id")
    message_id = payload.get("message_id")
    updated_at = _serialize_datetime(_parse_datetime(payload.get("updated_at")))
    if not isinstance(guild_id, int) or guild_id <= 0:
        return None
    return {
        "guild_id": guild_id,
        "channel_id": channel_id if isinstance(channel_id, int) and channel_id > 0 else None,
        "message_id": message_id if isinstance(message_id, int) and message_id > 0 else None,
        "updated_at": updated_at,
    }


def normalize_verification_notification_snapshot(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    run_context = _clean_optional_text(payload.get("run_context"))
    operation = _clean_optional_text(payload.get("operation"))
    outcome = _clean_optional_text(payload.get("outcome"))
    reason_code = _clean_optional_text(payload.get("reason_code"))
    signature = _clean_optional_text(payload.get("signature"))
    notified_at = _serialize_datetime(_parse_datetime(payload.get("notified_at")))
    if not isinstance(guild_id, int) or guild_id <= 0:
        return None
    if not all((run_context, operation, outcome, reason_code)):
        return None
    return {
        "guild_id": guild_id,
        "run_context": run_context,
        "operation": operation,
        "outcome": outcome,
        "reason_code": reason_code,
        "signature": signature,
        "notified_at": notified_at,
    }


def normalize_channel_lock(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    channel_id = payload.get("channel_id")
    actor_id = payload.get("actor_id")
    created_at = _serialize_datetime(_parse_datetime(payload.get("created_at")))
    due_at = _serialize_datetime(_parse_datetime(payload.get("due_at")))
    category_id = payload.get("category_id")
    marker_only = bool(payload.get("marker_only"))
    locked_permissions = _clean_permission_name_list(payload.get("locked_permissions"))
    original_permissions = _clean_permission_state_map(payload.get("original_permissions"))
    if not isinstance(guild_id, int) or guild_id <= 0:
        return None
    if not isinstance(channel_id, int) or channel_id <= 0:
        return None
    if created_at is None or (not locked_permissions and not marker_only):
        return None
    return {
        "guild_id": guild_id,
        "channel_id": channel_id,
        "actor_id": actor_id if isinstance(actor_id, int) and actor_id > 0 else None,
        "created_at": created_at,
        "due_at": due_at,
        "category_id": category_id if isinstance(category_id, int) and category_id > 0 else None,
        "permissions_synced": bool(payload.get("permissions_synced")),
        "marker_only": marker_only,
        "locked_permissions": locked_permissions,
        "original_permissions": {name: original_permissions.get(name) for name in locked_permissions},
    }


class _BaseAdminStore:
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

    async def upsert_ban_candidate(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_ban_candidate(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_ban_candidate(self, guild_id: int, user_id: int):
        raise NotImplementedError

    async def prune_expired_ban_candidates(self, now: datetime, *, limit: int = 200) -> int:
        raise NotImplementedError

    async def upsert_followup(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_followup(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_followup(self, guild_id: int, user_id: int):
        raise NotImplementedError

    async def list_due_followups(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_review_views(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_verification_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_verification_review_queue(self, record: dict[str, Any]):
        raise NotImplementedError

    async def delete_verification_review_queue(self, guild_id: int):
        raise NotImplementedError

    async def fetch_verification_notification_snapshot(
        self,
        guild_id: int,
        *,
        run_context: str,
        operation: str,
        outcome: str,
        reason_code: str,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        raise NotImplementedError

    async def upsert_channel_lock(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_channel_lock(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_channel_lock(self, guild_id: int, channel_id: int):
        raise NotImplementedError

    async def list_due_channel_locks(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def upsert_verification_state(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_verification_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_verification_state(self, guild_id: int, user_id: int):
        raise NotImplementedError

    async def list_due_verification_warnings(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        raise NotImplementedError


class _MemoryAdminStore(_BaseAdminStore):
    backend_name = "memory"

    def __init__(self):
        self.configs: dict[int, dict[str, Any]] = {}
        self.ban_candidates: dict[tuple[int, int], dict[str, Any]] = {}
        self.followups: dict[tuple[int, int], dict[str, Any]] = {}
        self.verification_review_queues: dict[int, dict[str, Any]] = {}
        self.verification_notification_snapshots: dict[tuple[int, str, str, str, str], dict[str, Any]] = {}
        self.verification_states: dict[tuple[int, int], dict[str, Any]] = {}
        self.channel_locks: dict[tuple[int, int], dict[str, Any]] = {}

    async def load(self):
        return None

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return {guild_id: deepcopy(config) for guild_id, config in self.configs.items()}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        config = self.configs.get(guild_id)
        return deepcopy(config) if config is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_admin_config(int(config["guild_id"]), config)
        self.configs[int(normalized["guild_id"])] = normalized

    async def upsert_ban_candidate(self, record: dict[str, Any]):
        normalized = normalize_ban_candidate(record)
        if normalized is not None:
            self.ban_candidates[(normalized["guild_id"], normalized["user_id"])] = normalized

    async def fetch_ban_candidate(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        record = self.ban_candidates.get((guild_id, user_id))
        return deepcopy(record) if record is not None else None

    async def delete_ban_candidate(self, guild_id: int, user_id: int):
        self.ban_candidates.pop((guild_id, user_id), None)

    async def prune_expired_ban_candidates(self, now: datetime, *, limit: int = 200) -> int:
        removed = 0
        for key, record in list(self.ban_candidates.items()):
            expires_at = _parse_datetime(record.get("expires_at"))
            if expires_at is None or expires_at > now:
                continue
            self.ban_candidates.pop(key, None)
            removed += 1
            if removed >= limit:
                break
        return removed

    async def upsert_followup(self, record: dict[str, Any]):
        normalized = normalize_followup_record(record)
        if normalized is not None:
            self.followups[(normalized["guild_id"], normalized["user_id"])] = normalized

    async def fetch_followup(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        record = self.followups.get((guild_id, user_id))
        return deepcopy(record) if record is not None else None

    async def delete_followup(self, guild_id: int, user_id: int):
        self.followups.pop((guild_id, user_id), None)

    async def list_due_followups(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = []
        for record in self.followups.values():
            due_at = _parse_datetime(record.get("due_at"))
            if due_at is None or due_at > now:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda record: (_parse_datetime(record.get("due_at")) or now, int(record.get("user_id", 0) or 0)))
        return rows[:limit]

    async def list_review_views(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.followups.values() if record.get("review_pending") and record.get("review_message_id") is not None]
        rows.sort(key=lambda record: (int(record.get("guild_id", 0) or 0), record.get("assigned_at") or "", int(record.get("user_id", 0) or 0)))
        return rows

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.verification_states.values() if record.get("review_pending") and record.get("review_message_id") is not None]
        rows.sort(key=lambda record: (int(record.get("guild_id", 0) or 0), record.get("joined_at") or "", int(record.get("user_id", 0) or 0)))
        return rows

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.verification_review_queues.values()]
        rows.sort(key=lambda record: int(record.get("guild_id", 0) or 0))
        return rows

    async def fetch_verification_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        record = self.verification_review_queues.get(guild_id)
        return deepcopy(record) if record is not None else None

    async def upsert_verification_review_queue(self, record: dict[str, Any]):
        normalized = normalize_verification_review_queue(record)
        if normalized is not None:
            self.verification_review_queues[int(normalized["guild_id"])] = normalized

    async def delete_verification_review_queue(self, guild_id: int):
        self.verification_review_queues.pop(guild_id, None)

    async def fetch_verification_notification_snapshot(
        self,
        guild_id: int,
        *,
        run_context: str,
        operation: str,
        outcome: str,
        reason_code: str,
    ) -> dict[str, Any] | None:
        key = (guild_id, run_context, operation, outcome, reason_code)
        record = self.verification_notification_snapshots.get(key)
        return deepcopy(record) if record is not None else None

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        normalized = normalize_verification_notification_snapshot(record)
        if normalized is not None:
            key = (normalized["guild_id"], normalized["run_context"], normalized["operation"], normalized["outcome"], normalized["reason_code"])
            self.verification_notification_snapshots[key] = normalized

    async def upsert_channel_lock(self, record: dict[str, Any]):
        normalized = normalize_channel_lock(record)
        if normalized is not None:
            self.channel_locks[(normalized["guild_id"], normalized["channel_id"])] = normalized

    async def fetch_channel_lock(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        record = self.channel_locks.get((guild_id, channel_id))
        return deepcopy(record) if record is not None else None

    async def delete_channel_lock(self, guild_id: int, channel_id: int):
        self.channel_locks.pop((guild_id, channel_id), None)

    async def list_due_channel_locks(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = []
        for record in self.channel_locks.values():
            due_at = _parse_datetime(record.get("due_at"))
            if due_at is None or due_at > now:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda record: (_parse_datetime(record.get("due_at")) or now, int(record.get("channel_id", 0) or 0)))
        return rows[:limit]

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.followups.values() if record.get("guild_id") == guild_id]
        rows.sort(key=lambda record: (record.get("assigned_at") or "", int(record.get("user_id", 0) or 0)))
        return rows

    async def upsert_verification_state(self, record: dict[str, Any]):
        normalized = normalize_verification_state(record)
        if normalized is not None:
            self.verification_states[(normalized["guild_id"], normalized["user_id"])] = normalized

    async def fetch_verification_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        record = self.verification_states.get((guild_id, user_id))
        return deepcopy(record) if record is not None else None

    async def delete_verification_state(self, guild_id: int, user_id: int):
        self.verification_states.pop((guild_id, user_id), None)

    async def list_due_verification_warnings(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = []
        for record in self.verification_states.values():
            if record.get("warning_sent_at") is not None:
                continue
            warning_at = _parse_datetime(record.get("warning_at"))
            if warning_at is None or warning_at > now:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda record: (_parse_datetime(record.get("warning_at")) or now, int(record.get("user_id", 0) or 0)))
        return rows[:limit]

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = []
        for record in self.verification_states.values():
            kick_at = _parse_datetime(record.get("kick_at"))
            if kick_at is None or kick_at > now:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda record: (_parse_datetime(record.get("kick_at")) or now, int(record.get("user_id", 0) or 0)))
        return rows[:limit]

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.verification_states.values() if record.get("guild_id") == guild_id]
        rows.sort(key=lambda record: (record.get("joined_at") or "", int(record.get("user_id", 0) or 0)))
        return rows

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        followup_rows = [record for record in self.followups.values() if record.get("guild_id") == guild_id]
        verification_rows = [record for record in self.verification_states.values() if record.get("guild_id") == guild_id]
        return {
            "ban_candidates": sum(1 for record in self.ban_candidates.values() if record.get("guild_id") == guild_id),
            "active_followups": len(followup_rows),
            "pending_reviews": sum(1 for record in followup_rows if record.get("review_pending")),
            "verification_pending": len(verification_rows),
            "verification_warned": sum(1 for record in verification_rows if record.get("warning_sent_at") is not None),
            "active_channel_locks": sum(1 for record in self.channel_locks.values() if record.get("guild_id") == guild_id),
        }


def _config_from_row(row) -> dict[str, Any]:
    payload = {
        "guild_id": row.get("guild_id"),
        "followup_enabled": row.get("followup_enabled", False),
        "followup_role_id": row.get("followup_role_id"),
        "followup_mode": row.get("followup_mode", "review"),
        "followup_duration_value": int(row.get("followup_duration_value", 30) or 30),
        "followup_duration_unit": row.get("followup_duration_unit", "days"),
        "verification_enabled": row.get("verification_enabled", False),
        "verification_role_id": row.get("verification_role_id"),
        "verification_logic": row.get("verification_logic", "must_have_role"),
        "verification_deadline_action": row.get("verification_deadline_action", "auto_kick"),
        "verification_kick_after_seconds": int(row.get("verification_kick_after_seconds", 7 * 24 * 3600) or 7 * 24 * 3600),
        "verification_warning_lead_seconds": int(row.get("verification_warning_lead_seconds", 24 * 3600) or 24 * 3600),
        "verification_help_channel_id": row.get("verification_help_channel_id"),
        "verification_help_extension_seconds": int(row.get("verification_help_extension_seconds", 3 * 24 * 3600) or 3 * 24 * 3600),
        "verification_max_extensions": int(row.get("verification_max_extensions", 1) or 1),
        "admin_log_channel_id": row.get("admin_log_channel_id"),
        "admin_alert_role_id": row.get("admin_alert_role_id"),
        "warning_template": row.get("warning_template"),
        "kick_template": row.get("kick_template"),
        "invite_link": row.get("invite_link"),
        "lock_notice_template": row.get("lock_notice_template"),
        "lock_admin_only": row.get("lock_admin_only", False),
        "excluded_user_ids": decode_postgres_json_array(row.get("excluded_user_ids"), label="admin_guild_configs.excluded_user_ids"),
        "excluded_role_ids": decode_postgres_json_array(row.get("excluded_role_ids"), label="admin_guild_configs.excluded_role_ids"),
        "trusted_role_ids": decode_postgres_json_array(row.get("trusted_role_ids"), label="admin_guild_configs.trusted_role_ids"),
        "followup_exempt_staff": row.get("followup_exempt_staff", True),
        "verification_exempt_staff": row.get("verification_exempt_staff", True),
        "verification_exempt_bots": row.get("verification_exempt_bots", True),
    }
    return normalize_admin_config(int(payload["guild_id"]), payload)


def _followup_from_row(row) -> dict[str, Any] | None:
    return normalize_followup_record(
        {
            "guild_id": row.get("guild_id"),
            "user_id": row.get("user_id"),
            "role_id": row.get("role_id"),
            "assigned_at": row.get("assigned_at"),
            "due_at": row.get("due_at"),
            "mode": row.get("mode", "review"),
            "review_pending": row.get("review_pending", False),
            "review_version": row.get("review_version", 0),
            "review_message_channel_id": row.get("review_message_channel_id"),
            "review_message_id": row.get("review_message_id"),
        }
    )


def _verification_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_state(
        {
            "guild_id": row.get("guild_id"),
            "user_id": row.get("user_id"),
            "joined_at": row.get("joined_at"),
            "warning_at": row.get("warning_at"),
            "kick_at": row.get("kick_at"),
            "warning_sent_at": row.get("warning_sent_at"),
            "extension_count": row.get("extension_count", 0),
            "review_pending": row.get("review_pending", False),
            "review_version": row.get("review_version", 0),
            "review_message_channel_id": row.get("review_message_channel_id"),
            "review_message_id": row.get("review_message_id"),
            "last_result_code": row.get("last_result_code"),
            "last_result_at": row.get("last_result_at"),
            "last_notified_code": row.get("last_notified_code"),
            "last_notified_at": row.get("last_notified_at"),
        }
    )


def _verification_review_queue_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_review_queue(
        {
            "guild_id": row.get("guild_id"),
            "channel_id": row.get("channel_id"),
            "message_id": row.get("message_id"),
            "updated_at": row.get("updated_at"),
        }
    )


def _verification_notification_snapshot_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_notification_snapshot(
        {
            "guild_id": row.get("guild_id"),
            "run_context": row.get("run_context"),
            "operation": row.get("operation"),
            "outcome": row.get("outcome"),
            "reason_code": row.get("reason_code"),
            "signature": row.get("signature"),
            "notified_at": row.get("notified_at"),
        }
    )


def _channel_lock_from_row(row) -> dict[str, Any] | None:
    if row is None:
        return None
    locked_permissions = decode_postgres_json_array(
        row.get("locked_permissions"),
        label="admin_channel_locks.locked_permissions",
    )
    original_permissions = row.get("original_permissions")
    if isinstance(original_permissions, str):
        try:
            original_permissions = json.loads(original_permissions)
        except json.JSONDecodeError:
            original_permissions = {}
    return normalize_channel_lock(
        {
            "guild_id": row.get("guild_id"),
            "channel_id": row.get("channel_id"),
            "actor_id": row.get("actor_id"),
            "created_at": row.get("created_at"),
            "due_at": row.get("due_at"),
            "category_id": row.get("category_id"),
            "permissions_synced": row.get("permissions_synced", False),
            "marker_only": row.get("marker_only", False),
            "locked_permissions": locked_permissions,
            "original_permissions": original_permissions,
        }
    )


class _PostgresAdminStore(_BaseAdminStore):
    backend_name = "postgres"

    def __init__(self, database_url: str):
        self.database_url = database_url
        self._asyncpg = None
        self.pool = None

    async def load(self):
        await self._connect()
        await self._ensure_schema()

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def _connect(self):
        if self.pool is not None:
            return
        try:
            self._asyncpg = importlib.import_module("asyncpg")
        except ModuleNotFoundError as exc:
            raise AdminStorageUnavailable("Postgres admin storage requires the 'asyncpg' package.") from exc
        try:
            self.pool = await self._asyncpg.create_pool(self.database_url, min_size=1, max_size=4, command_timeout=30)
        except Exception as exc:
            raise AdminStorageUnavailable(f"Could not connect to Postgres admin storage: {exc}") from exc

    async def _ensure_schema(self):
        table_statements = [
            "CREATE TABLE IF NOT EXISTS admin_guild_configs ("
            "guild_id BIGINT PRIMARY KEY, followup_enabled BOOLEAN NOT NULL DEFAULT FALSE, followup_role_id BIGINT NULL, "
            "followup_mode TEXT NOT NULL DEFAULT 'review', followup_duration_value SMALLINT NOT NULL DEFAULT 30, "
            "followup_duration_unit TEXT NOT NULL DEFAULT 'days', verification_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
            "verification_role_id BIGINT NULL, verification_logic TEXT NOT NULL DEFAULT 'must_have_role', "
            "verification_deadline_action TEXT NOT NULL DEFAULT 'auto_kick', verification_kick_after_seconds INTEGER NOT NULL DEFAULT 604800, "
            "verification_warning_lead_seconds INTEGER NOT NULL DEFAULT 86400, verification_help_channel_id BIGINT NULL, "
            "verification_help_extension_seconds INTEGER NOT NULL DEFAULT 259200, verification_max_extensions SMALLINT NOT NULL DEFAULT 1, "
            "admin_log_channel_id BIGINT NULL, admin_alert_role_id BIGINT NULL, warning_template TEXT NULL, kick_template TEXT NULL, invite_link TEXT NULL, "
            "lock_notice_template TEXT NULL, lock_admin_only BOOLEAN NOT NULL DEFAULT FALSE, "
            "excluded_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, excluded_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, trusted_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
            "followup_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE, verification_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE, verification_exempt_bots BOOLEAN NOT NULL DEFAULT TRUE, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now()))",
            "CREATE TABLE IF NOT EXISTS admin_ban_return_candidates (guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, banned_at TIMESTAMPTZ NOT NULL, expires_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (guild_id, user_id))",
            "CREATE TABLE IF NOT EXISTS admin_followup_roles (guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, role_id BIGINT NOT NULL, assigned_at TIMESTAMPTZ NOT NULL, due_at TIMESTAMPTZ NULL, mode TEXT NOT NULL, review_pending BOOLEAN NOT NULL DEFAULT FALSE, review_version INTEGER NOT NULL DEFAULT 0, review_message_channel_id BIGINT NULL, review_message_id BIGINT NULL, PRIMARY KEY (guild_id, user_id))",
            "CREATE TABLE IF NOT EXISTS admin_verification_states (guild_id BIGINT NOT NULL, user_id BIGINT NOT NULL, joined_at TIMESTAMPTZ NOT NULL, warning_at TIMESTAMPTZ NOT NULL, kick_at TIMESTAMPTZ NOT NULL, warning_sent_at TIMESTAMPTZ NULL, extension_count SMALLINT NOT NULL DEFAULT 0, review_pending BOOLEAN NOT NULL DEFAULT FALSE, review_version INTEGER NOT NULL DEFAULT 0, review_message_channel_id BIGINT NULL, review_message_id BIGINT NULL, last_result_code TEXT NULL, last_result_at TIMESTAMPTZ NULL, last_notified_code TEXT NULL, last_notified_at TIMESTAMPTZ NULL, PRIMARY KEY (guild_id, user_id))",
            "CREATE TABLE IF NOT EXISTS admin_verification_review_queues (guild_id BIGINT PRIMARY KEY, channel_id BIGINT NULL, message_id BIGINT NULL, updated_at TIMESTAMPTZ NULL)",
            "CREATE TABLE IF NOT EXISTS admin_verification_notification_snapshots (guild_id BIGINT NOT NULL, run_context TEXT NOT NULL, operation TEXT NOT NULL, outcome TEXT NOT NULL, reason_code TEXT NOT NULL, signature TEXT NULL, notified_at TIMESTAMPTZ NULL, PRIMARY KEY (guild_id, run_context, operation, outcome, reason_code))",
            "CREATE TABLE IF NOT EXISTS admin_channel_locks (guild_id BIGINT NOT NULL, channel_id BIGINT NOT NULL, actor_id BIGINT NULL, created_at TIMESTAMPTZ NOT NULL, due_at TIMESTAMPTZ NULL, category_id BIGINT NULL, permissions_synced BOOLEAN NOT NULL DEFAULT FALSE, marker_only BOOLEAN NOT NULL DEFAULT FALSE, locked_permissions JSONB NOT NULL DEFAULT '[]'::jsonb, original_permissions JSONB NOT NULL DEFAULT '{}'::jsonb, PRIMARY KEY (guild_id, channel_id))",
        ]
        alter_statements = [
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_role_id BIGINT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_mode TEXT NOT NULL DEFAULT 'review'",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_duration_value SMALLINT NOT NULL DEFAULT 30",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_duration_unit TEXT NOT NULL DEFAULT 'days'",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_role_id BIGINT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_logic TEXT NOT NULL DEFAULT 'must_have_role'",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_deadline_action TEXT NOT NULL DEFAULT 'auto_kick'",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_kick_after_seconds INTEGER NOT NULL DEFAULT 604800",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_warning_lead_seconds INTEGER NOT NULL DEFAULT 86400",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_help_channel_id BIGINT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_help_extension_seconds INTEGER NOT NULL DEFAULT 259200",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_max_extensions SMALLINT NOT NULL DEFAULT 1",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS admin_log_channel_id BIGINT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS admin_alert_role_id BIGINT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS warning_template TEXT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS kick_template TEXT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS invite_link TEXT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS lock_notice_template TEXT NULL",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS lock_admin_only BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS excluded_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS excluded_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS trusted_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS followup_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_exempt_bots BOOLEAN NOT NULL DEFAULT TRUE",
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL",
            "ALTER TABLE admin_followup_roles ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_result_code TEXT NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_result_at TIMESTAMPTZ NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_notified_code TEXT NULL",
            "ALTER TABLE admin_verification_states ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ NULL",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS actor_id BIGINT NULL",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS due_at TIMESTAMPTZ NULL",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS category_id BIGINT NULL",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS permissions_synced BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS marker_only BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS locked_permissions JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE admin_channel_locks ADD COLUMN IF NOT EXISTS original_permissions JSONB NOT NULL DEFAULT '{}'::jsonb",
            "DROP TABLE IF EXISTS admin_member_risk_states",
            "DROP TABLE IF EXISTS admin_member_risk_review_queues",
            "DROP TABLE IF EXISTS admin_emergency_incidents",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS member_risk_enabled",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS member_risk_mode",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_enabled",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS security_posture",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_mode",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_strict_auto_containment",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_ping_mode",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS control_lock_enabled",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS editor_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS editor_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_operator_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_operator_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS control_deny_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS control_deny_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS protected_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS protected_role_granter_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS protected_role_granter_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS trusted_actor_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS trusted_actor_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS trusted_bot_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS allowlisted_target_user_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS allowlisted_target_role_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS channel_whitelist_ids",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS quarantine_role_id",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS enabled_dangerous_permission_flags",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS permission_sync_rules",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_role_grant_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_role_grant_target_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_kick_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_ban_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_channel_delete_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_role_delete_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_webhook_churn_threshold",
            "ALTER TABLE admin_guild_configs DROP COLUMN IF EXISTS emergency_bot_add_threshold",
        ]
        index_statements = [
            "CREATE INDEX IF NOT EXISTS ix_admin_ban_return_expires ON admin_ban_return_candidates (expires_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_followup_due ON admin_followup_roles (due_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_followup_review_pending ON admin_followup_roles (review_pending, review_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_warning_due ON admin_verification_states (warning_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_kick_due ON admin_verification_states (kick_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_guild ON admin_verification_states (guild_id)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_review_pending ON admin_verification_states (review_pending, review_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_last_notified ON admin_verification_states (guild_id, last_notified_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_verification_snapshot_notified ON admin_verification_notification_snapshots (guild_id, notified_at)",
            "CREATE INDEX IF NOT EXISTS ix_admin_channel_locks_due ON admin_channel_locks (due_at)",
        ]
        async with self.pool.acquire() as conn:
            for statement in table_statements:
                await conn.execute(statement)
            for statement in alter_statements:
                await conn.execute(statement)
            for statement in index_statements:
                await conn.execute(statement)

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_guild_configs")
        return {int(row["guild_id"]): _config_from_row(row) for row in rows}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_guild_configs WHERE guild_id = $1", guild_id)
        return _config_from_row(row) if row is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_admin_config(int(config["guild_id"]), config)
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admin_guild_configs (guild_id, followup_enabled, followup_role_id, followup_mode, followup_duration_value, followup_duration_unit, verification_enabled, verification_role_id, verification_logic, verification_deadline_action, verification_kick_after_seconds, verification_warning_lead_seconds, verification_help_channel_id, verification_help_extension_seconds, verification_max_extensions, admin_log_channel_id, admin_alert_role_id, warning_template, kick_template, invite_link, lock_notice_template, lock_admin_only, excluded_user_ids, excluded_role_ids, trusted_role_ids, followup_exempt_staff, verification_exempt_staff, verification_exempt_bots, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23::jsonb, $24::jsonb, $25::jsonb, $26, $27, $28, timezone('utc', now())) ON CONFLICT (guild_id) DO UPDATE SET followup_enabled = EXCLUDED.followup_enabled, followup_role_id = EXCLUDED.followup_role_id, followup_mode = EXCLUDED.followup_mode, followup_duration_value = EXCLUDED.followup_duration_value, followup_duration_unit = EXCLUDED.followup_duration_unit, verification_enabled = EXCLUDED.verification_enabled, verification_role_id = EXCLUDED.verification_role_id, verification_logic = EXCLUDED.verification_logic, verification_deadline_action = EXCLUDED.verification_deadline_action, verification_kick_after_seconds = EXCLUDED.verification_kick_after_seconds, verification_warning_lead_seconds = EXCLUDED.verification_warning_lead_seconds, verification_help_channel_id = EXCLUDED.verification_help_channel_id, verification_help_extension_seconds = EXCLUDED.verification_help_extension_seconds, verification_max_extensions = EXCLUDED.verification_max_extensions, admin_log_channel_id = EXCLUDED.admin_log_channel_id, admin_alert_role_id = EXCLUDED.admin_alert_role_id, warning_template = EXCLUDED.warning_template, kick_template = EXCLUDED.kick_template, invite_link = EXCLUDED.invite_link, lock_notice_template = EXCLUDED.lock_notice_template, lock_admin_only = EXCLUDED.lock_admin_only, excluded_user_ids = EXCLUDED.excluded_user_ids, excluded_role_ids = EXCLUDED.excluded_role_ids, trusted_role_ids = EXCLUDED.trusted_role_ids, followup_exempt_staff = EXCLUDED.followup_exempt_staff, verification_exempt_staff = EXCLUDED.verification_exempt_staff, verification_exempt_bots = EXCLUDED.verification_exempt_bots, updated_at = EXCLUDED.updated_at",
                normalized["guild_id"], normalized["followup_enabled"], normalized["followup_role_id"], normalized["followup_mode"], normalized["followup_duration_value"], normalized["followup_duration_unit"], normalized["verification_enabled"], normalized["verification_role_id"], normalized["verification_logic"], normalized["verification_deadline_action"], normalized["verification_kick_after_seconds"], normalized["verification_warning_lead_seconds"], normalized["verification_help_channel_id"], normalized["verification_help_extension_seconds"], normalized["verification_max_extensions"], normalized["admin_log_channel_id"], normalized["admin_alert_role_id"], normalized["warning_template"], normalized["kick_template"], normalized["invite_link"], normalized["lock_notice_template"], normalized["lock_admin_only"], json.dumps(normalized["excluded_user_ids"]), json.dumps(normalized["excluded_role_ids"]), json.dumps(normalized["trusted_role_ids"]), normalized["followup_exempt_staff"], normalized["verification_exempt_staff"], normalized["verification_exempt_bots"],
            )

    async def upsert_ban_candidate(self, record: dict[str, Any]):
        normalized = normalize_ban_candidate(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(normalized, "banned_at", "expires_at")
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO admin_ban_return_candidates (guild_id, user_id, banned_at, expires_at) VALUES ($1, $2, $3, $4) ON CONFLICT (guild_id, user_id) DO UPDATE SET banned_at = EXCLUDED.banned_at, expires_at = EXCLUDED.expires_at", coerced["guild_id"], coerced["user_id"], coerced["banned_at"], coerced["expires_at"])

    async def fetch_ban_candidate(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT guild_id, user_id, banned_at, expires_at FROM admin_ban_return_candidates WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        return normalize_ban_candidate(dict(row)) if row is not None else None

    async def delete_ban_candidate(self, guild_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admin_ban_return_candidates WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def prune_expired_ban_candidates(self, now: datetime, *, limit: int = 200) -> int:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("WITH doomed AS (SELECT guild_id, user_id FROM admin_ban_return_candidates WHERE expires_at <= $1 ORDER BY expires_at ASC LIMIT $2) DELETE FROM admin_ban_return_candidates target USING doomed WHERE target.guild_id = doomed.guild_id AND target.user_id = doomed.user_id RETURNING target.guild_id", now, limit)
        return len(rows)

    async def upsert_followup(self, record: dict[str, Any]):
        normalized = normalize_followup_record(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(normalized, "assigned_at", "due_at")
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO admin_followup_roles (guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, review_message_channel_id, review_message_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (guild_id, user_id) DO UPDATE SET role_id = EXCLUDED.role_id, assigned_at = EXCLUDED.assigned_at, due_at = EXCLUDED.due_at, mode = EXCLUDED.mode, review_pending = EXCLUDED.review_pending, review_version = EXCLUDED.review_version, review_message_channel_id = EXCLUDED.review_message_channel_id, review_message_id = EXCLUDED.review_message_id", coerced["guild_id"], coerced["user_id"], coerced["role_id"], coerced["assigned_at"], coerced["due_at"], coerced["mode"], coerced["review_pending"], coerced["review_version"], coerced["review_message_channel_id"], coerced["review_message_id"])

    async def fetch_followup(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_followup_roles WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        return _followup_from_row(row) if row is not None else None

    async def delete_followup(self, guild_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admin_followup_roles WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def list_due_followups(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_followup_roles WHERE due_at IS NOT NULL AND due_at <= $1 ORDER BY due_at ASC, user_id ASC LIMIT $2", now, limit)
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def list_review_views(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_followup_roles WHERE review_pending = TRUE AND review_message_id IS NOT NULL ORDER BY guild_id ASC, assigned_at ASC, user_id ASC")
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_verification_states WHERE review_pending = TRUE AND review_message_id IS NOT NULL ORDER BY guild_id ASC, joined_at ASC, user_id ASC")
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT guild_id, channel_id, message_id, updated_at FROM admin_verification_review_queues ORDER BY guild_id ASC")
        return [record for row in rows if (record := _verification_review_queue_from_row(row)) is not None]

    async def fetch_verification_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT guild_id, channel_id, message_id, updated_at FROM admin_verification_review_queues WHERE guild_id = $1", guild_id)
        return _verification_review_queue_from_row(row) if row is not None else None

    async def upsert_verification_review_queue(self, record: dict[str, Any]):
        normalized = normalize_verification_review_queue(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(normalized, "updated_at")
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO admin_verification_review_queues (guild_id, channel_id, message_id, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (guild_id) DO UPDATE SET channel_id = EXCLUDED.channel_id, message_id = EXCLUDED.message_id, updated_at = EXCLUDED.updated_at", coerced["guild_id"], coerced["channel_id"], coerced["message_id"], coerced["updated_at"])

    async def delete_verification_review_queue(self, guild_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admin_verification_review_queues WHERE guild_id = $1", guild_id)

    async def fetch_verification_notification_snapshot(self, guild_id: int, *, run_context: str, operation: str, outcome: str, reason_code: str) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_verification_notification_snapshots WHERE guild_id = $1 AND run_context = $2 AND operation = $3 AND outcome = $4 AND reason_code = $5", guild_id, run_context, operation, outcome, reason_code)
        return _verification_notification_snapshot_from_row(row) if row is not None else None

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        normalized = normalize_verification_notification_snapshot(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(normalized, "notified_at")
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO admin_verification_notification_snapshots (guild_id, run_context, operation, outcome, reason_code, signature, notified_at) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (guild_id, run_context, operation, outcome, reason_code) DO UPDATE SET signature = EXCLUDED.signature, notified_at = EXCLUDED.notified_at", coerced["guild_id"], coerced["run_context"], coerced["operation"], coerced["outcome"], coerced["reason_code"], coerced["signature"], coerced["notified_at"])

    async def upsert_channel_lock(self, record: dict[str, Any]):
        normalized = normalize_channel_lock(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(normalized, "created_at", "due_at")
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admin_channel_locks (guild_id, channel_id, actor_id, created_at, due_at, category_id, permissions_synced, marker_only, locked_permissions, original_permissions) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb) ON CONFLICT (guild_id, channel_id) DO UPDATE SET actor_id = EXCLUDED.actor_id, created_at = EXCLUDED.created_at, due_at = EXCLUDED.due_at, category_id = EXCLUDED.category_id, permissions_synced = EXCLUDED.permissions_synced, marker_only = EXCLUDED.marker_only, locked_permissions = EXCLUDED.locked_permissions, original_permissions = EXCLUDED.original_permissions",
                coerced["guild_id"],
                coerced["channel_id"],
                coerced["actor_id"],
                coerced["created_at"],
                coerced["due_at"],
                coerced["category_id"],
                coerced["permissions_synced"],
                coerced["marker_only"],
                json.dumps(coerced["locked_permissions"]),
                json.dumps(coerced["original_permissions"]),
            )

    async def fetch_channel_lock(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_channel_locks WHERE guild_id = $1 AND channel_id = $2", guild_id, channel_id)
        return _channel_lock_from_row(row)

    async def delete_channel_lock(self, guild_id: int, channel_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admin_channel_locks WHERE guild_id = $1 AND channel_id = $2", guild_id, channel_id)

    async def list_due_channel_locks(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_channel_locks WHERE due_at IS NOT NULL AND due_at <= $1 ORDER BY due_at ASC, channel_id ASC LIMIT $2", now, limit)
        return [record for row in rows if (record := _channel_lock_from_row(row)) is not None]

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_followup_roles WHERE guild_id = $1 ORDER BY assigned_at ASC, user_id ASC", guild_id)
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def upsert_verification_state(self, record: dict[str, Any]):
        normalized = normalize_verification_state(record)
        if normalized is None:
            return
        coerced = _coerce_storage_datetimes(
            normalized,
            "joined_at",
            "warning_at",
            "kick_at",
            "warning_sent_at",
            "last_result_at",
            "last_notified_at",
        )
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO admin_verification_states (guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, review_pending, review_version, review_message_channel_id, review_message_id, last_result_code, last_result_at, last_notified_code, last_notified_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15) ON CONFLICT (guild_id, user_id) DO UPDATE SET joined_at = EXCLUDED.joined_at, warning_at = EXCLUDED.warning_at, kick_at = EXCLUDED.kick_at, warning_sent_at = EXCLUDED.warning_sent_at, extension_count = EXCLUDED.extension_count, review_pending = EXCLUDED.review_pending, review_version = EXCLUDED.review_version, review_message_channel_id = EXCLUDED.review_message_channel_id, review_message_id = EXCLUDED.review_message_id, last_result_code = EXCLUDED.last_result_code, last_result_at = EXCLUDED.last_result_at, last_notified_code = EXCLUDED.last_notified_code, last_notified_at = EXCLUDED.last_notified_at", coerced["guild_id"], coerced["user_id"], coerced["joined_at"], coerced["warning_at"], coerced["kick_at"], coerced["warning_sent_at"], coerced["extension_count"], coerced["review_pending"], coerced["review_version"], coerced["review_message_channel_id"], coerced["review_message_id"], coerced["last_result_code"], coerced["last_result_at"], coerced["last_notified_code"], coerced["last_notified_at"])

    async def fetch_verification_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_verification_states WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)
        return _verification_from_row(row) if row is not None else None

    async def delete_verification_state(self, guild_id: int, user_id: int):
        async with self.pool.acquire() as conn:
            await conn.execute("DELETE FROM admin_verification_states WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def list_due_verification_warnings(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_verification_states WHERE warning_sent_at IS NULL AND warning_at <= $1 ORDER BY warning_at ASC, user_id ASC LIMIT $2", now, limit)
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_verification_states WHERE kick_at <= $1 ORDER BY kick_at ASC, user_id ASC LIMIT $2", now, limit)
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM admin_verification_states WHERE guild_id = $1 ORDER BY joined_at ASC, user_id ASC", guild_id)
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT (SELECT COUNT(*) FROM admin_ban_return_candidates WHERE guild_id = $1) AS ban_candidates, (SELECT COUNT(*) FROM admin_followup_roles WHERE guild_id = $1) AS active_followups, (SELECT COUNT(*) FROM admin_followup_roles WHERE guild_id = $1 AND review_pending = TRUE) AS pending_reviews, (SELECT COUNT(*) FROM admin_verification_states WHERE guild_id = $1) AS verification_pending, (SELECT COUNT(*) FROM admin_verification_states WHERE guild_id = $1 AND warning_sent_at IS NOT NULL) AS verification_warned, (SELECT COUNT(*) FROM admin_channel_locks WHERE guild_id = $1) AS active_channel_locks", guild_id)
        return {"ban_candidates": int(row["ban_candidates"] or 0), "active_followups": int(row["active_followups"] or 0), "pending_reviews": int(row["pending_reviews"] or 0), "verification_pending": int(row["verification_pending"] or 0), "verification_warned": int(row["verification_warned"] or 0), "active_channel_locks": int(row["active_channel_locks"] or 0)}


class AdminStore:
    def __init__(self, *, backend: str | None = None, database_url: str | None = None):
        requested_backend = (backend or os.getenv("ADMIN_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.backend_name = requested_backend
        self._store: _BaseAdminStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        print("Admin storage init: " f"backend_preference={requested_backend}, " f"database_url_configured={'yes' if self.database_url else 'no'}, " f"database_url_source={self.database_url_source or 'none'}, " f"database_target={_redact_database_url(self.database_url)}")
        if requested_backend in {"memory", "test", "dev"}:
            self._store = _MemoryAdminStore()
        elif requested_backend in {"postgres", "postgresql", "supabase", "auto"}:
            if not self.database_url:
                raise AdminStorageUnavailable("No Postgres admin database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL.")
            self._store = _PostgresAdminStore(self.database_url)
        else:
            raise AdminStorageUnavailable(f"Unsupported admin storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        print(f"Admin storage init succeeded: backend={self.backend_name}")

    async def load(self):
        if self._store is None:
            raise AdminStorageUnavailable("Admin storage was not initialized.")
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

    async def upsert_ban_candidate(self, record: dict[str, Any]):
        await self._store.upsert_ban_candidate(record)

    async def fetch_ban_candidate(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_ban_candidate(guild_id, user_id)

    async def delete_ban_candidate(self, guild_id: int, user_id: int):
        await self._store.delete_ban_candidate(guild_id, user_id)

    async def prune_expired_ban_candidates(self, now: datetime, *, limit: int = 200) -> int:
        return await self._store.prune_expired_ban_candidates(now, limit=limit)

    async def upsert_followup(self, record: dict[str, Any]):
        await self._store.upsert_followup(record)

    async def fetch_followup(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_followup(guild_id, user_id)

    async def delete_followup(self, guild_id: int, user_id: int):
        await self._store.delete_followup(guild_id, user_id)

    async def list_due_followups(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        return await self._store.list_due_followups(now, limit=limit)

    async def list_review_views(self) -> list[dict[str, Any]]:
        return await self._store.list_review_views()

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        return await self._store.list_verification_review_views()

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        return await self._store.list_verification_review_queues()

    async def fetch_verification_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_verification_review_queue(guild_id)

    async def upsert_verification_review_queue(self, record: dict[str, Any]):
        await self._store.upsert_verification_review_queue(record)

    async def delete_verification_review_queue(self, guild_id: int):
        await self._store.delete_verification_review_queue(guild_id)

    async def fetch_verification_notification_snapshot(self, guild_id: int, *, run_context: str, operation: str, outcome: str, reason_code: str) -> dict[str, Any] | None:
        return await self._store.fetch_verification_notification_snapshot(guild_id, run_context=run_context, operation=operation, outcome=outcome, reason_code=reason_code)

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        await self._store.upsert_verification_notification_snapshot(record)

    async def upsert_channel_lock(self, record: dict[str, Any]):
        await self._store.upsert_channel_lock(record)

    async def fetch_channel_lock(self, guild_id: int, channel_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_channel_lock(guild_id, channel_id)

    async def delete_channel_lock(self, guild_id: int, channel_id: int):
        await self._store.delete_channel_lock(guild_id, channel_id)

    async def list_due_channel_locks(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        return await self._store.list_due_channel_locks(now, limit=limit)

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        return await self._store.list_followups_for_guild(guild_id)

    async def upsert_verification_state(self, record: dict[str, Any]):
        await self._store.upsert_verification_state(record)

    async def fetch_verification_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_verification_state(guild_id, user_id)

    async def delete_verification_state(self, guild_id: int, user_id: int):
        await self._store.delete_verification_state(guild_id, user_id)

    async def list_due_verification_warnings(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        return await self._store.list_due_verification_warnings(now, limit=limit)

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        return await self._store.list_due_verification_kicks(now, limit=limit)

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        return await self._store.list_verification_states_for_guild(guild_id)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        return await self._store.fetch_guild_counts(guild_id)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
