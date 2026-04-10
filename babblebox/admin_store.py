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


DEFAULT_BACKEND = "postgres"
VALID_FOLLOWUP_MODES = {"auto_remove", "review"}
VALID_FOLLOWUP_DURATION_UNITS = {"days", "weeks", "months"}
VALID_VERIFICATION_LOGIC = {"must_have_role", "must_not_have_role"}
VALID_VERIFICATION_DEADLINE_ACTIONS = {"auto_kick", "review"}
VALID_MEMBER_RISK_MODES = {"log", "review", "review_or_kick"}


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
        "excluded_user_ids": [],
        "excluded_role_ids": [],
        "trusted_role_ids": [],
        "followup_exempt_staff": True,
        "verification_exempt_staff": True,
        "verification_exempt_bots": True,
        "member_risk_enabled": False,
        "member_risk_mode": "review",
    }


MEMBER_RISK_SIGNAL_PRIORITY = {
    "malicious_link": 10,
    "scam_high": 20,
    "fresh_campaign_cluster_3": 30,
    "fresh_campaign_cluster_2": 40,
    "campaign_lure_reuse": 50,
    "campaign_path_shape": 60,
    "campaign_host_family": 65,
    "unknown_suspicious_link": 70,
    "scam_medium": 80,
    "suspicious_attachment": 90,
    "cta_download": 100,
    "newcomer_first_messages_risky": 110,
    "first_external_link": 120,
    "first_message_link": 130,
    "newcomer_early_message": 140,
    "name_impersonation": 150,
    "name_mixed_script": 160,
    "account_new_1d": 170,
    "joined_recently": 180,
    "account_new_7d": 190,
    "default_avatar": 200,
    "name_zero_width": 210,
    "name_unreadable": 220,
    "name_separator_heavy": 230,
}


def order_member_risk_signal_codes(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        values = []
    unique = {
        str(value).strip()
        for value in values
        if isinstance(value, str) and str(value).strip()
    }
    return sorted(unique, key=lambda value: (MEMBER_RISK_SIGNAL_PRIORITY.get(value, 999), value))[:10]


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
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _clean_optional_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned or None


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
    cleaned["verification_deadline_action"] = (
        verification_deadline_action
        if verification_deadline_action in VALID_VERIFICATION_DEADLINE_ACTIONS
        else "auto_kick"
    )
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
    for field in ("excluded_user_ids", "excluded_role_ids", "trusted_role_ids"):
        cleaned[field] = _clean_int_list(payload.get(field))
    cleaned["followup_exempt_staff"] = bool(payload.get("followup_exempt_staff", True))
    cleaned["verification_exempt_staff"] = bool(payload.get("verification_exempt_staff", True))
    cleaned["verification_exempt_bots"] = bool(payload.get("verification_exempt_bots", True))
    cleaned_member_risk_mode = str(payload.get("member_risk_mode", "review")).strip().lower()
    cleaned["member_risk_enabled"] = bool(payload.get("member_risk_enabled"))
    cleaned["member_risk_mode"] = (
        cleaned_member_risk_mode if cleaned_member_risk_mode in VALID_MEMBER_RISK_MODES else "review"
    )
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
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "banned_at": banned_at,
        "expires_at": expires_at,
    }


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


def normalize_member_risk_state(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    guild_id = payload.get("guild_id")
    user_id = payload.get("user_id")
    first_seen_at = _serialize_datetime(_parse_datetime(payload.get("first_seen_at")))
    last_seen_at = _serialize_datetime(_parse_datetime(payload.get("last_seen_at")))
    snooze_until = _serialize_datetime(_parse_datetime(payload.get("snooze_until")))
    risk_level = _clean_optional_text(payload.get("risk_level"))
    primary_domain = _clean_optional_text(payload.get("primary_domain"))
    review_version = payload.get("review_version")
    review_message_channel_id = payload.get("review_message_channel_id")
    review_message_id = payload.get("review_message_id")
    last_result_code = _clean_optional_text(payload.get("last_result_code"))
    last_result_at = _serialize_datetime(_parse_datetime(payload.get("last_result_at")))
    last_notified_code = _clean_optional_text(payload.get("last_notified_code"))
    last_notified_at = _serialize_datetime(_parse_datetime(payload.get("last_notified_at")))
    message_event_count = payload.get("message_event_count")
    latest_message_basis = _clean_optional_text(payload.get("latest_message_basis"))
    latest_message_confidence = _clean_optional_text(payload.get("latest_message_confidence"))
    latest_scan_source = _clean_optional_text(payload.get("latest_scan_source"))
    signal_codes_raw = payload.get("signal_codes", [])
    if not all(isinstance(value, int) and value > 0 for value in (guild_id, user_id)):
        return None
    if first_seen_at is None or last_seen_at is None:
        return None
    if risk_level not in {"note", "review", "critical"}:
        return None
    signal_codes = order_member_risk_signal_codes(signal_codes_raw)
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "first_seen_at": first_seen_at,
        "last_seen_at": last_seen_at,
        "snooze_until": snooze_until,
        "risk_level": risk_level,
        "signal_codes": signal_codes,
        "primary_domain": primary_domain,
        "review_pending": bool(payload.get("review_pending")),
        "review_version": review_version if isinstance(review_version, int) and review_version >= 0 else 0,
        "review_message_channel_id": review_message_channel_id if isinstance(review_message_channel_id, int) and review_message_channel_id > 0 else None,
        "review_message_id": review_message_id if isinstance(review_message_id, int) and review_message_id > 0 else None,
        "last_result_code": last_result_code,
        "last_result_at": last_result_at,
        "last_notified_code": last_notified_code,
        "last_notified_at": last_notified_at,
        "message_event_count": message_event_count if isinstance(message_event_count, int) and message_event_count >= 0 else 0,
        "latest_message_basis": latest_message_basis,
        "latest_message_confidence": latest_message_confidence,
        "latest_scan_source": latest_scan_source,
    }


def normalize_member_risk_review_queue(payload: Any) -> dict[str, Any] | None:
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

    async def upsert_member_risk_state(self, record: dict[str, Any]):
        raise NotImplementedError

    async def fetch_member_risk_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def delete_member_risk_state(self, guild_id: int, user_id: int):
        raise NotImplementedError

    async def list_member_risk_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def list_member_risk_review_queues(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    async def fetch_member_risk_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        raise NotImplementedError

    async def upsert_member_risk_review_queue(self, record: dict[str, Any]):
        raise NotImplementedError

    async def delete_member_risk_review_queue(self, guild_id: int):
        raise NotImplementedError

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        raise NotImplementedError


class _MemoryAdminStore(_BaseAdminStore):
    backend_name = "memory"

    def __init__(self):
        self.configs: dict[int, dict[str, Any]] = {}
        self.ban_candidates: dict[tuple[int, int], dict[str, Any]] = {}
        self.followups: dict[tuple[int, int], dict[str, Any]] = {}
        self.verification_states: dict[tuple[int, int], dict[str, Any]] = {}
        self.verification_review_queues: dict[int, dict[str, Any]] = {}
        self.verification_notification_snapshots: dict[tuple[int, str, str, str, str], dict[str, Any]] = {}
        self.member_risk_states: dict[tuple[int, int], dict[str, Any]] = {}
        self.member_risk_review_queues: dict[int, dict[str, Any]] = {}

    async def load(self):
        self.configs = {}
        self.ban_candidates = {}
        self.followups = {}
        self.verification_states = {}
        self.verification_review_queues = {}
        self.verification_notification_snapshots = {}
        self.member_risk_states = {}
        self.member_risk_review_queues = {}

    async def fetch_all_configs(self) -> dict[int, dict[str, Any]]:
        return {guild_id: deepcopy(config) for guild_id, config in self.configs.items()}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        config = self.configs.get(guild_id)
        return deepcopy(config) if config is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_admin_config(int(config["guild_id"]), deepcopy(config))
        self.configs[int(config["guild_id"])] = normalized

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
            if due_at is None or due_at > now or record.get("review_pending"):
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda item: item.get("due_at") or "")
        return rows[:limit]

    async def list_review_views(self) -> list[dict[str, Any]]:
        rows = []
        for record in self.followups.values():
            if record.get("review_pending") and record.get("review_message_id"):
                rows.append(deepcopy(record))
        rows.sort(key=lambda item: (item.get("guild_id", 0), item.get("user_id", 0)))
        return rows

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        rows = []
        for record in self.verification_states.values():
            if record.get("review_pending") and record.get("review_message_id"):
                rows.append(deepcopy(record))
        rows.sort(key=lambda item: (item.get("guild_id", 0), item.get("user_id", 0)))
        return rows

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.verification_review_queues.values()]
        rows.sort(key=lambda item: item.get("guild_id", 0))
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
        record = self.verification_notification_snapshots.get((guild_id, run_context, operation, outcome, reason_code))
        return deepcopy(record) if record is not None else None

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        normalized = normalize_verification_notification_snapshot(record)
        if normalized is not None:
            key = (
                normalized["guild_id"],
                normalized["run_context"],
                normalized["operation"],
                normalized["outcome"],
                normalized["reason_code"],
            )
            self.verification_notification_snapshots[key] = normalized

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.followups.values() if record.get("guild_id") == guild_id]
        rows.sort(key=lambda item: item.get("assigned_at") or "")
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
            warning_at = _parse_datetime(record.get("warning_at"))
            if record.get("warning_sent_at") is not None or warning_at is None or warning_at > now:
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda item: item.get("warning_at") or "")
        return rows[:limit]

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = []
        for record in self.verification_states.values():
            kick_at = _parse_datetime(record.get("kick_at"))
            if kick_at is None or kick_at > now or record.get("review_pending"):
                continue
            rows.append(deepcopy(record))
        rows.sort(key=lambda item: item.get("kick_at") or "")
        return rows[:limit]

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.verification_states.values() if record.get("guild_id") == guild_id]
        rows.sort(key=lambda item: item.get("joined_at") or "")
        return rows

    async def upsert_member_risk_state(self, record: dict[str, Any]):
        normalized = normalize_member_risk_state(record)
        if normalized is not None:
            self.member_risk_states[(normalized["guild_id"], normalized["user_id"])] = normalized

    async def fetch_member_risk_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        record = self.member_risk_states.get((guild_id, user_id))
        return deepcopy(record) if record is not None else None

    async def delete_member_risk_state(self, guild_id: int, user_id: int):
        self.member_risk_states.pop((guild_id, user_id), None)

    async def list_member_risk_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.member_risk_states.values() if record.get("guild_id") == guild_id]
        rows.sort(key=lambda item: item.get("first_seen_at") or "")
        return rows

    async def list_member_risk_review_queues(self) -> list[dict[str, Any]]:
        rows = [deepcopy(record) for record in self.member_risk_review_queues.values()]
        rows.sort(key=lambda item: item.get("guild_id", 0))
        return rows

    async def fetch_member_risk_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        record = self.member_risk_review_queues.get(guild_id)
        return deepcopy(record) if record is not None else None

    async def upsert_member_risk_review_queue(self, record: dict[str, Any]):
        normalized = normalize_member_risk_review_queue(record)
        if normalized is not None:
            self.member_risk_review_queues[int(normalized["guild_id"])] = normalized

    async def delete_member_risk_review_queue(self, guild_id: int):
        self.member_risk_review_queues.pop(guild_id, None)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        followups = [record for record in self.followups.values() if record.get("guild_id") == guild_id]
        verification_rows = [record for record in self.verification_states.values() if record.get("guild_id") == guild_id]
        member_risk_rows = [record for record in self.member_risk_states.values() if record.get("guild_id") == guild_id]
        return {
            "ban_candidates": sum(1 for record in self.ban_candidates.values() if record.get("guild_id") == guild_id),
            "active_followups": len(followups),
            "pending_reviews": sum(1 for record in followups if record.get("review_pending")),
            "verification_pending": len(verification_rows),
            "verification_warned": sum(1 for record in verification_rows if record.get("warning_sent_at")),
            "member_risk_pending": sum(1 for record in member_risk_rows if record.get("review_pending")),
        }


def _config_from_row(row) -> dict[str, Any]:
    guild_id = int(row["guild_id"])
    return normalize_admin_config(
        guild_id,
        {
            "guild_id": guild_id,
            "followup_enabled": row["followup_enabled"],
            "followup_role_id": row["followup_role_id"],
            "followup_mode": row["followup_mode"],
            "followup_duration_value": int(row["followup_duration_value"]),
            "followup_duration_unit": row["followup_duration_unit"],
            "verification_enabled": row["verification_enabled"],
            "verification_role_id": row["verification_role_id"],
            "verification_logic": row["verification_logic"],
            "verification_deadline_action": row["verification_deadline_action"],
            "verification_kick_after_seconds": int(row["verification_kick_after_seconds"]),
            "verification_warning_lead_seconds": int(row["verification_warning_lead_seconds"]),
            "verification_help_channel_id": row["verification_help_channel_id"],
            "verification_help_extension_seconds": int(row["verification_help_extension_seconds"]),
            "verification_max_extensions": int(row["verification_max_extensions"]),
            "admin_log_channel_id": row["admin_log_channel_id"],
            "admin_alert_role_id": row["admin_alert_role_id"],
            "warning_template": row["warning_template"],
            "kick_template": row["kick_template"],
            "invite_link": row["invite_link"],
            "excluded_user_ids": decode_postgres_json_array(
                row["excluded_user_ids"],
                label="admin_guild_configs.excluded_user_ids",
            ),
            "excluded_role_ids": decode_postgres_json_array(
                row["excluded_role_ids"],
                label="admin_guild_configs.excluded_role_ids",
            ),
            "trusted_role_ids": decode_postgres_json_array(
                row["trusted_role_ids"],
                label="admin_guild_configs.trusted_role_ids",
            ),
            "followup_exempt_staff": row["followup_exempt_staff"],
            "verification_exempt_staff": row["verification_exempt_staff"],
            "verification_exempt_bots": row["verification_exempt_bots"],
            "member_risk_enabled": row.get("member_risk_enabled", False),
            "member_risk_mode": row.get("member_risk_mode", "review"),
        },
    )


def _followup_from_row(row) -> dict[str, Any] | None:
    return normalize_followup_record(
        {
            "guild_id": row["guild_id"],
            "user_id": row["user_id"],
            "role_id": row["role_id"],
            "assigned_at": _serialize_datetime(row["assigned_at"]),
            "due_at": _serialize_datetime(row["due_at"]),
            "mode": row["mode"],
            "review_pending": row["review_pending"],
            "review_version": int(row["review_version"]),
            "review_message_channel_id": row["review_message_channel_id"],
            "review_message_id": row["review_message_id"],
        }
    )


def _verification_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_state(
        {
            "guild_id": row["guild_id"],
            "user_id": row["user_id"],
            "joined_at": _serialize_datetime(row["joined_at"]),
            "warning_at": _serialize_datetime(row["warning_at"]),
            "kick_at": _serialize_datetime(row["kick_at"]),
            "warning_sent_at": _serialize_datetime(row["warning_sent_at"]),
            "extension_count": int(row["extension_count"]),
            "review_pending": row["review_pending"],
            "review_version": int(row["review_version"]),
            "review_message_channel_id": row["review_message_channel_id"],
            "review_message_id": row["review_message_id"],
            "last_result_code": row.get("last_result_code"),
            "last_result_at": _serialize_datetime(row.get("last_result_at")),
            "last_notified_code": row.get("last_notified_code"),
            "last_notified_at": _serialize_datetime(row.get("last_notified_at")),
        }
    )


def _verification_review_queue_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_review_queue(
        {
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "message_id": row["message_id"],
            "updated_at": _serialize_datetime(row["updated_at"]),
        }
    )


def _verification_notification_snapshot_from_row(row) -> dict[str, Any] | None:
    return normalize_verification_notification_snapshot(
        {
            "guild_id": row["guild_id"],
            "run_context": row["run_context"],
            "operation": row["operation"],
            "outcome": row["outcome"],
            "reason_code": row["reason_code"],
            "signature": row["signature"],
            "notified_at": _serialize_datetime(row["notified_at"]),
        }
    )


def _member_risk_from_row(row) -> dict[str, Any] | None:
    return normalize_member_risk_state(
        {
            "guild_id": row["guild_id"],
            "user_id": row["user_id"],
            "first_seen_at": _serialize_datetime(row["first_seen_at"]),
            "last_seen_at": _serialize_datetime(row["last_seen_at"]),
            "snooze_until": _serialize_datetime(row["snooze_until"]),
            "risk_level": row["risk_level"],
            "signal_codes": decode_postgres_json_array(
                row["signal_codes"],
                label="admin_member_risk_states.signal_codes",
            ),
            "primary_domain": row["primary_domain"],
            "review_pending": row["review_pending"],
            "review_version": int(row["review_version"]),
            "review_message_channel_id": row["review_message_channel_id"],
            "review_message_id": row["review_message_id"],
            "last_result_code": row["last_result_code"],
            "last_result_at": _serialize_datetime(row["last_result_at"]),
            "last_notified_code": row["last_notified_code"],
            "last_notified_at": _serialize_datetime(row["last_notified_at"]),
            "message_event_count": int(row["message_event_count"]),
            "latest_message_basis": row["latest_message_basis"],
            "latest_message_confidence": row["latest_message_confidence"],
            "latest_scan_source": row["latest_scan_source"],
        }
    )


def _member_risk_review_queue_from_row(row) -> dict[str, Any] | None:
    return normalize_member_risk_review_queue(
        {
            "guild_id": row["guild_id"],
            "channel_id": row["channel_id"],
            "message_id": row["message_id"],
            "updated_at": _serialize_datetime(row["updated_at"]),
        }
    )


class _PostgresAdminStore(_BaseAdminStore):
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
            raise AdminStorageUnavailable("asyncpg is not installed, so Babblebox admin storage is unavailable.") from exc
        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=2,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-admin-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise AdminStorageUnavailable(f"Could not connect to Babblebox admin storage: {last_error}") from last_error

    async def _ensure_schema(self):
        table_statements = [
            (
                "CREATE TABLE IF NOT EXISTS admin_guild_configs ("
                "guild_id BIGINT PRIMARY KEY, "
                "followup_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "followup_role_id BIGINT NULL, "
                "followup_mode TEXT NOT NULL DEFAULT 'review', "
                "followup_duration_value SMALLINT NOT NULL DEFAULT 30, "
                "followup_duration_unit TEXT NOT NULL DEFAULT 'days', "
                "verification_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "verification_role_id BIGINT NULL, "
                "verification_logic TEXT NOT NULL DEFAULT 'must_have_role', "
                "verification_deadline_action TEXT NOT NULL DEFAULT 'auto_kick', "
                "verification_kick_after_seconds INTEGER NOT NULL DEFAULT 604800, "
                "verification_warning_lead_seconds INTEGER NOT NULL DEFAULT 86400, "
                "verification_help_channel_id BIGINT NULL, "
                "verification_help_extension_seconds INTEGER NOT NULL DEFAULT 259200, "
                "verification_max_extensions SMALLINT NOT NULL DEFAULT 1, "
                "admin_log_channel_id BIGINT NULL, "
                "admin_alert_role_id BIGINT NULL, "
                "warning_template TEXT NULL, "
                "kick_template TEXT NULL, "
                "invite_link TEXT NULL, "
                "excluded_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "trusted_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "followup_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE, "
                "verification_exempt_staff BOOLEAN NOT NULL DEFAULT TRUE, "
                "verification_exempt_bots BOOLEAN NOT NULL DEFAULT TRUE, "
                "member_risk_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "member_risk_mode TEXT NOT NULL DEFAULT 'review', "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_ban_return_candidates ("
                "guild_id BIGINT NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "banned_at TIMESTAMPTZ NOT NULL, "
                "expires_at TIMESTAMPTZ NOT NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_followup_roles ("
                "guild_id BIGINT NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "role_id BIGINT NOT NULL, "
                "assigned_at TIMESTAMPTZ NOT NULL, "
                "due_at TIMESTAMPTZ NULL, "
                "mode TEXT NOT NULL, "
                "review_pending BOOLEAN NOT NULL DEFAULT FALSE, "
                "review_version INTEGER NOT NULL DEFAULT 0, "
                "review_message_channel_id BIGINT NULL, "
                "review_message_id BIGINT NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_verification_states ("
                "guild_id BIGINT NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "joined_at TIMESTAMPTZ NOT NULL, "
                "warning_at TIMESTAMPTZ NOT NULL, "
                "kick_at TIMESTAMPTZ NOT NULL, "
                "warning_sent_at TIMESTAMPTZ NULL, "
                "extension_count SMALLINT NOT NULL DEFAULT 0, "
                "review_pending BOOLEAN NOT NULL DEFAULT FALSE, "
                "review_version INTEGER NOT NULL DEFAULT 0, "
                "review_message_channel_id BIGINT NULL, "
                "review_message_id BIGINT NULL, "
                "last_result_code TEXT NULL, "
                "last_result_at TIMESTAMPTZ NULL, "
                "last_notified_code TEXT NULL, "
                "last_notified_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_member_risk_states ("
                "guild_id BIGINT NOT NULL, "
                "user_id BIGINT NOT NULL, "
                "first_seen_at TIMESTAMPTZ NOT NULL, "
                "last_seen_at TIMESTAMPTZ NOT NULL, "
                "snooze_until TIMESTAMPTZ NULL, "
                "risk_level TEXT NOT NULL, "
                "signal_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "primary_domain TEXT NULL, "
                "review_pending BOOLEAN NOT NULL DEFAULT FALSE, "
                "review_version INTEGER NOT NULL DEFAULT 0, "
                "review_message_channel_id BIGINT NULL, "
                "review_message_id BIGINT NULL, "
                "last_result_code TEXT NULL, "
                "last_result_at TIMESTAMPTZ NULL, "
                "last_notified_code TEXT NULL, "
                "last_notified_at TIMESTAMPTZ NULL, "
                "message_event_count INTEGER NOT NULL DEFAULT 0, "
                "latest_message_basis TEXT NULL, "
                "latest_message_confidence TEXT NULL, "
                "latest_scan_source TEXT NULL, "
                "PRIMARY KEY (guild_id, user_id)"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_member_risk_review_queues ("
                "guild_id BIGINT PRIMARY KEY, "
                "channel_id BIGINT NULL, "
                "message_id BIGINT NULL, "
                "updated_at TIMESTAMPTZ NULL"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_verification_review_queues ("
                "guild_id BIGINT PRIMARY KEY, "
                "channel_id BIGINT NULL, "
                "message_id BIGINT NULL, "
                "updated_at TIMESTAMPTZ NULL"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS admin_verification_notification_snapshots ("
                "guild_id BIGINT NOT NULL, "
                "run_context TEXT NOT NULL, "
                "operation TEXT NOT NULL, "
                "outcome TEXT NOT NULL, "
                "reason_code TEXT NOT NULL, "
                "signature TEXT NULL, "
                "notified_at TIMESTAMPTZ NULL, "
                "PRIMARY KEY (guild_id, run_context, operation, outcome, reason_code)"
                ")"
            ),
        ]
        alter_statements = [
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS verification_deadline_action TEXT NOT NULL DEFAULT 'auto_kick'",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS member_risk_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_guild_configs ADD COLUMN IF NOT EXISTS member_risk_mode TEXT NOT NULL DEFAULT 'review'",
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
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS snooze_until TIMESTAMPTZ NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS primary_domain TEXT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS review_pending BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS review_version INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS review_message_channel_id BIGINT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS review_message_id BIGINT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS last_result_code TEXT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS last_result_at TIMESTAMPTZ NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS last_notified_code TEXT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS message_event_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS latest_message_basis TEXT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS latest_message_confidence TEXT NULL",
            "ALTER TABLE admin_member_risk_states ADD COLUMN IF NOT EXISTS latest_scan_source TEXT NULL",
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
            "CREATE INDEX IF NOT EXISTS ix_admin_member_risk_guild ON admin_member_risk_states (guild_id)",
            "CREATE INDEX IF NOT EXISTS ix_admin_member_risk_review_pending ON admin_member_risk_states (review_pending, review_message_id)",
            "CREATE INDEX IF NOT EXISTS ix_admin_member_risk_last_notified ON admin_member_risk_states (guild_id, last_notified_at)",
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
            rows = await conn.fetch("SELECT * FROM admin_guild_configs")
        return {int(row["guild_id"]): _config_from_row(row) for row in rows}

    async def fetch_config(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_guild_configs WHERE guild_id = $1", guild_id)
        return _config_from_row(row) if row is not None else None

    async def upsert_config(self, config: dict[str, Any]):
        normalized = normalize_admin_config(int(config["guild_id"]), deepcopy(config))
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_guild_configs ("
                        "guild_id, followup_enabled, followup_role_id, followup_mode, followup_duration_value, followup_duration_unit, "
                        "verification_enabled, verification_role_id, verification_logic, verification_deadline_action, verification_kick_after_seconds, "
                        "verification_warning_lead_seconds, verification_help_channel_id, verification_help_extension_seconds, verification_max_extensions, "
                        "admin_log_channel_id, admin_alert_role_id, warning_template, kick_template, invite_link, "
                        "excluded_user_ids, excluded_role_ids, trusted_role_ids, "
                        "followup_exempt_staff, verification_exempt_staff, verification_exempt_bots, "
                        "member_risk_enabled, member_risk_mode, updated_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, "
                        "$7, $8, $9, $10, $11, "
                        "$12, $13, $14, $15, "
                        "$16, $17, $18, $19, $20, "
                        "$21::jsonb, $22::jsonb, $23::jsonb, "
                        "$24, $25, $26, $27, $28, timezone('utc', now())"
                        ") "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "followup_enabled = EXCLUDED.followup_enabled, "
                        "followup_role_id = EXCLUDED.followup_role_id, "
                        "followup_mode = EXCLUDED.followup_mode, "
                        "followup_duration_value = EXCLUDED.followup_duration_value, "
                        "followup_duration_unit = EXCLUDED.followup_duration_unit, "
                        "verification_enabled = EXCLUDED.verification_enabled, "
                        "verification_role_id = EXCLUDED.verification_role_id, "
                        "verification_logic = EXCLUDED.verification_logic, "
                        "verification_deadline_action = EXCLUDED.verification_deadline_action, "
                        "verification_kick_after_seconds = EXCLUDED.verification_kick_after_seconds, "
                        "verification_warning_lead_seconds = EXCLUDED.verification_warning_lead_seconds, "
                        "verification_help_channel_id = EXCLUDED.verification_help_channel_id, "
                        "verification_help_extension_seconds = EXCLUDED.verification_help_extension_seconds, "
                        "verification_max_extensions = EXCLUDED.verification_max_extensions, "
                        "admin_log_channel_id = EXCLUDED.admin_log_channel_id, "
                        "admin_alert_role_id = EXCLUDED.admin_alert_role_id, "
                        "warning_template = EXCLUDED.warning_template, "
                        "kick_template = EXCLUDED.kick_template, "
                        "invite_link = EXCLUDED.invite_link, "
                        "excluded_user_ids = EXCLUDED.excluded_user_ids, "
                        "excluded_role_ids = EXCLUDED.excluded_role_ids, "
                        "trusted_role_ids = EXCLUDED.trusted_role_ids, "
                        "followup_exempt_staff = EXCLUDED.followup_exempt_staff, "
                        "verification_exempt_staff = EXCLUDED.verification_exempt_staff, "
                        "verification_exempt_bots = EXCLUDED.verification_exempt_bots, "
                        "member_risk_enabled = EXCLUDED.member_risk_enabled, "
                        "member_risk_mode = EXCLUDED.member_risk_mode, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["guild_id"],
                    normalized["followup_enabled"],
                    normalized["followup_role_id"],
                    normalized["followup_mode"],
                    normalized["followup_duration_value"],
                    normalized["followup_duration_unit"],
                    normalized["verification_enabled"],
                    normalized["verification_role_id"],
                    normalized["verification_logic"],
                    normalized["verification_deadline_action"],
                    normalized["verification_kick_after_seconds"],
                    normalized["verification_warning_lead_seconds"],
                    normalized["verification_help_channel_id"],
                    normalized["verification_help_extension_seconds"],
                    normalized["verification_max_extensions"],
                    normalized["admin_log_channel_id"],
                    normalized["admin_alert_role_id"],
                    normalized["warning_template"],
                    normalized["kick_template"],
                    normalized["invite_link"],
                    json.dumps(normalized["excluded_user_ids"]),
                    json.dumps(normalized["excluded_role_ids"]),
                    json.dumps(normalized["trusted_role_ids"]),
                    normalized["followup_exempt_staff"],
                    normalized["verification_exempt_staff"],
                    normalized["verification_exempt_bots"],
                    normalized["member_risk_enabled"],
                    normalized["member_risk_mode"],
                )

    async def upsert_ban_candidate(self, record: dict[str, Any]):
        normalized = normalize_ban_candidate(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_ban_return_candidates (guild_id, user_id, banned_at, expires_at) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                        "banned_at = EXCLUDED.banned_at, expires_at = EXCLUDED.expires_at"
                    ),
                    normalized["guild_id"],
                    normalized["user_id"],
                    _parse_datetime(normalized["banned_at"]),
                    _parse_datetime(normalized["expires_at"]),
                )

    async def fetch_ban_candidate(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT guild_id, user_id, banned_at, expires_at FROM admin_ban_return_candidates WHERE guild_id = $1 AND user_id = $2",
                guild_id,
                user_id,
            )
        if row is None:
            return None
        return normalize_ban_candidate(
            {
                "guild_id": row["guild_id"],
                "user_id": row["user_id"],
                "banned_at": _serialize_datetime(row["banned_at"]),
                "expires_at": _serialize_datetime(row["expires_at"]),
            }
        )

    async def delete_ban_candidate(self, guild_id: int, user_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_ban_return_candidates WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def prune_expired_ban_candidates(self, now: datetime, *, limit: int = 200) -> int:
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                rows = await conn.fetch(
                    (
                        "WITH doomed AS ("
                        "SELECT guild_id, user_id FROM admin_ban_return_candidates WHERE expires_at <= $1 ORDER BY expires_at ASC LIMIT $2"
                        ") "
                        "DELETE FROM admin_ban_return_candidates target USING doomed "
                        "WHERE target.guild_id = doomed.guild_id AND target.user_id = doomed.user_id "
                        "RETURNING target.guild_id"
                    ),
                    now,
                    limit,
                )
        return len(rows)

    async def upsert_followup(self, record: dict[str, Any]):
        normalized = normalize_followup_record(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_followup_roles ("
                        "guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, review_message_channel_id, review_message_id"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10"
                        ") ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                        "role_id = EXCLUDED.role_id, "
                        "assigned_at = EXCLUDED.assigned_at, "
                        "due_at = EXCLUDED.due_at, "
                        "mode = EXCLUDED.mode, "
                        "review_pending = EXCLUDED.review_pending, "
                        "review_version = EXCLUDED.review_version, "
                        "review_message_channel_id = EXCLUDED.review_message_channel_id, "
                        "review_message_id = EXCLUDED.review_message_id"
                    ),
                    normalized["guild_id"],
                    normalized["user_id"],
                    normalized["role_id"],
                    _parse_datetime(normalized["assigned_at"]),
                    _parse_datetime(normalized["due_at"]),
                    normalized["mode"],
                    normalized["review_pending"],
                    normalized["review_version"],
                    normalized["review_message_channel_id"],
                    normalized["review_message_id"],
                )

    async def fetch_followup(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, "
                    "review_message_channel_id, review_message_id "
                    "FROM admin_followup_roles WHERE guild_id = $1 AND user_id = $2"
                ),
                guild_id,
                user_id,
            )
        return _followup_from_row(row) if row is not None else None

    async def delete_followup(self, guild_id: int, user_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_followup_roles WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def list_due_followups(self, now: datetime, *, limit: int = 50) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, "
                    "review_message_channel_id, review_message_id "
                    "FROM admin_followup_roles "
                    "WHERE due_at IS NOT NULL AND due_at <= $1 AND review_pending = FALSE "
                    "ORDER BY due_at ASC LIMIT $2"
                ),
                now,
                limit,
            )
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def list_review_views(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, "
                    "review_message_channel_id, review_message_id "
                    "FROM admin_followup_roles "
                    "WHERE review_pending = TRUE AND review_message_id IS NOT NULL "
                    "ORDER BY guild_id ASC, user_id ASC"
                )
            )
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                    "review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at "
                    "FROM admin_verification_states "
                    "WHERE review_pending = TRUE AND review_message_id IS NOT NULL "
                    "ORDER BY guild_id ASC, user_id ASC"
                )
            )
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT guild_id, channel_id, message_id, updated_at FROM admin_verification_review_queues ORDER BY guild_id ASC"
            )
        return [record for row in rows if (record := _verification_review_queue_from_row(row)) is not None]

    async def fetch_verification_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT guild_id, channel_id, message_id, updated_at FROM admin_verification_review_queues WHERE guild_id = $1",
                guild_id,
            )
        return _verification_review_queue_from_row(row) if row is not None else None

    async def upsert_verification_review_queue(self, record: dict[str, Any]):
        normalized = normalize_verification_review_queue(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_verification_review_queues (guild_id, channel_id, message_id, updated_at) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "channel_id = EXCLUDED.channel_id, "
                        "message_id = EXCLUDED.message_id, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["guild_id"],
                    normalized["channel_id"],
                    normalized["message_id"],
                    _parse_datetime(normalized["updated_at"]),
                )

    async def delete_verification_review_queue(self, guild_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_verification_review_queues WHERE guild_id = $1", guild_id)

    async def fetch_verification_notification_snapshot(
        self,
        guild_id: int,
        *,
        run_context: str,
        operation: str,
        outcome: str,
        reason_code: str,
    ) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, run_context, operation, outcome, reason_code, signature, notified_at "
                    "FROM admin_verification_notification_snapshots "
                    "WHERE guild_id = $1 AND run_context = $2 AND operation = $3 AND outcome = $4 AND reason_code = $5"
                ),
                guild_id,
                run_context,
                operation,
                outcome,
                reason_code,
            )
        return _verification_notification_snapshot_from_row(row) if row is not None else None

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        normalized = normalize_verification_notification_snapshot(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_verification_notification_snapshots ("
                        "guild_id, run_context, operation, outcome, reason_code, signature, notified_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7"
                        ") ON CONFLICT (guild_id, run_context, operation, outcome, reason_code) DO UPDATE SET "
                        "signature = EXCLUDED.signature, "
                        "notified_at = EXCLUDED.notified_at"
                    ),
                    normalized["guild_id"],
                    normalized["run_context"],
                    normalized["operation"],
                    normalized["outcome"],
                    normalized["reason_code"],
                    normalized["signature"],
                    _parse_datetime(normalized["notified_at"]),
                )

    async def list_followups_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, role_id, assigned_at, due_at, mode, review_pending, review_version, "
                    "review_message_channel_id, review_message_id "
                    "FROM admin_followup_roles WHERE guild_id = $1 ORDER BY assigned_at ASC"
                ),
                guild_id,
            )
        return [record for row in rows if (record := _followup_from_row(row)) is not None]

    async def upsert_verification_state(self, record: dict[str, Any]):
        normalized = normalize_verification_state(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_verification_states ("
                        "guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                        "review_pending, review_version, review_message_channel_id, review_message_id, "
                        "last_result_code, last_result_at, last_notified_code, last_notified_at"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15"
                        ") ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                        "joined_at = EXCLUDED.joined_at, "
                        "warning_at = EXCLUDED.warning_at, "
                        "kick_at = EXCLUDED.kick_at, "
                        "warning_sent_at = EXCLUDED.warning_sent_at, "
                        "extension_count = EXCLUDED.extension_count, "
                        "review_pending = EXCLUDED.review_pending, "
                        "review_version = EXCLUDED.review_version, "
                        "review_message_channel_id = EXCLUDED.review_message_channel_id, "
                        "review_message_id = EXCLUDED.review_message_id, "
                        "last_result_code = EXCLUDED.last_result_code, "
                        "last_result_at = EXCLUDED.last_result_at, "
                        "last_notified_code = EXCLUDED.last_notified_code, "
                        "last_notified_at = EXCLUDED.last_notified_at"
                    ),
                    normalized["guild_id"],
                    normalized["user_id"],
                    _parse_datetime(normalized["joined_at"]),
                    _parse_datetime(normalized["warning_at"]),
                    _parse_datetime(normalized["kick_at"]),
                    _parse_datetime(normalized["warning_sent_at"]),
                    normalized["extension_count"],
                    normalized["review_pending"],
                    normalized["review_version"],
                    normalized["review_message_channel_id"],
                    normalized["review_message_id"],
                    normalized["last_result_code"],
                    _parse_datetime(normalized["last_result_at"]),
                    normalized["last_notified_code"],
                    _parse_datetime(normalized["last_notified_at"]),
                )

    async def fetch_verification_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                    "review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at "
                    "FROM admin_verification_states WHERE guild_id = $1 AND user_id = $2"
                ),
                guild_id,
                user_id,
            )
        return _verification_from_row(row) if row is not None else None

    async def delete_verification_state(self, guild_id: int, user_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_verification_states WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def list_due_verification_warnings(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                    "review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at "
                    "FROM admin_verification_states "
                    "WHERE warning_sent_at IS NULL AND warning_at <= $1 "
                    "ORDER BY warning_at ASC LIMIT $2"
                ),
                now,
                limit,
            )
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_due_verification_kicks(self, now: datetime, *, limit: int = 100) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                    "review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at "
                    "FROM admin_verification_states "
                    "WHERE kick_at <= $1 AND review_pending = FALSE "
                    "ORDER BY kick_at ASC LIMIT $2"
                ),
                now,
                limit,
            )
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def list_verification_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, joined_at, warning_at, kick_at, warning_sent_at, extension_count, "
                    "review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at "
                    "FROM admin_verification_states WHERE guild_id = $1 ORDER BY joined_at ASC"
                ),
                guild_id,
            )
        return [record for row in rows if (record := _verification_from_row(row)) is not None]

    async def upsert_member_risk_state(self, record: dict[str, Any]):
        normalized = normalize_member_risk_state(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_member_risk_states ("
                        "guild_id, user_id, first_seen_at, last_seen_at, snooze_until, risk_level, signal_codes, "
                        "primary_domain, review_pending, review_version, review_message_channel_id, review_message_id, "
                        "last_result_code, last_result_at, last_notified_code, last_notified_at, "
                        "message_event_count, latest_message_basis, latest_message_confidence, latest_scan_source"
                        ") VALUES ("
                        "$1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20"
                        ") ON CONFLICT (guild_id, user_id) DO UPDATE SET "
                        "first_seen_at = EXCLUDED.first_seen_at, "
                        "last_seen_at = EXCLUDED.last_seen_at, "
                        "snooze_until = EXCLUDED.snooze_until, "
                        "risk_level = EXCLUDED.risk_level, "
                        "signal_codes = EXCLUDED.signal_codes, "
                        "primary_domain = EXCLUDED.primary_domain, "
                        "review_pending = EXCLUDED.review_pending, "
                        "review_version = EXCLUDED.review_version, "
                        "review_message_channel_id = EXCLUDED.review_message_channel_id, "
                        "review_message_id = EXCLUDED.review_message_id, "
                        "last_result_code = EXCLUDED.last_result_code, "
                        "last_result_at = EXCLUDED.last_result_at, "
                        "last_notified_code = EXCLUDED.last_notified_code, "
                        "last_notified_at = EXCLUDED.last_notified_at, "
                        "message_event_count = EXCLUDED.message_event_count, "
                        "latest_message_basis = EXCLUDED.latest_message_basis, "
                        "latest_message_confidence = EXCLUDED.latest_message_confidence, "
                        "latest_scan_source = EXCLUDED.latest_scan_source"
                    ),
                    normalized["guild_id"],
                    normalized["user_id"],
                    _parse_datetime(normalized["first_seen_at"]),
                    _parse_datetime(normalized["last_seen_at"]),
                    _parse_datetime(normalized["snooze_until"]),
                    normalized["risk_level"],
                    json.dumps(normalized["signal_codes"]),
                    normalized["primary_domain"],
                    normalized["review_pending"],
                    normalized["review_version"],
                    normalized["review_message_channel_id"],
                    normalized["review_message_id"],
                    normalized["last_result_code"],
                    _parse_datetime(normalized["last_result_at"]),
                    normalized["last_notified_code"],
                    _parse_datetime(normalized["last_notified_at"]),
                    normalized["message_event_count"],
                    normalized["latest_message_basis"],
                    normalized["latest_message_confidence"],
                    normalized["latest_scan_source"],
                )

    async def fetch_member_risk_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT guild_id, user_id, first_seen_at, last_seen_at, snooze_until, risk_level, signal_codes, "
                    "primary_domain, review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at, "
                    "message_event_count, latest_message_basis, latest_message_confidence, latest_scan_source "
                    "FROM admin_member_risk_states WHERE guild_id = $1 AND user_id = $2"
                ),
                guild_id,
                user_id,
            )
        return _member_risk_from_row(row) if row is not None else None

    async def delete_member_risk_state(self, guild_id: int, user_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_member_risk_states WHERE guild_id = $1 AND user_id = $2", guild_id, user_id)

    async def list_member_risk_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                (
                    "SELECT guild_id, user_id, first_seen_at, last_seen_at, snooze_until, risk_level, signal_codes, "
                    "primary_domain, review_pending, review_version, review_message_channel_id, review_message_id, "
                    "last_result_code, last_result_at, last_notified_code, last_notified_at, "
                    "message_event_count, latest_message_basis, latest_message_confidence, latest_scan_source "
                    "FROM admin_member_risk_states WHERE guild_id = $1 ORDER BY first_seen_at ASC"
                ),
                guild_id,
            )
        return [record for row in rows if (record := _member_risk_from_row(row)) is not None]

    async def list_member_risk_review_queues(self) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT guild_id, channel_id, message_id, updated_at FROM admin_member_risk_review_queues ORDER BY guild_id ASC"
            )
        return [record for row in rows if (record := _member_risk_review_queue_from_row(row)) is not None]

    async def fetch_member_risk_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT guild_id, channel_id, message_id, updated_at FROM admin_member_risk_review_queues WHERE guild_id = $1",
                guild_id,
            )
        return _member_risk_review_queue_from_row(row) if row is not None else None

    async def upsert_member_risk_review_queue(self, record: dict[str, Any]):
        normalized = normalize_member_risk_review_queue(record)
        if normalized is None:
            return
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    (
                        "INSERT INTO admin_member_risk_review_queues (guild_id, channel_id, message_id, updated_at) "
                        "VALUES ($1, $2, $3, $4) "
                        "ON CONFLICT (guild_id) DO UPDATE SET "
                        "channel_id = EXCLUDED.channel_id, "
                        "message_id = EXCLUDED.message_id, "
                        "updated_at = EXCLUDED.updated_at"
                    ),
                    normalized["guild_id"],
                    normalized["channel_id"],
                    normalized["message_id"],
                    _parse_datetime(normalized["updated_at"]),
                )

    async def delete_member_risk_review_queue(self, guild_id: int):
        async with self._io_lock:
            async with self._pool.acquire() as conn:
                await conn.execute("DELETE FROM admin_member_risk_review_queues WHERE guild_id = $1", guild_id)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                (
                    "SELECT "
                    "(SELECT COUNT(*) FROM admin_ban_return_candidates WHERE guild_id = $1) AS ban_candidates, "
                    "(SELECT COUNT(*) FROM admin_followup_roles WHERE guild_id = $1) AS active_followups, "
                    "(SELECT COUNT(*) FROM admin_followup_roles WHERE guild_id = $1 AND review_pending = TRUE) AS pending_reviews, "
                    "(SELECT COUNT(*) FROM admin_verification_states WHERE guild_id = $1) AS verification_pending, "
                    "(SELECT COUNT(*) FROM admin_verification_states WHERE guild_id = $1 AND warning_sent_at IS NOT NULL) AS verification_warned, "
                    "(SELECT COUNT(*) FROM admin_member_risk_states WHERE guild_id = $1 AND review_pending = TRUE) AS member_risk_pending"
                ),
                guild_id,
            )
        return {
            "ban_candidates": int(row["ban_candidates"] or 0),
            "active_followups": int(row["active_followups"] or 0),
            "pending_reviews": int(row["pending_reviews"] or 0),
            "verification_pending": int(row["verification_pending"] or 0),
            "verification_warned": int(row["verification_warned"] or 0),
            "member_risk_pending": int(row["member_risk_pending"] or 0),
        }


class AdminStore:
    def __init__(
        self,
        *,
        backend: str | None = None,
        database_url: str | None = None,
    ):
        requested_backend = (backend or os.getenv("ADMIN_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.backend_name = requested_backend
        self._store: _BaseAdminStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        print(
            "Admin storage init: "
            f"backend_preference={requested_backend}, "
            f"database_url_configured={'yes' if self.database_url else 'no'}, "
            f"database_url_source={self.database_url_source or 'none'}, "
            f"database_target={_redact_database_url(self.database_url)}"
        )
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

    async def fetch_verification_notification_snapshot(
        self,
        guild_id: int,
        *,
        run_context: str,
        operation: str,
        outcome: str,
        reason_code: str,
    ) -> dict[str, Any] | None:
        return await self._store.fetch_verification_notification_snapshot(
            guild_id,
            run_context=run_context,
            operation=operation,
            outcome=outcome,
            reason_code=reason_code,
        )

    async def upsert_verification_notification_snapshot(self, record: dict[str, Any]):
        await self._store.upsert_verification_notification_snapshot(record)

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

    async def upsert_member_risk_state(self, record: dict[str, Any]):
        await self._store.upsert_member_risk_state(record)

    async def fetch_member_risk_state(self, guild_id: int, user_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_member_risk_state(guild_id, user_id)

    async def delete_member_risk_state(self, guild_id: int, user_id: int):
        await self._store.delete_member_risk_state(guild_id, user_id)

    async def list_member_risk_states_for_guild(self, guild_id: int) -> list[dict[str, Any]]:
        return await self._store.list_member_risk_states_for_guild(guild_id)

    async def list_member_risk_review_queues(self) -> list[dict[str, Any]]:
        return await self._store.list_member_risk_review_queues()

    async def fetch_member_risk_review_queue(self, guild_id: int) -> dict[str, Any] | None:
        return await self._store.fetch_member_risk_review_queue(guild_id)

    async def upsert_member_risk_review_queue(self, record: dict[str, Any]):
        await self._store.upsert_member_risk_review_queue(record)

    async def delete_member_risk_review_queue(self, guild_id: int):
        await self._store.delete_member_risk_review_queue(guild_id)

    async def fetch_guild_counts(self, guild_id: int) -> dict[str, int]:
        return await self._store.fetch_guild_counts(guild_id)

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
