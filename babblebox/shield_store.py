from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from babblebox.postgres_json import decode_postgres_json_array, decode_postgres_json_object
from babblebox.premium_limits import (
    LIMIT_SHIELD_CUSTOM_PATTERNS,
    LIMIT_SHIELD_SEVERE_TERMS,
    storage_ceiling as premium_storage_ceiling,
)
from babblebox.shield_ai import (
    SHIELD_AI_MIN_CONFIDENCE_CHOICES,
    SHIELD_AI_MODEL_ORDER,
    SHIELD_AI_REVIEW_PACKS,
    parse_shield_ai_model_list,
)


LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE_URL_ENV_ORDER = ("UTILITY_DATABASE_URL", "SUPABASE_DB_URL", "DATABASE_URL")
DEFAULT_BACKEND = "postgres"
DEFAULT_VERSION = 12
VALID_SCAN_MODES = {"all", "only_included"}
VALID_SHIELD_ACTIONS = {"disabled", "detect", "log", "delete_log", "delete_escalate", "timeout_log", "delete_timeout_log"}
VALID_SHIELD_SENSITIVITIES = {"low", "normal", "high"}
VALID_SHIELD_LINK_POLICY_MODES = {"default", "trusted_only"}
DEFAULT_SHIELD_LINK_POLICY_MODE = "default"
VALID_SHIELD_SEVERE_CATEGORIES = (
    "sexual_exploitation",
    "self_harm_encouragement",
    "eliminationist_hate",
    "severe_slur_abuse",
)
DEFAULT_SHIELD_SEVERE_CATEGORIES = list(VALID_SHIELD_SEVERE_CATEGORIES)
SHIELD_SEVERE_TERM_LIMIT = 20
LOW_CONFIDENCE_ACTIONS = {"detect", "log"}
MEDIUM_CONFIDENCE_ACTIONS = {"detect", "log", "delete_log"}
HIGH_CONFIDENCE_ACTIONS = VALID_SHIELD_ACTIONS - {"disabled"}
CONFIDENCE_TIERS = ("low", "medium", "high")
SHIELD_META_GLOBAL_AI_OVERRIDE_KEY = "global_ai_override"
SHIELD_META_ORDINARY_AI_POLICY_KEY = "ordinary_ai_policy"
VALID_SHIELD_AI_ACCESS_MODES = {"inherit", "enabled", "disabled"}
VALID_SPAM_MODERATOR_POLICIES = {"exempt", "delete_only", "full"}
PACK_EXEMPTION_PACKS = ("privacy", "promo", "scam", "spam", "gif", "adult", "severe")
PACK_TIMEOUT_PACKS = PACK_EXEMPTION_PACKS + ("link_policy",)
VALID_SHIELD_LOG_STYLES = {"adaptive", "compact"}
VALID_SHIELD_LOG_PING_MODES = {"smart", "never"}
VALID_PACK_LOG_OVERRIDE_STYLES = {"inherit"} | VALID_SHIELD_LOG_STYLES
VALID_PACK_LOG_OVERRIDE_PING_MODES = {"inherit"} | VALID_SHIELD_LOG_PING_MODES
SHIELD_SEVERE_STORAGE_LIMIT = premium_storage_ceiling(LIMIT_SHIELD_SEVERE_TERMS, SHIELD_SEVERE_TERM_LIMIT)
SHIELD_CUSTOM_PATTERN_STORAGE_LIMIT = premium_storage_ceiling(LIMIT_SHIELD_CUSTOM_PATTERNS, 10)
SHIELD_NUMERIC_CONFIG_SPECS: dict[str, tuple[int, int, int]] = {
    "escalation_threshold": (2, 6, 3),
    "escalation_window_minutes": (5, 120, 15),
    "timeout_minutes": (1, 60, 10),
    "spam_message_threshold": (4, 12, 7),
    "spam_message_window_seconds": (3, 30, 5),
    "spam_burst_threshold": (4, 10, 5),
    "spam_burst_window_seconds": (5, 30, 10),
    "spam_near_duplicate_threshold": (3, 10, 5),
    "spam_near_duplicate_window_seconds": (5, 45, 10),
    "spam_emote_threshold": (8, 40, 18),
    "spam_caps_threshold": (12, 80, 28),
    "spam_low_value_threshold": (4, 12, 5),
    "spam_low_value_window_seconds": (20, 120, 60),
    "gif_message_threshold": (3, 12, 4),
    "gif_window_seconds": (3, 45, 20),
    "gif_consecutive_threshold": (3, 10, 5),
    "gif_repeat_threshold": (2, 6, 3),
    "gif_same_asset_threshold": (2, 6, 3),
    "gif_min_ratio_percent": (50, 95, 70),
}


def shield_numeric_config_default(field: str) -> int:
    return SHIELD_NUMERIC_CONFIG_SPECS[field][2]


class ShieldStorageUnavailable(RuntimeError):
    pass


def default_shield_meta() -> dict[str, Any]:
    return {
        "ordinary_ai_enabled": False,
        "ordinary_ai_allowed_models": list(SHIELD_AI_MODEL_ORDER),
        "ordinary_ai_updated_by": None,
        "ordinary_ai_updated_at": None,
    }


def _default_pack_exemptions() -> dict[str, dict[str, list[int]]]:
    return {
        pack: {
            "channel_ids": [],
            "role_ids": [],
            "user_ids": [],
        }
        for pack in PACK_EXEMPTION_PACKS
    }


def _default_pack_timeout_minutes() -> dict[str, int | None]:
    return {pack: None for pack in PACK_TIMEOUT_PACKS}


def _default_pack_log_overrides() -> dict[str, dict[str, str]]:
    return {
        pack: {
            "style": "inherit",
            "ping_mode": "inherit",
        }
        for pack in PACK_EXEMPTION_PACKS
    }


def _legacy_action_policy(action: str) -> tuple[str, str, str]:
    cleaned = str(action).strip().lower()
    if cleaned == "detect":
        return ("detect", "detect", "detect")
    if cleaned == "log":
        return ("log", "log", "log")
    if cleaned == "delete_log":
        return ("log", "delete_log", "delete_log")
    if cleaned == "timeout_log":
        return ("log", "delete_log", "timeout_log")
    if cleaned == "delete_timeout_log":
        return ("log", "delete_log", "delete_timeout_log")
    if cleaned == "delete_escalate":
        return ("log", "delete_log", "delete_escalate")
    return ("log", "log", "log")


def _clean_action_value(value: Any, *, allowed: set[str], fallback: str) -> str:
    cleaned = str(value).strip().lower() if isinstance(value, str) else fallback
    return cleaned if cleaned in allowed else fallback


def default_guild_shield_config(guild_id: int | None = None) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "module_enabled": False,
        "baseline_version": 0,
        "log_channel_id": None,
        "alert_role_id": None,
        "log_style": "adaptive",
        "log_ping_mode": "smart",
        "scan_mode": "all",
        "included_channel_ids": [],
        "excluded_channel_ids": [],
        "included_user_ids": [],
        "excluded_user_ids": [],
        "included_role_ids": [],
        "excluded_role_ids": [],
        "trusted_role_ids": [],
        "allow_domains": [],
        "allow_invite_codes": [],
        "allow_phrases": [],
        "trusted_builtin_disabled_families": [],
        "trusted_builtin_disabled_domains": [],
        "pack_log_overrides": _default_pack_log_overrides(),
        "pack_exemptions": _default_pack_exemptions(),
        "pack_timeout_minutes": _default_pack_timeout_minutes(),
        "privacy_enabled": False,
        "privacy_action": "log",
        "privacy_low_action": "log",
        "privacy_medium_action": "log",
        "privacy_high_action": "log",
        "privacy_sensitivity": "normal",
        "promo_enabled": False,
        "promo_action": "log",
        "promo_low_action": "log",
        "promo_medium_action": "log",
        "promo_high_action": "log",
        "promo_sensitivity": "normal",
        "scam_enabled": False,
        "scam_action": "log",
        "scam_low_action": "log",
        "scam_medium_action": "log",
        "scam_high_action": "log",
        "scam_sensitivity": "normal",
        "spam_enabled": False,
        "spam_action": "log",
        "spam_low_action": "log",
        "spam_medium_action": "log",
        "spam_high_action": "log",
        "spam_sensitivity": "normal",
        "spam_message_enabled": True,
        "spam_message_threshold": shield_numeric_config_default("spam_message_threshold"),
        "spam_message_window_seconds": shield_numeric_config_default("spam_message_window_seconds"),
        "spam_burst_enabled": True,
        "spam_burst_threshold": shield_numeric_config_default("spam_burst_threshold"),
        "spam_burst_window_seconds": shield_numeric_config_default("spam_burst_window_seconds"),
        "spam_near_duplicate_enabled": True,
        "spam_near_duplicate_threshold": shield_numeric_config_default("spam_near_duplicate_threshold"),
        "spam_near_duplicate_window_seconds": shield_numeric_config_default("spam_near_duplicate_window_seconds"),
        "spam_emote_enabled": False,
        "spam_emote_threshold": shield_numeric_config_default("spam_emote_threshold"),
        "spam_caps_enabled": False,
        "spam_caps_threshold": shield_numeric_config_default("spam_caps_threshold"),
        "spam_low_value_enabled": False,
        "spam_low_value_threshold": shield_numeric_config_default("spam_low_value_threshold"),
        "spam_low_value_window_seconds": shield_numeric_config_default("spam_low_value_window_seconds"),
        "spam_moderator_policy": "exempt",
        "gif_enabled": False,
        "gif_action": "log",
        "gif_low_action": "log",
        "gif_medium_action": "log",
        "gif_high_action": "log",
        "gif_sensitivity": "normal",
        "gif_message_enabled": True,
        "gif_message_threshold": shield_numeric_config_default("gif_message_threshold"),
        "gif_window_seconds": shield_numeric_config_default("gif_window_seconds"),
        "gif_consecutive_enabled": True,
        "gif_consecutive_threshold": shield_numeric_config_default("gif_consecutive_threshold"),
        "gif_repeat_enabled": True,
        "gif_repeat_threshold": shield_numeric_config_default("gif_repeat_threshold"),
        "gif_same_asset_enabled": True,
        "gif_same_asset_threshold": shield_numeric_config_default("gif_same_asset_threshold"),
        "gif_min_ratio_percent": shield_numeric_config_default("gif_min_ratio_percent"),
        "adult_enabled": False,
        "adult_action": "log",
        "adult_low_action": "log",
        "adult_medium_action": "log",
        "adult_high_action": "log",
        "adult_sensitivity": "normal",
        "adult_solicitation_enabled": False,
        "adult_solicitation_excluded_channel_ids": [],
        "severe_enabled": False,
        "severe_action": "log",
        "severe_low_action": "log",
        "severe_medium_action": "log",
        "severe_high_action": "log",
        "severe_sensitivity": "normal",
        "severe_enabled_categories": list(DEFAULT_SHIELD_SEVERE_CATEGORIES),
        "severe_custom_terms": [],
        "severe_removed_terms": [],
        "link_policy_mode": DEFAULT_SHIELD_LINK_POLICY_MODE,
        "link_policy_action": "log",
        "link_policy_low_action": "log",
        "link_policy_medium_action": "log",
        "link_policy_high_action": "log",
        "ai_enabled": False,
        "ai_access_mode": "inherit",
        "ai_allowed_models_override": [],
        "ai_access_updated_by": None,
        "ai_access_updated_at": None,
        "ai_min_confidence": "high",
        "ai_enabled_packs": list(SHIELD_AI_REVIEW_PACKS),
        "escalation_threshold": shield_numeric_config_default("escalation_threshold"),
        "escalation_window_minutes": shield_numeric_config_default("escalation_window_minutes"),
        "timeout_minutes": shield_numeric_config_default("timeout_minutes"),
        "custom_patterns": [],
    }


def default_shield_state() -> dict[str, Any]:
    return {"version": DEFAULT_VERSION, "meta": default_shield_meta(), "guilds": {}, "alert_actions": {}}


def _clean_int_list(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _clean_text_list(values: Any) -> list[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return sorted({str(value).strip().casefold() for value in values if isinstance(value, str) and str(value).strip()})


def _clean_model_list(values: Any) -> list[str]:
    try:
        return list(parse_shield_ai_model_list(values))
    except ValueError:
        return []


def _clean_severe_category_list(values: Any) -> list[str]:
    return [value for value in _clean_text_list(values) if value in VALID_SHIELD_SEVERE_CATEGORIES]


def _clean_pack_exemption_entry(value: Any) -> dict[str, list[int]]:
    if not isinstance(value, dict):
        return {"channel_ids": [], "role_ids": [], "user_ids": []}
    return {
        "channel_ids": _clean_int_list(value.get("channel_ids")),
        "role_ids": _clean_int_list(value.get("role_ids")),
        "user_ids": _clean_int_list(value.get("user_ids")),
    }


def _clean_pack_exemptions(values: Any) -> dict[str, dict[str, list[int]]]:
    cleaned = _default_pack_exemptions()
    if not isinstance(values, dict):
        return cleaned
    for pack in PACK_EXEMPTION_PACKS:
        cleaned[pack] = _clean_pack_exemption_entry(values.get(pack))
    return cleaned


def _clean_pack_timeout_minutes(values: Any) -> dict[str, int | None]:
    cleaned = _default_pack_timeout_minutes()
    if not isinstance(values, dict):
        return cleaned
    for pack in PACK_TIMEOUT_PACKS:
        value = values.get(pack)
        cleaned[pack] = value if isinstance(value, int) and 1 <= value <= 60 else None
    return cleaned


def _clean_pack_log_override_entry(value: Any) -> dict[str, str]:
    if not isinstance(value, dict):
        return {"style": "inherit", "ping_mode": "inherit"}
    style = str(value.get("style", "inherit")).strip().lower()
    ping_mode = str(value.get("ping_mode", "inherit")).strip().lower()
    return {
        "style": style if style in VALID_PACK_LOG_OVERRIDE_STYLES else "inherit",
        "ping_mode": ping_mode if ping_mode in VALID_PACK_LOG_OVERRIDE_PING_MODES else "inherit",
    }


def _clean_pack_log_overrides(values: Any) -> dict[str, dict[str, str]]:
    cleaned = _default_pack_log_overrides()
    if not isinstance(values, dict):
        return cleaned
    for pack in PACK_EXEMPTION_PACKS:
        cleaned[pack] = _clean_pack_log_override_entry(values.get(pack))
    return cleaned


def _clean_alert_action_records(values: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(values, dict):
        return {}
    cleaned: dict[str, dict[str, Any]] = {}
    for raw_token, raw_record in values.items():
        token = str(raw_token or "").strip()
        if not token or len(token) > 96 or not isinstance(raw_record, dict):
            continue
        record = dict(raw_record)
        record["token"] = token
        for field in (
            "guild_id",
            "log_channel_id",
            "alert_message_id",
            "target_channel_id",
            "target_message_id",
            "target_user_id",
            "moderator_user_id",
        ):
            value = record.get(field)
            record[field] = value if isinstance(value, int) and value > 0 else None
        for field in ("deleted_by_shield", "deleted_by_moderator", "timed_out_by_shield", "used"):
            record[field] = bool(record.get(field))
        for field in (
            "pack",
            "action",
            "match_class",
            "jump_url",
            "recovery_content",
            "created_at",
            "expires_at",
            "used_at",
            "status",
        ):
            value = record.get(field)
            record[field] = str(value)[:8000] if isinstance(value, str) else None
        if record["guild_id"] is None or record["log_channel_id"] is None or record["target_user_id"] is None:
            continue
        cleaned[token] = record
    return cleaned


def _legacy_pack_payload(config: dict[str, Any], pack: str) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    sources = []
    pack_map = config.get("packs")
    if isinstance(pack_map, dict):
        sources.append(pack_map.get(pack))
    sources.append(config.get(pack))

    for candidate in sources:
        if isinstance(candidate, bool):
            payload.setdefault("enabled", candidate)
            continue
        if not isinstance(candidate, dict):
            continue
        if "enabled" in candidate:
            payload.setdefault("enabled", candidate.get("enabled"))
        elif "tracking" in candidate:
            payload.setdefault("enabled", candidate.get("tracking"))
        if "action" in candidate:
            payload.setdefault("action", candidate.get("action"))
        if "sensitivity" in candidate:
            payload.setdefault("sensitivity", candidate.get("sensitivity"))
    return payload


def normalize_guild_shield_config(guild_id: int, config: Any) -> dict[str, Any]:
    cleaned = default_guild_shield_config(guild_id)
    if not isinstance(config, dict):
        return cleaned

    cleaned["module_enabled"] = bool(config.get("module_enabled"))
    baseline_version = config.get("baseline_version")
    cleaned["baseline_version"] = baseline_version if isinstance(baseline_version, int) and baseline_version >= 0 else 0
    cleaned["log_channel_id"] = config.get("log_channel_id") if isinstance(config.get("log_channel_id"), int) else None
    cleaned["alert_role_id"] = config.get("alert_role_id") if isinstance(config.get("alert_role_id"), int) else None
    log_style = str(config.get("log_style", "adaptive")).strip().lower()
    cleaned["log_style"] = log_style if log_style in VALID_SHIELD_LOG_STYLES else "adaptive"
    log_ping_mode = str(config.get("log_ping_mode", "smart")).strip().lower()
    cleaned["log_ping_mode"] = log_ping_mode if log_ping_mode in VALID_SHIELD_LOG_PING_MODES else "smart"
    scan_mode = config.get("scan_mode", "all")
    cleaned["scan_mode"] = scan_mode if scan_mode in VALID_SCAN_MODES else "all"

    for field in (
        "included_channel_ids",
        "excluded_channel_ids",
        "adult_solicitation_excluded_channel_ids",
        "included_user_ids",
        "excluded_user_ids",
        "included_role_ids",
        "excluded_role_ids",
        "trusted_role_ids",
    ):
        cleaned[field] = _clean_int_list(config.get(field))
    for field in (
        "allow_domains",
        "allow_invite_codes",
        "allow_phrases",
        "trusted_builtin_disabled_families",
        "trusted_builtin_disabled_domains",
    ):
        cleaned[field] = _clean_text_list(config.get(field))
    cleaned["pack_log_overrides"] = _clean_pack_log_overrides(config.get("pack_log_overrides"))
    cleaned["pack_exemptions"] = _clean_pack_exemptions(config.get("pack_exemptions"))
    cleaned["pack_timeout_minutes"] = _clean_pack_timeout_minutes(config.get("pack_timeout_minutes"))

    for pack in ("privacy", "promo", "scam", "spam", "gif", "adult", "severe"):
        enabled_field = f"{pack}_enabled"
        action_field = f"{pack}_action"
        low_action_field = f"{pack}_low_action"
        medium_action_field = f"{pack}_medium_action"
        high_action_field = f"{pack}_high_action"
        sensitivity_field = f"{pack}_sensitivity"
        legacy = _legacy_pack_payload(config, pack)

        if enabled_field in config:
            cleaned[enabled_field] = bool(config.get(enabled_field))
        else:
            cleaned[enabled_field] = bool(legacy.get("enabled"))

        action = str(config.get(action_field, legacy.get("action", "log"))).strip().lower()
        cleaned[action_field] = action if action in VALID_SHIELD_ACTIONS else "log"
        default_low, default_medium, default_high = _legacy_action_policy(cleaned[action_field])
        cleaned[low_action_field] = _clean_action_value(config.get(low_action_field), allowed=LOW_CONFIDENCE_ACTIONS, fallback=default_low)
        cleaned[medium_action_field] = _clean_action_value(
            config.get(medium_action_field),
            allowed=MEDIUM_CONFIDENCE_ACTIONS,
            fallback=default_medium,
        )
        cleaned[high_action_field] = _clean_action_value(config.get(high_action_field), allowed=HIGH_CONFIDENCE_ACTIONS, fallback=default_high)
        cleaned[action_field] = cleaned[high_action_field]

        sensitivity = str(config.get(sensitivity_field, legacy.get("sensitivity", "normal"))).strip().lower()
        cleaned[sensitivity_field] = sensitivity if sensitivity in VALID_SHIELD_SENSITIVITIES else "normal"
    cleaned["adult_solicitation_enabled"] = bool(config.get("adult_solicitation_enabled"))
    severe_categories = _clean_severe_category_list(config.get("severe_enabled_categories", DEFAULT_SHIELD_SEVERE_CATEGORIES))
    cleaned["severe_enabled_categories"] = (
        severe_categories if "severe_enabled_categories" in config else severe_categories or list(DEFAULT_SHIELD_SEVERE_CATEGORIES)
    )
    cleaned["severe_custom_terms"] = _clean_text_list(config.get("severe_custom_terms"))[:SHIELD_SEVERE_STORAGE_LIMIT]
    cleaned["severe_removed_terms"] = _clean_text_list(config.get("severe_removed_terms"))[:SHIELD_SEVERE_STORAGE_LIMIT]

    link_policy_mode = str(config.get("link_policy_mode", DEFAULT_SHIELD_LINK_POLICY_MODE)).strip().lower()
    cleaned["link_policy_mode"] = link_policy_mode if link_policy_mode in VALID_SHIELD_LINK_POLICY_MODES else DEFAULT_SHIELD_LINK_POLICY_MODE
    link_policy_action = str(config.get("link_policy_action", "log")).strip().lower()
    cleaned["link_policy_action"] = link_policy_action if link_policy_action in VALID_SHIELD_ACTIONS else "log"
    link_low_default, link_medium_default, link_high_default = _legacy_action_policy(cleaned["link_policy_action"])
    cleaned["link_policy_low_action"] = _clean_action_value(
        config.get("link_policy_low_action"),
        allowed=LOW_CONFIDENCE_ACTIONS,
        fallback=link_low_default,
    )
    cleaned["link_policy_medium_action"] = _clean_action_value(
        config.get("link_policy_medium_action"),
        allowed=MEDIUM_CONFIDENCE_ACTIONS,
        fallback=link_medium_default,
    )
    cleaned["link_policy_high_action"] = _clean_action_value(
        config.get("link_policy_high_action"),
        allowed=HIGH_CONFIDENCE_ACTIONS,
        fallback=link_high_default,
    )
    cleaned["link_policy_action"] = cleaned["link_policy_high_action"]

    cleaned["ai_enabled"] = bool(config.get("ai_enabled"))
    ai_access_mode = str(config.get("ai_access_mode", "inherit")).strip().lower()
    cleaned["ai_access_mode"] = ai_access_mode if ai_access_mode in VALID_SHIELD_AI_ACCESS_MODES else "inherit"
    cleaned["ai_allowed_models_override"] = _clean_model_list(config.get("ai_allowed_models_override"))
    updated_by = config.get("ai_access_updated_by")
    cleaned["ai_access_updated_by"] = updated_by if isinstance(updated_by, int) and updated_by > 0 else None
    updated_at = config.get("ai_access_updated_at")
    cleaned["ai_access_updated_at"] = updated_at if isinstance(updated_at, str) and updated_at.strip() else None
    ai_min_confidence = str(config.get("ai_min_confidence", "high")).strip().lower()
    cleaned["ai_min_confidence"] = ai_min_confidence if ai_min_confidence in SHIELD_AI_MIN_CONFIDENCE_CHOICES else "high"
    raw_ai_packs = config.get("ai_enabled_packs", list(SHIELD_AI_REVIEW_PACKS))
    if isinstance(raw_ai_packs, (list, tuple, set)):
        cleaned["ai_enabled_packs"] = sorted(
            {
                str(value).strip().lower()
                for value in raw_ai_packs
                if str(value).strip().lower() in SHIELD_AI_REVIEW_PACKS
            }
        )
    else:
        cleaned["ai_enabled_packs"] = list(SHIELD_AI_REVIEW_PACKS)

    legacy_numeric_aliases = {
        "spam_near_duplicate_threshold": "spam_duplicate_threshold",
        "spam_near_duplicate_window_seconds": "spam_duplicate_window_seconds",
    }
    for field, (minimum, maximum, default) in SHIELD_NUMERIC_CONFIG_SPECS.items():
        value = config.get(field)
        if value is None and field in legacy_numeric_aliases:
            value = config.get(legacy_numeric_aliases[field])
        cleaned[field] = value if isinstance(value, int) and minimum <= value <= maximum else default
    for field in (
        "spam_message_enabled",
        "spam_burst_enabled",
        "spam_near_duplicate_enabled",
        "gif_message_enabled",
        "gif_consecutive_enabled",
        "gif_repeat_enabled",
        "gif_same_asset_enabled",
    ):
        if field in config:
            cleaned[field] = bool(config.get(field))
        elif field == "spam_near_duplicate_enabled" and "spam_duplicate_enabled" in config:
            cleaned[field] = bool(config.get("spam_duplicate_enabled"))
        else:
            cleaned[field] = bool(cleaned[field])
    cleaned["spam_emote_enabled"] = bool(config.get("spam_emote_enabled"))
    cleaned["spam_caps_enabled"] = bool(config.get("spam_caps_enabled"))
    cleaned["spam_low_value_enabled"] = bool(config.get("spam_low_value_enabled"))
    moderator_policy = str(config.get("spam_moderator_policy", "exempt")).strip().lower()
    cleaned["spam_moderator_policy"] = (
        moderator_policy if moderator_policy in VALID_SPAM_MODERATOR_POLICIES else "exempt"
    )

    patterns = []
    for item in config.get("custom_patterns", []):
        if not isinstance(item, dict):
            continue
        pattern_id = item.get("pattern_id")
        label = item.get("label")
        pattern = item.get("pattern")
        mode = item.get("mode", "contains")
        action = item.get("action", "log")
        if not all(isinstance(value, str) and value.strip() for value in (pattern_id, label, pattern)):
            continue
        if mode not in {"contains", "word", "wildcard"}:
            continue
        if action not in VALID_SHIELD_ACTIONS - {"disabled"}:
            continue
        patterns.append(
            {
                "pattern_id": pattern_id.strip(),
                "label": label.strip(),
                "pattern": pattern.strip(),
                "mode": mode,
                "action": action,
                "enabled": bool(item.get("enabled", True)),
            }
        )
    cleaned["custom_patterns"] = patterns[:SHIELD_CUSTOM_PATTERN_STORAGE_LIMIT]
    return cleaned


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


class _BaseShieldStore:
    backend_name = "unknown"

    def __init__(self):
        self.state: dict[str, Any] = default_shield_state()

    async def load(self) -> dict[str, Any]:
        raise NotImplementedError

    async def flush(self) -> bool:
        raise NotImplementedError

    async def close(self):
        return None

    def normalize_state(self, payload: Any) -> dict[str, Any]:
        normalized = default_shield_state()
        if not isinstance(payload, dict):
            return normalized
        version = payload.get("version")
        normalized["version"] = version if isinstance(version, int) and version > 0 else DEFAULT_VERSION
        meta = payload.get("meta")
        if isinstance(meta, dict):
            cleaned_meta = default_shield_meta()
            ordinary_enabled = meta.get("ordinary_ai_enabled")
            if ordinary_enabled is None:
                ordinary_enabled = meta.get("global_ai_override_enabled")
            cleaned_meta["ordinary_ai_enabled"] = bool(ordinary_enabled)
            allowed_models = meta.get("ordinary_ai_allowed_models")
            if allowed_models is None and bool(meta.get("global_ai_override_enabled")):
                allowed_models = list(SHIELD_AI_MODEL_ORDER)
            cleaned_meta["ordinary_ai_allowed_models"] = _clean_model_list(allowed_models) or list(SHIELD_AI_MODEL_ORDER)
            updated_by = meta.get("ordinary_ai_updated_by")
            if updated_by is None:
                updated_by = meta.get("global_ai_override_updated_by")
            cleaned_meta["ordinary_ai_updated_by"] = updated_by if isinstance(updated_by, int) and updated_by > 0 else None
            updated_at = meta.get("ordinary_ai_updated_at")
            if updated_at is None:
                updated_at = meta.get("global_ai_override_updated_at")
            cleaned_meta["ordinary_ai_updated_at"] = updated_at if isinstance(updated_at, str) and updated_at.strip() else None
            normalized["meta"] = cleaned_meta
        guilds = payload.get("guilds")
        if isinstance(guilds, dict):
            cleaned_guilds: dict[str, Any] = {}
            for guild_id_text, config in guilds.items():
                try:
                    guild_id = int(guild_id_text)
                except (TypeError, ValueError):
                    continue
                cleaned = self.normalize_config(guild_id, config)
                if cleaned is not None:
                    cleaned_guilds[str(guild_id)] = cleaned
            normalized["guilds"] = cleaned_guilds
        normalized["alert_actions"] = _clean_alert_action_records(payload.get("alert_actions"))
        return normalized

    def normalize_config(self, guild_id: int, config: Any) -> dict[str, Any] | None:
        if not isinstance(config, dict):
            return None
        return normalize_guild_shield_config(guild_id, config)


class _MemoryShieldStore(_BaseShieldStore):
    backend_name = "memory"

    async def load(self) -> dict[str, Any]:
        self.state = default_shield_state()
        return self.state

    async def flush(self) -> bool:
        self.state = deepcopy(self.state)
        return True


class _PostgresShieldStore(_BaseShieldStore):
    backend_name = "postgres"

    def __init__(self, dsn: str):
        super().__init__()
        self.dsn = dsn
        self._asyncpg = None
        self._pool = None
        self._io_lock = asyncio.Lock()
        self._last_flushed_state = default_shield_state()

    async def load(self) -> dict[str, Any]:
        await self._connect()
        await self._ensure_schema()
        await self._reload_from_db()
        self._last_flushed_state = deepcopy(self.state)
        return self.state

    async def flush(self) -> bool:
        snapshot = self.normalize_state(deepcopy(self.state))
        async with self._io_lock:
            try:
                await self._flush_snapshot(snapshot)
            except Exception as exc:
                LOGGER.warning("Shield Postgres store flush failed: error_type=%s", type(exc).__name__)
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
            raise ShieldStorageUnavailable("asyncpg is not installed, so Postgres Shield storage is unavailable.") from exc
        last_error = None
        for attempt in range(1, 4):
            try:
                self._pool = await self._asyncpg.create_pool(
                    dsn=self.dsn,
                    min_size=1,
                    max_size=3,
                    command_timeout=30,
                    max_inactive_connection_lifetime=60,
                    server_settings={"application_name": "babblebox-shield-store"},
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt < 3:
                    await asyncio.sleep(0.5 * (2 ** (attempt - 1)))
        raise ShieldStorageUnavailable(f"Could not connect to Postgres Shield storage: {last_error}") from last_error

    async def _ensure_schema(self):
        statements = [
            (
                "CREATE TABLE IF NOT EXISTS shield_guild_configs ("
                "guild_id BIGINT PRIMARY KEY, "
                "module_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "baseline_version SMALLINT NOT NULL DEFAULT 0, "
                "log_channel_id BIGINT NULL, "
                "alert_role_id BIGINT NULL, "
                "log_style TEXT NOT NULL DEFAULT 'adaptive', "
                "log_ping_mode TEXT NOT NULL DEFAULT 'smart', "
                "scan_mode TEXT NOT NULL DEFAULT 'all', "
                "included_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "included_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "included_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "excluded_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "trusted_role_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_invite_codes JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "allow_phrases JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "trusted_builtin_disabled_families JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "trusted_builtin_disabled_domains JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "pack_log_overrides JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "pack_exemptions JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "pack_timeout_minutes JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "privacy_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "privacy_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_low_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_medium_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_high_action TEXT NOT NULL DEFAULT 'log', "
                "privacy_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "promo_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "promo_action TEXT NOT NULL DEFAULT 'log', "
                "promo_low_action TEXT NOT NULL DEFAULT 'log', "
                "promo_medium_action TEXT NOT NULL DEFAULT 'log', "
                "promo_high_action TEXT NOT NULL DEFAULT 'log', "
                "promo_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "scam_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "scam_action TEXT NOT NULL DEFAULT 'log', "
                "scam_low_action TEXT NOT NULL DEFAULT 'log', "
                "scam_medium_action TEXT NOT NULL DEFAULT 'log', "
                "scam_high_action TEXT NOT NULL DEFAULT 'log', "
                "scam_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "spam_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "spam_action TEXT NOT NULL DEFAULT 'log', "
                "spam_low_action TEXT NOT NULL DEFAULT 'log', "
                "spam_medium_action TEXT NOT NULL DEFAULT 'log', "
                "spam_high_action TEXT NOT NULL DEFAULT 'log', "
                "spam_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "spam_message_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"spam_message_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_message_threshold')}, "
                f"spam_message_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_message_window_seconds')}, "
                "spam_burst_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"spam_burst_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_burst_threshold')}, "
                f"spam_burst_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_burst_window_seconds')}, "
                "spam_near_duplicate_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"spam_near_duplicate_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_near_duplicate_threshold')}, "
                f"spam_near_duplicate_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_near_duplicate_window_seconds')}, "
                "spam_emote_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                f"spam_emote_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_emote_threshold')}, "
                "spam_caps_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                f"spam_caps_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_caps_threshold')}, "
                "spam_low_value_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                f"spam_low_value_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_low_value_threshold')}, "
                f"spam_low_value_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_low_value_window_seconds')}, "
                "spam_moderator_policy TEXT NOT NULL DEFAULT 'exempt', "
                "gif_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "gif_action TEXT NOT NULL DEFAULT 'log', "
                "gif_low_action TEXT NOT NULL DEFAULT 'log', "
                "gif_medium_action TEXT NOT NULL DEFAULT 'log', "
                "gif_high_action TEXT NOT NULL DEFAULT 'log', "
                "gif_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "gif_message_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"gif_message_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_message_threshold')}, "
                f"gif_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_window_seconds')}, "
                "gif_consecutive_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"gif_consecutive_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_consecutive_threshold')}, "
                "gif_repeat_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"gif_repeat_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_repeat_threshold')}, "
                "gif_same_asset_enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                f"gif_same_asset_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_same_asset_threshold')}, "
                f"gif_min_ratio_percent SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_min_ratio_percent')}, "
                "adult_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "adult_action TEXT NOT NULL DEFAULT 'log', "
                "adult_low_action TEXT NOT NULL DEFAULT 'log', "
                "adult_medium_action TEXT NOT NULL DEFAULT 'log', "
                "adult_high_action TEXT NOT NULL DEFAULT 'log', "
                "adult_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "adult_solicitation_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "adult_solicitation_excluded_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "severe_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "severe_action TEXT NOT NULL DEFAULT 'log', "
                "severe_low_action TEXT NOT NULL DEFAULT 'log', "
                "severe_medium_action TEXT NOT NULL DEFAULT 'log', "
                "severe_high_action TEXT NOT NULL DEFAULT 'log', "
                "severe_sensitivity TEXT NOT NULL DEFAULT 'normal', "
                "severe_enabled_categories JSONB NOT NULL DEFAULT '[\"sexual_exploitation\",\"self_harm_encouragement\",\"eliminationist_hate\",\"severe_slur_abuse\"]'::jsonb, "
                "severe_custom_terms JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "severe_removed_terms JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "link_policy_mode TEXT NOT NULL DEFAULT 'default', "
                "link_policy_action TEXT NOT NULL DEFAULT 'log', "
                "link_policy_low_action TEXT NOT NULL DEFAULT 'log', "
                "link_policy_medium_action TEXT NOT NULL DEFAULT 'log', "
                "link_policy_high_action TEXT NOT NULL DEFAULT 'log', "
                "ai_enabled BOOLEAN NOT NULL DEFAULT FALSE, "
                "ai_access_mode TEXT NOT NULL DEFAULT 'inherit', "
                "ai_allowed_models_override JSONB NOT NULL DEFAULT '[]'::jsonb, "
                "ai_access_updated_by BIGINT NULL, "
                "ai_access_updated_at TEXT NULL, "
                "ai_min_confidence TEXT NOT NULL DEFAULT 'high', "
                "ai_enabled_packs JSONB NOT NULL DEFAULT '[\"privacy\",\"promo\",\"scam\",\"adult\",\"severe\"]'::jsonb, "
                "escalation_threshold SMALLINT NOT NULL DEFAULT 3, "
                "escalation_window_minutes SMALLINT NOT NULL DEFAULT 15, "
                "timeout_minutes SMALLINT NOT NULL DEFAULT 10, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS shield_meta ("
                "key TEXT PRIMARY KEY, "
                "value JSONB NOT NULL DEFAULT '{}'::jsonb, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            (
                "CREATE TABLE IF NOT EXISTS shield_alert_action_records ("
                "token TEXT PRIMARY KEY, "
                "payload JSONB NOT NULL, "
                "expires_at TIMESTAMPTZ NOT NULL, "
                "alert_message_id BIGINT NULL, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_shield_alert_action_records_expires ON shield_alert_action_records (expires_at)",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS baseline_version SMALLINT NOT NULL DEFAULT 0",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS log_style TEXT NOT NULL DEFAULT 'adaptive'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS log_ping_mode TEXT NOT NULL DEFAULT 'smart'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS privacy_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS promo_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS scam_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_solicitation_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS adult_solicitation_excluded_channel_ids JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_sensitivity TEXT NOT NULL DEFAULT 'normal'",
            (
                "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_enabled_categories JSONB "
                "NOT NULL DEFAULT '[\"sexual_exploitation\",\"self_harm_encouragement\",\"eliminationist_hate\",\"severe_slur_abuse\"]'::jsonb"
            ),
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_custom_terms JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS severe_removed_terms JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS link_policy_mode TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS link_policy_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS link_policy_low_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS link_policy_medium_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS link_policy_high_action TEXT NOT NULL DEFAULT 'log'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS trusted_builtin_disabled_families JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS trusted_builtin_disabled_domains JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS pack_log_overrides JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS pack_exemptions JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS pack_timeout_minutes JSONB NOT NULL DEFAULT '{}'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_message_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_message_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_message_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_message_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_message_window_seconds')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_burst_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_burst_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_burst_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_burst_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_burst_window_seconds')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_near_duplicate_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_near_duplicate_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_near_duplicate_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_near_duplicate_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_near_duplicate_window_seconds')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_emote_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_emote_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_emote_threshold')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_caps_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_caps_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_caps_threshold')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_low_value_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_low_value_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_low_value_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_low_value_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('spam_low_value_window_seconds')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS spam_moderator_policy TEXT NOT NULL DEFAULT 'exempt'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_message_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_message_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_message_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_window_seconds SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_window_seconds')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_consecutive_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_consecutive_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_consecutive_threshold')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_repeat_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_repeat_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_repeat_threshold')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_same_asset_enabled BOOLEAN NOT NULL DEFAULT TRUE",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_same_asset_threshold SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_same_asset_threshold')}",
            f"ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS gif_min_ratio_percent SMALLINT NOT NULL DEFAULT {shield_numeric_config_default('gif_min_ratio_percent')}",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_enabled BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_access_mode TEXT NOT NULL DEFAULT 'inherit'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_allowed_models_override JSONB NOT NULL DEFAULT '[]'::jsonb",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_access_updated_by BIGINT NULL",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_access_updated_at TEXT NULL",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_min_confidence TEXT NOT NULL DEFAULT 'high'",
            "ALTER TABLE shield_guild_configs ADD COLUMN IF NOT EXISTS ai_enabled_packs JSONB NOT NULL DEFAULT '[\"privacy\",\"promo\",\"scam\",\"adult\",\"severe\"]'::jsonb",
            (
                "CREATE TABLE IF NOT EXISTS shield_custom_patterns ("
                "pattern_id TEXT PRIMARY KEY, "
                "guild_id BIGINT NOT NULL REFERENCES shield_guild_configs(guild_id) ON DELETE CASCADE, "
                "label TEXT NOT NULL, "
                "pattern TEXT NOT NULL, "
                "mode TEXT NOT NULL, "
                "action TEXT NOT NULL, "
                "enabled BOOLEAN NOT NULL DEFAULT TRUE, "
                "created_at TIMESTAMPTZ NOT NULL DEFAULT timezone('utc', now())"
                ")"
            ),
            "CREATE INDEX IF NOT EXISTS ix_shield_custom_patterns_guild ON shield_custom_patterns (guild_id)",
        ]
        async with self._pool.acquire() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def _reload_from_db(self):
        loaded = default_shield_state()
        async with self._pool.acquire() as conn:
            config_rows = await conn.fetch("SELECT * FROM shield_guild_configs")
            pattern_rows = await conn.fetch("SELECT pattern_id, guild_id, label, pattern, mode, action, enabled FROM shield_custom_patterns ORDER BY created_at ASC")
            meta_rows = await conn.fetch("SELECT key, value FROM shield_meta")
            alert_rows = await conn.fetch("SELECT token, payload FROM shield_alert_action_records WHERE expires_at > timezone('utc', now())")
        for row in config_rows:
            guild_id = int(row["guild_id"])
            loaded["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "module_enabled": bool(row["module_enabled"]),
                "baseline_version": int(row["baseline_version"]) if "baseline_version" in row else 0,
                "log_channel_id": row["log_channel_id"],
                "alert_role_id": row["alert_role_id"],
                "log_style": row["log_style"] if "log_style" in row else "adaptive",
                "log_ping_mode": row["log_ping_mode"] if "log_ping_mode" in row else "smart",
                "scan_mode": row["scan_mode"],
                "included_channel_ids": decode_postgres_json_array(
                    row["included_channel_ids"],
                    label="shield_guild_configs.included_channel_ids",
                ),
                "excluded_channel_ids": decode_postgres_json_array(
                    row["excluded_channel_ids"],
                    label="shield_guild_configs.excluded_channel_ids",
                ),
                "included_user_ids": decode_postgres_json_array(
                    row["included_user_ids"],
                    label="shield_guild_configs.included_user_ids",
                ),
                "excluded_user_ids": decode_postgres_json_array(
                    row["excluded_user_ids"],
                    label="shield_guild_configs.excluded_user_ids",
                ),
                "included_role_ids": decode_postgres_json_array(
                    row["included_role_ids"],
                    label="shield_guild_configs.included_role_ids",
                ),
                "excluded_role_ids": decode_postgres_json_array(
                    row["excluded_role_ids"],
                    label="shield_guild_configs.excluded_role_ids",
                ),
                "trusted_role_ids": decode_postgres_json_array(
                    row["trusted_role_ids"],
                    label="shield_guild_configs.trusted_role_ids",
                ),
                "allow_domains": decode_postgres_json_array(
                    row["allow_domains"],
                    label="shield_guild_configs.allow_domains",
                ),
                "allow_invite_codes": decode_postgres_json_array(
                    row["allow_invite_codes"],
                    label="shield_guild_configs.allow_invite_codes",
                ),
                "allow_phrases": decode_postgres_json_array(
                    row["allow_phrases"],
                    label="shield_guild_configs.allow_phrases",
                ),
                "trusted_builtin_disabled_families": decode_postgres_json_array(
                    row["trusted_builtin_disabled_families"],
                    label="shield_guild_configs.trusted_builtin_disabled_families",
                )
                if "trusted_builtin_disabled_families" in row
                else [],
                "trusted_builtin_disabled_domains": decode_postgres_json_array(
                    row["trusted_builtin_disabled_domains"],
                    label="shield_guild_configs.trusted_builtin_disabled_domains",
                )
                if "trusted_builtin_disabled_domains" in row
                else [],
                "pack_log_overrides": decode_postgres_json_object(
                    row["pack_log_overrides"],
                    label="shield_guild_configs.pack_log_overrides",
                )
                if "pack_log_overrides" in row
                else _default_pack_log_overrides(),
                "pack_exemptions": decode_postgres_json_object(
                    row["pack_exemptions"],
                    label="shield_guild_configs.pack_exemptions",
                )
                if "pack_exemptions" in row
                else _default_pack_exemptions(),
                "pack_timeout_minutes": decode_postgres_json_object(
                    row["pack_timeout_minutes"],
                    label="shield_guild_configs.pack_timeout_minutes",
                )
                if "pack_timeout_minutes" in row
                else _default_pack_timeout_minutes(),
                "privacy_enabled": bool(row["privacy_enabled"]),
                "privacy_action": row["privacy_action"],
                "privacy_low_action": row["privacy_low_action"],
                "privacy_medium_action": row["privacy_medium_action"],
                "privacy_high_action": row["privacy_high_action"],
                "privacy_sensitivity": row["privacy_sensitivity"],
                "promo_enabled": bool(row["promo_enabled"]),
                "promo_action": row["promo_action"],
                "promo_low_action": row["promo_low_action"],
                "promo_medium_action": row["promo_medium_action"],
                "promo_high_action": row["promo_high_action"],
                "promo_sensitivity": row["promo_sensitivity"],
                "scam_enabled": bool(row["scam_enabled"]),
                "scam_action": row["scam_action"],
                "scam_low_action": row["scam_low_action"],
                "scam_medium_action": row["scam_medium_action"],
                "scam_high_action": row["scam_high_action"],
                "scam_sensitivity": row["scam_sensitivity"],
                "spam_enabled": bool(row["spam_enabled"]) if "spam_enabled" in row else False,
                "spam_action": row["spam_action"] if "spam_action" in row else "log",
                "spam_low_action": row["spam_low_action"] if "spam_low_action" in row else "log",
                "spam_medium_action": row["spam_medium_action"] if "spam_medium_action" in row else "log",
                "spam_high_action": row["spam_high_action"] if "spam_high_action" in row else "log",
                "spam_sensitivity": row["spam_sensitivity"] if "spam_sensitivity" in row else "normal",
                "spam_message_enabled": bool(row["spam_message_enabled"]) if "spam_message_enabled" in row else True,
                "spam_message_threshold": int(row["spam_message_threshold"])
                if "spam_message_threshold" in row
                else shield_numeric_config_default("spam_message_threshold"),
                "spam_message_window_seconds": int(row["spam_message_window_seconds"])
                if "spam_message_window_seconds" in row
                else shield_numeric_config_default("spam_message_window_seconds"),
                "spam_burst_enabled": bool(row["spam_burst_enabled"]) if "spam_burst_enabled" in row else True,
                "spam_burst_threshold": int(row["spam_burst_threshold"])
                if "spam_burst_threshold" in row
                else shield_numeric_config_default("spam_burst_threshold"),
                "spam_burst_window_seconds": int(row["spam_burst_window_seconds"])
                if "spam_burst_window_seconds" in row
                else shield_numeric_config_default("spam_burst_window_seconds"),
                "spam_near_duplicate_enabled": (
                    bool(row["spam_near_duplicate_enabled"])
                    if "spam_near_duplicate_enabled" in row
                    else bool(row["spam_duplicate_enabled"])
                    if "spam_duplicate_enabled" in row
                    else True
                ),
                "spam_near_duplicate_threshold": int(row["spam_near_duplicate_threshold"])
                if "spam_near_duplicate_threshold" in row
                else int(row["spam_duplicate_threshold"])
                if "spam_duplicate_threshold" in row
                else shield_numeric_config_default("spam_near_duplicate_threshold"),
                "spam_near_duplicate_window_seconds": int(row["spam_near_duplicate_window_seconds"])
                if "spam_near_duplicate_window_seconds" in row
                else int(row["spam_duplicate_window_seconds"])
                if "spam_duplicate_window_seconds" in row
                else shield_numeric_config_default("spam_near_duplicate_window_seconds"),
                "spam_emote_enabled": bool(row["spam_emote_enabled"]) if "spam_emote_enabled" in row else False,
                "spam_emote_threshold": int(row["spam_emote_threshold"])
                if "spam_emote_threshold" in row
                else shield_numeric_config_default("spam_emote_threshold"),
                "spam_caps_enabled": bool(row["spam_caps_enabled"]) if "spam_caps_enabled" in row else False,
                "spam_caps_threshold": int(row["spam_caps_threshold"])
                if "spam_caps_threshold" in row
                else shield_numeric_config_default("spam_caps_threshold"),
                "spam_low_value_enabled": bool(row["spam_low_value_enabled"]) if "spam_low_value_enabled" in row else False,
                "spam_low_value_threshold": int(row["spam_low_value_threshold"])
                if "spam_low_value_threshold" in row
                else shield_numeric_config_default("spam_low_value_threshold"),
                "spam_low_value_window_seconds": int(row["spam_low_value_window_seconds"])
                if "spam_low_value_window_seconds" in row
                else shield_numeric_config_default("spam_low_value_window_seconds"),
                "spam_moderator_policy": row["spam_moderator_policy"] if "spam_moderator_policy" in row else "exempt",
                "gif_enabled": bool(row["gif_enabled"]) if "gif_enabled" in row else False,
                "gif_action": row["gif_action"] if "gif_action" in row else "log",
                "gif_low_action": row["gif_low_action"] if "gif_low_action" in row else "log",
                "gif_medium_action": row["gif_medium_action"] if "gif_medium_action" in row else "log",
                "gif_high_action": row["gif_high_action"] if "gif_high_action" in row else "log",
                "gif_sensitivity": row["gif_sensitivity"] if "gif_sensitivity" in row else "normal",
                "gif_message_enabled": bool(row["gif_message_enabled"]) if "gif_message_enabled" in row else True,
                "gif_message_threshold": int(row["gif_message_threshold"])
                if "gif_message_threshold" in row
                else shield_numeric_config_default("gif_message_threshold"),
                "gif_window_seconds": int(row["gif_window_seconds"])
                if "gif_window_seconds" in row
                else shield_numeric_config_default("gif_window_seconds"),
                "gif_consecutive_enabled": bool(row["gif_consecutive_enabled"]) if "gif_consecutive_enabled" in row else True,
                "gif_consecutive_threshold": int(row["gif_consecutive_threshold"])
                if "gif_consecutive_threshold" in row
                else shield_numeric_config_default("gif_consecutive_threshold"),
                "gif_repeat_enabled": bool(row["gif_repeat_enabled"]) if "gif_repeat_enabled" in row else True,
                "gif_repeat_threshold": int(row["gif_repeat_threshold"])
                if "gif_repeat_threshold" in row
                else shield_numeric_config_default("gif_repeat_threshold"),
                "gif_same_asset_enabled": bool(row["gif_same_asset_enabled"]) if "gif_same_asset_enabled" in row else True,
                "gif_same_asset_threshold": int(row["gif_same_asset_threshold"])
                if "gif_same_asset_threshold" in row
                else shield_numeric_config_default("gif_same_asset_threshold"),
                "gif_min_ratio_percent": int(row["gif_min_ratio_percent"])
                if "gif_min_ratio_percent" in row
                else shield_numeric_config_default("gif_min_ratio_percent"),
                "adult_enabled": bool(row["adult_enabled"]) if "adult_enabled" in row else False,
                "adult_action": row["adult_action"] if "adult_action" in row else "log",
                "adult_low_action": row["adult_low_action"] if "adult_low_action" in row else "log",
                "adult_medium_action": row["adult_medium_action"] if "adult_medium_action" in row else "log",
                "adult_high_action": row["adult_high_action"] if "adult_high_action" in row else "log",
                "adult_sensitivity": row["adult_sensitivity"] if "adult_sensitivity" in row else "normal",
                "adult_solicitation_enabled": bool(row["adult_solicitation_enabled"]) if "adult_solicitation_enabled" in row else False,
                "adult_solicitation_excluded_channel_ids": decode_postgres_json_array(
                    row["adult_solicitation_excluded_channel_ids"],
                    label="shield_guild_configs.adult_solicitation_excluded_channel_ids",
                )
                if "adult_solicitation_excluded_channel_ids" in row
                else [],
                "severe_enabled": bool(row["severe_enabled"]) if "severe_enabled" in row else False,
                "severe_action": row["severe_action"] if "severe_action" in row else "log",
                "severe_low_action": row["severe_low_action"] if "severe_low_action" in row else "log",
                "severe_medium_action": row["severe_medium_action"] if "severe_medium_action" in row else "log",
                "severe_high_action": row["severe_high_action"] if "severe_high_action" in row else "log",
                "severe_sensitivity": row["severe_sensitivity"] if "severe_sensitivity" in row else "normal",
                "severe_enabled_categories": decode_postgres_json_array(
                    row["severe_enabled_categories"],
                    label="shield_guild_configs.severe_enabled_categories",
                )
                if "severe_enabled_categories" in row
                else list(DEFAULT_SHIELD_SEVERE_CATEGORIES),
                "severe_custom_terms": decode_postgres_json_array(
                    row["severe_custom_terms"],
                    label="shield_guild_configs.severe_custom_terms",
                )
                if "severe_custom_terms" in row
                else [],
                "severe_removed_terms": decode_postgres_json_array(
                    row["severe_removed_terms"],
                    label="shield_guild_configs.severe_removed_terms",
                )
                if "severe_removed_terms" in row
                else [],
                "link_policy_mode": row["link_policy_mode"] if "link_policy_mode" in row else DEFAULT_SHIELD_LINK_POLICY_MODE,
                "link_policy_action": row["link_policy_action"] if "link_policy_action" in row else "log",
                "link_policy_low_action": row["link_policy_low_action"] if "link_policy_low_action" in row else "log",
                "link_policy_medium_action": row["link_policy_medium_action"] if "link_policy_medium_action" in row else "log",
                "link_policy_high_action": row["link_policy_high_action"] if "link_policy_high_action" in row else "log",
                "ai_enabled": bool(row["ai_enabled"]),
                "ai_access_mode": row["ai_access_mode"] if "ai_access_mode" in row else "inherit",
                "ai_allowed_models_override": decode_postgres_json_array(
                    row["ai_allowed_models_override"],
                    label="shield_guild_configs.ai_allowed_models_override",
                )
                if "ai_allowed_models_override" in row
                else [],
                "ai_access_updated_by": int(row["ai_access_updated_by"])
                if "ai_access_updated_by" in row and row["ai_access_updated_by"] is not None
                else None,
                "ai_access_updated_at": row["ai_access_updated_at"] if "ai_access_updated_at" in row else None,
                "ai_min_confidence": row["ai_min_confidence"],
                "ai_enabled_packs": decode_postgres_json_array(
                    row["ai_enabled_packs"],
                    label="shield_guild_configs.ai_enabled_packs",
                ),
                "escalation_threshold": int(row["escalation_threshold"]),
                "escalation_window_minutes": int(row["escalation_window_minutes"]),
                "timeout_minutes": int(row["timeout_minutes"]),
                "custom_patterns": [],
            }
        for row in pattern_rows:
            guild = loaded["guilds"].setdefault(str(int(row["guild_id"])), default_guild_shield_config(int(row["guild_id"])))
            guild["custom_patterns"].append(
                {
                    "pattern_id": row["pattern_id"],
                    "label": row["label"],
                    "pattern": row["pattern"],
                    "mode": row["mode"],
                    "action": row["action"],
                    "enabled": bool(row["enabled"]),
                }
            )
        for row in meta_rows:
            value = decode_postgres_json_object(
                row["value"],
                label="shield_meta.value",
            )
            if row["key"] == SHIELD_META_ORDINARY_AI_POLICY_KEY:
                loaded["meta"] = {
                    "ordinary_ai_enabled": bool(value.get("enabled")),
                    "ordinary_ai_allowed_models": value.get("allowed_models") if isinstance(value.get("allowed_models"), list) else [],
                    "ordinary_ai_updated_by": value.get("updated_by") if isinstance(value.get("updated_by"), int) and value.get("updated_by") > 0 else None,
                    "ordinary_ai_updated_at": value.get("updated_at") if isinstance(value.get("updated_at"), str) and value.get("updated_at").strip() else None,
                }
                continue
            if row["key"] == SHIELD_META_GLOBAL_AI_OVERRIDE_KEY:
                loaded["meta"] = {
                    "ordinary_ai_enabled": bool(value.get("enabled")),
                    "ordinary_ai_allowed_models": list(SHIELD_AI_MODEL_ORDER),
                    "ordinary_ai_updated_by": value.get("updated_by") if isinstance(value.get("updated_by"), int) and value.get("updated_by") > 0 else None,
                    "ordinary_ai_updated_at": value.get("updated_at") if isinstance(value.get("updated_at"), str) and value.get("updated_at").strip() else None,
                }
        for row in alert_rows:
            token = str(row["token"])
            payload = decode_postgres_json_object(
                row["payload"],
                label="shield_alert_action_records.payload",
            )
            if isinstance(payload, dict):
                loaded["alert_actions"][token] = payload
        self.state = self.normalize_state(loaded)

    async def _flush_snapshot(self, snapshot: dict[str, Any]):
        previous = self._last_flushed_state
        current_guilds = snapshot.get("guilds", {})
        previous_guilds = previous.get("guilds", {})
        removed_guild_ids = sorted(
            int(guild_id_text)
            for guild_id_text in previous_guilds
            if guild_id_text not in current_guilds
        )
        changed_configs = [
            config
            for guild_id_text, config in current_guilds.items()
            if previous_guilds.get(guild_id_text) != config
        ]
        meta_changed = snapshot.get("meta") != previous.get("meta")
        alert_actions_changed = snapshot.get("alert_actions") != previous.get("alert_actions")
        if not removed_guild_ids and not changed_configs and not meta_changed and not alert_actions_changed:
            return
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if meta_changed:
                    await conn.execute(
                        (
                            "INSERT INTO shield_meta (key, value, updated_at) VALUES ($1, $2::jsonb, timezone('utc', now())) "
                            "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"
                        ),
                        SHIELD_META_ORDINARY_AI_POLICY_KEY,
                        json.dumps(
                            {
                                "enabled": bool(snapshot["meta"]["ordinary_ai_enabled"]),
                                "allowed_models": snapshot["meta"]["ordinary_ai_allowed_models"],
                                "updated_by": snapshot["meta"]["ordinary_ai_updated_by"],
                                "updated_at": snapshot["meta"]["ordinary_ai_updated_at"],
                            }
                        ),
                    )
                    await conn.execute("DELETE FROM shield_meta WHERE key = $1", SHIELD_META_GLOBAL_AI_OVERRIDE_KEY)
                for guild_id in removed_guild_ids:
                    await conn.execute("DELETE FROM shield_guild_configs WHERE guild_id = $1", guild_id)
                for config in changed_configs:
                    await self._upsert_guild_config(conn, config)
                    await self._replace_custom_patterns_for_guild(conn, config["guild_id"], config.get("custom_patterns", []))
                if alert_actions_changed:
                    await self._replace_alert_action_records(conn, snapshot.get("alert_actions", {}))

    async def _upsert_guild_config(self, conn, config: dict[str, Any]):
        columns: list[tuple[str, Any, str]] = [
            ("guild_id", config["guild_id"], ""),
            ("module_enabled", config["module_enabled"], ""),
            ("baseline_version", config["baseline_version"], ""),
            ("log_channel_id", config["log_channel_id"], ""),
            ("alert_role_id", config["alert_role_id"], ""),
            ("log_style", config["log_style"], ""),
            ("log_ping_mode", config["log_ping_mode"], ""),
            ("scan_mode", config["scan_mode"], ""),
            ("included_channel_ids", json.dumps(config["included_channel_ids"]), "::jsonb"),
            ("excluded_channel_ids", json.dumps(config["excluded_channel_ids"]), "::jsonb"),
            ("included_user_ids", json.dumps(config["included_user_ids"]), "::jsonb"),
            ("excluded_user_ids", json.dumps(config["excluded_user_ids"]), "::jsonb"),
            ("included_role_ids", json.dumps(config["included_role_ids"]), "::jsonb"),
            ("excluded_role_ids", json.dumps(config["excluded_role_ids"]), "::jsonb"),
            ("trusted_role_ids", json.dumps(config["trusted_role_ids"]), "::jsonb"),
            ("allow_domains", json.dumps(config["allow_domains"]), "::jsonb"),
            ("allow_invite_codes", json.dumps(config["allow_invite_codes"]), "::jsonb"),
            ("allow_phrases", json.dumps(config["allow_phrases"]), "::jsonb"),
            ("trusted_builtin_disabled_families", json.dumps(config["trusted_builtin_disabled_families"]), "::jsonb"),
            ("trusted_builtin_disabled_domains", json.dumps(config["trusted_builtin_disabled_domains"]), "::jsonb"),
            ("pack_log_overrides", json.dumps(config["pack_log_overrides"]), "::jsonb"),
            ("pack_exemptions", json.dumps(config["pack_exemptions"]), "::jsonb"),
            ("pack_timeout_minutes", json.dumps(config["pack_timeout_minutes"]), "::jsonb"),
            ("privacy_enabled", config["privacy_enabled"], ""),
            ("privacy_action", config["privacy_action"], ""),
            ("privacy_low_action", config["privacy_low_action"], ""),
            ("privacy_medium_action", config["privacy_medium_action"], ""),
            ("privacy_high_action", config["privacy_high_action"], ""),
            ("privacy_sensitivity", config["privacy_sensitivity"], ""),
            ("promo_enabled", config["promo_enabled"], ""),
            ("promo_action", config["promo_action"], ""),
            ("promo_low_action", config["promo_low_action"], ""),
            ("promo_medium_action", config["promo_medium_action"], ""),
            ("promo_high_action", config["promo_high_action"], ""),
            ("promo_sensitivity", config["promo_sensitivity"], ""),
            ("scam_enabled", config["scam_enabled"], ""),
            ("scam_action", config["scam_action"], ""),
            ("scam_low_action", config["scam_low_action"], ""),
            ("scam_medium_action", config["scam_medium_action"], ""),
            ("scam_high_action", config["scam_high_action"], ""),
            ("scam_sensitivity", config["scam_sensitivity"], ""),
            ("spam_enabled", config["spam_enabled"], ""),
            ("spam_action", config["spam_action"], ""),
            ("spam_low_action", config["spam_low_action"], ""),
            ("spam_medium_action", config["spam_medium_action"], ""),
            ("spam_high_action", config["spam_high_action"], ""),
            ("spam_sensitivity", config["spam_sensitivity"], ""),
            ("spam_message_enabled", config["spam_message_enabled"], ""),
            ("spam_message_threshold", config["spam_message_threshold"], ""),
            ("spam_message_window_seconds", config["spam_message_window_seconds"], ""),
            ("spam_burst_enabled", config["spam_burst_enabled"], ""),
            ("spam_burst_threshold", config["spam_burst_threshold"], ""),
            ("spam_burst_window_seconds", config["spam_burst_window_seconds"], ""),
            ("spam_near_duplicate_enabled", config["spam_near_duplicate_enabled"], ""),
            ("spam_near_duplicate_threshold", config["spam_near_duplicate_threshold"], ""),
            ("spam_near_duplicate_window_seconds", config["spam_near_duplicate_window_seconds"], ""),
            ("spam_emote_enabled", config["spam_emote_enabled"], ""),
            ("spam_emote_threshold", config["spam_emote_threshold"], ""),
            ("spam_caps_enabled", config["spam_caps_enabled"], ""),
            ("spam_caps_threshold", config["spam_caps_threshold"], ""),
            ("spam_low_value_enabled", config["spam_low_value_enabled"], ""),
            ("spam_low_value_threshold", config["spam_low_value_threshold"], ""),
            ("spam_low_value_window_seconds", config["spam_low_value_window_seconds"], ""),
            ("spam_moderator_policy", config["spam_moderator_policy"], ""),
            ("gif_enabled", config["gif_enabled"], ""),
            ("gif_action", config["gif_action"], ""),
            ("gif_low_action", config["gif_low_action"], ""),
            ("gif_medium_action", config["gif_medium_action"], ""),
            ("gif_high_action", config["gif_high_action"], ""),
            ("gif_sensitivity", config["gif_sensitivity"], ""),
            ("gif_message_enabled", config["gif_message_enabled"], ""),
            ("gif_message_threshold", config["gif_message_threshold"], ""),
            ("gif_window_seconds", config["gif_window_seconds"], ""),
            ("gif_consecutive_enabled", config["gif_consecutive_enabled"], ""),
            ("gif_consecutive_threshold", config["gif_consecutive_threshold"], ""),
            ("gif_repeat_enabled", config["gif_repeat_enabled"], ""),
            ("gif_repeat_threshold", config["gif_repeat_threshold"], ""),
            ("gif_same_asset_enabled", config["gif_same_asset_enabled"], ""),
            ("gif_same_asset_threshold", config["gif_same_asset_threshold"], ""),
            ("gif_min_ratio_percent", config["gif_min_ratio_percent"], ""),
            ("adult_enabled", config["adult_enabled"], ""),
            ("adult_action", config["adult_action"], ""),
            ("adult_low_action", config["adult_low_action"], ""),
            ("adult_medium_action", config["adult_medium_action"], ""),
            ("adult_high_action", config["adult_high_action"], ""),
            ("adult_sensitivity", config["adult_sensitivity"], ""),
            ("adult_solicitation_enabled", config["adult_solicitation_enabled"], ""),
            ("adult_solicitation_excluded_channel_ids", json.dumps(config["adult_solicitation_excluded_channel_ids"]), "::jsonb"),
            ("severe_enabled", config["severe_enabled"], ""),
            ("severe_action", config["severe_action"], ""),
            ("severe_low_action", config["severe_low_action"], ""),
            ("severe_medium_action", config["severe_medium_action"], ""),
            ("severe_high_action", config["severe_high_action"], ""),
            ("severe_sensitivity", config["severe_sensitivity"], ""),
            ("severe_enabled_categories", json.dumps(config["severe_enabled_categories"]), "::jsonb"),
            ("severe_custom_terms", json.dumps(config["severe_custom_terms"]), "::jsonb"),
            ("severe_removed_terms", json.dumps(config["severe_removed_terms"]), "::jsonb"),
            ("link_policy_mode", config["link_policy_mode"], ""),
            ("link_policy_action", config["link_policy_action"], ""),
            ("link_policy_low_action", config["link_policy_low_action"], ""),
            ("link_policy_medium_action", config["link_policy_medium_action"], ""),
            ("link_policy_high_action", config["link_policy_high_action"], ""),
            ("ai_enabled", config["ai_enabled"], ""),
            ("ai_access_mode", config["ai_access_mode"], ""),
            ("ai_allowed_models_override", json.dumps(config["ai_allowed_models_override"]), "::jsonb"),
            ("ai_access_updated_by", config["ai_access_updated_by"], ""),
            ("ai_access_updated_at", config["ai_access_updated_at"], ""),
            ("ai_min_confidence", config["ai_min_confidence"], ""),
            ("ai_enabled_packs", json.dumps(config["ai_enabled_packs"]), "::jsonb"),
            ("escalation_threshold", config["escalation_threshold"], ""),
            ("escalation_window_minutes", config["escalation_window_minutes"], ""),
            ("timeout_minutes", config["timeout_minutes"], ""),
        ]
        column_names = ", ".join(name for name, _value, _cast in columns)
        placeholders = ", ".join(f"${index}{cast}" for index, (_name, _value, cast) in enumerate(columns, start=1))
        update_assignments = ", ".join(f"{name} = EXCLUDED.{name}" for name, _value, _cast in columns if name != "guild_id")
        sql = (
            f"INSERT INTO shield_guild_configs ({column_names}, updated_at) "
            f"VALUES ({placeholders}, timezone('utc', now())) "
            "ON CONFLICT (guild_id) DO UPDATE SET "
            f"{update_assignments}, updated_at = EXCLUDED.updated_at"
        )
        await conn.execute(sql, *(value for _name, value, _cast in columns))

    async def _replace_custom_patterns_for_guild(self, conn, guild_id: int, patterns: list[dict[str, Any]]):
        await conn.execute("DELETE FROM shield_custom_patterns WHERE guild_id = $1", guild_id)
        rows = [
            (
                item["pattern_id"],
                guild_id,
                item["label"],
                item["pattern"],
                item["mode"],
                item["action"],
                item["enabled"],
            )
            for item in patterns
            if isinstance(item, dict)
        ]
        if rows:
            await conn.executemany(
                "INSERT INTO shield_custom_patterns (pattern_id, guild_id, label, pattern, mode, action, enabled) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                rows,
            )

    async def _replace_alert_action_records(self, conn, records: dict[str, dict[str, Any]]):
        await conn.execute("DELETE FROM shield_alert_action_records")
        rows = [
            (
                token,
                json.dumps(record),
                record.get("expires_at"),
                record.get("alert_message_id"),
            )
            for token, record in sorted(records.items())
            if isinstance(record, dict) and isinstance(record.get("expires_at"), str)
        ]
        if rows:
            await conn.executemany(
                (
                    "INSERT INTO shield_alert_action_records (token, payload, expires_at, alert_message_id) "
                    "VALUES ($1, $2::jsonb, $3::timestamptz, $4)"
                ),
                rows,
            )


class ShieldStateStore:
    def __init__(
        self,
        *,
        backend: str | None = None,
        database_url: str | None = None,
    ):
        requested_backend = (backend or os.getenv("SHIELD_STORAGE_BACKEND", "").strip() or DEFAULT_BACKEND).lower()
        self.database_url, self.database_url_source = _resolve_database_url(database_url)
        self.backend_preference = requested_backend
        self.database_url_source = self.database_url_source or "none"
        self.backend_name = requested_backend
        self._store: _BaseShieldStore | None = None
        self._construct_store(requested_backend)

    def _construct_store(self, requested_backend: str):
        LOGGER.info(
            "Shield storage init: backend_preference=%s database_url_configured=%s database_url_source=%s database_target=%s",
            requested_backend,
            "yes" if self.database_url else "no",
            self.database_url_source,
            _redact_database_url(self.database_url),
        )
        if requested_backend == "memory":
            self._store = _MemoryShieldStore()
        elif requested_backend == "postgres":
            if not self.database_url:
                raise ShieldStorageUnavailable("No Postgres Shield database URL is configured. Set UTILITY_DATABASE_URL, SUPABASE_DB_URL, or DATABASE_URL.")
            self._store = _PostgresShieldStore(self.database_url)
        else:
            raise ShieldStorageUnavailable(f"Unsupported Shield storage backend '{requested_backend}'.")
        self.backend_name = self._store.backend_name
        self.state = self._store.state
        LOGGER.info("Shield storage init succeeded: backend=%s", self.backend_name)

    async def load(self) -> dict[str, Any]:
        if self._store is None:
            raise ShieldStorageUnavailable("Shield storage was not initialized.")
        state = await self._store.load()
        self.state = state
        return state

    async def flush(self) -> bool:
        if self._store is None:
            raise ShieldStorageUnavailable("Shield storage was not initialized.")
        self._store.state = self.state
        flushed = await self._store.flush()
        self.state = self._store.state
        return flushed

    async def close(self):
        if self._store is not None:
            await self._store.close()

    def redacted_database_url(self) -> str:
        return _redact_database_url(self.database_url)
