from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


PERMISSION_SYNC_SCOPE_ALL_CHANNELS = "all_channels"
PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS = "selected_channels"
PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES = "selected_categories"
PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN = "category_children"
VALID_PERMISSION_SYNC_SCOPE_MODES = {
    PERMISSION_SYNC_SCOPE_ALL_CHANNELS,
    PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS,
    PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES,
    PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN,
}

PERMISSION_SYNC_APPLY_EXISTING = "existing"
PERMISSION_SYNC_APPLY_FUTURE = "future"
PERMISSION_SYNC_APPLY_BOTH = "both"
VALID_PERMISSION_SYNC_APPLY_TARGETS = {
    PERMISSION_SYNC_APPLY_EXISTING,
    PERMISSION_SYNC_APPLY_FUTURE,
    PERMISSION_SYNC_APPLY_BOTH,
}

PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS = "all_channels"
PERMISSION_SYNC_RULE_SCOPE_SELECTED_CATEGORIES = "selected_categories"
VALID_PERMISSION_SYNC_RULE_SCOPE_MODES = {
    PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS,
    PERMISSION_SYNC_RULE_SCOPE_SELECTED_CATEGORIES,
}

VALID_PERMISSION_SYNC_STATES = {"allow", "deny", "clear"}
VALID_PERMISSION_SYNC_CHANNEL_TYPES = {
    "text",
    "announcement",
    "voice",
    "stage",
    "forum",
    "category",
}

PERMISSION_SYNC_FLAG_LABELS = {
    "view_channel": "View Channel",
    "send_messages": "Send Messages",
    "send_messages_in_threads": "Send Messages In Threads",
    "create_public_threads": "Create Public Threads",
    "create_private_threads": "Create Private Threads",
    "add_reactions": "Add Reactions",
    "embed_links": "Embed Links",
    "attach_files": "Attach Files",
    "use_external_emojis": "Use External Emojis",
    "use_application_commands": "Use Application Commands",
    "connect": "Connect",
    "speak": "Speak",
    "stream": "Video",
    "use_voice_activation": "Use Voice Activity",
    "request_to_speak": "Request To Speak",
}
VALID_PERMISSION_SYNC_FLAGS = set(PERMISSION_SYNC_FLAG_LABELS)

PERMISSION_SYNC_STATE_LABELS = {
    "allow": "Allow",
    "deny": "Deny",
    "clear": "Clear",
}

PERMISSION_SYNC_SCOPE_LABELS = {
    PERMISSION_SYNC_SCOPE_ALL_CHANNELS: "All channels",
    PERMISSION_SYNC_SCOPE_SELECTED_CHANNELS: "Selected channels",
    PERMISSION_SYNC_SCOPE_SELECTED_CATEGORIES: "Selected categories only",
    PERMISSION_SYNC_SCOPE_CATEGORY_CHILDREN: "Channels inside selected categories",
}

PERMISSION_SYNC_APPLY_TARGET_LABELS = {
    PERMISSION_SYNC_APPLY_EXISTING: "Existing channels only",
    PERMISSION_SYNC_APPLY_FUTURE: "Future channels only",
    PERMISSION_SYNC_APPLY_BOTH: "Existing and future channels",
}

PERMISSION_SYNC_CHANNEL_TYPE_LABELS = {
    "text": "Text",
    "announcement": "Announcement",
    "voice": "Voice",
    "stage": "Stage",
    "forum": "Forum",
    "category": "Category",
}

PERMISSION_SYNC_PRESET_KEYS = {
    "quarantine",
    "muted",
    "not_verified",
    "verified",
}

PERMISSION_SYNC_RULE_LIMIT = 20


@dataclass(frozen=True)
class PermissionPresetDefinition:
    key: str
    name: str
    description: str
    recommended_scope: str
    caution: str
    permission_map: dict[str, str]


PERMISSION_SYNC_PRESETS: dict[str, PermissionPresetDefinition] = {
    "quarantine": PermissionPresetDefinition(
        key="quarantine",
        name="Quarantine",
        description="Hide the role and shut down text, thread, and voice participation in the selected scope.",
        recommended_scope="Best for all channels or tightly defined containment categories.",
        caution="Use this when the role should lose visibility almost everywhere.",
        permission_map={
            "view_channel": "deny",
            "send_messages": "deny",
            "send_messages_in_threads": "deny",
            "create_public_threads": "deny",
            "create_private_threads": "deny",
            "add_reactions": "deny",
            "use_application_commands": "deny",
            "connect": "deny",
            "speak": "deny",
            "stream": "deny",
            "use_voice_activation": "deny",
            "request_to_speak": "deny",
        },
    ),
    "muted": PermissionPresetDefinition(
        key="muted",
        name="Muted",
        description="Keep visibility untouched while denying posting, thread activity, reactions, and speaking-related actions.",
        recommended_scope="Best for selected text channels or selected moderation-sensitive categories.",
        caution="This preset does not hide channels; it focuses on participation limits.",
        permission_map={
            "send_messages": "deny",
            "send_messages_in_threads": "deny",
            "create_public_threads": "deny",
            "create_private_threads": "deny",
            "add_reactions": "deny",
            "speak": "deny",
            "stream": "deny",
            "use_voice_activation": "deny",
            "request_to_speak": "deny",
        },
    ),
    "not_verified": PermissionPresetDefinition(
        key="not_verified",
        name="Not Verified",
        description="Allow limited visibility while keeping normal participation and voice access turned off.",
        recommended_scope="Best for onboarding categories or pre-verification holding areas.",
        caution="This preset assumes onboarding channels should stay visible but quiet.",
        permission_map={
            "view_channel": "allow",
            "send_messages": "deny",
            "send_messages_in_threads": "deny",
            "create_public_threads": "deny",
            "create_private_threads": "deny",
            "add_reactions": "deny",
            "use_application_commands": "deny",
            "connect": "deny",
            "speak": "deny",
            "stream": "deny",
            "use_voice_activation": "deny",
            "request_to_speak": "deny",
        },
    ),
    "verified": PermissionPresetDefinition(
        key="verified",
        name="Verified",
        description="Grant normal visibility and participation in the selected trusted member scope.",
        recommended_scope="Best for member-access channels after you review the preview carefully.",
        caution="Allowing channel permissions can override a more restrictive baseline for this role.",
        permission_map={
            "view_channel": "allow",
            "send_messages": "allow",
            "send_messages_in_threads": "allow",
            "create_public_threads": "allow",
            "add_reactions": "allow",
            "embed_links": "allow",
            "attach_files": "allow",
            "use_external_emojis": "allow",
            "use_application_commands": "allow",
            "connect": "allow",
            "speak": "allow",
            "stream": "allow",
            "use_voice_activation": "allow",
            "request_to_speak": "allow",
        },
    ),
}


def default_permission_sync_channel_types() -> list[str]:
    return sorted(VALID_PERMISSION_SYNC_CHANNEL_TYPES)


def permission_flag_label(flag: str) -> str:
    return PERMISSION_SYNC_FLAG_LABELS.get(flag, flag.replace("_", " ").title())


def permission_state_label(state: str) -> str:
    return PERMISSION_SYNC_STATE_LABELS.get(state, state.title())


def permission_scope_label(scope: str) -> str:
    return PERMISSION_SYNC_SCOPE_LABELS.get(scope, scope.replace("_", " ").title())


def permission_apply_target_label(target: str) -> str:
    return PERMISSION_SYNC_APPLY_TARGET_LABELS.get(target, target.replace("_", " ").title())


def permission_channel_type_label(channel_type: str) -> str:
    return PERMISSION_SYNC_CHANNEL_TYPE_LABELS.get(channel_type, channel_type.replace("_", " ").title())


def _clean_int_list(values: Any) -> list[int]:
    if not isinstance(values, (list, tuple, set, frozenset)):
        return []
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _clean_text_list(values: Any, *, allowed: set[str]) -> list[str]:
    if not isinstance(values, (list, tuple, set, frozenset)):
        return []
    cleaned: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        normalized = value.strip().lower()
        if normalized in allowed:
            cleaned.add(normalized)
    return sorted(cleaned)


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
        parsed = _parse_datetime(value)
        return parsed.isoformat() if parsed is not None else None
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def normalize_permission_map(payload: Any) -> dict[str, str]:
    if not isinstance(payload, dict):
        return {}
    cleaned: dict[str, str] = {}
    for raw_flag, raw_state in payload.items():
        if not isinstance(raw_flag, str) or not isinstance(raw_state, str):
            continue
        flag = raw_flag.strip().lower()
        state = raw_state.strip().lower()
        if flag not in VALID_PERMISSION_SYNC_FLAGS or state not in VALID_PERMISSION_SYNC_STATES:
            continue
        cleaned[flag] = state
    return dict(sorted(cleaned.items()))


def normalize_permission_sync_rule(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    role_id = payload.get("role_id")
    if not isinstance(role_id, int) or role_id <= 0:
        return None
    permission_map = normalize_permission_map(payload.get("permission_map"))
    if not permission_map:
        return None
    scope_mode = str(payload.get("scope_mode", PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS)).strip().lower()
    if scope_mode not in VALID_PERMISSION_SYNC_RULE_SCOPE_MODES:
        scope_mode = PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS
    category_ids = _clean_int_list(payload.get("category_ids"))
    if scope_mode == PERMISSION_SYNC_RULE_SCOPE_ALL_CHANNELS:
        category_ids = []
    channel_type_filters = _clean_text_list(
        payload.get("channel_type_filters"),
        allowed=VALID_PERMISSION_SYNC_CHANNEL_TYPES,
    )
    preset_key_raw = payload.get("preset_key")
    preset_key = None
    if isinstance(preset_key_raw, str):
        candidate = preset_key_raw.strip().lower()
        if candidate in PERMISSION_SYNC_PRESET_KEYS:
            preset_key = candidate
    return {
        "role_id": role_id,
        "enabled": bool(payload.get("enabled", True)),
        "scope_mode": scope_mode,
        "category_ids": category_ids,
        "channel_type_filters": channel_type_filters or default_permission_sync_channel_types(),
        "permission_map": permission_map,
        "preset_key": preset_key,
        "updated_at": _serialize_datetime(payload.get("updated_at")),
    }


def summarize_permission_map(permission_map: dict[str, str]) -> list[str]:
    lines: list[str] = []
    for flag, state in sorted(permission_map.items()):
        lines.append(f"{permission_flag_label(flag)}: {permission_state_label(state)}")
    return lines
