from __future__ import annotations

import asyncio
import calendar
import contextlib
import hashlib
import re
import types
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.admin_store import (
    AdminStorageUnavailable,
    AdminStore,
    VALID_CHANNEL_LOCK_PERMISSION_NAMES,
    VALID_FOLLOWUP_MODES,
    VALID_VERIFICATION_DEADLINE_ACTIONS,
    VALID_VERIFICATION_LOGIC,
    default_admin_config,
    normalize_admin_config,
)
from babblebox.text_safety import normalize_plain_text
from babblebox.utility_helpers import deserialize_datetime, format_duration_brief, parse_duration_string, serialize_datetime


SWEEP_INTERVAL_SECONDS = 60.0
FOLLOWUP_BAN_RETURN_WINDOW_DAYS = 30
FOLLOWUP_REVIEW_LIMIT = 25
VERIFICATION_BATCH_LIMIT = 100
LOG_DEDUP_SECONDS = 3600.0
OPERATION_BACKOFF_SECONDS = 3600
TEMPLATE_MAX_LEN = 700
EXCLUSION_LIMIT = 20
HELP_MIN_CONTENT_LEN = 4
LOCK_MAX_DURATION_SECONDS = 30 * 24 * 3600
LOCK_NOTICE_MAX_LEN = 700
VERIFICATION_SYNC_DM_PACE_SECONDS = 1.0
VERIFICATION_SYNC_PROGRESS_INTERVAL = 10
VERIFICATION_SYNC_YIELD_INTERVAL = 25
VERIFICATION_SYNC_RUNTIME_ISSUE_LIMIT = 5
GROUPED_MEMBER_PREVIEW_LIMIT = 3
VERIFICATION_NOTIFICATION_SUPPRESSION_SECONDS = 24 * 3600
VERIFICATION_QUEUE_PREVIEW_LIMIT = 5
VERIFICATION_SUMMARY_LINE_LIMIT = 8
CONFIG_UNCHANGED = object()
VERIFICATION_QUEUE_RELEVANT_CONFIG_FIELDS = frozenset(
    {
        "admin_log_channel_id",
        "verification_enabled",
        "verification_role_id",
        "verification_logic",
        "verification_deadline_action",
        "excluded_user_ids",
        "excluded_role_ids",
        "trusted_role_ids",
        "verification_exempt_staff",
        "verification_exempt_bots",
    }
)

FOLLOWUP_MODE_LABELS = {"auto_remove": "Auto-remove", "review": "Moderator review"}
VERIFICATION_LOGIC_LABELS = {
    "must_have_role": "Unverified if member DOES NOT have this role",
    "must_not_have_role": "Unverified if member DOES have this role",
}
VERIFICATION_DEADLINE_ACTION_LABELS = {
    "auto_kick": "Kick automatically",
    "review": "Moderator review",
}
REVIEW_ACTION_LABELS = {
    "remove": "Remove role now",
    "delay_week": "Delay 1 week",
    "delay_month": "Delay 1 month",
    "keep": "Keep role for now",
}
VERIFICATION_REVIEW_ACTION_LABELS = {
    "kick": "Kick",
    "delay": "Delay",
    "ignore": "Ignore",
}
LOCK_PERMISSION_NAMES = tuple(
    name
    for name in (
        "send_messages",
        "create_public_threads",
        "create_private_threads",
        "send_messages_in_threads",
        "add_reactions",
    )
    if name in VALID_CHANNEL_LOCK_PERMISSION_NAMES
)
LOCK_NOTICE_FALLBACK = (
    "Dear members, due to an emergency this channel is temporarily locked. "
    "It will be unlocked as soon as we resolve the issue. "
    "Thank you for your patience and understanding."
)
LOCK_MODERATOR_PERMISSION_NAMES = (
    "manage_channels",
    "manage_messages",
    "moderate_members",
    "kick_members",
    "ban_members",
)
LOCK_ADMIN_ONLY_ACCESS_SUMMARY = "Admins only"
LOCK_MODERATOR_ACCESS_SUMMARY = "Moderators who can manage channels or messages, timeout, kick, or ban members, plus admins"
FOLLOWUP_DURATION_RE = re.compile(r"(?ix)^\s*(\d+)\s*(d|day|days|w|week|weeks|mo|mon|month|months|y|yr|year|years)\s*$")
VERIFICATION_REVIEW_DELAY_SECONDS = 24 * 3600


@dataclass(frozen=True)
class CompiledAdminConfig:
    guild_id: int
    followup_enabled: bool
    followup_role_id: int | None
    followup_mode: str
    followup_duration_value: int
    followup_duration_unit: str
    verification_enabled: bool
    verification_role_id: int | None
    verification_logic: str
    verification_deadline_action: str
    verification_kick_after_seconds: int
    verification_warning_lead_seconds: int
    verification_help_channel_id: int | None
    verification_help_extension_seconds: int
    verification_max_extensions: int
    admin_log_channel_id: int | None
    admin_alert_role_id: int | None
    warning_template: str | None
    kick_template: str | None
    invite_link: str | None
    lock_notice_template: str | None
    lock_admin_only: bool
    excluded_user_ids: frozenset[int]
    excluded_role_ids: frozenset[int]
    trusted_role_ids: frozenset[int]
    followup_exempt_staff: bool
    verification_exempt_staff: bool
    verification_exempt_bots: bool


@dataclass(frozen=True)
class VerificationPrecheck:
    severity: str
    message: str


@dataclass(frozen=True)
class VerificationSyncPreview:
    scanned_members: int
    matched_unverified: int
    newly_tracked: int
    stale_rows_to_clear: int
    already_tracked: int
    warnings_due_now: int
    blocked_kick_matches: int
    total_existing_rows: int
    warning_template_label: str
    warning_template_preview: str
    prechecks: tuple[VerificationPrecheck, ...]
    exact_member_scan: bool

    @property
    def blocking_prechecks(self) -> tuple[VerificationPrecheck, ...]:
        return tuple(check for check in self.prechecks if check.severity == "blocked")


@dataclass(frozen=True)
class VerificationSyncSummary:
    scanned_members: int
    matched_unverified: int
    tracked_count: int
    cleared_count: int
    warned_count: int
    failed_dm_count: int
    skipped_count: int
    manually_stopped: bool
    issues: tuple[str, ...]
    partial_failure: str | None = None


@dataclass
class VerificationSyncSession:
    guild_id: int
    actor_id: int
    created_at: datetime
    preview: VerificationSyncPreview
    stop_requested: bool = False
    running: bool = False
    finished_at: datetime | None = None
    current_member_id: int | None = None
    scanned_members: int = 0
    matched_unverified: int = 0
    tracked_count: int = 0
    cleared_count: int = 0
    warned_count: int = 0
    failed_dm_count: int = 0
    skipped_count: int = 0
    runtime_issues: list[str] = field(default_factory=list)
    partial_failure: str | None = None
    summary: VerificationSyncSummary | None = None


@dataclass(frozen=True)
class AdminActionIssue:
    code: str
    detail: str
    because_text: str


@dataclass(frozen=True)
class GroupedAdminLogKey:
    kind: str
    reason_code: str = ""
    reason_text: str | None = None
    role_mention: str | None = None
    duration_label: str | None = None
    dm_status: str | None = None


@dataclass(frozen=True)
class VerificationBatchKey:
    run_context: str
    operation: str
    outcome: str
    reason_code: str
    reason_text: str | None = None
    dm_status: str | None = None


@dataclass
class VerificationBatchGroup:
    mentions: list[str] = field(default_factory=list)
    member_ids: list[int] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class VerificationSweepBatch:
    run_context: str
    grouped_by_guild: dict[int, dict[VerificationBatchKey, VerificationBatchGroup]] = field(default_factory=dict)
    counts_by_guild: dict[int, dict[str, int]] = field(default_factory=dict)
    queue_refresh_guild_ids: set[int] = field(default_factory=set)


def _compile_config(raw: dict[str, Any]) -> CompiledAdminConfig:
    return CompiledAdminConfig(
        guild_id=int(raw["guild_id"]),
        followup_enabled=bool(raw["followup_enabled"]),
        followup_role_id=raw["followup_role_id"],
        followup_mode=raw["followup_mode"],
        followup_duration_value=int(raw["followup_duration_value"]),
        followup_duration_unit=raw["followup_duration_unit"],
        verification_enabled=bool(raw["verification_enabled"]),
        verification_role_id=raw["verification_role_id"],
        verification_logic=raw["verification_logic"],
        verification_deadline_action=raw["verification_deadline_action"],
        verification_kick_after_seconds=int(raw["verification_kick_after_seconds"]),
        verification_warning_lead_seconds=int(raw["verification_warning_lead_seconds"]),
        verification_help_channel_id=raw["verification_help_channel_id"],
        verification_help_extension_seconds=int(raw["verification_help_extension_seconds"]),
        verification_max_extensions=int(raw["verification_max_extensions"]),
        admin_log_channel_id=raw["admin_log_channel_id"],
        admin_alert_role_id=raw["admin_alert_role_id"],
        warning_template=raw["warning_template"],
        kick_template=raw["kick_template"],
        invite_link=raw["invite_link"],
        lock_notice_template=raw["lock_notice_template"],
        lock_admin_only=bool(raw["lock_admin_only"]),
        excluded_user_ids=frozenset(int(value) for value in raw.get("excluded_user_ids", [])),
        excluded_role_ids=frozenset(int(value) for value in raw.get("excluded_role_ids", [])),
        trusted_role_ids=frozenset(int(value) for value in raw.get("trusted_role_ids", [])),
        followup_exempt_staff=bool(raw["followup_exempt_staff"]),
        verification_exempt_staff=bool(raw["verification_exempt_staff"]),
        verification_exempt_bots=bool(raw["verification_exempt_bots"]),
    )


def _followup_duration_label(value: int, unit: str) -> str:
    if unit == "months":
        suffix = "" if value == 1 else "s"
        return f"{value} month{suffix}"
    if unit == "weeks":
        suffix = "" if value == 1 else "s"
        return f"{value} week{suffix}"
    suffix = "" if value == 1 else "s"
    return f"{value} day{suffix}"


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def add_followup_duration(start: datetime, *, value: int, unit: str) -> datetime:
    if unit == "months":
        return _add_months(start, value)
    if unit == "weeks":
        return start + timedelta(weeks=value)
    return start + timedelta(days=value)


def parse_followup_duration(raw: str | None) -> tuple[bool, tuple[int, str] | str]:
    if raw is None or not raw.strip():
        return False, "Provide a duration like `14d`, `3w`, or `6mo`."
    match = FOLLOWUP_DURATION_RE.fullmatch(raw.strip())
    if match is None:
        return False, "Use days, weeks, or months like `14d`, `3w`, or `6mo`."
    amount = int(match.group(1))
    token = match.group(2).lower()
    if token in {"y", "yr", "year", "years"}:
        amount *= 12
        unit = "months"
    elif token in {"mo", "mon", "month", "months"}:
        unit = "months"
    elif token in {"w", "week", "weeks"}:
        unit = "weeks"
    else:
        unit = "days"
    limits = {"days": 365, "weeks": 52, "months": 12}
    if not (1 <= amount <= limits[unit]):
        return False, f"That duration is too large. Follow-up supports up to {limits[unit]} {unit}."
    return True, (amount, unit)


def _parse_template_text(raw: str | None, *, label: str) -> tuple[bool, str | None]:
    if raw is None:
        return True, None
    cleaned = normalize_plain_text(raw)
    if not cleaned:
        return True, None
    if len(cleaned) > TEMPLATE_MAX_LEN:
        return False, f"{label} must be {TEMPLATE_MAX_LEN} characters or fewer."
    return True, cleaned


def _parse_lock_duration(raw: str | None) -> tuple[bool, int | None | str]:
    if raw is None:
        return True, None
    parsed = parse_duration_string(raw)
    if parsed is None:
        return False, "Lock duration must use a value like `30m`, `2h`, or `1d`."
    if parsed <= 0:
        return False, "Lock duration must be greater than zero."
    if parsed < 60:
        return False, "Lock duration must be at least 1 minute."
    if parsed > LOCK_MAX_DURATION_SECONDS:
        return False, f"Lock duration can be at most {format_duration_brief(LOCK_MAX_DURATION_SECONDS)}."
    return True, parsed


def _parse_lock_notice_text(raw: str | None, *, label: str) -> tuple[bool, str | None]:
    if raw is None:
        return True, None
    cleaned = normalize_plain_text(raw)
    if not cleaned:
        return True, None
    if len(cleaned) > LOCK_NOTICE_MAX_LEN:
        return False, f"{label} must be {LOCK_NOTICE_MAX_LEN} characters or fewer."
    return True, cleaned


def _parse_invite_link(raw: str | None) -> tuple[bool, str | None]:
    if raw is None:
        return True, None
    cleaned = raw.strip()
    if not cleaned:
        return True, None
    parsed = urlsplit(cleaned)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False, "Invite link must be a full `https://...` URL."
    if len(cleaned) > 300:
        return False, "Invite link is too long."
    return True, cleaned


class AdminService:
    def __init__(self, bot: commands.Bot, store: AdminStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = AdminStore()
            except AdminStorageUnavailable as exc:
                print(f"Admin storage constructor failed: {exc}")
                self.store = AdminStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._compiled_configs: dict[int, CompiledAdminConfig] = {}
        self._log_dedup: dict[tuple[int, str], float] = {}
        self._verification_sync_sessions: dict[int, VerificationSyncSession] = {}
        self._verification_sync_lock = asyncio.Lock()
        self._startup_resume_pending = True

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Admin storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except AdminStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Admin storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        await self._rebuild_config_cache()
        self._startup_resume_pending = True
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="babblebox-admin-scheduler")
        self._wake_event.set()
        return True

    async def close(self):
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task
        await self.store.close()

    def storage_message(self, feature_name: str = "Admin systems") -> str:
        return f"{feature_name} are temporarily unavailable because Babblebox could not reach its admin database."

    async def _rebuild_config_cache(self):
        configs = await self.store.fetch_all_configs()
        self._compiled_configs = {guild_id: _compile_config(config) for guild_id, config in configs.items()}

    def get_config(self, guild_id: int) -> dict[str, Any]:
        compiled = self._compiled_configs.get(guild_id)
        if compiled is None:
            return default_admin_config(guild_id)
        return normalize_admin_config(guild_id, dict(compiled.__dict__))

    async def get_counts(self, guild_id: int) -> dict[str, int]:
        if not self.storage_ready:
            return {
                "ban_candidates": 0,
                "active_followups": 0,
                "pending_reviews": 0,
                "verification_pending": 0,
                "verification_warned": 0,
                "active_channel_locks": 0,
            }
        return await self.store.fetch_guild_counts(guild_id)

    def get_compiled_config(self, guild_id: int) -> CompiledAdminConfig:
        return self._compiled_configs.get(guild_id) or _compile_config(default_admin_config(guild_id))

    async def _update_config(
        self,
        guild_id: int,
        mutator,
        *,
        success_message: str,
        post_update_hook=None,
        requested_fields: set[str] | None = None,
        force_post_update: bool = False,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Admin systems")
        before: dict[str, Any] | None = None
        async with self._lock:
            before = self.get_config(guild_id)
            before["guild_id"] = guild_id
            current = dict(before)
            try:
                mutator(current)
            except ValueError as exc:
                return False, str(exc)
            normalized = normalize_admin_config(guild_id, current)
            validation_error = self._validate_config(normalized)
            if validation_error is not None:
                return False, validation_error
            await self.store.upsert_config(normalized)
            self._compiled_configs[guild_id] = _compile_config(normalized)
        before = before or default_admin_config(guild_id)
        changed_fields = {
            field
            for field in set(before) | set(normalized)
            if before.get(field) != normalized.get(field)
        }
        if post_update_hook is not None:
            await post_update_hook(
                guild_id,
                before=before,
                after=normalized,
                changed_fields=changed_fields,
                requested_fields=set(requested_fields or ()),
                force=force_post_update,
            )
        self._wake_event.set()
        return True, success_message

    def _validate_config(self, config: dict[str, Any]) -> str | None:
        if config["followup_mode"] not in VALID_FOLLOWUP_MODES:
            return "Follow-up mode must be `auto_remove` or `review`."
        if config["verification_logic"] not in VALID_VERIFICATION_LOGIC:
            return (
                "Verification logic must be `must_have_role` "
                "(unverified if the member is missing the role) or `must_not_have_role` "
                "(unverified if the member has the role)."
            )
        if config["verification_deadline_action"] not in VALID_VERIFICATION_DEADLINE_ACTIONS:
            return "Verification deadline action must be `auto_kick` or `review`."
        if config["followup_duration_unit"] == "months" and config["followup_duration_value"] > 12:
            return "Follow-up month durations can be at most 12 months."
        if config["verification_warning_lead_seconds"] >= config["verification_kick_after_seconds"]:
            return "Warning lead time must be shorter than the full verification kick timer."
        if config.get("lock_notice_template") and len(str(config["lock_notice_template"])) > LOCK_NOTICE_MAX_LEN:
            return f"Lock notice must be {LOCK_NOTICE_MAX_LEN} characters or fewer."
        for field in ("excluded_user_ids", "excluded_role_ids", "trusted_role_ids"):
            if len(config[field]) > EXCLUSION_LIMIT:
                label = field.replace("_ids", "").replace("_", " ")
                return f"You can keep up to {EXCLUSION_LIMIT} entries in `{label}`."
        return None

    async def set_followup_config(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        role_id: int | None | object = CONFIG_UNCHANGED,
        mode: str | None = None,
        duration_text: str | None = None,
    ) -> tuple[bool, str]:
        cleaned_mode = mode.strip().lower() if isinstance(mode, str) else None
        if cleaned_mode is not None and cleaned_mode not in VALID_FOLLOWUP_MODES:
            return False, "Follow-up mode must be `auto_remove` or `review`."
        parsed_duration: tuple[int, str] | None = None
        if duration_text is not None:
            ok, parsed_or_message = parse_followup_duration(duration_text)
            if not ok:
                return False, str(parsed_or_message)
            parsed_duration = parsed_or_message

        def mutate(config: dict[str, Any]):
            if enabled is not None:
                config["followup_enabled"] = bool(enabled)
            if role_id is not CONFIG_UNCHANGED:
                config["followup_role_id"] = role_id
            if cleaned_mode is not None:
                config["followup_mode"] = cleaned_mode
            if parsed_duration is not None:
                config["followup_duration_value"] = parsed_duration[0]
                config["followup_duration_unit"] = parsed_duration[1]

        preview = self.get_config(guild_id)
        final_enabled = preview["followup_enabled"] if enabled is None else bool(enabled)
        final_role = preview["followup_role_id"] if role_id is CONFIG_UNCHANGED else role_id
        final_mode = preview["followup_mode"] if cleaned_mode is None else cleaned_mode
        final_value = preview["followup_duration_value"] if parsed_duration is None else parsed_duration[0]
        final_unit = preview["followup_duration_unit"] if parsed_duration is None else parsed_duration[1]
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Punishment follow-up is {'enabled' if final_enabled else 'disabled'} "
                f"for role <@&{final_role}> with `{final_mode}` after {_followup_duration_label(final_value, final_unit)}."
                if final_role
                else f"Punishment follow-up is {'enabled' if final_enabled else 'disabled'}."
            ),
        )

    async def set_verification_config(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        role_id: int | None | object = CONFIG_UNCHANGED,
        logic: str | None = None,
        deadline_action: str | None = None,
        kick_after_text: str | None = None,
        warning_lead_text: str | None = None,
        help_channel_id: int | None | object = CONFIG_UNCHANGED,
        help_extension_text: str | None = None,
        max_extensions: int | None = None,
    ) -> tuple[bool, str]:
        cleaned_logic = logic.strip().lower() if isinstance(logic, str) else None
        if cleaned_logic is not None and cleaned_logic not in VALID_VERIFICATION_LOGIC:
            return (
                False,
                "Verification logic must be `must_have_role` "
                "(unverified if the member is missing the role) or `must_not_have_role` "
                "(unverified if the member has the role).",
            )
        cleaned_deadline_action = deadline_action.strip().lower() if isinstance(deadline_action, str) else None
        if cleaned_deadline_action is not None and cleaned_deadline_action not in VALID_VERIFICATION_DEADLINE_ACTIONS:
            return False, "Verification deadline action must be `auto_kick` or `review`."
        parsed_kick_after = parse_duration_string(kick_after_text) if kick_after_text is not None else None
        if kick_after_text is not None and parsed_kick_after is None:
            return False, "Kick timer must use a duration like `7d` or `24h`."
        parsed_warning_lead = parse_duration_string(warning_lead_text) if warning_lead_text is not None else None
        if warning_lead_text is not None and parsed_warning_lead is None:
            return False, "Warning lead must use a duration like `24h` or `2d`."
        parsed_help_extension = parse_duration_string(help_extension_text) if help_extension_text is not None else None
        if help_extension_text is not None and parsed_help_extension is None:
            return False, "Help extension must use a duration like `12h` or `3d`."
        if max_extensions is not None and not (0 <= max_extensions <= 5):
            return False, "Help extensions can be limited from 0 to 5."

        def mutate(config: dict[str, Any]):
            if enabled is not None:
                config["verification_enabled"] = bool(enabled)
            if role_id is not CONFIG_UNCHANGED:
                config["verification_role_id"] = role_id
            if cleaned_logic is not None:
                config["verification_logic"] = cleaned_logic
            if cleaned_deadline_action is not None:
                config["verification_deadline_action"] = cleaned_deadline_action
            if parsed_kick_after is not None:
                config["verification_kick_after_seconds"] = parsed_kick_after
            if parsed_warning_lead is not None:
                config["verification_warning_lead_seconds"] = parsed_warning_lead
            if help_channel_id is not CONFIG_UNCHANGED:
                config["verification_help_channel_id"] = help_channel_id
            if parsed_help_extension is not None:
                config["verification_help_extension_seconds"] = parsed_help_extension
            if max_extensions is not None:
                config["verification_max_extensions"] = max_extensions

        preview = self.get_config(guild_id)
        final_enabled = preview["verification_enabled"] if enabled is None else bool(enabled)
        final_role = preview["verification_role_id"] if role_id is CONFIG_UNCHANGED else role_id
        final_logic = preview["verification_logic"] if cleaned_logic is None else cleaned_logic
        final_deadline_action = preview["verification_deadline_action"] if cleaned_deadline_action is None else cleaned_deadline_action
        final_kick_after = preview["verification_kick_after_seconds"] if parsed_kick_after is None else parsed_kick_after
        final_warning_lead = preview["verification_warning_lead_seconds"] if parsed_warning_lead is None else parsed_warning_lead
        requested_fields = {
            field
            for field, supplied in (
                ("verification_enabled", enabled is not None),
                ("verification_role_id", role_id is not CONFIG_UNCHANGED),
                ("verification_logic", cleaned_logic is not None),
                ("verification_deadline_action", cleaned_deadline_action is not None),
            )
            if supplied
        }
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Verification cleanup is {'enabled' if final_enabled else 'disabled'} "
                f"for role <@&{final_role}> using `{final_logic}` with warning {format_duration_brief(final_warning_lead)} "
                f"before a {format_duration_brief(final_kick_after)} deadline and `{final_deadline_action}` action."
                if final_role
                else f"Verification cleanup is {'enabled' if final_enabled else 'disabled'}."
            ),
            post_update_hook=self._reconcile_verification_review_backlog_after_config_change,
            requested_fields=requested_fields,
            force_post_update=bool(requested_fields),
        )

    async def set_logs_config(
        self,
        guild_id: int,
        *,
        channel_id: int | None = None,
        alert_role_id: int | None = None,
    ) -> tuple[bool, str]:
        def mutate(config: dict[str, Any]):
            config["admin_log_channel_id"] = channel_id
            config["admin_alert_role_id"] = alert_role_id

        return await self._update_config(
            guild_id,
            mutate,
            success_message="Admin log channel and alert role updated.",
            post_update_hook=self._reconcile_verification_review_backlog_after_config_change,
            requested_fields={"admin_log_channel_id"},
            force_post_update=True,
        )

    async def set_exclusion_target(self, guild_id: int, field: str, target_id: int, enabled: bool) -> tuple[bool, str]:
        if field not in {"excluded_user_ids", "excluded_role_ids", "trusted_role_ids"}:
            return False, "Unknown exclusion bucket."

        def mutate(config: dict[str, Any]):
            values = set(int(value) for value in config.get(field, []))
            if enabled:
                values.add(target_id)
            else:
                values.discard(target_id)
            if len(values) > EXCLUSION_LIMIT:
                raise ValueError(f"You can keep up to {EXCLUSION_LIMIT} entries in `{field}`.")
            config[field] = sorted(values)

        label = field.replace("_ids", "").replace("_", " ")
        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Admin {label} was {'updated' if enabled else 'trimmed'}.",
            post_update_hook=self._reconcile_verification_review_backlog_after_config_change,
            requested_fields={field},
            force_post_update=field in VERIFICATION_QUEUE_RELEVANT_CONFIG_FIELDS,
        )

    async def replace_exclusion_targets(self, guild_id: int, field: str, target_ids: list[int]) -> tuple[bool, str]:
        if field not in {"excluded_user_ids", "excluded_role_ids", "trusted_role_ids"}:
            return False, "Unknown exclusion bucket."

        cleaned = sorted({int(value) for value in target_ids if isinstance(value, int) and value > 0})
        if len(cleaned) > EXCLUSION_LIMIT:
            return False, f"You can keep up to {EXCLUSION_LIMIT} entries in `{field}`."

        label = field.replace("_ids", "").replace("_", " ")
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__(field, cleaned),
            success_message=f"Admin {label} list updated.",
            post_update_hook=self._reconcile_verification_review_backlog_after_config_change,
            requested_fields={field},
            force_post_update=field in VERIFICATION_QUEUE_RELEVANT_CONFIG_FIELDS,
        )

    async def set_exemption_toggle(self, guild_id: int, field: str, enabled: bool) -> tuple[bool, str]:
        if field not in {"followup_exempt_staff", "verification_exempt_staff", "verification_exempt_bots"}:
            return False, "Unknown exemption toggle."
        label = field.replace("_", " ")
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__(field, bool(enabled)),
            success_message=f"{label.title()} is now {'enabled' if enabled else 'disabled'}.",
            post_update_hook=self._reconcile_verification_review_backlog_after_config_change,
            requested_fields={field},
            force_post_update=field in VERIFICATION_QUEUE_RELEVANT_CONFIG_FIELDS,
        )

    async def set_templates(
        self,
        guild_id: int,
        *,
        warning_template: str | None | object = ...,
        kick_template: str | None | object = ...,
        invite_link: str | None | object = ...,
    ) -> tuple[bool, str]:
        if warning_template is not ...:
            ok, warning_template_or_message = _parse_template_text(warning_template, label="Warning template")
            if not ok:
                return False, str(warning_template_or_message)
            warning_value = warning_template_or_message
        else:
            warning_value = ...
        if kick_template is not ...:
            ok, kick_template_or_message = _parse_template_text(kick_template, label="Kick template")
            if not ok:
                return False, str(kick_template_or_message)
            kick_value = kick_template_or_message
        else:
            kick_value = ...
        if invite_link is not ...:
            ok, invite_link_or_message = _parse_invite_link(invite_link)
            if not ok:
                return False, str(invite_link_or_message)
            invite_value = invite_link_or_message
        else:
            invite_value = ...

        def mutate(config: dict[str, Any]):
            if warning_value is not ...:
                config["warning_template"] = warning_value
            if kick_value is not ...:
                config["kick_template"] = kick_value
            if invite_value is not ...:
                config["invite_link"] = invite_value

        return await self._update_config(
            guild_id,
            mutate,
            success_message="Verification templates and invite link updated.",
        )

    def lock_notice_text(self, guild_id: int) -> str:
        compiled = self.get_compiled_config(guild_id)
        return (compiled.lock_notice_template or LOCK_NOTICE_FALLBACK).strip()

    def lock_access_summary(self, guild_id: int) -> str:
        compiled = self.get_compiled_config(guild_id)
        if compiled.lock_admin_only:
            return LOCK_ADMIN_ONLY_ACCESS_SUMMARY
        return LOCK_MODERATOR_ACCESS_SUMMARY

    async def set_lock_config(
        self,
        guild_id: int,
        *,
        notice_template: str | None | object = ...,
        admin_only: bool | None = None,
    ) -> tuple[bool, str]:
        if notice_template is not ...:
            ok, notice_or_message = _parse_lock_notice_text(notice_template, label="Lock notice")
            if not ok:
                return False, str(notice_or_message)
            notice_value = notice_or_message
        else:
            notice_value = ...

        def mutate(config: dict[str, Any]):
            if notice_value is not ...:
                config["lock_notice_template"] = notice_value
            if admin_only is not None:
                config["lock_admin_only"] = bool(admin_only)

        preview = self.get_config(guild_id)
        final_notice = preview["lock_notice_template"] if notice_value is ... else notice_value
        final_admin_only = preview["lock_admin_only"] if admin_only is None else bool(admin_only)
        notice_label = "Custom" if final_notice else "Babblebox default"
        access_label = LOCK_ADMIN_ONLY_ACCESS_SUMMARY if final_admin_only else LOCK_MODERATOR_ACCESS_SUMMARY
        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Emergency lock settings updated. Default notice: **{notice_label}**. Access: **{access_label}**.",
        )

    def _default_role(self, guild: discord.Guild | None):
        if guild is None:
            return None
        role = getattr(guild, "default_role", None)
        if role is not None:
            return role
        get_role = getattr(guild, "get_role", None)
        guild_id = getattr(guild, "id", None)
        if callable(get_role) and isinstance(guild_id, int):
            return get_role(guild_id)
        return None

    def _copy_overwrite(self, overwrite: discord.PermissionOverwrite) -> discord.PermissionOverwrite:
        allow, deny = overwrite.pair()
        return discord.PermissionOverwrite.from_pair(allow, deny)

    def _overwrite_is_empty(self, overwrite: discord.PermissionOverwrite) -> bool:
        allow, deny = overwrite.pair()
        return int(getattr(allow, "value", 0) or 0) == 0 and int(getattr(deny, "value", 0) or 0) == 0

    def _lock_manage_issue(self, guild: discord.Guild, channel) -> AdminActionIssue | None:
        me = self._bot_member(guild)
        if me is None:
            return AdminActionIssue(
                code="bot_member_unavailable",
                detail="Babblebox could not resolve its own server member for channel locks.",
                because_text="Babblebox could not resolve its own server member for channel locks",
            )
        guild_perms = getattr(me, "guild_permissions", None)
        if guild_perms is None or not getattr(guild_perms, "manage_channels", False):
            return AdminActionIssue(
                code="missing_manage_channels",
                detail="Babblebox is missing Manage Channels.",
                because_text="Babblebox is missing Manage Channels",
            )
        channel_perms = channel.permissions_for(me)
        if not getattr(channel_perms, "view_channel", False):
            return AdminActionIssue(
                code="missing_view_channel",
                detail=f"Babblebox cannot view {getattr(channel, 'mention', 'that channel')}.",
                because_text="Babblebox cannot view that channel",
            )
        if not getattr(channel_perms, "manage_channels", False):
            return AdminActionIssue(
                code="channel_manage_denied",
                detail=f"Manage Channels is denied for Babblebox in {getattr(channel, 'mention', 'that channel')}.",
                because_text="Manage Channels is denied in that channel",
            )
        return None

    def _channel_lock_supported(self, channel) -> bool:
        if isinstance(channel, discord.TextChannel):
            return True
        channel_type = getattr(channel, "type", None)
        supported_types = {discord.ChannelType.text}
        news_type = getattr(discord.ChannelType, "news", None)
        if news_type is not None:
            supported_types.add(news_type)
        return channel_type in supported_types

    def _lock_restore_blocker(self, channel, record: dict[str, Any]) -> str | None:
        if bool(getattr(channel, "permissions_synced", False)):
            return "The channel is synced to its category, so Babblebox will not guess at a safe overwrite restore."
        stored_category_id = record.get("category_id")
        current_category_id = getattr(channel, "category_id", None)
        if stored_category_id != current_category_id:
            return "The channel moved to a different category while it was locked, so Babblebox will not guess which overwrite should win now."
        return None

    def _lock_reason_text(self, *, actor: discord.Member | None, automatic: bool, action: str) -> str:
        if automatic or actor is None:
            return f"Babblebox timed emergency channel {action}"
        return f"Babblebox emergency channel {action} by {ge.display_name_of(actor)}"

    def _lock_record_is_marker_only(self, record: dict[str, Any] | None) -> bool:
        return bool(record and record.get("marker_only"))

    def _overwrite_matches_lock_restrictions(self, overwrite: discord.PermissionOverwrite) -> bool:
        return all(getattr(overwrite, name, None) is False for name in LOCK_PERMISSION_NAMES)

    def _build_channel_lock_record(
        self,
        *,
        guild_id: int,
        channel,
        actor_id: int | None,
        created_at: datetime,
        due_at: datetime | None,
        marker_only: bool,
        locked_permissions: list[str],
        original_permissions: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "guild_id": guild_id,
            "channel_id": channel.id,
            "actor_id": actor_id,
            "created_at": serialize_datetime(created_at),
            "due_at": serialize_datetime(due_at),
            "category_id": getattr(channel, "category_id", None),
            "permissions_synced": bool(getattr(channel, "permissions_synced", False)),
            "marker_only": marker_only,
            "locked_permissions": locked_permissions,
            "original_permissions": {name: original_permissions.get(name) for name in locked_permissions},
        }

    def _timeout_actor_issue(self, guild: discord.Guild, actor: discord.Member, member: discord.Member) -> AdminActionIssue | None:
        perms = getattr(actor, "guild_permissions", None)
        if perms is None or not (
            getattr(perms, "administrator", False)
            or getattr(perms, "manage_guild", False)
            or getattr(perms, "moderate_members", False)
        ):
            return AdminActionIssue(
                code="missing_timeout_access",
                detail="You need Timeout Members, Manage Server, or administrator access to remove timeouts.",
                because_text="you are missing Timeout Members, Manage Server, or administrator access",
            )
        if getattr(member.guild_permissions, "administrator", False):
            return AdminActionIssue(
                code="target_is_administrator",
                detail="They are administrators.",
                because_text="they are administrators",
            )
        if getattr(guild, "owner_id", None) == member.id:
            return AdminActionIssue(
                code="target_is_owner",
                detail="They are the server owner.",
                because_text="they are the server owner",
            )
        if actor.id != getattr(guild, "owner_id", None):
            if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(actor, "top_role", None), "position", 0):
                return AdminActionIssue(
                    code="target_above_actor_role",
                    detail="They are at or above your top role.",
                    because_text="they are at or above your top role",
                )
        return None

    def _timeout_issue(self, guild: discord.Guild, member: discord.Member) -> AdminActionIssue | None:
        me = self._bot_member(guild)
        if me is None:
            return AdminActionIssue(
                code="bot_member_unavailable",
                detail="Babblebox could not resolve its server member for timeout removal.",
                because_text="Babblebox could not resolve its server member for timeout removal",
            )
        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "moderate_members", False):
            return AdminActionIssue(
                code="missing_moderate_members",
                detail="Babblebox is missing Timeout Members.",
                because_text="Babblebox is missing Timeout Members",
            )
        if getattr(member.guild_permissions, "administrator", False):
            return AdminActionIssue(
                code="target_is_administrator",
                detail="They are administrators.",
                because_text="they are administrators",
            )
        if getattr(guild, "owner_id", None) == member.id:
            return AdminActionIssue(
                code="target_is_owner",
                detail="They are the server owner.",
                because_text="they are the server owner",
            )
        if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return AdminActionIssue(
                code="target_above_bot_role",
                detail="They are at or above Babblebox's top role.",
                because_text="they are at or above Babblebox's top role",
            )
        return None

    def _member_timeout_until(self, member: discord.Member) -> datetime | None:
        timed_out_until = getattr(member, "timed_out_until", None)
        if timed_out_until is None:
            timed_out_until = getattr(member, "communication_disabled_until", None)
        return timed_out_until if isinstance(timed_out_until, datetime) else None

    async def _post_lock_notice(self, channel, text: str) -> tuple[bool, str]:
        try:
            await channel.send(
                content=text,
                allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
            )
            return True, "Posted in the locked channel."
        except (discord.Forbidden, discord.HTTPException):
            return False, "Babblebox could not post the lock notice in that channel."

    def _build_lock_log_embed(
        self,
        *,
        title: str,
        description: str,
        tone: str,
        channel,
        timer_label: str,
        notice_status: str | None = None,
        restore_status: str | None = None,
    ) -> discord.Embed:
        embed = ge.make_status_embed(title, description, tone=tone, footer="Babblebox Lock")
        embed.add_field(name="Channel", value=getattr(channel, "mention", f"<#{getattr(channel, 'id', 0)}>"), inline=True)
        embed.add_field(name="Timer", value=timer_label, inline=True)
        embed.add_field(
            name="Restrictions",
            value="@everyone cannot send messages, create threads, send in threads, or add reactions.",
            inline=False,
        )
        if notice_status is not None:
            embed.add_field(name="Notice", value=notice_status, inline=False)
        if restore_status is not None:
            embed.add_field(name="Restore", value=restore_status, inline=False)
        return embed

    async def lock_channel(
        self,
        guild: discord.Guild,
        channel,
        *,
        actor: discord.Member,
        duration_text: str | None = None,
        notice_message: str | None = None,
        post_notice: bool = True,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Emergency lock tools")
        if not self._channel_lock_supported(channel):
            return False, "Babblebox only supports direct emergency locks for normal text channels."
        if bool(getattr(channel, "permissions_synced", False)):
            return False, "Babblebox will not lock a category-synced channel because that would break sync and make restore less trustworthy."
        lock_issue = self._lock_manage_issue(guild, channel)
        if lock_issue is not None:
            return False, lock_issue.detail
        ok, duration_or_message = _parse_lock_duration(duration_text)
        if not ok:
            return False, str(duration_or_message)
        ok, notice_or_message = _parse_lock_notice_text(notice_message, label="Lock notice")
        if not ok:
            return False, str(notice_or_message)
        duration_seconds = duration_or_message if isinstance(duration_or_message, int) else None
        compiled = self.get_compiled_config(guild.id)
        everyone_role = self._default_role(guild)
        if everyone_role is None:
            return False, "Babblebox could not resolve the server's @everyone role for this lock."
        current_record = await self.store.fetch_channel_lock(guild.id, channel.id)
        current_overwrite = channel.overwrites_for(everyone_role)
        now = ge.now_utc()

        if current_record is not None:
            if self._lock_record_is_marker_only(current_record):
                if self._overwrite_matches_lock_restrictions(current_overwrite):
                    due_at = deserialize_datetime(current_record.get("due_at"))
                    updated_due_at = now + timedelta(seconds=duration_seconds) if duration_seconds is not None else due_at
                    if duration_seconds is not None:
                        current_record["due_at"] = serialize_datetime(updated_due_at)
                        current_record["actor_id"] = actor.id
                        await self.store.upsert_channel_lock(current_record)
                        self._wake_event.set()
                    notice_status = "Suppressed for this run."
                    if post_notice:
                        final_notice = notice_or_message or self.lock_notice_text(guild.id)
                        posted, notice_status = await self._post_lock_notice(channel, final_notice)
                        if not posted:
                            notice_status = "Timer updated, but the notice could not be posted."
                    timer_label = (
                        format_duration_brief(duration_seconds)
                        if duration_seconds is not None
                        else (f"Until {ge.format_timestamp(updated_due_at, 'R')}" if updated_due_at is not None else "Manual unlock required")
                    )
                    log_sent = await self.send_log(
                        guild,
                        compiled,
                        embed=self._build_lock_log_embed(
                            title="Channel Lock Updated",
                            description=f"{actor.mention} refreshed the tracked emergency lock for {channel.mention}.",
                            tone="info",
                            channel=channel,
                            timer_label=timer_label,
                            notice_status=notice_status,
                            restore_status="Babblebox is only tracking this lock state. Later unlocks will clear Babblebox's marker and timer without reopening the overwrite.",
                        ),
                        alert=False,
                    )
                    status_lines = [f"{channel.mention} is already locked by Babblebox."]
                    if duration_seconds is not None:
                        status_lines.append(f"Timer updated to **{format_duration_brief(duration_seconds)}** from now.")
                    elif due_at is not None:
                        status_lines.append(f"Current timer ends {ge.format_timestamp(due_at, 'R')}.")
                    else:
                        status_lines.append("This tracked lock stays in place until someone removes it.")
                    status_lines.append("Babblebox is tracking the existing overwrite and will only clear its own lock state later.")
                    if post_notice:
                        status_lines.append(notice_status)
                    status_lines.append("Admin log updated." if log_sent else "Admin log delivery was unavailable.")
                    return True, " ".join(status_lines)
            else:
                tracked_flags = tuple(current_record.get("locked_permissions", ()))
                if any(getattr(current_overwrite, name, None) is not False for name in tracked_flags):
                    return False, "Babblebox already tracks a lock here, but the overwrite changed while it was active. Review the channel and use `/lock remove` before locking it again."
                due_at = deserialize_datetime(current_record.get("due_at"))
                updated_due_at = now + timedelta(seconds=duration_seconds) if duration_seconds is not None else due_at
                if duration_seconds is not None:
                    current_record["due_at"] = serialize_datetime(updated_due_at)
                    current_record["actor_id"] = actor.id
                    await self.store.upsert_channel_lock(current_record)
                    self._wake_event.set()
                notice_status = "Suppressed for this run."
                if post_notice:
                    final_notice = notice_or_message or self.lock_notice_text(guild.id)
                    posted, notice_status = await self._post_lock_notice(channel, final_notice)
                    if not posted:
                        notice_status = "Timer updated, but the notice could not be posted."
                timer_label = (
                    format_duration_brief(duration_seconds)
                    if duration_seconds is not None
                    else (f"Until {ge.format_timestamp(updated_due_at, 'R')}" if updated_due_at is not None else "Manual unlock required")
                )
                log_sent = await self.send_log(
                    guild,
                    compiled,
                    embed=self._build_lock_log_embed(
                        title="Channel Lock Updated",
                        description=f"{actor.mention} refreshed the emergency lock for {channel.mention}.",
                        tone="info",
                        channel=channel,
                        timer_label=timer_label,
                        notice_status=notice_status,
                    ),
                    alert=False,
                )
                status_lines = [f"{channel.mention} is already locked by Babblebox."]
                if duration_seconds is not None:
                    status_lines.append(f"Timer updated to **{format_duration_brief(duration_seconds)}** from now.")
                elif due_at is not None:
                    status_lines.append(f"Current timer ends {ge.format_timestamp(due_at, 'R')}.")
                else:
                    status_lines.append("This lock stays in place until someone removes it.")
                if post_notice:
                    status_lines.append(notice_status)
                status_lines.append("Admin log updated." if log_sent else "Admin log delivery was unavailable.")
                return True, " ".join(status_lines)

        previous_values = {name: getattr(current_overwrite, name, None) for name in LOCK_PERMISSION_NAMES}
        locked_permissions = [name for name, value in previous_values.items() if value is not False]
        if not locked_permissions:
            due_at = now + timedelta(seconds=duration_seconds) if duration_seconds is not None else None
            await self.store.upsert_channel_lock(
                self._build_channel_lock_record(
                    guild_id=guild.id,
                    channel=channel,
                    actor_id=actor.id,
                    created_at=now,
                    due_at=due_at,
                    marker_only=True,
                    locked_permissions=[],
                    original_permissions={},
                )
            )
            self._wake_event.set()
            notice_status = "Notice suppressed for this run."
            if post_notice:
                final_notice = notice_or_message or self.lock_notice_text(guild.id)
                posted, notice_status = await self._post_lock_notice(channel, final_notice)
                if not posted:
                    notice_status = "Tracked lock started, but the notice could not be posted."
            timer_label = format_duration_brief(duration_seconds) if duration_seconds is not None else "Manual unlock required"
            log_sent = await self.send_log(
                guild,
                compiled,
                embed=self._build_lock_log_embed(
                    title="Channel Lock Tracked",
                    description=f"{actor.mention} started tracking the existing emergency lock state on {channel.mention}.",
                    tone="warning",
                    channel=channel,
                    timer_label=timer_label,
                    notice_status=notice_status,
                    restore_status="Babblebox did not change the @everyone overwrite here. Later unlocks will only clear Babblebox's lock marker and timer.",
                ),
                alert=False,
            )
            message_parts = [f"{channel.mention} already matched Babblebox's lock restrictions, so Babblebox started tracking it without changing the overwrite."]
            if due_at is not None:
                message_parts.append(f"Timer: **{format_duration_brief(duration_seconds)}**.")
            else:
                message_parts.append("Timer: **manual unlock required**.")
            message_parts.append("Babblebox will only clear its own tracked lock state later.")
            message_parts.append(notice_status)
            message_parts.append("Admin log updated." if log_sent else "Admin log delivery was unavailable.")
            return True, " ".join(message_parts)
        updated_overwrite = self._copy_overwrite(current_overwrite)
        for name in locked_permissions:
            setattr(updated_overwrite, name, False)
        reason = self._lock_reason_text(actor=actor, automatic=False, action="lock")
        try:
            await channel.set_permissions(everyone_role, overwrite=updated_overwrite, reason=reason)
        except (discord.Forbidden, discord.HTTPException):
            return False, "Babblebox could not apply the channel overwrite. No emergency lock was recorded."

        due_at = now + timedelta(seconds=duration_seconds) if duration_seconds is not None else None
        await self.store.upsert_channel_lock(
            self._build_channel_lock_record(
                guild_id=guild.id,
                channel=channel,
                actor_id=actor.id,
                created_at=now,
                due_at=due_at,
                marker_only=False,
                locked_permissions=locked_permissions,
                original_permissions=previous_values,
            )
        )
        self._wake_event.set()

        notice_status = "Notice suppressed for this run."
        if post_notice:
            final_notice = notice_or_message or self.lock_notice_text(guild.id)
            posted, notice_status = await self._post_lock_notice(channel, final_notice)
            if not posted:
                notice_status = "Lock applied, but the notice could not be posted."
        timer_label = format_duration_brief(duration_seconds) if duration_seconds is not None else "Manual unlock required"
        log_sent = await self.send_log(
            guild,
            compiled,
            embed=self._build_lock_log_embed(
                title="Channel Locked",
                description=f"{actor.mention} applied an emergency lock to {channel.mention}.",
                tone="warning",
                channel=channel,
                timer_label=timer_label,
                notice_status=notice_status,
                restore_status="Babblebox will only restore the @everyone flags it changed, and only if they still match the Babblebox lock state.",
            ),
            alert=False,
        )
        message_parts = [f"Locked {channel.mention}."]
        if due_at is not None:
            message_parts.append(f"Timer: **{format_duration_brief(duration_seconds)}**.")
        else:
            message_parts.append("Timer: **manual unlock required**.")
        message_parts.append(notice_status)
        message_parts.append("Admin log updated." if log_sent else "Admin log delivery was unavailable.")
        return True, " ".join(message_parts)

    async def remove_channel_lock(
        self,
        guild: discord.Guild,
        channel,
        *,
        actor: discord.Member | None = None,
        automatic: bool = False,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Emergency lock tools")
        if not self._channel_lock_supported(channel):
            return False, "Babblebox only supports direct emergency locks for normal text channels."
        compiled = self.get_compiled_config(guild.id)
        everyone_role = self._default_role(guild)
        if everyone_role is None:
            return False, "Babblebox could not resolve the server's @everyone role for this unlock."
        record = await self.store.fetch_channel_lock(guild.id, channel.id)
        if record is None:
            current_overwrite = channel.overwrites_for(everyone_role)
            if any(getattr(current_overwrite, name, None) is False for name in LOCK_PERMISSION_NAMES):
                return False, "That channel already matches the lock restrictions, but Babblebox is not tracking an active emergency lock there."
            return False, "Babblebox is not tracking an active emergency lock for that channel."
        marker_only = self._lock_record_is_marker_only(record)
        if not marker_only:
            lock_issue = self._lock_manage_issue(guild, channel)
            if lock_issue is not None:
                return False, lock_issue.detail

        current_overwrite = channel.overwrites_for(everyone_role)
        if marker_only:
            await self.store.delete_channel_lock(guild.id, channel.id)
            restore_notes = "Cleared Babblebox's tracked lock state only. Babblebox did not reopen the @everyone overwrite because it had not changed it."
            timer_label = "Automatic unlock" if automatic else "Manual unlock"
            log_description = (
                f"Babblebox automatically cleared the tracked emergency lock state from {channel.mention}."
                if automatic or actor is None
                else f"{actor.mention} cleared the tracked emergency lock state from {channel.mention}."
            )
            log_sent = await self.send_log(
                guild,
                compiled,
                embed=self._build_lock_log_embed(
                    title="Channel Unlocked",
                    description=log_description,
                    tone="success",
                    channel=channel,
                    timer_label=timer_label,
                    restore_status=restore_notes,
                ),
                alert=False,
            )
            summary = f"Cleared Babblebox's tracked lock state for {channel.mention}. Babblebox did not change the overwrite because it had not locked any @everyone flags here."
            if not self._overwrite_matches_lock_restrictions(current_overwrite):
                summary += " The channel no longer matched the lock restrictions, so no overwrite restore was needed."
            if not log_sent:
                summary += " Admin log delivery was unavailable."
            return True, summary

        tracked_permissions = tuple(record.get("locked_permissions", ()))
        still_locked = [name for name in tracked_permissions if getattr(current_overwrite, name, None) is False]
        if not still_locked:
            await self.store.delete_channel_lock(guild.id, channel.id)
            return True, "Babblebox cleared the stale lock record because the channel was already unlocked manually."

        blocker = self._lock_restore_blocker(channel, record)
        if blocker is not None:
            return False, blocker

        restored_overwrite = self._copy_overwrite(current_overwrite)
        restored_flags: list[str] = []
        preserved_flags: list[str] = []
        original_permissions = dict(record.get("original_permissions", {}))
        for name in tracked_permissions:
            current_value = getattr(current_overwrite, name, None)
            if current_value is False:
                setattr(restored_overwrite, name, original_permissions.get(name))
                restored_flags.append(name)
            else:
                preserved_flags.append(name)
        overwrite_to_apply = None if self._overwrite_is_empty(restored_overwrite) else restored_overwrite
        reason = self._lock_reason_text(actor=actor, automatic=automatic, action="unlock")
        try:
            await channel.set_permissions(everyone_role, overwrite=overwrite_to_apply, reason=reason)
        except (discord.Forbidden, discord.HTTPException):
            return False, "Babblebox could not restore the @everyone overwrite for that channel."

        await self.store.delete_channel_lock(guild.id, channel.id)
        restore_notes = "Restored every Babblebox-applied flag."
        if preserved_flags:
            labels = ", ".join(sorted(name.replace("_", " ") for name in preserved_flags))
            restore_notes = f"Restored the remaining Babblebox-applied flags and preserved manual changes to: {labels}."
        timer_label = "Automatic unlock" if automatic else "Manual unlock"
        log_description = (
            f"Babblebox automatically removed the emergency lock from {channel.mention}."
            if automatic or actor is None
            else f"{actor.mention} removed the emergency lock from {channel.mention}."
        )
        log_sent = await self.send_log(
            guild,
            compiled,
            embed=self._build_lock_log_embed(
                title="Channel Unlocked",
                description=log_description,
                tone="success",
                channel=channel,
                timer_label=timer_label,
                restore_status=restore_notes,
            ),
            alert=False,
        )
        summary = f"Unlocked {channel.mention}. {restore_notes}"
        if not log_sent:
            summary += " Admin log delivery was unavailable."
        return True, summary

    async def remove_timeout(
        self,
        guild: discord.Guild,
        member: discord.Member,
        *,
        actor: discord.Member,
        reason_text: str | None = None,
    ) -> tuple[bool, str]:
        if guild is None or member is None or actor is None:
            return False, "Timeout removal only works inside a server."
        actor_issue = self._timeout_actor_issue(guild, actor, member)
        if actor_issue is not None:
            return False, actor_issue.detail
        timeout_issue = self._timeout_issue(guild, member)
        if timeout_issue is not None:
            return False, timeout_issue.detail
        timed_out_until = self._member_timeout_until(member)
        now = ge.now_utc()
        if timed_out_until is None or timed_out_until <= now:
            return False, f"{member.mention} is not currently timed out."

        compiled = self.get_compiled_config(guild.id)
        cleaned_reason = normalize_plain_text(reason_text) if reason_text is not None else None
        cleaned_reason = cleaned_reason or None
        audit_reason = "Babblebox timeout removed"
        if cleaned_reason:
            audit_reason = f"{audit_reason}: {cleaned_reason}"
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await member.timeout(None, reason=audit_reason)
            embed = ge.make_status_embed(
                "Timeout Removed",
                f"{actor.mention} removed the timeout from {member.mention}.",
                tone="success",
                footer="Babblebox Timeout",
            )
            embed.add_field(
                name="Reason",
                value=cleaned_reason or "No reason provided.",
                inline=False,
            )
            await self.send_log(guild, compiled, embed=embed, alert=False)
            if cleaned_reason:
                return True, f"Removed the timeout from {member.mention}. Reason recorded: {cleaned_reason}"
            return True, f"Removed the timeout from {member.mention}."
        return False, f"Babblebox could not remove the timeout from {member.mention}."

    async def list_review_views(self) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        return await self.store.list_review_views()

    async def list_verification_review_views(self) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        return await self.store.list_verification_review_views()

    async def list_verification_review_queues(self) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        return await self.store.list_verification_review_queues()

    async def current_verification_review_target(self, guild_id: int) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return None
        compiled = self.get_compiled_config(guild_id)
        pending = await self._active_verification_review_rows(guild, compiled)
        return pending[0] if pending else None

    async def get_member_status(self, member: discord.Member) -> dict[str, Any]:
        compiled = self.get_compiled_config(member.guild.id)
        followup = await self.store.fetch_followup(member.guild.id, member.id) if self.storage_ready else None
        candidate = await self.store.fetch_ban_candidate(member.guild.id, member.id) if self.storage_ready else None
        verification = await self.store.fetch_verification_state(member.guild.id, member.id) if self.storage_ready else None
        verified_state, verified_reason = self._verification_status(member, compiled)
        return {
            "followup": followup,
            "candidate": candidate,
            "verification": verification,
            "verified_state": verified_state,
            "verified_reason": verified_reason,
            "followup_exempt_reason": self._followup_exempt_reason(member, compiled),
            "verification_exempt_reason": self._verification_exempt_reason(member, compiled),
        }

    def _bot_member(self, guild: discord.Guild | None):
        if guild is None:
            return None
        me = getattr(guild, "me", None)
        if me is not None:
            return me
        get_member = getattr(guild, "get_member", None)
        bot_user = getattr(self.bot, "user", None)
        if callable(get_member) and bot_user is not None:
            return get_member(getattr(bot_user, "id", 0))
        return None

    def _guild_role(self, guild: discord.Guild | None, role_id: int | None):
        if guild is None or role_id is None:
            return None
        get_role = getattr(guild, "get_role", None)
        return get_role(role_id) if callable(get_role) else None

    def _guild_channel(self, guild: discord.Guild | None, channel_id: int | None):
        if guild is None or channel_id is None:
            return None
        get_channel = getattr(guild, "get_channel", None)
        if callable(get_channel):
            channel = get_channel(channel_id)
            if channel is not None:
                return channel
        get_global = getattr(self.bot, "get_channel", None)
        return get_global(channel_id) if callable(get_global) else None

    def _iter_guild_members(self, guild: discord.Guild):
        members = getattr(guild, "members", [])
        iterable = members.values() if isinstance(members, dict) else members
        for member in iterable:
            if getattr(member, "id", None) is None:
                continue
            if getattr(member, "guild", guild) is not guild:
                continue
            yield member

    def _collect_grouped_member_log(
        self,
        grouped: dict[GroupedAdminLogKey, list[str]],
        key: GroupedAdminLogKey,
        member: discord.Member | discord.abc.User | str,
    ):
        mention = member if isinstance(member, str) else getattr(member, "mention", f"<@{getattr(member, 'id', 0)}>")
        bucket = grouped.setdefault(key, [])
        if mention not in bucket:
            bucket.append(mention)

    def _format_grouped_member_mentions(self, mentions: list[str]) -> str:
        count = len(mentions)
        if count == 0:
            return "Nobody"
        if count == 1:
            return mentions[0]
        if count == 2:
            return f"{mentions[0]} and {mentions[1]}"
        if count <= GROUPED_MEMBER_PREVIEW_LIMIT:
            return f"{', '.join(mentions[:-1])}, and {mentions[-1]}"
        preview = ", ".join(mentions[:GROUPED_MEMBER_PREVIEW_LIMIT])
        remaining = count - GROUPED_MEMBER_PREVIEW_LIMIT
        return f"{preview}, and {remaining} more"

    def _reason_fragment(self, reason_text: str | None, *, fallback: str) -> str:
        cleaned = (reason_text or "").strip().rstrip(".!?")
        return cleaned or fallback

    def _grouped_log_text(self, key: GroupedAdminLogKey, mentions: list[str]) -> str:
        member_text = self._format_grouped_member_mentions(mentions)
        count = len(mentions)
        was_were = "was" if count == 1 else "were"
        reason = self._reason_fragment(key.reason_text, fallback="an unexpected issue occurred")
        dm_status = key.dm_status or "unknown"
        role_mention = key.role_mention or "the configured follow-up role"
        duration_label = key.duration_label or "the configured follow-up window"

        if key.kind == "verification-sync-skip":
            return f"{member_text} {was_were} skipped during verification sync because {reason}."
        if key.kind == "verification-sync-warning-dm-failed":
            return f"Warning DMs failed for {member_text} during verification sync."
        if key.kind == "verification-warning":
            if dm_status == "failed":
                return f"Verification warning DMs failed for {member_text}."
            return f"Babblebox warned {member_text} about pending verification cleanup."
        if key.kind == "verification-warning-skipped":
            return f"{member_text} {was_were} not warned because {reason}."
        if key.kind == "verification-kick-deferred":
            return (
                f"Babblebox warned {member_text} instead of kicking immediately because no prior warning had been recorded. "
                f"DM status: {dm_status}."
            )
        if key.kind == "verification-kick-skipped":
            return f"{member_text} {was_were} not kicked for verification cleanup because {reason}."
        if key.kind == "verification-kick-success":
            return f"Babblebox kicked {member_text} after the verification deadline expired. Final DM status: {dm_status}."
        if key.kind == "followup-auto-remove-skipped":
            return f"Babblebox could not auto-remove {role_mention} from {member_text} because {reason}."
        if key.kind == "followup-auto-remove-success":
            return f"Babblebox auto-removed {role_mention} from {member_text} after {duration_label}."
        return f"{member_text} {was_were} affected by an admin automation outcome."

    def _grouped_admin_log_payload(
        self,
        key: GroupedAdminLogKey,
        mentions: list[str],
    ) -> tuple[str, str, str, str, bool]:
        count = len(mentions)
        description = self._grouped_log_text(key, mentions)
        verification_footer = "Babblebox Admin | Verification cleanup"
        followup_footer = "Babblebox Admin | Returned-after-ban follow-up"

        if key.kind == "verification-warning":
            if key.dm_status == "failed":
                return "Verification Warning Delivery Failed", description, "warning", verification_footer, False
            title = "Verification Warning Sent" if count == 1 else "Verification Warnings Sent"
            return title, description, "warning", verification_footer, False
        if key.kind == "verification-warning-skipped":
            return "Verification Warning Skipped", description, "warning", verification_footer, False
        if key.kind == "verification-kick-deferred":
            return "Verification Kick Deferred", description, "warning", verification_footer, False
        if key.kind == "verification-kick-skipped":
            return "Verification Kick Skipped", description, "warning", verification_footer, False
        if key.kind == "verification-kick-success":
            title = "Member Removed For Verification Cleanup" if count == 1 else "Members Removed For Verification Cleanup"
            return title, description, "danger", verification_footer, False
        if key.kind == "followup-auto-remove-skipped":
            return "Follow-up Role Removal Skipped", description, "warning", followup_footer, False
        if key.kind == "followup-auto-remove-success":
            title = "Follow-up Role Removed" if count == 1 else "Follow-up Roles Removed"
            return title, description, "success", followup_footer, False
        return "Admin Automation Update", description, "info", "Babblebox Admin", False

    def _grouped_admin_log_dedup_key(self, key: GroupedAdminLogKey) -> str | None:
        if key.kind == "verification-warning-skipped":
            return f"{key.kind}:{key.reason_code}"
        if key.kind == "verification-kick-skipped" and key.reason_code.startswith("ambiguous:"):
            return f"{key.kind}:{key.reason_code}"
        return None

    def _render_grouped_issue_lines(
        self,
        grouped: dict[GroupedAdminLogKey, list[str]],
        *,
        limit: int,
    ) -> list[str]:
        lines = [self._grouped_log_text(key, mentions) for key, mentions in grouped.items()]
        if len(lines) <= limit:
            return lines
        remaining = len(lines) - limit
        suffix = "" if remaining == 1 else "s"
        return [*lines[:limit], f"... and {remaining} more grouped issue{suffix}."]

    async def _flush_grouped_admin_logs(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        grouped: dict[GroupedAdminLogKey, list[str]],
    ):
        for key, mentions in grouped.items():
            title, description, tone, footer, alert = self._grouped_admin_log_payload(key, mentions)
            dedup_key = self._grouped_admin_log_dedup_key(key)
            if dedup_key is not None:
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key=dedup_key,
                    message=description,
                    title=title,
                    footer=footer,
                    alert=alert,
                )
                continue
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(title, description, tone=tone, footer=footer),
                alert=alert,
            )

    def _verification_result_code(self, key: VerificationBatchKey) -> str:
        return f"{key.operation}:{key.outcome}:{key.reason_code}"

    def _set_verification_result(self, record: dict[str, Any], key: VerificationBatchKey, *, now: datetime) -> dict[str, Any]:
        updated = dict(record)
        updated["last_result_code"] = self._verification_result_code(key)
        updated["last_result_at"] = serialize_datetime(now)
        return updated

    def _set_verification_notified(self, record: dict[str, Any], key: VerificationBatchKey, *, now: datetime) -> dict[str, Any]:
        updated = dict(record)
        updated["last_notified_code"] = self._verification_result_code(key)
        updated["last_notified_at"] = serialize_datetime(now)
        return updated

    def _verification_notification_signature(self, member_ids: list[int]) -> str:
        joined = ",".join(str(member_id) for member_id in sorted(set(member_ids)))
        return hashlib.sha1(joined.encode("ascii"), usedforsecurity=False).hexdigest()

    def _count_verification_batch_outcome(self, batch: VerificationSweepBatch, guild_id: int, key: VerificationBatchKey):
        counts = batch.counts_by_guild.setdefault(guild_id, {})
        count_key = f"{key.operation}:{key.outcome}"
        counts[count_key] = counts.get(count_key, 0) + 1

    def _collect_verification_batch_outcome(
        self,
        batch: VerificationSweepBatch,
        guild_id: int,
        key: VerificationBatchKey,
        member: discord.Member | discord.abc.User | str,
        *,
        record: dict[str, Any] | None = None,
    ):
        grouped = batch.grouped_by_guild.setdefault(guild_id, {})
        bucket = grouped.setdefault(key, VerificationBatchGroup())
        mention = member if isinstance(member, str) else getattr(member, "mention", f"<@{getattr(member, 'id', 0)}>")
        if mention not in bucket.mentions:
            bucket.mentions.append(mention)
        member_id = getattr(member, "id", None)
        if isinstance(member_id, int) and member_id not in bucket.member_ids:
            bucket.member_ids.append(member_id)
        if record is not None:
            bucket.records.append(dict(record))
        self._count_verification_batch_outcome(batch, guild_id, key)

    def _verification_reason_text(self, reason_code: str, *, detail: str | None = None) -> str:
        mapping = {
            "missing_kick_members": "Babblebox is missing Kick Members",
            "target_above_bot_role": "their top role is at or above Babblebox's",
            "target_is_administrator": "they are administrators",
            "target_is_owner": "they are the server owner",
            "discord_rejected_kick": "Discord rejected the kick",
            "bot_member_unavailable": "Babblebox could not resolve its own server member for kicks",
            "verification_rule_ambiguous": detail or "the verification rule could not be evaluated",
            "review_queued": "they were added to the verification review queue",
            "missing_prior_warning": "no prior warning had been recorded",
            "dm_failed": "DM delivery failed",
            "dm_sent": "DM delivery succeeded",
        }
        return mapping.get(reason_code, detail or reason_code.replace("_", " "))

    def _verification_group_text(self, key: VerificationBatchKey, mentions: list[str]) -> str:
        member_text = self._format_grouped_member_mentions(mentions)
        count = len(mentions)
        was_were = "was" if count == 1 else "were"
        noun = "member" if count == 1 else "members"
        reason = self._reason_fragment(
            key.reason_text or self._verification_reason_text(key.reason_code),
            fallback="an unexpected issue occurred",
        )
        if key.operation == "warning" and key.outcome == "sent":
            if key.reason_code == "dm_failed":
                return f"Warning DMs failed for {member_text}."
            return f"Babblebox warned {member_text} about pending verification cleanup."
        if key.operation == "warning" and key.outcome == "skipped":
            return f"{member_text} {was_were} not warned because {reason}."
        if key.operation == "kick" and key.outcome == "deferred":
            return (
                f"Babblebox warned {member_text} instead of kicking immediately because no prior warning had been recorded. "
                f"DM status: {key.dm_status or 'unknown'}."
            )
        if key.operation == "kick" and key.outcome == "blocked":
            return f"{member_text} {was_were} not kicked because {reason}."
        if key.operation == "kick" and key.outcome == "success":
            return f"Babblebox kicked {member_text} after the verification deadline expired. Final DM status: {key.dm_status or 'unknown'}."
        if key.operation == "review" and key.outcome == "queued":
            if count == 1:
                return f"{member_text} was added to the verification review queue."
            return f"{count} {noun} were added to the verification review queue."
        return f"{member_text} {was_were} affected by verification automation."

    def _verification_summary_lines(self, counts: dict[str, int]) -> list[str]:
        labels = (
            ("warning:sent", "Warnings sent"),
            ("warning:skipped", "Warnings skipped"),
            ("kick:deferred", "Kick deadlines deferred"),
            ("kick:blocked", "Kicks blocked"),
            ("kick:success", "Members kicked"),
            ("review:queued", "Queued for review"),
        )
        lines = []
        for key, label in labels:
            value = counts.get(key, 0)
            if value:
                lines.append(f"{label}: **{value}**")
        return lines or ["No operator-facing verification changes were emitted."]

    def _verification_summary_title(self, run_context: str) -> str:
        if run_context == "startup_resume":
            return "Verification Reconciliation Resumed"
        return "Verification Automation Summary"

    def _verification_summary_description(self, run_context: str) -> str:
        if run_context == "startup_resume":
            return "Babblebox resumed overdue verification reconciliation after startup and only surfaced changes that still matter."
        return "Babblebox processed the current overdue verification sweep and grouped identical outcomes into one calm summary."

    async def _should_notify_verification_group(
        self,
        guild_id: int,
        key: VerificationBatchKey,
        group: VerificationBatchGroup,
        *,
        now: datetime,
    ) -> bool:
        cutoff = now - timedelta(seconds=VERIFICATION_NOTIFICATION_SUPPRESSION_SECONDS)
        result_code = self._verification_result_code(key)
        row_requires_notification = False
        for record in group.records:
            last_code = str(record.get("last_notified_code") or "").strip()
            last_at = deserialize_datetime(record.get("last_notified_at"))
            if last_code != result_code or last_at is None or last_at <= cutoff:
                row_requires_notification = True
                break
        snapshot = await self.store.fetch_verification_notification_snapshot(
            guild_id,
            run_context=key.run_context,
            operation=key.operation,
            outcome=key.outcome,
            reason_code=key.reason_code,
        )
        if snapshot is None:
            return True
        snapshot_time = deserialize_datetime(snapshot.get("notified_at"))
        if snapshot_time is None or snapshot_time <= cutoff:
            return True
        return snapshot.get("signature") != self._verification_notification_signature(group.member_ids) or row_requires_notification

    async def _mark_verification_group_notified(
        self,
        guild_id: int,
        key: VerificationBatchKey,
        group: VerificationBatchGroup,
        *,
        now: datetime,
    ):
        for record in group.records:
            updated = self._set_verification_notified(record, key, now=now)
            await self.store.upsert_verification_state(updated)
        await self.store.upsert_verification_notification_snapshot(
            {
                "guild_id": guild_id,
                "run_context": key.run_context,
                "operation": key.operation,
                "outcome": key.outcome,
                "reason_code": key.reason_code,
                "signature": self._verification_notification_signature(group.member_ids),
                "notified_at": serialize_datetime(now),
            }
        )

    async def _flush_verification_sweep_batch(
        self,
        batch: VerificationSweepBatch,
        *,
        now: datetime,
    ):
        severity_order = {
            ("kick", "blocked"): 0,
            ("warning", "skipped"): 1,
            ("kick", "deferred"): 2,
            ("review", "queued"): 3,
            ("warning", "sent"): 4,
            ("kick", "success"): 5,
        }
        for guild_id, grouped in batch.grouped_by_guild.items():
            guild = self.bot.get_guild(guild_id)
            compiled = self.get_compiled_config(guild_id)
            if guild is None:
                continue
            visible_groups: list[tuple[VerificationBatchKey, VerificationBatchGroup]] = []
            for key, group in grouped.items():
                if await self._should_notify_verification_group(guild_id, key, group, now=now):
                    visible_groups.append((key, group))
            if not visible_groups:
                continue
            visible_groups.sort(key=lambda item: (severity_order.get((item[0].operation, item[0].outcome), 99), item[0].reason_code))
            queue_only = all(key.operation == "review" and key.outcome == "queued" for key, _ in visible_groups)
            if queue_only:
                for key, group in visible_groups:
                    await self._mark_verification_group_notified(guild_id, key, group, now=now)
                continue
            embed = ge.make_status_embed(
                self._verification_summary_title(batch.run_context),
                self._verification_summary_description(batch.run_context),
                tone="warning" if any(key.outcome in {"blocked", "skipped"} for key, _ in visible_groups) else "info",
                footer="Babblebox Admin | Verification cleanup",
            )
            embed.add_field(
                name="Run Summary",
                value="\n".join(self._verification_summary_lines(batch.counts_by_guild.get(guild_id, {}))),
                inline=False,
            )
            lines = [self._verification_group_text(key, group.mentions) for key, group in visible_groups]
            if len(lines) > VERIFICATION_SUMMARY_LINE_LIMIT:
                remaining = len(lines) - VERIFICATION_SUMMARY_LINE_LIMIT
                suffix = "" if remaining == 1 else "s"
                lines = [*lines[:VERIFICATION_SUMMARY_LINE_LIMIT], f"... and {remaining} more grouped outcome{suffix}."]
            embed.add_field(name="Grouped Outcomes", value=ge.join_limited_lines(lines, limit=1024), inline=False)
            await self.send_log(guild, compiled, embed=embed, alert=False)
            for key, group in visible_groups:
                await self._mark_verification_group_notified(guild_id, key, group, now=now)

    def _warning_template_summary(self, compiled: CompiledAdminConfig) -> tuple[str, str]:
        default_text = (
            "You are still waiting on server verification in {guild}. Please finish verification before {deadline_relative}. "
            "If you need help, use {help_channel}."
        )
        source = compiled.warning_template or default_text
        label = "Custom warning DM" if compiled.warning_template else "Default warning DM"
        return label, ge.safe_field_text(" ".join(source.split()), limit=180)

    def _verification_prechecks(
        self,
        guild: discord.Guild,
        *,
        matched_unverified: int = -1,
        blocked_kick_matches: int = 0,
        exact_member_scan: bool = True,
    ) -> list[VerificationPrecheck]:
        compiled = self.get_compiled_config(guild.id)
        me = self._bot_member(guild)
        if me is None:
            return [VerificationPrecheck("blocked", "Babblebox could not resolve its own server member for verification checks.")]

        checks: list[VerificationPrecheck] = []
        if not compiled.verification_enabled:
            checks.append(VerificationPrecheck("blocked", "Verification cleanup is disabled. Turn it on before running a sync."))
        if compiled.verification_role_id is None:
            checks.append(VerificationPrecheck("blocked", "No verification role is configured, so Babblebox cannot tell who counts as unverified."))
        elif self._guild_role(guild, compiled.verification_role_id) is None:
            checks.append(VerificationPrecheck("blocked", "The configured verification role is missing or no longer accessible."))

        if compiled.verification_help_channel_id is None:
            checks.append(
                VerificationPrecheck(
                    "warning",
                    "No verification-help channel is configured, so warning DMs will fall back to a generic help reference.",
                )
            )
        elif self._guild_channel(guild, compiled.verification_help_channel_id) is None:
            checks.append(
                VerificationPrecheck(
                    "warning",
                    f"I cannot access the configured verification-help channel <#{compiled.verification_help_channel_id}>, so warning DMs will fall back to a generic help reference.",
                )
            )

        if compiled.admin_log_channel_id is None:
            checks.append(
                VerificationPrecheck(
                    "warning",
                    (
                        "No admin log channel is configured, so sync and test summaries will only be shown privately to the admin who runs them, and moderator-review deadline mode cannot send Kick, Delay, and Ignore buttons."
                        if compiled.verification_deadline_action == "review"
                        else "No admin log channel is configured, so sync and test summaries will only be shown privately to the admin who runs them."
                    ),
                )
            )
        else:
            channel = self._guild_channel(guild, compiled.admin_log_channel_id)
            if channel is None:
                checks.append(
                    VerificationPrecheck(
                        "warning",
                        (
                            f"I cannot access the configured admin log channel <#{compiled.admin_log_channel_id}>, so moderator-review deadline mode cannot send review buttons there."
                            if compiled.verification_deadline_action == "review"
                            else f"I cannot access the configured admin log channel <#{compiled.admin_log_channel_id}>."
                        ),
                    )
                )
            else:
                perms = channel.permissions_for(me)
                if not getattr(perms, "view_channel", False):
                    checks.append(VerificationPrecheck("warning", f"I cannot view {channel.mention}."))
                if not getattr(perms, "send_messages", False):
                    checks.append(VerificationPrecheck("warning", f"I cannot send messages in {channel.mention}."))
                if not getattr(perms, "embed_links", False):
                    checks.append(VerificationPrecheck("warning", f"I cannot embed messages in {channel.mention}."))

        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "kick_members", False):
            checks.append(
                VerificationPrecheck(
                    "warning",
                    (
                        "Kick enforcement is enabled, but Babblebox is missing Kick Members. "
                        "Sync can still track and warn members, but manual Kick actions from review mode will fail."
                        if compiled.verification_deadline_action == "review"
                        else "Kick enforcement is enabled, but Babblebox is missing Kick Members. Sync can still track and warn members, but later kicks will fail."
                    ),
                )
            )
        elif blocked_kick_matches > 0:
            noun = "member is" if blocked_kick_matches == 1 else "members are"
            checks.append(
                VerificationPrecheck(
                    "warning",
                    (
                        f"{blocked_kick_matches} currently matched {noun} above Babblebox's role or protected by administrator or owner rules, so manual kick actions will still be blocked for them."
                        if compiled.verification_deadline_action == "review"
                        else f"{blocked_kick_matches} currently matched {noun} above Babblebox's role or protected by administrator or owner rules, so later kicks will be blocked for them."
                    ),
                )
            )

        if not exact_member_scan:
            checks.append(
                VerificationPrecheck(
                    "note",
                    "Member cache is incomplete, so preview counts use the currently cached members only and unseen stale rows will not be cleared in this run.",
                )
            )
        checks.append(
            VerificationPrecheck(
                "note",
                "DM delivery is never guaranteed. Members with closed DMs or privacy settings may still fail warning delivery.",
            )
        )
        if matched_unverified == 0:
            checks.append(VerificationPrecheck("note", "No currently cached members match the unverified rule."))
        return checks

    async def build_verification_sync_preview(self, guild: discord.Guild) -> VerificationSyncPreview:
        compiled = self.get_compiled_config(guild.id)
        existing_rows = {
            int(row["user_id"]): row
            for row in await self.store.list_verification_states_for_guild(guild.id)
        } if self.storage_ready else {}
        exact_member_scan = bool(getattr(guild, "chunked", True))
        now = ge.now_utc()
        scanned_members = 0
        matched_unverified = 0
        newly_tracked = 0
        stale_rows_to_clear = 0
        already_tracked = 0
        warnings_due_now = 0
        blocked_kick_matches = 0
        seen_member_ids: set[int] = set()

        for member in self._iter_guild_members(guild):
            scanned_members += 1
            seen_member_ids.add(int(member.id))
            status, _ = self._verification_status(member, compiled)
            existing = existing_rows.get(int(member.id))
            if status == "unverified":
                matched_unverified += 1
                preview_record = existing
                if preview_record is None:
                    newly_tracked += 1
                    preview_record = self._build_verification_state(member, compiled, now=now)
                else:
                    already_tracked += 1
                warning_at = deserialize_datetime(preview_record.get("warning_at")) if preview_record is not None else None
                if preview_record is not None and preview_record.get("warning_sent_at") is None and (warning_at is None or warning_at <= now):
                    warnings_due_now += 1
                if self._kick_hierarchy_issue(guild, member) is not None:
                    blocked_kick_matches += 1
                continue
            if existing is not None:
                stale_rows_to_clear += 1

        if exact_member_scan:
            stale_rows_to_clear += len(set(existing_rows).difference(seen_member_ids))

        warning_template_label, warning_template_preview = self._warning_template_summary(compiled)
        prechecks = tuple(
            self._verification_prechecks(
                guild,
                matched_unverified=matched_unverified,
                blocked_kick_matches=blocked_kick_matches,
                exact_member_scan=exact_member_scan,
            )
        )
        return VerificationSyncPreview(
            scanned_members=scanned_members,
            matched_unverified=matched_unverified,
            newly_tracked=newly_tracked,
            stale_rows_to_clear=stale_rows_to_clear,
            already_tracked=already_tracked,
            warnings_due_now=warnings_due_now,
            blocked_kick_matches=blocked_kick_matches,
            total_existing_rows=len(existing_rows),
            warning_template_label=warning_template_label,
            warning_template_preview=warning_template_preview,
            prechecks=prechecks,
            exact_member_scan=exact_member_scan,
        )

    def get_verification_prechecks(
        self,
        guild: discord.Guild,
        *,
        matched_unverified: int = -1,
        blocked_kick_matches: int = 0,
        exact_member_scan: bool = True,
    ) -> tuple[VerificationPrecheck, ...]:
        return tuple(
            self._verification_prechecks(
                guild,
                matched_unverified=matched_unverified,
                blocked_kick_matches=blocked_kick_matches,
                exact_member_scan=exact_member_scan,
            )
        )

    def get_verification_sync_session(self, guild_id: int) -> VerificationSyncSession | None:
        return self._verification_sync_sessions.get(guild_id)

    async def create_verification_sync_session(
        self,
        guild: discord.Guild,
        *,
        actor_id: int,
        preview: VerificationSyncPreview | None = None,
    ) -> tuple[bool, VerificationSyncSession]:
        async with self._verification_sync_lock:
            existing = self._verification_sync_sessions.get(guild.id)
            if existing is not None and existing.running:
                return False, existing
            session = VerificationSyncSession(
                guild_id=guild.id,
                actor_id=actor_id,
                created_at=ge.now_utc(),
                preview=preview or await self.build_verification_sync_preview(guild),
                running=True,
            )
            self._verification_sync_sessions[guild.id] = session
            return True, session

    async def request_verification_sync_stop(self, guild_id: int) -> bool:
        async with self._verification_sync_lock:
            session = self._verification_sync_sessions.get(guild_id)
            if session is None or not session.running:
                return False
            session.stop_requested = True
            return True

    async def clear_verification_sync_session(self, guild_id: int, session: VerificationSyncSession):
        async with self._verification_sync_lock:
            if self._verification_sync_sessions.get(guild_id) is session:
                self._verification_sync_sessions.pop(guild_id, None)

    def _role_ids_for(self, member: discord.Member | discord.abc.User) -> set[int]:
        return {
            int(role.id)
            for role in getattr(member, "roles", [])
            if getattr(role, "id", None) is not None
        }






























    def _is_staff_member(self, member: discord.Member) -> bool:
        perms = getattr(member, "guild_permissions", None)
        if perms is None:
            return False
        return bool(
            getattr(perms, "administrator", False)
            or getattr(perms, "manage_guild", False)
            or getattr(perms, "manage_roles", False)
            or getattr(perms, "kick_members", False)
            or getattr(perms, "ban_members", False)
            or getattr(perms, "moderate_members", False)
        )

    def _followup_exempt_reason(self, member: discord.Member, compiled: CompiledAdminConfig) -> str | None:
        if member.id in compiled.excluded_user_ids:
            return "Member is explicitly excluded."
        role_ids = self._role_ids_for(member)
        if compiled.excluded_role_ids.intersection(role_ids):
            return "Member has an excluded role."
        if compiled.followup_exempt_staff and compiled.trusted_role_ids.intersection(role_ids):
            return "Member has a trusted role."
        if compiled.followup_exempt_staff and self._is_staff_member(member):
            return "Member has staff permissions."
        return None

    def _verification_exempt_reason(self, member: discord.Member, compiled: CompiledAdminConfig) -> str | None:
        if getattr(member, "bot", False) and compiled.verification_exempt_bots:
            return "Bots are exempt."
        if member.id in compiled.excluded_user_ids:
            return "Member is explicitly excluded."
        role_ids = self._role_ids_for(member)
        if compiled.excluded_role_ids.intersection(role_ids):
            return "Member has an excluded role."
        if compiled.verification_exempt_staff and compiled.trusted_role_ids.intersection(role_ids):
            return "Member has a trusted role."
        if compiled.verification_exempt_staff and self._is_staff_member(member):
            return "Member has staff permissions."
        return None

    def _verification_status(self, member: discord.Member, compiled: CompiledAdminConfig) -> tuple[str, str]:
        exempt_reason = self._verification_exempt_reason(member, compiled)
        if exempt_reason is not None:
            return "exempt", exempt_reason
        if compiled.verification_role_id is None:
            return "ambiguous", "Verification role is not configured."
        role_ids = self._role_ids_for(member)
        has_role = compiled.verification_role_id in role_ids
        if compiled.verification_logic == "must_have_role":
            return ("verified", "Member has the verification role.") if has_role else ("unverified", "Member is missing the verification role.")
        return ("unverified", "Member still has the unverified role.") if has_role else ("verified", "Member does not have the unverified role.")












    def _prune_runtime_state(self, *, now: float):
        self._log_dedup = {
            key: seen_at
            for key, seen_at in self._log_dedup.items()
            if now - seen_at <= LOG_DEDUP_SECONDS
        }
























    def _render_template(
        self,
        template: str | None,
        *,
        member: discord.Member,
        guild: discord.Guild,
        deadline: datetime,
        help_channel: discord.abc.GuildChannel | discord.Thread | None,
        invite_link: str | None,
        final: bool,
    ) -> str:
        default_text = (
            "You are still waiting on server verification in {guild}. Please finish verification before {deadline_relative}. "
            "If you need help, use {help_channel}."
            if not final
            else "You were removed from {guild} because verification was not completed in time."
        )
        text = template or default_text
        replacements = self.verification_template_placeholders(
            member,
            guild=guild,
            deadline=deadline,
            help_channel=help_channel,
            invite_link=invite_link,
            preview=False,
        )
        for placeholder, value in replacements.items():
            text = text.replace(placeholder, value)
        if final and invite_link:
            text = f"{text}\n\nRejoin: {invite_link}"
        return text.strip()

    def verification_template_placeholders(
        self,
        member: discord.Member,
        *,
        guild: discord.Guild,
        deadline: datetime,
        help_channel: discord.abc.GuildChannel | discord.Thread | None,
        invite_link: str | None,
        preview: bool,
    ) -> dict[str, str]:
        return {
            "{member}": ge.display_name_of(member),
            "{guild}": guild.name,
            "{deadline}": ge.format_timestamp(deadline, "f"),
            "{deadline_relative}": ge.format_timestamp(deadline, "R"),
            "{help_channel}": getattr(help_channel, "mention", "the server's verification-help channel"),
            "{invite_link}": invite_link or ("[not set]" if preview else ""),
        }

    def build_warning_embed(self, member: discord.Member, *, guild: discord.Guild, deadline: datetime, compiled: CompiledAdminConfig) -> discord.Embed:
        help_channel = self._guild_channel(guild, compiled.verification_help_channel_id)
        embed = ge.make_status_embed(
            "Verification Reminder",
            self._render_template(
                compiled.warning_template,
                member=member,
                guild=guild,
                deadline=deadline,
                help_channel=help_channel,
                invite_link=compiled.invite_link,
                final=False,
            ),
            tone="warning",
            footer="Babblebox Admin | Verification cleanup",
        )
        return embed

    def build_kick_embed(self, member: discord.Member, *, guild: discord.Guild, deadline: datetime, compiled: CompiledAdminConfig) -> discord.Embed:
        help_channel = self._guild_channel(guild, compiled.verification_help_channel_id)
        embed = ge.make_status_embed(
            "Verification Window Ended",
            self._render_template(
                compiled.kick_template,
                member=member,
                guild=guild,
                deadline=deadline,
                help_channel=help_channel,
                invite_link=compiled.invite_link,
                final=True,
            ),
            tone="danger",
            footer="Babblebox Admin | Verification cleanup",
        )
        return embed

    def build_verification_review_embed(
        self,
        guild: discord.Guild,
        member: discord.Member,
        record: dict[str, Any],
        *,
        compiled: CompiledAdminConfig,
    ) -> discord.Embed:
        kick_at = deserialize_datetime(record.get("kick_at"))
        embed = discord.Embed(
            title="Verification Deadline Review",
            description=(
                f"{member.mention} reached the verification deadline and still matches the configured unverified rule.\n"
                "Kick now, delay the deadline by 24 hours, or ignore this deadline for now."
            ),
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(name="Member", value=f"{ge.display_name_of(member)} (`{member.id}`)", inline=True)
        embed.add_field(name="Rule", value=VERIFICATION_LOGIC_LABELS.get(compiled.verification_logic, "Verification rule"), inline=False)
        if kick_at is not None:
            embed.add_field(name="Deadline Reached", value=f"{ge.format_timestamp(kick_at, 'R')} ({ge.format_timestamp(kick_at, 'f')})", inline=False)
        issue = self._kick_issue(guild, member)
        kick_status = issue.detail if issue is not None else "Kick is currently available if permissions and hierarchy stay the same."
        embed.add_field(name="Kick Check", value=kick_status, inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Verification cleanup")

    def build_verification_review_resolution_embed(self, record: dict[str, Any], *, message: str, success: bool) -> discord.Embed:
        embed = ge.make_status_embed(
            "Verification Review Updated" if success else "Verification Review Failed",
            message,
            tone="success" if success else "warning",
            footer="Babblebox Admin | Verification cleanup",
        )
        embed.add_field(name="Member", value=f"<@{record['user_id']}>", inline=True)
        return embed

    def _verification_review_sort_key(self, record: dict[str, Any]) -> tuple[datetime, datetime, int]:
        fallback = ge.now_utc()
        return (
            deserialize_datetime(record.get("kick_at")) or fallback,
            deserialize_datetime(record.get("joined_at")) or fallback,
            int(record.get("user_id") or 0),
        )

    async def _active_verification_review_rows(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
    ) -> list[dict[str, Any]]:
        pending: list[dict[str, Any]] = []
        for record in await self.store.list_verification_states_for_guild(guild.id):
            if not record.get("review_pending"):
                continue
            member = guild.get_member(int(record["user_id"]))
            if member is None:
                await self.store.delete_verification_state(guild.id, int(record["user_id"]))
                continue
            if not compiled.verification_enabled:
                continue
            if compiled.verification_deadline_action != "review":
                await self.store.upsert_verification_state(self._close_verification_review_record(record))
                continue
            status, _ = self._verification_status(member, compiled)
            if status in {"verified", "exempt"}:
                await self.store.delete_verification_state(guild.id, int(record["user_id"]))
                continue
            if status != "unverified":
                await self.store.upsert_verification_state(self._close_verification_review_record(record))
                continue
            if record.get("review_message_channel_id") is not None or record.get("review_message_id") is not None:
                cleaned = dict(record)
                cleaned["review_message_channel_id"] = None
                cleaned["review_message_id"] = None
                await self.store.upsert_verification_state(cleaned)
                record = cleaned
            pending.append(record)
        pending.sort(key=self._verification_review_sort_key)
        return pending

    def build_verification_review_queue_embed(
        self,
        guild: discord.Guild,
        pending_rows: list[dict[str, Any]],
        *,
        compiled: CompiledAdminConfig,
        note: str | None = None,
    ) -> discord.Embed:
        embed = discord.Embed(
            title="Verification Review Queue",
            description=(
                "Overdue verification cases are queued here. The oldest actionable case is shown first."
                if pending_rows
                else "No pending verification reviews remain."
            ),
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(
            name="Queue",
            value=f"Pending reviews: **{len(pending_rows)}**",
            inline=False,
        )
        if not pending_rows:
            if note:
                embed.add_field(name="Last Update", value=note, inline=False)
            return ge.style_embed(embed, footer="Babblebox Admin | Verification cleanup")
        current = pending_rows[0]
        member = guild.get_member(int(current["user_id"]))
        kick_at = deserialize_datetime(current.get("kick_at"))
        member_label = (
            f"{ge.display_name_of(member)} (`{member.id}`)"
            if member is not None
            else f"<@{current['user_id']}> (`{current['user_id']}`)"
        )
        embed.add_field(name="Current Case", value=member_label, inline=False)
        embed.add_field(
            name="Rule",
            value=VERIFICATION_LOGIC_LABELS.get(compiled.verification_logic, "Verification rule"),
            inline=False,
        )
        if kick_at is not None:
            embed.add_field(
                name="Deadline Reached",
                value=f"{ge.format_timestamp(kick_at, 'R')} ({ge.format_timestamp(kick_at, 'f')})",
                inline=False,
            )
        if member is not None:
            issue = self._kick_issue(guild, member)
            kick_status = issue.detail if issue is not None else "Kick is currently available if permissions and hierarchy stay the same."
        else:
            kick_status = "Kick cannot be checked because the member is no longer cached."
        embed.add_field(name="Kick Check", value=kick_status, inline=False)
        preview_lines: list[str] = []
        for row in pending_rows[:VERIFICATION_QUEUE_PREVIEW_LIMIT]:
            user_id = int(row["user_id"])
            queued_member = guild.get_member(user_id)
            mention = queued_member.mention if queued_member is not None else f"<@{user_id}>"
            deadline = deserialize_datetime(row.get("kick_at"))
            if deadline is not None:
                preview_lines.append(f"{mention} - due {ge.format_timestamp(deadline, 'R')}")
            else:
                preview_lines.append(mention)
        if len(pending_rows) > VERIFICATION_QUEUE_PREVIEW_LIMIT:
            remaining = len(pending_rows) - VERIFICATION_QUEUE_PREVIEW_LIMIT
            suffix = "" if remaining == 1 else "s"
            preview_lines.append(f"... and {remaining} more queued case{suffix}.")
        embed.add_field(name="Backlog Preview", value="\n".join(preview_lines), inline=False)
        if note:
            embed.add_field(name="Last Update", value=note, inline=False)
        return ge.style_embed(embed, footer="Babblebox Admin | Verification cleanup")












    async def _verification_queue_message(
        self,
        channel,
        *,
        message_id: int | None,
    ):
        if not isinstance(message_id, int):
            return None
        fetch_message = getattr(channel, "fetch_message", None)
        if not callable(fetch_message):
            return None
        with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
            return await fetch_message(message_id)
        return None

    def build_verification_review_queue_notice_embed(
        self,
        *,
        title: str,
        message: str,
        tone: str = "info",
    ) -> discord.Embed:
        return ge.make_status_embed(title, message, tone=tone, footer="Babblebox Admin | Verification cleanup")

    async def _retire_verification_review_queue(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        *,
        queue_record: dict[str, Any] | None,
        title: str,
        message: str,
        tone: str = "info",
    ):
        if queue_record is None:
            return
        channel = self._guild_channel(guild, queue_record.get("channel_id"))
        message_obj = await self._verification_queue_message(channel, message_id=queue_record.get("message_id")) if channel is not None else None
        if message_obj is not None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await message_obj.edit(
                    embed=self.build_verification_review_queue_notice_embed(title=title, message=message, tone=tone),
                    view=None,
                )
        await self.store.delete_verification_review_queue(guild.id)

    async def _sync_verification_review_queue(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        *,
        now: datetime,
        note: str | None = None,
        inactive_reason: str | None = None,
    ):
        from babblebox.cogs.admin import VerificationDeadlineReviewView

        pending_rows = await self._active_verification_review_rows(guild, compiled)
        queue_record = await self.store.fetch_verification_review_queue(guild.id)
        if not pending_rows:
            if queue_record is not None:
                if inactive_reason is not None:
                    await self._retire_verification_review_queue(
                        guild,
                        compiled,
                        queue_record=queue_record,
                        title="Verification Review Queue Updated",
                        message=inactive_reason,
                    )
                else:
                    channel = self._guild_channel(guild, queue_record.get("channel_id"))
                    message = await self._verification_queue_message(channel, message_id=queue_record.get("message_id")) if channel is not None else None
                    if message is not None:
                        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                            await message.edit(
                                embed=self.build_verification_review_queue_embed(guild, [], compiled=compiled, note=note),
                                view=None,
                            )
                    await self.store.delete_verification_review_queue(guild.id)
            return
        if compiled.admin_log_channel_id is None:
            await self._retire_verification_review_queue(
                guild,
                compiled,
                queue_record=queue_record,
                title="Verification Review Queue Unavailable",
                message="The shared verification review queue is unavailable until an admin log channel is configured.",
                tone="warning",
            )
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="verification-review-queue-no-log-channel",
                message="Babblebox has verification review backlog but no admin log channel is configured for the shared review queue.",
                alert=False,
            )
            return
        channel = self._guild_channel(guild, compiled.admin_log_channel_id)
        if channel is None:
            await self._retire_verification_review_queue(
                guild,
                compiled,
                queue_record=queue_record,
                title="Verification Review Queue Unavailable",
                message="The shared verification review queue is unavailable until the configured admin log channel is accessible again.",
                tone="warning",
            )
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="verification-review-queue-missing-log-channel",
                message="Babblebox has verification review backlog but could not access the configured admin log channel for the shared review queue.",
                alert=False,
            )
            return
        if queue_record is not None and queue_record.get("channel_id") != channel.id:
            await self._retire_verification_review_queue(
                guild,
                compiled,
                queue_record=queue_record,
                title="Verification Review Queue Moved",
                message=f"The shared verification review queue moved to {channel.mention}.",
            )
            queue_record = None
        current = pending_rows[0]
        view = VerificationDeadlineReviewView(
            guild_id=guild.id,
            user_id=int(current["user_id"]),
            version=int(current.get("review_version", 0) or 0),
        )
        embed = self.build_verification_review_queue_embed(guild, pending_rows, compiled=compiled, note=note)
        message = await self._verification_queue_message(channel, message_id=queue_record.get("message_id") if queue_record else None)
        if message is None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                message = await channel.send(
                    embed=embed,
                    view=view,
                    allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                )
        else:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await message.edit(embed=embed, view=view)
        if message is None:
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="verification-review-queue-send-failed",
                message="Babblebox has verification review backlog but could not create or update the shared review queue message.",
                alert=False,
            )
            return
        await self.store.upsert_verification_review_queue(
            {
                "guild_id": guild.id,
                "channel_id": channel.id,
                "message_id": message.id,
                "updated_at": serialize_datetime(now),
            }
        )
        with contextlib.suppress(Exception):
            self.bot.add_view(view, message_id=message.id)

    async def _reconcile_verification_review_backlog_after_config_change(
        self,
        guild_id: int,
        *,
        before: dict[str, Any],
        after: dict[str, Any],
        changed_fields: set[str],
        requested_fields: set[str],
        force: bool = False,
    ):
        relevant_fields = (changed_fields | requested_fields) & VERIFICATION_QUEUE_RELEVANT_CONFIG_FIELDS
        if not relevant_fields and not force:
            return
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return
        compiled_before = _compile_config(before)
        compiled_after = _compile_config(after)
        now = ge.now_utc()
        active_note = None
        if compiled_after.verification_enabled and compiled_after.verification_deadline_action == "review":
            batch = VerificationSweepBatch(run_context="config_change")
            for record in await self.store.list_verification_states_for_guild(guild_id):
                if record.get("review_pending"):
                    continue
                kick_at = deserialize_datetime(record.get("kick_at"))
                if kick_at is None or kick_at > now:
                    continue
                member = guild.get_member(int(record["user_id"]))
                if member is None:
                    continue
                status, _ = self._verification_status(member, compiled_after)
                if status in {"verified", "exempt"}:
                    await self.store.delete_verification_state(guild_id, member.id)
                    continue
                if status != "unverified":
                    continue
                await self._queue_verification_review(guild, compiled_after, record, now=now, batch=batch)
            if batch.grouped_by_guild:
                await self._flush_verification_sweep_batch(batch, now=now)
            if (
                "admin_log_channel_id" in relevant_fields
                or "verification_deadline_action" in relevant_fields
                or "verification_enabled" in relevant_fields
            ):
                active_note = "Verification review backlog was reconciled after the latest config change."
        inactive_reason = None
        if compiled_before.admin_log_channel_id != compiled_after.admin_log_channel_id and compiled_before.admin_log_channel_id is not None:
            active_note = f"Verification review backlog moved to <#{compiled_after.admin_log_channel_id}>." if compiled_after.admin_log_channel_id is not None else active_note
        if not compiled_after.verification_enabled:
            inactive_reason = "Verification cleanup is disabled, so this review queue is inactive."
        elif compiled_after.verification_deadline_action != "review":
            inactive_reason = "Verification cleanup is no longer using moderator review."
        await self._sync_verification_review_queue(
            guild,
            compiled_after,
            now=now,
            note=active_note,
            inactive_reason=inactive_reason,
        )

    def _close_verification_review_record(self, record: dict[str, Any]) -> dict[str, Any]:
        updated = dict(record)
        updated["review_pending"] = False
        updated["review_version"] = int(updated.get("review_version", 0) or 0) + 1
        updated["review_message_channel_id"] = None
        updated["review_message_id"] = None
        return updated





    def build_followup_review_embed(self, guild: discord.Guild, member: discord.Member, record: dict[str, Any]) -> discord.Embed:
        due_at = deserialize_datetime(record.get("due_at"))
        assigned_at = deserialize_datetime(record.get("assigned_at"))
        embed = discord.Embed(
            title="Follow-up Role Review",
            description=(
                f"{member.mention} returned within 30 days of a ban event and still has the configured follow-up role.\n"
                "Babblebox only knows they returned after a ban event. It does not know the original ban length."
            ),
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(name="Member", value=f"{ge.display_name_of(member)} (`{member.id}`)", inline=True)
        embed.add_field(name="Role", value=f"<@&{record['role_id']}>", inline=True)
        embed.add_field(name="Policy", value=FOLLOWUP_MODE_LABELS.get(record.get("mode", "review"), "Review"), inline=True)
        if assigned_at is not None:
            embed.add_field(name="Assigned", value=f"{ge.format_timestamp(assigned_at, 'R')} ({ge.format_timestamp(assigned_at, 'f')})", inline=False)
        if due_at is not None:
            embed.add_field(name="Review Due", value=f"{ge.format_timestamp(due_at, 'R')} ({ge.format_timestamp(due_at, 'f')})", inline=False)
        embed.add_field(
            name="Actions",
            value="Remove the role now, delay the review, or keep the role without another automatic review.",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Admin | Returned-after-ban follow-up")

    def build_followup_resolution_embed(self, record: dict[str, Any], *, message: str, success: bool) -> discord.Embed:
        embed = ge.make_status_embed(
            "Follow-up Review Updated" if success else "Follow-up Review Failed",
            message,
            tone="success" if success else "warning",
            footer="Babblebox Admin | Returned-after-ban follow-up",
        )
        embed.add_field(name="Member", value=f"<@{record['user_id']}>", inline=True)
        embed.add_field(name="Role", value=f"<@&{record['role_id']}>", inline=True)
        return embed

    def _followup_role_issue(self, guild: discord.Guild, member: discord.Member, role: discord.Role) -> AdminActionIssue | None:
        me = self._bot_member(guild)
        if me is None:
            return AdminActionIssue(
                code="followup-bot-member-missing",
                detail="Babblebox could not resolve its server member for role management.",
                because_text="Babblebox could not resolve its server member for role management",
            )
        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "manage_roles", False):
            return AdminActionIssue(
                code="followup-missing-manage-roles",
                detail="Babblebox is missing Manage Roles.",
                because_text="Babblebox is missing Manage Roles",
            )
        if getattr(role, "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return AdminActionIssue(
                code="followup-role-above-bot",
                detail=f"{role.mention} is at or above Babblebox's top role.",
                because_text="the role is at or above Babblebox's top role",
            )
        if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return AdminActionIssue(
                code="followup-member-above-bot",
                detail="They are at or above Babblebox's top role.",
                because_text="they are at or above Babblebox's top role",
            )
        return None

    def _kick_hierarchy_issue(self, guild: discord.Guild, member: discord.Member) -> AdminActionIssue | None:
        me = self._bot_member(guild)
        if me is None:
            return AdminActionIssue(
                code="bot_member_unavailable",
                detail="Babblebox could not resolve its server member for kicks.",
                because_text="Babblebox could not resolve its server member for kicks",
            )
        if getattr(member.guild_permissions, "administrator", False):
            return AdminActionIssue(
                code="target_is_administrator",
                detail="They are administrators.",
                because_text="they are administrators",
            )
        if getattr(guild, "owner_id", None) == member.id:
            return AdminActionIssue(
                code="target_is_owner",
                detail="They are the server owner.",
                because_text="they are the server owner",
            )
        if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return AdminActionIssue(
                code="target_above_bot_role",
                detail="They are at or above Babblebox's top role.",
                because_text="their top role is at or above Babblebox's",
            )
        return None

    def _kick_issue(self, guild: discord.Guild, member: discord.Member) -> AdminActionIssue | None:
        me = self._bot_member(guild)
        if me is None:
            return AdminActionIssue(
                code="bot_member_unavailable",
                detail="Babblebox could not resolve its server member for kicks.",
                because_text="Babblebox could not resolve its server member for kicks",
            )
        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "kick_members", False):
            return AdminActionIssue(
                code="missing_kick_members",
                detail="Babblebox is missing Kick Members.",
                because_text="Babblebox is missing Kick Members",
            )
        return self._kick_hierarchy_issue(guild, member)

    async def _deliver_verification_warning(
        self,
        guild: discord.Guild,
        member: discord.Member,
        compiled: CompiledAdminConfig,
        record: dict[str, Any],
        *,
        now: datetime,
        log_result: bool,
    ) -> tuple[dict[str, Any], bool]:
        warning_deadline = deserialize_datetime(record.get("kick_at")) or (now + timedelta(seconds=compiled.verification_warning_lead_seconds))
        dm_sent = False
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await member.send(embed=self.build_warning_embed(member, guild=guild, deadline=warning_deadline, compiled=compiled))
            dm_sent = True
        updated_record = dict(record)
        updated_record["warning_sent_at"] = serialize_datetime(now)
        await self.store.upsert_verification_state(updated_record)
        if log_result:
            grouped = {
                GroupedAdminLogKey(
                    kind="verification-warning",
                    reason_code=f"dm-{'sent' if dm_sent else 'failed'}",
                    dm_status="sent" if dm_sent else "failed",
                ): [member.mention]
            }
            await self._flush_grouped_admin_logs(guild, compiled, grouped)
        return updated_record, dm_sent

    def can_ping_alert_role(self, guild: discord.Guild, compiled: CompiledAdminConfig) -> bool:
        if compiled.admin_alert_role_id is None:
            return False
        me = self._bot_member(guild)
        role = self._guild_role(guild, compiled.admin_alert_role_id)
        if me is None or role is None:
            return False
        perms = getattr(me, "guild_permissions", None)
        if getattr(role, "mentionable", False):
            return True
        return bool(perms and getattr(perms, "mention_everyone", False))

    async def send_log(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        *,
        embed: discord.Embed,
        alert: bool = False,
    ) -> bool:
        if compiled.admin_log_channel_id is None:
            return False
        channel = self._guild_channel(guild, compiled.admin_log_channel_id)
        if channel is None:
            return False
        me = self._bot_member(guild)
        if me is None:
            return False
        permissions = channel.permissions_for(me)
        if not all(getattr(permissions, name, False) for name in ("view_channel", "send_messages", "embed_links")):
            return False
        content = None
        if alert and compiled.admin_alert_role_id is not None and self.can_ping_alert_role(guild, compiled):
            content = f"<@&{compiled.admin_alert_role_id}>"
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(
                content=content,
                embed=embed,
                allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
            )
            return True
        return False

    async def log_operability_warning_once(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        *,
        key: str,
        message: str,
        title: str = "Admin Automation Warning",
        footer: str = "Babblebox Admin",
        alert: bool = False,
    ):
        now = asyncio.get_running_loop().time()
        dedup_key = (guild.id, key)
        seen_at = self._log_dedup.get(dedup_key)
        if seen_at is not None and now - seen_at < LOG_DEDUP_SECONDS:
            return
        self._log_dedup[dedup_key] = now
        embed = ge.make_status_embed(title, message, tone="warning", footer=footer)
        sent = await self.send_log(guild, compiled, embed=embed, alert=alert)
        if not sent:
            print(f"Admin automation warning for guild {guild.id}: {message}")








    def _build_verification_state(self, member: discord.Member, compiled: CompiledAdminConfig, *, now: datetime) -> dict[str, Any]:
        joined_at = getattr(member, "joined_at", None) or now
        warning_lead = timedelta(seconds=compiled.verification_warning_lead_seconds)
        kick_after = timedelta(seconds=compiled.verification_kick_after_seconds)
        warning_at = joined_at + kick_after - warning_lead
        kick_at = joined_at + kick_after
        if warning_at <= now:
            warning_at = now
            kick_at = now + warning_lead
        return {
            "guild_id": member.guild.id,
            "user_id": member.id,
            "joined_at": serialize_datetime(joined_at),
            "warning_at": serialize_datetime(warning_at),
            "kick_at": serialize_datetime(kick_at),
            "warning_sent_at": None,
            "extension_count": 0,
            "review_pending": False,
            "review_version": 0,
            "review_message_channel_id": None,
            "review_message_id": None,
            "last_result_code": None,
            "last_result_at": None,
            "last_notified_code": None,
            "last_notified_at": None,
        }

    async def handle_member_ban(self, guild: discord.Guild, user: discord.abc.User):
        if not self.storage_ready:
            return
        compiled = self.get_compiled_config(guild.id)
        if not compiled.followup_enabled or compiled.followup_role_id is None:
            return
        if user.id in compiled.excluded_user_ids:
            return
        now = ge.now_utc()
        await self.store.upsert_ban_candidate(
            {
                "guild_id": guild.id,
                "user_id": user.id,
                "banned_at": serialize_datetime(now),
                "expires_at": serialize_datetime(now + timedelta(days=FOLLOWUP_BAN_RETURN_WINDOW_DAYS)),
            }
        )

    async def handle_member_join(self, member: discord.Member):
        if not self.storage_ready:
            return
        await self._maybe_handle_return_followup(member)
        await self._ensure_verification_state(member, reason="join")

    async def handle_member_remove(self, member: discord.Member):
        if not self.storage_ready:
            return
        existing = await self.store.fetch_verification_state(member.guild.id, member.id)
        await self.store.delete_verification_state(member.guild.id, member.id)
        await self.store.delete_followup(member.guild.id, member.id)
        if existing and existing.get("review_pending"):
            await self._sync_verification_review_queue(
                member.guild,
                self.get_compiled_config(member.guild.id),
                now=ge.now_utc(),
                note=f"<@{member.id}> left the server, so the review queue was refreshed.",
            )







    async def handle_message(self, message: discord.Message):
        if not self.storage_ready or message.guild is None or message.author.bot or message.webhook_id is not None:
            return
        if getattr(message.author, "guild", None) is not message.guild:
            return
        compiled = self.get_compiled_config(message.guild.id)
        if not compiled.verification_enabled or compiled.verification_help_channel_id is None:
            return
        if message.channel.id != compiled.verification_help_channel_id:
            return
        if len(normalize_plain_text(message.content or "")) < HELP_MIN_CONTENT_LEN:
            return
        verification_state = await self.store.fetch_verification_state(message.guild.id, message.author.id)
        if verification_state is None:
            return
        status, _ = self._verification_status(message.author, compiled)
        if status != "unverified":
            review_pending = bool(verification_state.get("review_pending"))
            await self.store.delete_verification_state(message.guild.id, message.author.id)
            if review_pending:
                await self._sync_verification_review_queue(
                    message.guild,
                    compiled,
                    now=ge.now_utc(),
                    note=f"{message.author.mention} no longer needs review, so the queue was refreshed.",
                )
            return
        if int(verification_state.get("extension_count", 0) or 0) >= compiled.verification_max_extensions:
            return
        warning_at = deserialize_datetime(verification_state.get("warning_at"))
        kick_at = deserialize_datetime(verification_state.get("kick_at"))
        extension = timedelta(seconds=compiled.verification_help_extension_seconds)
        if warning_at is not None and verification_state.get("warning_sent_at") is None:
            verification_state["warning_at"] = serialize_datetime(warning_at + extension)
        if kick_at is not None:
            verification_state["kick_at"] = serialize_datetime(kick_at + extension)
        verification_state["extension_count"] = int(verification_state.get("extension_count", 0) or 0) + 1
        queue_note = None
        if verification_state.get("review_pending"):
            verification_state = self._close_verification_review_record(verification_state)
            queue_note = (
                f"{message.author.mention} asked for help in {message.channel.mention}, so their pending review was converted back into a delayed deadline."
            )
        await self.store.upsert_verification_state(verification_state)
        self._wake_event.set()
        if queue_note is not None:
            await self._sync_verification_review_queue(message.guild, compiled, now=ge.now_utc(), note=queue_note)
        help_embed = ge.make_status_embed(
            "Verification Deadline Extended",
            (
                f"{message.author.mention} asked for verification help in {message.channel.mention}, "
                f"so Babblebox extended the deadline by {format_duration_brief(compiled.verification_help_extension_seconds)}."
            ),
            tone="info",
            footer="Babblebox Admin | Verification cleanup",
        )
        await self.send_log(message.guild, compiled, embed=help_embed, alert=False)

    async def _maybe_handle_return_followup(self, member: discord.Member):
        candidate = await self.store.fetch_ban_candidate(member.guild.id, member.id)
        if candidate is None:
            return
        await self.store.delete_ban_candidate(member.guild.id, member.id)
        expires_at = deserialize_datetime(candidate.get("expires_at"))
        if expires_at is None or expires_at <= ge.now_utc():
            return
        compiled = self.get_compiled_config(member.guild.id)
        if not compiled.followup_enabled or compiled.followup_role_id is None:
            return
        exempt_reason = self._followup_exempt_reason(member, compiled)
        if exempt_reason is not None:
            await self.send_log(
                member.guild,
                compiled,
                embed=ge.make_status_embed(
                    "Returned Member Exempt",
                    f"{member.mention} returned within 30 days of a ban event, but Babblebox skipped the follow-up role. {exempt_reason}",
                    tone="info",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return
        role = self._guild_role(member.guild, compiled.followup_role_id)
        if role is None:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key="followup-missing-role",
                message="Babblebox cannot assign the follow-up role because the configured role no longer exists.",
            )
            return
        if role in getattr(member, "roles", []):
            await self.send_log(
                member.guild,
                compiled,
                embed=ge.make_status_embed(
                    "Follow-up Role Already Present",
                    f"{member.mention} returned within 30 days of a ban event, but already had {role.mention}. Babblebox left it unchanged.",
                    tone="info",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return
        issue = self._followup_role_issue(member.guild, member, role)
        if issue is not None:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-assign-{member.id}",
                message=f"Babblebox could not assign {role.mention} to {member.mention}. {issue.detail}",
            )
            return
        assigned = False
        try:
            await member.add_roles(role, reason="Babblebox follow-up after return within 30 days of a ban event.")
            assigned = True
        except (discord.Forbidden, discord.HTTPException):
            assigned = False
        if not assigned:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-assign-http-{member.id}",
                message=f"Babblebox tried to assign {role.mention} to {member.mention}, but Discord did not confirm the role change.",
            )
            return
        assigned_at = ge.now_utc()
        due_at = add_followup_duration(
            assigned_at,
            value=compiled.followup_duration_value,
            unit=compiled.followup_duration_unit,
        )
        await self.store.upsert_followup(
            {
                "guild_id": member.guild.id,
                "user_id": member.id,
                "role_id": role.id,
                "assigned_at": serialize_datetime(assigned_at),
                "due_at": serialize_datetime(due_at),
                "mode": compiled.followup_mode,
                "review_pending": False,
                "review_version": 0,
                "review_message_channel_id": None,
                "review_message_id": None,
            }
        )
        self._wake_event.set()
        await self.send_log(
            member.guild,
            compiled,
            embed=ge.make_status_embed(
                "Follow-up Role Assigned",
                (
                    f"{member.mention} returned within 30 days of a ban event, so Babblebox assigned {role.mention}.\n"
                    f"Next action: {FOLLOWUP_MODE_LABELS[compiled.followup_mode]} after {_followup_duration_label(compiled.followup_duration_value, compiled.followup_duration_unit)}."
                ),
                tone="warning",
                footer="Babblebox Admin | Returned-after-ban follow-up",
            ),
            alert=False,
        )

    async def _ensure_verification_state(self, member: discord.Member, *, reason: str):
        compiled = self.get_compiled_config(member.guild.id)
        if not compiled.verification_enabled:
            return
        status, status_reason = self._verification_status(member, compiled)
        if status in {"verified", "exempt"}:
            await self.store.delete_verification_state(member.guild.id, member.id)
            return
        if status != "unverified":
            if status == "ambiguous":
                await self.log_operability_warning_once(
                    member.guild,
                    compiled,
                    key="verification-ambiguous",
                    message=f"Babblebox cannot evaluate verification cleanup. {status_reason}",
                )
            return
        existing = await self.store.fetch_verification_state(member.guild.id, member.id)
        if existing is not None:
            return
        await self.store.upsert_verification_state(self._build_verification_state(member, compiled, now=ge.now_utc()))
        self._wake_event.set()

    def build_verification_sync_summary_embed(self, summary: VerificationSyncSummary) -> discord.Embed:
        stopped = summary.manually_stopped
        title = "Verification Sync Stopped" if stopped else "Verification Sync Complete"
        description = (
            "Manual verification sync scanned the current member list, updated compact verification state, and processed warning DMs that were already due."
        )
        tone = "warning" if stopped or summary.partial_failure or summary.failed_dm_count else "success"
        embed = ge.make_status_embed(title, description, tone=tone, footer="Babblebox Admin | Verification cleanup")
        embed.add_field(
            name="Run Summary",
            value=(
                f"Scanned members: **{summary.scanned_members}**\n"
                f"Matched unverified: **{summary.matched_unverified}**\n"
                f"Newly tracked: **{summary.tracked_count}**\n"
                f"Stale rows cleared: **{summary.cleared_count}**\n"
                f"Warnings processed: **{summary.warned_count}**\n"
                f"Failed DMs: **{summary.failed_dm_count}**\n"
                f"Skipped without change: **{summary.skipped_count}**\n"
                f"Manually stopped: **{'Yes' if summary.manually_stopped else 'No'}**"
            ),
            inline=False,
        )
        issue_lines = list(summary.issues)
        if summary.partial_failure and summary.partial_failure not in issue_lines:
            issue_lines.append(summary.partial_failure)
        if issue_lines:
            embed.add_field(name="Issues", value=ge.join_limited_lines(issue_lines, limit=1024), inline=False)
        return embed

    async def run_verification_sync_session(
        self,
        guild: discord.Guild,
        session: VerificationSyncSession,
        *,
        progress_callback=None,
    ) -> VerificationSyncSummary:
        compiled = self.get_compiled_config(guild.id)
        if session.preview.blocking_prechecks:
            session.running = False
            session.finished_at = ge.now_utc()
            precheck_issues = tuple(check.message for check in session.preview.prechecks if check.severity in {"blocked", "warning"})
            session.summary = VerificationSyncSummary(
                scanned_members=0,
                matched_unverified=0,
                tracked_count=0,
                cleared_count=0,
                warned_count=0,
                failed_dm_count=0,
                skipped_count=0,
                manually_stopped=False,
                issues=precheck_issues,
                partial_failure="Sync was blocked by configuration or permission prechecks.",
            )
            if progress_callback is not None:
                with contextlib.suppress(Exception):
                    await progress_callback(session, True)
            await self.clear_verification_sync_session(guild.id, session)
            return session.summary

        existing_rows = {
            int(row["user_id"]): row
            for row in await self.store.list_verification_states_for_guild(guild.id)
        }
        seen_member_ids: set[int] = set()
        grouped_runtime_issues: dict[GroupedAdminLogKey, list[str]] = {}
        now = ge.now_utc()
        try:
            for member in self._iter_guild_members(guild):
                if session.stop_requested:
                    break
                member_id = int(member.id)
                session.current_member_id = member_id
                session.scanned_members += 1
                seen_member_ids.add(member_id)
                changed = False
                try:
                    status, status_reason = self._verification_status(member, compiled)
                    existing = existing_rows.get(member_id)
                    if status == "unverified":
                        session.matched_unverified += 1
                        record = existing
                        if record is None:
                            record = self._build_verification_state(member, compiled, now=now)
                            await self.store.upsert_verification_state(record)
                            existing_rows[member_id] = dict(record)
                            session.tracked_count += 1
                            changed = True
                        warning_at = deserialize_datetime(record.get("warning_at")) if record is not None else None
                        if record is not None and record.get("warning_sent_at") is None and (warning_at is None or warning_at <= now):
                            updated_record, dm_sent = await self._deliver_verification_warning(
                                guild,
                                member,
                                compiled,
                                record,
                                now=now,
                                log_result=False,
                            )
                            existing_rows[member_id] = updated_record
                            session.warned_count += 1
                            if not dm_sent:
                                session.failed_dm_count += 1
                                self._collect_grouped_member_log(
                                    grouped_runtime_issues,
                                    GroupedAdminLogKey(
                                        kind="verification-sync-warning-dm-failed",
                                        reason_code="warning-dm-failed",
                                    ),
                                    member,
                                )
                            changed = True
                            await asyncio.sleep(VERIFICATION_SYNC_DM_PACE_SECONDS)
                        if not changed:
                            session.skipped_count += 1
                    else:
                        if existing is not None:
                            await self.store.delete_verification_state(guild.id, member_id)
                            existing_rows.pop(member_id, None)
                            session.cleared_count += 1
                            changed = True
                        elif status == "ambiguous":
                            session.skipped_count += 1
                            self._collect_grouped_member_log(
                                grouped_runtime_issues,
                                GroupedAdminLogKey(
                                    kind="verification-sync-skip",
                                    reason_code=f"ambiguous:{status_reason}",
                                    reason_text=status_reason,
                                ),
                                member,
                            )
                except Exception as exc:
                    session.skipped_count += 1
                    reason_text = f"unexpected {type(exc).__name__}: {exc}" if str(exc).strip() else f"unexpected {type(exc).__name__}"
                    self._collect_grouped_member_log(
                        grouped_runtime_issues,
                        GroupedAdminLogKey(
                            kind="verification-sync-skip",
                            reason_code=f"exception:{type(exc).__name__}:{reason_text}",
                            reason_text=reason_text,
                        ),
                        member,
                    )
                if progress_callback is not None and (changed or session.scanned_members % VERIFICATION_SYNC_PROGRESS_INTERVAL == 0 or session.stop_requested):
                    with contextlib.suppress(Exception):
                        await progress_callback(session, False)
                if session.scanned_members % VERIFICATION_SYNC_YIELD_INTERVAL == 0:
                    await asyncio.sleep(0)

            if not session.stop_requested and session.preview.exact_member_scan:
                for stale_user_id in list(set(existing_rows).difference(seen_member_ids)):
                    await self.store.delete_verification_state(guild.id, stale_user_id)
                    existing_rows.pop(stale_user_id, None)
                    session.cleared_count += 1
                    if progress_callback is not None and session.cleared_count % VERIFICATION_SYNC_PROGRESS_INTERVAL == 0:
                        with contextlib.suppress(Exception):
                            await progress_callback(session, False)
                    if session.cleared_count % VERIFICATION_SYNC_YIELD_INTERVAL == 0:
                        await asyncio.sleep(0)
        except Exception as exc:
            session.partial_failure = f"Unexpected sync error: {exc}"
        finally:
            session.running = False
            session.finished_at = ge.now_utc()
            session.current_member_id = None
            precheck_issues = [check.message for check in session.preview.prechecks if check.severity in {"blocked", "warning"}]
            session.runtime_issues = self._render_grouped_issue_lines(
                grouped_runtime_issues,
                limit=VERIFICATION_SYNC_RUNTIME_ISSUE_LIMIT,
            )
            issues = tuple(dict.fromkeys([*precheck_issues, *session.runtime_issues]))
            session.summary = VerificationSyncSummary(
                scanned_members=session.scanned_members,
                matched_unverified=session.matched_unverified,
                tracked_count=session.tracked_count,
                cleared_count=session.cleared_count,
                warned_count=session.warned_count,
                failed_dm_count=session.failed_dm_count,
                skipped_count=session.skipped_count,
                manually_stopped=bool(session.stop_requested),
                issues=issues,
                partial_failure=session.partial_failure,
            )
            await self.send_log(guild, compiled, embed=self.build_verification_sync_summary_embed(session.summary), alert=False)
            if progress_callback is not None:
                with contextlib.suppress(Exception):
                    await progress_callback(session, True)
            await self.clear_verification_sync_session(guild.id, session)
        return session.summary

    async def sync_verification_guild(self, guild: discord.Guild) -> tuple[int, int]:
        if not self.storage_ready:
            return 0, 0
        compiled = self.get_compiled_config(guild.id)
        existing_rows = {
            int(row["user_id"]): row
            for row in await self.store.list_verification_states_for_guild(guild.id)
        }
        tracked = 0
        cleared = 0
        for member in self._iter_guild_members(guild):
            status, _ = self._verification_status(member, compiled)
            if status == "unverified":
                if member.id not in existing_rows:
                    await self.store.upsert_verification_state(self._build_verification_state(member, compiled, now=ge.now_utc()))
                    tracked += 1
                    self._wake_event.set()
                continue
            if member.id in existing_rows:
                await self.store.delete_verification_state(guild.id, member.id)
                cleared += 1
                self._wake_event.set()
        return tracked, cleared

    async def handle_review_action(
        self,
        *,
        guild_id: int,
        user_id: int,
        version: int,
        action: str,
        actor: discord.Member,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        if action not in REVIEW_ACTION_LABELS:
            return False, "That review action is no longer supported.", None
        record = await self.store.fetch_followup(guild_id, user_id)
        if record is None:
            return False, "That follow-up review is already closed.", None
        if not record.get("review_pending") or int(record.get("review_version", 0) or 0) != version:
            return False, "That review view is stale. Open the latest review message instead.", record
        guild = getattr(actor, "guild", None)
        if guild is None or guild.id != guild_id:
            return False, "This review action must be used inside the correct server.", record
        compiled = self.get_compiled_config(guild_id)
        member = guild.get_member(user_id)
        role = self._guild_role(guild, int(record["role_id"]))
        if action == "remove":
            if member is None or role is None or role not in getattr(member, "roles", []):
                await self.store.delete_followup(guild_id, user_id)
                return True, "The follow-up role was already gone, so Babblebox cleared the pending record.", record
            issue = self._followup_role_issue(guild, member, role)
            if issue is not None:
                return False, issue.detail, record
            try:
                await member.remove_roles(role, reason=f"Babblebox follow-up review action by {ge.display_name_of(actor)}.")
            except (discord.Forbidden, discord.HTTPException):
                return False, "Babblebox could not remove the follow-up role right now.", record
            await self.store.delete_followup(guild_id, user_id)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Follow-up Role Removed",
                    f"{actor.mention} removed {role.mention} from <@{user_id}> during follow-up review.",
                    tone="success",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return True, "The follow-up role was removed.", record

        updated = dict(record)
        updated["review_pending"] = False
        updated["review_version"] = int(updated.get("review_version", 0) or 0) + 1
        updated["review_message_channel_id"] = None
        updated["review_message_id"] = None
        now = ge.now_utc()
        if action == "delay_week":
            updated["due_at"] = serialize_datetime(add_followup_duration(now, value=1, unit="weeks"))
            await self.store.upsert_followup(updated)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Follow-up Review Delayed",
                    f"{actor.mention} delayed follow-up review for <@{user_id}> by 1 week.",
                    tone="info",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return True, "The follow-up review was delayed by 1 week.", updated
        if action == "delay_month":
            updated["due_at"] = serialize_datetime(add_followup_duration(now, value=1, unit="months"))
            await self.store.upsert_followup(updated)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Follow-up Review Delayed",
                    f"{actor.mention} delayed follow-up review for <@{user_id}> by 1 month.",
                    tone="info",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return True, "The follow-up review was delayed by 1 month.", updated
        updated["due_at"] = None
        await self.store.upsert_followup(updated)
        await self.send_log(
            guild,
            compiled,
            embed=ge.make_status_embed(
                "Follow-up Review Dismissed",
                f"{actor.mention} kept the follow-up role on <@{user_id}> and dismissed the automatic review.",
                tone="info",
                footer="Babblebox Admin | Returned-after-ban follow-up",
            ),
            alert=False,
        )
        return True, "The follow-up role was kept without another automatic review.", updated

    async def handle_verification_review_action(
        self,
        *,
        guild_id: int,
        user_id: int,
        version: int,
        action: str,
        actor: discord.Member,
    ) -> tuple[bool, str, dict[str, Any] | None]:
        if action not in VERIFICATION_REVIEW_ACTION_LABELS:
            return False, "That verification review action is no longer supported.", None
        record = await self.store.fetch_verification_state(guild_id, user_id)
        if record is None:
            return False, "That verification review is already closed.", None
        if not record.get("review_pending") or int(record.get("review_version", 0) or 0) != version:
            return False, "That verification review queue view is stale. Refresh the shared queue message instead.", record
        guild = getattr(actor, "guild", None)
        if guild is None or guild.id != guild_id:
            return False, "This verification review action must be used inside the correct server.", record
        compiled = self.get_compiled_config(guild_id)
        member = guild.get_member(user_id)
        if member is None:
            await self.store.delete_verification_state(guild_id, user_id)
            await self._sync_verification_review_queue(
                guild,
                compiled,
                now=ge.now_utc(),
                note=f"<@{user_id}> already left the server, so the queue was refreshed.",
            )
            return True, "That member already left the server, so Babblebox cleared the pending review.", record
        status, status_reason = self._verification_status(member, compiled)
        if status in {"verified", "exempt"}:
            await self.store.delete_verification_state(guild_id, user_id)
            await self._sync_verification_review_queue(
                guild,
                compiled,
                now=ge.now_utc(),
                note=f"{member.mention} no longer needs verification cleanup, so the queue was refreshed.",
            )
            return True, "That member no longer needs verification cleanup, so Babblebox cleared the pending review.", record
        if action == "kick":
            if status != "unverified":
                return False, status_reason, record
            issue = self._kick_issue(guild, member)
            if issue is not None:
                return False, issue.detail, record
            dm_sent = False
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await member.send(
                    embed=self.build_kick_embed(
                        member,
                        guild=guild,
                        deadline=deserialize_datetime(record.get("kick_at")) or ge.now_utc(),
                        compiled=compiled,
                    )
                )
                dm_sent = True
            try:
                await member.kick(reason=f"Babblebox verification cleanup review action by {ge.display_name_of(actor)}.")
            except (discord.Forbidden, discord.HTTPException):
                return False, "Babblebox could not kick that member right now.", record
            await self.store.delete_verification_state(guild_id, user_id)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Verification Review Kick",
                    (
                        f"{actor.mention} kicked <@{user_id}> from verification cleanup review."
                        if dm_sent
                        else f"{actor.mention} kicked <@{user_id}> from verification cleanup review after the final DM could not be delivered."
                    ),
                    tone="success",
                    footer="Babblebox Admin | Verification cleanup",
                ),
                alert=False,
            )
            await self._sync_verification_review_queue(
                guild,
                compiled,
                now=ge.now_utc(),
                note=f"{actor.mention} kicked <@{user_id}> from the verification review queue.",
            )
            return True, "The member was kicked.", record

        updated = self._close_verification_review_record(record)
        now = ge.now_utc()
        if action == "delay":
            updated["kick_at"] = serialize_datetime(now + timedelta(seconds=VERIFICATION_REVIEW_DELAY_SECONDS))
            await self.store.upsert_verification_state(updated)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Verification Review Delayed",
                    f"{actor.mention} delayed verification cleanup review for <@{user_id}> by 24 hours.",
                    tone="info",
                    footer="Babblebox Admin | Verification cleanup",
                ),
                alert=False,
            )
            await self._sync_verification_review_queue(
                guild,
                compiled,
                now=now,
                note=f"{actor.mention} delayed verification cleanup for <@{user_id}> by 24 hours.",
            )
            return True, "The verification review was delayed by 24 hours.", updated

        await self.store.delete_verification_state(guild_id, user_id)
        await self.send_log(
            guild,
            compiled,
            embed=ge.make_status_embed(
                "Verification Review Ignored",
                f"{actor.mention} dismissed verification deadline enforcement for <@{user_id}>.",
                tone="info",
                footer="Babblebox Admin | Verification cleanup",
            ),
            alert=False,
        )
        await self._sync_verification_review_queue(
            guild,
            compiled,
            now=now,
            note=f"{actor.mention} ignored verification deadline enforcement for <@{user_id}>.",
        )
        return True, "The verification deadline was ignored for now.", record





    async def _wait_for_ready_state(self) -> bool:
        while True:
            try:
                await self.bot.wait_until_ready()
                return True
            except RuntimeError:
                if self.bot.is_closed():
                    return False
                await asyncio.sleep(0.5)

    async def _scheduler_loop(self):
        if not await self._wait_for_ready_state():
            return
        while True:
            self._wake_event.clear()
            processed_any = await self._run_sweep()
            if processed_any:
                continue
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=SWEEP_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    async def _refresh_startup_verification_review_queues(self, *, now: datetime):
        guild_ids = {int(guild_id) for guild_id in self._compiled_configs}
        for record in await self.store.list_verification_review_queues():
            guild_ids.add(int(record["guild_id"]))
        for guild_id in guild_ids:
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            compiled = self.get_compiled_config(guild_id)
            if compiled.verification_deadline_action != "review" and not await self.store.fetch_verification_review_queue(guild_id):
                continue
            await self._sync_verification_review_queue(guild, compiled, now=now)

    async def _process_due_channel_locks(self, now: datetime) -> bool:
        processed = False
        while True:
            due_records = await self.store.list_due_channel_locks(now, limit=50)
            if not due_records:
                break
            for record in due_records:
                guild_id = int(record["guild_id"])
                channel_id = int(record["channel_id"])
                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    await self.store.delete_channel_lock(guild_id, channel_id)
                    processed = True
                    continue
                channel = self._guild_channel(guild, channel_id)
                compiled = self.get_compiled_config(guild.id)
                if channel is None:
                    await self.store.delete_channel_lock(guild.id, channel_id)
                    await self.send_log(
                        guild,
                        compiled,
                        embed=ge.make_status_embed(
                            "Channel Lock Record Cleared",
                            f"Babblebox removed a timed lock record for <#{record['channel_id']}> because the channel is gone or inaccessible.",
                            tone="info",
                            footer="Babblebox Lock",
                        ),
                        alert=False,
                    )
                    processed = True
                    continue

                ok, message = await self.remove_channel_lock(guild, channel, actor=None, automatic=True)
                if ok:
                    processed = True
                    continue

                updated = dict(record)
                updated["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                await self.store.upsert_channel_lock(updated)
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key=f"channel-lock-unlock:{channel.id}",
                    title="Channel Unlock Needs Review",
                    message=f"{channel.mention} is still carrying a Babblebox lock because the timed unlock could not complete safely. {message}",
                    footer="Babblebox Lock",
                    alert=False,
                )
                processed = True
        return processed


    async def _run_sweep(self) -> bool:
        if not self.storage_ready:
            return False
        now = ge.now_utc()
        self._prune_runtime_state(now=asyncio.get_running_loop().time())
        run_context = "startup_resume" if self._startup_resume_pending else "scheduled"
        processed = False
        if await self.store.prune_expired_ban_candidates(now, limit=200):
            processed = True
        if await self._process_due_followups(now):
            processed = True
        if await self._process_due_channel_locks(now):
            processed = True
        verification_batch = VerificationSweepBatch(run_context=run_context)
        if await self._process_due_verification_warnings(now, batch=verification_batch):
            processed = True
        if await self._process_due_verification_kicks(now, batch=verification_batch):
            processed = True
        if verification_batch.grouped_by_guild:
            await self._flush_verification_sweep_batch(verification_batch, now=now)
        for guild_id in sorted(verification_batch.queue_refresh_guild_ids):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            await self._sync_verification_review_queue(guild, self.get_compiled_config(guild_id), now=now)
        if self._startup_resume_pending:
            await self._refresh_startup_verification_review_queues(now=now)
            self._startup_resume_pending = False
        return processed


    async def _process_due_followups(self, now: datetime) -> bool:
        processed = False
        grouped_by_guild: dict[int, dict[GroupedAdminLogKey, list[str]]] = {}
        compiled_by_guild: dict[int, CompiledAdminConfig] = {}
        for record in await self.store.list_due_followups(now, limit=FOLLOWUP_REVIEW_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_followup(int(record["guild_id"]), int(record["user_id"]))
                processed = True
                continue
            compiled = self.get_compiled_config(guild.id)
            compiled_by_guild[guild.id] = compiled
            grouped_logs = grouped_by_guild.setdefault(guild.id, {})
            member = guild.get_member(int(record["user_id"]))
            role = self._guild_role(guild, int(record["role_id"]))
            if member is None or role is None or role not in getattr(member, "roles", []):
                await self.store.delete_followup(guild.id, int(record["user_id"]))
                processed = True
                continue
            if record.get("mode") == "auto_remove":
                issue = self._followup_role_issue(guild, member, role)
                if issue is not None:
                    record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                    await self.store.upsert_followup(record)
                    self._collect_grouped_member_log(
                        grouped_logs,
                        GroupedAdminLogKey(
                            kind="followup-auto-remove-skipped",
                            reason_code=issue.code,
                            reason_text=issue.because_text,
                            role_mention=role.mention,
                        ),
                        member,
                    )
                    processed = True
                    continue
                try:
                    await member.remove_roles(role, reason="Babblebox auto-removed an expired follow-up role.")
                except (discord.Forbidden, discord.HTTPException):
                    record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                    await self.store.upsert_followup(record)
                    self._collect_grouped_member_log(
                        grouped_logs,
                        GroupedAdminLogKey(
                            kind="followup-auto-remove-skipped",
                            reason_code="discord-rejected",
                            reason_text="Discord rejected the change",
                            role_mention=role.mention,
                        ),
                        member,
                    )
                    processed = True
                    continue
                await self.store.delete_followup(guild.id, member.id)
                self._collect_grouped_member_log(
                    grouped_logs,
                    GroupedAdminLogKey(
                        kind="followup-auto-remove-success",
                        reason_code=f"removed:{role.id}",
                        role_mention=role.mention,
                        duration_label=_followup_duration_label(compiled.followup_duration_value, compiled.followup_duration_unit),
                    ),
                    member,
                )
                processed = True
                continue
            await self._send_followup_review_alert(guild, compiled, member, role, record, now=now)
            processed = True
        for guild_id, grouped_logs in grouped_by_guild.items():
            guild = self.bot.get_guild(guild_id)
            compiled = compiled_by_guild.get(guild_id)
            if guild is None or compiled is None:
                continue
            await self._flush_grouped_admin_logs(guild, compiled, grouped_logs)
        return processed

    async def _send_followup_review_alert(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        member: discord.Member,
        role: discord.Role,
        record: dict[str, Any],
        *,
        now: datetime,
    ):
        from babblebox.cogs.admin import FollowupReviewView

        if compiled.admin_log_channel_id is None:
            record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
            await self.store.upsert_followup(record)
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="followup-review-no-log-channel",
                message="Babblebox reached a follow-up review deadline but no admin log channel is configured.",
            )
            return
        channel = self._guild_channel(guild, compiled.admin_log_channel_id)
        if channel is None:
            record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
            await self.store.upsert_followup(record)
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="followup-review-missing-log-channel",
                message="Babblebox reached a follow-up review deadline but could not access the configured admin log channel.",
            )
            return
        next_version = int(record.get("review_version", 0) or 0) + 1
        view = FollowupReviewView(guild_id=guild.id, user_id=member.id, version=next_version)
        try:
            message = await channel.send(
                embed=self.build_followup_review_embed(guild, member, record),
                view=view,
                allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
            )
        except (discord.Forbidden, discord.HTTPException):
            record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
            await self.store.upsert_followup(record)
            await self.log_operability_warning_once(
                guild,
                compiled,
                key="followup-review-send-failed",
                message="Babblebox reached a follow-up review deadline but could not send the review alert to the admin log channel.",
            )
            return
        record["review_pending"] = True
        record["review_version"] = next_version
        record["review_message_channel_id"] = channel.id
        record["review_message_id"] = message.id
        await self.store.upsert_followup(record)
        with contextlib.suppress(Exception):
            self.bot.add_view(view, message_id=message.id)

    async def _queue_verification_review(
        self,
        guild: discord.Guild,
        compiled: CompiledAdminConfig,
        record: dict[str, Any],
        *,
        now: datetime,
        batch: VerificationSweepBatch,
    ):
        updated = dict(record)
        updated["review_pending"] = True
        updated["review_version"] = int(updated.get("review_version", 0) or 0) + 1
        updated["review_message_channel_id"] = None
        updated["review_message_id"] = None
        key = VerificationBatchKey(
            run_context=batch.run_context,
            operation="review",
            outcome="queued",
            reason_code="review_queued",
            reason_text="they were added to the verification review queue",
        )
        updated = self._set_verification_result(updated, key, now=now)
        await self.store.upsert_verification_state(updated)
        member = guild.get_member(int(updated["user_id"])) or f"<@{updated['user_id']}>"
        self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
        batch.queue_refresh_guild_ids.add(guild.id)

    async def _process_due_verification_warnings(
        self,
        now: datetime,
        *,
        batch: VerificationSweepBatch | None = None,
    ) -> bool:
        owned_batch = batch is None
        batch = batch or VerificationSweepBatch(run_context="scheduled")
        processed = False
        for record in await self.store.list_due_verification_warnings(now, limit=VERIFICATION_BATCH_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_verification_state(int(record["guild_id"]), int(record["user_id"]))
                processed = True
                continue
            if self.get_verification_sync_session(guild.id) is not None:
                continue
            member = guild.get_member(int(record["user_id"]))
            if member is None:
                await self.store.delete_verification_state(guild.id, int(record["user_id"]))
                processed = True
                continue
            compiled = self.get_compiled_config(guild.id)
            if not compiled.verification_enabled:
                await self.store.delete_verification_state(guild.id, member.id)
                processed = True
                continue
            status, status_reason = self._verification_status(member, compiled)
            if status in {"verified", "exempt"}:
                await self.store.delete_verification_state(guild.id, member.id)
                processed = True
                continue
            if status != "unverified":
                key = VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="warning",
                    outcome="skipped",
                    reason_code="verification_rule_ambiguous",
                    reason_text=status_reason,
                )
                updated = self._set_verification_result(record, key, now=now)
                await self.store.upsert_verification_state(updated)
                self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
                processed = True
                continue
            reason_code = "dm_sent"
            reason_text = None
            outcome = "sent"
            _, dm_sent = await self._deliver_verification_warning(
                guild,
                member,
                compiled,
                record,
                now=now,
                log_result=False,
            )
            if not dm_sent:
                reason_code = "dm_failed"
            key = VerificationBatchKey(
                run_context=batch.run_context,
                operation="warning",
                outcome=outcome,
                reason_code=reason_code,
                reason_text=reason_text,
                dm_status="sent" if dm_sent else "failed",
            )
            updated = await self.store.fetch_verification_state(guild.id, member.id)
            if updated is not None:
                updated = self._set_verification_result(updated, key, now=now)
                await self.store.upsert_verification_state(updated)
            self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
            processed = True
        if owned_batch and batch.grouped_by_guild:
            await self._flush_verification_sweep_batch(batch, now=now)
        return processed

    async def _process_due_verification_kicks(
        self,
        now: datetime,
        *,
        batch: VerificationSweepBatch | None = None,
    ) -> bool:
        owned_batch = batch is None
        batch = batch or VerificationSweepBatch(run_context="scheduled")
        processed = False
        for record in await self.store.list_due_verification_kicks(now, limit=VERIFICATION_BATCH_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_verification_state(int(record["guild_id"]), int(record["user_id"]))
                processed = True
                continue
            if self.get_verification_sync_session(guild.id) is not None:
                continue
            member = guild.get_member(int(record["user_id"]))
            if member is None:
                await self.store.delete_verification_state(guild.id, int(record["user_id"]))
                processed = True
                continue
            compiled = self.get_compiled_config(guild.id)
            if not compiled.verification_enabled:
                await self.store.delete_verification_state(guild.id, member.id)
                processed = True
                continue
            status, status_reason = self._verification_status(member, compiled)
            if status in {"verified", "exempt"}:
                await self.store.delete_verification_state(guild.id, member.id)
                processed = True
                continue
            if status != "unverified":
                key = VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="kick",
                    outcome="blocked",
                    reason_code="verification_rule_ambiguous",
                    reason_text=status_reason,
                )
                updated = self._set_verification_result(record, key, now=now)
                await self.store.upsert_verification_state(updated)
                self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
                processed = True
                continue
            if record.get("warning_sent_at") is None and compiled.verification_warning_lead_seconds > 0:
                updated = dict(record)
                updated["warning_at"] = serialize_datetime(now)
                updated["warning_sent_at"] = serialize_datetime(now)
                updated["kick_at"] = serialize_datetime(now + timedelta(seconds=compiled.verification_warning_lead_seconds))
                dm_sent = False
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await member.send(
                        embed=self.build_warning_embed(
                            member,
                            guild=guild,
                            deadline=deserialize_datetime(updated["kick_at"]) or (now + timedelta(seconds=compiled.verification_warning_lead_seconds)),
                            compiled=compiled,
                        )
                    )
                    dm_sent = True
                key = VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="kick",
                    outcome="deferred",
                    reason_code="missing_prior_warning",
                    dm_status="sent" if dm_sent else "failed",
                )
                updated = self._set_verification_result(updated, key, now=now)
                await self.store.upsert_verification_state(updated)
                self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
                processed = True
                continue
            if compiled.verification_deadline_action == "review":
                await self._queue_verification_review(guild, compiled, record, now=now, batch=batch)
                processed = True
                continue
            issue = self._kick_issue(guild, member)
            if issue is not None:
                updated = dict(record)
                updated["kick_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                key = VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="kick",
                    outcome="blocked",
                    reason_code=issue.code,
                    reason_text=issue.because_text,
                )
                updated = self._set_verification_result(updated, key, now=now)
                await self.store.upsert_verification_state(updated)
                self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
                processed = True
                continue
            dm_sent = False
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await member.send(
                    embed=self.build_kick_embed(
                        member,
                        guild=guild,
                        deadline=deserialize_datetime(record.get("kick_at")) or now,
                        compiled=compiled,
                    )
                )
                dm_sent = True
            try:
                await member.kick(reason="Babblebox verification cleanup timer expired.")
            except (discord.Forbidden, discord.HTTPException):
                updated = dict(record)
                updated["kick_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                key = VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="kick",
                    outcome="blocked",
                    reason_code="discord_rejected_kick",
                    reason_text="Discord rejected the kick",
                )
                updated = self._set_verification_result(updated, key, now=now)
                await self.store.upsert_verification_state(updated)
                self._collect_verification_batch_outcome(batch, guild.id, key, member, record=updated)
                processed = True
                continue
            await self.store.delete_verification_state(guild.id, member.id)
            self._collect_verification_batch_outcome(
                batch,
                guild.id,
                VerificationBatchKey(
                    run_context=batch.run_context,
                    operation="kick",
                    outcome="success",
                    reason_code="dm_sent" if dm_sent else "dm_failed",
                    dm_status="sent" if dm_sent else "failed",
                ),
                member,
            )
            processed = True
        if owned_batch:
            if batch.grouped_by_guild:
                await self._flush_verification_sweep_batch(batch, now=now)
            for guild_id in sorted(batch.queue_refresh_guild_ids):
                guild = self.bot.get_guild(guild_id)
                if guild is None:
                    continue
                await self._sync_verification_review_queue(guild, self.get_compiled_config(guild_id), now=now)
        return processed
