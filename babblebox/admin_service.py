from __future__ import annotations

import asyncio
import calendar
import contextlib
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.admin_store import (
    AdminStorageUnavailable,
    AdminStore,
    VALID_CHANNEL_LOCK_PERMISSION_NAMES,
    VALID_FOLLOWUP_MODES,
    default_admin_config,
    normalize_admin_config,
)
from babblebox.text_safety import normalize_plain_text
from babblebox.utility_helpers import deserialize_datetime, format_duration_brief, parse_duration_string, serialize_datetime


SWEEP_INTERVAL_SECONDS = 60.0
FOLLOWUP_BAN_RETURN_WINDOW_DAYS = 30
FOLLOWUP_REVIEW_LIMIT = 25
LOG_DEDUP_SECONDS = 3600.0
OPERATION_BACKOFF_SECONDS = 3600
EXCLUSION_LIMIT = 20
LOCK_MAX_DURATION_SECONDS = 30 * 24 * 3600
LOCK_NOTICE_MAX_LEN = 700
GROUPED_MEMBER_PREVIEW_LIMIT = 3
CONFIG_UNCHANGED = object()

FOLLOWUP_MODE_LABELS = {"auto_remove": "Auto-remove", "review": "Moderator review"}
REVIEW_ACTION_LABELS = {
    "remove": "Remove role now",
    "delay_week": "Delay 1 week",
    "delay_month": "Delay 1 month",
    "keep": "Keep role for now",
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

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompiledAdminConfig:
    guild_id: int
    followup_enabled: bool
    followup_role_id: int | None
    followup_mode: str
    followup_duration_value: int
    followup_duration_unit: str
    admin_log_channel_id: int | None
    admin_alert_role_id: int | None
    lock_notice_template: str | None
    lock_admin_only: bool
    excluded_user_ids: frozenset[int]
    excluded_role_ids: frozenset[int]
    trusted_role_ids: frozenset[int]
    followup_exempt_staff: bool


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


def _compile_config(raw: dict[str, Any]) -> CompiledAdminConfig:
    return CompiledAdminConfig(
        guild_id=int(raw["guild_id"]),
        followup_enabled=bool(raw["followup_enabled"]),
        followup_role_id=raw["followup_role_id"],
        followup_mode=raw["followup_mode"],
        followup_duration_value=int(raw["followup_duration_value"]),
        followup_duration_unit=raw["followup_duration_unit"],
        admin_log_channel_id=raw["admin_log_channel_id"],
        admin_alert_role_id=raw["admin_alert_role_id"],
        lock_notice_template=raw["lock_notice_template"],
        lock_admin_only=bool(raw["lock_admin_only"]),
        excluded_user_ids=frozenset(int(value) for value in raw.get("excluded_user_ids", [])),
        excluded_role_ids=frozenset(int(value) for value in raw.get("excluded_role_ids", [])),
        trusted_role_ids=frozenset(int(value) for value in raw.get("trusted_role_ids", [])),
        followup_exempt_staff=bool(raw["followup_exempt_staff"]),
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


class AdminService:
    def __init__(self, bot: commands.Bot, store: AdminStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        self.storage_backend_preference = (
            getattr(store, "backend_preference", None)
            or (os.getenv("ADMIN_STORAGE_BACKEND", "").strip() or "postgres")
        ).strip().lower()
        if store is not None:
            self.store = store
        else:
            try:
                self.store = AdminStore()
                self.storage_backend_preference = getattr(self.store, "backend_preference", self.storage_backend_preference)
            except AdminStorageUnavailable as exc:
                LOGGER.warning("Admin storage constructor failed: %s", exc)
                self.store = AdminStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._compiled_configs: dict[int, CompiledAdminConfig] = {}
        self._log_dedup: dict[tuple[int, str], float] = {}

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            LOGGER.warning("Admin storage unavailable: %s", self._startup_storage_error)
            return False
        try:
            await self.store.load()
        except AdminStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            LOGGER.warning("Admin storage unavailable: %s", exc)
            return False
        self.storage_ready = True
        self.storage_error = None
        await self._rebuild_config_cache()
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
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Admin systems")
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
        self._wake_event.set()
        return True, success_message

    def _validate_config(self, config: dict[str, Any]) -> str | None:
        if config["followup_mode"] not in VALID_FOLLOWUP_MODES:
            return "Follow-up mode must be `auto_remove` or `review`."
        if config["followup_duration_unit"] == "months" and config["followup_duration_value"] > 12:
            return "Follow-up month durations can be at most 12 months."
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
        )

    async def set_exemption_toggle(self, guild_id: int, field: str, enabled: bool) -> tuple[bool, str]:
        if field != "followup_exempt_staff":
            return False, "Unknown exemption toggle."
        label = field.replace("_", " ")
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__(field, bool(enabled)),
            success_message=f"{label.title()} is now {'enabled' if enabled else 'disabled'}.",
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

    def _exception_detail(self, exc: Exception) -> str:
        detail = str(exc).strip()
        return detail or type(exc).__name__

    async def _rollback_lock_overwrite(
        self,
        channel,
        everyone_role: discord.Role,
        original_overwrite: discord.PermissionOverwrite,
        *,
        actor: discord.Member,
    ) -> bool:
        overwrite_to_apply = None if self._overwrite_is_empty(original_overwrite) else original_overwrite
        try:
            await channel.set_permissions(
                everyone_role,
                overwrite=overwrite_to_apply,
                reason=self._lock_reason_text(actor=actor, automatic=False, action="lock rollback"),
            )
        except (discord.Forbidden, discord.HTTPException):
            return False
        return True

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
                        try:
                            await self.store.upsert_channel_lock(current_record)
                        except Exception as exc:
                            return False, f"{channel.mention} stayed locked, but Babblebox could not refresh the tracked timer/state. {self._exception_detail(exc)}"
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
                    try:
                        await self.store.upsert_channel_lock(current_record)
                    except Exception as exc:
                        return False, f"{channel.mention} stayed locked, but Babblebox could not refresh the emergency lock timer/state. {self._exception_detail(exc)}"
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
            try:
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
            except Exception as exc:
                return False, f"{channel.mention} already matched Babblebox's lock restrictions, but Babblebox could not start tracking that state. {self._exception_detail(exc)}"
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
        try:
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
        except Exception as exc:
            rolled_back = await self._rollback_lock_overwrite(channel, everyone_role, current_overwrite, actor=actor)
            if rolled_back:
                return False, f"Babblebox rolled the channel overwrite back because it could not record the emergency lock. {self._exception_detail(exc)}"
            return False, f"Babblebox locked {channel.mention}, but could not record the emergency lock or restore the overwrite automatically. Review the @everyone overwrite manually. {self._exception_detail(exc)}"
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

    async def get_member_status(self, member: discord.Member) -> dict[str, Any]:
        compiled = self.get_compiled_config(member.guild.id)
        followup = await self.store.fetch_followup(member.guild.id, member.id) if self.storage_ready else None
        candidate = await self.store.fetch_ban_candidate(member.guild.id, member.id) if self.storage_ready else None
        return {
            "followup": followup,
            "candidate": candidate,
            "followup_exempt_reason": self._followup_exempt_reason(member, compiled),
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
        role_mention = key.role_mention or "the configured follow-up role"
        duration_label = key.duration_label or "the configured follow-up window"
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
        followup_footer = "Babblebox Admin | Returned-after-ban follow-up"

        if key.kind == "followup-auto-remove-skipped":
            return "Follow-up Role Removal Skipped", description, "warning", followup_footer, False
        if key.kind == "followup-auto-remove-success":
            title = "Follow-up Role Removed" if count == 1 else "Follow-up Roles Removed"
            return title, description, "success", followup_footer, False
        return "Admin Automation Update", description, "info", "Babblebox Admin", False

    def _grouped_admin_log_dedup_key(self, key: GroupedAdminLogKey) -> str | None:
        return None

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












    def _prune_runtime_state(self, *, now: float):
        self._log_dedup = {
            key: seen_at
            for key, seen_at in self._log_dedup.items()
            if now - seen_at <= LOG_DEDUP_SECONDS
        }











































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
            LOGGER.warning("Admin automation warning: guild_id=%s note=%s", guild.id, message)









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

    async def handle_member_remove(self, member: discord.Member):
        if not self.storage_ready:
            return
        await self.store.delete_followup(member.guild.id, member.id)

    async def _maybe_handle_return_followup(self, member: discord.Member):
        candidate = await self.store.fetch_ban_candidate(member.guild.id, member.id)
        if candidate is None:
            return
        now = ge.now_utc()
        expires_at = deserialize_datetime(candidate.get("expires_at"))
        compiled = self.get_compiled_config(member.guild.id)
        if expires_at is None or expires_at <= now:
            await self.store.delete_ban_candidate(member.guild.id, member.id)
            await self.send_log(
                member.guild,
                compiled,
                embed=ge.make_status_embed(
                    "Return Follow-up Window Expired",
                    f"{member.mention} returned after the 30-day ban-return follow-up window, so Babblebox cleared the stale candidate.",
                    tone="info",
                    footer="Babblebox Admin | Returned-after-ban follow-up",
                ),
                alert=False,
            )
            return
        if not compiled.followup_enabled:
            await self.store.delete_ban_candidate(member.guild.id, member.id)
            return
        if compiled.followup_role_id is None:
            await self.store.delete_ban_candidate(member.guild.id, member.id)
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key="followup-role-not-configured",
                title="Follow-up Role Missing",
                message=f"{member.mention} returned within 30 days of a ban event, but Babblebox skipped the follow-up because no role is configured.",
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        exempt_reason = self._followup_exempt_reason(member, compiled)
        if exempt_reason is not None:
            await self.store.delete_ban_candidate(member.guild.id, member.id)
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
            await self.store.delete_ban_candidate(member.guild.id, member.id)
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key="followup-missing-role",
                title="Follow-up Role Missing",
                message="Babblebox cannot assign the follow-up role because the configured role no longer exists.",
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        existing_followup = await self.store.fetch_followup(member.guild.id, member.id)
        if role in getattr(member, "roles", []):
            if existing_followup is None:
                assigned_at = ge.now_utc()
                due_at = add_followup_duration(
                    assigned_at,
                    value=compiled.followup_duration_value,
                    unit=compiled.followup_duration_unit,
                )
                try:
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
                except Exception as exc:
                    await self.log_operability_warning_once(
                        member.guild,
                        compiled,
                        key=f"followup-persist-resume-{member.id}",
                        title="Follow-up Persistence Failed",
                        message=(
                            f"{member.mention} returned within 30 days of a ban event and already had {role.mention}, "
                            f"but Babblebox could not rebuild the follow-up timer/review state. {self._exception_detail(exc)} "
                            "Babblebox kept the return candidate so a later retry is still possible."
                        ),
                        footer="Babblebox Admin | Returned-after-ban follow-up",
                    )
                    return
                await self.store.delete_ban_candidate(member.guild.id, member.id)
                self._wake_event.set()
                await self.send_log(
                    member.guild,
                    compiled,
                    embed=ge.make_status_embed(
                        "Follow-up Tracking Resumed",
                        (
                            f"{member.mention} returned within 30 days of a ban event and already had {role.mention}, "
                            "so Babblebox rebuilt the follow-up timer/review state.\n"
                            f"Next action: {FOLLOWUP_MODE_LABELS[compiled.followup_mode]} after "
                            f"{_followup_duration_label(compiled.followup_duration_value, compiled.followup_duration_unit)}."
                        ),
                        tone="warning",
                        footer="Babblebox Admin | Returned-after-ban follow-up",
                    ),
                    alert=False,
                )
                return
            await self.store.delete_ban_candidate(member.guild.id, member.id)
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
            await self.store.delete_ban_candidate(member.guild.id, member.id)
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-assign-{member.id}",
                title="Follow-up Assignment Blocked",
                message=f"Babblebox could not assign {role.mention} to {member.mention}. {issue.detail}",
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        try:
            await member.add_roles(role, reason="Babblebox follow-up after return within 30 days of a ban event.")
        except (discord.Forbidden, discord.HTTPException) as exc:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-assign-http-{member.id}",
                title="Follow-up Assignment Failed",
                message=(
                    f"Babblebox tried to assign {role.mention} to {member.mention}, but Discord did not confirm the role change. "
                    f"{self._exception_detail(exc)} Babblebox kept the return candidate so a later retry is still possible."
                ),
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        except Exception as exc:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-assign-unexpected-{member.id}",
                title="Follow-up Assignment Failed",
                message=(
                    f"Babblebox hit an unexpected error while assigning {role.mention} to {member.mention}. "
                    f"{self._exception_detail(exc)} Babblebox kept the return candidate so a later retry is still possible."
                ),
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        assigned_at = ge.now_utc()
        due_at = add_followup_duration(
            assigned_at,
            value=compiled.followup_duration_value,
            unit=compiled.followup_duration_unit,
        )
        try:
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
        except Exception as exc:
            await self.log_operability_warning_once(
                member.guild,
                compiled,
                key=f"followup-persist-{member.id}",
                title="Follow-up Persistence Failed",
                message=(
                    f"Babblebox assigned {role.mention} to {member.mention}, but could not persist the follow-up timer/review state. "
                    f"{self._exception_detail(exc)} Babblebox kept the return candidate so a later retry is still possible."
                ),
                footer="Babblebox Admin | Returned-after-ban follow-up",
            )
            return
        await self.store.delete_ban_candidate(member.guild.id, member.id)
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
        processed = False
        if await self.store.prune_expired_ban_candidates(now, limit=200):
            processed = True
        if await self._process_due_followups(now):
            processed = True
        if await self._process_due_channel_locks(now):
            processed = True
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

