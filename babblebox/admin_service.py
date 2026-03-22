from __future__ import annotations

import asyncio
import calendar
import contextlib
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.admin_store import (
    AdminStorageUnavailable,
    AdminStore,
    VALID_FOLLOWUP_MODES,
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

FOLLOWUP_MODE_LABELS = {"auto_remove": "Auto-remove", "review": "Moderator review"}
VERIFICATION_LOGIC_LABELS = {
    "must_have_role": "Unverified if member DOES NOT have this role",
    "must_not_have_role": "Unverified if member DOES have this role",
}
REVIEW_ACTION_LABELS = {
    "remove": "Remove role now",
    "delay_week": "Delay 1 week",
    "delay_month": "Delay 1 month",
    "keep": "Keep role for now",
}
FOLLOWUP_DURATION_RE = re.compile(r"(?ix)^\s*(\d+)\s*(d|day|days|w|week|weeks|mo|mon|month|months|y|yr|year|years)\s*$")


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
    excluded_user_ids: frozenset[int]
    excluded_role_ids: frozenset[int]
    trusted_role_ids: frozenset[int]
    followup_exempt_staff: bool
    verification_exempt_staff: bool
    verification_exempt_bots: bool


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
        return normalize_admin_config(guild_id, compiled.__dict__)

    async def get_counts(self, guild_id: int) -> dict[str, int]:
        if not self.storage_ready:
            return {
                "ban_candidates": 0,
                "active_followups": 0,
                "pending_reviews": 0,
                "verification_pending": 0,
                "verification_warned": 0,
            }
        return await self.store.fetch_guild_counts(guild_id)

    def get_compiled_config(self, guild_id: int) -> CompiledAdminConfig:
        return self._compiled_configs.get(guild_id) or _compile_config(default_admin_config(guild_id))

    async def _update_config(self, guild_id: int, mutator, *, success_message: str) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Admin systems")
        async with self._lock:
            current = self.get_config(guild_id)
            current["guild_id"] = guild_id
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
        if config["verification_logic"] not in VALID_VERIFICATION_LOGIC:
            return (
                "Verification logic must be `must_have_role` "
                "(unverified if the member is missing the role) or `must_not_have_role` "
                "(unverified if the member has the role)."
            )
        if config["followup_duration_unit"] == "months" and config["followup_duration_value"] > 12:
            return "Follow-up month durations can be at most 12 months."
        if config["verification_warning_lead_seconds"] >= config["verification_kick_after_seconds"]:
            return "Warning lead time must be shorter than the full verification kick timer."
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
        role_id: int | None = None,
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
            if role_id is not None:
                config["followup_role_id"] = role_id
            if cleaned_mode is not None:
                config["followup_mode"] = cleaned_mode
            if parsed_duration is not None:
                config["followup_duration_value"] = parsed_duration[0]
                config["followup_duration_unit"] = parsed_duration[1]

        preview = self.get_config(guild_id)
        final_enabled = preview["followup_enabled"] if enabled is None else bool(enabled)
        final_role = preview["followup_role_id"] if role_id is None else role_id
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
        role_id: int | None = None,
        logic: str | None = None,
        kick_after_text: str | None = None,
        warning_lead_text: str | None = None,
        help_channel_id: int | None = None,
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
            if role_id is not None:
                config["verification_role_id"] = role_id
            if cleaned_logic is not None:
                config["verification_logic"] = cleaned_logic
            if parsed_kick_after is not None:
                config["verification_kick_after_seconds"] = parsed_kick_after
            if parsed_warning_lead is not None:
                config["verification_warning_lead_seconds"] = parsed_warning_lead
            if help_channel_id is not None:
                config["verification_help_channel_id"] = help_channel_id
            if parsed_help_extension is not None:
                config["verification_help_extension_seconds"] = parsed_help_extension
            if max_extensions is not None:
                config["verification_max_extensions"] = max_extensions

        preview = self.get_config(guild_id)
        final_enabled = preview["verification_enabled"] if enabled is None else bool(enabled)
        final_role = preview["verification_role_id"] if role_id is None else role_id
        final_logic = preview["verification_logic"] if cleaned_logic is None else cleaned_logic
        final_kick_after = preview["verification_kick_after_seconds"] if parsed_kick_after is None else parsed_kick_after
        final_warning_lead = preview["verification_warning_lead_seconds"] if parsed_warning_lead is None else parsed_warning_lead
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Verification cleanup is {'enabled' if final_enabled else 'disabled'} "
                f"for role <@&{final_role}> using `{final_logic}` with warning {format_duration_brief(final_warning_lead)} "
                f"before a {format_duration_brief(final_kick_after)} kick timer."
                if final_role
                else f"Verification cleanup is {'enabled' if final_enabled else 'disabled'}."
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

    async def set_exemption_toggle(self, guild_id: int, field: str, enabled: bool) -> tuple[bool, str]:
        if field not in {"followup_exempt_staff", "verification_exempt_staff", "verification_exempt_bots"}:
            return False, "Unknown exemption toggle."
        label = field.replace("_", " ")
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__(field, bool(enabled)),
            success_message=f"{label.title()} is now {'enabled' if enabled else 'disabled'}.",
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

    async def list_review_views(self) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        return await self.store.list_review_views()

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
        replacements = {
            "{member}": ge.display_name_of(member),
            "{guild}": guild.name,
            "{deadline}": ge.format_timestamp(deadline, "f"),
            "{deadline_relative}": ge.format_timestamp(deadline, "R"),
            "{help_channel}": getattr(help_channel, "mention", "the server's verification-help channel"),
            "{invite_link}": invite_link or "",
        }
        for placeholder, value in replacements.items():
            text = text.replace(placeholder, value)
        if final and invite_link:
            text = f"{text}\n\nRejoin: {invite_link}"
        return text.strip()

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

    def _followup_role_issue(self, guild: discord.Guild, member: discord.Member, role: discord.Role) -> str | None:
        me = self._bot_member(guild)
        if me is None:
            return "Babblebox could not resolve its server member for role management."
        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "manage_roles", False):
            return "Babblebox is missing Manage Roles."
        if getattr(role, "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return f"{role.mention} is at or above Babblebox's top role."
        if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return f"{member.mention} is at or above Babblebox's top role."
        return None

    def _kick_issue(self, guild: discord.Guild, member: discord.Member) -> str | None:
        me = self._bot_member(guild)
        if me is None:
            return "Babblebox could not resolve its server member for kicks."
        perms = getattr(me, "guild_permissions", None)
        if perms is None or not getattr(perms, "kick_members", False):
            return "Babblebox is missing Kick Members."
        if getattr(member.guild_permissions, "administrator", False):
            return "Babblebox cannot kick administrators."
        if getattr(guild, "owner_id", None) == member.id:
            return "Babblebox cannot kick the server owner."
        if getattr(getattr(member, "top_role", None), "position", 0) >= getattr(getattr(me, "top_role", None), "position", 0):
            return f"{member.mention} is at or above Babblebox's top role."
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

    async def log_operability_warning_once(self, guild: discord.Guild, compiled: CompiledAdminConfig, *, key: str, message: str):
        now = asyncio.get_running_loop().time()
        dedup_key = (guild.id, key)
        if now - self._log_dedup.get(dedup_key, 0.0) < LOG_DEDUP_SECONDS:
            return
        self._log_dedup[dedup_key] = now
        embed = ge.make_status_embed("Admin Automation Warning", message, tone="warning", footer="Babblebox Admin")
        sent = await self.send_log(guild, compiled, embed=embed, alert=True)
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
        await self.store.delete_verification_state(member.guild.id, member.id)
        await self.store.delete_followup(member.guild.id, member.id)

    async def handle_member_update(self, before: discord.Member, after: discord.Member):
        if not self.storage_ready:
            return
        before_compiled = self.get_compiled_config(after.guild.id)
        before_status, _ = self._verification_status(before, before_compiled)
        after_status, _ = self._verification_status(after, before_compiled)
        if before_status == after_status and self._role_ids_for(before) == self._role_ids_for(after):
            return
        if after_status in {"verified", "exempt"}:
            await self.store.delete_verification_state(after.guild.id, after.id)
            return
        if after_status == "unverified":
            await self._ensure_verification_state(after, reason="role update")

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
            await self.store.delete_verification_state(message.guild.id, message.author.id)
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
        await self.store.upsert_verification_state(verification_state)
        self._wake_event.set()
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
                message=f"Babblebox could not assign {role.mention} to {member.mention}. {issue}",
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
            alert=compiled.followup_mode == "review",
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
        for member in getattr(guild, "members", []):
            if not isinstance(member, discord.Member):
                continue
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
                return False, issue, record
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

    async def _run_sweep(self) -> bool:
        if not self.storage_ready:
            return False
        now = ge.now_utc()
        processed = False
        if await self.store.prune_expired_ban_candidates(now, limit=200):
            processed = True
        if await self._process_due_followups(now):
            processed = True
        if await self._process_due_verification_warnings(now):
            processed = True
        if await self._process_due_verification_kicks(now):
            processed = True
        return processed

    async def _process_due_followups(self, now: datetime) -> bool:
        processed = False
        for record in await self.store.list_due_followups(now, limit=FOLLOWUP_REVIEW_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_followup(int(record["guild_id"]), int(record["user_id"]))
                processed = True
                continue
            compiled = self.get_compiled_config(guild.id)
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
                    await self.log_operability_warning_once(
                        guild,
                        compiled,
                        key=f"followup-remove-{member.id}",
                        message=f"Babblebox could not auto-remove {role.mention} from {member.mention}. {issue}",
                    )
                    processed = True
                    continue
                try:
                    await member.remove_roles(role, reason="Babblebox auto-removed an expired follow-up role.")
                except (discord.Forbidden, discord.HTTPException):
                    record["due_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                    await self.store.upsert_followup(record)
                    await self.log_operability_warning_once(
                        guild,
                        compiled,
                        key=f"followup-remove-http-{member.id}",
                        message=f"Babblebox could not auto-remove {role.mention} from {member.mention}. Discord rejected the change.",
                    )
                    processed = True
                    continue
                await self.store.delete_followup(guild.id, member.id)
                await self.send_log(
                    guild,
                    compiled,
                    embed=ge.make_status_embed(
                        "Follow-up Role Removed",
                        f"Babblebox auto-removed {role.mention} from {member.mention} after {_followup_duration_label(compiled.followup_duration_value, compiled.followup_duration_unit)}.",
                        tone="success",
                        footer="Babblebox Admin | Returned-after-ban follow-up",
                    ),
                    alert=False,
                )
                processed = True
                continue
            await self._send_followup_review_alert(guild, compiled, member, role, record, now=now)
            processed = True
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
                content=f"<@&{compiled.admin_alert_role_id}>" if compiled.admin_alert_role_id and self.can_ping_alert_role(guild, compiled) else None,
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

    async def _process_due_verification_warnings(self, now: datetime) -> bool:
        processed = False
        for record in await self.store.list_due_verification_warnings(now, limit=VERIFICATION_BATCH_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_verification_state(int(record["guild_id"]), int(record["user_id"]))
                processed = True
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
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key="verification-warning-ambiguous",
                    message=f"Babblebox skipped a verification warning for {member.mention}. {status_reason}",
                )
                continue
            warning_deadline = deserialize_datetime(record.get("kick_at")) or (now + timedelta(seconds=compiled.verification_warning_lead_seconds))
            dm_sent = False
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await member.send(embed=self.build_warning_embed(member, guild=guild, deadline=warning_deadline, compiled=compiled))
                dm_sent = True
            record["warning_sent_at"] = serialize_datetime(now)
            await self.store.upsert_verification_state(record)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Verification Warning Sent",
                    (
                        f"Babblebox warned {member.mention} about pending verification cleanup.\n"
                        f"Kick deadline: {ge.format_timestamp(warning_deadline, 'R')}.\n"
                        f"DM status: {'sent' if dm_sent else 'failed'}."
                    ),
                    tone="warning",
                    footer="Babblebox Admin | Verification cleanup",
                ),
                alert=False,
            )
            processed = True
        return processed

    async def _process_due_verification_kicks(self, now: datetime) -> bool:
        processed = False
        for record in await self.store.list_due_verification_kicks(now, limit=VERIFICATION_BATCH_LIMIT):
            guild = self.bot.get_guild(int(record["guild_id"]))
            if guild is None:
                await self.store.delete_verification_state(int(record["guild_id"]), int(record["user_id"]))
                processed = True
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
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key="verification-kick-ambiguous",
                    message=f"Babblebox skipped a verification kick for {member.mention}. {status_reason}",
                )
                continue
            if record.get("warning_sent_at") is None and compiled.verification_warning_lead_seconds > 0:
                record["warning_at"] = serialize_datetime(now)
                record["warning_sent_at"] = serialize_datetime(now)
                record["kick_at"] = serialize_datetime(now + timedelta(seconds=compiled.verification_warning_lead_seconds))
                dm_sent = False
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await member.send(
                        embed=self.build_warning_embed(
                            member,
                            guild=guild,
                            deadline=deserialize_datetime(record["kick_at"]) or (now + timedelta(seconds=compiled.verification_warning_lead_seconds)),
                            compiled=compiled,
                        )
                    )
                    dm_sent = True
                await self.store.upsert_verification_state(record)
                await self.send_log(
                    guild,
                    compiled,
                    embed=ge.make_status_embed(
                        "Verification Warning Deferred A Kick",
                        f"Babblebox warned {member.mention} instead of kicking immediately because no prior warning had been recorded. DM status: {'sent' if dm_sent else 'failed'}.",
                        tone="warning",
                        footer="Babblebox Admin | Verification cleanup",
                    ),
                    alert=False,
                )
                processed = True
                continue
            issue = self._kick_issue(guild, member)
            if issue is not None:
                record["kick_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                await self.store.upsert_verification_state(record)
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key=f"verification-kick-{member.id}",
                    message=f"Babblebox could not kick {member.mention} for verification cleanup. {issue}",
                )
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
                record["kick_at"] = serialize_datetime(now + timedelta(seconds=OPERATION_BACKOFF_SECONDS))
                await self.store.upsert_verification_state(record)
                await self.log_operability_warning_once(
                    guild,
                    compiled,
                    key=f"verification-kick-http-{member.id}",
                    message=f"Babblebox tried to kick {member.mention} for verification cleanup, but Discord rejected the action.",
                )
                processed = True
                continue
            await self.store.delete_verification_state(guild.id, member.id)
            await self.send_log(
                guild,
                compiled,
                embed=ge.make_status_embed(
                    "Member Removed For Verification Cleanup",
                    f"Babblebox kicked {member.mention} after the verification deadline expired. Final DM status: {'sent' if dm_sent else 'failed'}.",
                    tone="danger",
                    footer="Babblebox Admin | Verification cleanup",
                ),
                alert=True,
            )
            processed = True
        return processed
