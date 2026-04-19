from __future__ import annotations

import asyncio
import contextlib
import re
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.premium_limits import (
    LIMIT_AFK_SCHEDULES,
    LIMIT_BUMP_DETECTION_CHANNELS,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
    guild_limit as premium_guild_limit,
    user_limit as premium_user_limit,
)
from babblebox.premium_models import PLAN_FREE
from babblebox.shield_service import (
    FEATURE_SURFACE_AFK_REASON,
    FEATURE_SURFACE_AFK_SCHEDULE_REASON,
    FEATURE_SURFACE_REMINDER_CREATE,
    FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY,
    FEATURE_SURFACE_WATCH_KEYWORD,
    ShieldFeatureDecision,
    ShieldFeatureSafetyGateway,
)
from babblebox.text_safety import find_private_pattern, normalize_plain_text, sanitize_short_plain_text
from babblebox.utility_helpers import (
    build_afk_notice_line,
    build_afk_status_embed,
    build_bump_reminder_embed,
    build_bump_thanks_embed,
    build_capture_delivery_embed,
    build_capture_transcript_file,
    build_jump_view,
    build_later_marker_embed,
    build_reminder_delivery_embed,
    build_reminder_delivery_view,
    build_watch_alert_embed,
    canonicalize_afk_timezone,
    compute_latest_afk_schedule_start,
    compute_next_afk_schedule_start,
    default_afk_weekday_mask,
    deserialize_datetime,
    format_afk_clock,
    format_afk_repeat_label,
    format_afk_timezone_label,
    format_afk_weekday,
    format_duration_brief,
    get_afk_preset_default_duration,
    make_attachment_labels,
    make_message_preview,
    parse_duration_string,
    serialize_datetime,
)
from babblebox.utility_store import UtilityStateStore, UtilityStorageUnavailable


WATCH_KEYWORD_LIMIT = premium_user_limit(PLAN_FREE, LIMIT_WATCH_KEYWORDS)
WATCH_FILTER_LIMIT = premium_user_limit(PLAN_FREE, LIMIT_WATCH_FILTERS)
WATCH_KEYWORD_MAX_LEN = 40
WATCH_DM_COOLDOWN_SECONDS = 20.0
WATCH_DEDUP_TTL_SECONDS = 300.0
RETURN_WATCH_ALLOWED_SECONDS = (3600, 6 * 3600, 24 * 3600)
CAPTURE_COOLDOWN_SECONDS = 45.0
REMINDER_COOLDOWN_SECONDS = 60.0
REMINDER_MAX_ACTIVE = premium_user_limit(PLAN_FREE, LIMIT_REMINDERS_ACTIVE)
REMINDER_MAX_PUBLIC_ACTIVE = premium_user_limit(PLAN_FREE, LIMIT_REMINDERS_PUBLIC_ACTIVE)
REMINDER_TEXT_MAX_LEN = 120
REMINDER_MIN_SECONDS = 5 * 60
REMINDER_PUBLIC_MIN_SECONDS = 15 * 60
REMINDER_MAX_SECONDS = 14 * 24 * 3600
REMINDER_RETRY_BASE_SECONDS = 10 * 60
REMINDER_RETRY_MAX_SECONDS = 6 * 3600
AFK_NOTICE_COOLDOWN_SECONDS = 30.0
AFK_SCHEDULE_LIMIT = premium_user_limit(PLAN_FREE, LIMIT_AFK_SCHEDULES)
BUMP_PROVIDER_DISBOARD = "disboard"
BUMP_DETECTION_CHANNEL_LIMIT = premium_guild_limit(PLAN_FREE, LIMIT_BUMP_DETECTION_CHANNELS)
BUMP_REMINDER_TEXT_MAX_LEN = 180
BUMP_THANKS_TEXT_MAX_LEN = 240
BUMP_REMINDER_SENTENCE_LIMIT = 2
BUMP_THANKS_SENTENCE_LIMIT = 3
BUMP_THANKS_DELETE_AFTER_SECONDS = 15.0
BUMP_CYCLE_RETRY_BASE_SECONDS = REMINDER_RETRY_BASE_SECONDS
BUMP_CYCLE_RETRY_MAX_SECONDS = REMINDER_RETRY_MAX_SECONDS
BUMP_CONFIG_UNCHANGED = object()
BUMP_PROVIDER_SPECS = {
    BUMP_PROVIDER_DISBOARD: {
        "label": "Disboard",
        "bot_id": 302050872383242240,
        "cooldown_seconds": 2 * 3600,
        "success_fragments": ("bump done", "you can bump again"),
        "cooldown_fragments": ("please wait another", "wait another"),
    }
}


def _watch_default_config() -> dict:
    return {
        "mention_global": False,
        "mention_guild_ids": [],
        "mention_channel_ids": [],
        "reply_global": False,
        "reply_guild_ids": [],
        "reply_channel_ids": [],
        "excluded_channel_ids": [],
        "ignored_user_ids": [],
        "keywords": [],
    }


def _bump_default_config(guild_id: int) -> dict:
    return {
        "guild_id": guild_id,
        "enabled": False,
        "provider": BUMP_PROVIDER_DISBOARD,
        "detection_channel_ids": [],
        "reminder_channel_id": None,
        "reminder_role_id": None,
        "reminder_text": None,
        "thanks_text": None,
        "thanks_mode": "quiet",
    }


def _bump_cycle_id(guild_id: int, provider: str) -> str:
    return f"{guild_id}:{provider}"


def _normalize_bump_provider(provider: str | None) -> str:
    normalized = str(provider or "").strip().casefold()
    return normalized or BUMP_PROVIDER_DISBOARD


def _bump_provider_label(provider: str | None) -> str:
    normalized = _normalize_bump_provider(provider)
    return str(BUMP_PROVIDER_SPECS.get(normalized, {}).get("label") or normalized.replace("_", " ").title())


def _default_bump_reminder_text(provider: str | None) -> str:
    provider_label = _bump_provider_label(provider)
    return f"The next {provider_label} bump window is open."


def _default_bump_thanks_text(provider: str | None) -> str:
    provider_label = _bump_provider_label(provider)
    return f"Thanks for keeping the server visible on {provider_label}."


def _build_keyword_matcher(phrase: str, mode: str):
    lowered = phrase.casefold()
    if mode == "word":
        pattern = re.compile(rf"(?<!\w){re.escape(lowered)}(?!\w)", re.IGNORECASE)
        return lambda content: bool(pattern.search(content))
    return lambda content: lowered in content


class UtilityService:
    def __init__(self, bot: commands.Bot, store: UtilityStateStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = UtilityStateStore()
            except UtilityStorageUnavailable as exc:
                # Keep the bot loadable when the utility database is missing or offline.
                print(f"Utility storage constructor failed: {exc}")
                self.store = UtilityStateStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._shield_feature_gateway_fallback = ShieldFeatureSafetyGateway()

        self._mention_global: set[int] = set()
        self._mention_by_guild: dict[int, set[int]] = {}
        self._mention_by_channel: dict[int, set[int]] = {}
        self._reply_global: set[int] = set()
        self._reply_by_guild: dict[int, set[int]] = {}
        self._reply_by_channel: dict[int, set[int]] = {}
        self._keywords_global: dict[int, list[dict]] = {}
        self._keywords_by_guild: dict[int, dict[int, list[dict]]] = {}
        self._keywords_by_channel: dict[int, dict[int, list[dict]]] = {}
        self._excluded_channels_by_user: dict[int, set[int]] = {}
        self._ignored_users_by_user: dict[int, set[int]] = {}
        self._return_user_watch_ids_by_target: dict[tuple[int, int], set[str]] = {}
        self._return_channel_watch_ids_by_target: dict[int, set[str]] = {}
        self._return_watch_id_by_dedupe_key: dict[tuple[int, int, str, int], str] = {}

        self._watch_dm_cooldowns: dict[int, float] = {}
        self._watch_dedup: dict[tuple[int, int], float] = {}
        self._capture_cooldowns: dict[int, float] = {}
        self._reminder_cooldowns: dict[int, float] = {}
        self._afk_notice_cooldowns: dict[tuple[int, int], float] = {}
        self._watch_alert_counts: dict[int, dict[str, int]] = {}

    async def start(self):
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Utility storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except UtilityStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Utility storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        self._rebuild_watch_indexes()
        self._rebuild_return_watch_indexes()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="babblebox-utility-scheduler")
        self._wake_event.set()
        return True

    async def close(self):
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task
        await self._shield_feature_gateway_fallback.close()
        await self.store.close()

    def storage_message(self, feature_name: str = "This feature") -> str:
        return f"{feature_name} is temporarily unavailable because Babblebox could not reach its utility database."

    def _has_storage(self) -> bool:
        return self.storage_ready

    def _shield_feature_gateway(self) -> ShieldFeatureSafetyGateway:
        shield_service = getattr(self.bot, "shield_service", None)
        gateway = getattr(shield_service, "feature_gateway", None)
        return gateway if callable(getattr(gateway, "evaluate", None)) else self._shield_feature_gateway_fallback

    def _premium_service(self):
        premium_service = getattr(self.bot, "premium_service", None)
        if callable(getattr(premium_service, "resolve_user_limit", None)):
            return premium_service
        return None

    def _resolve_user_limit(self, user_id: int, limit_key: str, fallback: int) -> int:
        premium_service = self._premium_service()
        if premium_service is None:
            return fallback
        return premium_service.resolve_user_limit(user_id, limit_key)

    def _resolve_guild_limit(self, guild_id: int, limit_key: str, fallback: int) -> int:
        premium_service = self._premium_service()
        if premium_service is None:
            return fallback
        return premium_service.resolve_guild_limit(guild_id, limit_key)

    def _premium_limit_error(self, *, limit_key: str, limit_value: int, default_message: str) -> str:
        premium_service = self._premium_service()
        if premium_service is None:
            return default_message
        return premium_service.describe_limit_error(limit_key=limit_key, limit_value=limit_value)

    def _count_change_exceeds_limit(
        self,
        *,
        previous_count: int,
        next_count: int,
        limit_value: int,
    ) -> bool:
        return next_count > limit_value and next_count > previous_count

    def watch_keyword_limit(self, user_id: int) -> int:
        return self._resolve_user_limit(user_id, LIMIT_WATCH_KEYWORDS, WATCH_KEYWORD_LIMIT)

    def watch_filter_limit(self, user_id: int) -> int:
        return self._resolve_user_limit(user_id, LIMIT_WATCH_FILTERS, WATCH_FILTER_LIMIT)

    def reminder_limit(self, user_id: int) -> int:
        return self._resolve_user_limit(user_id, LIMIT_REMINDERS_ACTIVE, REMINDER_MAX_ACTIVE)

    def public_reminder_limit(self, user_id: int) -> int:
        return self._resolve_user_limit(user_id, LIMIT_REMINDERS_PUBLIC_ACTIVE, REMINDER_MAX_PUBLIC_ACTIVE)

    def afk_schedule_limit(self, user_id: int) -> int:
        return self._resolve_user_limit(user_id, LIMIT_AFK_SCHEDULES, AFK_SCHEDULE_LIMIT)

    def bump_detection_channel_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_BUMP_DETECTION_CHANNELS, BUMP_DETECTION_CHANNEL_LIMIT)

    def _evaluate_feature_text(self, surface: str, text: str | None) -> ShieldFeatureDecision:
        return self._shield_feature_gateway().evaluate(surface, text)

    def _bump_config_record(self, guild_id: int, *, create: bool = False) -> dict | None:
        configs = self.store.state.setdefault("bump_configs", {})
        key = str(guild_id)
        record = configs.get(key)
        if record is None and create:
            record = _bump_default_config(guild_id)
            configs[key] = record
        if isinstance(record, dict):
            return record
        if create:
            configs[key] = _bump_default_config(guild_id)
            return configs[key]
        return None

    def _bump_cycle_record(self, guild_id: int, provider: str | None, *, create: bool = False) -> dict | None:
        normalized_provider = _normalize_bump_provider(provider)
        cycles = self.store.state.setdefault("bump_cycles", {})
        cycle_id = _bump_cycle_id(guild_id, normalized_provider)
        record = cycles.get(cycle_id)
        if record is None and create:
            record = {
                "id": cycle_id,
                "guild_id": guild_id,
                "provider": normalized_provider,
                "last_provider_event_at": None,
                "last_provider_event_kind": None,
                "last_bump_at": None,
                "last_bumper_user_id": None,
                "last_success_message_id": None,
                "last_success_channel_id": None,
                "due_at": None,
                "reminder_sent_at": None,
                "delivery_attempts": 0,
                "last_delivery_attempt_at": None,
                "retry_after": None,
                "last_delivery_error": None,
            }
            cycles[cycle_id] = record
        return record if isinstance(record, dict) else None

    def get_bump_config(self, guild_id: int) -> dict:
        record = self._bump_config_record(guild_id)
        return dict(record) if record is not None else _bump_default_config(guild_id)

    def get_bump_cycle(self, guild_id: int, *, provider: str | None = None) -> dict | None:
        record = self._bump_cycle_record(guild_id, provider or self.get_bump_config(guild_id).get("provider"))
        return dict(record) if isinstance(record, dict) else None

    def resolved_bump_reminder_text(self, guild_id: int, *, provider: str | None = None) -> str:
        config = self.get_bump_config(guild_id)
        resolved_provider = provider or config.get("provider")
        return str(config.get("reminder_text") or _default_bump_reminder_text(resolved_provider))

    def resolved_bump_thanks_text(self, guild_id: int, *, provider: str | None = None) -> str:
        config = self.get_bump_config(guild_id)
        resolved_provider = provider or config.get("provider")
        return str(config.get("thanks_text") or _default_bump_thanks_text(resolved_provider))

    def _validate_bump_message_text(self, text: str, *, field_name: str, max_length: int, sentence_limit: int) -> tuple[bool, str]:
        valid, cleaned_or_error = sanitize_short_plain_text(
            text,
            field_name=field_name,
            max_length=max_length,
            sentence_limit=sentence_limit,
            reject_blocklist=False,
            allow_empty=False,
        )
        if not valid:
            return False, cleaned_or_error
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_REMINDER_CREATE, cleaned_or_error)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or f"That {field_name.lower()} is not allowed."
        return True, cleaned_or_error

    async def configure_bump(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        provider: str | None = None,
        detection_channel_ids: list[int] | object = BUMP_CONFIG_UNCHANGED,
        reminder_channel_id: int | None | object = BUMP_CONFIG_UNCHANGED,
        reminder_role_id: int | None | object = BUMP_CONFIG_UNCHANGED,
        reminder_text: str | None | object = BUMP_CONFIG_UNCHANGED,
        thanks_text: str | None | object = BUMP_CONFIG_UNCHANGED,
        thanks_mode: str | None = None,
    ) -> tuple[bool, str | dict]:
        if not self._has_storage():
            return False, self.storage_message("Bump reminders")
        current_config = self.get_bump_config(guild_id)
        bump_limit = self.bump_detection_channel_limit(guild_id)

        normalized_provider = None
        if provider is not None:
            normalized_provider = _normalize_bump_provider(provider)
            if normalized_provider not in BUMP_PROVIDER_SPECS:
                return False, "Only `disboard` is supported right now."

        normalized_detection_channel_ids: list[int] | object = BUMP_CONFIG_UNCHANGED
        if detection_channel_ids is not BUMP_CONFIG_UNCHANGED:
            normalized_detection_channel_ids = sorted(
                {
                    channel_id
                    for channel_id in detection_channel_ids
                    if isinstance(channel_id, int) and channel_id > 0
                }
            )
            previous_count = len(current_config.get("detection_channel_ids", []))
            if self._count_change_exceeds_limit(
                previous_count=previous_count,
                next_count=len(normalized_detection_channel_ids),
                limit_value=bump_limit,
            ):
                return False, self._premium_limit_error(
                    limit_key=LIMIT_BUMP_DETECTION_CHANNELS,
                    limit_value=bump_limit,
                    default_message=f"You can keep up to {bump_limit} bump detection channels.",
                )

        cleaned_reminder_text: str | None | object = BUMP_CONFIG_UNCHANGED
        if reminder_text is not BUMP_CONFIG_UNCHANGED:
            if reminder_text is None:
                cleaned_reminder_text = None
            else:
                ok, cleaned_or_error = self._validate_bump_message_text(
                    reminder_text,
                    field_name="Reminder message",
                    max_length=BUMP_REMINDER_TEXT_MAX_LEN,
                    sentence_limit=BUMP_REMINDER_SENTENCE_LIMIT,
                )
                if not ok:
                    return False, cleaned_or_error
                cleaned_reminder_text = cleaned_or_error

        cleaned_thanks_text: str | None | object = BUMP_CONFIG_UNCHANGED
        if thanks_text is not BUMP_CONFIG_UNCHANGED:
            if thanks_text is None:
                cleaned_thanks_text = None
            else:
                ok, cleaned_or_error = self._validate_bump_message_text(
                    thanks_text,
                    field_name="Thank-you message",
                    max_length=BUMP_THANKS_TEXT_MAX_LEN,
                    sentence_limit=BUMP_THANKS_SENTENCE_LIMIT,
                )
                if not ok:
                    return False, cleaned_or_error
                cleaned_thanks_text = cleaned_or_error

        normalized_thanks_mode = None
        if thanks_mode is not None:
            normalized_thanks_mode = str(thanks_mode).strip().casefold()
            if normalized_thanks_mode not in {"quiet", "public", "off"}:
                return False, "Thank-you mode must be `quiet`, `public`, or `off`."

        async with self._lock:
            record = self._bump_config_record(guild_id, create=True)
            if record is None:
                return False, "Babblebox could not update bump reminders right now."
            if enabled is not None:
                record["enabled"] = bool(enabled)
            if normalized_provider is not None:
                record["provider"] = normalized_provider
            if normalized_detection_channel_ids is not BUMP_CONFIG_UNCHANGED:
                record["detection_channel_ids"] = normalized_detection_channel_ids
            if reminder_channel_id is not BUMP_CONFIG_UNCHANGED:
                record["reminder_channel_id"] = reminder_channel_id if isinstance(reminder_channel_id, int) and reminder_channel_id > 0 else None
            if reminder_role_id is not BUMP_CONFIG_UNCHANGED:
                record["reminder_role_id"] = reminder_role_id if isinstance(reminder_role_id, int) and reminder_role_id > 0 else None
            if cleaned_reminder_text is not BUMP_CONFIG_UNCHANGED:
                record["reminder_text"] = cleaned_reminder_text
            if cleaned_thanks_text is not BUMP_CONFIG_UNCHANGED:
                record["thanks_text"] = cleaned_thanks_text
            if normalized_thanks_mode is not None:
                record["thanks_mode"] = normalized_thanks_mode
            await self.store.flush()
            self._wake_event.set()
            return True, dict(record)

    def _member_can_ping_role(self, guild: discord.Guild, role_id: int | None) -> bool:
        if role_id is None:
            return False
        me = getattr(guild, "me", None)
        bot_user = getattr(self.bot, "user", None)
        if me is None and bot_user is not None:
            me = guild.get_member(bot_user.id)
        role = guild.get_role(role_id) if hasattr(guild, "get_role") else None
        if me is None or role is None:
            return False
        if getattr(role, "mentionable", False):
            return True
        perms = getattr(me, "guild_permissions", None)
        return bool(perms and getattr(perms, "mention_everyone", False))

    def get_bump_operability(self, guild: discord.Guild) -> list[str]:
        config = self.get_bump_config(guild.id)
        me = getattr(guild, "me", None)
        bot_user = getattr(self.bot, "user", None)
        if me is None and bot_user is not None:
            me = guild.get_member(bot_user.id)
        if me is None:
            return ["Babblebox could not resolve its own server member for bump-reminder checks."]
        lines: list[str] = []
        provider = _normalize_bump_provider(config.get("provider"))
        if provider not in BUMP_PROVIDER_SPECS:
            lines.append("The configured bump provider is not supported in this build.")
        detection_channel_ids = [channel_id for channel_id in config.get("detection_channel_ids", []) if isinstance(channel_id, int)]
        if not detection_channel_ids:
            lines.append("No detection channels are configured, so verified bumps cannot start a cycle yet.")
        thanks_mode = str(config.get("thanks_mode") or "quiet")
        for channel_id in detection_channel_ids:
            channel = self.bot.get_channel(channel_id) or guild.get_channel(channel_id)
            if channel is None:
                lines.append(f"Babblebox cannot see configured detection channel <#{channel_id}>.")
                continue
            perms = channel.permissions_for(me)
            if thanks_mode != "off" and not getattr(perms, "send_messages", False):
                lines.append(f"Babblebox cannot send thank-you messages in {channel.mention}.")
            if thanks_mode != "off" and not getattr(perms, "embed_links", False):
                lines.append(f"Thank-you messages in {channel.mention} will fall back to plain text because `Embed Links` is missing.")
        reminder_channel_id = config.get("reminder_channel_id")
        if reminder_channel_id is None:
            lines.append("No reminder destination channel is configured yet.")
        else:
            channel = self.bot.get_channel(reminder_channel_id) or guild.get_channel(reminder_channel_id)
            if channel is None:
                lines.append(f"Babblebox cannot see the configured reminder channel <#{reminder_channel_id}>.")
            else:
                perms = channel.permissions_for(me)
                if not getattr(perms, "send_messages", False):
                    lines.append(f"Babblebox cannot send reminders in {channel.mention}.")
                if not getattr(perms, "embed_links", False):
                    lines.append(f"Reminders in {channel.mention} will fall back to plain text because `Embed Links` is missing.")
        reminder_role_id = config.get("reminder_role_id")
        if reminder_role_id is not None:
            role = guild.get_role(reminder_role_id) if hasattr(guild, "get_role") else None
            if role is None:
                lines.append("The configured reminder role is missing or no longer accessible.")
            elif not self._member_can_ping_role(guild, reminder_role_id):
                lines.append("Babblebox cannot ping the configured reminder role with current permissions, so reminders will post without that role mention.")
        return lines

    def _extract_bump_message_text(self, message: discord.Message) -> str:
        parts: list[str] = []
        if isinstance(getattr(message, "content", None), str) and message.content.strip():
            parts.append(message.content)
        for embed in getattr(message, "embeds", []) or []:
            title = getattr(embed, "title", None)
            description = getattr(embed, "description", None)
            footer = getattr(getattr(embed, "footer", None), "text", None)
            if isinstance(title, str) and title.strip():
                parts.append(title)
            if isinstance(description, str) and description.strip():
                parts.append(description)
            if isinstance(footer, str) and footer.strip():
                parts.append(footer)
            for field in getattr(embed, "fields", []) or []:
                field_name = getattr(field, "name", None)
                field_value = getattr(field, "value", None)
                if isinstance(field_name, str) and field_name.strip():
                    parts.append(field_name)
                if isinstance(field_value, str) and field_value.strip():
                    parts.append(field_value)
        return normalize_plain_text("\n".join(parts)).casefold()

    def _classify_bump_provider_message(self, provider: str, text: str) -> str | None:
        spec = BUMP_PROVIDER_SPECS.get(provider)
        if spec is None or not text:
            return None
        cooldown_fragments = tuple(spec.get("cooldown_fragments", ()))
        if any(fragment in text for fragment in cooldown_fragments):
            return "cooldown"
        success_fragments = tuple(spec.get("success_fragments", ()))
        if any(fragment in text for fragment in success_fragments):
            return "success"
        return None

    def _bump_provider_message_context(
        self,
        message: discord.Message,
    ) -> tuple[dict, str, dict] | None:
        if not self.storage_ready or message.guild is None:
            return None
        config = self.get_bump_config(message.guild.id)
        if not config.get("enabled"):
            return None
        provider = _normalize_bump_provider(config.get("provider"))
        spec = BUMP_PROVIDER_SPECS.get(provider)
        if spec is None:
            return None
        detection_channel_ids = {
            channel_id for channel_id in config.get("detection_channel_ids", []) if isinstance(channel_id, int)
        }
        channel_id = int(getattr(getattr(message, "channel", None), "id", 0) or 0)
        if channel_id not in detection_channel_ids:
            return None
        author_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        if author_id != int(spec["bot_id"]):
            return None
        return config, provider, spec

    def is_bump_provider_message_candidate(self, message: discord.Message) -> bool:
        return self._bump_provider_message_context(message) is not None

    def _bump_initiator_user_id(self, message: discord.Message) -> int | None:
        interaction_metadata = getattr(message, "interaction_metadata", None)
        user = getattr(interaction_metadata, "user", None)
        if isinstance(getattr(user, "id", None), int):
            return user.id
        interaction = getattr(message, "interaction", None)
        user = getattr(interaction, "user", None)
        if isinstance(getattr(user, "id", None), int):
            return user.id
        return None

    async def handle_bump_provider_message(self, message: discord.Message):
        context = self._bump_provider_message_context(message)
        if context is None:
            return
        config, provider, spec = context
        classification = self._classify_bump_provider_message(provider, self._extract_bump_message_text(message))
        if classification is None:
            return
        event_time = message.created_at or ge.now_utc()
        bumper_user_id = self._bump_initiator_user_id(message)
        should_send_thanks = False
        async with self._lock:
            cycle = self._bump_cycle_record(message.guild.id, provider, create=True)
            if cycle is None:
                return
            cycle["last_provider_event_at"] = serialize_datetime(event_time)
            cycle["last_provider_event_kind"] = classification
            if classification == "cooldown":
                await self.store.flush()
                return
            if cycle.get("last_success_message_id") == message.id:
                return
            cycle["last_bump_at"] = serialize_datetime(event_time)
            cycle["last_bumper_user_id"] = bumper_user_id
            cycle["last_success_message_id"] = message.id
            cycle["last_success_channel_id"] = message.channel.id
            cycle["due_at"] = serialize_datetime(event_time + timedelta(seconds=int(spec["cooldown_seconds"])))
            cycle["reminder_sent_at"] = None
            cycle["delivery_attempts"] = 0
            cycle["last_delivery_attempt_at"] = None
            cycle["retry_after"] = None
            cycle["last_delivery_error"] = None
            await self.store.flush()
            self._wake_event.set()
            should_send_thanks = True
        if should_send_thanks:
            await self._send_bump_thanks(message, config=config, provider=provider, bumper_user_id=bumper_user_id)

    async def _send_bump_thanks(
        self,
        message: discord.Message,
        *,
        config: dict,
        provider: str,
        bumper_user_id: int | None,
    ):
        thanks_mode = str(config.get("thanks_mode") or "quiet").casefold()
        if thanks_mode == "off":
            return
        provider_label = _bump_provider_label(provider)
        thanks_text = str(config.get("thanks_text") or _default_bump_thanks_text(provider))
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY, thanks_text)
        if not feature_decision.allowed:
            return
        guild = message.guild
        channel = message.channel
        me = getattr(guild, "me", None)
        bot_user = getattr(self.bot, "user", None)
        if me is None and bot_user is not None:
            me = guild.get_member(bot_user.id)
        if me is None:
            return
        perms = channel.permissions_for(me)
        if not getattr(perms, "send_messages", False):
            return
        bumper_name = None
        if isinstance(bumper_user_id, int):
            bumper = guild.get_member(bumper_user_id) or self.bot.get_user(bumper_user_id)
            bumper_name = ge.display_name_of(bumper) if bumper is not None else None
        embed = build_bump_thanks_embed(
            provider_label=provider_label,
            thanks_text=thanks_text,
            bumper_name=bumper_name,
        )
        send_kwargs = {
            "reference": message,
            "mention_author": False,
            "allowed_mentions": discord.AllowedMentions.none(),
        }
        if getattr(perms, "embed_links", False):
            send_kwargs["embed"] = embed
        else:
            fallback_text = thanks_text if bumper_name is None else f"{bumper_name} - {thanks_text}"
            send_kwargs["content"] = fallback_text
        if thanks_mode == "quiet":
            send_kwargs["delete_after"] = BUMP_THANKS_DELETE_AFTER_SECONDS
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(**send_kwargs)

    def _build_bump_retry_update(self, cycle: dict, *, now: datetime | None = None, error: str | None = None) -> dict[str, int | str | None]:
        current_time = now or ge.now_utc()
        previous_attempts = cycle.get("delivery_attempts", 0)
        attempts = int(previous_attempts) if isinstance(previous_attempts, int) and previous_attempts >= 0 else 0
        attempts += 1
        backoff_seconds = min(
            BUMP_CYCLE_RETRY_MAX_SECONDS,
            BUMP_CYCLE_RETRY_BASE_SECONDS * (2 ** min(attempts - 1, 5)),
        )
        return {
            "delivery_attempts": attempts,
            "last_delivery_attempt_at": serialize_datetime(current_time),
            "retry_after": serialize_datetime(current_time + timedelta(seconds=backoff_seconds)),
            "last_delivery_error": error,
        }

    async def _deliver_due_bump_cycles(self, cycles: list[dict]):
        now = ge.now_utc()
        success_updates: dict[str, dict[str, int | str | None]] = {}
        retry_updates: dict[str, dict[str, int | str | None]] = {}
        for cycle in cycles:
            cycle_id = cycle.get("id")
            if not isinstance(cycle_id, str):
                continue
            success, error = await self._deliver_single_bump_cycle(cycle)
            if success:
                attempts = cycle.get("delivery_attempts", 0)
                attempts = int(attempts) if isinstance(attempts, int) and attempts >= 0 else 0
                success_updates[cycle_id] = {
                    "reminder_sent_at": serialize_datetime(now),
                    "delivery_attempts": attempts + 1,
                    "last_delivery_attempt_at": serialize_datetime(now),
                    "retry_after": None,
                    "last_delivery_error": None,
                }
            else:
                retry_updates[cycle_id] = self._build_bump_retry_update(cycle, now=now, error=error)
        if not success_updates and not retry_updates:
            return
        async with self._lock:
            cycles_state = self.store.state.get("bump_cycles", {})
            dirty = False
            for cycle_id, update in success_updates.items():
                current = cycles_state.get(cycle_id)
                if not isinstance(current, dict):
                    continue
                current.update(update)
                dirty = True
            for cycle_id, update in retry_updates.items():
                current = cycles_state.get(cycle_id)
                if not isinstance(current, dict):
                    continue
                current.update(update)
                dirty = True
            if dirty:
                await self.store.flush()
                self._wake_event.set()

    def _bump_plain_text_content(self, cycle: dict, *, provider_label: str, reminder_text: str, role_id: int | None, role_ping_allowed: bool) -> str:
        parts = []
        if role_ping_allowed and isinstance(role_id, int):
            parts.append(f"<@&{role_id}>")
        parts.append(reminder_text)
        last_bump_at = deserialize_datetime(cycle.get("last_bump_at"))
        if last_bump_at is not None:
            parts.append(f"Last verified bump {ge.format_timestamp(last_bump_at, 'R')}.")
        return "\n".join(parts)

    async def _deliver_single_bump_cycle(self, cycle: dict) -> tuple[bool, str | None]:
        guild_id = cycle.get("guild_id")
        if not isinstance(guild_id, int):
            return False, "Missing guild for bump reminder delivery."
        config = self.get_bump_config(guild_id)
        if not config.get("enabled"):
            return False, "Bump reminders are disabled for this server."
        provider = _normalize_bump_provider(cycle.get("provider") or config.get("provider"))
        provider_label = _bump_provider_label(provider)
        reminder_channel_id = config.get("reminder_channel_id")
        if not isinstance(reminder_channel_id, int):
            return False, "No reminder destination channel is configured."
        get_guild = getattr(self.bot, "get_guild", None)
        guild = get_guild(guild_id) if callable(get_guild) else None
        if guild is None:
            return False, "Babblebox could not resolve the guild for bump reminder delivery."
        channel = self.bot.get_channel(reminder_channel_id) or guild.get_channel(reminder_channel_id)
        if channel is None:
            return False, "Babblebox cannot access the configured reminder channel."
        me = getattr(guild, "me", None)
        bot_user = getattr(self.bot, "user", None)
        if me is None and bot_user is not None:
            me = guild.get_member(bot_user.id)
        if me is None:
            return False, "Babblebox could not resolve its server member for bump reminder delivery."
        perms = channel.permissions_for(me)
        if not getattr(perms, "send_messages", False):
            return False, "Babblebox cannot send messages in the configured reminder channel."
        reminder_text = str(config.get("reminder_text") or _default_bump_reminder_text(provider))
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY, reminder_text)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or "Shield blocked that bump reminder text."
        reminder_role_id = config.get("reminder_role_id")
        role_ping_allowed = self._member_can_ping_role(guild, reminder_role_id if isinstance(reminder_role_id, int) else None)
        due_at = deserialize_datetime(cycle.get("due_at"))
        delayed = bool(due_at is not None and (ge.now_utc() - due_at).total_seconds() > 120)
        if getattr(perms, "embed_links", False):
            embed = build_bump_reminder_embed(
                provider_label=provider_label,
                reminder_text=reminder_text,
                cycle=cycle,
                delayed=delayed,
            )
            content = f"<@&{reminder_role_id}>" if role_ping_allowed and isinstance(reminder_role_id, int) else None
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await channel.send(
                    content=content,
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
                )
                return True, None
            return False, "Discord rejected the bump reminder embed delivery."
        plain_text = self._bump_plain_text_content(
            cycle,
            provider_label=provider_label,
            reminder_text=reminder_text,
            role_id=reminder_role_id if isinstance(reminder_role_id, int) else None,
            role_ping_allowed=role_ping_allowed,
        )
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(
                content=plain_text,
                allowed_mentions=discord.AllowedMentions(users=False, roles=True, everyone=False),
            )
            return True, None
        return False, "Discord rejected the bump reminder text delivery."

    def _watch_config(self, user_id: int, *, create: bool = False) -> dict | None:
        configs = self.store.state.setdefault("watch", {})
        key = str(user_id)
        config = configs.get(key)
        if config is None and create:
            config = _watch_default_config()
            configs[key] = config
        if isinstance(config, dict):
            return config
        if create:
            configs[key] = _watch_default_config()
            return configs[key]
        return None

    def _cleanup_watch_user_if_empty(self, user_id: int):
        config = self._watch_config(user_id)
        if config is None:
            return
        if (
            config.get("mention_global")
            or config.get("mention_guild_ids")
            or config.get("mention_channel_ids")
            or config.get("reply_global")
            or config.get("reply_guild_ids")
            or config.get("reply_channel_ids")
            or config.get("excluded_channel_ids")
            or config.get("ignored_user_ids")
            or config.get("keywords")
        ):
            return
        self.store.state.get("watch", {}).pop(str(user_id), None)

    def _rebuild_watch_indexes(self):
        mention_global: set[int] = set()
        mention_by_guild: defaultdict[int, set[int]] = defaultdict(set)
        mention_by_channel: defaultdict[int, set[int]] = defaultdict(set)
        reply_global: set[int] = set()
        reply_by_guild: defaultdict[int, set[int]] = defaultdict(set)
        reply_by_channel: defaultdict[int, set[int]] = defaultdict(set)
        keywords_global: defaultdict[int, list[dict]] = defaultdict(list)
        keywords_by_guild: defaultdict[int, defaultdict[int, list[dict]]] = defaultdict(lambda: defaultdict(list))
        keywords_by_channel: defaultdict[int, defaultdict[int, list[dict]]] = defaultdict(lambda: defaultdict(list))
        excluded_channels_by_user: dict[int, set[int]] = {}
        ignored_users_by_user: dict[int, set[int]] = {}
        for user_id_text, config in self.store.state.get("watch", {}).items():
            try:
                user_id = int(user_id_text)
            except (TypeError, ValueError):
                continue
            if not isinstance(config, dict):
                continue
            if config.get("mention_global"):
                mention_global.add(user_id)
            for guild_id in config.get("mention_guild_ids", []):
                if isinstance(guild_id, int):
                    mention_by_guild[guild_id].add(user_id)
            for channel_id in config.get("mention_channel_ids", []):
                if isinstance(channel_id, int):
                    mention_by_channel[channel_id].add(user_id)
            if config.get("reply_global"):
                reply_global.add(user_id)
            for guild_id in config.get("reply_guild_ids", []):
                if isinstance(guild_id, int):
                    reply_by_guild[guild_id].add(user_id)
            for channel_id in config.get("reply_channel_ids", []):
                if isinstance(channel_id, int):
                    reply_by_channel[channel_id].add(user_id)
            excluded_channels_by_user[user_id] = {channel_id for channel_id in config.get("excluded_channel_ids", []) if isinstance(channel_id, int)}
            ignored_users_by_user[user_id] = {other_user_id for other_user_id in config.get("ignored_user_ids", []) if isinstance(other_user_id, int)}
            for item in config.get("keywords", []):
                if not isinstance(item, dict):
                    continue
                phrase = normalize_plain_text(item.get("phrase"))
                mode = item.get("mode", "contains")
                if not phrase or mode not in {"contains", "word"}:
                    continue
                guild_id = item.get("guild_id")
                channel_id = item.get("channel_id")
                entry = {
                    "phrase": phrase,
                    "mode": mode,
                    "guild_id": guild_id if isinstance(guild_id, int) else None,
                    "channel_id": channel_id if isinstance(channel_id, int) and isinstance(guild_id, int) else None,
                    "matcher": _build_keyword_matcher(phrase, mode),
                }
                if entry["channel_id"] is not None:
                    keywords_by_channel[entry["channel_id"]][user_id].append(entry)
                elif entry["guild_id"] is None:
                    keywords_global[user_id].append(entry)
                else:
                    keywords_by_guild[entry["guild_id"]][user_id].append(entry)
        self._mention_global = mention_global
        self._mention_by_guild = {guild_id: set(user_ids) for guild_id, user_ids in mention_by_guild.items()}
        self._mention_by_channel = {channel_id: set(user_ids) for channel_id, user_ids in mention_by_channel.items()}
        self._reply_global = reply_global
        self._reply_by_guild = {guild_id: set(user_ids) for guild_id, user_ids in reply_by_guild.items()}
        self._reply_by_channel = {channel_id: set(user_ids) for channel_id, user_ids in reply_by_channel.items()}
        self._keywords_global = {user_id: list(entries) for user_id, entries in keywords_global.items()}
        self._keywords_by_guild = {guild_id: {user_id: list(entries) for user_id, entries in by_user.items()} for guild_id, by_user in keywords_by_guild.items()}
        self._keywords_by_channel = {channel_id: {user_id: list(entries) for user_id, entries in by_user.items()} for channel_id, by_user in keywords_by_channel.items()}
        self._excluded_channels_by_user = excluded_channels_by_user
        self._ignored_users_by_user = ignored_users_by_user

    def _rebuild_return_watch_indexes(self):
        user_targets: defaultdict[tuple[int, int], set[str]] = defaultdict(set)
        channel_targets: defaultdict[int, set[str]] = defaultdict(set)
        dedupe_keys: dict[tuple[int, int, str, int], str] = {}
        for watch_id, record in self.store.state.get("return_watches", {}).items():
            if not isinstance(record, dict):
                continue
            try:
                watcher_user_id = int(record.get("watcher_user_id"))
                guild_id = int(record.get("guild_id"))
                target_id = int(record.get("target_id"))
            except (TypeError, ValueError):
                continue
            target_type = record.get("target_type")
            if target_type == "user":
                user_targets[(guild_id, target_id)].add(watch_id)
            elif target_type == "channel":
                channel_targets[target_id].add(watch_id)
            else:
                continue
            dedupe_keys[(watcher_user_id, guild_id, target_type, target_id)] = watch_id
        self._return_user_watch_ids_by_target = {key: set(value) for key, value in user_targets.items()}
        self._return_channel_watch_ids_by_target = {key: set(value) for key, value in channel_targets.items()}
        self._return_watch_id_by_dedupe_key = dedupe_keys

    def _make_return_watch_record(
        self,
        *,
        watch_id: str,
        watcher_user_id: int,
        guild_id: int,
        target_type: str,
        target_id: int,
        created_at: datetime,
        expires_at: datetime,
        created_from: str | None,
    ) -> dict:
        return {
            "id": watch_id,
            "watcher_user_id": watcher_user_id,
            "guild_id": guild_id,
            "target_type": target_type,
            "target_id": target_id,
            "created_at": serialize_datetime(created_at),
            "expires_at": serialize_datetime(expires_at),
            "created_from": created_from,
        }

    def _return_watch_is_expired(self, record: dict, *, now: datetime | None = None) -> bool:
        expires_at = deserialize_datetime(record.get("expires_at"))
        if expires_at is None:
            return True
        return expires_at <= (now or ge.now_utc())

    async def upsert_return_watch(
        self,
        *,
        watcher_user_id: int,
        guild_id: int,
        target_type: str,
        target_id: int,
        duration_seconds: int,
        created_from: str | None = None,
    ) -> tuple[bool, str | dict, bool]:
        if not self._has_storage():
            return False, self.storage_message("Watch"), False
        if target_type not in {"user", "channel"}:
            return False, "Unknown one-shot watch target.", False
        if duration_seconds not in RETURN_WATCH_ALLOWED_SECONDS:
            return False, "Pick 1 hour, 6 hours, or 24 hours for this alert.", False

        now = ge.now_utc()
        expires_at = now + timedelta(seconds=duration_seconds)
        created_from_value = created_from.strip() if isinstance(created_from, str) and created_from.strip() else None
        dedupe_key = (watcher_user_id, guild_id, target_type, target_id)

        async with self._lock:
            watches = self.store.state.setdefault("return_watches", {})
            existing_id = self._return_watch_id_by_dedupe_key.get(dedupe_key)
            existing = watches.get(existing_id) if existing_id is not None else None
            refreshed = isinstance(existing, dict) and not self._return_watch_is_expired(existing, now=now)
            if refreshed:
                existing["created_at"] = serialize_datetime(now)
                existing["expires_at"] = serialize_datetime(expires_at)
                existing["created_from"] = created_from_value
                record = dict(existing)
            else:
                if existing_id is not None:
                    watches.pop(existing_id, None)
                watch_id = uuid.uuid4().hex
                record = self._make_return_watch_record(
                    watch_id=watch_id,
                    watcher_user_id=watcher_user_id,
                    guild_id=guild_id,
                    target_type=target_type,
                    target_id=target_id,
                    created_at=now,
                    expires_at=expires_at,
                    created_from=created_from_value,
                )
                watches[watch_id] = record
            self._rebuild_return_watch_indexes()
            await self.store.flush()
        return True, record, refreshed

    def validate_watch_keyword(self, raw_keyword: str) -> tuple[bool, str]:
        cleaned = normalize_plain_text(raw_keyword)
        if not cleaned:
            return False, "Keyword cannot be empty."
        if len(cleaned) < 2:
            return False, "Keyword must be at least 2 characters."
        if len(cleaned) > WATCH_KEYWORD_MAX_LEN:
            return False, f"Keyword must be {WATCH_KEYWORD_MAX_LEN} characters or fewer."
        if find_private_pattern(cleaned) is not None:
            return False, "Keywords cannot contain mentions, links, invites, or other private-looking text."
        if not any(ch.isalnum() for ch in cleaned):
            return False, "Keyword must include letters or numbers."
        token = cleaned.replace(" ", "")
        if token.isdigit():
            return False, "Keyword cannot be only numbers."
        if len(token) >= 4 and len(set(token.casefold())) == 1:
            return False, "Keyword is too repetitive to be useful."
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_WATCH_KEYWORD, cleaned)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or "That keyword is not allowed."
        return True, cleaned

    def _watch_scope_label(self, scope: str) -> str:
        labels = {
            "channel": "this channel",
            "server": "this server",
            "global": "global",
        }
        return labels.get(scope, "that scope")

    def _resolve_watch_scope(self, *, guild_id: int | None, channel_id: int | None, scope: str) -> tuple[int | None, int | None] | tuple[None, None]:
        if scope == "global":
            return None, None
        if scope == "server":
            if guild_id is None:
                return None, None
            return guild_id, None
        if scope == "channel":
            if guild_id is None or channel_id is None:
                return None, None
            return guild_id, channel_id
        return None, None

    def _sorted_unique_ints(self, values) -> list[int]:
        return sorted({value for value in values if isinstance(value, int) and value > 0})

    def _upsert_watch_scope_target(self, items: list[int], *, target_id: int, enabled: bool) -> list[int]:
        values = {value for value in items if isinstance(value, int)}
        if enabled:
            values.add(target_id)
        else:
            values.discard(target_id)
        return sorted(values)

    def _check_watch_target_limits(self, user_id: int, items: list[int], *, item_name: str, previous_count: int) -> tuple[bool, str | None]:
        next_count = len(self._sorted_unique_ints(items))
        limit_value = self.watch_filter_limit(user_id)
        if self._count_change_exceeds_limit(previous_count=previous_count, next_count=next_count, limit_value=limit_value):
            return False, self._premium_limit_error(
                limit_key=LIMIT_WATCH_FILTERS,
                limit_value=limit_value,
                default_message=f"You can keep up to {limit_value} watched {item_name}.",
            )
        return True, None

    def _watch_filters_block(self, user_id: int, *, author_id: int, channel_id: int) -> bool:
        if channel_id in self._excluded_channels_by_user.get(user_id, set()):
            return True
        if author_id in self._ignored_users_by_user.get(user_id, set()):
            return True
        return False

    def _record_watch_alert(self, user_id: int, trigger_labels: set[str]):
        counts = self._watch_alert_counts.setdefault(user_id, {"mentions": 0, "replies": 0, "keywords": 0, "total": 0})
        if "Mention" in trigger_labels:
            counts["mentions"] += 1
        if "Reply" in trigger_labels:
            counts["replies"] += 1
        if "Keyword" in trigger_labels:
            counts["keywords"] += 1
        counts["total"] += 1

    def _channel_belongs_to_guild(self, channel_id: int, guild_id: int) -> bool:
        get_channel = getattr(self.bot, "get_channel", None)
        if get_channel is None:
            return False
        channel = get_channel(channel_id)
        channel_guild = getattr(channel, "guild", None)
        return getattr(channel_guild, "id", None) == guild_id

    async def set_watch_mentions(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        channel_id: int | None,
        scope: str,
        enabled: bool,
    ) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            if scope == "global":
                config["mention_global"] = enabled
            elif scope == "server":
                if guild_id is None:
                    return False, "Server-scoped mention watch can only be changed inside a server."
                config["mention_guild_ids"] = self._upsert_watch_scope_target(
                    config.get("mention_guild_ids", []),
                    target_id=guild_id,
                    enabled=enabled,
                )
            elif scope == "channel":
                if guild_id is None or channel_id is None:
                    return False, "Channel-scoped mention watch can only be changed inside a server channel."
                previous_count = len(self._sorted_unique_ints(config.get("mention_channel_ids", [])))
                config["mention_channel_ids"] = self._upsert_watch_scope_target(
                    config.get("mention_channel_ids", []),
                    target_id=channel_id,
                    enabled=enabled,
                )
                ok, error = self._check_watch_target_limits(
                    user_id,
                    config["mention_channel_ids"],
                    item_name="watch channels",
                    previous_count=previous_count,
                )
                if not ok:
                    return False, error
            else:
                return False, "Unknown watch scope."
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, f"Mention alerts are now {'enabled' if enabled else 'disabled'} for {self._watch_scope_label(scope)}."

    async def set_watch_replies(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        channel_id: int | None,
        scope: str,
        enabled: bool,
    ) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            if scope == "global":
                config["reply_global"] = enabled
            elif scope == "server":
                if guild_id is None:
                    return False, "Server-scoped reply watch can only be changed inside a server."
                config["reply_guild_ids"] = self._upsert_watch_scope_target(
                    config.get("reply_guild_ids", []),
                    target_id=guild_id,
                    enabled=enabled,
                )
            elif scope == "channel":
                if guild_id is None or channel_id is None:
                    return False, "Channel-scoped reply watch can only be changed inside a server channel."
                previous_count = len(self._sorted_unique_ints(config.get("reply_channel_ids", [])))
                config["reply_channel_ids"] = self._upsert_watch_scope_target(
                    config.get("reply_channel_ids", []),
                    target_id=channel_id,
                    enabled=enabled,
                )
                ok, error = self._check_watch_target_limits(
                    user_id,
                    config["reply_channel_ids"],
                    item_name="reply channels",
                    previous_count=previous_count,
                )
                if not ok:
                    return False, error
            else:
                return False, "Unknown watch scope."
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, f"Reply alerts are now {'enabled' if enabled else 'disabled'} for {self._watch_scope_label(scope)}."

    async def add_watch_ignored_channel(self, user_id: int, *, channel_id: int | None) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        if channel_id is None:
            return False, "Channel ignores can only be changed inside a server channel."
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            previous_count = len(self._sorted_unique_ints(config.get("excluded_channel_ids", [])))
            ignored_channels = self._upsert_watch_scope_target(
                config.get("excluded_channel_ids", []),
                target_id=channel_id,
                enabled=True,
            )
            ok, error = self._check_watch_target_limits(
                user_id,
                ignored_channels,
                item_name="ignored channels",
                previous_count=previous_count,
            )
            if not ok:
                return False, error
            config["excluded_channel_ids"] = ignored_channels
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, "This channel is now excluded from Watch alerts."

    async def remove_watch_ignored_channel(self, user_id: int, *, channel_id: int | None) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        if channel_id is None:
            return False, "Channel ignores can only be changed inside a server channel."
        async with self._lock:
            config = self._watch_config(user_id)
            if config is None:
                return False, "You do not have any Watch filters saved."
            before = list(config.get("excluded_channel_ids", []))
            config["excluded_channel_ids"] = self._upsert_watch_scope_target(before, target_id=channel_id, enabled=False)
            if len(before) == len(config["excluded_channel_ids"]):
                return False, "This channel is not on your ignore list."
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, "This channel will trigger Watch alerts again."

    async def add_watch_ignored_user(self, user_id: int, *, ignored_user_id: int) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        if ignored_user_id == user_id:
            return False, "You cannot ignore yourself."
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            previous_count = len(self._sorted_unique_ints(config.get("ignored_user_ids", [])))
            ignored_users = self._upsert_watch_scope_target(
                config.get("ignored_user_ids", []),
                target_id=ignored_user_id,
                enabled=True,
            )
            ok, error = self._check_watch_target_limits(
                user_id,
                ignored_users,
                item_name="ignored users",
                previous_count=previous_count,
            )
            if not ok:
                return False, error
            config["ignored_user_ids"] = ignored_users
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, "That user is now ignored by Watch."

    async def remove_watch_ignored_user(self, user_id: int, *, ignored_user_id: int) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        async with self._lock:
            config = self._watch_config(user_id)
            if config is None:
                return False, "You do not have any Watch filters saved."
            before = list(config.get("ignored_user_ids", []))
            config["ignored_user_ids"] = self._upsert_watch_scope_target(before, target_id=ignored_user_id, enabled=False)
            if len(before) == len(config["ignored_user_ids"]):
                return False, "That user is not on your ignore list."
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, "That user can trigger Watch alerts again."

    async def add_watch_keyword(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        channel_id: int | None,
        phrase: str,
        scope: str,
        mode: str,
    ) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        valid, cleaned = self.validate_watch_keyword(phrase)
        if not valid:
            return False, cleaned
        if mode not in {"contains", "word"}:
            return False, "Keyword mode must be either `contains` or `word`."
        target_guild_id, target_channel_id = self._resolve_watch_scope(guild_id=guild_id, channel_id=channel_id, scope=scope)
        if scope != "global" and target_guild_id is None:
            return False, f"{self._watch_scope_label(scope).capitalize()} keywords can only be added inside a server."
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            keywords = list(config.get("keywords", []))
            keyword_limit = self.watch_keyword_limit(user_id)
            if len(keywords) >= keyword_limit:
                return False, self._premium_limit_error(
                    limit_key=LIMIT_WATCH_KEYWORDS,
                    limit_value=keyword_limit,
                    default_message=f"You can store up to {keyword_limit} watch keywords.",
                )
            duplicate = next(
                (
                    item
                    for item in keywords
                    if normalize_plain_text(item.get("phrase")) == cleaned
                    and item.get("mode", "contains") == mode
                    and item.get("guild_id") == target_guild_id
                    and item.get("channel_id") == target_channel_id
                ),
                None,
            )
            if duplicate is not None:
                return False, "That keyword is already watched in that scope."
            keywords.append(
                {
                    "phrase": cleaned,
                    "mode": mode,
                    "guild_id": target_guild_id,
                    "channel_id": target_channel_id,
                    "created_at": serialize_datetime(ge.now_utc()),
                }
            )
            config["keywords"] = keywords
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, f"Watching `{cleaned}` in {self._watch_scope_label(scope)} using {'whole-word' if mode == 'word' else 'contains'} matching."

    async def remove_watch_keyword(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        channel_id: int | None,
        phrase: str,
        scope: str,
    ) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        cleaned = normalize_plain_text(phrase)
        if not cleaned:
            return False, "Keyword cannot be empty."
        target_guild_id, target_channel_id = self._resolve_watch_scope(guild_id=guild_id, channel_id=channel_id, scope=scope)
        if scope != "global" and target_guild_id is None:
            return False, f"{self._watch_scope_label(scope).capitalize()} keywords can only be removed inside a server."
        async with self._lock:
            config = self._watch_config(user_id)
            if config is None:
                return False, "You do not have any watch keywords yet."
            keywords = list(config.get("keywords", []))
            new_keywords = [
                item
                for item in keywords
                if not (
                    normalize_plain_text(item.get("phrase")) == cleaned
                    and item.get("guild_id") == target_guild_id
                    and item.get("channel_id") == target_channel_id
                )
            ]
            if len(new_keywords) == len(keywords):
                return False, "No matching keyword was found in that scope."
            config["keywords"] = new_keywords
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, f"Stopped watching `{cleaned}`."

    async def disable_watch(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        channel_id: int | None,
        scope: str,
    ) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Watch")
        async with self._lock:
            if scope == "all":
                self.store.state.get("watch", {}).pop(str(user_id), None)
                self._rebuild_watch_indexes()
                await self.store.flush()
                return True, "All watch settings were cleared."
            config = self._watch_config(user_id)
            if config is None:
                return False, "You do not have any watch settings saved."
            if scope == "global":
                config["mention_global"] = False
                config["reply_global"] = False
                config["keywords"] = [item for item in config.get("keywords", []) if item.get("guild_id") is not None]
                label = "global watch settings"
            elif scope == "server":
                if guild_id is None:
                    return False, "Server-scoped watch settings can only be cleared inside a server."
                guild_ids = {value for value in config.get("mention_guild_ids", []) if isinstance(value, int)}
                guild_ids.discard(guild_id)
                config["mention_guild_ids"] = sorted(guild_ids)
                reply_guild_ids = {value for value in config.get("reply_guild_ids", []) if isinstance(value, int)}
                reply_guild_ids.discard(guild_id)
                config["reply_guild_ids"] = sorted(reply_guild_ids)
                config["mention_channel_ids"] = [
                    value for value in config.get("mention_channel_ids", [])
                    if not self._channel_belongs_to_guild(value, guild_id)
                ]
                config["reply_channel_ids"] = [
                    value for value in config.get("reply_channel_ids", [])
                    if not self._channel_belongs_to_guild(value, guild_id)
                ]
                config["keywords"] = [
                    item
                    for item in config.get("keywords", [])
                    if item.get("guild_id") != guild_id
                ]
                label = "watch settings for this server"
            elif scope == "channel":
                if guild_id is None or channel_id is None:
                    return False, "Channel-scoped watch settings can only be cleared inside a server channel."
                config["mention_channel_ids"] = [value for value in config.get("mention_channel_ids", []) if value != channel_id]
                config["reply_channel_ids"] = [value for value in config.get("reply_channel_ids", []) if value != channel_id]
                config["excluded_channel_ids"] = [value for value in config.get("excluded_channel_ids", []) if value != channel_id]
                config["keywords"] = [
                    item for item in config.get("keywords", []) if item.get("channel_id") != channel_id
                ]
                label = "watch settings for this channel"
            else:
                return False, "Unknown watch scope."
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()
        return True, f"Cleared {label}."

    def get_watch_summary(self, user_id: int, *, guild_id: int | None, channel_id: int | None = None) -> dict:
        config = self._watch_config(user_id) or _watch_default_config()
        mention_guild_ids = {value for value in config.get("mention_guild_ids", []) if isinstance(value, int)}
        mention_channel_ids = {value for value in config.get("mention_channel_ids", []) if isinstance(value, int)}
        reply_guild_ids = {value for value in config.get("reply_guild_ids", []) if isinstance(value, int)}
        reply_channel_ids = {value for value in config.get("reply_channel_ids", []) if isinstance(value, int)}
        excluded_channel_ids = {value for value in config.get("excluded_channel_ids", []) if isinstance(value, int)}
        ignored_user_ids = {value for value in config.get("ignored_user_ids", []) if isinstance(value, int)}
        keywords = list(config.get("keywords", []))
        recent_counts = dict(self._watch_alert_counts.get(user_id, {"mentions": 0, "replies": 0, "keywords": 0, "total": 0}))
        return {
            "mention_global": bool(config.get("mention_global")),
            "mention_server_enabled": guild_id in mention_guild_ids if guild_id is not None else False,
            "mention_channel_enabled": channel_id in mention_channel_ids if channel_id is not None else False,
            "reply_global": bool(config.get("reply_global")),
            "reply_server_enabled": guild_id in reply_guild_ids if guild_id is not None else False,
            "reply_channel_enabled": channel_id in reply_channel_ids if channel_id is not None else False,
            "global_keywords": [item for item in keywords if item.get("guild_id") is None],
            "server_keywords": [
                item for item in keywords
                if guild_id is not None and item.get("guild_id") == guild_id and item.get("channel_id") is None
            ],
            "channel_keywords": [
                item for item in keywords
                if channel_id is not None and item.get("channel_id") == channel_id
            ],
            "ignored_channel_ids": sorted(excluded_channel_ids),
            "ignored_user_ids": sorted(ignored_user_ids),
            "mention_channel_ids": sorted(mention_channel_ids),
            "reply_channel_ids": sorted(reply_channel_ids),
            "total_keywords": len(keywords),
            "recent_counts": recent_counts,
        }

    def _member_can_access_message(self, member: discord.Member, message: discord.Message) -> bool:
        if member.id == message.author.id:
            return False
        perms = message.channel.permissions_for(member)
        return perms.view_channel and perms.read_message_history

    def _watch_mentioned_members(self, message: discord.Message) -> dict[int, discord.Member]:
        mentioned_members: dict[int, discord.Member] = {}
        for member in getattr(message, "mentions", []) or []:
            member_id = getattr(member, "id", None)
            if member_id is None or member_id == message.author.id or getattr(member, "bot", False):
                continue
            mentioned_members[int(member_id)] = member
        return mentioned_members

    def _watch_explicit_mention_user_ids(self, message: discord.Message) -> set[int]:
        raw_mentions = getattr(message, "raw_mentions", ...)
        if isinstance(raw_mentions, (list, tuple, set)):
            return {
                int(user_id)
                for user_id in raw_mentions
                if isinstance(user_id, int) and user_id != message.author.id
            }
        # Synthetic test messages may not expose raw_mentions. In that case,
        # fall back to the provided mention objects rather than guessing from content.
        return {
            int(member.id)
            for member in getattr(message, "mentions", []) or []
            if getattr(member, "id", None) is not None
            and member.id != message.author.id
            and not getattr(member, "bot", False)
        }

    def _watch_reply_target(self, message: discord.Message) -> tuple[int | None, discord.Member | None]:
        if getattr(message, "type", discord.MessageType.default) != discord.MessageType.reply:
            return None, None
        reference = getattr(message, "reference", None)
        if reference is None:
            return None, None
        resolved = getattr(reference, "resolved", None)
        cached_message = getattr(reference, "cached_message", None)
        reply_message = resolved if isinstance(resolved, discord.Message) else cached_message
        reply_author = getattr(reply_message, "author", None)
        reply_author_id = getattr(reply_author, "id", None)
        if (
            reply_author_id is None
            or reply_author_id == message.author.id
            or getattr(reply_author, "bot", False)
        ):
            return None, None
        return int(reply_author_id), reply_author

    def _member_can_access_channel(self, member: discord.Member, channel) -> bool:
        perms = channel.permissions_for(member)
        return perms.view_channel and perms.read_message_history

    def _build_return_watch_alert_embed(self, message: discord.Message, *, watch_types: set[str]) -> discord.Embed:
        channel_name = getattr(message.channel, "mention", "#unknown")
        guild_name = message.guild.name if message.guild else "Direct Messages"
        description = f"{channel_name} has a new message."
        if "user" in watch_types:
            description = f"{ge.display_name_of(message.author)} is active again in {channel_name}."
        embed = discord.Embed(
            title="Babblebox Return Ping",
            description=description,
            color=ge.EMBED_THEME["accent"],
            timestamp=message.created_at or ge.now_utc(),
        )
        embed.add_field(name="Server", value=guild_name, inline=True)
        embed.add_field(name="From", value=ge.display_name_of(message.author), inline=True)
        embed.add_field(name="Peek", value=make_message_preview(message.content, attachments=message.attachments), inline=False)
        return ge.style_embed(embed, footer="Babblebox Watch | One-shot DM alert with a jump link")

    async def handle_return_watch_message(self, message: discord.Message):
        if not self.storage_ready or message.guild is None:
            return
        candidate_ids = set(self._return_channel_watch_ids_by_target.get(message.channel.id, set()))
        candidate_ids.update(self._return_user_watch_ids_by_target.get((message.guild.id, message.author.id), set()))
        if not candidate_ids:
            return

        message_time = message.created_at or ge.now_utc()
        matched_by_watcher: defaultdict[int, set[str]] = defaultdict(set)
        remove_ids: set[str] = set()

        async with self._lock:
            watches = self.store.state.get("return_watches", {})
            for watch_id in candidate_ids:
                record = watches.get(watch_id)
                if not isinstance(record, dict):
                    remove_ids.add(watch_id)
                    continue
                target_type = record.get("target_type")
                if target_type not in {"user", "channel"}:
                    remove_ids.add(watch_id)
                    continue
                if int(record.get("guild_id", 0) or 0) != message.guild.id:
                    continue
                if self._return_watch_is_expired(record, now=message_time):
                    remove_ids.add(watch_id)
                    continue
                created_at = deserialize_datetime(record.get("created_at"))
                if created_at is None or message_time <= created_at:
                    continue
                remove_ids.add(watch_id)
                watcher_user_id = record.get("watcher_user_id")
                if isinstance(watcher_user_id, int):
                    matched_by_watcher[watcher_user_id].add(target_type)

            if remove_ids:
                for watch_id in remove_ids:
                    watches.pop(watch_id, None)
                self._rebuild_return_watch_indexes()
                await self.store.flush()

        if not matched_by_watcher:
            return

        for watcher_user_id, watch_types in matched_by_watcher.items():
            watcher = message.guild.get_member(watcher_user_id)
            if watcher is None or not self._member_can_access_channel(watcher, message.channel):
                continue
            embed = self._build_return_watch_alert_embed(message, watch_types=watch_types)
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await watcher.send(embed=embed, view=build_jump_view(message.jump_url, label="Open Message"))

    def _prune_hot_path_caches(self, now: float):
        if len(self._watch_dedup) > 512:
            self._watch_dedup = {key: timestamp for key, timestamp in self._watch_dedup.items() if now - timestamp < WATCH_DEDUP_TTL_SECONDS}
        if len(self._watch_dm_cooldowns) > 256:
            self._watch_dm_cooldowns = {key: timestamp for key, timestamp in self._watch_dm_cooldowns.items() if now - timestamp < WATCH_DM_COOLDOWN_SECONDS * 3}
        if len(self._afk_notice_cooldowns) > 256:
            self._afk_notice_cooldowns = {key: timestamp for key, timestamp in self._afk_notice_cooldowns.items() if now - timestamp < AFK_NOTICE_COOLDOWN_SECONDS * 3}
        if len(self._watch_alert_counts) > 256:
            active_user_ids = {int(user_id_text) for user_id_text in self.store.state.get("watch", {}).keys()}
            self._watch_alert_counts = {user_id: payload for user_id, payload in self._watch_alert_counts.items() if user_id in active_user_ids}

    async def handle_watch_message(self, message: discord.Message):
        if not self.storage_ready or message.guild is None:
            return
        if not (
            self._mention_global
            or self._mention_by_guild
            or self._mention_by_channel
            or self._reply_global
            or self._reply_by_guild
            or self._reply_by_channel
            or self._keywords_global
            or self._keywords_by_guild
            or self._keywords_by_channel
        ):
            return
        alerts: dict[int, dict[str, set[str]]] = {}
        guild_id = message.guild.id
        channel_id = message.channel.id
        mentioned_members = self._watch_mentioned_members(message)
        explicit_mention_user_ids = self._watch_explicit_mention_user_ids(message)
        watched_mentions = self._mention_by_guild.get(guild_id, set())
        watched_mention_channels = self._mention_by_channel.get(channel_id, set())
        for user_id in explicit_mention_user_ids:
            if user_id in self._mention_global or user_id in watched_mentions or user_id in watched_mention_channels:
                if self._watch_filters_block(user_id, author_id=message.author.id, channel_id=channel_id):
                    continue
                member = mentioned_members.get(user_id) or message.guild.get_member(user_id)
                if member is not None and self._member_can_access_message(member, message):
                    alerts.setdefault(user_id, {"triggers": set(), "keywords": set()})["triggers"].add("Mention")

        reply_author_id, reply_author = self._watch_reply_target(message)
        if (
            reply_author_id is not None
            and (
                reply_author_id in self._reply_global
                or reply_author_id in self._reply_by_guild.get(guild_id, set())
                or reply_author_id in self._reply_by_channel.get(channel_id, set())
            )
        ):
            if not self._watch_filters_block(reply_author_id, author_id=message.author.id, channel_id=channel_id):
                reply_member = mentioned_members.get(reply_author_id) or message.guild.get_member(reply_author_id)
                if reply_member is not None and self._member_can_access_message(reply_member, message):
                    alerts.setdefault(reply_author_id, {"triggers": set(), "keywords": set()})["triggers"].add("Reply")

        content = normalize_plain_text(message.content).casefold()
        if content:
            keyword_candidates: defaultdict[int, list[dict]] = defaultdict(list)
            for user_id, entries in self._keywords_global.items():
                keyword_candidates[user_id].extend(entries)
            for user_id, entries in self._keywords_by_guild.get(guild_id, {}).items():
                keyword_candidates[user_id].extend(entries)
            for user_id, entries in self._keywords_by_channel.get(channel_id, {}).items():
                keyword_candidates[user_id].extend(entries)
            for user_id, entries in keyword_candidates.items():
                if user_id == message.author.id:
                    continue
                if self._watch_filters_block(user_id, author_id=message.author.id, channel_id=channel_id):
                    continue
                member = mentioned_members.get(user_id) or message.guild.get_member(user_id)
                if member is None or not self._member_can_access_message(member, message):
                    continue
                matched = {entry["phrase"] for entry in entries if entry["matcher"](content)}
                if matched:
                    payload = alerts.setdefault(user_id, {"triggers": set(), "keywords": set()})
                    payload["triggers"].add("Keyword")
                    payload["keywords"].update(matched)
        if not alerts:
            return
        now = asyncio.get_running_loop().time()
        self._prune_hot_path_caches(now)
        for user_id, payload in alerts.items():
            if self._watch_dedup.get((user_id, message.id)):
                continue
            if now - self._watch_dm_cooldowns.get(user_id, 0.0) < WATCH_DM_COOLDOWN_SECONDS:
                continue
            self._watch_dedup[(user_id, message.id)] = now
            self._watch_dm_cooldowns[user_id] = now
            if await self._send_watch_alert(user_id, message, payload):
                self._record_watch_alert(user_id, payload["triggers"])

    async def _send_watch_alert(self, user_id: int, message: discord.Message, payload: dict[str, set[str]]) -> bool:
        recipient = message.guild.get_member(user_id) or self.bot.get_user(user_id)
        if recipient is None:
            return False
        embed = build_watch_alert_embed(message, trigger_labels=sorted(payload["triggers"]), matched_keywords=sorted(payload["keywords"]))
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await recipient.send(embed=embed, view=build_jump_view(message.jump_url))
            return True
        return False

    async def save_later_marker(self, *, user: discord.abc.User, channel: discord.abc.GuildChannel, message: discord.Message) -> tuple[bool, str | dict]:
        if not self._has_storage():
            return False, self.storage_message("Later")
        marker = {
            "user_id": user.id,
            "guild_id": channel.guild.id,
            "guild_name": channel.guild.name,
            "channel_id": channel.id,
            "channel_name": channel.name,
            "message_id": message.id,
            "message_jump_url": message.jump_url,
            "message_created_at": serialize_datetime(message.created_at or ge.now_utc()),
            "saved_at": serialize_datetime(ge.now_utc()),
            "author_name": ge.display_name_of(message.author),
            "author_id": message.author.id,
            "preview": make_message_preview(message.content, attachments=message.attachments, limit=280),
            "attachment_labels": make_attachment_labels(message, include_urls=True),
        }
        async with self._lock:
            self.store.state.setdefault("later", {}).setdefault(str(user.id), {})[str(channel.id)] = marker
            await self.store.flush()
        return True, marker

    def list_later_markers(self, user_id: int, *, guild_id: int | None = None) -> list[dict]:
        if not self.storage_ready:
            return []
        markers = list((self.store.state.get("later", {}).get(str(user_id), {}) or {}).values())
        output = [marker for marker in markers if isinstance(marker, dict) and (guild_id is None or marker.get("guild_id") == guild_id)]
        output.sort(key=lambda item: item.get("saved_at", ""), reverse=True)
        return output

    async def clear_later_marker(self, user_id: int, *, channel_id: int | None = None) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Later")
        async with self._lock:
            per_user = self.store.state.get("later", {}).get(str(user_id))
            if not isinstance(per_user, dict) or not per_user:
                return False, "You do not have any Later markers saved."
            if channel_id is None:
                self.store.state.get("later", {}).pop(str(user_id), None)
                await self.store.flush()
                return True, "All of your Later markers were cleared."
            removed = per_user.pop(str(channel_id), None)
            if removed is None:
                return False, "There is no Later marker saved for this channel."
            if not per_user:
                self.store.state.get("later", {}).pop(str(user_id), None)
            await self.store.flush()
        return True, "Your Later marker for this channel was cleared."

    def can_run_capture(self, user_id: int) -> tuple[bool, str | None]:
        now = asyncio.get_running_loop().time()
        remaining = CAPTURE_COOLDOWN_SECONDS - (now - self._capture_cooldowns.get(user_id, 0.0))
        if remaining > 0:
            return False, f"Capture is on cooldown. Try again in about {int(remaining)} seconds."
        self._capture_cooldowns[user_id] = now
        return True, None

    def parse_relative_duration(self, raw: str | None) -> int | None:
        return parse_duration_string(raw)

    def list_reminders(self, user_id: int) -> list[dict]:
        if not self.storage_ready:
            return []
        reminders = [item for item in self.store.state.get("reminders", {}).values() if isinstance(item, dict) and item.get("user_id") == user_id]
        reminders.sort(key=lambda item: item.get("due_at", ""))
        return reminders

    async def create_reminder(self, *, user: discord.abc.User, text: str, delay_seconds: int, delivery: str, guild: discord.Guild | None, channel: discord.abc.GuildChannel | discord.DMChannel | discord.Thread | None, origin_jump_url: str | None) -> tuple[bool, str | dict]:
        if not self._has_storage():
            return False, self.storage_message("Reminders")
        if delivery not in {"dm", "here"}:
            return False, "Reminder delivery must be either `dm` or `here`."
        max_length = 80 if delivery == "here" else REMINDER_TEXT_MAX_LEN
        sentence_limit = 1 if delivery == "here" else 2
        valid, cleaned_or_error = sanitize_short_plain_text(
            text,
            field_name="Reminder text",
            max_length=max_length,
            sentence_limit=sentence_limit,
            reject_blocklist=False,
            allow_empty=False,
        )
        if not valid:
            return False, cleaned_or_error
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_REMINDER_CREATE, cleaned_or_error)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or "That reminder text is not allowed."
        if delay_seconds < REMINDER_MIN_SECONDS or delay_seconds > REMINDER_MAX_SECONDS:
            return False, f"Reminders must be between {format_duration_brief(REMINDER_MIN_SECONDS)} and {format_duration_brief(REMINDER_MAX_SECONDS)}."
        if delivery == "here" and delay_seconds < REMINDER_PUBLIC_MIN_SECONDS:
            return False, f"Channel reminders must be scheduled at least {format_duration_brief(REMINDER_PUBLIC_MIN_SECONDS)} ahead."
        now = asyncio.get_running_loop().time()
        remaining = REMINDER_COOLDOWN_SECONDS - (now - self._reminder_cooldowns.get(user.id, 0.0))
        if remaining > 0:
            return False, f"Reminder creation is on cooldown. Try again in about {int(remaining)} seconds."
        active = [item for item in self.store.state.get("reminders", {}).values() if isinstance(item, dict) and item.get("user_id") == user.id]
        reminder_limit = self.reminder_limit(user.id)
        if len(active) >= reminder_limit:
            return False, self._premium_limit_error(
                limit_key=LIMIT_REMINDERS_ACTIVE,
                limit_value=reminder_limit,
                default_message=f"You can keep up to {reminder_limit} active reminders.",
            )
        public_active = [item for item in active if item.get("delivery") == "here"]
        public_limit = self.public_reminder_limit(user.id)
        if delivery == "here" and len(public_active) >= public_limit:
            return False, self._premium_limit_error(
                limit_key=LIMIT_REMINDERS_PUBLIC_ACTIVE,
                limit_value=public_limit,
                default_message=f"You can keep only {public_limit} active channel reminder at a time.",
            )
        created_at = ge.now_utc()
        due_at = created_at + timedelta(seconds=delay_seconds)
        reminder_id = uuid.uuid4().hex
        record = {
            "id": reminder_id,
            "user_id": user.id,
            "text": cleaned_or_error,
            "delivery": delivery,
            "created_at": serialize_datetime(created_at),
            "due_at": serialize_datetime(due_at),
            "guild_id": guild.id if guild is not None else None,
            "guild_name": guild.name if guild is not None else None,
            "channel_id": getattr(channel, "id", None) if delivery == "here" else None,
            "channel_name": getattr(channel, "name", None) if delivery == "here" else None,
            "origin_jump_url": origin_jump_url if guild is not None else None,
            "delivery_attempts": 0,
            "last_attempt_at": None,
            "retry_after": None,
        }
        async with self._lock:
            self.store.state.setdefault("reminders", {})[reminder_id] = record
            await self.store.flush()
            self._wake_event.set()
        self._reminder_cooldowns[user.id] = now
        return True, record

    async def cancel_reminder(self, user_id: int, reminder_id_prefix: str) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("Reminders")
        reminder_id_prefix = reminder_id_prefix.strip().lower()
        if not reminder_id_prefix:
            return False, "Provide the reminder ID from `/remind list`."
        async with self._lock:
            matches = [reminder_id for reminder_id, record in self.store.state.get("reminders", {}).items() if isinstance(record, dict) and record.get("user_id") == user_id and reminder_id.lower().startswith(reminder_id_prefix)]
            if not matches:
                return False, "No reminder matched that ID."
            if len(matches) > 1:
                return False, "That ID prefix matches multiple reminders. Use a longer ID."
            self.store.state.get("reminders", {}).pop(matches[0], None)
            await self.store.flush()
            self._wake_event.set()
        return True, f"Reminder `{matches[0][:8]}` was cancelled."

    async def clear_afk(self, user_id: int, *, active_only: bool = False) -> tuple[bool, dict | None]:
        if not self._has_storage():
            return False, None
        async with self._lock:
            record = self.store.state.get("afk", {}).get(str(user_id))
            if not isinstance(record, dict):
                return False, None
            if active_only and record.get("status") != "active":
                return False, None
            removed = self.store.state.get("afk", {}).pop(str(user_id), None)
            await self.store.flush()
            self._wake_event.set()
        return True, removed

    async def clear_afk_on_activity(self, user_id: int) -> dict | None:
        ok, removed = await self.clear_afk(user_id, active_only=True)
        return removed if ok else None

    def build_afk_status_embed_for(self, user: discord.abc.User, record: dict, *, title: str | None = None) -> discord.Embed:
        return build_afk_status_embed(user, record, title=title)

    def collect_afk_notice_targets(self, *, channel_id: int, author_id: int, targets: list[discord.abc.User]) -> list[tuple[discord.abc.User, dict]]:
        if not self.storage_ready:
            return []
        now = asyncio.get_running_loop().time()
        self._prune_hot_path_caches(now)
        notices: list[tuple[discord.abc.User, dict]] = []
        seen: set[int] = set()
        for member in targets:
            if member.id == author_id or member.bot or member.id in seen:
                continue
            record = self.get_active_afk_record(member.id)
            if record is None:
                continue
            cooldown_key = (channel_id, member.id)
            if now - self._afk_notice_cooldowns.get(cooldown_key, 0.0) < AFK_NOTICE_COOLDOWN_SECONDS:
                continue
            self._afk_notice_cooldowns[cooldown_key] = now
            seen.add(member.id)
            notices.append((member, record))
            if len(notices) >= 5:
                break
        return notices

    def build_afk_notice_lines_for_targets(self, *, channel_id: int, author_id: int, targets: list[discord.abc.User]) -> list[str]:
        return [
            build_afk_notice_line(member, record)
            for member, record in self.collect_afk_notice_targets(
                channel_id=channel_id,
                author_id=author_id,
                targets=targets,
            )
        ]

    async def _wait_for_ready_state(self) -> bool:
        while True:
            try:
                await self.bot.wait_until_ready()
                return True
            except RuntimeError:
                if self.bot.is_closed():
                    return False
                await asyncio.sleep(0.5)

    def _build_reminder_retry_update(self, record: dict, *, now: datetime | None = None) -> dict[str, int | str | None]:
        current_time = now or ge.now_utc()
        previous_attempts = record.get("delivery_attempts", 0)
        attempts = int(previous_attempts) if isinstance(previous_attempts, int) and previous_attempts >= 0 else 0
        attempts += 1
        backoff_seconds = min(
            REMINDER_RETRY_MAX_SECONDS,
            REMINDER_RETRY_BASE_SECONDS * (2 ** min(attempts - 1, 5)),
        )
        return {
            "delivery_attempts": attempts,
            "last_attempt_at": serialize_datetime(current_time),
            "retry_after": serialize_datetime(current_time + timedelta(seconds=backoff_seconds)),
        }

    async def _deliver_due_reminders(self, reminders: list[dict]):
        delivered_ids: list[str] = []
        retry_updates: dict[str, dict[str, int | str | None]] = {}
        now = ge.now_utc()
        for record in reminders:
            reminder_id = record.get("id")
            if not isinstance(reminder_id, str):
                continue
            if await self._deliver_single_reminder(record):
                delivered_ids.append(reminder_id)
            else:
                retry_updates[reminder_id] = self._build_reminder_retry_update(record, now=now)
        if not delivered_ids and not retry_updates:
            return
        async with self._lock:
            reminders_state = self.store.state.get("reminders", {})
            dirty = False
            for reminder_id in delivered_ids:
                if reminders_state.pop(reminder_id, None) is not None:
                    dirty = True
            for reminder_id, update in retry_updates.items():
                current = reminders_state.get(reminder_id)
                if not isinstance(current, dict):
                    continue
                current.update(update)
                dirty = True
            if dirty:
                await self.store.flush()
                self._wake_event.set()

    async def _deliver_single_reminder(self, record: dict) -> bool:
        due_at = deserialize_datetime(record.get("due_at"))
        delayed = bool(due_at is not None and (ge.now_utc() - due_at).total_seconds() > 120)
        embed = build_reminder_delivery_embed(record, delayed=delayed)
        user_id = record.get("user_id")
        if record.get("delivery") == "here" and isinstance(record.get("channel_id"), int):
            feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY, record.get("text"))
            if not feature_decision.allowed:
                user = self.bot.get_user(user_id)
                if user is None:
                    with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                        user = await self.bot.fetch_user(user_id)
                if user is not None:
                    with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                        await user.send(feature_decision.user_message or "Babblebox withheld that public reminder.")
                return True
            channel = self.bot.get_channel(record["channel_id"])
            if channel is None:
                with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                    channel = await self.bot.fetch_channel(record["channel_id"])
            if channel is not None:
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await channel.send(
                        content=f"<@{user_id}>",
                        embed=embed,
                        view=build_reminder_delivery_view(record, delivered_in_guild_channel=True),
                        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                    )
                    return True
        user = self.bot.get_user(user_id)
        if user is None:
            with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                user = await self.bot.fetch_user(user_id)
        if user is not None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await user.send(embed=embed, view=None)
                return True
        return False

    async def send_later_marker_dm(self, user: discord.abc.User, marker: dict):
        await user.send(embed=build_later_marker_embed(marker), view=build_jump_view(marker["message_jump_url"], label="Open Saved Message"))

    async def send_capture_dm(self, *, user: discord.abc.User, guild_name: str, channel_name: str, messages: list[discord.Message], requested_count: int):
        jump_url = messages[-1].jump_url if messages else None
        preview_lines = [
            f"[{message.created_at.strftime('%H:%M') if message.created_at else '--:--'}] "
            f"{ge.display_name_of(message.author)}: {make_message_preview(message.content, attachments=message.attachments, limit=90)}"
            for message in reversed(messages[-4:])
        ]
        embed, view = build_capture_delivery_embed(guild_name=guild_name, channel_name=channel_name, captured_count=len(messages), requested_count=requested_count, preview_lines=preview_lines, jump_url=jump_url)
        transcript = build_capture_transcript_file(guild_name=guild_name, channel_name=channel_name, messages=messages)
        await user.send(embed=embed, view=view, file=transcript)

    def _afk_settings_record(self, user_id: int, *, create: bool = False) -> dict | None:
        settings = self.store.state.setdefault("afk_settings", {})
        key = str(user_id)
        record = settings.get(key)
        if record is None and create:
            record = {}
            settings[key] = record
        return record if isinstance(record, dict) else None

    def get_afk_timezone(self, user_id: int) -> str | None:
        record = self._afk_settings_record(user_id)
        timezone_name = record.get("timezone") if isinstance(record, dict) else None
        return timezone_name if isinstance(timezone_name, str) and timezone_name.strip() else None

    async def set_afk_timezone(self, user_id: int, timezone_name: str) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        ok, canonical, error = canonicalize_afk_timezone(timezone_name)
        if not ok or canonical is None:
            return False, error or "That timezone could not be saved."
        async with self._lock:
            record = self._afk_settings_record(user_id, create=True)
            if record is None:
                return False, "Your AFK timezone could not be saved right now."
            record["timezone"] = canonical
            await self.store.flush()
        return True, canonical

    async def clear_afk_timezone(self, user_id: int) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        async with self._lock:
            removed = self.store.state.get("afk_settings", {}).pop(str(user_id), None)
            if removed is None:
                return False, "You do not have an AFK timezone saved."
            await self.store.flush()
        return True, "Your AFK timezone was cleared."

    def list_afk_schedules(self, user_id: int) -> list[dict]:
        schedules = [
            dict(record)
            for record in self.store.state.get("afk_schedules", {}).values()
            if isinstance(record, dict) and record.get("user_id") == user_id
        ]
        schedules.sort(key=lambda item: item.get("next_start_at") or "")
        return schedules

    def get_next_afk_schedule(self, user_id: int) -> dict | None:
        schedules = self.list_afk_schedules(user_id)
        return schedules[0] if schedules else None

    def build_afk_schedule_summary_line(self, schedule: dict) -> str:
        schedule_id = str(schedule.get("id", ""))[:8]
        repeat_text = format_afk_repeat_label(schedule.get("repeat"), int(schedule.get("weekday_mask", 0) or 0))
        clock_text = format_afk_clock(int(schedule.get("local_hour", 0) or 0), int(schedule.get("local_minute", 0) or 0))
        timezone_text = format_afk_timezone_label(schedule.get("timezone"))
        next_start = deserialize_datetime(schedule.get("next_start_at"))
        duration_seconds = schedule.get("duration_seconds")
        parts = [f"`{schedule_id}`", f"{repeat_text} at **{clock_text}**", f"({timezone_text})"]
        if isinstance(duration_seconds, int) and duration_seconds > 0:
            parts.append(f"for {format_duration_brief(duration_seconds)}")
        if next_start is not None:
            parts.append(f"next {ge.format_timestamp(next_start, 'R')}")
        if schedule.get("reason"):
            parts.append(ge.safe_field_text(schedule["reason"], limit=90))
        return " - ".join(parts)

    def _make_afk_record(
        self,
        *,
        user_id: int,
        status: str,
        reason: str | None,
        preset: str | None,
        created_at: datetime,
        starts_at: datetime,
        ends_at: datetime | None,
        schedule_id: str | None = None,
        occurrence_at: datetime | None = None,
    ) -> dict:
        return {
            "user_id": user_id,
            "status": status,
            "reason": reason,
            "preset": preset,
            "created_at": serialize_datetime(created_at),
            "set_at": None if status == "scheduled" else serialize_datetime(starts_at),
            "starts_at": serialize_datetime(starts_at),
            "ends_at": serialize_datetime(ends_at),
            "schedule_id": schedule_id,
            "occurrence_at": serialize_datetime(occurrence_at),
        }

    def _afk_record_window(self, record: dict) -> tuple[datetime | None, datetime | None]:
        starts_at = deserialize_datetime(record.get("starts_at")) or deserialize_datetime(record.get("set_at")) or deserialize_datetime(record.get("created_at"))
        ends_at = deserialize_datetime(record.get("ends_at"))
        return starts_at, ends_at

    def _afk_record_is_live(self, record: dict | None, *, now: datetime | None = None) -> bool:
        if not isinstance(record, dict):
            return False
        current_time = now or ge.now_utc()
        starts_at, ends_at = self._afk_record_window(record)
        if ends_at is not None and ends_at <= current_time:
            return False
        if record.get("status") == "scheduled":
            return starts_at is not None and starts_at > current_time
        return True

    def _afk_record_matches_schedule_occurrence(self, record: dict | None, schedule_id: str, occurrence_at: datetime) -> bool:
        if not isinstance(record, dict) or record.get("schedule_id") != schedule_id or record.get("status") != "active":
            return False
        current_occurrence = deserialize_datetime(record.get("occurrence_at"))
        return current_occurrence == occurrence_at

    def get_afk_record(self, user_id: int, *, include_scheduled: bool = True) -> dict | None:
        if not self.storage_ready:
            return None
        record = self.store.state.get("afk", {}).get(str(user_id))
        if not isinstance(record, dict):
            return None
        now = ge.now_utc()
        starts_at, ends_at = self._afk_record_window(record)
        if ends_at is not None and ends_at <= now:
            return None
        if record.get("status") == "scheduled":
            if starts_at is not None and starts_at <= now:
                activated = dict(record)
                activated["status"] = "active"
                activated["set_at"] = activated.get("starts_at") or activated.get("set_at") or serialize_datetime(now)
                return activated
            return record if include_scheduled and starts_at and starts_at > now else None
        return record

    def get_active_afk_record(self, user_id: int) -> dict | None:
        record = self.get_afk_record(user_id, include_scheduled=False)
        return record if record is not None and record.get("status") == "active" else None

    async def set_afk(
        self,
        *,
        user: discord.abc.User,
        reason: str | None,
        duration_seconds: int | None,
        start_in_seconds: int | None,
        start_at: datetime | None = None,
        preset: str | None = None,
        schedule_id: str | None = None,
        occurrence_at: datetime | None = None,
    ) -> tuple[bool, str | dict]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        valid, cleaned_or_error = ge.sanitize_afk_reason(reason)
        if not valid:
            return False, cleaned_or_error
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_AFK_REASON, cleaned_or_error)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or "That AFK reason is not allowed."
        created_at = ge.now_utc()
        scheduled = start_in_seconds is not None or start_at is not None
        starts_at = start_at or (created_at + timedelta(seconds=start_in_seconds) if start_in_seconds is not None else created_at)
        ends_at = starts_at + timedelta(seconds=duration_seconds) if duration_seconds is not None else None
        record = self._make_afk_record(
            user_id=user.id,
            status="scheduled" if scheduled else "active",
            reason=cleaned_or_error,
            preset=preset,
            created_at=created_at,
            starts_at=starts_at,
            ends_at=ends_at,
            schedule_id=schedule_id,
            occurrence_at=occurrence_at,
        )
        async with self._lock:
            self.store.state.setdefault("afk", {})[str(user.id)] = record
            await self.store.flush()
            self._wake_event.set()
        return True, record

    async def create_afk_schedule(
        self,
        *,
        user: discord.abc.User,
        repeat: str,
        timezone_name: str,
        local_hour: int,
        local_minute: int,
        weekday: int | None,
        reason: str | None,
        preset: str | None,
        duration_seconds: int | None,
    ) -> tuple[bool, str | dict]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        if duration_seconds is None:
            return False, "Recurring AFK schedules need a duration so Babblebox can activate and clear them reliably."
        valid, cleaned_or_error = ge.sanitize_afk_reason(reason)
        if not valid:
            return False, cleaned_or_error
        feature_decision = self._evaluate_feature_text(FEATURE_SURFACE_AFK_SCHEDULE_REASON, cleaned_or_error)
        if not feature_decision.allowed:
            return False, feature_decision.user_message or "That recurring AFK reason is not allowed."
        ok, canonical_timezone, error = canonicalize_afk_timezone(timezone_name)
        if not ok or canonical_timezone is None:
            return False, error or "That AFK timezone is invalid."
        repeat_rule = str(repeat or "").strip().casefold()
        weekday_mask = default_afk_weekday_mask(repeat_rule, weekday=weekday)
        if repeat_rule not in {"daily", "weekdays", "weekly"} or weekday_mask <= 0:
            return False, "Recurring AFK supports `daily`, `weekdays`, and `weekly` schedules."
        if not (0 <= local_hour <= 23 and 0 <= local_minute <= 59):
            return False, "Use a valid local schedule time."

        created_at = ge.now_utc()
        schedule = {
            "id": uuid.uuid4().hex,
            "user_id": user.id,
            "reason": cleaned_or_error,
            "preset": preset,
            "timezone": canonical_timezone,
            "repeat": repeat_rule,
            "weekday_mask": weekday_mask,
            "local_hour": local_hour,
            "local_minute": local_minute,
            "duration_seconds": duration_seconds,
            "created_at": serialize_datetime(created_at),
            "next_start_at": None,
        }
        schedule["next_start_at"] = serialize_datetime(compute_next_afk_schedule_start(schedule, after=created_at))

        async with self._lock:
            user_schedules = [item for item in self.store.state.get("afk_schedules", {}).values() if isinstance(item, dict) and item.get("user_id") == user.id]
            schedule_limit = self.afk_schedule_limit(user.id)
            if len(user_schedules) >= schedule_limit:
                return False, self._premium_limit_error(
                    limit_key=LIMIT_AFK_SCHEDULES,
                    limit_value=schedule_limit,
                    default_message=f"You can keep up to {schedule_limit} recurring AFK schedules.",
                )
            duplicate = next(
                (
                    item
                    for item in user_schedules
                    if item.get("repeat") == schedule["repeat"]
                    and item.get("weekday_mask") == schedule["weekday_mask"]
                    and item.get("local_hour") == schedule["local_hour"]
                    and item.get("local_minute") == schedule["local_minute"]
                    and item.get("timezone") == schedule["timezone"]
                    and item.get("duration_seconds") == schedule["duration_seconds"]
                    and item.get("preset") == schedule["preset"]
                    and item.get("reason") == schedule["reason"]
                ),
                None,
            )
            if duplicate is not None:
                return False, "You already have that AFK schedule saved."
            self.store.state.setdefault("afk_schedules", {})[schedule["id"]] = schedule
            await self.store.flush()
            self._wake_event.set()
        return True, schedule

    async def remove_afk_schedule(self, user_id: int, schedule_id_prefix: str) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        schedule_id_prefix = schedule_id_prefix.strip().lower()
        if not schedule_id_prefix:
            return False, "Provide the schedule ID from `/afkschedule list`."
        async with self._lock:
            matches = [
                schedule_id
                for schedule_id, record in self.store.state.get("afk_schedules", {}).items()
                if isinstance(record, dict) and record.get("user_id") == user_id and schedule_id.lower().startswith(schedule_id_prefix)
            ]
            if not matches:
                return False, "No recurring AFK schedule matched that ID."
            if len(matches) > 1:
                return False, "That schedule ID prefix matches multiple schedules. Use a longer ID."
            removed_id = matches[0]
            self.store.state.get("afk_schedules", {}).pop(removed_id, None)
            await self.store.flush()
            self._wake_event.set()
        return True, f"Recurring AFK schedule `{removed_id[:8]}` was removed."

    async def clear_all_afk_schedules(self, user_id: int) -> tuple[bool, str]:
        if not self._has_storage():
            return False, self.storage_message("AFK")
        async with self._lock:
            matches = [
                schedule_id
                for schedule_id, record in self.store.state.get("afk_schedules", {}).items()
                if isinstance(record, dict) and record.get("user_id") == user_id
            ]
            if not matches:
                return False, "You do not have any recurring AFK schedules saved."
            for schedule_id in matches:
                self.store.state.get("afk_schedules", {}).pop(schedule_id, None)
            await self.store.flush()
            self._wake_event.set()
        return True, f"Cleared {len(matches)} recurring AFK schedule(s)."

    async def _scheduler_loop(self):
        if not await self._wait_for_ready_state():
            return
        while True:
            self._wake_event.clear()
            due_reminders, due_bump_cycles, afk_to_activate, afk_to_expire, afk_schedule_candidates, next_due = self._collect_due_records()
            if due_reminders or due_bump_cycles or afk_to_activate or afk_to_expire or afk_schedule_candidates:
                if afk_to_activate:
                    await self._activate_due_afk(afk_to_activate)
                if afk_schedule_candidates:
                    await self._activate_due_afk_schedules(afk_schedule_candidates)
                if due_reminders:
                    await self._deliver_due_reminders(due_reminders)
                if due_bump_cycles:
                    await self._deliver_due_bump_cycles(due_bump_cycles)
                if afk_to_expire:
                    await self._expire_due_afk(afk_to_expire)
                continue
            timeout = max(1.0, (next_due - ge.now_utc()).total_seconds()) if next_due is not None else None
            try:
                if timeout is None:
                    await self._wake_event.wait()
                else:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

    def _collect_due_records(self) -> tuple[list[dict], list[dict], list[dict], list[dict], list[dict], datetime | None]:
        now = ge.now_utc()
        due_reminders: list[dict] = []
        due_bump_cycles: list[dict] = []
        afk_to_activate: list[dict] = []
        afk_to_expire: list[dict] = []
        afk_schedule_candidates: list[dict] = []
        next_due = None
        for record in self.store.state.get("reminders", {}).values():
            if not isinstance(record, dict):
                continue
            due_at = deserialize_datetime(record.get("due_at"))
            if due_at is None:
                continue
            retry_after = deserialize_datetime(record.get("retry_after"))
            if due_at <= now and (retry_after is None or retry_after <= now):
                due_reminders.append(record)
                continue
            candidate = retry_after if retry_after is not None and retry_after > now else due_at
            if candidate > now and (next_due is None or candidate < next_due):
                next_due = candidate
        for cycle in self.store.state.get("bump_cycles", {}).values():
            if not isinstance(cycle, dict):
                continue
            config = self.get_bump_config(int(cycle.get("guild_id", 0) or 0))
            provider = _normalize_bump_provider(cycle.get("provider") or config.get("provider"))
            if not config.get("enabled") or provider not in BUMP_PROVIDER_SPECS:
                continue
            due_at = deserialize_datetime(cycle.get("due_at"))
            reminder_sent_at = deserialize_datetime(cycle.get("reminder_sent_at"))
            if due_at is None or reminder_sent_at is not None:
                continue
            retry_after = deserialize_datetime(cycle.get("retry_after"))
            if due_at <= now and (retry_after is None or retry_after <= now):
                due_bump_cycles.append(cycle)
                continue
            candidate = retry_after if retry_after is not None and retry_after > now else due_at
            if candidate > now and (next_due is None or candidate < next_due):
                next_due = candidate
        for record in self.store.state.get("afk", {}).values():
            if not isinstance(record, dict):
                continue
            status = record.get("status", "active")
            starts_at, ends_at = self._afk_record_window(record)
            if status == "scheduled":
                if starts_at is not None and starts_at <= now:
                    if ends_at is not None and ends_at <= now:
                        afk_to_expire.append(record)
                    else:
                        afk_to_activate.append(record)
                else:
                    for candidate in (starts_at, ends_at):
                        if candidate is not None and (next_due is None or candidate < next_due):
                            next_due = candidate
            else:
                if ends_at is not None and ends_at <= now:
                    afk_to_expire.append(record)
                elif ends_at is not None and (next_due is None or ends_at < next_due):
                    next_due = ends_at
        for schedule in self.store.state.get("afk_schedules", {}).values():
            if not isinstance(schedule, dict):
                continue
            next_start_at = deserialize_datetime(schedule.get("next_start_at"))
            if next_start_at is None or next_start_at <= now:
                afk_schedule_candidates.append(schedule)
            elif next_due is None or next_start_at < next_due:
                next_due = next_start_at
        return due_reminders, due_bump_cycles, afk_to_activate, afk_to_expire, afk_schedule_candidates, next_due

    async def _activate_due_afk(self, records: list[dict]):
        async with self._lock:
            dirty = False
            for record in records:
                user_id = record.get("user_id")
                if not isinstance(user_id, int):
                    continue
                current = self.store.state.get("afk", {}).get(str(user_id))
                if not isinstance(current, dict) or current.get("status") != "scheduled":
                    continue
                current["status"] = "active"
                current["set_at"] = current.get("starts_at") or serialize_datetime(ge.now_utc())
                dirty = True
            if dirty:
                await self.store.flush()
                self._wake_event.set()

    async def _activate_due_afk_schedules(self, schedules: list[dict]):
        async with self._lock:
            now = ge.now_utc()
            dirty = False
            for schedule in schedules:
                schedule_id = schedule.get("id")
                if not isinstance(schedule_id, str):
                    continue
                current_schedule = self.store.state.get("afk_schedules", {}).get(schedule_id)
                if not isinstance(current_schedule, dict):
                    continue
                next_start_at = compute_next_afk_schedule_start(current_schedule, after=now)
                serialized_next = serialize_datetime(next_start_at)
                if current_schedule.get("next_start_at") != serialized_next:
                    current_schedule["next_start_at"] = serialized_next
                    dirty = True

                latest_start = compute_latest_afk_schedule_start(current_schedule, at_or_before=now)
                duration_seconds = current_schedule.get("duration_seconds")
                user_id = current_schedule.get("user_id")
                if latest_start is None or not isinstance(duration_seconds, int) or not isinstance(user_id, int):
                    continue
                ends_at = latest_start + timedelta(seconds=duration_seconds)
                if ends_at <= now:
                    continue
                current_afk = self.store.state.get("afk", {}).get(str(user_id))
                if self._afk_record_matches_schedule_occurrence(current_afk, schedule_id, latest_start):
                    continue
                if self._afk_record_is_live(current_afk, now=now):
                    continue
                self.store.state.setdefault("afk", {})[str(user_id)] = self._make_afk_record(
                    user_id=user_id,
                    status="active",
                    reason=current_schedule.get("reason"),
                    preset=current_schedule.get("preset"),
                    created_at=now,
                    starts_at=latest_start,
                    ends_at=ends_at,
                    schedule_id=schedule_id,
                    occurrence_at=latest_start,
                )
                dirty = True
            if dirty:
                await self.store.flush()
                self._wake_event.set()

    async def _expire_due_afk(self, records: list[dict]):
        async with self._lock:
            dirty = False
            for record in records:
                user_id = record.get("user_id")
                if isinstance(user_id, int):
                    self.store.state.get("afk", {}).pop(str(user_id), None)
                    dirty = True
            if dirty:
                await self.store.flush()
                self._wake_event.set()
