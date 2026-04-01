from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.question_drops_content import (
    QUESTION_DROP_CATEGORIES,
    QUESTION_DROP_CATEGORY_LABELS,
    QUESTION_DROP_DIFFICULTY_LABELS,
    QUESTION_DROP_TONES,
    QuestionDropVariant,
    answer_points_for_difficulty,
    build_variant_hash,
    is_answer_attempt,
    iter_candidate_variants,
    judge_answer,
    question_drop_seed_for_concept,
    render_answer_instruction,
    render_answer_summary,
    validate_content_pack,
)
from babblebox.question_drops_ai import build_question_drop_ai_provider
from babblebox.question_drops_style import (
    category_emoji,
    category_label,
    category_label_with_emoji,
    leaderboard_marker,
    progression_emoji,
    scholar_label,
    tier_label,
)
from babblebox.question_drops_store import (
    QUESTION_DROP_MAX_DROPS_PER_DAY,
    QUESTION_DROP_MIN_DROPS_PER_DAY,
    QUESTION_DROP_MASTERY_TIERS,
    QuestionDropsStorageUnavailable,
    QuestionDropsStore,
    default_question_drops_config,
    default_question_drops_meta,
    normalize_active_drop,
    normalize_question_drops_config,
    normalize_question_drops_meta,
)
from babblebox.utility_helpers import canonicalize_afk_timezone, load_afk_timezone


QUESTION_DROP_CURATED_CONCEPT_MIN_DAYS = 1
QUESTION_DROP_CURATED_VARIANT_MIN_DAYS = 3
QUESTION_DROP_GENERATED_CONCEPT_MIN_DAYS = 0
QUESTION_DROP_GENERATED_VARIANT_MIN_DAYS = 1
QUESTION_DROP_PENDING_LEASE_SECONDS = 5 * 60
QUESTION_DROP_EXPOSURE_RETENTION_DAYS = 90
QUESTION_DROP_ACTIVITY_WINDOW_SECONDS = 90 * 60
QUESTION_DROP_SLOT_GRACE_SECONDS = 45 * 60
QUESTION_DROP_SCHEDULER_INTERVAL_SECONDS = 45.0
QUESTION_DROP_WRONG_FEEDBACK_GLOBAL_LIMIT = 2
QUESTION_DROP_PRUNE_INTERVAL_SECONDS = 6 * 60 * 60
QUESTION_DROP_EXPOSURE_FETCH_LIMIT = 220
QUESTION_DROP_GENERATED_VARIANTS_PER_SEED = 8


def _config_signature(config: dict[str, Any]) -> str:
    parts = [
        str(config.get("drops_per_day")),
        str(config.get("timezone")),
        str(config.get("answer_window_seconds")),
        str(config.get("tone_mode")),
        str(config.get("activity_gate")),
        str(config.get("active_start_hour")),
        str(config.get("active_end_hour")),
        ",".join(str(value) for value in config.get("enabled_channel_ids", [])),
        ",".join(str(value) for value in config.get("enabled_categories", [])),
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]


def _slot_key(local_day: date, slot_index: int) -> str:
    return f"{local_day.isoformat()}:{slot_index}"


def _guild_day_seed(guild_id: int, local_day: date, config: dict[str, Any]) -> str:
    return f"{guild_id}:{local_day.isoformat()}:{_config_signature(config)}"


def _slot_seed_material(guild_id: int, channel_id: int, slot_key: str) -> str:
    return f"{guild_id}:{channel_id}:{slot_key}"


def _daily_slot_datetimes(guild_id: int, local_day: date, config: dict[str, Any]) -> list[datetime]:
    start_hour = int(config.get("active_start_hour", 10) or 10)
    end_hour = int(config.get("active_end_hour", 22) or 22)
    drops_per_day = int(config.get("drops_per_day", 2) or 2)
    if start_hour >= end_hour:
        return []
    tzinfo = load_afk_timezone(config.get("timezone")) or timezone.utc
    start_of_window = datetime.combine(local_day, time(hour=start_hour, minute=0, tzinfo=tzinfo))
    end_of_window = datetime.combine(local_day, time(hour=end_hour, minute=0, tzinfo=tzinfo))
    total_minutes = int((end_of_window - start_of_window).total_seconds() // 60)
    if total_minutes <= drops_per_day:
        return []
    rng = random.Random(int(hashlib.sha256(_guild_day_seed(guild_id, local_day, config).encode("utf-8")).hexdigest()[:16], 16))
    chosen_offsets = sorted(rng.sample(range(total_minutes), drops_per_day))
    return [start_of_window + timedelta(minutes=offset) for offset in chosen_offsets]


def _tone_failure_line(tone_mode: str) -> str | None:
    if tone_mode == "playful":
        return random.choice(
            (
                "Not this one. Another clean guess can still steal it.",
                "Close enough to keep trying, not close enough to score.",
                "Clean guess, wrong answer. The drop is still live.",
            )
        )
    if tone_mode == "roast-light":
        return random.choice(
            (
                "Confident answer. Wrong answer.",
                "That guess had energy. The points stayed put.",
                "Clean format, wrong target.",
            )
        )
    return None


@dataclass(frozen=True)
class QuestionDropStatusSnapshot:
    config: dict[str, Any]
    active_drop_count: int
    next_slot_at: datetime | None
    enabled_channel_mentions: tuple[str, ...]


class QuestionDropsService:
    def __init__(self, bot: commands.Bot, store: QuestionDropsStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = QuestionDropsStore()
            except QuestionDropsStorageUnavailable as exc:
                print(f"Question Drops storage constructor failed: {exc}")
                self.store = QuestionDropsStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._configs: dict[int, dict[str, Any]] = {}
        self._active_drops: dict[tuple[int, int], dict[str, Any]] = {}
        self._pending_posts: dict[tuple[int, str], dict[str, Any]] = {}
        self._recent_activity: dict[tuple[int, int], datetime] = {}
        self._wrong_feedback_users: dict[int, set[int]] = defaultdict(set)
        self._wrong_feedback_count: dict[int, int] = defaultdict(int)
        self._attempted_users: dict[int, set[int]] = defaultdict(set)
        self._next_prune_at: datetime | None = None
        self._meta = default_question_drops_meta()
        self._ai_provider = build_question_drop_ai_provider()

    async def start(self) -> bool:
        valid, message = validate_content_pack()
        if not valid:
            self.storage_ready = False
            self.storage_error = message or "Question Drops content pack validation failed."
            print(f"Question Drops content pack validation failed: {self.storage_error}")
            return False
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Question Drops storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except QuestionDropsStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Question Drops storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        self._configs = await self.store.fetch_all_configs()
        self._meta = normalize_question_drops_meta(await self.store.fetch_meta() or default_question_drops_meta())
        await self._sweep_pending_posts(await self.store.list_pending_posts(), force=True)
        self._next_prune_at = ge.now_utc()
        await self._restore_active_rows(await self.store.list_active_drops())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="babblebox-question-drops-scheduler")
        self._wake_event.set()
        return True

    async def close(self):
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task
        await self._ai_provider.close()
        await self.store.close()

    async def _restore_active_rows(self, active_rows: list[dict[str, Any]]):
        now = ge.now_utc()
        self._active_drops = {}
        for record in active_rows:
            if not self._active_drop_is_live(record, now=now):
                await self._expire_drop(record, timed_out=True, announce=False)
                continue
            if self._channel_has_live_party_session(record["guild_id"], record["channel_id"]):
                await self._expire_drop(record, timed_out=False, announce=False, delete_post_message=True)
                continue
            if not await self._message_still_exists(record):
                await self._expire_drop(record, timed_out=False, announce=False)
                continue
            self._active_drops[(record["guild_id"], record["channel_id"])] = record
            exposure_id = int(record["exposure_id"])
            self._wrong_feedback_users.setdefault(exposure_id, set())
            self._wrong_feedback_count.setdefault(exposure_id, 0)
            self._attempted_users[exposure_id] = set(record.get("participant_user_ids", []) or [])

    async def _message_still_exists(self, record: dict[str, Any]) -> bool:
        channel = self.bot.get_channel(record["channel_id"]) if hasattr(self.bot, "get_channel") else None
        if channel is None:
            return True
        fetch_message = getattr(channel, "fetch_message", None)
        if fetch_message is None or not callable(fetch_message):
            return True
        try:
            await fetch_message(int(record["message_id"]))
        except discord.NotFound:
            return False
        except discord.HTTPException:
            return True
        return True

    async def _delete_message_if_exists(self, channel_id: int, message_id: int | None):
        if not isinstance(message_id, int) or message_id <= 0:
            return
        channel = self.bot.get_channel(channel_id) if hasattr(self.bot, "get_channel") else None
        if channel is None:
            return
        fetch_message = getattr(channel, "fetch_message", None)
        if fetch_message is None or not callable(fetch_message):
            return
        try:
            message = await fetch_message(message_id)
        except (discord.NotFound, discord.HTTPException):
            return
        with contextlib.suppress(discord.NotFound, discord.HTTPException):
            await message.delete()

    async def _sweep_pending_posts(self, pending_rows: list[dict[str, Any]], *, force: bool = False):
        self._pending_posts = {}
        now = ge.now_utc()
        for record in pending_rows:
            if not force and not self._pending_post_is_stale(record, now=now):
                self._pending_posts[(record["guild_id"], record["slot_key"])] = record
                continue
            await self._delete_message_if_exists(record["channel_id"], record.get("message_id"))
            await self.store.release_pending_post(record["guild_id"], record["slot_key"])

    def _pending_post_is_stale(self, record: dict[str, Any], *, now: datetime) -> bool:
        lease_raw = record.get("lease_expires_at")
        if isinstance(lease_raw, datetime):
            lease_expires_at = lease_raw.astimezone(timezone.utc) if lease_raw.tzinfo else lease_raw.replace(tzinfo=timezone.utc)
        else:
            lease_expires_at = datetime.fromisoformat(str(lease_raw))
            if lease_expires_at.tzinfo is None:
                lease_expires_at = lease_expires_at.replace(tzinfo=timezone.utc)
        return lease_expires_at <= now

    async def _release_pending_post(self, guild_id: int, slot_key: str):
        self._pending_posts.pop((guild_id, slot_key), None)
        await self.store.release_pending_post(guild_id, slot_key)

    def _channel_has_pending_post(self, guild_id: int, channel_id: int) -> bool:
        return any(
            record["guild_id"] == guild_id and record["channel_id"] == channel_id
            for record in self._pending_posts.values()
        )

    def has_live_drop(self, guild_id: int, channel_id: int) -> bool:
        record = self._active_drops.get((guild_id, channel_id))
        return record is not None and self._active_drop_is_live(record, now=ge.now_utc())

    async def retire_drop_for_party_game(self, guild_id: int, channel_id: int) -> bool:
        record = self._active_drops.get((guild_id, channel_id))
        if record is None:
            return False
        await self._expire_drop(record, timed_out=False, announce=False, delete_post_message=True)
        return True

    def storage_message(self, feature_name: str = "Question Drops") -> str:
        return f"{feature_name} are temporarily unavailable because Babblebox could not reach the Question Drops database."

    def get_config(self, guild_id: int) -> dict[str, Any]:
        config = self._configs.get(guild_id)
        if config is None:
            return default_question_drops_config(guild_id)
        return normalize_question_drops_config(guild_id, config)

    def get_meta(self) -> dict[str, Any]:
        return normalize_question_drops_meta(self._meta)

    def _enabled_categories(self, config: dict[str, Any]) -> list[str]:
        return [
            category
            for category in config.get("enabled_categories", [])
            if str(category).strip().casefold() in QUESTION_DROP_CATEGORIES
        ]

    def _feature_status_label(self, *, enabled_count: int, configured_count: int) -> str:
        if enabled_count <= 0:
            return "Off"
        if configured_count <= 0:
            return "Setup needed"
        if configured_count < enabled_count:
            return f"Partial ({configured_count}/{enabled_count} ready)"
        return "Ready"

    def _announcement_channel_issue(self, guild: discord.Guild, channel_id: int | None, *, label: str) -> str | None:
        if not isinstance(channel_id, int) or channel_id <= 0 or not hasattr(guild, "get_channel"):
            return None
        channel = guild.get_channel(channel_id)
        if channel is None:
            return f"{label}: announcement channel is missing."
        permissions_for = getattr(channel, "permissions_for", None)
        bot_member = self._bot_member_for_guild(guild)
        if callable(permissions_for) and bot_member is not None:
            perms = permissions_for(bot_member)
            if not bool(getattr(perms, "view_channel", False)):
                return f"{label}: cannot view the announcement channel."
            if not bool(getattr(perms, "send_messages", False)):
                return f"{label}: cannot send messages in the announcement channel."
            if not bool(getattr(perms, "embed_links", False)):
                return f"{label}: missing `Embed Links` in the announcement channel."
        return None

    async def update_config(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        drops_per_day: int | None = None,
        timezone_name: str | None = None,
        answer_window_seconds: int | None = None,
        tone_mode: str | None = None,
        activity_gate: str | None = None,
        active_start_hour: int | None = None,
        active_end_hour: int | None = None,
        ai_celebrations_enabled: bool | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        async with self._lock:
            current = dict(self.get_config(guild_id))
            if enabled is not None:
                current["enabled"] = bool(enabled)
            if drops_per_day is not None:
                if not isinstance(drops_per_day, int) or isinstance(drops_per_day, bool):
                    return False, f"Drops/day must be a whole number from {QUESTION_DROP_MIN_DROPS_PER_DAY}-{QUESTION_DROP_MAX_DROPS_PER_DAY}."
                if not QUESTION_DROP_MIN_DROPS_PER_DAY <= drops_per_day <= QUESTION_DROP_MAX_DROPS_PER_DAY:
                    return (
                        False,
                        f"Drops/day must stay between {QUESTION_DROP_MIN_DROPS_PER_DAY} and {QUESTION_DROP_MAX_DROPS_PER_DAY}.",
                    )
                current["drops_per_day"] = drops_per_day
            if timezone_name is not None:
                timezone_text = str(timezone_name).strip()
                if timezone_text.casefold() in {"utc", "z"}:
                    ok, canonical, error = True, "UTC", None
                else:
                    ok, canonical, error = canonicalize_afk_timezone(timezone_name)
                if not ok or canonical is None:
                    return False, error or "Use a valid timezone like `Asia/Yerevan` or `UTC+04:00`."
                current["timezone"] = canonical
            if answer_window_seconds is not None:
                current["answer_window_seconds"] = answer_window_seconds
            if tone_mode is not None:
                current["tone_mode"] = str(tone_mode).strip().casefold()
            if activity_gate is not None:
                current["activity_gate"] = str(activity_gate).strip().casefold()
            if active_start_hour is not None:
                current["active_start_hour"] = active_start_hour
            if active_end_hour is not None:
                current["active_end_hour"] = active_end_hour
            if ai_celebrations_enabled is not None:
                current["ai_celebrations_enabled"] = bool(ai_celebrations_enabled)
            normalized = normalize_question_drops_config(guild_id, current)
            if normalized["active_start_hour"] >= normalized["active_end_hour"]:
                return False, "Active hours need a clear daytime window. Use a start hour earlier than the end hour."
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        self._wake_event.set()
        return True, "Question Drops settings updated."

    async def update_channels(
        self,
        guild_id: int,
        *,
        action: str,
        channel_id: int | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        action = str(action).strip().casefold()
        if action not in {"add", "remove", "clear"}:
            return False, "Channel action must be `add`, `remove`, or `clear`."
        async with self._lock:
            config = dict(self.get_config(guild_id))
            channels = list(config.get("enabled_channel_ids", []))
            if action == "clear":
                channels = []
            elif channel_id is None or channel_id <= 0:
                return False, "Pick a channel for that update."
            elif action == "add":
                channels = sorted(set(channels) | {channel_id})
            else:
                channels = [value for value in channels if value != channel_id]
            config["enabled_channel_ids"] = channels
            normalized = normalize_question_drops_config(guild_id, config)
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        self._wake_event.set()
        return True, "Question Drops channels updated."

    async def update_categories(
        self,
        guild_id: int,
        *,
        action: str,
        category: str | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        action = str(action).strip().casefold()
        if action not in {"enable", "disable", "reset"}:
            return False, "Category action must be `enable`, `disable`, or `reset`."
        async with self._lock:
            config = dict(self.get_config(guild_id))
            categories = set(config.get("enabled_categories", []))
            if action == "reset":
                categories = set(QUESTION_DROP_CATEGORIES)
            else:
                normalized_category = str(category or "").strip().casefold()
                if normalized_category not in QUESTION_DROP_CATEGORIES:
                    return False, f"Unknown category. Choose from {', '.join(QUESTION_DROP_CATEGORIES)}."
                if action == "enable":
                    categories.add(normalized_category)
                else:
                    categories.discard(normalized_category)
            config["enabled_categories"] = sorted(categories)
            normalized = normalize_question_drops_config(guild_id, config)
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        self._wake_event.set()
        return True, "Question Drops categories updated."

    async def set_global_ai_celebration_mode(self, mode: str, *, actor_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        normalized_mode = str(mode or "").strip().casefold()
        if normalized_mode not in {"off", "rare", "event_only"}:
            return False, "Use `off`, `rare`, or `event_only`."
        next_meta = {
            "ai_celebration_mode": normalized_mode,
            "updated_by": actor_id if isinstance(actor_id, int) and actor_id > 0 else None,
            "updated_at": ge.now_utc(),
        }
        async with self._lock:
            await self.store.upsert_meta(next_meta)
            self._meta = normalize_question_drops_meta(next_meta)
        return True, f"Question Drops AI celebrations are now `{normalized_mode}`."

    async def update_category_mastery(
        self,
        guild_id: int,
        *,
        category: str,
        enabled: bool | None = None,
        tier: int | None = None,
        role_id: int | None = None,
        threshold: int | None = None,
        announcement_channel_id: int | None = None,
        clear_announcement_channel: bool = False,
        silent_grant: bool | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        normalized_category = str(category or "").strip().casefold()
        if normalized_category not in QUESTION_DROP_CATEGORIES:
            return False, f"Unknown category. Choose from {', '.join(QUESTION_DROP_CATEGORIES)}."
        if tier is not None and tier not in QUESTION_DROP_MASTERY_TIERS:
            return False, "Tier must be 1, 2, or 3."
        if threshold is not None and int(threshold) < 0:
            return False, "Threshold must be 0 or higher."
        async with self._lock:
            config = dict(self.get_config(guild_id))
            mastery = dict(config.get("category_mastery", {}))
            category_config = dict(mastery.get(normalized_category, {}))
            if enabled is not None:
                category_config["enabled"] = bool(enabled)
            if silent_grant is not None:
                category_config["silent_grant"] = bool(silent_grant)
            if clear_announcement_channel:
                category_config["announcement_channel_id"] = None
            elif announcement_channel_id is not None:
                category_config["announcement_channel_id"] = int(announcement_channel_id) if announcement_channel_id > 0 else None
            tiers = [dict(item) for item in category_config.get("tiers", [])]
            if tier is not None:
                for item in tiers:
                    if int(item.get("tier", 0) or 0) != int(tier):
                        continue
                    if role_id is not None:
                        item["role_id"] = int(role_id) if role_id > 0 else None
                    if threshold is not None:
                        item["threshold"] = max(0, int(threshold))
                    break
            category_config["tiers"] = tiers
            mastery[normalized_category] = category_config
            config["category_mastery"] = mastery
            normalized = normalize_question_drops_config(guild_id, config)
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        return True, f"{category_label(normalized_category)} mastery settings updated."

    async def update_scholar_ladder(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        tier: int | None = None,
        role_id: int | None = None,
        threshold: int | None = None,
        announcement_channel_id: int | None = None,
        clear_announcement_channel: bool = False,
        silent_grant: bool | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        if tier is not None and tier not in QUESTION_DROP_MASTERY_TIERS:
            return False, "Tier must be 1, 2, or 3."
        if threshold is not None and int(threshold) < 0:
            return False, "Threshold must be 0 or higher."
        async with self._lock:
            config = dict(self.get_config(guild_id))
            scholar = dict(config.get("scholar_ladder", {}))
            if enabled is not None:
                scholar["enabled"] = bool(enabled)
            if silent_grant is not None:
                scholar["silent_grant"] = bool(silent_grant)
            if clear_announcement_channel:
                scholar["announcement_channel_id"] = None
            elif announcement_channel_id is not None:
                scholar["announcement_channel_id"] = int(announcement_channel_id) if announcement_channel_id > 0 else None
            tiers = [dict(item) for item in scholar.get("tiers", [])]
            if tier is not None:
                for item in tiers:
                    if int(item.get("tier", 0) or 0) != int(tier):
                        continue
                    if role_id is not None:
                        item["role_id"] = int(role_id) if role_id > 0 else None
                    if threshold is not None:
                        item["threshold"] = max(0, int(threshold))
                    break
            scholar["tiers"] = tiers
            config["scholar_ladder"] = scholar
            normalized = normalize_question_drops_config(guild_id, config)
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        return True, "Scholar ladder settings updated."

    def _profile_service(self):
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return None
        return profile_service

    async def _ensure_guild_backfill(self, guild_id: int):
        profile_service = self._profile_service()
        if profile_service is None:
            return
        backfill = getattr(profile_service, "backfill_question_drop_guild_points_from_exposures", None)
        if not callable(backfill):
            return
        exposures = await self.store.list_exposures_for_guild(guild_id, limit=QUESTION_DROP_EXPOSURE_FETCH_LIMIT)
        await backfill(guild_id=guild_id, exposures=exposures)

    def _bot_member_for_guild(self, guild: discord.Guild):
        bot_user = getattr(self.bot, "user", None)
        bot_user_id = getattr(bot_user, "id", 0)
        guild_me = getattr(guild, "me", None)
        if guild_me is not None:
            return guild_me
        get_member = getattr(guild, "get_member", None)
        if callable(get_member) and isinstance(bot_user_id, int) and bot_user_id > 0:
            return get_member(bot_user_id)
        return None

    def _configured_category_mastery(self, config: dict[str, Any], category: str) -> dict[str, Any]:
        mastery = config.get("category_mastery", {})
        if not isinstance(mastery, dict):
            return {}
        return dict(mastery.get(str(category or "").strip().casefold(), {}))

    def _configured_tiers(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        tiers = payload.get("tiers", []) if isinstance(payload, dict) else []
        return [dict(item) for item in tiers if isinstance(item, dict)]

    def _configured_unlock_tiers(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return [
            item
            for item in self._configured_tiers(payload)
            if isinstance(item.get("role_id"), int) and int(item["role_id"]) > 0 and int(item.get("threshold", 0) or 0) > 0
        ]

    def _unlocks_for_scope(self, unlocks: list[dict[str, Any]], *, scope_type: str, scope_key: str) -> list[dict[str, Any]]:
        return [
            item
            for item in unlocks
            if str(item.get("scope_type", "")).casefold() == scope_type
            and str(item.get("scope_key", "")).casefold() == scope_key
        ]

    def _current_scope_tier(self, unlocks: list[dict[str, Any]], *, scope_type: str, scope_key: str) -> int:
        scoped_unlocks = self._unlocks_for_scope(unlocks, scope_type=scope_type, scope_key=scope_key)
        return max((int(item.get("tier", 0) or 0) for item in scoped_unlocks), default=0)

    def _next_scope_tier(self, tiers: list[dict[str, Any]], *, points: int, current_tier: int) -> dict[str, Any] | None:
        for tier in sorted(tiers, key=lambda item: int(item.get("tier", 0) or 0)):
            if int(tier.get("tier", 0) or 0) <= current_tier:
                continue
            threshold = int(tier.get("threshold", 0) or 0)
            if threshold <= 0:
                continue
            return {
                "tier": int(tier["tier"]),
                "threshold": threshold,
                "remaining": max(0, threshold - max(0, int(points))),
                "role_id": int(tier.get("role_id", 0) or 0) or None,
            }
        return None

    def _rank_delta(self, before_rank: int | None, after_rank: int | None) -> int:
        if not isinstance(before_rank, int) or before_rank <= 0:
            return 0
        if not isinstance(after_rank, int) or after_rank <= 0:
            return 0
        return max(0, before_rank - after_rank)

    def _crossed_hundred_points(self, before_points: int, after_points: int) -> int | None:
        before_bucket = max(0, int(before_points)) // 100
        after_bucket = max(0, int(after_points)) // 100
        if after_bucket > before_bucket and after_bucket > 0:
            return after_bucket * 100
        return None

    def _milestone_flags(self, update: dict[str, Any], role_events: list[dict[str, Any]]) -> dict[str, Any]:
        guild_before = update.get("guild_before", {}) if isinstance(update.get("guild_before"), dict) else {}
        guild_after = update.get("guild_after", {}) if isinstance(update.get("guild_after"), dict) else {}
        category_before = update.get("guild_category_before", {}) if isinstance(update.get("guild_category_before"), dict) else {}
        category_after = update.get("guild_category_after", {}) if isinstance(update.get("guild_category_after"), dict) else {}
        category_role_events = [event for event in role_events if event.get("scope_type") == "category"]
        scholar_role_events = [event for event in role_events if event.get("scope_type") == "scholar"]
        return {
            "first_category_correct": int(category_before.get("correct_count", 0) or 0) == 0 and int(category_after.get("correct_count", 0) or 0) == 1,
            "bounce_back": int(guild_before.get("current_streak", 0) or 0) == 0 and int(guild_before.get("attempts", 0) or 0) > 0,
            "new_best_streak": int(guild_after.get("best_streak", 0) or 0) > int(guild_before.get("best_streak", 0) or 0),
            "guild_rank_jump": self._rank_delta(update.get("guild_rank_before"), update.get("guild_rank_after")),
            "category_rank_jump": self._rank_delta(update.get("category_rank_before"), update.get("category_rank_after")),
            "guild_points_milestone": self._crossed_hundred_points(
                int(guild_before.get("points", 0) or 0),
                int(guild_after.get("points", 0) or 0),
            ),
            "category_role_events": category_role_events,
            "scholar_role_events": scholar_role_events,
            "took_guild_first": update.get("guild_rank_after") == 1 and update.get("guild_rank_before") not in {1, None},
            "took_category_first": update.get("category_rank_after") == 1 and update.get("category_rank_before") not in {1, None},
        }

    async def _resolve_announcement_channel(self, guild: discord.Guild, channel_id: int | None):
        if not isinstance(channel_id, int) or channel_id <= 0:
            return None
        if self._announcement_channel_issue(guild, channel_id, label="Announcement") is not None:
            return None
        return guild.get_channel(channel_id)

    async def _announce_role_grant(self, guild: discord.Guild, fallback_channel, event: dict[str, Any]):
        description = (
            f"{progression_emoji('role')} {event['member_mention']} earned <@&{event['role_id']}> "
            f"for {event['scope_label']} at **{event['threshold']}** points."
        )
        if event.get("tier") == 3:
            description += f" {progression_emoji('mastery')} Top tier secured."
        channels_to_try = []
        configured_channel = await self._resolve_announcement_channel(guild, event.get("announcement_channel_id"))
        if configured_channel is not None:
            channels_to_try.append(configured_channel)
        if fallback_channel is not None and fallback_channel not in channels_to_try:
            channels_to_try.append(fallback_channel)
        for channel in channels_to_try:
            try:
                await channel.send(
                    embed=ge.make_status_embed(
                        f"{event['headline']} Unlocked",
                        description,
                        tone="success",
                        footer="Babblebox Question Drops",
                    ),
                    allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
                )
                return
            except discord.HTTPException:
                continue

    def _resolve_user_label(self, user_id: int) -> str:
        get_user = getattr(self.bot, "get_user", None)
        cached = get_user(user_id) if callable(get_user) else None
        if cached is not None:
            return ge.display_name_of(cached)
        return f"User {user_id}"

    async def get_status_snapshot(self, guild: discord.Guild) -> QuestionDropStatusSnapshot:
        await self._ensure_guild_backfill(guild.id)
        config = self.get_config(guild.id)
        active_channels = [
            record
            for (record_guild_id, _), record in self._active_drops.items()
            if record_guild_id == guild.id
        ]
        enabled_channel_mentions = []
        for channel_id in config.get("enabled_channel_ids", []):
            channel = guild.get_channel(channel_id)
            if channel is not None:
                enabled_channel_mentions.append(channel.mention)
        next_slot_at = await self._next_slot_for_guild(guild.id, config=config)
        return QuestionDropStatusSnapshot(
            config=config,
            active_drop_count=len(active_channels),
            next_slot_at=next_slot_at,
            enabled_channel_mentions=tuple(enabled_channel_mentions),
        )

    def build_status_embed(self, guild: discord.Guild, snapshot: QuestionDropStatusSnapshot) -> discord.Embed:
        config = snapshot.config
        enabled_categories = self._enabled_categories(config)
        enabled_category_labels = [category_label_with_emoji(category) for category in enabled_categories]
        meta = self.get_meta()
        scholar = dict(config.get("scholar_ladder", {}))
        description = "Guild-first knowledge mastery with compact drops, visible thresholds, and low-noise role prestige."
        if not config.get("enabled"):
            description = "Question Drops are off for this server."
        else:
            if not enabled_categories:
                description += " No categories are enabled, so scheduled drops are paused until you re-enable at least one."
            if str(config.get("activity_gate", "light")).casefold() == "light":
                description += " Quiet channels can skip a slot."
            if int(config.get("drops_per_day", 2) or 2) >= 6:
                description += " Higher daily counts recycle sooner once the fresh pool thins."
        bot_member = self._bot_member_for_guild(guild)
        bot_perms = getattr(bot_member, "guild_permissions", None)
        operability_lines = []
        if bot_member is None:
            operability_lines.append("Bot member could not be resolved for role checks.")
        elif not bool(getattr(bot_perms, "manage_roles", False)):
            operability_lines.append("Missing `Manage Roles`, so mastery and scholar roles cannot be granted.")
        if config.get("enabled") and not enabled_categories:
            operability_lines.append("No categories are enabled, so scheduled drops cannot post yet.")
        if config.get("enabled") and not config.get("enabled_channel_ids"):
            operability_lines.append("No delivery channels are enabled, so scheduled drops cannot post yet.")
        enabled_mastery_categories = [
            category
            for category in QUESTION_DROP_CATEGORIES
            if self._configured_category_mastery(config, category).get("enabled")
        ]
        mastery_lines = []
        mastery_setup_lines = []
        configured_mastery_count = 0
        for category in enabled_mastery_categories:
            category_mastery = self._configured_category_mastery(config, category)
            announcement_issue = self._announcement_channel_issue(
                guild,
                category_mastery.get("announcement_channel_id"),
                label=f"{category_label(category)} mastery",
            )
            if announcement_issue is not None:
                operability_lines.append(announcement_issue)
            if not category_mastery.get("enabled"):
                continue
            tiers = self._configured_unlock_tiers(category_mastery)
            if not tiers:
                mastery_setup_lines.append(f"{category_label_with_emoji(category)} enabled, but thresholds and roles still need setup.")
                continue
            configured_mastery_count += 1
            for tier in tiers:
                role = guild.get_role(int(tier["role_id"])) if hasattr(guild, "get_role") else None
                if role is None:
                    operability_lines.append(
                        f"{category_label(category)} {tier_label(int(tier['tier']))}: configured role is missing."
                    )
            tier_text = ", ".join(
                f"{tier_label(int(item['tier']))} `{int(item['threshold'])}`"
                for item in sorted(tiers, key=lambda item: int(item.get("tier", 0) or 0))
            )
            flags = []
            if category_mastery.get("silent_grant"):
                flags.append("silent")
            if isinstance(category_mastery.get("announcement_channel_id"), int):
                flags.append(f"announce <#{int(category_mastery['announcement_channel_id'])}>")
            mastery_lines.append(f"{category_label_with_emoji(category)} {tier_text}" + (f" [{' | '.join(flags)}]" if flags else ""))
        scholar_tiers = self._configured_unlock_tiers(scholar)
        scholar_enabled = bool(scholar.get("enabled"))
        if scholar_enabled:
            announcement_issue = self._announcement_channel_issue(
                guild,
                scholar.get("announcement_channel_id"),
                label="Scholar ladder",
            )
            if announcement_issue is not None:
                operability_lines.append(announcement_issue)
        for tier in scholar_tiers:
            role = guild.get_role(int(tier["role_id"])) if hasattr(guild, "get_role") else None
            if role is None:
                operability_lines.append(f"{scholar_label(int(tier['tier']))}: configured role is missing.")
        mastery_status = self._feature_status_label(
            enabled_count=len(enabled_mastery_categories),
            configured_count=configured_mastery_count,
        )
        scholar_status = self._feature_status_label(
            enabled_count=1 if scholar_enabled else 0,
            configured_count=1 if scholar_tiers else 0,
        )
        scholar_lines = [
            f"{scholar_label(int(item['tier']))} `{int(item['threshold'])}`"
            for item in sorted(scholar_tiers, key=lambda item: int(item.get("tier", 0) or 0))
        ]
        embed = discord.Embed(
            title="Question Drops",
            description=description,
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Schedule",
            value=(
                f"Enabled: **{'Yes' if config.get('enabled') else 'No'}**\n"
                f"Drops/day: **{config.get('drops_per_day', 2)}** (1-10)\n"
                f"Window: **{config.get('active_start_hour', 10):02d}:00-{config.get('active_end_hour', 22):02d}:00**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Rules",
            value=(
                f"Timezone: **{config.get('timezone', 'UTC')}**\n"
                f"Answer window: **{config.get('answer_window_seconds', 60)}s**\n"
                f"Activity gate: **{str(config.get('activity_gate', 'light')).title()}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Tone: **{str(config.get('tone_mode', 'clean')).title()}**\n"
                f"Channels: **{len(snapshot.enabled_channel_mentions)}**\n"
                f"Live now: **{snapshot.active_drop_count}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Knowledge Lane",
            value=(
                f"Categories: **{len(enabled_categories)}**\n"
                f"{progression_emoji('role')} Mastery: **{mastery_status}**\n"
                f"{progression_emoji('scholar')} Scholar ladder: **{scholar_status}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Categories",
            value=", ".join(enabled_category_labels) if enabled_category_labels else "No enabled categories. Re-enable one to resume scheduled drops.",
            inline=False,
        )
        if mastery_lines:
            embed.add_field(name="Mastery Roles", value="\n".join(mastery_lines[:6]), inline=False)
        if mastery_setup_lines:
            embed.add_field(name="Mastery Setup", value="\n".join(mastery_setup_lines[:6]), inline=False)
        if scholar_lines:
            scholar_flags = []
            if scholar.get("silent_grant"):
                scholar_flags.append("silent")
            if isinstance(scholar.get("announcement_channel_id"), int):
                scholar_flags.append(f"announce <#{int(scholar['announcement_channel_id'])}>")
            scholar_value = "\n".join(scholar_lines)
            if scholar_flags:
                scholar_value += f"\nSettings: {' | '.join(scholar_flags)}"
            embed.add_field(name="Scholar Ladder", value=scholar_value, inline=False)
        elif scholar_enabled:
            embed.add_field(name="Scholar Ladder", value="Enabled, but thresholds and roles still need setup.", inline=False)
        if snapshot.enabled_channel_mentions:
            embed.add_field(name="Channels", value="\n".join(snapshot.enabled_channel_mentions[:8]), inline=False)
        if snapshot.next_slot_at is not None:
            embed.add_field(
                name="Next Slot",
                value=f"{ge.format_timestamp(snapshot.next_slot_at, 'R')} ({ge.format_timestamp(snapshot.next_slot_at, 'f')})",
                inline=False,
            )
        embed.add_field(
            name="AI Celebrations",
            value=(
                f"Guild opt-in: **{'On' if config.get('ai_celebrations_enabled') else 'Off'}**\n"
                f"Global override: **{meta.get('ai_celebration_mode', 'off')}**\n"
                f"Provider: **{self._ai_provider.diagnostics().get('status', 'Unavailable')}**"
            ),
            inline=False,
        )
        if operability_lines:
            embed.add_field(name="Operability", value="\n".join(operability_lines[:6]), inline=False)
        return ge.style_embed(embed, footer="Babblebox Question Drops | Compact, offline, channel-safe")

    def build_stats_embed(self, user: discord.abc.User, summary: dict[str, Any]) -> discord.Embed:
        profile = summary["profile"]
        guild_profile = summary.get("guild_profile")
        primary = guild_profile if isinstance(guild_profile, dict) else summary.get("global_profile", {})
        guild_categories = summary.get("guild_categories") or []
        categories = guild_categories if guild_categories else summary.get("top_categories") or []
        unlocks = summary.get("guild_unlocks") or []
        guild_id = summary.get("guild_id")
        config = self.get_config(guild_id) if isinstance(guild_id, int) and guild_id > 0 else None
        embed = discord.Embed(
            title="Question Drops Stats",
            description=f"Knowledge mastery snapshot for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["info"],
        )
        participations = int(primary.get("attempts", 0) or 0)
        correct = int(primary.get("correct_count", 0) or 0)
        accuracy = (correct / participations * 100.0) if participations else 0.0
        embed.add_field(
            name="Knowledge",
            value=(
                f"Scope: **{'This server' if isinstance(guild_profile, dict) else 'Lifetime'}**\n"
                f"Points: **{int(primary.get('points', 0) or 0)}**\n"
                f"Solved: **{correct} / {participations} drops**\n"
                f"Accuracy: **{accuracy:.0f}%**"
            ),
            inline=True,
        )
        embed.add_field(
            name=f"{progression_emoji('streak')} Streak",
            value=(
                f"Current: **{int(primary.get('current_streak', 0) or 0)}**\n"
                f"Best: **{int(primary.get('best_streak', 0) or 0)}**"
            ),
            inline=True,
        )
        if isinstance(summary.get("guild_rank"), int):
            embed.add_field(
                name=f"{progression_emoji('move')} Rank",
                value=f"Knowledge board: **#{int(summary['guild_rank'])}**",
                inline=True,
            )
        scholar_tier = self._current_scope_tier(unlocks, scope_type="scholar", scope_key="global")
        scholar_next = None
        scholar_config = dict(config.get("scholar_ladder", {})) if config is not None else {}
        if config is not None and scholar_config.get("enabled"):
            scholar_next = self._next_scope_tier(
                self._configured_unlock_tiers(scholar_config),
                points=int(primary.get("points", 0) or 0),
                current_tier=scholar_tier,
            )
        scholar_lines = [f"{progression_emoji('scholar')} Current: **{scholar_label(scholar_tier)}**" if scholar_tier else f"{progression_emoji('scholar')} Current: **Unranked**"]
        if scholar_next is not None:
            scholar_lines.append(
                f"{progression_emoji('next')} Next: **{scholar_label(int(scholar_next['tier']))}** in **{int(scholar_next['remaining'])}** pts"
            )
        elif config is not None and scholar_config.get("enabled") and not self._configured_unlock_tiers(scholar_config):
            scholar_lines.append(f"{progression_emoji('next')} Setup needed before scholar ranks can grant.")
        embed.add_field(name="Scholar", value="\n".join(scholar_lines), inline=False)
        if categories:
            lines = []
            for entry in categories[:4]:
                category_id = str(entry.get("category", "")).strip().casefold()
                current_tier = self._current_scope_tier(unlocks, scope_type="category", scope_key=category_id)
                next_tier = None
                category_config = self._configured_category_mastery(config, category_id) if config is not None else {}
                if guild_categories and config is not None and category_config.get("enabled"):
                    next_tier = self._next_scope_tier(
                        self._configured_unlock_tiers(category_config),
                        points=int(entry.get("points", 0) or 0),
                        current_tier=current_tier,
                    )
                line = (
                    f"{category_label_with_emoji(category_id)} **{int(entry.get('points', 0) or 0)}** pts | "
                    f"{int(entry.get('correct_count', 0) or 0)} solves"
                )
                if current_tier > 0:
                    line += f" | {progression_emoji('role')} {tier_label(current_tier)}"
                if next_tier is not None:
                    line += f" | {progression_emoji('next')} {int(next_tier['remaining'])} to {tier_label(int(next_tier['tier']))}"
                lines.append(line)
            embed.add_field(name="Top Categories" if guild_categories else "Lifetime Category Flavor", value="\n".join(lines), inline=False)
        if isinstance(guild_profile, dict):
            global_profile = summary.get("global_profile", {})
            embed.add_field(
                name="Lifetime Flavor",
                value=(
                    f"Lifetime points: **{int(global_profile.get('points', 0) or 0)}**\n"
                    f"Lifetime solves: **{int(global_profile.get('correct_count', 0) or 0)} / {int(global_profile.get('attempts', 0) or 0)}**"
                ),
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Question Drops | Aggregates only, no answer archive")

    def build_leaderboard_embed(self, guild: discord.Guild, entries: list[dict[str, Any]], *, category: str | None = None) -> discord.Embed:
        normalized_category = str(category or "").strip().casefold()
        if normalized_category:
            title = f"{category_label_with_emoji(normalized_category)} Knowledge Leaders"
            footer = "Babblebox Question Drops | Category mastery board"
        else:
            title = "Knowledge Leaderboard"
            footer = "Babblebox Question Drops | Guild-first knowledge board"
        if not entries:
            return ge.make_status_embed(title, "No knowledge results are on the board yet.", tone="info", footer=footer)
        lines = []
        for index, entry in enumerate(entries[:10], start=1):
            user_id = int(entry.get("user_id", 0) or 0)
            label = self._resolve_user_label(user_id)
            points = int(entry.get("points", 0) or 0)
            correct = int(entry.get("correct_count", 0) or 0)
            attempts = int(entry.get("attempts", 0) or 0)
            streak = int(entry.get("current_streak", 0) or 0)
            lines.append(
                f"**{leaderboard_marker(index)}** {label} | **{points}** pts | {correct}/{attempts} solved | {progression_emoji('streak')} {streak}"
            )
        embed = discord.Embed(
            title=title,
            description="\n".join(lines),
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(name="Guild", value=guild.name, inline=True)
        embed.add_field(name="Lane", value="Knowledge mastery only", inline=True)
        if normalized_category:
            embed.add_field(name="Category", value=category_label_with_emoji(normalized_category), inline=True)
        return ge.style_embed(embed, footer=footer)

    def _guild_members_for_recalc(self, guild: discord.Guild) -> list[Any]:
        members = getattr(guild, "members", None)
        if isinstance(members, list):
            return [member for member in members if not bool(getattr(member, "bot", False))]
        cached_members = getattr(guild, "_members", None)
        if isinstance(cached_members, dict):
            return [member for member in cached_members.values() if not bool(getattr(member, "bot", False))]
        return []

    def _pending_unlocks_for_summary(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        guild_id = int(summary.get("guild_id", 0) or 0)
        if guild_id <= 0:
            return []
        config = self.get_config(guild_id)
        unlocks = summary.get("guild_unlocks") or []
        pending: list[dict[str, Any]] = []
        guild_profile = summary.get("guild_profile") or {}
        scholar_config = dict(config.get("scholar_ladder", {}))
        if scholar_config.get("enabled"):
            scholar_points = int(guild_profile.get("points", 0) or 0)
            for tier_config in self._configured_unlock_tiers(scholar_config):
                tier = int(tier_config.get("tier", 0) or 0)
                role_id = int(tier_config.get("role_id", 0) or 0)
                threshold = int(tier_config.get("threshold", 0) or 0)
                if scholar_points >= threshold and not self._unlock_exists(unlocks, scope_type="scholar", scope_key="global", tier=tier, role_id=role_id):
                    pending.append(
                        {
                            "scope_type": "scholar",
                            "scope_key": "global",
                            "scope_label": "the scholar ladder",
                            "tier": tier,
                            "threshold": threshold,
                            "role_id": role_id,
                        }
                    )
        for row in summary.get("guild_categories") or []:
            category_id = str(row.get("category", "")).strip().casefold()
            category_config = self._configured_category_mastery(config, category_id)
            if not category_config.get("enabled"):
                continue
            category_points = int(row.get("points", 0) or 0)
            for tier_config in self._configured_unlock_tiers(category_config):
                tier = int(tier_config.get("tier", 0) or 0)
                role_id = int(tier_config.get("role_id", 0) or 0)
                threshold = int(tier_config.get("threshold", 0) or 0)
                if category_points >= threshold and not self._unlock_exists(unlocks, scope_type="category", scope_key=category_id, tier=tier, role_id=role_id):
                    pending.append(
                        {
                            "scope_type": "category",
                            "scope_key": category_id,
                            "scope_label": category_label_with_emoji(category_id),
                            "tier": tier,
                            "threshold": threshold,
                            "role_id": role_id,
                        }
                    )
        return pending

    async def recalculate_mastery_roles(
        self,
        guild: discord.Guild,
        *,
        member=None,
        preview: bool = True,
    ) -> dict[str, Any]:
        profile_service = self._profile_service()
        if profile_service is None:
            return {"preview": preview, "scanned": 0, "pending": 0, "granted": 0}
        await self._ensure_guild_backfill(guild.id)
        targets = [member] if member is not None else self._guild_members_for_recalc(guild)
        pending_total = 0
        granted_total = 0
        scanned = 0
        for target in targets:
            if target is None or bool(getattr(target, "bot", False)):
                continue
            scanned += 1
            summary = await profile_service.get_question_drop_summary(int(target.id), guild_id=guild.id)
            if not isinstance(summary, dict):
                continue
            pending_unlocks = self._pending_unlocks_for_summary(summary)
            pending_total += len(pending_unlocks)
            if preview:
                continue
            for candidate in pending_unlocks:
                event = await self._grant_unlock_role(
                    guild=guild,
                    member=target,
                    scope_type=str(candidate["scope_type"]),
                    scope_key=str(candidate["scope_key"]),
                    scope_label=str(candidate["scope_label"]),
                    tier=int(candidate["tier"]),
                    threshold=int(candidate["threshold"]),
                    role_id=int(candidate["role_id"]),
                    silent_grant=True,
                    announcement_channel_id=None,
                )
                if event is not None:
                    granted_total += 1
        return {"preview": preview, "scanned": scanned, "pending": pending_total, "granted": granted_total}

    def observe_message_activity(self, message: discord.Message):
        if message.guild is None:
            return
        self._recent_activity[(message.guild.id, message.channel.id)] = ge.now_utc()

    async def handle_message(self, message: discord.Message) -> bool:
        if not self.storage_ready or message.guild is None:
            return False
        active = self._active_drops.get((message.guild.id, message.channel.id))
        if active is None:
            return False
        if self._channel_has_live_party_session(message.guild.id, message.channel.id):
            await self._expire_drop(active, timed_out=False, announce=False, delete_post_message=True)
            return False
        now = ge.now_utc()
        if not self._active_drop_is_live(active, now=now):
            await self._expire_drop(active, timed_out=True)
            return False
        reply_target_id = _extract_reply_target_id(message)
        if not is_answer_attempt(active["answer_spec"], message.content, direct_reply=reply_target_id == int(active["message_id"])):
            return False
        result_payload: dict[str, Any] | None = None
        feedback_line: str | None = None
        async with self._lock:
            current = self._active_drops.get((message.guild.id, message.channel.id))
            if current is None or int(current["message_id"]) != int(active["message_id"]):
                return False
            if self._channel_has_live_party_session(current["guild_id"], current["channel_id"]):
                await self._expire_drop(current, timed_out=False, announce=False, delete_post_message=True)
                return False
            exposure_id = int(current["exposure_id"])
            participants = self._attempted_users.setdefault(exposure_id, set(current.get("participant_user_ids", []) or []))
            first_attempt = message.author.id not in participants
            correct = judge_answer(current["answer_spec"], message.content)
            persisted_participants = False
            if first_attempt:
                participants.add(message.author.id)
                participant_ids = sorted(participants)
                current["participant_user_ids"] = participant_ids
                if not correct:
                    await self.store.update_active_drop_participants(current["guild_id"], current["channel_id"], participant_ids)
                    persisted_participants = True
            if correct:
                participant_ids = sorted(participants)
                try:
                    await self.store.resolve_exposure(exposure_id, resolved_at=now, winner_user_id=message.author.id)
                    await self.store.delete_active_drop(current["guild_id"], current["channel_id"])
                except Exception:
                    if first_attempt and not persisted_participants:
                        with contextlib.suppress(Exception):
                            await self.store.update_active_drop_participants(
                                current["guild_id"],
                                current["channel_id"],
                                participant_ids,
                            )
                    raise
                self._active_drops.pop((current["guild_id"], current["channel_id"]), None)
                self._clear_active_drop_runtime_state(exposure_id)
                result_payload = {
                    "guild_id": current["guild_id"],
                    "channel_id": current["channel_id"],
                    "category": current["category"],
                    "difficulty": int(current["difficulty"]),
                    "participant_ids": participant_ids,
                    "winner_user_id": message.author.id,
                    "answer_spec": current["answer_spec"],
                }
            elif (
                message.author.id not in self._wrong_feedback_users[exposure_id]
                and self._wrong_feedback_count[exposure_id] < QUESTION_DROP_WRONG_FEEDBACK_GLOBAL_LIMIT
            ):
                feedback_line = _tone_failure_line(current.get("tone_mode", "clean"))
                if feedback_line:
                    self._wrong_feedback_users[exposure_id].add(message.author.id)
                    self._wrong_feedback_count[exposure_id] += 1
        if result_payload is not None:
            updates = await self._record_participation_batch(
                guild_id=result_payload["guild_id"],
                category=result_payload["category"],
                difficulty=result_payload["difficulty"],
                participant_ids=result_payload["participant_ids"],
                winner_user_id=result_payload["winner_user_id"],
            )
            role_events = []
            update = updates.get(int(message.author.id), {}) if isinstance(updates, dict) else {}
            if isinstance(update, dict) and update:
                role_events = await self._grant_progression_rewards(
                    guild=message.guild,
                    member=message.author,
                    fallback_channel=message.channel,
                    category=str(result_payload["category"]),
                    update=update,
                )
            result_embed = await self._build_solve_embed(
                winner=message.author,
                category=str(result_payload["category"]),
                answer_spec=result_payload["answer_spec"],
                update=update,
                role_events=role_events,
                fallback_points=answer_points_for_difficulty(result_payload["difficulty"]),
            )
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=result_embed,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            return True
        if feedback_line:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Not Yet",
                        feedback_line,
                        tone="warning",
                        footer="Babblebox Question Drops",
                    ),
                    delete_after=6.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        return False

    def _clear_active_drop_runtime_state(self, exposure_id: int):
        self._wrong_feedback_users.pop(exposure_id, None)
        self._wrong_feedback_count.pop(exposure_id, None)
        self._attempted_users.pop(exposure_id, None)

    async def _record_participation_batch(
        self,
        *,
        guild_id: int,
        category: str,
        difficulty: int,
        participant_ids: list[int],
        winner_user_id: int | None,
    ) -> dict[int, dict[str, Any]]:
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return {}
        participant_set = {user_id for user_id in participant_ids if isinstance(user_id, int) and user_id > 0}
        if winner_user_id is not None and winner_user_id > 0:
            participant_set.add(winner_user_id)
        points = answer_points_for_difficulty(int(difficulty))
        results = [
            {
                "user_id": user_id,
                "category": category,
                "correct": user_id == winner_user_id,
                "points": points if user_id == winner_user_id else 0,
            }
            for user_id in sorted(participant_set)
        ]
        batch_recorder = getattr(profile_service, "record_question_drop_results_batch", None)
        if callable(batch_recorder):
            updates = await batch_recorder(results, guild_id=guild_id)
            return updates if isinstance(updates, dict) else {}
        updates = {}
        for result in results:
            updates[int(result["user_id"])] = await profile_service.record_question_drop_result(
                int(result["user_id"]),
                guild_id=guild_id,
                category=str(result["category"]),
                correct=bool(result["correct"]),
                points=int(result["points"]),
            )
        return updates

    def _unlock_exists(
        self,
        unlocks: list[dict[str, Any]],
        *,
        scope_type: str,
        scope_key: str,
        tier: int,
        role_id: int,
    ) -> bool:
        return any(
            str(item.get("scope_type", "")).casefold() == scope_type
            and str(item.get("scope_key", "")).casefold() == scope_key
            and int(item.get("tier", 0) or 0) == int(tier)
            and int(item.get("role_id", 0) or 0) == int(role_id)
            for item in unlocks
        )

    def _member_has_role(self, member, role_id: int) -> bool:
        roles = getattr(member, "roles", None)
        if not isinstance(roles, list):
            return False
        return any(int(getattr(role, "id", 0) or 0) == int(role_id) for role in roles)

    async def _grant_unlock_role(
        self,
        *,
        guild: discord.Guild,
        member,
        scope_type: str,
        scope_key: str,
        scope_label: str,
        tier: int,
        threshold: int,
        role_id: int,
        silent_grant: bool,
        announcement_channel_id: int | None,
    ) -> dict[str, Any] | None:
        if not hasattr(guild, "get_role"):
            return None
        role = guild.get_role(int(role_id))
        if role is None:
            return None
        already_has_role = self._member_has_role(member, int(role_id))
        if not already_has_role:
            add_roles = getattr(member, "add_roles", None)
            bot_member = self._bot_member_for_guild(guild)
            if not callable(add_roles) or bot_member is None:
                return None
            bot_permissions = getattr(bot_member, "guild_permissions", None)
            if not bool(getattr(bot_permissions, "manage_roles", False)):
                return None
            bot_top_role = getattr(bot_member, "top_role", None)
            bot_top_position = int(getattr(bot_top_role, "position", 0) or 0)
            role_position = int(getattr(role, "position", 0) or 0)
            if bot_top_position and role_position >= bot_top_position:
                return None
            try:
                await add_roles(role, reason=f"Babblebox Question Drops {scope_type} milestone reached")
            except (discord.Forbidden, discord.HTTPException):
                return None
        profile_service = self._profile_service()
        if profile_service is None:
            return None
        unlock_row = {
            "guild_id": guild.id,
            "user_id": int(getattr(member, "id", 0) or 0),
            "scope_type": scope_type,
            "scope_key": scope_key,
            "tier": int(tier),
            "role_id": int(role_id),
            "granted_at": ge.now_utc(),
        }
        await profile_service.store.save_question_drop_unlock(unlock_row)
        event = {
            "scope_type": scope_type,
            "scope_key": scope_key,
            "scope_label": scope_label,
            "tier": int(tier),
            "threshold": int(threshold),
            "role_id": int(role_id),
            "member_mention": getattr(member, "mention", f"<@{getattr(member, 'id', 0)}>"),
            "announcement_channel_id": announcement_channel_id,
            "silent_grant": bool(silent_grant),
            "headline": scholar_label(int(tier)) if scope_type == "scholar" else f"{category_label(scope_key)} {tier_label(int(tier))}",
        }
        return event

    async def _grant_progression_rewards(
        self,
        *,
        guild: discord.Guild,
        member,
        fallback_channel,
        category: str,
        update: dict[str, Any],
    ) -> list[dict[str, Any]]:
        profile_service = self._profile_service()
        member_id = int(getattr(member, "id", 0) or 0)
        if profile_service is None or member_id <= 0:
            return []
        config = self.get_config(guild.id)
        unlocks = await profile_service.store.fetch_question_drop_unlocks(guild_id=guild.id, user_id=member_id)
        events: list[dict[str, Any]] = []
        category_id = str(category or "").strip().casefold()
        category_points = int(update.get("guild_category_after", {}).get("points", 0) or 0)
        guild_points = int(update.get("guild_after", {}).get("points", 0) or 0)
        category_config = self._configured_category_mastery(config, category_id)
        if category_config.get("enabled"):
            for tier_config in self._configured_unlock_tiers(category_config):
                tier = int(tier_config.get("tier", 0) or 0)
                role_id = int(tier_config.get("role_id", 0) or 0)
                threshold = int(tier_config.get("threshold", 0) or 0)
                if category_points < threshold or self._unlock_exists(unlocks, scope_type="category", scope_key=category_id, tier=tier, role_id=role_id):
                    continue
                event = await self._grant_unlock_role(
                    guild=guild,
                    member=member,
                    scope_type="category",
                    scope_key=category_id,
                    scope_label=category_label_with_emoji(category_id),
                    tier=tier,
                    threshold=threshold,
                    role_id=role_id,
                    silent_grant=bool(category_config.get("silent_grant")),
                    announcement_channel_id=category_config.get("announcement_channel_id"),
                )
                if event is not None:
                    events.append(event)
                    unlocks.append(
                        {
                            "scope_type": "category",
                            "scope_key": category_id,
                            "tier": tier,
                            "role_id": role_id,
                        }
                    )
                    if not bool(category_config.get("silent_grant")):
                        await self._announce_role_grant(guild, fallback_channel, event)
        scholar_config = dict(config.get("scholar_ladder", {}))
        if scholar_config.get("enabled"):
            for tier_config in self._configured_unlock_tiers(scholar_config):
                tier = int(tier_config.get("tier", 0) or 0)
                role_id = int(tier_config.get("role_id", 0) or 0)
                threshold = int(tier_config.get("threshold", 0) or 0)
                if guild_points < threshold or self._unlock_exists(unlocks, scope_type="scholar", scope_key="global", tier=tier, role_id=role_id):
                    continue
                event = await self._grant_unlock_role(
                    guild=guild,
                    member=member,
                    scope_type="scholar",
                    scope_key="global",
                    scope_label="the scholar ladder",
                    tier=tier,
                    threshold=threshold,
                    role_id=role_id,
                    silent_grant=bool(scholar_config.get("silent_grant")),
                    announcement_channel_id=scholar_config.get("announcement_channel_id"),
                )
                if event is not None:
                    events.append(event)
                    unlocks.append(
                        {
                            "scope_type": "scholar",
                            "scope_key": "global",
                            "tier": tier,
                            "role_id": role_id,
                        }
                    )
                    if not bool(scholar_config.get("silent_grant")):
                        await self._announce_role_grant(guild, fallback_channel, event)
        return events

    def _ai_event_allowed(self, *, mode: str, flags: dict[str, Any]) -> bool:
        if mode == "off":
            return False
        event_only = bool(
            any(int(event.get("tier", 0) or 0) >= 3 for event in flags.get("category_role_events", []))
            or any(int(event.get("tier", 0) or 0) >= 1 for event in flags.get("scholar_role_events", []))
            or flags.get("took_guild_first")
            or flags.get("took_category_first")
        )
        if mode == "event_only":
            return event_only
        return bool(
            event_only
            or flags.get("guild_points_milestone")
            or flags.get("new_best_streak")
            or flags.get("guild_rank_jump")
        )

    async def _maybe_ai_highlight(
        self,
        *,
        winner,
        category: str,
        answer: str,
        update: dict[str, Any],
        flags: dict[str, Any],
    ) -> str | None:
        config = self.get_config(int(update.get("guild_id", 0) or 0))
        if not config.get("ai_celebrations_enabled"):
            return None
        mode = str(self.get_meta().get("ai_celebration_mode", "off")).casefold()
        if not self._ai_event_allowed(mode=mode, flags=flags):
            return None
        return await self._ai_provider.highlight(
            {
                "mode": mode,
                "winner": ge.display_name_of(winner),
                "category": category_label(category),
                "answer": answer,
                "points_awarded": int(update.get("points_awarded", 0) or 0),
                "current_streak": int(update.get("guild_after", {}).get("current_streak", 0) or 0),
                "best_streak": int(update.get("guild_after", {}).get("best_streak", 0) or 0),
                "guild_rank_before": update.get("guild_rank_before"),
                "guild_rank_after": update.get("guild_rank_after"),
                "category_rank_before": update.get("category_rank_before"),
                "category_rank_after": update.get("category_rank_after"),
                "role_events": [
                    f"{event.get('headline')} at {event.get('threshold')} points"
                    for event in (flags.get("category_role_events", []) + flags.get("scholar_role_events", []))
                ],
            }
        )

    async def _build_solve_embed(
        self,
        *,
        winner,
        category: str,
        answer_spec: dict[str, Any],
        update: dict[str, Any],
        role_events: list[dict[str, Any]],
        fallback_points: int,
    ) -> discord.Embed:
        category_id = str(category or "").strip().casefold()
        answer = render_answer_summary(answer_spec)
        flags = self._milestone_flags(update, role_events)
        guild_after = update.get("guild_after", {}) if isinstance(update, dict) else {}
        category_after = update.get("guild_category_after", {}) if isinstance(update, dict) else {}
        title = f"{category_emoji(category_id)} Solved"
        if any(int(event.get("tier", 0) or 0) >= 3 for event in flags["category_role_events"]):
            title = f"{progression_emoji('mastery')} {category_label(category_id)} Mastery"
        elif flags["scholar_role_events"]:
            title = f"{progression_emoji('scholar')} Scholar Rank Up"
        elif flags["took_guild_first"] or flags["took_category_first"]:
            title = f"{progression_emoji('move')} New #1"
        elif flags["guild_points_milestone"] is not None:
            title = f"{progression_emoji('scholar')} {int(flags['guild_points_milestone'])} Knowledge Points"
        elif flags["new_best_streak"]:
            title = f"{progression_emoji('streak')} New Best Streak"
        elif flags["first_category_correct"]:
            title = f"{category_emoji(category_id)} First {category_label(category_id)} Solve"
        ai_highlight = await self._maybe_ai_highlight(
            winner=winner,
            category=category_id,
            answer=answer,
            update=update,
            flags=flags,
        )
        description_lines = []
        if ai_highlight:
            description_lines.append(ai_highlight)
        else:
            description_lines.append(
                f"{getattr(winner, 'mention', ge.display_name_of(winner))} solved **{answer}** for **{int(update.get('points_awarded', 0) or fallback_points)}** points."
            )
        detail_bits = [category_label_with_emoji(category_id)]
        current_streak = int(guild_after.get("current_streak", 0) or 0)
        if current_streak > 1:
            detail_bits.append(f"{progression_emoji('streak')} {current_streak} streak")
        rank_jump = max(int(flags.get("guild_rank_jump", 0) or 0), int(flags.get("category_rank_jump", 0) or 0))
        if rank_jump > 0:
            detail_bits.append(f"{progression_emoji('move')} up {rank_jump}")
        description_lines.append(" | ".join(detail_bits))
        highlight_lines = []
        for event in role_events[:2]:
            highlight_lines.append(
                f"{progression_emoji('role')} {event['headline']} unlocked at **{int(event['threshold'])}** pts"
            )
        if flags["guild_points_milestone"] is not None:
            highlight_lines.append(
                f"{progression_emoji('scholar')} Reached **{int(flags['guild_points_milestone'])}** guild knowledge points"
            )
        profile_service = self._profile_service()
        unlocks = []
        profile_store = getattr(profile_service, "store", None) if profile_service is not None else None
        if profile_store is not None and hasattr(profile_store, "fetch_question_drop_unlocks"):
            unlocks = await profile_store.fetch_question_drop_unlocks(
                guild_id=int(update.get("guild_id", 0) or 0),
                user_id=int(update.get("user_id", 0) or 0),
            )
        config = self.get_config(int(update.get("guild_id", 0) or 0))
        current_category_tier = self._current_scope_tier(unlocks, scope_type="category", scope_key=category_id)
        category_config = self._configured_category_mastery(config, category_id)
        next_category_tier = None
        if category_config.get("enabled"):
            next_category_tier = self._next_scope_tier(
                self._configured_unlock_tiers(category_config),
                points=int(category_after.get("points", 0) or 0),
                current_tier=current_category_tier,
            )
        current_scholar_tier = self._current_scope_tier(unlocks, scope_type="scholar", scope_key="global")
        scholar_config = dict(config.get("scholar_ladder", {}))
        next_scholar_tier = None
        if scholar_config.get("enabled"):
            next_scholar_tier = self._next_scope_tier(
                self._configured_unlock_tiers(scholar_config),
                points=int(guild_after.get("points", 0) or 0),
                current_tier=current_scholar_tier,
            )
        if next_category_tier is not None:
            highlight_lines.append(
                f"{progression_emoji('next')} {int(next_category_tier['remaining'])} pts to {tier_label(int(next_category_tier['tier']))} in {category_label(category_id)}"
            )
        elif next_scholar_tier is not None:
            highlight_lines.append(
                f"{progression_emoji('next')} {int(next_scholar_tier['remaining'])} pts to {scholar_label(int(next_scholar_tier['tier']))}"
            )
        if highlight_lines:
            description_lines.extend(highlight_lines[:2])
        return ge.make_status_embed(
            title,
            "\n".join(description_lines),
            tone="success",
            footer=f"Babblebox Question Drops | {category_label(category_id)}",
        )

    async def handle_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return
        active = next(
            (
                record
                for (guild_id, _), record in self._active_drops.items()
                if guild_id == payload.guild_id and int(record["message_id"]) == int(payload.message_id)
            ),
            None,
        )
        if active is None:
            return
        await self._expire_drop(active, timed_out=False)

    async def _scheduler_loop(self):
        while True:
            self._wake_event.clear()
            await self._expire_due_drops()
            await self._expire_stale_pending_posts()
            await self._maybe_post_due_drops()
            await self._prune_old_exposures()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=QUESTION_DROP_SCHEDULER_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    async def _expire_stale_pending_posts(self):
        if not self._pending_posts:
            return
        await self._sweep_pending_posts(list(self._pending_posts.values()), force=False)

    async def _prune_old_exposures(self):
        if not self.storage_ready:
            return
        now = ge.now_utc()
        if self._next_prune_at is not None and now < self._next_prune_at:
            return
        cutoff = ge.now_utc() - timedelta(days=QUESTION_DROP_EXPOSURE_RETENTION_DAYS)
        await self.store.prune_exposures(before=cutoff, limit=500)
        self._next_prune_at = now + timedelta(seconds=QUESTION_DROP_PRUNE_INTERVAL_SECONDS)

    def _active_drop_is_live(self, record: dict[str, Any], *, now: datetime) -> bool:
        expires_raw = record.get("expires_at")
        if isinstance(expires_raw, datetime):
            expires_at = expires_raw.astimezone(timezone.utc) if expires_raw.tzinfo else expires_raw.replace(tzinfo=timezone.utc)
        else:
            expires_at = datetime.fromisoformat(str(expires_raw))
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
        return expires_at > now

    async def _expire_due_drops(self):
        now = ge.now_utc()
        for record in list(self._active_drops.values()):
            if self._active_drop_is_live(record, now=now):
                continue
            await self._expire_drop(record, timed_out=True)

    async def _expire_drop(
        self,
        record: dict[str, Any],
        *,
        timed_out: bool,
        announce: bool = True,
        delete_post_message: bool = False,
    ):
        exposure_id = int(record["exposure_id"])
        await self.store.resolve_exposure(exposure_id, resolved_at=ge.now_utc(), winner_user_id=None)
        await self.store.delete_active_drop(record["guild_id"], record["channel_id"])
        self._active_drops.pop((record["guild_id"], record["channel_id"]), None)
        participant_ids = sorted(self._attempted_users.get(exposure_id, set(record.get("participant_user_ids", []) or [])))
        self._clear_active_drop_runtime_state(exposure_id)
        if delete_post_message:
            await self._delete_message_if_exists(record["channel_id"], record.get("message_id"))
        if participant_ids:
            await self._record_participation_batch(
                guild_id=record["guild_id"],
                category=record["category"],
                difficulty=int(record["difficulty"]),
                participant_ids=participant_ids,
                winner_user_id=None,
            )
        if not announce:
            return
        channel = self.bot.get_channel(record["channel_id"]) if hasattr(self.bot, "get_channel") else None
        if channel is None:
            return
        answer = render_answer_summary(record["answer_spec"])
        title = "Time's Up" if timed_out else "Drop Closed"
        with contextlib.suppress(discord.HTTPException):
            await channel.send(
                embed=ge.make_status_embed(
                    title,
                    f"No clean solve this time. Answer: **{answer}**.",
                    tone="info",
                    footer="Babblebox Question Drops",
                )
            )

    async def _maybe_post_due_drops(self):
        if not self.storage_ready:
            return
        now = ge.now_utc()
        for guild_id, config in list(self._configs.items()):
            if not config.get("enabled"):
                continue
            if not config.get("enabled_channel_ids"):
                continue
            tzinfo = load_afk_timezone(config.get("timezone")) or timezone.utc
            now_local = now.astimezone(tzinfo)
            slots = _daily_slot_datetimes(guild_id, now_local.date(), config)
            if not slots:
                continue
            active_slot_keys = {
                record["slot_key"]
                for (active_guild_id, _), record in self._active_drops.items()
                if active_guild_id == guild_id
            }
            active_slot_keys.update(
                record["slot_key"] for (pending_guild_id, _), record in self._pending_posts.items() if pending_guild_id == guild_id
            )
            due_slots: list[str] = []
            for index, local_slot in enumerate(slots):
                slot_key = _slot_key(now_local.date(), index)
                if slot_key in active_slot_keys:
                    continue
                slot_utc = local_slot.astimezone(timezone.utc)
                if slot_utc > now or (now - slot_utc).total_seconds() > QUESTION_DROP_SLOT_GRACE_SECONDS:
                    continue
                due_slots.append(slot_key)
            if not due_slots:
                continue
            exposures = await self.store.list_exposures_for_guild(guild_id, limit=QUESTION_DROP_EXPOSURE_FETCH_LIMIT)
            used_slot_keys = {record["slot_key"] for record in exposures if isinstance(record.get("slot_key"), str)}
            for slot_key in due_slots:
                if slot_key in used_slot_keys:
                    continue
                channel = self._select_channel_for_slot(guild_id, config, exposures, slot_key=slot_key, now=now)
                if channel is None:
                    continue
                await self._post_drop_to_channel(guild_id, channel, config=config, exposures=exposures, slot_key=slot_key, asked_at=now)
                break

    def _select_channel_for_slot(
        self,
        guild_id: int,
        config: dict[str, Any],
        exposures: list[dict[str, Any]],
        *,
        slot_key: str,
        now: datetime,
    ) -> discord.abc.Messageable | None:
        candidates = []
        for channel_id in config.get("enabled_channel_ids", []):
            if (guild_id, channel_id) in self._active_drops:
                continue
            if self._channel_has_pending_post(guild_id, channel_id):
                continue
            if self._channel_has_live_party_session(guild_id, channel_id):
                continue
            channel = self.bot.get_channel(channel_id) if hasattr(self.bot, "get_channel") else None
            if channel is None:
                continue
            if config.get("activity_gate") == "light":
                last_activity = self._recent_activity.get((guild_id, channel_id))
                if last_activity is None or (now - last_activity).total_seconds() > QUESTION_DROP_ACTIVITY_WINDOW_SECONDS:
                    continue
            recent_count = sum(1 for record in exposures[:40] if int(record["channel_id"]) == int(channel_id))
            jitter = int(build_variant_hash(str(channel_id), slot_key), 16) % 7
            candidates.append((recent_count, jitter, channel))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates[0][2]

    def _channel_has_live_party_session(self, guild_id: int, channel_id: int) -> bool:
        game = ge.games.get(guild_id)
        if game is None or game.get("closing"):
            return False
        live_channel = getattr(game.get("channel"), "id", None)
        return int(live_channel or 0) == int(channel_id)

    async def _post_drop_to_channel(
        self,
        guild_id: int,
        channel: discord.abc.Messageable,
        *,
        config: dict[str, Any],
        exposures: list[dict[str, Any]],
        slot_key: str,
        asked_at: datetime,
    ):
        variant = self._select_variant(guild_id, getattr(channel, "id", 0), exposures=exposures, slot_key=slot_key, config=config)
        if variant is None:
            return
        channel_id = getattr(channel, "id", 0)
        pending = await self.store.claim_pending_post(
            {
                "guild_id": guild_id,
                "channel_id": channel_id,
                "slot_key": slot_key,
                "concept_id": variant.concept_id,
                "variant_hash": variant.variant_hash,
                "claimed_at": asked_at,
                "lease_expires_at": asked_at + timedelta(seconds=QUESTION_DROP_PENDING_LEASE_SECONDS),
                "message_id": None,
            }
        )
        if pending is None:
            return
        self._pending_posts[(guild_id, slot_key)] = pending
        prompt = variant.prompt
        embed = discord.Embed(
            title="Question Drop",
            description=prompt,
            color=ge.EMBED_THEME["accent"],
            timestamp=asked_at,
        )
        embed.add_field(
            name="Round",
            value=(
                f"Category: **{QUESTION_DROP_CATEGORY_LABELS.get(variant.category, variant.category.title())}**\n"
                f"Difficulty: **{QUESTION_DROP_DIFFICULTY_LABELS.get(variant.difficulty, 'Easy')}**\n"
                f"Window: **{int(config.get('answer_window_seconds', 60))} seconds**"
            ),
            inline=False,
        )
        answer_lane = render_answer_instruction(variant.answer_spec)
        embed.add_field(name="Answering", value=answer_lane, inline=False)
        embed = ge.style_embed(embed, footer="Babblebox Question Drops | First correct answer scores")
        message = None
        try:
            message = await channel.send(embed=embed)
            await self.store.attach_pending_post_message(guild_id, slot_key, message.id)
            self._pending_posts[(guild_id, slot_key)]["message_id"] = message.id
        except Exception:
            if message is not None:
                with contextlib.suppress(discord.HTTPException, AttributeError):
                    await message.delete()
            await self._release_pending_post(guild_id, slot_key)
            return
        exposure_record = {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "concept_id": variant.concept_id,
            "variant_hash": variant.variant_hash,
            "category": variant.category,
            "difficulty": variant.difficulty,
            "asked_at": asked_at,
            "resolved_at": None,
            "winner_user_id": None,
            "slot_key": slot_key,
        }
        active_record = {
            "guild_id": guild_id,
            "channel_id": channel_id,
            "message_id": message.id,
            "author_user_id": int(getattr(getattr(self.bot, "user", None), "id", 1) or 1),
            "concept_id": variant.concept_id,
            "variant_hash": variant.variant_hash,
            "category": variant.category,
            "difficulty": variant.difficulty,
            "prompt": variant.prompt,
            "answer_spec": variant.answer_spec,
            "asked_at": asked_at,
            "expires_at": asked_at + timedelta(seconds=int(config.get("answer_window_seconds", 60))),
            "slot_key": slot_key,
            "tone_mode": config.get("tone_mode", "clean"),
            "participant_user_ids": [],
        }
        try:
            stored_exposure, stored_active = await self.store.finalize_pending_post(
                guild_id,
                slot_key,
                exposure_record=exposure_record,
                active_record=active_record,
            )
        except Exception:
            with contextlib.suppress(discord.HTTPException):
                await message.delete()
            await self._release_pending_post(guild_id, slot_key)
            return
        normalized_record = normalize_active_drop(stored_active)
        if normalized_record is None:
            with contextlib.suppress(discord.HTTPException):
                await message.delete()
            await self.store.delete_active_drop(guild_id, channel_id)
            exposure_id = stored_exposure.get("id")
            if isinstance(exposure_id, int) and exposure_id > 0:
                await self.store.delete_exposure(exposure_id)
            return
        self._pending_posts.pop((guild_id, slot_key), None)
        self._active_drops[(guild_id, channel_id)] = normalized_record
        exposure_id = int(normalized_record["exposure_id"])
        self._wrong_feedback_users[exposure_id] = set()
        self._wrong_feedback_count[exposure_id] = 0
        self._attempted_users[exposure_id] = set()

    def _repeat_windows_for_variant(self, variant: QuestionDropVariant, *, category_variant_capacity: Counter, category_concept_counts: Counter) -> tuple[int, int, float, float]:
        if variant.source_type == "generated":
            preferred_concept_days = 1.0 if category_variant_capacity[variant.category] <= 8 else 2.0
            preferred_variant_days = 2.0 if category_variant_capacity[variant.category] <= 8 else 3.0
            return (
                QUESTION_DROP_GENERATED_CONCEPT_MIN_DAYS,
                QUESTION_DROP_GENERATED_VARIANT_MIN_DAYS,
                preferred_concept_days,
                preferred_variant_days,
            )
        preferred_concept_days = 2.0 if category_concept_counts[variant.category] <= 3 else 3.0
        preferred_variant_days = 4.0 if category_variant_capacity[variant.category] <= 6 else 6.0
        return (
            QUESTION_DROP_CURATED_CONCEPT_MIN_DAYS,
            QUESTION_DROP_CURATED_VARIANT_MIN_DAYS,
            preferred_concept_days,
            preferred_variant_days,
        )

    def _select_variant(
        self,
        guild_id: int,
        channel_id: int,
        *,
        exposures: list[dict[str, Any]],
        slot_key: str,
        config: dict[str, Any],
    ) -> QuestionDropVariant | None:
        allowed_categories = set(self._enabled_categories(config))
        if not allowed_categories:
            return None
        candidates = iter_candidate_variants(
            categories=allowed_categories,
            seed_material=_slot_seed_material(guild_id, channel_id, slot_key),
            variants_per_seed=QUESTION_DROP_GENERATED_VARIANTS_PER_SEED,
        )
        if not candidates:
            return None
        recent_by_concept: dict[str, datetime] = {}
        recent_by_variant: dict[str, datetime] = {}
        category_counts = Counter()
        difficulty_counts = Counter()
        source_counts = Counter()
        category_concept_counts = Counter()
        category_variant_capacity = Counter()
        candidate_categories = {candidate.category for candidate in candidates}
        candidate_difficulties = {int(candidate.difficulty) for candidate in candidates}
        same_day_concepts: set[str] = set()
        seen_category_concepts: set[tuple[str, str]] = set()
        now = ge.now_utc()
        slot_day_key = str(slot_key).split(":", 1)[0]
        for candidate in candidates:
            category_variant_capacity[candidate.category] += 1
            concept_key = (candidate.category, candidate.concept_id)
            if concept_key not in seen_category_concepts:
                seen_category_concepts.add(concept_key)
                category_concept_counts[candidate.category] += 1
        for record in exposures:
            asked_at = datetime.fromisoformat(record["asked_at"])
            if asked_at.tzinfo is None:
                asked_at = asked_at.replace(tzinfo=timezone.utc)
            recent_by_concept.setdefault(record["concept_id"], asked_at)
            recent_by_variant.setdefault(record["variant_hash"], asked_at)
            if str(record.get("slot_key") or "").split(":", 1)[0] == slot_day_key:
                same_day_concepts.add(str(record["concept_id"]))
            if (now - asked_at).days <= 14:
                category_counts[record["category"]] += 1
                difficulty_counts[int(record["difficulty"])] += 1
                seed = question_drop_seed_for_concept(record["concept_id"]) or {}
                source_counts[str(seed.get("source_type", "curated"))] += 1
        scored: list[tuple[float, QuestionDropVariant]] = []
        same_day_scored: list[tuple[float, QuestionDropVariant]] = []
        generated_gap = max(0, source_counts["curated"] - source_counts["generated"])
        drop_pressure = max(0, int(config.get("drops_per_day", 2) or 2) - 2)
        category_floor = min((category_counts[category] for category in candidate_categories), default=0)
        difficulty_floor = min((difficulty_counts[difficulty] for difficulty in candidate_difficulties), default=0)
        for variant in candidates:
            concept_cooldown_days, variant_cooldown_days, preferred_concept_days, preferred_variant_days = self._repeat_windows_for_variant(
                variant,
                category_variant_capacity=category_variant_capacity,
                category_concept_counts=category_concept_counts,
            )
            if drop_pressure:
                if variant.source_type == "generated":
                    preferred_concept_days += min(drop_pressure * 0.25, 1.5)
                    preferred_variant_days += min(drop_pressure * 0.35, 2.0)
                else:
                    preferred_concept_days += min(drop_pressure * 0.75, 3.0)
                    preferred_variant_days += min(drop_pressure * 0.9, 4.0)
            concept_seen_at = recent_by_concept.get(variant.concept_id)
            days_since_concept = ((now - concept_seen_at).total_seconds() / 86400.0) if concept_seen_at is not None else None
            if days_since_concept is not None and days_since_concept < concept_cooldown_days:
                continue
            variant_seen_at = recent_by_variant.get(variant.variant_hash)
            days_since_variant = ((now - variant_seen_at).total_seconds() / 86400.0) if variant_seen_at is not None else None
            if days_since_variant is not None and days_since_variant < variant_cooldown_days:
                continue
            freshness = 18.0 if concept_seen_at is None else min(days_since_concept * 4.0, 16.0)
            variant_freshness = 9.0 if variant_seen_at is None else min(days_since_variant * 2.0, 8.0)
            if days_since_concept is not None and days_since_concept < preferred_concept_days:
                freshness -= (preferred_concept_days - days_since_concept) * (6.0 if variant.source_type == "curated" else 2.5)
            if days_since_variant is not None and days_since_variant < preferred_variant_days:
                variant_freshness -= (preferred_variant_days - days_since_variant) * (4.0 if variant.source_type == "curated" else 1.75)
            category_balance = 8.0 - category_counts[variant.category]
            category_gap = max(0, category_counts[variant.category] - category_floor)
            free_category_repeats = 2 if drop_pressure >= 4 else 1
            if category_gap > free_category_repeats:
                spread_penalty = category_gap - free_category_repeats
                category_balance -= spread_penalty * (0.9 + min(drop_pressure * 0.2, 1.6))
                if variant.source_type == "generated":
                    category_balance -= spread_penalty * min(drop_pressure * 0.18, 1.2)
            difficulty_balance = 5.0 - difficulty_counts[int(variant.difficulty)]
            difficulty_gap = max(0, difficulty_counts[int(variant.difficulty)] - difficulty_floor - 1)
            if difficulty_gap:
                difficulty_balance -= difficulty_gap * (0.35 + min(drop_pressure * 0.08, 0.6))
            if variant.source_type == "generated":
                source_balance = 2.5 + min(generated_gap * 0.75, 4.0) + min(drop_pressure * 0.5, 3.0)
                pool_depth_bonus = (min(category_variant_capacity[variant.category], 12) * 0.16) + min(drop_pressure * 0.2, 1.2)
            else:
                source_balance = 1.5 - min(generated_gap * 0.25, 2.0) - min(drop_pressure * 0.35, 2.0)
                pool_depth_bonus = min(category_variant_capacity[variant.category], 12) * 0.06
            jitter = (int(build_variant_hash(slot_key, variant.variant_hash), 16) % 1000) / 1000.0
            score = freshness + variant_freshness + category_balance + difficulty_balance + source_balance + pool_depth_bonus + jitter
            if variant.concept_id in same_day_concepts:
                same_day_scored.append((score - 12.0, variant))
            else:
                scored.append((score, variant))
        if not scored:
            scored = same_day_scored
        if not scored:
            return None
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    async def _next_slot_for_guild(self, guild_id: int, *, config: dict[str, Any] | None = None) -> datetime | None:
        config = config or self.get_config(guild_id)
        if not config.get("enabled"):
            return None
        if not self._enabled_categories(config):
            return None
        if not config.get("enabled_channel_ids"):
            return None
        tzinfo = load_afk_timezone(config.get("timezone")) or timezone.utc
        now = ge.now_utc()
        exposures = await self.store.list_exposures_for_guild(guild_id, limit=QUESTION_DROP_EXPOSURE_FETCH_LIMIT)
        used_slot_keys = {record["slot_key"] for record in exposures if isinstance(record.get("slot_key"), str)}
        active_slot_keys = {
            record["slot_key"]
            for (active_guild_id, _), record in self._active_drops.items()
            if active_guild_id == guild_id
        }
        active_slot_keys.update(
            record["slot_key"] for (pending_guild_id, _), record in self._pending_posts.items() if pending_guild_id == guild_id
        )
        for day_offset in range(0, 2):
            local_day = now.astimezone(tzinfo).date() + timedelta(days=day_offset)
            for index, local_slot in enumerate(_daily_slot_datetimes(guild_id, local_day, config)):
                slot_key = _slot_key(local_day, index)
                if slot_key in used_slot_keys or slot_key in active_slot_keys:
                    continue
                return local_slot.astimezone(timezone.utc)
        return None


def _extract_reply_target_id(message: discord.Message) -> int | None:
    reference = getattr(message, "reference", None)
    message_id = getattr(reference, "message_id", None)
    if isinstance(message_id, int):
        return message_id
    resolved = getattr(reference, "resolved", None)
    cached = getattr(reference, "cached_message", None)
    for candidate in (resolved, cached):
        candidate_id = getattr(candidate, "id", None)
        if isinstance(candidate_id, int):
            return candidate_id
    return None
