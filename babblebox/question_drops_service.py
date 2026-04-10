from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import random
import traceback
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
    QUESTION_DROP_DIFFICULTY_PROFILE_LABELS,
    QUESTION_DROP_DIFFICULTY_PROFILES,
    QUESTION_DROP_TONES,
    QuestionDropVariant,
    answer_attempt_limit,
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
    answer_type_emoji,
    category_emoji,
    category_label,
    category_label_with_emoji,
    leaderboard_marker,
    progression_emoji,
    scholar_label,
    state_emoji,
    tier_label,
)
from babblebox.question_drops_store import (
    QUESTION_DROP_DIGEST_KINDS,
    QUESTION_DROP_DIGEST_MENTION_MODES,
    QUESTION_DROP_MAX_DROPS_PER_DAY,
    QUESTION_DROP_MIN_DROPS_PER_DAY,
    QUESTION_DROP_MASTERY_TIERS,
    QuestionDropsStorageUnavailable,
    QuestionDropsStore,
    default_question_drop_digest_settings,
    default_question_drops_config,
    default_question_drops_meta,
    normalize_active_drop,
    normalize_question_drops_config,
    normalize_question_drops_meta,
)
from babblebox.text_safety import contains_blocklisted_term, find_private_pattern, normalize_plain_text, sanitize_short_plain_template
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
QUESTION_DROP_LATE_CORRECT_WINDOW_SECONDS = 8
QUESTION_DROP_LATE_CORRECT_MAX_ACKS = 2
QUESTION_DROP_CLOSE_BUFFER_SECONDS = 3
QUESTION_DROP_PRUNE_INTERVAL_SECONDS = 6 * 60 * 60
QUESTION_DROP_EXPOSURE_FETCH_LIMIT = 220
QUESTION_DROP_GENERATED_VARIANTS_PER_SEED = 8
QUESTION_DROP_PARTICIPATION_RETENTION_DAYS = 180
QUESTION_DROP_DIGEST_LEASE_SECONDS = 10 * 60
QUESTION_DROP_DIGEST_POST_HOUR = 9
QUESTION_DROP_DIGEST_WEEKLY_MIN_SOLVES = 4
QUESTION_DROP_DIGEST_WEEKLY_MIN_PARTICIPANTS = 2
QUESTION_DROP_DIGEST_MONTHLY_MIN_SOLVES = 10
QUESTION_DROP_DIGEST_MONTHLY_MIN_PARTICIPANTS = 3
QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_MAX_LENGTH = 220
QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_SENTENCE_LIMIT = 2
QUESTION_DROP_SHARED_ANNOUNCEMENT_PLACEHOLDERS = (
    "{user.mention}",
    "{user.name}",
    "{user.display_name}",
    "{role.name}",
    "{tier.label}",
    "{threshold}",
)
QUESTION_DROP_CATEGORY_ANNOUNCEMENT_PLACEHOLDERS = QUESTION_DROP_SHARED_ANNOUNCEMENT_PLACEHOLDERS + ("{category.name}",)
QUESTION_DROP_DIFFICULTY_MIX = {
    "standard": {"low": {1: 50, 2: 40, 3: 10}, "medium": {1: 35, 2: 45, 3: 20}, "high": {1: 25, 2: 45, 3: 30}},
    "smart": {"low": {1: 35, 2: 45, 3: 20}, "medium": {1: 25, 2: 45, 3: 30}, "high": {1: 15, 2: 45, 3: 40}},
    "hard": {"low": {1: 20, 2: 45, 3: 35}, "medium": {1: 15, 2: 40, 3: 45}, "high": {1: 10, 2: 35, 3: 55}},
}


LOGGER = logging.getLogger(__name__)


def _config_signature(config: dict[str, Any]) -> str:
    parts = [
        str(config.get("drops_per_day")),
        str(config.get("timezone")),
        str(config.get("answer_window_seconds")),
        str(config.get("tone_mode")),
        str(config.get("difficulty_profile")),
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


def _difficulty_bucket(drops_per_day: int) -> str:
    if drops_per_day <= 3:
        return "low"
    if drops_per_day <= 6:
        return "medium"
    return "high"


def _difficulty_mix_for(config: dict[str, Any]) -> dict[int, int]:
    profile = str(config.get("difficulty_profile", "standard")).strip().casefold()
    if profile not in QUESTION_DROP_DIFFICULTY_PROFILES:
        profile = "standard"
    bucket = _difficulty_bucket(int(config.get("drops_per_day", 2) or 2))
    return dict(QUESTION_DROP_DIFFICULTY_MIX[profile][bucket])


def _attempt_feedback_line(tone_mode: str, *, attempts_left: int) -> str:
    if attempts_left <= 0:
        if tone_mode == "playful":
            return "Not this one. You're out of attempts for this drop."
        if tone_mode == "roast-light":
            return "Confident guess, wrong answer. You're out of attempts for this drop."
        return "Wrong answer. You're out of attempts for this drop."
    remaining_label = "1 attempt left" if attempts_left == 1 else f"{attempts_left} attempts left"
    if tone_mode == "playful":
        return f"Not this one. {remaining_label} for this drop."
    if tone_mode == "roast-light":
        return f"Confident guess, wrong answer. {remaining_label} for this drop."
    return f"Wrong answer. {remaining_label} for this drop."


@dataclass(frozen=True)
class QuestionDropStatusSnapshot:
    config: dict[str, Any]
    active_drop_count: int
    next_slot_at: datetime | None
    enabled_channel_mentions: tuple[str, ...]
    digest_status: dict[str, Any]


@dataclass(frozen=True)
class QuestionDropDigestPeriod:
    kind: str
    period_key: str
    period_start_at: datetime
    period_end_at: datetime
    scheduled_post_at: datetime
    timezone_name: str


@dataclass
class QuestionDropRecentResolutionContext:
    guild_id: int
    channel_id: int
    message_id: int
    exposure_id: int
    category: str
    difficulty: int
    answer_spec: dict[str, Any]
    asked_at: datetime
    score_deadline_at: datetime
    winner_user_id: int | None
    resolved_at: datetime
    participant_user_ids: list[int]
    attempt_counts_by_user: dict[int, int]
    acknowledged_user_ids: set[int]
    timeout_result_message_id: int | None = None
    participation_finalized: bool = False

    @property
    def late_window_ends_at(self) -> datetime:
        return self.resolved_at + timedelta(seconds=QUESTION_DROP_LATE_CORRECT_WINDOW_SECONDS)


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
        self._recently_resolved_drops: dict[tuple[int, int], QuestionDropRecentResolutionContext] = {}
        self._attempted_users: dict[int, set[int]] = defaultdict(set)
        self._attempt_counts_by_user: dict[int, dict[int, int]] = defaultdict(dict)
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
        try:
            self._configs = await self.store.fetch_all_configs()
            self._meta = normalize_question_drops_meta(await self.store.fetch_meta() or default_question_drops_meta())
            await self._sweep_pending_posts(await self.store.list_pending_posts(), force=True)
            self._next_prune_at = ge.now_utc()
            await self._restore_active_rows(await self.store.list_active_drops())
        except Exception as exc:
            self.storage_ready = False
            self.storage_error = f"Question Drops storage state could not be loaded: {exc}"
            self._configs = {}
            self._active_drops = {}
            self._pending_posts = {}
            self._recent_activity.clear()
            self._attempted_users.clear()
            self._attempt_counts_by_user.clear()
            self._next_prune_at = None
            self._meta = default_question_drops_meta()
            print(f"Question Drops storage hydration failed: {exc}")
            traceback.print_exc()
            return False
        self.storage_ready = True
        self.storage_error = None
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
            self._attempted_users[exposure_id] = set(record.get("participant_user_ids", []) or [])
            self._attempt_counts_by_user[exposure_id] = dict(record.get("attempt_counts_by_user", {}) or {})

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
        return f"{feature_name} are temporarily unavailable because Babblebox could not load their storage state."

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

    def _effective_digest_settings(self, config: dict[str, Any]) -> dict[str, Any]:
        merged = default_question_drop_digest_settings()
        raw = config.get("digest_settings", {})
        if isinstance(raw, dict):
            merged.update(raw)
        timezone_name = str(merged.get("timezone") or config.get("timezone") or "UTC").strip() or "UTC"
        merged["timezone"] = timezone_name
        merged["weekly_enabled"] = bool(merged.get("weekly_enabled"))
        merged["monthly_enabled"] = bool(merged.get("monthly_enabled"))
        merged["skip_low_activity"] = bool(merged.get("skip_low_activity", True))
        mention_mode = str(merged.get("mention_mode", "none")).strip().casefold()
        merged["mention_mode"] = mention_mode if mention_mode in QUESTION_DROP_DIGEST_MENTION_MODES else "none"
        for field in ("weekly_channel_id", "monthly_channel_id"):
            value = merged.get(field)
            merged[field] = int(value) if isinstance(value, int) and value > 0 else None
        return merged

    def _digest_timezone(self, settings: dict[str, Any]):
        return load_afk_timezone(settings.get("timezone")) or timezone.utc

    def _digest_channel_issue(self, guild: discord.Guild, channel_id: int | None, *, label: str) -> str | None:
        if not isinstance(channel_id, int) or channel_id <= 0:
            return f"{label}: digest channel is not configured."
        if not hasattr(guild, "get_channel"):
            return f"{label}: digest channel could not be resolved."
        channel = guild.get_channel(channel_id)
        if channel is None:
            return f"{label}: digest channel is missing."
        permissions_for = getattr(channel, "permissions_for", None)
        bot_member = self._bot_member_for_guild(guild)
        if callable(permissions_for) and bot_member is not None:
            perms = permissions_for(bot_member)
            if not bool(getattr(perms, "view_channel", False)):
                return f"{label}: cannot view the digest channel."
            if not bool(getattr(perms, "send_messages", False)):
                return f"{label}: cannot send messages in the digest channel."
            if not bool(getattr(perms, "embed_links", False)):
                return f"{label}: missing `Embed Links` in the digest channel."
        return None

    def _digest_period_for(self, *, kind: str, timezone_name: str, now: datetime) -> QuestionDropDigestPeriod:
        tzinfo = load_afk_timezone(timezone_name) or timezone.utc
        local_now = now.astimezone(tzinfo)
        if kind == "weekly":
            current_week_start_date = local_now.date() - timedelta(days=local_now.weekday())
            period_end_local = datetime.combine(current_week_start_date, time(hour=0, minute=0, tzinfo=tzinfo))
            period_start_local = period_end_local - timedelta(days=7)
            scheduled_post_at = period_end_local + timedelta(hours=QUESTION_DROP_DIGEST_POST_HOUR)
            period_key = f"weekly:{period_start_local.date().isoformat()}"
        else:
            current_month_start_date = date(local_now.year, local_now.month, 1)
            period_end_local = datetime.combine(current_month_start_date, time(hour=0, minute=0, tzinfo=tzinfo))
            if current_month_start_date.month == 1:
                period_start_date = date(current_month_start_date.year - 1, 12, 1)
            else:
                period_start_date = date(current_month_start_date.year, current_month_start_date.month - 1, 1)
            period_start_local = datetime.combine(period_start_date, time(hour=0, minute=0, tzinfo=tzinfo))
            scheduled_post_at = period_end_local + timedelta(hours=QUESTION_DROP_DIGEST_POST_HOUR)
            period_key = f"monthly:{period_start_local.year:04d}-{period_start_local.month:02d}"
        return QuestionDropDigestPeriod(
            kind=kind,
            period_key=period_key,
            period_start_at=period_start_local.astimezone(timezone.utc),
            period_end_at=period_end_local.astimezone(timezone.utc),
            scheduled_post_at=scheduled_post_at.astimezone(timezone.utc),
            timezone_name=timezone_name,
        )

    def _next_digest_post_at(self, *, kind: str, timezone_name: str, now: datetime) -> datetime:
        tzinfo = load_afk_timezone(timezone_name) or timezone.utc
        local_now = now.astimezone(tzinfo)
        if kind == "weekly":
            current_week_start_date = local_now.date() - timedelta(days=local_now.weekday())
            candidate = datetime.combine(current_week_start_date, time(hour=QUESTION_DROP_DIGEST_POST_HOUR, tzinfo=tzinfo))
            if local_now >= candidate:
                candidate += timedelta(days=7)
            return candidate.astimezone(timezone.utc)
        month_start = datetime.combine(date(local_now.year, local_now.month, 1), time(hour=QUESTION_DROP_DIGEST_POST_HOUR, tzinfo=tzinfo))
        if local_now >= month_start:
            year = local_now.year + (1 if local_now.month == 12 else 0)
            month = 1 if local_now.month == 12 else local_now.month + 1
            month_start = datetime.combine(date(year, month, 1), time(hour=QUESTION_DROP_DIGEST_POST_HOUR, tzinfo=tzinfo))
        return month_start.astimezone(timezone.utc)

    def _digest_period_window_label(self, period: QuestionDropDigestPeriod) -> str:
        tzinfo = load_afk_timezone(period.timezone_name) or timezone.utc
        local_start = period.period_start_at.astimezone(tzinfo)
        local_end = period.period_end_at.astimezone(tzinfo)
        if period.kind == "weekly":
            final_day = local_end - timedelta(days=1)
            return f"{local_start:%b} {local_start.day} to {final_day:%b} {final_day.day}"
        return f"{local_start:%B %Y}"

    def _format_digest_run_summary(self, run: dict[str, Any] | None) -> str:
        if not isinstance(run, dict):
            return "No run yet."
        status = str(run.get("status") or "").strip().title() or "Unknown"
        detail = str(run.get("detail") or "").strip()
        completed_at = run.get("completed_at")
        if isinstance(completed_at, str):
            completed_at = datetime.fromisoformat(completed_at)
            if completed_at.tzinfo is None:
                completed_at = completed_at.replace(tzinfo=timezone.utc)
        completed_text = ge.format_timestamp(completed_at, "R") if completed_at is not None else ""
        summary = status
        if detail:
            summary += f" - {detail}"
        if completed_text:
            summary += f" ({completed_text})"
        return summary

    async def update_config(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        drops_per_day: int | None = None,
        timezone_name: str | None = None,
        answer_window_seconds: int | None = None,
        tone_mode: str | None = None,
        difficulty_profile: str | None = None,
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
            if difficulty_profile is not None:
                current["difficulty_profile"] = str(difficulty_profile).strip().casefold()
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

    async def update_digest_config(
        self,
        guild: discord.Guild | None,
        *,
        weekly_enabled: bool | None = None,
        monthly_enabled: bool | None = None,
        timezone_name: str | None = None,
        shared_channel_id: int | None = None,
        weekly_channel_id: int | None = None,
        monthly_channel_id: int | None = None,
        skip_low_activity: bool | None = None,
        mention_mode: str | None = None,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        guild_id = int(getattr(guild, "id", 0) or 0)
        if guild_id <= 0:
            return False, "This update needs a server context."
        async with self._lock:
            current = dict(self.get_config(guild_id))
            raw_digest = current.get("digest_settings", {})
            digest = default_question_drop_digest_settings()
            if isinstance(raw_digest, dict):
                digest.update(raw_digest)
            if weekly_enabled is not None:
                digest["weekly_enabled"] = bool(weekly_enabled)
            if monthly_enabled is not None:
                digest["monthly_enabled"] = bool(monthly_enabled)
            if timezone_name is not None:
                timezone_text = str(timezone_name).strip()
                if timezone_text.casefold() in {"utc", "z"}:
                    ok, canonical, error = True, "UTC", None
                else:
                    ok, canonical, error = canonicalize_afk_timezone(timezone_text)
                if not ok or canonical is None:
                    return False, error or "Use a valid timezone like `Asia/Yerevan` or `UTC+04:00`."
                digest["timezone"] = canonical
            if shared_channel_id is not None:
                normalized_channel_id = int(shared_channel_id) if int(shared_channel_id) > 0 else None
                digest["weekly_channel_id"] = normalized_channel_id
                digest["monthly_channel_id"] = normalized_channel_id
            if weekly_channel_id is not None:
                digest["weekly_channel_id"] = int(weekly_channel_id) if int(weekly_channel_id) > 0 else None
            if monthly_channel_id is not None:
                digest["monthly_channel_id"] = int(monthly_channel_id) if int(monthly_channel_id) > 0 else None
            if skip_low_activity is not None:
                digest["skip_low_activity"] = bool(skip_low_activity)
            if mention_mode is not None:
                normalized_mention_mode = str(mention_mode).strip().casefold()
                if normalized_mention_mode not in QUESTION_DROP_DIGEST_MENTION_MODES:
                    return False, "Mention mode must be `none` or `here`."
                digest["mention_mode"] = normalized_mention_mode
            current["digest_settings"] = digest
            normalized = normalize_question_drops_config(guild_id, current)
            effective = self._effective_digest_settings(normalized)
            for kind in QUESTION_DROP_DIGEST_KINDS:
                if not effective.get(f"{kind}_enabled"):
                    continue
                channel_id = effective.get(f"{kind}_channel_id")
                issue = self._digest_channel_issue(guild, channel_id, label=f"{kind.title()} digest")
                if issue is not None:
                    return False, issue
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        self._wake_event.set()
        return True, "Knowledge digest settings updated."

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

    def _announcement_placeholder_tokens(self, scope_type: str) -> tuple[str, ...]:
        if str(scope_type).strip().casefold() == "category":
            return QUESTION_DROP_CATEGORY_ANNOUNCEMENT_PLACEHOLDERS
        return QUESTION_DROP_SHARED_ANNOUNCEMENT_PLACEHOLDERS

    def _announcement_template_label(self, *, scope_type: str, scope_key: str | None = None) -> str:
        if str(scope_type).strip().casefold() == "scholar":
            return "Scholar announcement"
        return f"{category_label(str(scope_key or ''))} mastery announcement"

    def _announcement_title(self, *, scope_type: str, scope_key: str | None = None, tier: int | None = None) -> str:
        normalized_scope_type = str(scope_type).strip().casefold()
        normalized_scope_key = str(scope_key or "").strip().casefold()
        if tier in QUESTION_DROP_MASTERY_TIERS:
            if normalized_scope_type == "scholar":
                return f"{scholar_label(int(tier))} Announcement"
            return f"{category_label(normalized_scope_key)} {tier_label(int(tier))} Announcement"
        if normalized_scope_type == "scholar":
            return "Scholar Announcement"
        return f"{category_label(normalized_scope_key)} Mastery Announcement"

    def _announcement_target_label(self, *, scope_type: str, scope_key: str | None = None, tier: int | None = None) -> str:
        normalized_scope_type = str(scope_type).strip().casefold()
        normalized_scope_key = str(scope_key or "").strip().casefold()
        if tier in QUESTION_DROP_MASTERY_TIERS:
            if normalized_scope_type == "scholar":
                return f"{scholar_label(int(tier))} override"
            return f"{category_label(normalized_scope_key)} {tier_label(int(tier))} override"
        if normalized_scope_type == "scholar":
            return "Scholar default"
        return f"{category_label(normalized_scope_key)} default"

    def _announcement_source_label(self, source: str) -> str:
        normalized_source = str(source or "").strip().casefold()
        if normalized_source == "tier_override":
            return "Tier override"
        if normalized_source == "scope_default":
            return "Scope default"
        return "Babblebox default"

    def _announcement_scope_config(self, config: dict[str, Any], *, scope_type: str, scope_key: str | None = None) -> dict[str, Any]:
        if str(scope_type).strip().casefold() == "scholar":
            scholar = config.get("scholar_ladder", {})
            return dict(scholar) if isinstance(scholar, dict) else {}
        return self._configured_category_mastery(config, str(scope_key or "").strip().casefold())

    def _announcement_scope_headline(self, *, scope_type: str, scope_key: str | None = None, tier: int) -> str:
        if str(scope_type).strip().casefold() == "scholar":
            return scholar_label(tier)
        return f"{category_label(str(scope_key or ''))} {tier_label(tier)}"

    def _normalize_announcement_template(self, value: Any, *, scope_type: str) -> str | None:
        allowed = set(self._announcement_placeholder_tokens(scope_type))
        ok, cleaned, _error = sanitize_short_plain_template(
            value,
            field_name="Announcement template",
            max_length=QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_MAX_LENGTH,
            allowed_placeholders=allowed,
            sentence_limit=QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_SENTENCE_LIMIT,
        )
        return cleaned if ok else None

    def _configured_announcement_template(self, payload: dict[str, Any], *, scope_type: str) -> str | None:
        raw = payload.get("announcement_template") if isinstance(payload, dict) else None
        return self._normalize_announcement_template(raw, scope_type=scope_type)

    def _configured_tier(self, payload: dict[str, Any], *, tier: int | None) -> dict[str, Any]:
        if tier not in QUESTION_DROP_MASTERY_TIERS:
            return {}
        for item in self._configured_tiers(payload):
            if int(item.get("tier", 0) or 0) == int(tier):
                return dict(item)
        return {}

    def _configured_tier_announcement_template(self, payload: dict[str, Any], *, scope_type: str, tier: int | None) -> str | None:
        tier_payload = self._configured_tier(payload, tier=tier)
        raw = tier_payload.get("announcement_template") if isinstance(tier_payload, dict) else None
        return self._normalize_announcement_template(raw, scope_type=scope_type)

    def _active_tier_template_overrides(self, payload: dict[str, Any], *, scope_type: str) -> tuple[int, ...]:
        overrides: list[int] = []
        for item in self._configured_tiers(payload):
            tier = int(item.get("tier", 0) or 0)
            if tier not in QUESTION_DROP_MASTERY_TIERS:
                continue
            if self._configured_tier_announcement_template(payload, scope_type=scope_type, tier=tier) is not None:
                overrides.append(tier)
        return tuple(sorted(overrides))

    def _effective_announcement_template(
        self,
        payload: dict[str, Any],
        *,
        scope_type: str,
        tier: int | None = None,
    ) -> tuple[str | None, str]:
        tier_template = self._configured_tier_announcement_template(payload, scope_type=scope_type, tier=tier)
        if tier_template is not None:
            return tier_template, "tier_override"
        scope_template = self._configured_announcement_template(payload, scope_type=scope_type)
        if scope_template is not None:
            return scope_template, "scope_default"
        return None, "babblebox_default"

    def _override_summary_labels(self, *, scope_type: str, tiers: tuple[int, ...]) -> tuple[str, ...]:
        labels: list[str] = []
        normalized_scope_type = str(scope_type).strip().casefold()
        for tier in tiers:
            if normalized_scope_type == "scholar":
                labels.append(scholar_label(int(tier)))
            else:
                labels.append(tier_label(int(tier)))
        return tuple(labels)

    def _sanitize_announcement_value(self, value: Any, *, fallback: str) -> str:
        cleaned = normalize_plain_text(str(value or ""))
        if not cleaned:
            return fallback
        if contains_blocklisted_term(cleaned):
            return fallback
        escaped = discord.utils.escape_mentions(discord.utils.escape_markdown(cleaned))
        if find_private_pattern(escaped) is not None:
            return fallback
        return escaped

    def _announcement_template_values(
        self,
        *,
        event: dict[str, Any],
        ping_user: bool,
    ) -> dict[str, str]:
        member = event.get("member")
        role = event.get("role")
        scope_type = str(event.get("scope_type", "")).strip().casefold()
        user_display = ge.display_name_of(member) if member is not None else f"User {int(event.get('user_id', 0) or 0)}"
        user_name = getattr(member, "name", None) or user_display
        user_mention = getattr(member, "mention", None) or f"<@{int(event.get('user_id', 0) or 0)}>"
        if ping_user:
            rendered_user_mention = user_mention
        else:
            rendered_user_mention = self._sanitize_announcement_value(user_display, fallback="this member")
        values = {
            "{user.mention}": rendered_user_mention,
            "{user.name}": self._sanitize_announcement_value(user_name, fallback="this member"),
            "{user.display_name}": self._sanitize_announcement_value(user_display, fallback="this member"),
            "{role.name}": self._sanitize_announcement_value(getattr(role, "name", None), fallback="this role"),
            "{tier.label}": self._sanitize_announcement_value(
                scholar_label(int(event.get("tier", 0) or 0)) if scope_type == "scholar" else tier_label(int(event.get("tier", 0) or 0)),
                fallback="this tier",
            ),
            "{threshold}": str(int(event.get("threshold", 0) or 0)),
        }
        if scope_type == "category":
            values["{category.name}"] = self._sanitize_announcement_value(
                category_label(str(event.get("scope_key", "") or "")),
                fallback="this category",
            )
        return values

    def _render_custom_announcement_template(self, event: dict[str, Any], *, ping_user: bool) -> str | None:
        template = self._configured_announcement_template(event, scope_type=str(event.get("scope_type", "")))
        if template is None:
            return None
        rendered = template
        for token, value in self._announcement_template_values(event=event, ping_user=ping_user).items():
            rendered = rendered.replace(token, value)
        return rendered

    def _default_role_grant_description(self, event: dict[str, Any], *, ping_user: bool) -> str:
        member_token = (
            event.get("member_mention")
            if ping_user
            else self._sanitize_announcement_value(ge.display_name_of(event.get("member")), fallback="this member")
        )
        role_token = f"<@&{int(event['role_id'])}>"
        if not ping_user:
            role_token = self._sanitize_announcement_value(getattr(event.get("role"), "name", None), fallback="this role")
        description = (
            f"{progression_emoji('role')} {member_token} earned {role_token} "
            f"for {event['scope_label']} at **{event['threshold']}** points."
        )
        if event.get("tier") == 3:
            description += f" {progression_emoji('mastery')} Top tier secured."
        return description

    def _build_role_grant_embed(self, event: dict[str, Any], *, compact: bool = False) -> discord.Embed:
        description = self._default_role_grant_description(event, ping_user=not compact)
        if compact:
            description = "Babblebox mastery role granted."
            if int(event.get("tier", 0) or 0) >= 3:
                description = "Babblebox mastery role granted. Top tier secured."
        return ge.make_status_embed(
            f"{event['headline']} Unlocked",
            description,
            tone="success",
            footer="Babblebox Question Drops",
        )

    def _build_announcement_preview(self, payload: dict[str, Any]) -> str:
        event = dict(payload.get("sample_event", {})) if isinstance(payload.get("sample_event"), dict) else {}
        event["announcement_template"] = payload.get("effective_announcement_template")
        if payload.get("effective_source") != "babblebox_default":
            preview = self._render_custom_announcement_template(event, ping_user=False)
            if preview is not None:
                return preview
        return self._default_role_grant_description(event, ping_user=False)

    async def _mastery_announcement_status_payload(
        self,
        guild: discord.Guild,
        *,
        scope_type: str,
        scope_key: str | None = None,
        tier: int | None = None,
    ) -> dict[str, Any]:
        config = self.get_config(guild.id)
        payload = self._announcement_scope_config(config, scope_type=scope_type, scope_key=scope_key)
        normalized_scope_type = str(scope_type).strip().casefold()
        normalized_scope_key = str(scope_key or "").strip().casefold() if normalized_scope_type == "category" else "global"
        if tier is not None and tier not in QUESTION_DROP_MASTERY_TIERS:
            return {
                "status": "unknown_tier",
                "title": self._announcement_title(scope_type=normalized_scope_type, scope_key=normalized_scope_key),
                "scope_label": self._announcement_template_label(scope_type=normalized_scope_type, scope_key=normalized_scope_key),
            }
        tiers = sorted(self._configured_tiers(payload), key=lambda item: int(item.get("tier", 0) or 0))
        selected_tier = self._configured_tier(payload, tier=tier) if tier is not None else {}
        sample_tier = selected_tier or next(
            (item for item in tiers if int(item.get("tier", 0) or 0) > 0),
            {"tier": int(tier or 1), "threshold": 25, "role_id": None, "announcement_template": None},
        )
        role_id = int(sample_tier.get("role_id", 0) or 0) or None
        role = guild.get_role(role_id) if role_id is not None and hasattr(guild, "get_role") else None
        headline = self._announcement_scope_headline(scope_type=normalized_scope_type, scope_key=normalized_scope_key, tier=int(sample_tier.get("tier", 1) or 1))
        sample_event = {
            "scope_type": normalized_scope_type,
            "scope_key": normalized_scope_key,
            "scope_label": "the scholar ladder" if normalized_scope_type == "scholar" else category_label_with_emoji(normalized_scope_key),
            "tier": int(sample_tier.get("tier", 1) or 1),
            "threshold": int(sample_tier.get("threshold", 25) or 25),
            "role_id": int(role_id or 0),
            "role": role or type("PreviewRole", (), {"name": headline})(),
            "member": type("PreviewMember", (), {"display_name": "Ava", "name": "Ava", "mention": "<@123>"})(),
            "member_mention": "<@123>",
            "user_id": 123,
            "headline": headline,
            "announcement_channel_id": payload.get("announcement_channel_id"),
        }
        target_template = (
            self._configured_tier_announcement_template(payload, scope_type=normalized_scope_type, tier=tier)
            if tier is not None
            else self._configured_announcement_template(payload, scope_type=normalized_scope_type)
        )
        effective_template, effective_source = self._effective_announcement_template(
            payload,
            scope_type=normalized_scope_type,
            tier=tier,
        )
        active_override_tiers = self._active_tier_template_overrides(payload, scope_type=normalized_scope_type)
        other_override_tiers = tuple(value for value in active_override_tiers if value != tier) if tier is not None else active_override_tiers
        status_payload = {
            "scope_type": normalized_scope_type,
            "scope_key": normalized_scope_key,
            "target_tier": int(tier) if tier is not None else None,
            "target_kind": "tier" if tier is not None else "scope",
            "target_label": self._announcement_target_label(scope_type=normalized_scope_type, scope_key=normalized_scope_key, tier=tier),
            "scope_label": self._announcement_template_label(scope_type=normalized_scope_type, scope_key=normalized_scope_key),
            "title": self._announcement_title(scope_type=normalized_scope_type, scope_key=normalized_scope_key, tier=tier),
            "announcement_channel_id": payload.get("announcement_channel_id"),
            "announcement_issue": self._announcement_channel_issue(
                guild,
                payload.get("announcement_channel_id"),
                label=self._announcement_template_label(scope_type=normalized_scope_type, scope_key=normalized_scope_key),
            ),
            "silent_grant": bool(payload.get("silent_grant")),
            "announcement_template": target_template,
            "has_target_template": target_template is not None,
            "has_custom_template": effective_source != "babblebox_default",
            "effective_announcement_template": effective_template,
            "effective_source": effective_source,
            "effective_source_label": self._announcement_source_label(effective_source),
            "placeholder_tokens": self._announcement_placeholder_tokens(normalized_scope_type),
            "tier_override_count": len(active_override_tiers),
            "other_tier_overrides": other_override_tiers,
            "other_tier_override_labels": self._override_summary_labels(scope_type=normalized_scope_type, tiers=other_override_tiers),
            "sample_event": sample_event,
        }
        status_payload["preview"] = self._build_announcement_preview(status_payload)
        return status_payload

    async def get_category_mastery_announcement_status(
        self,
        guild: discord.Guild,
        *,
        category: str,
        tier: int | None = None,
    ) -> dict[str, Any]:
        normalized_category = str(category or "").strip().casefold()
        if normalized_category not in QUESTION_DROP_CATEGORIES:
            return {
                "status": "unknown_category",
                "title": "Category Mastery Announcement",
                "scope_label": "Category mastery announcement",
            }
        payload = await self._mastery_announcement_status_payload(
            guild,
            scope_type="category",
            scope_key=normalized_category,
            tier=tier,
        )
        payload.setdefault("status", "ok")
        return payload

    async def get_scholar_announcement_status(self, guild: discord.Guild, *, tier: int | None = None) -> dict[str, Any]:
        payload = await self._mastery_announcement_status_payload(guild, scope_type="scholar", scope_key="global", tier=tier)
        payload.setdefault("status", "ok")
        return payload

    def build_mastery_announcement_status_embed(self, payload: dict[str, Any], *, note: str | None = None, success: bool = False) -> discord.Embed:
        title = str(payload.get("title") or "Mastery Announcement")
        if payload.get("status") == "unknown_category":
            return ge.make_status_embed(title, "Unknown category.", tone="warning", footer="Babblebox Question Drops")
        if payload.get("status") == "unknown_tier":
            return ge.make_status_embed(title, "Tier must be 1, 2, or 3.", tone="warning", footer="Babblebox Question Drops")
        description = f"Current source: **{payload.get('effective_source_label', 'Babblebox default')}**."
        if note:
            description = f"{note} {description}"
        embed = discord.Embed(title=title, description=description, color=ge.EMBED_THEME["accent"])
        channel_id = payload.get("announcement_channel_id")
        channel_text = f"<#{int(channel_id)}>" if isinstance(channel_id, int) else "Not set"
        embed.add_field(
            name="Announcement",
            value=(
                f"Target: **{payload.get('target_label', 'Default')}**\n"
                f"In effect: **{payload.get('effective_source_label', 'Babblebox default')}**\n"
                f"Channel: **{channel_text}**\n"
                f"Silent grant: **{'On' if payload.get('silent_grant') else 'Off'}**"
            ),
            inline=False,
        )
        configured_copy = payload.get("announcement_template")
        if configured_copy is None:
            if payload.get("target_kind") == "tier":
                if payload.get("effective_source") == "scope_default":
                    configured_copy = "No tier override saved here. This tier is currently using the scope default template."
                else:
                    configured_copy = "No tier override saved here. This tier is currently using Babblebox's built-in mastery announcement."
            else:
                configured_copy = "Using Babblebox's built-in mastery announcement."
        embed.add_field(name="Configured Copy", value=ge.safe_field_text(str(configured_copy), limit=1024), inline=False)
        embed.add_field(
            name="Placeholders",
            value=" ".join(f"`{token}`" for token in payload.get("placeholder_tokens", ())),
            inline=False,
        )
        embed.add_field(name="Preview", value=ge.safe_field_text(str(payload.get("preview") or "No preview available."), limit=1024), inline=False)
        if payload.get("other_tier_override_labels"):
            field_name = "Other Overrides" if payload.get("target_kind") == "tier" else "Tier Overrides"
            embed.add_field(
                name=field_name,
                value=", ".join(str(label) for label in payload.get("other_tier_override_labels", ())),
                inline=False,
            )
        if payload.get("announcement_issue") is not None:
            embed.add_field(name="Channel Check", value=str(payload["announcement_issue"]), inline=False)
        return ge.style_embed(embed, footer="Babblebox Question Drops | Compact, offline, channel-safe")

    async def _set_mastery_announcement_template(
        self,
        guild_id: int,
        *,
        scope_type: str,
        scope_key: str | None = None,
        tier: int | None = None,
        template: str | None = None,
        clear: bool = False,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        normalized_scope_type = str(scope_type or "").strip().casefold()
        normalized_scope_key = str(scope_key or "").strip().casefold()
        if normalized_scope_type == "category" and normalized_scope_key not in QUESTION_DROP_CATEGORIES:
            return False, f"Unknown category. Choose from {', '.join(QUESTION_DROP_CATEGORIES)}."
        if tier is not None and tier not in QUESTION_DROP_MASTERY_TIERS:
            return False, "Tier must be 1, 2, or 3."
        if not clear:
            ok, cleaned, error = sanitize_short_plain_template(
                template,
                field_name="Announcement template",
                max_length=QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_MAX_LENGTH,
                allowed_placeholders=set(self._announcement_placeholder_tokens(normalized_scope_type)),
                sentence_limit=QUESTION_DROP_ANNOUNCEMENT_TEMPLATE_SENTENCE_LIMIT,
            )
            if not ok:
                return False, str(error)
            template = cleaned
        async with self._lock:
            config = dict(self.get_config(guild_id))
            if normalized_scope_type == "scholar":
                scholar = dict(config.get("scholar_ladder", {}))
                if tier is None:
                    scholar["announcement_template"] = None if clear else template
                else:
                    tiers = [dict(item) for item in scholar.get("tiers", [])]
                    for item in tiers:
                        if int(item.get("tier", 0) or 0) == int(tier):
                            item["announcement_template"] = None if clear else template
                            break
                    scholar["tiers"] = tiers
                config["scholar_ladder"] = scholar
            else:
                mastery = dict(config.get("category_mastery", {}))
                category_config = dict(mastery.get(normalized_scope_key, {}))
                if tier is None:
                    category_config["announcement_template"] = None if clear else template
                else:
                    tiers = [dict(item) for item in category_config.get("tiers", [])]
                    for item in tiers:
                        if int(item.get("tier", 0) or 0) == int(tier):
                            item["announcement_template"] = None if clear else template
                            break
                    category_config["tiers"] = tiers
                mastery[normalized_scope_key] = category_config
                config["category_mastery"] = mastery
            normalized = normalize_question_drops_config(guild_id, config)
            await self.store.upsert_config(normalized)
            self._configs[guild_id] = normalized
        if clear:
            if tier in QUESTION_DROP_MASTERY_TIERS and normalized_scope_type == "scholar":
                return True, f"{scholar_label(int(tier))} announcement override cleared."
            if tier in QUESTION_DROP_MASTERY_TIERS:
                return True, f"{category_label(normalized_scope_key)} {tier_label(int(tier))} announcement override cleared."
            if normalized_scope_type == "scholar":
                return True, "Scholar announcement template cleared."
            return True, f"{category_label(normalized_scope_key)} mastery announcement template cleared."
        if tier in QUESTION_DROP_MASTERY_TIERS and normalized_scope_type == "scholar":
            return True, f"{scholar_label(int(tier))} announcement override saved."
        if tier in QUESTION_DROP_MASTERY_TIERS:
            return True, f"{category_label(normalized_scope_key)} {tier_label(int(tier))} announcement override saved."
        if normalized_scope_type == "scholar":
            return True, "Scholar announcement template saved."
        return True, f"{category_label(normalized_scope_key)} mastery announcement template saved."

    async def save_category_mastery_announcement_template(
        self,
        guild_id: int,
        *,
        category: str,
        template: str,
        tier: int | None = None,
    ) -> tuple[bool, str]:
        return await self._set_mastery_announcement_template(
            guild_id,
            scope_type="category",
            scope_key=category,
            tier=tier,
            template=template,
        )

    async def clear_category_mastery_announcement_template(
        self,
        guild_id: int,
        *,
        category: str,
        tier: int | None = None,
    ) -> tuple[bool, str]:
        return await self._set_mastery_announcement_template(
            guild_id,
            scope_type="category",
            scope_key=category,
            tier=tier,
            clear=True,
        )

    async def save_scholar_announcement_template(
        self,
        guild_id: int,
        *,
        template: str,
        tier: int | None = None,
    ) -> tuple[bool, str]:
        return await self._set_mastery_announcement_template(
            guild_id,
            scope_type="scholar",
            scope_key="global",
            tier=tier,
            template=template,
        )

    async def clear_scholar_announcement_template(self, guild_id: int, *, tier: int | None = None) -> tuple[bool, str]:
        return await self._set_mastery_announcement_template(
            guild_id,
            scope_type="scholar",
            scope_key="global",
            tier=tier,
            clear=True,
        )

    def _default_role_grant_preference(self, *, guild_id: int, user_id: int) -> dict[str, Any]:
        return {
            "guild_id": int(guild_id),
            "user_id": int(user_id),
            "role_grants_enabled": True,
            "opted_out_at": None,
        }

    async def _member_role_grant_preference(self, *, guild_id: int, user_id: int) -> dict[str, Any]:
        default_value = self._default_role_grant_preference(guild_id=guild_id, user_id=user_id)
        profile_service = self._profile_service()
        if profile_service is None:
            return default_value
        getter = getattr(profile_service, "get_question_drop_role_preference", None)
        if not callable(getter):
            return default_value
        payload = await getter(user_id, guild_id=guild_id)
        return dict(payload) if isinstance(payload, dict) else default_value

    async def _set_member_role_grants_enabled(self, *, guild_id: int, user_id: int, enabled: bool) -> dict[str, Any]:
        default_value = self._default_role_grant_preference(guild_id=guild_id, user_id=user_id)
        profile_service = self._profile_service()
        if profile_service is None:
            return default_value
        setter = getattr(profile_service, "set_question_drop_role_grants_enabled", None)
        if not callable(setter):
            return default_value
        payload = await setter(user_id, guild_id=guild_id, enabled=enabled)
        return dict(payload) if isinstance(payload, dict) else default_value

    def _managed_role_records(self, guild: discord.Guild, *, enabled_only: bool = False) -> list[dict[str, Any]]:
        config = self.get_config(guild.id)
        records_by_role_id: dict[int, dict[str, Any]] = {}
        category_mastery = config.get("category_mastery", {})
        if isinstance(category_mastery, dict):
            for category_id, payload in category_mastery.items():
                if not isinstance(payload, dict):
                    continue
                if enabled_only and not payload.get("enabled"):
                    continue
                for tier_config in self._configured_tiers(payload):
                    role_id = int(tier_config.get("role_id", 0) or 0)
                    if role_id <= 0:
                        continue
                    record = records_by_role_id.setdefault(
                        role_id,
                        {
                            "role_id": role_id,
                            "role": guild.get_role(role_id) if hasattr(guild, "get_role") else None,
                            "scopes": [],
                        },
                    )
                    record["scopes"].append(
                        {
                            "scope_type": "category",
                            "scope_key": str(category_id).strip().casefold(),
                            "tier": int(tier_config.get("tier", 0) or 0),
                        }
                    )
        scholar_payload = config.get("scholar_ladder", {})
        if isinstance(scholar_payload, dict) and (not enabled_only or scholar_payload.get("enabled")):
            for tier_config in self._configured_tiers(scholar_payload):
                role_id = int(tier_config.get("role_id", 0) or 0)
                if role_id <= 0:
                    continue
                record = records_by_role_id.setdefault(
                    role_id,
                    {
                        "role_id": role_id,
                        "role": guild.get_role(role_id) if hasattr(guild, "get_role") else None,
                        "scopes": [],
                    },
                )
                record["scopes"].append(
                    {
                        "scope_type": "scholar",
                        "scope_key": "global",
                        "tier": int(tier_config.get("tier", 0) or 0),
                    }
                )
        records = list(records_by_role_id.values())
        for record in records:
            record["scopes"].sort(
                key=lambda item: (
                    str(item.get("scope_type", "")),
                    str(item.get("scope_key", "")),
                    int(item.get("tier", 0) or 0),
                )
            )
        records.sort(
            key=lambda record: (
                -int(getattr(record.get("role"), "position", 0) or 0),
                str(getattr(record.get("role"), "name", "")),
                int(record.get("role_id", 0) or 0),
            )
        )
        return records

    def _scope_label_for_managed_role(self, scope: dict[str, Any]) -> str:
        scope_type = str(scope.get("scope_type", "")).casefold()
        tier = int(scope.get("tier", 0) or 0)
        if scope_type == "scholar":
            return scholar_label(tier)
        return f"{category_label(str(scope.get('scope_key', '')))} {tier_label(tier)}"

    def _role_display(self, role, role_id: int) -> str:
        mention = getattr(role, "mention", None)
        if isinstance(mention, str) and mention:
            return mention
        name = getattr(role, "name", None)
        if isinstance(name, str) and name:
            return f"`{name}`"
        return f"<@&{int(role_id)}>"

    def _managed_role_line(self, record: dict[str, Any]) -> str:
        role = record.get("role")
        role_id = int(record.get("role_id", 0) or 0)
        scope_labels = [self._scope_label_for_managed_role(scope) for scope in record.get("scopes", [])]
        suffix = f" | {', '.join(scope_labels)}" if scope_labels else ""
        return f"{self._role_display(role, role_id)}{suffix}"

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

    def _eligible_unlocks_for_summary(self, summary: dict[str, Any]) -> list[dict[str, Any]]:
        guild_id = int(summary.get("guild_id", 0) or 0)
        if guild_id <= 0:
            return []
        config = self.get_config(guild_id)
        unlocks = summary.get("guild_unlocks") or []
        eligible: list[dict[str, Any]] = []
        guild_profile = summary.get("guild_profile") or {}
        scholar_config = dict(config.get("scholar_ladder", {}))
        if scholar_config.get("enabled"):
            scholar_points = int(guild_profile.get("points", 0) or 0)
            for tier_config in self._configured_unlock_tiers(scholar_config):
                tier = int(tier_config.get("tier", 0) or 0)
                role_id = int(tier_config.get("role_id", 0) or 0)
                threshold = int(tier_config.get("threshold", 0) or 0)
                if scholar_points < threshold:
                    continue
                eligible.append(
                    {
                        "scope_type": "scholar",
                        "scope_key": "global",
                        "scope_label": "the scholar ladder",
                        "tier": tier,
                        "threshold": threshold,
                        "role_id": role_id,
                        "unlocked": self._unlock_exists(
                            unlocks,
                            scope_type="scholar",
                            scope_key="global",
                            tier=tier,
                            role_id=role_id,
                        ),
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
                if category_points < threshold:
                    continue
                eligible.append(
                    {
                        "scope_type": "category",
                        "scope_key": category_id,
                        "scope_label": category_label_with_emoji(category_id),
                        "tier": tier,
                        "threshold": threshold,
                        "role_id": role_id,
                        "unlocked": self._unlock_exists(
                            unlocks,
                            scope_type="category",
                            scope_key=category_id,
                            tier=tier,
                            role_id=role_id,
                        ),
                    }
                )
        eligible.sort(
            key=lambda item: (
                str(item.get("scope_type", "")),
                str(item.get("scope_key", "")),
                int(item.get("tier", 0) or 0),
                int(item.get("role_id", 0) or 0),
            )
        )
        return eligible

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
        del fallback_channel
        channel = await self._resolve_announcement_channel(guild, event.get("announcement_channel_id"))
        if channel is None:
            return
        try:
            custom_content = self._render_custom_announcement_template(event, ping_user=True)
            if custom_content is not None:
                await channel.send(
                    content=custom_content,
                    embed=self._build_role_grant_embed(event, compact=True),
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
                return
            await channel.send(
                embed=self._build_role_grant_embed(event, compact=False),
                allowed_mentions=discord.AllowedMentions(users=True, roles=True, everyone=False),
            )
        except discord.HTTPException:
            return

    def _resolve_user_label(self, user_id: int) -> str:
        get_user = getattr(self.bot, "get_user", None)
        cached = get_user(user_id) if callable(get_user) else None
        if cached is not None:
            return ge.display_name_of(cached)
        return f"User {user_id}"

    def _resolve_guild_user_label(self, guild: discord.Guild, user_id: int) -> str:
        get_member = getattr(guild, "get_member", None)
        member = get_member(user_id) if callable(get_member) else None
        if member is not None:
            return ge.display_name_of(member)
        return self._resolve_user_label(user_id)

    async def _build_digest_status_snapshot(self, guild: discord.Guild, config: dict[str, Any]) -> dict[str, Any]:
        settings = self._effective_digest_settings(config)
        now = ge.now_utc()
        cadence: dict[str, Any] = {}
        issues: list[str] = []
        latest_runs: dict[str, Any] = {}
        for kind in QUESTION_DROP_DIGEST_KINDS:
            channel_id = settings.get(f"{kind}_channel_id")
            latest_runs[kind] = await self.store.fetch_latest_digest_run(guild.id, digest_kind=kind)
            if settings.get(f"{kind}_enabled"):
                issue = self._digest_channel_issue(guild, channel_id, label=f"{kind.title()} digest")
                if issue is not None:
                    issues.append(issue)
            cadence[kind] = {
                "enabled": bool(settings.get(f"{kind}_enabled")),
                "channel_id": channel_id,
                "next_post_at": self._next_digest_post_at(kind=kind, timezone_name=settings["timezone"], now=now),
                "last_run": latest_runs[kind],
            }
        return {
            "settings": settings,
            "cadence": cadence,
            "issues": issues,
            "shared_channel": bool(
                isinstance(settings.get("weekly_channel_id"), int)
                and settings.get("weekly_channel_id") == settings.get("monthly_channel_id")
            ),
        }

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
            digest_status=await self._build_digest_status_snapshot(guild, config),
        )

    def build_status_embed(self, guild: discord.Guild, snapshot: QuestionDropStatusSnapshot) -> discord.Embed:
        config = snapshot.config
        digest_status = snapshot.digest_status
        digest_settings = digest_status.get("settings", {}) if isinstance(digest_status, dict) else {}
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
        operability_lines.extend(digest_status.get("issues", []) if isinstance(digest_status, dict) else [])
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
            elif isinstance(category_mastery.get("announcement_channel_id"), int):
                flags.append(f"announce <#{int(category_mastery['announcement_channel_id'])}>")
            else:
                flags.append("announce off")
            flags.append(
                "custom default"
                if self._configured_announcement_template(category_mastery, scope_type="category") is not None
                else "default copy"
            )
            override_count = len(self._active_tier_template_overrides(category_mastery, scope_type="category"))
            if override_count > 0:
                label = "tier override" if override_count == 1 else "tier overrides"
                flags.append(f"{override_count} {label}")
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
                f"Activity gate: **{str(config.get('activity_gate', 'light')).title()}**\n"
                f"Profile: **{str(config.get('difficulty_profile', 'standard')).title()}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Tone: **{str(config.get('tone_mode', 'clean')).title()}**\n"
                f"Channels: **{len(snapshot.enabled_channel_mentions)}**\n"
                f"Live now: **{snapshot.active_drop_count}**\n"
                f"Mix: **{QUESTION_DROP_DIFFICULTY_PROFILE_LABELS.get(str(config.get('difficulty_profile', 'standard')).casefold(), QUESTION_DROP_DIFFICULTY_PROFILE_LABELS['standard'])}**"
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
        weekly_channel_id = digest_settings.get("weekly_channel_id")
        monthly_channel_id = digest_settings.get("monthly_channel_id")
        weekly_line = "Off"
        monthly_line = "Off"
        if digest_settings.get("weekly_enabled"):
            weekly_line = f"<#{weekly_channel_id}>" if isinstance(weekly_channel_id, int) else "Setup needed"
        if digest_settings.get("monthly_enabled"):
            monthly_line = f"<#{monthly_channel_id}>" if isinstance(monthly_channel_id, int) else "Setup needed"
        embed.add_field(
            name="Knowledge Digests",
            value=(
                f"Weekly: **{weekly_line}**\n"
                f"Monthly: **{monthly_line}**\n"
                f"Timezone: **{digest_settings.get('timezone', 'UTC')}**"
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
            elif isinstance(scholar.get("announcement_channel_id"), int):
                scholar_flags.append(f"announce <#{int(scholar['announcement_channel_id'])}>")
            else:
                scholar_flags.append("announce off")
            scholar_flags.append(
                "custom default"
                if self._configured_announcement_template(scholar, scope_type="scholar") is not None
                else "default copy"
            )
            override_count = len(self._active_tier_template_overrides(scholar, scope_type="scholar"))
            if override_count > 0:
                label = "tier override" if override_count == 1 else "tier overrides"
                scholar_flags.append(f"{override_count} {label}")
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
        if isinstance(digest_status, dict):
            cadence = digest_status.get("cadence", {})
            weekly = cadence.get("weekly", {})
            monthly = cadence.get("monthly", {})
            digest_lines = [
                f"Weekly: {self._format_digest_run_summary(weekly.get('last_run'))}",
                f"Monthly: {self._format_digest_run_summary(monthly.get('last_run'))}",
                f"Low activity: **{'Skip' if digest_settings.get('skip_low_activity', True) else 'Post anyway'}**",
                f"Mentions: **{'@here' if digest_settings.get('mention_mode') == 'here' else 'No pings'}**",
            ]
            embed.add_field(name="Digest Runs", value="\n".join(digest_lines), inline=False)
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

    def build_digest_status_embed(self, guild: discord.Guild, snapshot: QuestionDropStatusSnapshot) -> discord.Embed:
        digest_status = snapshot.digest_status if isinstance(snapshot.digest_status, dict) else {}
        settings = digest_status.get("settings", {}) if isinstance(digest_status, dict) else {}
        cadence = digest_status.get("cadence", {}) if isinstance(digest_status, dict) else {}
        weekly_channel_text = f"<#{int(settings['weekly_channel_id'])}>" if isinstance(settings.get("weekly_channel_id"), int) else "Not set"
        monthly_channel_text = f"<#{int(settings['monthly_channel_id'])}>" if isinstance(settings.get("monthly_channel_id"), int) else "Not set"
        embed = discord.Embed(
            title="Knowledge Digests",
            description="Weekly and monthly prestige recaps for the Question Drops knowledge lane.",
            color=ge.EMBED_THEME["accent"],
        )
        weekly = cadence.get("weekly", {})
        monthly = cadence.get("monthly", {})
        embed.add_field(
            name="Cadence",
            value=(
                f"Weekly: **{'On' if settings.get('weekly_enabled') else 'Off'}**\n"
                f"Monthly: **{'On' if settings.get('monthly_enabled') else 'Off'}**\n"
                f"Timezone: **{settings.get('timezone', 'UTC')}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Delivery",
            value=(
                f"Weekly channel: {weekly_channel_text}\n"
                f"Monthly channel: {monthly_channel_text}\n"
                f"Mentions: **{'@here' if settings.get('mention_mode') == 'here' else 'No pings'}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Posting Rules",
            value=(
                f"Low activity: **{'Skip' if settings.get('skip_low_activity', True) else 'Post anyway'}**\n"
                f"Weekly post time: **Monday 09:00** local\n"
                f"Monthly post time: **Day 1 at 09:00** local"
            ),
            inline=True,
        )
        embed.add_field(
            name="Last Runs",
            value=(
                f"Weekly: {self._format_digest_run_summary(weekly.get('last_run'))}\n"
                f"Monthly: {self._format_digest_run_summary(monthly.get('last_run'))}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Next Windows",
            value=(
                f"Weekly: {ge.format_timestamp(weekly.get('next_post_at'), 'R') if weekly.get('next_post_at') is not None else 'Not scheduled'}\n"
                f"Monthly: {ge.format_timestamp(monthly.get('next_post_at'), 'R') if monthly.get('next_post_at') is not None else 'Not scheduled'}"
            ),
            inline=False,
        )
        issues = digest_status.get("issues", []) if isinstance(digest_status, dict) else []
        if issues:
            embed.add_field(name="Operability", value="\n".join(issues[:6]), inline=False)
        return ge.style_embed(embed, footer="Babblebox Question Drops | Digest control surface")

    def _digest_activity_thresholds(self, kind: str) -> tuple[int, int]:
        if kind == "monthly":
            return QUESTION_DROP_DIGEST_MONTHLY_MIN_SOLVES, QUESTION_DROP_DIGEST_MONTHLY_MIN_PARTICIPANTS
        return QUESTION_DROP_DIGEST_WEEKLY_MIN_SOLVES, QUESTION_DROP_DIGEST_WEEKLY_MIN_PARTICIPANTS

    async def _collect_digest_period_data(self, guild_id: int, period: QuestionDropDigestPeriod) -> dict[str, Any]:
        events = await self.store.list_participation_events_for_guild(
            guild_id,
            start=period.period_start_at,
            end=period.period_end_at,
        )
        user_rows: dict[int, dict[str, Any]] = {}
        user_events: dict[int, list[dict[str, Any]]] = defaultdict(list)
        category_rows: dict[str, dict[str, Any]] = {}
        distinct_participants: set[int] = set()
        solved_exposures: set[int] = set()
        total_points = 0
        for event in events:
            user_id = int(event["user_id"])
            category_id = str(event["category"]).strip().casefold()
            row = user_rows.setdefault(
                user_id,
                {
                    "user_id": user_id,
                    "attempt_count": 0,
                    "correct_count": 0,
                    "miss_count": 0,
                    "points_awarded": 0,
                    "best_streak": 0,
                },
            )
            row["attempt_count"] += 1
            distinct_participants.add(user_id)
            if event.get("correct"):
                row["correct_count"] += 1
                solved_exposures.add(int(event["exposure_id"]))
            else:
                row["miss_count"] += 1
            points_awarded = int(event.get("points_awarded", 0) or 0)
            row["points_awarded"] += points_awarded
            total_points += points_awarded
            user_events[user_id].append(event)
            category_row = category_rows.setdefault(
                category_id,
                {
                    "category": category_id,
                    "points_awarded": 0,
                    "solves": 0,
                    "unique_scorers": set(),
                },
            )
            category_row["points_awarded"] += points_awarded
            if event.get("correct"):
                category_row["solves"] += 1
            if points_awarded > 0:
                category_row["unique_scorers"].add(user_id)
        hot_streak = None
        for user_id, rows in user_events.items():
            current_streak = 0
            best_streak = 0
            for event in rows:
                if event.get("correct"):
                    current_streak += 1
                    best_streak = max(best_streak, current_streak)
                else:
                    current_streak = 0
            user_rows[user_id]["best_streak"] = best_streak
            if best_streak <= 0:
                continue
            candidate = {
                "user_id": user_id,
                "streak": best_streak,
                "points_awarded": int(user_rows[user_id]["points_awarded"]),
                "correct_count": int(user_rows[user_id]["correct_count"]),
            }
            if hot_streak is None or (
                candidate["streak"],
                candidate["points_awarded"],
                candidate["correct_count"],
                -candidate["user_id"],
            ) > (
                hot_streak["streak"],
                hot_streak["points_awarded"],
                hot_streak["correct_count"],
                -hot_streak["user_id"],
            ):
                hot_streak = candidate
        ranked_users = sorted(
            user_rows.values(),
            key=lambda item: (
                -int(item["points_awarded"]),
                -int(item["correct_count"]),
                int(item["miss_count"]),
                int(item["user_id"]),
            ),
        )
        ranked_categories = sorted(
            (
                {
                    "category": row["category"],
                    "points_awarded": int(row["points_awarded"]),
                    "solves": int(row["solves"]),
                    "unique_scorers": len(row["unique_scorers"]),
                }
                for row in category_rows.values()
            ),
            key=lambda item: (
                -int(item["points_awarded"]),
                -int(item["solves"]),
                -int(item["unique_scorers"]),
                str(item["category"]),
            ),
        )
        return {
            "events": events,
            "ranked_users": ranked_users,
            "ranked_categories": ranked_categories,
            "hot_streak": hot_streak,
            "solved_drops": len(solved_exposures),
            "distinct_participants": len(distinct_participants),
            "total_points": total_points,
        }

    async def _fetch_digest_unlock_highlights(self, guild_id: int, period: QuestionDropDigestPeriod) -> list[dict[str, Any]]:
        profile_service = self._profile_service()
        profile_store = getattr(profile_service, "store", None) if profile_service is not None else None
        if profile_store is None or not hasattr(profile_store, "fetch_question_drop_unlocks_for_period"):
            return []
        rows = await profile_store.fetch_question_drop_unlocks_for_period(
            guild_id=guild_id,
            start=period.period_start_at,
            end=period.period_end_at,
        )
        return list(rows[:4]) if isinstance(rows, list) else []

    async def _digest_next_milestone_note(
        self,
        guild_id: int,
        ranked_users: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> dict[str, Any] | None:
        profile_service = self._profile_service()
        profile_store = getattr(profile_service, "store", None) if profile_service is not None else None
        if profile_store is None:
            return None
        best_candidate = None
        for row in ranked_users[:5]:
            user_id = int(row["user_id"])
            unlocks = await profile_store.fetch_question_drop_unlocks(guild_id=guild_id, user_id=user_id)
            guild_profile = await profile_store.fetch_question_drop_guild_profile(guild_id=guild_id, user_id=user_id)
            guild_points = int((guild_profile or {}).get("points", 0) or 0)
            scholar_config = dict(config.get("scholar_ladder", {}))
            if scholar_config.get("enabled"):
                next_scholar = self._next_scope_tier(
                    self._configured_unlock_tiers(scholar_config),
                    points=guild_points,
                    current_tier=self._current_scope_tier(unlocks, scope_type="scholar", scope_key="global"),
                )
                if next_scholar is not None:
                    candidate = {
                        "remaining": int(next_scholar["remaining"]),
                        "user_id": user_id,
                        "label": f"{scholar_label(int(next_scholar['tier']))}",
                        "scope": "scholar",
                    }
                    if best_candidate is None or (
                        candidate["remaining"],
                        -int(row["points_awarded"]),
                        int(candidate["user_id"]),
                    ) < (
                        best_candidate["remaining"],
                        -int(best_candidate["points_awarded"]),
                        int(best_candidate["user_id"]),
                    ):
                        candidate["points_awarded"] = int(row["points_awarded"])
                        best_candidate = candidate
            for category_row in sorted(
                await profile_store.fetch_question_drop_guild_categories(guild_id=guild_id, user_id=user_id),
                key=lambda item: (-int(item.get("points", 0) or 0), str(item.get("category", ""))),
            ):
                category_id = str(category_row.get("category") or "").strip().casefold()
                category_config = self._configured_category_mastery(config, category_id)
                if not category_config.get("enabled"):
                    continue
                next_category = self._next_scope_tier(
                    self._configured_unlock_tiers(category_config),
                    points=int(category_row.get("points", 0) or 0),
                    current_tier=self._current_scope_tier(unlocks, scope_type="category", scope_key=category_id),
                )
                if next_category is None:
                    continue
                candidate = {
                    "remaining": int(next_category["remaining"]),
                    "user_id": user_id,
                    "label": f"{category_label(category_id)} {tier_label(int(next_category['tier']))}",
                    "scope": "category",
                    "points_awarded": int(row["points_awarded"]),
                }
                if best_candidate is None or (
                    candidate["remaining"],
                    -int(candidate["points_awarded"]),
                    int(candidate["user_id"]),
                ) < (
                    best_candidate["remaining"],
                    -int(best_candidate["points_awarded"]),
                    int(best_candidate["user_id"]),
                ):
                    best_candidate = candidate
                break
        return best_candidate

    def _render_digest_unlock_line(self, guild: discord.Guild, row: dict[str, Any]) -> str:
        user_label = self._resolve_guild_user_label(guild, int(row.get("user_id", 0) or 0))
        scope_type = str(row.get("scope_type") or "").casefold()
        tier = int(row.get("tier", 0) or 0)
        if scope_type == "scholar":
            return f"{progression_emoji('scholar')} **{user_label}** reached **{scholar_label(tier)}**"
        category_id = str(row.get("scope_key") or "").strip().casefold()
        return f"{progression_emoji('role')} **{user_label}** unlocked **{category_label(category_id)} {tier_label(tier)}**"

    def _build_digest_embed(
        self,
        guild: discord.Guild,
        *,
        period: QuestionDropDigestPeriod,
        metrics: dict[str, Any],
        unlock_rows: list[dict[str, Any]],
        next_milestone: dict[str, Any] | None,
    ) -> discord.Embed:
        ranked_users = metrics.get("ranked_users", [])
        ranked_categories = metrics.get("ranked_categories", [])
        title = f"{progression_emoji('scholar')} {'Weekly' if period.kind == 'weekly' else 'Monthly'} Knowledge Digest"
        description = f"**{guild.name}** | {self._digest_period_window_label(period)}"
        color = ge.EMBED_THEME["accent"] if period.kind == "weekly" else ge.EMBED_THEME["info"]
        embed = discord.Embed(title=title, description=description, color=color)
        if ranked_users:
            podium_lines = []
            for index, row in enumerate(ranked_users[:3], start=1):
                user_label = self._resolve_guild_user_label(guild, int(row["user_id"]))
                bits = [f"{int(row['points_awarded'])} pts", f"{int(row['correct_count'])} solves"]
                if int(row.get("miss_count", 0) or 0) > 0:
                    bits.append(f"{int(row['miss_count'])} misses")
                podium_lines.append(f"{leaderboard_marker(index)} **{user_label}** - {' | '.join(bits)}")
            embed.add_field(name="Podium", value="\n".join(podium_lines), inline=False)
        hot_streak = metrics.get("hot_streak")
        if period.kind == "weekly" and isinstance(hot_streak, dict):
            user_label = self._resolve_guild_user_label(guild, int(hot_streak["user_id"]))
            embed.add_field(
                name="🔥 Hot Streak",
                value=f"**{user_label}** ran off **{int(hot_streak['streak'])}** straight solves.",
                inline=False,
            )
        if ranked_categories:
            if period.kind == "weekly":
                leader = ranked_categories[0]
                embed.add_field(
                    name="🧠 Category Spotlight",
                    value=(
                        f"{category_label_with_emoji(str(leader['category']))} led with **{int(leader['points_awarded'])}** pts, "
                        f"**{int(leader['solves'])}** solves, and **{int(leader['unique_scorers'])}** scorers."
                    ),
                    inline=False,
                )
            else:
                lines = [
                    (
                        f"{category_label_with_emoji(str(row['category']))} **{int(row['points_awarded'])}** pts | "
                        f"**{int(row['solves'])}** solves"
                    )
                    for row in ranked_categories[:2]
                ]
                embed.add_field(name="Category Leaders", value="\n".join(lines), inline=False)
        if unlock_rows:
            field_name = "Prestige Moments" if period.kind == "monthly" else "Unlock Highlights"
            embed.add_field(
                name=field_name,
                value="\n".join(self._render_digest_unlock_line(guild, row) for row in unlock_rows[:2]),
                inline=False,
            )
        if next_milestone is not None:
            user_label = self._resolve_guild_user_label(guild, int(next_milestone["user_id"]))
            embed.add_field(
                name="📈 Up Next",
                value=f"**{user_label}** is **{int(next_milestone['remaining'])}** pts from **{next_milestone['label']}**.",
                inline=False,
            )
        summary_line = (
            f"🧠 **{int(metrics.get('total_points', 0) or 0)}** pts across **{int(metrics.get('solved_drops', 0) or 0)}** solved drops "
            f"from **{int(metrics.get('distinct_participants', 0) or 0)}** scholars."
        )
        field_name = "Month In Knowledge" if period.kind == "monthly" else "Period Note"
        embed.add_field(name=field_name, value=summary_line, inline=False)
        footer = f"Babblebox Question Drops | {self._digest_period_window_label(period)} | Knowledge digest"
        return ge.style_embed(embed, footer=footer)

    def _digest_skip_reason(self, *, kind: str, settings: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        solved_drops = int(metrics.get("solved_drops", 0) or 0)
        distinct_participants = int(metrics.get("distinct_participants", 0) or 0)
        if solved_drops <= 0 or distinct_participants <= 0:
            return "No meaningful activity in this period."
        if not settings.get("skip_low_activity", True):
            return None
        min_solves, min_participants = self._digest_activity_thresholds(kind)
        if solved_drops < min_solves or distinct_participants < min_participants:
            return (
                f"Skipped for low activity ({solved_drops} solves, {distinct_participants} participants; "
                f"needs {min_solves} and {min_participants})."
            )
        return None

    async def _post_digest_for_period(self, guild: discord.Guild, *, config: dict[str, Any], kind: str, now: datetime):
        settings = self._effective_digest_settings(config)
        period = self._digest_period_for(kind=kind, timezone_name=settings["timezone"], now=now)
        if now < period.scheduled_post_at:
            return
        channel_id = settings.get(f"{kind}_channel_id")
        claimed = await self.store.claim_digest_run(
            {
                "guild_id": guild.id,
                "digest_kind": kind,
                "period_key": period.period_key,
                "period_start_at": period.period_start_at,
                "period_end_at": period.period_end_at,
                "scheduled_post_at": period.scheduled_post_at,
                "status": "claimed",
                "claimed_at": now,
                "lease_expires_at": now + timedelta(seconds=QUESTION_DROP_DIGEST_LEASE_SECONDS),
                "completed_at": None,
                "detail": "",
                "channel_id": channel_id,
                "message_id": None,
            }
        )
        if claimed is None:
            return
        issue = self._digest_channel_issue(guild, channel_id, label=f"{kind.title()} digest")
        if issue is not None:
            await self.store.finish_digest_run(
                guild.id,
                kind,
                period.period_key,
                status="failed",
                detail=issue,
                channel_id=channel_id,
                completed_at=ge.now_utc(),
            )
            return
        channel = guild.get_channel(int(channel_id)) if hasattr(guild, "get_channel") and isinstance(channel_id, int) else None
        if channel is None:
            await self.store.finish_digest_run(
                guild.id,
                kind,
                period.period_key,
                status="failed",
                detail="Digest channel is unavailable.",
                channel_id=channel_id,
                completed_at=ge.now_utc(),
            )
            return
        metrics = await self._collect_digest_period_data(guild.id, period)
        skip_reason = self._digest_skip_reason(kind=kind, settings=settings, metrics=metrics)
        if skip_reason is not None:
            await self.store.finish_digest_run(
                guild.id,
                kind,
                period.period_key,
                status="skipped",
                detail=skip_reason,
                channel_id=channel_id,
                completed_at=ge.now_utc(),
            )
            return
        unlock_rows = await self._fetch_digest_unlock_highlights(guild.id, period)
        next_milestone = await self._digest_next_milestone_note(guild.id, metrics.get("ranked_users", []), config)
        embed = self._build_digest_embed(
            guild,
            period=period,
            metrics=metrics,
            unlock_rows=unlock_rows,
            next_milestone=next_milestone,
        )
        content = "@here" if settings.get("mention_mode") == "here" else None
        allowed_mentions = (
            discord.AllowedMentions(users=False, roles=False, everyone=True)
            if content is not None
            else discord.AllowedMentions.none()
        )
        try:
            message = await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)
        except discord.HTTPException as exc:
            await self.store.finish_digest_run(
                guild.id,
                kind,
                period.period_key,
                status="failed",
                detail=f"Send failed: {exc}",
                channel_id=channel_id,
                completed_at=ge.now_utc(),
            )
            return
        await self.store.finish_digest_run(
            guild.id,
            kind,
            period.period_key,
            status="posted",
            detail=f"Posted {kind} digest.",
            channel_id=channel_id,
            message_id=int(getattr(message, "id", 0) or 0) or None,
            completed_at=ge.now_utc(),
        )

    async def _maybe_post_due_digests(self):
        if not self.storage_ready:
            return
        now = ge.now_utc()
        for guild_id, config in list(self._configs.items()):
            settings = self._effective_digest_settings(config)
            if not settings.get("weekly_enabled") and not settings.get("monthly_enabled"):
                continue
            guild = self.bot.get_guild(guild_id) if hasattr(self.bot, "get_guild") else None
            if guild is None:
                continue
            for kind in QUESTION_DROP_DIGEST_KINDS:
                if not settings.get(f"{kind}_enabled"):
                    continue
                await self._post_digest_for_period(guild, config=config, kind=kind, now=now)

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

    async def get_member_roles_status(self, guild: discord.Guild, member) -> dict[str, Any]:
        profile_service = self._profile_service()
        summary = {}
        if profile_service is not None:
            fetched_summary = await profile_service.get_question_drop_summary(int(getattr(member, "id", 0) or 0), guild_id=guild.id)
            if isinstance(fetched_summary, dict):
                summary = fetched_summary
        preference = summary.get("guild_role_preference") if isinstance(summary.get("guild_role_preference"), dict) else None
        if preference is None:
            preference = await self._member_role_grant_preference(guild_id=guild.id, user_id=int(getattr(member, "id", 0) or 0))
        managed_records = self._managed_role_records(guild, enabled_only=False)
        held_records = [record for record in managed_records if self._member_has_role(member, int(record.get("role_id", 0) or 0))]
        eligible_candidates = self._eligible_unlocks_for_summary(summary)
        restorable_role_ids = {
            int(candidate.get("role_id", 0) or 0)
            for candidate in eligible_candidates
            if int(candidate.get("role_id", 0) or 0) > 0 and not self._member_has_role(member, int(candidate.get("role_id", 0) or 0))
        }
        stale_managed_count = sum(1 for record in managed_records if record.get("role") is None)
        return {
            "member_id": int(getattr(member, "id", 0) or 0),
            "preference": preference,
            "summary": summary,
            "managed_records": managed_records,
            "held_records": held_records,
            "eligible_candidates": eligible_candidates,
            "restorable_role_ids": sorted(restorable_role_ids),
            "stale_managed_count": stale_managed_count,
        }

    def build_member_roles_status_embed(self, guild: discord.Guild, member, payload: dict[str, Any]) -> discord.Embed:
        preference = dict(payload.get("preference") or {})
        held_records = payload.get("held_records") or []
        grants_enabled = bool(preference.get("role_grants_enabled", True))
        restorable_count = len(payload.get("restorable_role_ids") or [])
        stale_managed_count = int(payload.get("stale_managed_count", 0) or 0)
        embed = discord.Embed(
            title="Question Drops Roles",
            description=f"Private control over Babblebox-managed Question Drops roles for **{ge.display_name_of(member)}** in **{guild.name}**.",
            color=ge.EMBED_THEME["info" if grants_enabled else "warning"],
        )
        future_lines = [
            f"Status: **{'On' if grants_enabled else 'Off'}**",
            (
                "Babblebox can grant future Question Drops roles when you earn them."
                if grants_enabled
                else "Babblebox will not grant future Question Drops roles until you turn this back on."
            ),
        ]
        if not grants_enabled and restorable_count > 0:
            future_lines.append(f"Restore-ready now: **{restorable_count}** role(s)")
        embed.add_field(name="Future Grants", value="\n".join(future_lines), inline=False)
        if held_records:
            held_lines = [self._managed_role_line(record) for record in held_records[:8]]
            if len(held_records) > 8:
                held_lines.append(f"...and **{len(held_records) - 8}** more")
            current_roles_value = "\n".join(held_lines)
        else:
            current_roles_value = "You are not wearing any Babblebox-managed Question Drops roles right now."
        embed.add_field(name="Current Roles", value=current_roles_value, inline=False)
        control_lines = [
            "`/drops roles remove` takes off current Babblebox Question Drops roles only.",
            "`/drops roles preference stop` turns future Babblebox role grants off.",
        ]
        if grants_enabled:
            control_lines.append("Removing roles now does not change future eligibility.")
            if restorable_count > 0:
                control_lines.append("Missing eligible roles stay off unless you explicitly choose restore now.")
        else:
            control_lines.append("Existing roles stay as they are unless you remove them.")
            control_lines.append("`/drops roles preference receive` turns grants back on. Add restore now if you want eligible roles back immediately.")
        embed.add_field(name="What Changes", value="\n".join(control_lines), inline=False)
        if stale_managed_count > 0:
            embed.add_field(
                name="Config Notes",
                value=f"Babblebox ignored **{stale_managed_count}** managed role id(s) that no longer exist in this server.",
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Question Drops | Current roles and future grants stay separate")

    def _role_update_issue_text(self, code: str) -> str:
        return {
            "missing_manage_roles": "Babblebox is missing `Manage Roles`.",
            "hierarchy_blocked": "Blocked by Discord role hierarchy.",
            "forbidden": "Discord rejected the role update.",
            "http_error": "Discord returned an API error.",
            "member_unavailable": "Member role access is unavailable.",
            "bot_unavailable": "Bot member could not be resolved.",
            "missing_role": "Role no longer exists in this server.",
        }.get(str(code), "Role update failed.")

    async def remove_member_managed_roles(self, guild: discord.Guild, member, *, role_id: int | None = None) -> dict[str, Any]:
        preference = await self._member_role_grant_preference(guild_id=guild.id, user_id=int(getattr(member, "id", 0) or 0))
        managed_records = self._managed_role_records(guild, enabled_only=False)
        managed_by_id = {int(record.get("role_id", 0) or 0): record for record in managed_records}
        requested_role_id = int(role_id or 0) or None
        if requested_role_id is not None and requested_role_id not in managed_by_id:
            return {
                "status": "not_managed",
                "preference": preference,
                "requested_role_id": requested_role_id,
            }
        if requested_role_id is not None:
            target_records = [managed_by_id[requested_role_id]]
        else:
            target_records = [record for record in managed_records if self._member_has_role(member, int(record.get("role_id", 0) or 0))]
        if not target_records:
            return {
                "status": "nothing_to_remove",
                "preference": preference,
                "requested_role_id": requested_role_id,
            }
        removed: list[dict[str, Any]] = []
        already_missing: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        for record in target_records:
            role = record.get("role")
            status = await self._attempt_remove_role(
                guild,
                member,
                role,
                reason="Babblebox Question Drops role removal requested by member",
            )
            if status == "removed":
                removed.append(record)
            elif status == "already_missing":
                already_missing.append(record)
            else:
                issues.append({"record": record, "code": status})
        return {
            "status": "ok",
            "preference": preference,
            "requested_role_id": requested_role_id,
            "removed": removed,
            "already_missing": already_missing,
            "issues": issues,
        }

    def build_member_roles_remove_embed(self, payload: dict[str, Any]) -> discord.Embed:
        preference = dict(payload.get("preference") or {})
        grants_enabled = bool(preference.get("role_grants_enabled", True))
        status = str(payload.get("status", "ok"))
        if status == "not_managed":
            return ge.make_status_embed(
                "Question Drops Roles",
                "That role is not a Babblebox-managed Question Drops role, so Babblebox left everything alone.",
                tone="warning",
                footer="Babblebox Question Drops",
            )
        if status == "nothing_to_remove":
            return ge.make_status_embed(
                "Question Drops Roles",
                "You are not wearing any matching Babblebox-managed Question Drops roles right now.",
                tone="info",
                footer="Babblebox Question Drops",
            )
        removed = payload.get("removed") or []
        issues = payload.get("issues") or []
        already_missing = payload.get("already_missing") or []
        description_lines = []
        if removed:
            description_lines.append(f"Removed **{len(removed)}** Babblebox-managed Question Drops role(s).")
        else:
            description_lines.append("No Babblebox-managed Question Drops roles were removed.")
        description_lines.append(
            "Future Babblebox role grants are still **on**."
            if grants_enabled
            else "Future Babblebox role grants are still **off**."
        )
        if grants_enabled:
            description_lines.append("Use `/drops roles preference stop` if you also want Babblebox to stop future grants.")
        embed = discord.Embed(
            title="Question Drops Roles Updated",
            description="\n".join(description_lines),
            color=ge.EMBED_THEME["success" if removed and not issues else "info"],
        )
        if removed:
            embed.add_field(name="Removed", value="\n".join(self._managed_role_line(record) for record in removed[:8]), inline=False)
        if issues:
            embed.add_field(
                name="Could Not Remove",
                value="\n".join(
                    f"{self._managed_role_line(item['record'])} | {self._role_update_issue_text(str(item.get('code', '')))}"
                    for item in issues[:8]
                ),
                inline=False,
            )
        if already_missing:
            embed.add_field(
                name="Already Off",
                value="\n".join(self._managed_role_line(record) for record in already_missing[:8]),
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Question Drops | Role removal does not change achievement history")

    async def restore_member_eligible_roles(self, guild: discord.Guild, member, *, summary: dict[str, Any] | None = None) -> dict[str, Any]:
        if summary is None:
            profile_service = self._profile_service()
            summary = {}
            if profile_service is not None:
                fetched_summary = await profile_service.get_question_drop_summary(int(getattr(member, "id", 0) or 0), guild_id=guild.id)
                if isinstance(fetched_summary, dict):
                    summary = fetched_summary
        eligible_candidates = self._eligible_unlocks_for_summary(summary or {})
        grouped_candidates: dict[int, dict[str, Any]] = {}
        for candidate in eligible_candidates:
            role_id = int(candidate.get("role_id", 0) or 0)
            if role_id <= 0:
                continue
            grouped = grouped_candidates.setdefault(
                role_id,
                {
                    "role_id": role_id,
                    "role": guild.get_role(role_id) if hasattr(guild, "get_role") else None,
                    "candidates": [],
                },
            )
            grouped["candidates"].append(candidate)
        if not grouped_candidates:
            return {"status": "nothing_to_restore", "restored": [], "already_had": [], "issues": []}
        restored: list[dict[str, Any]] = []
        already_had: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []
        records = sorted(
            grouped_candidates.values(),
            key=lambda item: (
                -int(getattr(item.get("role"), "position", 0) or 0),
                str(getattr(item.get("role"), "name", "")),
                int(item.get("role_id", 0) or 0),
            ),
        )
        for item in records:
            role = item.get("role")
            role_id = int(item.get("role_id", 0) or 0)
            status = await self._attempt_add_role(
                guild,
                member,
                role,
                reason="Babblebox Question Drops role restore requested by member",
            )
            if status in {"added", "already_has"}:
                for candidate in item.get("candidates", []):
                    await self._record_unlock_history(
                        guild_id=guild.id,
                        user_id=int(getattr(member, "id", 0) or 0),
                        scope_type=str(candidate.get("scope_type", "")),
                        scope_key=str(candidate.get("scope_key", "")),
                        tier=int(candidate.get("tier", 0) or 0),
                        role_id=role_id,
                    )
                record = {
                    "role_id": role_id,
                    "role": role,
                    "scopes": [
                        {
                            "scope_type": candidate.get("scope_type"),
                            "scope_key": candidate.get("scope_key"),
                            "tier": candidate.get("tier"),
                        }
                        for candidate in item.get("candidates", [])
                    ],
                }
                if status == "added":
                    restored.append(record)
                else:
                    already_had.append(record)
            else:
                issues.append(
                    {
                        "role_id": role_id,
                        "role": role,
                        "scopes": [
                            {
                                "scope_type": candidate.get("scope_type"),
                                "scope_key": candidate.get("scope_key"),
                                "tier": candidate.get("tier"),
                            }
                            for candidate in item.get("candidates", [])
                        ],
                        "code": status,
                    }
                )
        return {
            "status": "ok",
            "restored": restored,
            "already_had": already_had,
            "issues": issues,
        }

    async def update_member_role_preference(
        self,
        guild: discord.Guild,
        member,
        *,
        mode: str,
        remove_current_roles: bool = False,
        restore_current_roles: bool = False,
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().casefold()
        before = await self._member_role_grant_preference(guild_id=guild.id, user_id=int(getattr(member, "id", 0) or 0))
        profile_service = self._profile_service()
        summary = {}
        if profile_service is not None:
            fetched_summary = await profile_service.get_question_drop_summary(int(getattr(member, "id", 0) or 0), guild_id=guild.id)
            if isinstance(fetched_summary, dict):
                summary = fetched_summary
        if normalized_mode == "stop":
            after = await self._set_member_role_grants_enabled(
                guild_id=guild.id,
                user_id=int(getattr(member, "id", 0) or 0),
                enabled=False,
            )
            removal = None
            if remove_current_roles:
                removal = await self.remove_member_managed_roles(guild, member)
            return {
                "mode": "stop",
                "before": before,
                "after": after,
                "remove_current_roles": bool(remove_current_roles),
                "restore_current_roles": False,
                "removal": removal,
                "restore": None,
                "summary": summary,
            }
        if normalized_mode == "receive":
            after = await self._set_member_role_grants_enabled(
                guild_id=guild.id,
                user_id=int(getattr(member, "id", 0) or 0),
                enabled=True,
            )
            restore = None
            if restore_current_roles:
                restore = await self.restore_member_eligible_roles(guild, member, summary=summary)
            return {
                "mode": "receive",
                "before": before,
                "after": after,
                "remove_current_roles": False,
                "restore_current_roles": bool(restore_current_roles),
                "removal": None,
                "restore": restore,
                "summary": summary,
            }
        raise ValueError(f"Unsupported Question Drops role preference mode '{mode}'.")

    def build_member_role_preference_embed(self, payload: dict[str, Any]) -> discord.Embed:
        mode = str(payload.get("mode", "")).casefold()
        before = dict(payload.get("before") or {})
        after = dict(payload.get("after") or {})
        if mode == "stop":
            lines = [
                "Future Babblebox Question Drops role grants are now **off** for you in this server."
                if bool(before.get("role_grants_enabled", True))
                else "Future Babblebox Question Drops role grants were already **off** for you in this server.",
            ]
            if payload.get("remove_current_roles"):
                removal = payload.get("removal") or {}
                removed_count = len(removal.get("removed") or [])
                issue_count = len(removal.get("issues") or [])
                lines.append(f"Current role cleanup removed **{removed_count}** role(s)." if removed_count else "Current role cleanup did not remove any roles.")
                if issue_count:
                    lines.append(f"Some roles still need attention: **{issue_count}** could not be removed.")
            else:
                lines.append("Your current roles stay as they are unless you remove them.")
            embed = discord.Embed(
                title="Question Drops Role Grants Off",
                description="\n".join(lines),
                color=ge.EMBED_THEME["warning"],
            )
            if payload.get("removal"):
                removal = payload["removal"]
                if removal.get("removed"):
                    embed.add_field(
                        name="Removed Now",
                        value="\n".join(self._managed_role_line(record) for record in (removal.get("removed") or [])[:8]),
                        inline=False,
                    )
                if removal.get("issues"):
                    embed.add_field(
                        name="Could Not Remove",
                        value="\n".join(
                            f"{self._managed_role_line(item['record'])} | {self._role_update_issue_text(str(item.get('code', '')))}"
                            for item in (removal.get("issues") or [])[:8]
                        ),
                        inline=False,
                    )
            return ge.style_embed(embed, footer="Babblebox Question Drops | Achievement history stays intact")
        restore = payload.get("restore") or {}
        lines = [
            "Future Babblebox Question Drops role grants are now **on** for you in this server."
            if not bool(before.get("role_grants_enabled", True))
            else "Future Babblebox Question Drops role grants were already **on** for you in this server.",
        ]
        if payload.get("restore_current_roles"):
            restored_count = len(restore.get("restored") or [])
            if restored_count:
                lines.append(f"Restored **{restored_count}** currently eligible role(s) now.")
            elif str(restore.get("status", "")) == "nothing_to_restore":
                lines.append("There were no currently eligible Babblebox roles to restore right now.")
            else:
                lines.append("No additional roles were restored right now.")
        else:
            lines.append("Existing roles stay as they are unless you explicitly restore them.")
        embed = discord.Embed(
            title="Question Drops Role Grants On",
            description="\n".join(lines),
            color=ge.EMBED_THEME["success"],
        )
        if payload.get("restore_current_roles"):
            if restore.get("restored"):
                embed.add_field(
                    name="Restored Now",
                    value="\n".join(self._managed_role_line(record) for record in (restore.get("restored") or [])[:8]),
                    inline=False,
                )
            if restore.get("issues"):
                embed.add_field(
                    name="Could Not Restore",
                    value="\n".join(
                        f"{self._managed_role_line(item)} | {self._role_update_issue_text(str(item.get('code', '')))}"
                        for item in (restore.get("issues") or [])[:8]
                    ),
                    inline=False,
                )
        return ge.style_embed(embed, footer="Babblebox Question Drops | Opt-in does not post public catch-up grants")

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
        return [candidate for candidate in self._eligible_unlocks_for_summary(summary) if not bool(candidate.get("unlocked"))]

    async def recalculate_mastery_roles(
        self,
        guild: discord.Guild,
        *,
        member=None,
        preview: bool = True,
    ) -> dict[str, Any]:
        profile_service = self._profile_service()
        if profile_service is None:
            return {"preview": preview, "scanned": 0, "pending": 0, "granted": 0, "skipped_opted_out": 0}
        await self._ensure_guild_backfill(guild.id)
        targets = [member] if member is not None else self._guild_members_for_recalc(guild)
        pending_total = 0
        granted_total = 0
        skipped_opted_out = 0
        scanned = 0
        for target in targets:
            if target is None or bool(getattr(target, "bot", False)):
                continue
            scanned += 1
            summary = await profile_service.get_question_drop_summary(int(target.id), guild_id=guild.id)
            if not isinstance(summary, dict):
                continue
            preference = summary.get("guild_role_preference") if isinstance(summary.get("guild_role_preference"), dict) else None
            if preference is None:
                preference = await self._member_role_grant_preference(guild_id=guild.id, user_id=int(target.id))
            if not bool(preference.get("role_grants_enabled", True)):
                skipped_opted_out += 1
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
        return {
            "preview": preview,
            "scanned": scanned,
            "pending": pending_total,
            "granted": granted_total,
            "skipped_opted_out": skipped_opted_out,
        }

    def observe_message_activity(self, message: discord.Message):
        if message.guild is None:
            return
        self._recent_activity[(message.guild.id, message.channel.id)] = ge.now_utc()

    def _log_drop_event(self, marker: str, *, level: int = logging.DEBUG, **fields: Any):
        if not LOGGER.isEnabledFor(level):
            return
        rendered_fields: list[str] = []
        for key, value in fields.items():
            if value is None:
                continue
            if isinstance(value, datetime):
                value = value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
                rendered = value.isoformat()
            elif isinstance(value, bool):
                rendered = "true" if value else "false"
            else:
                rendered = str(value)
            rendered_fields.append(f"{key}={rendered}")
        if rendered_fields:
            LOGGER.log(level, "%s %s", marker, " ".join(rendered_fields))
            return
        LOGGER.log(level, "%s", marker)

    def _recently_resolved_key(self, guild_id: int, channel_id: int) -> tuple[int, int]:
        return (int(guild_id), int(channel_id))

    def _clear_recently_resolved_drop(self, guild_id: int, channel_id: int):
        self._recently_resolved_drops.pop(self._recently_resolved_key(guild_id, channel_id), None)

    def _recently_resolved_drop_locked(
        self,
        guild_id: int,
        channel_id: int,
        *,
        now: datetime,
    ) -> QuestionDropRecentResolutionContext | None:
        record = self._recently_resolved_drops.get(self._recently_resolved_key(guild_id, channel_id))
        if record is None:
            return None
        if now > record.late_window_ends_at:
            return None
        return record

    def _collect_due_timeout_finalizations_locked(self, *, now: datetime) -> list[QuestionDropRecentResolutionContext]:
        due: list[QuestionDropRecentResolutionContext] = []
        for key, record in list(self._recently_resolved_drops.items()):
            if now <= record.late_window_ends_at:
                continue
            self._recently_resolved_drops.pop(key, None)
            if record.winner_user_id is None and not record.participation_finalized:
                record.participation_finalized = True
                due.append(record)
        return due

    async def _finalize_due_recent_timeouts(self, *, now: datetime):
        async with self._lock:
            due = self._collect_due_timeout_finalizations_locked(now=now)
        for record in due:
            await self._finalize_timeout_context(record)

    async def _finalize_timeout_context(self, record: QuestionDropRecentResolutionContext):
        if not record.participant_user_ids:
            return
        self._log_drop_event(
            "drop_timeout_claim",
            level=logging.INFO,
            guild_id=record.guild_id,
            channel_id=record.channel_id,
            exposure_id=record.exposure_id,
            finalized=True,
            winner_user_id=record.winner_user_id,
        )
        await self._record_participation_batch(
            guild_id=record.guild_id,
            exposure_id=record.exposure_id,
            occurred_at=record.asked_at,
            category=record.category,
            difficulty=record.difficulty,
            participant_ids=list(record.participant_user_ids),
            winner_user_id=record.winner_user_id,
            persist_events=True,
            record_profiles=True,
        )

    def _remember_recently_resolved_drop(
        self,
        record: dict[str, Any],
        *,
        winner_user_id: int | None,
        resolved_at: datetime,
        participant_user_ids: list[int] | None = None,
        attempt_counts_by_user: dict[int, int] | None = None,
        participation_finalized: bool = True,
    ):
        self._recently_resolved_drops[self._recently_resolved_key(record["guild_id"], record["channel_id"])] = QuestionDropRecentResolutionContext(
            guild_id=int(record["guild_id"]),
            channel_id=int(record["channel_id"]),
            message_id=int(record["message_id"]),
            exposure_id=int(record["exposure_id"]),
            category=str(record["category"]),
            difficulty=int(record["difficulty"]),
            answer_spec=dict(record["answer_spec"]),
            asked_at=self._coerce_datetime(record["asked_at"]),
            score_deadline_at=self._active_drop_expires_at(record),
            winner_user_id=int(winner_user_id) if isinstance(winner_user_id, int) and winner_user_id > 0 else None,
            resolved_at=resolved_at,
            participant_user_ids=sorted(
                user_id for user_id in (participant_user_ids or record.get("participant_user_ids", []) or []) if isinstance(user_id, int) and user_id > 0
            ),
            attempt_counts_by_user=dict(sorted((attempt_counts_by_user or record.get("attempt_counts_by_user", {}) or {}).items())),
            acknowledged_user_ids=set(),
            participation_finalized=bool(participation_finalized),
        )

    def _set_recent_timeout_message_id(
        self,
        guild_id: int,
        channel_id: int,
        *,
        exposure_id: int,
        message_id: int | None,
    ):
        record = self._recently_resolved_drops.get(self._recently_resolved_key(guild_id, channel_id))
        if record is None or int(record.exposure_id) != int(exposure_id):
            return
        record.timeout_result_message_id = int(message_id) if isinstance(message_id, int) and message_id > 0 else None

    async def _send_late_correct_ack(
        self,
        message: discord.Message,
        *,
        category: str,
        winner_user_id: int | None,
    ) -> bool:
        await self._try_add_answer_reaction(message, state_emoji("correct"))
        if isinstance(winner_user_id, int) and winner_user_id > 0:
            title = f"{state_emoji('late')} Just Late"
            description = f"Correct, but <@{winner_user_id}> locked the drop first."
        else:
            title = f"{state_emoji('late')} Too Late"
            description = "Too late to score, but that was the right answer."
        self._log_drop_event(
            "drop_late_correct_ack",
            level=logging.INFO,
            guild_id=getattr(getattr(message, "guild", None), "id", None),
            channel_id=getattr(getattr(message, "channel", None), "id", None),
            author_id=getattr(getattr(message, "author", None), "id", None),
            winner_user_id=winner_user_id,
        )
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send(
                embed=ge.make_status_embed(
                    title,
                    description,
                    tone="info",
                    footer=f"Babblebox Question Drops | {category_label(category)}",
                ),
                delete_after=6.0,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        return False

    async def _try_edit_timeout_result_message(
        self,
        channel,
        *,
        timeout_message_id: int | None,
        winner,
        answer_spec: dict[str, Any],
    ) -> bool:
        if not isinstance(timeout_message_id, int) or timeout_message_id <= 0:
            return False
        fetch_message = getattr(channel, "fetch_message", None)
        if not callable(fetch_message):
            return False
        try:
            timeout_message = await fetch_message(timeout_message_id)
        except (discord.NotFound, discord.HTTPException):
            return False
        edit = getattr(timeout_message, "edit", None)
        if not callable(edit):
            return False
        answer = render_answer_summary(answer_spec)
        corrected_embed = ge.make_status_embed(
            f"{state_emoji('correct')} Corrected Result",
            f"{ge.display_name_of(winner)} had the right answer in time. Answer: **{answer}**.",
            tone="success",
            footer="Babblebox Question Drops",
        )
        try:
            await edit(embed=corrected_embed, allowed_mentions=discord.AllowedMentions.none())
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, TypeError):
            return False
        return True

    def _can_add_answer_reaction(self, guild: discord.Guild | None, channel) -> bool:
        if guild is None:
            return True
        permissions_for = getattr(channel, "permissions_for", None)
        if not callable(permissions_for):
            return True
        bot_member = self._bot_member_for_guild(guild)
        if bot_member is None:
            return True
        perms = permissions_for(bot_member)
        return all(
            bool(getattr(perms, field, True))
            for field in ("view_channel", "read_message_history", "add_reactions")
        )

    async def _try_add_answer_reaction(self, message: discord.Message, emoji: str) -> bool:
        if not emoji or not self._can_add_answer_reaction(getattr(message, "guild", None), getattr(message, "channel", None)):
            return False
        add_reaction = getattr(message, "add_reaction", None)
        if not callable(add_reaction):
            return False
        existing_reactions = getattr(message, "reactions", None)
        if isinstance(existing_reactions, list):
            for reaction in existing_reactions:
                if str(getattr(reaction, "emoji", "")) == emoji and bool(getattr(reaction, "me", False)):
                    return False
        try:
            await add_reaction(emoji)
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            return False
        return True

    async def _acknowledge_late_correct_answer(
        self,
        message: discord.Message,
        *,
        now: datetime,
        reply_target_id: int | None,
        message_created_at: datetime,
    ) -> bool:
        if message.guild is None:
            return False
        candidate_content = self._sanitize_answer_candidate(message.content)
        author_id = int(getattr(message.author, "id", 0) or 0)
        if author_id <= 0:
            return False
        recovery_payload: dict[str, Any] | None = None
        late_ack_payload: dict[str, Any] | None = None
        async with self._lock:
            due_finalizations = self._collect_due_timeout_finalizations_locked(now=now)
            recent = self._recently_resolved_drop_locked(message.guild.id, message.channel.id, now=now)
            if recent is not None and author_id != recent.winner_user_id:
                direct_reply = reply_target_id == int(recent.message_id)
                attempt_limit = answer_attempt_limit(recent.answer_spec)
                attempts_used = int(recent.attempt_counts_by_user.get(author_id, 0) or 0)
                locked_out = attempts_used >= attempt_limit
                if locked_out:
                    self._log_drop_event(
                        "drop_attempt_locked_out",
                        level=logging.INFO,
                        guild_id=recent.guild_id,
                        channel_id=recent.channel_id,
                        exposure_id=recent.exposure_id,
                        author_id=author_id,
                        attempt_limit=attempt_limit,
                        attempts_used=attempts_used,
                    )
                elif is_answer_attempt(recent.answer_spec, candidate_content, direct_reply=direct_reply) and judge_answer(
                    recent.answer_spec, candidate_content
                ):
                    if (
                        recent.winner_user_id is None
                        and not recent.participation_finalized
                        and message_created_at <= recent.score_deadline_at
                    ):
                        claimed = await self.store.claim_timed_out_exposure_winner(
                            int(recent.exposure_id),
                            resolved_at=now,
                            winner_user_id=author_id,
                        )
                        if claimed is not None:
                            recent.winner_user_id = author_id
                            recent.participation_finalized = True
                            recent.participant_user_ids = sorted({*recent.participant_user_ids, author_id})
                            recovery_payload = {
                                "guild_id": recent.guild_id,
                                "channel_id": recent.channel_id,
                                "exposure_id": recent.exposure_id,
                                "occurred_at": recent.asked_at,
                                "category": recent.category,
                                "difficulty": recent.difficulty,
                                "answer_spec": dict(recent.answer_spec),
                                "participant_ids": list(recent.participant_user_ids),
                                "winner_user_id": author_id,
                                "timeout_result_message_id": recent.timeout_result_message_id,
                            }
                            self._log_drop_event(
                                "drop_timeout_recovered",
                                level=logging.INFO,
                                guild_id=recent.guild_id,
                                channel_id=recent.channel_id,
                                exposure_id=recent.exposure_id,
                                author_id=author_id,
                                message_created_at=message_created_at,
                                score_deadline_at=recent.score_deadline_at,
                            )
                    if recovery_payload is None:
                        if author_id not in recent.acknowledged_user_ids and len(recent.acknowledged_user_ids) < QUESTION_DROP_LATE_CORRECT_MAX_ACKS:
                            recent.acknowledged_user_ids.add(author_id)
                            late_ack_payload = {
                                "category": recent.category,
                                "winner_user_id": recent.winner_user_id,
                            }
                            self._log_drop_event(
                                "drop_race_lost" if recent.winner_user_id is not None else "drop_correct_but_late",
                                level=logging.INFO,
                                guild_id=recent.guild_id,
                                channel_id=recent.channel_id,
                                exposure_id=recent.exposure_id,
                                author_id=author_id,
                                message_created_at=message_created_at,
                                score_deadline_at=recent.score_deadline_at,
                            )
        for record in due_finalizations:
            await self._finalize_timeout_context(record)
        if recovery_payload is None and late_ack_payload is not None:
            return await self._send_late_correct_ack(
                message,
                category=str(late_ack_payload["category"]),
                winner_user_id=late_ack_payload.get("winner_user_id"),
            )
        if recovery_payload is None:
            return False
        await self._try_add_answer_reaction(message, state_emoji("correct"))
        updates = await self._record_participation_batch(
            guild_id=int(recovery_payload["guild_id"]),
            exposure_id=int(recovery_payload["exposure_id"]),
            occurred_at=recovery_payload["occurred_at"],
            category=str(recovery_payload["category"]),
            difficulty=int(recovery_payload["difficulty"]),
            participant_ids=list(recovery_payload["participant_ids"]),
            winner_user_id=int(recovery_payload["winner_user_id"]),
            persist_events=True,
            record_profiles=True,
        )
        update = updates.get(author_id, {}) if isinstance(updates, dict) else {}
        role_events = []
        if isinstance(update, dict) and update:
            role_events = await self._grant_progression_rewards(
                guild=message.guild,
                member=message.author,
                fallback_channel=message.channel,
                category=str(recovery_payload["category"]),
                update=update,
            )
        result_embed = await self._build_solve_embed(
            winner=message.author,
            category=str(recovery_payload["category"]),
            answer_spec=dict(recovery_payload["answer_spec"]),
            update=update,
            role_events=role_events,
            fallback_points=answer_points_for_difficulty(int(recovery_payload["difficulty"])),
        )
        edited = await self._try_edit_timeout_result_message(
            message.channel,
            timeout_message_id=recovery_payload.get("timeout_result_message_id"),
            winner=message.author,
            answer_spec=dict(recovery_payload["answer_spec"]),
        )
        if not edited:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=result_embed,
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
        return True

    async def handle_message(self, message: discord.Message) -> bool:
        if not self.storage_ready or message.guild is None:
            return False
        now = ge.now_utc()
        message_created_at = self._message_created_at_utc(message, fallback=now)
        reply_target_id = _extract_reply_target_id(message)
        candidate_content = self._sanitize_answer_candidate(message.content)
        author_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        self._log_drop_event(
            "drop_attempt_received",
            guild_id=message.guild.id,
            channel_id=message.channel.id,
            author_id=author_id,
            message_id=getattr(message, "id", None),
            message_created_at=message_created_at,
        )
        result_payload: dict[str, Any] | None = None
        attempt_reaction: str | None = None
        feedback_payload: dict[str, Any] | None = None
        check_recent = False
        late_ack_payload: dict[str, Any] | None = None
        expire_request: dict[str, Any] | None = None
        async with self._lock:
            due_finalizations = self._collect_due_timeout_finalizations_locked(now=now)
            current = self._active_drops.get((message.guild.id, message.channel.id))
            if current is None:
                check_recent = True
            elif self._channel_has_live_party_session(current["guild_id"], current["channel_id"]):
                expire_request = {"record": dict(current), "timed_out": False, "announce": False, "delete_post_message": True}
            else:
                direct_reply = reply_target_id == int(current["message_id"])
                is_attempt = is_answer_attempt(current["answer_spec"], candidate_content, direct_reply=direct_reply)
                exposure_id = int(current["exposure_id"])
                attempt_limit = answer_attempt_limit(current["answer_spec"])
                attempt_counts = self._attempt_counts_by_user.setdefault(exposure_id, dict(current.get("attempt_counts_by_user", {}) or {}))
                attempts_used = int(attempt_counts.get(author_id, 0) or 0)
                locked_out = author_id > 0 and attempts_used >= attempt_limit
                self._log_drop_event(
                    "drop_attempt_window_check",
                    guild_id=current["guild_id"],
                    channel_id=current["channel_id"],
                    exposure_id=exposure_id,
                    author_id=author_id,
                    is_attempt=is_attempt,
                    locked_out=locked_out,
                    attempt_limit=attempt_limit,
                    attempts_used=attempts_used,
                    live=self._active_drop_is_live(current, now=now),
                    message_created_at=message_created_at,
                    score_deadline_at=self._active_drop_expires_at(current),
                    close_after_at=self._active_drop_close_after_at(current),
                )
                if not is_attempt:
                    if not self._active_drop_is_live(current, now=now):
                        expire_request = {"record": dict(current), "timed_out": True, "announce": True, "delete_post_message": False}
                elif locked_out:
                    self._log_drop_event(
                        "drop_attempt_locked_out",
                        level=logging.INFO,
                        guild_id=current["guild_id"],
                        channel_id=current["channel_id"],
                        exposure_id=exposure_id,
                        author_id=author_id,
                        attempt_limit=attempt_limit,
                        attempts_used=attempts_used,
                    )
                    if not self._active_drop_is_live(current, now=now):
                        expire_request = {"record": dict(current), "timed_out": True, "announce": True, "delete_post_message": False}
                elif not self._message_is_within_answer_window(current, message_created_at=message_created_at):
                    correct_but_late = judge_answer(current["answer_spec"], candidate_content)
                    if correct_but_late:
                        late_ack_payload = {"category": str(current["category"]), "winner_user_id": None}
                        self._log_drop_event(
                            "drop_correct_but_late",
                            level=logging.INFO,
                            guild_id=current["guild_id"],
                            channel_id=current["channel_id"],
                            exposure_id=exposure_id,
                            author_id=author_id,
                            message_created_at=message_created_at,
                            score_deadline_at=self._active_drop_expires_at(current),
                        )
                    if not self._active_drop_is_live(current, now=now):
                        expire_request = {"record": dict(current), "timed_out": True, "announce": True, "delete_post_message": False}
                    else:
                        self._log_drop_event(
                            "drop_correct_but_outside_grace",
                            level=logging.INFO,
                            guild_id=current["guild_id"],
                            channel_id=current["channel_id"],
                            exposure_id=exposure_id,
                            author_id=author_id,
                            message_created_at=message_created_at,
                            close_after_at=self._active_drop_close_after_at(current),
                        )
                else:
                    correct = judge_answer(current["answer_spec"], candidate_content)
                    self._log_drop_event(
                        "drop_attempt_judge",
                        guild_id=current["guild_id"],
                        channel_id=current["channel_id"],
                        exposure_id=exposure_id,
                        author_id=author_id,
                        correct=correct,
                        attempt_limit=attempt_limit,
                        attempts_used=attempts_used,
                    )
                    if not correct:
                        attempt_reaction = state_emoji("wrong")
                    participants = self._attempted_users.setdefault(exposure_id, set(current.get("participant_user_ids", []) or []))
                    first_attempt = author_id not in participants
                    if first_attempt:
                        participants.add(author_id)
                    participant_ids = sorted(participants)
                    current["participant_user_ids"] = participant_ids
                    if correct:
                        participant_ids = sorted(participants)
                        claimed = await self._claim_and_close_active_drop(
                            current,
                            resolved_at=now,
                            winner_user_id=author_id,
                        )
                        if claimed is None:
                            check_recent = True
                            self._log_drop_event(
                                "drop_race_lost",
                                level=logging.INFO,
                                guild_id=current["guild_id"],
                                channel_id=current["channel_id"],
                                exposure_id=current["exposure_id"],
                                author_id=author_id,
                            )
                        else:
                            self._remember_recently_resolved_drop(
                                claimed,
                                winner_user_id=author_id,
                                resolved_at=now,
                                participant_user_ids=participant_ids,
                                attempt_counts_by_user=dict(current.get("attempt_counts_by_user", {}) or {}),
                                participation_finalized=True,
                            )
                            self._clear_active_drop_runtime_state(exposure_id)
                            attempt_reaction = state_emoji("correct")
                            result_payload = {
                                "guild_id": claimed["guild_id"],
                                "channel_id": claimed["channel_id"],
                                "occurred_at": claimed["asked_at"],
                                "category": claimed["category"],
                                "difficulty": int(claimed["difficulty"]),
                                "answer_spec": claimed["answer_spec"],
                                "exposure_id": exposure_id,
                                "participant_ids": participant_ids,
                                "winner_user_id": author_id,
                            }
                            self._log_drop_event(
                                "drop_attempt_claim",
                                level=logging.INFO,
                                guild_id=claimed["guild_id"],
                                channel_id=claimed["channel_id"],
                                exposure_id=claimed["exposure_id"],
                                author_id=author_id,
                            )
                    else:
                        attempt_counts[author_id] = attempts_used + 1
                        current["attempt_counts_by_user"] = dict(sorted(attempt_counts.items()))
                        attempts_left = max(0, attempt_limit - int(current["attempt_counts_by_user"].get(author_id, 0) or 0))
                        await self.store.update_active_drop_progress(
                            current["guild_id"],
                            current["channel_id"],
                            participant_user_ids=participant_ids,
                            attempt_counts_by_user=current["attempt_counts_by_user"],
                        )
                        feedback_payload = {
                            "tone_mode": current.get("tone_mode", "clean"),
                            "attempts_left": attempts_left,
                        }
        for record in due_finalizations:
            await self._finalize_timeout_context(record)
        if expire_request is not None:
            await self._expire_drop(
                expire_request["record"],
                timed_out=bool(expire_request["timed_out"]),
                announce=bool(expire_request["announce"]),
                delete_post_message=bool(expire_request["delete_post_message"]),
            )
        if late_ack_payload is not None:
            return await self._send_late_correct_ack(
                message,
                category=str(late_ack_payload["category"]),
                winner_user_id=late_ack_payload.get("winner_user_id"),
            )
        if check_recent:
            return await self._acknowledge_late_correct_answer(
                message,
                now=ge.now_utc(),
                reply_target_id=reply_target_id,
                message_created_at=message_created_at,
            )
        if attempt_reaction:
            await self._try_add_answer_reaction(message, attempt_reaction)
        if result_payload is not None:
            updates = await self._record_participation_batch(
                guild_id=result_payload["guild_id"],
                exposure_id=result_payload["exposure_id"],
                occurred_at=result_payload["occurred_at"],
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
        if feedback_payload is not None:
            title = f"{state_emoji('wrong')} Not Yet"
            if int(feedback_payload["attempts_left"]) <= 0:
                title = f"{state_emoji('wrong')} Out of Attempts"
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        title,
                        _attempt_feedback_line(
                            str(feedback_payload.get("tone_mode", "clean")),
                            attempts_left=int(feedback_payload["attempts_left"]),
                        ),
                        tone="warning",
                        footer="Babblebox Question Drops",
                    ),
                    delete_after=6.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        return False

    def _clear_active_drop_runtime_state(self, exposure_id: int):
        self._attempted_users.pop(exposure_id, None)
        self._attempt_counts_by_user.pop(exposure_id, None)

    def _build_participation_results(
        self,
        *,
        category: str,
        difficulty: int,
        participant_ids: list[int],
        winner_user_id: int | None,
    ) -> list[dict[str, Any]]:
        participant_set = {user_id for user_id in participant_ids if isinstance(user_id, int) and user_id > 0}
        if winner_user_id is not None and winner_user_id > 0:
            participant_set.add(winner_user_id)
        points = answer_points_for_difficulty(int(difficulty))
        return [
            {
                "user_id": user_id,
                "category": category,
                "correct": user_id == winner_user_id,
                "points": points if user_id == winner_user_id else 0,
            }
            for user_id in sorted(participant_set)
        ]

    async def _record_participation_events(
        self,
        *,
        guild_id: int,
        exposure_id: int,
        occurred_at: datetime | str,
        category: str,
        results: list[dict[str, Any]],
    ):
        if not results:
            return
        await self.store.record_participation_events(
            [
                {
                    "guild_id": guild_id,
                    "exposure_id": exposure_id,
                    "user_id": int(result["user_id"]),
                    "occurred_at": occurred_at,
                    "category": category,
                    "correct": bool(result["correct"]),
                    "points_awarded": int(result["points"]),
                }
                for result in results
            ]
        )

    async def _record_profile_results(self, results: list[dict[str, Any]], *, guild_id: int) -> dict[int, dict[str, Any]]:
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return {}
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

    async def _record_participation_batch(
        self,
        *,
        guild_id: int,
        exposure_id: int,
        occurred_at: datetime | str,
        category: str,
        difficulty: int,
        participant_ids: list[int],
        winner_user_id: int | None,
        persist_events: bool = True,
        record_profiles: bool = True,
    ) -> dict[int, dict[str, Any]]:
        results = self._build_participation_results(
            category=category,
            difficulty=int(difficulty),
            participant_ids=participant_ids,
            winner_user_id=winner_user_id,
        )
        if persist_events:
            await self._record_participation_events(
                guild_id=guild_id,
                exposure_id=exposure_id,
                occurred_at=occurred_at,
                category=category,
                results=results,
            )
        if not record_profiles or not results:
            return {}
        return await self._record_profile_results(results, guild_id=guild_id)

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

    async def _record_unlock_history(
        self,
        *,
        guild_id: int,
        user_id: int,
        scope_type: str,
        scope_key: str,
        tier: int,
        role_id: int,
    ) -> bool:
        profile_service = self._profile_service()
        if profile_service is None:
            return False
        await profile_service.store.save_question_drop_unlock(
            {
                "guild_id": int(guild_id),
                "user_id": int(user_id),
                "scope_type": str(scope_type),
                "scope_key": str(scope_key),
                "tier": int(tier),
                "role_id": int(role_id),
                "granted_at": ge.now_utc(),
            }
        )
        return True

    def _role_manage_block_code(self, guild: discord.Guild, role) -> str | None:
        bot_member = self._bot_member_for_guild(guild)
        if bot_member is None:
            return "bot_unavailable"
        bot_permissions = getattr(bot_member, "guild_permissions", None)
        if not bool(getattr(bot_permissions, "manage_roles", False)):
            return "missing_manage_roles"
        bot_top_role = getattr(bot_member, "top_role", None)
        bot_top_position = int(getattr(bot_top_role, "position", 0) or 0)
        role_position = int(getattr(role, "position", 0) or 0)
        if bot_top_position and role_position >= bot_top_position:
            return "hierarchy_blocked"
        return None

    async def _attempt_add_role(self, guild: discord.Guild, member, role, *, reason: str) -> str:
        if role is None:
            return "missing_role"
        if self._member_has_role(member, int(getattr(role, "id", 0) or 0)):
            return "already_has"
        add_roles = getattr(member, "add_roles", None)
        if not callable(add_roles):
            return "member_unavailable"
        block_code = self._role_manage_block_code(guild, role)
        if block_code is not None:
            return block_code
        try:
            await add_roles(role, reason=reason)
        except discord.Forbidden:
            return "forbidden"
        except discord.HTTPException:
            return "http_error"
        return "added"

    async def _attempt_remove_role(self, guild: discord.Guild, member, role, *, reason: str) -> str:
        if role is None:
            return "missing_role"
        if not self._member_has_role(member, int(getattr(role, "id", 0) or 0)):
            return "already_missing"
        remove_roles = getattr(member, "remove_roles", None)
        if not callable(remove_roles):
            return "member_unavailable"
        block_code = self._role_manage_block_code(guild, role)
        if block_code is not None:
            return block_code
        try:
            await remove_roles(role, reason=reason)
        except discord.Forbidden:
            return "forbidden"
        except discord.HTTPException:
            return "http_error"
        return "removed"

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
        announcement_template: str | None = None,
    ) -> dict[str, Any] | None:
        if not hasattr(guild, "get_role"):
            return None
        role = guild.get_role(int(role_id))
        if role is None:
            return None
        status = await self._attempt_add_role(
            guild,
            member,
            role,
            reason=f"Babblebox Question Drops {scope_type} milestone reached",
        )
        if status not in {"added", "already_has"}:
            return None
        if not await self._record_unlock_history(
            guild_id=guild.id,
            user_id=int(getattr(member, "id", 0) or 0),
            scope_type=scope_type,
            scope_key=scope_key,
            tier=tier,
            role_id=role_id,
        ):
            return None
        event = {
            "scope_type": scope_type,
            "scope_key": scope_key,
            "scope_label": scope_label,
            "tier": int(tier),
            "threshold": int(threshold),
            "role_id": int(role_id),
            "role": role,
            "member": member,
            "user_id": int(getattr(member, "id", 0) or 0),
            "member_mention": getattr(member, "mention", f"<@{getattr(member, 'id', 0)}>"),
            "announcement_channel_id": announcement_channel_id,
            "announcement_template": announcement_template,
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
        preference = await self._member_role_grant_preference(guild_id=guild.id, user_id=member_id)
        role_grants_enabled = bool(preference.get("role_grants_enabled", True))
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
                if not role_grants_enabled:
                    await self._record_unlock_history(
                        guild_id=guild.id,
                        user_id=member_id,
                        scope_type="category",
                        scope_key=category_id,
                        tier=tier,
                        role_id=role_id,
                    )
                    unlocks.append(
                        {
                            "scope_type": "category",
                            "scope_key": category_id,
                            "tier": tier,
                            "role_id": role_id,
                        }
                    )
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
                    announcement_template=self._effective_announcement_template(
                        category_config,
                        scope_type="category",
                        tier=tier,
                    )[0],
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
                if not role_grants_enabled:
                    await self._record_unlock_history(
                        guild_id=guild.id,
                        user_id=member_id,
                        scope_type="scholar",
                        scope_key="global",
                        tier=tier,
                        role_id=role_id,
                    )
                    unlocks.append(
                        {
                            "scope_type": "scholar",
                            "scope_key": "global",
                            "tier": tier,
                            "role_id": role_id,
                        }
                    )
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
                    announcement_template=self._effective_announcement_template(
                        scholar_config,
                        scope_type="scholar",
                        tier=tier,
                    )[0],
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
        title = f"{state_emoji('correct')} {category_label(category_id)} Solved"
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
                f"{getattr(winner, 'mention', ge.display_name_of(winner))} locked in **{answer}** for **{int(update.get('points_awarded', 0) or fallback_points)}** points."
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
            await self._maybe_post_due_digests()
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
        participation_cutoff = ge.now_utc() - timedelta(days=QUESTION_DROP_PARTICIPATION_RETENTION_DAYS)
        await self.store.prune_participation_events(before=participation_cutoff, limit=500)
        self._next_prune_at = now + timedelta(seconds=QUESTION_DROP_PRUNE_INTERVAL_SECONDS)

    def _coerce_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
        coerced = datetime.fromisoformat(str(value))
        if coerced.tzinfo is None:
            coerced = coerced.replace(tzinfo=timezone.utc)
        return coerced.astimezone(timezone.utc)

    def _active_drop_is_live(self, record: dict[str, Any], *, now: datetime) -> bool:
        return self._active_drop_close_after_at(record) > now

    def _active_drop_expires_at(self, record: dict[str, Any]) -> datetime:
        return self._coerce_datetime(record.get("expires_at"))

    def _active_drop_close_after_at(self, record: dict[str, Any]) -> datetime:
        close_after_raw = record.get("close_after_at")
        if close_after_raw is not None:
            return self._coerce_datetime(close_after_raw)
        return self._active_drop_expires_at(record) + timedelta(seconds=QUESTION_DROP_CLOSE_BUFFER_SECONDS)

    def _message_created_at_utc(self, message: discord.Message, *, fallback: datetime) -> datetime:
        created_at = getattr(message, "created_at", None)
        if isinstance(created_at, datetime):
            return created_at.astimezone(timezone.utc) if created_at.tzinfo else created_at.replace(tzinfo=timezone.utc)
        return fallback

    def _sanitize_answer_candidate(self, raw_content: str | None) -> str:
        content = str(raw_content or "")
        bot_id = int(getattr(getattr(self.bot, "user", None), "id", 0) or 0)
        if bot_id <= 0:
            return content
        candidate = content.lstrip()
        changed = False
        bot_mentions = (f"<@{bot_id}>", f"<@!{bot_id}>")
        while True:
            matched = next((mention for mention in bot_mentions if candidate.startswith(mention)), None)
            if matched is None:
                break
            candidate = candidate[len(matched) :].lstrip(" \t\r\n,.:;-")
            changed = True
        return candidate if changed else content

    def _message_is_within_answer_window(self, record: dict[str, Any], *, message_created_at: datetime) -> bool:
        return message_created_at <= self._active_drop_expires_at(record)

    async def _claim_and_close_active_drop(
        self,
        record: dict[str, Any],
        *,
        resolved_at: datetime,
        winner_user_id: int | None,
    ) -> dict[str, Any] | None:
        claimed = await self.store.claim_active_drop_resolution(
            int(record["guild_id"]),
            int(record["channel_id"]),
            int(record["message_id"]),
            resolved_at=resolved_at,
            winner_user_id=winner_user_id,
        )
        if claimed is None:
            return None
        self._active_drops.pop((claimed["guild_id"], claimed["channel_id"]), None)
        return claimed

    async def _expire_due_drops(self):
        now = ge.now_utc()
        await self._finalize_due_recent_timeouts(now=now)
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
        now = ge.now_utc()
        async with self._lock:
            due_finalizations = self._collect_due_timeout_finalizations_locked(now=now)
            current = self._active_drops.get((int(record["guild_id"]), int(record["channel_id"])))
            target = current if current is not None and int(current["message_id"]) == int(record["message_id"]) else record
            close_after_at = self._active_drop_close_after_at(target)
            resolved_at = close_after_at if timed_out else now
            allow_recovery = timed_out and now <= close_after_at + timedelta(seconds=QUESTION_DROP_LATE_CORRECT_WINDOW_SECONDS)
            claimed = await self._claim_and_close_active_drop(target, resolved_at=resolved_at, winner_user_id=None)
            if claimed is None:
                participant_ids: list[int] = []
            else:
                exposure_id = int(claimed["exposure_id"])
                participant_ids = sorted(self._attempted_users.get(exposure_id, set(claimed.get("participant_user_ids", []) or [])))
                attempt_counts_by_user = dict(
                    sorted(self._attempt_counts_by_user.get(exposure_id, dict(claimed.get("attempt_counts_by_user", {}) or {})).items())
                )
                self._clear_recently_resolved_drop(claimed["guild_id"], claimed["channel_id"])
                self._clear_active_drop_runtime_state(exposure_id)
                if timed_out and allow_recovery:
                    self._remember_recently_resolved_drop(
                        claimed,
                        winner_user_id=None,
                        resolved_at=resolved_at,
                        participant_user_ids=participant_ids,
                        attempt_counts_by_user=attempt_counts_by_user,
                        participation_finalized=False,
                    )
                    self._log_drop_event(
                        "drop_timeout_claim",
                        level=logging.INFO,
                        guild_id=claimed["guild_id"],
                        channel_id=claimed["channel_id"],
                        exposure_id=claimed["exposure_id"],
                        participant_count=len(participant_ids),
                        score_deadline_at=self._active_drop_expires_at(claimed),
                        close_after_at=self._active_drop_close_after_at(claimed),
                    )
        for due in due_finalizations:
            await self._finalize_timeout_context(due)
        if claimed is None:
            return
        if delete_post_message:
            await self._delete_message_if_exists(claimed["channel_id"], claimed.get("message_id"))
        if participant_ids:
            await self._record_participation_batch(
                guild_id=claimed["guild_id"],
                exposure_id=int(claimed["exposure_id"]),
                occurred_at=claimed["asked_at"],
                category=claimed["category"],
                difficulty=int(claimed["difficulty"]),
                participant_ids=participant_ids,
                winner_user_id=None,
                persist_events=True,
                record_profiles=not timed_out or not allow_recovery,
            )
        if not announce:
            return
        channel = self.bot.get_channel(claimed["channel_id"]) if hasattr(self.bot, "get_channel") else None
        if channel is None:
            return
        answer = render_answer_summary(claimed["answer_spec"])
        title = f"{state_emoji('timeout')} Time's Up" if timed_out else f"{state_emoji('result')} Drop Closed"
        with contextlib.suppress(discord.HTTPException):
            timeout_message = await channel.send(
                embed=ge.make_status_embed(
                    title,
                    f"No in-time solve. Answer: **{answer}**." if timed_out else f"Drop closed. Answer: **{answer}**.",
                    tone="info",
                    footer="Babblebox Question Drops",
                )
            )
            if timed_out:
                self._set_recent_timeout_message_id(
                    claimed["guild_id"],
                    claimed["channel_id"],
                    exposure_id=int(claimed["exposure_id"]),
                    message_id=getattr(timeout_message, "id", None),
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
        self._clear_recently_resolved_drop(guild_id, channel_id)
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
            title=f"{category_label_with_emoji(variant.category)} Question Drop",
            description=prompt,
            color=ge.EMBED_THEME["accent"],
            timestamp=asked_at,
        )
        embed.add_field(
            name=f"{state_emoji('round')} Round",
            value=(
                f"{state_emoji('difficulty')} Difficulty: **{QUESTION_DROP_DIFFICULTY_LABELS.get(variant.difficulty, 'Easy')}**\n"
                f"{state_emoji('window')} Window: **{int(config.get('answer_window_seconds', 60))} seconds**"
            ),
            inline=False,
        )
        answer_lane = render_answer_instruction(variant.answer_spec)
        embed.add_field(
            name=f"{answer_type_emoji(str(variant.answer_spec.get('type') or 'text'))} Answering",
            value=answer_lane,
            inline=False,
        )
        embed = ge.style_embed(embed, footer="Babblebox Question Drops | First correct answer wins")
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
            "close_after_at": asked_at
            + timedelta(seconds=int(config.get("answer_window_seconds", 60)) + QUESTION_DROP_CLOSE_BUFFER_SECONDS),
            "slot_key": slot_key,
            "tone_mode": config.get("tone_mode", "clean"),
            "participant_user_ids": [],
            "attempt_counts_by_user": {},
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
        self._attempted_users[exposure_id] = set()
        self._attempt_counts_by_user[exposure_id] = {}

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
        family_counts = Counter()
        source_counts = Counter()
        category_concept_counts = Counter()
        category_variant_capacity = Counter()
        candidate_categories = {candidate.category for candidate in candidates}
        same_day_concepts: set[str] = set()
        seen_category_concepts: set[tuple[str, str]] = set()
        now = ge.now_utc()
        slot_day_key = str(slot_key).split(":", 1)[0]
        recent_records: list[tuple[datetime, dict[str, Any]]] = []
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
            recent_records.append((asked_at, record))
            recent_by_concept.setdefault(record["concept_id"], asked_at)
            recent_by_variant.setdefault(record["variant_hash"], asked_at)
            if str(record.get("slot_key") or "").split(":", 1)[0] == slot_day_key:
                same_day_concepts.add(str(record["concept_id"]))
            if (now - asked_at).days <= 14:
                category_counts[record["category"]] += 1
                difficulty_counts[int(record["difficulty"])] += 1
                seed = question_drop_seed_for_concept(record["concept_id"]) or {}
                family_id = str(seed.get("family_id") or "").strip()
                if family_id:
                    family_counts[family_id] += 1
                source_counts[str(seed.get("source_type", "curated"))] += 1
        recent_records.sort(key=lambda item: item[0], reverse=True)
        recent_family_window = [
            str((question_drop_seed_for_concept(record["concept_id"]) or {}).get("family_id") or "").strip()
            for _, record in recent_records[:4]
        ]
        recent_difficulty_window = [int(record["difficulty"]) for _, record in recent_records[:4]]
        scored: list[tuple[float, QuestionDropVariant]] = []
        same_day_scored: list[tuple[float, QuestionDropVariant]] = []
        generated_gap = max(0, source_counts["curated"] - source_counts["generated"])
        drop_pressure = max(0, int(config.get("drops_per_day", 2) or 2) - 2)
        category_floor = min((category_counts[category] for category in candidate_categories), default=0)
        target_mix = _difficulty_mix_for(config)
        recent_total = sum(difficulty_counts.values())
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
            target_share = float(target_mix.get(int(variant.difficulty), 0)) / 100.0
            expected_count = recent_total * target_share
            difficulty_balance = 4.5 + ((expected_count - difficulty_counts[int(variant.difficulty)]) * 1.15)
            if recent_total < 6:
                difficulty_balance += target_share * 1.5
            if int(variant.difficulty) == 3 and recent_difficulty_window[:2] == [3, 3]:
                difficulty_balance -= 9.0
            elif int(variant.difficulty) == 3 and recent_difficulty_window[:1] == [3]:
                difficulty_balance -= 2.5
            family_balance = 2.5 - min(family_counts[variant.family_id], 4) * 0.9
            for index, family_id in enumerate(recent_family_window):
                if family_id and family_id == variant.family_id:
                    family_balance -= max(2.0, 5.5 - index)
            if variant.source_type == "generated":
                source_balance = 2.5 + min(generated_gap * 0.75, 4.0) + min(drop_pressure * 0.5, 3.0)
                pool_depth_bonus = (min(category_variant_capacity[variant.category], 12) * 0.16) + min(drop_pressure * 0.2, 1.2)
            else:
                source_balance = 1.5 - min(generated_gap * 0.25, 2.0) - min(drop_pressure * 0.35, 2.0)
                pool_depth_bonus = min(category_variant_capacity[variant.category], 12) * 0.06
            jitter = (int(build_variant_hash(slot_key, variant.variant_hash), 16) % 1000) / 1000.0
            score = freshness + variant_freshness + category_balance + difficulty_balance + family_balance + source_balance + pool_depth_bonus + jitter
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
