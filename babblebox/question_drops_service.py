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
    render_answer_summary,
    validate_content_pack,
)
from babblebox.question_drops_store import (
    QuestionDropsStorageUnavailable,
    QuestionDropsStore,
    default_question_drops_config,
    normalize_active_drop,
    normalize_question_drops_config,
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
                "Not quite. The box remains unimpressed.",
                "Swing and a miss. Someone else can snag it.",
                "Close in spirit, wrong in reality.",
            )
        )
    if tone_mode == "roast-light":
        return random.choice(
            (
                "Respectfully: that answer was brave, not correct.",
                "That guess had confidence. Accuracy is still pending.",
                "The question asked for a winner, not a plot twist.",
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
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        async with self._lock:
            current = dict(self.get_config(guild_id))
            if enabled is not None:
                current["enabled"] = bool(enabled)
            if drops_per_day is not None:
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
                categories = set()
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

    async def get_status_snapshot(self, guild: discord.Guild) -> QuestionDropStatusSnapshot:
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
        enabled_categories = config.get("enabled_categories") or list(QUESTION_DROP_CATEGORIES)
        enabled_category_labels = [QUESTION_DROP_CATEGORY_LABELS.get(category, category.title()) for category in enabled_categories]
        description = "Compact scheduled prompts with offline content, cautious repeat control, and same-channel conflict blocking."
        if not config.get("enabled"):
            description = "Question Drops are currently disabled for this server."
        embed = discord.Embed(
            title="Question Drops",
            description=description,
            color=ge.EMBED_THEME["accent"],
        )
        embed.add_field(
            name="Status",
            value=(
                f"Enabled: **{'Yes' if config.get('enabled') else 'No'}**\n"
                f"Drops/day: **{config.get('drops_per_day', 2)}**\n"
                f"Window: **{config.get('active_start_hour', 10):02d}:00-{config.get('active_end_hour', 22):02d}:00**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Style",
            value=(
                f"Timezone: **{config.get('timezone', 'UTC')}**\n"
                f"Answer window: **{config.get('answer_window_seconds', 60)}s**\n"
                f"Tone: **{str(config.get('tone_mode', 'clean')).title()}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Routing",
            value=(
                f"Activity gate: **{str(config.get('activity_gate', 'light')).title()}**\n"
                f"Configured channels: **{len(snapshot.enabled_channel_mentions)}**\n"
                f"Active drops now: **{snapshot.active_drop_count}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Categories",
            value=", ".join(enabled_category_labels),
            inline=False,
        )
        if snapshot.enabled_channel_mentions:
            embed.add_field(name="Channels", value="\n".join(snapshot.enabled_channel_mentions[:8]), inline=False)
        if snapshot.next_slot_at is not None:
            embed.add_field(
                name="Next Slot",
                value=f"{ge.format_timestamp(snapshot.next_slot_at, 'R')} ({ge.format_timestamp(snapshot.next_slot_at, 'f')})",
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Question Drops | Compact, offline, channel-safe")

    def build_stats_embed(self, user: discord.abc.User, summary: dict[str, Any]) -> discord.Embed:
        profile = summary["profile"]
        categories = summary["top_categories"]
        embed = discord.Embed(
            title="Question Drops Stats",
            description=f"Performance snapshot for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["info"],
        )
        participations = int(profile.get("question_drop_attempts", 0) or 0)
        correct = int(profile.get("question_drop_correct", 0) or 0)
        accuracy = (correct / participations * 100.0) if participations else 0.0
        embed.add_field(
            name="Overall",
            value=(
                f"Points: **{int(profile.get('question_drop_points', 0) or 0)}**\n"
                f"Solves: **{correct} / {participations} participated drops**\n"
                f"Accuracy: **{accuracy:.0f}%**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Streak",
            value=(
                f"Current: **{int(profile.get('question_drop_current_streak', 0) or 0)}**\n"
                f"Best: **{int(profile.get('question_drop_best_streak', 0) or 0)}**"
            ),
            inline=True,
        )
        if categories:
            lines = []
            for entry in categories[:4]:
                label = QUESTION_DROP_CATEGORY_LABELS.get(entry["category"], entry["category"].title())
                lines.append(
                    f"**{label}**: {int(entry.get('correct_count', 0) or 0)} correct, {int(entry.get('points', 0) or 0)} pts"
                )
            embed.add_field(name="Top Categories", value="\n".join(lines), inline=False)
        return ge.style_embed(embed, footer="Babblebox Question Drops | Aggregates only, no answer archive")

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
            if first_attempt:
                participants.add(message.author.id)
                await self.store.update_active_drop_participants(current["guild_id"], current["channel_id"], sorted(participants))
                current["participant_user_ids"] = sorted(participants)
            if judge_answer(current["answer_spec"], message.content):
                participant_ids = sorted(participants | {message.author.id})
                await self.store.resolve_exposure(exposure_id, resolved_at=now, winner_user_id=message.author.id)
                await self.store.delete_active_drop(current["guild_id"], current["channel_id"])
                self._active_drops.pop((current["guild_id"], current["channel_id"]), None)
                self._wrong_feedback_users.pop(exposure_id, None)
                self._wrong_feedback_count.pop(exposure_id, None)
                self._attempted_users.pop(exposure_id, None)
                result_payload = {
                    "category": current["category"],
                    "difficulty": int(current["difficulty"]),
                    "participant_ids": participant_ids,
                    "winner_user_id": message.author.id,
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
            await self._record_participation_batch(
                category=result_payload["category"],
                difficulty=result_payload["difficulty"],
                participant_ids=result_payload["participant_ids"],
                winner_user_id=result_payload["winner_user_id"],
            )
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Correct",
                        f"{message.author.mention} solved the drop and earned **{answer_points_for_difficulty(result_payload['difficulty'])}** points.",
                        tone="success",
                        footer=f"Babblebox Question Drops | {QUESTION_DROP_CATEGORY_LABELS.get(result_payload['category'], result_payload['category'].title())}",
                    ),
                    allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            return True
        if feedback_line:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(
                    embed=ge.make_status_embed(
                        "Nope",
                        feedback_line,
                        tone="warning",
                        footer="Babblebox Question Drops",
                    ),
                    delete_after=6.0,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        return False

    async def _record_participation_batch(
        self,
        *,
        category: str,
        difficulty: int,
        participant_ids: list[int],
        winner_user_id: int | None,
    ):
        profile_service = getattr(self.bot, "profile_service", None)
        if profile_service is None or not getattr(profile_service, "storage_ready", False):
            return
        participant_set = {user_id for user_id in participant_ids if isinstance(user_id, int) and user_id > 0}
        if winner_user_id is not None and winner_user_id > 0:
            participant_set.add(winner_user_id)
        points = answer_points_for_difficulty(int(difficulty))
        for user_id in sorted(participant_set):
            await profile_service.record_question_drop_result(
                user_id,
                category=category,
                correct=user_id == winner_user_id,
                points=points if user_id == winner_user_id else 0,
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
        self._wrong_feedback_users.pop(exposure_id, None)
        self._wrong_feedback_count.pop(exposure_id, None)
        self._attempted_users.pop(exposure_id, None)
        if delete_post_message:
            await self._delete_message_if_exists(record["channel_id"], record.get("message_id"))
        if participant_ids:
            await self._record_participation_batch(
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
        title = "Time's Up" if timed_out else "Question Closed"
        with contextlib.suppress(discord.HTTPException):
            await channel.send(
                embed=ge.make_status_embed(
                    title,
                    f"No clean solve this round. The answer was **{answer}**.",
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
        answer_lane = "Reply to this drop or send a clean standalone answer."
        if variant.answer_spec.get("type") == "multiple_choice":
            answer_lane = "Reply to this drop or send the option text. Matching letters like `C` or `option c` also work."
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
        allowed_categories = set(config.get("enabled_categories") or QUESTION_DROP_CATEGORIES)
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
        seen_category_concepts: set[tuple[str, str]] = set()
        now = ge.now_utc()
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
            if (now - asked_at).days <= 14:
                category_counts[record["category"]] += 1
                difficulty_counts[int(record["difficulty"])] += 1
                seed = question_drop_seed_for_concept(record["concept_id"]) or {}
                source_counts[str(seed.get("source_type", "curated"))] += 1
        scored: list[tuple[float, QuestionDropVariant]] = []
        for variant in candidates:
            concept_cooldown_days, variant_cooldown_days, preferred_concept_days, preferred_variant_days = self._repeat_windows_for_variant(
                variant,
                category_variant_capacity=category_variant_capacity,
                category_concept_counts=category_concept_counts,
            )
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
                freshness -= (preferred_concept_days - days_since_concept) * (4.0 if variant.source_type == "curated" else 2.0)
            if days_since_variant is not None and days_since_variant < preferred_variant_days:
                variant_freshness -= (preferred_variant_days - days_since_variant) * (3.0 if variant.source_type == "curated" else 1.5)
            category_balance = 8.0 - category_counts[variant.category]
            difficulty_balance = 5.0 - difficulty_counts[int(variant.difficulty)]
            source_balance = 3.0 - source_counts[variant.source_type]
            pool_depth_bonus = min(category_variant_capacity[variant.category], 12) * (0.15 if variant.source_type == "generated" else 0.08)
            jitter = (int(build_variant_hash(slot_key, variant.variant_hash), 16) % 1000) / 1000.0
            score = freshness + variant_freshness + category_balance + difficulty_balance + source_balance + pool_depth_bonus + jitter
            scored.append((score, variant))
        if not scored:
            return None
        scored.sort(key=lambda item: item[0], reverse=True)
        return scored[0][1]

    async def _next_slot_for_guild(self, guild_id: int, *, config: dict[str, Any] | None = None) -> datetime | None:
        config = config or self.get_config(guild_id)
        if not config.get("enabled"):
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
