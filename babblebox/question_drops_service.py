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
    iter_candidate_variants,
    judge_answer,
    normalize_answer_text,
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


QUESTION_DROP_CONCEPT_COOLDOWN_DAYS = 5
QUESTION_DROP_VARIANT_COOLDOWN_DAYS = 14
QUESTION_DROP_EXPOSURE_RETENTION_DAYS = 90
QUESTION_DROP_ACTIVITY_WINDOW_SECONDS = 90 * 60
QUESTION_DROP_SLOT_GRACE_SECONDS = 45 * 60
QUESTION_DROP_SCHEDULER_INTERVAL_SECONDS = 45.0
QUESTION_DROP_WRONG_FEEDBACK_GLOBAL_LIMIT = 2


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
        self._recent_activity: dict[tuple[int, int], datetime] = {}
        self._wrong_feedback_users: dict[int, set[int]] = defaultdict(set)
        self._wrong_feedback_count: dict[int, int] = defaultdict(int)
        self._attempted_users: dict[int, set[int]] = defaultdict(set)

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
        active_rows = await self.store.list_active_drops()
        self._active_drops = {
            (record["guild_id"], record["channel_id"]): record
            for record in active_rows
            if self._active_drop_is_live(record, now=ge.now_utc())
        }
        for record in self._active_drops.values():
            exposure_id = int(record["exposure_id"])
            self._wrong_feedback_users.setdefault(exposure_id, set())
            self._wrong_feedback_count.setdefault(exposure_id, 0)
            self._attempted_users.setdefault(exposure_id, set())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="babblebox-question-drops-scheduler")
        self._wake_event.set()
        return True

    async def close(self):
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task
        await self.store.close()

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
        description = "Compact scheduled prompts with offline content and a strict anti-repeat selector."
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
        return ge.style_embed(embed, footer="Babblebox Question Drops | Compact, offline, anti-repeat")

    def build_stats_embed(self, user: discord.abc.User, summary: dict[str, Any]) -> discord.Embed:
        profile = summary["profile"]
        categories = summary["top_categories"]
        embed = discord.Embed(
            title="Question Drops Stats",
            description=f"Performance snapshot for **{ge.display_name_of(user)}**.",
            color=ge.EMBED_THEME["info"],
        )
        attempts = int(profile.get("question_drop_attempts", 0) or 0)
        correct = int(profile.get("question_drop_correct", 0) or 0)
        accuracy = (correct / attempts * 100.0) if attempts else 0.0
        embed.add_field(
            name="Overall",
            value=(
                f"Points: **{int(profile.get('question_drop_points', 0) or 0)}**\n"
                f"Correct: **{correct} / {attempts}**\n"
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
        now = ge.now_utc()
        if not self._active_drop_is_live(active, now=now):
            await self._expire_drop(active, timed_out=True)
            return False
        answer_text = normalize_answer_text(message.content)
        if not answer_text:
            return False
        exposure_id = int(active["exposure_id"])
        profile_service = getattr(self.bot, "profile_service", None)
        async with self._lock:
            current = self._active_drops.get((message.guild.id, message.channel.id))
            if current is None or int(current["message_id"]) != int(active["message_id"]):
                return False
            if judge_answer(current["answer_spec"], message.content):
                await self.store.resolve_exposure(exposure_id, resolved_at=now, winner_user_id=message.author.id)
                await self.store.delete_active_drop(current["guild_id"], current["channel_id"])
                self._active_drops.pop((current["guild_id"], current["channel_id"]), None)
                self._wrong_feedback_users.pop(exposure_id, None)
                self._wrong_feedback_count.pop(exposure_id, None)
                self._attempted_users.pop(exposure_id, None)
                if profile_service is not None and getattr(profile_service, "storage_ready", False):
                    await profile_service.record_question_drop_result(
                        message.author.id,
                        category=current["category"],
                        correct=True,
                        points=answer_points_for_difficulty(int(current["difficulty"])),
                    )
                with contextlib.suppress(discord.HTTPException):
                    await message.channel.send(
                        embed=ge.make_status_embed(
                            "Correct",
                            f"{message.author.mention} solved the drop and earned **{answer_points_for_difficulty(int(current['difficulty']))}** points.",
                            tone="success",
                            footer=f"Babblebox Question Drops | {QUESTION_DROP_CATEGORY_LABELS.get(current['category'], current['category'].title())}",
                        ),
                        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                    )
                return True
            first_attempt = message.author.id not in self._attempted_users[exposure_id]
            if first_attempt:
                self._attempted_users[exposure_id].add(message.author.id)
                if profile_service is not None and getattr(profile_service, "storage_ready", False):
                    await profile_service.record_question_drop_result(
                        message.author.id,
                        category=current["category"],
                        correct=False,
                        points=0,
                    )
            if (
                message.author.id not in self._wrong_feedback_users[exposure_id]
                and self._wrong_feedback_count[exposure_id] < QUESTION_DROP_WRONG_FEEDBACK_GLOBAL_LIMIT
            ):
                line = _tone_failure_line(current.get("tone_mode", "clean"))
                if line:
                    self._wrong_feedback_users[exposure_id].add(message.author.id)
                    self._wrong_feedback_count[exposure_id] += 1
                    with contextlib.suppress(discord.HTTPException):
                        await message.channel.send(
                            embed=ge.make_status_embed(
                                "Nope",
                                line,
                                tone="warning",
                                footer="Babblebox Question Drops",
                            ),
                            delete_after=6.0,
                            allowed_mentions=discord.AllowedMentions.none(),
                        )
        return False

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
            await self._maybe_post_due_drops()
            await self._prune_old_exposures()
            try:
                await asyncio.wait_for(self._wake_event.wait(), timeout=QUESTION_DROP_SCHEDULER_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                continue

    async def _prune_old_exposures(self):
        if not self.storage_ready:
            return
        cutoff = ge.now_utc() - timedelta(days=QUESTION_DROP_EXPOSURE_RETENTION_DAYS)
        await self.store.prune_exposures(before=cutoff, limit=500)

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

    async def _expire_drop(self, record: dict[str, Any], *, timed_out: bool):
        exposure_id = int(record["exposure_id"])
        await self.store.resolve_exposure(exposure_id, resolved_at=ge.now_utc(), winner_user_id=None)
        await self.store.delete_active_drop(record["guild_id"], record["channel_id"])
        self._active_drops.pop((record["guild_id"], record["channel_id"]), None)
        self._wrong_feedback_users.pop(exposure_id, None)
        self._wrong_feedback_count.pop(exposure_id, None)
        self._attempted_users.pop(exposure_id, None)
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
            exposures = await self.store.list_exposures_for_guild(guild_id, limit=300)
            used_slot_keys = {record["slot_key"] for record in exposures if isinstance(record.get("slot_key"), str)}
            active_slot_keys = {
                record["slot_key"]
                for (active_guild_id, _), record in self._active_drops.items()
                if active_guild_id == guild_id
            }
            for index, local_slot in enumerate(slots):
                slot_key = _slot_key(now_local.date(), index)
                if slot_key in used_slot_keys or slot_key in active_slot_keys:
                    continue
                slot_utc = local_slot.astimezone(timezone.utc)
                if slot_utc > now or (now - slot_utc).total_seconds() > QUESTION_DROP_SLOT_GRACE_SECONDS:
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
        exposure = await self.store.insert_exposure(
            {
                "guild_id": guild_id,
                "channel_id": getattr(channel, "id", 0),
                "concept_id": variant.concept_id,
                "variant_hash": variant.variant_hash,
                "category": variant.category,
                "difficulty": variant.difficulty,
                "asked_at": asked_at,
                "resolved_at": None,
                "winner_user_id": None,
                "slot_key": slot_key,
            }
        )
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
        embed = ge.style_embed(embed, footer="Babblebox Question Drops | First correct answer scores")
        with contextlib.suppress(discord.HTTPException):
            message = await channel.send(embed=embed)
        if "message" not in locals():
            return
        record = {
            "guild_id": guild_id,
            "channel_id": getattr(channel, "id", 0),
            "message_id": message.id,
            "author_user_id": int(getattr(getattr(self.bot, "user", None), "id", 1) or 1),
            "exposure_id": int(exposure["id"]),
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
        }
        await self.store.upsert_active_drop(record)
        normalized_record = normalize_active_drop(record)
        if normalized_record is None:
            return
        self._active_drops[(guild_id, getattr(channel, "id", 0))] = normalized_record
        exposure_id = int(exposure["id"])
        self._wrong_feedback_users[exposure_id] = set()
        self._wrong_feedback_count[exposure_id] = 0
        self._attempted_users[exposure_id] = set()

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
        candidates = iter_candidate_variants(categories=allowed_categories, seed_material=_slot_seed_material(guild_id, channel_id, slot_key), variants_per_seed=2)
        if not candidates:
            return None
        recent_by_concept: dict[str, datetime] = {}
        recent_by_variant: dict[str, datetime] = {}
        category_counts = Counter()
        difficulty_counts = Counter()
        now = ge.now_utc()
        for record in exposures:
            asked_at = datetime.fromisoformat(record["asked_at"])
            if asked_at.tzinfo is None:
                asked_at = asked_at.replace(tzinfo=timezone.utc)
            recent_by_concept.setdefault(record["concept_id"], asked_at)
            recent_by_variant.setdefault(record["variant_hash"], asked_at)
            if (now - asked_at).days <= 14:
                category_counts[record["category"]] += 1
                difficulty_counts[int(record["difficulty"])] += 1
        scored: list[tuple[float, QuestionDropVariant]] = []
        for variant in candidates:
            concept_seen_at = recent_by_concept.get(variant.concept_id)
            if concept_seen_at is not None and (now - concept_seen_at).days < QUESTION_DROP_CONCEPT_COOLDOWN_DAYS:
                continue
            variant_seen_at = recent_by_variant.get(variant.variant_hash)
            if variant_seen_at is not None and (now - variant_seen_at).days < QUESTION_DROP_VARIANT_COOLDOWN_DAYS:
                continue
            freshness = 30.0 if concept_seen_at is None else min((now - concept_seen_at).days * 2.0, 20.0)
            variant_freshness = 10.0 if variant_seen_at is None else min((now - variant_seen_at).days * 0.5, 6.0)
            category_balance = 8.0 - category_counts[variant.category]
            difficulty_balance = 6.0 - difficulty_counts[int(variant.difficulty)]
            jitter = (int(build_variant_hash(slot_key, variant.variant_hash), 16) % 1000) / 1000.0
            score = freshness + variant_freshness + category_balance + difficulty_balance + jitter
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
        exposures = await self.store.list_exposures_for_guild(guild_id, limit=300)
        used_slot_keys = {record["slot_key"] for record in exposures if isinstance(record.get("slot_key"), str)}
        active_slot_keys = {
            record["slot_key"]
            for (active_guild_id, _), record in self._active_drops.items()
            if active_guild_id == guild_id
        }
        for day_offset in range(0, 2):
            local_day = now.astimezone(tzinfo).date() + timedelta(days=day_offset)
            for index, local_slot in enumerate(_daily_slot_datetimes(guild_id, local_day, config)):
                slot_key = _slot_key(local_day, index)
                if slot_key in used_slot_keys or slot_key in active_slot_keys:
                    continue
                return local_slot.astimezone(timezone.utc)
        return None
