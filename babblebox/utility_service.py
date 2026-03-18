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
from babblebox.text_safety import find_private_pattern, normalize_plain_text, sanitize_short_plain_text
from babblebox.utility_helpers import (
    build_brb_status_embed,
    build_capture_delivery_embed,
    build_capture_transcript_file,
    build_jump_view,
    build_later_marker_embed,
    build_reminder_delivery_embed,
    build_watch_alert_embed,
    deserialize_datetime,
    format_duration_brief,
    parse_duration_string,
    serialize_datetime,
)
from babblebox.utility_store import UtilityStateStore


WATCH_KEYWORD_LIMIT = 10
WATCH_KEYWORD_MAX_LEN = 40
WATCH_DM_COOLDOWN_SECONDS = 20.0
WATCH_DEDUP_TTL_SECONDS = 300.0
CAPTURE_COOLDOWN_SECONDS = 45.0
REMINDER_COOLDOWN_SECONDS = 15.0
REMINDER_MAX_ACTIVE = 10
REMINDER_MIN_SECONDS = 60
REMINDER_MAX_SECONDS = 30 * 24 * 3600
BRB_MIN_SECONDS = 60
BRB_MAX_SECONDS = 7 * 24 * 3600
BRB_NOTICE_COOLDOWN_SECONDS = 30.0
BRB_SET_COOLDOWN_SECONDS = 15.0
BRB_REASON_MAX_LEN = ge.AFK_REASON_MAX_LEN


def _watch_default_config() -> dict:
    return {
        "mention_global": False,
        "mention_guild_ids": [],
        "keywords": [],
    }


def _build_keyword_matcher(phrase: str, mode: str):
    lowered = phrase.casefold()
    if mode == "word":
        pattern = re.compile(rf"(?<!\w){re.escape(lowered)}(?!\w)", re.IGNORECASE)
        return lambda content: bool(pattern.search(content))
    return lambda content: lowered in content


class UtilityService:
    def __init__(self, bot: commands.Bot, store: UtilityStateStore | None = None):
        self.bot = bot
        self.store = store or UtilityStateStore()
        self._lock = asyncio.Lock()
        self._wake_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None

        self._mention_global: set[int] = set()
        self._mention_by_guild: dict[int, set[int]] = {}
        self._keywords_global: dict[int, list[dict]] = {}
        self._keywords_by_guild: dict[int, dict[int, list[dict]]] = {}

        self._watch_dm_cooldowns: dict[int, float] = {}
        self._watch_dedup: dict[tuple[int, int], float] = {}
        self._capture_cooldowns: dict[int, float] = {}
        self._reminder_cooldowns: dict[int, float] = {}
        self._brb_set_cooldowns: dict[int, float] = {}
        self._brb_notice_cooldowns: dict[tuple[int, int], float] = {}

    async def start(self):
        await self.store.load()
        self._rebuild_watch_indexes()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="babblebox-utility-scheduler")
        self._wake_event.set()

    async def close(self):
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task

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
        if config.get("mention_global") or config.get("mention_guild_ids") or config.get("keywords"):
            return
        self.store.state.get("watch", {}).pop(str(user_id), None)

    def _rebuild_watch_indexes(self):
        mention_global: set[int] = set()
        mention_by_guild: defaultdict[int, set[int]] = defaultdict(set)
        keywords_global: defaultdict[int, list[dict]] = defaultdict(list)
        keywords_by_guild: defaultdict[int, defaultdict[int, list[dict]]] = defaultdict(lambda: defaultdict(list))

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

            for item in config.get("keywords", []):
                if not isinstance(item, dict):
                    continue
                phrase = normalize_plain_text(item.get("phrase"))
                mode = item.get("mode", "contains")
                if not phrase or mode not in {"contains", "word"}:
                    continue
                guild_id = item.get("guild_id")
                entry = {
                    "phrase": phrase,
                    "mode": mode,
                    "guild_id": guild_id if isinstance(guild_id, int) else None,
                    "matcher": _build_keyword_matcher(phrase, mode),
                }
                if entry["guild_id"] is None:
                    keywords_global[user_id].append(entry)
                else:
                    keywords_by_guild[entry["guild_id"]][user_id].append(entry)

        self._mention_global = mention_global
        self._mention_by_guild = {guild_id: set(user_ids) for guild_id, user_ids in mention_by_guild.items()}
        self._keywords_global = {user_id: list(entries) for user_id, entries in keywords_global.items()}
        self._keywords_by_guild = {
            guild_id: {user_id: list(entries) for user_id, entries in by_user.items()}
            for guild_id, by_user in keywords_by_guild.items()
        }

    async def set_watch_mentions(self, user_id: int, *, guild_id: int | None, scope: str, enabled: bool) -> tuple[bool, str]:
        async with self._lock:
            config = self._watch_config(user_id, create=True)
            if scope == "global":
                config["mention_global"] = enabled
            else:
                if guild_id is None:
                    return False, "Server-scoped mention watch can only be changed inside a server."
                guild_ids = {value for value in config.get("mention_guild_ids", []) if isinstance(value, int)}
                if enabled:
                    guild_ids.add(guild_id)
                else:
                    guild_ids.discard(guild_id)
                config["mention_guild_ids"] = sorted(guild_ids)

            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()

        state_label = "enabled" if enabled else "disabled"
        scope_label = "global" if scope == "global" else "this server"
        return True, f"Mention alerts are now {state_label} for {scope_label}."

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
        return True, cleaned

    async def add_watch_keyword(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        phrase: str,
        scope: str,
        mode: str,
    ) -> tuple[bool, str]:
        valid, cleaned_or_error = self.validate_watch_keyword(phrase)
        if not valid:
            return False, cleaned_or_error
        if mode not in {"contains", "word"}:
            return False, "Keyword mode must be either `contains` or `word`."
        if scope == "server" and guild_id is None:
            return False, "Server-scoped keywords can only be added inside a server."

        cleaned = cleaned_or_error
        target_guild_id = None if scope == "global" else guild_id

        async with self._lock:
            config = self._watch_config(user_id, create=True)
            keywords = list(config.get("keywords", []))
            if len(keywords) >= WATCH_KEYWORD_LIMIT:
                return False, f"You can store up to {WATCH_KEYWORD_LIMIT} watch keywords."

            duplicate = next(
                (
                    item
                    for item in keywords
                    if normalize_plain_text(item.get("phrase")) == cleaned
                    and item.get("mode", "contains") == mode
                    and item.get("guild_id") == target_guild_id
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
                    "created_at": serialize_datetime(ge.now_utc()),
                }
            )
            config["keywords"] = keywords
            self._rebuild_watch_indexes()
            await self.store.flush()

        scope_label = "globally" if scope == "global" else "in this server"
        mode_label = "whole-word" if mode == "word" else "contains"
        return True, f"Watching `{cleaned}` {scope_label} in {mode_label} mode."

    async def remove_watch_keyword(
        self,
        user_id: int,
        *,
        guild_id: int | None,
        phrase: str,
        scope: str,
    ) -> tuple[bool, str]:
        cleaned = normalize_plain_text(phrase)
        if not cleaned:
            return False, "Keyword cannot be empty."
        if scope == "server" and guild_id is None:
            return False, "Server-scoped keywords can only be removed inside a server."

        target_guild_id = None if scope == "global" else guild_id
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
                )
            ]
            if len(new_keywords) == len(keywords):
                return False, "No matching keyword was found in that scope."

            config["keywords"] = new_keywords
            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()

        return True, f"Stopped watching `{cleaned}`."

    async def disable_watch(self, user_id: int, *, guild_id: int | None, scope: str) -> tuple[bool, str]:
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
                config["keywords"] = [item for item in config.get("keywords", []) if item.get("guild_id") is not None]
                label = "global watch settings"
            else:
                if guild_id is None:
                    return False, "Server-scoped watch settings can only be cleared inside a server."
                guild_ids = {value for value in config.get("mention_guild_ids", []) if isinstance(value, int)}
                guild_ids.discard(guild_id)
                config["mention_guild_ids"] = sorted(guild_ids)
                config["keywords"] = [item for item in config.get("keywords", []) if item.get("guild_id") != guild_id]
                label = "watch settings for this server"

            self._cleanup_watch_user_if_empty(user_id)
            self._rebuild_watch_indexes()
            await self.store.flush()

        return True, f"Cleared {label}."

    def get_watch_summary(self, user_id: int, *, guild_id: int | None) -> dict:
        config = self._watch_config(user_id) or _watch_default_config()
        mention_guild_ids = {value for value in config.get("mention_guild_ids", []) if isinstance(value, int)}
        keywords = list(config.get("keywords", []))
        server_keywords = [item for item in keywords if guild_id is not None and item.get("guild_id") == guild_id]
        global_keywords = [item for item in keywords if item.get("guild_id") is None]
        return {
            "mention_global": bool(config.get("mention_global")),
            "mention_server_enabled": guild_id in mention_guild_ids if guild_id is not None else False,
            "global_keywords": global_keywords,
            "server_keywords": server_keywords,
            "total_keywords": len(keywords),
        }

    def _member_can_access_message(self, member: discord.Member, message: discord.Message) -> bool:
        if member.id == message.author.id:
            return False
        perms = message.channel.permissions_for(member)
        return perms.view_channel and perms.read_message_history

    def _prune_watch_caches(self, now: float):
        if len(self._watch_dedup) > 512:
            self._watch_dedup = {
                key: timestamp
                for key, timestamp in self._watch_dedup.items()
                if now - timestamp < WATCH_DEDUP_TTL_SECONDS
            }
        if len(self._watch_dm_cooldowns) > 256:
            self._watch_dm_cooldowns = {
                key: timestamp
                for key, timestamp in self._watch_dm_cooldowns.items()
                if now - timestamp < WATCH_DM_COOLDOWN_SECONDS * 3
            }

    async def handle_watch_message(self, message: discord.Message):
        if message.guild is None:
            return
        if not (self._mention_global or self._mention_by_guild or self._keywords_global or self._keywords_by_guild):
            return

        alerts: dict[int, dict[str, set[str]]] = {}
        guild_id = message.guild.id
        mentioned_members = {
            member.id: member
            for member in message.mentions
            if member.id != message.author.id and not member.bot
        }

        watched_mentions = self._mention_by_guild.get(guild_id, set())
        for user_id, member in mentioned_members.items():
            if user_id in self._mention_global or user_id in watched_mentions:
                if self._member_can_access_message(member, message):
                    alerts.setdefault(user_id, {"triggers": set(), "keywords": set()})["triggers"].add("Mention")

        content = normalize_plain_text(message.content).casefold()
        keyword_candidates: defaultdict[int, list[dict]] = defaultdict(list)
        for user_id, entries in self._keywords_global.items():
            keyword_candidates[user_id].extend(entries)
        for user_id, entries in self._keywords_by_guild.get(guild_id, {}).items():
            keyword_candidates[user_id].extend(entries)

        if content and keyword_candidates:
            for user_id, entries in keyword_candidates.items():
                if user_id == message.author.id:
                    continue
                member = mentioned_members.get(user_id) or message.guild.get_member(user_id)
                if member is None or not self._member_can_access_message(member, message):
                    continue
                matched = {entry["phrase"] for entry in entries if entry["matcher"](content)}
                if matched:
                    item = alerts.setdefault(user_id, {"triggers": set(), "keywords": set()})
                    item["triggers"].add("Keyword")
                    item["keywords"].update(matched)

        if not alerts:
            return

        now = asyncio.get_running_loop().time()
        self._prune_watch_caches(now)
        for user_id, payload in alerts.items():
            if self._watch_dedup.get((user_id, message.id)):
                continue
            last_sent = self._watch_dm_cooldowns.get(user_id, 0.0)
            if now - last_sent < WATCH_DM_COOLDOWN_SECONDS:
                continue
            self._watch_dedup[(user_id, message.id)] = now
            self._watch_dm_cooldowns[user_id] = now
            await self._send_watch_alert(user_id, message, payload)

    async def _send_watch_alert(self, user_id: int, message: discord.Message, payload: dict[str, set[str]]):
        recipient = message.guild.get_member(user_id) or self.bot.get_user(user_id)
        if recipient is None:
            return
        embed = build_watch_alert_embed(
            message,
            trigger_labels=sorted(payload["triggers"]),
            matched_keywords=sorted(payload["keywords"]),
        )
        view = build_jump_view(message.jump_url)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await recipient.send(embed=embed, view=view)

    async def save_later_marker(self, *, user: discord.abc.User, channel: discord.abc.GuildChannel, message: discord.Message) -> dict:
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
            "preview": ge.safe_field_text(message.content.strip() or "[no text content]", limit=280),
        }

        async with self._lock:
            per_user = self.store.state.setdefault("later", {}).setdefault(str(user.id), {})
            per_user[str(channel.id)] = marker
            await self.store.flush()

        return marker

    def list_later_markers(self, user_id: int, *, guild_id: int | None = None) -> list[dict]:
        markers = list((self.store.state.get("later", {}).get(str(user_id), {}) or {}).values())
        output = []
        for marker in markers:
            if not isinstance(marker, dict):
                continue
            if guild_id is not None and marker.get("guild_id") != guild_id:
                continue
            output.append(marker)
        output.sort(key=lambda item: item.get("saved_at", ""), reverse=True)
        return output

    async def clear_later_marker(self, user_id: int, *, channel_id: int | None = None) -> tuple[bool, str]:
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
        last_used = self._capture_cooldowns.get(user_id, 0.0)
        remaining = CAPTURE_COOLDOWN_SECONDS - (now - last_used)
        if remaining > 0:
            return False, f"Capture is on cooldown. Try again in about {int(remaining)} seconds."
        self._capture_cooldowns[user_id] = now
        return True, None

    def parse_relative_duration(self, raw: str | None) -> int | None:
        return parse_duration_string(raw)

    async def create_reminder(
        self,
        *,
        user: discord.abc.User,
        text: str,
        delay_seconds: int,
        delivery: str,
        guild: discord.Guild | None,
        channel: discord.abc.GuildChannel | discord.DMChannel | discord.Thread | None,
        origin_jump_url: str | None,
    ) -> tuple[bool, str | dict]:
        valid, cleaned_or_error = sanitize_short_plain_text(
            text,
            field_name="Reminder text",
            max_length=200,
            sentence_limit=4,
            reject_blocklist=True,
            allow_empty=False,
        )
        if not valid:
            return False, cleaned_or_error
        if delivery not in {"dm", "here"}:
            return False, "Reminder delivery must be either `dm` or `here`."
        if delay_seconds < REMINDER_MIN_SECONDS or delay_seconds > REMINDER_MAX_SECONDS:
            return False, f"Reminders must be between 1 minute and {format_duration_brief(REMINDER_MAX_SECONDS)}."

        now = asyncio.get_running_loop().time()
        last_used = self._reminder_cooldowns.get(user.id, 0.0)
        remaining = REMINDER_COOLDOWN_SECONDS - (now - last_used)
        if remaining > 0:
            return False, f"Reminder creation is on cooldown. Try again in about {int(remaining)} seconds."

        active_count = len(
            [
                item
                for item in self.store.state.get("reminders", {}).values()
                if isinstance(item, dict) and item.get("user_id") == user.id
            ]
        )
        if active_count >= REMINDER_MAX_ACTIVE:
            return False, f"You can keep up to {REMINDER_MAX_ACTIVE} active reminders."

        reminder_id = uuid.uuid4().hex
        created_at = ge.now_utc()
        due_at = created_at + timedelta(seconds=delay_seconds)
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
            "origin_jump_url": origin_jump_url,
        }

        async with self._lock:
            self.store.state.setdefault("reminders", {})[reminder_id] = record
            await self.store.flush()
            self._wake_event.set()

        self._reminder_cooldowns[user.id] = now
        return True, record

    def list_reminders(self, user_id: int) -> list[dict]:
        reminders = [
            item
            for item in self.store.state.get("reminders", {}).values()
            if isinstance(item, dict) and item.get("user_id") == user_id
        ]
        reminders.sort(key=lambda item: item.get("due_at", ""))
        return reminders

    async def cancel_reminder(self, user_id: int, reminder_id_prefix: str) -> tuple[bool, str]:
        reminder_id_prefix = reminder_id_prefix.strip().lower()
        if not reminder_id_prefix:
            return False, "Provide the reminder ID from `/remind list`."

        async with self._lock:
            matches = [
                reminder_id
                for reminder_id, record in self.store.state.get("reminders", {}).items()
                if isinstance(record, dict)
                and record.get("user_id") == user_id
                and reminder_id.lower().startswith(reminder_id_prefix)
            ]
            if not matches:
                return False, "No reminder matched that ID."
            if len(matches) > 1:
                return False, "That ID prefix matches multiple reminders. Use a longer ID."
            self.store.state.get("reminders", {}).pop(matches[0], None)
            await self.store.flush()
            self._wake_event.set()

        return True, f"Reminder `{matches[0][:8]}` was cancelled."

    async def set_brb(self, *, user: discord.abc.User, delay_seconds: int, reason: str | None, guild: discord.Guild | None) -> tuple[bool, str | dict]:
        if delay_seconds < BRB_MIN_SECONDS or delay_seconds > BRB_MAX_SECONDS:
            return False, f"BRB duration must be between 1 minute and {format_duration_brief(BRB_MAX_SECONDS)}."

        valid, cleaned_or_error = sanitize_short_plain_text(
            reason,
            field_name="BRB reason",
            max_length=BRB_REASON_MAX_LEN,
            sentence_limit=3,
            reject_blocklist=True,
            allow_empty=True,
        )
        if not valid:
            return False, cleaned_or_error

        now = asyncio.get_running_loop().time()
        last_used = self._brb_set_cooldowns.get(user.id, 0.0)
        remaining = BRB_SET_COOLDOWN_SECONDS - (now - last_used)
        if remaining > 0:
            return False, f"BRB is on cooldown. Try again in about {int(remaining)} seconds."

        created_at = ge.now_utc()
        ends_at = created_at + timedelta(seconds=delay_seconds)
        record = {
            "user_id": user.id,
            "reason": cleaned_or_error,
            "created_at": serialize_datetime(created_at),
            "ends_at": serialize_datetime(ends_at),
            "guild_id": guild.id if guild is not None else None,
            "guild_name": guild.name if guild is not None else None,
        }

        async with self._lock:
            self.store.state.setdefault("brb", {})[str(user.id)] = record
            await self.store.flush()
            self._wake_event.set()

        self._brb_set_cooldowns[user.id] = now
        return True, record

    def get_brb_record(self, user_id: int) -> dict | None:
        record = self.store.state.get("brb", {}).get(str(user_id))
        if not isinstance(record, dict):
            return None
        ends_at = deserialize_datetime(record.get("ends_at"))
        if ends_at is None or ends_at <= ge.now_utc():
            return None
        return record

    async def clear_brb(self, user_id: int) -> tuple[bool, str]:
        async with self._lock:
            record = self.store.state.get("brb", {}).pop(str(user_id), None)
            if record is None:
                return False, "You do not currently have an active BRB timer."
            await self.store.flush()
            self._wake_event.set()
        return True, "Your BRB timer was cleared."

    async def clear_brb_on_activity(self, user_id: int) -> bool:
        if self.get_brb_record(user_id) is None:
            return False
        async with self._lock:
            removed = self.store.state.get("brb", {}).pop(str(user_id), None)
            if removed is None:
                return False
            await self.store.flush()
            self._wake_event.set()
        return True

    def build_brb_notice_lines(self, message: discord.Message) -> list[str]:
        if message.guild is None:
            return []

        now_loop = asyncio.get_running_loop().time()
        lines = []
        seen = set()
        for member in message.mentions:
            if member.id == message.author.id or member.bot or member.id in seen:
                continue
            record = self.get_brb_record(member.id)
            if record is None:
                continue
            cooldown_key = (message.channel.id, member.id)
            last_notified = self._brb_notice_cooldowns.get(cooldown_key, 0.0)
            if now_loop - last_notified < BRB_NOTICE_COOLDOWN_SECONDS:
                continue
            self._brb_notice_cooldowns[cooldown_key] = now_loop
            seen.add(member.id)

            ends_at = deserialize_datetime(record.get("ends_at"))
            line = f"**{ge.display_name_of(member)}** is BRB until {ge.format_timestamp(ends_at, 'R')}"
            if record.get("reason"):
                line += f" - {record['reason']}"
            lines.append(line)
            if len(lines) >= 5:
                break
        return lines

    async def _scheduler_loop(self):
        await self.bot.wait_until_ready()
        while True:
            self._wake_event.clear()
            due_reminders, due_brb, next_due = self._collect_due_records()
            if due_reminders or due_brb:
                if due_reminders:
                    await self._deliver_due_reminders(due_reminders)
                if due_brb:
                    await self._expire_due_brb(due_brb)
                continue

            timeout = None
            if next_due is not None:
                timeout = max(1.0, (next_due - ge.now_utc()).total_seconds())

            try:
                if timeout is None:
                    await self._wake_event.wait()
                else:
                    await asyncio.wait_for(self._wake_event.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                continue

    def _collect_due_records(self) -> tuple[list[dict], list[dict], datetime | None]:
        now = ge.now_utc()
        due_reminders = []
        due_brb = []
        next_due = None

        for record in self.store.state.get("reminders", {}).values():
            if not isinstance(record, dict):
                continue
            due_at = deserialize_datetime(record.get("due_at"))
            if due_at is None:
                continue
            if due_at <= now:
                due_reminders.append(record)
            elif next_due is None or due_at < next_due:
                next_due = due_at

        for record in self.store.state.get("brb", {}).values():
            if not isinstance(record, dict):
                continue
            ends_at = deserialize_datetime(record.get("ends_at"))
            if ends_at is None:
                continue
            if ends_at <= now:
                due_brb.append(record)
            elif next_due is None or ends_at < next_due:
                next_due = ends_at

        return due_reminders, due_brb, next_due

    async def _deliver_due_reminders(self, reminders: list[dict]):
        to_remove = []
        for record in reminders:
            await self._deliver_single_reminder(record)
            reminder_id = record.get("id")
            if isinstance(reminder_id, str):
                to_remove.append(reminder_id)

        if not to_remove:
            return

        async with self._lock:
            for reminder_id in to_remove:
                self.store.state.get("reminders", {}).pop(reminder_id, None)
            await self.store.flush()

    async def _deliver_single_reminder(self, record: dict):
        due_at = deserialize_datetime(record.get("due_at"))
        delayed = bool(due_at is not None and (ge.now_utc() - due_at).total_seconds() > 120)
        embed = build_reminder_delivery_embed(record, delayed=delayed)
        view = build_jump_view(record["origin_jump_url"]) if record.get("origin_jump_url") else None
        user_id = record.get("user_id")

        if record.get("delivery") == "here" and isinstance(record.get("channel_id"), int):
            channel = self.bot.get_channel(record["channel_id"])
            if channel is None:
                with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                    channel = await self.bot.fetch_channel(record["channel_id"])
            if channel is not None:
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await channel.send(
                        content=f"<@{user_id}>",
                        embed=embed,
                        view=view,
                        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
                    )
                    return

        user = self.bot.get_user(user_id)
        if user is None:
            with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                user = await self.bot.fetch_user(user_id)
        if user is not None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await user.send(embed=embed, view=view)

    async def _expire_due_brb(self, records: list[dict]):
        user_keys = []
        for record in records:
            user_id = record.get("user_id")
            if not isinstance(user_id, int):
                continue
            user_keys.append(str(user_id))
            user = self.bot.get_user(user_id)
            if user is None:
                with contextlib.suppress(discord.HTTPException, discord.Forbidden, discord.NotFound):
                    user = await self.bot.fetch_user(user_id)
            if user is not None:
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await user.send(
                        embed=ge.make_status_embed(
                            "BRB Timer Ended",
                            "Your BRB timer has expired.",
                            tone="success",
                            footer="Babblebox BRB",
                        )
                    )

        if not user_keys:
            return

        async with self._lock:
            for key in user_keys:
                self.store.state.get("brb", {}).pop(key, None)
            await self.store.flush()

    async def send_later_marker_dm(self, user: discord.abc.User, marker: dict):
        embed = build_later_marker_embed(marker)
        view = build_jump_view(marker["message_jump_url"])
        await user.send(embed=embed, view=view)

    async def send_capture_dm(
        self,
        *,
        user: discord.abc.User,
        guild_name: str,
        channel_name: str,
        messages: list[discord.Message],
        requested_count: int,
    ):
        jump_url = messages[-1].jump_url if messages else None
        preview_lines = []
        for message in reversed(messages[-4:]):
            stamp = message.created_at.strftime("%H:%M") if message.created_at else "--:--"
            preview_lines.append(
                f"[{stamp}] {ge.display_name_of(message.author)}: "
                f"{ge.safe_field_text(message.content or '[no text]', limit=80)}"
            )
        embed, view = build_capture_delivery_embed(
            guild_name=guild_name,
            channel_name=channel_name,
            captured_count=len(messages),
            requested_count=requested_count,
            preview_lines=preview_lines,
            jump_url=jump_url,
        )
        transcript = build_capture_transcript_file(
            guild_name=guild_name,
            channel_name=channel_name,
            messages=messages,
        )
        await user.send(embed=embed, view=view, file=transcript)

    def build_brb_status_embed_for(self, user: discord.abc.User, record: dict) -> discord.Embed:
        return build_brb_status_embed(user, record)
