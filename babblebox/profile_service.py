from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.daily_challenges import DAILY_MAX_ATTEMPTS, build_daily_shuffle, normalize_daily_guess
from babblebox.profile_store import ProfileStorageUnavailable, ProfileStore
from babblebox.text_safety import sanitize_short_plain_text


PROFILE_PRUNE_META_KEY = "daily_prune_v1"
PROFILE_DAILY_RETENTION_DAYS = 180

DAILY_PARTICIPATION_XP = 8
DAILY_CLEAR_XP = 12
UTILITY_ACTION_XP = 4
GAME_PLAY_XP = 6
GAME_WIN_XP = 5

XP_CAPS = {
    "daily": 2,
    "utility": 2,
    "game": 3,
}

BUDDY_SPECIES = {
    "cloudlet": {
        "label": "Cloudlet",
        "badge": "Cloudlet",
        "description": "A soft floating sidekick that likes quiet wins.",
        "default_name": "Mallow",
    },
    "bytebug": {
        "label": "Bytebug",
        "badge": "Bytebug",
        "description": "A tiny curious crawler powered by little victories.",
        "default_name": "Bitsy",
    },
    "sprout": {
        "label": "Sprout",
        "badge": "Sprout",
        "description": "A patient green buddy that grows with your streaks.",
        "default_name": "Sprig",
    },
    "puddlefox": {
        "label": "Puddlefox",
        "badge": "Puddlefox",
        "description": "A rainy-day friend that loves bookmarks and reminders.",
        "default_name": "Drizzle",
    },
    "moondrop": {
        "label": "Moondrop",
        "badge": "Moondrop",
        "description": "A sleepy cosmic pal that wakes up for daily puzzles.",
        "default_name": "Nova",
    },
}

BUDDY_STYLES = {
    "mint": {"label": "Mint", "color": discord.Color.from_rgb(84, 198, 169)},
    "sunset": {"label": "Sunset", "color": discord.Color.from_rgb(255, 141, 107)},
    "sky": {"label": "Sky", "color": discord.Color.from_rgb(102, 152, 255)},
    "midnight": {"label": "Midnight", "color": discord.Color.from_rgb(123, 112, 255)},
}

TITLE_DEFINITIONS = {
    "day-one": {"label": "Day One", "description": "Completed a first Babblebox Daily."},
    "streak-keeper": {"label": "Streak Keeper", "description": "Built a 3-day Daily streak."},
    "quiet-helper": {"label": "Quiet Helper", "description": "Used Babblebox utility tools consistently."},
    "party-starter": {"label": "Party Starter", "description": "Showed up for the party games."},
    "daily-core": {"label": "Daily Core", "description": "Reached double-digit Daily clears."},
    "arcade-heart": {"label": "Arcade Heart", "description": "Stacked up multiplayer wins."},
}

BADGE_DEFINITIONS = {
    "daily-spark": {"label": "Daily Spark", "prefix": "[spark]"},
    "watchful": {"label": "Watchful", "prefix": "[watch]"},
    "bookmarker": {"label": "Bookmarker", "prefix": "[bookmark]"},
    "steady": {"label": "Steady", "prefix": "[steady]"},
    "crowd-favorite": {"label": "Crowd Favorite", "prefix": "[party]"},
}

UTILITY_ACTION_FIELDS = {
    "watch_keyword": "watch_actions",
    "later": "later_saves",
    "capture": "capture_uses",
    "reminder": "reminders_created",
    "afk": "afk_sessions",
}

GAME_TYPE_FIELDS = {
    "telephone": "telephone_rounds",
    "corpse": "corpse_rounds",
    "spyfall": "spyfall_rounds",
    "bomb": "bomb_rounds",
}


def _profile_default(user_id: int) -> dict[str, Any]:
    species_ids = tuple(BUDDY_SPECIES)
    style_ids = tuple(BUDDY_STYLES)
    species_id = species_ids[user_id % len(species_ids)]
    style_id = style_ids[(user_id // len(species_ids)) % len(style_ids)]
    return {
        "user_id": user_id,
        "buddy_species": species_id,
        "buddy_name": BUDDY_SPECIES[species_id]["default_name"],
        "buddy_style": style_id,
        "selected_title": None,
        "featured_badge": None,
        "buddy_mood": "curious",
        "xp_total": 0,
        "last_interaction_at": None,
        "last_daily_clear_date": None,
        "current_daily_streak": 0,
        "best_daily_streak": 0,
        "total_daily_participations": 0,
        "total_daily_clears": 0,
        "watch_actions": 0,
        "later_saves": 0,
        "capture_uses": 0,
        "reminders_created": 0,
        "afk_sessions": 0,
        "games_played": 0,
        "games_hosted": 0,
        "games_won": 0,
        "telephone_rounds": 0,
        "telephone_completions": 0,
        "corpse_rounds": 0,
        "corpse_masterpieces": 0,
        "spyfall_rounds": 0,
        "spyfall_wins": 0,
        "bomb_rounds": 0,
        "bomb_wins": 0,
        "xp_window_date": None,
        "daily_xp_actions": 0,
        "utility_xp_actions": 0,
        "game_xp_actions": 0,
    }


def _xp_for_level(level: int) -> int:
    if level <= 1:
        return 0
    total = 0
    for current in range(1, level):
        total += 20 + ((current - 1) * 10)
    return total


def _level_from_xp(xp_total: int) -> tuple[int, int, int]:
    level = 1
    while xp_total >= _xp_for_level(level + 1):
        level += 1
    current_floor = _xp_for_level(level)
    next_floor = _xp_for_level(level + 1)
    return level, xp_total - current_floor, next_floor - current_floor


def _effective_streak(profile: dict[str, Any], today: date) -> int:
    last_clear = profile.get("last_daily_clear_date")
    if not isinstance(last_clear, date):
        return 0
    return int(profile.get("current_daily_streak", 0) or 0) if (today - last_clear).days <= 1 else 0


def _utility_score(profile: dict[str, Any]) -> int:
    return sum(int(profile.get(field, 0) or 0) for field in UTILITY_ACTION_FIELDS.values())


def _unlocked_titles(profile: dict[str, Any]) -> list[str]:
    unlocked: list[str] = []
    if int(profile.get("total_daily_clears", 0) or 0) >= 1:
        unlocked.append("day-one")
    if int(profile.get("best_daily_streak", 0) or 0) >= 3:
        unlocked.append("streak-keeper")
    if _utility_score(profile) >= 6:
        unlocked.append("quiet-helper")
    if int(profile.get("games_played", 0) or 0) >= 6 or int(profile.get("games_hosted", 0) or 0) >= 3:
        unlocked.append("party-starter")
    if int(profile.get("total_daily_clears", 0) or 0) >= 10:
        unlocked.append("daily-core")
    if int(profile.get("games_won", 0) or 0) >= 5:
        unlocked.append("arcade-heart")
    return unlocked


def _earned_badges(profile: dict[str, Any], today: date) -> list[str]:
    badges: list[str] = []
    if _effective_streak(profile, today) >= 3:
        badges.append("daily-spark")
    if int(profile.get("watch_actions", 0) or 0) >= 2:
        badges.append("watchful")
    if int(profile.get("later_saves", 0) or 0) >= 3:
        badges.append("bookmarker")
    if int(profile.get("reminders_created", 0) or 0) >= 2 or int(profile.get("afk_sessions", 0) or 0) >= 2:
        badges.append("steady")
    if int(profile.get("games_played", 0) or 0) >= 5:
        badges.append("crowd-favorite")
    return badges


def _resolve_mood(profile: dict[str, Any], today: date) -> str:
    if _effective_streak(profile, today) >= 7:
        return "locked in"
    if int(profile.get("games_won", 0) or 0) >= 1 and profile.get("buddy_mood") == "celebrating":
        return "celebrating"
    last_interaction = profile.get("last_interaction_at")
    if isinstance(last_interaction, datetime):
        delta = ge.now_utc() - last_interaction.astimezone(timezone.utc)
        if delta <= timedelta(hours=18):
            return profile.get("buddy_mood") or "sparky"
        if delta >= timedelta(days=3):
            return "sleepy"
    return profile.get("buddy_mood") or "curious"


def _format_badges(badge_ids: list[str]) -> str:
    if not badge_ids:
        return "None yet"
    return ", ".join(f"{BADGE_DEFINITIONS[badge]['prefix']} {BADGE_DEFINITIONS[badge]['label']}" for badge in badge_ids[:3])


def _share_grid(result: dict[str, Any]) -> str:
    attempts = int(result.get("attempt_count", 0) or 0)
    solved = bool(result.get("solved"))
    cells = []
    for index in range(DAILY_MAX_ATTEMPTS):
        if index >= attempts:
            cells.append("⬛")
        elif solved and index == attempts - 1:
            cells.append("🟩")
        else:
            cells.append("🟥")
    return "".join(cells)


class ProfileService:
    def __init__(self, bot: commands.Bot, store: ProfileStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = ProfileStore()
            except ProfileStorageUnavailable as exc:
                print(f"Profile storage constructor failed: {exc}")
                self.store = ProfileStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()

    async def start(self):
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Profile storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
            await self._maybe_prune_daily_rows()
        except ProfileStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Profile storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        return True

    async def close(self):
        await self.store.close()

    def storage_message(self, feature_name: str = "This feature") -> str:
        return f"{feature_name} is temporarily unavailable because Babblebox could not reach its profile database."

    async def _maybe_prune_daily_rows(self):
        today = ge.now_utc().date()
        meta = await self.store.get_meta(PROFILE_PRUNE_META_KEY)
        if isinstance(meta, dict) and meta.get("last_pruned_date") == today.isoformat():
            return
        keep_after = today - timedelta(days=PROFILE_DAILY_RETENTION_DAYS)
        removed = await self.store.prune_daily_results(
            challenge_id=build_daily_shuffle(today).challenge_id,
            keep_after=keep_after,
        )
        await self.store.set_meta(
            PROFILE_PRUNE_META_KEY,
            {"last_pruned_date": today.isoformat(), "removed_rows": removed, "retention_days": PROFILE_DAILY_RETENTION_DAYS},
        )

    async def _ensure_profile(self, user_id: int) -> dict[str, Any]:
        profile = await self.store.fetch_profile(user_id)
        if profile is None:
            profile = _profile_default(user_id)
            await self.store.save_profile(profile)
        return profile

    def _reset_xp_window_if_needed(self, profile: dict[str, Any], today: date):
        if profile.get("xp_window_date") != today:
            profile["xp_window_date"] = today
            profile["daily_xp_actions"] = 0
            profile["utility_xp_actions"] = 0
            profile["game_xp_actions"] = 0

    def _grant_xp(self, profile: dict[str, Any], *, bucket: str, amount: int, today: date) -> bool:
        bucket_field = f"{bucket}_xp_actions"
        cap = XP_CAPS[bucket]
        self._reset_xp_window_if_needed(profile, today)
        if int(profile.get(bucket_field, 0) or 0) >= cap:
            return False
        profile[bucket_field] = int(profile.get(bucket_field, 0) or 0) + 1
        profile["xp_total"] = int(profile.get("xp_total", 0) or 0) + amount
        return True

    def _touch_profile(self, profile: dict[str, Any], *, mood: str):
        profile["last_interaction_at"] = ge.now_utc()
        profile["buddy_mood"] = mood

    def _sync_identity_fields(self, profile: dict[str, Any], today: date):
        titles = _unlocked_titles(profile)
        badges = _earned_badges(profile, today)
        if titles:
            selected = profile.get("selected_title")
            profile["selected_title"] = selected if selected in titles else titles[-1]
        else:
            profile["selected_title"] = None
        if badges:
            featured = profile.get("featured_badge")
            profile["featured_badge"] = featured if featured in badges else badges[0]
        else:
            profile["featured_badge"] = None

    def _enrich_profile(self, profile: dict[str, Any]) -> dict[str, Any]:
        today = ge.now_utc().date()
        enriched = dict(profile)
        level, xp_into_level, xp_needed = _level_from_xp(int(enriched.get("xp_total", 0) or 0))
        enriched.update(
            {
                "species_meta": BUDDY_SPECIES[enriched["buddy_species"]],
                "style_meta": BUDDY_STYLES[enriched["buddy_style"]],
                "level": level,
                "xp_into_level": xp_into_level,
                "xp_needed_this_level": xp_needed,
                "active_streak": _effective_streak(enriched, today),
                "titles_unlocked": _unlocked_titles(enriched),
                "badges_unlocked": _earned_badges(enriched, today),
                "display_title": TITLE_DEFINITIONS[enriched["selected_title"]]["label"] if enriched.get("selected_title") in TITLE_DEFINITIONS else "No title yet",
                "display_badge": BADGE_DEFINITIONS[enriched["featured_badge"]]["label"] if enriched.get("featured_badge") in BADGE_DEFINITIONS else "No featured badge",
                "resolved_mood": _resolve_mood(enriched, today),
            }
        )
        return enriched

    async def get_profile(self, user_id: int) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        async with self._lock:
            profile = await self._ensure_profile(user_id)
            today = ge.now_utc().date()
            self._sync_identity_fields(profile, today)
            return self._enrich_profile(profile)

    async def rename_buddy(self, user_id: int, nickname: str) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Buddy")
        ok, cleaned = sanitize_short_plain_text(
            nickname,
            field_name="Buddy nickname",
            max_length=24,
            sentence_limit=1,
            reject_blocklist=True,
            allow_empty=False,
        )
        if not ok:
            return False, cleaned
        async with self._lock:
            profile = await self._ensure_profile(user_id)
            profile["buddy_name"] = cleaned
            self._touch_profile(profile, mood="sparky")
            self._sync_identity_fields(profile, ge.now_utc().date())
            await self.store.save_profile(profile)
        return True, f"Your Babblebox Buddy is now named **{cleaned}**."

    async def set_buddy_style(self, user_id: int, style_id: str) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Buddy")
        if style_id not in BUDDY_STYLES:
            choices = ", ".join(f"`{key}`" for key in BUDDY_STYLES)
            return False, f"Unknown buddy style. Try one of: {choices}."
        async with self._lock:
            profile = await self._ensure_profile(user_id)
            profile["buddy_style"] = style_id
            self._touch_profile(profile, mood="sparky")
            self._sync_identity_fields(profile, ge.now_utc().date())
            await self.store.save_profile(profile)
        return True, f"Buddy style set to **{BUDDY_STYLES[style_id]['label']}**."

    async def get_daily_status(self, user_id: int) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        async with self._lock:
            today = ge.now_utc().date()
            profile = await self._ensure_profile(user_id)
            self._sync_identity_fields(profile, today)
            puzzle = build_daily_shuffle(today)
            result = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
            return {"profile": self._enrich_profile(profile), "puzzle": puzzle, "result": result}

    async def submit_daily_guess(self, user_id: int, guess: str) -> tuple[bool, dict[str, Any] | str]:
        if not self.storage_ready:
            return False, self.storage_message("Daily")
        normalized = normalize_daily_guess(guess)
        if not normalized:
            return False, "Your guess has to include letters."
        async with self._lock:
            now = ge.now_utc()
            today = now.date()
            puzzle = build_daily_shuffle(today)
            profile = await self._ensure_profile(user_id)
            result = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
            if result is None:
                result = {
                    "challenge_id": puzzle.challenge_id,
                    "puzzle_date": today,
                    "user_id": user_id,
                    "attempt_count": 0,
                    "solved": False,
                    "first_attempt_at": now,
                    "completed_at": None,
                    "solve_seconds": None,
                }
            if result.get("solved"):
                return False, "You already solved today's Daily. Use `/daily share` to post the result."
            if result.get("completed_at") is not None or int(result.get("attempt_count", 0) or 0) >= DAILY_MAX_ATTEMPTS:
                return False, "You already finished today's Daily. Come back at the next UTC reset."

            first_attempt = int(result.get("attempt_count", 0) or 0) == 0
            result["attempt_count"] = int(result.get("attempt_count", 0) or 0) + 1
            if first_attempt:
                profile["total_daily_participations"] = int(profile.get("total_daily_participations", 0) or 0) + 1
                self._grant_xp(profile, bucket="daily", amount=DAILY_PARTICIPATION_XP, today=today)

            solved = normalized == puzzle.answer
            if solved:
                result["solved"] = True
                result["completed_at"] = now
                first_attempt_at = result.get("first_attempt_at") or now
                elapsed = max(1, int((now - first_attempt_at).total_seconds())) if isinstance(first_attempt_at, datetime) else 1
                result["solve_seconds"] = elapsed
                yesterday = today - timedelta(days=1)
                last_clear = profile.get("last_daily_clear_date")
                if last_clear == today:
                    next_streak = int(profile.get("current_daily_streak", 0) or 0)
                elif last_clear == yesterday:
                    next_streak = int(profile.get("current_daily_streak", 0) or 0) + 1
                else:
                    next_streak = 1
                profile["last_daily_clear_date"] = today
                profile["current_daily_streak"] = next_streak
                profile["best_daily_streak"] = max(int(profile.get("best_daily_streak", 0) or 0), next_streak)
                profile["total_daily_clears"] = int(profile.get("total_daily_clears", 0) or 0) + 1
                self._grant_xp(profile, bucket="daily", amount=DAILY_CLEAR_XP, today=today)
                self._touch_profile(profile, mood="proud")
                status = "solved"
            else:
                remaining = DAILY_MAX_ATTEMPTS - int(result["attempt_count"])
                if remaining <= 0:
                    result["completed_at"] = now
                    profile["current_daily_streak"] = 0
                    self._touch_profile(profile, mood="determined")
                    status = "failed"
                else:
                    self._touch_profile(profile, mood="focused")
                    status = "retry"

            self._sync_identity_fields(profile, today)
            await self.store.save_daily_result(result)
            await self.store.save_profile(profile)
            return True, {"status": status, "profile": self._enrich_profile(profile), "puzzle": puzzle, "result": result}

    async def build_daily_share(self, user_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Daily")
        async with self._lock:
            today = ge.now_utc().date()
            puzzle = build_daily_shuffle(today)
            profile = await self._ensure_profile(user_id)
            result = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
            if result is None:
                return False, "You have not played today's Daily yet."
            if not result.get("solved") and result.get("completed_at") is None:
                return False, "Finish today's Daily first, then use `/daily share`."
            self._sync_identity_fields(profile, today)
            share_line = (
                f"Babblebox Daily #{puzzle.challenge_number} | "
                f"{'Solved' if result.get('solved') else 'Tried'} "
                f"{int(result.get('attempt_count', 0) or 0)}/{DAILY_MAX_ATTEMPTS}"
            )
            if result.get("solved") and result.get("solve_seconds"):
                share_line += f" | {int(result['solve_seconds'])}s"
            share_line += f" | streak {_effective_streak(profile, today)}"
            return True, f"{share_line}\n{_share_grid(result)}"

    async def get_daily_stats(self, user_id: int) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        async with self._lock:
            today = ge.now_utc().date()
            profile = await self._ensure_profile(user_id)
            recent = await self.store.fetch_recent_daily_results(user_id=user_id, limit=5)
            self._sync_identity_fields(profile, today)
            return {"profile": self._enrich_profile(profile), "recent_results": recent, "today_puzzle": build_daily_shuffle(today)}

    async def get_daily_leaderboard(self, *, metric: str = "clears", limit: int = 10) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        today = ge.now_utc().date()
        return await self.store.fetch_daily_leaderboard(metric=metric, today=today, limit=limit)

    async def record_utility_action(self, user_id: int, action: str):
        if not self.storage_ready or action not in UTILITY_ACTION_FIELDS:
            return
        async with self._lock:
            today = ge.now_utc().date()
            profile = await self._ensure_profile(user_id)
            field = UTILITY_ACTION_FIELDS[action]
            profile[field] = int(profile.get(field, 0) or 0) + 1
            self._grant_xp(profile, bucket="utility", amount=UTILITY_ACTION_XP, today=today)
            self._touch_profile(profile, mood="steady")
            self._sync_identity_fields(profile, today)
            await self.store.save_profile(profile)

    async def record_game_started(self, *, game_type: str, host_id: int, player_ids: list[int]):
        if not self.storage_ready or game_type not in GAME_TYPE_FIELDS:
            return
        unique_player_ids = list(dict.fromkeys(player_ids))
        async with self._lock:
            today = ge.now_utc().date()
            for user_id in unique_player_ids:
                profile = await self._ensure_profile(user_id)
                profile["games_played"] = int(profile.get("games_played", 0) or 0) + 1
                field = GAME_TYPE_FIELDS[game_type]
                profile[field] = int(profile.get(field, 0) or 0) + 1
                if user_id == host_id:
                    profile["games_hosted"] = int(profile.get("games_hosted", 0) or 0) + 1
                self._grant_xp(profile, bucket="game", amount=GAME_PLAY_XP, today=today)
                self._touch_profile(profile, mood="hyped")
                self._sync_identity_fields(profile, today)
                await self.store.save_profile(profile)

    async def record_telephone_completion(self, player_ids: list[int]):
        if not self.storage_ready:
            return
        async with self._lock:
            today = ge.now_utc().date()
            for user_id in dict.fromkeys(player_ids):
                profile = await self._ensure_profile(user_id)
                profile["telephone_completions"] = int(profile.get("telephone_completions", 0) or 0) + 1
                self._touch_profile(profile, mood="proud")
                self._sync_identity_fields(profile, today)
                await self.store.save_profile(profile)

    async def record_corpse_completion(self, player_ids: list[int]):
        if not self.storage_ready:
            return
        async with self._lock:
            today = ge.now_utc().date()
            for user_id in dict.fromkeys(player_ids):
                profile = await self._ensure_profile(user_id)
                profile["corpse_masterpieces"] = int(profile.get("corpse_masterpieces", 0) or 0) + 1
                self._touch_profile(profile, mood="proud")
                self._sync_identity_fields(profile, today)
                await self.store.save_profile(profile)

    async def record_spyfall_result(self, *, spy_id: int, player_ids: list[int], village_won: bool):
        if not self.storage_ready:
            return
        winners = [user_id for user_id in player_ids if (user_id != spy_id and village_won) or (user_id == spy_id and not village_won)]
        async with self._lock:
            today = ge.now_utc().date()
            for user_id in dict.fromkeys(winners):
                profile = await self._ensure_profile(user_id)
                profile["games_won"] = int(profile.get("games_won", 0) or 0) + 1
                if user_id == spy_id and not village_won:
                    profile["spyfall_wins"] = int(profile.get("spyfall_wins", 0) or 0) + 1
                self._grant_xp(profile, bucket="game", amount=GAME_WIN_XP, today=today)
                self._touch_profile(profile, mood="celebrating")
                self._sync_identity_fields(profile, today)
                await self.store.save_profile(profile)

    async def record_bomb_win(self, winner_id: int):
        if not self.storage_ready:
            return
        async with self._lock:
            today = ge.now_utc().date()
            profile = await self._ensure_profile(winner_id)
            profile["games_won"] = int(profile.get("games_won", 0) or 0) + 1
            profile["bomb_wins"] = int(profile.get("bomb_wins", 0) or 0) + 1
            self._grant_xp(profile, bucket="game", amount=GAME_WIN_XP, today=today)
            self._touch_profile(profile, mood="celebrating")
            self._sync_identity_fields(profile, today)
            await self.store.save_profile(profile)

    def _resolve_user_label(self, user_id: int) -> str:
        cached = self.bot.get_user(user_id)
        if cached is not None:
            return ge.display_name_of(cached)
        return f"User {user_id}"

    def build_daily_embed(self, user: discord.abc.User, daily_status: dict[str, Any]) -> discord.Embed:
        profile = daily_status["profile"]
        puzzle = daily_status["puzzle"]
        result = daily_status["result"]
        embed = discord.Embed(
            title="Babblebox Daily",
            description="Today's shared solo puzzle is a lightweight word shuffle. Everyone gets the same one per UTC day.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(name="Puzzle", value=f"`{puzzle.scramble}`", inline=False)
        embed.add_field(name="Hint", value=puzzle.hint, inline=False)
        embed.add_field(name="Length", value=str(puzzle.length), inline=True)
        embed.add_field(name="Streak", value=str(profile["active_streak"]), inline=True)
        if result is None:
            embed.add_field(
                name="How To Play",
                value=f"Use `/daily play <guess>` or `bb!daily play <guess>`. You get {DAILY_MAX_ATTEMPTS} attempts.",
                inline=False,
            )
        elif result.get("solved"):
            embed.add_field(
                name="Today's Result",
                value=(
                    f"Solved in **{result['attempt_count']}** attempt(s) and "
                    f"**{int(result.get('solve_seconds', 0) or 0)}s**.\nUse `/daily share` to post it."
                ),
                inline=False,
            )
        elif result.get("completed_at") is not None:
            embed.add_field(
                name="Today's Result",
                value=(
                    f"You used all {DAILY_MAX_ATTEMPTS} attempts. The answer was **{puzzle.answer.upper()}**.\n"
                    "Use `/daily share` if you still want to post the run."
                ),
                inline=False,
            )
        else:
            remaining = DAILY_MAX_ATTEMPTS - int(result.get("attempt_count", 0) or 0)
            embed.add_field(
                name="Progress",
                value=f"Attempts used: **{result['attempt_count']} / {DAILY_MAX_ATTEMPTS}**\nAttempts left: **{remaining}**",
                inline=False,
            )
        return ge.style_embed(embed, footer="Babblebox Daily | Shared UTC puzzle, compact results, no rerolls")

    def build_daily_result_embed(self, user: discord.abc.User, payload: dict[str, Any]) -> discord.Embed:
        profile = payload["profile"]
        puzzle = payload["puzzle"]
        result = payload["result"]
        if payload["status"] == "solved":
            title = "Daily Cleared"
            tone = "success"
            description = (
                f"**{ge.display_name_of(user)}** solved Daily #{puzzle.challenge_number} in "
                f"**{result['attempt_count']}** attempt(s) and **{int(result.get('solve_seconds', 0) or 0)}s**."
            )
        elif payload["status"] == "failed":
            title = "Daily Finished"
            tone = "warning"
            description = f"That was the last attempt for today. The answer was **{puzzle.answer.upper()}**."
        else:
            title = "Not Quite Yet"
            tone = "info"
            remaining = DAILY_MAX_ATTEMPTS - int(result.get("attempt_count", 0) or 0)
            description = f"That guess was not it. You still have **{remaining}** attempt(s) left today."
        embed = ge.make_status_embed(title, description, tone=tone, footer="Babblebox Daily")
        embed.add_field(name="Puzzle", value=f"`{puzzle.scramble}`", inline=False)
        embed.add_field(name="Hint", value=puzzle.hint, inline=False)
        embed.add_field(name="Current Streak", value=str(profile["active_streak"]), inline=True)
        embed.add_field(name="Total Clears", value=str(profile["total_daily_clears"]), inline=True)
        return embed

    def build_daily_stats_embed(self, user: discord.abc.User, stats_payload: dict[str, Any]) -> discord.Embed:
        profile = stats_payload["profile"]
        recent = stats_payload["recent_results"]
        embed = discord.Embed(
            title="Babblebox Daily Stats",
            description=f"Daily puzzle history for **{ge.display_name_of(user)}**.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(name="Current Streak", value=str(profile["active_streak"]), inline=True)
        embed.add_field(name="Best Streak", value=str(profile["best_daily_streak"]), inline=True)
        embed.add_field(name="Total Clears", value=str(profile["total_daily_clears"]), inline=True)
        embed.add_field(name="Participations", value=str(profile["total_daily_participations"]), inline=True)
        recent_lines = []
        for row in recent[:5]:
            marker = "cleared" if row.get("solved") else "tried"
            recent_lines.append(f"**{row['puzzle_date'].isoformat()}** - {marker} {int(row.get('attempt_count', 0) or 0)}/{DAILY_MAX_ATTEMPTS}")
        embed.add_field(name="Recent Runs", value="\n".join(recent_lines) if recent_lines else "No runs recorded yet.", inline=False)
        return ge.style_embed(embed, footer="Babblebox Daily | Raw rows prune after 180 days, summary stats stay")

    def build_daily_leaderboard_embed(self, entries: list[dict[str, Any]], *, metric: str) -> discord.Embed:
        title = "Babblebox Daily Leaderboard"
        if not entries:
            return ge.make_status_embed(title, "No Daily results are on the board yet.", tone="info", footer="Babblebox Daily")
        lines = []
        for index, entry in enumerate(entries[:10], start=1):
            label = self._resolve_user_label(entry["user_id"])
            if metric == "streak":
                lines.append(
                    f"**{index}.** {label} - streak **{entry['active_streak']}** "
                    f"(best {entry['best_daily_streak']}, clears {entry['total_daily_clears']})"
                )
            else:
                lines.append(
                    f"**{index}.** {label} - clears **{entry['total_daily_clears']}** "
                    f"(streak {entry['active_streak']}, best {entry['best_daily_streak']})"
                )
        embed = discord.Embed(
            title=title,
            description="\n".join(lines),
            color=ge.EMBED_THEME["accent"],
        )
        return ge.style_embed(embed, footer="Babblebox Daily | Shared UTC challenge, compact lifetime stats")

    def build_buddy_embed(self, user: discord.abc.User, profile: dict[str, Any]) -> discord.Embed:
        species = profile["species_meta"]
        embed = discord.Embed(
            title="Babblebox Buddy",
            description=f"**{profile['buddy_name']}** is your {species['label'].lower()} companion.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(name="Type", value=species["badge"], inline=True)
        embed.add_field(name="Mood", value=profile["resolved_mood"], inline=True)
        embed.add_field(name="Style", value=profile["style_meta"]["label"], inline=True)
        embed.add_field(name="Title", value=profile["display_title"], inline=True)
        embed.add_field(
            name="Level",
            value=f"Level **{profile['level']}**\nXP: **{profile['xp_into_level']} / {profile['xp_needed_this_level']}**",
            inline=True,
        )
        embed.add_field(name="Streak", value=str(profile["active_streak"]), inline=True)
        embed.add_field(name="Badges", value=_format_badges(profile["badges_unlocked"]), inline=False)
        embed.add_field(name="Buddy Note", value=species["description"], inline=False)
        return ge.style_embed(embed, footer="Babblebox Buddy | Lightweight, cosmetic-first, anti-farm progression")

    def build_buddy_stats_embed(self, user: discord.abc.User, profile: dict[str, Any]) -> discord.Embed:
        embed = discord.Embed(
            title="Buddy Stats",
            description=f"Progress summary for **{profile['buddy_name']}**.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(name="Level", value=str(profile["level"]), inline=True)
        embed.add_field(name="Total XP", value=str(profile["xp_total"]), inline=True)
        embed.add_field(name="Daily Clears", value=str(profile["total_daily_clears"]), inline=True)
        embed.add_field(name="Utilities Used", value=str(_utility_score(profile)), inline=True)
        embed.add_field(name="Games Played", value=str(profile["games_played"]), inline=True)
        embed.add_field(name="Games Won", value=str(profile["games_won"]), inline=True)
        unlocked_titles = [TITLE_DEFINITIONS[title]["label"] for title in profile["titles_unlocked"]]
        embed.add_field(name="Unlocked Titles", value=", ".join(unlocked_titles) if unlocked_titles else "None yet", inline=False)
        embed.add_field(name="Badges", value=_format_badges(profile["badges_unlocked"]), inline=False)
        embed.add_field(name="XP Rules", value="Daily, utility, and game XP are all capped per UTC day to keep progression fair.", inline=False)
        return ge.style_embed(embed, footer="Babblebox Buddy | No inventory grind, no economy spam")

    def build_profile_embed(
        self,
        target: discord.abc.User,
        profile: dict[str, Any],
        *,
        utility_summary: dict[str, Any] | None,
        session_stats: dict[str, Any] | None,
        title: str = "Babblebox Profile",
    ) -> discord.Embed:
        species = profile["species_meta"]
        embed = discord.Embed(
            title=title,
            description=f"**{ge.display_name_of(target)}** with buddy **{profile['buddy_name']}**.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(
            name="Buddy",
            value=(
                f"{species['label']} | {profile['style_meta']['label']} style\n"
                f"Mood: **{profile['resolved_mood']}** | Title: **{profile['display_title']}**\n"
                f"Badges: {_format_badges(profile['badges_unlocked'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Daily",
            value=(
                f"Streak: **{profile['active_streak']}**\n"
                f"Best: **{profile['best_daily_streak']}**\n"
                f"Clears: **{profile['total_daily_clears']} / {profile['total_daily_participations']}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Progress",
            value=(
                f"Level **{profile['level']}**\n"
                f"XP this level: **{profile['xp_into_level']} / {profile['xp_needed_this_level']}**\n"
                f"Games won: **{profile['games_won']}**"
            ),
            inline=True,
        )
        if utility_summary is not None:
            embed.add_field(
                name="Utilities",
                value=(
                    f"Watch active: **{'Yes' if utility_summary['watch_enabled'] else 'No'}**\n"
                    f"Later markers: **{utility_summary['active_later_markers']}**\n"
                    f"Active reminders: **{utility_summary['active_reminders']}**\n"
                    f"Capture uses: **{profile['capture_uses']}**"
                ),
                inline=False,
            )
        else:
            embed.add_field(
                name="Utilities",
                value=(
                    f"Later saves: **{profile['later_saves']}**\n"
                    f"Reminders made: **{profile['reminders_created']}**\n"
                    f"AFK sessions: **{profile['afk_sessions']}**\n"
                    "Private utility settings stay personal."
                ),
                inline=False,
            )
        game_lines = [
            f"Rounds: telephone **{profile['telephone_rounds']}**, corpse **{profile['corpse_rounds']}**, spyfall **{profile['spyfall_rounds']}**, bomb **{profile['bomb_rounds']}**",
            f"Highlights: telephone clears **{profile['telephone_completions']}**, corpse masterpieces **{profile['corpse_masterpieces']}**, bomb wins **{profile['bomb_wins']}**",
        ]
        if session_stats is not None:
            game_lines.append(
                f"Session snapshot: wins **{session_stats.get('wins', 0)}**, bomb words **{session_stats.get('bomb_words', 0)}**"
            )
        embed.add_field(name="Party Games", value="\n".join(game_lines), inline=False)
        return ge.style_embed(embed, footer="Babblebox Profile | Utilities, Daily, Buddy, and games in one compact view")
