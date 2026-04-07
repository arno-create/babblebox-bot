from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
from typing import Any

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.daily_challenges import (
    DAILY_DEFAULT_MODE,
    DAILY_MAX_ATTEMPTS,
    build_daily_arcade,
    build_daily_puzzle,
    build_daily_shuffle,
    list_daily_modes,
    normalize_daily_guess,
    resolve_daily_mode,
)
from babblebox.question_drops_content import answer_points_for_difficulty
from babblebox.question_drops_style import category_label_with_emoji, progression_emoji, scholar_label
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

DAILY_MODE_ICONS = {
    "shuffle": "\U0001f500",
    "emoji": "\u2728",
    "signal": "\U0001f4e1",
}

BUDDY_SPECIES_ICONS = {
    "cloudlet": "\u2601\ufe0f",
    "bytebug": "\U0001f41b",
    "sprout": "\U0001f331",
    "puddlefox": "\U0001f98a",
    "moondrop": "\U0001f319",
}

BUDDY_MOOD_COPY = {
    "locked in": "Arcade lights on, eyes forward, already reaching for the next clear.",
    "celebrating": "Still glowing from a recent win and ready to show it off.",
    "sparky": "Chatty, bright, and looking for a little momentum.",
    "steady": "Settled into a calm rhythm with your quieter utility wins.",
    "proud": "Keeping score in the softest possible way.",
    "determined": "Regrouping now so tomorrow's board feels better.",
    "focused": "Leaning over the controls and not wasting an attempt.",
    "hyped": "Ready for the next party-room moment.",
    "sleepy": "Resting for now, but still keeping your corner warm.",
    "curious": "Poking around for the next cozy thing to do.",
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
    "pattern_hunt": "pattern_hunt_rounds",
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
        "pattern_hunt_rounds": 0,
        "pattern_hunt_wins": 0,
        "question_drop_attempts": 0,
        "question_drop_correct": 0,
        "question_drop_points": 0,
        "question_drop_current_streak": 0,
        "question_drop_best_streak": 0,
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
            cells.append("\u2b1c")
        elif solved and index == attempts - 1:
            cells.append("\U0001f7e9")
        else:
            cells.append("\U0001f7e8")
    return "".join(cells)


def _level_meter(current: int, total: int, *, width: int = 8) -> str:
    if total <= 0:
        return "\u25a1" * width
    ratio = max(0.0, min(1.0, current / total))
    filled = int(round(ratio * width))
    if current > 0 and filled == 0:
        filled = 1
    return ("\u25a0" * filled) + ("\u25a1" * max(0, width - filled))


def _daily_mode_icon(mode: str) -> str:
    return DAILY_MODE_ICONS.get(mode, "\u2022")


def _buddy_icon(profile: dict[str, Any]) -> str:
    return BUDDY_SPECIES_ICONS.get(str(profile.get("buddy_species", "")), "\u2728")


def _buddy_mood_line(profile: dict[str, Any]) -> str:
    return BUDDY_MOOD_COPY.get(str(profile.get("resolved_mood", "")), "Keeping your corner of Babblebox cozy.")


def _level_track(profile: dict[str, Any]) -> str:
    return (
        f"Level **{profile['level']}**  {_level_meter(int(profile['xp_into_level']), int(profile['xp_needed_this_level']))}\n"
        f"**{profile['xp_into_level']} / {profile['xp_needed_this_level']} XP** this level"
    )


def _blank_question_drop_category(user_id: int, category: str) -> dict[str, Any]:
    return {
        "user_id": user_id,
        "category": category,
        "attempts": 0,
        "correct_count": 0,
        "points": 0,
        "current_streak": 0,
        "best_streak": 0,
    }


def _blank_question_drop_guild_profile(guild_id: int, user_id: int) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "attempts": 0,
        "correct_count": 0,
        "points": 0,
        "current_streak": 0,
        "best_streak": 0,
    }


def _blank_question_drop_guild_category(guild_id: int, user_id: int, category: str) -> dict[str, Any]:
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "category": category,
        "attempts": 0,
        "correct_count": 0,
        "points": 0,
        "current_streak": 0,
        "best_streak": 0,
    }


def _global_question_drop_snapshot(profile: dict[str, Any]) -> dict[str, int]:
    return {
        "attempts": int(profile.get("question_drop_attempts", 0) or 0),
        "correct_count": int(profile.get("question_drop_correct", 0) or 0),
        "points": int(profile.get("question_drop_points", 0) or 0),
        "current_streak": int(profile.get("question_drop_current_streak", 0) or 0),
        "best_streak": int(profile.get("question_drop_best_streak", 0) or 0),
    }


def _scholar_tier_from_unlocks(unlocks: list[dict[str, Any]]) -> int:
    scholar_tiers = {
        int(item.get("tier", 0) or 0)
        for item in unlocks
        if str(item.get("scope_type", "")).casefold() == "scholar"
    }
    return max(scholar_tiers, default=0)


def _top_mastery_from_unlocks(unlocks: list[dict[str, Any]]) -> tuple[str | None, int]:
    top_category = None
    top_tier = 0
    for item in unlocks:
        if str(item.get("scope_type", "")).casefold() != "category":
            continue
        tier = int(item.get("tier", 0) or 0)
        category = str(item.get("scope_key", "")).strip().casefold()
        if tier > top_tier:
            top_category = category
            top_tier = tier
    return top_category, top_tier


def _knowledge_accuracy(row: dict[str, Any]) -> int:
    attempts = int(row.get("attempts", 0) or 0)
    correct = int(row.get("correct_count", 0) or 0)
    if attempts <= 0:
        return 0
    return int(round((correct / attempts) * 100.0))


def _question_drop_role_preference_payload(
    *,
    guild_id: int,
    user_id: int,
    opt_out_row: dict[str, Any] | None,
) -> dict[str, Any]:
    opted_out_at = opt_out_row.get("opted_out_at") if isinstance(opt_out_row, dict) else None
    return {
        "guild_id": guild_id,
        "user_id": user_id,
        "role_grants_enabled": opted_out_at is None,
        "opted_out_at": opted_out_at,
    }


def _daily_booth_line(mode: str, puzzle, result: dict[str, Any] | None, *, active_mode: str) -> str:
    prefix = "\u25b6" if mode == active_mode else "\u2022"
    return f"{prefix} {_daily_mode_icon(mode)} **{puzzle.label}** | {_daily_progress_line(result)}"


def _daily_modes() -> tuple[str, ...]:
    return list_daily_modes()


def _daily_mode_label(mode: str) -> str:
    return build_daily_puzzle(ge.now_utc().date(), mode).label


def _daily_progress_line(result: dict[str, Any] | None) -> str:
    if result is None:
        return "Fresh"
    attempts = int(result.get("attempt_count", 0) or 0)
    if result.get("solved"):
        solve_seconds = int(result.get("solve_seconds", 0) or 0)
        if solve_seconds > 0:
            return f"Clear {attempts}/{DAILY_MAX_ATTEMPTS} | {solve_seconds}s"
        return f"Clear {attempts}/{DAILY_MAX_ATTEMPTS}"
    if result.get("completed_at") is not None:
        return f"Wrapped {attempts}/{DAILY_MAX_ATTEMPTS}"
    return f"{attempts}/{DAILY_MAX_ATTEMPTS} used"


def _daily_guess_command(mode: str) -> str:
    if mode == DAILY_DEFAULT_MODE:
        return "/daily play <guess>"
    return f"/daily play {mode} <guess>"


def _daily_share_command(mode: str) -> str:
    if mode == DAILY_DEFAULT_MODE:
        return "/daily share"
    return f"/daily share {mode}"


def _daily_active_progress_line(
    puzzle,
    result: dict[str, Any] | None,
    *,
    public: bool,
) -> str:
    if result is None:
        return f"Progress: Fresh board | {DAILY_MAX_ATTEMPTS} tries left"
    attempts = int(result.get("attempt_count", 0) or 0)
    if result.get("solved"):
        solve_seconds = int(result.get("solve_seconds", 0) or 0)
        if solve_seconds > 0:
            return f"Progress: Clear {attempts}/{DAILY_MAX_ATTEMPTS} | {solve_seconds}s | share ready"
        return f"Progress: Clear {attempts}/{DAILY_MAX_ATTEMPTS} | share ready"
    if result.get("completed_at") is not None:
        if public:
            return f"Progress: Wrapped {attempts}/{DAILY_MAX_ATTEMPTS} | share ready summary"
        return f"Progress: Wrapped {attempts}/{DAILY_MAX_ATTEMPTS} | answer **{puzzle.answer.upper()}**"
    remaining = max(0, DAILY_MAX_ATTEMPTS - attempts)
    return f"Progress: {attempts}/{DAILY_MAX_ATTEMPTS} used | {remaining} left"


def _daily_hint_line(hint: str | None) -> str | None:
    hint_text = str(hint or "").strip()
    if not hint_text:
        return None
    if hint_text.startswith("||") and hint_text.endswith("||"):
        return f"Hint: {hint_text}"
    return f"Hint: ||{hint_text}||"


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
        removed = await self.store.prune_daily_results(keep_after=keep_after)
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

    async def get_question_drop_role_preference(self, user_id: int, *, guild_id: int) -> dict[str, Any]:
        if not self.storage_ready or not isinstance(guild_id, int) or guild_id <= 0:
            return _question_drop_role_preference_payload(guild_id=max(0, int(guild_id or 0)), user_id=int(user_id), opt_out_row=None)
        async with self._lock:
            opt_out_row = await self.store.fetch_question_drop_role_opt_out(guild_id=guild_id, user_id=user_id)
            return _question_drop_role_preference_payload(guild_id=guild_id, user_id=user_id, opt_out_row=opt_out_row)

    async def set_question_drop_role_grants_enabled(self, user_id: int, *, guild_id: int, enabled: bool) -> dict[str, Any]:
        if not self.storage_ready or not isinstance(guild_id, int) or guild_id <= 0:
            return _question_drop_role_preference_payload(guild_id=max(0, int(guild_id or 0)), user_id=int(user_id), opt_out_row=None)
        async with self._lock:
            if enabled:
                await self.store.delete_question_drop_role_opt_out(guild_id=guild_id, user_id=user_id)
                opt_out_row = None
            else:
                opt_out_row = {
                    "guild_id": guild_id,
                    "user_id": int(user_id),
                    "opted_out_at": ge.now_utc(),
                }
                await self.store.save_question_drop_role_opt_out(opt_out_row)
            return _question_drop_role_preference_payload(guild_id=guild_id, user_id=user_id, opt_out_row=opt_out_row)

    async def get_question_drop_summary(self, user_id: int, *, guild_id: int | None = None) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        async with self._lock:
            profile = await self._ensure_profile(user_id)
            today = ge.now_utc().date()
            self._sync_identity_fields(profile, today)
            global_categories = await self.store.fetch_question_drop_categories(user_id=user_id)
            guild_profile = None
            guild_categories: list[dict[str, Any]] = []
            guild_rank = None
            guild_unlocks: list[dict[str, Any]] = []
            guild_role_preference = None
            if isinstance(guild_id, int) and guild_id > 0:
                guild_profile = await self.store.fetch_question_drop_guild_profile(guild_id=guild_id, user_id=user_id)
                if guild_profile is None:
                    guild_profile = _blank_question_drop_guild_profile(guild_id, user_id)
                guild_categories = await self.store.fetch_question_drop_guild_categories(guild_id=guild_id, user_id=user_id)
                guild_rank = await self.store.fetch_question_drop_guild_rank(guild_id=guild_id, user_id=user_id)
                guild_unlocks = await self.store.fetch_question_drop_unlocks(guild_id=guild_id, user_id=user_id)
                guild_role_preference = _question_drop_role_preference_payload(
                    guild_id=guild_id,
                    user_id=user_id,
                    opt_out_row=await self.store.fetch_question_drop_role_opt_out(guild_id=guild_id, user_id=user_id),
                )
            top_categories = guild_categories[:3] if guild_categories else global_categories[:3]
            return {
                "profile": self._enrich_profile(profile),
                "global_profile": _global_question_drop_snapshot(profile),
                "global_categories": global_categories,
                "categories": guild_categories if guild_categories else global_categories,
                "top_categories": top_categories,
                "guild_id": guild_id,
                "guild_profile": guild_profile,
                "guild_categories": guild_categories,
                "guild_rank": guild_rank,
                "guild_unlocks": guild_unlocks,
                "guild_role_preference": guild_role_preference,
            }

    async def get_question_drop_leaderboard(self, *, guild_id: int, category: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
        if not self.storage_ready:
            return []
        if not isinstance(guild_id, int) or guild_id <= 0:
            return []
        normalized_category = str(category or "").strip().lower()
        if normalized_category:
            return await self.store.fetch_question_drop_guild_category_leaderboard(guild_id=guild_id, category=normalized_category, limit=limit)
        return await self.store.fetch_question_drop_guild_leaderboard(guild_id=guild_id, limit=limit)

    async def backfill_question_drop_guild_points_from_exposures(self, *, guild_id: int, exposures: list[dict[str, Any]]) -> dict[str, int]:
        if not self.storage_ready or not isinstance(guild_id, int) or guild_id <= 0:
            return {"updated_profiles": 0, "updated_categories": 0}
        meta_key = f"question_drop_guild_backfill:{guild_id}"
        async with self._lock:
            existing_meta = await self.store.get_meta(meta_key)
            if isinstance(existing_meta, dict) and existing_meta.get("completed_at"):
                return {
                    "updated_profiles": int(existing_meta.get("updated_profiles", 0) or 0),
                    "updated_categories": int(existing_meta.get("updated_categories", 0) or 0),
                }
            points_by_user: dict[int, int] = {}
            correct_by_user: dict[int, int] = {}
            points_by_category: dict[tuple[int, str], int] = {}
            correct_by_category: dict[tuple[int, str], int] = {}
            for exposure in exposures:
                if not isinstance(exposure, dict):
                    continue
                winner_user_id = exposure.get("winner_user_id")
                if not isinstance(winner_user_id, int) or winner_user_id <= 0:
                    continue
                category_id = str(exposure.get("category") or "").strip().lower()
                if not category_id:
                    continue
                points = answer_points_for_difficulty(int(exposure.get("difficulty", 1) or 1))
                points_by_user[winner_user_id] = points_by_user.get(winner_user_id, 0) + points
                correct_by_user[winner_user_id] = correct_by_user.get(winner_user_id, 0) + 1
                category_key = (winner_user_id, category_id)
                points_by_category[category_key] = points_by_category.get(category_key, 0) + points
                correct_by_category[category_key] = correct_by_category.get(category_key, 0) + 1
            updated_profiles = 0
            updated_categories = 0
            for user_id, points in points_by_user.items():
                guild_profile = await self.store.fetch_question_drop_guild_profile(guild_id=guild_id, user_id=user_id)
                if guild_profile is None:
                    guild_profile = _blank_question_drop_guild_profile(guild_id, user_id)
                guild_profile["points"] = max(int(guild_profile.get("points", 0) or 0), points)
                guild_profile["correct_count"] = max(int(guild_profile.get("correct_count", 0) or 0), correct_by_user.get(user_id, 0))
                await self.store.save_question_drop_guild_profile(guild_profile)
                updated_profiles += 1
            for (user_id, category_id), points in points_by_category.items():
                guild_category = await self.store.fetch_question_drop_guild_category(guild_id=guild_id, user_id=user_id, category=category_id)
                if guild_category is None:
                    guild_category = _blank_question_drop_guild_category(guild_id, user_id, category_id)
                guild_category["points"] = max(int(guild_category.get("points", 0) or 0), points)
                guild_category["correct_count"] = max(int(guild_category.get("correct_count", 0) or 0), correct_by_category.get((user_id, category_id), 0))
                await self.store.save_question_drop_guild_category(guild_category)
                updated_categories += 1
            await self.store.set_meta(
                meta_key,
                {
                    "completed_at": ge.now_utc().isoformat(),
                    "updated_profiles": updated_profiles,
                    "updated_categories": updated_categories,
                    "winner_rows": len(points_by_category),
                },
            )
        return {"updated_profiles": updated_profiles, "updated_categories": updated_categories}

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

    def _resolve_daily_mode_or_default(self, mode: str | None) -> str:
        return resolve_daily_mode(mode) or DAILY_DEFAULT_MODE

    def _blank_daily_result(self, *, challenge_id: str, puzzle_date: date, user_id: int, now: datetime) -> dict[str, Any]:
        return {
            "challenge_id": challenge_id,
            "puzzle_date": puzzle_date,
            "user_id": user_id,
            "attempt_count": 0,
            "solved": False,
            "first_attempt_at": now,
            "completed_at": None,
            "solve_seconds": None,
        }

    async def _fetch_daily_arcade_state(self, user_id: int, *, mode: str | None = None) -> dict[str, Any]:
        today = ge.now_utc().date()
        profile = await self._ensure_profile(user_id)
        self._sync_identity_fields(profile, today)
        puzzles = build_daily_arcade(today)
        results: dict[str, dict[str, Any] | None] = {}
        for mode_id, puzzle in puzzles.items():
            results[mode_id] = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
        active_mode = self._resolve_daily_mode_or_default(mode)
        solved_today = sum(1 for result in results.values() if isinstance(result, dict) and result.get("solved"))
        finished_today = sum(1 for result in results.values() if isinstance(result, dict) and result.get("completed_at") is not None)
        return {
            "profile": self._enrich_profile(profile),
            "puzzles": puzzles,
            "results": results,
            "active_mode": active_mode,
            "puzzle": puzzles[active_mode],
            "result": results.get(active_mode),
            "solved_today": solved_today,
            "finished_today": finished_today,
        }

    async def get_daily_status(self, user_id: int, *, mode: str | None = None) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        async with self._lock:
            return await self._fetch_daily_arcade_state(user_id, mode=mode)

    async def submit_daily_guess(self, user_id: int, guess: str, *, mode: str = DAILY_DEFAULT_MODE) -> tuple[bool, dict[str, Any] | str]:
        if not self.storage_ready:
            return False, self.storage_message("Daily")
        normalized = normalize_daily_guess(guess)
        if not normalized:
            return False, "Your guess has to include letters."
        async with self._lock:
            now = ge.now_utc()
            today = now.date()
            active_mode = self._resolve_daily_mode_or_default(mode)
            puzzle = build_daily_puzzle(today, active_mode)
            profile = await self._ensure_profile(user_id)
            result = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
            if result is None:
                result = self._blank_daily_result(
                    challenge_id=puzzle.challenge_id,
                    puzzle_date=today,
                    user_id=user_id,
                    now=now,
                )
            if result.get("solved"):
                return False, f"You already solved {puzzle.label}. Use `/daily share {active_mode}` to post it."
            if result.get("completed_at") is not None or int(result.get("attempt_count", 0) or 0) >= DAILY_MAX_ATTEMPTS:
                return False, f"You already finished {puzzle.label} today. Come back at the next UTC reset."

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
                    self._touch_profile(profile, mood="determined")
                    status = "failed"
                else:
                    self._touch_profile(profile, mood="focused")
                    status = "retry"

            self._sync_identity_fields(profile, today)
            await self.store.save_daily_result(result)
            await self.store.save_profile(profile)
            arcade_state = await self._fetch_daily_arcade_state(user_id, mode=active_mode)
            arcade_state.update({"status": status, "puzzle": puzzle, "result": result})
            return True, arcade_state

    async def build_daily_share(self, user_id: int, *, mode: str = DAILY_DEFAULT_MODE) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Daily")
        async with self._lock:
            today = ge.now_utc().date()
            active_mode = self._resolve_daily_mode_or_default(mode)
            puzzle = build_daily_puzzle(today, active_mode)
            profile = await self._ensure_profile(user_id)
            result = await self.store.fetch_daily_result(
                challenge_id=puzzle.challenge_id,
                puzzle_date=today,
                user_id=user_id,
            )
            if result is None:
                return False, f"You have not played {puzzle.label} yet."
            if not result.get("solved") and result.get("completed_at") is None:
                return False, f"Finish {puzzle.label} first, then use `/daily share {active_mode}`."
            self._sync_identity_fields(profile, today)
            share_line = (
                f"Babblebox Daily Arcade #{puzzle.challenge_number} {_daily_mode_icon(active_mode)} {puzzle.label} | "
                f"{'Solved' if result.get('solved') else 'Tried'} {int(result.get('attempt_count', 0) or 0)}/{DAILY_MAX_ATTEMPTS}"
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
            recent = await self.store.fetch_recent_daily_results(user_id=user_id, limit=9)
            self._sync_identity_fields(profile, today)
            arcade = build_daily_arcade(today)
            challenge_ids = {puzzle.challenge_id: mode for mode, puzzle in arcade.items()}
            today_results = {}
            for mode, puzzle in arcade.items():
                today_results[mode] = await self.store.fetch_daily_result(
                    challenge_id=puzzle.challenge_id,
                    puzzle_date=today,
                    user_id=user_id,
                )
            return {
                "profile": self._enrich_profile(profile),
                "recent_results": recent,
                "today_puzzles": arcade,
                "today_results": today_results,
                "challenge_modes": challenge_ids,
            }

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

    async def record_pattern_hunt_win(self, winner_id: int):
        if not self.storage_ready:
            return
        async with self._lock:
            today = ge.now_utc().date()
            profile = await self._ensure_profile(winner_id)
            profile["games_won"] = int(profile.get("games_won", 0) or 0) + 1
            profile["pattern_hunt_wins"] = int(profile.get("pattern_hunt_wins", 0) or 0) + 1
            self._grant_xp(profile, bucket="game", amount=GAME_WIN_XP, today=today)
            self._touch_profile(profile, mood="celebrating")
            self._sync_identity_fields(profile, today)
            await self.store.save_profile(profile)

    async def record_question_drop_result(self, user_id: int, *, guild_id: int, category: str, correct: bool, points: int):
        if not self.storage_ready:
            return {}
        category_id = str(category or "").strip().lower()
        if not category_id:
            return {}
        granted_points = max(0, int(points))
        async with self._lock:
            today = ge.now_utc().date()
            update = await self._apply_question_drop_result_locked(
                today=today,
                guild_id=guild_id,
                user_id=user_id,
                category_id=category_id,
                correct=bool(correct),
                granted_points=granted_points,
            )
        return update

    async def record_question_drop_results_batch(self, results: list[dict[str, Any]], *, guild_id: int):
        if not self.storage_ready:
            return {}
        normalized: dict[tuple[int, str], dict[str, Any]] = {}
        for raw in results:
            if not isinstance(raw, dict):
                continue
            user_id = raw.get("user_id")
            if not isinstance(user_id, int) or user_id <= 0:
                continue
            category_id = str(raw.get("category") or "").strip().lower()
            if not category_id:
                continue
            key = (user_id, category_id)
            candidate = normalized.setdefault(
                key,
                {
                    "user_id": user_id,
                    "category_id": category_id,
                    "correct": False,
                    "points": 0,
                },
            )
            candidate["correct"] = bool(candidate["correct"] or raw.get("correct"))
            if raw.get("correct"):
                candidate["points"] = max(int(candidate["points"]), max(0, int(raw.get("points", 0) or 0)))
        if not normalized:
            return {}
        updates: dict[int, dict[str, Any]] = {}
        async with self._lock:
            today = ge.now_utc().date()
            for result in normalized.values():
                update = await self._apply_question_drop_result_locked(
                    today=today,
                    guild_id=guild_id,
                    user_id=int(result["user_id"]),
                    category_id=str(result["category_id"]),
                    correct=bool(result["correct"]),
                    granted_points=max(0, int(result["points"])),
                )
                updates[int(result["user_id"])] = update
        return updates

    async def _apply_question_drop_result_locked(
        self,
        *,
        today,
        guild_id: int,
        user_id: int,
        category_id: str,
        correct: bool,
        granted_points: int,
    ) -> dict[str, Any]:
        profile = await self._ensure_profile(user_id)
        before_global = _global_question_drop_snapshot(profile)
        before_guild = await self.store.fetch_question_drop_guild_profile(guild_id=guild_id, user_id=user_id)
        if before_guild is None:
            before_guild = _blank_question_drop_guild_profile(guild_id, user_id)
        guild_profile = dict(before_guild)
        profile["question_drop_attempts"] = int(profile.get("question_drop_attempts", 0) or 0) + 1
        if correct:
            profile["question_drop_correct"] = int(profile.get("question_drop_correct", 0) or 0) + 1
            profile["question_drop_points"] = int(profile.get("question_drop_points", 0) or 0) + granted_points
            next_streak = int(profile.get("question_drop_current_streak", 0) or 0) + 1
            profile["question_drop_current_streak"] = next_streak
            profile["question_drop_best_streak"] = max(int(profile.get("question_drop_best_streak", 0) or 0), next_streak)
            mood = "proud"
        else:
            profile["question_drop_current_streak"] = 0
            mood = "focused"
        category_row = await self.store.fetch_question_drop_category(user_id=user_id, category=category_id)
        if category_row is None:
            category_row = _blank_question_drop_category(user_id, category_id)
        before_global_category = dict(category_row)
        category_row["attempts"] = int(category_row.get("attempts", 0) or 0) + 1
        if correct:
            category_row["correct_count"] = int(category_row.get("correct_count", 0) or 0) + 1
            category_row["points"] = int(category_row.get("points", 0) or 0) + granted_points
            next_category_streak = int(category_row.get("current_streak", 0) or 0) + 1
            category_row["current_streak"] = next_category_streak
            category_row["best_streak"] = max(int(category_row.get("best_streak", 0) or 0), next_category_streak)
        else:
            category_row["current_streak"] = 0
        before_guild_category = await self.store.fetch_question_drop_guild_category(guild_id=guild_id, user_id=user_id, category=category_id)
        if before_guild_category is None:
            before_guild_category = _blank_question_drop_guild_category(guild_id, user_id, category_id)
        guild_category_row = dict(before_guild_category)
        before_rank = await self.store.fetch_question_drop_guild_rank(guild_id=guild_id, user_id=user_id)
        before_category_rank = await self.store.fetch_question_drop_guild_category_rank(guild_id=guild_id, user_id=user_id, category=category_id)
        guild_profile["attempts"] = int(guild_profile.get("attempts", 0) or 0) + 1
        if correct:
            guild_profile["correct_count"] = int(guild_profile.get("correct_count", 0) or 0) + 1
            guild_profile["points"] = int(guild_profile.get("points", 0) or 0) + granted_points
            next_guild_streak = int(guild_profile.get("current_streak", 0) or 0) + 1
            guild_profile["current_streak"] = next_guild_streak
            guild_profile["best_streak"] = max(int(guild_profile.get("best_streak", 0) or 0), next_guild_streak)
        else:
            guild_profile["current_streak"] = 0
        guild_category_row["attempts"] = int(guild_category_row.get("attempts", 0) or 0) + 1
        if correct:
            guild_category_row["correct_count"] = int(guild_category_row.get("correct_count", 0) or 0) + 1
            guild_category_row["points"] = int(guild_category_row.get("points", 0) or 0) + granted_points
            next_guild_category_streak = int(guild_category_row.get("current_streak", 0) or 0) + 1
            guild_category_row["current_streak"] = next_guild_category_streak
            guild_category_row["best_streak"] = max(int(guild_category_row.get("best_streak", 0) or 0), next_guild_category_streak)
        else:
            guild_category_row["current_streak"] = 0
        self._touch_profile(profile, mood=mood)
        self._sync_identity_fields(profile, today)
        await self.store.save_question_drop_category(category_row)
        await self.store.save_question_drop_guild_category(guild_category_row)
        await self.store.save_question_drop_guild_profile(guild_profile)
        await self.store.save_profile(profile)
        after_rank = await self.store.fetch_question_drop_guild_rank(guild_id=guild_id, user_id=user_id)
        after_category_rank = await self.store.fetch_question_drop_guild_category_rank(guild_id=guild_id, user_id=user_id, category=category_id)
        return {
            "user_id": user_id,
            "guild_id": guild_id,
            "category": category_id,
            "correct": bool(correct),
            "points_awarded": granted_points if correct else 0,
            "global_before": before_global,
            "global_after": _global_question_drop_snapshot(profile),
            "global_category_before": before_global_category,
            "global_category_after": dict(category_row),
            "guild_before": before_guild,
            "guild_after": dict(guild_profile),
            "guild_category_before": before_guild_category,
            "guild_category_after": dict(guild_category_row),
            "guild_rank_before": before_rank,
            "guild_rank_after": after_rank,
            "category_rank_before": before_category_rank,
            "category_rank_after": after_category_rank,
        }

    def _resolve_user_label(self, user_id: int) -> str:
        get_user = getattr(self.bot, "get_user", None)
        cached = get_user(user_id) if callable(get_user) else None
        if cached is not None:
            return ge.display_name_of(cached)
        return f"User {user_id}"

    def _knowledge_context(self, profile: dict[str, Any], knowledge_summary: dict[str, Any] | None) -> dict[str, Any]:
        summary = knowledge_summary if isinstance(knowledge_summary, dict) else {}
        global_profile = summary.get("global_profile") or _global_question_drop_snapshot(profile)
        global_categories = summary.get("global_categories") or []
        guild_profile = summary.get("guild_profile")
        guild_categories = summary.get("guild_categories") or []
        unlocks = summary.get("guild_unlocks") or []
        has_guild_scope = isinstance(guild_profile, dict)
        primary_profile = guild_profile if has_guild_scope else global_profile
        primary_categories = guild_categories if has_guild_scope else global_categories
        scope_label = "This server" if has_guild_scope else "Lifetime"
        scholar_tier = _scholar_tier_from_unlocks(unlocks)
        mastery_category, mastery_tier = _top_mastery_from_unlocks(unlocks)
        top_category = primary_categories[0] if primary_categories else None
        lifetime_top_category = global_categories[0] if has_guild_scope and not primary_categories and global_categories else None
        return {
            "scope_label": scope_label,
            "primary_profile": primary_profile,
            "primary_categories": primary_categories,
            "global_profile": global_profile,
            "global_categories": global_categories,
            "guild_rank": summary.get("guild_rank"),
            "scholar_tier": scholar_tier,
            "scholar_label": scholar_label(scholar_tier) if scholar_tier > 0 else "No scholar rank yet",
            "mastery_category": mastery_category,
            "mastery_tier": mastery_tier,
            "top_category": top_category,
            "top_category_scope_label": scope_label if top_category is not None else None,
            "lifetime_top_category": lifetime_top_category,
        }

    def build_daily_embed(self, user: discord.abc.User, daily_status: dict[str, Any], *, public: bool = False) -> discord.Embed:
        profile = daily_status["profile"]
        puzzles = daily_status["puzzles"]
        results = daily_status["results"]
        active_mode = daily_status["active_mode"]
        puzzle = daily_status["puzzle"]
        result = daily_status["result"]
        hint_line = _daily_hint_line(getattr(puzzle, "hint", None))
        description_lines = [
            *[
                _daily_booth_line(mode, puzzles[mode], results.get(mode), active_mode=active_mode)
                for mode in _daily_modes()
            ],
            "",
            f"{puzzle.prompt_label}: `{puzzle.scramble}`",
            f"Difficulty: **{puzzle.difficulty_label}** | Length: **{puzzle.length}** | Profile: **{puzzle.profile.title()}**",
            _daily_active_progress_line(puzzle, result, public=public),
            f"Today: **{daily_status['solved_today']} / {len(puzzles)}** cleared | streak **{profile['active_streak']}**",
        ]
        if hint_line is not None:
            description_lines.insert(-2, hint_line)
        if not public:
            description_lines.append(f"You: **{profile['total_daily_clears']}** clears | level **{profile['level']}**")
        embed = discord.Embed(
            title=f"Daily Arcade #{puzzle.challenge_number}",
            description="\n".join(description_lines),
            color=profile["style_meta"]["color"],
        )
        if result is None:
            next_step = f"Guess: `{_daily_guess_command(active_mode)}`"
        elif result.get("solved"):
            next_step = f"Share: `{_daily_share_command(active_mode)}`"
        elif result.get("completed_at") is not None:
            next_step = f"Share: `{_daily_share_command(active_mode)}`"
        else:
            next_step = f"Guess: `{_daily_guess_command(active_mode)}`"
        embed.add_field(name="Next", value=next_step, inline=False)
        return ge.style_embed(embed, footer="Babblebox Daily Arcade | Shared UTC booths, spoiler-safe public cards")

    def build_daily_result_embed(self, user: discord.abc.User, payload: dict[str, Any], *, public: bool = False) -> discord.Embed:
        profile = payload["profile"]
        puzzle = payload["puzzle"]
        result = payload["result"]
        actor = f"**{ge.display_name_of(user)}**" if public else "You"
        if payload["status"] == "solved":
            title = f"{puzzle.label} Cleared"
            tone = "success"
            lead = f"{actor} cleared Daily Arcade #{puzzle.challenge_number}."
        elif payload["status"] == "failed":
            title = f"{puzzle.label} Wrapped"
            tone = "warning"
            lead = f"{actor} used the last try for Daily Arcade #{puzzle.challenge_number}."
        else:
            title = f"{puzzle.label} Still Live"
            tone = "info"
            lead = "Not it yet."
        hint_line = _daily_hint_line(getattr(puzzle, "hint", None))
        description_lines = [
            lead,
            _share_grid(result),
            f"{puzzle.prompt_label}: `{puzzle.scramble}`",
            f"Difficulty: **{puzzle.difficulty_label}** | Length: **{puzzle.length}** | Profile: **{puzzle.profile.title()}**",
            _daily_active_progress_line(puzzle, result, public=public),
        ]
        if hint_line is not None:
            description_lines.insert(-1, hint_line)
        if not public:
            description_lines.append(f"You: **{profile['total_daily_clears']}** clears | level **{profile['level']}**")
        if payload["status"] == "solved":
            description_lines.append(f"Share: `{_daily_share_command(puzzle.mode)}`")
        elif payload["status"] == "failed":
            description_lines.append(f"Share: `{_daily_share_command(puzzle.mode)}`")
        else:
            description_lines.append(f"Guess: `{_daily_guess_command(puzzle.mode)}`")
        embed = discord.Embed(
            title=title,
            description="\n".join(description_lines),
            color=ge.EMBED_THEME.get(tone, ge.EMBED_THEME["info"]),
        )
        return ge.style_embed(embed, footer="Babblebox Daily Arcade | Spoiler-safe public cards")

    def build_daily_stats_embed(self, user: discord.abc.User, stats_payload: dict[str, Any], *, public: bool = False) -> discord.Embed:
        profile = stats_payload["profile"]
        recent = stats_payload["recent_results"]
        today_puzzles = stats_payload["today_puzzles"]
        today_results = stats_payload["today_results"]
        challenge_modes = stats_payload["challenge_modes"]
        summary_bits = [
            f"Streak **{profile['active_streak']}**",
            f"Best **{profile['best_daily_streak']}**",
            f"Clears **{profile['total_daily_clears']}**",
            f"Booths **{profile['total_daily_participations']}**",
        ]
        if not public:
            summary_bits.append(f"Level **{profile['level']}**")
        embed = discord.Embed(
            title=f"Daily Stats | {ge.display_name_of(user)}",
            description=" | ".join(summary_bits),
            color=profile["style_meta"]["color"],
        )
        today_lines = [
            _daily_booth_line(mode, puzzle, today_results.get(mode), active_mode=mode)
            for mode, puzzle in today_puzzles.items()
        ]
        embed.add_field(name="Today", value="\n".join(today_lines), inline=False)
        recent_lines = []
        for row in recent[:6]:
            mode = challenge_modes.get(row["challenge_id"], "shuffle")
            label = _daily_mode_label(mode)
            recent_lines.append(
                f"{row['puzzle_date'].isoformat()} | {_daily_mode_icon(mode)} {label} | {_daily_progress_line(row)}"
            )
        embed.add_field(name="Recent", value="\n".join(recent_lines) if recent_lines else "No runs recorded yet.", inline=False)
        return ge.style_embed(embed, footer="Babblebox Daily Arcade | Raw rows prune after 180 days, summary streaks stay")

    def build_daily_leaderboard_embed(self, entries: list[dict[str, Any]], *, metric: str) -> discord.Embed:
        title = "Daily Arcade Leaderboard"
        if not entries:
            return ge.make_status_embed(title, "No Daily Arcade results are on the board yet.", tone="info", footer="Babblebox Daily Arcade")
        lines = []
        for index, entry in enumerate(entries[:10], start=1):
            label = self._resolve_user_label(entry["user_id"])
            rank = {1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949"}.get(index, f"{index}.")
            if metric == "streak":
                lines.append(
                    f"**{rank}** {label} - streak **{entry['active_streak']}** "
                    f"(best {entry['best_daily_streak']}, clears {entry['total_daily_clears']})"
                )
            else:
                lines.append(
                    f"**{rank}** {label} - clears **{entry['total_daily_clears']}** "
                    f"(streak {entry['active_streak']}, best {entry['best_daily_streak']})"
                )
        embed = discord.Embed(
            title=title,
            description="\n".join(lines),
            color=ge.EMBED_THEME["accent"],
        )
        return ge.style_embed(embed, footer="Babblebox Daily Arcade | Shared UTC booths, compact lifetime stats")

    def build_buddy_embed(self, user: discord.abc.User, profile: dict[str, Any], *, knowledge_summary: dict[str, Any] | None = None) -> discord.Embed:
        species = profile["species_meta"]
        knowledge = self._knowledge_context(profile, knowledge_summary)
        primary = knowledge["primary_profile"]
        top_category = knowledge["top_category"]
        lifetime_top_category = knowledge.get("lifetime_top_category")
        if isinstance(top_category, dict):
            lead_prefix = "Lifetime lead" if knowledge.get("top_category_scope_label") == "Lifetime" else "Server lead"
            top_category_text = (
                f"{lead_prefix}: {category_label_with_emoji(top_category['category'])} | **{int(top_category.get('points', 0) or 0)}** pts"
            )
        elif isinstance(lifetime_top_category, dict):
            top_category_text = (
                f"Lifetime lead: {category_label_with_emoji(lifetime_top_category['category'])} | "
                f"**{int(lifetime_top_category.get('points', 0) or 0)}** pts"
            )
        else:
            top_category_text = "No category lead yet"
        mastery_text = (
            f"{progression_emoji('mastery')} {category_label_with_emoji(knowledge['mastery_category'])} | Tier **{knowledge['mastery_tier']}**"
            if knowledge["mastery_category"] and int(knowledge["mastery_tier"] or 0) > 0
            else f"{progression_emoji('mastery')} No mastery badge yet"
        )
        embed = discord.Embed(
            title=f"Buddy | {profile['buddy_name']}",
            description=(
                f"{_buddy_icon(profile)} **{species['label']}** companion feeling **{profile['resolved_mood']}**.\n"
                f"{_buddy_mood_line(profile)}"
            ),
            color=profile["style_meta"]["color"],
        )
        embed.add_field(
            name="Identity",
            value=(
                f"Badge: **{species['badge']}**\n"
                f"Style: **{profile['style_meta']['label']}**\n"
                f"Featured title: **{profile['display_title']}**"
            ),
            inline=True,
        )
        embed.add_field(name="Level Track", value=_level_track(profile), inline=True)
        embed.add_field(
            name="Arcade Glow",
            value=(
                f"Streak: **{profile['active_streak']}**\n"
                f"Lifetime clears: **{profile['total_daily_clears']}**\n"
                f"Games won: **{profile['games_won']}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="🎓 Knowledge Lane",
            value=(
                f"{knowledge['scope_label']}: **{int(primary.get('points', 0) or 0)}** pts\n"
                f"{progression_emoji('streak')} Streak: **{int(primary.get('current_streak', 0) or 0)}**\n"
                f"{progression_emoji('scholar')} {knowledge['scholar_label']}"
            ),
            inline=True,
        )
        embed.add_field(name="Badges", value=_format_badges(profile["badges_unlocked"]), inline=False)
        embed.add_field(
            name="Little Snapshot",
            value=(
                f"Utilities used: **{_utility_score(profile)}**\n"
                f"Games played: **{profile['games_played']}**\n"
                f"{top_category_text}\n"
                f"{mastery_text}"
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Buddy | Cosmetic-first progression with daily caps")

    def build_buddy_stats_embed(self, user: discord.abc.User, profile: dict[str, Any], *, knowledge_summary: dict[str, Any] | None = None) -> discord.Embed:
        knowledge = self._knowledge_context(profile, knowledge_summary)
        primary = knowledge["primary_profile"]
        lifetime_top_category = knowledge.get("lifetime_top_category") or knowledge.get("top_category")
        flavor_lines = [
            f"{progression_emoji('scholar')} {knowledge['scholar_label']}",
            f"{progression_emoji('streak')} Best streak: **{int(primary.get('best_streak', 0) or 0)}**",
            f"{progression_emoji('next')} Lifetime total: **{int(knowledge['global_profile'].get('points', 0) or 0)}** pts",
        ]
        if isinstance(lifetime_top_category, dict):
            flavor_lines.append(
                f"Lifetime lead: {category_label_with_emoji(lifetime_top_category['category'])} | "
                f"**{int(lifetime_top_category.get('points', 0) or 0)}** pts"
            )
        embed = discord.Embed(
            title=f"Buddy Stats | {profile['buddy_name']}",
            description="Progress, titles, badges, and the activity mix shaping your companion.",
            color=profile["style_meta"]["color"],
        )
        embed.add_field(name="Level Track", value=_level_track(profile), inline=False)
        embed.add_field(
            name="Activity Mix",
            value=(
                f"Mood: **{profile['resolved_mood']}**\n"
                f"Arcade clears: **{profile['total_daily_clears']}**\n"
                f"Utilities used: **{_utility_score(profile)}**\n"
                f"Games won: **{profile['games_won']}**\n"
                f"{knowledge['scope_label']} knowledge: **{int(primary.get('points', 0) or 0)}** pts"
            ),
            inline=True,
        )
        unlocked_titles = [TITLE_DEFINITIONS[title]["label"] for title in profile["titles_unlocked"]]
        embed.add_field(name="Unlocked Titles", value=", ".join(unlocked_titles) if unlocked_titles else "None yet", inline=True)
        embed.add_field(name="Badges", value=_format_badges(profile["badges_unlocked"]), inline=False)
        embed.add_field(
            name="Knowledge Flavor",
            value="\n".join(flavor_lines),
            inline=False,
        )
        embed.add_field(
            name="XP Rules",
            value="Daily, utility, and game XP are capped per UTC day so progress stays cozy instead of grindy.",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Buddy | No inventory, no economy spam, no lootbox grind")

    def build_profile_embed(
        self,
        target: discord.abc.User,
        profile: dict[str, Any],
        *,
        knowledge_summary: dict[str, Any] | None = None,
        utility_summary: dict[str, Any] | None,
        session_stats: dict[str, Any] | None,
        title: str = "Babblebox Profile",
    ) -> discord.Embed:
        species = profile["species_meta"]
        is_vault = title.lower().endswith("vault")
        knowledge = self._knowledge_context(profile, knowledge_summary)
        primary = knowledge["primary_profile"]
        guild_rank_text = f"#{knowledge['guild_rank']}" if isinstance(knowledge.get("guild_rank"), int) else "Unranked"
        top_category = knowledge["top_category"]
        lifetime_top_category = knowledge.get("lifetime_top_category")
        if isinstance(top_category, dict):
            lead_prefix = "Lifetime lead" if knowledge.get("top_category_scope_label") == "Lifetime" else "Server lead"
            top_category_text = (
                f"{lead_prefix}: {category_label_with_emoji(top_category['category'])} | "
                f"**{int(top_category.get('points', 0) or 0)}** pts"
            )
        elif isinstance(lifetime_top_category, dict):
            top_category_text = (
                f"Lifetime lead: {category_label_with_emoji(lifetime_top_category['category'])} | "
                f"**{int(lifetime_top_category.get('points', 0) or 0)}** pts"
            )
        else:
            top_category_text = "No top category yet"
        embed = discord.Embed(
            title=title,
            description=(
                f"**{ge.display_name_of(target)}** with buddy **{profile['buddy_name']}** in the Babblebox lounge.\n"
                f"{'Private snapshot with live utility context.' if is_vault else 'Showable snapshot of knowledge, arcade, buddy, and party highlights.'}"
            ),
            color=profile["style_meta"]["color"],
        )
        embed.add_field(
            name="Identity",
            value=(
                f"{_buddy_icon(profile)} {species['label']} | {profile['style_meta']['label']} style\n"
                f"Mood: **{profile['resolved_mood']}** | Title: **{profile['display_title']}**\n"
                f"Badges: {_format_badges(profile['badges_unlocked'])}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Daily Arcade",
            value=(
                f"Streak: **{profile['active_streak']}**\n"
                f"Best: **{profile['best_daily_streak']}**\n"
                f"Clears: **{profile['total_daily_clears']} / {profile['total_daily_participations']}**"
            ),
            inline=True,
        )
        embed.add_field(name="Level Track", value=_level_track(profile), inline=True)
        embed.add_field(
            name="🎓 Knowledge Lane",
            value=(
                f"{knowledge['scope_label']}: **{int(primary.get('points', 0) or 0)}** pts\n"
                f"Solved: **{int(primary.get('correct_count', 0) or 0)} / {int(primary.get('attempts', 0) or 0)}**\n"
                f"{progression_emoji('streak')} Best streak: **{int(primary.get('best_streak', 0) or 0)}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Knowledge Highlights",
            value=(
                f"{progression_emoji('scholar')} {knowledge['scholar_label']}\n"
                f"{progression_emoji('move')} Rank: **{guild_rank_text}**\n"
                f"{top_category_text}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Lifetime Flavor",
            value=(
                f"Question Drop points: **{int(knowledge['global_profile'].get('points', 0) or 0)}**\n"
                f"Solved: **{int(knowledge['global_profile'].get('correct_count', 0) or 0)} / {int(knowledge['global_profile'].get('attempts', 0) or 0)}**"
            ),
            inline=False,
        )
        if utility_summary is not None:
            embed.add_field(
                name="Quiet Utility",
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
                name="Everyday Utility",
                value=(
                    f"Later saves: **{profile['later_saves']}**\n"
                    f"Reminders made: **{profile['reminders_created']}**\n"
                    f"AFK sessions: **{profile['afk_sessions']}**\n"
                    "Private utility settings stay personal."
                ),
                inline=False,
            )
        game_lines = [
            (
                f"Rounds: telephone **{profile['telephone_rounds']}**, corpse **{profile['corpse_rounds']}**, "
                f"spyfall **{profile['spyfall_rounds']}**, bomb **{profile['bomb_rounds']}**, "
                f"pattern hunt **{profile['pattern_hunt_rounds']}**"
            ),
            (
                f"Highlights: telephone clears **{profile['telephone_completions']}**, corpse masterpieces **{profile['corpse_masterpieces']}**, "
                f"bomb wins **{profile['bomb_wins']}**, pattern hunt wins **{profile['pattern_hunt_wins']}**"
            ),
        ]
        if session_stats is not None:
            game_lines.append(
                f"Session snapshot: wins **{session_stats.get('wins', 0)}**, bomb words **{session_stats.get('bomb_words', 0)}**"
            )
        embed.add_field(name="Party Games", value="\n".join(game_lines), inline=False)
        footer = "Babblebox Vault | Personal snapshot with live utility context" if is_vault else "Babblebox Profile | Party games, daily arcade, utilities, and buddy identity together"
        return ge.style_embed(embed, footer=footer)
