from __future__ import annotations

from typing import Iterable

from babblebox.premium_models import PLAN_FREE, PLAN_GUILD_PRO, PLAN_PLUS, PLAN_SUPPORTER


LIMIT_WATCH_KEYWORDS = "watch_keywords"
LIMIT_WATCH_FILTERS = "watch_filters"
LIMIT_REMINDERS_ACTIVE = "reminders_active"
LIMIT_REMINDERS_PUBLIC_ACTIVE = "reminders_public_active"
LIMIT_AFK_SCHEDULES = "afk_schedules"
LIMIT_BUMP_DETECTION_CHANNELS = "bump_detection_channels"
LIMIT_SHIELD_CUSTOM_PATTERNS = "shield_custom_patterns"
LIMIT_SHIELD_FILTERS = "shield_filters"
LIMIT_SHIELD_ALLOWLIST = "shield_allowlist"
LIMIT_SHIELD_PACK_EXEMPTIONS = "shield_pack_exemptions"
LIMIT_SHIELD_SEVERE_TERMS = "shield_severe_terms"
LIMIT_CONFESSIONS_MAX_IMAGES = "confessions_max_images"

CAPABILITY_SHIELD_AI_REVIEW = "shield_ai_review"

USER_PLAN_ORDER = (PLAN_FREE, PLAN_SUPPORTER, PLAN_PLUS)
GUILD_PLAN_ORDER = (PLAN_FREE, PLAN_GUILD_PRO)

USER_LIMITS = {
    PLAN_FREE: {
        LIMIT_WATCH_KEYWORDS: 10,
        LIMIT_WATCH_FILTERS: 8,
        LIMIT_REMINDERS_ACTIVE: 3,
        LIMIT_REMINDERS_PUBLIC_ACTIVE: 1,
        LIMIT_AFK_SCHEDULES: 6,
    },
    PLAN_SUPPORTER: {
        LIMIT_WATCH_KEYWORDS: 10,
        LIMIT_WATCH_FILTERS: 8,
        LIMIT_REMINDERS_ACTIVE: 3,
        LIMIT_REMINDERS_PUBLIC_ACTIVE: 1,
        LIMIT_AFK_SCHEDULES: 6,
    },
    PLAN_PLUS: {
        LIMIT_WATCH_KEYWORDS: 25,
        LIMIT_WATCH_FILTERS: 25,
        LIMIT_REMINDERS_ACTIVE: 15,
        LIMIT_REMINDERS_PUBLIC_ACTIVE: 5,
        LIMIT_AFK_SCHEDULES: 20,
    },
}

GUILD_LIMITS = {
    PLAN_FREE: {
        LIMIT_BUMP_DETECTION_CHANNELS: 5,
        LIMIT_SHIELD_CUSTOM_PATTERNS: 10,
        LIMIT_SHIELD_FILTERS: 20,
        LIMIT_SHIELD_ALLOWLIST: 20,
        LIMIT_SHIELD_PACK_EXEMPTIONS: 20,
        LIMIT_SHIELD_SEVERE_TERMS: 20,
        LIMIT_CONFESSIONS_MAX_IMAGES: 3,
    },
    PLAN_GUILD_PRO: {
        LIMIT_BUMP_DETECTION_CHANNELS: 15,
        LIMIT_SHIELD_CUSTOM_PATTERNS: 25,
        LIMIT_SHIELD_FILTERS: 50,
        LIMIT_SHIELD_ALLOWLIST: 50,
        LIMIT_SHIELD_PACK_EXEMPTIONS: 50,
        LIMIT_SHIELD_SEVERE_TERMS: 50,
        LIMIT_CONFESSIONS_MAX_IMAGES: 6,
    },
}

STORAGE_CEILINGS = {
    LIMIT_BUMP_DETECTION_CHANNELS: 15,
    LIMIT_SHIELD_CUSTOM_PATTERNS: 25,
    LIMIT_SHIELD_FILTERS: 50,
    LIMIT_SHIELD_ALLOWLIST: 50,
    LIMIT_SHIELD_PACK_EXEMPTIONS: 50,
    LIMIT_SHIELD_SEVERE_TERMS: 50,
    LIMIT_CONFESSIONS_MAX_IMAGES: 6,
}

GUILD_CAPABILITIES = {
    PLAN_FREE: frozenset(),
    PLAN_GUILD_PRO: frozenset({CAPABILITY_SHIELD_AI_REVIEW}),
}


def highest_user_plan(plans: Iterable[str]) -> str:
    available = set(plans)
    for plan_code in reversed(USER_PLAN_ORDER):
        if plan_code in available:
            return plan_code
    return PLAN_FREE


def highest_guild_plan(plans: Iterable[str]) -> str:
    available = set(plans)
    for plan_code in reversed(GUILD_PLAN_ORDER):
        if plan_code in available:
            return plan_code
    return PLAN_FREE


def user_limit(plan_code: str, limit_key: str) -> int:
    plan_limits = USER_LIMITS.get(plan_code) or USER_LIMITS[PLAN_FREE]
    return int(plan_limits.get(limit_key, USER_LIMITS[PLAN_FREE][limit_key]))


def guild_limit(plan_code: str, limit_key: str) -> int:
    plan_limits = GUILD_LIMITS.get(plan_code) or GUILD_LIMITS[PLAN_FREE]
    return int(plan_limits.get(limit_key, GUILD_LIMITS[PLAN_FREE][limit_key]))


def storage_ceiling(limit_key: str, fallback: int) -> int:
    return int(STORAGE_CEILINGS.get(limit_key, fallback))


def guild_capabilities(plan_code: str) -> frozenset[str]:
    return GUILD_CAPABILITIES.get(plan_code, frozenset())

