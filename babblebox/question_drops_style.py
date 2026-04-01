from __future__ import annotations

from babblebox.question_drops_content import QUESTION_DROP_CATEGORY_LABELS


CATEGORY_EMOJIS = {
    "science": "\U0001f52c",
    "history": "\U0001f4dc",
    "geography": "\U0001f30d",
    "language": "\u270d\ufe0f",
    "logic": "\U0001f9e0",
    "math": "\u2797",
    "culture": "\U0001f3ad",
}

PROGRESSION_EMOJIS = {
    "role": "\U0001f3c5",
    "next": "\u23eb",
    "mastery": "\U0001f451",
    "streak": "\U0001f525",
    "move": "\U0001f4c8",
    "scholar": "\U0001f393",
}


def category_label(category: str) -> str:
    normalized = str(category or "").strip().casefold()
    return QUESTION_DROP_CATEGORY_LABELS.get(normalized, normalized.title() or "Unknown")


def category_emoji(category: str) -> str:
    return CATEGORY_EMOJIS.get(str(category or "").strip().casefold(), "\U0001f4da")


def category_label_with_emoji(category: str) -> str:
    return f"{category_emoji(category)} {category_label(category)}"


def progression_emoji(name: str, *, fallback: str = "\u2022") -> str:
    return PROGRESSION_EMOJIS.get(name, fallback)


def tier_label(tier: int) -> str:
    return {1: "Tier I", 2: "Tier II", 3: "Tier III"}.get(int(tier), f"Tier {tier}")


def scholar_label(tier: int) -> str:
    return {1: "Scholar I", 2: "Scholar II", 3: "Scholar III"}.get(int(tier), f"Scholar {tier}")


def leaderboard_marker(rank: int) -> str:
    return {1: "\U0001f947", 2: "\U0001f948", 3: "\U0001f949"}.get(int(rank), f"{rank}.")
