from __future__ import annotations

import asyncio
import contextlib
import random
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import discord

from babblebox import game_engine as ge


PATTERN_HUNT_GUESS_LIMIT = 3
PATTERN_HUNT_STRIKE_LIMIT = 3
PATTERN_HUNT_TUTORIAL_PROMPT_TIMEOUT_SECONDS = 90
PATTERN_HUNT_PROMPT_TIMEOUT_SECONDS = 75
PATTERN_HUNT_TUTORIAL_ANSWER_TIMEOUT_SECONDS = 75
PATTERN_HUNT_ANSWER_TIMEOUT_SECONDS = 60
PATTERN_HUNT_RECENT_SIGNATURES = 6
_PATTERN_HUNT_ANCHOR_SLOT = "pattern_hunt"
PATTERN_HUNT_RULE_FAMILIES = (
    "starts_with_letter",
    "ends_with_punctuation",
    "contains_digits",
    "contains_emoji",
    "contains_category_word",
    "forbid_letter",
    "exact_word_count",
    "word_count_range",
    "question_form",
    "same_initial_letter",
)

_WORD_RE = re.compile(r"[a-zA-Z']+")
_PROMPT_TOKEN_RE = re.compile(r"[a-zA-Z0-9']+")
_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]")
_DIGIT_RE = re.compile(r"\d")
_COLOR_WORDS = {"red", "blue", "green", "yellow", "orange", "purple", "black", "white"}
_ANIMAL_WORDS = {"cat", "dog", "fox", "owl", "bear", "whale", "shark", "lion", "tiger", "rabbit"}
_FOOD_WORDS = {"pizza", "taco", "apple", "bread", "soup", "burger", "noodle", "grape", "tea", "cookie"}
_CATEGORY_WORDS = {
    "color": _COLOR_WORDS,
    "animal": _ANIMAL_WORDS,
    "food": _FOOD_WORDS,
}
_FAMILY_LABELS = {
    "starts_with_letter": "Starts With Letter",
    "ends_with_punctuation": "Ends With Punctuation",
    "contains_digits": "Contains Digits",
    "contains_emoji": "Contains Emoji",
    "contains_category_word": "Contains Category Word",
    "forbid_letter": "Forbid Letter",
    "exact_word_count": "Exact Word Count",
    "word_count_range": "Word Count Range",
    "question_form": "Question Form",
    "same_initial_letter": "Same Initial Letter",
}
_PATTERN_HUNT_DRY_SOLO_FAMILIES = {"exact_word_count", "word_count_range", "ends_with_punctuation", "starts_with_letter", "forbid_letter"}
_PATTERN_HUNT_FINE_GRAINED_FAMILIES = {"exact_word_count", "word_count_range", "ends_with_punctuation", "starts_with_letter", "forbid_letter"}
_PATTERN_HUNT_SOCIAL_FAMILIES = {"contains_category_word", "question_form", "same_initial_letter", "contains_emoji", "contains_digits"}
_PATTERN_HUNT_LOW_SIGNAL_PROMPTS = {
    "fr",
    "help",
    "hmm",
    "hmmm",
    "huh",
    "idk",
    "k",
    "kk",
    "lmao",
    "lmfao",
    "lol",
    "nah",
    "no",
    "nope",
    "ok",
    "okay",
    "omg",
    "real",
    "same",
    "sure",
    "uh",
    "uhh",
    "umm",
    "wait",
    "what",
    "wild",
    "why",
    "yep",
    "yup",
}
_PATTERN_HUNT_LOW_SIGNAL_WORDS = _PATTERN_HUNT_LOW_SIGNAL_PROMPTS | {"haha", "hehe", "pls", "plz", "yo"}
_PATTERN_HUNT_PROMPT_ACTION_WORDS = {
    "animal",
    "another",
    "category",
    "clue",
    "color",
    "digit",
    "drop",
    "emoji",
    "food",
    "give",
    "hint",
    "line",
    "message",
    "number",
    "phrase",
    "prompt",
    "question",
    "send",
    "show",
    "something",
    "theme",
    "topic",
    "vibe",
    "word",
}
_SAMPLE_MESSAGES = (
    "Blue bears bake bread!",
    "Do green grapes glow?",
    "7 tiny foxes sprint.",
    "Red rabbits relax.",
    "Taco time tonight!",
    "Can black cats cook?",
    "12 bold owls watch.",
    "Sunny soup sings!",
    "Purple pizza party!",
    "Do orange foxes yodel?",
    "Three tiny tigers?",
    "Bright bread blooms.",
    "Whales whisper warmly.",
    "Blue birds bounce!",
    "9 lucky lions laugh.",
    "Can yellow noodles dance?",
    "Fresh tea trembles.",
    "Do white whales wink?",
    "Cats carry cookies.",
    "Green grapes glide gracefully.",
    "11 brave bears roar!",
    "Orange owls orbit.",
    "Do tacos taste terrific?",
    "Red roses rest.",
    "Pizza pirates parade!",
    "Do foxes bring bread?",
    "Whispering wolves wait.",
    "8 green goblets gleam.",
    "Can lions lick lemons?",
    "Tea time ticks.",
    "Bold blue balloons burst!",
    "Sharks share soup.",
    "Do purple bears bake?",
    "Happy noodles hum!",
    "10 tiny tacos tumble.",
    "Can red rabbits read?",
    "Bright birds bloom.",
    "Do cats carry cookies?",
    "Golden grapes grow.",
    "7 sleepy sharks smile.",
    "Blue bears bake bread 🍞!",
    "Do green grapes glow ✨?",
    "7 tiny foxes sprint 🦊.",
    "Sunny soup sings 🍜!",
    "Can black cats cook 🐈?",
    "Do white whales wink 🐳?",
)
_RECENT_RULE_SIGNATURES: dict[int, deque[tuple[tuple[str, str], ...]]] = defaultdict(
    lambda: deque(maxlen=PATTERN_HUNT_RECENT_SIGNATURES)
)


@dataclass(frozen=True)
class RuleAtom:
    family: str
    value: Any = None


def _normalize_text(text: str | None) -> str:
    return " ".join(str(text or "").strip().casefold().split())


def _normalize_pattern_prompt(text: str | None) -> str:
    return " ".join(str(text or "").split())


def _words(text: str | None) -> list[str]:
    return [token.casefold() for token in _WORD_RE.findall(str(text or ""))]


def _prompt_tokens(text: str | None) -> list[str]:
    return [token.casefold() for token in _PROMPT_TOKEN_RE.findall(str(text or ""))]


def _singularish_words(words: list[str]) -> list[str]:
    singularish = []
    for word in words:
        singularish.append(word)
        if word.endswith("es") and len(word) > 2:
            singularish.append(word[:-2])
        if word.endswith("s") and len(word) > 1:
            singularish.append(word[:-1])
    return singularish


def _matches_atom(atom: RuleAtom, text: str | None) -> bool:
    raw = str(text or "")
    lowered = _normalize_text(raw)
    words = _words(raw)
    singularish = _singularish_words(words)
    if atom.family == "starts_with_letter":
        first_letter = next((char.casefold() for char in raw if char.isalpha()), "")
        return first_letter == str(atom.value).casefold()
    if atom.family == "ends_with_punctuation":
        return raw.rstrip().endswith(str(atom.value))
    if atom.family == "contains_digits":
        return _DIGIT_RE.search(raw) is not None
    if atom.family == "contains_emoji":
        return _EMOJI_RE.search(raw) is not None
    if atom.family == "contains_category_word":
        return any(word in _CATEGORY_WORDS.get(str(atom.value), set()) for word in singularish)
    if atom.family == "forbid_letter":
        return str(atom.value).casefold() not in lowered
    if atom.family == "exact_word_count":
        return len(words) == int(atom.value)
    if atom.family == "word_count_range":
        minimum, maximum = atom.value
        return minimum <= len(words) <= maximum
    if atom.family == "question_form":
        return raw.rstrip().endswith("?")
    if atom.family == "same_initial_letter":
        initials = [word[0] for word in words if word]
        return len(initials) >= 2 and len(set(initials)) == 1
    return False


def message_matches_rule(rule_atoms: list[RuleAtom], text: str | None) -> bool:
    if not rule_atoms:
        return False
    return all(_matches_atom(atom, text) for atom in rule_atoms)


def rule_family_label(family: str) -> str:
    return _FAMILY_LABELS.get(family, family.replace("_", " ").title())


def render_rule_atom(atom: RuleAtom) -> str:
    if atom.family == "starts_with_letter":
        return f"starts with `{atom.value}`"
    if atom.family == "ends_with_punctuation":
        return f"ends with `{atom.value}`"
    if atom.family == "contains_digits":
        return "contains a digit (`0-9`)"
    if atom.family == "contains_emoji":
        return "contains an emoji"
    if atom.family == "contains_category_word":
        return f"includes a {atom.value} word"
    if atom.family == "forbid_letter":
        return f"does not contain `{atom.value}`"
    if atom.family == "exact_word_count":
        return f"has exactly {atom.value} words"
    if atom.family == "word_count_range":
        return f"has {atom.value[0]}-{atom.value[1]} words"
    if atom.family == "question_form":
        return "is phrased as a question"
    if atom.family == "same_initial_letter":
        return "uses words that all start with the same letter"
    return atom.family


def render_rule(rule_atoms: list[RuleAtom]) -> str:
    return "; ".join(render_rule_atom(atom) for atom in rule_atoms)


def validate_pattern_guess_atoms(guessed_atoms: list[RuleAtom]) -> tuple[bool, str | None]:
    if not guessed_atoms:
        return False, "Add at least one rule family to make a real guess."
    atoms: list[RuleAtom] = []
    seen_families: set[str] = set()
    for atom in guessed_atoms:
        if atom.family in seen_families:
            return False, "Use each rule family at most once per Pattern Hunt guess."
        if not _compatible(atoms, atom):
            return False, "That combination overlaps awkwardly. Try a cleaner rule bundle."
        atoms.append(atom)
        seen_families.add(atom.family)
    return True, None


def parse_guess_atom(family: str | None, value: str | None) -> tuple[bool, RuleAtom | str]:
    normalized_family = str(family or "").strip().casefold()
    if not normalized_family:
        return False, "Pick a rule family first."
    if normalized_family not in PATTERN_HUNT_RULE_FAMILIES:
        return False, "Unknown rule family."
    raw_value = str(value or "").strip()
    if normalized_family in {"contains_digits", "contains_emoji", "question_form", "same_initial_letter"}:
        return True, RuleAtom(normalized_family)
    if normalized_family in {"starts_with_letter", "forbid_letter"}:
        if len(raw_value) != 1 or not raw_value.isalpha():
            return False, "That rule needs a single letter."
        return True, RuleAtom(normalized_family, raw_value.casefold())
    if normalized_family == "ends_with_punctuation":
        if raw_value not in {"?", "!", "."}:
            return False, "That rule needs `?`, `!`, or `.`."
        return True, RuleAtom(normalized_family, raw_value)
    if normalized_family == "contains_category_word":
        category = raw_value.casefold()
        if category not in _CATEGORY_WORDS:
            return False, "That rule needs `color`, `animal`, or `food`."
        return True, RuleAtom(normalized_family, category)
    if normalized_family == "exact_word_count":
        if not raw_value.isdigit():
            return False, "That rule needs a whole number."
        return True, RuleAtom(normalized_family, int(raw_value))
    if normalized_family == "word_count_range":
        if "-" not in raw_value:
            return False, "That rule needs a range like `3-5`."
        left_text, right_text = [part.strip() for part in raw_value.split("-", 1)]
        if not left_text.isdigit() or not right_text.isdigit():
            return False, "That rule needs a range like `3-5`."
        left = int(left_text)
        right = int(right_text)
        if left > right:
            return False, "The lower bound must come first."
        return True, RuleAtom(normalized_family, (left, right))
    return False, "Unsupported rule family."


def _parse_pattern_theory_piece(piece: str) -> RuleAtom | None:
    text = _normalize_text(piece)
    if not text:
        return None
    direct = text.replace("-", "_").replace(" ", "_")
    if direct in PATTERN_HUNT_RULE_FAMILIES:
        ok, atom_or_message = parse_guess_atom(direct, None)
        return atom_or_message if ok else None
    if direct in {"contains_number", "contains_numbers", "contains_digit", "contains_digits", "number", "digits"}:
        return RuleAtom("contains_digits")
    if direct in {"contains_emoji", "emoji", "has_emoji"}:
        return RuleAtom("contains_emoji")
    if direct in {"question", "question_form", "is_question", "is_a_question"}:
        return RuleAtom("question_form")
    if direct in {"same_initial", "same_initial_letter", "all_words_same_letter", "all_words_start_same_letter"}:
        return RuleAtom("same_initial_letter")

    match = re.search(r"\bstarts?\s+with(?:\s+letter)?\s+([a-z])\b", text)
    if match:
        return RuleAtom("starts_with_letter", match.group(1))
    match = re.search(r"\b(?:does\s+not|doesn't|without|forbid(?:s|ding)?|no)\s+(?:contain\s+)?(?:the\s+)?(?:letter\s+)?([a-z])\b", text)
    if match:
        return RuleAtom("forbid_letter", match.group(1))
    match = re.search(r"\bends?\s+with\s+(\?|!|\.)", text)
    if match:
        return RuleAtom("ends_with_punctuation", match.group(1))
    if "question mark" in text:
        return RuleAtom("ends_with_punctuation", "?")
    if "exclamation" in text:
        return RuleAtom("ends_with_punctuation", "!")
    if "period" in text or "full stop" in text:
        return RuleAtom("ends_with_punctuation", ".")
    if re.search(r"\b\d+\s*-\s*\d+\s+words?\b", text):
        left, right = re.search(r"\b(\d+)\s*-\s*(\d+)\s+words?\b", text).groups()
        minimum = int(left)
        maximum = int(right)
        if minimum <= maximum:
            return RuleAtom("word_count_range", (minimum, maximum))
        return None
    match = re.search(r"\b(?:has|have|exactly|is)\s+(?:exactly\s+)?(\d+)\s+words?\b", text)
    if match:
        return RuleAtom("exact_word_count", int(match.group(1)))
    if re.search(r"\b(?:contains?|has|includes?)\s+(?:a\s+)?(?:number|digit)s?\b", text):
        return RuleAtom("contains_digits")
    if re.search(r"\b(?:contains?|has|includes?)\s+(?:an?\s+)?emoji\b", text):
        return RuleAtom("contains_emoji")
    for category in _CATEGORY_WORDS:
        if re.search(rf"\b(?:contains?|has|includes?)\s+(?:an?\s+)?{category}\s+word\b", text):
            return RuleAtom("contains_category_word", category)
        if text in {f"{category} word", f"contains {category}", f"has {category}", f"includes {category}"}:
            return RuleAtom("contains_category_word", category)
    if re.search(r"\b(?:is|must\s+be|phrased\s+as)\s+(?:a\s+)?question\b", text) or text == "a question":
        return RuleAtom("question_form")
    if "all words start with the same letter" in text or "same first letter" in text:
        return RuleAtom("same_initial_letter")
    return None


def parse_pattern_theory(theory: str | None) -> tuple[bool, list[RuleAtom] | str]:
    text = _normalize_text(theory)
    if not text:
        return False, "Try a natural theory like `contains a number`, `starts with b`, or `has 3-5 words`."
    pieces = [piece.strip() for piece in re.split(r"\s+(?:and|plus)\s+|\s*,\s*", text) if piece.strip()]
    atoms = []
    for piece in pieces:
        atom = _parse_pattern_theory_piece(piece)
        if atom is None:
            return False, "Try a natural theory like `contains a number`, `starts with b`, or `has 3-5 words`."
        atoms.append(atom)
    valid, message = validate_pattern_guess_atoms(atoms)
    if not valid:
        return False, message or "Try a cleaner natural theory."
    return True, atoms


def validate_pattern_prompt(text: str | None) -> tuple[bool, str | None]:
    prompt = _normalize_pattern_prompt(text)
    if not prompt:
        return False, None
    lowered = prompt.casefold()
    if lowered in _PATTERN_HUNT_LOW_SIGNAL_PROMPTS:
        return False, None
    tokens = _prompt_tokens(prompt)
    if not tokens:
        return False, None
    meaningful = [token for token in tokens if token not in _PATTERN_HUNT_LOW_SIGNAL_WORDS]
    if not meaningful:
        return False, None
    if prompt.endswith("?"):
        if len(meaningful) >= 2:
            return True, prompt
        if any(token in _PATTERN_HUNT_PROMPT_ACTION_WORDS for token in meaningful):
            return True, prompt
        if len(meaningful[0]) >= 3:
            return True, prompt
        return False, None
    if len(meaningful) >= 2:
        return True, prompt
    if any(token in _PATTERN_HUNT_PROMPT_ACTION_WORDS for token in meaningful):
        return True, prompt
    if len(meaningful[0]) >= 4:
        return True, prompt
    return False, None


def _rule_signature(rule_atoms: list[RuleAtom]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((atom.family, str(atom.value)) for atom in rule_atoms))


def _compatible(existing: list[RuleAtom], candidate: RuleAtom) -> bool:
    for atom in existing:
        if atom.family == candidate.family:
            return False
        if atom.family == "exact_word_count" and candidate.family == "word_count_range":
            return False
        if atom.family == "word_count_range" and candidate.family == "exact_word_count":
            return False
        if atom.family == "question_form" and candidate.family == "ends_with_punctuation":
            return False
        if candidate.family == "question_form" and atom.family == "ends_with_punctuation":
            return False
        if atom.family == "forbid_letter" and candidate.family == "starts_with_letter" and atom.value == candidate.value:
            return False
        if atom.family == "starts_with_letter" and candidate.family == "forbid_letter" and atom.value == candidate.value:
            return False
        if atom.family == "same_initial_letter" and candidate.family == "starts_with_letter":
            return False
        if atom.family == "starts_with_letter" and candidate.family == "same_initial_letter":
            return False
    return True


def _bundle_quality_ok(atoms: list[RuleAtom], valid_examples: list[str], invalid_examples: list[str]) -> bool:
    if len(valid_examples) < 3 or len(invalid_examples) < 3:
        return False
    unique_valid = {sample.casefold() for sample in valid_examples}
    if len(unique_valid) < 3:
        return False
    if len(atoms) == 1 and atoms[0].family in _PATTERN_HUNT_DRY_SOLO_FAMILIES:
        return False
    if len(atoms) >= 2 and not any(atom.family in _PATTERN_HUNT_SOCIAL_FAMILIES for atom in atoms):
        return False
    if len(atoms) >= 3 and sum(atom.family in _PATTERN_HUNT_FINE_GRAINED_FAMILIES for atom in atoms) >= 2:
        return False
    return True


def _atom_candidates(rng: random.Random) -> list[RuleAtom]:
    weighted_candidates: list[tuple[int, RuleAtom]] = [
        (3, RuleAtom("contains_category_word", rng.choice(("color", "animal", "food")))),
        (3, RuleAtom("question_form")),
        (3, RuleAtom("contains_digits")),
        (3, RuleAtom("same_initial_letter")),
        (2, RuleAtom("exact_word_count", rng.choice((2, 3, 4, 5)))),
        (2, RuleAtom("word_count_range", rng.choice(((2, 3), (3, 4), (3, 5))))),
        (2, RuleAtom("starts_with_letter", rng.choice(("b", "c", "g", "p", "t")))),
        (2, RuleAtom("forbid_letter", rng.choice(("a", "e", "i", "o", "u")))),
        (2, RuleAtom("ends_with_punctuation", rng.choice(("?", "!", ".")))),
        (1, RuleAtom("contains_emoji")),
    ]
    expanded = [atom for weight, atom in weighted_candidates for _ in range(weight)]
    rng.shuffle(expanded)
    return expanded


def select_rule_bundle(
    seed_value: int,
    *,
    recent_signatures: set[tuple[tuple[str, str], ...]] | None = None,
) -> tuple[list[RuleAtom], list[str], str]:
    recent_signatures = recent_signatures or set()
    rng = random.Random(seed_value)
    fallback: tuple[list[RuleAtom], list[str], str] | None = None
    for _ in range(300):
        atom_count = rng.choices((1, 2, 3), weights=(4, 5, 1))[0]
        atoms: list[RuleAtom] = []
        for candidate in _atom_candidates(rng):
            if _compatible(atoms, candidate):
                atoms.append(candidate)
            if len(atoms) >= atom_count:
                break
        if len(atoms) != atom_count:
            continue
        signature = _rule_signature(atoms)
        valid_examples = [sample for sample in _SAMPLE_MESSAGES if message_matches_rule(atoms, sample)]
        invalid_examples = [sample for sample in _SAMPLE_MESSAGES if not message_matches_rule(atoms, sample)]
        if not _bundle_quality_ok(atoms, valid_examples, invalid_examples):
            continue
        rng.shuffle(valid_examples)
        rng.shuffle(invalid_examples)
        candidate_bundle = (atoms, valid_examples[:2], invalid_examples[0])
        if fallback is None:
            fallback = candidate_bundle
        if signature in recent_signatures:
            continue
        return candidate_bundle
    if fallback is not None:
        return fallback
    fallback_atoms = [RuleAtom("contains_digits")]
    fallback_valid = [sample for sample in _SAMPLE_MESSAGES if _DIGIT_RE.search(sample)][:2]
    return fallback_atoms, fallback_valid, "Blue bears bake bread!"


def ensure_pattern_hunt_state(game: dict[str, Any]) -> dict[str, Any]:
    state = game.setdefault("pattern_hunt", {})
    state.setdefault("phase", "setup")
    state.setdefault("accepted_answers", [])
    if "clue_limit" not in state and "turn_limit" in state:
        state["clue_limit"] = state.get("turn_limit", 0)
    if "clues_used" not in state and "turns_used" in state:
        state["clues_used"] = state.get("turns_used", 0)
    state.setdefault("clue_limit", 0)
    state.setdefault("clues_used", 0)
    state.setdefault("deadline_at", None)
    state.setdefault("tutorial_cycle_active", False)
    state.setdefault("tutorial_grace_used", False)
    return state


def _pattern_hunt_clue_recap_lines(
    accepted_answers: list[dict[str, Any]],
    *,
    limit: int,
    prompt_limit: int,
    clue_limit: int,
) -> list[str]:
    lines: list[str] = []
    for item in accepted_answers[-limit:]:
        clue = ge.safe_field_text(item["answer"], limit=clue_limit)
        prompt = ge.safe_field_text(item.get("prompt") or "", limit=prompt_limit)
        if prompt:
            lines.append(f"`{prompt}` -> **{item['coder']}**: {clue}")
        else:
            lines.append(f"**{item['coder']}**: {clue}")
    return lines


def _pattern_hunt_prompt_timeout_seconds(state: dict[str, Any]) -> int:
    return PATTERN_HUNT_TUTORIAL_PROMPT_TIMEOUT_SECONDS if state.get("tutorial_cycle_active") else PATTERN_HUNT_PROMPT_TIMEOUT_SECONDS


def _pattern_hunt_answer_timeout_seconds(state: dict[str, Any]) -> int:
    return PATTERN_HUNT_TUTORIAL_ANSWER_TIMEOUT_SECONDS if state.get("tutorial_cycle_active") else PATTERN_HUNT_ANSWER_TIMEOUT_SECONDS


def _pattern_hunt_deadline_copy(deadline: Any) -> str:
    if deadline is None or not hasattr(deadline, "tzinfo"):
        return "No live timer."
    return f"{ge.format_timestamp(deadline, 'R')} ({ge.format_timestamp(deadline, 't')})"


def _pattern_hunt_progress_lines(state: dict[str, Any]) -> str:
    guesses_left = max(0, int(state.get("guess_limit", 3)) - int(state.get("guesses_used", 0)))
    misses_left = max(0, int(state.get("strike_limit", 3)) - int(state.get("strikes", 0)))
    return (
        f"Q&A: **{int(state.get('clues_used', 0))}/{int(state.get('clue_limit', 0))}**\n"
        f"Guesses left: **{guesses_left}**\n"
        f"Misses left: **{misses_left}**"
    )


def _pattern_hunt_next_step_copy(state: dict[str, Any]) -> str:
    if str(state.get("phase", "setup")).casefold() == "answer":
        return "After the answer lands, the next pattern holder takes the table."
    return "When the question lands, the named pattern holder answers publicly while keeping the pattern hidden."


def _pattern_hunt_now_copy(game: dict[str, Any]) -> str:
    state = ensure_pattern_hunt_state(game)
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
    guesser_name = ge.display_name_of(guesser) if guesser is not None else "The guesser"
    coder_name = ge.display_name_of(coder) if coder is not None else "the pattern holder"
    if str(state.get("phase", "setup")).casefold() == "answer":
        return (
            f"{coder_name}, answer the question in chat while following the hidden pattern.\n"
            f"{guesser_name} reads the answers and can lock in a private theory with `/hunt guess` using natural text."
        )
    return (
        f"{guesser_name}, ask {coder_name} a normal question in chat.\n"
        f"{coder_name}, answer naturally when the question lands."
    )


async def _refresh_pattern_hunt_anchor(game: dict[str, Any]):
    await ge.upsert_game_anchor(game, _PATTERN_HUNT_ANCHOR_SLOT, embed=build_pattern_hunt_status_embed(game, public=True))


def _finish_pattern_hunt_tutorial_cycle(state: dict[str, Any]):
    state["tutorial_cycle_active"] = False


def _pattern_hunt_first_round_copy() -> str:
    return (
        "1. The guesser asks the named pattern holder a normal question.\n"
        "2. The holder answers in chat while secretly following the pattern.\n"
        "3. Babblebox rotates the table; `/hunt guess` stays private when a theory is ready."
    )


async def start_pattern_hunt_game_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    players = list(game.get("players", []))
    if len(players) < 3:
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Not Enough Players",
                "Pattern Hunt needs at least 3 players so one person can guess while the pattern holders hide the rule.",
                tone="warning",
                footer="Babblebox Pattern Hunt",
            )
        )
        await ge.cleanup_game(guild_id)
        return
    secure_rng = random.SystemRandom()
    guesser = secure_rng.choice(players)
    coders = [player for player in players if player.id != guesser.id]
    recent_signatures = set(_RECENT_RULE_SIGNATURES[guild_id])
    seed_value = secure_rng.randrange(1, 1_000_000_000)
    rule_atoms, valid_examples, invalid_example = select_rule_bundle(seed_value, recent_signatures=recent_signatures)
    _RECENT_RULE_SIGNATURES[guild_id].append(_rule_signature(rule_atoms))
    for coder in coders:
        try:
            await coder.send(
                embed=ge.style_embed(
                    discord.Embed(
                        title="Pattern Hunt: Pattern Holder",
                        description="Keep this private. You know the hidden pattern; answer the guesser's question naturally when the table reaches you.",
                        color=ge.EMBED_THEME["accent"],
                    ).add_field(name="Secret Rule", value=render_rule(rule_atoms), inline=False).add_field(
                        name="Good Answer Examples",
                        value=f"- {valid_examples[0]}\n- {valid_examples[1]}",
                        inline=False,
                    ).add_field(
                        name="Misses the Rule",
                        value=f"- {invalid_example}",
                        inline=False,
                    ).add_field(
                        name="At the Table",
                        value="Wait until the public card names you, then answer naturally in chat. Keep the logic offstage. If Babblebox rejects an answer, send one fresh answer and move on.",
                        inline=False,
                    ),
                    footer="Babblebox Pattern Hunt | The guesser never sees this DM",
                )
            )
        except Exception:
            await game["channel"].send(
                embed=ge.make_status_embed(
                    "DM Failure",
                    (
                        f"I could not DM {coder.mention}, so Pattern Hunt stopped before the hidden rule got uneven. "
                        "Ask every pattern holder to open server DMs, then start the room again."
                    ),
                    tone="danger",
                    footer="Babblebox Pattern Hunt",
                ),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
            await ge.cleanup_game(guild_id)
            return
    with contextlib.suppress(Exception):
        await guesser.send(
            embed=ge.style_embed(
                discord.Embed(
                    title="Pattern Hunt: You Are Hunting",
                    description="Everyone else knows the hidden pattern. Ask the named pattern holder normal questions in chat and read their answers.",
                    color=discord.Color.dark_teal(),
                ).add_field(
                    name="How to Play",
                    value="Ask the person on the public card one casual question. When a theory clicks, use `/hunt guess` privately.",
                    inline=False,
                ).add_field(
                    name="Good Questions",
                    value="Try everyday prompts like `what did you bring?`, `what are you ordering?`, or `how is your day going?`.",
                    inline=False,
                ),
                footer="Babblebox Pattern Hunt | Your guesses stay private",
            )
        )
    turn_limit = max(5, (2 * len(coders)) + 1)
    state.update(
        {
            "phase": "prompt",
            "guesser_id": guesser.id,
            "coder_order": [player.id for player in coders],
            "current_coder_index": 0,
            "rule_atoms": rule_atoms,
            "valid_examples": valid_examples,
            "invalid_example": invalid_example,
            "guesses_used": 0,
            "guess_limit": PATTERN_HUNT_GUESS_LIMIT,
            "strikes": 0,
            "strike_limit": PATTERN_HUNT_STRIKE_LIMIT,
            "clues_used": 0,
            "clue_limit": turn_limit,
            "turns_used": 0,
            "turn_limit": turn_limit,
            "current_prompt": None,
            "retry_used": False,
            "hint_revealed": False,
            "hint_text": None,
            "accepted_answers": [],
            "deadline_at": None,
            "tutorial_cycle_active": True,
            "tutorial_grace_used": False,
        }
    )
    ge.mark_game_started(game)
    await _begin_pattern_turn_locked(guild_id, game)


def _pattern_hunt_private_role_copy(game: dict[str, Any], viewer: Any | None) -> tuple[str, str] | None:
    if viewer is None:
        return None
    state = ensure_pattern_hunt_state(game)
    viewer_id = getattr(viewer, "id", None)
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
    coder_name = ge.display_name_of(coder) if coder is not None else "the named pattern holder"
    if viewer_id == state.get("guesser_id"):
        return (
            "Your Role",
            (
                f"You are hunting: ask {coder_name} a normal question in chat, read the public answers, "
                "and use `/hunt guess` privately when your theory is ready."
            ),
        )
    if viewer_id in {int(user_id) for user_id in state.get("coder_order", [])}:
        if viewer_id == current_pattern_hunt_coder_id(game):
            lead = "You know the hidden rule and are the named pattern holder right now."
        else:
            lead = "You know the hidden rule; wait until the table reaches you."
        return (
            "Your Role",
            f"{lead} When the guesser asks, answer naturally when the question lands and keep the logic offstage.",
        )
    guesser_name = ge.display_name_of(guesser) if guesser is not None else "the guesser"
    return ("Your Role", f"Watching only. Let {guesser_name} and the named pattern holder drive the live question.")


def build_pattern_hunt_status_embed(game: dict[str, Any], *, public: bool, viewer: Any | None = None) -> discord.Embed:
    state = ensure_pattern_hunt_state(game)
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
    description = "Guesser-led table talk. One hidden pattern. Private natural theories stay in `/hunt guess`."
    if state.get("tutorial_cycle_active"):
        description += "\nFirst round is intentionally slower and more forgiving so the room can catch the rhythm."
    if public and state.get("hint_text"):
        description += f"\nShared hint: {state['hint_text']}"
    if not public:
        description += "\nUse `/hunt guess` with a natural theory like `contains a number`. `Contains Digits` means digits `0-9` only."

    embed = discord.Embed(title="🧩 Pattern Hunt", description=description, color=discord.Color.dark_teal())
    embed.add_field(
        name="Who's Up",
        value=(
            f"Guesser: **{ge.display_name_of(guesser) if guesser else 'Unknown'}**\n"
            f"Pattern holder: **{ge.display_name_of(coder) if coder else 'Unknown'}**"
        ),
        inline=True,
    )
    embed.add_field(name="Time Left", value=_pattern_hunt_deadline_copy(state.get("deadline_at")), inline=True)
    embed.add_field(name="Do This Now", value=_pattern_hunt_now_copy(game), inline=False)
    if state.get("current_prompt"):
        embed.add_field(name="Current Question", value=ge.safe_field_text(state.get("current_prompt"), limit=240), inline=False)
    embed.add_field(name="What Happens Next", value=_pattern_hunt_next_step_copy(state), inline=False)
    embed.add_field(name="Progress", value=_pattern_hunt_progress_lines(state), inline=False)
    role_field = None if public else _pattern_hunt_private_role_copy(game, viewer)
    if role_field is not None:
        embed.add_field(name=role_field[0], value=role_field[1], inline=False)
        viewer_id = getattr(viewer, "id", None)
        if viewer_id in {int(user_id) for user_id in state.get("coder_order", [])} and state.get("rule_atoms"):
            embed.add_field(name="Secret Pattern", value=render_rule(state.get("rule_atoms", [])), inline=False)
    if state.get("tutorial_cycle_active"):
        embed.add_field(
            name="First Round",
            value=_pattern_hunt_first_round_copy(),
            inline=False,
        )
    if not public:
        embed.add_field(
            name="Private Guess",
            value=(
                "Use one natural theory in `/hunt guess`, like `starts with b` or `has 3-5 words`.\n"
                "You can still combine clear families with `and`, like `contains a number and is a question`.\n"
                "`Contains Digits` means digits `0-9` only."
            ),
            inline=False,
        )
    if state.get("accepted_answers"):
        lines = _pattern_hunt_clue_recap_lines(
            state["accepted_answers"],
            limit=5,
            prompt_limit=36,
            clue_limit=64,
        )
        embed.add_field(name="Recent Q&A", value="\n".join(lines), inline=False)
    return ge.style_embed(embed, footer="Babblebox Pattern Hunt | Hidden rule stays private")


def current_pattern_hunt_coder_id(game: dict[str, Any]) -> int | None:
    state = ensure_pattern_hunt_state(game)
    coder_order = state.get("coder_order", [])
    if not coder_order:
        return None
    index = int(state.get("current_coder_index", 0) or 0) % len(coder_order)
    return int(coder_order[index])


async def handle_pattern_hunt_message_locked(message: discord.Message, guild_id: int, game: dict[str, Any]) -> bool:
    state = ensure_pattern_hunt_state(game)
    if message.channel.id != game["channel"].id:
        return False
    if not ge.is_player_in_game(game, message.author.id):
        return False
    if state.get("phase") == "prompt" and message.author.id == state.get("guesser_id"):
        valid_prompt, cleaned_prompt = validate_pattern_prompt(message.content)
        if not valid_prompt:
            if ge.can_emit_notice(game, "last_pattern_prompt_notice_at", interval=4.0):
                with contextlib.suppress(discord.HTTPException):
                    await message.channel.send(
                        "Pattern Hunt: ask the named pattern holder one real question or theme.",
                        delete_after=4.0,
                    )
            return True
        state["current_prompt"] = cleaned_prompt
        await _start_pattern_answer_locked(guild_id, game)
        return True
    if state.get("phase") != "answer" or message.author.id != current_pattern_hunt_coder_id(game):
        return False
    if message_matches_rule(state["rule_atoms"], message.content):
        state["accepted_answers"].append(
            {
                "coder": ge.display_name_of(message.author),
                "answer": message.content,
                "prompt": state.get("current_prompt") or "",
            }
        )
        await _advance_pattern_turn_locked(guild_id, game, count_clue=True)
        return True
    if not state.get("retry_used"):
        state["retry_used"] = True
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Fresh Answer",
                f"{message.author.mention}, that answer does not fit. Send one fresh answer without explaining why.",
                tone="warning",
                footer="Babblebox Pattern Hunt",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        return True
    await _handle_pattern_penalty_locked(
        guild_id,
        game,
        reason=f"{message.author.mention} sent two answers that did not fit in the same turn.",
        reset_phase="answer",
    )
    return True


async def submit_pattern_guess_locked(
    guild_id: int,
    game: dict[str, Any],
    actor: discord.abc.User,
    guessed_atoms: list[RuleAtom],
) -> tuple[bool, str]:
    state = ensure_pattern_hunt_state(game)
    if actor.id != state.get("guesser_id"):
        return False, "Only the current guesser can submit a Pattern Hunt guess."
    valid_bundle, validation_message = validate_pattern_guess_atoms(guessed_atoms)
    if not valid_bundle:
        return False, validation_message or "That Pattern Hunt guess needs a cleaner rule bundle."
    if _rule_signature(guessed_atoms) == _rule_signature(state.get("rule_atoms", [])):
        await _finish_pattern_hunt_locked(guild_id, game, guesser_won=True, reason=f"{actor.mention} read the room and cracked the rule.")
        return True, "You cracked it."
    state["guesses_used"] = int(state.get("guesses_used", 0) or 0) + 1
    remaining = int(state.get("guess_limit", 3)) - int(state.get("guesses_used", 0))
    if remaining <= 0:
        await _finish_pattern_hunt_locked(
            guild_id,
            game,
            guesser_won=False,
            reason=f"{actor.mention} ran out of guesses. The pattern holders held the pattern.",
        )
        return True, "Out of guesses."
    await _refresh_pattern_hunt_anchor(game)
    return False, f"Not the rule yet. You have **{remaining}** guess(es) left."


async def submit_pattern_theory_locked(
    guild_id: int,
    game: dict[str, Any],
    actor: discord.abc.User,
    theory: str,
) -> tuple[bool, str]:
    ok, atoms_or_message = parse_pattern_theory(theory)
    if not ok:
        return False, str(atoms_or_message)
    return await submit_pattern_guess_locked(guild_id, game, actor, atoms_or_message)


async def _start_pattern_answer_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    state["phase"] = "answer"
    state["retry_used"] = False
    timeout_seconds = _pattern_hunt_answer_timeout_seconds(state)
    state["deadline_at"] = ge.now_utc() + timedelta(seconds=timeout_seconds)
    token = ge.bump_token(game, "turn_token")
    await ge.cancel_task(game.get("turn_task"))
    await _refresh_pattern_hunt_anchor(game)
    game["turn_task"] = asyncio.create_task(
        _pattern_hunt_answer_timeout(guild_id, token, timeout_seconds),
        name=f"babblebox-pattern-answer-{guild_id}",
    )
    ge.reset_idle_timer(guild_id)


async def _begin_pattern_turn_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    state["phase"] = "prompt"
    state["current_prompt"] = None
    state["retry_used"] = False
    timeout_seconds = _pattern_hunt_prompt_timeout_seconds(state)
    state["deadline_at"] = ge.now_utc() + timedelta(seconds=timeout_seconds)
    token = ge.bump_token(game, "turn_token")
    await ge.cancel_task(game.get("turn_task"))
    await _refresh_pattern_hunt_anchor(game)
    game["turn_task"] = asyncio.create_task(
        _pattern_hunt_prompt_timeout(guild_id, token, timeout_seconds),
        name=f"babblebox-pattern-question-{guild_id}",
    )
    ge.reset_idle_timer(guild_id)


async def _advance_pattern_turn_locked(guild_id: int, game: dict[str, Any], *, count_clue: bool):
    state = ensure_pattern_hunt_state(game)
    if count_clue:
        state["clues_used"] = int(state.get("clues_used", 0) or 0) + 1
        state["turns_used"] = state["clues_used"]
        if int(state.get("clues_used", 0)) >= int(state.get("clue_limit", 0)):
            _finish_pattern_hunt_tutorial_cycle(state)
            await _finish_pattern_hunt_locked(guild_id, game, guesser_won=False, reason="The pattern holders survived the full question budget.")
            return
    else:
        state["turns_used"] = int(state.get("clues_used", 0) or 0)
    _finish_pattern_hunt_tutorial_cycle(state)
    coder_order = state.get("coder_order", [])
    if coder_order:
        state["current_coder_index"] = (int(state.get("current_coder_index", 0) or 0) + 1) % len(coder_order)
    await _begin_pattern_turn_locked(guild_id, game)


async def _handle_pattern_penalty_locked(guild_id: int, game: dict[str, Any], *, reason: str, reset_phase: str):
    state = ensure_pattern_hunt_state(game)
    if not state.get("tutorial_grace_used"):
        state["tutorial_grace_used"] = True
        if reset_phase == "answer":
            body = "Opening round stays forgiving. Same holder, fresh answer timer."
        else:
            body = "Opening round stays forgiving. Same guesser, fresh question timer."
        await game["channel"].send(
            embed=ge.make_status_embed("🕊️ Opening Grace", body, tone="info", footer="Babblebox Pattern Hunt"),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        if reset_phase == "answer":
            await _start_pattern_answer_locked(guild_id, game)
        else:
            await _begin_pattern_turn_locked(guild_id, game)
        return
    await _apply_pattern_strike_locked(guild_id, game, reason=reason)


async def _apply_pattern_strike_locked(guild_id: int, game: dict[str, Any], *, reason: str):
    state = ensure_pattern_hunt_state(game)
    state["strikes"] = int(state.get("strikes", 0) or 0) + 1
    if not state.get("hint_revealed") and int(state.get("strikes", 0)) >= 1:
        first_family = state["rule_atoms"][0].family if state.get("rule_atoms") else "unknown"
        state["hint_revealed"] = True
        state["hint_text"] = (
            f"The rule has **{len(state.get('rule_atoms', []))}** part(s), and one family is **{rule_family_label(first_family)}**."
        )
    misses_left = max(0, int(state.get("strike_limit", 3)) - int(state.get("strikes", 0)))
    body = f"{reason}\nMisses left: **{misses_left}**."
    if int(state.get("strikes", 0)) == 1 and state.get("hint_text"):
        body += f"\nHint: {state['hint_text']}"
    body += "\nThe table moves to the next question."
    await game["channel"].send(
        embed=ge.make_status_embed(
            "⚠️ Missed Beat",
            body,
            tone="warning",
            footer="Babblebox Pattern Hunt",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    if int(state.get("strikes", 0)) >= int(state.get("strike_limit", 3)):
        _finish_pattern_hunt_tutorial_cycle(state)
        await _finish_pattern_hunt_locked(guild_id, game, guesser_won=True, reason="The pattern holders ran out of misses, so the guesser takes it.")
        return
    await _advance_pattern_turn_locked(guild_id, game, count_clue=False)


async def _finish_pattern_hunt_locked(guild_id: int, game: dict[str, Any], *, guesser_won: bool, reason: str):
    state = ensure_pattern_hunt_state(game)
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    if guesser_won and guesser is not None:
        stats = ge.get_player_stats(guesser)
        stats["wins"] += 1
        stats["pattern_hunt_wins"] = int(stats.get("pattern_hunt_wins", 0) or 0) + 1
        ge.schedule_profile_update("record_pattern_hunt_win", guesser.id)
    await ge.cancel_task(game.get("turn_task"))
    embed = discord.Embed(
        title="🎭 Pattern Hunt Reveal",
        description=reason,
        color=discord.Color.gold() if guesser_won else discord.Color.green(),
    )
    embed.add_field(
        name="Outcome",
        value=(
            f"{ge.display_name_of(guesser)} cracked the pattern."
            if guesser_won and guesser is not None
            else "The pattern holders protected the pattern."
        ),
        inline=False,
    )
    embed.add_field(name="Secret Rule", value=render_rule(state.get("rule_atoms", [])), inline=False)
    if state.get("accepted_answers"):
        lines = _pattern_hunt_clue_recap_lines(
            state["accepted_answers"],
            limit=5,
            prompt_limit=30,
            clue_limit=54,
        )
        embed.add_field(name="Recent Q&A", value="\n".join(lines), inline=False)
    await game["channel"].send(embed=ge.style_embed(embed, footer="Babblebox Pattern Hunt | Hidden rule revealed"))
    await ge.cleanup_game(guild_id)


async def _pattern_hunt_prompt_timeout(guild_id: int, token: int, timeout_seconds: int):
    await asyncio.sleep(timeout_seconds)
    game = ge.games.get(guild_id)
    if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
        return
    async with game["lock"]:
        game = ge.games.get(guild_id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            return
        if game.get("turn_token") != token:
            return
        await _handle_pattern_penalty_locked(
            guild_id,
            game,
            reason="The guesser ran out of time to ask the named holder a question.",
            reset_phase="prompt",
        )


async def _pattern_hunt_answer_timeout(guild_id: int, token: int, timeout_seconds: int):
    await asyncio.sleep(timeout_seconds)
    game = ge.games.get(guild_id)
    if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
        return
    async with game["lock"]:
        game = ge.games.get(guild_id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            return
        if game.get("turn_token") != token:
            return
        coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
        await _handle_pattern_penalty_locked(
            guild_id,
            game,
            reason=f"{coder.mention if coder else 'The current holder'} ran out of time to answer the question.",
            reset_phase="answer",
        )
