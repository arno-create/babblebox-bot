from __future__ import annotations

import hashlib
import random
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from babblebox.question_drops_packs import EXPANDED_STATIC_SEEDS


QUESTION_DROP_CATEGORIES = (
    "science",
    "history",
    "geography",
    "language",
    "logic",
    "math",
    "culture",
)

QUESTION_DROP_TONES = ("clean", "playful", "roast-light")
QUESTION_DROP_ACTIVITY_GATES = ("off", "light")
QUESTION_DROP_DIFFICULTY_PROFILES = ("standard", "smart", "hard")
QUESTION_DROP_CATEGORY_LABELS = {
    "science": "Science",
    "history": "History",
    "geography": "Geography",
    "language": "Language",
    "logic": "Logic",
    "math": "Math",
    "culture": "Culture",
}
QUESTION_DROP_DIFFICULTY_LABELS = {1: "Easy", 2: "Medium", 3: "Hard"}
QUESTION_DROP_DIFFICULTY_PROFILE_LABELS = {
    "standard": "Welcoming mix, occasional spikes",
    "smart": "More medium and hard, less farmable",
    "hard": "Noticeably tougher lane",
}
QUESTION_DROP_ANSWER_ATTEMPT_LIMITS = {
    "multiple_choice": 1,
    "boolean": 1,
    "text": 3,
    "numeric": 3,
    "ordered_tokens": 1,
}

TRUE_ALIASES = {"true", "t", "yes", "y", "correct"}
FALSE_ALIASES = {"false", "f", "no", "n", "incorrect"}

_PUNCT_RE = re.compile(r"[^\w\s#&+\-]")
_SPACE_RE = re.compile(r"\s+")
_NUMERIC_TOKEN_RE = re.compile(r"(?<![\w/])[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?(?![\w/])")
_EXACT_NUMERIC_RE = re.compile(r"^[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?$")
_CHOICE_LETTER_RE = re.compile(r"^\s*(?:option|answer)?\s*\(?([a-z])\)?(?:[\)\].:\-])?(?:\s+(.*))?\s*$", re.IGNORECASE)
_ANSWER_LEAD_RE = re.compile(
    r"^\s*(?:(?:the\s+)?answer(?:\s+is)?|guess(?:\s+is)?|my guess(?:\s+is)?|i pick|i choose|i(?:'ll| will) go with|it(?:'s| is))\s*:?\s+(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)
_HEDGED_ANSWER_LEAD_RE = re.compile(
    r"^\s*(?:(?:i\s+think(?:\s+that)?|i\s+guess|maybe|perhaps|is\s+it|could\s+it\s+be)(?:\s+it(?:'s| is))?)\s*:?\s+(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://|\bdiscord\.gg/\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"<(?:@!?\d+|@&\d+|#\d+)>")
_NUMBER_WORD_RE = re.compile(r"[a-z]+", re.IGNORECASE)
_ORDERED_ARROW_SPLIT_RE = re.compile(r"\s*(?:->|=>|→)\s*")
_ORDERED_ITEM_PREFIX_RE = re.compile(r"^\s*\(?\d{1,2}\)?[.):-]?\s*")
_ORDERED_INLINE_NUMBERED_ITEM_RE = re.compile(r"(?:^|\s)\(?\d{1,2}\)?[.)]\s*")
_DIVISIBILITY_PROMPT_RE = re.compile(r"^Which number is divisible by (\d+)\? A\) (\d+) B\) (\d+) C\) (\d+)$")
_AVERAGE_PROMPT_RE = re.compile(r"^Find the average: ([\d,\s]+)$")
_MEDIAN_PROMPT_RE = re.compile(r"^Find the median: ([\d,\s]+)$")
_PERCENT_CHANGE_PROMPT_RE = re.compile(r"^A \$(\d+) item is discounted by (\d+)%\. What is the sale price\?$")
_ANAGRAM_PROMPT_RE = re.compile(r"^Unscramble the clue-backed word\. Clue: (.+?) Letters: \*\*([A-Z]+)\*\*$")
_BLAND_MATH_PROMPT_RE = re.compile(
    r"^(?:What is \d+ [+\-*] \d+\?|What number makes this true\? \? [+\-*] \d+ = \d+|Start with \d+\. .+ What do you get\?)$"
)
_BLAND_LOGIC_PROMPT_RE = re.compile(
    r"^(?:True or false: [A-Z][a-z]+ finished before [A-Z][a-z]+, and [A-Z][a-z]+ finished before [A-Z][a-z]+\. Therefore .+|Which direction comes next\? A\) .+ Sequence: .+)$"
)
QUESTION_DROP_VALIDATION_VARIANTS_PER_GENERATOR = 32
_SMART_PUNCT_TRANSLATION = str.maketrans(
    {
        "\u2018": "'",
        "\u2019": "'",
        "\u201a": "'",
        "\u201b": "'",
        "\u2032": "'",
        "\u2035": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u201e": '"',
        "\u201f": '"',
        "\xab": '"',
        "\xbb": '"',
        "\u02bc": "'",
    }
)
_CHATTER_LEAD_TOKENS = {
    "and",
    "because",
    "bro",
    "bruh",
    "but",
    "damn",
    "hey",
    "how",
    "huh",
    "i",
    "if",
    "it",
    "lol",
    "lmao",
    "maybe",
    "nah",
    "no",
    "nope",
    "ok",
    "okay",
    "oh",
    "omg",
    "perhaps",
    "same",
    "seriously",
    "that",
    "this",
    "true",
    "wait",
    "what",
    "when",
    "where",
    "why",
    "wild",
    "wow",
    "yeah",
    "yes",
    "yep",
    "you",
}
_CHATTER_TOKENS = {
    "bro",
    "bruh",
    "crazy",
    "fr",
    "huh",
    "lol",
    "lmao",
    "later",
    "no",
    "oh",
    "omg",
    "perhaps",
    "probably",
    "real",
    "really",
    "same",
    "seriously",
    "sure",
    "thanks",
    "thank",
    "true",
    "wait",
    "way",
    "wild",
    "wow",
    "yes",
}
_NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
}
_TENS_WORDS = {
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}
_SIGN_WORDS = {"minus", "negative"}
_NUMBER_WORD_VOCAB = set(_NUMBER_WORDS) | set(_TENS_WORDS) | {"hundred"} | _SIGN_WORDS
_TEXT_MATCH_OPTIONAL_TOKENS = {"a", "an", "the"}


@dataclass(frozen=True)
class QuestionDropVariant:
    concept_id: str
    category: str
    difficulty: int
    source_type: str
    generator_type: str
    prompt: str
    answer_spec: dict[str, Any]
    variant_hash: str
    family_id: str = ""
    tags: tuple[str, ...] = ()
    attribution: str | None = None


def normalize_answer_text(raw: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(raw or "")).casefold().strip()
    text = text.translate(_SMART_PUNCT_TRANSLATION)
    text = text.replace("ƒ?t", "'")
    text = text.replace("ƒ?o", '"').replace("ƒ??", '"')
    text = text.replace("'", "")
    text = text.replace('"', " ")
    text = _PUNCT_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text)
    return text.strip()


def _normalize_token_sequence(raw: str | None) -> tuple[str, ...]:
    cleaned = normalize_answer_text(raw)
    if not cleaned:
        return ()
    return tuple(token for token in cleaned.split(" ") if token)


def extract_single_number(raw: str | None) -> float | None:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return None
    matches = list(_NUMERIC_TOKEN_RE.finditer(cleaned))
    if len(matches) != 1:
        return None
    try:
        return float(matches[0].group(0).replace(",", ""))
    except ValueError:
        return None


def _decimal_from_numeric_value(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        decimal_value = Decimal(str(value))
    except InvalidOperation:
        return None
    if not decimal_value.is_finite():
        return None
    return decimal_value


def _format_decimal_value(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(int(value))
    text = format(value.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _json_numeric_value(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(_format_decimal_value(value))


def _numeric_words_allowed(answer_spec: dict[str, Any]) -> bool:
    expected = _decimal_from_numeric_value(answer_spec.get("value"))
    return expected is not None and expected == expected.to_integral_value()


def _strip_trailing_terminal_punctuation(raw: str | None) -> str:
    return str(raw or "").strip().rstrip(".!?").strip()


def _normalize_ordered_expected_items(answer_spec: dict[str, Any]) -> tuple[str, ...]:
    normalized_items = []
    for token in answer_spec.get("tokens", []):
        if not isinstance(token, str):
            continue
        normalized = normalize_answer_text(token)
        if normalized:
            normalized_items.append(normalized)
    return tuple(normalized_items)


def _normalize_ordered_item_sequence(parts: list[str]) -> tuple[str, ...]:
    normalized_items = []
    for part in parts:
        normalized = normalize_answer_text(_ORDERED_ITEM_PREFIX_RE.sub("", str(part or "").strip(), count=1))
        if not normalized:
            return ()
        normalized_items.append(normalized)
    return tuple(normalized_items)


def _split_numbered_ordered_items(raw: str) -> list[str] | None:
    matches = list(_ORDERED_INLINE_NUMBERED_ITEM_RE.finditer(raw))
    if len(matches) < 2:
        return None
    items: list[str] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        item = raw[start:end].strip().rstrip(",;").strip()
        if not item:
            return None
        items.append(item)
    return items


def _parse_ordered_answer_items(raw: str | None, *, expected_items: tuple[str, ...]) -> tuple[str, ...]:
    content = _strip_trailing_terminal_punctuation(raw)
    if not content:
        return ()
    numbered_items = _split_numbered_ordered_items(content)
    if numbered_items is not None:
        return _normalize_ordered_item_sequence(numbered_items)
    if "\n" in content:
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        if len(lines) >= 2:
            return _normalize_ordered_item_sequence(lines)
    if _ORDERED_ARROW_SPLIT_RE.search(content):
        return _normalize_ordered_item_sequence([part for part in _ORDERED_ARROW_SPLIT_RE.split(content) if part.strip()])
    for delimiter in (";", ","):
        if delimiter in content:
            return _normalize_ordered_item_sequence([part for part in content.split(delimiter) if part.strip()])
    if expected_items and all(" " not in item for item in expected_items):
        tokens = _normalize_token_sequence(content)
        if len(tokens) >= 2:
            return tokens
    return ()


def _parse_number_words_exact(raw: str | None) -> int | None:
    content = _strip_trailing_terminal_punctuation(raw)
    matches = list(_NUMBER_WORD_RE.finditer(content))
    if not matches:
        return None
    if content[: matches[0].start()].strip() or content[matches[-1].end() :].strip():
        return None
    previous_end = matches[0].start()
    words: list[str] = []
    for match in matches:
        word = match.group(0).casefold()
        if word not in _NUMBER_WORD_VOCAB:
            return None
        separator = content[previous_end : match.start()]
        if separator and not re.fullmatch(r"[-\s]+", separator):
            return None
        words.append(word)
        previous_end = match.end()
    return _parse_number_word_tokens(words)


def _parse_number_word_tokens(words: list[str]) -> int | None:
    if not words:
        return None
    sign = 1
    tokens = list(words)
    if tokens[0] in _SIGN_WORDS:
        sign = -1
        tokens = tokens[1:]
    if not tokens:
        return None
    if tokens == ["hundred"]:
        return 100 * sign

    def parse_sub_hundred(rest: list[str]) -> int | None:
        if not rest:
            return 0
        if len(rest) == 1:
            if rest[0] in _NUMBER_WORDS:
                return _NUMBER_WORDS[rest[0]]
            if rest[0] in _TENS_WORDS:
                return _TENS_WORDS[rest[0]]
            return None
        if len(rest) == 2 and rest[0] in _TENS_WORDS and rest[1] in _NUMBER_WORDS and 1 <= _NUMBER_WORDS[rest[1]] <= 9:
            return _TENS_WORDS[rest[0]] + _NUMBER_WORDS[rest[1]]
        return None

    if "hundred" not in tokens:
        value = parse_sub_hundred(tokens)
        return (sign * value) if value is not None else None
    if tokens.count("hundred") != 1:
        return None
    hundred_index = tokens.index("hundred")
    if hundred_index != 1:
        return None
    head = tokens[0]
    if head not in _NUMBER_WORDS or not 1 <= _NUMBER_WORDS[head] <= 9:
        return None
    tail_value = parse_sub_hundred(tokens[2:])
    if tail_value is None:
        return None
    return sign * ((_NUMBER_WORDS[head] * 100) + tail_value)


def _parse_clean_numeric_payload(raw: str | None, *, allow_number_words: bool = True) -> Decimal | None:
    content = _strip_trailing_terminal_punctuation(raw)
    if not content:
        return None
    if _EXACT_NUMERIC_RE.fullmatch(content):
        try:
            return Decimal(content.replace(",", ""))
        except InvalidOperation:
            return None
    if allow_number_words:
        word_value = _parse_number_words_exact(content)
        if word_value is not None:
            return Decimal(word_value)
    return None


def _correct_choice_letter(answer_spec: dict[str, Any]) -> str | None:
    choices = [normalize_answer_text(choice) for choice in answer_spec.get("choices", []) if isinstance(choice, str)]
    answer = normalize_answer_text(answer_spec.get("answer"))
    if not choices or not answer:
        return None
    try:
        index = choices.index(answer)
    except ValueError:
        return None
    if index >= 26:
        return None
    return chr(ord("a") + index)


def _parse_choice_letter(raw_answer: str | None, answer_spec: dict[str, Any]) -> str | None:
    match = _CHOICE_LETTER_RE.match(_strip_trailing_terminal_punctuation(raw_answer))
    if match is None:
        return None
    letter = str(match.group(1) or "").casefold()
    trailing = normalize_answer_text(match.group(2) or "")
    choices = [normalize_answer_text(choice) for choice in answer_spec.get("choices", []) if isinstance(choice, str)]
    if trailing and trailing not in choices:
        return None
    return letter or None


def _answer_payload_candidates(raw_answer: str | None) -> list[str]:
    content = str(raw_answer or "").strip()
    if not content:
        return []
    candidates = [content]
    index = 0
    while index < len(candidates) and len(candidates) < 6:
        candidate = candidates[index]
        for pattern in (_ANSWER_LEAD_RE, _HEDGED_ANSWER_LEAD_RE):
            lead_match = pattern.match(candidate)
            if lead_match is None:
                continue
            payload = str(lead_match.group("payload") or "").strip()
            if payload and payload not in candidates:
                candidates.append(payload)
        index += 1
    return candidates


def _contains_attempt_noise(raw_answer: str) -> bool:
    return _URL_RE.search(raw_answer) is not None or _MENTION_RE.search(raw_answer) is not None


def _looks_like_free_text_guess(raw_answer: str | None, *, max_tokens: int) -> bool:
    raw = _strip_trailing_terminal_punctuation(raw_answer)
    if not raw or "?" in raw or "\n" in raw:
        return False
    tokens = _normalize_token_sequence(raw)
    if not tokens or len(tokens) > max_tokens:
        return False
    if tokens[0] in _CHATTER_LEAD_TOKENS:
        return False
    if all(token in (_CHATTER_TOKENS | _CHATTER_LEAD_TOKENS | TRUE_ALIASES | FALSE_ALIASES) for token in tokens):
        return False
    if sum(1 for token in tokens if token in _CHATTER_TOKENS) >= 2:
        return False
    return True


def _starts_with_soft_hedge(raw_answer: str | None) -> bool:
    content = str(raw_answer or "").strip().casefold()
    return content.startswith("maybe ") or content.startswith("maybe:") or content.startswith("perhaps ")


def _normalized_text_match_tokens(raw: str | None) -> tuple[str, ...]:
    return tuple(token for token in _normalize_token_sequence(raw) if token not in _TEXT_MATCH_OPTIONAL_TOKENS)


def _adjacent_transposition(left: str, right: str) -> bool:
    if len(left) != len(right):
        return False
    diffs = [index for index, (a, b) in enumerate(zip(left, right)) if a != b]
    if len(diffs) != 2 or diffs[1] != diffs[0] + 1:
        return False
    first = diffs[0]
    second = diffs[1]
    return left[first] == right[second] and left[second] == right[first]


def _limited_edit_distance(left: str, right: str, *, max_distance: int) -> int:
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1
    if left == right:
        return 0
    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, start=1):
        current = [left_index]
        row_min = current[0]
        for right_index, right_char in enumerate(right, start=1):
            cost = 0 if left_char == right_char else 1
            current_value = min(
                previous[right_index] + 1,
                current[right_index - 1] + 1,
                previous[right_index - 1] + cost,
            )
            current.append(current_value)
            row_min = min(row_min, current_value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def _small_token_typo(expected: str, observed: str) -> bool:
    if expected == observed:
        return True
    if min(len(expected), len(observed)) < 4:
        return False
    return _adjacent_transposition(expected, observed) or _limited_edit_distance(expected, observed, max_distance=1) <= 1


def _fuzzy_text_match(expected: str, observed: str) -> bool:
    expected_tokens = _normalized_text_match_tokens(expected)
    observed_tokens = _normalized_text_match_tokens(observed)
    if not expected_tokens or not observed_tokens:
        return False
    if expected_tokens == observed_tokens:
        return True
    if len(expected_tokens) != len(observed_tokens):
        return False
    mismatches = 0
    for expected_token, observed_token in zip(expected_tokens, observed_tokens):
        if expected_token == observed_token:
            continue
        if not _small_token_typo(expected_token, observed_token):
            return False
        mismatches += 1
        if mismatches > 1:
            return False
    return mismatches > 0


def validate_answer_spec(spec: dict[str, Any]) -> tuple[bool, str | None]:
    if not isinstance(spec, dict):
        return False, "Answer spec must be a dictionary."
    answer_type = spec.get("type")
    if answer_type == "text":
        accepted = spec.get("accepted")
        if not isinstance(accepted, list) or not any(isinstance(item, str) and normalize_answer_text(item) for item in accepted):
            return False, "Text answers need at least one accepted alias."
        return True, None
    if answer_type == "numeric":
        if _decimal_from_numeric_value(spec.get("value")) is None:
            return False, "Numeric answers need a numeric value."
        return True, None
    if answer_type == "boolean":
        if not isinstance(spec.get("value"), bool):
            return False, "Boolean answers need a true/false value."
        return True, None
    if answer_type == "multiple_choice":
        choices = spec.get("choices")
        answer = spec.get("answer")
        normalized_choices = [normalize_answer_text(choice) for choice in choices] if isinstance(choices, list) else []
        normalized_answer = normalize_answer_text(answer) if isinstance(answer, str) else ""
        if len(normalized_choices) < 2 or not all(normalized_choices):
            return False, "Multiple-choice answers need at least two non-empty options."
        if len(set(normalized_choices)) != len(normalized_choices):
            return False, "Multiple-choice answers need distinct options."
        if not normalized_answer:
            return False, "Multiple-choice answers need a correct option."
        if normalized_answer not in normalized_choices:
            return False, "Multiple-choice answers need the correct option to match one of the choices."
        return True, None
    if answer_type == "ordered_tokens":
        tokens = spec.get("tokens")
        if not isinstance(tokens, list) or len(tokens) < 2:
            return False, "Ordered token answers need at least two tokens."
        if not all(isinstance(token, str) and normalize_answer_text(token) for token in tokens):
            return False, "Ordered token answers need non-empty string tokens."
        return True, None
    return False, f"Unsupported answer type '{answer_type}'."


def judge_answer(answer_spec: dict[str, Any], raw_answer: str | None) -> bool:
    candidates = _answer_payload_candidates(raw_answer)
    answer_type = answer_spec.get("type")
    if answer_type == "text":
        accepted = [item for item in answer_spec.get("accepted", []) if isinstance(item, str)]
        normalized_accepted = {normalize_answer_text(item) for item in accepted}
        for candidate in candidates:
            normalized = normalize_answer_text(candidate)
            if normalized and normalized in normalized_accepted:
                return True
            if any(_fuzzy_text_match(alias, candidate) for alias in accepted):
                return True
        return False
    if answer_type == "numeric":
        expected = _decimal_from_numeric_value(answer_spec.get("value"))
        if expected is None:
            return False
        allow_number_words = expected == expected.to_integral_value()
        return any(
            candidate is not None and candidate == expected
            for candidate in (_parse_clean_numeric_payload(item, allow_number_words=allow_number_words) for item in candidates)
        )
    if answer_type == "boolean":
        if answer_spec.get("value") is True:
            return any(normalize_answer_text(candidate) in TRUE_ALIASES for candidate in candidates if normalize_answer_text(candidate))
        return any(normalize_answer_text(candidate) in FALSE_ALIASES for candidate in candidates if normalize_answer_text(candidate))
    if answer_type == "multiple_choice":
        answer = normalize_answer_text(answer_spec.get("answer"))
        correct_letter = _correct_choice_letter(answer_spec)
        if correct_letter is None:
            return False
        for candidate in candidates:
            normalized = normalize_answer_text(candidate)
            if normalized and normalized == answer:
                return True
            if _parse_choice_letter(candidate, answer_spec) == correct_letter:
                return True
        return False
    if answer_type == "ordered_tokens":
        expected_items = _normalize_ordered_expected_items(answer_spec)
        return any(
            (candidate_items := _parse_ordered_answer_items(candidate, expected_items=expected_items))
            and candidate_items == expected_items
            for candidate in candidates
        )
    return False


def is_answer_attempt(answer_spec: dict[str, Any], raw_answer: str | None, *, direct_reply: bool = False) -> bool:
    content = str(raw_answer or "").strip()
    if not content or len(content) > 120 or _contains_attempt_noise(content):
        return False
    candidates = _answer_payload_candidates(content)
    if not candidates:
        return False
    answer_type = answer_spec.get("type")
    if answer_type == "multiple_choice":
        choices = {normalize_answer_text(choice) for choice in answer_spec.get("choices", []) if isinstance(choice, str)}
        for candidate in candidates:
            normalized = normalize_answer_text(candidate)
            if normalized and normalized in choices:
                return True
            if _parse_choice_letter(candidate, answer_spec) is not None:
                return True
        return False
    if answer_type == "numeric":
        return any(
            _parse_clean_numeric_payload(candidate, allow_number_words=_numeric_words_allowed(answer_spec)) is not None
            for candidate in candidates
        )
    if answer_type == "boolean":
        normalized_values = {normalize_answer_text(candidate) for candidate in candidates if normalize_answer_text(candidate)}
        return bool(normalized_values.intersection(TRUE_ALIASES | FALSE_ALIASES))
    if answer_type == "ordered_tokens":
        expected_items = _normalize_ordered_expected_items(answer_spec)
        if not expected_items:
            return False
        expected_counter = Counter(expected_items)
        for candidate in candidates:
            candidate_items = _parse_ordered_answer_items(candidate, expected_items=expected_items)
            if candidate_items and len(candidate_items) == len(expected_items) and Counter(candidate_items) == expected_counter:
                return True
        return False
    if answer_type == "text":
        accepted = {normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)}
        max_alias_tokens = max((len(_normalize_token_sequence(item)) for item in accepted), default=1)
        standalone_max_tokens = max(4, max_alias_tokens + 2)
        for candidate in candidates:
            normalized = normalize_answer_text(candidate)
            if normalized and normalized in accepted:
                return True
        if (
            len(candidates) > 1
            and _looks_like_free_text_guess(candidates[-1], max_tokens=standalone_max_tokens + 1)
            and (direct_reply or not _starts_with_soft_hedge(content))
        ):
            return True
        if direct_reply and _looks_like_free_text_guess(candidates[0], max_tokens=standalone_max_tokens + 1):
            return True
        if _looks_like_free_text_guess(candidates[0], max_tokens=standalone_max_tokens):
            return True
        return False
    return False


def render_answer_summary(answer_spec: dict[str, Any]) -> str:
    answer_type = answer_spec.get("type")
    if answer_type == "text":
        accepted = [normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)]
        return accepted[0] if accepted else "unknown"
    if answer_type == "numeric":
        value = _decimal_from_numeric_value(answer_spec.get("value"))
        return _format_decimal_value(value) if value is not None else "unknown"
    if answer_type == "boolean":
        return "true" if answer_spec.get("value") else "false"
    if answer_type == "multiple_choice":
        letter = _correct_choice_letter(answer_spec)
        answer = str(answer_spec.get("answer") or "unknown")
        return f"{letter.upper()}) {answer}" if letter is not None else answer
    if answer_type == "ordered_tokens":
        return ", ".join(str(token) for token in answer_spec.get("tokens", [])) or "unknown"
    return "unknown"


def answer_attempt_limit(answer_spec: dict[str, Any]) -> int:
    answer_type = str(answer_spec.get("type") or "").strip().casefold()
    return int(QUESTION_DROP_ANSWER_ATTEMPT_LIMITS.get(answer_type, 3))


def _attempt_limit_instruction_prefix(answer_spec: dict[str, Any]) -> str:
    limit = answer_attempt_limit(answer_spec)
    label = "1 attempt" if limit == 1 else f"{limit} attempts"
    return f"You get **{label}**."


def render_answer_instruction(answer_spec: dict[str, Any]) -> str:
    answer_type = answer_spec.get("type")
    if answer_type == "multiple_choice":
        return (
            f"{_attempt_limit_instruction_prefix(answer_spec)} "
            "Reply is optional. Send a clean same-channel answer with the option text, or use the letter: `C` or `option c`."
        )
    if answer_type == "numeric":
        if _numeric_words_allowed(answer_spec):
            return (
                f"{_attempt_limit_instruction_prefix(answer_spec)} "
                "Reply is optional. Send a clean same-channel answer with just the number. Digits work, and simple number words count for whole-number answers."
            )
        return (
            f"{_attempt_limit_instruction_prefix(answer_spec)} "
            "Reply is optional. Send a clean same-channel answer with just the number. Use digits for decimals, like `14.4`."
        )
    if answer_type == "boolean":
        return (
            f"{_attempt_limit_instruction_prefix(answer_spec)} "
            "Reply is optional. Send a clean same-channel answer like `true` / `false` or `yes` / `no`."
        )
    if answer_type == "ordered_tokens":
        return (
            f"{_attempt_limit_instruction_prefix(answer_spec)} "
            "Reply is optional. Send a clean same-channel answer with the full sequence in order using commas, like `red, blue, green`."
        )
    return f"{_attempt_limit_instruction_prefix(answer_spec)} Reply is optional. Send a short clean same-channel guess."


def answer_points_for_difficulty(difficulty: int) -> int:
    return {1: 10, 2: 15, 3: 20}.get(int(difficulty), 10)


def content_seed_signature(seed: dict[str, Any]) -> str:
    parts = [
        str(seed.get("concept_id") or ""),
        str(seed.get("family_id") or ""),
        str(seed.get("category") or ""),
        str(seed.get("difficulty") or ""),
        str(seed.get("source_type") or ""),
        str(seed.get("generator_type") or ""),
    ]
    return "|".join(parts)


def build_variant_hash(*parts: str) -> str:
    digest = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def _make_rng(seed_material: str) -> random.Random:
    digest = hashlib.sha256(seed_material.encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))


def _text_spec(*accepted: str) -> dict[str, Any]:
    return {"type": "text", "accepted": list(accepted)}


def _numeric_spec(value: int | float) -> dict[str, Any]:
    return {"type": "numeric", "value": value}


def _boolean_spec(value: bool) -> dict[str, Any]:
    return {"type": "boolean", "value": value}


def _multiple_choice_spec(*choices: str, answer: str) -> dict[str, Any]:
    return {"type": "multiple_choice", "choices": list(choices), "answer": answer}


def _ordered_spec(*tokens: str) -> dict[str, Any]:
    return {"type": "ordered_tokens", "tokens": list(tokens)}


def _variant(prompt: str, answer_spec: dict[str, Any]) -> dict[str, Any]:
    return {"prompt": prompt, "answer_spec": answer_spec}


def _static_seed(
    concept_id: str,
    category: str,
    difficulty: int,
    family_id: str,
    *variants: dict[str, Any],
    tags: tuple[str, ...] = (),
    attribution: str | None = None,
) -> dict[str, Any]:
    return {
        "concept_id": concept_id,
        "family_id": family_id,
        "category": category,
        "difficulty": difficulty,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": tuple(variants),
        "tags": tuple(tags),
        "attribution": attribution,
    }


def _generated_seed(
    concept_id: str,
    category: str,
    difficulty: int,
    generator_type: str,
    family_id: str,
    *,
    tags: tuple[str, ...] = (),
) -> dict[str, Any]:
    return {
        "concept_id": concept_id,
        "family_id": family_id,
        "category": category,
        "difficulty": difficulty,
        "source_type": "generated",
        "generator_type": generator_type,
        "variants": (),
        "tags": tuple(tags),
    }


def _seed_family_id(seed: dict[str, Any]) -> str:
    family_id = str(seed.get("family_id") or "").strip()
    if family_id:
        return family_id
    generator_type = str(seed.get("generator_type") or "").strip()
    if generator_type:
        return generator_type
    return str(seed.get("concept_id") or "question-drop")


def _static_variant(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    variants = list(seed["variants"])
    rotation_rng = _make_rng(f"{seed_material}:{seed['concept_id']}:rotation")
    rotation = rotation_rng.randrange(len(variants)) if variants else 0
    payload_index = (rotation + variant_index) % len(variants)
    payload = variants[payload_index]
    family_id = _seed_family_id(seed)
    variant_hash = build_variant_hash(seed["concept_id"], family_id, payload["prompt"], str(payload_index))
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=payload["prompt"],
        answer_spec=payload["answer_spec"],
        variant_hash=variant_hash,
        tags=tuple(seed.get("tags", ())),
        attribution=seed.get("attribution"),
    )


def _build_numeric_variant(seed: dict[str, Any], *, prompt: str, answer: int | float) -> QuestionDropVariant:
    family_id = _seed_family_id(seed)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec=_numeric_spec(answer),
        variant_hash=build_variant_hash(seed["concept_id"], family_id, prompt, str(answer)),
        tags=tuple(seed.get("tags", ())),
    )


def _build_text_variant(seed: dict[str, Any], *, prompt: str, accepted: list[str]) -> QuestionDropVariant:
    family_id = _seed_family_id(seed)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "text", "accepted": accepted},
        variant_hash=build_variant_hash(seed["concept_id"], family_id, prompt, accepted[0]),
        tags=tuple(seed.get("tags", ())),
    )


def _build_boolean_variant(seed: dict[str, Any], *, prompt: str, value: bool) -> QuestionDropVariant:
    family_id = _seed_family_id(seed)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec=_boolean_spec(value),
        variant_hash=build_variant_hash(seed["concept_id"], family_id, prompt, str(value)),
        tags=tuple(seed.get("tags", ())),
    )


def _build_choice_variant(seed: dict[str, Any], *, prompt: str, choices: list[str], answer: str) -> QuestionDropVariant:
    family_id = _seed_family_id(seed)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "multiple_choice", "choices": choices, "answer": answer},
        variant_hash=build_variant_hash(seed["concept_id"], family_id, prompt, answer),
        tags=tuple(seed.get("tags", ())),
    )


def _build_ordered_variant(seed: dict[str, Any], *, prompt: str, tokens: list[str]) -> QuestionDropVariant:
    family_id = _seed_family_id(seed)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        family_id=family_id,
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "ordered_tokens", "tokens": tokens},
        variant_hash=build_variant_hash(seed["concept_id"], family_id, prompt, "|".join(tokens)),
        tags=tuple(seed.get("tags", ())),
    )


def _math_addition(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = (variant_index // 3) + (rotation % 5)
    kind = ("compensation", "friendly-tens", "quick-total")[variant_index % 3]
    if kind == "compensation":
        left = (39, 49, 59, 79, 89, 99, 199)[lane % 7]
        right = 13 + (lane * 3)
        prompt = (
            f"Use a quick compensation trick: {left} + {right} is the same as "
            f"{left + 1} + {right - 1}. What is the sum?"
        )
    elif kind == "friendly-tens":
        left = 21 + (lane * 4)
        right = (19, 29, 39)[lane % 3]
        prompt = f"Mental math: {left} + {right} can be seen as {left + 1} + {right - 1}. What total do you get?"
    else:
        left = 18 + (lane * 3)
        right = 16 + (lane * 2)
        prompt = f"A cafe sold {left} pastries before lunch and {right} after lunch. How many did it sell altogether?"
    return _build_numeric_variant(seed, prompt=prompt, answer=left + right)


def _math_multiplication(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = (variant_index // 3) + (rotation % 5)
    kind = ("double-and-half", "quarter-trick", "near-ten")[variant_index % 3]
    if kind == "double-and-half":
        left = (12, 14, 16, 18, 24)[lane % 5]
        right = 15 + (5 * lane)
        prompt = (
            f"Use double-and-half mental math: {left} x {right} has the same product as "
            f"{left // 2} x {right * 2}. What is it?"
        )
    elif kind == "quarter-trick":
        left = 25
        right = 12 + (4 * lane)
        prompt = f"Quarter trick: 25 x {right} is the same as 100 x {right // 4}. What is the product?"
    else:
        left = 9
        right = 12 + lane
        prompt = f"Near-ten mental math: 9 x {right} is one {right} less than 10 x {right}. What is it?"
    return _build_numeric_variant(seed, prompt=prompt, answer=left * right)


def _math_order_operations(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = variant_index + (rotation % 5)
    if variant_index % 2 == 0:
        left = 6 + lane
        middle = 2 + (lane % 5)
        right = 2 + (lane % 4)
        prompt = f"Use standard order of operations: {left} + {middle} * {right}"
        answer = left + (middle * right)
    else:
        left = 3 + (lane % 9)
        middle = 2 + (lane % 6)
        right = 2 + ((lane + 1) % 4)
        tail = 3 + (lane % 8)
        prompt = f"Use standard order of operations: ({left} + {middle}) * {right} - {tail}"
        answer = ((left + middle) * right) - tail
    return _build_numeric_variant(seed, prompt=prompt, answer=answer)


def _math_missing_value(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = (variant_index // 3) + (rotation % 5)
    kind = ("score-jump", "receipt", "equal-groups")[variant_index % 3]
    if kind == "score-jump":
        start = 18 + (lane * 3)
        missing = 7 + lane
        total = start + missing
        prompt = f"A team's score rose from {start} to {total}. By how many points did it increase?"
    elif kind == "receipt":
        subtotal = 22 + (lane * 4)
        fee = (4, 6, 8, 10, 12)[lane % 5]
        missing = subtotal
        total = subtotal + fee
        prompt = f"A receipt total is {total} after a {fee}-dollar fee was added. What was the subtotal?"
    else:
        per_group = 4 + (lane % 11)
        groups = 3 + (lane % 6)
        missing = per_group
        total = per_group * groups
        prompt = f"{groups} equal boxes hold {total} oranges altogether. How many oranges are in each box?"
    return _build_numeric_variant(seed, prompt=prompt, answer=missing)


def _math_compare_expressions(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    if rng.choice((True, False)):
        left_a = rng.randint(3, 12)
        left_b = rng.randint(2, 8)
        left_c = rng.randint(1, 9)
        right_a = rng.randint(4, 13)
        right_b = rng.randint(2, 7)
        right_c = rng.randint(1, 9)
        left = left_a * left_b + left_c
        right = right_a * right_b + right_c
        left_prompt = f"{left_a} * {left_b} + {left_c}"
        right_prompt = f"{right_a} * {right_b} + {right_c}"
    else:
        left_a = rng.randint(3, 11)
        left_b = rng.randint(2, 7)
        left_mul = rng.randint(2, 4)
        right_a = rng.randint(4, 10)
        right_b = rng.randint(2, 8)
        right_mul = rng.randint(2, 4)
        left = (left_a + left_b) * left_mul
        right = (right_a + right_b) * right_mul
        left_prompt = f"({left_a} + {left_b}) * {left_mul}"
        right_prompt = f"({right_a} + {right_b}) * {right_mul}"
    relation = "equal"
    if left > right:
        relation = "left"
    elif right > left:
        relation = "right"
    prompt = f"Which is larger? A) {left_prompt} B) {right_prompt} C) equal"
    return _build_choice_variant(seed, prompt=prompt, choices=["left", "right", "equal"], answer=relation)


def _math_multi_step(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = (variant_index // 3) + (rotation % 5)
    kind = ("halve-add-triple", "coupon-and-pair", "double-then-fee")[variant_index % 3]
    if kind == "halve-add-triple":
        start = (24, 30, 36, 42, 48, 54)[lane % 6]
        add = 3 + (lane % 7)
        answer = ((start // 2) + add) * 3
        prompt = f"Mental-math path: halve {start}, add {add}, then triple the result. What do you get?"
    elif kind == "coupon-and-pair":
        price = 18 + (lane * 2)
        discount = (3, 4, 5, 6, 8)[lane % 5]
        fee = (2, 3, 4)[lane % 3]
        answer = ((price - discount) + fee) * 2
        prompt = (
            f"Two friends each buy a ticket priced at {price}. A coupon removes {discount} from each ticket, "
            f"then a {fee}-dollar fee is added to each. What is the total bill for both tickets?"
        )
    else:
        start = 14 + lane
        doubled = start * 2
        add = 5 + (lane % 9)
        answer = doubled + add
        prompt = f"A score starts at {start}, gets doubled, then gains {add} bonus points. What is the final score?"
    return _build_numeric_variant(seed, prompt=prompt, answer=answer)


def _math_divisibility(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    divisor = rng.choice((3, 4, 5, 6, 8, 9))
    correct = divisor * rng.randint(6, 14)

    def pick_wrong(existing: set[int]) -> int:
        offsets = [offset for offset in range(-11, 12) if offset != 0 and (correct + offset) > 0 and (correct + offset) % divisor != 0]
        while True:
            candidate = correct + rng.choice(offsets)
            if candidate not in existing:
                return candidate

    existing = {correct}
    wrong_a = pick_wrong(existing)
    existing.add(wrong_a)
    wrong_b = pick_wrong(existing)
    options = [str(correct), str(wrong_a), str(wrong_b)]
    rng.shuffle(options)
    prompt = f"Which number is divisible by {divisor}? A) {options[0]} B) {options[1]} C) {options[2]}"
    return _build_choice_variant(seed, prompt=prompt, choices=options, answer=str(correct))


def _math_remainder(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    divisor = rng.randint(4, 9)
    quotient = rng.randint(5, 13)
    remainder = rng.randint(1, divisor - 1)
    dividend = (divisor * quotient) + remainder
    prompt = f"What is the remainder when {dividend} is divided by {divisor}?"
    return _build_numeric_variant(seed, prompt=prompt, answer=remainder)


def _math_percent_change(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    base_prices = (20, 24, 30, 32, 36, 40, 48, 60, 72)
    percents = (10, 15, 20, 25, 30, 40)
    base_price = base_prices[(rotation + variant_index) % len(base_prices)]
    percent = percents[((rotation // len(base_prices)) + (variant_index // len(base_prices))) % len(percents)]
    sale_price = Decimal(base_price) * (Decimal("1") - (Decimal(percent) / Decimal("100")))
    prompt = f"A ${base_price} item is discounted by {percent}%. What is the sale price?"
    return _build_numeric_variant(seed, prompt=prompt, answer=_json_numeric_value(sale_price))


def _math_average_or_median(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    values = [rng.randint(3, 18) for _ in range(5)]
    if rng.choice((True, False)):
        prompt = f"Find the median: {', '.join(str(value) for value in values)}"
        answer = sorted(values)[len(values) // 2]
    else:
        start = rng.randint(4, 18)
        step = rng.randint(2, 6)
        values = [start + (step * index) for index in range(4)]
        prompt = f"Find the average: {', '.join(str(value) for value in values)}"
        answer = Decimal(sum(values)) / Decimal(len(values))
    if isinstance(answer, Decimal):
        answer_value = _json_numeric_value(answer)
    else:
        answer_value = answer
    return _build_numeric_variant(seed, prompt=prompt, answer=answer_value)


def _math_algebra_lite(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    solution = rng.randint(3, 14)
    multiplier = rng.randint(2, 6)
    offset = rng.randint(4, 18)
    total = (solution * multiplier) + offset
    prompt = f"Solve for x: {multiplier}x + {offset} = {total}"
    return _build_numeric_variant(seed, prompt=prompt, answer=solution)


def _math_number_pattern(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    lane = (variant_index // 3) + (rotation % 5)
    kind = ("squares_plus_one", "triangular", "alternating")[variant_index % 3]
    if kind == "squares_plus_one":
        start = 2 + lane
        values = [(index * index) + 1 for index in range(start, start + 4)]
        answer = ((start + 4) * (start + 4)) + 1
    elif kind == "triangular":
        start = 2 + lane
        values = [int((n * (n + 1)) / 2) for n in range(start, start + 4)]
        next_n = start + 4
        answer = int((next_n * (next_n + 1)) / 2)
    else:
        start = 5 + (lane * 2)
        values = [start]
        deltas = [2, 4, 2, 4]
        for delta in deltas[:3]:
            values.append(values[-1] + delta)
        answer = values[-1] + deltas[3]
    prompt = "What number comes next? " + ", ".join(str(value) for value in values)
    return _build_numeric_variant(seed, prompt=prompt, answer=answer)


def _logic_sequence(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    family_offset = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:4], 16) % 5
    mode = variant_index % 4
    lane = (variant_index // 4) + family_offset
    if mode == 0:
        start = 5 + lane
        gap_a = 2 + (lane % 3)
        gap_b = gap_a + 2
        values = [start]
        for gap in (gap_a, gap_b, gap_a):
            values.append(values[-1] + gap)
        answer = values[-1] + gap_b
    elif mode == 1:
        start = 2 + (lane % 5)
        bump = 1 + (lane % 4)
        values = [start]
        for _ in range(3):
            values.append((values[-1] * 2) + bump)
        answer = (values[-1] * 2) + bump
    elif mode == 2:
        start = 1 + (lane % 4)
        values = [(index * index) + index + lane for index in range(start, start + 4)]
        next_index = start + 4
        answer = (next_index * next_index) + next_index + lane
    else:
        start = 3 + lane
        step = 3 + (lane % 4)
        values = [start]
        for index in range(1, 4):
            values.append(values[-1] + (step * index))
        answer = values[-1] + (step * 4)
    templates = (
        "What number comes next? {sequence}",
        "Continue the logic pattern: {sequence}",
        "Complete the sequence: {sequence}",
        "Number pattern round: {sequence}",
    )
    prompt = templates[variant_index % len(templates)].format(sequence=", ".join(str(value) for value in values))
    return _build_numeric_variant(seed, prompt=prompt, answer=answer)


def _logic_analogy(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    analogies = (
        ("bird", "nest", "bee", ["hive", "the hive"]),
        ("chef", "kitchen", "pilot", ["cockpit", "the cockpit"]),
        ("painter", "brush", "writer", ["pen", "a pen"]),
        ("clock", "time", "thermometer", ["temperature"]),
        ("key", "lock", "password", ["account", "login", "system"]),
        ("seed", "plant", "egg", ["animal", "bird", "creature"]),
        ("map", "navigate", "recipe", ["cook", "cooking"]),
        ("glove", "hand", "sock", ["foot", "the foot"]),
        ("orbit", "planet", "nest", ["bird", "a bird"]),
        ("score", "game", "grade", ["class", "schoolwork", "course"]),
    )
    templates = (
        "{a} is to {b} as {c} is to ?",
        "Complete the analogy: {a}:{b} :: {c}:?",
        "Which word finishes the analogy {a} -> {b}, {c} -> ?",
        "Analogy round: {a} belongs with {b}; {c} belongs with what?",
    )
    subject_a, subject_b, subject_c, accepted = analogies[(rotation + variant_index) % len(analogies)]
    prompt = templates[(rotation + (variant_index // len(analogies))) % len(templates)].format(
        a=subject_a.title(),
        b=subject_b,
        c=subject_c,
    )
    return _build_text_variant(seed, prompt=prompt, accepted=accepted)


def _logic_odd_one_out(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    sets = (
        (["violin", "cello", "cedar"], "cedar", "Which item does not belong with the other two?"),
        (["mercury", "venus", "granite"], "granite", "Which option breaks the category?"),
        (["ruby", "sapphire", "oak"], "oak", "Pick the odd one out."),
        (["triangle", "square", "sparrow"], "sparrow", "Which item comes from a different kind of set?"),
        (["salmon", "trout", "tulip"], "tulip", "Which option is the outsider?"),
        (["piano", "flute", "maple"], "maple", "Which one does not fit the group?"),
        (["hexagon", "pentagon", "thermometer"], "thermometer", "Choose the item that does not belong."),
        (["honey", "maple syrup", "granite"], "granite", "Which answer is the mismatch?"),
        (["oxygen", "nitrogen", "emerald"], "emerald", "Which item falls outside the category?"),
        (["eagle", "falcon", "fern"], "fern", "Which choice is the odd one out?"),
    )
    templates = (
        "{lead} A) {a} B) {b} C) {c}",
        "Logic lane: {lead} A) {a} B) {b} C) {c}",
        "Quick classification: {lead} A) {a} B) {b} C) {c}",
        "Lock in the odd one out. {lead} A) {a} B) {b} C) {c}",
    )
    choices, answer, lead = sets[(rotation + variant_index) % len(sets)]
    prompt = templates[(rotation + (variant_index // len(sets))) % len(templates)].format(
        lead=lead,
        a=choices[0],
        b=choices[1],
        c=choices[2],
    )
    return _build_choice_variant(seed, prompt=prompt, choices=choices, answer=answer)


def _logic_elimination(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    name_sets = (
        ("Mina", "Sol", "Theo"),
        ("Ava", "Ben", "Cole"),
        ("Lena", "Rui", "Tess"),
        ("Iris", "Jae", "Niko"),
        ("Pia", "Remy", "Sana"),
        ("Dara", "Eli", "Moss"),
        ("Nia", "Omar", "Paz"),
        ("Kira", "Luca", "Tao"),
    )
    item_sets = (
        ("tea", "juice", "cocoa"),
        ("red", "blue", "green"),
        ("cake", "fruit", "soup"),
        ("violin", "drum", "flute"),
        ("silver", "gold", "bronze"),
        ("bus", "train", "ferry"),
    )
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    names = name_sets[(rotation + variant_index) % len(name_sets)]
    items = item_sets[(rotation + (variant_index * 2)) % len(item_sets)]
    answer_index = (rotation + variant_index) % 3
    answer_name = names[answer_index]
    other_index = (answer_index + 1) % 3
    templates = (
        "{body}",
        "Elimination round: {body}",
        "Reason it out. {body}",
        "Logic lane elimination: {body}",
    )
    body = (
        f"{names[0]}, {names[1]}, and {names[2]} each picked one of these: {items[0]}, {items[1]}, {items[2]}. "
        f"{answer_name} picked neither {items[0]} nor {items[1]}. "
        f"{names[other_index]} did not pick {items[2]}. "
        f"Who picked {items[2]}?"
    )
    prompt = templates[(rotation + (variant_index // len(name_sets))) % len(templates)].format(body=body)
    return _build_text_variant(seed, prompt=prompt, accepted=[answer_name.casefold()])


def _logic_conditional(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    stems = (
        ("Every glorp is a blip, and no blip is silent.", "Can a glorp be silent?", False),
        ("Every rune is marked, and every marked object glows.", "Must every rune glow?", True),
        ("No silver key opens the vault, and every vault key is silver.", "Can a vault key open the vault?", False),
        ("Every lantern in the parade is lit, and every lit lantern is visible from the hill.", "Must every parade lantern be visible from the hill?", True),
        ("Every desert fox is nocturnal, and no nocturnal animal is active at noon.", "Can a desert fox be active at noon under those rules?", False),
        ("Every choir member rehearses, and every person who rehearses gets the schedule.", "Must every choir member get the schedule?", True),
        ("No cracked screen is waterproof, and every tablet in the test group is waterproof.", "Can a test-group tablet have a cracked screen?", False),
        ("Every archive key is numbered, and every numbered key is logged.", "Must every archive key be logged?", True),
        ("Every comet trail is bright, and some bright things fade fast.", "Does that prove every comet trail fades fast?", False),
        ("Every certified diver has training, and every trained diver passed the exam.", "Must every certified diver have passed the exam?", True),
    )
    stem, question, value = stems[(rotation + variant_index) % len(stems)]
    templates = (
        "True or false: {stem} {question}",
        "Conditional check, true or false: {stem} {question}",
        "Reasoning T/F: {stem} {question}",
        "Does this conclusion follow? {stem} {question}",
    )
    prompt = templates[(rotation + (variant_index // len(stems))) % len(templates)].format(stem=stem, question=question)
    return _build_boolean_variant(seed, prompt=prompt, value=value)


def _logic_parity_grouping(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    offset = 2 * ((rotation % 7) + variant_index)
    mode = variant_index % 3
    if mode == 0:
        set_a = [offset + 1, offset + 3, offset + 6]
        set_b = [offset + 1, offset + 2, offset + 4]
        set_c = [offset + 2, offset + 4, offset + 5]
        answer = "set a"
        lead = "Which set has an even total?"
    elif mode == 1:
        set_a = [offset + 1, offset + 2, offset + 5]
        set_b = [offset + 2, offset + 4, offset + 6]
        set_c = [offset + 3, offset + 5, offset + 6]
        answer = "set c"
        lead = "Which set contains exactly two odd numbers?"
    else:
        base = 3 * ((rotation % 5) + variant_index + 2)
        set_a = [base, base + 3, base + 6]
        set_b = [base + 1, base + 4, base + 6]
        set_c = [base + 2, base + 3, base + 7]
        answer = "set a"
        lead = "Which set adds to a multiple of 3?"
    templates = (
        "{lead} A) {a0}, {a1}, {a2} B) {b0}, {b1}, {b2} C) {c0}, {c1}, {c2}",
        "Parity round: {lead} A) {a0}, {a1}, {a2} B) {b0}, {b1}, {b2} C) {c0}, {c1}, {c2}",
        "Choose the set that fits the rule. {lead} A) {a0}, {a1}, {a2} B) {b0}, {b1}, {b2} C) {c0}, {c1}, {c2}",
        "Logic grouping: {lead} A) {a0}, {a1}, {a2} B) {b0}, {b1}, {b2} C) {c0}, {c1}, {c2}",
    )
    prompt = templates[(rotation + (variant_index // 3)) % len(templates)].format(
        lead=lead,
        a0=set_a[0],
        a1=set_a[1],
        a2=set_a[2],
        b0=set_b[0],
        b1=set_b[1],
        b2=set_b[2],
        c0=set_c[0],
        c1=set_c[1],
        c2=set_c[2],
    )
    return _build_choice_variant(seed, prompt=prompt, choices=["set a", "set b", "set c"], answer=answer)


def _logic_true_false(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    name_sets = (
        ("Ana", "Ben", "Cam"),
        ("Jules", "Micah", "Priya"),
        ("Mira", "Noor", "Pax"),
        ("Eden", "Finn", "Gia"),
        ("Lio", "Mara", "Sol"),
        ("Tara", "Vale", "Wren"),
        ("Iris", "Jae", "Kian"),
        ("Nova", "Orin", "Pia"),
    )
    relation_sets = (
        ("finished before", "finished before"),
        ("scored more than", "scored more than"),
        ("is taller than", "is taller than"),
        ("arrived earlier than", "arrived earlier than"),
    )
    names = name_sets[(rotation + variant_index) % len(name_sets)]
    relation_index = ((rotation // len(name_sets)) + (variant_index // len(name_sets))) % len(relation_sets)
    relation, conclusion = relation_sets[relation_index]
    if (variant_index // len(name_sets)) % 2 == 0:
        statement = f"{names[0]} {relation} {names[1]}, and {names[1]} {relation} {names[2]}. Therefore {names[0]} {conclusion} {names[2]}."
        value = True
    else:
        statement = f"{names[0]} {relation} {names[1]}, and {names[1]} {relation} {names[2]}. Therefore {names[2]} {conclusion} {names[0]}."
        value = False
    templates = (
        "Call this deduction true or false: {statement}",
        "Does this conclusion hold up? {statement}",
        "Reasoning check: {statement} True or false?",
        "Logic verdict: {statement} True or false?",
    )
    prompt = templates[(rotation + relation_index) % len(templates)].format(statement=statement)
    return _build_boolean_variant(seed, prompt=prompt, value=value)


def _logic_classification(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    sets = (
        ("Which word fits with level, radar, and civic?", ["river", "candle", "rotor"], "rotor"),
        ("Which item belongs with ruby, sapphire, and emerald?", ["cedar", "diamond", "copper"], "diamond"),
        ("Which item belongs with piano, violin, and flute?", ["trumpet", "bookshelf", "cello"], "cello"),
        ("Which item belongs with haiku, sonnet, and ode?", ["novel", "ballad", "bridge"], "ballad"),
        ("Which option belongs with mercury, venus, and mars?", ["jupiter", "granite", "sapphire"], "jupiter"),
        ("Which option belongs with cedar, pine, and spruce?", ["maple", "steel", "linen"], "maple"),
        ("Which item belongs with circle, square, and triangle?", ["hexagon", "violin", "harbor"], "hexagon"),
        ("Which option belongs with copper, silver, and gold?", ["bronze", "willow", "granite"], "bronze"),
    )
    templates = (
        "{question} A) {a} B) {b} C) {c}",
        "Classification round: {question} A) {a} B) {b} C) {c}",
        "Which answer belongs with the set? {question} A) {a} B) {b} C) {c}",
        "Logic lane classification: {question} A) {a} B) {b} C) {c}",
    )
    question, choices, answer = sets[(rotation + variant_index) % len(sets)]
    prompt = templates[(rotation + (variant_index // len(sets))) % len(templates)].format(
        question=question,
        a=choices[0],
        b=choices[1],
        c=choices[2],
    )
    return _build_choice_variant(seed, prompt=prompt, choices=choices, answer=answer)


def _logic_mini_deduction(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    sets = (
        (
            "Three lockers are labeled A, B, and C. Exactly one label is true. "
            "Locker A says 'The prize is in locker B.' Locker B says 'The prize is not in locker B.' "
            "Locker C says 'The prize is in locker C.' Where is the prize?",
            ["locker a", "locker b", "locker c"],
            "locker b",
        ),
        (
            "Exactly one statement is true. Nia says 'The code is blue.' Omar says 'The code is red.' "
            "Paz says 'The code is not blue.' What color is the code?",
            ["blue", "red", "green"],
            "red",
        ),
        (
            "Exactly one statement is true. Ivo says 'The gem is silver.' June says 'The gem is gold.' "
            "Kai says 'Ivo is telling the truth.' What color is the gem?",
            ["silver", "gold", "green"],
            "gold",
        ),
        (
            "Three cups are labeled left, middle, and right. Exactly one label is true. "
            "Left says 'The coin is in the middle cup.' Middle says 'The coin is not in the middle cup.' "
            "Right says 'The coin is in the right cup.' Which cup holds the coin?",
            ["left cup", "middle cup", "right cup"],
            "middle cup",
        ),
        (
            "Exactly one of these clues is true. Ava says 'The meeting is on Tuesday.' Ben says 'The meeting is on Thursday.' "
            "Cora says 'Ava is wrong.' Which day is the meeting on?",
            ["tuesday", "thursday", "wednesday"],
            "thursday",
        ),
        (
            "Exactly one statement is true. Remy says 'The package is heavy.' Sora says 'The package is light.' "
            "Tao says 'Remy is telling the truth.' Which description fits the package?",
            ["heavy", "light", "fragile"],
            "light",
        ),
        (
            "Exactly one statement is true. A says 'The note is in drawer B.' B says 'The note is not in drawer B.' "
            "C says 'The note is in drawer C.' Which drawer has the note?",
            ["drawer a", "drawer b", "drawer c"],
            "drawer b",
        ),
        (
            "Exactly one statement is true. Mina says 'The mural is blue.' Sol says 'The mural is red.' "
            "Theo says 'Mina is wrong.' What color is the mural?",
            ["blue", "red", "green"],
            "red",
        ),
    )
    templates = (
        "{core} A) {a} B) {b} C) {c}",
        "Deduction round: {core} A) {a} B) {b} C) {c}",
        "Reason it out. {core} A) {a} B) {b} C) {c}",
        "Mini deduction: {core} A) {a} B) {b} C) {c}",
    )
    core, choices, answer = sets[(rotation + variant_index) % len(sets)]
    prompt = templates[(rotation + (variant_index // len(sets))) % len(templates)].format(
        core=core,
        a=choices[0],
        b=choices[1],
        c=choices[2],
    )
    return _build_choice_variant(seed, prompt=prompt, choices=choices, answer=answer)


def _logic_rotation(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rotation = int(hashlib.sha256(f"{seed_material}:{seed['concept_id']}".encode("utf-8")).hexdigest()[:6], 16)
    token_sets = (
        ("north", "east", "south", "west"),
        ("up", "right", "down", "left"),
        ("northeast", "southeast", "southwest", "northwest"),
    )
    tokens = token_sets[(rotation + variant_index) % len(token_sets)]
    step_options = (1, -1, 2, -2)
    step = step_options[(rotation + (variant_index // len(token_sets))) % len(step_options)]
    start_index = (rotation + variant_index) % len(tokens)
    length = 5 + (variant_index // (len(token_sets) * len(step_options)))
    values = [tokens[(start_index + (step * index)) % len(tokens)] for index in range(length)]
    answer = tokens[(start_index + (step * length)) % len(tokens)]
    distractors = [token for token in tokens if token != answer]
    unique_choices = [answer, distractors[0], distractors[1]]
    templates = (
        "Which direction comes next in this turning pattern? Sequence: {sequence}. A) {a} B) {b} C) {c}",
        "Rotation round: Sequence: {sequence}. Which direction comes next? A) {a} B) {b} C) {c}",
        "Keep the turn pattern going. Sequence: {sequence}. A) {a} B) {b} C) {c}",
        "Logic lane rotation: Sequence: {sequence}. Which option is next? A) {a} B) {b} C) {c}",
    )
    prompt = templates[(rotation + (variant_index // len(token_sets))) % len(templates)].format(
        sequence=", ".join(values),
        a=unique_choices[0],
        b=unique_choices[1],
        c=unique_choices[2],
    )
    return _build_choice_variant(seed, prompt=prompt, choices=unique_choices, answer=answer)


def _language_anagram(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    targets = (
        ("planet", "a world that orbits a star"),
        ("camera", "a device used to take photos"),
        ("island", "land surrounded by water"),
        ("winter", "the coldest season"),
        ("garden", "a place where flowers or vegetables grow"),
        ("silver", "a gray-white precious metal"),
        ("bridge", "a structure used to cross a river"),
        ("library", "a place full of books"),
        ("rocket", "a vehicle launched into space"),
        ("blanket", "a soft cover used for warmth"),
        ("harvest", "the season or act of gathering crops"),
        ("lantern", "a portable light source"),
        ("compass", "a tool that points north"),
        ("harbor", "a sheltered place for ships"),
        ("thunder", "the sound that follows lightning"),
        ("velvet", "a soft woven fabric"),
        ("glacier", "a slow-moving river of ice"),
        ("orchard", "a place where fruit trees grow"),
        ("kingdom", "a realm ruled by a monarch"),
        ("bicycle", "a two-wheeled vehicle powered by pedals"),
        ("shelter", "a place offering protection"),
        ("journal", "a daily written record"),
        ("meadow", "an open field full of grass"),
        ("crystal", "a solid with a repeating pattern"),
        ("violin", "a bowed string instrument"),
        ("sunrise", "the moment the sun first appears"),
        ("blanket", "a soft cover used for warmth"),
        ("gallery", "a place where art is displayed"),
        ("network", "a connected system of people or things"),
        ("harpoon", "a spear used for hunting large sea animals"),
        ("laneway", "a narrow passage between buildings"),
        ("marble", "a stone used in sculpture and flooring"),
        ("quarter", "one fourth of a whole"),
        ("cabinet", "a piece of furniture with doors"),
        ("miracle", "an event seen as extraordinary"),
        ("citadel", "a fortified stronghold"),
        ("journey", "travel from one place to another"),
        ("kingfisher", "a brightly colored bird that hunts fish"),
        ("triangle", "a shape with three sides"),
        ("wildfire", "an uncontrolled fire in natural land"),
    )
    target, clue = targets[(variant_index + rng.randrange(len(targets))) % len(targets)]
    letters = list(target.upper())
    scrambled = target.upper()
    for _ in range(12):
        rng.shuffle(letters)
        scrambled = "".join(letters)
        if scrambled != target.upper():
            break
    if scrambled == target.upper():
        scrambled = target.upper()[::-1]
    prompt = f"Unscramble the clue-backed word. Clue: {clue}. Letters: **{scrambled}**"
    return _build_text_variant(seed, prompt=prompt, accepted=[target])


GENERATOR_HANDLERS: dict[str, Callable[..., QuestionDropVariant]] = {
    "static_pack": _static_variant,
    "math_addition": _math_addition,
    "math_multiplication": _math_multiplication,
    "math_order_operations": _math_order_operations,
    "math_missing_value": _math_missing_value,
    "math_compare_expressions": _math_compare_expressions,
    "math_multi_step": _math_multi_step,
    "math_divisibility": _math_divisibility,
    "math_remainder": _math_remainder,
    "math_percent_change": _math_percent_change,
    "math_average_or_median": _math_average_or_median,
    "math_algebra_lite": _math_algebra_lite,
    "math_number_pattern": _math_number_pattern,
    "logic_sequence": _logic_sequence,
    "logic_analogy": _logic_analogy,
    "logic_odd_one_out": _logic_odd_one_out,
    "logic_elimination": _logic_elimination,
    "logic_conditional": _logic_conditional,
    "logic_parity_grouping": _logic_parity_grouping,
    "logic_true_false": _logic_true_false,
    "logic_classification": _logic_classification,
    "logic_mini_deduction": _logic_mini_deduction,
    "logic_rotation": _logic_rotation,
    "language_anagram": _language_anagram,
}


_ANSWER_SHAPE_BY_GENERATOR = {
    "math_addition": "numeric",
    "math_multiplication": "numeric",
    "math_order_operations": "numeric",
    "math_missing_value": "numeric",
    "math_compare_expressions": "multiple_choice",
    "math_multi_step": "numeric",
    "math_divisibility": "multiple_choice",
    "math_remainder": "numeric",
    "math_percent_change": "numeric",
    "math_average_or_median": "numeric",
    "math_algebra_lite": "numeric",
    "math_number_pattern": "numeric",
    "logic_sequence": "numeric",
    "logic_analogy": "text",
    "logic_odd_one_out": "multiple_choice",
    "logic_elimination": "text",
    "logic_conditional": "boolean",
    "logic_parity_grouping": "multiple_choice",
    "logic_true_false": "boolean",
    "logic_classification": "multiple_choice",
    "logic_mini_deduction": "multiple_choice",
    "logic_rotation": "multiple_choice",
    "language_anagram": "text",
}


_BASE_QUESTION_DROP_SEEDS: tuple[dict[str, Any], ...] = (
    _static_seed(
        "science:planet-red",
        "science",
        1,
        "science.astronomy-basics",
        _variant("Which planet is known as the Red Planet?", _text_spec("mars")),
        _variant("Name the planet nicknamed the Red Planet.", _text_spec("mars")),
    ),
    _static_seed(
        "science:water-chemical",
        "science",
        1,
        "science.molecules",
        _variant("What is the chemical formula for water?", _text_spec("h2o")),
        _variant("Write the chemical formula for water.", _text_spec("h2o")),
    ),
    _static_seed(
        "science:earth-satellite",
        "science",
        1,
        "science.astronomy-basics",
        _variant("What is Earth's natural satellite?", _text_spec("moon", "the moon")),
        _variant("Name the natural satellite that orbits Earth.", _text_spec("moon", "the moon")),
    ),
    _static_seed(
        "science:plants-gas",
        "science",
        2,
        "science.photosynthesis",
        _variant("Which gas do plants absorb during photosynthesis?", _text_spec("carbon dioxide", "co2")),
        _variant("Plants pull in which gas during photosynthesis?", _text_spec("carbon dioxide", "co2")),
    ),
    _static_seed(
        "science:chemical-change",
        "science",
        2,
        "science.changes-of-matter",
        _variant(
            "Which is a chemical change? A) melting ice B) rusting iron C) cutting paper",
            _multiple_choice_spec("melting ice", "rusting iron", "cutting paper", answer="rusting iron"),
        ),
        _variant(
            "Pick the chemical change. A) freezing water B) rusting iron C) breaking glass",
            _multiple_choice_spec("freezing water", "rusting iron", "breaking glass", answer="rusting iron"),
        ),
    ),
    _static_seed(
        "science:food-chain",
        "science",
        2,
        "science.ecology",
        _variant("In a simple food chain, what comes right after a producer?", _text_spec("primary consumer", "first consumer", "herbivore")),
        _variant("A producer is eaten by what kind of consumer first?", _text_spec("primary consumer", "first consumer", "herbivore")),
    ),
    _static_seed(
        "science:experiment-control",
        "science",
        3,
        "science.experimental-design",
        _variant(
            "You are testing which paper towel absorbs the most water. What should stay the same in every trial? "
            "A) the amount of water B) which towel wins C) the final score",
            _multiple_choice_spec("the amount of water", "which towel wins", "the final score", answer="the amount of water"),
        ),
        _variant(
            "To compare seed growth in sun versus shade fairly, what should stay the same? "
            "A) soil and water B) the result you want C) the plant height after the test",
            _multiple_choice_spec("soil and water", "the result you want", "the plant height after the test", answer="soil and water"),
        ),
    ),
    _static_seed(
        "science:chlorophyll",
        "science",
        3,
        "science.plant-processes",
        _variant("A plant kept away from sunlight may turn pale because it cannot make enough what green pigment?", _text_spec("chlorophyll")),
        _variant("What green pigment helps plants capture light for photosynthesis?", _text_spec("chlorophyll")),
    ),
    _static_seed(
        "science:insulator-choice",
        "science",
        3,
        "science.materials",
        _variant(
            "Which material is the best electrical insulator? A) copper B) rubber C) aluminum",
            _multiple_choice_spec("copper", "rubber", "aluminum", answer="rubber"),
        ),
        _variant(
            "Pick the best insulator. A) steel B) rubber C) silver",
            _multiple_choice_spec("steel", "rubber", "silver", answer="rubber"),
        ),
    ),
    _static_seed(
        "history:first-us-president",
        "history",
        1,
        "history.foundations",
        _variant("Who was the first president of the United States?", _text_spec("george washington", "washington")),
        _variant("Name the first U.S. president.", _text_spec("george washington", "washington")),
    ),
    _static_seed(
        "history:titanic",
        "history",
        1,
        "history.iconic-events",
        _variant("What was the name of the ship that hit an iceberg and sank in 1912?", _text_spec("titanic", "the titanic")),
        _variant("Name the famous ocean liner that sank after striking an iceberg in 1912.", _text_spec("titanic", "the titanic")),
    ),
    _static_seed(
        "history:moon-landing-year",
        "history",
        2,
        "history.space-race",
        _variant("In what year did humans first land on the Moon?", _numeric_spec(1969)),
        _variant("Apollo 11 landed on the Moon in which year?", _numeric_spec(1969)),
    ),
    _static_seed(
        "history:berlin-wall-fall",
        "history",
        2,
        "history.twentieth-century",
        _variant("What year did the Berlin Wall fall?", _numeric_spec(1989)),
        _variant("In which year was the Berlin Wall opened and effectively brought down?", _numeric_spec(1989)),
    ),
    _static_seed(
        "history:printing-telephone-internet",
        "history",
        2,
        "history.chronology-order",
        _variant(
            "Order these from earliest to latest: printing press, telephone, internet",
            _ordered_spec("printing press", "telephone", "internet"),
        ),
        _variant(
            "Put these in time order: printing press, internet, telephone",
            _ordered_spec("printing press", "telephone", "internet"),
        ),
    ),
    _static_seed(
        "history:declaration-constitution",
        "history",
        2,
        "history.foundations",
        _variant("True or false: the U.S. Declaration of Independence came before the U.S. Constitution.", _boolean_spec(True)),
        _variant("True or false: the U.S. Constitution was written before the Declaration of Independence.", _boolean_spec(False)),
    ),
    _static_seed(
        "history:magnacarta-frenchrevolution-moonlanding",
        "history",
        3,
        "history.big-timeline",
        _variant(
            "Order these from earliest to latest: Magna Carta, French Revolution, Moon landing",
            _ordered_spec("magna carta", "french revolution", "moon landing"),
        ),
        _variant(
            "Put these in time order: Moon landing, Magna Carta, French Revolution",
            _ordered_spec("magna carta", "french revolution", "moon landing"),
        ),
    ),
    _static_seed(
        "history:constantinople-columbus-luther",
        "history",
        3,
        "history.big-timeline",
        _variant(
            "Order these from earliest to latest: fall of Constantinople, Columbus reaches the Caribbean, Martin Luther posts the 95 Theses",
            _ordered_spec("fall of constantinople", "columbus reaches the caribbean", "martin luther posts the 95 theses"),
        ),
        _variant(
            "Put these in time order: Martin Luther posts the 95 Theses, fall of Constantinople, Columbus reaches the Caribbean",
            _ordered_spec("fall of constantinople", "columbus reaches the caribbean", "martin luther posts the 95 theses"),
        ),
    ),
    _static_seed(
        "history:renaissance-enlightenment-industrial",
        "history",
        3,
        "history.movements",
        _variant(
            "Which came last? A) Renaissance B) Enlightenment C) Industrial Revolution",
            _multiple_choice_spec("renaissance", "enlightenment", "industrial revolution", answer="industrial revolution"),
        ),
        _variant(
            "Pick the latest movement here. A) Industrial Revolution B) Renaissance C) Enlightenment",
            _multiple_choice_spec("industrial revolution", "renaissance", "enlightenment", answer="industrial revolution"),
        ),
    ),
    _static_seed(
        "geography:nile-continent",
        "geography",
        1,
        "geography.continents",
        _variant("The Nile River is on which continent?", _text_spec("africa")),
        _variant("Which continent is home to the Nile?", _text_spec("africa")),
    ),
    _static_seed(
        "geography:japan-capital",
        "geography",
        1,
        "geography.capitals",
        _variant("What is the capital city of Japan?", _text_spec("tokyo")),
        _variant("Name Japan's capital.", _text_spec("tokyo")),
    ),
    _static_seed(
        "geography:largest-ocean",
        "geography",
        2,
        "geography.oceans",
        _variant("What is the largest ocean on Earth?", _text_spec("pacific", "pacific ocean")),
        _variant("Name Earth's largest ocean.", _text_spec("pacific", "pacific ocean")),
    ),
    _static_seed(
        "geography:sahara",
        "geography",
        1,
        "geography.landforms",
        _variant("What is the largest hot desert on Earth?", _text_spec("sahara", "sahara desert", "the sahara")),
        _variant("Name Earth's largest hot desert.", _text_spec("sahara", "sahara desert", "the sahara")),
    ),
    _static_seed(
        "geography:indian-ocean",
        "geography",
        2,
        "geography.oceans",
        _variant("Which ocean lies east of Africa and west of Australia?", _text_spec("indian", "indian ocean")),
        _variant("Name the ocean between Africa and Australia.", _text_spec("indian", "indian ocean")),
    ),
    _static_seed(
        "geography:landlocked-country",
        "geography",
        2,
        "geography.country-traits",
        _variant(
            "Which country here has no coastline? A) Nepal B) Portugal C) Japan",
            _multiple_choice_spec("nepal", "portugal", "japan", answer="nepal"),
        ),
        _variant(
            "Pick the landlocked country. A) Chile B) Nepal C) Iceland",
            _multiple_choice_spec("chile", "nepal", "iceland", answer="nepal"),
        ),
    ),
    _static_seed(
        "geography:paris-rome-latitude",
        "geography",
        3,
        "geography.relative-position",
        _variant(
            "Which city sits farther north? A) Rome B) Paris C) Madrid",
            _multiple_choice_spec("rome", "paris", "madrid", answer="paris"),
        ),
        _variant(
            "Pick the northernmost city. A) Paris B) Rome C) Athens",
            _multiple_choice_spec("paris", "rome", "athens", answer="paris"),
        ),
    ),
    _static_seed(
        "geography:transcontinental-country",
        "geography",
        3,
        "geography.country-traits",
        _variant(
            "Which country spans both Europe and Asia in common geographic usage? A) Turkey B) Peru C) Kenya",
            _multiple_choice_spec("turkey", "peru", "kenya", answer="turkey"),
        ),
        _variant(
            "Pick the transcontinental country here. A) Morocco B) Turkey C) Vietnam",
            _multiple_choice_spec("morocco", "turkey", "vietnam", answer="turkey"),
        ),
    ),
    _static_seed(
        "geography:cairo-nairobi-capetown",
        "geography",
        3,
        "geography.relative-position",
        _variant(
            "Order these from north to south: Cairo, Nairobi, Cape Town",
            _ordered_spec("cairo", "nairobi", "cape town"),
        ),
        _variant(
            "Put these in north-to-south order: Cape Town, Cairo, Nairobi",
            _ordered_spec("cairo", "nairobi", "cape town"),
        ),
    ),
    _static_seed(
        "language:oxford-comma-boolean",
        "language",
        2,
        "language.grammar",
        _variant("True or false: an Oxford comma appears before the final item in a list.", _boolean_spec(True)),
        _variant("True or false: the Oxford comma is the comma placed before the last item in a list of three or more.", _boolean_spec(True)),
    ),
    _static_seed(
        "language:plural-cactus",
        "language",
        2,
        "language.word-forms",
        _variant("What is the plural of 'cactus' in common English usage?", _text_spec("cacti", "cactuses")),
        _variant("Give a common English plural for 'cactus'.", _text_spec("cacti", "cactuses")),
    ),
    _generated_seed("language:anagram", "language", 2, "language_anagram", "language.anagrams"),
    _static_seed(
        "language:prefix-pre",
        "language",
        1,
        "language.word-parts",
        _variant("Which prefix means 'before'? A) pre B) post C) anti", _multiple_choice_spec("pre", "post", "anti", answer="pre")),
        _variant("Pick the prefix that means 'before'. A) re B) pre C) mis", _multiple_choice_spec("re", "pre", "mis", answer="pre")),
    ),
    _static_seed(
        "language:homophone-their",
        "language",
        2,
        "language.usage",
        _variant(
            "Which word correctly completes this sentence? 'The players carried ___ jerseys into the tunnel.' "
            "A) there B) their C) theyre",
            _multiple_choice_spec("there", "their", "theyre", answer="their"),
        ),
        _variant(
            "Pick the correct word: 'I left the books over ___. A) their B) there C) theyre'",
            _multiple_choice_spec("their", "there", "theyre", answer="there"),
        ),
    ),
    _static_seed(
        "language:book-song-analogy",
        "language",
        2,
        "language.analogy",
        _variant("Book is to read as song is to ___", _text_spec("listen", "listen to", "listening")),
        _variant("Page is to book as note is to ___", _text_spec("song", "music")),
    ),
    _static_seed(
        "language:alphabet-order",
        "language",
        3,
        "language.ordering",
        _variant("Put these in alphabetical order: cedar, birch, maple", _ordered_spec("birch", "cedar", "maple")),
        _variant("Alphabetize these: maple, cedar, birch", _ordered_spec("birch", "cedar", "maple")),
    ),
    _static_seed(
        "language:its-its-usage",
        "language",
        3,
        "language.usage",
        _variant(
            "Which sentence is correct? A) The robot lost its balance. B) The robot lost its balances. C) The robot lost itss balance.",
            _multiple_choice_spec("the robot lost its balance", "the robot lost its balances", "the robot lost itss balance", answer="the robot lost its balance"),
        ),
        _variant(
            "Pick the correct sentence. A) Its going to rain. B) Its a bright day. C) The team forgot its plan.",
            _multiple_choice_spec("its going to rain", "its a bright day", "the team forgot its plan", answer="the team forgot its plan"),
        ),
    ),
    _static_seed(
        "language:palindrome-classification",
        "language",
        3,
        "language.patterns",
        _variant(
            "Which word belongs with level, civic, and radar? A) river B) rotor C) lantern",
            _multiple_choice_spec("river", "rotor", "lantern", answer="rotor"),
        ),
        _variant(
            "Pick the palindrome. A) garden B) mirror C) refer",
            _multiple_choice_spec("garden", "mirror", "refer", answer="refer"),
        ),
    ),
    _generated_seed("logic:sequence", "logic", 2, "logic_sequence", "logic.sequence"),
    _generated_seed("logic:analogy", "logic", 2, "logic_analogy", "logic.analogy"),
    _generated_seed("logic:odd-one-out", "logic", 2, "logic_odd_one_out", "logic.odd-one-out"),
    _generated_seed("logic:elimination", "logic", 3, "logic_elimination", "logic.elimination"),
    _generated_seed("logic:conditional", "logic", 2, "logic_conditional", "logic.conditional"),
    _generated_seed("logic:parity-grouping", "logic", 2, "logic_parity_grouping", "logic.parity"),
    _generated_seed("logic:true-false", "logic", 2, "logic_true_false", "logic.inference"),
    _generated_seed("logic:classification", "logic", 3, "logic_classification", "logic.classification"),
    _generated_seed("logic:mini-deduction", "logic", 3, "logic_mini_deduction", "logic.mini-deduction"),
    _generated_seed("logic:rotation", "logic", 3, "logic_rotation", "logic.rotation"),
    _generated_seed("math:addition", "math", 1, "math_addition", "math.arithmetic-addition"),
    _generated_seed("math:multiplication", "math", 2, "math_multiplication", "math.arithmetic-multiplication"),
    _generated_seed("math:order-operations", "math", 2, "math_order_operations", "math.order-operations"),
    _generated_seed("math:missing-value", "math", 2, "math_missing_value", "math.missing-value"),
    _generated_seed("math:compare-expressions", "math", 2, "math_compare_expressions", "math.compare-expressions"),
    _generated_seed("math:multi-step", "math", 2, "math_multi_step", "math.multi-step"),
    _generated_seed("math:divisibility", "math", 2, "math_divisibility", "math.divisibility"),
    _generated_seed("math:remainder", "math", 3, "math_remainder", "math.remainder"),
    _generated_seed("math:percent-change", "math", 3, "math_percent_change", "math.percent-change"),
    _generated_seed("math:average-or-median", "math", 3, "math_average_or_median", "math.average-median"),
    _generated_seed("math:algebra-lite", "math", 3, "math_algebra_lite", "math.algebra-lite"),
    _generated_seed("math:number-pattern", "math", 3, "math_number_pattern", "math.number-pattern"),
    _static_seed(
        "culture:chess-piece",
        "culture",
        1,
        "culture.games-classic",
        _variant("Which chess piece can move in an L shape?", _text_spec("knight", "the knight")),
        _variant("Name the chess piece that moves in an L shape.", _text_spec("knight", "the knight")),
    ),
    _static_seed(
        "culture:primary-colors",
        "culture",
        2,
        "culture.art-basics",
        _variant("Put these additive primary colors in order from shortest answer length to longest: blue, red, green", _ordered_spec("red", "blue", "green")),
        _variant("Order these additive primary colors from shortest word to longest: blue, red, green", _ordered_spec("red", "blue", "green")),
    ),
    _static_seed(
        "culture:piano-keys",
        "culture",
        1,
        "culture.music-basics",
        _variant("How many keys does a standard piano have?", _numeric_spec(88)),
        _variant("A standard piano has how many keys?", _numeric_spec(88)),
    ),
    _static_seed(
        "culture:monopoly",
        "culture",
        1,
        "culture.games-classic",
        _variant("Which board game is built around properties, railroads, and hotels?", _text_spec("monopoly")),
        _variant("Name the board game where players buy properties and build hotels.", _text_spec("monopoly")),
    ),
    _static_seed(
        "culture:baseball-innings",
        "culture",
        2,
        "culture.sports",
        _variant("Which sport is played in innings? A) baseball B) soccer C) tennis", _multiple_choice_spec("baseball", "soccer", "tennis", answer="baseball")),
        _variant("Pick the sport with innings. A) hockey B) baseball C) rugby", _multiple_choice_spec("hockey", "baseball", "rugby", answer="baseball")),
    ),
    _static_seed(
        "culture:percussion-choice",
        "culture",
        2,
        "culture.music-basics",
        _variant("Which instrument is percussion? A) cello B) tambourine C) oboe", _multiple_choice_spec("cello", "tambourine", "oboe", answer="tambourine")),
        _variant("Pick the percussion instrument. A) tambourine B) trumpet C) violin", _multiple_choice_spec("tambourine", "trumpet", "violin", answer="tambourine")),
    ),
    _static_seed(
        "culture:impressionism-cubism",
        "culture",
        3,
        "culture.art-history",
        _variant("True or false: Impressionism came before Cubism.", _boolean_spec(True)),
        _variant("True or false: Cubism came before Impressionism.", _boolean_spec(False)),
    ),
    _static_seed(
        "culture:stanley-cup",
        "culture",
        3,
        "culture.sports",
        _variant("What trophy is awarded to the NHL champion?", _text_spec("stanley cup", "the stanley cup")),
        _variant("Name the trophy won by the NHL champion.", _text_spec("stanley cup", "the stanley cup")),
    ),
    _static_seed(
        "culture:tempo-order",
        "culture",
        3,
        "culture.music-terms",
        _variant("Order these from slowest to fastest: largo, andante, presto", _ordered_spec("largo", "andante", "presto")),
        _variant("Put these tempos in order from slowest to fastest: presto, largo, andante", _ordered_spec("largo", "andante", "presto")),
    ),
)


def _seed_answer_shape(seed: dict[str, Any]) -> str:
    generator_type = str(seed.get("generator_type") or "").strip()
    if generator_type == "static_pack":
        variants = seed.get("variants") or ()
        if variants:
            return str((variants[0] or {}).get("answer_spec", {}).get("type") or "text").strip().casefold() or "text"
        return "text"
    return _ANSWER_SHAPE_BY_GENERATOR.get(generator_type, "text")


def _seed_subcategory(seed: dict[str, Any]) -> str:
    family_id = str(seed.get("family_id") or "").strip().casefold()
    category = str(seed.get("category") or "question-drops").strip().casefold()
    if "." in family_id:
        tail = family_id.split(".", 1)[1]
        if tail:
            return tail
    return family_id or category


def _seed_reasoning_mode(seed: dict[str, Any], answer_shape: str) -> str:
    seed_blob = " ".join(
        str(value or "").casefold()
        for value in (
            seed.get("concept_id"),
            seed.get("family_id"),
            seed.get("generator_type"),
        )
    )
    keyword_modes = (
        ("ordering", ("order", "timeline", "chronology", "sequence", "rotation", "relative-position")),
        ("analogy", ("analogy",)),
        ("inference", ("conditional", "deduction", "inference", "elimination", "truth", "parity")),
        ("definition", ("word-parts", "roots", "grammar", "definition", "terms", "usage")),
        ("causal", ("photosynthesis", "ecology", "process", "weather", "motion", "climate", "immunology", "experimental")),
        ("comparison", ("compare", "classification", "country-traits", "patterns")),
        ("number-sense", ("math", "divisibility", "remainder", "average", "pattern", "algebra", "ratio")),
    )
    for mode, keywords in keyword_modes:
        if any(keyword in seed_blob for keyword in keywords):
            return mode
    if answer_shape == "ordered_tokens":
        return "ordering"
    if answer_shape == "boolean":
        return "verification"
    if answer_shape == "multiple_choice":
        return "classification"
    if answer_shape == "numeric":
        return "number-sense" if str(seed.get("category") or "").strip().casefold() == "math" else "fact"
    return "fact"


def _merge_seed_tags(seed: dict[str, Any]) -> tuple[str, ...]:
    answer_shape = _seed_answer_shape(seed)
    required = (
        f"sub:{_seed_subcategory(seed)}",
        f"mode:{_seed_reasoning_mode(seed, answer_shape)}",
        f"shape:{answer_shape}",
    )
    merged: list[str] = []
    for tag in tuple(seed.get("tags", ())) + required:
        cleaned = str(tag or "").strip().casefold()
        if cleaned and cleaned not in merged:
            merged.append(cleaned)
    return tuple(merged)


def _decorate_seed(seed: dict[str, Any]) -> dict[str, Any]:
    return {**seed, "tags": _merge_seed_tags(seed)}


QUESTION_DROP_SEEDS: tuple[dict[str, Any], ...] = tuple(
    _decorate_seed(seed) for seed in (_BASE_QUESTION_DROP_SEEDS + EXPANDED_STATIC_SEEDS)
)

QUESTION_DROP_SEED_BY_CONCEPT_ID = {seed["concept_id"]: seed for seed in QUESTION_DROP_SEEDS}


def question_drop_seed_for_concept(concept_id: str | None) -> dict[str, Any] | None:
    return QUESTION_DROP_SEED_BY_CONCEPT_ID.get(str(concept_id or ""))


def _parse_prompt_number_list(raw: str) -> list[int]:
    return [int(token.strip()) for token in raw.split(",") if token.strip()]


def _normalize_prompt_for_audit(prompt: str) -> str:
    return normalize_answer_text(prompt)


def _audit_generated_variant(seed: dict[str, Any], variant: QuestionDropVariant) -> str | None:
    generator_type = str(seed.get("generator_type") or "")
    answer_type = str(variant.answer_spec.get("type") or "").strip().casefold()
    if answer_type == "multiple_choice":
        choices = [str(choice).strip().casefold() for choice in variant.answer_spec.get("choices", []) if str(choice).strip()]
        if len(choices) != len(set(choices)):
            return "Multiple-choice options must be distinct."
    if variant.category == "math" and _BLAND_MATH_PROMPT_RE.fullmatch(variant.prompt):
        return "Math prompt slipped back into a bland worksheet shell."
    if variant.category == "logic" and _BLAND_LOGIC_PROMPT_RE.fullmatch(variant.prompt):
        return "Logic prompt slipped back into a stale shell."
    if generator_type == "math_percent_change":
        match = _PERCENT_CHANGE_PROMPT_RE.fullmatch(variant.prompt)
        if match is None:
            return "Percent-change prompt format is invalid."
        base_price = Decimal(match.group(1))
        percent = Decimal(match.group(2))
        expected = base_price * (Decimal("1") - (percent / Decimal("100")))
        actual = _decimal_from_numeric_value(variant.answer_spec.get("value"))
        if actual != expected:
            return f"Expected sale price {_format_decimal_value(expected)}, got {render_answer_summary(variant.answer_spec)}."
        return None

    if generator_type == "math_average_or_median":
        average_match = _AVERAGE_PROMPT_RE.fullmatch(variant.prompt)
        if average_match is not None:
            values = _parse_prompt_number_list(average_match.group(1))
            expected = Decimal(sum(values)) / Decimal(len(values))
        else:
            median_match = _MEDIAN_PROMPT_RE.fullmatch(variant.prompt)
            if median_match is None:
                return "Average-or-median prompt format is invalid."
            values = sorted(_parse_prompt_number_list(median_match.group(1)))
            expected = Decimal(values[len(values) // 2])
        actual = _decimal_from_numeric_value(variant.answer_spec.get("value"))
        if actual != expected:
            return f"Expected {_format_decimal_value(expected)}, got {render_answer_summary(variant.answer_spec)}."
        return None

    if generator_type == "math_divisibility":
        match = _DIVISIBILITY_PROMPT_RE.fullmatch(variant.prompt)
        if match is None:
            return "Divisibility prompt format is invalid."
        divisor = int(match.group(1))
        options = [int(match.group(index)) for index in range(2, 5)]
        divisible = [option for option in options if option % divisor == 0]
        if len(set(options)) != 3:
            return "Divisibility options must be distinct."
        if len(divisible) != 1:
            return "Divisibility prompt must have exactly one correct option."
        if str(divisible[0]) != str(variant.answer_spec.get("answer")):
            return "Divisibility answer spec does not match the only valid option."
        return None

    if generator_type == "language_anagram":
        match = _ANAGRAM_PROMPT_RE.fullmatch(variant.prompt)
        if match is None:
            return "Anagram prompt format is invalid."
        accepted = [str(item).strip().casefold() for item in variant.answer_spec.get("accepted", []) if isinstance(item, str)]
        if len(accepted) != 1:
            return "Anagram prompts must have a single intended answer."
        target = accepted[0]
        scrambled = match.group(2).casefold()
        if sorted(target) != sorted(scrambled):
            return "Anagram letters do not match the target answer."
        if target == scrambled:
            return "Anagram letters must be scrambled."
        if target in normalize_answer_text(variant.prompt):
            return "Anagram prompt leaks the answer."
        return None

    return None


def validate_content_pack(seeds: tuple[dict[str, Any], ...] | None = None) -> tuple[bool, str | None]:
    checked = seeds or QUESTION_DROP_SEEDS
    seen_ids: set[str] = set()
    for seed in checked:
        concept_id = seed.get("concept_id")
        if not isinstance(concept_id, str) or not concept_id.strip():
            return False, "Every Question Drops seed needs a concept_id."
        if concept_id in seen_ids:
            return False, f"Duplicate concept_id '{concept_id}'."
        seen_ids.add(concept_id)
        family_id = str(seed.get("family_id") or "").strip()
        if not family_id:
            return False, f"Seed '{concept_id}' needs a family_id."
        tags = tuple(seed.get("tags", ()))
        if not any(str(tag).startswith("sub:") for tag in tags):
            return False, f"Seed '{concept_id}' needs a subcategory tag."
        if not any(str(tag).startswith("mode:") for tag in tags):
            return False, f"Seed '{concept_id}' needs a reasoning-mode tag."
        if not any(str(tag).startswith("shape:") for tag in tags):
            return False, f"Seed '{concept_id}' needs an answer-shape tag."
        category = seed.get("category")
        if category not in QUESTION_DROP_CATEGORIES:
            return False, f"Seed '{concept_id}' uses unknown category '{category}'."
        difficulty = seed.get("difficulty")
        if difficulty not in {1, 2, 3}:
            return False, f"Seed '{concept_id}' has invalid difficulty '{difficulty}'."
        generator_type = seed.get("generator_type")
        if generator_type not in GENERATOR_HANDLERS:
            return False, f"Seed '{concept_id}' uses unknown generator '{generator_type}'."
        if generator_type == "static_pack":
            variants = seed.get("variants")
            if not isinstance(variants, (list, tuple)) or not variants:
                return False, f"Seed '{concept_id}' needs at least one static variant."
            prompt_audit: set[str] = set()
            for payload in variants:
                if not isinstance(payload, dict) or not isinstance(payload.get("prompt"), str) or not payload["prompt"].strip():
                    return False, f"Seed '{concept_id}' has an invalid prompt."
                prompt_key = _normalize_prompt_for_audit(payload["prompt"])
                if prompt_key in prompt_audit:
                    return False, f"Seed '{concept_id}' repeats a static prompt too closely."
                prompt_audit.add(prompt_key)
                valid, message = validate_answer_spec(payload.get("answer_spec", {}))
                if not valid:
                    return False, f"Seed '{concept_id}' has an invalid answer spec: {message}"
        else:
            prompt_audit: list[str] = []
            live_rotation_prompts: set[str] = set()
            for variant_index in range(QUESTION_DROP_VALIDATION_VARIANTS_PER_GENERATOR):
                try:
                    variant = build_variant(
                        seed,
                        seed_material=f"validation:{concept_id}",
                        variant_index=variant_index,
                    )
                except Exception as exc:
                    return False, f"Seed '{concept_id}' failed to build validation variant {variant_index}: {exc}"
                prompt_key = _normalize_prompt_for_audit(variant.prompt)
                prompt_audit.append(prompt_key)
                if variant_index < 12:
                    if prompt_key in live_rotation_prompts:
                        return False, f"Seed '{concept_id}' repeated a generated prompt inside the live rotation sample."
                    live_rotation_prompts.add(prompt_key)
                message = _audit_generated_variant(seed, variant)
                if message is not None:
                    return False, f"Seed '{concept_id}' failed generated audit at variant {variant_index}: {message}"
            if len(set(prompt_audit)) < 24:
                return False, f"Seed '{concept_id}' does not have enough prompt diversity across the 32-variant audit."
    return True, None


def build_variant(seed: dict[str, Any], *, seed_material: str, variant_index: int = 0) -> QuestionDropVariant:
    handler = GENERATOR_HANDLERS[seed["generator_type"]]
    variant = handler(seed, seed_material=seed_material, variant_index=variant_index)
    valid, message = validate_answer_spec(variant.answer_spec)
    if not valid:
        raise ValueError(message or f"Question Drops seed '{seed['concept_id']}' generated an invalid answer spec.")
    return variant


def iter_candidate_variants(
    *,
    categories: set[str] | None = None,
    seed_material: str,
    variants_per_seed: int = 6,
) -> list[QuestionDropVariant]:
    allowed_categories = categories or set(QUESTION_DROP_CATEGORIES)
    variants: list[QuestionDropVariant] = []
    for seed in QUESTION_DROP_SEEDS:
        if seed["category"] not in allowed_categories:
            continue
        if seed["generator_type"] == "static_pack":
            count = len(seed["variants"])
        else:
            count = max(1, int(variants_per_seed))
        for variant_index in range(count):
            variants.append(build_variant(seed, seed_material=seed_material, variant_index=variant_index))
    return variants
