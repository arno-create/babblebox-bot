from __future__ import annotations

import hashlib
import random
import re
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable


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
_URL_RE = re.compile(r"https?://|\bdiscord\.gg/\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"<[@#]&?\d+>")
_NUMBER_WORD_RE = re.compile(r"[a-z]+", re.IGNORECASE)
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
    "nah",
    "no",
    "nope",
    "ok",
    "okay",
    "oh",
    "omg",
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
    "no",
    "oh",
    "omg",
    "real",
    "really",
    "same",
    "seriously",
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
    tags: tuple[str, ...] = ()
    attribution: str | None = None


def normalize_answer_text(raw: str | None) -> str:
    text = str(raw or "").casefold().strip()
    text = text.replace("’", "'")
    text = text.replace("“", '"').replace("”", '"')
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


def _strip_trailing_terminal_punctuation(raw: str | None) -> str:
    return str(raw or "").strip().rstrip(".!?").strip()


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


def _parse_clean_numeric_payload(raw: str | None) -> float | None:
    content = _strip_trailing_terminal_punctuation(raw)
    if not content:
        return None
    if _EXACT_NUMERIC_RE.fullmatch(content):
        try:
            return float(content.replace(",", ""))
        except ValueError:
            return None
    word_value = _parse_number_words_exact(content)
    if word_value is not None:
        return float(word_value)
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
    match = _CHOICE_LETTER_RE.match(str(raw_answer or ""))
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
    lead_match = _ANSWER_LEAD_RE.match(content)
    if lead_match is not None:
        payload = str(lead_match.group("payload") or "").strip()
        if payload and payload not in candidates:
            candidates.append(payload)
    return candidates


def _contains_attempt_noise(raw_answer: str) -> bool:
    return _URL_RE.search(raw_answer) is not None or _MENTION_RE.search(raw_answer) is not None


def _looks_like_free_text_guess(raw_answer: str | None, *, max_tokens: int) -> bool:
    raw = str(raw_answer or "").strip()
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
        if not isinstance(spec.get("value"), (int, float)):
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
        accepted = {normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)}
        return any(bool(normalize_answer_text(candidate)) and normalize_answer_text(candidate) in accepted for candidate in candidates)
    if answer_type == "numeric":
        expected = float(answer_spec["value"])
        return any(
            candidate is not None and abs(candidate - expected) < 1e-9
            for candidate in (_parse_clean_numeric_payload(item) for item in candidates)
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
        expected_tokens = tuple(normalize_answer_text(token) for token in answer_spec.get("tokens", []))
        return any((candidate_tokens := _normalize_token_sequence(candidate)) and candidate_tokens == expected_tokens for candidate in candidates)
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
        return any(_parse_clean_numeric_payload(candidate) is not None for candidate in candidates)
    if answer_type == "boolean":
        normalized_values = {normalize_answer_text(candidate) for candidate in candidates if normalize_answer_text(candidate)}
        return bool(normalized_values.intersection(TRUE_ALIASES | FALSE_ALIASES))
    if answer_type == "ordered_tokens":
        expected_tokens = tuple(normalize_answer_text(token) for token in answer_spec.get("tokens", []))
        if not expected_tokens:
            return False
        expected_counter = Counter(expected_tokens)
        for candidate in candidates:
            candidate_tokens = _normalize_token_sequence(candidate)
            if candidate_tokens and Counter(candidate_tokens) == expected_counter:
                return True
        return False
    if answer_type == "text":
        accepted = {normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)}
        max_alias_tokens = max((len(_normalize_token_sequence(item)) for item in accepted), default=1)
        standalone_max_tokens = max(3, max_alias_tokens + 1)
        for candidate in candidates:
            normalized = normalize_answer_text(candidate)
            if normalized and normalized in accepted:
                return True
        if len(candidates) > 1 and _looks_like_free_text_guess(candidates[-1], max_tokens=standalone_max_tokens + 1):
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
        value = answer_spec.get("value")
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        return str(value)
    if answer_type == "boolean":
        return "true" if answer_spec.get("value") else "false"
    if answer_type == "multiple_choice":
        letter = _correct_choice_letter(answer_spec)
        answer = str(answer_spec.get("answer") or "unknown")
        return f"{letter.upper()}) {answer}" if letter is not None else answer
    if answer_type == "ordered_tokens":
        return " ".join(str(token) for token in answer_spec.get("tokens", [])) or "unknown"
    return "unknown"


def render_answer_instruction(answer_spec: dict[str, Any]) -> str:
    answer_type = answer_spec.get("type")
    if answer_type == "multiple_choice":
        return "Reply here or send the option text. The correct letter also works, like `C` or `option c`."
    if answer_type == "numeric":
        return "Reply here or send just the number. Clean digits and simple number words both work."
    if answer_type == "boolean":
        return "Reply here or send `true` / `false` or `yes` / `no`."
    if answer_type == "ordered_tokens":
        return "Reply here or send the full sequence in order, like `red, blue, green`."
    return "Reply here or send a short clean answer."


def answer_points_for_difficulty(difficulty: int) -> int:
    return {1: 10, 2: 15, 3: 20}.get(int(difficulty), 10)


def content_seed_signature(seed: dict[str, Any]) -> str:
    parts = [
        str(seed.get("concept_id") or ""),
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


def _static_variant(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    variants = list(seed["variants"])
    rotation_rng = _make_rng(f"{seed_material}:{seed['concept_id']}:rotation")
    rotation = rotation_rng.randrange(len(variants)) if variants else 0
    payload_index = (rotation + variant_index) % len(variants)
    payload = variants[payload_index]
    variant_hash = build_variant_hash(seed["concept_id"], payload["prompt"], str(payload_index))
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
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


def _math_addition(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    left = rng.randint(12, 68)
    right = rng.randint(11, 59)
    prompt = f"What is {left} + {right}?"
    answer = left + right
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "numeric", "value": answer},
        variant_hash=build_variant_hash(seed["concept_id"], prompt, str(answer)),
        tags=tuple(seed.get("tags", ())),
    )


def _math_multiplication(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    left = rng.randint(6, 12)
    right = rng.randint(4, 9)
    prompt = f"What is {left} * {right}?"
    answer = left * right
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "numeric", "value": answer},
        variant_hash=build_variant_hash(seed["concept_id"], prompt, str(answer)),
        tags=tuple(seed.get("tags", ())),
    )


def _logic_sequence(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    kind = rng.choice(("arithmetic", "squares"))
    if kind == "arithmetic":
        start = rng.randint(2, 10)
        step = rng.randint(2, 6)
        values = [start + step * index for index in range(4)]
        answer = values[-1] + step
    else:
        start = rng.randint(2, 5)
        values = [value * value for value in range(start, start + 4)]
        answer = (start + 4) * (start + 4)
    prompt = "What number comes next? " + ", ".join(str(value) for value in values)
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "numeric", "value": answer},
        variant_hash=build_variant_hash(seed["concept_id"], prompt, str(answer)),
        tags=tuple(seed.get("tags", ())),
    )


def _language_anagram(seed: dict[str, Any], *, seed_material: str, variant_index: int) -> QuestionDropVariant:
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    payload = rng.choice(
        (
            ("listen", "silent"),
            ("rescue", "secure"),
            ("alter", "later"),
            ("earth", "heart"),
            ("stare", "tears"),
            ("thing", "night"),
            ("save", "vase"),
        )
    )
    prompt = f"Unscramble this word: **{payload[0]}**"
    return QuestionDropVariant(
        concept_id=seed["concept_id"],
        category=seed["category"],
        difficulty=seed["difficulty"],
        source_type=seed["source_type"],
        generator_type=seed["generator_type"],
        prompt=prompt,
        answer_spec={"type": "text", "accepted": [payload[1]]},
        variant_hash=build_variant_hash(seed["concept_id"], prompt, payload[1]),
        tags=tuple(seed.get("tags", ())),
    )


GENERATOR_HANDLERS: dict[str, Callable[[dict[str, Any]], QuestionDropVariant] | Callable[..., QuestionDropVariant]] = {
    "static_pack": _static_variant,
    "math_addition": _math_addition,
    "math_multiplication": _math_multiplication,
    "logic_sequence": _logic_sequence,
    "language_anagram": _language_anagram,
}


QUESTION_DROP_SEEDS: tuple[dict[str, Any], ...] = (
    {
        "concept_id": "science:planet-red",
        "category": "science",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "Which planet is known as the Red Planet?", "answer_spec": {"type": "text", "accepted": ["mars"]}},
            {"prompt": "Name the planet nicknamed the Red Planet.", "answer_spec": {"type": "text", "accepted": ["mars"]}},
        ),
    },
    {
        "concept_id": "science:water-chemical",
        "category": "science",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is the chemical formula for water?", "answer_spec": {"type": "text", "accepted": ["h2o"]}},
            {"prompt": "Write the chemical formula for water.", "answer_spec": {"type": "text", "accepted": ["h2o"]}},
        ),
    },
    {
        "concept_id": "science:plants-gas",
        "category": "science",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "Which gas do plants absorb during photosynthesis?", "answer_spec": {"type": "text", "accepted": ["carbon dioxide", "co2"]}},
            {"prompt": "Plants pull in which gas during photosynthesis?", "answer_spec": {"type": "text", "accepted": ["carbon dioxide", "co2"]}},
        ),
    },
    {
        "concept_id": "science:earth-satellite",
        "category": "science",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is Earth's natural satellite?", "answer_spec": {"type": "text", "accepted": ["moon", "the moon"]}},
            {"prompt": "Name the natural satellite that orbits Earth.", "answer_spec": {"type": "text", "accepted": ["moon", "the moon"]}},
        ),
    },
    {
        "concept_id": "history:moon-landing-year",
        "category": "history",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "In what year did humans first land on the Moon?", "answer_spec": {"type": "numeric", "value": 1969}},
            {"prompt": "Apollo 11 landed on the Moon in which year?", "answer_spec": {"type": "numeric", "value": 1969}},
        ),
    },
    {
        "concept_id": "history:berlin-wall-fall",
        "category": "history",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What year did the Berlin Wall fall?", "answer_spec": {"type": "numeric", "value": 1989}},
            {"prompt": "In which year was the Berlin Wall opened and effectively brought down?", "answer_spec": {"type": "numeric", "value": 1989}},
        ),
    },
    {
        "concept_id": "history:first-us-president",
        "category": "history",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "Who was the first president of the United States?", "answer_spec": {"type": "text", "accepted": ["george washington", "washington"]}},
            {"prompt": "Name the first U.S. president.", "answer_spec": {"type": "text", "accepted": ["george washington", "washington"]}},
        ),
    },
    {
        "concept_id": "history:titanic",
        "category": "history",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What was the name of the ship that hit an iceberg and sank in 1912?", "answer_spec": {"type": "text", "accepted": ["titanic", "the titanic"]}},
            {"prompt": "Name the famous ocean liner that sank after striking an iceberg in 1912.", "answer_spec": {"type": "text", "accepted": ["titanic", "the titanic"]}},
        ),
    },
    {
        "concept_id": "geography:nile-continent",
        "category": "geography",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "The Nile River is on which continent?", "answer_spec": {"type": "text", "accepted": ["africa"]}},
            {"prompt": "Which continent is home to the Nile?", "answer_spec": {"type": "text", "accepted": ["africa"]}},
        ),
    },
    {
        "concept_id": "geography:japan-capital",
        "category": "geography",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is the capital city of Japan?", "answer_spec": {"type": "text", "accepted": ["tokyo"]}},
            {"prompt": "Name Japan's capital.", "answer_spec": {"type": "text", "accepted": ["tokyo"]}},
        ),
    },
    {
        "concept_id": "geography:largest-ocean",
        "category": "geography",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is the largest ocean on Earth?", "answer_spec": {"type": "text", "accepted": ["pacific", "pacific ocean"]}},
            {"prompt": "Name Earth's largest ocean.", "answer_spec": {"type": "text", "accepted": ["pacific", "pacific ocean"]}},
        ),
    },
    {
        "concept_id": "geography:sahara",
        "category": "geography",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is the largest hot desert on Earth?", "answer_spec": {"type": "text", "accepted": ["sahara", "sahara desert", "the sahara"]}},
            {"prompt": "Name Earth's largest hot desert.", "answer_spec": {"type": "text", "accepted": ["sahara", "sahara desert", "the sahara"]}},
        ),
    },
    {
        "concept_id": "language:oxford-comma-boolean",
        "category": "language",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "True or false: an Oxford comma appears before the final item in a list.", "answer_spec": {"type": "boolean", "value": True}},
            {"prompt": "True or false: the Oxford comma is the comma placed before the last item in a list of three or more.", "answer_spec": {"type": "boolean", "value": True}},
        ),
    },
    {
        "concept_id": "language:plural-cactus",
        "category": "language",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "What is the plural of 'cactus' in common English usage?", "answer_spec": {"type": "text", "accepted": ["cacti", "cactuses"]}},
            {"prompt": "Give a common English plural for 'cactus'.", "answer_spec": {"type": "text", "accepted": ["cacti", "cactuses"]}},
        ),
    },
    {
        "concept_id": "logic:sequence",
        "category": "logic",
        "difficulty": 2,
        "source_type": "generated",
        "generator_type": "logic_sequence",
        "variants": (),
    },
    {
        "concept_id": "logic:traffic-light",
        "category": "logic",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {
                "prompt": "Multiple choice: which color usually means 'go' on a traffic light? A) red B) yellow C) green",
                "answer_spec": {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"},
            },
            {
                "prompt": "Which traffic-light option usually means 'go'? A) red B) yellow C) green",
                "answer_spec": {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"},
            },
        ),
    },
    {
        "concept_id": "math:addition",
        "category": "math",
        "difficulty": 1,
        "source_type": "generated",
        "generator_type": "math_addition",
        "variants": (),
    },
    {
        "concept_id": "math:multiplication",
        "category": "math",
        "difficulty": 2,
        "source_type": "generated",
        "generator_type": "math_multiplication",
        "variants": (),
    },
    {
        "concept_id": "culture:chess-piece",
        "category": "culture",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "Which chess piece can move in an L shape?", "answer_spec": {"type": "text", "accepted": ["knight", "the knight"]}},
            {"prompt": "Name the chess piece that moves in an L shape.", "answer_spec": {"type": "text", "accepted": ["knight", "the knight"]}},
        ),
    },
    {
        "concept_id": "culture:primary-colors",
        "category": "culture",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {
                "prompt": "Put these additive primary colors in order from shortest answer length to longest: blue, red, green",
                "answer_spec": {"type": "ordered_tokens", "tokens": ["red", "blue", "green"]},
            },
            {
                "prompt": "Order these additive primary colors from the shortest word to the longest: blue, red, green",
                "answer_spec": {"type": "ordered_tokens", "tokens": ["red", "blue", "green"]},
            },
        ),
    },
    {
        "concept_id": "culture:piano-keys",
        "category": "culture",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "How many keys does a standard piano have?", "answer_spec": {"type": "numeric", "value": 88}},
            {"prompt": "A standard piano has how many keys?", "answer_spec": {"type": "numeric", "value": 88}},
        ),
    },
    {
        "concept_id": "culture:monopoly",
        "category": "culture",
        "difficulty": 1,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "Which board game is built around properties, railroads, and hotels?", "answer_spec": {"type": "text", "accepted": ["monopoly"]}},
            {"prompt": "Name the board game where players buy properties and build hotels.", "answer_spec": {"type": "text", "accepted": ["monopoly"]}},
        ),
    },
    {
        "concept_id": "language:anagram",
        "category": "language",
        "difficulty": 2,
        "source_type": "generated",
        "generator_type": "language_anagram",
        "variants": (),
    },
)

QUESTION_DROP_SEED_BY_CONCEPT_ID = {seed["concept_id"]: seed for seed in QUESTION_DROP_SEEDS}


def question_drop_seed_for_concept(concept_id: str | None) -> dict[str, Any] | None:
    return QUESTION_DROP_SEED_BY_CONCEPT_ID.get(str(concept_id or ""))


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
            for payload in variants:
                if not isinstance(payload, dict) or not isinstance(payload.get("prompt"), str) or not payload["prompt"].strip():
                    return False, f"Seed '{concept_id}' has an invalid prompt."
                valid, message = validate_answer_spec(payload.get("answer_spec", {}))
                if not valid:
                    return False, f"Seed '{concept_id}' has an invalid answer spec: {message}"
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
