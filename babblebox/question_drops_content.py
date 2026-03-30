from __future__ import annotations

import hashlib
import random
import re
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
_NUMBER_RE = re.compile(r"[-+]?\d+(?:\.\d+)?")


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
    text = _PUNCT_RE.sub(" ", text)
    text = _SPACE_RE.sub(" ", text)
    return text.strip()


def _normalize_token_sequence(raw: str | None) -> tuple[str, ...]:
    cleaned = normalize_answer_text(raw)
    if not cleaned:
        return ()
    return tuple(token for token in cleaned.split(" ") if token)


def extract_first_number(raw: str | None) -> float | None:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return None
    match = _NUMBER_RE.search(cleaned.replace(",", ""))
    if match is None:
        return None
    try:
        return float(match.group(0))
    except ValueError:
        return None


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
        if not isinstance(choices, list) or len(choices) < 2:
            return False, "Multiple-choice answers need at least two options."
        if not isinstance(answer, str) or normalize_answer_text(answer) == "":
            return False, "Multiple-choice answers need a correct option."
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
    answer_type = answer_spec.get("type")
    if answer_type == "text":
        candidate = normalize_answer_text(raw_answer)
        accepted = {normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)}
        return bool(candidate) and candidate in accepted
    if answer_type == "numeric":
        candidate = extract_first_number(raw_answer)
        if candidate is None:
            return False
        expected = float(answer_spec["value"])
        return abs(candidate - expected) < 1e-9
    if answer_type == "boolean":
        candidate = normalize_answer_text(raw_answer)
        if not candidate:
            return False
        if answer_spec.get("value") is True:
            return candidate in TRUE_ALIASES
        return candidate in FALSE_ALIASES
    if answer_type == "multiple_choice":
        candidate = normalize_answer_text(raw_answer)
        answer = normalize_answer_text(answer_spec.get("answer"))
        return bool(candidate) and candidate == answer
    if answer_type == "ordered_tokens":
        candidate_tokens = _normalize_token_sequence(raw_answer)
        expected_tokens = tuple(normalize_answer_text(token) for token in answer_spec.get("tokens", []))
        return bool(candidate_tokens) and candidate_tokens == expected_tokens
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
        return str(answer_spec.get("answer") or "unknown")
    if answer_type == "ordered_tokens":
        return " ".join(str(token) for token in answer_spec.get("tokens", [])) or "unknown"
    return "unknown"


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
    variants = seed["variants"]
    rng = _make_rng(f"{seed_material}:{seed['concept_id']}:{variant_index}")
    payload = variants[rng.randrange(len(variants))]
    variant_hash = build_variant_hash(seed["concept_id"], payload["prompt"], str(variant_index))
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
    prompt = f"What is {left} × {right}?"
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
        "concept_id": "language:oxford-comma-boolean",
        "category": "language",
        "difficulty": 2,
        "source_type": "curated",
        "generator_type": "static_pack",
        "variants": (
            {"prompt": "True or false: an Oxford comma appears before the final item in a list.", "answer_spec": {"type": "boolean", "value": True}},
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
    variants_per_seed: int = 2,
) -> list[QuestionDropVariant]:
    allowed_categories = categories or set(QUESTION_DROP_CATEGORIES)
    variants: list[QuestionDropVariant] = []
    for seed in QUESTION_DROP_SEEDS:
        if seed["category"] not in allowed_categories:
            continue
        for variant_index in range(max(1, int(variants_per_seed))):
            variants.append(build_variant(seed, seed_material=seed_material, variant_index=variant_index))
    return variants
