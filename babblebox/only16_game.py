from __future__ import annotations

import asyncio
import contextlib
import re
from dataclasses import dataclass
from datetime import timedelta
from fractions import Fraction
from typing import Any

import discord

from babblebox import game_engine as ge


ONLY16_MODE_LABELS = {
    "strict": "Strict",
    "smart": "Smart",
}
ONLY16_ASK_WINDOW_SECONDS = 25
ONLY16_TRAP_WINDOW_SECONDS = 12
ONLY16_MAX_EXPRESSION_LENGTH = 48
ONLY16_MAX_EXPRESSION_TOKENS = 24
ONLY16_MAX_EXPRESSION_DEPTH = 6

_QUESTION_START_RE = re.compile(r"^\s*(how|what|which|who)\b", re.IGNORECASE)
_QUANTITY_HINT_RE = re.compile(
    r"\b("
    r"how many|how much|what number|which number|how old|how long|how tall|how far|"
    r"how heavy|how wide|how often|what time|how large|how big|how deep|how fast|how slow|"
    r"how high|how low|how strong|how rare|how common|what age|what score|what year"
    r")\b",
    re.IGNORECASE,
)
_ARITHMETIC_QUESTION_RE = re.compile(
    r"^\s*(?:what(?:'s| is)|how much is|calculate|solve)\s+(?P<expr>.+?)\s*\??\s*$",
    re.IGNORECASE,
)
_ANSWER_LEAD_RE = re.compile(
    r"^\s*(?:it(?:'s| is)?|answer(?: is|'s)?|my guess is|i(?:'d| would)? say|i think|maybe|probably|there (?:is|are)|just)\s+(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)
_SMART_STANDALONE_LEAD_RE = re.compile(
    r"^\s*(?:it(?:'s| is)|answer(?::|\s+is|'s)|my guess is)\s+(?P<payload>.+?)\s*$",
    re.IGNORECASE,
)
_STANDALONE_TRAILING_PUNCTUATION_RE = re.compile(r"^(?P<body>.+?)(?P<punct>[.!]+)$")
_DIGIT_RE = re.compile(r"(?<![\w/])([+-]?\d+)(?![\w/])")
_INTEGER_FULL_RE = re.compile(r"^[+-]?\d+$")
_MATH_TEXT_RE = re.compile(r"^[\d\s()+\-*/^]+$")
_WORD_TOKEN_RE = re.compile(r"[a-z]+", re.IGNORECASE)

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
class ExplicitNumericPayload:
    start: int
    end: int
    value: Fraction
    raw: str
    source: str


@dataclass(frozen=True)
class Only16ParseResult:
    kind: str
    value: Fraction | None = None
    raw: str | None = None
    source: str | None = None

    @property
    def is_single(self) -> bool:
        return self.kind == "single" and self.value is not None


class _SafeArithmeticParser:
    def __init__(self, tokens: list[str]):
        self.tokens = tokens
        self.index = 0
        self.depth = 0

    def parse(self) -> Fraction:
        value = self._parse_additive()
        if self.index != len(self.tokens):
            raise ValueError("unexpected trailing tokens")
        return value

    def _peek(self) -> str | None:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def _consume(self) -> str:
        token = self.tokens[self.index]
        self.index += 1
        return token

    def _parse_additive(self) -> Fraction:
        value = self._parse_multiplicative()
        while self._peek() in {"+", "-"}:
            operator = self._consume()
            right = self._parse_multiplicative()
            value = value + right if operator == "+" else value - right
        return value

    def _parse_multiplicative(self) -> Fraction:
        value = self._parse_power()
        while self._peek() in {"*", "/"}:
            operator = self._consume()
            right = self._parse_power()
            if operator == "*":
                value *= right
            else:
                if right == 0:
                    raise ValueError("division by zero")
                value /= right
        return value

    def _parse_power(self) -> Fraction:
        value = self._parse_unary()
        if self._peek() == "^":
            self._consume()
            exponent = self._parse_power()
            if exponent.denominator != 1:
                raise ValueError("fractional exponent")
            exponent_value = exponent.numerator
            if exponent_value < 0 or exponent_value > 6:
                raise ValueError("unsupported exponent")
            value = value**exponent_value
        return value

    def _parse_unary(self) -> Fraction:
        if self._peek() in {"+", "-"}:
            operator = self._consume()
            value = self._parse_unary()
            return value if operator == "+" else -value
        return self._parse_primary()

    def _parse_primary(self) -> Fraction:
        token = self._peek()
        if token is None:
            raise ValueError("unexpected end")
        if token == "(":
            self._consume()
            self.depth += 1
            if self.depth > ONLY16_MAX_EXPRESSION_DEPTH:
                raise ValueError("expression too deep")
            value = self._parse_additive()
            if self._peek() != ")":
                raise ValueError("missing closing parenthesis")
            self._consume()
            self.depth -= 1
            return value
        if token == ")":
            raise ValueError("unexpected closing parenthesis")
        self._consume()
        return Fraction(int(token))


def only16_mode_label(mode: str | None) -> str:
    return ONLY16_MODE_LABELS.get(str(mode or "").casefold(), "Strict")


def ensure_only16_state(game: dict[str, Any]) -> dict[str, Any]:
    state = game.setdefault("only16", {})
    state.setdefault("mode", game.get("only16_mode", "strict"))
    state.setdefault("trap", None)
    state.setdefault("ask_started_at", None)
    return state


def has_question_intent(text: str | None) -> bool:
    content = str(text or "").strip()
    if not content:
        return False
    return "?" in content or _QUESTION_START_RE.search(content) is not None


def has_quantity_intent(text: str | None) -> bool:
    return _QUANTITY_HINT_RE.search(str(text or "")) is not None


def detect_numeric_question(text: str | None) -> bool:
    content = str(text or "").strip()
    if not content or not has_question_intent(content):
        return False
    if has_quantity_intent(content):
        return True
    arithmetic_match = _ARITHMETIC_QUESTION_RE.match(content)
    if arithmetic_match is None:
        return False
    parsed = _parse_exact_numeric_payload(arithmetic_match.group("expr"))
    return parsed.kind == "single" and parsed.source == "expression"


def detect_count_question(text: str | None) -> bool:
    return detect_numeric_question(text)


def extract_reply_target_id(message: discord.Message) -> int | None:
    reference = getattr(message, "reference", None)
    message_id = getattr(reference, "message_id", None)
    if isinstance(message_id, int):
        return message_id
    resolved = getattr(reference, "resolved", None)
    cached = getattr(reference, "cached_message", None)
    for candidate in (resolved, cached):
        candidate_id = getattr(candidate, "id", None)
        if isinstance(candidate_id, int):
            return candidate_id
    return None


def parse_only16_numeric_answer(text: str | None) -> Only16ParseResult:
    content = str(text or "").strip()
    if not content:
        return Only16ParseResult("none")
    for candidate in _exact_payload_candidates(content):
        parsed = _parse_exact_numeric_payload(candidate)
        if parsed.kind != "none":
            return parsed
    payloads = _find_digit_payloads(content) + _find_number_word_payloads(content)
    if not payloads:
        return Only16ParseResult("none")
    values = {payload.value for payload in payloads}
    if len(values) == 1:
        first = min(payloads, key=lambda payload: payload.start)
        return Only16ParseResult("single", value=first.value, raw=first.raw, source=first.source)
    return Only16ParseResult("ambiguous")


def _exact_payload_candidates(content: str) -> list[str]:
    candidates = [content]
    lead_match = _ANSWER_LEAD_RE.match(content)
    if lead_match is not None:
        payload = lead_match.group("payload").strip()
        if payload:
            candidates.append(payload)
    return candidates


def _parse_exact_numeric_payload(text: str | None) -> Only16ParseResult:
    content = str(text or "").strip()
    if not content:
        return Only16ParseResult("none")
    if _INTEGER_FULL_RE.fullmatch(content):
        return Only16ParseResult("single", value=Fraction(int(content)), raw=content, source="digits")
    if _looks_like_math_payload(content):
        expression_result = _parse_safe_arithmetic_expression(content)
        if expression_result is not None:
            return expression_result
    word_value = _parse_number_words_exact(content)
    if word_value is not None:
        return Only16ParseResult("single", value=Fraction(word_value), raw=content, source="words")
    return Only16ParseResult("none")


def _looks_like_math_payload(content: str) -> bool:
    return bool(content) and _MATH_TEXT_RE.fullmatch(content) is not None and any(operator in content for operator in "+-*/^()")


def _parse_safe_arithmetic_expression(content: str) -> Only16ParseResult:
    stripped = str(content or "").strip()
    if len(stripped) > ONLY16_MAX_EXPRESSION_LENGTH:
        return Only16ParseResult("unsupported", raw=stripped, source="expression")
    try:
        tokens = _tokenize_arithmetic_expression(stripped)
        parser = _SafeArithmeticParser(tokens)
        value = parser.parse()
    except ValueError:
        return Only16ParseResult("unsupported", raw=stripped, source="expression")
    return Only16ParseResult("single", value=value, raw=stripped, source="expression")


def _tokenize_arithmetic_expression(content: str) -> list[str]:
    tokens: list[str] = []
    current = []
    for char in content:
        if char.isspace():
            if current:
                tokens.append("".join(current))
                current = []
            continue
        if char.isdigit():
            current.append(char)
            continue
        if current:
            tokens.append("".join(current))
            current = []
        if char not in "+-*/^()":
            raise ValueError("unsupported character")
        tokens.append(char)
    if current:
        tokens.append("".join(current))
    if not tokens or len(tokens) > ONLY16_MAX_EXPRESSION_TOKENS:
        raise ValueError("too many tokens")
    if all(token.isdigit() for token in tokens):
        raise ValueError("not an expression")
    return tokens


def _find_digit_payloads(content: str) -> list[ExplicitNumericPayload]:
    payloads: list[ExplicitNumericPayload] = []
    for match in _DIGIT_RE.finditer(content):
        payloads.append(
            ExplicitNumericPayload(
                start=match.start(),
                end=match.end(),
                value=Fraction(int(match.group(1))),
                raw=match.group(1),
                source="digits",
            )
        )
    return payloads


def _find_number_word_payloads(content: str) -> list[ExplicitNumericPayload]:
    matches = list(_WORD_TOKEN_RE.finditer(content))
    if not matches:
        return []
    payloads: list[ExplicitNumericPayload] = []
    index = 0
    while index < len(matches):
        best_payload: ExplicitNumericPayload | None = None
        max_end = min(len(matches), index + 5)
        for end_index in range(max_end, index, -1):
            candidate_matches = matches[index:end_index]
            if not _tokens_form_number_phrase(content, candidate_matches):
                continue
            words = [match.group(0).casefold() for match in candidate_matches]
            value = _parse_number_word_tokens(words)
            if value is None:
                continue
            raw = content[candidate_matches[0].start() : candidate_matches[-1].end()]
            best_payload = ExplicitNumericPayload(
                start=candidate_matches[0].start(),
                end=candidate_matches[-1].end(),
                value=Fraction(value),
                raw=raw,
                source="words",
            )
            break
        if best_payload is not None:
            payloads.append(best_payload)
            index += len(_WORD_TOKEN_RE.findall(best_payload.raw))
            continue
        index += 1
    return payloads


def _tokens_form_number_phrase(content: str, matches: list[re.Match[str]]) -> bool:
    if not matches:
        return False
    first_start = matches[0].start()
    last_end = matches[-1].end()
    if first_start > 0 and content[first_start - 1] == "-":
        return False
    if last_end < len(content) and content[last_end] == "-":
        return False
    previous_end = matches[0].start()
    for match in matches:
        word = match.group(0).casefold()
        if word not in _NUMBER_WORD_VOCAB:
            return False
        separator = content[previous_end : match.start()]
        if separator and not re.fullmatch(r"[-\s]+", separator):
            return False
        previous_end = match.end()
    return True


def _parse_number_words_exact(content: str) -> int | None:
    matches = list(_WORD_TOKEN_RE.finditer(content))
    if not matches:
        return None
    if content[: matches[0].start()].strip() or content[matches[-1].end() :].strip():
        return None
    if not _tokens_form_number_phrase(content, matches):
        return None
    return _parse_number_word_tokens([match.group(0).casefold() for match in matches])


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


def _trap_is_live(trap: dict[str, Any] | None) -> bool:
    if not isinstance(trap, dict):
        return False
    expires_at = trap.get("expires_at")
    if expires_at is None or not hasattr(expires_at, "tzinfo"):
        return False
    return expires_at > ge.now_utc()


def _only16_player_names(game: dict[str, Any]) -> str:
    return ge.join_limited_lines([f"**{index + 1}.** {player.display_name}" for index, player in enumerate(game.get("players", []))])


def _only16_mode_guide() -> str:
    return (
        "Strict: direct replies to the armed question only.\n"
        "Smart: direct replies plus one clean standalone answer like `16!` or `sixteen.`.\n"
        "Chatter stays out, and ambiguity never eliminates."
    )


def _only16_supported_math_copy() -> str:
    return "Safe math: integers with `+ - * / ^`, unary `+/-`, and parentheses."


def _smart_exact_payload(text: str | None) -> Only16ParseResult:
    content = str(text or "").strip()
    if not content:
        return Only16ParseResult("none")
    candidates = [content]
    punctuated_match = _STANDALONE_TRAILING_PUNCTUATION_RE.match(content)
    if punctuated_match is not None:
        trimmed = str(punctuated_match.group("body") or "").strip()
        if trimmed and trimmed not in candidates:
            candidates.append(trimmed)
    for candidate in candidates:
        parsed = _parse_exact_numeric_payload(candidate)
        if parsed.kind != "none":
            return parsed
    return Only16ParseResult("none")


def _format_fraction(value: Fraction | int) -> str:
    fraction = value if isinstance(value, Fraction) else Fraction(int(value))
    if fraction.denominator == 1:
        return str(fraction.numerator)
    return f"{fraction.numerator}/{fraction.denominator}"


def _classify_smart_follow_up(text: str | None, parsed: Only16ParseResult) -> tuple[str, Only16ParseResult]:
    content = str(text or "").strip()
    if not content or len(content) > 60:
        return "ignore", parsed
    if "?" in content or has_question_intent(content):
        return "ignore", parsed
    exact = _smart_exact_payload(content)
    if exact.kind == "single":
        return "judge", exact
    if exact.kind == "unsupported":
        return "void", exact
    lead_match = _SMART_STANDALONE_LEAD_RE.match(content)
    if lead_match is None:
        return "ignore", parsed
    payload = str(lead_match.group("payload") or "").strip()
    if not payload:
        return "ignore", parsed
    payload_exact = _smart_exact_payload(payload)
    if payload_exact.kind == "single":
        return "judge", payload_exact
    if payload_exact.kind == "unsupported":
        return "void", payload_exact
    payload_parsed = parse_only16_numeric_answer(payload)
    if payload_parsed.kind == "ambiguous":
        return "void", payload_parsed
    if parsed.kind == "unsupported":
        return "void", parsed
    return "ignore", parsed


def _trap_live_copy(mode: str | None) -> str:
    normalized = str(mode or "strict").casefold()
    if normalized == "smart":
        return "Trap live. Smart mode counts direct replies plus one clean standalone answer. Chatter stays out."
    return "Trap live. Strict mode counts direct replies to the armed question only. Chatter stays out."


def _message_created_at_utc(message: discord.Message) -> Any:
    created_at = getattr(message, "created_at", None)
    if created_at is None or not hasattr(created_at, "tzinfo"):
        return None
    return created_at.astimezone(ge.now_utc().tzinfo) if created_at.tzinfo else created_at.replace(tzinfo=ge.now_utc().tzinfo)


async def start_only16_game_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_only16_state(game)
    state["trap"] = None
    intro = discord.Embed(
        title="Only 16",
        description=(
            f"Mode: **{only16_mode_label(state.get('mode'))}**\n"
            "Ask one clean number question. Babblebox only judges explicit answers: digits, clear number words, or safe math like `8*2`, `17-1`, or `(10+6)`."
        ),
        color=discord.Color.orange(),
    )
    intro.add_field(name="Judging", value=f"Current mode: **{only16_mode_label(state.get('mode'))}**\n{_only16_mode_guide()}", inline=False)
    intro.add_field(name="Safe Math", value=_only16_supported_math_copy(), inline=False)
    intro.add_field(name="Live Players", value=_only16_player_names(game), inline=False)
    intro = ge.style_embed(intro, footer="Babblebox Only 16 | Ambiguity never eliminates")
    await game["channel"].send(embed=intro)
    ge.mark_game_started(game)
    await _start_only16_turn_locked(guild_id, game)


async def handle_only16_message_locked(message: discord.Message, guild_id: int, game: dict[str, Any]) -> bool:
    state = ensure_only16_state(game)
    if message.channel.id != game["channel"].id:
        return False
    if not ge.is_player_in_game(game, message.author.id):
        return False

    trap = state.get("trap")
    current_asker = ge.get_current_player(game)
    if current_asker is not None and current_asker.id == message.author.id and not _trap_is_live(trap):
        if detect_numeric_question(message.content):
            await _arm_only16_trap_locked(guild_id, game, message, manual=False)
            return True
        return False

    if not _trap_is_live(trap):
        return False
    if message.author.id == trap["asker_id"]:
        return False
    return await _handle_only16_answer_locked(message, guild_id, game, trap)


async def handle_only16_message_delete_locked(message_id: int, guild_id: int, game: dict[str, Any]) -> bool:
    state = ensure_only16_state(game)
    trap = state.get("trap")
    if not _trap_is_live(trap):
        return False
    if int(trap.get("question_message_id", 0) or 0) != int(message_id):
        return False
    await _consume_only16_trap_locked(guild_id, game, reason="That armed question vanished, so Babblebox voided the trap.")
    return True


async def manually_arm_only16_message(message: discord.Message, guild_id: int, game: dict[str, Any], actor: discord.abc.User) -> tuple[bool, str]:
    state = ensure_only16_state(game)
    current_asker = ge.get_current_player(game)
    if current_asker is None or actor.id != current_asker.id or message.author.id != actor.id:
        return False, "Only the current asker can arm their own trap."
    if message.channel.id != game["channel"].id:
        return False, "That message is not in the live Only 16 channel."
    if _trap_is_live(state.get("trap")):
        return False, "A trap is already armed."
    if not detect_numeric_question(message.content):
        return False, "Manual arming still needs a clear number question."
    ask_started_at = state.get("ask_started_at")
    created_at = _message_created_at_utc(message)
    if ask_started_at is None or created_at is None or created_at < ask_started_at:
        return False, "You can only arm a question from your current ask window."
    await _arm_only16_trap_locked(guild_id, game, message, manual=True)
    return True, "Trap armed. Babblebox is watching for 16."


async def _arm_only16_trap_locked(guild_id: int, game: dict[str, Any], message: discord.Message, *, manual: bool):
    state = ensure_only16_state(game)
    await ge.cancel_task(game.get("turn_task"))
    trap = {
        "asker_id": message.author.id,
        "question_message_id": message.id,
        "armed_at": ge.now_utc(),
        "expires_at": ge.now_utc() + timedelta(seconds=ONLY16_TRAP_WINDOW_SECONDS),
        "mode": state.get("mode", "strict"),
        "manual": manual,
    }
    state["trap"] = trap
    with contextlib.suppress(discord.HTTPException):
        await message.add_reaction("🎯")
    with contextlib.suppress(discord.HTTPException):
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Trap Live",
                (
                    f"{message.author.mention} armed the trap. {_trap_live_copy(trap.get('mode'))}"
                    if manual
                    else _trap_live_copy(trap.get("mode"))
                ),
                tone="accent",
                footer="Babblebox Only 16",
            ),
            delete_after=6.0,
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
    token = ge.bump_token(game, "turn_token")
    game["turn_task"] = asyncio.create_task(_only16_trap_timeout(guild_id, token), name=f"babblebox-only16-trap-{guild_id}")


async def _handle_only16_answer_locked(message: discord.Message, guild_id: int, game: dict[str, Any], trap: dict[str, Any]) -> bool:
    reply_target_id = extract_reply_target_id(message)
    mode = str(trap.get("mode", "strict")).casefold()
    is_reply = reply_target_id == trap["question_message_id"]
    parsed = parse_only16_numeric_answer(message.content)

    if is_reply:
        return await _resolve_only16_answer_locked(message, guild_id, game, trap, parsed)

    if mode != "smart":
        return False
    smart_outcome, smart_parsed = _classify_smart_follow_up(message.content, parsed)
    if smart_outcome == "ignore":
        return False
    if smart_outcome == "judge":
        parsed = smart_parsed
    if smart_outcome == "void":
        if smart_parsed.kind in {"ambiguous", "unsupported"}:
            return await _resolve_only16_answer_locked(message, guild_id, game, trap, smart_parsed)
        await _consume_only16_trap_locked(
            guild_id,
            game,
            title="Trap Voided",
            reason="Smart mode only judges one clean standalone answer, so Babblebox skipped that fuzzy non-reply.",
        )
        return True
    return await _resolve_only16_answer_locked(message, guild_id, game, trap, parsed)


async def _resolve_only16_answer_locked(
    message: discord.Message,
    guild_id: int,
    game: dict[str, Any],
    trap: dict[str, Any],
    parsed: Only16ParseResult,
) -> bool:
    await ge.cancel_task(game.get("turn_task"))
    state = ensure_only16_state(game)
    state["trap"] = None
    asker_id = trap["asker_id"]

    if parsed.kind == "none":
        body = f"{message.author.mention} never gave one clear explicit number, so the trap passes harmlessly."
        await game["channel"].send(
            embed=ge.make_status_embed("Safe Pass", body, tone="info", footer="Babblebox Only 16"),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True
    if parsed.kind == "ambiguous":
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Trap Voided",
                f"{message.author.mention} gave more than one explicit number, so Babblebox voided the trap on fairness.",
                tone="info",
                footer="Babblebox Only 16",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True
    if parsed.kind == "unsupported":
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Trap Voided",
                (
                    f"{message.author.mention} used math outside the safe judge grammar, so Babblebox would not auto-eliminate on it.\n"
                    f"{_only16_supported_math_copy()}"
                ),
                tone="info",
                footer="Babblebox Only 16",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True

    assert parsed.value is not None
    numeric_value = parsed.value
    rendered_value = _format_fraction(numeric_value)
    if numeric_value == 16:
        body = (
            f"{message.author.mention}'s math lands on **16**, so they slip through."
            if parsed.source == "expression"
            else f"{message.author.mention} lands on **16** and slips through."
        )
        await game["channel"].send(
            embed=ge.make_status_embed("Still Alive", body, tone="success", footer="Babblebox Only 16"),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True

    eliminated = ge.get_player_by_id(game, message.author.id)
    if eliminated is not None:
        game["players"] = [player for player in game["players"] if player.id != message.author.id]
    body = (
        f"{message.author.mention}'s math lands on **{rendered_value}**, not **16**, and they are out."
        if parsed.source == "expression"
        else f"{message.author.mention} lands on **{rendered_value}**, not **16**, and is out."
    )
    await game["channel"].send(
        embed=ge.make_status_embed("Eliminated", body, tone="danger", footer="Babblebox Only 16"),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    if len(game["players"]) <= 1:
        await _finish_only16_locked(guild_id, game)
        return True
    if asker_id not in {player.id for player in game["players"]}:
        game["current_player_index"] = 0
    await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
    return True


async def _consume_only16_trap_locked(guild_id: int, game: dict[str, Any], *, reason: str, title: str = "Trap Voided"):
    await ge.cancel_task(game.get("turn_task"))
    state = ensure_only16_state(game)
    trap = state.get("trap")
    asker_id = trap.get("asker_id") if isinstance(trap, dict) else None
    state["trap"] = None
    await game["channel"].send(
        embed=ge.make_status_embed(
            title,
            reason,
            tone="info",
            footer="Babblebox Only 16",
        )
    )
    await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)


async def _advance_to_next_asker_locked(guild_id: int, game: dict[str, Any], *, asker_id: int | None):
    if not game.get("players"):
        await ge.cleanup_game(guild_id)
        return
    asker_index = next((index for index, player in enumerate(game["players"]) if player.id == asker_id), None)
    if asker_index is None:
        game["current_player_index"] = 0
    else:
        game["current_player_index"] = (asker_index + 1) % len(game["players"])
    await _start_only16_turn_locked(guild_id, game)


async def _start_only16_turn_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_only16_state(game)
    if len(game.get("players", [])) <= 1:
        await _finish_only16_locked(guild_id, game)
        return
    current_asker = ge.get_current_player(game)
    if current_asker is None:
        game["current_player_index"] = 0
        current_asker = ge.get_current_player(game)
    state["trap"] = None
    state["ask_started_at"] = ge.now_utc()
    token = ge.bump_token(game, "turn_token")
    await ge.cancel_task(game.get("turn_task"))
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Your Trap Turn",
            (
                f"{current_asker.mention}, ask one clean number question in the next **{ONLY16_ASK_WINDOW_SECONDS} seconds**.\n"
                f"Mode: **{only16_mode_label(state.get('mode'))}**\n"
                f"{_only16_mode_guide()}"
            ),
            tone="accent",
            footer="Babblebox Only 16 | Explicit numbers only",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    game["turn_task"] = asyncio.create_task(_only16_ask_timeout(guild_id, token), name=f"babblebox-only16-ask-{guild_id}")
    ge.reset_idle_timer(guild_id)


async def _finish_only16_locked(guild_id: int, game: dict[str, Any]):
    if not game.get("players"):
        await ge.cleanup_game(guild_id)
        return
    winner = game["players"][0]
    stats = ge.get_player_stats(winner)
    stats["wins"] += 1
    stats["only16_wins"] = int(stats.get("only16_wins", 0) or 0) + 1
    ge.schedule_profile_update("record_only16_win", winner.id)
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Winner",
            f"{winner.mention} is the last player standing and takes **Only 16**.",
            tone="success",
            footer="Babblebox Only 16",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    await ge.cleanup_game(guild_id)


async def _only16_ask_timeout(guild_id: int, token: int):
    await asyncio.sleep(ONLY16_ASK_WINDOW_SECONDS)
    game = ge.games.get(guild_id)
    if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
        return
    async with game["lock"]:
        game = ge.games.get(guild_id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
            return
        if game.get("turn_token") != token:
            return
        current_asker = ge.get_current_player(game)
        if current_asker is None:
            return
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Turn Skipped",
                f"{current_asker.mention} did not arm a clean number question in time, so the turn moves on.",
                tone="info",
                footer="Babblebox Only 16",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=current_asker.id)


async def _only16_trap_timeout(guild_id: int, token: int):
    await asyncio.sleep(ONLY16_TRAP_WINDOW_SECONDS)
    game = ge.games.get(guild_id)
    if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
        return
    async with game["lock"]:
        game = ge.games.get(guild_id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "only16":
            return
        if game.get("turn_token") != token:
            return
        state = ensure_only16_state(game)
        trap = state.get("trap")
        if not _trap_is_live(trap):
            return
        await _consume_only16_trap_locked(
            guild_id,
            game,
            title="Trap Window Closed",
            reason="Nobody bit before the trap window closed.",
        )
