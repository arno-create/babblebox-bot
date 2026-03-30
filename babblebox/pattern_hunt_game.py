from __future__ import annotations

import asyncio
import contextlib
import random
import re
from dataclasses import dataclass
from typing import Any

import discord

from babblebox import game_engine as ge


PATTERN_HUNT_GUESS_LIMIT = 3
PATTERN_HUNT_STRIKE_LIMIT = 3
PATTERN_HUNT_PROMPT_TIMEOUT_SECONDS = 35
PATTERN_HUNT_ANSWER_TIMEOUT_SECONDS = 30
PATTERN_HUNT_RULE_FAMILIES = (
    "starts_with_letter",
    "ends_with_punctuation",
    "contains_number",
    "contains_emoji",
    "contains_category_word",
    "forbid_letter",
    "exact_word_count",
    "word_count_range",
    "question_form",
    "same_initial_letter",
    "exact_punctuation_count",
    "char_length_range",
)

_WORD_RE = re.compile(r"[a-zA-Z']+")
_PUNCT_RE = re.compile(r"[.!?,;:]")
_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]")
_NUMBER_RE = re.compile(r"\d")
_COLOR_WORDS = {"red", "blue", "green", "yellow", "orange", "purple", "black", "white"}
_ANIMAL_WORDS = {"cat", "dog", "fox", "owl", "bear", "whale", "shark", "lion", "tiger", "rabbit"}
_FOOD_WORDS = {"pizza", "taco", "apple", "bread", "soup", "burger", "noodle", "grape", "tea", "cookie"}
_CATEGORY_WORDS = {
    "color": _COLOR_WORDS,
    "animal": _ANIMAL_WORDS,
    "food": _FOOD_WORDS,
}
_SAMPLE_MESSAGES = (
    "Blue bears bake bread!",
    "Do green grapes glow?",
    "7 tiny foxes sprint.",
    "Red rabbits relax.",
    "Taco time tonight!",
    "Can black cats cook?",
    "12 bold owls watch.",
    "Sunny soup sings 🙂",
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
    "Pizza pirates parade 🙂",
    "Do foxes bring bread?",
    "Whispering wolves wait.",
    "8 green goblets gleam.",
    "Can lions lick lemons?",
    "Tea time ticks.",
    "Bold blue balloons burst!",
    "Sharks share soup.",
    "Do purple bears bake?",
    "Happy noodles hum 🙂",
    "10 tiny tacos tumble.",
    "Can red rabbits read?",
    "Bright birds bloom.",
    "Do cats carry cookies?",
    "Golden grapes grow.",
    "7 sleepy sharks smile.",
)


@dataclass(frozen=True)
class RuleAtom:
    family: str
    value: Any = None


def _normalize_text(text: str | None) -> str:
    return " ".join(str(text or "").strip().casefold().split())


def _words(text: str | None) -> list[str]:
    return [token.casefold() for token in _WORD_RE.findall(str(text or ""))]


def _punctuation_count(text: str | None) -> int:
    return len(_PUNCT_RE.findall(str(text or "")))


def _matches_atom(atom: RuleAtom, text: str | None) -> bool:
    raw = str(text or "")
    lowered = _normalize_text(raw)
    words = _words(raw)
    singularish = []
    for word in words:
        singularish.append(word)
        if word.endswith("es") and len(word) > 2:
            singularish.append(word[:-2])
        if word.endswith("s") and len(word) > 1:
            singularish.append(word[:-1])
    if atom.family == "starts_with_letter":
        first_letter = next((char.casefold() for char in raw if char.isalpha()), "")
        return first_letter == str(atom.value).casefold()
    if atom.family == "ends_with_punctuation":
        return raw.rstrip().endswith(str(atom.value))
    if atom.family == "contains_number":
        return _NUMBER_RE.search(raw) is not None
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
    if atom.family == "exact_punctuation_count":
        return _punctuation_count(raw) == int(atom.value)
    if atom.family == "char_length_range":
        minimum, maximum = atom.value
        return minimum <= len(raw.strip()) <= maximum
    return False


def message_matches_rule(rule_atoms: list[RuleAtom], text: str | None) -> bool:
    if not rule_atoms:
        return False
    return all(_matches_atom(atom, text) for atom in rule_atoms)


def render_rule_atom(atom: RuleAtom) -> str:
    if atom.family == "starts_with_letter":
        return f"starts with `{atom.value}`"
    if atom.family == "ends_with_punctuation":
        return f"ends with `{atom.value}`"
    if atom.family == "contains_number":
        return "contains a number"
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
    if atom.family == "exact_punctuation_count":
        return f"contains exactly {atom.value} punctuation mark(s)"
    if atom.family == "char_length_range":
        return f"is {atom.value[0]}-{atom.value[1]} characters long"
    return atom.family


def render_rule(rule_atoms: list[RuleAtom]) -> str:
    return "; ".join(render_rule_atom(atom) for atom in rule_atoms)


def parse_guess_atom(family: str | None, value: str | None) -> tuple[bool, RuleAtom | str]:
    normalized_family = str(family or "").strip().casefold()
    if not normalized_family:
        return False, "Pick a rule family first."
    if normalized_family not in PATTERN_HUNT_RULE_FAMILIES:
        return False, "Unknown rule family."
    raw_value = str(value or "").strip()
    if normalized_family in {"contains_number", "contains_emoji", "question_form", "same_initial_letter"}:
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
    if normalized_family in {"exact_word_count", "exact_punctuation_count"}:
        if not raw_value.isdigit():
            return False, "That rule needs a whole number."
        return True, RuleAtom(normalized_family, int(raw_value))
    if normalized_family in {"word_count_range", "char_length_range"}:
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


def _rule_signature(rule_atoms: list[RuleAtom]) -> tuple[tuple[str, str], ...]:
    parts = []
    for atom in rule_atoms:
        parts.append((atom.family, str(atom.value)))
    return tuple(sorted(parts))


def _compatible(existing: list[RuleAtom], candidate: RuleAtom) -> bool:
    for atom in existing:
        if atom.family == candidate.family:
            return False
        if atom.family == "exact_word_count" and candidate.family == "word_count_range":
            return False
        if atom.family == "word_count_range" and candidate.family == "exact_word_count":
            return False
        if atom.family == "question_form" and candidate.family == "ends_with_punctuation" and candidate.value != "?":
            return False
        if candidate.family == "question_form" and atom.family == "ends_with_punctuation" and atom.value != "?":
            return False
        if atom.family == "exact_punctuation_count" and candidate.family == "question_form" and atom.value == 0:
            return False
        if candidate.family == "exact_punctuation_count" and atom.family == "question_form" and candidate.value == 0:
            return False
        if atom.family == "forbid_letter" and candidate.family == "starts_with_letter" and atom.value == candidate.value:
            return False
        if atom.family == "starts_with_letter" and candidate.family == "forbid_letter" and atom.value == candidate.value:
            return False
    return True


def _atom_candidates(rng: random.Random) -> list[RuleAtom]:
    return [
        RuleAtom("starts_with_letter", rng.choice(("b", "c", "g", "p", "t"))),
        RuleAtom("ends_with_punctuation", rng.choice(("?", "!", "."))),
        RuleAtom("contains_number"),
        RuleAtom("contains_emoji"),
        RuleAtom("contains_category_word", rng.choice(("color", "animal", "food"))),
        RuleAtom("forbid_letter", rng.choice(("a", "e", "i", "o", "u"))),
        RuleAtom("exact_word_count", rng.choice((2, 3, 4, 5))),
        RuleAtom("word_count_range", rng.choice(((2, 3), (3, 4), (3, 5)))),
        RuleAtom("question_form"),
        RuleAtom("same_initial_letter"),
        RuleAtom("exact_punctuation_count", rng.choice((0, 1, 2))),
        RuleAtom("char_length_range", rng.choice(((10, 18), (12, 22), (14, 26)))),
    ]


def select_rule_bundle(seed_value: int) -> tuple[list[RuleAtom], list[str], str]:
    rng = random.Random(seed_value)
    for _ in range(200):
        atom_count = rng.choices((1, 2, 3), weights=(2, 5, 3))[0]
        atoms: list[RuleAtom] = []
        candidates = _atom_candidates(rng)
        rng.shuffle(candidates)
        for candidate in candidates:
            if _compatible(atoms, candidate):
                atoms.append(candidate)
            if len(atoms) >= atom_count:
                break
        if len(atoms) != atom_count:
            continue
        valid_examples = [sample for sample in _SAMPLE_MESSAGES if message_matches_rule(atoms, sample)]
        invalid_examples = [sample for sample in _SAMPLE_MESSAGES if not message_matches_rule(atoms, sample)]
        if len(valid_examples) >= 2 and invalid_examples:
            rng.shuffle(valid_examples)
            return atoms, valid_examples[:2], invalid_examples[0]
    fallback = [RuleAtom("contains_number")]
    return fallback, [sample for sample in _SAMPLE_MESSAGES if _NUMBER_RE.search(sample)][:2], "Blue bears bake bread!"


def ensure_pattern_hunt_state(game: dict[str, Any]) -> dict[str, Any]:
    state = game.setdefault("pattern_hunt", {})
    state.setdefault("phase", "setup")
    state.setdefault("accepted_answers", [])
    return state


async def start_pattern_hunt_game_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    players = list(game.get("players", []))
    if len(players) < 3:
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Not Enough Players",
                "Pattern Hunt needs at least 3 players so one person can guess while the coders hide the rule.",
                tone="warning",
                footer="Babblebox Pattern Hunt",
            )
        )
        await ge.cleanup_game(guild_id)
        return
    seed_value = sum(player.id for player in players) + len(players) * 31
    guesser = random.choice(players)
    coders = [player for player in players if player.id != guesser.id]
    rule_atoms, valid_examples, invalid_example = select_rule_bundle(seed_value)
    for coder in coders:
        try:
            await coder.send(
                embed=ge.make_status_embed(
                    "Pattern Hunt Rule",
                    (
                        f"Secret rule: **{render_rule(rule_atoms)}**\n"
                        f"Valid examples:\n- {valid_examples[0]}\n- {valid_examples[1]}\n"
                        f"Invalid example:\n- {invalid_example}"
                    ),
                    tone="accent",
                    footer="Babblebox Pattern Hunt | Keep it hidden from the guesser",
                )
            )
        except Exception:
            await game["channel"].send(
                embed=ge.make_status_embed(
                    "DM Failure",
                    f"I couldn't DM {coder.mention}, so Pattern Hunt was cancelled before the secret rule leaked unevenly.",
                    tone="danger",
                    footer="Babblebox Pattern Hunt",
                ),
                allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
            )
            await ge.cleanup_game(guild_id)
            return
    turn_limit = max(4, 2 * len(coders))
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
            "turns_used": 0,
            "turn_limit": turn_limit,
            "current_prompt": None,
            "retry_used": False,
            "hint_revealed": False,
            "hint_text": None,
            "accepted_answers": [],
        }
    )
    ge.mark_game_started(game)
    await game["channel"].send(embed=build_pattern_hunt_status_embed(game, public=True, title="Pattern Hunt Started"))
    await _begin_pattern_turn_locked(guild_id, game)


def build_pattern_hunt_status_embed(game: dict[str, Any], *, public: bool, title: str = "Pattern Hunt Status") -> discord.Embed:
    state = ensure_pattern_hunt_state(game)
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
    description = "The guesser asks a prompt, the current coder answers, and the hidden rule stays private."
    if public and state.get("hint_text"):
        description += f"\nHint: {state['hint_text']}"
    embed = discord.Embed(title=title, description=description, color=discord.Color.dark_teal())
    embed.add_field(name="Guesser", value=ge.display_name_of(guesser) if guesser else "Unknown", inline=True)
    embed.add_field(name="Current Coder", value=ge.display_name_of(coder) if coder else "Unknown", inline=True)
    embed.add_field(name="Phase", value=str(state.get("phase", "setup")).title(), inline=True)
    embed.add_field(
        name="Pressure",
        value=(
            f"Guesses left: **{int(state.get('guess_limit', 3)) - int(state.get('guesses_used', 0))}**\n"
            f"Strikes: **{int(state.get('strikes', 0))}/{int(state.get('strike_limit', 3))}**\n"
            f"Turns left: **{int(state.get('turn_limit', 0)) - int(state.get('turns_used', 0))}**"
        ),
        inline=True,
    )
    if state.get("accepted_answers"):
        lines = []
        for item in state["accepted_answers"][-4:]:
            lines.append(f"**{item['coder']}**: {ge.safe_field_text(item['answer'], limit=80)}")
        embed.add_field(name="Accepted Clues", value="\n".join(lines), inline=False)
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
        state["current_prompt"] = message.content
        state["phase"] = "answer"
        state["retry_used"] = False
        token = ge.bump_token(game, "turn_token")
        await ge.cancel_task(game.get("turn_task"))
        coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Coder Turn",
                f"{coder.mention if coder is not None else 'Coder'}, answer once without exposing the rule.",
                tone="accent",
                footer="Babblebox Pattern Hunt",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        game["turn_task"] = asyncio.create_task(_pattern_hunt_answer_timeout(guild_id, token), name=f"babblebox-pattern-answer-{guild_id}")
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
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Accepted",
                f"{message.author.mention}'s answer is locked in.",
                tone="success",
                footer="Babblebox Pattern Hunt",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_pattern_turn_locked(guild_id, game)
        return True
    if not state.get("retry_used"):
        state["retry_used"] = True
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Try Again",
                f"{message.author.mention}, that answer doesn't fit. Retry once without explaining the rule.",
                tone="warning",
                footer="Babblebox Pattern Hunt",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        return True
    await _apply_pattern_strike_locked(guild_id, game, reason=f"{message.author.mention} broke the hidden rule twice in the same turn.")
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
    if _rule_signature(guessed_atoms) == _rule_signature(state.get("rule_atoms", [])):
        await _finish_pattern_hunt_locked(guild_id, game, guesser_won=True, reason=f"{actor.mention} cracked the rule: **{render_rule(state['rule_atoms'])}**.")
        return True, "Correct"
    state["guesses_used"] = int(state.get("guesses_used", 0) or 0) + 1
    remaining = int(state.get("guess_limit", 3)) - int(state.get("guesses_used", 0))
    if remaining <= 0:
        await _finish_pattern_hunt_locked(
            guild_id,
            game,
            guesser_won=False,
            reason=f"{actor.mention} ran out of guesses. The coders held the pattern.",
        )
        return True, "Wrong and out of guesses"
    return False, f"Wrong guess. You have **{remaining}** guess(es) left."


async def _begin_pattern_turn_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    state["phase"] = "prompt"
    state["current_prompt"] = None
    state["retry_used"] = False
    token = ge.bump_token(game, "turn_token")
    await ge.cancel_task(game.get("turn_task"))
    guesser = ge.get_snapshot_player(game, state.get("guesser_id"))
    coder = ge.get_snapshot_player(game, current_pattern_hunt_coder_id(game))
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Prompt Phase",
            f"{guesser.mention if guesser else 'Guesser'}, ask a prompt. {coder.mention if coder else 'Coder'} will answer next.",
            tone="accent",
            footer="Babblebox Pattern Hunt",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    game["turn_task"] = asyncio.create_task(_pattern_hunt_prompt_timeout(guild_id, token), name=f"babblebox-pattern-prompt-{guild_id}")
    ge.reset_idle_timer(guild_id)


async def _advance_pattern_turn_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_pattern_hunt_state(game)
    state["turns_used"] = int(state.get("turns_used", 0) or 0) + 1
    if int(state.get("turns_used", 0)) >= int(state.get("turn_limit", 0)):
        await _finish_pattern_hunt_locked(guild_id, game, guesser_won=False, reason="The coders survived the full turn budget.")
        return
    coder_order = state.get("coder_order", [])
    if coder_order:
        state["current_coder_index"] = (int(state.get("current_coder_index", 0) or 0) + 1) % len(coder_order)
    await _begin_pattern_turn_locked(guild_id, game)


async def _apply_pattern_strike_locked(guild_id: int, game: dict[str, Any], *, reason: str):
    state = ensure_pattern_hunt_state(game)
    state["strikes"] = int(state.get("strikes", 0) or 0) + 1
    if not state.get("hint_revealed") and int(state.get("strikes", 0)) >= 1:
        first_family = state["rule_atoms"][0].family if state.get("rule_atoms") else "unknown"
        state["hint_revealed"] = True
        state["hint_text"] = f"The rule uses **{len(state.get('rule_atoms', []))}** atom(s), and one family is **{first_family.replace('_', ' ')}**."
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Team Strike",
            reason,
            tone="warning",
            footer="Babblebox Pattern Hunt",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    if int(state.get("strikes", 0)) >= int(state.get("strike_limit", 3)):
        await _finish_pattern_hunt_locked(guild_id, game, guesser_won=True, reason="The coders cracked under the rule pressure and hit the strike limit.")
        return
    await _advance_pattern_turn_locked(guild_id, game)


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
        title="Pattern Hunt Reveal",
        description=reason,
        color=discord.Color.gold() if guesser_won else discord.Color.green(),
    )
    embed.add_field(name="Secret Rule", value=render_rule(state.get("rule_atoms", [])), inline=False)
    if state.get("accepted_answers"):
        lines = []
        for item in state["accepted_answers"][-5:]:
            lines.append(f"**{item['coder']}**: {ge.safe_field_text(item['answer'], limit=90)}")
        embed.add_field(name="Clue Recap", value="\n".join(lines), inline=False)
    await game["channel"].send(embed=ge.style_embed(embed, footer="Babblebox Pattern Hunt | Rule revealed"))
    await ge.cleanup_game(guild_id)


async def _pattern_hunt_prompt_timeout(guild_id: int, token: int):
    await asyncio.sleep(PATTERN_HUNT_PROMPT_TIMEOUT_SECONDS)
    game = ge.games.get(guild_id)
    if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
        return
    async with game["lock"]:
        game = ge.games.get(guild_id)
        if not game or game.get("closing") or not game.get("active") or game.get("game_type") != "pattern_hunt":
            return
        if game.get("turn_token") != token:
            return
        await _apply_pattern_strike_locked(guild_id, game, reason="The guesser ran out of time to ask a prompt.")


async def _pattern_hunt_answer_timeout(guild_id: int, token: int):
    await asyncio.sleep(PATTERN_HUNT_ANSWER_TIMEOUT_SECONDS)
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
        await _apply_pattern_strike_locked(
            guild_id,
            game,
            reason=f"{coder.mention if coder else 'The coder'} ran out of time and the team took a strike.",
        )
