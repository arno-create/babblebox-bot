from __future__ import annotations

import asyncio
import contextlib
import re
from datetime import timedelta
from typing import Any

import discord

from babblebox import game_engine as ge


ONLY16_MODE_LABELS = {
    "strict": "Strict",
    "smart": "Smart",
}
ONLY16_ASK_WINDOW_SECONDS = 25
ONLY16_TRAP_WINDOW_SECONDS = 12

_QUESTION_START_RE = re.compile(r"^\s*(how|what|which|who)\b", re.IGNORECASE)
_QUANTITY_HINT_RE = re.compile(
    r"\b("
    r"how many|how much|what number|which number|how old|how long|how tall|how far|"
    r"how heavy|how wide|how often|what time|how large|how big|how deep|how fast|how slow|"
    r"how high|how low|how strong|how rare|how common"
    r")\b",
    re.IGNORECASE,
)
_DIGIT_RE = re.compile(r"(?<![\w-])(-?\d+)(?![\w-])")
_SIXTEEN_RE = re.compile(r"(?<![\w-])sixteen(?![\w-])", re.IGNORECASE)


def only16_mode_label(mode: str | None) -> str:
    return ONLY16_MODE_LABELS.get(str(mode or "").casefold(), "Strict")


def ensure_only16_state(game: dict[str, Any]) -> dict[str, Any]:
    state = game.setdefault("only16", {})
    state.setdefault("mode", game.get("only16_mode", "strict"))
    state.setdefault("trap", None)
    state.setdefault("asker_notice_sent_at", 0.0)
    state.setdefault("manual_hint", "Use the Arm 16 Trap message action if Babblebox misses a fair question.")
    return state


def cycle_only16_mode(game: dict[str, Any]) -> str:
    state = ensure_only16_state(game)
    current = str(state.get("mode", "strict")).casefold()
    next_mode = "smart" if current == "strict" else "strict"
    state["mode"] = next_mode
    game["only16_mode"] = next_mode
    return next_mode


def has_question_intent(text: str | None) -> bool:
    content = str(text or "").strip()
    if not content:
        return False
    return "?" in content or _QUESTION_START_RE.search(content) is not None


def has_quantity_intent(text: str | None) -> bool:
    return _QUANTITY_HINT_RE.search(str(text or "")) is not None


def detect_count_question(text: str | None) -> bool:
    return has_question_intent(text) and has_quantity_intent(text)


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


def parse_first_explicit_number(text: str | None) -> int | None:
    content = str(text or "")
    matches: list[tuple[int, int]] = []
    digit_match = _DIGIT_RE.search(content)
    if digit_match is not None:
        matches.append((digit_match.start(), int(digit_match.group(1))))
    sixteen_match = _SIXTEEN_RE.search(content)
    if sixteen_match is not None:
        matches.append((sixteen_match.start(), 16))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def _trap_is_live(trap: dict[str, Any] | None) -> bool:
    if not isinstance(trap, dict):
        return False
    expires_at = trap.get("expires_at")
    if expires_at is None or not hasattr(expires_at, "tzinfo"):
        return False
    return expires_at > ge.now_utc()


def _only16_player_names(game: dict[str, Any]) -> str:
    return ge.join_limited_lines([f"**{index + 1}.** {player.display_name}" for index, player in enumerate(game.get("players", []))])


async def start_only16_game_locked(guild_id: int, game: dict[str, Any]):
    state = ensure_only16_state(game)
    state["trap"] = None
    intro = discord.Embed(
        title="Only 16",
        description=(
            f"Mode: **{only16_mode_label(state.get('mode'))}**\n"
            "Ask clear quantity questions. If someone answers with a number other than **16**, they are out."
        ),
        color=discord.Color.orange(),
    )
    intro.add_field(name="Live Players", value=_only16_player_names(game), inline=False)
    intro = ge.style_embed(intro, footer="Babblebox Only 16 | Replies are safest")
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
        if detect_count_question(message.content):
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
    await _consume_only16_trap_locked(guild_id, game, reason="That armed question disappeared, so the trap was voided.")
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
    if not has_question_intent(message.content):
        return False, "Manual arming still needs a clear question."
    await _arm_only16_trap_locked(guild_id, game, message, manual=True)
    return True, "16 trap armed."


async def _arm_only16_trap_locked(guild_id: int, game: dict[str, Any], message: discord.Message, *, manual: bool):
    state = ensure_only16_state(game)
    await ge.cancel_task(game.get("turn_task"))
    trap = {
        "asker_id": message.author.id,
        "question_message_id": message.id,
        "armed_at": ge.now_utc(),
        "expires_at": ge.now_utc() + timedelta(seconds=ONLY16_TRAP_WINDOW_SECONDS),
        "mode": state.get("mode", "strict"),
        "interleaving_messages": 0,
        "manual": manual,
    }
    state["trap"] = trap
    with contextlib.suppress(discord.HTTPException):
        await message.add_reaction("🎯")
    if manual:
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(
                embed=ge.make_status_embed(
                    "Trap Armed",
                    f"{message.author.mention} manually armed that question. Replies are the fairest answer lane.",
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
    number = parse_first_explicit_number(message.content)

    if is_reply:
        return await _resolve_only16_answer_locked(message, guild_id, game, trap, number)

    if mode != "smart":
        return False
    if has_question_intent(message.content):
        trap["interleaving_messages"] += 2
        await _consume_only16_trap_locked(guild_id, game, reason="The room got too noisy to judge that fairly.")
        return True
    if number is None:
        trap["interleaving_messages"] += 1
        if trap["interleaving_messages"] > 1:
            await _consume_only16_trap_locked(guild_id, game, reason="Too much chatter. That trap expired without a clean target.")
            return True
        return False
    if trap["interleaving_messages"] > 1:
        await _consume_only16_trap_locked(guild_id, game, reason="Too much chatter. That trap expired without a clean target.")
        return True
    return await _resolve_only16_answer_locked(message, guild_id, game, trap, number)


async def _resolve_only16_answer_locked(message: discord.Message, guild_id: int, game: dict[str, Any], trap: dict[str, Any], number: int | None) -> bool:
    await ge.cancel_task(game.get("turn_task"))
    state = ensure_only16_state(game)
    state["trap"] = None
    asker_id = trap["asker_id"]
    if number is None:
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Trap Consumed",
                f"{message.author.mention} answered without a clear number, so nobody gets clipped on ambiguity.",
                tone="info",
                footer="Babblebox Only 16",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True

    if number == 16:
        await game["channel"].send(
            embed=ge.make_status_embed(
                "Still Alive",
                f"{message.author.mention} said **16** and slips through.",
                tone="success",
                footer="Babblebox Only 16",
            ),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
        await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
        return True

    eliminated = ge.get_player_by_id(game, message.author.id)
    if eliminated is not None:
        game["players"] = [player for player in game["players"] if player.id != message.author.id]
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Eliminated",
            f"{message.author.mention} said **{number}** instead of **16** and is out.",
            tone="danger",
            footer="Babblebox Only 16",
        ),
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    if len(game["players"]) <= 1:
        await _finish_only16_locked(guild_id, game)
        return True
    if asker_id not in {player.id for player in game["players"]}:
        game["current_player_index"] = 0
    await _advance_to_next_asker_locked(guild_id, game, asker_id=asker_id)
    return True


async def _consume_only16_trap_locked(guild_id: int, game: dict[str, Any], *, reason: str):
    await ge.cancel_task(game.get("turn_task"))
    state = ensure_only16_state(game)
    trap = state.get("trap")
    asker_id = trap.get("asker_id") if isinstance(trap, dict) else None
    state["trap"] = None
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Trap Expired",
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
    token = ge.bump_token(game, "turn_token")
    await ge.cancel_task(game.get("turn_task"))
    await game["channel"].send(
        embed=ge.make_status_embed(
            "Your Trap Turn",
            (
                f"{current_asker.mention}, ask a clear quantity question in the next **{ONLY16_ASK_WINDOW_SECONDS} seconds**.\n"
                f"Mode: **{only16_mode_label(state.get('mode'))}**"
            ),
            tone="accent",
            footer="Babblebox Only 16 | Replies are safest",
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
                f"{current_asker.mention} did not arm a clean 16 trap in time, so the turn moves on.",
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
        await _consume_only16_trap_locked(guild_id, game, reason="Nobody took the bait before the trap window closed.")
