import asyncio
import contextlib
import io
import random
import traceback
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands

from babblebox.text_safety import sanitize_short_plain_text

# ==========================================
# GLOBALS & CONSTANTS
# ==========================================
MAX_PLAYERS = 25  # Discord select menus support at most 25 options.
IDLE_TIMEOUT_SECONDS = 600
TURN_TIMEOUT_SECONDS = 60
SPYFALL_VOTE_TIMEOUT_SECONDS = 60
MAX_VOICE_BYTES = 8 * 1024 * 1024
DICTIONARY_URL = "https://raw.githubusercontent.com/dwyl/english-words/master/words_alpha.txt"

VALID_WORDS = set()
games = {}
dm_routes = {}
session_stats = {}
games_guard = asyncio.Lock()

SESSION_NOTE = "Session stats reset whenever the bot restarts."
AFK_REASON_MAX_LEN = 160
AFK_MAX_DURATION_MINUTES = 10080
AFK_MAX_SCHEDULE_MINUTES = 10080
AFK_REASON_SENTENCE_LIMIT = 3
CORPSE_STEP_LABELS = [
    "Adjective",
    "Noun",
    "Verb (past tense)",
    "Adjective",
    "Noun",
    "Location",
]
BOMB_MODE_ORDER = ["classic", "chaos", "hardcore"]
BOMB_MODE_CONFIGS = {
    "classic": {
        "label": "Classic",
        "description": "Balanced turns with the normal speed-up curve.",
        "start_time": 15.0,
        "warning": True,
        "speed_every": 5,
        "minimum_time": 3.0,
    },
    "chaos": {
        "label": "Chaos",
        "description": "Random turn modifiers keep everyone guessing.",
        "start_time": 15.0,
        "warning": True,
        "speed_every": 5,
        "minimum_time": 3.0,
    },
    "hardcore": {
        "label": "Hardcore",
        "description": "Short fuse, faster speed-ups, and no warning callouts.",
        "start_time": 8.0,
        "warning": False,
        "speed_every": 4,
        "minimum_time": 2.5,
    },
}
CHAOS_MODIFIERS = [
    {"type": "none", "label": "Standard Turn", "description": "No extra modifier this round."},
    {"type": "min_length", "value": 6, "label": "Long Word", "description": "Your word must be at least 6 letters long."},
    {"type": "fresh_start", "label": "Fresh Start", "description": "The used-word history is wiped for this turn."},
    {"type": "short_fuse", "value": 2.0, "label": "Short Fuse", "description": "This turn loses 2 seconds."},
    {"type": "bonus_breath", "value": 1.0, "label": "Bonus Breath", "description": "This turn gains 1 bonus second."},
]
CHAOS_CARD_ORDER = ["none", "reverse_order", "lightning_round", "encore_reveal"]
CHAOS_CARDS = {
    "none": {
        "label": "Off",
        "description": "Start the lobby with the standard ruleset.",
    },
    "reverse_order": {
        "label": "Reverse Order",
        "description": "After the player list is shuffled, the turn order is flipped.",
    },
    "lightning_round": {
        "label": "Lightning Round",
        "description": "Shorter DM turns, faster votes, and a tighter Word Bomb fuse.",
    },
    "encore_reveal": {
        "label": "Encore Reveal",
        "description": "Recaps get a dramatic headline for screenshot-worthy endings.",
    },
}

THEMES = [
    "Cyberpunk 🦾",
    "Horror 🧛‍♂️",
    "Wild West 🤠",
    "Office Drama 📎",
    "Medieval Fantasy 🐉",
    "Romantic Comedy 💕",
]
CORPSE_PROMPTS = [
    "1️⃣ Type an **Adjective** (e.g., creepy, shiny):",
    "2️⃣ Type a **Noun** (e.g., alien, toaster):",
    "3️⃣ Type a **Verb in past tense** (e.g., hugged, destroyed):",
    "4️⃣ Type another **Adjective** (e.g., depressed, radioactive):",
    "5️⃣ Type another **Noun** (e.g., refrigerator, ghost):",
    "6️⃣ Type a **Location** (e.g., in a bathroom, on Mars):",
]
SPYFALL_LOCATIONS = [
    "Airplane ✈️", "Bank 🏦", "Beach 🏖️", "Casino 🎰", "Cathedral ⛪",
    "Corporate Party 👔", "Crusader Army 🛡️", "Day Spa 💆", "Embassy 🌍",
    "Hospital 🏥", "Hotel 🏨", "Military Base 🪖", "Movie Studio 🎬",
    "Ocean Liner 🛳️", "Passenger Train 🚂", "Pirate Ship 🏴‍☠️", "Polar Station 🥶",
    "Police Station 🚓", "Restaurant 🍽️", "School 🏫", "Space Station 🚀",
    "Submarine 🌊", "Supermarket 🛒", "Theater 🎭", "University 🎓",
]
BOMB_SYLLABLES = [
    "TH", "ER", "IN", "ON", "AT", "CH", "ST", "RE",
    "QU", "BL", "CK", "ING", "OU", "SH", "TR", "PL",
]
PERMISSION_LABELS = {
    "view_channel": "View Channels",
    "send_messages": "Send Messages",
    "embed_links": "Embed Links",
    "attach_files": "Attach Files",
    "read_message_history": "Read Message History",
    "add_reactions": "Add Reactions",
}

HELP_REQUIRED_PERMS = (
    "send_messages",
    "embed_links",
)

PLAY_REQUIRED_PERMS = (
    "view_channel",
    "send_messages",
    "embed_links",
    "attach_files",
    "read_message_history",
    "add_reactions",
)

VOTE_REQUIRED_PERMS = (
    "view_channel",
    "send_messages",
    "embed_links",
    "read_message_history",
)

STOP_REQUIRED_PERMS = (
    "send_messages",
)

# ==========================================
# RUNTIME BOT REFERENCE
# ==========================================
BOT_REF: Optional[commands.Bot] = None


def set_runtime_bot(bot: commands.Bot):
    global BOT_REF
    BOT_REF = bot


def get_runtime_bot() -> Optional[commands.Bot]:
    return BOT_REF


def get_profile_service():
    runtime_bot = get_runtime_bot()
    if runtime_bot is None:
        return None
    return getattr(runtime_bot, "profile_service", None)


def schedule_profile_update(method_name, /, *args, **kwargs):
    profile_service = get_profile_service()
    if profile_service is None or not getattr(profile_service, "storage_ready", False):
        return
    method = getattr(profile_service, method_name, None)
    if method is None:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    loop.create_task(method(*args, **kwargs), name=f"babblebox-profile-{method_name}")


# ==========================================
# HELPERS
# ==========================================
async def send_prefix_embed(ctx: commands.Context, *, embed: discord.Embed, view=None):
    return await ctx.send(embed=embed, view=view)

async def send_prefix_text(ctx: commands.Context, content: str):
    return await ctx.send(content)

def get_ctx_channel_permissions(ctx: commands.Context):
    runtime_bot = get_runtime_bot()
    if ctx.guild is None or ctx.channel is None or runtime_bot is None or runtime_bot.user is None:
        return None

    me = ctx.guild.me or ctx.guild.get_member(runtime_bot.user.id)
    if me is None:
        return None

    return ctx.channel.permissions_for(me)

async def require_bot_permissions_prefix(
    ctx: commands.Context,
    required_permissions,
    command_name: str
) -> bool:
    channel_perms = get_ctx_channel_permissions(ctx)
    if channel_perms is None:
        return True

    missing = [
        PERMISSION_LABELS.get(name, name.replace("_", " ").title())
        for name in required_permissions
        if not getattr(channel_perms, name, False)
    ]

    if not missing:
        return True

    embed = discord.Embed(
        title="⚠️ Missing Bot Permission(s)",
        description=(
            f"I can’t run **{command_name}** in this channel because I’m missing:\n"
            + "\n".join(f"• {perm}" for perm in missing)
        ),
        color=discord.Color.orange(),
    )
    embed.add_field(
        name="How to fix it",
        value="Please restore those permissions for me in this channel or category, then try again.",
        inline=False,
    )
    await ctx.send(embed=embed)
    return False
    
def is_player_in_game(game, user_id):
    return any(player.id == user_id for player in game.get("players", []))


def get_player_by_id(game, user_id):
    return next((player for player in game.get("players", []) if player.id == user_id), None)


def get_current_player(game):
    players = game.get("players", [])
    if not players:
        return None
    idx = game.get("current_player_index", 0)
    if idx < 0 or idx >= len(players):
        return None
    return players[idx]


def bump_token(game, key):
    game[key] = game.get(key, 0) + 1
    return game[key]


def claim_dm_route(guild_id, user_id):
    current = dm_routes.get(user_id)
    if current is not None and current != guild_id:
        return False
    dm_routes[user_id] = guild_id
    return True


def release_dm_route(user_id, guild_id=None):
    current = dm_routes.get(user_id)
    if current is None:
        return
    if guild_id is None or current == guild_id:
        dm_routes.pop(user_id, None)


def register_view(guild_id, view, message):
    game = games.get(guild_id)
    if not game:
        return
    view.message = message
    if view not in game["views"]:
        game["views"].append(view)


def unregister_view(guild_id, view):
    game = games.get(guild_id)
    if not game:
        return
    with contextlib.suppress(ValueError):
        game["views"].remove(view)


def get_live_view(game, view_type):
    for view in reversed(game.get("views", [])):
        if isinstance(view, view_type) and getattr(view, "message", None) is not None:
            return view
    return None


def can_emit_notice(game, key, interval=1.5):
    now = asyncio.get_running_loop().time()
    last = game.get(key, 0.0)
    if now - last < interval:
        return False
    game[key] = now
    return True


def build_ping_string(players, max_length=1800):
    mentions = []
    total = 0
    for player in players:
        mention = player.mention
        projected = total + len(mention) + 1
        if projected > max_length:
            break
        mentions.append(mention)
        total = projected
    return " ".join(mentions) if mentions else "everyone"


def display_name_of(user):
    return getattr(user, "display_name", getattr(user, "name", f"User {getattr(user, 'id', '?')}"))


def get_bomb_mode_config(mode):
    return BOMB_MODE_CONFIGS.get(mode, BOMB_MODE_CONFIGS["classic"])


def get_next_bomb_mode(current_mode):
    try:
        idx = BOMB_MODE_ORDER.index(current_mode)
    except ValueError:
        return BOMB_MODE_ORDER[0]
    return BOMB_MODE_ORDER[(idx + 1) % len(BOMB_MODE_ORDER)]


def get_chaos_card_config(card_id):
    return CHAOS_CARDS.get(card_id, CHAOS_CARDS["none"])


def get_next_chaos_card(current_card):
    try:
        idx = CHAOS_CARD_ORDER.index(current_card)
    except ValueError:
        return CHAOS_CARD_ORDER[0]
    return CHAOS_CARD_ORDER[(idx + 1) % len(CHAOS_CARD_ORDER)]


def build_chaos_card_line(game):
    card = get_chaos_card_config(game.get("chaos_card", "none"))
    return f"**{card['label']}** - {card['description']}"


def apply_chaos_card(game):
    card_id = game.get("chaos_card", "none")
    game["dm_turn_timeout"] = TURN_TIMEOUT_SECONDS
    game["spyfall_vote_timeout"] = SPYFALL_VOTE_TIMEOUT_SECONDS
    game["bomb_time_modifier"] = 0.0

    if card_id == "reverse_order":
        game["players"].reverse()
    elif card_id == "lightning_round":
        game["dm_turn_timeout"] = 45
        game["spyfall_vote_timeout"] = 45
        game["bomb_time_modifier"] = -2.0


def build_chaos_headline(game):
    card_id = game.get("chaos_card", "none")
    if card_id != "encore_reveal":
        return None

    game_type = game.get("game_type")
    if game_type == "telephone":
        return "Encore headline: the relay survived just long enough to become a rumor."
    if game_type == "corpse":
        return f"Encore headline: the {game.get('theme', 'mystery')} writers' room has been shut down."
    if game_type == "spyfall":
        return f"Encore headline: the spy story from {game.get('location', 'somewhere suspicious')} is now local legend."
    if game_type == "bomb":
        survivors = len(game.get("starting_players", []))
        winner = game.get("winner_name", "The winner")
        return f"Encore headline: {winner} outlasted a {survivors}-player blast wave."
    return "Encore headline: the lobby somehow got even louder."


def apply_chaos_recap(embed, game):
    card_id = game.get("chaos_card", "none")
    if card_id != "none":
        embed.add_field(name="Chaos Card", value=build_chaos_card_line(game), inline=False)
    headline = build_chaos_headline(game)
    if headline:
        embed.add_field(name="Encore", value=safe_field_text(headline), inline=False)


def create_game_state(host, channel):
    return {
        "host": host,
        "lobby_open": True,
        "game_type": "none",
        "players": [],
        "active": False,
        "closing": False,
        "current_player_index": 0,
        "channel": channel,
        "turn_task": None,
        "idle_task": None,
        "vote_task": None,
        "lock": asyncio.Lock(),
        "views": [],
        "votes": {},
        "voting_active": False,
        "first_audio": None,
        "final_audio": None,
        "waiting_for_guess": False,
        "corpse_answers": [],
        "corpse_step": 0,
        "theme": "",
        "turn_token": 0,
        "vote_token": 0,
        "bomb_mode": "classic",
        "starting_players": [],
        "stats_recorded": False,
        "result_recorded": False,
        "corpse_contributions": [],
        "interrogation_log": [],
        "bomb_word_history": [],
        "bomb_eliminations": [],
        "bomb_modifier_log": [],
        "bomb_current_rule": None,
        "bomb_current_turn_time_limit": None,
        "bomb_warning_enabled": True,
        "bomb_speed_every": 5,
        "bomb_minimum_time": 3.0,
        "bomb_turn_started_at": None,
        "chaos_card": "none",
        "dm_turn_timeout": TURN_TIMEOUT_SECONDS,
        "spyfall_vote_timeout": SPYFALL_VOTE_TIMEOUT_SECONDS,
        "bomb_time_modifier": 0.0,
    }


def safe_field_text(text, limit=1024):
    if text is None:
        return "N/A"
    text = str(text)
    return text if len(text) <= limit else text[: limit - 3] + "..."


def join_limited_lines(lines, limit=1024, empty="Nothing to show yet."):
    if not lines:
        return empty

    output = []
    used = 0
    for index, line in enumerate(lines):
        add_len = len(line) + (1 if output else 0)
        if used + add_len > limit:
            remaining = len(lines) - index
            tail = f"… and {remaining} more"
            if output and used + len(tail) + 1 <= limit:
                output.append(tail)
            elif not output:
                return safe_field_text(line, limit=limit)
            break
        output.append(line)
        used += add_len
    return "\n".join(output)


def get_snapshot_player(game, user_id):
    player = get_player_by_id(game, user_id)
    if player is not None:
        return player
    return next((player for player in game.get("starting_players", []) if player.id == user_id), None)


def player_name_from_game(game, user_id):
    player = get_snapshot_player(game, user_id)
    if player is not None:
        return display_name_of(player)
    return f"User {user_id}"


def format_turn_order(players):
    return join_limited_lines(
        [f"**{index + 1}.** {display_name_of(player)}" for index, player in enumerate(players)],
        empty="No players recorded.",
    )


def get_player_stats(user):
    stats = session_stats.setdefault(
        user.id,
        {
            "user_id": user.id,
            "display_name": display_name_of(user),
            "games_played": 0,
            "games_hosted": 0,
            "wins": 0,
            "telephone_games": 0,
            "telephone_completions": 0,
            "corpse_games": 0,
            "corpse_masterpieces": 0,
            "spyfall_games": 0,
            "spy_wins": 0,
            "village_wins": 0,
            "spies_caught": 0,
            "bomb_games": 0,
            "bomb_wins": 0,
            "bomb_words": 0,
            "bomb_fastest_word_time": None,
            "bomb_fastest_word": "",
        },
    )
    stats["display_name"] = display_name_of(user)
    return stats


def mark_game_started(game):
    if game.get("stats_recorded"):
        return

    host_stats = get_player_stats(game["host"])
    host_stats["games_hosted"] += 1

    game_type = game.get("game_type")
    players = game.get("starting_players") or list(game.get("players", []))
    for player in players:
        stats = get_player_stats(player)
        stats["games_played"] += 1
        if game_type == "telephone":
            stats["telephone_games"] += 1
        elif game_type == "corpse":
            stats["corpse_games"] += 1
        elif game_type == "spyfall":
            stats["spyfall_games"] += 1
        elif game_type == "bomb":
            stats["bomb_games"] += 1

    game["stats_recorded"] = True
    schedule_profile_update(
        "record_game_started",
        game_type=game_type,
        host_id=game["host"].id,
        player_ids=[player.id for player in players],
    )


def mark_telephone_completion(game):
    if game.get("result_recorded"):
        return
    game["result_recorded"] = True
    for player in game.get("starting_players", []):
        get_player_stats(player)["telephone_completions"] += 1
    schedule_profile_update(
        "record_telephone_completion",
        player_ids=[player.id for player in game.get("starting_players", [])],
    )


def mark_corpse_completion(game):
    if game.get("result_recorded"):
        return
    game["result_recorded"] = True
    for player in game.get("starting_players", []):
        get_player_stats(player)["corpse_masterpieces"] += 1
    schedule_profile_update(
        "record_corpse_completion",
        player_ids=[player.id for player in game.get("starting_players", [])],
    )


def mark_spyfall_result(game, *, village_won):
    if game.get("result_recorded"):
        return
    game["result_recorded"] = True

    spy_id = game["spy"].id
    for player in game.get("starting_players", []):
        stats = get_player_stats(player)
        if player.id == spy_id:
            if not village_won:
                stats["wins"] += 1
                stats["spy_wins"] += 1
        else:
            if village_won:
                stats["wins"] += 1
                stats["village_wins"] += 1
                stats["spies_caught"] += 1
    schedule_profile_update(
        "record_spyfall_result",
        spy_id=spy_id,
        player_ids=[player.id for player in game.get("starting_players", [])],
        village_won=village_won,
    )


def mark_bomb_win(game, winner):
    if game.get("result_recorded"):
        return
    game["result_recorded"] = True
    stats = get_player_stats(winner)
    stats["wins"] += 1
    stats["bomb_wins"] += 1
    schedule_profile_update("record_bomb_win", winner_id=winner.id)


def record_bomb_word(player, word, elapsed):
    stats = get_player_stats(player)
    stats["bomb_words"] += 1
    best_time = stats.get("bomb_fastest_word_time")
    if best_time is None or elapsed < best_time:
        stats["bomb_fastest_word_time"] = elapsed
        stats["bomb_fastest_word"] = word


def build_telephone_recap_embed(game, guess_text):
    embed = discord.Embed(
        title="🎬 Babblebox Recap: Broken Telephone",
        description="One clip, several panic-induced mimics, and one final guess.",
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Turn Order",
        value=safe_field_text(format_turn_order(game.get("starting_players", []))),
        inline=False,
    )
    embed.add_field(
        name="Final Guess",
        value=safe_field_text(guess_text),
        inline=False,
    )
    audio_lines = [
        f"Original clip: {'✅' if game.get('first_audio') else '❌'}",
        f"Final mimic: {'✅' if game.get('final_audio') else '❌'}",
    ]
    embed.add_field(name="Audio Trail", value="\n".join(audio_lines), inline=False)
    apply_chaos_recap(embed, game)
    return style_embed(embed, footer="Babblebox Recap | /play or bb!play for another round")


def build_corpse_recap_embed(game):
    contribution_lines = []
    for item in game.get("corpse_contributions", []):
        label = CORPSE_STEP_LABELS[item["step"]]
        contribution_lines.append(
            f"**{item['step'] + 1}.** {player_name_from_game(game, item['player_id'])} — *{label}*: {item['answer']}"
        )

    embed = discord.Embed(
        title="🎬 Babblebox Recap: Exquisite Corpse",
        description=f"Theme: **{game.get('theme', 'Unknown')}**",
        color=discord.Color.purple(),
    )
    embed.add_field(
        name="Contributions",
        value=join_limited_lines(contribution_lines, empty="No contributions recorded."),
        inline=False,
    )
    apply_chaos_recap(embed, game)
    return style_embed(embed, footer="Babblebox Recap | /play or bb!play for another round")


def build_spyfall_recap_embed(game):
    vote_lines = []
    for voter_id, target_id in game.get("votes", {}).items():
        vote_lines.append(
            f"**{player_name_from_game(game, voter_id)}** → {player_name_from_game(game, target_id)}"
        )

    chain_lines = [
        f"{player_name_from_game(game, hop['from_id'])} → {player_name_from_game(game, hop['to_id'])}"
        for hop in game.get("interrogation_log", [])
    ]

    embed = discord.Embed(
        title="🎬 Babblebox Recap: Spyfall",
        description="Here is how the interrogation and voting played out.",
        color=discord.Color.dark_gray(),
    )
    embed.add_field(
        name="Role Reveal",
        value=safe_field_text(
            f"Spy: **{display_name_of(game['spy'])}**\nLocation: **{game.get('location', 'Unknown')}**"
        ),
        inline=False,
    )
    embed.add_field(
        name="Votes",
        value=join_limited_lines(vote_lines, empty="Nobody locked in a vote."),
        inline=False,
    )
    embed.add_field(
        name="Interrogation Chain",
        value=join_limited_lines(chain_lines[-8:], empty="The table barely got moving before the vote happened."),
        inline=False,
    )
    apply_chaos_recap(embed, game)
    return style_embed(embed, footer="Babblebox Recap | /play or bb!play for another round")


def build_bomb_recap_embed(game, winner):
    history = game.get("bomb_word_history", [])
    fastest = min(history, key=lambda item: item["elapsed"]) if history else None
    longest = max(history, key=lambda item: (len(item["word"]), -item["elapsed"])) if history else None
    elimination_lines = [
        f"**{index + 1}.** {item['name']}"
        for index, item in enumerate(game.get("bomb_eliminations", []))
    ]
    modifier_lines = [f"• {item}" for item in game.get("bomb_modifier_log", [])[-5:]]

    config = get_bomb_mode_config(game.get("bomb_mode", "classic"))
    embed = discord.Embed(
        title="🎬 Babblebox Recap: Word Bomb",
        description=(
            f"Mode: **{config['label']}** • Accepted words: **{len(history)}** • "
            f"Winner: **{display_name_of(winner)}**"
        ),
        color=discord.Color.red(),
    )

    if fastest is not None:
        embed.add_field(
            name="Quick Draw",
            value=safe_field_text(
                f"{player_name_from_game(game, fastest['player_id'])} — **{fastest['word']}** in **{fastest['elapsed']:.2f}s**"
            ),
            inline=False,
        )

    if longest is not None:
        embed.add_field(
            name="Longest Word",
            value=safe_field_text(
                f"{player_name_from_game(game, longest['player_id'])} — **{longest['word']}** ({len(longest['word'])} letters)"
            ),
            inline=False,
        )

    embed.add_field(
        name="Elimination Order",
        value=join_limited_lines(elimination_lines, empty="No eliminations were recorded."),
        inline=False,
    )

    if game.get("bomb_mode") == "chaos":
        embed.add_field(
            name="Chaos Log",
            value=join_limited_lines(modifier_lines, empty="The chaos wheel somehow behaved itself."),
            inline=False,
        )

    apply_chaos_recap(embed, game)
    return style_embed(embed, footer="Babblebox Recap | /play or bb!play for another round")


def prepare_bomb_turn(game):
    game["bomb_current_rule"] = None
    game["bomb_current_turn_time_limit"] = game["time_limit"]

    if game.get("bomb_mode") != "chaos":
        return

    modifier = dict(random.choice(CHAOS_MODIFIERS))
    if modifier["type"] == "fresh_start":
        cleared = len(game["used_words"])
        game["used_words"].clear()
        if cleared:
            modifier["description"] = f"The used-word history was wiped ({cleared} words cleared)."
        else:
            modifier["description"] = "The used-word history was already empty, but the slate is still clean."
    elif modifier["type"] == "short_fuse":
        game["bomb_current_turn_time_limit"] = max(
            game["bomb_minimum_time"],
            game["time_limit"] - modifier.get("value", 0.0),
        )
    elif modifier["type"] == "bonus_breath":
        game["bomb_current_turn_time_limit"] = game["time_limit"] + modifier.get("value", 0.0)

    game["bomb_current_rule"] = modifier
    if modifier["type"] != "none":
        game["bomb_modifier_log"].append(
            f"Turn {game['turn_count'] + 1}: {modifier['label']} — {modifier['description']}"
        )


    time_modifier = game.get("bomb_time_modifier", 0.0)
    if time_modifier:
        game["bomb_current_turn_time_limit"] = max(
            game.get("bomb_minimum_time", 3.0),
            game["bomb_current_turn_time_limit"] + time_modifier,
        )


def build_bomb_turn_message(game, player):
    base = (
        f"💣 Passed to {player.mention}! "
        f"(Syllable: **{game['syllable']}**, Time: **{game['bomb_current_turn_time_limit']:.1f}s**)"
    )

    modifier = game.get("bomb_current_rule")
    if modifier and modifier.get("type") != "none":
        base += f"\n⚡ **{modifier['label']}** — {modifier['description']}"
    return base


def build_bomb_turn_embed(game, player):
    embed = discord.Embed(
        title="Word Bomb Turn",
        description=f"{player.mention}, you are live. Send one valid English word before the timer expires.",
        color=EMBED_THEME["danger"],
    )
    embed.add_field(name="Syllable", value=f"**{game['syllable']}**", inline=True)
    embed.add_field(name="Timer", value=f"**{game['bomb_current_turn_time_limit']:.1f}s**", inline=True)
    embed.add_field(name="Mode", value=get_bomb_mode_config(game.get("bomb_mode", "classic"))["label"], inline=True)

    modifier = game.get("bomb_current_rule")
    if modifier and modifier.get("type") != "none":
        embed.add_field(
            name=f"Chaos Modifier: {modifier['label']}",
            value=safe_field_text(modifier["description"]),
            inline=False,
        )

    return style_embed(embed, footer="Babblebox Word Bomb | One word only")


def validate_bomb_modifier(game, word):
    modifier = game.get("bomb_current_rule")
    if not modifier:
        return None

    if modifier["type"] == "min_length" and len(word) < modifier["value"]:
        return f"❌ Chaos rule: the word must be at least {modifier['value']} letters long!"

    return None


def build_stats_embed(target, stats):
    embed = discord.Embed(
        title=f"📊 Babblebox Session Stats — {display_name_of(target)}",
        color=discord.Color.blurple(),
    )
    overview = (
        f"Games Played: **{stats['games_played']}**\n"
        f"Wins: **{stats['wins']}**\n"
        f"Games Hosted: **{stats['games_hosted']}**"
    )
    embed.add_field(name="Overview", value=overview, inline=False)

    creative = (
        f"Broken Telephone: **{stats['telephone_games']}** played / **{stats['telephone_completions']}** completed\n"
        f"Exquisite Corpse: **{stats['corpse_games']}** played / **{stats['corpse_masterpieces']}** masterpieces"
    )
    embed.add_field(name="Creative Games", value=creative, inline=False)

    spyfall = (
        f"Games: **{stats['spyfall_games']}**\n"
        f"Spy Wins: **{stats['spy_wins']}**\n"
        f"Village Wins: **{stats['village_wins']}**\n"
        f"Spies Caught: **{stats['spies_caught']}**"
    )
    embed.add_field(name="Spyfall", value=spyfall, inline=False)

    fastest_text = "No valid words recorded yet."
    if stats["bomb_fastest_word_time"] is not None and stats["bomb_fastest_word"]:
        fastest_text = f"**{stats['bomb_fastest_word']}** in **{stats['bomb_fastest_word_time']:.2f}s**"

    bomb = (
        f"Games: **{stats['bomb_games']}**\n"
        f"Wins: **{stats['bomb_wins']}**\n"
        f"Words Cleared: **{stats['bomb_words']}**\n"
        f"Fastest Word: {fastest_text}"
    )
    embed.add_field(name="Word Bomb", value=bomb, inline=False)
    return style_embed(embed, footer=f"Babblebox Session Stats | {SESSION_NOTE}")


def build_leaderboard_embed(metric_key, label, entries):
    embed = discord.Embed(
        title=f"🏆 Babblebox Leaderboard — {label}",
        color=discord.Color.gold(),
    )
    lines = [
        f"**{index + 1}.** {entry['display_name']} — **{entry.get(metric_key, 0)}**"
        for index, entry in enumerate(entries[:10])
    ]
    embed.description = join_limited_lines(lines, empty="Nobody has any stats for that category yet.")
    return style_embed(embed, footer=f"Babblebox Leaderboard | {SESSION_NOTE}")



def now_utc():
    return datetime.now(timezone.utc)


EMBED_THEME = {
    "info": discord.Color.from_rgb(88, 145, 255),
    "success": discord.Color.from_rgb(67, 185, 127),
    "warning": discord.Color.from_rgb(245, 188, 66),
    "danger": discord.Color.from_rgb(232, 86, 86),
    "accent": discord.Color.from_rgb(120, 110, 255),
}


def style_embed(embed: discord.Embed, *, footer: str = "Babblebox | /help or bb!help") -> discord.Embed:
    if embed.timestamp is None:
        embed.timestamp = now_utc()
    embed.set_footer(text=footer)
    return embed


def make_status_embed(
    title: str,
    description: str,
    *,
    tone: str = "info",
    footer: str = "Babblebox | /help or bb!help",
) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=description,
        color=EMBED_THEME.get(tone, EMBED_THEME["info"]),
    )
    return style_embed(embed, footer=footer)


def format_timestamp(value, style="R"):
    if value is None:
        return "Unknown"
    return f"<t:{int(value.timestamp())}:{style}>"


def sanitize_afk_reason(reason):
    return sanitize_short_plain_text(
        reason,
        field_name="AFK reason",
        max_length=AFK_REASON_MAX_LEN,
        sentence_limit=AFK_REASON_SENTENCE_LIMIT,
        reject_blocklist=True,
        allow_empty=True,
    )



def build_afk_status_embed(user, record, *, title=None):
    status = record.get("status", "active")
    embed = discord.Embed(
        title=title or ("⏰ AFK Scheduled" if status == "scheduled" else "💤 AFK Enabled"),
        color=discord.Color.orange(),
        description=f"**{display_name_of(user)}**",
    )

    if record.get("reason"):
        embed.add_field(name="Reason", value=safe_field_text(record["reason"], limit=512), inline=False)

    timing_lines = []
    if status == "scheduled":
        timing_lines.append(f"Starts: {format_timestamp(record.get('starts_at'), 'R')} ({format_timestamp(record.get('starts_at'), 'f')})")
    else:
        timing_lines.append(f"Since: {format_timestamp(record.get('set_at') or record.get('starts_at'), 'R')}")

    if record.get("ends_at"):
        timing_lines.append(f"Auto-clear: {format_timestamp(record['ends_at'], 'R')} ({format_timestamp(record['ends_at'], 'f')})")

    embed.add_field(name="Timing", value="\n".join(timing_lines), inline=False)


    return style_embed(embed, footer="Babblebox AFK | AFK clears on your next message. Timed away notices live here now.")


def build_afk_brief_line(user, record):
    line = f"**{display_name_of(user)}** is AFK"
    if record.get("reason"):
        line += f" — {record['reason']}"
    if record.get("ends_at"):
        line += f" • back {format_timestamp(record['ends_at'], 'R')}"
    else:
        since_value = record.get("set_at") or record.get("starts_at")
        if since_value is not None:
            line += f" • since {format_timestamp(since_value, 'R')}"
    return line
async def cancel_task(task):
    if task is None or task.done() or task is asyncio.current_task():
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def safe_add_reaction(message, emoji):
    with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
        await message.add_reaction(emoji)


async def safe_send_interaction(interaction, *args, **kwargs):
    try:
        if interaction.response.is_done():
            return await interaction.followup.send(*args, **kwargs)
        return await interaction.response.send_message(*args, **kwargs)
    except discord.InteractionResponded:
        with contextlib.suppress(discord.NotFound, discord.HTTPException):
            return await interaction.followup.send(*args, **kwargs)
        return None
    except (discord.NotFound, discord.HTTPException):
        return None

async def safe_edit_interaction_message(interaction, **kwargs):
    try:
        if interaction.response.is_done():
            await interaction.edit_original_response(**kwargs)
        else:
            await interaction.response.edit_message(**kwargs)
        return True
    except discord.InteractionResponded:
        try:
            await interaction.edit_original_response(**kwargs)
            return True
        except (discord.NotFound, discord.HTTPException):
            pass
    except (discord.NotFound, discord.HTTPException):
        pass

    message = getattr(interaction, "message", None)
    if message is not None:
        with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
            await message.edit(**kwargs)
            return True
    return False

async def disable_view(view):
    for child in view.children:
        child.disabled = True

    if getattr(view, "message", None) is not None:
        with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
            await view.message.edit(view=view)

    unregister_view(view.guild_id, view)
    view.stop()


async def cleanup_game(guild_id):
    game = games.get(guild_id)
    if not game:
        return

    if game.get("closing"):
        return

    game["closing"] = True
    game["active"] = False
    game["voting_active"] = False

    await cancel_task(game.get("turn_task"))
    await cancel_task(game.get("idle_task"))
    await cancel_task(game.get("vote_task"))

    for view in list(game.get("views", [])):
        await disable_view(view)

    seen_ids = set()
    for roster_name in ("players", "starting_players"):
        for player in list(game.get(roster_name, [])):
            if player.id in seen_ids:
                continue
            seen_ids.add(player.id)
            release_dm_route(player.id)

    game["first_audio"] = None
    game["final_audio"] = None

    async with games_guard:
        current = games.get(guild_id)
        if current is game:
            games.pop(guild_id, None)

def format_permission_list(permission_names):
    return "\n".join(
        f"• {PERMISSION_LABELS.get(name, name.replace('_', ' ').title())}"
        for name in permission_names
    )

async def send_interaction_ephemeral(interaction: discord.Interaction, **kwargs):
    if interaction.response.is_done():
        await interaction.followup.send(ephemeral=True, **kwargs)
    else:
        await interaction.response.send_message(ephemeral=True, **kwargs)

async def require_bot_permissions(
    interaction: discord.Interaction,
    required_permissions,
    command_name: str
) -> bool:
    runtime_bot = get_runtime_bot()
    if interaction.guild is None or interaction.channel is None or runtime_bot is None or runtime_bot.user is None:
        return True

    me = interaction.guild.me or interaction.guild.get_member(runtime_bot.user.id)
    if me is None:
        return True

    channel_perms = interaction.channel.permissions_for(me)
    missing = [
        PERMISSION_LABELS.get(name, name.replace("_", " ").title())
        for name in required_permissions
        if not getattr(channel_perms, name, False)
    ]

    if not missing:
        return True

    embed = discord.Embed(
        title="⚠️ Missing Bot Permission(s)",
        description=(
            f"I can’t run **{command_name}** in this channel because I’m missing:\n"
            + "\n".join(f"• {perm}" for perm in missing)
        ),
        color=discord.Color.orange(),
    )
    embed.add_field(
        name="How to fix it",
        value="Please restore those permissions for me in this channel or category, then try again.",
        inline=False,
    )

    await send_interaction_ephemeral(interaction, embed=embed)
    return False

def get_lobby_embed(guild_id):
    game = games.get(guild_id)
    if not game:
        return discord.Embed(
            title="❌ Lobby Closed",
            description="Start a new game with `/play` or `bb!play`.",
            color=discord.Color.red(),
        )

    players = game["players"]
    gt = game.get("game_type", "none")
    host = game["host"]

    titles = {
        "none": ("🎮 Babblebox Menu", "Select a mini-game from the dropdown below!", discord.Color.dark_theme()),
        "telephone": ("🎙️ Broken Telephone", "Voice mimicry game! (3+ players)", discord.Color.blue()),
        "corpse": ("📝 Exquisite Corpse", "Absurd collaborative story! (3+ players)", discord.Color.purple()),
        "spyfall": ("🕵️ Spyfall", "Find the spy among you! (3+ players)", discord.Color.dark_gray()),
        "bomb": ("💣 Word Bomb", "Battle Royale typing game! (2+ players)", discord.Color.red()),
    }

    title, desc, color = titles.get(gt, titles["none"])
    embed = discord.Embed(title=title, description=desc, color=color)
    embed.set_footer(text=f"Hosted by {host.display_name} | Use /stop or bb!stop to cancel | /help or bb!help for rules")
    embed.add_field(name="Chaos Card", value=build_chaos_card_line(game), inline=False)

    if gt == "bomb":
        config = get_bomb_mode_config(game.get("bomb_mode", "classic"))
        embed.add_field(
            name="Bomb Mode",
            value=f"**{config['label']}** — {config['description']}",
            inline=False,
        )

    if gt != "none":
        if not players:
            embed.add_field(
                name="Players Lobby",
                value=(
                    "No players yet. Click **Join** to open the room.\n"
                    "If the server is quiet right now, try `/daily`, `/profile`, `/buddy`, or `/later` while you wait."
                ),
                inline=False,
            )
        else:
            players_list = "\n".join(f"**{i + 1}.** 🎮 {p.display_name}" for i, p in enumerate(players))
            embed.add_field(name=f"Players Lobby ({len(players)}/{MAX_PLAYERS})", value=players_list, inline=False)

        if gt != "none":
            min_players = 2 if gt == "bomb" else 3
            embed.add_field(
                name="Start Guide",
                value=(
                    f"Minimum players: **{min_players}**\n"
                    "Host picks the game, everyone joins, then the host starts when the room feels ready.\n"
                    "Need a solo fallback? `/daily` and `/profile` are always available."
                ),
                inline=False,
            )

    return style_embed(embed, footer=f"Babblebox Lobby | Hosted by {host.display_name}")


def reset_idle_timer(guild_id):
    game = games.get(guild_id)
    if not game or game.get("closing"):
        return
    if game.get("idle_task") and not game["idle_task"].done():
        game["idle_task"].cancel()
    game["idle_task"] = asyncio.create_task(idle_timeout(guild_id, game))


# ==========================================
# VIEW BASE CLASSES
# ==========================================
class TrackedView(discord.ui.View):
    def __init__(self, guild_id, *, timeout):
        super().__init__(timeout=timeout)
        self.guild_id = guild_id
        self.message = None

    async def on_timeout(self):
        await disable_view(self)

    async def on_error(self, interaction, error, item):
        print(f"View error in guild {self.guild_id}: {error}")
        traceback.print_exception(type(error), error, error.__traceback__)
        with contextlib.suppress(discord.HTTPException):
            if interaction.response.is_done():
                await interaction.followup.send("❌ Something went wrong. The game state may have been reset.", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Something went wrong. The game state may have been reset.", ephemeral=True)


class GameSelect(discord.ui.Select):
    def __init__(self, guild_id):
        options = [
            discord.SelectOption(label="Broken Telephone", description="Voice mimicry game", emoji="🎙️", value="telephone"),
            discord.SelectOption(label="Exquisite Corpse", description="Absurd collaborative story", emoji="📝", value="corpse"),
            discord.SelectOption(label="Spyfall", description="Find the spy among you", emoji="🕵️", value="spyfall"),
            discord.SelectOption(label="Word Bomb", description="Battle Royale typing game", emoji="💣", value="bomb"),
        ]
        super().__init__(placeholder="Host, choose a game...", min_values=1, max_values=1, options=options)
        self.guild_id = guild_id

    async def callback(self, interaction):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)
            if interaction.user.id != game["host"].id:
                return await safe_send_interaction(interaction, "❌ Only the Host can pick the game!", ephemeral=True)

            game["game_type"] = self.values[0]
            game["waiting_for_guess"] = False
            game["corpse_answers"] = []
            game["theme"] = ""
            game["corpse_step"] = 0
            game["votes"] = {}
            game["voting_active"] = False

            if isinstance(self.view, LobbyView):
                self.view.refresh_components()

            ok = await safe_edit_interaction_message(interaction, embed=get_lobby_embed(self.guild_id), view=self.view)
            if not ok:
                await cleanup_game(self.guild_id)


class BombModeButton(discord.ui.Button):
    def __init__(self, guild_id):
        super().__init__(label="Bomb Mode: Classic", style=discord.ButtonStyle.secondary, row=2)
        self.guild_id = guild_id
        self.refresh()

    def refresh(self):
        game = games.get(self.guild_id)
        mode = game.get("bomb_mode", "classic") if game else "classic"
        config = get_bomb_mode_config(mode)
        self.label = f"Bomb Mode: {config['label']}"
        self.disabled = not game or game.get("game_type") != "bomb" or game.get("active") or game.get("closing")

    async def callback(self, interaction):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)
            if interaction.user.id != game["host"].id:
                return await safe_send_interaction(interaction, "❌ Only the Host can change bomb mode!", ephemeral=True)
            if game.get("game_type") != "bomb":
                return await safe_send_interaction(interaction, "❌ Select Word Bomb first, then choose a mode.", ephemeral=True)

            game["bomb_mode"] = get_next_bomb_mode(game.get("bomb_mode", "classic"))
            if isinstance(self.view, LobbyView):
                self.view.refresh_components()

            ok = await safe_edit_interaction_message(interaction, embed=get_lobby_embed(self.guild_id), view=self.view)
            if not ok:
                await cleanup_game(self.guild_id)
                return

            config = get_bomb_mode_config(game["bomb_mode"])
            await interaction.followup.send(
                f"💣 Bomb mode set to **{config['label']}** — {config['description']}",
                ephemeral=True,
            )


class ChaosCardButton(discord.ui.Button):
    def __init__(self, guild_id):
        super().__init__(label="Chaos Card: Off", style=discord.ButtonStyle.secondary, row=2)
        self.guild_id = guild_id
        self.refresh()

    def refresh(self):
        game = games.get(self.guild_id)
        card = get_chaos_card_config(game.get("chaos_card", "none") if game else "none")
        self.label = f"Chaos Card: {card['label']}"
        self.disabled = not game or game.get("active") or game.get("closing")

    async def callback(self, interaction):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "This lobby is closed.", ephemeral=True)
            if interaction.user.id != game["host"].id:
                return await safe_send_interaction(interaction, "Only the host can change the Chaos Card.", ephemeral=True)

            game["chaos_card"] = get_next_chaos_card(game.get("chaos_card", "none"))
            if isinstance(self.view, LobbyView):
                self.view.refresh_components()

            ok = await safe_edit_interaction_message(interaction, embed=get_lobby_embed(self.guild_id), view=self.view)
            if not ok:
                await cleanup_game(self.guild_id)
                return

            card = get_chaos_card_config(game["chaos_card"])
            await interaction.followup.send(
                f"Chaos Card set to **{card['label']}** — {card['description']}",
                ephemeral=True,
            )


class LobbyView(TrackedView):
    def __init__(self, guild_id):
        super().__init__(guild_id, timeout=900)
        self.game_select = GameSelect(guild_id)
        self.bomb_mode_button = BombModeButton(guild_id)
        self.chaos_card_button = ChaosCardButton(guild_id)
        self.add_item(self.game_select)
        self.add_item(self.bomb_mode_button)
        self.add_item(self.chaos_card_button)
        self.refresh_components()

    def refresh_components(self):
        self.bomb_mode_button.refresh()
        self.chaos_card_button.refresh()

    async def on_timeout(self):
        game = games.get(self.guild_id)
        await super().on_timeout()
        if game and not game.get("active") and not game.get("closing"):
            with contextlib.suppress(discord.HTTPException):
                await game["channel"].send("⌛ The lobby expired after 15 minutes of inactivity.")
            await cleanup_game(self.guild_id)

    @discord.ui.button(label="Join", style=discord.ButtonStyle.green, row=1)
    async def join_callback(self, interaction, button):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

            if is_player_in_game(game, interaction.user.id):
                return await safe_send_interaction(interaction, "You are already in!", ephemeral=True)
            if len(game["players"]) >= MAX_PLAYERS:
                return await safe_send_interaction(interaction, f"❌ This lobby is full ({MAX_PLAYERS} players max).", ephemeral=True)

            game["players"].append(interaction.user)
            ok = await safe_edit_interaction_message(interaction, embed=get_lobby_embed(self.guild_id), view=self)
            if not ok:
                await cleanup_game(self.guild_id)

    @discord.ui.button(label="Leave", style=discord.ButtonStyle.red, row=1)
    async def leave_callback(self, interaction, button):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)
            if not is_player_in_game(game, interaction.user.id):
                return await safe_send_interaction(interaction, "You are not in the game!", ephemeral=True)

            game["players"] = [player for player in game["players"] if player.id != interaction.user.id]
            ok = await safe_edit_interaction_message(interaction, embed=get_lobby_embed(self.guild_id), view=self)
            if not ok:
                await cleanup_game(self.guild_id)

    @discord.ui.button(label="Start Game", style=discord.ButtonStyle.blurple, row=1)
    async def start_callback(self, interaction, button):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or game.get("active"):
            return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or game.get("active"):
                return await safe_send_interaction(interaction, "❌ This lobby is closed.", ephemeral=True)
            if interaction.user.id != game["host"].id:
                return await safe_send_interaction(interaction, "❌ Only the Host can start the game!", ephemeral=True)
            if game["game_type"] == "none":
                return await safe_send_interaction(interaction, "Select a game from the dropdown first so the lobby knows what to launch.", ephemeral=True)
            runtime_bot = get_runtime_bot()
            if game["game_type"] == "bomb" and (runtime_bot is None or not getattr(runtime_bot, "dictionary_ready", False)):
                return await safe_send_interaction(interaction, "Word Bomb is unavailable right now because the dictionary did not finish loading.", ephemeral=True)

            min_players = 2 if game["game_type"] == "bomb" else 3
            if len(game["players"]) < min_players:
                return await safe_send_interaction(
                    interaction,
                    (
                        f"You need at least {min_players} players for this game. "
                        "If the server is quiet, try `/daily`, `/profile`, or `/buddy` while you gather a crew."
                    ),
                    ephemeral=True,
                )

            for child in self.children:
                child.disabled = True
            ok = await safe_edit_interaction_message(interaction, view=self)
            if not ok:
                await cleanup_game(self.guild_id)
                return

            unregister_view(self.guild_id, self)
            self.stop()

            random.shuffle(game["players"])
            apply_chaos_card(game)
            game["starting_players"] = list(game["players"])
            game["active"] = True
            game["current_player_index"] = 0
            game["turn_token"] = 0
            game["vote_token"] = 0
            game["waiting_for_guess"] = False
            game["votes"] = {}
            game["voting_active"] = False
            game["interrogation_log"] = []
            game["corpse_contributions"] = []
            game["bomb_word_history"] = []
            game["bomb_eliminations"] = []
            game["bomb_modifier_log"] = []
            game["bomb_current_rule"] = None
            game["bomb_current_turn_time_limit"] = None
            game["bomb_turn_started_at"] = None
            game["result_recorded"] = False

            gt = game["game_type"]

            if gt == "spyfall":
                game["location"] = random.choice(SPYFALL_LOCATIONS)
                game["spy"] = random.choice(game["players"])
                loc_list_str = "\n".join(SPYFALL_LOCATIONS)

                for player in game["players"]:
                    try:
                        if player.id == game["spy"].id:
                            await player.send(
                                "🕵️ **YOU ARE THE SPY!**\n"
                                "Blend in by answering vaguely.\n"
                                f"**Locations:**\n{loc_list_str}"
                            )
                        else:
                            await player.send(
                                f"📍 **Location:** {game['location']}\n"
                                "🕵️ **Goal:** Find the spy!\n"
                                "💡 **Tip:** Ask questions like: *'Are we wearing uniforms?'* or *'Is it hot here?'*\n"
                                f"**Locations:**\n{loc_list_str}"
                            )
                    except Exception:
                        with contextlib.suppress(discord.HTTPException):
                            await game["channel"].send(f"❌ Cannot DM {player.mention}. Game cancelled.")
                        await cleanup_game(self.guild_id)
                        return

                first_player = game["players"][0]
                embed = discord.Embed(title="🕵️ Spyfall Started!", color=discord.Color.dark_gray())
                embed.add_field(
                    name="Waiting on:",
                    value=f"⚠️ **{first_player.mention}**, start the game by selecting a target below!",
                    inline=False,
                )
                dashboard = SpyfallDashboard(self.guild_id)
                dashboard_message = await game["channel"].send(embed=embed, view=dashboard)
                register_view(self.guild_id, dashboard, dashboard_message)
                mark_game_started(game)
                reset_idle_timer(self.guild_id)
                return

            if gt == "bomb":
                config = get_bomb_mode_config(game.get("bomb_mode", "classic"))
                game["used_words"] = set()
                game["time_limit"] = config["start_time"]
                game["bomb_current_turn_time_limit"] = config["start_time"]
                game["bomb_warning_enabled"] = config["warning"]
                game["bomb_speed_every"] = config["speed_every"]
                game["bomb_minimum_time"] = config["minimum_time"]
                game["turn_count"] = 0

                start_embed = discord.Embed(
                    title="💣 BATTLE ROYALE BOMB STARTED!",
                    description=(
                        "Type a single, real English word containing the syllable to survive.\n"
                        f"Mode: **{config['label']}** — {config['description']}"
                    ),
                    color=discord.Color.red(),
                )
                if game.get("chaos_card") != "none":
                    start_embed.add_field(name="Chaos Card", value=build_chaos_card_line(game), inline=False)
                await game["channel"].send(embed=start_embed)
                mark_game_started(game)
                await _start_bomb_turn_locked(self.guild_id, game)
                return

            shuffled_list = "\n".join(f"**{i + 1}.** {player.display_name}" for i, player in enumerate(game["players"]))
            start_embed = discord.Embed(title="🚀 Game Started!", description="Check your DMs.", color=discord.Color.gold())
            start_embed.add_field(name="Turn Order:", value=shuffled_list, inline=False)
            if game.get("chaos_card") != "none":
                start_embed.add_field(name="Chaos Card", value=build_chaos_card_line(game), inline=False)
            await game["channel"].send(embed=start_embed)

            first_player = game["players"][0]
            if gt == "telephone":
                if await _prompt_telephone_first_player_locked(self.guild_id, game, first_player):
                    mark_game_started(game)
            elif gt == "corpse":
                game["theme"] = random.choice(THEMES)
                game["corpse_answers"] = []
                game["corpse_step"] = 0
                if await _prompt_corpse_player_locked(self.guild_id, game, first_player):
                    mark_game_started(game)


class SpyfallTargetSelect(discord.ui.Select):
    def __init__(self, players, current_player, guild_id):
        options = [
            discord.SelectOption(label=player.display_name, value=str(player.id))
            for player in players
            if player.id != current_player.id
        ]
        if not options:
            options = [discord.SelectOption(label="No valid targets", value="0")]
        super().__init__(placeholder="Select your target...", min_values=1, max_values=1, options=options)
        self.guild_id = guild_id

    async def callback(self, interaction):
        game = games.get(self.guild_id)
        if not game or game.get("closing") or not game.get("active"):
            return await safe_send_interaction(interaction, "❌ No active Spyfall game.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or not game.get("active"):
                return await safe_send_interaction(interaction, "❌ No active Spyfall game.", ephemeral=True)
            if game.get("voting_active"):
                return await safe_send_interaction(interaction, "❌ A vote is already in progress!", ephemeral=True)

            current_player = get_current_player(game)
            if not current_player or interaction.user.id != current_player.id:
                return await safe_send_interaction(interaction, "❌ It's not your turn!", ephemeral=True)

            target_id = int(self.values[0])
            target_player = get_player_by_id(game, target_id)
            if target_player is None:
                return await safe_send_interaction(interaction, "❌ That player is no longer available.", ephemeral=True)

            previous_player = current_player
            game["interrogation_log"].append({"from_id": previous_player.id, "to_id": target_player.id})
            game["current_player_index"] = game["players"].index(target_player)
            reset_idle_timer(self.guild_id)

            new_dashboard = SpyfallDashboard(self.guild_id)
            old_view = self.view if isinstance(self.view, SpyfallDashboard) else None

            embed = discord.Embed(title="🕵️ Spyfall: Interrogation Phase", color=discord.Color.dark_gray())
            embed.add_field(
                name="Waiting on:",
                value=f"⚠️ **{target_player.mention}**, it is YOUR turn! Answer the question, then use the menu to pick the next target.",
                inline=False,
            )
            ok = await safe_edit_interaction_message(interaction, embed=embed, view=new_dashboard)
            if not ok:
                await cleanup_game(self.guild_id)
                return

            register_view(self.guild_id, new_dashboard, interaction.message)
            if old_view is not None and old_view is not new_dashboard:
                old_view.message = None
                unregister_view(self.guild_id, old_view)
                old_view.stop()

            with contextlib.suppress(discord.HTTPException):
                await interaction.channel.send(
                    f"🗣️ **{target_player.mention}**, you are being interrogated by **{previous_player.mention}**!\n"
                    "Answer, then pick your target in the panel above."
                )

class SpyfallVoteButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Call Vote ⚖️", style=discord.ButtonStyle.danger, row=1)

    async def callback(self, interaction):
        await trigger_spyfall_vote(interaction)


class SpyfallDashboard(TrackedView):
    def __init__(self, guild_id):
        super().__init__(guild_id, timeout=None)
        self.rebuild()

    def rebuild(self):
        self.clear_items()
        game = games.get(self.guild_id)
        if not game or game.get("closing") or not game.get("active") or not game.get("players"):
            return
        current_player = get_current_player(game)
        if current_player and len(game["players"]) > 1:
            self.add_item(SpyfallTargetSelect(game["players"], current_player, self.guild_id))
        self.add_item(SpyfallVoteButton())


class SpyfallVoteSelect(discord.ui.Select):
    def __init__(self, players, guild_id):
        options = [discord.SelectOption(label=player.display_name, value=str(player.id)) for player in players]
        game = games.get(guild_id)
        vote_timeout = game.get("spyfall_vote_timeout", SPYFALL_VOTE_TIMEOUT_SECONDS) if game else SPYFALL_VOTE_TIMEOUT_SECONDS
        super().__init__(placeholder=f"Vote for the Spy ({vote_timeout}s)...", min_values=1, max_values=1, options=options)
        self.guild_id = guild_id

    async def callback(self, interaction):
        game = games.get(self.guild_id)
        if not game or game.get("closing"):
            return await safe_send_interaction(interaction, "❌ Voting is closed!", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing") or not game.get("voting_active"):
                return await safe_send_interaction(interaction, "❌ Voting is closed!", ephemeral=True)
            if not is_player_in_game(game, interaction.user.id):
                return await safe_send_interaction(interaction, "You aren't playing!", ephemeral=True)
            if interaction.user.id in game["votes"]:
                return await safe_send_interaction(interaction, "You already voted!", ephemeral=True)

            target_id = int(self.values[0])
            if not get_player_by_id(game, target_id):
                return await safe_send_interaction(interaction, "❌ That player is no longer available.", ephemeral=True)

            game["votes"][interaction.user.id] = target_id
            vote_count = len(game["votes"])
            total_players = len(game["players"])

            await safe_send_interaction(interaction, "✅ Your vote is locked.", ephemeral=True)
            with contextlib.suppress(discord.HTTPException):
                await interaction.channel.send(
                    f"🗳️ **{interaction.user.display_name}** has cast their vote! ({vote_count}/{total_players})"
                )

            if vote_count >= total_players:
                await _process_spyfall_votes_locked(self.guild_id, interaction.channel, game)


class SpyfallVoteView(TrackedView):
    def __init__(self, guild_id):
        game = games.get(guild_id)
        timeout = game.get("spyfall_vote_timeout", SPYFALL_VOTE_TIMEOUT_SECONDS) if game else SPYFALL_VOTE_TIMEOUT_SECONDS
        super().__init__(guild_id, timeout=timeout)
        if game:
            self.add_item(SpyfallVoteSelect(game["players"], guild_id))


class ResignViewEnd(TrackedView):
    def __init__(self, guild_id):
        game = games.get(guild_id)
        timeout = game.get("dm_turn_timeout", TURN_TIMEOUT_SECONDS) if game else TURN_TIMEOUT_SECONDS
        super().__init__(guild_id, timeout=timeout)

    @discord.ui.button(label="Resign 🏳️", style=discord.ButtonStyle.red)
    async def resign_callback(self, interaction, button):
        game = games.get(self.guild_id)
        if not game or game.get("closing"):
            return await safe_send_interaction(interaction, "❌ This game is already closed.", ephemeral=True)

        async with game["lock"]:
            game = games.get(self.guild_id)
            if not game or game.get("closing"):
                return await safe_send_interaction(interaction, "❌ This game is already closed.", ephemeral=True)

            with contextlib.suppress(discord.NotFound, discord.HTTPException):
                if interaction.response.is_done():
                    await interaction.edit_original_response(view=None)
                else:
                    await interaction.response.edit_message(view=None)
            await _finish_telephone_locked(self.guild_id, game, "*Resigned / No idea* 🏳️")


# ==========================================
# TIMERS
# ==========================================
async def idle_timeout(guild_id, game_ref):
    try:
        await asyncio.sleep(IDLE_TIMEOUT_SECONDS)
    except asyncio.CancelledError:
        return

    game = games.get(guild_id)
    if game is not game_ref or game.get("closing") or not game.get("active"):
        return

    async with game["lock"]:
        if games.get(guild_id) is not game_ref or game_ref.get("closing") or not game_ref.get("active"):
            return
        with contextlib.suppress(discord.HTTPException):
            await game_ref["channel"].send(
                embed=discord.Embed(
                    title="💤 Game Abandoned",
                    description="No activity for 10 minutes. The game has been closed.",
                    color=discord.Color.dark_grey(),
                )
            )
        await cleanup_game(guild_id)


async def spyfall_vote_timeout(guild_id, vote_token, game_ref):
    try:
        await asyncio.sleep(game_ref.get("spyfall_vote_timeout", SPYFALL_VOTE_TIMEOUT_SECONDS))
    except asyncio.CancelledError:
        return

    game = games.get(guild_id)
    if game is not game_ref or game.get("closing"):
        return

    async with game["lock"]:
        if games.get(guild_id) is not game_ref:
            return
        if game_ref.get("closing") or not game_ref.get("voting_active"):
            return
        if game_ref.get("vote_token") != vote_token:
            return
        with contextlib.suppress(discord.HTTPException):
            await game_ref["channel"].send("🚨 **Voting Time is UP! Tallying votes...**")
        await _process_spyfall_votes_locked(guild_id, game_ref["channel"], game_ref)


async def dm_timeout(guild_id, player_id, turn_token, game_ref):
    try:
        await asyncio.sleep(game_ref.get("dm_turn_timeout", TURN_TIMEOUT_SECONDS))
    except asyncio.CancelledError:
        return

    game = games.get(guild_id)
    if game is not game_ref or game.get("closing"):
        return

    async with game["lock"]:
        if games.get(guild_id) is not game_ref:
            return
        if game_ref.get("closing") or not game_ref.get("active"):
            return
        if game_ref.get("turn_token") != turn_token:
            return

        current_player = get_current_player(game_ref)
        if not current_player or current_player.id != player_id:
            return

        release_dm_route(player_id, guild_id)

        if game_ref.get("game_type") == "telephone" and game_ref.get("waiting_for_guess"):
            await _finish_telephone_locked(guild_id, game_ref, "*Ran out of time!* ⏰")
            return

        timed_out_player = get_player_by_id(game_ref, player_id)
        if timed_out_player:
            with contextlib.suppress(discord.HTTPException):
                await timed_out_player.send("⏳ Time's up! The game was cancelled.")
            with contextlib.suppress(discord.HTTPException):
                await game_ref["channel"].send(
                    embed=discord.Embed(
                        title="⏰ Time's Up!",
                        description=f"{timed_out_player.mention} took too long in DMs. Game over!",
                        color=discord.Color.red(),
                    )
                )
        await cleanup_game(guild_id)


async def bomb_timeout(guild_id, player_id, turn_token, game_ref):
    try:
        time_limit = game_ref.get("bomb_current_turn_time_limit", game_ref["time_limit"])
        warning_enabled = game_ref.get("bomb_warning_enabled", True)
        if warning_enabled and time_limit > 5:
            await asyncio.sleep(time_limit - 5)
            game = games.get(guild_id)
            if game is not game_ref or game.get("closing"):
                return
            async with game["lock"]:
                if games.get(guild_id) is not game_ref:
                    return
                if game_ref.get("closing") or not game_ref.get("active"):
                    return
                if game_ref.get("turn_token") != turn_token:
                    return
                current_player = get_current_player(game_ref)
                if not current_player or current_player.id != player_id:
                    return
                with contextlib.suppress(discord.HTTPException):
                    await game_ref["channel"].send(
                        f"⚠️ **5 SECONDS LEFT, {current_player.mention}!**",
                        delete_after=4.0,
                    )
            await asyncio.sleep(5)
        else:
            await asyncio.sleep(time_limit)
    except asyncio.CancelledError:
        return

    game = games.get(guild_id)
    if game is not game_ref or game.get("closing"):
        return

    async with game["lock"]:
        if games.get(guild_id) is not game_ref:
            return
        if game_ref.get("closing") or not game_ref.get("active"):
            return
        if game_ref.get("turn_token") != turn_token:
            return

        player = get_player_by_id(game_ref, player_id)
        current_player = get_current_player(game_ref)
        if not player or not current_player or current_player.id != player_id:
            return

        with contextlib.suppress(discord.HTTPException):
            await game_ref["channel"].send(
                embed=discord.Embed(
                    title="💥 BOOM!",
                    description=f"{player.mention} exploded and is eliminated!",
                    color=discord.Color.dark_red(),
                )
            )

        game_ref["bomb_eliminations"].append({"player_id": player.id, "name": display_name_of(player)})
        game_ref["players"] = [p for p in game_ref["players"] if p.id != player_id]
        if game_ref["current_player_index"] >= len(game_ref["players"]) and game_ref["players"]:
            game_ref["current_player_index"] = 0

        await _start_bomb_turn_locked(guild_id, game_ref)


# ==========================================
# GAME LOGIC
# ==========================================
async def _prompt_dm_player_locked(guild_id, game, player, content, *, file=None, view=None, delete_after=None):
    if not claim_dm_route(guild_id, player.id):
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(
                f"❌ {player.mention} is already taking a DM turn in another server. Game cancelled."
            )
        await cleanup_game(guild_id)
        return False

    try:
        dm_message = await player.send(content, file=file, view=view, delete_after=delete_after)
    except Exception:
        release_dm_route(player.id, guild_id)
        if view:
            view.stop()
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(f"❌ Cannot DM {player.mention}. Game cancelled.")
        await cleanup_game(guild_id)
        return False

    if view is not None:
        register_view(guild_id, view, dm_message)

    await cancel_task(game.get("turn_task"))
    turn_token = bump_token(game, "turn_token")
    game["turn_task"] = asyncio.create_task(dm_timeout(guild_id, player.id, turn_token, game))
    return True


async def _prompt_telephone_first_player_locked(guild_id, game, player):
    game["waiting_for_guess"] = False
    game["current_player_index"] = 0
    timeout = game.get("dm_turn_timeout", TURN_TIMEOUT_SECONDS)
    return await _prompt_dm_player_locked(
        guild_id,
        game,
        player,
        f"You are the **FIRST** player! 🎙️\nRecord a VOICE message. You have {timeout}s!",
    )


async def _prompt_corpse_player_locked(guild_id, game, player):
    step = game["corpse_step"]
    idx = game["players"].index(player)
    game["current_player_index"] = idx
    timeout = game.get("dm_turn_timeout", TURN_TIMEOUT_SECONDS)
    return await _prompt_dm_player_locked(
        guild_id,
        game,
        player,
        f"🎭 **Exquisite Corpse** started!\n🎬 **Theme:** {game['theme']}\n\n"
        f"**Step {step + 1} of 6**\n{CORPSE_PROMPTS[step]}\n"
        f"*({timeout}s to reply - any word works, but follow the prompt for best results!)*",
    )


async def _finish_telephone_locked(guild_id, game, guess_text):
    last_player = game["players"][-1] if game.get("players") else None
    if last_player is not None:
        with contextlib.suppress(discord.HTTPException):
            await last_player.send("✅ Received! Returning to the server...")

    files = []
    if game.get("first_audio"):
        files.append(discord.File(io.BytesIO(game["first_audio"]), filename="1_Original.ogg"))
    if game.get("final_audio"):
        files.append(discord.File(io.BytesIO(game["final_audio"]), filename="2_Final_Mimic.ogg"))

    safe_guess = guess_text[:1000] + "..." if len(guess_text) > 1000 else guess_text
    embed = discord.Embed(title="🏁 The Broken Telephone has finished!", color=discord.Color.green())
    embed.add_field(name="🧠 The Final Guess", value=f"**{safe_guess}**", inline=False)

    mark_telephone_completion(game)
    recap_embed = build_telephone_recap_embed(game, safe_guess)

    try:
        if files:
            await game["channel"].send(embed=embed, files=files)
        else:
            await game["channel"].send(embed=embed)
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(embed=recap_embed)
    finally:
        await cleanup_game(guild_id)


async def finish_telephone(guild_id, guess_text):
    game = games.get(guild_id)
    if not game or game.get("closing"):
        return
    async with game["lock"]:
        game = games.get(guild_id)
        if not game or game.get("closing"):
            return
        await _finish_telephone_locked(guild_id, game, guess_text)


async def _finish_corpse_locked(guild_id, game):
    ans = game.get("corpse_answers", [])
    if len(ans) < 6:
        await cleanup_game(guild_id)
        return

    final_sentence = f"The **{ans[0]}** **{ans[1]}** **{ans[2]}** the **{ans[3]}** **{ans[4]}** — **{ans[5]}**."
    embed = discord.Embed(title="📝 Exquisite Corpse: The Masterpiece!", color=discord.Color.purple())
    embed.add_field(name=f"Theme: {game['theme']}", value=final_sentence, inline=False)

    mark_corpse_completion(game)
    recap_embed = build_corpse_recap_embed(game)

    try:
        await game["channel"].send(embed=embed)
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(embed=recap_embed)
    finally:
        await cleanup_game(guild_id)


async def finish_corpse(guild_id):
    game = games.get(guild_id)
    if not game or game.get("closing"):
        return
    async with game["lock"]:
        game = games.get(guild_id)
        if not game or game.get("closing"):
            return
        await _finish_corpse_locked(guild_id, game)


async def _start_bomb_turn_locked(guild_id, game):
    if not game.get("players"):
        await cleanup_game(guild_id)
        return

    if len(game["players"]) == 1:
        winner = game["players"][0]
        game["winner_name"] = display_name_of(winner)
        mark_bomb_win(game, winner)
        recap_embed = build_bomb_recap_embed(game, winner)
        with contextlib.suppress(discord.HTTPException):
            await game["channel"].send(
                embed=discord.Embed(
                    title="🏆 BOMB SURVIVOR!",
                    description=f"**{winner.mention} is the last one standing and wins the game!** 🎉",
                    color=discord.Color.gold(),
                )
            )
            await game["channel"].send(embed=recap_embed)
        await cleanup_game(guild_id)
        return

    game["current_player_index"] %= len(game["players"])
    next_player = game["players"][game["current_player_index"]]
    game["syllable"] = random.choice(BOMB_SYLLABLES)
    prepare_bomb_turn(game)
    game["bomb_turn_started_at"] = asyncio.get_running_loop().time()

    await cancel_task(game.get("turn_task"))
    turn_token = bump_token(game, "turn_token")

    with contextlib.suppress(discord.HTTPException):
        await game["channel"].send(embed=build_bomb_turn_embed(game, next_player))

    game["turn_task"] = asyncio.create_task(bomb_timeout(guild_id, next_player.id, turn_token, game))


async def _process_spyfall_votes_locked(guild_id, channel, game):
    if game.get("closing") or not game.get("voting_active"):
        return

    game["voting_active"] = False
    await cancel_task(game.get("vote_task"))

    if not game["votes"]:
        mark_spyfall_result(game, village_won=False)
        try:
            with contextlib.suppress(discord.HTTPException):
                await channel.send("⏳ Time is up! Nobody voted. The Spy escapes!")
                await channel.send(embed=build_spyfall_recap_embed(game))
        finally:
            await cleanup_game(guild_id)
        return

    vote_counts = {}
    for voted_for_id in game["votes"].values():
        vote_counts[voted_for_id] = vote_counts.get(voted_for_id, 0) + 1

    highest_votes = max(vote_counts.values())
    tied = list(vote_counts.values()).count(highest_votes) > 1

    if tied:
        village_won = False
        embed = discord.Embed(
            title="⚖️ Split Vote!",
            description=(
                "The village couldn't agree! It's a tie.\n\n"
                "💀 **SPY WINS!** They escaped!\n\n"
                f"The real Spy was {game['spy'].mention}.\n"
                f"Location: **{game['location']}**"
            ),
            color=discord.Color.red(),
        )
    else:
        accused_id = max(vote_counts, key=vote_counts.get)
        accused = get_player_by_id(game, accused_id)
        accused_mention = accused.mention if accused else f"<@{accused_id}>"
        village_won = accused is not None and accused.id == game["spy"].id

        embed = discord.Embed(title="⚖️ The Village has spoken!", color=discord.Color.red())
        embed.add_field(name="Executed:", value=accused_mention, inline=False)
        if village_won:
            embed.add_field(name="Result:", value="🎉 **VILLAGE WINS!** You caught the Spy!", inline=False)
        else:
            embed.add_field(
                name="Result:",
                value=(
                    "💀 **SPY WINS!** You executed an innocent!\n\n"
                    f"The real Spy was {game['spy'].mention}.\n"
                    f"Location: **{game['location']}**"
                ),
                inline=False,
            )

    mark_spyfall_result(game, village_won=village_won)
    recap_embed = build_spyfall_recap_embed(game)

    try:
        await channel.send(embed=embed)
        with contextlib.suppress(discord.HTTPException):
            await channel.send(embed=recap_embed)
    finally:
        await cleanup_game(guild_id)


async def trigger_spyfall_vote(interaction):
    if interaction.guild is None:
        return await safe_send_interaction(interaction, "❌ This command only works in a server.", ephemeral=True)

    guild_id = interaction.guild.id
    game = games.get(guild_id)
    if not game or game.get("closing"):
        return await safe_send_interaction(interaction, "❌ No active Spyfall game to vote on.", ephemeral=True)

    async with game["lock"]:
        game = games.get(guild_id)
        if not game or game.get("closing"):
            return await safe_send_interaction(interaction, "❌ No active Spyfall game to vote on.", ephemeral=True)
        if game.get("game_type") != "spyfall" or not game.get("active"):
            return await safe_send_interaction(interaction, "❌ No active Spyfall game to vote on.", ephemeral=True)
        if not is_player_in_game(game, interaction.user.id):
            return await safe_send_interaction(interaction, "❌ You are not playing!", ephemeral=True)
        if game.get("voting_active"):
            return await safe_send_interaction(interaction, "❌ A vote is already in progress!", ephemeral=True)

        game["voting_active"] = True
        game["votes"] = {}
        vote_token = bump_token(game, "vote_token")
        reset_idle_timer(guild_id)

        dashboard = get_live_view(game, SpyfallDashboard)
        if dashboard is not None:
            for child in dashboard.children:
                child.disabled = True
            if dashboard.message is not None:
                with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException):
                    await dashboard.message.edit(view=dashboard)
            unregister_view(guild_id, dashboard)
            dashboard.stop()

        vote_view = SpyfallVoteView(guild_id)
        vote_timeout = game.get("spyfall_vote_timeout", SPYFALL_VOTE_TIMEOUT_SECONDS)
        embed = discord.Embed(
            title="🚨 EMERGENCY MEETING",
            description=(
                f"{interaction.user.mention} called a vote!\n"
                f"Select who you think the spy is. You have **{vote_timeout} seconds**."
            ),
            color=discord.Color.red(),
        )
        content = f"Attention {build_ping_string(game['players'])}!"

        if interaction.response.is_done():
            vote_message = await interaction.followup.send(content=content, embed=embed, view=vote_view, wait=True)
        else:
            await interaction.response.send_message(content=content, embed=embed, view=vote_view)
            vote_message = await interaction.original_response()

        register_view(guild_id, vote_view, vote_message)
        await cancel_task(game.get("vote_task"))
        game["vote_task"] = asyncio.create_task(spyfall_vote_timeout(guild_id, vote_token, game))


# ==========================================
# TURN HANDLERS
# ==========================================
async def handle_bomb_turn_locked(message, guild_id, game):
    if not game.get("active"):
        return

    if len(message.content.split()) > 1:
        if can_emit_notice(game, "last_bomb_notice_at"):
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send("❌ Single words only!", delete_after=3.0)
        return

    word = message.content.strip().lower()
    syllable = game["syllable"].lower()

    if not word or syllable not in word:
        return

    modifier_error = validate_bomb_modifier(game, word)
    if modifier_error:
        if can_emit_notice(game, "last_bomb_notice_at"):
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(modifier_error, delete_after=3.0)
        return

    if word not in VALID_WORDS:
        if can_emit_notice(game, "last_bomb_notice_at"):
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(f"❌ '{word}' is not a valid English word!", delete_after=3.0)
        return

    if word in game["used_words"]:
        if can_emit_notice(game, "last_bomb_notice_at"):
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send(f"❌ '{word}' was already used!", delete_after=3.0)
        return

    elapsed = max(0.0, asyncio.get_running_loop().time() - game.get("bomb_turn_started_at", asyncio.get_running_loop().time()))

    await cancel_task(game.get("turn_task"))
    game["used_words"].add(word)
    game["bomb_word_history"].append(
        {
            "player_id": message.author.id,
            "word": word,
            "elapsed": elapsed,
            "time_limit": game.get("bomb_current_turn_time_limit", game["time_limit"]),
            "modifier": game.get("bomb_current_rule", {}).get("label") if game.get("bomb_current_rule") else None,
        }
    )
    record_bomb_word(message.author, word, elapsed)
    await safe_add_reaction(message, "✅")

    game["turn_count"] += 1
    if game["turn_count"] % game.get("bomb_speed_every", 5) == 0 and game["time_limit"] > game.get("bomb_minimum_time", 3.0):
        game["time_limit"] = max(game.get("bomb_minimum_time", 3.0), game["time_limit"] - 1.0)
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send(f"⚠️ **SPEED UP!** Timer is now {game['time_limit']:.1f}s!")

    game["current_player_index"] += 1
    await _start_bomb_turn_locked(guild_id, game)


async def handle_corpse_turn_locked(message, guild_id, game):
    if not message.content or message.attachments:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ Please send TEXT!")
        return

    safe_text = message.content.strip()[:100]
    if not safe_text:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ Please send TEXT!")
        return

    current_player = get_current_player(game)
    if current_player:
        release_dm_route(current_player.id, guild_id)

    await cancel_task(game.get("turn_task"))
    with contextlib.suppress(discord.HTTPException):
        await message.channel.send("✅ Saved!")

    game["corpse_contributions"].append({
        "player_id": current_player.id if current_player else message.author.id,
        "step": game["corpse_step"],
        "answer": safe_text,
    })
    game["corpse_answers"].append(safe_text)
    game["corpse_step"] += 1

    if game["corpse_step"] >= 6:
        await _finish_corpse_locked(guild_id, game)
        return

    next_idx = game["corpse_step"] % len(game["players"])
    next_player = game["players"][next_idx]
    await _prompt_corpse_player_locked(guild_id, game, next_player)


async def handle_telephone_turn_locked(message, guild_id, game):
    current_player = get_current_player(game)

    if game.get("waiting_for_guess"):
        if not message.content or message.attachments:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send("❌ Please TYPE your guess in text!")
            return

        guess = message.content.strip()
        if not guess:
            with contextlib.suppress(discord.HTTPException):
                await message.channel.send("❌ Please TYPE your guess in text!")
            return

        if current_player:
            release_dm_route(current_player.id, guild_id)
        await cancel_task(game.get("turn_task"))
        await _finish_telephone_locked(guild_id, game, guess)
        return

    if not message.attachments:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ Please send a VOICE message!")
        return

    attachment = message.attachments[0]
    is_audio = attachment.filename.lower().endswith(".ogg") or (
        attachment.content_type and "audio" in attachment.content_type.lower()
    )
    if not is_audio:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ Send a VOICE message!")
        return

    if attachment.size > MAX_VOICE_BYTES:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ Voice file too large. Keep it under 8 MB.")
        return

    try:
        audio_bytes = await attachment.read()
    except Exception:
        with contextlib.suppress(discord.HTTPException):
            await message.channel.send("❌ I couldn't read that audio file. Please try again.")
        return

    if current_player:
        release_dm_route(current_player.id, guild_id)
    await cancel_task(game.get("turn_task"))
    with contextlib.suppress(discord.HTTPException):
        await message.channel.send("✅ Voice message received!")

    if game["current_player_index"] == 0:
        game["first_audio"] = audio_bytes
    game["final_audio"] = audio_bytes
    game["current_player_index"] += 1

    if game["current_player_index"] == len(game["players"]) - 1:
        game["waiting_for_guess"] = True
        last_player = game["players"][-1]
        resign_view = ResignViewEnd(guild_id)
        audio_file = discord.File(io.BytesIO(audio_bytes), filename="voice.ogg")
        await _prompt_dm_player_locked(
            guild_id,
            game,
            last_player,
            "You are the **FINAL** player! 🎧\nListen and **TYPE your guess**!\n*(Audio self-destructs in 15s)*",
            file=audio_file,
            view=resign_view,
            delete_after=15.0,
        )
    else:
        next_player = game["players"][game["current_player_index"]]
        audio_file = discord.File(io.BytesIO(audio_bytes), filename="voice.ogg")
        await _prompt_dm_player_locked(
            guild_id,
            game,
            next_player,
            "Your turn (Mimic)! 🎙️\nRECORD your best mimic!\n*(Audio self-destructs in 15s)*",
            file=audio_file,
            delete_after=15.0,
        )


# ==========================================
# ==========================================
# HELP
# ==========================================
def build_help_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Babblebox Manual",
        description=(
            "Babblebox has four clear pillars: Party Games, Everyday Utilities, Daily Arcade, and Buddy/Profile."
        ),
        color=discord.Color.gold(),
    )
    embed.add_field(
        name="Party Games",
        value=(
            "`/play` or `bb!play` opens the lobby.\n"
            "Broken Telephone and Exquisite Corpse need 3+ players.\n"
            "Spyfall needs 3+ players and supports `/vote`.\n"
            "Word Bomb needs 2+ players and supports bomb modes plus Chaos Cards."
        ),
        inline=False,
    )
    embed.add_field(
        name="Everyday Utilities",
        value=(
            "`/watch mentions`, `/watch replies`, and `/watch keyword ...` split your alerts cleanly.\n"
            "`/later mark` to save your reading spot.\n"
            "`/capture` for a private channel snapshot.\n"
            "`/moment create`, `/moment from-reply`, or `/moment recent` make shareable cards.\n"
            "`/remind set` for one-time reminders.\n"
            "`/afk` and `/afkstatus` for away scheduling."
        ),
        inline=False,
    )
    embed.add_field(
        name="Daily Arcade",
        value=(
            "`/daily` opens today's three booths.\n"
            "`/daily play <guess>` still defaults to Shuffle Booth.\n"
            "`/daily play emoji <guess>` and `/daily play signal <guess>` open the other booths.\n"
            "`/daily share` and `/daily leaderboard` are public-friendly by default."
        ),
        inline=False,
    )
    embed.add_field(
        name="Buddy And Profile",
        value=(
            "`/buddy` opens your companion card.\n"
            "`/buddy rename`, `/buddy style`, and `/buddy stats` manage identity and progression.\n"
            "`/profile` is showable by default, while `/vault` stays more personal.\n"
            "Buddy, Daily Arcade, utilities, and multiplayer highlights all live in one compact product layer."
        ),
        inline=False,
    )
    embed.add_field(
        name="If You Are Solo",
        value="Babblebox is still useful when a lobby is not available. Try `/daily`, `/buddy`, `/profile`, `/moment recent`, `/remind`, or `/later`.",
        inline=False,
    )
    embed.add_field(
        name="Required Channel Permissions",
        value=format_permission_list(PLAY_REQUIRED_PERMS),
        inline=False,
    )
    embed.add_field(
        name="DM Requirement",
        value="Broken Telephone, Exquisite Corpse, Spyfall role messages, Watch alerts, Later markers, Capture, and DM reminders rely on open DMs.",
        inline=False,
    )
    return style_embed(embed, footer="Babblebox Manual | Party Games + Utilities + Daily Arcade + Buddy/Profile")
