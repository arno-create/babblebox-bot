from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date


DAILY_MAX_ATTEMPTS = 3
DAILY_START_DATE = date(2026, 1, 1)
DAILY_DEFAULT_MODE = "shuffle"
DAILY_MODE_ORDER = ("shuffle", "emoji", "signal")


@dataclass(frozen=True)
class DailyPuzzle:
    challenge_id: str
    mode: str
    label: str
    prompt_label: str
    puzzle_date: date
    challenge_number: int
    answer: str
    scramble: str
    hint: str
    length: int
    instructions: str
    share_flair: str


DAILY_MODE_META = {
    "shuffle": {
        "challenge_id": "daily_shuffle_v2",
        "label": "Shuffle Booth",
        "prompt_label": "Shuffle Tray",
        "instructions": "Unscramble the letters into one cozy one-word answer.",
        "share_flair": "🌀",
    },
    "emoji": {
        "challenge_id": "daily_emoji_v1",
        "label": "Emoji Booth",
        "prompt_label": "Emoji Trail",
        "instructions": "Read the emoji clue and name the matching one-word answer.",
        "share_flair": "✨",
    },
    "signal": {
        "challenge_id": "daily_signal_v1",
        "label": "Signal Booth",
        "prompt_label": "Signal Strip",
        "instructions": "Decode the shifted word and type the original answer.",
        "share_flair": "📻",
    },
}


WORD_BANK: tuple[tuple[str, str], ...] = (
    ("anchor", "keeps a ship from drifting"),
    ("arcade", "room full of buttons, cabinets, and high scores"),
    ("backpack", "what you carry class or travel stuff in"),
    ("bagel", "round breakfast bread with a hole"),
    ("blanket", "soft thing you steal on the couch"),
    ("boba", "tea drink with chewy pearls"),
    ("camera", "captures a moment"),
    ("candle", "small flame for light or scent"),
    ("canvas", "surface painters like to work on"),
    ("cereal", "crunchy breakfast in a bowl"),
    ("charger", "rescues a dying battery"),
    ("cloud", "floating thing in the sky"),
    ("clover", "lucky green plant"),
    ("comet", "bright object with a tail in space"),
    ("cookie", "small sweet baked treat"),
    ("coral", "reef builder under the sea"),
    ("cosmic", "spacey and star-filled"),
    ("cozy", "warm and comforting"),
    ("crystal", "clear gem-like stone"),
    ("cupcake", "small frosted dessert"),
    ("doodle", "casual little drawing"),
    ("earbuds", "tiny speakers for your ears"),
    ("echo", "sound that bounces back"),
    ("ember", "glowing leftover in a fire"),
    ("festival", "big event with music or celebration"),
    ("firefly", "small glowing bug"),
    ("fringe", "hair that hangs over the forehead"),
    ("frosting", "sweet topping on cake"),
    ("galaxy", "huge star system"),
    ("garden", "where flowers and herbs grow"),
    ("glimmer", "tiny flash of light"),
    ("goblin", "small mischievous fantasy creature"),
    ("granola", "clustered oat snack"),
    ("headphones", "bigger version of earbuds"),
    ("hoodie", "soft sweatshirt with a hood"),
    ("jelly", "fruit spread that jiggles"),
    ("journal", "private notebook for thoughts"),
    ("lantern", "portable light source"),
    ("latte", "espresso drink with milk"),
    ("lemonade", "sweet citrus drink"),
    ("marble", "small polished glass sphere"),
    ("matcha", "green tea powder drink"),
    ("meadow", "wide grassy field"),
    ("meteor", "space rock burning through the sky"),
    ("mochi", "soft chewy rice dessert"),
    ("moonbeam", "soft light from the moon"),
    ("mosaic", "picture made from small pieces"),
    ("notebook", "paper companion for school or work"),
    ("oatmilk", "plant-based milk alternative"),
    ("orbit", "path around a planet or star"),
    ("pancake", "flat breakfast stack"),
    ("pebble", "small smooth stone"),
    ("peppermint", "cool minty flavor"),
    ("picnic", "meal outside on a blanket"),
    ("pixel", "tiny square on a screen"),
    ("planet", "big world in space"),
    ("playlist", "carefully chosen queue of songs"),
    ("pocket", "small fabric storage space"),
    ("postcard", "short message sent while traveling"),
    ("puddle", "small pool after rain"),
    ("puzzle", "something built to be solved"),
    ("raincoat", "jacket for wet weather"),
    ("ramen", "noodle soup in a bowl"),
    ("ribbon", "decorative strip of fabric"),
    ("rocket", "vehicle that blasts into space"),
    ("sandbox", "play area full of sand"),
    ("scooter", "small two-wheeled ride"),
    ("seashell", "spiraled treasure from the shore"),
    ("shadow", "dark shape made by blocked light"),
    ("sketch", "quick rough drawing"),
    ("snowfall", "when snow starts coming down"),
    ("sparkler", "handheld firework"),
    ("sticker", "peel-and-place decoration"),
    ("sunrise", "the start of morning"),
    ("sunset", "the colorful end of day"),
    ("sweater", "knit layer for chilly weather"),
    ("teacup", "small cup for hot tea"),
    ("thunder", "the sound that follows lightning"),
    ("ticket", "what gets you through the gate"),
    ("toaster", "turns bread crispy"),
    ("trinket", "small keepsake object"),
    ("umbrella", "rain shield you forget at home"),
    ("velvet", "very soft fabric"),
    ("vinyl", "record you can spin"),
    ("waffle", "grid-pattern breakfast food"),
    ("window", "glass opening in a wall"),
)

EMOJI_BANK: tuple[tuple[str, str, str], ...] = (
    ("anchor", "⚓🌊", "nautical gear"),
    ("bagel", "🥯☕", "popular cafe order"),
    ("blanket", "🛋️🧶", "couch comfort"),
    ("boba", "🧋🫧", "chewy tea"),
    ("camera", "📷✨", "captures a moment"),
    ("candle", "🕯️🌙", "quiet glow"),
    ("cereal", "🥣🌾", "morning bowl"),
    ("charger", "🔌🔋", "battery rescue"),
    ("clover", "🍀✨", "lucky green"),
    ("comet", "☄️🌌", "tail in the sky"),
    ("cookie", "🍪🥛", "classic snack duo"),
    ("cupcake", "🧁🎉", "small frosted treat"),
    ("doodle", "✏️🌀", "casual drawing"),
    ("earbuds", "🎧👂", "tiny speakers"),
    ("ember", "🔥🪵", "glow after the flame"),
    ("festival", "🎪🎵", "big celebration"),
    ("firefly", "✨🐞", "glowing bug"),
    ("galaxy", "🌌⭐", "stars everywhere"),
    ("garden", "🌿🌼", "where flowers grow"),
    ("granola", "🥣🥜", "clustered oat snack"),
    ("hoodie", "🧥🌧️", "cozy layer"),
    ("journal", "📔🖊️", "private notebook"),
    ("lantern", "🏮🌙", "portable light"),
    ("latte", "☕🥛", "espresso with milk"),
    ("lemonade", "🍋🧊", "sweet citrus drink"),
    ("marble", "🔵✨", "small polished sphere"),
    ("matcha", "🍵💚", "green tea powder"),
    ("meteor", "☄️🔥", "space rock burn"),
    ("mochi", "🍡☁️", "soft chewy dessert"),
    ("notebook", "📓📝", "paper companion"),
    ("orbit", "🪐🔄", "path around a world"),
    ("pancake", "🥞🍯", "flat breakfast stack"),
    ("pebble", "🪨🌊", "small smooth stone"),
    ("peppermint", "🍬❄️", "cool minty flavor"),
    ("picnic", "🧺🌤️", "meal outside"),
    ("pixel", "🟦🟨", "tiny square on a screen"),
    ("planet", "🪐🌍", "big world in space"),
    ("playlist", "🎵📋", "carefully chosen songs"),
    ("postcard", "✉️🗺️", "travel message"),
    ("puddle", "🌧️💧", "small pool after rain"),
    ("puzzle", "🧩💡", "made to be solved"),
    ("ramen", "🍜🥢", "noodle soup"),
    ("rocket", "🚀🌠", "blasts into space"),
    ("sandbox", "🪣🏖️", "play area with sand"),
    ("seashell", "🐚🌊", "shore treasure"),
    ("shadow", "🌤️🕶️", "dark shape from light"),
    ("snowfall", "❄️🌨️", "flakes coming down"),
    ("sparkler", "✨🎆", "handheld firework"),
    ("sticker", "⭐📎", "peel-and-place decoration"),
    ("sunrise", "🌅⏰", "start of morning"),
    ("sunset", "🌇🧡", "end of day glow"),
    ("sweater", "🧶🧥", "knit layer"),
    ("teacup", "🍵🫖", "small cup for tea"),
    ("thunder", "⚡🌩️", "sound after lightning"),
    ("ticket", "🎟️🎪", "gets you through the gate"),
    ("toaster", "🍞🔥", "bread crisp-maker"),
    ("trinket", "🎀🪙", "small keepsake"),
    ("umbrella", "☔🌧️", "rain shield"),
    ("vinyl", "💿🎶", "record you can spin"),
    ("waffle", "🧇🍓", "grid-pattern breakfast"),
    ("window", "🪟☀️", "glass opening"),
)


def normalize_daily_guess(raw_guess: str | None) -> str:
    if raw_guess is None:
        return ""
    letters = [ch for ch in raw_guess.strip().lower() if ch.isalpha()]
    return "".join(letters)


def get_daily_mode_meta(mode: str) -> dict[str, str] | None:
    return DAILY_MODE_META.get(mode)


def list_daily_modes() -> tuple[str, ...]:
    return DAILY_MODE_ORDER


def resolve_daily_mode(raw_mode: str | None) -> str | None:
    if raw_mode is None:
        return None
    normalized = raw_mode.strip().lower()
    if not normalized:
        return None
    aliases = {
        "scramble": "shuffle",
        "shuffle": "shuffle",
        "emoji": "emoji",
        "emojis": "emoji",
        "signal": "signal",
        "decode": "signal",
        "cipher": "signal",
    }
    return aliases.get(normalized)


def _challenge_number(puzzle_date: date) -> int:
    return max(1, (puzzle_date - DAILY_START_DATE).days + 1)


def _build_rng(puzzle_date: date, *, salt: int) -> random.Random:
    return random.Random((puzzle_date.toordinal() * 7919) + salt)


def _build_puzzle(
    *,
    mode: str,
    puzzle_date: date,
    answer: str,
    scramble: str,
    hint: str,
) -> DailyPuzzle:
    meta = DAILY_MODE_META[mode]
    return DailyPuzzle(
        challenge_id=meta["challenge_id"],
        mode=mode,
        label=meta["label"],
        prompt_label=meta["prompt_label"],
        puzzle_date=puzzle_date,
        challenge_number=_challenge_number(puzzle_date),
        answer=answer,
        scramble=scramble,
        hint=hint,
        length=len(answer),
        instructions=meta["instructions"],
        share_flair=meta["share_flair"],
    )


def _choose_word(rng: random.Random) -> tuple[str, str]:
    return WORD_BANK[rng.randrange(len(WORD_BANK))]


def build_daily_shuffle(puzzle_date: date) -> DailyPuzzle:
    rng = _build_rng(puzzle_date, salt=104729)
    answer, hint = _choose_word(rng)

    letters = list(answer.upper())
    scrambled = letters[:]
    for _ in range(8):
        rng.shuffle(scrambled)
        if "".join(scrambled) != answer.upper():
            break
    if "".join(scrambled) == answer.upper():
        scrambled = letters[1:] + letters[:1]

    return _build_puzzle(
        mode="shuffle",
        puzzle_date=puzzle_date,
        answer=answer,
        scramble=" ".join(scrambled),
        hint=hint,
    )


def build_daily_emoji(puzzle_date: date) -> DailyPuzzle:
    rng = _build_rng(puzzle_date, salt=130363)
    answer, emoji_clue, hint = EMOJI_BANK[rng.randrange(len(EMOJI_BANK))]
    return _build_puzzle(
        mode="emoji",
        puzzle_date=puzzle_date,
        answer=answer,
        scramble=emoji_clue,
        hint=hint,
    )


def _shift_letters(text: str, shift: int) -> str:
    output = []
    for char in text.upper():
        if "A" <= char <= "Z":
            offset = ((ord(char) - ord("A")) + shift) % 26
            output.append(chr(ord("A") + offset))
        else:
            output.append(char)
    return "".join(output)


def build_daily_signal(puzzle_date: date) -> DailyPuzzle:
    rng = _build_rng(puzzle_date, salt=15485863)
    answer, hint = _choose_word(rng)
    shift = 1 + rng.randrange(5)
    encoded = _shift_letters(answer, shift)
    return _build_puzzle(
        mode="signal",
        puzzle_date=puzzle_date,
        answer=answer,
        scramble=" ".join(encoded),
        hint=f"{hint} | letters shifted by +{shift}",
    )


def build_daily_puzzle(puzzle_date: date, mode: str) -> DailyPuzzle:
    resolved = resolve_daily_mode(mode)
    if resolved == "emoji":
        return build_daily_emoji(puzzle_date)
    if resolved == "signal":
        return build_daily_signal(puzzle_date)
    return build_daily_shuffle(puzzle_date)


def build_daily_arcade(puzzle_date: date) -> dict[str, DailyPuzzle]:
    return {mode: build_daily_puzzle(puzzle_date, mode) for mode in DAILY_MODE_ORDER}
