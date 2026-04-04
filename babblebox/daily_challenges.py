from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass
from datetime import date


DAILY_MAX_ATTEMPTS = 3
DAILY_START_DATE = date(2026, 1, 1)
DAILY_DEFAULT_MODE = "shuffle"
DAILY_MODE_ORDER = ("shuffle", "emoji", "signal")
DAILY_DIFFICULTY_LABELS = {1: "Easy", 2: "Medium", 3: "Hard"}
DAILY_PROFILE_ORDER = ("standard", "smart", "hard")
DAILY_PROFILE_DIFFICULTY_ORDER = {
    "standard": (1, 2, 3),
    "smart": (2, 1, 3),
    "hard": (3, 2, 1),
}
DAILY_PROFILE_WEIGHTS = {
    "shuffle": {"standard": 50, "smart": 35, "hard": 15},
    "emoji": {"standard": 45, "smart": 40, "hard": 15},
    "signal": {"standard": 35, "smart": 45, "hard": 20},
}
SIGNAL_CODEC_META = {
    "caesar": {"label": "Caesar shift"},
    "mirror": {"label": "Mirror alphabet"},
    "swap": {"label": "Adjacent-pair swap"},
}


@dataclass(frozen=True)
class DailySeedEntry:
    answer: str
    hint: str
    emoji_clue: str
    difficulty: int
    family: str


@dataclass(frozen=True)
class DailyBankEntry:
    answer: str
    hint: str
    difficulty: int
    family: str
    clue: str = ""
    codecs: tuple[str, ...] = ()


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
    difficulty: int
    difficulty_label: str
    profile: str
    family: str
    codec: str | None = None


DAILY_MODE_META = {
    "shuffle": {
        "challenge_id": "daily_shuffle_v2",
        "label": "Shuffle Booth",
        "prompt_label": "Shuffle Tray",
        "instructions": "Unscramble the letters into one one-word answer.",
        "share_flair": "[shuffle]",
    },
    "emoji": {
        "challenge_id": "daily_emoji_v1",
        "label": "Emoji Booth",
        "prompt_label": "Emoji Trail",
        "instructions": "Read the emoji clue and name the matching one-word answer.",
        "share_flair": "[emoji]",
    },
    "signal": {
        "challenge_id": "daily_signal_v1",
        "label": "Signal Booth",
        "prompt_label": "Signal Strip",
        "instructions": "Decode the pattern and type the original one-word answer.",
        "share_flair": "[signal]",
    },
}


def _family_entries(family: str, rows: tuple[tuple[str, str, str, int], ...]) -> tuple[DailySeedEntry, ...]:
    return tuple(DailySeedEntry(answer=answer, hint=hint, emoji_clue=emoji, difficulty=difficulty, family=family) for answer, hint, emoji, difficulty in rows)


COZY_HOME_ENTRIES = _family_entries(
    "cozy-home",
    (
        ("blanket", "couch comfort layer", "🛋️🧵😴", 1),
        ("candle", "small flame for light or scent", "🕯️🌙✨", 1),
        ("lantern", "portable glow for dark paths", "🏮🌙🚶", 2),
        ("window", "glass opening to the outside", "🪟🌤️👀", 1),
        ("pocket", "small fabric storage spot", "👖🪙🤏", 1),
        ("zipper", "teeth that close a jacket or bag", "🧥🦷⬆️", 2),
        ("teapot", "hot-water pourer for tea", "🫖☕💨", 2),
        ("sweater", "knit layer for chilly weather", "🧶❄️🧥", 1),
        ("wardrobe", "standing home for clothes", "🚪👗🏠", 3),
        ("pillow", "soft headrest for sleep", "🛏️☁️😴", 1),
        ("kettle", "water boiler for steam", "💧🔥💨", 2),
        ("doormat", "thing you wipe shoes on", "🚪👟🧽", 2),
        ("teacup", "small cup for tea", "☕🤏🍃", 1),
    ),
)

FOOD_DRINK_ENTRIES = _family_entries(
    "food-drink",
    (
        ("bagel", "round bread with a hole", "🥯⭕🧂", 1),
        ("cereal", "crunchy breakfast in a bowl", "🥣🥛🌞", 1),
        ("cookie", "small sweet baked treat", "🍪🥛🙂", 1),
        ("cupcake", "small frosted dessert", "🧁🎉🍬", 1),
        ("granola", "clustered oat snack", "🥣🌾🍯", 2),
        ("lemonade", "sweet citrus drink", "🍋🧊🥤", 1),
        ("matcha", "green tea powder drink", "🍵🌿💚", 2),
        ("pancake", "flat breakfast stack", "🥞🍯🌞", 1),
        ("peppermint", "cool minty flavor", "🌿❄️🍬", 2),
        ("ramen", "noodle soup in a bowl", "🍜🥢🔥", 1),
        ("waffle", "grid-pattern breakfast food", "🧇🧈🍯", 1),
        ("boba", "tea drink with chewy pearls", "🧋⚫🙂", 1),
        ("oatmeal", "warm breakfast made from oats", "🥣🌾🔥", 2),
    ),
)

NATURE_ENTRIES = _family_entries(
    "nature-weather",
    (
        ("clover", "lucky green plant", "🍀🎲🙂", 1),
        ("coral", "reef builder under the sea", "🪸🌊🐠", 2),
        ("ember", "glow after the flame", "🔥🪵✨", 2),
        ("firefly", "small glowing bug", "🌙🐞✨", 1),
        ("meadow", "wide grassy field", "🌾🌼🐝", 1),
        ("pebble", "small smooth stone", "🪨🤏🌊", 1),
        ("puddle", "small pool after rain", "🌧️💧👟", 1),
        ("rainfall", "water coming down from clouds", "🌧️⬇️💧", 2),
        ("seashell", "shore treasure from the tide", "🐚🌊🏖️", 1),
        ("thunder", "sound after lightning", "⚡🔊🌩️", 1),
        ("waterfall", "river drop over a cliff", "💧⬇️🪨", 2),
        ("sunflower", "tall yellow bloom that follows light", "🌻☀️🌱", 2),
        ("stormcloud", "dark cloud packed with rain", "☁️⚡🌧️", 3),
    ),
)

SPACE_ENTRIES = _family_entries(
    "space-light",
    (
        ("comet", "bright object with a tail", "☄️🌌✨", 1),
        ("cosmic", "spacey and star-filled", "🌌⭐🛰️", 2),
        ("eclipse", "sun or moon briefly hidden", "🌞🌑😮", 2),
        ("galaxy", "huge system of stars", "🌌⭐🌀", 1),
        ("meteor", "space rock burning through the sky", "☄️🔥🌠", 2),
        ("moonbeam", "soft light from the moon", "🌙✨🫧", 2),
        ("nebula", "cloud of gas in space", "☁️🌌⭐", 3),
        ("orbit", "path around a world", "🪐🔄🌍", 1),
        ("planet", "big world in space", "🪐🌌🛰️", 1),
        ("starlight", "light from distant stars", "⭐✨🌙", 2),
        ("sunrise", "the start of morning", "🌅☀️⏰", 1),
        ("sunset", "the colorful end of day", "🌇☀️🌙", 1),
        ("northstar", "steady guide in the night sky", "⭐🧭🌌", 3),
    ),
)

TRAVEL_ENTRIES = _family_entries(
    "travel-gear",
    (
        ("anchor", "keeps a ship from drifting", "⚓🌊⛵", 1),
        ("backpack", "carryall for class or travel", "🎒🗺️🚶", 1),
        ("beacon", "signal light for guidance", "💡🌊🧭", 2),
        ("caravan", "travel group moving together", "🐪🛣️🌙", 2),
        ("compass", "tool that points direction", "🧭⬆️🗺️", 1),
        ("postcard", "short note sent from a trip", "✉️🌍📮", 2),
        ("raincoat", "jacket for wet weather", "🌧️🧥☂️", 1),
        ("scooter", "small two-wheeled ride", "🛴💨🛣️", 1),
        ("ticket", "pass that gets you in", "🎟️🚪🙂", 1),
        ("umbrella", "rain shield you forget", "☔🌧️🤦", 1),
        ("satchel", "cross-body bag for carrying things", "👜📚🚶", 2),
        ("lighthouse", "shore tower with a guiding lamp", "🗼🌊💡", 3),
        ("trailhead", "where a hike begins", "🥾🪧🌲", 3),
    ),
)

ARTS_ENTRIES = _family_entries(
    "arts-play",
    (
        ("arcade", "room full of buttons and high scores", "🕹️🎯✨", 1),
        ("camera", "captures a moment", "📷✨🕒", 1),
        ("canvas", "surface painters like to use", "🖌️🟦🖼️", 1),
        ("doodle", "casual little drawing", "✏️🙂📄", 1),
        ("journal", "private notebook for thoughts", "📓🖊️🤫", 1),
        ("marble", "small polished glass sphere", "🔵🤏🎯", 1),
        ("mosaic", "picture made from many small pieces", "🧩🖼️🔹", 2),
        ("playlist", "carefully chosen queue of songs", "🎵📋🎧", 2),
        ("puzzle", "something made to be solved", "🧩🤔✨", 1),
        ("sketch", "quick rough drawing", "✏️🖼️⚡", 1),
        ("sticker", "peel-and-place decoration", "🌟🧻📌", 1),
        ("vinyl", "record you can spin", "💿🎶🖤", 2),
        ("paintbrush", "tool that spreads color", "🖌️🎨🌈", 3),
    ),
)

TECH_CITY_ENTRIES = _family_entries(
    "tech-city",
    (
        ("charger", "rescues a dying battery", "🔌🔋⚡", 1),
        ("earbuds", "tiny speakers for your ears", "🎧👂🎵", 1),
        ("keyboard", "key grid for typing", "⌨️✍️💬", 2),
        ("notebook", "paper companion for work or study", "📒✏️📚", 1),
        ("pixel", "tiny square on a screen", "🟦📺🔍", 1),
        ("skyline", "city outline against the sky", "🏙️🌆✨", 2),
        ("blueprint", "plan drawing before a build", "📐📘🏗️", 3),
        ("workshop", "room for making and fixing", "🛠️🪚🏠", 2),
        ("rooftop", "top surface of a building", "🏙️⬆️🌤️", 2),
        ("paperclip", "bent metal helper for pages", "📎📄📚", 2),
        ("turntable", "spinning player for records", "🎚️💿🔄", 3),
        ("typewriter", "old machine for tapping letters", "⌨️📄🕰️", 3),
        ("windmill", "blades turning in the breeze", "🌬️⚙️🌾", 2),
    ),
)

WONDER_ENTRIES = _family_entries(
    "wonder-adventure",
    (
        ("crystal", "clear gem-like stone", "💎✨🪨", 1),
        ("dragonfly", "fast winged insect by the water", "🐉🪰🌿", 2),
        ("festival", "big event with music or celebration", "🎉🎶🌟", 1),
        ("glitter", "tiny sparkly pieces", "✨✨🫧", 2),
        ("harbor", "sheltered place for boats", "⚓🌊🏘️", 2),
        ("hoodie", "soft sweatshirt with a hood", "🧥🙂🧵", 1),
        ("labyrinth", "maze with many twisting paths", "🌀🧱🤔", 3),
        ("moonstone", "gem with a soft milky glow", "🌙💎✨", 3),
        ("riverbank", "edge beside a flowing river", "🌊🌿🪨", 2),
        ("starfish", "sea creature shaped like a star", "⭐🐚🌊", 1),
        ("telescope", "tool for seeing faraway things", "🔭🌌👀", 2),
        ("treasure", "hidden prize worth finding", "🗺️📦✨", 1),
        ("wildflower", "untamed bloom growing on its own", "🌼🌿🦋", 3),
    ),
)

MASTER_DAILY_ENTRIES: tuple[DailySeedEntry, ...] = (
    COZY_HOME_ENTRIES
    + FOOD_DRINK_ENTRIES
    + NATURE_ENTRIES
    + SPACE_ENTRIES
    + TRAVEL_ENTRIES
    + ARTS_ENTRIES
    + TECH_CITY_ENTRIES
    + WONDER_ENTRIES
)

SHUFFLE_BANK: tuple[DailyBankEntry, ...] = tuple(
    DailyBankEntry(answer=entry.answer, hint=entry.hint, difficulty=entry.difficulty, family=entry.family)
    for entry in MASTER_DAILY_ENTRIES
)
EMOJI_BANK: tuple[DailyBankEntry, ...] = tuple(
    DailyBankEntry(answer=entry.answer, hint=entry.hint, difficulty=entry.difficulty, family=entry.family, clue=entry.emoji_clue)
    for entry in MASTER_DAILY_ENTRIES
)
SIGNAL_BANK: tuple[DailyBankEntry, ...] = tuple(
    DailyBankEntry(
        answer=entry.answer,
        hint=entry.hint,
        difficulty=entry.difficulty,
        family=entry.family,
        codecs=("caesar", "mirror") if entry.difficulty == 1 else ("caesar", "mirror", "swap") if entry.difficulty == 2 else ("mirror", "swap", "caesar"),
    )
    for entry in MASTER_DAILY_ENTRIES
)
DAILY_BANKS = {
    "shuffle": SHUFFLE_BANK,
    "emoji": EMOJI_BANK,
    "signal": SIGNAL_BANK,
}


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


def _build_rng(puzzle_date: date, *, salt: str) -> random.Random:
    digest = hashlib.sha256(f"{puzzle_date.isoformat()}::{salt}".encode("utf-8")).hexdigest()
    return random.Random(int(digest[:16], 16))


def _weighted_profile_choice(puzzle_date: date, mode: str) -> str:
    weights = DAILY_PROFILE_WEIGHTS[mode]
    rng = _build_rng(puzzle_date, salt=f"profile:{mode}")
    pick = rng.randrange(sum(weights.values()))
    cursor = 0
    for profile in DAILY_PROFILE_ORDER:
        cursor += weights[profile]
        if pick < cursor:
            return profile
    return "standard"


def _resolve_daily_profiles(puzzle_date: date) -> dict[str, str]:
    profiles = {mode: _weighted_profile_choice(puzzle_date, mode) for mode in DAILY_MODE_ORDER}
    if all(profile != "standard" for profile in profiles.values()):
        profiles["shuffle"] = "standard"
    hard_modes = [mode for mode in DAILY_MODE_ORDER if profiles[mode] == "hard"]
    if len(hard_modes) > 1:
        keep_hard = next((mode for mode in ("signal", "shuffle", "emoji") if mode in hard_modes), "signal")
        for mode in hard_modes:
            if mode != keep_hard:
                profiles[mode] = "smart"
    return profiles


def _ordered_bank(bank: tuple[DailyBankEntry, ...], puzzle_date: date, mode: str, profile: str) -> list[DailyBankEntry]:
    return sorted(
        bank,
        key=lambda entry: hashlib.sha256(f"{puzzle_date.isoformat()}::{mode}::{profile}::{entry.answer}".encode("utf-8")).hexdigest(),
    )


def _pick_bank_entry(
    *,
    puzzle_date: date,
    mode: str,
    profile: str,
    used_answers: set[str],
    used_families: set[str],
) -> DailyBankEntry:
    ordered = _ordered_bank(DAILY_BANKS[mode], puzzle_date, mode, profile)
    for allow_reused_family in (False, True):
        for difficulty in DAILY_PROFILE_DIFFICULTY_ORDER[profile]:
            for entry in ordered:
                if entry.answer in used_answers:
                    continue
                if entry.difficulty != difficulty:
                    continue
                if not allow_reused_family and entry.family in used_families:
                    continue
                return entry
    for entry in ordered:
        if entry.answer not in used_answers:
            return entry
    raise RuntimeError("Daily bank unexpectedly ran out of unique answers.")


def _scramble_answer(answer: str, rng: random.Random) -> str:
    letters = list(answer.upper())
    scrambled = letters[:]
    for _ in range(10):
        rng.shuffle(scrambled)
        if "".join(scrambled) != answer.upper():
            break
    if "".join(scrambled) == answer.upper():
        scrambled = letters[1:] + letters[:1]
    return " ".join(scrambled)


def _encode_caesar(answer: str, shift: int) -> str:
    output = []
    for char in answer.upper():
        offset = ((ord(char) - ord("A")) + shift) % 26
        output.append(chr(ord("A") + offset))
    return "".join(output)


def _encode_mirror(answer: str) -> str:
    output = []
    for char in answer.upper():
        output.append(chr(ord("Z") - (ord(char) - ord("A"))))
    return "".join(output)


def _encode_swap(answer: str) -> str:
    letters = list(answer.upper())
    swapped: list[str] = []
    index = 0
    while index < len(letters):
        if index + 1 < len(letters):
            swapped.append(letters[index + 1])
            swapped.append(letters[index])
            index += 2
        else:
            swapped.append(letters[index])
            index += 1
    return "".join(swapped)


def _build_puzzle(*, mode: str, puzzle_date: date, entry: DailyBankEntry, profile: str, scramble: str, hint: str, codec: str | None = None) -> DailyPuzzle:
    meta = DAILY_MODE_META[mode]
    return DailyPuzzle(
        challenge_id=meta["challenge_id"],
        mode=mode,
        label=meta["label"],
        prompt_label=meta["prompt_label"],
        puzzle_date=puzzle_date,
        challenge_number=_challenge_number(puzzle_date),
        answer=entry.answer,
        scramble=scramble,
        hint=hint,
        length=len(entry.answer),
        instructions=meta["instructions"],
        share_flair=meta["share_flair"],
        difficulty=entry.difficulty,
        difficulty_label=DAILY_DIFFICULTY_LABELS[entry.difficulty],
        profile=profile,
        family=entry.family,
        codec=codec,
    )


def _build_shuffle_puzzle(puzzle_date: date, entry: DailyBankEntry, profile: str) -> DailyPuzzle:
    rng = _build_rng(puzzle_date, salt=f"shuffle:{entry.answer}")
    return _build_puzzle(
        mode="shuffle",
        puzzle_date=puzzle_date,
        entry=entry,
        profile=profile,
        scramble=_scramble_answer(entry.answer, rng),
        hint=entry.hint,
    )


def _build_emoji_puzzle(puzzle_date: date, entry: DailyBankEntry, profile: str) -> DailyPuzzle:
    return _build_puzzle(
        mode="emoji",
        puzzle_date=puzzle_date,
        entry=entry,
        profile=profile,
        scramble=entry.clue,
        hint=entry.hint,
    )


def _build_signal_puzzle(puzzle_date: date, entry: DailyBankEntry, profile: str) -> DailyPuzzle:
    rng = _build_rng(puzzle_date, salt=f"signal:{entry.answer}")
    codec = entry.codecs[rng.randrange(len(entry.codecs))]
    if codec == "caesar":
        shift = 1 + rng.randrange(5)
        encoded = _encode_caesar(entry.answer, shift)
        hint = f"{entry.hint} | Codec: {SIGNAL_CODEC_META[codec]['label']} (+{shift})"
    elif codec == "mirror":
        encoded = _encode_mirror(entry.answer)
        hint = f"{entry.hint} | Codec: {SIGNAL_CODEC_META[codec]['label']}"
    else:
        encoded = _encode_swap(entry.answer)
        hint = f"{entry.hint} | Codec: {SIGNAL_CODEC_META[codec]['label']}"
    return _build_puzzle(
        mode="signal",
        puzzle_date=puzzle_date,
        entry=entry,
        profile=profile,
        scramble=" ".join(encoded),
        hint=hint,
        codec=codec,
    )


def build_daily_arcade(puzzle_date: date) -> dict[str, DailyPuzzle]:
    profiles = _resolve_daily_profiles(puzzle_date)
    used_answers: set[str] = set()
    used_families: set[str] = set()
    puzzles: dict[str, DailyPuzzle] = {}
    for mode in DAILY_MODE_ORDER:
        entry = _pick_bank_entry(
            puzzle_date=puzzle_date,
            mode=mode,
            profile=profiles[mode],
            used_answers=used_answers,
            used_families=used_families,
        )
        used_answers.add(entry.answer)
        used_families.add(entry.family)
        if mode == "shuffle":
            puzzles[mode] = _build_shuffle_puzzle(puzzle_date, entry, profiles[mode])
        elif mode == "emoji":
            puzzles[mode] = _build_emoji_puzzle(puzzle_date, entry, profiles[mode])
        else:
            puzzles[mode] = _build_signal_puzzle(puzzle_date, entry, profiles[mode])
    return puzzles


def build_daily_shuffle(puzzle_date: date) -> DailyPuzzle:
    return build_daily_arcade(puzzle_date)["shuffle"]


def build_daily_emoji(puzzle_date: date) -> DailyPuzzle:
    return build_daily_arcade(puzzle_date)["emoji"]


def build_daily_signal(puzzle_date: date) -> DailyPuzzle:
    return build_daily_arcade(puzzle_date)["signal"]


def build_daily_puzzle(puzzle_date: date, mode: str) -> DailyPuzzle:
    resolved = resolve_daily_mode(mode) or DAILY_DEFAULT_MODE
    return build_daily_arcade(puzzle_date)[resolved]
