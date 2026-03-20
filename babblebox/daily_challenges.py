from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date


DAILY_CHALLENGE_ID = "daily_shuffle_v1"
DAILY_MAX_ATTEMPTS = 3
DAILY_START_DATE = date(2026, 1, 1)


@dataclass(frozen=True)
class DailyPuzzle:
    challenge_id: str
    puzzle_date: date
    challenge_number: int
    answer: str
    scramble: str
    hint: str
    length: int


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


def normalize_daily_guess(raw_guess: str | None) -> str:
    if raw_guess is None:
        return ""
    letters = [ch for ch in raw_guess.strip().lower() if ch.isalpha()]
    return "".join(letters)


def build_daily_shuffle(puzzle_date: date) -> DailyPuzzle:
    seed = puzzle_date.toordinal() * 7919 + 104729
    rng = random.Random(seed)
    answer, hint = WORD_BANK[rng.randrange(len(WORD_BANK))]

    letters = list(answer.upper())
    scrambled = letters[:]
    for _ in range(8):
        rng.shuffle(scrambled)
        if "".join(scrambled) != answer.upper():
            break
    if "".join(scrambled) == answer.upper():
        scrambled = letters[1:] + letters[:1]

    challenge_number = max(1, (puzzle_date - DAILY_START_DATE).days + 1)
    return DailyPuzzle(
        challenge_id=DAILY_CHALLENGE_ID,
        puzzle_date=puzzle_date,
        challenge_number=challenge_number,
        answer=answer,
        scramble=" ".join(scrambled),
        hint=hint,
        length=len(answer),
    )
