from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
import unittest

from babblebox.daily_challenges import (
    DAILY_MODE_ORDER,
    EMOJI_BANK,
    SHUFFLE_BANK,
    SIGNAL_BANK,
    SIGNAL_CODEC_META,
    build_daily_arcade,
    build_daily_emoji,
    build_daily_puzzle,
    build_daily_shuffle,
    build_daily_signal,
    normalize_daily_guess,
)


class DailyChallengesTests(unittest.TestCase):
    def test_daily_banks_have_depth_difficulty_and_length_spread(self):
        for mode, bank in (("shuffle", SHUFFLE_BANK), ("emoji", EMOJI_BANK), ("signal", SIGNAL_BANK)):
            with self.subTest(mode=mode):
                self.assertGreaterEqual(len(bank), 100)
                self.assertEqual(len({entry.answer for entry in bank}), len(bank))
                self.assertEqual({1, 2, 3}, {entry.difficulty for entry in bank})
                self.assertGreaterEqual(len({entry.family for entry in bank}), 8)
                lengths = {len(entry.answer) for entry in bank}
                self.assertGreaterEqual(len(lengths), 4)
                self.assertLessEqual(min(lengths), 5)
                self.assertGreaterEqual(max(lengths), 9)

        for entry in EMOJI_BANK:
            with self.subTest(answer=entry.answer):
                self.assertTrue(entry.clue.strip())
                self.assertNotIn(entry.answer.casefold(), entry.hint.casefold())
                self.assertNotIn(entry.answer.casefold(), entry.clue.casefold())

        for entry in SIGNAL_BANK:
            with self.subTest(answer=entry.answer):
                self.assertTrue(entry.codecs)
                self.assertTrue(set(entry.codecs).issubset(SIGNAL_CODEC_META))

    def test_daily_arcade_generation_is_deterministic_and_mode_builders_delegate(self):
        puzzle_date = date(2026, 2, 14)
        first = build_daily_arcade(puzzle_date)
        second = build_daily_arcade(puzzle_date)

        self.assertEqual(first, second)
        for mode in DAILY_MODE_ORDER:
            self.assertEqual(build_daily_puzzle(puzzle_date, mode), first[mode])

        self.assertEqual(build_daily_shuffle(puzzle_date), first["shuffle"])
        self.assertEqual(build_daily_emoji(puzzle_date), first["emoji"])
        self.assertEqual(build_daily_signal(puzzle_date), first["signal"])

    def test_long_horizon_arcade_keeps_answers_unique_and_profiles_constrained(self):
        profile_counts: Counter[str] = Counter()

        for day_offset in range(180):
            puzzle_date = date(2026, 1, 1) + timedelta(days=day_offset)
            arcade = build_daily_arcade(puzzle_date)
            answers = [arcade[mode].answer for mode in DAILY_MODE_ORDER]
            families = [arcade[mode].family for mode in DAILY_MODE_ORDER]
            day_profiles = Counter(arcade[mode].profile for mode in DAILY_MODE_ORDER)

            self.assertEqual(len(set(answers)), len(answers))
            self.assertEqual(len(set(families)), len(families))
            self.assertGreaterEqual(day_profiles["standard"], 1)
            self.assertLessEqual(day_profiles["hard"], 1)
            profile_counts.update(day_profiles)

        self.assertGreater(profile_counts["standard"], 0)
        self.assertGreater(profile_counts["smart"], 0)
        self.assertGreater(profile_counts["hard"], 0)

    def test_profile_to_difficulty_mapping_and_signal_codecs_hold_over_180_days(self):
        seen_codecs: set[str] = set()

        for day_offset in range(180):
            puzzle_date = date(2026, 1, 1) + timedelta(days=day_offset)
            arcade = build_daily_arcade(puzzle_date)

            for puzzle in arcade.values():
                if puzzle.profile == "standard":
                    self.assertEqual(puzzle.difficulty, 1)
                elif puzzle.profile == "smart":
                    self.assertEqual(puzzle.difficulty, 2)
                else:
                    self.assertEqual(puzzle.difficulty, 3)

            shuffle = arcade["shuffle"]
            emoji = arcade["emoji"]
            signal = arcade["signal"]

            self.assertNotEqual(normalize_daily_guess(shuffle.scramble), normalize_daily_guess(shuffle.answer))
            self.assertNotIn(normalize_daily_guess(emoji.answer), normalize_daily_guess(emoji.scramble))
            self.assertIsNotNone(signal.codec)
            self.assertIn(signal.codec, SIGNAL_CODEC_META)
            self.assertIn(SIGNAL_CODEC_META[signal.codec]["label"], signal.hint)
            seen_codecs.add(str(signal.codec))

        self.assertEqual(seen_codecs, set(SIGNAL_CODEC_META))

    def test_daily_guess_normalization_stays_low_friction(self):
        self.assertEqual(normalize_daily_guess(" Moon-stone!! "), "moonstone")
        self.assertEqual(normalize_daily_guess("signal: BLUEPRINT"), "signalblueprint")
        self.assertEqual(normalize_daily_guess(None), "")
