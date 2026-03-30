from __future__ import annotations

import types
import unittest
from unittest.mock import AsyncMock, patch

from babblebox.only16_game import detect_count_question, parse_first_explicit_number
from babblebox.pattern_hunt_game import RuleAtom, message_matches_rule, parse_guess_atom, select_rule_bundle, submit_pattern_guess_locked


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id
        self.mention = f"<@{user_id}>"
        self.display_name = f"User {user_id}"


class PartyGameLogicTests(unittest.IsolatedAsyncioTestCase):
    def test_only16_question_detection_prefers_clear_quantity_prompts(self):
        self.assertTrue(detect_count_question("How many moons does Mars have?"))
        self.assertTrue(detect_count_question("What number is on the jersey?"))
        self.assertFalse(detect_count_question("What's up?"))
        self.assertFalse(detect_count_question("Tell me a joke."))

    def test_only16_number_parser_uses_first_explicit_number(self):
        self.assertEqual(parse_first_explicit_number("I think 12 or maybe 16"), 12)
        self.assertEqual(parse_first_explicit_number("sixteen, obviously"), 16)
        self.assertIsNone(parse_first_explicit_number("probably sixteen-ish"))

    def test_pattern_rule_matchers_are_machine_checkable(self):
        rule = [
            RuleAtom("contains_number"),
            RuleAtom("contains_category_word", "animal"),
            RuleAtom("ends_with_punctuation", "!"),
        ]
        self.assertTrue(message_matches_rule(rule, "7 foxes sprint!"))
        self.assertFalse(message_matches_rule(rule, "foxes sprint!"))
        self.assertFalse(message_matches_rule(rule, "7 foxes sprint."))

    def test_pattern_guess_parser_rejects_bad_values(self):
        ok, atom = parse_guess_atom("starts_with_letter", "b")
        self.assertTrue(ok)
        self.assertEqual(atom, RuleAtom("starts_with_letter", "b"))

        ok, error = parse_guess_atom("char_length_range", "wide")
        self.assertFalse(ok)
        self.assertIn("range", str(error).lower())

    def test_pattern_rule_generation_always_has_examples(self):
        atoms, valid_examples, invalid_example = select_rule_bundle(42)
        self.assertGreaterEqual(len(atoms), 1)
        self.assertGreaterEqual(len(valid_examples), 2)
        for sample in valid_examples:
            self.assertTrue(message_matches_rule(atoms, sample))
        self.assertFalse(message_matches_rule(atoms, invalid_example))

    async def test_pattern_guess_compares_structured_atoms(self):
        guesser = DummyUser(10)
        game = {
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "rule_atoms": [RuleAtom("contains_number"), RuleAtom("question_form")],
                "guesses_used": 0,
                "guess_limit": 3,
            }
        }
        with patch("babblebox.pattern_hunt_game._finish_pattern_hunt_locked", new=AsyncMock()) as finish:
            ok, message = await submit_pattern_guess_locked(
                99,
                game,
                guesser,
                [RuleAtom("question_form"), RuleAtom("contains_number")],
            )
        self.assertTrue(ok)
        self.assertEqual(message, "Correct")
        finish.assert_awaited_once()

    async def test_pattern_wrong_guess_spends_budget(self):
        guesser = DummyUser(10)
        game = {
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "rule_atoms": [RuleAtom("contains_number")],
                "guesses_used": 0,
                "guess_limit": 3,
            }
        }
        with patch("babblebox.pattern_hunt_game._finish_pattern_hunt_locked", new=AsyncMock()) as finish:
            ok, message = await submit_pattern_guess_locked(
                99,
                game,
                guesser,
                [RuleAtom("question_form")],
            )
        self.assertFalse(ok)
        self.assertIn("2", message)
        finish.assert_not_awaited()
