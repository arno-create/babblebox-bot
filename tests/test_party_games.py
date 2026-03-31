from __future__ import annotations

import types
import unittest
from unittest.mock import AsyncMock, patch

from babblebox.only16_game import detect_count_question, parse_only16_numeric_answer
from babblebox.pattern_hunt_game import (
    RuleAtom,
    message_matches_rule,
    parse_guess_atom,
    select_rule_bundle,
    submit_pattern_guess_locked,
)


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id
        self.mention = f"<@{user_id}>"
        self.display_name = f"User {user_id}"


class PartyGameLogicTests(unittest.IsolatedAsyncioTestCase):
    def test_only16_question_detection_prefers_clear_quantity_and_math_prompts(self):
        self.assertTrue(detect_count_question("How many moons does Mars have?"))
        self.assertTrue(detect_count_question("What number is on the jersey?"))
        self.assertTrue(detect_count_question("What is 8+8?"))
        self.assertTrue(detect_count_question("Calculate (10 + 6)?"))
        self.assertFalse(detect_count_question("What's up?"))
        self.assertFalse(detect_count_question("Tell me a joke."))

    def test_only16_number_parser_handles_words_math_and_ambiguity(self):
        self.assertEqual(parse_only16_numeric_answer("fifteen").kind, "single")
        self.assertEqual(parse_only16_numeric_answer("fifteen").value, 15)
        self.assertEqual(parse_only16_numeric_answer("negative sixteen").value, -16)
        self.assertEqual(parse_only16_numeric_answer("seventy-two").value, 72)
        self.assertEqual(parse_only16_numeric_answer("one hundred six").value, 106)
        self.assertEqual(parse_only16_numeric_answer("17-1").value, 16)
        self.assertEqual(parse_only16_numeric_answer("(10+6)").value, 16)
        self.assertEqual(parse_only16_numeric_answer("32/2").value, 16)
        self.assertEqual(parse_only16_numeric_answer("4^2").value, 16)
        self.assertEqual(parse_only16_numeric_answer("I think 12 or maybe 16").kind, "ambiguous")
        self.assertEqual(parse_only16_numeric_answer("16/0").kind, "unsupported")
        self.assertEqual(parse_only16_numeric_answer("probably sixteen-ish").kind, "none")

    def test_pattern_rule_matchers_are_machine_checkable(self):
        rule = [
            RuleAtom("contains_digits"),
            RuleAtom("contains_category_word", "animal"),
            RuleAtom("ends_with_punctuation", "!"),
        ]
        self.assertTrue(message_matches_rule(rule, "7 foxes sprint!"))
        self.assertFalse(message_matches_rule(rule, "three foxes sprint!"))
        self.assertFalse(message_matches_rule(rule, "7 foxes sprint."))

    def test_pattern_guess_parser_rejects_bad_values(self):
        ok, atom = parse_guess_atom("starts_with_letter", "b")
        self.assertTrue(ok)
        self.assertEqual(atom, RuleAtom("starts_with_letter", "b"))

        ok, atom = parse_guess_atom("contains_digits", None)
        self.assertTrue(ok)
        self.assertEqual(atom, RuleAtom("contains_digits"))

        ok, error = parse_guess_atom("word_count_range", "wide")
        self.assertFalse(ok)
        self.assertIn("range", str(error).lower())

    def test_pattern_rule_generation_always_has_examples_and_respects_recent_signatures(self):
        atoms, valid_examples, invalid_example = select_rule_bundle(42)
        self.assertGreaterEqual(len(atoms), 1)
        self.assertGreaterEqual(len(valid_examples), 2)
        for sample in valid_examples:
            self.assertTrue(message_matches_rule(atoms, sample))
        self.assertFalse(message_matches_rule(atoms, invalid_example))

        recent = {tuple(sorted((atom.family, str(atom.value)) for atom in atoms))}
        new_atoms, _, _ = select_rule_bundle(42, recent_signatures=recent)
        self.assertNotEqual(tuple(sorted((atom.family, str(atom.value)) for atom in new_atoms)), next(iter(recent)))

    async def test_pattern_guess_compares_structured_atoms(self):
        guesser = DummyUser(10)
        game = {
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "rule_atoms": [RuleAtom("contains_digits"), RuleAtom("question_form")],
                "guesses_used": 0,
                "guess_limit": 3,
            }
        }
        with patch("babblebox.pattern_hunt_game._finish_pattern_hunt_locked", new=AsyncMock()) as finish:
            ok, message = await submit_pattern_guess_locked(
                99,
                game,
                guesser,
                [RuleAtom("question_form"), RuleAtom("contains_digits")],
            )
        self.assertTrue(ok)
        self.assertEqual(message, "Correct")
        finish.assert_awaited_once()

    async def test_pattern_wrong_guess_spends_budget(self):
        guesser = DummyUser(10)
        game = {
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "rule_atoms": [RuleAtom("contains_digits")],
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
