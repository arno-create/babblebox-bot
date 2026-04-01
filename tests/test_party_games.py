from __future__ import annotations

import asyncio
import types
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from babblebox import game_engine as ge
from babblebox.only16_game import (
    detect_count_question,
    ensure_only16_state,
    handle_only16_message_locked,
    handle_only16_message_delete_locked,
    manually_arm_only16_message,
    parse_only16_numeric_answer,
)
from babblebox.pattern_hunt_game import (
    RuleAtom,
    _pattern_hunt_answer_timeout,
    _pattern_hunt_prompt_timeout,
    _SAMPLE_MESSAGES,
    _bundle_quality_ok,
    build_pattern_hunt_status_embed,
    handle_pattern_hunt_message_locked,
    message_matches_rule,
    parse_guess_atom,
    select_rule_bundle,
    start_pattern_hunt_game_locked,
    submit_pattern_guess_locked,
)


class DummyUser:
    def __init__(self, user_id: int):
        self.id = user_id
        self.mention = f"<@{user_id}>"
        self.display_name = f"User {user_id}"


class DummyChannel:
    def __init__(self, channel_id: int = 20):
        self.id = channel_id
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))


class DummyMessage:
    def __init__(
        self,
        *,
        channel: DummyChannel,
        author: DummyUser,
        content: str,
        message_id: int = 999,
        created_at=None,
        reference=None,
    ):
        self.channel = channel
        self.author = author
        self.content = content
        self.id = message_id
        self.created_at = created_at or ge.now_utc()
        self.reference = reference
        self.add_reaction = AsyncMock()


class PartyGameLogicTests(unittest.IsolatedAsyncioTestCase):
    def _make_only16_game(self, *, mode: str = "smart"):
        asker = DummyUser(1)
        responder = DummyUser(2)
        channel = DummyChannel()
        game = {
            "channel": channel,
            "players": [asker, responder],
            "current_player_index": 0,
            "turn_task": None,
            "game_type": "only16",
            "active": True,
            "closing": False,
            "only16_mode": mode,
        }
        state = ensure_only16_state(game)
        state["mode"] = mode
        state["ask_started_at"] = ge.now_utc() - timedelta(seconds=1)
        state["trap"] = {
            "asker_id": asker.id,
            "question_message_id": 100,
            "armed_at": ge.now_utc(),
            "expires_at": ge.now_utc() + timedelta(seconds=10),
            "mode": mode,
            "manual": False,
        }
        return game, asker, responder, channel

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

    async def test_only16_smart_mode_ignores_unrelated_chatter(self):
        game, _asker, responder, _channel = self._make_only16_game(mode="smart")
        message = DummyMessage(channel=game["channel"], author=responder, content="wait that's wild")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertFalse(handled)
        self.assertIsNotNone(ensure_only16_state(game).get("trap"))
        advance.assert_not_awaited()

    async def test_only16_smart_mode_accepts_clean_standalone_number(self):
        game, _asker, responder, channel = self._make_only16_game(mode="smart")
        message = DummyMessage(channel=channel, author=responder, content="16")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertTrue(handled)
        self.assertIsNone(ensure_only16_state(game).get("trap"))
        self.assertEqual(channel.sent[-1][1]["embed"].title, "Still Alive")
        advance.assert_awaited_once()

    async def test_only16_smart_mode_accepts_clean_standalone_word_and_math(self):
        for content in ("sixteen", "17-1"):
            with self.subTest(content=content):
                game, _asker, responder, channel = self._make_only16_game(mode="smart")
                message = DummyMessage(channel=channel, author=responder, content=content)

                with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
                    handled = await handle_only16_message_locked(message, 99, game)

                self.assertTrue(handled)
                self.assertIsNone(ensure_only16_state(game).get("trap"))
                self.assertEqual(channel.sent[-1][1]["embed"].title, "Still Alive")
                advance.assert_awaited_once()

    async def test_only16_smart_mode_accepts_compact_answer_wrapper(self):
        game, _asker, responder, channel = self._make_only16_game(mode="smart")
        message = DummyMessage(channel=channel, author=responder, content="answer: 16")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertTrue(handled)
        self.assertIsNone(ensure_only16_state(game).get("trap"))
        self.assertEqual(channel.sent[-1][1]["embed"].title, "Still Alive")
        advance.assert_awaited_once()

    async def test_only16_smart_mode_accepts_clean_punctuation_wrappers(self):
        for content in ("16!", "16.", "sixteen!", "answer: 16!"):
            with self.subTest(content=content):
                game, _asker, responder, channel = self._make_only16_game(mode="smart")
                message = DummyMessage(channel=channel, author=responder, content=content)

                with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
                    handled = await handle_only16_message_locked(message, 99, game)

                self.assertTrue(handled)
                self.assertIsNone(ensure_only16_state(game).get("trap"))
                self.assertEqual(channel.sent[-1][1]["embed"].title, "Still Alive")
                advance.assert_awaited_once()

    async def test_only16_strict_mode_ignores_non_reply_answers(self):
        game, _asker, responder, _channel = self._make_only16_game(mode="strict")
        message = DummyMessage(channel=game["channel"], author=responder, content="16")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertFalse(handled)
        self.assertIsNotNone(ensure_only16_state(game).get("trap"))
        advance.assert_not_awaited()

    async def test_only16_strict_mode_ignores_replies_to_the_wrong_message(self):
        game, _asker, responder, _channel = self._make_only16_game(mode="strict")
        message = DummyMessage(
            channel=game["channel"],
            author=responder,
            content="16",
            reference=types.SimpleNamespace(message_id=9999),
        )

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertFalse(handled)
        self.assertIsNotNone(ensure_only16_state(game).get("trap"))
        advance.assert_not_awaited()

    async def test_only16_smart_mode_voids_unsupported_exact_math_without_elimination(self):
        game, _asker, responder, channel = self._make_only16_game(mode="smart")
        message = DummyMessage(channel=channel, author=responder, content="16/0")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertTrue(handled)
        self.assertIsNone(ensure_only16_state(game).get("trap"))
        self.assertEqual(channel.sent[-1][1]["embed"].title, "Trap Voided")
        advance.assert_awaited_once()

    async def test_only16_smart_mode_voids_punctuated_unsupported_math_without_elimination(self):
        game, _asker, responder, channel = self._make_only16_game(mode="smart")
        message = DummyMessage(channel=channel, author=responder, content="16/0!")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_locked(message, 99, game)

        self.assertTrue(handled)
        self.assertIsNone(ensure_only16_state(game).get("trap"))
        self.assertEqual(channel.sent[-1][1]["embed"].title, "Trap Voided")
        self.assertIn("safe judge grammar", channel.sent[-1][1]["embed"].description)
        advance.assert_awaited_once()

    async def test_only16_smart_mode_ignores_soft_standalone_wrappers(self):
        for content in ("i think 16", "maybe sixteen", "there are 16", "just 16"):
            with self.subTest(content=content):
                game, _asker, responder, _channel = self._make_only16_game(mode="smart")
                message = DummyMessage(channel=game["channel"], author=responder, content=content)

                with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
                    handled = await handle_only16_message_locked(message, 99, game)

                self.assertFalse(handled)
                self.assertIsNotNone(ensure_only16_state(game).get("trap"))
                advance.assert_not_awaited()

    async def test_only16_manual_arm_rejects_stale_messages(self):
        game, asker, _responder, _channel = self._make_only16_game(mode="smart")
        state = ensure_only16_state(game)
        state["trap"] = None
        state["ask_started_at"] = ge.now_utc()
        message = DummyMessage(
            channel=game["channel"],
            author=asker,
            content="How many moons does Mars have?",
            created_at=state["ask_started_at"] - timedelta(seconds=2),
        )

        ok, note = await manually_arm_only16_message(message, 99, game, asker)

        self.assertFalse(ok)
        self.assertIn("current ask window", note)

    async def test_only16_manual_arm_rejects_non_numeric_questions(self):
        game, asker, _responder, _channel = self._make_only16_game(mode="smart")
        state = ensure_only16_state(game)
        state["trap"] = None
        message = DummyMessage(channel=game["channel"], author=asker, content="What's up?")

        ok, note = await manually_arm_only16_message(message, 99, game, asker)

        self.assertFalse(ok)
        self.assertIn("clear number question", note)

    async def test_only16_deleting_the_armed_question_voids_and_advances(self):
        game, _asker, _responder, channel = self._make_only16_game(mode="smart")

        with patch("babblebox.only16_game._advance_to_next_asker_locked", new=AsyncMock()) as advance:
            handled = await handle_only16_message_delete_locked(100, 99, game)

        self.assertTrue(handled)
        self.assertIsNone(ensure_only16_state(game).get("trap"))
        self.assertEqual(channel.sent[-1][1]["embed"].title, "Trap Voided")
        self.assertIn("armed question vanished", channel.sent[-1][1]["embed"].description)
        advance.assert_awaited_once()

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

    def test_pattern_hunt_sample_pool_supports_contains_digits_and_contains_emoji(self):
        self.assertTrue(any(message_matches_rule([RuleAtom("contains_digits")], sample) for sample in _SAMPLE_MESSAGES))
        self.assertTrue(any(message_matches_rule([RuleAtom("contains_emoji")], sample) for sample in _SAMPLE_MESSAGES))

    def test_pattern_hunt_bundle_quality_rejects_dry_solo_rules(self):
        atoms = [RuleAtom("exact_word_count", 3)]
        valid_examples = [sample for sample in _SAMPLE_MESSAGES if message_matches_rule(atoms, sample)]
        invalid_examples = [sample for sample in _SAMPLE_MESSAGES if not message_matches_rule(atoms, sample)]

        self.assertFalse(_bundle_quality_ok(atoms, valid_examples, invalid_examples))

    def test_pattern_hunt_status_embed_clarifies_contains_digits_and_guess_flow(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": DummyChannel(),
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "coder_order": [coder.id],
                "current_coder_index": 0,
                "phase": "prompt",
                "guess_limit": 3,
                "guesses_used": 1,
                "strike_limit": 3,
                "strikes": 1,
                "turn_limit": 6,
                "turns_used": 2,
                "accepted_answers": [{"coder": coder.display_name, "answer": "7 foxes sprint!"}],
                "hint_text": None,
            },
        }

        embed = build_pattern_hunt_status_embed(game, public=False)

        values = "\n".join(field.value for field in embed.fields)
        self.assertIn("digits `0-9` only", values)
        self.assertIn("/hunt guess", values)

    async def test_pattern_hunt_valid_clue_advances_without_extra_acceptance_chatter(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": channel,
            "turn_task": None,
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "coder_order": [coder.id],
                "current_coder_index": 0,
                "phase": "answer",
                "rule_atoms": [RuleAtom("contains_digits")],
                "current_prompt": "digit clue",
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._advance_pattern_turn_locked", new=AsyncMock()) as advance:
            handled = await handle_pattern_hunt_message_locked(
                DummyMessage(channel=channel, author=coder, content="7 foxes sprint!"),
                99,
                game,
            )

        self.assertTrue(handled)
        self.assertEqual(channel.sent, [])
        self.assertEqual(game["pattern_hunt"]["accepted_answers"][0]["prompt"], "digit clue")
        advance.assert_awaited_once()

    async def test_pattern_hunt_retry_feedback_stays_non_leaky(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": channel,
            "turn_task": None,
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "coder_order": [coder.id],
                "current_coder_index": 0,
                "phase": "answer",
                "rule_atoms": [RuleAtom("contains_digits")],
                "current_prompt": "digit clue",
                "accepted_answers": [],
                "retry_used": False,
            },
        }

        handled = await handle_pattern_hunt_message_locked(
            DummyMessage(channel=channel, author=coder, content="Blue foxes sprint!"),
            99,
            game,
        )

        self.assertTrue(handled)
        retry_copy = channel.sent[-1][1]["embed"].description
        self.assertIn("Rewrite it once with a fresh clue only.", retry_copy)
        self.assertNotIn("rule", retry_copy.casefold())

    async def test_pattern_hunt_reveal_recap_uses_prompt_to_answer_wording(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": channel,
            "turn_task": None,
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "rule_atoms": [RuleAtom("contains_digits")],
                "accepted_answers": [{"coder": coder.display_name, "answer": "7 foxes sprint!", "prompt": "fox clue"}],
            },
        }

        with patch("babblebox.pattern_hunt_game.ge.cleanup_game", new=AsyncMock()):
            from babblebox.pattern_hunt_game import _finish_pattern_hunt_locked

            await _finish_pattern_hunt_locked(99, game, guesser_won=False, reason="Coders held the pattern.")

        recap = next(field.value for field in channel.sent[-1][1]["embed"].fields if field.name == "Clue Recap")
        self.assertIn("`fox clue` ->", recap)
        self.assertIn("7 foxes sprint!", recap)
        self.assertNotIn("Prompt:", recap)

    async def test_pattern_hunt_dm_failure_cleans_up_without_starting(self):
        guesser = DummyUser(10)
        coder_one = DummyUser(11)
        coder_two = DummyUser(12)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder_one, coder_two],
            "starting_players": [guesser, coder_one, coder_two],
            "channel": channel,
            "turn_task": None,
            "game_type": "pattern_hunt",
            "active": True,
            "closing": False,
            "lock": asyncio.Lock(),
        }
        fake_rng = types.SimpleNamespace(choice=lambda players: players[0], randrange=lambda *_args, **_kwargs: 42)

        with patch("babblebox.pattern_hunt_game.random.SystemRandom", return_value=fake_rng), patch(
            "babblebox.pattern_hunt_game.select_rule_bundle",
            return_value=([RuleAtom("contains_digits")], ["7 foxes sprint!", "12 bold owls watch."], "Blue bears bake bread!"),
        ), patch("babblebox.pattern_hunt_game.ge.cleanup_game", new=AsyncMock()) as cleanup:
            await start_pattern_hunt_game_locked(99, game)

        self.assertEqual(channel.sent[-1][1]["embed"].title, "DM Failure")
        self.assertIn("hidden rule got uneven", channel.sent[-1][1]["embed"].description)
        cleanup.assert_awaited_once_with(99)

    async def test_pattern_hunt_prompt_timeout_applies_a_strike(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": channel,
            "turn_task": None,
            "game_type": "pattern_hunt",
            "active": True,
            "closing": False,
            "lock": asyncio.Lock(),
            "turn_token": 7,
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "coder_order": [coder.id],
                "current_coder_index": 0,
                "phase": "prompt",
                "rule_atoms": [RuleAtom("contains_digits")],
            },
        }
        saved_games = ge.games
        ge.games = {99: game}
        try:
            with patch("babblebox.pattern_hunt_game.asyncio.sleep", new=AsyncMock()), patch(
                "babblebox.pattern_hunt_game._apply_pattern_strike_locked",
                new=AsyncMock(),
            ) as strike:
                await _pattern_hunt_prompt_timeout(99, 7)
        finally:
            ge.games = saved_games

        strike.assert_awaited_once()
        self.assertEqual(strike.await_args.kwargs["reason"], "The guesser ran out of time before asking for a clue.")

    async def test_pattern_hunt_answer_timeout_applies_a_strike(self):
        guesser = DummyUser(10)
        coder = DummyUser(11)
        channel = DummyChannel()
        game = {
            "players": [guesser, coder],
            "starting_players": [guesser, coder],
            "channel": channel,
            "turn_task": None,
            "game_type": "pattern_hunt",
            "active": True,
            "closing": False,
            "lock": asyncio.Lock(),
            "turn_token": 9,
            "pattern_hunt": {
                "guesser_id": guesser.id,
                "coder_order": [coder.id],
                "current_coder_index": 0,
                "phase": "answer",
                "rule_atoms": [RuleAtom("contains_digits")],
            },
        }
        saved_games = ge.games
        ge.games = {99: game}
        try:
            with patch("babblebox.pattern_hunt_game.asyncio.sleep", new=AsyncMock()), patch(
                "babblebox.pattern_hunt_game._apply_pattern_strike_locked",
                new=AsyncMock(),
            ) as strike:
                await _pattern_hunt_answer_timeout(99, 9)
        finally:
            ge.games = saved_games

        strike.assert_awaited_once()
        reason = strike.await_args.kwargs["reason"]
        self.assertIn(coder.mention, reason)
        self.assertIn("ran out of time", reason)
        self.assertIn("coder team took a strike", reason)

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
