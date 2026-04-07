from __future__ import annotations

import asyncio
import types
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, Mock, patch

import discord

from babblebox import game_engine as ge
from babblebox.pattern_hunt_game import (
    RuleAtom,
    _pattern_hunt_answer_timeout,
    _pattern_hunt_answer_timeout_seconds,
    _pattern_hunt_prompt_timeout,
    _pattern_hunt_prompt_timeout_seconds,
    _handle_pattern_penalty_locked,
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


class DummyDmUser(DummyUser):
    def __init__(self, user_id: int):
        super().__init__(user_id)
        self.send = AsyncMock()


class DummySentMessage:
    def __init__(self):
        self.edits = []
        self.edit_error = None

    async def edit(self, *args, **kwargs):
        if self.edit_error is not None:
            raise self.edit_error
        self.edits.append((args, kwargs))
        return self


class DummyChannel:
    def __init__(self, channel_id: int = 20):
        self.id = channel_id
        self.sent = []

    async def send(self, *args, **kwargs):
        self.sent.append((args, kwargs))
        return DummySentMessage()


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
    async def test_shared_game_anchor_recreates_after_missing_message(self):
        channel = DummyChannel()
        game = {"channel": channel, "state_anchors": {}}

        first_anchor = await ge.upsert_game_anchor(game, "test", embed=ge.make_status_embed("One", "First"))
        first_anchor.edit_error = discord.NotFound(Mock(status=404, reason="Not Found", text="gone"), "gone")

        second_anchor = await ge.upsert_game_anchor(game, "test", embed=ge.make_status_embed("Two", "Second"))

        self.assertEqual(len(channel.sent), 2)
        self.assertIs(second_anchor, game["state_anchors"]["test"])

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
                "clue_limit": 6,
                "clues_used": 2,
                "accepted_answers": [{"coder": coder.display_name, "answer": "7 foxes sprint!"}],
                "hint_text": None,
                "deadline_at": ge.now_utc() + timedelta(seconds=30),
                "tutorial_cycle_active": True,
            },
        }

        embed = build_pattern_hunt_status_embed(game, public=False)

        self.assertIn("Pattern Hunt", embed.title)
        self.assertIn("Who's Up", [field.name for field in embed.fields])
        self.assertIn("Time Left", [field.name for field in embed.fields])
        self.assertIn("What Happens Next", [field.name for field in embed.fields])
        values = "\n".join(field.value for field in embed.fields)
        self.assertIn("digits `0-9` only", values)
        self.assertIn("/hunt guess", values)
        self.assertIn("ask User 11 for one clue in chat", values)
        self.assertIn("Clues: **2/6**", values)
        self.assertIn("Misses left: **2**", values)

    def test_pattern_hunt_public_answer_embed_shows_current_ask_and_private_guess_note(self):
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
                "phase": "answer",
                "current_prompt": "animal clue",
                "guess_limit": 3,
                "guesses_used": 0,
                "strike_limit": 3,
                "strikes": 0,
                "clue_limit": 5,
                "clues_used": 1,
                "accepted_answers": [],
                "deadline_at": ge.now_utc() + timedelta(seconds=30),
                "tutorial_cycle_active": False,
            },
        }

        embed = build_pattern_hunt_status_embed(game, public=True)
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Current Ask", fields)
        self.assertIn("lock in a private theory with `/hunt guess`", fields["Do This Now"])
        self.assertIn("animal clue", fields["Current Ask"])
        self.assertIn("Once the clue lands, the next coder takes the room", fields["What Happens Next"])

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

    async def test_pattern_hunt_prompt_phase_rejects_low_signal_guesser_chatter(self):
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
                "phase": "prompt",
                "current_prompt": None,
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._start_pattern_answer_locked", new=AsyncMock()) as start_answer:
            handled = await handle_pattern_hunt_message_locked(
                DummyMessage(channel=channel, author=guesser, content="lol"),
                99,
                game,
            )

        self.assertTrue(handled)
        self.assertEqual(game["pattern_hunt"]["phase"], "prompt")
        self.assertIsNone(game["pattern_hunt"]["current_prompt"])
        self.assertEqual(channel.sent[-1][0][0], "Pattern Hunt: ask for one real clue request or theme so the coder has a clear lane.")
        self.assertEqual(channel.sent[-1][1]["delete_after"], 4.0)
        start_answer.assert_not_awaited()

    async def test_pattern_hunt_prompt_phase_rejects_whitespace_without_advancing(self):
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
                "phase": "prompt",
                "current_prompt": None,
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._start_pattern_answer_locked", new=AsyncMock()) as start_answer:
            handled = await handle_pattern_hunt_message_locked(
                DummyMessage(channel=channel, author=guesser, content="   "),
                99,
                game,
            )

        self.assertTrue(handled)
        self.assertEqual(game["pattern_hunt"]["phase"], "prompt")
        self.assertIsNone(game["pattern_hunt"]["current_prompt"])
        start_answer.assert_not_awaited()

    async def test_pattern_hunt_prompt_phase_accepts_natural_prompt_and_normalizes_spacing(self):
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
                "phase": "prompt",
                "current_prompt": None,
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._start_pattern_answer_locked", new=AsyncMock()) as start_answer:
            handled = await handle_pattern_hunt_message_locked(
                DummyMessage(channel=channel, author=guesser, content="  animal   clue?  "),
                99,
                game,
            )

        self.assertTrue(handled)
        self.assertEqual(game["pattern_hunt"]["current_prompt"], "animal clue?")
        start_answer.assert_awaited_once()

    async def test_pattern_hunt_invalid_prompt_feedback_is_throttled(self):
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
                "phase": "prompt",
                "current_prompt": None,
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._start_pattern_answer_locked", new=AsyncMock()) as start_answer:
            await handle_pattern_hunt_message_locked(DummyMessage(channel=channel, author=guesser, content="lol"), 99, game)
            await handle_pattern_hunt_message_locked(DummyMessage(channel=channel, author=guesser, content="ok"), 99, game)

        self.assertEqual(len(channel.sent), 1)
        start_answer.assert_not_awaited()

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
        self.assertIn("Send one fresh clue", retry_copy)
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

        embed = channel.sent[-1][1]["embed"]
        recap = next(field.value for field in embed.fields if field.name == "Recent Clues")
        self.assertIn("`fox clue` ->", recap)
        self.assertIn("7 foxes sprint!", recap)
        self.assertNotIn("Prompt:", recap)
        self.assertIn("Outcome", [field.name for field in embed.fields])

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
        self.assertIn("open server DMs", channel.sent[-1][1]["embed"].description)
        self.assertIn("start the room again", channel.sent[-1][1]["embed"].description)
        cleanup.assert_awaited_once_with(99)

    async def test_pattern_hunt_clue_budget_scales_with_coder_count(self):
        guesser = DummyUser(10)
        coder_one = DummyDmUser(11)
        coder_two = DummyDmUser(12)
        coder_three = DummyDmUser(13)
        channel = DummyChannel()
        game = {
            "host": guesser,
            "players": [guesser, coder_one, coder_two, coder_three],
            "starting_players": [guesser, coder_one, coder_two, coder_three],
            "channel": channel,
            "turn_task": None,
            "game_type": "pattern_hunt",
            "active": True,
            "closing": False,
            "lock": asyncio.Lock(),
            "stats_recorded": False,
        }
        fake_rng = types.SimpleNamespace(choice=lambda players: players[0], randrange=lambda *_args, **_kwargs: 42)

        with patch("babblebox.pattern_hunt_game.random.SystemRandom", return_value=fake_rng), patch(
            "babblebox.pattern_hunt_game.select_rule_bundle",
            return_value=([RuleAtom("contains_digits")], ["7 foxes sprint!", "12 bold owls watch."], "Blue bears bake bread!"),
        ), patch("babblebox.pattern_hunt_game._begin_pattern_turn_locked", new=AsyncMock()) as begin:
            await start_pattern_hunt_game_locked(99, game)

        self.assertEqual(game["pattern_hunt"]["clue_limit"], 7)
        dm_embed = coder_one.send.await_args.kwargs["embed"]
        self.assertIn("Pattern Hunt Role", dm_embed.title)
        self.assertIn("Keep the logic offstage.", next(field.value for field in dm_embed.fields if field.name == "Guardrails"))
        begin.assert_awaited_once()

    async def test_pattern_hunt_tutorial_grace_absorbs_first_penalty(self):
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
                "phase": "prompt",
                "rule_atoms": [RuleAtom("contains_digits")],
                "tutorial_cycle_active": True,
                "tutorial_grace_used": False,
                "accepted_answers": [],
            },
        }

        with patch("babblebox.pattern_hunt_game._begin_pattern_turn_locked", new=AsyncMock()) as begin:
            await _handle_pattern_penalty_locked(99, game, reason="The guesser stalled.", reset_phase="prompt")

        self.assertTrue(game["pattern_hunt"]["tutorial_grace_used"])
        self.assertEqual(game["pattern_hunt"].get("strikes", 0), 0)
        self.assertIn("Opening Grace", channel.sent[-1][1]["embed"].title)
        self.assertIn("Same guesser, fresh prompt timer.", channel.sent[-1][1]["embed"].description)
        begin.assert_awaited_once()

    def test_pattern_turn_deadlines_use_tutorial_then_standard_windows(self):
        tutorial_state = {"tutorial_cycle_active": True}
        standard_state = {"tutorial_cycle_active": False}

        self.assertEqual(_pattern_hunt_prompt_timeout_seconds(tutorial_state), 90)
        self.assertEqual(_pattern_hunt_prompt_timeout_seconds(standard_state), 75)
        self.assertEqual(_pattern_hunt_answer_timeout_seconds(tutorial_state), 75)
        self.assertEqual(_pattern_hunt_answer_timeout_seconds(standard_state), 60)

    async def test_pattern_hunt_second_penalty_becomes_team_miss(self):
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
                "phase": "prompt",
                "rule_atoms": [RuleAtom("contains_digits")],
                "tutorial_cycle_active": True,
                "tutorial_grace_used": True,
                "accepted_answers": [],
                "strike_limit": 3,
                "strikes": 0,
            },
        }

        with patch("babblebox.pattern_hunt_game._advance_pattern_turn_locked", new=AsyncMock()) as advance:
            await _handle_pattern_penalty_locked(99, game, reason="The guesser stalled.", reset_phase="prompt")

        self.assertEqual(game["pattern_hunt"]["strikes"], 1)
        self.assertEqual(channel.sent[-1][1]["embed"].title, "⚠️ Missed Beat")
        advance.assert_awaited_once()

    def test_pattern_hunt_timeout_profiles_use_tutorial_then_standard_windows(self):
        state = {"tutorial_cycle_active": True}
        self.assertEqual(_pattern_hunt_prompt_timeout_seconds(state), 90)
        self.assertEqual(_pattern_hunt_answer_timeout_seconds(state), 75)
        state["tutorial_cycle_active"] = False
        self.assertEqual(_pattern_hunt_prompt_timeout_seconds(state), 75)
        self.assertEqual(_pattern_hunt_answer_timeout_seconds(state), 60)

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
                "tutorial_cycle_active": True,
            },
        }
        saved_games = ge.games
        ge.games = {99: game}
        try:
            with patch("babblebox.pattern_hunt_game.asyncio.sleep", new=AsyncMock()), patch(
                "babblebox.pattern_hunt_game._handle_pattern_penalty_locked",
                new=AsyncMock(),
            ) as penalty:
                await _pattern_hunt_prompt_timeout(99, 7, 60)
        finally:
            ge.games = saved_games

        penalty.assert_awaited_once()
        self.assertEqual(penalty.await_args.kwargs["reason"], "The guesser ran out of time to ask for a clue.")

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
                "tutorial_cycle_active": True,
            },
        }
        saved_games = ge.games
        ge.games = {99: game}
        try:
            with patch("babblebox.pattern_hunt_game.asyncio.sleep", new=AsyncMock()), patch(
                "babblebox.pattern_hunt_game._handle_pattern_penalty_locked",
                new=AsyncMock(),
            ) as penalty:
                await _pattern_hunt_answer_timeout(99, 9, 50)
        finally:
            ge.games = saved_games

        penalty.assert_awaited_once()
        reason = penalty.await_args.kwargs["reason"]
        self.assertIn(coder.mention, reason)
        self.assertIn("ran out of time", reason)
        self.assertIn("send a clue", reason)

    async def test_pattern_guess_compares_structured_atoms(self):
        guesser = DummyUser(10)
        game = {
            "channel": DummyChannel(),
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
        self.assertEqual(message, "You cracked it.")
        finish.assert_awaited_once()

    async def test_pattern_wrong_guess_spends_budget(self):
        guesser = DummyUser(10)
        game = {
            "channel": DummyChannel(),
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
