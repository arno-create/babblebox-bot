import types
import unittest
from datetime import timedelta

from babblebox.daily_challenges import build_daily_shuffle
from babblebox.profile_service import (
    DAILY_CLEAR_XP,
    DAILY_PARTICIPATION_XP,
    GAME_PLAY_XP,
    UTILITY_ACTION_XP,
    ProfileService,
)
from babblebox.profile_store import ProfileStore
from babblebox import game_engine as ge


class ProfileServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        bot = types.SimpleNamespace(get_user=lambda user_id: None)
        store = ProfileStore(backend="memory")
        self.service = ProfileService(bot, store=store)
        started = await self.service.start()
        self.assertTrue(started)

    async def test_daily_guess_flow_updates_profile_and_share_output(self):
        status = await self.service.get_daily_status(11)
        puzzle = status["puzzles"]["shuffle"]

        ok, payload = await self.service.submit_daily_guess(11, puzzle.answer)
        self.assertTrue(ok)
        self.assertEqual(payload["status"], "solved")
        self.assertEqual(payload["result"]["attempt_count"], 1)
        self.assertEqual(payload["profile"]["total_daily_participations"], 1)
        self.assertEqual(payload["profile"]["total_daily_clears"], 1)
        self.assertEqual(payload["profile"]["active_streak"], 1)
        self.assertEqual(payload["profile"]["xp_total"], DAILY_PARTICIPATION_XP + DAILY_CLEAR_XP)

        ok, share_text = await self.service.build_daily_share(11)
        self.assertTrue(ok)
        self.assertIn("Babblebox Daily Arcade", share_text)
        self.assertIn("1/3", share_text)
        self.assertIn("\U0001f7e9", share_text)

    async def test_daily_duplicate_submission_is_blocked_after_solve(self):
        puzzle = build_daily_shuffle(ge.now_utc().date())
        ok, _ = await self.service.submit_daily_guess(22, puzzle.answer)
        self.assertTrue(ok)

        ok, message = await self.service.submit_daily_guess(22, puzzle.answer)
        self.assertFalse(ok)
        self.assertIn("already solved", message)

    async def test_daily_arcade_supports_multiple_modes(self):
        status = await self.service.get_daily_status(77)
        self.assertEqual(set(status["puzzles"].keys()), {"shuffle", "emoji", "signal"})
        emoji_puzzle = status["puzzles"]["emoji"]
        ok, payload = await self.service.submit_daily_guess(77, emoji_puzzle.answer, mode="emoji")
        self.assertTrue(ok)
        self.assertEqual(payload["puzzle"].mode, "emoji")
        ok, share_text = await self.service.build_daily_share(77, mode="emoji")
        self.assertTrue(ok)
        self.assertIn("Emoji Booth", share_text)

    async def test_public_daily_embeds_hide_failed_answer(self):
        status = await self.service.get_daily_status(80, mode="shuffle")
        puzzle = status["puzzle"]
        await self.service.submit_daily_guess(80, "wrong", mode="shuffle")
        await self.service.submit_daily_guess(80, "still wrong", mode="shuffle")
        ok, payload = await self.service.submit_daily_guess(80, "last wrong", mode="shuffle")
        self.assertTrue(ok)
        self.assertEqual(payload["status"], "failed")

        user = types.SimpleNamespace(display_name="User 80")
        public_open = self.service.build_daily_embed(user, payload, public=True)
        private_open = self.service.build_daily_embed(user, payload, public=False)
        public_result = self.service.build_daily_result_embed(user, payload, public=True)
        private_result = self.service.build_daily_result_embed(user, payload, public=False)

        self.assertNotIn(puzzle.answer.upper(), public_open.description)
        self.assertIn(puzzle.answer.upper(), private_open.description)
        self.assertNotIn(puzzle.answer.upper(), public_result.description)
        self.assertIn(puzzle.answer.upper(), private_result.description)

    async def test_buddy_defaults_and_style_change_work(self):
        profile = await self.service.get_profile(33)
        self.assertIsNotNone(profile)
        self.assertIn(profile["buddy_style"], {"mint", "sunset", "sky", "midnight"})
        self.assertTrue(profile["buddy_name"])

        ok, message = await self.service.rename_buddy(33, "Pebble")
        self.assertTrue(ok)
        self.assertIn("Pebble", message)

        ok, message = await self.service.set_buddy_style(33, "sunset")
        self.assertTrue(ok)
        self.assertIn("Sunset", message)

        updated = await self.service.get_profile(33)
        self.assertEqual(updated["buddy_name"], "Pebble")
        self.assertEqual(updated["buddy_style"], "sunset")

    async def test_utility_and_game_xp_are_capped_per_day(self):
        await self.service.record_utility_action(44, "later")
        await self.service.record_utility_action(44, "capture")
        await self.service.record_utility_action(44, "reminder")
        profile = await self.service.get_profile(44)
        self.assertEqual(profile["xp_total"], UTILITY_ACTION_XP * 2)
        self.assertEqual(profile["later_saves"], 1)
        self.assertEqual(profile["capture_uses"], 1)
        self.assertEqual(profile["reminders_created"], 1)

        await self.service.record_game_started(game_type="bomb", host_id=44, player_ids=[44])
        await self.service.record_game_started(game_type="bomb", host_id=44, player_ids=[44])
        await self.service.record_game_started(game_type="bomb", host_id=44, player_ids=[44])
        await self.service.record_game_started(game_type="bomb", host_id=44, player_ids=[44])
        updated = await self.service.get_profile(44)
        self.assertEqual(updated["games_played"], 4)
        self.assertEqual(updated["bomb_rounds"], 4)
        self.assertEqual(updated["xp_total"], (UTILITY_ACTION_XP * 2) + (GAME_PLAY_XP * 3))

    async def test_memory_store_prunes_old_daily_rows(self):
        today = ge.now_utc().date()
        old_date = today - timedelta(days=400)
        recent_date = today - timedelta(days=2)
        challenge_id = build_daily_shuffle(today).challenge_id
        await self.service.store.save_daily_result(
            {
                "challenge_id": challenge_id,
                "puzzle_date": old_date,
                "user_id": 99,
                "attempt_count": 3,
                "solved": False,
                "first_attempt_at": ge.now_utc(),
                "completed_at": ge.now_utc(),
                "solve_seconds": None,
            }
        )
        await self.service.store.save_daily_result(
            {
                "challenge_id": challenge_id,
                "puzzle_date": recent_date,
                "user_id": 99,
                "attempt_count": 1,
                "solved": True,
                "first_attempt_at": ge.now_utc(),
                "completed_at": ge.now_utc(),
                "solve_seconds": 12,
            }
        )

        removed = await self.service.store.prune_daily_results(challenge_id=challenge_id, keep_after=today - timedelta(days=180))
        self.assertEqual(removed, 1)
        self.assertIsNone(
            await self.service.store.fetch_daily_result(challenge_id=challenge_id, puzzle_date=old_date, user_id=99)
        )
        self.assertIsNotNone(
            await self.service.store.fetch_daily_result(challenge_id=challenge_id, puzzle_date=recent_date, user_id=99)
        )

    async def test_question_drop_results_update_profile_and_category_summary(self):
        await self.service.record_question_drop_result(55, category="logic", correct=True, points=12)
        await self.service.record_question_drop_result(55, category="logic", correct=True, points=8)
        await self.service.record_question_drop_result(55, category="science", correct=False, points=10)

        profile = await self.service.get_profile(55)
        self.assertEqual(profile["question_drop_attempts"], 3)
        self.assertEqual(profile["question_drop_correct"], 2)
        self.assertEqual(profile["question_drop_points"], 20)
        self.assertEqual(profile["question_drop_current_streak"], 0)
        self.assertEqual(profile["question_drop_best_streak"], 2)

        summary = await self.service.get_question_drop_summary(55)
        self.assertIsNotNone(summary)
        self.assertEqual(len(summary["categories"]), 2)
        self.assertEqual(summary["categories"][0]["category"], "logic")
        self.assertEqual(summary["categories"][0]["points"], 20)
        self.assertEqual(summary["categories"][0]["best_streak"], 2)
        self.assertEqual(summary["categories"][1]["category"], "science")
        self.assertEqual(summary["categories"][1]["attempts"], 1)
        self.assertEqual(summary["categories"][1]["correct_count"], 0)

    async def test_question_drop_batch_results_dedupe_user_updates(self):
        await self.service.record_question_drop_results_batch(
            [
                {"user_id": 55, "category": "logic", "correct": False, "points": 0},
                {"user_id": 55, "category": "logic", "correct": True, "points": 12},
                {"user_id": 55, "category": "logic", "correct": True, "points": 8},
                {"user_id": 55, "category": "science", "correct": False, "points": 10},
            ]
        )

        profile = await self.service.get_profile(55)
        self.assertEqual(profile["question_drop_attempts"], 2)
        self.assertEqual(profile["question_drop_correct"], 1)
        self.assertEqual(profile["question_drop_points"], 12)
        self.assertEqual(profile["question_drop_current_streak"], 0)
        self.assertEqual(profile["question_drop_best_streak"], 1)

        summary = await self.service.get_question_drop_summary(55)
        self.assertEqual(summary["categories"][0]["category"], "logic")
        self.assertEqual(summary["categories"][0]["attempts"], 1)
        self.assertEqual(summary["categories"][0]["correct_count"], 1)
        self.assertEqual(summary["categories"][0]["points"], 12)
        self.assertEqual(summary["categories"][1]["category"], "science")
        self.assertEqual(summary["categories"][1]["attempts"], 1)
        self.assertEqual(summary["categories"][1]["correct_count"], 0)

    async def test_profile_embed_uses_clean_question_drop_copy(self):
        await self.service.record_question_drop_result(88, category="logic", correct=True, points=12)
        profile = await self.service.get_profile(88)
        user = types.SimpleNamespace(display_name="User 88")

        embed = self.service.build_profile_embed(user, profile, utility_summary=None, session_stats=None)
        question_drops = next(field.value for field in embed.fields if field.name == "Question Drops")

        self.assertIn("Solved: **1 / 1 drops**", question_drops)
        self.assertNotIn("participated drops", question_drops)
        self.assertIn("party highlights", embed.description)

    async def test_new_party_game_round_and_win_fields_are_recorded(self):
        await self.service.record_game_started(game_type="only16", host_id=70, player_ids=[70, 71])
        await self.service.record_game_started(game_type="pattern_hunt", host_id=70, player_ids=[70, 71, 72])
        await self.service.record_only16_win(71)
        await self.service.record_pattern_hunt_win(72)

        host = await self.service.get_profile(70)
        only16_winner = await self.service.get_profile(71)
        hunt_winner = await self.service.get_profile(72)

        self.assertEqual(host["only16_rounds"], 1)
        self.assertEqual(host["pattern_hunt_rounds"], 1)
        self.assertEqual(host["games_hosted"], 2)
        self.assertEqual(only16_winner["only16_wins"], 1)
        self.assertEqual(only16_winner["games_won"], 1)
        self.assertEqual(hunt_winner["pattern_hunt_wins"], 1)
        self.assertEqual(hunt_winner["games_won"], 1)
