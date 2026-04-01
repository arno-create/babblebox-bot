from __future__ import annotations

import asyncio
import contextlib
import types
import unittest
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import discord

from babblebox import game_engine as ge
from babblebox.question_drops_content import (
    answer_points_for_difficulty,
    build_variant,
    is_answer_attempt,
    judge_answer,
    normalize_answer_text,
    validate_content_pack,
)
from babblebox.question_drops_service import QuestionDropsService
from babblebox.question_drops_store import QuestionDropsStore


class DummyUser:
    def __init__(self, user_id: int, *, display_name: str | None = None):
        self.id = user_id
        self.display_name = display_name or f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.bot = False


class DummyChannel:
    def __init__(self, channel_id: int, *, fail_send: bool = False):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self.fail_send = fail_send

    async def send(self, *args, **kwargs):
        if self.fail_send:
            raise discord.HTTPException(types.SimpleNamespace(status=500, reason="fail"), "send failed")
        message = types.SimpleNamespace(id=5000 + len(self.sent), channel=self, kwargs=kwargs, delete=AsyncMock())
        self.sent.append((args, kwargs, message))
        return message


class DummyGuild:
    def __init__(self, guild_id: int, channels=None):
        self.id = guild_id
        self.name = "Guild"
        self._channels = {channel.id: channel for channel in (channels or [])}

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)


class DummyBot:
    def __init__(self, guild: DummyGuild, channels: list[DummyChannel]):
        self.guild = guild
        self.user = types.SimpleNamespace(id=999)
        self._channels = {channel.id: channel for channel in channels}
        self.profile_service = types.SimpleNamespace(
            storage_ready=True,
            record_question_drop_result=AsyncMock(),
            record_question_drop_results_batch=AsyncMock(),
            get_question_drop_summary=AsyncMock(
                return_value={"profile": {}, "categories": [], "top_categories": []}
            ),
        )

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_guild(self, guild_id: int):
        return self.guild if guild_id == self.guild.id else None


class DummyMessage:
    def __init__(self, *, guild: DummyGuild, channel: DummyChannel, author: DummyUser, content: str, reference=None):
        self.guild = guild
        self.channel = channel
        self.author = author
        self.content = content
        self.reference = reference


class DummyDeletePayload:
    def __init__(self, *, guild_id: int, message_id: int):
        self.guild_id = guild_id
        self.message_id = message_id


class QuestionDropsContentTests(unittest.TestCase):
    def test_content_pack_validates(self):
        ok, message = validate_content_pack()
        self.assertTrue(ok)
        self.assertIsNone(message)

    def test_generator_variants_are_deterministic(self):
        seed = {
            "concept_id": "math:addition",
            "category": "math",
            "difficulty": 1,
            "source_type": "generated",
            "generator_type": "math_addition",
            "variants": (),
        }
        first = build_variant(seed, seed_material="guild:slot:1", variant_index=0)
        second = build_variant(seed, seed_material="guild:slot:1", variant_index=0)
        third = build_variant(seed, seed_material="guild:slot:2", variant_index=0)
        self.assertEqual(first.prompt, second.prompt)
        self.assertEqual(first.answer_spec, second.answer_spec)
        self.assertNotEqual(first.prompt, third.prompt)

    def test_answer_judging_is_strict_for_numeric_and_natural_for_multiple_choice(self):
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Mars"]}, " mars!! "))
        self.assertTrue(judge_answer({"type": "numeric", "value": 42}, "The answer is 42."))
        self.assertFalse(judge_answer({"type": "numeric", "value": 42}, "42 or 43"))
        self.assertFalse(judge_answer({"type": "numeric", "value": 1989}, "19,89"))
        self.assertFalse(judge_answer({"type": "numeric", "value": 42}, "forty two"))
        multiple_choice = {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"}
        self.assertTrue(judge_answer(multiple_choice, "green"))
        self.assertTrue(judge_answer(multiple_choice, "C"))
        self.assertTrue(judge_answer(multiple_choice, "c) green"))
        self.assertFalse(judge_answer(multiple_choice, "c maybe"))
        self.assertTrue(judge_answer({"type": "ordered_tokens", "tokens": ["red", "blue", "green"]}, "red, blue, green"))
        self.assertEqual(normalize_answer_text("  Hello,  World! "), "hello world")

    def test_answer_attempt_gate_accepts_clean_guesses_and_rejects_chatter(self):
        text_spec = {"type": "text", "accepted": ["mars"]}
        self.assertTrue(is_answer_attempt(text_spec, "venus"))
        self.assertTrue(is_answer_attempt(text_spec, "guess venus"))
        self.assertTrue(is_answer_attempt(text_spec, "maybe venus", direct_reply=True))
        self.assertFalse(is_answer_attempt(text_spec, "wait that's wild"))
        self.assertFalse(is_answer_attempt(text_spec, "what do you mean"))

        numeric_spec = {"type": "numeric", "value": 16}
        self.assertTrue(is_answer_attempt(numeric_spec, "16"))
        self.assertTrue(is_answer_attempt(numeric_spec, "answer 16"))
        self.assertFalse(is_answer_attempt(numeric_spec, "numbers are hard lol"))

        multiple_choice = {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"}
        self.assertTrue(is_answer_attempt(multiple_choice, "option c"))
        self.assertTrue(is_answer_attempt(multiple_choice, "green"))
        self.assertFalse(is_answer_attempt(multiple_choice, "which one was c again"))


class QuestionDropsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.channel = DummyChannel(20)
        self.guild = DummyGuild(10, channels=[self.channel])
        self.bot = DummyBot(self.guild, [self.channel])
        self.store = QuestionDropsStore(backend="memory")
        await self.store.load()
        self.service = QuestionDropsService(self.bot, store=self.store)
        started = await self.service.start()
        self.assertTrue(started)
        if self.service._scheduler_task is not None:
            self.service._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.service._scheduler_task
            self.service._scheduler_task = None
        ok, message = await self.service.update_config(
            self.guild.id,
            enabled=True,
            drops_per_day=1,
            timezone_name="UTC",
            answer_window_seconds=60,
            activity_gate="off",
            active_start_hour=0,
            active_end_hour=23,
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.update_channels(self.guild.id, action="add", channel_id=self.channel.id)
        self.assertTrue(ok, message)

    async def asyncTearDown(self):
        await self.service.close()

    async def _post_one_drop(self):
        now = ge.now_utc().replace(second=0, microsecond=0)
        with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
            await self.service._maybe_post_due_drops()
        self.assertEqual(len(self.service._active_drops), 1)
        return next(iter(self.service._active_drops.values()))

    def _wrong_attempt_content(self, active: dict[str, object]) -> str:
        answer_spec = active["answer_spec"]
        answer_type = str(answer_spec.get("type"))
        if answer_type == "numeric":
            return str(int(answer_spec.get("value", 0)) + 1)
        if answer_type == "boolean":
            return "no" if bool(answer_spec.get("value")) else "yes"
        if answer_type == "multiple_choice":
            answer = normalize_answer_text(answer_spec.get("answer"))
            for index, choice in enumerate(answer_spec.get("choices", [])):
                if normalize_answer_text(choice) != answer:
                    return f"option {chr(ord('a') + index)}"
            return "option a"
        if answer_type == "ordered_tokens":
            tokens = [str(token) for token in answer_spec.get("tokens", [])]
            if len(tokens) >= 2:
                return ", ".join(reversed(tokens))
            return "alpha beta"
        accepted = {normalize_answer_text(item) for item in answer_spec.get("accepted", []) if isinstance(item, str)}
        for candidate in ("venus", "saturn", "alpha", "banana"):
            if normalize_answer_text(candidate) not in accepted:
                return f"guess {candidate}"
        return "guess zebra"

    def _last_batch_results(self):
        return self.bot.profile_service.record_question_drop_results_batch.await_args.args[0]

    async def test_selector_avoids_recent_concept_repeats(self):
        old_variant = self.service._select_variant(
            self.guild.id,
            self.channel.id,
            exposures=[],
            slot_key="2026-03-30:0",
            config=self.service.get_config(self.guild.id),
        )
        self.assertIsNotNone(old_variant)
        exposure = await self.store.insert_exposure(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "concept_id": old_variant.concept_id,
                "variant_hash": old_variant.variant_hash,
                "category": old_variant.category,
                "difficulty": old_variant.difficulty,
                "asked_at": ge.now_utc(),
                "resolved_at": None,
                "winner_user_id": None,
                "slot_key": "2026-03-29:0",
            }
        )
        next_variant = self.service._select_variant(
            self.guild.id,
            self.channel.id,
            exposures=[exposure],
            slot_key="2026-03-30:1",
            config=self.service.get_config(self.guild.id),
        )
        self.assertIsNotNone(next_variant)
        self.assertNotEqual(next_variant.concept_id, old_variant.concept_id)

    async def test_selector_avoids_same_day_concept_reuse_when_alternatives_exist(self):
        first_variant = self.service._select_variant(
            self.guild.id,
            self.channel.id,
            exposures=[],
            slot_key="2026-03-30:0",
            config=self.service.get_config(self.guild.id),
        )
        self.assertIsNotNone(first_variant)
        exposure = await self.store.insert_exposure(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "concept_id": first_variant.concept_id,
                "variant_hash": first_variant.variant_hash,
                "category": first_variant.category,
                "difficulty": first_variant.difficulty,
                "asked_at": ge.now_utc(),
                "resolved_at": None,
                "winner_user_id": None,
                "slot_key": "2026-03-30:1",
            }
        )

        next_variant = self.service._select_variant(
            self.guild.id,
            self.channel.id,
            exposures=[exposure],
            slot_key="2026-03-30:2",
            config=self.service.get_config(self.guild.id),
        )

        self.assertIsNotNone(next_variant)
        self.assertNotEqual(next_variant.concept_id, first_variant.concept_id)

    async def test_update_config_accepts_maximum_drop_range(self):
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=10)

        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_config(self.guild.id)["drops_per_day"], 10)

    async def test_update_config_rejects_out_of_range_drop_counts(self):
        for invalid in (0, 11):
            with self.subTest(invalid=invalid):
                ok, message = await self.service.update_config(self.guild.id, drops_per_day=invalid)

                self.assertFalse(ok)
                self.assertIn("between 1 and 10", message)
                self.assertEqual(self.service.get_config(self.guild.id)["drops_per_day"], 1)

    async def test_scheduler_posts_single_active_drop_and_correct_answer_resolves(self):
        active = await self._post_one_drop()
        answer = str(active["answer_spec"].get("value", active["answer_spec"].get("accepted", [""])[0]))
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(44), content=answer)

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.assertEqual(len(self.service._active_drops), 0)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 44,
                    "category": active["category"],
                    "correct": True,
                    "points": answer_points_for_difficulty(int(active["difficulty"])),
                }
            ],
        )

    async def test_wrong_feedback_is_rate_limited_and_attempts_only_count_once_per_user(self):
        await self.service.update_config(self.guild.id, tone_mode="playful")
        active = await self._post_one_drop()
        author = DummyUser(55)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))

        await self.service.handle_message(wrong)
        await self.service.handle_message(wrong)

        self.assertEqual(self.bot.profile_service.record_question_drop_results_batch.await_count, 0)
        exposure_id = int(active["exposure_id"])
        self.assertEqual(self.service._wrong_feedback_count[exposure_id], 1)
        self.assertEqual(self.service._attempted_users[exposure_id], {55})

    async def test_same_user_wrong_then_correct_counts_once_as_correct_participation(self):
        active = await self._post_one_drop()
        winner = DummyUser(55)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=self._wrong_attempt_content(active))
        correct = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=winner,
            content=str(active["answer_spec"].get("value", active["answer_spec"].get("accepted", [""])[0])),
        )

        await self.service.handle_message(wrong)
        await self.service.handle_message(correct)

        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 55,
                    "category": active["category"],
                    "correct": True,
                    "points": answer_points_for_difficulty(int(active["difficulty"])),
                }
            ],
        )

    async def test_expiring_drop_records_first_wrong_attempt_once(self):
        active = await self._post_one_drop()
        author = DummyUser(61)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))
        await self.service.handle_message(wrong)

        await self.service._expire_drop(active, timed_out=True)

        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 61,
                    "category": active["category"],
                    "correct": False,
                    "points": 0,
                }
            ],
        )

    async def test_send_failure_does_not_burn_slot_or_create_ghost_exposure(self):
        failing_channel = DummyChannel(77, fail_send=True)
        guild = DummyGuild(20, channels=[failing_channel])
        bot = DummyBot(guild, [failing_channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        try:
            self.assertTrue(await service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            await service.update_config(
                guild.id,
                enabled=True,
                drops_per_day=1,
                timezone_name="UTC",
                activity_gate="off",
                active_start_hour=0,
                active_end_hour=23,
            )
            await service.update_channels(guild.id, action="add", channel_id=failing_channel.id)
            now = ge.now_utc().replace(second=0, microsecond=0)
            with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
                await service._maybe_post_due_drops()

            self.assertEqual(len(await store.list_exposures_for_guild(guild.id)), 0)
            self.assertEqual(len(service._active_drops), 0)
        finally:
            await service.close()

    async def test_attach_failure_releases_pending_claim_and_deletes_post(self):
        active_store = QuestionDropsStore(backend="memory")
        await active_store.load()
        service = QuestionDropsService(self.bot, store=active_store)
        try:
            self.assertTrue(await service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            await service.update_config(
                self.guild.id,
                enabled=True,
                drops_per_day=1,
                timezone_name="UTC",
                activity_gate="off",
                active_start_hour=0,
                active_end_hour=23,
            )
            await service.update_channels(self.guild.id, action="add", channel_id=self.channel.id)
            service.store.attach_pending_post_message = AsyncMock(side_effect=RuntimeError("db attach failed"))
            now = ge.now_utc().replace(second=0, microsecond=0)

            with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
                await service._maybe_post_due_drops()

            self.assertEqual(len(await service.store.list_pending_posts()), 0)
            self.assertEqual(len(await service.store.list_exposures_for_guild(self.guild.id)), 0)
            self.assertEqual(len(service._active_drops), 0)
            self.channel.sent[-1][2].delete.assert_awaited_once()
        finally:
            await service.close()

    async def test_non_answer_chatter_does_not_count_as_attempt_or_feedback(self):
        active = await self._post_one_drop()
        author = DummyUser(56)
        chatter = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="wait that's wild")

        handled = await self.service.handle_message(chatter)

        self.assertFalse(handled)
        exposure_id = int(active["exposure_id"])
        self.assertEqual(self.service._wrong_feedback_count[exposure_id], 0)
        self.assertEqual(self.service._attempted_users[exposure_id], set())
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

    async def test_party_game_overlap_retires_live_drop_before_judging(self):
        active = await self._post_one_drop()
        saved_games = ge.games
        ge.games = {
            self.guild.id: {
                "channel": self.channel,
                "closing": False,
            }
        }
        try:
            handled = await self.service.handle_message(
                DummyMessage(
                    guild=self.guild,
                    channel=self.channel,
                    author=DummyUser(77),
                    content=self._wrong_attempt_content(active),
                )
            )
        finally:
            ge.games = saved_games

        self.assertFalse(handled)
        self.assertEqual(len(self.service._active_drops), 0)
        self.assertIsNone(await self.store.fetch_active_drop(self.guild.id, self.channel.id))

    async def test_startup_sweeps_stale_pending_posts(self):
        store = QuestionDropsStore(backend="memory")
        await store.load()
        claimed = await store.claim_pending_post(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "slot_key": "2026-04-01:0",
                "concept_id": "science:planet-red",
                "variant_hash": "pending-abc",
                "claimed_at": ge.now_utc() - timedelta(minutes=15),
                "lease_expires_at": ge.now_utc() - timedelta(minutes=10),
                "message_id": None,
            }
        )
        self.assertIsNotNone(claimed)
        store.load = AsyncMock()
        bot = DummyBot(self.guild, [self.channel])
        service = QuestionDropsService(bot, store=store)
        try:
            self.assertTrue(await service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None

            self.assertEqual(await store.list_pending_posts(), [])
            self.assertEqual(service._pending_posts, {})
        finally:
            await service.close()

    async def test_raw_delete_closes_active_drop(self):
        active = await self._post_one_drop()

        await self.service.handle_raw_message_delete(DummyDeletePayload(guild_id=self.guild.id, message_id=active["message_id"]))

        self.assertEqual(len(self.service._active_drops), 0)

    async def test_party_game_channel_blocks_drop(self):
        now = ge.now_utc().replace(second=0, microsecond=0)
        saved_games = ge.games
        ge.games = {
            self.guild.id: {
                "channel": self.channel,
                "closing": False,
            }
        }
        try:
            with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
                await self.service._maybe_post_due_drops()
        finally:
            ge.games = saved_games
        self.assertEqual(len(self.service._active_drops), 0)

    async def test_startup_sweeps_stale_active_rows(self):
        store = QuestionDropsStore(backend="memory")
        await store.load()
        exposure = await store.insert_exposure(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "concept_id": "science:planet-red",
                "variant_hash": "abc123",
                "category": "science",
                "difficulty": 1,
                "asked_at": ge.now_utc() - timedelta(minutes=5),
                "resolved_at": None,
                "winner_user_id": None,
                "slot_key": "2026-04-01:0",
            }
        )
        await store.upsert_active_drop(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "message_id": 5001,
                "author_user_id": 999,
                "exposure_id": int(exposure["id"]),
                "concept_id": "science:planet-red",
                "variant_hash": "abc123",
                "category": "science",
                "difficulty": 1,
                "prompt": "Which planet is known as the Red Planet?",
                "answer_spec": {"type": "text", "accepted": ["mars"]},
                "asked_at": ge.now_utc() - timedelta(minutes=5),
                "expires_at": ge.now_utc() - timedelta(seconds=1),
                "slot_key": "2026-04-01:0",
                "tone_mode": "clean",
                "participant_user_ids": [88],
            }
        )
        store.load = AsyncMock()
        bot = DummyBot(self.guild, [self.channel])
        service = QuestionDropsService(bot, store=store)
        try:
            self.assertTrue(await service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None

            self.assertEqual(len(service._active_drops), 0)
            exposures = await store.list_exposures_for_guild(self.guild.id)
            self.assertIsNotNone(exposures[0]["resolved_at"])
            bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
            self.assertEqual(
                bot.profile_service.record_question_drop_results_batch.await_args.args[0],
                [{"user_id": 88, "category": "science", "correct": False, "points": 0}],
            )
        finally:
            await service.close()
