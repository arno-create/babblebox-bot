from __future__ import annotations

import asyncio
import types
import unittest
import contextlib
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import discord

from babblebox import game_engine as ge
from babblebox.question_drops_content import (
    answer_points_for_difficulty,
    build_variant,
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
    def __init__(self, channel_id: int):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []

    async def send(self, *args, **kwargs):
        message = types.SimpleNamespace(id=5000 + len(self.sent), channel=self, kwargs=kwargs)
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
            get_question_drop_summary=AsyncMock(
                return_value={"profile": {}, "categories": [], "top_categories": []}
            ),
        )

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_guild(self, guild_id: int):
        return self.guild if guild_id == self.guild.id else None


class DummyMessage:
    def __init__(self, *, guild: DummyGuild, channel: DummyChannel, author: DummyUser, content: str):
        self.guild = guild
        self.channel = channel
        self.author = author
        self.content = content


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

    def test_answer_judging_stays_strict_but_normalized(self):
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Mars"]}, " mars!! "))
        self.assertTrue(judge_answer({"type": "numeric", "value": 42}, "The answer is 42."))
        self.assertFalse(judge_answer({"type": "numeric", "value": 42}, "forty two"))
        self.assertTrue(judge_answer({"type": "ordered_tokens", "tokens": ["red", "blue", "green"]}, "red, blue, green"))
        self.assertEqual(normalize_answer_text("  Hello,  World! "), "hello world")


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

    async def test_selector_avoids_recent_concept_repeats(self):
        old_variant = self.service._select_variant(self.guild.id, self.channel.id, exposures=[], slot_key="2026-03-30:0", config=self.service.get_config(self.guild.id))
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

    async def test_scheduler_posts_single_active_drop_and_correct_answer_resolves(self):
        now = ge.now_utc().replace(second=0, microsecond=0)
        with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
            await self.service._maybe_post_due_drops()
            await self.service._maybe_post_due_drops()

        self.assertEqual(len(self.channel.sent), 1)
        self.assertEqual(len(self.service._active_drops), 1)
        active = next(iter(self.service._active_drops.values()))
        answer = str(active["answer_spec"].get("value", active["answer_spec"].get("accepted", [""])[0]))
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(44), content=answer)

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.assertEqual(len(self.service._active_drops), 0)
        self.bot.profile_service.record_question_drop_result.assert_any_await(
            44,
            category=active["category"],
            correct=True,
            points=answer_points_for_difficulty(int(active["difficulty"])),
        )

    async def test_wrong_feedback_is_rate_limited_and_attempts_only_count_once_per_user(self):
        now = ge.now_utc().replace(second=0, microsecond=0)
        await self.service.update_config(self.guild.id, tone_mode="playful")
        with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
            await self.service._maybe_post_due_drops()
        active = next(iter(self.service._active_drops.values()))
        author = DummyUser(55)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="wrong answer")

        await self.service.handle_message(wrong)
        await self.service.handle_message(wrong)

        await_calls = self.bot.profile_service.record_question_drop_result.await_args_list
        wrong_attempts = [call for call in await_calls if call.args[0] == 55 and call.kwargs["correct"] is False]
        self.assertEqual(len(wrong_attempts), 1)
        exposure_id = int(active["exposure_id"])
        self.assertEqual(self.service._wrong_feedback_count[exposure_id], 1)

    async def test_raw_delete_closes_active_drop(self):
        now = ge.now_utc().replace(second=0, microsecond=0)
        with patch("babblebox.question_drops_service._daily_slot_datetimes", return_value=[now.astimezone(ge.now_utc().tzinfo)]):
            await self.service._maybe_post_due_drops()
        active = next(iter(self.service._active_drops.values()))

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
