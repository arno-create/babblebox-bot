from __future__ import annotations

import asyncio
import contextlib
import json
import types
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import discord

from babblebox import game_engine as ge
from babblebox.question_drops_content import (
    QUESTION_DROP_CATEGORIES,
    QUESTION_DROP_DIFFICULTY_PROFILES,
    QUESTION_DROP_SEEDS,
    QuestionDropVariant,
    _language_anagram,
    _math_average_or_median,
    _math_divisibility,
    _math_percent_change,
    answer_points_for_difficulty,
    build_variant,
    is_answer_attempt,
    iter_candidate_variants,
    judge_answer,
    normalize_answer_text,
    question_drop_seed_for_concept,
    render_answer_instruction,
    render_answer_summary,
    validate_content_pack,
)
from babblebox.question_drops_service import QuestionDropsService
from babblebox.question_drops_store import QuestionDropsStore, _active_drop_from_row, _config_from_row
from babblebox.profile_service import ProfileService
from babblebox.profile_store import ProfileStore


class DummyRole:
    def __init__(self, role_id: int, *, position: int = 10, name: str | None = None):
        self.id = role_id
        self.position = position
        self.name = name or f"Role {role_id}"


class DummyBotMember:
    def __init__(self, user_id: int = 999, *, position: int = 50):
        self.id = user_id
        self.top_role = DummyRole(100000 + user_id, position=position)
        self.guild_permissions = types.SimpleNamespace(manage_roles=True)


class DummyUser:
    def __init__(self, user_id: int, *, display_name: str | None = None, name: str | None = None, roles=None, bot: bool = False):
        self.id = user_id
        self.display_name = display_name or f"User {user_id}"
        self.name = name or self.display_name
        self.mention = f"<@{user_id}>"
        self.bot = bot
        self.roles = list(roles or [])
        self.add_roles = AsyncMock(side_effect=self._add_roles)
        self.remove_roles = AsyncMock(side_effect=self._remove_roles)

    async def _add_roles(self, *roles, reason: str | None = None):
        for role in roles:
            if all(int(getattr(existing, "id", 0) or 0) != int(getattr(role, "id", 0) or 0) for existing in self.roles):
                self.roles.append(role)

    async def _remove_roles(self, *roles, reason: str | None = None):
        remove_ids = {int(getattr(role, "id", 0) or 0) for role in roles}
        self.roles = [role for role in self.roles if int(getattr(role, "id", 0) or 0) not in remove_ids]


class DummyChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        fail_send: bool = False,
        can_view: bool = True,
        can_send: bool = True,
        can_embed: bool = True,
    ):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self.fail_send = fail_send
        self.can_view = can_view
        self.can_send = can_send
        self.can_embed = can_embed

    async def send(self, *args, **kwargs):
        if self.fail_send:
            raise discord.HTTPException(types.SimpleNamespace(status=500, reason="fail"), "send failed")
        message = types.SimpleNamespace(id=5000 + len(self.sent), channel=self, kwargs=kwargs, delete=AsyncMock())
        self.sent.append((args, kwargs, message))
        return message

    def permissions_for(self, member):
        return types.SimpleNamespace(
            view_channel=self.can_view,
            send_messages=self.can_send,
            embed_links=self.can_embed,
        )


class DummyGuild:
    def __init__(self, guild_id: int, channels=None, *, roles=None, members=None, me=None):
        self.id = guild_id
        self.name = "Guild"
        self._channels = {channel.id: channel for channel in (channels or [])}
        self._roles = {role.id: role for role in (roles or [])}
        self.me = me or DummyBotMember()
        member_list = list(members or [])
        self._members = {member.id: member for member in member_list}
        if self.me is not None:
            self._members[self.me.id] = self.me
        self.members = [member for member in self._members.values() if member is not self.me]

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_role(self, role_id: int):
        return self._roles.get(role_id)

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class DummyBot:
    def __init__(self, guild: DummyGuild, channels: list[DummyChannel]):
        self.guild = guild
        self.user = types.SimpleNamespace(id=999)
        self._channels = {channel.id: channel for channel in channels}
        self.profile_service = types.SimpleNamespace(
            storage_ready=True,
            record_question_drop_result=AsyncMock(),
            record_question_drop_results_batch=AsyncMock(),
            backfill_question_drop_guild_points_from_exposures=AsyncMock(),
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
    def _seed_for(self, concept_id: str) -> dict[str, object]:
        seed = question_drop_seed_for_concept(concept_id)
        self.assertIsNotNone(seed)
        return seed

    def _find_variant_by_prompt(self, concept_id: str, builder, prompt: str) -> QuestionDropVariant:
        seed = self._seed_for(concept_id)
        for variant_index in range(400):
            variant = builder(seed, seed_material="regression", variant_index=variant_index)
            if variant.prompt == prompt:
                return variant
        self.fail(f"Could not build expected prompt for {concept_id}: {prompt}")

    def _correct_answer_text(self, answer_spec: dict[str, object]) -> str:
        answer_type = str(answer_spec.get("type"))
        if answer_type == "text":
            return str(answer_spec.get("accepted", [""])[0])
        if answer_type == "numeric":
            value = answer_spec.get("value", 0)
            if isinstance(value, float) and value.is_integer():
                value = int(value)
            return str(value)
        if answer_type == "boolean":
            return "true" if bool(answer_spec.get("value")) else "false"
        if answer_type == "multiple_choice":
            return str(answer_spec.get("answer", ""))
        if answer_type == "ordered_tokens":
            return ", ".join(str(token) for token in answer_spec.get("tokens", []))
        raise AssertionError(f"Unsupported answer spec for test helper: {answer_spec}")

    def test_content_pack_validates(self):
        ok, message = validate_content_pack()
        self.assertTrue(ok)
        self.assertIsNone(message)

    def test_content_pack_has_family_depth_and_real_hard_inventory(self):
        self.assertGreaterEqual(len(QUESTION_DROP_SEEDS), 60)
        hard_seeds = [seed for seed in QUESTION_DROP_SEEDS if int(seed["difficulty"]) == 3]
        self.assertGreaterEqual(len(hard_seeds), 20)
        self.assertTrue(all(str(seed.get("family_id") or "").strip() for seed in QUESTION_DROP_SEEDS))
        for category in QUESTION_DROP_CATEGORIES:
            with self.subTest(category=category):
                self.assertGreaterEqual(
                    sum(1 for seed in hard_seeds if seed["category"] == category),
                    1,
                )
        self.assertGreaterEqual(
            len({seed["family_id"] for seed in QUESTION_DROP_SEEDS if seed["category"] == "math"}),
            10,
        )
        self.assertGreaterEqual(
            len({seed["family_id"] for seed in QUESTION_DROP_SEEDS if seed["category"] == "logic"}),
            10,
        )

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

    def test_generated_families_are_deterministic_and_judgeable(self):
        generated_seeds = [seed for seed in QUESTION_DROP_SEEDS if seed["source_type"] == "generated"]
        self.assertGreaterEqual(len(generated_seeds), 20)

        seen_generators: set[str] = set()
        for seed in generated_seeds:
            with self.subTest(concept_id=seed["concept_id"]):
                first = build_variant(seed, seed_material=f"{seed['concept_id']}:slot", variant_index=1)
                second = build_variant(seed, seed_material=f"{seed['concept_id']}:slot", variant_index=1)
                self.assertEqual(first.prompt, second.prompt)
                self.assertEqual(first.answer_spec, second.answer_spec)
                self.assertEqual(first.family_id, seed["family_id"])
                self.assertTrue(judge_answer(first.answer_spec, self._correct_answer_text(first.answer_spec)))
                seen_generators.add(str(seed["generator_type"]))

        expected_math_generators = {
            "math_addition",
            "math_multiplication",
            "math_order_operations",
            "math_missing_value",
            "math_compare_expressions",
            "math_multi_step",
            "math_divisibility",
            "math_remainder",
            "math_percent_change",
            "math_average_or_median",
            "math_algebra_lite",
            "math_number_pattern",
        }
        expected_logic_generators = {
            "logic_sequence",
            "logic_analogy",
            "logic_odd_one_out",
            "logic_elimination",
            "logic_conditional",
            "logic_parity_grouping",
            "logic_true_false",
            "logic_classification",
            "logic_mini_deduction",
            "logic_rotation",
        }
        self.assertTrue(expected_math_generators.issubset(seen_generators))
        self.assertTrue(expected_logic_generators.issubset(seen_generators))

    def test_candidate_iteration_preserves_family_ids_for_generated_depth(self):
        variants = iter_candidate_variants(categories={"math", "logic"}, seed_material="coverage", variants_per_seed=3)
        self.assertTrue(variants)
        self.assertTrue(all(variant.family_id for variant in variants))
        self.assertGreaterEqual(len({variant.family_id for variant in variants if variant.category == "math"}), 10)
        self.assertGreaterEqual(len({variant.family_id for variant in variants if variant.category == "logic"}), 10)

    def test_answer_judging_is_strict_for_numeric_and_natural_for_multiple_choice(self):
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Mars"]}, " mars!! "))
        self.assertTrue(judge_answer({"type": "numeric", "value": 42}, "The answer is 42."))
        self.assertFalse(judge_answer({"type": "numeric", "value": 42}, "42 or 43"))
        self.assertFalse(judge_answer({"type": "numeric", "value": 1989}, "19,89"))
        self.assertTrue(judge_answer({"type": "numeric", "value": 42}, "forty two"))
        self.assertTrue(judge_answer({"type": "numeric", "value": 42}, "answer: forty two"))
        self.assertFalse(judge_answer({"type": "numeric", "value": 42}, "I have 42 cats"))
        self.assertTrue(judge_answer({"type": "boolean", "value": True}, "it's true"))
        multiple_choice = {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"}
        self.assertTrue(judge_answer(multiple_choice, "green"))
        self.assertTrue(judge_answer(multiple_choice, "C"))
        self.assertTrue(judge_answer(multiple_choice, "c) green"))
        self.assertFalse(judge_answer(multiple_choice, "c maybe"))
        self.assertTrue(judge_answer({"type": "ordered_tokens", "tokens": ["red", "blue", "green"]}, "red, blue, green"))
        self.assertFalse(judge_answer({"type": "ordered_tokens", "tokens": ["red", "blue", "green"]}, "green, blue, red"))
        self.assertEqual(normalize_answer_text("  Hello,  World! "), "hello world")

    def test_decimal_numeric_judging_accepts_equivalent_clean_inputs_only(self):
        answer_spec = {"type": "numeric", "value": 14.4}

        self.assertTrue(judge_answer(answer_spec, "14.4"))
        self.assertTrue(judge_answer(answer_spec, "14.40"))
        self.assertTrue(judge_answer(answer_spec, "answer: 14.4"))
        self.assertFalse(judge_answer(answer_spec, "14"))
        self.assertFalse(judge_answer(answer_spec, "15"))
        self.assertFalse(judge_answer(answer_spec, "fourteen point four"))

    def test_decimal_numeric_attempt_gate_and_rendering_stay_truthful(self):
        answer_spec = {"type": "numeric", "value": 14.4}

        self.assertTrue(is_answer_attempt(answer_spec, "14.4"))
        self.assertTrue(is_answer_attempt(answer_spec, "answer 14.40"))
        self.assertFalse(is_answer_attempt(answer_spec, "fourteen point four"))
        self.assertIn("Use digits for decimals", render_answer_instruction(answer_spec))
        self.assertEqual(render_answer_summary(answer_spec), "14.4")

    def test_answer_attempt_gate_accepts_clean_guesses_and_rejects_chatter(self):
        text_spec = {"type": "text", "accepted": ["mars"]}
        self.assertTrue(is_answer_attempt(text_spec, "venus"))
        self.assertTrue(is_answer_attempt(text_spec, "guess venus"))
        self.assertTrue(is_answer_attempt(text_spec, "maybe venus", direct_reply=True))
        self.assertFalse(is_answer_attempt(text_spec, "wait that's wild"))
        self.assertFalse(is_answer_attempt(text_spec, "what do you mean"))
        self.assertFalse(is_answer_attempt(text_spec, "no way"))
        self.assertFalse(is_answer_attempt(text_spec, "true"))

        numeric_spec = {"type": "numeric", "value": 16}
        self.assertTrue(is_answer_attempt(numeric_spec, "16"))
        self.assertTrue(is_answer_attempt(numeric_spec, "answer 16"))
        self.assertTrue(is_answer_attempt(numeric_spec, "sixteen"))
        self.assertTrue(is_answer_attempt(numeric_spec, "the answer is sixteen"))
        self.assertFalse(is_answer_attempt(numeric_spec, "numbers are hard lol"))
        self.assertFalse(is_answer_attempt(numeric_spec, "I have 16 cats"))
        self.assertFalse(is_answer_attempt(numeric_spec, "1989 was wild"))

        multiple_choice = {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"}
        self.assertTrue(is_answer_attempt(multiple_choice, "option c"))
        self.assertTrue(is_answer_attempt(multiple_choice, "green"))
        self.assertFalse(is_answer_attempt(multiple_choice, "which one was c again"))

        ordered = {"type": "ordered_tokens", "tokens": ["red", "blue", "green"]}
        self.assertTrue(is_answer_attempt(ordered, "green, blue, red"))
        self.assertFalse(is_answer_attempt(ordered, "it is true"))

    def test_render_answer_instruction_matches_answer_type(self):
        self.assertIn("Reply here or send", render_answer_instruction({"type": "numeric", "value": 12}))
        self.assertIn("option text", render_answer_instruction({"type": "multiple_choice", "choices": ["a"], "answer": "a"}))
        self.assertIn("number words", render_answer_instruction({"type": "numeric", "value": 12}))
        self.assertIn("true", render_answer_instruction({"type": "boolean", "value": True}))
        self.assertIn("full sequence", render_answer_instruction({"type": "ordered_tokens", "tokens": ["red", "blue"]}))
        self.assertIn("short clean answer", render_answer_instruction({"type": "text", "accepted": ["mars"]}))

    def test_percent_change_generator_hits_exact_sale_price_regressions(self):
        expected = {
            "A $24 item is discounted by 40%. What is the sale price?": "14.4",
            "A $48 item is discounted by 20%. What is the sale price?": "38.4",
            "A $72 item is discounted by 10%. What is the sale price?": "64.8",
            "A $30 item is discounted by 40%. What is the sale price?": "18",
        }

        for prompt, answer in expected.items():
            with self.subTest(prompt=prompt):
                variant = self._find_variant_by_prompt("math:percent-change", _math_percent_change, prompt)
                self.assertEqual(render_answer_summary(variant.answer_spec), answer)
                self.assertTrue(judge_answer(variant.answer_spec, answer))

    def test_average_generator_keeps_decimal_half_values(self):
        target_prompt = "Find the average: 5, 8, 11, 14"
        variant = self._find_variant_by_prompt("math:average-or-median", _math_average_or_median, target_prompt)

        self.assertEqual(render_answer_summary(variant.answer_spec), "9.5")
        self.assertTrue(judge_answer(variant.answer_spec, "9.5"))
        self.assertFalse(judge_answer(variant.answer_spec, "9"))

    def test_average_generator_still_handles_integer_averages(self):
        seed = self._seed_for("math:average-or-median")
        found_integer_average = False
        for variant_index in range(120):
            variant = _math_average_or_median(seed, seed_material="regression", variant_index=variant_index)
            if variant.prompt.startswith("Find the average: ") and "." not in render_answer_summary(variant.answer_spec):
                found_integer_average = True
                self.assertTrue(judge_answer(variant.answer_spec, render_answer_summary(variant.answer_spec)))
                break
        self.assertTrue(found_integer_average)

    def test_divisibility_generator_has_one_distinct_correct_choice(self):
        seed = self._seed_for("math:divisibility")

        for variant_index in range(48):
            variant = _math_divisibility(seed, seed_material="regression", variant_index=variant_index)
            with self.subTest(variant_index=variant_index):
                choices = [int(choice) for choice in variant.answer_spec["choices"]]
                divisor = int(variant.prompt.split(" by ")[1].split("?")[0])
                divisible = [choice for choice in choices if choice % divisor == 0]
                self.assertEqual(len(set(choices)), len(choices))
                self.assertEqual(len(divisible), 1)
                self.assertEqual(str(divisible[0]), str(variant.answer_spec["answer"]))

    def test_language_anagram_variants_are_clue_backed_and_unambiguous(self):
        seed = self._seed_for("language:anagram")

        for variant_index in range(24):
            variant = _language_anagram(seed, seed_material="regression", variant_index=variant_index)
            with self.subTest(variant_index=variant_index):
                self.assertIn("Unscramble the clue-backed word.", variant.prompt)
                self.assertIn("Clue:", variant.prompt)
                self.assertEqual(len(variant.answer_spec["accepted"]), 1)
                answer = variant.answer_spec["accepted"][0]
                self.assertTrue(judge_answer(variant.answer_spec, answer))

    def test_curated_aliases_stay_specific(self):
        food_chain_variant = next(
            variant
            for variant in iter_candidate_variants(categories={"science"}, seed_material="curated", variants_per_seed=2)
            if variant.concept_id == "science:food-chain" and "producer" in variant.prompt
        )
        self.assertTrue(judge_answer(food_chain_variant.answer_spec, "primary consumer"))
        self.assertFalse(judge_answer(food_chain_variant.answer_spec, "consumer"))

        analogy_variant = next(
            variant
            for variant in iter_candidate_variants(categories={"language"}, seed_material="curated", variants_per_seed=2)
            if variant.concept_id == "language:book-song-analogy" and "Book is to read" in variant.prompt
        )
        self.assertTrue(judge_answer(analogy_variant.answer_spec, "listen"))
        self.assertFalse(judge_answer(analogy_variant.answer_spec, "hear"))


class QuestionDropsPostgresDecodeTests(unittest.TestCase):
    def test_config_row_decodes_json_string_fields(self):
        row = {
            "guild_id": 10,
            "enabled": True,
            "drops_per_day": 3,
            "difficulty_profile": "hard",
            "timezone": "UTC",
            "answer_window_seconds": 75,
            "tone_mode": "clean",
            "activity_gate": "light",
            "active_start_hour": 9,
            "active_end_hour": 22,
            "enabled_channel_ids": json.dumps([20, 30, 30]),
            "enabled_categories": json.dumps(["science", "math", "invalid"]),
            "category_mastery": json.dumps(
                {
                    "science": {
                        "enabled": True,
                        "announcement_channel_id": 88,
                        "announcement_template": "{user.mention} reached {category.name}.",
                        "silent_grant": True,
                        "tiers": [{"tier": 1, "role_id": 301, "threshold": 10, "announcement_template": "{user.mention} reached Tier I."}],
                    }
                }
            ),
            "scholar_ladder": json.dumps(
                {
                    "enabled": True,
                    "announcement_channel_id": 99,
                    "announcement_template": "{user.mention} reached {tier.label}.",
                    "silent_grant": False,
                    "tiers": [{"tier": 1, "role_id": 401, "threshold": 25, "announcement_template": "{user.mention} reached Scholar I."}],
                }
            ),
            "digest_settings": json.dumps(
                {
                    "weekly_enabled": True,
                    "weekly_channel_id": 77,
                    "monthly_channel_id": 78,
                    "timezone": "Asia/Yerevan",
                    "skip_low_activity": False,
                    "mention_mode": "here",
                }
            ),
            "ai_celebrations_enabled": True,
        }

        config = _config_from_row(row)

        self.assertEqual(config["enabled_channel_ids"], [20, 30])
        self.assertEqual(config["enabled_categories"], ["math", "science"])
        self.assertEqual(config["difficulty_profile"], "hard")
        self.assertTrue(config["category_mastery"]["science"]["enabled"])
        self.assertEqual(config["category_mastery"]["science"]["announcement_channel_id"], 88)
        self.assertEqual(config["category_mastery"]["science"]["announcement_template"], "{user.mention} reached {category.name}.")
        self.assertEqual(config["category_mastery"]["science"]["tiers"][0]["role_id"], 301)
        self.assertEqual(config["category_mastery"]["science"]["tiers"][0]["announcement_template"], "{user.mention} reached Tier I.")
        self.assertEqual(config["scholar_ladder"]["tiers"][0]["threshold"], 25)
        self.assertEqual(config["scholar_ladder"]["tiers"][0]["announcement_template"], "{user.mention} reached Scholar I.")
        self.assertEqual(config["scholar_ladder"]["announcement_template"], "{user.mention} reached {tier.label}.")
        self.assertTrue(config["digest_settings"]["weekly_enabled"])
        self.assertEqual(config["digest_settings"]["weekly_channel_id"], 77)
        self.assertEqual(config["digest_settings"]["mention_mode"], "here")

    def test_config_row_malformed_json_falls_back_without_raising(self):
        row = {
            "guild_id": 10,
            "enabled": True,
            "drops_per_day": 2,
            "difficulty_profile": "wild",
            "timezone": "UTC",
            "answer_window_seconds": 60,
            "tone_mode": "clean",
            "activity_gate": "light",
            "active_start_hour": 10,
            "active_end_hour": 22,
            "enabled_channel_ids": "{\"broken\": true}",
            "enabled_categories": '{"broken"',
            "category_mastery": '["wrong-shape"]',
            "scholar_ladder": '{"enabled": true',
            "digest_settings": '{"weekly_enabled": true',
            "ai_celebrations_enabled": False,
        }

        config = _config_from_row(row)

        self.assertEqual(config["enabled_channel_ids"], [])
        self.assertEqual(config["enabled_categories"], [])
        self.assertEqual(config["difficulty_profile"], "standard")
        self.assertFalse(config["category_mastery"]["science"]["enabled"])
        self.assertFalse(config["scholar_ladder"]["enabled"])
        self.assertFalse(config["digest_settings"]["weekly_enabled"])

    def test_config_row_missing_or_invalid_tier_templates_normalize_to_none(self):
        row = {
            "guild_id": 10,
            "enabled": True,
            "drops_per_day": 2,
            "timezone": "UTC",
            "answer_window_seconds": 60,
            "tone_mode": "clean",
            "activity_gate": "light",
            "active_start_hour": 10,
            "active_end_hour": 22,
            "enabled_channel_ids": json.dumps([20]),
            "enabled_categories": json.dumps(["science"]),
            "category_mastery": json.dumps(
                {
                    "science": {
                        "enabled": True,
                        "tiers": [
                            {"tier": 1, "role_id": 301, "threshold": 10},
                            {"tier": 2, "role_id": 302, "threshold": 20, "announcement_template": 99},
                        ],
                    }
                }
            ),
            "scholar_ladder": json.dumps(
                {
                    "enabled": True,
                    "tiers": [{"tier": 1, "role_id": 401, "threshold": 25, "announcement_template": ["bad"]}],
                }
            ),
            "digest_settings": json.dumps({}),
            "ai_celebrations_enabled": False,
        }

        config = _config_from_row(row)

        self.assertIsNone(config["category_mastery"]["science"]["tiers"][0]["announcement_template"])
        self.assertIsNone(config["category_mastery"]["science"]["tiers"][1]["announcement_template"])
        self.assertIsNone(config["scholar_ladder"]["tiers"][0]["announcement_template"])

    def test_active_drop_row_decodes_json_string_fields(self):
        now = ge.now_utc()
        row = {
            "guild_id": 10,
            "channel_id": 20,
            "message_id": 30,
            "author_user_id": 40,
            "exposure_id": 50,
            "concept_id": "science:planet",
            "variant_hash": "abc123",
            "category": "science",
            "difficulty": 2,
            "prompt": "Which planet is known as the Red Planet?",
            "answer_spec": json.dumps({"type": "text", "accepted": ["Mars"]}),
            "asked_at": now,
            "expires_at": now + timedelta(minutes=1),
            "slot_key": "2026-04-02:0",
            "tone_mode": "clean",
            "participant_user_ids": json.dumps([5, 7, 7]),
        }

        record = _active_drop_from_row(row)

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record["answer_spec"]["accepted"], ["Mars"])
        self.assertEqual(record["participant_user_ids"], [5, 7])


class QuestionDropsStartupFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_start_fails_soft_when_storage_hydration_breaks(self):
        store = types.SimpleNamespace(
            load=AsyncMock(),
            fetch_all_configs=AsyncMock(side_effect=ValueError("bad legacy json")),
            fetch_meta=AsyncMock(),
            list_pending_posts=AsyncMock(),
            list_active_drops=AsyncMock(),
            close=AsyncMock(),
        )
        service = QuestionDropsService(types.SimpleNamespace(), store=store)
        try:
            started = await service.start()

            self.assertFalse(started)
            self.assertFalse(service.storage_ready)
            self.assertIn("could not be loaded", str(service.storage_error))
            self.assertIsNone(service._scheduler_task)
            self.assertIn("could not load", service.storage_message().lower())
            self.assertNotIn("could not reach", service.storage_message().lower())
        finally:
            await service.close()


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

    async def _attach_real_profile_service(self):
        profile_bot = types.SimpleNamespace(get_user=lambda user_id: None)
        profile_service = ProfileService(profile_bot, store=ProfileStore(backend="memory"))
        started = await profile_service.start()
        self.assertTrue(started)
        self.bot.profile_service = profile_service
        return profile_service

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

    async def _simulate_selector_mix(self, *, profile: str, drops_per_day: int, days: int = 30) -> dict[str, object]:
        self.assertIn(profile, QUESTION_DROP_DIFFICULTY_PROFILES)
        ok, message = await self.service.update_config(
            self.guild.id,
            drops_per_day=drops_per_day,
            difficulty_profile=profile,
        )
        self.assertTrue(ok, message)

        exposures: list[dict[str, object]] = []
        counts: Counter[int] = Counter()
        family_repeats = 0
        max_hard_run = 0
        current_hard_run = 0
        previous_family: str | None = None
        base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

        for day in range(days):
            current_time = base + timedelta(days=day)
            slot_day = current_time.date().isoformat()
            with patch("babblebox.question_drops_service.ge.now_utc", return_value=current_time):
                for slot in range(drops_per_day):
                    slot_key = f"{slot_day}:{slot}"
                    variant = self.service._select_variant(
                        self.guild.id,
                        self.channel.id,
                        exposures=exposures,
                        slot_key=slot_key,
                        config=self.service.get_config(self.guild.id),
                    )
                    self.assertIsNotNone(variant)
                    counts[int(variant.difficulty)] += 1
                    if int(variant.difficulty) == 3:
                        current_hard_run += 1
                        max_hard_run = max(max_hard_run, current_hard_run)
                    else:
                        current_hard_run = 0
                    if previous_family == variant.family_id:
                        family_repeats += 1
                    previous_family = variant.family_id
                    exposures.insert(
                        0,
                        {
                            "guild_id": self.guild.id,
                            "channel_id": self.channel.id,
                            "concept_id": variant.concept_id,
                            "variant_hash": variant.variant_hash,
                            "category": variant.category,
                            "difficulty": variant.difficulty,
                            "asked_at": (current_time - timedelta(minutes=(drops_per_day - slot))).isoformat(),
                            "resolved_at": None,
                            "winner_user_id": None,
                            "slot_key": slot_key,
                        },
                    )

        total = sum(counts.values()) or 1
        shares = {difficulty: counts[difficulty] / total for difficulty in (1, 2, 3)}
        return {
            "counts": counts,
            "shares": shares,
            "family_repeats": family_repeats,
            "max_hard_run": max_hard_run,
        }

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

    async def test_selector_high_drop_pressure_prefers_generated_depth_over_curated_reuse(self):
        curated_variant = QuestionDropVariant(
            concept_id="custom:science-curated",
            category="science",
            difficulty=1,
            source_type="curated",
            generator_type="static",
            prompt="Curated science prompt",
            answer_spec={"type": "text", "accepted": ["alpha"]},
            variant_hash="curated-science",
        )
        generated_variant = QuestionDropVariant(
            concept_id="custom:math-generated",
            category="math",
            difficulty=1,
            source_type="generated",
            generator_type="math_custom",
            prompt="Generated math prompt",
            answer_spec={"type": "numeric", "value": 7},
            variant_hash="generated-math",
        )
        exposures = [
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "concept_id": f"old-math-{index}",
                "variant_hash": f"old-math-{index}",
                "category": "math",
                "difficulty": 1,
                "asked_at": ge.now_utc().isoformat(),
                "resolved_at": None,
                "winner_user_id": None,
                "slot_key": f"2026-03-2{index}:0",
            }
            for index in range(3)
        ]

        low_config = {**self.service.get_config(self.guild.id), "drops_per_day": 1}
        high_config = {**self.service.get_config(self.guild.id), "drops_per_day": 10}
        with patch(
            "babblebox.question_drops_service.iter_candidate_variants",
            return_value=[curated_variant, generated_variant],
        ), patch(
            "babblebox.question_drops_service.question_drop_seed_for_concept",
            side_effect=lambda concept_id: {"source_type": "generated"} if str(concept_id).startswith("old-math-") else None,
        ):
            low_pick = self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=exposures,
                slot_key="2026-03-30:0",
                config=low_config,
            )
            high_pick = self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=exposures,
                slot_key="2026-03-30:0",
                config=high_config,
            )

        self.assertEqual(low_pick, curated_variant)
        self.assertEqual(high_pick, generated_variant)

    async def test_selector_high_drop_pressure_keeps_category_spread_healthier(self):
        high_config = {**self.service.get_config(self.guild.id), "drops_per_day": 10}
        exposures: list[dict[str, object]] = []
        category_counts: Counter[str] = Counter()
        base_now = ge.now_utc()

        for day_offset in range(2):
            slot_day = (base_now - timedelta(days=2 - day_offset)).date()
            for slot_index in range(10):
                slot_key = f"{slot_day.isoformat()}:{slot_index}"
                variant = self.service._select_variant(
                    self.guild.id,
                    self.channel.id,
                    exposures=exposures,
                    slot_key=slot_key,
                    config=high_config,
                )
                self.assertIsNotNone(variant)
                category_counts[variant.category] += 1
                exposures.insert(
                    0,
                    {
                        "guild_id": self.guild.id,
                        "channel_id": self.channel.id,
                        "concept_id": variant.concept_id,
                        "variant_hash": variant.variant_hash,
                        "category": variant.category,
                        "difficulty": variant.difficulty,
                        "asked_at": (base_now - timedelta(days=2 - day_offset, minutes=slot_index)).isoformat(),
                        "resolved_at": None,
                        "winner_user_id": None,
                        "slot_key": slot_key,
                    },
                )

        self.assertGreaterEqual(len(category_counts), 4)
        self.assertLessEqual(max(category_counts.values()), 8)

    async def test_update_config_accepts_maximum_drop_range(self):
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=10)

        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_config(self.guild.id)["drops_per_day"], 10)

    async def test_update_config_accepts_difficulty_profile_and_status_embed_surfaces_it(self):
        ok, message = await self.service.update_config(self.guild.id, difficulty_profile="SMART")

        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_config(self.guild.id)["difficulty_profile"], "smart")

        embed = self.service.build_status_embed(self.guild, await self.service.get_status_snapshot(self.guild))
        rules_field = next(field.value for field in embed.fields if field.name == "Rules")
        delivery_field = next(field.value for field in embed.fields if field.name == "Delivery")

        self.assertIn("Profile", rules_field)
        self.assertIn("**Smart**", rules_field)
        self.assertIn("More medium and hard, less farmable", delivery_field)

    async def test_selector_profiles_shift_mix_and_avoid_family_spam_over_time(self):
        results: dict[tuple[str, int], dict[str, object]] = {}
        for profile in QUESTION_DROP_DIFFICULTY_PROFILES:
            for drops_per_day in (2, 5, 9):
                with self.subTest(profile=profile, drops_per_day=drops_per_day):
                    result = await self._simulate_selector_mix(profile=profile, drops_per_day=drops_per_day)
                    self.assertEqual(result["family_repeats"], 0)
                    self.assertLessEqual(result["max_hard_run"], 2)
                    self.assertGreater(result["shares"][2], 0.34)
                    results[(profile, drops_per_day)] = result

        for drops_per_day in (2, 5, 9):
            standard = results[("standard", drops_per_day)]["shares"]
            smart = results[("smart", drops_per_day)]["shares"]
            hard = results[("hard", drops_per_day)]["shares"]
            self.assertGreater(standard[1], smart[1])
            self.assertGreater(smart[1], hard[1])
            self.assertLess(standard[3], smart[3])
            self.assertLess(smart[3], hard[3])

        for profile in QUESTION_DROP_DIFFICULTY_PROFILES:
            low = results[(profile, 2)]["shares"]
            medium = results[(profile, 5)]["shares"]
            high = results[(profile, 9)]["shares"]
            self.assertGreater(low[1], medium[1])
            self.assertGreater(medium[1], high[1])
            self.assertLess(low[3], medium[3])
            self.assertLess(medium[3], high[3])

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
        self.assertIn("Solved", self.channel.sent[-1][1]["embed"].title)

    async def test_decimal_numeric_drop_accepts_equivalent_decimal_and_surfaces_clean_summary(self):
        variant = QuestionDropVariant(
            concept_id="math:decimal-sale",
            category="math",
            difficulty=3,
            source_type="generated",
            generator_type="math_percent_change",
            prompt="A $24 item is discounted by 40%. What is the sale price?",
            answer_spec={"type": "numeric", "value": 14.4},
            variant_hash="decimal-sale",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()

        handled = await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(46), content="14.40")
        )

        self.assertTrue(handled)
        self.assertEqual(active["answer_spec"]["value"], 14.4)
        self.assertIn("**14.4**", self.channel.sent[-1][1]["embed"].description)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_first_try_correct_solve_skips_participant_persist_write(self):
        active = await self._post_one_drop()
        self.service.store.update_active_drop_participants = AsyncMock()
        answer = str(active["answer_spec"].get("value", active["answer_spec"].get("accepted", [""])[0]))

        handled = await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(45), content=answer)
        )

        self.assertTrue(handled)
        self.service.store.update_active_drop_participants.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_first_wrong_attempt_persists_participant_state(self):
        active = await self._post_one_drop()
        self.service.store.update_active_drop_participants = AsyncMock()
        author = DummyUser(54)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))

        handled = await self.service.handle_message(wrong)

        self.assertFalse(handled)
        self.service.store.update_active_drop_participants.assert_awaited_once_with(self.guild.id, self.channel.id, [54])

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
        self.assertEqual(self.channel.sent[-1][1]["embed"].title, "Not Yet")

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


class QuestionDropsDigestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.members = [
            DummyUser(11, display_name="Ada"),
            DummyUser(12, display_name="Lin"),
            DummyUser(13, display_name="Sol"),
            DummyUser(14, display_name="Mira"),
        ]
        self.channel = DummyChannel(20)
        self.secondary_channel = DummyChannel(21)
        self.guild = DummyGuild(10, channels=[self.channel, self.secondary_channel], members=self.members)
        self.bot = DummyBot(self.guild, [self.channel, self.secondary_channel])
        self.store = QuestionDropsStore(backend="memory")
        await self.store.load()
        self.service = QuestionDropsService(self.bot, store=self.store)
        self.assertTrue(await self.service.start())
        if self.service._scheduler_task is not None:
            self.service._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.service._scheduler_task
            self.service._scheduler_task = None
        self.profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        self.assertTrue(await self.profile_service.start())
        self.bot.profile_service = self.profile_service
        self._slot_index = 0

    async def asyncTearDown(self):
        if isinstance(getattr(self, "service", None), QuestionDropsService):
            await self.service.close()
        if isinstance(getattr(self, "profile_service", None), ProfileService):
            await self.profile_service.close()

    async def _configure_digest(
        self,
        *,
        weekly: bool = False,
        monthly: bool = False,
        shared_channel: DummyChannel | None = None,
        weekly_channel: DummyChannel | None = None,
        monthly_channel: DummyChannel | None = None,
        timezone_name: str = "UTC",
        skip_low_activity: bool = True,
        mention_mode: str = "none",
    ):
        ok, message = await self.service.update_digest_config(
            self.guild,
            weekly_enabled=weekly,
            monthly_enabled=monthly,
            shared_channel_id=shared_channel.id if shared_channel is not None else None,
            weekly_channel_id=weekly_channel.id if weekly_channel is not None else None,
            monthly_channel_id=monthly_channel.id if monthly_channel is not None else None,
            timezone_name=timezone_name,
            skip_low_activity=skip_low_activity,
            mention_mode=mention_mode,
        )
        self.assertTrue(ok, message)

    async def _record_digest_drop(
        self,
        asked_at: datetime,
        *,
        category: str,
        difficulty: int,
        participant_ids: list[int],
        winner_user_id: int | None,
    ):
        self._slot_index += 1
        exposure = await self.store.insert_exposure(
            {
                "guild_id": self.guild.id,
                "channel_id": self.channel.id,
                "concept_id": f"{category}:digest-{self._slot_index}",
                "variant_hash": f"digest-{self._slot_index}",
                "category": category,
                "difficulty": difficulty,
                "asked_at": asked_at,
                "resolved_at": asked_at + timedelta(minutes=1),
                "winner_user_id": winner_user_id,
                "slot_key": f"{asked_at.date().isoformat()}:{self._slot_index}",
            }
        )
        await self.service._record_participation_batch(
            guild_id=self.guild.id,
            exposure_id=int(exposure["id"]),
            occurred_at=asked_at,
            category=category,
            difficulty=difficulty,
            participant_ids=participant_ids,
            winner_user_id=winner_user_id,
        )
        return exposure

    async def _save_unlock(
        self,
        *,
        user_id: int,
        scope_type: str,
        scope_key: str,
        tier: int,
        role_id: int,
        granted_at: datetime,
    ):
        await self.profile_service.store.save_question_drop_unlock(
            {
                "guild_id": self.guild.id,
                "user_id": user_id,
                "scope_type": scope_type,
                "scope_key": scope_key,
                "tier": tier,
                "role_id": role_id,
                "granted_at": granted_at,
            }
        )

    def _fields(self, embed: discord.Embed) -> dict[str, str]:
        return {field.name: field.value for field in embed.fields}

    async def test_digest_config_rejects_enable_without_channel(self):
        ok, message = await self.service.update_digest_config(self.guild, weekly_enabled=True)

        self.assertFalse(ok)
        self.assertIn("digest channel", message.lower())

    async def test_digest_period_boundaries_follow_weekly_and_monthly_rules(self):
        weekly_now = datetime(2026, 4, 6, 8, 30, tzinfo=timezone.utc)
        weekly_period = self.service._digest_period_for(kind="weekly", timezone_name="UTC", now=weekly_now)
        self.assertEqual(weekly_period.period_start_at, datetime(2026, 3, 30, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(weekly_period.period_end_at, datetime(2026, 4, 6, 0, 0, tzinfo=timezone.utc))
        self.assertEqual(weekly_period.scheduled_post_at, datetime(2026, 4, 6, 9, 0, tzinfo=timezone.utc))

        monthly_now = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
        monthly_period = self.service._digest_period_for(kind="monthly", timezone_name="UTC+04:00", now=monthly_now)
        self.assertEqual(monthly_period.period_start_at.astimezone(timezone.utc), datetime(2026, 9, 30, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(monthly_period.period_end_at.astimezone(timezone.utc), datetime(2026, 10, 31, 20, 0, tzinfo=timezone.utc))
        self.assertEqual(monthly_period.scheduled_post_at.astimezone(timezone.utc), datetime(2026, 11, 1, 5, 0, tzinfo=timezone.utc))

    async def test_record_participation_batch_persists_period_events(self):
        asked_at = datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc)
        await self._record_digest_drop(
            asked_at,
            category="science",
            difficulty=2,
            participant_ids=[11, 12],
            winner_user_id=11,
        )

        rows = await self.store.list_participation_events_for_guild(
            self.guild.id,
            start=asked_at - timedelta(minutes=1),
            end=asked_at + timedelta(minutes=1),
        )

        self.assertEqual(
            [(row["user_id"], row["correct"], row["points_awarded"]) for row in rows],
            [(11, True, answer_points_for_difficulty(2)), (12, False, 0)],
        )

    async def test_weekly_digest_posts_once_for_active_guild(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel, skip_low_activity=True)
        for offset in range(4):
            await self._record_digest_drop(
                datetime(2026, 3, 31, 12 + offset, 0, tzinfo=timezone.utc),
                category="science" if offset % 2 == 0 else "history",
                difficulty=2,
                participant_ids=[11, 12],
                winner_user_id=11 if offset % 2 == 0 else 12,
            )

        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()
            await self.service._maybe_post_due_digests()

        self.assertEqual(len(self.channel.sent), 1)
        embed = self.channel.sent[0][1]["embed"]
        self.assertIn("Weekly", embed.title)
        latest = await self.store.fetch_latest_digest_run(self.guild.id, digest_kind="weekly")
        self.assertEqual(latest["status"], "posted")

    async def test_weekly_digest_skips_low_activity(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel, skip_low_activity=True)
        await self._record_digest_drop(
            datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            category="science",
            difficulty=1,
            participant_ids=[11],
            winner_user_id=11,
        )

        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        self.assertEqual(self.channel.sent, [])
        latest = await self.store.fetch_latest_digest_run(self.guild.id, digest_kind="weekly")
        self.assertEqual(latest["status"], "skipped")
        self.assertIn("low activity", latest["detail"].lower())

    async def test_digest_invalid_channel_marks_terminal_failure(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel)
        await self._record_digest_drop(
            datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            category="science",
            difficulty=2,
            participant_ids=[11, 12],
            winner_user_id=11,
        )
        self.guild._channels.pop(self.channel.id, None)

        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        latest = await self.store.fetch_latest_digest_run(self.guild.id, digest_kind="weekly")
        self.assertEqual(latest["status"], "failed")
        self.assertIn("missing", latest["detail"].lower())

    async def test_digest_rankings_ignore_out_of_period_activity_and_break_ties_by_user_id(self):
        period = self.service._digest_period_for(
            kind="weekly",
            timezone_name="UTC",
            now=datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc),
        )
        await self._record_digest_drop(
            datetime(2026, 3, 20, 12, 0, tzinfo=timezone.utc),
            category="science",
            difficulty=3,
            participant_ids=[13],
            winner_user_id=13,
        )
        await self._record_digest_drop(
            datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc),
            category="science",
            difficulty=2,
            participant_ids=[11],
            winner_user_id=11,
        )
        await self._record_digest_drop(
            datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            category="history",
            difficulty=2,
            participant_ids=[12],
            winner_user_id=12,
        )

        metrics = await self.service._collect_digest_period_data(self.guild.id, period)

        self.assertEqual([row["user_id"] for row in metrics["ranked_users"][:2]], [11, 12])
        self.assertNotIn(13, [row["user_id"] for row in metrics["ranked_users"]])

    async def test_digest_unlock_highlights_use_only_period_unlocks(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel, skip_low_activity=False)
        for offset in range(4):
            await self._record_digest_drop(
                datetime(2026, 3, 31, 13 + offset, 0, tzinfo=timezone.utc),
                category="science",
                difficulty=2,
                participant_ids=[11, 12],
                winner_user_id=11,
            )
        await self._save_unlock(
            user_id=11,
            scope_type="scholar",
            scope_key="global",
            tier=2,
            role_id=5001,
            granted_at=datetime(2026, 4, 2, 15, 0, tzinfo=timezone.utc),
        )
        await self._save_unlock(
            user_id=12,
            scope_type="category",
            scope_key="science",
            tier=1,
            role_id=5002,
            granted_at=datetime(2026, 4, 3, 15, 0, tzinfo=timezone.utc),
        )
        await self._save_unlock(
            user_id=13,
            scope_type="scholar",
            scope_key="global",
            tier=3,
            role_id=5003,
            granted_at=datetime(2026, 3, 20, 15, 0, tzinfo=timezone.utc),
        )

        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        embed = self.channel.sent[-1][1]["embed"]
        fields = self._fields(embed)
        self.assertIn("Scholar II", fields["Unlock Highlights"])
        self.assertIn("Science Tier I", fields["Unlock Highlights"])
        self.assertNotIn("Tier III", fields["Unlock Highlights"])

    async def test_restart_does_not_double_post_same_period(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel)
        for offset in range(4):
            await self._record_digest_drop(
                datetime(2026, 3, 31, 12 + offset, 0, tzinfo=timezone.utc),
                category="logic",
                difficulty=2,
                participant_ids=[11, 12],
                winner_user_id=11 if offset % 2 == 0 else 12,
            )
        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        self.store.load = AsyncMock()
        restarted = QuestionDropsService(self.bot, store=self.store)
        try:
            self.assertTrue(await restarted.start())
            if restarted._scheduler_task is not None:
                restarted._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await restarted._scheduler_task
                restarted._scheduler_task = None
            with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
                await restarted._maybe_post_due_digests()
            self.assertEqual(len(self.channel.sent), 1)
        finally:
            await restarted.close()

    async def test_monthly_and_weekly_can_post_independently_to_shared_channel(self):
        await self._configure_digest(weekly=True, monthly=True, shared_channel=self.channel, skip_low_activity=False)
        now = next(
            datetime(2026, month, 1, 10, 0, tzinfo=timezone.utc)
            for month in range(1, 13)
            if datetime(2026, month, 1, 10, 0, tzinfo=timezone.utc).weekday() == 0
        )
        weekly_period = self.service._digest_period_for(kind="weekly", timezone_name="UTC", now=now)
        monthly_period = self.service._digest_period_for(kind="monthly", timezone_name="UTC", now=now)
        for index in range(4):
            await self._record_digest_drop(
                weekly_period.period_start_at + timedelta(days=index + 1, hours=12),
                category="science",
                difficulty=2,
                participant_ids=[11, 12],
                winner_user_id=11 if index % 2 == 0 else 12,
            )
        for index in range(10):
            await self._record_digest_drop(
                monthly_period.period_start_at + timedelta(days=index + 2, hours=10),
                category="history" if index % 2 == 0 else "logic",
                difficulty=1,
                participant_ids=[11, 12, 13],
                winner_user_id=11 if index % 3 == 0 else 12,
            )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        self.assertEqual(len(self.channel.sent), 2)
        titles = [entry[1]["embed"].title for entry in self.channel.sent]
        self.assertTrue(any("Weekly" in title for title in titles))
        self.assertTrue(any("Monthly" in title for title in titles))

    async def test_digest_status_embed_shows_last_run_outcomes(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel, skip_low_activity=True)
        await self._record_digest_drop(
            datetime(2026, 4, 2, 12, 0, tzinfo=timezone.utc),
            category="science",
            difficulty=1,
            participant_ids=[11],
            winner_user_id=11,
        )
        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()
            snapshot = await self.service.get_status_snapshot(self.guild)

        embed = self.service.build_digest_status_embed(self.guild, snapshot)
        fields = self._fields(embed)
        self.assertIn("Skipped", fields["Last Runs"])
        self.assertIn("Weekly", fields["Cadence"])

    async def test_overlapping_digest_checks_do_not_double_post(self):
        await self._configure_digest(weekly=True, shared_channel=self.channel)
        for offset in range(4):
            await self._record_digest_drop(
                datetime(2026, 3, 31, 12 + offset, 0, tzinfo=timezone.utc),
                category="culture",
                difficulty=2,
                participant_ids=[11, 12],
                winner_user_id=11,
            )
        now = datetime(2026, 4, 6, 10, 0, tzinfo=timezone.utc)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await asyncio.gather(self.service._maybe_post_due_digests(), self.service._maybe_post_due_digests())

        self.assertEqual(len(self.channel.sent), 1)

    async def test_monthly_digest_uses_here_ping_only_as_message_prefix(self):
        await self._configure_digest(monthly=True, shared_channel=self.channel, skip_low_activity=False, mention_mode="here")
        now = datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc)
        monthly_period = self.service._digest_period_for(kind="monthly", timezone_name="UTC", now=now)
        for index in range(10):
            await self._record_digest_drop(
                monthly_period.period_start_at + timedelta(days=index + 1, hours=9),
                category="math",
                difficulty=1,
                participant_ids=[11, 12, 13],
                winner_user_id=11 if index % 2 == 0 else 12,
            )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now):
            await self.service._maybe_post_due_digests()

        self.assertEqual(self.channel.sent[-1][1]["content"], "@here")
        self.assertTrue(self.channel.sent[-1][1]["allowed_mentions"].everyone)
        self.assertNotIn("<@", self.channel.sent[-1][1]["embed"].description)


class QuestionDropsServiceContinuationTests(QuestionDropsServiceTests):
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


class QuestionDropsMemberRolePreferenceTests(QuestionDropsServiceTests):
    def _attach_member(self, member: DummyUser, *roles: DummyRole):
        for role in roles:
            self.guild._roles[role.id] = role
        self.guild._members[member.id] = member
        self.guild.members = [guild_member for guild_member in self.guild._members.values() if guild_member is not self.guild.me]

    async def test_member_role_status_payload_lists_current_roles_preference_and_stale_managed_roles(self):
        profile_service = await self._attach_real_profile_service()
        science_role = DummyRole(9101, position=14, name="Science I")
        unrelated_role = DummyRole(9102, position=4, name="Unrelated")
        member = DummyUser(201, roles=[science_role, unrelated_role])
        self._attach_member(member, science_role, unrelated_role)
        ok, message = await self.service.update_category_mastery(
            self.guild.id,
            category="science",
            enabled=True,
            tier=1,
            role_id=science_role.id,
            threshold=10,
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.update_category_mastery(
            self.guild.id,
            category="logic",
            enabled=True,
            tier=1,
            role_id=science_role.id,
            threshold=25,
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.update_category_mastery(
            self.guild.id,
            category="history",
            tier=1,
            role_id=999999,
            threshold=20,
        )
        self.assertTrue(ok, message)
        await profile_service.record_question_drop_result(member.id, guild_id=self.guild.id, category="science", correct=True, points=10)

        payload = await self.service.get_member_roles_status(self.guild, member)
        embed = self.service.build_member_roles_status_embed(self.guild, member, payload)
        field_map = {field.name: field.value for field in embed.fields}

        self.assertTrue(payload["preference"]["role_grants_enabled"])
        self.assertEqual([record["role_id"] for record in payload["held_records"]], [science_role.id])
        self.assertEqual(payload["stale_managed_count"], 1)
        self.assertIn("Status: **On**", field_map["Future Grants"])
        self.assertIn("Science I", field_map["Current Roles"])
        self.assertNotIn("Unrelated", field_map["Current Roles"])

    async def test_remove_specific_managed_role_only_removes_that_role_and_keeps_history(self):
        profile_service = await self._attach_real_profile_service()
        science_role = DummyRole(9201, position=14, name="Science I")
        scholar_role = DummyRole(9202, position=13, name="Scholar I")
        unrelated_role = DummyRole(9203, position=3, name="Unrelated")
        member = DummyUser(202, roles=[science_role, scholar_role, unrelated_role])
        self._attach_member(member, science_role, scholar_role, unrelated_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=science_role.id, threshold=10)
        await self.service.update_scholar_ladder(self.guild.id, enabled=True, tier=1, role_id=scholar_role.id, threshold=20)
        await profile_service.store.save_question_drop_unlock(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "scope_type": "category",
                "scope_key": "science",
                "tier": 1,
                "role_id": science_role.id,
                "granted_at": ge.now_utc(),
            }
        )

        payload = await self.service.remove_member_managed_roles(self.guild, member, role_id=science_role.id)

        self.assertEqual([record["role_id"] for record in payload["removed"]], [science_role.id])
        self.assertCountEqual([role.id for role in member.roles], [scholar_role.id, unrelated_role.id])
        unlocks = await profile_service.store.fetch_question_drop_unlocks(guild_id=self.guild.id, user_id=member.id)
        self.assertEqual(len(unlocks), 1)
        self.assertEqual(unlocks[0]["role_id"], science_role.id)

    async def test_remove_all_managed_roles_leaves_unrelated_roles_alone(self):
        await self._attach_real_profile_service()
        science_role = DummyRole(9301, position=14, name="Science I")
        scholar_role = DummyRole(9302, position=13, name="Scholar I")
        unrelated_role = DummyRole(9303, position=3, name="Unrelated")
        member = DummyUser(203, roles=[science_role, scholar_role, unrelated_role])
        self._attach_member(member, science_role, scholar_role, unrelated_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=science_role.id, threshold=10)
        await self.service.update_scholar_ladder(self.guild.id, enabled=True, tier=1, role_id=scholar_role.id, threshold=20)

        payload = await self.service.remove_member_managed_roles(self.guild, member)

        self.assertEqual(sorted(record["role_id"] for record in payload["removed"]), [science_role.id, scholar_role.id])
        self.assertEqual([role.id for role in member.roles], [unrelated_role.id])

    async def test_remove_rejects_non_managed_role(self):
        await self._attach_real_profile_service()
        unrelated_role = DummyRole(9401, position=3, name="Unrelated")
        member = DummyUser(204, roles=[unrelated_role])
        self._attach_member(member, unrelated_role)

        payload = await self.service.remove_member_managed_roles(self.guild, member, role_id=unrelated_role.id)

        self.assertEqual(payload["status"], "not_managed")
        self.assertEqual([role.id for role in member.roles], [unrelated_role.id])

    async def test_opt_out_can_leave_current_roles_on_or_remove_them(self):
        await self._attach_real_profile_service()
        science_role = DummyRole(9501, position=14, name="Science I")
        member = DummyUser(205, roles=[science_role])
        self._attach_member(member, science_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=science_role.id, threshold=10)

        keep_payload = await self.service.update_member_role_preference(
            self.guild,
            member,
            mode="stop",
            remove_current_roles=False,
        )
        self.assertFalse(keep_payload["after"]["role_grants_enabled"])
        self.assertEqual([role.id for role in member.roles], [science_role.id])

        remove_payload = await self.service.update_member_role_preference(
            self.guild,
            member,
            mode="stop",
            remove_current_roles=True,
        )
        self.assertFalse(remove_payload["after"]["role_grants_enabled"])
        self.assertEqual(len(remove_payload["removal"]["removed"]), 1)
        self.assertEqual(member.roles, [])

    async def test_opted_out_progression_records_unlock_history_without_granting_role(self):
        profile_service = await self._attach_real_profile_service()
        science_role = DummyRole(9601, position=14, name="Science I")
        member = DummyUser(206)
        self._attach_member(member, science_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=science_role.id, threshold=10)
        await profile_service.set_question_drop_role_grants_enabled(member.id, guild_id=self.guild.id, enabled=False)

        update = await profile_service.record_question_drop_result(member.id, guild_id=self.guild.id, category="science", correct=True, points=10)
        events = await self.service._grant_progression_rewards(
            guild=self.guild,
            member=member,
            fallback_channel=self.channel,
            category="science",
            update=update,
        )

        self.assertEqual(events, [])
        self.assertEqual(member.roles, [])
        unlocks = await profile_service.store.fetch_question_drop_unlocks(guild_id=self.guild.id, user_id=member.id)
        self.assertEqual([(row["scope_type"], row["scope_key"], row["tier"], row["role_id"]) for row in unlocks], [("category", "science", 1, science_role.id)])

    async def test_opt_in_restore_reapplies_currently_eligible_roles_only_when_explicit(self):
        profile_service = await self._attach_real_profile_service()
        science_role = DummyRole(9701, position=14, name="Science I")
        member = DummyUser(207)
        self._attach_member(member, science_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=science_role.id, threshold=10)
        await profile_service.set_question_drop_role_grants_enabled(member.id, guild_id=self.guild.id, enabled=False)
        update = await profile_service.record_question_drop_result(member.id, guild_id=self.guild.id, category="science", correct=True, points=10)
        await self.service._grant_progression_rewards(
            guild=self.guild,
            member=member,
            fallback_channel=self.channel,
            category="science",
            update=update,
        )

        no_restore = await self.service.update_member_role_preference(
            self.guild,
            member,
            mode="receive",
            restore_current_roles=False,
        )
        self.assertTrue(no_restore["after"]["role_grants_enabled"])
        self.assertEqual(member.roles, [])

        restored = await self.service.update_member_role_preference(
            self.guild,
            member,
            mode="receive",
            restore_current_roles=True,
        )
        self.assertTrue(restored["after"]["role_grants_enabled"])
        self.assertEqual([role.id for role in member.roles], [science_role.id])
        self.assertEqual(len(restored["restore"]["restored"]), 1)

    async def test_opt_in_restore_skips_disabled_legacy_role_configs(self):
        profile_service = await self._attach_real_profile_service()
        disabled_role = DummyRole(9801, position=14, name="Legacy History I")
        member = DummyUser(208)
        self._attach_member(member, disabled_role)
        await self.service.update_category_mastery(self.guild.id, category="history", tier=1, role_id=disabled_role.id, threshold=10)
        await profile_service.store.save_question_drop_unlock(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "scope_type": "category",
                "scope_key": "history",
                "tier": 1,
                "role_id": disabled_role.id,
                "granted_at": ge.now_utc(),
            }
        )

        payload = await self.service.update_member_role_preference(
            self.guild,
            member,
            mode="receive",
            restore_current_roles=True,
        )

        self.assertTrue(payload["after"]["role_grants_enabled"])
        self.assertEqual(payload["restore"]["status"], "nothing_to_restore")
        self.assertEqual(member.roles, [])

    async def test_recalculate_mastery_roles_skips_opted_out_members(self):
        profile_service = await self._attach_real_profile_service()
        mastery_role = DummyRole(9901, position=14, name="Logic I")
        member = DummyUser(209)
        self._attach_member(member, mastery_role)
        await self.service.update_category_mastery(self.guild.id, category="logic", enabled=True, tier=1, role_id=mastery_role.id, threshold=10)
        await profile_service.record_question_drop_result(member.id, guild_id=self.guild.id, category="logic", correct=True, points=12)
        await profile_service.set_question_drop_role_grants_enabled(member.id, guild_id=self.guild.id, enabled=False)

        preview = await self.service.recalculate_mastery_roles(self.guild, member=member, preview=True)
        execute = await self.service.recalculate_mastery_roles(self.guild, member=member, preview=False)

        self.assertEqual(preview, {"preview": True, "scanned": 1, "pending": 0, "granted": 0, "skipped_opted_out": 1})
        self.assertEqual(execute, {"preview": False, "scanned": 1, "pending": 0, "granted": 0, "skipped_opted_out": 1})
        member.add_roles.assert_not_awaited()

    async def test_remove_handles_missing_manage_roles_and_partial_hierarchy_failures_gracefully(self):
        await self._attach_real_profile_service()
        removable_role = DummyRole(9911, position=10, name="Science I")
        blocked_role = DummyRole(9912, position=60, name="Scholar III")
        member = DummyUser(210, roles=[removable_role, blocked_role])
        self._attach_member(member, removable_role, blocked_role)
        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True, tier=1, role_id=removable_role.id, threshold=10)
        await self.service.update_scholar_ladder(self.guild.id, enabled=True, tier=3, role_id=blocked_role.id, threshold=30)

        self.guild.me.guild_permissions.manage_roles = False
        no_manage = await self.service.remove_member_managed_roles(self.guild, member)
        self.assertEqual(len(no_manage["removed"]), 0)
        self.assertEqual(len(no_manage["issues"]), 2)
        self.assertCountEqual([role.id for role in member.roles], [removable_role.id, blocked_role.id])

        self.guild.me.guild_permissions.manage_roles = True
        partial = await self.service.remove_member_managed_roles(self.guild, member)
        self.assertEqual([record["role_id"] for record in partial["removed"]], [removable_role.id])
        self.assertEqual([item["record"]["role_id"] for item in partial["issues"]], [blocked_role.id])
        self.assertEqual([role.id for role in member.roles], [blocked_role.id])

    async def test_category_template_save_status_and_clear_round_trip(self):
        ok, message = await self.service.save_category_mastery_announcement_template(
            self.guild.id,
            category="science",
            template="  {user.mention} reached {category.name} at {threshold}.  ",
        )

        self.assertTrue(ok, message)
        payload = await self.service.get_category_mastery_announcement_status(self.guild, category="science")
        self.assertTrue(payload["has_custom_template"])
        self.assertEqual(payload["announcement_template"], "{user.mention} reached {category.name} at {threshold}.")
        self.assertEqual(payload["effective_source"], "scope_default")
        self.assertEqual(payload["target_label"], "Science default")
        self.assertIn("{category.name}", payload["placeholder_tokens"])
        self.assertIn("Ava", payload["preview"])

        ok, message = await self.service.clear_category_mastery_announcement_template(self.guild.id, category="science")
        self.assertTrue(ok, message)
        cleared = await self.service.get_category_mastery_announcement_status(self.guild, category="science")
        self.assertFalse(cleared["has_custom_template"])
        self.assertIsNone(cleared["announcement_template"])

    async def test_scholar_template_save_and_scope_specific_placeholder_validation(self):
        ok, message = await self.service.save_scholar_announcement_template(
            self.guild.id,
            template="{user.mention} reached {tier.label} at {threshold}.",
        )
        self.assertTrue(ok, message)

        payload = await self.service.get_scholar_announcement_status(self.guild)
        self.assertTrue(payload["has_custom_template"])
        self.assertEqual(payload["announcement_template"], "{user.mention} reached {tier.label} at {threshold}.")

        ok, message = await self.service.save_scholar_announcement_template(
            self.guild.id,
            template="{user.mention} reached {category.name}.",
        )
        self.assertFalse(ok)
        self.assertIn("Unsupported placeholder", message)

    async def test_category_tier_override_precedence_and_clear_fallbacks_to_scope_default(self):
        ok, message = await self.service.save_category_mastery_announcement_template(
            self.guild.id,
            category="science",
            template="{user.mention} reached {category.name}.",
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.save_category_mastery_announcement_template(
            self.guild.id,
            category="science",
            tier=2,
            template="{user.mention} locked {tier.label} at {threshold}.",
        )
        self.assertTrue(ok, message)

        tier_payload = await self.service.get_category_mastery_announcement_status(self.guild, category="science", tier=2)
        self.assertEqual(tier_payload["announcement_template"], "{user.mention} locked {tier.label} at {threshold}.")
        self.assertEqual(tier_payload["effective_source"], "tier_override")
        self.assertEqual(tier_payload["target_label"], "Science Tier II override")

        scope_payload = await self.service.get_category_mastery_announcement_status(self.guild, category="science")
        self.assertEqual(scope_payload["effective_source"], "scope_default")
        self.assertIn("Tier II", scope_payload["other_tier_override_labels"])

        ok, message = await self.service.clear_category_mastery_announcement_template(self.guild.id, category="science", tier=2)
        self.assertTrue(ok, message)
        cleared = await self.service.get_category_mastery_announcement_status(self.guild, category="science", tier=2)
        self.assertIsNone(cleared["announcement_template"])
        self.assertEqual(cleared["effective_source"], "scope_default")
        self.assertIn("reached Science", cleared["preview"])

    async def test_tier_override_only_status_works_without_scope_default(self):
        ok, message = await self.service.save_scholar_announcement_template(
            self.guild.id,
            tier=2,
            template="{user.mention} reached {tier.label} at {threshold}.",
        )
        self.assertTrue(ok, message)

        tier_payload = await self.service.get_scholar_announcement_status(self.guild, tier=2)
        scope_payload = await self.service.get_scholar_announcement_status(self.guild)

        self.assertEqual(tier_payload["effective_source"], "tier_override")
        self.assertEqual(tier_payload["target_label"], "Scholar II override")
        self.assertFalse(scope_payload["has_custom_template"])
        self.assertEqual(scope_payload["effective_source"], "babblebox_default")

    async def test_template_validation_rejects_unsafe_or_invalid_copy(self):
        cases = (
            ("{user.password}", "Unsupported placeholder"),
            ("Visit https://bad.example", "links or invites"),
            ("[click](https://bad.example)", "links or invites"),
            ("@everyone won {threshold}", "raw mentions"),
            ("horny {user.mention}", "blocked or inappropriate"),
            ("   ", "cannot be empty"),
            ("x" * 221, "220 characters or fewer"),
        )

        for template, expected in cases:
            ok, message = await self.service.save_category_mastery_announcement_template(
                self.guild.id,
                category="science",
                template=template,
            )
            self.assertFalse(ok)
            self.assertIn(expected, message)

    async def test_default_built_in_announcement_copy_remains_when_no_template_is_configured(self):
        profile_service = await self._attach_real_profile_service()
        member = DummyUser(211)
        mastery_role = DummyRole(9951, position=10, name="History Scholar")
        announce_channel = DummyChannel(99)
        guild = DummyGuild(
            10,
            channels=[self.channel, announce_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        self.bot.guild = guild
        self.bot._channels[announce_channel.id] = announce_channel
        self.bot.profile_service = profile_service

        await self.service.update_category_mastery(
            guild.id,
            category="history",
            enabled=True,
            tier=1,
            role_id=mastery_role.id,
            threshold=10,
            announcement_channel_id=announce_channel.id,
        )

        update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="history", correct=True, points=10)
        events = await self.service._grant_progression_rewards(
            guild=guild,
            member=member,
            fallback_channel=self.channel,
            category="history",
            update=update,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(len(announce_channel.sent), 1)
        self.assertNotIn("content", announce_channel.sent[-1][1])
        self.assertIn("Unlocked", announce_channel.sent[-1][1]["embed"].title)
        self.assertIn(member.mention, announce_channel.sent[-1][1]["embed"].description)
        self.assertIn(f"<@&{mastery_role.id}>", announce_channel.sent[-1][1]["embed"].description)
        self.assertTrue(announce_channel.sent[-1][1]["allowed_mentions"].users)
        self.assertTrue(announce_channel.sent[-1][1]["allowed_mentions"].roles)

    async def test_custom_template_renders_safely_with_users_only_mentions(self):
        profile_service = await self._attach_real_profile_service()
        member = DummyUser(212, display_name="**Ada** @here")
        mastery_role = DummyRole(9952, position=10, name="[Elite](https://bad.example) @everyone")
        announce_channel = DummyChannel(100)
        guild = DummyGuild(
            10,
            channels=[self.channel, announce_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        self.bot.guild = guild
        self.bot._channels[announce_channel.id] = announce_channel
        self.bot.profile_service = profile_service

        await self.service.update_category_mastery(
            guild.id,
            category="science",
            enabled=True,
            tier=1,
            role_id=mastery_role.id,
            threshold=10,
            announcement_channel_id=announce_channel.id,
        )
        ok, message = await self.service.save_category_mastery_announcement_template(
            guild.id,
            category="science",
            template="{user.mention} | {user.display_name} | {role.name} | {category.name} | {threshold}",
        )
        self.assertTrue(ok, message)

        update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="science", correct=True, points=10)
        events = await self.service._grant_progression_rewards(
            guild=guild,
            member=member,
            fallback_channel=self.channel,
            category="science",
            update=update,
        )

        self.assertEqual(len(events), 1)
        sent = announce_channel.sent[-1][1]
        self.assertEqual(sent["content"].count(member.mention), 1)
        self.assertIn("this role", sent["content"])
        self.assertNotIn(f"<@&{mastery_role.id}>", sent["content"])
        self.assertFalse(sent["allowed_mentions"].roles)
        self.assertTrue(sent["allowed_mentions"].users)
        self.assertFalse(sent["allowed_mentions"].everyone)

    async def test_tier_override_wins_over_scope_default_for_live_announcements(self):
        profile_service = await self._attach_real_profile_service()
        member = DummyUser(215)
        mastery_role = DummyRole(9957, position=10, name="Science I")
        announce_channel = DummyChannel(103)
        guild = DummyGuild(
            10,
            channels=[self.channel, announce_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        self.bot.guild = guild
        self.bot._channels[announce_channel.id] = announce_channel
        self.bot.profile_service = profile_service

        await self.service.update_category_mastery(
            guild.id,
            category="science",
            enabled=True,
            tier=1,
            role_id=mastery_role.id,
            threshold=10,
            announcement_channel_id=announce_channel.id,
        )
        ok, message = await self.service.save_category_mastery_announcement_template(
            guild.id,
            category="science",
            template="scope default {threshold}",
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.save_category_mastery_announcement_template(
            guild.id,
            category="science",
            tier=1,
            template="tier override {tier.label}",
        )
        self.assertTrue(ok, message)

        update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="science", correct=True, points=10)
        events = await self.service._grant_progression_rewards(
            guild=guild,
            member=member,
            fallback_channel=self.channel,
            category="science",
            update=update,
        )

        self.assertEqual(len(events), 1)
        sent = announce_channel.sent[-1][1]
        self.assertIn("tier override", sent["content"])
        self.assertNotIn("scope default", sent["content"])

    async def test_missing_or_blocked_announcement_channel_skips_template_announcement_without_fallback(self):
        profile_service = await self._attach_real_profile_service()
        member = DummyUser(213)
        mastery_role = DummyRole(9953, position=10, name="History I")
        blocked_channel = DummyChannel(101, can_send=False)
        guild = DummyGuild(
            10,
            channels=[self.channel, blocked_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        self.bot.guild = guild
        self.bot._channels[blocked_channel.id] = blocked_channel
        self.bot.profile_service = profile_service

        await self.service.update_category_mastery(
            guild.id,
            category="history",
            enabled=True,
            tier=1,
            role_id=mastery_role.id,
            threshold=10,
            announcement_channel_id=blocked_channel.id,
        )
        ok, message = await self.service.save_category_mastery_announcement_template(
            guild.id,
            category="history",
            template="{user.mention} reached {category.name}.",
        )
        self.assertTrue(ok, message)

        update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="history", correct=True, points=10)
        events = await self.service._grant_progression_rewards(
            guild=guild,
            member=member,
            fallback_channel=self.channel,
            category="history",
            update=update,
        )

        self.assertEqual(len(events), 1)
        self.assertEqual(blocked_channel.sent, [])
        self.assertEqual(self.channel.sent, [])

    async def test_recalculate_mastery_roles_never_posts_custom_template_announcements(self):
        profile_service = await self._attach_real_profile_service()
        member = DummyUser(214)
        mastery_role = DummyRole(9954, position=10, name="Logic I")
        announce_channel = DummyChannel(102)
        guild = DummyGuild(
            10,
            channels=[self.channel, announce_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        self.bot.guild = guild
        self.bot._channels[announce_channel.id] = announce_channel
        self.bot.profile_service = profile_service

        await self.service.update_category_mastery(
            guild.id,
            category="logic",
            enabled=True,
            tier=1,
            role_id=mastery_role.id,
            threshold=10,
            announcement_channel_id=announce_channel.id,
        )
        ok, message = await self.service.save_category_mastery_announcement_template(
            guild.id,
            category="logic",
            template="{user.mention} reached {category.name}.",
        )
        self.assertTrue(ok, message)
        await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="logic", correct=True, points=12)

        preview = await self.service.recalculate_mastery_roles(guild, member=member, preview=True)
        execute = await self.service.recalculate_mastery_roles(guild, member=member, preview=False)

        self.assertEqual(preview["pending"], 1)
        self.assertEqual(execute["granted"], 1)
        self.assertEqual(announce_channel.sent, [])

    async def test_status_embed_surfaces_default_vs_custom_copy_flags(self):
        science_role = DummyRole(9955, position=10, name="Science I")
        scholar_role = DummyRole(9956, position=11, name="Scholar I")
        self.guild._roles[science_role.id] = science_role
        self.guild._roles[scholar_role.id] = scholar_role
        await self.service.update_category_mastery(
            self.guild.id,
            category="science",
            enabled=True,
            tier=1,
            role_id=science_role.id,
            threshold=10,
        )
        await self.service.update_scholar_ladder(
            self.guild.id,
            enabled=True,
            tier=1,
            role_id=scholar_role.id,
            threshold=20,
        )
        ok, message = await self.service.save_scholar_announcement_template(
            self.guild.id,
            template="{user.mention} reached {tier.label}.",
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.save_category_mastery_announcement_template(
            self.guild.id,
            category="science",
            tier=1,
            template="{user.mention} reached {tier.label}.",
        )
        self.assertTrue(ok, message)

        embed = self.service.build_status_embed(self.guild, await self.service.get_status_snapshot(self.guild))
        mastery_roles = next(field.value for field in embed.fields if field.name == "Mastery Roles")
        scholar_field = next(field.value for field in embed.fields if field.name == "Scholar Ladder")

        self.assertIn("default copy", mastery_roles)
        self.assertIn("1 tier override", mastery_roles)
        self.assertIn("announce off", mastery_roles)
        self.assertIn("custom default", scholar_field)
        self.assertIn("announce off", scholar_field)
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

    async def test_status_embed_honestly_notes_idle_skip_and_high_frequency_reuse(self):
        await self.service.update_config(self.guild.id, drops_per_day=10, activity_gate="light")

        snapshot = await self.service.get_status_snapshot(self.guild)
        embed = self.service.build_status_embed(self.guild, snapshot)

        self.assertIn("Quiet channels can skip a slot.", embed.description)
        self.assertIn("Higher daily counts recycle sooner once the fresh pool thins.", embed.description)

    async def test_disabling_all_categories_pauses_selection_and_next_slot(self):
        for category in list(self.service.get_config(self.guild.id)["enabled_categories"]):
            ok, message = await self.service.update_categories(self.guild.id, action="disable", category=category)
            self.assertTrue(ok, message)

        config = self.service.get_config(self.guild.id)
        self.assertEqual(config["enabled_categories"], [])
        self.assertIsNone(
            self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=[],
                slot_key="2026-03-30:0",
                config=config,
            )
        )
        self.assertIsNone(await self.service._next_slot_for_guild(self.guild.id, config=config))

        snapshot = await self.service.get_status_snapshot(self.guild)
        embed = self.service.build_status_embed(self.guild, snapshot)
        categories_field = next(field.value for field in embed.fields if field.name == "Categories")
        self.assertIn("No enabled categories", categories_field)

    async def test_reset_restores_all_categories_explicitly(self):
        for category in list(self.service.get_config(self.guild.id)["enabled_categories"]):
            ok, message = await self.service.update_categories(self.guild.id, action="disable", category=category)
            self.assertTrue(ok, message)

        ok, message = await self.service.update_categories(self.guild.id, action="reset")
        self.assertTrue(ok, message)
        self.assertCountEqual(self.service.get_config(self.guild.id)["enabled_categories"], QUESTION_DROP_CATEGORIES)

    async def test_status_embed_distinguishes_off_setup_needed_and_ready_progression(self):
        def field_value(embed: discord.Embed, name: str) -> str:
            return next(field.value for field in embed.fields if field.name == name)

        snapshot = await self.service.get_status_snapshot(self.guild)
        embed = self.service.build_status_embed(self.guild, snapshot)
        knowledge_lane = field_value(embed, "Knowledge Lane")
        self.assertIn("Mastery: **Off**", knowledge_lane)
        self.assertIn("Scholar ladder: **Off**", knowledge_lane)

        await self.service.update_category_mastery(self.guild.id, category="science", enabled=True)
        await self.service.update_scholar_ladder(self.guild.id, enabled=True)
        snapshot = await self.service.get_status_snapshot(self.guild)
        embed = self.service.build_status_embed(self.guild, snapshot)
        knowledge_lane = field_value(embed, "Knowledge Lane")
        self.assertIn("Mastery: **Setup needed**", knowledge_lane)
        self.assertIn("Scholar ladder: **Setup needed**", knowledge_lane)
        self.assertIn("thresholds and roles still need setup", field_value(embed, "Mastery Setup"))
        self.assertIn("thresholds and roles still need setup", field_value(embed, "Scholar Ladder"))

        science_role = DummyRole(8801, position=10, name="Science Explorer")
        scholar_role = DummyRole(8802, position=11, name="Scholar I")
        ready_guild = DummyGuild(
            self.guild.id,
            channels=[self.channel],
            roles=[science_role, scholar_role],
            me=DummyBotMember(position=50),
        )
        await self.service.update_category_mastery(
            self.guild.id,
            category="science",
            enabled=True,
            tier=1,
            role_id=science_role.id,
            threshold=10,
        )
        await self.service.update_scholar_ladder(
            self.guild.id,
            enabled=True,
            tier=1,
            role_id=scholar_role.id,
            threshold=25,
        )
        snapshot = await self.service.get_status_snapshot(ready_guild)
        embed = self.service.build_status_embed(ready_guild, snapshot)
        knowledge_lane = field_value(embed, "Knowledge Lane")
        self.assertIn("Mastery: **Ready**", knowledge_lane)
        self.assertIn("Scholar ladder: **Ready**", knowledge_lane)

    async def test_exact_threshold_category_role_unlocks_once_without_regrant(self):
        member = DummyUser(91)
        role = DummyRole(301, position=10, name="Science Explorer")
        guild = DummyGuild(10, channels=[self.channel], roles=[role], members=[member], me=DummyBotMember(position=50))
        bot = DummyBot(guild, [self.channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            ok, message = await service.update_category_mastery(
                guild.id,
                category="science",
                enabled=True,
                tier=1,
                role_id=role.id,
                threshold=10,
            )
            self.assertTrue(ok, message)

            update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="science", correct=True, points=10)
            events = await service._grant_progression_rewards(
                guild=guild,
                member=member,
                fallback_channel=self.channel,
                category="science",
                update=update,
            )
            self.assertEqual([event["tier"] for event in events], [1])
            self.assertTrue(any(saved_role.id == role.id for saved_role in member.roles))

            second_events = await service._grant_progression_rewards(
                guild=guild,
                member=member,
                fallback_channel=self.channel,
                category="science",
                update=update,
            )
            self.assertEqual(second_events, [])
            self.assertEqual(member.add_roles.await_count, 1)
        finally:
            await service.close()
            await profile_service.close()

    async def test_multiple_thresholds_crossed_at_once_grants_all_new_roles(self):
        member = DummyUser(92)
        roles = [
            DummyRole(401, position=10, name="Logic I"),
            DummyRole(402, position=11, name="Logic II"),
            DummyRole(403, position=12, name="Logic III"),
        ]
        guild = DummyGuild(10, channels=[self.channel], roles=roles, members=[member], me=DummyBotMember(position=50))
        bot = DummyBot(guild, [self.channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            await service.update_category_mastery(guild.id, category="logic", enabled=True, tier=1, role_id=roles[0].id, threshold=10)
            await service.update_category_mastery(guild.id, category="logic", enabled=True, tier=2, role_id=roles[1].id, threshold=20)
            await service.update_category_mastery(guild.id, category="logic", enabled=True, tier=3, role_id=roles[2].id, threshold=30)

            update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="logic", correct=True, points=35)
            events = await service._grant_progression_rewards(
                guild=guild,
                member=member,
                fallback_channel=self.channel,
                category="logic",
                update=update,
            )

            self.assertEqual([event["tier"] for event in events], [1, 2, 3])
            self.assertEqual(sorted(role.id for role in member.roles), [401, 402, 403])
        finally:
            await service.close()
            await profile_service.close()

    async def test_silent_mode_suppresses_role_announcement(self):
        member = DummyUser(93)
        mastery_role = DummyRole(501, position=10, name="History Scholar")
        announce_channel = DummyChannel(99)
        guild = DummyGuild(
            10,
            channels=[self.channel, announce_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        bot = DummyBot(guild, [self.channel, announce_channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            await service.update_category_mastery(
                guild.id,
                category="history",
                enabled=True,
                tier=1,
                role_id=mastery_role.id,
                threshold=10,
                announcement_channel_id=announce_channel.id,
                silent_grant=True,
            )

            update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="history", correct=True, points=10)
            events = await service._grant_progression_rewards(
                guild=guild,
                member=member,
                fallback_channel=self.channel,
                category="history",
                update=update,
            )

            self.assertEqual(len(events), 1)
            self.assertEqual(announce_channel.sent, [])
            self.assertEqual(self.channel.sent, [])
        finally:
            await service.close()
            await profile_service.close()

    async def test_unusable_announcement_channel_skips_default_announcement_without_fallback(self):
        member = DummyUser(94)
        mastery_role = DummyRole(601, position=10, name="History Scholar")
        blocked_channel = DummyChannel(99, can_send=False)
        guild = DummyGuild(
            10,
            channels=[self.channel, blocked_channel],
            roles=[mastery_role],
            members=[member],
            me=DummyBotMember(position=50),
        )
        bot = DummyBot(guild, [self.channel, blocked_channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            await service.update_category_mastery(
                guild.id,
                category="history",
                enabled=True,
                tier=1,
                role_id=mastery_role.id,
                threshold=10,
                announcement_channel_id=blocked_channel.id,
            )

            update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="history", correct=True, points=10)
            events = await service._grant_progression_rewards(
                guild=guild,
                member=member,
                fallback_channel=self.channel,
                category="history",
                update=update,
            )

            self.assertEqual(len(events), 1)
            self.assertEqual(blocked_channel.sent, [])
            self.assertEqual(self.channel.sent, [])
        finally:
            await service.close()
            await profile_service.close()

    async def test_recalculate_mastery_roles_preview_execute_and_no_regrant(self):
        member = DummyUser(95)
        mastery_role = DummyRole(701, position=10, name="Logic Explorer")
        guild = DummyGuild(10, channels=[self.channel], roles=[mastery_role], members=[member], me=DummyBotMember(position=50))
        bot = DummyBot(guild, [self.channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            await service.update_category_mastery(
                guild.id,
                category="logic",
                enabled=True,
                tier=1,
                role_id=mastery_role.id,
                threshold=10,
            )
            await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="logic", correct=True, points=12)

            preview = await service.recalculate_mastery_roles(guild, member=member, preview=True)
            self.assertEqual(preview, {"preview": True, "scanned": 1, "pending": 1, "granted": 0, "skipped_opted_out": 0})
            member.add_roles.assert_not_awaited()

            execute = await service.recalculate_mastery_roles(guild, member=member, preview=False)
            self.assertEqual(execute, {"preview": False, "scanned": 1, "pending": 1, "granted": 1, "skipped_opted_out": 0})
            self.assertEqual(member.add_roles.await_count, 1)

            rerun = await service.recalculate_mastery_roles(guild, member=member, preview=False)
            self.assertEqual(rerun, {"preview": False, "scanned": 1, "pending": 0, "granted": 0, "skipped_opted_out": 0})
            self.assertEqual(member.add_roles.await_count, 1)
        finally:
            await service.close()
            await profile_service.close()

    async def test_disabled_progression_features_do_not_tease_next_tiers(self):
        member = DummyUser(96)
        role = DummyRole(801, position=10, name="Science Explorer")
        guild = DummyGuild(10, channels=[self.channel], roles=[role], members=[member], me=DummyBotMember(position=50))
        bot = DummyBot(guild, [self.channel])
        store = QuestionDropsStore(backend="memory")
        await store.load()
        service = QuestionDropsService(bot, store=store)
        profile_service = ProfileService(types.SimpleNamespace(get_user=lambda user_id: None), store=ProfileStore(backend="memory"))
        try:
            self.assertTrue(await service.start())
            self.assertTrue(await profile_service.start())
            if service._scheduler_task is not None:
                service._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await service._scheduler_task
                service._scheduler_task = None
            bot.profile_service = profile_service
            await service.update_category_mastery(guild.id, category="science", tier=1, role_id=role.id, threshold=20)
            await service.update_scholar_ladder(guild.id, tier=1, role_id=role.id, threshold=30)

            update = await profile_service.record_question_drop_result(member.id, guild_id=guild.id, category="science", correct=True, points=12)
            summary = await profile_service.get_question_drop_summary(member.id, guild_id=guild.id)
            stats_embed = service.build_stats_embed(member, summary)
            stats_fields = {field.name: field.value for field in stats_embed.fields}

            self.assertNotIn("Next:", stats_fields["Scholar"])
            self.assertNotIn(" to Tier ", stats_fields["Top Categories"])

            solve_embed = await service._build_solve_embed(
                winner=member,
                category="science",
                answer_spec={"type": "text", "accepted": ["mars"]},
                update=update,
                role_events=[],
                fallback_points=12,
            )
            self.assertNotIn(" pts to ", solve_embed.description)
        finally:
            await service.close()
            await profile_service.close()

    async def test_timed_out_drop_uses_clean_closure_copy(self):
        active = await self._post_one_drop()

        await self.service._expire_drop(active, timed_out=True)

        self.assertEqual(len(self.service._active_drops), 0)
        self.assertEqual(self.channel.sent[-1][1]["embed"].title, "Time's Up")
        self.assertIn("No clean solve this time.", self.channel.sent[-1][1]["embed"].description)

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
