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
    answer_attempt_limit,
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
from babblebox.question_drops_service import QUESTION_DROP_LATE_CORRECT_WINDOW_SECONDS, QuestionDropsService, _slot_seed_material
from babblebox.question_drops_store import QuestionDropsStore, _active_drop_from_row, _config_from_row
from babblebox.premium_limits import CAPABILITY_QUESTION_DROPS_AI_CELEBRATIONS
from babblebox.profile_service import ProfileService
from babblebox.profile_store import ProfileStore


class DummyRole:
    def __init__(self, role_id: int, *, position: int = 10, name: str | None = None, mentionable: bool = True, is_default: bool = False):
        self.id = role_id
        self.position = position
        self.name = name or f"Role {role_id}"
        self.mentionable = mentionable
        self.mention = f"<@&{role_id}>"
        self._is_default = is_default

    def is_default(self) -> bool:
        return self._is_default


class DummyBotMember:
    def __init__(self, user_id: int = 999, *, position: int = 50, manage_roles: bool = True, mention_everyone: bool = False):
        self.id = user_id
        self.top_role = DummyRole(100000 + user_id, position=position)
        self.guild_permissions = types.SimpleNamespace(manage_roles=manage_roles, mention_everyone=mention_everyone)


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


class DummySentMessage:
    def __init__(self, message_id: int, channel: "DummyChannel", *, args=(), kwargs=None):
        self.id = message_id
        self.channel = channel
        self.args = args
        self.kwargs = dict(kwargs or {})
        self.deleted = False
        self.delete = AsyncMock(side_effect=self._delete)
        self.edit = AsyncMock(side_effect=self._edit)

    async def _delete(self):
        self.deleted = True

    async def _edit(self, **kwargs):
        self.kwargs.update(kwargs)
        return self


class DummyChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        fail_send: bool = False,
        can_view: bool = True,
        can_send: bool = True,
        can_embed: bool = True,
        can_read_history: bool = True,
        can_add_reactions: bool = True,
        can_mention_everyone: bool = False,
    ):
        self.id = channel_id
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self.fail_send = fail_send
        self.can_view = can_view
        self.can_send = can_send
        self.can_embed = can_embed
        self.can_read_history = can_read_history
        self.can_add_reactions = can_add_reactions
        self.can_mention_everyone = can_mention_everyone
        self._messages: dict[int, DummySentMessage] = {}
        self.guild = None

    async def send(self, *args, **kwargs):
        if self.fail_send:
            raise discord.HTTPException(types.SimpleNamespace(status=500, reason="fail"), "send failed")
        payload = dict(kwargs)
        message = DummySentMessage(5000 + len(self.sent), self, args=args, kwargs=payload)
        self.sent.append((args, payload, message))
        self._messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int):
        message = self._messages.get(int(message_id))
        if message is None or message.deleted:
            raise discord.NotFound(types.SimpleNamespace(status=404, reason="missing"), "missing")
        return message

    def permissions_for(self, member):
        return types.SimpleNamespace(
            view_channel=self.can_view,
            send_messages=self.can_send,
            embed_links=self.can_embed,
            read_message_history=self.can_read_history,
            add_reactions=self.can_add_reactions,
            mention_everyone=self.can_mention_everyone,
        )


class DummyGuild:
    def __init__(self, guild_id: int, channels=None, *, roles=None, members=None, me=None):
        self.id = guild_id
        self.name = "Guild"
        self._channels = {channel.id: channel for channel in (channels or [])}
        self._roles = {role.id: role for role in (roles or [])}
        self.me = me or DummyBotMember()
        for channel in self._channels.values():
            channel.guild = self
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
        self.premium_service = None
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
    _next_id = 9000

    def __init__(
        self,
        *,
        guild: DummyGuild,
        channel: DummyChannel,
        author: DummyUser,
        content: str,
        reference=None,
        message_id: int | None = None,
        created_at: datetime | None = None,
    ):
        self.guild = guild
        self.channel = channel
        self.author = author
        self.content = content
        self.reference = reference
        self.id = int(message_id) if isinstance(message_id, int) else DummyMessage._next_id
        self.created_at = created_at or ge.now_utc()
        DummyMessage._next_id = max(DummyMessage._next_id + 1, self.id + 1)
        self.reactions = []
        self.add_reaction = AsyncMock(side_effect=self._add_reaction)

    async def _add_reaction(self, emoji: str):
        if any(str(getattr(reaction, "emoji", "")) == str(emoji) and bool(getattr(reaction, "me", False)) for reaction in self.reactions):
            return
        self.reactions.append(types.SimpleNamespace(emoji=emoji, me=True))


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
        self.assertGreaterEqual(len(QUESTION_DROP_SEEDS), 150)
        hard_seeds = [seed for seed in QUESTION_DROP_SEEDS if int(seed["difficulty"]) == 3]
        self.assertGreaterEqual(len(hard_seeds), 35)
        self.assertTrue(all(str(seed.get("family_id") or "").strip() for seed in QUESTION_DROP_SEEDS))
        for category in QUESTION_DROP_CATEGORIES:
            with self.subTest(category=category):
                family_ids = {seed["family_id"] for seed in QUESTION_DROP_SEEDS if seed["category"] == category}
                self.assertGreaterEqual(len(family_ids), 18)
                self.assertGreaterEqual(
                    sum(1 for seed in hard_seeds if seed["category"] == category),
                    1 if category == "culture" else 4,
                )
        self.assertGreaterEqual(
            len({seed["family_id"] for seed in QUESTION_DROP_SEEDS if seed["category"] == "math"}),
            18,
        )
        self.assertGreaterEqual(
            len({seed["family_id"] for seed in QUESTION_DROP_SEEDS if seed["category"] == "logic"}),
            18,
        )

    def test_content_pack_tags_cover_subcategory_reasoning_and_answer_shape(self):
        for seed in QUESTION_DROP_SEEDS:
            with self.subTest(concept_id=seed["concept_id"]):
                tags = tuple(seed.get("tags", ()))
                self.assertTrue(any(str(tag).startswith("sub:") for tag in tags))
                self.assertTrue(any(str(tag).startswith("mode:") for tag in tags))
                self.assertTrue(any(str(tag).startswith("shape:") for tag in tags))

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

    def test_generated_live_rotation_samples_keep_unique_prompts(self):
        generated_seeds = [seed for seed in QUESTION_DROP_SEEDS if seed["source_type"] == "generated"]

        for seed in generated_seeds:
            with self.subTest(concept_id=seed["concept_id"]):
                prompts = [
                    " ".join(str(build_variant(seed, seed_material=f"audit:{seed['concept_id']}", variant_index=index).prompt).casefold().split())
                    for index in range(12)
                ]
                self.assertEqual(len(prompts), len(set(prompts)))

    def test_candidate_iteration_preserves_family_ids_for_generated_depth(self):
        variants = iter_candidate_variants(categories=set(QUESTION_DROP_CATEGORIES), seed_material="coverage", variants_per_seed=12)
        self.assertTrue(variants)
        self.assertTrue(all(variant.family_id for variant in variants))
        self.assertGreaterEqual(len(variants), 800)
        for category in QUESTION_DROP_CATEGORIES:
            with self.subTest(category=category):
                self.assertGreaterEqual(len({variant.family_id for variant in variants if variant.category == category}), 18)

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
        ordered = {"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]}
        self.assertTrue(judge_answer(ordered, "printing press, telephone, internet"))
        self.assertTrue(judge_answer(ordered, "printing press -> telephone -> internet"))
        self.assertTrue(judge_answer(ordered, "1) printing press 2) telephone 3) internet"))
        self.assertTrue(judge_answer(ordered, "printing press\ntelephone\ninternet"))
        self.assertFalse(judge_answer(ordered, "telephone, printing press, internet"))
        self.assertFalse(judge_answer(ordered, "printing press telephone internet"))
        self.assertEqual(normalize_answer_text("  Hello,  World! "), "hello world")

    def test_text_answer_judging_allows_bounded_typos_and_quote_normalization_only(self):
        self.assertEqual(normalize_answer_text("Newton’s law"), "newtons law")
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Mercury"]}, "mercuri"))
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Mercury"]}, "mrecury"))
        self.assertTrue(judge_answer({"type": "text", "accepted": ["Newton's law"]}, "newtons law"))
        self.assertTrue(judge_answer({"type": "text", "accepted": ["the moon"]}, "moon"))
        self.assertFalse(judge_answer({"type": "text", "accepted": ["Mercury"]}, "planet mercury"))
        self.assertFalse(judge_answer({"type": "text", "accepted": ["Mercury"]}, "venus"))

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
        self.assertEqual(answer_attempt_limit(answer_spec), 3)
        self.assertIn("3 attempts", render_answer_instruction(answer_spec))
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

        ordered = {"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]}
        self.assertTrue(is_answer_attempt(ordered, "telephone, printing press, internet"))
        self.assertTrue(is_answer_attempt(ordered, "1) telephone 2) printing press 3) internet"))
        self.assertTrue(is_answer_attempt(ordered, "I think it's telephone -> printing press -> internet"))
        self.assertFalse(is_answer_attempt(ordered, "printing press changed everything"))
        self.assertFalse(is_answer_attempt(ordered, "it is true"))

    def test_answer_attempt_gate_handles_hedged_payloads_without_grabbing_soft_chatter(self):
        text_spec = {"type": "text", "accepted": ["mars"]}
        self.assertTrue(is_answer_attempt(text_spec, "I think it's venus"))
        self.assertTrue(is_answer_attempt(text_spec, "is it venus?"))
        self.assertTrue(judge_answer(text_spec, "I think it's mars"))
        self.assertFalse(is_answer_attempt(text_spec, "maybe later"))
        self.assertFalse(is_answer_attempt(text_spec, "perhaps tomorrow"))

        numeric_spec = {"type": "numeric", "value": 14}
        self.assertTrue(is_answer_attempt(numeric_spec, "I think maybe 14?"))
        self.assertTrue(judge_answer(numeric_spec, "I think maybe 14?"))

        multiple_choice = {"type": "multiple_choice", "choices": ["red", "yellow", "green"], "answer": "green"}
        self.assertTrue(is_answer_attempt(multiple_choice, "is it c?"))
        self.assertTrue(judge_answer(multiple_choice, "is it c?"))

    def test_answer_attempt_gate_rejects_both_user_mention_token_forms(self):
        text_spec = {"type": "text", "accepted": ["moon"]}
        self.assertFalse(is_answer_attempt(text_spec, "<@123> moon", direct_reply=True))
        self.assertFalse(is_answer_attempt(text_spec, "<@!123> moon", direct_reply=True))
        self.assertFalse(judge_answer(text_spec, "<@!123> moon"))

    def test_render_answer_instruction_matches_answer_type(self):
        numeric_instruction = render_answer_instruction({"type": "numeric", "value": 12})
        multiple_choice_instruction = render_answer_instruction({"type": "multiple_choice", "choices": ["a"], "answer": "a"})
        boolean_instruction = render_answer_instruction({"type": "boolean", "value": True})
        ordered_instruction = render_answer_instruction({"type": "ordered_tokens", "tokens": ["red", "blue"]})
        text_instruction = render_answer_instruction({"type": "text", "accepted": ["mars"]})

        self.assertIn("Reply is optional", numeric_instruction)
        self.assertIn("same-channel", numeric_instruction)
        self.assertIn("number words", numeric_instruction)
        self.assertIn("3 attempts", numeric_instruction)

        self.assertIn("option text", multiple_choice_instruction)
        self.assertIn("1 attempt", multiple_choice_instruction)

        self.assertIn("true", boolean_instruction)
        self.assertIn("1 attempt", boolean_instruction)

        self.assertIn("full sequence", ordered_instruction)
        self.assertIn("1 attempt", ordered_instruction)
        self.assertIn("using commas", ordered_instruction)
        self.assertEqual(answer_attempt_limit({"type": "ordered_tokens", "tokens": ["red", "blue"]}), 1)

        self.assertIn("short clean same-channel guess", text_instruction)
        self.assertIn("3 attempts", text_instruction)

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
            "drop_ping_role_id": 555,
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
        self.assertEqual(config["drop_ping_role_id"], 555)
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
            "drop_ping_role_id": "bad",
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
        self.assertIsNone(config["drop_ping_role_id"])
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
            "attempt_counts_by_user": json.dumps({"5": 2, "9": 1}),
        }

        record = _active_drop_from_row(row)

        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record["answer_spec"]["accepted"], ["Mars"])
        self.assertEqual(record["participant_user_ids"], [5, 7])
        self.assertEqual(record["attempt_counts_by_user"], {5: 2, 9: 1})


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

    async def _post_one_drop(self, *, now: datetime | None = None, round_to_minute: bool = True):
        now = now or ge.now_utc()
        if round_to_minute:
            now = now.replace(second=0, microsecond=0)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=now), patch(
            "babblebox.question_drops_service._daily_slot_datetimes",
            return_value=[now.astimezone(now.tzinfo or timezone.utc)],
        ):
            await self.service._maybe_post_due_drops()
        self.assertEqual(len(self.service._active_drops), 1)
        return next(iter(self.service._active_drops.values()))

    def _correct_attempt_content(self, active: dict[str, object], *, prefer_letter: bool = False) -> str:
        answer_spec = active["answer_spec"]
        answer_type = str(answer_spec.get("type"))
        if answer_type == "numeric":
            value = answer_spec.get("value", 0)
            if isinstance(value, float) and value.is_integer():
                value = int(value)
            return str(value)
        if answer_type == "boolean":
            return "true" if bool(answer_spec.get("value")) else "false"
        if answer_type == "multiple_choice":
            if prefer_letter:
                answer = normalize_answer_text(answer_spec.get("answer"))
                for index, choice in enumerate(answer_spec.get("choices", [])):
                    if normalize_answer_text(choice) == answer:
                        return chr(ord("A") + index)
            return str(answer_spec.get("answer", ""))
        if answer_type == "ordered_tokens":
            return ", ".join(str(token) for token in answer_spec.get("tokens", []))
        return str(answer_spec.get("accepted", [""])[0])

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

    def _reply_reference(self, message_id: int):
        return types.SimpleNamespace(message_id=message_id)

    def _last_batch_results(self):
        return self.bot.profile_service.record_question_drop_results_batch.await_args.args[0]

    def _variant_tag_value(self, variant: QuestionDropVariant, prefix: str) -> str:
        for tag in getattr(variant, "tags", ()):
            if isinstance(tag, str) and tag.startswith(prefix):
                value = tag[len(prefix) :].strip().casefold()
                if value:
                    return value
        return ""

    def _variant_repeat_keys(self, variant: QuestionDropVariant) -> dict[str, object]:
        answer_shape = self._variant_tag_value(variant, "shape:") or str(variant.answer_spec.get("type") or "text").strip().casefold()
        subcategory = self._variant_tag_value(variant, "sub:")
        reasoning_mode = self._variant_tag_value(variant, "mode:")
        return {
            "answer_shape": answer_shape,
            "category_sub_shape": (variant.category, subcategory, answer_shape),
            "category_mode_shape": (variant.category, reasoning_mode, answer_shape),
        }

    def _category_repeat_candidates_for_slot(self, slot_key: str) -> dict[str, QuestionDropVariant]:
        config = self.service.get_config(self.guild.id)
        return {
            variant.concept_id: variant
            for variant in iter_candidate_variants(
                categories=set(self.service._enabled_categories(config)),
                seed_material=_slot_seed_material(self.guild.id, self.channel.id, slot_key),
                variants_per_seed=12,
            )
        }

    async def _build_selector_state(self, *, profile: str, drops_per_day: int, until_slot: str) -> tuple[datetime, list[dict[str, object]], list[tuple[str, QuestionDropVariant]]]:
        self.assertIn(profile, QUESTION_DROP_DIFFICULTY_PROFILES)
        ok, message = await self.service.update_config(
            self.guild.id,
            drops_per_day=drops_per_day,
            difficulty_profile=profile,
        )
        self.assertTrue(ok, message)

        exposures: list[dict[str, object]] = []
        picks: list[tuple[str, QuestionDropVariant]] = []
        base = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)

        for day in range(30):
            current_time = base + timedelta(days=day)
            slot_day = current_time.date().isoformat()
            with patch("babblebox.question_drops_service.ge.now_utc", return_value=current_time):
                for slot in range(drops_per_day):
                    slot_key = f"{slot_day}:{slot}"
                    if slot_key == until_slot:
                        return current_time, exposures, picks
                    variant = self.service._select_variant(
                        self.guild.id,
                        self.channel.id,
                        exposures=exposures,
                        slot_key=slot_key,
                        config=self.service.get_config(self.guild.id),
                    )
                    self.assertIsNotNone(variant)
                    picks.append((slot_key, variant))
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

        self.fail(f"Did not reach selector slot {until_slot}.")

    def _recent_exposures(self, *, current_time: datetime, concept_ids: list[str]) -> list[dict[str, object]]:
        exposures: list[dict[str, object]] = []
        for index, concept_id in enumerate(concept_ids):
            seed = question_drop_seed_for_concept(concept_id)
            self.assertIsNotNone(seed)
            exposures.append(
                {
                    "guild_id": self.guild.id,
                    "channel_id": self.channel.id,
                    "concept_id": concept_id,
                    "variant_hash": f"{concept_id}:{index}",
                    "category": seed["category"],
                    "difficulty": seed["difficulty"],
                    "asked_at": (current_time - timedelta(minutes=index + 1)).isoformat(),
                    "resolved_at": None,
                    "winner_user_id": None,
                    "slot_key": f"recent:{index}",
                }
            )
        return exposures

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
        category_counts: Counter[str] = Counter()
        family_repeats = 0
        max_hard_run = 0
        current_hard_run = 0
        max_shape_run = 0
        current_shape_run = 0
        previous_family: str | None = None
        previous_shape: str | None = None
        category_sub_shape_repeats: list[dict[str, object]] = []
        category_mode_shape_repeats: list[dict[str, object]] = []
        known_offender_repeats: list[dict[str, object]] = []
        last_sub_shape_seen: dict[tuple[str, str, str], tuple[int, str, QuestionDropVariant]] = {}
        last_mode_shape_seen: dict[tuple[str, str, str], tuple[int, str, QuestionDropVariant]] = {}
        known_offender_pairs = {
            frozenset(("logic:true-false", "logic:not-icy-not-comet")),
            frozenset(("language:oxford-comma-boolean", "language:semicolon-clauses")),
            frozenset(("language:parallel-structure", "language:malapropism-definition")),
        }
        pick_sequence: list[dict[str, object]] = []
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
                    category_counts[variant.category] += 1
                    if int(variant.difficulty) == 3:
                        current_hard_run += 1
                        max_hard_run = max(max_hard_run, current_hard_run)
                    else:
                        current_hard_run = 0
                    repeat_keys = self._variant_repeat_keys(variant)
                    answer_shape = str(repeat_keys["answer_shape"])
                    if answer_shape == previous_shape:
                        current_shape_run += 1
                    else:
                        current_shape_run = 1
                        previous_shape = answer_shape
                    max_shape_run = max(max_shape_run, current_shape_run)
                    pick_index = len(pick_sequence)
                    sub_shape_key = tuple(repeat_keys["category_sub_shape"])
                    mode_shape_key = tuple(repeat_keys["category_mode_shape"])
                    previous_sub_shape = last_sub_shape_seen.get(sub_shape_key)
                    if previous_sub_shape is not None:
                        previous_index, previous_slot_key, previous_variant = previous_sub_shape
                        gap = pick_index - previous_index
                        event = {
                            "gap": gap,
                            "key": sub_shape_key,
                            "slot_key": slot_key,
                            "previous_slot_key": previous_slot_key,
                            "concept_id": variant.concept_id,
                            "previous_concept_id": previous_variant.concept_id,
                        }
                        category_sub_shape_repeats.append(event)
                        if gap <= 6 and frozenset((variant.concept_id, previous_variant.concept_id)) in known_offender_pairs:
                            known_offender_repeats.append(event)
                    last_sub_shape_seen[sub_shape_key] = (pick_index, slot_key, variant)
                    previous_mode_shape = last_mode_shape_seen.get(mode_shape_key)
                    if previous_mode_shape is not None:
                        previous_index, previous_slot_key, previous_variant = previous_mode_shape
                        category_mode_shape_repeats.append(
                            {
                                "gap": pick_index - previous_index,
                                "key": mode_shape_key,
                                "slot_key": slot_key,
                                "previous_slot_key": previous_slot_key,
                                "concept_id": variant.concept_id,
                                "previous_concept_id": previous_variant.concept_id,
                            }
                        )
                    last_mode_shape_seen[mode_shape_key] = (pick_index, slot_key, variant)
                    if previous_family == variant.family_id:
                        family_repeats += 1
                    previous_family = variant.family_id
                    pick_sequence.append(
                        {
                            "slot_key": slot_key,
                            "concept_id": variant.concept_id,
                            "family_id": variant.family_id,
                            "category": variant.category,
                            "difficulty": int(variant.difficulty),
                            "answer_shape": answer_shape,
                            "category_sub_shape": sub_shape_key,
                            "category_mode_shape": mode_shape_key,
                        }
                    )
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
            "category_counts": category_counts,
            "family_repeats": family_repeats,
            "max_hard_run": max_hard_run,
            "max_shape_run": max_shape_run,
            "pick_sequence": pick_sequence,
            "category_sub_shape_repeats": category_sub_shape_repeats,
            "category_mode_shape_repeats": category_mode_shape_repeats,
            "known_offender_repeats": known_offender_repeats,
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

    async def test_selector_high_drop_pressure_does_not_force_generated_source_bias(self):
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
        self.assertEqual(high_pick, curated_variant)

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

    async def test_update_config_ai_opt_in_stays_truthful_without_guild_pro(self):
        await self.service.set_global_ai_celebration_mode("rare", actor_id=1)

        ok, message = await self.service.update_config(self.guild.id, ai_celebrations_enabled=True)

        self.assertTrue(ok, message)
        self.assertIn("needs Guild Pro", message)

    async def test_build_status_embed_surfaces_live_ping_and_ai_premium_state(self):
        role = DummyRole(777, name="Drops Squad")
        self.guild._roles[role.id] = role
        await self.service.set_global_ai_celebration_mode("rare", actor_id=1)
        ok, message = await self.service.update_config(self.guild.id, ai_celebrations_enabled=True)
        self.assertTrue(ok, message)
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_status_embed(self.guild, await self.service.get_status_snapshot(self.guild))
        ai_field = next(field.value for field in embed.fields if field.name == "AI Celebrations")
        ping_field = next(field.value for field in embed.fields if field.name == "Live Ping")

        self.assertIn("Entitlement: **Free**", ai_field)
        self.assertIn("Live state: **Requires Guild Pro**", ai_field)
        self.assertIn("Status: **Configured**", ping_field)
        self.assertIn(role.mention, ping_field)

    async def test_build_drop_ping_status_embed_reports_missing_role(self):
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=9191)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Blocked**", status_field)
        self.assertIn("Configured role is missing.", checks_field)

    async def test_build_drop_ping_status_embed_reports_unmentionable_role_blockers(self):
        role = DummyRole(818, mentionable=False)
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Blocked**", status_field)
        self.assertIn("role is not mentionable", checks_field.lower())
        self.assertIn("Mention Everyone", checks_field)

    async def test_clear_drop_ping_role_returns_off_and_future_posts_stay_unpinged(self):
        role = DummyRole(819, name="Drops Squad")
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=None)
        self.assertTrue(ok, message)
        self.assertEqual(self.service.get_config(self.guild.id)["drop_ping_role_id"], None)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Off**", status_field)
        self.assertIn("No live drop role ping is configured.", checks_field)

        await self._post_one_drop()

        payload = self.channel.sent[0][1]
        self.assertNotIn("content", payload)
        self.assertNotIn("allowed_mentions", payload)

    async def test_build_drop_ping_status_embed_reports_partial_delivery_checks(self):
        blocked_channel = DummyChannel(21, can_send=False)
        blocked_channel.guild = self.guild
        self.guild._channels[blocked_channel.id] = blocked_channel
        self.bot._channels[blocked_channel.id] = blocked_channel
        ok, message = await self.service.update_channels(self.guild.id, action="add", channel_id=blocked_channel.id)
        self.assertTrue(ok, message)
        role = DummyRole(820, name="Drops Squad")
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Partial**", status_field)
        self.assertIn(f"{self.channel.mention}: role ping will post with the live drop.", checks_field)
        self.assertIn(f"{blocked_channel.mention}: cannot send messages.", checks_field)

    async def test_build_drop_ping_status_embed_reports_missing_view_channel_blocker(self):
        blocked_channel = DummyChannel(22, can_view=False)
        blocked_channel.guild = self.guild
        self.guild._channels[blocked_channel.id] = blocked_channel
        self.bot._channels[blocked_channel.id] = blocked_channel
        ok, message = await self.service.update_channels(self.guild.id, action="clear")
        self.assertTrue(ok, message)
        ok, message = await self.service.update_channels(self.guild.id, action="add", channel_id=blocked_channel.id)
        self.assertTrue(ok, message)
        role = DummyRole(821, name="Drops Squad")
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Blocked**", status_field)
        self.assertIn("cannot view channel", checks_field)

    async def test_build_drop_ping_status_embed_reports_missing_send_messages_blocker(self):
        blocked_channel = DummyChannel(23, can_send=False)
        blocked_channel.guild = self.guild
        self.guild._channels[blocked_channel.id] = blocked_channel
        self.bot._channels[blocked_channel.id] = blocked_channel
        ok, message = await self.service.update_channels(self.guild.id, action="clear")
        self.assertTrue(ok, message)
        ok, message = await self.service.update_channels(self.guild.id, action="add", channel_id=blocked_channel.id)
        self.assertTrue(ok, message)
        role = DummyRole(822, name="Drops Squad")
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Blocked**", status_field)
        self.assertIn("cannot send messages", checks_field)

    async def test_build_drop_ping_status_embed_reports_missing_embed_links_blocker(self):
        blocked_channel = DummyChannel(24, can_embed=False)
        blocked_channel.guild = self.guild
        self.guild._channels[blocked_channel.id] = blocked_channel
        self.bot._channels[blocked_channel.id] = blocked_channel
        ok, message = await self.service.update_channels(self.guild.id, action="clear")
        self.assertTrue(ok, message)
        ok, message = await self.service.update_channels(self.guild.id, action="add", channel_id=blocked_channel.id)
        self.assertTrue(ok, message)
        role = DummyRole(823, name="Drops Squad")
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        embed = self.service.build_drop_ping_status_embed(self.guild)
        status_field = next(field.value for field in embed.fields if field.name == "Status")
        checks_field = next(field.value for field in embed.fields if field.name == "Delivery Checks")

        self.assertIn("State: **Blocked**", status_field)
        self.assertIn("missing Embed Links", checks_field)

    async def test_post_drop_uses_safe_role_ping_when_configured_and_allowed(self):
        role = DummyRole(828, mentionable=True)
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        await self._post_one_drop()

        payload = self.channel.sent[0][1]
        allowed_mentions = payload.get("allowed_mentions")
        self.assertEqual(payload.get("content"), role.mention)
        self.assertIsNotNone(allowed_mentions)
        self.assertTrue(allowed_mentions.roles)
        self.assertFalse(allowed_mentions.users)
        self.assertFalse(allowed_mentions.everyone)

    async def test_post_drop_omits_role_ping_when_role_cannot_be_mentioned(self):
        role = DummyRole(838, mentionable=False)
        self.guild._roles[role.id] = role
        ok, message = await self.service.update_drop_ping_role(self.guild, role_id=role.id)
        self.assertTrue(ok, message)

        await self._post_one_drop()

        payload = self.channel.sent[0][1]
        self.assertNotIn("content", payload)
        self.assertNotIn("allowed_mentions", payload)

    async def test_maybe_ai_highlight_requires_guild_pro_capability(self):
        provider = types.SimpleNamespace(
            diagnostics=lambda: {"available": True, "status": "Ready."},
            highlight=AsyncMock(return_value="Premium celebration line."),
            close=AsyncMock(),
        )
        self.service._ai_provider = provider
        await self.service.set_global_ai_celebration_mode("rare", actor_id=1)
        ok, message = await self.service.update_config(self.guild.id, ai_celebrations_enabled=True)
        self.assertTrue(ok, message)
        payload = {
            "guild_id": self.guild.id,
            "points_awarded": 20,
            "guild_after": {"current_streak": 2, "best_streak": 2},
            "guild_rank_before": 3,
            "guild_rank_after": 1,
            "category_rank_before": 2,
            "category_rank_after": 1,
        }
        flags = {
            "category_role_events": [],
            "scholar_role_events": [],
            "took_guild_first": True,
            "took_category_first": False,
            "guild_points_milestone": None,
            "new_best_streak": False,
            "guild_rank_jump": 2,
        }

        without_premium = await self.service._maybe_ai_highlight(
            winner=DummyUser(42, display_name="Winner"),
            category="science",
            answer="gravity",
            update=payload,
            flags=flags,
        )

        self.assertIsNone(without_premium)
        provider.highlight.assert_not_awaited()

        self.bot.premium_service = types.SimpleNamespace(
            guild_has_capability=lambda guild_id, capability: capability == CAPABILITY_QUESTION_DROPS_AI_CELEBRATIONS
        )
        with_premium = await self.service._maybe_ai_highlight(
            winner=DummyUser(42, display_name="Winner"),
            category="science",
            answer="gravity",
            update=payload,
            flags=flags,
        )

        self.assertEqual(with_premium, "Premium celebration line.")
        provider.highlight.assert_awaited_once()

    async def test_selector_avoids_same_day_family_reuse_when_alternatives_exist(self):
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
        self.assertNotEqual(next_variant.family_id, first_variant.family_id)

    async def test_selector_prefers_healthier_logic_bucket_over_recent_boolean_inference_clone(self):
        slot_key = "2026-01-26:3"
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=10, difficulty_profile="smart")
        self.assertTrue(ok, message)
        current_time = datetime(2026, 1, 26, 12, 0, tzinfo=timezone.utc)
        exposures = self._recent_exposures(
            current_time=current_time,
            concept_ids=[
                "geography:landlocked-country",
                "culture:haiku-syllables",
                "science:adaptation-definition",
                "logic:book-order-left-right",
                "history:ghana-independence",
                "logic:true-false",
                "culture:solo-duet-quartet",
                "geography:rain-shadow",
            ],
        )

        candidates = self._category_repeat_candidates_for_slot(slot_key)
        subset = [candidates["logic:not-icy-not-comet"], candidates["logic:bird-owner-paz"]]

        with patch("babblebox.question_drops_service.iter_candidate_variants", return_value=subset), patch(
            "babblebox.question_drops_service.ge.now_utc",
            return_value=current_time,
        ):
            pick = self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=exposures,
                slot_key=slot_key,
                config=self.service.get_config(self.guild.id),
            )

        self.assertIsNotNone(pick)
        self.assertEqual(pick.concept_id, "logic:bird-owner-paz")

    async def test_selector_prefers_healthier_language_bucket_over_recent_grammar_boolean_clone(self):
        slot_key = "2026-01-27:2"
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=10, difficulty_profile="hard")
        self.assertTrue(ok, message)
        current_time = datetime(2026, 1, 27, 12, 0, tzinfo=timezone.utc)
        exposures = self._recent_exposures(
            current_time=current_time,
            concept_ids=[
                "logic:not-icy-not-comet",
                "science:insulator-choice",
                "math:multiplication",
                "geography:cairo-nairobi-capetown",
                "history:zheng-he-ming",
                "language:oxford-comma-boolean",
                "history:order-feudalism-renaissance-enlightenment",
                "logic:mini-deduction",
            ],
        )

        candidates = self._category_repeat_candidates_for_slot(slot_key)
        subset = [candidates["language:semicolon-clauses"], candidates["language:formal-email"]]

        with patch("babblebox.question_drops_service.iter_candidate_variants", return_value=subset), patch(
            "babblebox.question_drops_service.ge.now_utc",
            return_value=current_time,
        ):
            pick = self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=exposures,
                slot_key=slot_key,
                config=self.service.get_config(self.guild.id),
            )

        self.assertIsNotNone(pick)
        self.assertEqual(pick.concept_id, "language:formal-email")

    async def test_selector_prefers_healthier_language_bucket_over_recent_usage_multiple_choice_clone(self):
        slot_key = "2026-01-14:3"
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=10, difficulty_profile="hard")
        self.assertTrue(ok, message)
        current_time = datetime(2026, 1, 14, 12, 0, tzinfo=timezone.utc)
        exposures = self._recent_exposures(
            current_time=current_time,
            concept_ids=[
                "math:clock-angle-three",
                "language:parallel-structure",
                "logic:all-some-rust",
                "culture:lp-speed",
                "geography:monsoon-definition",
                "science:neutral-ph",
            ],
        )

        candidates = self._category_repeat_candidates_for_slot(slot_key)
        subset = [candidates["language:malapropism-definition"], candidates["language:formal-email"]]

        with patch("babblebox.question_drops_service.iter_candidate_variants", return_value=subset), patch(
            "babblebox.question_drops_service.ge.now_utc",
            return_value=current_time,
        ):
            pick = self.service._select_variant(
                self.guild.id,
                self.channel.id,
                exposures=exposures,
                slot_key=slot_key,
                config=self.service.get_config(self.guild.id),
            )

        self.assertIsNotNone(pick)
        self.assertEqual(pick.concept_id, "language:formal-email")

    async def test_selector_keeps_answer_shape_spread_under_repeated_scheduling(self):
        ok, message = await self.service.update_config(self.guild.id, drops_per_day=8, difficulty_profile="smart")
        self.assertTrue(ok, message)
        exposures: list[dict[str, object]] = []
        shapes: list[str] = []
        base = datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc)

        for day in range(10):
            current_time = base + timedelta(days=day)
            with patch("babblebox.question_drops_service.ge.now_utc", return_value=current_time):
                for slot in range(8):
                    slot_key = f"{current_time.date().isoformat()}:{slot}"
                    variant = self.service._select_variant(
                        self.guild.id,
                        self.channel.id,
                        exposures=exposures,
                        slot_key=slot_key,
                        config=self.service.get_config(self.guild.id),
                    )
                    self.assertIsNotNone(variant)
                    shapes.append(str(variant.answer_spec.get("type") or "text"))
                    exposures.insert(
                        0,
                        {
                            "guild_id": self.guild.id,
                            "channel_id": self.channel.id,
                            "concept_id": variant.concept_id,
                            "variant_hash": variant.variant_hash,
                            "category": variant.category,
                            "difficulty": variant.difficulty,
                            "asked_at": (current_time - timedelta(minutes=(8 - slot))).isoformat(),
                            "resolved_at": None,
                            "winner_user_id": None,
                            "slot_key": slot_key,
                        },
                    )

        max_run = 0
        current_run = 0
        previous_shape = None
        for shape in shapes:
            if shape == previous_shape:
                current_run += 1
            else:
                current_run = 1
                previous_shape = shape
            max_run = max(max_run, current_run)

        self.assertGreaterEqual(len(set(shapes)), 4)
        self.assertLessEqual(max_run, 2)

    async def test_selector_profiles_shift_mix_and_avoid_family_spam_over_time(self):
        results: dict[tuple[str, int], dict[str, object]] = {}
        for profile in QUESTION_DROP_DIFFICULTY_PROFILES:
            for drops_per_day in (2, 5, 9, 10):
                with self.subTest(profile=profile, drops_per_day=drops_per_day):
                    result = await self._simulate_selector_mix(profile=profile, drops_per_day=drops_per_day)
                    self.assertEqual(result["family_repeats"], 0)
                    allowed_hard_run = 3 if profile == "hard" and drops_per_day == 10 else 2
                    self.assertLessEqual(result["max_hard_run"], allowed_hard_run)
                    self.assertLessEqual(result["max_shape_run"], 2)
                    self.assertGreater(result["shares"][2], 0.34)
                    self.assertFalse(
                        [event for event in result["category_sub_shape_repeats"] if int(event["gap"]) <= 4],
                        result["category_sub_shape_repeats"],
                    )
                    self.assertFalse(result["known_offender_repeats"], result["known_offender_repeats"])
                    self.assertLessEqual(
                        max(result["category_counts"].values()) - min(result["category_counts"].values()),
                        5,
                    )
                    results[(profile, drops_per_day)] = result

        for drops_per_day in (2, 5, 9, 10):
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
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(44), content=self._correct_attempt_content(active))

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.assertEqual(len(self.service._active_drops), 0)
        message.add_reaction.assert_awaited_once_with("\u2705")
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

    async def test_direct_reply_correct_answer_resolves_and_reacts(self):
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(144),
            content=self._correct_attempt_content(active),
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

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

    async def test_numeric_hedged_guess_still_solves_without_a_reply(self):
        variant = QuestionDropVariant(
            concept_id="math:hedged-sale",
            category="math",
            difficulty=2,
            source_type="generated",
            generator_type="math_percent_change",
            prompt="A $20 item is discounted by 30%. What is the sale price?",
            answer_spec={"type": "numeric", "value": 14},
            variant_hash="hedged-sale",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        await self._post_one_drop()
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(47), content="I think maybe 14?")

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_text_drop_accepts_bot_mention_prefix_reply_noise(self):
        variant = QuestionDropVariant(
            concept_id="science:moon-bot-mention",
            category="science",
            difficulty=1,
            source_type="curated",
            generator_type="static",
            prompt="What is Earth's natural satellite?",
            answer_spec={"type": "text", "accepted": ["moon", "the moon"]},
            variant_hash="science-moon-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(47),
            content=f"<@{self.bot.user.id}> The moon",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_text_drop_accepts_bang_bot_mention_prefix_with_separator(self):
        variant = QuestionDropVariant(
            concept_id="science:moon-bang-bot-mention",
            category="science",
            difficulty=1,
            source_type="curated",
            generator_type="static",
            prompt="What is Earth's natural satellite?",
            answer_spec={"type": "text", "accepted": ["moon", "the moon"]},
            variant_hash="science-moon-bang-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(48),
            content=f"<@!{self.bot.user.id}>: The moon",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_numeric_drop_accepts_bot_mention_prefix_reply_noise(self):
        variant = QuestionDropVariant(
            concept_id="math:remainder-bot-mention",
            category="math",
            difficulty=3,
            source_type="generated",
            generator_type="math_remainder",
            prompt="What is the remainder when 85 is divided by 8?",
            answer_spec={"type": "numeric", "value": 5},
            variant_hash="math-remainder-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(49),
            content=f"<@{self.bot.user.id}>, 5",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_numeric_drop_accepts_bang_bot_mention_prefix_reply_noise(self):
        variant = QuestionDropVariant(
            concept_id="math:remainder-bang-bot-mention",
            category="math",
            difficulty=3,
            source_type="generated",
            generator_type="math_remainder",
            prompt="What is the remainder when 85 is divided by 8?",
            answer_spec={"type": "numeric", "value": 5},
            variant_hash="math-remainder-bang-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(50),
            content=f"<@!{self.bot.user.id}>: 5",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_text_drop_accepts_same_channel_non_reply_with_small_typo(self):
        variant = QuestionDropVariant(
            concept_id="science:planet-mercury",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which planet is closest to the Sun?",
            answer_spec={"type": "text", "accepted": ["Mercury"]},
            variant_hash="science-mercury",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        await self._post_one_drop()
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(47), content="mercuri")

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_text_drop_accepts_same_channel_non_reply_article_variant(self):
        variant = QuestionDropVariant(
            concept_id="logic:cockpit-article-variant",
            category="logic",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="A pilot sits in what section of an airplane?",
            answer_spec={"type": "text", "accepted": ["cockpit", "the cockpit"]},
            variant_hash="logic-cockpit-article-variant",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        await self._post_one_drop()

        handled = await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(52), content="the cockpit")
        )

        self.assertTrue(handled)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_non_bot_mentions_remain_rejected_even_in_reply_format(self):
        variant = QuestionDropVariant(
            concept_id="science:moon-non-bot-mention",
            category="science",
            difficulty=1,
            source_type="curated",
            generator_type="static",
            prompt="What is Earth's natural satellite?",
            answer_spec={"type": "text", "accepted": ["moon", "the moon"]},
            variant_hash="science-moon-non-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(51),
            content="<@!123> The moon",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(message)

        self.assertFalse(handled)
        message.add_reaction.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()
        self.assertEqual(len(self.service._active_drops), 1)

    async def test_multiple_choice_letter_answer_still_solves_without_a_reply(self):
        variant = QuestionDropVariant(
            concept_id="science:mc-react",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which is a chemical change? A) melting ice B) rusting iron C) cutting paper",
            answer_spec={"type": "multiple_choice", "choices": ["melting ice", "rusting iron", "cutting paper"], "answer": "rusting iron"},
            variant_hash="science-mc-react",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(48), content=self._correct_attempt_content(active, prefer_letter=True))

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_first_try_correct_solve_skips_participant_persist_write(self):
        active = await self._post_one_drop()
        self.service.store.update_active_drop_progress = AsyncMock()
        answer = self._correct_attempt_content(active)

        handled = await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(45), content=answer)
        )

        self.assertTrue(handled)
        self.service.store.update_active_drop_progress.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_first_wrong_attempt_persists_participant_state(self):
        active = await self._post_one_drop()
        self.service.store.update_active_drop_progress = AsyncMock()
        author = DummyUser(54)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))

        handled = await self.service.handle_message(wrong)

        self.assertFalse(handled)
        wrong.add_reaction.assert_awaited_once_with("\u274c")
        self.service.store.update_active_drop_progress.assert_awaited_once_with(
            self.guild.id,
            self.channel.id,
            participant_user_ids=[54],
            attempt_counts_by_user={54: 1},
        )

    async def test_direct_reply_wrong_answer_reacts_cleanly(self):
        active = await self._post_one_drop()
        wrong = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(154),
            content=self._wrong_attempt_content(active),
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(wrong)

        self.assertFalse(handled)
        wrong.add_reaction.assert_awaited_once_with("\u274c")

    async def test_wrong_feedback_shows_remaining_attempts_for_three_attempt_drop(self):
        await self.service.update_config(self.guild.id, tone_mode="playful")
        variant = QuestionDropVariant(
            concept_id="science:attempt-copy",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which planet is known as the Red Planet?",
            answer_spec={"type": "text", "accepted": ["mars"]},
            variant_hash="science-attempt-copy",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        author = DummyUser(55)

        await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=author, content="venus")
        )
        await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=author, content="saturn")
        )

        feedback_embeds = [
            item[1]["embed"]
            for item in self.channel.sent
            if "Not Yet" in item[1]["embed"].title or "Out of Attempts" in item[1]["embed"].title
        ]
        exposure_id = int(active["exposure_id"])

        self.assertEqual(self.bot.profile_service.record_question_drop_results_batch.await_count, 0)
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {55: 2})
        self.assertEqual(self.service._attempted_users[exposure_id], {55})
        self.assertEqual(len(feedback_embeds), 2)
        self.assertIn("2 attempts left", feedback_embeds[0].description)
        self.assertIn("1 attempt left", feedback_embeds[1].description)

    async def test_multiple_choice_wrong_once_locks_out_follow_up_correct(self):
        variant = QuestionDropVariant(
            concept_id="science:single-shot-mc",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which is a chemical change? A) melting ice B) rusting iron C) cutting paper",
            answer_spec={"type": "multiple_choice", "choices": ["melting ice", "rusting iron", "cutting paper"], "answer": "rusting iron"},
            variant_hash="science-single-shot-mc",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        author = DummyUser(55)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="option a")
        correct = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="option b")

        first_handled = await self.service.handle_message(wrong)
        sent_count_after_wrong = len(self.channel.sent)
        second_handled = await self.service.handle_message(correct)

        feedback_embed = self.channel.sent[-1][1]["embed"]
        exposure_id = int(active["exposure_id"])
        self.assertFalse(first_handled)
        self.assertFalse(second_handled)
        wrong.add_reaction.assert_awaited_once_with("\u274c")
        correct.add_reaction.assert_not_awaited()
        self.assertEqual(len(self.channel.sent), sent_count_after_wrong)
        self.assertIn("Out of Attempts", feedback_embed.title)
        self.assertIn("out of attempts", feedback_embed.description.lower())
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {55: 1})
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()
        self.assertEqual(len(self.service._active_drops), 1)

    async def test_boolean_wrong_once_locks_out_follow_up_correct(self):
        variant = QuestionDropVariant(
            concept_id="history:true-false-lockout",
            category="history",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="True or false: the U.S. Declaration of Independence came before the U.S. Constitution.",
            answer_spec={"type": "boolean", "value": True},
            variant_hash="history-true-false-lockout",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        author = DummyUser(56)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="false")
        correct = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="true")

        await self.service.handle_message(wrong)
        sent_count_after_wrong = len(self.channel.sent)
        handled = await self.service.handle_message(correct)

        exposure_id = int(active["exposure_id"])
        self.assertFalse(handled)
        wrong.add_reaction.assert_awaited_once_with("\u274c")
        correct.add_reaction.assert_not_awaited()
        self.assertEqual(len(self.channel.sent), sent_count_after_wrong)
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {56: 1})
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

    async def test_text_third_attempt_can_still_solve(self):
        variant = QuestionDropVariant(
            concept_id="science:text-three-attempts",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which planet is known as the Red Planet?",
            answer_spec={"type": "text", "accepted": ["mars"]},
            variant_hash="science-text-three-attempts",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        winner = DummyUser(57)

        await self.service.handle_message(DummyMessage(guild=self.guild, channel=self.channel, author=winner, content="venus"))
        await self.service.handle_message(DummyMessage(guild=self.guild, channel=self.channel, author=winner, content="saturn"))
        correct = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content="mars")

        handled = await self.service.handle_message(correct)

        self.assertTrue(handled)
        correct.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 57,
                    "category": active["category"],
                    "correct": True,
                    "points": answer_points_for_difficulty(int(active["difficulty"])),
                }
            ],
        )

    async def test_ordered_tokens_wrong_first_attempt_locks_out_follow_up_correct(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-one-attempt",
            category="history",
            difficulty=3,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Rome, Renaissance, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["rome", "renaissance", "internet"]},
            variant_hash="history-ordered-one-attempt",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        winner = DummyUser(58)

        wrong_guess = "internet, rome, renaissance"
        first_wrong = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=wrong_guess)
        await self.service.handle_message(first_wrong)
        correct = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=winner,
            content="rome, renaissance, internet",
        )

        handled = await self.service.handle_message(correct)

        exposure_id = int(active["exposure_id"])
        self.assertFalse(handled)
        first_wrong.add_reaction.assert_awaited_once_with("\u274c")
        correct.add_reaction.assert_not_awaited()
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {58: 1})
        self.assertEqual(sum(1 for item in self.channel.sent if "Out of Attempts" in item[1]["embed"].title), 1)
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

    async def test_ordered_tokens_same_channel_multiword_answer_solves_cleanly(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-multiword",
            category="history",
            difficulty=3,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Printing Press, Telephone, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]},
            variant_hash="history-ordered-multiword",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        winner = DummyUser(581)
        answer = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=winner,
            content="printing press, telephone, internet",
        )

        handled = await self.service.handle_message(answer)

        self.assertTrue(handled)
        answer.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 581,
                    "category": active["category"],
                    "correct": True,
                    "points": answer_points_for_difficulty(int(active["difficulty"])),
                }
            ],
        )

    async def test_ordered_tokens_soft_chatter_does_not_count_as_attempt(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-chatter",
            category="history",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Printing Press, Telephone, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]},
            variant_hash="history-ordered-chatter",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        chatter = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(582),
            content="printing press changed everything",
        )

        handled = await self.service.handle_message(chatter)

        self.assertFalse(handled)
        self.assertEqual(self.service._attempt_counts_by_user[int(active["exposure_id"])], {})
        chatter.add_reaction.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

    async def test_numeric_third_wrong_locks_out_follow_up_correct(self):
        variant = QuestionDropVariant(
            concept_id="math:numeric-lockout",
            category="math",
            difficulty=2,
            source_type="generated",
            generator_type="math_percent_change",
            prompt="A $20 item is discounted by 30%. What is the sale price?",
            answer_spec={"type": "numeric", "value": 14},
            variant_hash="math-numeric-lockout",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        author = DummyUser(59)

        for guess in ("15", "16", "17"):
            handled = await self.service.handle_message(
                DummyMessage(guild=self.guild, channel=self.channel, author=author, content=guess)
            )
            self.assertFalse(handled)

        sent_count_after_lockout = len(self.channel.sent)
        correct = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="14")
        handled = await self.service.handle_message(correct)

        exposure_id = int(active["exposure_id"])
        self.assertFalse(handled)
        correct.add_reaction.assert_not_awaited()
        self.assertEqual(len(self.channel.sent), sent_count_after_lockout)
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {59: 3})
        self.assertIn("Out of Attempts", self.channel.sent[-1][1]["embed"].title)
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()
        self.assertEqual(len(self.service._active_drops), 1)

    async def test_restart_restores_attempt_counts_for_active_drop(self):
        variant = QuestionDropVariant(
            concept_id="science:restart-lockout",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which is a chemical change? A) melting ice B) rusting iron C) cutting paper",
            answer_spec={"type": "multiple_choice", "choices": ["melting ice", "rusting iron", "cutting paper"], "answer": "rusting iron"},
            variant_hash="science-restart-lockout",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        author = DummyUser(60)

        await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=author, content="option a")
        )

        self.store.load = AsyncMock()
        restarted = QuestionDropsService(self.bot, store=self.store)
        try:
            self.assertTrue(await restarted.start())
            if restarted._scheduler_task is not None:
                restarted._scheduler_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await restarted._scheduler_task
                restarted._scheduler_task = None

            exposure_id = int(active["exposure_id"])
            self.assertEqual(restarted._attempt_counts_by_user[exposure_id], {60: 1})
            self.assertEqual(restarted._attempted_users[exposure_id], {60})

            correct = DummyMessage(guild=self.guild, channel=self.channel, author=author, content="option b")
            handled = await restarted.handle_message(correct)

            self.assertFalse(handled)
            correct.add_reaction.assert_not_awaited()
            self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()
            self.assertEqual(len(restarted._active_drops), 1)
        finally:
            await restarted.close()

    async def test_missing_reaction_permissions_fail_gracefully(self):
        active = await self._post_one_drop()
        self.channel.can_add_reactions = False
        self.channel.can_read_history = False
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(62), content=self._correct_attempt_content(active))

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_reaction_failure_does_not_break_correct_solve(self):
        active = await self._post_one_drop()
        message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(63), content=self._correct_attempt_content(active))
        message.add_reaction.side_effect = discord.NotFound(types.SimpleNamespace(status=404, reason="missing"), "gone")

        handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()

    async def test_recently_solved_correct_answer_gets_late_ack_only(self):
        active = await self._post_one_drop()
        winner = DummyUser(70)
        runner_up = DummyUser(71)
        self.bot.profile_service.record_question_drop_results_batch = AsyncMock(
            return_value={
                winner.id: {
                    "guild_id": self.guild.id,
                    "user_id": winner.id,
                    "points_awarded": answer_points_for_difficulty(int(active["difficulty"])),
                    "guild_after": {},
                    "guild_category_after": {},
                }
            }
        )
        self.service._grant_progression_rewards = AsyncMock(return_value=[])
        winning_message = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=self._correct_attempt_content(active))
        late_message = DummyMessage(guild=self.guild, channel=self.channel, author=runner_up, content=self._correct_attempt_content(active))

        handled = await self.service.handle_message(winning_message)
        late_handled = await self.service.handle_message(late_message)

        self.assertTrue(handled)
        self.assertFalse(late_handled)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.service._grant_progression_rewards.assert_awaited_once()
        self.assertEqual(self._last_batch_results()[0]["user_id"], winner.id)
        late_message.add_reaction.assert_awaited_once_with("\u2705")
        titles = [item[1]["embed"].title for item in self.channel.sent]
        self.assertEqual(sum(1 for title in titles if "Solved" in title), 1)
        self.assertEqual(sum(1 for title in titles if "Just Late" in title), 1)

    async def test_recently_solved_text_non_reply_gets_late_ack_with_fuzzy_match(self):
        variant = QuestionDropVariant(
            concept_id="science:planet-mercury-late",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which planet is closest to the Sun?",
            answer_spec={"type": "text", "accepted": ["Mercury"]},
            variant_hash="science-mercury-late",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        await self._post_one_drop()
        winner = DummyUser(170)
        runner_up = DummyUser(171)
        self.service._grant_progression_rewards = AsyncMock(return_value=[])
        winning_message = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content="mercury")
        late_message = DummyMessage(guild=self.guild, channel=self.channel, author=runner_up, content="mercuri")

        handled = await self.service.handle_message(winning_message)
        late_handled = await self.service.handle_message(late_message)

        self.assertTrue(handled)
        self.assertFalse(late_handled)
        late_message.add_reaction.assert_awaited_once_with("\u2705")
        titles = [item[1]["embed"].title for item in self.channel.sent]
        self.assertEqual(sum(1 for title in titles if "Solved" in title), 1)
        self.assertEqual(sum(1 for title in titles if "Just Late" in title), 1)

    async def test_late_correct_ack_is_bounded_by_user_and_cap(self):
        active = await self._post_one_drop()
        winner = DummyUser(80)
        late_one = DummyUser(81)
        late_two = DummyUser(82)
        late_three = DummyUser(83)

        await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=self._correct_attempt_content(active))
        )
        first_late = DummyMessage(guild=self.guild, channel=self.channel, author=late_one, content=self._correct_attempt_content(active))
        repeat_late = DummyMessage(guild=self.guild, channel=self.channel, author=late_one, content=self._correct_attempt_content(active))
        second_late = DummyMessage(guild=self.guild, channel=self.channel, author=late_two, content=self._correct_attempt_content(active))
        capped_late = DummyMessage(guild=self.guild, channel=self.channel, author=late_three, content=self._correct_attempt_content(active))

        await self.service.handle_message(first_late)
        await self.service.handle_message(repeat_late)
        await self.service.handle_message(second_late)
        await self.service.handle_message(capped_late)

        titles = [item[1]["embed"].title for item in self.channel.sent]
        self.assertEqual(sum(1 for title in titles if "Just Late" in title), 2)
        repeat_late.add_reaction.assert_not_awaited()
        capped_late.add_reaction.assert_not_awaited()

    async def test_late_correct_ack_accepts_bot_mention_reply_noise(self):
        variant = QuestionDropVariant(
            concept_id="science:moon-late-bot-mention",
            category="science",
            difficulty=1,
            source_type="curated",
            generator_type="static",
            prompt="What is Earth's natural satellite?",
            answer_spec={"type": "text", "accepted": ["moon", "the moon"]},
            variant_hash="science-moon-late-bot-mention",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        winner = DummyUser(84)
        runner_up = DummyUser(85)
        winning_message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=winner,
            content="moon",
            reference=self._reply_reference(active["message_id"]),
        )
        late_message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=runner_up,
            content=f"<@!{self.bot.user.id}>: the moon",
            reference=self._reply_reference(active["message_id"]),
        )

        handled = await self.service.handle_message(winning_message)
        late_handled = await self.service.handle_message(late_message)

        self.assertTrue(handled)
        self.assertFalse(late_handled)
        late_message.add_reaction.assert_awaited_once_with("\u2705")
        titles = [item[1]["embed"].title for item in self.channel.sent]
        self.assertEqual(sum(1 for title in titles if "Solved" in title), 1)
        self.assertEqual(sum(1 for title in titles if "Just Late" in title), 1)

    async def test_late_correct_ack_expires_cleanly(self):
        active = await self._post_one_drop()
        winner = DummyUser(90)
        await self.service.handle_message(
            DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=self._correct_attempt_content(active))
        )
        late = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(91), content=self._correct_attempt_content(active))
        future = ge.now_utc() + timedelta(seconds=9)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=future):
            handled = await self.service.handle_message(late)

        self.assertFalse(handled)
        late.add_reaction.assert_not_awaited()
        self.assertEqual(sum(1 for item in self.channel.sent if "Just Late" in item[1]["embed"].title), 0)

    async def test_near_simultaneous_correct_answers_keep_one_winner_and_one_late_ack(self):
        active = await self._post_one_drop()
        first = DummyUser(92)
        second = DummyUser(93)
        first_message = DummyMessage(guild=self.guild, channel=self.channel, author=first, content=self._correct_attempt_content(active))
        second_message = DummyMessage(guild=self.guild, channel=self.channel, author=second, content=self._correct_attempt_content(active))

        results = await asyncio.gather(
            self.service.handle_message(first_message),
            self.service.handle_message(second_message),
        )

        self.assertEqual(sorted(results), [False, True])
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        titles = [item[1]["embed"].title for item in self.channel.sent]
        self.assertEqual(sum(1 for title in titles if "Solved" in title), 1)
        self.assertEqual(sum(1 for title in titles if "Just Late" in title), 1)
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertIn(exposures[0]["winner_user_id"], {first.id, second.id})

    async def test_concurrent_timeouts_only_announce_and_record_once(self):
        active = await self._post_one_drop()
        author = DummyUser(61)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))
        await self.service.handle_message(wrong)

        await asyncio.gather(
            self.service._expire_drop(active, timed_out=True),
            self.service._expire_drop(active, timed_out=True),
        )

        future = self.service._active_drop_close_after_at(active) + timedelta(seconds=9)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=future):
            await self.service._finalize_due_recent_timeouts(now=future)

        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 1)
        self.assertEqual(len(self.service._active_drops), 0)
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

    async def test_correct_solve_racing_timeout_keeps_winner_and_skips_timeout_copy(self):
        active = await self._post_one_drop()
        winner = DummyUser(556)
        message = DummyMessage(guild=self.guild, channel=self.channel, author=winner, content=self._correct_attempt_content(active))
        real_claim = self.service.store.claim_active_drop_resolution
        timeout_release = asyncio.Event()

        async def coordinated_claim(guild_id: int, channel_id: int, message_id: int, *, resolved_at: datetime, winner_user_id: int | None):
            if winner_user_id is None:
                await timeout_release.wait()
                return await real_claim(
                    guild_id,
                    channel_id,
                    message_id,
                    resolved_at=resolved_at,
                    winner_user_id=winner_user_id,
                )
            claimed = await real_claim(
                guild_id,
                channel_id,
                message_id,
                resolved_at=resolved_at,
                winner_user_id=winner_user_id,
            )
            timeout_release.set()
            return claimed

        self.service.store.claim_active_drop_resolution = AsyncMock(side_effect=coordinated_claim)

        timeout_task = asyncio.create_task(self.service._expire_drop(active, timed_out=True))
        handled = await self.service.handle_message(message)
        await timeout_task

        self.assertTrue(handled)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 1)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertEqual(exposures[0]["winner_user_id"], winner.id)

    async def test_timeout_first_recovery_edits_timeout_result_and_awards_once(self):
        base_now = datetime(2026, 4, 1, 12, 0, 0, 123456, tzinfo=timezone.utc)
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        close_after_at = self.service._active_drop_close_after_at(active)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at):
            await self.service._expire_drop(active, timed_out=True)

        timeout_message = self.channel.sent[-1][2]
        winner = DummyUser(557)
        recovery_message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=winner,
            content=self._correct_attempt_content(active),
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at + timedelta(milliseconds=50)):
            handled = await self.service.handle_message(recovery_message)

        self.assertTrue(handled)
        recovery_message.add_reaction.assert_awaited_once_with("\u2705")
        timeout_message.edit.assert_awaited_once()
        self.assertIn("Corrected Result", timeout_message.kwargs["embed"].title)
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 0)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertEqual(exposures[0]["winner_user_id"], winner.id)

    async def test_timeout_recovery_honors_remaining_attempt_budget(self):
        base_now = datetime(2026, 4, 1, 13, 0, 0, 654321, tzinfo=timezone.utc)
        variant = QuestionDropVariant(
            concept_id="science:timeout-recovery-third-try",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Which planet is known as the Red Planet?",
            answer_spec={"type": "text", "accepted": ["mars"]},
            variant_hash="science-timeout-recovery-third-try",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        close_after_at = self.service._active_drop_close_after_at(active)
        author = DummyUser(558)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=base_now + timedelta(seconds=5)):
            await self.service.handle_message(
                DummyMessage(guild=self.guild, channel=self.channel, author=author, content="venus")
            )
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=base_now + timedelta(seconds=10)):
            await self.service.handle_message(
                DummyMessage(guild=self.guild, channel=self.channel, author=author, content="saturn")
            )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at):
            await self.service._expire_drop(active, timed_out=True)

        timeout_message = self.channel.sent[-1][2]
        recovery_message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=author,
            content="mars",
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at + timedelta(milliseconds=50)):
            handled = await self.service.handle_message(recovery_message)

        self.assertTrue(handled)
        recovery_message.add_reaction.assert_awaited_once_with("\u2705")
        timeout_message.edit.assert_awaited_once()
        self.assertIn("Corrected Result", timeout_message.kwargs["embed"].title)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 558,
                    "category": active["category"],
                    "correct": True,
                    "points": answer_points_for_difficulty(int(active["difficulty"])),
                }
            ],
        )
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertEqual(exposures[0]["winner_user_id"], author.id)

    async def test_timeout_recovery_does_not_bypass_exhausted_attempt_cap(self):
        base_now = datetime(2026, 4, 1, 14, 0, 0, 765432, tzinfo=timezone.utc)
        variant = QuestionDropVariant(
            concept_id="math:timeout-recovery-locked-out",
            category="math",
            difficulty=2,
            source_type="generated",
            generator_type="math_percent_change",
            prompt="A $20 item is discounted by 30%. What is the sale price?",
            answer_spec={"type": "numeric", "value": 14},
            variant_hash="math-timeout-recovery-locked-out",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        close_after_at = self.service._active_drop_close_after_at(active)
        author = DummyUser(559)

        for second_offset, guess in ((5, "15"), (10, "16"), (15, "17")):
            with patch("babblebox.question_drops_service.ge.now_utc", return_value=base_now + timedelta(seconds=second_offset)):
                handled = await self.service.handle_message(
                    DummyMessage(guild=self.guild, channel=self.channel, author=author, content=guess)
                )
            self.assertFalse(handled)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at):
            await self.service._expire_drop(active, timed_out=True)

        timeout_message = self.channel.sent[-1][2]
        recovery_message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=author,
            content="14",
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at + timedelta(milliseconds=50)):
            handled = await self.service.handle_message(recovery_message)

        self.assertFalse(handled)
        recovery_message.add_reaction.assert_not_awaited()
        timeout_message.edit.assert_not_awaited()
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertIsNone(exposures[0]["winner_user_id"])
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

        future = close_after_at + timedelta(seconds=QUESTION_DROP_LATE_CORRECT_WINDOW_SECONDS + 1)
        with patch("babblebox.question_drops_service.ge.now_utc", return_value=future):
            await self.service._finalize_due_recent_timeouts(now=future)

        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(
            self._last_batch_results(),
            [
                {
                    "user_id": 559,
                    "category": active["category"],
                    "correct": False,
                    "points": 0,
                }
            ],
        )
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertIsNone(exposures[0]["winner_user_id"])

    async def test_expiring_drop_records_first_wrong_attempt_once(self):
        active = await self._post_one_drop()
        author = DummyUser(61)
        wrong = DummyMessage(guild=self.guild, channel=self.channel, author=author, content=self._wrong_attempt_content(active))
        await self.service.handle_message(wrong)

        await self.service._expire_drop(active, timed_out=True)
        future = self.service._active_drop_close_after_at(active) + timedelta(seconds=9)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=future):
            await self.service._finalize_due_recent_timeouts(now=future)

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
        self.assertIn(science_role.mention, field_map["Current Roles"])
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
        self.assertEqual(self.service._attempt_counts_by_user[exposure_id], {})
        self.assertEqual(self.service._attempted_users[exposure_id], set())
        chatter.add_reaction.assert_not_awaited()
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()

    async def test_reply_to_another_user_chatter_is_ignored(self):
        active = await self._post_one_drop()
        other_message = DummyMessage(guild=self.guild, channel=self.channel, author=DummyUser(57), content="no clue")
        chatter = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(58),
            content="what",
            reference=self._reply_reference(other_message.id),
        )

        handled = await self.service.handle_message(chatter)

        self.assertFalse(handled)
        self.assertEqual(self.service._attempt_counts_by_user[int(active["exposure_id"])], {})
        self.assertEqual(self.service._attempted_users[int(active["exposure_id"])], set())
        chatter.add_reaction.assert_not_awaited()

    async def test_question_embed_uses_polished_layout(self):
        variant = QuestionDropVariant(
            concept_id="science:polish-layout",
            category="science",
            difficulty=2,
            source_type="curated",
            generator_type="static",
            prompt="Name the planet nicknamed the Red Planet.",
            answer_spec={"type": "text", "accepted": ["mars"]},
            variant_hash="science-polish-layout",
        )
        self.service._select_variant = lambda *args, **kwargs: variant

        await self._post_one_drop()

        embed = self.channel.sent[-1][1]["embed"]
        self.assertTrue(embed.title.endswith("Question Drop"))
        self.assertIn("Science", embed.title)
        self.assertIn("Round", embed.fields[0].name)
        self.assertIn("Difficulty", embed.fields[0].value)
        self.assertIn("Window", embed.fields[0].value)
        self.assertIn("Answering", embed.fields[1].name)
        self.assertIn("3 attempts", embed.fields[1].value)
        self.assertIn("wins", embed.footer.text)

    async def test_ordered_question_embed_uses_one_attempt_copy(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-embed-copy",
            category="history",
            difficulty=3,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Printing Press, Telephone, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]},
            variant_hash="history-ordered-embed-copy",
        )
        self.service._select_variant = lambda *args, **kwargs: variant

        await self._post_one_drop()

        embed = self.channel.sent[-1][1]["embed"]
        self.assertIn("1 attempt", embed.fields[1].value)
        self.assertIn("using commas", embed.fields[1].value)

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
        self.assertIn("Time's Up", self.channel.sent[-1][1]["embed"].title)
        self.assertIn("No in-time solve.", self.channel.sent[-1][1]["embed"].description)

    async def test_answer_after_timeout_gets_too_late_ack(self):
        active = await self._post_one_drop()
        await self.service._expire_drop(active, timed_out=True)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(97),
            content=self._correct_attempt_content(active),
            created_at=self.service._active_drop_expires_at(active) + timedelta(milliseconds=1),
        )

        handled = await self.service.handle_message(message)

        self.assertFalse(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.assertEqual(sum(1 for item in self.channel.sent if "Too Late" in item[1]["embed"].title), 1)

    async def test_message_at_deadline_still_solves_if_processed_late(self):
        active = await self._post_one_drop()
        expires_at = self.service._active_drop_expires_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(98),
            content=self._correct_attempt_content(active),
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=expires_at + timedelta(seconds=1)):
            handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.bot.profile_service.record_question_drop_results_batch.assert_awaited_once()
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 1)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)

    async def test_ordered_multiword_answer_at_deadline_still_solves_if_processed_late(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-late-window",
            category="history",
            difficulty=3,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Printing Press, Telephone, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]},
            variant_hash="history-ordered-late-window",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        active = await self._post_one_drop()
        expires_at = self.service._active_drop_expires_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(583),
            content="printing press, telephone, internet",
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=expires_at + timedelta(seconds=1)):
            handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 1)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)

    async def test_timeout_sweep_at_score_deadline_keeps_exact_deadline_answer_live(self):
        base_now = datetime(2026, 4, 2, 18, 15, 0, 432100, tzinfo=timezone.utc)
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(198),
            content=self._correct_attempt_content(active),
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=expires_at):
            await self.service._expire_due_drops()
            handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 1)

    async def test_ordered_timeout_sweep_at_score_deadline_keeps_exact_deadline_answer_live(self):
        variant = QuestionDropVariant(
            concept_id="history:ordered-timeout-recovery",
            category="history",
            difficulty=3,
            source_type="curated",
            generator_type="static",
            prompt="Order these from earliest to latest: Printing Press, Telephone, Internet",
            answer_spec={"type": "ordered_tokens", "tokens": ["printing press", "telephone", "internet"]},
            variant_hash="history-ordered-timeout-recovery",
        )
        self.service._select_variant = lambda *args, **kwargs: variant
        base_now = datetime(2026, 4, 2, 18, 15, 0, 432100, tzinfo=timezone.utc)
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(584),
            content="printing press, telephone, internet",
            created_at=expires_at,
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=expires_at):
            await self.service._expire_due_drops()
            handled = await self.service.handle_message(message)

        self.assertTrue(handled)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 1)

    async def test_message_after_deadline_does_not_solve_and_closes_once(self):
        active = await self._post_one_drop()
        expires_at = self.service._active_drop_expires_at(active)
        close_after_at = self.service._active_drop_close_after_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(99),
            content=self._correct_attempt_content(active),
            created_at=expires_at + timedelta(microseconds=1),
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at + timedelta(seconds=1)):
            handled = await self.service.handle_message(message)

        self.assertFalse(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.bot.profile_service.record_question_drop_results_batch.assert_not_awaited()
        self.assertEqual(sum(1 for item in self.channel.sent if "Solved" in item[1]["embed"].title), 0)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 1)
        self.assertEqual(sum(1 for item in self.channel.sent if "Too Late" in item[1]["embed"].title), 1)
        exposures = await self.store.list_exposures_for_guild(self.guild.id)
        self.assertIsNone(exposures[0]["winner_user_id"])

    async def test_message_just_after_deadline_gets_too_late_ack_before_timeout_copy(self):
        base_now = datetime(2026, 4, 1, 15, 30, 0, 987654, tzinfo=timezone.utc)
        active = await self._post_one_drop(now=base_now, round_to_minute=False)
        expires_at = self.service._active_drop_expires_at(active)
        close_after_at = self.service._active_drop_close_after_at(active)
        message = DummyMessage(
            guild=self.guild,
            channel=self.channel,
            author=DummyUser(199),
            content=self._correct_attempt_content(active),
            created_at=expires_at + timedelta(milliseconds=1),
        )

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=expires_at + timedelta(milliseconds=500)):
            handled = await self.service.handle_message(message)

        self.assertFalse(handled)
        message.add_reaction.assert_awaited_once_with("\u2705")
        self.assertEqual(sum(1 for item in self.channel.sent if "Too Late" in item[1]["embed"].title), 1)
        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 0)
        self.assertIn((self.guild.id, self.channel.id), self.service._active_drops)

        with patch("babblebox.question_drops_service.ge.now_utc", return_value=close_after_at + timedelta(milliseconds=1)):
            await self.service._expire_due_drops()

        self.assertEqual(sum(1 for item in self.channel.sent if "Time's Up" in item[1]["embed"].title), 1)

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
