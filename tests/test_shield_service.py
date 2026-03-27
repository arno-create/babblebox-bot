import types
import unittest
from copy import deepcopy
from typing import Optional
from unittest.mock import AsyncMock, patch

from babblebox.shield_ai import SHIELD_AI_ALLOWED_GUILD_ID, ShieldAIReviewResult
from babblebox.shield_service import ShieldDecision, ShieldMatch, ShieldService
from babblebox.shield_store import ShieldStateStore, _MemoryShieldStore


class FakeRole:
    def __init__(self, role_id: int, *, position: int = 1):
        self.id = role_id
        self.position = position
        self.mention = f"<@&{role_id}>"

    def __ge__(self, other):
        return self.position >= getattr(other, "position", 0)


class FakeGuildPermissions:
    def __init__(self, *, administrator: bool = False, manage_guild: bool = False):
        self.administrator = administrator
        self.manage_guild = manage_guild


class FakeAuthor:
    def __init__(self, user_id: int, *, roles=None, administrator: bool = False, bot: bool = False):
        self.id = user_id
        self.bot = bot
        self.roles = roles or []
        self.mention = f"<@{user_id}>"
        self.display_name = f"User {user_id}"
        self.guild_permissions = FakeGuildPermissions(administrator=administrator)


class FakeBotMember(FakeAuthor):
    def __init__(self, user_id: int = 999):
        super().__init__(user_id, roles=[FakeRole(900, position=100)])
        self.top_role = self.roles[0]


class FakePermissions:
    manage_messages = True
    moderate_members = True
    send_messages = True
    embed_links = True


class FakeChannel:
    def __init__(self, channel_id: int = 20, *, name: str = "general"):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self.sent = []

    def permissions_for(self, member):
        return FakePermissions()

    async def send(self, **kwargs):
        self.sent.append(kwargs)


class FakeGuild:
    def __init__(self, guild_id: int = 10):
        self.id = guild_id
        self.me = FakeBotMember()

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return None


class FakeAttachment:
    def __init__(self, filename: str):
        self.filename = filename
        self.url = f"https://cdn.discordapp.com/{filename}"
        self.content_type = None


class FakeMessage:
    _next_id = 1000

    def __init__(self, *, guild, channel, author, content: str, attachments=None):
        self.guild = guild
        self.channel = channel
        self.author = author
        self.content = content
        self.attachments = attachments or []
        self.webhook_id = None
        self.id = FakeMessage._next_id
        FakeMessage._next_id += 1
        self.jump_url = f"https://discord.com/channels/{guild.id}/{channel.id}/{self.id}"
        self.deleted = False

    async def delete(self):
        self.deleted = True


class FakeBot:
    def __init__(self):
        self.user = types.SimpleNamespace(id=999)
        self._channels = {}

    def register_channel(self, channel):
        self._channels[channel.id] = channel

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    async def fetch_channel(self, channel_id: int):
        return self._channels.get(channel_id)


class FakeAIProvider:
    def __init__(self, *, available: bool = True, result: Optional[ShieldAIReviewResult] = None):
        self._available = available
        self._result = result
        self.review = AsyncMock(return_value=result)

    def diagnostics(self):
        return {
            "provider": "OpenAI",
            "available": self._available,
            "configured": self._available,
            "model": "gpt-4.1-mini" if self._available else None,
            "timeout_seconds": 4.0,
            "max_chars": 160,
            "status": "Ready." if self._available else "OpenAI API key is not configured.",
        }

    async def close(self):
        return None


class ShieldServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = FakeBot()
        self.store = ShieldStateStore(backend="memory")
        self.service = ShieldService(self.bot, store=self.store)
        started = await self.service.start()
        self.assertTrue(started)

    async def test_privacy_pack_matches_email(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Email me at friend@example.com")

        self.assertTrue(matches)
        self.assertEqual(matches[0].pack, "privacy")
        self.assertIn("email", matches[0].label.lower())

    async def test_privacy_high_sensitivity_does_not_flag_random_long_number_as_phone(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Build number 1234567890 finished successfully.")

        self.assertFalse([match for match in matches if match.pack == "privacy" and "phone" in match.label.lower()])

    async def test_privacy_high_sensitivity_still_detects_phone_with_contact_context(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Call me at +1 (555) 123-4567 when you are free.")

        self.assertTrue([match for match in matches if match.pack == "privacy" and "phone" in match.label.lower()])

    async def test_privacy_card_requires_more_than_a_raw_number_on_high(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Reference 4111 1111 1111 1111 for the test fixture.")

        self.assertFalse([match for match in matches if match.pack == "privacy" and "payment" in match.label.lower()])

    async def test_privacy_card_with_payment_context_still_detects(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Card 4111 1111 1111 1111 expires next month.")

        self.assertTrue([match for match in matches if match.pack == "privacy" and "payment" in match.label.lower()])

    async def test_privacy_ip_inside_link_does_not_overtrigger(self):
        ok, _ = await self.service.set_pack_config(10, "privacy", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Local docs run at http://127.0.0.1:8000/health during development.")

        self.assertFalse([match for match in matches if match.pack == "privacy" and "ip" in match.label.lower()])

    async def test_legacy_nested_pack_shape_is_respected_by_service_and_compiled_cache(self):
        self.service.store.state["guilds"]["10"] = {
            "module_enabled": True,
            "packs": {
                "privacy": {"enabled": True, "action": "log", "sensitivity": "high"},
                "promo": {"tracking": True, "action": "delete_log", "sensitivity": "normal"},
                "scam": {"enabled": False, "action": "log", "sensitivity": "normal"},
            },
            "ai_enabled": True,
            "ai_enabled_packs": ["privacy", "promo"],
        }

        config = self.service.get_config(10)
        self.assertTrue(config["privacy_enabled"])
        self.assertTrue(config["promo_enabled"])
        self.assertEqual(config["privacy_sensitivity"], "high")
        self.assertEqual(config["promo_action"], "delete_log")
        self.assertEqual(config["promo_low_action"], "log")
        self.assertEqual(config["promo_medium_action"], "delete_log")
        self.assertEqual(config["promo_high_action"], "delete_log")

        self.service._rebuild_config_cache()
        compiled = self.service._compiled_configs[10]
        self.assertTrue(compiled.privacy.enabled)
        self.assertTrue(compiled.promo.enabled)
        self.assertEqual(compiled.privacy.sensitivity, "high")
        self.assertEqual(compiled.promo.low_action, "log")
        self.assertEqual(compiled.promo.medium_action, "delete_log")
        self.assertEqual(compiled.promo.high_action, "delete_log")

    async def test_allowlisted_invite_suppresses_promo_match(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_invite_codes", "abc123", True)
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Join us here https://discord.gg/abc123")

        self.assertFalse([match for match in matches if match.pack == "promo"])

    async def test_promo_high_sensitivity_skips_generic_info_link(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Check out the docs at https://example.com/guide for setup details.")

        self.assertFalse([match for match in matches if match.pack == "promo"])

    async def test_promo_invite_still_detects_without_allowlist(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Join us here https://discord.gg/notallowed")

        self.assertTrue([match for match in matches if match.pack == "promo"])

    async def test_pack_policy_validation_rejects_unsafe_low_confidence_action(self):
        ok, message = await self.service.set_pack_config(10, "promo", enabled=True, low_action="delete_log")

        self.assertFalse(ok)
        self.assertIn("low-confidence", message.lower())

    async def test_pack_policy_explicit_actions_are_persisted(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "promo",
            enabled=True,
            low_action="log",
            medium_action="delete_log",
            high_action="delete_escalate",
            sensitivity="high",
        )
        self.assertTrue(ok)

        config = self.service.get_config(10)
        compiled = self.service._compiled_configs[10]

        self.assertEqual((config["promo_low_action"], config["promo_medium_action"], config["promo_high_action"]), ("log", "delete_log", "delete_escalate"))
        self.assertEqual((compiled.promo.low_action, compiled.promo.medium_action, compiled.promo.high_action), ("log", "delete_log", "delete_escalate"))

    async def test_repeated_tenor_link_stays_low_confidence_noise(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(
            guild.id,
            "promo",
            enabled=True,
            low_action="log",
            medium_action="delete_log",
            high_action="delete_escalate",
            sensitivity="high",
        )
        self.assertTrue(ok)

        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for _ in range(5):
                decisions.append(
                    await self.service.handle_message(
                        FakeMessage(
                            guild=guild,
                            channel=channel,
                            author=author,
                            content="https://tenor.com/view/cat-dance-gif-12345",
                        )
                    )
                )

        self.assertEqual(decisions[:-1], [None, None, None, None])
        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.action, "log")
        self.assertFalse(final.deleted)
        self.assertEqual(final.reasons[0].match_class, "repetitive_link_noise")
        self.assertEqual(final.reasons[0].confidence, "low")

    async def test_repeated_generic_link_stays_log_only_noise(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(
            guild.id,
            "promo",
            enabled=True,
            low_action="log",
            medium_action="delete_log",
            high_action="delete_escalate",
            sensitivity="normal",
        )
        self.assertTrue(ok)

        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for _ in range(4):
                decisions.append(
                    await self.service.handle_message(
                        FakeMessage(
                            guild=guild,
                            channel=channel,
                            author=author,
                            content="https://example.com/docs/guide",
                        )
                    )
                )

        self.assertEqual(decisions[:-1], [None, None, None])
        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.action, "log")
        self.assertFalse(final.deleted)
        self.assertEqual(final.reasons[0].match_class, "repetitive_link_noise")

    async def test_allowlisted_invite_repetition_stays_suppressed(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(guild.id, "allow_invite_codes", "abc123", True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://discord.gg/abc123")
                )
                for _ in range(5)
            ]

        self.assertEqual(decisions, [None, None, None, None, None])

    async def test_repeated_invite_spam_can_reach_high_confidence_policy(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(
            guild.id,
            "promo",
            enabled=True,
            low_action="log",
            medium_action="delete_log",
            high_action="delete_escalate",
            sensitivity="normal",
        )
        self.assertTrue(ok)
        ok, _ = await self.service.set_escalation(guild.id, threshold=3, window_minutes=10, timeout_minutes=5)
        self.assertTrue(ok)

        with patch.object(self.service, "_timeout_member", new=AsyncMock(return_value=True)) as timeout_mock, patch.object(
            self.service,
            "_send_alert",
            new=AsyncMock(),
        ):
            first = await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="https://discord.gg/notallowed"))
            second = await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="https://discord.gg/notallowed"))
            third = await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="https://discord.gg/notallowed"))

        self.assertEqual(first.action, "delete_log")
        self.assertEqual(second.action, "delete_log")
        self.assertEqual(third.action, "delete_escalate")
        self.assertEqual(third.reasons[0].match_class, "discord_invite")
        self.assertTrue(third.deleted)
        self.assertFalse(third.timed_out)
        timeout_mock.assert_not_awaited()

    async def test_custom_wildcard_pattern_matches_safely(self):
        ok, _ = await self.service.add_custom_pattern(
            10,
            label="Gift bait",
            pattern="claim*gift",
            mode="wildcard",
            action="log",
        )
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Please claim your gift now")

        self.assertTrue(matches)
        self.assertEqual(matches[0].pack, "advanced")

    async def test_scam_warning_message_is_not_flagged(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Warning: fake free nitro link here, do not click https://bit.ly/bait")

        self.assertFalse([match for match in matches if match.pack == "scam"])

    async def test_trusted_role_bypass_skips_live_scan(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        trusted_role = FakeRole(77, position=10)
        author = FakeAuthor(42, roles=[trusted_role])
        message = FakeMessage(guild=guild, channel=channel, author=author, content="Join https://discord.gg/not-allowed")

        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_filter_target(10, "trusted_role_ids", trusted_role.id, True)
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNone(decision)
        self.assertFalse(message.deleted)

    async def test_delete_escalate_triggers_timeout_after_threshold(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message_one = FakeMessage(guild=guild, channel=channel, author=author, content="Free nitro claim now https://bit.ly/bait")
        message_two = FakeMessage(guild=guild, channel=channel, author=author, content="Free nitro claim now https://bit.ly/bait2")

        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_escalate", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_escalation(10, threshold=2, window_minutes=10, timeout_minutes=5)
        self.assertTrue(ok)

        with patch.object(self.service, "_timeout_member", new=AsyncMock(return_value=True)) as timeout_mock, patch.object(
            self.service,
            "_send_alert",
            new=AsyncMock(),
        ) as alert_mock:
            first = await self.service.handle_message(message_one)
            second = await self.service.handle_message(message_two)

        self.assertTrue(first.deleted)
        self.assertFalse(first.timed_out)
        self.assertTrue(second.deleted)
        self.assertTrue(second.timed_out)
        self.assertTrue(second.escalated)
        self.assertEqual(timeout_mock.await_count, 1)
        self.assertEqual(alert_mock.await_count, 2)

    async def test_alert_embed_is_readable_and_deduped(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")

        ok, _ = await self.service.set_log_channel(10, log_channel.id)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[10]
        decision = ShieldDecision(
            matched=True,
            action="log",
            pack="privacy",
            reasons=(
                ShieldMatch(
                    pack="privacy",
                    label="Possible email address",
                    reason="Looks like an email address was posted in chat.",
                    action="log",
                    confidence="high",
                    heuristic=False,
                    match_class="privacy_email",
                ),
            ),
        )

        await self.service._send_alert(message, compiled, decision)
        await self.service._send_alert(message, compiled, decision)

        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Shield Alert | Privacy Leak")
        self.assertIn("Possible email address", embed.fields[0].value)
        self.assertIn("Class:", embed.fields[0].value)
        self.assertIn("Resolved action:", embed.fields[0].value)

    async def test_ai_config_is_restricted_to_allowed_guild(self):
        ok, message = await self.service.set_ai_config(10, enabled=True)

        self.assertFalse(ok)
        self.assertIn("not available", message.lower())

    async def test_ai_config_cannot_enable_without_provider(self):
        self.service.ai_provider = FakeAIProvider(available=False)

        ok, message = await self.service.set_ai_config(SHIELD_AI_ALLOWED_GUILD_ID, enabled=True)

        self.assertFalse(ok)
        self.assertIn("provider", message.lower())

    async def test_allowed_guild_flagged_message_can_enrich_alert_with_ai_review(self):
        guild = FakeGuild(SHIELD_AI_ALLOWED_GUILD_ID)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")
        self.service.ai_provider = FakeAIProvider(
            result=ShieldAIReviewResult(
                classification="privacy_leak",
                confidence="high",
                priority="high",
                false_positive=False,
                explanation="Likely a real contact detail rather than harmless chatter.",
                model="gpt-4.1-mini",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=False)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=True, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.ai_review)
        self.service.ai_provider.review.assert_awaited_once()
        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        ai_field = next(field for field in embed.fields if field.name == "AI Assist")
        self.assertIn("Likely privacy leak", ai_field.value)
        self.assertIn("gpt-4.1-mini", ai_field.value)

    async def test_non_allowed_guild_never_calls_ai_provider(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")
        self.service.ai_provider = FakeAIProvider(
            result=ShieldAIReviewResult(
                classification="privacy_leak",
                confidence="high",
                priority="high",
                false_positive=False,
                explanation="Likely a real contact detail rather than harmless chatter.",
                model="gpt-4.1-mini",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNone(decision.ai_review)
        self.service.ai_provider.review.assert_not_awaited()

    async def test_noise_match_never_requests_ai_review(self):
        guild = FakeGuild(SHIELD_AI_ALLOWED_GUILD_ID)
        channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        self.service.ai_provider = FakeAIProvider(
            result=ShieldAIReviewResult(
                classification="false_positive",
                confidence="medium",
                priority="low",
                false_positive=True,
                explanation="Looks repetitive, but not like actual promotion.",
                model="gpt-4.1-mini",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=True, enabled_packs=["promo"], min_confidence="low")
        self.assertTrue(ok)

        decisions = []
        for _ in range(4):
            decisions.append(
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://example.com/docs/guide")
                )
            )

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "repetitive_link_noise")
        self.assertIsNone(final.ai_review)
        self.service.ai_provider.review.assert_not_awaited()

    async def test_global_ai_override_allows_non_support_guild(self):
        self.service.ai_provider = FakeAIProvider(available=True)

        ok, message = await self.service.set_ai_config(10, enabled=True)
        self.assertFalse(ok)
        self.assertIn("not available", message.lower())

        ok, message = await self.service.set_global_ai_override(True, actor_id=1266444952779620413)
        self.assertTrue(ok)
        self.assertIn("now on", message.lower())
        self.assertTrue(self.service.is_ai_supported_guild(10))

        ok, message = await self.service.set_ai_config(10, enabled=True, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)
        self.assertIn("enabled", message.lower())

    async def test_ai_review_failure_does_not_block_local_alert(self):
        guild = FakeGuild(SHIELD_AI_ALLOWED_GUILD_ID)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")
        self.service.ai_provider = FakeAIProvider(result=None)

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=False)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=True, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNone(decision.ai_review)
        self.assertEqual(len(log_channel.sent), 1)


class ShieldStoreNormalizationTests(unittest.TestCase):
    def test_normalize_state_migrates_legacy_nested_pack_config(self):
        store = _MemoryShieldStore()
        legacy = {
            "version": 1,
            "guilds": {
                "123": {
                    "module_enabled": True,
                    "packs": {
                        "privacy": {"enabled": True, "action": "log", "sensitivity": "high"},
                        "promo": {"tracking": True, "action": "delete_log", "sensitivity": "normal"},
                    },
                }
            },
        }

        normalized = store.normalize_state(legacy)
        config = normalized["guilds"]["123"]
        self.assertTrue(config["privacy_enabled"])
        self.assertTrue(config["promo_enabled"])
        self.assertEqual(config["privacy_sensitivity"], "high")
        self.assertEqual(config["promo_action"], "delete_log")
        self.assertEqual(config["promo_low_action"], "log")
        self.assertEqual(config["promo_medium_action"], "delete_log")
        self.assertEqual(config["promo_high_action"], "delete_log")

    def test_normalize_state_preserves_global_ai_override_meta(self):
        store = _MemoryShieldStore()
        snapshot = {
            "version": 2,
            "meta": {
                "global_ai_override_enabled": True,
                "global_ai_override_updated_by": 1266444952779620413,
                "global_ai_override_updated_at": "2026-03-27T10:00:00+00:00",
            },
            "guilds": {},
        }

        normalized = store.normalize_state(deepcopy(snapshot))

        self.assertTrue(normalized["meta"]["global_ai_override_enabled"])
        self.assertEqual(normalized["meta"]["global_ai_override_updated_by"], 1266444952779620413)
        self.assertEqual(normalized["meta"]["global_ai_override_updated_at"], "2026-03-27T10:00:00+00:00")
