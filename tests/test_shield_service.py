import types
import unittest
from typing import Optional
from unittest.mock import AsyncMock, patch

from babblebox.shield_ai import SHIELD_AI_ALLOWED_GUILD_ID, ShieldAIReviewResult
from babblebox.shield_service import ShieldDecision, ShieldMatch, ShieldService
from babblebox.shield_store import ShieldStateStore


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

    async def test_allowlisted_invite_suppresses_promo_match(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_invite_codes", "abc123", True)
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Join us here https://discord.gg/abc123")

        self.assertFalse([match for match in matches if match.pack == "promo"])

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
                ),
            ),
        )

        await self.service._send_alert(message, compiled, decision)
        await self.service._send_alert(message, compiled, decision)

        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Shield Alert | Privacy Leak")
        self.assertIn("Possible email address", embed.fields[0].value)

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
