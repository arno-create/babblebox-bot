import json
import types
import unittest
from datetime import timedelta
from copy import deepcopy
from typing import Optional
from unittest.mock import AsyncMock, patch

from babblebox import game_engine as ge

from babblebox.shield_ai import SHIELD_AI_ALLOWED_GUILD_ID, ShieldAIReviewResult
from babblebox.shield_service import (
    ShieldDecision,
    ShieldMatch,
    ShieldService,
    _alert_content_fingerprint,
    _build_snapshot,
    _campaign_kind_label,
)
from babblebox.shield_store import SHIELD_META_GLOBAL_AI_OVERRIDE_KEY, ShieldStateStore, _MemoryShieldStore, _PostgresShieldStore


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
    def __init__(
        self,
        user_id: int,
        *,
        roles=None,
        administrator: bool = False,
        bot: bool = False,
        created_at=None,
        joined_at=None,
        avatar=None,
        display_name: Optional[str] = None,
    ):
        self.id = user_id
        self.bot = bot
        self.roles = roles or []
        self.mention = f"<@{user_id}>"
        self.display_name = display_name or f"User {user_id}"
        self.guild_permissions = FakeGuildPermissions(administrator=administrator)
        self.created_at = created_at or ge.now_utc()
        self.joined_at = joined_at or ge.now_utc()
        self.avatar = avatar
        self.default_avatar = object()


class FakeBotMember(FakeAuthor):
    def __init__(self, user_id: int = 999):
        super().__init__(user_id, roles=[FakeRole(900, position=100)])
        self.top_role = self.roles[0]


class FakePermissions:
    def __init__(
        self,
        *,
        manage_messages: bool = True,
        moderate_members: bool = True,
        send_messages: bool = True,
        embed_links: bool = True,
        view_channel: bool = True,
    ):
        self.manage_messages = manage_messages
        self.moderate_members = moderate_members
        self.send_messages = send_messages
        self.embed_links = embed_links
        self.view_channel = view_channel


class FakeChannel:
    def __init__(self, channel_id: int = 20, *, name: str = "general", permissions: Optional[FakePermissions] = None):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self._permissions = permissions or FakePermissions()

    def permissions_for(self, member):
        return self._permissions

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
    def __init__(self, filename: str, *, description: Optional[str] = None, title: Optional[str] = None):
        self.filename = filename
        self.url = f"https://cdn.discordapp.com/{filename}"
        self.content_type = None
        self.description = description
        self.title = title


class FakeEmbedField:
    def __init__(self, name: str, value: str):
        self.name = name
        self.value = value


class FakeEmbed:
    def __init__(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        url: Optional[str] = None,
        fields: Optional[list[FakeEmbedField]] = None,
    ):
        self.title = title
        self.description = description
        self.url = url
        self.fields = fields or []
        self.footer = types.SimpleNamespace(text=None)
        self.author = types.SimpleNamespace(name=None, url=None)
        self.image = types.SimpleNamespace(url=None)
        self.thumbnail = types.SimpleNamespace(url=None)


class FakeMessageSnapshot:
    def __init__(self, *, content: str = "", embeds=None, attachments=None):
        self.content = content
        self.embeds = embeds or []
        self.attachments = attachments or []


class FakeMessage:
    _next_id = 1000

    def __init__(
        self,
        *,
        guild,
        channel,
        author,
        content: str,
        attachments=None,
        embeds=None,
        webhook_id=None,
        message_snapshots=None,
        system_content: Optional[str] = None,
        message_id: Optional[int] = None,
    ):
        self.guild = guild
        self.channel = channel
        self.author = author
        self.content = content
        self.attachments = attachments or []
        self.embeds = embeds or []
        self.webhook_id = webhook_id
        self.message_snapshots = message_snapshots or []
        self.system_content = system_content or content
        self.id = FakeMessage._next_id if message_id is None else message_id
        if message_id is None:
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


class _FakeShieldAcquireContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeShieldPool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeShieldAcquireContext(self.connection)


class _FakeShieldReloadConnection:
    def __init__(self, *, config_rows=None, pattern_rows=None, meta_rows=None):
        self._config_rows = config_rows or []
        self._pattern_rows = pattern_rows or []
        self._meta_rows = meta_rows or []

    async def fetch(self, query):
        if "FROM shield_guild_configs" in query:
            return self._config_rows
        if "FROM shield_custom_patterns" in query:
            return self._pattern_rows
        if "FROM shield_meta" in query:
            return self._meta_rows
        raise AssertionError(f"Unexpected fetch query: {query}")


class ShieldPostgresReloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_reload_decodes_json_string_config_lists_and_meta(self):
        connection = _FakeShieldReloadConnection(
            config_rows=[
                {
                    "guild_id": 10,
                    "module_enabled": True,
                    "log_channel_id": 50,
                    "alert_role_id": 60,
                    "scan_mode": "all",
                    "included_channel_ids": json.dumps([20, 21]),
                    "excluded_channel_ids": json.dumps([22]),
                    "included_user_ids": json.dumps([30]),
                    "excluded_user_ids": json.dumps([31]),
                    "included_role_ids": json.dumps([40]),
                    "excluded_role_ids": json.dumps([41]),
                    "trusted_role_ids": json.dumps([42, 42]),
                    "allow_domains": json.dumps(["example.com"]),
                    "allow_invite_codes": json.dumps(["abc123"]),
                    "allow_phrases": json.dumps(["friendly server"]),
                    "privacy_enabled": True,
                    "privacy_action": "log",
                    "privacy_low_action": "log",
                    "privacy_medium_action": "log",
                    "privacy_high_action": "log",
                    "privacy_sensitivity": "normal",
                    "promo_enabled": True,
                    "promo_action": "delete_log",
                    "promo_low_action": "log",
                    "promo_medium_action": "delete_log",
                    "promo_high_action": "delete_log",
                    "promo_sensitivity": "high",
                    "scam_enabled": True,
                    "scam_action": "delete_escalate",
                    "scam_low_action": "log",
                    "scam_medium_action": "delete_log",
                    "scam_high_action": "delete_escalate",
                    "scam_sensitivity": "high",
                    "adult_enabled": True,
                    "adult_action": "delete_log",
                    "adult_low_action": "log",
                    "adult_medium_action": "delete_log",
                    "adult_high_action": "delete_log",
                    "adult_sensitivity": "normal",
                    "ai_enabled": True,
                    "ai_min_confidence": "medium",
                    "ai_enabled_packs": json.dumps(["privacy", "promo"]),
                    "escalation_threshold": 3,
                    "escalation_window_minutes": 15,
                    "timeout_minutes": 10,
                }
            ],
            meta_rows=[
                {
                    "key": SHIELD_META_GLOBAL_AI_OVERRIDE_KEY,
                    "value": json.dumps(
                        {
                            "enabled": True,
                            "updated_by": 77,
                            "updated_at": "2026-04-02T00:00:00+00:00",
                        }
                    ),
                }
            ],
        )
        store = _PostgresShieldStore("postgresql://shield-user:secret@db.example.com:5432/app")
        store._pool = _FakeShieldPool(connection)

        await store._reload_from_db()

        config = store.state["guilds"]["10"]
        meta = store.state["meta"]
        self.assertEqual(config["included_channel_ids"], [20, 21])
        self.assertEqual(config["excluded_channel_ids"], [22])
        self.assertEqual(config["included_user_ids"], [30])
        self.assertEqual(config["excluded_user_ids"], [31])
        self.assertEqual(config["trusted_role_ids"], [42])
        self.assertEqual(config["allow_domains"], ["example.com"])
        self.assertEqual(config["allow_invite_codes"], ["abc123"])
        self.assertEqual(config["allow_phrases"], ["friendly server"])
        self.assertEqual(config["ai_enabled_packs"], ["privacy", "promo"])
        self.assertTrue(meta["global_ai_override_enabled"])
        self.assertEqual(meta["global_ai_override_updated_by"], 77)


class ShieldServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = FakeBot()
        self.store = ShieldStateStore(backend="memory")
        self.service = ShieldService(self.bot, store=self.store)
        started = await self.service.start()
        self.assertTrue(started)

    def _content_fingerprint(self, content: str, *, attachments=None) -> str:
        return _alert_content_fingerprint(_build_snapshot(content, attachments))

    def test_campaign_kind_labels_stay_operator_friendly(self):
        self.assertEqual(_campaign_kind_label("path_shape"), "shared risky link shape")
        self.assertEqual(_campaign_kind_label("host_family"), "shared risky host pattern")
        self.assertEqual(_campaign_kind_label("lure"), "reused lure wording")

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
        self.assertFalse(config["adult_enabled"])

        self.service._rebuild_config_cache()
        compiled = self.service._compiled_configs[10]
        self.assertTrue(compiled.privacy.enabled)
        self.assertTrue(compiled.promo.enabled)
        self.assertEqual(compiled.privacy.sensitivity, "high")
        self.assertEqual(compiled.promo.low_action, "log")
        self.assertEqual(compiled.promo.medium_action, "delete_log")
        self.assertEqual(compiled.promo.high_action, "delete_log")
        self.assertFalse(compiled.adult.enabled)

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

    async def test_known_safe_domain_family_stays_safe_and_does_not_trigger_scam(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Claim now https://youtube.com/watch?v=abc123 right away")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].normalized_domain, "youtube.com")
        self.assertEqual(result.link_assessments[0].category, "safe")
        self.assertIn("safe_family:social", result.link_assessments[0].matched_signals)

    async def test_safe_family_includes_discord_status_and_google_docs(self):
        result = self.service.test_message_details(
            10,
            "Status https://discordstatus.com and docs https://docs.google.com/document/d/abc123/edit",
        )

        categories = {item.normalized_domain: item.category for item in result.link_assessments}
        self.assertEqual(categories["discordstatus.com"], "safe")
        self.assertEqual(categories["docs.google.com"], "safe")

    async def test_official_opensea_domain_stays_safe_even_with_mint_language(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Official OpenSea mint details are live now: https://opensea.io/events",
        )

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "safe")

    async def test_safe_mainstream_link_with_scary_words_but_helpful_context_stays_safe(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Discord verify outage update: use https://discordstatus.com for the real status page before you log in again.",
        )

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "safe")

    async def test_mixed_safe_and_suspicious_links_keep_primary_risky_domain(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(
            142,
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
        )
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content=(
                "Official note: read the docs at https://docs.google.com/document/d/abc123/edit "
                "and verify your slot at https://mint-pass.live/opensea before the window closes."
            ),
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        categories = {item.normalized_domain: item.category for item in decision.link_assessments}
        self.assertEqual(categories["docs.google.com"], "safe")
        self.assertEqual(categories["mint-pass.live"], "unknown_suspicious")
        self.assertEqual(decision.member_risk_evidence.primary_domain, "mint-pass.live")

    async def test_polished_opensea_mint_lure_hits_scam_pack(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            (
                "We are excited to announce a new, free Mint opportunity in partnership with OpenSea! "
                "Members of this server are invited to participate. To secure your spot, please visit "
                "the official minting page: https://opensea-mint-event.com/claim. "
                "We encourage you to participate soon, as selection is limited."
            ),
        )

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")

    async def test_live_tld_mint_lure_hits_scam_pack(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            (
                "Community post: Members are invited to secure their spot in the official mint. "
                "Visit https://mint-pass.live/opensea soon because selection is limited."
            ),
        )

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")

    async def test_unknown_suspicious_link_with_weak_copy_stays_link_only(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Reference guide mirror: https://secure-auth-session.click/guide",
        )

        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertFalse([match for match in result.matches if match.pack == "scam"])

    async def test_unknown_suspicious_link_can_emit_low_confidence_risky_unknown_link_lure(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Community update: https://verify-hub.live/news",
        )

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_risky_unknown_link")
        self.assertEqual(scam_matches[0].confidence, "low")
        self.assertEqual(scam_matches[0].action, "log")

    async def test_known_malicious_domain_matches_scam_pack(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Free nitro gift https://dlscord-gift.com/claim")

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "known_malicious_domain")
        self.assertEqual(result.link_assessments[0].category, "malicious")

    async def test_malicious_domain_family_subdomain_matches(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Please verify at https://cdn.dlscord-gift.com/login")

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "malicious")
        self.assertIn("bundled_malicious_domain_family", result.link_assessments[0].matched_signals)

    async def test_adult_domain_matches_when_adult_pack_is_enabled(self):
        ok, _ = await self.service.set_pack_config(10, "adult", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Adult link https://pornhub.com/view_video.php?viewkey=test")

        adult_matches = [match for match in result.matches if match.pack == "adult"]
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "adult_domain")
        self.assertEqual(result.link_assessments[0].category, "adult")

    async def test_adult_domain_does_not_match_when_adult_pack_is_disabled(self):
        result = self.service.test_message_details(10, "Adult link https://xvideos.com/video123")

        self.assertFalse([match for match in result.matches if match.pack == "adult"])
        self.assertEqual(result.link_assessments[0].category, "adult")

    async def test_adult_warning_context_still_matches_when_enabled(self):
        ok, _ = await self.service.set_pack_config(10, "adult", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Warning: NSFW link ahead https://pornhub.com/view_video.php?viewkey=test")

        self.assertTrue([match for match in result.matches if match.pack == "adult"])
        self.assertEqual(result.link_assessments[0].category, "adult")

    async def test_allowlisted_domain_overrides_local_malicious_intelligence(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "dlscord-gift.com", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Free nitro https://dlscord-gift.com/claim")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "safe")
        self.assertIn("guild_allow_domain", result.link_assessments[0].matched_signals)

    async def test_allowlisted_domain_stays_safe_even_with_suspicious_path(self):
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "example.com", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Please verify https://example.com/login?token=abc123")

        self.assertEqual(result.link_assessments[0].category, "safe")
        self.assertFalse(result.link_assessments[0].provider_lookup_warranted)

    async def test_allow_phrase_bypasses_known_malicious_domain(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Scam example: https://dlscord-gift.com/claim",
        )

        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_phrases", "scam example", True)
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNone(decision)

    async def test_test_message_details_reports_allow_phrase_bypass_without_matches(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_phrases", "scam example", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "scam example https://dlscord-gift.com/claim")

        self.assertFalse(result.matches)
        self.assertIsNotNone(result.bypass_reason)
        self.assertIn("allow phrase", result.bypass_reason.lower())
        self.assertEqual(result.link_assessments[0].category, "malicious")

    async def test_unknown_safe_domain_stays_unknown_without_provider_lookup(self):
        result = self.service.test_message_details(10, "Reference link https://example.org/guide")

        self.assertEqual(result.link_assessments[0].category, "unknown")
        self.assertFalse(result.link_assessments[0].provider_lookup_warranted)

    async def test_unknown_login_and_docs_urls_stay_non_actionable_without_host_risk(self):
        cases = (
            "Normal page https://accounts.example.com/login",
            "Reference https://example.com/reset?token=abc123&redirect=dashboard",
            "Reference https://docs.example.com/guide?redirect=install",
        )

        for text in cases:
            result = self.service.test_message_details(10, text)

            self.assertFalse(result.matches)
            self.assertEqual(result.link_assessments[0].category, "unknown")
            self.assertFalse(result.link_assessments[0].provider_lookup_warranted)
            self.assertNotIn("message_social_engineering", result.link_assessments[0].matched_signals)

    async def test_unknown_suspicious_domain_now_matches_scam_pack_when_language_is_strong(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Visit https://wallet-bonus-drop.click/account?redirect=%2Flogin%2Fauth%2Ftoken%2Fseed to claim access.",
        )

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertTrue(result.link_assessments[0].provider_lookup_warranted)

    async def test_unknown_suspicious_domain_in_report_context_stays_non_lookup(self):
        result = self.service.test_message_details(
            10,
            "Reference https://wallet-bonus-drop.click/account?redirect=%2Flogin%2Fauth%2Ftoken%2Fseed for triage.",
        )

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertFalse(result.link_assessments[0].provider_lookup_warranted)

    async def test_provider_disabled_status_is_exposed_through_link_safety_runtime(self):
        status = self.service.get_link_safety_status()

        self.assertIn("bundled", status["intel_source"])
        self.assertGreaterEqual(status["bundled_malicious_domains"], 10)
        self.assertGreaterEqual(status["effective_malicious_domains"], status["bundled_malicious_domains"])
        self.assertIn("external_malicious_source_paths", status)
        self.assertIn("external_malicious_skipped_lines", status)
        self.assertFalse(status["provider_available"])
        self.assertIn("local domain intelligence", status["provider_status"].lower())

    async def test_scam_warning_context_suppresses_known_malicious_domain_action(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Warning: fake gift site https://dlscord-gift.com/claim do not click")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "malicious")

    async def test_bare_malicious_domain_discussion_context_is_not_punished(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        for text in (
            "dlscord-gift.com is malicious",
            "We blocked dlscord-gift.com yesterday",
        ):
            result = self.service.test_message_details(10, text)

            self.assertFalse([match for match in result.matches if match.pack == "scam"])
            self.assertEqual(result.link_assessments[0].category, "malicious")

    async def test_not_fake_wording_no_longer_suppresses_real_malicious_link(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "This is not fake. Official free nitro gift, verify now at https://dlscord-gift.com/claim",
        )

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "malicious")

    async def test_punycode_lure_with_bait_still_hits_scam_pack(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Claim reward now https://xn--discod-bonus-q5a.click/verify")

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertTrue(result.link_assessments[0].provider_lookup_warranted)

    async def test_weird_query_shape_marks_unknown_domain_as_suspicious(self):
        result = self.service.test_message_details(
            10,
            "Open https://fresh-offer.example.top/download?redirect=%2Fgift%2Fclaim%2Flogin%2Ftoken%2Fsession%2Fwallet",
        )

        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertTrue(result.link_assessments[0].provider_lookup_warranted)

    async def test_multiple_links_produce_safe_and_suspicious_assessments_together(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Compare https://github.com/arno-create/babblebox-bot and https://dlscord-gift.com/claim now",
        )

        categories = {item.normalized_domain: item.category for item in result.link_assessments}
        self.assertEqual(categories["github.com"], "safe")
        self.assertEqual(categories["dlscord-gift.com"], "malicious")
        self.assertTrue([match for match in result.matches if match.pack == "scam"])

    async def test_shortener_without_other_signals_stays_conservative(self):
        result = self.service.test_message_details(10, "Useful article https://bit.ly/example")

        self.assertEqual(result.link_assessments[0].category, "unknown")
        self.assertFalse(result.link_assessments[0].provider_lookup_warranted)

    async def test_malformed_host_is_ignored_in_link_assessment(self):
        result = self.service.test_message_details(10, "Broken link https://www..example.com/login")

        self.assertFalse(result.matches)
        self.assertEqual(result.link_assessments, ())

    async def test_malicious_like_wording_without_risky_link_does_not_trigger_scam(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Claim reward now and verify your account on https://youtube.com/watch?v=abc123")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "safe")

    async def test_cache_stays_bounded_after_many_unique_domains(self):
        for index in range(300):
            self.service.test_message_details(10, f"https://offer-{index}.example.top/claim?token={index}")

        status = self.service.get_link_safety_status()

        self.assertLessEqual(status["cache_entries"], status["cache_max_entries"])

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

    async def test_webhook_message_is_scanned_for_scam_matches(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, bot=True)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            webhook_id=9988,
            content=(
                "Official community post: Members are invited to participate in the OpenSea mint. "
                "Visit https://opensea-mint-event.com/claim soon because selection is limited."
            ),
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message, scan_source="webhook_message")

        self.assertIsNotNone(decision)
        self.assertEqual(decision.scan_source, "webhook_message")
        self.assertTrue(decision.deleted)
        self.assertTrue([match for match in decision.reasons if match.pack == "scam"])

    async def test_embed_only_scam_message_is_scanned(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="",
            embeds=[
                FakeEmbed(
                    title="Official OpenSea partnership",
                    description="Secure your spot in the mint and visit the official page now.",
                    url="https://opensea-mint-event.com/claim",
                )
            ],
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIn("embeds", decision.scan_surface_labels)
        self.assertTrue(decision.deleted)

    async def test_attachment_metadata_and_link_combo_increase_scam_confidence(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Download the official wallet verification package at https://wallet-verify-hub.click/download now.",
            attachments=[FakeAttachment("mint-pass.zip", description="official wallet verification package")],
        )

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertIn("suspicious_attachment_link_combo", result.link_assessments[0].matched_signals)

    async def test_forwarded_snapshot_content_is_scanned(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Forwarded announcement",
            message_snapshots=[
                FakeMessageSnapshot(
                    content="OpenSea members are invited to the official mint. Visit https://opensea-mint-event.com/claim soon.",
                )
            ],
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIn("forwarded_snapshot", decision.scan_surface_labels)
        self.assertTrue([match for match in decision.reasons if match.pack == "scam"])

    async def test_message_edit_rescans_only_when_shield_surfaces_change(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42)
        before = FakeMessage(guild=guild, channel=channel, author=author, content="just chatting", message_id=4001)
        unchanged = FakeMessage(guild=guild, channel=channel, author=author, content="just chatting", message_id=4001)
        after = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="just chatting https://opensea-mint-event.com/claim verify now",
            message_id=4001,
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        self.assertIsNone(await self.service.handle_message_edit(before, unchanged))
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message_edit(before, after)

        self.assertIsNotNone(decision)
        self.assertEqual(decision.scan_source, "message_edit")
        self.assertTrue(decision.deleted)

    async def test_message_edit_with_changed_content_alerts_again_for_same_message_id(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42)
        original = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Email me at first@example.com",
            message_id=4101,
        )
        edited = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Email me at second@example.com",
            message_id=4101,
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()) as alert_mock:
            first = await self.service.handle_message(original)
            second = await self.service.handle_message_edit(original, edited)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertEqual(alert_mock.await_count, 2)
        self.assertEqual(second.scan_source, "message_edit")

    async def test_missing_manage_messages_permission_degrades_cleanly(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20, permissions=FakePermissions(manage_messages=False))
        author = FakeAuthor(42)
        message = FakeMessage(guild=guild, channel=channel, author=author, content="Free nitro gift https://dlscord-gift.com/claim")

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertFalse(decision.deleted)
        self.assertIn("could not delete", decision.action_note.lower())

    async def test_alert_embed_includes_evidence_basis_for_unknown_suspicious_lure(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(
            77,
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=10),
            avatar=None,
            display_name="Support Desk",
        )
        message = FakeMessage(
            guild=guild,
            channel=public_channel,
            author=author,
            content=(
                "Official community post: Discord members should verify access before the window closes. "
                "Visit https://secure-auth-session.click/login now."
            ),
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        embed = log_channel.sent[0]["embed"]
        evidence_field = next(field for field in embed.fields if field.name == "Evidence Basis")
        self.assertIn("Combined suspicion around an unknown risky link.", evidence_field.value)
        self.assertIn("Primary risky domain: `secure-auth-session.click`", evidence_field.value)
        self.assertIn("Confidence rose with:", evidence_field.value)

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
        fingerprint = self._content_fingerprint(message.content)

        await self.service._send_alert(message, compiled, decision, content_fingerprint=fingerprint)
        await self.service._send_alert(message, compiled, decision, content_fingerprint=fingerprint)

        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Shield Alert | Privacy Leak")
        self.assertIn("Possible email address", embed.fields[0].value)
        self.assertIn("Class:", embed.fields[0].value)
        self.assertIn("Resolved action:", embed.fields[0].value)

    async def test_alert_signature_dedupes_near_identical_different_messages(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        first = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")
        second = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")

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
        fingerprint = self._content_fingerprint(first.content)

        await self.service._send_alert(first, compiled, decision, content_fingerprint=fingerprint)
        await self.service._send_alert(second, compiled, decision, content_fingerprint=fingerprint)

        self.assertEqual(len(log_channel.sent), 1)

    async def test_alert_signature_allows_material_outcome_change(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        first = FakeMessage(guild=guild, channel=public_channel, author=author, content="Free nitro claim now https://bit.ly/bait")
        second = FakeMessage(guild=guild, channel=public_channel, author=author, content="Free nitro claim now https://bit.ly/bait")

        ok, _ = await self.service.set_log_channel(10, log_channel.id)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[10]
        base_reason = ShieldMatch(
            pack="scam",
            label="Scam bait wording",
            reason="A high-risk scam lure appeared next to a suspicious link.",
            action="delete_escalate",
            confidence="high",
            heuristic=True,
            match_class="known_malicious_domain",
        )
        first_decision = ShieldDecision(
            matched=True,
            action="delete_escalate",
            pack="scam",
            reasons=(base_reason,),
            deleted=True,
        )
        second_decision = ShieldDecision(
            matched=True,
            action="delete_escalate",
            pack="scam",
            reasons=(base_reason,),
            deleted=True,
            timed_out=True,
            escalated=True,
            action_note="Repeated-hit escalation triggered after 3 strikes in 10 minutes.",
        )
        fingerprint = self._content_fingerprint(first.content)

        await self.service._send_alert(first, compiled, first_decision, content_fingerprint=fingerprint)
        await self.service._send_alert(second, compiled, second_decision, content_fingerprint=fingerprint)

        self.assertEqual(len(log_channel.sent), 2)

    async def test_alert_signature_can_send_again_after_short_window(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        first = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")
        second = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")

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
        fingerprint = self._content_fingerprint(first.content)
        clock = {"now": 100.0}
        fake_loop = types.SimpleNamespace(time=lambda: clock["now"])

        with patch("babblebox.shield_service.asyncio.get_running_loop", return_value=fake_loop):
            await self.service._send_alert(first, compiled, decision, content_fingerprint=fingerprint)
            clock["now"] = 106.0
            await self.service._send_alert(second, compiled, decision, content_fingerprint=fingerprint)

        self.assertEqual(len(log_channel.sent), 2)

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

    async def test_ai_review_uses_embed_only_scanned_text_when_message_body_is_empty(self):
        guild = FakeGuild(SHIELD_AI_ALLOWED_GUILD_ID)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])
        message = FakeMessage(
            guild=guild,
            channel=public_channel,
            author=author,
            content="",
            embeds=[FakeEmbed(description="Contact me at friend@example.com for access.")],
        )
        self.service.ai_provider = FakeAIProvider(
            result=ShieldAIReviewResult(
                classification="privacy_leak",
                confidence="high",
                priority="normal",
                false_positive=False,
                explanation="Looks like contact information shared in the embed text.",
                model="gpt-4.1-mini",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled=True, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.ai_review)
        request = self.service.ai_provider.review.await_args.args[0]
        self.assertIn("Contact me at [EMAIL]", request.sanitized_content)
        self.assertIn("embeds", decision.scan_surface_labels)

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

    async def test_member_risk_handoff_runs_for_fresh_account_scam_message(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(
            42,
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=15),
            avatar=None,
            display_name="Official Support",
        )
        admin_service = types.SimpleNamespace(handle_member_risk_message=AsyncMock())
        self.bot.admin_service = admin_service
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content=(
                "Official community post: Members are invited to secure their spot in the OpenSea mint. "
                "Visit https://opensea-mint-event.com/claim soon because selection is limited."
            ),
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.member_risk_evidence)
        self.assertIn("scam_high", decision.member_risk_evidence.signal_codes)
        self.assertIn("newcomer_early_message", decision.member_risk_evidence.signal_codes)
        admin_service.handle_member_risk_message.assert_awaited_once_with(message, decision)

    async def test_member_risk_handoff_skips_webhook_messages(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, bot=True)
        admin_service = types.SimpleNamespace(handle_member_risk_message=AsyncMock())
        self.bot.admin_service = admin_service
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            webhook_id=991,
            content=(
                "Official community post: Members are invited to participate in the mint. "
                "Visit https://opensea-mint-event.com/claim soon because selection is limited."
            ),
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message, scan_source="webhook_message")

        self.assertIsNotNone(decision)
        self.assertIsNone(decision.member_risk_evidence)
        admin_service.handle_member_risk_message.assert_not_awaited()

    async def test_fresh_campaign_cluster_signal_accumulates_across_fresh_accounts(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        decisions = []
        for user_id in (200, 201, 202):
            author = FakeAuthor(
                user_id,
                created_at=ge.now_utc() - timedelta(hours=3),
                joined_at=ge.now_utc() - timedelta(minutes=25),
                avatar=None,
            )
            message = FakeMessage(
                guild=guild,
                channel=channel,
                author=author,
                content=(
                    "Official community post: Members are invited to verify their access and keep their member slot. "
                    "Visit https://secure-auth-session.click/login now before the window closes."
                ),
            )
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions.append(await self.service.handle_message(message))

        self.assertTrue(all(decision is not None for decision in decisions))
        self.assertIsNotNone(decisions[-1].member_risk_evidence)
        self.assertIn("fresh_campaign_cluster_3", decisions[-1].member_risk_evidence.signal_codes)

    async def test_newcomer_first_link_signals_only_fire_once_for_new_messages(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(
            301,
            created_at=ge.now_utc() - timedelta(hours=2),
            joined_at=ge.now_utc() - timedelta(minutes=15),
            avatar=None,
            display_name="Support Desk",
        )
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        first = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Verify your slot at https://secure-auth-session.click/login before the window closes.",
        )
        second = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Members should still verify at https://secure-auth-session.click/login to keep access.",
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            first_decision = await self.service.handle_message(first)
            second_decision = await self.service.handle_message(second)

        self.assertIn("first_message_link", first_decision.member_risk_evidence.signal_codes)
        self.assertIn("first_external_link", first_decision.member_risk_evidence.signal_codes)
        self.assertNotIn("first_message_link", second_decision.member_risk_evidence.signal_codes)
        self.assertNotIn("first_external_link", second_decision.member_risk_evidence.signal_codes)

    async def test_campaign_lure_reuse_tracks_rotated_domains(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        decisions = []
        for user_id, domain in ((401, "secure-auth-session.click"), (402, "member-access-pass.live")):
            author = FakeAuthor(
                user_id,
                created_at=ge.now_utc() - timedelta(hours=3),
                joined_at=ge.now_utc() - timedelta(minutes=20),
                avatar=None,
            )
            message = FakeMessage(
                guild=guild,
                channel=channel,
                author=author,
                content=(
                    "Official community post: Discord members should verify access before the window closes. "
                    f"Visit https://{domain}/login now."
                ),
            )
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions.append(await self.service.handle_message(message))

        self.assertIn("campaign_lure_reuse", decisions[-1].member_risk_evidence.signal_codes)
        self.assertIn("fresh_campaign_cluster_2", decisions[-1].member_risk_evidence.signal_codes)

    async def test_campaign_path_shape_tracks_same_domain_login_pattern(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        decisions = []
        for user_id, token in ((501, "abc123"), (502, "xyz789")):
            author = FakeAuthor(
                user_id,
                created_at=ge.now_utc() - timedelta(hours=3),
                joined_at=ge.now_utc() - timedelta(minutes=20),
                avatar=None,
            )
            message = FakeMessage(
                guild=guild,
                channel=channel,
                author=author,
                content=(
                    "Official community post: Members should verify access immediately. "
                    f"Visit https://secure-auth-session.click/login?token={token} now."
                ),
            )
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions.append(await self.service.handle_message(message))

        self.assertIn("campaign_path_shape", decisions[-1].member_risk_evidence.signal_codes)
        self.assertIn("fresh_campaign_cluster_2", decisions[-1].member_risk_evidence.signal_codes)


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
