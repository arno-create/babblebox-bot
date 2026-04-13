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
    FEATURE_SURFACE_AFK_REASON,
    FEATURE_SURFACE_AFK_SCHEDULE_REASON,
    FEATURE_SURFACE_CONFESSIONS_LINKS,
    FEATURE_SURFACE_REMINDER_CREATE,
    FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY,
    FEATURE_SURFACE_WATCH_KEYWORD,
    ShieldDecision,
    ShieldFeatureLinkScan,
    ShieldMatch,
    ShieldRaidEvidence,
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
    def __init__(self, guild_id: int = 10, *, name: Optional[str] = None):
        self.id = guild_id
        self.name = name or f"Guild {guild_id}"
        self.me = FakeBotMember()

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return None


class FakeAttachment:
    def __init__(
        self,
        filename: str,
        *,
        description: Optional[str] = None,
        title: Optional[str] = None,
        content_type: Optional[str] = None,
    ):
        self.filename = filename
        self.url = f"https://cdn.discordapp.com/{filename}"
        self.content_type = content_type
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
            "model": "gpt-5.4-nano" if self._available else None,
            "routing_strategy": "two_tier_with_dormant_top",
            "single_model_override": False,
            "fast_model": "gpt-5.4-nano" if self._available else None,
            "complex_model": "gpt-5.4-mini" if self._available else None,
            "top_model": "gpt-5.4" if self._available else None,
            "top_tier_enabled": False,
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
                    "adult_solicitation_enabled": True,
                    "link_policy_mode": "trusted_only",
                    "link_policy_action": "delete_log",
                    "link_policy_low_action": "log",
                    "link_policy_medium_action": "delete_log",
                    "link_policy_high_action": "delete_log",
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
        self.assertTrue(config["adult_solicitation_enabled"])
        self.assertEqual(config["link_policy_mode"], "trusted_only")
        self.assertEqual(config["link_policy_low_action"], "log")
        self.assertEqual(config["link_policy_medium_action"], "delete_log")
        self.assertEqual(config["link_policy_high_action"], "delete_log")
        self.assertEqual(config["ai_enabled_packs"], ["privacy", "promo"])
        self.assertTrue(meta["ordinary_ai_enabled"])
        self.assertEqual(meta["ordinary_ai_updated_by"], 77)


class ShieldServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = FakeBot()
        self.store = ShieldStateStore(backend="memory")
        self.service = ShieldService(self.bot, store=self.store)
        started = await self.service.start()
        self.assertTrue(started)

    def _content_fingerprint(self, content: str, *, attachments=None) -> str:
        return _alert_content_fingerprint(_build_snapshot(content, attachments))

    async def _enable_spam_pack(
        self,
        guild_id: int,
        *,
        sensitivity: str = "normal",
        low_action: str = "log",
        medium_action: str = "delete_log",
        high_action: str = "delete_escalate",
    ):
        ok, _ = await self.service.set_module_enabled(guild_id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(
            guild_id,
            "spam",
            enabled=True,
            low_action=low_action,
            medium_action=medium_action,
            high_action=high_action,
            sensitivity=sensitivity,
        )
        self.assertTrue(ok)

    def _make_member(
        self,
        guild: FakeGuild,
        user_id: int,
        *,
        created_delta: timedelta = timedelta(days=30),
        joined_delta: timedelta = timedelta(days=30),
        bot: bool = False,
    ) -> FakeAuthor:
        member = FakeAuthor(
            user_id,
            roles=[FakeRole(11, position=1)],
            bot=bot,
            created_at=ge.now_utc() - created_delta,
            joined_at=ge.now_utc() - joined_delta,
        )
        member.guild = guild
        return member

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

    async def test_first_enable_applies_recommended_non_ai_baseline_once(self):
        ok, message = await self.service.set_module_enabled(10, True)

        self.assertTrue(ok)
        self.assertIn("Shield AI stays second-pass only and owner-managed", message)
        self.assertIn("recommended non-AI baseline", message)
        config = self.service.get_config(10)
        self.assertTrue(config["module_enabled"])
        self.assertEqual(config["baseline_version"], 2)
        self.assertTrue(config["adult_solicitation_enabled"])
        for pack in ("privacy", "promo", "scam", "adult", "severe"):
            with self.subTest(pack=pack):
                self.assertTrue(config[f"{pack}_enabled"])
                self.assertEqual(config[f"{pack}_low_action"], "log")
                self.assertEqual(config[f"{pack}_medium_action"], "delete_log")
                self.assertEqual(config[f"{pack}_high_action"], "delete_log")
                self.assertEqual(config[f"{pack}_sensitivity"], "normal")
        self.assertTrue(config["spam_enabled"])
        self.assertEqual(config["spam_low_action"], "log")
        self.assertEqual(config["spam_medium_action"], "delete_log")
        self.assertEqual(config["spam_high_action"], "delete_escalate")
        self.assertEqual(config["spam_sensitivity"], "normal")

    async def test_startup_baseline_upgrade_backfills_spam_pack_for_existing_live_guilds(self):
        self.service.store.state["guilds"]["10"] = {
            "module_enabled": True,
            "baseline_version": 1,
            "privacy_enabled": True,
            "promo_enabled": True,
            "scam_enabled": True,
            "adult_enabled": True,
            "severe_enabled": True,
        }

        changed = self.service._apply_startup_baseline_upgrades()

        self.assertTrue(changed)
        config = self.service.get_config(10)
        self.assertEqual(config["baseline_version"], 2)
        self.assertTrue(config["spam_enabled"])
        self.assertEqual(config["spam_low_action"], "log")
        self.assertEqual(config["spam_medium_action"], "delete_log")
        self.assertEqual(config["spam_high_action"], "delete_escalate")

    async def test_reenable_preserves_customized_rules_after_first_enable(self):
        ok, _ = await self.service.set_pack_config(10, "severe", enabled=False, low_action="detect", medium_action="log", high_action="delete_log", sensitivity="high")
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(10, "scam", high_action="delete_escalate")
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(10, False)
        self.assertTrue(ok)

        ok, message = await self.service.set_module_enabled(10, True)

        self.assertTrue(ok)
        self.assertIn("Shield AI stays second-pass only and owner-managed", message)
        self.assertNotIn("recommended non-AI baseline", message)
        config = self.service.get_config(10)
        self.assertTrue(config["module_enabled"])
        self.assertFalse(config["severe_enabled"])
        self.assertEqual(config["severe_low_action"], "detect")
        self.assertEqual(config["severe_medium_action"], "log")
        self.assertEqual(config["severe_high_action"], "delete_log")
        self.assertEqual(config["severe_sensitivity"], "high")
        self.assertEqual(config["scam_high_action"], "delete_escalate")

    async def test_allowlisted_invite_does_not_bypass_promo_match(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_invite_codes", "abc123", True)
        self.assertTrue(ok)

        matches = self.service.test_message(10, "Join us here https://discord.gg/abc123")

        promo_matches = [match for match in matches if match.pack == "promo"]
        self.assertTrue(promo_matches)
        self.assertEqual(promo_matches[0].match_class, "discord_invite")

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
        self.assertEqual(result.link_assessments[0].category, "impersonation")
        self.assertIn("brand_piggyback_host", result.link_assessments[0].matched_signals)

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

    async def test_adult_solicitation_text_is_off_by_default(self):
        ok, _ = await self.service.set_pack_config(10, "adult", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "DM me for nudes")

        self.assertFalse([match for match in result.matches if match.pack == "adult"])

    async def test_adult_dm_ad_matches_when_enabled(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "DM me for nudes")

        adult_matches = [match for match in result.matches if match.pack == "adult"]
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "adult_dm_ad")
        self.assertEqual(adult_matches[0].label, "Adult-content DM ad")
        self.assertEqual(adult_matches[0].confidence, "medium")

    async def test_adult_solicitation_high_confidence_requires_sales_and_dm_routing(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "selling nude pics, more in DMs")

        adult_matches = [match for match in result.matches if match.pack == "adult"]
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "adult_dm_ad")
        self.assertEqual(adult_matches[0].confidence, "high")
        self.assertEqual(adult_matches[0].action, "delete_log")

    async def test_adult_solicitation_suppresses_education_reporting_moderation_and_disapproval_contexts(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="high",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text in (
            "This sexual health workshop covers consent, trafficking awareness, and adult content moderation.",
            'Reported screenshot for review: they said "DM me for nudes" in another server.',
            'Moderation log quote: user said "selling nude pics, more in DMs" in another server.',
            "report this ad: DM me for nudes",
            'moderation note: user said "DM me for nudes"',
            "mods deleted DM me for nudes spam",
            "please do not say DM me for nudes here",
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "adult"])

    async def test_adult_solicitation_generic_policy_words_do_not_suppress_real_dm_ads(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text, expected_confidence in (
            ("server rules: DM me for nudes", "medium"),
            ("policy update: selling nude pics, more in DMs", "high"),
            ("example pricing: DM me for nudes", "medium"),
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                adult_matches = [match for match in result.matches if match.pack == "adult"]
                self.assertTrue(adult_matches)
                self.assertEqual(adult_matches[0].match_class, "adult_dm_ad")
                self.assertEqual(adult_matches[0].confidence, expected_confidence)

    async def test_adult_solicitation_requires_bounded_adult_offer_structure(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="high",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text in ("DM me later", "nudes are not allowed here", "more content in DMs"):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "adult"])

    async def test_adult_solicitation_low_confidence_requires_high_sensitivity(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        normal_result = self.service.test_message_details(10, "NSFW customs available")
        self.assertFalse([match for match in normal_result.matches if match.pack == "adult"])

        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="high",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        high_result = self.service.test_message_details(10, "NSFW customs available")

        adult_matches = [match for match in high_result.matches if match.pack == "adult"]
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "adult_solicitation")
        self.assertEqual(adult_matches[0].confidence, "low")

    async def test_adult_solicitation_catches_common_dm_gated_variants(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text, expected_class in (
            ("My DMs are open for nudes", "adult_dm_ad"),
            ("18+ pics in DM", "adult_dm_ad"),
            ("DM me for OF", "adult_dm_ad"),
            ("DM for pics", "adult_dm_ad"),
            ("menu in DM", "adult_dm_ad"),
            ("prices via DM", "adult_dm_ad"),
            ("Buy my nudes", "adult_solicitation"),
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                adult_matches = [match for match in result.matches if match.pack == "adult"]
                self.assertTrue(adult_matches)
                self.assertEqual(adult_matches[0].match_class, expected_class)

    async def test_adult_solicitation_dm_menu_euphemisms_require_normal_sensitivity(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="low",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        low_result = self.service.test_message_details(10, "menu in DM")
        self.assertFalse([match for match in low_result.matches if match.pack == "adult"])

        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        normal_result = self.service.test_message_details(10, "menu in DM")
        adult_matches = [match for match in normal_result.matches if match.pack == "adult"]
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "adult_dm_ad")
        self.assertEqual(adult_matches[0].confidence, "medium")

    async def test_adult_solicitation_suppresses_benign_photo_and_service_contexts(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="high",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text in (
            "DM me for pics of the event tonight.",
            "Message me for menu prices for tattoo designs.",
            "catering menu in DM for tonight's event",
            "Sexual health workshop tomorrow.",
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "adult"])

    async def test_adult_solicitation_catches_prices_customs_and_open_for_requests_variants(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text in (
            "Prices in DM for custom content",
            "Customs open, DM me",
            "Open for requests, custom content in DM",
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                adult_matches = [match for match in result.matches if match.pack == "adult"]
                self.assertTrue(adult_matches)
                self.assertEqual(adult_matches[0].match_class, "adult_dm_ad")

    async def test_adult_solicitation_stays_suppressed_for_benign_custom_content_and_requests_contexts(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="high",
            adult_solicitation=True,
        )
        self.assertTrue(ok)

        for text in (
            "Open for requests for website design.",
            "Custom content guidelines for the art contest.",
            'Moderation example: they said "Customs open, DM me" in another server.',
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "adult"])

    async def test_adult_solicitation_channel_carve_out_only_relaxes_optional_text_detector(self):
        ok, _ = await self.service.set_pack_config(
            10,
            "adult",
            enabled=True,
            action="delete_log",
            sensitivity="normal",
            adult_solicitation=True,
        )
        self.assertTrue(ok)
        ok, _ = await self.service.set_filter_target(10, "adult_solicitation_excluded_channel_ids", 55, True)
        self.assertTrue(ok)

        carve_out_result = self.service.test_message_details(10, "DM me for nudes", channel_id=55)
        normal_result = self.service.test_message_details(10, "DM me for nudes", channel_id=56)
        domain_result = self.service.test_message_details(
            10,
            "Adult link https://pornhub.com/view_video.php?viewkey=test",
            channel_id=55,
        )

        self.assertFalse([match for match in carve_out_result.matches if match.pack == "adult"])
        self.assertIn("relaxes only the optional adult-solicitation detector", carve_out_result.bypass_reason or "")
        self.assertTrue([match for match in normal_result.matches if match.pack == "adult"])
        adult_domain_matches = [match for match in domain_result.matches if match.pack == "adult"]
        self.assertTrue(adult_domain_matches)
        self.assertEqual(adult_domain_matches[0].match_class, "adult_domain")

    async def test_feature_gateway_blocks_private_reminder_text_without_live_side_effects(self):
        before_state = (
            len(self.service._alert_dedup),
            len(self.service._alert_signature_dedup),
            len(self.service._strike_windows),
            len(self.service._recent_promos),
            len(self.service._recent_scam_campaigns),
            len(self.service._recent_newcomer_activity),
        )
        with patch.object(self.service, "_send_alert", new=AsyncMock()) as alert_mock, patch.object(self.service.ai_provider, "review", new=AsyncMock()) as ai_mock:
            decision = self.service.evaluate_feature_text(FEATURE_SURFACE_REMINDER_CREATE, "Call me at +1 (212) 555-0189.")

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.surface, FEATURE_SURFACE_REMINDER_CREATE)
        self.assertEqual(decision.reason_code, "privacy_phone")
        self.assertIn("private", decision.user_message.lower())
        self.assertEqual(
            before_state,
            (
                len(self.service._alert_dedup),
                len(self.service._alert_signature_dedup),
                len(self.service._strike_windows),
                len(self.service._recent_promos),
                len(self.service._recent_scam_campaigns),
                len(self.service._recent_newcomer_activity),
            ),
        )
        alert_mock.assert_not_awaited()
        ai_mock.assert_not_awaited()

    async def test_feature_gateway_watch_surface_stays_privacy_only(self):
        decision = self.service.evaluate_feature_text(FEATURE_SURFACE_WATCH_KEYWORD, "dm me for nudes")

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.surface, FEATURE_SURFACE_WATCH_KEYWORD)
        self.assertFalse(decision.matches)

    async def test_feature_gateway_allows_health_context_after_legacy_blocklist_removal(self):
        decision = self.service.evaluate_feature_text(FEATURE_SURFACE_AFK_REASON, "Sexual health workshop tomorrow")

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.surface, FEATURE_SURFACE_AFK_REASON)
        self.assertFalse(decision.matches)

    async def test_feature_gateway_blocks_adult_and_severe_text_on_bounded_surfaces(self):
        adult_decision = self.service.evaluate_feature_text(FEATURE_SURFACE_AFK_SCHEDULE_REASON, "DM me for nudes")
        severe_decision = self.service.evaluate_feature_text(FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY, "kill yourself")

        self.assertFalse(adult_decision.allowed)
        self.assertEqual(adult_decision.reason_code, "adult_dm_ad")
        self.assertIn("adult", adult_decision.user_message.lower())
        self.assertFalse(severe_decision.allowed)
        self.assertEqual(severe_decision.reason_code, "self_harm_encouragement")
        self.assertIn("severe", severe_decision.user_message.lower())

    async def test_feature_gateway_assesses_confession_links_with_shared_shield_intel(self):
        scan = self.service.assess_feature_links(
            FEATURE_SURFACE_CONFESSIONS_LINKS,
            text="verify here https://dlscord-gift.com/claim",
            link_policy_mode="trusted_only",
        )

        self.assertIsInstance(scan, ShieldFeatureLinkScan)
        self.assertTrue(scan.has_links)
        self.assertIn("malicious_link", scan.flags)
        self.assertEqual(scan.link_assessments[0].normalized_domain, "dlscord-gift.com")
        self.assertEqual(scan.link_assessments[0].category, "malicious")

    async def test_feature_gateway_blocks_trusted_brand_impersonation_links(self):
        scan = self.service.assess_feature_links(
            FEATURE_SURFACE_CONFESSIONS_LINKS,
            text="verify here https://youtub.e.com/watch?v=1",
            link_policy_mode="trusted_only",
        )

        self.assertIsInstance(scan, ShieldFeatureLinkScan)
        self.assertTrue(scan.has_links)
        self.assertIn("malicious_link", scan.flags)
        self.assertEqual(scan.link_assessments[0].category, "impersonation")
        self.assertIn("brand_split", scan.link_assessments[0].matched_signals)

    async def test_trusted_only_link_policy_allows_trusted_docs_link(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Useful docs: https://docs.github.com/en")

        self.assertFalse([match for match in result.matches if match.pack == "link_policy"])

    async def test_trusted_only_link_policy_blocks_safe_unknown_domain(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Read this https://example.com/guide")

        policy_matches = [match for match in result.matches if match.pack == "link_policy"]
        self.assertTrue(policy_matches)
        self.assertEqual(policy_matches[0].match_class, "untrusted_external_link")
        self.assertEqual(policy_matches[0].confidence, "low")

    async def test_trusted_only_link_policy_blocks_unallowlisted_invites_even_without_promo_pack(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Join us https://discord.gg/notallowed")

        policy_matches = [match for match in result.matches if match.pack == "link_policy"]
        self.assertTrue(policy_matches)
        self.assertEqual(policy_matches[0].match_class, "untrusted_invite_link")
        self.assertEqual(policy_matches[0].confidence, "medium")
        self.assertFalse([match for match in result.matches if match.pack == "promo"])

    async def test_trusted_only_link_policy_respects_domain_and_invite_allowlists(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "example.com", True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_invite_codes", "abc123", True)
        self.assertTrue(ok)

        domain_result = self.service.test_message_details(10, "Read this https://example.com/guide")
        invite_result = self.service.test_message_details(10, "Join us https://discord.gg/abc123")

        self.assertFalse([match for match in domain_result.matches if match.pack == "link_policy"])
        self.assertFalse([match for match in invite_result.matches if match.pack == "link_policy"])
        self.assertEqual(domain_result.link_assessments[0].category, "unknown")
        self.assertIn("guild_allow_domain", domain_result.link_assessments[0].matched_signals)

    async def test_trusted_only_link_policy_blocks_link_hubs_and_suspicious_domains(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        hub_result = self.service.test_message_details(10, "Creator links https://linktr.ee/example")
        suspicious_result = self.service.test_message_details(10, "Community update https://verify-hub.live/news")

        hub_matches = [match for match in hub_result.matches if match.pack == "link_policy"]
        suspicious_matches = [match for match in suspicious_result.matches if match.pack == "link_policy"]
        self.assertTrue(hub_matches)
        self.assertEqual(hub_matches[0].match_class, "blocked_link_hub")
        self.assertEqual(hub_matches[0].confidence, "medium")
        self.assertTrue(suspicious_matches)
        self.assertEqual(suspicious_matches[0].match_class, "link_policy_suspicious")
        self.assertEqual(suspicious_matches[0].confidence, "medium")

    async def test_trusted_only_link_policy_blocks_malicious_and_adult_domains_when_specialized_packs_are_off(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        malicious_result = self.service.test_message_details(10, "Free nitro https://dlscord-gift.com/claim")
        adult_result = self.service.test_message_details(10, "Adult link https://pornhub.com/view_video.php?viewkey=test")

        malicious_matches = [match for match in malicious_result.matches if match.pack == "link_policy"]
        adult_matches = [match for match in adult_result.matches if match.pack == "link_policy"]
        self.assertTrue(malicious_matches)
        self.assertEqual(malicious_matches[0].match_class, "link_policy_malicious")
        self.assertEqual(malicious_matches[0].confidence, "high")
        self.assertTrue(adult_matches)
        self.assertEqual(adult_matches[0].match_class, "link_policy_adult")
        self.assertEqual(adult_matches[0].confidence, "high")

    async def test_trusted_only_link_policy_still_blocks_allowlisted_risky_domains(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        for domain in ("dlscord-gift.com", "pornhub.com", "verify-hub.live"):
            ok, _ = await self.service.set_allow_entry(10, "allow_domains", domain, True)
            self.assertTrue(ok)

        malicious_result = self.service.test_message_details(10, "Free nitro https://dlscord-gift.com/claim")
        adult_result = self.service.test_message_details(10, "Adult link https://pornhub.com/view_video.php?viewkey=test")
        suspicious_result = self.service.test_message_details(10, "Community update https://verify-hub.live/news")

        self.assertEqual(
            [match.match_class for match in malicious_result.matches if match.pack == "link_policy"],
            ["link_policy_malicious"],
        )
        self.assertEqual(
            [match.match_class for match in adult_result.matches if match.pack == "link_policy"],
            ["link_policy_adult"],
        )
        self.assertEqual(
            [match.match_class for match in suspicious_result.matches if match.pack == "link_policy"],
            ["link_policy_suspicious"],
        )
        self.assertIn("guild_allow_domain", malicious_result.link_assessments[0].matched_signals)
        self.assertIn("guild_allow_domain", adult_result.link_assessments[0].matched_signals)
        self.assertIn("guild_allow_domain", suspicious_result.link_assessments[0].matched_signals)

    async def test_trusted_only_link_policy_hard_blocks_trusted_brand_impersonation(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Use https://youtub.e.com/watch?v=1 right now")

        policy_matches = [match for match in result.matches if match.pack == "link_policy"]
        self.assertEqual(policy_matches[0].match_class, "link_policy_impersonation")
        self.assertEqual(policy_matches[0].confidence, "high")
        self.assertEqual(result.link_assessments[0].category, "impersonation")
        self.assertIn("brand_split", result.link_assessments[0].matched_signals)

    async def test_allowlisted_domain_does_not_override_trusted_brand_impersonation(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "youtub.e.com", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Use https://youtub.e.com/watch?v=1 right now")

        policy_matches = [match for match in result.matches if match.pack == "link_policy"]
        self.assertEqual(policy_matches[0].match_class, "link_policy_impersonation")
        self.assertIn("guild_allow_domain", result.link_assessments[0].matched_signals)

    async def test_disabling_builtin_trusted_family_blocks_that_family_until_admin_allowlisted(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        ok, _ = await self.service.set_trusted_builtin_family_enabled(10, "social", False)
        self.assertTrue(ok)

        blocked = self.service.test_message_details(10, "Read https://reddit.com/r/python")
        blocked_matches = [match for match in blocked.matches if match.pack == "link_policy"]
        self.assertEqual(blocked_matches[0].match_class, "untrusted_external_link")
        self.assertEqual(blocked.link_assessments[0].category, "safe")
        self.assertEqual(blocked.link_assessments[0].safe_family, "social")

        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "reddit.com", True)
        self.assertTrue(ok)
        allowed = self.service.test_message_details(10, "Read https://reddit.com/r/python")
        self.assertFalse([match for match in allowed.matches if match.pack == "link_policy"])
        self.assertIn("guild_allow_domain", allowed.link_assessments[0].matched_signals)

    async def test_disabling_builtin_trusted_domain_blocks_that_domain_until_admin_allowlisted(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        ok, _ = await self.service.set_trusted_builtin_domain_enabled(10, "github.com", False)
        self.assertTrue(ok)

        blocked = self.service.test_message_details(10, "Read https://github.com/openai")
        blocked_matches = [match for match in blocked.matches if match.pack == "link_policy"]
        self.assertEqual(blocked_matches[0].match_class, "untrusted_external_link")
        self.assertEqual(blocked.link_assessments[0].category, "safe")

        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "github.com", True)
        self.assertTrue(ok)
        allowed = self.service.test_message_details(10, "Read https://github.com/openai")
        self.assertFalse([match for match in allowed.matches if match.pack == "link_policy"])
        self.assertIn("guild_allow_domain", allowed.link_assessments[0].matched_signals)

    async def test_trusted_pack_state_exposes_builtins_examples_and_local_overrides(self):
        ok, _ = await self.service.set_trusted_builtin_family_enabled(10, "social", False)
        self.assertTrue(ok)
        ok, _ = await self.service.set_trusted_builtin_domain_enabled(10, "github.com", False)
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "example.com", True)
        self.assertTrue(ok)

        state = self.service.trusted_pack_state(10)

        social = next(item for item in state["families"] if item["name"] == "social")
        github = next(item for item in state["direct_domains"] if item["domain"] == "github.com")
        self.assertTrue(social["examples"])
        self.assertTrue(social["disabled"])
        self.assertTrue(github["disabled"])
        self.assertIn("social", state["disabled_families"])
        self.assertIn("github.com", state["disabled_domains"])
        self.assertIn("example.com", state["allow_domains"])

    async def test_specialized_scam_and_adult_packs_still_own_matches_under_trusted_only_mode(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(10, "adult", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)

        malicious_result = self.service.test_message_details(10, "Free nitro https://dlscord-gift.com/claim")
        adult_result = self.service.test_message_details(10, "Adult link https://pornhub.com/view_video.php?viewkey=test")

        self.assertTrue([match for match in malicious_result.matches if match.pack == "scam"])
        self.assertFalse([match for match in malicious_result.matches if match.pack == "link_policy"])
        self.assertTrue([match for match in adult_result.matches if match.pack == "adult"])
        self.assertFalse([match for match in adult_result.matches if match.pack == "link_policy"])

    async def test_default_link_policy_mode_preserves_current_non_policy_behavior(self):
        result = self.service.test_message_details(10, "Read this https://example.com/guide")

        self.assertFalse([match for match in result.matches if match.pack == "link_policy"])

    async def test_allowlisted_domain_does_not_override_local_malicious_intelligence(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "dlscord-gift.com", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Free nitro https://dlscord-gift.com/claim")

        self.assertTrue([match for match in result.matches if match.pack == "scam"])
        self.assertEqual(result.link_assessments[0].category, "malicious")
        self.assertIn("guild_allow_domain", result.link_assessments[0].matched_signals)

    async def test_allowlisted_domain_only_bypasses_policy_lane_when_no_risky_intel_matches(self):
        ok, _ = await self.service.set_allow_entry(10, "allow_domains", "example.com", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Please verify https://example.com/login?token=abc123")

        self.assertFalse(result.matches)
        self.assertEqual(result.link_assessments[0].category, "unknown")
        self.assertIn("guild_allow_domain", result.link_assessments[0].matched_signals)
        self.assertFalse(result.link_assessments[0].provider_lookup_warranted)

    async def test_allow_phrase_does_not_bypass_known_malicious_domain(self):
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

        self.assertIsNotNone(decision)
        self.assertTrue([reason for reason in decision.reasons if reason.pack == "scam"])
        self.assertEqual(decision.link_assessments[0].category, "malicious")

    async def test_test_message_details_reports_bounded_allow_phrase_bypass_for_promo(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_phrases", "join us here", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "join us here https://discord.gg/abc123")

        self.assertFalse(result.matches)
        self.assertIsNotNone(result.bypass_reason)
        self.assertIn("allow phrase", result.bypass_reason.lower())
        self.assertEqual(result.link_assessments[0].category, "safe")

    async def test_allow_phrase_does_not_bypass_trusted_link_policy(self):
        ok, _ = await self.service.set_link_policy_config(10, mode="trusted_only")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_phrases", "reference link", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "reference link https://example.com/guide")

        policy_matches = [match for match in result.matches if match.pack == "link_policy"]
        self.assertTrue(policy_matches)
        self.assertEqual(policy_matches[0].match_class, "untrusted_external_link")

    async def test_allow_phrase_still_suppresses_adult_solicitation_text_match(self):
        ok, _ = await self.service.set_pack_config(10, "adult", enabled=True, action="log", sensitivity="normal", adult_solicitation=True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(10, "allow_phrases", "policy example", True)
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "policy example dm me for nudes")

        self.assertFalse([match for match in result.matches if match.pack == "adult"])
        self.assertIsNotNone(result.bypass_reason)
        self.assertIn("allow phrase", result.bypass_reason.lower())

    async def test_severe_pack_is_off_by_default(self):
        result = self.service.test_message_details(10, "kill yourself")

        self.assertFalse([match for match in result.matches if match.pack == "severe"])

    async def test_severe_pack_matches_self_harm_csam_hate_and_slur_abuse(self):
        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        expectations = {
            "kill yourself": "self_harm_encouragement",
            "DM me for CSAM prices": "sexual_exploitation_solicitation",
            "exterminate jews": "eliminationist_hate",
            "you retard": "severe_slur_abuse",
        }
        for text, expected_class in expectations.items():
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                severe_matches = [match for match in result.matches if match.pack == "severe"]
                self.assertTrue(severe_matches)
                self.assertEqual(severe_matches[0].match_class, expected_class)

    async def test_severe_pack_suppresses_reporting_contexts_and_normal_rudeness(self):
        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        for text in (
            'Reported screenshot for moderation: they said "kill yourself" in another server.',
            'History example for review: "exterminate jews" is genocidal rhetoric.',
            'moderation note: user said "kill yourself"',
            "mods deleted you retard spam",
            "report this ad: DM me for CSAM prices",
            "please do not tell people to kill yourself",
            "please do not call people retard",
            "shut up",
            "no one cares",
        ):
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "severe"])

    async def test_severe_pack_high_sensitivity_adds_extreme_degradation_layer(self):
        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        normal_result = self.service.test_message_details(10, "those immigrants are vermin")
        self.assertFalse([match for match in normal_result.matches if match.pack == "severe"])

        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)
        high_result = self.service.test_message_details(10, "those immigrants are vermin")
        severe_matches = [match for match in high_result.matches if match.pack == "severe"]
        self.assertTrue(severe_matches)
        self.assertEqual(severe_matches[0].match_class, "eliminationist_hate")

    async def test_severe_pack_category_toggle_and_term_customization_work(self):
        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        ok, _ = await self.service.set_severe_category(10, "self_harm_encouragement", False)
        self.assertTrue(ok)
        disabled_result = self.service.test_message_details(10, "kill yourself")
        self.assertFalse([match for match in disabled_result.matches if match.pack == "severe"])

        ok, _ = await self.service.set_severe_category(10, "self_harm_encouragement", True)
        self.assertTrue(ok)
        restored_result = self.service.test_message_details(10, "kill yourself")
        self.assertTrue([match for match in restored_result.matches if match.pack == "severe"])

        ok, _ = await self.service.update_severe_term(10, "add", "you scumlord")
        self.assertTrue(ok)
        custom_result = self.service.test_message_details(10, "you scumlord")
        self.assertTrue([match for match in custom_result.matches if match.pack == "severe"])

        ok, _ = await self.service.update_severe_term(10, "remove_custom", "you scumlord")
        self.assertTrue(ok)
        removed_custom_result = self.service.test_message_details(10, "you scumlord")
        self.assertFalse([match for match in removed_custom_result.matches if match.pack == "severe"])

        ok, _ = await self.service.update_severe_term(10, "remove_default", "retard")
        self.assertTrue(ok)
        removed_default_result = self.service.test_message_details(10, "you retard")
        self.assertFalse([match for match in removed_default_result.matches if match.pack == "severe"])

        ok, _ = await self.service.update_severe_term(10, "restore_default", "retard")
        self.assertTrue(ok)
        restored_default_result = self.service.test_message_details(10, "you retard")
        self.assertTrue([match for match in restored_default_result.matches if match.pack == "severe"])

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

    async def test_obvious_safe_domain_impersonation_is_a_first_class_local_block(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Claim reward now https://githuub.com/login")

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "trusted_brand_impersonation_domain")
        self.assertEqual(result.link_assessments[0].category, "impersonation")
        self.assertIn("near_brand_host", result.link_assessments[0].matched_signals)

    async def test_confusable_brand_host_is_blocked_as_punycode_brand_impersonation(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        confusable_domain = "disc" + chr(0x043E) + "rd.com"

        result = self.service.test_message_details(10, f"Verify here https://{confusable_domain}/login")

        self.assertEqual(result.link_assessments[0].category, "impersonation")
        self.assertIn("punycode_brand", result.link_assessments[0].matched_signals)
        self.assertTrue([match for match in result.matches if match.pack == "scam"])

    async def test_weaker_brand_overlap_stays_suspicious_only_instead_of_hard_impersonation(self):
        result = self.service.test_message_details(10, "Reference link https://discordstatuspage.com/update")

        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertIn("near_brand_host", result.link_assessments[0].matched_signals)

    async def test_generic_overlap_domain_does_not_false_positive_as_brand_impersonation(self):
        result = self.service.test_message_details(10, "Reference link https://steamcleaningpros.com")

        self.assertEqual(result.link_assessments[0].category, "unknown")
        self.assertFalse(result.link_assessments[0].matched_signals)

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

    def test_common_filename_tokens_do_not_become_bare_links(self):
        for token in ("voice-message.ogg", "podcast.mp3", "image.png", "photo.jpg", "manual.pdf", "report.docx"):
            snapshot = _build_snapshot(token)
            self.assertEqual(snapshot.urls, (), msg=token)
            self.assertFalse(snapshot.has_links, msg=token)

    def test_file_like_tlds_still_extract_when_a_real_url_is_present(self):
        snapshot = _build_snapshot("Download https://voice-message.ogg right now")

        self.assertEqual(snapshot.urls, ("https://voice-message.ogg",))
        self.assertTrue(snapshot.has_links)

    async def test_suspicious_archive_target_still_marks_link_as_suspicious(self):
        result = self.service.test_message_details(
            10,
            "Install https://wallet-verify-hub.click/download/update.zip now to verify access.",
        )

        self.assertEqual(result.link_assessments[0].category, "unknown_suspicious")
        self.assertIn("suspicious_file_target", result.link_assessments[0].matched_signals)

    async def test_discord_native_media_link_stays_non_promo_without_context(self):
        ok, _ = await self.service.set_pack_config(10, "promo", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "https://cdn.discordapp.com/attachments/123/456/voice-message.ogg",
        )

        self.assertFalse([match for match in result.matches if match.pack == "promo"])

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

    async def test_repeated_same_external_link_with_varied_captions_still_clusters(self):
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

        messages = (
            "check this out https://example.com/docs/guide",
            "useful resource https://example.com/docs/guide",
            "read here https://example.com/docs/guide",
            "docs again https://example.com/docs/guide",
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for text in messages
            ]

        self.assertEqual(decisions[:-1], [None, None, None])
        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "repetitive_link_noise")
        self.assertEqual(final.reasons[0].label, "Repeated external link")

    async def test_repeated_voice_message_attachments_do_not_create_link_noise(self):
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

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(
                        guild=guild,
                        channel=channel,
                        author=author,
                        content="",
                        attachments=[FakeAttachment("voice-message.ogg", content_type="audio/ogg")],
                    )
                )
                for _ in range(4)
            ]

        self.assertEqual(decisions, [None, None, None, None])

    async def test_repeated_audio_attachments_without_links_do_not_trigger_link_noise(self):
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

        messages = (
            FakeMessage(guild=guild, channel=channel, author=author, content="", attachments=[FakeAttachment("voice-message.ogg", content_type="audio/ogg")]),
            FakeMessage(guild=guild, channel=channel, author=author, content="", attachments=[FakeAttachment("podcast.mp3", content_type="audio/mpeg")]),
            FakeMessage(guild=guild, channel=channel, author=author, content="", attachments=[FakeAttachment("meeting.wav", content_type="audio/wav")]),
            FakeMessage(guild=guild, channel=channel, author=author, content="", attachments=[FakeAttachment("memo.m4a", content_type="audio/mp4")]),
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        self.assertEqual(decisions, [None, None, None, None])

    async def test_repeated_harmless_attachment_only_posts_do_not_enter_link_noise_logic(self):
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

        attachments = ("image.png", "photo.jpg", "manual.pdf", "report.docx")
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="", attachments=[FakeAttachment(filename)])
                )
                for filename in attachments
            ]

        self.assertEqual(decisions, [None, None, None, None])

    async def test_allowlisted_domain_repetition_still_hits_link_noise_logic(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=True, action="log", sensitivity="high")
        self.assertTrue(ok)
        ok, _ = await self.service.set_allow_entry(guild.id, "allow_domains", "example.com", True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://example.com")
                )
                for _ in range(4)
            ]

        self.assertEqual(decisions[:3], [None, None, None])
        self.assertIsNotNone(decisions[3])
        self.assertEqual(decisions[3].reasons[0].match_class, "repetitive_link_noise")

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

    async def test_identical_duplicate_spam_escalates_after_corroborated_hits(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 42)

        await self._enable_spam_pack(guild.id, sensitivity="high")

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="claim your starter pack now")
                )
                for _ in range(4)
            ]

        self.assertEqual(decisions[:2], [None, None])
        self.assertIsNotNone(decisions[2])
        self.assertEqual(decisions[2].action, "delete_log")
        self.assertIsNotNone(decisions[3])
        self.assertEqual(decisions[3].pack, "spam")
        self.assertEqual(decisions[3].action, "delete_escalate")
        self.assertTrue(decisions[3].deleted)
        self.assertTrue({reason.match_class for reason in decisions[3].reasons}.issuperset({"spam_duplicate", "spam_near_duplicate"}))

    async def test_near_duplicate_spam_detection_handles_spacing_and_punctuation_variants(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 52)

        await self._enable_spam_pack(guild.id, sensitivity="normal")
        variants = (
            "join my stream right now!!!",
            "join my stream right now!!",
            "join my stream right now!",
            "join my stream right now ?",
            "join my stream right now...",
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for text in variants
            ]

        self.assertEqual(decisions[:4], [None, None, None, None])
        self.assertIsNotNone(decisions[4])
        self.assertEqual(decisions[4].pack, "spam")
        self.assertEqual(decisions[4].action, "delete_log")
        self.assertIn("spam_near_duplicate", {reason.match_class for reason in decisions[4].reasons})

    async def test_repeated_link_flood_uses_spam_pack_when_promo_is_disabled(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 62)

        await self._enable_spam_pack(guild.id)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=False)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content=f"https://example.com/docs/{index}")
                )
                for index in range(4)
            ]

        self.assertEqual(decisions[:3], [None, None, None])
        self.assertIsNotNone(decisions[3])
        self.assertEqual(decisions[3].pack, "spam")
        self.assertEqual(decisions[3].action, "delete_log")
        self.assertIn("spam_link_flood", {reason.match_class for reason in decisions[3].reasons})

    async def test_repeated_invite_flood_detects_rotating_codes_in_spam_pack(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 72)

        await self._enable_spam_pack(guild.id)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=False)
        self.assertTrue(ok)
        invites = (
            "join https://discord.gg/alpha111",
            "join https://discord.gg/beta222",
            "join https://discord.gg/alpha111",
            "join https://discord.gg/beta222",
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for text in invites
            ]

        self.assertEqual(decisions[:2], [None, None])
        self.assertIsNotNone(decisions[2])
        self.assertIsNotNone(decisions[3])
        self.assertEqual(decisions[3].pack, "spam")
        self.assertEqual(decisions[3].action, "delete_escalate")
        self.assertIn("spam_invite_flood", {reason.match_class for reason in decisions[3].reasons})

    async def test_mention_flood_is_detected_without_needing_rate_history(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 82)

        await self._enable_spam_pack(guild.id)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(
                FakeMessage(
                    guild=guild,
                    channel=channel,
                    author=author,
                    content="@everyone <@1> <@2> <@3>",
                )
            )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.pack, "spam")
        self.assertEqual(decision.action, "delete_log")
        self.assertIn("spam_mention_flood", {reason.match_class for reason in decision.reasons})

    async def test_emoji_flood_detects_but_normal_emoji_use_stays_allowed(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 92)

        await self._enable_spam_pack(guild.id)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            flood = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=author, content="😀" * 20)
            )
            normal = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=author, content="great round 😀😀😀")
            )

        self.assertIsNotNone(flood)
        self.assertEqual(flood.pack, "spam")
        self.assertIn("spam_emoji_flood", {reason.match_class for reason in flood.reasons})
        self.assertIsNone(normal)

    async def test_burst_posting_detects_low_value_noise_but_not_normal_conversation(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        spammer = self._make_member(guild, 102)
        chatter = self._make_member(guild, 103)

        await self._enable_spam_pack(guild.id)
        low_value_posts = ("lol", "lmao", "haha", "yo", "ok", "???", "bruh")
        normal_posts = tuple(f"I think we should queue round {index} after the break." for index in range(7))

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            spam_decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=spammer, content=text))
                for text in low_value_posts
            ]
            normal_decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=chatter, content=text))
                for text in normal_posts
            ]

        self.assertIsNotNone(spam_decisions[-1])
        self.assertEqual(spam_decisions[-1].pack, "spam")
        self.assertIn("spam_burst", {reason.match_class for reason in spam_decisions[-1].reasons})
        self.assertTrue(all(item is None for item in normal_decisions))

    async def test_bot_messages_stay_conservative_for_generic_spam_signals(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        bot_author = self._make_member(guild, 112, bot=True)

        await self._enable_spam_pack(guild.id, sensitivity="high")

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=bot_author, content="claim your starter pack now")
                )
                for _ in range(8)
            ]

        self.assertTrue(all(item is None for item in decisions))

    async def test_normal_growth_below_join_wave_threshold_stays_quiet(self):
        guild = FakeGuild(10)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)

        await self._enable_spam_pack(guild.id)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)

        for index in range(4):
            await self.service.handle_member_join(
                self._make_member(
                    guild,
                    200 + index,
                    created_delta=timedelta(days=60),
                    joined_delta=timedelta(minutes=1),
                )
            )

        self.assertEqual(log_channel.sent, [])

    async def test_join_wave_watch_alert_stays_compact_and_deduped(self):
        guild = FakeGuild(10)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)

        await self._enable_spam_pack(guild.id)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)

        for index in range(6):
            evidence = await self.service.handle_member_join(
                self._make_member(
                    guild,
                    300 + index,
                    created_delta=timedelta(days=45),
                    joined_delta=timedelta(minutes=1),
                )
            )

        self.assertIsNotNone(evidence)
        self.assertTrue(evidence.join_wave)
        self.assertFalse(evidence.fresh_join_wave)
        self.assertEqual(len(log_channel.sent), 1)
        self.assertEqual(log_channel.sent[0]["embed"].title, "Shield Alert | Spam / Raid")
        self.assertIn("Raid Watch", log_channel.sent[0]["embed"].fields[0].value)

    async def test_fresh_join_wave_plus_shared_newcomer_spam_pattern_confirms_raid(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        admin_service = types.SimpleNamespace(handle_member_risk_message=AsyncMock())
        self.bot.admin_service = admin_service

        await self._enable_spam_pack(guild.id)
        members = [
            self._make_member(
                guild,
                400 + index,
                created_delta=timedelta(hours=2),
                joined_delta=timedelta(minutes=2),
            )
            for index in range(5)
        ]
        for member in members:
            await self.service.handle_member_join(member)

        content = "@everyone <@1> <@2> <@3>"
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            first = await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=members[0], content=content))
            second = await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=members[1], content=content))

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertIsNotNone(first.raid_evidence)
        self.assertIsNotNone(second.raid_evidence)
        self.assertTrue(first.raid_evidence.join_wave)
        self.assertTrue(first.raid_evidence.fresh_join_wave)
        self.assertFalse(first.raid_evidence.confirmed)
        self.assertTrue(second.raid_evidence.confirmed)
        self.assertTrue(second.raid_evidence.confidence_lifted)
        self.assertEqual(second.pack, "spam")
        self.assertEqual(second.action, "delete_escalate")
        self.assertTrue(second.deleted)
        self.assertIn("spam_high", second.member_risk_evidence.signal_codes)
        self.assertIn("raid_join_wave", second.member_risk_evidence.signal_codes)
        self.assertIn("raid_fresh_join_wave", second.member_risk_evidence.signal_codes)
        self.assertIn("raid_pattern_cluster", second.member_risk_evidence.signal_codes)
        admin_service.handle_member_risk_message.assert_awaited()

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

    async def test_legitimate_bot_embed_with_support_ticket_copy_stays_below_scam_threshold(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(52, bot=True)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="",
            embeds=[
                FakeEmbed(
                    title="PayPal support ticket update",
                    description="Review your case notes at https://verify-hub.live/paypal/ticket",
                )
            ],
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNone(decision)

    async def test_human_message_keeps_medium_signal_scam_detection_that_bots_now_suppress(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(53)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="Official PayPal support ticket update: verify your case at https://verify-hub.live/paypal/ticket",
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertTrue([match for match in decision.reasons if match.pack == "scam"])
        self.assertEqual(decision.link_assessments[0].category, "unknown_suspicious")

    async def test_webhook_message_with_medium_signal_brand_ticket_copy_stays_conservative(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(54, bot=True)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            webhook_id=777,
            content="Official community post: PayPal support ticket update. Verify your case at https://verify-hub.live/paypal/ticket",
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message, scan_source="webhook_message")

        self.assertIsNone(decision)

    async def test_webhook_message_with_hard_impersonation_still_gets_caught(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(55, bot=True)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            webhook_id=778,
            content="Official community post: Discord account check. Verify now at https://youtub.e.com/login",
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(message, scan_source="webhook_message")

        self.assertIsNotNone(decision)
        self.assertTrue(decision.deleted)
        self.assertTrue([match for match in decision.reasons if match.match_class == "trusted_brand_impersonation_domain"])
        self.assertEqual(decision.link_assessments[0].category, "impersonation")
        self.assertIsNone(decision.member_risk_evidence)

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

    async def test_attachment_only_scam_bait_uses_non_link_match_class(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Free nitro claim now",
            attachments=[FakeAttachment("reward.zip", description="gift package")],
        )

        match_classes = {match.match_class for match in result.matches if match.pack == "scam"}
        self.assertIn("scam_bait_attachment", match_classes)
        self.assertNotIn("scam_bait_link", match_classes)

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

    async def test_confirmed_raid_signature_dedup_suppresses_followup_alerts(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        first_author = self._make_member(guild, 501, created_delta=timedelta(hours=2), joined_delta=timedelta(minutes=3))
        second_author = self._make_member(guild, 502, created_delta=timedelta(hours=2), joined_delta=timedelta(minutes=2))
        first = FakeMessage(guild=guild, channel=public_channel, author=first_author, content="@everyone <@1> <@2> <@3>")
        second = FakeMessage(guild=guild, channel=public_channel, author=second_author, content="@everyone <@4> <@5> <@6>")

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[guild.id]
        reason = ShieldMatch(
            pack="spam",
            label="Mention flood",
            reason="The message tagged 4 accounts in one shot. Active raid corroboration raised confidence.",
            action="delete_escalate",
            confidence="high",
            heuristic=True,
            match_class="spam_mention_flood",
        )
        raid_evidence = ShieldRaidEvidence(
            join_count_60s=5,
            join_count_5m=5,
            fresh_join_count_60s=4,
            fresh_join_count_5m=4,
            join_wave=True,
            fresh_join_wave=True,
            pattern_cluster_size=2,
            pattern_kind="exact",
            pattern_signature="raid-sig",
            confirmed=True,
            confidence_lifted=True,
        )
        first_decision = ShieldDecision(
            matched=True,
            action="delete_escalate",
            pack="spam",
            reasons=(reason,),
            deleted=True,
            raid_evidence=raid_evidence,
            alert_evidence_signature="raid-sig",
        )
        second_decision = ShieldDecision(
            matched=True,
            action="delete_escalate",
            pack="spam",
            reasons=(reason,),
            deleted=True,
            raid_evidence=raid_evidence,
            alert_evidence_signature="raid-sig",
        )

        await self.service._send_alert(first, compiled, first_decision, content_fingerprint="raid-a")
        await self.service._send_alert(second, compiled, second_decision, content_fingerprint="raid-b")

        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Shield Alert | Spam / Raid")
        evidence_field = next(field for field in embed.fields if field.name == "Evidence Basis")
        self.assertIn("Join wave: 5 in 60s / 5 in 5m", evidence_field.value)
        self.assertIn("Shared newcomer pattern: 2 accounts (exact)", evidence_field.value)

    async def test_low_confidence_repetition_alert_is_compact_and_does_not_ping(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_alert_role(guild.id, 777)
        self.assertTrue(ok)

        for _ in range(4):
            decision = await self.service.handle_message(
                FakeMessage(guild=guild, channel=public_channel, author=author, content="https://example.com/docs/guide")
            )

        self.assertIsNotNone(decision)
        self.assertEqual(len(log_channel.sent), 1)
        sent = log_channel.sent[0]
        self.assertIsNone(sent["content"])
        embed = sent["embed"]
        self.assertEqual(embed.title, "Shield Note | Promo / Invite")
        self.assertEqual([field.name for field in embed.fields], ["Detection", "Why it was noted", "Scan Source", "Preview", "Jump"])
        self.assertIn("Repeated external link", embed.fields[0].value)
        self.assertIn("posted 4 times in 10 minutes", embed.fields[1].value)
        self.assertIn("https://example.com/docs/guide", embed.fields[3].value)

    async def test_low_confidence_repetition_cohort_dedup_suppresses_repeated_notes(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_alert_role(guild.id, 777)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[guild.id]
        decision = ShieldDecision(
            matched=True,
            action="log",
            pack="promo",
            reasons=(
                ShieldMatch(
                    pack="promo",
                    label="Repeated external link",
                    reason="The same external link was posted 4 times in 10 minutes without enough promo evidence to treat it as self-promo.",
                    action="log",
                    confidence="low",
                    heuristic=True,
                    match_class="repetitive_link_noise",
                ),
            ),
            alert_evidence_signature="repeat-fingerprint",
            alert_evidence_summary="The same external link was posted 4 times in 10 minutes without enough promo evidence to treat it as self-promo.",
        )
        first = FakeMessage(guild=guild, channel=public_channel, author=author, content="https://example.com/docs/guide")
        second = FakeMessage(guild=guild, channel=public_channel, author=author, content="check this out https://example.com/docs/guide")
        clock = {"now": 100.0}
        fake_loop = types.SimpleNamespace(time=lambda: clock["now"])

        with patch("babblebox.shield_service.asyncio.get_running_loop", return_value=fake_loop):
            await self.service._send_alert(first, compiled, decision, content_fingerprint="first")
            clock["now"] = 106.0
            await self.service._send_alert(second, compiled, decision, content_fingerprint="second")

        self.assertEqual(len(log_channel.sent), 1)
        self.assertIsNone(log_channel.sent[0]["content"])

    async def test_high_confidence_scam_log_only_alert_can_still_ping(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_alert_role(guild.id, 777)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[guild.id]
        decision = ShieldDecision(
            matched=True,
            action="log",
            pack="scam",
            reasons=(
                ShieldMatch(
                    pack="scam",
                    label="Known malicious domain",
                    reason="A linked domain matched Shield's local malicious-domain intelligence.",
                    action="log",
                    confidence="high",
                    heuristic=False,
                    match_class="known_malicious_domain",
                ),
            ),
        )
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="https://dlscord-gift.com/claim")

        await self.service._send_alert(message, compiled, decision, content_fingerprint="danger")

        self.assertEqual(len(log_channel.sent), 1)
        self.assertEqual(log_channel.sent[0]["content"], "<@&777>")

    async def test_support_guild_ai_policy_defaults_to_enabled_with_full_models(self):
        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)

        self.assertTrue(status["enabled"])
        self.assertEqual(status["policy_source"], "support_default")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4"])

    async def test_ordinary_guild_ai_policy_defaults_to_disabled_with_nano(self):
        status = self.service.get_ai_status(10)

        self.assertFalse(status["enabled"])
        self.assertEqual(status["policy_source"], "ordinary_global")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])

    async def test_public_ai_config_only_changes_scope(self):
        ok, message = await self.service.set_ai_config(10, enabled=True)
        self.assertFalse(ok)
        self.assertIn("owner-managed", message.lower())

        ok, message = await self.service.set_ai_config(10, enabled_packs=["privacy", "adult", "severe"], min_confidence="medium")
        self.assertTrue(ok)
        self.assertIn("review scope", message.lower())
        self.assertIn("Adult Links + Solicitation", message)
        self.assertIn("Severe Harm / Hate", message)
        config = self.service.get_config(10)
        self.assertEqual(set(config["ai_enabled_packs"]), {"privacy", "adult", "severe"})
        self.assertEqual(config["ai_min_confidence"], "medium")

    async def test_per_guild_override_can_enable_ai_while_global_default_is_off(self):
        ok, message = await self.service.set_guild_ai_access_policy(
            10,
            mode="enabled",
            allowed_models="nano,mini",
            actor_id=1266444952779620413,
        )

        self.assertTrue(ok)
        self.assertIn("enabled", message.lower())
        status = self.service.get_ai_status(10)
        self.assertTrue(status["enabled"])
        self.assertEqual(status["policy_source"], "guild_override")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])

    async def test_per_guild_override_can_disable_ai_while_global_default_is_on(self):
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_guild_ai_access_policy(10, mode="disabled", actor_id=1266444952779620413)
        self.assertTrue(ok)

        status = self.service.get_ai_status(10)
        self.assertFalse(status["enabled"])
        self.assertEqual(status["policy_source"], "guild_override")

    async def test_support_defaults_can_be_restored_after_override(self):
        ok, _ = await self.service.set_guild_ai_access_policy(
            SHIELD_AI_ALLOWED_GUILD_ID,
            mode="disabled",
            allowed_models="nano",
            actor_id=1266444952779620413,
        )
        self.assertTrue(ok)

        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)
        self.assertFalse(status["enabled"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])

        ok, message = await self.service.restore_support_ai_defaults(actor_id=1266444952779620413)
        self.assertTrue(ok)
        self.assertIn("restored", message.lower())
        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)
        self.assertTrue(status["enabled"])
        self.assertEqual(status["policy_source"], "support_default")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4"])

    async def test_support_guild_flagged_message_can_enrich_alert_with_ai_review(self):
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
                model="gpt-5.4-nano",
                tier="fast",
                target_tier="fast",
                route_reasons=(),
                attempted_models=("gpt-5.4-nano",),
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.ai_review)
        self.service.ai_provider.review.assert_awaited_once()
        request = self.service.ai_provider.review.await_args.args[0]
        self.assertEqual(request.allowed_models, ("gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4"))
        embed = log_channel.sent[0]["embed"]
        ai_field = next(field for field in embed.fields if field.name == "AI Assist")
        self.assertIn("Likely privacy leak", ai_field.value)
        self.assertIn("Tier: `fast`", ai_field.value)
        self.assertIn("gpt-5.4-nano", ai_field.value)

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
                model="gpt-5.4-nano",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.ai_review)
        request = self.service.ai_provider.review.await_args.args[0]
        self.assertIn("Contact me at [EMAIL]", request.sanitized_content)
        self.assertIn("embeds", decision.scan_surface_labels)

    async def test_global_ordinary_policy_can_enable_nano_for_other_guilds(self):
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
                model="gpt-5.4-nano",
            )
        )

        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        decision = await self.service.handle_message(message)

        self.assertIsNotNone(decision)
        self.assertIsNotNone(decision.ai_review)
        request = self.service.ai_provider.review.await_args.args[0]
        self.assertEqual(request.allowed_models, ("gpt-5.4-nano",))

    async def test_guild_disabled_by_owner_policy_never_calls_ai_provider(self):
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
                model="gpt-5.4-nano",
            )
        )

        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_guild_ai_access_policy(guild.id, mode="disabled", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "privacy", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["privacy"], min_confidence="high")
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
                model="gpt-5.4-nano",
            )
        )

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "promo", enabled=True, action="log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["promo"], min_confidence="low")
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
        ok, _ = await self.service.set_ai_config(guild.id, enabled_packs=["privacy"], min_confidence="high")
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
        self.assertFalse(config["adult_solicitation_enabled"])
        self.assertEqual(config["link_policy_mode"], "default")
        self.assertEqual(config["link_policy_low_action"], "log")
        self.assertEqual(config["link_policy_medium_action"], "log")
        self.assertEqual(config["link_policy_high_action"], "log")

    def test_normalize_state_migrates_legacy_global_ai_override_meta(self):
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

        self.assertTrue(normalized["meta"]["ordinary_ai_enabled"])
        self.assertEqual(normalized["meta"]["ordinary_ai_allowed_models"], ["gpt-5.4-nano"])
        self.assertEqual(normalized["meta"]["ordinary_ai_updated_by"], 1266444952779620413)
        self.assertEqual(normalized["meta"]["ordinary_ai_updated_at"], "2026-03-27T10:00:00+00:00")

    def test_normalize_state_preserves_trusted_link_policy_and_adult_solicitation_fields(self):
        store = _MemoryShieldStore()
        snapshot = {
            "version": 3,
            "guilds": {
                "123": {
                    "adult_solicitation_enabled": True,
                    "adult_solicitation_excluded_channel_ids": [987, 654, 987],
                    "link_policy_mode": "trusted_only",
                    "link_policy_action": "timeout_log",
                }
            },
        }

        normalized = store.normalize_state(deepcopy(snapshot))
        config = normalized["guilds"]["123"]

        self.assertTrue(config["adult_solicitation_enabled"])
        self.assertEqual(config["adult_solicitation_excluded_channel_ids"], [654, 987])
        self.assertEqual(config["link_policy_mode"], "trusted_only")
        self.assertEqual(config["link_policy_low_action"], "log")
        self.assertEqual(config["link_policy_medium_action"], "delete_log")
        self.assertEqual(config["link_policy_high_action"], "timeout_log")

    def test_normalize_state_preserves_severe_pack_fields(self):
        store = _MemoryShieldStore()
        snapshot = {
            "version": 3,
            "guilds": {
                "123": {
                    "severe_enabled": True,
                    "severe_action": "delete_log",
                    "severe_sensitivity": "high",
                    "severe_enabled_categories": ["self_harm_encouragement", "severe_slur_abuse", "invalid"],
                    "severe_custom_terms": ["you scumlord", "you scumlord"],
                    "severe_removed_terms": ["retard", "retard"],
                }
            },
        }

        normalized = store.normalize_state(deepcopy(snapshot))
        config = normalized["guilds"]["123"]

        self.assertTrue(config["severe_enabled"])
        self.assertEqual(config["severe_low_action"], "log")
        self.assertEqual(config["severe_medium_action"], "delete_log")
        self.assertEqual(config["severe_high_action"], "delete_log")
        self.assertEqual(config["severe_sensitivity"], "high")
        self.assertEqual(config["severe_enabled_categories"], ["self_harm_encouragement", "severe_slur_abuse"])
        self.assertEqual(config["severe_custom_terms"], ["you scumlord"])
        self.assertEqual(config["severe_removed_terms"], ["retard"])

    def test_normalize_state_preserves_spam_pack_fields(self):
        store = _MemoryShieldStore()
        snapshot = {
            "version": 7,
            "guilds": {
                "123": {
                    "spam_enabled": True,
                    "spam_action": "delete_escalate",
                    "spam_sensitivity": "high",
                }
            },
        }

        normalized = store.normalize_state(deepcopy(snapshot))
        config = normalized["guilds"]["123"]

        self.assertTrue(config["spam_enabled"])
        self.assertEqual(config["spam_low_action"], "log")
        self.assertEqual(config["spam_medium_action"], "delete_log")
        self.assertEqual(config["spam_high_action"], "delete_escalate")
        self.assertEqual(config["spam_sensitivity"], "high")
