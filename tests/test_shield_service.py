import json
import types
import unittest
from datetime import timedelta
from copy import deepcopy
from typing import Optional
from unittest.mock import AsyncMock, patch

from babblebox import game_engine as ge

from babblebox.premium_limits import (
    CAPABILITY_SHIELD_AI_REVIEW,
    LIMIT_SHIELD_ALLOWLIST,
    LIMIT_SHIELD_CUSTOM_PATTERNS,
    LIMIT_SHIELD_PACK_EXEMPTIONS,
    guild_capabilities as premium_guild_capabilities,
    guild_limit as premium_guild_limit,
)
from babblebox.premium_models import PLAN_FREE, PLAN_GUILD_PRO
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
    ShieldService,
    _alert_content_fingerprint,
    _build_snapshot,
    _campaign_kind_label,
)
from babblebox.shield_store import (
    SHIELD_META_GLOBAL_AI_OVERRIDE_KEY,
    ShieldStateStore,
    _MemoryShieldStore,
    _PostgresShieldStore,
    normalize_guild_shield_config,
)


class PremiumShieldStub:
    def __init__(
        self,
        *,
        guild_plans: Optional[dict[int, str]] = None,
        capabilities: Optional[dict[int, set[str]]] = None,
        snapshots: Optional[dict[int, dict[str, object]]] = None,
    ):
        self.guild_plans = dict(guild_plans or {})
        self.capabilities = {guild_id: set(values) for guild_id, values in (capabilities or {}).items()}
        self.snapshots = {guild_id: dict(values) for guild_id, values in (snapshots or {}).items()}

    def resolve_guild_limit(self, guild_id: int, limit_key: str) -> int:
        return premium_guild_limit(self.guild_plans.get(guild_id, PLAN_FREE), limit_key)

    def guild_has_capability(self, guild_id: int, capability: str) -> bool:
        return (
            capability in self.capabilities.get(guild_id, set())
            or capability in premium_guild_capabilities(self.guild_plans.get(guild_id, PLAN_FREE))
        )

    def get_guild_snapshot(self, guild_id: int) -> dict[str, object]:
        snapshot = self.snapshots.get(guild_id)
        if snapshot is not None:
            return dict(snapshot)
        plan_code = self.guild_plans.get(guild_id, PLAN_FREE)
        return {
            "plan_code": plan_code,
            "active_plans": () if plan_code == PLAN_FREE else (plan_code,),
            "blocked": False,
            "stale": False,
            "in_grace": False,
            "claim": None,
            "system_access": False,
            "system_access_scope": None,
        }

    def describe_limit_error(self, *, limit_key: str, limit_value: int) -> str:
        return (
            f"You reached this plan's active limit of {limit_value}. Babblebox Guild Pro unlocks more. "
            "Use `/premium plans` to compare tiers. Previously saved over-limit state stays preserved."
        )


class FakeRole:
    def __init__(self, role_id: int, *, position: int = 1):
        self.id = role_id
        self.position = position
        self.mention = f"<@&{role_id}>"

    def __ge__(self, other):
        return self.position >= getattr(other, "position", 0)


class FakeGuildPermissions:
    def __init__(
        self,
        *,
        administrator: bool = False,
        manage_guild: bool = False,
        manage_messages: bool = False,
        moderate_members: bool = False,
        kick_members: bool = False,
        ban_members: bool = False,
    ):
        self.administrator = administrator
        self.manage_guild = manage_guild
        self.manage_messages = manage_messages
        self.moderate_members = moderate_members
        self.kick_members = kick_members
        self.ban_members = ban_members


class FakeAuthor:
    def __init__(
        self,
        user_id: int,
        *,
        roles=None,
        administrator: bool = False,
        manage_guild: bool = False,
        manage_messages: bool = False,
        moderate_members: bool = False,
        kick_members: bool = False,
        ban_members: bool = False,
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
        self.guild_permissions = FakeGuildPermissions(
            administrator=administrator,
            manage_guild=manage_guild,
            manage_messages=manage_messages,
            moderate_members=moderate_members,
            kick_members=kick_members,
            ban_members=ban_members,
        )
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


class FakeSentLogMessage:
    def __init__(self, *, channel, message_id: int, payload: dict):
        self.channel = channel
        self.id = message_id
        self.content = payload.get("content")
        self.embed = payload.get("embed")
        self.allowed_mentions = payload.get("allowed_mentions")

    async def edit(self, **kwargs):
        if "content" in kwargs:
            self.content = kwargs["content"]
        if "embed" in kwargs:
            self.embed = kwargs["embed"]
        if "allowed_mentions" in kwargs:
            self.allowed_mentions = kwargs["allowed_mentions"]
        self.channel.edits.append(kwargs)


class FakeChannel:
    def __init__(self, channel_id: int = 20, *, name: str = "general", permissions: Optional[FakePermissions] = None):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self.sent = []
        self.edits = []
        self._permissions = permissions or FakePermissions()
        self._sent_messages = {}
        self._next_sent_id = 5000

    def permissions_for(self, member):
        return self._permissions

    async def send(self, **kwargs):
        payload = dict(kwargs)
        self.sent.append(payload)
        message = FakeSentLogMessage(channel=self, message_id=self._next_sent_id, payload=payload)
        self._sent_messages[message.id] = message
        self._next_sent_id += 1
        return message

    async def fetch_message(self, message_id: int):
        return self._sent_messages[message_id]


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
        delete_error: Optional[Exception] = None,
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
        self.delete_error = delete_error
        self.delete_attempts = 0

    async def delete(self):
        self.delete_attempts += 1
        if self.delete_error is not None:
            raise self.delete_error
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
            "routing_strategy": "routed_fast_complex",
            "single_model_override": False,
            "ignored_model_settings": [],
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
                    "pack_exemptions": json.dumps({"spam": {"channel_ids": [777], "role_ids": [42], "user_ids": [30]}}),
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
                    "spam_message_threshold": 6,
                    "spam_message_window_seconds": 8,
                    "spam_burst_threshold": 4,
                    "spam_burst_window_seconds": 9,
                    "spam_near_duplicate_threshold": 4,
                    "spam_near_duplicate_window_seconds": 12,
                    "spam_emote_enabled": True,
                    "spam_emote_threshold": 16,
                    "spam_caps_enabled": True,
                    "spam_caps_threshold": 24,
                    "spam_moderator_policy": "delete_only",
                    "gif_message_threshold": 5,
                    "gif_window_seconds": 25,
                    "gif_repeat_threshold": 4,
                    "gif_same_asset_threshold": 2,
                    "gif_min_ratio_percent": 80,
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
        self.assertEqual(config["pack_exemptions"]["spam"]["channel_ids"], [777])
        self.assertEqual(config["pack_exemptions"]["spam"]["role_ids"], [42])
        self.assertEqual(config["pack_exemptions"]["spam"]["user_ids"], [30])
        self.assertTrue(config["adult_solicitation_enabled"])
        self.assertEqual(config["spam_message_threshold"], 6)
        self.assertEqual(config["spam_burst_threshold"], 4)
        self.assertTrue(config["spam_emote_enabled"])
        self.assertEqual(config["spam_caps_threshold"], 24)
        self.assertEqual(config["spam_moderator_policy"], "delete_only")
        self.assertEqual(config["gif_message_threshold"], 5)
        self.assertEqual(config["gif_same_asset_threshold"], 2)
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

    def _attach_premium(
        self,
        *,
        guild_plans: Optional[dict[int, str]] = None,
        capabilities: Optional[dict[int, set[str]]] = None,
        snapshots: Optional[dict[int, dict[str, object]]] = None,
    ):
        self.bot.premium_service = PremiumShieldStub(guild_plans=guild_plans, capabilities=capabilities, snapshots=snapshots)
        for guild_id in list(self.service._compiled_configs):
            self.service._compiled_configs[guild_id] = self.service._compile_config(guild_id, self.service.get_config(guild_id))

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
        **extra_config,
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
            **extra_config,
        )
        self.assertTrue(ok)

    async def _enable_gif_pack(
        self,
        guild_id: int,
        *,
        sensitivity: str = "normal",
        low_action: str = "log",
        medium_action: str = "delete_log",
        high_action: str = "delete_escalate",
        **extra_config,
    ):
        ok, _ = await self.service.set_module_enabled(guild_id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(
            guild_id,
            "gif",
            enabled=True,
            low_action=low_action,
            medium_action=medium_action,
            high_action=high_action,
            sensitivity=sensitivity,
            **extra_config,
        )
        self.assertTrue(ok)

    def _make_member(
        self,
        guild: FakeGuild,
        user_id: int,
        *,
        roles=None,
        administrator: bool = False,
        manage_guild: bool = False,
        manage_messages: bool = False,
        moderate_members: bool = False,
        kick_members: bool = False,
        ban_members: bool = False,
        created_delta: timedelta = timedelta(days=30),
        joined_delta: timedelta = timedelta(days=30),
        bot: bool = False,
    ) -> FakeAuthor:
        member = FakeAuthor(
            user_id,
            roles=roles or [FakeRole(11, position=1)],
            administrator=administrator,
            manage_guild=manage_guild,
            manage_messages=manage_messages,
            moderate_members=moderate_members,
            kick_members=kick_members,
            ban_members=ban_members,
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
        self.assertIn("Shield AI stays second-pass only, owner policy controls whether review runs", message)
        self.assertIn("recommended non-AI baseline", message)
        config = self.service.get_config(10)
        self.assertTrue(config["module_enabled"])
        self.assertEqual(config["baseline_version"], 4)
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
        self.assertEqual(config["spam_high_action"], "delete_timeout_log")
        self.assertEqual(config["spam_sensitivity"], "normal")
        self.assertTrue(config["gif_enabled"])
        self.assertEqual(config["gif_low_action"], "log")
        self.assertEqual(config["gif_medium_action"], "delete_log")
        self.assertEqual(config["gif_high_action"], "delete_timeout_log")
        self.assertEqual(config["gif_sensitivity"], "normal")

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
        self.assertEqual(config["baseline_version"], 4)
        self.assertTrue(config["spam_enabled"])
        self.assertEqual(config["spam_low_action"], "log")
        self.assertEqual(config["spam_medium_action"], "delete_log")
        self.assertEqual(config["spam_high_action"], "delete_timeout_log")
        self.assertTrue(config["gif_enabled"])
        self.assertEqual(config["gif_low_action"], "log")
        self.assertEqual(config["gif_medium_action"], "delete_log")
        self.assertEqual(config["gif_high_action"], "delete_timeout_log")

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
        self.assertIn("Shield AI stays second-pass only, owner policy controls whether review runs", message)
        self.assertNotIn("recommended non-AI baseline", message)
        config = self.service.get_config(10)
        self.assertTrue(config["module_enabled"])
        self.assertFalse(config["severe_enabled"])
        self.assertEqual(config["severe_low_action"], "detect")
        self.assertEqual(config["severe_medium_action"], "log")
        self.assertEqual(config["severe_high_action"], "delete_log")
        self.assertEqual(config["severe_sensitivity"], "high")
        self.assertEqual(config["scam_high_action"], "delete_escalate")

    async def test_gif_pack_config_persists_three_without_store_reread_drift(self):
        ok, message = await self.service.set_pack_config(
            10,
            "gif",
            enabled=True,
            consecutive_threshold=3,
            message_threshold=3,
            window_seconds=3,
        )

        self.assertTrue(ok)
        self.assertIn("True channel streak rule: 3+ consecutive GIF-heavy messages.", message)
        self.assertEqual(self.service.store.state["guilds"]["10"]["gif_consecutive_threshold"], 3)
        self.assertEqual(self.service.store.state["guilds"]["10"]["gif_message_threshold"], 3)
        self.assertEqual(self.service.store.state["guilds"]["10"]["gif_window_seconds"], 3)

        config = self.service.get_config(10)
        self.assertEqual(config["gif_consecutive_threshold"], 3)
        self.assertEqual(config["gif_message_threshold"], 3)
        self.assertEqual(config["gif_window_seconds"], 3)
        self.assertEqual(self.service._compiled_configs[10].gif_rules.consecutive_threshold, 3)

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
            "go jump off a bridge": "self_harm_encouragement",
            "DM me for CSAM prices": "sexual_exploitation_solicitation",
            "exterminate jews": "eliminationist_hate",
            "all muslims are animals": "eliminationist_hate",
            "all trans people should be wiped out": "eliminationist_hate",
            "you retard": "severe_slur_abuse",
            "you are human garbage": "targeted_extreme_degradation",
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
            "the phrase kill yourself is bannable here",
            "saying kill yourself gets you banned",
            "quote: retard",
            "user said go die in DMs",
            "mods said you retard is banned",
            "example: you filthy kike",
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
        normal_result = self.service.test_message_details(10, "those immigrants, absolute vermin")
        self.assertFalse([match for match in normal_result.matches if match.pack == "severe"])

        ok, _ = await self.service.set_pack_config(10, "severe", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)
        high_result = self.service.test_message_details(10, "those immigrants, absolute vermin")
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

    async def test_repeated_tenor_link_is_caught_by_gif_lane(self):
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

        self.assertEqual(decisions[:2], [None, None])
        gif_decisions = [decision for decision in decisions if decision is not None and decision.pack == "gif"]
        self.assertEqual(len(gif_decisions), 1)
        final = gif_decisions[0]
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertIn(final.action, {"delete_log", "delete_timeout_log", "delete_escalate"})
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 3)
        self.assertIn("spam_gif_flood", {reason.match_class for reason in final.reasons})
        self.assertEqual(decisions[3], None)

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

    async def test_gif_flood_is_detected_as_distinct_spam_lane(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(43, roles=[FakeRole(11, position=1)])

        await self._enable_gif_pack(guild.id)

        final = None
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for _ in range(6):
                final = await self.service.handle_message(
                    FakeMessage(
                        guild=guild,
                        channel=channel,
                        author=author,
                        content="https://tenor.com/view/cat-dance-gif-12345",
                    )
                )

        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertEqual(final.reasons[0].match_class, "spam_gif_flood")
        self.assertIn("GIF", final.reasons[0].label)

    async def test_delete_action_removes_full_gif_incident_burst(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 430)

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/cat-dance-gif-12345")
            for _ in range(4)
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        self.assertEqual(decisions[:2], [None, None])
        actionable = [decision for decision in decisions if decision is not None]
        self.assertEqual(len(actionable), 1)
        final = actionable[0]
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 3)
        self.assertEqual(final.delete_attempt_count, 3)
        self.assertTrue(all(message.deleted for message in messages[:3]))
        self.assertFalse(messages[3].deleted)
        self.assertIn("spam_gif_flood", {reason.match_class for reason in final.reasons})
        self.assertEqual(decisions[3], None)

    async def test_gif_incident_logging_is_grouped_and_quiet(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = self._make_member(guild, 431)

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)

        decisions = []
        for _ in range(5):
            decisions.append(
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=public_channel, author=author, content="https://tenor.com/view/cat-dance-gif-12345")
                )
            )

        self.assertEqual(len(log_channel.sent), 1)
        self.assertEqual(len(log_channel.edits), 0)
        actionable = [decision for decision in decisions if decision is not None]
        self.assertEqual(len(actionable), 1)
        self.assertEqual(actionable[0].pack, "gif")
        self.assertEqual(decisions[3:], [None, None])

    async def test_mixed_text_and_occasional_gifs_stay_safe(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(44, roles=[FakeRole(11, position=1)])

        await self._enable_gif_pack(guild.id)

        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions.append(await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="that build finally shipped")))
            decisions.append(await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/celebrate-gif-2000 nice")))
            decisions.append(await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="lol same here")))
            decisions.append(await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/thumbs-up-gif-3000 agreed")))

        self.assertEqual(decisions, [None, None, None, None])

    async def test_slow_personal_gif_pressure_catches_mixed_text_domination(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 770)

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")

        messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content="Hi everyone"),
            FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/g1"),
            FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/g2"),
            FakeMessage(guild=guild, channel=channel, author=author, content="what's up guys"),
            FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/g3"),
            FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/g4"),
        ]

        with patch("babblebox.shield_service.time.monotonic", side_effect=[100.0 + 7.0 * index for index in range(20)]):
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions = [await self.service.handle_message(message) for message in messages]

        final = decisions[-1]
        self.assertEqual(decisions[:5], [None, None, None, None, None])
        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "spam_gif_flood")
        self.assertIn("trigger mode: personal pressure", (final.alert_evidence_summary or "").lower())
        self.assertIn("2 filler text messages", (final.alert_evidence_summary or "").lower())
        self.assertEqual(final.deleted_count, 4)
        self.assertFalse(messages[0].deleted)
        self.assertFalse(messages[3].deleted)
        self.assertTrue(all(messages[index].deleted for index in (1, 2, 4, 5)))

    async def test_collective_gif_streak_threshold_is_configurable(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 780 + index) for index in range(4)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_config(guild.id, "gif", consecutive_threshold=4)
        self.assertTrue(ok)

        messages = []
        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors[0], "https://tenor.com/view/g1"),
                (authors[1], "https://tenor.com/view/g2"),
                (authors[2], "https://tenor.com/view/g3"),
                (authors[3], "https://tenor.com/view/g4"),
            ):
                message = FakeMessage(guild=guild, channel=channel, author=author, content=content)
                messages.append(message)
                decisions.append(await self.service.handle_message(message))

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("trigger mode: collective streak", (final.alert_evidence_summary or "").lower())
        self.assertIn("4 consecutive gif-heavy messages", (final.alert_evidence_summary or "").lower())
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 4)
        self.assertTrue(all(message.deleted for message in messages))

    async def test_collective_gif_streak_threshold_can_be_three(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 790 + index) for index in range(3)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_config(guild.id, "gif", consecutive_threshold=3)
        self.assertTrue(ok)

        messages = []
        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors[0], "https://tenor.com/view/g1"),
                (authors[1], "https://tenor.com/view/g2"),
                (authors[2], "https://tenor.com/view/g3"),
            ):
                message = FakeMessage(guild=guild, channel=channel, author=author, content=content)
                messages.append(message)
                decisions.append(await self.service.handle_message(message))

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("trigger mode: collective streak", (final.alert_evidence_summary or "").lower())
        self.assertIn("3 consecutive gif-heavy messages", (final.alert_evidence_summary or "").lower())
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 3)
        self.assertTrue(all(message.deleted for message in messages))

    async def test_collective_gif_streak_can_span_large_time_gaps(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 795 + index) for index in range(3)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_config(guild.id, "gif", consecutive_threshold=3, window_seconds=3, message_threshold=6)
        self.assertTrue(ok)

        messages = []
        with patch("babblebox.shield_service.time.monotonic", side_effect=(100.0, 124.0, 148.0)):
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions = []
                for author, content in (
                    (authors[0], "https://tenor.com/view/g1"),
                    (authors[1], "https://tenor.com/view/g2"),
                    (authors[2], "https://tenor.com/view/g3"),
                ):
                    message = FakeMessage(guild=guild, channel=channel, author=author, content=content)
                    messages.append(message)
                    decisions.append(await self.service.handle_message(message))

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("trigger mode: collective streak", (final.alert_evidence_summary or "").lower())
        self.assertIn("3 consecutive gif-heavy messages", (final.alert_evidence_summary or "").lower())

    async def test_collective_gif_streak_deletes_exact_suffix_and_preserves_unrelated_text(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 798 + index) for index in range(4)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_config(
            guild.id,
            "gif",
            consecutive_threshold=3,
            message_threshold=10,
            ratio_percent=95,
        )
        self.assertTrue(ok)

        messages = [
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/early-g1"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="https://tenor.com/view/early-g2"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="actual release update with enough words"),
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/streak-g1"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="https://tenor.com/view/streak-g2"),
            FakeMessage(guild=guild, channel=channel, author=authors[3], content="https://tenor.com/view/streak-g3"),
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertIn("trigger mode: collective streak", (final.alert_evidence_summary or "").lower())
        self.assertEqual(final.deleted_count, 3)
        self.assertFalse(messages[0].deleted)
        self.assertFalse(messages[1].deleted)
        self.assertFalse(messages[2].deleted)
        self.assertTrue(all(message.deleted for message in messages[3:]))

    async def test_personal_gif_rate_threshold_can_be_three(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 799)

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_config(guild.id, "gif", message_threshold=3)
        self.assertTrue(ok)

        final = None
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for content in (
                "https://tenor.com/view/g1",
                "https://tenor.com/view/g2",
                "https://tenor.com/view/g3",
            ):
                final = await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content=content)
                )

        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertIn("spam_gif_flood", {reason.match_class for reason in final.reasons})
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 3)

    async def test_multi_user_gif_pressure_is_detected_without_needing_one_spammer(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [
            FakeAuthor(801 + index, roles=[FakeRole(11, position=1)])
            for index in range(3)
        ]

        await self._enable_gif_pack(guild.id)

        messages = []
        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors[0], "https://tenor.com/view/cat-dance-gif-1000"),
                (authors[1], "https://tenor.com/view/cat-dance-gif-1000"),
                (authors[2], "https://tenor.com/view/cat-dance-gif-1000"),
                (authors[0], "https://tenor.com/view/hype-gif-2000"),
                (authors[1], "https://tenor.com/view/hype-gif-2000"),
                (authors[2], "https://tenor.com/view/hype-gif-2000"),
            ):
                message = FakeMessage(guild=guild, channel=channel, author=author, content=content)
                messages.append(message)
                decisions.append(await self.service.handle_message(message))

        final = decisions[4]
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertEqual(final.action, "delete_log")
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 5)
        self.assertFalse(final.timed_out)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("5 consecutive gif-heavy messages from 3 members", final.alert_evidence_summary.lower())
        self.assertIn("trigger mode: collective streak", final.alert_evidence_summary.lower())
        self.assertIn("channel-safe cleanup only", (final.action_note or "").lower())
        self.assertTrue(all(message.deleted for message in messages[:5]))
        self.assertFalse(messages[5].deleted)
        self.assertIsNone(decisions[5])

    async def test_captioned_collective_gif_run_still_hits_collective_streak(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 810 + index) for index in range(3)]

        await self._enable_gif_pack(guild.id)

        final = None
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors[0], "https://tenor.com/view/g1 that deploy landed clean"),
                (authors[1], "https://tenor.com/view/g2 logs finally look normal"),
                (authors[2], "https://tenor.com/view/g3 this release feels good"),
                (authors[0], "https://tenor.com/view/g4 shipping party time"),
                (authors[1], "https://tenor.com/view/g5 we can close the incident"),
            ):
                final = await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content=content)
                )

        self.assertIsNotNone(final)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("trigger mode: collective streak", (final.alert_evidence_summary or "").lower())
        self.assertIn("5 consecutive gif-heavy messages from 3 members", (final.alert_evidence_summary or "").lower())

    async def test_collective_gif_pressure_only_deletes_current_excess_posts(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 820 + index) for index in range(3)]
        messages = [
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/g1"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="solid deploy notes here"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="https://tenor.com/view/g2"),
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/g3"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="https://tenor.com/view/g4"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="https://tenor.com/view/g5"),
        ]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")
        ok, _ = await self.service.set_pack_config(guild.id, "gif", consecutive_threshold=6)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        self.assertEqual(decisions[:4], [None, None, None, None])
        first_excess = decisions[4]
        second_excess = decisions[5]
        self.assertIsNotNone(first_excess)
        self.assertIsNotNone(second_excess)
        self.assertTrue(messages[4].deleted)
        self.assertTrue(messages[5].deleted)
        self.assertFalse(any(message.deleted for message in messages[:4]))
        for decision in (first_excess, second_excess):
            self.assertEqual(decision.deleted_count, 1)
            self.assertEqual(decision.delete_attempt_count, 1)
            self.assertFalse(decision.timed_out)
            self.assertIn("trigger mode: collective pressure", (decision.alert_evidence_summary or "").lower())
            self.assertIn("1 substantive and 0 filler text messages", (decision.alert_evidence_summary or "").lower())
            self.assertIn("newest contributing gif posts", (decision.action_note or "").lower())

    async def test_slow_collective_gif_pressure_catches_mixed_text_takeover_without_single_spammer(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 830 + index) for index in range(3)]
        messages = [
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/g1"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="hi"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="https://tenor.com/view/g2"),
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/g3"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="okay"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="https://tenor.com/view/g4"),
        ]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")

        with patch("babblebox.shield_service.time.monotonic", side_effect=[100.0 + 10.0 * index for index in range(20)]):
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                decisions = [await self.service.handle_message(message) for message in messages]

        self.assertEqual(decisions[:5], [None, None, None, None, None])
        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "spam_group_gif_pressure")
        self.assertEqual({reason.match_class for reason in final.reasons}, {"spam_group_gif_pressure"})
        self.assertIn("trigger mode: collective pressure", (final.alert_evidence_summary or "").lower())
        self.assertIn("2 filler text messages", (final.alert_evidence_summary or "").lower())
        self.assertIn("preserved 2 filler text messages", (final.action_note or "").lower())
        self.assertFalse(final.timed_out)
        self.assertEqual(final.deleted_count, 1)
        self.assertFalse(messages[1].deleted)
        self.assertFalse(messages[4].deleted)
        self.assertTrue(messages[5].deleted)

    async def test_collective_gif_pressure_low_value_replies_do_not_hide_channel_takeover(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)

        await self._enable_gif_pack(guild.id)

        final = None
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for user_id, content in (
                (840, "https://tenor.com/view/g1"),
                (841, "lol"),
                (842, "https://tenor.com/view/g2"),
                (843, "nice"),
                (840, "https://tenor.com/view/g3"),
                (841, "haha"),
                (842, "https://tenor.com/view/g4"),
                (843, "yep"),
                (840, "https://tenor.com/view/g5"),
                (842, "https://tenor.com/view/g6"),
            ):
                final = await self.service.handle_message(
                    FakeMessage(
                        guild=guild,
                        channel=channel,
                        author=FakeAuthor(user_id, roles=[FakeRole(11, position=1)]),
                        content=content,
                    )
                )

        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "spam_group_gif_pressure")

    async def test_meaningful_text_balances_group_gif_pressure_and_keeps_fun_chat_safe(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)

        await self._enable_gif_pack(guild.id)

        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for user_id, content in (
                (850, "https://tenor.com/view/g1"),
                (851, "that deploy finally landed"),
                (852, "https://tenor.com/view/g2"),
                (853, "nice work team"),
                (850, "https://tenor.com/view/g3"),
                (851, "the rollback stayed clean"),
                (852, "https://tenor.com/view/g4"),
                (853, "great catch on the logs"),
                (850, "https://tenor.com/view/g5"),
                (852, "https://tenor.com/view/g6"),
            ):
                decisions.append(
                    await self.service.handle_message(
                        FakeMessage(
                            guild=guild,
                            channel=channel,
                            author=FakeAuthor(user_id, roles=[FakeRole(11, position=1)]),
                            content=content,
                        )
                    )
                )

        self.assertEqual([decision for decision in decisions if decision is not None], [])

    async def test_collective_gif_pressure_logs_group_once_per_channel(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        authors = [self._make_member(guild, 860 + index) for index in range(3)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")
        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_alert_role(guild.id, 777)
        self.assertTrue(ok)

        decisions = []
        for author, content in (
            (authors[0], "https://tenor.com/view/cat-dance-gif-1000"),
            (authors[1], "https://tenor.com/view/cat-dance-gif-1000"),
            (authors[2], "https://tenor.com/view/cat-dance-gif-1000"),
            (authors[0], "https://tenor.com/view/hype-gif-2000"),
            (authors[1], "https://tenor.com/view/hype-gif-2000"),
            (authors[2], "https://tenor.com/view/hype-gif-2000"),
        ):
            decisions.append(
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=public_channel, author=author, content=content)
                )
            )

        self.assertEqual(len(log_channel.sent), 1)
        self.assertEqual(len(log_channel.edits), 0)
        self.assertIsNone(log_channel.sent[0]["content"])
        embed = log_channel.sent[0]["embed"]
        self.assertIn("channel-wide pressure", (embed.description or "").lower())
        note_field = next(field for field in embed.fields if field.name == "Operational Note")
        self.assertIn("channel-safe cleanup only", note_field.value.lower())
        self.assertIn("exact 5-message live gif streak", note_field.value.lower())
        reason_field = next(field for field in embed.fields if field.name == "Reason")
        self.assertIn("trigger mode: collective streak", reason_field.value.lower())
        actionable = [decision for decision in decisions if decision is not None]
        self.assertEqual(len(actionable), 1)

    async def test_collective_gif_pressure_never_records_strikes_under_delete_escalate(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = [self._make_member(guild, 870 + index) for index in range(3)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_escalate")

        decisions = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors[0], "https://tenor.com/view/g1"),
                (authors[1], "https://tenor.com/view/g2"),
                (authors[2], "https://tenor.com/view/g3"),
                (authors[0], "https://tenor.com/view/g4"),
                (authors[1], "https://tenor.com/view/g5"),
                (authors[2], "https://tenor.com/view/g6"),
            ):
                decisions.append(
                    await self.service.handle_message(
                        FakeMessage(guild=guild, channel=channel, author=author, content=content)
                    )
                )

        actionable = [decision for decision in decisions if decision is not None]
        self.assertEqual(len(actionable), 1)
        final = actionable[0]
        self.assertIsNotNone(final)
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertFalse(final.timed_out)
        self.assertFalse(final.escalated)
        self.assertEqual(self.service._strike_windows, {})
        self.assertIn("channel-safe cleanup only", (final.action_note or "").lower())

    async def test_individual_gif_abuse_can_still_win_inside_collective_pressure(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        authors = {
            "heavy": self._make_member(guild, 880),
            "light_a": self._make_member(guild, 881),
            "light_b": self._make_member(guild, 882),
        }

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")

        final = None
        messages = []
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for author, content in (
                (authors["heavy"], "https://tenor.com/view/g1"),
                (authors["light_a"], "https://tenor.com/view/g2"),
                (authors["heavy"], "https://tenor.com/view/g1"),
                (authors["light_b"], "https://tenor.com/view/g4"),
                (authors["heavy"], "https://tenor.com/view/g1"),
            ):
                message = FakeMessage(guild=guild, channel=channel, author=author, content=content)
                messages.append(message)
                final = await self.service.handle_message(message)

        self.assertIsNotNone(final)
        self.assertEqual(final.reasons[0].match_class, "spam_gif_flood")
        self.assertIn("spam_group_gif_pressure", {reason.match_class for reason in final.reasons})
        self.assertIn("personal gif-abuse threshold", (final.action_note or "").lower())
        self.assertTrue(all(message.deleted for message in messages))

    async def test_gif_pack_exempt_messages_do_not_count_toward_collective_pressure(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        exempt_role = FakeRole(77, position=5)
        exempt_author = FakeAuthor(890, roles=[exempt_role])
        authors = [self._make_member(guild, 891 + index) for index in range(4)]

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_exemption(guild.id, "gif", "role", exempt_role.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "gif", consecutive_threshold=4)
        self.assertTrue(ok)

        messages = [
            FakeMessage(guild=guild, channel=channel, author=exempt_author, content="https://tenor.com/view/exempt"),
            FakeMessage(guild=guild, channel=channel, author=authors[0], content="https://tenor.com/view/g1"),
            FakeMessage(guild=guild, channel=channel, author=authors[1], content="https://tenor.com/view/g2"),
            FakeMessage(guild=guild, channel=channel, author=authors[2], content="https://tenor.com/view/g3"),
            FakeMessage(guild=guild, channel=channel, author=authors[3], content="https://tenor.com/view/g4"),
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        self.assertEqual(decisions[:4], [None, None, None, None])
        final = decisions[4]
        self.assertIsNotNone(final)
        self.assertEqual(final.deleted_count, 4)
        self.assertFalse(messages[0].deleted)
        self.assertTrue(all(message.deleted for message in messages[1:]))

    async def test_gif_pack_runs_even_when_spam_pack_is_disabled(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(901, roles=[FakeRole(11, position=1)])

        await self._enable_gif_pack(guild.id)
        ok, _ = await self.service.set_pack_config(guild.id, "spam", enabled=False)
        self.assertTrue(ok)

        final = None
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for _ in range(6):
                final = await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/cat-dance-gif-12345")
                )

        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "gif")
        self.assertEqual(final.reasons[0].match_class, "spam_gif_flood")

    async def test_personal_gif_pressure_never_exceeds_one_hundred_percent(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 903)

        await self._enable_gif_pack(guild.id)

        final = None
        with patch("babblebox.shield_service.time.monotonic", side_effect=(100.0, 104.0, 108.0, 112.0)):
            with patch.object(self.service, "_send_alert", new=AsyncMock()):
                for content in (
                    "https://tenor.com/view/cat-dance-gif-12345",
                    "https://tenor.com/view/cat-jump-gif-22345",
                    "https://tenor.com/view/cat-spin-gif-32345",
                    "https://tenor.com/view/cat-wave-gif-42345",
                ):
                    final = await self.service.handle_message(
                        FakeMessage(guild=guild, channel=channel, author=author, content=content)
                    )

        self.assertIsNotNone(final)
        self.assertIn("100% effective gif pressure", (final.alert_evidence_summary or "").lower())
        self.assertNotIn("400% effective gif pressure", (final.alert_evidence_summary or "").lower())

    async def test_gif_flood_stays_off_when_gif_pack_is_disabled_even_if_spam_is_on(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(902, roles=[FakeRole(11, position=1)])

        await self._enable_spam_pack(guild.id)
        ok, _ = await self.service.set_pack_config(guild.id, "gif", enabled=False)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/cat-dance-gif-12345")
                )
                for _ in range(6)
            ]

        self.assertEqual(decisions, [None, None, None, None, None, None])

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
                for _ in range(5)
            ]

        self.assertEqual(decisions[:3], [None, None, None])
        self.assertIsNotNone(decisions[3])
        self.assertEqual(decisions[3].action, "delete_log")
        self.assertIsNotNone(decisions[4])
        self.assertEqual(decisions[4].pack, "spam")
        self.assertEqual(decisions[4].action, "delete_escalate")
        self.assertTrue(decisions[4].deleted)
        self.assertTrue({reason.match_class for reason in decisions[4].reasons}.issuperset({"spam_duplicate", "spam_near_duplicate"}))

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

    async def test_emote_spam_toggle_defaults_off_then_detects_when_enabled(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        default_author = self._make_member(guild, 92)
        enabled_author = self._make_member(guild, 93)

        await self._enable_spam_pack(guild.id)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            default_flood = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=default_author, content="😀" * 20)
            )
        self.assertIsNone(default_flood)
        self.assertFalse(self.service.get_config(guild.id)["spam_emote_enabled"])

        ok, _ = await self.service.set_pack_config(guild.id, "spam", emote_enabled=True, emote_threshold=18)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            flood = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=enabled_author, content="😀" * 20)
            )
            normal = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=enabled_author, content="great round 😀😀😀")
            )

        self.assertIsNotNone(flood)
        self.assertEqual(flood.pack, "spam")
        self.assertIn("spam_emoji_flood", {reason.match_class for reason in flood.reasons})
        self.assertIsNone(normal)

    async def test_capitals_spam_toggle_defaults_off_then_detects_when_enabled(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        default_author = self._make_member(guild, 94)
        enabled_author = self._make_member(guild, 95)
        flood_text = "THIS CHAT IS ABSOLUTELY UNPLAYABLE RIGHT NOW PLEASE STOP SPAMMING"

        await self._enable_spam_pack(guild.id)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            default_caps = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=default_author, content=flood_text)
            )
        self.assertIsNone(default_caps)
        self.assertFalse(self.service.get_config(guild.id)["spam_caps_enabled"])

        ok, _ = await self.service.set_pack_config(guild.id, "spam", caps_enabled=True, caps_threshold=28)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            flood = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=enabled_author, content=flood_text)
            )
            normal = await self.service.handle_message(
                FakeMessage(guild=guild, channel=channel, author=enabled_author, content="THIS part is fine really.")
            )

        self.assertIsNotNone(flood)
        self.assertEqual(flood.pack, "spam")
        self.assertIn("spam_caps_flood", {reason.match_class for reason in flood.reasons})
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
        self.assertIn("spam_message_rate", {reason.match_class for reason in spam_decisions[-1].reasons})
        self.assertTrue(all(item is None for item in normal_decisions))

    async def test_custom_spam_threshold_profile_triggers_and_explains_itself(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 1040)

        await self._enable_spam_pack(
            guild.id,
            medium_action="delete_log",
            high_action="delete_log",
            message_threshold=4,
            window_seconds=8,
        )

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for text in ("lol", "bruh", "yo", "ok")
            ]

        final = decisions[-1]
        self.assertEqual(decisions[:3], [None, None, None])
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "spam")
        self.assertEqual(final.action, "delete_log")
        self.assertIn("threshold 4", final.reasons[0].reason.lower())
        self.assertIn("4 messages landed inside 8 seconds", final.alert_evidence_summary)

    async def test_delete_action_removes_full_spam_burst(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 1041)

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content=text)
            for text in ("lol", "lmao", "haha", "yo", "ok")
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        final = decisions[-1]
        self.assertEqual(decisions[:4], [None, None, None, None])
        self.assertIsNotNone(final)
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 5)
        self.assertEqual(final.delete_attempt_count, 5)
        self.assertIn("spam_burst", {reason.match_class for reason in final.reasons})
        self.assertTrue(all(message.deleted for message in messages))
        self.assertIn("full 5-message incident burst", final.action_note)

    async def test_duplicate_spam_delete_does_not_overdelete_unrelated_messages(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 1042)

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        healthy_messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content="setting up the queue now"),
            FakeMessage(guild=guild, channel=channel, author=author, content="drop your map votes in the thread"),
        ]
        spam_messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content="claim your starter pack now")
            for _ in range(4)
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in [*healthy_messages, *spam_messages]]

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "spam")
        self.assertEqual(final.deleted_count, 4)
        self.assertEqual(final.delete_attempt_count, 4)
        self.assertTrue(all(message.deleted for message in spam_messages))
        self.assertTrue(all(not message.deleted for message in healthy_messages))
        self.assertIn("spam_duplicate", {reason.match_class for reason in final.reasons})

    async def test_partial_spam_burst_delete_failure_is_reported_clearly(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 1043)

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        messages = [
            FakeMessage(guild=guild, channel=channel, author=author, content="lol"),
            FakeMessage(guild=guild, channel=channel, author=author, content="lmao", delete_error=RuntimeError("nope")),
            FakeMessage(guild=guild, channel=channel, author=author, content="haha"),
            FakeMessage(guild=guild, channel=channel, author=author, content="yo"),
            FakeMessage(guild=guild, channel=channel, author=author, content="ok"),
        ]

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [await self.service.handle_message(message) for message in messages]

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertTrue(final.deleted)
        self.assertEqual(final.deleted_count, 4)
        self.assertEqual(final.delete_attempt_count, 5)
        self.assertIn("some could not be removed", final.action_note.lower())
        self.assertEqual(messages[1].delete_attempts, 1)

    async def test_bot_turn_taking_game_loop_does_not_trigger_duplicate_spam(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        player = self._make_member(guild, 104)
        game_bot = self._make_member(guild, 105, bot=True)

        await self._enable_spam_pack(guild.id, sensitivity="high")

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for author, text in (
                    (player, "accepted"),
                    (game_bot, "round 1 locked"),
                    (player, "accepted"),
                    (game_bot, "round 2 locked"),
                    (player, "accepted"),
                    (game_bot, "round 3 locked"),
                )
            ]

        self.assertEqual(decisions, [None, None, None, None, None, None])

    async def test_short_human_turn_taking_loop_does_not_trigger_low_value_noise(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        player_one = self._make_member(guild, 106)
        player_two = self._make_member(guild, 107)

        await self._enable_spam_pack(guild.id, sensitivity="high")

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for author, text in (
                    (player_one, "cat"),
                    (player_two, "hat"),
                    (player_one, "bat"),
                    (player_two, "mat"),
                    (player_one, "rat"),
                    (player_two, "gnat"),
                )
            ]

        self.assertEqual(decisions, [None, None, None, None, None, None])

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

    async def test_custom_patterns_stay_saved_after_downgrade_but_only_free_subset_stays_active(self):
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
        for index in range(11):
            ok, result = await self.service.add_custom_pattern(
                10,
                label=f"Pattern {index}",
                pattern=f"gift-bait-{index}",
                mode="contains",
                action="log",
            )
            self.assertTrue(ok, result)

        self._attach_premium()
        config = self.service.get_config(10)
        compiled = self.service._compiled_configs[10]
        self.assertEqual(len(config["custom_patterns"]), 11)
        self.assertEqual(len(compiled.custom_patterns), 10)
        active_pattern_ids = {pattern.pattern_id for pattern in compiled.custom_patterns}
        self.assertNotIn(config["custom_patterns"][-1]["pattern_id"], active_pattern_ids)

        ok, message = await self.service.add_custom_pattern(
            10,
            label="Pattern 11",
            pattern="gift-bait-11",
            mode="contains",
            action="log",
        )
        self.assertFalse(ok)
        self.assertIn("active limit of 10", message)
        self.assertIn("/premium plans", message)
        self.assertIn("stays preserved", message)

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

    async def test_embed_only_no_link_money_wins_lure_is_scanned(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(420)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="",
            embeds=[
                FakeEmbed(
                    title="Who is active tonight",
                    description="Let's get it up to $2,700 tonight. Hit me up to get wins.",
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
        self.assertTrue([match for match in decision.reasons if match.match_class == "scam_dm_lure"])

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

    async def test_no_link_nitro_dm_lure_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "I'm giving away free nitro, DM me for more info")

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")
        self.assertIn(scam_matches[0].confidence, {"medium", "high"})

    async def test_no_link_crypto_dm_lure_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Free crypto drop, DM me now to claim it")

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")

    async def test_no_link_prize_dm_lure_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "$1000 giveaway, DM me for details")

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")

    async def test_no_link_expensive_item_dm_lure_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "I'm selling a new iPhone 17 Pro Max for a very cheap price, DM me now for more info",
        )

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")
        self.assertIn("Too-good-to-be-true", scam_matches[0].label)

    async def test_no_link_money_wins_lure_family_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Who is active let's get it up to $2,700 tonight Don't miss out Hit me up to get wins",
        )

        scam_matches = [match for match in result.matches if match.pack == "scam"]
        self.assertTrue(scam_matches)
        self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")
        self.assertEqual(scam_matches[0].label, "Money / wins DM lure")
        self.assertEqual(scam_matches[0].confidence, "high")

    async def test_no_link_money_and_betting_lure_corpus_is_caught(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        samples = (
            "Tap in for VIP picks tonight, DM me",
            "Message me for wins tonight",
            "Tap in for easy money tonight DM me",
            "w h o active lets get it to 2700 tonight hit me up",
            "h1t me up to get w1ns tonight",
            "d m me for w1ns tonight",
            "Who active\nlets get it up to $2,700 tonight\nhit me up to get wins \U0001F525\U0001F4B8",
            "Message me for premium picks tonight",
        )
        for text in samples:
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                scam_matches = [match for match in result.matches if match.pack == "scam"]
                self.assertTrue(scam_matches)
                self.assertEqual(scam_matches[0].match_class, "scam_dm_lure")

    async def test_short_no_link_wins_phrase_without_corroboration_stays_safe(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "Hit me up to get wins")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])

    async def test_benign_dm_chatter_does_not_false_positive_as_scam_lure(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(10, "DM me later about the build notes from tonight's game.")

        self.assertFalse([match for match in result.matches if match.pack == "scam"])

    async def test_benign_marketplace_discussion_does_not_false_positive_as_scam_lure(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        result = self.service.test_message_details(
            10,
            "Price check: what is the resale value on an iPhone 17 Pro Max right now? I saw someone selling one in the marketplace channel.",
        )

        self.assertFalse([match for match in result.matches if match.pack == "scam"])

    async def test_benign_sports_and_warning_corpus_stays_safe(self):
        ok, _ = await self.service.set_pack_config(10, "scam", enabled=True, action="delete_log", sensitivity="high")
        self.assertTrue(ok)

        safe_samples = (
            "Who is active tonight?",
            "Tonight's picks: Lakers -4 and Celtics ML.",
            "DM me if you want the sportsbook article link",
            "Don't miss out on our event tonight",
            "Scam warning: don't miss out + hit me up to get wins is a common lure",
            "Message me later about the lock settings",
        )
        for text in safe_samples:
            with self.subTest(text=text):
                result = self.service.test_message_details(10, text)
                self.assertFalse([match for match in result.matches if match.pack == "scam"])

    async def test_no_link_money_wins_lures_feed_fresh_campaign_tracking(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        messages = (
            "Who is active let's get it up to $2,700 tonight Hit me up to get wins",
            "Who is active let's get it up to $3,100 tonight Hit me up to get wins",
            "Who is active let's get it up to $4,200 tonight Hit me up to get wins",
        )
        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            for index, text in enumerate(messages, start=1):
                author = self._make_member(
                    guild,
                    7000 + index,
                    created_delta=timedelta(hours=2),
                    joined_delta=timedelta(minutes=10),
                )
                decision = await self.service.handle_message(
                    FakeMessage(
                        guild=guild,
                        channel=channel,
                        author=author,
                        content=text,
                    )
                )
                self.assertIsNotNone(decision)
                self.assertTrue([match for match in decision.reasons if match.match_class == "scam_dm_lure"])

        lure_rows = [
            rows
            for (guild_id, kind, _signature), rows in self.service._recent_scam_campaigns.items()
            if guild_id == guild.id and kind == "lure"
        ]
        self.assertTrue(lure_rows)
        self.assertGreaterEqual(max(len(rows) for rows in lure_rows), 2)

    async def test_pack_specific_member_exemption_blocks_scam_only(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 610)

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_exemption(guild.id, "scam", "user", author.id, True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(
                FakeMessage(
                    guild=guild,
                    channel=channel,
                    author=author,
                    content="I'm selling a new iPhone 17 Pro Max for a very cheap price, DM me now for more info",
                )
            )

        self.assertIsNone(decision)

    async def test_pack_specific_role_exemption_blocks_spam_only(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        exempt_role = FakeRole(611, position=5)
        author = self._make_member(guild, 6110, roles=[exempt_role])

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_exemption(guild.id, "spam", "role", exempt_role.id, True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(FakeMessage(guild=guild, channel=channel, author=author, content=text))
                for text in ("lol", "lmao", "haha", "yo", "ok")
            ]

        self.assertEqual(decisions, [None, None, None, None, None])

    async def test_pack_specific_channel_exemption_blocks_gif_only(self):
        guild = FakeGuild(10)
        channel = FakeChannel(620)
        author = self._make_member(guild, 6200)

        await self._enable_gif_pack(guild.id, medium_action="delete_log", high_action="delete_log")
        ok, _ = await self.service.set_pack_exemption(guild.id, "gif", "channel", channel.id, True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=author, content="https://tenor.com/view/cat-dance-gif-12345")
                )
                for _ in range(4)
            ]

        self.assertEqual(decisions, [None, None, None, None])

    async def test_pack_specific_spam_exemption_does_not_disable_scam_pack(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = self._make_member(guild, 6300)

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "spam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_exemption(guild.id, "spam", "user", author.id, True)
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(
                FakeMessage(
                    guild=guild,
                    channel=channel,
                    author=author,
                    content="Free nitro, DM me now to claim it",
                )
            )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.pack, "scam")

    async def test_moderators_are_exempt_from_spam_by_default(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        moderator = self._make_member(guild, 6400, manage_messages=True, moderate_members=True)

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=moderator, content="claim your starter pack now")
                )
                for _ in range(5)
            ]

        self.assertEqual(decisions, [None, None, None, None, None])

    async def test_moderator_delete_only_policy_caps_spam_without_timeout(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        moderator = self._make_member(guild, 6401, manage_messages=True, moderate_members=True)

        await self._enable_spam_pack(guild.id, medium_action="delete_log", high_action="delete_timeout_log")
        ok, _ = await self.service.set_pack_config(guild.id, "spam", moderator_policy="delete_only")
        self.assertTrue(ok)

        with patch.object(self.service, "_timeout_member", new=AsyncMock(return_value=True)) as timeout_mock, patch.object(
            self.service,
            "_send_alert",
            new=AsyncMock(),
        ):
            decisions = [
                await self.service.handle_message(
                    FakeMessage(guild=guild, channel=channel, author=moderator, content="claim your starter pack now")
                )
                for _ in range(5)
            ]

        final = decisions[-1]
        self.assertIsNotNone(final)
        self.assertEqual(final.pack, "spam")
        self.assertEqual(final.action, "delete_log")
        self.assertTrue(final.deleted)
        self.assertFalse(final.timed_out)
        timeout_mock.assert_not_awaited()

    async def test_other_packs_still_apply_to_moderators_by_default(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        moderator = self._make_member(guild, 6402, manage_messages=True, moderate_members=True)

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_config(guild.id, "scam", enabled=True, action="delete_log", sensitivity="normal")
        self.assertTrue(ok)

        with patch.object(self.service, "_send_alert", new=AsyncMock()):
            decision = await self.service.handle_message(
                FakeMessage(
                    guild=guild,
                    channel=channel,
                    author=moderator,
                    content="Free nitro, DM me now to claim it",
                )
            )

        self.assertIsNotNone(decision)
        self.assertEqual(decision.pack, "scam")

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

    async def test_forwarded_snapshot_no_link_money_wins_lure_is_scanned(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(4200)
        message = FakeMessage(
            guild=guild,
            channel=channel,
            author=author,
            content="fwd",
            message_snapshots=[
                FakeMessageSnapshot(
                    content="Who active? Let's get it up to $2,700 tonight. Hit me up to get wins.",
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
        self.assertTrue([match for match in decision.reasons if match.match_class == "scam_dm_lure"])

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
        note_field = next(field for field in embed.fields if field.name == "Note")
        self.assertIn("Combined suspicion around an unknown risky link.", evidence_field.value)
        self.assertIn("Primary risky domain: `secure-auth-session.click`", evidence_field.value)
        self.assertEqual(note_field.value, "Scam detection can still be wrong; recheck the context before taking action.")

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

    async def test_alert_embed_uses_polished_confidence_and_action_labels(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(43, roles=[FakeRole(11, position=1)])
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="lol")

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[guild.id]
        decision = ShieldDecision(
            matched=True,
            action="delete_timeout_log",
            pack="spam",
            reasons=(
                ShieldMatch(
                    pack="spam",
                    label="Fast burst posting",
                    reason="5 messages landed inside 10 seconds (threshold 5).",
                    action="delete_timeout_log",
                    confidence="high",
                    heuristic=True,
                    match_class="spam_burst",
                ),
            ),
            deleted=True,
            deleted_count=5,
            delete_attempt_count=5,
            timed_out=True,
        )
        fingerprint = self._content_fingerprint(message.content)

        await self.service._send_alert(message, compiled, decision, content_fingerprint=fingerprint)

        embed = log_channel.sent[0]["embed"]
        detection_field = next(field for field in embed.fields if field.name == "Detection")
        action_field = next(field for field in embed.fields if field.name == "Action")
        self.assertIn("Confidence: High confidence", detection_field.value)
        self.assertIn("Resolved action: Delete + Timeout + log", detection_field.value)
        self.assertIn("Deleted 5/5 messages", action_field.value)

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

    async def test_global_compact_logging_applies_across_shield_packs(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_delivery(guild.id, style="compact")
        self.assertTrue(ok)
        compiled = self.service._compiled_configs[guild.id]
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
        message = FakeMessage(guild=guild, channel=public_channel, author=author, content="Email me at friend@example.com")

        await self.service._send_alert(message, compiled, decision, content_fingerprint="privacy")

        self.assertEqual(len(log_channel.sent), 1)
        embed = log_channel.sent[0]["embed"]
        self.assertEqual(embed.title, "Shield Note | Privacy Leak")
        self.assertEqual([field.name for field in embed.fields], ["Detection", "Why it was noted", "Scan Source", "Preview", "Jump"])

    async def test_global_no_ping_mode_applies_across_shield_packs(self):
        guild = FakeGuild(10)
        public_channel = FakeChannel(20)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        author = FakeAuthor(42, roles=[FakeRole(11, position=1)])

        ok, _ = await self.service.set_log_channel(guild.id, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_alert_role(guild.id, 777)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_delivery(guild.id, ping_mode="never")
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

        await self.service._send_alert(message, compiled, decision, content_fingerprint="danger-no-ping")

        self.assertEqual(len(log_channel.sent), 1)
        self.assertIsNone(log_channel.sent[0]["content"])

    async def test_pack_log_override_inherits_and_isolates_correctly(self):
        ok, _ = await self.service.set_log_delivery(10, style="adaptive", ping_mode="smart")
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_log_override(10, "gif", style="compact", ping_mode="never")
        self.assertTrue(ok)

        compiled = self.service._compiled_configs[10]
        gif_delivery = compiled.resolved_log_delivery("gif")
        privacy_delivery = compiled.resolved_log_delivery("privacy")

        self.assertEqual((gif_delivery.style, gif_delivery.ping_mode), ("compact", "never"))
        self.assertEqual((privacy_delivery.style, privacy_delivery.ping_mode), ("adaptive", "smart"))

    async def test_support_guild_keeps_paid_ai_models_unlocked_without_bypassing_owner_policy(self):
        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)

        self.assertFalse(status["enabled"])
        self.assertEqual(status["policy_source"], "ordinary_global")
        self.assertTrue(status["premium_unlocked"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])
        self.assertEqual(status["configured_allowed_models"], ["gpt-5.4-nano"])
        self.assertFalse(status["configured_models_capped"])

    async def test_ordinary_guild_ai_policy_defaults_to_disabled_with_nano(self):
        status = self.service.get_ai_status(10)

        self.assertFalse(status["enabled"])
        self.assertEqual(status["policy_source"], "ordinary_global")
        self.assertFalse(status["premium_unlocked"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])
        self.assertEqual(status["configured_allowed_models"], ["gpt-5.4-nano"])
        self.assertEqual(status["premium_plan_code"], PLAN_FREE)
        self.assertEqual(status["premium_source"], "free")
        self.assertIn("owner enables it", status["status"].lower())

    async def test_ai_status_reports_capped_higher_tiers_when_guild_pro_is_inactive(self):
        self.service.ai_provider = FakeAIProvider(available=True)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini,full", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_guild_ai_access_policy(10, mode="enabled", allowed_models="nano,mini,full", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(10, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(10, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        status = self.service.get_ai_status(10)

        self.assertTrue(status["enabled"])
        self.assertTrue(status["ready_for_review"])
        self.assertEqual(status["status"], "Ready for second-pass review with the nano tier.")
        self.assertEqual(status["configured_allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])
        self.assertTrue(status["configured_models_capped"])
        self.assertEqual(status["premium_plan_code"], PLAN_FREE)
        self.assertEqual(status["premium_source"], "free")
        self.assertFalse(status["premium_stale"])
        self.assertFalse(status["premium_in_grace"])
        self.assertIn("Guild Pro is not active", status["premium_summary"])
        self.assertIn("higher-tier Shield AI settings stay configured", status["premium_summary"])

    async def test_ai_status_reports_stale_guild_pro_grace_window_honestly(self):
        self.service.ai_provider = FakeAIProvider(available=True)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        self._attach_premium(
            guild_plans={10: PLAN_GUILD_PRO},
            snapshots={
                10: {
                    "plan_code": PLAN_GUILD_PRO,
                    "active_plans": (PLAN_GUILD_PRO,),
                    "blocked": False,
                    "stale": True,
                    "in_grace": True,
                    "claim": {"source_kind": "entitlement", "owner_user_id": 42},
                    "system_access": False,
                    "system_access_scope": None,
                }
            },
        )
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(10, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(10, log_channel.id)
        self.assertTrue(ok)
        ok, _ = await self.service.set_ai_config(10, enabled_packs=["privacy"], min_confidence="high")
        self.assertTrue(ok)

        status = self.service.get_ai_status(10)

        self.assertTrue(status["enabled"])
        self.assertTrue(status["ready_for_review"])
        self.assertTrue(status["premium_unlocked"])
        self.assertFalse(status["configured_models_capped"])
        self.assertEqual(status["configured_allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])
        self.assertEqual(status["premium_plan_code"], PLAN_GUILD_PRO)
        self.assertEqual(status["premium_source"], "claim:entitlement")
        self.assertTrue(status["premium_stale"])
        self.assertTrue(status["premium_in_grace"])
        self.assertIn("last verified Guild Pro entitlement", status["premium_summary"])
        self.assertIn("grace window", status["premium_summary"])

    async def test_ai_status_reports_log_channel_blocker_when_policy_and_provider_are_ready(self):
        self.service.ai_provider = FakeAIProvider(available=True)
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(SHIELD_AI_ALLOWED_GUILD_ID, True)
        self.assertTrue(ok)

        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)

        self.assertFalse(status["ready_for_review"])
        self.assertIn("log channel", status["status"].lower())
        self.assertIn("delivery lane", status["setup_blockers"][0].lower())

    async def test_ai_status_reports_ready_once_local_delivery_lane_is_configured(self):
        self.service.ai_provider = FakeAIProvider(available=True)
        log_channel = FakeChannel(99, name="shield-log")
        self.bot.register_channel(log_channel)
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_module_enabled(SHIELD_AI_ALLOWED_GUILD_ID, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_log_channel(SHIELD_AI_ALLOWED_GUILD_ID, log_channel.id)
        self.assertTrue(ok)

        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)

        self.assertTrue(status["ready_for_review"])
        self.assertEqual(status["status"], "Ready for second-pass review.")
        self.assertEqual(status["setup_blockers"], [])
        self.assertEqual(status["routing_strategy"], "routed_fast_complex")

    async def test_public_ai_config_only_changes_scope(self):
        ok, message = await self.service.set_ai_config(10, enabled=True)
        self.assertFalse(ok)
        self.assertIn("availability is resolved by owner policy", message.lower())
        self.assertIn("gpt-5.4-mini", message)

        ok, message = await self.service.set_ai_config(10, enabled_packs=["privacy", "adult", "severe"], min_confidence="medium")
        self.assertTrue(ok)
        self.assertIn("review scope", message.lower())
        self.assertIn("gpt-5.4-mini", message)
        self.assertIn("Adult Links + Solicitation", message)
        self.assertIn("Severe Harm / Hate", message)
        config = self.service.get_config(10)
        self.assertEqual(set(config["ai_enabled_packs"]), {"privacy", "adult", "severe"})
        self.assertEqual(config["ai_min_confidence"], "medium")

    async def test_guild_pro_unlocks_enhanced_ai_models_when_global_policy_allows_ordinary_guilds(self):
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini", actor_id=1266444952779620413)
        self.assertTrue(ok)

        ok, message = await self.service.set_ai_config(10, enabled_packs=["privacy"], min_confidence="high")

        self.assertTrue(ok, message)
        status = self.service.get_ai_status(10)
        self.assertTrue(status["enabled"])
        self.assertTrue(status["premium_unlocked"])
        self.assertEqual(status["policy_source"], "ordinary_global")
        self.assertEqual(status["configured_allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])

    async def test_support_guild_gets_enhanced_models_when_owner_policy_allows_them(self):
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini,full", actor_id=1266444952779620413)
        self.assertTrue(ok)

        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)

        self.assertTrue(status["enabled"])
        self.assertTrue(status["premium_unlocked"])
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4"])

    async def test_allowlist_limit_scales_with_guild_pro(self):
        for index in range(20):
            ok, result = await self.service.set_allow_entry(10, "allow_domains", f"free-{index}.example", True)
            self.assertTrue(ok, result)

        ok, message = await self.service.set_allow_entry(10, "allow_domains", "free-20.example", True)
        self.assertFalse(ok)
        self.assertIn("keep up to 20 entries in that allowlist", message)

        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
        ok, result = await self.service.set_allow_entry(10, "allow_domains", "free-20.example", True)
        self.assertTrue(ok, result)
        self.assertEqual(self.service.allowlist_limit(10), 50)

    async def test_per_guild_override_can_enable_ai_while_global_default_is_off(self):
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
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
        self.assertTrue(status["premium_unlocked"])
        self.assertEqual(status["policy_source"], "guild_override")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano", "gpt-5.4-mini"])

    async def test_per_guild_override_can_disable_ai_while_global_default_is_on(self):
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
        ok, _ = await self.service.set_guild_ai_access_policy(10, mode="disabled", actor_id=1266444952779620413)
        self.assertTrue(ok)

        status = self.service.get_ai_status(10)
        self.assertFalse(status["enabled"])
        self.assertTrue(status["premium_unlocked"])
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
        self.assertIn("inherits", message.lower())
        status = self.service.get_ai_status(SHIELD_AI_ALLOWED_GUILD_ID)
        self.assertFalse(status["enabled"])
        self.assertEqual(status["policy_source"], "ordinary_global")
        self.assertEqual(status["allowed_models"], ["gpt-5.4-nano"])

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

        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini", actor_id=1266444952779620413)
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
        self.service.ai_provider.review.assert_awaited_once()
        request = self.service.ai_provider.review.await_args.args[0]
        self.assertEqual(request.allowed_models, ("gpt-5.4-nano", "gpt-5.4-mini"))
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
        self.assertIn("Contact me at [EMAIL]", request.sanitized_content)
        self.assertIn("embeds", decision.scan_surface_labels)

    async def test_global_ordinary_policy_can_enable_nano_for_other_guilds_without_guild_pro(self):
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
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
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

        ok, _ = await self.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano", actor_id=1266444952779620413)
        self.assertTrue(ok)
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
        self.assertIsNone(decision.ai_review)
        self.assertEqual(len(log_channel.sent), 1)

    async def test_replace_pack_exemptions_replaces_sets_without_bleeding(self):
        ok, _ = await self.service.replace_pack_exemptions(10, "spam", "channel", [11, 12])
        self.assertTrue(ok)
        ok, _ = await self.service.replace_pack_exemptions(10, "spam", "role", [21])
        self.assertTrue(ok)
        ok, _ = await self.service.replace_pack_exemptions(10, "spam", "user", [31, 32])
        self.assertTrue(ok)

        config = self.service.get_config(10)

        self.assertEqual(config["pack_exemptions"]["spam"]["channel_ids"], [11, 12])
        self.assertEqual(config["pack_exemptions"]["spam"]["role_ids"], [21])
        self.assertEqual(config["pack_exemptions"]["spam"]["user_ids"], [31, 32])
        self.assertEqual(config["pack_exemptions"]["severe"]["channel_ids"], [])

    async def test_pack_exemptions_stay_saved_after_downgrade_but_runtime_only_uses_free_subset(self):
        self._attach_premium(guild_plans={10: PLAN_GUILD_PRO})
        ok, _ = await self.service.replace_pack_exemptions(10, "spam", "channel", list(range(100, 121)))
        self.assertTrue(ok)

        self._attach_premium()
        config = self.service.get_config(10)
        compiled = self.service._compiled_configs[10]
        self.assertEqual(len(config["pack_exemptions"]["spam"]["channel_ids"]), 21)
        self.assertEqual(len(compiled.pack_exemptions["spam"].channel_ids), 20)
        self.assertNotIn(120, compiled.pack_exemptions["spam"].channel_ids)

        ok, message = await self.service.replace_pack_exemptions(10, "spam", "channel", list(range(100, 122)))
        self.assertFalse(ok)
        self.assertIn("active limit of 20", message)
        self.assertIn("/premium plans", message)
        self.assertIn("stays preserved", message)

    async def test_pack_timeout_overrides_compile_with_global_fallback(self):
        ok, _ = await self.service.set_escalation(10, timeout_minutes=9)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_timeout_override(10, "spam", 25)
        self.assertTrue(ok)
        ok, _ = await self.service.set_link_policy_timeout_override(10, 14)
        self.assertTrue(ok)

        compiled = self.service._compiled_configs[10]

        self.assertEqual(compiled.timeout_minutes_for_pack("spam"), 25)
        self.assertEqual(compiled.timeout_minutes_for_pack("link_policy"), 14)
        self.assertEqual(compiled.timeout_minutes_for_pack("severe"), 9)

    async def test_pack_timeout_override_is_passed_into_timeout_actions(self):
        guild = FakeGuild(10)
        channel = FakeChannel(20)
        author = FakeAuthor(42, created_at=ge.now_utc() - timedelta(hours=3), joined_at=ge.now_utc() - timedelta(hours=1))
        message = FakeMessage(guild=guild, channel=channel, author=author, content="hello there")

        ok, _ = await self.service.set_module_enabled(guild.id, True)
        self.assertTrue(ok)
        ok, _ = await self.service.set_pack_timeout_override(guild.id, "spam", 27)
        self.assertTrue(ok)

        with (
            patch.object(
                self.service,
                "_collect_matches",
                return_value=[
                    ShieldMatch(
                        pack="spam",
                        label="Spam burst",
                        reason="Synthetic test hit",
                        action="delete_timeout_log",
                        confidence="high",
                        heuristic=True,
                        match_class="spam_message_rate",
                    )
                ],
            ),
            patch.object(self.service, "_timeout_member", new=AsyncMock(return_value=True)) as timeout_mock,
            patch.object(self.service, "_send_alert", new=AsyncMock()),
        ):
            await self.service.handle_message(message)

        self.assertTrue(timeout_mock.await_args)
        self.assertEqual(timeout_mock.await_args.kwargs["pack"], "spam")

    async def test_link_policy_timeout_override_persists_separately_from_pack_timeouts(self):
        ok, _ = await self.service.set_pack_timeout_override(10, "scam", 18)
        self.assertTrue(ok)
        ok, _ = await self.service.set_link_policy_timeout_override(10, 12)
        self.assertTrue(ok)

        config = self.service.get_config(10)

        self.assertEqual(config["pack_timeout_minutes"]["scam"], 18)
        self.assertEqual(config["pack_timeout_minutes"]["link_policy"], 12)
        self.assertIsNone(config["pack_timeout_minutes"]["spam"])


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
            "version": 8,
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

    def test_normalize_state_preserves_gif_pack_fields(self):
        store = _MemoryShieldStore()
        snapshot = {
            "version": 8,
            "guilds": {
                "123": {
                    "gif_enabled": True,
                    "gif_action": "delete_escalate",
                    "gif_sensitivity": "high",
                }
            },
        }

        normalized = store.normalize_state(deepcopy(snapshot))
        config = normalized["guilds"]["123"]

        self.assertTrue(config["gif_enabled"])
        self.assertEqual(config["gif_low_action"], "log")
        self.assertEqual(config["gif_medium_action"], "delete_log")
        self.assertEqual(config["gif_high_action"], "delete_escalate")
        self.assertEqual(config["gif_sensitivity"], "high")

    def test_normalize_guild_shield_config_preserves_low_end_gif_and_spam_bounds(self):
        normalized = normalize_guild_shield_config(
            10,
            {
                "gif_consecutive_threshold": 3,
                "gif_message_threshold": 12,
                "gif_window_seconds": 3,
                "spam_message_window_seconds": 3,
            },
        )

        self.assertEqual(normalized["gif_consecutive_threshold"], 3)
        self.assertEqual(normalized["gif_message_threshold"], 12)
        self.assertEqual(normalized["gif_window_seconds"], 3)
        self.assertEqual(normalized["spam_message_window_seconds"], 3)
