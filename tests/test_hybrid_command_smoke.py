import asyncio
import os
import types
import unittest
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import discord
from discord import app_commands
from discord.ext import commands

from babblebox.app_command_hardening import harden_admin_root_group, harden_lock_root_group, harden_timeout_root_group
from babblebox import game_engine as ge
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore
from babblebox.command_utils import HybridPanelSendResult
from babblebox.cogs.admin import AdminCog
from babblebox.cogs.confessions import ConfessionsCog
from babblebox.cogs.gameplay import GameplayCog
from babblebox.cogs.identity import IdentityCog
from babblebox.cogs.meta import HELP_PAGES, MetaCog, build_help_embed, build_help_page_embed, build_support_embed
from babblebox.cogs.party_games import PartyGamesCog
from babblebox.cogs.premium import PremiumCog
from babblebox.cogs.question_drops import QuestionDropsCog
from babblebox.cogs.shield import ShieldCog, ShieldPanelView
from babblebox.cogs.utilities import AfkReturnWatchDurationSelect, UtilityCog
from babblebox.cogs.vote import VoteCog
from babblebox.premium_models import SYSTEM_PREMIUM_OWNER_USER_IDS
from babblebox.profile_service import ProfileService
from babblebox.profile_store import ProfileStore


class FakeMessage:
    _next_id = 1000

    def __init__(self, *, channel=None, message_id: Optional[int] = None, **kwargs):
        if message_id is None:
            message_id = FakeMessage._next_id
            FakeMessage._next_id += 1
        self.id = message_id
        self.channel = channel
        self.embed = kwargs.get("embed")
        self.view = kwargs.get("view")
        self.ephemeral = kwargs.get("ephemeral")
        self.content = kwargs.get("content")
        self.edit_calls = []
        self.delete_calls = []

    async def edit(self, **kwargs):
        self.edit_calls.append(kwargs)
        if "content" in kwargs:
            self.content = kwargs["content"]
        if "embed" in kwargs:
            self.embed = kwargs["embed"]
        if "view" in kwargs:
            self.view = kwargs["view"]
        return self

    async def delete(self, *, delay=None):
        self.delete_calls.append(delay)


class FakeInteractionCallbackResponse:
    def __init__(self, *, resource=None, message_id=None):
        self.resource = resource
        self.message_id = message_id


class FakeResponse:
    def __init__(self, interaction=None):
        self._interaction = interaction
        self._done = False
        self.send_calls = []
        self.edit_calls = []
        self.defer_calls = []
        self.modal_calls = []

    def is_done(self):
        return self._done

    async def send_message(self, *args, **kwargs):
        self.send_calls.append((args, kwargs))
        if self._interaction is None:
            self._done = True
            return FakeInteractionCallbackResponse()
        response = self._interaction.build_initial_response(kwargs)
        self._done = True
        return response

    async def edit_message(self, *args, **kwargs):
        if self._interaction is not None and getattr(self._interaction, "edit_exception", None) is not None:
            raise self._interaction.edit_exception
        self._done = True
        self.edit_calls.append((args, kwargs))
        if self._interaction is not None and getattr(self._interaction, "message", None) is not None:
            await self._interaction.message.edit(**kwargs)
        return FakeInteractionCallbackResponse(resource=getattr(self._interaction, "message", None))

    async def defer(self, *args, **kwargs):
        self.defer_calls.append((args, kwargs))
        if self._interaction is not None and getattr(self._interaction, "defer_exception", None) is not None:
            raise self._interaction.defer_exception
        self._done = True
        return FakeInteractionCallbackResponse(resource=getattr(self._interaction, "message", None))

    async def send_modal(self, modal):
        self._done = True
        self.modal_calls.append(modal)


class FakeInteraction:
    def __init__(
        self,
        *,
        expired: bool = False,
        user=None,
        guild=None,
        client=None,
        message=None,
        channel=None,
        initial_response_mode: str = "message",
        followup_mode: str = "message",
        initial_send_exception: Optional[Exception] = None,
        followup_exception: Optional[Exception] = None,
        original_response_exception: Optional[Exception] = None,
        edit_original_response_exception: Optional[Exception] = None,
        defer_exception: Optional[Exception] = None,
        edit_exception: Optional[Exception] = None,
    ):
        self.message = message
        self.channel = channel
        self.response = FakeResponse(self)
        self._expired = expired
        self.user = user or FakeAuthor()
        self.guild = guild
        self.client = client or types.SimpleNamespace(get_guild=lambda guild_id: guild)
        self.initial_response_mode = initial_response_mode
        self.followup_mode = followup_mode
        self.initial_send_exception = initial_send_exception
        self.followup_exception = followup_exception
        self.original_response_exception = original_response_exception
        self.edit_original_response_exception = edit_original_response_exception
        self.defer_exception = defer_exception
        self.edit_exception = edit_exception
        self.original_response_calls = []
        self._original_response_message = None
        self._last_followup_message = None
        self.followup_calls = []
        self.edit_original_response_calls = []
        self.followup = types.SimpleNamespace(send=self._followup_send)

    def is_expired(self):
        return self._expired

    def _register_message(self, message: FakeMessage):
        if self.channel is not None and hasattr(self.channel, "register_message"):
            self.channel.register_message(message)
        return message

    def create_message(self, payload: dict) -> FakeMessage:
        return self._register_message(FakeMessage(channel=self.channel, **payload))

    def build_initial_response(self, payload: dict) -> FakeInteractionCallbackResponse:
        if self.initial_send_exception is not None:
            raise self.initial_send_exception

        if self.initial_response_mode == "none":
            self._original_response_message = None
            return FakeInteractionCallbackResponse(resource=None, message_id=None)

        message = self.create_message(payload)
        self._original_response_message = message
        if self.initial_response_mode == "message":
            return FakeInteractionCallbackResponse(resource=message, message_id=message.id)
        if self.initial_response_mode == "message_id_only":
            return FakeInteractionCallbackResponse(resource=None, message_id=message.id)
        if self.initial_response_mode == "message_no_id":
            return FakeInteractionCallbackResponse(resource=message, message_id=None)
        if self.initial_response_mode == "resource_none_message_missing":
            self._original_response_message = None
            return FakeInteractionCallbackResponse(resource=None, message_id=message.id)
        raise AssertionError(f"Unknown initial_response_mode: {self.initial_response_mode}")

    async def _followup_send(self, *args, **kwargs):
        self.followup_calls.append((args, kwargs))
        if self.followup_exception is not None:
            raise self.followup_exception
        if self.followup_mode == "none":
            return None
        message = self.create_message(kwargs)
        self._last_followup_message = message
        return message if kwargs.get("wait") else None

    async def original_response(self):
        self.original_response_calls.append(None)
        if self.original_response_exception is not None:
            raise self.original_response_exception
        if self._original_response_message is None:
            raise discord.ClientException("Original response unavailable")
        return self._original_response_message

    async def edit_original_response(self, **kwargs):
        self.edit_original_response_calls.append(kwargs)
        if self.edit_original_response_exception is not None:
            raise self.edit_original_response_exception
        target = self._original_response_message or self.message
        if target is None:
            raise discord.ClientException("Original response unavailable")
        await target.edit(**kwargs)
        return target


class FakeGuildPermissions:
    administrator = False
    manage_guild = False


class FakeAuthor:
    def __init__(self, user_id: int = 1, *, manage_guild: bool = False):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions()
        self.guild_permissions.manage_guild = manage_guild


class FakeGuild:
    def __init__(self, guild_id: int = 10, *, members=None):
        self.id = guild_id
        self.name = "Guild"
        self._members = {member.id: member for member in (members or [])}

    def get_member(self, user_id: int):
        return self._members.get(user_id)


class FakeChannel:
    def __init__(self, channel_id: int = 20, *, allowed_user_ids=None):
        self.id = channel_id
        self.name = "general"
        self.mention = "#general"
        self._allowed_user_ids = set(allowed_user_ids or set())
        self._messages = {}

    def register_message(self, message: FakeMessage):
        self._messages[message.id] = message
        return message

    def get_partial_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            message = self.register_message(FakeMessage(channel=self, message_id=message_id))
        return message

    def permissions_for(self, member):
        allowed = not self._allowed_user_ids or getattr(member, "id", None) in self._allowed_user_ids
        return types.SimpleNamespace(
            view_channel=allowed,
            read_message_history=allowed,
            send_messages=allowed,
            embed_links=allowed,
        )


class ShieldPermissionSnapshot:
    def __init__(self, **overrides):
        defaults = {
            "view_channel": True,
            "send_messages": True,
            "embed_links": True,
            "manage_messages": True,
            "moderate_members": True,
        }
        defaults.update(overrides)
        for name, value in defaults.items():
            setattr(self, name, value)


class ShieldRole:
    def __init__(self, *, position: int):
        self.position = position

    def __ge__(self, other):
        return self.position >= getattr(other, "position", 0)


class ShieldAwareChannel(FakeChannel):
    def __init__(self, channel_id: int, *, name: str = "general", permissions: Optional[ShieldPermissionSnapshot] = None):
        super().__init__(channel_id)
        self.name = name
        self.mention = f"<#{channel_id}>"
        self._permissions = permissions or ShieldPermissionSnapshot()

    def permissions_for(self, member):
        return self._permissions


class ShieldAwareGuild(FakeGuild):
    def __init__(self, guild_id: int = 10, *, channels=None):
        super().__init__(guild_id)
        self.me = types.SimpleNamespace(id=999, top_role=ShieldRole(position=50))
        self._channels = {channel.id: channel for channel in (channels or [])}

    def get_channel(self, channel_id: int):
        return self._channels.get(channel_id)

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return None


class ShieldAwareBot:
    def __init__(self, guild: ShieldAwareGuild):
        self.loop = asyncio.get_running_loop()
        self.user = types.SimpleNamespace(id=999)
        self._guild = guild

    def get_guild(self, guild_id: int):
        if guild_id == self._guild.id:
            return self._guild
        return None

    def get_channel(self, channel_id: int):
        return self._guild.get_channel(channel_id)


class FakeContext:
    def __init__(
        self,
        *,
        interaction=None,
        author=None,
        guild=None,
        channel=None,
        message=None,
        send_exception: Optional[Exception] = None,
        send_exception_if_view: Optional[Exception] = None,
    ):
        self.interaction = interaction
        self.author = author or FakeAuthor()
        self.guild = guild
        self.channel = channel
        self.message = message
        self.send_calls = []
        self.defer_calls = []
        self.send_exception = send_exception
        self.send_exception_if_view = send_exception_if_view
        if self.interaction is not None:
            if getattr(self.interaction, "guild", None) is None:
                self.interaction.guild = guild
            if getattr(self.interaction, "channel", None) is None:
                self.interaction.channel = channel

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        if self.send_exception is not None:
            raise self.send_exception
        if self.send_exception_if_view is not None and kwargs.get("view") is not None:
            raise self.send_exception_if_view
        if self.interaction is None or self.interaction.is_expired():
            message = FakeMessage(channel=self.channel, **kwargs)
            if self.channel is not None and hasattr(self.channel, "register_message"):
                self.channel.register_message(message)
            return message

        if self.interaction.response.is_done():
            return await self.interaction.followup.send(**kwargs, wait=True)

        response = await self.interaction.response.send_message(**kwargs)
        if getattr(response, "resource", None) is not None:
            return response.resource
        return await self.interaction.original_response()

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        if self.interaction is not None:
            self.interaction.response._done = True
            if getattr(self.interaction, "_original_response_message", None) is None:
                self.interaction._original_response_message = self.interaction.create_message({"ephemeral": kwargs.get("ephemeral", False)})


class FakeLobbyView:
    def __init__(self, guild_id):
        self.guild_id = guild_id
        self.message = None


class HybridCommandSmokeTests(unittest.IsolatedAsyncioTestCase):
    def _patch_meta_global(self, name: str, value):
        return patch.dict(MetaCog.help_command.callback.__globals__, {name: value})

    def _service_env_patch(self):
        return patch.dict(
            os.environ,
            {
                "ADMIN_STORAGE_BACKEND": "memory",
                "SHIELD_STORAGE_BACKEND": "memory",
                "CONFESSIONS_STORAGE_BACKEND": "memory",
                "QUESTION_DROPS_STORAGE_BACKEND": "memory",
                "UTILITY_STORAGE_BACKEND": "memory",
                "PROFILE_STORAGE_BACKEND": "memory",
                "PREMIUM_STORAGE_BACKEND": "memory",
                "PREMIUM_SECRET_KEY": "p" * 32,
                "CONFESSIONS_CONTENT_KEY": "c" * 32,
                "CONFESSIONS_IDENTITY_KEY": "i" * 32,
            },
            clear=False,
        )

    async def _registered_root(self, cog_factory, *, root_name: str):
        with self._service_env_patch():
            bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
            try:
                cog = cog_factory(bot)
                await bot.add_cog(cog)
                command = next(command for command in bot.tree.get_commands() if command.name == root_name)
                return command, command.to_dict(bot.tree)
            finally:
                for loaded in list(bot.cogs.values()):
                    service = getattr(loaded, "service", None)
                    if service is not None:
                        await service.close()
                await bot.close()

    def _link_buttons(self, view) -> dict[str, str]:
        return {
            child.label: child.url
            for child in getattr(view, "children", [])
            if getattr(child, "style", None) == discord.ButtonStyle.link
        }

    def _sent_kwargs(self, ctx: FakeContext) -> dict:
        if ctx.send_calls:
            return ctx.send_calls[-1]
        interaction = getattr(ctx, "interaction", None)
        if interaction is not None:
            if interaction.followup_calls:
                return interaction.followup_calls[-1][1]
            if interaction.response.send_calls:
                return interaction.response.send_calls[-1][1]
        self.fail("No send payload recorded.")

    def _sent_view(self, ctx: FakeContext):
        return self._sent_kwargs(ctx)["view"]

    def _link_button_labels(self, view) -> list[str]:
        return [
            child.label
            for child in getattr(view, "children", [])
            if getattr(child, "style", None) == discord.ButtonStyle.link
        ]

    def _assert_embed_within_discord_limits(self, embed: discord.Embed):
        self.assertLessEqual(len(embed.title or ""), 256)
        self.assertLessEqual(len(embed.description or ""), 4096)
        self.assertLessEqual(len(embed.fields), 25)
        self.assertLessEqual(len(embed.footer.text or ""), 2048)

        total = len(embed.title or "") + len(embed.description or "") + len(embed.footer.text or "")
        for field in embed.fields:
            self.assertLessEqual(len(field.name), 256)
            self.assertLessEqual(len(field.value), 1024)
            total += len(field.name) + len(field.value)

        self.assertLessEqual(total, 6000)

    async def _build_admin_cog(self, guild: FakeGuild):
        bot = types.SimpleNamespace(
            loop=asyncio.get_running_loop(),
            user=types.SimpleNamespace(id=999),
            get_guild=lambda guild_id: guild if guild_id == guild.id else None,
            get_channel=lambda channel_id: None,
        )
        cog = AdminCog(bot)
        original_service = cog.service
        store = AdminStore(backend="memory")
        await store.load()
        cog.service = AdminService(bot, store=store)
        cog.service.storage_ready = True
        bot.admin_service = cog.service
        return cog, original_service

    def test_help_pages_reflect_five_game_party_copy(self):
        party_page = next(page for page in HELP_PAGES if page["title"] == "Party Games")
        self.assertIn("Broken Telephone", party_page["body"])
        self.assertIn("Exquisite Corpse", party_page["body"])
        self.assertIn("Spyfall", party_page["body"])
        self.assertIn("Word Bomb stays fast", party_page["body"])
        self.assertIn("private guesses with `/hunt guess`", party_page["body"])
        self.assertIn("Pattern holders need server DMs open before start", party_page["body"])
        self.assertIn("digits `0-9` only", party_page["body"])
        self.assertNotIn("Only 16", party_page["body"])

    def test_help_pages_reflect_question_drop_option_copy(self):
        question_drops_page = next(page for page in HELP_PAGES if page["title"] == "Question Drops")
        self.assertIn("/drops status", question_drops_page["body"])
        self.assertIn("/drops roles status", question_drops_page["body"])
        self.assertIn("/dropsadmin config", question_drops_page["body"])
        self.assertIn("/dropsadmin ping", question_drops_page["body"])
        self.assertNotIn("/drops panel", question_drops_page["body"])
        self.assertIn("/dropsadmin mastery category", question_drops_page["body"])
        self.assertNotIn("/drops mastery category", question_drops_page["body"])
        self.assertIn("difficulty profile", question_drops_page["body"])
        self.assertIn("Guild Pro", question_drops_page["body"])
        self.assertIn("template_action", question_drops_page["body"])
        self.assertIn("{user.mention}", question_drops_page["body"])
        self.assertIn("{category.name}", question_drops_page["body"])
        self.assertNotIn("category-template", question_drops_page["body"])
        self.assertIn("scholar ladder", question_drops_page["body"])
        daily_page = next(page for page in HELP_PAGES if page["title"] == "Daily Arcade")
        self.assertIn("Question Drops stay separate as the guild knowledge lane", daily_page["body"])

    def test_help_pages_include_support_links_page(self):
        support_index, support_page = next(
            (index, page) for index, page in enumerate(HELP_PAGES) if page["title"] == "Support / Links"
        )

        self.assertIn("/support", support_page["body"])
        self.assertIn("/premium plans", support_page["body"])
        self.assertIn("/premium subscribe", support_page["body"])
        self.assertIn("/premium status", support_page["body"])
        self.assertIn("discord.com/servers/inevitable-friendship-1322933864360050688", support_page["links"])
        self.assertIn("github.com/arno-create/babblebox-bot", support_page["links"])
        self.assertIn("arno-create.github.io/babblebox-bot/", support_page["links"])
        self.assertIn("patreon.com/cw/InevitableFriendship", support_page["links"])

        embed = build_help_page_embed(support_index)
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Links", fields)
        self.assertIn("GitHub Repository", fields["Links"])
        self.assertIn("Support Server", fields["Links"])
        self.assertIn("Official Website", fields["Links"])
        self.assertIn("Patreon Membership", fields["Links"])

    def test_help_pages_include_premium_lane(self):
        premium_index, premium_page = next(
            (index, page) for index, page in enumerate(HELP_PAGES) if page["title"] == "Premium / Plans"
        )

        self.assertIn("Buy on Patreon", premium_page["description"])
        self.assertIn("How Premium Activates", "\n".join(name for name, _value in premium_page["fields"]))
        self.assertIn("Patreon Tier Mapping", "\n".join(name for name, _value in premium_page["fields"]))
        self.assertIn("/premium subscribe", premium_page["try"])
        self.assertIn("/premium guild status", premium_page["try"])

        embed = build_help_page_embed(premium_index)
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Choose a Plan", fields)
        self.assertIn("How Premium Activates", fields)
        self.assertIn("Patreon Tier Mapping", fields)
        self.assertIn("Trust / Downgrade", fields)
        self.assertIn("Babblebox Plus maps to IF Epic Patron", fields["Patreon Tier Mapping"])
        self.assertIn("Refund outcomes follow Patreon or Apple policy", fields["Payment / Billing"])
        self.assertIn("Patreon refund help", fields["Payment / Billing"])
        self.assertIn("Downgrades or Guild Pro release do not delete saved", fields["Trust / Downgrade"])
        self.assertIn("extra runtime headroom simply pauses", fields["Trust / Downgrade"])

    def test_help_pages_keep_downgrade_truth_for_utilities_shield_and_confessions(self):
        utilities_page = next(page for page in HELP_PAGES if page["title"] == "Everyday Utilities")
        premium_page = next(page for page in HELP_PAGES if page["title"] == "Premium / Plans")
        shield_page = next(page for page in HELP_PAGES if page["title"] == "Shield / Admin Safety")

        self.assertIn("Babblebox Plus raises saved-vs-active headroom for Watch, reminders, and recurring AFK", utilities_page["body"])
        self.assertIn("gpt-5.4-nano", shield_page["body"])
        self.assertIn("effective higher-tier use still needs Guild Pro plus provider/runtime readiness", shield_page["body"])
        self.assertIn("effective lane plus local readiness, entitlement state, and provider gates", shield_page["body"])
        self.assertIn("larger safe Confessions image ceiling", premium_page["fields"][0][1])
        self.assertIn("extra runtime headroom simply pauses", premium_page["fields"][3][1])

    def test_help_surfaces_do_not_reference_removed_moment_feature(self):
        for page in HELP_PAGES:
            self.assertNotIn("/moment", page.get("body", ""))
            self.assertNotIn("/moment", page.get("try", ""))
            self.assertNotIn("Babblebox Moment", page.get("body", ""))

        compact_embed = build_help_embed()
        compact_text = "\n".join(
            [compact_embed.description or ""] + [field.value for field in compact_embed.fields if isinstance(field.value, str)]
        )
        self.assertNotIn("/moment", compact_text)
        self.assertNotIn("Babblebox Moment", compact_text)

    def test_help_surfaces_describe_shipped_shield_admin_flow(self):
        shield_page = next(page for page in HELP_PAGES if page["title"] == "Shield / Admin Safety")
        self.assertIn("/shield links", shield_page["body"])
        self.assertIn("/shield filters", shield_page["body"])
        self.assertIn("/shield severe category", shield_page["body"])
        self.assertIn("/shield severe term", shield_page["body"])
        self.assertIn("Trusted Links Only", shield_page["body"])
        self.assertIn("Anti-Spam", shield_page["body"])
        self.assertIn("GIF Flood / Media Pressure", shield_page["body"])
        self.assertIn("no-link DM-lure", shield_page["body"])
        self.assertIn("Severe Harm / Hate", shield_page["body"])
        self.assertIn("panel-first editor", shield_page["body"])
        self.assertIn("Actions, Options, or Exemptions", shield_page["body"])
        self.assertIn("dedicated timeout profile", shield_page["body"])
        self.assertNotIn("experimental scam heuristics", shield_page["body"])
        self.assertIn("/lock channel", shield_page["body"])
        self.assertIn("/lock remove", shield_page["body"])
        self.assertIn("/lock settings", shield_page["body"])
        self.assertIn("/timeout remove", shield_page["body"])
        self.assertIn("/admin permissions", shield_page["body"])
        self.assertNotIn("/admin risk", shield_page["body"])
        self.assertNotIn("/admin emergency", shield_page["body"])

        compact_embed = build_help_embed()
        shield_field = next(field for field in compact_embed.fields if field.name == "Shield / Admin")
        self.assertIn("/shield links", shield_field.value)
        self.assertIn("/shield filters", shield_field.value)
        self.assertIn("/shield severe category", shield_field.value)
        self.assertIn("/lock channel", shield_field.value)
        self.assertIn("/lock remove", shield_field.value)
        self.assertIn("/timeout remove", shield_field.value)
        self.assertIn("/admin followup", shield_field.value)
        self.assertIn("/admin logs", shield_field.value)
        self.assertIn("/admin exclusions", shield_field.value)
        self.assertIn("/admin permissions", shield_field.value)

    def test_help_embeds_stay_within_discord_limits(self):
        for index, _page in enumerate(HELP_PAGES):
            with self.subTest(page=index):
                self._assert_embed_within_discord_limits(build_help_page_embed(index))

    def test_compact_help_embed_stays_within_discord_limits(self):
        embed = build_help_embed()
        self._assert_embed_within_discord_limits(embed)
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Party Games", fields)
        self.assertIn("Question Drops", fields)
        self.assertIn("Premium", fields)
        self.assertIn("Support / Links", fields)
        self.assertIn("/premium plans", fields["Premium"])
        self.assertIn("/premium status", fields["Premium"])
        self.assertIn("/support", fields["Support / Links"])
        self.assertIn("/premium plans", fields["Support / Links"])

    def test_support_embed_highlights_premium_routes(self):
        embed = build_support_embed()
        fields = {field.name: field.value for field in embed.fields}

        self.assertIn("Babblebox Support", embed.title)
        self.assertIn("buying Babblebox premium", embed.description)
        self.assertEqual(tuple(fields.keys()), ("Official Links", "Premium", "Best Route"))
        self.assertIn("three combined Babblebox + Inevitable Friendship tiers", fields["Premium"])
        self.assertIn("/premium link", fields["Premium"])
        self.assertIn("/premium guild claim", fields["Premium"])
        self.assertIn("/premium refresh", fields["Premium"])
        self.assertIn("`Support Server` for live help, combined-tier questions", fields["Best Route"])
        self.assertIn("`Official Website` for the public premium guide", fields["Best Route"])

    def test_question_drops_help_page_is_split_across_multiple_fields(self):
        page_index = next(index for index, page in enumerate(HELP_PAGES) if page["title"] == "Question Drops")

        embed = build_help_page_embed(page_index)
        content_fields = [field for field in embed.fields if field.name not in {"Try", "Page", "Visibility", "Links"}]

        self.assertGreaterEqual(len(content_fields), 2)
        self.assertTrue(any(field.name == "Guild Lane" for field in content_fields))
        self.assertTrue(any(field.name == "Mastery / Scholar" for field in content_fields))
        self.assertTrue(all(len(field.value) <= 1024 for field in content_fields))

    def test_dropsadmin_config_slash_choices_include_difficulty_profiles(self):
        bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
        payload = QuestionDropsCog.dropsadmin_config_command.app_command.to_dict(bot.tree)

        difficulty_option = next(option for option in payload["options"] if option["name"] == "difficulty_profile")

        self.assertEqual(
            difficulty_option["choices"],
            [
                {"name": "Standard", "value": "standard"},
                {"name": "Smart", "value": "smart"},
                {"name": "Hard", "value": "hard"},
            ],
        )

    def test_question_drops_slash_tree_splits_public_and_admin_surfaces(self):
        cog = QuestionDropsCog(types.SimpleNamespace(loop=None))
        public_slash_names = {command.name for command in cog.drops_group.app_command.commands}
        prefix_alias_names = {command.name for command in cog.drops_group.commands}
        admin_slash_names = {command.name for command in cog.dropsadmin_group.app_command.commands}
        prefix_mastery_names = {command.name for command in cog.drops_mastery_group.commands}
        admin_slash_mastery_names = {command.name for command in cog.dropsadmin_mastery_group.app_command.commands}

        self.assertEqual(public_slash_names, {"leaderboard", "roles", "stats", "status"})
        self.assertTrue({"config", "channels", "categories", "digest", "mastery", "ping"}.issubset(prefix_alias_names))
        self.assertEqual(admin_slash_names, {"categories", "channels", "config", "digest", "mastery", "ping"})
        self.assertEqual(prefix_mastery_names, {"category", "recalc", "scholar"})
        self.assertEqual(admin_slash_mastery_names, {"category", "recalc", "scholar"})

    async def test_admin_roots_and_lock_root_emit_expected_hidden_guild_only_metadata(self):
        expected = {
            "admin": (AdminCog, int(discord.Permissions(manage_guild=True).value)),
            "lock": (AdminCog, None),
            "timeout": (AdminCog, None),
            "shield": (ShieldCog, int(discord.Permissions(manage_guild=True).value)),
            "confessions": (ConfessionsCog, int(discord.Permissions(manage_guild=True).value)),
            "dropsadmin": (QuestionDropsCog, int(discord.Permissions(manage_guild=True).value)),
            "bremind": (UtilityCog, int(discord.Permissions(manage_guild=True).value)),
        }

        for name, (cog_cls, default_permissions) in expected.items():
            with self.subTest(command=name):
                command, payload = await self._registered_root(cog_cls, root_name=name)

                if default_permissions is None:
                    self.assertIsNone(payload["default_member_permissions"])
                else:
                    self.assertEqual(payload["default_member_permissions"], default_permissions)
                self.assertEqual(payload["contexts"], [0])
                self.assertEqual(payload["integration_types"], [0])
                self.assertFalse(payload["dm_permission"])
                self.assertTrue(command.guild_only)
                self.assertTrue(command.allowed_contexts.guild)
                self.assertFalse(command.allowed_contexts.dm_channel)
                self.assertFalse(command.allowed_contexts.private_channel)
                self.assertTrue(command.allowed_installs.guild)
                self.assertFalse(command.allowed_installs.user)
                if name == "lock":
                    self.assertEqual({option["name"] for option in payload["options"]}, {"channel", "remove", "settings"})
                    settings_command = next(subcommand for subcommand in command.commands if subcommand.name == "settings")
                    settings_app_command = getattr(settings_command, "app_command", settings_command)
                    self.assertEqual(
                        int(settings_app_command.default_permissions.value),
                        int(discord.Permissions(manage_guild=True).value),
                    )
                if name == "timeout":
                    self.assertEqual({option["name"] for option in payload["options"]}, {"remove"})

    async def test_lock_root_keeps_expected_prefix_and_slash_children(self):
        cog = AdminCog(types.SimpleNamespace(loop=None))
        try:
            prefix_names = {command.name for command in cog.lock_group.commands}
            slash_names = {command.name for command in cog.lock_group.app_command.commands}
            timeout_prefix_names = {command.name for command in cog.timeout_group.commands}
            timeout_slash_names = {command.name for command in cog.timeout_group.app_command.commands}

            self.assertEqual(prefix_names, {"channel", "remove", "settings"})
            self.assertEqual(slash_names, {"channel", "remove", "settings"})
            self.assertEqual(timeout_prefix_names, {"remove"})
            self.assertEqual(timeout_slash_names, {"remove"})
        finally:
            await cog.service.close()

    async def test_public_member_roots_remain_visible(self):
        _, drops_payload = await self._registered_root(QuestionDropsCog, root_name="drops")
        _, confess_payload = await self._registered_root(ConfessionsCog, root_name="confess")

        self.assertIsNone(drops_payload["default_member_permissions"])
        self.assertIsNone(confess_payload["default_member_permissions"])
        self.assertEqual({option["name"] for option in drops_payload["options"]}, {"leaderboard", "roles", "stats", "status"})
        self.assertEqual(
            {option["name"] for option in confess_payload["options"]},
            {"about", "appeal", "create", "manage", "reply-to-user", "report"},
        )

    async def test_premium_root_keeps_public_status_commands_and_managed_guild_subgroup(self):
        command, payload = await self._registered_root(PremiumCog, root_name="premium")

        self.assertIsNone(payload["default_member_permissions"])
        self.assertEqual({option["name"] for option in payload["options"]}, {"status", "plans", "subscribe", "link", "refresh", "unlink", "guild"})

        guild_group = next(child for child in command.commands if child.name == "guild")
        guild_app_command = getattr(guild_group, "app_command", guild_group)
        self.assertTrue(guild_group.guild_only)
        self.assertEqual(int(guild_app_command.default_permissions.value), int(discord.Permissions(manage_guild=True).value))
        self.assertEqual({child.name for child in guild_group.commands}, {"status", "claim", "release"})

    async def test_premium_subscribe_command_opens_patreon_and_support_server(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_subscribe_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            self.assertTrue(payload["ephemeral"])
            self.assertEqual(payload["embed"].title, "Subscribe on Patreon")
            self.assertIn("Discord linking is the second step", payload["embed"].description)
            fields = {field.name: field.value for field in payload["embed"].fields}
            self.assertIn("Choose The Right Tier", fields)
            self.assertIn("Before You Buy", fields)
            self.assertIn("After You Buy", fields)
            self.assertIn("Need Help?", fields)
            self.assertIn("IF Epic Patron", fields["Choose The Right Tier"])
            self.assertIn("Refund outcomes follow Patreon or Apple policy", fields["Before You Buy"])
            self.assertIn("reportaproblem.apple.com", fields["Before You Buy"])
            self.assertIn("terms.html", fields["Before You Buy"])
            self.assertEqual(self._link_button_labels(payload["view"]), ["View Patreon", "Compare Plans", "Support Server"])
            self.assertEqual(
                self._link_buttons(payload["view"]),
                {
                    "View Patreon": "https://www.patreon.com/cw/InevitableFriendship",
                    "Compare Plans": "https://arno-create.github.io/babblebox-bot/help.html#premium",
                    "Support Server": "https://discord.com/servers/inevitable-friendship-1322933864360050688",
                },
            )
        finally:
            await cog.service.close()

    async def test_premium_status_not_linked_guides_subscribe_then_link(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_user_snapshot = lambda _user_id: {
                "plan_code": "free",
                "active_plans": (),
                "claimable_sources": (),
                "blocked": False,
                "stale": False,
                "system_access": False,
                "system_guild_claims": 0,
            }
            cog.service.get_link = lambda _user_id, provider=None: None
            cog.service.list_cached_claims_for_user = lambda _user_id: []
            limit_values = {
                "watch_keywords": 10,
                "watch_filters": 8,
                "reminders_active": 3,
                "reminders_public_active": 1,
                "afk_schedules": 6,
            }
            cog.service.resolve_user_limit = lambda _user_id, limit_key: limit_values[limit_key]
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_status_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            embed = payload["embed"]
            fields = {field.name: field.value for field in embed.fields}
            self.assertEqual(embed.title, "Premium Status")
            self.assertIn("Personal plan: **Free**", embed.description)
            self.assertIn("Patreon link: **Not linked**", embed.description)
            self.assertIn("/premium subscribe", fields["Next Step"])
            self.assertIn("/premium link", fields["Next Step"])
            self.assertEqual(self._link_button_labels(payload["view"]), ["View Patreon", "Compare Plans", "Support Server"])
        finally:
            await cog.service.close()

    async def test_premium_status_separates_free_personal_lane_from_claim_ready_guild_pro(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_user_snapshot = lambda _user_id: {
                "plan_code": "free",
                "active_plans": (),
                "claimable_sources": ({"source_kind": "entitlement", "source_id": "ent-1", "entitlement_id": "ent-1"},),
                "blocked": False,
                "stale": False,
                "system_access": False,
                "system_guild_claims": 0,
            }
            cog.service.get_link = lambda _user_id, provider=None: {
                "link_status": "active",
                "display_name": "Guild Pro Patron",
                "email": "guildpro@example.com",
                "metadata": {},
            }
            cog.service.list_cached_claims_for_user = lambda _user_id: []
            cog.service.resolve_user_limit = lambda _user_id, _limit_key: 10
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_status_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            embed = payload["embed"]
            fields = {field.name: field.value for field in embed.fields}
            self.assertIn("Personal plan: **Free**", embed.description)
            self.assertIn("Paid personal tier: **None**", fields["Current Access"])
            self.assertIn("Resolved Babblebox Guild Pro access: **Available to claim in a server**", fields["Current Access"])
            self.assertIn("server claim", fields["Status Notes"].lower())
            self.assertIn("/premium guild claim", fields["Next Step"])
        finally:
            await cog.service.close()

    async def test_premium_status_mentions_saved_state_after_personal_downgrade(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_user_snapshot = lambda _user_id: {
                "plan_code": "free",
                "active_plans": (),
                "claimable_sources": (),
                "blocked": False,
                "stale": False,
                "system_access": False,
                "system_guild_claims": 0,
            }
            cog.service.get_link = lambda _user_id, provider=None: None
            cog.service.list_cached_claims_for_user = lambda _user_id: []
            limit_values = {
                "watch_keywords": 10,
                "watch_filters": 8,
                "reminders_active": 3,
                "reminders_public_active": 1,
                "afk_schedules": 6,
            }
            cog.service.resolve_user_limit = lambda _user_id, limit_key: limit_values[limit_key]
            cog._utility_counts = lambda _user_id: {
                "watch_keywords": {"saved": 12, "active": 10},
                "watch_filters": {"saved": 13, "active": 11, "limit_style": "per_bucket"},
                "reminders": {"saved": 5, "active": 3},
                "public_reminders": {"saved": 2, "active": 1},
                "afk_schedules": {"saved": 8, "active": 6},
            }
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_status_command.callback(cog, ctx)

            fields = {field.name: field.value for field in self._sent_kwargs(ctx)["embed"].fields}
            self.assertIn("Watch keywords: saved **12** | active on this plan **10 / 10**", fields["Resolved Personal Limits"])
            self.assertIn("Watch filters: saved **13** | active on this plan **11** (limit **8** each bucket)", fields["Resolved Personal Limits"])
            self.assertIn("Active reminders: saved **5** | active on this plan **3 / 3**", fields["Resolved Personal Limits"])
            self.assertIn("Channel reminders: saved **2** | active on this plan **1 / 1**", fields["Resolved Personal Limits"])
            self.assertIn("Recurring AFK schedules: saved **8** | active on this plan **6 / 6**", fields["Resolved Personal Limits"])
            self.assertIn("stays preserved", fields["Resolved Personal Limits"])
        finally:
            await cog.service.close()

    async def test_premium_status_warns_when_linked_account_has_no_mapped_babblebox_tier(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_user_snapshot = lambda _user_id: {
                "plan_code": "free",
                "active_plans": (),
                "claimable_sources": (),
                "blocked": False,
                "stale": False,
                "system_access": False,
                "system_guild_claims": 0,
            }
            cog.service.get_link = lambda _user_id, provider=None: {
                "link_status": "active",
                "display_name": "Patreon Tester",
                "email": "tester@example.com",
                "metadata": {},
            }
            cog.service.list_cached_claims_for_user = lambda _user_id: []
            cog.service.resolve_user_limit = lambda _user_id, _limit_key: 10
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_status_command.callback(cog, ctx)

            fields = {field.name: field.value for field in self._sent_kwargs(ctx)["embed"].fields}
            self.assertIn("No mapped Babblebox tier detected yet", fields["Current Access"])
            self.assertIn("three combined tiers", fields["Status Notes"])
            self.assertIn("three combined tiers", fields["Next Step"])
        finally:
            await cog.service.close()

    async def test_premium_link_command_explains_linking_and_privacy_boundary(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.create_link_url = AsyncMock(return_value=(True, "https://example.com/patreon-link"))
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_link_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            fields = {field.name: field.value for field in payload["embed"].fields}
            self.assertEqual(payload["embed"].title, "Link Patreon to Babblebox")
            self.assertIn("does not buy the tier for you", payload["embed"].description)
            self.assertIn("What Linking Does", fields)
            self.assertIn("Use The Right Patreon Account", fields)
            self.assertIn("Privacy Boundary", fields)
            self.assertIn("Link Session", fields)
            self.assertEqual(self._link_button_labels(payload["view"]), ["Link Patreon", "Compare Plans", "Support Server"])
        finally:
            await cog.service.close()

    async def test_premium_refresh_without_link_routes_back_to_linking(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.refresh_user_link = AsyncMock(return_value=(False, "No Patreon account is linked for this Discord user."))
            cog.service.get_user_snapshot = lambda _user_id: {
                "plan_code": "free",
                "active_plans": (),
                "claimable_sources": (),
                "blocked": False,
                "stale": False,
                "system_access": False,
                "system_guild_claims": 0,
            }
            cog.service.get_link = lambda _user_id, provider=None: None
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_refresh_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            self.assertEqual(payload["embed"].title, "No Patreon Link")
            self.assertIn("Buy the Babblebox tier on Patreon first", payload["embed"].description)
            self.assertEqual(self._link_button_labels(payload["view"]), ["View Patreon", "Compare Plans", "Support Server"])
        finally:
            await cog.service.close()

    async def test_premium_unlink_success_keeps_saved_state_reassurance(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.unlink_user = AsyncMock(return_value=(True, "ok"))
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await PremiumCog.premium_unlink_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            self.assertEqual(payload["embed"].title, "Patreon Unlinked")
            self.assertIn("Saved Watch, reminder, AFK, Shield, and Confessions configuration stays preserved.", payload["embed"].description)
        finally:
            await cog.service.close()

    async def test_premium_guild_status_unclaimed_guides_claim_flow(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_guild_snapshot = lambda _guild_id: {
                "plan_code": "free",
                "active_plans": (),
                "blocked": False,
                "system_access": False,
                "stale": False,
                "claim": None,
            }
            cog.service.resolve_guild_limit = lambda _guild_id, _limit_key: 3
            cog.service.guild_has_capability = lambda _guild_id, _capability: False
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await PremiumCog.premium_guild_status_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            fields = {field.name: field.value for field in payload["embed"].fields}
            self.assertEqual(payload["embed"].title, "Guild Premium Status")
            self.assertIn("Claim state: **Unclaimed**", payload["embed"].description)
            self.assertIn("No Guild Pro claim is attached", fields["Claim Summary"])
            self.assertIn("/premium guild claim", fields["Next Step"])
        finally:
            await cog.service.close()

    async def test_guild_premium_status_mentions_saved_state_after_downgrade(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_guild_snapshot = lambda _guild_id: {
                "plan_code": "free",
                "active_plans": (),
                "blocked": False,
                "system_access": False,
                "stale": False,
                "claim": None,
            }
            cog.service.resolve_guild_limit = lambda _guild_id, limit_key: {
                "bump_detection_channels": 5,
                "shield_custom_patterns": 10,
                "confessions_max_images": 3,
            }[limit_key]
            cog.service.guild_has_capability = lambda _guild_id, _capability: False
            cog._guild_feature_counts = lambda _guild_id: {
                "bump_channels": {"saved": 6, "active": 5},
                "custom_patterns": {"saved": 11, "active": 10},
                "max_images": {"saved": 6, "active": 3},
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await PremiumCog.premium_guild_status_command.callback(cog, ctx)

            fields = {field.name: field.value for field in self._sent_kwargs(ctx)["embed"].fields}
            self.assertIn("Bump detection channels: saved **6** | active on this plan **5 / 5**", fields["What Guild Pro Changes Here"])
            self.assertIn("Shield advanced patterns: saved **11** | active on this plan **10 / 10**", fields["What Guild Pro Changes Here"])
            self.assertIn("Confession images: saved **6** | active on this plan **3 / 3**", fields["What Guild Pro Changes Here"])
            self.assertIn("stays preserved", fields["What Guild Pro Changes Here"])
        finally:
            await cog.service.close()

    async def test_premium_guild_claim_failure_explains_missing_guild_pro_source(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.claim_guild = AsyncMock(return_value=(False, "No unclaimed Guild Pro entitlement is available for this user."))
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await PremiumCog.premium_guild_claim_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            self.assertEqual(payload["embed"].title, "Guild Pro Claim")
            self.assertIn("Buy Babblebox Guild Pro on Patreon", payload["embed"].description)
        finally:
            await cog.service.close()

    async def test_premium_guild_release_success_preserves_saved_configuration_copy(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = PremiumCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.release_guild = AsyncMock(return_value=(True, "ok"))
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await PremiumCog.premium_guild_release_command.callback(cog, ctx)

            payload = self._sent_kwargs(ctx)
            self.assertEqual(payload["embed"].title, "Guild Pro Released")
            self.assertIn("preserved the saved server configuration", payload["embed"].description)
        finally:
            await cog.service.close()

    async def test_registered_tree_requires_instance_hardening_for_hybrid_root_visibility(self):
        class UnhardenedHybridRoot(commands.Cog):
            def __init__(self, bot: commands.Bot):
                self.bot = bot

            @app_commands.allowed_installs(guilds=True, users=False)
            @app_commands.guild_only()
            @app_commands.default_permissions(manage_guild=True)
            @commands.hybrid_group(name="opsbare", with_app_command=True, invoke_without_command=True)
            async def ops_group(self, ctx: commands.Context):
                return

        class HardenedHybridRoot(commands.Cog):
            def __init__(self, bot: commands.Bot):
                self.bot = bot
                harden_admin_root_group(self.ops_group)

            @app_commands.allowed_installs(guilds=True, users=False)
            @app_commands.guild_only()
            @app_commands.default_permissions(manage_guild=True)
            @commands.hybrid_group(name="opshardened", with_app_command=True, invoke_without_command=True)
            async def ops_group(self, ctx: commands.Context):
                return

        _, bare_payload = await self._registered_root(UnhardenedHybridRoot, root_name="opsbare")
        hardened_command, hardened_payload = await self._registered_root(HardenedHybridRoot, root_name="opshardened")

        self.assertIsNone(bare_payload["default_member_permissions"])
        self.assertEqual(hardened_payload["default_member_permissions"], 32)
        self.assertEqual(hardened_payload["contexts"], [0])
        self.assertEqual(hardened_payload["integration_types"], [0])
        self.assertTrue(hardened_command.guild_only)

    def test_telephone_lobby_copy_stays_aligned_with_manual(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            55: {
                "host": host,
                "players": [host, FakeAuthor(2), FakeAuthor(3)],
                "game_type": "telephone",
            }
        }
        try:
            embed = ge.get_lobby_embed(55)
        finally:
            ge.games = saved_games

        setup_field = next(field.value for field in embed.fields if field.name == "Telephone Setup")
        self.assertIn("Everyone gets a private DM turn.", setup_field)
        self.assertIn("Player 1 records the original clip", setup_field)
        self.assertIn("final player listens once before typing the guess", setup_field)

    def test_word_bomb_lobby_copy_surfaces_mode_and_speed_pressure(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            56: {
                "host": host,
                "players": [host, FakeAuthor(2)],
                "game_type": "bomb",
                "bomb_mode": "classic",
            }
        }
        try:
            embed = ge.get_lobby_embed(56)
        finally:
            ge.games = saved_games

        setup_field = next(field.value for field in embed.fields if field.name == "Word Bomb Setup")
        mode_field = next(field.value for field in embed.fields if field.name == "Bomb Mode")
        self.assertIn("Type one real English word that contains the live syllable.", setup_field)
        self.assertIn("fuse keeps shrinking", setup_field)
        self.assertIn("Classic", mode_field)

    def test_party_game_player_limits_are_game_specific(self):
        self.assertEqual(ge.get_game_player_limits("telephone"), (3, 25))
        self.assertEqual(ge.get_game_player_limits("spyfall"), (3, 25))
        self.assertEqual(ge.get_game_player_limits("bomb"), (2, 25))
        self.assertEqual(ge.get_game_player_limits("corpse"), (3, 6))
        self.assertEqual(ge.get_game_player_limits("pattern_hunt"), (3, 10))

    def test_exquisite_corpse_lobby_copy_surfaces_six_player_cap(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            57: {
                "host": host,
                "players": [FakeAuthor(i) for i in range(1, 8)],
                "game_type": "corpse",
            }
        }
        try:
            embed = ge.get_lobby_embed(57)
        finally:
            ge.games = saved_games

        setup_field = next(field.value for field in embed.fields if field.name == "Corpse Setup")
        players_field = next(field for field in embed.fields if field.name.startswith("Players"))
        self.assertIn("six hidden prompts", setup_field.casefold())
        self.assertIn("/6", players_field.name)
        self.assertIn("too many", players_field.value.casefold())

    def test_pattern_hunt_lobby_copy_surfaces_dm_requirement_and_private_guess_flow(self):
        saved_games = ge.games
        host = FakeAuthor(1)
        ge.games = {
            77: {
                "host": host,
                "players": [host, FakeAuthor(2), FakeAuthor(3)],
                "game_type": "pattern_hunt",
            }
        }
        try:
            embed = ge.get_lobby_embed(77)
        finally:
            ge.games = saved_games

        setup_field = next(field.value for field in embed.fields if field.name == "Pattern Hunt Setup")
        self.assertIn("Pattern holders need server DMs open before the room starts.", setup_field)
        self.assertIn("guesser asks the named holder", setup_field)
        self.assertIn("private theories stay in `/hunt guess`", setup_field)

    async def test_ping_command_responds_through_context_send(self):
        cog = MetaCog(object())
        ctx = FakeContext(interaction=FakeInteraction())

        await MetaCog.ping_command.callback(cog, ctx)

        self.assertEqual(ctx.defer_calls, [])
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_play_command_defers_before_sending_lobby(self):
        saved_games = ge.games
        ge.games = {}
        try:
            cog = GameplayCog(object())
            ctx = FakeContext(
                interaction=FakeInteraction(),
                author=FakeAuthor(),
                guild=FakeGuild(),
                channel=FakeChannel(),
            )

            with patch("babblebox.cogs.gameplay.require_channel_permissions", new=AsyncMock(return_value=True)), patch.object(
                ge,
                "create_game_state",
                return_value={"host": ctx.author, "channel": ctx.channel, "views": []},
            ), patch.object(ge, "LobbyView", FakeLobbyView), patch.object(ge, "get_lobby_embed", return_value=object()), patch.object(
                ge,
                "register_view",
            ) as register_view, patch.object(ge, "cleanup_game", new=AsyncMock()):
                await GameplayCog.play_command.callback(cog, ctx)

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            register_view.assert_called_once()
        finally:
            ge.games = saved_games

    async def test_play_command_blocks_same_channel_when_question_drop_is_live(self):
        saved_games = ge.games
        ge.games = {}
        try:
            bot = types.SimpleNamespace(
                question_drops_service=types.SimpleNamespace(storage_ready=True, has_live_drop=lambda guild_id, channel_id: True)
            )
            cog = GameplayCog(bot)
            ctx = FakeContext(
                interaction=FakeInteraction(),
                author=FakeAuthor(),
                guild=FakeGuild(),
                channel=FakeChannel(),
            )

            with patch("babblebox.cogs.gameplay.require_channel_permissions", new=AsyncMock(return_value=True)):
                await GameplayCog.play_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Question Drop", ctx.send_calls[0]["embed"].description)
        finally:
            ge.games = saved_games

    async def test_watch_mentions_storage_unavailable_still_responds(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = False
            cog.service.storage_error = "db down"
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await UtilityCog.watch_mentions_command.callback(cog, ctx, state="on", scope="server")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_watch_user_rejects_self_watch_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            author = FakeAuthor(user_id=7)
            guild = FakeGuild(members=[author])
            ctx = FakeContext(interaction=FakeInteraction(user=author, guild=guild), guild=guild, channel=FakeChannel(), author=author)

            await UtilityCog.watch_user_command.callback(cog, ctx, author, "6h")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("someone else", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_watch_user_rejects_bot_target_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            author = FakeAuthor(user_id=7)
            bot_target = types.SimpleNamespace(id=8, bot=True, display_name="Reminder Bot", mention="<@8>")
            guild = FakeGuild(members=[author])
            ctx = FakeContext(interaction=FakeInteraction(user=author, guild=guild), guild=guild, channel=FakeChannel(), author=author)

            await UtilityCog.watch_user_command.callback(cog, ctx, bot_target, "6h")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("bots", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_afk_return_duration_select_creates_user_watch(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = True
            watcher = FakeAuthor(user_id=11)
            target = FakeAuthor(user_id=12)
            target.bot = False
            guild = FakeGuild(10, members=[watcher, target])
            interaction = FakeInteraction(
                user=watcher,
                guild=guild,
                client=types.SimpleNamespace(get_guild=lambda guild_id: guild),
            )
            select = AfkReturnWatchDurationSelect(cog, guild_id=guild.id, target_user_id=target.id, target_name=target.display_name)
            select._values = ["6h"]

            await select.callback(interaction)

            self.assertEqual(len(interaction.response.edit_calls), 1)
            self.assertEqual(len(cog.service.store.state["return_watches"]), 1)
            record = next(iter(cog.service.store.state["return_watches"].values()))
            self.assertEqual(record["watcher_user_id"], watcher.id)
            self.assertEqual(record["target_type"], "user")
            self.assertEqual(record["target_id"], target.id)
        finally:
            await cog.service.close()

    async def test_remind_set_storage_unavailable_still_responds(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = UtilityCog(bot)
        try:
            cog.service.storage_ready = False
            cog.service.storage_error = "db down"
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await UtilityCog.remind_set_command.callback(cog, ctx, "10m", "dm", text="take a break")

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_renders_with_memory_profile_service(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx)

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_private_stays_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx, mode=None, visibility="private")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_group_storage_unavailable_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        try:
            cog.service.storage_ready = False
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_group.callback(cog, ctx, mode=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_open_state_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="emoji", guess=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_open_state_private_is_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="emoji", guess=None, visibility="private")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_success_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzles"]["shuffle"].answer
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess=answer, visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_prefix_guess_still_defaults_to_shuffle(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzles"]["shuffle"].answer
            ctx = FakeContext(guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode=answer, guess=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_retry_warning_stays_private_even_when_public_requested(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong", visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_play_final_failed_result_can_be_public_without_spoiler(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            answer = status["puzzle"].answer.upper()
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong one", visibility="public")
            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong two", visibility="public")
            await IdentityCog.daily_play_command.callback(cog, ctx, mode="shuffle", guess="wrong three", visibility="public")

            self.assertEqual(len(ctx.send_calls), 3)
            self.assertFalse(ctx.send_calls[2]["ephemeral"])
            self.assertNotIn(answer, ctx.send_calls[2]["embed"].description)
        finally:
            await cog.service.close()

    async def test_daily_stats_public_default_is_non_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_stats_command.callback(cog, ctx, user=None, visibility="public")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_stats_private_is_ephemeral(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_stats_command.callback(cog, ctx, user=None, visibility="private")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_public_panel_cooldown_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx_one = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
            ctx_two = FakeContext(interaction=FakeInteraction(), guild=ctx_one.guild, channel=ctx_one.channel, author=ctx_one.author)

            await IdentityCog.daily_group.callback(cog, ctx_one, mode=None, visibility="public")
            await IdentityCog.daily_group.callback(cog, ctx_two, mode=None, visibility="public")

            self.assertFalse(ctx_one.send_calls[0]["ephemeral"])
            self.assertTrue(ctx_two.send_calls[0]["ephemeral"])
            self.assertIn("cooldown", ctx_two.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_daily_share_invalid_public_request_does_not_consume_public_cooldown(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
            second_ctx = FakeContext(interaction=FakeInteraction(), guild=first_ctx.guild, channel=first_ctx.channel, author=first_ctx.author)
            status = await cog.service.get_daily_status(1)
            await cog.service.submit_daily_guess(1, status["puzzles"]["shuffle"].answer, mode="shuffle")

            await IdentityCog.daily_share_command.callback(cog, first_ctx, mode="emoji", visibility="public")
            await IdentityCog.daily_share_command.callback(cog, second_ctx, mode="shuffle", visibility="public")

            self.assertTrue(first_ctx.send_calls[0]["ephemeral"])
            self.assertFalse(second_ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_profile_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.profile_command.callback(cog, ctx, user=None, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_daily_share_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            status = await cog.service.get_daily_status(1)
            await cog.service.submit_daily_guess(1, status["puzzles"]["shuffle"].answer, mode="shuffle")
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.daily_share_command.callback(cog, ctx, mode="shuffle", visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_buddy_public_does_not_ephemeral_defer(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), get_user=lambda user_id: None)
        cog = IdentityCog(bot)
        memory_service = ProfileService(bot, store=ProfileStore(backend="memory"))
        try:
            await memory_service.start()
            cog.service = memory_service
            ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

            await IdentityCog.buddy_group.callback(cog, ctx, visibility="public")

            self.assertEqual(ctx.defer_calls, [])
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertFalse(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_help_public_sends_compact_embed_without_view(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, ctx, visibility="public")

        payload = self._sent_kwargs(ctx)
        self.assertEqual(ctx.defer_calls, [])
        self.assertEqual(len(interaction.response.send_calls), 1)
        self.assertEqual(interaction.followup_calls, [])
        self.assertFalse(payload["ephemeral"])
        self.assertIn("Babblebox Help", payload["embed"].title)
        self.assertNotIn("view", payload)

    async def test_help_private_sends_ephemeral_compact_embed_without_defer(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, ctx, visibility="private")

        payload = self._sent_kwargs(ctx)
        self.assertEqual(ctx.defer_calls, [])
        self.assertEqual(len(interaction.response.send_calls), 1)
        self.assertTrue(payload["ephemeral"])
        self.assertIn("Babblebox Help", payload["embed"].title)
        self.assertNotIn("view", payload)

    async def test_help_prefix_still_works_without_view(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        ctx = FakeContext(interaction=None, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, ctx, visibility="public")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertFalse(ctx.send_calls[0]["ephemeral"])
        self.assertNotIn("view", ctx.send_calls[0])
        self.assertIn("Babblebox Help", ctx.send_calls[0]["embed"].title)

    async def test_help_private_interaction_skips_channel_permission_gate(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        permission_check = AsyncMock(return_value=False)

        with self._patch_meta_global("require_channel_permissions", permission_check):
            await MetaCog.help_command.callback(cog, ctx, visibility="private")

        permission_check.assert_not_awaited()
        self.assertTrue(self._sent_kwargs(ctx)["ephemeral"])

    async def test_help_success_without_message_handle_does_not_trigger_recovery_and_consumes_cooldown(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        second_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=first_ctx.guild,
            channel=first_ctx.channel,
            author=first_ctx.author,
        )

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)), self._patch_meta_global(
            "send_hybrid_panel_response",
            AsyncMock(return_value=HybridPanelSendResult(delivered=True, path="context_send", message=None, handle_status="missing")),
        ):
            await MetaCog.help_command.callback(cog, first_ctx, visibility="public")

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, second_ctx, visibility="public")

        cooldown_payload = self._sent_kwargs(second_ctx)
        self.assertEqual(cooldown_payload["embed"].title, "Help Cooldown")
        self.assertTrue(cooldown_payload["ephemeral"])

    async def test_help_true_send_failure_returns_recovery_without_consuming_public_cooldown(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        second_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=first_ctx.guild,
            channel=first_ctx.channel,
            author=first_ctx.author,
        )

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)), self._patch_meta_global(
            "send_hybrid_panel_response",
            AsyncMock(return_value=HybridPanelSendResult(delivered=False, path="context_send", error=TypeError("broken"))),
        ):
            await MetaCog.help_command.callback(cog, first_ctx, visibility="public")

        self.assertEqual(self._sent_kwargs(first_ctx)["embed"].title, "Help Unavailable")
        self.assertTrue(self._sent_kwargs(first_ctx)["ephemeral"])

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, second_ctx, visibility="public")

        self.assertFalse(self._sent_kwargs(second_ctx)["ephemeral"])
        self.assertIn("Babblebox Help", self._sent_kwargs(second_ctx)["embed"].title)

    async def test_support_public_uses_view_and_public_visibility(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, ctx, visibility="public")

        payload = self._sent_kwargs(ctx)
        self.assertEqual(ctx.defer_calls, [])
        self.assertEqual(len(interaction.response.send_calls), 1)
        self.assertFalse(payload["ephemeral"])
        self.assertIn("Babblebox Support", payload["embed"].title)
        self.assertEqual(
            self._link_buttons(payload["view"]),
            {
                "Support Server": "https://discord.com/servers/inevitable-friendship-1322933864360050688",
                "Top.gg Vote": "https://top.gg/bot/1480903089518022739/vote",
                "GitHub Repository": "https://github.com/arno-create/babblebox-bot",
                "Official Website": "https://arno-create.github.io/babblebox-bot/",
                "Patreon Membership": "https://www.patreon.com/cw/InevitableFriendship",
            },
        )

    async def test_support_private_stays_ephemeral_without_defer(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, ctx, visibility="private")

        payload = self._sent_kwargs(ctx)
        self.assertEqual(ctx.defer_calls, [])
        self.assertTrue(payload["ephemeral"])
        self.assertIn("Babblebox Support", payload["embed"].title)

    async def test_support_prefix_still_works(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        ctx = FakeContext(interaction=None, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, ctx, visibility="public")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertFalse(ctx.send_calls[0]["ephemeral"])

    async def test_support_private_interaction_skips_channel_permission_gate(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        permission_check = AsyncMock(return_value=False)

        with self._patch_meta_global("require_channel_permissions", permission_check):
            await MetaCog.support_command.callback(cog, ctx, visibility="private")

        permission_check.assert_not_awaited()
        self.assertTrue(self._sent_kwargs(ctx)["ephemeral"])

    async def test_support_view_failure_falls_back_to_embed_without_view_and_consumes_cooldown(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        first_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=FakeGuild(),
            channel=FakeChannel(),
            author=FakeAuthor(),
            send_exception_if_view=TypeError("view broke"),
        )
        second_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=first_ctx.guild,
            channel=first_ctx.channel,
            author=first_ctx.author,
        )

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, first_ctx, visibility="public")
            await MetaCog.support_command.callback(cog, second_ctx, visibility="public")

        self.assertEqual(len(first_ctx.send_calls), 2)
        self.assertIsNotNone(first_ctx.send_calls[0]["view"])
        self.assertIsNone(first_ctx.send_calls[1].get("view"))
        self.assertIn("Babblebox Support", first_ctx.send_calls[1]["embed"].title)
        self.assertEqual(self._sent_kwargs(second_ctx)["embed"].title, "Support Cooldown")

    async def test_support_success_without_message_handle_does_not_trigger_recovery_and_consumes_cooldown(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        second_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=first_ctx.guild,
            channel=first_ctx.channel,
            author=first_ctx.author,
        )

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)), self._patch_meta_global(
            "send_hybrid_panel_response",
            AsyncMock(return_value=HybridPanelSendResult(delivered=True, path="context_send", message=None, handle_status="missing")),
        ):
            await MetaCog.support_command.callback(cog, first_ctx, visibility="public")

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, second_ctx, visibility="public")

        self.assertEqual(self._sent_kwargs(second_ctx)["embed"].title, "Support Cooldown")

    async def test_support_true_send_failure_returns_recovery_without_consuming_public_cooldown(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        first_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        second_ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=first_ctx.guild,
            channel=first_ctx.channel,
            author=first_ctx.author,
        )

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)), self._patch_meta_global(
            "send_hybrid_panel_response",
            AsyncMock(return_value=HybridPanelSendResult(delivered=False, path="context_send", error=TypeError("broken"))),
        ):
            await MetaCog.support_command.callback(cog, first_ctx, visibility="public")

        self.assertEqual(self._sent_kwargs(first_ctx)["embed"].title, "Support Panel Unavailable")
        self.assertTrue(self._sent_kwargs(first_ctx)["ephemeral"])

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, second_ctx, visibility="public")

        self.assertFalse(self._sent_kwargs(second_ctx)["ephemeral"])
        self.assertIn("Babblebox Support", self._sent_kwargs(second_ctx)["embed"].title)

    async def test_support_public_uses_separate_cooldown_from_help(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        help_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        support_ctx = FakeContext(interaction=FakeInteraction(), guild=help_ctx.guild, channel=help_ctx.channel, author=help_ctx.author)

        with self._patch_meta_global("require_channel_permissions", AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, help_ctx, visibility="public")
            await MetaCog.support_command.callback(cog, support_ctx, visibility="public")

        self.assertFalse(self._sent_kwargs(help_ctx)["ephemeral"])
        self.assertFalse(self._sent_kwargs(support_ctx)["ephemeral"])
        self.assertIn("Babblebox Help", self._sent_kwargs(help_ctx)["embed"].title)
        self.assertIn("Babblebox Support", self._sent_kwargs(support_ctx)["embed"].title)

    async def test_shield_status_is_private_for_admins(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_status_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
        finally:
            await cog.service.close()

    async def test_shield_status_denies_members_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await ShieldCog.shield_status_command.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_shield_ai_command_updates_scope_without_owner_access_toggle(self):
        bot = types.SimpleNamespace(
            loop=asyncio.get_running_loop(),
            premium_service=types.SimpleNamespace(
                guild_has_capability=lambda guild_id, capability: True,
                resolve_guild_limit=lambda guild_id, limit_key: 50,
                describe_limit_error=lambda **kwargs: "premium",
            ),
        )
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_ai_command.callback(
                cog,
                ctx,
                min_confidence="medium",
                privacy=True,
                promo=None,
                scam=None,
                adult=None,
                severe=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("review scope", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_utility_command_surface_excludes_removed_moment_feature(self):
        with self._service_env_patch():
            bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
            try:
                cog = UtilityCog(bot)
                await bot.add_cog(cog)

                slash_roots = {command.name for command in bot.tree.get_commands()}
                prefix_commands = {command.name for command in cog.walk_commands()}

                self.assertTrue({"watch", "later", "capture", "remind", "bremind"}.issubset(slash_roots))
                self.assertNotIn("moment", slash_roots)
                self.assertNotIn("moment", prefix_commands)
            finally:
                for loaded in list(bot.cogs.values()):
                    service = getattr(loaded, "service", None)
                    if service is not None:
                        await service.close()
                await bot.close()

    async def test_vote_root_moves_to_topgg_while_spyfall_vote_stays_available(self):
        with self._service_env_patch(), patch.dict(os.environ, {"TOPGG_STORAGE_BACKEND": "memory"}, clear=False):
            bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
            try:
                gameplay_cog = GameplayCog(bot)
                vote_cog = VoteCog(bot)
                await bot.add_cog(gameplay_cog)
                await bot.add_cog(vote_cog)

                slash_roots = {command.name: command for command in bot.tree.get_commands()}
                gameplay_prefix = {command.qualified_name for command in gameplay_cog.walk_commands()}

                self.assertIn("vote", slash_roots)
                self.assertIn("spyfall", slash_roots)
                self.assertEqual({command.name for command in gameplay_cog.spyfall_group.app_command.commands}, {"vote", "target"})
                self.assertIn("spyfall vote", gameplay_prefix)
                self.assertIn("spyfall target", gameplay_prefix)
                self.assertIn("vote", gameplay_prefix)
            finally:
                for loaded in list(bot.cogs.values()):
                    service = getattr(loaded, "service", None)
                    if service is not None:
                        await service.close()
                await bot.close()

    async def test_hunt_guess_uses_single_natural_theory_argument(self):
        bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
        try:
            party_cog = PartyGamesCog(bot)
            await bot.add_cog(party_cog)

            slash_roots = {command.name: command for command in bot.tree.get_commands()}
            hunt_root = slash_roots["hunt"]
            hunt_guess = next(command for command in hunt_root.commands if command.name == "guess")
            gameplay_prefix = {command.qualified_name for command in party_cog.walk_commands()}

            self.assertEqual([param.name for param in hunt_guess.parameters], ["theory"])
            self.assertIn("hunt guess", gameplay_prefix)
        finally:
            await bot.close()

    async def test_vote_embed_reports_disabled_state_without_sounding_like_premium(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = VoteCog(bot)
        try:
            snapshot = {
                "user_id": 1,
                "plan_code": "free",
                "plan_label": "Free",
                "configuration_state": "disabled",
                "configuration_message": "Top.gg vote bonuses are disabled until an operator explicitly sets `TOPGG_ENABLED=true` on this deployment.",
                "active": False,
                "eligible": True,
                "created_at": None,
                "expires_at": None,
                "weight": 1,
                "reminder_opt_in": False,
                "vote_url": "https://top.gg/bot/1480903089518022739/vote",
                "bonus_limits": {},
                "api_refresh_available": False,
                "timing_source": "exact",
                "timing_note": None,
            }

            embed = cog.build_vote_embed(snapshot)
            fields = {field.name: field.value for field in embed.fields}

            self.assertIn("topgg_enabled=true", embed.description.casefold())
            self.assertNotIn("premium", embed.description.casefold())
            self.assertIn("explicitly enabled", fields["Refresh"].casefold())
        finally:
            await cog.service.close()

    async def test_hidden_topgg_vote_admin_command_rejects_guild_invocation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = VoteCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=1266444952779620413, manage_guild=True),
            )

            await VoteCog.topggvote_admin_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is only available in DM.")
        finally:
            await cog.service.close()

    async def test_hidden_topgg_vote_admin_command_rejects_unauthorized_dm(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = VoteCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=None,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=777),
            )

            await VoteCog.topggvote_admin_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is unavailable.")
        finally:
            await cog.service.close()

    async def test_hidden_topgg_vote_admin_command_reports_global_and_user_status(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = VoteCog(bot)
        try:
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)
            cog.service.storage_ready = True
            cog.service.diagnostics_snapshot = lambda: {
                "enabled": True,
                "configuration_state": "configured",
                "configuration_message": "Top.gg vote bonuses are configured.",
                "webhook_mode": "v2",
                "storage_ready": True,
                "storage_backend": "memory",
                "runtime_attached": True,
                "public_routes_ready": True,
                "api_refresh_available": True,
                "refresh_cooldown_seconds": 60,
                "webhook_summary": {"status": "ready"},
            }
            cog.service.get_vote_record = lambda user_id: {
                "discord_user_id": int(user_id),
                "topgg_vote_id": "vote-5511",
                "created_at": "2026-04-24T10:00:00+00:00",
                "expires_at": "2026-04-24T22:00:00+00:00",
                "weight": 1,
                "reminder_opt_in": True,
                "webhook_status": "processed",
                "webhook_trace_id": "trace-5511",
            }
            cog.service.status_snapshot = lambda user_id: {
                "user_id": int(user_id),
                "plan_label": "Free",
                "configuration_state": "configured",
                "active": True,
                "eligible": True,
                "expires_at": "2026-04-24T22:00:00+00:00",
                "timing_source": "exact",
                "reminder_opt_in": True,
            }

            await VoteCog.topggvote_admin_command.callback(cog, ctx, "status")
            await VoteCog.topggvote_admin_command.callback(cog, ctx, "status", "user", "5511")

            self.assertEqual(len(ctx.send_calls), 2)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Top.gg Vote Owner Status")
            self.assertIn("Private maintainer status", ctx.send_calls[0]["embed"].description)
            self.assertIn("5511", ctx.send_calls[1]["embed"].fields[1].value)
            self.assertIn("2026-04-24T22:00:00+00:00", ctx.send_calls[1]["embed"].fields[1].value)
        finally:
            await cog.service.close()

    async def test_bremind_root_keeps_expected_admin_children(self):
        with self._service_env_patch():
            bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
            try:
                cog = UtilityCog(bot)
                await bot.add_cog(cog)

                slash_names = {command.name for command in cog.bremind_group.app_command.commands}
                prefix_names = {command.name for command in cog.bremind_group.commands}

                expected = {"status", "setup", "test", "enable", "disable", "detect", "destination", "message", "provider"}
                self.assertEqual(slash_names, expected)
                self.assertEqual(prefix_names, expected)
            finally:
                for loaded in list(bot.cogs.values()):
                    service = getattr(loaded, "service", None)
                    if service is not None:
                        await service.close()
                await bot.close()

    async def test_shield_panel_overview_reflects_legacy_pack_state(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            guild_id = 10
            cog.service.store.state["guilds"][str(guild_id)] = {
                "guild_id": guild_id,
                "packs": {
                    "privacy": {"enabled": True, "action": "delete_log", "sensitivity": "high"},
                    "promo": {"tracking": True},
                },
                "scam_enabled": True,
            }

            embed = cog.build_panel_embed(guild_id, "overview")
            pack_status_field = next(field for field in embed.fields if field.name == "Pack Status")
            link_policy_field = next(field for field in embed.fields if field.name == "Link Policy")

            self.assertIn("**Privacy Leak**: On | High | high Delete + log", pack_status_field.value)
            self.assertIn("**Promo / Invite**: On | Normal | high Log only", pack_status_field.value)
            self.assertIn("**Anti-Spam**: Off | Normal | high Log only", pack_status_field.value)
            self.assertIn("**GIF Flood / Media Pressure**: Off | Normal | high Log only", pack_status_field.value)
            self.assertIn("**Scam / Malicious Links**: On | Normal | high Log only", pack_status_field.value)
            self.assertIn("**Adult Links + Solicitation**: Off | Normal | high Log only", pack_status_field.value)
            self.assertIn("**Severe Harm / Hate**: Off | Normal | high Log only", pack_status_field.value)
            self.assertIn("Mode: **Default**", link_policy_field.value)
            self.assertIn("Strongest action: Log only", link_policy_field.value)
        finally:
            await cog.service.close()

    async def test_shield_panel_view_does_not_render_dead_owner_managed_button(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            view = ShieldPanelView(cog, guild_id=10, author_id=1)

            labels = [child.label for child in view.children if hasattr(child, "label")]
            self.assertEqual(labels, ["Overview", "Rules", "Links", "Scope", "AI", "Logs", "Refresh", "Enable Live Moderation"])
            self.assertNotIn("Owner-Managed Access", labels)
        finally:
            await cog.service.close()

    async def test_shield_ai_panel_embed_keeps_premium_policy_explanation_without_extra_control(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True

            embed = cog.build_panel_embed(10, "ai")
            fields = {field.name: field.value for field in embed.fields}

            self.assertEqual(embed.title, "Shield AI Assist")
            self.assertIn("owner policy controls availability", embed.description.lower())
            self.assertIn("Access Policy", fields)
            self.assertIn("Provider and Routing", fields)
            self.assertIn("Runtime Policy", fields)
            self.assertIn("Policy source", fields["Access Policy"])
            self.assertIn("Entitlement:", fields["Access Policy"])
            self.assertIn("Configured models", fields["Access Policy"])
            self.assertIn("Effective models right now", fields["Access Policy"])
            self.assertIn("Ordinary-guild default", fields["Access Policy"])
            self.assertIn("gpt-5.4-nano", fields["Access Policy"])
            self.assertIn("Babblebox Guild Pro can make", fields["Access Policy"])
            self.assertIn("review scope is admin-configurable", embed.footer.text.lower())
            self.assertIn("guild pro can make mini/full available", embed.footer.text.lower())
        finally:
            await cog.service.close()

    async def test_shield_overview_and_ai_panel_share_calm_capped_model_truth(self):
        premium_service = types.SimpleNamespace(
            guild_has_capability=lambda guild_id, capability: False,
            resolve_guild_limit=lambda guild_id, limit_key: 20,
            describe_limit_error=lambda **kwargs: "premium",
            get_guild_snapshot=lambda guild_id: {
                "plan_code": "free",
                "active_plans": (),
                "blocked": False,
                "stale": False,
                "in_grace": False,
                "claim": None,
                "system_access": False,
                "system_access_scope": None,
            },
        )
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), premium_service=premium_service)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.ai_provider = types.SimpleNamespace(
                diagnostics=lambda: {
                    "provider": "OpenAI",
                    "available": True,
                    "configured": True,
                    "model": "gpt-5.4-nano",
                    "routing_strategy": "routed_fast_complex",
                    "single_model_override": False,
                    "ignored_model_settings": [],
                    "fast_model": "gpt-5.4-nano",
                    "complex_model": "gpt-5.4-mini",
                    "top_model": "gpt-5.4",
                    "top_tier_enabled": False,
                    "timeout_seconds": 4.0,
                    "max_chars": 160,
                    "status": "Ready.",
                },
                close=AsyncMock(return_value=None),
            )
            ok, _ = await cog.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini", actor_id=1266444952779620413)
            self.assertTrue(ok)
            ok, _ = await cog.service.set_guild_ai_access_policy(10, mode="enabled", allowed_models="nano,mini", actor_id=1266444952779620413)
            self.assertTrue(ok)
            ok, _ = await cog.service.set_module_enabled(10, True)
            self.assertTrue(ok)

            overview = cog.build_panel_embed(10, "overview")
            ai_panel = cog.build_panel_embed(10, "ai")
            overview_value = next(field.value for field in overview.fields if field.name == "AI Assist")
            access_value = next(field.value for field in ai_panel.fields if field.name == "Access Policy")

            self.assertIn("Effective models right now:", overview_value)
            self.assertIn("nano (gpt-5.4-nano)", overview_value)
            self.assertIn("Policy source: Per-guild owner override", overview_value)
            self.assertNotIn("Configured models:", overview_value)
            self.assertNotIn("Entitlement:", overview_value)

            for text in (
                "Entitlement:",
                "Configured models:",
                "Effective models right now:",
                "higher-tier Shield AI settings stay configured",
            ):
                self.assertIn(text, access_value)
        finally:
            await cog.service.close()

    async def test_shield_ai_command_reports_configured_vs_effective_model_truth(self):
        premium_service = types.SimpleNamespace(
            guild_has_capability=lambda guild_id, capability: False,
            resolve_guild_limit=lambda guild_id, limit_key: 20,
            describe_limit_error=lambda **kwargs: "premium",
            get_guild_snapshot=lambda guild_id: {
                "plan_code": "free",
                "active_plans": (),
                "blocked": False,
                "stale": False,
                "in_grace": False,
                "claim": None,
                "system_access": False,
                "system_access_scope": None,
            },
        )
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), premium_service=premium_service)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ok, _ = await cog.service.set_ordinary_ai_policy(enabled=True, allowed_models="nano,mini", actor_id=1266444952779620413)
            self.assertTrue(ok)
            ok, _ = await cog.service.set_guild_ai_access_policy(10, mode="enabled", allowed_models="nano,mini", actor_id=1266444952779620413)
            self.assertTrue(ok)
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_ai_command.callback(
                cog,
                ctx,
                min_confidence="medium",
                privacy=True,
                promo=None,
                scam=None,
                adult=None,
                severe=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            embed = ctx.send_calls[0]["embed"]
            self.assertIn("Configured models:", embed.description)
            self.assertIn("Effective models right now:", embed.description)
            self.assertIn("higher-tier Shield AI settings stay configured", embed.description)
            self.assertIn("Shield AI review scope now uses `medium`", embed.description)
        finally:
            await cog.service.close()

    async def test_shield_panel_view_section_switching_keeps_compact_button_set(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            view = ShieldPanelView(cog, guild_id=10, author_id=1, section="overview")
            interaction = FakeInteraction(message=FakeMessage(channel=FakeChannel()))

            await view._switch_section(interaction, "ai")

            labels = [child.label for child in view.children if hasattr(child, "label")]
            self.assertEqual(view.section, "ai")
            self.assertEqual(labels, ["Overview", "Rules", "Links", "Scope", "AI", "Logs", "Refresh", "Enable Live Moderation"])
            self.assertEqual(interaction.message.embed.title, "Shield AI Assist")
            self.assertNotIn("Owner-Managed Access", labels)
        finally:
            await cog.service.close()

    async def test_shield_panel_warns_when_operability_is_missing(self):
        current_channel = ShieldAwareChannel(20, permissions=ShieldPermissionSnapshot(manage_messages=False, moderate_members=False))
        log_channel = ShieldAwareChannel(
            30,
            name="mod-logs",
            permissions=ShieldPermissionSnapshot(view_channel=False, send_messages=False, embed_links=False),
        )
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
                "scam_enabled": True,
                "scam_action": "timeout_log",
            }

            embed = cog.build_panel_embed(guild.id, "overview", channel_id=current_channel.id)
            operability = next(field for field in embed.fields if field.name == "Operability")

            self.assertIn("Manage Messages", operability.value)
            self.assertIn("Moderate Members", operability.value)
            self.assertIn("View Channel", operability.value)
            self.assertIn("Send Messages", operability.value)
            self.assertIn("Embed Links", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_logs_command_surfaces_log_channel_permission_warning(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs", permissions=ShieldPermissionSnapshot(send_messages=False))
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_logs_command.callback(cog, ctx, channel=log_channel, role=None, clear_channel=False, clear_role=False)

            self.assertEqual(len(ctx.send_calls), 1)
            operability = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Operability")
            self.assertIn("Send Messages", operability.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_stays_quiet_when_permissions_are_available(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs")
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "privacy_enabled": True,
                "privacy_action": "delete_log",
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="Email me at friend@example.com")

            self.assertEqual(len(ctx.send_calls), 1)
            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertNotIn("Operability", field_names)
        finally:
            await cog.service.close()

    async def test_shield_rules_command_supports_adult_pack(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_rules_command.callback(
                cog,
                ctx,
                pack="adult",
                enabled=True,
                action="delete_log",
                low_action=None,
                medium_action=None,
                high_action=None,
                sensitivity="normal",
                timeout_minutes=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Adult Links + Solicitation", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_shield_rules_command_accepts_adult_solicitation_toggle(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_rules_command.callback(
                cog,
                ctx,
                pack="adult",
                enabled=True,
                action=None,
                low_action="log",
                medium_action="delete_log",
                high_action="delete_log",
                sensitivity="normal",
                adult_solicitation=True,
                timeout_minutes=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Optional solicitation text detection is on", ctx.send_calls[0]["embed"].description)
            self.assertTrue(cog.service.get_config(10)["adult_solicitation_enabled"])
        finally:
            await cog.service.close()

    async def test_shield_module_command_updates_live_moderation_toggle(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_module_command.callback(cog, ctx, enabled=True)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Shield live-message moderation is now enabled", ctx.send_calls[0]["embed"].description)
            self.assertTrue(cog.service.get_config(10)["module_enabled"])
        finally:
            await cog.service.close()

    async def test_shield_escalation_command_updates_global_escalation_settings(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_escalation_command.callback(
                cog,
                ctx,
                threshold=4,
                window_minutes=12,
                timeout_minutes=9,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            description = ctx.send_calls[0]["embed"].description
            self.assertIn("Escalation now uses `4` hits in `12` minutes", description)
            self.assertIn("`9` minute timeout", description)
            config = cog.service.get_config(10)
            self.assertEqual(config["escalation_threshold"], 4)
            self.assertEqual(config["escalation_window_minutes"], 12)
            self.assertEqual(config["timeout_minutes"], 9)
        finally:
            await cog.service.close()

    async def test_shield_rules_command_redirects_global_timeout_updates_to_escalation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_rules_command.callback(
                cog,
                ctx,
                pack=None,
                timeout_minutes=9,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("/shield escalation timeout_minutes:...", ctx.send_calls[0]["embed"].description)
            self.assertEqual(cog.service.get_config(10)["timeout_minutes"], 10)
        finally:
            await cog.service.close()

    async def test_shield_links_command_returns_private_summary(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_links_command.callback(cog, ctx, mode=None, action=None, low_action=None, medium_action=None, high_action=None)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            embed = ctx.send_calls[0]["embed"]
            self.assertEqual(embed.title, "Shield Link Policy")
            self.assertIn("separate from Confessions link mode", embed.description)
            self.assertIn("Mode: **Default**", embed.fields[0].value)
        finally:
            await cog.service.close()

    async def test_shield_links_command_updates_policy(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_links_command.callback(
                cog,
                ctx,
                mode="trusted_only",
                action=None,
                low_action="log",
                medium_action="delete_log",
                high_action="delete_log",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Shield link policy is now **Trusted Links Only**", ctx.send_calls[0]["embed"].description)
            config = cog.service.get_config(10)
            self.assertEqual(config["link_policy_mode"], "trusted_only")
            self.assertEqual(config["link_policy_medium_action"], "delete_log")
            self.assertEqual(config["link_policy_high_action"], "delete_log")
        finally:
            await cog.service.close()

    async def test_shield_filters_command_supports_solicitation_channel_carve_out(self):
        current_channel = FakeChannel(20)
        carve_out_channel = FakeChannel(77)
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_filters_command.callback(
                cog,
                ctx,
                mode=None,
                target="adult_solicitation_excluded_channel_ids",
                state="on",
                channel=carve_out_channel,
                role=None,
                user=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("adult-solicitation carve-out channels", ctx.send_calls[0]["embed"].description)
            self.assertEqual(cog.service.get_config(10)["adult_solicitation_excluded_channel_ids"], [77])
        finally:
            await cog.service.close()

    async def test_shield_severe_category_command_updates_config(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_severe_category_command.callback(
                cog,
                ctx,
                category="self_harm_encouragement",
                state="off",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Self-Harm Encouragement", ctx.send_calls[0]["embed"].description)
            self.assertNotIn("self_harm_encouragement", cog.service.get_config(10)["severe_enabled_categories"])
        finally:
            await cog.service.close()

    async def test_shield_severe_term_command_updates_config(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_severe_term_command.callback(
                cog,
                ctx,
                action="add",
                phrase="you scumlord",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Custom severe term", ctx.send_calls[0]["embed"].description)
            self.assertIn("you scumlord", cog.service.get_config(10)["severe_custom_terms"])
        finally:
            await cog.service.close()

    async def test_shield_test_command_includes_link_decision_explanations(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="Free nitro https://dlscord-gift.com/claim")

            self.assertEqual(len(ctx.send_calls), 1)
            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertIn("Link Decisions", field_names)
            link_decisions_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Decisions")
            self.assertIn("dlscord-gift.com", link_decisions_field.value)
            self.assertIn("Review-only", link_decisions_field.value)
            self.assertNotIn("signals:", link_decisions_field.value)
            self.assertNotIn("bundled_malicious_domain", link_decisions_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_surfaces_no_link_money_wins_lure(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"]["10"] = {
                "guild_id": 10,
                "scam_enabled": True,
                "scam_action": "delete_log",
                "scam_sensitivity": "normal",
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(
                cog,
                ctx,
                text="Who is active let's get it up to $2,700 tonight. Hit me up to get wins.",
            )

            matches_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Matches")
            self.assertIn("Money / wins DM lure", matches_field.value)
            self.assertIn("No-link DM lure", matches_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_marks_lookup_candidates_as_review_only(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(
                cog,
                ctx,
                text="Visit https://wallet-bonus-drop.click/account?redirect=%2Flogin%2Fauth%2Ftoken%2Fseed to claim access.",
            )

            link_decisions_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Decisions")
            self.assertIn("Review-only", link_decisions_field.value)
            self.assertIn("no scam intent", link_decisions_field.value)
            self.assertNotIn("query_token", link_decisions_field.value)
            self.assertNotIn("provider_lookup", link_decisions_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_shows_ignored_bare_idna_candidate(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(
                cog,
                ctx,
                text="About me! Name apple, pronouns she/her. xn--jwh.xn--jj8c",
            )

            link_decisions_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Decisions")
            self.assertIn("Ignored", link_decisions_field.value)
            self.assertIn("explicit URL", link_decisions_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_surfaces_allow_phrase_bypass(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"]["10"] = {
                "guild_id": 10,
                "promo_enabled": True,
                "allow_phrases": ["join us here"],
            }
            ctx = FakeContext(guild=FakeGuild(10), channel=FakeChannel(), author=FakeAuthor(manage_guild=True))

            await cog.shield_test_command.callback(cog, ctx, text="join us here https://discord.gg/abc123")

            field_names = [field.name for field in ctx.send_calls[0]["embed"].fields]
            self.assertIn("Bypass", field_names)
            bypass_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Bypass")
            self.assertIn("allow phrase", bypass_field.value.lower())
        finally:
            await cog.service.close()

    async def test_shield_test_command_respects_solicitation_channel_carve_out(self):
        current_channel = ShieldAwareChannel(20)
        log_channel = ShieldAwareChannel(30, name="mod-logs")
        guild = ShieldAwareGuild(10, channels=[current_channel, log_channel])
        bot = ShieldAwareBot(guild)
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.store.state["guilds"][str(guild.id)] = {
                "guild_id": guild.id,
                "module_enabled": True,
                "log_channel_id": log_channel.id,
                "adult_enabled": True,
                "adult_action": "delete_log",
                "adult_solicitation_enabled": True,
                "adult_solicitation_excluded_channel_ids": [current_channel.id],
            }
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=guild,
                channel=current_channel,
                author=FakeAuthor(manage_guild=True),
            )

            await ShieldCog.shield_test_command.callback(cog, ctx, text="DM me for nudes")

            self.assertEqual(len(ctx.send_calls), 1)
            embed = ctx.send_calls[0]["embed"]
            bypass_field = next(field for field in embed.fields if field.name == "Bypass")
            result_field = next(field for field in embed.fields if field.name == "Result")
            self.assertIn("relaxes only the optional adult-solicitation detector", bypass_field.value)
            self.assertIn("No Shield pack matched", result_field.value)
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_owner_command_rejects_guild_invocation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=1266444952779620413, manage_guild=True),
            )

            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is only available in DM.")
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_owner_command_rejects_unauthorized_dm(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=None,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=777),
            )

            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is unavailable.")
        finally:
            await cog.service.close()

    async def test_hidden_shield_ai_owner_command_accepts_both_system_owner_ids(self):
        for owner_id in sorted(SYSTEM_PREMIUM_OWNER_USER_IDS):
            bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
            cog = ShieldCog(bot)
            try:
                cog.service.storage_ready = True
                ctx = FakeContext(
                    interaction=None,
                    guild=None,
                    channel=FakeChannel(),
                    author=FakeAuthor(user_id=owner_id),
                )

                await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "status")

                self.assertEqual(len(ctx.send_calls), 1)
                self.assertEqual(ctx.send_calls[0]["embed"].title, "Shield AI Owner Policy")
                status_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Default Owner Policy")
                self.assertIn("gpt-5.4-nano", status_field.value)
                self.assertIn("gpt-5.4-mini", status_field.value)
                self.assertIn("gpt-5.4", status_field.value)
            finally:
                await cog.service.close()

    async def test_hidden_shield_ai_owner_command_updates_global_and_guild_policy(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "status")
            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "global", "enable", "nano")
            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "guild", "10", "enable", "nano,mini")
            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "guild", "10", "inherit")
            await ShieldCog.shield_ai_owner_command.callback(cog, ctx, "support", "defaults")

            self.assertEqual(len(ctx.send_calls), 5)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Shield AI Owner Policy")
            self.assertIn("Private maintainer status", ctx.send_calls[0]["embed"].description)
            self.assertIn("enabled", ctx.send_calls[1]["embed"].description.lower())
            self.assertIn("guild shield ai access", ctx.send_calls[2]["embed"].description.lower())
            self.assertIn("inherits", ctx.send_calls[3]["embed"].description.lower())
            self.assertIn("inherits", ctx.send_calls[4]["embed"].description.lower())
            self.assertTrue(cog.service.get_meta()["ordinary_ai_enabled"])
        finally:
            await cog.service.close()

    async def test_drops_mastery_group_denies_non_admins_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await QuestionDropsCog.drops_mastery_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_dropsadmin_group_denies_non_admins_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=FakeInteraction(),
                guild=FakeGuild(),
                channel=FakeChannel(),
                author=FakeAuthor(manage_guild=False),
            )

            await QuestionDropsCog.dropsadmin_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    async def test_drops_roles_group_rejects_dm_invocation_privately(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(interaction=FakeInteraction(), guild=None, channel=FakeChannel(), author=FakeAuthor())

            await QuestionDropsCog.drops_roles_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("only works inside a server", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_drops_roles_group_sends_private_member_status(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_member_roles_status = AsyncMock(return_value={"preference": {"role_grants_enabled": True}, "held_records": []})
            cog.service.build_member_roles_status_embed = lambda guild, member, payload: discord.Embed(title="Question Drops Roles")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=7)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=7),
            )

            await QuestionDropsCog.drops_roles_group.callback(cog, ctx)

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops Roles")
            cog.service.get_member_roles_status.assert_awaited_once()
        finally:
            await cog.service.close()

    async def test_drops_roles_preference_command_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop(), profile_service=types.SimpleNamespace(storage_ready=True))
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.update_member_role_preference = AsyncMock(return_value={"mode": "stop", "before": {}, "after": {}})
            cog.service.build_member_role_preference_embed = lambda payload: discord.Embed(title="Question Drops Role Grants Off")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=8)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=8),
            )

            await QuestionDropsCog.drops_roles_preference_command.callback(
                cog,
                ctx,
                mode="stop",
                remove_current_roles=False,
                restore_current_roles=False,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops Role Grants Off")
            cog.service.update_member_role_preference.assert_awaited_once()
        finally:
            await cog.service.close()

    async def test_drops_mastery_category_template_action_edit_opens_modal_for_slash(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_category_mastery_announcement_status = AsyncMock(
                return_value={
                    "status": "ok",
                    "announcement_template": "Hello {user.mention}",
                    "placeholder_tokens": ("{user.mention}", "{user.name}", "{user.display_name}", "{role.name}", "{tier.label}", "{threshold}", "{category.name}"),
                }
            )
            guild = FakeGuild(10, members=[FakeAuthor(user_id=9, manage_guild=True)])
            interaction = FakeInteraction(guild=guild, user=FakeAuthor(user_id=9, manage_guild=True))
            ctx = FakeContext(
                interaction=interaction,
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=9, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                template_action="edit",
            )

            self.assertEqual(len(interaction.response.modal_calls), 1)
            modal = interaction.response.modal_calls[0]
            self.assertEqual(modal.title, "Edit Science Mastery Announcement")
            self.assertIn("{user.mention}", modal.template_input.placeholder)
            self.assertIn("{category.name}", modal.template_input.placeholder)
            self.assertNotIn("Plain text only", modal.template_input.placeholder)
            self.assertEqual(ctx.send_calls, [])
        finally:
            await cog.service.close()

    async def test_drops_mastery_scholar_template_action_edit_opens_tier_modal_for_slash(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.get_scholar_announcement_status = AsyncMock(
                return_value={
                    "status": "ok",
                    "announcement_template": "Hello {user.mention}",
                    "placeholder_tokens": ("{user.mention}", "{user.name}", "{user.display_name}", "{role.name}", "{tier.label}", "{threshold}"),
                }
            )
            guild = FakeGuild(10, members=[FakeAuthor(user_id=10, manage_guild=True)])
            interaction = FakeInteraction(guild=guild, user=FakeAuthor(user_id=10, manage_guild=True))
            ctx = FakeContext(
                interaction=interaction,
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=10, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_scholar_command.callback(
                cog,
                ctx,
                tier=2,
                template_action="edit",
            )

            self.assertEqual(len(interaction.response.modal_calls), 1)
            modal = interaction.response.modal_calls[0]
            self.assertEqual(modal.title, "Edit Scholar II Announcement")
            self.assertIn("{user.mention}", modal.template_input.placeholder)
            self.assertIn("{threshold}", modal.template_input.placeholder)
            self.assertNotIn("{category.name}", modal.template_input.placeholder)
        finally:
            await cog.service.close()

    async def test_drops_mastery_scholar_template_edit_requires_slash_for_prefix(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=10, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_scholar_command.callback(
                cog,
                ctx,
                enabled="edit",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("slash form", ctx.send_calls[0]["embed"].description.lower())
        finally:
            await cog.service.close()

    async def test_drops_mastery_category_template_clear_stays_private(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            cog.service.clear_category_mastery_announcement_template = AsyncMock(return_value=(True, "Science Tier II announcement override cleared."))
            cog.service.get_category_mastery_announcement_status = AsyncMock(
                return_value={"status": "ok", "title": "Science Tier II Announcement"}
            )
            cog.service.build_mastery_announcement_status_embed = lambda payload, note=None: discord.Embed(title="Science Tier II Announcement")
            guild = FakeGuild(10, members=[FakeAuthor(user_id=11, manage_guild=True)])
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild, user=FakeAuthor(user_id=11, manage_guild=True)),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=11, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                tier=2,
                template_action="clear",
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Science Tier II Announcement")
            cog.service.clear_category_mastery_announcement_template.assert_awaited_once_with(guild.id, category="science", tier=2)
        finally:
            await cog.service.close()

    async def test_drops_mastery_template_mode_rejects_mixed_role_fields(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            guild = FakeGuild(10, members=[FakeAuthor(user_id=12, manage_guild=True)])
            role = types.SimpleNamespace(id=222, mention="<@&222>")
            ctx = FakeContext(
                interaction=FakeInteraction(guild=guild, user=FakeAuthor(user_id=12, manage_guild=True)),
                guild=guild,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=12, manage_guild=True),
            )

            await QuestionDropsCog.drops_mastery_category_command.callback(
                cog,
                ctx,
                category="science",
                template_action="status",
                role=role,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertTrue(ctx.send_calls[0]["ephemeral"])
            self.assertIn("template mode only uses", ctx.send_calls[0]["embed"].description.casefold())
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_rejects_guild_invocation(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=FakeGuild(10),
                channel=FakeChannel(),
                author=FakeAuthor(user_id=1266444952779620413, manage_guild=True),
            )

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is only available in DM.")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_rejects_unauthorized_dm(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            ctx = FakeContext(
                interaction=None,
                guild=None,
                channel=FakeChannel(),
                author=FakeAuthor(user_id=777),
            )

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["content"], "That command is unavailable.")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_status_and_toggle_work_in_dm_for_owner(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "status")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "rare")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "event_only")
            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "off")

            self.assertEqual(len(ctx.send_calls), 4)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops AI Override")
            self.assertIn("Private maintainer status", ctx.send_calls[0]["embed"].description)
            self.assertIn("now `rare`", ctx.send_calls[1]["embed"].description.lower())
            self.assertIn("now `event_only`", ctx.send_calls[2]["embed"].description.lower())
            self.assertIn("now `off`", ctx.send_calls[3]["embed"].description.lower())
            self.assertEqual(cog.service.get_meta()["ai_celebration_mode"], "off")
        finally:
            await cog.service.close()

    async def test_hidden_drops_ai_override_invalid_mode_in_dm_shows_usage(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = QuestionDropsCog(bot)
        try:
            cog.service.storage_ready = True
            owner = FakeAuthor(user_id=1266444952779620413)
            ctx = FakeContext(interaction=None, guild=None, channel=FakeChannel(), author=owner)

            await QuestionDropsCog.drops_celebration_ai_global_override_command.callback(cog, ctx, "loud")

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Question Drops AI Override")
            self.assertIn("Use `status`, `off`, `rare`, or `event_only`.", ctx.send_calls[0]["embed"].description)
        finally:
            await cog.service.close()

    def test_hidden_override_command_is_not_in_public_help_pages(self):
        serialized_help = " ".join(
            " ".join(
                filter(
                    None,
                    [
                        page.get("body", ""),
                        " ".join(value for _name, value in page.get("fields", ())),
                        page.get("try", ""),
                    ],
                )
            )
            for page in HELP_PAGES
        ).casefold()

        self.assertNotIn("shieldai", serialized_help)
        self.assertNotIn("dropscelebaiglobal", serialized_help)
        self.assertNotIn("topggvoteadmin", serialized_help)
