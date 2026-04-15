import asyncio
import types
import unittest
from datetime import datetime, timezone
from typing import Optional
from unittest.mock import AsyncMock, patch

import discord
from discord import app_commands
from discord.ext import commands

from babblebox.app_command_hardening import harden_admin_root_group, harden_lock_root_group
from babblebox import game_engine as ge
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore
from babblebox.command_utils import HybridPanelSendResult
from babblebox.cogs.admin import AdminCog
from babblebox.cogs.confessions import ConfessionsCog
from babblebox.cogs.gameplay import GameplayCog
from babblebox.cogs.identity import IdentityCog
from babblebox.cogs.meta import HELP_PAGES, MetaCog, build_help_embed, build_help_page_embed
from babblebox.cogs.question_drops import QuestionDropsCog
from babblebox.cogs.shield import ShieldCog, ShieldPanelView
from babblebox.cogs.utilities import AfkReturnWatchDurationSelect, UtilityCog
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
        self._done = True
        self.edit_calls.append((args, kwargs))
        if self._interaction is not None and getattr(self._interaction, "message", None) is not None:
            await self._interaction.message.edit(**kwargs)
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
    async def _registered_root(self, cog_factory, *, root_name: str):
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
        self.assertIn("Coders need server DMs open before start", party_page["body"])
        self.assertIn("digits `0-9` only", party_page["body"])
        self.assertNotIn("Only 16", party_page["body"])

    def test_help_pages_reflect_question_drop_option_copy(self):
        question_drops_page = next(page for page in HELP_PAGES if page["title"] == "Question Drops")
        self.assertIn("/drops status", question_drops_page["body"])
        self.assertIn("/drops roles status", question_drops_page["body"])
        self.assertIn("/dropsadmin config", question_drops_page["body"])
        self.assertNotIn("/drops panel", question_drops_page["body"])
        self.assertIn("/dropsadmin mastery category", question_drops_page["body"])
        self.assertNotIn("/drops mastery category", question_drops_page["body"])
        self.assertIn("difficulty profile", question_drops_page["body"])
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
        self.assertIn("genuinely appreciated", support_page["body"])
        self.assertIn("discord.com/servers/inevitable-friendship-1322933864360050688", support_page["links"])
        self.assertIn("github.com/arno-create/babblebox-bot", support_page["links"])
        self.assertIn("arno-create.github.io/babblebox-bot/", support_page["links"])

        embed = build_help_page_embed(support_index)
        fields = {field.name: field.value for field in embed.fields}
        self.assertIn("Links", fields)
        self.assertIn("GitHub Repository", fields["Links"])
        self.assertIn("Support Server", fields["Links"])
        self.assertIn("Official Website", fields["Links"])

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
        self.assertNotIn("experimental scam heuristics", shield_page["body"])
        self.assertIn("/lock channel", shield_page["body"])
        self.assertIn("/lock remove", shield_page["body"])
        self.assertIn("/lock settings", shield_page["body"])
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
        self.assertIn("/admin followup", shield_field.value)
        self.assertIn("/admin verification", shield_field.value)
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
        self.assertIn("Support / Links", fields)
        self.assertIn("/support", fields["Support / Links"])

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
        self.assertTrue({"config", "channels", "categories", "digest", "mastery"}.issubset(prefix_alias_names))
        self.assertEqual(admin_slash_names, {"categories", "channels", "config", "digest", "mastery"})
        self.assertEqual(prefix_mastery_names, {"category", "recalc", "scholar"})
        self.assertEqual(admin_slash_mastery_names, {"category", "recalc", "scholar"})

    async def test_admin_only_roots_emit_hidden_guild_only_metadata(self):
        expected = {
            "admin": (AdminCog, int(discord.Permissions(manage_guild=True).value)),
            "lock": (AdminCog, int(discord.Permissions(manage_channels=True).value)),
            "shield": (ShieldCog, int(discord.Permissions(manage_guild=True).value)),
            "confessions": (ConfessionsCog, int(discord.Permissions(manage_guild=True).value)),
            "dropsadmin": (QuestionDropsCog, int(discord.Permissions(manage_guild=True).value)),
        }

        for name, (cog_cls, default_permissions) in expected.items():
            with self.subTest(command=name):
                command, payload = await self._registered_root(cog_cls, root_name=name)

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
        self.assertIn("Coders need server DMs open before the room starts.", setup_field)
        self.assertIn("private rule theories stay in `/hunt guess`", setup_field)

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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=permission_check):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)), patch(
            "babblebox.cogs.meta.send_hybrid_panel_response",
            new=AsyncMock(return_value=HybridPanelSendResult(delivered=True, path="context_send", message=None, handle_status="missing")),
        ):
            await MetaCog.help_command.callback(cog, first_ctx, visibility="public")

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)), patch(
            "babblebox.cogs.meta.send_hybrid_panel_response",
            new=AsyncMock(return_value=HybridPanelSendResult(delivered=False, path="context_send", error=TypeError("broken"))),
        ):
            await MetaCog.help_command.callback(cog, first_ctx, visibility="public")

        self.assertEqual(self._sent_kwargs(first_ctx)["embed"].title, "Help Unavailable")
        self.assertTrue(self._sent_kwargs(first_ctx)["ephemeral"])

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
            await MetaCog.help_command.callback(cog, second_ctx, visibility="public")

        self.assertFalse(self._sent_kwargs(second_ctx)["ephemeral"])
        self.assertIn("Babblebox Help", self._sent_kwargs(second_ctx)["embed"].title)

    async def test_support_public_uses_view_and_public_visibility(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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
                "GitHub Repository": "https://github.com/arno-create/babblebox-bot",
                "Official Website": "https://arno-create.github.io/babblebox-bot/",
            },
        )

    async def test_support_private_stays_ephemeral_without_defer(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, ctx, visibility="private")

        payload = self._sent_kwargs(ctx)
        self.assertEqual(ctx.defer_calls, [])
        self.assertTrue(payload["ephemeral"])
        self.assertIn("Babblebox Support", payload["embed"].title)

    async def test_support_prefix_still_works(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        ctx = FakeContext(interaction=None, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, ctx, visibility="public")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertFalse(ctx.send_calls[0]["ephemeral"])

    async def test_support_private_interaction_skips_channel_permission_gate(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        interaction = FakeInteraction()
        ctx = FakeContext(interaction=interaction, guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        permission_check = AsyncMock(return_value=False)

        with patch("babblebox.cogs.meta.require_channel_permissions", new=permission_check):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)), patch(
            "babblebox.cogs.meta.send_hybrid_panel_response",
            new=AsyncMock(return_value=HybridPanelSendResult(delivered=True, path="context_send", message=None, handle_status="missing")),
        ):
            await MetaCog.support_command.callback(cog, first_ctx, visibility="public")

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)), patch(
            "babblebox.cogs.meta.send_hybrid_panel_response",
            new=AsyncMock(return_value=HybridPanelSendResult(delivered=False, path="context_send", error=TypeError("broken"))),
        ):
            await MetaCog.support_command.callback(cog, first_ctx, visibility="public")

        self.assertEqual(self._sent_kwargs(first_ctx)["embed"].title, "Support Panel Unavailable")
        self.assertTrue(self._sent_kwargs(first_ctx)["ephemeral"])

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
            await MetaCog.support_command.callback(cog, second_ctx, visibility="public")

        self.assertFalse(self._sent_kwargs(second_ctx)["ephemeral"])
        self.assertIn("Babblebox Support", self._sent_kwargs(second_ctx)["embed"].title)

    async def test_support_public_uses_separate_cooldown_from_help(self):
        cog = MetaCog(types.SimpleNamespace(loop=asyncio.get_running_loop()))
        help_ctx = FakeContext(interaction=FakeInteraction(), guild=FakeGuild(), channel=FakeChannel(), author=FakeAuthor())
        support_ctx = FakeContext(interaction=FakeInteraction(), guild=help_ctx.guild, channel=help_ctx.channel, author=help_ctx.author)

        with patch("babblebox.cogs.meta.require_channel_permissions", new=AsyncMock(return_value=True)):
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
        bot = commands.Bot(command_prefix="!", intents=discord.Intents.none())
        try:
            cog = UtilityCog(bot)
            await bot.add_cog(cog)

            slash_roots = {command.name for command in bot.tree.get_commands()}
            prefix_commands = {command.name for command in cog.walk_commands()}

            self.assertTrue({"watch", "later", "capture", "remind"}.issubset(slash_roots))
            self.assertNotIn("moment", slash_roots)
            self.assertNotIn("moment", prefix_commands)
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
            protection_field = next(field for field in embed.fields if field.name == "Protection Packs")
            link_safety_field = next(field for field in embed.fields if field.name == "High-Risk Packs")
            link_policy_field = next(field for field in embed.fields if field.name == "Link Policy")

            self.assertIn("**Privacy Leak**", protection_field.value)
            self.assertIn("Enabled: Yes | Sensitivity: High", protection_field.value)
            self.assertIn("Low confidence: Log only", protection_field.value)
            self.assertIn("Medium confidence: Delete + log", protection_field.value)
            self.assertIn("High confidence: Delete + log", protection_field.value)
            self.assertIn("**Promo / Invite**", protection_field.value)
            self.assertIn("**Anti-Spam**", protection_field.value)
            self.assertIn("**GIF Flood / Media Pressure**", protection_field.value)
            self.assertIn("Enabled: Yes | Sensitivity: Normal", protection_field.value)
            self.assertIn("Moderator anti-spam: Exempt moderators", protection_field.value)
            self.assertIn("Delete actions remove the matched GIF burst, not just the last message", protection_field.value)
            self.assertIn("**Scam / Malicious Links**", link_safety_field.value)
            self.assertIn("**Adult Links + Solicitation**", link_safety_field.value)
            self.assertIn("**Severe Harm / Hate**", link_safety_field.value)
            self.assertIn("Mode: **Default**", link_policy_field.value)
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

    async def test_shield_ai_panel_embed_keeps_owner_policy_explanation_without_extra_control(self):
        bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        cog = ShieldCog(bot)
        try:
            cog.service.storage_ready = True

            embed = cog.build_panel_embed(10, "ai")
            fields = {field.name: field.value for field in embed.fields}

            self.assertEqual(embed.title, "Shield AI Assist")
            self.assertIn("owner-managed", embed.description.lower())
            self.assertIn("Access Policy", fields)
            self.assertIn("Provider and Routing", fields)
            self.assertIn("Runtime Policy", fields)
            self.assertIn("Policy source", fields["Access Policy"])
            self.assertIn("Allowed models", fields["Access Policy"])
            self.assertIn("Ordinary-guild default", fields["Access Policy"])
            self.assertIn("Review scope is admin-configurable; access is owner-managed", embed.footer.text)
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
                module=None,
                pack="adult",
                enabled=True,
                action="delete_log",
                low_action=None,
                medium_action=None,
                high_action=None,
                sensitivity="normal",
                escalation_threshold=None,
                escalation_window_minutes=None,
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
                module=None,
                pack="adult",
                enabled=True,
                action=None,
                low_action="log",
                medium_action="delete_log",
                high_action="delete_log",
                sensitivity="normal",
                adult_solicitation=True,
                escalation_threshold=None,
                escalation_window_minutes=None,
                timeout_minutes=None,
            )

            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("Optional solicitation text detection is on", ctx.send_calls[0]["embed"].description)
            self.assertTrue(cog.service.get_config(10)["adult_solicitation_enabled"])
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

    async def test_shield_test_command_includes_link_safety_assessments(self):
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
            self.assertIn("Link Safety", field_names)
            link_safety_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Safety")
            self.assertIn("dlscord-gift.com", link_safety_field.value)
            self.assertIn("matched local intel", link_safety_field.value)
        finally:
            await cog.service.close()

    async def test_shield_test_command_marks_lookup_candidates_as_no_action(self):
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

            link_safety_field = next(field for field in ctx.send_calls[0]["embed"].fields if field.name == "Link Safety")
            self.assertIn("lookup candidate, link-only caution", link_safety_field.value)
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
            self.assertIn("restored", ctx.send_calls[4]["embed"].description.lower())
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
        serialized_help = " ".join(page["body"] + " " + page.get("try", "") for page in HELP_PAGES).casefold()

        self.assertNotIn("shieldai", serialized_help)
        self.assertNotIn("dropscelebaiglobal", serialized_help)
