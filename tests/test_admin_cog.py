from __future__ import annotations

import asyncio
import types
import unittest
from unittest.mock import AsyncMock

import discord

from babblebox import game_engine as ge
from babblebox.cogs.admin import AdminCog, AdminPanelView, FollowupReviewView, VerificationDeadlineReviewView
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore


class FakeMessage:
    def __init__(self, **kwargs):
        self.embed = kwargs.get("embed")
        self.view = kwargs.get("view")
        self.ephemeral = kwargs.get("ephemeral")
        self.edits = []

    async def edit(self, **kwargs):
        self.edits.append(kwargs)
        if "embed" in kwargs:
            self.embed = kwargs["embed"]
        if "view" in kwargs:
            self.view = kwargs["view"]
        return self


class FakeResponse:
    def __init__(self, interaction=None):
        self._interaction = interaction
        self._done = False
        self.edits = []
        self.sent_messages = []

    def is_done(self):
        return self._done

    async def edit_message(self, **kwargs):
        self._done = True
        self.edits.append(kwargs)
        if self._interaction is not None and self._interaction.message is not None:
            await self._interaction.message.edit(**kwargs)

    async def send_message(self, *args, **kwargs):
        self._done = True
        self.sent_messages.append({"args": args, "kwargs": kwargs})


class FakeInteraction:
    def __init__(self, *, user=None, guild=None, message=None):
        self.user = user
        self.guild = guild
        self.message = message
        self.response = FakeResponse(self)
        self.followup = types.SimpleNamespace(send=self._followup_send)
        self.followup_calls = []

    def is_expired(self):
        return False

    async def _followup_send(self, *args, **kwargs):
        self.followup_calls.append({"args": args, "kwargs": kwargs})


class FakeGuildPermissions:
    administrator = False
    manage_guild = False
    manage_channels = False
    manage_messages = False
    moderate_members = False
    kick_members = False
    ban_members = False


class FakePermissionSnapshot:
    def __init__(self, **overrides):
        defaults = {
            "manage_roles": False,
            "manage_channels": False,
            "manage_webhooks": False,
            "manage_messages": False,
            "kick_members": False,
            "ban_members": False,
            "moderate_members": False,
            "view_audit_log": True,
            "view_channel": True,
            "send_messages": True,
            "embed_links": True,
            "mention_everyone": False,
            "administrator": False,
            "manage_guild": False,
        }
        defaults.update(overrides)
        for name, value in defaults.items():
            setattr(self, name, value)


class FakeRole:
    def __init__(self, role_id: int, *, position: int = 1, mentionable: bool = True, name: str | None = None):
        self.id = role_id
        self.name = name or f"Role {role_id}"
        self.position = position
        self.mention = f"<@&{role_id}>"
        self.mentionable = mentionable


class FakeAuthor:
    def __init__(
        self,
        user_id: int = 1,
        *,
        manage_guild: bool = False,
        manage_channels: bool = False,
        manage_messages: bool = False,
        moderate_members: bool = False,
        kick_members: bool = False,
        ban_members: bool = False,
        administrator: bool = False,
    ):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions()
        self.guild_permissions.manage_guild = manage_guild
        self.guild_permissions.manage_channels = manage_channels
        self.guild_permissions.manage_messages = manage_messages
        self.guild_permissions.moderate_members = moderate_members
        self.guild_permissions.kick_members = kick_members
        self.guild_permissions.ban_members = ban_members
        self.guild_permissions.administrator = administrator
        self.sent = []

    async def send(self, *, embed=None):
        self.sent.append(embed)


class FakeMember(FakeAuthor):
    def __init__(
        self,
        user_id: int,
        guild,
        *,
        roles=None,
        manage_guild: bool = False,
        manage_channels: bool = False,
        administrator: bool = False,
        joined_at=None,
    ):
        super().__init__(user_id=user_id, manage_guild=manage_guild, manage_channels=manage_channels, administrator=administrator)
        self.guild = guild
        self.roles = list(roles or [])
        self.top_role = self.roles[0] if self.roles else FakeRole(0, position=1)
        self.guild_permissions = FakePermissionSnapshot(manage_guild=manage_guild, manage_channels=manage_channels, administrator=administrator)
        self.joined_at = joined_at or ge.now_utc()
        self.bot = False


class FakeChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        name: str = "general",
        permissions: FakePermissionSnapshot | None = None,
        channel_type=discord.ChannelType.text,
        category=None,
        permissions_synced: bool = False,
    ):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self._permissions = permissions or FakePermissionSnapshot(
            manage_channels=True,
            view_channel=True,
            send_messages=True,
            embed_links=True,
        )
        self.sent = []
        self._messages: dict[int, FakeMessage] = {}
        self.type = channel_type
        self.category = category
        self.category_id = getattr(category, "id", None)
        self.permissions_synced = permissions_synced
        self._role_overwrites: dict[int, discord.PermissionOverwrite] = {}
        self.permission_edits: list[dict[str, object]] = []
        self.raise_forbidden_on_set_permissions = False

    def permissions_for(self, member):
        return self._permissions

    def overwrites_for(self, role):
        overwrite = self._role_overwrites.get(int(getattr(role, "id", 0) or 0))
        if overwrite is None:
            return discord.PermissionOverwrite()
        allow, deny = overwrite.pair()
        return discord.PermissionOverwrite.from_pair(allow, deny)

    async def set_permissions(self, role, *, overwrite=None, reason=None):
        if self.raise_forbidden_on_set_permissions:
            response = types.SimpleNamespace(status=403, reason="Forbidden", headers={})
            raise discord.Forbidden(response=response, message="missing permissions")
        role_id = int(getattr(role, "id", 0) or 0)
        self.permission_edits.append({"role_id": role_id, "overwrite": overwrite, "reason": reason})
        if overwrite is None:
            self._role_overwrites.pop(role_id, None)
            return
        allow, deny = overwrite.pair()
        self._role_overwrites[role_id] = discord.PermissionOverwrite.from_pair(allow, deny)

    async def send(self, **kwargs):
        message = FakeMessage(**kwargs)
        message.id = 1000 + len(self.sent)
        self.sent.append({"message": message, **kwargs})
        self._messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            raise Exception("missing")
        return message


class FakeGuild:
    def __init__(self, guild_id: int = 10):
        self.id = guild_id
        self.name = "Guild"
        self.owner_id = 1
        self.chunked = True
        self.channels: dict[int, FakeChannel] = {}
        self.roles: dict[int, FakeRole] = {}
        self.members: dict[int, object] = {}
        self.default_role = FakeRole(guild_id, position=0, name="@everyone")
        self.roles[self.default_role.id] = self.default_role
        self.me = types.SimpleNamespace(
            id=999,
            top_role=FakeRole(900, position=50),
            guild_permissions=FakePermissionSnapshot(
                manage_roles=True,
                manage_channels=True,
                manage_messages=True,
                moderate_members=True,
                kick_members=True,
                view_channel=True,
                send_messages=True,
                embed_links=True,
                mention_everyone=True,
            ),
        )

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)

    def get_role(self, role_id: int):
        return self.roles.get(role_id)

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return self.members.get(user_id)

    async def chunk(self, cache=True):
        self.chunked = True


class FakeBot:
    def __init__(self, guild: FakeGuild):
        self.loop = asyncio.get_running_loop()
        self.user = types.SimpleNamespace(id=999)
        self._guild = guild
        self.views = []

    def get_guild(self, guild_id: int):
        if guild_id == self._guild.id:
            return self._guild
        return None

    def get_channel(self, channel_id: int):
        return self._guild.get_channel(channel_id)

    def add_view(self, view, *, message_id=None):
        self.views.append((view, message_id))


class FakeContext:
    def __init__(self, *, interaction=None, author=None, guild=None, channel=None):
        self.interaction = interaction
        self.author = author or FakeAuthor()
        self.guild = guild
        self.channel = channel
        self.message = None
        self.send_calls = []
        self.defer_calls = []

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage(**kwargs)

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        if self.interaction is not None:
            self.interaction.response._done = True


class AdminCogSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.log_channel = FakeChannel(30, name="admin-log")
        self.guild.channels[self.log_channel.id] = self.log_channel
        self.followup_role = FakeRole(70, position=10, name="Probation")
        self.verified_role = FakeRole(80, position=10, name="Verified")
        self.guild.roles[self.followup_role.id] = self.followup_role
        self.guild.roles[self.verified_role.id] = self.verified_role
        self.bot = FakeBot(self.guild)
        self.cog = AdminCog(self.bot)
        self.original_service = self.cog.service
        store = AdminStore(backend="memory")
        await store.load()
        self.cog.service = AdminService(self.bot, store=store)
        self.cog.service.storage_ready = True
        self.bot.admin_service = self.cog.service

    async def asyncTearDown(self):
        await self.cog.service.close()
        await self.original_service.close()

    async def test_admin_status_is_private_for_admins(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_status_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_admin_status_denies_members_privately(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=False),
        )

        await AdminCog.admin_status_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)

    async def test_admin_followup_command_updates_config(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_followup_command.callback(
            self.cog,
            ctx,
            enabled=True,
            role=self.followup_role,
            mode="review",
            duration="30d",
            clear_role=False,
        )

        config = self.cog.service.get_config(self.guild.id)
        self.assertTrue(config["followup_enabled"])
        self.assertEqual(config["followup_role_id"], self.followup_role.id)
        self.assertEqual(config["followup_mode"], "review")
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_lock_channel_allows_manage_channels_moderator_by_default(self):
        channel = FakeChannel(20)
        self.guild.channels[channel.id] = channel
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=channel,
            author=FakeAuthor(manage_channels=True),
        )

        await AdminCog.lock_channel_command.callback(
            self.cog,
            ctx,
            channel=channel,
            duration="30m",
            notice_message=None,
            post_notice=False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Locked", ctx.send_calls[0]["embed"].description)
        everyone_overwrite = channel.overwrites_for(self.guild.default_role)
        self.assertFalse(everyone_overwrite.send_messages)

    async def test_lock_channel_allows_manage_messages_moderator_by_default(self):
        channel = FakeChannel(23)
        self.guild.channels[channel.id] = channel
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=channel,
            author=FakeAuthor(manage_messages=True),
        )

        await AdminCog.lock_channel_command.callback(
            self.cog,
            ctx,
            channel=channel,
            duration="30m",
            notice_message=None,
            post_notice=False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Locked", ctx.send_calls[0]["embed"].description)
        everyone_overwrite = channel.overwrites_for(self.guild.default_role)
        self.assertFalse(everyone_overwrite.send_messages)

    async def test_lock_settings_is_admin_only(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_channels=True),
        )

        await AdminCog.lock_settings_command.callback(
            self.cog,
            ctx,
            default_notice="Please pause here while moderators review the situation.",
            clear_notice=False,
            admin_only=None,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)

    async def test_lock_settings_can_limit_lane_to_admins_only(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.lock_settings_command.callback(
            self.cog,
            ctx,
            default_notice=None,
            clear_notice=False,
            admin_only=True,
        )

        config = self.cog.service.get_config(self.guild.id)
        self.assertTrue(config["lock_admin_only"])
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_lock_channel_denies_moderator_when_admin_only_is_enabled(self):
        ok, _ = await self.cog.service.set_lock_config(self.guild.id, admin_only=True)
        self.assertTrue(ok)
        channel = FakeChannel(21)
        self.guild.channels[channel.id] = channel
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=channel,
            author=FakeAuthor(manage_messages=True),
        )

        await AdminCog.lock_channel_command.callback(
            self.cog,
            ctx,
            channel=channel,
            duration=None,
            notice_message=None,
            post_notice=True,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("limited emergency locks", ctx.send_calls[0]["embed"].description)

    async def test_lock_channel_denies_member_without_moderator_permissions_by_default(self):
        channel = FakeChannel(24)
        self.guild.channels[channel.id] = channel
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=channel,
            author=FakeAuthor(),
        )

        await AdminCog.lock_channel_command.callback(
            self.cog,
            ctx,
            channel=channel,
            duration="30m",
            notice_message=None,
            post_notice=False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Manage Messages", ctx.send_calls[0]["embed"].description)

    async def test_lock_channel_still_allows_manage_guild_admin_when_admin_only_is_enabled(self):
        ok, _ = await self.cog.service.set_lock_config(self.guild.id, admin_only=True)
        self.assertTrue(ok)
        channel = FakeChannel(22)
        self.guild.channels[channel.id] = channel
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=channel,
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.lock_channel_command.callback(
            self.cog,
            ctx,
            channel=channel,
            duration="30m",
            notice_message=None,
            post_notice=False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Locked", ctx.send_calls[0]["embed"].description)

    async def test_admin_permissions_surfaces_missing_manage_channels(self):
        self.guild.me.guild_permissions = FakePermissionSnapshot(
            manage_roles=True,
            manage_channels=False,
            manage_messages=True,
            moderate_members=True,
            kick_members=True,
            mention_everyone=True,
        )
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_permissions_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.send_calls), 1)
        embed = ctx.send_calls[0]["embed"]
        missing = next(field for field in embed.fields if field.name == "Missing Server Permissions")
        self.assertIn("Manage Channels", missing.value)
        self.assertIn("/lock channel", missing.value)

    async def test_verification_panel_spells_out_verified_and_unverified_members(self):
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            deadline_action="auto_kick",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "verification")
        current_rule = next(field for field in embed.fields if field.name == "Current Rule")
        target = next(field for field in embed.fields if field.name == "Deadline Path")

        self.assertIn("Members are considered verified only if they HAVE <@&80>.", current_rule.value)
        self.assertIn("Users WITHOUT <@&80> are treated as unverified.", current_rule.value)
        self.assertIn("Deadline action: **Kick automatically**", current_rule.value)
        self.assertIn("users who do NOT have <@&80> will be warned after 5 days and kicked after 1 week.", target.value)
        self.assertIn("Exempt from warning/kick", target.value)

    async def test_verification_panel_adds_review_note_for_confusing_role_name(self):
        not_verified_role = FakeRole(81, position=10, name="Not Verified")
        self.guild.roles[not_verified_role.id] = not_verified_role
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=not_verified_role.id,
            logic="must_not_have_role",
            deadline_action="auto_kick",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "verification")
        review = next(field for field in embed.fields if field.name == "Please Review Carefully")
        target = next(field for field in embed.fields if field.name == "Deadline Path")

        self.assertIn("sounds like an unverified-state role", review.value)
        self.assertIn("users WITH <@&81> should be warned and kicked", review.value)
        self.assertIn("users who still have <@&81> will be warned after 5 days and kicked after 1 week.", target.value)

    async def test_verification_panel_review_mode_spells_out_moderator_review(self):
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            deadline_action="review",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "verification")
        current_rule = next(field for field in embed.fields if field.name == "Current Rule")
        target = next(field for field in embed.fields if field.name == "Deadline Path")

        self.assertIn("Deadline action: **Moderator review**", current_rule.value)
        self.assertIn("sent for moderator review after 1 week", target.value)

    async def test_admin_verification_command_updates_deadline_action(self):
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_verification_command.callback(
            self.cog,
            ctx,
            enabled=True,
            role=self.verified_role,
            logic="must_have_role",
            deadline_action="review",
            kick_after="7d",
            warning_lead="2d",
            help_channel=None,
            help_extension="1d",
            max_extensions=1,
            clear_role=False,
            clear_help_channel=False,
        )

        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["verification_deadline_action"], "review")
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])

    async def test_verification_review_view_denies_non_admins_privately(self):
        view = VerificationDeadlineReviewView(guild_id=self.guild.id, user_id=123, version=1)
        message = FakeMessage(embed=None, view=view)
        interaction = FakeInteraction(
            user=FakeAuthor(manage_guild=False),
            guild=self.guild,
            message=message,
        )
        interaction.client = types.SimpleNamespace(admin_service=self.cog.service)
        kick_button = next(child for child in view.children if child.label == "Kick")

        await kick_button.callback(interaction)

        self.assertEqual(len(interaction.response.sent_messages), 1)
        self.assertTrue(interaction.response.sent_messages[0]["kwargs"]["ephemeral"])
        self.assertIn("Manage Server", interaction.response.sent_messages[0]["kwargs"]["embed"].description)

    async def test_cog_load_registers_followup_and_verification_review_views(self):
        record_followup = {
            "guild_id": self.guild.id,
            "user_id": 501,
            "review_version": 2,
            "review_message_id": 1501,
        }
        record_queue = {
            "guild_id": self.guild.id,
            "channel_id": 31,
            "message_id": 1502,
            "updated_at": ge.now_utc().isoformat(),
        }
        record_verification = {
            "guild_id": self.guild.id,
            "user_id": 502,
            "review_version": 3,
        }
        self.cog.service.start = AsyncMock(return_value=True)
        self.cog.service.list_review_views = AsyncMock(return_value=[record_followup])
        self.cog.service.list_verification_review_queues = AsyncMock(return_value=[record_queue])
        self.cog.service.current_verification_review_target = AsyncMock(return_value=record_verification)

        await self.cog.cog_load()

        self.assertEqual(len(self.bot.views), 2)
        self.assertIsInstance(self.bot.views[0][0], FollowupReviewView)
        self.assertEqual(self.bot.views[0][1], 1501)
        self.assertIsInstance(self.bot.views[1][0], VerificationDeadlineReviewView)
        self.assertEqual(self.bot.views[1][1], 1502)

    async def test_admin_panel_warns_when_operability_is_missing(self):
        blocked_log_channel = FakeChannel(
            31,
            name="admin-log",
            permissions=FakePermissionSnapshot(view_channel=False, send_messages=False, embed_links=False),
        )
        self.guild.channels[blocked_log_channel.id] = blocked_log_channel
        self.guild.me.guild_permissions = FakePermissionSnapshot(manage_roles=True, kick_members=False, mention_everyone=False)
        self.guild.me.top_role = FakeRole(901, position=50)
        high_followup_role = FakeRole(71, position=60)
        self.guild.roles[high_followup_role.id] = high_followup_role
        alert_role = FakeRole(72, position=65, mentionable=False)
        self.guild.roles[alert_role.id] = alert_role

        ok, _ = await self.cog.service.set_followup_config(
            self.guild.id,
            enabled=True,
            role_id=high_followup_role.id,
            mode="review",
            duration_text="30d",
        )
        self.assertTrue(ok)
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        ok, _ = await self.cog.service.set_logs_config(
            self.guild.id,
            channel_id=blocked_log_channel.id,
            alert_role_id=alert_role.id,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "overview")
        operability = next(field for field in embed.fields if field.name == "Operability")

        self.assertIn("at or above", operability.value)
        self.assertIn("Kick Members", operability.value)
        self.assertIn("cannot see", operability.value)
        self.assertIn("cannot send", operability.value)
        self.assertIn("cannot embed", operability.value)

    async def test_admin_panel_only_shows_focused_sections(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)

        labels = [child.label for child in view.children]

        self.assertEqual(labels, ["Overview", "Follow-up", "Verification", "Exclusions", "Logs", "Templates", "Refresh"])
        self.assertNotIn("Risk", labels)
        self.assertNotIn("Emergency", labels)
        self.assertNotIn("Permissions", labels)

    async def test_admin_sync_command_opens_confirmation_panel_with_preview_count(self):
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        self.guild.members[101] = FakeMember(101, self.guild, roles=[])
        self.guild.members[102] = FakeMember(102, self.guild, roles=[self.verified_role])
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_sync_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        embed = ctx.send_calls[0]["embed"]
        view = ctx.send_calls[0]["view"]
        dry_run = next(field for field in embed.fields if field.name == "Dry Run")
        self.assertIn("Currently **1** members match this rule.", dry_run.value)
        self.assertIsNotNone(view)
        labels = [child.label for child in view.children]
        self.assertIn("Start Sync", labels)
        self.assertIn("Cancel", labels)

    async def test_sync_view_cancel_before_start_makes_no_changes(self):
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        self.guild.members[103] = FakeMember(103, self.guild, roles=[])
        author = FakeAuthor(manage_guild=True)
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=author,
        )

        await AdminCog.admin_sync_command.callback(self.cog, ctx)

        message = FakeMessage(**ctx.send_calls[0])
        view = ctx.send_calls[0]["view"]
        view.message = message
        interaction = FakeInteraction(user=author, guild=self.guild, message=message)
        cancel_button = next(child for child in view.children if child.label == "Cancel")

        await cancel_button.callback(interaction)

        self.assertEqual(message.embed.title, "Verification Sync Cancelled")
        counts = await self.cog.service.get_counts(self.guild.id)
        self.assertEqual(counts["verification_pending"], 0)
        self.assertTrue(all(child.disabled for child in view.children))

    async def test_admin_test_warning_preview_renders_placeholders_safely(self):
        ok, _ = await self.cog.service.set_verification_config(
            self.guild.id,
            enabled=True,
            role_id=self.verified_role.id,
            logic="must_have_role",
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=self.log_channel.id,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)
        ok, _ = await self.cog.service.set_templates(
            self.guild.id,
            warning_template="Hi {member}, finish verification in {guild} before {deadline_relative}. Use {help_channel}. {invite_link}",
            invite_link="https://discord.gg/example",
        )
        self.assertTrue(ok)
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_test_command.callback(self.cog, ctx, kind="warning_dm", member=None, dm_self=False, post_log=False)

        self.assertEqual(len(ctx.send_calls), 1)
        embed = ctx.send_calls[0]["embed"]
        resolved = next(field for field in embed.fields if field.name == "Resolved Placeholders")
        delivery = next(field for field in embed.fields if field.name == "Delivery")
        self.assertIn("Guild", resolved.value)
        self.assertIn("Invite link", resolved.value)
        self.assertIn("Bulk sends started: **No**", delivery.value)

    async def test_admin_test_logs_surfaces_log_delivery_failure(self):
        blocked_log_channel = FakeChannel(
            40,
            name="verification-logs",
            permissions=FakePermissionSnapshot(view_channel=True, send_messages=False, embed_links=False),
        )
        self.guild.channels[blocked_log_channel.id] = blocked_log_channel
        ok, _ = await self.cog.service.set_logs_config(self.guild.id, channel_id=blocked_log_channel.id, alert_role_id=None)
        self.assertTrue(ok)
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(20),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_test_command.callback(self.cog, ctx, kind="logs", member=None, dm_self=False, post_log=True)

        embed = ctx.send_calls[0]["embed"]
        delivery = next(field for field in embed.fields if field.name == "Delivery")
        prechecks = next(field for field in embed.fields if field.name == "Prechecks")
        self.assertIn("Could not post", delivery.value)
        self.assertIn("cannot send messages", prechecks.value.lower())

