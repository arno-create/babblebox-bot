from __future__ import annotations

import asyncio
from datetime import timedelta
import types
import unittest
from unittest import mock
from unittest.mock import AsyncMock

import discord

from babblebox import game_engine as ge
from babblebox.admin_panel_views import (
    ExclusionsEditorView,
    FollowupEditorView,
    LogsEditorView,
)
from babblebox.cogs.admin import AdminCog, AdminPanelView, FollowupReviewView
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore


class FakeMessage:
    _next_id = 1000

    def __init__(self, *, channel=None, message_id=None, **kwargs):
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
        self.edits = self.edit_calls
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
        self.edits = []
        self.sent_messages = []
        self.defer_calls = []
        self.modal_calls = []

    def is_done(self):
        return self._done

    async def edit_message(self, **kwargs):
        if self._interaction is not None and getattr(self._interaction, "edit_exception", None) is not None:
            raise self._interaction.edit_exception
        self._done = True
        self.edits.append(kwargs)
        target = None
        if self._interaction is not None:
            target = self._interaction.message or self._interaction._original_response_message
        if target is not None:
            await target.edit(**kwargs)
        return FakeInteractionCallbackResponse(resource=target, message_id=getattr(target, "id", None))

    async def send_message(self, *args, **kwargs):
        self.send_calls.append((args, kwargs))
        self._done = True
        self.sent_messages.append({"args": args, "kwargs": kwargs})
        if self._interaction is not None and getattr(self._interaction, "initial_send_exception", None) is not None:
            raise self._interaction.initial_send_exception
        if self._interaction is None:
            return FakeInteractionCallbackResponse()
        return self._interaction.build_initial_response(kwargs)

    async def defer(self, *args, **kwargs):
        self.defer_calls.append((args, kwargs))
        if self._interaction is not None and getattr(self._interaction, "defer_exception", None) is not None:
            raise self._interaction.defer_exception
        self._done = True
        return FakeInteractionCallbackResponse(resource=getattr(self._interaction, "message", None))

    async def send_modal(self, modal):
        self.modal_calls.append(modal)
        self._done = True
        return None


class FakeInteraction:
    def __init__(
        self,
        *,
        user=None,
        guild=None,
        message=None,
        channel=None,
        expired: bool = False,
        initial_send_exception: Exception | None = None,
        followup_exception: Exception | None = None,
        original_response_exception: Exception | None = None,
        edit_original_response_exception: Exception | None = None,
        defer_exception: Exception | None = None,
        edit_exception: Exception | None = None,
    ):
        self.user = user
        self.guild = guild
        self.message = message
        self.channel = channel or getattr(message, "channel", None)
        self.response = FakeResponse(self)
        self.followup = types.SimpleNamespace(send=self._followup_send)
        self._expired = expired
        self.initial_send_exception = initial_send_exception
        self.followup_exception = followup_exception
        self.original_response_exception = original_response_exception
        self.edit_original_response_exception = edit_original_response_exception
        self.defer_exception = defer_exception
        self.edit_exception = edit_exception
        self.original_response_calls = []
        self.edit_original_response_calls = []
        self._original_response_message = None
        self._last_followup_message = None
        self.followup_calls = []

    def is_expired(self):
        return self._expired

    def _register_message(self, message: FakeMessage):
        if self.channel is not None and hasattr(self.channel, "register_message"):
            self.channel.register_message(message)
        return message

    def create_message(self, payload: dict) -> FakeMessage:
        return self._register_message(FakeMessage(channel=self.channel, **payload))

    def build_initial_response(self, payload: dict):
        message = self.create_message(payload)
        self._original_response_message = message
        return FakeInteractionCallbackResponse(resource=message, message_id=message.id)

    async def _followup_send(self, *args, **kwargs):
        if self.followup_exception is not None:
            raise self.followup_exception
        self.followup_calls.append({"args": args, "kwargs": kwargs})
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
        moderate_members: bool = False,
        administrator: bool = False,
        joined_at=None,
    ):
        super().__init__(
            user_id=user_id,
            manage_guild=manage_guild,
            manage_channels=manage_channels,
            moderate_members=moderate_members,
            administrator=administrator,
        )
        self.guild = guild
        self.roles = list(roles or [])
        self.top_role = self.roles[0] if self.roles else FakeRole(0, position=1)
        self.guild_permissions = FakePermissionSnapshot(
            manage_guild=manage_guild,
            manage_channels=manage_channels,
            moderate_members=moderate_members,
            administrator=administrator,
        )
        self.joined_at = joined_at or ge.now_utc()
        self.bot = False
        self.timed_out_until = None
        self.communication_disabled_until = None
        self.timeout_calls = []
        self.raise_forbidden_on_timeout = False

    async def timeout(self, until, reason=None):
        if self.raise_forbidden_on_timeout:
            response = types.SimpleNamespace(status=403, reason="Forbidden", headers={})
            raise discord.Forbidden(response=response, message="missing permissions")
        self.timed_out_until = until
        self.communication_disabled_until = until
        self.timeout_calls.append({"until": until, "reason": reason})


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

    def register_message(self, message: FakeMessage):
        self._messages[message.id] = message

    def get_partial_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            message = FakeMessage(channel=self, message_id=message_id)
            self._messages[message.id] = message
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
        return FakeMessage(channel=self.channel, **kwargs)

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

    async def _panel_message(self, view) -> FakeMessage:
        channel = FakeChannel(900, name="panel-ui")
        message = FakeMessage(channel=channel, embed=await view.current_embed(), view=view)
        channel.register_message(message)
        view.message = message
        return message

    def _view_interaction(self, *, message: FakeMessage | None = None, user: object | None = None, **kwargs) -> FakeInteraction:
        target_message = message or FakeMessage(channel=FakeChannel(901))
        return FakeInteraction(
            user=user or FakeAuthor(manage_guild=True),
            guild=self.guild,
            message=target_message,
            channel=target_message.channel,
            **kwargs,
        )

    def _button(self, view, label: str):
        return next(child for child in view.children if getattr(child, "label", None) == label)

    def _select(self, view, cls, *, placeholder_contains: str | None = None):
        for child in view.children:
            if isinstance(child, cls) and (placeholder_contains is None or placeholder_contains in getattr(child, "placeholder", "")):
                return child
        raise AssertionError(f"missing {cls.__name__} with placeholder {placeholder_contains!r}")

    def _assert_embed_valid(self, embed: discord.Embed):
        total = len(embed.title or "") + len(embed.description or "") + len(getattr(embed.footer, "text", "") or "")
        for field in embed.fields:
            total += len(field.name or "") + len(field.value or "")
            self.assertLessEqual(len(field.name or ""), 256)
            self.assertLessEqual(len(field.value or ""), 1024)
        self.assertLessEqual(len(embed.title or ""), 256)
        self.assertLessEqual(len(embed.description or ""), 4096)
        self.assertLessEqual(total, 6000)

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

    async def test_lock_channel_reports_private_failure_when_service_raises_after_defer(self):
        channel = FakeChannel(230)
        self.guild.channels[channel.id] = channel

        async def failing_lock_channel(*args, **kwargs):
            raise RuntimeError("write failed")

        self.cog.service.lock_channel = failing_lock_channel
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
            post_notice=True,
        )

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("could not finish the emergency lock action", ctx.send_calls[0]["embed"].description.lower())

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

    async def test_timeout_remove_allows_timeout_moderator(self):
        actor_role = FakeRole(500, position=40, name="Moderator")
        actor = FakeMember(50, self.guild, roles=[actor_role], moderate_members=True)
        target_role = FakeRole(501, position=10, name="Member")
        member = FakeMember(51, self.guild, roles=[target_role])
        member.timed_out_until = ge.now_utc() + timedelta(minutes=15)
        member.communication_disabled_until = member.timed_out_until
        self.guild.members[actor.id] = actor
        self.guild.members[member.id] = member
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(220),
            author=actor,
        )

        await AdminCog.timeout_remove_command.callback(self.cog, ctx, member=member, reason="Appeal accepted")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Removed the timeout", ctx.send_calls[0]["embed"].description)
        self.assertEqual(len(member.timeout_calls), 1)
        self.assertIsNone(member.timed_out_until)

    async def test_timeout_remove_denies_member_without_timeout_access(self):
        actor_role = FakeRole(510, position=40, name="Helper")
        actor = FakeMember(52, self.guild, roles=[actor_role])
        target_role = FakeRole(511, position=10, name="Member")
        member = FakeMember(53, self.guild, roles=[target_role])
        member.timed_out_until = ge.now_utc() + timedelta(minutes=15)
        member.communication_disabled_until = member.timed_out_until
        self.guild.members[actor.id] = actor
        self.guild.members[member.id] = member
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(221),
            author=actor,
        )

        await AdminCog.timeout_remove_command.callback(self.cog, ctx, member=member, reason=None)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Timeout Members", ctx.send_calls[0]["embed"].description)

    async def test_timeout_remove_handles_member_without_active_timeout(self):
        actor_role = FakeRole(520, position=40, name="Moderator")
        actor = FakeMember(54, self.guild, roles=[actor_role], moderate_members=True)
        member = FakeMember(55, self.guild, roles=[FakeRole(521, position=10, name="Member")])
        self.guild.members[actor.id] = actor
        self.guild.members[member.id] = member
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(222),
            author=actor,
        )

        await AdminCog.timeout_remove_command.callback(self.cog, ctx, member=member, reason=None)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("not currently timed out", ctx.send_calls[0]["embed"].description)

    async def test_timeout_remove_reports_private_failure_when_service_raises_after_defer(self):
        actor_role = FakeRole(522, position=40, name="Moderator")
        actor = FakeMember(56, self.guild, roles=[actor_role], moderate_members=True)
        member = FakeMember(57, self.guild, roles=[FakeRole(523, position=10, name="Member")])
        self.guild.members[actor.id] = actor
        self.guild.members[member.id] = member

        async def failing_remove_timeout(*args, **kwargs):
            raise RuntimeError("timeout write failed")

        self.cog.service.remove_timeout = failing_remove_timeout
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(224),
            author=actor,
        )

        await AdminCog.timeout_remove_command.callback(self.cog, ctx, member=member, reason="Appeal accepted")

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("could not finish the timeout removal", ctx.send_calls[0]["embed"].description.lower())

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

    async def test_admin_permissions_surfaces_missing_timeout_members_for_timeout_remove(self):
        self.guild.me.guild_permissions = FakePermissionSnapshot(
            manage_roles=True,
            manage_channels=True,
            manage_messages=True,
            moderate_members=False,
            kick_members=True,
            mention_everyone=True,
        )
        ctx = FakeContext(
            interaction=FakeInteraction(),
            guild=self.guild,
            channel=FakeChannel(223),
            author=FakeAuthor(manage_guild=True),
        )

        await AdminCog.admin_permissions_command.callback(self.cog, ctx)

        embed = ctx.send_calls[0]["embed"]
        missing = next(field for field in embed.fields if field.name == "Missing Server Permissions")
        self.assertIn("Timeout Members", missing.value)
        self.assertIn("/timeout remove", missing.value)

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
        ok, _ = await self.cog.service.set_logs_config(
            self.guild.id,
            channel_id=blocked_log_channel.id,
            alert_role_id=alert_role.id,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "overview")
        operability = next(field for field in embed.fields if field.name == "Operability")

        self.assertIn("at or above", operability.value)
        self.assertIn("cannot see", operability.value)
        self.assertIn("cannot send", operability.value)
        self.assertIn("cannot embed", operability.value)
        self.assertIn("cannot ping", operability.value)

    async def test_cog_load_registers_followup_review_views(self):
        record_followup = {
            "guild_id": self.guild.id,
            "user_id": 501,
            "review_version": 2,
            "review_message_id": 1501,
        }
        self.cog.service.start = AsyncMock(return_value=True)
        self.cog.service.list_review_views = AsyncMock(return_value=[record_followup])

        await self.cog.cog_load()

        self.assertEqual(len(self.bot.views), 1)
        self.assertIsInstance(self.bot.views[0][0], FollowupReviewView)
        self.assertEqual(self.bot.views[0][1], 1501)

    async def test_admin_panel_only_shows_focused_sections(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)

        labels = [child.label for child in view.children]

        self.assertEqual(
            labels,
            [
                "Overview",
                "Follow-up",
                "Exclusions",
                "Logs",
                "Refresh",
                "Edit Follow-up",
                "Edit Logs",
                "Run Permission Check",
            ],
        )
        self.assertNotIn("Risk", labels)
        self.assertNotIn("Emergency", labels)
        self.assertNotIn("Permissions", labels)

    async def test_admin_panel_section_switching_updates_contextual_actions(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        message = await self._panel_message(view)
        interaction = self._view_interaction(message=message)

        await self._button(view, "Exclusions").callback(interaction)

        labels = [child.label for child in view.children if hasattr(child, "label")]
        self.assertEqual(view.section, "exclusions")
        self.assertEqual(message.embed.title, "Exclusions And Trusted Roles")
        self.assertIn("Edit Exclusions", labels)
        self.assertNotIn("Run Permission Check", labels)
        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(interaction.response.defer_calls[0][1]["thinking"], False)

    async def test_admin_panel_wrong_user_is_locked_out_privately(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        interaction = self._view_interaction(user=FakeAuthor(user_id=2, manage_guild=True))

        allowed = await view.interaction_check(interaction)

        self.assertFalse(allowed)
        self.assertTrue(interaction.response.sent_messages)
        payload = interaction.response.sent_messages[-1]["kwargs"]
        self.assertTrue(payload["ephemeral"])
        self.assertEqual(payload["embed"].title, "This Panel Is Locked")

    async def test_admin_panel_expired_interaction_fails_gracefully(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        view._expired = True
        interaction = self._view_interaction()

        allowed = await view.interaction_check(interaction)

        self.assertFalse(allowed)
        self.assertTrue(interaction.response.sent_messages)
        self.assertIn("expired", interaction.response.sent_messages[-1]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_timeout_disables_controls(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1, section="exclusions")
        message = await self._panel_message(view)

        await view.on_timeout()

        self.assertTrue(view._expired)
        self.assertTrue(all(child.disabled for child in view.children))
        self.assertIs(message.view, view)

    async def test_admin_panel_stale_edit_falls_back_to_private_warning(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        interaction = FakeInteraction(
            user=FakeAuthor(manage_guild=True),
            guild=self.guild,
            message=None,
            channel=FakeChannel(920),
            edit_original_response_exception=discord.ClientException("edit failed"),
        )

        await self._button(view, "Exclusions").callback(interaction)

        self.assertTrue(interaction.followup_calls)
        embed = interaction.followup_calls[-1]["kwargs"]["embed"]
        self.assertIn("expired", embed.description.lower())

    async def test_admin_panel_overview_tools_open_permission_diagnostics(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        message = await self._panel_message(view)

        permission_interaction = self._view_interaction(message=message)
        await self._button(view, "Run Permission Check").callback(permission_interaction)
        self.assertTrue(permission_interaction.followup_calls)
        self.assertEqual(permission_interaction.followup_calls[-1]["kwargs"]["embed"].title, "Babblebox Permission Health")

    async def test_admin_panel_overview_quick_config_buttons_open_direct_editors(self):
        view = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1)
        message = await self._panel_message(view)

        followup_interaction = self._view_interaction(message=message)
        await self._button(view, "Edit Follow-up").callback(followup_interaction)
        self.assertIsInstance(followup_interaction._last_followup_message.view, FollowupEditorView)

        logs_interaction = self._view_interaction(message=message)
        await self._button(view, "Edit Logs").callback(logs_interaction)
        self.assertIsInstance(logs_interaction._last_followup_message.view, LogsEditorView)

    async def test_followup_panel_editor_updates_role_and_parent_panel(self):
        panel = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1, section="followup")
        panel_message = await self._panel_message(panel)
        open_interaction = self._view_interaction(message=panel_message)

        await self._button(panel, "Edit Follow-up").callback(open_interaction)

        child_message = open_interaction._last_followup_message
        child_view = child_message.view
        self.assertIsInstance(child_view, FollowupEditorView)

        role_select = self._select(child_view, discord.ui.RoleSelect, placeholder_contains="Follow-up role")
        role_select._values = [types.SimpleNamespace(id=self.followup_role.id)]
        update_interaction = self._view_interaction(message=child_message)

        await role_select.callback(update_interaction)

        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["followup_role_id"], self.followup_role.id)
        self.assertIn(self.followup_role.mention, panel_message.embed.fields[0].value)
        self.assertTrue(update_interaction.followup_calls)
        self.assertIn(self.followup_role.mention, update_interaction.followup_calls[-1]["kwargs"]["embed"].description)

    async def test_followup_editor_custom_duration_modal_updates_state_and_parent_panel(self):
        panel = AdminPanelView(self.cog, guild_id=self.guild.id, author_id=1, section="followup")
        panel_message = await self._panel_message(panel)
        view = FollowupEditorView(self.cog, guild_id=self.guild.id, author_id=1, panel_view=panel)
        child_message = await self._panel_message(view)
        interaction = self._view_interaction(message=child_message)

        await self._button(view, "Custom Duration").callback(interaction)

        modal = interaction.response.modal_calls[-1]
        modal.value_input._value = "6w"
        submit_interaction = self._view_interaction(message=child_message)

        await modal.on_submit(submit_interaction)

        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["followup_duration_value"], 6)
        self.assertEqual(config["followup_duration_unit"], "weeks")
        self.assertIn("6 weeks", panel_message.embed.fields[0].value)
        self.assertTrue(submit_interaction.response.sent_messages)

    async def test_followup_editor_modal_open_failure_returns_private_feedback(self):
        view = FollowupEditorView(self.cog, guild_id=self.guild.id, author_id=1)
        child_message = await self._panel_message(view)
        interaction = self._view_interaction(message=child_message)
        interaction.response.send_modal = mock.AsyncMock(side_effect=RuntimeError("modal boom"))

        await self._button(view, "Custom Duration").callback(interaction)

        self.assertTrue(interaction.response.sent_messages)
        self.assertIn("could not open", interaction.response.sent_messages[-1]["kwargs"]["embed"].description.lower())

    async def test_logs_editor_updates_and_clears_delivery_targets(self):
        alert_role = FakeRole(91, position=11, name="Moderators")
        second_channel = FakeChannel(33, name="staff-log")
        self.guild.roles[alert_role.id] = alert_role
        self.guild.channels[second_channel.id] = second_channel
        view = LogsEditorView(self.cog, guild_id=self.guild.id, author_id=1)
        child_message = await self._panel_message(view)

        channel_select = self._select(view, discord.ui.ChannelSelect, placeholder_contains="Admin log channel")
        channel_select._values = [types.SimpleNamespace(id=second_channel.id)]
        await channel_select.callback(self._view_interaction(message=child_message))

        role_select = self._select(view, discord.ui.RoleSelect, placeholder_contains="Admin alert role")
        role_select._values = [types.SimpleNamespace(id=alert_role.id)]
        await role_select.callback(self._view_interaction(message=child_message))

        await self._button(view, "Clear Channel").callback(self._view_interaction(message=child_message))
        await self._button(view, "Clear Alert Role").callback(self._view_interaction(message=child_message))

        config = self.cog.service.get_config(self.guild.id)
        self.assertIsNone(config["admin_log_channel_id"])
        self.assertIsNone(config["admin_alert_role_id"])

    async def test_exclusions_editor_replaces_lists_clears_and_toggles(self):
        member = FakeMember(55, self.guild, roles=[])
        self.guild.members[member.id] = member
        trusted_role = FakeRole(92, position=9, name="Trusted")
        self.guild.roles[trusted_role.id] = trusted_role
        view = ExclusionsEditorView(self.cog, guild_id=self.guild.id, author_id=1)
        child_message = await self._panel_message(view)
        starting = self.cog.service.get_config(self.guild.id)["followup_exempt_staff"]

        user_select = self._select(view, discord.ui.UserSelect, placeholder_contains="Excluded members")
        user_select._values = [types.SimpleNamespace(id=member.id)]
        await user_select.callback(self._view_interaction(message=child_message))

        excluded_role_select = self._select(view, discord.ui.RoleSelect, placeholder_contains="Excluded roles")
        excluded_role_select._values = [types.SimpleNamespace(id=self.followup_role.id)]
        await excluded_role_select.callback(self._view_interaction(message=child_message))

        trusted_role_select = self._select(view, discord.ui.RoleSelect, placeholder_contains="Trusted roles")
        trusted_role_select._values = [types.SimpleNamespace(id=trusted_role.id)]
        await trusted_role_select.callback(self._view_interaction(message=child_message))

        await self._button(view, "Clear Members").callback(self._view_interaction(message=child_message))
        toggle = next(child for child in view.children if getattr(child, "label", "").startswith("Follow-up Staff:"))
        await toggle.callback(self._view_interaction(message=child_message))

        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["excluded_user_ids"], [])
        self.assertEqual(config["excluded_role_ids"], [self.followup_role.id])
        self.assertEqual(config["trusted_role_ids"], [trusted_role.id])
        self.assertEqual(config["followup_exempt_staff"], (not starting))

    async def test_admin_panel_embeds_stay_compact_for_dense_config(self):
        dense = self.cog.service.get_config(self.guild.id)
        dense["excluded_user_ids"] = list(range(100, 120))
        dense["excluded_role_ids"] = list(range(200, 220))
        dense["trusted_role_ids"] = list(range(300, 320))
        await self.cog.service.store.upsert_config(dense)
        self.cog.service._compiled_configs.pop(self.guild.id, None)

        for section in ("overview", "followup", "exclusions", "logs"):
            with self.subTest(section=section):
                embed = await self.cog.build_panel_embed(self.guild.id, section)
                self._assert_embed_valid(embed)



