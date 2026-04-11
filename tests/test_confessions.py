from __future__ import annotations

import ast
import asyncio
import json
import types
import unittest
from pathlib import Path
from unittest import mock

import discord

from babblebox.cogs.confessions import (
    AppealModal,
    ConfessionComposerModal,
    ConfessionsCog,
    EditConfessionModal,
    OwnerReplyComposerModal,
    ReplyComposerModal,
    ReportModal,
    StatelessConfessionMemberPanelView,
    StatelessConfessionsAdminPanelView,
    StatelessOwnerReplyPromptView,
    StatelessPublishedConfessionReplyView,
    _validate_discord_modal_payload,
)
from babblebox.confessions_service import CASE_ID_PREFIX, CONFESSION_ID_PREFIX, ConfessionSubmissionResult, ConfessionsService
from babblebox.confessions_store import ConfessionsStore, _PostgresConfessionsStore
from tests.test_confessions_store import _FakeConnection, _FakePool, _privacy


def _embed_fields_by_name(embed: discord.Embed) -> dict[str, dict[str, object]]:
    return {field["name"]: field for field in embed.to_dict().get("fields", [])}


def _view_custom_ids(view) -> list[str]:
    if view is None:
        return []
    return [child.custom_id for child in view.children if getattr(child, "custom_id", None)]


class FakeGuildPermissions:
    def __init__(self, *, administrator: bool = False, manage_guild: bool = False):
        self.administrator = administrator
        self.manage_guild = manage_guild


class FakeChannelPermissions:
    def __init__(self, **values):
        self.__dict__.update(values)

    def __getattr__(self, name: str):
        return False


class FakeRole:
    def __init__(self, role_id: int, *, name: str | None = None, guild=None):
        self.id = role_id
        self.name = name or f"Role {role_id}"
        self.mention = f"<@&{role_id}>"
        self.guild = guild

    def is_default(self):
        return self.guild is not None and self.id == self.guild.id


class FakeUser:
    def __init__(self, user_id: int, *, manage_guild: bool = False, roles: list[FakeRole] | None = None):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions(manage_guild=manage_guild, administrator=manage_guild)
        self.roles = list(roles or [])
        self.sent: list[FakeMessage] = []

    async def send(self, content=None, embed=None, embeds=None, view=None, ephemeral=None, allowed_mentions=None, **kwargs):
        message = FakeMessage(
            content=content,
            embed=embed,
            embeds=embeds,
            view=view,
            ephemeral=ephemeral,
            allowed_mentions=allowed_mentions,
            author=None,
            guild=None,
            channel=None,
        )
        self.sent.append(message)
        return message


class FakeMessage:
    _next_id = 1000

    def __init__(
        self,
        *,
        content=None,
        embed=None,
        embeds=None,
        view=None,
        ephemeral=None,
        allowed_mentions=None,
        author=None,
        guild=None,
        channel=None,
        reference=None,
        attachments=None,
    ):
        self.id = FakeMessage._next_id
        FakeMessage._next_id += 1
        self.content = content
        self.embed = embed or (embeds[0] if embeds else None)
        self.embeds = list(embeds or ([embed] if embed is not None else []))
        self.view = view
        self.ephemeral = ephemeral
        self.allowed_mentions = allowed_mentions
        self.author = author
        self.guild = guild
        self.channel = channel
        self.reference = reference
        self.attachments = list(attachments or [])
        self.thread = None
        self.jump_url = (
            f"https://discord.com/channels/{guild.id}/{channel.id}/{self.id}"
            if guild is not None and channel is not None
            else None
        )
        self.deleted = False
        self.edits = []

    async def edit(self, **kwargs):
        self.edits.append(kwargs)
        if "content" in kwargs:
            self.content = kwargs["content"]
        if "embed" in kwargs:
            self.embed = kwargs["embed"]
            self.embeds = [kwargs["embed"]] if kwargs["embed"] is not None else []
        if "embeds" in kwargs:
            self.embeds = list(kwargs["embeds"] or [])
            self.embed = self.embeds[0] if self.embeds else None
        if "view" in kwargs:
            self.view = kwargs["view"]
        return self

    async def delete(self):
        self.deleted = True
        if self.channel is not None:
            self.channel._messages.pop(self.id, None)

    async def create_thread(self, *, name: str, auto_archive_duration=None, slowmode_delay=None, reason=None):
        create_thread = getattr(self.channel, "create_thread_from_message", None)
        if not callable(create_thread):
            raise RuntimeError("thread creation unavailable")
        return await create_thread(
            self,
            name=name,
            auto_archive_duration=auto_archive_duration,
            slowmode_delay=slowmode_delay,
            reason=reason,
        )


class FakeChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        name: str = "general",
        public_view: bool = False,
        bot_can_view: bool = True,
        bot_can_send: bool = True,
        bot_can_embed: bool = True,
        bot_can_create_public_threads: bool = True,
        bot_can_send_in_threads: bool = True,
        bot_can_manage_threads: bool = True,
        bot_can_read_history: bool = True,
    ):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self.guild = None
        self.public_view = public_view
        self.bot_can_view = bot_can_view
        self.bot_can_send = bot_can_send
        self.bot_can_embed = bot_can_embed
        self.bot_can_create_public_threads = bot_can_create_public_threads
        self.bot_can_send_in_threads = bot_can_send_in_threads
        self.bot_can_manage_threads = bot_can_manage_threads
        self.bot_can_read_history = bot_can_read_history
        self.sent: list[FakeMessage] = []
        self._messages: dict[int, FakeMessage] = {}

    async def send(self, content=None, embed=None, embeds=None, view=None, ephemeral=None, allowed_mentions=None, **kwargs):
        _validate_message_payload(embed=embed, embeds=embeds, view=view)
        message = FakeMessage(
            content=content,
            embed=embed,
            embeds=embeds,
            view=view,
            ephemeral=ephemeral,
            allowed_mentions=allowed_mentions,
            guild=self.guild,
            channel=self,
        )
        self.sent.append(message)
        self._messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            raise Exception("missing")
        return message

    async def history(self, *, limit: int | None = None):
        remaining = None if limit is None else max(0, int(limit))
        for message in reversed(list(self._messages.values())):
            if message.deleted:
                continue
            if remaining is not None and remaining <= 0:
                break
            if remaining is not None:
                remaining -= 1
            yield message

    def permissions_for(self, target):
        is_default = getattr(target, "is_default", None)
        if callable(is_default) and is_default():
            return FakeChannelPermissions(view_channel=self.public_view)
        if getattr(target, "id", None) == 999:
            return FakeChannelPermissions(
                view_channel=self.bot_can_view,
                send_messages=self.bot_can_send,
                embed_links=self.bot_can_embed,
                create_public_threads=self.bot_can_create_public_threads,
                send_messages_in_threads=self.bot_can_send_in_threads,
                manage_threads=self.bot_can_manage_threads,
                read_message_history=self.bot_can_read_history,
            )
        return FakeChannelPermissions(
            view_channel=True,
            send_messages=True,
            embed_links=True,
            create_public_threads=True,
            send_messages_in_threads=True,
            manage_threads=True,
            read_message_history=True,
        )

    async def create_thread_from_message(self, message: FakeMessage, *, name: str, auto_archive_duration=None, slowmode_delay=None, reason=None):
        if not self.bot_can_create_public_threads or not self.bot_can_send_in_threads:
            raise RuntimeError("thread creation unavailable")
        thread = FakeThread(name=name, parent=self)
        if self.guild is not None:
            self.guild.add_thread(thread)
        message.thread = thread
        return thread


class FakeThread(FakeChannel):
    _next_id = 5000

    def __init__(self, *, name: str, parent: FakeChannel, archived: bool = False, locked: bool = False):
        super().__init__(
            FakeThread._next_id,
            name=name,
            public_view=parent.public_view,
            bot_can_view=parent.bot_can_view,
            bot_can_send=parent.bot_can_send,
            bot_can_embed=parent.bot_can_embed,
            bot_can_create_public_threads=parent.bot_can_create_public_threads,
            bot_can_send_in_threads=parent.bot_can_send_in_threads,
            bot_can_manage_threads=parent.bot_can_manage_threads,
            bot_can_read_history=parent.bot_can_read_history,
        )
        FakeThread._next_id += 1
        self.parent = parent
        self.archived = archived
        self.locked = locked
        self.deleted = False

    async def send(self, content=None, embed=None, embeds=None, view=None, ephemeral=None, allowed_mentions=None, **kwargs):
        if self.deleted or self.locked or self.archived or not self.bot_can_send_in_threads:
            raise RuntimeError("thread unavailable")
        return await super().send(
            content=content,
            embed=embed,
            embeds=embeds,
            view=view,
            ephemeral=ephemeral,
            allowed_mentions=allowed_mentions,
            **kwargs,
        )

    async def edit(self, *, archived=None, locked=None, name=None, reason=None, **kwargs):
        if archived is not None:
            self.archived = bool(archived)
        if locked is not None:
            self.locked = bool(locked)
        if name is not None:
            self.name = name
        return self

    async def delete(self, *, reason=None):
        self.deleted = True
        if self.guild is not None:
            self.guild.threads.pop(self.id, None)


class FakeGuild:
    def __init__(self, guild_id: int):
        self.id = guild_id
        self.name = f"Guild {guild_id}"
        self.channels: dict[int, FakeChannel] = {}
        self.threads: dict[int, FakeThread] = {}
        self.roles: dict[int, FakeRole] = {}
        self.members: dict[int, FakeUser] = {}
        self.default_role = FakeRole(guild_id, name="@everyone", guild=self)
        self.me = types.SimpleNamespace(id=999)
        self.roles[self.default_role.id] = self.default_role

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)

    def get_thread(self, channel_id: int):
        return self.threads.get(channel_id)

    def get_channel_or_thread(self, channel_id: int):
        return self.get_channel(channel_id) or self.get_thread(channel_id)

    def get_role(self, role_id: int):
        return self.roles.get(role_id)

    def add_role(self, role: FakeRole):
        role.guild = self
        self.roles[role.id] = role
        return role

    def get_member(self, user_id: int):
        return self.members.get(user_id)

    def add_member(self, member: FakeUser):
        self.members[member.id] = member
        return member

    def add_channel(self, channel: FakeChannel):
        channel.guild = self
        self.channels[channel.id] = channel
        return channel

    def add_thread(self, thread: FakeThread):
        thread.guild = self
        self.threads[thread.id] = thread
        return thread


class FakeMessageReference:
    def __init__(self, *, message_id: int, resolved=None, cached_message=None):
        self.message_id = message_id
        self.resolved = resolved
        self.cached_message = cached_message


class FakeAttachment:
    def __init__(self, filename: str, *, content_type: str = "image/png", size: int = 1024, url: str | None = None):
        self.filename = filename
        self.content_type = content_type
        self.size = size
        self.url = url or f"https://cdn.discordapp.com/attachments/1/2/{filename}"
        self.width = 100
        self.height = 100

    def is_spoiler(self):
        return False


class FakeRawDeletePayload:
    def __init__(self, *, guild_id: int, message_id: int):
        self.guild_id = guild_id
        self.message_id = message_id


def _validate_modal_payload(payload: dict[str, object]):
    _validate_discord_modal_payload(payload)


def _validate_embed_payload(embed: discord.Embed | None):
    if embed is None:
        return
    payload = embed.to_dict()
    title = payload.get("title")
    if title and len(str(title)) > 256:
        raise ValueError("embed title exceeds Discord limit")
    description = payload.get("description")
    if description and len(str(description)) > 4096:
        raise ValueError("embed description exceeds Discord limit")
    footer = payload.get("footer") or {}
    footer_text = footer.get("text")
    if footer_text and len(str(footer_text)) > 2048:
        raise ValueError("embed footer exceeds Discord limit")
    fields = payload.get("fields") or []
    if len(fields) > 25:
        raise ValueError("embed field count exceeds Discord limit")
    for field in fields:
        name = field.get("name")
        value = field.get("value")
        if name and len(str(name)) > 256:
            raise ValueError("embed field name exceeds Discord limit")
        if value and len(str(value)) > 1024:
            raise ValueError("embed field value exceeds Discord limit")


def _validate_view_payload(view):
    if view is None:
        return
    children = list(getattr(view, "children", []) or [])
    if len(children) > 25:
        raise ValueError("view child count exceeds Discord limit")
    for child in children:
        custom_id = getattr(child, "custom_id", None)
        if custom_id and len(str(custom_id)) > 100:
            raise ValueError("component custom_id exceeds Discord limit")
        label = getattr(child, "label", None)
        if label and len(str(label)) > 80:
            raise ValueError("button label exceeds Discord limit")
        placeholder = getattr(child, "placeholder", None)
        if placeholder and len(str(placeholder)) > 150:
            raise ValueError("select placeholder exceeds Discord limit")
        options = list(getattr(child, "options", []) or [])
        if len(options) > 25:
            raise ValueError("select option count exceeds Discord limit")
        for option in options:
            if option.label and len(str(option.label)) > 100:
                raise ValueError("select option label exceeds Discord limit")
            if option.description and len(str(option.description)) > 100:
                raise ValueError("select option description exceeds Discord limit")
            if option.value and len(str(option.value)) > 100:
                raise ValueError("select option value exceeds Discord limit")


def _validate_message_payload(*, embed=None, embeds=None, view=None):
    embed_list = list(embeds or ([] if embed is None else [embed]))
    if len(embed_list) > 10:
        raise ValueError("embed count exceeds Discord limit")
    for item in embed_list:
        _validate_embed_payload(item)
    _validate_view_payload(view)


class FakeResponse:
    def __init__(self):
        self._done = False
        self.defer_calls = []
        self.sent = []
        self.edits = []
        self.modal_calls = []
        self.modal_payloads = []

    def is_done(self):
        return self._done

    async def defer(self, *, ephemeral=False, thinking=False):
        self._done = True
        self.defer_calls.append({"ephemeral": ephemeral, "thinking": thinking})

    async def send_message(self, *args, **kwargs):
        _validate_message_payload(embed=kwargs.get("embed"), embeds=kwargs.get("embeds"), view=kwargs.get("view"))
        self._done = True
        self.sent.append({"args": args, "kwargs": kwargs})
        return FakeMessage(
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
            embeds=kwargs.get("embeds"),
            view=kwargs.get("view"),
            ephemeral=kwargs.get("ephemeral"),
            allowed_mentions=kwargs.get("allowed_mentions"),
        )

    async def edit_message(self, **kwargs):
        _validate_message_payload(embed=kwargs.get("embed"), embeds=kwargs.get("embeds"), view=kwargs.get("view"))
        self._done = True
        self.edits.append(kwargs)

    async def send_modal(self, modal):
        payload = modal.to_dict()
        _validate_modal_payload(payload)
        self._done = True
        self.modal_calls.append(modal)
        self.modal_payloads.append(payload)


class FakeInteraction:
    def __init__(self, *, guild=None, user=None, message=None, client=None):
        self.guild = guild
        self.user = user
        self.message = message
        self.client = client
        self.response = FakeResponse()
        self.followup = types.SimpleNamespace(send=self._followup_send)
        self.followup_calls = []
        self.original_response_edits = []

    async def _followup_send(self, *args, **kwargs):
        _validate_message_payload(embed=kwargs.get("embed"), embeds=kwargs.get("embeds"), view=kwargs.get("view"))
        self.followup_calls.append({"args": args, "kwargs": kwargs})
        return FakeMessage(
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
            embeds=kwargs.get("embeds"),
            view=kwargs.get("view"),
            ephemeral=kwargs.get("ephemeral"),
            allowed_mentions=kwargs.get("allowed_mentions"),
        )

    async def edit_original_response(self, **kwargs):
        _validate_message_payload(embed=kwargs.get("embed"), embeds=kwargs.get("embeds"), view=kwargs.get("view"))
        self.original_response_edits.append(kwargs)
        return FakeMessage(
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
            embeds=kwargs.get("embeds"),
            view=kwargs.get("view"),
            ephemeral=True if self.guild is not None else None,
            allowed_mentions=kwargs.get("allowed_mentions"),
        )

    def is_expired(self):
        return False


class FakeContext:
    def __init__(self, *, guild=None, author=None):
        self.guild = guild
        self.author = author
        self.interaction = FakeInteraction(guild=guild, user=author)
        self.send_calls = []
        self.defer_calls = []
        self.channel = next(iter(guild.channels.values())) if guild and guild.channels else None
        self.message = None

    async def send(self, **kwargs):
        self.send_calls.append(kwargs)
        return FakeMessage(**kwargs)

    async def defer(self, **kwargs):
        self.defer_calls.append(kwargs)
        self.interaction.response._done = True


class FakeBot:
    def __init__(self, guilds: list[FakeGuild]):
        self.loop = asyncio.get_running_loop()
        self.user = types.SimpleNamespace(id=999)
        self._guilds = {guild.id: guild for guild in guilds}
        self._cog = None
        self.views = []
        self._ready = False

    def get_channel(self, channel_id: int):
        for guild in self._guilds.values():
            channel = guild.get_channel_or_thread(channel_id)
            if channel is not None:
                return channel
        return None

    async def fetch_channel(self, channel_id: int):
        return self.get_channel(channel_id)

    def get_guild(self, guild_id: int):
        return self._guilds.get(guild_id)

    def get_user(self, user_id: int):
        for guild in self._guilds.values():
            member = guild.get_member(user_id)
            if member is not None:
                return member
        return None

    @property
    def guilds(self):
        return list(self._guilds.values())

    async def fetch_user(self, user_id: int):
        return self.get_user(user_id)

    def add_view(self, view, *, message_id=None):
        self.views.append((view, message_id))

    def get_cog(self, name: str):
        if name == "ConfessionsCog":
            return self._cog
        return None

    def is_ready(self):
        return self._ready


class ServiceCogStub:
    def __init__(self, service):
        self.service = service

    def build_member_panel_view(self, *, guild_id: int):
        return types.SimpleNamespace(
            send_button=types.SimpleNamespace(disabled=self.service.operability_message(guild_id) != "Confessions are ready."),
            children=[],
        )

    def build_review_view(self, *, case_id: str, version: int):
        return types.SimpleNamespace(case_id=case_id, version=version, children=[])

    def build_support_ticket_view(self, *, ticket_id: str, kind: str, actionable: bool):
        labels = ["Resolve", "Details", "Refresh"]
        if actionable:
            labels.insert(1, "False Positive" if kind == "appeal" else "Delete")
        return types.SimpleNamespace(
            ticket_id=ticket_id,
            kind=kind,
            actionable=actionable,
            children=[types.SimpleNamespace(label=label, custom_id=f"bb-confession-support:test:{ticket_id}") for label in labels],
        )

    def build_public_confession_view(self, *, guild_id: int, show_create: bool = False, show_reply: bool = True):
        children = []
        if show_create:
            children.append(types.SimpleNamespace(custom_id="bb-confession-post:compose"))
        if show_reply:
            children.append(types.SimpleNamespace(custom_id="bb-confession-post:reply"))
        return types.SimpleNamespace(guild_id=guild_id, children=children)

    def build_owner_reply_prompt_view(self):
        return types.SimpleNamespace(
            children=[
                types.SimpleNamespace(custom_id="bb-confession-owner-reply:open"),
                types.SimpleNamespace(custom_id="bb-confession-owner-reply:dismiss"),
            ]
        )


class _PostgresStoreFacade:
    def __init__(self, inner_store):
        self._inner_store = inner_store
        self.backend_name = getattr(inner_store, "backend_name", "postgres")

    async def load(self):
        await self._inner_store.load()

    async def close(self):
        await self._inner_store.close()

    def __getattr__(self, name: str):
        return getattr(self._inner_store, name)


class ConfessionsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.confession_channel = FakeChannel(20, name="confessions")
        self.review_channel = FakeChannel(30, name="confession-review")
        self.panel_channel = FakeChannel(40, name="confession-panel")
        self.appeals_channel = FakeChannel(50, name="confession-appeals")
        self.allowed_role = self.guild.add_role(FakeRole(501, name="Allowed"))
        self.blocked_role = self.guild.add_role(FakeRole(502, name="Blocked"))
        self.guild.add_channel(self.confession_channel)
        self.guild.add_channel(self.review_channel)
        self.guild.add_channel(self.panel_channel)
        self.guild.add_channel(self.appeals_channel)
        self.other_guild = FakeGuild(11)
        self.other_confession_channel = FakeChannel(21, name="other-confessions")
        self.other_allowed_role = self.other_guild.add_role(FakeRole(601, name="Other Allowed"))
        self.other_guild.add_channel(self.other_confession_channel)
        self.bot = FakeBot([self.guild, self.other_guild])
        self.store = ConfessionsStore(backend="memory")
        self.service = ConfessionsService(self.bot, store=self.store)
        await self.service.start()
        self.bot._cog = ServiceCogStub(self.service)

    async def asyncTearDown(self):
        await self.service.close()

    def _member(
        self,
        user_id: int,
        *,
        guild: FakeGuild | None = None,
        roles: list[FakeRole] | None = None,
        manage_guild: bool = False,
    ) -> FakeUser:
        target = guild or self.guild
        member = FakeUser(user_id, manage_guild=manage_guild, roles=roles)
        target.add_member(member)
        return member

    def _reply_message(
        self,
        *,
        guild: FakeGuild | None = None,
        channel: FakeChannel | None = None,
        author: FakeUser | None = None,
        reply_to_message_id: int,
        content: str,
    ) -> FakeMessage:
        target_guild = guild or self.guild
        target_channel = channel or self.confession_channel
        message = FakeMessage(
            content=content,
            author=author or self._member(9900, guild=target_guild),
            guild=target_guild,
            channel=target_channel,
            reference=FakeMessageReference(message_id=reply_to_message_id),
        )
        target_channel._messages[message.id] = message
        return message

    async def _configure(
        self,
        *,
        guild: FakeGuild | None = None,
        review_mode: bool = False,
        review_channel: bool = False,
        appeals_channel: bool = False,
        adult_block: bool = True,
        panel: bool = False,
        allow_images: bool | None = None,
        image_review_required: bool | None = None,
        allow_replies: bool | None = None,
        anonymous_reply_review_required: bool | None = None,
        allow_self_edit: bool | None = None,
        link_mode: str | None = None,
    ):
        target = guild or self.guild
        ok, message = await self.service.configure_guild(
            target.id,
            enabled=True,
            confession_channel_id=next(iter(target.channels.values())).id,
            panel_channel_id=self.panel_channel.id if (target is self.guild and panel) else None,
            review_channel_id=self.review_channel.id if (target is self.guild and review_channel) else None,
            appeals_channel_id=self.appeals_channel.id if (target is self.guild and appeals_channel) else None,
            review_mode=review_mode,
            block_adult_language=adult_block,
            allow_images=allow_images,
            image_review_required=image_review_required,
            allow_anonymous_replies=allow_replies,
            anonymous_reply_review_required=anonymous_reply_review_required,
            allow_self_edit=allow_self_edit,
            link_policy_mode=link_mode,
        )
        self.assertTrue(ok, message)

    async def test_disabled_and_missing_channel_config_block_submission(self):
        result = await self.service.submit_confession(self.guild, author_id=101, content="hello", attachments=[])
        self.assertFalse(result.ok)
        self.assertEqual(result.state, "unavailable")
        self.assertIn("off", result.message.lower())

        ok, message = await self.service.configure_guild(self.guild.id, enabled=True)
        self.assertTrue(ok, message)
        result = await self.service.submit_confession(self.guild, author_id=101, content="hello", attachments=[])
        self.assertFalse(result.ok)
        self.assertIn("confession channel", result.message.lower())

    async def test_safe_confession_publishes_premium_embed_and_prunes_body(self):
        await self._configure()

        result = await self.service.submit_confession(self.guild, author_id=123456789, content="hello world", attachments=[])

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")
        self.assertEqual(len(self.confession_channel.sent), 1)
        rendered = json.dumps([embed.to_dict() for embed in self.confession_channel.sent[0].embeds])
        self.assertNotIn("123456789", rendered)
        self.assertIn(result.confession_id, rendered)
        self.assertIsNotNone(result.jump_url)
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        author_link = await self.service.store.fetch_author_link(submission["submission_id"])
        raw_submission = self.service.store._store.submissions[submission["submission_id"]]
        raw_author_link = self.service.store._store.secure_author_links[submission["submission_id"]]
        self.assertEqual(submission["status"], "published")
        self.assertIsNone(submission["content_body"])
        self.assertIsNone(submission["staff_preview"])
        self.assertIsNone(submission["shared_link_url"])
        self.assertIsNotNone(submission["content_fingerprint"])
        self.assertIsNone(submission["similarity_key"])
        self.assertIsNotNone(submission["fuzzy_signature"])
        self.assertEqual(submission["attachment_meta"], [])
        self.assertEqual(author_link["author_user_id"], 123456789)
        self.assertIsNone(raw_submission["content_body"])
        self.assertIsNone(raw_submission["staff_preview"])
        self.assertIsNone(raw_submission["shared_link_url"])
        self.assertIsNone(raw_submission["content_ciphertext"])
        self.assertTrue(str(raw_submission["content_fingerprint"]).startswith("h2:ephemeral:"))
        self.assertNotIn("author_user_id", raw_author_link)
        self.assertTrue(str(raw_author_link["author_lookup_hash"]).startswith("bi2:ephemeral:"))

    async def test_text_link_and_images_queue_for_review_and_keep_private_media_out_of_staff_storage(self):
        await self._configure(review_channel=True, allow_images=True, image_review_required=True)

        result = await self.service.submit_confession(
            self.guild,
            author_id=125,
            content="use this",
            link="https://www.google.com/search?q=babblebox",
            attachments=[FakeAttachment("one.png"), FakeAttachment("two.png")],
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "queued")
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        private_media = await self.service.store.fetch_private_media(submission["submission_id"])
        raw_submission = self.service.store._store.submissions[submission["submission_id"]]
        raw_private_media = self.service.store._store.private_media[submission["submission_id"]]
        self.assertEqual(submission["shared_link_url"], "https://www.google.com/search?q=babblebox")
        self.assertEqual(len(submission["attachment_meta"]), 2)
        self.assertEqual(set(submission["attachment_meta"][0].keys()), {"kind", "size", "width", "height", "spoiler"})
        self.assertEqual(
            private_media["attachment_urls"],
            [
                "https://cdn.discordapp.com/attachments/1/2/one.png",
                "https://cdn.discordapp.com/attachments/1/2/two.png",
            ],
        )
        self.assertIsNone(raw_submission["content_body"])
        self.assertIsNone(raw_submission["shared_link_url"])
        self.assertTrue(str(raw_submission["content_ciphertext"]).startswith("bbx2:ephemeral:"))
        self.assertEqual(raw_private_media["attachment_urls"], [])
        self.assertTrue(str(raw_private_media["attachment_payload"]).startswith("bbx2:ephemeral:"))

        ok, message = await self.service.handle_case_action(self.guild, case_id=result.case_id, action="approve", version=1)

        self.assertTrue(ok, message)
        self.assertEqual(len(self.confession_channel.sent[0].embeds), 3)
        approved = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        self.assertEqual(approved["status"], "published")
        self.assertEqual(approved["attachment_meta"], [])
        self.assertIsNone(await self.service.store.fetch_private_media(submission["submission_id"]))

    async def test_review_queue_is_shared_and_staff_surfaces_hide_identity(self):
        await self._configure(review_mode=True, review_channel=True)

        first = await self.service.submit_confession(self.guild, author_id=123456789, content="borderless hello", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=987654321, content="another safe note", attachments=[])

        self.assertEqual(first.state, "queued")
        self.assertEqual(second.state, "queued")
        self.assertEqual(len(self.review_channel.sent), 1)
        self.assertGreaterEqual(len(self.review_channel.sent[0].edits), 1)
        current = await self.service.current_review_target(self.guild.id)
        pending = await self.service.list_review_targets(self.guild.id, limit=10)
        embed = self.service.build_review_queue_embed(self.guild, pending, note="refreshed")
        rendered = json.dumps(embed.to_dict())
        self.assertNotIn("123456789", rendered)
        self.assertNotIn("987654321", rendered)
        self.assertNotIn("author_user_id", current)
        self.assertNotIn("user_id", current)
        self.assertNotIn("seconds ago", rendered)
        self.assertTrue(current["case_id"].startswith(f"{CASE_ID_PREFIX}-"))
        self.assertTrue(current["confession_id"].startswith(f"{CONFESSION_ID_PREFIX}-"))

    async def test_review_queue_renders_current_case_media_as_compact_embeds_and_refresh_preserves_them(self):
        await self._configure(review_channel=True, allow_images=True, image_review_required=True)

        queued = await self.service.submit_confession(
            self.guild,
            author_id=1234,
            content="review my images",
            attachments=[FakeAttachment("one.png"), FakeAttachment("two.png"), FakeAttachment("three.png")],
        )

        self.assertEqual(queued.state, "queued")
        self.assertEqual(len(self.review_channel.sent), 1)
        queue_message = self.review_channel.sent[0]
        self.assertEqual(len(queue_message.embeds), 4)
        summary = json.dumps(queue_message.embeds[0].to_dict())
        self.assertIn("Showing 3 image preview(s) below.", summary)
        self.assertEqual(
            [embed.to_dict()["image"]["url"] for embed in queue_message.embeds[1:]],
            [
                "https://cdn.discordapp.com/attachments/1/2/one.png",
                "https://cdn.discordapp.com/attachments/1/2/two.png",
                "https://cdn.discordapp.com/attachments/1/2/three.png",
            ],
        )

        ok, message = await self.service.sync_review_queue(self.guild, note="manual refresh")
        self.assertTrue(ok, message)
        self.assertEqual(len(self.review_channel.sent), 1)
        self.assertGreaterEqual(len(queue_message.edits), 1)
        self.assertEqual(len(queue_message.edits[-1]["embeds"]), 4)

    async def test_adult_toggle_and_educational_context(self):
        await self._configure(review_mode=False, review_channel=True, adult_block=True)

        result = await self.service.submit_confession(
            self.guild,
            author_id=200,
            content="Sex education needs better medical consent lessons.",
            attachments=[],
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

        ok, message = await self.service.configure_guild(self.guild.id, block_adult_language=False)
        self.assertTrue(ok, message)
        result = await self.service.submit_confession(self.guild, author_id=201, content="sexual health matters", attachments=[])
        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

    async def test_link_policy_allows_safe_families_and_blocks_unknown_or_promotional_domains(self):
        await self._configure()

        safe = await self.service.submit_confession(
            self.guild,
            author_id=300,
            content="Useful resource",
            link="https://www.google.com/search?q=privacy",
            attachments=[],
        )
        self.assertTrue(safe.ok)
        self.assertEqual(safe.state, "published")

        social = await self.service.submit_confession(
            self.guild,
            author_id=303,
            content="Useful social link",
            link="https://www.instagram.com/example",
            attachments=[],
        )
        docs = await self.service.submit_confession(
            self.guild,
            author_id=304,
            content="Useful docs link",
            link="https://docs.github.com/en",
            attachments=[],
        )
        self.assertTrue(social.ok)
        self.assertTrue(docs.ok)
        self.assertEqual(social.state, "published")
        self.assertEqual(docs.state, "published")

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=301,
            content="click this https://totally-unknown-example.click/free",
            attachments=[],
        )
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")

    async def test_link_mode_disabled_only_allows_custom_allowlist_without_bypassing_hard_blocks(self):
        await self._configure(link_mode="disabled")

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=307,
            content="docs link blocked in disabled mode",
            link="https://docs.github.com/en",
            attachments=[],
        )
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")

        ok, message = await self.service.update_domain_policy(self.guild.id, bucket="allow", domain="docs.github.com", enabled=True)
        self.assertTrue(ok, message)
        allowlisted = await self.service.submit_confession(
            self.guild,
            author_id=308,
            content="allowlisted docs link",
            link="https://docs.github.com/en",
            attachments=[],
        )
        self.assertTrue(allowlisted.ok)
        self.assertEqual(allowlisted.state, "published")

        ok, message = await self.service.update_domain_policy(self.guild.id, bucket="allow", domain="linktr.ee", enabled=True)
        self.assertTrue(ok, message)
        still_blocked = await self.service.submit_confession(
            self.guild,
            author_id=309,
            content="hard blocked shortener should stay blocked",
            link="https://linktr.ee/example",
            attachments=[],
        )
        self.assertFalse(still_blocked.ok)
        self.assertEqual(still_blocked.state, "blocked")

    async def test_link_mode_allow_all_safe_allows_non_mainstream_safe_links_but_keeps_shield_blocks(self):
        await self._configure(link_mode="allow_all_safe")

        safe_unknown = await self.service.submit_confession(
            self.guild,
            author_id=310,
            content="safe personal site",
            link="https://example.com/about",
            attachments=[],
        )
        self.assertTrue(safe_unknown.ok)
        self.assertEqual(safe_unknown.state, "published")

        shortener = await self.service.submit_confession(
            self.guild,
            author_id=311,
            content="shortener still blocked",
            link="https://linktr.ee/example",
            attachments=[],
        )
        storefront = await self.service.submit_confession(
            self.guild,
            author_id=312,
            content="storefront still blocked",
            link="https://patreon.com/example",
            attachments=[],
        )
        self.assertFalse(shortener.ok)
        self.assertFalse(storefront.ok)
        self.assertEqual(shortener.state, "blocked")
        self.assertEqual(storefront.state, "blocked")

        ok, message = await self.service.update_domain_policy(self.guild.id, bucket="block", domain="example.com", enabled=True)
        self.assertTrue(ok, message)
        custom_blocked = await self.service.submit_confession(
            self.guild,
            author_id=313,
            content="custom blocklist still wins",
            link="https://example.com/contact",
            attachments=[],
        )
        self.assertFalse(custom_blocked.ok)
        self.assertEqual(custom_blocked.state, "blocked")

        promo = await self.service.submit_confession(
            self.guild,
            author_id=305,
            content="link in bio",
            link="https://linktr.ee/example",
            attachments=[],
        )
        storefront = await self.service.submit_confession(
            self.guild,
            author_id=306,
            content="shop link",
            link="https://patreon.com/example",
            attachments=[],
        )
        self.assertFalse(promo.ok)
        self.assertFalse(storefront.ok)
        self.assertEqual(promo.state, "blocked")
        self.assertEqual(storefront.state, "blocked")

        ok, message = await self.service.update_domain_policy(self.guild.id, bucket="block", domain="example.com", enabled=True)
        self.assertTrue(ok, message)
        blocked = await self.service.submit_confession(
            self.guild,
            author_id=302,
            content="forbidden",
            link="https://example.com/path",
            attachments=[],
        )
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")

    async def test_mention_abuse_image_limits_and_member_safe_block_copy(self):
        await self._configure()

        mention = await self.service.submit_confession(self.guild, author_id=400, content="hello <@123456789>", attachments=[])
        self.assertFalse(mention.ok)
        self.assertEqual(mention.state, "blocked")
        member_embed = self.service.build_member_result_embed(mention)
        rendered = json.dumps(member_embed.to_dict())
        self.assertNotIn("CS-", rendered)
        self.assertNotIn("author_user_id", rendered)

        attachments = [FakeAttachment(f"image-{index}.png") for index in range(4)]
        too_many = await self.service.submit_confession(self.guild, author_id=401, content="images", attachments=attachments)
        self.assertFalse(too_many.ok)
        self.assertEqual(too_many.state, "blocked")

        svg = await self.service.submit_confession(
            self.guild,
            author_id=402,
            content="svg attempt",
            attachments=[FakeAttachment("vector.txt", content_type="image/svg+xml")],
        )
        self.assertFalse(svg.ok)
        self.assertEqual(svg.state, "blocked")

    async def test_images_require_review_channel_and_trusted_discord_attachment_urls(self):
        await self._configure()

        ok, message = await self.service.configure_guild(self.guild.id, allow_images=True)
        self.assertTrue(ok, message)
        self.assertFalse(self.service.get_config(self.guild.id)["image_review_required"])

        ok, message = await self.service.configure_guild(self.guild.id, allow_images=True, image_review_required=True)
        self.assertFalse(ok)
        self.assertIn("review channel", message.lower())

        await self.service.configure_guild(self.guild.id, review_channel_id=self.review_channel.id)
        ok, message = await self.service.configure_guild(
            self.guild.id,
            allow_images=True,
            image_review_required=True,
            max_images=2,
        )
        self.assertTrue(ok, message)

        external = await self.service.submit_confession(
            self.guild,
            author_id=451,
            content="external image",
            attachments=[FakeAttachment("image.png", url="https://evil.example/image.png")],
        )
        self.assertFalse(external.ok)
        self.assertEqual(external.state, "blocked")
        self.assertIn("safely accept", external.message.lower())

    async def test_strike_escalation_clear_action_and_guild_scoping(self):
        await self._configure()
        await self.service.configure_guild(self.other_guild.id, enabled=True, confession_channel_id=self.other_confession_channel.id, review_mode=False)

        first = await self.service.submit_confession(self.guild, author_id=555555555, content="nigger", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=555555555, content="nigger again", attachments=[])
        self.assertFalse(first.ok)
        self.assertFalse(second.ok)

        state = await self.service.store.fetch_enforcement_state(self.guild.id, 555555555)
        self.assertEqual(state["strike_count"], 2)
        self.assertEqual(state["active_restriction"], "suspended")

        blocked = await self.service.submit_confession(self.guild, author_id=555555555, content="safe text", attachments=[])
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "restricted")

        ok, message = await self.service.handle_staff_action(
            self.guild,
            target_id=first.confession_id,
            action="clear",
            clear_strikes=False,
        )
        self.assertTrue(ok, message)
        cleared = await self.service.store.fetch_enforcement_state(self.guild.id, 555555555)
        self.assertEqual(cleared["strike_count"], 2)
        self.assertEqual(cleared["active_restriction"], "none")

        third = await self.service.submit_confession(self.guild, author_id=555555555, content="nigger third", attachments=[])
        self.assertFalse(third.ok)
        escalated = await self.service.store.fetch_enforcement_state(self.guild.id, 555555555)
        self.assertEqual(escalated["strike_count"], 3)
        self.assertEqual(escalated["active_restriction"], "temp_ban")

        other = await self.service.submit_confession(self.other_guild, author_id=555555555, content="safe in other guild", attachments=[])
        self.assertTrue(other.ok)
        self.assertEqual(other.state, "published")

    async def test_admin_exemption_skips_automatic_strikes_but_still_blocks_content(self):
        await self._configure()
        admin = self._member(556, manage_guild=True)

        first = await self.service.submit_confession(self.guild, author_id=admin.id, member=admin, content="nigger", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=admin.id, member=admin, content="nigger again", attachments=[])

        self.assertFalse(first.ok)
        self.assertFalse(second.ok)
        self.assertEqual(first.state, "blocked")
        self.assertEqual(second.state, "blocked")
        state = await self.service.store.fetch_enforcement_state(self.guild.id, admin.id)
        self.assertTrue(state is None or state["strike_count"] == 0)
        if state is not None:
            self.assertEqual(state["active_restriction"], "none")

    async def test_exempt_role_skips_automatic_burst_suspend(self):
        await self._configure()
        ok, message = await self.service.configure_guild(
            self.guild.id,
            burst_limit=1,
            cooldown_seconds=15,
        )
        self.assertTrue(ok, message)
        ok, message = await self.service.update_automatic_moderation_role_exemption(
            self.guild.id,
            role_id=self.allowed_role.id,
            enabled=True,
        )
        self.assertTrue(ok, message)
        member = self._member(557, roles=[self.allowed_role])

        first = await self.service.submit_confession(self.guild, author_id=member.id, member=member, content="first", attachments=[])
        self.assertTrue(first.ok)
        state = await self.service.store.fetch_enforcement_state(self.guild.id, member.id)
        state["cooldown_until"] = None
        await self.service.store.upsert_enforcement_state(state)

        second = await self.service.submit_confession(self.guild, author_id=member.id, member=member, content="second", attachments=[])

        self.assertTrue(second.ok)
        refreshed = await self.service.store.fetch_enforcement_state(self.guild.id, member.id)
        self.assertEqual(refreshed["active_restriction"], "none")
        self.assertEqual(refreshed["strike_count"], 0)

    async def test_preflight_submission_access_blocks_restricted_member_before_modal(self):
        await self._configure()
        member = self._member(558)
        await self.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "active_restriction": "suspended",
                "restricted_until": "2999-01-01T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 1,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": "CS-LOCKED",
                "image_restriction_active": False,
                "image_restricted_until": None,
                "image_restriction_case_id": None,
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )

        gate = await self.service.preflight_submission_access(
            self.guild,
            author_id=member.id,
            member=member,
            submission_kind="confession",
        )

        self.assertFalse(gate.ok)
        self.assertEqual(gate.title, "Confessions Paused")
        self.assertEqual(gate.result.state, "restricted")
        self.assertIn("temporarily pausing", gate.result.message)

    async def test_preflight_image_restriction_allows_text_only_but_blocks_uploads(self):
        await self._configure(review_channel=True, allow_images=True)
        member = self._member(559)
        await self.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "active_restriction": "none",
                "restricted_until": None,
                "is_permanent_ban": False,
                "strike_count": 0,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": None,
                "image_restriction_active": True,
                "image_restricted_until": "2999-01-01T00:00:00+00:00",
                "image_restriction_case_id": "CS-IMAGES",
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )

        gate = await self.service.preflight_submission_access(
            self.guild,
            author_id=member.id,
            member=member,
            submission_kind="confession",
            image_restriction_mode="advisory",
        )
        self.assertTrue(gate.ok)
        self.assertIn("image attachments are paused", gate.image_restriction_message.lower())

        text_only = await self.service.submit_confession(self.guild, author_id=member.id, member=member, content="text only", attachments=[])
        blocked = await self.service.submit_confession(
            self.guild,
            author_id=member.id,
            member=member,
            content="with image",
            attachments=[FakeAttachment("proof.png")],
        )

        self.assertTrue(text_only.ok)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("image attachments are paused", blocked.message.lower())

    async def test_review_approval_stale_version_and_raw_delete_reconciliation(self):
        await self._configure(review_mode=True, review_channel=True)
        result = await self.service.submit_confession(self.guild, author_id=700, content="needs approval", attachments=[])
        self.assertEqual(result.state, "queued")

        ok, message = await self.service.handle_case_action(self.guild, case_id=result.case_id, action="approve", version=1)
        self.assertTrue(ok, message)
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        self.assertEqual(submission["status"], "published")
        self.assertIsNone(submission["content_body"])
        self.assertIsNone(submission["staff_preview"])

        stale_ok, stale_message = await self.service.handle_case_action(self.guild, case_id=result.case_id, action="deny", version=1)
        self.assertFalse(stale_ok)
        self.assertIn("closed", stale_message.lower())

        await self.service.handle_raw_message_delete(FakeRawDeletePayload(guild_id=self.guild.id, message_id=submission["posted_message_id"]))
        deleted = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        self.assertEqual(deleted["status"], "deleted")

    async def test_manual_moderation_by_confession_id_creates_anonymous_case(self):
        await self._configure()
        result = await self.service.submit_confession(self.guild, author_id=808, content="published note", attachments=[])
        self.assertEqual(result.state, "published")

        ok, message = await self.service.handle_staff_action(self.guild, target_id=result.confession_id, action="clear")

        self.assertTrue(ok, message)
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, result.confession_id)
        case = await self.service.store.fetch_case(self.guild.id, submission["current_case_id"])
        self.assertEqual(case["case_kind"], "published_moderation")
        self.assertEqual(case["resolution_action"], "clear")
        detail = await self.service.build_target_status_embed(self.guild, result.confession_id)
        rendered = json.dumps(detail.to_dict())
        self.assertNotIn("808", rendered)
        self.assertIn("Manual staff action", rendered)

    async def test_multi_link_and_attachment_leak_surfaces_are_blocked_or_sanitized(self):
        await self._configure(review_mode=True, review_channel=True)

        multi = await self.service.submit_confession(
            self.guild,
            author_id=900,
            content="see https://docs.github.com and https://wikipedia.org/wiki/Privacy",
            attachments=[],
        )
        self.assertFalse(multi.ok)
        self.assertEqual(multi.state, "blocked")

        queued = await self.service.submit_confession(
            self.guild,
            author_id=901,
            content="image with note",
            attachments=[FakeAttachment("secret-name.png")],
        )
        await self.service._sync_review_queue(self.guild)
        detail = await self.service.build_target_status_embed(self.guild, queued.confession_id)
        queue = self.service.build_review_queue_embed(self.guild, await self.service.list_review_targets(self.guild.id, limit=10))
        rendered_detail = json.dumps(detail.to_dict())
        rendered_queue = json.dumps(queue.to_dict())
        self.assertNotIn("secret-name.png", rendered_detail)
        self.assertNotIn("secret-name.png", rendered_queue)
        self.assertNotIn("cdn.discordapp.com", rendered_detail)
        self.assertNotIn("cdn.discordapp.com", rendered_queue)

    async def test_false_positive_on_review_case_publishes_and_clear_is_rejected(self):
        await self._configure(review_mode=True, review_channel=True)

        queued = await self.service.submit_confession(self.guild, author_id=904, content="review me", attachments=[])
        self.assertEqual(queued.state, "queued")

        cleared_ok, cleared_message = await self.service.handle_staff_action(
            self.guild,
            target_id=queued.case_id,
            action="clear",
        )
        self.assertFalse(cleared_ok)
        self.assertIn("approve", cleared_message.lower())

        ok, message = await self.service.handle_staff_action(
            self.guild,
            target_id=queued.case_id,
            action="false_positive",
        )

        self.assertTrue(ok, message)
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, queued.confession_id)
        case = await self.service.store.fetch_case(self.guild.id, queued.case_id)
        self.assertEqual(submission["status"], "published")
        self.assertEqual(submission["review_status"], "overridden")
        self.assertEqual(case["resolution_action"], "false_positive")

    async def test_obfuscated_slur_is_blocked_but_reporting_context_can_still_queue(self):
        await self._configure(review_mode=True, review_channel=True)

        blocked = await self.service.submit_confession(self.guild, author_id=905, content="nіggеr", attachments=[])
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")

        quoted = await self.service.submit_confession(
            self.guild,
            author_id=906,
            content='For review: someone said "nіggеr" in chat.',
            attachments=[],
        )
        self.assertTrue(quoted.ok)
        self.assertEqual(quoted.state, "queued")

    async def test_blocked_false_positive_reuses_original_case_and_publishes_without_synthetic_case(self):
        await self._configure(review_mode=False, review_channel=True)

        blocked = await self.service.submit_confession(self.guild, author_id=902, content="nigger", attachments=[])
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")

        ok, message = await self.service.handle_staff_action(self.guild, target_id=blocked.confession_id, action="false_positive")

        self.assertTrue(ok, message)
        submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, blocked.confession_id)
        case = await self.service.store.fetch_case(self.guild.id, blocked.case_id)
        self.assertEqual(submission["status"], "published")
        self.assertEqual(submission["current_case_id"], blocked.case_id)
        self.assertEqual(case["case_kind"], "safety_block")
        self.assertEqual(case["status"], "resolved")
        self.assertEqual(case["resolution_action"], "false_positive")

    async def test_blocked_spam_attempts_consume_cooldown_state(self):
        ok, message = await self.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=self.confession_channel.id,
            review_mode=False,
            cooldown_seconds=15,
        )
        self.assertTrue(ok, message)

        first = await self.service.submit_confession(self.guild, author_id=903, content="", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=903, content="a", attachments=[])

        self.assertFalse(first.ok)
        self.assertEqual(first.state, "blocked")
        self.assertFalse(second.ok)
        self.assertEqual(second.state, "restricted")
        state = await self.service.store.fetch_enforcement_state(self.guild.id, 903)
        self.assertIsNotNone(state)
        self.assertIsNotNone(state["cooldown_until"])

    async def test_dashboard_policy_surfaces_automatic_moderation_exemptions(self):
        await self._configure()
        ok, message = await self.service.update_automatic_moderation_role_exemption(
            self.guild.id,
            role_id=self.allowed_role.id,
            enabled=True,
        )
        self.assertTrue(ok, message)

        embed = await self.service.build_dashboard_embed(self.guild, section="policy")
        rendered = json.dumps(embed.to_dict())

        self.assertIn("Automatic Moderation", rendered)
        self.assertIn("Admins exempt by default", rendered)
        self.assertIn("Hard content blocking", rendered)

    async def test_member_panel_sync_keeps_one_message_and_disables_when_unavailable(self):
        await self._configure(panel=True)

        ok, message = await self.service.sync_member_panel(self.guild)
        self.assertTrue(ok, message)
        again_ok, again_message = await self.service.sync_member_panel(self.guild)
        self.assertTrue(again_ok, again_message)
        self.assertEqual(len(self.panel_channel.sent), 1)
        self.assertEqual(len(self.bot.views), 2)
        self.assertFalse(self.panel_channel.sent[0].view.send_button.disabled)

        await self.service.configure_guild(self.guild.id, enabled=False)
        disabled_ok, disabled_message = await self.service.sync_member_panel(self.guild)
        self.assertTrue(disabled_ok, disabled_message)
        self.assertTrue(self.panel_channel.sent[0].view.send_button.disabled)

    async def test_member_panel_sync_reports_partial_success_when_record_update_fails(self):
        await self._configure(panel=True)

        with mock.patch.object(
            self.service,
            "persist_panel_record",
            new=mock.AsyncMock(return_value=(False, "Babblebox could not save the updated panel location.")),
        ):
            ok, message = await self.service.sync_member_panel(self.guild)

        self.assertTrue(ok)
        self.assertIn("live in", message)
        self.assertIn("could not save the updated panel location", message)
        self.assertEqual(len(self.panel_channel.sent), 1)
        self.assertIsNone(self.service.get_config(self.guild.id)["panel_message_id"])

    async def test_review_queue_sync_reports_partial_success_when_record_update_fails(self):
        await self._configure(review_mode=True, review_channel=True)
        queued = await self.service.submit_confession(self.guild, author_id=904, content="queue me", attachments=[])
        self.assertEqual(queued.state, "queued")

        with mock.patch.object(self.service.store, "upsert_review_queue", side_effect=RuntimeError("queue boom")):
            ok, message = await self.service.sync_review_queue(self.guild, note="Confession review queue refreshed.")

        self.assertTrue(ok)
        self.assertIn("Confession review queue is live", message)
        self.assertIn("could not save the refreshed review queue state", message)
        self.assertEqual(len(self.review_channel.sent), 1)

    async def test_runtime_surface_sync_refreshes_panel_views_and_review_queue(self):
        await self._configure(panel=True, review_channel=True, review_mode=False, allow_replies=True)
        published = await self.service.submit_confession(self.guild, author_id=905, content="published", attachments=[])
        self.assertEqual(published.state, "published")
        ok, message = await self.service.configure_guild(self.guild.id, review_mode=True)
        self.assertTrue(ok, message)
        queued = await self.service.submit_confession(self.guild, author_id=906, content="queue me", attachments=[])
        self.assertEqual(queued.state, "queued")
        self.bot.views.clear()

        result = await self.service.sync_runtime_surfaces(self.guild, stage_prefix="test_runtime")

        self.assertTrue(result.ok)
        self.assertEqual(result.issues, ())
        self.assertEqual(len(self.panel_channel.sent), 1)
        self.assertEqual(len(self.review_channel.sent), 1)
        restored_ids = {message_id for _, message_id in self.bot.views}
        self.assertEqual(
            restored_ids,
            {
                self.panel_channel.sent[0].id,
                self.confession_channel.sent[0].id,
                self.review_channel.sent[0].id,
            },
        )

    async def test_configure_guild_stays_successful_when_privacy_status_lookup_fails(self):
        with mock.patch.object(self.service.store, "fetch_privacy_status", side_effect=RuntimeError("privacy boom")):
            ok, message = await self.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20)

        self.assertTrue(ok)
        self.assertIn("enabled", message)
        self.assertIn("Privacy hardening status is unavailable right now.", message)

    async def test_dashboard_embed_falls_back_when_counts_and_privacy_status_fail(self):
        await self._configure()

        with (
            mock.patch.object(self.service.store, "fetch_guild_counts", side_effect=RuntimeError("counts boom")),
            mock.patch.object(self.service.store, "fetch_privacy_status", side_effect=RuntimeError("privacy boom")),
        ):
            embed = await self.service.build_dashboard_embed(self.guild, section="overview")

        rendered = json.dumps(embed.to_dict())
        self.assertEqual(embed.title, "Confessions Control Panel")
        self.assertIn("State: **Unknown**", rendered)
        self.assertIn("Queued: **0**", rendered)

    async def test_dashboard_counts_distinguish_review_queue_from_other_cases(self):
        await self._configure(review_mode=True, review_channel=True)
        queued = await self.service.submit_confession(self.guild, author_id=907, content="queue me", attachments=[])
        blocked = await self.service.submit_confession(self.guild, author_id=908, content="nigger", attachments=[])
        self.assertEqual(queued.state, "queued")
        self.assertEqual(blocked.state, "blocked")

        review_embed = await self.service.build_dashboard_embed(self.guild, section="review")
        rendered = json.dumps(review_embed.to_dict())
        self.assertIn("Open queue", rendered)
        self.assertIn("**1** case", rendered)
        self.assertIn("Open safety blocks", rendered)

    async def test_replies_are_disabled_by_default_and_queue_when_enabled(self):
        await self._configure(review_channel=True)
        published = await self.service.submit_confession(self.guild, author_id=910, content="base confession", attachments=[])
        self.assertEqual(published.state, "published")

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=911,
            content="reply text",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("off by default", blocked.message.lower())

        ok, message = await self.service.configure_guild(
            self.guild.id,
            allow_anonymous_replies=True,
            anonymous_reply_review_required=True,
        )
        self.assertTrue(ok, message)
        reply = await self.service.submit_confession(
            self.guild,
            author_id=911,
            content="reply text",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertTrue(reply.ok)
        self.assertEqual(reply.state, "queued")
        self.assertEqual(reply.submission_kind, "reply")
        self.assertEqual(reply.parent_confession_id, published.confession_id)
        self.assertIn("stays anonymous", reply.message)
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, reply.confession_id)
        self.assertEqual(stored["submission_kind"], "reply")
        self.assertEqual(stored["reply_flow"], "reply_to_confession")
        self.assertEqual(stored["parent_confession_id"], published.confession_id)
        self.assertEqual(stored["reply_target_label"], published.confession_id)
        self.assertEqual(stored["reply_target_preview"], "base confession")

    async def test_direct_member_reply_creates_owner_reply_opportunity_and_dm_prompt(self):
        await self._configure(review_channel=True)
        owner = self._member(922)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="lonely tonight", attachments=[])
        self.assertEqual(published.state, "published")

        responder = self._member(923)
        response = self._reply_message(
            author=responder,
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="We're here for you",
        )

        await self.service.handle_member_response_message(response)

        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["root_confession_id"], published.confession_id)
        self.assertEqual(pending[0]["source_author_name"], responder.display_name)
        self.assertEqual(pending[0]["notification_status"], "sent")
        self.assertEqual(len(owner.sent), 1)
        self.assertEqual(owner.sent[0].embed.title, "Someone responded to your confession")
        self.assertEqual(
            [child.custom_id for child in owner.sent[0].view.children if getattr(child, "custom_id", None)],
            ["bb-confession-owner-reply:open", "bb-confession-owner-reply:dismiss"],
        )

    async def test_published_anonymous_reply_embed_includes_parent_preview_context(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=True)
        published = await self.service.submit_confession(self.guild, author_id=9230, content="base confession for preview", attachments=[])
        self.assertEqual(published.state, "published")

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9231,
            content="reply text",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertEqual(reply.state, "queued")

        ok, message = await self.service.handle_case_action(self.guild, case_id=reply.case_id, action="approve", version=1)
        self.assertTrue(ok, message)

        thread = list(self.guild.threads.values())[0]
        self.assertEqual(thread.parent.id, self.confession_channel.id)
        fields = _embed_fields_by_name(thread.sent[0].embeds[0])
        self.assertIn("Replying To", fields)
        self.assertFalse(fields["Replying To"]["inline"])
        self.assertIn(f"Confession `{published.confession_id}`", str(fields["Replying To"]["value"]))
        self.assertIn("Preview: base confession for preview", str(fields["Replying To"]["value"]))

    async def test_responses_to_public_anonymous_replies_do_not_create_owner_opportunities(self):
        await self._configure(
            review_channel=True,
            allow_replies=True,
            anonymous_reply_review_required=True,
        )
        owner = self._member(924)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="base confession", attachments=[])
        anonymous_reply = await self.service.submit_confession(
            self.guild,
            author_id=925,
            content="anonymous support",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertEqual(anonymous_reply.state, "queued")
        ok, message = await self.service.handle_case_action(self.guild, case_id=anonymous_reply.case_id, action="approve", version=1)
        self.assertTrue(ok, message)
        thread = list(self.guild.threads.values())[0]

        member_response = self._reply_message(
            author=self._member(926),
            channel=thread,
            reply_to_message_id=thread.sent[0].id,
            content="responding to the anonymous reply",
        )
        await self.service.handle_member_response_message(member_response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(pending, [])

    async def test_owner_reply_publishes_immediately_by_default_without_review_channel(self):
        await self._configure()
        owner = self._member(925)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="public owner reply", attachments=[])
        response = self._reply_message(
            author=self._member(926),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="please reply publicly",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending), 1)

        owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending[0]["opportunity_id"],
            content="Thanks for responding.",
        )

        self.assertTrue(owner_reply.ok)
        self.assertEqual(owner_reply.state, "published")
        self.assertEqual(owner_reply.reply_flow, "owner_reply_to_user")
        self.assertEqual(len(self.confession_channel.sent), 2)
        stored_owner_reply = await self.service.store.fetch_submission_by_confession_id(self.guild.id, owner_reply.confession_id)
        self.assertEqual(stored_owner_reply["reply_flow"], "owner_reply_to_user")
        self.assertEqual(stored_owner_reply["owner_reply_generation"], 1)
        fields = _embed_fields_by_name(self.confession_channel.sent[1].embeds[0])
        self.assertIn("Replying To", fields)
        self.assertFalse(fields["Replying To"]["inline"])
        self.assertIn("**User 926**", str(fields["Replying To"]["value"]))
        self.assertIn("Preview: please reply publicly", str(fields["Replying To"]["value"]))
        self.assertEqual(fields["Confession"]["value"], f"`{published.confession_id}`")
        self.assertEqual(fields["Flow"]["value"], "Owner reply")
        rendered = json.dumps([embed.to_dict() for embed in self.confession_channel.sent[1].embeds])
        self.assertIn("Anonymous Owner Reply", rendered)
        self.assertIn(published.confession_id, rendered)
        self.assertIn("User 926", rendered)
        self.assertNotIn("User 925", rendered)
        used = await self.service.store.fetch_owner_reply_opportunity(pending[0]["opportunity_id"])
        self.assertEqual(used["status"], "used")

    async def test_owner_reply_queues_only_when_owner_reply_review_is_enabled(self):
        await self._configure(review_channel=True)
        ok, message = await self.service.configure_guild(self.guild.id, owner_reply_review_mode=True)
        self.assertTrue(ok, message)
        owner = self._member(927)
        await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="queued owner reply", attachments=[])
        response = self._reply_message(
            author=self._member(928),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="owner reply should queue",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)

        owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending[0]["opportunity_id"],
            content="Thanks for checking in. I appreciate it.",
        )

        self.assertTrue(owner_reply.ok)
        self.assertEqual(owner_reply.state, "queued")
        self.assertIsNotNone(owner_reply.case_id)
        stored_owner_reply = await self.service.store.fetch_submission_by_confession_id(self.guild.id, owner_reply.confession_id)
        self.assertEqual(stored_owner_reply["review_status"], "pending")
        queue = self.service.build_review_queue_embed(self.guild, await self.service.list_review_targets(self.guild.id, limit=10))
        self.assertIn("Owner Reply", json.dumps(queue.to_dict()))

    async def test_approved_queued_owner_reply_uses_stored_responder_snapshot_in_public_embed(self):
        await self._configure(review_channel=True)
        ok, message = await self.service.configure_guild(self.guild.id, owner_reply_review_mode=True)
        self.assertTrue(ok, message)
        owner = self._member(9270)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="queued owner reply snapshot", attachments=[])
        self.assertEqual(published.state, "published")

        response = self._reply_message(
            author=self._member(9271),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="stored responder snapshot",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)

        owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending[0]["opportunity_id"],
            content="Thanks. Queue this first.",
        )
        self.assertEqual(owner_reply.state, "queued")

        stored_owner_reply = await self.service.store.fetch_submission_by_confession_id(self.guild.id, owner_reply.confession_id)
        self.assertEqual(stored_owner_reply["reply_target_label"], "User 9271")
        self.assertEqual(stored_owner_reply["reply_target_preview"], "stored responder snapshot")

        ok, message = await self.service.handle_case_action(self.guild, case_id=owner_reply.case_id, action="approve", version=1)
        self.assertTrue(ok, message)

        fields = _embed_fields_by_name(self.confession_channel.sent[1].embeds[0])
        self.assertIn("Replying To", fields)
        self.assertIn("**User 9271**", str(fields["Replying To"]["value"]))
        self.assertIn("Preview: stored responder snapshot", str(fields["Replying To"]["value"]))
        self.assertEqual(fields["Confession"]["value"], f"`{published.confession_id}`")

    async def test_public_reply_embed_falls_back_to_id_only_when_target_preview_is_missing(self):
        embeds = await self.service._build_public_confession_embeds(
            {
                "submission_id": "sub-fallback",
                "guild_id": self.guild.id,
                "confession_id": "CF-REPLY999",
                "submission_kind": "reply",
                "reply_flow": "owner_reply_to_user",
                "owner_reply_generation": 1,
                "parent_confession_id": "CF-ROOT999",
                "reply_target_label": "Responder",
                "reply_target_preview": None,
                "content_body": "fallback body",
                "shared_link_url": None,
                "attachment_meta": [],
            }
        )
        fields = _embed_fields_by_name(embeds[0])
        self.assertEqual(fields["Replying To"]["value"], "`CF-ROOT999`")
        self.assertTrue(fields["Replying To"]["inline"])
        self.assertNotIn("Confession", fields)

    async def test_owner_reply_to_first_owner_reply_creates_second_round_but_stops_at_generation_two(self):
        await self._configure()
        owner = self._member(929)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="bounded thread", attachments=[])

        first_response = self._reply_message(
            author=self._member(930),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="first public response",
        )
        await self.service.handle_member_response_message(first_response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        first_owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending[0]["opportunity_id"],
            content="first owner reply",
        )
        self.assertEqual(first_owner_reply.state, "published")

        second_response = self._reply_message(
            author=self._member(931),
            reply_to_message_id=self.confession_channel.sent[1].id,
            content="replying to the owner reply",
        )
        await self.service.handle_member_response_message(second_response)
        pending_round_two = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending_round_two), 1)
        second_owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending_round_two[0]["opportunity_id"],
            content="second owner reply",
        )
        self.assertEqual(second_owner_reply.state, "published")
        stored_owner_reply = await self.service.store.fetch_submission_by_confession_id(self.guild.id, second_owner_reply.confession_id)
        self.assertEqual(stored_owner_reply["owner_reply_generation"], 2)

        no_retrigger = self._reply_message(
            author=self._member(932),
            reply_to_message_id=self.confession_channel.sent[2].id,
            content="this should stop here",
        )
        await self.service.handle_member_response_message(no_retrigger)
        pending_after = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(pending_after, [])

    async def test_owner_reply_prompts_dedupe_source_and_respect_dm_cooldown(self):
        await self._configure(review_channel=True)
        owner = self._member(933)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="cooldown test", attachments=[])
        self.assertEqual(published.state, "published")

        first = self._reply_message(
            author=self._member(934),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="first response",
        )
        await self.service.handle_member_response_message(first)
        await self.service.handle_member_response_message(first)

        second = self._reply_message(
            author=self._member(935),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="second response",
        )
        await self.service.handle_member_response_message(second)

        opportunities = await self.service.store.list_owner_reply_opportunities_for_root_submission(
            (await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id))["submission_id"],
            limit=10,
        )
        self.assertEqual(len(opportunities), 2)
        self.assertEqual(sorted(row["notification_status"] for row in opportunities), ["cooldown", "sent"])
        self.assertEqual(len(owner.sent), 1)

    async def test_owner_reply_prompt_dm_failure_keeps_pending_fallback_context(self):
        await self._configure(review_channel=True)
        owner = self._member(936)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="fallback owner reply", attachments=[])
        response = self._reply_message(
            author=self._member(937),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="dm fallback please",
        )
        with mock.patch.object(self.service, "_send_owner_reply_notification", new=mock.AsyncMock(return_value=(False, None, None))):
            await self.service.handle_member_response_message(response)

        self.assertEqual(published.state, "published")
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["notification_status"], "failed")
        contexts = await self.service.list_pending_owner_reply_contexts(self.guild, author_id=owner.id, limit=5)
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["opportunity"]["source_preview"], "dm fallback please")

    async def test_owner_reply_opportunity_expires_when_source_message_is_deleted(self):
        await self._configure(review_channel=True)
        owner = self._member(938)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="source delete", attachments=[])
        response = self._reply_message(
            author=self._member(939),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="temporary response",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending), 1)

        await self.service.handle_raw_message_delete(FakeRawDeletePayload(guild_id=self.guild.id, message_id=response.id))

        expired = await self.service.store.fetch_owner_reply_opportunity(pending[0]["opportunity_id"])
        self.assertEqual(expired["status"], "expired")

    async def test_owner_reply_opportunity_expires_when_source_message_is_edited(self):
        await self._configure(review_channel=True)
        owner = self._member(940)
        await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="source edit", attachments=[])
        response = self._reply_message(
            author=self._member(941),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="initial response",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        self.assertEqual(len(pending), 1)

        response.content = "edited response"
        await self.service.handle_message_edit(response)

        expired = await self.service.store.fetch_owner_reply_opportunity(pending[0]["opportunity_id"])
        self.assertEqual(expired["status"], "expired")

    async def test_owner_reply_submit_rejects_arbitrary_owner_reply_flow_without_opportunity(self):
        await self._configure()
        owner = self._member(942)
        published = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="arbitrary owner reply", attachments=[])

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="fake owner reply",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
            reply_flow="owner_reply_to_user",
        )

        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("owner-reply opportunity", blocked.message.lower())

    async def test_role_allowlist_blocks_members_without_allowed_roles(self):
        await self._configure()
        ok, message = await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        self.assertTrue(ok, message)
        member = self._member(912)

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=member.id,
            member=member,
            content="not allowed",
            attachments=[],
        )

        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("selected roles", blocked.message.lower())

    async def test_role_allowlist_allows_members_with_allowed_roles(self):
        await self._configure()
        ok, message = await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        self.assertTrue(ok, message)
        member = self._member(913, roles=[self.allowed_role])

        result = await self.service.submit_confession(
            self.guild,
            author_id=member.id,
            member=member,
            content="allowed",
            attachments=[],
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

    async def test_role_blacklist_blocks_members_and_wins_over_allowlist(self):
        await self._configure()
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        await self.service.update_role_policy(self.guild.id, bucket="block", role_id=self.blocked_role.id, enabled=True)
        blocked_member = self._member(914, roles=[self.blocked_role])
        both_member = self._member(915, roles=[self.allowed_role, self.blocked_role])

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=blocked_member.id,
            member=blocked_member,
            content="blacklisted",
            attachments=[],
        )
        self.assertFalse(blocked.ok)
        self.assertIn("role setup", blocked.message.lower())

        conflict = await self.service.submit_confession(
            self.guild,
            author_id=both_member.id,
            member=both_member,
            content="conflict",
            attachments=[],
        )
        self.assertFalse(conflict.ok)
        self.assertIn("role setup", conflict.message.lower())

    async def test_empty_allowlist_means_no_allowlist_restriction(self):
        await self._configure()
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=False)
        member = self._member(916)

        result = await self.service.submit_confession(
            self.guild,
            author_id=member.id,
            member=member,
            content="open again",
            attachments=[],
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

    async def test_role_restrictions_are_guild_scoped(self):
        await self._configure()
        await self._configure(guild=self.other_guild)
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)

        result = await self.service.submit_confession(self.other_guild, author_id=917, content="other guild open", attachments=[])

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

    async def test_role_policy_status_surfaces_include_counts_mentions_and_stale_roles(self):
        await self._configure()
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=99991, enabled=True)
        await self.service.update_role_policy(self.guild.id, bucket="block", role_id=self.blocked_role.id, enabled=True)

        embed = await self.service.build_dashboard_embed(self.guild, section="policy")
        rendered = json.dumps(embed.to_dict())

        self.assertIn("Role Eligibility", rendered)
        self.assertIn(self.allowed_role.mention, rendered)
        self.assertIn(self.blocked_role.mention, rendered)
        self.assertIn("Blacklist wins", rendered)
        self.assertIn("Stale configured roles", rendered)

    async def test_stale_role_allowlist_entries_do_not_lock_submission(self):
        await self._configure()
        ok, message = await self.service.update_role_policy(self.guild.id, bucket="allow", role_id=99992, enabled=True)
        self.assertTrue(ok, message)

        result = await self.service.submit_confession(self.guild, author_id=9180, content="stale allowlist ignored", attachments=[])

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")

    async def test_published_confessions_show_reply_button_when_replies_enabled_and_not_on_nested_replies(self):
        await self._configure(
            review_channel=True,
            allow_replies=True,
            anonymous_reply_review_required=True,
        )
        published = await self.service.submit_confession(self.guild, author_id=918, content="base confession", attachments=[])

        self.assertIsNotNone(self.confession_channel.sent[0].view)
        self.assertEqual(
            _view_custom_ids(self.confession_channel.sent[0].view),
            ["bb-confession-post:compose", "bb-confession-post:reply"],
        )

        reply = await self.service.submit_confession(
            self.guild,
            author_id=919,
            content="reply body",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertEqual(reply.state, "queued")
        ok, message = await self.service.handle_case_action(self.guild, case_id=reply.case_id, action="approve", version=1)
        self.assertTrue(ok, message)
        self.assertEqual(len(self.confession_channel.sent), 1)
        self.assertEqual(len(self.guild.threads), 1)
        thread = list(self.guild.threads.values())[0]
        self.assertEqual(thread.name, f"Replies {published.confession_id}")
        self.assertEqual(_view_custom_ids(thread.sent[0].view), [])

    async def test_top_level_replies_create_and_reuse_one_discussion_thread(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9184, content="thread root", attachments=[])
        self.assertEqual(published.state, "published")

        first_reply = await self.service.submit_confession(
            self.guild,
            author_id=9185,
            content="first threaded reply",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        second_reply = await self.service.submit_confession(
            self.guild,
            author_id=9186,
            content="second threaded reply",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(first_reply.ok)
        self.assertTrue(second_reply.ok)
        self.assertEqual(len(self.guild.threads), 1)
        thread = list(self.guild.threads.values())[0]
        self.assertEqual(len(thread.sent), 2)
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id)
        self.assertEqual(stored["discussion_thread_id"], thread.id)
        self.assertEqual(thread.sent[0].channel.id, thread.id)
        self.assertEqual(thread.sent[1].channel.id, thread.id)

    async def test_reply_to_reply_in_thread_reuses_thread_without_creating_nested_thread(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9187, content="nested fallback root", attachments=[])
        first_reply = await self.service.submit_confession(
            self.guild,
            author_id=9188,
            content="reply one",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        stored_first_reply = await self.service.store.fetch_submission_by_confession_id(self.guild.id, first_reply.confession_id)
        thread = list(self.guild.threads.values())[0]

        second_reply = await self.service.submit_confession(
            self.guild,
            author_id=9189,
            content="reply to the reply",
            submission_kind="reply",
            parent_confession_id=stored_first_reply["confession_id"],
        )

        self.assertTrue(second_reply.ok)
        self.assertEqual(len(self.guild.threads), 1)
        self.assertEqual(len(thread.sent), 2)
        fields = _embed_fields_by_name(thread.sent[1].embeds[0])
        self.assertIn("Replying To", fields)
        self.assertIn(f"Confession `{stored_first_reply['confession_id']}`", str(fields["Replying To"]["value"]))

    async def test_archived_discussion_thread_is_reopened_and_reused(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=91895, content="archived root", attachments=[])
        await self.service.submit_confession(
            self.guild,
            author_id=91896,
            content="seed thread",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        thread = list(self.guild.threads.values())[0]
        thread.archived = True

        follow_up = await self.service.submit_confession(
            self.guild,
            author_id=91897,
            content="reopen archived thread",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(follow_up.ok)
        self.assertFalse(thread.archived)
        self.assertEqual(len(thread.sent), 2)

    async def test_locked_or_unavailable_reply_threads_fall_back_to_channel_posts(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9190, content="thread fallback root", attachments=[])
        first_reply = await self.service.submit_confession(
            self.guild,
            author_id=9191,
            content="thread me first",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        self.assertTrue(first_reply.ok)
        thread = list(self.guild.threads.values())[0]
        thread.locked = True

        fallback_reply = await self.service.submit_confession(
            self.guild,
            author_id=9192,
            content="fallback because thread is locked",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(fallback_reply.ok)
        self.assertEqual(len(thread.sent), 1)
        self.assertEqual(len(self.confession_channel.sent), 2)
        self.assertIn("Anonymous Reply", json.dumps([embed.to_dict() for embed in self.confession_channel.sent[1].embeds]))

    async def test_missing_thread_creation_permission_falls_back_to_channel_reply_post(self):
        self.confession_channel.bot_can_create_public_threads = False
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9193, content="no thread perms", attachments=[])

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9194,
            content="fallback to channel",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(reply.ok)
        self.assertEqual(len(self.guild.threads), 0)
        self.assertEqual(len(self.confession_channel.sent), 2)

    async def test_deleted_discussion_thread_falls_back_cleanly_and_does_not_create_nested_state(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9195, content="deleted thread root", attachments=[])
        await self.service.submit_confession(
            self.guild,
            author_id=9196,
            content="seed deleted thread",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        thread = list(self.guild.threads.values())[0]
        stored_root = await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id)
        stored_root["discussion_thread_id"] = thread.id
        await self.service.store.upsert_submission(stored_root)
        await thread.delete(reason="simulate deleted thread")

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9197,
            content="recover after deleted thread",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(reply.ok)
        self.assertEqual(len(self.confession_channel.sent), 2)

    async def test_images_can_publish_without_forced_review_when_review_requirement_is_off(self):
        await self._configure(review_channel=True, allow_images=True, image_review_required=False)

        result = await self.service.submit_confession(
            self.guild,
            author_id=9181,
            content="direct image",
            attachments=[FakeAttachment("one.png")],
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.state, "published")
        self.assertEqual(len(self.review_channel.sent), 0)
        self.assertEqual(len(self.confession_channel.sent), 1)

    async def test_replies_can_publish_without_forced_review_when_review_requirement_is_off(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=9182, content="reply to me", attachments=[])

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9183,
            content="publish directly",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )

        self.assertTrue(reply.ok)
        self.assertEqual(reply.state, "published")
        self.assertEqual(len(self.confession_channel.sent), 1)
        self.assertEqual(len(self.guild.threads), 1)
        self.assertEqual(len(list(self.guild.threads.values())[0].sent), 1)
        self.assertEqual(len(self.review_channel.sent), 0)

    async def test_published_confessions_do_not_show_reply_button_when_replies_disabled(self):
        await self._configure()
        published = await self.service.submit_confession(self.guild, author_id=920, content="no reply button", attachments=[])

        self.assertEqual(published.state, "published")
        self.assertEqual(_view_custom_ids(self.confession_channel.sent[0].view), ["bb-confession-post:compose"])

    async def test_sync_published_confession_views_updates_existing_posts_when_reply_policy_changes(self):
        await self._configure(review_channel=True, allow_replies=True)
        published = await self.service.submit_confession(self.guild, author_id=921, content="toggle me", attachments=[])
        self.assertEqual(published.state, "published")
        live_message = self.confession_channel.sent[0]
        self.assertIsNotNone(live_message.view)

        ok, message = await self.service.configure_guild(self.guild.id, allow_anonymous_replies=False)
        self.assertTrue(ok, message)
        await self.service.sync_published_confession_views(self.guild)
        self.assertEqual(_view_custom_ids(live_message.view), ["bb-confession-post:compose"])

        ok, message = await self.service.configure_guild(self.guild.id, allow_anonymous_replies=True)
        self.assertTrue(ok, message)
        await self.service.sync_published_confession_views(self.guild)
        self.assertIsNotNone(live_message.view)
        self.assertEqual(_view_custom_ids(live_message.view), ["bb-confession-post:compose", "bb-confession-post:reply"])

    async def test_latest_top_level_confession_is_only_post_with_create_launcher(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        first = await self.service.submit_confession(self.guild, author_id=9211, content="first top level", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=9212, content="second top level", attachments=[])

        self.assertEqual(first.state, "published")
        self.assertEqual(second.state, "published")
        self.assertEqual(_view_custom_ids(self.confession_channel.sent[0].view), ["bb-confession-post:reply"])
        self.assertEqual(
            _view_custom_ids(self.confession_channel.sent[1].view),
            ["bb-confession-post:compose", "bb-confession-post:reply"],
        )

    async def test_latest_launcher_with_replies_disabled_keeps_only_latest_create_button(self):
        await self._configure()
        first = await self.service.submit_confession(self.guild, author_id=9216, content="first latest off", attachments=[])
        second = await self.service.submit_confession(self.guild, author_id=9217, content="second latest off", attachments=[])

        self.assertEqual(first.state, "published")
        self.assertEqual(second.state, "published")
        self.assertIsNone(self.confession_channel.sent[0].view)
        self.assertEqual(_view_custom_ids(self.confession_channel.sent[1].view), ["bb-confession-post:compose"])

    async def test_latest_launcher_ignores_replies_and_owner_replies(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        owner = self._member(9213)
        root = await self.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="stay latest", attachments=[])
        self.assertEqual(root.state, "published")

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9214,
            content="thread reply",
            submission_kind="reply",
            parent_confession_id=root.confession_id,
        )
        self.assertTrue(reply.ok)
        response = self._reply_message(
            author=self._member(9215),
            reply_to_message_id=self.confession_channel.sent[0].id,
            content="owner ping",
        )
        await self.service.handle_member_response_message(response)
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, owner.id, limit=5)
        owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=owner.id,
            member=owner,
            opportunity_id=pending[0]["opportunity_id"],
            content="owner follow-up",
        )

        self.assertTrue(owner_reply.ok)
        self.assertEqual(_view_custom_ids(self.confession_channel.sent[0].view), ["bb-confession-post:compose", "bb-confession-post:reply"])
        self.assertEqual(len(self.confession_channel.sent), 2)

    async def test_self_delete_enforces_ownership_and_withdraws_pending_confession(self):
        await self._configure(review_mode=True, review_channel=True)
        queued = await self.service.submit_confession(self.guild, author_id=920, content="pending delete", attachments=[])
        self.assertEqual(queued.state, "queued")

        denied, denied_message = await self.service.self_delete_confession(self.guild, author_id=921, target_id=queued.confession_id)
        self.assertFalse(denied)
        self.assertIn("does not belong", denied_message.lower())

        ok, message = await self.service.self_delete_confession(self.guild, author_id=920, target_id=queued.confession_id)
        self.assertTrue(ok, message)
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, queued.confession_id)
        case = await self.service.store.fetch_case(self.guild.id, queued.case_id)
        self.assertEqual(stored["status"], "deleted")
        self.assertEqual(stored["review_status"], "withdrawn")
        self.assertIsNone(stored["content_body"])
        self.assertEqual(case["resolution_action"], "self_delete")

    async def test_self_delete_published_confession_removes_live_message(self):
        await self._configure()
        published = await self.service.submit_confession(self.guild, author_id=930, content="delete live", attachments=[])
        self.assertEqual(published.state, "published")
        live_message = self.confession_channel.sent[0]

        ok, message = await self.service.self_delete_confession(self.guild, author_id=930, target_id=published.confession_id)
        self.assertTrue(ok, message)
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id)
        self.assertTrue(live_message.deleted)
        self.assertEqual(stored["status"], "deleted")
        self.assertIsNone(stored["posted_message_id"])

    async def test_self_delete_published_root_confession_retires_discussion_thread(self):
        await self._configure(review_channel=True, allow_replies=True, anonymous_reply_review_required=False)
        published = await self.service.submit_confession(self.guild, author_id=931, content="thread root delete", attachments=[])
        await self.service.submit_confession(
            self.guild,
            author_id=932,
            content="reply before delete",
            submission_kind="reply",
            parent_confession_id=published.confession_id,
        )
        thread = list(self.guild.threads.values())[0]

        ok, message = await self.service.self_delete_confession(self.guild, author_id=931, target_id=published.confession_id)

        self.assertTrue(ok, message)
        self.assertTrue(thread.deleted or (thread.archived and thread.locked))
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id)
        self.assertIsNone(stored["discussion_thread_id"])

    async def test_self_edit_is_disabled_by_default_and_updates_pending_when_enabled(self):
        await self._configure(review_mode=True, review_channel=True)
        queued = await self.service.submit_confession(self.guild, author_id=940, content="draft text", attachments=[])
        blocked = await self.service.self_edit_confession(
            self.guild,
            author_id=940,
            target_id=queued.confession_id,
            content="edited text",
        )
        self.assertFalse(blocked.ok)
        self.assertIn("admins enable it", blocked.message.lower())

        ok, message = await self.service.configure_guild(self.guild.id, allow_self_edit=True)
        self.assertTrue(ok, message)
        edited = await self.service.self_edit_confession(
            self.guild,
            author_id=940,
            target_id=queued.confession_id,
            content="edited text",
        )
        self.assertTrue(edited.ok)
        self.assertEqual(edited.state, "queued")
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, queued.confession_id)
        case = await self.service.store.fetch_case(self.guild.id, queued.case_id)
        self.assertEqual(stored["content_body"], "edited text")
        self.assertEqual(case["review_version"], 2)

    async def test_confession_and_reply_respect_4000_character_limit(self):
        sentence = "Long but readable confession text for Babblebox. "
        max_body = (sentence * ((4000 // len(sentence)) + 1))[:4000]
        too_long = max_body + "!"
        await self._configure(review_mode=False, review_channel=True, allow_replies=True, anonymous_reply_review_required=False)

        confession = await self.service.submit_confession(self.guild, author_id=9410, content=max_body, attachments=[])
        self.assertTrue(confession.ok)
        self.assertEqual(confession.state, "published")
        self.assertEqual(len(self.confession_channel.sent[0].embeds[0].description), 4000)

        blocked_confession = await self.service.submit_confession(self.guild, author_id=9411, content=too_long, attachments=[])
        self.assertFalse(blocked_confession.ok)
        self.assertEqual(blocked_confession.state, "blocked")
        self.assertIn("4000", blocked_confession.message)

        reply = await self.service.submit_confession(
            self.guild,
            author_id=9412,
            content=max_body,
            submission_kind="reply",
            parent_confession_id=confession.confession_id,
        )
        self.assertTrue(reply.ok)
        self.assertEqual(reply.state, "published")
        self.assertEqual(len(list(self.guild.threads.values())[0].sent[0].embeds[0].description), 4000)

        blocked_reply = await self.service.submit_confession(
            self.guild,
            author_id=9413,
            content=too_long,
            submission_kind="reply",
            parent_confession_id=confession.confession_id,
        )
        self.assertFalse(blocked_reply.ok)
        self.assertEqual(blocked_reply.state, "blocked")
        self.assertIn("4000", blocked_reply.message)

    async def test_owner_reply_respects_4000_character_limit(self):
        owner_max_body = " ".join(f"owner-reply-{index:04d}" for index in range(300))[:4000]
        owner_too_long = owner_max_body + "!"
        await self._configure(review_mode=False, review_channel=True)
        responder = self._member(9414)
        root_owner = self._member(9415)
        await self.service.submit_confession(self.guild, author_id=root_owner.id, member=root_owner, content="owner root", attachments=[])
        await self.service.handle_member_response_message(
            self._reply_message(
                author=responder,
                reply_to_message_id=self.confession_channel.sent[0].id,
                content="owner prompt please",
            )
        )
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, root_owner.id, limit=5)

        owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=root_owner.id,
            member=root_owner,
            opportunity_id=pending[0]["opportunity_id"],
            content=owner_max_body,
        )
        self.assertTrue(owner_reply.ok)
        self.assertEqual(owner_reply.state, "published")

        second_owner = self._member(9416)
        await self.service.submit_confession(self.guild, author_id=second_owner.id, member=second_owner, content="owner root two", attachments=[])
        await self.service.handle_member_response_message(
            self._reply_message(
                author=self._member(9417),
                reply_to_message_id=self.confession_channel.sent[2].id,
                content="owner prompt too long",
            )
        )
        pending = await self.service.store.list_pending_owner_reply_opportunities_for_author(self.guild.id, second_owner.id, limit=5)

        blocked_owner_reply = await self.service.submit_owner_reply(
            self.guild,
            author_id=second_owner.id,
            member=second_owner,
            opportunity_id=pending[0]["opportunity_id"],
            content=owner_too_long,
        )
        self.assertFalse(blocked_owner_reply.ok)
        self.assertEqual(blocked_owner_reply.state, "blocked")
        self.assertIn("4000", blocked_owner_reply.message)

    async def test_self_edit_respects_4000_character_limit(self):
        edit_max_body = " ".join(f"pending-edit-{index:04d}" for index in range(300))[:4000]
        edit_too_long = edit_max_body + "!"
        await self._configure(review_mode=True, review_channel=True, allow_self_edit=True)
        queued = await self.service.submit_confession(self.guild, author_id=9416, content="edit me", attachments=[])
        self.assertEqual(queued.state, "queued")
        edited = await self.service.self_edit_confession(
            self.guild,
            author_id=9416,
            target_id=queued.confession_id,
            content=edit_max_body,
        )
        self.assertTrue(edited.ok)
        blocked_edit = await self.service.self_edit_confession(
            self.guild,
            author_id=9416,
            target_id=queued.confession_id,
            content=edit_too_long,
        )
        self.assertFalse(blocked_edit.ok)
        self.assertIn("4000", blocked_edit.message)

    async def test_support_request_details_stay_capped_at_1800(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        target = await self.service.submit_confession(self.guild, author_id=9417, content="report target", attachments=[])
        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=9417,
            kind="report",
            target_id=target.confession_id,
            details="r" * 1800,
        )
        self.assertTrue(ok, message)

        blocked_ok, blocked_message = await self.service.submit_support_request(
            self.guild,
            author_id=9418,
            kind="report",
            target_id=target.confession_id,
            details="r" * 1801,
        )
        self.assertFalse(blocked_ok)
        self.assertIn("1800", blocked_message)

    async def test_staff_detail_embed_splits_long_preview_across_multiple_fields(self):
        await self._configure(review_mode=True, review_channel=True)
        queued = await self.service.submit_confession(self.guild, author_id=9418, content="z" * 2500, attachments=[])

        detail = await self.service.build_target_status_embed(self.guild, queued.confession_id)

        preview_fields = [field for field in detail.to_dict().get("fields", []) if str(field.get("name", "")).startswith("Preview")]
        self.assertGreaterEqual(len(preview_fields), 3)
        self.assertTrue(all(len(str(field["value"])) <= 1024 for field in preview_fields))

    async def test_support_requests_require_configured_channel_and_hide_identity(self):
        await self._configure(review_mode=True, review_channel=True)
        blocked = await self.service.submit_confession(self.guild, author_id=950, content="nigger", attachments=[])
        self.assertEqual(blocked.state, "blocked")

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=950,
            kind="appeal",
            target_id=blocked.case_id,
            details="This was quoting harassment for review.",
        )
        self.assertFalse(ok)
        self.assertIn("configure", message.lower())

        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=950,
            kind="appeal",
            target_id=blocked.case_id,
            details="This was quoting harassment for review.",
        )
        self.assertTrue(ok, message)
        self.assertEqual(len(self.appeals_channel.sent), 1)
        rendered = json.dumps(self.appeals_channel.sent[0].embed.to_dict())
        self.assertIn("CT-", rendered)
        self.assertIn(blocked.confession_id, rendered)
        self.assertIn(blocked.case_id, rendered)
        self.assertNotIn("950", rendered)
        self.assertNotIn("<@950>", rendered)

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=951,
            kind="report",
            target_id=blocked.confession_id,
            details="This confession needs staff attention.",
        )
        self.assertTrue(ok, message)
        self.assertEqual(len(self.appeals_channel.sent), 2)

    async def test_support_requests_render_staff_action_views_without_identity_leaks(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        blocked = await self.service.submit_confession(self.guild, author_id=953, content="nigger", attachments=[])
        published = await self.service.submit_confession(self.guild, author_id=954, content="report me", attachments=[])

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=953,
            kind="appeal",
            target_id=blocked.case_id,
            details="Please review the full context.",
        )
        self.assertTrue(ok, message)
        appeal_message = self.appeals_channel.sent[0]
        self.assertIsNotNone(appeal_message.view)
        appeal_labels = [child.label for child in appeal_message.view.children if getattr(child, "label", None)]
        self.assertIn("Resolve", appeal_labels)
        self.assertIn("False Positive", appeal_labels)
        self.assertIn("Details", appeal_labels)
        self.assertIn("Refresh", appeal_labels)
        appeal_custom_ids = [child.custom_id for child in appeal_message.view.children if getattr(child, "custom_id", None)]
        self.assertTrue(all(custom_id.count("CT-") == 1 for custom_id in appeal_custom_ids))
        self.assertTrue(all(blocked.case_id not in custom_id for custom_id in appeal_custom_ids))
        self.assertTrue(all(blocked.confession_id not in custom_id for custom_id in appeal_custom_ids))
        self.assertTrue(all("953" not in custom_id for custom_id in appeal_custom_ids))

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=954,
            kind="report",
            target_id=published.confession_id,
            details="This confession needs moderation.",
        )
        self.assertTrue(ok, message)
        report_message = self.appeals_channel.sent[1]
        report_labels = [child.label for child in report_message.view.children if getattr(child, "label", None)]
        self.assertIn("Resolve", report_labels)
        self.assertIn("Delete", report_labels)
        self.assertIn("Details", report_labels)
        self.assertIn("Refresh", report_labels)
        rendered = json.dumps([message.embed.to_dict() for message in self.appeals_channel.sent])
        self.assertNotIn("953", rendered)
        self.assertNotIn("954", rendered)
        stored = await self.service.store.list_support_tickets(self.guild.id, status="open", limit=10)
        self.assertEqual({ticket["kind"] for ticket in stored}, {"appeal", "report"})

    async def test_support_ticket_actions_resolve_and_close_stale_targets_cleanly(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        blocked = await self.service.submit_confession(self.guild, author_id=955, content="nigger", attachments=[])
        self.assertEqual(blocked.state, "blocked")

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=955,
            kind="appeal",
            target_id=blocked.case_id,
            details="Please review the quote in context.",
        )
        self.assertTrue(ok, message)
        open_ticket = (await self.service.store.list_support_tickets(self.guild.id, status="open", limit=10))[0]
        appeal_message = self.appeals_channel.sent[0]

        ok, message, updated_ticket = await self.service.handle_support_ticket_action(
            self.guild,
            ticket_id=open_ticket["ticket_id"],
            action="resolve",
        )

        self.assertTrue(ok, message)
        self.assertEqual(updated_ticket["status"], "resolved")
        self.assertEqual(updated_ticket["resolution_action"], "resolve")
        self.assertIsNone(appeal_message.view)

        await self.service.store.upsert_support_ticket(
            {
                "ticket_id": "CT-STALE01",
                "guild_id": self.guild.id,
                "kind": "report",
                "action_target_id": "CF-MISSING01",
                "reference_confession_id": "CF-MISSING01",
                "reference_case_id": None,
                "context_label": "Stale published confession report",
                "details": "This target vanished before review.",
                "status": "open",
                "resolution_action": None,
                "message_channel_id": self.appeals_channel.id,
                "message_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "resolved_at": None,
            }
        )
        ok, message = await self.service.sync_support_ticket(self.guild, "CT-STALE01")
        self.assertTrue(ok, message)
        stale_message = self.appeals_channel.sent[-1]

        ok, message, stale_ticket = await self.service.handle_support_ticket_action(
            self.guild,
            ticket_id="CT-STALE01",
            action="delete",
        )

        self.assertTrue(ok, message)
        self.assertEqual(stale_ticket["status"], "resolved")
        self.assertEqual(stale_ticket["resolution_action"], "stale")
        self.assertIsNone(stale_message.view)

    async def test_support_channel_snapshot_marks_public_channel_unsafe(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        self.appeals_channel.public_view = True

        snapshot = self.service.support_channel_snapshot(self.guild)

        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["status"], "public")
        self.assertIn("@everyone", snapshot["message"])

    async def test_support_channel_snapshot_reports_missing_bot_permissions(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        self.appeals_channel.bot_can_embed = False

        snapshot = self.service.support_channel_snapshot(self.guild)

        self.assertFalse(snapshot["ok"])
        self.assertEqual(snapshot["status"], "bot_missing_permissions")
        self.assertIn("Embed Links", snapshot["message"])

    async def test_support_requests_fail_closed_when_channel_becomes_public(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        blocked = await self.service.submit_confession(self.guild, author_id=952, content="nigger", attachments=[])
        self.assertEqual(blocked.state, "blocked")
        self.appeals_channel.public_view = True

        ok, message = await self.service.submit_support_request(
            self.guild,
            author_id=952,
            kind="appeal",
            target_id=blocked.case_id,
            details="Please review the context.",
        )

        self.assertFalse(ok)
        self.assertIn("@everyone", message)
        self.assertEqual(len(self.appeals_channel.sent), 0)

    async def test_member_panel_embed_reports_support_only_when_private_channel_is_ready(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        ready_embed = self.service.build_member_panel_embed(self.guild)
        ready_rendered = json.dumps(ready_embed.to_dict())
        self.assertIn("Status: **Ready**", ready_rendered)

        self.appeals_channel.public_view = True
        unsafe_embed = self.service.build_member_panel_embed(self.guild)
        unsafe_rendered = json.dumps(unsafe_embed.to_dict())
        self.assertIn("Public / Unsafe", unsafe_rendered)
        self.assertIn("@everyone", unsafe_rendered)

    async def test_dashboard_embed_reports_support_channel_health(self):
        await self._configure(review_mode=True, review_channel=True, appeals_channel=True)
        self.appeals_channel.public_view = True

        embed = await self.service.build_dashboard_embed(self.guild, section="review")
        rendered = json.dumps(embed.to_dict())

        self.assertIn("Support Channel", rendered)
        self.assertIn("Public / Unsafe", rendered)

    async def test_image_only_restriction_blocks_attachments_but_not_text_and_clear_restores(self):
        await self._configure(review_channel=True, allow_images=True)
        published = await self.service.submit_confession(self.guild, author_id=960, content="moderate me", attachments=[])
        self.assertEqual(published.state, "published")

        ok, message = await self.service.handle_staff_action(self.guild, target_id=published.confession_id, action="restrict_images")
        self.assertTrue(ok, message)
        state_link = await self.service.store.fetch_author_link((await self.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id))["submission_id"])
        state = await self.service.store.fetch_enforcement_state(self.guild.id, state_link["author_user_id"])
        self.assertTrue(state["image_restriction_active"])

        blocked = await self.service.submit_confession(
            self.guild,
            author_id=960,
            content="with image",
            attachments=[FakeAttachment("image.png")],
        )
        self.assertFalse(blocked.ok)
        self.assertEqual(blocked.state, "blocked")
        self.assertIn("image", blocked.message.lower())

        state = await self.service.store.fetch_enforcement_state(self.guild.id, state_link["author_user_id"])
        state["cooldown_until"] = None
        await self.service.store.upsert_enforcement_state(state)
        allowed = await self.service.submit_confession(self.guild, author_id=960, content="text only still works", attachments=[])
        self.assertTrue(allowed.ok)

        clear_ok, clear_message = await self.service.handle_staff_action(self.guild, target_id=published.confession_id, action="clear")
        self.assertTrue(clear_ok, clear_message)
        cleared_state = await self.service.store.fetch_enforcement_state(self.guild.id, state_link["author_user_id"])
        self.assertFalse(cleared_state["image_restriction_active"])

    async def test_published_duplicate_and_near_duplicate_signatures_still_block_after_publish(self):
        await self._configure()
        first = await self.service.submit_confession(self.guild, author_id=970, content="duplicate probe text", attachments=[])
        self.assertTrue(first.ok)
        state = await self.service.store.fetch_enforcement_state(self.guild.id, 970)
        state["cooldown_until"] = None
        await self.service.store.upsert_enforcement_state(state)

        duplicate = await self.service.submit_confession(self.guild, author_id=970, content="duplicate probe text", attachments=[])
        self.assertFalse(duplicate.ok)
        self.assertIn("duplicate_spam", duplicate.flag_codes)

        state = await self.service.store.fetch_enforcement_state(self.guild.id, 970)
        state["cooldown_until"] = None
        await self.service.store.upsert_enforcement_state(state)
        near_duplicate = await self.service.submit_confession(self.guild, author_id=970, content="duplicate probe text!", attachments=[])
        self.assertFalse(near_duplicate.ok)
        self.assertIn("near_duplicate_spam", near_duplicate.flag_codes)

    async def test_same_confession_text_hashes_differ_across_guilds(self):
        await self._configure()
        ok, message = await self.service.configure_guild(
            self.other_guild.id,
            enabled=True,
            confession_channel_id=self.other_confession_channel.id,
            review_mode=False,
        )
        self.assertTrue(ok, message)

        first = await self.service.submit_confession(self.guild, author_id=971, content="same text across guilds", attachments=[])
        second = await self.service.submit_confession(self.other_guild, author_id=971, content="same text across guilds", attachments=[])

        self.assertTrue(first.ok)
        self.assertTrue(second.ok)
        first_submission = await self.service.store.fetch_submission_by_confession_id(self.guild.id, first.confession_id)
        second_submission = await self.service.store.fetch_submission_by_confession_id(self.other_guild.id, second.confession_id)
        self.assertNotEqual(first_submission["content_fingerprint"], second_submission["content_fingerprint"])
        self.assertNotEqual(first_submission["fuzzy_signature"], second_submission["fuzzy_signature"])

    async def test_dashboard_embed_reports_partial_privacy_hardening_status(self):
        raw_store = self.service.store._store
        raw_store.submissions["sub-legacy"] = {
            "submission_id": "sub-legacy",
            "guild_id": self.guild.id,
            "confession_id": "CF-LEGACY1",
            "submission_kind": "confession",
            "reply_flow": None,
            "owner_reply_generation": None,
            "parent_confession_id": None,
            "status": "queued",
            "review_status": "pending",
            "staff_preview": "Legacy preview",
            "content_body": "Legacy body",
            "shared_link_url": None,
            "content_fingerprint": "legacy-fingerprint",
            "similarity_key": "legacy similarity key",
            "fuzzy_signature": "feedfacefeedface",
            "flag_codes": [],
            "attachment_meta": [],
            "posted_channel_id": None,
            "posted_message_id": None,
            "current_case_id": None,
            "created_at": "2026-04-03T00:00:00+00:00",
            "published_at": None,
            "resolved_at": None,
        }

        embed = await self.service.build_dashboard_embed(self.guild, section="overview")
        rendered = json.dumps(embed.to_dict())

        self.assertIn("Privacy Hardening", rendered)
        self.assertIn("State: **Partial**", rendered)
        self.assertIn("Backfill: **Still needed for this server**", rendered)

    async def test_start_logs_privacy_warning_when_backfill_is_needed(self):
        service = ConfessionsService(self.bot, store=ConfessionsStore(backend="memory"))
        try:
            with mock.patch.object(
                service.store,
                "fetch_privacy_status",
                new=mock.AsyncMock(
                    return_value={
                        "state": "partial",
                        "needs_backfill": True,
                        "categories": ["plaintext_submission_content", "legacy_author_links"],
                    }
                ),
            ), self.assertLogs("babblebox.confessions_service", level="WARNING") as captured:
                started = await service.start()
            self.assertTrue(started)
            rendered = " ".join(captured.output)
            self.assertIn("Confessions privacy warning: hardening is partial.", rendered)
            self.assertIn("python -m babblebox.confessions_backfill --dry-run", rendered)
        finally:
            await service.close()

    async def test_start_logs_ready_when_privacy_hardening_is_clean(self):
        service = ConfessionsService(self.bot, store=ConfessionsStore(backend="memory"))
        try:
            with mock.patch.object(
                service.store,
                "fetch_privacy_status",
                new=mock.AsyncMock(return_value={"state": "ready", "needs_backfill": False, "categories": []}),
            ), self.assertLogs("babblebox.confessions_service", level="INFO") as captured:
                started = await service.start()
            self.assertTrue(started)
            rendered = " ".join(captured.output)
            self.assertIn("Confessions privacy status: hardening is ready.", rendered)
        finally:
            await service.close()


class ConfessionsCogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.allowed_role = self.guild.add_role(FakeRole(701, name="Allowed"))
        self.blocked_role = self.guild.add_role(FakeRole(702, name="Blocked"))
        self.guild.add_channel(FakeChannel(20, name="confessions"))
        self.guild.add_channel(FakeChannel(30, name="review"))
        self.guild.add_channel(FakeChannel(40, name="panel"))
        self.guild.add_channel(FakeChannel(50, name="appeals"))
        self.bot = FakeBot([self.guild])
        self.cog = ConfessionsCog(self.bot)
        self.bot._cog = self.cog
        original = self.cog.service
        store = ConfessionsStore(backend="memory")
        self.cog.service = ConfessionsService(self.bot, store=store)
        await self.cog.service.start()
        self.bot.confessions_service = self.cog.service
        self._original_service = original

    async def _flush_background_tasks(self, cog: ConfessionsCog | None = None):
        target = cog or self.cog
        tasks = tuple(target._background_tasks)
        if tasks:
            await asyncio.gather(*tasks)

    async def asyncTearDown(self):
        tasks = tuple(self.cog._background_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.cog.service.close()
        await self._original_service.close()

    def _member(self, user_id: int, *, roles: list[FakeRole] | None = None, manage_guild: bool = False) -> FakeUser:
        member = FakeUser(user_id, roles=roles, manage_guild=manage_guild)
        self.guild.add_member(member)
        return member

    def _reply_message(self, *, author: FakeUser, reply_to_message_id: int, content: str) -> FakeMessage:
        channel = self.guild.get_channel(20)
        message = FakeMessage(
            content=content,
            author=author,
            guild=self.guild,
            channel=channel,
            reference=FakeMessageReference(message_id=reply_to_message_id),
        )
        channel._messages[message.id] = message
        return message

    async def _seed_owner_reply_inbox(
        self,
        *,
        owner_id: int,
        pending_count: int = 1,
        name_length: int = 12,
        preview_length: int = 24,
    ) -> FakeUser:
        owner = self._member(owner_id)
        await self.cog.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="owner reply inbox seed",
            attachments=[],
        )
        confession_message_id = self.guild.get_channel(20).sent[-1].id
        for offset in range(pending_count):
            responder = self._member(owner_id + 100 + offset)
            responder.display_name = f"Responder {offset} " + ("X" * name_length)
            content_parts = [f"public-response-{offset}"]
            while len(" ".join(content_parts)) < preview_length:
                content_parts.append(f"detail-{offset}-{len(content_parts)}")
            response = self._reply_message(
                author=responder,
                reply_to_message_id=confession_message_id,
                content=" ".join(content_parts),
            )
            await self.cog.service.handle_member_response_message(response)
        return owner

    async def test_status_command_opens_private_dashboard(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(1, manage_guild=True))

        await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, None)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Control Panel")
        self.assertIsNotNone(ctx.send_calls[0]["view"])

    async def test_status_command_denies_members_privately(self):
        ctx = FakeContext(guild=self.guild, author=self._member(2))

        await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, None)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertIn("Manage Server", ctx.send_calls[0]["embed"].description)

    async def test_status_command_returns_private_failure_when_dashboard_render_raises_after_defer(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(9, manage_guild=True))

        with mock.patch.object(self.cog.service, "build_dashboard_embed", side_effect=RuntimeError("dashboard boom")):
            await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, None)

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Status")
        self.assertIn("could not open", ctx.send_calls[0]["embed"].description.lower())

    async def test_slash_confess_create_command_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        member = self._member(11)
        ctx = FakeContext(guild=self.guild, author=member)

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 1)
        self.assertEqual(ctx.interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_confess_create_blocks_restricted_members_before_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        member = self._member(12)
        ctx = FakeContext(guild=self.guild, author=member)
        await self.cog.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "active_restriction": "temp_ban",
                "restricted_until": "2999-01-01T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 3,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": "CS-LOCKED",
                "image_restriction_active": False,
                "image_restricted_until": None,
                "image_restriction_case_id": None,
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )
        self.cog.service._cache_enforcement_state(await self.cog.service.store.fetch_enforcement_state(self.guild.id, member.id))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Paused")

    async def test_confess_create_turns_image_restriction_into_text_only_modal(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
        )
        member = self._member(13)
        ctx = FakeContext(guild=self.guild, author=member)
        await self.cog.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": member.id,
                "active_restriction": "none",
                "restricted_until": None,
                "is_permanent_ban": False,
                "strike_count": 0,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": None,
                "image_restriction_active": True,
                "image_restricted_until": "2999-01-01T00:00:00+00:00",
                "image_restriction_case_id": "CS-IMG",
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )
        self.cog.service._cache_enforcement_state(await self.cog.service.store.fetch_enforcement_state(self.guild.id, member.id))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        modal = ctx.interaction.response.modal_calls[0]
        self.assertIsNone(modal.upload_input)
        self.assertIn("image attachments are paused", modal.body_input.placeholder.lower())

    async def test_confess_create_with_images_enabled_uses_labeled_upload_component(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
        )
        member = self._member(113)
        ctx = FakeContext(guild=self.guild, author=member)

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 1)
        modal = ctx.interaction.response.modal_calls[0]
        payload = ctx.interaction.response.modal_payloads[0]
        self.assertIsNotNone(modal.upload_input)
        self.assertTrue(any(int(component.get("type") or 0) == 18 for component in payload["components"]))
        self.assertFalse(any(int(component.get("type") or 0) == 19 for component in payload["components"]))

    async def test_confess_create_storage_unavailable_returns_private_failure_without_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        self.cog.service.storage_ready = False
        self.cog.service.storage_error = "db offline"
        ctx = FakeContext(guild=self.guild, author=self._member(114))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")
        self.assertIn("database", ctx.interaction.response.sent[0]["kwargs"]["embed"].description.lower())

    async def test_confess_create_operability_unavailable_returns_private_failure_without_modal(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=None,
            review_mode=True,
        )
        ctx = FakeContext(guild=self.guild, author=self._member(115))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")
        self.assertIn("review channel", ctx.interaction.response.sent[0]["kwargs"]["embed"].description.lower())

    async def test_confess_create_role_gated_member_sees_gate_before_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        await self.cog.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        ctx = FakeContext(guild=self.guild, author=self._member(116))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertIn("selected roles", ctx.interaction.response.sent[0]["kwargs"]["embed"].description.lower())

    async def test_confess_create_falls_back_to_text_only_when_modal_upload_runtime_is_not_safe(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
        )
        ctx = FakeContext(guild=self.guild, author=self._member(117))

        with mock.patch.object(self.cog, "modal_file_upload_available", return_value=False):
            await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 1)
        modal = ctx.interaction.response.modal_calls[0]
        self.assertIsNone(modal.upload_input)
        self.assertIn("temporarily unavailable", modal.body_input.placeholder.lower())

    async def test_confess_create_send_modal_failure_returns_private_create_unavailable(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=self._member(118))

        with mock.patch.object(ctx.interaction.response, "send_modal", side_effect=RuntimeError("send boom")):
            await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confession Composer Unavailable")
        self.assertTrue(ctx.interaction.response.is_done())

    async def test_confess_create_modal_construction_failure_returns_private_create_unavailable(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=self._member(119))

        with mock.patch("babblebox.cogs.confessions.ConfessionComposerModal", side_effect=RuntimeError("construct boom")):
            await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 0)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confession Composer Unavailable")

    async def test_confess_reply_to_user_blocks_restricted_owner_before_inbox(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        owner = self._member(14)
        ctx = FakeContext(guild=self.guild, author=owner)
        await self.cog.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": owner.id,
                "active_restriction": "suspended",
                "restricted_until": "2999-01-01T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 1,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": "CS-OWNER",
                "image_restriction_active": False,
                "image_restricted_until": None,
                "image_restriction_case_id": None,
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )

        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.sent), 1)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Paused")

    def test_confess_create_slash_subcommand_is_registered(self):
        command_names = {command.name for command in self.cog.confess_group.app_command.commands}

        self.assertEqual(command_names, {"about", "appeal", "create", "manage", "reply-to-user", "report"})

    async def test_cog_load_registers_global_fallback_views(self):
        self.bot.views.clear()
        self.bot._ready = False

        with mock.patch.object(self.cog.service, "start", new=mock.AsyncMock(return_value=True)):
            await self.cog.cog_load()

        self.assertIs(self.bot.confessions_service, self.cog.service)
        self.assertEqual(len(self.bot.views), 4)
        self.assertEqual([message_id for _, message_id in self.bot.views], [None, None, None, None])
        self.assertCountEqual(
            [type(view) for view, _ in self.bot.views],
            [
                StatelessConfessionMemberPanelView,
                StatelessConfessionsAdminPanelView,
                StatelessPublishedConfessionReplyView,
                StatelessOwnerReplyPromptView,
            ],
        )

    async def test_reply_ui_uses_explicit_anonymity_copy(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
        )
        published = await self.cog.service.submit_confession(self.guild, author_id=200, content="posted", attachments=[])
        self.assertEqual(published.state, "published")

        reply_modal = ReplyComposerModal(self.cog, guild_id=self.guild.id, default_target=published.confession_id)
        result_view = self.cog.build_member_result_view(result=published, guild_id=self.guild.id)
        public_view = self.cog.build_public_confession_view(guild_id=self.guild.id)

        self.assertEqual(reply_modal.body_input.label, "Anonymous reply")
        self.assertIn("stays anonymous", reply_modal.body_input.placeholder)
        self.assertIn("private approval", reply_modal.body_input.placeholder)
        self.assertNotIn("reviewed", reply_modal.body_input.placeholder.casefold())
        self.assertIn("Reply to confession anonymously", [child.label for child in result_view.children if getattr(child, "label", None)])
        self.assertIn("Reply to confession anonymously", [child.label for child in public_view.children if getattr(child, "label", None)])

    async def test_reply_to_user_command_opens_private_owner_reply_inbox(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(201)
        published = await self.cog.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="need support", attachments=[])
        response = self._reply_message(author=self._member(202), reply_to_message_id=self.guild.get_channel(20).sent[0].id, content="we hear you")
        await self.cog.service.handle_member_response_message(response)

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        self.assertEqual(published.state, "published")
        self.assertEqual(len(ctx.interaction.response.sent), 1)
        payload = ctx.interaction.response.sent[0]["kwargs"]
        self.assertTrue(payload["ephemeral"])
        self.assertEqual(payload["embed"].title, "Owner Reply Inbox")
        self.assertIsNotNone(payload["view"])
        self.assertEqual(payload["view"].children[0].placeholder, "Choose a member response to review privately")

    async def test_reply_to_user_command_opens_private_owner_reply_inbox_with_long_pending_summaries(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = await self._seed_owner_reply_inbox(
            owner_id=320,
            pending_count=5,
            name_length=120,
            preview_length=220,
        )

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        payload = ctx.interaction.response.sent[0]["kwargs"]
        pending_value = _embed_fields_by_name(payload["embed"])["Pending Responses"]["value"]
        self.assertLessEqual(len(pending_value), 1024)
        self.assertTrue(payload["ephemeral"])
        self.assertIsNotNone(payload["view"])
        self.assertEqual(len(payload["view"].children[0].options), 5)

    async def test_owner_reply_inbox_embed_condenses_long_pending_summaries_with_overflow_note(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = await self._seed_owner_reply_inbox(
            owner_id=330,
            pending_count=5,
            name_length=120,
            preview_length=220,
        )
        contexts = await self.cog.service.list_pending_owner_reply_contexts(
            self.guild,
            author_id=owner.id,
            limit=5,
        )
        for context in contexts:
            context["opportunity"]["source_author_name"] = "*" * 120

        embed = self.cog.service.build_owner_reply_inbox_embed(self.guild, contexts)
        pending_value = _embed_fields_by_name(embed)["Pending Responses"]["value"]

        self.assertLessEqual(len(pending_value), 1024)
        self.assertIn("more pending response(s) in the selector below.", pending_value)

    async def test_reply_to_user_command_falls_back_to_simple_inbox_when_rich_payload_send_fails(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = await self._seed_owner_reply_inbox(owner_id=340, pending_count=1, name_length=24, preview_length=48)
        invalid_embed = discord.Embed(
            title="Owner Reply Inbox",
            description="invalid rich inbox",
            color=discord.Color.blurple(),
        )
        invalid_embed.add_field(name="Pending Responses", value="x" * 1025, inline=False)

        ctx = FakeContext(guild=self.guild, author=owner)
        with mock.patch.object(self.cog.service, "build_owner_reply_inbox_embed", return_value=invalid_embed):
            await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        payload = ctx.interaction.response.sent[0]["kwargs"]
        self.assertEqual(payload["embed"].title, "Owner Reply Inbox")
        self.assertIn("pending member response", payload["embed"].description.casefold())
        self.assertIsNotNone(payload["view"])
        self.assertNotIn("could not open your private owner-reply inbox", payload["embed"].description.casefold())

    async def test_owner_reply_prompt_open_and_dismiss_resolve_private_context(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(203)
        await self.cog.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="prompt me", attachments=[])
        response = self._reply_message(author=self._member(204), reply_to_message_id=self.guild.get_channel(20).sent[0].id, content="direct response")
        await self.cog.service.handle_member_response_message(response)
        prompt_message = owner.sent[0]

        open_interaction = FakeInteraction(guild=None, user=owner, message=prompt_message, client=self.bot)
        await self.cog._handle_owner_reply_prompt_open(open_interaction)

        self.assertEqual(len(open_interaction.response.modal_calls), 1)
        self.assertEqual(open_interaction.response.modal_calls[0].title, "Reply to Member Anonymously")

        dismiss_interaction = FakeInteraction(guild=None, user=owner, message=prompt_message, client=self.bot)
        await self.cog._handle_owner_reply_prompt_dismiss(dismiss_interaction)

        self.assertEqual(len(dismiss_interaction.response.edits), 1)
        self.assertEqual(dismiss_interaction.response.edits[0]["embed"].title, "Owner Reply Prompt")
        self.assertIsNone(dismiss_interaction.response.edits[0]["view"])

    async def test_owner_reply_prompt_rejects_non_owner(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(205)
        await self.cog.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="private owner flow", attachments=[])
        response = self._reply_message(author=self._member(206), reply_to_message_id=self.guild.get_channel(20).sent[0].id, content="hello owner")
        await self.cog.service.handle_member_response_message(response)

        interaction = FakeInteraction(guild=None, user=self._member(207), message=owner.sent[0], client=self.bot)
        await self.cog._handle_owner_reply_prompt_open(interaction)

        self.assertEqual(len(interaction.response.sent), 1)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Unavailable")
        self.assertIn("does not belong to you", interaction.response.sent[0]["kwargs"]["embed"].description)

    async def test_owner_reply_inbox_action_reply_opens_modal_when_payload_is_valid(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(208)
        await self.cog.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="open the inbox",
            attachments=[],
        )
        response = self._reply_message(
            author=self._member(209),
            reply_to_message_id=self.guild.get_channel(20).sent[0].id,
            content="please reply privately",
        )
        await self.cog.service.handle_member_response_message(response)

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        inbox_view = ctx.interaction.response.sent[0]["kwargs"]["view"]
        select = inbox_view.children[0]
        select._values = [select.options[0].value]
        select_interaction = FakeInteraction(guild=self.guild, user=owner)

        await select.callback(select_interaction)

        action_view = select_interaction.response.edits[0]["view"]
        reply_interaction = FakeInteraction(guild=self.guild, user=owner)

        await action_view.reply_button.callback(reply_interaction)

        self.assertEqual(len(reply_interaction.response.modal_calls), 1)
        self.assertEqual(reply_interaction.response.modal_calls[0].title, "Reply to Member Anonymously")
        self.assertEqual(len(reply_interaction.response.sent), 0)

    async def test_owner_reply_inbox_select_opens_detail_and_action_reply_handles_modal_failure_privately(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(208)
        published = await self.cog.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="open the inbox",
            attachments=[],
        )
        response = self._reply_message(
            author=self._member(209),
            reply_to_message_id=self.guild.get_channel(20).sent[0].id,
            content="please reply privately",
        )
        await self.cog.service.handle_member_response_message(response)

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        inbox_view = ctx.interaction.response.sent[0]["kwargs"]["view"]
        select = inbox_view.children[0]
        select._values = [select.options[0].value]
        select_interaction = FakeInteraction(guild=self.guild, user=owner)

        await select.callback(select_interaction)

        self.assertEqual(select_interaction.response.edits[0]["embed"].title, "Reply to Member Anonymously")
        action_view = select_interaction.response.edits[0]["view"]
        reply_interaction = FakeInteraction(guild=self.guild, user=owner)
        reply_interaction.response.send_modal = mock.AsyncMock(side_effect=RuntimeError("send failed"))

        await action_view.reply_button.callback(reply_interaction)

        self.assertEqual(published.state, "published")
        self.assertEqual(len(reply_interaction.response.modal_calls), 0)
        self.assertEqual(reply_interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Unavailable")

    async def test_owner_reply_inbox_action_reply_handles_payload_validation_failure_privately(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(214)
        await self.cog.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="payload invalid",
            attachments=[],
        )
        response = self._reply_message(
            author=self._member(215),
            reply_to_message_id=self.guild.get_channel(20).sent[0].id,
            content="please answer this one",
        )
        await self.cog.service.handle_member_response_message(response)

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        inbox_view = ctx.interaction.response.sent[0]["kwargs"]["view"]
        select = inbox_view.children[0]
        select._values = [select.options[0].value]
        select_interaction = FakeInteraction(guild=self.guild, user=owner)

        await select.callback(select_interaction)

        action_view = select_interaction.response.edits[0]["view"]
        reply_interaction = FakeInteraction(guild=self.guild, user=owner)
        original_init = OwnerReplyComposerModal.__init__

        def broken_init(modal_self, *args, **kwargs):
            original_init(modal_self, *args, **kwargs)
            modal_self.body_input.placeholder = "x" * 101

        with mock.patch.object(OwnerReplyComposerModal, "__init__", new=broken_init):
            await action_view.reply_button.callback(reply_interaction)

        self.assertEqual(len(reply_interaction.response.modal_calls), 0)
        self.assertEqual(reply_interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Unavailable")
        self.assertIn("private owner-reply composer", reply_interaction.response.sent[0]["kwargs"]["embed"].description.lower())

    async def test_owner_reply_inbox_action_reply_rechecks_preflight_before_opening_modal(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(216)
        await self.cog.service.submit_confession(
            self.guild,
            author_id=owner.id,
            member=owner,
            content="stale owner reply",
            attachments=[],
        )
        response = self._reply_message(
            author=self._member(217),
            reply_to_message_id=self.guild.get_channel(20).sent[0].id,
            content="reply after inbox opens",
        )
        await self.cog.service.handle_member_response_message(response)

        ctx = FakeContext(guild=self.guild, author=owner)
        await ConfessionsCog.confess_reply_to_user_command.callback(self.cog, ctx)

        inbox_view = ctx.interaction.response.sent[0]["kwargs"]["view"]
        select = inbox_view.children[0]
        select._values = [select.options[0].value]
        select_interaction = FakeInteraction(guild=self.guild, user=owner)

        await select.callback(select_interaction)

        action_view = select_interaction.response.edits[0]["view"]
        await self.cog.service.store.upsert_enforcement_state(
            {
                "guild_id": self.guild.id,
                "user_id": owner.id,
                "active_restriction": "temp_ban",
                "restricted_until": "2999-01-01T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 3,
                "last_strike_at": None,
                "cooldown_until": None,
                "burst_count": 0,
                "burst_window_started_at": None,
                "last_case_id": "CS-LOCKED",
                "image_restriction_active": False,
                "image_restricted_until": None,
                "image_restriction_case_id": None,
                "updated_at": "2999-01-01T00:00:00+00:00",
            }
        )
        self.cog.service._cache_enforcement_state(await self.cog.service.store.fetch_enforcement_state(self.guild.id, owner.id))
        reply_interaction = FakeInteraction(guild=self.guild, user=owner)

        await action_view.reply_button.callback(reply_interaction)

        self.assertEqual(len(reply_interaction.response.modal_calls), 0)
        self.assertEqual(reply_interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Paused")

    async def test_owner_reply_prompt_open_handles_modal_construction_failure_privately(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(210)
        await self.cog.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="construct fail", attachments=[])
        response = self._reply_message(author=self._member(211), reply_to_message_id=self.guild.get_channel(20).sent[0].id, content="reply please")
        await self.cog.service.handle_member_response_message(response)
        prompt_message = owner.sent[0]
        interaction = FakeInteraction(guild=None, user=owner, message=prompt_message, client=self.bot)

        with mock.patch("babblebox.cogs.confessions.OwnerReplyComposerModal", side_effect=RuntimeError("modal boom")):
            await self.cog._handle_owner_reply_prompt_open(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 0)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Unavailable")

    async def test_owner_reply_prompt_open_handles_send_modal_failure_privately(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        owner = self._member(212)
        await self.cog.service.submit_confession(self.guild, author_id=owner.id, member=owner, content="send fail", attachments=[])
        response = self._reply_message(author=self._member(213), reply_to_message_id=self.guild.get_channel(20).sent[0].id, content="prompt send fail")
        await self.cog.service.handle_member_response_message(response)
        prompt_message = owner.sent[0]
        interaction = FakeInteraction(guild=None, user=owner, message=prompt_message, client=self.bot)
        interaction.response.send_modal = mock.AsyncMock(side_effect=RuntimeError("discord modal failure"))

        await self.cog._handle_owner_reply_prompt_open(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 0)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Owner Reply Unavailable")

    async def test_safe_open_member_modal_rejects_invalid_payload_before_send_modal(self):
        interaction = FakeInteraction(guild=self.guild, user=self._member(218))
        interaction.response.send_modal = mock.AsyncMock()

        class OversizedPlaceholderModal(discord.ui.Modal, title="Test Modal"):
            def __init__(self):
                super().__init__(timeout=60)
                self.add_item(discord.ui.TextInput(label="Reason", placeholder="x" * 101))

        opened = await self.cog._safe_open_member_modal(
            interaction,
            modal_factory=OversizedPlaceholderModal,
            failure_title="Composer Unavailable",
            failure_message="Babblebox blocked that private composer before it reached Discord.",
            construct_code="test_modal_construct_failed",
            construct_stage="test_modal_construct",
            payload_code="test_modal_payload_invalid",
            payload_stage="test_send_modal",
            send_code="test_send_modal_failed",
            send_stage="test_send_modal",
        )

        self.assertFalse(opened)
        interaction.response.send_modal.assert_not_awaited()
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Composer Unavailable")

    async def test_member_panel_button_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=self._member(12))
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)

        await view.send_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")
        self.assertIsNone(interaction.response.modal_calls[0].upload_input)

    async def test_stateless_member_panel_fallback_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=self._member(112), client=self.bot)

        await StatelessConfessionMemberPanelView().send_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_stateless_member_panel_fallback_fails_closed_privately_when_cog_is_missing(self):
        interaction = FakeInteraction(
            guild=self.guild,
            user=self._member(113),
            client=types.SimpleNamespace(get_cog=lambda name: None),
        )

        await StatelessConfessionMemberPanelView().send_button.callback(interaction)

        self.assertEqual(len(interaction.response.sent), 1)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")

    async def test_member_panel_manage_and_support_buttons_open_private_flows(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)
        self.assertFalse(view.support_button.disabled)

        manage_interaction = FakeInteraction(guild=self.guild, user=self._member(120))
        await view.manage_button.callback(manage_interaction)
        self.assertEqual(manage_interaction.response.modal_calls[0].title, "Manage My Confession")

        support_interaction = FakeInteraction(guild=self.guild, user=self._member(121))
        await view.support_button.callback(support_interaction)
        self.assertEqual(support_interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support")
        self.assertIsNotNone(support_interaction.response.sent[0]["kwargs"]["view"])

    async def test_confessions_setup_rejects_public_appeals_channel(self):
        self.guild.channels[50].public_view = True
        ctx = FakeContext(guild=self.guild, author=FakeUser(91, manage_guild=True))

        await ConfessionsCog.confessions_setup_command.callback(
            self.cog,
            ctx,
            True,
            self.guild.channels[20],
            None,
            None,
            self.guild.channels[50],
            False,
            False,
            False,
            False,
            False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertIn("@everyone", ctx.send_calls[0]["embed"].description)
        self.assertIsNone(self.cog.service.get_config(self.guild.id)["appeals_channel_id"])

    async def test_confessions_setup_rejects_appeals_channel_missing_bot_permissions(self):
        self.guild.channels[50].bot_can_embed = False
        ctx = FakeContext(guild=self.guild, author=FakeUser(92, manage_guild=True))

        await ConfessionsCog.confessions_setup_command.callback(
            self.cog,
            ctx,
            True,
            self.guild.channels[20],
            None,
            None,
            self.guild.channels[50],
            False,
            False,
            False,
            False,
            False,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertIn("Embed Links", ctx.send_calls[0]["embed"].description)
        self.assertIsNone(self.cog.service.get_config(self.guild.id)["appeals_channel_id"])

    async def test_confessions_setup_accepts_private_appeals_channel(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(93, manage_guild=True))

        await ConfessionsCog.confessions_setup_command.callback(
            self.cog,
            ctx,
            True,
            self.guild.channels[20],
            None,
            None,
            self.guild.channels[50],
            False,
            False,
            False,
            False,
            False,
        )
        await self._flush_background_tasks()

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertEqual(self.cog.service.get_config(self.guild.id)["appeals_channel_id"], 50)

    async def test_confessions_setup_successfully_syncs_runtime_surfaces_for_valid_channels(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(931, manage_guild=True))

        with mock.patch.object(
            self.cog.service,
            "sync_runtime_surfaces",
            wraps=self.cog.service.sync_runtime_surfaces,
        ) as sync_runtime:
            await ConfessionsCog.confessions_setup_command.callback(
                self.cog,
                ctx,
                True,
                self.guild.channels[20],
                self.guild.channels[40],
                self.guild.channels[30],
                self.guild.channels[50],
                False,
                False,
                False,
                False,
                False,
            )
            await self._flush_background_tasks()

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(sync_runtime.await_count, 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertIn("refreshing the live confessions panel", ctx.send_calls[0]["embed"].description.lower())
        self.assertNotIn("runtime follow-up still needs attention", ctx.send_calls[0]["embed"].description.lower())
        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["confession_channel_id"], 20)
        self.assertEqual(config["panel_channel_id"], 40)
        self.assertEqual(config["review_channel_id"], 30)
        self.assertEqual(config["appeals_channel_id"], 50)
        self.assertEqual(len(self.guild.get_channel(40).sent), 1)
        self.assertEqual(config["panel_message_id"], self.guild.get_channel(40).sent[0].id)

    async def test_confessions_setup_persists_postgres_backed_config_with_runtime_fields(self):
        guild = FakeGuild(110)
        guild.add_channel(FakeChannel(20, name="confessions"))
        guild.add_channel(FakeChannel(30, name="review"))
        guild.add_channel(FakeChannel(40, name="panel"))
        guild.add_channel(FakeChannel(50, name="appeals"))
        bot = FakeBot([guild])
        connection = _FakeConnection()
        inner_store = _PostgresConfessionsStore("postgresql://example", _privacy())
        inner_store._pool = _FakePool(connection)
        service = ConfessionsService(bot, store=_PostgresStoreFacade(inner_store))
        cog = ConfessionsCog(bot)
        bot._cog = cog
        bot.confessions_service = service
        original_service = cog.service
        ctx = FakeContext(guild=guild, author=FakeUser(933, manage_guild=True))

        try:
            cog.service = service
            with mock.patch.object(inner_store, "_connect", new=mock.AsyncMock()):
                started = await service.start()
            self.assertTrue(started)

            await ConfessionsCog.confessions_setup_command.callback(
                cog,
                ctx,
                True,
                guild.channels[20],
                guild.channels[40],
                guild.channels[30],
                guild.channels[50],
                True,
                False,
                False,
                False,
                False,
            )
            await self._flush_background_tasks(cog)

            self.assertEqual(len(ctx.defer_calls), 1)
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
            self.assertNotIn("could not save", ctx.send_calls[0]["embed"].description.lower())
            self.assertIn("refreshing the live confessions panel", ctx.send_calls[0]["embed"].description.lower())
            config = cog.service.get_config(guild.id)
            self.assertTrue(config["enabled"])
            self.assertEqual(config["confession_channel_id"], 20)
            self.assertEqual(config["panel_channel_id"], 40)
            self.assertEqual(config["review_channel_id"], 30)
            self.assertEqual(config["appeals_channel_id"], 50)
            self.assertEqual(len(guild.get_channel(40).sent), 1)
            self.assertEqual(config["panel_message_id"], guild.get_channel(40).sent[0].id)

            config_upserts = [
                args
                for statement, args in connection.execute_calls
                if "INSERT INTO confession_guild_configs" in statement
            ]
            self.assertGreaterEqual(len(config_upserts), 2)
            self.assertEqual(config_upserts[0][0], guild.id)
            self.assertEqual(config_upserts[0][3], 40)
            self.assertEqual(config_upserts[0][4], None)
            self.assertEqual(config_upserts[0][6], 50)
            self.assertEqual(config_upserts[-1][4], guild.get_channel(40).sent[0].id)
            self.assertEqual(config_upserts[-1][-1], config["strike_perm_ban_threshold"])
        finally:
            await service.close()
            cog.service = original_service
            await original_service.close()

    async def test_confessions_setup_switches_to_alternate_channels_and_replaces_stale_panel(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            review_channel_id=30,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        panel_ok, panel_message = await self.cog.service.sync_member_panel(self.guild)
        self.assertTrue(panel_ok, panel_message)
        old_panel_message = self.guild.get_channel(40).sent[0]
        alt_confession = self.guild.add_channel(FakeChannel(60, name="alt-confessions"))
        alt_panel = self.guild.add_channel(FakeChannel(70, name="alt-panel"))
        alt_review = self.guild.add_channel(FakeChannel(80, name="alt-review"))
        alt_appeals = self.guild.add_channel(FakeChannel(90, name="alt-appeals"))
        ctx = FakeContext(guild=self.guild, author=FakeUser(932, manage_guild=True))

        await ConfessionsCog.confessions_setup_command.callback(
            self.cog,
            ctx,
            True,
            alt_confession,
            alt_panel,
            alt_review,
            alt_appeals,
            False,
            False,
            False,
            False,
            False,
        )
        await self._flush_background_tasks()

        self.assertEqual(len(ctx.send_calls), 1)
        config = self.cog.service.get_config(self.guild.id)
        self.assertEqual(config["confession_channel_id"], alt_confession.id)
        self.assertEqual(config["panel_channel_id"], alt_panel.id)
        self.assertEqual(config["review_channel_id"], alt_review.id)
        self.assertEqual(config["appeals_channel_id"], alt_appeals.id)
        self.assertTrue(old_panel_message.deleted)
        self.assertEqual(len(alt_panel.sent), 1)
        self.assertEqual(config["panel_message_id"], alt_panel.sent[0].id)

    async def test_confessions_setup_returns_private_failure_when_configure_raises_after_defer(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(94, manage_guild=True))

        with mock.patch.object(self.cog.service, "configure_guild", side_effect=RuntimeError("configure boom")):
            await ConfessionsCog.confessions_setup_command.callback(
                self.cog,
                ctx,
                True,
                self.guild.channels[20],
                None,
                None,
                None,
                False,
                False,
                False,
                False,
                False,
            )

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertIn("could not finish", ctx.send_calls[0]["embed"].description.lower())

    async def test_confessions_setup_reports_runtime_sync_issues_after_saving_config(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(95, manage_guild=True))

        with (
            mock.patch.object(self.cog.service, "sync_member_panel", return_value=(False, "That panel channel is unavailable.")),
            mock.patch.object(self.cog.service, "sync_review_queue", return_value=(True, "Confession review queue refreshed.")),
        ):
            await ConfessionsCog.confessions_setup_command.callback(
                self.cog,
                ctx,
                True,
                self.guild.channels[20],
                self.guild.channels[40],
                None,
                None,
                False,
                False,
                False,
                False,
                False,
            )
            await self._flush_background_tasks()

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertIn("refreshing the live confessions panel", ctx.send_calls[0]["embed"].description.lower())
        self.assertEqual(len(ctx.interaction.followup_calls), 1)
        self.assertIn("runtime follow-up still needs attention", ctx.interaction.followup_calls[0]["kwargs"]["embed"].description.lower())
        self.assertIn("panel channel is unavailable", ctx.interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_confess_manage_appeal_report_and_about_commands_open_private_flows(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        manage_ctx = FakeContext(guild=self.guild, author=self._member(122))
        appeal_ctx = FakeContext(guild=self.guild, author=self._member(123))
        report_ctx = FakeContext(guild=self.guild, author=self._member(124))
        about_ctx = FakeContext(guild=self.guild, author=self._member(125))

        await ConfessionsCog.confess_manage_command.callback(self.cog, manage_ctx)
        await ConfessionsCog.confess_appeal_command.callback(self.cog, appeal_ctx)
        await ConfessionsCog.confess_report_command.callback(self.cog, report_ctx)
        await ConfessionsCog.confess_about_command.callback(self.cog, about_ctx)

        self.assertEqual(manage_ctx.interaction.response.modal_calls[0].title, "Manage My Confession")
        self.assertEqual(appeal_ctx.interaction.response.modal_calls[0].title, "Anonymous Appeal")
        self.assertEqual(report_ctx.interaction.response.modal_calls[0].title, "Anonymous Report")
        self.assertEqual(about_ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "How Anonymous Confessions Work")

    async def test_confess_appeal_and_report_warn_privately_when_support_channel_is_unsafe(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        self.guild.channels[50].public_view = True
        appeal_ctx = FakeContext(guild=self.guild, author=self._member(127))
        report_ctx = FakeContext(guild=self.guild, author=self._member(128))

        await ConfessionsCog.confess_appeal_command.callback(self.cog, appeal_ctx)
        await ConfessionsCog.confess_report_command.callback(self.cog, report_ctx)

        self.assertEqual(len(appeal_ctx.interaction.response.modal_calls), 0)
        self.assertEqual(appeal_ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support Unavailable")
        self.assertEqual(len(report_ctx.interaction.response.modal_calls), 0)
        self.assertEqual(report_ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support Unavailable")

    async def test_stale_private_support_view_warns_instead_of_opening_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        entry_interaction = FakeInteraction(guild=self.guild, user=self._member(129))

        await self.cog._send_support_entry(entry_interaction, default_target="CF-123456")

        support_view = entry_interaction.response.sent[0]["kwargs"]["view"]
        self.guild.channels[50].public_view = True
        stale_interaction = FakeInteraction(guild=self.guild, user=self._member(130))

        await support_view.appeal_button.callback(stale_interaction)

        self.assertEqual(len(stale_interaction.response.modal_calls), 0)
        self.assertEqual(stale_interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support Unavailable")

    async def test_expired_private_support_view_returns_private_expired_notice(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        entry_interaction = FakeInteraction(guild=self.guild, user=self._member(1291))

        await self.cog._send_support_entry(entry_interaction, default_target="CF-123456")

        support_view = entry_interaction.response.sent[0]["kwargs"]["view"]
        await support_view.on_timeout()
        stale_interaction = FakeInteraction(guild=self.guild, user=self._member(1292))

        await support_view.appeal_button.callback(stale_interaction)

        self.assertEqual(len(stale_interaction.response.modal_calls), 0)
        self.assertEqual(stale_interaction.response.sent[0]["kwargs"]["embed"].title, "Private View Expired")

    async def test_confess_command_blocks_non_allowlisted_members_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        await self.cog.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        ctx = FakeContext(guild=self.guild, author=self._member(126))

        await ConfessionsCog.confess_create_command.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.sent), 1)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confession Access")

    async def test_panel_button_failure_is_caught_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)
        interaction = FakeInteraction(guild=self.guild, user=self._member(1261), client=self.bot)

        with mock.patch.object(self.cog, "_open_confession_modal", side_effect=RuntimeError("open boom")):
            await view.send_button.callback(interaction)

        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")

    async def test_member_panel_create_button_uses_clear_label_and_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)
        interaction = FakeInteraction(guild=self.guild, user=self._member(1262), client=self.bot)

        await view.send_button.callback(interaction)

        self.assertEqual(view.send_button.label, "Create a confession")
        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_stateless_member_panel_create_button_uses_clear_label_and_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        view = StatelessConfessionMemberPanelView()
        interaction = FakeInteraction(guild=self.guild, user=self._member(1263), client=self.bot)

        await view.send_button.callback(interaction)

        self.assertEqual(view.send_button.label, "Create a confession")
        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_member_panel_no_longer_shows_generic_reply_button(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)

        custom_ids = [child.custom_id for child in view.children if getattr(child, "custom_id", None)]

        self.assertIn("bb-confession-panel:compose", custom_ids)
        self.assertNotIn("bb-confession-panel:reply", custom_ids)

    async def test_member_panel_support_button_is_disabled_when_support_channel_is_not_private(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        self.guild.channels[50].public_view = True

        view = self.cog.build_member_panel_view(guild_id=self.guild.id)

        self.assertTrue(view.support_button.disabled)

    async def test_member_result_view_support_button_is_active_only_with_private_support_channel(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, appeals_channel_id=50, review_mode=False)
        published = await self.cog.service.submit_confession(self.guild, author_id=130, content="ready", attachments=[])
        ready_view = self.cog.build_member_result_view(result=published, guild_id=self.guild.id)
        self.assertFalse(ready_view.support_button.disabled)

        self.guild.channels[50].public_view = True
        unsafe_view = self.cog.build_member_result_view(result=published, guild_id=self.guild.id)
        self.assertTrue(unsafe_view.support_button.disabled)

    async def test_role_changes_after_modal_open_are_rechecked_on_submit(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        await self.cog.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        member = self._member(129, roles=[self.allowed_role])
        interaction = FakeInteraction(guild=self.guild, user=member)
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "role changed"
        modal.link_input._value = ""
        member.roles = []

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confession Not Sent")
        self.assertIn("selected roles", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_policy_command_requires_warning_before_enabling_risky_features(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_channel_id=30, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=FakeUser(90, manage_guild=True))

        await ConfessionsCog.confessions_policy_command.callback(
            self.cog,
            ctx,
            allow_images=True,
            allow_replies=True,
            allow_self_edit=True,
        )

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confirm Risky Policy Change")
        self.assertIsNotNone(ctx.send_calls[0]["view"])
        config = self.cog.service.get_config(self.guild.id)
        self.assertFalse(config["allow_images"])
        self.assertFalse(config["allow_anonymous_replies"])
        self.assertFalse(config["allow_self_edit"])

    async def test_policy_command_warns_before_enabling_allow_all_safe_links(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_channel_id=30, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=FakeUser(91, manage_guild=True))

        await ConfessionsCog.confessions_policy_command.callback(self.cog, ctx, link_mode="allow_all_safe")

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confirm Risky Policy Change")
        self.assertIn("Shield still blocks unsafe links", json.dumps(ctx.send_calls[0]["embed"].to_dict()))
        self.assertIsNotNone(ctx.send_calls[0]["view"])
        self.assertEqual(self.cog.service.get_config(self.guild.id)["link_policy_mode"], "trusted_only")

    async def test_policy_warning_offers_review_choices_only_for_reviewable_features(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_channel_id=30, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=FakeUser(190, manage_guild=True))

        await ConfessionsCog.confessions_policy_command.callback(
            self.cog,
            ctx,
            allow_images=True,
            allow_replies=True,
        )

        labels = [child.label for child in ctx.send_calls[0]["view"].children if getattr(child, "label", None)]
        self.assertEqual(labels, ["Enable With Review", "Enable Without Review", "Cancel"])

        self_only_ctx = FakeContext(guild=self.guild, author=FakeUser(191, manage_guild=True))
        await ConfessionsCog.confessions_policy_command.callback(self.cog, self_only_ctx, allow_self_edit=True)
        self_only_labels = [child.label for child in self_only_ctx.send_calls[0]["view"].children if getattr(child, "label", None)]
        self.assertEqual(self_only_labels, ["Enable With Warning", "Cancel"])

    async def test_policy_confirm_enable_without_review_sets_review_flags_off(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_channel_id=30, review_mode=False)
        admin = FakeUser(192, manage_guild=True)
        ctx = FakeContext(guild=self.guild, author=admin)

        await ConfessionsCog.confessions_policy_command.callback(
            self.cog,
            ctx,
            allow_images=True,
            allow_replies=True,
        )

        view = ctx.send_calls[0]["view"]
        without_review = next(child for child in view.children if getattr(child, "label", None) == "Enable Without Review")
        interaction = FakeInteraction(guild=self.guild, user=admin)

        await without_review.callback(interaction)
        await self._flush_background_tasks()

        config = self.cog.service.get_config(self.guild.id)
        self.assertTrue(config["allow_images"])
        self.assertFalse(config["image_review_required"])
        self.assertTrue(config["allow_anonymous_replies"])
        self.assertFalse(config["anonymous_reply_review_required"])
        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(interaction.original_response_edits[0]["embed"].title, "Confessions Policy")

    async def test_modal_submission_text_only_defers_and_posts_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(13))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello from the modal"
        modal.link_input._value = ""

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.response.sent), 0)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertTrue(interaction.followup_calls[0]["kwargs"]["ephemeral"])
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confession Posted")

    async def test_modal_submission_with_trusted_link_defers_and_posts_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(14))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "Useful reading"
        modal.link_input._value = "https://www.google.com/search?q=babblebox"

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        embed = interaction.followup_calls[0]["kwargs"]["embed"]
        self.assertEqual(embed.title, "Confession Posted")

    async def test_modal_submission_supports_image_only_and_stays_private(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
            image_review_required=True,
        )
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(13))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = ""
        modal.link_input._value = ""
        modal.upload_input._values = [FakeAttachment("image.png")]

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertTrue(interaction.followup_calls[0]["kwargs"]["ephemeral"])
        embed = interaction.followup_calls[0]["kwargs"]["embed"]
        self.assertEqual(embed.title, "Confession Received")

    async def test_modal_submission_falls_back_to_text_when_runtime_upload_support_is_unavailable(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
        )
        self.cog.modal_file_upload_available = lambda: False
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(15))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "text only fallback"
        modal.link_input._value = ""

        self.assertIsNone(modal.upload_input)
        self.assertIn("temporarily unavailable", modal.body_input.placeholder.lower())

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confession Posted")

    async def test_modal_submission_handles_attachment_extraction_failure_privately(self):
        class BrokenUpload:
            @property
            def values(self):
                raise RuntimeError("attachment payload mismatch")

        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_images=True,
        )
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(115))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello with image"
        modal.link_input._value = ""
        modal.upload_input = BrokenUpload()

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Image Upload Unavailable")

    async def test_modal_submission_acknowledges_before_slow_service_finishes(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(16))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "slow path"
        modal.link_input._value = ""
        original_submit = self.cog.service.submit_confession

        async def slow_submit(*args, **kwargs):
            await asyncio.sleep(0.1)
            return ConfessionSubmissionResult(True, "published", "ok", confession_id="CF-SLOW000", jump_url="https://discord.com/channels/10/20/30")

        self.cog.service.submit_confession = slow_submit
        try:
            task = asyncio.create_task(modal.on_submit(interaction))
            await asyncio.sleep(0.02)
            self.assertTrue(interaction.response.is_done())
            self.assertEqual(len(interaction.response.defer_calls), 1)
            self.assertEqual(len(interaction.followup_calls), 0)
            await task
        finally:
            self.cog.service.submit_confession = original_submit

        self.assertEqual(len(interaction.followup_calls), 1)

    async def test_modal_submission_storage_unavailable_returns_private_feedback_without_deferring(self):
        self.cog.service.storage_ready = False
        self.cog.service.storage_error = "db down"
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(17))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello"
        modal.link_input._value = ""

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 0)
        self.assertEqual(len(interaction.response.sent), 1)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")

    async def test_modal_submission_operability_failure_returns_private_feedback_without_deferring(self):
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(18))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello"
        modal.link_input._value = ""

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 0)
        self.assertEqual(len(interaction.response.sent), 1)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Unavailable")

    async def test_modal_submission_uses_fallback_embed_when_result_rendering_breaks(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(19))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello"
        modal.link_input._value = ""
        original_builder = self.cog.service.build_member_result_embed

        def broken_builder(*args, **kwargs):
            raise RuntimeError("builder exploded")

        self.cog.service.build_member_result_embed = broken_builder
        try:
            await modal.on_submit(interaction)
        finally:
            self.cog.service.build_member_result_embed = original_builder

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confession Posted")

    async def test_modal_submission_uses_fallback_when_result_view_breaks(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(116))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello"
        modal.link_input._value = ""
        original_view_builder = self.cog.build_member_result_view

        def broken_view_builder(*args, **kwargs):
            raise RuntimeError("view exploded")

        self.cog.build_member_result_view = broken_view_builder
        try:
            await modal.on_submit(interaction)
        finally:
            self.cog.build_member_result_view = original_view_builder

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confession Posted")

    async def test_modal_submission_logs_safe_diagnostics_without_content_leaks(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(99991))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "super private confession body"
        modal.link_input._value = "https://private.example/path"
        original_submit = self.cog.service.submit_confession

        async def broken_submit(*args, **kwargs):
            raise RuntimeError("payload should not leak")

        self.cog.service.submit_confession = broken_submit
        try:
            with self.assertLogs("babblebox.cogs.confessions", level="ERROR") as captured:
                await modal.on_submit(interaction)
        finally:
            self.cog.service.submit_confession = original_submit

        rendered = " ".join(captured.output)
        self.assertIn("code=confession_modal_submit_failed", rendered)
        self.assertNotIn("super private confession body", rendered)
        self.assertNotIn("https://private.example/path", rendered)
        self.assertNotIn("99991", rendered)
        self.assertNotIn("payload should not leak", interaction.followup_calls[0]["kwargs"]["embed"].description)

    async def test_reply_edit_and_support_modals_defer_and_send_followups(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            appeals_channel_id=50,
            review_mode=True,
            allow_anonymous_replies=True,
            allow_self_edit=True,
        )
        pending = await self.cog.service.submit_confession(self.guild, author_id=21, content="pending edit", attachments=[])
        self.assertEqual(pending.state, "queued")

        reply_interaction = FakeInteraction(guild=self.guild, user=FakeUser(22))
        reply_modal = ReplyComposerModal(self.cog, guild_id=self.guild.id, default_target=pending.confession_id)
        reply_modal.target_input._value = pending.confession_id
        reply_modal.body_input._value = "reply body"
        await reply_modal.on_submit(reply_interaction)
        self.assertEqual(len(reply_interaction.response.defer_calls), 1)
        self.assertEqual(len(reply_interaction.followup_calls), 1)

        edit_interaction = FakeInteraction(guild=self.guild, user=FakeUser(21))
        submission = await self.cog.service.store.fetch_submission_by_confession_id(self.guild.id, pending.confession_id)
        edit_modal = EditConfessionModal(self.cog, guild_id=self.guild.id, target_id=pending.confession_id, submission=submission)
        edit_modal.body_input._value = "updated pending edit"
        if edit_modal.link_input is not None:
            edit_modal.link_input._value = ""
        await edit_modal.on_submit(edit_interaction)
        self.assertEqual(len(edit_interaction.response.defer_calls), 1)
        self.assertEqual(len(edit_interaction.followup_calls), 1)

        appeal_interaction = FakeInteraction(guild=self.guild, user=FakeUser(21))
        appeal_modal = AppealModal(self.cog, default_target=pending.confession_id)
        appeal_modal.target_input._value = pending.confession_id
        appeal_modal.details_input._value = "Please review this restriction."
        await appeal_modal.on_submit(appeal_interaction)
        self.assertEqual(len(appeal_interaction.response.defer_calls), 1)
        self.assertEqual(len(appeal_interaction.followup_calls), 1)

        report_interaction = FakeInteraction(guild=self.guild, user=FakeUser(23))
        report_modal = ReportModal(self.cog, default_target=pending.confession_id)
        report_modal.target_input._value = pending.confession_id
        report_modal.details_input._value = "This confession needs review."
        await report_modal.on_submit(report_interaction)
        self.assertEqual(len(report_interaction.response.defer_calls), 1)
        self.assertEqual(len(report_interaction.followup_calls), 1)

    async def test_stale_support_view_fails_closed_when_channel_becomes_public(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            appeals_channel_id=50,
            review_mode=False,
        )
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)
        self.guild.channels[50].public_view = True
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(24))

        await view.support_button.callback(interaction)

        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support Unavailable")

    async def test_status_command_with_target_returns_anonymous_detail_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        published = await self.cog.service.submit_confession(self.guild, author_id=31, content="status me", attachments=[])
        ctx = FakeContext(guild=self.guild, author=FakeUser(1, manage_guild=True))

        await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, published.confession_id)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        rendered = json.dumps(ctx.send_calls[0]["embed"].to_dict())
        self.assertIn(published.confession_id, rendered)
        self.assertNotIn("<@31>", rendered)
        self.assertNotIn("author_user_id", rendered)

    async def test_panel_command_publishes_member_panel_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, panel_channel_id=40)
        ctx = FakeContext(guild=self.guild, author=FakeUser(2, manage_guild=True))

        await ConfessionsCog.confessions_panel_command.callback(self.cog, ctx, self.guild.get_channel(40))

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertEqual(len(self.guild.get_channel(40).sent), 1)
        self.assertEqual(self.guild.get_channel(40).sent[0].embed.title, "Anonymous Confessions")

    async def test_panel_command_returns_private_failure_when_publish_raises_after_defer(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(6, manage_guild=True))

        with mock.patch.object(self.cog.service, "sync_member_panel", side_effect=RuntimeError("publish boom")):
            await ConfessionsCog.confessions_panel_command.callback(self.cog, ctx, self.guild.get_channel(40))

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Panel")
        self.assertIn("could not publish", ctx.send_calls[0]["embed"].description.lower())

    async def test_role_allowlist_command_reports_background_refresh_failure_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=FakeUser(7, manage_guild=True))

        with mock.patch.object(self.cog.service, "sync_runtime_surfaces", side_effect=RuntimeError("runtime boom")):
            await ConfessionsCog.confessions_role_allowlist_command.callback(self.cog, ctx, self.allowed_role, "on")
            await self._flush_background_tasks()

        self.assertEqual(len(ctx.defer_calls), 1)
        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Role Eligibility")
        self.assertIn("refreshing the live confessions panel", ctx.send_calls[0]["embed"].description.lower())
        self.assertEqual(len(ctx.interaction.followup_calls), 1)
        self.assertIn("could not finish refreshing live confessions surfaces", ctx.interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_role_blacklist_command_returns_before_slow_runtime_sync_finishes(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        ctx = FakeContext(guild=self.guild, author=FakeUser(70, manage_guild=True))
        started = asyncio.Event()
        release = asyncio.Event()
        original_sync = self.cog.service.sync_runtime_surfaces

        async def slow_sync(*args, **kwargs):
            started.set()
            await release.wait()
            return await original_sync(*args, **kwargs)

        with mock.patch.object(self.cog.service, "sync_runtime_surfaces", side_effect=slow_sync):
            task = asyncio.create_task(
                ConfessionsCog.confessions_role_blacklist_command.callback(self.cog, ctx, self.blocked_role, "on")
            )
            await started.wait()
            await asyncio.sleep(0)
            self.assertTrue(task.done())
            self.assertEqual(len(ctx.send_calls), 1)
            self.assertIn("refreshing the live confessions panel", ctx.send_calls[0]["embed"].description.lower())
            self.assertEqual(len(ctx.interaction.followup_calls), 0)
            release.set()
            await task
            await self._flush_background_tasks()

    async def test_live_admin_panel_refresh_still_works_and_sends_private_note(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(8, manage_guild=True))

        await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, None)

        view = ctx.send_calls[0]["view"]
        interaction = FakeInteraction(guild=self.guild, user=ctx.author)
        await view.refresh_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.original_response_edits), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confessions Panel")

    async def test_admin_panel_toggle_success_updates_runtime_and_replies_privately(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=False,
            confession_channel_id=20,
            panel_channel_id=40,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        admin = FakeUser(81, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        await view.toggle_button.callback(interaction)
        await self._flush_background_tasks()

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.original_response_edits), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertTrue(self.cog.service.get_config(self.guild.id)["enabled"])
        self.assertIn("enabled", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())
        self.assertNotIn("could not", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_publish_success_updates_panel_record_and_replies_privately(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        admin = FakeUser(82, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        await view.publish_panel_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.original_response_edits), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(len(self.guild.get_channel(40).sent), 1)
        self.assertEqual(
            self.cog.service.get_config(self.guild.id)["panel_message_id"],
            self.guild.get_channel(40).sent[0].id,
        )
        self.assertIn("panel is live", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_refresh_queue_success_replies_privately(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=True,
        )
        self.assertTrue(ok, message)
        queued = await self.cog.service.submit_confession(self.guild, author_id=83, content="queue me", attachments=[])
        self.assertEqual(queued.state, "queued")
        admin = FakeUser(84, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        await view.refresh_queue_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.original_response_edits), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(len(self.guild.get_channel(30).sent), 1)
        self.assertIn("review queue is live", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_toggle_failure_is_handled_privately(self):
        admin = FakeUser(10, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        with mock.patch.object(self.cog.service, "configure_guild", side_effect=RuntimeError("toggle boom")):
            await view.toggle_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confessions Panel")
        self.assertIn("could not update", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_publish_failure_is_handled_privately(self):
        admin = FakeUser(11, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        with mock.patch.object(self.cog.service, "sync_member_panel", side_effect=RuntimeError("publish boom")):
            await view.publish_panel_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertIn("could not publish", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_refresh_queue_failure_is_handled_privately(self):
        admin = FakeUser(12, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        with mock.patch.object(self.cog.service, "sync_review_queue", side_effect=RuntimeError("queue boom")):
            await view.refresh_queue_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertIn("could not refresh the confession review queue", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_admin_panel_section_switch_failure_is_handled_privately(self):
        admin = FakeUser(13, manage_guild=True)
        self.guild.add_member(admin)
        view = self.cog.build_admin_panel_view(guild_id=self.guild.id, author_id=admin.id)
        interaction = FakeInteraction(guild=self.guild, user=admin)

        with mock.patch.object(self.cog.service, "build_dashboard_embed", side_effect=RuntimeError("render boom")):
            await view.policy_button.callback(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(len(interaction.followup_calls), 1)
        self.assertIn("could not refresh that private confessions panel", interaction.followup_calls[0]["kwargs"]["embed"].description.lower())

    async def test_stateless_admin_panel_fallback_warns_privately_when_panel_is_expired(self):
        interaction = FakeInteraction(guild=self.guild, user=self._member(300), client=self.bot)

        await StatelessConfessionsAdminPanelView().refresh_button.callback(interaction)

        self.assertEqual(len(interaction.response.sent), 1)
        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Confessions Panel Expired")
        self.assertIn("run `/confessions` again", interaction.response.sent[0]["kwargs"]["embed"].description.lower())

    async def test_confessions_role_commands_update_status_and_reject_everyone(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        status_ctx = FakeContext(guild=self.guild, author=FakeUser(2, manage_guild=True))
        allow_ctx = FakeContext(guild=self.guild, author=FakeUser(3, manage_guild=True))
        reject_ctx = FakeContext(guild=self.guild, author=FakeUser(4, manage_guild=True))
        reset_ctx = FakeContext(guild=self.guild, author=FakeUser(5, manage_guild=True))

        await ConfessionsCog.confessions_role_group.callback(self.cog, status_ctx)
        await ConfessionsCog.confessions_role_allowlist_command.callback(self.cog, allow_ctx, self.allowed_role, "on")
        allow_config = self.cog.service.get_config(self.guild.id)
        await ConfessionsCog.confessions_role_blacklist_command.callback(self.cog, reject_ctx, self.guild.default_role, "on")
        await ConfessionsCog.confessions_role_reset_command.callback(self.cog, reset_ctx, "allowlist")
        await self._flush_background_tasks()

        self.assertEqual(status_ctx.send_calls[0]["embed"].title, "Confessions Role Eligibility")
        self.assertIn(self.allowed_role.id, allow_config["allowed_role_ids"])
        self.assertIn("does not allow `@everyone`", reject_ctx.send_calls[0]["embed"].description)
        self.assertEqual(self.cog.service.get_config(self.guild.id)["allowed_role_ids"], [])

    async def test_published_confession_reply_button_opens_modal_without_leaking_ids_and_preserves_review_flow(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
            anonymous_reply_review_required=True,
        )
        published = await self.cog.service.submit_confession(self.guild, author_id=31, content="reply to me", attachments=[])
        live_message = self.guild.get_channel(20).sent[0]
        custom_ids = [child.custom_id for child in live_message.view.children if getattr(child, "custom_id", None)]

        self.assertEqual(custom_ids, ["bb-confession-post:compose", "bb-confession-post:reply"])
        self.assertTrue(all(published.confession_id not in value for value in custom_ids))

        member = self._member(127)
        open_interaction = FakeInteraction(guild=self.guild, user=member, message=live_message)
        await live_message.view.reply_button.callback(open_interaction)
        self.assertEqual(open_interaction.response.modal_calls[0].title, "Anonymous Reply")

        modal = open_interaction.response.modal_calls[0]
        modal.target_input._value = published.confession_id
        modal.body_input._value = "reply body"
        submit_interaction = FakeInteraction(guild=self.guild, user=member)
        await modal.on_submit(submit_interaction)
        self.assertEqual(submit_interaction.followup_calls[0]["kwargs"]["embed"].title, "Reply Received")

    async def test_latest_public_create_button_opens_private_confession_modal(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        await self.cog.service.submit_confession(self.guild, author_id=311, content="latest launcher", attachments=[])
        live_message = self.guild.get_channel(20).sent[0]

        interaction = FakeInteraction(guild=self.guild, user=self._member(130), message=live_message)
        await live_message.view.compose_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_stale_public_reply_button_fails_closed_after_replies_are_disabled(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
        )
        await self.cog.service.submit_confession(self.guild, author_id=32, content="stale button", attachments=[])
        live_message = self.guild.get_channel(20).sent[0]
        stale_view = live_message.view
        ok, message = await self.cog.service.configure_guild(self.guild.id, allow_anonymous_replies=False)
        self.assertTrue(ok, message)

        interaction = FakeInteraction(guild=self.guild, user=self._member(128), message=live_message)
        await stale_view.reply_button.callback(interaction)

        self.assertEqual(interaction.response.sent[0]["kwargs"]["embed"].title, "Replies Are Off")

    async def test_stateless_public_reply_view_uses_live_message_lookup(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
        )
        published = await self.cog.service.submit_confession(self.guild, author_id=33, content="reply fallback", attachments=[])
        self.assertEqual(published.state, "published")
        live_message = self.guild.get_channel(20).sent[0]
        interaction = FakeInteraction(guild=self.guild, user=self._member(129), message=live_message, client=self.bot)

        await StatelessPublishedConfessionReplyView().reply_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Reply")

    async def test_stateless_public_create_view_uses_live_runtime_lookup(self):
        await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
        )
        await self.cog.service.submit_confession(self.guild, author_id=34, content="compose fallback", attachments=[])
        live_message = self.guild.get_channel(20).sent[0]
        interaction = FakeInteraction(guild=self.guild, user=self._member(131), message=live_message, client=self.bot)

        await StatelessPublishedConfessionReplyView().compose_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_review_view_custom_ids_are_case_only(self):
        view = self.cog.build_review_view(case_id="CS-AAAA1111", version=7)
        custom_ids = [child.custom_id for child in view.children if getattr(child, "custom_id", None)]

        self.assertTrue(all("CS-AAAA1111" in value for value in custom_ids))
        self.assertTrue(all(":7" in value for value in custom_ids))
        self.assertTrue(all("123456789" not in value for value in custom_ids))

    async def test_resume_member_panels_and_review_queues_restore_persistent_views(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            review_channel_id=30,
            review_mode=True,
        )
        self.assertTrue(ok, message)
        panel_ok, panel_message = await self.cog.service.sync_member_panel(self.guild)
        self.assertTrue(panel_ok, panel_message)
        queued = await self.cog.service.submit_confession(self.guild, author_id=42, content="queued for restore", attachments=[])
        self.assertEqual(queued.state, "queued")
        self.bot.views.clear()

        await self.cog.service.resume_member_panels()
        await self.cog.service.resume_review_queues()

        self.assertEqual(len(self.bot.views), 2)
        custom_ids = []
        for view, message_id in self.bot.views:
            self.assertIsNotNone(message_id)
            custom_ids.extend([child.custom_id for child in view.children if getattr(child, "custom_id", None)])
        self.assertTrue(any("bb-confession-panel:compose" == value for value in custom_ids))
        self.assertTrue(any(queued.case_id in value for value in custom_ids))

    async def test_resume_member_panels_repairs_missing_stored_message_id(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        self.assertIsNone(self.cog.service.get_config(self.guild.id)["panel_message_id"])
        self.bot.views.clear()

        await self.cog.service.resume_member_panels()

        self.assertEqual(len(self.guild.get_channel(40).sent), 1)
        stored_message_id = self.cog.service.get_config(self.guild.id)["panel_message_id"]
        self.assertEqual(len(self.bot.views), 1)
        self.assertEqual(self.bot.views[0][1], stored_message_id)
        self.assertEqual(stored_message_id, self.guild.get_channel(40).sent[0].id)

    async def test_resume_member_panels_repairs_stale_tracked_message(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            panel_message_id=999999,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        self.bot.views.clear()

        await self.cog.service.resume_member_panels()

        self.assertEqual(len(self.guild.get_channel(40).sent), 1)
        self.assertNotEqual(self.cog.service.get_config(self.guild.id)["panel_message_id"], 999999)

    async def test_resume_member_panels_prunes_recent_orphan_panel_duplicates_when_republishing(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            panel_message_id=999999,
            review_mode=False,
        )
        self.assertTrue(ok, message)
        panel_channel = self.guild.get_channel(40)
        orphan_one = await panel_channel.send(
            embed=self.cog.service.build_member_panel_embed(self.guild),
            view=self.cog.build_member_panel_view(guild_id=self.guild.id),
        )
        orphan_two = await panel_channel.send(
            embed=self.cog.service.build_member_panel_embed(self.guild),
            view=self.cog.build_member_panel_view(guild_id=self.guild.id),
        )
        self.bot.views.clear()

        await self.cog.service.resume_member_panels()

        current_message_id = self.cog.service.get_config(self.guild.id)["panel_message_id"]
        self.assertEqual(current_message_id, panel_channel.sent[-1].id)
        self.assertTrue(orphan_one.deleted)
        self.assertTrue(orphan_two.deleted)

    async def test_on_ready_restores_runtime_surfaces_once_after_guild_cache_is_available(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            panel_channel_id=40,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
        )
        self.assertTrue(ok, message)
        panel_ok, panel_message = await self.cog.service.sync_member_panel(self.guild)
        self.assertTrue(panel_ok, panel_message)
        published = await self.cog.service.submit_confession(self.guild, author_id=44, content="restore published", attachments=[])
        self.assertEqual(published.state, "published")
        ok, message = await self.cog.service.configure_guild(self.guild.id, review_mode=True)
        self.assertTrue(ok, message)
        queued = await self.cog.service.submit_confession(self.guild, author_id=45, content="restore review queue", attachments=[])
        self.assertEqual(queued.state, "queued")
        self.bot.views.clear()
        self.bot._ready = True
        self.cog._persistent_views_restored = False

        await self.cog.on_ready()

        self.assertEqual(len(self.bot.views), 3)
        restored_ids = {message_id for _, message_id in self.bot.views}
        self.assertEqual(
            restored_ids,
            {
                self.guild.get_channel(40).sent[0].id,
                self.guild.get_channel(20).sent[0].id,
                self.guild.get_channel(30).sent[0].id,
            },
        )

        await self.cog.on_ready()

        self.assertEqual(len(self.bot.views), 3)

    async def test_restore_runtime_surfaces_isolated_by_surface_and_retries_only_failed_surface(self):
        member_panels = mock.AsyncMock(side_effect=RuntimeError("panel restore boom"))
        public_views = mock.AsyncMock()
        review_queues = mock.AsyncMock()
        self.cog.service.resume_member_panels = member_panels
        self.cog.service.resume_public_confession_views = public_views
        self.cog.service.resume_review_queues = review_queues

        await self.cog._restore_runtime_surfaces_once()

        member_panels.assert_awaited_once()
        public_views.assert_awaited_once()
        review_queues.assert_awaited_once()
        self.assertFalse(self.cog._persistent_views_restored)
        self.assertFalse(self.cog._persistent_surface_restore_status["member_panels"])
        self.assertTrue(self.cog._persistent_surface_restore_status["public_views"])
        self.assertTrue(self.cog._persistent_surface_restore_status["review_queues"])

        self.cog.service.resume_member_panels = mock.AsyncMock()
        self.cog.service.resume_public_confession_views = mock.AsyncMock()
        self.cog.service.resume_review_queues = mock.AsyncMock()

        await self.cog._restore_runtime_surfaces_once()

        self.cog.service.resume_member_panels.assert_awaited_once()
        self.cog.service.resume_public_confession_views.assert_not_awaited()
        self.cog.service.resume_review_queues.assert_not_awaited()
        self.assertTrue(self.cog._persistent_views_restored)

    async def test_resume_public_confession_views_restores_persistent_reply_buttons(self):
        ok, message = await self.cog.service.configure_guild(
            self.guild.id,
            enabled=True,
            confession_channel_id=20,
            review_channel_id=30,
            review_mode=False,
            allow_anonymous_replies=True,
        )
        self.assertTrue(ok, message)
        published = await self.cog.service.submit_confession(self.guild, author_id=43, content="restore live reply", attachments=[])
        self.assertEqual(published.state, "published")
        live_message = self.guild.get_channel(20).sent[0]
        self.bot.views.clear()

        await self.cog.service.resume_public_confession_views()

        self.assertEqual(len(self.bot.views), 1)
        view, message_id = self.bot.views[0]
        self.assertEqual(message_id, live_message.id)
        custom_ids = [child.custom_id for child in view.children if getattr(child, "custom_id", None)]
        self.assertEqual(custom_ids, ["bb-confession-post:compose", "bb-confession-post:reply"])

    async def test_confession_related_modals_use_4000_character_limit_but_support_details_stay_1800(self):
        composer = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        reply = ReplyComposerModal(self.cog, guild_id=self.guild.id, default_target="CF-AAAA1111")
        owner_reply = OwnerReplyComposerModal(self.cog, guild_id=self.guild.id, opportunity_id="opp-123")
        edit = EditConfessionModal(
            self.cog,
            guild_id=self.guild.id,
            target_id="CF-AAAA1111",
            submission={
                "submission_kind": "confession",
                "content_body": "draft",
                "shared_link_url": None,
            },
        )
        appeal = AppealModal(self.cog)
        report = ReportModal(self.cog)

        self.assertEqual(composer.body_input.max_length, 4000)
        self.assertEqual(reply.body_input.max_length, 4000)
        self.assertEqual(owner_reply.body_input.max_length, 4000)
        self.assertEqual(edit.body_input.max_length, 4000)
        self.assertEqual(appeal.details_input.max_length, 1800)
        self.assertEqual(report.details_input.max_length, 1800)

    async def test_modal_and_review_callbacks_return_generic_errors(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_channel_id=30, review_mode=True)

        original_submit = self.cog.service.submit_confession
        original_handle = self.cog.service.handle_case_action

        async def broken_submit(*args, **kwargs):
            raise RuntimeError("payload should not leak")

        async def broken_handle(*args, **kwargs):
            raise RuntimeError("queue payload should not leak")

        self.cog.service.submit_confession = broken_submit
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(13))
        modal = ConfessionComposerModal(self.cog, guild_id=self.guild.id)
        modal.body_input._value = "hello"
        modal.link_input._value = ""
        if modal.upload_input is not None:
            modal.upload_input._values = []

        await modal.on_submit(interaction)

        self.assertEqual(len(interaction.response.defer_calls), 1)
        self.assertEqual(interaction.followup_calls[0]["kwargs"]["embed"].title, "Confessions Unavailable")

        self.cog.service.submit_confession = original_submit
        queued = await self.cog.service.submit_confession(self.guild, author_id=42, content="needs review", attachments=[])
        self.cog.service.handle_case_action = broken_handle
        review_view = self.cog.build_review_view(case_id=queued.case_id, version=1)
        review_interaction = FakeInteraction(guild=self.guild, user=FakeUser(5, manage_guild=True))

        await review_view.children[0].callback(review_interaction)

        self.assertEqual(review_interaction.response.sent[0]["kwargs"]["embed"].title, "Review Action Failed")
        self.cog.service.handle_case_action = original_handle

    async def test_moderate_command_pause_7d_maps_to_temp_ban(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        published = await self.cog.service.submit_confession(self.guild, author_id=77, content="moderate me", attachments=[])
        ctx = FakeContext(guild=self.guild, author=FakeUser(5, manage_guild=True))

        await ConfessionsCog.confessions_moderate_command.callback(self.cog, ctx, published.confession_id, "pause_7d", False)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        state_link = await self.cog.service.store.fetch_author_link((await self.cog.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id))["submission_id"])
        state = await self.cog.service.store.fetch_enforcement_state(self.guild.id, state_link["author_user_id"])
        self.assertEqual(state["active_restriction"], "temp_ban")

    async def test_moderate_command_pause_24h_maps_to_suspend(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        published = await self.cog.service.submit_confession(self.guild, author_id=78, content="pause me", attachments=[])
        ctx = FakeContext(guild=self.guild, author=FakeUser(6, manage_guild=True))

        await ConfessionsCog.confessions_moderate_command.callback(self.cog, ctx, published.confession_id, "pause_24h", False)

        state_link = await self.cog.service.store.fetch_author_link(
            (await self.cog.service.store.fetch_submission_by_confession_id(self.guild.id, published.confession_id))["submission_id"]
        )
        state = await self.cog.service.store.fetch_enforcement_state(self.guild.id, state_link["author_user_id"])
        self.assertEqual(state["active_restriction"], "suspended")


class ConfessionsApiConsistencyTests(unittest.TestCase):
    @staticmethod
    def _collect_self_attribute_calls(source_path: Path, attribute_name: str) -> set[str]:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        calls: set[str] = set()

        class _Visitor(ast.NodeVisitor):
            def visit_Call(self, node: ast.Call):
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and isinstance(func.value, ast.Attribute)
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == "self"
                    and func.value.attr == attribute_name
                ):
                    calls.add(func.attr)
                self.generic_visit(node)

        _Visitor().visit(tree)
        return calls

    def test_confessions_cog_only_calls_public_service_methods_that_exist(self):
        repo_root = Path(__file__).resolve().parents[1]
        called_methods = self._collect_self_attribute_calls(
            repo_root / "babblebox" / "cogs" / "confessions.py",
            "service",
        )

        private_calls = sorted(name for name in called_methods if name.startswith("_"))
        self.assertEqual(private_calls, [])

        missing = sorted(name for name in called_methods if not callable(getattr(ConfessionsService, name, None)))
        self.assertEqual(missing, [])

    def test_confessions_service_only_calls_store_methods_that_exist(self):
        repo_root = Path(__file__).resolve().parents[1]
        called_methods = self._collect_self_attribute_calls(
            repo_root / "babblebox" / "confessions_service.py",
            "store",
        )

        missing = sorted(name for name in called_methods if not callable(getattr(ConfessionsStore, name, None)))
        self.assertEqual(missing, [])
