from __future__ import annotations

import asyncio
import json
import types
import unittest
from unittest import mock

from babblebox.cogs.confessions import (
    AppealModal,
    ConfessionComposerModal,
    ConfessionsCog,
    EditConfessionModal,
    ReplyComposerModal,
    ReportModal,
    StatelessConfessionMemberPanelView,
    StatelessPublishedConfessionReplyView,
)
from babblebox.confessions_service import CASE_ID_PREFIX, CONFESSION_ID_PREFIX, ConfessionSubmissionResult, ConfessionsService
from babblebox.confessions_store import ConfessionsStore


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


class FakeMessage:
    _next_id = 1000

    def __init__(self, *, content=None, embed=None, embeds=None, view=None, ephemeral=None, allowed_mentions=None):
        self.id = FakeMessage._next_id
        FakeMessage._next_id += 1
        self.content = content
        self.embed = embed or (embeds[0] if embeds else None)
        self.embeds = list(embeds or ([embed] if embed is not None else []))
        self.view = view
        self.ephemeral = ephemeral
        self.allowed_mentions = allowed_mentions
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
    ):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self.public_view = public_view
        self.bot_can_view = bot_can_view
        self.bot_can_send = bot_can_send
        self.bot_can_embed = bot_can_embed
        self.sent: list[FakeMessage] = []
        self._messages: dict[int, FakeMessage] = {}

    async def send(self, content=None, embed=None, embeds=None, view=None, ephemeral=None, allowed_mentions=None, **kwargs):
        message = FakeMessage(
            content=content,
            embed=embed,
            embeds=embeds,
            view=view,
            ephemeral=ephemeral,
            allowed_mentions=allowed_mentions,
        )
        self.sent.append(message)
        self._messages[message.id] = message
        return message

    async def fetch_message(self, message_id: int):
        message = self._messages.get(message_id)
        if message is None:
            raise Exception("missing")
        return message

    def permissions_for(self, target):
        is_default = getattr(target, "is_default", None)
        if callable(is_default) and is_default():
            return FakeChannelPermissions(view_channel=self.public_view)
        if getattr(target, "id", None) == 999:
            return FakeChannelPermissions(
                view_channel=self.bot_can_view,
                send_messages=self.bot_can_send,
                embed_links=self.bot_can_embed,
            )
        return FakeChannelPermissions(view_channel=True, send_messages=True, embed_links=True)


class FakeGuild:
    def __init__(self, guild_id: int):
        self.id = guild_id
        self.name = f"Guild {guild_id}"
        self.channels: dict[int, FakeChannel] = {}
        self.roles: dict[int, FakeRole] = {}
        self.members: dict[int, FakeUser] = {}
        self.default_role = FakeRole(guild_id, name="@everyone", guild=self)
        self.me = types.SimpleNamespace(id=999)
        self.roles[self.default_role.id] = self.default_role

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)

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


class FakeResponse:
    def __init__(self):
        self._done = False
        self.defer_calls = []
        self.sent = []
        self.edits = []
        self.modal_calls = []

    def is_done(self):
        return self._done

    async def defer(self, *, ephemeral=False, thinking=False):
        self._done = True
        self.defer_calls.append({"ephemeral": ephemeral, "thinking": thinking})

    async def send_message(self, *args, **kwargs):
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
        self._done = True
        self.edits.append(kwargs)

    async def send_modal(self, modal):
        self._done = True
        self.modal_calls.append(modal)


class FakeInteraction:
    def __init__(self, *, guild=None, user=None, message=None, client=None):
        self.guild = guild
        self.user = user
        self.message = message
        self.client = client
        self.response = FakeResponse()
        self.followup = types.SimpleNamespace(send=self._followup_send)
        self.followup_calls = []

    async def _followup_send(self, *args, **kwargs):
        self.followup_calls.append({"args": args, "kwargs": kwargs})
        return FakeMessage(
            content=kwargs.get("content"),
            embed=kwargs.get("embed"),
            embeds=kwargs.get("embeds"),
            view=kwargs.get("view"),
            ephemeral=kwargs.get("ephemeral"),
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
            channel = guild.get_channel(channel_id)
            if channel is not None:
                return channel
        return None

    def get_guild(self, guild_id: int):
        return self._guilds.get(guild_id)

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

    def build_public_confession_view(self, *, guild_id: int):
        return types.SimpleNamespace(
            guild_id=guild_id,
            children=[types.SimpleNamespace(custom_id="bb-confession-post:reply")],
        )


class ConfessionsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.confession_channel = FakeChannel(20, name="confessions")
        self.review_channel = FakeChannel(30, name="confession-review")
        self.panel_channel = FakeChannel(40, name="confession-panel")
        self.appeals_channel = FakeChannel(50, name="confession-appeals")
        self.allowed_role = self.guild.add_role(FakeRole(501, name="Allowed"))
        self.blocked_role = self.guild.add_role(FakeRole(502, name="Blocked"))
        self.guild.channels[self.confession_channel.id] = self.confession_channel
        self.guild.channels[self.review_channel.id] = self.review_channel
        self.guild.channels[self.panel_channel.id] = self.panel_channel
        self.guild.channels[self.appeals_channel.id] = self.appeals_channel
        self.other_guild = FakeGuild(11)
        self.other_confession_channel = FakeChannel(21, name="other-confessions")
        self.other_allowed_role = self.other_guild.add_role(FakeRole(601, name="Other Allowed"))
        self.other_guild.channels[self.other_confession_channel.id] = self.other_confession_channel
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
        allow_replies: bool | None = None,
        allow_self_edit: bool | None = None,
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
            allow_anonymous_replies=allow_replies,
            allow_self_edit=allow_self_edit,
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
        self.assertEqual(submission["status"], "published")
        self.assertIsNone(submission["content_body"])
        self.assertIsNone(submission["staff_preview"])
        self.assertIsNone(submission["shared_link_url"])
        self.assertIsNotNone(submission["content_fingerprint"])
        self.assertIsNone(submission["similarity_key"])
        self.assertIsNotNone(submission["fuzzy_signature"])
        self.assertEqual(submission["attachment_meta"], [])
        self.assertEqual(author_link["author_user_id"], 123456789)

    async def test_text_link_and_images_queue_for_review_and_keep_private_media_out_of_staff_storage(self):
        await self._configure(review_channel=True, allow_images=True)

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
        self.assertFalse(ok)
        self.assertIn("review channel", message.lower())

        await self.service.configure_guild(self.guild.id, review_channel_id=self.review_channel.id)
        ok, message = await self.service.configure_guild(self.guild.id, allow_images=True, max_images=2)
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

        ok, message = await self.service.configure_guild(self.guild.id, allow_anonymous_replies=True)
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
        stored = await self.service.store.fetch_submission_by_confession_id(self.guild.id, reply.confession_id)
        self.assertEqual(stored["submission_kind"], "reply")
        self.assertEqual(stored["parent_confession_id"], published.confession_id)

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
        await self._configure(review_channel=True, allow_replies=True)
        published = await self.service.submit_confession(self.guild, author_id=918, content="base confession", attachments=[])

        self.assertIsNotNone(self.confession_channel.sent[0].view)
        self.assertEqual(self.confession_channel.sent[0].view.children[0].custom_id, "bb-confession-post:reply")

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
        self.assertIsNone(self.confession_channel.sent[1].view)

    async def test_published_confessions_do_not_show_reply_button_when_replies_disabled(self):
        await self._configure()
        published = await self.service.submit_confession(self.guild, author_id=920, content="no reply button", attachments=[])

        self.assertEqual(published.state, "published")
        self.assertIsNone(self.confession_channel.sent[0].view)

    async def test_sync_published_confession_views_updates_existing_posts_when_reply_policy_changes(self):
        await self._configure(review_channel=True, allow_replies=True)
        published = await self.service.submit_confession(self.guild, author_id=921, content="toggle me", attachments=[])
        self.assertEqual(published.state, "published")
        live_message = self.confession_channel.sent[0]
        self.assertIsNotNone(live_message.view)

        ok, message = await self.service.configure_guild(self.guild.id, allow_anonymous_replies=False)
        self.assertTrue(ok, message)
        await self.service.sync_published_confession_views(self.guild)
        self.assertIsNone(live_message.view)

        ok, message = await self.service.configure_guild(self.guild.id, allow_anonymous_replies=True)
        self.assertTrue(ok, message)
        await self.service.sync_published_confession_views(self.guild)
        self.assertIsNotNone(live_message.view)
        self.assertEqual(live_message.view.children[0].custom_id, "bb-confession-post:reply")

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


class ConfessionsCogTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.allowed_role = self.guild.add_role(FakeRole(701, name="Allowed"))
        self.blocked_role = self.guild.add_role(FakeRole(702, name="Blocked"))
        self.guild.channels[20] = FakeChannel(20, name="confessions")
        self.guild.channels[30] = FakeChannel(30, name="review")
        self.guild.channels[40] = FakeChannel(40, name="panel")
        self.guild.channels[50] = FakeChannel(50, name="appeals")
        self.bot = FakeBot([self.guild])
        self.cog = ConfessionsCog(self.bot)
        self.bot._cog = self.cog
        original = self.cog.service
        store = ConfessionsStore(backend="memory")
        self.cog.service = ConfessionsService(self.bot, store=store)
        await self.cog.service.start()
        self.bot.confessions_service = self.cog.service
        self._original_service = original

    async def asyncTearDown(self):
        await self.cog.service.close()
        await self._original_service.close()

    def _member(self, user_id: int, *, roles: list[FakeRole] | None = None, manage_guild: bool = False) -> FakeUser:
        member = FakeUser(user_id, roles=roles, manage_guild=manage_guild)
        self.guild.add_member(member)
        return member

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

    async def test_slash_confess_opens_modal_with_no_arguments(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        member = self._member(11)
        ctx = FakeContext(guild=self.guild, author=member)

        await ConfessionsCog.confess_group.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.modal_calls), 1)
        self.assertEqual(ctx.interaction.response.modal_calls[0].title, "Anonymous Confession")

    def test_confess_create_slash_fallback_is_registered(self):
        command_names = {command.name for command in self.cog.confess_group.app_command.commands}

        self.assertEqual(command_names, {"about", "appeal", "create", "manage", "report"})

    async def test_cog_load_registers_global_fallback_views(self):
        self.bot.views.clear()
        self.bot._ready = False

        with mock.patch.object(self.cog.service, "start", new=mock.AsyncMock(return_value=True)):
            await self.cog.cog_load()

        self.assertIs(self.bot.confessions_service, self.cog.service)
        self.assertEqual(len(self.bot.views), 2)
        self.assertEqual([message_id for _, message_id in self.bot.views], [None, None])
        self.assertCountEqual(
            [type(view) for view, _ in self.bot.views],
            [StatelessConfessionMemberPanelView, StatelessPublishedConfessionReplyView],
        )

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

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Setup")
        self.assertEqual(self.cog.service.get_config(self.guild.id)["appeals_channel_id"], 50)

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

    async def test_confess_command_blocks_non_allowlisted_members_privately(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        await self.cog.service.update_role_policy(self.guild.id, bucket="allow", role_id=self.allowed_role.id, enabled=True)
        ctx = FakeContext(guild=self.guild, author=self._member(126))

        await ConfessionsCog.confess_group.callback(self.cog, ctx)

        self.assertEqual(len(ctx.interaction.response.sent), 1)
        self.assertEqual(ctx.interaction.response.sent[0]["kwargs"]["embed"].title, "Confession Access")

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
            with mock.patch("builtins.print") as mocked_print:
                await modal.on_submit(interaction)
        finally:
            self.cog.service.submit_confession = original_submit

        printed = " ".join(" ".join(str(value) for value in call.args) for call in mocked_print.call_args_list)
        self.assertIn("code=confession_modal_submit_failed", printed)
        self.assertNotIn("super private confession body", printed)
        self.assertNotIn("https://private.example/path", printed)
        self.assertNotIn("99991", printed)
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
        )
        published = await self.cog.service.submit_confession(self.guild, author_id=31, content="reply to me", attachments=[])
        live_message = self.guild.get_channel(20).sent[0]
        custom_ids = [child.custom_id for child in live_message.view.children if getattr(child, "custom_id", None)]

        self.assertEqual(custom_ids, ["bb-confession-post:reply"])
        self.assertNotIn(published.confession_id, custom_ids[0])

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
        self.assertEqual(custom_ids, ["bb-confession-post:reply"])

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
