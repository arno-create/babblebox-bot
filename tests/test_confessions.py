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
)
from babblebox.confessions_service import CASE_ID_PREFIX, CONFESSION_ID_PREFIX, ConfessionSubmissionResult, ConfessionsService
from babblebox.confessions_store import ConfessionsStore


class FakeGuildPermissions:
    def __init__(self, *, administrator: bool = False, manage_guild: bool = False):
        self.administrator = administrator
        self.manage_guild = manage_guild


class FakeUser:
    def __init__(self, user_id: int, *, manage_guild: bool = False):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions(manage_guild=manage_guild, administrator=manage_guild)


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
    def __init__(self, channel_id: int, *, name: str = "general"):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
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


class FakeGuild:
    def __init__(self, guild_id: int):
        self.id = guild_id
        self.name = f"Guild {guild_id}"
        self.channels: dict[int, FakeChannel] = {}

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)


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
    def __init__(self, *, guild=None, user=None):
        self.guild = guild
        self.user = user
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


class ConfessionsServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guild = FakeGuild(10)
        self.confession_channel = FakeChannel(20, name="confessions")
        self.review_channel = FakeChannel(30, name="confession-review")
        self.panel_channel = FakeChannel(40, name="confession-panel")
        self.appeals_channel = FakeChannel(50, name="confession-appeals")
        self.guild.channels[self.confession_channel.id] = self.confession_channel
        self.guild.channels[self.review_channel.id] = self.review_channel
        self.guild.channels[self.panel_channel.id] = self.panel_channel
        self.guild.channels[self.appeals_channel.id] = self.appeals_channel
        self.other_guild = FakeGuild(11)
        self.other_confession_channel = FakeChannel(21, name="other-confessions")
        self.other_guild.channels[self.other_confession_channel.id] = self.other_confession_channel
        self.bot = FakeBot([self.guild, self.other_guild])
        self.store = ConfessionsStore(backend="memory")
        self.service = ConfessionsService(self.bot, store=self.store)
        await self.service.start()
        self.bot._cog = ServiceCogStub(self.service)

    async def asyncTearDown(self):
        await self.service.close()

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

    async def test_status_command_opens_private_dashboard(self):
        ctx = FakeContext(guild=self.guild, author=FakeUser(1, manage_guild=True))

        await ConfessionsCog.confessions_status_command.callback(self.cog, ctx, None)

        self.assertEqual(len(ctx.send_calls), 1)
        self.assertTrue(ctx.send_calls[0]["ephemeral"])
        self.assertEqual(ctx.send_calls[0]["embed"].title, "Confessions Control Panel")
        self.assertIsNotNone(ctx.send_calls[0]["view"])

    async def test_slash_confess_opens_modal_with_no_arguments(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(11))

        await ConfessionsCog.confess_command.callback(self.cog, interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")

    async def test_member_panel_button_opens_modal(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        interaction = FakeInteraction(guild=self.guild, user=FakeUser(12))
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)

        await view.send_button.callback(interaction)

        self.assertEqual(len(interaction.response.modal_calls), 1)
        self.assertEqual(interaction.response.modal_calls[0].title, "Anonymous Confession")
        self.assertIsNone(interaction.response.modal_calls[0].upload_input)

    async def test_member_panel_manage_and_support_buttons_open_private_flows(self):
        await self.cog.service.configure_guild(self.guild.id, enabled=True, confession_channel_id=20, review_mode=False)
        view = self.cog.build_member_panel_view(guild_id=self.guild.id)

        manage_interaction = FakeInteraction(guild=self.guild, user=FakeUser(12))
        await view.manage_button.callback(manage_interaction)
        self.assertEqual(manage_interaction.response.modal_calls[0].title, "Manage My Confession")

        support_interaction = FakeInteraction(guild=self.guild, user=FakeUser(12))
        await view.support_button.callback(support_interaction)
        self.assertEqual(support_interaction.response.sent[0]["kwargs"]["embed"].title, "Private Support")
        self.assertIsNotNone(support_interaction.response.sent[0]["kwargs"]["view"])

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
