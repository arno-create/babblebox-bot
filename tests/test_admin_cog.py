from __future__ import annotations

import asyncio
import types
import unittest

from babblebox.cogs.admin import AdminCog
from babblebox.admin_service import AdminService
from babblebox.admin_store import AdminStore


class FakeMessage:
    pass


class FakeResponse:
    def __init__(self):
        self._done = False

    def is_done(self):
        return self._done


class FakeInteraction:
    def __init__(self):
        self.response = FakeResponse()

    def is_expired(self):
        return False


class FakeGuildPermissions:
    administrator = False
    manage_guild = False


class FakePermissionSnapshot:
    def __init__(self, **overrides):
        defaults = {
            "manage_roles": False,
            "kick_members": False,
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
    def __init__(self, user_id: int = 1, *, manage_guild: bool = False):
        self.id = user_id
        self.display_name = f"User {user_id}"
        self.mention = f"<@{user_id}>"
        self.guild_permissions = FakeGuildPermissions()
        self.guild_permissions.manage_guild = manage_guild


class FakeChannel:
    def __init__(self, channel_id: int, *, name: str = "general", permissions: FakePermissionSnapshot | None = None):
        self.id = channel_id
        self.name = name
        self.mention = f"<#{channel_id}>"
        self._permissions = permissions or FakePermissionSnapshot()

    def permissions_for(self, member):
        return self._permissions


class FakeGuild:
    def __init__(self, guild_id: int = 10):
        self.id = guild_id
        self.name = "Guild"
        self.owner_id = 1
        self.channels: dict[int, FakeChannel] = {}
        self.roles: dict[int, FakeRole] = {}
        self.members: dict[int, object] = {}
        self.me = types.SimpleNamespace(
            id=999,
            top_role=FakeRole(900, position=50),
            guild_permissions=FakePermissionSnapshot(manage_roles=True, kick_members=True, mention_everyone=True),
        )

    def get_channel(self, channel_id: int):
        return self.channels.get(channel_id)

    def get_role(self, role_id: int):
        return self.roles.get(role_id)

    def get_member(self, user_id: int):
        if user_id == self.me.id:
            return self.me
        return self.members.get(user_id)


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
        return FakeMessage()

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

    async def test_verification_panel_spells_out_verified_and_unverified_members(self):
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

        embed = await self.cog.build_panel_embed(self.guild.id, "verification")
        current_rule = next(field for field in embed.fields if field.name == "Current Rule")
        target = next(field for field in embed.fields if field.name == "Warn / Kick Target")

        self.assertIn("Members are considered verified only if they HAVE <@&80>.", current_rule.value)
        self.assertIn("Users WITHOUT <@&80> are treated as unverified.", current_rule.value)
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
            kick_after_text="7d",
            warning_lead_text="2d",
            help_channel_id=None,
            help_extension_text="1d",
            max_extensions=1,
        )
        self.assertTrue(ok)

        embed = await self.cog.build_panel_embed(self.guild.id, "verification")
        review = next(field for field in embed.fields if field.name == "Please Review Carefully")
        target = next(field for field in embed.fields if field.name == "Warn / Kick Target")

        self.assertIn("sounds like an unverified-state role", review.value)
        self.assertIn("users WITH <@&81> should be warned and kicked", review.value)
        self.assertIn("users who still have <@&81> will be warned after 5 days and kicked after 1 week.", target.value)

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
