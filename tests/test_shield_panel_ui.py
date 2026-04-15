import asyncio
import types
import unittest
from copy import deepcopy

import discord

from babblebox.cogs.shield import (
    ShieldCog,
    ShieldLinkPolicyEditorView,
    ShieldPackActionEditorView,
    ShieldPackExemptionsEditorView,
    ShieldPackOptionsEditorView,
    ShieldPanelView,
)
from tests.test_hybrid_command_smoke import FakeAuthor, FakeChannel, FakeGuild, FakeInteraction, FakeMessage


def _embed_total_chars(embed: discord.Embed) -> int:
    total = len(embed.title or "") + len(embed.description or "") + len(getattr(embed.footer, "text", "") or "")
    for field in embed.fields:
        total += len(field.name or "") + len(field.value or "")
    return total


class ShieldPanelUiTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.bot = types.SimpleNamespace(loop=asyncio.get_running_loop())
        self.cog = ShieldCog(self.bot)
        self.cog.service.storage_ready = True

    async def asyncTearDown(self):
        await self.cog.service.close()

    def _interaction(self, *, message=None, user=None):
        return FakeInteraction(
            message=message or FakeMessage(channel=FakeChannel()),
            channel=FakeChannel(),
            guild=FakeGuild(10),
            user=user or FakeAuthor(manage_guild=True),
        )

    def _assert_embed_valid(self, embed: discord.Embed):
        self.assertLessEqual(len(embed.title or ""), 256)
        self.assertLessEqual(len(embed.description or ""), 4096)
        self.assertLessEqual(_embed_total_chars(embed), 6000)
        for field in embed.fields:
            self.assertLessEqual(len(field.name or ""), 256)
            self.assertLessEqual(len(field.value or ""), 1024)

    async def test_rules_embed_stays_within_discord_limits_by_default(self):
        embed = self.cog._rules_embed(10, selected_pack="spam")

        self._assert_embed_valid(embed)
        self.assertEqual(embed.title, "Shield Rules")
        self.assertTrue(any(field.name == "Anti-Spam Details" for field in embed.fields))

    async def test_rules_and_scope_embeds_stay_within_limits_for_dense_config(self):
        dense = deepcopy(self.cog.service.get_config(10))
        dense["allow_domains"] = [f"example{i}.com" for i in range(25)]
        dense["allow_invite_codes"] = [f"invite{i}" for i in range(25)]
        dense["allow_phrases"] = [f"phrase-{i}" for i in range(25)]
        dense["custom_patterns"] = [
            {"pattern_id": f"p{i}", "label": f"Pattern {i}", "pattern": f"test-{i}", "mode": "contains", "action": "log"}
            for i in range(20)
        ]
        dense["pack_timeout_minutes"]["spam"] = 20
        for pack in ("privacy", "promo", "scam", "spam", "gif", "adult", "severe"):
            dense["pack_exemptions"][pack] = {
                "channel_ids": list(range(100, 110)),
                "role_ids": list(range(200, 210)),
                "user_ids": list(range(300, 310)),
            }
        self.cog.service.store.state["guilds"]["10"] = dense
        self.cog.service._compiled_configs.pop(10, None)

        rules = self.cog._rules_embed(10, selected_pack="spam")
        scope = self.cog._scope_embed(10)

        self._assert_embed_valid(rules)
        self._assert_embed_valid(scope)

    async def test_rules_button_switches_panel_without_failure(self):
        view = ShieldPanelView(self.cog, guild_id=10, author_id=1, section="overview")
        message = FakeMessage(channel=FakeChannel())
        interaction = self._interaction(message=message)
        button = next(child for child in view.children if getattr(child, "label", None) == "Rules")

        await button.callback(interaction)

        self.assertEqual(view.section, "rules")
        self.assertEqual(interaction.message.embed.title, "Shield Rules")
        self.assertEqual(interaction.response.defer_calls[0][1]["thinking"], False)

    async def test_links_navigation_still_works_after_panel_refactor(self):
        view = ShieldPanelView(self.cog, guild_id=10, author_id=1, section="rules")
        message = FakeMessage(channel=FakeChannel())
        interaction = self._interaction(message=message)
        button = next(child for child in view.children if getattr(child, "label", None) == "Links")

        await button.callback(interaction)

        self.assertEqual(view.section, "links")
        self.assertEqual(interaction.message.embed.title, "Shield Link Policy")
        self.assertIn("Edit Link Policy", [child.label for child in view.children if hasattr(child, "label")])

    async def test_panel_render_failure_returns_private_feedback(self):
        view = ShieldPanelView(self.cog, guild_id=10, author_id=1, section="overview")
        interaction = FakeInteraction(
            message=None,
            channel=FakeChannel(),
            guild=FakeGuild(10),
            user=FakeAuthor(manage_guild=True),
            edit_original_response_exception=discord.ClientException("edit failed"),
        )
        button = next(child for child in view.children if getattr(child, "label", None) == "Rules")

        await button.callback(interaction)

        self.assertTrue(interaction.followup_calls)
        warning = interaction.followup_calls[-1][1]["embed"]
        self.assertIn("Run `/shield panel` again", warning.description)

    async def test_expired_panel_view_fails_gracefully(self):
        view = ShieldPanelView(self.cog, guild_id=10, author_id=1, section="rules")
        view._expired = True
        interaction = self._interaction()

        allowed = await view.interaction_check(interaction)

        self.assertFalse(allowed)
        self.assertTrue(interaction.response.send_calls)
        embed = interaction.response.send_calls[-1][1]["embed"]
        self.assertIn("expired", embed.description.lower())

    async def test_spam_pack_detail_shows_spam_only_controls(self):
        embed = self.cog._rules_embed(10, selected_pack="spam")
        detail = next(field for field in embed.fields if field.name == "Anti-Spam Details")

        self.assertIn("Emoji / emote lane", detail.value)
        self.assertIn("Capitals lane", detail.value)
        self.assertNotIn("Same asset", detail.value)

    async def test_severe_pack_detail_hides_spam_and_gif_only_controls(self):
        embed = self.cog._rules_embed(10, selected_pack="severe")
        detail = next(field for field in embed.fields if field.name == "Severe Harm / Hate Details")

        self.assertNotIn("Emoji / emote lane", detail.value)
        self.assertNotIn("Capitals lane", detail.value)
        self.assertNotIn("Same asset", detail.value)
        self.assertIn("Categories:", detail.value)

    async def test_gif_options_editor_only_shows_gif_controls(self):
        embed = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="gif").current_embed()
        options = next(field for field in embed.fields if field.name == "Current Options")

        self.assertIn("Same asset", options.value)
        self.assertIn("GIF-heavy rate", options.value)
        self.assertNotIn("Emoji / emote", options.value)

    async def test_spam_options_editor_exposes_emoji_and_caps_controls(self):
        view = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="spam")
        placeholders = [getattr(child, "placeholder", "") for child in view.children if hasattr(child, "placeholder")]

        self.assertIn("Emoji / emote lane + threshold", placeholders)
        self.assertIn("Capitals lane + threshold", placeholders)

    async def test_action_editor_shows_dedicated_pack_timeout(self):
        current = deepcopy(self.cog.service.get_config(10))
        current["pack_timeout_minutes"]["severe"] = 22
        self.cog.service.store.state["guilds"]["10"] = current
        self.cog.service._compiled_configs.pop(10, None)

        embed = ShieldPackActionEditorView(self.cog, guild_id=10, author_id=1, pack="severe").current_embed()
        current_profile = next(field for field in embed.fields if field.name == "Current Profile")

        self.assertIn("Dedicated `22` minute timeout", current_profile.value)

    async def test_pack_exemptions_editor_updates_only_selected_pack(self):
        view = ShieldPackExemptionsEditorView(self.cog, guild_id=10, author_id=1, pack="spam")
        role_select = next(child for child in view.children if isinstance(child, discord.ui.RoleSelect))
        role_select._values = [types.SimpleNamespace(id=77)]
        interaction = self._interaction(message=FakeMessage(channel=FakeChannel()))

        await role_select.callback(interaction)

        config = self.cog.service.get_config(10)
        self.assertEqual(config["pack_exemptions"]["spam"]["role_ids"], [77])
        self.assertEqual(config["pack_exemptions"]["severe"]["role_ids"], [])

    async def test_global_scope_and_pack_exemptions_stay_separate(self):
        await self.cog.service.set_filter_target(10, "excluded_channel_ids", 55, True)
        view = ShieldPackExemptionsEditorView(self.cog, guild_id=10, author_id=1, pack="gif")
        channel_select = next(child for child in view.children if isinstance(child, discord.ui.ChannelSelect))
        channel_select._values = [types.SimpleNamespace(id=88)]
        interaction = self._interaction(message=FakeMessage(channel=FakeChannel()))

        await channel_select.callback(interaction)

        config = self.cog.service.get_config(10)
        self.assertEqual(config["excluded_channel_ids"], [55])
        self.assertEqual(config["pack_exemptions"]["gif"]["channel_ids"], [88])

    async def test_link_policy_editor_surfaces_timeout_and_mode_controls(self):
        view = ShieldLinkPolicyEditorView(self.cog, guild_id=10, author_id=1)
        embed = view.current_embed()
        placeholders = [getattr(child, "placeholder", "") for child in view.children if hasattr(child, "placeholder")]

        self.assertEqual(embed.title, "Shield Link Policy")
        self.assertIn("Trusted-link timeout profile", placeholders)
        self.assertIn("Trusted-link mode", placeholders)
