import asyncio
import asyncio
import types
import unittest
from copy import deepcopy

import discord

from babblebox.cogs.shield import (
    ShieldCog,
    ShieldLinkPolicyEditorView,
    ShieldLogsEditorView,
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

    async def test_overview_protection_packs_keep_full_gif_summary_without_truncation(self):
        embed = self.cog._overview_embed(10)
        protection = next(field for field in embed.fields if field.name == "Protection Packs")

        self._assert_embed_valid(embed)
        self.assertIn(
            "Delete lane removes bounded GIF bursts; collective cleanup uses the exact streak or trims the newest contributing GIFs inside the active pressure slice while personal abuse still targets one member.",
            protection.value,
        )
        self.assertFalse(protection.value.endswith("..."))

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

    async def test_rules_and_scope_embed_report_saved_state_above_current_plan(self):
        dense = deepcopy(self.cog.service.get_config(10))
        dense["custom_patterns"] = [
            {"pattern_id": f"p{i}", "label": f"Pattern {i}", "pattern": f"test-{i}", "mode": "contains", "action": "log"}
            for i in range(11)
        ]
        dense["pack_exemptions"]["spam"] = {
            "channel_ids": list(range(100, 122)),
            "role_ids": [],
            "user_ids": [],
        }
        self.cog.service.store.state["guilds"]["10"] = dense
        self.cog.service._compiled_configs.pop(10, None)

        rules = self.cog._rules_embed(10, selected_pack="spam")
        scope = self.cog._scope_embed(10)
        rule_fields = {field.name: field.value for field in rules.fields}
        scope_fields = {field.name: field.value for field in scope.fields}

        self.assertIn("saved **11** | active on this plan **10 / 10**", rule_fields["Global Fallbacks"])
        self.assertIn("stays preserved", rule_fields["Global Fallbacks"])
        self.assertIn("Spam", scope_fields["Saved Above Current Plan"])
        self.assertIn("Anti-Spam channels: saved **22** | active on this plan **20 / 20**", scope_fields["Saved Above Current Plan"])
        self.assertIn("stays preserved", scope_fields["Saved Above Current Plan"])

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
        self.assertIn("True channel streak", options.value)
        self.assertNotIn("Emoji / emote", options.value)

    async def test_gif_options_editor_uses_lane_selector_and_broad_values(self):
        view = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="gif")
        lane_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF lane")
        count_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF-heavy rate count")
        window_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF-heavy rate window")
        ratio_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Minimum GIF ratio")

        self.assertIn("rate", [option.value for option in lane_select.options])
        self.assertIn("same_asset", [option.value for option in lane_select.options])
        self.assertIn("3", [option.value for option in count_select.options])
        self.assertIn("12", [option.value for option in count_select.options])
        self.assertIn("3", [option.value for option in window_select.options])
        self.assertIn("45", [option.value for option in window_select.options])
        self.assertIn("50", [option.value for option in ratio_select.options])
        self.assertIn("95", [option.value for option in ratio_select.options])

    async def test_gif_rate_selection_persists_custom_values_and_rerenders_cleanly(self):
        view = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="gif")
        interaction = self._interaction(message=FakeMessage(channel=FakeChannel()))
        count_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF-heavy rate count")
        window_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF-heavy rate window")

        count_select._values = ["3"]
        await count_select.callback(interaction)

        window_select = next(child for child in view.children if getattr(child, "placeholder", "") == "GIF-heavy rate window")
        window_select._values = ["15"]
        await window_select.callback(interaction)

        config = self.cog.service.get_config(10)
        self.assertEqual(config["gif_message_threshold"], 3)
        self.assertEqual(config["gif_window_seconds"], 15)

        refreshed = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="gif")
        refreshed_count = next(child for child in refreshed.children if getattr(child, "placeholder", "") == "GIF-heavy rate count")
        refreshed_window = next(child for child in refreshed.children if getattr(child, "placeholder", "") == "GIF-heavy rate window")
        default_count = next(option for option in refreshed_count.options if option.default)
        default_window = next(option for option in refreshed_window.options if option.default)
        self.assertEqual(default_count.value, "3")
        self.assertEqual(default_window.value, "15")

    async def test_spam_options_editor_uses_lane_selector_and_broad_values(self):
        view = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="spam")
        placeholders = [getattr(child, "placeholder", "") for child in view.children if hasattr(child, "placeholder")]

        self.assertIn("Anti-Spam lane", placeholders)
        self.assertIn("Rate lane state", placeholders)
        self.assertIn("Rate message count", placeholders)
        self.assertIn("Rate window", placeholders)
        self.assertIn("Moderator anti-spam policy", placeholders)

        count_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Rate message count")
        window_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Rate window")
        self.assertIn("4", [option.value for option in count_select.options])
        self.assertIn("12", [option.value for option in count_select.options])
        self.assertIn("3", [option.value for option in window_select.options])
        self.assertIn("30", [option.value for option in window_select.options])

    async def test_spam_near_duplicate_selection_persists_and_rerenders_cleanly(self):
        view = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="spam")
        interaction = self._interaction(message=FakeMessage(channel=FakeChannel()))
        lane_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Anti-Spam lane")
        lane_select._values = ["near_duplicate"]
        await lane_select.callback(interaction)

        count_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Near-duplicate count")
        count_select._values = ["3"]
        await count_select.callback(interaction)

        window_select = next(child for child in view.children if getattr(child, "placeholder", "") == "Near-duplicate window")
        window_select._values = ["8"]
        await window_select.callback(interaction)

        config = self.cog.service.get_config(10)
        self.assertEqual(config["spam_near_duplicate_threshold"], 3)
        self.assertEqual(config["spam_near_duplicate_window_seconds"], 8)

        current_count = next(child for child in view.children if getattr(child, "placeholder", "") == "Near-duplicate count")
        current_window = next(child for child in view.children if getattr(child, "placeholder", "") == "Near-duplicate window")
        self.assertEqual(next(option for option in current_count.options if option.default).value, "3")
        self.assertEqual(next(option for option in current_window.options if option.default).value, "8")

    async def test_spam_pack_summary_reports_disabled_lanes_truthfully(self):
        current = deepcopy(self.cog.service.get_config(10))
        current["spam_message_enabled"] = False
        current["spam_burst_enabled"] = False
        current["spam_near_duplicate_enabled"] = True
        current["spam_emote_enabled"] = True
        current["spam_emote_threshold"] = 24
        self.cog.service.store.state["guilds"]["10"] = current
        self.cog.service._compiled_configs.pop(10, None)

        embed = ShieldPackOptionsEditorView(self.cog, guild_id=10, author_id=1, pack="spam").current_embed()
        options = next(field for field in embed.fields if field.name == "Current Options")

        self.assertIn("Rate lane: Off", options.value)
        self.assertIn("Burst lane: Off", options.value)
        self.assertIn("Near-duplicate lane: On", options.value)
        self.assertIn("Emoji / emote lane: On at 24+", options.value)

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

    async def test_logs_embed_shows_global_delivery_and_pack_overrides(self):
        await self.cog.service.set_log_delivery(10, style="compact", ping_mode="never")
        await self.cog.service.set_pack_log_override(10, "gif", style="compact", ping_mode="never")

        embed = self.cog._logs_embed(10)

        self.assertEqual(embed.title, "Shield Logs")
        self.assertTrue(any(field.name == "Global Delivery" for field in embed.fields))
        self.assertTrue(any(field.name == "Per-Pack Overrides" for field in embed.fields))
        delivery = next(field for field in embed.fields if field.name == "Global Delivery")
        overrides = next(field for field in embed.fields if field.name == "Per-Pack Overrides")
        self.assertIn("Compact", delivery.value)
        self.assertIn("Never ping", delivery.value)
        self.assertIn("GIF Flood / Media Pressure", overrides.value)

    async def test_logs_editor_exposes_global_and_pack_override_controls(self):
        view = ShieldLogsEditorView(self.cog, guild_id=10, author_id=1)
        embed = view.current_embed()
        placeholders = [getattr(child, "placeholder", "") for child in view.children if hasattr(child, "placeholder")]

        self.assertEqual(embed.title, "Shield Log Delivery")
        self.assertIn("Global log style", placeholders)
        self.assertIn("Global ping mode", placeholders)
        self.assertIn("Pack override target", placeholders)
        self.assertTrue(any("style override" in placeholder for placeholder in placeholders))
        self.assertTrue(any("ping override" in placeholder for placeholder in placeholders))
