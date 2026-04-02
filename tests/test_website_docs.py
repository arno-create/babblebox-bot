from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent


class WebsiteDocsTests(unittest.TestCase):
    def test_help_page_exists_and_covers_required_sections(self):
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        for anchor in (
            'id="getting-started"',
            'id="visibility"',
            'id="party-games"',
            'id="daily-arcade"',
            'id="buddy-profile-vault"',
            'id="utilities"',
            'id="shield-admin"',
            'id="faq"',
        ):
            self.assertIn(anchor, help_html)
        for command in ("/daily", "/buddy", "/profile", "/vault", "/watch", "/later", "/capture", "/remind", "/afk", "/shield panel"):
            self.assertIn(command, help_html)
        self.assertIn("Strict = reply to the armed question only", help_html)
        self.assertIn("1-10 drops per day", help_html)
        self.assertIn("/drops leaderboard", help_html)
        self.assertIn("/drops roles status", help_html)
        self.assertIn("/drops mastery category", help_html)
        self.assertIn("template_action", help_html)
        self.assertIn("{user.mention}", help_html)
        self.assertIn("{category.name}", help_html)
        self.assertNotIn("/drops mastery category-template", help_html)
        self.assertNotIn("/drops mastery scholar-template", help_html)
        self.assertIn("scholar ladder", help_html)
        self.assertIn("Pattern Hunt", help_html)
        self.assertIn("Coders need server DMs open before the room starts.", help_html)
        self.assertNotIn("shieldaiglobal", help_html.casefold())
        self.assertNotIn("dropscelebaiglobal", help_html.casefold())

    def test_help_page_is_linked_from_site_shells(self):
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")
        terms_html = (ROOT / "terms.html").read_text(encoding="utf-8")

        self.assertIn('href="help.html"', index_html)
        self.assertIn('href="help.html"', privacy_html)
        self.assertIn('href="help.html"', terms_html)

    def test_sitemap_includes_help_page(self):
        sitemap = (ROOT / "sitemap.xml").read_text(encoding="utf-8")

        self.assertIn("help.html", sitemap)

    def test_readme_and_homepage_reflect_recent_feature_set(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "Only 16",
            "Pattern Hunt",
            "Question Drops",
            "category mastery roles",
            "guild scholar ladder",
            "/drops leaderboard",
            "/drops roles status",
            "/drops mastery category",
            "template_action",
            "{user.mention}",
            "{category.name}",
            "/hunt guess",
            "Pattern Hunt coder role DMs",
        ):
            self.assertIn(text, readme)

        for text in (
            "Only 16",
            "Pattern Hunt",
            "Question Drops",
            "mastery",
            "scholar",
            "/drops status",
            "/drops roles status",
            "/drops mastery category",
            "{user.mention}",
            "{category.name}",
            "Pattern Hunt coders need server DMs open before the round starts.",
        ):
            self.assertIn(text, index_html)

        self.assertNotIn("dropscelebaiglobal", readme.casefold())
        self.assertNotIn("dropscelebaiglobal", index_html.casefold())
        self.assertNotIn("/drops panel", readme)

    def test_homepage_and_readme_use_current_proof_assets(self):
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        assets_dir = ROOT / "assets"

        self.assertIn("assets/drops_status_example.png", index_html)

        for asset_name in (
            "lobby.png",
            "drops_status_example.png",
            "buddy_profile_example.png",
            "watch_settings.png",
            "shield_panel_example.png",
        ):
            self.assertIn(f"assets/{asset_name}", readme)
            self.assertTrue((assets_dir / asset_name).exists())

    def test_help_and_readme_do_not_reintroduce_duplicate_drops_panel_copy(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        self.assertIn("/drops status", readme)
        self.assertIn("/drops status", help_html)
        self.assertNotIn("/drops panel", readme)
        self.assertNotIn("/drops panel", help_html)
        self.assertNotIn("category-template", readme)
        self.assertNotIn("scholar-template", readme)
        self.assertNotIn("category-template", help_html)
        self.assertNotIn("scholar-template", help_html)

    def test_homepage_keeps_trust_and_utility_positioning_grounded(self):
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "private-first",
            "off by default",
            "/shield panel",
            "Watch is DM-only by design",
            "AFK stays clear about time",
        ):
            self.assertIn(text, index_html)

    def test_readme_examples_drop_known_fake_prefix_shapes(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")

        for bad_example in (
            "bb!hunt guess contains_digits contains_category_word:animal",
            "bb!drops config enabled:true drops_per_day:4",
            "bb!drops mastery category science enabled true tier 1 @Role 25",
            "bb!shield rules pack promo enabled true action log sensitivity normal",
            "bb!shield filters only_included trusted_role_ids on @Mods",
            "bb!admin followup enabled true @Probation review 30d",
            "bb!admin verification enabled true @Verified must_have_role 7d 2d",
            "bb!admin exclusions trusted_role_ids on @Mods",
            "bb!admin templates invite_link https://discord.gg/example",
        ):
            self.assertNotIn(bad_example, readme)

        for expected_text in (
            "Use slash for multi-family Pattern Hunt guesses.",
            "Slash is recommended for multi-option setup here.",
            "Slash is the best fit for multi-option admin setup here.",
            "Slash is recommended for the heavier config flows here.",
            "bb!hunt guess contains_digits",
            "bb!drops config true 4",
            "bb!shield rules true promo true log",
            "bb!admin followup true @Probation review 30d",
        ):
            self.assertIn(expected_text, readme)
