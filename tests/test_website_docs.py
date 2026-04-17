from pathlib import Path
import unittest

from babblebox.web import app


ROOT = Path(__file__).resolve().parent.parent


class WebsiteDocsTests(unittest.TestCase):
    def test_all_hosted_pages_use_shared_site_shell_contract(self):
        for page_name in ("index.html", "help.html", "privacy.html", "terms.html"):
            with self.subTest(page=page_name):
                html = (ROOT / page_name).read_text(encoding="utf-8")
                self.assertIn('href="assets/site-shell.css"', html)
                self.assertIn('src="assets/site-shell.js"', html)
                self.assertIn("data-site-nav", html)
                self.assertIn("data-nav-toggle", html)
                self.assertIn("data-nav-panel", html)
                self.assertIn("site-nav-shell", html)
                self.assertIn("site-footer", html)
                self.assertIn("site-footer-grid", html)

    def test_help_page_exists_and_covers_required_sections(self):
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        for anchor in (
            'id="getting-started"',
            'id="visibility"',
            'id="confessions"',
            'id="party-games"',
            'id="daily-arcade"',
            'id="buddy-profile-vault"',
            'id="utilities"',
            'id="shield-admin"',
            'id="faq"',
        ):
            self.assertIn(anchor, help_html)
        for command in ("/support", "/daily", "/buddy", "/profile", "/vault", "/watch", "/later", "/capture", "/remind", "/afk", "/shield panel", "/shield links", "/shield trusted", "/lock channel", "/lock remove", "/lock settings", "/timeout remove", "/admin panel", "/admin followup", "/admin verification", "/admin permissions", "/confess", "/confessions moderate"):
            self.assertIn(command, help_html)
        self.assertIn("Broken Telephone, Exquisite Corpse, Spyfall, Word Bomb, and Pattern Hunt", help_html)
        self.assertIn("1-10 drops per day", help_html)
        self.assertIn("/drops leaderboard", help_html)
        self.assertIn("/drops roles status", help_html)
        self.assertIn("/dropsadmin config", help_html)
        self.assertIn("/dropsadmin mastery category", help_html)
        self.assertIn("difficulty profile", help_html)
        self.assertIn("Difficulty, Length, and the booth Profile", help_html)
        self.assertIn("template_action", help_html)
        self.assertIn("{user.mention}", help_html)
        self.assertIn("{category.name}", help_html)
        self.assertNotIn("/drops mastery category-template", help_html)
        self.assertNotIn("/drops mastery scholar-template", help_html)
        self.assertIn("scholar ladder", help_html)
        self.assertIn("Pattern Hunt", help_html)
        self.assertIn("Coders need server DMs open before the room starts.", help_html)
        self.assertNotIn("Only 16", help_html)
        self.assertNotIn("shieldaiglobal", help_html.casefold())
        self.assertNotIn("dropscelebaiglobal", help_html.casefold())
        self.assertIn("staff-blind", help_html)
        self.assertIn("optional in Babblebox", help_html)
        self.assertIn("until admins enable and configure them", help_html)
        self.assertIn("Babblebox still enforces safety internally", help_html)
        self.assertIn("adult / 18+ language is blocked by default", help_html)
        self.assertIn("images stay off by default", help_html)
        self.assertIn("/confess reply-to-user", help_html)
        self.assertIn("Reply anonymously", help_html)
        self.assertNotIn("Reply to confession anonymously", help_html)
        self.assertIn("Create a confession", help_html)
        self.assertIn("Anonymous Owner Reply", help_html)
        self.assertIn("Owner replies are enabled by default", help_html)
        self.assertIn("private approval before posting", help_html)
        self.assertIn("4000 characters", help_html)
        self.assertIn("reuse one Babblebox thread", help_html)
        self.assertIn("without nested threads", help_html)
        self.assertIn("Allow All Safe", help_html)
        self.assertIn("explicitly replies to your confession or your first public owner reply", help_html)
        self.assertIn("can still reveal you if you include it", help_html)
        self.assertIn("simple number words only count for whole-number answers", help_html)
        self.assertIn("ordered-sequence prompts are one-shot", help_html)
        self.assertNotIn("jump-nav", help_html)
        self.assertIn("guide-band", help_html)
        self.assertIn("Anti-Spam", help_html)
        self.assertNotIn("/admin risk", help_html)
        self.assertNotIn("/admin emergency", help_html)

    def test_help_page_is_linked_from_site_shells(self):
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")
        terms_html = (ROOT / "terms.html").read_text(encoding="utf-8")

        self.assertIn('href="help.html"', index_html)
        self.assertIn('href="help.html"', privacy_html)
        self.assertIn('href="help.html"', terms_html)

    def test_bday_project_link_target_stays_intentional(self):
        target = 'href="https://arno-create.github.io/Bdayblaze/"'

        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")
        terms_html = (ROOT / "terms.html").read_text(encoding="utf-8")

        self.assertIn(target, index_html)
        self.assertIn(target, help_html)
        self.assertIn(target, privacy_html)
        self.assertIn(target, terms_html)

    def test_sitemap_includes_help_page(self):
        sitemap = (ROOT / "sitemap.xml").read_text(encoding="utf-8")

        self.assertIn("help.html", sitemap)

    def test_readme_and_homepage_reflect_recent_feature_set(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "Broken Telephone",
            "Word Bomb",
            "Pattern Hunt",
            "Question Drops",
            "category mastery roles",
            "guild scholar ladder",
            "/drops leaderboard",
            "/drops roles status",
            "/dropsadmin config",
            "/dropsadmin mastery category",
            "difficulty profile",
            "Difficulty, Length, and Profile",
            "template_action",
            "{user.mention}",
            "{category.name}",
            "one-shot ordered-sequence",
            "/hunt guess",
            "Pattern Hunt coder role DMs",
            "Anonymous Confessions",
            "/confess",
            "/confess reply-to-user",
            "/confessions moderate",
            "/support",
            "/lock channel",
            "/lock remove",
            "/lock settings",
            "/timeout remove",
            "/admin permissions",
            "PYTHONPATH=. pytest -q",
            "optional feature that only works after admins enable and configure it",
            "adult / 18+ language is blocked by default",
            "images are off by default",
            "/shield trusted",
            "recommended non-AI baseline",
            "Anti-Spam",
            "GIF Flood / Media Pressure",
            "Reply anonymously",
            "Create a confession",
            "private approval happens first",
            "4000 characters",
            "Allow All Safe",
            "Trusted Only",
            "reusable thread",
            "without nested threads",
            "Anonymous Owner Reply",
            "owner replies are a separate feature",
            "automatically suspend or confession-ban",
            "can still reveal who sent it",
            "duplicate-abuse signals are keyed and guild-scoped",
            "privacy hardening is `Ready` or `Partial`",
            "CONFESSIONS_CONTENT_KEY_ID",
            "CONFESSIONS_CONTENT_LEGACY_KEYS",
            "Confessions Key Rotation",
        ):
            self.assertIn(text, readme)

        for text in (
            "Broken Telephone",
            "Word Bomb",
            "Pattern Hunt",
            "Question Drops",
            "mastery",
            "scholar",
            "/drops status",
            "/drops roles status",
            "/dropsadmin config",
            "/dropsadmin mastery category",
            "/lock channel",
            "/timeout remove",
            "/admin permissions",
            "{user.mention}",
            "{category.name}",
            "Pattern Hunt coders need server DMs open before the round starts.",
            "anonymous confessions",
            "admin-enabled anonymous confessions",
            "images stay off by default unless admins explicitly turn them on",
            "explicit anti-spam thresholds",
            "grouped incident dedupe",
        ):
            self.assertIn(text, index_html)

        self.assertNotIn("Only 16", readme)
        self.assertNotIn("Only 16", index_html)
        self.assertNotIn("dropscelebaiglobal", readme.casefold())
        self.assertNotIn("dropscelebaiglobal", index_html.casefold())
        self.assertNotIn("/drops panel", readme)

    def test_shield_docs_keep_local_malicious_domain_feed_copy_grounded(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        self.assertIn("~200k known malicious domains", readme)
        self.assertIn("about 200k known malicious domains", index_html)
        self.assertIn("about 200k known malicious domains", help_html)

    def test_shield_docs_describe_local_scam_corroboration(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        self.assertIn("newcomer first-link context", readme)
        self.assertIn("fresh-campaign reuse", readme)
        self.assertIn("no-link DM-lure", help_html)
        self.assertIn("no-link DM-lure", index_html)
        self.assertIn("combined local scam evidence", help_html)

    def test_shield_docs_cover_compact_antispam_and_gif_grouping(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        self.assertIn("Anti-Spam", readme)
        self.assertIn("Anti-Spam", help_html)
        self.assertIn("short-lived per-user windows", readme)
        self.assertIn("grouped incidents per offender and incident window", help_html)
        self.assertIn("explicit anti-spam thresholds", index_html)
        self.assertIn("grouped incident dedupe", index_html)

    def test_shield_docs_cover_hybrid_gif_fairness_and_compact_logging(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        self.assertIn("one-member rate, low-text repeat gate, same-asset repeat, channel streak, and GIF-to-text balance", help_html)
        self.assertIn("configurable one-member rate, low-text repeat, same-asset, channel-streak, and GIF-to-text pressure controls", index_html)
        self.assertIn("consecutive GIF streaks or GIF-to-text imbalance", readme)
        self.assertIn("full live GIF streak", readme)
        self.assertIn("newest excess GIF posts", readme)
        self.assertIn("collective pressure never adds strikes or timeouts on its own", readme)
        self.assertIn("shared cleanup with one member's personal enforcement", help_html)
        self.assertIn("shared cleanup with one member's personal GIF enforcement", index_html)
        self.assertIn("tighter low-end GIF options", readme)
        self.assertIn("Adaptive", help_html)
        self.assertIn("Compact", help_html)
        self.assertIn("smart or never-ping alerts", index_html)
        self.assertIn("compact/no-ping defaults", readme)

    def test_shield_and_admin_docs_cover_gif_spam_and_focused_admin_surfaces(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        for text in (
            "GIF Flood / Media Pressure",
            "/lock channel",
            "/lock remove",
            "/lock settings",
            "/timeout remove",
            "/admin panel",
            "/admin followup",
            "/admin verification",
            "/admin logs",
            "/admin templates",
            "/admin permissions",
        ):
            self.assertIn(text, readme)
            self.assertIn(text, help_html)
        for text in ("GIF Flood / Media Pressure", "/admin panel", "/lock channel", "/admin permissions"):
            self.assertIn(text, index_html)
        for text in (
            "manage channels or messages, timeout, kick, or ban members",
            "admins-only",
        ):
            self.assertIn(text, readme)
            self.assertIn(text, help_html)
            self.assertIn(text, index_html)

    def test_admin_panel_docs_reflect_interactive_control_surface_story(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        self.assertIn("interactive admin control surface", readme)
        self.assertIn("commands remain precise fallbacks", readme)
        self.assertIn("overview quick-config row", readme)
        self.assertIn("interactive admin lifecycle surface", help_html)
        self.assertIn("sectioned control surface", help_html)
        self.assertIn("precise fallback path", help_html)
        self.assertIn("overview quick-config row", help_html)
        self.assertIn("sectioned interactive control surface", index_html)
        self.assertIn("illustrative rather than exhaustive", index_html)
        self.assertIn("overview quick-config row", index_html)

    def test_shield_docs_cover_no_link_dm_lure_and_truthful_ai_models(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "no-link DM-lure",
            "gpt-5.4-nano",
            "gpt-5.4-mini",
            "gpt-5.4",
            "resolved routing lane plus local readiness",
        ):
            self.assertIn(text, readme)
        for text in ("no-link DM-lure", "gpt-5.4-nano", "gpt-5.4-mini", "gpt-5.4", "resolved routing lane plus local readiness"):
            self.assertIn(text, help_html)
        self.assertIn("no-link DM-lure", index_html)
        self.assertIn("resolved routing lane plus local readiness", index_html)
        self.assertNotIn("gpt-4.1-mini", readme)
        self.assertNotIn("gpt-4.1-mini", help_html)
        self.assertNotIn("gpt-4.1-mini", index_html)

    def test_shield_docs_cover_panel_first_pack_local_editing_and_timeout_profiles(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "Actions`, `Options`, and `Exemptions`",
            "dedicated timeout override",
            "trusted-link timeout profile",
            "quick slash fallback",
        ):
            self.assertIn(text, readme)
        for text in (
            "panel-first Shield admin surface",
            "Actions, Options, and Exemptions",
            "dedicated timeout profile",
            "quick slash fallback",
        ):
            self.assertIn(text, help_html)
        for text in (
            "panel-first rules flow",
            "Actions, Options, and Exemptions",
            "dedicated timeout profile",
        ):
            self.assertIn(text, index_html)

    def test_removed_admin_control_plane_surfaces_are_absent_from_docs(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        privacy_md = (ROOT / "PRIVACY.md").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")

        for text in (
            "/admin risk",
            "/admin emergency",
            "one-role permission orchestration",
            "anti-nuke",
            "anti-raid",
            "admin_permissions_ui.py",
            "permission_orchestration.py",
            "suspicious-member",
            "member-risk",
            "member risk",
            "emergency incidents",
        ):
            self.assertNotIn(text, readme)
            self.assertNotIn(text, help_html)
            self.assertNotIn(text, index_html)
            self.assertNotIn(text, privacy_md)
            self.assertNotIn(text, privacy_html)

    def test_shield_docs_cover_trusted_link_mode_and_optional_adult_solicitation(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")

        for text in (
            "Trusted Links Only",
            "/shield links",
            "/shield trusted",
            "/shield logs",
            "/shield module",
            "/shield escalation",
            "/shield filters",
            "separate from Confessions link mode",
            "DM-ad",
        ):
            self.assertIn(text, readme)
            self.assertIn(text, help_html)
        self.assertIn("admin allowlisted domains and invite codes as policy exceptions", readme)
        self.assertIn("built-in trusted pack plus bounded domain or invite allowlists only", help_html)
        self.assertIn("built-in trusted families", help_html)
        self.assertIn("built-in trusted families", readme)
        self.assertIn("trusted-brand impersonation", readme)
        self.assertIn("trusted-brand impersonation", help_html)
        self.assertIn("phrase allowlists stay narrower", readme)
        self.assertIn("phrase allowlists suppress only targeted promo or adult-solicitation text matches", help_html)
        for text in ("solicitation carve-out", "optional solicitation"):
            self.assertIn(text, readme)
            self.assertIn(text, help_html)
        self.assertIn("solicitation carve-out channels", readme)
        self.assertIn("solicitation carve-out channels", help_html)
        for text in ("Adult Links + Solicitation", "Severe Harm / Hate", "/shield severe category", "/shield severe term"):
            self.assertIn(text, readme)
            self.assertIn(text, help_html)
        self.assertNotIn("Adult / 18+ Safety", readme)
        self.assertNotIn("Adult / 18+ Safety", help_html)

    def test_shield_docs_cover_cross_feature_immunity_boundary_and_live_ai_scope(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        help_html = (ROOT / "help.html").read_text(encoding="utf-8")
        privacy_md = (ROOT / "PRIVACY.md").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")
        index_html = (ROOT / "index.html").read_text(encoding="utf-8")

        for text in (
            "bounded cross-feature immunity layer",
            "live-message moderation remains optional",
            "AFK reasons",
            "reminder text plus public reminder delivery",
            "watch keyword",
            "Confessions unsafe-link parity",
            "live-message-only",
        ):
            self.assertIn(text, readme)
        for text in ("watch keyword setup stays privacy-only", "privacy, adult, and severe"):
            self.assertIn(text, readme)
        for text in (
            "bounded feature-surface checks",
            "The toggle controls live-message moderation",
            "live-message-only",
            "recommended non-AI baseline",
        ):
            self.assertIn(text, help_html)
        for text in ("watch keyword setup stays privacy-only", "privacy, adult, and severe", "Anti-Spam"):
            self.assertIn(text, help_html)
        for text in (
            "feature-surface checks",
            "live-message content",
            "Shield live moderation",
        ):
            self.assertIn(text, privacy_md)
            self.assertIn(text, privacy_html)
        self.assertIn("bounded private immunity", index_html)
        self.assertIn("private feature-surface checks", index_html)

    def test_privacy_docs_cover_confessions_storage_and_staff_blind_behavior(self):
        privacy_md = (ROOT / "PRIVACY.md").read_text(encoding="utf-8")
        privacy_html = (ROOT / "privacy.html").read_text(encoding="utf-8")

        for text in (
            "anonymous confessions",
            "staff-blind",
            "bot-private author mapping",
            "owner reply opportunities",
            "Anonymous Owner Reply",
            "raw attachment filenames",
            "raw Discord CDN URLs",
            "Resolved anonymous confession rows scrub previews",
            "self-identifying link destination or image content",
            "application-level encryption",
            "trust model",
            "privacy-hardening readiness state",
            "guild-scoped",
            "operator-facing warnings",
            "4000 characters",
            "Create a confession",
            "Allow All Safe",
            "reusable thread",
            "without nested threads",
        ):
            self.assertIn(text, privacy_md if text != "Resolved anonymous confession rows scrub previews" else privacy_html)
        self.assertIn("anonymous confession rows scrub previews", privacy_md.casefold())
        self.assertIn("confession ID and case ID only", privacy_html)
        self.assertIn("images are off by default", privacy_md)
        self.assertIn("Babblebox still enforces safety internally", privacy_html)
        self.assertIn("operator-proof", privacy_html)

    def test_env_example_matches_confessions_deploy_model(self):
        env_example = (ROOT / ".env.example").read_text(encoding="utf-8")
        readme = (ROOT / "README.md").read_text(encoding="utf-8")

        for text in (
            "CONFESSIONS_CONTENT_KEY",
            "CONFESSIONS_IDENTITY_KEY",
            "CONFESSIONS_CONTENT_KEY_ID",
            "CONFESSIONS_IDENTITY_KEY_ID",
            "CONFESSIONS_CONTENT_LEGACY_KEYS",
            "CONFESSIONS_IDENTITY_LEGACY_KEYS",
            "Required for Postgres-backed Confessions privacy hardening",
            "Optional but recommended active key labels",
            "Optional only during Confessions key rotation or compatibility windows",
            "python -m babblebox.confessions_backfill --dry-run",
            "python -m babblebox.confessions_backfill --apply --batch-size 100",
        ):
            self.assertIn(text, env_example)

        for text in (
            "required for Postgres-backed Confessions",
            "optional but recommended active key labels",
            "used only during key rotation or compatibility windows",
            "code deploy plus keys is not enough",
            "privacy hardening is `Ready` or `Partial`",
        ):
            self.assertIn(text, readme)

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
        self.assertIn("/dropsadmin config", readme)
        self.assertIn("/dropsadmin config", help_html)
        self.assertNotIn("/drops panel", readme)
        self.assertNotIn("/drops panel", help_html)
        self.assertNotIn("category-template", readme)
        self.assertNotIn("scholar-template", readme)
        self.assertNotIn("category-template", help_html)
        self.assertNotIn("scholar-template", help_html)
        self.assertIn("whole-number prompts", readme)

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
            "bb!shield module true",
            "bb!shield escalation 3 10 15",
            "bb!shield rules promo true log",
            "bb!admin followup true @Probation review 30d",
        ):
            self.assertIn(expected_text, readme)

    def test_removed_moment_feature_is_absent_from_docs_and_health(self):
        for path in ("README.md", "help.html", "PRIVACY.md", "privacy.html", "TERMS.md", "terms.html"):
            content = (ROOT / path).read_text(encoding="utf-8")
            self.assertNotIn("/moment", content, msg=path)
            self.assertNotIn("bb!moment", content, msg=path)
            self.assertNotIn("Moment Card", content, msg=path)
            self.assertNotIn("Moment Cards", content, msg=path)
            self.assertNotIn("Babblebox Moment", content, msg=path)

        response = app.test_client().get("/health")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertNotIn("/moment recent", payload["commands"])
