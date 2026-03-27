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
        self.assertNotIn("shieldaiglobal", help_html.casefold())

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
