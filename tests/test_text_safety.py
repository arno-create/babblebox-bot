import unittest

from babblebox.text_safety import (
    is_harmful_context_suppressed,
    is_reporting_or_educational_context,
    sanitize_short_plain_text,
)


class TextSafetyTests(unittest.TestCase):
    def test_accepts_plain_text(self):
        ok, cleaned = sanitize_short_plain_text(
            "Back in a few minutes.",
            field_name="AFK reason",
            max_length=160,
            sentence_limit=3,
        )
        self.assertTrue(ok)
        self.assertEqual(cleaned, "Back in a few minutes.")

    def test_rejects_links(self):
        ok, message = sanitize_short_plain_text(
            "check https://example.com",
            field_name="Reminder text",
            max_length=200,
            sentence_limit=4,
            allow_empty=False,
        )
        self.assertFalse(ok)
        self.assertIn("cannot contain links or invites", message)

    def test_rejects_blocklisted_terms(self):
        ok, message = sanitize_short_plain_text(
            "this is nsfw",
            field_name="AFK reason",
            max_length=160,
            sentence_limit=3,
            allow_empty=True,
        )
        self.assertFalse(ok)
        self.assertIn("blocked or inappropriate", message)

    def test_reporting_or_educational_context_recognizes_moderation_variants(self):
        for text in (
            "report this person for saying kill yourself",
            "moderation note: user said dm me for nudes",
            "mods deleted you retard spam",
            "sexual health workshop tomorrow",
        ):
            with self.subTest(text=text):
                self.assertTrue(is_reporting_or_educational_context(text))

    def test_harmful_context_suppression_handles_reporting_and_disapproval_variants(self):
        for text in (
            "report this person for saying kill yourself",
            "moderation note: user said dm me for nudes",
            "mods deleted you retard spam",
            "please do not call people retard",
            "please do not tell people to kill yourself",
        ):
            with self.subTest(text=text):
                self.assertTrue(is_harmful_context_suppressed(text, include_disapproval=True))

        self.assertFalse(is_harmful_context_suppressed("selling nudes in DMs", include_disapproval=True))
