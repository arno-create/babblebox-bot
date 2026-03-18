import unittest

from babblebox.text_safety import sanitize_short_plain_text


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
            field_name="BRB reason",
            max_length=160,
            sentence_limit=3,
            allow_empty=True,
        )
        self.assertFalse(ok)
        self.assertIn("blocked or inappropriate", message)
