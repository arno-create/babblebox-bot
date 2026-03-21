import unittest

from babblebox.shield_ai import SHIELD_AI_ALLOWED_GUILD_ID, sanitize_message_for_ai, shield_ai_available_in_guild


class ShieldAITests(unittest.TestCase):
    def test_sanitize_message_redacts_private_patterns(self):
        result = sanitize_message_for_ai(
            "Email me at friend@example.com, call +1 555 123 4567, and visit https://discord.gg/example",
            max_chars=200,
        )

        self.assertIn("[EMAIL]", result.text)
        self.assertIn("[PHONE]", result.text)
        self.assertIn("[LINK]", result.text)
        self.assertGreaterEqual(result.redaction_count, 3)

    def test_sanitize_message_truncates_safely(self):
        result = sanitize_message_for_ai("word " * 80, max_chars=80)

        self.assertTrue(result.truncated)
        self.assertLessEqual(len(result.text), 80)
        self.assertTrue(result.text.endswith("..."))

    def test_ai_guild_gate_is_explicit(self):
        self.assertTrue(shield_ai_available_in_guild(SHIELD_AI_ALLOWED_GUILD_ID))
        self.assertFalse(shield_ai_available_in_guild(10))
