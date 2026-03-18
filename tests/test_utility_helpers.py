import unittest

from babblebox.utility_helpers import build_reminder_delivery_view, format_duration_brief, parse_duration_string


class UtilityHelperTests(unittest.TestCase):
    def test_parse_duration_string_accepts_compound_values(self):
        self.assertEqual(parse_duration_string("1h 30m"), 5400)
        self.assertEqual(parse_duration_string("1d12h"), 129600)

    def test_parse_duration_string_rejects_invalid_text(self):
        self.assertIsNone(parse_duration_string("tomorrow"))
        self.assertIsNone(parse_duration_string("5m later"))

    def test_format_duration_brief_prefers_two_units(self):
        self.assertEqual(format_duration_brief(3665), "1 hour 1 minute")

    def test_reminder_jump_view_only_exists_for_dm_guild_reminders(self):
        self.assertIsNotNone(
            build_reminder_delivery_view(
                {
                    "delivery": "dm",
                    "guild_id": 123,
                    "origin_jump_url": "https://discord.com/channels/1/2/3",
                }
            )
        )
        self.assertIsNone(
            build_reminder_delivery_view(
                {
                    "delivery": "here",
                    "guild_id": 123,
                    "origin_jump_url": "https://discord.com/channels/1/2/3",
                }
            )
        )
