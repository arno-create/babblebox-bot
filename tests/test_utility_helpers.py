import unittest

from datetime import datetime, timezone
from types import SimpleNamespace

import discord

from babblebox.utility_helpers import build_reminder_delivery_view, format_duration_brief, make_message_preview, parse_duration_string


class DummyAttachment:
    def __init__(self, filename: str, content_type: str):
        self.filename = filename
        self.content_type = content_type
        self.url = f"https://cdn.example/{filename}"


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

    def test_message_preview_uses_media_placeholder_when_text_is_missing(self):
        preview = make_message_preview("", attachments=[DummyAttachment("clip.mp4", "video/mp4")])
        self.assertIn("[video: clip.mp4]", preview)
        self.assertIsNone(
            build_reminder_delivery_view(
                {
                    "delivery": "here",
                    "guild_id": 123,
                    "origin_jump_url": "https://discord.com/channels/1/2/3",
                }
            )
        )

    def test_moment_card_embed_uses_clean_scene_and_echo_labels(self):
        created_at = datetime(2026, 3, 21, 9, 15, tzinfo=timezone.utc)
        author = SimpleNamespace(
            id=1,
            display_name="Mira",
            color=discord.Color.blue(),
            display_avatar=SimpleNamespace(url="https://cdn.example/avatar.png"),
        )
        followup_author = SimpleNamespace(
            id=2,
            display_name="Noah",
            color=discord.Color.gold(),
            display_avatar=SimpleNamespace(url="https://cdn.example/avatar2.png"),
        )
        message = SimpleNamespace(
            content="That line was way funnier than expected.",
            attachments=[],
            author=author,
            channel=SimpleNamespace(mention="#clips"),
            guild=SimpleNamespace(name="Babblebox HQ"),
            created_at=created_at,
        )
        followup = SimpleNamespace(
            content="I am saving this one.",
            attachments=[],
            author=followup_author,
            channel=message.channel,
            guild=message.guild,
            created_at=created_at,
        )

        from babblebox.utility_helpers import build_moment_card_embed

        embed = build_moment_card_embed(message, followup=followup)

        self.assertEqual(embed.fields[0].name, "Scene")
        self.assertIn("Babblebox HQ | #clips", embed.fields[0].value)
        self.assertEqual(embed.fields[1].name, "Echo | Noah")
