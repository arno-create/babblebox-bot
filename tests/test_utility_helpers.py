import unittest

from datetime import datetime, timezone
from types import SimpleNamespace

import discord

from babblebox.utility_helpers import (
    AFK_QUICK_REASONS,
    build_afk_reason_text,
    build_afk_status_embed,
    build_reminder_delivery_view,
    canonicalize_afk_timezone,
    compute_latest_afk_schedule_start,
    compute_next_afk_schedule_start,
    default_afk_weekday_mask,
    format_duration_brief,
    make_message_preview,
    parse_afk_start_at,
    parse_duration_string,
)


class DummyAttachment:
    def __init__(self, filename: str, content_type: str):
        self.filename = filename
        self.content_type = content_type
        self.url = f"https://cdn.example/{filename}"


class UtilityHelperTests(unittest.TestCase):
    def test_parse_duration_string_accepts_compound_values(self):
        self.assertEqual(parse_duration_string("1h 30m"), 5400)
        self.assertEqual(parse_duration_string("1d12h"), 129600)
        self.assertEqual(parse_duration_string("2d"), 172800)

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

    def test_afk_reason_builder_formats_quick_presets(self):
        self.assertEqual(
            build_afk_reason_text(preset="sleeping", custom_reason=None),
            f"{AFK_QUICK_REASONS['sleeping']['emoji']} Sleeping",
        )
        self.assertEqual(
            build_afk_reason_text(preset="working", custom_reason="Heads-down block"),
            f"{AFK_QUICK_REASONS['working']['emoji']} Working - Heads-down block",
        )

    def test_parse_afk_start_at_uses_saved_timezone(self):
        now = datetime(2026, 3, 22, 16, 0, tzinfo=timezone.utc)
        ok, parsed = parse_afk_start_at("23:00", timezone_name="UTC+04:00", now=now)
        self.assertTrue(ok)
        self.assertEqual(parsed, datetime(2026, 3, 22, 19, 0, tzinfo=timezone.utc))

        ok, parsed = parse_afk_start_at("tomorrow 08:30", timezone_name="UTC+04:00", now=now)
        self.assertTrue(ok)
        self.assertEqual(parsed, datetime(2026, 3, 23, 4, 30, tzinfo=timezone.utc))

        ok, parsed = parse_afk_start_at("2026-03-24 07:45", timezone_name="UTC+04:00", now=now)
        self.assertTrue(ok)
        self.assertEqual(parsed, datetime(2026, 3, 24, 3, 45, tzinfo=timezone.utc))

    def test_timezone_helpers_accept_fixed_offsets(self):
        ok, canonical, error = canonicalize_afk_timezone("utc+4")
        self.assertTrue(ok)
        self.assertEqual(canonical, "UTC+04:00")
        self.assertIsNone(error)

    def test_recurring_afk_helpers_compute_latest_and_next_occurrence(self):
        schedule = {
            "timezone": "UTC+00:00",
            "repeat": "weekdays",
            "weekday_mask": default_afk_weekday_mask("weekdays"),
            "local_hour": 18,
            "local_minute": 0,
            "created_at": "2026-03-20T12:00:00+00:00",
        }

        next_start = compute_next_afk_schedule_start(schedule, after=datetime(2026, 3, 22, 20, 0, tzinfo=timezone.utc))
        latest_start = compute_latest_afk_schedule_start(schedule, at_or_before=datetime(2026, 3, 24, 19, 0, tzinfo=timezone.utc))

        self.assertEqual(next_start, datetime(2026, 3, 23, 18, 0, tzinfo=timezone.utc))
        self.assertEqual(latest_start, datetime(2026, 3, 24, 18, 0, tzinfo=timezone.utc))

    def test_afk_status_embed_uses_reason_aware_styling(self):
        user = SimpleNamespace(display_name="Ari")
        record = {
            "status": "active",
            "reason": f"{AFK_QUICK_REASONS['studying']['emoji']} Studying - Finals tonight",
            "created_at": "2026-03-22T18:00:00+00:00",
            "set_at": "2026-03-22T18:00:00+00:00",
            "starts_at": "2026-03-22T18:00:00+00:00",
            "ends_at": "2026-03-22T20:00:00+00:00",
        }

        embed = build_afk_status_embed(user, record)

        self.assertIn(AFK_QUICK_REASONS["studying"]["emoji"], embed.title)
        self.assertEqual(embed.fields[0].name, "Status")
        self.assertIn("Studying", embed.fields[0].value)
        self.assertNotEqual(embed.color.value, 0)

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
