import unittest

from datetime import datetime, timezone
from types import SimpleNamespace

from babblebox.utility_helpers import (
    AFK_QUICK_REASONS,
    build_afk_reason_text,
    build_capture_delivery_embed,
    build_afk_status_embed,
    build_later_marker_embed,
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

    def test_reminder_jump_view_only_exists_for_public_guild_reminders(self):
        self.assertIsNotNone(
            build_reminder_delivery_view(
                {
                    "delivery": "here",
                    "guild_id": 123,
                    "origin_jump_url": "https://discord.com/channels/1/2/3",
                },
                delivered_in_guild_channel=True,
            )
        )

    def test_message_preview_uses_media_placeholder_when_text_is_missing(self):
        preview = make_message_preview("", attachments=[DummyAttachment("clip.mp4", "video/mp4")])
        self.assertIn("[video: clip.mp4]", preview)
        self.assertIsNone(
            build_reminder_delivery_view(
                {
                    "delivery": "dm",
                    "guild_id": 123,
                    "origin_jump_url": "https://discord.com/channels/1/2/3",
                }
            )
        )

    def test_later_marker_embed_prioritizes_location_and_compact_attachments(self):
        embed = build_later_marker_embed(
            {
                "guild_name": "Guild",
                "channel_name": "clips",
                "author_name": "Ari",
                "saved_at": "2026-03-22T18:05:00+00:00",
                "message_created_at": "2026-03-22T18:00:00+00:00",
                "preview": "Line one\nMedia: [image: clip.png]",
                "attachment_labels": ["clip.png", "clip.mp4", "clip.mp3", "notes.txt"],
            }
        )

        self.assertEqual(embed.fields[0].name, "Location")
        self.assertIn("Guild / #clips", embed.fields[0].value)
        self.assertEqual(embed.fields[1].name, "Saved")
        self.assertEqual(embed.fields[2].name, "Author")
        attachments = next(field.value for field in embed.fields if field.name == "Attachments")
        self.assertIn("+1 more", attachments)

    def test_capture_delivery_embed_uses_clearer_button_label(self):
        embed, view = build_capture_delivery_embed(
            guild_name="Guild",
            channel_name="general",
            captured_count=6,
            requested_count=8,
            preview_lines=["[12:00] Ari: First line", "[12:01] Mira: Second line"],
            jump_url="https://discord.com/channels/1/2/3",
        )

        self.assertEqual(embed.title, "Capture Ready")
        self.assertEqual(embed.fields[0].name, "Source")
        self.assertEqual(embed.fields[1].name, "Privacy")
        self.assertEqual(embed.fields[2].name, "Latest Messages")
        self.assertIsNotNone(view)
        self.assertEqual(view.children[0].label, "Back to Channel")

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

