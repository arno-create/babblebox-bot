import json
import unittest

from babblebox.confessions_store import (
    _PostgresConfessionsStore,
    _owner_reply_opportunity_from_row,
    _submission_from_row,
    default_confession_config,
    normalize_confession_config,
    normalize_owner_reply_opportunity,
    normalize_private_media,
    normalize_submission,
)


class _FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, statement: str, *args):
        self.executed.append(statement)


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


class ConfessionsStoreNormalizationTests(unittest.TestCase):
    def test_default_config_matches_conservative_defaults(self):
        config = default_confession_config(10)
        self.assertFalse(config["enabled"])
        self.assertTrue(config["review_mode"])
        self.assertTrue(config["block_adult_language"])
        self.assertTrue(config["allow_trusted_mainstream_links"])
        self.assertFalse(config["allow_images"])
        self.assertFalse(config["allow_anonymous_replies"])
        self.assertFalse(config["allow_self_edit"])
        self.assertIsNone(config["appeals_channel_id"])
        self.assertIsNone(config["panel_channel_id"])
        self.assertIsNone(config["panel_message_id"])
        self.assertEqual(config["max_images"], 3)
        self.assertEqual(config["cooldown_seconds"], 300)
        self.assertEqual(config["burst_limit"], 3)

    def test_normalize_config_enforces_bounds_and_sorted_domains(self):
        config = normalize_confession_config(
            10,
            {
                "enabled": True,
                "review_mode": False,
                "panel_channel_id": 444,
                "panel_message_id": 555,
                "appeals_channel_id": 666,
                "custom_allow_domains": ["YouTube.com", "youtube.com", "google.com"],
                "custom_block_domains": ["bad.example", "Bad.Example"],
                "allow_images": True,
                "allow_anonymous_replies": True,
                "allow_self_edit": True,
                "max_images": 99,
                "cooldown_seconds": 1,
                "burst_limit": 20,
                "burst_window_seconds": 999999,
                "auto_suspend_hours": 0,
                "strike_temp_ban_threshold": 9,
                "strike_perm_ban_threshold": 2,
            },
        )
        self.assertEqual(config["panel_channel_id"], 444)
        self.assertEqual(config["panel_message_id"], 555)
        self.assertEqual(config["appeals_channel_id"], 666)
        self.assertEqual(config["custom_allow_domains"], ["google.com", "youtube.com"])
        self.assertEqual(config["custom_block_domains"], ["bad.example"])
        self.assertFalse(config["allow_images"])
        self.assertFalse(config["allow_anonymous_replies"])
        self.assertTrue(config["allow_self_edit"])
        self.assertEqual(config["max_images"], 3)
        self.assertEqual(config["cooldown_seconds"], 300)
        self.assertEqual(config["burst_limit"], 3)
        self.assertEqual(config["burst_window_seconds"], 1800)
        self.assertEqual(config["auto_suspend_hours"], 12)
        self.assertEqual(config["strike_temp_ban_threshold"], 9)
        self.assertEqual(config["strike_perm_ban_threshold"], 9)

    def test_normalize_config_keeps_images_enabled_only_with_private_review_channel(self):
        config = normalize_confession_config(
            10,
            {
                "enabled": True,
                "confession_channel_id": 111,
                "review_channel_id": 222,
                "allow_images": True,
                "max_images": 2,
            },
        )
        self.assertTrue(config["allow_images"])
        self.assertEqual(config["max_images"], 2)

    def test_normalize_submission_drops_binary_bloat_from_attachment_meta(self):
        record = normalize_submission(
            {
                "submission_id": "sub-1",
                "guild_id": 10,
                "confession_id": "CF-AAAA1111",
                "submission_kind": "reply",
                "parent_confession_id": "CF-ZZZZ9999",
                "status": "queued",
                "review_status": "pending",
                "staff_preview": "hello",
                "content_body": "hello",
                "shared_link_url": "https://www.google.com/search?q=hello",
                "fuzzy_signature": "abc123",
                "attachment_meta": [
                    {
                        "filename": "image.png",
                        "url": "https://cdn.discordapp.com/image.png",
                        "content_type": "image/png",
                        "kind": "image",
                        "size": 1234,
                        "bytes": b"raw-bytes-should-not-survive",
                    }
                ],
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        self.assertIsNotNone(record)
        self.assertEqual(record["submission_kind"], "reply")
        self.assertEqual(record["parent_confession_id"], "CF-ZZZZ9999")
        self.assertEqual(record["shared_link_url"], "https://www.google.com/search?q=hello")
        self.assertEqual(record["fuzzy_signature"], "abc123")
        self.assertNotIn("bytes", record["attachment_meta"][0])
        self.assertEqual(set(record["attachment_meta"][0].keys()), {"kind", "size", "width", "height", "spoiler"})

    def test_normalize_submission_defaults_reply_flow_for_reply_records_only(self):
        reply_record = normalize_submission(
            {
                "submission_id": "sub-2",
                "guild_id": 10,
                "confession_id": "CF-BBBB2222",
                "submission_kind": "reply",
                "status": "queued",
                "review_status": "pending",
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        confession_record = normalize_submission(
            {
                "submission_id": "sub-3",
                "guild_id": 10,
                "confession_id": "CF-CCCC3333",
                "submission_kind": "confession",
                "reply_flow": "owner_reply_to_user",
                "status": "queued",
                "review_status": "pending",
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )

        self.assertEqual(reply_record["reply_flow"], "reply_to_confession")
        self.assertIsNone(confession_record["reply_flow"])

    def test_normalize_owner_reply_opportunity_keeps_compact_private_state(self):
        record = normalize_owner_reply_opportunity(
            {
                "opportunity_id": "opp-1",
                "guild_id": 10,
                "root_submission_id": "sub-root",
                "root_confession_id": "CF-ROOT111",
                "referenced_submission_id": "sub-ref",
                "source_channel_id": 20,
                "source_message_id": 30,
                "source_author_name": "Responder",
                "source_preview": "Thanks for sharing",
                "status": "invalid-status",
                "notification_status": "invalid-notification",
                "created_at": "2026-04-03T00:00:00+00:00",
                "expires_at": "2026-04-06T00:00:00+00:00",
            }
        )

        self.assertEqual(record["status"], "pending")
        self.assertEqual(record["notification_status"], "none")
        self.assertEqual(record["source_author_name"], "Responder")
        self.assertEqual(record["source_preview"], "Thanks for sharing")

    def test_normalize_private_media_keeps_only_attachment_urls(self):
        record = normalize_private_media(
            {
                "guild_id": 10,
                "submission_id": "sub-1",
                "attachment_urls": [
                    "https://cdn.discordapp.com/attachments/1/2/image.png",
                    "https://evil.example/track.png",
                    " ",
                    None,
                ],
                "created_at": "2026-04-03T00:00:00+00:00",
                "updated_at": "2026-04-03T00:01:00+00:00",
            }
        )
        self.assertEqual(record["attachment_urls"], ["https://cdn.discordapp.com/attachments/1/2/image.png"])


class ConfessionsPostgresStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_submission_row_decodes_json_string_columns(self):
        row = {
            "submission_id": "sub-1",
            "guild_id": 10,
            "confession_id": "CF-AAAA1111",
            "submission_kind": "reply",
            "reply_flow": "owner_reply_to_user",
            "parent_confession_id": "CF-ZZZZ9999",
            "status": "queued",
            "review_status": "pending",
            "staff_preview": "queued preview",
            "content_body": "full text",
            "shared_link_url": "https://www.google.com/search?q=preview",
            "content_fingerprint": "abc",
            "similarity_key": "abc",
            "fuzzy_signature": "def",
            "flag_codes": json.dumps(["adult_language", "link_unsafe"]),
            "attachment_meta": json.dumps(
                [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}]
            ),
            "posted_channel_id": None,
            "posted_message_id": None,
            "current_case_id": "CS-BBBB2222",
            "created_at": "2026-04-03T00:00:00+00:00",
            "published_at": None,
            "resolved_at": None,
        }
        record = _submission_from_row(row)
        self.assertEqual(record["submission_kind"], "reply")
        self.assertEqual(record["reply_flow"], "owner_reply_to_user")
        self.assertEqual(record["parent_confession_id"], "CF-ZZZZ9999")
        self.assertEqual(record["shared_link_url"], "https://www.google.com/search?q=preview")
        self.assertEqual(record["fuzzy_signature"], "def")
        self.assertEqual(record["flag_codes"], ["adult_language", "link_unsafe"])
        self.assertEqual(record["attachment_meta"][0]["kind"], "image")

    async def test_owner_reply_opportunity_row_decodes_private_notification_fields(self):
        row = {
            "opportunity_id": "opp-1",
            "guild_id": 10,
            "root_submission_id": "sub-root",
            "root_confession_id": "CF-ROOT111",
            "referenced_submission_id": "sub-ref",
            "source_channel_id": 20,
            "source_message_id": 30,
            "source_author_name": "Responder",
            "source_preview": "Kind reply",
            "status": "pending",
            "notification_status": "sent",
            "notification_message_id": 40,
            "created_at": "2026-04-03T00:00:00+00:00",
            "expires_at": "2026-04-06T00:00:00+00:00",
            "notified_at": "2026-04-03T00:01:00+00:00",
            "resolved_at": None,
        }

        record = _owner_reply_opportunity_from_row(row)

        self.assertEqual(record["notification_status"], "sent")
        self.assertEqual(record["notification_message_id"], 40)
        self.assertEqual(record["source_preview"], "Kind reply")

    async def test_schema_bootstrap_creates_confession_tables_and_indexes(self):
        connection = _FakeConnection()
        store = _PostgresConfessionsStore("postgresql://example")
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        executed = "\n".join(connection.executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_guild_configs", executed)
        self.assertIn("allow_images BOOLEAN NOT NULL DEFAULT FALSE", executed)
        self.assertIn("allow_anonymous_replies BOOLEAN NOT NULL DEFAULT FALSE", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_submissions", executed)
        self.assertIn("reply_flow TEXT NULL", executed)
        self.assertIn("fuzzy_signature TEXT NULL", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_author_links", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_owner_reply_opportunities", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_private_media", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_enforcement_states", executed)
        self.assertIn("image_restriction_active BOOLEAN NOT NULL DEFAULT FALSE", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_cases", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_review_queues", executed)
        self.assertIn("ALTER TABLE confession_guild_configs ALTER COLUMN allow_images SET DEFAULT FALSE", executed)
        self.assertIn("ALTER TABLE confession_submissions ADD COLUMN IF NOT EXISTS reply_flow TEXT NULL", executed)
        self.assertIn("ix_confession_submissions_confession_id", executed)
        self.assertIn("ix_confession_submissions_reply_flow", executed)
        self.assertIn("ix_confession_author_links_author_created", executed)
        self.assertIn("ix_confession_owner_reply_source_message", executed)
        self.assertIn("ix_confession_owner_reply_notification_message_id", executed)
