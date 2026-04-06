import json
import unittest
from unittest import mock

from babblebox.confessions_crypto import ConfessionsCrypto
from babblebox.confessions_store import (
    PROTECTED_OWNER_REPLY_PREVIEW,
    ConfessionsStorageUnavailable,
    ConfessionsStore,
    _PostgresConfessionsStore,
    _owner_reply_opportunity_from_row,
    _submission_from_row,
    default_confession_config,
    normalize_confession_config,
    normalize_owner_reply_opportunity,
    normalize_private_media,
    normalize_submission,
)


def _privacy() -> ConfessionsCrypto:
    return ConfessionsCrypto.from_environment(backend_name="test")


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
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_results: list[list[object]] = []
        self.fetchrow_results: list[object] = []

    async def execute(self, statement: str, *args):
        self.executed.append(statement)
        self.execute_calls.append((statement, args))
        return "EXECUTE"

    async def fetch(self, statement: str, *args):
        self.fetch_calls.append((statement, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return []

    async def fetchrow(self, statement: str, *args):
        self.fetchrow_calls.append((statement, args))
        if self.fetchrow_results:
            return self.fetchrow_results.pop(0)
        return None


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)

    async def close(self):
        return None


class ConfessionsStoreNormalizationTests(unittest.TestCase):
    def test_default_config_matches_conservative_defaults(self):
        config = default_confession_config(10)
        self.assertFalse(config["enabled"])
        self.assertTrue(config["review_mode"])
        self.assertTrue(config["block_adult_language"])
        self.assertTrue(config["allow_trusted_mainstream_links"])
        self.assertFalse(config["allow_images"])
        self.assertFalse(config["allow_anonymous_replies"])
        self.assertTrue(config["allow_owner_replies"])
        self.assertFalse(config["owner_reply_review_mode"])
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
                "allow_owner_replies": False,
                "owner_reply_review_mode": True,
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
        self.assertFalse(config["allow_owner_replies"])
        self.assertFalse(config["owner_reply_review_mode"])
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
                "fuzzy_signature": "fh1:abc123",
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
        self.assertEqual(record["fuzzy_signature"], "fh1:abc123")
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
        self.assertEqual(confession_record["owner_reply_generation"], None)
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
                "source_author_user_id": 40,
                "source_author_name": "Responder",
                "source_preview": "Thanks for sharing",
                "source_message_fingerprint": "abc123",
                "notification_channel_id": 50,
                "status": "invalid-status",
                "notification_status": "invalid-notification",
                "created_at": "2026-04-03T00:00:00+00:00",
                "expires_at": "2026-04-06T00:00:00+00:00",
            }
        )
        self.assertEqual(record["status"], "pending")
        self.assertEqual(record["notification_status"], "none")
        self.assertEqual(record["source_author_user_id"], 40)
        self.assertEqual(record["source_author_name"], "Responder")
        self.assertEqual(record["source_preview"], "Thanks for sharing")
        self.assertEqual(record["source_message_fingerprint"], "abc123")
        self.assertEqual(record["notification_channel_id"], 50)

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


class ConfessionsStorePrivacyTests(unittest.IsolatedAsyncioTestCase):
    async def test_submission_row_decrypts_ciphertext_without_leaking_raw_columns(self):
        privacy = _privacy()
        row = {
            "submission_id": "sub-1",
            "guild_id": 10,
            "confession_id": "CF-AAAA1111",
            "submission_kind": "reply",
            "reply_flow": "owner_reply_to_user",
            "owner_reply_generation": 2,
            "parent_confession_id": "CF-ZZZZ9999",
            "status": "queued",
            "review_status": "pending",
            "staff_preview": None,
            "content_body": None,
            "shared_link_url": None,
            "content_ciphertext": privacy.encrypt_payload(
                domain="submission-content",
                aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
                payload={
                    "staff_preview": "queued preview",
                    "content_body": "full text",
                    "shared_link_url": "https://www.google.com/search?q=preview",
                },
                key_domain="content",
            ),
            "content_fingerprint": "h1:abc",
            "similarity_key": None,
            "fuzzy_signature": "fh1:def",
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
        record = _submission_from_row(row, privacy)
        self.assertEqual(record["content_body"], "full text")
        self.assertEqual(record["shared_link_url"], "https://www.google.com/search?q=preview")
        self.assertEqual(record["fuzzy_signature"], "fh1:def")
        self.assertEqual(record["flag_codes"], ["adult_language", "link_unsafe"])
        self.assertEqual(record["attachment_meta"][0]["kind"], "image")

    async def test_owner_reply_opportunity_row_decrypts_private_payload(self):
        privacy = _privacy()
        row = {
            "opportunity_id": "opp-1",
            "guild_id": 10,
            "root_submission_id": "sub-root",
            "root_confession_id": "CF-ROOT111",
            "referenced_submission_id": "sub-ref",
            "source_channel_id": 20,
            "source_message_id": 30,
            "source_author_user_id": None,
            "source_author_lookup_hash": privacy.blind_index(label="owner-reply-source-author", guild_id=10, value=40),
            "source_author_name": "Protected member",
            "source_preview": PROTECTED_OWNER_REPLY_PREVIEW,
            "source_message_fingerprint": None,
            "private_payload": privacy.encrypt_payload(
                domain="owner-reply-opportunity",
                aad_fields={"guild_id": 10, "opportunity_id": "opp-1", "root_submission_id": "sub-root"},
                payload={
                    "source_author_user_id": 40,
                    "source_author_name": "Responder",
                    "source_preview": "Kind reply",
                    "source_message_fingerprint": "abc123",
                },
                key_domain="content",
            ),
            "status": "pending",
            "notification_status": "sent",
            "notification_message_id": 40,
            "created_at": "2026-04-03T00:00:00+00:00",
            "expires_at": "2026-04-06T00:00:00+00:00",
            "notified_at": "2026-04-03T00:01:00+00:00",
            "resolved_at": None,
        }
        record = _owner_reply_opportunity_from_row(row, privacy)
        self.assertEqual(record["notification_status"], "sent")
        self.assertEqual(record["notification_message_id"], 40)
        self.assertEqual(record["source_author_user_id"], 40)
        self.assertEqual(record["source_preview"], "Kind reply")

    async def test_schema_bootstrap_creates_secure_confession_tables_and_indexes(self):
        connection = _FakeConnection()
        store = _PostgresConfessionsStore("postgresql://example", _privacy())
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        executed = "\n".join(connection.executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_guild_configs", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_submissions", executed)
        self.assertIn("content_ciphertext TEXT NULL", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_author_links", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_author_identities", executed)
        self.assertIn("author_lookup_hash TEXT NOT NULL", executed)
        self.assertIn("author_identity_ciphertext TEXT NOT NULL", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_private_media", executed)
        self.assertIn("attachment_payload TEXT NULL", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_owner_reply_opportunities", executed)
        self.assertIn("source_author_lookup_hash TEXT NULL", executed)
        self.assertIn("private_payload TEXT NULL", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_enforcement_states", executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS confession_enforcement_states_secure", executed)
        self.assertIn("user_lookup_hash TEXT NOT NULL", executed)
        self.assertIn("user_identity_ciphertext TEXT NOT NULL", executed)
        self.assertIn("ix_confession_author_identities_lookup_created", executed)
        self.assertIn("ix_confession_enforcement_states_secure_lookup", executed)
        self.assertIn("ix_confession_owner_reply_responder_path_lookup_status", executed)

    async def test_memory_store_backfill_encrypts_sensitive_rows_and_preserves_lookup_flows(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            raw_store = store._store
            raw_store.submissions["sub-active"] = {
                "submission_id": "sub-active",
                "guild_id": 10,
                "confession_id": "CF-ACTIVE1",
                "submission_kind": "confession",
                "reply_flow": None,
                "owner_reply_generation": None,
                "parent_confession_id": None,
                "status": "queued",
                "review_status": "pending",
                "staff_preview": "Legacy preview",
                "content_body": "Legacy queued confession body",
                "shared_link_url": "https://www.google.com/search?q=queued",
                "content_fingerprint": "legacy-fingerprint",
                "similarity_key": "legacy similarity key",
                "fuzzy_signature": "deadbeefdeadbeef",
                "flag_codes": [],
                "attachment_meta": [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}],
                "posted_channel_id": None,
                "posted_message_id": None,
                "current_case_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": None,
                "resolved_at": None,
            }
            raw_store.submissions["sub-terminal"] = {
                "submission_id": "sub-terminal",
                "guild_id": 10,
                "confession_id": "CF-DONE111",
                "submission_kind": "confession",
                "reply_flow": None,
                "owner_reply_generation": None,
                "parent_confession_id": None,
                "status": "published",
                "review_status": "approved",
                "staff_preview": "Old published preview",
                "content_body": "Old published body",
                "shared_link_url": "https://www.google.com/search?q=done",
                "content_fingerprint": "legacy-fingerprint-2",
                "similarity_key": "legacy similarity key 2",
                "fuzzy_signature": "feedfacefeedface",
                "flag_codes": [],
                "attachment_meta": [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}],
                "posted_channel_id": 20,
                "posted_message_id": 30,
                "current_case_id": None,
                "created_at": "2026-04-02T00:00:00+00:00",
                "published_at": "2026-04-02T00:05:00+00:00",
                "resolved_at": None,
            }
            raw_store.private_media["sub-active"] = {
                "submission_id": "sub-active",
                "guild_id": 10,
                "attachment_urls": ["https://cdn.discordapp.com/attachments/1/2/image.png"],
                "created_at": "2026-04-03T00:00:00+00:00",
                "updated_at": "2026-04-03T00:01:00+00:00",
            }
            raw_store.private_media["sub-terminal"] = {
                "submission_id": "sub-terminal",
                "guild_id": 10,
                "attachment_urls": ["https://cdn.discordapp.com/attachments/1/2/old.png"],
                "created_at": "2026-04-02T00:00:00+00:00",
                "updated_at": "2026-04-02T00:01:00+00:00",
            }
            raw_store.author_links["sub-active"] = {
                "submission_id": "sub-active",
                "guild_id": 10,
                "author_user_id": 40,
                "created_at": "2026-04-03T00:00:00+00:00",
            }
            raw_store.owner_reply_opportunities["opp-1"] = {
                "opportunity_id": "opp-1",
                "guild_id": 10,
                "root_submission_id": "sub-active",
                "root_confession_id": "CF-ACTIVE1",
                "referenced_submission_id": "sub-active",
                "source_channel_id": 20,
                "source_message_id": 31,
                "source_author_user_id": 40,
                "source_author_name": "Responder",
                "source_preview": "Reply preview",
                "source_message_fingerprint": "fingerprint",
                "status": "pending",
                "notification_status": "none",
                "notification_channel_id": None,
                "notification_message_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "expires_at": "2026-04-06T00:00:00+00:00",
                "notified_at": None,
                "resolved_at": None,
            }
            raw_store.enforcement_states[(10, 40)] = {
                "guild_id": 10,
                "user_id": 40,
                "active_restriction": "suspended",
                "restricted_until": "2026-04-04T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 2,
                "last_strike_at": "2026-04-03T00:00:00+00:00",
                "cooldown_until": None,
                "burst_count": 1,
                "burst_window_started_at": "2026-04-03T00:00:00+00:00",
                "last_case_id": "CS-STATE01",
                "image_restriction_active": False,
                "image_restricted_until": None,
                "image_restriction_case_id": None,
                "updated_at": "2026-04-03T00:00:00+00:00",
            }

            dry_run = await store.run_privacy_backfill(apply=False, batch_size=10)
            self.assertEqual(dry_run["submissions"], 2)
            self.assertEqual(dry_run["private_media"], 2)
            self.assertEqual(dry_run["author_links"], 1)
            self.assertEqual(dry_run["owner_reply_opportunities"], 1)
            self.assertEqual(dry_run["enforcement_states"], 1)
            self.assertEqual(dry_run["privacy_status"]["state"], "partial")
            self.assertIn("plaintext_submission_content", dry_run["privacy_status"]["categories"])

            applied = await store.run_privacy_backfill(apply=True, batch_size=10)
            self.assertEqual(applied["mode"], "apply")
            self.assertEqual(applied["privacy_status"]["state"], "ready")

            active_submission = await store.fetch_submission("sub-active")
            self.assertEqual(active_submission["content_body"], "Legacy queued confession body")
            self.assertEqual(active_submission["shared_link_url"], "https://www.google.com/search?q=queued")
            active_private_media = await store.fetch_private_media("sub-active")
            self.assertEqual(active_private_media["attachment_urls"], ["https://cdn.discordapp.com/attachments/1/2/image.png"])
            author_link = await store.fetch_author_link("sub-active")
            self.assertEqual(author_link["author_user_id"], 40)
            owner_reply = await store.fetch_owner_reply_opportunity("opp-1")
            self.assertEqual(owner_reply["source_author_user_id"], 40)
            state = await store.fetch_enforcement_state(10, 40)
            self.assertEqual(state["user_id"], 40)
            recent = await store.list_recent_submissions_for_author(10, 40, limit=5)
            self.assertEqual([row["submission_id"] for row in recent], ["sub-active"])

            raw_active_submission = raw_store.submissions["sub-active"]
            self.assertIsNone(raw_active_submission["staff_preview"])
            self.assertIsNone(raw_active_submission["content_body"])
            self.assertIsNone(raw_active_submission["shared_link_url"])
            self.assertIsNone(raw_active_submission["similarity_key"])
            self.assertTrue(str(raw_active_submission["content_ciphertext"]).startswith("bbx2:ephemeral:"))
            self.assertTrue(str(raw_active_submission["content_fingerprint"]).startswith("h2:ephemeral:"))
            self.assertTrue(str(raw_active_submission["fuzzy_signature"]).startswith("fh2:ephemeral:"))

            raw_terminal_submission = raw_store.submissions["sub-terminal"]
            self.assertIsNone(raw_terminal_submission["staff_preview"])
            self.assertIsNone(raw_terminal_submission["content_body"])
            self.assertIsNone(raw_terminal_submission["shared_link_url"])
            self.assertIsNone(raw_terminal_submission["content_ciphertext"])
            self.assertIsNone(raw_terminal_submission["similarity_key"])
            self.assertEqual(raw_terminal_submission["attachment_meta"], [])
            self.assertNotIn("sub-terminal", raw_store.private_media)

            raw_private_media = raw_store.private_media["sub-active"]
            self.assertEqual(raw_private_media["attachment_urls"], [])
            self.assertTrue(str(raw_private_media["attachment_payload"]).startswith("bbx2:ephemeral:"))

            self.assertEqual(raw_store.author_links, {})
            secure_author_link = raw_store.secure_author_links["sub-active"]
            self.assertNotIn("author_user_id", secure_author_link)
            self.assertTrue(str(secure_author_link["author_lookup_hash"]).startswith("bi2:ephemeral:"))
            self.assertTrue(str(secure_author_link["author_identity_ciphertext"]).startswith("bbx2:ephemeral:"))

            raw_owner_reply = raw_store.owner_reply_opportunities["opp-1"]
            self.assertIsNone(raw_owner_reply["source_author_user_id"])
            self.assertEqual(raw_owner_reply["source_preview"], PROTECTED_OWNER_REPLY_PREVIEW)
            self.assertTrue(str(raw_owner_reply["source_author_lookup_hash"]).startswith("bi2:ephemeral:"))
            self.assertTrue(str(raw_owner_reply["private_payload"]).startswith("bbx2:ephemeral:"))

            self.assertEqual(raw_store.enforcement_states, {})
            secure_enforcement = next(iter(raw_store.secure_enforcement_states.values()))
            self.assertNotIn("user_id", secure_enforcement)
            self.assertTrue(str(secure_enforcement["user_lookup_hash"]).startswith("bi2:ephemeral:"))
            self.assertTrue(str(secure_enforcement["user_identity_ciphertext"]).startswith("bbx2:ephemeral:"))
        finally:
            await store.close()

    async def test_memory_store_privacy_status_reports_partial_then_ready(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            raw_store = store._store
            raw_store.submissions["sub-legacy"] = {
                "submission_id": "sub-legacy",
                "guild_id": 77,
                "confession_id": "CF-LEGACY1",
                "submission_kind": "confession",
                "reply_flow": None,
                "owner_reply_generation": None,
                "parent_confession_id": None,
                "status": "queued",
                "review_status": "pending",
                "staff_preview": "Legacy preview",
                "content_body": "Legacy body",
                "shared_link_url": None,
                "content_fingerprint": "legacy-fingerprint",
                "similarity_key": "legacy similarity key",
                "fuzzy_signature": "feedfacefeedface",
                "flag_codes": [],
                "attachment_meta": [],
                "posted_channel_id": None,
                "posted_message_id": None,
                "current_case_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": None,
                "resolved_at": None,
            }
            raw_store.author_links["sub-legacy"] = {
                "submission_id": "sub-legacy",
                "guild_id": 77,
                "author_user_id": 400,
                "created_at": "2026-04-03T00:00:00+00:00",
            }

            status = await store.fetch_privacy_status(77)
            self.assertEqual(status["state"], "partial")
            self.assertIn("plaintext_submission_content", status["categories"])
            self.assertIn("legacy_author_links", status["categories"])

            await store.run_privacy_backfill(apply=True, batch_size=10)

            status = await store.fetch_privacy_status(77)
            self.assertEqual(status["state"], "ready")
            self.assertEqual(status["categories"], [])
        finally:
            await store.close()

    async def test_memory_store_supports_legacy_key_lookup_then_rewrites_to_active_keys(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            old_only = ConfessionsCrypto(
                content_keys=[("old", b"o" * 32)],
                identity_keys=[("old", b"i" * 32)],
                content_source="test",
                identity_source="test",
                ephemeral=False,
            )
            rotated = ConfessionsCrypto(
                content_keys=[("current", b"c" * 32), ("old", b"o" * 32)],
                identity_keys=[("current", b"n" * 32), ("old", b"i" * 32)],
                content_source="test",
                identity_source="test",
                ephemeral=False,
            )
            raw_store = store._store
            raw_store._privacy = rotated
            store.privacy = rotated
            raw_store.submissions["sub-rotated"] = {
                "submission_id": "sub-rotated",
                "guild_id": 10,
                "confession_id": "CF-ROTATE1",
                "submission_kind": "confession",
                "reply_flow": None,
                "owner_reply_generation": None,
                "parent_confession_id": None,
                "status": "published",
                "review_status": "approved",
                "staff_preview": None,
                "content_body": None,
                "shared_link_url": None,
                "content_ciphertext": None,
                "content_fingerprint": None,
                "similarity_key": None,
                "fuzzy_signature": None,
                "flag_codes": [],
                "attachment_meta": [],
                "posted_channel_id": 20,
                "posted_message_id": 30,
                "current_case_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": "2026-04-03T00:05:00+00:00",
                "resolved_at": None,
            }
            raw_store.secure_author_links["sub-rotated"] = {
                "submission_id": "sub-rotated",
                "guild_id": 10,
                "author_lookup_hash": old_only.blind_index(label="author-link", guild_id=10, value=400),
                "author_identity_ciphertext": old_only.encrypt_payload(
                    domain="author-link",
                    aad_fields={"guild_id": 10, "submission_id": "sub-rotated"},
                    payload={"author_user_id": 400},
                    key_domain="identity",
                ),
                "created_at": "2026-04-03T00:00:00+00:00",
            }

            link = await store.fetch_author_link("sub-rotated")
            self.assertEqual(link["author_user_id"], 400)
            recent = await store.list_recent_submissions_for_author(10, 400, limit=5)
            self.assertEqual([row["submission_id"] for row in recent], ["sub-rotated"])

            status = await store.fetch_privacy_status(10)
            self.assertEqual(status["state"], "partial")
            self.assertIn("stale_key_rows", status["categories"])

            await store.run_privacy_backfill(apply=True, batch_size=10)

            status = await store.fetch_privacy_status(10)
            self.assertEqual(status["state"], "ready")
            self.assertTrue(
                str(raw_store.secure_author_links["sub-rotated"]["author_lookup_hash"]).startswith("bi2:current:")
            )
            self.assertTrue(
                str(raw_store.secure_author_links["sub-rotated"]["author_identity_ciphertext"]).startswith("bbx2:current:")
            )
        finally:
            await store.close()

    async def test_memory_store_uses_ephemeral_keys(self):
        store = ConfessionsStore(backend="memory")
        try:
            self.assertTrue(store.privacy.status.ephemeral)
        finally:
            await store.close()

    async def test_postgres_backend_requires_privacy_keys(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(ConfessionsStorageUnavailable):
                ConfessionsStore(backend="postgres", database_url="postgresql://example")


class PostgresConfessionsStoreTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.connection = _FakeConnection()
        self.store = _PostgresConfessionsStore("postgresql://example", _privacy())
        self.store._pool = _FakePool(self.connection)

    async def asyncTearDown(self):
        await self.store.close()

    async def test_postgres_store_upsert_config_includes_admin_runtime_fields(self):
        await self.store.upsert_config(
            {
                "guild_id": 10,
                "enabled": True,
                "confession_channel_id": 20,
                "panel_channel_id": 40,
                "panel_message_id": 50,
                "review_channel_id": 30,
                "appeals_channel_id": 60,
                "review_mode": False,
                "block_adult_language": True,
                "allow_trusted_mainstream_links": True,
                "custom_allow_domains": ["google.com"],
                "custom_block_domains": ["bad.example"],
                "allowed_role_ids": [501],
                "blocked_role_ids": [502],
                "allow_images": True,
                "allow_anonymous_replies": True,
                "allow_owner_replies": True,
                "owner_reply_review_mode": True,
                "allow_self_edit": True,
                "max_images": 2,
                "cooldown_seconds": 600,
                "burst_limit": 2,
                "burst_window_seconds": 900,
                "auto_suspend_hours": 24,
                "strike_temp_ban_threshold": 3,
                "temp_ban_days": 14,
                "strike_perm_ban_threshold": 5,
            }
        )

        statement, args = self.connection.execute_calls[-1]
        self.assertIn("panel_message_id", statement)
        self.assertIn("appeals_channel_id", statement)
        self.assertIn("owner_reply_review_mode", statement)
        self.assertEqual(args[0], 10)
        self.assertEqual(args[3], 40)
        self.assertEqual(args[4], 50)
        self.assertEqual(args[6], 60)
        self.assertEqual(json.loads(args[10]), ["google.com"])
        self.assertEqual(json.loads(args[11]), ["bad.example"])
        self.assertEqual(json.loads(args[12]), [501])
        self.assertEqual(json.loads(args[13]), [502])
        self.assertTrue(args[14])
        self.assertTrue(args[15])
        self.assertTrue(args[16])
        self.assertTrue(args[17])

    async def test_postgres_store_fetch_config_round_trips_admin_runtime_fields(self):
        self.connection.fetchrow_results.append(
            {
                "guild_id": 10,
                "enabled": True,
                "confession_channel_id": 20,
                "panel_channel_id": 40,
                "panel_message_id": 50,
                "review_channel_id": 30,
                "appeals_channel_id": 60,
                "review_mode": False,
                "block_adult_language": True,
                "allow_trusted_mainstream_links": True,
                "custom_allow_domains": json.dumps(["google.com"]),
                "custom_block_domains": json.dumps(["bad.example"]),
                "allowed_role_ids": json.dumps([501]),
                "blocked_role_ids": json.dumps([502]),
                "allow_images": True,
                "allow_anonymous_replies": True,
                "allow_owner_replies": True,
                "owner_reply_review_mode": True,
                "allow_self_edit": True,
                "max_images": 2,
                "cooldown_seconds": 600,
                "burst_limit": 2,
                "burst_window_seconds": 900,
                "auto_suspend_hours": 24,
                "strike_temp_ban_threshold": 3,
                "temp_ban_days": 14,
                "strike_perm_ban_threshold": 5,
            }
        )

        config = await self.store.fetch_config(10)

        self.assertEqual(config["panel_channel_id"], 40)
        self.assertEqual(config["panel_message_id"], 50)
        self.assertEqual(config["appeals_channel_id"], 60)
        self.assertEqual(config["allowed_role_ids"], [501])
        self.assertEqual(config["blocked_role_ids"], [502])
        self.assertTrue(config["allow_images"])
        self.assertTrue(config["allow_anonymous_replies"])
        self.assertTrue(config["allow_owner_replies"])
        self.assertTrue(config["owner_reply_review_mode"])
        self.assertTrue(config["allow_self_edit"])

    async def test_postgres_store_fetch_all_configs_round_trips_admin_runtime_fields(self):
        self.connection.fetch_results.append(
            [
                {
                    "guild_id": 10,
                    "enabled": True,
                    "confession_channel_id": 20,
                    "panel_channel_id": 40,
                    "panel_message_id": 50,
                    "review_channel_id": 30,
                    "appeals_channel_id": 60,
                    "review_mode": False,
                    "block_adult_language": True,
                    "allow_trusted_mainstream_links": True,
                    "custom_allow_domains": json.dumps(["google.com"]),
                    "custom_block_domains": json.dumps(["bad.example"]),
                    "allowed_role_ids": json.dumps([501]),
                    "blocked_role_ids": json.dumps([502]),
                    "allow_images": True,
                    "allow_anonymous_replies": True,
                    "allow_owner_replies": True,
                    "owner_reply_review_mode": True,
                    "allow_self_edit": True,
                    "max_images": 2,
                    "cooldown_seconds": 600,
                    "burst_limit": 2,
                    "burst_window_seconds": 900,
                    "auto_suspend_hours": 24,
                    "strike_temp_ban_threshold": 3,
                    "temp_ban_days": 14,
                    "strike_perm_ban_threshold": 5,
                },
                {
                    "guild_id": 11,
                    "enabled": False,
                    "confession_channel_id": 21,
                    "panel_channel_id": None,
                    "panel_message_id": None,
                    "review_channel_id": None,
                    "appeals_channel_id": None,
                    "review_mode": True,
                    "block_adult_language": True,
                    "allow_trusted_mainstream_links": True,
                    "custom_allow_domains": json.dumps([]),
                    "custom_block_domains": json.dumps([]),
                    "allowed_role_ids": json.dumps([]),
                    "blocked_role_ids": json.dumps([]),
                    "allow_images": False,
                    "allow_anonymous_replies": False,
                    "allow_owner_replies": True,
                    "owner_reply_review_mode": False,
                    "allow_self_edit": False,
                    "max_images": 1,
                    "cooldown_seconds": 300,
                    "burst_limit": 3,
                    "burst_window_seconds": 1800,
                    "auto_suspend_hours": 12,
                    "strike_temp_ban_threshold": 3,
                    "temp_ban_days": 7,
                    "strike_perm_ban_threshold": 5,
                },
            ]
        )

        configs = await self.store.fetch_all_configs()

        self.assertEqual(sorted(configs), [10, 11])
        self.assertEqual(configs[10]["panel_message_id"], 50)
        self.assertEqual(configs[10]["appeals_channel_id"], 60)
        self.assertEqual(configs[11]["confession_channel_id"], 21)
        self.assertEqual(configs[11]["allowed_role_ids"], [])

    async def test_postgres_store_review_queue_round_trip_methods(self):
        await self.store.upsert_review_queue(
            {
                "guild_id": 10,
                "channel_id": 30,
                "message_id": 40,
                "updated_at": "2026-04-03T00:00:00+00:00",
            }
        )

        statement, args = self.connection.execute_calls[-1]
        self.assertIn("confession_review_queues", statement)
        self.assertEqual(args[0], 10)
        self.assertEqual(args[1], 30)
        self.assertEqual(args[2], 40)
        self.assertIsNotNone(args[3])

        self.connection.fetchrow_results.append(
            {
                "guild_id": 10,
                "channel_id": 30,
                "message_id": 40,
                "updated_at": "2026-04-03T00:00:00+00:00",
            }
        )
        record = await self.store.fetch_review_queue(10)
        self.assertEqual(record["channel_id"], 30)
        self.assertEqual(record["message_id"], 40)

        self.connection.fetch_results.append(
            [
                {
                    "guild_id": 10,
                    "channel_id": 30,
                    "message_id": 40,
                    "updated_at": "2026-04-03T00:00:00+00:00",
                },
                {
                    "guild_id": 11,
                    "channel_id": 31,
                    "message_id": 41,
                    "updated_at": "2026-04-03T00:05:00+00:00",
                },
            ]
        )
        rows = await self.store.list_review_queues()
        self.assertEqual([row["guild_id"] for row in rows], [10, 11])

    async def test_postgres_store_privacy_status_accepts_current_row_shapes(self):
        privacy = _privacy()
        submission_ciphertext = privacy.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"staff_preview": "Preview", "content_body": "Body"},
            key_domain="content",
        )
        private_media_payload = privacy.encrypt_payload(
            domain="private-media",
            aad_fields={"guild_id": 10, "submission_id": "sub-1"},
            payload={"attachment_urls": ["https://cdn.discordapp.com/attachments/1/2/image.png"]},
            key_domain="content",
        )
        author_lookup_hash = privacy.blind_index(label="author-link", guild_id=10, value=400)
        author_ciphertext = privacy.encrypt_payload(
            domain="author-link",
            aad_fields={"guild_id": 10, "submission_id": "sub-1"},
            payload={"author_user_id": 400},
            key_domain="identity",
        )
        source_lookup_hash = privacy.blind_index(label="owner-reply-source-author", guild_id=10, value=401)
        owner_reply_payload = privacy.encrypt_payload(
            domain="owner-reply-opportunity",
            aad_fields={"guild_id": 10, "opportunity_id": "opp-1", "root_submission_id": "sub-root"},
            payload={"source_author_user_id": 401, "source_author_name": "Responder", "source_preview": "Preview"},
            key_domain="content",
        )
        enforcement_lookup_hash = privacy.blind_index(label="enforcement-state", guild_id=10, value=402)
        enforcement_ciphertext = privacy.encrypt_payload(
            domain="enforcement-state",
            aad_fields={"guild_id": 10, "user_lookup_hash": enforcement_lookup_hash},
            payload={"user_id": 402},
            key_domain="identity",
        )
        self.connection.fetch_results = [
            [
                {
                    "guild_id": 10,
                    "staff_preview": None,
                    "content_body": None,
                    "shared_link_url": None,
                    "content_ciphertext": submission_ciphertext,
                    "content_fingerprint": privacy.exact_duplicate_hash("hello", guild_id=10),
                    "similarity_key": None,
                    "fuzzy_signature": privacy.fuzzy_duplicate_signature(["hello"], guild_id=10),
                }
            ],
            [
                {
                    "guild_id": 10,
                    "attachment_urls": "[]",
                    "attachment_payload": private_media_payload,
                }
            ],
            [
                {
                    "guild_id": 10,
                    "author_lookup_hash": author_lookup_hash,
                    "author_identity_ciphertext": author_ciphertext,
                }
            ],
            [],
            [
                {
                    "guild_id": 10,
                    "source_author_user_id": None,
                    "source_author_lookup_hash": source_lookup_hash,
                    "source_author_name": None,
                    "source_preview": None,
                    "source_message_fingerprint": None,
                    "private_payload": owner_reply_payload,
                }
            ],
            [
                {
                    "guild_id": 10,
                    "user_lookup_hash": enforcement_lookup_hash,
                    "user_identity_ciphertext": enforcement_ciphertext,
                }
            ],
            [],
        ]

        status = await self.store.fetch_privacy_status(10)

        self.assertEqual(status["scope"], "guild")
        self.assertEqual(status["guild_id"], 10)
        self.assertEqual(status["state"], "ready")
        self.assertEqual(status["categories"], [])
