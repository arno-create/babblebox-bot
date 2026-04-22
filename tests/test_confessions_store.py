from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import re
import unittest
from unittest import mock

from babblebox.confessions_crypto import ConfessionsCrypto
from babblebox.confessions_store import (
    DEFAULT_LINK_POLICY_MODE,
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


_PLACEHOLDER_RE = re.compile(r"\$(\d+)")


def _privacy() -> ConfessionsCrypto:
    return ConfessionsCrypto.from_environment(backend_name="test")


def _placeholder_numbers(statement: str) -> list[int]:
    return [int(match.group(1)) for match in _PLACEHOLDER_RE.finditer(statement)]


def _validate_statement_args(statement: str, args: tuple[object, ...]):
    placeholders = _placeholder_numbers(statement)
    if not placeholders:
        if args:
            raise AssertionError(f"SQL statement has no placeholders but received {len(args)} args: {statement}")
        return
    expected_numbers = list(range(1, max(placeholders) + 1))
    missing = [number for number in expected_numbers if number not in placeholders]
    if missing:
        raise AssertionError(f"SQL statement skipped placeholders {missing}: {statement}")
    if len(args) != expected_numbers[-1]:
        raise AssertionError(
            f"SQL statement expected {expected_numbers[-1]} args from placeholders but received {len(args)}: {statement}"
        )


def _literal_sql_text(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        parts: list[str] = []
        for value in node.values:
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                parts.append(value.value)
            elif isinstance(value, ast.FormattedValue):
                parts.append("{expr}")
            else:
                return None
        return "".join(parts)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _literal_sql_text(node.left)
        right = _literal_sql_text(node.right)
        if left is None or right is None:
            return None
        return left + right
    return None


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
        _validate_statement_args(statement, args)
        self.executed.append(statement)
        self.execute_calls.append((statement, args))
        return "EXECUTE"

    async def fetch(self, statement: str, *args):
        _validate_statement_args(statement, args)
        self.fetch_calls.append((statement, args))
        if self.fetch_results:
            return self.fetch_results.pop(0)
        return []

    async def fetchrow(self, statement: str, *args):
        _validate_statement_args(statement, args)
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
        self.assertEqual(config["link_policy_mode"], DEFAULT_LINK_POLICY_MODE)
        self.assertTrue(config["allow_trusted_mainstream_links"])
        self.assertFalse(config["allow_images"])
        self.assertFalse(config["image_review_required"])
        self.assertFalse(config["allow_anonymous_replies"])
        self.assertFalse(config["anonymous_reply_review_required"])
        self.assertTrue(config["allow_owner_replies"])
        self.assertFalse(config["owner_reply_review_mode"])
        self.assertFalse(config["allow_self_edit"])
        self.assertIsNone(config["appeals_channel_id"])
        self.assertIsNone(config["panel_channel_id"])
        self.assertIsNone(config["panel_message_id"])
        self.assertEqual(config["max_images"], 3)
        self.assertEqual(config["cooldown_seconds"], 300)
        self.assertEqual(config["burst_limit"], 3)
        self.assertTrue(config["auto_moderation_exempt_admins"])
        self.assertEqual(config["auto_moderation_exempt_role_ids"], [])

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
                "auto_moderation_exempt_admins": False,
                "auto_moderation_exempt_role_ids": [1000, 999, 1000],
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
        self.assertEqual(config["link_policy_mode"], DEFAULT_LINK_POLICY_MODE)
        self.assertEqual(config["custom_allow_domains"], ["google.com", "youtube.com"])
        self.assertEqual(config["custom_block_domains"], ["bad.example"])
        self.assertTrue(config["allow_images"])
        self.assertFalse(config["image_review_required"])
        self.assertTrue(config["allow_anonymous_replies"])
        self.assertFalse(config["anonymous_reply_review_required"])
        self.assertFalse(config["allow_owner_replies"])
        self.assertFalse(config["owner_reply_review_mode"])
        self.assertTrue(config["allow_self_edit"])
        self.assertFalse(config["auto_moderation_exempt_admins"])
        self.assertEqual(config["auto_moderation_exempt_role_ids"], [999, 1000])
        self.assertEqual(config["max_images"], 6)
        self.assertEqual(config["cooldown_seconds"], 300)
        self.assertEqual(config["burst_limit"], 3)
        self.assertEqual(config["burst_window_seconds"], 1800)
        self.assertEqual(config["auto_suspend_hours"], 12)
        self.assertEqual(config["strike_temp_ban_threshold"], 9)
        self.assertEqual(config["strike_perm_ban_threshold"], 9)

    def test_normalize_config_maps_legacy_and_explicit_link_modes(self):
        disabled = normalize_confession_config(10, {"allow_trusted_mainstream_links": False})
        self.assertEqual(disabled["link_policy_mode"], "disabled")
        self.assertFalse(disabled["allow_trusted_mainstream_links"])

        allow_all_safe = normalize_confession_config(10, {"link_policy_mode": "allow_all_safe"})
        self.assertEqual(allow_all_safe["link_policy_mode"], "allow_all_safe")
        self.assertTrue(allow_all_safe["allow_trusted_mainstream_links"])

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
        self.assertTrue(config["image_review_required"])
        self.assertEqual(config["max_images"], 2)

    def test_normalize_config_keeps_risky_features_enabled_but_disables_review_requirement_without_private_review_channel(self):
        config = normalize_confession_config(
            10,
            {
                "enabled": True,
                "confession_channel_id": 111,
                "allow_images": True,
                "image_review_required": True,
                "allow_anonymous_replies": True,
                "anonymous_reply_review_required": True,
            },
        )
        self.assertTrue(config["allow_images"])
        self.assertFalse(config["image_review_required"])
        self.assertTrue(config["allow_anonymous_replies"])
        self.assertFalse(config["anonymous_reply_review_required"])

    def test_normalize_config_legacy_enablement_defaults_review_flags_on(self):
        config = normalize_confession_config(
            10,
            {
                "enabled": True,
                "confession_channel_id": 111,
                "review_channel_id": 222,
                "allow_images": True,
                "allow_anonymous_replies": True,
            },
        )
        self.assertTrue(config["image_review_required"])
        self.assertTrue(config["anonymous_reply_review_required"])

    def test_normalize_config_clears_review_flags_when_feature_is_off(self):
        config = normalize_confession_config(
            10,
            {
                "allow_images": False,
                "image_review_required": True,
                "allow_anonymous_replies": False,
                "anonymous_reply_review_required": True,
            },
        )
        self.assertFalse(config["image_review_required"])
        self.assertFalse(config["anonymous_reply_review_required"])

    def test_normalize_submission_drops_binary_bloat_from_attachment_meta(self):
        record = normalize_submission(
            {
                "submission_id": "sub-1",
                "guild_id": 10,
                "confession_id": "CF-AAAA1111",
                "submission_kind": "reply",
                "parent_confession_id": "CF-ZZZZ9999",
                "reply_target_label": "Responder",
                "reply_target_preview": "Public preview",
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
        self.assertEqual(record["reply_target_label"], "Responder")
        self.assertEqual(record["reply_target_preview"], "Public preview")
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

    def test_normalize_submission_preserves_4000_chars_and_thread_id_for_root_only(self):
        root_body = "x" * 4000
        root_record = normalize_submission(
            {
                "submission_id": "sub-root",
                "guild_id": 10,
                "confession_id": "CF-ROOT111",
                "submission_kind": "confession",
                "status": "published",
                "review_status": "approved",
                "content_body": root_body,
                "discussion_thread_id": 999,
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        reply_record = normalize_submission(
            {
                "submission_id": "sub-reply",
                "guild_id": 10,
                "confession_id": "CF-REPLY11",
                "submission_kind": "reply",
                "status": "published",
                "review_status": "approved",
                "content_body": root_body,
                "discussion_thread_id": 777,
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )

        self.assertEqual(root_record["content_body"], root_body)
        self.assertEqual(root_record["discussion_thread_id"], 999)
        self.assertEqual(reply_record["content_body"], root_body)
        self.assertIsNone(reply_record["discussion_thread_id"])

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
                    "reply_target_label": "Responder",
                    "reply_target_preview": "Public preview",
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
        self.assertEqual(record["reply_target_label"], "Responder")
        self.assertEqual(record["reply_target_preview"], "Public preview")
        self.assertEqual(record["content_body"], "full text")
        self.assertEqual(record["shared_link_url"], "https://www.google.com/search?q=preview")
        self.assertEqual(record["fuzzy_signature"], "fh1:def")
        self.assertEqual(record["flag_codes"], ["adult_language", "link_unsafe"])
        self.assertEqual(record["attachment_meta"][0]["kind"], "image")

    async def test_memory_store_round_trips_reply_target_snapshot_fields(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            await store.upsert_submission(
                {
                    "submission_id": "sub-snapshot",
                    "guild_id": 10,
                    "confession_id": "CF-SNAP001",
                    "submission_kind": "reply",
                    "reply_flow": "owner_reply_to_user",
                    "owner_reply_generation": 1,
                    "parent_confession_id": "CF-ROOT001",
                    "reply_target_label": "Responder",
                    "reply_target_preview": "Snapshot preview",
                    "status": "queued",
                    "review_status": "pending",
                    "staff_preview": "Queued preview",
                    "content_body": "Queued body",
                    "shared_link_url": None,
                    "content_fingerprint": "h1:snapshot",
                    "similarity_key": "sim:snapshot",
                    "fuzzy_signature": "fh1:snapshot",
                    "flag_codes": [],
                    "attachment_meta": [],
                    "created_at": "2026-04-03T00:00:00+00:00",
                }
            )

            record = await store.fetch_submission("sub-snapshot")
            self.assertEqual(record["reply_target_label"], "Responder")
            self.assertEqual(record["reply_target_preview"], "Snapshot preview")
        finally:
            await store.close()

    async def test_memory_store_lists_only_published_public_reply_submissions(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            await store.upsert_submission(
                {
                    "submission_id": "sub-root",
                    "guild_id": 10,
                    "confession_id": "CF-ROOT100",
                    "submission_kind": "confession",
                    "status": "published",
                    "review_status": "approved",
                    "staff_preview": "Root preview",
                    "content_body": "Root body",
                    "shared_link_url": None,
                    "content_fingerprint": "h1:root",
                    "similarity_key": "sim:root",
                    "fuzzy_signature": "fh1:root",
                    "flag_codes": [],
                    "attachment_meta": [],
                    "posted_channel_id": 20,
                    "posted_message_id": 21,
                    "created_at": "2026-04-03T00:00:00+00:00",
                    "published_at": "2026-04-03T00:01:00+00:00",
                }
            )
            await store.upsert_submission(
                {
                    "submission_id": "sub-reply-public",
                    "guild_id": 10,
                    "confession_id": "CF-REPLY100",
                    "submission_kind": "reply",
                    "reply_flow": "reply_to_confession",
                    "parent_confession_id": "CF-ROOT100",
                    "status": "published",
                    "review_status": "approved",
                    "staff_preview": "Reply preview",
                    "content_body": "Reply body",
                    "shared_link_url": None,
                    "content_fingerprint": "h1:reply",
                    "similarity_key": "sim:reply",
                    "fuzzy_signature": "fh1:reply",
                    "flag_codes": [],
                    "attachment_meta": [],
                    "posted_channel_id": 30,
                    "posted_message_id": 31,
                    "created_at": "2026-04-03T00:02:00+00:00",
                    "published_at": "2026-04-03T00:03:00+00:00",
                }
            )
            await store.upsert_submission(
                {
                    "submission_id": "sub-reply-owner",
                    "guild_id": 10,
                    "confession_id": "CF-OWNER100",
                    "submission_kind": "reply",
                    "reply_flow": "owner_reply_to_user",
                    "owner_reply_generation": 1,
                    "parent_confession_id": "CF-ROOT100",
                    "status": "published",
                    "review_status": "approved",
                    "staff_preview": "Owner preview",
                    "content_body": "Owner body",
                    "shared_link_url": None,
                    "content_fingerprint": "h1:owner",
                    "similarity_key": "sim:owner",
                    "fuzzy_signature": "fh1:owner",
                    "flag_codes": [],
                    "attachment_meta": [],
                    "posted_channel_id": 30,
                    "posted_message_id": 32,
                    "created_at": "2026-04-03T00:04:00+00:00",
                    "published_at": "2026-04-03T00:05:00+00:00",
                }
            )

            records = await store.list_published_public_reply_submissions(10)
            self.assertEqual([record["confession_id"] for record in records], ["CF-REPLY100"])
        finally:
            await store.close()

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
        self.assertIn("reply_target_label TEXT NULL", executed)
        self.assertIn("reply_target_preview TEXT NULL", executed)
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
                "reply_target_label": "Queued target",
                "reply_target_preview": "Queued target preview",
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
                "reply_target_label": "Published target",
                "reply_target_preview": "Published target preview",
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
            self.assertEqual(active_submission["reply_target_label"], "Queued target")
            self.assertEqual(active_submission["reply_target_preview"], "Queued target preview")
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
            self.assertIsNone(raw_active_submission["reply_target_label"])
            self.assertIsNone(raw_active_submission["reply_target_preview"])
            self.assertIsNone(raw_active_submission["staff_preview"])
            self.assertIsNone(raw_active_submission["content_body"])
            self.assertIsNone(raw_active_submission["shared_link_url"])
            self.assertIsNone(raw_active_submission["similarity_key"])
            self.assertTrue(str(raw_active_submission["content_ciphertext"]).startswith("bbx2:ephemeral:"))
            self.assertTrue(str(raw_active_submission["content_fingerprint"]).startswith("h2:ephemeral:"))
            self.assertTrue(str(raw_active_submission["fuzzy_signature"]).startswith("fh2:ephemeral:"))

            raw_terminal_submission = raw_store.submissions["sub-terminal"]
            self.assertIsNone(raw_terminal_submission["reply_target_label"])
            self.assertIsNone(raw_terminal_submission["reply_target_preview"])
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

    async def test_memory_store_lists_active_enforcement_states_for_gate_cache(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            await store.upsert_enforcement_state(
                {
                    "guild_id": 10,
                    "user_id": 401,
                    "active_restriction": "temp_ban",
                    "restricted_until": "2026-04-10T00:00:00+00:00",
                    "is_permanent_ban": False,
                    "strike_count": 2,
                    "last_strike_at": None,
                    "cooldown_until": None,
                    "burst_count": 0,
                    "burst_window_started_at": None,
                    "last_case_id": "CS-GATE401",
                    "image_restriction_active": False,
                    "image_restricted_until": None,
                    "image_restriction_case_id": None,
                    "updated_at": "2026-04-03T00:00:00+00:00",
                }
            )
            await store.upsert_enforcement_state(
                {
                    "guild_id": 10,
                    "user_id": 402,
                    "active_restriction": "none",
                    "restricted_until": None,
                    "is_permanent_ban": False,
                    "strike_count": 0,
                    "last_strike_at": None,
                    "cooldown_until": None,
                    "burst_count": 0,
                    "burst_window_started_at": None,
                    "last_case_id": "CS-GATE402",
                    "image_restriction_active": True,
                    "image_restricted_until": "2026-04-11T00:00:00+00:00",
                    "image_restriction_case_id": "CS-IMG402",
                    "updated_at": "2026-04-03T00:00:00+00:00",
                }
            )
            await store.upsert_enforcement_state(
                {
                    "guild_id": 10,
                    "user_id": 403,
                    "active_restriction": "none",
                    "restricted_until": None,
                    "is_permanent_ban": False,
                    "strike_count": 0,
                    "last_strike_at": None,
                    "cooldown_until": None,
                    "burst_count": 0,
                    "burst_window_started_at": None,
                    "last_case_id": None,
                    "image_restriction_active": False,
                    "image_restricted_until": None,
                    "image_restriction_case_id": None,
                    "updated_at": "2026-04-03T00:00:00+00:00",
                }
            )

            active_rows = await store.list_active_enforcement_states()

            self.assertEqual({(row["guild_id"], row["user_id"]) for row in active_rows}, {(10, 401), (10, 402)})
        finally:
            await store.close()

    async def test_memory_store_list_review_surfaces_includes_private_attachment_urls(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            await store.upsert_submission(
                {
                    "submission_id": "sub-review",
                    "guild_id": 10,
                    "confession_id": "CF-REVIEW01",
                    "submission_kind": "confession",
                    "status": "queued",
                    "review_status": "pending",
                    "staff_preview": "Queued preview",
                    "content_body": "Queued body",
                    "shared_link_url": None,
                    "content_fingerprint": "h1:review",
                    "similarity_key": "sim:review",
                    "fuzzy_signature": "fh1:review",
                    "flag_codes": [],
                    "attachment_meta": [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}],
                    "created_at": "2026-04-03T00:00:00+00:00",
                }
            )
            await store.upsert_case(
                {
                    "case_id": "CS-REVIEW01",
                    "guild_id": 10,
                    "submission_id": "sub-review",
                    "confession_id": "CF-REVIEW01",
                    "case_kind": "review",
                    "status": "open",
                    "reason_codes": ["images"],
                    "review_version": 1,
                    "created_at": "2026-04-03T00:00:00+00:00",
                }
            )
            await store.upsert_private_media(
                {
                    "guild_id": 10,
                    "submission_id": "sub-review",
                    "attachment_urls": [
                        "https://cdn.discordapp.com/attachments/1/2/one.png",
                        "https://cdn.discordapp.com/attachments/1/2/two.png",
                    ],
                    "created_at": "2026-04-03T00:00:00+00:00",
                    "updated_at": "2026-04-03T00:01:00+00:00",
                }
            )

            surfaces = await store.list_review_surfaces(10, limit=5)

            self.assertEqual(len(surfaces), 1)
            self.assertEqual(
                surfaces[0]["attachment_urls"],
                [
                    "https://cdn.discordapp.com/attachments/1/2/one.png",
                    "https://cdn.discordapp.com/attachments/1/2/two.png",
                ],
            )
        finally:
            await store.close()

    async def test_memory_store_support_ticket_round_trip(self):
        store = ConfessionsStore(backend="memory")
        await store.load()
        try:
            await store.upsert_support_ticket(
                {
                    "ticket_id": "CT-APPEAL1",
                    "guild_id": 10,
                    "kind": "appeal",
                    "action_target_id": "CS-AAAA1111",
                    "reference_confession_id": "CF-AAAA1111",
                    "reference_case_id": "CS-AAAA1111",
                    "context_label": "Appeal against automatic restriction",
                    "details": "Please review the full context.",
                    "status": "open",
                    "resolution_action": None,
                    "message_channel_id": 50,
                    "message_id": 60,
                    "created_at": "2026-04-03T00:00:00+00:00",
                    "resolved_at": None,
                }
            )

            record = await store.fetch_support_ticket(10, "CT-APPEAL1")
            rows = await store.list_support_tickets(10, status="open", limit=5)

            self.assertIsNotNone(record)
            self.assertEqual(record["kind"], "appeal")
            self.assertEqual(record["action_target_id"], "CS-AAAA1111")
            self.assertEqual(record["message_channel_id"], 50)
            self.assertEqual([row["ticket_id"] for row in rows], ["CT-APPEAL1"])
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
                "link_policy_mode": "trusted_only",
                "allow_trusted_mainstream_links": True,
                "custom_allow_domains": ["google.com"],
                "custom_block_domains": ["bad.example"],
                "allowed_role_ids": [501],
                "blocked_role_ids": [502],
                "allow_images": True,
                "image_review_required": True,
                "allow_anonymous_replies": True,
                "anonymous_reply_review_required": False,
                "allow_owner_replies": True,
                "owner_reply_review_mode": True,
                "allow_self_edit": True,
                "auto_moderation_exempt_admins": False,
                "auto_moderation_exempt_role_ids": [901, 902],
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
        self.assertEqual(args[9], "trusted_only")
        self.assertTrue(args[10])
        self.assertEqual(json.loads(args[11]), ["google.com"])
        self.assertEqual(json.loads(args[12]), ["bad.example"])
        self.assertEqual(json.loads(args[13]), [501])
        self.assertEqual(json.loads(args[14]), [502])
        self.assertTrue(args[15])
        self.assertTrue(args[16])
        self.assertTrue(args[17])
        self.assertFalse(args[18])
        self.assertTrue(args[19])
        self.assertTrue(args[20])
        self.assertTrue(args[21])
        self.assertFalse(args[22])
        self.assertEqual(json.loads(args[23]), [901, 902])

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
                "link_policy_mode": "trusted_only",
                "allow_trusted_mainstream_links": True,
                "custom_allow_domains": json.dumps(["google.com"]),
                "custom_block_domains": json.dumps(["bad.example"]),
                "allowed_role_ids": json.dumps([501]),
                "blocked_role_ids": json.dumps([502]),
                "allow_images": True,
                "image_review_required": True,
                "allow_anonymous_replies": True,
                "anonymous_reply_review_required": False,
                "allow_owner_replies": True,
                "owner_reply_review_mode": True,
                "allow_self_edit": True,
                "auto_moderation_exempt_admins": False,
                "auto_moderation_exempt_role_ids": json.dumps([901, 902]),
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
        self.assertEqual(config["link_policy_mode"], "trusted_only")
        self.assertEqual(config["allowed_role_ids"], [501])
        self.assertEqual(config["blocked_role_ids"], [502])
        self.assertTrue(config["allow_images"])
        self.assertTrue(config["image_review_required"])
        self.assertTrue(config["allow_anonymous_replies"])
        self.assertFalse(config["anonymous_reply_review_required"])
        self.assertTrue(config["allow_owner_replies"])
        self.assertTrue(config["owner_reply_review_mode"])
        self.assertTrue(config["allow_self_edit"])
        self.assertFalse(config["auto_moderation_exempt_admins"])
        self.assertEqual(config["auto_moderation_exempt_role_ids"], [901, 902])

    async def test_postgres_store_upsert_submission_includes_reply_target_snapshot_fields(self):
        await self.store.upsert_submission(
            {
                "submission_id": "sub-1",
                "guild_id": 10,
                "confession_id": "CF-AAAA1111",
                "submission_kind": "reply",
                "reply_flow": "owner_reply_to_user",
                "owner_reply_generation": 1,
                "parent_confession_id": "CF-ROOT111",
                "reply_target_label": "Responder",
                "reply_target_preview": "Snapshot preview",
                "status": "queued",
                "review_status": "pending",
                "staff_preview": "Queued preview",
                "content_body": "Queued body",
                "shared_link_url": None,
                "content_fingerprint": "h1:test",
                "similarity_key": "sim:test",
                "fuzzy_signature": "fh1:test",
                "flag_codes": ["adult_language"],
                "attachment_meta": [],
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )

        statement, args = self.connection.execute_calls[-1]
        self.assertIn("reply_target_label", statement)
        self.assertIn("reply_target_preview", statement)
        self.assertIsNone(args[7])
        self.assertIsNone(args[8])
        self.assertIsNotNone(args[14])

        self.connection.fetchrow_results.append(
            {
                "submission_id": "sub-1",
                "guild_id": 10,
                "confession_id": "CF-AAAA1111",
                "submission_kind": "reply",
                "reply_flow": "owner_reply_to_user",
                "owner_reply_generation": 1,
                "parent_confession_id": "CF-ROOT111",
                "reply_target_label": None,
                "reply_target_preview": None,
                "status": "queued",
                "review_status": "pending",
                "staff_preview": None,
                "content_body": None,
                "shared_link_url": None,
                "content_ciphertext": args[14],
                "content_fingerprint": "h1:test",
                "similarity_key": "sim:test",
                "fuzzy_signature": "fh1:test",
                "flag_codes": json.dumps(["adult_language"]),
                "attachment_meta": json.dumps([]),
                "posted_channel_id": None,
                "posted_message_id": None,
                "current_case_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": None,
                "resolved_at": None,
            }
        )

        record = await self.store.fetch_submission("sub-1")
        self.assertEqual(record["reply_target_label"], "Responder")
        self.assertEqual(record["reply_target_preview"], "Snapshot preview")

    async def test_postgres_store_upsert_submission_round_trips_discussion_thread_id(self):
        await self.store.upsert_submission(
            {
                "submission_id": "sub-root-thread",
                "guild_id": 10,
                "confession_id": "CF-THREAD1",
                "submission_kind": "confession",
                "status": "published",
                "review_status": "approved",
                "staff_preview": "Published preview",
                "content_body": "Published body",
                "shared_link_url": None,
                "content_fingerprint": "h1:thread",
                "similarity_key": "sim:thread",
                "fuzzy_signature": "fh1:thread",
                "flag_codes": [],
                "attachment_meta": [],
                "posted_channel_id": 20,
                "posted_message_id": 30,
                "discussion_thread_id": 40,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": "2026-04-03T00:05:00+00:00",
            }
        )

        statement, args = self.connection.execute_calls[-1]
        self.assertIn("discussion_thread_id", statement)
        self.assertEqual(args[22], 40)

        self.connection.fetchrow_results.append(
            {
                "submission_id": "sub-root-thread",
                "guild_id": 10,
                "confession_id": "CF-THREAD1",
                "submission_kind": "confession",
                "reply_flow": None,
                "owner_reply_generation": None,
                "parent_confession_id": None,
                "reply_target_label": None,
                "reply_target_preview": None,
                "status": "published",
                "review_status": "approved",
                "staff_preview": None,
                "content_body": None,
                "shared_link_url": None,
                "content_ciphertext": args[14],
                "content_fingerprint": "h1:thread",
                "similarity_key": "sim:thread",
                "fuzzy_signature": "fh1:thread",
                "flag_codes": json.dumps([]),
                "attachment_meta": json.dumps([]),
                "posted_channel_id": 20,
                "posted_message_id": 30,
                "discussion_thread_id": 40,
                "current_case_id": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "published_at": "2026-04-03T00:05:00+00:00",
                "resolved_at": None,
            }
        )

        record = await self.store.fetch_submission("sub-root-thread")
        self.assertEqual(record["discussion_thread_id"], 40)

    async def test_postgres_store_lists_published_public_reply_submissions(self):
        self.connection.fetch_results.append(
            [
                {
                    "submission_id": "sub-reply-public",
                    "guild_id": 10,
                    "confession_id": "CF-REPLY200",
                    "submission_kind": "reply",
                    "reply_flow": "reply_to_confession",
                    "owner_reply_generation": None,
                    "parent_confession_id": "CF-ROOT200",
                    "reply_target_label": None,
                    "reply_target_preview": None,
                    "status": "published",
                    "review_status": "approved",
                    "staff_preview": "Reply preview",
                    "content_body": "Reply body",
                    "shared_link_url": None,
                    "content_ciphertext": None,
                    "content_fingerprint": "h1:reply",
                    "similarity_key": None,
                    "fuzzy_signature": "fh1:reply",
                    "flag_codes": json.dumps([]),
                    "attachment_meta": json.dumps([]),
                    "posted_channel_id": 40,
                    "posted_message_id": 41,
                    "discussion_thread_id": None,
                    "current_case_id": None,
                    "created_at": "2026-04-03T00:02:00+00:00",
                    "published_at": "2026-04-03T00:03:00+00:00",
                    "resolved_at": None,
                }
            ]
        )

        records = await self.store.list_published_public_reply_submissions(10)

        self.assertEqual([record["confession_id"] for record in records], ["CF-REPLY200"])
        statement, args = self.connection.fetch_calls[-1]
        self.assertIn("submission_kind = 'reply'", statement)
        self.assertIn("reply_flow = 'reply_to_confession'", statement)
        self.assertEqual(args, (10,))

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
                    "link_policy_mode": "trusted_only",
                    "allow_trusted_mainstream_links": True,
                    "custom_allow_domains": json.dumps(["google.com"]),
                    "custom_block_domains": json.dumps(["bad.example"]),
                    "allowed_role_ids": json.dumps([501]),
                    "blocked_role_ids": json.dumps([502]),
                    "allow_images": True,
                    "image_review_required": True,
                    "allow_anonymous_replies": True,
                    "anonymous_reply_review_required": False,
                    "allow_owner_replies": True,
                    "owner_reply_review_mode": True,
                    "allow_self_edit": True,
                    "auto_moderation_exempt_admins": False,
                    "auto_moderation_exempt_role_ids": [901, 902],
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
                    "link_policy_mode": "trusted_only",
                    "allow_trusted_mainstream_links": True,
                    "custom_allow_domains": json.dumps([]),
                    "custom_block_domains": json.dumps([]),
                    "allowed_role_ids": json.dumps([]),
                    "blocked_role_ids": json.dumps([]),
                    "allow_images": False,
                    "image_review_required": False,
                    "allow_anonymous_replies": False,
                    "anonymous_reply_review_required": False,
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
        self.assertEqual(configs[10]["link_policy_mode"], "trusted_only")
        self.assertTrue(configs[10]["image_review_required"])
        self.assertFalse(configs[10]["anonymous_reply_review_required"])
        self.assertEqual(configs[11]["confession_channel_id"], 21)
        self.assertEqual(configs[11]["allowed_role_ids"], [])
        self.assertFalse(configs[11]["image_review_required"])
        self.assertFalse(configs[11]["anonymous_reply_review_required"])

    async def test_postgres_store_support_ticket_round_trip_methods(self):
        await self.store.upsert_support_ticket(
            {
                "ticket_id": "CT-REPORT1",
                "guild_id": 10,
                "kind": "report",
                "action_target_id": "CF-AAAA1111",
                "reference_confession_id": "CF-AAAA1111",
                "reference_case_id": None,
                "context_label": "Report against published confession",
                "details": "This confession needs moderation.",
                "status": "open",
                "resolution_action": None,
                "message_channel_id": 50,
                "message_id": 60,
                "created_at": "2026-04-03T00:00:00+00:00",
                "resolved_at": None,
            }
        )

        statement, args = self.connection.execute_calls[-1]
        self.assertIn("confession_support_tickets", statement)
        self.assertEqual(args[0], "CT-REPORT1")
        self.assertEqual(args[1], 10)
        self.assertEqual(args[2], "report")
        self.assertEqual(args[3], "CF-AAAA1111")
        self.assertEqual(args[10], 50)
        self.assertEqual(args[11], 60)

        self.connection.fetchrow_results.append(
            {
                "ticket_id": "CT-REPORT1",
                "guild_id": 10,
                "kind": "report",
                "action_target_id": "CF-AAAA1111",
                "reference_confession_id": "CF-AAAA1111",
                "reference_case_id": None,
                "context_label": "Report against published confession",
                "details": "This confession needs moderation.",
                "status": "open",
                "resolution_action": None,
                "message_channel_id": 50,
                "message_id": 60,
                "created_at": "2026-04-03T00:00:00+00:00",
                "resolved_at": None,
            }
        )
        record = await self.store.fetch_support_ticket(10, "CT-REPORT1")
        self.assertEqual(record["ticket_id"], "CT-REPORT1")
        self.assertEqual(record["details"], "This confession needs moderation.")

        self.connection.fetch_results.append(
            [
                {
                    "ticket_id": "CT-REPORT1",
                    "guild_id": 10,
                    "kind": "report",
                    "action_target_id": "CF-AAAA1111",
                    "reference_confession_id": "CF-AAAA1111",
                    "reference_case_id": None,
                    "context_label": "Report against published confession",
                    "details": "This confession needs moderation.",
                    "status": "open",
                    "resolution_action": None,
                    "message_channel_id": 50,
                    "message_id": 60,
                    "created_at": "2026-04-03T00:00:00+00:00",
                    "resolved_at": None,
                }
            ]
        )
        rows = await self.store.list_support_tickets(10, status="open", limit=5)
        self.assertEqual([row["ticket_id"] for row in rows], ["CT-REPORT1"])

    async def test_postgres_store_list_review_surfaces_includes_private_attachment_urls(self):
        self.connection.fetch_results.append(
            [
                {
                    "case_id": "CS-AAAA1111",
                    "case_kind": "review",
                    "status": "open",
                    "review_version": 3,
                    "submission_id": "sub-1",
                    "guild_id": 10,
                    "confession_id": "CF-AAAA1111",
                    "submission_kind": "confession",
                    "reply_flow": None,
                    "owner_reply_generation": None,
                    "parent_confession_id": None,
                    "staff_preview": None,
                    "content_body": None,
                    "shared_link_url": None,
                    "content_ciphertext": self.store._privacy.encrypt_payload(
                        domain="submission-content",
                        aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
                        payload={"staff_preview": "Queued preview", "content_body": "Queued body", "shared_link_url": None},
                        key_domain="content",
                    ),
                    "flag_codes": json.dumps(["images"]),
                    "attachment_meta": json.dumps(
                        [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}]
                    ),
                    "created_at": "2026-04-03T00:00:00+00:00",
                }
            ]
        )
        self.connection.fetch_results.append(
            [
                {
                    "submission_id": "sub-1",
                    "guild_id": 10,
                    "attachment_urls": [],
                    "attachment_payload": self.store._privacy.encrypt_payload(
                        domain="private-media",
                        aad_fields={"guild_id": 10, "submission_id": "sub-1"},
                        payload={
                            "attachment_urls": [
                                "https://cdn.discordapp.com/attachments/1/2/image.png",
                                "https://cdn.discordapp.com/attachments/1/2/extra.png",
                            ]
                        },
                        key_domain="content",
                    ),
                    "created_at": "2026-04-03T00:00:00+00:00",
                    "updated_at": "2026-04-03T00:01:00+00:00",
                }
            ]
        )

        surfaces = await self.store.list_review_surfaces(10, limit=25)

        self.assertEqual(len(surfaces), 1)
        self.assertEqual(
            surfaces[0]["attachment_urls"],
            [
                "https://cdn.discordapp.com/attachments/1/2/image.png",
                "https://cdn.discordapp.com/attachments/1/2/extra.png",
            ],
        )

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

    async def test_fake_connection_validator_rejects_placeholder_arg_mismatches(self):
        with self.assertRaises(AssertionError):
            await self.connection.execute("SELECT $1, $2", 1)

        with self.assertRaises(AssertionError):
            await self.connection.fetch("SELECT 1", 1)

        with self.assertRaises(AssertionError):
            await self.connection.fetchrow("SELECT $1, $3", 1, 2, 3)

    async def test_postgres_store_major_write_queries_validate_placeholder_arity(self):
        await self.store.upsert_config(
            {
                "guild_id": 10,
                "enabled": True,
                "confession_channel_id": 20,
                "panel_channel_id": 40,
                "panel_message_id": 50,
                "review_channel_id": 30,
                "appeals_channel_id": 60,
                "review_mode": True,
                "block_adult_language": True,
                "allow_trusted_mainstream_links": True,
                "custom_allow_domains": ["google.com"],
                "custom_block_domains": ["bad.example"],
                "allowed_role_ids": [501],
                "blocked_role_ids": [502],
                "allow_images": True,
                "image_review_required": True,
                "allow_anonymous_replies": True,
                "anonymous_reply_review_required": False,
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
        await self.store.upsert_submission(
            {
                "submission_id": "sub-1",
                "guild_id": 10,
                "confession_id": "CF-AAAA1111",
                "submission_kind": "confession",
                "status": "queued",
                "review_status": "pending",
                "staff_preview": "Queued preview",
                "content_body": "Queued body",
                "shared_link_url": "https://www.google.com/search?q=babblebox",
                "content_fingerprint": "h1:test",
                "similarity_key": "sim:test",
                "fuzzy_signature": "fh1:test",
                "flag_codes": ["adult_language"],
                "attachment_meta": [{"kind": "image", "size": 12, "width": 8, "height": 8, "spoiler": False}],
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        await self.store.upsert_case(
            {
                "case_id": "CS-AAAA1111",
                "guild_id": 10,
                "submission_id": "sub-1",
                "confession_id": "CF-AAAA1111",
                "case_kind": "review",
                "status": "open",
                "reason_codes": ["adult_language"],
                "review_version": 1,
                "review_message_channel_id": 30,
                "review_message_id": 70,
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        await self.store.upsert_author_link(
            {
                "guild_id": 10,
                "submission_id": "sub-1",
                "author_user_id": 101,
                "created_at": "2026-04-03T00:00:00+00:00",
            }
        )
        await self.store.upsert_private_media(
            {
                "guild_id": 10,
                "submission_id": "sub-1",
                "attachment_urls": ["https://cdn.discordapp.com/attachments/1/2/image.png"],
                "created_at": "2026-04-03T00:00:00+00:00",
                "updated_at": "2026-04-03T00:01:00+00:00",
            }
        )
        await self.store.upsert_owner_reply_opportunity(
            {
                "opportunity_id": "opp-1",
                "guild_id": 10,
                "root_submission_id": "sub-1",
                "root_confession_id": "CF-AAAA1111",
                "referenced_submission_id": "sub-2",
                "source_channel_id": 20,
                "source_message_id": 30,
                "source_author_user_id": 102,
                "source_author_name": "Responder",
                "source_preview": "Helpful reply",
                "source_message_fingerprint": "fp:test",
                "status": "pending",
                "notification_status": "sent",
                "notification_channel_id": 40,
                "notification_message_id": 50,
                "created_at": "2026-04-03T00:00:00+00:00",
                "expires_at": "2026-04-06T00:00:00+00:00",
                "notified_at": "2026-04-03T00:05:00+00:00",
            }
        )
        await self.store.upsert_enforcement_state(
            {
                "guild_id": 10,
                "user_id": 101,
                "active_restriction": "temp_ban",
                "restricted_until": "2026-04-10T00:00:00+00:00",
                "is_permanent_ban": False,
                "strike_count": 3,
                "last_strike_at": "2026-04-03T00:00:00+00:00",
                "cooldown_until": "2026-04-03T01:00:00+00:00",
                "burst_count": 1,
                "burst_window_started_at": "2026-04-03T00:00:00+00:00",
                "last_case_id": "CS-AAAA1111",
                "image_restriction_active": True,
                "image_restricted_until": "2026-04-04T00:00:00+00:00",
                "image_restriction_case_id": "CS-IMG1111",
                "updated_at": "2026-04-03T00:10:00+00:00",
            }
        )
        await self.store.upsert_review_queue(
            {
                "guild_id": 10,
                "channel_id": 30,
                "message_id": 40,
                "updated_at": "2026-04-03T00:00:00+00:00",
            }
        )
        await self.store.upsert_support_ticket(
            {
                "ticket_id": "CT-AAAA1111",
                "guild_id": 10,
                "kind": "appeal",
                "action_target_id": "CS-AAAA1111",
                "reference_confession_id": "CF-AAAA1111",
                "reference_case_id": "CS-AAAA1111",
                "context_label": "Appeal against a queued case",
                "details": "Please review the context.",
                "status": "open",
                "resolution_action": None,
                "message_channel_id": 50,
                "message_id": 60,
                "created_at": "2026-04-03T00:00:00+00:00",
                "resolved_at": None,
            }
        )
        await self.store.delete_review_queue(10)

        self.assertGreaterEqual(len(self.connection.execute_calls), 11)

    async def test_postgres_store_major_fetch_queries_validate_parameter_arity(self):
        await self.store.fetch_all_configs()
        await self.store.fetch_config(10)
        await self.store.fetch_submission("sub-1")
        await self.store.fetch_submission_by_confession_id(10, "CF-AAAA1111")
        await self.store.fetch_submission_by_message_id(10, 20)
        await self.store.list_published_top_level_submissions(10)
        await self.store.list_published_public_reply_submissions(10)
        await self.store.list_recent_submissions_for_author(10, 101, limit=5)
        await self.store.list_review_cases(10, limit=25)
        await self.store.fetch_case(10, "CS-AAAA1111")
        await self.store.fetch_author_link("sub-1")
        await self.store.fetch_private_media("sub-1")
        await self.store.fetch_owner_reply_opportunity("opp-1")
        await self.store.fetch_owner_reply_opportunity_by_source_message_id(10, 30)
        await self.store.fetch_owner_reply_opportunity_by_notification_message_id(50)
        await self.store.fetch_pending_owner_reply_opportunity_for_path(10, "sub-1", "sub-2", 102)
        await self.store.list_pending_owner_reply_opportunities_for_author(10, 101, limit=5)
        await self.store.list_owner_reply_opportunities_for_root_submission("sub-1", limit=25)
        await self.store.list_owner_reply_opportunities_for_submission("sub-2", limit=25)
        await self.store.list_owner_reply_opportunities_for_responder_path(10, "sub-1", "sub-2", 102, limit=25)
        await self.store.fetch_enforcement_state(10, 101)
        await self.store.list_active_enforcement_states()
        await self.store.list_active_enforcement_states(guild_id=10)
        await self.store.list_review_queues()
        await self.store.fetch_review_queue(10)
        await self.store.list_review_surfaces(10, limit=25)
        await self.store.fetch_support_ticket(10, "CT-AAAA1111")
        await self.store.list_support_tickets(10, limit=25)
        await self.store.list_support_tickets(10, status="open", limit=25)
        counts = await self.store.fetch_guild_counts(10)
        global_status = await self.store.fetch_privacy_status()
        guild_status = await self.store.fetch_privacy_status(10)

        self.assertEqual(counts["queued_submissions"], 0)
        self.assertEqual(global_status["scope"], "global")
        self.assertEqual(guild_status["scope"], "guild")
        self.assertEqual(guild_status["guild_id"], 10)

    async def test_postgres_store_fetch_privacy_status_uses_expected_param_shape_for_global_and_guild_scopes(self):
        await self.store.fetch_privacy_status()
        global_calls = self.connection.fetch_calls[-7:]
        self.assertEqual(len(global_calls), 7)
        self.assertTrue(all(args == () for _, args in global_calls))

        await self.store.fetch_privacy_status(10)
        guild_calls = self.connection.fetch_calls[-7:]
        self.assertEqual(len(guild_calls), 7)
        self.assertTrue(all(args == (10,) for _, args in guild_calls))

    async def test_postgres_store_list_active_enforcement_states_uses_expected_param_shape(self):
        self.connection.fetch_results = [
            [],
            [],
            [],
            [],
        ]

        await self.store.list_active_enforcement_states()
        global_calls = self.connection.fetch_calls[-2:]
        self.assertEqual(len(global_calls), 2)
        self.assertTrue(all(args == () for _, args in global_calls))

        await self.store.list_active_enforcement_states(guild_id=10)
        guild_calls = self.connection.fetch_calls[-2:]
        self.assertEqual(len(guild_calls), 2)
        self.assertTrue(all(args == (10,) for _, args in guild_calls))


class ConfessionsStoreSqlAuditTests(unittest.TestCase):
    def test_postgres_literal_sql_statements_match_placeholder_arity(self):
        source_path = Path(__file__).resolve().parents[1] / "babblebox" / "confessions_store.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        mismatches: list[str] = []
        audited_calls = 0
        target_class = next(
            node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "_PostgresConfessionsStore"
        )

        class _Visitor(ast.NodeVisitor):
            def __init__(self):
                self.current_method = "<class>"

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
                previous = self.current_method
                self.current_method = node.name
                self.generic_visit(node)
                self.current_method = previous

            def visit_Call(self, node: ast.Call):
                nonlocal audited_calls
                func = node.func
                if (
                    isinstance(func, ast.Attribute)
                    and func.attr in {"execute", "fetch", "fetchrow"}
                    and node.args
                    and not any(isinstance(arg, ast.Starred) for arg in node.args[1:])
                ):
                    statement = _literal_sql_text(node.args[0])
                    if statement is not None:
                        audited_calls += 1
                        try:
                            _validate_statement_args(statement, tuple(node.args[1:]))
                        except AssertionError as exc:
                            mismatches.append(f"{self.current_method}:{func.attr}:{exc}")
                self.generic_visit(node)

        _Visitor().visit(target_class)

        self.assertGreater(audited_calls, 20)
        self.assertEqual(mismatches, [])


@unittest.skipUnless(os.getenv("CONFESSIONS_TEST_DATABASE_URL"), "requires CONFESSIONS_TEST_DATABASE_URL")
class PostgresConfessionsStoreLiveSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_postgres_upsert_config_round_trips_new_admin_fields(self):
        store = _PostgresConfessionsStore(os.environ["CONFESSIONS_TEST_DATABASE_URL"], _privacy())
        guild_id = 800000000 + (int.from_bytes(os.urandom(4), "big") % 1000000)
        try:
            await store.load()
            await store.upsert_config(
                {
                    "guild_id": guild_id,
                    "enabled": True,
                    "confession_channel_id": 20,
                    "panel_channel_id": 40,
                    "panel_message_id": 50,
                    "review_channel_id": 30,
                    "appeals_channel_id": 60,
                    "review_mode": True,
                    "block_adult_language": True,
                    "allow_trusted_mainstream_links": True,
                    "custom_allow_domains": ["google.com"],
                    "custom_block_domains": ["bad.example"],
                    "allowed_role_ids": [501],
                    "blocked_role_ids": [502],
                    "allow_images": True,
                    "image_review_required": True,
                    "allow_anonymous_replies": True,
                    "anonymous_reply_review_required": False,
                    "allow_owner_replies": True,
                    "owner_reply_review_mode": True,
                    "allow_self_edit": True,
                    "auto_moderation_exempt_admins": False,
                    "auto_moderation_exempt_role_ids": [901, 902],
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

            stored = await store.fetch_config(guild_id)

            self.assertIsNotNone(stored)
            self.assertEqual(stored["panel_channel_id"], 40)
            self.assertEqual(stored["panel_message_id"], 50)
            self.assertEqual(stored["appeals_channel_id"], 60)
            self.assertEqual(stored["allowed_role_ids"], [501])
            self.assertEqual(stored["blocked_role_ids"], [502])
            self.assertTrue(stored["allow_images"])
            self.assertTrue(stored["image_review_required"])
            self.assertTrue(stored["allow_anonymous_replies"])
            self.assertFalse(stored["anonymous_reply_review_required"])
            self.assertTrue(stored["allow_owner_replies"])
            self.assertTrue(stored["owner_reply_review_mode"])
            self.assertTrue(stored["allow_self_edit"])
            self.assertFalse(stored["auto_moderation_exempt_admins"])
            self.assertEqual(stored["auto_moderation_exempt_role_ids"], [901, 902])
            self.assertEqual(stored["strike_perm_ban_threshold"], 5)
        finally:
            if store._pool is not None:
                async with store._pool.acquire() as conn:
                    await conn.execute("DELETE FROM confession_guild_configs WHERE guild_id = $1", guild_id)
            await store.close()
