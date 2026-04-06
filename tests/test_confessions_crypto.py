import unittest

from babblebox.confessions_crypto import ConfessionsCrypto, ConfessionsCryptoError


class ConfessionsCryptoTests(unittest.TestCase):
    def setUp(self):
        self.privacy = ConfessionsCrypto.from_environment(backend_name="test")

    def test_encrypt_round_trip_uses_key_aware_v2_envelope(self):
        envelope = self.privacy.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"content_body": "hello", "staff_preview": "preview"},
            key_domain="content",
        )
        self.assertTrue(envelope.startswith("bbx2:ephemeral:"))
        decoded = self.privacy.decrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            envelope=envelope,
            key_domain="content",
        )
        self.assertEqual(decoded["content_body"], "hello")
        self.assertEqual(decoded["staff_preview"], "preview")

    def test_v1_envelope_remains_readable(self):
        envelope = self.privacy.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"content_body": "hello"},
            key_domain="content",
        )
        legacy_envelope = "bbx1:" + envelope.split(":", 2)[2]
        decoded = self.privacy.decrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            envelope=legacy_envelope,
            key_domain="content",
        )
        self.assertEqual(decoded["content_body"], "hello")

    def test_decrypt_rejects_wrong_aad_and_wrong_key(self):
        envelope = self.privacy.encrypt_payload(
            domain="author-link",
            aad_fields={"guild_id": 10, "submission_id": "sub-1"},
            payload={"author_user_id": 123},
            key_domain="identity",
        )
        with self.assertRaises(ConfessionsCryptoError):
            self.privacy.decrypt_payload(
                domain="author-link",
                aad_fields={"guild_id": 11, "submission_id": "sub-1"},
                envelope=envelope,
                key_domain="identity",
            )
        other_privacy = ConfessionsCrypto.from_environment(backend_name="test")
        with self.assertRaises(ConfessionsCryptoError):
            other_privacy.decrypt_payload(
                domain="author-link",
                aad_fields={"guild_id": 10, "submission_id": "sub-1"},
                envelope=envelope,
                key_domain="identity",
            )

    def test_blind_indexes_and_duplicate_helpers_are_prefixed_stable_and_guild_scoped(self):
        blind_index = self.privacy.blind_index(label="author-link", guild_id=10, value=123)
        exact_hash = self.privacy.exact_duplicate_hash("hello world", guild_id=10)
        other_guild_exact_hash = self.privacy.exact_duplicate_hash("hello world", guild_id=11)
        fuzzy_signature = self.privacy.fuzzy_duplicate_signature(["hello", "world"], guild_id=10)
        other_guild_fuzzy_signature = self.privacy.fuzzy_duplicate_signature(["hello", "world"], guild_id=11)

        self.assertTrue(blind_index.startswith("bi2:ephemeral:"))
        self.assertTrue(exact_hash.startswith("h2:ephemeral:"))
        self.assertTrue(fuzzy_signature.startswith("fh2:ephemeral:"))
        self.assertEqual(
            blind_index,
            self.privacy.blind_index(label="author-link", guild_id=10, value=123),
        )
        self.assertNotEqual(
            blind_index,
            self.privacy.blind_index(label="author-link", guild_id=10, value=124),
        )
        self.assertNotEqual(exact_hash, other_guild_exact_hash)
        self.assertNotEqual(fuzzy_signature, other_guild_fuzzy_signature)

    def test_active_write_and_legacy_read_work_with_keyring(self):
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

        old_envelope = old_only.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"content_body": "hello"},
            key_domain="content",
        )
        decoded = rotated.decrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            envelope=old_envelope,
            key_domain="content",
        )
        self.assertEqual(decoded["content_body"], "hello")

        new_envelope = rotated.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-2", "confession_id": "CF-BBBB2222"},
            payload={"content_body": "new"},
            key_domain="content",
        )
        self.assertTrue(new_envelope.startswith("bbx2:current:"))
        self.assertIn("bi2:current:", rotated.blind_index_candidates(label="author-link", guild_id=10, value=123)[0])
        self.assertIn("old", rotated.status.legacy_content_key_ids)
        self.assertIn("old", rotated.status.legacy_identity_key_ids)

    def test_unknown_key_id_fails_closed(self):
        envelope = self.privacy.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"content_body": "hello"},
            key_domain="content",
        )
        tampered = envelope.replace("bbx2:ephemeral:", "bbx2:missing:", 1)
        with self.assertRaises(ConfessionsCryptoError):
            self.privacy.decrypt_payload(
                domain="submission-content",
                aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
                envelope=tampered,
                key_domain="content",
            )
