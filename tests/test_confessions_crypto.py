import unittest

from babblebox.confessions_crypto import ConfessionsCrypto, ConfessionsCryptoError


class ConfessionsCryptoTests(unittest.TestCase):
    def setUp(self):
        self.privacy = ConfessionsCrypto.from_environment(backend_name="test")

    def test_encrypt_round_trip_uses_versioned_envelope(self):
        envelope = self.privacy.encrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            payload={"content_body": "hello", "staff_preview": "preview"},
            key_domain="content",
        )
        self.assertTrue(envelope.startswith("bbx1:"))
        decoded = self.privacy.decrypt_payload(
            domain="submission-content",
            aad_fields={"guild_id": 10, "submission_id": "sub-1", "confession_id": "CF-AAAA1111"},
            envelope=envelope,
            key_domain="content",
        )
        self.assertEqual(decoded["content_body"], "hello")
        self.assertEqual(decoded["staff_preview"], "preview")

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

    def test_blind_indexes_and_duplicate_helpers_are_prefixed_and_stable(self):
        blind_index = self.privacy.blind_index(label="author-link", guild_id=10, value=123)
        exact_hash = self.privacy.exact_duplicate_hash("hello world")
        fuzzy_signature = self.privacy.fuzzy_duplicate_signature(["hello", "world"])

        self.assertTrue(blind_index.startswith("bi1:"))
        self.assertTrue(exact_hash.startswith("h1:"))
        self.assertTrue(fuzzy_signature.startswith("fh1:"))
        self.assertEqual(
            blind_index,
            self.privacy.blind_index(label="author-link", guild_id=10, value=123),
        )
        self.assertNotEqual(
            blind_index,
            self.privacy.blind_index(label="author-link", guild_id=10, value=124),
        )
