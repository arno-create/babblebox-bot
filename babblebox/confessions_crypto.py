from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


CONFESSIONS_CONTENT_KEY_ENV = "CONFESSIONS_CONTENT_KEY"
CONFESSIONS_IDENTITY_KEY_ENV = "CONFESSIONS_IDENTITY_KEY"

_ENVELOPE_VERSION = "bbx1"
_ENVELOPE_PREFIX = f"{_ENVELOPE_VERSION}:"
_KDF_SALT = b"babblebox-confessions-v1"
_BLIND_INDEX_PREFIX = "bi1:"
_EXACT_HASH_PREFIX = "h1:"
_FUZZY_HASH_PREFIX = "fh1:"


class ConfessionsCryptoError(RuntimeError):
    pass


class ConfessionsKeyConfigError(ConfessionsCryptoError):
    pass


@dataclass(frozen=True)
class ConfessionsCryptoStatus:
    content_source: str
    identity_source: str
    ephemeral: bool


def _urlsafe_b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _urlsafe_b64decode(raw: str) -> bytes:
    padded = raw + ("=" * ((4 - (len(raw) % 4)) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _normalize_secret(raw: str, *, env_name: str) -> bytes:
    cleaned = raw.strip()
    if len(cleaned) < 32:
        raise ConfessionsKeyConfigError(f"{env_name} must be at least 32 characters long.")
    return cleaned.encode("utf-8")


def _derive_key(seed: bytes, label: str) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=_KDF_SALT,
        info=f"babblebox:{label}".encode("utf-8"),
    ).derive(seed)


def _aad_bytes(domain: str, aad_fields: dict[str, Any]) -> bytes:
    payload = {"domain": domain, **aad_fields}
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


class ConfessionsCrypto:
    def __init__(
        self,
        *,
        content_seed: bytes,
        identity_seed: bytes,
        content_source: str,
        identity_source: str,
        ephemeral: bool,
    ):
        self.status = ConfessionsCryptoStatus(
            content_source=content_source,
            identity_source=identity_source,
            ephemeral=ephemeral,
        )
        self._content_aead = AESGCM(_derive_key(content_seed, "content-aead"))
        self._identity_aead = AESGCM(_derive_key(identity_seed, "identity-aead"))
        self._identity_lookup_key = _derive_key(identity_seed, "identity-lookup")
        self._duplicate_exact_key = _derive_key(content_seed, "duplicate-exact")
        self._duplicate_fuzzy_key = _derive_key(content_seed, "duplicate-fuzzy")

    @classmethod
    def from_environment(cls, *, backend_name: str) -> "ConfessionsCrypto":
        content_raw = os.getenv(CONFESSIONS_CONTENT_KEY_ENV, "").strip()
        identity_raw = os.getenv(CONFESSIONS_IDENTITY_KEY_ENV, "").strip()
        ephemeral = False
        if content_raw and identity_raw:
            content_seed = _normalize_secret(content_raw, env_name=CONFESSIONS_CONTENT_KEY_ENV)
            identity_seed = _normalize_secret(identity_raw, env_name=CONFESSIONS_IDENTITY_KEY_ENV)
            return cls(
                content_seed=content_seed,
                identity_seed=identity_seed,
                content_source="environment",
                identity_source="environment",
                ephemeral=ephemeral,
            )
        if backend_name in {"memory", "test", "dev"}:
            return cls(
                content_seed=secrets.token_bytes(32),
                identity_seed=secrets.token_bytes(32),
                content_source="ephemeral",
                identity_source="ephemeral",
                ephemeral=True,
            )
        missing = []
        if not content_raw:
            missing.append(CONFESSIONS_CONTENT_KEY_ENV)
        if not identity_raw:
            missing.append(CONFESSIONS_IDENTITY_KEY_ENV)
        raise ConfessionsKeyConfigError(
            "Confessions privacy keys are missing. Set "
            + ", ".join(missing)
            + " before starting Postgres-backed Confessions."
        )

    def encrypt_payload(
        self,
        *,
        domain: str,
        aad_fields: dict[str, Any],
        payload: dict[str, Any],
        key_domain: str,
    ) -> str:
        nonce = secrets.token_bytes(12)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        aad = _aad_bytes(domain, aad_fields)
        if key_domain == "identity":
            cipher = self._identity_aead.encrypt(nonce, encoded, aad)
        else:
            cipher = self._content_aead.encrypt(nonce, encoded, aad)
        return f"{_ENVELOPE_PREFIX}{_urlsafe_b64encode(nonce + cipher)}"

    def decrypt_payload(
        self,
        *,
        domain: str,
        aad_fields: dict[str, Any],
        envelope: str,
        key_domain: str,
    ) -> dict[str, Any]:
        if not isinstance(envelope, str) or not envelope.startswith(_ENVELOPE_PREFIX):
            raise ConfessionsCryptoError("Unsupported confessions privacy envelope version.")
        try:
            packed = _urlsafe_b64decode(envelope[len(_ENVELOPE_PREFIX) :])
        except Exception as exc:
            raise ConfessionsCryptoError("Malformed confessions privacy envelope.") from exc
        if len(packed) <= 12:
            raise ConfessionsCryptoError("Malformed confessions privacy envelope.")
        nonce = packed[:12]
        cipher = packed[12:]
        aad = _aad_bytes(domain, aad_fields)
        try:
            if key_domain == "identity":
                decoded = self._identity_aead.decrypt(nonce, cipher, aad)
            else:
                decoded = self._content_aead.decrypt(nonce, cipher, aad)
        except Exception as exc:
            raise ConfessionsCryptoError("Confessions privacy payload could not be decrypted.") from exc
        try:
            payload = json.loads(decoded.decode("utf-8"))
        except Exception as exc:
            raise ConfessionsCryptoError("Confessions privacy payload was not valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ConfessionsCryptoError("Confessions privacy payload must decode to an object.")
        return payload

    def blind_index(self, *, label: str, guild_id: int, value: str | int) -> str:
        canonical = f"{label}:{guild_id}:{value}".encode("utf-8")
        digest = hmac.new(self._identity_lookup_key, canonical, hashlib.sha256).hexdigest()
        return f"{_BLIND_INDEX_PREFIX}{digest}"

    def exact_duplicate_hash(self, canonical_text: str) -> str:
        digest = hmac.new(self._duplicate_exact_key, canonical_text.encode("utf-8"), hashlib.sha256).hexdigest()
        return f"{_EXACT_HASH_PREFIX}{digest}"

    def fuzzy_duplicate_signature(self, tokens: list[str]) -> str | None:
        if not tokens:
            return None
        vector = [0] * 64
        for token in tokens:
            digest = hmac.new(self._duplicate_fuzzy_key, token.encode("utf-8"), hashlib.sha256).digest()
            bits = int.from_bytes(digest[:8], "big")
            for index in range(64):
                vector[index] += 1 if bits & (1 << index) else -1
        value = 0
        for index, score in enumerate(vector):
            if score >= 0:
                value |= 1 << index
        return f"{_FUZZY_HASH_PREFIX}{value:016x}"

    @staticmethod
    def strip_fuzzy_signature_prefix(value: str | None) -> str:
        cleaned = str(value or "").strip()
        return cleaned[len(_FUZZY_HASH_PREFIX) :] if cleaned.startswith(_FUZZY_HASH_PREFIX) else cleaned

    @staticmethod
    def is_keyed_exact_hash(value: str | None) -> bool:
        return str(value or "").startswith(_EXACT_HASH_PREFIX)

    @staticmethod
    def is_keyed_fuzzy_signature(value: str | None) -> bool:
        return str(value or "").startswith(_FUZZY_HASH_PREFIX)

    @staticmethod
    def is_blind_index(value: str | None) -> bool:
        return str(value or "").startswith(_BLIND_INDEX_PREFIX)
