from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


CONFESSIONS_CONTENT_KEY_ENV = "CONFESSIONS_CONTENT_KEY"
CONFESSIONS_IDENTITY_KEY_ENV = "CONFESSIONS_IDENTITY_KEY"
CONFESSIONS_CONTENT_KEY_ID_ENV = "CONFESSIONS_CONTENT_KEY_ID"
CONFESSIONS_IDENTITY_KEY_ID_ENV = "CONFESSIONS_IDENTITY_KEY_ID"
CONFESSIONS_CONTENT_LEGACY_KEYS_ENV = "CONFESSIONS_CONTENT_LEGACY_KEYS"
CONFESSIONS_IDENTITY_LEGACY_KEYS_ENV = "CONFESSIONS_IDENTITY_LEGACY_KEYS"

_DEFAULT_ACTIVE_KEY_ID = "active"
_KEY_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

_ENVELOPE_VERSION_V1 = "bbx1"
_ENVELOPE_VERSION_V2 = "bbx2"
_ENVELOPE_PREFIX_V1 = f"{_ENVELOPE_VERSION_V1}:"
_ENVELOPE_PREFIX_V2 = f"{_ENVELOPE_VERSION_V2}:"
_KDF_SALT = b"babblebox-confessions-v1"
_BLIND_INDEX_PREFIX_V1 = "bi1:"
_BLIND_INDEX_PREFIX_V2 = "bi2:"
_EXACT_HASH_PREFIX_V1 = "h1:"
_EXACT_HASH_PREFIX_V2 = "h2:"
_FUZZY_HASH_PREFIX_V1 = "fh1:"
_FUZZY_HASH_PREFIX_V2 = "fh2:"


class ConfessionsCryptoError(RuntimeError):
    pass


class ConfessionsKeyConfigError(ConfessionsCryptoError):
    pass


@dataclass(frozen=True)
class ConfessionsCryptoStatus:
    content_source: str
    identity_source: str
    ephemeral: bool
    active_content_key_id: str
    active_identity_key_id: str
    legacy_content_key_ids: tuple[str, ...]
    legacy_identity_key_ids: tuple[str, ...]


@dataclass(frozen=True)
class _ContentKeyMaterial:
    key_id: str
    aead: AESGCM
    exact_key: bytes
    fuzzy_key: bytes


@dataclass(frozen=True)
class _IdentityKeyMaterial:
    key_id: str
    aead: AESGCM
    lookup_key: bytes


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


def _normalize_key_id(raw: str | None, *, env_name: str) -> str:
    cleaned = str(raw or "").strip() or _DEFAULT_ACTIVE_KEY_ID
    if not _KEY_ID_RE.fullmatch(cleaned):
        raise ConfessionsKeyConfigError(
            f"{env_name} must use only letters, numbers, underscore, or dash and be at most 32 characters."
        )
    return cleaned


def _parse_legacy_key_entries(raw: str | None, *, env_name: str) -> list[tuple[str, bytes]]:
    cleaned = str(raw or "").strip()
    if not cleaned:
        return []
    entries: list[tuple[str, bytes]] = []
    for item in re.split(r"[\n,;]+", cleaned):
        candidate = item.strip()
        if not candidate:
            continue
        if "=" not in candidate:
            raise ConfessionsKeyConfigError(
                f"{env_name} entries must use the format key_id=secret and may be comma-separated."
            )
        key_id_raw, secret_raw = candidate.split("=", 1)
        key_id = _normalize_key_id(key_id_raw, env_name=env_name)
        secret = _normalize_secret(secret_raw, env_name=env_name)
        entries.append((key_id, secret))
    return entries


def _dedupe_preserve_order(values: list[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


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


def _parse_v2_prefixed_value(value: str | None, *, prefix: str) -> tuple[str, str] | None:
    cleaned = str(value or "").strip()
    if not cleaned.startswith(prefix):
        return None
    remainder = cleaned[len(prefix) :]
    if ":" not in remainder:
        return None
    key_id, payload = remainder.split(":", 1)
    if not key_id or not payload:
        return None
    return key_id, payload


class ConfessionsCrypto:
    def __init__(
        self,
        *,
        content_keys: list[tuple[str, bytes]],
        identity_keys: list[tuple[str, bytes]],
        content_source: str,
        identity_source: str,
        ephemeral: bool,
    ):
        if not content_keys or not identity_keys:
            raise ConfessionsKeyConfigError("Confessions privacy keys could not be initialized.")
        self._content_keys = self._build_content_keyring(content_keys)
        self._identity_keys = self._build_identity_keyring(identity_keys)
        self._active_content_key = self._content_keys[0]
        self._active_identity_key = self._identity_keys[0]
        self._content_key_by_id = {item.key_id: item for item in self._content_keys}
        self._identity_key_by_id = {item.key_id: item for item in self._identity_keys}
        self.status = ConfessionsCryptoStatus(
            content_source=content_source,
            identity_source=identity_source,
            ephemeral=ephemeral,
            active_content_key_id=self._active_content_key.key_id,
            active_identity_key_id=self._active_identity_key.key_id,
            legacy_content_key_ids=tuple(item.key_id for item in self._content_keys[1:]),
            legacy_identity_key_ids=tuple(item.key_id for item in self._identity_keys[1:]),
        )

    @staticmethod
    def _build_content_keyring(entries: list[tuple[str, bytes]]) -> list[_ContentKeyMaterial]:
        keyring: list[_ContentKeyMaterial] = []
        seen: set[str] = set()
        for key_id, seed in entries:
            if key_id in seen:
                raise ConfessionsKeyConfigError(f"Duplicate Confessions content key id '{key_id}' is not allowed.")
            seen.add(key_id)
            keyring.append(
                _ContentKeyMaterial(
                    key_id=key_id,
                    aead=AESGCM(_derive_key(seed, "content-aead")),
                    exact_key=_derive_key(seed, "duplicate-exact"),
                    fuzzy_key=_derive_key(seed, "duplicate-fuzzy"),
                )
            )
        return keyring

    @staticmethod
    def _build_identity_keyring(entries: list[tuple[str, bytes]]) -> list[_IdentityKeyMaterial]:
        keyring: list[_IdentityKeyMaterial] = []
        seen: set[str] = set()
        for key_id, seed in entries:
            if key_id in seen:
                raise ConfessionsKeyConfigError(f"Duplicate Confessions identity key id '{key_id}' is not allowed.")
            seen.add(key_id)
            keyring.append(
                _IdentityKeyMaterial(
                    key_id=key_id,
                    aead=AESGCM(_derive_key(seed, "identity-aead")),
                    lookup_key=_derive_key(seed, "identity-lookup"),
                )
            )
        return keyring

    @classmethod
    def from_environment(cls, *, backend_name: str) -> "ConfessionsCrypto":
        content_raw = os.getenv(CONFESSIONS_CONTENT_KEY_ENV, "").strip()
        identity_raw = os.getenv(CONFESSIONS_IDENTITY_KEY_ENV, "").strip()
        ephemeral = False
        if content_raw and identity_raw:
            content_key_id = _normalize_key_id(
                os.getenv(CONFESSIONS_CONTENT_KEY_ID_ENV),
                env_name=CONFESSIONS_CONTENT_KEY_ID_ENV,
            )
            identity_key_id = _normalize_key_id(
                os.getenv(CONFESSIONS_IDENTITY_KEY_ID_ENV),
                env_name=CONFESSIONS_IDENTITY_KEY_ID_ENV,
            )
            content_keys = [(content_key_id, _normalize_secret(content_raw, env_name=CONFESSIONS_CONTENT_KEY_ENV))]
            content_keys.extend(
                _parse_legacy_key_entries(
                    os.getenv(CONFESSIONS_CONTENT_LEGACY_KEYS_ENV),
                    env_name=CONFESSIONS_CONTENT_LEGACY_KEYS_ENV,
                )
            )
            identity_keys = [(identity_key_id, _normalize_secret(identity_raw, env_name=CONFESSIONS_IDENTITY_KEY_ENV))]
            identity_keys.extend(
                _parse_legacy_key_entries(
                    os.getenv(CONFESSIONS_IDENTITY_LEGACY_KEYS_ENV),
                    env_name=CONFESSIONS_IDENTITY_LEGACY_KEYS_ENV,
                )
            )
            return cls(
                content_keys=content_keys,
                identity_keys=identity_keys,
                content_source="environment",
                identity_source="environment",
                ephemeral=ephemeral,
            )
        if backend_name in {"memory", "test", "dev"}:
            return cls(
                content_keys=[("ephemeral", secrets.token_bytes(32))],
                identity_keys=[("ephemeral", secrets.token_bytes(32))],
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

    def _keyring_for_domain(self, key_domain: str) -> list[_ContentKeyMaterial] | list[_IdentityKeyMaterial]:
        return self._identity_keys if key_domain == "identity" else self._content_keys

    def _active_key_for_domain(self, key_domain: str) -> _ContentKeyMaterial | _IdentityKeyMaterial:
        return self._active_identity_key if key_domain == "identity" else self._active_content_key

    def _lookup_key_for_domain(self, key_domain: str, key_id: str) -> _ContentKeyMaterial | _IdentityKeyMaterial | None:
        if key_domain == "identity":
            return self._identity_key_by_id.get(key_id)
        return self._content_key_by_id.get(key_id)

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
        material = self._active_key_for_domain(key_domain)
        cipher = material.aead.encrypt(nonce, encoded, aad)
        return f"{_ENVELOPE_PREFIX_V2}{material.key_id}:{_urlsafe_b64encode(nonce + cipher)}"

    def decrypt_payload(
        self,
        *,
        domain: str,
        aad_fields: dict[str, Any],
        envelope: str,
        key_domain: str,
    ) -> dict[str, Any]:
        cleaned = str(envelope or "").strip()
        if not cleaned.startswith((_ENVELOPE_PREFIX_V1, _ENVELOPE_PREFIX_V2)):
            raise ConfessionsCryptoError("Unsupported confessions privacy envelope version.")
        if cleaned.startswith(_ENVELOPE_PREFIX_V2):
            parsed = _parse_v2_prefixed_value(cleaned, prefix=_ENVELOPE_PREFIX_V2)
            if parsed is None:
                raise ConfessionsCryptoError("Malformed confessions privacy envelope.")
            key_id, packed_value = parsed
            materials = [self._lookup_key_for_domain(key_domain, key_id)]
        else:
            packed_value = cleaned[len(_ENVELOPE_PREFIX_V1) :]
            materials = list(self._keyring_for_domain(key_domain))
        try:
            packed = _urlsafe_b64decode(packed_value)
        except Exception as exc:
            raise ConfessionsCryptoError("Malformed confessions privacy envelope.") from exc
        if len(packed) <= 12:
            raise ConfessionsCryptoError("Malformed confessions privacy envelope.")
        nonce = packed[:12]
        cipher = packed[12:]
        aad = _aad_bytes(domain, aad_fields)
        decoded = None
        for material in materials:
            if material is None:
                continue
            try:
                decoded = material.aead.decrypt(nonce, cipher, aad)
                break
            except Exception:
                continue
        if decoded is None:
            raise ConfessionsCryptoError("Confessions privacy payload could not be decrypted.")
        try:
            payload = json.loads(decoded.decode("utf-8"))
        except Exception as exc:
            raise ConfessionsCryptoError("Confessions privacy payload was not valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ConfessionsCryptoError("Confessions privacy payload must decode to an object.")
        return payload

    @staticmethod
    def _identity_canonical(label: str, guild_id: int, value: str | int) -> bytes:
        return f"{label}:{guild_id}:{value}".encode("utf-8")

    @staticmethod
    def _content_canonical(guild_id: int, canonical_text: str) -> bytes:
        return f"{guild_id}:{canonical_text}".encode("utf-8")

    def blind_index(self, *, label: str, guild_id: int, value: str | int) -> str:
        canonical = self._identity_canonical(label, guild_id, value)
        digest = hmac.new(self._active_identity_key.lookup_key, canonical, hashlib.sha256).hexdigest()
        return f"{_BLIND_INDEX_PREFIX_V2}{self._active_identity_key.key_id}:{digest}"

    def blind_index_candidates(self, *, label: str, guild_id: int, value: str | int) -> tuple[str, ...]:
        canonical = self._identity_canonical(label, guild_id, value)
        values: list[str] = []
        for material in self._identity_keys:
            digest = hmac.new(material.lookup_key, canonical, hashlib.sha256).hexdigest()
            values.append(f"{_BLIND_INDEX_PREFIX_V2}{material.key_id}:{digest}")
        for material in self._identity_keys:
            digest = hmac.new(material.lookup_key, canonical, hashlib.sha256).hexdigest()
            values.append(f"{_BLIND_INDEX_PREFIX_V1}{digest}")
        return _dedupe_preserve_order(values)

    def exact_duplicate_hash(self, canonical_text: str, *, guild_id: int) -> str:
        canonical = self._content_canonical(guild_id, canonical_text)
        digest = hmac.new(self._active_content_key.exact_key, canonical, hashlib.sha256).hexdigest()
        return f"{_EXACT_HASH_PREFIX_V2}{self._active_content_key.key_id}:{digest}"

    def exact_duplicate_hash_candidates(self, canonical_text: str, *, guild_id: int) -> tuple[str, ...]:
        scoped = self._content_canonical(guild_id, canonical_text)
        legacy = canonical_text.encode("utf-8")
        values: list[str] = []
        for material in self._content_keys:
            digest = hmac.new(material.exact_key, scoped, hashlib.sha256).hexdigest()
            values.append(f"{_EXACT_HASH_PREFIX_V2}{material.key_id}:{digest}")
        for material in self._content_keys:
            digest = hmac.new(material.exact_key, legacy, hashlib.sha256).hexdigest()
            values.append(f"{_EXACT_HASH_PREFIX_V1}{digest}")
        return _dedupe_preserve_order(values)

    def transform_legacy_exact_hash(self, legacy_value: str | None, *, guild_id: int) -> str | None:
        cleaned = str(legacy_value or "").strip()
        if not cleaned:
            return None
        canonical = f"legacy-scope:{guild_id}:{cleaned}".encode("utf-8")
        digest = hmac.new(self._active_content_key.exact_key, canonical, hashlib.sha256).hexdigest()
        return f"{_EXACT_HASH_PREFIX_V2}{self._active_content_key.key_id}:{digest}"

    @staticmethod
    def _simhash(material_key: bytes, tokens: list[str]) -> str | None:
        if not tokens:
            return None
        vector = [0] * 64
        for token in tokens:
            digest = hmac.new(material_key, token.encode("utf-8"), hashlib.sha256).digest()
            bits = int.from_bytes(digest[:8], "big")
            for index in range(64):
                vector[index] += 1 if bits & (1 << index) else -1
        value = 0
        for index, score in enumerate(vector):
            if score >= 0:
                value |= 1 << index
        return f"{value:016x}"

    def fuzzy_duplicate_signature(self, tokens: list[str], *, guild_id: int) -> str | None:
        scoped_tokens = [f"{guild_id}:{token}" for token in tokens]
        encoded = self._simhash(self._active_content_key.fuzzy_key, scoped_tokens)
        if encoded is None:
            return None
        return f"{_FUZZY_HASH_PREFIX_V2}{self._active_content_key.key_id}:{encoded}"

    def fuzzy_duplicate_signature_candidates(self, tokens: list[str], *, guild_id: int) -> tuple[str, ...]:
        values: list[str] = []
        scoped_tokens = [f"{guild_id}:{token}" for token in tokens]
        for material in self._content_keys:
            encoded = self._simhash(material.fuzzy_key, scoped_tokens)
            if encoded is not None:
                values.append(f"{_FUZZY_HASH_PREFIX_V2}{material.key_id}:{encoded}")
        for material in self._content_keys:
            encoded = self._simhash(material.fuzzy_key, tokens)
            if encoded is not None:
                values.append(f"{_FUZZY_HASH_PREFIX_V1}{encoded}")
        return _dedupe_preserve_order(values)

    def transform_legacy_fuzzy_signature(self, legacy_value: str | None, *, guild_id: int) -> str | None:
        stripped = self.strip_fuzzy_signature_prefix(legacy_value)
        if not stripped:
            return None
        try:
            raw_bits = int(stripped, 16)
        except ValueError:
            return None
        mask = int.from_bytes(
            hmac.new(
                self._active_content_key.fuzzy_key,
                f"legacy-scope:{guild_id}".encode("utf-8"),
                hashlib.sha256,
            ).digest()[:8],
            "big",
        )
        transformed = raw_bits ^ mask
        return f"{_FUZZY_HASH_PREFIX_V2}{self._active_content_key.key_id}:{transformed:016x}"

    @staticmethod
    def strip_fuzzy_signature_prefix(value: str | None) -> str:
        cleaned = str(value or "").strip()
        if cleaned.startswith(_FUZZY_HASH_PREFIX_V1):
            return cleaned[len(_FUZZY_HASH_PREFIX_V1) :]
        parsed = _parse_v2_prefixed_value(cleaned, prefix=_FUZZY_HASH_PREFIX_V2)
        return parsed[1] if parsed is not None else cleaned

    @staticmethod
    def is_keyed_exact_hash(value: str | None) -> bool:
        cleaned = str(value or "").strip()
        return cleaned.startswith(_EXACT_HASH_PREFIX_V1) or cleaned.startswith(_EXACT_HASH_PREFIX_V2)

    @staticmethod
    def is_keyed_fuzzy_signature(value: str | None) -> bool:
        cleaned = str(value or "").strip()
        return cleaned.startswith(_FUZZY_HASH_PREFIX_V1) or cleaned.startswith(_FUZZY_HASH_PREFIX_V2)

    @staticmethod
    def is_blind_index(value: str | None) -> bool:
        cleaned = str(value or "").strip()
        return cleaned.startswith(_BLIND_INDEX_PREFIX_V1) or cleaned.startswith(_BLIND_INDEX_PREFIX_V2)

    @staticmethod
    def is_versioned_envelope(value: str | None) -> bool:
        cleaned = str(value or "").strip()
        return cleaned.startswith(_ENVELOPE_PREFIX_V1) or cleaned.startswith(_ENVELOPE_PREFIX_V2)

    def envelope_is_active(self, envelope: str | None, *, key_domain: str) -> bool:
        parsed = _parse_v2_prefixed_value(envelope, prefix=_ENVELOPE_PREFIX_V2)
        if parsed is None:
            return False
        key_id, _ = parsed
        active = self._active_identity_key.key_id if key_domain == "identity" else self._active_content_key.key_id
        return key_id == active

    def active_envelope_prefix(self, *, key_domain: str) -> str:
        active = self._active_identity_key.key_id if key_domain == "identity" else self._active_content_key.key_id
        return f"{_ENVELOPE_PREFIX_V2}{active}:"

    def blind_index_is_active(self, value: str | None) -> bool:
        parsed = _parse_v2_prefixed_value(value, prefix=_BLIND_INDEX_PREFIX_V2)
        return parsed is not None and parsed[0] == self._active_identity_key.key_id

    def active_blind_index_prefix(self) -> str:
        return f"{_BLIND_INDEX_PREFIX_V2}{self._active_identity_key.key_id}:"

    def exact_duplicate_hash_is_active(self, value: str | None) -> bool:
        parsed = _parse_v2_prefixed_value(value, prefix=_EXACT_HASH_PREFIX_V2)
        return parsed is not None and parsed[0] == self._active_content_key.key_id

    def active_exact_duplicate_hash_prefix(self) -> str:
        return f"{_EXACT_HASH_PREFIX_V2}{self._active_content_key.key_id}:"

    def fuzzy_signature_is_active(self, value: str | None) -> bool:
        parsed = _parse_v2_prefixed_value(value, prefix=_FUZZY_HASH_PREFIX_V2)
        return parsed is not None and parsed[0] == self._active_content_key.key_id

    def active_fuzzy_signature_prefix(self) -> str:
        return f"{_FUZZY_HASH_PREFIX_V2}{self._active_content_key.key_id}:"
