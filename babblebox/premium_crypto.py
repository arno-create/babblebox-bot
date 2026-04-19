from __future__ import annotations

import base64
import json
import os
import re
import secrets
from dataclasses import dataclass
from typing import Any

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF


PREMIUM_SECRET_KEY_ENV = "PREMIUM_SECRET_KEY"
PREMIUM_SECRET_KEY_ID_ENV = "PREMIUM_SECRET_KEY_ID"
PREMIUM_SECRET_LEGACY_KEYS_ENV = "PREMIUM_SECRET_LEGACY_KEYS"

_DEFAULT_ACTIVE_KEY_ID = "active"
_KEY_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
_ENVELOPE_PREFIX = "bbxp1:"
_KDF_SALT = b"babblebox-premium-v1"


class PremiumCryptoError(RuntimeError):
    pass


class PremiumKeyConfigError(PremiumCryptoError):
    pass


@dataclass(frozen=True)
class PremiumCryptoStatus:
    source: str
    ephemeral: bool
    active_key_id: str
    legacy_key_ids: tuple[str, ...]


@dataclass(frozen=True)
class _SecretKeyMaterial:
    key_id: str
    aead: AESGCM


def _urlsafe_b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _urlsafe_b64decode(raw: str) -> bytes:
    padded = raw + ("=" * ((4 - (len(raw) % 4)) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


def _normalize_secret(raw: str, *, env_name: str) -> bytes:
    cleaned = str(raw or "").strip()
    if len(cleaned) < 32:
        raise PremiumKeyConfigError(f"{env_name} must be at least 32 characters long.")
    return cleaned.encode("utf-8")


def _normalize_key_id(raw: str | None, *, env_name: str) -> str:
    cleaned = str(raw or "").strip() or _DEFAULT_ACTIVE_KEY_ID
    if not _KEY_ID_RE.fullmatch(cleaned):
        raise PremiumKeyConfigError(
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
            raise PremiumKeyConfigError(
                f"{env_name} entries must use key_id=secret and may be comma-separated."
            )
        key_id_raw, secret_raw = candidate.split("=", 1)
        entries.append(
            (
                _normalize_key_id(key_id_raw, env_name=env_name),
                _normalize_secret(secret_raw, env_name=env_name),
            )
        )
    return entries


def _derive_key(seed: bytes) -> bytes:
    return HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=_KDF_SALT,
        info=b"babblebox:premium-secret",
    ).derive(seed)


def _aad_bytes(label: str, aad_fields: dict[str, Any]) -> bytes:
    return json.dumps({"label": label, **aad_fields}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


class PremiumCrypto:
    def __init__(self, *, keys: list[tuple[str, bytes]], source: str, ephemeral: bool):
        if not keys:
            raise PremiumKeyConfigError("Premium secret keys could not be initialized.")
        seen: set[str] = set()
        self._keys: list[_SecretKeyMaterial] = []
        for key_id, seed in keys:
            if key_id in seen:
                raise PremiumKeyConfigError(f"Duplicate Premium key id '{key_id}' is not allowed.")
            seen.add(key_id)
            self._keys.append(_SecretKeyMaterial(key_id=key_id, aead=AESGCM(_derive_key(seed))))
        self._active_key = self._keys[0]
        self._key_by_id = {item.key_id: item for item in self._keys}
        self.status = PremiumCryptoStatus(
            source=source,
            ephemeral=ephemeral,
            active_key_id=self._active_key.key_id,
            legacy_key_ids=tuple(item.key_id for item in self._keys[1:]),
        )

    @classmethod
    def from_environment(cls, *, backend_name: str) -> "PremiumCrypto":
        secret_raw = os.getenv(PREMIUM_SECRET_KEY_ENV, "").strip()
        if secret_raw:
            keys = [
                (
                    _normalize_key_id(os.getenv(PREMIUM_SECRET_KEY_ID_ENV), env_name=PREMIUM_SECRET_KEY_ID_ENV),
                    _normalize_secret(secret_raw, env_name=PREMIUM_SECRET_KEY_ENV),
                )
            ]
            keys.extend(
                _parse_legacy_key_entries(
                    os.getenv(PREMIUM_SECRET_LEGACY_KEYS_ENV),
                    env_name=PREMIUM_SECRET_LEGACY_KEYS_ENV,
                )
            )
            return cls(keys=keys, source="environment", ephemeral=False)
        if backend_name in {"memory", "test", "dev"}:
            return cls(keys=[("ephemeral", secrets.token_bytes(32))], source="ephemeral", ephemeral=True)
        raise PremiumKeyConfigError(
            f"Premium secret keys are missing. Set {PREMIUM_SECRET_KEY_ENV} before starting Postgres-backed Premium storage."
        )

    def encrypt_secret(self, *, label: str, aad_fields: dict[str, Any], secret: str) -> str:
        nonce = secrets.token_bytes(12)
        encoded = secret.encode("utf-8")
        cipher = self._active_key.aead.encrypt(nonce, encoded, _aad_bytes(label, aad_fields))
        return f"{_ENVELOPE_PREFIX}{self._active_key.key_id}:{_urlsafe_b64encode(nonce + cipher)}"

    def decrypt_secret(self, *, label: str, aad_fields: dict[str, Any], envelope: str | None) -> str | None:
        cleaned = str(envelope or "").strip()
        if not cleaned:
            return None
        if not cleaned.startswith(_ENVELOPE_PREFIX):
            raise PremiumCryptoError("Unsupported premium secret envelope version.")
        payload = cleaned[len(_ENVELOPE_PREFIX) :]
        if ":" not in payload:
            raise PremiumCryptoError("Malformed premium secret envelope.")
        key_id, packed_value = payload.split(":", 1)
        materials = [self._key_by_id.get(key_id)]
        packed = _urlsafe_b64decode(packed_value)
        if len(packed) <= 12:
            raise PremiumCryptoError("Malformed premium secret envelope.")
        nonce = packed[:12]
        cipher = packed[12:]
        decoded = None
        for material in materials:
            if material is None:
                continue
            try:
                decoded = material.aead.decrypt(nonce, cipher, _aad_bytes(label, aad_fields))
                break
            except Exception:
                continue
        if decoded is None:
            raise PremiumCryptoError("Premium secret could not be decrypted.")
        return decoded.decode("utf-8")

