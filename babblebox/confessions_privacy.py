from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Sequence

from babblebox.confessions_crypto import ConfessionsCrypto
from babblebox.text_safety import normalize_plain_text


TOKEN_RE = re.compile(r"[a-z0-9']+")


@dataclass(frozen=True)
class DuplicateSignals:
    exact_hash: str | None
    fuzzy_signature: str | None
    keyed_exact_candidates: tuple[str, ...]
    keyed_fuzzy_candidates: tuple[str, ...]
    legacy_exact_hash: str | None
    legacy_similarity_key: str | None
    legacy_fuzzy_signature: str | None


def duplicate_tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text.casefold())


def canonical_duplicate_text(
    text: str,
    attachment_meta: Sequence[dict[str, Any]],
    shared_link_url: str | None = None,
) -> str:
    lowered = normalize_plain_text(text).casefold()
    shared_link = normalize_plain_text(shared_link_url).casefold() if shared_link_url else ""
    attachment_signature = " ".join(str(item.get("kind") or "").casefold() for item in attachment_meta)
    attachment_count = f"attachments:{len(attachment_meta)}" if attachment_meta else ""
    return normalize_plain_text(f"{lowered} {shared_link} {attachment_signature} {attachment_count}").casefold()


def _legacy_duplicate_signals(canonical: str) -> tuple[str | None, str | None, str | None]:
    if not canonical:
        return None, None, None
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    similarity = " ".join(duplicate_tokens(canonical)[:24])[:160] or canonical[:160]
    signature_tokens = duplicate_tokens(canonical)
    if len(signature_tokens) > 1:
        signature_tokens.extend(f"{left}|{right}" for left, right in zip(signature_tokens, signature_tokens[1:]))
    signature_tokens = signature_tokens[:64]
    vector = [0] * 64
    for token in signature_tokens:
        bits = int.from_bytes(hashlib.sha256(token.encode("utf-8")).digest()[:8], "big")
        for index in range(64):
            vector[index] += 1 if bits & (1 << index) else -1
    fuzzy_value = 0
    for index, score in enumerate(vector):
        if score >= 0:
            fuzzy_value |= 1 << index
    fuzzy_signature = f"{fuzzy_value:016x}" if signature_tokens else None
    return fingerprint, similarity, fuzzy_signature


def build_duplicate_signals(
    privacy: ConfessionsCrypto,
    guild_id: int,
    text: str,
    attachment_meta: Sequence[dict[str, Any]],
    shared_link_url: str | None = None,
) -> DuplicateSignals:
    canonical = canonical_duplicate_text(text, attachment_meta, shared_link_url)
    legacy_exact_hash, legacy_similarity_key, legacy_fuzzy_signature = _legacy_duplicate_signals(canonical)
    if not canonical:
        return DuplicateSignals(
            exact_hash=None,
            fuzzy_signature=None,
            keyed_exact_candidates=(),
            keyed_fuzzy_candidates=(),
            legacy_exact_hash=legacy_exact_hash,
            legacy_similarity_key=legacy_similarity_key,
            legacy_fuzzy_signature=legacy_fuzzy_signature,
        )
    signature_tokens = duplicate_tokens(canonical)
    if len(signature_tokens) > 1:
        signature_tokens.extend(f"{left}|{right}" for left, right in zip(signature_tokens, signature_tokens[1:]))
    keyed_exact_values = list(privacy.exact_duplicate_hash_candidates(canonical, guild_id=guild_id))
    keyed_fuzzy_values = list(privacy.fuzzy_duplicate_signature_candidates(signature_tokens[:64], guild_id=guild_id))
    for legacy_value in keyed_exact_values[1:]:
        transformed = privacy.transform_legacy_exact_hash(legacy_value, guild_id=guild_id)
        if transformed is not None:
            keyed_exact_values.append(transformed)
    if legacy_exact_hash is not None:
        transformed = privacy.transform_legacy_exact_hash(legacy_exact_hash, guild_id=guild_id)
        if transformed is not None:
            keyed_exact_values.append(transformed)
    for legacy_value in keyed_fuzzy_values[1:]:
        transformed = privacy.transform_legacy_fuzzy_signature(legacy_value, guild_id=guild_id)
        if transformed is not None:
            keyed_fuzzy_values.append(transformed)
    if legacy_fuzzy_signature is not None:
        transformed = privacy.transform_legacy_fuzzy_signature(legacy_fuzzy_signature, guild_id=guild_id)
        if transformed is not None:
            keyed_fuzzy_values.append(transformed)
    keyed_exact_candidates = tuple(dict.fromkeys(keyed_exact_values))
    keyed_fuzzy_candidates = tuple(dict.fromkeys(keyed_fuzzy_values))
    return DuplicateSignals(
        exact_hash=keyed_exact_candidates[0] if keyed_exact_candidates else None,
        fuzzy_signature=keyed_fuzzy_candidates[0] if keyed_fuzzy_candidates else None,
        keyed_exact_candidates=keyed_exact_candidates,
        keyed_fuzzy_candidates=keyed_fuzzy_candidates,
        legacy_exact_hash=legacy_exact_hash,
        legacy_similarity_key=legacy_similarity_key,
        legacy_fuzzy_signature=legacy_fuzzy_signature,
    )


def fuzzy_signature_ratio(privacy: ConfessionsCrypto, left: str, right: str) -> float:
    left_value = privacy.strip_fuzzy_signature_prefix(left)
    right_value = privacy.strip_fuzzy_signature_prefix(right)
    try:
        left_bits = int(left_value, 16)
        right_bits = int(right_value, 16)
    except ValueError:
        return 0.0
    distance = bin(left_bits ^ right_bits).count("1")
    return 1.0 - (distance / 64.0)


def legacy_similarity_ratio(left: str, right: str) -> float:
    return SequenceMatcher(None, left, right).ratio()
