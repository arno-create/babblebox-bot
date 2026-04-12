from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Sequence

import aiohttp

from babblebox.text_safety import (
    CARD_RE,
    EMAIL_RE,
    IPV4_RE,
    IPV6_RE,
    MARKDOWN_LINK_RE,
    MENTION_RE,
    PHONE_RE,
    SSN_RE,
    URL_RE,
    normalize_plain_text,
)


SHIELD_AI_ALLOWED_GUILD_ID = 1322933864360050688
SHIELD_AI_REVIEW_PACKS = ("privacy", "promo", "scam", "adult", "severe")
SHIELD_AI_MIN_CONFIDENCE_CHOICES = ("low", "medium", "high")
DEFAULT_SHIELD_AI_MODEL = "gpt-4.1-mini"
DEFAULT_SHIELD_AI_TIMEOUT_SECONDS = 4.0
DEFAULT_SHIELD_AI_MAX_CHARS = 340
DEFAULT_SHIELD_AI_CONCURRENCY = 2
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
OPENAI_PROVIDER_NAME = "OpenAI"

ETH_WALLET_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
BTC_WALLET_RE = re.compile(r"\b(?:bc1|[13])[a-zA-HJ-NP-Z0-9]{25,59}\b")
ATTACHMENT_EXTENSION_RE = re.compile(r"\.([a-z0-9]{1,10})(?:$|[?#])", re.IGNORECASE)

AI_CLASSIFICATION_LABELS = {
    "privacy_leak": "Likely privacy leak",
    "ad_invite_promo": "Likely ad / invite / promo",
    "scam_social_engineering": "Likely scam / social engineering",
    "adult_solicitation_or_adult_risk": "Likely adult solicitation / adult risk",
    "severe_harm_or_hate": "Likely severe harm / hate",
    "false_positive": "Possible false positive",
    "uncertain": "Uncertain",
}
AI_ALLOWED_CLASSIFICATIONS = frozenset(AI_CLASSIFICATION_LABELS)
AI_ALLOWED_CONFIDENCE = frozenset({"low", "medium", "high"})
AI_ALLOWED_PRIORITIES = frozenset({"low", "normal", "high"})

_REDACTION_RULES: tuple[tuple[re.Pattern[str], str], ...] = (
    (EMAIL_RE, "[EMAIL]"),
    (MARKDOWN_LINK_RE, "[LINK]"),
    (URL_RE, "[LINK]"),
    (ETH_WALLET_RE, "[WALLET]"),
    (BTC_WALLET_RE, "[WALLET]"),
    (CARD_RE, "[CARD]"),
    (SSN_RE, "[SENSITIVE_ID]"),
    (PHONE_RE, "[PHONE]"),
    (IPV4_RE, "[IP]"),
    (IPV6_RE, "[IP]"),
    (MENTION_RE, "[MENTION]"),
)

_SYSTEM_PROMPT = (
    "You are Babblebox Shield AI Assist. "
    "You are a second-pass moderator helper for already-flagged Discord messages. "
    "You do not decide punishments and you must not recommend automatic enforcement. "
    "Work only from the provided sanitized excerpt and compact metadata. "
    "If the excerpt is too redacted or too limited, return uncertain. "
    "Return only strict JSON with these keys: "
    "classification, confidence, priority, false_positive, explanation. "
    "classification must be one of: privacy_leak, ad_invite_promo, scam_social_engineering, adult_solicitation_or_adult_risk, severe_harm_or_hate, false_positive, uncertain. "
    "confidence must be one of: low, medium, high. "
    "priority must be one of: low, normal, high. "
    "false_positive must be true or false. "
    "explanation must be a short moderator-facing sentence under 180 characters."
)


def shield_ai_available_in_guild(guild_id: int | None) -> bool:
    return int(guild_id or 0) == SHIELD_AI_ALLOWED_GUILD_ID


def _read_float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return min(maximum, max(minimum, value))


def _read_int_env(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return min(maximum, max(minimum, value))


def _truncate_text(text: str, limit: int) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    if limit <= 3:
        return text[:limit], True
    window = text[: limit - 3].rstrip()
    if " " in window:
        window = window.rsplit(" ", 1)[0].rstrip()
    if not window:
        window = text[: limit - 3]
    return f"{window}...", True


def summarize_attachment_extensions(filenames: Sequence[str]) -> tuple[str, ...]:
    extensions: list[str] = []
    seen: set[str] = set()
    for filename in filenames:
        match = ATTACHMENT_EXTENSION_RE.search(filename or "")
        if not match:
            continue
        extension = match.group(1).casefold()
        if extension in seen:
            continue
        seen.add(extension)
        extensions.append(extension)
        if len(extensions) >= 4:
            break
    return tuple(extensions)


@dataclass(frozen=True)
class SanitizedShieldAIContent:
    text: str
    redaction_count: int
    truncated: bool


@dataclass(frozen=True)
class ShieldAIReviewRequest:
    guild_id: int
    pack: str
    local_confidence: str
    local_action: str
    local_labels: tuple[str, ...]
    local_reasons: tuple[str, ...]
    sanitized_content: str
    has_links: bool
    domains: tuple[str, ...]
    has_suspicious_attachment: bool
    attachment_extensions: tuple[str, ...]
    invite_detected: bool
    repetitive_promo: bool


@dataclass(frozen=True)
class ShieldAIReviewResult:
    classification: str
    confidence: str
    priority: str
    false_positive: bool
    explanation: str
    model: str
    provider: str = OPENAI_PROVIDER_NAME

    @property
    def classification_label(self) -> str:
        return AI_CLASSIFICATION_LABELS.get(self.classification, self.classification.replace("_", " ").title())


def sanitize_message_for_ai(text: str | None, *, max_chars: int = DEFAULT_SHIELD_AI_MAX_CHARS) -> SanitizedShieldAIContent:
    cleaned = normalize_plain_text(text)
    redaction_count = 0
    for pattern, replacement in _REDACTION_RULES:
        cleaned, count = pattern.subn(replacement, cleaned)
        redaction_count += count
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned, truncated = _truncate_text(cleaned, max_chars)
    return SanitizedShieldAIContent(text=cleaned, redaction_count=redaction_count, truncated=truncated)


class ShieldAIProvider:
    provider_name = "disabled"

    def diagnostics(self) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "available": False,
            "configured": False,
            "model": None,
            "timeout_seconds": None,
            "max_chars": None,
            "status": "AI review is unavailable because no provider is configured.",
        }

    async def review(self, request: ShieldAIReviewRequest) -> ShieldAIReviewResult | None:
        return None

    async def close(self):
        return None


class OpenAIShieldAIProvider(ShieldAIProvider):
    provider_name = "openai"

    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("SHIELD_AI_MODEL", "").strip() or DEFAULT_SHIELD_AI_MODEL
        self.timeout_seconds = _read_float_env(
            "SHIELD_AI_TIMEOUT_SECONDS",
            DEFAULT_SHIELD_AI_TIMEOUT_SECONDS,
            minimum=1.0,
            maximum=15.0,
        )
        self.max_chars = _read_int_env(
            "SHIELD_AI_MAX_CHARS",
            DEFAULT_SHIELD_AI_MAX_CHARS,
            minimum=80,
            maximum=600,
        )
        self._session: aiohttp.ClientSession | None = None
        self._semaphore = asyncio.Semaphore(DEFAULT_SHIELD_AI_CONCURRENCY)
        print(
            "Shield AI init: "
            f"provider={OPENAI_PROVIDER_NAME.lower()}, "
            f"configured={'yes' if self.api_key else 'no'}, "
            f"model={self.model}, "
            f"timeout_seconds={self.timeout_seconds}, "
            f"max_chars={self.max_chars}, "
            f"allowed_guild_id={SHIELD_AI_ALLOWED_GUILD_ID}"
        )

    def diagnostics(self) -> dict[str, Any]:
        available = bool(self.api_key)
        return {
            "provider": OPENAI_PROVIDER_NAME,
            "available": available,
            "configured": available,
            "model": self.model,
            "timeout_seconds": self.timeout_seconds,
            "max_chars": self.max_chars,
            "status": "Ready." if available else "OpenAI API key is not configured.",
        }

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def review(self, request: ShieldAIReviewRequest) -> ShieldAIReviewResult | None:
        if not self.api_key:
            return None
        if not request.sanitized_content and not request.domains and not request.attachment_extensions:
            return None

        acquired = False
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.05)
            acquired = True
        except asyncio.TimeoutError:
            print("Shield AI review skipped: reviewer queue is busy.")
            return None

        try:
            session = await self._get_session()
            async with session.post(
                OPENAI_CHAT_COMPLETIONS_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=self._build_payload(request),
            ) as response:
                if response.status == 429:
                    print("Shield AI review skipped: provider rate limit.")
                    return None
                if response.status >= 500:
                    print(f"Shield AI review skipped: provider server error ({response.status}).")
                    return None
                if response.status >= 400:
                    print(f"Shield AI review skipped: provider request error ({response.status}).")
                    return None
                payload = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
            print(f"Shield AI review skipped: {type(exc).__name__}.")
            return None
        finally:
            if acquired:
                self._semaphore.release()

        try:
            return self._parse_response(payload)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            print(f"Shield AI review skipped: malformed provider output ({type(exc).__name__}).")
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            connector = aiohttp.TCPConnector(limit=DEFAULT_SHIELD_AI_CONCURRENCY + 1, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    def _build_payload(self, request: ShieldAIReviewRequest) -> dict[str, Any]:
        return {
            "model": self.model,
            "temperature": 0,
            "max_tokens": 180,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": self._build_user_prompt(request)},
            ],
        }

    def _build_user_prompt(self, request: ShieldAIReviewRequest) -> str:
        body = {
            "local_pack": request.pack,
            "local_confidence": request.local_confidence,
            "local_action": request.local_action,
            "local_labels": list(request.local_labels[:3]),
            "local_reasons": list(request.local_reasons[:2]),
            "metadata": {
                "has_links": request.has_links,
                "domains": list(request.domains[:3]),
                "invite_detected": request.invite_detected,
                "has_suspicious_attachment": request.has_suspicious_attachment,
                "attachment_extensions": list(request.attachment_extensions[:3]),
                "repetitive_promo": request.repetitive_promo,
            },
            "sanitized_excerpt": request.sanitized_content or "[no text excerpt]",
        }
        return json.dumps(body, ensure_ascii=True)

    def _parse_response(self, payload: dict[str, Any]) -> ShieldAIReviewResult:
        choices = payload.get("choices")
        if not isinstance(choices, list) or not choices:
            raise ValueError("Missing choices.")
        content = choices[0].get("message", {}).get("content")
        if isinstance(content, list):
            text_parts = [
                str(part.get("text", "")).strip()
                for part in content
                if isinstance(part, dict) and str(part.get("type", "")).strip() in {"text", "output_text"}
            ]
            content = "\n".join(part for part in text_parts if part)
        if not isinstance(content, str) or not content.strip():
            raise ValueError("Missing content.")
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("AI output was not an object.")
        classification = str(parsed.get("classification", "")).strip().lower()
        confidence = str(parsed.get("confidence", "")).strip().lower()
        priority = str(parsed.get("priority", "")).strip().lower()
        false_positive = bool(parsed.get("false_positive", False))
        explanation = normalize_plain_text(str(parsed.get("explanation", "")))
        if classification not in AI_ALLOWED_CLASSIFICATIONS:
            raise ValueError("Invalid classification.")
        if confidence not in AI_ALLOWED_CONFIDENCE:
            raise ValueError("Invalid confidence.")
        if priority not in AI_ALLOWED_PRIORITIES:
            raise ValueError("Invalid priority.")
        explanation, _ = _truncate_text(explanation or "No additional moderator note.", 180)
        return ShieldAIReviewResult(
            classification=classification,
            confidence=confidence,
            priority=priority,
            false_positive=false_positive,
            explanation=explanation,
            model=self.model,
        )


def build_shield_ai_provider() -> ShieldAIProvider:
    return OpenAIShieldAIProvider()
