from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import aiohttp

from babblebox.premium_models import SYSTEM_PREMIUM_SUPPORT_GUILD_ID
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


LOGGER = logging.getLogger(__name__)


SHIELD_AI_SUPPORT_GUILD_ID = SYSTEM_PREMIUM_SUPPORT_GUILD_ID
SHIELD_AI_ALLOWED_GUILD_ID = SHIELD_AI_SUPPORT_GUILD_ID
SHIELD_AI_REVIEW_PACKS = ("privacy", "promo", "scam", "adult", "severe")
SHIELD_AI_MIN_CONFIDENCE_CHOICES = ("low", "medium", "high")
SHIELD_AI_ROUTING_TIERS = ("fast", "complex", "frontier")
SHIELD_AI_MODEL_ORDER = ("gpt-5-nano", "gpt-5-mini", "gpt-5")
SHIELD_AI_MODEL_ALIASES = {
    "nano": "gpt-5-nano",
    "mini": "gpt-5-mini",
    "full": "gpt-5",
    "gpt-5-nano": "gpt-5-nano",
    "gpt-5-mini": "gpt-5-mini",
    "gpt-5": "gpt-5",
}
SHIELD_AI_MODEL_SHORT_NAMES = {
    "gpt-5-nano": "nano",
    "gpt-5-mini": "mini",
    "gpt-5": "full",
}
DEFAULT_SHIELD_AI_FAST_MODEL = "gpt-5-nano"
DEFAULT_SHIELD_AI_COMPLEX_MODEL = "gpt-5-mini"
DEFAULT_SHIELD_AI_TOP_MODEL = "gpt-5"
DEFAULT_SHIELD_AI_MODEL = DEFAULT_SHIELD_AI_FAST_MODEL
DEFAULT_SHIELD_AI_ENABLE_TOP_TIER = False
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
    return int(guild_id or 0) == SHIELD_AI_SUPPORT_GUILD_ID


def normalize_shield_ai_model_name(value: str | None) -> str | None:
    cleaned = normalize_plain_text(value).casefold()
    if not cleaned:
        return None
    return SHIELD_AI_MODEL_ALIASES.get(cleaned)


def parse_shield_ai_model_list(values: Iterable[str] | str | None) -> tuple[str, ...]:
    if values is None:
        return ()
    raw_items: Iterable[str]
    if isinstance(values, str):
        raw_items = values.split(",")
    else:
        raw_items = values
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        normalized = normalize_shield_ai_model_name(item)
        if normalized is None:
            raise ValueError("Allowed Shield AI models must be `nano`, `mini`, `full`, or canonical model names.")
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    cleaned.sort(key=_model_rank)
    return tuple(cleaned)


def format_shield_ai_model(model: str | None) -> str:
    normalized = normalize_shield_ai_model_name(model)
    if normalized is None:
        return str(model or "").strip() or "unknown"
    return f"{SHIELD_AI_MODEL_SHORT_NAMES[normalized]} ({normalized})"


def format_shield_ai_model_list(models: Iterable[str] | None) -> str:
    items = []
    for model in models or ():
        normalized = normalize_shield_ai_model_name(model)
        if normalized is None:
            continue
        items.append(format_shield_ai_model(normalized))
    return ", ".join(items) if items else "None"


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


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().casefold()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _read_model_env(name: str, default: str) -> tuple[str, str, bool]:
    raw = os.getenv(name, "").strip()
    normalized = normalize_shield_ai_model_name(raw)
    invalid = bool(raw and normalized is None)
    return (normalized or default, raw, invalid)


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


def _model_rank(model: str) -> int:
    normalized = normalize_shield_ai_model_name(model)
    if normalized is None:
        return len(SHIELD_AI_MODEL_ORDER)
    return SHIELD_AI_MODEL_ORDER.index(normalized)


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
    sanitized_redaction_count: int = 0
    sanitized_truncated: bool = False
    has_links: bool = False
    domains: tuple[str, ...] = ()
    has_suspicious_attachment: bool = False
    attachment_extensions: tuple[str, ...] = ()
    invite_detected: bool = False
    repetitive_promo: bool = False
    allowed_models: tuple[str, ...] = SHIELD_AI_MODEL_ORDER


@dataclass(frozen=True)
class ShieldAIReviewResult:
    classification: str
    confidence: str
    priority: str
    false_positive: bool
    explanation: str
    model: str
    provider: str = OPENAI_PROVIDER_NAME
    tier: str = "fast"
    target_tier: str = "fast"
    route_reasons: tuple[str, ...] = ()
    attempted_models: tuple[str, ...] = ()
    fallback_used: bool = False
    policy_capped: bool = False

    @property
    def classification_label(self) -> str:
        return AI_CLASSIFICATION_LABELS.get(self.classification, self.classification.replace("_", " ").title())


@dataclass(frozen=True)
class ShieldAIRoutePlan:
    target_tier: str
    selected_tier: str
    selected_model: str
    route_reasons: tuple[str, ...]
    attempted_models: tuple[str, ...]
    policy_capped: bool
    single_model_override: bool = False


class _RetryableProviderFailure(RuntimeError):
    pass


class _NonRetryableProviderFailure(RuntimeError):
    pass


class _TimeoutProviderFailure(RuntimeError):
    pass


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
            "routing_strategy": "disabled",
            "single_model_override": False,
            "provider_readiness": "AI review is unavailable because no provider is configured.",
            "model_override_state": "blank",
            "model_override_note": "No single-model override configured.",
            "routed_default_model": None,
            "invalid_model_settings_note": None,
            "ignored_model_settings": [],
            "fast_model": None,
            "complex_model": None,
            "top_model": None,
            "top_tier_enabled": False,
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
        self.single_model_override_raw = os.getenv("SHIELD_AI_MODEL", "").strip()
        self.single_model_override = normalize_shield_ai_model_name(self.single_model_override_raw)
        self.fast_model, _fast_model_raw, invalid_fast_model = _read_model_env("SHIELD_AI_FAST_MODEL", DEFAULT_SHIELD_AI_FAST_MODEL)
        self.complex_model, _complex_model_raw, invalid_complex_model = _read_model_env("SHIELD_AI_COMPLEX_MODEL", DEFAULT_SHIELD_AI_COMPLEX_MODEL)
        self.top_model, _top_model_raw, invalid_top_model = _read_model_env("SHIELD_AI_TOP_MODEL", DEFAULT_SHIELD_AI_TOP_MODEL)
        self.top_tier_enabled = _read_bool_env("SHIELD_AI_ENABLE_TOP_TIER", DEFAULT_SHIELD_AI_ENABLE_TOP_TIER)
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
        self._last_review_failure: dict[str, Any] | None = None
        self.ignored_model_settings = tuple(
            name
            for name, invalid in (
                ("SHIELD_AI_MODEL", bool(self.single_model_override_raw and self.single_model_override is None)),
                ("SHIELD_AI_FAST_MODEL", invalid_fast_model),
                ("SHIELD_AI_COMPLEX_MODEL", invalid_complex_model),
                ("SHIELD_AI_TOP_MODEL", invalid_top_model),
            )
            if invalid
        )
        invalid_settings_note = ""
        if self.ignored_model_settings:
            invalid_settings_note = f", ignored_invalid_model_settings={','.join(self.ignored_model_settings)}"
        LOGGER.info(
            "Shield AI init: provider=%s configured=%s fast_model=%s complex_model=%s top_model=%s top_tier_enabled=%s single_model_override=%s%s timeout_seconds=%s max_chars=%s support_guild_id=%s",
            OPENAI_PROVIDER_NAME.lower(),
            "yes" if self.api_key else "no",
            self.fast_model,
            self.complex_model,
            self.top_model,
            "yes" if self.top_tier_enabled else "no",
            self.single_model_override or "none",
            invalid_settings_note,
            self.timeout_seconds,
            self.max_chars,
            SHIELD_AI_SUPPORT_GUILD_ID,
        )

    def diagnostics(self) -> dict[str, Any]:
        available = bool(self.api_key)
        status = "Ready." if available else "OpenAI API key is not configured."
        routing_strategy = "single_model_override" if self.single_model_override else ("routed_fast_complex_frontier" if self.top_tier_enabled else "routed_fast_complex")
        if self.single_model_override:
            model_override_state = "valid"
            model_override_note = f"Single-model override active: {format_shield_ai_model(self.single_model_override)}."
        elif self.single_model_override_raw:
            model_override_state = "invalid"
            model_override_note = "Invalid override ignored: SHIELD_AI_MODEL. Shield is using routed defaults instead."
        else:
            model_override_state = "blank"
            model_override_note = "No single-model override configured. Shield is using routed defaults."
        tier_invalid_settings = [name for name in self.ignored_model_settings if name != "SHIELD_AI_MODEL"]
        invalid_model_settings_note = (
            f"Invalid tier model settings ignored: {', '.join(tier_invalid_settings)}. Shield is using safe defaults for those tiers."
            if tier_invalid_settings
            else None
        )
        return {
            "provider": OPENAI_PROVIDER_NAME,
            "available": available,
            "configured": available,
            "model": self.single_model_override or self.fast_model,
            "routing_strategy": routing_strategy,
            "single_model_override": bool(self.single_model_override),
            "ignored_model_settings": list(self.ignored_model_settings),
            "provider_readiness": status,
            "model_override_state": model_override_state,
            "model_override_note": model_override_note,
            "routed_default_model": self.fast_model,
            "invalid_model_settings_note": invalid_model_settings_note,
            "fast_model": self.fast_model,
            "complex_model": self.complex_model,
            "top_model": self.top_model,
            "top_tier_enabled": self.top_tier_enabled,
            "timeout_seconds": self.timeout_seconds,
            "max_chars": self.max_chars,
            "last_review_failure": dict(self._last_review_failure) if self._last_review_failure else None,
            "status": status,
        }

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def review(self, request: ShieldAIReviewRequest) -> ShieldAIReviewResult | None:
        self._last_review_failure = None
        if not self.api_key:
            self._remember_review_failure("provider_unavailable", "OpenAI API key is not configured.")
            return None
        if not request.sanitized_content and not request.domains and not request.attachment_extensions:
            self._remember_review_failure("empty_review_request", "The review request had no sanitized text or compact metadata.")
            return None

        route = self._route_request(request)
        acquired = False
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.05)
            acquired = True
        except asyncio.TimeoutError:
            LOGGER.info("Shield AI review skipped: reviewer queue is busy.")
            self._remember_review_failure("queue_busy", "Reviewer concurrency limit was busy.")
            return None

        try:
            payload = None
            final_model = route.selected_model
            for attempt_index, model in enumerate(route.attempted_models):
                final_model = model
                try:
                    payload = await self._request_completion(request, model=model)
                    break
                except _RetryableProviderFailure:
                    if attempt_index + 1 >= len(route.attempted_models):
                        LOGGER.info("Shield AI review skipped: retryable provider failure with no fallback remaining.")
                        self._remember_review_failure("retryable_provider_failure", "Provider returned a retryable error and no fallback model remained.")
                        return None
                    continue
                except _TimeoutProviderFailure:
                    LOGGER.info("Shield AI review skipped: provider timeout.")
                    self._remember_review_failure("provider_timeout", "Provider request timed out.")
                    return None
                except _NonRetryableProviderFailure as exc:
                    reason = str(exc).strip() or "provider_request_error"
                    self._remember_review_failure(reason, "Provider rejected the review request.")
                    return None
            if payload is None:
                self._remember_review_failure("provider_empty_response", "Provider returned no payload.")
                return None
        finally:
            if acquired:
                self._semaphore.release()

        try:
            return self._parse_response(
                payload,
                model=final_model,
                tier=_tier_for_model(final_model),
                target_tier=route.target_tier,
                route_reasons=route.route_reasons,
                attempted_models=route.attempted_models,
                fallback_used=len(route.attempted_models) > 1,
                policy_capped=route.policy_capped,
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.info(
                "Shield AI review skipped: malformed provider output (%s).",
                type(exc).__name__,
            )
            self._remember_review_failure("provider_malformed_output", f"Provider returned malformed output ({type(exc).__name__}).")
            return None

    def _remember_review_failure(self, reason: str, detail: str | None = None):
        cleaned_reason = normalize_plain_text(reason).casefold().replace(" ", "_") or "provider_no_review"
        payload: dict[str, Any] = {"reason": cleaned_reason}
        cleaned_detail = normalize_plain_text(detail)
        if cleaned_detail:
            payload["detail"] = cleaned_detail[:220]
        self._last_review_failure = payload

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            connector = aiohttp.TCPConnector(limit=DEFAULT_SHIELD_AI_CONCURRENCY + 1, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def _request_completion(self, request: ShieldAIReviewRequest, *, model: str) -> dict[str, Any]:
        session = await self._get_session()
        try:
            async with session.post(
                OPENAI_CHAT_COMPLETIONS_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json=self._build_payload(request, model=model),
            ) as response:
                if response.status == 429:
                    LOGGER.info("Shield AI review retryable: provider rate limit.")
                    raise _RetryableProviderFailure("rate_limit")
                if response.status >= 500:
                    LOGGER.info("Shield AI review retryable: provider server error (%s).", response.status)
                    raise _RetryableProviderFailure("server_error")
                if response.status >= 400:
                    LOGGER.info("Shield AI review skipped: provider request error (%s).", response.status)
                    raise _NonRetryableProviderFailure(f"provider_request_error_{response.status}")
                try:
                    return await response.json(content_type=None)
                except json.JSONDecodeError as exc:
                    LOGGER.info("Shield AI review skipped: provider returned non-JSON output.")
                    raise _NonRetryableProviderFailure("malformed_json") from exc
        except asyncio.TimeoutError as exc:
            raise _TimeoutProviderFailure("timeout") from exc
        except aiohttp.ClientError as exc:
            LOGGER.info(
                "Shield AI review retryable: transport failure (%s).",
                type(exc).__name__,
            )
            raise _RetryableProviderFailure("client_error") from exc

    def _route_request(self, request: ShieldAIReviewRequest) -> ShieldAIRoutePlan:
        if self.single_model_override:
            return ShieldAIRoutePlan(
                target_tier=_tier_for_model(self.single_model_override),
                selected_tier=_tier_for_model(self.single_model_override),
                selected_model=self.single_model_override,
                route_reasons=("single_model_override",),
                attempted_models=(self.single_model_override,),
                policy_capped=False,
                single_model_override=True,
            )

        allowed_models = tuple(
            model
            for model in (
                normalize_shield_ai_model_name(item)
                for item in (request.allowed_models or SHIELD_AI_MODEL_ORDER)
            )
            if model is not None
        )
        if not allowed_models:
            allowed_models = (self.fast_model,)
        route_reasons = _route_reasons_for_request(request)
        target_tier = _target_tier_for_request(request, route_reasons, top_tier_enabled=self.top_tier_enabled)
        selected_model, selected_tier, policy_capped = self._select_model_for_tier(target_tier, allowed_models)
        attempted_models = [selected_model]
        fallback_model = self._fallback_model(selected_model, allowed_models)
        if fallback_model is not None:
            attempted_models.append(fallback_model)
        return ShieldAIRoutePlan(
            target_tier=target_tier,
            selected_tier=selected_tier,
            selected_model=selected_model,
            route_reasons=tuple(route_reasons),
            attempted_models=tuple(attempted_models),
            policy_capped=policy_capped,
        )

    def _select_model_for_tier(self, target_tier: str, allowed_models: Sequence[str]) -> tuple[str, str, bool]:
        tier_models = {
            "fast": self.fast_model,
            "complex": self.complex_model,
            "frontier": self.top_model,
        }
        target_rank = SHIELD_AI_ROUTING_TIERS.index(target_tier)
        allowed = tuple(sorted({normalize_shield_ai_model_name(item) for item in allowed_models if normalize_shield_ai_model_name(item)}, key=_model_rank))
        satisfying = [model for model in allowed if _tier_rank(_tier_for_model(model)) >= target_rank]
        if satisfying:
            selected = satisfying[0]
            return selected, _tier_for_model(selected), False
        selected = allowed[-1]
        return selected, _tier_for_model(selected), True

    def _fallback_model(self, current_model: str, allowed_models: Sequence[str]) -> str | None:
        current_rank = _model_rank(current_model)
        lower_models = [model for model in allowed_models if _model_rank(model) < current_rank]
        if not lower_models:
            return None
        return max(lower_models, key=_model_rank)

    def _build_payload(self, request: ShieldAIReviewRequest, *, model: str) -> dict[str, Any]:
        return {
            "model": model,
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
                "sanitized_redaction_count": request.sanitized_redaction_count,
                "sanitized_truncated": request.sanitized_truncated,
            },
            "sanitized_excerpt": request.sanitized_content or "[no text excerpt]",
        }
        return json.dumps(body, ensure_ascii=True)

    def _parse_response(
        self,
        payload: dict[str, Any],
        *,
        model: str,
        tier: str,
        target_tier: str,
        route_reasons: Sequence[str],
        attempted_models: Sequence[str],
        fallback_used: bool,
        policy_capped: bool,
    ) -> ShieldAIReviewResult:
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
            model=model,
            tier=tier,
            target_tier=target_tier,
            route_reasons=tuple(route_reasons),
            attempted_models=tuple(attempted_models),
            fallback_used=fallback_used,
            policy_capped=policy_capped,
        )


def _tier_for_model(model: str) -> str:
    normalized = normalize_shield_ai_model_name(model)
    if normalized == DEFAULT_SHIELD_AI_TOP_MODEL:
        return "frontier"
    if normalized == DEFAULT_SHIELD_AI_COMPLEX_MODEL:
        return "complex"
    return "fast"


def _tier_rank(tier: str) -> int:
    try:
        return SHIELD_AI_ROUTING_TIERS.index(tier)
    except ValueError:
        return 0


def _route_reasons_for_request(request: ShieldAIReviewRequest) -> list[str]:
    reasons: list[str] = []
    if request.pack in {"scam", "severe"}:
        reasons.append("high_risk_pack")
    if request.local_confidence != "high":
        reasons.append("local_confidence_below_high")
    if request.local_action in {"timeout_log", "delete_escalate"}:
        reasons.append("high_severity_action")
    if len(request.local_labels) >= 2:
        reasons.append("multiple_local_labels")
    if request.has_suspicious_attachment:
        reasons.append("suspicious_attachment")
    if request.sanitized_truncated:
        reasons.append("sanitized_excerpt_truncated")
    if request.sanitized_redaction_count >= 3:
        reasons.append("heavy_redaction")
    compound_link_context = request.invite_detected or request.repetitive_promo or len(request.domains) >= 2
    if compound_link_context:
        reasons.append("compound_link_context")
    return reasons


def _target_tier_for_request(request: ShieldAIReviewRequest, route_reasons: Sequence[str], *, top_tier_enabled: bool) -> str:
    if not route_reasons:
        return "fast"
    target = "complex"
    if not top_tier_enabled:
        return target
    has_frontier_trigger = "high_risk_pack" in route_reasons or "high_severity_action" in route_reasons
    if has_frontier_trigger and len(route_reasons) >= 3:
        return "frontier"
    return target


def build_shield_ai_provider() -> ShieldAIProvider:
    return OpenAIShieldAIProvider()
