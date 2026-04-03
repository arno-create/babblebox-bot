from __future__ import annotations

import asyncio
import contextlib
import hashlib
import ipaddress
import re
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.shield_ai import (
    SHIELD_AI_MIN_CONFIDENCE_CHOICES,
    SHIELD_AI_REVIEW_PACKS,
    ShieldAIReviewRequest,
    ShieldAIReviewResult,
    build_shield_ai_provider,
    sanitize_message_for_ai,
    shield_ai_available_in_guild,
    summarize_attachment_extensions,
)
from babblebox.shield_link_safety import (
    ADULT_LINK_CATEGORY,
    MALICIOUS_LINK_CATEGORY,
    MEDIA_EMBED_DOMAINS,
    SHORTENER_DOMAINS,
    SOCIAL_PROMO_DOMAINS,
    STOREFRONT_DOMAINS,
    UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
    ShieldLinkAssessment,
    ShieldLinkSafetyEngine,
    domain_in_set as link_domain_in_set,
    domain_matches as link_domain_matches,
    merge_link_assessments,
)
from babblebox.shield_store import (
    LOW_CONFIDENCE_ACTIONS,
    MEDIUM_CONFIDENCE_ACTIONS,
    ShieldStateStore,
    ShieldStorageUnavailable,
    default_guild_shield_config,
    normalize_guild_shield_config,
)
from babblebox.text_safety import (
    CARD_RE,
    EMAIL_RE,
    IPV4_RE,
    IPV6_RE,
    PHONE_RE,
    SSN_RE,
    URL_RE,
    normalize_plain_text,
    sanitize_short_plain_text,
    squash_for_evasion_checks,
)
from babblebox.utility_helpers import make_attachment_labels, make_message_preview


RULE_PACKS = ("privacy", "promo", "scam", "adult")
SHIELD_ACTIONS = {"disabled", "detect", "log", "delete_log", "delete_escalate", "timeout_log"}
SHIELD_SENSITIVITIES = {"low", "normal", "high"}
CUSTOM_PATTERN_MODES = {"contains", "word", "wildcard"}

FILTER_LIMIT = 20
ALLOWLIST_LIMIT = 20
ALLOW_PHRASE_MAX_LEN = 60
CUSTOM_PATTERN_LIMIT = 10
CUSTOM_PATTERN_LABEL_MAX_LEN = 32
CUSTOM_PATTERN_MAX_LEN = 80
CUSTOM_PATTERN_WILDCARD_LIMIT = 4
MAX_MESSAGE_PREVIEW = 220
ALERT_DEDUP_SECONDS = 30.0
REPETITION_WINDOW_SECONDS = 10 * 60.0
DIRECT_PROMO_REPEAT_THRESHOLD = 3
GENERIC_LINK_NOISE_THRESHOLD = 4
MEDIA_LINK_NOISE_THRESHOLD = 5
RUNTIME_PRUNE_INTERVAL_SECONDS = 60.0

PACK_LABELS = {
    "privacy": "Privacy Leak",
    "promo": "Promo / Invite",
    "scam": "Scam / Malicious Links",
    "adult": "Adult / 18+ Links",
    "advanced": "Advanced Pattern",
}
MATCH_CLASS_LABELS = {
    "discord_invite": "Discord invite",
    "self_promo": "Self-promo link",
    "monetized_promo": "Monetized promo",
    "cta_promo": "Call-to-action promo",
    "repetitive_link_noise": "Repetitive link noise",
    "known_malicious_domain": "Known malicious domain",
    "adult_domain": "Known adult domain",
}
ESCALATION_BLOCKED_MATCH_CLASSES = {"repetitive_link_noise"}
ACTION_LABELS = {
    "disabled": "Disabled",
    "detect": "Detect only",
    "log": "Log only",
    "delete_log": "Delete + log",
    "delete_escalate": "Delete + log + repeated-hit escalation",
    "timeout_log": "Timeout + log",
}
SENSITIVITY_LABELS = {"low": "Low", "normal": "Normal", "high": "High"}
ACTION_STRENGTH = {"disabled": -1, "detect": 0, "log": 1, "delete_log": 2, "timeout_log": 3, "delete_escalate": 4}
CONFIDENCE_STRENGTH = {"low": 1, "medium": 2, "high": 3, "custom": 4}
PACK_STRENGTH = {"privacy": 1, "promo": 2, "scam": 3, "adult": 3, "advanced": 4}
AI_PRIORITY_LABELS = {"low": "Low", "normal": "Normal", "high": "High"}
AI_REVIEW_PACK_SET = frozenset(SHIELD_AI_REVIEW_PACKS)

INVITE_RE = re.compile(r"(?i)(?:https?://)?(?:www\.)?(?:discord(?:app)?\.com/invite|discord\.gg)/([a-z0-9-]{2,32})")
ETH_WALLET_RE = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
BTC_WALLET_RE = re.compile(r"\b(?:bc1|[13])[a-zA-HJ-NP-Z0-9]{25,59}\b")
IP_CONTEXT_RE = re.compile(r"(?i)\b(?:ip|address|server|host|router|login)\b")
EMAIL_CONTEXT_RE = re.compile(r"(?i)\b(?:email|e-mail|mail|contact|reach me|write to|send(?: me)?(?: an)? email)\b")
PHONE_CONTEXT_RE = re.compile(r"(?i)\b(?:call|text|phone|contact|whatsapp|telegram|reach me)\b")
PAYMENT_CONTEXT_RE = re.compile(
    r"(?i)\b(?:card|credit|debit|cvv|cvc|expiry|routing|bank|account|iban|payment|paypal|cashapp|venmo|zelle|wire)\b"
)
OTP_CONTEXT_RE = re.compile(
    r"(?i)\b(?:otp|2fa|verification code|auth(?:entication)? code|login code|security code|one[- ]time code|sms code)\b"
)
ROUTING_CONTEXT_RE = re.compile(r"(?i)\b(?:routing|aba)\b")
ACCOUNT_ID_CONTEXT_RE = re.compile(r"(?i)\b(?:account(?: number)?|passport|tax id|taxpayer|member id|customer id)\b")
CRYPTO_CONTEXT_RE = re.compile(r"(?i)\b(?:wallet|address|seed phrase|crypto|bitcoin|btc|ethereum|eth|usdt)\b")
PROMO_CTA_RE = re.compile(r"(?i)\b(?:join|check out|follow|subscribe|support|buy|shop|hire|order|commission(?:s)? open)\b")
INVITE_CTA_RE = re.compile(r"(?i)\b(?:join|check out|new|growing|active|friendly)\b.{0,24}\b(?:server|community)\b")
PROMO_CONTEXT_RE = re.compile(r"(?i)\b(?:server|community|channel|stream|shop|store|commission(?:s)?|prices|portfolio|page)\b")
MONETIZED_PROMO_RE = re.compile(
    r"(?i)\b(?:commission(?:s)? open|patreon|ko-fi|gumroad|etsy|shop|store|prices|paid promo|sponsored)\b"
)
SOCIAL_ENGINEERING_RE = re.compile(r"(?i)\b(?:download|run|install|open|verify|claim|login|log in|connect wallet|sync)\b")
SCAM_BAIT_RE = re.compile(
    r"(?i)\b(?:free nitro|nitro gift|steam gift|claim reward|claim now|verify your account|wallet connect|seed phrase|airdrop|gift inventory|limited time claim)\b"
)
BRAND_BAIT_RE = re.compile(r"(?i)\b(?:discord|nitro|steam|epic|wallet|crypto|gift)\b")
SUSPICIOUS_FILE_RE = re.compile(r"(?i)\.(?:exe|scr|bat|cmd|msi|zip|rar|7z|iso)(?:$|[?#])")
SCAM_WARNING_RE = re.compile(
    r"(?i)(?:\b(?:beware|warning|avoid|do not|don't|never|fake|malicious|phish(?:ing)?|report(?:ed|ing)?|blocked|blocklist(?:ed)?|heads up|for review|for triage|triage)\b|\b(?:example|sample)\b.{0,24}\b(?:link|site|domain|url)\b|\b(?:scam|phish(?:ing)?|malicious)\b.{0,24}\b(?:example|sample)\b)"
)
LINK_HOST_LABEL_RE = re.compile(r"[a-z0-9-]+")
GENERIC_DIGIT_RE = re.compile(r"\b\d{4,12}\b")
@dataclass(frozen=True)
class PackSettings:
    enabled: bool
    low_action: str
    medium_action: str
    high_action: str
    sensitivity: str

    def action_for_confidence(self, confidence: str) -> str:
        if confidence == "high":
            return self.high_action
        if confidence == "medium":
            return self.medium_action
        return self.low_action


@dataclass(frozen=True)
class CompiledCustomPattern:
    pattern_id: str
    label: str
    pattern: str
    mode: str
    action: str
    enabled: bool
    word_re: re.Pattern[str] | None
    wildcard_tokens: tuple[str, ...]

    def matches(self, text: str, squashed: str) -> bool:
        if not self.enabled:
            return False
        if self.mode == "contains":
            token = self.pattern.casefold()
            return token in text or token in squashed
        if self.mode == "word":
            if self.word_re is None:
                return False
            return bool(self.word_re.search(text) or self.word_re.search(squashed))
        return _ordered_token_match(text, self.wildcard_tokens) or _ordered_token_match(squashed, self.wildcard_tokens)


@dataclass(frozen=True)
class CompiledShieldConfig:
    guild_id: int
    module_enabled: bool
    log_channel_id: int | None
    alert_role_id: int | None
    scan_mode: str
    included_channel_ids: frozenset[int]
    excluded_channel_ids: frozenset[int]
    included_user_ids: frozenset[int]
    excluded_user_ids: frozenset[int]
    included_role_ids: frozenset[int]
    excluded_role_ids: frozenset[int]
    trusted_role_ids: frozenset[int]
    allow_domains: frozenset[str]
    allow_invite_codes: frozenset[str]
    allow_phrases: tuple[str, ...]
    privacy: PackSettings
    promo: PackSettings
    scam: PackSettings
    adult: PackSettings
    ai_enabled: bool
    ai_min_confidence: str
    ai_enabled_packs: frozenset[str]
    escalation_threshold: int
    escalation_window_minutes: int
    timeout_minutes: int
    custom_patterns: tuple[CompiledCustomPattern, ...]

    def pack_settings(self, pack: str) -> PackSettings:
        if pack == "privacy":
            return self.privacy
        if pack == "promo":
            return self.promo
        if pack == "scam":
            return self.scam
        if pack == "adult":
            return self.adult
        return PackSettings(enabled=True, low_action="log", medium_action="log", high_action="log", sensitivity="normal")


@dataclass(frozen=True)
class ShieldLink:
    raw_url: str
    canonical_url: str
    domain: str
    path: str
    query: str
    category: str
    invite_code: str | None = None


@dataclass(frozen=True)
class ShieldSnapshot:
    text: str
    squashed: str
    context_text: str
    context_squashed: str
    urls: tuple[str, ...]
    links: tuple[ShieldLink, ...]
    canonical_links: tuple[str, ...]
    domains: frozenset[str]
    link_categories: frozenset[str]
    invite_codes: frozenset[str]
    attachment_names: tuple[str, ...]
    has_links: bool
    has_suspicious_attachment: bool


@dataclass(frozen=True)
class ShieldMatch:
    pack: str
    label: str
    reason: str
    action: str
    confidence: str
    heuristic: bool
    match_class: str = ""


@dataclass
class ShieldDecision:
    matched: bool
    action: str
    pack: str | None
    reasons: tuple[ShieldMatch, ...]
    deleted: bool = False
    logged: bool = False
    timed_out: bool = False
    escalated: bool = False
    action_note: str | None = None
    ai_review: ShieldAIReviewResult | None = None
    link_assessments: tuple[ShieldLinkAssessment, ...] = ()


@dataclass(frozen=True)
class ShieldTestResult:
    matches: tuple[ShieldMatch, ...]
    link_assessments: tuple[ShieldLinkAssessment, ...]
    bypass_reason: str | None = None


@dataclass(frozen=True)
class RepetitionSignals:
    fingerprint: str | None
    hits: int
    pure_media_links: bool
    has_unallowlisted_links: bool


def _ordered_token_match(text: str, tokens: Sequence[str]) -> bool:
    if not tokens:
        return False
    position = 0
    for token in tokens:
        index = text.find(token, position)
        if index < 0:
            return False
        position = index + len(token)
    return True


def _sorted_unique_ints(values: Iterable[Any]) -> list[int]:
    return sorted({value for value in values if isinstance(value, int) and value > 0})


def _sorted_unique_text(values: Iterable[Any]) -> list[str]:
    cleaned = {normalize_plain_text(str(value)).casefold() for value in values if isinstance(value, str) and normalize_plain_text(str(value))}
    return sorted(cleaned)


def _domain_matches(domain: str, candidate: str) -> bool:
    return link_domain_matches(domain, candidate)


def _domain_in_set(domain: str, candidates: set[str]) -> bool:
    return link_domain_in_set(domain, candidates)


def _clean_url_candidate(raw_url: str) -> str | None:
    if not raw_url:
        return None
    candidate = raw_url.strip().strip("()[]{}<>,.!?\"'")
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    return candidate


def _normalize_link_host(raw_host: str) -> str | None:
    host = normalize_plain_text(raw_host).casefold().strip()
    if not host:
        return None
    if "@" in host:
        host = host.rsplit("@", 1)[1]
    if host.startswith("[") or host.endswith("]"):
        return None
    if ":" in host:
        host = host.split(":", 1)[0]
    host = host.rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host or host.startswith(".") or host.endswith(".") or ".." in host:
        return None
    try:
        host = host.encode("idna").decode("ascii")
    except UnicodeError:
        return None
    host = host.casefold().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if not host or host.startswith(".") or host.endswith(".") or ".." in host:
        return None
    labels = host.split(".")
    if len(labels) < 2:
        return None
    for label in labels:
        if not label or len(label) > 63:
            return None
        if label.startswith("-") or label.endswith("-"):
            return None
        if LINK_HOST_LABEL_RE.fullmatch(label) is None:
            return None
    return host


def _extract_domain(raw_url: str) -> str | None:
    candidate = _clean_url_candidate(raw_url)
    if candidate is None:
        return None
    parsed = urlsplit(candidate)
    return _normalize_link_host(parsed.netloc)


def _extract_urls(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    return tuple(match.group(0) for match in URL_RE.finditer(text))


def _strip_urls_from_text(text: str, urls: Sequence[str]) -> str:
    if not text:
        return ""
    stripped = text
    for url in urls:
        stripped = stripped.replace(url, " ")
    return re.sub(r"\s+", " ", stripped).strip()


def _extract_invite_codes(urls: Sequence[str]) -> frozenset[str]:
    codes: set[str] = set()
    for url in urls:
        match = INVITE_RE.search(url)
        if match:
            codes.add(match.group(1).casefold())
    return frozenset(codes)


def _classify_link(domain: str, *, invite_code: str | None) -> str:
    if invite_code is not None or _domain_matches(domain, "discord.gg") or _domain_matches(domain, "discord.com"):
        return "discord_invite"
    if _domain_in_set(domain, MEDIA_EMBED_DOMAINS):
        return "media_embed"
    if _domain_in_set(domain, STOREFRONT_DOMAINS):
        return "storefront"
    if _domain_in_set(domain, SOCIAL_PROMO_DOMAINS):
        return "creator_social"
    if _domain_in_set(domain, SHORTENER_DOMAINS):
        return "shortener"
    return "generic_external"


def _build_link(raw_url: str) -> ShieldLink | None:
    candidate = _clean_url_candidate(raw_url)
    if candidate is None:
        return None
    parsed = urlsplit(candidate)
    domain = _normalize_link_host(parsed.netloc)
    if domain is None:
        return None
    invite_match = INVITE_RE.search(candidate.casefold())
    invite_code = invite_match.group(1).casefold() if invite_match else None
    path = re.sub(r"/{2,}", "/", (parsed.path or "/").casefold()).rstrip("/")
    path = path or "/"
    canonical_url = f"discord-invite:{invite_code}" if invite_code else f"{domain}{path}"
    return ShieldLink(
        raw_url=raw_url,
        canonical_url=canonical_url,
        domain=domain,
        path=path,
        query=(parsed.query or "").casefold(),
        category=_classify_link(domain, invite_code=invite_code),
        invite_code=invite_code,
    )


def _canonical_repetition_fingerprint(snapshot: ShieldSnapshot) -> str | None:
    if not snapshot.text or len(snapshot.text) < 6:
        return None
    canonical_text = snapshot.text
    for link in snapshot.links:
        canonical_text = canonical_text.replace(link.raw_url, f"[{link.category}:{link.canonical_url}]")
    canonical_text = re.sub(r"\s+", " ", canonical_text).strip()
    if not canonical_text:
        return None
    return hashlib.sha1(canonical_text.encode("utf-8")).hexdigest()


def _confidence_rank(confidence: str) -> int:
    return CONFIDENCE_STRENGTH.get(confidence, 0)


def _boost_confidence(confidence: str) -> str:
    if confidence == "low":
        return "medium"
    if confidence == "medium":
        return "high"
    return confidence


def _match_class_label(match_class: str) -> str:
    if not match_class:
        return "Not specified"
    return MATCH_CLASS_LABELS.get(match_class, match_class.replace("_", " ").title())


def _link_assessment_summary(assessment: ShieldLinkAssessment) -> str:
    if assessment.category == MALICIOUS_LINK_CATEGORY:
        return "matched local malicious intel"
    if assessment.category == ADULT_LINK_CATEGORY:
        return "matched local adult intel"
    if assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
        return "lookup candidate only, no action" if assessment.provider_lookup_warranted else "local caution only, no action"
    if assessment.category == UNKNOWN_LINK_CATEGORY:
        return "unknown, no action"
    return "safe or allowlisted"


def _legacy_action_policy(action: str) -> tuple[str, str, str]:
    cleaned = str(action).strip().lower()
    if cleaned == "detect":
        return ("detect", "detect", "detect")
    if cleaned == "log":
        return ("log", "log", "log")
    if cleaned == "delete_log":
        return ("log", "delete_log", "delete_log")
    if cleaned == "timeout_log":
        return ("log", "delete_log", "timeout_log")
    if cleaned == "delete_escalate":
        return ("log", "delete_log", "delete_escalate")
    return ("log", "log", "log")


def _build_snapshot(text: str | None, attachments: Sequence[Any] | None = None) -> ShieldSnapshot:
    normalized = normalize_plain_text(text)
    squashed = squash_for_evasion_checks(normalized.casefold())
    lowered = normalized.casefold()
    urls = _extract_urls(lowered)
    context_text = _strip_urls_from_text(lowered, urls)
    context_squashed = squash_for_evasion_checks(context_text)
    links = tuple(link for link in (_build_link(url) for url in urls) if link is not None)
    domains = frozenset(link.domain for link in links)
    invite_codes = frozenset(link.invite_code for link in links if link.invite_code)
    attachment_names = tuple(
        normalize_plain_text(getattr(attachment, "filename", "")).casefold()
        for attachment in (attachments or [])
        if normalize_plain_text(getattr(attachment, "filename", ""))
    )
    return ShieldSnapshot(
        text=lowered,
        squashed=squashed,
        context_text=context_text,
        context_squashed=context_squashed,
        urls=urls,
        links=links,
        canonical_links=tuple(link.canonical_url for link in links),
        domains=domains,
        link_categories=frozenset(link.category for link in links),
        invite_codes=invite_codes,
        attachment_names=attachment_names,
        has_links=bool(urls),
        has_suspicious_attachment=any(SUSPICIOUS_FILE_RE.search(name) for name in attachment_names),
    )


def _candidate_window(text: str, start: int, end: int, *, radius: int = 28) -> str:
    return text[max(0, start - radius):min(len(text), end + radius)]


def _candidate_is_standalone(text: str, start: int, end: int) -> bool:
    remainder = (text[:start] + text[end:]).strip(" \t\r\n-:;,.!?/\\|()[]{}<>\"'")
    return not remainder


def _sensitivity_threshold(sensitivity: str, *, low: int, normal: int, high: int) -> int:
    return {"low": low, "normal": normal, "high": high}.get(sensitivity, normal)


def _confidence_from_score(score: int) -> str:
    if score >= 3:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _validate_email_candidate(candidate: str) -> str | None:
    cleaned = candidate.strip().strip("()[]{}<>,;:\"'")
    if "@" not in cleaned or cleaned.count("@") != 1:
        return None
    local_part, domain = cleaned.split("@", 1)
    if not (1 <= len(local_part) <= 64 and 4 <= len(domain) <= 255):
        return None
    if local_part.startswith(".") or local_part.endswith(".") or ".." in local_part or ".." in domain:
        return None
    labels = domain.split(".")
    if len(labels) < 2:
        return None
    tld = labels[-1]
    if not (2 <= len(tld) <= 24 and tld.isalpha()):
        return None
    for label in labels:
        if not label or len(label) > 63:
            return None
        if label.startswith("-") or label.endswith("-"):
            return None
        if not re.fullmatch(r"[a-z0-9-]+", label):
            return None
    if not re.fullmatch(r"[a-z0-9.!#$%&'*+/=?^_`{|}~-]+", local_part):
        return None
    return f"{local_part}@{domain}"


def _passes_luhn(number: str) -> bool:
    if not number.isdigit():
        return False
    checksum = 0
    double = False
    for digit in reversed(number):
        value = int(digit)
        if double:
            value *= 2
            if value > 9:
                value -= 9
        checksum += value
        double = not double
    return checksum % 10 == 0


def _is_valid_ssn(candidate: str) -> bool:
    try:
        area, group, serial = (int(part) for part in candidate.split("-"))
    except ValueError:
        return False
    if area == 0 or group == 0 or serial == 0:
        return False
    if area == 666 or area >= 900:
        return False
    return True


def _is_valid_routing_number(candidate: str) -> bool:
    if not re.fullmatch(r"\d{9}", candidate):
        return False
    digits = [int(char) for char in candidate]
    checksum = 3 * (digits[0] + digits[3] + digits[6]) + 7 * (digits[1] + digits[4] + digits[7]) + (digits[2] + digits[5] + digits[8])
    return checksum % 10 == 0


def _candidate_appears_in_url(candidate: str, urls: Sequence[str]) -> bool:
    return any(candidate in url for url in urls)


def _looks_like_scam_warning(text: str) -> bool:
    return bool(SCAM_WARNING_RE.search(text))


class ShieldService:
    def __init__(self, bot: commands.Bot, store: ShieldStateStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = ShieldStateStore()
            except ShieldStorageUnavailable as exc:
                print(f"Shield storage constructor failed: {exc}")
                self.store = ShieldStateStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self.ai_provider = build_shield_ai_provider()
        self.link_safety = ShieldLinkSafetyEngine()
        self._compiled_configs: dict[int, CompiledShieldConfig] = {}
        self._alert_dedup: dict[tuple[int, int], float] = {}
        self._strike_windows: dict[tuple[int, int, str], list[float]] = {}
        self._recent_promos: dict[tuple[int, int, str], list[float]] = {}
        self._last_runtime_prune = 0.0

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Shield storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except ShieldStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Shield storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        self._rebuild_config_cache()
        return True

    async def close(self):
        await self.ai_provider.close()
        await self.link_safety.close()
        await self.store.close()

    def storage_message(self, feature_name: str = "Shield") -> str:
        return f"{feature_name} is temporarily unavailable because Babblebox could not reach its Shield database."

    def get_meta(self) -> dict[str, Any]:
        meta = self.store.state.get("meta")
        if isinstance(meta, dict):
            return {
                "global_ai_override_enabled": bool(meta.get("global_ai_override_enabled")),
                "global_ai_override_updated_by": meta.get("global_ai_override_updated_by"),
                "global_ai_override_updated_at": meta.get("global_ai_override_updated_at"),
            }
        return {
            "global_ai_override_enabled": False,
            "global_ai_override_updated_by": None,
            "global_ai_override_updated_at": None,
        }

    def get_config(self, guild_id: int) -> dict[str, Any]:
        raw = self.store.state.get("guilds", {}).get(str(guild_id))
        if isinstance(raw, dict):
            return normalize_guild_shield_config(guild_id, raw)
        return default_guild_shield_config(guild_id)

    def is_ai_supported_guild(self, guild_id: int | None) -> bool:
        return shield_ai_available_in_guild(guild_id) or self.get_meta()["global_ai_override_enabled"]

    def get_ai_status(self, guild_id: int) -> dict[str, Any]:
        config = self.get_config(guild_id)
        meta = self.get_meta()
        diagnostics = self.ai_provider.diagnostics()
        supported = self.is_ai_supported_guild(guild_id)
        enabled_packs = [pack for pack in config.get("ai_enabled_packs", []) if pack in AI_REVIEW_PACK_SET]
        status_message = diagnostics["status"]
        if not supported:
            status_message = "AI review is not available in this server yet."
        elif not diagnostics["available"]:
            status_message = "AI review is disabled because the provider is not configured."
        elif not config.get("ai_enabled"):
            status_message = "AI review is configured but currently disabled for this server."
        return {
            "supported": supported,
            "support_server_default": shield_ai_available_in_guild(guild_id),
            "global_override_enabled": meta["global_ai_override_enabled"],
            "enabled": bool(config.get("ai_enabled")),
            "enabled_packs": enabled_packs,
            "min_confidence": config.get("ai_min_confidence", "high"),
            "provider": diagnostics.get("provider"),
            "provider_available": bool(diagnostics.get("available")),
            "model": diagnostics.get("model"),
            "timeout_seconds": diagnostics.get("timeout_seconds"),
            "max_chars": diagnostics.get("max_chars"),
            "status": status_message,
        }

    def get_link_safety_status(self) -> dict[str, Any]:
        return self.link_safety.diagnostics()

    async def set_global_ai_override(self, enabled: bool, *, actor_id: int) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Shield AI")
        async with self._lock:
            meta = self.get_meta()
            meta["global_ai_override_enabled"] = bool(enabled)
            meta["global_ai_override_updated_by"] = actor_id
            meta["global_ai_override_updated_at"] = ge.now_utc().isoformat()
            self.store.state["meta"] = meta
            flushed = await self.store.flush()
            if not flushed:
                return False, "Shield AI override could not be saved."
        print(
            "Shield AI override changed: "
            f"enabled={'yes' if enabled else 'no'}, "
            f"actor_id={actor_id}, "
            f"updated_at={self.get_meta()['global_ai_override_updated_at']}"
        )
        return True, f"Global Shield AI override is now {'on' if enabled else 'off'}."

    def test_message(self, guild_id: int, text: str, *, attachments: Sequence[str] | None = None) -> list[ShieldMatch]:
        return list(self.test_message_details(guild_id, text, attachments=attachments).matches)

    def test_message_details(self, guild_id: int, text: str, *, attachments: Sequence[str] | None = None) -> ShieldTestResult:
        compiled = self._compiled_configs.get(guild_id) or self._compile_config(guild_id, self.get_config(guild_id))
        fake_attachments = [type("Attachment", (), {"filename": value})() for value in (attachments or [])]
        snapshot = _build_snapshot(text, fake_attachments)
        now = time.monotonic()
        if self._allow_phrase_bypass(compiled, snapshot):
            link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
            return ShieldTestResult(
                matches=(),
                link_assessments=link_assessments,
                bypass_reason="A guild allow phrase matched this sample, so live Shield handling would bypass it.",
            )
        link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
        matches = tuple(self._collect_matches(compiled, snapshot, link_assessments=link_assessments))
        return ShieldTestResult(matches=matches, link_assessments=link_assessments)

    async def set_module_enabled(self, guild_id: int, enabled: bool) -> tuple[bool, str]:
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__("module_enabled", bool(enabled)),
            success_message=f"Shield is now {'enabled' if enabled else 'disabled'} for this server.",
        )

    def _policy_summary(self, *, low_action: str, medium_action: str, high_action: str) -> str:
        return f"low `{low_action}` | medium `{medium_action}` | high `{high_action}`"

    async def set_pack_config(
        self,
        guild_id: int,
        pack: str,
        *,
        enabled: bool | None = None,
        action: str | None = None,
        low_action: str | None = None,
        medium_action: str | None = None,
        high_action: str | None = None,
        sensitivity: str | None = None,
    ) -> tuple[bool, str]:
        if pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        cleaned_action = action.strip().lower() if isinstance(action, str) else None
        cleaned_low_action = low_action.strip().lower() if isinstance(low_action, str) else None
        cleaned_medium_action = medium_action.strip().lower() if isinstance(medium_action, str) else None
        cleaned_high_action = high_action.strip().lower() if isinstance(high_action, str) else None
        cleaned_sensitivity = sensitivity.strip().lower() if isinstance(sensitivity, str) else None
        if cleaned_action is not None and cleaned_action not in SHIELD_ACTIONS - {"disabled"}:
            return False, "That action is not supported."
        if cleaned_action is not None and any(value is not None for value in (cleaned_low_action, cleaned_medium_action, cleaned_high_action)):
            return False, "Use either the legacy `action` shorthand or explicit low/medium/high actions."
        if cleaned_low_action is not None and cleaned_low_action not in LOW_CONFIDENCE_ACTIONS:
            return False, "Low-confidence actions must be `detect` or `log`."
        if cleaned_medium_action is not None and cleaned_medium_action not in MEDIUM_CONFIDENCE_ACTIONS:
            return False, "Medium-confidence actions must be `detect`, `log`, or `delete_log`."
        if cleaned_high_action is not None and cleaned_high_action not in SHIELD_ACTIONS - {"disabled"}:
            return False, "High-confidence action is not supported."
        if cleaned_sensitivity is not None and cleaned_sensitivity not in SHIELD_SENSITIVITIES:
            return False, "Sensitivity must be low, normal, or high."

        current = self.get_config(guild_id)
        if cleaned_action is not None:
            derived_low, derived_medium, derived_high = _legacy_action_policy(cleaned_action)
        else:
            derived_low = current[f"{pack}_low_action"]
            derived_medium = current[f"{pack}_medium_action"]
            derived_high = current[f"{pack}_high_action"]
        final_low_action = derived_low if cleaned_low_action is None else cleaned_low_action
        final_medium_action = derived_medium if cleaned_medium_action is None else cleaned_medium_action
        final_high_action = derived_high if cleaned_high_action is None else cleaned_high_action

        def mutate(config: dict[str, Any]):
            if enabled is not None:
                config[f"{pack}_enabled"] = bool(enabled)
            if cleaned_action is not None:
                config[f"{pack}_action"] = cleaned_action
                low_default, medium_default, high_default = _legacy_action_policy(cleaned_action)
                config[f"{pack}_low_action"] = low_default
                config[f"{pack}_medium_action"] = medium_default
                config[f"{pack}_high_action"] = high_default
            if cleaned_low_action is not None:
                config[f"{pack}_low_action"] = cleaned_low_action
            if cleaned_medium_action is not None:
                config[f"{pack}_medium_action"] = cleaned_medium_action
            if cleaned_high_action is not None:
                config[f"{pack}_high_action"] = cleaned_high_action
            config[f"{pack}_action"] = config.get(f"{pack}_high_action", config.get(f"{pack}_action", "log"))
            if cleaned_sensitivity is not None:
                config[f"{pack}_sensitivity"] = cleaned_sensitivity

        new_enabled = current[f"{pack}_enabled"] if enabled is None else bool(enabled)
        new_sensitivity = current[f"{pack}_sensitivity"] if cleaned_sensitivity is None else cleaned_sensitivity
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[pack]} is {'enabled' if new_enabled else 'disabled'} "
                f"with {self._policy_summary(low_action=final_low_action, medium_action=final_medium_action, high_action=final_high_action)} "
                f"at {SENSITIVITY_LABELS[new_sensitivity].lower()} sensitivity."
            ),
        )

    async def set_log_channel(self, guild_id: int, channel_id: int | None) -> tuple[bool, str]:
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__("log_channel_id", channel_id),
            success_message="Shield log channel updated." if channel_id else "Shield log channel cleared.",
        )

    async def set_alert_role(self, guild_id: int, role_id: int | None) -> tuple[bool, str]:
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__("alert_role_id", role_id),
            success_message="Shield alert role updated." if role_id else "Shield alert role cleared.",
        )

    async def set_scan_mode(self, guild_id: int, mode: str) -> tuple[bool, str]:
        cleaned = str(mode).strip().lower()
        if cleaned not in {"all", "only_included"}:
            return False, "Scan mode must be `all` or `only_included`."
        return await self._update_config(
            guild_id,
            lambda config: config.__setitem__("scan_mode", cleaned),
            success_message=(
                "Shield now scans the full server scope." if cleaned == "all" else "Shield now scans only explicitly included channels, users, or roles."
            ),
        )

    async def set_filter_target(self, guild_id: int, field: str, target_id: int, enabled: bool) -> tuple[bool, str]:
        if field not in {
            "included_channel_ids",
            "excluded_channel_ids",
            "included_user_ids",
            "excluded_user_ids",
            "included_role_ids",
            "excluded_role_ids",
            "trusted_role_ids",
        }:
            return False, "Unknown Shield filter."
        label = field.replace("_ids", "").replace("_", " ")

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_ints(config.get(field, [])))
            if enabled:
                values.add(target_id)
            else:
                values.discard(target_id)
            if len(values) > FILTER_LIMIT:
                raise ValueError(f"You can keep up to {FILTER_LIMIT} entries in `{label}`.")
            config[field] = sorted(values)

        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Shield {label} was {'updated' if enabled else 'trimmed'}.",
        )

    async def set_allow_entry(self, guild_id: int, field: str, value: str, enabled: bool) -> tuple[bool, str]:
        if field == "allow_domains":
            valid, cleaned = self._normalize_domain(value)
        elif field == "allow_invite_codes":
            valid, cleaned = self._normalize_invite_code(value)
        elif field == "allow_phrases":
            valid, cleaned = self._normalize_allow_phrase(value)
        else:
            return False, "Unknown allowlist bucket."
        if not valid:
            return False, cleaned

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_text(config.get(field, [])))
            if enabled:
                values.add(cleaned)
            else:
                values.discard(cleaned)
            if len(values) > ALLOWLIST_LIMIT:
                raise ValueError(f"You can keep up to {ALLOWLIST_LIMIT} entries in that allowlist.")
            config[field] = sorted(values)

        pretty = field.replace("allow_", "").replace("_", " ")
        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Shield allowlist for {pretty} was {'updated' if enabled else 'trimmed'}.",
        )

    async def set_escalation(
        self,
        guild_id: int,
        *,
        threshold: int | None = None,
        window_minutes: int | None = None,
        timeout_minutes: int | None = None,
    ) -> tuple[bool, str]:
        if threshold is not None and not (2 <= threshold <= 6):
            return False, "Escalation threshold must be between 2 and 6."
        if window_minutes is not None and not (5 <= window_minutes <= 120):
            return False, "Escalation window must be between 5 and 120 minutes."
        if timeout_minutes is not None and not (1 <= timeout_minutes <= 60):
            return False, "Timeout length must be between 1 and 60 minutes."

        def mutate(config: dict[str, Any]):
            if threshold is not None:
                config["escalation_threshold"] = threshold
            if window_minutes is not None:
                config["escalation_window_minutes"] = window_minutes
            if timeout_minutes is not None:
                config["timeout_minutes"] = timeout_minutes

        preview = self.get_config(guild_id)
        final_threshold = preview["escalation_threshold"] if threshold is None else threshold
        final_window = preview["escalation_window_minutes"] if window_minutes is None else window_minutes
        final_timeout = preview["timeout_minutes"] if timeout_minutes is None else timeout_minutes
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Escalation now uses `{final_threshold}` hits in `{final_window}` minutes, "
                f"with a `{final_timeout}` minute timeout when Babblebox has permission."
            ),
        )

    async def set_ai_config(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        min_confidence: str | None = None,
        enabled_packs: Sequence[str] | None = None,
    ) -> tuple[bool, str]:
        if not self.is_ai_supported_guild(guild_id):
            return False, "AI review is not available in this server yet."
        cleaned_min_confidence = str(min_confidence).strip().lower() if isinstance(min_confidence, str) else None
        if cleaned_min_confidence is not None and cleaned_min_confidence not in SHIELD_AI_MIN_CONFIDENCE_CHOICES:
            return False, "AI review confidence threshold must be low, medium, or high."
        cleaned_packs: list[str] | None = None
        if enabled_packs is not None:
            cleaned_packs = []
            for item in enabled_packs:
                pack = str(item).strip().lower()
                if pack not in AI_REVIEW_PACK_SET:
                    return False, "AI review packs must be privacy, promo, or scam."
                if pack not in cleaned_packs:
                    cleaned_packs.append(pack)
        if enabled is True and not self.ai_provider.diagnostics().get("available"):
            return False, "AI review cannot be enabled until the provider is configured."

        def mutate(config: dict[str, Any]):
            if enabled is not None:
                config["ai_enabled"] = bool(enabled)
            if cleaned_min_confidence is not None:
                config["ai_min_confidence"] = cleaned_min_confidence
            if cleaned_packs is not None:
                config["ai_enabled_packs"] = cleaned_packs

        current = self.get_config(guild_id)
        final_enabled = current["ai_enabled"] if enabled is None else bool(enabled)
        final_min_confidence = current["ai_min_confidence"] if cleaned_min_confidence is None else cleaned_min_confidence
        final_packs = current["ai_enabled_packs"] if cleaned_packs is None else cleaned_packs
        if final_enabled and not final_packs:
            return False, "Select at least one local Shield pack before enabling AI review."
        pack_summary = ", ".join(PACK_LABELS[pack] for pack in final_packs) if final_packs else "no packs selected"
        provider_status = self.ai_provider.diagnostics().get("status", "Unavailable.")
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Shield AI review is now {'enabled' if final_enabled else 'disabled'} "
                f"at `{final_min_confidence}` local-confidence threshold for {pack_summary}. "
                f"Provider status: {provider_status}"
            ),
        )

    async def add_custom_pattern(
        self,
        guild_id: int,
        *,
        label: str,
        pattern: str,
        mode: str,
        action: str,
    ) -> tuple[bool, str]:
        valid, payload_or_error = self._validate_custom_pattern(label=label, pattern=pattern, mode=mode, action=action)
        if not valid:
            return False, payload_or_error
        payload = payload_or_error

        def mutate(config: dict[str, Any]):
            items = [item for item in config.get("custom_patterns", []) if isinstance(item, dict)]
            if len(items) >= CUSTOM_PATTERN_LIMIT:
                raise ValueError(f"You can keep up to {CUSTOM_PATTERN_LIMIT} advanced Shield patterns.")
            items.append(payload)
            config["custom_patterns"] = items

        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Advanced Shield pattern `{payload['label']}` added. "
                "Raw user regex is intentionally not supported; Babblebox uses safe contains, whole-word, and wildcard matching instead."
            ),
        )

    async def remove_custom_pattern(self, guild_id: int, pattern_id_prefix: str) -> tuple[bool, str]:
        cleaned = normalize_plain_text(pattern_id_prefix).casefold()
        if not cleaned:
            return False, "Provide the pattern ID from `/shield advanced list`."
        current = self.get_config(guild_id)
        matches = [item for item in current.get("custom_patterns", []) if isinstance(item, dict) and str(item.get("pattern_id", "")).casefold().startswith(cleaned)]
        if not matches:
            return False, "No advanced Shield pattern matched that ID."
        if len(matches) > 1:
            return False, "That ID prefix matches multiple patterns. Use a longer ID."
        target_id = matches[0]["pattern_id"]

        def mutate(config: dict[str, Any]):
            config["custom_patterns"] = [
                item for item in config.get("custom_patterns", []) if not (isinstance(item, dict) and item.get("pattern_id") == target_id)
            ]

        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Advanced Shield pattern `{target_id}` was removed.",
        )

    async def handle_message(self, message: discord.Message) -> ShieldDecision | None:
        if not self.storage_ready or message.guild is None or message.webhook_id is not None or getattr(message.author, "bot", False):
            return None

        compiled = self._compiled_configs.get(message.guild.id)
        if compiled is None or not compiled.module_enabled:
            return None
        if not self._message_in_scope(compiled, message):
            return None

        now = asyncio.get_running_loop().time()
        self._prune_runtime_state(now)
        snapshot = _build_snapshot(message.content, message.attachments)
        if self._allow_phrase_bypass(compiled, snapshot):
            return None

        repetition = self._track_repetitive_promo(message, compiled, snapshot, now)
        link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
        matches = self._collect_matches(compiled, snapshot, repetitive_promo=repetition, link_assessments=link_assessments)
        if not matches:
            return None

        best = max(
            matches,
            key=lambda item: (
                ACTION_STRENGTH.get(item.action, 0),
                CONFIDENCE_STRENGTH.get(item.confidence, 0),
                PACK_STRENGTH.get(item.pack, 0),
            ),
        )
        decision = ShieldDecision(
            matched=True,
            action=best.action,
            pack=best.pack,
            reasons=tuple(matches[:3]),
            link_assessments=link_assessments,
        )

        if best.action.startswith("delete"):
            decision.deleted = await self._delete_message(message)
            if not decision.deleted:
                decision.action_note = "Delete was configured, but Babblebox could not delete the message."

        if best.action == "timeout_log":
            decision.timed_out = await self._timeout_member(message, compiled, reason=f"Babblebox Shield matched {PACK_LABELS.get(best.pack, 'Shield')}.")
            if not decision.timed_out:
                decision.action_note = "Timeout was configured, but Babblebox could not time out that member."

        if self._is_escalation_eligible(best):
            strike_count = self._record_strike(message.guild.id, message.author.id, best.pack, compiled, now)
            if strike_count >= compiled.escalation_threshold:
                decision.timed_out = await self._timeout_member(
                    message,
                    compiled,
                    reason=f"Babblebox Shield escalation after repeated {PACK_LABELS.get(best.pack, 'Shield').lower()} hits.",
                )
                decision.escalated = decision.timed_out
                if decision.timed_out:
                    decision.action_note = (
                        f"Repeated-hit escalation triggered after {strike_count} strikes in {compiled.escalation_window_minutes} minutes."
                    )
                elif decision.action_note is None:
                    decision.action_note = "Repeated-hit escalation was configured, but Babblebox could not time out that member."
        elif best.action == "delete_escalate":
            decision.action_note = "Repeated-hit escalation is reserved for high-confidence, non-noise Shield matches."

        if self._should_request_ai_review(compiled, decision):
            request = self._build_ai_review_request(
                message,
                snapshot,
                decision,
                repetitive_promo=repetition.hits >= DIRECT_PROMO_REPEAT_THRESHOLD,
            )
            if request is not None:
                decision.ai_review = await self.ai_provider.review(request)

        if best.action not in {"disabled", "detect"}:
            await self._send_alert(message, compiled, decision)

        return decision

    def _should_request_ai_review(self, compiled: CompiledShieldConfig, decision: ShieldDecision) -> bool:
        if decision.action in {"disabled", "detect"}:
            return False
        if compiled.log_channel_id is None or not compiled.ai_enabled:
            return False
        if not self.is_ai_supported_guild(compiled.guild_id):
            return False
        if decision.pack not in compiled.ai_enabled_packs or decision.pack not in AI_REVIEW_PACK_SET:
            return False
        if not self.ai_provider.diagnostics().get("available"):
            return False
        top_reason = decision.reasons[0] if decision.reasons else None
        if top_reason is None or top_reason.match_class == "repetitive_link_noise":
            return False
        return CONFIDENCE_STRENGTH.get(top_reason.confidence, 0) >= CONFIDENCE_STRENGTH.get(compiled.ai_min_confidence, 3)

    def _build_ai_review_request(
        self,
        message: discord.Message,
        snapshot: ShieldSnapshot,
        decision: ShieldDecision,
        *,
        repetitive_promo: bool,
    ) -> ShieldAIReviewRequest | None:
        top_reason = decision.reasons[0] if decision.reasons else None
        if top_reason is None or decision.pack is None:
            return None
        max_chars = int(self.ai_provider.diagnostics().get("max_chars") or 340)
        sanitized = sanitize_message_for_ai(message.content, max_chars=max_chars)
        return ShieldAIReviewRequest(
            guild_id=message.guild.id,
            pack=decision.pack,
            local_confidence=top_reason.confidence,
            local_action=decision.action,
            local_labels=tuple(item.label for item in decision.reasons[:3]),
            local_reasons=tuple(item.reason for item in decision.reasons[:2]),
            sanitized_content=sanitized.text,
            has_links=snapshot.has_links,
            domains=tuple(sorted(snapshot.domains)[:3]),
            has_suspicious_attachment=snapshot.has_suspicious_attachment,
            attachment_extensions=summarize_attachment_extensions(snapshot.attachment_names),
            invite_detected=bool(snapshot.invite_codes),
            repetitive_promo=repetitive_promo,
        )

    def _allow_phrase_bypass(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> bool:
        return any(phrase in snapshot.text for phrase in compiled.allow_phrases)

    def _message_in_scope(self, compiled: CompiledShieldConfig, message: discord.Message) -> bool:
        author_id = getattr(message.author, "id", 0)
        if message.channel.id in compiled.excluded_channel_ids or author_id in compiled.excluded_user_ids:
            return False
        role_ids = {
            role.id
            for role in getattr(message.author, "roles", [])
            if getattr(role, "id", None) is not None
        }
        if compiled.trusted_role_ids.intersection(role_ids):
            return False
        if compiled.excluded_role_ids.intersection(role_ids):
            return False
        if compiled.scan_mode != "only_included":
            return True
        return (
            (message.channel.id in compiled.included_channel_ids)
            or (author_id in compiled.included_user_ids)
            or bool(compiled.included_role_ids.intersection(role_ids))
        )

    def _domain_is_allowlisted(self, domain: str, allow_domains: frozenset[str]) -> bool:
        return any(_domain_matches(domain, candidate) for candidate in allow_domains)

    def _collect_link_assessments(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        *,
        now: float,
    ) -> tuple[ShieldLinkAssessment, ...]:
        if not snapshot.links:
            return ()
        by_domain: dict[str, ShieldLinkAssessment] = {}
        for link in snapshot.links:
            allowlisted = False
            if link.invite_code is None:
                allowlisted = self._domain_is_allowlisted(link.domain, compiled.allow_domains)
            assessment = self.link_safety.assess_domain(
                link.domain,
                path=link.path,
                query=link.query,
                message_text=snapshot.context_text,
                squashed_text=snapshot.context_squashed,
                has_suspicious_attachment=snapshot.has_suspicious_attachment,
                allowlisted=allowlisted,
                now=now,
            )
            by_domain[link.domain] = merge_link_assessments(by_domain.get(link.domain), assessment)
        return tuple(sorted(by_domain.values(), key=lambda item: item.normalized_domain))

    def _make_pack_match(
        self,
        *,
        pack: str,
        settings: PackSettings,
        label: str,
        reason: str,
        confidence: str,
        heuristic: bool,
        match_class: str,
    ) -> ShieldMatch:
        return ShieldMatch(
            pack=pack,
            label=label,
            reason=reason,
            action=settings.action_for_confidence(confidence),
            confidence=confidence,
            heuristic=heuristic,
            match_class=match_class,
        )

    def _boost_match_for_repetition(self, match: ShieldMatch, settings: PackSettings, *, hits: int) -> ShieldMatch:
        boosted_confidence = _boost_confidence(match.confidence)
        return ShieldMatch(
            pack=match.pack,
            label=match.label,
            reason=f"{match.reason} It was repeated {hits} times in a short window.",
            action=settings.action_for_confidence(boosted_confidence),
            confidence=boosted_confidence,
            heuristic=match.heuristic,
            match_class=match.match_class,
        )

    def _is_escalation_eligible(self, match: ShieldMatch) -> bool:
        return (
            match.action == "delete_escalate"
            and match.confidence == "high"
            and match.match_class not in ESCALATION_BLOCKED_MATCH_CLASSES
        )

    def _collect_matches(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        *,
        repetitive_promo: RepetitionSignals | None = None,
        link_assessments: Sequence[ShieldLinkAssessment] | None = None,
    ) -> list[ShieldMatch]:
        active_link_assessments = tuple(link_assessments or ())
        matches: list[ShieldMatch] = []
        matches.extend(self._detect_privacy(compiled, snapshot))
        matches.extend(self._detect_promo(compiled, snapshot, repetitive_promo=repetitive_promo or RepetitionSignals(None, 0, False, False)))
        matches.extend(self._detect_link_safety_domains(compiled, snapshot, active_link_assessments))
        matches.extend(self._detect_scam(compiled, snapshot, active_link_assessments))
        matches.extend(self._detect_custom_patterns(compiled, snapshot))
        matches.sort(
            key=lambda item: (
                ACTION_STRENGTH.get(item.action, 0),
                CONFIDENCE_STRENGTH.get(item.confidence, 0),
                PACK_STRENGTH.get(item.pack, 0),
            ),
            reverse=True,
        )
        return matches

    def _detect_link_safety_domains(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
    ) -> list[ShieldMatch]:
        if not link_assessments:
            return []
        warning_context = _looks_like_scam_warning(snapshot.context_text)
        matches: list[ShieldMatch] = []
        for assessment in link_assessments:
            if assessment.category == MALICIOUS_LINK_CATEGORY and compiled.scam.enabled:
                if warning_context:
                    continue
                matches.append(
                    self._make_pack_match(
                        pack="scam",
                        settings=compiled.scam,
                        label="Known malicious domain",
                        reason="A linked domain matched Shield's bundled malicious-domain intelligence.",
                        confidence="high",
                        heuristic=False,
                        match_class="known_malicious_domain",
                    )
                )
            if assessment.category == ADULT_LINK_CATEGORY and compiled.adult.enabled:
                matches.append(
                    self._make_pack_match(
                        pack="adult",
                        settings=compiled.adult,
                        label="Adult / 18+ domain",
                        reason="A linked domain matched Shield's bundled adult / 18+ domain intelligence.",
                        confidence="high",
                        heuristic=False,
                        match_class="adult_domain",
                    )
                )
        return self._dedupe_matches(matches)

    def _detect_privacy(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> list[ShieldMatch]:
        settings = compiled.privacy
        if not settings.enabled:
            return []
        matches: list[ShieldMatch] = []
        email_match = self._detect_privacy_email(settings, snapshot)
        if email_match is not None:
            matches.append(email_match)
        phone_match = self._detect_privacy_phone(settings, snapshot)
        if phone_match is not None:
            matches.append(phone_match)
        ip_match = self._detect_privacy_ip(settings, snapshot)
        if ip_match is not None:
            matches.append(ip_match)
        crypto_match = self._detect_privacy_crypto(settings, snapshot)
        if crypto_match is not None:
            matches.append(crypto_match)
        payment_match = self._detect_privacy_payment(settings, snapshot)
        if payment_match is not None:
            matches.append(payment_match)
        matches.extend(self._detect_privacy_sensitive_ids(settings, snapshot))
        return self._dedupe_matches(matches)

    def _detect_privacy_email(self, settings: PackSettings, snapshot: ShieldSnapshot) -> ShieldMatch | None:
        seen: set[str] = set()
        for match in EMAIL_RE.finditer(snapshot.text):
            candidate = _validate_email_candidate(match.group(0))
            if candidate is None or candidate in seen:
                continue
            seen.add(candidate)
            nearby = _candidate_window(snapshot.text, *match.span())
            score = 2
            if EMAIL_CONTEXT_RE.search(nearby):
                score += 1
            if _candidate_is_standalone(snapshot.text, *match.span()):
                score += 1
            return ShieldMatch(
                pack="privacy",
                label="Possible email address",
                reason="A structured email address was posted in chat.",
                action=settings.action_for_confidence(_confidence_from_score(score)),
                confidence=_confidence_from_score(score),
                heuristic=False,
                match_class="privacy_email",
            )
        return None

    def _detect_privacy_phone(self, settings: PackSettings, snapshot: ShieldSnapshot) -> ShieldMatch | None:
        threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        best_score = 0
        for match in PHONE_RE.finditer(snapshot.text):
            candidate = match.group(0).strip()
            digits = re.sub(r"\D", "", candidate)
            if not (7 <= len(digits) <= 15):
                continue
            if len(set(digits)) == 1:
                continue
            nearby = _candidate_window(snapshot.text, *match.span())
            has_context = bool(PHONE_CONTEXT_RE.search(nearby))
            if len(digits) < 10 and not has_context:
                continue
            if candidate.count(".") >= 2 and not has_context:
                continue
            score = 0
            if len(digits) >= 10:
                score += 1
            if any(token in candidate for token in ("+", "-", "(", ")", " ")):
                score += 1
            if has_context:
                score += 1
            if _candidate_is_standalone(snapshot.text, *match.span()) and len(digits) >= 10:
                score += 1
            best_score = max(best_score, score)
        if best_score < threshold:
            return None
        return ShieldMatch(
            pack="privacy",
            label="Possible phone number",
            reason="A phone-like number passed structure checks and looked like a contact detail.",
            action=settings.action_for_confidence(_confidence_from_score(best_score)),
            confidence=_confidence_from_score(best_score),
            heuristic=False,
            match_class="privacy_phone",
        )

    def _detect_privacy_ip(self, settings: PackSettings, snapshot: ShieldSnapshot) -> ShieldMatch | None:
        threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        best_score = 0
        for pattern in (IPV4_RE, IPV6_RE):
            for match in pattern.finditer(snapshot.text):
                candidate = match.group(0)
                try:
                    parsed = ipaddress.ip_address(candidate)
                except ValueError:
                    continue
                nearby = _candidate_window(snapshot.text, *match.span())
                in_url = _candidate_appears_in_url(candidate, snapshot.urls)
                score = 0
                if not in_url:
                    score += 1
                if IP_CONTEXT_RE.search(nearby):
                    score += 1
                if not parsed.is_global:
                    score += 1
                if _candidate_is_standalone(snapshot.text, *match.span()):
                    score += 1
                best_score = max(best_score, score)
        if best_score < threshold:
            return None
        return ShieldMatch(
            pack="privacy",
            label="Possible IP or host detail",
            reason="A validated network address appeared with signals that looked more private than harmless.",
            action=settings.action_for_confidence(_confidence_from_score(best_score)),
            confidence=_confidence_from_score(best_score),
            heuristic=True,
            match_class="privacy_ip",
        )

    def _detect_privacy_crypto(self, settings: PackSettings, snapshot: ShieldSnapshot) -> ShieldMatch | None:
        threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        best_score = 0
        patterns = [ETH_WALLET_RE]
        if settings.sensitivity != "low":
            patterns.append(BTC_WALLET_RE)
        for pattern in patterns:
            for match in pattern.finditer(snapshot.text):
                nearby = _candidate_window(snapshot.text, *match.span())
                score = 2
                if CRYPTO_CONTEXT_RE.search(nearby):
                    score += 1
                if _candidate_is_standalone(snapshot.text, *match.span()):
                    score += 1
                best_score = max(best_score, score)
        if best_score < threshold:
            return None
        return ShieldMatch(
            pack="privacy",
            label="Possible crypto wallet",
            reason="A wallet-style address was posted with enough structure and context to look intentional.",
            action=settings.action_for_confidence(_confidence_from_score(best_score)),
            confidence=_confidence_from_score(best_score),
            heuristic=True,
            match_class="privacy_wallet",
        )

    def _detect_privacy_payment(self, settings: PackSettings, snapshot: ShieldSnapshot) -> ShieldMatch | None:
        threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=3, high=3)
        best_score = 0
        for match in CARD_RE.finditer(snapshot.text):
            candidate = match.group(0)
            digits = re.sub(r"\D", "", candidate)
            if not (13 <= len(digits) <= 19):
                continue
            if len(set(digits)) == 1 or not _passes_luhn(digits):
                continue
            nearby = _candidate_window(snapshot.text, *match.span())
            score = 2
            if PAYMENT_CONTEXT_RE.search(nearby):
                score += 1
            if _candidate_is_standalone(snapshot.text, *match.span()) and any(token in candidate for token in (" ", "-")):
                score += 1
            best_score = max(best_score, score)
        if best_score < threshold:
            return None
        return ShieldMatch(
            pack="privacy",
            label="Possible payment detail",
            reason="A card-like number passed checksum validation and matched payment-style context.",
            action=settings.action_for_confidence(_confidence_from_score(best_score)),
            confidence=_confidence_from_score(best_score),
            heuristic=False,
            match_class="privacy_payment",
        )

    def _detect_privacy_sensitive_ids(self, settings: PackSettings, snapshot: ShieldSnapshot) -> list[ShieldMatch]:
        matches: list[ShieldMatch] = []
        for match in SSN_RE.finditer(snapshot.text):
            if _is_valid_ssn(match.group(0)):
                matches.append(
                    ShieldMatch(
                        pack="privacy",
                        label="Possible sensitive ID number",
                        reason="A structured SSN-style number passed basic validity checks.",
                        action=settings.action_for_confidence("high"),
                        confidence="high",
                        heuristic=False,
                        match_class="privacy_ssn",
                    )
                )
                break

        routing_threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        otp_threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        account_threshold = _sensitivity_threshold(settings.sensitivity, low=2, normal=2, high=3)
        for match in GENERIC_DIGIT_RE.finditer(snapshot.text):
            candidate = match.group(0)
            nearby = _candidate_window(snapshot.text, *match.span())
            if len(candidate) == 9 and ROUTING_CONTEXT_RE.search(nearby) and _is_valid_routing_number(candidate):
                score = 2
                if _candidate_is_standalone(snapshot.text, *match.span()):
                    score += 1
                if score >= routing_threshold:
                    matches.append(
                        ShieldMatch(
                            pack="privacy",
                            label="Possible routing number",
                            reason="A 9-digit number matched routing context and a checksum-style validation.",
                            action=settings.action_for_confidence(_confidence_from_score(score)),
                            confidence=_confidence_from_score(score),
                            heuristic=False,
                            match_class="privacy_routing",
                        )
                    )
                    break

        for match in GENERIC_DIGIT_RE.finditer(snapshot.text):
            candidate = match.group(0)
            nearby = _candidate_window(snapshot.text, *match.span())
            if not OTP_CONTEXT_RE.search(nearby):
                continue
            score = 2
            if _candidate_is_standalone(snapshot.text, *match.span()) or ":" in nearby or "#" in nearby:
                score += 1
            if score >= otp_threshold:
                matches.append(
                    ShieldMatch(
                        pack="privacy",
                        label="Possible verification code",
                        reason="A short code appeared next to OTP or verification wording.",
                        action=settings.action_for_confidence(_confidence_from_score(score)),
                        confidence=_confidence_from_score(score),
                        heuristic=True,
                        match_class="privacy_otp",
                    )
                )
                break

        for match in GENERIC_DIGIT_RE.finditer(snapshot.text):
            candidate = match.group(0)
            if not (8 <= len(candidate) <= 12):
                continue
            nearby = _candidate_window(snapshot.text, *match.span())
            if not ACCOUNT_ID_CONTEXT_RE.search(nearby):
                continue
            score = 1
            if _candidate_is_standalone(snapshot.text, *match.span()) or ":" in nearby or "#" in nearby:
                score += 1
            if score >= account_threshold:
                matches.append(
                    ShieldMatch(
                        pack="privacy",
                        label="Possible sensitive ID number",
                        reason="A long ID-like number appeared with account, passport, or tax-ID context.",
                        action=settings.action_for_confidence(_confidence_from_score(score)),
                        confidence=_confidence_from_score(score),
                        heuristic=True,
                        match_class="privacy_account_id",
                    )
                )
                break
        return matches

    def _detect_promo(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot, *, repetitive_promo: RepetitionSignals) -> list[ShieldMatch]:
        settings = compiled.promo
        if not settings.enabled:
            return []
        matches: list[ShieldMatch] = []
        unallowlisted_links = [
            link
            for link in snapshot.links
            if (
                (link.invite_code is not None and link.invite_code not in compiled.allow_invite_codes)
                or (link.invite_code is None and not self._domain_is_allowlisted(link.domain, compiled.allow_domains))
            )
        ]
        unallowlisted_invites = [link for link in unallowlisted_links if link.invite_code is not None]
        creator_links = [link for link in unallowlisted_links if link.category == "creator_social"]
        storefront_links = [link for link in unallowlisted_links if link.category == "storefront"]
        shortener_links = [link for link in unallowlisted_links if link.category == "shortener"]
        cta = bool(PROMO_CTA_RE.search(snapshot.text) or PROMO_CTA_RE.search(snapshot.squashed))
        invite_cta = bool(INVITE_CTA_RE.search(snapshot.text))
        monetized = bool(MONETIZED_PROMO_RE.search(snapshot.text))
        promo_context = bool(PROMO_CONTEXT_RE.search(snapshot.text))

        if unallowlisted_invites:
            matches.append(
                self._make_pack_match(
                    pack="promo",
                    settings=settings,
                    label="Discord invite link",
                    reason="A Discord invite was posted with enough server-promo context to warrant review.",
                    confidence="high" if invite_cta or len(unallowlisted_invites) > 1 else "medium",
                    heuristic=False,
                    match_class="discord_invite",
                )
            )
        if creator_links and (cta or monetized or promo_context):
            matches.append(
                self._make_pack_match(
                    pack="promo",
                    settings=settings,
                    label="Self-promo link",
                    reason="A creator or social link was paired with promo wording.",
                    confidence="medium" if cta or monetized else "low",
                    heuristic=True,
                    match_class="self_promo",
                )
            )
        if monetized and storefront_links:
            matches.append(
                self._make_pack_match(
                    pack="promo",
                    settings=settings,
                    label="Monetized promo wording",
                    reason="Sales or commission language appeared next to an external link.",
                    confidence="high" if cta else "medium",
                    heuristic=True,
                    match_class="monetized_promo",
                )
            )
        if settings.sensitivity == "high" and cta and (creator_links or storefront_links or shortener_links) and promo_context:
            matches.append(
                self._make_pack_match(
                    pack="promo",
                    settings=settings,
                    label="Call-to-action promo link",
                    reason="A promo-style call to action was paired with external links and other promotion signals.",
                    confidence="low",
                    heuristic=True,
                    match_class="cta_promo",
                )
            )
        if repetitive_promo.hits >= DIRECT_PROMO_REPEAT_THRESHOLD and matches:
            boosted_matches: list[ShieldMatch] = []
            for item in matches:
                if item.match_class in {"discord_invite", "self_promo", "monetized_promo", "cta_promo"}:
                    boosted_matches.append(self._boost_match_for_repetition(item, settings, hits=repetitive_promo.hits))
                else:
                    boosted_matches.append(item)
            matches = boosted_matches
        elif repetitive_promo.has_unallowlisted_links:
            noise_threshold = MEDIA_LINK_NOISE_THRESHOLD if repetitive_promo.pure_media_links else GENERIC_LINK_NOISE_THRESHOLD
            if repetitive_promo.hits >= noise_threshold and (settings.sensitivity == "high" or not repetitive_promo.pure_media_links):
                reason = (
                    "The same media or GIF link was repeated several times. This looks noisy, not promotional."
                    if repetitive_promo.pure_media_links
                    else "The same external link message was repeated several times without enough promo evidence to treat it as self-promo."
                )
                matches.append(
                    self._make_pack_match(
                        pack="promo",
                        settings=settings,
                        label="Repetitive link noise",
                        reason=reason,
                        confidence="low",
                        heuristic=True,
                        match_class="repetitive_link_noise",
                    )
                )
        return self._dedupe_matches(matches)

    def _track_repetitive_promo(
        self,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        now: float,
    ) -> RepetitionSignals:
        fingerprint = _canonical_repetition_fingerprint(snapshot)
        if fingerprint is None:
            return RepetitionSignals(None, 0, False, False)
        key = (message.guild.id, message.author.id, fingerprint)
        hits = [value for value in self._recent_promos.get(key, []) if now - value <= REPETITION_WINDOW_SECONDS]
        hits.append(now)
        self._recent_promos[key] = hits
        has_unallowlisted_links = any(
            (
                (link.invite_code is not None and link.invite_code not in compiled.allow_invite_codes)
                or (link.invite_code is None and not self._domain_is_allowlisted(link.domain, compiled.allow_domains))
            )
            for link in snapshot.links
        )
        pure_media_links = bool(snapshot.links) and all(link.category == "media_embed" for link in snapshot.links)
        return RepetitionSignals(
            fingerprint=fingerprint,
            hits=len(hits),
            pure_media_links=pure_media_links,
            has_unallowlisted_links=has_unallowlisted_links,
        )

    def _detect_scam(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
    ) -> list[ShieldMatch]:
        settings = compiled.scam
        if not settings.enabled:
            return []
        if _looks_like_scam_warning(snapshot.context_text):
            return []
        matches: list[ShieldMatch] = []
        risky_domains = [
            assessment.normalized_domain
            for assessment in link_assessments
            if assessment.category in {MALICIOUS_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
        ]
        shortener = any(_domain_in_set(domain, SHORTENER_DOMAINS) or "xn--" in domain for domain in risky_domains)
        bait = bool(SCAM_BAIT_RE.search(snapshot.context_text) or SCAM_BAIT_RE.search(snapshot.context_squashed))
        social_engineering = bool(SOCIAL_ENGINEERING_RE.search(snapshot.context_text))
        brand_bait = bool(BRAND_BAIT_RE.search(snapshot.context_text))
        dangerous_link = any(SUSPICIOUS_FILE_RE.search(url) for url in snapshot.urls)
        risky_link_present = bool(risky_domains)

        if bait and ((snapshot.has_links and risky_link_present) or snapshot.has_suspicious_attachment or dangerous_link):
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Scam bait + link",
                    reason="Gift, claim, or verification bait appeared next to a link or suspicious file.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_bait_link",
                )
            )
        if shortener and social_engineering:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Shortened or punycode lure",
                    reason="A shortened or punycode-style link appeared with instructions to open, claim, or verify something.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_shortener",
                )
            )
        if snapshot.has_suspicious_attachment and social_engineering:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Executable or archive lure",
                    reason="Suspicious file types were paired with social-engineering wording.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_attachment",
                )
            )
        if dangerous_link and social_engineering:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Executable download link",
                    reason="A download-style instruction pointed to an executable or archive-style URL.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_download",
                )
            )
        if brand_bait and social_engineering and risky_domains:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Brand-linked lure",
                    reason="Brand bait appeared with a link and coercive wording.",
                    action=settings.action_for_confidence("medium"),
                    confidence="medium",
                    heuristic=True,
                    match_class="scam_brand_lure",
                )
            )
        if settings.sensitivity == "high" and bait and (social_engineering or brand_bait) and (risky_link_present or snapshot.has_suspicious_attachment):
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Scam bait wording",
                    reason="The message contains known claim or gift-bait wording reinforced by scam-style instructions or branding.",
                    action=settings.action_for_confidence("low"),
                    confidence="low",
                    heuristic=True,
                    match_class="scam_bait_wording",
                )
            )
        return self._dedupe_matches(matches)

    def _detect_custom_patterns(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> list[ShieldMatch]:
        matches: list[ShieldMatch] = []
        for pattern in compiled.custom_patterns:
            if pattern.matches(snapshot.text, snapshot.squashed):
                matches.append(
                    ShieldMatch(
                        pack="advanced",
                        label=f"Custom pattern: {pattern.label}",
                        reason=f"Matched the advanced safe pattern `{pattern.label}`.",
                        action=pattern.action,
                        confidence="custom",
                        heuristic=False,
                        match_class="advanced_pattern",
                    )
                )
        return self._dedupe_matches(matches)

    def _dedupe_matches(self, matches: list[ShieldMatch]) -> list[ShieldMatch]:
        seen: set[tuple[str, str, str]] = set()
        output: list[ShieldMatch] = []
        for item in matches:
            key = (item.pack, item.label, item.match_class)
            if key in seen:
                continue
            seen.add(key)
            output.append(item)
        return output

    def _record_strike(self, guild_id: int, user_id: int, pack: str, compiled: CompiledShieldConfig, now: float) -> int:
        key = (guild_id, user_id, pack)
        window_seconds = compiled.escalation_window_minutes * 60.0
        hits = [value for value in self._strike_windows.get(key, []) if now - value <= window_seconds]
        hits.append(now)
        self._strike_windows[key] = hits
        return len(hits)

    def _prune_runtime_state(self, now: float):
        if now - self._last_runtime_prune < RUNTIME_PRUNE_INTERVAL_SECONDS:
            return
        self._last_runtime_prune = now
        self._alert_dedup = {key: value for key, value in self._alert_dedup.items() if now - value <= ALERT_DEDUP_SECONDS}
        self._recent_promos = {
            key: [value for value in values if now - value <= REPETITION_WINDOW_SECONDS]
            for key, values in self._recent_promos.items()
            if any(now - value <= REPETITION_WINDOW_SECONDS for value in values)
        }
        max_window_seconds = max([compiled.escalation_window_minutes * 60.0 for compiled in self._compiled_configs.values()] or [15 * 60.0])
        self._strike_windows = {
            key: [value for value in values if now - value <= max_window_seconds]
            for key, values in self._strike_windows.items()
            if any(now - value <= max_window_seconds for value in values)
        }
        self.link_safety.prune(now)

    async def _delete_message(self, message: discord.Message) -> bool:
        me = self._guild_member(message.guild, getattr(self.bot, "user", None))
        if me is None:
            return False
        permissions = message.channel.permissions_for(me)
        if not permissions.manage_messages:
            return False
        with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
            await message.delete()
            return True
        return False

    async def _timeout_member(self, message: discord.Message, compiled: CompiledShieldConfig, *, reason: str) -> bool:
        member = message.author if isinstance(message.author, discord.Member) else None
        me = self._guild_member(message.guild, getattr(self.bot, "user", None))
        if member is None or me is None:
            return False
        permissions = message.channel.permissions_for(me)
        if not permissions.moderate_members:
            return False
        if member.guild_permissions.administrator:
            return False
        if getattr(member, "top_role", None) is not None and getattr(me, "top_role", None) is not None:
            if member.top_role >= me.top_role:
                return False
        until = ge.now_utc() + timedelta(minutes=compiled.timeout_minutes)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await member.timeout(until, reason=reason)
            return True
        return False

    async def _send_alert(self, message: discord.Message, compiled: CompiledShieldConfig, decision: ShieldDecision):
        if compiled.log_channel_id is None:
            return
        dedupe_key = (message.guild.id, message.id)
        now = asyncio.get_running_loop().time()
        if now - self._alert_dedup.get(dedupe_key, 0.0) < ALERT_DEDUP_SECONDS:
            return
        self._alert_dedup[dedupe_key] = now

        channel = self.bot.get_channel(compiled.log_channel_id)
        if channel is None and hasattr(self.bot, "fetch_channel"):
            with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                channel = await self.bot.fetch_channel(compiled.log_channel_id)
        if channel is None:
            return

        preview = make_message_preview(message.content, attachments=message.attachments, limit=MAX_MESSAGE_PREVIEW)
        attachment_summary = make_attachment_labels(message, include_urls=False)
        top_reason = decision.reasons[0] if decision.reasons else None
        alert_title = f"Shield Alert | {PACK_LABELS.get(decision.pack or '', 'Shield')}"
        embed = discord.Embed(
            title=alert_title,
            description=f"{message.author.mention} in {message.channel.mention}",
            color=ge.EMBED_THEME["danger"] if decision.deleted or decision.timed_out else ge.EMBED_THEME["warning"],
        )
        if top_reason is not None:
            embed.add_field(
                name="Detection",
                value=(
                    f"**{top_reason.label}**\n"
                    f"Pack: {PACK_LABELS.get(top_reason.pack, top_reason.pack.title())}\n"
                    f"Class: {_match_class_label(top_reason.match_class)}\n"
                    f"Confidence: {top_reason.confidence.title()}\n"
                    f"Resolved action: {ACTION_LABELS.get(decision.action, decision.action)}"
                ),
                inline=False,
            )
        embed.add_field(name="Action", value=self._format_action_summary(decision), inline=False)
        embed.add_field(name="Reason", value="\n".join(f"- {item.reason}" for item in decision.reasons[:3]), inline=False)
        embed.add_field(name="Preview", value=preview or "[no text content]", inline=False)
        if attachment_summary:
            embed.add_field(name="Attachments", value="\n".join(attachment_summary[:4]), inline=False)
        if decision.link_assessments and top_reason is not None and top_reason.pack in {"scam", "adult"}:
            embed.add_field(
                name="Link Safety",
                value="\n".join(
                    f"`{item.normalized_domain}` | {_link_assessment_summary(item)}"
                    for item in decision.link_assessments[:3]
                ),
                inline=False,
            )
        if decision.ai_review is not None:
            ai_review = decision.ai_review
            ai_lines = [
                f"Classification: **{ai_review.classification_label}**",
                f"Confidence: {ai_review.confidence.title()}",
                f"Priority: {AI_PRIORITY_LABELS.get(ai_review.priority, ai_review.priority.title())}",
            ]
            if ai_review.false_positive:
                ai_lines.append("Possible false positive: Yes")
            ai_lines.append(ai_review.explanation)
            ai_lines.append(f"Model: `{ai_review.model}`")
            embed.add_field(name="AI Assist", value="\n".join(ai_lines), inline=False)
        embed.add_field(name="Jump", value=f"[Open message]({message.jump_url})", inline=True)
        if top_reason is not None and top_reason.pack == "scam" and top_reason.heuristic:
            embed.add_field(name="Note", value="Scam detection is heuristic and experimental.", inline=True)
        if decision.action_note:
            embed.add_field(name="Operational Note", value=decision.action_note, inline=False)
        ge.style_embed(embed, footer="Babblebox Shield | No message archive is stored")

        content = f"<@&{compiled.alert_role_id}>" if compiled.alert_role_id is not None else None
        allowed_mentions = discord.AllowedMentions(users=False, roles=True, everyone=False)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)
            decision.logged = True

    def _format_action_summary(self, decision: ShieldDecision) -> str:
        parts = [ACTION_LABELS.get(decision.action, decision.action)]
        if decision.deleted:
            parts.append("Message deleted")
        elif decision.action.startswith("delete"):
            parts.append("Delete not performed")
        if decision.timed_out:
            parts.append("Member timed out")
        elif decision.action in {"timeout_log", "delete_escalate"} and decision.action_note:
            parts.append("Timeout not performed")
        return " | ".join(parts)

    def _guild_member(self, guild: discord.Guild, bot_user: Any) -> Any:
        if guild is None or bot_user is None:
            return None
        me = getattr(guild, "me", None)
        if me is not None:
            return me
        get_member = getattr(guild, "get_member", None)
        if callable(get_member):
            return get_member(getattr(bot_user, "id", 0))
        return None

    def _compile_config(self, guild_id: int, raw: dict[str, Any]) -> CompiledShieldConfig:
        custom_patterns: list[CompiledCustomPattern] = []
        for item in raw.get("custom_patterns", []):
            if not isinstance(item, dict) or not item.get("enabled", True):
                continue
            mode = str(item.get("mode", "contains")).strip().lower()
            pattern = normalize_plain_text(str(item.get("pattern", ""))).casefold()
            if mode not in CUSTOM_PATTERN_MODES or not pattern:
                continue
            word_re = None
            wildcard_tokens: tuple[str, ...] = ()
            if mode == "word":
                word_re = re.compile(rf"(?<!\w){re.escape(pattern)}(?!\w)", re.IGNORECASE)
            elif mode == "wildcard":
                wildcard_tokens = tuple(token for token in pattern.split("*") if token)
            custom_patterns.append(
                CompiledCustomPattern(
                    pattern_id=str(item.get("pattern_id", "")).strip(),
                    label=normalize_plain_text(str(item.get("label", ""))) or "Custom pattern",
                    pattern=pattern,
                    mode=mode,
                    action=str(item.get("action", "log")).strip().lower(),
                    enabled=bool(item.get("enabled", True)),
                    word_re=word_re,
                    wildcard_tokens=wildcard_tokens,
                )
            )

        return CompiledShieldConfig(
            guild_id=guild_id,
            module_enabled=bool(raw.get("module_enabled")),
            log_channel_id=raw.get("log_channel_id") if isinstance(raw.get("log_channel_id"), int) else None,
            alert_role_id=raw.get("alert_role_id") if isinstance(raw.get("alert_role_id"), int) else None,
            scan_mode=raw.get("scan_mode", "all"),
            included_channel_ids=frozenset(_sorted_unique_ints(raw.get("included_channel_ids", []))),
            excluded_channel_ids=frozenset(_sorted_unique_ints(raw.get("excluded_channel_ids", []))),
            included_user_ids=frozenset(_sorted_unique_ints(raw.get("included_user_ids", []))),
            excluded_user_ids=frozenset(_sorted_unique_ints(raw.get("excluded_user_ids", []))),
            included_role_ids=frozenset(_sorted_unique_ints(raw.get("included_role_ids", []))),
            excluded_role_ids=frozenset(_sorted_unique_ints(raw.get("excluded_role_ids", []))),
            trusted_role_ids=frozenset(_sorted_unique_ints(raw.get("trusted_role_ids", []))),
            allow_domains=frozenset(_sorted_unique_text(raw.get("allow_domains", []))),
            allow_invite_codes=frozenset(_sorted_unique_text(raw.get("allow_invite_codes", []))),
            allow_phrases=tuple(_sorted_unique_text(raw.get("allow_phrases", []))),
            privacy=PackSettings(
                enabled=bool(raw.get("privacy_enabled")),
                low_action=str(raw.get("privacy_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("privacy_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("privacy_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("privacy_sensitivity", "normal")).strip().lower(),
            ),
            promo=PackSettings(
                enabled=bool(raw.get("promo_enabled")),
                low_action=str(raw.get("promo_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("promo_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("promo_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("promo_sensitivity", "normal")).strip().lower(),
            ),
            scam=PackSettings(
                enabled=bool(raw.get("scam_enabled")),
                low_action=str(raw.get("scam_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("scam_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("scam_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("scam_sensitivity", "normal")).strip().lower(),
            ),
            adult=PackSettings(
                enabled=bool(raw.get("adult_enabled")),
                low_action=str(raw.get("adult_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("adult_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("adult_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("adult_sensitivity", "normal")).strip().lower(),
            ),
            ai_enabled=bool(raw.get("ai_enabled")),
            ai_min_confidence=(
                str(raw.get("ai_min_confidence", "high")).strip().lower()
                if str(raw.get("ai_min_confidence", "high")).strip().lower() in SHIELD_AI_MIN_CONFIDENCE_CHOICES
                else "high"
            ),
            ai_enabled_packs=frozenset(
                pack
                for pack in _sorted_unique_text(raw.get("ai_enabled_packs", list(SHIELD_AI_REVIEW_PACKS)))
                if pack in AI_REVIEW_PACK_SET
            ),
            escalation_threshold=int(raw.get("escalation_threshold", 3)),
            escalation_window_minutes=int(raw.get("escalation_window_minutes", 15)),
            timeout_minutes=int(raw.get("timeout_minutes", 10)),
            custom_patterns=tuple(custom_patterns[:CUSTOM_PATTERN_LIMIT]),
        )

    def _rebuild_config_cache(self):
        self._compiled_configs = {}
        for guild_id_text, raw in self.store.state.get("guilds", {}).items():
            try:
                guild_id = int(guild_id_text)
            except (TypeError, ValueError):
                continue
            if not isinstance(raw, dict):
                continue
            self._compiled_configs[guild_id] = self._compile_config(guild_id, self.get_config(guild_id))

    async def _update_config(
        self,
        guild_id: int,
        mutator,
        *,
        success_message: str,
    ) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        async with self._lock:
            guilds = self.store.state.setdefault("guilds", {})
            key = str(guild_id)
            config = self.get_config(guild_id)
            try:
                mutator(config)
            except ValueError as exc:
                return False, str(exc)
            guilds[key] = config
            flushed = await self.store.flush()
            if not flushed:
                return False, "Shield could not save that configuration change."
            self._compiled_configs[guild_id] = self._compile_config(guild_id, config)
        return True, success_message

    def _normalize_domain(self, raw_value: str) -> tuple[bool, str]:
        cleaned = normalize_plain_text(raw_value).casefold()
        if not cleaned:
            return False, "Provide a domain to allowlist."
        if "/" in cleaned or "://" in cleaned:
            cleaned = _extract_domain(cleaned) or ""
        if cleaned.startswith("www."):
            cleaned = cleaned[4:]
        if not re.fullmatch(r"[a-z0-9.-]+\.[a-z]{2,}", cleaned):
            return False, "Allowlisted domains must look like `example.com`."
        return True, cleaned

    def _normalize_invite_code(self, raw_value: str) -> tuple[bool, str]:
        cleaned = normalize_plain_text(raw_value).casefold()
        if not cleaned:
            return False, "Provide an invite code or invite URL to allowlist."
        match = INVITE_RE.search(cleaned)
        if match:
            return True, match.group(1).casefold()
        if not re.fullmatch(r"[a-z0-9-]{2,32}", cleaned):
            return False, "Invite allowlist entries must be an invite code or Discord invite URL."
        return True, cleaned

    def _normalize_allow_phrase(self, raw_value: str) -> tuple[bool, str]:
        ok, cleaned_or_error = sanitize_short_plain_text(
            raw_value,
            field_name="Allowlisted phrase",
            max_length=ALLOW_PHRASE_MAX_LEN,
            sentence_limit=1,
            reject_blocklist=False,
            allow_empty=False,
        )
        if not ok:
            return False, cleaned_or_error
        return True, cleaned_or_error.casefold()

    def _validate_custom_pattern(
        self,
        *,
        label: str,
        pattern: str,
        mode: str,
        action: str,
    ) -> tuple[bool, dict[str, Any] | str]:
        cleaned_mode = str(mode).strip().lower()
        cleaned_action = str(action).strip().lower()
        if cleaned_mode not in CUSTOM_PATTERN_MODES:
            return False, "Advanced patterns only support `contains`, `word`, or `wildcard`."
        if cleaned_action not in SHIELD_ACTIONS - {"disabled"}:
            return False, "Advanced patterns can use detect, log, delete_log, delete_escalate, or timeout_log."

        ok, clean_label = sanitize_short_plain_text(
            label,
            field_name="Pattern label",
            max_length=CUSTOM_PATTERN_LABEL_MAX_LEN,
            sentence_limit=1,
            reject_blocklist=False,
            allow_empty=False,
        )
        if not ok:
            return False, clean_label

        cleaned_pattern = normalize_plain_text(pattern).casefold()
        if not cleaned_pattern:
            return False, "Pattern text cannot be empty."
        if len(cleaned_pattern) > CUSTOM_PATTERN_MAX_LEN:
            return False, f"Pattern text must be {CUSTOM_PATTERN_MAX_LEN} characters or fewer."
        if "http://" in cleaned_pattern or "https://" in cleaned_pattern:
            return False, "Advanced patterns are for safe text matching, not raw regex or URL payloads."
        if cleaned_mode == "wildcard":
            if cleaned_pattern.count("*") > CUSTOM_PATTERN_WILDCARD_LIMIT:
                return False, f"Wildcard patterns can use up to {CUSTOM_PATTERN_WILDCARD_LIMIT} `*` tokens."
            if not any(token.strip() for token in cleaned_pattern.split("*")):
                return False, "Wildcard patterns must include real text between wildcards."
        if cleaned_mode == "word" and "*" in cleaned_pattern:
            return False, "Whole-word patterns cannot contain `*`."
        payload = {
            "pattern_id": uuid.uuid4().hex[:8],
            "label": clean_label,
            "pattern": cleaned_pattern,
            "mode": cleaned_mode,
            "action": cleaned_action,
            "enabled": True,
        }
        return True, payload
