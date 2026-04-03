from __future__ import annotations

import asyncio
import contextlib
import hashlib
import re
import secrets
import time
from dataclasses import dataclass
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit, urlunsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.confessions_store import (
    ConfessionsStorageUnavailable,
    ConfessionsStore,
    default_confession_config,
    default_enforcement_state,
    normalize_confession_config,
)
from babblebox.shield_link_safety import (
    ADULT_LINK_CATEGORY,
    MALICIOUS_LINK_CATEGORY,
    SAFE_LINK_CATEGORY,
    SHORTENER_DOMAINS,
    STOREFRONT_DOMAINS,
    UNKNOWN_LINK_CATEGORY,
    UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
    ShieldLinkAssessment,
    ShieldLinkSafetyEngine,
    domain_in_set,
)
from babblebox.text_safety import (
    CARD_RE,
    EMAIL_RE,
    IPV4_RE,
    IPV6_RE,
    MENTION_RE,
    PHONE_RE,
    SSN_RE,
    URL_RE,
    normalize_plain_text,
    squash_for_evasion_checks,
)
from babblebox.utility_helpers import (
    deserialize_datetime,
    format_duration_brief,
)


PUBLIC_ID_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CONFESSION_ID_PREFIX = "CF"
CASE_ID_PREFIX = "CS"
MAX_ID_BODY = 8
MAX_CONFESSION_LENGTH = 1800
MAX_STAFF_PREVIEW = 220
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
REVIEW_PREVIEW_LIMIT = 5
REVIEW_QUEUE_DEBOUNCE_SECONDS = 120
EXACT_DUPLICATE_WINDOW_SECONDS = 24 * 3600
NEAR_DUPLICATE_RATIO = 0.92
STRIKE_SUSPEND_HOURS = 24
QUEUE_AGE_NEW_SECONDS = 15 * 60
QUEUE_AGE_RECENT_SECONDS = 2 * 3600
LINK_IN_BIO_DOMAINS = frozenset({"linktr.ee", "beacons.ai", "carrd.co"})
SPAM_RATE_LIMIT_FLAGS = frozenset(
    {"duplicate_spam", "empty_content", "low_signal_spam", "near_duplicate_spam", "repetitive_spam"}
)
RASTER_IMAGE_CONTENT_TYPES = frozenset(
    {"image/png", "image/jpeg", "image/jpg", "image/gif", "image/webp", "image/bmp"}
)
RASTER_IMAGE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"})
TRUSTED_SAFE_FAMILIES = frozenset({"social", "media", "docs", "dev", "wiki"})
TRUSTED_MAINSTREAM_DOMAINS = {
    "google.com",
    "youtube.com",
    "youtu.be",
    "wikipedia.org",
    "github.com",
    "gitlab.com",
    "python.org",
    "docs.python.org",
    "mozilla.org",
}

TOKEN_RE = re.compile(r"[a-z0-9']+")
RAW_MENTION_RE = re.compile(r"(?i)<\s*[@#][!&]?\s*\d+\s*>|@\s*(?:everyone|here)")
LOW_SIGNAL_RE = re.compile(r"(?i)^(?:[a-z0-9]\s*){1,3}$")
REPEATED_CHAR_RE = re.compile(r"(.)\1{7,}")
REPEATED_WORD_RE = re.compile(r"(?i)\b([a-z0-9']{2,})\b(?:\s+\1\b){3,}")
REPORTING_CONTEXT_RE = re.compile(
    r"(?i)\b(?:quote|quoted|quoting|report|reported|reporting|context|example|sample|they said|someone said|called me|called them|for review)\b"
)
EDUCATIONAL_CONTEXT_RE = re.compile(
    r"(?i)\b(?:medical|medicine|doctor|clinic|health|sexual health|biology|education|educational|therapy|consent|pregnancy|assault|prevention|awareness|study|class)\b"
)
TARGETING_RE = re.compile(r"(?i)\b(?:you|your|they|them|he|she|someone|somebody|mods?|admins?)\b")
TARGETED_ACCUSATION_RE = re.compile(
    r"(?i)\b(?:you|they|them|he|she|someone|somebody|mods?|admins?)\b.{0,28}\b"
    r"(?:creep|predator|pervert|abuser|bully|scammer|liar|cheater|racist|groomer|harasser|unsafe|gross|disgusting|toxic)\b"
)
PRESSURE_CAMPAIGN_RE = re.compile(
    r"(?i)\b(?:watch out for|stay away from|don't trust|do not trust|avoid)\b.{0,36}\b"
    r"(?:him|her|them|that person|this person|mods?|admins?)\b"
)
HOST_LABEL_RE = re.compile(r"[a-z0-9-]+")

SEVERE_HATE_TERMS = {
    "chink",
    "coon",
    "dyke",
    "faggot",
    "gook",
    "kike",
    "mongoloid",
    "nigga",
    "nigger",
    "n1gga",
    "n1gger",
    "n1gg3r",
    "n1gg@",
    "paki",
    "spic",
    "tranny",
    "wetback",
}
DEROGATORY_TERMS = {"bitch", "slut", "whore", "retard", "cripple"}
VULGAR_TERMS = {"fuck", "fucked", "fucking", "shit", "dick", "cock", "pussy", "asshole", "motherfucker"}
ADULT_TERMS = {
    "anal",
    "bdsm",
    "blowjob",
    "boob",
    "boobs",
    "breast",
    "breasts",
    "cum",
    "fetish",
    "handjob",
    "horny",
    "kink",
    "masturbate",
    "masturbation",
    "naked",
    "nipple",
    "nipples",
    "nsfw",
    "nude",
    "nudes",
    "onlyfans",
    "orgasm",
    "penis",
    "porn",
    "pornhub",
    "sex",
    "sexual",
    "sext",
    "vagina",
}
STRIKE_FLAGS = {
    "abusive_language",
    "hate_speech",
    "link_unsafe",
    "malicious_link",
    "private_pattern",
    "mention_abuse",
}
STAFF_REASON_LABELS = {
    "abusive_language": "Abusive language",
    "adult_language": "Adult content",
    "adult_language_context": "Adult-content context",
    "adult_link": "Adult domain",
    "duplicate_spam": "Duplicate spam",
    "empty_content": "Empty submission",
    "hate_speech": "Hate speech",
    "hate_speech_context": "Quoted/reporting context",
    "link_unsafe": "Untrusted link",
    "malformed_link": "Malformed link",
    "malicious_link": "Malicious link",
    "mention_abuse": "Mentions",
    "near_duplicate_spam": "Near-duplicate spam",
    "private_pattern": "Private information",
    "targeted_harassment": "Targeted harassment",
    "repetitive_spam": "Repetitive spam",
    "too_long": "Too long",
    "vulgar_language": "Borderline language",
    "vulgar_language_context": "Quoted harsh language",
}
MANUAL_CASE_ACTIONS = {"delete", "clear", "false_positive", "perm_ban", "suspend", "temp_ban"}


@dataclass(frozen=True)
class ConfessionSubmissionResult:
    ok: bool
    state: str
    message: str
    confession_id: str | None = None
    case_id: str | None = None
    flag_codes: tuple[str, ...] = ()
    jump_url: str | None = None


@dataclass(frozen=True)
class SafetyResult:
    outcome: str
    flag_codes: tuple[str, ...]
    strike_worthy: bool
    reason: str
    link_assessments: tuple[ShieldLinkAssessment, ...] = ()


def _sorted_unique_text(values: Iterable[str]) -> list[str]:
    return sorted({normalize_plain_text(value).casefold() for value in values if normalize_plain_text(value)})


def _attachment_kind_meta(item: dict[str, Any]) -> str:
    return str(item.get("kind") or "attachment").casefold()


def _attachment_summary_from_meta(values: Sequence[dict[str, Any]]) -> str | None:
    if not values:
        return None
    image_count = sum(1 for item in values if _attachment_kind_meta(item) == "image")
    file_count = max(0, len(values) - image_count)
    parts = []
    if image_count:
        suffix = "" if image_count == 1 else "s"
        parts.append(f"{image_count} image{suffix}")
    if file_count:
        suffix = "" if file_count == 1 else "s"
        parts.append(f"{file_count} file{suffix}")
    if not parts:
        return None
    summary = " and ".join(parts[:2])
    return f"{summary} attached"


def _attachment_urls(attachments: Sequence[Any]) -> list[str]:
    urls: list[str] = []
    for item in attachments[:3]:
        url = normalize_plain_text(getattr(item, "url", None))
        if url:
            urls.append(url)
    return urls


def _rounded_age_text(iso_value: str | None) -> str:
    created_at = deserialize_datetime(iso_value)
    if created_at is None:
        return "Unknown"
    seconds = int(max(0, (ge.now_utc() - created_at).total_seconds()))
    if seconds < QUEUE_AGE_NEW_SECONDS:
        return "New"
    if seconds < QUEUE_AGE_RECENT_SECONDS:
        return "Recent"
    return "Older"


def _tokens(text: str) -> list[str]:
    return TOKEN_RE.findall(text.casefold())


def _canonical_duplicate_text(text: str, attachment_meta: Sequence[dict[str, Any]], shared_link_url: str | None = None) -> str:
    lowered = normalize_plain_text(text).casefold()
    shared_link = normalize_plain_text(shared_link_url).casefold() if shared_link_url else ""
    attachment_signature = " ".join(str(item.get("kind") or "").casefold() for item in attachment_meta)
    attachment_count = f"attachments:{len(attachment_meta)}" if attachment_meta else ""
    return normalize_plain_text(f"{lowered} {shared_link} {attachment_signature} {attachment_count}").casefold()


def _fingerprint_text(
    text: str,
    attachment_meta: Sequence[dict[str, Any]],
    shared_link_url: str | None = None,
) -> tuple[str | None, str | None]:
    canonical = _canonical_duplicate_text(text, attachment_meta, shared_link_url)
    if not canonical:
        return None, None
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:32]
    similarity = " ".join(_tokens(canonical)[:24])[:160] or canonical[:160]
    return fingerprint, similarity


def _public_id(prefix: str) -> str:
    body = "".join(secrets.choice(PUBLIC_ID_ALPHABET) for _ in range(MAX_ID_BODY))
    return f"{prefix}-{body}"


def _url_candidates(text: str) -> list[str]:
    return [match.group(0) for match in URL_RE.finditer(text)]


def _clean_url_candidate(raw_url: str) -> str | None:
    cleaned = normalize_plain_text(raw_url).strip("()[]<>.,!?\"'")
    if not cleaned:
        return None
    if cleaned.startswith("www."):
        return f"https://{cleaned}"
    if "://" not in cleaned:
        return f"https://{cleaned}"
    return cleaned


def _normalize_link_host(raw_host: str) -> str | None:
    host = normalize_plain_text(raw_host).casefold().strip().strip(".")
    if not host:
        return None
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    if ":" in host:
        host = host.split(":", 1)[0]
    host = host.strip(".")
    if not host:
        return None
    labels = [label for label in host.split(".") if label]
    if len(labels) < 2:
        return None
    for label in labels:
        if label.startswith("-") or label.endswith("-"):
            return None
        if HOST_LABEL_RE.fullmatch(label) is None:
            return None
    return ".".join(labels)


def _normalize_domain_input(raw_value: str) -> tuple[bool, str]:
    cleaned = normalize_plain_text(raw_value).casefold().strip()
    if not cleaned:
        return False, "Provide a domain or full URL."
    if "://" in cleaned:
        try:
            host = _normalize_link_host(urlsplit(cleaned).netloc)
        except ValueError:
            host = None
    else:
        host = _normalize_link_host(cleaned)
    if host is None:
        return False, "That domain is not valid."
    return True, host


def _normalize_shared_link_input(raw_value: str | None) -> tuple[bool, str | None]:
    cleaned = normalize_plain_text(raw_value)
    if not cleaned:
        return True, None
    if len(_url_candidates(cleaned)) > 1:
        return False, "Use one trusted link per confession."
    if any(character.isspace() for character in cleaned):
        return False, "Use one full link in the trusted link field."
    candidate = _clean_url_candidate(cleaned)
    if candidate is None:
        return False, "That link is not valid."
    try:
        parsed = urlsplit(candidate)
    except ValueError:
        return False, "That link is not valid."
    host = _normalize_link_host(parsed.netloc)
    if host is None:
        return False, "That link is not valid."
    normalized = urlunsplit((parsed.scheme or "https", host, parsed.path or "", parsed.query or "", ""))
    return True, normalized


def _staff_preview_text(body: str, attachment_meta: Sequence[dict[str, Any]]) -> str:
    preview = normalize_plain_text(body)
    attachment_summary = _attachment_summary_from_meta(attachment_meta)
    if preview and attachment_summary:
        preview = f"{preview}\nMedia: {attachment_summary}"
    elif not preview:
        preview = attachment_summary or "[quiet confession]"
    return ge.safe_field_text(preview, limit=MAX_STAFF_PREVIEW)


def _staff_reason_labels(flag_codes: Sequence[str]) -> list[str]:
    labels = []
    for code in flag_codes:
        labels.append(STAFF_REASON_LABELS.get(str(code), str(code).replace("_", " ").title()))
    return labels or ["None"]


def _contains_term(term: str, text: str, squashed: str) -> bool:
    if " " in term:
        return term in text or term in squashed
    pattern = rf"\b{re.escape(term)}\b"
    return re.search(pattern, text, re.IGNORECASE) is not None or re.search(pattern, squashed, re.IGNORECASE) is not None


def _term_hits(terms: set[str], text: str, squashed: str) -> list[str]:
    return sorted(term for term in terms if _contains_term(term, text, squashed))


def _is_reporting_or_educational_context(text: str) -> bool:
    return bool(REPORTING_CONTEXT_RE.search(text) or EDUCATIONAL_CONTEXT_RE.search(text))


def _has_targeted_harassment_signal(text: str) -> bool:
    return bool(TARGETED_ACCUSATION_RE.search(text) or PRESSURE_CAMPAIGN_RE.search(text))


def _find_private_leak(text: str, squashed: str) -> str | None:
    checks = (
        ("email addresses", EMAIL_RE),
        ("IP addresses", IPV4_RE),
        ("IP addresses", IPV6_RE),
        ("phone numbers", PHONE_RE),
        ("card-like numbers", CARD_RE),
        ("sensitive ID numbers", SSN_RE),
    )
    for label, pattern in checks:
        if pattern.search(text) or pattern.search(squashed):
            return label
    return None


class ConfessionsService:
    def __init__(self, bot: commands.Bot, store: ConfessionsStore | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = ConfessionsStore()
            except ConfessionsStorageUnavailable as exc:
                print(f"Confessions storage constructor failed: {exc}")
                self.store = ConfessionsStore(backend="memory")
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._lock = asyncio.Lock()
        self.link_safety = ShieldLinkSafetyEngine()
        self._compiled_configs: dict[int, dict[str, Any]] = {}
        self._review_queue_refresh_tasks: dict[int, asyncio.Task[Any]] = {}

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            print(f"Confessions storage unavailable: {self._startup_storage_error}")
            return False
        try:
            await self.store.load()
        except ConfessionsStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            print(f"Confessions storage unavailable: {exc}")
            return False
        self.storage_ready = True
        self.storage_error = None
        await self._rebuild_config_cache()
        return True

    async def close(self):
        for task in list(self._review_queue_refresh_tasks.values()):
            task.cancel()
        for task in list(self._review_queue_refresh_tasks.values()):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._review_queue_refresh_tasks.clear()
        await self.link_safety.close()
        await self.store.close()

    def storage_message(self, feature_name: str = "Confessions") -> str:
        return f"{feature_name} are temporarily unavailable because Babblebox could not reach the confessions database."

    async def _rebuild_config_cache(self):
        self._compiled_configs = {}
        for guild_id, raw in (await self.store.fetch_all_configs()).items():
            self._compiled_configs[guild_id] = self._compile_config(guild_id, raw)

    def _compile_config(self, guild_id: int, raw: Any) -> dict[str, Any]:
        config = normalize_confession_config(guild_id, raw)
        compiled = dict(config)
        compiled["custom_allow_domain_set"] = frozenset(config["custom_allow_domains"])
        compiled["custom_block_domain_set"] = frozenset(config["custom_block_domains"])
        return compiled

    def get_config(self, guild_id: int) -> dict[str, Any]:
        compiled = self._compiled_configs.get(guild_id)
        if compiled is not None:
            config = dict(compiled)
            config.pop("custom_allow_domain_set", None)
            config.pop("custom_block_domain_set", None)
            return config
        return default_confession_config(guild_id)

    def get_compiled_config(self, guild_id: int) -> dict[str, Any]:
        compiled = self._compiled_configs.get(guild_id)
        if compiled is not None:
            return compiled
        compiled = self._compile_config(guild_id, default_confession_config(guild_id))
        self._compiled_configs[guild_id] = compiled
        return compiled

    async def _update_config(self, guild_id: int, mutate) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Confessions")
        async with self._lock:
            config = self.get_config(guild_id)
            try:
                mutate(config)
            except ValueError as exc:
                return False, str(exc)
            normalized = normalize_confession_config(guild_id, config)
            if (
                normalized["enabled"]
                and normalized["review_mode"]
                and normalized["confession_channel_id"] is not None
                and normalized["review_channel_id"] is not None
                and normalized["confession_channel_id"] == normalized["review_channel_id"]
            ):
                return False, "Confession and review channels must be different."
            await self.store.upsert_config(normalized)
            self._compiled_configs[guild_id] = self._compile_config(guild_id, normalized)
        return True, "Confessions settings updated."

    async def configure_guild(
        self,
        guild_id: int,
        *,
        enabled: bool | None = None,
        confession_channel_id: int | None = None,
        panel_channel_id: int | None = None,
        panel_message_id: int | None = None,
        review_channel_id: int | None = None,
        review_mode: bool | None = None,
        block_adult_language: bool | None = None,
        allow_trusted_mainstream_links: bool | None = None,
        allow_images: bool | None = None,
        max_images: int | None = None,
        cooldown_seconds: int | None = None,
        burst_limit: int | None = None,
        burst_window_seconds: int | None = None,
        auto_suspend_hours: int | None = None,
        strike_temp_ban_threshold: int | None = None,
        temp_ban_days: int | None = None,
        strike_perm_ban_threshold: int | None = None,
        clear_confession_channel: bool = False,
        clear_panel: bool = False,
        clear_review_channel: bool = False,
    ) -> tuple[bool, str]:
        def mutate(config: dict[str, Any]):
            if enabled is not None:
                config["enabled"] = bool(enabled)
            if clear_confession_channel:
                config["confession_channel_id"] = None
            elif confession_channel_id is not None:
                config["confession_channel_id"] = confession_channel_id
            if clear_panel:
                config["panel_channel_id"] = None
                config["panel_message_id"] = None
            else:
                if panel_channel_id is not None:
                    config["panel_channel_id"] = panel_channel_id
                if panel_message_id is not None:
                    config["panel_message_id"] = panel_message_id
            if clear_review_channel:
                config["review_channel_id"] = None
            elif review_channel_id is not None:
                config["review_channel_id"] = review_channel_id
            if review_mode is not None:
                config["review_mode"] = bool(review_mode)
            if block_adult_language is not None:
                config["block_adult_language"] = bool(block_adult_language)
            if allow_trusted_mainstream_links is not None:
                config["allow_trusted_mainstream_links"] = bool(allow_trusted_mainstream_links)
            if allow_images is not None:
                config["allow_images"] = bool(allow_images)
            if max_images is not None:
                config["max_images"] = max_images
            if cooldown_seconds is not None:
                config["cooldown_seconds"] = cooldown_seconds
            if burst_limit is not None:
                config["burst_limit"] = burst_limit
            if burst_window_seconds is not None:
                config["burst_window_seconds"] = burst_window_seconds
            if auto_suspend_hours is not None:
                config["auto_suspend_hours"] = auto_suspend_hours
            if strike_temp_ban_threshold is not None:
                config["strike_temp_ban_threshold"] = strike_temp_ban_threshold
            if temp_ban_days is not None:
                config["temp_ban_days"] = temp_ban_days
            if strike_perm_ban_threshold is not None:
                config["strike_perm_ban_threshold"] = strike_perm_ban_threshold

        ok, message = await self._update_config(guild_id, mutate)
        if not ok:
            return False, message
        current = self.get_config(guild_id)
        ready = self.operability_message(guild_id)
        return (
            True,
            (
                f"Confessions are {'enabled' if current['enabled'] else 'disabled'}. "
                f"Review mode is {'on' if current['review_mode'] else 'off'}. {ready}"
            ),
        )

    async def update_panel_record(
        self,
        guild_id: int,
        *,
        channel_id: int | None,
        message_id: int | None,
    ) -> tuple[bool, str]:
        return await self.configure_guild(
            guild_id,
            panel_channel_id=channel_id,
            panel_message_id=message_id,
            clear_panel=channel_id is None or message_id is None,
        )

    async def update_domain_policy(self, guild_id: int, *, bucket: str, domain: str, enabled: bool) -> tuple[bool, str]:
        if bucket not in {"allow", "block"}:
            return False, "Use the allow or block domain list."
        valid, cleaned = _normalize_domain_input(domain)
        if not valid:
            return False, cleaned

        def mutate(config: dict[str, Any]):
            field = "custom_allow_domains" if bucket == "allow" else "custom_block_domains"
            values = set(config.get(field, []))
            if enabled:
                values.add(cleaned)
            else:
                values.discard(cleaned)
            config[field] = sorted(values)

        ok, message = await self._update_config(guild_id, mutate)
        if not ok:
            return False, message
        return True, f"Confessions {bucket}list updated for `{cleaned}`."

    def operability_message(self, guild_id: int) -> str:
        config = self.get_config(guild_id)
        if not config["enabled"]:
            return "Confessions are currently off."
        if config["confession_channel_id"] is None:
            return "Set a confession channel before members can submit."
        if config["review_mode"]:
            if config["review_channel_id"] is None:
                return "Review mode is on, so a review channel is still required."
            if config["review_channel_id"] == config["confession_channel_id"]:
                return "Use different channels for public confessions and private review."
        return "Confessions are ready."

    async def _generate_confession_id(self, guild_id: int) -> str:
        for _ in range(20):
            candidate = _public_id(CONFESSION_ID_PREFIX)
            if await self.store.fetch_submission_by_confession_id(guild_id, candidate) is None:
                return candidate
        raise RuntimeError("Could not allocate a confession ID.")

    async def _generate_case_id(self, guild_id: int) -> str:
        for _ in range(20):
            candidate = _public_id(CASE_ID_PREFIX)
            if await self.store.fetch_case(guild_id, candidate) is None:
                return candidate
        raise RuntimeError("Could not allocate a case ID.")

    async def _enforcement_state(self, guild_id: int, user_id: int) -> dict[str, Any]:
        state = await self.store.fetch_enforcement_state(guild_id, user_id)
        if state is None:
            state = default_enforcement_state(guild_id, user_id)
        return state

    def _normalize_restriction_state(self, state: dict[str, Any]) -> dict[str, Any]:
        now = ge.now_utc()
        updated = dict(state)
        if updated.get("is_permanent_ban"):
            updated["active_restriction"] = "perm_ban"
            updated["restricted_until"] = None
        else:
            restricted_until = deserialize_datetime(updated.get("restricted_until"))
            if restricted_until is not None and restricted_until <= now:
                updated["active_restriction"] = "none"
                updated["restricted_until"] = None
        burst_start = deserialize_datetime(updated.get("burst_window_started_at"))
        if burst_start is not None and (now - burst_start).total_seconds() > 24 * 3600:
            updated["burst_count"] = 0
            updated["burst_window_started_at"] = None
        updated["updated_at"] = now.isoformat()
        return updated

    def _restriction_message(self, state: dict[str, Any]) -> str | None:
        if state.get("is_permanent_ban"):
            return "Babblebox confessions are permanently disabled for you in this server."
        active = str(state.get("active_restriction") or "none")
        if active == "none":
            return None
        until = deserialize_datetime(state.get("restricted_until"))
        if until is not None:
            remaining = int(max(0, (until - ge.now_utc()).total_seconds()))
            return f"Babblebox confessions are temporarily restricted for you for about {format_duration_brief(remaining)}."
        return "Babblebox confessions are temporarily restricted for you."

    def _needs_review(
        self,
        compiled: dict[str, Any],
        *,
        safety: SafetyResult,
        attachment_meta: Sequence[dict[str, Any]],
    ) -> bool:
        return bool(attachment_meta or compiled["review_mode"] or safety.outcome == "review")

    def _has_review_channel(self, guild_id: int) -> bool:
        compiled = self.get_compiled_config(guild_id)
        review_channel_id = compiled.get("review_channel_id")
        return isinstance(review_channel_id, int) and review_channel_id != compiled.get("confession_channel_id")

    def _review_channel_requirement_message(self, *, for_images: bool = False) -> str:
        if for_images:
            return "Image confessions always go through private review, and this server still needs a separate review channel configured."
        return "This confession needs moderator review, but the server has not configured a separate private review channel yet."

    async def _upsert_private_media(self, guild_id: int, submission_id: str, attachments: Sequence[Any], *, now_iso: str):
        urls = _attachment_urls(attachments)
        if urls:
            await self.store.upsert_private_media(
                {
                    "guild_id": guild_id,
                    "submission_id": submission_id,
                    "attachment_urls": urls,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

    async def _scrub_submission_for_terminal_state(self, submission: dict[str, Any]):
        submission["staff_preview"] = None
        submission["content_body"] = None
        submission["shared_link_url"] = None
        submission["content_fingerprint"] = None
        submission["similarity_key"] = None
        submission["attachment_meta"] = []
        await self.store.upsert_submission(submission)
        await self.store.delete_private_media(submission["submission_id"])

    def _attachment_metadata(self, attachments: Sequence[Any]) -> list[dict[str, Any]]:
        return [
            {
                "kind": "image" if self._is_allowed_image(item) else "attachment",
                "size": getattr(item, "size", None),
                "width": getattr(item, "width", None),
                "height": getattr(item, "height", None),
                "spoiler": bool(getattr(item, "is_spoiler", lambda: False)()) if hasattr(item, "is_spoiler") else bool(getattr(item, "spoiler", False)),
            }
            for item in attachments
        ]

    def _is_allowed_image(self, attachment: Any) -> bool:
        content_type = str(getattr(attachment, "content_type", "") or "").casefold()
        filename = str(getattr(attachment, "filename", "") or "").casefold()
        extension_ok = any(filename.endswith(suffix) for suffix in RASTER_IMAGE_EXTENSIONS) if filename else False
        content_type_ok = content_type in RASTER_IMAGE_CONTENT_TYPES if content_type else False
        if content_type and filename:
            return content_type_ok and extension_ok
        return content_type_ok or extension_ok

    def _validate_attachments(self, compiled: dict[str, Any], attachments: Sequence[Any]) -> tuple[bool, str]:
        if not attachments:
            return True, ""
        if not compiled["allow_images"]:
            return False, "Image attachments are currently disabled for confessions in this server."
        if len(attachments) > int(compiled["max_images"]):
            return False, f"You can attach up to {compiled['max_images']} images per confession."
        for attachment in attachments:
            if not self._is_allowed_image(attachment):
                return False, "Confessions only allow images right now."
            size = getattr(attachment, "size", 0)
            if isinstance(size, int) and size > MAX_ATTACHMENT_SIZE:
                return False, "Each confession image must stay under 10 MB."
        return True, ""

    async def _update_rate_limits(
        self,
        compiled: dict[str, Any],
        state: dict[str, Any],
        *,
        case_id: str | None = None,
    ) -> tuple[bool, dict[str, Any], str | None]:
        now = ge.now_utc()
        updated = self._normalize_restriction_state(state)
        cooldown_until = deserialize_datetime(updated.get("cooldown_until"))
        if cooldown_until is not None and cooldown_until > now:
            updated["updated_at"] = now.isoformat()
            await self.store.upsert_enforcement_state(updated)
            return False, updated, f"Please wait about {format_duration_brief(int((cooldown_until - now).total_seconds()))} before sending another confession."

        burst_start = deserialize_datetime(updated.get("burst_window_started_at"))
        if burst_start is None or (now - burst_start).total_seconds() > int(compiled["burst_window_seconds"]):
            updated["burst_window_started_at"] = now.isoformat()
            updated["burst_count"] = 1
        else:
            updated["burst_count"] = int(updated.get("burst_count") or 0) + 1
        if int(updated.get("burst_count") or 0) > int(compiled["burst_limit"]):
            until = now + timedelta(hours=int(compiled["auto_suspend_hours"]))
            updated["active_restriction"] = "suspended"
            updated["restricted_until"] = until.isoformat()
            updated["last_case_id"] = case_id
            updated["updated_at"] = now.isoformat()
            await self.store.upsert_enforcement_state(updated)
            return False, updated, f"Confessions are temporarily suspended for about {format_duration_brief(int((until - now).total_seconds()))} due to rapid repeat submissions."

        updated["cooldown_until"] = (now + timedelta(seconds=int(compiled["cooldown_seconds"]))).isoformat()
        updated["updated_at"] = now.isoformat()
        await self.store.upsert_enforcement_state(updated)
        return True, updated, None

    def _strike_escalation(self, compiled: dict[str, Any], state: dict[str, Any], *, case_id: str | None) -> dict[str, Any]:
        now = ge.now_utc()
        updated = dict(state)
        updated["strike_count"] = int(updated.get("strike_count") or 0) + 1
        updated["last_strike_at"] = now.isoformat()
        updated["last_case_id"] = case_id
        if updated["strike_count"] >= int(compiled["strike_perm_ban_threshold"]):
            updated["is_permanent_ban"] = True
            updated["active_restriction"] = "perm_ban"
            updated["restricted_until"] = None
        elif updated["strike_count"] >= int(compiled["strike_temp_ban_threshold"]):
            updated["active_restriction"] = "temp_ban"
            updated["restricted_until"] = (now + timedelta(days=int(compiled["temp_ban_days"]))).isoformat()
        elif updated["strike_count"] >= 2:
            updated["active_restriction"] = "suspended"
            updated["restricted_until"] = (now + timedelta(hours=STRIKE_SUSPEND_HOURS)).isoformat()
        updated["updated_at"] = now.isoformat()
        return updated

    def _link_domain_allowed(self, compiled: dict[str, Any], assessment: ShieldLinkAssessment, *, domain: str) -> bool:
        if domain_in_set(domain, set(compiled["custom_block_domain_set"])):
            return False
        if domain_in_set(domain, set(compiled["custom_allow_domain_set"])):
            return True
        if not compiled["allow_trusted_mainstream_links"]:
            return False
        if assessment.safe_family in TRUSTED_SAFE_FAMILIES:
            return True
        if domain_in_set(domain, TRUSTED_MAINSTREAM_DOMAINS):
            return True
        return False

    def _assess_links(
        self,
        compiled: dict[str, Any],
        text: str,
        squashed: str,
        attachment_meta: Sequence[dict[str, Any]],
        shared_link_url: str | None = None,
    ) -> tuple[tuple[str, ...], tuple[ShieldLinkAssessment, ...], bool]:
        assessments: list[ShieldLinkAssessment] = []
        flags: list[str] = []
        now = time.monotonic()
        urls = _url_candidates(text)
        if shared_link_url:
            urls.append(shared_link_url)
        for raw_url in urls:
            candidate = _clean_url_candidate(raw_url)
            if candidate is None:
                flags.append("malformed_link")
                continue
            try:
                parsed = urlsplit(candidate)
            except ValueError:
                flags.append("malformed_link")
                continue
            domain = _normalize_link_host(parsed.netloc)
            if domain is None:
                flags.append("malformed_link")
                continue
            if domain_in_set(domain, set(compiled["custom_block_domain_set"])):
                assessments.append(
                    ShieldLinkAssessment(
                        normalized_domain=domain,
                        category=UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
                        matched_signals=("guild_block_domain",),
                        provider_lookup_warranted=False,
                        provider_status="Blocked by guild policy.",
                        intel_version="local",
                    )
                )
                flags.append("link_unsafe")
                continue
            allowlisted = domain_in_set(domain, set(compiled["custom_allow_domain_set"]))
            if not allowlisted and (
                domain_in_set(domain, SHORTENER_DOMAINS)
                or domain_in_set(domain, LINK_IN_BIO_DOMAINS)
                or domain_in_set(domain, STOREFRONT_DOMAINS)
            ):
                assessments.append(
                    ShieldLinkAssessment(
                        normalized_domain=domain,
                        category=UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
                        matched_signals=("category_blocked",),
                        provider_lookup_warranted=False,
                        provider_status="Blocked by confession link policy.",
                        intel_version="local",
                    )
                )
                flags.append("link_unsafe")
                continue
            assessment = self.link_safety.assess_domain(
                domain,
                path=parsed.path or "/",
                query=parsed.query or "",
                message_text=text.casefold(),
                squashed_text=squashed.casefold(),
                has_suspicious_attachment=False,
                allowlisted=allowlisted,
                now=now,
            )
            assessments.append(assessment)
            if self._link_domain_allowed(compiled, assessment, domain=domain):
                continue
            if assessment.category == MALICIOUS_LINK_CATEGORY:
                flags.append("malicious_link")
            elif assessment.category == ADULT_LINK_CATEGORY:
                flags.append("adult_link")
            elif assessment.category in {UNKNOWN_SUSPICIOUS_LINK_CATEGORY, UNKNOWN_LINK_CATEGORY}:
                flags.append("link_unsafe")
        return tuple(_sorted_unique_text(flags)), tuple(assessments), bool(urls)

    def _classify_language(self, compiled: dict[str, Any], text: str, squashed: str) -> SafetyResult | None:
        lowered = text.casefold()
        dampened = _is_reporting_or_educational_context(lowered)
        hate_hits = _term_hits(SEVERE_HATE_TERMS, lowered, squashed)
        adult_hits = _term_hits(ADULT_TERMS, lowered, squashed)
        derog_hits = _term_hits(DEROGATORY_TERMS, lowered, squashed)
        vulgar_hits = _term_hits(VULGAR_TERMS, lowered, squashed)
        targeted = bool(TARGETING_RE.search(lowered))
        harassment_signal = _has_targeted_harassment_signal(lowered)

        if hate_hits:
            if dampened and not targeted and not harassment_signal:
                return None
            if dampened:
                return SafetyResult("review", ("hate_speech_context",), False, "Quoted or reporting context needs review.")
            return SafetyResult("blocked", ("hate_speech",), True, "Severe derogatory language was blocked.")

        if adult_hits and compiled["block_adult_language"]:
            if dampened and not targeted and not harassment_signal:
                return None
            if dampened:
                return SafetyResult("review", ("adult_language_context",), False, "Adult language context needs moderator review.")
            return SafetyResult("blocked", ("adult_language",), False, "Adult or 18+ language is blocked by this server's policy.")

        if derog_hits or vulgar_hits:
            if dampened and not targeted and not harassment_signal:
                return None
            if dampened:
                return SafetyResult("review", ("vulgar_language_context",), False, "Harsh language appeared in a quoted or reporting context.")
            if targeted or len(derog_hits) >= 1 or len(vulgar_hits) >= 2:
                return SafetyResult("blocked", ("abusive_language",), True, "Aggressive or derogatory language was blocked.")
            return SafetyResult("review", ("vulgar_language",), False, "Borderline vulgar language needs moderator review.")
        return None

    async def _evaluate_safety(
        self,
        compiled: dict[str, Any],
        *,
        text: str,
        squashed: str,
        shared_link_url: str | None,
        attachment_meta: Sequence[dict[str, Any]],
        recent_rows: Sequence[dict[str, Any]],
    ) -> SafetyResult:
        total_links = len(_url_candidates(text)) + (1 if shared_link_url else 0)
        if not text and not shared_link_url and not attachment_meta:
            return SafetyResult("blocked", ("empty_content",), False, "Confessions cannot be empty.")
        if total_links > 1:
            return SafetyResult("blocked", ("link_unsafe",), False, "Use one trusted link total per confession.")
        if text and len(re.sub(r"[^a-z0-9]", "", squashed.casefold())) < 3 and not shared_link_url and not attachment_meta:
            return SafetyResult("blocked", ("low_signal_spam",), False, "That confession is too low-signal to post.")
        if text and (LOW_SIGNAL_RE.fullmatch(text) or REPEATED_CHAR_RE.search(text) or REPEATED_WORD_RE.search(text)):
            return SafetyResult("blocked", ("repetitive_spam",), False, "That confession looks spammy or repetitive.")
        if MENTION_RE.search(text) or RAW_MENTION_RE.search(text) or RAW_MENTION_RE.search(squashed):
            return SafetyResult("blocked", ("mention_abuse",), True, "Confessions cannot contain user, role, or mass mentions.")
        private_pattern = _find_private_leak(text, squashed)
        if private_pattern is not None:
            return SafetyResult("blocked", ("private_pattern",), True, f"Confessions cannot contain {private_pattern}.")

        link_flags, assessments, has_links = self._assess_links(compiled, text, squashed, attachment_meta, shared_link_url)
        if link_flags:
            primary = "malicious_link" if "malicious_link" in link_flags else "link_unsafe"
            return SafetyResult("blocked", tuple(link_flags), primary in STRIKE_FLAGS, "That confession contains blocked links.", assessments)
        if has_links and not assessments and not compiled["allow_trusted_mainstream_links"] and not compiled["custom_allow_domains"]:
            return SafetyResult("blocked", ("link_unsafe",), True, "Links are disabled for confessions in this server.")

        fingerprint, similarity_key = _fingerprint_text(text, attachment_meta, shared_link_url)
        now = ge.now_utc()
        for row in recent_rows:
            created_at = deserialize_datetime(row.get("created_at"))
            if created_at is None:
                continue
            age = (now - created_at).total_seconds()
            if fingerprint and row.get("content_fingerprint") == fingerprint and age <= EXACT_DUPLICATE_WINDOW_SECONDS:
                return SafetyResult("blocked", ("duplicate_spam",), False, "That looks like a duplicate confession.")
            previous_similarity = str(row.get("similarity_key") or "")
            if similarity_key and previous_similarity:
                ratio = SequenceMatcher(None, similarity_key, previous_similarity).ratio()
                if ratio >= NEAR_DUPLICATE_RATIO and age <= EXACT_DUPLICATE_WINDOW_SECONDS:
                    return SafetyResult("blocked", ("near_duplicate_spam",), False, "That looks too close to a recent confession.")

        language_result = self._classify_language(compiled, text, squashed)
        if language_result is not None:
            return language_result

        if text and _has_targeted_harassment_signal(text.casefold()):
            return SafetyResult("review", ("targeted_harassment",), False, "Potential targeted harassment needs moderator review.")

        if has_links and assessments:
            allowed_only = all(
                self._link_domain_allowed(compiled, item, domain=item.normalized_domain)
                for item in assessments
            )
            if not allowed_only:
                return SafetyResult("blocked", ("link_unsafe",), True, "That confession contains blocked links.", assessments)

        if text and len(text) > MAX_CONFESSION_LENGTH:
            return SafetyResult("blocked", ("too_long",), False, f"Confessions must stay under {MAX_CONFESSION_LENGTH} characters.")
        return SafetyResult("safe", (), False, "Safe to publish.", assessments)

    async def submit_confession(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        content: str | None,
        link: str | None = None,
        attachments: Sequence[Any] | None = None,
    ) -> ConfessionSubmissionResult:
        if not self.storage_ready:
            return ConfessionSubmissionResult(False, "unavailable", self.storage_message("Confessions"))
        compiled = self.get_compiled_config(guild.id)
        ready_message = self.operability_message(guild.id)
        if ready_message != "Confessions are ready.":
            return ConfessionSubmissionResult(False, "unavailable", ready_message)

        state = self._normalize_restriction_state(await self._enforcement_state(guild.id, author_id))
        restriction_message = self._restriction_message(state)
        if restriction_message is not None:
            await self.store.upsert_enforcement_state(state)
            return ConfessionSubmissionResult(False, "restricted", restriction_message)

        attachment_list = list(attachments or [])
        ok, attachment_message = self._validate_attachments(compiled, attachment_list)
        if not ok:
            return ConfessionSubmissionResult(False, "blocked", attachment_message)

        normalized = normalize_plain_text(content)
        squashed = squash_for_evasion_checks(normalized.casefold())
        link_ok, shared_link_url = _normalize_shared_link_input(link)
        if not link_ok:
            return ConfessionSubmissionResult(False, "blocked", str(shared_link_url or "That link is not valid."))
        attachment_meta = self._attachment_metadata(attachment_list)
        recent_rows = await self.store.list_recent_submissions_for_author(guild.id, author_id, limit=5)
        safety = await self._evaluate_safety(
            compiled,
            text=normalized,
            squashed=squashed,
            shared_link_url=shared_link_url,
            attachment_meta=attachment_meta,
            recent_rows=recent_rows,
        )
        updated_state = state
        confession_id = await self._generate_confession_id(guild.id)
        submission_id = secrets.token_hex(16)
        preview = _staff_preview_text(normalized, attachment_meta)
        fingerprint, similarity_key = _fingerprint_text(normalized, attachment_meta, shared_link_url)
        now = ge.now_utc()
        now_iso = now.isoformat()
        requires_review = self._needs_review(compiled, safety=safety, attachment_meta=attachment_meta)

        submission = {
            "submission_id": submission_id,
            "guild_id": guild.id,
            "confession_id": confession_id,
            "status": "queued" if requires_review else "published",
            "review_status": "pending" if requires_review else "none",
            "staff_preview": preview,
            "content_body": normalized or None,
            "shared_link_url": shared_link_url,
            "content_fingerprint": fingerprint,
            "similarity_key": similarity_key,
            "flag_codes": list(safety.flag_codes),
            "attachment_meta": attachment_meta,
            "posted_channel_id": None,
            "posted_message_id": None,
            "current_case_id": None,
            "created_at": now_iso,
            "published_at": None,
            "resolved_at": None,
        }

        if safety.outcome == "blocked":
            case_id = await self._generate_case_id(guild.id)
            submission["status"] = "blocked"
            submission["review_status"] = "blocked"
            submission["current_case_id"] = case_id
            await self.store.upsert_submission(submission)
            await self._upsert_private_media(guild.id, submission_id, attachment_list, now_iso=now_iso)
            await self.store.upsert_author_link(
                {
                    "submission_id": submission_id,
                    "guild_id": guild.id,
                    "author_user_id": author_id,
                    "created_at": now_iso,
                }
            )
            await self.store.upsert_case(
                {
                    "guild_id": guild.id,
                    "submission_id": submission_id,
                    "confession_id": confession_id,
                    "case_id": case_id,
                    "case_kind": "safety_block",
                    "status": "open",
                    "reason_codes": list(safety.flag_codes),
                    "review_version": 1,
                    "resolution_action": None,
                    "resolution_note": None,
                    "review_message_channel_id": None,
                    "review_message_id": None,
                    "created_at": now_iso,
                    "resolved_at": None,
                }
            )
            if safety.strike_worthy:
                escalated = self._strike_escalation(compiled, updated_state, case_id=case_id)
                await self.store.upsert_enforcement_state(escalated)
            elif set(safety.flag_codes) & SPAM_RATE_LIMIT_FLAGS:
                rate_ok, _, rate_message = await self._update_rate_limits(compiled, state, case_id=case_id)
                if not rate_ok:
                    return ConfessionSubmissionResult(
                        False,
                        "restricted",
                        rate_message or "Confessions are temporarily limited.",
                        confession_id=confession_id,
                        case_id=case_id,
                        flag_codes=safety.flag_codes,
                    )
            return ConfessionSubmissionResult(False, "blocked", safety.reason, confession_id=confession_id, case_id=case_id, flag_codes=safety.flag_codes)

        if attachment_meta and not self._has_review_channel(guild.id):
            return ConfessionSubmissionResult(False, "blocked", self._review_channel_requirement_message(for_images=True))

        if requires_review and not self._has_review_channel(guild.id):
            return ConfessionSubmissionResult(False, "blocked", self._review_channel_requirement_message())

        rate_ok, updated_state, rate_message = await self._update_rate_limits(compiled, state)
        if not rate_ok:
            return ConfessionSubmissionResult(False, "restricted", rate_message or "Confessions are temporarily limited.")

        if requires_review:
            case_id = await self._generate_case_id(guild.id)
            submission["status"] = "queued"
            submission["review_status"] = "pending"
            submission["current_case_id"] = case_id
            await self.store.upsert_submission(submission)
            await self._upsert_private_media(guild.id, submission_id, attachment_list, now_iso=now_iso)
            await self.store.upsert_author_link(
                {
                    "submission_id": submission_id,
                    "guild_id": guild.id,
                    "author_user_id": author_id,
                    "created_at": now_iso,
                }
            )
            await self.store.upsert_case(
                {
                    "guild_id": guild.id,
                    "submission_id": submission_id,
                    "confession_id": confession_id,
                    "case_id": case_id,
                    "case_kind": "review",
                    "status": "open",
                    "reason_codes": list(safety.flag_codes),
                    "review_version": 1,
                    "resolution_action": None,
                    "resolution_note": None,
                    "review_message_channel_id": None,
                    "review_message_id": None,
                    "created_at": now_iso,
                    "resolved_at": None,
                }
            )
            self._schedule_review_queue_refresh(guild.id)
            return ConfessionSubmissionResult(
                True,
                "queued",
                "Your confession was received and queued for anonymous review.",
                confession_id=confession_id,
                case_id=case_id,
                flag_codes=safety.flag_codes,
            )

        publish_ok, publish_message_id, channel_id, publish_message = await self._publish_submission(guild, submission)
        if not publish_ok:
            return ConfessionSubmissionResult(False, "unavailable", publish_message or "Babblebox could not post that confession right now.")
        submission["status"] = "published"
        submission["review_status"] = "none"
        submission["posted_channel_id"] = channel_id
        submission["posted_message_id"] = publish_message_id
        submission["published_at"] = now_iso
        submission["resolved_at"] = now_iso
        await self._scrub_submission_for_terminal_state(submission)
        await self.store.upsert_author_link(
            {
                "submission_id": submission_id,
                "guild_id": guild.id,
                "author_user_id": author_id,
                "created_at": now_iso,
            }
        )
        return ConfessionSubmissionResult(
            True,
            "published",
            "Your anonymous confession was posted.",
            confession_id=confession_id,
            jump_url=self._message_jump_url(guild.id, channel_id, publish_message_id),
        )

    def _message_jump_url(self, guild_id: int, channel_id: int | None, message_id: int | None) -> str | None:
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return None
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

    def _format_channel_label(self, channel_id: int | None) -> str:
        return f"<#{channel_id}>" if isinstance(channel_id, int) else "Not set"

    def _restriction_label(self, state: dict[str, Any]) -> str:
        if state.get("is_permanent_ban"):
            return "Permanent confession ban"
        active = str(state.get("active_restriction") or "none")
        if active == "none":
            return "None"
        until = deserialize_datetime(state.get("restricted_until"))
        if until is not None:
            remaining = int(max(0, (until - ge.now_utc()).total_seconds()))
            if active == "temp_ban":
                return f"Paused for about {format_duration_brief(remaining)}"
            return f"Suspended for about {format_duration_brief(remaining)}"
        return "Temporary restriction"

    def _restriction_origin_labels(self, *, current_case: dict[str, Any] | None, last_case: dict[str, Any] | None) -> tuple[str, str]:
        source_case = last_case or current_case
        if source_case is None:
            return "No active restriction", "No cleared or overridden restriction is on record."
        manual = source_case.get("case_kind") == "published_moderation" or source_case.get("resolution_action") in MANUAL_CASE_ACTIONS
        source = "Manual staff action" if manual else "Automatic safety escalation"
        resolution_action = str(source_case.get("resolution_action") or "").strip().lower()
        if resolution_action in {"clear", "false_positive"}:
            return source, "A prior restriction was cleared or overridden."
        if resolution_action:
            return source, f"Last action: {resolution_action.replace('_', ' ').title()}."
        return source, "No clear or override is recorded."

    def _member_reason_message(self, result: ConfessionSubmissionResult) -> str:
        flags = set(result.flag_codes)
        if result.state == "published":
            return "Your confession was posted without your name attached."
        if result.state == "queued":
            return "Your confession was received and queued for private review."
        if result.state == "restricted":
            return result.message
        if result.state == "unavailable":
            return result.message
        if "adult_language" in flags or "adult_language_context" in flags:
            return "This server blocks adult content in anonymous confessions."
        if "malicious_link" in flags or "adult_link" in flags or "link_unsafe" in flags or "malformed_link" in flags:
            return "That confession includes a link this server does not allow."
        if "mention_abuse" in flags:
            return "Confessions cannot include user, role, channel, or mass mentions."
        if "private_pattern" in flags:
            return "Confessions cannot include private contact or identifying details."
        if "duplicate_spam" in flags or "near_duplicate_spam" in flags or "repetitive_spam" in flags:
            return "That confession is too close to recent submissions."
        if "hate_speech" in flags or "abusive_language" in flags or "vulgar_language" in flags:
            return "That confession includes language this server blocks."
        return result.message

    def build_member_result_embed(self, result: ConfessionSubmissionResult) -> discord.Embed:
        title_map = {
            "published": "Confession Posted",
            "queued": "Confession Received",
            "blocked": "Confession Not Sent",
            "restricted": "Confessions Paused",
            "unavailable": "Confessions Unavailable",
        }
        tone_map = {
            "published": "success",
            "queued": "info",
            "blocked": "warning",
            "restricted": "warning",
            "unavailable": "warning",
        }
        embed = ge.make_status_embed(
            title_map.get(result.state, "Anonymous Confession"),
            self._member_reason_message(result),
            tone=tone_map.get(result.state, "info"),
            footer="Babblebox Confessions",
        )
        if result.confession_id is not None:
            embed.add_field(name="Confession ID", value=f"`{result.confession_id}`", inline=True)
        if result.state == "queued":
            embed.add_field(name="Status", value="Private review", inline=True)
        elif result.state == "published":
            embed.add_field(name="Status", value="Live", inline=True)
        return embed

    def build_member_result_view(self, result: ConfessionSubmissionResult) -> discord.ui.View | None:
        if result.state != "published" or not result.jump_url:
            return None
        view = discord.ui.View(timeout=180)
        view.add_item(discord.ui.Button(label="Open Post", style=discord.ButtonStyle.link, url=result.jump_url))
        return view

    def build_member_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        config = self.get_config(guild.id)
        ready = self.operability_message(guild.id) == "Confessions are ready."
        description = (
            "Share something quietly through a private composer. Babblebox posts without your name, and staff moderate by confession ID only."
            if ready
            else "Anonymous confessions are not ready in this server yet. The panel stays here so admins can finish setup without reposting it."
        )
        embed = discord.Embed(
            title="Anonymous Confessions",
            description=description,
            color=ge.EMBED_THEME["accent"] if ready else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="How It Works",
            value=(
                "Tap **Send Confession**.\n"
                "Add text and at most one trusted link. Images stay optional.\n"
                "If images are enabled, they always go through private review before Babblebox posts them."
            ),
            inline=False,
        )
        embed.add_field(
            name="Server Policy",
            value=(
                f"Review mode: **{'On' if config['review_mode'] else 'Off'}**\n"
                f"Adult content: **{'Blocked' if config['block_adult_language'] else 'Allowed'}**\n"
                f"Trusted links: **{'Allowed' if config['allow_trusted_mainstream_links'] else 'Blocked'}**\n"
                f"Images: **{'Allowed' if config['allow_images'] else 'Blocked'}**"
            ),
            inline=False,
        )
        if not ready:
            embed.add_field(name="Availability", value=self.operability_message(guild.id), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Private composer")

    def build_member_panel_help_embed(self, guild: discord.Guild) -> discord.Embed:
        embed = discord.Embed(
            title="How Anonymous Confessions Work",
            description="Confessions are submitted privately and published by Babblebox, not by you.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Privacy", value="Members and server staff see confession IDs, not the author behind them.", inline=False)
        embed.add_field(
            name="What You Can Add",
            value="Text, one trusted link total, and up to 3 images when this server allows them. Image confessions always enter private review.",
            inline=False,
        )
        embed.add_field(
            name="What Gets Blocked",
            value="Mentions, unsafe links, private details, spam, unsupported image types, and anything this server has disabled.",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Confessions")

    async def build_dashboard_embed(self, guild: discord.Guild, *, section: str = "overview") -> discord.Embed:
        config = self.get_config(guild.id)
        counts = await self.store.fetch_guild_counts(guild.id) if self.storage_ready else {
            "queued_submissions": 0,
            "published_submissions": 0,
            "blocked_submissions": 0,
            "open_cases": 0,
        }
        embed = discord.Embed(
            title="Confessions Control Panel",
            description="Staff work by confession ID and case ID only. Author identity remains bot-private.",
            color=ge.EMBED_THEME["info"],
        )
        if section == "policy":
            embed.add_field(
                name="Safety",
                value=(
                    f"Adult content: **{'Blocked' if config['block_adult_language'] else 'Allowed'}**\n"
                    f"Trusted links: **{'Allowed' if config['allow_trusted_mainstream_links'] else 'Blocked'}**\n"
                    f"Images: **{'Allowed' if config['allow_images'] else 'Blocked'}** (max {config['max_images']}, always reviewed)"
                ),
                inline=False,
            )
            embed.add_field(
                name="Flood Control",
                value=(
                    f"Cooldown: **{format_duration_brief(int(config['cooldown_seconds']))}**\n"
                    f"Burst window: **{config['burst_limit']} in {format_duration_brief(int(config['burst_window_seconds']))}**\n"
                    f"Auto suspend: **{config['auto_suspend_hours']}h**"
                ),
                inline=False,
            )
            embed.add_field(
                name="Domains",
                value=(
                    f"Allowlist: **{len(config['custom_allow_domains'])}** custom\n"
                    f"Blocklist: **{len(config['custom_block_domains'])}** custom"
                ),
                inline=False,
            )
        elif section == "review":
            embed.add_field(
                name="Review",
                value=(
                    f"Review mode: **{'On' if config['review_mode'] else 'Off'}**\n"
                    f"Review channel: {self._format_channel_label(config['review_channel_id'])}\n"
                    f"Open queue: **{counts['open_cases']}** case(s)"
                ),
                inline=False,
            )
            embed.add_field(
                name="Quick Help",
                value="Use `/confessions moderate` with a confession ID or case ID to approve, deny, delete, pause, ban, clear, or mark false positive.",
                inline=False,
            )
        elif section == "launch":
            panel_status = "Published" if config.get("panel_message_id") else "Not published"
            embed.add_field(
                name="Public Panel",
                value=(
                    f"Panel channel: {self._format_channel_label(config.get('panel_channel_id'))}\n"
                    f"Panel status: **{panel_status}**\n"
                    f"Confession channel: {self._format_channel_label(config.get('confession_channel_id'))}"
                ),
                inline=False,
            )
            embed.add_field(
                name="Runtime",
                value=(
                    f"Published: **{counts['published_submissions']}**\n"
                    f"Queued: **{counts['queued_submissions']}**\n"
                    f"Blocked: **{counts['blocked_submissions']}**"
                ),
                inline=False,
            )
            embed.add_field(name="Operability", value=self.operability_message(guild.id), inline=False)
        else:
            embed.add_field(
                name="Overview",
                value=(
                    f"Enabled: **{'Yes' if config['enabled'] else 'No'}**\n"
                    f"Confession channel: {self._format_channel_label(config['confession_channel_id'])}\n"
                    f"Panel channel: {self._format_channel_label(config.get('panel_channel_id'))}\n"
                    f"Review channel: {self._format_channel_label(config['review_channel_id'])}"
                ),
                inline=False,
            )
            embed.add_field(
                name="Live Counts",
                value=(
                    f"Queued: **{counts['queued_submissions']}**\n"
                    f"Published: **{counts['published_submissions']}**\n"
                    f"Blocked: **{counts['blocked_submissions']}**\n"
                    f"Open cases: **{counts['open_cases']}**"
                ),
                inline=False,
            )
            embed.add_field(name="Operability", value=self.operability_message(guild.id), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind moderation")

    async def _build_public_confession_embeds(self, submission: dict[str, Any]) -> list[discord.Embed]:
        embeds: list[discord.Embed] = []
        body = normalize_plain_text(submission.get("content_body"))
        shared_link_url = normalize_plain_text(submission.get("shared_link_url"))
        private_media = await self.store.fetch_private_media(submission["submission_id"])
        attachment_urls = list((private_media or {}).get("attachment_urls") or [])
        main = discord.Embed(
            title=f"Anonymous Confession `{submission['confession_id']}`",
            description=body or "Shared quietly through Babblebox.",
            color=ge.EMBED_THEME["accent"],
        )
        if shared_link_url:
            main.add_field(name="Trusted Link", value=shared_link_url, inline=False)
        attachment_meta = list(submission.get("attachment_meta") or [])
        if attachment_meta:
            main.add_field(name="Images", value=f"{len(attachment_meta[:3])} attached", inline=True)
        embeds.append(ge.style_embed(main, footer="Babblebox Confessions | Anonymous post"))
        for index, url in enumerate(attachment_urls[:3], start=1):
            cleaned_url = normalize_plain_text(url)
            if not cleaned_url:
                continue
            image_embed = discord.Embed(
                title=f"Confession `{submission['confession_id']}`",
                description=f"Image {index}",
                color=ge.EMBED_THEME["info"],
            )
            image_embed.set_image(url=cleaned_url)
            embeds.append(ge.style_embed(image_embed, footer="Babblebox Confessions | Anonymous post"))
        return embeds

    async def _publish_submission(self, guild: discord.Guild, submission: dict[str, Any]) -> tuple[bool, int | None, int | None, str | None]:
        channel_id = submission.get("posted_channel_id") or self.get_config(guild.id).get("confession_channel_id")
        channel = guild.get_channel(int(channel_id)) if isinstance(channel_id, int) else None
        if channel is None:
            channel = self.bot.get_channel(self.get_config(guild.id).get("confession_channel_id"))
        if channel is None:
            return False, None, None, "The confession channel is unavailable."

        embeds = await self._build_public_confession_embeds(submission)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            message = await channel.send(embeds=embeds, allowed_mentions=discord.AllowedMentions.none())
            return True, getattr(message, "id", None), getattr(channel, "id", None), None
        return False, None, None, "Babblebox could not send to the confession channel."

    async def _submission_for_case(self, guild_id: int, case_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        case = await self.store.fetch_case(guild_id, case_id)
        if case is None:
            return None, None
        submission = await self.store.fetch_submission(case["submission_id"])
        return submission, case

    def _safe_case_surface(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "case_id": row["case_id"],
            "confession_id": row["confession_id"],
            "case_kind": row["case_kind"],
            "status": row["status"],
            "review_version": int(row.get("review_version") or 0),
            "preview": row.get("staff_preview") or "[quiet confession]",
            "flag_codes": tuple(row.get("flag_codes") or ()),
            "reason_labels": tuple(_staff_reason_labels(row.get("flag_codes") or ())),
            "attachment_summary": _attachment_summary_from_meta(row.get("attachment_meta", [])),
            "shared_link_url": row.get("shared_link_url"),
            "age": _rounded_age_text(row.get("created_at")),
        }

    async def current_review_target(self, guild_id: int) -> dict[str, Any] | None:
        rows = await self.store.list_review_surfaces(guild_id, limit=1)
        if not rows:
            return None
        return self._safe_case_surface(rows[0])

    async def list_review_targets(self, guild_id: int, *, limit: int = REVIEW_PREVIEW_LIMIT) -> list[dict[str, Any]]:
        return [self._safe_case_surface(record) for record in await self.store.list_review_surfaces(guild_id, limit=limit)]

    def build_review_queue_embed(self, guild: discord.Guild, pending_rows: list[dict[str, Any]], *, note: str | None = None) -> discord.Embed:
        embed = discord.Embed(
            title="Confession Review Queue",
            description="The oldest anonymous case is shown first." if pending_rows else "No anonymous confessions are waiting for review right now.",
            color=ge.EMBED_THEME["warning"],
        )
        embed.add_field(name="Queue Depth", value=f"**{len(pending_rows)}** open review case(s)", inline=False)
        if not pending_rows:
            if note:
                embed.add_field(name="Last Update", value=ge.safe_field_text(note), inline=False)
            return ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind review")
        current = pending_rows[0]
        embed.add_field(name="Current", value=f"Case `{current['case_id']}` | Confession `{current['confession_id']}`", inline=False)
        embed.add_field(name="Age", value=current["age"], inline=True)
        embed.add_field(name="Reasons", value=", ".join(current.get("reason_labels") or ("None",)), inline=True)
        embed.add_field(name="Preview", value=current["preview"], inline=False)
        if current.get("shared_link_url"):
            embed.add_field(name="Link", value=str(current["shared_link_url"]), inline=False)
        if current.get("attachment_summary"):
            embed.add_field(name="Attachments", value=current["attachment_summary"], inline=False)
        backlog = []
        for row in pending_rows[:REVIEW_PREVIEW_LIMIT]:
            backlog.append(f"`{row['case_id']}` / `{row['confession_id']}`")
        if len(pending_rows) > REVIEW_PREVIEW_LIMIT:
            backlog.append(f"... and {len(pending_rows) - REVIEW_PREVIEW_LIMIT} more queued case(s).")
        embed.add_field(name="Backlog", value=ge.safe_field_text("\n".join(backlog), limit=1024), inline=False)
        if note:
            embed.add_field(name="Last Update", value=ge.safe_field_text(note), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind review")

    async def _queue_message(self, channel: Any, *, message_id: int | None):
        if not isinstance(message_id, int):
            return None
        fetch_message = getattr(channel, "fetch_message", None)
        if not callable(fetch_message):
            return None
        with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException, Exception):
            return await fetch_message(message_id)
        return None

    async def _sync_member_panel(self, guild: discord.Guild, *, channel_id: int | None = None) -> tuple[bool, str]:
        config = self.get_config(guild.id)
        target_channel_id = channel_id or config.get("panel_channel_id") or config.get("confession_channel_id")
        if not isinstance(target_channel_id, int):
            return False, "Choose a panel channel before publishing the confession panel."
        channel = guild.get_channel(target_channel_id) or self.bot.get_channel(target_channel_id)
        if channel is None:
            return False, "That panel channel is unavailable."
        previous_channel_id = config.get("panel_channel_id")
        previous_message_id = config.get("panel_message_id")
        if isinstance(previous_channel_id, int) and isinstance(previous_message_id, int) and previous_channel_id != target_channel_id:
            previous_channel = guild.get_channel(previous_channel_id) or self.bot.get_channel(previous_channel_id)
            if previous_channel is not None:
                previous_message = await self._queue_message(previous_channel, message_id=previous_message_id)
                if previous_message is not None:
                    with contextlib.suppress(discord.Forbidden, discord.HTTPException, Exception):
                        await previous_message.delete()
        message = await self._queue_message(channel, message_id=previous_message_id if previous_channel_id == target_channel_id else None)
        embed = self.build_member_panel_embed(guild)
        view = None
        cog = self.bot.get_cog("ConfessionsCog")
        if cog is not None:
            build_view = getattr(cog, "build_member_panel_view", None)
            if callable(build_view):
                view = build_view(guild_id=guild.id)
        if message is None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                message = await channel.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
        else:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await message.edit(embed=embed, view=view)
        if message is None:
            return False, "Babblebox could not publish the confession panel in that channel."
        await self.update_panel_record(guild.id, channel_id=getattr(channel, "id", None), message_id=getattr(message, "id", None))
        if view is not None:
            with contextlib.suppress(Exception):
                self.bot.add_view(view, message_id=message.id)
        return True, f"Confession panel is live in <#{channel.id}>."

    async def resume_member_panels(self):
        for guild_id, config in sorted(self._compiled_configs.items()):
            if not config.get("panel_channel_id") or not config.get("panel_message_id"):
                continue
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            await self._sync_member_panel(guild)

    async def sync_member_panel(self, guild: discord.Guild, *, channel_id: int | None = None) -> tuple[bool, str]:
        return await self._sync_member_panel(guild, channel_id=channel_id)

    def _cancel_review_queue_refresh(self, guild_id: int):
        task = self._review_queue_refresh_tasks.pop(guild_id, None)
        if task is not None:
            task.cancel()

    def _schedule_review_queue_refresh(self, guild_id: int):
        if guild_id in self._review_queue_refresh_tasks:
            return

        async def _runner():
            try:
                await asyncio.sleep(REVIEW_QUEUE_DEBOUNCE_SECONDS)
                self._review_queue_refresh_tasks.pop(guild_id, None)
                guild = self.bot.get_guild(guild_id)
                if guild is not None:
                    await self._sync_review_queue(guild)
            except asyncio.CancelledError:
                raise
            finally:
                self._review_queue_refresh_tasks.pop(guild_id, None)

        self._review_queue_refresh_tasks[guild_id] = asyncio.create_task(_runner())

    async def _retire_review_queue(self, guild: discord.Guild, *, note: str | None = None):
        self._cancel_review_queue_refresh(guild.id)
        record = await self.store.fetch_review_queue(guild.id)
        if record is None:
            return
        channel = guild.get_channel(record.get("channel_id")) or self.bot.get_channel(record.get("channel_id"))
        if channel is not None:
            message = await self._queue_message(channel, message_id=record.get("message_id"))
            if message is not None:
                with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                    await message.edit(embed=self.build_review_queue_embed(guild, [], note=note), view=None)
        await self.store.delete_review_queue(guild.id)

    async def _sync_review_queue(self, guild: discord.Guild, *, note: str | None = None):
        self._cancel_review_queue_refresh(guild.id)
        compiled = self.get_compiled_config(guild.id)
        if not compiled["enabled"] or compiled["review_channel_id"] is None:
            await self._retire_review_queue(guild, note=note or "Confession review is inactive.")
            return
        if compiled["confession_channel_id"] == compiled["review_channel_id"]:
            await self._retire_review_queue(guild, note="Confession review is disabled until the public and private channels differ.")
            return
        pending_rows = await self.list_review_targets(guild.id, limit=25)
        if not pending_rows:
            await self._retire_review_queue(guild, note=note)
            return
        channel = guild.get_channel(compiled["review_channel_id"]) or self.bot.get_channel(compiled["review_channel_id"])
        if channel is None:
            return
        current = pending_rows[0]
        queue_record = await self.store.fetch_review_queue(guild.id)
        view = None
        cog = self.bot.get_cog("ConfessionsCog")
        if cog is not None:
            build_view = getattr(cog, "build_review_view", None)
            if callable(build_view):
                view = build_view(case_id=current["case_id"], version=current["review_version"])
        embed = self.build_review_queue_embed(guild, pending_rows, note=note)
        message = await self._queue_message(channel, message_id=queue_record.get("message_id") if queue_record else None)
        if message is None:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                message = await channel.send(embed=embed, view=view, allowed_mentions=discord.AllowedMentions.none())
        else:
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await message.edit(embed=embed, view=view)
        if message is None:
            return
        if view is not None:
            with contextlib.suppress(Exception):
                self.bot.add_view(view, message_id=message.id)
        await self.store.upsert_review_queue(
            {
                "guild_id": guild.id,
                "channel_id": getattr(channel, "id", None),
                "message_id": getattr(message, "id", None),
                "updated_at": ge.now_utc().isoformat(),
            }
        )

    async def resume_review_queues(self):
        guild_ids = {guild_id for guild_id, config in self._compiled_configs.items() if config["enabled"] and config.get("review_channel_id")}
        for record in await self.store.list_review_queues():
            guild_ids.add(int(record["guild_id"]))
        for guild_id in sorted(guild_ids):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            await self._sync_review_queue(guild)

    async def _detail_payload_for_target(self, guild_id: int, target_id: str) -> tuple[dict[str, Any] | None, str | None]:
        cleaned_target = normalize_plain_text(target_id).upper()
        submission = None
        case = None
        if cleaned_target.startswith(f"{CASE_ID_PREFIX}-"):
            submission, case = await self._submission_for_case(guild_id, cleaned_target)
        elif cleaned_target.startswith(f"{CONFESSION_ID_PREFIX}-"):
            submission = await self.store.fetch_submission_by_confession_id(guild_id, cleaned_target)
            if submission is not None and submission.get("current_case_id"):
                case = await self.store.fetch_case(guild_id, str(submission["current_case_id"]))
        else:
            return None, "Use a confession ID like `CF-XXXXXX` or a case ID like `CS-XXXXXX`."
        if submission is None:
            return None, "That confession record was not found."
        author_link = await self.store.fetch_author_link(submission["submission_id"])
        state = None
        last_case = None
        if author_link is not None:
            state = self._normalize_restriction_state(await self._enforcement_state(guild_id, int(author_link["author_user_id"])))
            if state.get("last_case_id"):
                last_case = await self.store.fetch_case(guild_id, str(state["last_case_id"]))
        return {
            "submission": submission,
            "case": case,
            "state": state,
            "last_case": last_case,
        }, None

    async def build_target_status_embed(self, guild: discord.Guild, target_id: str) -> discord.Embed:
        payload, error = await self._detail_payload_for_target(guild.id, target_id)
        if payload is None:
            return ge.make_status_embed("Confession Detail", error or "That confession record was not found.", tone="warning", footer="Babblebox Confessions")
        submission = payload["submission"]
        case = payload["case"]
        state = payload["state"] or default_enforcement_state(guild.id, 0)
        last_case = payload["last_case"]
        restriction_source, override_note = self._restriction_origin_labels(current_case=case, last_case=last_case)
        identifiers_value = f"Confession: `{submission['confession_id']}`\nCase: `{case['case_id']}`" if case is not None else f"Confession: `{submission['confession_id']}`\nCase: `None`"
        embed = discord.Embed(
            title="Confession Detail",
            description="Anonymous moderation detail. Staff see the confession and case state, never the author.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Identifiers", value=identifiers_value, inline=False)
        embed.add_field(
            name="State",
            value=(
                f"Post state: **{submission.get('status', 'unknown').replace('_', ' ').title()}**\n"
                f"Review state: **{submission.get('review_status', 'none').replace('_', ' ').title()}**\n"
                f"Case kind: **{str(case.get('case_kind') if case else 'none').replace('_', ' ').title()}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Restriction",
            value=(
                f"Current: **{self._restriction_label(state)}**\n"
                f"Source: **{restriction_source}**\n"
                f"{override_note}"
            ),
            inline=False,
        )
        embed.add_field(name="Reasons", value=", ".join(_staff_reason_labels(submission.get("flag_codes") or ())), inline=False)
        preview_value = submission.get("content_body") or submission.get("staff_preview") or "[quiet confession]"
        embed.add_field(name="Preview", value=ge.safe_field_text(preview_value, limit=1024), inline=False)
        if submission.get("shared_link_url"):
            embed.add_field(name="Trusted Link", value=str(submission["shared_link_url"]), inline=False)
        if submission.get("attachment_meta"):
            attachment_summary = _attachment_summary_from_meta(submission.get("attachment_meta", [])) or "Images attached"
            embed.add_field(name="Attachments", value=ge.safe_field_text(attachment_summary, limit=1024), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind detail")

    async def _ensure_published_moderation_case(self, guild_id: int, submission: dict[str, Any]) -> str:
        current_case_id = normalize_plain_text(submission.get("current_case_id")).upper() if submission.get("current_case_id") else None
        if current_case_id:
            current_case = await self.store.fetch_case(guild_id, current_case_id)
            if current_case is not None and current_case.get("case_kind") == "published_moderation" and current_case.get("status") == "open":
                return current_case["case_id"]
        case_id = await self._generate_case_id(guild_id)
        now = ge.now_utc().isoformat()
        await self.store.upsert_case(
            {
                "guild_id": guild_id,
                "submission_id": submission["submission_id"],
                "confession_id": submission["confession_id"],
                "case_id": case_id,
                "case_kind": "published_moderation",
                "status": "open",
                "reason_codes": list(submission.get("flag_codes") or ()),
                "review_version": 1,
                "resolution_action": None,
                "resolution_note": None,
                "review_message_channel_id": None,
                "review_message_id": None,
                "created_at": now,
                "resolved_at": None,
            }
        )
        submission["current_case_id"] = case_id
        await self.store.upsert_submission(submission)
        return case_id

    async def _queue_existing_submission_for_review(self, guild_id: int, submission: dict[str, Any], *, now_iso: str) -> str:
        case_id = await self._generate_case_id(guild_id)
        submission["status"] = "queued"
        submission["review_status"] = "pending"
        submission["current_case_id"] = case_id
        submission["resolved_at"] = None
        submission["published_at"] = None
        submission["posted_channel_id"] = None
        submission["posted_message_id"] = None
        await self.store.upsert_submission(submission)
        await self.store.upsert_case(
            {
                "guild_id": guild_id,
                "submission_id": submission["submission_id"],
                "confession_id": submission["confession_id"],
                "case_id": case_id,
                "case_kind": "review",
                "status": "open",
                "reason_codes": list(submission.get("flag_codes") or ()),
                "review_version": 1,
                "resolution_action": None,
                "resolution_note": None,
                "review_message_channel_id": None,
                "review_message_id": None,
                "created_at": now_iso,
                "resolved_at": None,
            }
        )
        return case_id

    def _relax_case_penalty(self, state: dict[str, Any], *, case_id: str, now_iso: str, clear_strikes: bool = False) -> dict[str, Any]:
        updated = dict(state)
        updated["is_permanent_ban"] = False
        updated["active_restriction"] = "none"
        updated["restricted_until"] = None
        if clear_strikes:
            updated["strike_count"] = 0
        elif updated.get("last_case_id") == case_id and int(updated.get("strike_count") or 0) > 0:
            updated["strike_count"] = max(0, int(updated["strike_count"]) - 1)
        updated["updated_at"] = now_iso
        return updated

    async def handle_case_action(
        self,
        guild: discord.Guild,
        *,
        case_id: str,
        action: str,
        actor: object | None = None,
        version: int | None = None,
        duration_seconds: int | None = None,
        clear_strikes: bool = False,
    ) -> tuple[bool, str]:
        submission, case = await self._submission_for_case(guild.id, case_id)
        if submission is None or case is None:
            return False, "That case no longer exists."
        if case["status"] != "open":
            return False, "That case is already closed."
        if version is not None and int(case.get("review_version") or 0) != int(version):
            return False, "That review view is stale. Refresh the queue message first."
        author_link = await self.store.fetch_author_link(submission["submission_id"])
        if author_link is None:
            return False, "That anonymous mapping is unavailable."
        state = self._normalize_restriction_state(await self._enforcement_state(guild.id, int(author_link["author_user_id"])))
        now = ge.now_utc()
        now_iso = now.isoformat()

        if action in {"approve", "false_positive"} and case.get("case_kind") == "safety_block":
            compiled = self.get_compiled_config(guild.id)
            attachment_meta = list(submission.get("attachment_meta") or [])
            requires_review = bool(attachment_meta or compiled["review_mode"])
            relaxed_state = self._relax_case_penalty(state, case_id=case_id, now_iso=now_iso, clear_strikes=clear_strikes)
            resolution_status = "approved" if action == "approve" else "overridden"
            submission["flag_codes"] = []

            if requires_review:
                if not self._has_review_channel(guild.id):
                    return False, self._review_channel_requirement_message(for_images=bool(attachment_meta))
                new_case_id = await self._queue_existing_submission_for_review(guild.id, submission, now_iso=now_iso)
                case["status"] = "resolved"
                case["resolution_action"] = action
                case["resolved_at"] = now_iso
                await self.store.upsert_case(case)
                await self.store.upsert_enforcement_state(relaxed_state)
                await self._sync_review_queue(guild, note=f"Case `{case_id}` was cleared and moved into review.")
                return True, f"Case `{case_id}` was resolved and confession `{submission['confession_id']}` moved into review as `{new_case_id}`."

            publish_ok, message_id, channel_id, error = await self._publish_submission(guild, submission)
            if not publish_ok:
                return False, error or "Babblebox could not publish that confession."
            submission["status"] = "published"
            submission["review_status"] = resolution_status
            submission["posted_channel_id"] = channel_id
            submission["posted_message_id"] = message_id
            submission["published_at"] = now_iso
            submission["resolved_at"] = now_iso
            await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = action
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            await self.store.upsert_enforcement_state(relaxed_state)
            return True, f"Case `{case_id}` was resolved and confession `{submission['confession_id']}` was published."

        if action == "approve":
            publish_ok, message_id, channel_id, error = await self._publish_submission(guild, submission)
            if not publish_ok:
                return False, error or "Babblebox could not publish that confession."
            submission["status"] = "published"
            submission["review_status"] = "approved"
            submission["posted_channel_id"] = channel_id
            submission["posted_message_id"] = message_id
            submission["published_at"] = now_iso
            submission["resolved_at"] = now_iso
            await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = "approve"
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            if state.get("last_case_id") == case_id and (state.get("is_permanent_ban") or state.get("active_restriction") != "none" or int(state.get("strike_count") or 0) > 0):
                await self.store.upsert_enforcement_state(self._relax_case_penalty(state, case_id=case_id, now_iso=now_iso, clear_strikes=clear_strikes))
            await self._sync_review_queue(guild, note=f"Case `{case_id}` was approved.")
            return True, f"Case `{case_id}` was approved and posted as confession `{submission['confession_id']}`."

        if action == "deny":
            submission["status"] = "denied"
            submission["review_status"] = "denied"
            submission["resolved_at"] = now_iso
            await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = "deny"
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            await self._sync_review_queue(guild, note=f"Case `{case_id}` was denied.")
            return True, f"Case `{case_id}` was denied."

        if action == "delete":
            channel = guild.get_channel(submission.get("posted_channel_id")) or self.bot.get_channel(submission.get("posted_channel_id"))
            if channel is not None:
                message = await self._queue_message(channel, message_id=submission.get("posted_message_id"))
                if message is not None:
                    with contextlib.suppress(discord.Forbidden, discord.HTTPException, Exception):
                        await message.delete()
            prior_status = str(submission.get("status") or "")
            submission["status"] = "deleted" if submission.get("posted_message_id") else ("denied" if prior_status in {"queued", "blocked"} else "deleted")
            if prior_status in {"queued", "blocked"}:
                submission["review_status"] = "denied"
            submission["posted_channel_id"] = None
            submission["posted_message_id"] = None
            submission["resolved_at"] = now_iso
            await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = "delete"
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            await self._sync_review_queue(guild, note=f"Case `{case_id}` was removed from the queue.")
            return True, f"Confession `{submission['confession_id']}` was deleted."

        if action in {"suspend", "temp_ban", "perm_ban", "clear", "false_positive"}:
            if action == "perm_ban":
                state["is_permanent_ban"] = True
                state["active_restriction"] = "perm_ban"
                state["restricted_until"] = None
            elif action == "clear":
                state["is_permanent_ban"] = False
                state["active_restriction"] = "none"
                state["restricted_until"] = None
                if clear_strikes:
                    state["strike_count"] = 0
            elif action == "false_positive":
                state = self._relax_case_penalty(state, case_id=case_id, now_iso=now_iso, clear_strikes=clear_strikes)
            else:
                seconds = duration_seconds
                if seconds is None:
                    seconds = 24 * 3600 if action == "suspend" else int(self.get_config(guild.id)["temp_ban_days"]) * 24 * 3600
                state["is_permanent_ban"] = False
                state["active_restriction"] = "suspended" if action == "suspend" else "temp_ban"
                state["restricted_until"] = (now + timedelta(seconds=seconds)).isoformat()
            state["last_case_id"] = case_id
            state["updated_at"] = now_iso
            await self.store.upsert_enforcement_state(state)
            if submission["status"] in {"queued", "blocked"}:
                submission["status"] = "denied"
                submission["review_status"] = "denied"
                submission["resolved_at"] = now_iso
                await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = action
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            await self._sync_review_queue(guild, note=f"Case `{case_id}` was resolved with `{action}`.")
            action_label = action.replace("_", " ")
            return True, f"Case `{case_id}` was resolved with `{action_label}`."

        return False, "That moderation action is not supported."

    async def handle_staff_action(
        self,
        guild: discord.Guild,
        *,
        target_id: str,
        action: str,
        actor: object | None = None,
        duration_seconds: int | None = None,
        clear_strikes: bool = False,
    ) -> tuple[bool, str]:
        cleaned_target = normalize_plain_text(target_id).upper()
        if cleaned_target.startswith(f"{CASE_ID_PREFIX}-"):
            return await self.handle_case_action(
                guild,
                case_id=cleaned_target,
                action=action,
                actor=actor,
                duration_seconds=duration_seconds,
                clear_strikes=clear_strikes,
            )
        if not cleaned_target.startswith(f"{CONFESSION_ID_PREFIX}-"):
            return False, "Use a confession ID like `CF-XXXXXX` or a case ID like `CS-XXXXXX`."
        submission = await self.store.fetch_submission_by_confession_id(guild.id, cleaned_target)
        if submission is None:
            return False, "That confession ID was not found."
        current_case_id = normalize_plain_text(submission.get("current_case_id")).upper() if submission.get("current_case_id") else None
        if current_case_id:
            current_case = await self.store.fetch_case(guild.id, current_case_id)
            if current_case is not None and current_case.get("status") == "open":
                delegated_action = "deny" if action == "delete" and submission.get("status") == "queued" else action
                return await self.handle_case_action(
                    guild,
                    case_id=current_case_id,
                    action=delegated_action,
                    actor=actor,
                    duration_seconds=duration_seconds,
                    clear_strikes=clear_strikes,
                )
        if action in {"delete", "suspend", "temp_ban", "perm_ban", "clear", "false_positive"}:
            case_id = await self._ensure_published_moderation_case(guild.id, submission)
            return await self.handle_case_action(
                guild,
                case_id=case_id,
                action=action,
                actor=actor,
                duration_seconds=duration_seconds,
                clear_strikes=clear_strikes,
            )
        return False, "Use a case ID for approve or deny actions on queued confessions."

    async def handle_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return
        submission = await self.store.fetch_submission_by_message_id(payload.guild_id, payload.message_id)
        if submission is None:
            return
        submission["status"] = "deleted"
        submission["posted_channel_id"] = None
        submission["posted_message_id"] = None
        submission["resolved_at"] = ge.now_utc().isoformat()
        await self.store.upsert_submission(submission)

    async def build_status_embed(self, guild: discord.Guild) -> discord.Embed:
        return await self.build_dashboard_embed(guild, section="overview")
