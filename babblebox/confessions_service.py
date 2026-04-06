from __future__ import annotations

import asyncio
import contextlib
import hashlib
import re
import secrets
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit, urlunsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.confessions_privacy import (
    build_duplicate_signals,
    fuzzy_signature_ratio,
    legacy_similarity_ratio,
)
from babblebox.confessions_store import (
    ConfessionsStorageUnavailable,
    ConfessionsStore,
    DISCORD_MEDIA_HOSTS,
    PRIVACY_CATEGORY_LABELS,
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
    fold_confusable_text,
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
SUPPORT_TICKET_ID_PREFIX = "CT"
MAX_ID_BODY = 8
MAX_CONFESSION_LENGTH = 1800
MAX_STAFF_PREVIEW = 220
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
REVIEW_PREVIEW_LIMIT = 5
ROLE_PREVIEW_LIMIT = 10
EXACT_DUPLICATE_WINDOW_SECONDS = 24 * 3600
NEAR_DUPLICATE_RATIO = 0.92
FUZZY_DUPLICATE_RATIO = 0.90
STRIKE_SUSPEND_HOURS = 24
QUEUE_AGE_NEW_SECONDS = 15 * 60
QUEUE_AGE_RECENT_SECONDS = 2 * 3600
SUPPORT_RATE_LIMIT_SECONDS = 5 * 60
OWNER_REPLY_NOTIFICATION_COOLDOWN_SECONDS = 10 * 60
OWNER_REPLY_OPPORTUNITY_TTL_SECONDS = 72 * 3600
OWNER_REPLY_INBOX_LIMIT = 5
OWNER_REPLY_PREVIEW_LIMIT = 220
OWNER_REPLY_PATH_COOLDOWN_SECONDS = 20 * 60
OWNER_REPLY_RESPONDER_WINDOW_SECONDS = 24 * 3600
OWNER_REPLY_RESPONDER_CONFESSION_CAP = 3
OWNER_REPLY_RESPONDER_GUILD_CAP = 8
REPLY_FLOW_TO_CONFESSION = "reply_to_confession"
REPLY_FLOW_OWNER_TO_USER = "owner_reply_to_user"
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
MANUAL_CASE_ACTIONS = {"delete", "clear", "false_positive", "perm_ban", "suspend", "temp_ban", "restrict_images"}


@dataclass(frozen=True)
class ConfessionSubmissionResult:
    ok: bool
    state: str
    message: str
    confession_id: str | None = None
    case_id: str | None = None
    flag_codes: tuple[str, ...] = ()
    jump_url: str | None = None
    submission_kind: str = "confession"
    reply_flow: str | None = None
    parent_confession_id: str | None = None


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
        url = _normalize_attachment_url(getattr(item, "url", None))
        if url:
            urls.append(url)
    return urls


def _normalize_attachment_url(raw_url: str | None) -> str | None:
    cleaned = normalize_plain_text(raw_url)
    if not cleaned:
        return None
    try:
        parsed = urlsplit(cleaned)
    except ValueError:
        return None
    host = normalize_plain_text(parsed.netloc).casefold().strip()
    if parsed.scheme != "https" or host not in DISCORD_MEDIA_HOSTS or not normalize_plain_text(parsed.path):
        return None
    return urlunsplit(("https", host, parsed.path, parsed.query or "", ""))


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


def _owner_reply_source_preview(content: str | None, attachments: Sequence[Any] | None = None) -> str:
    preview = normalize_plain_text(content)
    attachment_count = len(list(attachments or []))
    if preview:
        return ge.safe_field_text(preview, limit=OWNER_REPLY_PREVIEW_LIMIT)
    if attachment_count:
        noun = "attachment" if attachment_count == 1 else "attachments"
        return f"[{attachment_count} {noun}]"
    return "[message unavailable]"


def _owner_reply_opportunity_age_text(iso_value: str | None) -> str:
    created_at = deserialize_datetime(iso_value)
    if created_at is None:
        return "Unknown"
    seconds = int(max(0, (ge.now_utc() - created_at).total_seconds()))
    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        minutes = max(1, seconds // 60)
        noun = "minute" if minutes == 1 else "minutes"
        return f"{minutes} {noun} ago"
    hours = max(1, seconds // 3600)
    noun = "hour" if hours == 1 else "hours"
    return f"{hours} {noun} ago"


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
        self._support_rate_limits: dict[tuple[int, int, str], float] = {}
        self._privacy_status_global: dict[str, Any] | None = None

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
        self._privacy_status_global = await self.store.fetch_privacy_status()
        if self._privacy_status_global.get("needs_backfill"):
            print(
                "Confessions privacy warning: hardening is partial. "
                f"Categories: {', '.join(self._privacy_category_labels(self._privacy_status_global))}. "
                "Run `python -m babblebox.confessions_backfill --dry-run` and then "
                "`python -m babblebox.confessions_backfill --apply --batch-size 100`."
            )
        else:
            print("Confessions privacy status: hardening is ready.")
        return True

    async def close(self):
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
        compiled["allowed_role_id_set"] = frozenset(config["allowed_role_ids"])
        compiled["blocked_role_id_set"] = frozenset(config["blocked_role_ids"])
        return compiled

    def get_config(self, guild_id: int) -> dict[str, Any]:
        compiled = self._compiled_configs.get(guild_id)
        if compiled is not None:
            config = dict(compiled)
            config.pop("custom_allow_domain_set", None)
            config.pop("custom_block_domain_set", None)
            config.pop("allowed_role_id_set", None)
            config.pop("blocked_role_id_set", None)
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
            requested_allow_images = bool(config.get("allow_images"))
            requested_allow_replies = bool(config.get("allow_anonymous_replies"))
            requested_owner_reply_review = bool(config.get("owner_reply_review_mode"))
            requested_review_channel_id = config.get("review_channel_id")
            requested_confession_channel_id = config.get("confession_channel_id")
            if requested_allow_images and requested_review_channel_id is None:
                return False, "Image confessions require a separate private review channel before admins can enable them."
            if requested_allow_images and requested_review_channel_id == requested_confession_channel_id:
                return False, "Image confessions require a review channel that is separate from the public confession channel."
            if requested_allow_replies and requested_review_channel_id is None:
                return False, "Anonymous replies require a separate private review channel before admins can enable them."
            if requested_allow_replies and requested_review_channel_id == requested_confession_channel_id:
                return False, "Anonymous replies require a review channel that is separate from the public confession channel."
            if requested_owner_reply_review and requested_review_channel_id is None:
                return False, "Owner-reply review requires a separate private review channel before admins can enable it."
            if requested_owner_reply_review and requested_review_channel_id == requested_confession_channel_id:
                return False, "Owner-reply review requires a review channel that is separate from the public confession channel."
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
        appeals_channel_id: int | None = None,
        review_mode: bool | None = None,
        block_adult_language: bool | None = None,
        allow_trusted_mainstream_links: bool | None = None,
        allowed_role_ids: list[int] | None = None,
        blocked_role_ids: list[int] | None = None,
        allow_images: bool | None = None,
        allow_anonymous_replies: bool | None = None,
        allow_owner_replies: bool | None = None,
        owner_reply_review_mode: bool | None = None,
        allow_self_edit: bool | None = None,
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
        clear_appeals_channel: bool = False,
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
            if clear_appeals_channel:
                config["appeals_channel_id"] = None
            elif appeals_channel_id is not None:
                config["appeals_channel_id"] = appeals_channel_id
            if review_mode is not None:
                config["review_mode"] = bool(review_mode)
            if block_adult_language is not None:
                config["block_adult_language"] = bool(block_adult_language)
            if allow_trusted_mainstream_links is not None:
                config["allow_trusted_mainstream_links"] = bool(allow_trusted_mainstream_links)
            if allowed_role_ids is not None:
                config["allowed_role_ids"] = list(allowed_role_ids)
            if blocked_role_ids is not None:
                config["blocked_role_ids"] = list(blocked_role_ids)
            if allow_images is not None:
                config["allow_images"] = bool(allow_images)
            if allow_anonymous_replies is not None:
                config["allow_anonymous_replies"] = bool(allow_anonymous_replies)
            if allow_owner_replies is not None:
                config["allow_owner_replies"] = bool(allow_owner_replies)
            if owner_reply_review_mode is not None:
                config["owner_reply_review_mode"] = bool(owner_reply_review_mode)
            if allow_self_edit is not None:
                config["allow_self_edit"] = bool(allow_self_edit)
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
        privacy_status = await self._guild_privacy_status(guild_id)
        privacy_message = self._privacy_admin_message(privacy_status, scoped=True)
        return (
            True,
            (
                f"Confessions are {'enabled' if current['enabled'] else 'disabled'}. "
                f"Review mode is {'on' if current['review_mode'] else 'off'}. "
                f"Owner replies are {'on' if current['allow_owner_replies'] else 'off'}, "
                f"owner-reply review is {'on' if current['owner_reply_review_mode'] else 'off'}. "
                f"{ready} {privacy_message}"
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

    async def update_role_policy(self, guild_id: int, *, bucket: str, role_id: int, enabled: bool) -> tuple[bool, str]:
        if bucket not in {"allow", "block"}:
            return False, "Use the allowlist or blacklist role bucket."

        def mutate(config: dict[str, Any]):
            field = "allowed_role_ids" if bucket == "allow" else "blocked_role_ids"
            values = {int(value) for value in config.get(field, []) if isinstance(value, int) and value > 0}
            if enabled:
                values.add(int(role_id))
            else:
                values.discard(int(role_id))
            config[field] = sorted(values)

        ok, message = await self._update_config(guild_id, mutate)
        if not ok:
            return False, message
        action = "added to" if enabled else "removed from"
        return True, f"<@&{role_id}> was {action} the Confessions role {bucket}list."

    async def reset_role_policy(self, guild_id: int, *, target: str) -> tuple[bool, str]:
        if target not in {"allowlist", "blacklist", "all"}:
            return False, "Choose allowlist, blacklist, or all."

        def mutate(config: dict[str, Any]):
            if target in {"allowlist", "all"}:
                config["allowed_role_ids"] = []
            if target in {"blacklist", "all"}:
                config["blocked_role_ids"] = []

        ok, message = await self._update_config(guild_id, mutate)
        if not ok:
            return False, message
        label = "allowlist and blacklist" if target == "all" else target
        return True, f"Confessions role {label} reset."

    def _resolve_submission_member(
        self,
        guild: discord.Guild,
        *,
        author_id: int | None = None,
        member: object | None = None,
    ) -> object | None:
        if member is not None:
            return member
        get_member = getattr(guild, "get_member", None)
        if callable(get_member) and isinstance(author_id, int):
            return get_member(author_id)
        return None

    def _member_role_ids(self, member: object | None) -> set[int]:
        role_ids: set[int] = set()
        for role in getattr(member, "roles", ()) or ():
            role_id = getattr(role, "id", None)
            if isinstance(role_id, int) and role_id > 0:
                role_ids.add(role_id)
        return role_ids

    def _resolve_role_labels(self, guild: discord.Guild, role_ids: Sequence[int]) -> tuple[set[int], list[str], int]:
        active_ids: set[int] = set()
        labels: list[str] = []
        stale_count = 0
        get_role = getattr(guild, "get_role", None)
        for role_id in role_ids:
            if not isinstance(role_id, int) or role_id <= 0:
                continue
            if callable(get_role):
                role = get_role(role_id)
                if role is None:
                    stale_count += 1
                    continue
                label = getattr(role, "mention", None) or f"<@&{role_id}>"
            else:
                label = f"<@&{role_id}>"
            active_ids.add(role_id)
            labels.append(str(label))
        return active_ids, labels, stale_count

    def _role_policy_snapshot(self, guild: discord.Guild) -> dict[str, Any]:
        config = self.get_config(guild.id)
        active_allowed_ids, allow_labels, stale_allowed = self._resolve_role_labels(guild, list(config["allowed_role_ids"]))
        active_blocked_ids, block_labels, stale_blocked = self._resolve_role_labels(guild, list(config["blocked_role_ids"]))
        return {
            "active_allowed_ids": active_allowed_ids,
            "active_blocked_ids": active_blocked_ids,
            "allow_labels": allow_labels,
            "block_labels": block_labels,
            "stale_allowed": stale_allowed,
            "stale_blocked": stale_blocked,
        }

    def _format_role_labels(self, labels: Sequence[str]) -> str:
        if not labels:
            return "None"
        visible = list(labels[:ROLE_PREVIEW_LIMIT])
        overflow = max(0, len(labels) - ROLE_PREVIEW_LIMIT)
        if overflow:
            visible.append(f"(+{overflow} more)")
        return " ".join(visible)

    def _role_policy_rule_text(self) -> str:
        return "Blacklist wins. Non-empty allowlist means allowed roles only. Empty allowlist means everyone except blocked roles."

    def _member_role_access_label(self, guild: discord.Guild) -> str:
        snapshot = self._role_policy_snapshot(guild)
        allowed_count = len(snapshot["active_allowed_ids"])
        blocked_count = len(snapshot["active_blocked_ids"])
        if allowed_count and blocked_count:
            return f"Selected roles only ({allowed_count} active); {blocked_count} blocked role(s) still denied first"
        if allowed_count:
            return f"Selected roles only ({allowed_count} active)"
        if blocked_count:
            return f"Open to everyone except {blocked_count} blocked role(s)"
        return "Open to everyone"

    def member_submission_gate_message(
        self,
        guild: discord.Guild,
        *,
        submission_kind: str = "confession",
        author_id: int | None = None,
        member: object | None = None,
    ) -> str | None:
        snapshot = self._role_policy_snapshot(guild)
        resolved_member = self._resolve_submission_member(guild, author_id=author_id, member=member)
        member_role_ids = self._member_role_ids(resolved_member)
        noun = "anonymous replies" if submission_kind == "reply" else "anonymous confessions"
        if snapshot["active_blocked_ids"] & member_role_ids:
            return f"{noun.capitalize()} are not available for your current role setup in this server."
        if snapshot["active_allowed_ids"] and not (snapshot["active_allowed_ids"] & member_role_ids):
            return f"This server only allows {noun} from selected roles."
        return None

    def build_role_policy_embed(self, guild: discord.Guild) -> discord.Embed:
        snapshot = self._role_policy_snapshot(guild)
        embed = discord.Embed(
            title="Confessions Role Eligibility",
            description="Guild-scoped role controls for who can submit anonymous confessions and replies.",
            color=ge.EMBED_THEME["info"],
        )
        current_lines = [
            f"Allowlist: **{len(snapshot['active_allowed_ids'])}** active",
            self._format_role_labels(snapshot["allow_labels"]),
            f"Blacklist: **{len(snapshot['active_blocked_ids'])}** active",
            self._format_role_labels(snapshot["block_labels"]),
        ]
        if snapshot["stale_allowed"] or snapshot["stale_blocked"]:
            current_lines.append(
                f"Stale configured roles: allowlist **{snapshot['stale_allowed']}**, blacklist **{snapshot['stale_blocked']}**"
            )
        embed.add_field(name="Current", value=ge.safe_field_text("\n".join(current_lines), limit=1024), inline=False)
        embed.add_field(name="Rule", value=self._role_policy_rule_text(), inline=False)
        embed.add_field(
            name="Commands",
            value=(
                "Use `/confessions role allowlist` or `/confessions role blacklist` with `state:on|off`.\n"
                "Use `/confessions role reset` to clear allowlist, blacklist, or both."
            ),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Confessions | Role eligibility")

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

    def _privacy_category_labels(self, status: dict[str, Any] | None) -> list[str]:
        if not isinstance(status, dict):
            return []
        return [PRIVACY_CATEGORY_LABELS.get(name, str(name)) for name in list(status.get("categories") or ())]

    async def _guild_privacy_status(self, guild_id: int) -> dict[str, Any] | None:
        if not self.storage_ready:
            return None
        return await self.store.fetch_privacy_status(guild_id)

    def _privacy_admin_message(self, status: dict[str, Any] | None, *, scoped: bool) -> str:
        if not isinstance(status, dict):
            return "Privacy hardening status is unavailable right now."
        if not status.get("needs_backfill"):
            return "Privacy hardening is ready."
        scope_text = " for this server" if scoped else ""
        categories = "; ".join(self._privacy_category_labels(status))
        return (
            f"Privacy hardening is partial{scope_text}. "
            f"Backfill still needs to rewrite legacy or stale-key Confessions rows. Categories: {categories}."
        )

    def _privacy_dashboard_value(self, status: dict[str, Any] | None) -> str:
        if not isinstance(status, dict):
            return "State: **Unknown**\nStatus could not be loaded."
        if not status.get("needs_backfill"):
            return "State: **Ready**\nBackfill: **Complete for this server**"
        category_lines = "\n".join(f"- {label}" for label in self._privacy_category_labels(status))
        return (
            "State: **Partial**\n"
            "Backfill: **Still needed for this server**\n"
            "Issues:\n"
            f"{category_lines}"
        )

    @staticmethod
    def _matching_fuzzy_duplicate_candidates(duplicate_signals: Any, previous_fuzzy_signature: str) -> list[str]:
        if previous_fuzzy_signature.startswith("fh2:"):
            return [item for item in duplicate_signals.keyed_fuzzy_candidates if str(item).startswith("fh2:")]
        if previous_fuzzy_signature.startswith("fh1:"):
            return [item for item in duplicate_signals.keyed_fuzzy_candidates if str(item).startswith("fh1:")]
        if duplicate_signals.legacy_fuzzy_signature:
            return [duplicate_signals.legacy_fuzzy_signature]
        return []

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
        image_restricted_until = deserialize_datetime(updated.get("image_restricted_until"))
        if image_restricted_until is not None and image_restricted_until <= now:
            updated["image_restriction_active"] = False
            updated["image_restricted_until"] = None
            updated["image_restriction_case_id"] = None
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

    def _image_restriction_message(self, state: dict[str, Any]) -> str | None:
        if not state.get("image_restriction_active"):
            return None
        until = deserialize_datetime(state.get("image_restricted_until"))
        if until is not None:
            remaining = int(max(0, (until - ge.now_utc()).total_seconds()))
            return f"Image attachments are paused for you for about {format_duration_brief(remaining)}."
        return "Image attachments are currently disabled for you in this server."

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

    def _owner_reply_source_fingerprint(self, content: str | None, attachments: Sequence[Any] | None = None) -> str | None:
        normalized = normalize_plain_text(content)
        attachment_meta = self._attachment_metadata(list(attachments or []))
        parts = [normalized or "", str(len(attachment_meta))]
        for item in attachment_meta[:3]:
            parts.append(
                "|".join(
                    [
                        str(item.get("kind") or ""),
                        str(item.get("size") or ""),
                        str(item.get("width") or ""),
                        str(item.get("height") or ""),
                        "1" if item.get("spoiler") else "0",
                    ]
                )
            )
        canonical = "\n".join(parts).strip()
        if not canonical:
            return None
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:48]

    def _owner_reply_response_is_actionable(self, content: str | None) -> bool:
        normalized = normalize_plain_text(content)
        if not normalized:
            return False
        squashed = squash_for_evasion_checks(normalized.casefold())
        alnum_count = len(re.sub(r"[^a-z0-9]", "", squashed))
        if alnum_count < 3:
            return False
        if LOW_SIGNAL_RE.fullmatch(normalized) or REPEATED_CHAR_RE.search(normalized) or REPEATED_WORD_RE.search(normalized):
            return False
        if MENTION_RE.search(normalized) or RAW_MENTION_RE.search(normalized) or RAW_MENTION_RE.search(squashed):
            return False
        stripped_links = re.sub(r"https?://\S+", "", normalized, flags=re.IGNORECASE).strip()
        return bool(stripped_links)

    def _recent_owner_reply_rows_within(
        self,
        rows: Sequence[dict[str, Any]],
        *,
        seconds: int,
        timestamp_field: str,
    ) -> list[dict[str, Any]]:
        now = ge.now_utc()
        filtered: list[dict[str, Any]] = []
        for row in rows:
            timestamp = deserialize_datetime(row.get(timestamp_field))
            if timestamp is None:
                continue
            if (now - timestamp).total_seconds() <= seconds:
                filtered.append(row)
        return filtered

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
            if _normalize_attachment_url(getattr(attachment, "url", None)) is None:
                return False, "Babblebox could not safely accept one of those images."
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
        ignore_existing_cooldown: bool = False,
    ) -> tuple[bool, dict[str, Any], str | None]:
        now = ge.now_utc()
        updated = self._normalize_restriction_state(state)
        cooldown_until = deserialize_datetime(updated.get("cooldown_until"))
        if not ignore_existing_cooldown and cooldown_until is not None and cooldown_until > now:
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
        lowered = fold_confusable_text(text)
        dampened = _is_reporting_or_educational_context(lowered)
        squashed_folded = squash_for_evasion_checks(lowered)
        hate_hits = _term_hits(SEVERE_HATE_TERMS, lowered, squashed_folded)
        adult_hits = _term_hits(ADULT_TERMS, lowered, squashed_folded)
        derog_hits = _term_hits(DEROGATORY_TERMS, lowered, squashed_folded)
        vulgar_hits = _term_hits(VULGAR_TERMS, lowered, squashed_folded)
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

        duplicate_signals = build_duplicate_signals(self.store.privacy, int(compiled["guild_id"]), text, attachment_meta, shared_link_url)
        now = ge.now_utc()
        for row in recent_rows:
            created_at = deserialize_datetime(row.get("created_at"))
            if created_at is None:
                continue
            age = (now - created_at).total_seconds()
            previous_fingerprint = str(row.get("content_fingerprint") or "")
            if age <= EXACT_DUPLICATE_WINDOW_SECONDS and duplicate_signals.exact_hash:
                if previous_fingerprint in duplicate_signals.keyed_exact_candidates:
                    return SafetyResult("blocked", ("duplicate_spam",), False, "That looks like a duplicate confession.")
                if duplicate_signals.legacy_exact_hash and previous_fingerprint == duplicate_signals.legacy_exact_hash:
                    return SafetyResult("blocked", ("duplicate_spam",), False, "That looks like a duplicate confession.")
            previous_fuzzy_signature = str(row.get("fuzzy_signature") or "")
            previous_similarity = str(row.get("similarity_key") or "")
            if age > EXACT_DUPLICATE_WINDOW_SECONDS:
                continue
            ratio: float | None = None
            if previous_fuzzy_signature:
                fuzzy_candidates = self._matching_fuzzy_duplicate_candidates(duplicate_signals, previous_fuzzy_signature)
                if fuzzy_candidates:
                    ratio = max(
                        fuzzy_signature_ratio(
                            privacy=self.store.privacy,
                            left=candidate,
                            right=previous_fuzzy_signature,
                        )
                        for candidate in fuzzy_candidates
                    )
            if ratio is not None:
                if ratio >= FUZZY_DUPLICATE_RATIO:
                    return SafetyResult("blocked", ("near_duplicate_spam",), False, "That looks too close to a recent confession.")
            elif duplicate_signals.legacy_similarity_key and previous_similarity:
                ratio = legacy_similarity_ratio(duplicate_signals.legacy_similarity_key, previous_similarity)
                if ratio >= NEAR_DUPLICATE_RATIO:
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
        member: object | None = None,
        content: str | None,
        link: str | None = None,
        attachments: Sequence[Any] | None = None,
        submission_kind: str = "confession",
        parent_confession_id: str | None = None,
        reply_flow: str | None = None,
        _owner_reply_context: dict[str, Any] | None = None,
    ) -> ConfessionSubmissionResult:
        if not self.storage_ready:
            return ConfessionSubmissionResult(False, "unavailable", self.storage_message("Confessions"))
        compiled = self.get_compiled_config(guild.id)
        ready_message = self.operability_message(guild.id)
        if ready_message != "Confessions are ready.":
            return ConfessionSubmissionResult(False, "unavailable", ready_message)
        submission_kind = normalize_plain_text(submission_kind).casefold() or "confession"
        if submission_kind not in {"confession", "reply"}:
            return ConfessionSubmissionResult(False, "blocked", "That anonymous submission type is not supported.")
        normalized_reply_flow = normalize_plain_text(reply_flow).casefold() if reply_flow else ""
        if submission_kind == "reply":
            normalized_reply_flow = normalized_reply_flow or REPLY_FLOW_TO_CONFESSION
            if normalized_reply_flow not in {REPLY_FLOW_TO_CONFESSION, REPLY_FLOW_OWNER_TO_USER}:
                return ConfessionSubmissionResult(False, "blocked", "That anonymous reply flow is not supported.", submission_kind=submission_kind)
            if normalized_reply_flow == REPLY_FLOW_OWNER_TO_USER and not isinstance(_owner_reply_context, dict):
                return ConfessionSubmissionResult(
                    False,
                    "blocked",
                    "Owner replies can only start from a Babblebox owner-reply opportunity.",
                    submission_kind=submission_kind,
                    reply_flow=normalized_reply_flow,
                )
        else:
            normalized_reply_flow = None
        normalized_parent_confession_id = normalize_plain_text(parent_confession_id).upper() if parent_confession_id else None
        role_gate_message = self.member_submission_gate_message(
            guild,
            submission_kind=submission_kind,
            author_id=author_id,
            member=member,
        )
        if role_gate_message is not None:
            return ConfessionSubmissionResult(
                False,
                "blocked",
                role_gate_message,
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

        state = self._normalize_restriction_state(await self._enforcement_state(guild.id, author_id))
        restriction_message = self._restriction_message(state)
        if restriction_message is not None:
            await self.store.upsert_enforcement_state(state)
            return ConfessionSubmissionResult(False, "restricted", restriction_message, submission_kind=submission_kind, reply_flow=normalized_reply_flow)

        attachment_list = list(attachments or [])
        image_restriction_message = self._image_restriction_message(state)
        if attachment_list and image_restriction_message is not None:
            await self.store.upsert_enforcement_state(state)
            return ConfessionSubmissionResult(False, "blocked", image_restriction_message, submission_kind=submission_kind, reply_flow=normalized_reply_flow)

        if submission_kind == "reply":
            is_owner_reply = normalized_reply_flow == REPLY_FLOW_OWNER_TO_USER
            if not is_owner_reply and not compiled["allow_anonymous_replies"]:
                return ConfessionSubmissionResult(
                    False,
                    "blocked",
                    "Anonymous replies are off by default in this server unless admins explicitly enable them.",
                    submission_kind=submission_kind,
                    reply_flow=normalized_reply_flow,
                )
            if attachment_list:
                return ConfessionSubmissionResult(False, "blocked", "Anonymous replies are text-only right now.", submission_kind=submission_kind, reply_flow=normalized_reply_flow)
            if normalize_plain_text(link):
                return ConfessionSubmissionResult(False, "blocked", "Anonymous replies do not allow links right now.", submission_kind=submission_kind, reply_flow=normalized_reply_flow)
            if is_owner_reply:
                if not compiled.get("allow_owner_replies", True):
                    return ConfessionSubmissionResult(
                        False,
                        "blocked",
                        "Owner replies are currently disabled in this server.",
                        submission_kind=submission_kind,
                        reply_flow=normalized_reply_flow,
                    )
                owner_context = _owner_reply_context or {}
                root_submission = owner_context.get("root_submission")
                referenced_submission = owner_context.get("referenced_submission")
                if not isinstance(root_submission, dict) or not isinstance(referenced_submission, dict):
                    return ConfessionSubmissionResult(
                        False,
                        "blocked",
                        "That owner-reply opportunity is no longer available.",
                        submission_kind=submission_kind,
                        reply_flow=normalized_reply_flow,
                    )
                normalized_parent_confession_id = str(root_submission.get("confession_id") or normalized_parent_confession_id or "")
                owner_reply_generation = 1 if referenced_submission.get("submission_kind") == "confession" else 2
            else:
                owner_reply_generation = None
                if not self._has_review_channel(guild.id):
                    return ConfessionSubmissionResult(False, "blocked", self._review_channel_requirement_message(), submission_kind=submission_kind, reply_flow=normalized_reply_flow)
                if not normalized_parent_confession_id or not normalized_parent_confession_id.startswith(f"{CONFESSION_ID_PREFIX}-"):
                    return ConfessionSubmissionResult(False, "blocked", "Reply to a published confession ID like `CF-XXXXXX`.", submission_kind=submission_kind, reply_flow=normalized_reply_flow)
                parent_submission = await self.store.fetch_submission_by_confession_id(guild.id, normalized_parent_confession_id)
                if parent_submission is None or parent_submission.get("status") != "published":
                    return ConfessionSubmissionResult(False, "blocked", "That confession is not available for anonymous replies.", submission_kind=submission_kind, reply_flow=normalized_reply_flow)
                if parent_submission.get("submission_kind") != "confession":
                    return ConfessionSubmissionResult(False, "blocked", "Anonymous replies only support one level of depth right now.", submission_kind=submission_kind, reply_flow=normalized_reply_flow)
        else:
            owner_reply_generation = None

        ok, attachment_message = self._validate_attachments(compiled, attachment_list)
        if not ok:
            return ConfessionSubmissionResult(False, "blocked", attachment_message, submission_kind=submission_kind, reply_flow=normalized_reply_flow)

        normalized = normalize_plain_text(content)
        squashed = squash_for_evasion_checks(normalized.casefold())
        if submission_kind == "reply":
            link_ok, shared_link_url = True, None
        else:
            link_ok, shared_link_url = _normalize_shared_link_input(link)
        if not link_ok:
            return ConfessionSubmissionResult(False, "blocked", str(shared_link_url or "That link is not valid."), submission_kind=submission_kind, reply_flow=normalized_reply_flow)
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
        duplicate_signals = build_duplicate_signals(
            self.store.privacy,
            guild.id,
            normalized,
            attachment_meta,
            shared_link_url,
        )
        now = ge.now_utc()
        now_iso = now.isoformat()
        if submission_kind == "reply" and normalized_reply_flow == REPLY_FLOW_OWNER_TO_USER and safety.outcome == "review" and not compiled.get("owner_reply_review_mode"):
            return ConfessionSubmissionResult(
                False,
                "blocked",
                "That owner reply needs moderator review, but owner-reply review is off in this server. Edit it and try again, or ask an admin to enable owner-reply review.",
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )
        requires_review = (
            bool(compiled.get("owner_reply_review_mode"))
            if submission_kind == "reply" and normalized_reply_flow == REPLY_FLOW_OWNER_TO_USER
            else submission_kind == "reply" or self._needs_review(compiled, safety=safety, attachment_meta=attachment_meta)
        )
        ignore_existing_cooldown = submission_kind == "reply" and normalized_reply_flow == REPLY_FLOW_OWNER_TO_USER

        submission = {
            "submission_id": submission_id,
            "guild_id": guild.id,
            "confession_id": confession_id,
            "submission_kind": submission_kind,
            "reply_flow": normalized_reply_flow,
            "owner_reply_generation": owner_reply_generation,
            "parent_confession_id": normalized_parent_confession_id,
            "status": "queued" if requires_review else "published",
            "review_status": "pending" if requires_review else "none",
            "staff_preview": preview,
            "content_body": normalized or None,
            "shared_link_url": shared_link_url,
            "content_fingerprint": duplicate_signals.exact_hash,
            "similarity_key": None,
            "fuzzy_signature": duplicate_signals.fuzzy_signature,
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
                rate_ok, _, rate_message = await self._update_rate_limits(
                    compiled,
                    state,
                    case_id=case_id,
                    ignore_existing_cooldown=ignore_existing_cooldown,
                )
                if not rate_ok:
                    return ConfessionSubmissionResult(
                        False,
                        "restricted",
                        rate_message or "Confessions are temporarily limited.",
                        confession_id=confession_id,
                        case_id=case_id,
                        flag_codes=safety.flag_codes,
                        submission_kind=submission_kind,
                        reply_flow=normalized_reply_flow,
                        parent_confession_id=normalized_parent_confession_id,
                    )
            return ConfessionSubmissionResult(
                False,
                "blocked",
                safety.reason,
                confession_id=confession_id,
                case_id=case_id,
                flag_codes=safety.flag_codes,
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

        if attachment_meta and not self._has_review_channel(guild.id):
            return ConfessionSubmissionResult(
                False,
                "blocked",
                self._review_channel_requirement_message(for_images=True),
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

        if requires_review and not self._has_review_channel(guild.id):
            return ConfessionSubmissionResult(
                False,
                "blocked",
                self._review_channel_requirement_message(),
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

        rate_ok, updated_state, rate_message = await self._update_rate_limits(
            compiled,
            state,
            ignore_existing_cooldown=ignore_existing_cooldown,
        )
        if not rate_ok:
            return ConfessionSubmissionResult(
                False,
                "restricted",
                rate_message or "Confessions are temporarily limited.",
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

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
            await self._sync_review_queue(guild, note=f"Case `{case_id}` entered review.")
            return ConfessionSubmissionResult(
                True,
                "queued",
                "Your anonymous reply stays anonymous and may go through private approval before posting."
                if submission_kind == "reply"
                else "Your confession was received and queued for anonymous review.",
                confession_id=confession_id,
                case_id=case_id,
                flag_codes=safety.flag_codes,
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )

        publish_ok, publish_message_id, channel_id, publish_message = await self._publish_submission(guild, submission)
        if not publish_ok:
            return ConfessionSubmissionResult(
                False,
                "unavailable",
                publish_message or "Babblebox could not post that confession right now.",
                submission_kind=submission_kind,
                reply_flow=normalized_reply_flow,
                parent_confession_id=normalized_parent_confession_id,
            )
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
            "Your anonymous reply was posted without your name attached."
            if submission_kind == "reply"
            else "Your anonymous confession was posted.",
            confession_id=confession_id,
            jump_url=self._message_jump_url(guild.id, channel_id, publish_message_id),
            submission_kind=submission_kind,
            reply_flow=normalized_reply_flow,
            parent_confession_id=normalized_parent_confession_id,
        )

    async def submit_owner_reply(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        member: object | None = None,
        opportunity_id: str,
        content: str | None,
    ) -> ConfessionSubmissionResult:
        context, error = await self.get_owner_reply_opportunity_context(
            guild,
            author_id=author_id,
            opportunity_id=opportunity_id,
        )
        parent_confession_id = str((context or {}).get("root_submission", {}).get("confession_id") or "")
        if context is None:
            return ConfessionSubmissionResult(
                False,
                "blocked",
                error or "That owner-reply opportunity is no longer available.",
                submission_kind="reply",
                reply_flow=REPLY_FLOW_OWNER_TO_USER,
                parent_confession_id=parent_confession_id or None,
            )
        claimed = await self.store.claim_owner_reply_opportunity(context["opportunity"]["opportunity_id"])
        if claimed is None:
            return ConfessionSubmissionResult(
                False,
                "blocked",
                "That owner-reply opportunity is no longer available.",
                submission_kind="reply",
                reply_flow=REPLY_FLOW_OWNER_TO_USER,
                parent_confession_id=parent_confession_id or None,
            )
        claimed_context, error = await self._validate_owner_reply_opportunity(
            guild,
            claimed,
            author_id=author_id,
            allow_locked=True,
        )
        if claimed_context is None:
            latest = await self.store.fetch_owner_reply_opportunity(claimed["opportunity_id"])
            if latest is not None and latest.get("status") == "locked":
                await self.store.release_owner_reply_opportunity(claimed["opportunity_id"])
            return ConfessionSubmissionResult(
                False,
                "blocked",
                error or "That owner-reply opportunity is no longer available.",
                submission_kind="reply",
                reply_flow=REPLY_FLOW_OWNER_TO_USER,
                parent_confession_id=parent_confession_id or None,
            )
        try:
            result = await self.submit_confession(
                guild,
                author_id=author_id,
                member=member,
                content=content,
                submission_kind="reply",
                parent_confession_id=claimed_context["root_submission"]["confession_id"],
                reply_flow=REPLY_FLOW_OWNER_TO_USER,
                _owner_reply_context=claimed_context,
            )
        except Exception:
            await self.store.release_owner_reply_opportunity(claimed["opportunity_id"])
            raise
        if result.ok and result.state in {"queued", "published"}:
            await self._mark_owner_reply_opportunity_used_record(claimed_context["opportunity"])
            return result
        latest = await self.store.fetch_owner_reply_opportunity(claimed["opportunity_id"])
        if latest is not None and latest.get("status") == "locked":
            await self.store.release_owner_reply_opportunity(claimed["opportunity_id"])
        return result

    def _message_jump_url(self, guild_id: int, channel_id: int | None, message_id: int | None) -> str | None:
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return None
        return f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

    async def _resolve_public_reply_target(
        self,
        guild_id: int,
        target_submission: dict[str, Any] | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None, int | None]:
        if target_submission is None or target_submission.get("status") != "published":
            return None, None, None
        if target_submission.get("submission_kind") == "confession":
            return target_submission, target_submission, 1
        if (
            target_submission.get("submission_kind") == "reply"
            and target_submission.get("reply_flow") == REPLY_FLOW_OWNER_TO_USER
            and target_submission.get("parent_confession_id")
        ):
            root_submission = await self.store.fetch_submission_by_confession_id(
                guild_id,
                str(target_submission["parent_confession_id"]),
            )
            if (
                root_submission is not None
                and root_submission.get("status") == "published"
                and root_submission.get("submission_kind") == "confession"
                and int(target_submission.get("owner_reply_generation") or 1) == 1
            ):
                return root_submission, target_submission, 2
        return None, None, None

    async def _resolve_dm_recipient(self, guild: discord.Guild, user_id: int) -> object | None:
        recipient = guild.get_member(user_id)
        if recipient is not None:
            return recipient
        get_user = getattr(self.bot, "get_user", None)
        if callable(get_user):
            with contextlib.suppress(Exception):
                recipient = get_user(user_id)
                if recipient is not None:
                    return recipient
        fetch_user = getattr(self.bot, "fetch_user", None)
        if callable(fetch_user):
            with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException, Exception):
                recipient = await fetch_user(user_id)
                if recipient is not None:
                    return recipient
        return None

    async def _expire_owner_reply_opportunity(self, opportunity: dict[str, Any]) -> dict[str, Any]:
        if opportunity.get("status") not in {"pending", "locked"}:
            return opportunity
        updated = dict(opportunity)
        updated["status"] = "expired"
        updated["resolved_at"] = ge.now_utc().isoformat()
        await self.store.upsert_owner_reply_opportunity(updated)
        await self._close_owner_reply_notification_message(
            updated,
            title="Owner Reply Prompt Closed",
            message="That owner-reply opportunity is no longer available.",
        )
        return updated

    async def _dismiss_owner_reply_opportunity_record(self, opportunity: dict[str, Any]) -> dict[str, Any]:
        if opportunity.get("status") != "pending":
            return opportunity
        updated = dict(opportunity)
        updated["status"] = "dismissed"
        updated["resolved_at"] = ge.now_utc().isoformat()
        await self.store.upsert_owner_reply_opportunity(updated)
        await self._close_owner_reply_notification_message(
            updated,
            title="Owner Reply Prompt Closed",
            message="Babblebox dismissed that owner-reply opportunity.",
        )
        return updated

    async def _mark_owner_reply_opportunity_used_record(self, opportunity: dict[str, Any]) -> dict[str, Any]:
        if opportunity.get("status") not in {"pending", "locked"}:
            return opportunity
        updated = dict(opportunity)
        updated["status"] = "used"
        updated["resolved_at"] = ge.now_utc().isoformat()
        await self.store.upsert_owner_reply_opportunity(updated)
        await self._close_owner_reply_notification_message(
            updated,
            title="Owner Reply Prompt Closed",
            message="That owner-reply opportunity was already used.",
        )
        return updated

    async def _update_owner_reply_notification(
        self,
        opportunity: dict[str, Any],
        *,
        status: str,
        notification_channel_id: int | None = None,
        notification_message_id: int | None = None,
        notified_at: str | None = None,
    ) -> dict[str, Any]:
        updated = dict(opportunity)
        updated["notification_status"] = status
        updated["notification_channel_id"] = notification_channel_id
        updated["notification_message_id"] = notification_message_id
        updated["notified_at"] = notified_at
        await self.store.upsert_owner_reply_opportunity(updated)
        return updated

    async def _close_owner_reply_notification_message(
        self,
        opportunity: dict[str, Any],
        *,
        title: str,
        message: str,
    ):
        channel_id = opportunity.get("notification_channel_id")
        message_id = opportunity.get("notification_message_id")
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            fetch_channel = getattr(self.bot, "fetch_channel", None)
            if callable(fetch_channel):
                with contextlib.suppress(discord.NotFound, discord.Forbidden, discord.HTTPException, Exception):
                    channel = await fetch_channel(channel_id)
        if channel is None:
            return
        prompt_message = await self._queue_message(channel, message_id=message_id)
        if prompt_message is None:
            return
        with contextlib.suppress(discord.Forbidden, discord.HTTPException, Exception):
            await prompt_message.edit(
                embed=ge.make_status_embed(title, message, tone="info", footer="Babblebox Confessions"),
                view=None,
            )

    def _owner_reply_notification_is_on_cooldown(self, opportunity_rows: Sequence[dict[str, Any]]) -> bool:
        now = ge.now_utc()
        for row in opportunity_rows:
            if row.get("notification_status") != "sent":
                continue
            notified_at = deserialize_datetime(row.get("notified_at"))
            if notified_at is None:
                continue
            if (now - notified_at).total_seconds() < OWNER_REPLY_NOTIFICATION_COOLDOWN_SECONDS:
                return True
        return False

    async def _send_owner_reply_notification(
        self,
        guild: discord.Guild,
        *,
        owner_user_id: int,
        opportunity: dict[str, Any],
        referenced_submission: dict[str, Any],
    ) -> tuple[bool, int | None, int | None]:
        recipient = await self._resolve_dm_recipient(guild, owner_user_id)
        if recipient is None:
            return False, None, None
        embed = self.build_owner_reply_notification_embed(guild, opportunity, referenced_submission)
        view = None
        cog = self.bot.get_cog("ConfessionsCog")
        if cog is not None:
            build_view = getattr(cog, "build_owner_reply_prompt_view", None)
            if callable(build_view):
                view = build_view()
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            message = await recipient.send(embed=embed, view=view)
            channel_id = getattr(getattr(message, "channel", None), "id", None)
            return True, getattr(message, "id", None), channel_id if isinstance(channel_id, int) else None
        return False, None, None

    async def handle_member_response_message(self, message: discord.Message):
        guild = getattr(message, "guild", None)
        author = getattr(message, "author", None)
        if guild is None or author is None or not self.storage_ready:
            return
        compiled = self.get_compiled_config(guild.id)
        if not compiled["enabled"] or not compiled.get("allow_owner_replies", True):
            return
        if bool(getattr(author, "bot", False)) or getattr(message, "webhook_id", None) is not None:
            return
        reference = getattr(message, "reference", None)
        target_message_id = getattr(reference, "message_id", None)
        if not isinstance(target_message_id, int):
            return
        target_submission = await self.store.fetch_submission_by_message_id(guild.id, target_message_id)
        root_submission, referenced_submission, next_generation = await self._resolve_public_reply_target(guild.id, target_submission)
        if root_submission is None or referenced_submission is None or next_generation is None:
            return
        owner_link = await self.store.fetch_author_link(root_submission["submission_id"])
        owner_user_id = int((owner_link or {}).get("author_user_id") or 0)
        if owner_user_id <= 0 or owner_user_id == int(getattr(author, "id", 0) or 0):
            return
        gate_message = self.member_submission_gate_message(guild, submission_kind="reply", author_id=owner_user_id, member=guild.get_member(owner_user_id))
        if gate_message is not None:
            return
        responder_gate = self.member_submission_gate_message(guild, submission_kind="reply", author_id=int(getattr(author, "id", 0) or 0), member=author)
        if responder_gate is not None:
            return
        responder_state = self._normalize_restriction_state(await self._enforcement_state(guild.id, int(getattr(author, "id", 0) or 0)))
        if self._restriction_message(responder_state) is not None:
            return
        normalized_content = normalize_plain_text(getattr(message, "content", None))
        if not self._owner_reply_response_is_actionable(normalized_content):
            return
        existing = await self.store.fetch_owner_reply_opportunity_by_source_message_id(guild.id, int(message.id))
        if existing is not None:
            return
        source_author_user_id = int(getattr(author, "id", 0) or 0)
        if source_author_user_id <= 0:
            return
        path_rows = await self.store.list_owner_reply_opportunities_for_responder_path(
            guild.id,
            root_submission["submission_id"],
            referenced_submission["submission_id"],
            source_author_user_id,
            limit=10,
        )
        if len(self._recent_owner_reply_rows_within(path_rows, seconds=OWNER_REPLY_RESPONDER_WINDOW_SECONDS, timestamp_field="created_at")) >= OWNER_REPLY_RESPONDER_CONFESSION_CAP:
            return
        guild_rows = await self.store.list_owner_reply_opportunities_for_source_author(guild.id, source_author_user_id, limit=25)
        if len(self._recent_owner_reply_rows_within(guild_rows, seconds=OWNER_REPLY_RESPONDER_WINDOW_SECONDS, timestamp_field="created_at")) >= OWNER_REPLY_RESPONDER_GUILD_CAP:
            return
        for row in path_rows:
            if row.get("status") in {"used", "dismissed"}:
                recent_terminal = self._recent_owner_reply_rows_within(
                    [row],
                    seconds=OWNER_REPLY_PATH_COOLDOWN_SECONDS,
                    timestamp_field="resolved_at",
                )
                if recent_terminal:
                    return
        now_iso = ge.now_utc().isoformat()
        source_preview = _owner_reply_source_preview(getattr(message, "content", None), getattr(message, "attachments", None))
        source_fingerprint = self._owner_reply_source_fingerprint(getattr(message, "content", None), getattr(message, "attachments", None))
        pending_path = await self.store.fetch_pending_owner_reply_opportunity_for_path(
            guild.id,
            root_submission["submission_id"],
            referenced_submission["submission_id"],
            source_author_user_id,
        )
        if pending_path is not None:
            opportunity = dict(pending_path)
            opportunity.update(
                {
                    "source_channel_id": int(message.channel.id),
                    "source_message_id": int(message.id),
                    "source_author_user_id": source_author_user_id,
                    "source_author_name": ge.display_name_of(author),
                    "source_preview": source_preview,
                    "source_message_fingerprint": source_fingerprint,
                    "created_at": now_iso,
                    "expires_at": (ge.now_utc() + timedelta(seconds=OWNER_REPLY_OPPORTUNITY_TTL_SECONDS)).isoformat(),
                    "resolved_at": None,
                }
            )
        else:
            opportunity = {
                "opportunity_id": secrets.token_hex(16),
                "guild_id": guild.id,
                "root_submission_id": root_submission["submission_id"],
                "root_confession_id": root_submission["confession_id"],
                "referenced_submission_id": referenced_submission["submission_id"],
                "source_channel_id": int(message.channel.id),
                "source_message_id": int(message.id),
                "source_author_user_id": source_author_user_id,
                "source_author_name": ge.display_name_of(author),
                "source_preview": source_preview,
                "source_message_fingerprint": source_fingerprint,
                "status": "pending",
                "notification_status": "none",
                "notification_channel_id": None,
                "notification_message_id": None,
                "created_at": now_iso,
                "expires_at": (ge.now_utc() + timedelta(seconds=OWNER_REPLY_OPPORTUNITY_TTL_SECONDS)).isoformat(),
                "notified_at": None,
                "resolved_at": None,
            }
        await self.store.upsert_owner_reply_opportunity(opportunity)
        if pending_path is not None and opportunity.get("notification_status") == "sent" and opportunity.get("notification_message_id"):
            return
        recent_rows = await self.store.list_owner_reply_opportunities_for_root_submission(root_submission["submission_id"], limit=10)
        if self._owner_reply_notification_is_on_cooldown(recent_rows):
            await self._update_owner_reply_notification(opportunity, status="cooldown")
            return
        notification_result = await self._send_owner_reply_notification(
            guild,
            owner_user_id=owner_user_id,
            opportunity=opportunity,
            referenced_submission=referenced_submission,
        )
        if isinstance(notification_result, tuple):
            if len(notification_result) >= 3:
                sent, notification_message_id, notification_channel_id = notification_result[:3]
            elif len(notification_result) == 2:
                sent, notification_message_id = notification_result
                notification_channel_id = None
            elif len(notification_result) == 1:
                sent = bool(notification_result[0])
                notification_message_id = None
                notification_channel_id = None
            else:
                sent = False
                notification_message_id = None
                notification_channel_id = None
        else:
            sent = bool(notification_result)
            notification_message_id = None
            notification_channel_id = None
        if sent:
            await self._update_owner_reply_notification(
                opportunity,
                status="sent",
                notification_channel_id=notification_channel_id,
                notification_message_id=notification_message_id,
                notified_at=ge.now_utc().isoformat(),
            )
            return
        await self._update_owner_reply_notification(opportunity, status="failed")

    async def _validate_owner_reply_opportunity(
        self,
        guild: discord.Guild,
        opportunity: dict[str, Any],
        *,
        author_id: int,
        allow_locked: bool = False,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if opportunity.get("status") not in ({"pending", "locked"} if allow_locked else {"pending"}):
            return None, "That reply opportunity is no longer available."
        compiled = self.get_compiled_config(guild.id)
        if not compiled["enabled"] or not compiled.get("allow_owner_replies", True):
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "Owner replies are currently disabled in this server."
        expires_at = deserialize_datetime(opportunity.get("expires_at"))
        if expires_at is None or expires_at <= ge.now_utc():
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That reply opportunity expired. Babblebox left the response anonymous and unchanged."
        root_submission = await self.store.fetch_submission(opportunity["root_submission_id"])
        if (
            root_submission is None
            or root_submission.get("status") != "published"
            or root_submission.get("submission_kind") != "confession"
        ):
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That confession is no longer available for owner replies."
        owner_link = await self.store.fetch_author_link(root_submission["submission_id"])
        if owner_link is None or int(owner_link.get("author_user_id") or 0) != int(author_id):
            return None, "That reply opportunity does not belong to you."
        referenced_submission = await self.store.fetch_submission(opportunity["referenced_submission_id"])
        if referenced_submission is None or referenced_submission.get("status") != "published":
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response is no longer available for an owner reply."
        if referenced_submission.get("submission_kind") == "reply":
            if referenced_submission.get("reply_flow") != REPLY_FLOW_OWNER_TO_USER or int(referenced_submission.get("owner_reply_generation") or 1) != 1:
                await self._expire_owner_reply_opportunity(opportunity)
                return None, "That response is no longer available for an owner reply."
        source_channel = guild.get_channel(opportunity["source_channel_id"]) or self.bot.get_channel(opportunity["source_channel_id"])
        if source_channel is None:
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response is no longer available for an owner reply."
        source_message = await self._queue_message(source_channel, message_id=opportunity["source_message_id"])
        if source_message is None:
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response is no longer available for an owner reply."
        source_reference_id = getattr(getattr(source_message, "reference", None), "message_id", None)
        if int(source_reference_id or 0) != int(referenced_submission.get("posted_message_id") or 0):
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response is no longer available for an owner reply."
        live_author_id = int(getattr(getattr(source_message, "author", None), "id", 0) or 0)
        stored_author_id = int(opportunity.get("source_author_user_id") or 0)
        if stored_author_id and live_author_id and stored_author_id != live_author_id:
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response is no longer available for an owner reply."
        stored_fingerprint = normalize_plain_text(opportunity.get("source_message_fingerprint"))
        live_fingerprint = self._owner_reply_source_fingerprint(getattr(source_message, "content", None), getattr(source_message, "attachments", None))
        if stored_fingerprint and live_fingerprint and stored_fingerprint != live_fingerprint:
            await self._expire_owner_reply_opportunity(opportunity)
            return None, "That response changed, so Babblebox closed the owner-reply opportunity."
        return {
            "guild": guild,
            "opportunity": opportunity,
            "root_submission": root_submission,
            "referenced_submission": referenced_submission,
            "source_channel": source_channel,
            "source_message": source_message,
            "source_jump_url": self._message_jump_url(guild.id, opportunity["source_channel_id"], opportunity["source_message_id"]),
        }, None

    async def get_owner_reply_opportunity_context(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        opportunity_id: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        cleaned_id = normalize_plain_text(opportunity_id)
        if not cleaned_id:
            return None, "That reply opportunity is no longer available."
        opportunity = await self.store.fetch_owner_reply_opportunity(cleaned_id)
        if opportunity is None or int(opportunity.get("guild_id") or 0) != int(guild.id):
            return None, "That reply opportunity is no longer available."
        return await self._validate_owner_reply_opportunity(guild, opportunity, author_id=author_id)

    async def get_owner_reply_opportunity_context_from_notification_message(
        self,
        *,
        notification_message_id: int,
        author_id: int,
    ) -> tuple[dict[str, Any] | None, str | None]:
        opportunity = await self.store.fetch_owner_reply_opportunity_by_notification_message_id(notification_message_id)
        if opportunity is None:
            return None, "That reply prompt is no longer available."
        guild = self.bot.get_guild(opportunity["guild_id"])
        if guild is None:
            return None, "That server is no longer available to Babblebox."
        return await self._validate_owner_reply_opportunity(guild, opportunity, author_id=author_id)

    async def list_pending_owner_reply_contexts(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        limit: int = OWNER_REPLY_INBOX_LIMIT,
    ) -> list[dict[str, Any]]:
        if not self.get_compiled_config(guild.id).get("allow_owner_replies", True):
            return []
        rows = await self.store.list_pending_owner_reply_opportunities_for_author(guild.id, author_id, limit=max(limit * 3, limit))
        contexts: list[dict[str, Any]] = []
        for row in rows:
            context, _ = await self._validate_owner_reply_opportunity(guild, row, author_id=author_id)
            if context is not None:
                contexts.append(context)
            if len(contexts) >= limit:
                break
        return contexts

    async def dismiss_owner_reply_opportunity(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        opportunity_id: str,
    ) -> tuple[bool, str]:
        context, error = await self.get_owner_reply_opportunity_context(guild, author_id=author_id, opportunity_id=opportunity_id)
        if context is None:
            return False, error or "That reply opportunity is no longer available."
        await self._dismiss_owner_reply_opportunity_record(context["opportunity"])
        return True, "Babblebox dismissed that owner reply prompt privately."

    async def dismiss_owner_reply_opportunity_from_notification(
        self,
        *,
        notification_message_id: int,
        author_id: int,
    ) -> tuple[bool, str]:
        context, error = await self.get_owner_reply_opportunity_context_from_notification_message(
            notification_message_id=notification_message_id,
            author_id=author_id,
        )
        if context is None:
            return False, error or "That reply prompt is no longer available."
        await self._dismiss_owner_reply_opportunity_record(context["opportunity"])
        return True, "Babblebox dismissed that owner reply prompt privately."

    async def mark_owner_reply_opportunity_used(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        opportunity_id: str,
    ) -> tuple[bool, str]:
        context, error = await self.get_owner_reply_opportunity_context(guild, author_id=author_id, opportunity_id=opportunity_id)
        if context is None:
            return False, error or "That reply opportunity is no longer available."
        await self._mark_owner_reply_opportunity_used_record(context["opportunity"])
        return True, "Owner reply opportunity completed."

    def _format_channel_label(self, channel_id: int | None) -> str:
        return f"<#{channel_id}>" if isinstance(channel_id, int) else "Not set"

    def _bot_member_for_guild(self, guild: discord.Guild) -> object | None:
        member = getattr(guild, "me", None)
        if member is not None:
            return member
        bot_id = getattr(getattr(self.bot, "user", None), "id", None)
        get_member = getattr(guild, "get_member", None)
        if callable(get_member) and isinstance(bot_id, int):
            with contextlib.suppress(Exception):
                resolved = get_member(bot_id)
                if resolved is not None:
                    return resolved
        return getattr(self.bot, "user", None)

    def support_channel_snapshot(self, guild: discord.Guild, *, channel_id: int | None = None) -> dict[str, Any]:
        configured_id = channel_id
        if configured_id is None:
            configured_id = self.get_config(guild.id).get("appeals_channel_id")
        snapshot = {
            "ok": False,
            "status": "missing",
            "status_label": "Missing",
            "channel_id": configured_id,
            "channel": None,
            "message": "Admins still need to configure a private appeals/report channel for this server.",
            "detail": "No appeals/report channel is configured yet.",
            "missing_permissions": (),
        }
        if not isinstance(configured_id, int):
            return snapshot
        channel = guild.get_channel(configured_id)
        snapshot["channel"] = channel
        if channel is None:
            snapshot["status"] = "unavailable"
            snapshot["status_label"] = "Unavailable"
            snapshot["message"] = "The configured appeals/report channel is unavailable."
            snapshot["detail"] = "The stored appeals/report channel no longer exists in this server."
            return snapshot
        permissions_for = getattr(channel, "permissions_for", None)
        everyone_perms = None
        if callable(permissions_for):
            with contextlib.suppress(Exception):
                everyone_perms = permissions_for(guild.default_role)
        if getattr(everyone_perms, "view_channel", False):
            snapshot["status"] = "public"
            snapshot["status_label"] = "Public / Unsafe"
            snapshot["message"] = (
                "Babblebox can only use a private appeals/report channel. "
                "This channel is visible to @everyone, so support is unavailable until an admin fixes it."
            )
            snapshot["detail"] = f"{getattr(channel, 'mention', self._format_channel_label(configured_id))} is visible to @everyone."
            return snapshot
        bot_target = self._bot_member_for_guild(guild)
        bot_perms = None
        if callable(permissions_for) and bot_target is not None:
            with contextlib.suppress(Exception):
                bot_perms = permissions_for(bot_target)
        required_permissions = (
            ("view_channel", "View Channel"),
            ("send_messages", "Send Messages"),
            ("embed_links", "Embed Links"),
        )
        missing_permissions = tuple(
            label for attr, label in required_permissions if not getattr(bot_perms, attr, False)
        )
        if missing_permissions:
            snapshot["status"] = "bot_missing_permissions"
            snapshot["status_label"] = "Bot Missing Permissions"
            snapshot["message"] = (
                "Babblebox cannot use the configured appeals/report channel until it has "
                f"{', '.join(missing_permissions)}."
            )
            snapshot["detail"] = f"Missing bot permissions: {', '.join(missing_permissions)}."
            snapshot["missing_permissions"] = missing_permissions
            return snapshot
        snapshot["ok"] = True
        snapshot["status"] = "ready"
        snapshot["status_label"] = "Ready"
        snapshot["message"] = "Private support is ready."
        snapshot["detail"] = "Private and ready for anonymous appeals and reports."
        return snapshot

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

    def _submission_kind_label(self, submission: dict[str, Any] | None) -> str:
        record = submission or {}
        if record.get("submission_kind") != "reply":
            return "Confession"
        if record.get("reply_flow") == REPLY_FLOW_OWNER_TO_USER:
            generation = int(record.get("owner_reply_generation") or 1)
            return f"Owner Reply Round {generation}" if generation > 1 else "Owner Reply"
        return "Reply"

    def _owner_reply_delivery_copy(self, guild_id: int) -> str:
        config = self.get_config(guild_id)
        if config.get("owner_reply_review_mode"):
            return (
                "Your reply posts publicly as an Anonymous Owner Reply, stays text-only, may go through private review first, "
                "and Babblebox keeps your identity hidden from members and staff."
            )
        return (
            "Your reply posts publicly as an Anonymous Owner Reply, stays text-only, and Babblebox keeps your identity hidden from members and staff."
        )

    async def get_owned_submission_context(
        self,
        guild_id: int,
        *,
        author_id: int,
        target_id: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        cleaned_target = normalize_plain_text(target_id).upper()
        if not cleaned_target:
            return None, "Use a confession ID like `CF-XXXXXX` or a case ID like `CS-XXXXXX`."
        submission = None
        case = None
        if cleaned_target.startswith(f"{CASE_ID_PREFIX}-"):
            case = await self.store.fetch_case(guild_id, cleaned_target)
            if case is None:
                return None, "That case ID was not found."
            submission = await self.store.fetch_submission(case["submission_id"])
        elif cleaned_target.startswith(f"{CONFESSION_ID_PREFIX}-"):
            submission = await self.store.fetch_submission_by_confession_id(guild_id, cleaned_target)
        else:
            return None, "Use a confession ID like `CF-XXXXXX` or a case ID like `CS-XXXXXX`."
        if submission is None:
            return None, "That confession ID was not found."
        author_link = await self.store.fetch_author_link(submission["submission_id"])
        if author_link is None or int(author_link.get("author_user_id") or 0) != int(author_id):
            return None, "That confession ID does not belong to you."
        if case is None and submission.get("current_case_id"):
            case = await self.store.fetch_case(guild_id, str(submission["current_case_id"]))
        config = self.get_config(guild_id)
        can_edit = bool(config["allow_self_edit"] and submission.get("status") == "queued" and submission.get("review_status") == "pending")
        can_delete = submission.get("status") in {"published", "queued", "blocked"}
        return {
            "target_id": cleaned_target,
            "submission": submission,
            "case": case,
            "can_edit": can_edit,
            "can_delete": can_delete,
        }, None

    def build_member_manage_embed(self, context: dict[str, Any]) -> discord.Embed:
        submission = context["submission"]
        case = context.get("case")
        kind_label = self._submission_kind_label(submission)
        embed = discord.Embed(
            title=f"My Anonymous {kind_label}",
            description=(
                "Babblebox verified ownership privately. Staff still do not see your account through this flow, "
                "and the private ownership link stays protected in Confessions storage."
            ),
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Status",
            value=(
                f"Type: **{kind_label}**\n"
                f"Public ID: **`{submission['confession_id']}`**\n"
                f"State: **{str(submission.get('status') or 'unknown').replace('_', ' ').title()}**\n"
                f"Review: **{str(submission.get('review_status') or 'none').replace('_', ' ').title()}**"
            ),
            inline=False,
        )
        if submission.get("parent_confession_id"):
            embed.add_field(name="Replying To", value=f"`{submission['parent_confession_id']}`", inline=False)
        if case is not None:
            embed.add_field(name="Case", value=f"`{case['case_id']}`", inline=True)
        actions = []
        actions.append("Delete is available." if context["can_delete"] else "Delete is no longer available for this item.")
        if context["can_edit"]:
            actions.append("Edit is available while the submission is still pending review.")
        else:
            actions.append("Edit is only available when admins enable it and the submission is still pending review.")
        embed.add_field(name="Available Actions", value="\n".join(actions), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Private owner tools")

    def _owner_reply_target_label(self, referenced_submission: dict[str, Any]) -> str:
        if referenced_submission.get("submission_kind") == "confession":
            return "your confession"
        if referenced_submission.get("reply_flow") == REPLY_FLOW_OWNER_TO_USER:
            return "your earlier owner reply"
        return "your confession discussion"

    def build_owner_reply_notification_embed(
        self,
        guild: discord.Guild,
        opportunity: dict[str, Any],
        referenced_submission: dict[str, Any],
    ) -> discord.Embed:
        target_label = self._owner_reply_target_label(referenced_submission)
        embed = discord.Embed(
            title="Someone responded to your confession",
            description=(
                f"**{opportunity['source_author_name']}** replied to {target_label} in **{guild.name}**. "
                "You can answer publicly as an anonymous owner reply without revealing yourself."
            ),
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Confession", value=f"`{opportunity['root_confession_id']}`", inline=True)
        embed.add_field(name="When", value=_owner_reply_opportunity_age_text(opportunity.get("created_at")), inline=True)
        embed.add_field(name="Response", value=ge.safe_field_text(opportunity["source_preview"], limit=1024), inline=False)
        embed.add_field(
            name="Your Privacy",
            value=self._owner_reply_delivery_copy(guild.id),
            inline=False,
        )
        jump_url = self._message_jump_url(guild.id, opportunity.get("source_channel_id"), opportunity.get("source_message_id"))
        if jump_url:
            embed.add_field(name="Source", value=f"[Open response]({jump_url})", inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Private owner prompt")

    def build_owner_reply_inbox_embed(self, guild: discord.Guild, contexts: Sequence[dict[str, Any]]) -> discord.Embed:
        if not contexts:
            return ge.make_status_embed(
                "Owner Reply Inbox",
                "No current member responses are waiting for an owner reply. Babblebox will DM you if someone explicitly replies to your confession or first owner reply and DMs are available.",
                tone="info",
                footer="Babblebox Confessions",
            )
        lines = []
        for index, context in enumerate(contexts[:OWNER_REPLY_INBOX_LIMIT], start=1):
            opportunity = context["opportunity"]
            target_label = self._owner_reply_target_label(context["referenced_submission"])
            lines.append(
                f"**{index}. {opportunity['source_author_name']}** replied to {target_label} about `{opportunity['root_confession_id']}`.\n"
                f"{ge.safe_field_text(opportunity['source_preview'], limit=120)}"
            )
        embed = discord.Embed(
            title="Owner Reply Inbox",
            description="Choose a member response below to review it privately and decide whether to post an anonymous owner reply publicly.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Pending Responses", value="\n\n".join(lines), inline=False)
        embed.add_field(
            name="Privacy",
            value=self._owner_reply_delivery_copy(guild.id),
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Confessions | Private owner prompt")

    def build_owner_reply_detail_embed(self, guild: discord.Guild, context: dict[str, Any]) -> discord.Embed:
        opportunity = context["opportunity"]
        target_label = self._owner_reply_target_label(context["referenced_submission"])
        embed = discord.Embed(
            title="Reply to Member Anonymously",
            description=(
                f"**{opportunity['source_author_name']}** replied to {target_label}. "
                "Babblebox verified privately that this confession belongs to you."
            ),
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Confession", value=f"`{opportunity['root_confession_id']}`", inline=True)
        embed.add_field(name="Age", value=_owner_reply_opportunity_age_text(opportunity.get("created_at")), inline=True)
        embed.add_field(name="Response", value=ge.safe_field_text(opportunity["source_preview"], limit=1024), inline=False)
        jump_url = context.get("source_jump_url")
        if jump_url:
            embed.add_field(name="Source", value=f"[Open response]({jump_url})", inline=False)
        embed.add_field(
            name="Your Privacy",
            value=f"{self._owner_reply_delivery_copy(guild.id)} Owner replies stay text-only.",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Confessions | Private owner prompt")

    async def self_delete_confession(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        target_id: str,
    ) -> tuple[bool, str]:
        context, error = await self.get_owned_submission_context(guild.id, author_id=author_id, target_id=target_id)
        if context is None:
            return False, error or "That confession could not be verified."
        submission = context["submission"]
        case = context.get("case")
        if submission.get("status") not in {"published", "queued", "blocked"}:
            kind_label = self._submission_kind_label(submission).casefold()
            return False, f"That {kind_label} is already closed and cannot be deleted from this flow."
        now_iso = ge.now_utc().isoformat()
        previous_status = str(submission.get("status") or "")
        previous_review_status = str(submission.get("review_status") or "")
        if submission.get("posted_channel_id"):
            channel = guild.get_channel(submission.get("posted_channel_id")) or self.bot.get_channel(submission.get("posted_channel_id"))
            if channel is not None:
                message = await self._queue_message(channel, message_id=submission.get("posted_message_id"))
                if message is not None:
                    with contextlib.suppress(discord.Forbidden, discord.HTTPException, Exception):
                        await message.delete()
        submission["status"] = "deleted"
        submission["review_status"] = "withdrawn" if previous_status in {"queued", "blocked"} or previous_review_status == "pending" else previous_review_status
        submission["posted_channel_id"] = None
        submission["posted_message_id"] = None
        submission["resolved_at"] = now_iso
        await self._scrub_submission_for_terminal_state(submission)
        if case is not None and case.get("status") == "open":
            case["status"] = "resolved"
            case["resolution_action"] = "self_delete"
            case["resolution_note"] = "Deleted privately by the owner."
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
        await self._sync_review_queue(guild, note=f"{self._submission_kind_label(submission)} `{submission['confession_id']}` was withdrawn by its owner.")
        return True, f"{self._submission_kind_label(submission)} `{submission['confession_id']}` was deleted privately."

    async def self_edit_confession(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        target_id: str,
        content: str | None,
        link: str | None = None,
    ) -> ConfessionSubmissionResult:
        context, error = await self.get_owned_submission_context(guild.id, author_id=author_id, target_id=target_id)
        if context is None:
            return ConfessionSubmissionResult(False, "blocked", error or "That confession could not be verified.")
        submission = context["submission"]
        case = context.get("case")
        if not context["can_edit"]:
            return ConfessionSubmissionResult(
                False,
                "blocked",
                "Editing is only available when admins enable it and the submission is still pending review.",
                confession_id=submission["confession_id"],
                case_id=case["case_id"] if case else None,
                submission_kind=str(submission.get("submission_kind") or "confession"),
                parent_confession_id=submission.get("parent_confession_id"),
            )
        compiled = self.get_compiled_config(guild.id)
        normalized = normalize_plain_text(content)
        squashed = squash_for_evasion_checks(normalized.casefold())
        if submission.get("submission_kind") == "reply":
            link_ok, shared_link_url = True, None
        else:
            link_ok, shared_link_url = _normalize_shared_link_input(link)
        if not link_ok:
            return ConfessionSubmissionResult(False, "blocked", str(shared_link_url or "That link is not valid."), confession_id=submission["confession_id"])
        recent_rows = [
            row
            for row in await self.store.list_recent_submissions_for_author(guild.id, author_id, limit=6)
            if row.get("submission_id") != submission["submission_id"]
        ]
        safety = await self._evaluate_safety(
            compiled,
            text=normalized,
            squashed=squashed,
            shared_link_url=shared_link_url,
            attachment_meta=list(submission.get("attachment_meta") or []),
            recent_rows=recent_rows,
        )
        if safety.outcome == "blocked":
            return ConfessionSubmissionResult(
                False,
                "blocked",
                safety.reason,
                confession_id=submission["confession_id"],
                case_id=case["case_id"] if case else None,
                flag_codes=safety.flag_codes,
                submission_kind=str(submission.get("submission_kind") or "confession"),
                parent_confession_id=submission.get("parent_confession_id"),
            )
        duplicate_signals = build_duplicate_signals(
            self.store.privacy,
            guild.id,
            normalized,
            list(submission.get("attachment_meta") or []),
            shared_link_url,
        )
        submission["content_body"] = normalized or None
        submission["shared_link_url"] = shared_link_url
        submission["staff_preview"] = _staff_preview_text(normalized, list(submission.get("attachment_meta") or []))
        submission["content_fingerprint"] = duplicate_signals.exact_hash
        submission["similarity_key"] = None
        submission["fuzzy_signature"] = duplicate_signals.fuzzy_signature
        submission["flag_codes"] = list(safety.flag_codes)
        await self.store.upsert_submission(submission)
        if case is not None:
            case["reason_codes"] = list(safety.flag_codes)
            case["review_version"] = int(case.get("review_version") or 0) + 1
            case["resolution_note"] = "Updated privately by the owner while pending review."
            await self.store.upsert_case(case)
        await self._sync_review_queue(guild, note=f"{self._submission_kind_label(submission)} `{submission['confession_id']}` was updated by its owner.")
        return ConfessionSubmissionResult(
            True,
            "queued",
            "Your update was saved and remains in private review.",
            confession_id=submission["confession_id"],
            case_id=case["case_id"] if case else None,
            flag_codes=tuple(safety.flag_codes),
            submission_kind=str(submission.get("submission_kind") or "confession"),
            parent_confession_id=submission.get("parent_confession_id"),
        )

    def _support_rate_limit_message(self, guild_id: int, author_id: int, kind: str) -> str | None:
        now = time.monotonic()
        key = (guild_id, author_id, kind)
        expires_at = self._support_rate_limits.get(key)
        if expires_at is not None and expires_at > now:
            return f"Please wait about {format_duration_brief(int(expires_at - now))} before sending another {kind}."
        return None

    def _mark_support_rate_limit(self, guild_id: int, author_id: int, kind: str):
        self._support_rate_limits[(guild_id, author_id, kind)] = time.monotonic() + SUPPORT_RATE_LIMIT_SECONDS

    async def _post_support_ticket(
        self,
        guild: discord.Guild,
        *,
        title: str,
        description: str,
        fields: Sequence[tuple[str, str]],
    ) -> tuple[bool, str]:
        support_snapshot = self.support_channel_snapshot(guild)
        if not support_snapshot["ok"]:
            return False, str(support_snapshot["message"])
        channel = support_snapshot["channel"]
        if channel is None:
            return False, "The configured appeals/report channel is unavailable."
        ticket_id = _public_id(SUPPORT_TICKET_ID_PREFIX)
        embed = discord.Embed(title=f"{title} `{ticket_id}`", description=description, color=ge.EMBED_THEME["info"])
        for name, value in fields:
            embed.add_field(name=name, value=ge.safe_field_text(value, limit=1024), inline=False)
        embed = ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind support")
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            return True, ticket_id
        return False, "Babblebox could not deliver that support request right now."

    def _sanitize_support_body(self, raw_text: str | None) -> tuple[bool, str]:
        normalized = normalize_plain_text(raw_text)
        squashed = squash_for_evasion_checks(normalized.casefold())
        if not normalized:
            return False, "Please add a short explanation."
        if MENTION_RE.search(normalized) or RAW_MENTION_RE.search(normalized) or RAW_MENTION_RE.search(squashed):
            return False, "Support requests cannot include user, role, channel, or mass mentions."
        private_pattern = _find_private_leak(normalized, squashed)
        if private_pattern is not None:
            return False, f"Support requests cannot include {private_pattern}."
        return True, normalized[:1800]

    async def submit_support_request(
        self,
        guild: discord.Guild,
        *,
        author_id: int,
        kind: str,
        details: str,
        target_id: str | None = None,
    ) -> tuple[bool, str]:
        kind = normalize_plain_text(kind).casefold()
        if kind not in {"appeal", "report"}:
            return False, "That support flow is not available."
        valid_details, cleaned_details = self._sanitize_support_body(details)
        if not valid_details:
            return False, cleaned_details
        cleaned_target = normalize_plain_text(target_id).upper() if target_id else None
        fields: list[tuple[str, str]] = []
        if kind == "appeal":
            state = self._normalize_restriction_state(await self._enforcement_state(guild.id, author_id))
            restriction_label = self._restriction_label(state)
            if cleaned_target:
                context, error = await self.get_owned_submission_context(guild.id, author_id=author_id, target_id=cleaned_target)
                if context is None:
                    return False, error or "That confession or case could not be verified for appeal."
                submission = context["submission"]
                case = context.get("case")
                fields.append(("Reference", f"{submission['confession_id']}" if case is None else f"{submission['confession_id']} / {case['case_id']}"))
                fields.append(("Submission State", f"{submission.get('status', 'unknown').replace('_', ' ').title()} / {submission.get('review_status', 'none').replace('_', ' ').title()}"))
            elif restriction_label != "None":
                fields.append(("Restriction", restriction_label))
                if state.get("last_case_id"):
                    fields.append(("Reference", str(state["last_case_id"])))
            else:
                return False, "Reference your own confession or case ID, or send the appeal while the restriction is still active."
            title = "Anonymous Appeal"
        else:
            if not cleaned_target:
                return False, "Reports should include a confession ID or case ID."
            if cleaned_target.startswith(f"{CASE_ID_PREFIX}-"):
                case = await self.store.fetch_case(guild.id, cleaned_target)
                if case is None:
                    return False, "That case ID was not found."
                fields.append(("Reference", f"{case['confession_id']} / {case['case_id']}"))
                fields.append(("Case Kind", str(case.get("case_kind") or "unknown").replace("_", " ").title()))
            elif cleaned_target.startswith(f"{CONFESSION_ID_PREFIX}-"):
                submission = await self.store.fetch_submission_by_confession_id(guild.id, cleaned_target)
                if submission is None:
                    return False, "That confession ID was not found."
                fields.append(("Reference", submission["confession_id"]))
                fields.append(("Submission State", str(submission.get("status") or "unknown").replace("_", " ").title()))
            else:
                return False, "Reports should reference a confession ID like `CF-XXXXXX` or a case ID like `CS-XXXXXX`."
            title = "Anonymous Report"
        rate_limit_message = self._support_rate_limit_message(guild.id, author_id, kind)
        if rate_limit_message is not None:
            return False, rate_limit_message
        fields.append(("Details", cleaned_details))
        ok, support_result = await self._post_support_ticket(
            guild,
            title=title,
            description="The author identity remains hidden from staff. Use the public IDs below for follow-up.",
            fields=fields,
        )
        if not ok:
            return False, support_result
        self._mark_support_rate_limit(guild.id, author_id, kind)
        return True, f"{title} `{support_result}` was sent privately."

    def _member_reason_message(self, result: ConfessionSubmissionResult) -> str:
        flags = set(result.flag_codes)
        noun = "reply" if result.submission_kind == "reply" else "confession"
        is_owner_reply = result.submission_kind == "reply" and result.reply_flow == REPLY_FLOW_OWNER_TO_USER
        if result.state == "published":
            if is_owner_reply and result.parent_confession_id:
                return f"Your anonymous owner reply to `{result.parent_confession_id}` was posted publicly without revealing you."
            if result.submission_kind == "reply" and result.parent_confession_id:
                return f"Your reply to `{result.parent_confession_id}` was posted without your name attached."
            return "Your confession was posted without your name attached."
        if result.state == "queued":
            if is_owner_reply and result.parent_confession_id:
                return (
                    f"Your anonymous owner reply to `{result.parent_confession_id}` was received and will post publicly after private review."
                )
            if result.submission_kind == "reply" and result.parent_confession_id:
                return (
                    f"Your anonymous reply to `{result.parent_confession_id}` stays anonymous and may go through private approval before posting."
                )
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
            return f"{noun.title()}s cannot include private contact or identifying details."
        if "duplicate_spam" in flags or "near_duplicate_spam" in flags or "repetitive_spam" in flags:
            return f"That {noun} is too close to recent submissions."
        if "hate_speech" in flags or "abusive_language" in flags or "vulgar_language" in flags:
            return f"That {noun} includes language this server blocks."
        return result.message

    def build_member_result_embed(self, result: ConfessionSubmissionResult) -> discord.Embed:
        if result.submission_kind == "reply" and result.reply_flow == REPLY_FLOW_OWNER_TO_USER:
            noun_title = "Owner Reply"
        else:
            noun_title = "Reply" if result.submission_kind == "reply" else "Confession"
        title_map = {
            "published": f"{noun_title} Posted",
            "queued": f"{noun_title} Received",
            "blocked": f"{noun_title} Not Sent",
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
        if result.parent_confession_id is not None:
            embed.add_field(name="Replying To", value=f"`{result.parent_confession_id}`", inline=True)
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
        image_policy = (
            f"Enabled (max {config['max_images']}, private review)"
            if config["allow_images"]
            else "Off by default"
        )
        reply_policy = "Enabled with warning" if config["allow_anonymous_replies"] else "Off by default"
        owner_reply_policy = "Enabled by default" if config["allow_owner_replies"] else "Off"
        owner_reply_review = "Private review" if config["owner_reply_review_mode"] else "Direct publish"
        edit_policy = "Enabled with warning" if config["allow_self_edit"] else "Off by default"
        role_access = self._member_role_access_label(guild)
        support_snapshot = self.support_channel_snapshot(guild)
        description = (
            "Share something quietly through a private composer. Use `/confess create` or the panel button below. "
            "When admins enable Confessions, Babblebox keeps the author hidden from members and staff in normal use while still enforcing safety internally."
            if ready
            else "Anonymous confessions are optional in Babblebox and are not ready in this server yet. The panel stays here so admins can finish setup without reposting it."
        )
        embed = discord.Embed(
            title="Anonymous Confessions",
            description=description,
            color=ge.EMBED_THEME["accent"] if ready else ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="How It Works",
            value=(
                "Run `/confess create` or tap **Send Confession**.\n"
                "Add text and at most one trusted link.\n"
                "Use **Reply to confession anonymously** from a live confession post when replies are enabled.\n"
                "If someone explicitly replies to your confession or first owner reply, Babblebox can privately offer you a public anonymous owner reply.\n"
                "Use **Manage My Confession** to delete your own submission privately.\n"
                "Use **Appeal / Report** for false positives, restrictions, or problem reports when this server has private support configured.\n"
                "Images and public anonymous replies stay off by default unless admins explicitly enable them."
            ),
            inline=False,
        )
        embed.add_field(
            name="Server Policy",
            value=(
                f"Review mode: **{'On' if config['review_mode'] else 'Off'}**\n"
                f"Adult content: **{'Blocked' if config['block_adult_language'] else 'Allowed'}**\n"
                f"Trusted links: **{'Allowed' if config['allow_trusted_mainstream_links'] else 'Blocked'}**\n"
                f"Images: **{image_policy}**\n"
                f"Reply to confession anonymously: **{reply_policy}**\n"
                f"Owner replies: **{owner_reply_policy}**\n"
                f"Owner-reply publishing: **{owner_reply_review}**\n"
                f"Self-edit: **{edit_policy}**\n"
                f"Role access: **{role_access}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Private Support",
            value=(
                "Appeals / reports channel: "
                f"**{self._format_channel_label(support_snapshot['channel_id'])}**\n"
                f"Status: **{support_snapshot['status_label']}**\n"
                + (
                    "Members can use appeal/report while admins keep that channel private."
                    if support_snapshot["ok"]
                    else str(support_snapshot["message"])
                )
            ),
            inline=False,
        )
        embed.add_field(
            name="Stay Anonymous",
            value=(
                "Babblebox hides your account from members and staff, and private Confessions data is protected in storage. "
                "The service still enforces safety internally, operators are still part of the trust model, and the words, link destination, or image contents you choose can still identify you."
            ),
            inline=False,
        )
        if not ready:
            embed.add_field(name="Availability", value=self.operability_message(guild.id), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Private composer")

    def build_member_panel_help_embed(self, guild: discord.Guild) -> discord.Embed:
        config = self.get_config(guild.id)
        role_snapshot = self._role_policy_snapshot(guild)
        support_snapshot = self.support_channel_snapshot(guild)
        image_line = (
            f"Text, one trusted link total, and up to {config['max_images']} images only if admins explicitly enable them. Enabled images always enter private review."
            if config["allow_images"]
            else "Text and one trusted link total. Images are off by default unless admins explicitly enable them."
        )
        reply_line = (
            "Reply to confession anonymously is enabled with extra moderation burden, so every reply stays anonymous, text-only, launches from a live confession post, and may go through private approval before posting."
            if config["allow_anonymous_replies"]
            else "Anonymous replies are off by default unless admins explicitly enable them."
        )
        owner_reply_line = (
            "Owner replies are enabled. If someone explicitly replies to your confession or first owner reply, Babblebox can privately offer you a public Anonymous Owner Reply that stays text-only."
            if config["allow_owner_replies"]
            else "Owner replies are off in this server."
        )
        role_line = (
            "This server limits who can submit based on role settings. Selected blocked roles are always denied first."
            if role_snapshot["active_allowed_ids"] or role_snapshot["active_blocked_ids"]
            else "This server currently leaves confession access open to everyone."
        )
        embed = discord.Embed(
            title="How Anonymous Confessions Work",
            description="Confessions are submitted privately and published by Babblebox, not by you. `/confess create` is the direct fallback entry point if the public panel is unavailable.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(
            name="Privacy",
            value=(
                "Members and server staff see confession IDs, not the author behind them. "
                "Babblebox protects private Confessions data in storage and still enforces safety internally, but it does not remove the service operator from the trust model."
            ),
            inline=False,
        )
        embed.add_field(
            name="What You Can Add",
            value=f"{image_line}\n{reply_line}\n{owner_reply_line}\n{role_line}",
            inline=False,
        )
        embed.add_field(
            name="Owner Controls",
            value=(
                "Use `/confess create` whenever you want the direct private composer.\n"
                "Use `/confess reply-to-user` to review member responses to your confession and post an anonymous owner reply publicly.\n"
                "You can privately delete your own confession or reply.\n"
                "Self-edit is only available if this server enables it and the submission is still pending review."
            ),
            inline=False,
        )
        embed.add_field(
            name="Private Support",
            value=(
                "Appeal/report opens a private member flow only when admins configure a private appeals/report channel Babblebox can use."
                if not support_snapshot["ok"]
                else "Appeal/report stays member-facing, but Babblebox only delivers it while the configured support channel stays private."
            ),
            inline=False,
        )
        embed.add_field(
            name="What Gets Blocked",
            value="Mentions, unsafe links, private details, spam, offensive or derogatory language, unsupported image types, and anything this server has disabled.",
            inline=False,
        )
        embed.add_field(
            name="Self-Identifying Content",
            value="Staff cannot see your Discord identity through Babblebox, but a personal profile link, full name, face, screenshot, or document inside your confession can still reveal you.",
            inline=False,
        )
        return ge.style_embed(embed, footer="Babblebox Confessions")

    async def build_dashboard_embed(self, guild: discord.Guild, *, section: str = "overview") -> discord.Embed:
        config = self.get_config(guild.id)
        role_snapshot = self._role_policy_snapshot(guild)
        support_snapshot = self.support_channel_snapshot(guild)
        privacy_status = await self._guild_privacy_status(guild.id)
        role_value = (
            f"Allowlist: **{len(role_snapshot['active_allowed_ids'])}** active\n"
            f"{self._format_role_labels(role_snapshot['allow_labels'])}\n"
            f"Blacklist: **{len(role_snapshot['active_blocked_ids'])}** active\n"
            f"{self._format_role_labels(role_snapshot['block_labels'])}\n"
            f"Rule: **{self._role_policy_rule_text()}**"
        )
        if role_snapshot["stale_allowed"] or role_snapshot["stale_blocked"]:
            role_value += (
                f"\nStale configured roles: allowlist **{role_snapshot['stale_allowed']}**, "
                f"blacklist **{role_snapshot['stale_blocked']}**"
            )
        support_value = (
            f"Channel: {self._format_channel_label(support_snapshot['channel_id'])}\n"
            f"Status: **{support_snapshot['status_label']}**\n"
            f"{support_snapshot['detail']}"
        )
        counts = await self.store.fetch_guild_counts(guild.id) if self.storage_ready else {
            "queued_submissions": 0,
            "published_submissions": 0,
            "blocked_submissions": 0,
            "open_cases_total": 0,
            "open_review_cases": 0,
            "open_safety_cases": 0,
            "open_moderation_cases": 0,
        }
        embed = discord.Embed(
            title="Confessions Control Panel",
            description="Staff work by confession ID and case ID only. Author identity remains bot-private.",
            color=ge.EMBED_THEME["info"],
        )
        if section == "policy":
            embed.add_field(
                name="Risky Features",
                value=(
                    f"Images: **{'Enabled with warning' if config['allow_images'] else 'Off by default'}**\n"
                    f"Public anonymous replies: **{'Enabled with warning' if config['allow_anonymous_replies'] else 'Off by default'}**\n"
                    f"Owner replies: **{'Enabled by default' if config['allow_owner_replies'] else 'Off'}**\n"
                    f"Owner-reply review: **{'On' if config['owner_reply_review_mode'] else 'Off by default'}**\n"
                    f"Self-edit: **{'Enabled with warning' if config['allow_self_edit'] else 'Off by default'}**\n"
                    f"Review mode: **{'On' if config['review_mode'] else 'Off'}**"
                ),
                inline=False,
            )
            embed.add_field(
                name="Warnings",
                value=(
                    "Images can increase moderation burden and abuse risk.\n"
                    "Public anonymous replies can increase abuse, drama, and moderation complexity.\n"
                    "Owner replies stay owner-bound, text-only, and limited to one extra bounce.\n"
                    "Editing can create bait-and-switch moderation problems."
                ),
                inline=False,
            )
            embed.add_field(
                name="Link Policy",
                value=(
                    f"Trusted families: **{'Allowed' if config['allow_trusted_mainstream_links'] else 'Blocked'}**\n"
                    f"Allowlist: **{len(config['custom_allow_domains'])}** custom\n"
                    f"Blocklist: **{len(config['custom_block_domains'])}** custom\n"
                    "Unknown links: **Blocked**"
                ),
                inline=False,
            )
            embed.add_field(name="Role Eligibility", value=ge.safe_field_text(role_value, limit=1024), inline=False)
            embed.add_field(
                name="Restrictions",
                value=(
                    f"Cooldown: **{format_duration_brief(int(config['cooldown_seconds']))}**\n"
                    f"Burst limit: **{config['burst_limit']} in {format_duration_brief(int(config['burst_window_seconds']))}**\n"
                    f"Auto suspend: **{config['auto_suspend_hours']}h**\n"
                    f"Temp ban at **{config['strike_temp_ban_threshold']}** strikes, permanent at **{config['strike_perm_ban_threshold']}**\n"
                    "ID actions: **Delete, pause, temp-ban, perm-ban, restrict images, clear, false positive**"
                ),
                inline=False,
            )
            embed.add_field(name="Support Channel", value=ge.safe_field_text(support_value, limit=1024), inline=False)
        elif section == "review":
            embed.add_field(
                name="Review",
                value=(
                    f"Review mode: **{'On' if config['review_mode'] else 'Off'}**\n"
                    f"Review channel: {self._format_channel_label(config['review_channel_id'])}\n"
                    f"Open queue: **{counts['open_review_cases']}** case(s)\n"
                    f"Open safety blocks: **{counts['open_safety_cases']}**\n"
                    f"Open moderation cases: **{counts['open_moderation_cases']}**"
                ),
                inline=False,
            )
            embed.add_field(name="Support Channel", value=ge.safe_field_text(support_value, limit=1024), inline=False)
            embed.add_field(
                name="Quick Help",
                value="Use `/confessions moderate` with a confession ID or case ID to approve, deny, delete, pause, ban, restrict images, clear, or override a false positive without seeing the author.",
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
                    f"Open cases: **{counts['open_cases_total']}**"
                ),
                inline=False,
            )
            embed.add_field(name="Role Eligibility", value=ge.safe_field_text(role_value, limit=1024), inline=False)
            embed.add_field(name="Support Channel", value=ge.safe_field_text(support_value, limit=1024), inline=False)
            embed.add_field(name="Operability", value=self.operability_message(guild.id), inline=False)
        embed.add_field(name="Privacy Hardening", value=ge.safe_field_text(self._privacy_dashboard_value(privacy_status), limit=1024), inline=False)
        return ge.style_embed(embed, footer="Babblebox Confessions | Staff-blind moderation")

    async def _build_public_confession_embeds(self, submission: dict[str, Any]) -> list[discord.Embed]:
        embeds: list[discord.Embed] = []
        body = normalize_plain_text(submission.get("content_body"))
        shared_link_url = normalize_plain_text(submission.get("shared_link_url"))
        private_media = await self.store.fetch_private_media(submission["submission_id"])
        attachment_urls = list((private_media or {}).get("attachment_urls") or [])
        is_reply = submission.get("submission_kind") == "reply"
        is_owner_reply = is_reply and submission.get("reply_flow") == REPLY_FLOW_OWNER_TO_USER
        title = (
            f"Anonymous Owner Reply `{submission['confession_id']}`"
            if is_owner_reply
            else (f"Anonymous Reply `{submission['confession_id']}`" if is_reply else f"Anonymous Confession `{submission['confession_id']}`")
        )
        main = discord.Embed(
            title=title,
            description=body or "Shared quietly through Babblebox.",
            color=ge.EMBED_THEME["accent"],
        )
        if submission.get("parent_confession_id"):
            main.add_field(name="Replying To", value=f"`{submission['parent_confession_id']}`", inline=True)
        if is_owner_reply:
            generation = int(submission.get("owner_reply_generation") or 1)
            main.add_field(name="Flow", value="Owner reply" if generation == 1 else f"Owner reply round {generation}", inline=True)
        if shared_link_url:
            main.add_field(name="Trusted Link", value=shared_link_url, inline=False)
        attachment_meta = list(submission.get("attachment_meta") or [])
        if attachment_meta:
            main.add_field(name="Images", value=f"{len(attachment_meta[:3])} attached", inline=True)
        embeds.append(ge.style_embed(main, footer="Babblebox Confessions | Anonymous owner reply" if is_owner_reply else "Babblebox Confessions | Anonymous post"))
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

    def _build_public_confession_view(self, guild_id: int, submission: dict[str, Any]) -> discord.ui.View | None:
        if submission.get("status") != "published" or submission.get("submission_kind") != "confession":
            return None
        if not self.get_config(guild_id)["allow_anonymous_replies"]:
            return None
        cog = self.bot.get_cog("ConfessionsCog")
        if cog is None:
            return None
        build_view = getattr(cog, "build_public_confession_view", None)
        if not callable(build_view):
            return None
        return build_view(guild_id=guild_id)

    async def _publish_submission(self, guild: discord.Guild, submission: dict[str, Any]) -> tuple[bool, int | None, int | None, str | None]:
        channel_id = submission.get("posted_channel_id") or self.get_config(guild.id).get("confession_channel_id")
        channel = guild.get_channel(int(channel_id)) if isinstance(channel_id, int) else None
        if channel is None:
            channel = self.bot.get_channel(self.get_config(guild.id).get("confession_channel_id"))
        if channel is None:
            return False, None, None, "The confession channel is unavailable."

        embeds = await self._build_public_confession_embeds(submission)
        view = self._build_public_confession_view(guild.id, {**submission, "status": "published"})
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            message = await channel.send(embeds=embeds, view=view, allowed_mentions=discord.AllowedMentions.none())
            if view is not None:
                with contextlib.suppress(Exception):
                    self.bot.add_view(view, message_id=message.id)
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
            "submission_kind": row.get("submission_kind") or "confession",
            "parent_confession_id": row.get("parent_confession_id"),
            "reply_flow": row.get("reply_flow"),
            "owner_reply_generation": int(row.get("owner_reply_generation") or 0) or None,
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
        current_label = self._submission_kind_label(current)
        current_value = f"Case `{current['case_id']}` | {current_label} `{current['confession_id']}`"
        if current.get("parent_confession_id"):
            current_value += f"\nParent: `{current['parent_confession_id']}`"
        embed.add_field(name="Current", value=current_value, inline=False)
        embed.add_field(name="Age", value=current["age"], inline=True)
        embed.add_field(name="Reasons", value=", ".join(current.get("reason_labels") or ("None",)), inline=True)
        embed.add_field(name="Preview", value=current["preview"], inline=False)
        if current.get("shared_link_url"):
            embed.add_field(name="Link", value=str(current["shared_link_url"]), inline=False)
        if current.get("attachment_summary"):
            embed.add_field(name="Attachments", value=current["attachment_summary"], inline=False)
        backlog = []
        for row in pending_rows[:REVIEW_PREVIEW_LIMIT]:
            backlog.append(f"`{row['case_id']}` / `{row['confession_id']}` ({self._submission_kind_label(row)})")
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

    async def _sync_published_confession_views(self, guild: discord.Guild):
        submissions = await self.store.list_published_top_level_submissions(guild.id)
        for submission in submissions:
            channel_id = submission.get("posted_channel_id")
            message_id = submission.get("posted_message_id")
            channel = guild.get_channel(channel_id) or self.bot.get_channel(channel_id)
            if channel is None:
                continue
            message = await self._queue_message(channel, message_id=message_id)
            if message is None:
                continue
            view = self._build_public_confession_view(guild.id, submission)
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await message.edit(view=view)
            if view is not None:
                with contextlib.suppress(Exception):
                    self.bot.add_view(view, message_id=message.id)

    async def resume_public_confession_views(self):
        for guild_id in sorted(self._compiled_configs):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            await self._sync_published_confession_views(guild)

    async def sync_published_confession_views(self, guild: discord.Guild):
        await self._sync_published_confession_views(guild)

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
            if not config.get("panel_channel_id"):
                continue
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                continue
            await self._sync_member_panel(guild)

    async def sync_member_panel(self, guild: discord.Guild, *, channel_id: int | None = None) -> tuple[bool, str]:
        return await self._sync_member_panel(guild, channel_id=channel_id)

    async def _retire_review_queue(self, guild: discord.Guild, *, note: str | None = None):
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
        if submission.get("parent_confession_id"):
            identifiers_value += f"\nParent: `{submission['parent_confession_id']}`"
        embed = discord.Embed(
            title="Confession Detail",
            description="Anonymous moderation detail. Staff see the confession and case state, never the author.",
            color=ge.EMBED_THEME["info"],
        )
        embed.add_field(name="Identifiers", value=identifiers_value, inline=False)
        embed.add_field(
            name="State",
            value=(
                f"Type: **{self._submission_kind_label(submission)}**\n"
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
                f"Images: **{self._image_restriction_message(state) or 'Allowed'}**\n"
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
        if updated.get("image_restriction_case_id") == case_id:
            updated["image_restriction_active"] = False
            updated["image_restricted_until"] = None
            updated["image_restriction_case_id"] = None
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
            requires_review = bool(submission.get("submission_kind") == "reply" or attachment_meta or compiled["review_mode"])
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

        if action == "false_positive" and case.get("case_kind") == "review":
            publish_ok, message_id, channel_id, error = await self._publish_submission(guild, submission)
            if not publish_ok:
                return False, error or "Babblebox could not publish that confession."
            submission["status"] = "published"
            submission["review_status"] = "overridden"
            submission["posted_channel_id"] = channel_id
            submission["posted_message_id"] = message_id
            submission["published_at"] = now_iso
            submission["resolved_at"] = now_iso
            await self._scrub_submission_for_terminal_state(submission)
            case["status"] = "resolved"
            case["resolution_action"] = "false_positive"
            case["resolved_at"] = now_iso
            await self.store.upsert_case(case)
            if state.get("last_case_id") == case_id and (
                state.get("is_permanent_ban") or state.get("active_restriction") != "none" or int(state.get("strike_count") or 0) > 0
            ):
                await self.store.upsert_enforcement_state(
                    self._relax_case_penalty(state, case_id=case_id, now_iso=now_iso, clear_strikes=clear_strikes)
                )
            await self._sync_review_queue(guild, note=f"Case `{case_id}` was overridden and posted.")
            return True, f"Case `{case_id}` was overridden and posted as confession `{submission['confession_id']}`."

        if action == "clear" and case.get("case_kind") == "review":
            return False, "Use approve, deny, or false positive on a queued review case."

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

        if action == "false_positive":
            return False, "False positive is only available for automatic safety or review cases."

        if action in {"suspend", "temp_ban", "perm_ban", "clear", "restrict_images"}:
            if action == "perm_ban":
                state["is_permanent_ban"] = True
                state["active_restriction"] = "perm_ban"
                state["restricted_until"] = None
            elif action == "clear":
                state["is_permanent_ban"] = False
                state["active_restriction"] = "none"
                state["restricted_until"] = None
                state["image_restriction_active"] = False
                state["image_restricted_until"] = None
                state["image_restriction_case_id"] = None
                if clear_strikes:
                    state["strike_count"] = 0
            elif action == "restrict_images":
                state["image_restriction_active"] = True
                state["image_restricted_until"] = (now + timedelta(seconds=duration_seconds)).isoformat() if duration_seconds else None
                state["image_restriction_case_id"] = case_id
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
        if action == "restrict_images":
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
        source_opportunity = await self.store.fetch_owner_reply_opportunity_by_source_message_id(payload.guild_id, payload.message_id)
        if source_opportunity is not None and source_opportunity.get("status") in {"pending", "locked"}:
            await self._expire_owner_reply_opportunity(source_opportunity)
        submission = await self.store.fetch_submission_by_message_id(payload.guild_id, payload.message_id)
        if submission is None:
            return
        submission["status"] = "deleted"
        submission["posted_channel_id"] = None
        submission["posted_message_id"] = None
        submission["resolved_at"] = ge.now_utc().isoformat()
        await self.store.upsert_submission(submission)
        related_opportunities = await self.store.list_owner_reply_opportunities_for_submission(submission["submission_id"], limit=50)
        for opportunity in related_opportunities:
            if opportunity.get("status") in {"pending", "locked"}:
                await self._expire_owner_reply_opportunity(opportunity)

    async def handle_message_edit(self, message: discord.Message):
        guild = getattr(message, "guild", None)
        if guild is None:
            return
        opportunity = await self.store.fetch_owner_reply_opportunity_by_source_message_id(guild.id, int(getattr(message, "id", 0) or 0))
        if opportunity is None or opportunity.get("status") not in {"pending", "locked"}:
            return
        await self._expire_owner_reply_opportunity(opportunity)

    async def build_status_embed(self, guild: discord.Guild) -> discord.Embed:
        return await self.build_dashboard_embed(guild, section="overview")
