from __future__ import annotations

import asyncio
import contextlib
import difflib
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
    DEFAULT_SHIELD_AI_FAST_MODEL,
    SHIELD_AI_MIN_CONFIDENCE_CHOICES,
    SHIELD_AI_MODEL_ORDER,
    SHIELD_AI_REVIEW_PACKS,
    SHIELD_AI_SUPPORT_GUILD_ID,
    ShieldAIReviewRequest,
    ShieldAIReviewResult,
    build_shield_ai_provider,
    format_shield_ai_model,
    format_shield_ai_model_list,
    parse_shield_ai_model_list,
    sanitize_message_for_ai,
    shield_ai_available_in_guild,
    summarize_attachment_extensions,
)
from babblebox.shield_link_safety import (
    ADULT_LINK_CATEGORY,
    IMPERSONATION_LINK_CATEGORY,
    LINK_IN_BIO_DOMAINS,
    MALICIOUS_LINK_CATEGORY,
    MEDIA_EMBED_DOMAINS,
    SAFE_LINK_CATEGORY,
    SHORTENER_DOMAINS,
    SOCIAL_PROMO_DOMAINS,
    STOREFRONT_DOMAINS,
    TRUSTED_LINK_SAFE_FAMILIES,
    TRUSTED_MAINSTREAM_DOMAINS,
    UNKNOWN_LINK_CATEGORY,
    UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
    ShieldLinkAssessment,
    ShieldLinkSafetyEngine,
    domain_in_set as link_domain_in_set,
    domain_matches as link_domain_matches,
    extract_link_domain as link_extract_domain,
    is_trusted_destination,
    looks_like_warning_discussion,
    merge_link_assessments,
    normalize_link_host as link_normalize_link_host,
)
from babblebox.shield_store import (
    DEFAULT_SHIELD_SEVERE_CATEGORIES,
    DEFAULT_SHIELD_LINK_POLICY_MODE,
    LOW_CONFIDENCE_ACTIONS,
    MEDIUM_CONFIDENCE_ACTIONS,
    SHIELD_SEVERE_TERM_LIMIT,
    ShieldStateStore,
    ShieldStorageUnavailable,
    VALID_SHIELD_AI_ACCESS_MODES,
    VALID_SHIELD_SEVERE_CATEGORIES,
    VALID_SHIELD_LINK_POLICY_MODES,
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
    find_safety_term_hits,
    fold_confusable_text,
    is_harmful_context_suppressed,
    normalize_plain_text,
    sanitize_short_plain_text,
    squash_for_evasion_checks,
)
from babblebox.utility_helpers import deserialize_datetime, make_attachment_labels, make_message_preview


RULE_PACKS = ("privacy", "promo", "scam", "spam", "adult", "severe")
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
ALERT_SIGNATURE_DEDUP_SECONDS = 5.0
REPETITION_WINDOW_SECONDS = 10 * 60.0
LOW_CONFIDENCE_ALERT_COHORT_SECONDS = REPETITION_WINDOW_SECONDS
DIRECT_PROMO_REPEAT_THRESHOLD = 3
GENERIC_LINK_NOISE_THRESHOLD = 4
MEDIA_LINK_NOISE_THRESHOLD = 5
RUNTIME_PRUNE_INTERVAL_SECONDS = 60.0
FRESH_CAMPAIGN_WINDOW_SECONDS = 30 * 60.0
FRESH_CAMPAIGN_TIGHT_WINDOW_SECONDS = 20 * 60.0
SPAM_EVENT_WINDOW_SECONDS = 120.0
SPAM_EVENT_LIMIT_PER_USER = 12
SPAM_EXACT_WINDOW_SECONDS = 90.0
SPAM_NEAR_WINDOW_SECONDS = 120.0
SPAM_BURST_WINDOW_SECONDS = 12.0
SPAM_LINK_WINDOW_SECONDS = 45.0
SPAM_INVITE_WINDOW_SECONDS = 60.0
SPAM_MENTION_WINDOW_SECONDS = 20.0
SPAM_LOW_VALUE_WINDOW_SECONDS = 60.0
SPAM_GIF_WINDOW_SECONDS = 45.0
SPAM_GIF_REPEAT_WINDOW_SECONDS = 30.0
HEALTHY_CHAT_WINDOW_SECONDS = 20.0
HEALTHY_CHAT_AUTHOR_THRESHOLD = 4
GIF_EMBED_DOMAINS = frozenset({"tenor.com", "media.tenor.com", "giphy.com", "media.giphy.com"})
RAID_JOIN_SHORT_WINDOW_SECONDS = 60.0
RAID_JOIN_LONG_WINDOW_SECONDS = 5 * 60.0
RAID_JOIN_WINDOW_SECONDS = RAID_JOIN_LONG_WINDOW_SECONDS
RAID_JOIN_EVENT_LIMIT = 128
RAID_PATTERN_WINDOW_SECONDS = 10 * 60.0
RAID_PATTERN_LIMIT = 256
RAID_PATTERN_USERS_PER_SIGNATURE_LIMIT = 12
RAID_ALERT_DEDUP_SECONDS = 120.0
RECENT_ACCOUNT_WINDOW = timedelta(days=7)
EARLY_MEMBER_WINDOW = timedelta(days=1)
NEWCOMER_ACTIVITY_TTL_SECONDS = 6 * 3600.0
NEWCOMER_MESSAGE_WINDOW = 3
CAMPAIGN_SIGNATURE_LIMIT = 256
CAMPAIGN_USERS_PER_SIGNATURE_LIMIT = 12
NEWCOMER_STATE_LIMIT = 512
SHIELD_BASELINE_VERSION = 2
TRUSTED_ONLY_BUILTIN_FAMILIES = frozenset(TRUSTED_LINK_SAFE_FAMILIES)
TRUSTED_ONLY_BUILTIN_DOMAINS = frozenset(TRUSTED_MAINSTREAM_DOMAINS)
AUTOMATED_AUTHOR_KINDS = frozenset({"bot", "webhook"})

PACK_LABELS = {
    "privacy": "Privacy Leak",
    "promo": "Promo / Invite",
    "scam": "Scam / Malicious Links",
    "spam": "Spam / Raid",
    "adult": "Adult Links + Solicitation",
    "severe": "Severe Harm / Hate",
    "link_policy": "Link Policy",
    "advanced": "Advanced Pattern",
}
MATCH_CLASS_LABELS = {
    "discord_invite": "Discord invite",
    "self_promo": "Self-promo link",
    "monetized_promo": "Monetized promo",
    "cta_promo": "Call-to-action promo",
    "repetitive_link_noise": "Repeated link pattern",
    "known_malicious_domain": "Known malicious domain",
    "trusted_brand_impersonation_domain": "Trusted-brand impersonation domain",
    "adult_domain": "Known adult domain",
    "adult_dm_ad": "Adult-content DM ad",
    "adult_solicitation": "Sexual solicitation",
    "sexual_exploitation_solicitation": "Sexual exploitation solicitation",
    "self_harm_encouragement": "Self-harm encouragement",
    "eliminationist_hate": "Eliminationist hate",
    "severe_slur_abuse": "Severe slur abuse",
    "targeted_extreme_degradation": "Extreme degrading abuse",
    "scam_bait_attachment": "Scam bait + suspicious file",
    "scam_campaign_lure": "Weighted scam campaign lure",
    "scam_risky_unknown_link": "Risky unknown-link lure",
    "scam_brand_impersonation": "Brand impersonation lure",
    "scam_mint_wallet_lure": "Mint or wallet lure",
    "untrusted_external_link": "Untrusted external link",
    "untrusted_invite_link": "Untrusted invite link",
    "blocked_link_hub": "Blocked link hub or storefront",
    "spam_duplicate": "Repeated duplicate spam",
    "spam_near_duplicate": "Repeated near-duplicate spam",
    "spam_link_flood": "Repeated link flood",
    "spam_invite_flood": "Repeated invite flood",
    "spam_mention_flood": "Mention flood",
    "spam_emoji_flood": "Emoji flood",
    "spam_gif_flood": "GIF flood",
    "spam_burst": "Fast burst posting",
    "spam_low_value_noise": "Repeated low-value noise",
    "spam_padding_noise": "Character-padding spam",
    "raid_watch": "Raid watch",
    "raid_pattern_cluster": "Coordinated newcomer pattern",
    "link_policy_malicious": "Known malicious domain",
    "link_policy_impersonation": "Trusted-brand impersonation domain",
    "link_policy_adult": "Known adult domain",
    "link_policy_suspicious": "Suspicious external link",
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
PACK_STRENGTH = {"privacy": 1, "spam": 1, "promo": 2, "link_policy": 2, "scam": 3, "adult": 3, "severe": 4, "advanced": 5}
AI_PRIORITY_LABELS = {"low": "Low", "normal": "Normal", "high": "High"}
AI_REVIEW_PACK_SET = frozenset(SHIELD_AI_REVIEW_PACKS)
SCAN_SOURCE_LABELS = {
    "new_message": "New message",
    "message_edit": "Edited message",
    "webhook_message": "Webhook or community post",
}
ALLOW_PHRASE_SUPPRESSED_MATCH_CLASSES = frozenset(
    {
        "discord_invite",
        "self_promo",
        "monetized_promo",
        "cta_promo",
        "adult_dm_ad",
        "adult_solicitation",
    }
)

INVITE_RE = re.compile(r"(?i)(?:https?://)?(?:www\.)?(?:discord(?:app)?\.com/invite|discord\.gg)/([a-z0-9-]{2,32})")
MENTION_RE = re.compile(r"<@!?(\d+)>|<@&(\d+)>")
EVERYONE_HERE_RE = re.compile(r"(?i)@(everyone|here)\b")
CUSTOM_EMOJI_RE = re.compile(r"<a?:[A-Za-z0-9_]{2,32}:\d+>")
UNICODE_EMOJI_RE = re.compile("[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U000024C2-\U0001F251]")
WORD_TOKEN_RE = re.compile(r"[a-z0-9']+")
PADDED_RUN_RE = re.compile(r"(.)\1{5,}")
SEPARATOR_COLLAPSE_RE = re.compile(r"[\s\-_~`=|]+")
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
ADULT_SOLICIT_CONTACT_RE = re.compile(r"(?i)\b(?:dm me|dm us|message me|message us|msg me|msg us|pm me|pm us|inbox me|inbox us|hit me up)\b")
ADULT_SOLICIT_DM_GATE_RE = re.compile(r"(?i)\b(?:more in dms?|dm for|message for|ask in dms?|ask me in dms?)\b")
ADULT_SOLICIT_SALE_RE = re.compile(
    r"(?i)\b(?:sell(?:ing)?|buy|paid|pay|prices?|menu|subscribe|purchase|order|customs? open|taking requests|requests open|open for requests?)\b"
)
ADULT_SOLICIT_WEAK_OFFER_RE = re.compile(r"(?i)\b(?:available|open|taking requests|open for requests?)\b")
ADULT_SOLICIT_DISAPPROVAL_RE = re.compile(
    r"(?i)\b(?:don['’]?t say|stop posting|not allowed|against the rules|rule violation|banned phrase|keep that out)\b"
)
ADULT_SOLICIT_STRONG_DISAPPROVAL_RE = re.compile(
    r"(?i)\b(?:don['']?t say|stop posting|not allowed|against (?:the )?(?:rules|policy)|rule violation|policy violation|banned phrase|keep that out)\b"
)
ADULT_SOLICIT_STRONG_TERMS = frozenset(
    {
        "18+",
        "adult content",
        "lewd",
        "lewds",
        "nsfw",
        "nude",
        "nudes",
        "only fans",
        "onlyfans",
        "porn",
        "sexting",
    }
)
ADULT_SOLICIT_EUPHEMISM_TERMS = frozenset({"custom content", "customs", "paid pics", "paid vids"})
ADULT_SOLICIT_OF_SIGNAL_RE = re.compile(r"(?<![A-Za-z])(?:OF|O\.F\.)(?![A-Za-z])")
ADULT_SOLICIT_PRODUCT_TERMS = frozenset({"content", "menu", "photo", "photos", "pic", "pics", "price", "prices", "vid", "video", "videos", "vids"})
ADULT_SOLICIT_DIRECT_ROUTE_RE = re.compile(
    r"(?i)\b(?:dm(?:s)?(?: me)?|message me|msg me|pm me|inbox me|send me|show me)\b.{0,18}\b(?:nudes?|lewds?|pics?|photos?|vids?|videos?|onlyfans|only fans|menu|prices?|custom content|customs?|18\+)\b"
)
ADULT_SOLICIT_OPEN_DM_RE = re.compile(
    r"(?i)\b(?:my\s+)?dms?\s+(?:are\s+)?open\b.{0,18}\b(?:nudes?|lewds?|pics?|photos?|vids?|videos?|menu|prices?|custom content|customs?|requests?|18\+)\b"
)
ADULT_SOLICIT_DM_DESTINATION_RE = re.compile(
    r"(?i)\b(?:nudes?|lewds?|pics?|photos?|vids?|videos?|menu|prices?|custom content|customs?|18\+)\b.{0,12}\b(?:in|via|through)\s+dms?\b"
)
ADULT_SOLICIT_DM_MENU_PRICE_RE = re.compile(r"(?i)\b(?:menu|prices?)\b.{0,12}\b(?:in|via|through)\s+dms?\b")
ADULT_SOLICIT_SALES_OFFER_RE = re.compile(
    r"(?i)\b(?:selling|buy|paid|pay|menu|prices?|subscribe|order)\b.{0,18}\b(?:nudes?|lewds?|pics?|photos?|vids?|videos?|onlyfans|only fans|custom content|customs?|18\+)\b"
)
ADULT_SOLICIT_BENIGN_PHOTO_RE = re.compile(
    r"(?i)\b(?:art|camera|cat|cats|concert|dog|dogs|event|meme|memes|outfit|pet|pets|photo(?:graphy)?|receipt|reference|screenshot|screenshots|setup|travel|vacation|wedding)\b"
)
ADULT_SOLICIT_BENIGN_MENU_RE = re.compile(
    r"(?i)\b(?:art|artist|bakery|bar|brunch|cake|cafe|catering|coffee|commission|design|dinner|drink|drinks|food|hair|lunch|menu planning|nails|photo(?:graphy)?|portfolio|restaurant|salon|service|services|spa|tattoo)\b"
)
SEVERE_CATEGORY_LABELS = {
    "sexual_exploitation": "Sexual Exploitation",
    "self_harm_encouragement": "Self-Harm Encouragement",
    "eliminationist_hate": "Eliminationist Hate",
    "severe_slur_abuse": "Severe Slur Abuse",
}
SEVERE_TERM_MAX_LEN = 60
SEVERE_CHILD_ABUSE_TERMS = frozenset(
    {"child porn", "child pornography", "child nudes", "cp", "csam", "csem", "minor nudes", "minor porn", "underage porn"}
)
SEVERE_CHILD_ABUSE_ROUTING_RE = re.compile(
    r"(?i)\b(?:dm(?:s)?(?: me)?|message me|msg me|pm me|inbox me|telegram|trade|trading|sell(?:ing)?|buy|prices?|menu|collection|folder|drop|dropbox|send)\b"
)
SEVERE_SELF_HARM_LOW_TERMS = frozenset({"end your life", "kill yourself", "kys", "off yourself"})
SEVERE_SELF_HARM_NORMAL_TERMS = frozenset({"drink bleach", "go die", "hang yourself", "slit your wrists", "you should die"})
SEVERE_SELF_HARM_HIGH_TERMS = frozenset({"nobody would miss you if you died", "the world would be better if you were dead"})
SEVERE_SELF_HARM_NEGATION_RE = re.compile(
    r"(?i)\b(?:do not|don't|dont|never|please don't|please do not)\b.{0,10}\b(?:kill yourself|kys|off yourself|end your life|go die|you should die)\b"
)
SEVERE_SUPPORT_CONTEXT_RE = re.compile(r"(?i)\b(?:call 988|crisis line|hotline|please stay alive|suicide prevention|support)\b")
SEVERE_PROTECTED_GROUP_RE = re.compile(
    r"(?i)\b(?:asians?|black people|blacks|disabled people|gays?|immigrants?|jews?|latinos?|lesbians?|mexicans?|muslims?|queers?|trans(?: people)?|transgender(?: people)?)\b"
)
SEVERE_ELIMINATION_RE = re.compile(
    r"(?i)\b(?:eradicate|exterminate|gas|kill all|lynch|purge|should die|should not exist|wipe out|wipe them out)\b"
)
SEVERE_DEHUMANIZING_RE = re.compile(r"(?i)\b(?:animals|cockroaches|filth|parasites|subhuman|vermin)\b")
SEVERE_TARGETING_RE = re.compile(
    r"(?i)\b(?:you|your|you['’]re|you are|u|ur|he|she|they|them|that person|this person|mods?|admins?|member|user)\b"
)
SEVERE_SLUR_LOW_TERMS = frozenset({"chink", "faggot", "gook", "kike", "n1gg3r", "n1gg@", "n1gga", "n1gger", "nigga", "nigger", "spic", "wetback"})
SEVERE_SLUR_NORMAL_TERMS = frozenset({"coon", "cripple", "dyke", "mongoloid", "paki", "retard", "tranny"})
SEVERE_DEGRADING_TERMS = frozenset({"human garbage", "subhuman", "vermin", "worthless trash"})
SEVERE_REMOVABLE_DEFAULT_TERMS = frozenset(
    set(SEVERE_CHILD_ABUSE_TERMS)
    | set(SEVERE_SELF_HARM_LOW_TERMS)
    | set(SEVERE_SELF_HARM_NORMAL_TERMS)
    | set(SEVERE_SELF_HARM_HIGH_TERMS)
    | set(SEVERE_SLUR_LOW_TERMS)
    | set(SEVERE_SLUR_NORMAL_TERMS)
)
SOCIAL_ENGINEERING_RE = re.compile(
    r"(?i)\b(?:download|run|install|open|visit|click(?: here)?|verify|claim|login|log in|sign in|connect wallet|wallet connect|sync|mint|minting|authenticate|authorize)\b"
)
SCAM_BAIT_RE = re.compile(
    r"(?i)\b(?:free nitro|nitro gift|steam gift|claim reward|claim now|verify your account|wallet connect|seed phrase|airdrop|gift inventory|limited time claim|free mint|mint opportunity|minting page|whitelist spot)\b"
)
BRAND_BAIT_RE = re.compile(r"(?i)\b(?:discord|nitro|steam|epic|wallet|crypto|gift|opensea|metamask|coinbase|walletconnect)\b")
SCAM_CTA_RE = re.compile(
    r"(?i)\b(?:visit|open|click(?: here)?|go to|claim|verify|secure|connect wallet|wallet connect|login|log in|sign in|sync|mint|minting|authorize|authenticate)\b"
)
SCAM_URGENCY_RE = re.compile(
    r"(?i)\b(?:limited|limited spots|spots are limited|selection is limited|soon|act now|ending soon|expires|while it lasts|last chance|today only|secure your spot|first come)\b"
)
SCAM_OFFICIAL_FRAMING_RE = re.compile(r"(?i)\b(?:official|verified|trusted)\b")
SCAM_ANNOUNCEMENT_RE = re.compile(r"(?i)\b(?:announcement|community post|server update|news update)\b")
SCAM_PARTNERSHIP_RE = re.compile(r"(?i)\b(?:partnership|partnered|collaboration)\b")
SCAM_SUPPORT_RE = re.compile(r"(?i)\b(?:support|help\s*desk|helpdesk|ticket|case(?:\s*#\d+)?|service\s*desk)\b")
SCAM_SECURITY_NOTICE_RE = re.compile(
    r"(?i)\b(?:security (?:alert|check|review|notice)|session (?:expired|review|check|validation)|unusual activity|suspicious activity|account (?:locked|recovery|suspension)|password reset|re-authenticate|reauthenticate|device verification)\b"
)
SCAM_FAKE_AUTHORITY_RE = re.compile(
    r"(?i)\b(?:official bot|support bot|verification bot|security bot|system (?:message|notice)|staff(?: team)?|mod(?:erator)?(?: team)?|admin(?: team)?)\b"
)
SCAM_QR_SETUP_RE = re.compile(
    r"(?i)\b(?:qr(?:\s*code)?|scan the qr|scan to verify|device auth|pair your device|captcha|setup|installer|installation package)\b"
)
SCAM_LEGITIMACY_RE = re.compile(
    r"(?i)\b(?:official|partnership|partnered|community post|announcement|members? (?:are )?invited|verified|trusted|minting page|claim page|verification page)\b"
)
SCAM_CRYPTO_MINT_RE = re.compile(
    r"(?i)\b(?:mint|minting|wallet|wallet connect|connect wallet|airdrop|nft|token|whitelist|wl|opensea|seed phrase)\b"
)
SCAM_LOGIN_FLOW_RE = re.compile(
    r"(?i)\b(?:verify|verification|login|log in|sign in|authenticate|authorize|sync|secure)\b"
)
SUSPICIOUS_FILE_RE = re.compile(r"(?i)\.(?:exe|scr|bat|cmd|msi|zip|rar|7z|iso)(?:$|[?#])")
GENERIC_DIGIT_RE = re.compile(r"\b\d{4,12}\b")
BARE_URL_NUMERIC_RE = re.compile(r"(?i)^v?\d+(?:\.\d+){1,5}(?:[-_/][a-z0-9._-]+)?$")
BARE_URL_TLD_RE = re.compile(r"(?i)(?:xn--[a-z0-9-]{2,59}|[a-z]{2,24})$")
BARE_URL_MAX_TOKENS = 64
BARE_URL_MAX_LENGTH = 180
BARE_URL_FILENAME_TLDS = frozenset({"7z", "apk", "bat", "cmd", "exe", "iso", "msi", "rar", "scr", "zip"})
BARE_URL_AMBIGUOUS_FILE_TLDS = frozenset(
    {
        "aac",
        "ai",
        "avi",
        "bmp",
        "css",
        "csv",
        "doc",
        "docm",
        "docx",
        "eps",
        "gif",
        "heic",
        "html",
        "ico",
        "java",
        "jpeg",
        "jpg",
        "js",
        "json",
        "m4a",
        "md",
        "mid",
        "midi",
        "mkv",
        "mov",
        "mp3",
        "mp4",
        "ogg",
        "pdf",
        "php",
        "png",
        "ppt",
        "pptx",
        "psd",
        "py",
        "rtf",
        "svg",
        "tar",
        "tif",
        "tiff",
        "ts",
        "txt",
        "wav",
        "webm",
        "webp",
        "wma",
        "wmv",
        "xls",
        "xlsm",
        "xlsx",
        "xml",
        "yaml",
        "yml",
    }
)
CAMPAIGN_HOST_FAMILY_MAP = {
    "auth": frozenset({"auth", "login", "secure", "security", "session", "verify", "verification", "token"}),
    "reward": frozenset({"bonus", "claim", "drop", "gift", "inventory", "promo", "reward"}),
    "wallet": frozenset({"airdrop", "connect", "mint", "seed", "wallet"}),
    "support": frozenset({"case", "help", "helpdesk", "support", "ticket"}),
    "setup": frozenset({"captcha", "device", "download", "installer", "qr", "setup", "update"}),
}
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
    trusted_builtin_disabled_families: frozenset[str]
    trusted_builtin_disabled_domains: frozenset[str]
    privacy: PackSettings
    promo: PackSettings
    scam: PackSettings
    spam: PackSettings
    adult: PackSettings
    adult_solicitation_enabled: bool
    adult_solicitation_excluded_channel_ids: frozenset[int]
    severe: PackSettings
    severe_enabled_categories: frozenset[str]
    severe_custom_terms: tuple[str, ...]
    severe_removed_terms: frozenset[str]
    link_policy_mode: str
    link_policy: PackSettings
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
        if pack == "spam":
            return self.spam
        if pack == "adult":
            return self.adult
        if pack == "severe":
            return self.severe
        if pack == "link_policy":
            return self.link_policy
        return PackSettings(enabled=True, low_action="log", medium_action="log", high_action="log", sensitivity="normal")


@dataclass(frozen=True)
class ShieldAIAccessPolicy:
    guild_id: int
    enabled: bool
    source: str
    support_default: bool
    allowed_models: tuple[str, ...]
    ordinary_global_enabled: bool
    ordinary_global_allowed_models: tuple[str, ...]
    guild_access_mode: str
    guild_allowed_models_override: tuple[str, ...]
    updated_by: int | None
    updated_at: str | None


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
    scan_text: str
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
    mention_count: int = 0
    everyone_here_count: int = 0
    emoji_count: int = 0
    plain_word_count: int = 0
    low_value_text: bool = False
    repeated_char_run: int = 0
    is_gif_message: bool = False
    gif_signature: str | None = None
    gif_only: bool = False
    gif_low_text: bool = False
    exact_fingerprint: str | None = None
    near_duplicate_text: str = ""
    near_duplicate_fingerprint: str | None = None
    surface_labels: tuple[str, ...] = ()


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
    scan_source: str = "new_message"
    scan_surface_labels: tuple[str, ...] = ()
    member_risk_evidence: "ShieldMemberRiskEvidence | None" = None
    alert_evidence_signature: str | None = None
    alert_evidence_summary: str | None = None
    raid_evidence: "ShieldRaidEvidence | None" = None


@dataclass(frozen=True)
class ShieldMemberRiskEvidence:
    message_codes: tuple[str, ...]
    context_codes: tuple[str, ...]
    signal_codes: tuple[str, ...]
    message_match_class: str | None = None
    message_confidence: str | None = None
    scan_source: str = "new_message"
    primary_domain: str | None = None


@dataclass
class ShieldNewcomerActivityState:
    first_seen_at: float
    last_seen_at: float
    message_count: int = 0
    external_link_messages: int = 0


@dataclass(frozen=True)
class ShieldScamContext:
    author_kind: str = "human"
    primary_domain: str | None = None
    recent_account: bool = False
    early_member: bool = False
    newcomer_early_message: bool = False
    first_message_with_link: bool = False
    first_external_link: bool = False
    early_risky_activity: bool = False
    fresh_campaign_cluster_20m: int = 0
    fresh_campaign_cluster_30m: int = 0
    fresh_campaign_kinds: tuple[str, ...] = ()


@dataclass(frozen=True)
class ShieldSpamEvent:
    timestamp: float
    exact_fingerprint: str | None
    near_text: str
    near_fingerprint: str | None
    has_links: bool
    link_signature: str | None
    media_only_links: bool
    invite_codes: frozenset[str]
    mention_count: int
    everyone_here_count: int
    emoji_count: int
    plain_word_count: int
    low_value_text: bool
    repeated_char_run: int
    is_gif_message: bool = False
    gif_signature: str | None = None
    gif_only: bool = False
    gif_low_text: bool = False


@dataclass(frozen=True)
class ShieldRaidEvidence:
    join_count_60s: int = 0
    join_count_5m: int = 0
    fresh_join_count_60s: int = 0
    fresh_join_count_5m: int = 0
    join_wave: bool = False
    fresh_join_wave: bool = False
    pattern_cluster_size: int = 0
    pattern_kind: str | None = None
    pattern_signature: str | None = None
    confirmed: bool = False
    confidence_lifted: bool = False


@dataclass(frozen=True)
class ShieldScamFeatures:
    author_kind: str
    link_risk_score: int
    suspicious_link_present: bool
    risky_link_present: bool
    shortener_or_punycode: bool
    bait: bool
    social_engineering: bool
    cta: bool
    brand_bait: bool
    official_framing: bool
    announcement_framing: bool
    partnership_framing: bool
    support_framing: bool
    security_notice: bool
    fake_authority: bool
    qr_setup_lure: bool
    community_post_framing: bool
    urgency: bool
    wallet_or_mint: bool
    login_or_auth_flow: bool
    dangerous_link_target: bool
    suspicious_attachment_combo: bool
    scan_source: str
    newcomer_early_message: bool
    first_message_with_link: bool
    first_external_link: bool
    early_risky_activity: bool
    fresh_campaign_cluster_20m: int
    fresh_campaign_cluster_30m: int
    fresh_campaign_kinds: tuple[str, ...]

    @property
    def official_like_framing(self) -> bool:
        return (
            self.official_framing
            or self.announcement_framing
            or self.partnership_framing
            or self.fake_authority
            or self.community_post_framing
        )

    @property
    def automated_author(self) -> bool:
        return self.author_kind in AUTOMATED_AUTHOR_KINDS


@dataclass(frozen=True)
class ShieldTestResult:
    matches: tuple[ShieldMatch, ...]
    link_assessments: tuple[ShieldLinkAssessment, ...]
    bypass_reason: str | None = None


@dataclass(frozen=True)
class ShieldFeatureDecision:
    allowed: bool
    surface: str
    reason_code: str | None
    user_message: str | None
    matches: tuple[ShieldMatch, ...] = ()
    link_assessments: tuple[ShieldLinkAssessment, ...] = ()


@dataclass(frozen=True)
class ShieldFeatureLinkScan:
    surface: str
    has_links: bool
    flags: tuple[str, ...]
    link_assessments: tuple[ShieldLinkAssessment, ...]


@dataclass(frozen=True)
class RepetitionSignals:
    fingerprint: str | None
    hits: int
    pure_media_links: bool
    has_unallowlisted_links: bool
    evidence_kind: str = ""


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


def _domain_in_set(domain: str, candidates: frozenset[str] | set[str]) -> bool:
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
    return link_normalize_link_host(raw_host)


def _extract_domain(raw_url: str) -> str | None:
    return link_extract_domain(raw_url)


def _looks_like_bare_url_candidate(raw_token: str) -> str | None:
    candidate = (raw_token or "").strip().strip("()[]{}<>,.!?\"'")
    if not candidate or len(candidate) > BARE_URL_MAX_LENGTH:
        return None
    lowered = candidate.casefold()
    if "://" in lowered or lowered.startswith("www.") or "@" in lowered or lowered.count(".") < 1:
        return None
    if BARE_URL_NUMERIC_RE.fullmatch(lowered):
        return None
    domain = _extract_domain(candidate)
    if domain is None:
        return None
    labels = [label for label in domain.split(".") if label]
    if len(labels) < 2:
        return None
    tld = labels[-1]
    has_pathish_suffix = any(char in candidate for char in "/?#")
    if not has_pathish_suffix and tld in BARE_URL_AMBIGUOUS_FILE_TLDS:
        return None
    if not has_pathish_suffix and (tld in BARE_URL_FILENAME_TLDS or BARE_URL_TLD_RE.fullmatch(tld) is None):
        return None
    return candidate


def _extract_urls(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    extracted: list[str] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str | None):
        if raw_value and raw_value not in seen:
            seen.add(raw_value)
            extracted.append(raw_value)

    for match in URL_RE.finditer(text):
        add_candidate(match.group(0))
    for token in text.split()[:BARE_URL_MAX_TOKENS]:
        add_candidate(_looks_like_bare_url_candidate(token))
    return tuple(extracted)


def _strip_urls_from_text(text: str, urls: Sequence[str]) -> str:
    if not text:
        return ""
    stripped = text
    for url in urls:
        stripped = stripped.replace(url, " ")
    return re.sub(r"\s+", " ", stripped).strip()


def _count_mentions(text: str) -> tuple[int, int]:
    direct_mentions = sum(1 for _ in MENTION_RE.finditer(text or ""))
    everyone_here = sum(1 for _ in EVERYONE_HERE_RE.finditer(text or ""))
    return direct_mentions + everyone_here, everyone_here


def _count_emojis(text: str) -> int:
    if not text:
        return 0
    return len(CUSTOM_EMOJI_RE.findall(text)) + len(UNICODE_EMOJI_RE.findall(text))


def _plain_word_tokens(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    return tuple(WORD_TOKEN_RE.findall(text))


def _max_repeated_char_run(text: str) -> int:
    best = 0
    for match in PADDED_RUN_RE.finditer(text or ""):
        best = max(best, len(match.group(0)))
    return best


def _is_low_value_text(text: str, *, plain_word_count: int) -> bool:
    if not text:
        return False
    tokens = _plain_word_tokens(text)
    if not tokens:
        return False
    joined = "".join(tokens)
    unique_tokens = len(set(tokens))
    unique_chars = len(set(joined))
    return (
        plain_word_count <= 4
        and len(joined) <= 18
        and (unique_tokens <= 2 or unique_chars <= 4)
    )


def _normalize_near_duplicate_text(text: str, urls: Sequence[str]) -> str:
    if not text:
        return ""
    cleaned = fold_confusable_text(normalize_plain_text(_strip_urls_from_text(text, urls))).casefold()
    cleaned = re.sub(r"(.)\1{2,}", r"\1", cleaned)
    cleaned = SEPARATOR_COLLAPSE_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\b\d{3,}\b", "[n]", cleaned)
    return cleaned.strip()


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


def _alert_content_fingerprint(snapshot: ShieldSnapshot) -> str:
    fingerprint = _canonical_repetition_fingerprint(snapshot)
    if fingerprint is not None:
        return fingerprint
    content = "|".join(
        (
            snapshot.text,
            " ".join(snapshot.canonical_links),
            " ".join(snapshot.attachment_names),
        )
    )
    return hashlib.sha1(content.encode("utf-8")).hexdigest()


def _link_repetition_kind(links: Sequence[ShieldLink]) -> str:
    if not links:
        return ""
    categories = {link.category for link in links}
    if categories == {"media_embed"}:
        return "media_link"
    if categories == {"discord_invite"}:
        return "invite_link"
    return "external_link"


def _link_repetition_fingerprint(links: Sequence[ShieldLink]) -> tuple[str | None, str]:
    if not links:
        return None, ""
    kind = _link_repetition_kind(links)
    canonical_links = tuple(sorted({link.canonical_url for link in links}))
    if not canonical_links or not kind:
        return None, ""
    joined = "|".join((kind, *canonical_links))
    return hashlib.sha1(joined.encode("utf-8")).hexdigest(), kind


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
        if any(signal.startswith("external_malicious_domain_") for signal in assessment.matched_signals):
            return "matched external malicious intel"
        if any(signal.startswith("bundled_malicious_domain_") for signal in assessment.matched_signals):
            return "matched bundled malicious intel"
        return "matched local malicious intel"
    if assessment.category == IMPERSONATION_LINK_CATEGORY:
        return "matched local trusted-brand impersonation intel"
    if assessment.category == ADULT_LINK_CATEGORY:
        return "matched local adult intel"
    if assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
        return "lookup candidate; link-only caution" if assessment.provider_lookup_warranted else "local caution; link-only"
    if assessment.category == UNKNOWN_LINK_CATEGORY:
        return "unknown, no action"
    if "guild_allow_domain" in assessment.matched_signals:
        return "safe family; admin allowlisted"
    return "safe family"


def _link_assessment_basis(assessment: ShieldLinkAssessment) -> str:
    allowlist_note = " Admin allowlists do not override risky-link intel." if "guild_allow_domain" in assessment.matched_signals else ""
    if assessment.category == MALICIOUS_LINK_CATEGORY:
        if any(signal.startswith("external_malicious_domain_") for signal in assessment.matched_signals):
            return f"Hard intel from the external malicious-domain feed.{allowlist_note}"
        if any(signal.startswith("bundled_malicious_domain_") for signal in assessment.matched_signals):
            return f"Hard intel from the bundled malicious-domain feed.{allowlist_note}"
        return f"Hard intel from local malicious-domain intelligence.{allowlist_note}"
    if assessment.category == IMPERSONATION_LINK_CATEGORY:
        return f"Hard local spoof-domain intel for a trusted-brand lookalike host.{allowlist_note}"
    if assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
        return f"Combined suspicion around an unknown risky link.{allowlist_note}"
    if assessment.category == ADULT_LINK_CATEGORY:
        return f"Hard intel from the adult-domain list.{allowlist_note}"
    return "No risky-link intel matched."


def _scan_source_for_message(message: discord.Message, *, default: str = "new_message") -> str:
    if default == "new_message" and getattr(message, "webhook_id", None) is not None:
        return "webhook_message"
    return default


def _author_kind_for_message(message: discord.Message, *, scan_source: str) -> str:
    if scan_source == "webhook_message" or getattr(message, "webhook_id", None) is not None:
        return "webhook"
    if getattr(getattr(message, "author", None), "bot", False):
        return "bot"
    return "human"


def _score_link_assessment_for_scam(assessment: ShieldLinkAssessment) -> int:
    if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY}:
        return 5
    if assessment.category != UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
        return 0
    score = 3
    signals = assessment.matched_signals
    if any(signal in {"punycode_host", "hyphen_heavy_host", "shortener_domain", "deep_subdomain_stack"} for signal in signals):
        score += 1
    if any(
        signal.startswith("suspicious_tld:")
        or signal.startswith("host_token:")
        or signal.startswith("brand_token:")
        or signal.startswith("embedded_host_token:")
        or signal.startswith("embedded_brand_token:")
        for signal in signals
    ):
        score += 1
    if any(signal.startswith("path_token:") or signal.startswith("query_token:") or signal == "encoded_or_long_query" for signal in signals):
        score += 1
    if assessment.provider_lookup_warranted:
        score += 1
    return score


def _strongest_link_risk_signals(link_assessments: Sequence[ShieldLinkAssessment], *, limit: int = 4) -> tuple[str, ...]:
    signals: list[str] = []
    ranked = sorted(link_assessments, key=_score_link_assessment_for_scam, reverse=True)
    for assessment in ranked:
        for signal in assessment.matched_signals:
            if signal.startswith("safe_family:") or signal == "guild_allow_domain":
                continue
            if signal not in signals:
                signals.append(signal)
            if len(signals) >= limit:
                return tuple(signals)
    return tuple(signals)


def _member_datetime(value: Any):
    if value is None:
        return None
    if hasattr(value, "tzinfo"):
        return value
    try:
        return deserialize_datetime(value)
    except Exception:
        return None


def _campaign_kind_label(kind: str) -> str:
    labels = {
        "domain": "shared risky domain",
        "path_shape": "shared risky link shape",
        "host_family": "shared risky host pattern",
        "lure": "reused lure wording",
    }
    return labels.get(kind, kind.replace("_", " "))


def _cluster_reason_text(kinds: Sequence[str]) -> str:
    ordered = [_campaign_kind_label(kind) for kind in kinds if kind]
    if not ordered:
        return "fresh-account campaign cluster"
    if len(ordered) == 1:
        return ordered[0]
    return ", ".join(ordered[:2])


def _path_query_shape_signature(assessment: ShieldLinkAssessment | None, *, domain: str | None) -> str | None:
    if assessment is None or not domain:
        return None
    tags: list[str] = []
    for signal in assessment.matched_signals:
        if signal.startswith("path_token:"):
            tags.append(f"path:{signal.split(':', 1)[1]}")
        elif signal.startswith("query_token:"):
            tags.append(f"query:{signal.split(':', 1)[1]}")
        elif signal in {"encoded_or_long_query", "suspicious_file_target"}:
            tags.append(signal)
    deduped = tuple(dict.fromkeys(tags))
    if not deduped:
        return None
    return f"{domain}|{'|'.join(deduped[:4])}"


def _host_family_signature(assessment: ShieldLinkAssessment | None, *, domain: str | None) -> str | None:
    if assessment is None or not domain:
        return None
    tokens: set[str] = set()
    for signal in assessment.matched_signals:
        for prefix in ("host_token:", "embedded_host_token:", "path_token:", "query_token:"):
            if signal.startswith(prefix):
                tokens.add(signal.split(":", 1)[1])
    families = [
        family
        for family, family_tokens in CAMPAIGN_HOST_FAMILY_MAP.items()
        if tokens.intersection(family_tokens)
    ]
    if not families:
        return None
    return "|".join(sorted(families)[:3])


def _scam_lure_fingerprint(text: str) -> str | None:
    cleaned = normalize_plain_text(text).casefold()
    if len(cleaned) < 16:
        return None
    normalized = cleaned
    replacements = (
        (BRAND_BAIT_RE, "[brand]"),
        (SCAM_CTA_RE, "[cta]"),
        (SCAM_LOGIN_FLOW_RE, "[auth]"),
        (SCAM_LEGITIMACY_RE, "[official]"),
        (SCAM_URGENCY_RE, "[urgency]"),
        (SCAM_CRYPTO_MINT_RE, "[wallet]"),
        (re.compile(r"\b\d+\b"), "[n]"),
    )
    for pattern, replacement in replacements:
        normalized = pattern.sub(replacement, normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if len(normalized) < 16:
        return None
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


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


def _surface_text_or_none(value: Any) -> str | None:
    cleaned = normalize_plain_text(str(value or ""))
    return cleaned or None


def _append_surface_text(parts: list[str], value: Any):
    cleaned = _surface_text_or_none(value)
    if cleaned:
        parts.append(cleaned)


def _collect_attachment_surface_texts(attachments: Sequence[Any] | None) -> tuple[tuple[str, ...], tuple[str, ...]]:
    parts: list[str] = []
    link_parts: list[str] = []
    for attachment in attachments or ():
        _append_surface_text(parts, getattr(attachment, "filename", ""))
        title = _surface_text_or_none(getattr(attachment, "title", ""))
        if title is not None:
            parts.append(title)
            link_parts.append(title)
        description = _surface_text_or_none(getattr(attachment, "description", ""))
        if description is not None:
            parts.append(description)
            link_parts.append(description)
    return tuple(parts), tuple(link_parts)


def _collect_embed_surface_texts(embeds: Sequence[Any] | None) -> tuple[str, ...]:
    parts: list[str] = []
    for embed in embeds or ():
        payload = None
        to_dict = getattr(embed, "to_dict", None)
        if callable(to_dict):
            with contextlib.suppress(TypeError):
                payload = to_dict()
        if isinstance(payload, dict):
            for key in ("title", "description", "url"):
                _append_surface_text(parts, payload.get(key))
            footer = payload.get("footer")
            if isinstance(footer, dict):
                _append_surface_text(parts, footer.get("text"))
            author = payload.get("author")
            if isinstance(author, dict):
                for key in ("name", "url"):
                    _append_surface_text(parts, author.get(key))
            for key in ("image", "thumbnail"):
                asset = payload.get(key)
                if isinstance(asset, dict):
                    _append_surface_text(parts, asset.get("url"))
            for field in payload.get("fields", []) if isinstance(payload.get("fields", []), list) else []:
                if isinstance(field, dict):
                    _append_surface_text(parts, field.get("name"))
                    _append_surface_text(parts, field.get("value"))
            continue

        for key in ("title", "description", "url"):
            _append_surface_text(parts, getattr(embed, key, ""))
        footer = getattr(embed, "footer", None)
        _append_surface_text(parts, getattr(footer, "text", ""))
        author = getattr(embed, "author", None)
        for key in ("name", "url"):
            _append_surface_text(parts, getattr(author, key, ""))
        for key in ("image", "thumbnail"):
            asset = getattr(embed, key, None)
            _append_surface_text(parts, getattr(asset, "url", ""))
        for field in getattr(embed, "fields", []) or []:
            _append_surface_text(parts, getattr(field, "name", ""))
            _append_surface_text(parts, getattr(field, "value", ""))
    return tuple(parts)


def _collect_message_surface_texts(message: Any) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    extra_texts: list[str] = []
    link_texts: list[str] = []
    surface_labels: list[str] = []
    normalized_content = normalize_plain_text(getattr(message, "content", ""))

    system_content = _surface_text_or_none(getattr(message, "system_content", ""))
    if system_content is not None and system_content.casefold() != normalized_content.casefold():
        extra_texts.append(system_content)
        link_texts.append(system_content)
        surface_labels.append("system")

    embed_texts = _collect_embed_surface_texts(getattr(message, "embeds", ()))
    if embed_texts:
        extra_texts.extend(embed_texts)
        link_texts.extend(embed_texts)
        surface_labels.append("embeds")

    attachment_texts, attachment_link_texts = _collect_attachment_surface_texts(getattr(message, "attachments", ()))
    if attachment_texts:
        extra_texts.extend(attachment_texts)
        surface_labels.append("attachment_meta")
    if attachment_link_texts:
        link_texts.extend(attachment_link_texts)

    snapshot_texts: list[str] = []
    snapshot_link_texts: list[str] = []
    for forwarded in getattr(message, "message_snapshots", ()) or ():
        forwarded_content = _surface_text_or_none(getattr(forwarded, "content", ""))
        if forwarded_content is not None:
            snapshot_texts.append(forwarded_content)
            snapshot_link_texts.append(forwarded_content)
        forwarded_embed_texts = _collect_embed_surface_texts(getattr(forwarded, "embeds", ()))
        snapshot_texts.extend(forwarded_embed_texts)
        snapshot_link_texts.extend(forwarded_embed_texts)
        forwarded_attachment_texts, forwarded_attachment_link_texts = _collect_attachment_surface_texts(getattr(forwarded, "attachments", ()))
        snapshot_texts.extend(forwarded_attachment_texts)
        snapshot_link_texts.extend(forwarded_attachment_link_texts)
    if snapshot_texts:
        extra_texts.extend(snapshot_texts)
        link_texts.extend(snapshot_link_texts)
        surface_labels.append("forwarded_snapshot")

    return tuple(extra_texts), tuple(link_texts), tuple(dict.fromkeys(surface_labels))


def _build_snapshot(
    text: str | None,
    attachments: Sequence[Any] | None = None,
    *,
    extra_texts: Sequence[str] | None = None,
    link_texts: Sequence[str] | None = None,
    surface_labels: Sequence[str] | None = None,
) -> ShieldSnapshot:
    scan_text = normalize_plain_text(" ".join(part for part in (text or "", *(extra_texts or ())) if part))
    normalized = scan_text
    squashed = squash_for_evasion_checks(normalized.casefold())
    lowered = normalized.casefold()
    link_scan_text = normalize_plain_text(
        " ".join(part for part in (text or "", *((link_texts if link_texts is not None else extra_texts) or ())) if part)
    )
    urls = _extract_urls(link_scan_text.casefold())
    context_text = _strip_urls_from_text(lowered, urls)
    context_squashed = squash_for_evasion_checks(context_text)
    mention_count, everyone_here_count = _count_mentions(normalized)
    emoji_count = _count_emojis(normalized)
    plain_word_count = len(_plain_word_tokens(context_text))
    repeated_char_run = _max_repeated_char_run(context_text)
    low_value_text = _is_low_value_text(context_text, plain_word_count=plain_word_count)
    exact_fingerprint = hashlib.sha1(context_text.encode("utf-8")).hexdigest() if len(context_text) >= 3 else None
    near_duplicate_text = _normalize_near_duplicate_text(context_text, ())
    near_duplicate_fingerprint = (
        hashlib.sha1(near_duplicate_text.encode("utf-8")).hexdigest()
        if len(near_duplicate_text) >= 6
        else None
    )
    links = tuple(link for link in (_build_link(url) for url in urls) if link is not None)
    domains = frozenset(link.domain for link in links)
    invite_codes = frozenset(link.invite_code for link in links if link.invite_code)
    attachment_names = tuple(
        normalize_plain_text(getattr(attachment, "filename", "")).casefold()
        for attachment in (attachments or [])
        if normalize_plain_text(getattr(attachment, "filename", ""))
    )
    is_gif_message, gif_signature, gif_only, gif_low_text = _build_gif_features(
        urls,
        attachments,
        plain_word_count=plain_word_count,
    )
    return ShieldSnapshot(
        scan_text=normalized,
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
        mention_count=mention_count,
        everyone_here_count=everyone_here_count,
        emoji_count=emoji_count,
        plain_word_count=plain_word_count,
        low_value_text=low_value_text,
        repeated_char_run=repeated_char_run,
        is_gif_message=is_gif_message,
        gif_signature=gif_signature,
        gif_only=gif_only,
        gif_low_text=gif_low_text,
        exact_fingerprint=exact_fingerprint,
        near_duplicate_text=near_duplicate_text,
        near_duplicate_fingerprint=near_duplicate_fingerprint,
        surface_labels=tuple(dict.fromkeys(surface_labels or ())),
    )


def _build_message_snapshot(message: discord.Message) -> ShieldSnapshot:
    extra_texts, link_texts, surface_labels = _collect_message_surface_texts(message)
    return _build_snapshot(
        getattr(message, "content", None),
        getattr(message, "attachments", None),
        extra_texts=extra_texts,
        link_texts=link_texts,
        surface_labels=surface_labels,
    )


def _candidate_window(text: str, start: int, end: int, *, radius: int = 28) -> str:
    return text[max(0, start - radius):min(len(text), end + radius)]


def _candidate_is_standalone(text: str, start: int, end: int) -> bool:
    remainder = (text[:start] + text[end:]).strip(" \t\r\n-:;,.!?/\\|()[]{}<>\"'")
    return not remainder


def _attachment_filename(attachment: Any) -> str:
    return normalize_plain_text(getattr(attachment, "filename", "")).casefold()


def _is_gif_attachment(attachment: Any) -> bool:
    filename = _attachment_filename(attachment)
    content_type = str(getattr(attachment, "content_type", "") or "").casefold()
    url = str(getattr(attachment, "url", "") or "").casefold()
    return filename.endswith(".gif") or "image/gif" in content_type or url.endswith(".gif")


def _gif_signature_from_url(url: str) -> str | None:
    parsed = urlsplit(url)
    host = link_normalize_link_host(parsed.netloc)
    path = normalize_plain_text(parsed.path).casefold().strip()
    if not host:
        return None
    if host in GIF_EMBED_DOMAINS or path.endswith(".gif"):
        basis = f"{host}{path}"
        return hashlib.sha1(basis.encode("utf-8")).hexdigest()
    return None


def _build_gif_features(urls: Sequence[str], attachments: Sequence[Any] | None, *, plain_word_count: int) -> tuple[bool, str | None, bool, bool]:
    signatures: list[str] = []
    attachment_count = 0
    gif_attachments = 0
    for attachment in attachments or []:
        attachment_count += 1
        if not _is_gif_attachment(attachment):
            continue
        gif_attachments += 1
        filename = _attachment_filename(attachment) or str(getattr(attachment, "url", "") or "")
        if filename:
            signatures.append(hashlib.sha1(filename.encode("utf-8")).hexdigest())
    for url in urls:
        signature = _gif_signature_from_url(url)
        if signature is not None:
            signatures.append(signature)
    unique_signatures = tuple(dict.fromkeys(signatures))
    is_gif_message = bool(unique_signatures)
    gif_only = is_gif_message and plain_word_count <= 1 and (attachment_count == gif_attachments or attachment_count == 0)
    gif_low_text = is_gif_message and plain_word_count <= 4
    return is_gif_message, unique_signatures[0] if unique_signatures else None, gif_only, gif_low_text


def _sensitivity_threshold(sensitivity: str, *, low: int, normal: int, high: int) -> int:
    return {"low": low, "normal": normal, "high": high}.get(sensitivity, normal)


def _confidence_from_score(score: int) -> str:
    if score >= 3:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _adult_solicitation_context_suppressed(text: str) -> bool:
    if "server rules:" in text or "policy update:" in text or "example pricing:" in text:
        return False
    return is_harmful_context_suppressed(text, include_disapproval=True)


def _link_policy_mode_label(mode: str) -> str:
    return "Trusted Links Only" if mode == "trusted_only" else "Default"


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
    return looks_like_warning_discussion(text)


def _feature_pack_settings(*, enabled: bool, low_action: str = "log", medium_action: str = "delete_log", high_action: str = "delete_log", sensitivity: str = "normal") -> PackSettings:
    return PackSettings(
        enabled=enabled,
        low_action=low_action,
        medium_action=medium_action,
        high_action=high_action,
        sensitivity=sensitivity,
    )


def _build_feature_compiled_config(
    *,
    privacy_enabled: bool,
    adult_enabled: bool,
    adult_solicitation_enabled: bool,
    severe_enabled: bool,
) -> CompiledShieldConfig:
    disabled = _feature_pack_settings(enabled=False, low_action="detect", medium_action="detect", high_action="detect")
    return CompiledShieldConfig(
        guild_id=0,
        module_enabled=True,
        log_channel_id=None,
        alert_role_id=None,
        scan_mode="all",
        included_channel_ids=frozenset(),
        excluded_channel_ids=frozenset(),
        included_user_ids=frozenset(),
        excluded_user_ids=frozenset(),
        included_role_ids=frozenset(),
        excluded_role_ids=frozenset(),
        trusted_role_ids=frozenset(),
        allow_domains=frozenset(),
        allow_invite_codes=frozenset(),
        allow_phrases=(),
        trusted_builtin_disabled_families=frozenset(),
        trusted_builtin_disabled_domains=frozenset(),
        privacy=_feature_pack_settings(enabled=privacy_enabled),
        promo=disabled,
        scam=disabled,
        spam=disabled,
        adult=_feature_pack_settings(enabled=adult_enabled),
        adult_solicitation_enabled=adult_solicitation_enabled,
        adult_solicitation_excluded_channel_ids=frozenset(),
        severe=_feature_pack_settings(enabled=severe_enabled),
        severe_enabled_categories=frozenset(DEFAULT_SHIELD_SEVERE_CATEGORIES),
        severe_custom_terms=(),
        severe_removed_terms=frozenset(),
        link_policy_mode=DEFAULT_SHIELD_LINK_POLICY_MODE,
        link_policy=disabled,
        ai_enabled=False,
        ai_min_confidence="high",
        ai_enabled_packs=frozenset(),
        escalation_threshold=99,
        escalation_window_minutes=10,
        timeout_minutes=10,
        custom_patterns=(),
    )


FEATURE_SURFACE_AFK_REASON = "utility_afk_reason"
FEATURE_SURFACE_AFK_SCHEDULE_REASON = "utility_afk_schedule_reason"
FEATURE_SURFACE_REMINDER_CREATE = "utility_reminder_create"
FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY = "utility_reminder_public_delivery"
FEATURE_SURFACE_WATCH_KEYWORD = "utility_watch_keyword"
FEATURE_SURFACE_CONFESSIONS_LINKS = "confessions_link_assessment"

_FEATURE_CONFIG_PRIVACY_AND_ADULT = _build_feature_compiled_config(
    privacy_enabled=True,
    adult_enabled=True,
    adult_solicitation_enabled=True,
    severe_enabled=True,
)
_FEATURE_CONFIG_PRIVACY_ONLY = _build_feature_compiled_config(
    privacy_enabled=True,
    adult_enabled=False,
    adult_solicitation_enabled=False,
    severe_enabled=False,
)
_FEATURE_SURFACE_POLICIES: dict[str, dict[str, Any]] = {
    FEATURE_SURFACE_AFK_REASON: {
        "compiled": _FEATURE_CONFIG_PRIVACY_AND_ADULT,
        "privacy_message": "AFK reasons cannot contain private contact, account, or payment details.",
        "adult_message": "AFK reasons cannot advertise adult or DM-gated sexual content.",
        "severe_message": "AFK reasons cannot include severe hate, self-harm encouragement, or exploitative abuse solicitation.",
    },
    FEATURE_SURFACE_AFK_SCHEDULE_REASON: {
        "compiled": _FEATURE_CONFIG_PRIVACY_AND_ADULT,
        "privacy_message": "Recurring AFK reasons cannot contain private contact, account, or payment details.",
        "adult_message": "Recurring AFK reasons cannot advertise adult or DM-gated sexual content.",
        "severe_message": "Recurring AFK reasons cannot include severe hate, self-harm encouragement, or exploitative abuse solicitation.",
    },
    FEATURE_SURFACE_REMINDER_CREATE: {
        "compiled": _FEATURE_CONFIG_PRIVACY_AND_ADULT,
        "privacy_message": "Reminder text cannot contain private contact, account, or payment details.",
        "adult_message": "Reminder text cannot advertise adult or DM-gated sexual content.",
        "severe_message": "Reminder text cannot include severe hate, self-harm encouragement, or exploitative abuse solicitation.",
    },
    FEATURE_SURFACE_REMINDER_PUBLIC_DELIVERY: {
        "compiled": _FEATURE_CONFIG_PRIVACY_AND_ADULT,
        "privacy_message": "Babblebox withheld that public reminder because its text now looks like private contact or account information.",
        "adult_message": "Babblebox withheld that public reminder because its text looks like adult or DM-gated solicitation.",
        "severe_message": "Babblebox withheld that public reminder because its text looked like severe hate, self-harm encouragement, or exploitative abuse solicitation.",
    },
    FEATURE_SURFACE_WATCH_KEYWORD: {
        "compiled": _FEATURE_CONFIG_PRIVACY_ONLY,
        "privacy_message": "Watch keywords cannot contain private-looking contact, account, or payment details.",
        "adult_message": None,
        "severe_message": None,
    },
}


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
        self.feature_gateway = ShieldFeatureSafetyGateway(detector=self, link_safety=self.link_safety)
        self._compiled_configs: dict[int, CompiledShieldConfig] = {}
        self._alert_dedup: dict[tuple[int, int], tuple[float, str]] = {}
        self._alert_signature_dedup: dict[tuple[Any, ...], float] = {}
        self._compact_alert_cohorts: dict[tuple[Any, ...], float] = {}
        self._strike_windows: dict[tuple[int, int, str], list[float]] = {}
        self._recent_promos: dict[tuple[int, int, str], list[float]] = {}
        self._recent_scam_campaigns: dict[tuple[int, str, str], list[tuple[float, int]]] = {}
        self._recent_spam_events: dict[tuple[int, int], list[ShieldSpamEvent]] = {}
        self._recent_channel_activity: dict[tuple[int, int], list[tuple[float, int, int, bool]]] = {}
        self._recent_join_waves: dict[int, list[tuple[float, int, bool]]] = {}
        self._recent_raid_patterns: dict[tuple[int, str, str], list[tuple[float, int]]] = {}
        self._raid_alert_dedup: dict[tuple[Any, ...], float] = {}
        self._recent_newcomer_activity: dict[tuple[int, int], ShieldNewcomerActivityState] = {}
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
        if self._apply_startup_baseline_upgrades():
            await self.store.flush()
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
                "ordinary_ai_enabled": bool(meta.get("ordinary_ai_enabled")),
                "ordinary_ai_allowed_models": tuple(
                    model
                    for model in (meta.get("ordinary_ai_allowed_models") or (DEFAULT_SHIELD_AI_FAST_MODEL,))
                    if model in SHIELD_AI_MODEL_ORDER
                )
                or (DEFAULT_SHIELD_AI_FAST_MODEL,),
                "ordinary_ai_updated_by": meta.get("ordinary_ai_updated_by"),
                "ordinary_ai_updated_at": meta.get("ordinary_ai_updated_at"),
            }
        return {
            "ordinary_ai_enabled": False,
            "ordinary_ai_allowed_models": (DEFAULT_SHIELD_AI_FAST_MODEL,),
            "ordinary_ai_updated_by": None,
            "ordinary_ai_updated_at": None,
        }

    def get_config(self, guild_id: int) -> dict[str, Any]:
        raw = self.store.state.get("guilds", {}).get(str(guild_id))
        if isinstance(raw, dict):
            return normalize_guild_shield_config(guild_id, raw)
        return default_guild_shield_config(guild_id)

    def _parse_allowed_models(self, values: Sequence[str] | str | None, *, allow_empty: bool = False) -> tuple[bool, tuple[str, ...] | str]:
        try:
            models = parse_shield_ai_model_list(values)
        except ValueError:
            return False, "Allowed Shield AI models must be `nano`, `mini`, `full`, or canonical model names."
        if not models and not allow_empty:
            return False, "Select at least one allowed Shield AI model."
        return True, models

    def resolve_ai_access_policy(self, guild_id: int) -> ShieldAIAccessPolicy:
        config = self.get_config(guild_id)
        meta = self.get_meta()
        support_default = shield_ai_available_in_guild(guild_id)
        ordinary_global_enabled = bool(meta["ordinary_ai_enabled"])
        ordinary_global_allowed_models = tuple(meta["ordinary_ai_allowed_models"]) or (DEFAULT_SHIELD_AI_FAST_MODEL,)

        if support_default:
            enabled = True
            allowed_models = SHIELD_AI_MODEL_ORDER
            source = "support_default"
            updated_by = None
            updated_at = None
        else:
            enabled = ordinary_global_enabled
            allowed_models = ordinary_global_allowed_models
            source = "ordinary_global"
            updated_by = meta["ordinary_ai_updated_by"]
            updated_at = meta["ordinary_ai_updated_at"]

        guild_access_mode = str(config.get("ai_access_mode", "inherit")).strip().lower()
        if guild_access_mode not in VALID_SHIELD_AI_ACCESS_MODES:
            guild_access_mode = "inherit"
        guild_allowed_models_override = tuple(
            model for model in config.get("ai_allowed_models_override", []) if model in SHIELD_AI_MODEL_ORDER
        )
        if guild_access_mode == "enabled":
            enabled = True
            source = "guild_override"
            updated_by = config.get("ai_access_updated_by")
            updated_at = config.get("ai_access_updated_at")
        elif guild_access_mode == "disabled":
            enabled = False
            source = "guild_override"
            updated_by = config.get("ai_access_updated_by")
            updated_at = config.get("ai_access_updated_at")
        if guild_allowed_models_override:
            allowed_models = guild_allowed_models_override
            source = "guild_override"
            updated_by = config.get("ai_access_updated_by")
            updated_at = config.get("ai_access_updated_at")

        return ShieldAIAccessPolicy(
            guild_id=guild_id,
            enabled=enabled,
            source=source,
            support_default=support_default,
            allowed_models=tuple(allowed_models),
            ordinary_global_enabled=ordinary_global_enabled,
            ordinary_global_allowed_models=ordinary_global_allowed_models,
            guild_access_mode=guild_access_mode,
            guild_allowed_models_override=guild_allowed_models_override,
            updated_by=updated_by if isinstance(updated_by, int) and updated_by > 0 else None,
            updated_at=updated_at if isinstance(updated_at, str) and updated_at.strip() else None,
        )

    def is_ai_supported_guild(self, guild_id: int | None) -> bool:
        if guild_id is None:
            return False
        return self.resolve_ai_access_policy(guild_id).enabled

    def get_ai_status(self, guild_id: int) -> dict[str, Any]:
        config = self.get_config(guild_id)
        policy = self.resolve_ai_access_policy(guild_id)
        diagnostics = self.ai_provider.diagnostics()
        enabled_packs = [pack for pack in config.get("ai_enabled_packs", []) if pack in AI_REVIEW_PACK_SET]
        status_message = diagnostics["status"]
        if not policy.enabled:
            if policy.support_default:
                status_message = "AI review is disabled for the support server by owner override."
            elif policy.source == "guild_override":
                status_message = "AI review is disabled for this guild by owner override."
            else:
                status_message = "AI review is off for ordinary guilds until the owner enables it."
        elif not diagnostics["available"]:
            status_message = "AI review is enabled by policy but the provider is not configured."
        else:
            status_message = "Ready for second-pass review."
        return {
            "supported": policy.enabled,
            "enabled": policy.enabled,
            "policy_source": policy.source,
            "support_server_default": policy.support_default,
            "ordinary_global_enabled": policy.ordinary_global_enabled,
            "ordinary_global_allowed_models": list(policy.ordinary_global_allowed_models),
            "guild_access_mode": policy.guild_access_mode,
            "guild_allowed_models_override": list(policy.guild_allowed_models_override),
            "allowed_models": list(policy.allowed_models),
            "enabled_packs": enabled_packs,
            "min_confidence": config.get("ai_min_confidence", "high"),
            "provider": diagnostics.get("provider"),
            "provider_available": bool(diagnostics.get("available")),
            "model": diagnostics.get("model"),
            "routing_strategy": diagnostics.get("routing_strategy"),
            "single_model_override": bool(diagnostics.get("single_model_override")),
            "fast_model": diagnostics.get("fast_model"),
            "complex_model": diagnostics.get("complex_model"),
            "top_model": diagnostics.get("top_model"),
            "top_tier_enabled": bool(diagnostics.get("top_tier_enabled")),
            "timeout_seconds": diagnostics.get("timeout_seconds"),
            "max_chars": diagnostics.get("max_chars"),
            "status": status_message,
            "updated_by": policy.updated_by,
            "updated_at": policy.updated_at,
        }

    def get_link_safety_status(self) -> dict[str, Any]:
        return self.link_safety.diagnostics()

    def trusted_builtin_family_domains(self) -> dict[str, tuple[str, ...]]:
        return {
            family: tuple(sorted(self.link_safety.intel.safe_families.get(family, frozenset())))
            for family in sorted(TRUSTED_ONLY_BUILTIN_FAMILIES)
            if self.link_safety.intel.safe_families.get(family)
        }

    def trusted_builtin_domains(self) -> tuple[str, ...]:
        return tuple(sorted(TRUSTED_ONLY_BUILTIN_DOMAINS))

    def trusted_pack_state(self, guild_id: int) -> dict[str, Any]:
        config = self.get_config(guild_id)
        family_domains = self.trusted_builtin_family_domains()
        effective_disabled_domains = [
            domain
            for domain in config.get("trusted_builtin_disabled_domains", [])
            if self._domain_is_allowlisted(domain, frozenset(self._trusted_builtin_domain_candidates()))
        ]
        return {
            "mode": config.get("link_policy_mode", DEFAULT_SHIELD_LINK_POLICY_MODE),
            "families": [
                {
                    "name": family,
                    "count": len(domains),
                    "examples": list(domains[:4]),
                    "disabled": family in config.get("trusted_builtin_disabled_families", []),
                }
                for family, domains in family_domains.items()
            ],
            "direct_domains": [
                {
                    "domain": domain,
                    "disabled": self._domain_is_allowlisted(domain, frozenset(config.get("trusted_builtin_disabled_domains", []))),
                }
                for domain in self.trusted_builtin_domains()
            ],
            "disabled_families": [
                family for family in config.get("trusted_builtin_disabled_families", []) if family in family_domains
            ],
            "disabled_domains": effective_disabled_domains,
            "allow_domains": list(config.get("allow_domains", [])),
            "allow_invite_codes": list(config.get("allow_invite_codes", [])),
            "allow_phrases": list(config.get("allow_phrases", [])),
        }

    def evaluate_feature_text(
        self,
        surface: str,
        text: str | None,
        *,
        attachments: Sequence[Any] | None = None,
        channel_id: int | None = None,
    ) -> ShieldFeatureDecision:
        return self.feature_gateway.evaluate(surface, text, attachments=attachments, channel_id=channel_id)

    def assess_feature_links(
        self,
        surface: str,
        *,
        text: str,
        squashed: str | None = None,
        shared_link_url: str | None = None,
        allow_domain_set: Iterable[str] = (),
        block_domain_set: Iterable[str] = (),
        link_policy_mode: str = DEFAULT_SHIELD_LINK_POLICY_MODE,
        has_suspicious_attachment: bool = False,
    ) -> ShieldFeatureLinkScan:
        return self.feature_gateway.assess_links(
            surface,
            text=text,
            squashed=squashed,
            shared_link_url=shared_link_url,
            allow_domain_set=allow_domain_set,
            block_domain_set=block_domain_set,
            link_policy_mode=link_policy_mode,
            has_suspicious_attachment=has_suspicious_attachment,
        )

    async def _save_meta(self, meta: dict[str, Any], *, failure_message: str) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message("Shield AI")
        async with self._lock:
            self.store.state["meta"] = meta
            flushed = await self.store.flush()
            if not flushed:
                return False, failure_message
        return True, ""

    async def set_ordinary_ai_policy(
        self,
        *,
        enabled: bool | None = None,
        allowed_models: Sequence[str] | str | None = None,
        actor_id: int,
    ) -> tuple[bool, str]:
        meta = self.get_meta()
        next_enabled = meta["ordinary_ai_enabled"] if enabled is None else bool(enabled)
        next_models = meta["ordinary_ai_allowed_models"]
        if allowed_models is not None:
            ok, parsed_or_error = self._parse_allowed_models(allowed_models)
            if not ok:
                return False, parsed_or_error
            next_models = parsed_or_error
        updated_meta = {
            "ordinary_ai_enabled": next_enabled,
            "ordinary_ai_allowed_models": list(next_models),
            "ordinary_ai_updated_by": actor_id,
            "ordinary_ai_updated_at": ge.now_utc().isoformat(),
        }
        ok, failure = await self._save_meta(updated_meta, failure_message="Shield AI ordinary-guild policy could not be saved.")
        if not ok:
            return False, failure
        return (
            True,
            f"Ordinary-guild Shield AI is now {'enabled' if next_enabled else 'disabled'} with allowed models "
            f"{format_shield_ai_model_list(next_models)}.",
        )

    async def set_guild_ai_access_policy(
        self,
        guild_id: int,
        *,
        mode: str | None = None,
        allowed_models: Sequence[str] | str | None = None,
        actor_id: int,
    ) -> tuple[bool, str]:
        cleaned_mode = str(mode).strip().lower() if isinstance(mode, str) else None
        if cleaned_mode is not None and cleaned_mode not in VALID_SHIELD_AI_ACCESS_MODES:
            return False, "Guild AI access mode must be `inherit`, `enabled`, or `disabled`."
        cleaned_models: tuple[str, ...] | None = None
        if allowed_models is not None:
            ok, parsed_or_error = self._parse_allowed_models(allowed_models)
            if not ok:
                return False, parsed_or_error
            cleaned_models = parsed_or_error

        def mutate(config: dict[str, Any]):
            if cleaned_mode is not None:
                config["ai_access_mode"] = cleaned_mode
            if cleaned_models is not None:
                config["ai_allowed_models_override"] = list(cleaned_models)
            config["ai_access_updated_by"] = actor_id
            config["ai_access_updated_at"] = ge.now_utc().isoformat()

        preview = self.get_config(guild_id)
        final_mode = preview.get("ai_access_mode", "inherit") if cleaned_mode is None else cleaned_mode
        final_models = tuple(preview.get("ai_allowed_models_override", [])) if cleaned_models is None else cleaned_models
        ok, message = await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Guild Shield AI access now uses `{final_mode}` mode with model override "
                f"{format_shield_ai_model_list(final_models) if final_models else 'inherit'}."
            ),
        )
        return ok, message

    async def inherit_guild_ai_access_policy(self, guild_id: int, *, actor_id: int) -> tuple[bool, str]:
        def mutate(config: dict[str, Any]):
            config["ai_access_mode"] = "inherit"
            config["ai_allowed_models_override"] = []
            config["ai_access_updated_by"] = actor_id
            config["ai_access_updated_at"] = ge.now_utc().isoformat()

        return await self._update_config(
            guild_id,
            mutate,
            success_message="Guild Shield AI access policy now inherits the default owner policy again.",
        )

    async def restore_support_ai_defaults(self, *, actor_id: int) -> tuple[bool, str]:
        ok, message = await self.inherit_guild_ai_access_policy(SHIELD_AI_SUPPORT_GUILD_ID, actor_id=actor_id)
        if not ok:
            return False, message
        return True, "Support server Shield AI access restored to full default access."

    async def set_global_ai_override(self, enabled: bool, *, actor_id: int) -> tuple[bool, str]:
        return await self.set_ordinary_ai_policy(enabled=enabled, actor_id=actor_id)

    def test_message(
        self,
        guild_id: int,
        text: str,
        *,
        attachments: Sequence[Any] | None = None,
        channel_id: int | None = None,
    ) -> list[ShieldMatch]:
        return list(self.test_message_details(guild_id, text, attachments=attachments, channel_id=channel_id).matches)

    def test_message_details(
        self,
        guild_id: int,
        text: str,
        *,
        attachments: Sequence[Any] | None = None,
        channel_id: int | None = None,
    ) -> ShieldTestResult:
        compiled = self._compiled_configs.get(guild_id) or self._compile_config(guild_id, self.get_config(guild_id))
        fake_attachments = [
            type("Attachment", (), {"filename": value})() if isinstance(value, str) else value
            for value in (attachments or [])
        ]
        snapshot = _build_snapshot(text, fake_attachments)
        now = time.monotonic()
        link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
        allow_phrase = self._matching_allow_phrase(compiled, snapshot)
        raw_matches = self._collect_matches(compiled, snapshot, link_assessments=link_assessments, channel_id=channel_id)
        matches, allow_phrase_suppressed = self._apply_allow_phrase_suppression(raw_matches, allow_phrase=allow_phrase)
        bypass_reason = None
        if allow_phrase_suppressed:
            bypass_reason = (
                "A guild allow phrase matched this sample, so live Shield handling would suppress only targeted promo "
                "or adult-solicitation text matches here."
            )
        if (
            channel_id is not None
            and channel_id in compiled.adult_solicitation_excluded_channel_ids
            and compiled.adult_solicitation_enabled
        ):
            unsuppressed_adult = self._detect_adult_solicitation(compiled, snapshot, channel_id=None)
            if unsuppressed_adult and not any(match.match_class in {"adult_dm_ad", "adult_solicitation"} for match in matches):
                bypass_reason = (
                    "This channel relaxes only the optional adult-solicitation detector, so that specific text match would be skipped here."
                )
        return ShieldTestResult(matches=tuple(matches), link_assessments=link_assessments, bypass_reason=bypass_reason)

    async def set_module_enabled(self, guild_id: int, enabled: bool) -> tuple[bool, str]:
        baseline_applied = False
        current = self.get_config(guild_id)
        first_enable = enabled and int(current.get("baseline_version", 0) or 0) < SHIELD_BASELINE_VERSION

        def mutate(config: dict[str, Any]):
            nonlocal baseline_applied
            if enabled and int(config.get("baseline_version", 0) or 0) < SHIELD_BASELINE_VERSION:
                baseline_applied = self._apply_first_enable_baseline(config)
                config["baseline_version"] = SHIELD_BASELINE_VERSION
            config["module_enabled"] = bool(enabled)

        message = f"Shield live-message moderation is now {'enabled' if enabled else 'disabled'} for this server."
        if enabled:
            message += " Shield AI stays second-pass only and owner-managed."
            if first_enable:
                message += (
                    " On first enable, Babblebox applies its recommended non-AI baseline anywhere you had not already customized it. "
                    "Review `/shield links`, `/shield trusted`, and `/shield logs` next."
                )
        return await self._update_config(
            guild_id,
            mutate,
            success_message=message,
        )

    def _apply_first_enable_baseline(self, config: dict[str, Any]) -> bool:
        defaults = default_guild_shield_config(int(config.get("guild_id") or 0) or None)
        changed = False
        for pack in RULE_PACKS:
            fields = (
                f"{pack}_enabled",
                f"{pack}_action",
                f"{pack}_low_action",
                f"{pack}_medium_action",
                f"{pack}_high_action",
                f"{pack}_sensitivity",
            )
            if all(config.get(field) == defaults.get(field) for field in fields):
                if pack == "spam":
                    config[f"{pack}_enabled"] = True
                    config[f"{pack}_action"] = "delete_escalate"
                    config[f"{pack}_low_action"] = "log"
                    config[f"{pack}_medium_action"] = "delete_log"
                    config[f"{pack}_high_action"] = "delete_escalate"
                    config[f"{pack}_sensitivity"] = "normal"
                else:
                    config[f"{pack}_enabled"] = True
                    config[f"{pack}_action"] = "delete_log"
                    config[f"{pack}_low_action"] = "log"
                    config[f"{pack}_medium_action"] = "delete_log"
                    config[f"{pack}_high_action"] = "delete_log"
                    config[f"{pack}_sensitivity"] = "normal"
                changed = True
        if config.get("adult_solicitation_enabled") == defaults.get("adult_solicitation_enabled"):
            config["adult_solicitation_enabled"] = True
            changed = True
        return changed

    def _apply_startup_baseline_upgrades(self) -> bool:
        changed = False
        guilds = self.store.state.setdefault("guilds", {})
        for guild_id_text, raw in list(guilds.items()):
            try:
                guild_id = int(guild_id_text)
            except (TypeError, ValueError):
                continue
            if not isinstance(raw, dict):
                continue
            config = normalize_guild_shield_config(guild_id, raw)
            if int(config.get("baseline_version", 0) or 0) >= SHIELD_BASELINE_VERSION:
                guilds[guild_id_text] = config
                continue
            if not bool(config.get("module_enabled")):
                guilds[guild_id_text] = config
                continue
            if self._apply_first_enable_baseline(config):
                changed = True
            config["baseline_version"] = SHIELD_BASELINE_VERSION
            guilds[guild_id_text] = config
        return changed

    def _member_age_flags(self, member: Any) -> tuple[bool, bool, bool]:
        now_dt = ge.now_utc()
        created_at = _member_datetime(getattr(member, "created_at", None))
        joined_at = _member_datetime(getattr(member, "joined_at", None))
        recent_account = created_at is not None and now_dt - created_at <= RECENT_ACCOUNT_WINDOW
        early_member = joined_at is not None and now_dt - joined_at <= EARLY_MEMBER_WINDOW
        return recent_account, early_member, bool(recent_account or early_member)

    def _trusted_builtin_domain_candidates(self) -> tuple[str, ...]:
        domains = set(TRUSTED_ONLY_BUILTIN_DOMAINS)
        for family in TRUSTED_ONLY_BUILTIN_FAMILIES:
            domains.update(self.link_safety.intel.safe_families.get(family, frozenset()))
        return tuple(sorted(domains))

    async def set_trusted_builtin_family_enabled(self, guild_id: int, family: str, enabled: bool) -> tuple[bool, str]:
        cleaned_family = normalize_plain_text(family).casefold()
        if cleaned_family not in self.trusted_builtin_family_domains():
            return False, "That built-in trusted family is not part of Shield's trusted-only pack."

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_text(config.get("trusted_builtin_disabled_families", [])))
            if enabled:
                values.discard(cleaned_family)
            else:
                values.add(cleaned_family)
            config["trusted_builtin_disabled_families"] = sorted(values)

        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Trusted family `{cleaned_family}` is now {'enabled' if enabled else 'disabled'} for Shield trusted-only mode.",
        )

    async def set_trusted_builtin_domain_enabled(self, guild_id: int, domain: str, enabled: bool) -> tuple[bool, str]:
        valid, cleaned_domain = self._normalize_domain(domain)
        if not valid:
            return False, cleaned_domain
        if not self._domain_is_allowlisted(cleaned_domain, frozenset(self._trusted_builtin_domain_candidates())):
            return False, "That domain is not part of Shield's built-in trusted pack."

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_text(config.get("trusted_builtin_disabled_domains", [])))
            if enabled:
                values.discard(cleaned_domain)
            else:
                values.add(cleaned_domain)
            config["trusted_builtin_disabled_domains"] = sorted(values)

        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Trusted domain `{cleaned_domain}` is now {'enabled' if enabled else 'disabled'} for Shield trusted-only mode.",
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
        adult_solicitation: bool | None = None,
    ) -> tuple[bool, str]:
        if pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        if adult_solicitation is not None and pack != "adult":
            return False, "Adult solicitation can only be configured on the adult pack."
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
            if pack == "adult" and adult_solicitation is not None:
                config["adult_solicitation_enabled"] = bool(adult_solicitation)

        new_enabled = current[f"{pack}_enabled"] if enabled is None else bool(enabled)
        new_sensitivity = current[f"{pack}_sensitivity"] if cleaned_sensitivity is None else cleaned_sensitivity
        solicitation_note = ""
        if pack == "adult":
            solicitation_state = current.get("adult_solicitation_enabled", False) if adult_solicitation is None else bool(adult_solicitation)
            solicitation_note = f" Optional solicitation text detection is {'on' if solicitation_state else 'off'}."
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[pack]} is {'enabled' if new_enabled else 'disabled'} "
                f"with {self._policy_summary(low_action=final_low_action, medium_action=final_medium_action, high_action=final_high_action)} "
                f"at {SENSITIVITY_LABELS[new_sensitivity].lower()} sensitivity."
                f"{solicitation_note}"
            ),
        )

    async def set_severe_category(self, guild_id: int, category: str, enabled: bool) -> tuple[bool, str]:
        cleaned_category = normalize_plain_text(category).casefold()
        if cleaned_category not in VALID_SHIELD_SEVERE_CATEGORIES:
            return False, "Unknown severe-harm category."

        def mutate(config: dict[str, Any]):
            categories = set(_sorted_unique_text(config.get("severe_enabled_categories", DEFAULT_SHIELD_SEVERE_CATEGORIES)))
            if enabled:
                categories.add(cleaned_category)
            else:
                categories.discard(cleaned_category)
            config["severe_enabled_categories"] = [value for value in DEFAULT_SHIELD_SEVERE_CATEGORIES if value in categories]

        label = SEVERE_CATEGORY_LABELS.get(cleaned_category, cleaned_category.replace("_", " ").title())
        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Severe-harm category **{label}** is now {'on' if enabled else 'off'}.",
        )

    async def update_severe_term(self, guild_id: int, action: str, phrase: str) -> tuple[bool, str]:
        cleaned_action = normalize_plain_text(action).casefold()
        if cleaned_action not in {"add", "remove_default", "restore_default", "remove_custom"}:
            return False, "Severe term actions must be add, remove_default, restore_default, or remove_custom."
        ok, cleaned_or_error = self._normalize_severe_term(phrase)
        if not ok:
            return False, cleaned_or_error
        term = cleaned_or_error

        def mutate(config: dict[str, Any]):
            custom_terms = list(_sorted_unique_text(config.get("severe_custom_terms", [])))
            removed_terms = list(_sorted_unique_text(config.get("severe_removed_terms", [])))
            if cleaned_action == "add":
                if term in custom_terms or term in SEVERE_REMOVABLE_DEFAULT_TERMS:
                    raise ValueError("That severe term is already active.")
                if len(custom_terms) >= SHIELD_SEVERE_TERM_LIMIT:
                    raise ValueError(f"You can keep up to {SHIELD_SEVERE_TERM_LIMIT} custom severe terms.")
                custom_terms.append(term)
            elif cleaned_action == "remove_custom":
                if term not in custom_terms:
                    raise ValueError("That custom severe term was not configured.")
                custom_terms.remove(term)
            elif cleaned_action == "remove_default":
                if term not in SEVERE_REMOVABLE_DEFAULT_TERMS:
                    raise ValueError("That phrase is not one of Babblebox's removable bundled severe terms.")
                if term not in removed_terms:
                    if len(removed_terms) >= SHIELD_SEVERE_TERM_LIMIT:
                        raise ValueError(f"You can keep up to {SHIELD_SEVERE_TERM_LIMIT} removed bundled severe terms.")
                    removed_terms.append(term)
            elif cleaned_action == "restore_default":
                if term not in removed_terms:
                    raise ValueError("That bundled severe term is already active.")
                removed_terms.remove(term)
            config["severe_custom_terms"] = sorted(custom_terms)
            config["severe_removed_terms"] = sorted(removed_terms)

        success_messages = {
            "add": f"Custom severe term `{term}` added.",
            "remove_custom": f"Custom severe term `{term}` removed.",
            "remove_default": f"Bundled severe term `{term}` disabled for this server.",
            "restore_default": f"Bundled severe term `{term}` restored.",
        }
        return await self._update_config(guild_id, mutate, success_message=success_messages[cleaned_action])

    async def set_link_policy_config(
        self,
        guild_id: int,
        *,
        mode: str | None = None,
        action: str | None = None,
        low_action: str | None = None,
        medium_action: str | None = None,
        high_action: str | None = None,
    ) -> tuple[bool, str]:
        cleaned_mode = str(mode).strip().lower() if isinstance(mode, str) else None
        if cleaned_mode is not None and cleaned_mode not in VALID_SHIELD_LINK_POLICY_MODES:
            return False, "Link policy mode must be `default` or `trusted_only`."
        cleaned_action = action.strip().lower() if isinstance(action, str) else None
        cleaned_low_action = low_action.strip().lower() if isinstance(low_action, str) else None
        cleaned_medium_action = medium_action.strip().lower() if isinstance(medium_action, str) else None
        cleaned_high_action = high_action.strip().lower() if isinstance(high_action, str) else None
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

        current = self.get_config(guild_id)
        if cleaned_action is not None:
            derived_low, derived_medium, derived_high = _legacy_action_policy(cleaned_action)
        else:
            derived_low = current.get("link_policy_low_action", "log")
            derived_medium = current.get("link_policy_medium_action", "log")
            derived_high = current.get("link_policy_high_action", "log")
        final_low_action = derived_low if cleaned_low_action is None else cleaned_low_action
        final_medium_action = derived_medium if cleaned_medium_action is None else cleaned_medium_action
        final_high_action = derived_high if cleaned_high_action is None else cleaned_high_action

        def mutate(config: dict[str, Any]):
            if cleaned_mode is not None:
                config["link_policy_mode"] = cleaned_mode
            if cleaned_action is not None:
                config["link_policy_action"] = cleaned_action
                low_default, medium_default, high_default = _legacy_action_policy(cleaned_action)
                config["link_policy_low_action"] = low_default
                config["link_policy_medium_action"] = medium_default
                config["link_policy_high_action"] = high_default
            if cleaned_low_action is not None:
                config["link_policy_low_action"] = cleaned_low_action
            if cleaned_medium_action is not None:
                config["link_policy_medium_action"] = cleaned_medium_action
            if cleaned_high_action is not None:
                config["link_policy_high_action"] = cleaned_high_action
            config["link_policy_action"] = config.get("link_policy_high_action", config.get("link_policy_action", "log"))

        final_mode = current.get("link_policy_mode", DEFAULT_SHIELD_LINK_POLICY_MODE) if cleaned_mode is None else cleaned_mode
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Shield link policy is now **{_link_policy_mode_label(final_mode)}** with "
                f"{self._policy_summary(low_action=final_low_action, medium_action=final_medium_action, high_action=final_high_action)}."
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
            "adult_solicitation_excluded_channel_ids",
            "included_user_ids",
            "excluded_user_ids",
            "included_role_ids",
            "excluded_role_ids",
            "trusted_role_ids",
        }:
            return False, "Unknown Shield filter."
        label = {
            "adult_solicitation_excluded_channel_ids": "adult-solicitation carve-out channels",
        }.get(field, field.replace("_ids", "").replace("_", " "))

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
        if enabled is not None:
            return False, "Shield AI access is owner-managed privately. `/shield ai` only changes review threshold and eligible packs."
        cleaned_min_confidence = str(min_confidence).strip().lower() if isinstance(min_confidence, str) else None
        if cleaned_min_confidence is not None and cleaned_min_confidence not in SHIELD_AI_MIN_CONFIDENCE_CHOICES:
            return False, "AI review confidence threshold must be low, medium, or high."
        cleaned_packs: list[str] | None = None
        if enabled_packs is not None:
            cleaned_packs = []
            for item in enabled_packs:
                pack = str(item).strip().lower()
                if pack not in AI_REVIEW_PACK_SET:
                    return False, "AI review packs must be privacy, promo, scam, adult, or severe."
                if pack not in cleaned_packs:
                    cleaned_packs.append(pack)
        def mutate(config: dict[str, Any]):
            if cleaned_min_confidence is not None:
                config["ai_min_confidence"] = cleaned_min_confidence
            if cleaned_packs is not None:
                config["ai_enabled_packs"] = cleaned_packs

        current = self.get_config(guild_id)
        final_min_confidence = current["ai_min_confidence"] if cleaned_min_confidence is None else cleaned_min_confidence
        final_packs = current["ai_enabled_packs"] if cleaned_packs is None else cleaned_packs
        if not final_packs:
            return False, "Select at least one local Shield pack for AI review."
        pack_summary = ", ".join(PACK_LABELS[pack] for pack in final_packs) if final_packs else "no packs selected"
        policy = self.resolve_ai_access_policy(guild_id)
        provider_status = self.ai_provider.diagnostics().get("status", "Unavailable.")
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Shield AI review scope now uses `{final_min_confidence}` minimum local confidence for {pack_summary}. "
                f"Owner policy source: `{policy.source}`. "
                f"Allowed models: {format_shield_ai_model_list(policy.allowed_models)}. "
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

    async def handle_message(self, message: discord.Message, *, scan_source: str | None = None) -> ShieldDecision | None:
        resolved_scan_source = _scan_source_for_message(message, default=scan_source or "new_message")
        if not self.storage_ready or message.guild is None:
            return None
        if getattr(getattr(message, "author", None), "id", None) == getattr(getattr(self.bot, "user", None), "id", None):
            return None

        compiled = self._compiled_configs.get(message.guild.id)
        if compiled is None or not compiled.module_enabled:
            return None
        if not self._message_in_scope(compiled, message):
            return None

        now = asyncio.get_running_loop().time()
        self._prune_runtime_state(now)
        snapshot = _build_message_snapshot(message)
        return await self._scan_message(
            message,
            compiled,
            snapshot,
            now=now,
            scan_source=resolved_scan_source,
            track_repetition=resolved_scan_source != "message_edit",
            channel_id=getattr(getattr(message, "channel", None), "id", None),
        )

    async def handle_message_edit(self, before: discord.Message, after: discord.Message) -> ShieldDecision | None:
        if not self.storage_ready or getattr(after, "guild", None) is None:
            return None
        if getattr(getattr(after, "author", None), "id", None) == getattr(getattr(self.bot, "user", None), "id", None):
            return None

        compiled = self._compiled_configs.get(after.guild.id)
        if compiled is None or not compiled.module_enabled:
            return None
        if not self._message_in_scope(compiled, after):
            return None

        now = asyncio.get_running_loop().time()
        self._prune_runtime_state(now)
        before_snapshot = _build_message_snapshot(before)
        after_snapshot = _build_message_snapshot(after)
        if before_snapshot == after_snapshot:
            return None
        return await self._scan_message(
            after,
            compiled,
            after_snapshot,
            now=now,
            scan_source="message_edit",
            track_repetition=False,
            channel_id=getattr(getattr(after, "channel", None), "id", None),
        )

    async def handle_member_join(self, member: discord.Member) -> ShieldRaidEvidence | None:
        if not self.storage_ready or getattr(member, "guild", None) is None or getattr(member, "bot", False):
            return None
        compiled = self._compiled_configs.get(member.guild.id)
        if compiled is None or not compiled.module_enabled or not compiled.spam.enabled:
            return None
        now = asyncio.get_running_loop().time()
        self._prune_runtime_state(now)
        recent_account, _early_member, _newcomer = self._member_age_flags(member)
        rows = [
            row
            for row in self._recent_join_waves.get(member.guild.id, [])
            if now - row[0] <= RAID_JOIN_WINDOW_SECONDS and int(row[1]) != int(member.id)
        ]
        rows.append((now, int(member.id), bool(recent_account)))
        if len(rows) > RAID_JOIN_EVENT_LIMIT:
            rows = rows[-RAID_JOIN_EVENT_LIMIT:]
        self._recent_join_waves[member.guild.id] = rows
        raid_evidence = self._build_join_wave_evidence(member.guild.id, now=now)
        if raid_evidence.join_wave:
            await self._send_raid_watch_alert(member.guild, compiled, raid_evidence)
        return raid_evidence

    def _track_spam_event(
        self,
        guild_id: int,
        user_id: int,
        snapshot: ShieldSnapshot,
        *,
        now: float,
    ) -> tuple[ShieldSpamEvent, tuple[ShieldSpamEvent, ...]]:
        key = (guild_id, user_id)
        rows = [
            event
            for event in self._recent_spam_events.get(key, [])
            if now - event.timestamp <= SPAM_EVENT_WINDOW_SECONDS
        ]
        media_only_links = bool(snapshot.links) and all(link.category == "media_embed" for link in snapshot.links)
        link_signature = self._raid_signature_for_links(snapshot)
        current = ShieldSpamEvent(
            timestamp=now,
            exact_fingerprint=snapshot.exact_fingerprint,
            near_text=snapshot.near_duplicate_text,
            near_fingerprint=snapshot.near_duplicate_fingerprint,
            has_links=snapshot.has_links,
            link_signature=link_signature,
            media_only_links=media_only_links,
            invite_codes=snapshot.invite_codes,
            mention_count=snapshot.mention_count,
            everyone_here_count=snapshot.everyone_here_count,
            emoji_count=snapshot.emoji_count,
            plain_word_count=snapshot.plain_word_count,
            low_value_text=snapshot.low_value_text,
            repeated_char_run=snapshot.repeated_char_run,
            is_gif_message=snapshot.is_gif_message,
            gif_signature=snapshot.gif_signature,
            gif_only=snapshot.gif_only,
            gif_low_text=snapshot.gif_low_text,
        )
        rows.append(current)
        if len(rows) > SPAM_EVENT_LIMIT_PER_USER:
            rows = rows[-SPAM_EVENT_LIMIT_PER_USER:]
        self._recent_spam_events[key] = rows
        return current, tuple(rows)

    def _track_channel_activity(
        self,
        guild_id: int,
        channel_id: int | None,
        user_id: int,
        snapshot: ShieldSnapshot,
        *,
        now: float,
    ) -> tuple[int, int]:
        if channel_id is None:
            return 0, 0
        key = (guild_id, channel_id)
        rows = [
            row
            for row in self._recent_channel_activity.get(key, [])
            if now - row[0] <= HEALTHY_CHAT_WINDOW_SECONDS
        ]
        quality_message = snapshot.plain_word_count >= 3 and not snapshot.low_value_text
        rows.append((now, user_id, snapshot.plain_word_count, quality_message))
        if len(rows) > 100:
            rows = rows[-100:]
        self._recent_channel_activity[key] = rows
        distinct_authors = {author_id for _timestamp, author_id, _words, _quality in rows}
        quality_authors = {author_id for _timestamp, author_id, _words, quality in rows if quality}
        return len(distinct_authors), len(quality_authors)

    def _build_join_wave_evidence(self, guild_id: int, *, now: float) -> ShieldRaidEvidence:
        rows = [
            row
            for row in self._recent_join_waves.get(guild_id, [])
            if now - row[0] <= RAID_JOIN_WINDOW_SECONDS
        ]
        join_count_60s = sum(1 for timestamp, _user_id, _fresh in rows if now - timestamp <= RAID_JOIN_SHORT_WINDOW_SECONDS)
        join_count_5m = len(rows)
        fresh_join_count_60s = sum(1 for timestamp, _user_id, fresh in rows if fresh and now - timestamp <= RAID_JOIN_SHORT_WINDOW_SECONDS)
        fresh_join_count_5m = sum(1 for _timestamp, _user_id, fresh in rows if fresh)
        join_wave = join_count_60s >= 5 or join_count_5m >= 8
        fresh_join_wave = join_wave and (fresh_join_count_60s >= 4 or fresh_join_count_5m >= 6)
        return ShieldRaidEvidence(
            join_count_60s=join_count_60s,
            join_count_5m=join_count_5m,
            fresh_join_count_60s=fresh_join_count_60s,
            fresh_join_count_5m=fresh_join_count_5m,
            join_wave=join_wave,
            fresh_join_wave=fresh_join_wave,
        )

    def _raid_signature_for_links(self, snapshot: ShieldSnapshot) -> str | None:
        if not snapshot.canonical_links:
            return None
        joined = "|".join(sorted(snapshot.canonical_links)[:3])
        return hashlib.sha1(joined.encode("utf-8")).hexdigest()

    def _message_raid_signatures(
        self,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
    ) -> tuple[tuple[str, str], ...]:
        signatures: list[tuple[str, str]] = []
        if snapshot.exact_fingerprint is not None and len(snapshot.context_text) >= 6:
            signatures.append(("exact", snapshot.exact_fingerprint))
        if snapshot.near_duplicate_fingerprint is not None:
            signatures.append(("near", snapshot.near_duplicate_fingerprint))
        if snapshot.invite_codes:
            invite_signature = "|".join(sorted(snapshot.invite_codes))
            signatures.append(("invite", invite_signature))
        link_signature = self._raid_signature_for_links(snapshot)
        if link_signature is not None:
            signatures.append(("link", link_signature))
        if snapshot.gif_signature is not None:
            signatures.append(("gif", snapshot.gif_signature))
        primary_assessment = self._primary_risky_assessment(link_assessments)
        if primary_assessment is not None:
            signatures.append(("risky_domain", primary_assessment.normalized_domain))
        return tuple(dict.fromkeys(signatures))

    def _track_raid_pattern_signature(
        self,
        guild_id: int,
        *,
        kind: str,
        signature: str,
        user_id: int,
        now: float,
    ) -> int:
        key = (guild_id, kind, signature)
        active = {
            int(existing_user_id): timestamp
            for timestamp, existing_user_id in self._recent_raid_patterns.get(key, [])
            if now - timestamp <= RAID_PATTERN_WINDOW_SECONDS
        }
        active[int(user_id)] = now
        rows = sorted(((timestamp, existing_user_id) for existing_user_id, timestamp in active.items()), key=lambda item: item[0])
        if len(rows) > RAID_PATTERN_USERS_PER_SIGNATURE_LIMIT:
            rows = rows[-RAID_PATTERN_USERS_PER_SIGNATURE_LIMIT :]
        self._recent_raid_patterns[key] = rows
        return len(rows)

    def _build_raid_evidence(
        self,
        message: discord.Message,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        now: float,
        scan_source: str,
        author_kind: str,
        newcomer_early_message: bool,
        recent_account: bool,
    ) -> ShieldRaidEvidence:
        base = self._build_join_wave_evidence(message.guild.id, now=now)
        if author_kind != "human" or scan_source != "new_message" or not (newcomer_early_message or recent_account):
            return base
        best_size = 0
        best_kind: str | None = None
        best_signature: str | None = None
        for kind, signature in self._message_raid_signatures(snapshot, link_assessments):
            if not signature:
                continue
            size = self._track_raid_pattern_signature(
                message.guild.id,
                kind=kind,
                signature=signature,
                user_id=int(getattr(message.author, "id", 0) or 0),
                now=now,
            )
            if size > best_size:
                best_size = size
                best_kind = kind
                best_signature = signature
        threshold = 2 if base.fresh_join_wave else 3
        confirmed = bool(base.join_wave and best_size >= threshold)
        return ShieldRaidEvidence(
            join_count_60s=base.join_count_60s,
            join_count_5m=base.join_count_5m,
            fresh_join_count_60s=base.fresh_join_count_60s,
            fresh_join_count_5m=base.fresh_join_count_5m,
            join_wave=base.join_wave,
            fresh_join_wave=base.fresh_join_wave,
            pattern_cluster_size=best_size,
            pattern_kind=best_kind,
            pattern_signature=best_signature,
            confirmed=confirmed,
        )

    def _detect_spam(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        recent_events: Sequence[ShieldSpamEvent],
        *,
        now: float,
        scam_context: ShieldScamContext,
        raid_evidence: ShieldRaidEvidence,
        author_kind: str,
        distinct_channel_authors: int = 0,
        quality_channel_authors: int = 0,
    ) -> list[ShieldMatch]:
        settings = compiled.spam
        if not settings.enabled or author_kind != "human":
            return []
        attachment_name_text = " ".join(name for name in snapshot.attachment_names if name).strip()
        attachment_meta_only = (
            bool(snapshot.attachment_names)
            and set(snapshot.surface_labels) == {"attachment_meta"}
            and not snapshot.has_links
            and not snapshot.has_suspicious_attachment
            and snapshot.mention_count == 0
            and snapshot.emoji_count == 0
            and snapshot.everyone_here_count == 0
            and bool(snapshot.context_text)
            and snapshot.context_text.strip() == attachment_name_text
        )
        if attachment_meta_only:
            return []

        facts: list[dict[str, Any]] = []
        exact_window = [
            event
            for event in recent_events
            if snapshot.exact_fingerprint is not None
            and event.exact_fingerprint == snapshot.exact_fingerprint
            and now - event.timestamp <= SPAM_EXACT_WINDOW_SECONDS
        ]
        exact_threshold = _sensitivity_threshold(settings.sensitivity, low=5, normal=4, high=3)
        if len(exact_window) >= exact_threshold:
            facts.append(
                {
                    "match_class": "spam_duplicate",
                    "label": "Repeated duplicate spam",
                    "reason": f"The same message was posted {len(exact_window)} times inside 90 seconds.",
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.exact_fingerprint,
                }
            )

        near_threshold = _sensitivity_threshold(settings.sensitivity, low=6, normal=5, high=4)
        near_hits = 0
        near_duplicate_context = bool(
            snapshot.low_value_text
            or snapshot.has_links
            or snapshot.invite_codes
            or snapshot.mention_count > 0
            or snapshot.emoji_count >= 10
            or snapshot.repeated_char_run >= 12
            or snapshot.plain_word_count <= 7
            or scam_context.newcomer_early_message
            or raid_evidence.confirmed
        )
        if snapshot.near_duplicate_text:
            for event in recent_events:
                if now - event.timestamp > SPAM_NEAR_WINDOW_SECONDS or not event.near_text:
                    continue
                if difflib.SequenceMatcher(None, snapshot.near_duplicate_text, event.near_text).ratio() >= 0.88:
                    near_hits += 1
        if near_hits >= near_threshold and near_duplicate_context:
            facts.append(
                {
                    "match_class": "spam_near_duplicate",
                    "label": "Repeated near-duplicate spam",
                    "reason": f"Near-identical variants were posted {near_hits} times inside 120 seconds.",
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.near_duplicate_fingerprint,
                }
            )

        burst_events = [event for event in recent_events if now - event.timestamp <= SPAM_BURST_WINDOW_SECONDS]
        burst_threshold = _sensitivity_threshold(settings.sensitivity, low=8, normal=7, high=6)
        burst_count = len(burst_events)
        burst_suspicious = any(
            event.low_value_text
            or bool(event.invite_codes)
            or event.has_links
            or event.mention_count > 0
            or event.emoji_count >= 10
            or event.repeated_char_run >= 12
            for event in burst_events
        )
        if burst_count >= burst_threshold and burst_suspicious:
            facts.append(
                {
                    "match_class": "spam_burst",
                    "label": "Fast burst posting",
                    "reason": f"{burst_count} fast posts landed inside 12 seconds with spam-like pacing.",
                    "base_confidence": "low",
                    "strong": False,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )

        link_events = [event for event in recent_events if event.has_links and now - event.timestamp <= SPAM_LINK_WINDOW_SECONDS]
        link_threshold = _sensitivity_threshold(settings.sensitivity, low=5, normal=4, high=3)
        if link_events:
            if all(event.media_only_links for event in link_events):
                link_threshold += 1
            distinct_link_signatures = {event.link_signature for event in link_events if event.link_signature and not event.invite_codes}
            diversified_link_pressure = len(distinct_link_signatures) >= 2 or raid_evidence.confirmed
            if len(link_events) >= link_threshold and diversified_link_pressure:
                facts.append(
                    {
                        "match_class": "spam_link_flood",
                        "label": "Repeated link flood",
                        "reason": f"{len(link_events)} link drops landed inside 45 seconds.",
                        "base_confidence": "medium",
                        "strong": True,
                        "signature": self._raid_signature_for_links(snapshot),
                    }
                )

        invite_events = [event for event in recent_events if event.invite_codes and now - event.timestamp <= SPAM_INVITE_WINDOW_SECONDS]
        invite_threshold = _sensitivity_threshold(settings.sensitivity, low=4, normal=3, high=3)
        distinct_invites = sorted({code for event in invite_events for code in event.invite_codes})
        if len(invite_events) >= invite_threshold or (len(distinct_invites) >= 2 and len(recent_events) >= 4):
            facts.append(
                {
                    "match_class": "spam_invite_flood",
                    "label": "Repeated invite flood",
                    "reason": (
                        f"{len(invite_events)} invite drops landed inside 60 seconds."
                        if len(invite_events) >= invite_threshold
                        else f"Multiple invite codes rotated across {len(recent_events)} quick messages."
                    ),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": "|".join(distinct_invites) if distinct_invites else None,
                }
            )

        mention_window = [event for event in recent_events if event.mention_count > 0 and now - event.timestamp <= SPAM_MENTION_WINDOW_SECONDS]
        recent_mention_sum = sum(event.mention_count for event in mention_window[-3:])
        single_mention_threshold = 4 if snapshot.everyone_here_count > 0 else 8
        if snapshot.mention_count >= single_mention_threshold or recent_mention_sum >= 12:
            facts.append(
                {
                    "match_class": "spam_mention_flood",
                    "label": "Mention flood",
                    "reason": (
                        f"The message tagged {snapshot.mention_count} accounts in one shot."
                        if snapshot.mention_count >= single_mention_threshold
                        else f"{recent_mention_sum} mentions were stacked across 3 quick messages."
                    ),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.exact_fingerprint or snapshot.near_duplicate_fingerprint,
                }
            )

        if (snapshot.plain_word_count <= 6 and snapshot.emoji_count >= 20) or snapshot.emoji_count >= 35:
            facts.append(
                {
                    "match_class": "spam_emoji_flood",
                    "label": "Emoji flood",
                    "reason": f"The message packed {snapshot.emoji_count} emoji tokens with very little plain text.",
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )

        gif_events = [event for event in recent_events if event.is_gif_message and now - event.timestamp <= SPAM_GIF_WINDOW_SECONDS]
        repeated_same_gif = [
            event
            for event in recent_events
            if snapshot.gif_signature is not None
            and event.gif_signature == snapshot.gif_signature
            and now - event.timestamp <= SPAM_GIF_REPEAT_WINDOW_SECONDS
        ]
        gif_low_text_events = [event for event in gif_events if event.gif_low_text]
        gif_only_events = [event for event in gif_events if event.gif_only]
        if snapshot.is_gif_message:
            gif_ratio = len(gif_events) / max(1, len(recent_events))
            if (
                len(gif_events) >= 6
                and (len(gif_low_text_events) >= 3 or len(gif_only_events) >= 2)
                and gif_ratio >= 0.8
            ) or len(repeated_same_gif) >= 6:
                facts.append(
                    {
                        "match_class": "spam_gif_flood",
                        "label": "GIF flood",
                        "reason": (
                            f"{len(gif_events)} GIF-heavy posts landed inside 45 seconds with very little real text."
                            if len(repeated_same_gif) < 3
                            else f"The same GIF asset was repeated {len(repeated_same_gif)} times inside 30 seconds."
                        ),
                        "base_confidence": "medium",
                        "strong": True,
                        "signature": snapshot.gif_signature or snapshot.near_duplicate_fingerprint,
                    }
                )

        low_value_events = [event for event in recent_events if event.low_value_text and now - event.timestamp <= SPAM_LOW_VALUE_WINDOW_SECONDS]
        if len(low_value_events) >= 5:
            facts.append(
                {
                    "match_class": "spam_low_value_noise",
                    "label": "Repeated low-value noise",
                    "reason": f"{len(low_value_events)} short low-value messages landed inside 60 seconds.",
                    "base_confidence": "low",
                    "strong": False,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )
        if snapshot.repeated_char_run >= 12 and (burst_count >= max(2, burst_threshold - 2) or len(exact_window) >= 2 or near_hits >= 3):
            facts.append(
                {
                    "match_class": "spam_padding_noise",
                    "label": "Character-padding spam",
                    "reason": "Repeated character padding was combined with duplicate or burst spam behavior.",
                    "base_confidence": "low",
                    "strong": False,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )

        if not facts:
            return []

        healthy_chat = (
            distinct_channel_authors >= HEALTHY_CHAT_AUTHOR_THRESHOLD
            and quality_channel_authors >= max(3, HEALTHY_CHAT_AUTHOR_THRESHOLD - 1)
            and not raid_evidence.confirmed
        )
        strong_count = sum(1 for fact in facts if fact["strong"])
        corroborated = strong_count >= 2
        matches: list[ShieldMatch] = []
        for fact in facts:
            confidence = fact["base_confidence"]
            if fact["strong"] and corroborated:
                confidence = "high"
            elif not fact["strong"] and strong_count >= 1:
                confidence = "medium"
            if healthy_chat and fact["match_class"] in {"spam_burst", "spam_emoji_flood", "spam_gif_flood", "spam_low_value_noise"}:
                if confidence == "high":
                    confidence = "medium"
                elif confidence == "medium":
                    confidence = "low"
            match = self._make_pack_match(
                pack="spam",
                settings=settings,
                label=fact["label"],
                reason=fact["reason"],
                confidence=confidence,
                heuristic=True,
                match_class=fact["match_class"],
            )
            matches.append(match)
        return self._dedupe_matches(matches)

    def _apply_raid_confidence_lift(
        self,
        compiled: CompiledShieldConfig,
        matches: Sequence[ShieldMatch],
        *,
        raid_evidence: ShieldRaidEvidence,
        scam_context: ShieldScamContext,
    ) -> tuple[tuple[ShieldMatch, ...], bool]:
        if not raid_evidence.confirmed or not scam_context.newcomer_early_message:
            return tuple(matches), False
        boosted: list[ShieldMatch] = []
        lifted = False
        for match in matches:
            if match.pack not in {"spam", "scam"} or _confidence_rank(match.confidence) < _confidence_rank("medium"):
                boosted.append(match)
                continue
            new_confidence = _boost_confidence(match.confidence)
            if new_confidence == match.confidence:
                boosted.append(match)
                continue
            lifted = True
            boosted.append(
                ShieldMatch(
                    pack=match.pack,
                    label=match.label,
                    reason=f"{match.reason} Active raid corroboration raised confidence.",
                    action=compiled.pack_settings(match.pack).action_for_confidence(new_confidence),
                    confidence=new_confidence,
                    heuristic=match.heuristic,
                    match_class=match.match_class,
                )
            )
        boosted.sort(
            key=lambda item: (
                ACTION_STRENGTH.get(item.action, 0),
                CONFIDENCE_STRENGTH.get(item.confidence, 0),
                PACK_STRENGTH.get(item.pack, 0),
            ),
            reverse=True,
        )
        return tuple(boosted), lifted

    async def _scan_message(
        self,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        *,
        now: float,
        scan_source: str,
        track_repetition: bool,
        channel_id: int | None,
    ) -> ShieldDecision | None:
        alert_content_fingerprint = _alert_content_fingerprint(snapshot)
        author_kind = _author_kind_for_message(message, scan_source=scan_source)
        repetition = (
            self._track_repetitive_promo(message, compiled, snapshot, now)
            if track_repetition
            else RepetitionSignals(None, 0, False, False)
        )
        recent_spam_events: tuple[ShieldSpamEvent, ...] = ()
        distinct_channel_authors = 0
        quality_channel_authors = 0
        if author_kind == "human" and scan_source == "new_message":
            _spam_event, recent_spam_events = self._track_spam_event(
                message.guild.id,
                int(getattr(message.author, "id", 0) or 0),
                snapshot,
                now=now,
            )
            distinct_channel_authors, quality_channel_authors = self._track_channel_activity(
                message.guild.id,
                channel_id,
                int(getattr(message.author, "id", 0) or 0),
                snapshot,
                now=now,
            )
        link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
        scam_context = self._build_scam_context(
            message,
            snapshot,
            link_assessments,
            now=now,
            scan_source=scan_source,
        )
        raid_evidence = self._build_raid_evidence(
            message,
            snapshot,
            link_assessments,
            now=now,
            scan_source=scan_source,
            author_kind=author_kind,
            newcomer_early_message=scam_context.newcomer_early_message,
            recent_account=scam_context.recent_account,
        )
        matches = self._collect_matches(
            compiled,
            snapshot,
            repetitive_promo=repetition,
            link_assessments=link_assessments,
            scan_source=scan_source,
            scam_context=scam_context,
            raid_evidence=raid_evidence,
            recent_spam_events=recent_spam_events,
            author_kind=author_kind,
            now=now,
            channel_id=channel_id,
            distinct_channel_authors=distinct_channel_authors,
            quality_channel_authors=quality_channel_authors,
        )
        matches, _ = self._apply_allow_phrase_suppression(matches, allow_phrase=self._matching_allow_phrase(compiled, snapshot))
        matches, raid_confidence_lifted = self._apply_raid_confidence_lift(
            compiled,
            matches,
            raid_evidence=raid_evidence,
            scam_context=scam_context,
        )
        if raid_confidence_lifted:
            raid_evidence = ShieldRaidEvidence(
                join_count_60s=raid_evidence.join_count_60s,
                join_count_5m=raid_evidence.join_count_5m,
                fresh_join_count_60s=raid_evidence.fresh_join_count_60s,
                fresh_join_count_5m=raid_evidence.fresh_join_count_5m,
                join_wave=raid_evidence.join_wave,
                fresh_join_wave=raid_evidence.fresh_join_wave,
                pattern_cluster_size=raid_evidence.pattern_cluster_size,
                pattern_kind=raid_evidence.pattern_kind,
                pattern_signature=raid_evidence.pattern_signature,
                confirmed=raid_evidence.confirmed,
                confidence_lifted=True,
            )
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
            scan_source=scan_source,
            scan_surface_labels=snapshot.surface_labels,
            raid_evidence=raid_evidence if raid_evidence.join_wave or raid_evidence.confirmed else None,
        )
        if best.match_class == "repetitive_link_noise" and repetition.fingerprint is not None:
            decision.alert_evidence_signature = repetition.fingerprint
            decision.alert_evidence_summary = self._repetition_reason(repetition)
        elif best.pack == "spam":
            decision.alert_evidence_signature = raid_evidence.pattern_signature or snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint
            if best.match_class == "spam_duplicate":
                decision.alert_evidence_summary = f"Repeated duplicate spam in a short window ({best.reason.lower()})"
            elif best.match_class == "spam_near_duplicate":
                decision.alert_evidence_summary = best.reason
            elif best.match_class in {"spam_burst", "spam_low_value_noise", "spam_padding_noise", "spam_gif_flood", "spam_emoji_flood"}:
                decision.alert_evidence_summary = best.reason

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
        decision.member_risk_evidence = self._build_member_risk_evidence(
            message,
            decision,
            snapshot,
            link_assessments,
            scam_context=scam_context,
        )

        if best.action not in {"disabled", "detect"}:
            await self._send_alert(message, compiled, decision, content_fingerprint=alert_content_fingerprint, snapshot=snapshot)
        admin_service = getattr(self.bot, "admin_service", None)
        if decision.member_risk_evidence is not None and admin_service is not None:
            with contextlib.suppress(Exception):
                await admin_service.handle_member_risk_message(message, decision)

        return decision

    def _should_request_ai_review(self, compiled: CompiledShieldConfig, decision: ShieldDecision) -> bool:
        if decision.action in {"disabled", "detect"}:
            return False
        if compiled.log_channel_id is None:
            return False
        policy = self.resolve_ai_access_policy(compiled.guild_id)
        if not policy.enabled:
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
        policy = self.resolve_ai_access_policy(message.guild.id)
        max_chars = int(self.ai_provider.diagnostics().get("max_chars") or 340)
        sanitized = sanitize_message_for_ai(snapshot.scan_text, max_chars=max_chars)
        return ShieldAIReviewRequest(
            guild_id=message.guild.id,
            pack=decision.pack,
            local_confidence=top_reason.confidence,
            local_action=decision.action,
            local_labels=tuple(item.label for item in decision.reasons[:3]),
            local_reasons=tuple(item.reason for item in decision.reasons[:2]),
            sanitized_content=sanitized.text,
            sanitized_redaction_count=sanitized.redaction_count,
            sanitized_truncated=sanitized.truncated,
            has_links=snapshot.has_links,
            domains=tuple(sorted(snapshot.domains)[:3]),
            has_suspicious_attachment=snapshot.has_suspicious_attachment,
            attachment_extensions=summarize_attachment_extensions(snapshot.attachment_names),
            invite_detected=bool(snapshot.invite_codes),
            repetitive_promo=repetitive_promo,
            allowed_models=policy.allowed_models,
        )

    def _matching_allow_phrase(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> str | None:
        return next((phrase for phrase in compiled.allow_phrases if phrase in snapshot.text), None)

    def _apply_allow_phrase_suppression(
        self,
        matches: Sequence[ShieldMatch],
        *,
        allow_phrase: str | None,
    ) -> tuple[list[ShieldMatch], bool]:
        if not allow_phrase:
            return list(matches), False
        filtered = [
            match
            for match in matches
            if match.match_class not in ALLOW_PHRASE_SUPPRESSED_MATCH_CLASSES
        ]
        return filtered, len(filtered) != len(matches)

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
        return _domain_in_set(domain, allow_domains)

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

    def _primary_risky_assessment(self, link_assessments: Sequence[ShieldLinkAssessment]) -> ShieldLinkAssessment | None:
        risky = [
            assessment
            for assessment in link_assessments
            if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
        ]
        if not risky:
            return None
        return max(risky, key=_score_link_assessment_for_scam)

    def _primary_risky_domain(self, link_assessments: Sequence[ShieldLinkAssessment]) -> str | None:
        assessment = self._primary_risky_assessment(link_assessments)
        return assessment.normalized_domain if assessment is not None else None

    def _track_newcomer_activity(
        self,
        guild_id: int,
        user_id: int,
        snapshot: ShieldSnapshot,
        *,
        risky_message: bool,
        now: float,
    ) -> tuple[bool, bool, bool]:
        key = (guild_id, user_id)
        state = self._recent_newcomer_activity.get(key)
        if state is None or now - state.last_seen_at > NEWCOMER_ACTIVITY_TTL_SECONDS:
            state = ShieldNewcomerActivityState(first_seen_at=now, last_seen_at=now)
        first_message_with_link = snapshot.has_links and state.message_count == 0
        first_external_link = snapshot.has_links and state.external_link_messages == 0
        early_risky_activity = risky_message and state.message_count < NEWCOMER_MESSAGE_WINDOW
        state.message_count += 1
        if snapshot.has_links:
            state.external_link_messages += 1
        state.last_seen_at = now
        self._recent_newcomer_activity[key] = state
        return first_message_with_link, first_external_link, early_risky_activity

    def _track_campaign_signature(
        self,
        guild_id: int,
        *,
        kind: str,
        signature: str,
        user_id: int,
        now: float,
    ) -> tuple[int, int]:
        key = (guild_id, kind, signature)
        active = {
            int(existing_user_id): timestamp
            for timestamp, existing_user_id in self._recent_scam_campaigns.get(key, [])
            if now - timestamp <= FRESH_CAMPAIGN_WINDOW_SECONDS
        }
        active[int(user_id)] = now
        rows = sorted(((timestamp, existing_user_id) for existing_user_id, timestamp in active.items()), key=lambda item: item[0])
        if len(rows) > CAMPAIGN_USERS_PER_SIGNATURE_LIMIT:
            rows = rows[-CAMPAIGN_USERS_PER_SIGNATURE_LIMIT :]
        self._recent_scam_campaigns[key] = rows
        tight = sum(1 for timestamp, _existing_user_id in rows if now - timestamp <= FRESH_CAMPAIGN_TIGHT_WINDOW_SECONDS)
        wide = len(rows)
        return tight, wide

    def _track_fresh_campaigns(
        self,
        guild_id: int,
        signatures: Sequence[tuple[str, str]],
        *,
        user_id: int,
        now: float,
    ) -> tuple[int, int, tuple[str, ...]]:
        tight_best = 0
        wide_best = 0
        contributing_kinds: list[str] = []
        seen: set[tuple[str, str]] = set()
        for kind, signature in signatures:
            if not signature or (kind, signature) in seen:
                continue
            seen.add((kind, signature))
            tight, wide = self._track_campaign_signature(
                guild_id,
                kind=kind,
                signature=signature,
                user_id=user_id,
                now=now,
            )
            if wide >= 2:
                contributing_kinds.append(kind)
            tight_best = max(tight_best, tight)
            wide_best = max(wide_best, wide)
        return tight_best, wide_best, tuple(dict.fromkeys(contributing_kinds))

    def _build_scam_context(
        self,
        message: discord.Message,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        now: float,
        scan_source: str,
    ) -> ShieldScamContext:
        member = getattr(message, "author", None)
        author_kind = _author_kind_for_message(message, scan_source=scan_source)
        primary_assessment = self._primary_risky_assessment(link_assessments)
        primary_domain = primary_assessment.normalized_domain if primary_assessment is not None else None
        if member is None or author_kind in AUTOMATED_AUTHOR_KINDS:
            return ShieldScamContext(author_kind=author_kind, primary_domain=primary_domain)
        recent_account, early_member, newcomer_early_message = self._member_age_flags(member)
        first_message_with_link = False
        first_external_link = False
        early_risky_activity = False
        risky_message = primary_domain is not None or snapshot.has_suspicious_attachment
        if newcomer_early_message and scan_source == "new_message":
            first_message_with_link, first_external_link, early_risky_activity = self._track_newcomer_activity(
                message.guild.id,
                int(getattr(member, "id", 0) or 0),
                snapshot,
                risky_message=risky_message,
                now=now,
            )
        fresh_campaign_cluster_20m = 0
        fresh_campaign_cluster_30m = 0
        fresh_campaign_kinds: tuple[str, ...] = ()
        if primary_domain is not None and newcomer_early_message and scan_source == "new_message":
            signatures: list[tuple[str, str]] = [("domain", primary_domain)]
            path_shape_signature = _path_query_shape_signature(primary_assessment, domain=primary_domain)
            if path_shape_signature is not None:
                signatures.append(("path_shape", path_shape_signature))
            host_family_signature = _host_family_signature(primary_assessment, domain=primary_domain)
            if host_family_signature is not None:
                signatures.append(("host_family", host_family_signature))
            lure_signature = _scam_lure_fingerprint(snapshot.context_text)
            if lure_signature is not None:
                signatures.append(("lure", lure_signature))
            fresh_campaign_cluster_20m, fresh_campaign_cluster_30m, fresh_campaign_kinds = self._track_fresh_campaigns(
                message.guild.id,
                signatures,
                user_id=int(getattr(member, "id", 0) or 0),
                now=now,
            )
        return ShieldScamContext(
            author_kind=author_kind,
            primary_domain=primary_domain,
            recent_account=recent_account,
            early_member=early_member,
            newcomer_early_message=newcomer_early_message,
            first_message_with_link=first_message_with_link,
            first_external_link=first_external_link,
            early_risky_activity=early_risky_activity,
            fresh_campaign_cluster_20m=fresh_campaign_cluster_20m,
            fresh_campaign_cluster_30m=fresh_campaign_cluster_30m,
            fresh_campaign_kinds=fresh_campaign_kinds,
        )

    def _build_member_risk_evidence(
        self,
        message: discord.Message,
        decision: ShieldDecision,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        scam_context: ShieldScamContext,
    ) -> ShieldMemberRiskEvidence | None:
        if getattr(message, "webhook_id", None) is not None or getattr(getattr(message, "author", None), "bot", False):
            return None
        message_codes: list[str] = []
        context_codes: list[str] = []
        message_match_class: str | None = None
        message_confidence: str | None = None
        top_spam_reason = next((reason for reason in decision.reasons if reason.pack == "spam"), None)
        top_scam_reason = next((reason for reason in decision.reasons if reason.pack == "scam"), None)
        if top_scam_reason is not None:
            if top_scam_reason.confidence == "high":
                message_codes.append("scam_high")
            elif top_scam_reason.confidence == "medium":
                message_codes.append("scam_medium")
            message_match_class = top_scam_reason.match_class or None
            message_confidence = top_scam_reason.confidence
        raid_evidence = decision.raid_evidence or ShieldRaidEvidence()
        spam_is_member_risk_relevant = bool(
            top_spam_reason is not None
            and (
                scam_context.newcomer_early_message
                or scam_context.recent_account
                or raid_evidence.confirmed
                or raid_evidence.pattern_cluster_size >= 2
            )
        )
        if spam_is_member_risk_relevant:
            if top_spam_reason.confidence == "high":
                message_codes.append("spam_high")
            elif top_spam_reason.confidence == "medium":
                message_codes.append("spam_medium")
            if message_match_class is None:
                message_match_class = top_spam_reason.match_class or None
                message_confidence = top_spam_reason.confidence
        if any(assessment.category == MALICIOUS_LINK_CATEGORY for assessment in link_assessments):
            message_codes.append("malicious_link")
            message_match_class = message_match_class or "known_malicious_domain"
            message_confidence = message_confidence or "high"
        elif any(assessment.category == IMPERSONATION_LINK_CATEGORY for assessment in link_assessments):
            message_codes.append("trusted_brand_impersonation")
            message_match_class = message_match_class or "trusted_brand_impersonation_domain"
            message_confidence = message_confidence or "high"
        elif any(assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY for assessment in link_assessments):
            message_codes.append("unknown_suspicious_link")
            message_confidence = message_confidence or "medium"
        if snapshot.has_suspicious_attachment:
            message_codes.append("suspicious_attachment")
        if any(SUSPICIOUS_FILE_RE.search(url) for url in snapshot.urls):
            message_codes.append("cta_download")
        if scam_context.newcomer_early_message:
            context_codes.append("newcomer_early_message")
        if scam_context.first_message_with_link:
            context_codes.append("first_message_link")
        if scam_context.first_external_link:
            context_codes.append("first_external_link")
        if scam_context.early_risky_activity:
            context_codes.append("newcomer_first_messages_risky")
        if scam_context.fresh_campaign_cluster_30m >= 3:
            context_codes.append("fresh_campaign_cluster_3")
        elif scam_context.fresh_campaign_cluster_20m >= 2:
            context_codes.append("fresh_campaign_cluster_2")
        if "path_shape" in scam_context.fresh_campaign_kinds:
            context_codes.append("campaign_path_shape")
        if "host_family" in scam_context.fresh_campaign_kinds:
            context_codes.append("campaign_host_family")
        if "lure" in scam_context.fresh_campaign_kinds:
            context_codes.append("campaign_lure_reuse")
        if raid_evidence.join_wave:
            context_codes.append("raid_join_wave")
        if raid_evidence.fresh_join_wave:
            context_codes.append("raid_fresh_join_wave")
        if raid_evidence.pattern_cluster_size >= (2 if raid_evidence.fresh_join_wave else 3):
            context_codes.append("raid_pattern_cluster")
        ordered_message_codes = tuple(dict.fromkeys(message_codes))
        ordered_context_codes = tuple(dict.fromkeys(context_codes))
        deduped = tuple(dict.fromkeys((*ordered_message_codes, *ordered_context_codes)))
        if not deduped:
            return None
        if not any(
            code
            in {
                "scam_high",
                "scam_medium",
                "spam_high",
                "spam_medium",
                "malicious_link",
                "trusted_brand_impersonation",
                "unknown_suspicious_link",
                "suspicious_attachment",
                "cta_download",
            }
            for code in deduped
        ):
            return None
        return ShieldMemberRiskEvidence(
            message_codes=ordered_message_codes,
            context_codes=ordered_context_codes,
            signal_codes=deduped,
            message_match_class=message_match_class,
            message_confidence=message_confidence,
            scan_source=decision.scan_source,
            primary_domain=scam_context.primary_domain,
        )

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
        scan_source: str = "new_message",
        scam_context: ShieldScamContext | None = None,
        raid_evidence: ShieldRaidEvidence | None = None,
        recent_spam_events: Sequence[ShieldSpamEvent] | None = None,
        author_kind: str = "human",
        now: float | None = None,
        channel_id: int | None = None,
        distinct_channel_authors: int = 0,
        quality_channel_authors: int = 0,
    ) -> list[ShieldMatch]:
        active_link_assessments = tuple(link_assessments or ())
        matches: list[ShieldMatch] = []
        matches.extend(self._detect_privacy(compiled, snapshot))
        matches.extend(self._detect_promo(compiled, snapshot, repetitive_promo=repetitive_promo or RepetitionSignals(None, 0, False, False)))
        matches.extend(self._detect_link_safety_domains(compiled, snapshot, active_link_assessments))
        matches.extend(
            self._detect_spam(
                compiled,
                snapshot,
                tuple(recent_spam_events or ()),
                now=now if now is not None else time.monotonic(),
                scam_context=scam_context or ShieldScamContext(),
                raid_evidence=raid_evidence or ShieldRaidEvidence(),
                author_kind=author_kind,
                distinct_channel_authors=distinct_channel_authors,
                quality_channel_authors=quality_channel_authors,
            )
        )
        matches.extend(self._detect_adult_solicitation(compiled, snapshot, channel_id=channel_id))
        matches.extend(self._detect_severe_harm(compiled, snapshot))
        matches.extend(self._detect_scam(compiled, snapshot, active_link_assessments, scan_source=scan_source, scam_context=scam_context or ShieldScamContext()))
        matches.extend(
            self._detect_link_policy(
                compiled,
                snapshot,
                active_link_assessments,
                existing_matches=matches,
                scam_context=scam_context or ShieldScamContext(),
            )
        )
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
                        reason="A linked domain matched Shield's local malicious-domain intelligence.",
                        confidence="high",
                        heuristic=False,
                        match_class="known_malicious_domain",
                    )
                )
            if assessment.category == IMPERSONATION_LINK_CATEGORY and compiled.scam.enabled:
                if warning_context:
                    continue
                matches.append(
                    self._make_pack_match(
                        pack="scam",
                        settings=compiled.scam,
                        label="Trusted-brand impersonation domain",
                        reason="A linked host closely impersonated a trusted brand or official destination.",
                        confidence="high",
                        heuristic=False,
                        match_class="trusted_brand_impersonation_domain",
                    )
                )
            if assessment.category == ADULT_LINK_CATEGORY and compiled.adult.enabled:
                matches.append(
                    self._make_pack_match(
                        pack="adult",
                        settings=compiled.adult,
                        label="Known adult domain",
                        reason="A linked domain matched Shield's bundled adult-domain intelligence.",
                        confidence="high",
                        heuristic=False,
                        match_class="adult_domain",
                    )
                )
        return self._dedupe_matches(matches)

    def _detect_adult_solicitation(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        *,
        channel_id: int | None,
    ) -> list[ShieldMatch]:
        settings = compiled.adult
        if not settings.enabled or not compiled.adult_solicitation_enabled:
            return []
        if channel_id is not None and channel_id in compiled.adult_solicitation_excluded_channel_ids:
            return []
        raw_text = snapshot.scan_text or snapshot.context_text or ""
        text = fold_confusable_text(snapshot.context_text)
        squashed = squash_for_evasion_checks(text)
        if not text or _adult_solicitation_context_suppressed(text):
            return []

        strong_hits = find_safety_term_hits(ADULT_SOLICIT_STRONG_TERMS, text, squashed)
        euphemism_hits = find_safety_term_hits(ADULT_SOLICIT_EUPHEMISM_TERMS, text, squashed)
        if ADULT_SOLICIT_OF_SIGNAL_RE.search(raw_text):
            euphemism_hits = [*euphemism_hits, "onlyfans_handle"]
        product_hits = find_safety_term_hits(ADULT_SOLICIT_PRODUCT_TERMS, text, squashed)
        product_hit_set = set(product_hits)
        visual_hits = product_hit_set.intersection({"photo", "photos", "pic", "pics", "vid", "video", "videos", "vids"})
        menu_price_only = bool(product_hit_set) and product_hit_set.issubset({"content", "menu", "price", "prices"})
        benign_menu_context = bool(ADULT_SOLICIT_BENIGN_MENU_RE.search(text))
        direct_contact = bool(ADULT_SOLICIT_CONTACT_RE.search(text))
        dm_gate = bool(ADULT_SOLICIT_DM_GATE_RE.search(text))
        sale = bool(ADULT_SOLICIT_SALE_RE.search(text))
        weak_offer = bool(ADULT_SOLICIT_WEAK_OFFER_RE.search(text))
        direct_route_offer = bool(
            ADULT_SOLICIT_DIRECT_ROUTE_RE.search(text)
            or ADULT_SOLICIT_OPEN_DM_RE.search(text)
            or ADULT_SOLICIT_DM_DESTINATION_RE.search(text)
        )
        sales_offer = bool(ADULT_SOLICIT_SALES_OFFER_RE.search(text))
        menu_price_dm_euphemism = bool(ADULT_SOLICIT_DM_MENU_PRICE_RE.search(text))
        if menu_price_dm_euphemism and not benign_menu_context and settings.sensitivity in {"normal", "high"}:
            euphemism_hits = [*euphemism_hits, "dm_menu_price"]
        weak_photo_ad = direct_route_offer and not strong_hits and not euphemism_hits and bool(visual_hits)
        if weak_photo_ad and ADULT_SOLICIT_BENIGN_PHOTO_RE.search(text):
            return []
        if sales_offer and not strong_hits and not euphemism_hits and benign_menu_context:
            return []

        score = 0
        if strong_hits:
            score += 2
        if euphemism_hits:
            score += 2
        if direct_contact or dm_gate:
            score += 1
        if sale:
            score += 1
        if weak_offer and (strong_hits or euphemism_hits):
            score += 1
        if product_hits and (direct_contact or dm_gate or sale or weak_offer):
            score += 1
        if direct_route_offer:
            score += 2
        if sales_offer:
            score += 2
        if not strong_hits and not euphemism_hits and not direct_route_offer and not sales_offer:
            return []

        confidence: str | None = None
        if (strong_hits or euphemism_hits or sales_offer) and (direct_contact or dm_gate) and sale:
            confidence = "high"
        elif strong_hits or euphemism_hits:
            if direct_contact or dm_gate or sale or direct_route_offer or sales_offer:
                confidence = "medium"
            elif settings.sensitivity == "high" and weak_offer:
                confidence = "low"
        elif sales_offer:
            confidence = "medium"
        elif direct_route_offer and not menu_price_only:
            confidence = "medium"
        elif settings.sensitivity == "high" and weak_offer and product_hits and score >= 2:
            confidence = "low"
        if confidence is None:
            return []

        dm_routed = direct_contact or dm_gate or direct_route_offer
        label = "Adult-content DM ad" if dm_routed else "Sexual solicitation"
        reason = (
            "Adult-content offer signals were paired with DM routing and sales language."
            if dm_routed and sale
            else "Adult-content offer signals were paired with DM routing or gated-access language."
            if dm_routed
            else "Adult-content offer signals were paired with solicitation or sales language."
        )
        return [
            self._make_pack_match(
                pack="adult",
                settings=settings,
                label=label,
                reason=reason,
                confidence=confidence,
                heuristic=True,
                match_class="adult_dm_ad" if dm_routed else "adult_solicitation",
            )
        ]

    def _active_severe_terms(self, compiled: CompiledShieldConfig, terms: frozenset[str]) -> frozenset[str]:
        return frozenset(term for term in terms if term not in compiled.severe_removed_terms)

    def _detect_severe_harm(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> list[ShieldMatch]:
        settings = compiled.severe
        if not settings.enabled or not compiled.severe_enabled_categories:
            return []
        text = fold_confusable_text(snapshot.context_text)
        squashed = squash_for_evasion_checks(text)
        if not text or is_harmful_context_suppressed(text, include_disapproval=True):
            return []
        matches: list[ShieldMatch] = []
        matches.extend(self._detect_severe_sexual_exploitation(compiled, text, squashed))
        matches.extend(self._detect_severe_self_harm(compiled, text, squashed))
        matches.extend(self._detect_severe_eliminationist_hate(compiled, text))
        matches.extend(self._detect_severe_slur_abuse(compiled, text, squashed))
        return self._dedupe_matches(matches)

    def _detect_severe_sexual_exploitation(
        self,
        compiled: CompiledShieldConfig,
        text: str,
        squashed: str,
    ) -> list[ShieldMatch]:
        if "sexual_exploitation" not in compiled.severe_enabled_categories:
            return []
        hits = find_safety_term_hits(self._active_severe_terms(compiled, SEVERE_CHILD_ABUSE_TERMS), text, squashed)
        if not hits or not SEVERE_CHILD_ABUSE_ROUTING_RE.search(text):
            return []
        confidence = "high" if any(hit in {"cp", "csam", "csem", "child porn", "child pornography"} for hit in hits) else "medium"
        return [
            self._make_pack_match(
                pack="severe",
                settings=compiled.severe,
                label="Sexual exploitation solicitation",
                reason="Child sexual-abuse material wording was paired with routing, sale, or trade language.",
                confidence=confidence,
                heuristic=True,
                match_class="sexual_exploitation_solicitation",
            )
        ]

    def _detect_severe_self_harm(
        self,
        compiled: CompiledShieldConfig,
        text: str,
        squashed: str,
    ) -> list[ShieldMatch]:
        if "self_harm_encouragement" not in compiled.severe_enabled_categories:
            return []
        if SEVERE_SELF_HARM_NEGATION_RE.search(text) or SEVERE_SUPPORT_CONTEXT_RE.search(text):
            return []
        low_terms = self._active_severe_terms(compiled, SEVERE_SELF_HARM_LOW_TERMS)
        normal_terms = self._active_severe_terms(compiled, SEVERE_SELF_HARM_NORMAL_TERMS)
        high_terms = self._active_severe_terms(compiled, SEVERE_SELF_HARM_HIGH_TERMS)
        active_terms = set(low_terms)
        if compiled.severe.sensitivity in {"normal", "high"}:
            active_terms.update(normal_terms)
        if compiled.severe.sensitivity == "high":
            active_terms.update(high_terms)
        hits = find_safety_term_hits(frozenset(active_terms), text, squashed)
        if not hits:
            return []
        confidence = "high" if any(hit in low_terms for hit in hits) else "medium" if any(hit in normal_terms for hit in hits) else "low"
        return [
            self._make_pack_match(
                pack="severe",
                settings=compiled.severe,
                label="Self-harm encouragement",
                reason="Direct encouragement or imperatives urging self-harm were detected.",
                confidence=confidence,
                heuristic=True,
                match_class="self_harm_encouragement",
            )
        ]

    def _detect_severe_eliminationist_hate(self, compiled: CompiledShieldConfig, text: str) -> list[ShieldMatch]:
        if "eliminationist_hate" not in compiled.severe_enabled_categories:
            return []
        if not SEVERE_PROTECTED_GROUP_RE.search(text):
            return []
        if SEVERE_ELIMINATION_RE.search(text):
            confidence = "high"
        elif compiled.severe.sensitivity == "high" and SEVERE_DEHUMANIZING_RE.search(text):
            confidence = "low"
        else:
            return []
        return [
            self._make_pack_match(
                pack="severe",
                settings=compiled.severe,
                label="Eliminationist hate",
                reason="Protected-group targeting was paired with extermination or dehumanizing language.",
                confidence=confidence,
                heuristic=True,
                match_class="eliminationist_hate",
            )
        ]

    def _detect_severe_slur_abuse(
        self,
        compiled: CompiledShieldConfig,
        text: str,
        squashed: str,
    ) -> list[ShieldMatch]:
        if "severe_slur_abuse" not in compiled.severe_enabled_categories:
            return []
        low_terms = self._active_severe_terms(compiled, SEVERE_SLUR_LOW_TERMS)
        normal_terms = self._active_severe_terms(compiled, SEVERE_SLUR_NORMAL_TERMS)
        active_terms = set(low_terms)
        if compiled.severe.sensitivity in {"normal", "high"}:
            active_terms.update(normal_terms)
        active_terms.update(compiled.severe_custom_terms)
        hits = find_safety_term_hits(frozenset(active_terms), text, squashed)
        targeted = bool(SEVERE_TARGETING_RE.search(text))
        standalone = len([part for part in text.split(" ") if part]) <= 4
        if hits and (targeted or standalone):
            confidence = "high" if any(hit in low_terms for hit in hits) else "medium"
            return [
                self._make_pack_match(
                    pack="severe",
                    settings=compiled.severe,
                    label="Severe slur abuse",
                    reason="Severe slur language appeared in a directed abusive context.",
                    confidence=confidence,
                    heuristic=True,
                    match_class="severe_slur_abuse",
                )
            ]
        if compiled.severe.sensitivity == "high":
            degrading_hits = find_safety_term_hits(SEVERE_DEGRADING_TERMS, text, squashed)
            if degrading_hits and targeted:
                return [
                    self._make_pack_match(
                        pack="severe",
                        settings=compiled.severe,
                        label="Extreme degrading abuse",
                        reason="Targeted dehumanizing abuse crossed the severe-harm threshold.",
                        confidence="low",
                        heuristic=True,
                        match_class="targeted_extreme_degradation",
                    )
                ]
        return []

    def _link_is_trusted_under_policy(
        self,
        compiled: CompiledShieldConfig,
        link: ShieldLink,
        assessment: ShieldLinkAssessment | None,
    ) -> bool:
        if link.invite_code is not None:
            return link.invite_code in compiled.allow_invite_codes
        if assessment is not None and assessment.category in {
            MALICIOUS_LINK_CATEGORY,
            IMPERSONATION_LINK_CATEGORY,
            ADULT_LINK_CATEGORY,
            UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
        }:
            return False
        if self._domain_is_allowlisted(link.domain, compiled.allow_domains):
            return True
        return self._domain_is_builtin_trusted_under_policy(compiled, link.domain, assessment)

    def _domain_is_builtin_trusted_under_policy(
        self,
        compiled: CompiledShieldConfig,
        domain: str,
        assessment: ShieldLinkAssessment | None,
    ) -> bool:
        if self._domain_is_allowlisted(domain, compiled.trusted_builtin_disabled_domains):
            return False
        if link_domain_in_set(domain, TRUSTED_ONLY_BUILTIN_DOMAINS):
            return True
        safe_family = assessment.safe_family if assessment is not None else None
        return safe_family in TRUSTED_ONLY_BUILTIN_FAMILIES and safe_family not in compiled.trusted_builtin_disabled_families

    def _detect_link_policy(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        existing_matches: Sequence[ShieldMatch],
        scam_context: ShieldScamContext,
    ) -> list[ShieldMatch]:
        settings = compiled.link_policy
        if not settings.enabled or compiled.link_policy_mode == DEFAULT_SHIELD_LINK_POLICY_MODE or not snapshot.links:
            return []
        if any(match.pack in {"scam", "adult"} for match in existing_matches):
            return []

        assessments_by_domain = {assessment.normalized_domain: assessment for assessment in link_assessments}
        matches: list[ShieldMatch] = []
        seen_classes: set[str] = set()
        for link in snapshot.links:
            assessment = assessments_by_domain.get(link.domain)
            if self._link_is_trusted_under_policy(compiled, link, assessment):
                continue
            automated_author = scam_context.author_kind in AUTOMATED_AUTHOR_KINDS
            if automated_author:
                if link.invite_code is not None:
                    continue
                if assessment is None:
                    continue
                if assessment.category == UNKNOWN_LINK_CATEGORY:
                    continue
                if assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY and not assessment.provider_lookup_warranted:
                    continue
                if (
                    link.category in {"shortener", "storefront"}
                    or link_domain_in_set(link.domain, LINK_IN_BIO_DOMAINS)
                ) and assessment.category not in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY, ADULT_LINK_CATEGORY}:
                    continue

            label = "Untrusted external link"
            reason = "Shield trusted-only mode allows only trusted destinations or admin allowlist entries. This link was neither trusted nor allowlisted."
            confidence = "low"
            heuristic = True
            match_class = "untrusted_external_link"

            if link.invite_code is not None:
                label = "Untrusted invite link"
                reason = "Shield trusted-only mode requires Discord invites to be explicitly allowlisted. This invite was not on the invite allowlist."
                confidence = "medium"
                heuristic = False
                match_class = "untrusted_invite_link"
            elif assessment is not None and assessment.category == MALICIOUS_LINK_CATEGORY:
                label = "Known malicious domain"
                reason = "Shield trusted-only mode blocked a domain that matched local malicious-link intelligence."
                confidence = "high"
                heuristic = False
                match_class = "link_policy_malicious"
            elif assessment is not None and assessment.category == IMPERSONATION_LINK_CATEGORY:
                label = "Trusted-brand impersonation domain"
                reason = "Shield trusted-only mode blocked a host that locally impersonated a trusted or official destination."
                confidence = "high"
                heuristic = False
                match_class = "link_policy_impersonation"
            elif assessment is not None and assessment.category == ADULT_LINK_CATEGORY:
                label = "Known adult domain"
                reason = "Shield trusted-only mode blocked a domain that matched local adult-domain intelligence."
                confidence = "high"
                heuristic = False
                match_class = "link_policy_adult"
            elif link.category in {"shortener", "storefront"} or link_domain_in_set(link.domain, LINK_IN_BIO_DOMAINS):
                label = "Blocked link hub / storefront"
                reason = "Shield trusted-only mode blocks shorteners, link-in-bio hubs, and storefront-style destinations unless admins explicitly allow them."
                confidence = "medium"
                heuristic = False
                match_class = "blocked_link_hub"
            elif assessment is not None and assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
                reason = "Shield trusted-only mode blocked an untrusted destination that also carried local suspicious-link signals."
                confidence = "medium"
                heuristic = False
                match_class = "link_policy_suspicious"

            if match_class in seen_classes:
                continue
            seen_classes.add(match_class)
            matches.append(
                self._make_pack_match(
                    pack="link_policy",
                    settings=settings,
                    label=label,
                    reason=reason,
                    confidence=confidence,
                    heuristic=heuristic,
                    match_class=match_class,
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

    def _unallowlisted_links(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot) -> tuple[ShieldLink, ...]:
        links_by_canonical: dict[str, ShieldLink] = {}
        for link in snapshot.links:
            if link.canonical_url in links_by_canonical:
                continue
            links_by_canonical[link.canonical_url] = link
        return tuple(sorted(links_by_canonical.values(), key=lambda item: item.canonical_url))

    def _repetition_reason(self, repetitive_promo: RepetitionSignals) -> str:
        hit_count = repetitive_promo.hits
        window_minutes = int(REPETITION_WINDOW_SECONDS // 60)
        if repetitive_promo.evidence_kind == "media_link":
            return (
                f"The same external media link was posted {hit_count} times in {window_minutes} minutes "
                "without enough promo evidence to treat it as self-promo."
            )
        return (
            f"The same external link was posted {hit_count} times in {window_minutes} minutes "
            "without enough promo evidence to treat it as self-promo."
        )

    def _detect_promo(self, compiled: CompiledShieldConfig, snapshot: ShieldSnapshot, *, repetitive_promo: RepetitionSignals) -> list[ShieldMatch]:
        settings = compiled.promo
        if not settings.enabled:
            return []
        matches: list[ShieldMatch] = []
        unallowlisted_links = list(self._unallowlisted_links(compiled, snapshot))
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
                matches.append(
                    self._make_pack_match(
                        pack="promo",
                        settings=settings,
                        label="Repeated media link" if repetitive_promo.pure_media_links else "Repeated external link",
                        reason=self._repetition_reason(repetitive_promo),
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
        unallowlisted_links = self._unallowlisted_links(compiled, snapshot)
        fingerprint, evidence_kind = _link_repetition_fingerprint(unallowlisted_links)
        if fingerprint is None:
            return RepetitionSignals(None, 0, False, False)
        key = (message.guild.id, message.author.id, fingerprint)
        hits = [value for value in self._recent_promos.get(key, []) if now - value <= REPETITION_WINDOW_SECONDS]
        hits.append(now)
        self._recent_promos[key] = hits
        has_unallowlisted_links = bool(unallowlisted_links)
        pure_media_links = bool(unallowlisted_links) and all(link.category == "media_embed" for link in unallowlisted_links)
        return RepetitionSignals(
            fingerprint=fingerprint,
            hits=len(hits),
            pure_media_links=pure_media_links,
            has_unallowlisted_links=has_unallowlisted_links,
            evidence_kind=evidence_kind,
        )

    def _extract_scam_features(
        self,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        scan_source: str,
        scam_context: ShieldScamContext,
    ) -> ShieldScamFeatures:
        risky_domains = [
            assessment.normalized_domain
            for assessment in link_assessments
            if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
        ]
        link_risk_score = max((_score_link_assessment_for_scam(item) for item in link_assessments), default=0)
        shortener_or_punycode = any(
            _domain_in_set(domain, SHORTENER_DOMAINS) or "xn--" in domain
            for domain in risky_domains
        )
        bait = bool(SCAM_BAIT_RE.search(snapshot.context_text) or SCAM_BAIT_RE.search(snapshot.context_squashed))
        social_engineering = bool(SOCIAL_ENGINEERING_RE.search(snapshot.context_text))
        cta = bool(SCAM_CTA_RE.search(snapshot.context_text) or SCAM_CTA_RE.search(snapshot.context_squashed))
        brand_bait = bool(BRAND_BAIT_RE.search(snapshot.context_text))
        official_framing = bool(SCAM_OFFICIAL_FRAMING_RE.search(snapshot.context_text))
        announcement_framing = bool(SCAM_ANNOUNCEMENT_RE.search(snapshot.context_text))
        partnership_framing = bool(SCAM_PARTNERSHIP_RE.search(snapshot.context_text))
        support_framing = bool(SCAM_SUPPORT_RE.search(snapshot.context_text))
        security_notice = bool(SCAM_SECURITY_NOTICE_RE.search(snapshot.context_text))
        fake_authority = bool(SCAM_FAKE_AUTHORITY_RE.search(snapshot.context_text))
        qr_setup_lure = bool(SCAM_QR_SETUP_RE.search(snapshot.context_text) or SCAM_QR_SETUP_RE.search(snapshot.context_squashed))
        community_post_framing = "community post" in snapshot.context_text
        urgency = bool(SCAM_URGENCY_RE.search(snapshot.context_text))
        wallet_or_mint = bool(SCAM_CRYPTO_MINT_RE.search(snapshot.context_text) or SCAM_CRYPTO_MINT_RE.search(snapshot.context_squashed))
        login_or_auth_flow = bool(SCAM_LOGIN_FLOW_RE.search(snapshot.context_text) or SCAM_LOGIN_FLOW_RE.search(snapshot.context_squashed))
        dangerous_link_target = any(SUSPICIOUS_FILE_RE.search(url) for url in snapshot.urls)
        suspicious_attachment_combo = snapshot.has_suspicious_attachment and bool(social_engineering or cta or login_or_auth_flow)
        return ShieldScamFeatures(
            author_kind=scam_context.author_kind,
            link_risk_score=link_risk_score,
            suspicious_link_present=link_risk_score >= 2,
            risky_link_present=link_risk_score >= 3,
            shortener_or_punycode=shortener_or_punycode,
            bait=bait,
            social_engineering=social_engineering,
            cta=cta,
            brand_bait=brand_bait,
            official_framing=official_framing,
            announcement_framing=announcement_framing,
            partnership_framing=partnership_framing,
            support_framing=support_framing,
            security_notice=security_notice,
            fake_authority=fake_authority,
            qr_setup_lure=qr_setup_lure,
            community_post_framing=community_post_framing,
            urgency=urgency,
            wallet_or_mint=wallet_or_mint,
            login_or_auth_flow=login_or_auth_flow,
            dangerous_link_target=dangerous_link_target,
            suspicious_attachment_combo=suspicious_attachment_combo,
            scan_source=scan_source,
            newcomer_early_message=scam_context.newcomer_early_message,
            first_message_with_link=scam_context.first_message_with_link,
            first_external_link=scam_context.first_external_link,
            early_risky_activity=scam_context.early_risky_activity,
            fresh_campaign_cluster_20m=scam_context.fresh_campaign_cluster_20m,
            fresh_campaign_cluster_30m=scam_context.fresh_campaign_cluster_30m,
            fresh_campaign_kinds=scam_context.fresh_campaign_kinds,
        )

    def _should_emit_middle_lane_low(self, features: ShieldScamFeatures) -> bool:
        if features.automated_author:
            return False
        copy_signal_count = sum(
            (
                bool(features.bait),
                bool(features.brand_bait),
                bool(features.official_like_framing),
                bool(features.support_framing),
                bool(features.security_notice),
                bool(features.fake_authority),
                bool(features.qr_setup_lure),
                bool(features.urgency),
                bool(features.wallet_or_mint),
                bool(features.login_or_auth_flow or features.social_engineering or features.cta),
            )
        )
        context_signal_count = sum(
            (
                bool(features.first_message_with_link),
                bool(features.first_external_link),
                bool(features.early_risky_activity),
                bool(features.fresh_campaign_cluster_20m >= 2),
                bool(features.scan_source == "webhook_message" and (features.brand_bait or features.official_like_framing)),
                bool(
                    features.newcomer_early_message
                    and (
                        features.brand_bait
                        or features.official_like_framing
                        or features.wallet_or_mint
                        or features.login_or_auth_flow
                        or features.security_notice
                        or features.fake_authority
                    )
                ),
            )
        )
        return features.suspicious_link_present and copy_signal_count >= 2 and context_signal_count >= 1

    def _automated_author_has_strong_scam_evidence(
        self,
        features: ShieldScamFeatures,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
    ) -> bool:
        if any(assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY} for assessment in link_assessments):
            return True
        if snapshot.has_suspicious_attachment or features.dangerous_link_target or features.suspicious_attachment_combo:
            return True
        if features.link_risk_score < 4:
            return False
        if features.bait:
            return True
        if features.brand_bait and (
            features.wallet_or_mint
            or features.login_or_auth_flow
            or features.social_engineering
            or features.cta
            or features.urgency
            or features.fake_authority
        ):
            return True
        return features.fake_authority and (features.login_or_auth_flow or features.social_engineering or features.cta)

    def _detect_scam(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        *,
        scan_source: str,
        scam_context: ShieldScamContext,
    ) -> list[ShieldMatch]:
        settings = compiled.scam
        if not settings.enabled:
            return []
        if _looks_like_scam_warning(snapshot.context_text):
            return []
        matches: list[ShieldMatch] = []
        features = self._extract_scam_features(
            snapshot,
            link_assessments,
            scan_source=scan_source,
            scam_context=scam_context,
        )
        if features.bait and ((snapshot.has_links and features.risky_link_present) or features.dangerous_link_target):
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Scam bait + link",
                    reason="Gift, claim, or verification bait appeared next to a risky link or download target.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_bait_link",
                )
            )
        elif features.bait and snapshot.has_suspicious_attachment:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Scam bait + suspicious file",
                    reason="Gift, claim, or verification bait appeared next to a suspicious file attachment.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_bait_attachment",
                )
            )
        if features.shortener_or_punycode and (features.social_engineering or features.cta or features.login_or_auth_flow):
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
        if snapshot.has_suspicious_attachment and (features.social_engineering or features.cta or features.login_or_auth_flow):
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
        if features.dangerous_link_target and (features.social_engineering or features.cta or features.login_or_auth_flow):
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
        if features.suspicious_link_present or snapshot.has_suspicious_attachment or features.dangerous_link_target:
            if features.automated_author and not self._automated_author_has_strong_scam_evidence(features, snapshot, link_assessments):
                return self._dedupe_matches(matches)
            weighted_score = features.link_risk_score
            reason_bits: list[str] = []
            if features.bait:
                weighted_score += 2
                reason_bits.append("claim or reward bait")
            if features.social_engineering or features.cta or features.login_or_auth_flow:
                weighted_score += 1
                reason_bits.append("CTA or account-flow wording")
            if features.brand_bait:
                weighted_score += 1
                reason_bits.append("trusted-brand bait")
            if features.support_framing and not features.automated_author:
                weighted_score += 1
                reason_bits.append("support or ticket framing")
            if features.security_notice and not features.automated_author:
                weighted_score += 1
                reason_bits.append("security or session-warning framing")
            if features.fake_authority:
                weighted_score += 1
                reason_bits.append("fake bot, staff, or system framing")
            if features.qr_setup_lure:
                weighted_score += 1
                reason_bits.append("QR, setup, or device-auth lure")
            if features.announcement_framing and not features.automated_author:
                weighted_score += 1
                reason_bits.append("announcement framing")
            if features.partnership_framing and not features.automated_author:
                weighted_score += 1
                reason_bits.append("partnership framing")
            if features.community_post_framing and not features.automated_author:
                weighted_score += 1
                reason_bits.append("community-post framing")
            if features.official_framing and "official-looking framing" not in reason_bits and not features.automated_author:
                weighted_score += 1
                reason_bits.append("official-looking framing")
            if features.urgency:
                weighted_score += 1
                reason_bits.append("urgency or scarcity language")
            if features.wallet_or_mint:
                weighted_score += 1
                reason_bits.append("mint, wallet, or airdrop language")
            if features.brand_bait and features.official_like_framing and (features.urgency or features.login_or_auth_flow or features.social_engineering or features.cta):
                weighted_score += 1
            if not features.automated_author and features.newcomer_early_message and (
                features.brand_bait
                or features.official_like_framing
                or features.cta
                or features.urgency
                or features.wallet_or_mint
                or features.login_or_auth_flow
                or features.security_notice
                or features.fake_authority
                or features.support_framing
            ):
                weighted_score += 1
                reason_bits.append("newcomer early-message delivery")
            if not features.automated_author and features.first_message_with_link:
                weighted_score += 1
                reason_bits.append("first newcomer message carried a link")
            if not features.automated_author and features.first_external_link:
                weighted_score += 1
                reason_bits.append("first newcomer external link")
            if not features.automated_author and features.early_risky_activity:
                weighted_score += 1
                reason_bits.append("risky activity in first few newcomer messages")
            if not features.automated_author and features.fresh_campaign_cluster_30m >= 3:
                weighted_score += 2
                reason_bits.append(f"fresh-account campaign cluster ({_cluster_reason_text(features.fresh_campaign_kinds)})")
            elif not features.automated_author and features.fresh_campaign_cluster_20m >= 2:
                weighted_score += 1
                reason_bits.append(f"repeat fresh-account pattern ({_cluster_reason_text(features.fresh_campaign_kinds)})")
            if snapshot.has_suspicious_attachment:
                weighted_score += 2
                reason_bits.append("suspicious attachment metadata")
            elif features.dangerous_link_target:
                weighted_score += 2
                reason_bits.append("download-style link target")

            confidence: str | None = None
            if features.automated_author and weighted_score >= 7:
                confidence = "high"
            elif features.automated_author and weighted_score >= 6:
                confidence = "medium"
            elif weighted_score >= 7:
                confidence = "high"
            elif weighted_score >= 5:
                confidence = "medium"
            elif weighted_score >= 4 and (settings.sensitivity == "high" or self._should_emit_middle_lane_low(features)):
                confidence = "low"

            if confidence is not None:
                label = "Scam social-engineering pattern"
                match_class = "scam_campaign_lure"
                if confidence == "low" and any(
                    assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY
                    for assessment in link_assessments
                ):
                    label = "Risky unknown-link lure"
                    match_class = "scam_risky_unknown_link"
                elif features.wallet_or_mint and (features.brand_bait or features.official_like_framing):
                    label = "Mint or wallet lure"
                    match_class = "scam_mint_wallet_lure"
                elif features.brand_bait and features.official_like_framing:
                    label = "Official-looking brand lure"
                    match_class = "scam_brand_impersonation"
                strongest_link_signals = _strongest_link_risk_signals(link_assessments)
                if strongest_link_signals:
                    reason_bits.append(f"risky link signals ({', '.join(strongest_link_signals[:3])})")
                matches.append(
                    ShieldMatch(
                        pack="scam",
                        label=label,
                        reason=(
                            "Multiple scam signals combined: "
                            + ", ".join(reason_bits[:5])
                            + "."
                        ),
                        action=settings.action_for_confidence(confidence),
                        confidence=confidence,
                        heuristic=True,
                        match_class=match_class,
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
        self._alert_dedup = {
            key: value for key, value in self._alert_dedup.items() if now - value[0] <= ALERT_DEDUP_SECONDS
        }
        self._alert_signature_dedup = {
            key: value for key, value in self._alert_signature_dedup.items() if now - value <= ALERT_SIGNATURE_DEDUP_SECONDS
        }
        self._raid_alert_dedup = {
            key: value for key, value in self._raid_alert_dedup.items() if now - value <= RAID_ALERT_DEDUP_SECONDS
        }
        self._compact_alert_cohorts = {
            key: value for key, value in self._compact_alert_cohorts.items() if now - value <= LOW_CONFIDENCE_ALERT_COHORT_SECONDS
        }
        self._recent_promos = {
            key: [value for value in values if now - value <= REPETITION_WINDOW_SECONDS]
            for key, values in self._recent_promos.items()
            if any(now - value <= REPETITION_WINDOW_SECONDS for value in values)
        }
        self._recent_scam_campaigns = {
            key: [(timestamp, user_id) for timestamp, user_id in values if now - timestamp <= FRESH_CAMPAIGN_WINDOW_SECONDS]
            for key, values in self._recent_scam_campaigns.items()
            if any(now - timestamp <= FRESH_CAMPAIGN_WINDOW_SECONDS for timestamp, _user_id in values)
        }
        if len(self._recent_scam_campaigns) > CAMPAIGN_SIGNATURE_LIMIT:
            recent_campaign_items = sorted(
                self._recent_scam_campaigns.items(),
                key=lambda item: item[1][-1][0] if item[1] else 0.0,
                reverse=True,
            )[:CAMPAIGN_SIGNATURE_LIMIT]
            self._recent_scam_campaigns = dict(recent_campaign_items)
        self._recent_spam_events = {
            key: [event for event in values if now - event.timestamp <= SPAM_EVENT_WINDOW_SECONDS]
            for key, values in self._recent_spam_events.items()
            if any(now - event.timestamp <= SPAM_EVENT_WINDOW_SECONDS for event in values)
        }
        self._recent_channel_activity = {
            key: [row for row in values if now - row[0] <= HEALTHY_CHAT_WINDOW_SECONDS]
            for key, values in self._recent_channel_activity.items()
            if any(now - row[0] <= HEALTHY_CHAT_WINDOW_SECONDS for row in values)
        }
        self._recent_join_waves = {
            guild_id: [row for row in values if now - row[0] <= RAID_JOIN_WINDOW_SECONDS]
            for guild_id, values in self._recent_join_waves.items()
            if any(now - row[0] <= RAID_JOIN_WINDOW_SECONDS for row in values)
        }
        if len(self._recent_join_waves) > RAID_PATTERN_LIMIT:
            recent_join_items = sorted(
                self._recent_join_waves.items(),
                key=lambda item: item[1][-1][0] if item[1] else 0.0,
                reverse=True,
            )[:RAID_PATTERN_LIMIT]
            self._recent_join_waves = dict(recent_join_items)
        self._recent_raid_patterns = {
            key: [(timestamp, user_id) for timestamp, user_id in values if now - timestamp <= RAID_PATTERN_WINDOW_SECONDS]
            for key, values in self._recent_raid_patterns.items()
            if any(now - timestamp <= RAID_PATTERN_WINDOW_SECONDS for timestamp, _user_id in values)
        }
        if len(self._recent_raid_patterns) > RAID_PATTERN_LIMIT:
            recent_raid_items = sorted(
                self._recent_raid_patterns.items(),
                key=lambda item: item[1][-1][0] if item[1] else 0.0,
                reverse=True,
            )[:RAID_PATTERN_LIMIT]
            self._recent_raid_patterns = dict(recent_raid_items)
        self._recent_newcomer_activity = {
            key: value
            for key, value in self._recent_newcomer_activity.items()
            if now - value.last_seen_at <= NEWCOMER_ACTIVITY_TTL_SECONDS
        }
        if len(self._recent_newcomer_activity) > NEWCOMER_STATE_LIMIT:
            recent_newcomers = sorted(
                self._recent_newcomer_activity.items(),
                key=lambda item: item[1].last_seen_at,
                reverse=True,
            )[:NEWCOMER_STATE_LIMIT]
            self._recent_newcomer_activity = dict(recent_newcomers)
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

    def _should_use_compact_alert(self, decision: ShieldDecision, top_reason: ShieldMatch | None) -> bool:
        return bool(
            top_reason is not None
            and top_reason.heuristic
            and top_reason.confidence == "low"
            and decision.action in LOW_CONFIDENCE_ACTIONS
            and not decision.deleted
            and not decision.timed_out
            and not decision.escalated
        )

    def _should_ping_alert_role(self, decision: ShieldDecision, top_reason: ShieldMatch | None) -> bool:
        if top_reason is None or self._should_use_compact_alert(decision, top_reason):
            return False
        if decision.deleted or decision.timed_out or decision.escalated:
            return True
        return bool(top_reason.pack in {"scam", "adult"} and top_reason.confidence == "high")

    def _compact_alert_cohort_key(
        self,
        message: discord.Message,
        decision: ShieldDecision,
        top_reason: ShieldMatch | None,
        *,
        content_fingerprint: str,
    ) -> tuple[Any, ...]:
        return (
            message.guild.id,
            getattr(message.author, "id", 0),
            decision.pack or "",
            top_reason.match_class if top_reason is not None else "",
            decision.action,
            top_reason.confidence if top_reason is not None else "",
            decision.alert_evidence_signature or content_fingerprint,
        )

    def _build_compact_alert_embed(
        self,
        message: discord.Message,
        decision: ShieldDecision,
        *,
        preview: str,
        attachment_summary: Sequence[str],
        top_reason: ShieldMatch | None,
    ) -> discord.Embed:
        embed = discord.Embed(
            title=f"Shield Note | {PACK_LABELS.get(decision.pack or '', 'Shield')}",
            description=f"{message.author.mention} in {message.channel.mention}",
            color=ge.EMBED_THEME["info"],
        )
        if top_reason is not None:
            embed.add_field(
                name="Detection",
                value=(
                    f"**{top_reason.label}**\n"
                    f"Confidence: {top_reason.confidence.title()}\n"
                    f"Resolved action: {ACTION_LABELS.get(decision.action, decision.action)}"
                ),
                inline=False,
            )
            evidence_summary = decision.alert_evidence_summary or top_reason.reason
            if evidence_summary:
                embed.add_field(name="Why it was noted", value=evidence_summary, inline=False)
        source_summary = SCAN_SOURCE_LABELS.get(decision.scan_source, decision.scan_source.replace("_", " ").title())
        if decision.scan_surface_labels:
            source_summary = f"{source_summary} | Surfaces: {', '.join(decision.scan_surface_labels)}"
        embed.add_field(name="Scan Source", value=source_summary, inline=False)
        embed.add_field(name="Preview", value=preview or "[no text content]", inline=False)
        if attachment_summary:
            embed.add_field(name="Attachments", value="\n".join(attachment_summary[:2]), inline=False)
        embed.add_field(name="Jump", value=f"[Open message]({message.jump_url})", inline=False)
        ge.style_embed(embed, footer="Babblebox Shield | Compact low-confidence note")
        return embed

    async def _send_raid_watch_alert(
        self,
        guild: discord.Guild,
        compiled: CompiledShieldConfig,
        raid_evidence: ShieldRaidEvidence,
    ):
        if compiled.log_channel_id is None or not raid_evidence.join_wave:
            return
        now = asyncio.get_running_loop().time()
        state = "fresh_join_wave" if raid_evidence.fresh_join_wave else "join_wave"
        dedupe_key = (guild.id, "raid_watch", state)
        if now - self._raid_alert_dedup.get(dedupe_key, 0.0) < RAID_ALERT_DEDUP_SECONDS:
            return
        self._raid_alert_dedup[dedupe_key] = now

        channel = self.bot.get_channel(compiled.log_channel_id)
        if channel is None and hasattr(self.bot, "fetch_channel"):
            with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                channel = await self.bot.fetch_channel(compiled.log_channel_id)
        if channel is None:
            return

        guild_name = getattr(guild, "name", f"Guild {guild.id}")
        embed = discord.Embed(
            title="Shield Alert | Spam / Raid",
            description=f"Raid watch is active in **{guild_name}**.",
            color=ge.EMBED_THEME["warning"],
        )
        severity = "fresh-account pressure elevated" if raid_evidence.fresh_join_wave else "join velocity elevated"
        embed.add_field(
            name="Detection",
            value=(
                "**Raid Watch**\n"
                f"State: {severity}\n"
                f"Resolved action: {ACTION_LABELS['log']}"
            ),
            inline=False,
        )
        embed.add_field(
            name="Evidence Basis",
            value=(
                f"Joins in 60s: **{raid_evidence.join_count_60s}**\n"
                f"Joins in 5m: **{raid_evidence.join_count_5m}**\n"
                f"Fresh accounts in 60s: **{raid_evidence.fresh_join_count_60s}**\n"
                f"Fresh accounts in 5m: **{raid_evidence.fresh_join_count_5m}**"
            ),
            inline=False,
        )
        embed.add_field(
            name="Note",
            value=(
                "This is a bounded watch signal only. Babblebox does not auto-lock channels or keep a raid archive here."
            ),
            inline=False,
        )
        ge.style_embed(embed, footer="Babblebox Shield | Log-first and compact by design")
        content = f"<@&{compiled.alert_role_id}>" if compiled.alert_role_id is not None else None
        allowed_mentions = discord.AllowedMentions(users=False, roles=True, everyone=False)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions)

    async def _send_alert(
        self,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        decision: ShieldDecision,
        *,
        content_fingerprint: str,
        snapshot: ShieldSnapshot | None = None,
    ):
        if compiled.log_channel_id is None:
            return
        dedupe_key = (message.guild.id, message.id)
        now = asyncio.get_running_loop().time()
        last_alert = self._alert_dedup.get(dedupe_key)
        if last_alert is not None and now - last_alert[0] < ALERT_DEDUP_SECONDS and last_alert[1] == content_fingerprint:
            return
        top_reason = decision.reasons[0] if decision.reasons else None
        if (
            decision.raid_evidence is not None
            and decision.raid_evidence.confirmed
            and decision.alert_evidence_signature is not None
        ):
            raid_dedupe_key = (
                message.guild.id,
                decision.pack or "",
                decision.alert_evidence_signature,
                "confirmed" if decision.raid_evidence.confirmed else "watch",
            )
            if now - self._raid_alert_dedup.get(raid_dedupe_key, 0.0) < RAID_ALERT_DEDUP_SECONDS:
                return
            self._raid_alert_dedup[raid_dedupe_key] = now
        compact_alert = self._should_use_compact_alert(decision, top_reason)
        if compact_alert:
            cohort_key = self._compact_alert_cohort_key(
                message,
                decision,
                top_reason,
                content_fingerprint=content_fingerprint,
            )
            last_cohort_alert = self._compact_alert_cohorts.get(cohort_key)
            if last_cohort_alert is not None and now - last_cohort_alert < LOW_CONFIDENCE_ALERT_COHORT_SECONDS:
                return
            self._compact_alert_cohorts[cohort_key] = now
        signature_key = (
            message.guild.id,
            getattr(message.channel, "id", 0),
            getattr(message.author, "id", 0),
            decision.pack or "",
            decision.action,
            bool(decision.deleted),
            bool(decision.timed_out),
            bool(decision.escalated),
            top_reason.match_class if top_reason is not None else "",
            content_fingerprint,
        )
        if now - self._alert_signature_dedup.get(signature_key, 0.0) < ALERT_SIGNATURE_DEDUP_SECONDS:
            return
        self._alert_dedup[dedupe_key] = (now, content_fingerprint)
        self._alert_signature_dedup[signature_key] = now

        channel = self.bot.get_channel(compiled.log_channel_id)
        if channel is None and hasattr(self.bot, "fetch_channel"):
            with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                channel = await self.bot.fetch_channel(compiled.log_channel_id)
        if channel is None:
            return

        preview = make_message_preview(message.content, attachments=message.attachments, limit=MAX_MESSAGE_PREVIEW)
        if not preview and snapshot is not None:
            preview = make_message_preview(snapshot.scan_text, limit=MAX_MESSAGE_PREVIEW)
        attachment_summary = make_attachment_labels(message, include_urls=False)
        primary_risky_assessment = self._primary_risky_assessment(decision.link_assessments)
        if compact_alert:
            embed = self._build_compact_alert_embed(
                message,
                decision,
                preview=preview,
                attachment_summary=attachment_summary,
                top_reason=top_reason,
            )
        else:
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
            evidence_lines: list[str] = []
            if primary_risky_assessment is not None:
                evidence_lines.append(_link_assessment_basis(primary_risky_assessment))
                evidence_lines.append(f"Primary risky domain: `{primary_risky_assessment.normalized_domain}`")
            elif top_reason is not None and top_reason.heuristic:
                evidence_lines.append("Combined local heuristic signals drove this match.")
            if top_reason is not None and top_reason.pack == "scam" and top_reason.heuristic:
                signal_codes = set(getattr(getattr(decision, "member_risk_evidence", None), "signal_codes", ()) or ())
                context_bits: list[str] = []
                if "first_message_link" in signal_codes:
                    context_bits.append("first newcomer message carried a link")
                if "first_external_link" in signal_codes:
                    context_bits.append("first newcomer external link")
                if "newcomer_first_messages_risky" in signal_codes:
                    context_bits.append("risky activity in the first newcomer messages")
                if "campaign_path_shape" in signal_codes:
                    context_bits.append("shared risky link shape")
                if "campaign_host_family" in signal_codes:
                    context_bits.append("shared risky host pattern")
                if "campaign_lure_reuse" in signal_codes:
                    context_bits.append("reused lure wording")
                if "fresh_campaign_cluster_2" in signal_codes or "fresh_campaign_cluster_3" in signal_codes:
                    context_bits.append("fresh-account campaign repetition")
                if context_bits:
                    evidence_lines.append("Confidence rose with: " + ", ".join(context_bits[:3]) + ".")
            if decision.raid_evidence is not None and decision.raid_evidence.join_wave:
                raid_lines = [
                    f"Join wave: {decision.raid_evidence.join_count_60s} in 60s / {decision.raid_evidence.join_count_5m} in 5m",
                ]
                if decision.raid_evidence.fresh_join_wave:
                    raid_lines.append(
                        f"Fresh-account pressure: {decision.raid_evidence.fresh_join_count_60s} in 60s / {decision.raid_evidence.fresh_join_count_5m} in 5m"
                    )
                if decision.raid_evidence.pattern_cluster_size >= 2 and decision.raid_evidence.pattern_kind:
                    raid_lines.append(
                        f"Shared newcomer pattern: {decision.raid_evidence.pattern_cluster_size} accounts ({decision.raid_evidence.pattern_kind.replace('_', ' ')})"
                    )
                if decision.raid_evidence.confidence_lifted:
                    raid_lines.append("Active raid corroboration raised the final confidence tier.")
                evidence_lines.extend(raid_lines)
            if evidence_lines:
                embed.add_field(name="Evidence Basis", value="\n".join(evidence_lines), inline=False)
            source_summary = SCAN_SOURCE_LABELS.get(decision.scan_source, decision.scan_source.replace("_", " ").title())
            if decision.scan_surface_labels:
                source_summary = f"{source_summary} | Surfaces: {', '.join(decision.scan_surface_labels)}"
            embed.add_field(name="Scan Source", value=source_summary, inline=False)
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
                        + (
                            f" | signals: {', '.join(signal for signal in item.matched_signals[:3])}"
                            if item.matched_signals
                            else ""
                        )
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
                    f"Tier: `{ai_review.tier}` (target `{ai_review.target_tier}`)",
                    f"Model: `{ai_review.model}`",
                ]
                if ai_review.false_positive:
                    ai_lines.append("Possible false positive: Yes")
                if ai_review.route_reasons:
                    ai_lines.append(f"Route reasons: {', '.join(ai_review.route_reasons[:4])}")
                if ai_review.policy_capped:
                    ai_lines.append("Policy cap: Stronger tier was blocked by this guild's allowed-model policy.")
                if ai_review.fallback_used and ai_review.attempted_models:
                    ai_lines.append(f"Fallback: {' -> '.join(ai_review.attempted_models)}")
                ai_lines.append(ai_review.explanation)
                embed.add_field(name="AI Assist", value="\n".join(ai_lines), inline=False)
            embed.add_field(name="Jump", value=f"[Open message]({message.jump_url})", inline=True)
            if top_reason is not None and top_reason.pack == "scam" and top_reason.heuristic:
                embed.add_field(name="Note", value="Scam detection is heuristic and experimental.", inline=True)
            if decision.action_note:
                embed.add_field(name="Operational Note", value=decision.action_note, inline=False)
            ge.style_embed(embed, footer="Babblebox Shield | No message archive is stored")

        content = f"<@&{compiled.alert_role_id}>" if compiled.alert_role_id is not None and self._should_ping_alert_role(decision, top_reason) else None
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

        link_policy_mode = str(raw.get("link_policy_mode", DEFAULT_SHIELD_LINK_POLICY_MODE)).strip().lower()
        if link_policy_mode not in VALID_SHIELD_LINK_POLICY_MODES:
            link_policy_mode = DEFAULT_SHIELD_LINK_POLICY_MODE

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
            trusted_builtin_disabled_families=frozenset(_sorted_unique_text(raw.get("trusted_builtin_disabled_families", []))),
            trusted_builtin_disabled_domains=frozenset(_sorted_unique_text(raw.get("trusted_builtin_disabled_domains", []))),
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
            spam=PackSettings(
                enabled=bool(raw.get("spam_enabled")),
                low_action=str(raw.get("spam_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("spam_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("spam_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("spam_sensitivity", "normal")).strip().lower(),
            ),
            adult=PackSettings(
                enabled=bool(raw.get("adult_enabled")),
                low_action=str(raw.get("adult_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("adult_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("adult_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("adult_sensitivity", "normal")).strip().lower(),
            ),
            adult_solicitation_enabled=bool(raw.get("adult_solicitation_enabled")),
            adult_solicitation_excluded_channel_ids=frozenset(
                _sorted_unique_ints(raw.get("adult_solicitation_excluded_channel_ids", []))
            ),
            severe=PackSettings(
                enabled=bool(raw.get("severe_enabled")),
                low_action=str(raw.get("severe_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("severe_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("severe_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("severe_sensitivity", "normal")).strip().lower(),
            ),
            severe_enabled_categories=frozenset(
                category
                for category in _sorted_unique_text(raw.get("severe_enabled_categories", DEFAULT_SHIELD_SEVERE_CATEGORIES))
                if category in VALID_SHIELD_SEVERE_CATEGORIES
            ),
            severe_custom_terms=tuple(_sorted_unique_text(raw.get("severe_custom_terms", []))[:SHIELD_SEVERE_TERM_LIMIT]),
            severe_removed_terms=frozenset(_sorted_unique_text(raw.get("severe_removed_terms", []))[:SHIELD_SEVERE_TERM_LIMIT]),
            link_policy_mode=link_policy_mode,
            link_policy=PackSettings(
                enabled=link_policy_mode != DEFAULT_SHIELD_LINK_POLICY_MODE,
                low_action=str(raw.get("link_policy_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("link_policy_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("link_policy_high_action", "log")).strip().lower(),
                sensitivity="normal",
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
        else:
            cleaned = _normalize_link_host(cleaned) or ""
        if not cleaned:
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

    def _normalize_severe_term(self, raw_value: str) -> tuple[bool, str]:
        ok, cleaned_or_error = sanitize_short_plain_text(
            raw_value,
            field_name="Severe term",
            max_length=SEVERE_TERM_MAX_LEN,
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


class ShieldFeatureSafetyGateway:
    _make_pack_match = ShieldService._make_pack_match
    _dedupe_matches = ShieldService._dedupe_matches
    _detect_privacy = ShieldService._detect_privacy
    _detect_privacy_email = ShieldService._detect_privacy_email
    _detect_privacy_phone = ShieldService._detect_privacy_phone
    _detect_privacy_ip = ShieldService._detect_privacy_ip
    _detect_privacy_crypto = ShieldService._detect_privacy_crypto
    _detect_privacy_payment = ShieldService._detect_privacy_payment
    _detect_privacy_sensitive_ids = ShieldService._detect_privacy_sensitive_ids
    _detect_adult_solicitation = ShieldService._detect_adult_solicitation
    _active_severe_terms = ShieldService._active_severe_terms
    _detect_severe_sexual_exploitation = ShieldService._detect_severe_sexual_exploitation
    _detect_severe_self_harm = ShieldService._detect_severe_self_harm
    _detect_severe_eliminationist_hate = ShieldService._detect_severe_eliminationist_hate
    _detect_severe_slur_abuse = ShieldService._detect_severe_slur_abuse
    _detect_severe_harm = ShieldService._detect_severe_harm

    def __init__(self, *, detector: ShieldService | None = None, link_safety: ShieldLinkSafetyEngine | None = None):
        self._detector = detector
        self.link_safety = link_safety or getattr(detector, "link_safety", None) or ShieldLinkSafetyEngine()
        self._owns_link_safety = detector is None and link_safety is None

    async def close(self):
        if self._owns_link_safety:
            await self.link_safety.close()

    def evaluate(
        self,
        surface: str,
        text: str | None,
        *,
        attachments: Sequence[Any] | None = None,
        channel_id: int | None = None,
    ) -> ShieldFeatureDecision:
        policy = _FEATURE_SURFACE_POLICIES.get(surface)
        if policy is None:
            raise ValueError(f"Unknown Shield feature surface: {surface}")

        compiled: CompiledShieldConfig = policy["compiled"]
        snapshot = _build_snapshot(text, attachments, surface_labels=(surface,))
        matches: list[ShieldMatch] = []
        matches.extend(self._detect_privacy(compiled, snapshot))
        matches.extend(self._detect_adult_solicitation(compiled, snapshot, channel_id=channel_id))
        matches.extend(self._detect_severe_harm(compiled, snapshot))
        matches.sort(
            key=lambda item: (
                ACTION_STRENGTH.get(item.action, 0),
                CONFIDENCE_STRENGTH.get(item.confidence, 0),
                PACK_STRENGTH.get(item.pack, 0),
            ),
            reverse=True,
        )
        if not matches:
            return ShieldFeatureDecision(
                allowed=True,
                surface=surface,
                reason_code=None,
                user_message=None,
                matches=(),
                link_assessments=(),
            )

        top = matches[0]
        if top.pack == "privacy":
            user_message = policy["privacy_message"]
        elif top.pack == "adult":
            user_message = policy.get("adult_message")
        elif top.pack == "severe":
            user_message = policy.get("severe_message")
        else:
            user_message = None
        if not isinstance(user_message, str) or not user_message:
            user_message = "That text is not allowed in this Babblebox feature."
        return ShieldFeatureDecision(
            allowed=False,
            surface=surface,
            reason_code=top.match_class or top.pack,
            user_message=user_message,
            matches=tuple(matches[:3]),
            link_assessments=(),
        )

    def assess_links(
        self,
        surface: str,
        *,
        text: str,
        squashed: str | None = None,
        shared_link_url: str | None = None,
        allow_domain_set: Iterable[str] = (),
        block_domain_set: Iterable[str] = (),
        link_policy_mode: str = DEFAULT_SHIELD_LINK_POLICY_MODE,
        has_suspicious_attachment: bool = False,
    ) -> ShieldFeatureLinkScan:
        normalized_surface = normalize_plain_text(surface)
        if not normalized_surface:
            raise ValueError("Shield feature link assessment requires a stable surface label.")

        normalized_text = normalize_plain_text(text).casefold()
        squashed_text = normalize_plain_text(squashed).casefold() if squashed is not None else squash_for_evasion_checks(normalized_text)
        allow_domains = frozenset(_sorted_unique_text(allow_domain_set))
        block_domains = frozenset(_sorted_unique_text(block_domain_set))
        mode = normalize_plain_text(link_policy_mode).casefold() or DEFAULT_SHIELD_LINK_POLICY_MODE
        urls = list(_extract_urls(normalized_text))
        if shared_link_url:
            urls.append(shared_link_url)

        assessments: list[ShieldLinkAssessment] = []
        flags: list[str] = []
        now = time.monotonic()
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
            if _domain_in_set(domain, block_domains):
                assessments.append(
                    ShieldLinkAssessment(
                        normalized_domain=domain,
                        category=UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
                        matched_signals=("feature_block_domain",),
                        provider_lookup_warranted=False,
                        provider_status="Blocked by feature link policy.",
                        intel_version="local",
                    )
                )
                flags.append("link_unsafe")
                continue

            allowlisted = _domain_in_set(domain, allow_domains)
            if _domain_in_set(domain, SHORTENER_DOMAINS) or _domain_in_set(domain, LINK_IN_BIO_DOMAINS) or _domain_in_set(domain, STOREFRONT_DOMAINS):
                assessments.append(
                    ShieldLinkAssessment(
                        normalized_domain=domain,
                        category=UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
                        matched_signals=("feature_blocked_hub",),
                        provider_lookup_warranted=False,
                        provider_status="Blocked by feature link policy.",
                        intel_version="local",
                    )
                )
                flags.append("link_unsafe")
                continue

            assessment = self.link_safety.assess_domain(
                domain,
                path=parsed.path or "/",
                query=parsed.query or "",
                message_text=normalized_text,
                squashed_text=squashed_text,
                has_suspicious_attachment=has_suspicious_attachment,
                allowlisted=allowlisted,
                now=now,
            )
            assessments.append(assessment)
            if self._feature_link_allowed(
                domain=domain,
                assessment=assessment,
                allow_domains=allow_domains,
                block_domains=block_domains,
                link_policy_mode=mode,
            ):
                continue
            if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY}:
                flags.append("malicious_link")
            elif assessment.category == ADULT_LINK_CATEGORY:
                flags.append("adult_link")
            else:
                flags.append("link_unsafe")

        return ShieldFeatureLinkScan(
            surface=normalized_surface,
            has_links=bool(urls),
            flags=tuple(_sorted_unique_text(flags)),
            link_assessments=tuple(assessments),
        )

    def _feature_link_allowed(
        self,
        *,
        domain: str,
        assessment: ShieldLinkAssessment,
        allow_domains: frozenset[str],
        block_domains: frozenset[str],
        link_policy_mode: str,
    ) -> bool:
        if assessment.category in {
            MALICIOUS_LINK_CATEGORY,
            IMPERSONATION_LINK_CATEGORY,
            ADULT_LINK_CATEGORY,
            UNKNOWN_SUSPICIOUS_LINK_CATEGORY,
        }:
            return False
        if _domain_in_set(domain, block_domains):
            return False
        if _domain_in_set(domain, allow_domains):
            return True
        if link_policy_mode == "disabled":
            return False
        if link_policy_mode == "allow_all_safe":
            return assessment.category in {SAFE_LINK_CATEGORY, UNKNOWN_LINK_CATEGORY}
        return is_trusted_destination(domain, safe_family=assessment.safe_family)
