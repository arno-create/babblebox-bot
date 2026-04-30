from __future__ import annotations

import asyncio
import contextlib
import difflib
import hashlib
import ipaddress
import logging
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any, Iterable, Sequence
from urllib.parse import urlsplit

import discord
from discord.ext import commands

from babblebox import game_engine as ge
from babblebox.premium_limits import (
    CAPABILITY_SHIELD_AI_REVIEW,
    LIMIT_SHIELD_ALLOWLIST,
    LIMIT_SHIELD_CUSTOM_PATTERNS,
    LIMIT_SHIELD_FILTERS,
    LIMIT_SHIELD_PACK_EXEMPTIONS,
    LIMIT_SHIELD_SEVERE_TERMS,
    guild_capabilities,
    guild_limit as premium_guild_limit,
    storage_ceiling,
)
from babblebox.premium_models import PLAN_FREE, PLAN_GUILD_PRO, SYSTEM_PREMIUM_SUPPORT_GUILD_ID
from babblebox.official_links import SUPPORT_SERVER_URL
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
    PACK_TIMEOUT_PACKS,
    SHIELD_NUMERIC_CONFIG_SPECS,
    SHIELD_SEVERE_TERM_LIMIT,
    ShieldStateStore,
    ShieldStorageUnavailable,
    VALID_SPAM_MODERATOR_POLICIES,
    VALID_SHIELD_AI_ACCESS_MODES,
    VALID_SHIELD_LOG_PING_MODES,
    VALID_SHIELD_LOG_STYLES,
    VALID_SHIELD_SEVERE_CATEGORIES,
    VALID_SHIELD_LINK_POLICY_MODES,
    default_guild_shield_config,
    normalize_guild_shield_config,
    shield_numeric_config_default,
)
from babblebox.text_safety import (
    CARD_RE,
    EMAIL_RE,
    IPV4_RE,
    IPV6_RE,
    PHONE_RE,
    SSN_RE,
    URL_RE,
    contains_safety_term,
    find_safety_term_hits,
    fold_confusable_text,
    is_harmful_context_suppressed,
    is_severe_reference_context,
    normalize_plain_text,
    sanitize_short_plain_text,
    squash_for_evasion_checks,
)
from babblebox.utility_helpers import deserialize_datetime, make_attachment_labels, make_message_preview


LOGGER = logging.getLogger(__name__)


RULE_PACKS = ("privacy", "promo", "scam", "spam", "gif", "adult", "severe")
SHIELD_ACTIONS = {"disabled", "detect", "log", "delete_log", "delete_escalate", "timeout_log", "delete_timeout_log"}
SHIELD_SENSITIVITIES = {"low", "normal", "high"}
CUSTOM_PATTERN_MODES = {"contains", "word", "wildcard"}

FILTER_LIMIT = premium_guild_limit(PLAN_FREE, LIMIT_SHIELD_FILTERS)
ALLOWLIST_LIMIT = premium_guild_limit(PLAN_FREE, LIMIT_SHIELD_ALLOWLIST)
ALLOW_PHRASE_MAX_LEN = 60
CUSTOM_PATTERN_LIMIT = premium_guild_limit(PLAN_FREE, LIMIT_SHIELD_CUSTOM_PATTERNS)
CUSTOM_PATTERN_LABEL_MAX_LEN = 32
CUSTOM_PATTERN_MAX_LEN = 80
CUSTOM_PATTERN_WILDCARD_LIMIT = 4
SHIELD_SEVERE_COMPILED_LIMIT = storage_ceiling(LIMIT_SHIELD_SEVERE_TERMS, SHIELD_SEVERE_TERM_LIMIT)
MAX_MESSAGE_PREVIEW = 220
CHANNEL_ACTIVITY_LIMIT = 200
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
SHIELD_ALERT_ACTION_TTL_SECONDS = 24 * 60 * 60
SPAM_GIF_WINDOW_SECONDS = 45.0
SPAM_GIF_REPEAT_WINDOW_SECONDS = 30.0
HEALTHY_CHAT_WINDOW_SECONDS = 20.0
GIF_PRESSURE_WINDOW_MULTIPLIER = 4
GIF_PRESSURE_MIN_WINDOW_SECONDS = 60.0
GIF_PRESSURE_SLICE_LIMIT = 12
GIF_FILLER_RELIEF_WEIGHT = 0.25
MAX_GIF_PRESSURE_WINDOW_SECONDS = max(
    GIF_PRESSURE_MIN_WINDOW_SECONDS,
    float(SHIELD_NUMERIC_CONFIG_SPECS["gif_window_seconds"][1] * GIF_PRESSURE_WINDOW_MULTIPLIER),
)
CHANNEL_ACTIVITY_CONTEXT_WINDOW_SECONDS = max(HEALTHY_CHAT_WINDOW_SECONDS, SPAM_GIF_WINDOW_SECONDS)
CHANNEL_ACTIVITY_WINDOW_SECONDS = max(CHANNEL_ACTIVITY_CONTEXT_WINDOW_SECONDS, MAX_GIF_PRESSURE_WINDOW_SECONDS)
HEALTHY_CHAT_AUTHOR_THRESHOLD = 4
GIF_EMBED_DOMAINS = frozenset({"tenor.com", "media.tenor.com", "giphy.com", "media.giphy.com"})
GIF_STREAK_TRACK_LIMIT = 50
RECENT_ACCOUNT_WINDOW = timedelta(days=7)
EARLY_MEMBER_WINDOW = timedelta(days=1)
NEWCOMER_ACTIVITY_TTL_SECONDS = 6 * 3600.0
NEWCOMER_MESSAGE_WINDOW = 3
CAMPAIGN_SIGNATURE_LIMIT = 256
CAMPAIGN_USERS_PER_SIGNATURE_LIMIT = 12
NEWCOMER_STATE_LIMIT = 512
GIF_INCIDENT_WINDOW_SECONDS = 90.0
SPAM_INCIDENT_WINDOW_SECONDS = 90.0
INCIDENT_ALERT_EDIT_MIN_SECONDS = 8.0
SHIELD_BASELINE_VERSION = 4
TRUSTED_ONLY_BUILTIN_FAMILIES = frozenset(TRUSTED_LINK_SAFE_FAMILIES)
TRUSTED_ONLY_BUILTIN_DOMAINS = frozenset(TRUSTED_MAINSTREAM_DOMAINS)
AUTOMATED_AUTHOR_KINDS = frozenset({"bot", "webhook"})
PREVIEW_ONLY_LINK_SIGNAL = "preview_only"
SOURCE_SIGNAL_PREFIX = "source:"
PREVIEW_ONLY_LINK_SOURCES = frozenset({"embeds"})
SAFE_VISIBLE_PREVIEW_ANCHOR_DOMAINS = frozenset(
    set(TRUSTED_MAINSTREAM_DOMAINS)
    | {
        "cdn.discordapp.com",
        "discord.com",
        "discord.gg",
        "docs.github.com",
        "docs.google.com",
        "giphy.com",
        "i.imgur.com",
        "i.ytimg.com",
        "imgur.com",
        "media.discordapp.net",
        "media.giphy.com",
        "media.tenor.com",
        "tenor.com",
        "ytimg.com",
    }
)

PACK_LABELS = {
    "privacy": "Privacy Leak",
    "promo": "Promo / Invite",
    "scam": "Scam / Malicious Links",
    "spam": "Anti-Spam",
    "gif": "GIF Flood / Media Pressure",
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
    "scam_dm_lure": "No-link DM lure",
    "untrusted_external_link": "Untrusted external link",
    "untrusted_invite_link": "Untrusted invite link",
    "blocked_link_hub": "Blocked link hub or storefront",
    "spam_duplicate": "Repeated duplicate spam",
    "spam_near_duplicate": "Repeated near-duplicate spam",
    "spam_link_flood": "Repeated link flood",
    "spam_invite_flood": "Repeated invite flood",
    "spam_mention_flood": "Mention flood",
    "spam_emoji_flood": "Emote spam",
    "spam_caps_flood": "Excessive capitals",
    "spam_gif_flood": "GIF flood",
    "spam_group_gif_pressure": "Coordinated GIF pressure",
    "spam_message_rate": "Message-rate spam",
    "spam_burst": "Fast burst posting",
    "spam_low_value_noise": "Repeated low-value noise",
    "spam_padding_noise": "Character-padding spam",
    "link_policy_malicious": "Known malicious domain",
    "link_policy_impersonation": "Trusted-brand impersonation domain",
    "link_policy_adult": "Known adult domain",
    "link_policy_suspicious": "Suspicious external link",
}
ESCALATION_BLOCKED_MATCH_CLASSES = {"repetitive_link_noise", "spam_low_value_noise", "spam_burst", "spam_padding_noise"}
ESCALATION_ELIGIBLE_MATCH_CLASSES = frozenset(
    {
        "spam_duplicate",
        "spam_near_duplicate",
        "spam_link_flood",
        "spam_invite_flood",
        "spam_mention_flood",
        "spam_emoji_flood",
        "spam_gif_flood",
        "spam_group_gif_pressure",
        "scam_bait_link",
        "scam_bait_attachment",
        "scam_shortener",
        "scam_attachment",
        "scam_download",
        "scam_campaign_lure",
        "scam_risky_unknown_link",
        "scam_brand_impersonation",
        "scam_mint_wallet_lure",
        "scam_dm_lure",
    }
)
ACTION_LABELS = {
    "disabled": "Disabled",
    "detect": "Detect only",
    "log": "Log only",
    "delete_log": "Delete + log",
    "delete_escalate": "Delete + log + repeated-hit escalation",
    "timeout_log": "Timeout + log (keep message)",
    "delete_timeout_log": "Delete + Timeout + log",
}
CONFIDENCE_LABELS = {"low": "Low confidence", "medium": "Medium confidence", "high": "High confidence", "custom": "Custom"}
SPAM_MODERATOR_POLICY_LABELS = {
    "exempt": "Exempt moderators",
    "delete_only": "Delete only",
    "full": "Full anti-spam policy",
}
SENSITIVITY_LABELS = {"low": "Low", "normal": "Normal", "high": "High"}
ACTION_STRENGTH = {"disabled": -1, "detect": 0, "log": 1, "delete_log": 2, "timeout_log": 3, "delete_timeout_log": 4, "delete_escalate": 5}
CONFIDENCE_STRENGTH = {"low": 1, "medium": 2, "high": 3, "custom": 4}
PACK_STRENGTH = {"privacy": 1, "spam": 1, "gif": 1, "promo": 2, "link_policy": 2, "scam": 3, "adult": 3, "severe": 4, "advanced": 5}
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
SEVERE_SELF_HARM_NORMAL_TERMS = frozenset(
    {
        "drink bleach",
        "go die",
        "go jump off a bridge",
        "go jump off a cliff",
        "hang yourself",
        "slit your wrists",
        "you should die",
    }
)
SEVERE_SELF_HARM_HIGH_TERMS = frozenset({"nobody would miss you if you died", "the world would be better if you were dead"})
SEVERE_SELF_HARM_NEGATION_RE = re.compile(
    r"(?i)\b(?:do not|don't|dont|never|please don't|please do not)\b.{0,10}\b(?:kill yourself|kys|off yourself|end your life|go die|you should die)\b"
)
SEVERE_SUPPORT_CONTEXT_RE = re.compile(r"(?i)\b(?:call 988|crisis line|hotline|please stay alive|suicide prevention|support)\b")
SEVERE_PROTECTED_GROUP_RE = re.compile(
    r"(?i)\b(?:asians?|black people|blacks|disabled people|gays?|immigrants?|jews?|latinos?|lesbians?|mexicans?|muslims?|queers?|trans(?: people)?|transgender(?: people)?)\b"
)
SEVERE_ELIMINATION_RE = re.compile(
    r"(?i)\b(?:eradicate|eradicated|exterminate|exterminated|gas|kill all|lynch|purge|should die|should not exist|wipe out|wipe them out|wiped out|wiping out|wiping them out|should be wiped out|should be exterminated)\b"
)
SEVERE_DEHUMANIZING_RE = re.compile(r"(?i)\b(?:animals|cockroaches|filth|parasites|subhuman|vermin)\b")
SEVERE_GROUP_DEHUMANIZING_RE = re.compile(
    r"(?i)\b(?:are|r|be|being)\s+(?:just\s+|literal(?:ly)?\s+|filthy\s+)?(?:animals|cockroaches|filth|parasites|subhuman|vermin)\b"
)
SEVERE_TARGETING_RE = re.compile(
    r"(?i)\b(?:you|your|you['’]re|you are|u|ur|he|she|they|them|that person|this person|mods?|admins?|member|user)\b"
)
SEVERE_SLUR_LOW_TERMS = frozenset(
    {
        "beaner",
        "chink",
        "faggot",
        "gook",
        "kike",
        "n1gg3r",
        "n1gg@",
        "n1gga",
        "n1gger",
        "nigga",
        "nigger",
        "raghead",
        "spic",
        "towelhead",
        "wetback",
    }
)
SEVERE_SLUR_NORMAL_TERMS = frozenset({"coon", "cripple", "dyke", "mongoloid", "paki", "retard", "tranny"})
SEVERE_DEGRADING_TERMS = frozenset({"human garbage", "subhuman", "vermin", "worthless trash"})
SEVERE_TARGETED_DEGRADING_RE = re.compile(
    r"(?i)\b(?:you|you['ƒ?T]re|you are|u|ur|he|she|they|that person|this person)\b.{0,20}\b(?:human garbage|subhuman|vermin|worthless trash)\b"
)
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
SCAM_DM_ROUTE_RE = re.compile(
    r"(?i)\b(?:dm(?: me| us)?|message me|message us|msg me|msg us|pm me|pm us|inbox me|inbox us|my dms? are open|contact me privately|details? in dms?|more info in dms?|dm for (?:details?|info|more))\b"
)
SCAM_OFF_PLATFORM_ROUTE_RE = re.compile(
    r"(?i)\b(?:telegram|whatsapp|signal|instagram|snap(?:chat)?|twitter|x\.com|email me|contact me on)\b"
)
SCAM_PRIZE_BAIT_RE = re.compile(
    r"(?i)\b(?:free nitro|nitro|crypto|btc|bitcoin|eth|ethereum|usdt|robux|reward|prize|gift(?:away)?|giveaway|cash|money|dollars?|bucks|steam gift|skin|account)\b"
)
SCAM_CLAIM_OR_DETAILS_RE = re.compile(r"(?i)\b(?:claim|redeem|collect|grab|get|receive|win|details?|info|more info|more details)\b")
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
SCAM_VALUABLE_ITEM_RE = re.compile(
    r"(?i)\b(?:iphone(?:\s*\d{1,2}(?:\s*(?:pro(?:\s*max)?|plus|ultra))?)?|ps5|playstation\s*5|xbox(?:\s*series\s*[xs])?|macbook|ipad|airpods?|gift cards?|steam account|discord account|premium account|rare skins?|gaming laptop|graphics card|gpu)\b"
)
SCAM_DEAL_TOO_GOOD_RE = re.compile(
    r"(?i)\b(?:very cheap|cheap price|too cheap|below market|half price|for free|free of charge|giving it away|giving away|almost free|just pay shipping|lowest price|quick sale)\b"
)
SCAM_DIRECT_OFFER_RE = re.compile(
    r"(?i)\b(?:i(?:'m| am)? selling|selling|give(?:ing)? away|offering|for sale|available now|message me to claim|dm(?: me)? to claim)\b"
)
SCAM_BENIGN_TRADE_DISCUSSION_RE = re.compile(
    r"(?i)\b(?:price check|market value|msrp|resale value|worth|looking for|wtb|wts|is anyone selling|anyone selling|where can i buy|saw someone selling|someone is selling|discussion|report|reported|warning|beware|scam alert|article|news)\b"
)
SCAM_NO_LINK_SOFT_ROUTE_TERMS = frozenset(
    {
        "tap in",
        "lock in with me",
    }
)
SCAM_NO_LINK_STRONG_PRIVATE_ROUTE_TERMS = frozenset(
    {
        "dm me",
        "dm us",
        "message me",
        "message us",
        "msg me",
        "msg us",
        "pm me",
        "pm us",
        "inbox me",
        "inbox us",
        "my dms are open",
        "my dm is open",
        "contact me privately",
        "details in dms",
        "more info in dms",
        "dm for details",
        "dm for info",
        "dm for more",
        "hit me up",
        "hmu",
    }
)
SCAM_NO_LINK_ACTIVITY_PROBE_TERMS = frozenset(
    {
        "who active",
        "who is active",
        "who's active",
        "who up",
        "who is up",
        "who's up",
        "who tryna eat",
        "who trying to eat",
        "who needs money",
        "who online",
        "who is online",
        "who's online",
    }
)
SCAM_NO_LINK_EARNINGS_BAIT_TERMS = frozenset(
    {
        "easy money",
        "cash tonight",
        "get paid",
        "make money",
        "i can make you money",
        "run it up",
        "running it up",
        "let's run it up",
        "lets run it up",
        "let's get it up to",
        "lets get it up to",
        "let's get it to",
        "lets get it to",
    }
)
SCAM_NO_LINK_GAMBLING_BAIT_TERMS = frozenset(
    {
        "wins",
        "get wins",
        "picks",
        "free picks",
        "premium picks",
        "vip picks",
        "vip plays",
        "locks",
        "slips",
        "parlay",
        "sportsbook method",
        "betting method",
    }
)
SCAM_NO_LINK_PRESSURE_TERMS = frozenset(
    {
        "tonight",
        "right now",
        "don't miss out",
        "dont miss out",
        "spots left",
        "last spots",
        "running it up",
    }
)
SCAM_NO_LINK_BENIGN_SPORTS_DISCUSSION_RE = re.compile(
    r"(?i)\b(?:tonight'?s game|game tonight|scrim|watch party|injury report|moneyline|spread|odds|analysis|recap|sportsbook article)\b"
)
SCAM_NO_LINK_BENIGN_DM_COORDINATION_RE = re.compile(
    r"(?i)\b(?:dm|message|msg|pm|send)\s+me\b.{0,40}\b(?:later|the notes?|the article|the recap|the build|the link|the doc|the docs|the screenshot|the clip|the replay|the vod)\b"
)
SCAM_CASH_AMOUNT_RE = re.compile(
    r"(?ix)(?:"
    r"(?:[$]\s?(?:\d{1,3}(?:,\d{3})+|\d{2,6}|\d+(?:\.\d+)?k))"
    r"|(?:\b\d+(?:\.\d+)?k\b(?:\s*(?:usd|dollars?|bucks))?)"
    r"|(?:\b(?:\d{1,3}(?:,\d{3})+|\d{2,6})\b\s*(?:usd|dollars?|bucks)\b)"
    r")"
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
GIF_FILLER_TEXT_PHRASES = frozenset(
    {
        "agreed",
        "aha",
        "cool",
        "haha",
        "hello",
        "hello all",
        "hello everyone",
        "hello there",
        "hey",
        "hey all",
        "hey everyone",
        "hi",
        "hi all",
        "hi everyone",
        "hi guys",
        "hi jim",
        "hi team",
        "hi there",
        "lol",
        "lmao",
        "nice",
        "ok",
        "okay",
        "same",
        "sounds good",
        "sup",
        "thanks",
        "thank you",
        "what's up",
        "what's up guys",
        "yep",
        "yup",
    }
)
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
class PackExemptionScope:
    channel_ids: frozenset[int]
    role_ids: frozenset[int]
    user_ids: frozenset[int]

    def matches(self, *, channel_id: int | None, user_id: int, role_ids: frozenset[int]) -> bool:
        return (
            (channel_id is not None and channel_id in self.channel_ids)
            or user_id in self.user_ids
            or bool(self.role_ids.intersection(role_ids))
        )


@dataclass(frozen=True)
class SpamRuleSettings:
    message_enabled: bool
    message_threshold: int
    message_window_seconds: int
    burst_enabled: bool
    burst_threshold: int
    burst_window_seconds: int
    near_duplicate_enabled: bool
    near_duplicate_threshold: int
    near_duplicate_window_seconds: int
    emote_enabled: bool
    emote_threshold: int
    caps_enabled: bool
    caps_threshold: int
    low_value_enabled: bool
    low_value_threshold: int
    low_value_window_seconds: int
    moderator_policy: str


@dataclass(frozen=True)
class GifRuleSettings:
    message_enabled: bool
    message_threshold: int
    window_seconds: int
    consecutive_enabled: bool
    consecutive_threshold: int
    repeat_enabled: bool
    repeat_threshold: int
    same_asset_enabled: bool
    same_asset_threshold: int
    min_ratio_percent: int


@dataclass(frozen=True)
class ShieldPackLogOverride:
    style: str
    ping_mode: str


@dataclass(frozen=True)
class ShieldResolvedLogDelivery:
    style: str
    ping_mode: str


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
    log_style: str
    log_ping_mode: str
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
    pack_exemptions: dict[str, PackExemptionScope]
    pack_log_overrides: dict[str, ShieldPackLogOverride]
    pack_timeout_minutes: dict[str, int | None]
    privacy: PackSettings
    promo: PackSettings
    scam: PackSettings
    spam: PackSettings
    spam_rules: SpamRuleSettings
    gif: PackSettings
    gif_rules: GifRuleSettings
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
        if pack == "gif":
            return self.gif
        if pack == "adult":
            return self.adult
        if pack == "severe":
            return self.severe
        if pack == "link_policy":
            return self.link_policy
        return PackSettings(enabled=True, low_action="log", medium_action="log", high_action="log", sensitivity="normal")

    def timeout_minutes_for_pack(self, pack: str | None) -> int:
        if pack:
            override = self.pack_timeout_minutes.get(pack)
            if isinstance(override, int) and override >= 1:
                return override
        return self.timeout_minutes

    def resolved_log_delivery(self, pack: str | None) -> ShieldResolvedLogDelivery:
        style = self.log_style if self.log_style in VALID_SHIELD_LOG_STYLES else "adaptive"
        ping_mode = self.log_ping_mode if self.log_ping_mode in VALID_SHIELD_LOG_PING_MODES else "smart"
        if pack:
            override = self.pack_log_overrides.get(pack)
            if override is not None:
                if override.style in VALID_SHIELD_LOG_STYLES:
                    style = override.style
                if override.ping_mode in VALID_SHIELD_LOG_PING_MODES:
                    ping_mode = override.ping_mode
        return ShieldResolvedLogDelivery(style=style, ping_mode=ping_mode)


@dataclass(frozen=True)
class ShieldAIAccessPolicy:
    guild_id: int
    enabled: bool
    policy_enabled: bool
    source: str
    premium_unlocked: bool
    configured_allowed_models: tuple[str, ...]
    allowed_models: tuple[str, ...]
    plan_allowed_models: tuple[str, ...]
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
    source: str = "message"
    preview_only: bool = False


@dataclass(frozen=True)
class ShieldSnapshot:
    scan_text: str
    text: str
    squashed: str
    context_text: str
    context_squashed: str
    urls: tuple[str, ...]
    links: tuple[ShieldLink, ...]
    ignored_link_candidates: tuple[str, ...]
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
    uppercase_count: int = 0
    alpha_count: int = 0
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


@dataclass(frozen=True)
class ShieldLinkDecisionExplanation:
    domain: str
    disposition: str
    reason: str


@dataclass
class ShieldDecision:
    matched: bool
    action: str
    pack: str | None
    reasons: tuple[ShieldMatch, ...]
    deleted: bool = False
    deleted_count: int = 0
    delete_attempt_count: int = 0
    logged: bool = False
    timed_out: bool = False
    escalated: bool = False
    action_note: str | None = None
    ai_review: ShieldAIReviewResult | None = None
    link_assessments: tuple[ShieldLinkAssessment, ...] = ()
    link_explanations: tuple[ShieldLinkDecisionExplanation, ...] = ()
    scan_source: str = "new_message"
    scan_surface_labels: tuple[str, ...] = ()
    alert_evidence_signature: str | None = None
    alert_evidence_summary: str | None = None


class ShieldAlertActionView(discord.ui.View):
    def __init__(self, service: "ShieldService", record: dict[str, Any]):
        super().__init__(timeout=None)
        self.service = service
        self.token = str(record.get("token") or "")
        jump_url = str(record.get("jump_url") or "")
        target_message_id = record.get("target_message_id")
        target_channel_id = record.get("target_channel_id")
        if bool(record.get("used")):
            self.add_item(discord.ui.Button(label="Support Server", style=discord.ButtonStyle.link, url=SUPPORT_SERVER_URL, row=0))
            return
        deleted_by_shield = bool(record.get("deleted_by_shield") or record.get("deleted_by_moderator"))
        timed_out_by_shield = bool(record.get("timed_out_by_shield"))
        if jump_url and not deleted_by_shield:
            self.add_item(discord.ui.Button(label="Open Message", style=discord.ButtonStyle.link, url=jump_url, row=0))
        if target_message_id and target_channel_id and not deleted_by_shield:
            delete_button = discord.ui.Button(
                label="Delete Message",
                style=discord.ButtonStyle.danger,
                custom_id=f"bb:shield_alert:delete:{self.token}",
                row=0,
            )

            async def delete_callback(interaction: discord.Interaction):
                await self.service.handle_alert_delete_interaction(interaction, self.token)

            delete_button.callback = delete_callback
            self.add_item(delete_button)
        if timed_out_by_shield:
            untimeout_button = discord.ui.Button(
                label="Remove Timeout",
                style=discord.ButtonStyle.secondary,
                custom_id=f"bb:shield_alert:untimeout:{self.token}",
                row=0,
            )

            async def untimeout_callback(interaction: discord.Interaction):
                await self.service.handle_alert_remove_timeout_interaction(interaction, self.token)

            untimeout_button.callback = untimeout_callback
            self.add_item(untimeout_button)
        false_positive_button = discord.ui.Button(
            label="Mark False Positive",
            style=discord.ButtonStyle.success,
            custom_id=f"bb:shield_alert:false_positive:{self.token}",
            row=1,
        )

        async def false_positive_callback(interaction: discord.Interaction):
            await self.service.handle_alert_false_positive_interaction(interaction, self.token)

        false_positive_button.callback = false_positive_callback
        self.add_item(false_positive_button)
        self.add_item(discord.ui.Button(label="Support Server", style=discord.ButtonStyle.link, url=SUPPORT_SERVER_URL, row=1))


class ShieldAlertDeleteConfirmView(discord.ui.View):
    def __init__(self, service: "ShieldService", token: str):
        super().__init__(timeout=60)
        self.service = service
        self.token = token
        confirm = discord.ui.Button(label="Confirm Delete", style=discord.ButtonStyle.danger)

        async def confirm_callback(interaction: discord.Interaction):
            result = await self.service.handle_alert_delete_message(self.token, moderator=interaction.user, guild=interaction.guild)
            await self.service._send_alert_action_interaction_result(interaction, result, title="Shield Alert Action")

        confirm.callback = confirm_callback
        self.add_item(confirm)
        self.add_item(discord.ui.Button(label="Support Server", style=discord.ButtonStyle.link, url=SUPPORT_SERVER_URL))


class _StaticShieldAlertActionView:
    def __init__(self, record: dict[str, Any]):
        labels = []
        if bool(record.get("used")):
            self.children = [type("StaticShieldButton", (), {"label": "Support Server"})()]
            return
        deleted = bool(record.get("deleted_by_shield") or record.get("deleted_by_moderator"))
        if record.get("jump_url") and not deleted:
            labels.append("Open Message")
        if record.get("target_message_id") and record.get("target_channel_id") and not deleted:
            labels.append("Delete Message")
        if bool(record.get("timed_out_by_shield")):
            labels.append("Remove Timeout")
        labels.extend(("Mark False Positive", "Support Server"))
        self.children = [type("StaticShieldButton", (), {"label": label})() for label in labels]


@dataclass(frozen=True)
class ShieldGifIncidentPlan:
    primary_match: ShieldMatch
    personal_match: ShieldMatch | None = None
    group_match: ShieldMatch | None = None
    delete_targets: tuple[discord.Message, ...] = ()
    alert_evidence_signature: str | None = None
    alert_evidence_summary: str | None = None
    action_note: str | None = None


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
    channel_id: int | None
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
    uppercase_count: int
    alpha_count: int
    low_value_text: bool
    repeated_char_run: int
    text_substance: str = "ignored"
    text_relief_weight: float = 0.0
    is_gif_message: bool = False
    gif_signature: str | None = None
    gif_only: bool = False
    gif_low_text: bool = False
    gif_pack_exempt: bool = False
    message: Any | None = None


@dataclass(frozen=True)
class ShieldChannelActivityEvent:
    timestamp: float
    user_id: int
    author_kind: str
    plain_word_count: int
    low_value_text: bool
    quality_message: bool
    text_substance: str
    text_relief_weight: float
    risky: bool
    token_signature: str
    exact_fingerprint: str | None = None
    near_fingerprint: str | None = None
    is_gif_message: bool = False
    gif_signature: str | None = None
    gif_only: bool = False
    gif_low_text: bool = False
    gif_pack_exempt: bool = False
    message: Any | None = None


@dataclass(frozen=True)
class ShieldChannelGifStreakState:
    rows: tuple[ShieldChannelActivityEvent, ...] = ()
    capped: bool = False


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
    dm_route: bool
    off_platform_route: bool
    soft_private_route: bool
    activity_probe: bool
    earnings_bait: bool
    gambling_bait: bool
    no_link_pressure: bool
    emoji_hype: bool
    prize_or_money_bait: bool
    commodity_bait: bool
    too_good_offer: bool
    direct_offer: bool
    cash_amount_bait: bool
    claim_or_details_language: bool
    nitro_or_crypto_bait: bool
    benign_trade_context: bool
    benign_sports_context: bool
    benign_dm_coordination: bool
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
    link_explanations: tuple[ShieldLinkDecisionExplanation, ...] = ()
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
    if any(ord(char) > 127 for char in candidate):
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
    if not has_pathish_suffix and any(label.startswith("xn--") for label in labels):
        return None
    if not has_pathish_suffix and tld in BARE_URL_AMBIGUOUS_FILE_TLDS:
        return None
    if not has_pathish_suffix and (tld in BARE_URL_FILENAME_TLDS or BARE_URL_TLD_RE.fullmatch(tld) is None):
        return None
    return candidate


def _looks_like_ignored_bare_idna_candidate(raw_token: str) -> str | None:
    candidate = (raw_token or "").strip().strip("()[]{}<>,.!?\"'")
    if not candidate or len(candidate) > BARE_URL_MAX_LENGTH:
        return None
    lowered = candidate.casefold()
    if "://" in lowered or lowered.startswith("www.") or "@" in lowered or lowered.count(".") < 1:
        return None
    if any(char in candidate for char in "/?#"):
        return None
    if BARE_URL_NUMERIC_RE.fullmatch(lowered):
        return None
    domain = _extract_domain(candidate)
    if domain is None:
        return None
    labels = [label for label in domain.split(".") if label]
    if len(labels) < 2:
        return None
    if any(label.startswith("xn--") for label in labels):
        return candidate
    return None


def _extract_urls(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    extracted: list[str] = []
    seen: set[str] = set()

    def add_candidate(raw_value: str | None):
        if raw_value and _looks_like_ignored_bare_idna_candidate(raw_value):
            return
        if raw_value and raw_value not in seen:
            seen.add(raw_value)
            extracted.append(raw_value)

    for match in URL_RE.finditer(text):
        add_candidate(match.group(0))
    for token in text.split()[:BARE_URL_MAX_TOKENS]:
        add_candidate(_looks_like_bare_url_candidate(token))
    return tuple(extracted)


def _extract_ignored_link_candidates(text: str) -> tuple[str, ...]:
    if not text:
        return ()
    ignored: list[str] = []
    seen: set[str] = set()
    for token in text.split()[:BARE_URL_MAX_TOKENS]:
        candidate = _looks_like_ignored_bare_idna_candidate(token)
        if candidate and candidate not in seen:
            seen.add(candidate)
            ignored.append(candidate)
    return tuple(ignored)


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


def _classify_gif_text_substance(
    text: str,
    *,
    plain_word_count: int,
    low_value_text: bool,
    risky: bool,
) -> tuple[str, float]:
    cleaned = normalize_plain_text(text).casefold()
    if not cleaned or risky or plain_word_count <= 0:
        return ("ignored", 0.0)
    if low_value_text:
        return ("filler", GIF_FILLER_RELIEF_WEIGHT)
    if plain_word_count <= 5 and cleaned in GIF_FILLER_TEXT_PHRASES:
        return ("filler", GIF_FILLER_RELIEF_WEIGHT)
    if plain_word_count >= 3:
        return ("substantive", 1.0)
    return ("ignored", 0.0)


def _short_token_signature(text: str, *, limit: int = 3) -> str:
    tokens = _plain_word_tokens(text)
    if not tokens:
        return ""
    return " ".join(tokens[:limit])


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


def _build_link(raw_url: str, *, source: str = "message", preview_only: bool = False) -> ShieldLink | None:
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
        source=source,
        preview_only=bool(preview_only),
    )


def _iter_link_segments(
    text: str | None,
    *,
    extra_texts: Sequence[str] | None,
    link_texts: Sequence[str] | None,
    link_segments: Sequence[tuple[str, str, bool]] | None,
) -> tuple[tuple[str, str, bool], ...]:
    if link_segments is not None:
        return tuple(
            (normalize_plain_text(label).casefold() or "extra", segment_text, bool(preview_only))
            for label, segment_text, preview_only in link_segments
            if normalize_plain_text(segment_text)
        )
    segments: list[tuple[str, str, bool]] = []
    if normalize_plain_text(text or ""):
        segments.append(("message", text or "", False))
    for part in (link_texts if link_texts is not None else extra_texts) or ():
        if normalize_plain_text(part):
            segments.append(("extra", part, False))
    return tuple(segments)


def _extract_links_from_segments(segments: Sequence[tuple[str, str, bool]]) -> tuple[tuple[str, str, bool], ...]:
    links: list[tuple[str, str, bool]] = []
    for source, segment_text, preview_only in segments:
        for url in _extract_urls(normalize_plain_text(segment_text).casefold()):
            links.append((url, source, bool(preview_only)))
    return tuple(links)


def _link_is_safe_visible_preview_anchor(link: ShieldLink) -> bool:
    return link.invite_code is not None or _domain_in_set(link.domain, SAFE_VISIBLE_PREVIEW_ANCHOR_DOMAINS)


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


def _link_assessment_basis(assessment: ShieldLinkAssessment) -> str:
    allowlist_note = " Admin allowlists do not override risky-link intel." if "guild_allow_domain" in assessment.matched_signals else ""
    if _assessment_is_preview_only_advisory(assessment):
        return "Preview-only metadata behind a trusted visible link; treated as advisory instead of enforcement-grade."
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


def _format_link_decision_lines(
    explanations: Sequence[ShieldLinkDecisionExplanation],
    *,
    limit: int = 5,
) -> str:
    if not explanations:
        return "No link decisions."
    lines = [
        f"`{item.domain}` | {item.disposition}: {item.reason}"
        for item in explanations[: max(1, limit)]
    ]
    if len(explanations) > limit:
        lines.append(f"+{len(explanations) - limit} more link decisions")
    return "\n".join(lines)


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


def _assessment_is_preview_only_advisory(assessment: ShieldLinkAssessment | None) -> bool:
    if assessment is None:
        return False
    return (
        PREVIEW_ONLY_LINK_SIGNAL in assessment.matched_signals
        and assessment.category in {UNKNOWN_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
    )


def _assessment_with_source_signals(assessment: ShieldLinkAssessment, *, source: str, preview_only: bool) -> ShieldLinkAssessment:
    signals = list(assessment.matched_signals)
    source_signal = f"{SOURCE_SIGNAL_PREFIX}{source}"
    if preview_only and source_signal not in signals:
        signals.append(source_signal)
    if preview_only and PREVIEW_ONLY_LINK_SIGNAL not in signals:
        signals.append(PREVIEW_ONLY_LINK_SIGNAL)
    if tuple(signals) == assessment.matched_signals:
        return assessment
    return replace(assessment, matched_signals=tuple(signals))


def _assessment_is_weak_shortener_context(assessment: ShieldLinkAssessment) -> bool:
    signals = {
        signal
        for signal in assessment.matched_signals
        if not signal.startswith(SOURCE_SIGNAL_PREFIX) and signal not in {PREVIEW_ONLY_LINK_SIGNAL, "guild_allow_domain"}
    }
    return bool(signals) and signals.issubset({"shortener_domain", "message_social_engineering"})


def _score_link_assessment_for_scam(assessment: ShieldLinkAssessment) -> int:
    if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY}:
        return 5
    if _assessment_is_preview_only_advisory(assessment):
        return 0
    if assessment.category != UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
        return 0
    if _assessment_is_weak_shortener_context(assessment):
        return 2
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
            if signal.startswith("safe_family:") or signal.startswith(SOURCE_SIGNAL_PREFIX) or signal in {"guild_allow_domain", PREVIEW_ONLY_LINK_SIGNAL}:
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


def _contains_scam_phrase_signal(
    term: str,
    *,
    text: str,
    squashed: str,
    folded_text: str,
    folded_squashed: str,
) -> bool:
    normalized_term = normalize_plain_text(term).casefold()
    if not normalized_term:
        return False
    folded_term = fold_confusable_text(normalized_term)
    if contains_safety_term(normalized_term, text, squashed) or contains_safety_term(folded_term, folded_text, folded_squashed):
        return True
    if " " not in normalized_term and "'" not in normalized_term:
        return False
    squashed_term = squash_for_evasion_checks(normalized_term)
    folded_squashed_term = squash_for_evasion_checks(folded_term)
    return bool(
        (squashed_term and squashed_term in squashed)
        or (squashed_term and squashed_term in folded_squashed)
        or (folded_squashed_term and folded_squashed_term in squashed)
        or (folded_squashed_term and folded_squashed_term in folded_squashed)
    )


def _scam_phrase_signal_hits(
    terms: frozenset[str],
    *,
    text: str,
    squashed: str,
    folded_text: str,
    folded_squashed: str,
) -> tuple[str, ...]:
    return tuple(
        sorted(
            term
            for term in terms
            if _contains_scam_phrase_signal(
                term,
                text=text,
                squashed=squashed,
                folded_text=folded_text,
                folded_squashed=folded_squashed,
            )
        )
    )


def _replace_scam_phrase_terms(text: str, terms: Sequence[str], replacement: str) -> str:
    normalized = text
    for term in terms:
        folded_term = fold_confusable_text(term)
        if not folded_term:
            continue
        if " " in folded_term or "'" in folded_term:
            normalized = normalized.replace(folded_term, replacement)
        else:
            normalized = re.sub(rf"\b{re.escape(folded_term)}\b", replacement, normalized)
    return normalized


def _regex_signal(pattern: re.Pattern[str], *texts: str) -> bool:
    return any(pattern.search(text) for text in texts if text)


def _scam_lure_fingerprint(text: str) -> str | None:
    cleaned = normalize_plain_text(text).casefold()
    if len(cleaned) < 16:
        return None
    squashed = squash_for_evasion_checks(cleaned)
    normalized = fold_confusable_text(SCAM_CASH_AMOUNT_RE.sub("[money_amt]", cleaned))
    folded_squashed = squash_for_evasion_checks(normalized)
    replacements = (
        (BRAND_BAIT_RE, "[brand]"),
        (SCAM_PRIZE_BAIT_RE, "[prize]"),
        (SCAM_CASH_AMOUNT_RE, "[money_amt]"),
        (SCAM_CTA_RE, "[cta]"),
        (SCAM_LOGIN_FLOW_RE, "[auth]"),
        (SCAM_DM_ROUTE_RE, "[dm]"),
        (SCAM_OFF_PLATFORM_ROUTE_RE, "[offplatform]"),
        (SCAM_CLAIM_OR_DETAILS_RE, "[claim]"),
        (SCAM_LEGITIMACY_RE, "[official]"),
        (SCAM_URGENCY_RE, "[urgency]"),
        (SCAM_CRYPTO_MINT_RE, "[wallet]"),
        (re.compile(r"\b\d+\b"), "[n]"),
    )
    for pattern, replacement in replacements:
        normalized = pattern.sub(replacement, normalized)
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_STRONG_PRIVATE_ROUTE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[dm]",
    )
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_SOFT_ROUTE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[dm]",
    )
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_ACTIVITY_PROBE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[activity]",
    )
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_EARNINGS_BAIT_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[money]",
    )
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_GAMBLING_BAIT_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[wins]",
    )
    normalized = _replace_scam_phrase_terms(
        normalized,
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_PRESSURE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=normalized,
            folded_squashed=folded_squashed,
        ),
        "[urgency]",
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if len(normalized) < 16:
        return None
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def _looks_like_no_link_dm_lure(text: str) -> bool:
    cleaned = normalize_plain_text(text).casefold()
    if not cleaned or looks_like_warning_discussion(cleaned):
        return False
    if SCAM_BENIGN_TRADE_DISCUSSION_RE.search(cleaned):
        return False
    squashed = squash_for_evasion_checks(cleaned)
    folded = fold_confusable_text(cleaned)
    folded_squashed = squash_for_evasion_checks(folded)
    if SCAM_NO_LINK_BENIGN_SPORTS_DISCUSSION_RE.search(cleaned) or SCAM_NO_LINK_BENIGN_DM_COORDINATION_RE.search(cleaned):
        return False
    strong_private_route = bool(
        SCAM_DM_ROUTE_RE.search(cleaned)
        or SCAM_OFF_PLATFORM_ROUTE_RE.search(cleaned)
        or _scam_phrase_signal_hits(
            SCAM_NO_LINK_STRONG_PRIVATE_ROUTE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    soft_private_route = bool(
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_SOFT_ROUTE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    activity_probe = bool(
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_ACTIVITY_PROBE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    earnings_bait = bool(
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_EARNINGS_BAIT_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    gambling_bait = bool(
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_GAMBLING_BAIT_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    no_link_pressure = bool(
        _scam_phrase_signal_hits(
            SCAM_NO_LINK_PRESSURE_TERMS,
            text=cleaned,
            squashed=squashed,
            folded_text=folded,
            folded_squashed=folded_squashed,
        )
    )
    route = strong_private_route or (soft_private_route and (activity_probe or no_link_pressure or bool(SCAM_CASH_AMOUNT_RE.search(cleaned))))
    bait = bool(
        SCAM_PRIZE_BAIT_RE.search(cleaned)
        or SCAM_CASH_AMOUNT_RE.search(cleaned)
        or SCAM_VALUABLE_ITEM_RE.search(cleaned)
        or SCAM_DEAL_TOO_GOOD_RE.search(cleaned)
        or earnings_bait
        or gambling_bait
    )
    corroboration = bool(
        activity_probe
        or no_link_pressure
        or SCAM_CASH_AMOUNT_RE.search(cleaned)
        or SCAM_FAKE_AUTHORITY_RE.search(cleaned)
        or SCAM_OFFICIAL_FRAMING_RE.search(cleaned)
        or SCAM_DIRECT_OFFER_RE.search(cleaned)
        or (soft_private_route and no_link_pressure)
    )
    return route and bait and corroboration


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
    if cleaned == "delete_timeout_log":
        return ("log", "delete_log", "delete_timeout_log")
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


def _collect_message_surface_texts(
    message: Any,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[tuple[str, str, bool], ...]]:
    extra_texts: list[str] = []
    link_texts: list[str] = []
    link_segments: list[tuple[str, str, bool]] = []
    surface_labels: list[str] = []
    normalized_content = normalize_plain_text(getattr(message, "content", ""))
    if normalized_content:
        link_segments.append(("message", getattr(message, "content", ""), False))

    system_content = _surface_text_or_none(getattr(message, "system_content", ""))
    if system_content is not None and system_content.casefold() != normalized_content.casefold():
        extra_texts.append(system_content)
        link_texts.append(system_content)
        link_segments.append(("system", system_content, False))
        surface_labels.append("system")

    embed_texts = _collect_embed_surface_texts(getattr(message, "embeds", ()))
    if embed_texts:
        extra_texts.extend(embed_texts)
        link_texts.extend(embed_texts)
        link_segments.extend(("embeds", text, False) for text in embed_texts)
        surface_labels.append("embeds")

    attachment_texts, attachment_link_texts = _collect_attachment_surface_texts(getattr(message, "attachments", ()))
    if attachment_texts:
        extra_texts.extend(attachment_texts)
        surface_labels.append("attachment_meta")
    if attachment_link_texts:
        link_texts.extend(attachment_link_texts)
        link_segments.extend(("attachment_meta", text, False) for text in attachment_link_texts)

    snapshot_texts: list[str] = []
    snapshot_link_texts: list[str] = []
    for forwarded in getattr(message, "message_snapshots", ()) or ():
        forwarded_content = _surface_text_or_none(getattr(forwarded, "content", ""))
        if forwarded_content is not None:
            snapshot_texts.append(forwarded_content)
            snapshot_link_texts.append(forwarded_content)
            link_segments.append(("forwarded_snapshot", forwarded_content, False))
        forwarded_embed_texts = _collect_embed_surface_texts(getattr(forwarded, "embeds", ()))
        snapshot_texts.extend(forwarded_embed_texts)
        snapshot_link_texts.extend(forwarded_embed_texts)
        link_segments.extend(("forwarded_snapshot", text, False) for text in forwarded_embed_texts)
        forwarded_attachment_texts, forwarded_attachment_link_texts = _collect_attachment_surface_texts(getattr(forwarded, "attachments", ()))
        snapshot_texts.extend(forwarded_attachment_texts)
        snapshot_link_texts.extend(forwarded_attachment_link_texts)
        link_segments.extend(("forwarded_snapshot", text, False) for text in forwarded_attachment_link_texts)
    if snapshot_texts:
        extra_texts.extend(snapshot_texts)
        link_texts.extend(snapshot_link_texts)
        surface_labels.append("forwarded_snapshot")

    return tuple(extra_texts), tuple(link_texts), tuple(dict.fromkeys(surface_labels)), tuple(link_segments)


def _build_snapshot(
    text: str | None,
    attachments: Sequence[Any] | None = None,
    *,
    extra_texts: Sequence[str] | None = None,
    link_texts: Sequence[str] | None = None,
    link_segments: Sequence[tuple[str, str, bool]] | None = None,
    surface_labels: Sequence[str] | None = None,
) -> ShieldSnapshot:
    scan_text = normalize_plain_text(" ".join(part for part in (text or "", *(extra_texts or ())) if part))
    normalized = scan_text
    squashed = squash_for_evasion_checks(normalized.casefold())
    lowered = normalized.casefold()
    resolved_link_segments = _iter_link_segments(
        text,
        extra_texts=extra_texts,
        link_texts=link_texts,
        link_segments=link_segments,
    )
    extracted_link_items = _extract_links_from_segments(resolved_link_segments)
    urls = tuple(url for url, _source, _preview_only in extracted_link_items)
    ignored_link_candidates = _extract_ignored_link_candidates(normalized)
    context_source_text = _strip_urls_from_text(normalized, urls)
    context_text = _strip_urls_from_text(lowered, urls)
    context_squashed = squash_for_evasion_checks(context_text)
    mention_count, everyone_here_count = _count_mentions(normalized)
    emoji_count = _count_emojis(normalized)
    plain_word_count = len(_plain_word_tokens(context_text))
    alpha_count = sum(1 for char in context_source_text if char.isalpha())
    uppercase_count = sum(
        1
        for char in context_source_text
        if char.isalpha() and char == char.upper() and char != char.lower()
    )
    repeated_char_run = _max_repeated_char_run(context_source_text)
    low_value_text = _is_low_value_text(context_text, plain_word_count=plain_word_count)
    exact_fingerprint = hashlib.sha1(context_text.encode("utf-8")).hexdigest() if len(context_text) >= 3 else None
    near_duplicate_text = _normalize_near_duplicate_text(context_text, ())
    near_duplicate_fingerprint = (
        hashlib.sha1(near_duplicate_text.encode("utf-8")).hexdigest()
        if len(near_duplicate_text) >= 6
        else None
    )
    links = tuple(
        link
        for link in (
            _build_link(url, source=source, preview_only=preview_only)
            for url, source, preview_only in extracted_link_items
        )
        if link is not None
    )
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
        ignored_link_candidates=ignored_link_candidates,
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
        uppercase_count=uppercase_count,
        alpha_count=alpha_count,
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


def _build_message_snapshot(message: discord.Message, *, author_kind: str = "human") -> ShieldSnapshot:
    extra_texts, link_texts, surface_labels, link_segments = _collect_message_surface_texts(message)
    visible_segments = tuple(segment for segment in link_segments if segment[0] == "message")
    visible_links = tuple(
        link
        for link in (
            _build_link(url, source=source, preview_only=False)
            for url, source, _preview_only in _extract_links_from_segments(visible_segments)
        )
        if link is not None
    )
    has_safe_visible_anchor = author_kind == "human" and any(
        _link_is_safe_visible_preview_anchor(link) for link in visible_links
    )
    if has_safe_visible_anchor:
        link_segments = tuple(
            (source, segment_text, source in PREVIEW_ONLY_LINK_SOURCES)
            for source, segment_text, preview_only in link_segments
        )
    return _build_snapshot(
        getattr(message, "content", None),
        getattr(message, "attachments", None),
        extra_texts=extra_texts,
        link_texts=link_texts,
        link_segments=link_segments,
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
        log_style="adaptive",
        log_ping_mode="smart",
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
        pack_exemptions={pack: PackExemptionScope(channel_ids=frozenset(), role_ids=frozenset(), user_ids=frozenset()) for pack in RULE_PACKS},
        pack_log_overrides={},
        pack_timeout_minutes={},
        privacy=_feature_pack_settings(enabled=privacy_enabled),
        promo=disabled,
        scam=disabled,
        spam=disabled,
        spam_rules=SpamRuleSettings(
            message_enabled=True,
            message_threshold=7,
            message_window_seconds=5,
            burst_enabled=True,
            burst_threshold=5,
            burst_window_seconds=10,
            near_duplicate_enabled=True,
            near_duplicate_threshold=5,
            near_duplicate_window_seconds=10,
            emote_enabled=False,
            emote_threshold=18,
            caps_enabled=False,
            caps_threshold=28,
            low_value_enabled=False,
            low_value_threshold=5,
            low_value_window_seconds=60,
            moderator_policy="exempt",
        ),
        gif=disabled,
        gif_rules=GifRuleSettings(
            message_enabled=True,
            message_threshold=4,
            window_seconds=20,
            consecutive_enabled=True,
            consecutive_threshold=5,
            repeat_enabled=True,
            repeat_threshold=3,
            same_asset_enabled=True,
            same_asset_threshold=3,
            min_ratio_percent=70,
        ),
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
        self.storage_backend_preference = (
            getattr(store, "backend_preference", None)
            or (os.getenv("SHIELD_STORAGE_BACKEND", "").strip() or "postgres")
        ).strip().lower()
        if store is not None:
            self.store = store
        else:
            try:
                self.store = ShieldStateStore()
                self.storage_backend_preference = getattr(self.store, "backend_preference", self.storage_backend_preference)
            except ShieldStorageUnavailable as exc:
                LOGGER.warning("Shield storage constructor failed: %s", exc)
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
        self._deleted_message_ids: dict[int, float] = {}
        self._recent_promos: dict[tuple[int, int, str], list[float]] = {}
        self._recent_scam_campaigns: dict[tuple[int, str, str], list[tuple[float, int]]] = {}
        self._recent_spam_events: dict[tuple[int, int], list[ShieldSpamEvent]] = {}
        self._recent_channel_activity: dict[tuple[int, int], list[ShieldChannelActivityEvent]] = {}
        self._channel_gif_streaks: dict[tuple[int, int], ShieldChannelGifStreakState] = {}
        self._gif_incident_alerts: dict[tuple[int, int, int], dict[str, Any]] = {}
        self._spam_incident_alerts: dict[tuple[int, int, int, str], dict[str, Any]] = {}
        self._alert_action_message_refs: dict[str, discord.Message] = {}
        self._recent_newcomer_activity: dict[tuple[int, int], ShieldNewcomerActivityState] = {}
        self._last_runtime_prune = 0.0

    def format_link_decision_lines(
        self,
        explanations: Sequence[ShieldLinkDecisionExplanation],
        *,
        limit: int = 5,
    ) -> str:
        return _format_link_decision_lines(explanations, limit=limit)

    async def start(self) -> bool:
        if self._startup_storage_error is not None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            LOGGER.warning("Shield storage unavailable: %s", self._startup_storage_error)
            return False
        try:
            await self.store.load()
        except ShieldStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            LOGGER.warning("Shield storage unavailable: %s", exc)
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

    def _premium_service(self):
        premium_service = getattr(self.bot, "premium_service", None)
        if callable(getattr(premium_service, "resolve_guild_limit", None)):
            return premium_service
        return None

    def _resolve_guild_limit(self, guild_id: int, limit_key: str, fallback: int) -> int:
        premium_service = self._premium_service()
        if premium_service is None:
            if int(guild_id or 0) == SYSTEM_PREMIUM_SUPPORT_GUILD_ID:
                return premium_guild_limit(PLAN_GUILD_PRO, limit_key)
            return fallback
        return premium_service.resolve_guild_limit(guild_id, limit_key)

    def _guild_has_capability(self, guild_id: int, capability: str) -> bool:
        premium_service = self._premium_service()
        if premium_service is not None:
            return bool(premium_service.guild_has_capability(guild_id, capability))
        if int(guild_id or 0) == SYSTEM_PREMIUM_SUPPORT_GUILD_ID:
            return capability in guild_capabilities(PLAN_GUILD_PRO)
        return False

    def _premium_plan_title(self, plan_code: str) -> str:
        if plan_code == PLAN_GUILD_PRO:
            return "Babblebox Guild Pro"
        if plan_code == PLAN_FREE:
            return "Free"
        cleaned = str(plan_code or "").strip().replace("_", " ")
        return cleaned.title() if cleaned else "Unknown"

    def _describe_ai_premium_summary(
        self,
        *,
        guild_id: int,
        plan_code: str,
        source: str,
        stale: bool,
        in_grace: bool,
        premium_unlocked: bool,
        configured_models_capped: bool,
    ) -> str:
        if source == "blocked":
            return "Premium is suspended on this server. Higher Shield AI tiers are unavailable right now."
        if source == "support_guild":
            return "Internal Babblebox operator premium access is active on this server."
        if source == "manual_guild_grant":
            return "Guild Pro is active here through a direct server grant."
        if source.startswith("claim:"):
            if stale and in_grace:
                return "Patreon is stale, but Babblebox is still honoring the last verified Guild Pro entitlement inside its grace window."
            if stale and not premium_unlocked:
                return "The last verified Guild Pro entitlement is stale and its grace window has ended, so higher Shield AI tiers are inactive."
            return "Guild Pro is active here through the current premium claim."
        if int(guild_id or 0) == SYSTEM_PREMIUM_SUPPORT_GUILD_ID and premium_unlocked:
            return "Internal Babblebox operator premium access is active on this server."
        if stale and not premium_unlocked:
            return "The last verified Guild Pro entitlement is stale and its grace window has ended, so higher Shield AI tiers are inactive."
        if premium_unlocked:
            return f"{self._premium_plan_title(plan_code)} is active on this server."
        if configured_models_capped:
            return "Guild Pro is not active on this server. Stored higher-tier Shield AI settings stay configured, but the effective lane is capped until Guild Pro returns."
        return "Guild Pro is not active on this server."

    def _resolve_ai_premium_state(self, guild_id: int, policy: ShieldAIAccessPolicy) -> dict[str, Any]:
        premium_service = self._premium_service()
        snapshot: dict[str, Any] = {}
        if premium_service is not None and callable(getattr(premium_service, "get_guild_snapshot", None)):
            resolved_snapshot = premium_service.get_guild_snapshot(guild_id)
            if isinstance(resolved_snapshot, dict):
                snapshot = dict(resolved_snapshot)
        plan_code = str(snapshot.get("plan_code") or (PLAN_GUILD_PRO if policy.premium_unlocked else PLAN_FREE))
        active_plans_raw = snapshot.get("active_plans")
        if isinstance(active_plans_raw, (list, tuple)):
            active_plans = [str(value) for value in active_plans_raw if str(value or "").strip()]
        else:
            active_plans = []
        blocked = bool(snapshot.get("blocked"))
        stale = bool(snapshot.get("stale"))
        in_grace = bool(snapshot.get("in_grace"))
        claim = snapshot.get("claim") if isinstance(snapshot.get("claim"), dict) else None
        system_access = bool(snapshot.get("system_access"))
        system_access_scope = str(snapshot.get("system_access_scope") or "").strip().lower()
        configured_models_capped = tuple(policy.configured_allowed_models) != tuple(policy.allowed_models)

        if blocked:
            source = "blocked"
        elif system_access:
            source = system_access_scope or "support_guild"
        elif int(guild_id or 0) == SYSTEM_PREMIUM_SUPPORT_GUILD_ID and policy.premium_unlocked:
            source = "support_guild"
        elif claim is not None:
            source_kind = str(claim.get("source_kind") or "claim").strip().lower() or "claim"
            source = f"claim:{source_kind}"
        elif plan_code == PLAN_GUILD_PRO:
            source = "manual_guild_grant"
        else:
            source = "free"

        return {
            "plan_code": plan_code,
            "active_plans": active_plans,
            "source": source,
            "stale": stale,
            "in_grace": in_grace,
            "summary": self._describe_ai_premium_summary(
                guild_id=guild_id,
                plan_code=plan_code,
                source=source,
                stale=stale,
                in_grace=in_grace,
                premium_unlocked=policy.premium_unlocked,
                configured_models_capped=configured_models_capped,
            ),
        }

    def _premium_limit_error(self, *, guild_id: int, limit_key: str, limit_value: int, default_message: str) -> str:
        premium_service = self._premium_service()
        if premium_service is None:
            return default_message
        return premium_service.describe_limit_error(limit_key=limit_key, limit_value=limit_value)

    def _count_change_exceeds_limit(self, *, previous_count: int, next_count: int, limit_value: int) -> bool:
        return next_count > limit_value and next_count > previous_count

    def filter_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_SHIELD_FILTERS, FILTER_LIMIT)

    def allowlist_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_SHIELD_ALLOWLIST, ALLOWLIST_LIMIT)

    def custom_pattern_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_SHIELD_CUSTOM_PATTERNS, CUSTOM_PATTERN_LIMIT)

    def pack_exemption_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_SHIELD_PACK_EXEMPTIONS, FILTER_LIMIT)

    def severe_term_limit(self, guild_id: int) -> int:
        return self._resolve_guild_limit(guild_id, LIMIT_SHIELD_SEVERE_TERMS, SHIELD_SEVERE_TERM_LIMIT)

    def get_meta(self) -> dict[str, Any]:
        meta = self.store.state.get("meta")
        if isinstance(meta, dict):
            default_models = SHIELD_AI_MODEL_ORDER
            return {
                "ordinary_ai_enabled": bool(meta.get("ordinary_ai_enabled")),
                "ordinary_ai_allowed_models": tuple(
                    model
                    for model in (meta.get("ordinary_ai_allowed_models") or default_models)
                    if model in SHIELD_AI_MODEL_ORDER
                )
                or default_models,
                "ordinary_ai_updated_by": meta.get("ordinary_ai_updated_by"),
                "ordinary_ai_updated_at": meta.get("ordinary_ai_updated_at"),
            }
        return {
            "ordinary_ai_enabled": False,
            "ordinary_ai_allowed_models": SHIELD_AI_MODEL_ORDER,
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
        premium_enabled = self._guild_has_capability(guild_id, CAPABILITY_SHIELD_AI_REVIEW)
        ordinary_global_enabled = bool(meta["ordinary_ai_enabled"])
        ordinary_global_allowed_models = tuple(meta["ordinary_ai_allowed_models"]) or SHIELD_AI_MODEL_ORDER
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
        policy_enabled = enabled
        configured_allowed_models = allowed_models
        plan_allowed_models = SHIELD_AI_MODEL_ORDER if premium_enabled else (DEFAULT_SHIELD_AI_FAST_MODEL,)
        effective_allowed_models = tuple(model for model in configured_allowed_models if model in plan_allowed_models)
        if not effective_allowed_models:
            effective_allowed_models = (DEFAULT_SHIELD_AI_FAST_MODEL,)

        return ShieldAIAccessPolicy(
            guild_id=guild_id,
            enabled=policy_enabled,
            policy_enabled=policy_enabled,
            source=source,
            premium_unlocked=premium_enabled,
            configured_allowed_models=configured_allowed_models,
            allowed_models=effective_allowed_models,
            plan_allowed_models=plan_allowed_models,
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
        provider_status = str(diagnostics.get("status") or "Unavailable.")
        provider_available = bool(diagnostics.get("available"))
        provider_readiness = str(diagnostics.get("provider_readiness") or provider_status)
        model_override_note = str(
            diagnostics.get("model_override_note") or "No single-model override configured. Shield is using routed defaults."
        )
        invalid_model_settings_note = diagnostics.get("invalid_model_settings_note")
        configured_models_capped = tuple(policy.configured_allowed_models) != tuple(policy.allowed_models)
        single_model_override = bool(diagnostics.get("single_model_override"))
        top_tier_enabled = bool(diagnostics.get("top_tier_enabled"))
        provider_capped_models = (
            ("gpt-5",)
            if "gpt-5" in policy.allowed_models and not top_tier_enabled and not single_model_override
            else ()
        )
        provider_model_note = (
            "gpt-5 remains provider-gated until top-tier routing is enabled."
            if provider_capped_models
            else None
        )
        premium_state = self._resolve_ai_premium_state(guild_id, policy)
        setup_blockers: list[str] = []
        if policy.enabled and provider_available:
            if not bool(config.get("module_enabled")):
                setup_blockers.append("Shield live moderation is off.")
            if config.get("log_channel_id") is None:
                setup_blockers.append("Set a Shield log channel so AI-enriched alerts have a delivery lane.")
            if not enabled_packs:
                setup_blockers.append("Select at least one Shield pack for AI review.")
        status_message = provider_status
        if not policy.enabled:
            if policy.source == "guild_override":
                status_message = "AI review is disabled for this guild by owner override."
            else:
                status_message = "AI review is off until the owner enables it."
        elif not provider_available:
            status_message = "AI review is enabled by policy but the provider is not configured."
        elif setup_blockers:
            status_message = setup_blockers[0] if len(setup_blockers) == 1 else "AI review is enabled by policy, but local Shield setup is incomplete."
        elif policy.premium_unlocked:
            status_message = "Ready for second-pass review."
        else:
            status_message = "Ready for second-pass review with the nano tier."
        if provider_model_note and policy.enabled and provider_available and not setup_blockers:
            status_message = f"{status_message} {provider_model_note}"
        return {
            "supported": True,
            "enabled": policy.enabled,
            "policy_enabled": policy.policy_enabled,
            "policy_source": policy.source,
            "premium_unlocked": policy.premium_unlocked,
            "premium_required": not policy.premium_unlocked,
            "enhanced_models_unlocked": policy.premium_unlocked,
            "enhanced_models_required": not policy.premium_unlocked,
            "configured_allowed_models": list(policy.configured_allowed_models),
            "configured_models_capped": configured_models_capped,
            "plan_allowed_models": list(policy.plan_allowed_models),
            "ordinary_global_enabled": policy.ordinary_global_enabled,
            "ordinary_global_allowed_models": list(policy.ordinary_global_allowed_models),
            "guild_access_mode": policy.guild_access_mode,
            "guild_allowed_models_override": list(policy.guild_allowed_models_override),
            "effective_allowed_models": list(policy.allowed_models),
            "allowed_models": list(policy.allowed_models),
            "enabled_packs": enabled_packs,
            "min_confidence": config.get("ai_min_confidence", "high"),
            "provider": diagnostics.get("provider"),
            "provider_available": provider_available,
            "provider_status": provider_status,
            "provider_readiness": provider_readiness,
            "model": diagnostics.get("model"),
            "provider_capped_models": list(provider_capped_models),
            "provider_model_note": provider_model_note,
            "routing_strategy": diagnostics.get("routing_strategy"),
            "single_model_override": single_model_override,
            "ignored_model_settings": list(diagnostics.get("ignored_model_settings") or []),
            "model_override_state": diagnostics.get("model_override_state") or ("valid" if single_model_override else "blank"),
            "model_override_note": model_override_note,
            "routed_default_model": diagnostics.get("routed_default_model"),
            "invalid_model_settings_note": str(invalid_model_settings_note) if invalid_model_settings_note else None,
            "fast_model": diagnostics.get("fast_model"),
            "complex_model": diagnostics.get("complex_model"),
            "top_model": diagnostics.get("top_model"),
            "top_tier_enabled": top_tier_enabled,
            "timeout_seconds": diagnostics.get("timeout_seconds"),
            "max_chars": diagnostics.get("max_chars"),
            "premium_plan_code": premium_state["plan_code"],
            "premium_active_plans": list(premium_state["active_plans"]),
            "premium_source": premium_state["source"],
            "premium_stale": premium_state["stale"],
            "premium_in_grace": premium_state["in_grace"],
            "premium_summary": premium_state["summary"],
            "status": status_message,
            "setup_blockers": setup_blockers,
            "ready_for_review": policy.enabled and provider_available and not setup_blockers,
            "updated_by": policy.updated_by,
            "updated_at": policy.updated_at,
        }

    async def probe_ai_provider(self, guild_id: int) -> dict[str, Any]:
        ai_status = self.get_ai_status(guild_id)
        base: dict[str, Any] = {
            "ok": False,
            "guild_id": guild_id,
            "provider": ai_status.get("provider") or "Not configured",
            "provider_available": bool(ai_status.get("provider_available")),
            "provider_readiness": ai_status.get("provider_readiness") or ai_status.get("provider_status") or "Unavailable.",
            "provider_status": ai_status.get("provider_status") or "Unavailable.",
            "policy_enabled": bool(ai_status.get("enabled")),
            "ready_for_review": bool(ai_status.get("ready_for_review")),
            "live_blockers": list(ai_status.get("setup_blockers") or []),
            "model_override_note": ai_status.get("model_override_note"),
            "ignored_model_settings": list(ai_status.get("ignored_model_settings") or []),
            "routed_default_model": ai_status.get("routed_default_model"),
            "effective_allowed_models": list(ai_status.get("effective_allowed_models") or ai_status.get("allowed_models") or []),
        }
        if not base["policy_enabled"]:
            base["live_blockers"].append(str(ai_status.get("status") or "AI review is off until the owner enables it."))
        elif not base["ready_for_review"] and not base["live_blockers"]:
            base["live_blockers"].append(str(ai_status.get("status") or "AI review is not ready for live second-pass review."))
        if not base["provider_available"]:
            base.update(
                {
                    "failure_reason": "provider_unavailable",
                    "message": f"Provider probe skipped: {base['provider_readiness']}",
                }
            )
            return base

        diagnostics = self.ai_provider.diagnostics()
        max_chars = int(diagnostics.get("max_chars") or 340)
        sanitized = sanitize_message_for_ai("Provider probe: contact me at probe@example.com for access.", max_chars=max_chars)
        request = ShieldAIReviewRequest(
            guild_id=guild_id,
            pack="privacy",
            local_confidence="high",
            local_action="delete_log",
            local_labels=("Privacy Leak", "Provider Probe"),
            local_reasons=("Synthetic provider probe with redacted contact detail.",),
            sanitized_content=sanitized.text,
            sanitized_redaction_count=sanitized.redaction_count,
            sanitized_truncated=sanitized.truncated,
            has_links=False,
            domains=(),
            has_suspicious_attachment=False,
            attachment_extensions=(),
            invite_detected=False,
            repetitive_promo=False,
            allowed_models=tuple(base["effective_allowed_models"]) or (DEFAULT_SHIELD_AI_FAST_MODEL,),
        )
        try:
            result = await self.ai_provider.review(request)
        except Exception as exc:  # pragma: no cover - defensive around custom provider adapters.
            LOGGER.info("Shield AI provider probe failed with unexpected provider exception (%s).", type(exc).__name__)
            base.update(
                {
                    "failure_reason": "provider_exception",
                    "message": f"Provider probe failed before returning a review ({type(exc).__name__}).",
                }
            )
            return base
        if result is None:
            failure = self.ai_provider.diagnostics().get("last_review_failure")
            failure_reason = "provider_no_review"
            failure_detail = None
            if isinstance(failure, dict):
                candidate_reason = str(failure.get("reason") or "").strip()
                candidate_detail = str(failure.get("detail") or "").strip()
                if candidate_reason:
                    failure_reason = candidate_reason
                if candidate_detail:
                    failure_detail = candidate_detail
            base.update(
                {
                    "failure_reason": failure_reason,
                    "provider_failure_detail": failure_detail,
                    "message": (
                        f"Provider probe reached the review path but did not return a review: {failure_detail}"
                        if failure_detail
                        else "Provider probe reached the review path but did not return a review."
                    ),
                }
            )
            return base

        base.update(
            {
                "ok": True,
                "message": "Provider probe succeeded. Live second-pass review still follows owner policy and local Shield readiness.",
                "classification": result.classification,
                "classification_label": result.classification_label,
                "confidence": result.confidence,
                "priority": result.priority,
                "false_positive": result.false_positive,
                "explanation": result.explanation,
                "model": result.model,
                "tier": result.tier,
                "target_tier": result.target_tier,
                "route_reasons": list(result.route_reasons),
                "attempted_models": list(result.attempted_models),
                "fallback_used": result.fallback_used,
                "policy_capped": result.policy_capped,
            }
        )
        return base

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
        return True, "Support server Shield AI now inherits the ordinary owner policy again."

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
        attachment_texts, attachment_link_texts = _collect_attachment_surface_texts(fake_attachments)
        surface_labels = ("attachment_meta",) if attachment_texts else ()
        link_segments = []
        if normalize_plain_text(text):
            link_segments.append(("message", text, False))
        link_segments.extend(("attachment_meta", item, False) for item in attachment_link_texts)
        snapshot = _build_snapshot(
            text,
            fake_attachments,
            extra_texts=attachment_texts,
            link_texts=attachment_link_texts,
            link_segments=tuple(link_segments),
            surface_labels=surface_labels,
        )
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
        link_explanations = self._build_link_decision_explanations(compiled, snapshot, link_assessments, matches)
        return ShieldTestResult(
            matches=tuple(matches),
            link_assessments=link_assessments,
            link_explanations=link_explanations,
            bypass_reason=bypass_reason,
        )

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
            message += " Shield AI stays second-pass only, owner policy controls whether review runs, and Babblebox Guild Pro can make gpt-5-mini plus gpt-5 available when provider/runtime readiness also allows review."
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
                if pack in {"spam", "gif"}:
                    config[f"{pack}_enabled"] = True
                    config[f"{pack}_action"] = "delete_timeout_log"
                    config[f"{pack}_low_action"] = "log"
                    config[f"{pack}_medium_action"] = "delete_log"
                    config[f"{pack}_high_action"] = "delete_timeout_log"
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
        return (
            f"{CONFIDENCE_LABELS['low']}: {ACTION_LABELS.get(low_action, low_action)} | "
            f"{CONFIDENCE_LABELS['medium']}: {ACTION_LABELS.get(medium_action, medium_action)} | "
            f"{CONFIDENCE_LABELS['high']}: {ACTION_LABELS.get(high_action, high_action)}"
        )

    def _spam_moderator_policy_label(self, policy: str) -> str:
        return SPAM_MODERATOR_POLICY_LABELS.get(policy, policy.replace("_", " ").title())

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
        message_enabled: bool | None = None,
        message_threshold: int | None = None,
        window_seconds: int | None = None,
        burst_enabled: bool | None = None,
        burst_threshold: int | None = None,
        burst_window_seconds: int | None = None,
        near_duplicate_enabled: bool | None = None,
        duplicate_threshold: int | None = None,
        duplicate_window_seconds: int | None = None,
        emote_enabled: bool | None = None,
        emote_threshold: int | None = None,
        caps_enabled: bool | None = None,
        caps_threshold: int | None = None,
        low_value_enabled: bool | None = None,
        low_value_threshold: int | None = None,
        low_value_window_seconds: int | None = None,
        moderator_policy: str | None = None,
        consecutive_enabled: bool | None = None,
        consecutive_threshold: int | None = None,
        repeat_enabled: bool | None = None,
        repeat_threshold: int | None = None,
        same_asset_enabled: bool | None = None,
        same_asset_threshold: int | None = None,
        ratio_percent: int | None = None,
    ) -> tuple[bool, str]:
        if pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        if adult_solicitation is not None and pack != "adult":
            return False, "Adult solicitation can only be configured on the adult pack."
        if pack not in {"spam", "gif"} and any(
            value is not None
            for value in (
                message_enabled,
                message_threshold,
                window_seconds,
                burst_enabled,
                burst_threshold,
                burst_window_seconds,
                near_duplicate_enabled,
                duplicate_threshold,
                duplicate_window_seconds,
                emote_enabled,
                emote_threshold,
                caps_enabled,
                caps_threshold,
                low_value_enabled,
                low_value_threshold,
                low_value_window_seconds,
                moderator_policy,
                consecutive_enabled,
                consecutive_threshold,
                repeat_enabled,
                repeat_threshold,
                same_asset_enabled,
                same_asset_threshold,
                ratio_percent,
            )
        ):
            return False, "Explicit threshold settings are only available on the spam and GIF packs."
        if pack != "spam" and any(
            value is not None
            for value in (
                burst_enabled,
                burst_threshold,
                burst_window_seconds,
                near_duplicate_enabled,
                duplicate_threshold,
                duplicate_window_seconds,
                emote_enabled,
                emote_threshold,
                caps_enabled,
                caps_threshold,
                low_value_enabled,
                low_value_threshold,
                low_value_window_seconds,
                moderator_policy,
            )
        ):
            return False, "Burst thresholds, duplicate thresholds, emote, capitals, low-value chatter, and moderator policy only apply to the spam pack."
        if pack != "gif" and any(
            value is not None
            for value in (consecutive_enabled, consecutive_threshold, repeat_enabled, repeat_threshold, same_asset_enabled, same_asset_threshold, ratio_percent)
        ):
            return False, "GIF streak, repeat, same-asset, and ratio thresholds can only be configured on the GIF pack."
        cleaned_action = action.strip().lower() if isinstance(action, str) else None
        cleaned_low_action = low_action.strip().lower() if isinstance(low_action, str) else None
        cleaned_medium_action = medium_action.strip().lower() if isinstance(medium_action, str) else None
        cleaned_high_action = high_action.strip().lower() if isinstance(high_action, str) else None
        cleaned_sensitivity = sensitivity.strip().lower() if isinstance(sensitivity, str) else None
        cleaned_moderator_policy = moderator_policy.strip().lower() if isinstance(moderator_policy, str) else None
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
        if cleaned_moderator_policy is not None and cleaned_moderator_policy not in VALID_SPAM_MODERATOR_POLICIES:
            return False, "Moderator anti-spam policy must be exempt, delete_only, or full."
        if message_threshold is not None:
            field = "gif_message_threshold" if pack == "gif" else "spam_message_threshold"
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS[field]
            if not (minimum <= message_threshold <= maximum):
                if pack == "gif":
                    return False, f"GIF message threshold must be between {minimum} and {maximum}."
                return False, f"Message threshold must be between {minimum} and {maximum}."
        if window_seconds is not None:
            field = "gif_window_seconds" if pack == "gif" else "spam_message_window_seconds"
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS[field]
            if not (minimum <= window_seconds <= maximum):
                return False, f"Window length must be between {minimum} and {maximum} seconds."
        if burst_threshold is not None and not (4 <= burst_threshold <= 10):
            return False, "Burst threshold must be between 4 and 10."
        if burst_window_seconds is not None and not (5 <= burst_window_seconds <= 30):
            return False, "Burst window must be between 5 and 30 seconds."
        if duplicate_threshold is not None and not (3 <= duplicate_threshold <= 10):
            return False, "Near-duplicate threshold must be between 3 and 10."
        if duplicate_window_seconds is not None and not (5 <= duplicate_window_seconds <= 45):
            return False, "Near-duplicate window must be between 5 and 45 seconds."
        if emote_threshold is not None and not (8 <= emote_threshold <= 40):
            return False, "Emote threshold must be between 8 and 40."
        if caps_threshold is not None and not (12 <= caps_threshold <= 80):
            return False, "Capitals threshold must be between 12 and 80."
        if low_value_threshold is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["spam_low_value_threshold"]
            if not (minimum <= low_value_threshold <= maximum):
                return False, f"Low-value chatter threshold must be between {minimum} and {maximum}."
        if low_value_window_seconds is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["spam_low_value_window_seconds"]
            if not (minimum <= low_value_window_seconds <= maximum):
                return False, f"Low-value chatter window must be between {minimum} and {maximum} seconds."
        if repeat_threshold is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["gif_repeat_threshold"]
            if not (minimum <= repeat_threshold <= maximum):
                return False, f"GIF repeat threshold must be between {minimum} and {maximum}."
        if consecutive_threshold is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["gif_consecutive_threshold"]
            if not (minimum <= consecutive_threshold <= maximum):
                return False, f"GIF consecutive threshold must be between {minimum} and {maximum}."
        if same_asset_threshold is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["gif_same_asset_threshold"]
            if not (minimum <= same_asset_threshold <= maximum):
                return False, f"Same-GIF threshold must be between {minimum} and {maximum}."
        if ratio_percent is not None:
            minimum, maximum, _default = SHIELD_NUMERIC_CONFIG_SPECS["gif_min_ratio_percent"]
            if not (minimum <= ratio_percent <= maximum):
                return False, f"GIF ratio must be between {minimum} and {maximum} percent."

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
            if pack == "spam":
                if message_enabled is not None:
                    config["spam_message_enabled"] = bool(message_enabled)
                if message_threshold is not None:
                    config["spam_message_threshold"] = message_threshold
                if window_seconds is not None:
                    config["spam_message_window_seconds"] = window_seconds
                if burst_enabled is not None:
                    config["spam_burst_enabled"] = bool(burst_enabled)
                if burst_threshold is not None:
                    config["spam_burst_threshold"] = burst_threshold
                if burst_window_seconds is not None:
                    config["spam_burst_window_seconds"] = burst_window_seconds
                if near_duplicate_enabled is not None:
                    config["spam_near_duplicate_enabled"] = bool(near_duplicate_enabled)
                if duplicate_threshold is not None:
                    config["spam_near_duplicate_threshold"] = duplicate_threshold
                if duplicate_window_seconds is not None:
                    config["spam_near_duplicate_window_seconds"] = duplicate_window_seconds
                if emote_enabled is not None:
                    config["spam_emote_enabled"] = bool(emote_enabled)
                if emote_threshold is not None:
                    config["spam_emote_threshold"] = emote_threshold
                if caps_enabled is not None:
                    config["spam_caps_enabled"] = bool(caps_enabled)
                if caps_threshold is not None:
                    config["spam_caps_threshold"] = caps_threshold
                if low_value_enabled is not None:
                    config["spam_low_value_enabled"] = bool(low_value_enabled)
                if low_value_threshold is not None:
                    config["spam_low_value_threshold"] = low_value_threshold
                if low_value_window_seconds is not None:
                    config["spam_low_value_window_seconds"] = low_value_window_seconds
                if cleaned_moderator_policy is not None:
                    config["spam_moderator_policy"] = cleaned_moderator_policy
            if pack == "gif":
                if message_enabled is not None:
                    config["gif_message_enabled"] = bool(message_enabled)
                if message_threshold is not None:
                    config["gif_message_threshold"] = message_threshold
                if window_seconds is not None:
                    config["gif_window_seconds"] = window_seconds
                if consecutive_enabled is not None:
                    config["gif_consecutive_enabled"] = bool(consecutive_enabled)
                if consecutive_threshold is not None:
                    config["gif_consecutive_threshold"] = consecutive_threshold
                if repeat_enabled is not None:
                    config["gif_repeat_enabled"] = bool(repeat_enabled)
                if repeat_threshold is not None:
                    config["gif_repeat_threshold"] = repeat_threshold
                if same_asset_enabled is not None:
                    config["gif_same_asset_enabled"] = bool(same_asset_enabled)
                if same_asset_threshold is not None:
                    config["gif_same_asset_threshold"] = same_asset_threshold
                if ratio_percent is not None:
                    config["gif_min_ratio_percent"] = ratio_percent

        new_enabled = current[f"{pack}_enabled"] if enabled is None else bool(enabled)
        new_sensitivity = current[f"{pack}_sensitivity"] if cleaned_sensitivity is None else cleaned_sensitivity
        solicitation_note = ""
        if pack == "adult":
            solicitation_state = current.get("adult_solicitation_enabled", False) if adult_solicitation is None else bool(adult_solicitation)
            solicitation_note = f" Optional solicitation text detection is {'on' if solicitation_state else 'off'}."
        policy_note = ""
        if pack == "spam":
            final_message_enabled = current["spam_message_enabled"] if message_enabled is None else bool(message_enabled)
            final_threshold = current["spam_message_threshold"] if message_threshold is None else message_threshold
            final_window = current["spam_message_window_seconds"] if window_seconds is None else window_seconds
            final_burst_enabled = current["spam_burst_enabled"] if burst_enabled is None else bool(burst_enabled)
            final_burst_threshold = current["spam_burst_threshold"] if burst_threshold is None else burst_threshold
            final_burst_window = current["spam_burst_window_seconds"] if burst_window_seconds is None else burst_window_seconds
            final_near_duplicate_enabled = (
                current["spam_near_duplicate_enabled"] if near_duplicate_enabled is None else bool(near_duplicate_enabled)
            )
            final_duplicate_threshold = current["spam_near_duplicate_threshold"] if duplicate_threshold is None else duplicate_threshold
            final_duplicate_window = current["spam_near_duplicate_window_seconds"] if duplicate_window_seconds is None else duplicate_window_seconds
            final_emote_enabled = current["spam_emote_enabled"] if emote_enabled is None else bool(emote_enabled)
            final_emote_threshold = current["spam_emote_threshold"] if emote_threshold is None else emote_threshold
            final_caps_enabled = current["spam_caps_enabled"] if caps_enabled is None else bool(caps_enabled)
            final_caps_threshold = current["spam_caps_threshold"] if caps_threshold is None else caps_threshold
            final_low_value_enabled = current["spam_low_value_enabled"] if low_value_enabled is None else bool(low_value_enabled)
            final_low_value_threshold = current["spam_low_value_threshold"] if low_value_threshold is None else low_value_threshold
            final_low_value_window = (
                current["spam_low_value_window_seconds"] if low_value_window_seconds is None else low_value_window_seconds
            )
            final_moderator_policy = current["spam_moderator_policy"] if cleaned_moderator_policy is None else cleaned_moderator_policy
            policy_note = (
                f" Rate lane: {'on' if final_message_enabled else 'off'}"
                + (f" at {final_threshold} messages in {final_window}s. " if final_message_enabled else ". ")
                + f"Burst lane: {'on' if final_burst_enabled else 'off'}"
                + (f" at {final_burst_threshold} messages in {final_burst_window}s. " if final_burst_enabled else ". ")
                + f"Near-duplicate lane: {'on' if final_near_duplicate_enabled else 'off'}"
                + (
                    f" at {final_duplicate_threshold} variants in {final_duplicate_window}s. "
                    if final_near_duplicate_enabled
                    else ". "
                )
                +
                f"Emote spam: {'on' if final_emote_enabled else 'off'}"
                + (f" at {final_emote_threshold}+ tokens." if final_emote_enabled else ". ")
                + f" Capitals spam: {'on' if final_caps_enabled else 'off'}"
                + (f" at {final_caps_threshold}+ uppercase letters. " if final_caps_enabled else ". ")
                + f"Low-value chatter: {'on' if final_low_value_enabled else 'off'}"
                + (
                    f" at {final_low_value_threshold} messages in {final_low_value_window}s. "
                    if final_low_value_enabled
                    else ". "
                )
                + f" Moderator anti-spam policy: {self._spam_moderator_policy_label(final_moderator_policy)}."
            )
        if pack == "gif":
            final_message_enabled = current["gif_message_enabled"] if message_enabled is None else bool(message_enabled)
            final_threshold = current["gif_message_threshold"] if message_threshold is None else message_threshold
            final_window = current["gif_window_seconds"] if window_seconds is None else window_seconds
            final_consecutive_enabled = current["gif_consecutive_enabled"] if consecutive_enabled is None else bool(consecutive_enabled)
            final_consecutive = current["gif_consecutive_threshold"] if consecutive_threshold is None else consecutive_threshold
            final_repeat_enabled = current["gif_repeat_enabled"] if repeat_enabled is None else bool(repeat_enabled)
            final_repeat = current["gif_repeat_threshold"] if repeat_threshold is None else repeat_threshold
            final_same_asset_enabled = current["gif_same_asset_enabled"] if same_asset_enabled is None else bool(same_asset_enabled)
            final_same_asset = current["gif_same_asset_threshold"] if same_asset_threshold is None else same_asset_threshold
            final_ratio = current["gif_min_ratio_percent"] if ratio_percent is None else ratio_percent
            policy_note = (
                f" GIF-heavy rate lane: {'on' if final_message_enabled else 'off'}"
                + (f" at {final_threshold} posts in {final_window}s. " if final_message_enabled else ". ")
                + f"True channel streak lane: {'on' if final_consecutive_enabled else 'off'}"
                + (f" at {final_consecutive}+ consecutive GIF-heavy messages. " if final_consecutive_enabled else ". ")
                + f"Low-text repeat lane: {'on' if final_repeat_enabled else 'off'}"
                + (f" at {final_repeat}+ repeats with {final_ratio}%+ GIF pressure in the recent window. " if final_repeat_enabled else ". ")
                + f"Same-GIF lane: {'on' if final_same_asset_enabled else 'off'}"
                + (f" at {final_same_asset}+ uses of the same asset." if final_same_asset_enabled else ".")
            )
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[pack]} is {'enabled' if new_enabled else 'disabled'} "
                f"with {self._policy_summary(low_action=final_low_action, medium_action=final_medium_action, high_action=final_high_action)} "
                f"at {SENSITIVITY_LABELS[new_sensitivity].lower()} sensitivity."
                f"{solicitation_note}{policy_note}"
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
        severe_limit = self.severe_term_limit(guild_id)

        def mutate(config: dict[str, Any]):
            custom_terms = list(_sorted_unique_text(config.get("severe_custom_terms", [])))
            removed_terms = list(_sorted_unique_text(config.get("severe_removed_terms", [])))
            if cleaned_action == "add":
                if term in custom_terms or term in SEVERE_REMOVABLE_DEFAULT_TERMS:
                    raise ValueError("That severe term is already active.")
                if len(custom_terms) >= severe_limit:
                    raise ValueError(
                        self._premium_limit_error(
                            guild_id=guild_id,
                            limit_key=LIMIT_SHIELD_SEVERE_TERMS,
                            limit_value=severe_limit,
                            default_message=f"You can keep up to {severe_limit} custom severe terms.",
                        )
                    )
                custom_terms.append(term)
            elif cleaned_action == "remove_custom":
                if term not in custom_terms:
                    raise ValueError("That custom severe term was not configured.")
                custom_terms.remove(term)
            elif cleaned_action == "remove_default":
                if term not in SEVERE_REMOVABLE_DEFAULT_TERMS:
                    raise ValueError("That phrase is not one of Babblebox's removable bundled severe terms.")
                if term not in removed_terms:
                    if len(removed_terms) >= severe_limit:
                        raise ValueError(
                            self._premium_limit_error(
                                guild_id=guild_id,
                                limit_key=LIMIT_SHIELD_SEVERE_TERMS,
                                limit_value=severe_limit,
                                default_message=f"You can keep up to {severe_limit} removed bundled severe terms.",
                            )
                        )
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

    async def set_log_delivery(
        self,
        guild_id: int,
        *,
        style: str | None = None,
        ping_mode: str | None = None,
    ) -> tuple[bool, str]:
        cleaned_style = str(style).strip().lower() if isinstance(style, str) else None
        cleaned_ping_mode = str(ping_mode).strip().lower() if isinstance(ping_mode, str) else None
        if cleaned_style is not None and cleaned_style not in VALID_SHIELD_LOG_STYLES:
            return False, "Shield log style must be `adaptive` or `compact`."
        if cleaned_ping_mode is not None and cleaned_ping_mode not in VALID_SHIELD_LOG_PING_MODES:
            return False, "Shield log ping mode must be `smart` or `never`."
        if cleaned_style is None and cleaned_ping_mode is None:
            return False, "Choose a Shield log style and/or ping mode to update."

        current = self.get_config(guild_id)
        final_style = current.get("log_style", "adaptive") if cleaned_style is None else cleaned_style
        final_ping_mode = current.get("log_ping_mode", "smart") if cleaned_ping_mode is None else cleaned_ping_mode

        def mutate(config: dict[str, Any]):
            if cleaned_style is not None:
                config["log_style"] = cleaned_style
            if cleaned_ping_mode is not None:
                config["log_ping_mode"] = cleaned_ping_mode

        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Shield log delivery now defaults to `{final_style}` style with `{final_ping_mode}` ping mode."
            ),
        )

    async def set_pack_log_override(
        self,
        guild_id: int,
        pack: str,
        *,
        style: str | None = None,
        ping_mode: str | None = None,
    ) -> tuple[bool, str]:
        cleaned_pack = normalize_plain_text(pack).casefold()
        if cleaned_pack not in RULE_PACKS:
            return False, "Pack log overrides only apply to Shield's core packs."
        cleaned_style = str(style).strip().lower() if isinstance(style, str) else None
        cleaned_ping_mode = str(ping_mode).strip().lower() if isinstance(ping_mode, str) else None
        valid_styles = {"inherit", *VALID_SHIELD_LOG_STYLES}
        valid_ping_modes = {"inherit", *VALID_SHIELD_LOG_PING_MODES}
        if cleaned_style is not None and cleaned_style not in valid_styles:
            return False, "Pack log style must be `inherit`, `adaptive`, or `compact`."
        if cleaned_ping_mode is not None and cleaned_ping_mode not in valid_ping_modes:
            return False, "Pack log ping mode must be `inherit`, `smart`, or `never`."
        if cleaned_style is None and cleaned_ping_mode is None:
            return False, "Choose a pack log style and/or ping mode to update."

        current = self.get_config(guild_id)
        raw_overrides = current.get("pack_log_overrides", {})
        current_override = raw_overrides.get(cleaned_pack, {}) if isinstance(raw_overrides, dict) else {}
        final_style = current_override.get("style", "inherit") if cleaned_style is None else cleaned_style
        final_ping_mode = current_override.get("ping_mode", "inherit") if cleaned_ping_mode is None else cleaned_ping_mode

        def mutate(config: dict[str, Any]):
            overrides = dict(config.get("pack_log_overrides", {}))
            pack_override = dict(overrides.get(cleaned_pack, {}))
            if cleaned_style is not None:
                pack_override["style"] = cleaned_style
            if cleaned_ping_mode is not None:
                pack_override["ping_mode"] = cleaned_ping_mode
            overrides[cleaned_pack] = pack_override
            config["pack_log_overrides"] = overrides

        resolved_style = current.get("log_style", "adaptive") if final_style == "inherit" else final_style
        resolved_ping_mode = current.get("log_ping_mode", "smart") if final_ping_mode == "inherit" else final_ping_mode
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[cleaned_pack]} log delivery override saved. "
                f"Local style: `{final_style}` | local ping mode: `{final_ping_mode}` | "
                f"effective style: `{resolved_style}` | effective ping mode: `{resolved_ping_mode}`."
            ),
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
        filter_limit = self.filter_limit(guild_id)

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_ints(config.get(field, [])))
            previous_count = len(values)
            if enabled:
                values.add(target_id)
            else:
                values.discard(target_id)
            if self._count_change_exceeds_limit(previous_count=previous_count, next_count=len(values), limit_value=filter_limit):
                raise ValueError(
                    self._premium_limit_error(
                        guild_id=guild_id,
                        limit_key=LIMIT_SHIELD_FILTERS,
                        limit_value=filter_limit,
                        default_message=f"You can keep up to {filter_limit} entries in `{label}`.",
                    )
                )
            config[field] = sorted(values)

        return await self._update_config(
            guild_id,
            mutate,
            success_message=f"Shield {label} was {'updated' if enabled else 'trimmed'}.",
        )

    async def set_pack_exemption(self, guild_id: int, pack: str, target_kind: str, target_id: int, enabled: bool) -> tuple[bool, str]:
        cleaned_pack = normalize_plain_text(pack).casefold()
        cleaned_kind = normalize_plain_text(target_kind).casefold()
        if cleaned_pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        bucket = {"channel": "channel_ids", "role": "role_ids", "user": "user_ids"}.get(cleaned_kind)
        if bucket is None:
            return False, "Pack exemptions must target a channel, role, or member."
        exemption_limit = self.pack_exemption_limit(guild_id)

        def mutate(config: dict[str, Any]):
            pack_exemptions = dict(config.get("pack_exemptions", {}))
            pack_entry = dict(pack_exemptions.get(cleaned_pack, {}))
            values = set(_sorted_unique_ints(pack_entry.get(bucket, [])))
            previous_count = len(values)
            if enabled:
                values.add(target_id)
            else:
                values.discard(target_id)
            if self._count_change_exceeds_limit(previous_count=previous_count, next_count=len(values), limit_value=exemption_limit):
                raise ValueError(
                    self._premium_limit_error(
                        guild_id=guild_id,
                        limit_key=LIMIT_SHIELD_PACK_EXEMPTIONS,
                        limit_value=exemption_limit,
                        default_message=f"You can keep up to {exemption_limit} {cleaned_kind} exemptions on a single Shield pack.",
                    )
                )
            pack_entry[bucket] = sorted(values)
            pack_exemptions[cleaned_pack] = pack_entry
            config["pack_exemptions"] = pack_exemptions

        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[cleaned_pack]} {cleaned_kind} exemptions were {'updated' if enabled else 'trimmed'}. "
                "Pack-specific exemptions stay scoped to that pack only."
            ),
        )

    async def replace_pack_exemptions(
        self,
        guild_id: int,
        pack: str,
        target_kind: str,
        target_ids: Sequence[int],
    ) -> tuple[bool, str]:
        cleaned_pack = normalize_plain_text(pack).casefold()
        cleaned_kind = normalize_plain_text(target_kind).casefold()
        if cleaned_pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        bucket = {"channel": "channel_ids", "role": "role_ids", "user": "user_ids"}.get(cleaned_kind)
        if bucket is None:
            return False, "Pack exemptions must target a channel, role, or member."
        cleaned_ids = _sorted_unique_ints(target_ids)
        exemption_limit = self.pack_exemption_limit(guild_id)
        current = self.get_config(guild_id)
        current_entry = (current.get("pack_exemptions", {}) or {}).get(cleaned_pack, {})
        previous_count = len(_sorted_unique_ints((current_entry or {}).get(bucket, [])))
        if self._count_change_exceeds_limit(previous_count=previous_count, next_count=len(cleaned_ids), limit_value=exemption_limit):
            return False, self._premium_limit_error(
                guild_id=guild_id,
                limit_key=LIMIT_SHIELD_PACK_EXEMPTIONS,
                limit_value=exemption_limit,
                default_message=f"You can keep up to {exemption_limit} {cleaned_kind} exemptions on a single Shield pack.",
            )

        def mutate(config: dict[str, Any]):
            pack_exemptions = dict(config.get("pack_exemptions", {}))
            pack_entry = dict(pack_exemptions.get(cleaned_pack, {}))
            pack_entry[bucket] = list(cleaned_ids)
            pack_exemptions[cleaned_pack] = pack_entry
            config["pack_exemptions"] = pack_exemptions

        summary = "None configured" if not cleaned_ids else f"{len(cleaned_ids)} saved"
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"{PACK_LABELS[cleaned_pack]} {cleaned_kind} exemptions were replaced. "
                f"Current set: {summary}."
            ),
        )

    async def set_pack_timeout_override(
        self,
        guild_id: int,
        pack: str,
        timeout_minutes: int | None,
    ) -> tuple[bool, str]:
        cleaned_pack = normalize_plain_text(pack).casefold()
        if cleaned_pack not in RULE_PACKS:
            return False, "Unknown Shield pack."
        return await self._set_timeout_override(guild_id, cleaned_pack, timeout_minutes)

    async def set_link_policy_timeout_override(
        self,
        guild_id: int,
        timeout_minutes: int | None,
    ) -> tuple[bool, str]:
        return await self._set_timeout_override(guild_id, "link_policy", timeout_minutes)

    async def _set_timeout_override(
        self,
        guild_id: int,
        pack: str,
        timeout_minutes: int | None,
    ) -> tuple[bool, str]:
        if pack not in PACK_TIMEOUT_PACKS:
            return False, "Unknown Shield timeout target."
        if timeout_minutes is not None and not (1 <= timeout_minutes <= 60):
            return False, "Timeout length must be between 1 and 60 minutes."

        def mutate(config: dict[str, Any]):
            pack_timeout_minutes = dict(config.get("pack_timeout_minutes", {}))
            pack_timeout_minutes[pack] = timeout_minutes
            config["pack_timeout_minutes"] = pack_timeout_minutes

        preview = self.get_config(guild_id)
        global_timeout = int(preview.get("timeout_minutes", 10))
        if timeout_minutes is None:
            message = (
                f"{PACK_LABELS.get(pack, pack.replace('_', ' ').title())} now inherits the global `{global_timeout}` minute timeout."
            )
        else:
            message = (
                f"{PACK_LABELS.get(pack, pack.replace('_', ' ').title())} now uses a dedicated `{timeout_minutes}` minute timeout."
            )
        return await self._update_config(guild_id, mutate, success_message=message)

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
        allowlist_limit = self.allowlist_limit(guild_id)

        def mutate(config: dict[str, Any]):
            values = set(_sorted_unique_text(config.get(field, [])))
            previous_count = len(values)
            if enabled:
                values.add(cleaned)
            else:
                values.discard(cleaned)
            if self._count_change_exceeds_limit(previous_count=previous_count, next_count=len(values), limit_value=allowlist_limit):
                raise ValueError(
                    self._premium_limit_error(
                        guild_id=guild_id,
                        limit_key=LIMIT_SHIELD_ALLOWLIST,
                        limit_value=allowlist_limit,
                        default_message=f"You can keep up to {allowlist_limit} entries in that allowlist.",
                    )
                )
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
            return (
                False,
                "Shield AI availability is resolved by owner policy outside `/shield ai`. This command only changes review threshold and eligible packs, and Babblebox Guild Pro only makes gpt-5-mini and gpt-5 available when owner policy and provider/runtime readiness also allow review.",
            )
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
        entitlement_note = (
            "Guild Pro enhanced models are active on this server."
            if policy.premium_unlocked
            else "Guild Pro is still required for gpt-5-mini and gpt-5 on this server."
        )
        return await self._update_config(
            guild_id,
            mutate,
            success_message=(
                f"Shield AI review scope now uses `{final_min_confidence}` minimum local confidence for {pack_summary}. "
                f"Owner policy source: `{policy.source}`. "
                f"Allowed models right now: {format_shield_ai_model_list(policy.allowed_models)}. "
                f"{entitlement_note} "
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
        custom_pattern_limit = self.custom_pattern_limit(guild_id)

        def mutate(config: dict[str, Any]):
            items = [item for item in config.get("custom_patterns", []) if isinstance(item, dict)]
            if len(items) >= custom_pattern_limit:
                raise ValueError(
                    self._premium_limit_error(
                        guild_id=guild_id,
                        limit_key=LIMIT_SHIELD_CUSTOM_PATTERNS,
                        limit_value=custom_pattern_limit,
                        default_message=f"You can keep up to {custom_pattern_limit} advanced Shield patterns.",
                    )
                )
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
        author_kind = _author_kind_for_message(message, scan_source=resolved_scan_source)
        snapshot = _build_message_snapshot(message, author_kind=author_kind)
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
        author_kind = _author_kind_for_message(after, scan_source="message_edit")
        before_snapshot = _build_message_snapshot(before, author_kind=author_kind)
        after_snapshot = _build_message_snapshot(after, author_kind=author_kind)
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

    def _track_spam_event(
        self,
        guild_id: int,
        user_id: int,
        channel_id: int | None,
        message: discord.Message,
        snapshot: ShieldSnapshot,
        *,
        now: float,
        gif_pack_exempt: bool = False,
    ) -> tuple[ShieldSpamEvent, tuple[ShieldSpamEvent, ...]]:
        key = (guild_id, user_id)
        rows = [
            event
            for event in self._recent_spam_events.get(key, [])
            if now - event.timestamp <= SPAM_EVENT_WINDOW_SECONDS
        ]
        media_only_links = bool(snapshot.links) and all(link.category == "media_embed" for link in snapshot.links)
        link_signature = "|".join(sorted(snapshot.canonical_links)[:3]) if snapshot.canonical_links else None
        text_substance, text_relief_weight = _classify_gif_text_substance(
            snapshot.context_text,
            plain_word_count=snapshot.plain_word_count,
            low_value_text=snapshot.low_value_text,
            risky=bool(
                snapshot.has_links
                or snapshot.has_suspicious_attachment
                or snapshot.invite_codes
                or snapshot.mention_count > 0
                or snapshot.everyone_here_count > 0
                or snapshot.repeated_char_run >= 12
                or snapshot.emoji_count >= 25
            ),
        )
        current = ShieldSpamEvent(
            timestamp=now,
            channel_id=channel_id,
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
            uppercase_count=snapshot.uppercase_count,
            alpha_count=snapshot.alpha_count,
            low_value_text=snapshot.low_value_text,
            text_substance=text_substance,
            text_relief_weight=text_relief_weight,
            repeated_char_run=snapshot.repeated_char_run,
            is_gif_message=snapshot.is_gif_message,
            gif_signature=snapshot.gif_signature,
            gif_only=snapshot.gif_only,
            gif_low_text=snapshot.gif_low_text,
            gif_pack_exempt=gif_pack_exempt,
            message=message,
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
        message: discord.Message,
        snapshot: ShieldSnapshot,
        *,
        now: float,
        author_kind: str,
        gif_pack_exempt: bool = False,
    ) -> tuple[int, int, tuple[ShieldChannelActivityEvent, ...]]:
        if channel_id is None:
            return 0, 0, ()
        key = (guild_id, channel_id)
        rows = [
            row
            for row in self._recent_channel_activity.get(key, [])
            if now - row.timestamp <= CHANNEL_ACTIVITY_WINDOW_SECONDS
            and not self._message_is_deleted(row.message)
        ]
        risky = bool(
            snapshot.has_links
            or snapshot.has_suspicious_attachment
            or snapshot.invite_codes
            or snapshot.mention_count > 0
            or snapshot.everyone_here_count > 0
            or snapshot.repeated_char_run >= 12
            or snapshot.emoji_count >= 25
        )
        text_substance, text_relief_weight = _classify_gif_text_substance(
            snapshot.context_text,
            plain_word_count=snapshot.plain_word_count,
            low_value_text=snapshot.low_value_text,
            risky=risky,
        )
        quality_message = snapshot.plain_word_count >= 3 and not snapshot.low_value_text
        activity_event = ShieldChannelActivityEvent(
            timestamp=now,
            user_id=user_id,
            author_kind=author_kind,
            plain_word_count=snapshot.plain_word_count,
            low_value_text=snapshot.low_value_text,
            quality_message=quality_message,
            text_substance=text_substance,
            text_relief_weight=text_relief_weight,
            risky=risky,
            token_signature=_short_token_signature(snapshot.context_text),
            exact_fingerprint=snapshot.exact_fingerprint,
            near_fingerprint=snapshot.near_duplicate_fingerprint,
            is_gif_message=snapshot.is_gif_message,
            gif_signature=snapshot.gif_signature,
            gif_only=snapshot.gif_only,
            gif_low_text=snapshot.gif_low_text,
            gif_pack_exempt=gif_pack_exempt,
            message=message,
        )
        rows.append(activity_event)
        if len(rows) > CHANNEL_ACTIVITY_LIMIT:
            rows = rows[-CHANNEL_ACTIVITY_LIMIT:]
        self._recent_channel_activity[key] = rows
        self._update_channel_gif_streak_state(key, activity_event)
        recent_context_rows = [
            row
            for row in rows
            if now - row.timestamp <= CHANNEL_ACTIVITY_CONTEXT_WINDOW_SECONDS
            and not self._message_is_deleted(row.message)
        ]
        distinct_authors = {row.user_id for row in recent_context_rows}
        quality_authors = {row.user_id for row in recent_context_rows if row.quality_message}
        return len(distinct_authors), len(quality_authors), tuple(rows)

    def _update_channel_gif_streak_state(
        self,
        key: tuple[int, int],
        event: ShieldChannelActivityEvent,
    ) -> ShieldChannelGifStreakState:
        existing = self._channel_gif_streaks.get(key)
        rows = [
            row
            for row in (existing.rows if existing is not None else ())
            if not self._message_is_deleted(row.message)
        ]
        capped = bool(existing.capped) if existing is not None else False
        if not event.is_gif_message or event.gif_pack_exempt:
            self._channel_gif_streaks.pop(key, None)
            return ShieldChannelGifStreakState()
        rows.append(event)
        if len(rows) > GIF_STREAK_TRACK_LIMIT:
            rows = rows[-GIF_STREAK_TRACK_LIMIT:]
            capped = True
        state = ShieldChannelGifStreakState(rows=tuple(rows), capped=capped)
        self._channel_gif_streaks[key] = state
        return state

    def _gif_pressure_window_seconds(self, compiled: CompiledShieldConfig) -> float:
        return float(max(GIF_PRESSURE_MIN_WINDOW_SECONDS, compiled.gif_rules.window_seconds * GIF_PRESSURE_WINDOW_MULTIPLIER))

    def _channel_activity_context_rows(
        self,
        channel_activity: Sequence[ShieldChannelActivityEvent],
        *,
        now: float,
    ) -> list[ShieldChannelActivityEvent]:
        return [
            row
            for row in channel_activity
            if now - row.timestamp <= CHANNEL_ACTIVITY_CONTEXT_WINDOW_SECONDS
            and not self._message_is_deleted(row.message)
        ]

    def _select_gif_pressure_rows(
        self,
        channel_activity: Sequence[ShieldChannelActivityEvent],
        compiled: CompiledShieldConfig,
        *,
        now: float,
        user_id: int | None = None,
    ) -> list[ShieldChannelActivityEvent]:
        rows = [
            row
            for row in channel_activity
            if now - row.timestamp <= self._gif_pressure_window_seconds(compiled)
            and not self._message_is_deleted(row.message)
            and (user_id is None or row.user_id == user_id)
        ]
        if len(rows) > GIF_PRESSURE_SLICE_LIMIT:
            rows = rows[-GIF_PRESSURE_SLICE_LIMIT:]
        return rows

    def _effective_gif_text_relief(
        self,
        row: ShieldChannelActivityEvent,
        *,
        repeated_short_signatures: frozenset[str],
    ) -> tuple[str, float]:
        if row.is_gif_message:
            return ("ignored", 0.0)
        if (
            row.text_substance == "substantive"
            and row.plain_word_count <= 4
            and row.token_signature
            and row.token_signature in repeated_short_signatures
        ):
            return ("filler", GIF_FILLER_RELIEF_WEIGHT)
        return (row.text_substance, float(row.text_relief_weight))

    def _gif_pressure_metrics(self, rows: Sequence[ShieldChannelActivityEvent]) -> dict[str, Any]:
        repeated_short_signature_counts: dict[str, int] = {}
        for row in rows:
            if row.is_gif_message or not row.token_signature or row.plain_word_count > 4:
                continue
            repeated_short_signature_counts[row.token_signature] = repeated_short_signature_counts.get(row.token_signature, 0) + 1
        repeated_short_signatures = frozenset(
            signature
            for signature, count in repeated_short_signature_counts.items()
            if count >= 2
        )

        gif_rows = [row for row in rows if row.is_gif_message and not row.gif_pack_exempt]
        substantive_text_count = 0
        filler_text_count = 0
        text_relief_units = 0.0
        for row in rows:
            substance, weight = self._effective_gif_text_relief(
                row,
                repeated_short_signatures=repeated_short_signatures,
            )
            if substance == "substantive":
                substantive_text_count += 1
            elif substance == "filler":
                filler_text_count += 1
            text_relief_units += weight

        signature_counts: dict[str, int] = {}
        for row in gif_rows:
            if row.gif_signature:
                signature_counts[row.gif_signature] = signature_counts.get(row.gif_signature, 0) + 1
        gif_count = len(gif_rows)
        ratio_percent = (
            int(round((gif_count / max(1.0, gif_count + text_relief_units)) * 100))
            if gif_count > 0
            else 0
        )
        return {
            "rows": tuple(rows),
            "gif_rows": tuple(gif_rows),
            "gif_count": gif_count,
            "low_text_gif_count": sum(1 for row in gif_rows if row.gif_low_text),
            "distinct_gif_authors": frozenset(row.user_id for row in gif_rows),
            "substantive_text_count": substantive_text_count,
            "filler_text_count": filler_text_count,
            "text_relief_units": text_relief_units,
            "ratio_percent": ratio_percent,
            "signature_counts": signature_counts,
            "repeated_source": max(signature_counts.values(), default=0),
        }

    def _gif_pressure_triggered(
        self,
        metrics: dict[str, Any],
        compiled: CompiledShieldConfig,
        *,
        minimum_authors: int,
    ) -> bool:
        gif_count = int(metrics.get("gif_count", 0))
        if not compiled.gif_rules.message_enabled and not compiled.gif_rules.repeat_enabled:
            return False
        if gif_count < max(1, compiled.gif_rules.message_threshold):
            return False
        if len(metrics.get("distinct_gif_authors", ())) < minimum_authors:
            return False
        if int(metrics.get("ratio_percent", 0)) < compiled.gif_rules.min_ratio_percent:
            return False
        return (
            (
                compiled.gif_rules.repeat_enabled
                and int(metrics.get("low_text_gif_count", 0)) >= compiled.gif_rules.repeat_threshold
            )
            or (
                compiled.gif_rules.message_enabled
                and gif_count >= compiled.gif_rules.message_threshold + 1
            )
        )

    def _collect_personal_gif_streak_rows(
        self,
        channel_activity: Sequence[ShieldChannelActivityEvent],
        *,
        user_id: int,
    ) -> list[ShieldChannelActivityEvent]:
        streak_rows: list[ShieldChannelActivityEvent] = []
        for row in reversed(channel_activity):
            if self._message_is_deleted(row.message):
                continue
            if row.user_id != user_id or not row.is_gif_message or row.gif_pack_exempt:
                break
            streak_rows.append(row)
        streak_rows.reverse()
        return streak_rows

    def _is_benign_turn_taking_context(
        self,
        snapshot: ShieldSnapshot,
        channel_activity: Sequence[ShieldChannelActivityEvent],
        *,
        author_id: int,
    ) -> bool:
        if (
            not channel_activity
            or snapshot.has_links
            or snapshot.has_suspicious_attachment
            or snapshot.invite_codes
            or snapshot.mention_count > 0
            or snapshot.everyone_here_count > 0
            or snapshot.repeated_char_run >= 12
        ):
            return False
        safe_rows = [row for row in channel_activity if not row.risky]
        if len(safe_rows) < 4:
            return False
        distinct_authors = {row.user_id for row in safe_rows}
        if len(distinct_authors) < 2 or len(distinct_authors) > 4:
            return False
        author_counts: dict[int, int] = {}
        for row in safe_rows:
            author_counts[row.user_id] = author_counts.get(row.user_id, 0) + 1
        if author_counts.get(author_id, 0) < 2:
            return False
        if max(author_counts.values(), default=0) / max(1, len(safe_rows)) > 0.65:
            return False
        transitions = sum(1 for left, right in zip(safe_rows, safe_rows[1:]) if left.user_id != right.user_id)
        if transitions < len(safe_rows) - 2:
            return False
        short_rows = sum(1 for row in safe_rows if row.low_value_text or row.plain_word_count <= 4)
        if short_rows < max(3, len(safe_rows) - 1):
            return False
        bot_turns = any(row.author_kind != "human" for row in safe_rows)
        token_signatures = {row.token_signature for row in safe_rows if row.token_signature}
        if not bot_turns and len(token_signatures) < 3:
            return False
        current_signature = _short_token_signature(snapshot.context_text)
        repeated_current_signature = sum(1 for row in safe_rows if row.token_signature and row.token_signature == current_signature)
        if not bot_turns and repeated_current_signature >= len(safe_rows) - 1:
            return False
        return True

    def _build_personal_gif_pressure(
        self,
        snapshot: ShieldSnapshot,
        recent_events: Sequence[ShieldSpamEvent],
        channel_activity: Sequence[ShieldChannelActivityEvent],
        compiled: CompiledShieldConfig,
        *,
        now: float,
        author_id: int,
        channel_id: int | None,
        gif_pack_exempt: bool = False,
    ) -> dict[str, Any] | None:
        if channel_id is None or author_id <= 0 or not snapshot.is_gif_message or gif_pack_exempt:
            return None

        same_channel_gif_events = [
            event
            for event in recent_events
            if event.message is not None
            and event.channel_id == channel_id
            and event.is_gif_message
            and not event.gif_pack_exempt
            and now - event.timestamp <= float(compiled.gif_rules.window_seconds)
            and not self._message_is_deleted(event.message)
        ]
        same_asset_messages: tuple[discord.Message, ...] = ()
        same_asset_count = 0
        if snapshot.gif_signature is not None and compiled.gif_rules.same_asset_enabled:
            same_asset_events = [event for event in same_channel_gif_events if event.gif_signature == snapshot.gif_signature]
            same_asset_count = len(same_asset_events)
            if same_asset_count >= compiled.gif_rules.same_asset_threshold:
                same_asset_messages = tuple(
                    self._dedupe_message_targets(
                        [
                            event.message
                            for event in same_asset_events[-compiled.gif_rules.same_asset_threshold:]
                            if event.message is not None
                        ]
                    )
                )

        streak_rows = self._collect_personal_gif_streak_rows(channel_activity, user_id=author_id)
        pressure_rows = self._select_gif_pressure_rows(
            channel_activity,
            compiled,
            now=now,
            user_id=author_id,
        )
        metrics = self._gif_pressure_metrics(pressure_rows)
        streak_trigger = compiled.gif_rules.consecutive_enabled and len(streak_rows) >= compiled.gif_rules.consecutive_threshold
        pressure_trigger = self._gif_pressure_triggered(metrics, compiled, minimum_authors=1)
        if not same_asset_messages and not streak_trigger and not pressure_trigger:
            return None

        if same_asset_messages:
            trigger_mode = "same asset"
            triggered_by = ("same_asset",)
            summary = (
                f"Trigger mode: {trigger_mode}. The same GIF asset was repeated {same_asset_count} times inside "
                f"{compiled.gif_rules.window_seconds} seconds (threshold {compiled.gif_rules.same_asset_threshold})."
            )
        else:
            trigger_labels = [
                label
                for label, enabled in (("personal streak", streak_trigger), ("personal pressure", pressure_trigger))
                if enabled
            ]
            trigger_mode = " + ".join(trigger_labels)
            triggered_by = tuple("streak" if label == "personal streak" else "pressure" for label in trigger_labels)
            trigger_bits: list[str] = []
            if streak_trigger:
                trigger_bits.append(
                    f"{len(streak_rows)} consecutive GIF-heavy messages crossed the {compiled.gif_rules.consecutive_threshold}-message personal streak threshold"
                )
            if pressure_trigger:
                trigger_bits.append(
                    f"{metrics['gif_count']} GIFs vs {metrics['substantive_text_count']} substantive and "
                    f"{metrics['filler_text_count']} filler text messages in the active personal pressure slice "
                    f"({metrics['ratio_percent']}% effective GIF pressure, threshold {compiled.gif_rules.min_ratio_percent}%)"
                )
            summary = f"Trigger mode: {trigger_mode}. {' and '.join(trigger_bits)}."

        top_signatures = sorted(metrics["signature_counts"].items(), key=lambda item: (-item[1], item[0]))[:3]
        signature_seed = "|".join(
            [
                trigger_mode,
                str(author_id),
                str(metrics["gif_count"]),
                str(metrics["ratio_percent"]),
                str(len(streak_rows)),
                str(same_asset_count),
                ",".join(f"{signature}:{count}" for signature, count in top_signatures),
            ]
        )
        return {
            "reason": summary,
            "trigger_mode": trigger_mode,
            "triggered_by": triggered_by,
            "gif_posts": int(metrics["gif_count"]),
            "substantive_text_posts": int(metrics["substantive_text_count"]),
            "filler_text_posts": int(metrics["filler_text_count"]),
            "text_relief_units": float(metrics["text_relief_units"]),
            "ratio_percent": int(metrics["ratio_percent"]),
            "streak_rows": tuple(streak_rows),
            "pressure_rows": tuple(pressure_rows),
            "same_asset_messages": same_asset_messages,
            "same_asset_count": same_asset_count,
            "signature": hashlib.sha1(f"personal_gif|{channel_id}|{signature_seed}".encode("utf-8")).hexdigest(),
        }

    def _build_group_gif_pressure(
        self,
        guild_id: int,
        channel_id: int | None,
        snapshot: ShieldSnapshot,
        *,
        compiled: CompiledShieldConfig,
        now: float,
        gif_pack_exempt: bool = False,
    ) -> dict[str, Any] | None:
        if channel_id is None or not snapshot.is_gif_message or gif_pack_exempt:
            return None
        key = (guild_id, channel_id)
        streak_state = self._channel_gif_streaks.get(key, ShieldChannelGifStreakState())
        consecutive_rows = [
            row
            for row in streak_state.rows
            if not self._message_is_deleted(row.message)
        ]
        consecutive_gif_count = len(consecutive_rows)
        consecutive_authors = {row.user_id for row in consecutive_rows}
        pressure_rows = self._select_gif_pressure_rows(
            self._recent_channel_activity.get(key, []),
            compiled,
            now=now,
        )
        if not pressure_rows:
            return None
        metrics = self._gif_pressure_metrics(pressure_rows)
        if not metrics["gif_rows"]:
            return None
        streak_trigger = (
            compiled.gif_rules.consecutive_enabled
            and consecutive_gif_count >= compiled.gif_rules.consecutive_threshold
            and len(consecutive_authors) >= 2
        )
        pressure_trigger = self._gif_pressure_triggered(metrics, compiled, minimum_authors=2)
        pure_collective_gif_run = bool(pressure_rows) and all(
            row.is_gif_message and not row.gif_pack_exempt
            for row in pressure_rows
        )
        # Let the exact live-streak lane own uninterrupted multi-member GIF runs.
        # Pressure is for GIF-dominated conversation slices that still contain some text.
        if pure_collective_gif_run:
            pressure_trigger = False
        if not streak_trigger and not pressure_trigger:
            return None

        collective_author_count = max(len(metrics["distinct_gif_authors"]), len(consecutive_authors))
        trigger_bits: list[str] = []
        if streak_trigger:
            trigger_bits.append(
                f"{consecutive_gif_count} consecutive GIF-heavy messages from {len(consecutive_authors)} members crossed "
                f"the {compiled.gif_rules.consecutive_threshold}-message live streak threshold"
            )
        if pressure_trigger:
            trigger_bits.append(
                f"{metrics['gif_count']} GIFs from {collective_author_count} members vs {metrics['substantive_text_count']} substantive and "
                f"{metrics['filler_text_count']} filler text messages in the active channel pressure slice "
                f"({metrics['ratio_percent']}% effective GIF pressure, threshold {compiled.gif_rules.min_ratio_percent}%)"
            )
        trigger_label = " + ".join(
            trigger
            for trigger, enabled in (("collective streak", streak_trigger), ("collective pressure", pressure_trigger))
            if enabled
        )
        summary = f"Trigger mode: {trigger_label}. {' and '.join(trigger_bits)}."
        if metrics["repeated_source"] >= max(2, compiled.gif_rules.same_asset_threshold):
            summary = (
                f"{summary} The same GIF source repeated {metrics['repeated_source']} times in the active pressure slice."
            )
        if streak_trigger and streak_state.capped:
            summary = (
                f"{summary} The live streak tracker hit its {GIF_STREAK_TRACK_LIMIT}-message cap, so collective streak cleanup stays capped to the newest tracked GIFs."
            )

        top_signatures = sorted(metrics["signature_counts"].items(), key=lambda item: (-item[1], item[0]))[:3]
        signature_seed = "|".join(
            [
                trigger_label,
                str(consecutive_gif_count),
                str(metrics["gif_count"]),
                str(metrics["ratio_percent"]),
                ",".join(str(author_id) for author_id in sorted(metrics["distinct_gif_authors"])[:4]),
                ",".join(f"{signature}:{count}" for signature, count in top_signatures),
            ]
        )
        return {
            "reason": summary,
            "gif_posts": int(metrics["gif_count"]),
            "substantive_text_posts": int(metrics["substantive_text_count"]),
            "filler_text_posts": int(metrics["filler_text_count"]),
            "text_relief_units": float(metrics["text_relief_units"]),
            "ratio_percent": int(metrics["ratio_percent"]),
            "distinct_authors": collective_author_count,
            "consecutive_gif_posts": consecutive_gif_count,
            "consecutive_authors": len(consecutive_authors),
            "repeated_source": int(metrics["repeated_source"]),
            "triggered_by": tuple(
                trigger
                for trigger, enabled in (("streak", streak_trigger), ("pressure", pressure_trigger))
                if enabled
            ),
            "trigger_label": trigger_label,
            "streak_rows": tuple(consecutive_rows),
            "streak_capped": bool(streak_state.capped),
            "pressure_rows": tuple(pressure_rows),
            "window_seconds": int(self._gif_pressure_window_seconds(compiled)),
            "signature": hashlib.sha1(f"group_gif|{channel_id}|{signature_seed}".encode("utf-8")).hexdigest(),
        }

    def _detect_spam(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        recent_events: Sequence[ShieldSpamEvent],
        *,
        now: float,
        scam_context: ShieldScamContext,
        author_kind: str,
        author_id: int = 0,
        distinct_channel_authors: int = 0,
        quality_channel_authors: int = 0,
        channel_activity: Sequence[ShieldChannelActivityEvent] = (),
        personal_gif_pressure: dict[str, Any] | None = None,
        group_gif_pressure: dict[str, Any] | None = None,
    ) -> list[ShieldMatch]:
        spam_settings = compiled.spam
        gif_settings = compiled.gif
        if author_kind != "human" or (not spam_settings.enabled and not gif_settings.enabled):
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
        spam_rules = compiled.spam_rules
        gif_rules = compiled.gif_rules
        channel_context_rows = self._channel_activity_context_rows(channel_activity, now=now)
        benign_turn_taking = self._is_benign_turn_taking_context(
            snapshot,
            channel_context_rows,
            author_id=author_id,
        )
        channel_low_value_events = [row for row in channel_context_rows if row.low_value_text]
        current_author_message_count = sum(1 for row in channel_context_rows if row.user_id == author_id)
        current_author_low_value_count = sum(1 for row in channel_low_value_events if row.user_id == author_id)
        dominant_channel_presence = (
            distinct_channel_authors <= 2
            or current_author_message_count >= max(4, (len(channel_context_rows) // 2) + 1)
            or current_author_low_value_count >= max(4, len(channel_low_value_events) - 1)
        )
        burst_window = [
            event
            for event in recent_events
            if now - event.timestamp <= float(spam_rules.burst_window_seconds)
        ]
        message_window = [
            event
            for event in recent_events
            if now - event.timestamp <= float(spam_rules.message_window_seconds)
        ]
        exact_window = [
            event
            for event in message_window
            if snapshot.exact_fingerprint is not None and event.exact_fingerprint == snapshot.exact_fingerprint
        ]
        exact_threshold = max(3, spam_rules.near_duplicate_threshold - 1)
        if (
            spam_settings.enabled
            and spam_rules.near_duplicate_enabled
            and len(exact_window) >= exact_threshold
            and not benign_turn_taking
        ):
            facts.append(
                {
                    "match_class": "spam_duplicate",
                    "label": "Repeated duplicate spam",
                    "reason": (
                        f"The same message was posted {len(exact_window)} times inside {spam_rules.message_window_seconds} seconds "
                        f"(duplicate trigger starts at {exact_threshold})."
                    ),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.exact_fingerprint,
                }
            )

        near_window = [
            event
            for event in recent_events
            if now - event.timestamp <= float(spam_rules.near_duplicate_window_seconds)
        ]
        near_quality_count = sum(1 for event in near_window if event.plain_word_count >= 3 and not event.low_value_text)
        near_average_plain_words = (
            sum(event.plain_word_count for event in near_window) / max(1, len(near_window))
            if near_window
            else 0.0
        )
        low_substance_near_run = near_average_plain_words <= 6 or near_quality_count < max(1, len(near_window) - 1)
        near_hits = 0
        near_duplicate_context = bool(
            snapshot.low_value_text
            or snapshot.has_links
            or snapshot.invite_codes
            or snapshot.mention_count > 0
            or snapshot.emoji_count >= 10
            or snapshot.repeated_char_run >= 12
            or (dominant_channel_presence and low_substance_near_run)
        )
        if snapshot.near_duplicate_text:
            for event in near_window:
                if not event.near_text:
                    continue
                if difflib.SequenceMatcher(None, snapshot.near_duplicate_text, event.near_text).ratio() >= 0.88:
                    near_hits += 1
        if (
            spam_settings.enabled
            and spam_rules.near_duplicate_enabled
            and near_hits >= spam_rules.near_duplicate_threshold
            and near_duplicate_context
            and not benign_turn_taking
        ):
            facts.append(
                {
                    "match_class": "spam_near_duplicate",
                    "label": "Repeated near-duplicate spam",
                    "reason": (
                        f"Near-identical variants were posted {near_hits} times inside {spam_rules.near_duplicate_window_seconds} seconds "
                        f"(threshold {spam_rules.near_duplicate_threshold})."
                    ),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": snapshot.near_duplicate_fingerprint,
                }
            )

        if (
            spam_settings.enabled
            and spam_rules.burst_enabled
            and len(burst_window) >= spam_rules.burst_threshold
            and dominant_channel_presence
            and not benign_turn_taking
        ):
            burst_quality_count = sum(1 for event in burst_window if event.plain_word_count >= 3 and not event.low_value_text)
            pure_media_burst = bool(burst_window) and all(event.media_only_links for event in burst_window)
            burst_high_signal_count = sum(
                1
                for event in burst_window
                if event.has_links
                or bool(event.invite_codes)
                or event.mention_count > 0
                or event.everyone_here_count > 0
                or event.emoji_count >= max(10, spam_rules.emote_threshold)
                or event.repeated_char_run >= 12
                or event.low_value_text
            )
            burst_average_plain_words = sum(event.plain_word_count for event in burst_window) / max(1, len(burst_window))
            pure_substantive_burst = (
                len(burst_window) <= spam_rules.burst_threshold + 3
                and burst_quality_count >= len(burst_window) - 1
                and burst_average_plain_words >= 6
                and burst_high_signal_count == 0
            )
            if not pure_substantive_burst and not pure_media_burst:
                facts.append(
                    {
                        "match_class": "spam_burst",
                        "label": "Fast burst posting",
                        "reason": (
                            f"{len(burst_window)} messages landed inside {spam_rules.burst_window_seconds} seconds "
                            f"(threshold {spam_rules.burst_threshold})."
                        ),
                        "base_confidence": "medium" if burst_high_signal_count >= 1 or len(burst_window) >= spam_rules.burst_threshold + 2 else "low",
                        "strong": burst_high_signal_count >= 1,
                        "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                    }
                )

        if (
            spam_settings.enabled
            and spam_rules.message_enabled
            and len(message_window) >= spam_rules.message_threshold
            and dominant_channel_presence
            and not benign_turn_taking
        ):
            quality_message_count = sum(1 for event in message_window if event.plain_word_count >= 3 and not event.low_value_text)
            pure_media_window = bool(message_window) and all(event.media_only_links for event in message_window)
            risky_window_count = sum(
                1
                for event in message_window
                if event.has_links
                or bool(event.invite_codes)
                or event.mention_count > 0
                or event.everyone_here_count > 0
                or event.repeated_char_run >= 12
                or event.emoji_count >= 25
            )
            high_signal_count = sum(
                1
                for event in message_window
                if event.has_links
                or bool(event.invite_codes)
                or event.mention_count > 0
                or event.emoji_count >= 10
                or event.low_value_text
                or event.repeated_char_run >= 12
            )
            average_plain_word_count = sum(event.plain_word_count for event in message_window) / max(1, len(message_window))
            pure_substantive_burst = (
                len(message_window) < spam_rules.message_threshold + 2
                and quality_message_count >= len(message_window) - 1
                and average_plain_word_count >= 6
                and high_signal_count == 0
                and risky_window_count == 0
            )
            if not pure_substantive_burst and not pure_media_window:
                facts.append(
                    {
                        "match_class": "spam_message_rate",
                        "label": "Message-rate spam",
                        "reason": (
                            f"{len(message_window)} messages landed inside {spam_rules.message_window_seconds} seconds "
                            f"(threshold {spam_rules.message_threshold})."
                        ),
                        "base_confidence": "medium" if (high_signal_count >= 2 or risky_window_count >= 2) else "low",
                        "strong": high_signal_count >= 1 or risky_window_count >= 1,
                        "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                    }
                )

        link_events = [event for event in recent_events if event.has_links and now - event.timestamp <= SPAM_LINK_WINDOW_SECONDS]
        link_threshold = _sensitivity_threshold(spam_settings.sensitivity, low=5, normal=4, high=3)
        if spam_settings.enabled and link_events:
            if all(event.media_only_links for event in link_events):
                link_threshold = len(link_events) + 1
            distinct_link_signatures = {event.link_signature for event in link_events if event.link_signature and not event.invite_codes}
            diversified_link_pressure = len(distinct_link_signatures) >= 2
            if len(link_events) >= link_threshold and diversified_link_pressure:
                facts.append(
                    {
                        "match_class": "spam_link_flood",
                        "label": "Repeated link flood",
                        "reason": f"{len(link_events)} link drops landed inside 45 seconds (threshold {link_threshold}).",
                        "base_confidence": "medium",
                        "strong": True,
                        "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint or "|".join(sorted(snapshot.canonical_links)[:3]),
                    }
                )

        invite_events = [event for event in recent_events if event.invite_codes and now - event.timestamp <= SPAM_INVITE_WINDOW_SECONDS]
        invite_threshold = _sensitivity_threshold(spam_settings.sensitivity, low=4, normal=3, high=3)
        distinct_invites = sorted({code for event in invite_events for code in event.invite_codes})
        if spam_settings.enabled and (len(invite_events) >= invite_threshold or (len(distinct_invites) >= 2 and len(recent_events) >= 4)):
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
        if spam_settings.enabled and (snapshot.mention_count >= single_mention_threshold or recent_mention_sum >= 12):
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

        if (
            spam_settings.enabled
            and spam_rules.emote_enabled
            and snapshot.emoji_count >= spam_rules.emote_threshold
        ):
            repeated_emote_events = sum(
                1
                for event in burst_window
                if event.emoji_count >= max(8, spam_rules.emote_threshold - 4)
            )
            sparse_emote_message = snapshot.plain_word_count <= 6 or snapshot.low_value_text
            repeated_emote_pressure = repeated_emote_events >= 2
            extreme_low_substance_emote = snapshot.plain_word_count <= 12 and snapshot.emoji_count >= spam_rules.emote_threshold + 16
            if sparse_emote_message or repeated_emote_pressure or extreme_low_substance_emote:
                reason_suffix = (
                    "with repeated emote-heavy pressure."
                    if repeated_emote_pressure and not sparse_emote_message
                    else "with very little plain text."
                )
                facts.append(
                    {
                        "match_class": "spam_emoji_flood",
                        "label": "Emote spam",
                        "reason": (
                            f"The message packed {snapshot.emoji_count} emoji or emote tokens "
                            f"(threshold {spam_rules.emote_threshold}) {reason_suffix}"
                        ),
                        "base_confidence": "medium" if repeated_emote_pressure or extreme_low_substance_emote else "low",
                        "strong": repeated_emote_pressure or extreme_low_substance_emote,
                        "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                    }
                )

        caps_ratio = snapshot.uppercase_count / max(1, snapshot.alpha_count)
        if (
            spam_settings.enabled
            and spam_rules.caps_enabled
            and snapshot.uppercase_count >= spam_rules.caps_threshold
            and snapshot.alpha_count >= spam_rules.caps_threshold + 6
            and caps_ratio >= 0.72
            and snapshot.plain_word_count >= 4
        ):
            repeated_caps_events = sum(
                1
                for event in burst_window
                if event.uppercase_count >= spam_rules.caps_threshold
                and event.alpha_count >= spam_rules.caps_threshold + 6
                and (event.uppercase_count / max(1, event.alpha_count)) >= 0.72
            )
            facts.append(
                {
                    "match_class": "spam_caps_flood",
                    "label": "Excessive capitals",
                    "reason": (
                        f"{snapshot.uppercase_count} uppercase letters dominated the message "
                        f"({int(round(caps_ratio * 100))}% of letters, threshold {spam_rules.caps_threshold})."
                    ),
                    "base_confidence": "medium" if repeated_caps_events >= 2 else "low",
                    "strong": repeated_caps_events >= 2,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )

        if gif_settings.enabled and personal_gif_pressure is not None:
            facts.append(
                {
                    "match_class": "spam_gif_flood",
                    "label": "GIF flood",
                    "reason": str(personal_gif_pressure["reason"]),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": personal_gif_pressure.get("signature") or snapshot.gif_signature or snapshot.near_duplicate_fingerprint,
                    "pack": "gif",
                    "settings": compiled.gif,
                }
            )
        if gif_settings.enabled and group_gif_pressure is not None:
            facts.append(
                {
                    "match_class": "spam_group_gif_pressure",
                    "label": "Coordinated GIF pressure",
                    "reason": str(group_gif_pressure["reason"]),
                    "base_confidence": "medium",
                    "strong": True,
                    "signature": group_gif_pressure.get("signature"),
                    "pack": "gif",
                    "settings": compiled.gif,
                }
            )

        low_value_events = [
            event for event in recent_events if event.low_value_text and now - event.timestamp <= float(spam_rules.low_value_window_seconds)
        ]
        if (
            spam_settings.enabled
            and spam_rules.low_value_enabled
            and len(low_value_events) >= spam_rules.low_value_threshold
            and dominant_channel_presence
            and not benign_turn_taking
            and not any(fact["match_class"] in {"spam_message_rate", "spam_duplicate", "spam_near_duplicate"} for fact in facts)
        ):
            facts.append(
                {
                    "match_class": "spam_low_value_noise",
                    "label": "Repeated low-value noise",
                    "reason": (
                        f"{len(low_value_events)} short low-value messages landed inside "
                        f"{spam_rules.low_value_window_seconds} seconds."
                    ),
                    "base_confidence": "low",
                    "strong": False,
                    "signature": snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint,
                }
            )
        if (
            spam_settings.enabled
            and (spam_rules.message_enabled or spam_rules.burst_enabled or spam_rules.near_duplicate_enabled)
            and snapshot.repeated_char_run >= 12
            and not benign_turn_taking
            and (len(message_window) >= max(3, spam_rules.message_threshold - 2) or len(exact_window) >= 2 or near_hits >= 3)
        ):
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
            if healthy_chat and fact["match_class"] in {"spam_message_rate", "spam_burst", "spam_emoji_flood", "spam_gif_flood", "spam_low_value_noise"}:
                if confidence == "high":
                    confidence = "medium"
                elif confidence == "medium":
                    confidence = "low"
            if benign_turn_taking and fact["match_class"] in {"spam_message_rate", "spam_burst", "spam_low_value_noise"}:
                if confidence == "high":
                    confidence = "medium"
                elif confidence == "medium":
                    confidence = "low"
            pack = str(fact.get("pack", "spam"))
            fact_settings = fact.get("settings")
            resolved_settings = fact_settings if isinstance(fact_settings, PackSettings) else compiled.pack_settings(pack)
            match = self._make_pack_match(
                pack=pack,
                settings=resolved_settings,
                label=fact["label"],
                reason=fact["reason"],
                confidence=confidence,
                heuristic=True,
                match_class=fact["match_class"],
            )
            if match.match_class == "spam_group_gif_pressure":
                match = self._cap_group_gif_action(match)
            matches.append(match)
        return self._dedupe_matches(matches)

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
        channel_activity: tuple[ShieldChannelActivityEvent, ...] = ()
        gif_pack_exempt = self._message_pack_exempt(compiled, message, "gif")
        if scan_source == "new_message":
            distinct_channel_authors, quality_channel_authors, channel_activity = self._track_channel_activity(
                message.guild.id,
                channel_id,
                int(getattr(message.author, "id", 0) or 0),
                message,
                snapshot,
                now=now,
                author_kind=author_kind,
                gif_pack_exempt=gif_pack_exempt,
            )
        if author_kind == "human" and scan_source == "new_message":
            _spam_event, recent_spam_events = self._track_spam_event(
                message.guild.id,
                int(getattr(message.author, "id", 0) or 0),
                channel_id,
                message,
                snapshot,
                now=now,
                gif_pack_exempt=gif_pack_exempt,
            )
        link_assessments = self._collect_link_assessments(compiled, snapshot, now=now)
        scam_context = self._build_scam_context(
            message,
            snapshot,
            link_assessments,
            now=now,
            scan_source=scan_source,
        )
        personal_gif_pressure = self._build_personal_gif_pressure(
            snapshot,
            recent_spam_events,
            channel_activity,
            compiled,
            now=now,
            author_id=int(getattr(message.author, "id", 0) or 0),
            channel_id=channel_id,
            gif_pack_exempt=gif_pack_exempt,
        )
        group_gif_pressure = self._build_group_gif_pressure(
            message.guild.id,
            channel_id,
            snapshot,
            compiled=compiled,
            now=now,
            gif_pack_exempt=gif_pack_exempt,
        )
        matches = self._collect_matches(
            compiled,
            snapshot,
            repetitive_promo=repetition,
            link_assessments=link_assessments,
            scan_source=scan_source,
            scam_context=scam_context,
            recent_spam_events=recent_spam_events,
            author_kind=author_kind,
            author_id=int(getattr(message.author, "id", 0) or 0),
            now=now,
            channel_id=channel_id,
            distinct_channel_authors=distinct_channel_authors,
            quality_channel_authors=quality_channel_authors,
            channel_activity=channel_activity,
            personal_gif_pressure=personal_gif_pressure,
            group_gif_pressure=group_gif_pressure,
        )
        matches, _ = self._apply_allow_phrase_suppression(matches, allow_phrase=self._matching_allow_phrase(compiled, snapshot))
        matches = self._apply_pack_exemptions(compiled, message, matches)
        matches = self._apply_spam_moderator_policy(compiled, getattr(message, "author", None), matches)
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
        gif_plan = None
        if best.pack == "gif":
            gif_plan = self._build_gif_incident_plan(
                matches,
                best,
                message,
                compiled,
                now=now,
                channel_activity=channel_activity,
                personal_gif_pressure=personal_gif_pressure,
                group_gif_pressure=group_gif_pressure,
            )
            if gif_plan is not None:
                best = gif_plan.primary_match
        decision = ShieldDecision(
            matched=True,
            action=best.action,
            pack=best.pack,
            reasons=tuple(matches[:3]),
            link_assessments=link_assessments,
            link_explanations=self._build_link_decision_explanations(compiled, snapshot, link_assessments, matches),
            scan_source=scan_source,
            scan_surface_labels=snapshot.surface_labels,
        )
        if best.match_class == "repetitive_link_noise" and repetition.fingerprint is not None:
            decision.alert_evidence_signature = repetition.fingerprint
            decision.alert_evidence_summary = self._repetition_reason(repetition)
        elif best.pack in {"spam", "gif"}:
            if gif_plan is not None:
                decision.alert_evidence_signature = gif_plan.alert_evidence_signature
                decision.alert_evidence_summary = gif_plan.alert_evidence_summary
            else:
                decision.alert_evidence_signature = (
                    snapshot.gif_signature or snapshot.near_duplicate_fingerprint or snapshot.exact_fingerprint
                )
                if best.match_class == "spam_duplicate":
                    decision.alert_evidence_summary = f"Repeated duplicate spam in a short window ({best.reason.lower()})"
                elif best.match_class == "spam_near_duplicate":
                    decision.alert_evidence_summary = best.reason
                elif best.match_class in {
                    "spam_message_rate",
                    "spam_burst",
                    "spam_low_value_noise",
                    "spam_padding_noise",
                    "spam_gif_flood",
                    "spam_group_gif_pressure",
                    "spam_emoji_flood",
                    "spam_caps_flood",
                }:
                    decision.alert_evidence_summary = best.reason

        moderator_spam_policy_note = None
        if best.pack == "spam" and self._member_is_moderator(getattr(message, "author", None)):
            if compiled.spam_rules.moderator_policy == "delete_only" and best.action == "delete_log":
                moderator_spam_policy_note = "Moderator anti-spam policy capped this incident at delete + log."
        gif_group_note = gif_plan.action_note if gif_plan is not None else None

        delete_targets: Sequence[discord.Message] = ()
        if gif_plan is not None:
            delete_targets = gif_plan.delete_targets
        elif best.action.startswith("delete"):
            delete_targets = self._collect_incident_messages(
                best,
                message,
                snapshot,
                recent_spam_events,
                compiled,
                now=now,
                channel_activity=channel_activity,
                group_gif_pressure=group_gif_pressure,
            )
        if delete_targets:
            deleted_count, attempt_count = await self._delete_messages(delete_targets)
            decision.deleted = deleted_count > 0
            decision.deleted_count = deleted_count
            decision.delete_attempt_count = attempt_count
            if deleted_count <= 0:
                if attempt_count <= 0:
                    decision.action_note = "Delete was configured, but no live messages remained inside the matched incident."
                else:
                    decision.action_note = (
                        "Delete was configured, but Babblebox could not delete the message."
                        if attempt_count <= 1
                        else f"Delete was configured, but Babblebox could not delete the {attempt_count}-message incident burst."
                    )
            elif deleted_count < attempt_count:
                decision.action_note = f"Deleted {deleted_count} of {attempt_count} incident messages; some could not be removed."
            elif attempt_count > 1:
                decision.action_note = f"Deleted the full {attempt_count}-message incident burst."

        timeout_match = gif_plan.personal_match if gif_plan is not None and gif_plan.personal_match is not None else best
        if timeout_match.action in {"timeout_log", "delete_timeout_log"}:
            decision.timed_out = await self._timeout_member(
                message,
                compiled,
                pack=timeout_match.pack,
                reason=f"Babblebox Shield matched {PACK_LABELS.get(timeout_match.pack, 'Shield')}.",
            )
            if not decision.timed_out:
                decision.action_note = "Timeout was configured, but Babblebox could not time out that member."

        escalation_match = gif_plan.personal_match if gif_plan is not None and gif_plan.personal_match is not None else best
        if self._is_escalation_eligible(compiled, escalation_match):
            strike_count = self._record_strike(message.guild.id, message.author.id, escalation_match.pack, compiled, now)
            if strike_count >= compiled.escalation_threshold:
                decision.timed_out = await self._timeout_member(
                    message,
                    compiled,
                    pack=escalation_match.pack,
                    reason=f"Babblebox Shield escalation after repeated {PACK_LABELS.get(escalation_match.pack, 'Shield').lower()} hits.",
                )
                decision.escalated = decision.timed_out
                if decision.timed_out:
                    decision.action_note = (
                        f"Repeated-hit escalation triggered after {strike_count} strikes in {compiled.escalation_window_minutes} minutes."
                    )
                elif decision.action_note is None:
                    decision.action_note = "Repeated-hit escalation was configured, but Babblebox could not time out that member."
        elif best.action == "delete_escalate":
            decision.action_note = "Repeated-hit escalation is reserved for actionable medium/high-confidence spam, GIF, and scam patterns."
        if moderator_spam_policy_note is not None and decision.action_note is None:
            decision.action_note = moderator_spam_policy_note
        if gif_group_note is not None:
            if decision.action_note:
                decision.action_note = f"{decision.action_note} {gif_group_note}"
            else:
                decision.action_note = gif_group_note

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
            await self._send_alert(message, compiled, decision, content_fingerprint=alert_content_fingerprint, snapshot=snapshot)

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
        domain_has_visible_link: dict[str, bool] = {}
        for link in snapshot.links:
            domain_has_visible_link[link.domain] = domain_has_visible_link.get(link.domain, False) or not link.preview_only
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
            assessment = _assessment_with_source_signals(assessment, source=link.source, preview_only=link.preview_only)
            by_domain[link.domain] = merge_link_assessments(by_domain.get(link.domain), assessment)
        cleaned: dict[str, ShieldLinkAssessment] = {}
        for domain, assessment in by_domain.items():
            if domain_has_visible_link.get(domain, False) and PREVIEW_ONLY_LINK_SIGNAL in assessment.matched_signals:
                signals = tuple(
                    signal
                    for signal in assessment.matched_signals
                    if signal != PREVIEW_ONLY_LINK_SIGNAL and not signal.startswith(SOURCE_SIGNAL_PREFIX)
                )
                assessment = replace(assessment, matched_signals=signals)
            cleaned[domain] = assessment
        return tuple(sorted(cleaned.values(), key=lambda item: item.normalized_domain))

    def _build_link_decision_explanations(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        link_assessments: Sequence[ShieldLinkAssessment],
        matches: Sequence[ShieldMatch],
    ) -> tuple[ShieldLinkDecisionExplanation, ...]:
        links_by_domain: dict[str, ShieldLink] = {}
        for link in snapshot.links:
            links_by_domain.setdefault(link.domain, link)

        explanations: list[ShieldLinkDecisionExplanation] = []
        for assessment in sorted(link_assessments, key=lambda item: item.normalized_domain):
            explanations.append(
                self._explain_link_decision(
                    compiled,
                    snapshot,
                    assessment,
                    link=links_by_domain.get(assessment.normalized_domain),
                    matches=matches,
                )
            )
        for candidate in snapshot.ignored_link_candidates:
            explanations.append(
                ShieldLinkDecisionExplanation(
                    domain=candidate,
                    disposition="Ignored",
                    reason="Bare IDNA-looking text needs an explicit URL shape before Shield treats it as a link.",
                )
            )
        return tuple(explanations)

    def _explain_link_decision(
        self,
        compiled: CompiledShieldConfig,
        snapshot: ShieldSnapshot,
        assessment: ShieldLinkAssessment,
        *,
        link: ShieldLink | None,
        matches: Sequence[ShieldMatch],
    ) -> ShieldLinkDecisionExplanation:
        domain = assessment.normalized_domain
        match_packs = {match.pack for match in matches}
        warning_context = _looks_like_scam_warning(snapshot.context_text)
        policy_blocked = (
            link is not None
            and compiled.link_policy.enabled
            and compiled.link_policy_mode != DEFAULT_SHIELD_LINK_POLICY_MODE
            and "link_policy" in match_packs
            and not bool({"scam", "adult"}.intersection(match_packs))
            and not link.preview_only
            and not self._link_is_trusted_under_policy(compiled, link, assessment)
        )
        if policy_blocked:
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Blocked",
                reason="Trusted Links Only blocked this untrusted destination unless admins explicitly allow it.",
            )
        if _assessment_is_preview_only_advisory(assessment):
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Suppressed",
                reason="Preview metadata stayed advisory behind the visible message context.",
            )
        if assessment.category == MALICIOUS_LINK_CATEGORY:
            if warning_context:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Suppressed",
                    reason="warning or moderation context kept local malicious-link intel from enforcing.",
                )
            if "scam" in match_packs:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Flagged",
                    reason="Local malicious-domain intel matched the Scam / Malicious Links pack.",
                )
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Review-only",
                reason="Local malicious-domain intel matched, but the scam pack did not enforce in this dry run.",
            )
        if assessment.category == IMPERSONATION_LINK_CATEGORY:
            if warning_context:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Suppressed",
                    reason="warning or moderation context kept trusted-brand lookalike intel from enforcing.",
                )
            if "scam" in match_packs:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Flagged",
                    reason="Hard local trusted-brand lookalike intel matched the scam pack.",
                )
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Review-only",
                reason="Trusted-brand lookalike intel matched, but the scam pack did not enforce in this dry run.",
            )
        if assessment.category == ADULT_LINK_CATEGORY:
            if "adult" in match_packs:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Flagged",
                    reason="Local adult-domain intel matched the Adult Links + Solicitation pack.",
                )
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Review-only",
                reason="Local adult-domain intel matched, but the adult pack did not enforce in this dry run.",
            )
        if assessment.category == UNKNOWN_SUSPICIOUS_LINK_CATEGORY:
            if "scam" in match_packs:
                return ShieldLinkDecisionExplanation(
                    domain=domain,
                    disposition="Flagged",
                    reason="Suspicious link shape combined with scam intent or corroborating context.",
                )
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Review-only",
                reason="Suspicious link shape stayed review-only because there was no scam intent, hard local intel, or fresh-campaign corroboration.",
            )
        if "guild_allow_domain" in assessment.matched_signals:
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Allowed",
                reason="admin allowlist applies here; hard-risk intel would still override it.",
            )
        if assessment.category == SAFE_LINK_CATEGORY:
            family = f" `{assessment.safe_family}`" if assessment.safe_family else ""
            return ShieldLinkDecisionExplanation(
                domain=domain,
                disposition="Allowed",
                reason=f"Built-in trusted/safe family{family} matched.",
            )
        return ShieldLinkDecisionExplanation(
            domain=domain,
            disposition="Allowed",
            reason="No risky link intel matched.",
        )

    def _primary_risky_assessment(self, link_assessments: Sequence[ShieldLinkAssessment]) -> ShieldLinkAssessment | None:
        risky = [
            assessment
            for assessment in link_assessments
            if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
            and not _assessment_is_preview_only_advisory(assessment)
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
        if newcomer_early_message and scan_source == "new_message":
            signatures: list[tuple[str, str]] = []
            if primary_domain is not None:
                signatures.append(("domain", primary_domain))
                path_shape_signature = _path_query_shape_signature(primary_assessment, domain=primary_domain)
                if path_shape_signature is not None:
                    signatures.append(("path_shape", path_shape_signature))
                host_family_signature = _host_family_signature(primary_assessment, domain=primary_domain)
                if host_family_signature is not None:
                    signatures.append(("host_family", host_family_signature))
            lure_signature = _scam_lure_fingerprint(snapshot.context_text)
            if lure_signature is not None and (primary_domain is not None or _looks_like_no_link_dm_lure(snapshot.context_text)):
                signatures.append(("lure", lure_signature))
            if signatures:
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

    def _member_role_ids(self, member: Any) -> frozenset[int]:
        return frozenset(
            int(role.id)
            for role in getattr(member, "roles", []) or []
            if isinstance(getattr(role, "id", None), int) and int(role.id) > 0
        )

    def _member_is_moderator(self, member: Any) -> bool:
        permissions = getattr(member, "guild_permissions", None)
        if permissions is None:
            return False
        return any(
            bool(getattr(permissions, name, False))
            for name in ("administrator", "manage_guild", "manage_messages", "moderate_members", "kick_members", "ban_members")
        )

    def _apply_pack_exemptions(
        self,
        compiled: CompiledShieldConfig,
        message: discord.Message,
        matches: Sequence[ShieldMatch],
    ) -> list[ShieldMatch]:
        user_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        if user_id <= 0 or not matches:
            return list(matches)
        role_ids = self._member_role_ids(getattr(message, "author", None))
        channel_id = getattr(getattr(message, "channel", None), "id", None)
        filtered: list[ShieldMatch] = []
        for match in matches:
            scope = compiled.pack_exemptions.get(match.pack)
            if scope is not None and scope.matches(channel_id=channel_id, user_id=user_id, role_ids=role_ids):
                continue
            filtered.append(match)
        return filtered

    def _cap_spam_action_for_moderator(self, match: ShieldMatch) -> ShieldMatch:
        capped_action = match.action
        if match.action in {"timeout_log", "delete_timeout_log", "delete_escalate"}:
            capped_action = "delete_log"
        return ShieldMatch(
            pack=match.pack,
            label=match.label,
            reason=match.reason,
            action=capped_action,
            confidence=match.confidence,
            heuristic=match.heuristic,
            match_class=match.match_class,
        )

    def _cap_group_gif_action(self, match: ShieldMatch) -> ShieldMatch:
        capped_action = "delete_log" if match.action.startswith("delete") or match.action in {"timeout_log", "delete_timeout_log", "delete_escalate"} else "log"
        return ShieldMatch(
            pack=match.pack,
            label=match.label,
            reason=match.reason,
            action=capped_action,
            confidence=match.confidence,
            heuristic=match.heuristic,
            match_class=match.match_class,
        )

    def _apply_spam_moderator_policy(
        self,
        compiled: CompiledShieldConfig,
        member: Any,
        matches: Sequence[ShieldMatch],
    ) -> list[ShieldMatch]:
        if not matches or not self._member_is_moderator(member):
            return list(matches)
        policy = compiled.spam_rules.moderator_policy
        if policy == "full":
            return list(matches)
        filtered: list[ShieldMatch] = []
        for match in matches:
            if match.pack != "spam":
                filtered.append(match)
                continue
            if policy == "delete_only":
                filtered.append(self._cap_spam_action_for_moderator(match))
        return filtered

    def _dedupe_message_targets(self, messages: Sequence[discord.Message]) -> list[discord.Message]:
        unique: list[discord.Message] = []
        seen: set[int] = set()
        for message in messages:
            message_id = getattr(message, "id", None)
            if not isinstance(message_id, int):
                continue
            if message_id in seen:
                continue
            seen.add(message_id)
            unique.append(message)
        return unique

    def _message_id(self, message: Any) -> int | None:
        message_id = getattr(message, "id", None)
        return message_id if isinstance(message_id, int) and message_id > 0 else None

    def _message_is_deleted(self, message: Any) -> bool:
        if message is None:
            return False
        if bool(getattr(message, "deleted", False)):
            return True
        message_id = self._message_id(message)
        return message_id is not None and message_id in self._deleted_message_ids

    def _record_deleted_message(self, message: Any, *, now: float | None = None) -> None:
        message_id = self._message_id(message)
        if message_id is None:
            return
        self._deleted_message_ids[message_id] = time.monotonic() if now is None else now

    def _message_pack_exempt(self, compiled: CompiledShieldConfig, message: discord.Message, pack: str) -> bool:
        scope = compiled.pack_exemptions.get(pack)
        if scope is None:
            return False
        user_id = int(getattr(getattr(message, "author", None), "id", 0) or 0)
        if user_id <= 0:
            return False
        channel_id = getattr(getattr(message, "channel", None), "id", None)
        return scope.matches(
            channel_id=channel_id,
            user_id=user_id,
            role_ids=self._member_role_ids(getattr(message, "author", None)),
        )

    def _select_incident_tail(
        self,
        events: Sequence[ShieldSpamEvent],
        *,
        window_seconds: float,
        minimum_count: int,
    ) -> list[ShieldSpamEvent]:
        if not events:
            return []
        max_gap = max(1.0, min(4.0, window_seconds / max(2, minimum_count)))
        selected: list[ShieldSpamEvent] = []
        last_timestamp: float | None = None
        for event in reversed(events):
            if last_timestamp is not None and last_timestamp - event.timestamp > max_gap and len(selected) >= max(1, minimum_count - 1):
                break
            selected.append(event)
            last_timestamp = event.timestamp
        return list(reversed(selected))

    def _select_minimal_personal_gif_tail(
        self,
        rows: Sequence[ShieldChannelActivityEvent],
        compiled: CompiledShieldConfig,
        *,
        minimum_authors: int,
    ) -> list[ShieldChannelActivityEvent]:
        working_rows = [
            row
            for row in rows
            if row.message is not None
            and not self._message_is_deleted(row.message)
        ]
        if not working_rows:
            return []
        if sum(1 for row in working_rows if row.is_gif_message and not row.gif_pack_exempt) < compiled.gif_rules.message_threshold:
            return []
        for start_index in range(len(working_rows) - 1, -1, -1):
            tail_rows = working_rows[start_index:]
            metrics = self._gif_pressure_metrics(tail_rows)
            if not self._gif_pressure_triggered(metrics, compiled, minimum_authors=minimum_authors):
                continue
            selected_rows = [row for row in tail_rows if row.is_gif_message and not row.gif_pack_exempt]
            if selected_rows:
                return selected_rows
        return []

    def _select_newest_contributing_gif_rows(
        self,
        rows: Sequence[ShieldChannelActivityEvent],
        compiled: CompiledShieldConfig,
        *,
        minimum_authors: int,
    ) -> list[ShieldChannelActivityEvent]:
        working_rows = [
            row
            for row in rows
            if row.message is not None
            and not self._message_is_deleted(row.message)
        ]
        if not working_rows:
            return []
        removable_rows = [row for row in working_rows if row.is_gif_message and not row.gif_pack_exempt]
        if len(removable_rows) < compiled.gif_rules.message_threshold:
            return []
        selected_rows: list[ShieldChannelActivityEvent] = []
        remaining_rows = list(working_rows)
        for row in reversed(removable_rows):
            metrics = self._gif_pressure_metrics(remaining_rows)
            if not self._gif_pressure_triggered(metrics, compiled, minimum_authors=minimum_authors):
                break
            selected_rows.append(row)
            message_id = self._message_id(row.message)
            remove_index = next(
                (
                    index
                    for index, candidate in enumerate(remaining_rows)
                    if self._message_id(candidate.message) == message_id
                ),
                None,
            )
            if remove_index is None:
                continue
            remaining_rows.pop(remove_index)
        selected_rows.reverse()
        return selected_rows

    def _collect_group_gif_incident_messages(
        self,
        channel_activity: Sequence[ShieldChannelActivityEvent],
        compiled: CompiledShieldConfig,
        *,
        group_gif_pressure: dict[str, Any] | None,
        now: float,
    ) -> list[discord.Message]:
        if not channel_activity or group_gif_pressure is None:
            return []
        triggered_by = tuple(group_gif_pressure.get("triggered_by", ()))
        if "streak" in triggered_by:
            streak_rows = [
                row
                for row in group_gif_pressure.get("streak_rows", ())
                if row.message is not None and not self._message_is_deleted(row.message)
            ]
            if (
                len(streak_rows) >= compiled.gif_rules.consecutive_threshold
                and len({row.user_id for row in streak_rows}) >= 2
            ):
                return self._dedupe_message_targets(
                    [row.message for row in streak_rows if row.message is not None]
                )
        if "pressure" not in triggered_by:
            return []
        pressure_rows = [
            row
            for row in group_gif_pressure.get("pressure_rows", ())
            if row.message is not None
            and now - row.timestamp <= self._gif_pressure_window_seconds(compiled)
            and not self._message_is_deleted(row.message)
        ]
        if not pressure_rows:
            return []
        selected_rows = self._select_newest_contributing_gif_rows(
            pressure_rows,
            compiled,
            minimum_authors=2,
        )
        return self._dedupe_message_targets(
            [row.message for row in selected_rows if row.message is not None]
        )

    def _collect_personal_gif_incident_messages(
        self,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        *,
        now: float,
        personal_gif_pressure: dict[str, Any] | None,
    ) -> list[discord.Message]:
        if personal_gif_pressure is None:
            return [message]
        triggered_by = tuple(personal_gif_pressure.get("triggered_by", ()))
        if "same_asset" in triggered_by:
            same_asset_messages = list(personal_gif_pressure.get("same_asset_messages", ()))
            if same_asset_messages:
                return self._dedupe_message_targets(same_asset_messages) or [message]
        if "streak" in triggered_by:
            streak_rows = [
                row
                for row in personal_gif_pressure.get("streak_rows", ())
                if row.message is not None and not self._message_is_deleted(row.message)
            ]
            if len(streak_rows) >= compiled.gif_rules.consecutive_threshold:
                return self._dedupe_message_targets([row.message for row in streak_rows if row.message is not None]) or [message]
        pressure_rows = [
            row
            for row in personal_gif_pressure.get("pressure_rows", ())
            if row.message is not None
            and now - row.timestamp <= self._gif_pressure_window_seconds(compiled)
            and not self._message_is_deleted(row.message)
        ]
        selected_rows = self._select_minimal_personal_gif_tail(
            pressure_rows,
            compiled,
            minimum_authors=1,
        )
        if not selected_rows:
            selected_rows = [
                row
                for row in pressure_rows
                if row.is_gif_message and not row.gif_pack_exempt
            ][-max(1, compiled.gif_rules.message_threshold):]
        return self._dedupe_message_targets([row.message for row in selected_rows if row.message is not None]) or [message]

    def _primary_gif_match(
        self,
        *,
        personal_match: ShieldMatch | None,
        group_match: ShieldMatch | None,
        fallback: ShieldMatch,
    ) -> ShieldMatch:
        candidates: list[tuple[int, ShieldMatch]] = []
        if group_match is not None:
            candidates.append((0, group_match))
        if personal_match is not None:
            candidates.append((1, personal_match))
        if not candidates:
            return fallback
        _tie_rank, match = max(
            candidates,
            key=lambda item: (
                ACTION_STRENGTH.get(item[1].action, 0),
                CONFIDENCE_STRENGTH.get(item[1].confidence, 0),
                item[0],
            ),
        )
        return match

    def _gif_group_action_note(
        self,
        group_match: ShieldMatch,
        *,
        group_gif_pressure: dict[str, Any] | None,
        personal_match: ShieldMatch | None = None,
        group_delete_count: int = 0,
    ) -> str:
        triggers = tuple(group_gif_pressure.get("triggered_by", ())) if group_gif_pressure is not None else ()
        substantive_text_posts = int(group_gif_pressure.get("substantive_text_posts", 0)) if group_gif_pressure is not None else 0
        filler_text_posts = int(group_gif_pressure.get("filler_text_posts", 0)) if group_gif_pressure is not None else 0
        cleanup_phrase = "kept the collective signal channel-level only"
        delete_enabled = group_match.action.startswith("delete")
        if delete_enabled and "streak" in triggers:
            cleanup_phrase = (
                f"removed the exact {group_delete_count or int(group_gif_pressure.get('consecutive_gif_posts', 0) or 0)}-message live GIF streak"
                if group_delete_count > 0 or group_gif_pressure is not None
                else "removed the exact live GIF streak"
            )
        elif delete_enabled and "pressure" in triggers:
            cleanup_phrase = (
                f"trimmed the {group_delete_count} newest contributing GIF posts from the active pressure slice"
                if group_delete_count > 0
                else "trimmed only the newest contributing GIF posts from the active pressure slice"
            )
        elif delete_enabled:
            cleanup_phrase = "used channel-safe GIF cleanup"
        preserved_text_bits: list[str] = []
        if delete_enabled and substantive_text_posts > 0:
            preserved_text_bits.append(f"{substantive_text_posts} substantive text messages")
        if delete_enabled and filler_text_posts > 0:
            preserved_text_bits.append(f"{filler_text_posts} filler text messages")
        preserved_text_phrase = f" It preserved {', '.join(preserved_text_bits)}." if preserved_text_bits else ""
        if group_gif_pressure is not None and group_gif_pressure.get("streak_capped"):
            preserved_text_phrase += f" The live streak tracker was capped at {GIF_STREAK_TRACK_LIMIT} messages."

        if personal_match is None:
            if delete_enabled:
                return (
                    f"Collective channel GIF pressure triggered channel-safe cleanup only; Babblebox {cleanup_phrase} "
                    f"and did not add strikes or time out members on the group signal alone.{preserved_text_phrase}"
                )
            return (
                "Collective channel GIF pressure stayed channel-level only; Babblebox did not add strikes or time out "
                "members on the group signal alone."
            )

        personal_enforcement = personal_match.action not in {"detect", "log"}
        if personal_enforcement:
            if delete_enabled:
                return (
                    f"Channel-wide GIF pressure was also active, so Babblebox {cleanup_phrase} and followed this "
                    f"member's personal GIF-abuse threshold for individual enforcement.{preserved_text_phrase}"
                )
            return (
                "Channel-wide GIF pressure was also active, but Babblebox kept the collective signal channel-level "
                "only and followed this member's personal GIF-abuse threshold for individual enforcement."
            )
        if delete_enabled:
            return (
                f"Channel-wide GIF pressure was also active, so Babblebox {cleanup_phrase} while keeping any "
                f"member-specific action bounded to the channel-safe collective cleanup.{preserved_text_phrase}"
            )
        return (
            "Channel-wide GIF pressure was also active, and Babblebox kept the collective signal channel-level only "
            "without strikes or timeouts."
        )

    def _build_gif_incident_plan(
        self,
        matches: Sequence[ShieldMatch],
        preferred_match: ShieldMatch,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        *,
        now: float,
        channel_activity: Sequence[ShieldChannelActivityEvent] = (),
        personal_gif_pressure: dict[str, Any] | None = None,
        group_gif_pressure: dict[str, Any] | None = None,
    ) -> ShieldGifIncidentPlan | None:
        personal_match = next((item for item in matches if item.match_class == "spam_gif_flood"), None)
        group_match = next((item for item in matches if item.match_class == "spam_group_gif_pressure"), None)
        if personal_match is None and group_match is None:
            return None

        primary_match = self._primary_gif_match(
            personal_match=personal_match,
            group_match=group_match,
            fallback=preferred_match,
        )
        group_delete_targets: list[discord.Message] = []
        personal_delete_targets: list[discord.Message] = []
        if group_match is not None and group_match.action.startswith("delete"):
            group_delete_targets.extend(
                self._collect_group_gif_incident_messages(
                    channel_activity,
                    compiled,
                    group_gif_pressure=group_gif_pressure,
                    now=now,
                )
            )
        if personal_match is not None and personal_match.action.startswith("delete"):
            personal_delete_targets.extend(
                self._collect_personal_gif_incident_messages(
                    message,
                    compiled,
                    now=now,
                    personal_gif_pressure=personal_gif_pressure,
                )
            )
        deduped_targets = tuple(self._dedupe_message_targets([*group_delete_targets, *personal_delete_targets]))

        if personal_match is not None and group_match is not None:
            personal_signature = personal_gif_pressure.get("signature") if personal_gif_pressure is not None else None
            group_signature = group_gif_pressure.get("signature") if group_gif_pressure is not None else None
            signature_seed = "|".join(part for part in (personal_signature, group_signature) if part)
            alert_signature = (
                hashlib.sha1(f"gif_mix|{signature_seed}".encode("utf-8")).hexdigest()
                if signature_seed
                else personal_signature or group_signature
            )
            evidence_summary = (
                "Personal GIF abuse and collective channel GIF pressure were both active. "
                f"Personal signal: {personal_match.reason} Collective signal: {group_match.reason}"
            )
            action_note = self._gif_group_action_note(
                group_match,
                group_gif_pressure=group_gif_pressure,
                personal_match=personal_match,
                group_delete_count=len(self._dedupe_message_targets(group_delete_targets)),
            )
        elif group_match is not None:
            alert_signature = group_gif_pressure.get("signature") if group_gif_pressure is not None else None
            evidence_summary = group_match.reason
            action_note = self._gif_group_action_note(
                group_match,
                group_gif_pressure=group_gif_pressure,
                group_delete_count=len(self._dedupe_message_targets(group_delete_targets)),
            )
        else:
            alert_signature = personal_gif_pressure.get("signature") if personal_gif_pressure is not None else None
            evidence_summary = personal_match.reason if personal_match is not None else preferred_match.reason
            action_note = None

        return ShieldGifIncidentPlan(
            primary_match=primary_match,
            personal_match=personal_match,
            group_match=group_match,
            delete_targets=deduped_targets,
            alert_evidence_signature=alert_signature,
            alert_evidence_summary=evidence_summary,
            action_note=action_note,
        )

    def _collect_incident_messages(
        self,
        match: ShieldMatch,
        message: discord.Message,
        snapshot: ShieldSnapshot,
        recent_events: Sequence[ShieldSpamEvent],
        compiled: CompiledShieldConfig,
        *,
        now: float,
        channel_activity: Sequence[ShieldChannelActivityEvent] = (),
        group_gif_pressure: dict[str, Any] | None = None,
    ) -> list[discord.Message]:
        channel_id = getattr(getattr(message, "channel", None), "id", None)
        same_channel_events = [
            event
            for event in recent_events
            if event.message is not None
            and event.channel_id == channel_id
            and not self._message_is_deleted(event.message)
        ]
        if not same_channel_events:
            if match.match_class == "spam_group_gif_pressure":
                return self._collect_group_gif_incident_messages(
                    channel_activity,
                    compiled,
                    group_gif_pressure=group_gif_pressure,
                    now=now,
                )
            return [message]

        selected_events: list[ShieldSpamEvent]
        if match.match_class == "spam_group_gif_pressure":
            return self._collect_group_gif_incident_messages(
                channel_activity,
                compiled,
                group_gif_pressure=group_gif_pressure,
                now=now,
            )
        if match.pack == "gif":
            selected_events = [
                event
                for event in same_channel_events
                if event.is_gif_message
                and not event.gif_pack_exempt
                and now - event.timestamp <= float(compiled.gif_rules.window_seconds)
            ]
            if match.match_class == "spam_gif_flood" and snapshot.gif_signature is not None:
                same_asset_events = [event for event in selected_events if event.gif_signature == snapshot.gif_signature]
                if len(same_asset_events) >= compiled.gif_rules.same_asset_threshold:
                    selected_events = same_asset_events
        elif match.match_class == "spam_duplicate":
            selected_events = [
                event
                for event in same_channel_events
                if snapshot.exact_fingerprint is not None
                and event.exact_fingerprint == snapshot.exact_fingerprint
                and now - event.timestamp <= float(compiled.spam_rules.near_duplicate_window_seconds)
            ]
        elif match.match_class == "spam_near_duplicate":
            selected_events = [
                event
                for event in same_channel_events
                if event.near_text
                and snapshot.near_duplicate_text
                and now - event.timestamp <= float(compiled.spam_rules.near_duplicate_window_seconds)
                and difflib.SequenceMatcher(None, snapshot.near_duplicate_text, event.near_text).ratio() >= 0.88
            ]
        elif match.match_class == "spam_link_flood":
            selected_events = [
                event for event in same_channel_events if event.has_links and now - event.timestamp <= SPAM_LINK_WINDOW_SECONDS
            ]
        elif match.match_class == "spam_invite_flood":
            selected_events = [
                event for event in same_channel_events if event.invite_codes and now - event.timestamp <= SPAM_INVITE_WINDOW_SECONDS
            ]
        elif match.match_class == "spam_mention_flood":
            selected_events = [
                event
                for event in same_channel_events
                if (event.mention_count > 0 or event.everyone_here_count > 0) and now - event.timestamp <= SPAM_MENTION_WINDOW_SECONDS
            ]
        elif match.match_class == "spam_emoji_flood":
            threshold = max(8, compiled.spam_rules.emote_threshold - 4)
            selected_events = [
                event
                for event in same_channel_events
                if event.emoji_count >= threshold and now - event.timestamp <= float(compiled.spam_rules.burst_window_seconds)
            ]
        elif match.match_class == "spam_caps_flood":
            selected_events = [
                event
                for event in same_channel_events
                if event.uppercase_count >= compiled.spam_rules.caps_threshold
                and event.alpha_count >= compiled.spam_rules.caps_threshold + 6
                and (event.uppercase_count / max(1, event.alpha_count)) >= 0.72
                and now - event.timestamp <= float(compiled.spam_rules.burst_window_seconds)
            ]
        elif match.match_class == "spam_low_value_noise":
            selected_events = [
                event
                for event in same_channel_events
                if event.low_value_text and now - event.timestamp <= float(compiled.spam_rules.low_value_window_seconds)
            ]
        elif match.match_class == "spam_padding_noise":
            selected_events = [
                event
                for event in same_channel_events
                if event.repeated_char_run >= 12
                and now - event.timestamp <= float(max(compiled.spam_rules.message_window_seconds, compiled.spam_rules.burst_window_seconds))
            ]
        elif match.match_class == "spam_burst":
            burst_window = [
                event for event in same_channel_events if now - event.timestamp <= float(compiled.spam_rules.burst_window_seconds)
            ]
            selected_events = self._select_incident_tail(
                burst_window,
                window_seconds=float(compiled.spam_rules.burst_window_seconds),
                minimum_count=compiled.spam_rules.burst_threshold,
            )
        else:
            message_window = [
                event for event in same_channel_events if now - event.timestamp <= float(compiled.spam_rules.message_window_seconds)
            ]
            selected_events = self._select_incident_tail(
                message_window,
                window_seconds=float(compiled.spam_rules.message_window_seconds),
                minimum_count=compiled.spam_rules.message_threshold,
            )

        selected_messages = self._dedupe_message_targets([event.message for event in selected_events if event.message is not None])
        return selected_messages or [message]

    async def _delete_messages(self, messages: Sequence[discord.Message]) -> tuple[int, int]:
        targets = self._dedupe_message_targets(messages)
        deleted = 0
        for target in targets:
            if await self._delete_message(target):
                deleted += 1
        return deleted, len(targets)

    def _is_escalation_eligible(self, compiled: CompiledShieldConfig, match: ShieldMatch) -> bool:
        if match.match_class == "spam_group_gif_pressure":
            return False
        settings = compiled.pack_settings(match.pack)
        return (
            settings.high_action == "delete_escalate"
            and match.action in {"delete_log", "delete_escalate"}
            and match.confidence in {"medium", "high"}
            and match.match_class in ESCALATION_ELIGIBLE_MATCH_CLASSES
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
        recent_spam_events: Sequence[ShieldSpamEvent] | None = None,
        author_kind: str = "human",
        author_id: int = 0,
        now: float | None = None,
        channel_id: int | None = None,
        distinct_channel_authors: int = 0,
        quality_channel_authors: int = 0,
        channel_activity: Sequence[ShieldChannelActivityEvent] = (),
        personal_gif_pressure: dict[str, Any] | None = None,
        group_gif_pressure: dict[str, Any] | None = None,
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
                author_kind=author_kind,
                author_id=author_id,
                distinct_channel_authors=distinct_channel_authors,
                quality_channel_authors=quality_channel_authors,
                channel_activity=channel_activity,
                personal_gif_pressure=personal_gif_pressure,
                group_gif_pressure=group_gif_pressure,
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
        if not text:
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
        if is_severe_reference_context(text, matched_terms=hits):
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
        if SEVERE_SELF_HARM_NEGATION_RE.search(text):
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
        if is_severe_reference_context(text, matched_terms=hits):
            return []
        if SEVERE_SUPPORT_CONTEXT_RE.search(text) and is_harmful_context_suppressed(text, include_disapproval=True):
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
        if is_severe_reference_context(text):
            return []
        if SEVERE_ELIMINATION_RE.search(text):
            confidence = "high"
        elif SEVERE_GROUP_DEHUMANIZING_RE.search(text):
            confidence = "medium"
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
        if hits and is_severe_reference_context(text, matched_terms=hits):
            return []
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
        degrading_hits = find_safety_term_hits(SEVERE_DEGRADING_TERMS, text, squashed)
        if degrading_hits and is_severe_reference_context(text, matched_terms=degrading_hits):
            return []
        explicit_targeted_degradation = bool(SEVERE_TARGETED_DEGRADING_RE.search(text))
        if explicit_targeted_degradation or (compiled.severe.sensitivity == "high" and degrading_hits and targeted):
            return [
                self._make_pack_match(
                    pack="severe",
                    settings=compiled.severe,
                    label="Extreme degrading abuse",
                    reason="Targeted dehumanizing abuse crossed the severe-harm threshold.",
                    confidence="medium" if explicit_targeted_degradation else "low",
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
            if link.preview_only and _assessment_is_preview_only_advisory(assessment):
                continue
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
        folded_context_text = fold_confusable_text(snapshot.context_text)
        folded_context_squashed = squash_for_evasion_checks(folded_context_text)
        active_link_assessments = tuple(
            assessment for assessment in link_assessments if not _assessment_is_preview_only_advisory(assessment)
        )
        risky_domains = [
            assessment.normalized_domain
            for assessment in active_link_assessments
            if assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY, UNKNOWN_SUSPICIOUS_LINK_CATEGORY}
        ]
        link_risk_score = max((_score_link_assessment_for_scam(item) for item in active_link_assessments), default=0)
        shortener_or_punycode = any(
            _domain_in_set(domain, SHORTENER_DOMAINS) or "xn--" in domain
            for domain in risky_domains
        )
        bait = _regex_signal(SCAM_BAIT_RE, snapshot.context_text, snapshot.context_squashed, folded_context_text, folded_context_squashed)
        social_engineering = bool(SOCIAL_ENGINEERING_RE.search(snapshot.context_text))
        cta = _regex_signal(SCAM_CTA_RE, snapshot.context_text, snapshot.context_squashed, folded_context_text, folded_context_squashed)
        brand_bait = bool(BRAND_BAIT_RE.search(snapshot.context_text))
        official_framing = bool(SCAM_OFFICIAL_FRAMING_RE.search(snapshot.context_text))
        announcement_framing = bool(SCAM_ANNOUNCEMENT_RE.search(snapshot.context_text))
        partnership_framing = bool(SCAM_PARTNERSHIP_RE.search(snapshot.context_text))
        support_framing = bool(SCAM_SUPPORT_RE.search(snapshot.context_text))
        security_notice = bool(SCAM_SECURITY_NOTICE_RE.search(snapshot.context_text))
        fake_authority = bool(SCAM_FAKE_AUTHORITY_RE.search(snapshot.context_text))
        qr_setup_lure = _regex_signal(SCAM_QR_SETUP_RE, snapshot.context_text, snapshot.context_squashed, folded_context_text, folded_context_squashed)
        community_post_framing = "community post" in snapshot.context_text
        urgency = bool(SCAM_URGENCY_RE.search(snapshot.context_text))
        wallet_or_mint = _regex_signal(SCAM_CRYPTO_MINT_RE, snapshot.context_text, snapshot.context_squashed, folded_context_text, folded_context_squashed)
        login_or_auth_flow = _regex_signal(SCAM_LOGIN_FLOW_RE, snapshot.context_text, snapshot.context_squashed, folded_context_text, folded_context_squashed)
        strong_private_route_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_STRONG_PRIVATE_ROUTE_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        soft_private_route_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_SOFT_ROUTE_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        activity_probe_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_ACTIVITY_PROBE_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        earnings_bait_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_EARNINGS_BAIT_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        gambling_bait_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_GAMBLING_BAIT_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        no_link_pressure_hits = _scam_phrase_signal_hits(
            SCAM_NO_LINK_PRESSURE_TERMS,
            text=snapshot.context_text,
            squashed=snapshot.context_squashed,
            folded_text=folded_context_text,
            folded_squashed=folded_context_squashed,
        )
        dm_route = bool(SCAM_DM_ROUTE_RE.search(snapshot.context_text) or SCAM_DM_ROUTE_RE.search(snapshot.context_squashed) or strong_private_route_hits)
        off_platform_route = bool(
            SCAM_OFF_PLATFORM_ROUTE_RE.search(snapshot.context_text) or SCAM_OFF_PLATFORM_ROUTE_RE.search(snapshot.context_squashed)
        )
        soft_private_route = bool(soft_private_route_hits)
        activity_probe = bool(activity_probe_hits)
        earnings_bait = bool(earnings_bait_hits)
        gambling_bait = bool(gambling_bait_hits)
        no_link_pressure = bool(no_link_pressure_hits)
        prize_or_money_bait = _regex_signal(
            SCAM_PRIZE_BAIT_RE,
            snapshot.context_text,
            snapshot.context_squashed,
            folded_context_text,
            folded_context_squashed,
        )
        commodity_bait = _regex_signal(
            SCAM_VALUABLE_ITEM_RE,
            snapshot.context_text,
            snapshot.context_squashed,
            folded_context_text,
            folded_context_squashed,
        )
        too_good_offer = _regex_signal(
            SCAM_DEAL_TOO_GOOD_RE,
            snapshot.context_text,
            snapshot.context_squashed,
            folded_context_text,
            folded_context_squashed,
        )
        direct_offer = _regex_signal(
            SCAM_DIRECT_OFFER_RE,
            snapshot.context_text,
            snapshot.context_squashed,
            folded_context_text,
            folded_context_squashed,
        )
        cash_amount_bait = _regex_signal(SCAM_CASH_AMOUNT_RE, snapshot.context_text, folded_context_text)
        claim_or_details_language = bool(
            SCAM_CLAIM_OR_DETAILS_RE.search(snapshot.context_text) or SCAM_CLAIM_OR_DETAILS_RE.search(snapshot.context_squashed)
        )
        nitro_or_crypto_bait = bool(
            re.search(r"(?i)\b(?:nitro|crypto|btc|bitcoin|eth|ethereum|usdt)\b", snapshot.context_text)
            or re.search(r"(?i)\b(?:nitro|crypto|btc|bitcoin|eth|ethereum|usdt)\b", snapshot.context_squashed)
        )
        benign_trade_context = bool(SCAM_BENIGN_TRADE_DISCUSSION_RE.search(snapshot.context_text))
        benign_sports_context = bool(SCAM_NO_LINK_BENIGN_SPORTS_DISCUSSION_RE.search(snapshot.context_text))
        benign_dm_coordination = bool(SCAM_NO_LINK_BENIGN_DM_COORDINATION_RE.search(snapshot.context_text))
        emoji_hype = (
            snapshot.emoji_count > 0
            and snapshot.plain_word_count <= 12
            and (soft_private_route or activity_probe or earnings_bait or gambling_bait or prize_or_money_bait)
        )
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
            dm_route=dm_route,
            off_platform_route=off_platform_route,
            soft_private_route=soft_private_route,
            activity_probe=activity_probe,
            earnings_bait=earnings_bait,
            gambling_bait=gambling_bait,
            no_link_pressure=no_link_pressure,
            emoji_hype=emoji_hype,
            prize_or_money_bait=prize_or_money_bait,
            commodity_bait=commodity_bait,
            too_good_offer=too_good_offer,
            direct_offer=direct_offer,
            cash_amount_bait=cash_amount_bait,
            claim_or_details_language=claim_or_details_language,
            nitro_or_crypto_bait=nitro_or_crypto_bait,
            benign_trade_context=benign_trade_context,
            benign_sports_context=benign_sports_context,
            benign_dm_coordination=benign_dm_coordination,
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
                bool(features.commodity_bait),
                bool(features.too_good_offer),
                bool(features.direct_offer),
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
        active_link_assessments = tuple(
            assessment for assessment in link_assessments if not _assessment_is_preview_only_advisory(assessment)
        )
        shortener_signal_present = any("shortener_domain" in assessment.matched_signals for assessment in active_link_assessments)
        punycode_signal_present = any(
            "xn--" in assessment.normalized_domain
            or "punycode_host" in assessment.matched_signals
            or "punycode_brand" in assessment.matched_signals
            for assessment in active_link_assessments
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
        shortener_has_strong_lure = bool(
            features.bait
            or features.brand_bait
            or features.support_framing
            or features.security_notice
            or features.fake_authority
            or features.qr_setup_lure
            or features.wallet_or_mint
            or features.login_or_auth_flow
            or features.suspicious_attachment_combo
            or features.dangerous_link_target
            or (not features.automated_author and features.fresh_campaign_cluster_20m >= 2)
        )
        generic_shortener_cta = shortener_signal_present and (features.social_engineering or features.cta)
        if punycode_signal_present and (features.social_engineering or features.cta or features.login_or_auth_flow):
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Shortened or punycode lure",
                    reason="A punycode-style link appeared with instructions to open, claim, or verify something.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_shortener",
                )
            )
        elif generic_shortener_cta and shortener_has_strong_lure:
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Shortened-link lure",
                    reason="A shortened link appeared with concrete scam bait, account-flow, or impersonation context.",
                    action=settings.action_for_confidence("high"),
                    confidence="high",
                    heuristic=True,
                    match_class="scam_shortener",
                )
            )
        elif generic_shortener_cta and settings.sensitivity == "high":
            matches.append(
                ShieldMatch(
                    pack="scam",
                    label="Shortened-link caution",
                    reason="A shortened link appeared with generic open or visit wording, but no stronger scam bait.",
                    action=settings.action_for_confidence("low"),
                    confidence="low",
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
        no_link_lure_corrob = bool(
            features.urgency
            or features.no_link_pressure
            or features.activity_probe
            or features.cash_amount_bait
            or features.emoji_hype
            or (not features.automated_author and features.newcomer_early_message)
            or (not features.automated_author and features.fresh_campaign_cluster_20m >= 2)
        )
        no_link_route_signal = bool(
            features.dm_route
            or features.off_platform_route
            or (features.soft_private_route and no_link_lure_corrob)
        )
        no_link_bait_signal = bool(
            features.prize_or_money_bait
            or features.commodity_bait
            or features.too_good_offer
            or features.direct_offer
            or features.earnings_bait
            or features.gambling_bait
        )
        no_link_soft_family = bool(
            features.earnings_bait
            or features.gambling_bait
            or features.soft_private_route
        ) and not (
            features.prize_or_money_bait
            or features.commodity_bait
            or features.too_good_offer
            or features.direct_offer
            or features.cash_amount_bait
        )
        should_check_no_link_lure = (
            not snapshot.has_links
            and no_link_route_signal
            and no_link_bait_signal
            and (not no_link_soft_family or no_link_lure_corrob)
            and not (features.benign_trade_context or features.benign_sports_context or features.benign_dm_coordination)
        )
        if should_check_no_link_lure:
            dm_lure_score = 0
            reason_bits: list[str] = []
            if features.dm_route:
                dm_lure_score += 2
                reason_bits.append("private DM routing")
            if features.off_platform_route:
                dm_lure_score += 1
                reason_bits.append("off-platform routing")
            if features.soft_private_route:
                dm_lure_score += 1
                reason_bits.append("private follow-up CTA")
            if features.prize_or_money_bait:
                dm_lure_score += 2
                reason_bits.append("prize, money, nitro, or crypto bait")
            if features.commodity_bait:
                dm_lure_score += 2
                reason_bits.append("high-risk item or account bait")
            if features.too_good_offer:
                dm_lure_score += 2
                reason_bits.append("too-good-to-be-true pricing")
            if features.direct_offer:
                dm_lure_score += 1
                reason_bits.append("direct sale or giveaway framing")
            if features.earnings_bait:
                dm_lure_score += 2
                reason_bits.append("money or payout bait")
            if features.gambling_bait:
                dm_lure_score += 2
                reason_bits.append("wins, picks, or betting tout bait")
            if features.cash_amount_bait:
                dm_lure_score += 1
                reason_bits.append("cash-amount promise")
            if features.nitro_or_crypto_bait:
                dm_lure_score += 1
                reason_bits.append("nitro or crypto framing")
            if features.claim_or_details_language:
                dm_lure_score += 1
                reason_bits.append("claim or details wording")
            if features.urgency:
                dm_lure_score += 1
                reason_bits.append("urgency or scarcity")
            if features.no_link_pressure:
                dm_lure_score += 1
                reason_bits.append("time pressure or FOMO")
            if features.activity_probe:
                dm_lure_score += 1
                reason_bits.append("activity probe or recruitment copy")
            if features.fake_authority or features.official_like_framing:
                dm_lure_score += 1
                reason_bits.append("fake authority or official framing")
            if features.emoji_hype:
                dm_lure_score += 1
                reason_bits.append("emoji-heavy short hype")
            if not features.automated_author and features.newcomer_early_message:
                dm_lure_score += 1
                reason_bits.append("newcomer early-message delivery")
            if not features.automated_author and features.fresh_campaign_cluster_20m >= 2:
                dm_lure_score += 1
                reason_bits.append(f"repeat fresh-account pattern ({_cluster_reason_text(features.fresh_campaign_kinds)})")

            dm_lure_confidence: str | None = None
            if dm_lure_score >= 7:
                dm_lure_confidence = "high"
            elif dm_lure_score >= 5:
                dm_lure_confidence = "medium"
            elif dm_lure_score >= 4 and settings.sensitivity == "high":
                dm_lure_confidence = "low"

            if dm_lure_confidence is not None:
                label = "Money / wins DM lure"
                base_reason = "Money, payout, or wins bait pushed members into DMs or private follow-up without a safe public path."
                if features.commodity_bait or features.too_good_offer:
                    label = "Too-good-to-be-true DM lure"
                    base_reason = (
                        "A suspicious high-value item, account, or giveaway offer pushed members into DMs or private follow-up "
                        "without a safe public path."
                    )
                elif features.gambling_bait and not (
                    features.commodity_bait
                    or features.too_good_offer
                    or features.earnings_bait
                    or features.prize_or_money_bait
                    or features.cash_amount_bait
                ):
                    label = "Betting picks / wins lure"
                    base_reason = (
                        "Betting, picks, or guaranteed-wins style bait pushed members into DMs or private follow-up without a safe public path."
                    )
                matches.append(
                    ShieldMatch(
                        pack="scam",
                        label=label,
                        reason=(
                            base_reason
                            if not reason_bits
                            else f"{base_reason[:-1]} ({', '.join(reason_bits[:5])})."
                        ),
                        action=settings.action_for_confidence(dm_lure_confidence),
                        confidence=dm_lure_confidence,
                        heuristic=True,
                        match_class="scam_dm_lure",
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
            if features.commodity_bait:
                weighted_score += 1
                reason_bits.append("high-value item or account bait")
            if features.too_good_offer:
                weighted_score += 1
                reason_bits.append("too-good-to-be-true offer wording")
            if features.direct_offer and not features.automated_author:
                weighted_score += 1
                reason_bits.append("direct sale or giveaway framing")
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
            hard_link_evidence = (
                any(assessment.category in {MALICIOUS_LINK_CATEGORY, IMPERSONATION_LINK_CATEGORY} for assessment in link_assessments)
                or snapshot.has_suspicious_attachment
                or features.dangerous_link_target
            )
            scam_intent_present = bool(
                features.bait
                or features.commodity_bait
                or features.too_good_offer
                or features.direct_offer
                or features.social_engineering
                or features.cta
                or features.login_or_auth_flow
                or features.brand_bait
                or features.support_framing
                or features.security_notice
                or features.fake_authority
                or features.qr_setup_lure
                or features.announcement_framing
                or features.partnership_framing
                or features.community_post_framing
                or features.official_framing
                or features.urgency
                or features.wallet_or_mint
                or features.suspicious_attachment_combo
            )
            fresh_campaign_corroborated = bool(
                features.fresh_campaign_cluster_30m >= 3
                or (features.fresh_campaign_cluster_20m >= 2 and (scam_intent_present or features.risky_link_present))
            )
            ambiguous_unknown_link_context = (
                features.suspicious_link_present
                and not hard_link_evidence
                and not scam_intent_present
                and not fresh_campaign_corroborated
            )
            if ambiguous_unknown_link_context:
                if weighted_score >= 4 and (settings.sensitivity == "high" or self._should_emit_middle_lane_low(features)):
                    confidence = "low"
            elif features.automated_author and weighted_score >= 7:
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
        self._gif_incident_alerts = {
            key: value
            for key, value in self._gif_incident_alerts.items()
            if now - float(value.get("last_seen", 0.0)) <= GIF_INCIDENT_WINDOW_SECONDS
        }
        self._spam_incident_alerts = {
            key: value
            for key, value in self._spam_incident_alerts.items()
            if now - float(value.get("last_seen", 0.0)) <= SPAM_INCIDENT_WINDOW_SECONDS
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
            key: [
                row
                for row in values
                if now - row.timestamp <= CHANNEL_ACTIVITY_WINDOW_SECONDS
                and not self._message_is_deleted(row.message)
            ]
            for key, values in self._recent_channel_activity.items()
            if any(
                now - row.timestamp <= CHANNEL_ACTIVITY_WINDOW_SECONDS and not self._message_is_deleted(row.message)
                for row in values
            )
        }
        self._channel_gif_streaks = {
            key: ShieldChannelGifStreakState(
                rows=tuple(row for row in state.rows if not self._message_is_deleted(row.message)),
                capped=bool(state.capped),
            )
            for key, state in self._channel_gif_streaks.items()
            if any(not self._message_is_deleted(row.message) for row in state.rows)
        }
        deleted_window_seconds = max(SPAM_EVENT_WINDOW_SECONDS, CHANNEL_ACTIVITY_WINDOW_SECONDS, GIF_INCIDENT_WINDOW_SECONDS)
        self._deleted_message_ids = {
            message_id: deleted_at
            for message_id, deleted_at in self._deleted_message_ids.items()
            if now - deleted_at <= deleted_window_seconds
        }
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
        try:
            await message.delete()
            self._record_deleted_message(message)
            return True
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return False
        except Exception:
            return False
        return False

    async def _timeout_member(
        self,
        message: discord.Message,
        compiled: CompiledShieldConfig,
        *,
        pack: str | None = None,
        reason: str,
    ) -> bool:
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
        until = ge.now_utc() + timedelta(minutes=compiled.timeout_minutes_for_pack(pack))
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            await member.timeout(until, reason=reason)
            return True
        return False

    def _primary_alert_reason(self, decision: ShieldDecision) -> ShieldMatch | None:
        if not decision.reasons:
            return None
        return max(
            decision.reasons,
            key=lambda item: (
                1 if item.pack == (decision.pack or item.pack) else 0,
                ACTION_STRENGTH.get(item.action, 0),
                CONFIDENCE_STRENGTH.get(item.confidence, 0),
            ),
        )

    def _is_adaptive_low_confidence_note(self, decision: ShieldDecision, top_reason: ShieldMatch | None) -> bool:
        return bool(
            top_reason is not None
            and top_reason.heuristic
            and top_reason.confidence == "low"
            and decision.action in LOW_CONFIDENCE_ACTIONS
            and not decision.deleted
            and not decision.timed_out
            and not decision.escalated
        )

    def _should_use_compact_alert(
        self,
        compiled: CompiledShieldConfig,
        decision: ShieldDecision,
        top_reason: ShieldMatch | None,
    ) -> bool:
        delivery = compiled.resolved_log_delivery(decision.pack or (top_reason.pack if top_reason is not None else None))
        return delivery.style == "compact" or self._is_adaptive_low_confidence_note(decision, top_reason)

    def _should_ping_alert_role(
        self,
        compiled: CompiledShieldConfig,
        decision: ShieldDecision,
        top_reason: ShieldMatch | None,
    ) -> bool:
        if top_reason is None:
            return False
        delivery = compiled.resolved_log_delivery(decision.pack or top_reason.pack)
        if delivery.ping_mode == "never":
            return False
        if top_reason.match_class == "spam_group_gif_pressure":
            return False
        if delivery.style != "compact" and self._is_adaptive_low_confidence_note(decision, top_reason):
            return False
        if decision.deleted or decision.timed_out or decision.escalated:
            return True
        return bool(top_reason.pack in {"scam", "adult"} and top_reason.confidence == "high")

    def _alert_location_description(self, message: discord.Message, top_reason: ShieldMatch | None) -> str:
        if top_reason is not None and top_reason.match_class == "spam_group_gif_pressure":
            return f"Channel-wide pressure in {message.channel.mention}"
        return f"{message.author.mention} in {message.channel.mention}"

    def _alert_delivery_note(
        self,
        compiled: CompiledShieldConfig,
        decision: ShieldDecision,
        top_reason: ShieldMatch | None,
        *,
        compact_alert: bool,
        ping_alert_role: bool,
    ) -> str | None:
        flags: list[str] = []
        delivery = compiled.resolved_log_delivery(decision.pack or (top_reason.pack if top_reason is not None else None))
        if compact_alert:
            flags.append("compact log")
        if compiled.alert_role_id is not None and not ping_alert_role:
            if delivery.ping_mode == "never":
                flags.append("no role ping")
            elif top_reason is not None and top_reason.match_class == "spam_group_gif_pressure":
                flags.append("collective signal stayed no-ping")
            elif delivery.style != "compact" and self._is_adaptive_low_confidence_note(decision, top_reason):
                flags.append("low-confidence note stayed no-ping")
        return f"Delivery: {', '.join(flags)}." if flags else None

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
        compiled: CompiledShieldConfig,
        decision: ShieldDecision,
        *,
        preview: str,
        attachment_summary: Sequence[str],
        top_reason: ShieldMatch | None,
    ) -> discord.Embed:
        ping_alert_role = self._should_ping_alert_role(compiled, decision, top_reason)
        compact_delivery_note = self._alert_delivery_note(
            compiled,
            decision,
            top_reason,
            compact_alert=True,
            ping_alert_role=ping_alert_role,
        )
        embed = discord.Embed(
            title=f"Shield Note | {PACK_LABELS.get(decision.pack or '', 'Shield')}",
            description=self._alert_location_description(message, top_reason),
            color=ge.EMBED_THEME["info"],
        )
        if top_reason is not None:
            detection_lines = [
                f"**{top_reason.label}**",
                f"Confidence: {CONFIDENCE_LABELS.get(top_reason.confidence, top_reason.confidence.title())}",
                f"Resolved action: {ACTION_LABELS.get(decision.action, decision.action)}",
            ]
            if compact_delivery_note:
                detection_lines.append(compact_delivery_note)
            embed.add_field(
                name="Detection",
                value="\n".join(detection_lines),
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
        footer = "Babblebox Shield | Compact low-confidence note"
        if not self._is_adaptive_low_confidence_note(decision, top_reason):
            footer = "Babblebox Shield | Compact shield log"
        ge.style_embed(embed, footer=footer)
        return embed

    def _gif_incident_key(
        self,
        message: discord.Message,
        decision: ShieldDecision,
    ) -> tuple[Any, ...] | None:
        if decision.pack != "gif" or getattr(message, "guild", None) is None:
            return None
        channel_id = getattr(getattr(message, "channel", None), "id", None)
        if not isinstance(channel_id, int):
            return None
        top_reason = self._primary_alert_reason(decision)
        if top_reason is not None and top_reason.match_class == "spam_group_gif_pressure":
            return ("channel", message.guild.id, channel_id)
        author_id = getattr(getattr(message, "author", None), "id", None)
        if not isinstance(author_id, int) or author_id <= 0:
            return None
        return ("member", message.guild.id, channel_id, author_id)

    def _spam_incident_family(self, top_reason: ShieldMatch | None) -> str | None:
        if top_reason is None:
            return None
        if top_reason.match_class == "spam_duplicate":
            return "duplicate"
        if top_reason.match_class == "spam_near_duplicate":
            return "near_duplicate"
        if top_reason.match_class in {"spam_message_rate", "spam_burst"}:
            return "rate_burst"
        if top_reason.match_class in {"spam_link_flood", "spam_invite_flood"}:
            return "links_invites"
        if top_reason.match_class == "spam_mention_flood":
            return "mentions"
        if top_reason.match_class in {"spam_emoji_flood", "spam_caps_flood"}:
            return "emoji_caps"
        if top_reason.match_class in {"spam_low_value_noise", "spam_padding_noise"}:
            return "low_value"
        return None

    def _spam_incident_key(
        self,
        message: discord.Message,
        decision: ShieldDecision,
    ) -> tuple[Any, ...] | None:
        if decision.pack != "spam" or getattr(message, "guild", None) is None:
            return None
        channel_id = getattr(getattr(message, "channel", None), "id", None)
        author_id = getattr(getattr(message, "author", None), "id", None)
        if not isinstance(channel_id, int) or not isinstance(author_id, int) or author_id <= 0:
            return None
        family = self._spam_incident_family(self._primary_alert_reason(decision))
        if family is None:
            return None
        return (message.guild.id, channel_id, author_id, family)

    def _spam_incident_note(self, hits: int) -> str:
        return (
            f"Grouped with **{hits}** anti-spam alerts from this member inside the current "
            f"{int(SPAM_INCIDENT_WINDOW_SECONDS)}s incident window. Babblebox kept this to one evolving incident log."
        )

    def _spam_incident_rank(self, decision: ShieldDecision, top_reason: ShieldMatch | None) -> int:
        if decision.timed_out:
            return 4
        if decision.deleted:
            return 3
        if top_reason is not None and top_reason.confidence == "high":
            return 2
        if top_reason is not None and top_reason.confidence == "medium":
            return 1
        return 0

    def _gif_incident_note(self, incident_key: tuple[Any, ...], hits: int) -> str:
        if incident_key and incident_key[0] == "channel":
            return (
                f"Grouped with **{hits}** channel-level GIF-pressure alerts inside the current "
                f"{int(GIF_INCIDENT_WINDOW_SECONDS)}s alert-grouping window. Babblebox kept this collective and only used "
                "channel-safe GIF cleanup instead of punishing members on the group signal alone."
            )
        return (
            f"Grouped with **{hits}** GIF-heavy alerts from this member inside the current "
            f"{int(GIF_INCIDENT_WINDOW_SECONDS)}s alert-grouping window. Severity was raised as the incident continued."
        )

    def _gif_incident_rank(self, decision: ShieldDecision, top_reason: ShieldMatch | None) -> int:
        if decision.timed_out:
            return 4
        if decision.deleted:
            return 3
        if top_reason is not None and top_reason.confidence == "high":
            return 2
        if top_reason is not None and top_reason.confidence == "medium":
            return 1
        return 0

    def _alert_action_records(self) -> dict[str, dict[str, Any]]:
        records = self.store.state.setdefault("alert_actions", {})
        if not isinstance(records, dict):
            records = {}
            self.store.state["alert_actions"] = records
        return records

    def _alert_record_expired(self, record: dict[str, Any]) -> bool:
        expires_at = deserialize_datetime(record.get("expires_at"))
        return expires_at is not None and expires_at <= ge.now_utc()

    def _purge_expired_alert_action_records(self) -> bool:
        records = self._alert_action_records()
        expired = [token for token, record in records.items() if self._alert_record_expired(record)]
        for token in expired:
            records.pop(token, None)
            self._alert_action_message_refs.pop(token, None)
        return bool(expired)

    async def purge_expired_alert_action_records(self):
        if self._purge_expired_alert_action_records():
            await self.store.flush()

    def active_alert_action_records(self) -> list[dict[str, Any]]:
        self._purge_expired_alert_action_records()
        return sorted(
            (dict(record) for record in self._alert_action_records().values() if not self._alert_record_expired(record)),
            key=lambda item: str(item.get("created_at") or ""),
        )

    def build_alert_action_view(self, record: dict[str, Any]) -> Any:
        with contextlib.suppress(RuntimeError):
            loop = asyncio.get_running_loop()
            if not hasattr(loop, "create_future"):
                return _StaticShieldAlertActionView(record)
        return ShieldAlertActionView(self, record)

    def _new_alert_action_record(
        self,
        message: discord.Message,
        decision: ShieldDecision,
        *,
        top_reason: ShieldMatch | None,
    ) -> dict[str, Any]:
        token = secrets.token_urlsafe(16)
        now = ge.now_utc()
        destructive = bool(decision.deleted or decision.timed_out)
        return {
            "token": token,
            "guild_id": int(getattr(message.guild, "id", 0) or 0),
            "log_channel_id": None,
            "alert_message_id": None,
            "target_channel_id": int(getattr(getattr(message, "channel", None), "id", 0) or 0) or None,
            "target_message_id": int(getattr(message, "id", 0) or 0) or None,
            "target_user_id": int(getattr(getattr(message, "author", None), "id", 0) or 0) or None,
            "pack": decision.pack,
            "action": decision.action,
            "match_class": top_reason.match_class if top_reason is not None else None,
            "jump_url": getattr(message, "jump_url", None),
            "deleted_by_shield": bool(decision.deleted),
            "timed_out_by_shield": bool(decision.timed_out),
            "recovery_content": (str(getattr(message, "content", "") or "")[:8000] if destructive else None),
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(seconds=SHIELD_ALERT_ACTION_TTL_SECONDS)).isoformat(),
            "used": False,
            "used_at": None,
            "status": None,
        }

    async def _save_alert_action_record(self, record: dict[str, Any]):
        self._purge_expired_alert_action_records()
        token = str(record.get("token") or "")
        if not token:
            return
        self._alert_action_records()[token] = record
        await self.store.flush()

    def _alert_action_record_for_log_message(self, alert_message_id: int | None) -> dict[str, Any] | None:
        if not isinstance(alert_message_id, int) or alert_message_id <= 0:
            return None
        for record in self._alert_action_records().values():
            if isinstance(record, dict) and int(record.get("alert_message_id") or 0) == alert_message_id:
                return record
        return None

    async def _refresh_incident_alert_action_record(
        self,
        incident_state: dict[str, Any],
        message: discord.Message,
        decision: ShieldDecision,
        *,
        top_reason: ShieldMatch | None,
    ):
        token = str(incident_state.get("action_token") or "")
        record = self._get_alert_action_record(token) if token else None
        if record is None:
            record = self._alert_action_record_for_log_message(incident_state.get("log_message_id"))
        if record is None:
            return
        destructive = bool(decision.deleted or decision.timed_out)
        record.update(
            {
                "target_channel_id": int(getattr(getattr(message, "channel", None), "id", 0) or 0) or None,
                "target_message_id": int(getattr(message, "id", 0) or 0) or None,
                "target_user_id": int(getattr(getattr(message, "author", None), "id", 0) or 0) or None,
                "pack": decision.pack,
                "action": decision.action,
                "match_class": top_reason.match_class if top_reason is not None else None,
                "jump_url": getattr(message, "jump_url", None),
                "deleted_by_shield": bool(decision.deleted),
                "timed_out_by_shield": bool(decision.timed_out),
                "recovery_content": (str(getattr(message, "content", "") or "")[:8000] if destructive else None),
            }
        )
        token = str(record.get("token") or "")
        if token:
            self._alert_action_message_refs[token] = message
            incident_state["action_token"] = token
        await self._save_alert_action_record(record)

    def _get_alert_action_record(self, token: str) -> dict[str, Any] | None:
        record = self._alert_action_records().get(str(token or ""))
        if not isinstance(record, dict) or self._alert_record_expired(record):
            return None
        return record

    def _alert_actor_has_permission(self, actor: object, *permission_names: str) -> bool:
        permissions = getattr(actor, "guild_permissions", None)
        if bool(getattr(permissions, "administrator", False) or getattr(permissions, "manage_guild", False)):
            return True
        return any(bool(getattr(permissions, name, False)) for name in permission_names)

    async def _resolve_alert_member(self, guild: discord.Guild | None, user_id: int | None):
        if guild is None or not isinstance(user_id, int) or user_id <= 0:
            return None
        member = guild.get_member(user_id) if hasattr(guild, "get_member") else None
        if member is not None:
            return member
        if hasattr(guild, "fetch_member"):
            with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                return await guild.fetch_member(user_id)
        return None

    def _can_clear_alert_timeout(self, guild: discord.Guild | None, moderator: object, member: object | None) -> bool:
        if member is None or not self._alert_actor_has_permission(moderator, "moderate_members"):
            return False
        me = self._guild_member(guild, getattr(self.bot, "user", None)) if guild is not None else None
        if me is None:
            return False
        member_top = getattr(member, "top_role", None)
        me_top = getattr(me, "top_role", None)
        if member_top is not None and me_top is not None and member_top >= me_top:
            return False
        return True

    async def _resolve_alert_message(self, record: dict[str, Any]):
        token = str(record.get("token") or "")
        if token in self._alert_action_message_refs:
            return self._alert_action_message_refs[token]
        channel_id = record.get("target_channel_id")
        message_id = record.get("target_message_id")
        if not isinstance(channel_id, int) or not isinstance(message_id, int):
            return None
        channel = self.bot.get_channel(channel_id) if hasattr(self.bot, "get_channel") else None
        if channel is None and hasattr(self.bot, "fetch_channel"):
            with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                channel = await self.bot.fetch_channel(channel_id)
        if channel is None or not hasattr(channel, "fetch_message"):
            return None
        with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException, KeyError):
            return await channel.fetch_message(message_id)
        return None

    def _message_recovery_codeblock(self, content: str) -> str:
        safe = (content or "[no text content]").replace("```", "`\u200b``")
        return f"```\n{safe}\n```"

    async def _dm_false_positive_recovery(self, member: object, guild: discord.Guild | None, content: str):
        embed = discord.Embed(
            title="Shield False Positive",
            description=(
                "A server moderator marked a Babblebox Shield action as a false positive. "
                "Sorry about that. Your message text is below so you can repost it if needed."
            ),
            color=ge.EMBED_THEME["success"],
        )
        first_chunk = content[:950] if content else "[no text content]"
        embed.add_field(name="Your message", value=self._message_recovery_codeblock(first_chunk), inline=False)
        guild_name = getattr(guild, "name", None)
        ge.style_embed(embed, footer=f"Babblebox Shield | {guild_name}" if guild_name else "Babblebox Shield")
        await member.send(embed=embed)
        remaining = content[950:]
        while remaining:
            chunk = remaining[:1800]
            remaining = remaining[1800:]
            await member.send(content=self._message_recovery_codeblock(chunk))

    def _false_positive_fallback_text(self, member: object | None, content: str) -> str:
        mention = getattr(member, "mention", "the member")
        return (
            "DMs were closed or unavailable. With moderator discretion, you can send this apology manually:\n\n"
            f"Hi {mention}, Babblebox Shield removed or acted on your message by mistake. Sorry about that. "
            "Here is the message text so you can repost it if needed:\n"
            f"{self._message_recovery_codeblock(content)}"
        )

    async def handle_false_positive_recovery(self, token: str, *, moderator: object, guild: discord.Guild | None) -> dict[str, Any]:
        record = self._get_alert_action_record(token)
        if record is None:
            return {"ok": False, "reason": "That Shield alert action has expired or no longer exists."}
        if not self._alert_actor_has_permission(moderator, "manage_messages", "moderate_members"):
            return {"ok": False, "reason": "You need Manage Messages, Timeout Members, Manage Server, or administrator access."}
        member = await self._resolve_alert_member(guild, record.get("target_user_id"))
        destructive_record = bool(record.get("deleted_by_shield") or record.get("timed_out_by_shield") or record.get("deleted_by_moderator"))
        content = str(record.get("recovery_content") or "")
        if destructive_record and not content:
            message = await self._resolve_alert_message(record)
            content = str(getattr(message, "content", "") or "")
        timeout_removed = False
        if bool(record.get("timed_out_by_shield")) and member is not None and self._can_clear_alert_timeout(guild, moderator, member):
            with contextlib.suppress(discord.Forbidden, discord.HTTPException):
                await member.timeout(None, reason="Babblebox Shield false-positive recovery.")
                timeout_removed = True
        dm_sent = False
        moderator_fallback = None
        if member is not None and content:
            try:
                await self._dm_false_positive_recovery(member, guild, content)
                dm_sent = True
            except Exception:
                moderator_fallback = self._false_positive_fallback_text(member, content)
        elif content:
            moderator_fallback = self._false_positive_fallback_text(member, content)
        record["used"] = True
        record["used_at"] = ge.now_utc().isoformat()
        record["moderator_user_id"] = int(getattr(moderator, "id", 0) or 0) or None
        record["status"] = "false_positive_dm_sent" if dm_sent else "false_positive_dm_fallback"
        if destructive_record and content:
            record["recovery_content"] = content[:8000]
        await self._save_alert_action_record(record)
        note_bits = [
            "False positive recorded.",
            "Timeout removed." if timeout_removed else "No active timeout was removed.",
            "The member was DMed with their message copy." if dm_sent else "Use the private fallback copy if the member should be notified.",
            f"Reporting this in the support server is appreciated and important for improving Shield: {SUPPORT_SERVER_URL}",
        ]
        return {
            "ok": True,
            "timeout_removed": timeout_removed,
            "dm_sent": dm_sent,
            "moderator_note": " ".join(note_bits),
            "moderator_fallback": moderator_fallback,
            "record": dict(record),
        }

    async def handle_alert_delete_message(self, token: str, *, moderator: object, guild: discord.Guild | None) -> dict[str, Any]:
        record = self._get_alert_action_record(token)
        if record is None:
            return {"ok": False, "reason": "That Shield alert action has expired or no longer exists."}
        if not self._alert_actor_has_permission(moderator, "manage_messages"):
            return {"ok": False, "reason": "You need Manage Messages, Manage Server, or administrator access to delete the message."}
        message = await self._resolve_alert_message(record)
        if message is None:
            return {"ok": False, "reason": "Babblebox could not find the original message. It may already be deleted."}
        content = str(getattr(message, "content", "") or "")
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            return {"ok": False, "reason": "Babblebox could not delete that message. Check bot channel permissions."}
        record["recovery_content"] = content[:8000]
        record["deleted_by_moderator"] = True
        record["status"] = "moderator_deleted"
        await self._save_alert_action_record(record)
        return {"ok": True, "reason": "Message deleted. A short-lived recovery copy is available for false-positive repair.", "record": dict(record)}

    async def handle_alert_remove_timeout(self, token: str, *, moderator: object, guild: discord.Guild | None) -> dict[str, Any]:
        record = self._get_alert_action_record(token)
        if record is None:
            return {"ok": False, "reason": "That Shield alert action has expired or no longer exists."}
        member = await self._resolve_alert_member(guild, record.get("target_user_id"))
        if not self._can_clear_alert_timeout(guild, moderator, member):
            return {"ok": False, "reason": "You need Timeout Members, Manage Server, or administrator access, and role hierarchy must allow it."}
        try:
            await member.timeout(None, reason="Babblebox Shield timeout removed by moderator alert action.")
        except (discord.Forbidden, discord.HTTPException):
            return {"ok": False, "reason": "Babblebox could not remove that timeout. Check bot hierarchy and Timeout Members permission."}
        record["status"] = "timeout_removed"
        await self._save_alert_action_record(record)
        return {"ok": True, "reason": "Timeout removed.", "record": dict(record)}

    async def _send_alert_action_interaction_result(self, interaction: discord.Interaction, result: dict[str, Any], *, title: str):
        ok = bool(result.get("ok"))
        embed = ge.make_status_embed(
            title,
            str(result.get("moderator_note") or result.get("reason") or "Done."),
            tone="success" if ok else "warning",
            footer="Babblebox Shield",
        )
        fallback = result.get("moderator_fallback")
        fallback_chunks: list[str] = []
        if isinstance(fallback, str) and fallback:
            embed.add_field(
                name="Moderator Fallback",
                value=(fallback[:1000] + "\n[continued privately]" if len(fallback) > 1000 else fallback),
                inline=False,
            )
            if len(fallback) > 1000:
                fallback_chunks = [fallback[index : index + 1900] for index in range(0, len(fallback), 1900)]
        view = discord.ui.View()
        view.add_item(discord.ui.Button(label="Support Server", style=discord.ButtonStyle.link, url=SUPPORT_SERVER_URL))
        kwargs = {"embed": embed, "ephemeral": True}
        if ok:
            kwargs["view"] = view
        if interaction.response.is_done():
            await interaction.followup.send(**kwargs)
        else:
            await interaction.response.send_message(**kwargs)
        for chunk in fallback_chunks:
            with contextlib.suppress(discord.HTTPException, discord.NotFound):
                await interaction.followup.send(content=chunk, ephemeral=True)

    async def _update_alert_log_interaction_message(self, interaction: discord.Interaction, result: dict[str, Any], *, title: str):
        message = getattr(interaction, "message", None)
        if message is None:
            return
        embed = getattr(message, "embed", None)
        if embed is None:
            return
        status = str(result.get("moderator_note") or result.get("reason") or title)
        if len(embed.fields) < 25:
            embed.add_field(name=title, value=status[:1024], inline=False)
        record = result.get("record")
        view = self.build_alert_action_view(record) if isinstance(record, dict) else None
        with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
            await message.edit(embed=embed, view=view)

    async def handle_alert_false_positive_interaction(self, interaction: discord.Interaction, token: str):
        result = await self.handle_false_positive_recovery(token, moderator=interaction.user, guild=interaction.guild)
        await self._update_alert_log_interaction_message(interaction, result, title="False Positive Recovery")
        await self._send_alert_action_interaction_result(interaction, result, title="False Positive Recovery")

    async def handle_alert_delete_interaction(self, interaction: discord.Interaction, token: str):
        record = self._get_alert_action_record(token)
        if record is None or not self._alert_actor_has_permission(interaction.user, "manage_messages"):
            result = await self.handle_alert_delete_message(token, moderator=interaction.user, guild=interaction.guild)
            await self._send_alert_action_interaction_result(interaction, result, title="Shield Alert Action")
            return
        embed = ge.make_status_embed(
            "Confirm Delete",
            "This deletes the original message if it still exists and keeps a short-lived recovery copy for false-positive repair.",
            tone="warning",
            footer="Babblebox Shield",
        )
        view = ShieldAlertDeleteConfirmView(self, token)
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def handle_alert_remove_timeout_interaction(self, interaction: discord.Interaction, token: str):
        result = await self.handle_alert_remove_timeout(token, moderator=interaction.user, guild=interaction.guild)
        await self._update_alert_log_interaction_message(interaction, result, title="Timeout Removed")
        await self._send_alert_action_interaction_result(interaction, result, title="Shield Alert Action")

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
        top_reason = self._primary_alert_reason(decision)
        compact_alert = self._should_use_compact_alert(compiled, decision, top_reason)
        ping_alert_role = self._should_ping_alert_role(compiled, decision, top_reason)
        delivery_note = self._alert_delivery_note(
            compiled,
            decision,
            top_reason,
            compact_alert=compact_alert,
            ping_alert_role=ping_alert_role,
        )
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
        signature_seen_at = self._alert_signature_dedup.get(signature_key)
        if signature_seen_at is not None and now - signature_seen_at < ALERT_SIGNATURE_DEDUP_SECONDS:
            return
        self._alert_dedup[dedupe_key] = (now, content_fingerprint)
        self._alert_signature_dedup[signature_key] = now
        gif_incident_key = self._gif_incident_key(message, decision)
        spam_incident_key = self._spam_incident_key(message, decision)

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
                compiled,
                decision,
                preview=preview,
                attachment_summary=attachment_summary,
                top_reason=top_reason,
            )
        else:
            alert_title = f"Shield Alert | {PACK_LABELS.get(decision.pack or '', 'Shield')}"
            embed = discord.Embed(
                title=alert_title,
                description=self._alert_location_description(message, top_reason),
                color=ge.EMBED_THEME["danger"] if decision.deleted or decision.timed_out else ge.EMBED_THEME["warning"],
            )
            if top_reason is not None:
                embed.add_field(
                    name="Detection",
                    value=(
                        f"**{top_reason.label}**\n"
                        f"Pack: {PACK_LABELS.get(top_reason.pack, top_reason.pack.title())}\n"
                        f"Class: {_match_class_label(top_reason.match_class)}\n"
                        f"Confidence: {CONFIDENCE_LABELS.get(top_reason.confidence, top_reason.confidence.title())}\n"
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
            if evidence_lines:
                embed.add_field(name="Evidence Basis", value="\n".join(evidence_lines), inline=False)
            source_summary = SCAN_SOURCE_LABELS.get(decision.scan_source, decision.scan_source.replace("_", " ").title())
            if decision.scan_surface_labels:
                source_summary = f"{source_summary} | Surfaces: {', '.join(decision.scan_surface_labels)}"
            embed.add_field(name="Scan Source", value=source_summary, inline=False)
            embed.add_field(name="Action", value=self._format_action_summary(decision), inline=False)
            if delivery_note:
                embed.add_field(name="Alert Delivery", value=delivery_note, inline=False)
            embed.add_field(name="Reason", value="\n".join(f"- {item.reason}" for item in decision.reasons[:3]), inline=False)
            embed.add_field(name="Preview", value=preview or "[no text content]", inline=False)
            if attachment_summary:
                embed.add_field(name="Attachments", value="\n".join(attachment_summary[:4]), inline=False)
            if decision.link_explanations and top_reason is not None and top_reason.pack in {"scam", "adult", "link_policy"}:
                embed.add_field(
                    name="Link Decisions",
                    value=_format_link_decision_lines(decision.link_explanations, limit=3),
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
                embed.add_field(name="Note", value="Scam detection can still be wrong; recheck the context before taking action.", inline=True)
            if decision.action_note:
                embed.add_field(name="Operational Note", value=decision.action_note, inline=False)
            ge.style_embed(embed, footer="Babblebox Shield | No long-term archive; destructive actions keep a short recovery copy")

        if spam_incident_key is not None:
            incident_state = self._spam_incident_alerts.get(spam_incident_key)
            if incident_state is not None and now - float(incident_state.get("last_seen", 0.0)) <= SPAM_INCIDENT_WINDOW_SECONDS:
                hits = int(incident_state.get("hits", 1)) + 1
                new_severity_rank = self._spam_incident_rank(decision, top_reason)
                severity_increased = new_severity_rank > int(incident_state.get("severity_rank", -1))
                stronger = (
                    severity_increased
                    or bool(decision.deleted) != bool(incident_state.get("deleted"))
                    or bool(decision.timed_out) != bool(incident_state.get("timed_out"))
                    or decision.alert_evidence_signature != incident_state.get("signature")
                )
                incident_state["last_seen"] = now
                incident_state["hits"] = hits
                incident_state["signature"] = decision.alert_evidence_signature
                incident_state["deleted"] = bool(decision.deleted)
                incident_state["timed_out"] = bool(decision.timed_out)
                incident_state["severity_rank"] = new_severity_rank
                if not stronger:
                    self._spam_incident_alerts[spam_incident_key] = incident_state
                    decision.logged = True
                    return
                if not severity_increased and now - float(incident_state.get("last_edit_at", 0.0)) < INCIDENT_ALERT_EDIT_MIN_SECONDS:
                    self._spam_incident_alerts[spam_incident_key] = incident_state
                    decision.logged = True
                    return
                embed.add_field(name="Incident", value=self._spam_incident_note(hits), inline=False)
                existing_message_id = incident_state.get("log_message_id")
                if isinstance(existing_message_id, int):
                    with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                        existing_message = await channel.fetch_message(existing_message_id)
                        await existing_message.edit(
                            content=None,
                            embed=embed,
                            allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                        )
                        await self._refresh_incident_alert_action_record(
                            incident_state,
                            message,
                            decision,
                            top_reason=top_reason,
                        )
                        incident_state["last_edit_at"] = now
                        self._spam_incident_alerts[spam_incident_key] = incident_state
                        decision.logged = True
                        return
            elif incident_state is not None:
                self._spam_incident_alerts.pop(spam_incident_key, None)

        if gif_incident_key is not None:
            incident_state = self._gif_incident_alerts.get(gif_incident_key)
            if incident_state is not None and now - float(incident_state.get("last_seen", 0.0)) <= GIF_INCIDENT_WINDOW_SECONDS:
                hits = int(incident_state.get("hits", 1)) + 1
                channel_level_incident = bool(gif_incident_key and gif_incident_key[0] == "channel")
                new_severity_rank = self._gif_incident_rank(decision, top_reason)
                severity_increased = new_severity_rank > int(incident_state.get("severity_rank", -1))
                stronger = (
                    severity_increased
                    or (
                        not channel_level_incident
                        and decision.alert_evidence_signature != incident_state.get("signature")
                    )
                    or bool(decision.deleted) != bool(incident_state.get("deleted"))
                    or bool(decision.timed_out) != bool(incident_state.get("timed_out"))
                )
                incident_state["last_seen"] = now
                incident_state["hits"] = hits
                incident_state["signature"] = decision.alert_evidence_signature
                incident_state["deleted"] = bool(decision.deleted)
                incident_state["timed_out"] = bool(decision.timed_out)
                incident_state["severity_rank"] = new_severity_rank
                if not stronger:
                    self._gif_incident_alerts[gif_incident_key] = incident_state
                    decision.logged = True
                    return
                if not severity_increased and now - float(incident_state.get("last_edit_at", 0.0)) < INCIDENT_ALERT_EDIT_MIN_SECONDS:
                    self._gif_incident_alerts[gif_incident_key] = incident_state
                    decision.logged = True
                    return
                embed.add_field(
                    name="Incident",
                    value=self._gif_incident_note(gif_incident_key, hits),
                    inline=False,
                )
                existing_message_id = incident_state.get("log_message_id")
                if isinstance(existing_message_id, int):
                    with contextlib.suppress(discord.Forbidden, discord.NotFound, discord.HTTPException):
                        existing_message = await channel.fetch_message(existing_message_id)
                        await existing_message.edit(
                            content=None,
                            embed=embed,
                            allowed_mentions=discord.AllowedMentions(users=False, roles=False, everyone=False),
                        )
                        await self._refresh_incident_alert_action_record(
                            incident_state,
                            message,
                            decision,
                            top_reason=top_reason,
                        )
                        incident_state["last_edit_at"] = now
                        self._gif_incident_alerts[gif_incident_key] = incident_state
                        decision.logged = True
                        return
            elif incident_state is not None:
                self._gif_incident_alerts.pop(gif_incident_key, None)

        content = f"<@&{compiled.alert_role_id}>" if compiled.alert_role_id is not None and ping_alert_role else None
        allowed_mentions = discord.AllowedMentions(users=False, roles=True, everyone=False)
        sent_message = None
        action_record = self._new_alert_action_record(message, decision, top_reason=top_reason)
        action_record["log_channel_id"] = int(getattr(channel, "id", 0) or compiled.log_channel_id)
        action_view = self.build_alert_action_view(action_record)
        with contextlib.suppress(discord.Forbidden, discord.HTTPException):
            sent_message = await channel.send(content=content, embed=embed, allowed_mentions=allowed_mentions, view=action_view)
        if sent_message is None:
            return
        decision.logged = True
        action_record["alert_message_id"] = int(getattr(sent_message, "id", 0) or 0) or None
        self._alert_action_message_refs[str(action_record["token"])] = message
        await self._save_alert_action_record(action_record)
        if spam_incident_key is not None:
            self._spam_incident_alerts[spam_incident_key] = {
                "last_seen": now,
                "hits": 1,
                "log_message_id": int(getattr(sent_message, "id", 0) or 0),
                "severity_rank": self._spam_incident_rank(decision, top_reason),
                "signature": decision.alert_evidence_signature,
                "deleted": bool(decision.deleted),
                "timed_out": bool(decision.timed_out),
                "action_token": str(action_record["token"]),
                "last_edit_at": now,
            }
        if gif_incident_key is not None:
            self._gif_incident_alerts[gif_incident_key] = {
                "last_seen": now,
                "hits": 1,
                "log_message_id": int(getattr(sent_message, "id", 0) or 0),
                "severity_rank": self._gif_incident_rank(decision, top_reason),
                "signature": decision.alert_evidence_signature,
                "deleted": bool(decision.deleted),
                "timed_out": bool(decision.timed_out),
                "action_token": str(action_record["token"]),
                "last_edit_at": now,
            }

    def _format_action_summary(self, decision: ShieldDecision) -> str:
        parts = [ACTION_LABELS.get(decision.action, decision.action)]
        if decision.deleted:
            if decision.delete_attempt_count > 1:
                parts.append(f"Deleted {decision.deleted_count}/{decision.delete_attempt_count} messages")
            else:
                parts.append("Message deleted")
        elif decision.action.startswith("delete"):
            parts.append("Delete not performed")
        if decision.timed_out:
            parts.append("Member timed out")
        elif decision.action in {"timeout_log", "delete_timeout_log", "delete_escalate"} and decision.action_note:
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
        filter_limit = self.filter_limit(guild_id)
        allowlist_limit = self.allowlist_limit(guild_id)
        pack_exemption_limit = self.pack_exemption_limit(guild_id)
        severe_term_limit = min(self.severe_term_limit(guild_id), SHIELD_SEVERE_COMPILED_LIMIT)
        custom_pattern_limit = self.custom_pattern_limit(guild_id)
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
        custom_patterns = custom_patterns[:custom_pattern_limit]

        link_policy_mode = str(raw.get("link_policy_mode", DEFAULT_SHIELD_LINK_POLICY_MODE)).strip().lower()
        if link_policy_mode not in VALID_SHIELD_LINK_POLICY_MODES:
            link_policy_mode = DEFAULT_SHIELD_LINK_POLICY_MODE
        raw_pack_exemptions = raw.get("pack_exemptions", {})
        compiled_pack_exemptions = {
            pack: PackExemptionScope(
                channel_ids=frozenset(_sorted_unique_ints(((raw_pack_exemptions.get(pack) or {}).get("channel_ids", [])) if isinstance(raw_pack_exemptions, dict) else [])[:pack_exemption_limit]),
                role_ids=frozenset(_sorted_unique_ints(((raw_pack_exemptions.get(pack) or {}).get("role_ids", [])) if isinstance(raw_pack_exemptions, dict) else [])[:pack_exemption_limit]),
                user_ids=frozenset(_sorted_unique_ints(((raw_pack_exemptions.get(pack) or {}).get("user_ids", [])) if isinstance(raw_pack_exemptions, dict) else [])[:pack_exemption_limit]),
            )
            for pack in RULE_PACKS
        }
        raw_pack_timeout_minutes = raw.get("pack_timeout_minutes", {})
        compiled_pack_timeout_minutes = {
            pack: value
            for pack in PACK_TIMEOUT_PACKS
            for value in [raw_pack_timeout_minutes.get(pack) if isinstance(raw_pack_timeout_minutes, dict) else None]
            if isinstance(value, int) and 1 <= value <= 60
        }
        raw_pack_log_overrides = raw.get("pack_log_overrides", {})
        compiled_pack_log_overrides: dict[str, ShieldPackLogOverride] = {}
        if isinstance(raw_pack_log_overrides, dict):
            for pack in RULE_PACKS:
                raw_override = raw_pack_log_overrides.get(pack)
                if not isinstance(raw_override, dict):
                    continue
                style = str(raw_override.get("style", "inherit")).strip().lower()
                ping_mode = str(raw_override.get("ping_mode", "inherit")).strip().lower()
                compiled_pack_log_overrides[pack] = ShieldPackLogOverride(
                    style=style if style in {"inherit", *VALID_SHIELD_LOG_STYLES} else "inherit",
                    ping_mode=ping_mode if ping_mode in {"inherit", *VALID_SHIELD_LOG_PING_MODES} else "inherit",
                )

        return CompiledShieldConfig(
            guild_id=guild_id,
            module_enabled=bool(raw.get("module_enabled")),
            log_channel_id=raw.get("log_channel_id") if isinstance(raw.get("log_channel_id"), int) else None,
            alert_role_id=raw.get("alert_role_id") if isinstance(raw.get("alert_role_id"), int) else None,
            log_style=(
                str(raw.get("log_style", "adaptive")).strip().lower()
                if str(raw.get("log_style", "adaptive")).strip().lower() in VALID_SHIELD_LOG_STYLES
                else "adaptive"
            ),
            log_ping_mode=(
                str(raw.get("log_ping_mode", "smart")).strip().lower()
                if str(raw.get("log_ping_mode", "smart")).strip().lower() in VALID_SHIELD_LOG_PING_MODES
                else "smart"
            ),
            scan_mode=raw.get("scan_mode", "all"),
            included_channel_ids=frozenset(_sorted_unique_ints(raw.get("included_channel_ids", []))[:filter_limit]),
            excluded_channel_ids=frozenset(_sorted_unique_ints(raw.get("excluded_channel_ids", []))[:filter_limit]),
            included_user_ids=frozenset(_sorted_unique_ints(raw.get("included_user_ids", []))[:filter_limit]),
            excluded_user_ids=frozenset(_sorted_unique_ints(raw.get("excluded_user_ids", []))[:filter_limit]),
            included_role_ids=frozenset(_sorted_unique_ints(raw.get("included_role_ids", []))[:filter_limit]),
            excluded_role_ids=frozenset(_sorted_unique_ints(raw.get("excluded_role_ids", []))[:filter_limit]),
            trusted_role_ids=frozenset(_sorted_unique_ints(raw.get("trusted_role_ids", []))[:filter_limit]),
            allow_domains=frozenset(_sorted_unique_text(raw.get("allow_domains", []))[:allowlist_limit]),
            allow_invite_codes=frozenset(_sorted_unique_text(raw.get("allow_invite_codes", []))[:allowlist_limit]),
            allow_phrases=tuple(_sorted_unique_text(raw.get("allow_phrases", []))[:allowlist_limit]),
            trusted_builtin_disabled_families=frozenset(_sorted_unique_text(raw.get("trusted_builtin_disabled_families", []))),
            trusted_builtin_disabled_domains=frozenset(_sorted_unique_text(raw.get("trusted_builtin_disabled_domains", []))),
            pack_exemptions=compiled_pack_exemptions,
            pack_log_overrides=compiled_pack_log_overrides,
            pack_timeout_minutes=compiled_pack_timeout_minutes,
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
            spam_rules=SpamRuleSettings(
                message_enabled=bool(raw.get("spam_message_enabled", True)),
                message_threshold=int(raw.get("spam_message_threshold", shield_numeric_config_default("spam_message_threshold"))),
                message_window_seconds=int(
                    raw.get("spam_message_window_seconds", shield_numeric_config_default("spam_message_window_seconds"))
                ),
                burst_enabled=bool(raw.get("spam_burst_enabled", True)),
                burst_threshold=int(raw.get("spam_burst_threshold", shield_numeric_config_default("spam_burst_threshold"))),
                burst_window_seconds=int(
                    raw.get("spam_burst_window_seconds", shield_numeric_config_default("spam_burst_window_seconds"))
                ),
                near_duplicate_enabled=bool(raw.get("spam_near_duplicate_enabled", True)),
                near_duplicate_threshold=int(
                    raw.get("spam_near_duplicate_threshold", shield_numeric_config_default("spam_near_duplicate_threshold"))
                ),
                near_duplicate_window_seconds=int(
                    raw.get(
                        "spam_near_duplicate_window_seconds",
                        shield_numeric_config_default("spam_near_duplicate_window_seconds"),
                    )
                ),
                emote_enabled=bool(raw.get("spam_emote_enabled")),
                emote_threshold=int(raw.get("spam_emote_threshold", shield_numeric_config_default("spam_emote_threshold"))),
                caps_enabled=bool(raw.get("spam_caps_enabled")),
                caps_threshold=int(raw.get("spam_caps_threshold", shield_numeric_config_default("spam_caps_threshold"))),
                low_value_enabled=bool(raw.get("spam_low_value_enabled")),
                low_value_threshold=int(raw.get("spam_low_value_threshold", shield_numeric_config_default("spam_low_value_threshold"))),
                low_value_window_seconds=int(
                    raw.get("spam_low_value_window_seconds", shield_numeric_config_default("spam_low_value_window_seconds"))
                ),
                moderator_policy=(
                    str(raw.get("spam_moderator_policy", "exempt")).strip().lower()
                    if str(raw.get("spam_moderator_policy", "exempt")).strip().lower() in VALID_SPAM_MODERATOR_POLICIES
                    else "exempt"
                ),
            ),
            gif=PackSettings(
                enabled=bool(raw.get("gif_enabled")),
                low_action=str(raw.get("gif_low_action", "log")).strip().lower(),
                medium_action=str(raw.get("gif_medium_action", "log")).strip().lower(),
                high_action=str(raw.get("gif_high_action", "log")).strip().lower(),
                sensitivity=str(raw.get("gif_sensitivity", "normal")).strip().lower(),
            ),
            gif_rules=GifRuleSettings(
                message_enabled=bool(raw.get("gif_message_enabled", True)),
                message_threshold=int(raw.get("gif_message_threshold", shield_numeric_config_default("gif_message_threshold"))),
                window_seconds=int(raw.get("gif_window_seconds", shield_numeric_config_default("gif_window_seconds"))),
                consecutive_enabled=bool(raw.get("gif_consecutive_enabled", True)),
                consecutive_threshold=int(
                    raw.get("gif_consecutive_threshold", shield_numeric_config_default("gif_consecutive_threshold"))
                ),
                repeat_enabled=bool(raw.get("gif_repeat_enabled", True)),
                repeat_threshold=int(raw.get("gif_repeat_threshold", shield_numeric_config_default("gif_repeat_threshold"))),
                same_asset_enabled=bool(raw.get("gif_same_asset_enabled", True)),
                same_asset_threshold=int(
                    raw.get("gif_same_asset_threshold", shield_numeric_config_default("gif_same_asset_threshold"))
                ),
                min_ratio_percent=int(raw.get("gif_min_ratio_percent", shield_numeric_config_default("gif_min_ratio_percent"))),
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
                _sorted_unique_ints(raw.get("adult_solicitation_excluded_channel_ids", []))[:filter_limit]
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
            severe_custom_terms=tuple(_sorted_unique_text(raw.get("severe_custom_terms", []))[:severe_term_limit]),
            severe_removed_terms=frozenset(_sorted_unique_text(raw.get("severe_removed_terms", []))[:severe_term_limit]),
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
            custom_patterns=tuple(custom_patterns),
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
