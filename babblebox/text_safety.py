from __future__ import annotations

import re
import unicodedata


URL_RE = re.compile(
    r"(?ix)"
    r"(?:https?://|ftp://|www\.)\S+"
    r"|(?:discord(?:app)?\.com/invite/|discord\.gg/)\S+"
    r"|(?:[a-z0-9-]+\.)+(?:com|net|org|gg|io|me|dev|app|xyz|info|co|ru|am|uk|de|fr|ca|us)(?:/\S*)?"
)
EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\.)+[A-Z]{2,}\b")
IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\.){3}(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)\b")
IPV6_RE = re.compile(r"(?i)\b(?:[A-F0-9]{1,4}:){2,7}[A-F0-9]{1,4}\b")
PHONE_RE = re.compile(r"(?<!\w)(?:\+|00|011)?\s*(?:\(?\d{1,4}\)?[\s.-]*)?(?:\d[\s.-]*){7,14}(?!\w)")
CARD_RE = re.compile(r"\b(?:\d[ -]?){13,19}\b")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
MENTION_RE = re.compile(r"(?i)@(?:everyone|here)|<@!?&?\d+>|<#\d+>")
MARKDOWN_LINK_RE = re.compile(r"\[[^\]]{1,80}\]\((?:[^)]+)\)")

BLOCKLIST = {
    "anal",
    "bdsm",
    "bitch",
    "blowjob",
    "boob",
    "boobs",
    "breast",
    "breasts",
    "cock",
    "cum",
    "dick",
    "fetish",
    "fuck",
    "fucked",
    "fucking",
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
    "pussy",
    "rape",
    "raped",
    "raping",
    "sex",
    "sexual",
    "sext",
    "shit",
    "slut",
    "vagina",
    "whore",
    "bulimia",
    "*child grooming*",
    "chink",
    "coon",
    "cripple",
    "dyke",
    "edtwt",
    "faggot",
    "gook",
    "kike",
    "killyourself",
    "kkk",
    "loli",
    "meanspo",
    "mongoloid",
    "n1gg@",
    "n1gg3r",
    "n1gga",
    "n1gger",
    "nazi",
    "nigga",
    "paki",
    "pedophile",
    "proana",
    "redroom",
    "retard",
    "shemale",
    "shota",
    "snuff",
    "spic",
    "terrorism",
    "thinspo",
    "tranny",
    "wetback",
}

PRIVATE_PATTERNS = (
    ("links or invites", URL_RE),
    ("email addresses", EMAIL_RE),
    ("IP addresses", IPV4_RE),
    ("IP addresses", IPV6_RE),
    ("phone numbers", PHONE_RE),
    ("card-like numbers", CARD_RE),
    ("sensitive ID numbers", SSN_RE),
    ("mentions", MENTION_RE),
    ("markdown links", MARKDOWN_LINK_RE),
)


def normalize_plain_text(text: str | None) -> str:
    cleaned = unicodedata.normalize("NFKC", text or "")
    cleaned = "".join(ch for ch in cleaned if unicodedata.category(ch) != "Cf")
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def squash_for_evasion_checks(text: str) -> str:
    return re.sub(r"(?<=\w)[\s`~*_.,-]+(?=\w)", "", text)


def find_private_pattern(text: str) -> str | None:
    squashed = squash_for_evasion_checks(text)
    for label, pattern in PRIVATE_PATTERNS:
        if pattern.search(text) or pattern.search(squashed):
            return label
    return None


def contains_blocklisted_term(text: str) -> bool:
    squashed = squash_for_evasion_checks(text)
    for blocked_word in BLOCKLIST:
        blocked_re = re.compile(rf"\b{re.escape(blocked_word)}\b", re.IGNORECASE)
        if blocked_re.search(text) or blocked_re.search(squashed):
            return True
    return False


def sanitize_short_plain_text(
    text: str | None,
    *,
    field_name: str,
    max_length: int,
    sentence_limit: int | None = None,
    reject_blocklist: bool = True,
    allow_empty: bool = True,
) -> tuple[bool, str | None]:
    cleaned = normalize_plain_text(text)

    if not cleaned:
        return (True, None) if allow_empty else (False, f"{field_name} cannot be empty.")

    if len(cleaned) > max_length:
        return False, f"{field_name} must be {max_length} characters or fewer."

    if sentence_limit is not None:
        sentence_parts = [part.strip() for part in re.split(r"[.!?]+", cleaned) if part.strip()]
        if len(sentence_parts) > sentence_limit:
            return False, f"{field_name} must be at most {sentence_limit} short sentences."

    label = find_private_pattern(cleaned)
    if label is not None:
        return False, f"{field_name} cannot contain {label}. Use short plain text only."

    if reject_blocklist and contains_blocklisted_term(cleaned):
        return False, f"{field_name} contains blocked or inappropriate words. Use short plain text only."

    return True, cleaned
