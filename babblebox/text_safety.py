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
TEMPLATE_PLACEHOLDER_RE = re.compile(r"\{[a-z]+(?:\.[a-z_]+)+\}")
TEMPLATE_BRACED_VALUE_RE = re.compile(r"\{[^{}]{1,80}\}")

CONFUSABLE_TRANSLATION = str.maketrans(
    {
        "@": "a",
        "$": "s",
        "0": "o",
        "1": "i",
        "3": "e",
        "4": "a",
        "5": "s",
        "7": "t",
        "8": "b",
        "а": "a",
        "е": "e",
        "і": "i",
        "к": "k",
        "м": "m",
        "о": "o",
        "р": "p",
        "с": "c",
        "т": "t",
        "у": "y",
        "х": "x",
        "α": "a",
        "β": "b",
        "ε": "e",
        "ι": "i",
        "κ": "k",
        "ο": "o",
        "ρ": "p",
        "τ": "t",
        "υ": "y",
        "χ": "x",
    }
)

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


def fold_confusable_text(text: str | None) -> str:
    cleaned = normalize_plain_text(text or "").casefold()
    return cleaned.translate(CONFUSABLE_TRANSLATION)


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


def extract_template_placeholders(text: str) -> list[str]:
    return [match.group(0) for match in TEMPLATE_BRACED_VALUE_RE.finditer(text)]


def sanitize_short_plain_template(
    text: str | None,
    *,
    field_name: str,
    max_length: int,
    allowed_placeholders: set[str],
    sentence_limit: int | None = None,
) -> tuple[bool, str | None, str | None]:
    cleaned = normalize_plain_text(text)

    if not cleaned:
        return False, None, f"{field_name} cannot be empty. Use `clear` to restore the default copy."

    if len(cleaned) > max_length:
        return False, None, f"{field_name} must be {max_length} characters or fewer."

    placeholders = extract_template_placeholders(cleaned)
    unknown = sorted({token for token in placeholders if token not in allowed_placeholders})
    if unknown:
        if len(unknown) == 1:
            return False, None, f"Unsupported placeholder: `{unknown[0]}`."
        rendered = ", ".join(f"`{token}`" for token in unknown[:4])
        return False, None, f"Unsupported placeholders: {rendered}."

    stripped = cleaned
    for token in allowed_placeholders:
        stripped = stripped.replace(token, "template value")
    if "{" in stripped or "}" in stripped:
        return False, None, f"{field_name} has unsupported placeholder syntax. Use the approved placeholders only."

    if sentence_limit is not None:
        sentence_parts = [part.strip() for part in re.split(r"[.!?]+", stripped) if part.strip()]
        if len(sentence_parts) > sentence_limit:
            return False, None, f"{field_name} must be at most {sentence_limit} short sentences."

    label = find_private_pattern(stripped)
    if label is not None:
        if label == "mentions":
            return False, None, f"{field_name} cannot contain raw mentions. Use approved placeholders instead."
        return False, None, f"{field_name} cannot contain {label}. Use short plain text only."

    if contains_blocklisted_term(stripped):
        return False, None, f"{field_name} contains blocked or inappropriate words. Use short plain text only."

    return True, cleaned, None
