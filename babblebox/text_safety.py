from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence


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

REPORTING_CONTEXT_RE = re.compile(
    r"(?i)\b(?:quote|quoted|quoting|report|reported|reporting|report this|context|for review|for moderation|moderation(?: note| log)?|mod note|admin note|incident review|review queue|screenshot(?:ed)?|screencap|news|headline|history|historical|documentary|sample|example)\b"
)
EDUCATIONAL_CONTEXT_RE = re.compile(
    r"(?i)\b(?:medical|medicine|doctor|clinic|health|sexual health|biology|education|educational|therapy|consent|pregnancy|assault|prevention|awareness|study|class|support group|support worker|survivor|victim|trafficking|hotline|988)\b"
)
DISAPPROVAL_CONTEXT_RE = re.compile(
    r"(?i)\b(?:don['’]t say|do not say|don['’]t post|do not post|don['’]t tell people to|do not tell people to|don['’]t call people|do not call people|stop posting|stop saying|stop telling people to|stop calling people|not allowed|against (?:the )?(?:rules|policy)|rule violation|policy violation|banned phrase|keep that out|warning example)\b"
)
QUOTE_ATTRIBUTION_RE = re.compile(
    r"(?i)\b(?:they|he|she|someone|user|member|person|people|poster|account)\s+(?:said|saying|posted|posting|sent|sending|wrote|writing|advertised|advertising|offered|offering|asked|asking|told|telling|quoted|quoting|called|calling)\b|\bfor\s+(?:saying|posting|sending|writing|advertising|offering|asking|telling|calling)\b|\b(?:called me|called them|call people|tell people to)\b"
)
REPORTING_CONTEXT_ALWAYS_SUPPRESS_RE = re.compile(
    r"(?i)\b(?:report this|for review|for moderation|moderation(?: note| log)?|mod note|admin note|incident review|review queue|screenshot(?:ed)?|screencap|news|headline|history|historical|documentary)\b"
)
MODERATION_ACTION_CONTEXT_RE = re.compile(
    r"(?i)\b(?:mods?|moderators?|staff|admins?|filter|shield|automod)\s+(?:removed|deleted|flagged|warned|muted|timed out|banned)\b|\b(?:removed|deleted|flagged|warned)\s+(?:for|because)\b"
)
EXAMPLE_CONTEXT_RE = re.compile(r"(?i)\b(?:sample|example)\b")
SEVERE_REFERENCE_PREFIX_RE = re.compile(
    r"(?i)^(?:quote|quoted|quoting|example|sample|history|report(?:ed)?(?: this)?(?: ad| message| post| screenshot)?|moderation(?: note| log)?|mod note|admin note|warning example)\b.{0,12}(?:[:\-]|$)"
)
SEVERE_HARD_REFERENCE_CONTEXT_RE = re.compile(
    r"(?i)\b(?:for review|for moderation|moderation(?: note| log)?|mod note|admin note|incident review|review queue|news|headline|history|historical|documentary)\b"
)
SEVERE_REFERENCE_OBJECT_RE = re.compile(r"(?i)\b(?:phrase|term|slur|word|wording|language|quote)\b")
SEVERE_DISCUSSION_VERB_RE = re.compile(
    r"(?i)\b(?:said|saying|says|posted|posting|wrote|writing|sent|sending|called|calling|used|using|uses|discussed|discussing|mention(?:ed|ing)?|quoted|quoting)\b"
)
SEVERE_STAFF_ATTRIBUTION_RE = re.compile(
    r"(?i)\b(?:mods?|moderators?|staff|admins?)\s+(?:said|posted|wrote|sent|called|used|quoted|warned)\b"
)
SEVERE_RULES_CONTEXT_RE = re.compile(
    r"(?i)\b(?:bann(?:ed|able)|not allowed|against (?:the )?(?:rules|policy)|rule violation|policy violation|keep that out)\b"
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


def contains_safety_term(term: str, text: str, squashed: str | None = None) -> bool:
    candidate = squashed if squashed is not None else squash_for_evasion_checks(text)
    if " " in term:
        return term in text or term in candidate
    pattern = rf"\b{re.escape(term)}\b"
    return re.search(pattern, text, re.IGNORECASE) is not None or re.search(pattern, candidate, re.IGNORECASE) is not None


def find_safety_term_hits(terms: set[str] | frozenset[str], text: str, squashed: str | None = None) -> list[str]:
    candidate = squashed if squashed is not None else squash_for_evasion_checks(text)
    return sorted(term for term in terms if contains_safety_term(term, text, candidate))


def is_reporting_or_educational_context(text: str) -> bool:
    return bool(
        REPORTING_CONTEXT_RE.search(text)
        or REPORTING_CONTEXT_ALWAYS_SUPPRESS_RE.search(text)
        or MODERATION_ACTION_CONTEXT_RE.search(text)
        or EDUCATIONAL_CONTEXT_RE.search(text)
    )


def is_harmful_context_suppressed(text: str, *, include_disapproval: bool = False) -> bool:
    if EDUCATIONAL_CONTEXT_RE.search(text):
        return True
    if include_disapproval and DISAPPROVAL_CONTEXT_RE.search(text):
        return True
    if MODERATION_ACTION_CONTEXT_RE.search(text):
        return True
    if REPORTING_CONTEXT_ALWAYS_SUPPRESS_RE.search(text):
        return True
    reporting = REPORTING_CONTEXT_RE.search(text)
    if reporting is None:
        return False
    matched_reporting = reporting.group(0).casefold()
    if any(
        token in matched_reporting
        for token in ("for review", "for moderation", "moderation", "mod note", "admin note", "incident review", "review queue", "quoted", "screenshot", "screencap", "news", "headline", "history", "historical", "documentary", "report this")
    ):
        return True
    if EXAMPLE_CONTEXT_RE.search(text):
        return bool(QUOTE_ATTRIBUTION_RE.search(text))
    return bool(QUOTE_ATTRIBUTION_RE.search(text))


def is_severe_reference_context(text: str, *, matched_terms: Sequence[str] = ()) -> bool:
    cleaned = normalize_plain_text(text)
    if not cleaned:
        return False
    if MODERATION_ACTION_CONTEXT_RE.search(cleaned):
        return True
    squashed = squash_for_evasion_checks(cleaned)
    normalized_hits = tuple(term for term in matched_terms if contains_safety_term(term, cleaned, squashed))
    has_reporting = bool(REPORTING_CONTEXT_RE.search(cleaned))
    has_hard_reference = bool(SEVERE_HARD_REFERENCE_CONTEXT_RE.search(cleaned))
    has_reference_prefix = bool(SEVERE_REFERENCE_PREFIX_RE.search(cleaned))
    has_meta_object = bool(SEVERE_REFERENCE_OBJECT_RE.search(cleaned))
    has_discussion_verb = bool(SEVERE_DISCUSSION_VERB_RE.search(cleaned))
    has_attribution = bool(QUOTE_ATTRIBUTION_RE.search(cleaned) or SEVERE_STAFF_ATTRIBUTION_RE.search(cleaned))
    has_educational = bool(EDUCATIONAL_CONTEXT_RE.search(cleaned))
    has_rules_context = bool(DISAPPROVAL_CONTEXT_RE.search(cleaned) or SEVERE_RULES_CONTEXT_RE.search(cleaned))
    if has_hard_reference or has_reference_prefix:
        return True
    if has_rules_context and (has_meta_object or has_attribution or has_discussion_verb or bool(normalized_hits)):
        return True
    if has_attribution and (has_reporting or has_discussion_verb or has_meta_object or bool(normalized_hits)):
        return True
    if has_reporting and (has_meta_object or has_discussion_verb or bool(normalized_hits)):
        return True
    if has_educational and (has_meta_object or has_discussion_verb or has_attribution):
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
