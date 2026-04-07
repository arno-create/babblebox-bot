from __future__ import annotations

import json
import re
from collections.abc import Container, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from babblebox.text_safety import normalize_plain_text


DEFAULT_LINK_CACHE_TTL_SECONDS = 30.0 * 60.0
DEFAULT_LINK_CACHE_MAX_ENTRIES = 256
DEFAULT_LINK_INTEL_PATH = Path(__file__).resolve().parent / "data" / "shield_link_intel.json"
DEFAULT_EXTERNAL_MALICIOUS_PATHS = (
    Path(__file__).resolve().parent.parent / "malicious_links.txt",
    Path(__file__).resolve().parent.parent / "full-domains-aa.txt",
    Path(__file__).resolve().parent.parent / "full-domains-ab.txt",
    Path(__file__).resolve().parent.parent / "full-domains-ac.txt",
    Path(__file__).resolve().parent.parent / "malicious_files",
    Path(__file__).resolve().parent.parent / "malicious_files.txt",
)

SAFE_LINK_CATEGORY = "safe"
MALICIOUS_LINK_CATEGORY = "malicious"
ADULT_LINK_CATEGORY = "adult"
UNKNOWN_LINK_CATEGORY = "unknown"
UNKNOWN_SUSPICIOUS_LINK_CATEGORY = "unknown_suspicious"

LINK_CATEGORY_STRENGTH = {
    SAFE_LINK_CATEGORY: 0,
    UNKNOWN_LINK_CATEGORY: 1,
    UNKNOWN_SUSPICIOUS_LINK_CATEGORY: 2,
    ADULT_LINK_CATEGORY: 3,
    MALICIOUS_LINK_CATEGORY: 4,
}

WARNING_DISCUSSION_RE = re.compile(
    r"(?i)(?:\b(?:beware|warning|avoid|do not|don't|never|fake|malicious|phish(?:ing)?|report(?:ed|ing)?|blocked|blocklist(?:ed)?|heads up|scam|for review|for triage|triage)\b|\b(?:example|sample)\b.{0,24}\b(?:link|site|domain|url)\b|\b(?:scam|phish(?:ing)?|malicious)\b.{0,24}\b(?:example|sample)\b)"
)
SOCIAL_ENGINEERING_RE = re.compile(r"(?i)\b(?:download|run|install|open|verify|claim|login|log in|connect wallet|sync)\b")
SCAM_BAIT_RE = re.compile(
    r"(?i)\b(?:free nitro|nitro gift|steam gift|claim reward|claim now|verify your account|wallet connect|seed phrase|airdrop|gift inventory|limited time claim)\b"
)
BRAND_BAIT_RE = re.compile(r"(?i)\b(?:discord|nitro|steam|epic|wallet|crypto|gift)\b")
SUSPICIOUS_FILE_RE = re.compile(r"(?i)\.(?:exe|scr|bat|cmd|msi|zip|rar|7z|iso|apk)(?:$|[?#])")
ENCODED_QUERY_RE = re.compile(r"(?i)(?:%[0-9a-f]{2}){3,}")
TOKEN_RE = re.compile(r"[a-z0-9]+")
LINK_HOST_LABEL_RE = re.compile(r"[a-z0-9-]+")
HIGH_SEVERITY_CONTEXT_SIGNALS = frozenset({"suspicious_file_target", "message_scam_bait", "suspicious_attachment_link_combo"})
LOOKUP_CONTEXT_SIGNALS = HIGH_SEVERITY_CONTEXT_SIGNALS | frozenset(
    {"message_social_engineering", "message_brand_bait", "encoded_or_long_query"}
)


def domain_matches(domain: str, candidate: str) -> bool:
    return domain == candidate or domain.endswith(f".{candidate}")


def iter_domain_candidates(domain: str) -> tuple[str, ...]:
    normalized = normalize_plain_text(domain).casefold().strip().strip(".")
    if not normalized:
        return ()
    labels = [label for label in normalized.split(".") if label]
    if len(labels) < 2:
        return (normalized,)
    return tuple(".".join(labels[index:]) for index in range(len(labels) - 1))


def matching_domain(domain: str, candidates: Container[str]) -> str | None:
    for candidate in iter_domain_candidates(domain):
        if candidate in candidates:
            return candidate
    return None


def domain_in_set(domain: str, candidates: Container[str]) -> bool:
    return matching_domain(domain, candidates) is not None


def clean_url_candidate(raw_url: str) -> str | None:
    if not raw_url:
        return None
    candidate = raw_url.strip().strip("()[]{}<>,.!?\"'")
    if not candidate:
        return None
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    return candidate


def normalize_link_host(raw_host: str) -> str | None:
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


def extract_link_domain(raw_url: str) -> str | None:
    candidate = clean_url_candidate(raw_url)
    if candidate is None:
        return None
    parsed = urlsplit(candidate)
    return normalize_link_host(parsed.netloc)


@dataclass(frozen=True)
class ShieldLinkAssessment:
    normalized_domain: str
    category: str
    matched_signals: tuple[str, ...]
    provider_lookup_warranted: bool
    provider_status: str
    intel_version: str
    safe_family: str | None = None
    cache_hit: bool = False


@dataclass(frozen=True)
class ShieldLinkProviderRequest:
    domain: str
    suspicion_signals: tuple[str, ...]
    suspicion_score: int


@dataclass(frozen=True)
class _ExternalMaliciousFeed:
    domains: frozenset[str]
    source_paths: tuple[str, ...]
    skipped_lines: int
    load_errors: tuple[str, ...]


@dataclass(frozen=True)
class _BundledLinkIntel:
    intel_version: str
    source: str
    social_promo_domains: frozenset[str]
    storefront_domains: frozenset[str]
    media_embed_domains: frozenset[str]
    shortener_domains: frozenset[str]
    safe_families: dict[str, frozenset[str]]
    bundled_malicious_domains: frozenset[str]
    external_malicious_domains: frozenset[str]
    malicious_domains: frozenset[str]
    adult_domains: frozenset[str]
    external_malicious_source_paths: tuple[str, ...]
    external_malicious_skipped_lines: int
    external_malicious_load_errors: tuple[str, ...]
    suspicious_tlds: frozenset[str]
    suspicious_host_tokens: frozenset[str]
    brand_tokens: frozenset[str]
    suspicious_path_tokens: frozenset[str]
    suspicious_query_tokens: frozenset[str]
    suspicious_threshold: int
    provider_lookup_threshold: int

    def safe_family_for_domain(self, domain: str) -> str | None:
        for family, domains in self.safe_families.items():
            if matching_domain(domain, domains) is not None:
                return family
        return None


@dataclass(frozen=True)
class _CachedDomainProfile:
    domain: str
    safe_family: str | None
    known_category: str | None
    host_signals: tuple[str, ...]
    suspicious_base_score: int


def _clean_domain_list(values: Any) -> frozenset[str]:
    if not isinstance(values, list):
        return frozenset()
    cleaned = {
        normalize_plain_text(str(value)).casefold().strip(".")
        for value in values
        if isinstance(value, str) and normalize_plain_text(str(value)).strip()
    }
    return frozenset(value for value in cleaned if value)


def _load_external_malicious_domains(paths: Sequence[Path] | None = None) -> _ExternalMaliciousFeed:
    domains: set[str] = set()
    source_paths: list[str] = []
    load_errors: list[str] = []
    skipped_lines = 0
    for raw_path in paths or DEFAULT_EXTERNAL_MALICIOUS_PATHS:
        path = Path(raw_path)
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                source_paths.append(str(path.resolve()))
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    domain = extract_link_domain(line)
                    if domain is None:
                        skipped_lines += 1
                        continue
                    domains.add(domain)
        except OSError as exc:
            load_errors.append(f"{path}: {exc}")
    return _ExternalMaliciousFeed(
        domains=frozenset(domains),
        source_paths=tuple(source_paths),
        skipped_lines=skipped_lines,
        load_errors=tuple(load_errors),
    )


def _load_bundled_intel(
    path: Path = DEFAULT_LINK_INTEL_PATH,
    *,
    external_malicious_paths: Sequence[Path] | None = None,
) -> _BundledLinkIntel:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("Shield link intel payload must be an object.")

    safe_families_payload = payload.get("safe_families", {})
    safe_families: dict[str, frozenset[str]] = {}
    if isinstance(safe_families_payload, dict):
        for family, values in safe_families_payload.items():
            if isinstance(family, str):
                safe_families[family.strip().casefold()] = _clean_domain_list(values)

    thresholds = payload.get("thresholds", {})
    suspicious_threshold = int(thresholds.get("suspicious", 2)) if isinstance(thresholds, dict) else 2
    provider_lookup_threshold = int(thresholds.get("provider_lookup", 3)) if isinstance(thresholds, dict) else 3
    external_feed = _load_external_malicious_domains(external_malicious_paths)
    bundled_malicious_domains = _clean_domain_list(payload.get("malicious_domains", []))
    effective_malicious_domains = frozenset(set(bundled_malicious_domains) | set(external_feed.domains))

    return _BundledLinkIntel(
        intel_version=str(payload.get("intel_version", "unknown")).strip() or "unknown",
        source="bundled+external" if external_feed.domains else (str(payload.get("source", "bundled")).strip() or "bundled"),
        social_promo_domains=_clean_domain_list(payload.get("social_promo_domains", [])),
        storefront_domains=_clean_domain_list(payload.get("storefront_domains", [])),
        media_embed_domains=_clean_domain_list(payload.get("media_embed_domains", [])),
        shortener_domains=_clean_domain_list(payload.get("shortener_domains", [])),
        safe_families=safe_families,
        bundled_malicious_domains=bundled_malicious_domains,
        external_malicious_domains=external_feed.domains,
        malicious_domains=effective_malicious_domains,
        adult_domains=_clean_domain_list(payload.get("adult_domains", [])),
        external_malicious_source_paths=external_feed.source_paths,
        external_malicious_skipped_lines=external_feed.skipped_lines,
        external_malicious_load_errors=external_feed.load_errors,
        suspicious_tlds=_clean_domain_list(payload.get("suspicious_tlds", [])),
        suspicious_host_tokens=_clean_domain_list(payload.get("suspicious_host_tokens", [])),
        brand_tokens=_clean_domain_list(payload.get("brand_tokens", [])),
        suspicious_path_tokens=_clean_domain_list(payload.get("suspicious_path_tokens", [])),
        suspicious_query_tokens=_clean_domain_list(payload.get("suspicious_query_tokens", [])),
        suspicious_threshold=max(1, suspicious_threshold),
        provider_lookup_threshold=max(1, provider_lookup_threshold),
    )


_BUNDLED_INTEL = _load_bundled_intel()
SOCIAL_PROMO_DOMAINS = _BUNDLED_INTEL.social_promo_domains
STOREFRONT_DOMAINS = _BUNDLED_INTEL.storefront_domains
MEDIA_EMBED_DOMAINS = _BUNDLED_INTEL.media_embed_domains
SHORTENER_DOMAINS = _BUNDLED_INTEL.shortener_domains


class ShieldLinkProvider:
    provider_name = "disabled"

    def diagnostics(self) -> dict[str, Any]:
        return {
            "provider": self.provider_name,
            "available": False,
            "configured": False,
            "status": "Local domain intelligence is active. External reputation lookup is inactive.",
        }

    async def lookup(self, request: ShieldLinkProviderRequest) -> dict[str, Any] | None:
        return None

    async def close(self):
        return None


class DisabledShieldLinkProvider(ShieldLinkProvider):
    pass


def build_shield_link_provider() -> ShieldLinkProvider:
    return DisabledShieldLinkProvider()


class _LinkProfileCache:
    def __init__(self, *, ttl_seconds: float = DEFAULT_LINK_CACHE_TTL_SECONDS, max_entries: int = DEFAULT_LINK_CACHE_MAX_ENTRIES):
        self.ttl_seconds = max(60.0, float(ttl_seconds))
        self.max_entries = max(32, int(max_entries))
        self._entries: dict[str, tuple[float, _CachedDomainProfile]] = {}

    def get(self, domain: str, *, now: float) -> _CachedDomainProfile | None:
        item = self._entries.get(domain)
        if item is None:
            return None
        created_at, profile = item
        if now - created_at > self.ttl_seconds:
            self._entries.pop(domain, None)
            return None
        return profile

    def set(self, domain: str, profile: _CachedDomainProfile, *, now: float):
        self._entries[domain] = (now, profile)
        self.prune(now, aggressive=len(self._entries) > self.max_entries)

    def prune(self, now: float, *, aggressive: bool = False):
        self._entries = {
            key: (created_at, value)
            for key, (created_at, value) in self._entries.items()
            if now - created_at <= self.ttl_seconds
        }
        if aggressive and len(self._entries) > self.max_entries:
            for key in list(self._entries.keys())[: max(1, len(self._entries) - self.max_entries)]:
                self._entries.pop(key, None)

    def diagnostics(self) -> dict[str, Any]:
        return {
            "mode": "memory-ttl",
            "ttl_seconds": int(self.ttl_seconds),
            "max_entries": self.max_entries,
            "size": len(self._entries),
        }


class ShieldLinkSafetyEngine:
    def __init__(
        self,
        *,
        intel: _BundledLinkIntel | None = None,
        provider: ShieldLinkProvider | None = None,
        cache: _LinkProfileCache | None = None,
    ):
        self.intel = intel or _BUNDLED_INTEL
        self.provider = provider or build_shield_link_provider()
        self.cache = cache or _LinkProfileCache()

    async def close(self):
        await self.provider.close()

    def diagnostics(self) -> dict[str, Any]:
        provider = self.provider.diagnostics()
        cache = self.cache.diagnostics()
        return {
            "intel_version": self.intel.intel_version,
            "intel_source": self.intel.source,
            "bundled_malicious_domains": len(self.intel.bundled_malicious_domains),
            "external_malicious_domains": len(self.intel.external_malicious_domains),
            "effective_malicious_domains": len(self.intel.malicious_domains),
            "external_malicious_source_paths": list(self.intel.external_malicious_source_paths),
            "external_malicious_skipped_lines": self.intel.external_malicious_skipped_lines,
            "external_malicious_load_errors": list(self.intel.external_malicious_load_errors),
            "provider": provider.get("provider"),
            "provider_available": bool(provider.get("available")),
            "provider_status": provider.get("status", "Unavailable."),
            "cache_mode": cache["mode"],
            "cache_entries": cache["size"],
            "cache_max_entries": cache["max_entries"],
            "cache_ttl_seconds": cache["ttl_seconds"],
        }

    def prune(self, now: float):
        self.cache.prune(now)

    def assess_domain(
        self,
        domain: str,
        *,
        path: str,
        query: str,
        message_text: str,
        squashed_text: str,
        has_suspicious_attachment: bool,
        allowlisted: bool,
        now: float,
    ) -> ShieldLinkAssessment:
        diagnostics = self.provider.diagnostics()
        provider_status = diagnostics.get("status", "Unavailable.")
        if allowlisted:
            return ShieldLinkAssessment(
                normalized_domain=domain,
                category=SAFE_LINK_CATEGORY,
                matched_signals=("guild_allow_domain",),
                provider_lookup_warranted=False,
                provider_status=provider_status,
                intel_version=self.intel.intel_version,
            )

        cached = self.cache.get(domain, now=now)
        cache_hit = cached is not None
        if cached is None:
            cached = self._build_domain_profile(domain)
            self.cache.set(domain, cached, now=now)

        signals = list(cached.host_signals)
        if cached.safe_family is not None:
            signals.insert(0, f"safe_family:{cached.safe_family}")
            return ShieldLinkAssessment(
                normalized_domain=domain,
                category=SAFE_LINK_CATEGORY,
                matched_signals=tuple(dict.fromkeys(signals)),
                provider_lookup_warranted=False,
                provider_status=provider_status,
                intel_version=self.intel.intel_version,
                safe_family=cached.safe_family,
                cache_hit=cache_hit,
            )
        if cached.known_category is not None:
            return ShieldLinkAssessment(
                normalized_domain=domain,
                category=cached.known_category,
                matched_signals=tuple(dict.fromkeys(signals)),
                provider_lookup_warranted=False,
                provider_status=provider_status,
                intel_version=self.intel.intel_version,
                cache_hit=cache_hit,
            )

        context_signals, warning_context = self._context_signals(
            path=path,
            query=query,
            message_text=message_text,
            squashed_text=squashed_text,
            has_suspicious_attachment=has_suspicious_attachment,
        )
        signals.extend(context_signals)
        context_score = self._score_context_signals(context_signals)
        host_signal_set = set(cached.host_signals)
        shortener_only = host_signal_set == {"shortener_domain"}
        host_risk = bool(host_signal_set) and not shortener_only
        high_severity_context_count = sum(signal in HIGH_SEVERITY_CONTEXT_SIGNALS for signal in context_signals)
        lookup_context = any(signal in LOOKUP_CONTEXT_SIGNALS for signal in context_signals)
        suspicious_context = high_severity_context_count > 0
        suspicious_enough = host_risk or suspicious_context or (shortener_only and lookup_context)
        if warning_context and not host_risk and high_severity_context_count == 0:
            suspicious_enough = False
        provider_lookup_warranted = (
            not warning_context
            and (
                (host_risk and (lookup_context or context_score >= 2))
                or (shortener_only and lookup_context)
                or high_severity_context_count >= 2
            )
        )
        category = UNKNOWN_SUSPICIOUS_LINK_CATEGORY if (provider_lookup_warranted or suspicious_enough) else UNKNOWN_LINK_CATEGORY
        return ShieldLinkAssessment(
            normalized_domain=domain,
            category=category,
            matched_signals=tuple(dict.fromkeys(signals)),
            provider_lookup_warranted=provider_lookup_warranted,
            provider_status=provider_status,
            intel_version=self.intel.intel_version,
            cache_hit=cache_hit,
        )

    def _build_domain_profile(self, domain: str) -> _CachedDomainProfile:
        safe_family = self.intel.safe_family_for_domain(domain)
        if safe_family is not None:
            return _CachedDomainProfile(
                domain=domain,
                safe_family=safe_family,
                known_category=None,
                host_signals=(f"safe_family:{safe_family}",),
                suspicious_base_score=0,
            )
        bundled_malicious_match = matching_domain(domain, self.intel.bundled_malicious_domains)
        if bundled_malicious_match is not None:
            signal = "bundled_malicious_domain_exact" if bundled_malicious_match == domain else "bundled_malicious_domain_family"
            return _CachedDomainProfile(
                domain=domain,
                safe_family=None,
                known_category=MALICIOUS_LINK_CATEGORY,
                host_signals=(signal,),
                suspicious_base_score=0,
            )
        external_malicious_match = matching_domain(domain, self.intel.external_malicious_domains)
        if external_malicious_match is not None:
            signal = "external_malicious_domain_exact" if external_malicious_match == domain else "external_malicious_domain_family"
            return _CachedDomainProfile(
                domain=domain,
                safe_family=None,
                known_category=MALICIOUS_LINK_CATEGORY,
                host_signals=(signal,),
                suspicious_base_score=0,
            )
        adult_match = matching_domain(domain, self.intel.adult_domains)
        if adult_match is not None:
            signal = "bundled_adult_domain_exact" if adult_match == domain else "bundled_adult_domain_family"
            return _CachedDomainProfile(
                domain=domain,
                safe_family=None,
                known_category=ADULT_LINK_CATEGORY,
                host_signals=(signal,),
                suspicious_base_score=0,
            )

        host_signals: list[str] = []
        suspicious_score = 0
        if "xn--" in domain:
            host_signals.append("punycode_host")
            suspicious_score += 2
        if matching_domain(domain, self.intel.shortener_domains) is not None:
            host_signals.append("shortener_domain")
            suspicious_score += 1

        labels = [label for label in domain.split(".") if label]
        tld = labels[-1] if labels else ""
        if tld in self.intel.suspicious_tlds:
            host_signals.append(f"suspicious_tld:{tld}")
            suspicious_score += 1

        domain_tokens = {token for token in TOKEN_RE.findall(domain)}
        for token in sorted(domain_tokens):
            if token in self.intel.suspicious_host_tokens:
                host_signals.append(f"host_token:{token}")
                suspicious_score += 1
            if token in self.intel.brand_tokens:
                host_signals.append(f"brand_token:{token}")
                suspicious_score += 1
        if domain.count("-") >= 3:
            host_signals.append("hyphen_heavy_host")
            suspicious_score += 1

        return _CachedDomainProfile(
            domain=domain,
            safe_family=None,
            known_category=None,
            host_signals=tuple(dict.fromkeys(host_signals)),
            suspicious_base_score=suspicious_score,
        )

    def _context_signals(
        self,
        *,
        path: str,
        query: str,
        message_text: str,
        squashed_text: str,
        has_suspicious_attachment: bool,
    ) -> tuple[tuple[str, ...], bool]:
        signals: list[str] = []
        combined_path = f"{path}?{query}" if query else path
        path_tokens = set(TOKEN_RE.findall(path))
        for token in sorted(path_tokens):
            if token in self.intel.suspicious_path_tokens:
                signals.append(f"path_token:{token}")
        query_tokens = set(TOKEN_RE.findall(query))
        for token in sorted(query_tokens):
            if token in self.intel.suspicious_query_tokens:
                signals.append(f"query_token:{token}")
        if query and (len(query) >= 80 or ENCODED_QUERY_RE.search(query)):
            signals.append("encoded_or_long_query")
        if SUSPICIOUS_FILE_RE.search(combined_path):
            signals.append("suspicious_file_target")
        if SCAM_BAIT_RE.search(message_text) or SCAM_BAIT_RE.search(squashed_text):
            signals.append("message_scam_bait")
        if BRAND_BAIT_RE.search(message_text):
            signals.append("message_brand_bait")
        if SOCIAL_ENGINEERING_RE.search(message_text):
            signals.append("message_social_engineering")
        if has_suspicious_attachment:
            signals.append("suspicious_attachment_link_combo")
        warning_context = bool(WARNING_DISCUSSION_RE.search(message_text))
        if warning_context:
            signals.append("warning_or_discussion_context")
        return tuple(dict.fromkeys(signals)), warning_context

    def _score_context_signals(self, signals: tuple[str, ...]) -> int:
        score = 0
        for signal in signals:
            if signal in {"suspicious_file_target", "message_scam_bait"}:
                score += 2
            elif signal in {"message_social_engineering", "message_brand_bait", "suspicious_attachment_link_combo"}:
                score += 1
            elif signal.startswith("path_token:") or signal.startswith("query_token:") or signal == "encoded_or_long_query":
                score += 1
        return score


def merge_link_assessments(existing: ShieldLinkAssessment | None, incoming: ShieldLinkAssessment) -> ShieldLinkAssessment:
    if existing is None:
        return incoming
    existing_strength = LINK_CATEGORY_STRENGTH.get(existing.category, 0)
    incoming_strength = LINK_CATEGORY_STRENGTH.get(incoming.category, 0)
    chosen = incoming if incoming_strength > existing_strength else existing
    merged_signals = tuple(dict.fromkeys(existing.matched_signals + incoming.matched_signals))
    return ShieldLinkAssessment(
        normalized_domain=chosen.normalized_domain,
        category=chosen.category,
        matched_signals=merged_signals,
        provider_lookup_warranted=existing.provider_lookup_warranted or incoming.provider_lookup_warranted,
        provider_status=chosen.provider_status,
        intel_version=chosen.intel_version,
        safe_family=chosen.safe_family or existing.safe_family or incoming.safe_family,
        cache_hit=existing.cache_hit or incoming.cache_hit,
    )
