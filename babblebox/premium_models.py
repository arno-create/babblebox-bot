from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


PLAN_FREE = "free"
PLAN_SUPPORTER = "supporter"
PLAN_PLUS = "plus"
PLAN_GUILD_PRO = "guild_pro"

SCOPE_USER = "user"
SCOPE_GUILD = "guild"
PROVIDER_PATREON = "patreon"

SYSTEM_PREMIUM_OWNER_USER_IDS = frozenset({1266444952779620413, 1345860619836063754})
SYSTEM_PREMIUM_SUPPORT_GUILD_ID = 1322933864360050688
SYSTEM_PREMIUM_CLAIM_KIND = "system_owner"

MANUAL_KIND_GRANT = "grant"
MANUAL_KIND_BLOCK = "block"

LINK_STATUS_ACTIVE = "active"
LINK_STATUS_BROKEN = "broken"
LINK_STATUS_REVOKED = "revoked"

ENTITLEMENT_STATUS_ACTIVE = "active"
ENTITLEMENT_STATUS_INACTIVE = "inactive"

CLAIM_STATUS_ACTIVE = "active"
CLAIM_STATUS_RELEASED = "released"

WEBHOOK_STATUS_PENDING = "pending"
WEBHOOK_STATUS_PROCESSED = "processed"
WEBHOOK_STATUS_FAILED = "failed"
WEBHOOK_STATUS_UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class PremiumLinkRecord:
    provider: str
    discord_user_id: int
    provider_user_id: str
    link_status: str
    linked_at: datetime
    updated_at: datetime
    access_token_ciphertext: str | None = None
    refresh_token_ciphertext: str | None = None
    token_expires_at: datetime | None = None
    scopes: tuple[str, ...] = ()
    email: str | None = None
    display_name: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PremiumEntitlementRecord:
    entitlement_id: str
    provider: str
    source_ref: str
    discord_user_id: int
    plan_code: str
    status: str
    linked_provider_user_id: str
    last_verified_at: datetime
    stale_after: datetime
    grace_until: datetime
    current_period_end: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PremiumManualOverrideRecord:
    override_id: str
    target_type: str
    target_id: int
    kind: str
    plan_code: str | None
    active: bool
    created_at: datetime
    updated_at: datetime
    actor_user_id: int | None = None
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PremiumGuildClaimRecord:
    claim_id: str
    guild_id: int
    plan_code: str
    owner_user_id: int
    source_kind: str
    source_id: str
    status: str
    claimed_at: datetime
    updated_at: datetime
    entitlement_id: str | None = None
    released_at: datetime | None = None
    note: str | None = None


@dataclass(frozen=True)
class PremiumScopeSnapshot:
    scope: str
    target_id: int
    plan_code: str
    active_plans: tuple[str, ...]
    blocked: bool = False
    stale: bool = False
    in_grace: bool = False
    linked: bool = False
    claimable_entitlement_ids: tuple[str, ...] = ()
    claimed_entitlement_ids: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PatreonIdentity:
    provider_user_id: str
    email: str | None
    display_name: str | None
    member_id: str | None
    plan_codes: tuple[str, ...]
    patron_status: str | None
    tier_ids: tuple[str, ...]
    next_charge_date: datetime | None
    raw_user: dict[str, Any]
    raw_member: dict[str, Any] | None
    raw_tiers: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ProviderSyncResult:
    provider: str
    provider_user_id: str
    plan_codes: tuple[str, ...]
    stale_after: datetime
    grace_until: datetime
    current_period_end: datetime | None
    metadata: dict[str, Any]
