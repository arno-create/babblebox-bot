from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import discord
from discord.ext import commands

from babblebox.premium_limits import (
    LIMIT_AFK_SCHEDULES,
    LIMIT_REMINDERS_ACTIVE,
    LIMIT_REMINDERS_PUBLIC_ACTIVE,
    LIMIT_WATCH_FILTERS,
    LIMIT_WATCH_KEYWORDS,
)
from babblebox.premium_models import PLAN_FREE, PLAN_PLUS, PLAN_SUPPORTER, SYSTEM_PREMIUM_OWNER_USER_IDS
from babblebox.premium_provider import WebhookVerificationError
from babblebox.vote_store import VoteStorageUnavailable, VoteStore


LOGGER = logging.getLogger(__name__)

TOPGG_CONFIGURATION_DISABLED = "disabled"
TOPGG_CONFIGURATION_CONFIGURED = "configured"
TOPGG_CONFIGURATION_MISCONFIGURED = "misconfigured"
TOPGG_PROJECT_ID_DEFAULT = "1480903089518022739"
TOPGG_VOTE_URL_TEMPLATE = "https://top.gg/bot/{project_id}/vote"
TOPGG_VOTE_STATUS_URL_TEMPLATE = "https://top.gg/api/v1/projects/@me/votes/{user_id}"
TOPGG_LEGACY_VOTE_CHECK_URL_TEMPLATE = "https://top.gg/api/bots/{project_id}/check"
TOPGG_REFRESH_COOLDOWN_SECONDS = 60
TOPGG_REMINDER_LOOP_SECONDS = 300
TOPGG_LEGACY_CONFIRM_TIMEOUT_SECONDS = 3.0
TOPGG_LEGACY_VOTE_WINDOW = timedelta(hours=12)
TOPGG_LEGACY_DEDUPE_WINDOW_SECONDS = 1800
TOPGG_PLATFORM = "discord"
TOPGG_WEBHOOK_MODE_V2 = "v2"
TOPGG_WEBHOOK_MODE_LEGACY = "legacy"
TOPGG_TIMING_SOURCE_EXACT = "exact"
TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED = "legacy_estimated"
TOPGG_VOTE_LIMITS = {
    LIMIT_WATCH_KEYWORDS: 15,
    LIMIT_WATCH_FILTERS: 12,
    LIMIT_REMINDERS_ACTIVE: 5,
    LIMIT_REMINDERS_PUBLIC_ACTIVE: 2,
    LIMIT_AFK_SCHEDULES: 10,
}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _serialize_datetime(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


@dataclass(frozen=True)
class TopggWebhookResult:
    outcome: str
    message: str


class VoteService:
    def __init__(self, bot: commands.Bot, *, store: VoteStore | None = None, session: aiohttp.ClientSession | None = None):
        self.bot = bot
        self.storage_ready = False
        self.storage_error: str | None = None
        self._startup_storage_error: str | None = None
        self.storage_backend_preference = (
            getattr(store, "backend_preference", None)
            or os.getenv("TOPGG_STORAGE_BACKEND", "").strip()
            or os.getenv("VOTE_STORAGE_BACKEND", "").strip()
            or "postgres"
        ).strip().lower()
        self.store: VoteStore | None
        if store is not None:
            self.store = store
        else:
            try:
                self.store = VoteStore()
                self.storage_backend_preference = getattr(self.store, "backend_preference", self.storage_backend_preference)
            except VoteStorageUnavailable as exc:
                self.store = None
                self._startup_storage_error = str(exc)
                self.storage_error = str(exc)
        self._session = session
        self._session_owner = session is None
        self._votes_by_user: dict[int, dict[str, Any]] = {}
        self._refresh_cooldowns: dict[int, float] = {}
        self._lock = asyncio.Lock()
        self._reminder_task: asyncio.Task | None = None

    async def start(self) -> bool:
        if self._startup_storage_error is not None or self.store is None:
            self.storage_ready = False
            self.storage_error = self._startup_storage_error
            LOGGER.warning("Vote storage unavailable: %s", self._startup_storage_error)
            return False
        try:
            await self.store.load()
        except VoteStorageUnavailable as exc:
            self.storage_ready = False
            self.storage_error = str(exc)
            LOGGER.warning("Vote storage unavailable: %s", exc)
            return False
        self.storage_ready = True
        self.storage_error = None
        await self._reload_cache()
        self._register_web_runtime()
        self._ensure_session()
        self._reminder_task = asyncio.create_task(self._reminder_loop(), name="babblebox-topgg-reminders")
        return True

    async def close(self):
        if self._reminder_task is not None:
            self._reminder_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reminder_task
            self._reminder_task = None
        if self._session_owner and self._session is not None:
            await self._session.close()
        if self.store is not None:
            await self.store.close()
        self._register_web_runtime(clear=True)

    def _register_web_runtime(self, *, clear: bool = False):
        try:
            from babblebox import web
        except Exception:
            return
        setter = getattr(web, "set_vote_runtime", None)
        if callable(setter):
            setter(None if clear else self)

    def storage_message(self, feature_name: str = "Vote Bonus") -> str:
        return f"{feature_name} is temporarily unavailable because Babblebox could not reach its vote database."

    def configuration_state(self) -> str:
        token = self._topgg_token()
        mode = self.webhook_mode()
        secret = self._topgg_webhook_secret()
        if not token and not secret:
            return TOPGG_CONFIGURATION_DISABLED
        if not secret:
            return TOPGG_CONFIGURATION_MISCONFIGURED
        if mode == TOPGG_WEBHOOK_MODE_LEGACY and not token:
            return TOPGG_CONFIGURATION_MISCONFIGURED
        return TOPGG_CONFIGURATION_CONFIGURED

    def configuration_message(self) -> str:
        state = self.configuration_state()
        mode = self.webhook_mode()
        if state == TOPGG_CONFIGURATION_DISABLED:
            return "Top.gg vote bonuses are disabled on this deployment."
        if state == TOPGG_CONFIGURATION_MISCONFIGURED:
            if not self._topgg_webhook_secret():
                return "Top.gg vote bonuses are misconfigured. Set `TOPGG_WEBHOOK_SECRET` to the Top.gg webhook secret."
            if mode == TOPGG_WEBHOOK_MODE_LEGACY and not self._topgg_token():
                return (
                    "Top.gg vote bonuses are misconfigured. This Top.gg dashboard is using legacy webhooks, "
                    "so `TOPGG_TOKEN` is also required to confirm vote status safely."
                )
            return "Top.gg vote bonuses are misconfigured. Check `TOPGG_WEBHOOK_SECRET` and `TOPGG_TOKEN`."
        if mode == TOPGG_WEBHOOK_MODE_LEGACY:
            return (
                "Top.gg vote bonuses are configured with legacy webhooks. Babblebox verifies the shared "
                "Authorization header, confirms the vote through the legacy API, and estimates the standard 12-hour "
                "vote window because legacy Top.gg does not return exact timestamps. Webhooks V2 are still preferred."
            )
        if not self._topgg_token():
            return "Top.gg vote bonuses are configured. API refresh is disabled until `TOPGG_TOKEN` is set."
        return "Top.gg vote bonuses are configured."

    def webhook_mode(self) -> str | None:
        secret = self._topgg_webhook_secret()
        if not secret:
            return None
        return TOPGG_WEBHOOK_MODE_V2 if secret.startswith("whs_") else TOPGG_WEBHOOK_MODE_LEGACY

    def vote_url(self) -> str:
        return TOPGG_VOTE_URL_TEMPLATE.format(project_id=self.project_id())

    def project_id(self) -> str:
        configured = str(os.getenv("TOPGG_PROJECT_ID", "") or "").strip()
        if configured:
            return configured
        bot_user_id = getattr(getattr(self.bot, "user", None), "id", None)
        if bot_user_id:
            return str(bot_user_id)
        return TOPGG_PROJECT_ID_DEFAULT

    def resolve_plan_code(self, user_id: int) -> str:
        premium_service = getattr(self.bot, "premium_service", None)
        snapshot_getter = getattr(premium_service, "get_user_snapshot", None)
        if callable(snapshot_getter):
            with contextlib.suppress(Exception):
                snapshot = snapshot_getter(user_id)
                plan_code = str((snapshot or {}).get("plan_code") or "").strip().lower()
                if plan_code in {PLAN_FREE, PLAN_SUPPORTER, PLAN_PLUS}:
                    return plan_code
        if int(user_id or 0) in SYSTEM_PREMIUM_OWNER_USER_IDS:
            return PLAN_PLUS
        return PLAN_FREE

    def plan_label(self, plan_code: str) -> str:
        labels = {
            PLAN_FREE: "Free",
            PLAN_SUPPORTER: "Supporter",
            PLAN_PLUS: "Babblebox Plus",
        }
        return labels.get(plan_code, "Free")

    def bonus_limit_for(self, limit_key: str) -> int | None:
        value = TOPGG_VOTE_LIMITS.get(limit_key)
        return int(value) if value is not None else None

    def get_vote_record(self, user_id: int) -> dict[str, Any] | None:
        record = self._votes_by_user.get(int(user_id))
        return deepcopy(record) if record is not None else None

    def has_active_vote_bonus(self, user_id: int, *, plan_code: str | None = None, now: datetime | None = None) -> bool:
        current_plan = plan_code or self.resolve_plan_code(user_id)
        if current_plan not in {PLAN_FREE, PLAN_SUPPORTER}:
            return False
        record = self._votes_by_user.get(int(user_id))
        if record is None:
            return False
        expires_at = _parse_datetime(record.get("expires_at"))
        if expires_at is None:
            return False
        return expires_at > (now or _utcnow())

    def resolve_user_limit(self, *, user_id: int, plan_code: str, limit_key: str, current_limit: int) -> int:
        if not self.has_active_vote_bonus(user_id, plan_code=plan_code):
            return int(current_limit)
        vote_limit = self.bonus_limit_for(limit_key)
        if vote_limit is None:
            return int(current_limit)
        return max(int(current_limit), int(vote_limit))

    def describe_limit_error(
        self,
        *,
        user_id: int,
        plan_code: str,
        limit_key: str,
        limit_value: int,
        default_message: str,
    ) -> str | None:
        if self.configuration_state() != TOPGG_CONFIGURATION_CONFIGURED:
            return None
        if plan_code not in {PLAN_FREE, PLAN_SUPPORTER}:
            return None
        if self.has_active_vote_bonus(user_id, plan_code=plan_code):
            return None
        vote_limit = self.bonus_limit_for(limit_key)
        if vote_limit is None or vote_limit <= int(limit_value):
            return None
        return (
            f"{default_message} Use `/vote` to unlock a temporary Top.gg Vote Bonus up to {vote_limit}. "
            "Babblebox Plus still goes higher permanently."
        )

    def status_snapshot(self, user_id: int) -> dict[str, Any]:
        plan_code = self.resolve_plan_code(user_id)
        record = self.get_vote_record(user_id) or {}
        active = self.has_active_vote_bonus(user_id, plan_code=plan_code)
        weight = int(record.get("weight", 1) or 1)
        timing_source = self._timing_source_from_record(record)
        timing_note = self._timing_note_for_source(timing_source)
        return {
            "user_id": int(user_id),
            "plan_code": plan_code,
            "plan_label": self.plan_label(plan_code),
            "configuration_state": self.configuration_state(),
            "configuration_message": self.configuration_message(),
            "active": active,
            "eligible": plan_code in {PLAN_FREE, PLAN_SUPPORTER},
            "created_at": record.get("created_at"),
            "expires_at": record.get("expires_at"),
            "weight": weight,
            "reminder_opt_in": bool(record.get("reminder_opt_in", False)),
            "vote_url": self.vote_url(),
            "bonus_limits": dict(TOPGG_VOTE_LIMITS),
            "api_refresh_available": bool(self._topgg_token()),
            "timing_source": timing_source,
            "timing_note": timing_note,
        }

    async def set_reminder_preference(self, user_id: int, *, enabled: bool) -> dict[str, Any]:
        now = _utcnow().isoformat()
        return await self._upsert_vote_record(
            {
                "discord_user_id": int(user_id),
                "reminder_opt_in": bool(enabled),
                "updated_at": now,
            }
        )

    async def refresh_user_vote_status(self, user_id: int, *, force: bool = False) -> tuple[bool, str]:
        if not self.storage_ready:
            return False, self.storage_message()
        if not self._topgg_token():
            return False, "Top.gg refresh is unavailable because `TOPGG_TOKEN` is not configured on this deployment."
        user_id = int(user_id)
        loop_now = asyncio.get_running_loop().time()
        last_refresh = self._refresh_cooldowns.get(user_id, 0.0)
        remaining = TOPGG_REFRESH_COOLDOWN_SECONDS - (loop_now - last_refresh)
        if remaining > 0 and not force:
            return False, f"Vote status refresh is on cooldown. Try again in about {int(remaining)} seconds."
        active, payload, error = await self._fetch_vote_status_payload(user_id)
        if error:
            return False, error
        if force:
            self._refresh_cooldowns.pop(user_id, None)
        else:
            self._refresh_cooldowns[user_id] = loop_now
        if not active:
            await self._mark_vote_inactive(user_id, status="api_inactive")
            return True, "Top.gg vote bonus is not active right now."
        timing_source = str((payload or {}).get("_timing_source") or TOPGG_TIMING_SOURCE_EXACT)
        normalized = self._normalize_vote_record(
            discord_user_id=user_id,
            topgg_vote_id=(self._votes_by_user.get(user_id) or {}).get("topgg_vote_id"),
            created_at=payload.get("created_at"),
            expires_at=payload.get("expires_at"),
            weight=payload.get("weight"),
            webhook_status="api_refresh_legacy_estimated" if timing_source == TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED else "api_refresh",
            webhook_trace_id="api_refresh",
            webhook_received_at=_utcnow().isoformat(),
            webhook_payload_hash=self._payload_hash(payload),
        )
        await self._upsert_vote_record(normalized)
        if timing_source == TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED:
            return (
                True,
                "Top.gg legacy vote confirmed. Babblebox estimated the standard 12-hour vote window because legacy Top.gg does not return exact timestamps.",
            )
        return True, "Top.gg vote status refreshed."

    async def handle_topgg_webhook(self, *, body: bytes, signature: str, trace_id: str | None = None) -> TopggWebhookResult:
        if not self.storage_ready:
            return TopggWebhookResult("unavailable", self.storage_message())
        if self.configuration_state() != TOPGG_CONFIGURATION_CONFIGURED:
            return TopggWebhookResult("unavailable", self.configuration_message())
        self.verify_signature(body=body, signature_header=signature)
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return TopggWebhookResult("invalid", "Top.gg webhook payload was invalid.")
        if self.webhook_mode() == TOPGG_WEBHOOK_MODE_LEGACY:
            return await self._handle_legacy_webhook(payload=payload, body=body, trace_id=trace_id)
        return await self._handle_v2_webhook(payload=payload, body=body, trace_id=trace_id)

    async def _handle_v2_webhook(self, *, payload: dict[str, Any], body: bytes, trace_id: str | None) -> TopggWebhookResult:
        event_type = str(payload.get("type") or "").strip().lower()
        data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
        project = data.get("project") if isinstance(data.get("project"), dict) else {}
        project_platform = str(project.get("platform") or "").strip().lower()
        project_platform_id = str(project.get("platform_id") or project.get("id") or "").strip()
        if project_platform != TOPGG_PLATFORM or project_platform_id != self.project_id():
            return TopggWebhookResult("invalid", "This Top.gg webhook does not target the configured Babblebox project.")
        if event_type == "webhook.test":
            return TopggWebhookResult("processed", "Top.gg webhook test received.")
        if event_type != "vote.create":
            return TopggWebhookResult("invalid", "Babblebox only accepts `vote.create` or `webhook.test` Top.gg events.")

        vote_id = str(data.get("id") or "").strip()
        discord_user_id = _safe_int(((data.get("user") or {}).get("platform_id")))
        if not vote_id or discord_user_id is None:
            return TopggWebhookResult("invalid", "Top.gg vote payload was missing the required event or user identity.")
        event_record = {
            "event_id": vote_id,
            "discord_user_id": discord_user_id,
            "event_type": event_type,
            "received_at": _utcnow().isoformat(),
            "status": "pending",
            "error_text": None,
        }
        inserted = await self.store.record_webhook_event(event_record)
        if not inserted:
            return TopggWebhookResult("duplicate", "That Top.gg vote event was already processed.")
        try:
            normalized = self._normalize_vote_record(
                discord_user_id=discord_user_id,
                topgg_vote_id=vote_id,
                created_at=data.get("created_at"),
                expires_at=data.get("expires_at"),
                weight=data.get("weight"),
                webhook_status="processed",
                webhook_trace_id=trace_id,
                webhook_received_at=event_record["received_at"],
                webhook_payload_hash=hashlib.sha256(body).hexdigest(),
            )
            await self._upsert_vote_record(normalized)
        except ValueError as exc:
            await self.store.finish_webhook_event(vote_id, status="invalid", error_text=str(exc))
            return TopggWebhookResult("invalid", str(exc))
        await self.store.finish_webhook_event(vote_id, status="processed")
        return TopggWebhookResult("processed", "Top.gg vote recorded.")

    async def _handle_legacy_webhook(self, *, payload: dict[str, Any], body: bytes, trace_id: str | None) -> TopggWebhookResult:
        event_type = str(payload.get("type") or "").strip().lower()
        project_platform_id = str(payload.get("bot") or "").strip()
        if project_platform_id != self.project_id():
            return TopggWebhookResult("invalid", "This Top.gg webhook does not target the configured Babblebox project.")
        if event_type == "test":
            return TopggWebhookResult("processed", "Top.gg legacy webhook test received.")
        if event_type != "upvote":
            return TopggWebhookResult("invalid", "Babblebox only accepts `upvote` or `test` Top.gg legacy events.")

        discord_user_id = _safe_int(payload.get("user"))
        if discord_user_id is None:
            return TopggWebhookResult("invalid", "Top.gg vote payload was missing the required user identity.")

        active, status_payload, error = await self._fetch_vote_status_payload(
            discord_user_id,
            timeout_seconds=TOPGG_LEGACY_CONFIRM_TIMEOUT_SECONDS,
            weekend_hint=bool(payload.get("isWeekend", False)),
        )
        if error or not active or not isinstance(status_payload, dict):
            return TopggWebhookResult(
                "unavailable",
                "Top.gg legacy webhook could not be confirmed yet. Top.gg should retry shortly.",
            )

        payload_hash = hashlib.sha256(body).hexdigest()
        timing_source = str(status_payload.get("_timing_source") or TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED)
        try:
            normalized = self._normalize_vote_record(
                discord_user_id=discord_user_id,
                topgg_vote_id=None,
                created_at=status_payload.get("created_at"),
                expires_at=status_payload.get("expires_at"),
                weight=status_payload.get("weight"),
                webhook_status="processed_legacy_estimated" if timing_source == TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED else "processed_legacy",
                webhook_trace_id=trace_id,
                webhook_received_at=_utcnow().isoformat(),
                webhook_payload_hash=payload_hash,
            )
        except ValueError as exc:
            return TopggWebhookResult("invalid", str(exc))

        event_id = self._legacy_event_id(
            discord_user_id=discord_user_id,
            payload_hash=payload_hash,
            created_at=normalized["created_at"],
            estimated=(timing_source == TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED),
        )
        event_record = {
            "event_id": event_id,
            "discord_user_id": discord_user_id,
            "event_type": event_type,
            "received_at": normalized["webhook_received_at"],
            "status": "pending",
            "error_text": None,
        }
        inserted = await self.store.record_webhook_event(event_record)
        if not inserted:
            return TopggWebhookResult("duplicate", "That Top.gg vote event was already processed.")
        normalized["topgg_vote_id"] = event_id
        await self._upsert_vote_record(normalized)
        await self.store.finish_webhook_event(event_id, status="processed")
        if timing_source == TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED:
            return TopggWebhookResult(
                "processed",
                "Top.gg legacy vote recorded using the standard 12-hour vote window.",
            )
        return TopggWebhookResult("processed", "Top.gg legacy vote recorded.")

    def verify_signature(self, *, body: bytes, signature_header: str):
        secret = self._topgg_webhook_secret()
        if not secret:
            raise WebhookVerificationError("Top.gg webhook secret is not configured.")
        if self.webhook_mode() == TOPGG_WEBHOOK_MODE_LEGACY:
            provided = str(signature_header or "").strip()
            if not provided:
                raise WebhookVerificationError("Missing Top.gg authorization header.")
            if not hmac.compare_digest(secret, provided):
                raise WebhookVerificationError("Top.gg authorization mismatch.")
            return
        parts: dict[str, str] = {}
        for chunk in str(signature_header or "").split(","):
            key, _, value = chunk.partition("=")
            if key and value:
                parts[key.strip().lower()] = value.strip()
        timestamp = parts.get("t")
        provided = parts.get("v1")
        if not timestamp or not provided:
            raise WebhookVerificationError("Missing Top.gg signature fields.")
        message = f"{timestamp}.{body.decode('utf-8')}".encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), message, "sha256").hexdigest()
        if not hmac.compare_digest(expected, provided):
            raise WebhookVerificationError("Top.gg signature mismatch.")

    async def _reload_cache(self):
        if self.store is None:
            self._votes_by_user = {}
            return
        rows = await self.store.list_votes()
        self._votes_by_user = {int(record["discord_user_id"]): dict(record) for record in rows}

    async def _upsert_vote_record(self, record: dict[str, Any]) -> dict[str, Any]:
        if self.store is None:
            raise VoteStorageUnavailable(self.storage_message())
        user_id = int(record["discord_user_id"])
        current = dict(self._votes_by_user.get(user_id) or {})

        def merged_value(key: str, default: Any = None):
            return record[key] if key in record else current.get(key, default)

        merged = {
            "discord_user_id": user_id,
            "topgg_vote_id": merged_value("topgg_vote_id"),
            "created_at": merged_value("created_at"),
            "expires_at": merged_value("expires_at"),
            "weight": int(merged_value("weight", 1) or 1),
            "reminder_opt_in": bool(merged_value("reminder_opt_in", False)),
            "last_reminder_sent_at": merged_value("last_reminder_sent_at"),
            "webhook_status": merged_value("webhook_status"),
            "webhook_trace_id": merged_value("webhook_trace_id"),
            "webhook_received_at": merged_value("webhook_received_at"),
            "webhook_payload_hash": merged_value("webhook_payload_hash"),
            "updated_at": merged_value("updated_at", _utcnow().isoformat()),
        }
        saved = await self.store.upsert_vote(merged)
        self._votes_by_user[user_id] = dict(saved)
        return deepcopy(saved)

    async def _mark_vote_inactive(self, user_id: int, *, status: str):
        current = dict(self._votes_by_user.get(int(user_id)) or {})
        if not current:
            return None
        return await self._upsert_vote_record(
            {
                "discord_user_id": int(user_id),
                "expires_at": _utcnow().isoformat(),
                "webhook_status": status,
                "updated_at": _utcnow().isoformat(),
            }
        )

    def _normalize_vote_record(
        self,
        *,
        discord_user_id: int,
        topgg_vote_id: str | None,
        created_at: Any,
        expires_at: Any,
        weight: Any,
        webhook_status: str,
        webhook_trace_id: str | None,
        webhook_received_at: Any,
        webhook_payload_hash: str | None,
    ) -> dict[str, Any]:
        created_dt = _parse_datetime(created_at)
        expires_dt = _parse_datetime(expires_at)
        if created_dt is None or expires_dt is None:
            raise ValueError("Top.gg vote payload was missing `created_at` or `expires_at`.")
        return {
            "discord_user_id": int(discord_user_id),
            "topgg_vote_id": str(topgg_vote_id).strip() if topgg_vote_id else None,
            "created_at": created_dt.isoformat(),
            "expires_at": expires_dt.isoformat(),
            "weight": max(int(weight or 1), 1),
            "webhook_status": str(webhook_status or "processed").strip() or "processed",
            "webhook_trace_id": str(webhook_trace_id).strip() if webhook_trace_id else None,
            "webhook_received_at": _serialize_datetime(webhook_received_at) or _utcnow().isoformat(),
            "webhook_payload_hash": webhook_payload_hash,
            "updated_at": _utcnow().isoformat(),
        }

    async def _fetch_vote_status_payload(
        self,
        user_id: int,
        *,
        timeout_seconds: float | None = None,
        weekend_hint: bool | None = None,
    ) -> tuple[bool, dict[str, Any] | None, str | None]:
        if self.webhook_mode() == TOPGG_WEBHOOK_MODE_LEGACY:
            active, error = await self._check_legacy_vote_status(user_id, timeout_seconds=timeout_seconds)
            if error:
                return False, None, error
            if not active:
                return False, None, None
            cached_payload = self._active_vote_payload_from_record(user_id)
            if cached_payload is not None:
                return True, cached_payload, None
            estimated_payload = self._estimate_legacy_vote_payload(
                user_id=user_id,
                weekend_hint=weekend_hint,
            )
            return True, estimated_payload, None
        token = self._topgg_token()
        if not token:
            return False, None, "Top.gg refresh is unavailable because `TOPGG_TOKEN` is not configured on this deployment."
        session = self._ensure_session()
        request_kwargs: dict[str, Any] = {}
        if timeout_seconds is not None:
            request_kwargs["timeout"] = aiohttp.ClientTimeout(total=max(float(timeout_seconds), 1.0))
        try:
            async with session.get(
                TOPGG_VOTE_STATUS_URL_TEMPLATE.format(user_id=int(user_id)),
                params={"source": TOPGG_PLATFORM},
                headers={"Authorization": f"Bearer {token}"},
                **request_kwargs,
            ) as response:
                if response.status in {204, 404}:
                    return False, None, None
                if response.status in {401, 403}:
                    return False, None, "Top.gg refresh is unavailable because `TOPGG_TOKEN` was rejected."
                if response.status == 429:
                    return False, None, "Top.gg refresh is temporarily unavailable right now."
                if response.status >= 500:
                    return False, None, "Top.gg refresh is temporarily unavailable right now."
                if response.status >= 400:
                    return False, None, "Babblebox could not refresh Top.gg vote status right now."
                payload = await response.json()
        except aiohttp.ClientError:
            return False, None, "Top.gg refresh is temporarily unavailable right now."
        if not isinstance(payload, dict):
            return False, None, "Babblebox could not refresh Top.gg vote status right now."
        created_at = payload.get("created_at")
        expires_at = payload.get("expires_at")
        if not created_at or not expires_at:
            return False, None, None
        payload = dict(payload)
        payload["_timing_source"] = TOPGG_TIMING_SOURCE_EXACT
        return True, payload, None

    async def _check_legacy_vote_status(
        self,
        user_id: int,
        *,
        timeout_seconds: float | None = None,
    ) -> tuple[bool, str | None]:
        token = self._topgg_token()
        if not token:
            return False, "Top.gg refresh is unavailable because `TOPGG_TOKEN` is not configured on this deployment."
        session = self._ensure_session()
        request_kwargs: dict[str, Any] = {}
        if timeout_seconds is not None:
            request_kwargs["timeout"] = aiohttp.ClientTimeout(total=max(float(timeout_seconds), 1.0))
        try:
            async with session.get(
                TOPGG_LEGACY_VOTE_CHECK_URL_TEMPLATE.format(project_id=self.project_id()),
                params={"userId": str(int(user_id))},
                headers={"Authorization": token},
                **request_kwargs,
            ) as response:
                if response.status in {401, 403}:
                    return False, "Top.gg refresh is unavailable because `TOPGG_TOKEN` was rejected."
                if response.status == 429:
                    return False, "Top.gg refresh is temporarily unavailable right now."
                if response.status >= 500:
                    return False, "Top.gg refresh is temporarily unavailable right now."
                if response.status >= 400:
                    return False, "Babblebox could not refresh Top.gg vote status right now."
                payload = await response.json()
        except aiohttp.ClientError:
            return False, "Top.gg refresh is temporarily unavailable right now."
        if not isinstance(payload, dict):
            return False, "Babblebox could not refresh Top.gg vote status right now."
        voted = payload.get("voted")
        return bool(voted in {True, 1, "1", "true", "True"}), None

    def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
            self._session_owner = True
        return self._session

    def _topgg_token(self) -> str:
        return str(os.getenv("TOPGG_TOKEN", "") or "").strip()

    def _topgg_webhook_secret(self) -> str:
        return str(os.getenv("TOPGG_WEBHOOK_SECRET", "") or "").strip()

    def _payload_hash(self, payload: dict[str, Any]) -> str:
        return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()

    def _legacy_event_id(self, *, discord_user_id: int, payload_hash: str, created_at: str, estimated: bool) -> str:
        if not estimated:
            return f"legacy:{int(discord_user_id)}:{created_at}"
        created_dt = _parse_datetime(created_at) or _utcnow()
        bucket = int(created_dt.timestamp()) // TOPGG_LEGACY_DEDUPE_WINDOW_SECONDS
        return f"legacy:{int(discord_user_id)}:{payload_hash[:16]}:{bucket}"

    def _estimate_legacy_vote_payload(
        self,
        *,
        user_id: int,
        weekend_hint: bool | None,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        created_at = now or _utcnow()
        expires_at = created_at + TOPGG_LEGACY_VOTE_WINDOW
        estimated_weight = 2 if weekend_hint is True else 1
        current = self._votes_by_user.get(int(user_id)) or {}
        weight = max(int(current.get("weight", estimated_weight) or estimated_weight), estimated_weight)
        return {
            "created_at": created_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "weight": weight,
            "_timing_source": TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED,
        }

    def _active_vote_payload_from_record(self, user_id: int) -> dict[str, Any] | None:
        record = self._votes_by_user.get(int(user_id))
        if record is None:
            return None
        expires_at = _parse_datetime(record.get("expires_at"))
        created_at = _parse_datetime(record.get("created_at"))
        if expires_at is None or created_at is None or expires_at <= _utcnow():
            return None
        return {
            "created_at": created_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "weight": int(record.get("weight", 1) or 1),
            "_timing_source": self._timing_source_from_record(record),
        }

    def _timing_source_from_record(self, record: dict[str, Any]) -> str:
        webhook_status = str((record or {}).get("webhook_status") or "").strip().lower()
        if "legacy_estimated" in webhook_status:
            return TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED
        return TOPGG_TIMING_SOURCE_EXACT

    def _timing_note_for_source(self, timing_source: str) -> str | None:
        if timing_source != TOPGG_TIMING_SOURCE_LEGACY_ESTIMATED:
            return None
        return (
            "Legacy Top.gg mode: this vote window is estimated from the standard 12-hour voting cadence because "
            "legacy API responses do not include exact expiry timestamps."
        )

    async def _reminder_loop(self):
        while True:
            try:
                await asyncio.sleep(TOPGG_REMINDER_LOOP_SECONDS)
                await self._deliver_vote_reminders()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning("Vote reminder loop failed: error_type=%s", type(exc).__name__)

    async def _deliver_vote_reminders(self):
        if not self.storage_ready:
            return
        now = _utcnow()
        for record in list(self._votes_by_user.values()):
            if not bool(record.get("reminder_opt_in", False)):
                continue
            expires_at = _parse_datetime(record.get("expires_at"))
            if expires_at is None or expires_at > now:
                continue
            last_sent = _parse_datetime(record.get("last_reminder_sent_at"))
            if last_sent is not None and last_sent >= expires_at:
                continue
            user_id = int(record.get("discord_user_id", 0) or 0)
            if user_id <= 0:
                continue
            user = await self._fetch_user(user_id)
            if user is None:
                continue
            try:
                await user.send(
                    embed=self._vote_reminder_embed(user_id, expires_at=expires_at),
                    view=_VoteReminderView(self.vote_url()),
                )
            except Exception:
                continue
            await self._upsert_vote_record(
                {
                    "discord_user_id": user_id,
                    "last_reminder_sent_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            )

    async def _fetch_user(self, user_id: int):
        get_user = getattr(self.bot, "get_user", None)
        user = get_user(user_id) if callable(get_user) else None
        if user is not None:
            return user
        fetch_user = getattr(self.bot, "fetch_user", None)
        if not callable(fetch_user):
            return None
        with contextlib.suppress(Exception):
            return await fetch_user(user_id)
        return None

    def _vote_reminder_embed(self, user_id: int, *, expires_at: datetime) -> discord.Embed:
        plan_code = self.resolve_plan_code(user_id)
        description = (
            "Your Babblebox Vote Bonus window has ended. Vote again on Top.gg to restore the temporary utility boost.\n"
            "Open `/vote` any time to refresh status or turn reminders off."
        )
        embed = discord.Embed(
            title="Vote Bonus Ready Again",
            description=description,
            color=discord.Color.gold(),
            timestamp=expires_at,
        )
        embed.add_field(name="Current Plan", value=self.plan_label(plan_code), inline=True)
        embed.add_field(name="Vote Page", value=self.vote_url(), inline=False)
        embed.set_footer(text="Babblebox Vote Bonus")
        return embed


class _VoteReminderView(discord.ui.View):
    def __init__(self, vote_url: str):
        super().__init__(timeout=600)
        self.add_item(discord.ui.Button(label="Vote on Top.gg", style=discord.ButtonStyle.link, url=vote_url))
