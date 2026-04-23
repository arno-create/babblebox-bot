from __future__ import annotations

import asyncio
import contextlib
import html
import importlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from threading import Lock, Thread
from typing import Any

from flask import Flask, abort, jsonify, request, send_file, send_from_directory

from babblebox.premium_provider import PremiumProviderError, WebhookVerificationError
from babblebox.runtime_health import public_bot_service_snapshots


LOGGER = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parent.parent
INDEX_PATH = ROOT_DIR / "index.html"
HELP_PATH = ROOT_DIR / "help.html"
PRIVACY_PATH = ROOT_DIR / "privacy.html"
TERMS_PATH = ROOT_DIR / "terms.html"
SITEMAP_PATH = ROOT_DIR / "sitemap.xml"
PREMIUM_QUERY_VALUE_LIMIT = 1024
PREMIUM_WEBHOOK_MAX_BYTES = 65536
TOPGG_WEBHOOK_MAX_BYTES = 65536
PUBLIC_FILE_CACHE_CONTROL = "public, max-age=300, must-revalidate"
DEFAULT_HTTP_PORT = 10000
DEFAULT_WAITRESS_THREADS = 4
INGRESS_MODE = "embedded_waitress"

_premium_runtime = None
_vote_runtime = None
_bot_runtime = None
_server_thread: Thread | None = None
_server_lock = Lock()


def _empty_patreon_webhook_stats() -> dict[str, Any]:
    return {
        "total": 0,
        "processed": 0,
        "duplicate": 0,
        "unresolved": 0,
        "unavailable": 0,
        "invalid": 0,
        "error": 0,
        "invalid_signature_count": 0,
        "last_status": None,
        "last_http_status": None,
        "last_event_at": None,
    }


_patreon_webhook_stats = _empty_patreon_webhook_stats()


class PremiumRuntimeUnavailableError(RuntimeError):
    pass


class VoteRuntimeUnavailableError(RuntimeError):
    pass


def create_app() -> Flask:
    flask_app = Flask(__name__)
    _register_routes(flask_app)
    return flask_app


def set_premium_runtime(service):
    global _premium_runtime, _bot_runtime
    _premium_runtime = service
    if service is not None and getattr(service, "bot", None) is not None:
        _bot_runtime = service.bot


def set_vote_runtime(service):
    global _vote_runtime, _bot_runtime
    _vote_runtime = service
    if service is not None and getattr(service, "bot", None) is not None:
        _bot_runtime = service.bot


def set_bot_runtime(bot):
    global _bot_runtime
    _bot_runtime = bot


def reset_patreon_webhook_stats():
    global _patreon_webhook_stats
    _patreon_webhook_stats = _empty_patreon_webhook_stats()


def get_patreon_webhook_stats() -> dict[str, Any]:
    return dict(_patreon_webhook_stats)


def _persist_patreon_webhook_monitor(status: str, *, status_code: int, invalid_signature: bool = False):
    service = _premium_runtime
    recorder = getattr(service, "record_webhook_monitor_event", None)
    loop = getattr(getattr(service, "bot", None), "loop", None)
    if not callable(recorder) or loop is None or bool(getattr(loop, "is_closed", lambda: False)()):
        return
    try:
        future = asyncio.run_coroutine_threadsafe(
            recorder(
                status=status,
                status_code=int(status_code),
                invalid_signature=invalid_signature,
            ),
            loop,
        )
        future.result(timeout=10)
    except Exception as exc:
        LOGGER.warning(
            "Premium webhook monitor persistence failed: error_type=%s",
            type(exc).__name__,
        )


def _record_patreon_webhook(status: str, *, status_code: int, invalid_signature: bool = False):
    normalized = str(status or "error").strip().lower()
    if normalized not in {"processed", "duplicate", "unresolved", "unavailable", "invalid", "error"}:
        normalized = "error"
    _patreon_webhook_stats["total"] += 1
    _patreon_webhook_stats[normalized] += 1
    if invalid_signature:
        _patreon_webhook_stats["invalid_signature_count"] += 1
    _patreon_webhook_stats["last_status"] = normalized
    _patreon_webhook_stats["last_http_status"] = int(status_code)
    _patreon_webhook_stats["last_event_at"] = datetime.now(timezone.utc).isoformat()
    _persist_patreon_webhook_monitor(normalized, status_code=status_code, invalid_signature=invalid_signature)


def _run_premium_coroutine(coro):
    service = _premium_runtime
    loop = getattr(getattr(service, "bot", None), "loop", None)
    if service is None:
        raise PremiumRuntimeUnavailableError("Premium runtime is not attached.")
    if loop is None or bool(getattr(loop, "is_closed", lambda: False)()):
        raise PremiumRuntimeUnavailableError("Premium runtime loop is unavailable.")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=45)


def _run_vote_coroutine(coro):
    service = _vote_runtime
    loop = getattr(getattr(service, "bot", None), "loop", None)
    if service is None:
        raise VoteRuntimeUnavailableError("Vote runtime is not attached.")
    if loop is None or bool(getattr(loop, "is_closed", lambda: False)()):
        raise VoteRuntimeUnavailableError("Vote runtime loop is unavailable.")
    future = asyncio.run_coroutine_threadsafe(coro, loop)
    return future.result(timeout=45)


def _apply_security_headers(response, *, cache_control: str | None = None, no_store: bool = False):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Frame-Options"] = "DENY"
    if no_store:
        response.headers["Cache-Control"] = "no-store, max-age=0"
        response.headers["Pragma"] = "no-cache"
    elif cache_control:
        response.headers["Cache-Control"] = cache_control
    return response


def _json_response(payload: dict[str, Any], *, status_code: int, no_store: bool = False, cache_control: str | None = None):
    response = jsonify(payload)
    response.status_code = status_code
    return _apply_security_headers(response, cache_control=cache_control, no_store=no_store)


def _public_base_url() -> str:
    return str(os.getenv("PUBLIC_BASE_URL", "") or "").strip().rstrip("/")


def _public_base_url_configured() -> bool:
    return bool(_public_base_url())


def _resolve_bind_host() -> str:
    explicit = str(os.getenv("BABBLEBOX_WEB_HOST", "") or "").strip()
    if explicit:
        return explicit
    if _public_base_url_configured():
        return "0.0.0.0"
    return "127.0.0.1"


def _resolve_port() -> int:
    try:
        return int(os.getenv("PORT", str(DEFAULT_HTTP_PORT)))
    except ValueError:
        return DEFAULT_HTTP_PORT


def _resolve_waitress_threads() -> int:
    try:
        configured = int(os.getenv("BABBLEBOX_WEB_THREADS", str(DEFAULT_WAITRESS_THREADS)))
    except ValueError:
        configured = DEFAULT_WAITRESS_THREADS
    return max(2, min(configured, 32))


def _load_waitress_server():
    try:
        module = importlib.import_module("waitress")
    except Exception as exc:
        raise RuntimeError(
            "Waitress is required for Babblebox's embedded HTTP surface. "
            "Install the current requirements before starting the web runtime."
        ) from exc
    serve = getattr(module, "serve", None)
    if not callable(serve):
        raise RuntimeError("Waitress is installed but does not expose a callable `serve` entrypoint.")
    return serve


def _send_public_file(path: Path):
    if not path.exists() or not path.is_file():
        abort(404)
    response = send_file(path)
    return _apply_security_headers(response, cache_control=PUBLIC_FILE_CACHE_CONTROL)


def _serve_public_root_file(path: Path):
    return _send_public_file(path)


def _safe_asset_path(filename: str) -> bool:
    normalized = str(filename or "").replace("\\", "/")
    parts = PurePosixPath(normalized).parts
    return bool(parts) and all(part not in {"", ".", ".."} and not part.startswith(".") for part in parts)


def _bounded_query_value(name: str) -> str:
    value = str(request.args.get(name) or "").strip()
    if len(value) > PREMIUM_QUERY_VALUE_LIMIT:
        return ""
    return value


def _safe_issue_append(issues: list[str], *codes: str | None):
    for code in codes:
        cleaned = str(code or "").strip()
        if cleaned and cleaned not in issues:
            issues.append(cleaned)


def _safe_provider_monitor_summary(service: Any) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    getter = getattr(service, "public_provider_monitor_summary", None)
    if callable(getter):
        with contextlib.suppress(Exception):
            candidate = getter()
            if isinstance(candidate, dict):
                summary = dict(candidate)
    diagnostics = _safe_premium_diagnostics(service)
    process_local = get_patreon_webhook_stats()
    default_status = "ready" if summary else "process_local_only"
    if not summary and str(diagnostics.get("startup_state") or "") == "disabled":
        default_status = "disabled"
    return {
        "status": str(summary.get("status") or default_status),
        "last_webhook_status": summary.get("last_webhook_status") or process_local.get("last_status"),
        "last_webhook_http_status": summary.get("last_webhook_http_status") or process_local.get("last_http_status"),
        "last_webhook_at": summary.get("last_webhook_at") or process_local.get("last_event_at"),
        "invalid_signature_count": max(
            int(summary.get("invalid_signature_count", 0) or 0),
            int(process_local.get("invalid_signature_count", 0) or 0),
        ),
        "unresolved_issue_count": int(summary.get("unresolved_issue_count", 0) or 0),
        "recent_unavailable_count": max(
            int(summary.get("recent_unavailable_count", 0) or 0),
            int(process_local.get("unavailable", 0) or 0),
        ),
        "recent_server_error_count": max(
            int(summary.get("recent_server_error_count", 0) or 0),
            int(process_local.get("error", 0) or 0),
        ),
        "last_issue_type": summary.get("last_issue_type"),
        "last_issue_at": summary.get("last_issue_at"),
        "stale": bool(summary.get("stale", False)),
    }


def _safe_premium_diagnostics(service: Any) -> dict[str, Any]:
    getter = getattr(service, "provider_diagnostics", None)
    if callable(getter):
        with contextlib.suppress(Exception):
            candidate = getter()
            if isinstance(candidate, dict):
                return dict(candidate)
    return {}


def _safe_confessions_readiness_summary(bot: Any) -> dict[str, Any]:
    service = getattr(bot, "confessions_service", None) if bot is not None else None
    if service is None:
        return {
            "status": "missing",
            "ready": False,
            "configured_guild_count": 0,
            "review_required_guild_count": 0,
            "privacy_ready": False,
            "review_ready": False,
            "support_ready": False,
            "review_issue_counts": {},
            "support_issue_counts": {},
            "issue_codes": ("confessions_service_missing",),
        }
    snapshot = {
        "status": "ready" if getattr(service, "storage_ready", False) else "degraded",
        "ready": bool(getattr(service, "storage_ready", False)),
        "configured_guild_count": 0,
        "review_required_guild_count": 0,
        "privacy_ready": bool(getattr(service, "storage_ready", False)),
        "review_ready": True,
        "support_ready": True,
        "review_issue_counts": {},
        "support_issue_counts": {},
        "issue_codes": () if getattr(service, "storage_ready", False) else ("confessions_storage_unavailable",),
    }
    getter = getattr(service, "readiness_snapshot", None)
    if callable(getter):
        with contextlib.suppress(Exception):
            candidate = getter()
            if isinstance(candidate, dict):
                snapshot = {
                    "status": str(candidate.get("status") or snapshot["status"]),
                    "ready": bool(candidate.get("ready", snapshot["ready"])),
                    "configured_guild_count": int(candidate.get("configured_guild_count", snapshot["configured_guild_count"]) or 0),
                    "review_required_guild_count": int(candidate.get("review_required_guild_count", snapshot["review_required_guild_count"]) or 0),
                    "privacy_ready": bool(candidate.get("privacy_ready", snapshot["privacy_ready"])),
                    "review_ready": bool(candidate.get("review_ready", snapshot["review_ready"])),
                    "support_ready": bool(candidate.get("support_ready", snapshot["support_ready"])),
                    "review_issue_counts": dict(candidate.get("review_issue_counts") or {}),
                    "support_issue_counts": dict(candidate.get("support_issue_counts") or {}),
                    "issue_codes": tuple(candidate.get("issue_codes") or ()),
                }
    return snapshot


def _runtime_readiness_snapshot() -> dict[str, Any]:
    service = _premium_runtime
    bot = _bot_runtime or getattr(service, "bot", None)
    loop = getattr(bot, "loop", None)
    runtime_attached = bot is not None
    ready_probe = getattr(bot, "is_ready", None)
    bot_ready = bool(runtime_attached and (ready_probe() if callable(ready_probe) else True))
    loop_attached = loop is not None
    loop_closed = bool(loop_attached and getattr(loop, "is_closed", lambda: False)())
    services = public_bot_service_snapshots(bot)
    confessions = _safe_confessions_readiness_summary(bot)
    required_service_failures = sorted(
        name
        for name, snapshot in services.items()
        if snapshot["production_like_backend"] and not snapshot["storage_ready"]
    )
    if bot_ready:
        required_service_failures = sorted(
            {*required_service_failures, *[name for name, snapshot in services.items() if not snapshot["attached"]]}
        )
    premium_snapshot = services["premium"]
    premium_diagnostics = _safe_premium_diagnostics(service)
    patreon = getattr(service, "patreon", None)
    patreon_configured = bool(
        premium_diagnostics.get("patreon_configured")
        if "patreon_configured" in premium_diagnostics
        else (callable(getattr(patreon, "configured", None)) and patreon.configured())
    )
    premium_startup_state = str(premium_diagnostics.get("startup_state") or ("enabled_safe" if patreon_configured else "enabled_unsafe"))
    provider_monitor = _safe_provider_monitor_summary(service)
    public_expected = _public_base_url_configured()
    public_premium_routes_ready = not public_expected or (
        premium_startup_state == "disabled"
        or (
            premium_snapshot["attached"]
            and loop_attached
            and not loop_closed
            and premium_snapshot["storage_ready"]
            and premium_startup_state == "enabled_safe"
        )
    )
    issues: list[str] = []
    if not runtime_attached:
        _safe_issue_append(issues, "runtime_missing")
    if runtime_attached and not bot_ready:
        _safe_issue_append(issues, "runtime_not_ready")
    if not loop_attached:
        _safe_issue_append(issues, "runtime_loop_unavailable")
    elif loop_closed:
        _safe_issue_append(issues, "runtime_loop_closed")
    if public_expected and premium_startup_state == "enabled_unsafe":
        _safe_issue_append(issues, "premium_patreon_not_configured")
    if public_expected and not public_premium_routes_ready:
        _safe_issue_append(issues, "public_premium_routes_not_ready")
    for name in required_service_failures:
        snapshot = services.get(name) or {}
        _safe_issue_append(issues, *tuple(snapshot.get("issue_codes") or ()))
    _safe_issue_append(issues, *tuple(confessions.get("issue_codes") or ()))
    ready = bool(
        runtime_attached
        and bot_ready
        and loop_attached
        and not loop_closed
        and not required_service_failures
        and public_premium_routes_ready
        and bool(confessions.get("ready", True))
    )
    return {
        "status": "ok" if ready else "degraded",
        "live": True,
        "ready": ready,
        "ingress_mode": INGRESS_MODE,
        "public_base_url_configured": public_expected,
        "runtime": {
            "bot_runtime_attached": runtime_attached,
            "bot_ready": bot_ready,
            "bot_loop_attached": loop_attached,
            "bot_loop_closed": loop_closed,
        },
        "issues": tuple(issues),
        "issue_counts": {"total": len(issues)},
        "required_service_failures": required_service_failures,
        "services": services,
        "premium": {
            "runtime_attached": premium_snapshot["attached"],
            "storage_ready": premium_snapshot["storage_ready"],
            "public_routes_ready": public_premium_routes_ready,
            "patreon_configured": patreon_configured,
            "startup_state": premium_startup_state,
            "provider_monitor": provider_monitor,
        },
        "confessions": confessions,
        "website": "https://arno-create.github.io/babblebox-bot/",
    }


def _public_health_payload(*, detailed: bool) -> dict[str, Any]:
    readiness = _runtime_readiness_snapshot()
    payload = {
        "status": readiness["status"],
        "live": readiness["live"],
        "ready": readiness["ready"],
        "runtime_ready": readiness["ready"],
        "ingress_mode": readiness["ingress_mode"],
        "public_base_url_configured": readiness["public_base_url_configured"],
        "issues": readiness["issues"],
        "issue_counts": readiness["issue_counts"],
        "required_service_failures": readiness["required_service_failures"],
        "required_services_ready": not readiness["required_service_failures"],
        "premium": readiness["premium"],
        "confessions": readiness["confessions"],
        "website": readiness["website"],
        "bot_runtime_attached": readiness["runtime"]["bot_runtime_attached"],
        "bot_ready": readiness["runtime"]["bot_ready"],
        "bot_loop_attached": readiness["runtime"]["bot_loop_attached"],
        "bot_loop_closed": readiness["runtime"]["bot_loop_closed"],
        "premium_runtime_attached": readiness["premium"]["runtime_attached"],
        "premium_bot_loop_attached": readiness["runtime"]["bot_loop_attached"],
        "premium_bot_loop_closed": readiness["runtime"]["bot_loop_closed"],
        "premium_storage_ready": readiness["premium"]["storage_ready"],
        "patreon_configured": readiness["premium"]["patreon_configured"],
        "public_premium_routes_ready": readiness["premium"]["public_routes_ready"],
        "patreon_webhook_stats": get_patreon_webhook_stats(),
    }
    if detailed:
        payload["runtime"] = readiness["runtime"]
        payload["services"] = readiness["services"]
    else:
        payload["services"] = {
            name: {
                "status": snapshot["status"],
                "configured_backend": snapshot["configured_backend"],
                "storage_ready": snapshot["storage_ready"],
            }
            for name, snapshot in readiness["services"].items()
        }
    return payload


def _render_premium_page(*, title: str, message: str, tone: str = "info", status_code: int = 200):
    accent = {
        "success": "#1d7f49",
        "warning": "#a86a00",
        "danger": "#a12727",
        "info": "#275ca1",
    }.get(tone, "#275ca1")
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)} | Babblebox Premium</title>
  <style>
    :root {{
      color-scheme: light;
      --accent: {accent};
      --bg: #f5f2ea;
      --card: #fffdf8;
      --text: #1e1b18;
      --muted: #645c53;
      --border: #ddd1c1;
    }}
    body {{
      margin: 0;
      font-family: "Segoe UI", system-ui, sans-serif;
      background: radial-gradient(circle at top, #fffaf0 0%, var(--bg) 58%, #efe5d6 100%);
      color: var(--text);
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    main {{
      width: min(560px, 100%);
      background: var(--card);
      border: 1px solid var(--border);
      border-top: 6px solid var(--accent);
      border-radius: 18px;
      padding: 28px;
      box-shadow: 0 18px 50px rgba(49, 35, 12, 0.08);
    }}
    h1 {{
      margin: 0 0 12px;
      font-size: 1.8rem;
      line-height: 1.15;
    }}
    p {{
      margin: 0;
      line-height: 1.55;
      color: var(--muted);
    }}
    small {{
      display: block;
      margin-top: 18px;
      color: var(--muted);
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(title)}</h1>
    <p>{html.escape(message)}</p>
    <small>Return to Discord and open <code>/premium status</code> if you want to confirm the current entitlement state.</small>
  </main>
</body>
</html>"""
    return body, status_code, {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-store, max-age=0",
        "Pragma": "no-cache",
        "Referrer-Policy": "no-referrer",
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Content-Security-Policy": "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
    }


def _premium_json(*, status: str, message: str, status_code: int):
    return _json_response({"status": status, "message": message}, status_code=status_code, no_store=True)


def _premium_webhook_json(*, status: str, message: str, status_code: int, invalid_signature: bool = False):
    _record_patreon_webhook(status, status_code=status_code, invalid_signature=invalid_signature)
    return _premium_json(status=status, message=message, status_code=status_code)


def _vote_webhook_json(*, status: str, message: str, status_code: int):
    return _json_response({"status": status, "message": message}, status_code=status_code, no_store=True)


def _register_routes(flask_app: Flask):
    @flask_app.get("/")
    def home():
        if INDEX_PATH.exists():
            return _send_public_file(INDEX_PATH)

        return _json_response(
            {
                "bot": "Babblebox",
                "status": "online",
                "website": "https://arno-create.github.io/babblebox-bot/",
                "invite": "https://discord.com/oauth2/authorize?client_id=1480903089518022739",
            },
            status_code=200,
            cache_control=PUBLIC_FILE_CACHE_CONTROL,
        )

    @flask_app.get("/livez")
    def livez():
        return _json_response(
            {
                "status": "ok",
                "live": True,
                "ingress_mode": INGRESS_MODE,
            },
            status_code=200,
            no_store=True,
        )

    @flask_app.get("/health")
    def health():
        payload = _public_health_payload(detailed=False)
        return _json_response(
            payload,
            status_code=200 if payload["ready"] else 503,
            no_store=True,
        )

    @flask_app.get("/readyz")
    def readyz():
        payload = _public_health_payload(detailed=True)
        return _json_response(
            payload,
            status_code=200 if payload["ready"] else 503,
            no_store=True,
        )

    @flask_app.get("/premium/patreon/callback")
    def premium_patreon_callback():
        service = _premium_runtime
        if service is None:
            return _render_premium_page(
                title="Premium unavailable",
                message="Babblebox premium is not attached on this deployment right now.",
                tone="warning",
                status_code=503,
            )
        state_token = _bounded_query_value("state")
        code = _bounded_query_value("code") or None
        error = _bounded_query_value("error") or None
        if not state_token:
            return _render_premium_page(
                title="Link failed",
                message="Patreon did not return a valid link state. Start again from `/premium link` in Discord.",
                tone="warning",
                status_code=400,
            )
        try:
            result = _run_premium_coroutine(
                service.complete_link_callback(state_token=state_token, code=code, error=error)
            )
        except PremiumRuntimeUnavailableError:
            return _render_premium_page(
                title="Premium unavailable",
                message="Babblebox premium is not attached on this deployment right now.",
                tone="warning",
                status_code=503,
            )
        except Exception:
            return _render_premium_page(
                title="Link failed",
                message="Babblebox could not finish Patreon linking safely right now. Start again from `/premium link` in Discord.",
                tone="danger",
                status_code=500,
            )
        tone = "success" if str(result.get("title", "")).casefold() == "patreon linked" else "info"
        status_code = 200 if tone == "success" else 400
        return _render_premium_page(
            title=str(result.get("title") or "Premium status"),
            message=str(result.get("message") or "No premium result was returned."),
            tone=tone,
            status_code=status_code,
        )

    @flask_app.post("/premium/patreon/webhook")
    def premium_patreon_webhook():
        service = _premium_runtime
        if service is None:
            return _premium_webhook_json(status="unavailable", message="Premium runtime is not attached.", status_code=503)
        event_type = str(request.headers.get("X-Patreon-Event") or "").strip()
        signature = str(request.headers.get("X-Patreon-Signature") or "").strip()
        if not event_type or not signature:
            return _premium_webhook_json(status="invalid", message="Missing Patreon webhook headers.", status_code=400)
        content_length = int(request.content_length or 0)
        if content_length > PREMIUM_WEBHOOK_MAX_BYTES:
            return _premium_webhook_json(status="invalid", message="Patreon webhook payload exceeded the safe size limit.", status_code=413)
        body = request.get_data(cache=False)
        if len(body) > PREMIUM_WEBHOOK_MAX_BYTES:
            return _premium_webhook_json(status="invalid", message="Patreon webhook payload exceeded the safe size limit.", status_code=413)
        try:
            result = _run_premium_coroutine(
                service.handle_patreon_webhook(body=body, event_type=event_type, signature=signature)
            )
        except PremiumRuntimeUnavailableError:
            return _premium_webhook_json(status="unavailable", message="Premium runtime is not attached.", status_code=503)
        except Exception as exc:
            if isinstance(exc, WebhookVerificationError) or exc.__class__.__name__ == "WebhookVerificationError":
                return _premium_webhook_json(
                    status="invalid",
                    message="Patreon webhook signature was invalid.",
                    status_code=400,
                    invalid_signature=True,
                )
            if isinstance(exc, PremiumProviderError):
                status_code = int(exc.status_code or 500)
                safe_message = str(exc.safe_message or "Babblebox could not process the Patreon webhook safely.")
                status = "invalid" if status_code < 500 else "error"
                return _premium_webhook_json(status=status, message=safe_message, status_code=status_code)
            return _premium_webhook_json(status="error", message="Babblebox could not process the Patreon webhook safely.", status_code=500)
        if result.outcome == "processed":
            return _premium_webhook_json(status="processed", message=result.message, status_code=200)
        if result.outcome == "duplicate":
            return _premium_webhook_json(status="duplicate", message=result.message, status_code=200)
        if result.outcome == "unresolved":
            return _premium_webhook_json(status="unresolved", message=result.message, status_code=200)
        if result.outcome == "unavailable":
            return _premium_webhook_json(status="unavailable", message=result.message, status_code=503)
        if result.outcome == "invalid":
            status_code = 413 if "safe size limit" in result.message else 400
            return _premium_webhook_json(status="invalid", message=result.message, status_code=status_code)
        return _premium_webhook_json(status="error", message="Babblebox could not process the Patreon webhook safely.", status_code=500)

    @flask_app.post("/topgg/webhook")
    def topgg_webhook():
        service = _vote_runtime
        if service is None:
            return _vote_webhook_json(status="unavailable", message="Vote runtime is not attached.", status_code=503)
        signature = str(request.headers.get("x-topgg-signature") or request.headers.get("Authorization") or "").strip()
        if not signature:
            return _vote_webhook_json(status="invalid", message="Missing Top.gg webhook verification header.", status_code=400)
        content_length = int(request.content_length or 0)
        if content_length > TOPGG_WEBHOOK_MAX_BYTES:
            return _vote_webhook_json(status="invalid", message="Top.gg webhook payload exceeded the safe size limit.", status_code=413)
        body = request.get_data(cache=False)
        if len(body) > TOPGG_WEBHOOK_MAX_BYTES:
            return _vote_webhook_json(status="invalid", message="Top.gg webhook payload exceeded the safe size limit.", status_code=413)
        trace_id = (
            str(
                request.headers.get("x-topgg-trace")
                or request.headers.get("x-request-id")
                or request.headers.get("cf-ray")
                or ""
            ).strip()
            or None
        )
        try:
            result = _run_vote_coroutine(
                service.handle_topgg_webhook(body=body, signature=signature, trace_id=trace_id)
            )
        except VoteRuntimeUnavailableError:
            return _vote_webhook_json(status="unavailable", message="Vote runtime is not attached.", status_code=503)
        except Exception as exc:
            if isinstance(exc, WebhookVerificationError) or exc.__class__.__name__ == "WebhookVerificationError":
                return _vote_webhook_json(status="invalid", message="Top.gg webhook signature was invalid.", status_code=400)
            return _vote_webhook_json(status="error", message="Babblebox could not process the Top.gg webhook safely.", status_code=500)
        if result.outcome == "processed":
            return _vote_webhook_json(status="processed", message=result.message, status_code=200)
        if result.outcome == "duplicate":
            return _vote_webhook_json(status="duplicate", message=result.message, status_code=200)
        if result.outcome == "unavailable":
            return _vote_webhook_json(status="unavailable", message=result.message, status_code=503)
        if result.outcome == "invalid":
            status_code = 413 if "safe size limit" in result.message else 400
            return _vote_webhook_json(status="invalid", message=result.message, status_code=status_code)
        return _vote_webhook_json(status="error", message="Babblebox could not process the Top.gg webhook safely.", status_code=500)

    @flask_app.get("/assets/<path:filename>")
    def assets(filename: str):
        assets_dir = ROOT_DIR / "assets"
        if not assets_dir.exists() or not _safe_asset_path(filename):
            abort(404)
        response = send_from_directory(assets_dir, filename)
        return _apply_security_headers(response, cache_control=PUBLIC_FILE_CACHE_CONTROL)

    @flask_app.get("/help.html")
    def help_page():
        return _serve_public_root_file(HELP_PATH)

    @flask_app.get("/privacy.html")
    def privacy_page():
        return _serve_public_root_file(PRIVACY_PATH)

    @flask_app.get("/terms.html")
    def terms_page():
        return _serve_public_root_file(TERMS_PATH)

    @flask_app.get("/sitemap.xml")
    def sitemap():
        return _serve_public_root_file(SITEMAP_PATH)


def run(*, host: str | None = None, port: int | None = None):
    serve = _load_waitress_server()
    resolved_host = host or _resolve_bind_host()
    resolved_port = int(port or _resolve_port())
    threads = _resolve_waitress_threads()
    LOGGER.info(
        "Starting Babblebox HTTP surface: server=waitress host=%s port=%s threads=%s",
        resolved_host,
        resolved_port,
        threads,
    )
    serve(
        app,
        host=resolved_host,
        port=resolved_port,
        threads=threads,
        ident="Babblebox",
    )


def start_http_server() -> Thread:
    global _server_thread
    with _server_lock:
        if _server_thread is not None and _server_thread.is_alive():
            return _server_thread
        thread = Thread(target=run, daemon=True, name="babblebox-http")
        _server_thread = thread
        thread.start()
        return thread


def keep_alive():
    return start_http_server()


app = create_app()
