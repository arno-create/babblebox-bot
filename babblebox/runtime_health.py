from __future__ import annotations

from typing import Any


PERMISSIVE_STORAGE_BACKENDS = frozenset({"memory", "test", "dev"})
SERVICE_SPECS: tuple[tuple[str, str], ...] = (
    ("premium", "premium_service"),
    ("confessions", "confessions_service"),
    ("shield", "shield_service"),
    ("admin", "admin_service"),
    ("utility", "utility_service"),
    ("profile", "profile_service"),
    ("question_drops", "question_drops_service"),
)


def _normalize_backend_name(value: Any) -> str | None:
    cleaned = str(value or "").strip().lower()
    return cleaned or None


def _service_store(service: Any) -> Any:
    return getattr(service, "store", None)


def service_configured_backend(service: Any) -> str | None:
    configured = _normalize_backend_name(getattr(service, "storage_backend_preference", None))
    if configured:
        return configured
    store = _service_store(service)
    return _normalize_backend_name(getattr(store, "backend_preference", None))


def service_active_backend(service: Any) -> str | None:
    store = _service_store(service)
    active = _normalize_backend_name(getattr(store, "backend_name", None))
    if active:
        return active
    return _normalize_backend_name(getattr(service, "storage_backend_preference", None))


def service_storage_error(service: Any) -> str | None:
    raw = getattr(service, "storage_error", None)
    if not raw:
        raw = getattr(service, "_startup_storage_error", None)
    cleaned = str(raw or "").strip()
    return cleaned or None


def is_permissive_storage_backend(backend_name: str | None) -> bool:
    return bool(backend_name and backend_name in PERMISSIVE_STORAGE_BACKENDS)


def service_requires_fail_fast(service: Any) -> bool:
    configured = service_configured_backend(service) or service_active_backend(service)
    return bool(configured) and not is_permissive_storage_backend(configured)


def service_snapshot(service: Any) -> dict[str, Any]:
    configured_backend = service_configured_backend(service)
    active_backend = service_active_backend(service)
    production_like = bool(configured_backend) and not is_permissive_storage_backend(configured_backend)
    return {
        "attached": service is not None,
        "configured_backend": configured_backend,
        "active_backend": active_backend,
        "production_like_backend": production_like,
        "storage_ready": bool(service is not None and getattr(service, "storage_ready", False)),
        "storage_error": service_storage_error(service),
    }


def _service_issue_codes(name: str, snapshot: dict[str, Any]) -> tuple[str, ...]:
    if not snapshot["attached"]:
        return (f"{name}_service_missing", "service_missing")
    if not snapshot["storage_ready"]:
        return (f"{name}_service_storage_unavailable", "service_storage_unavailable")
    return ()


def public_service_snapshot(name: str, service: Any) -> dict[str, Any]:
    snapshot = service_snapshot(service)
    issues = _service_issue_codes(name, snapshot)
    status = "missing"
    if snapshot["attached"]:
        status = "ready" if snapshot["storage_ready"] else "degraded"
    return {
        "status": status,
        "attached": snapshot["attached"],
        "configured_backend": snapshot["configured_backend"],
        "active_backend": snapshot["active_backend"],
        "production_like_backend": snapshot["production_like_backend"],
        "storage_ready": snapshot["storage_ready"],
        "issue_codes": issues,
    }


def bot_service_snapshots(bot: Any) -> dict[str, dict[str, Any]]:
    snapshots: dict[str, dict[str, Any]] = {}
    for name, attr_name in SERVICE_SPECS:
        service = getattr(bot, attr_name, None) if bot is not None else None
        snapshots[name] = service_snapshot(service)
    return snapshots


def public_bot_service_snapshots(bot: Any) -> dict[str, dict[str, Any]]:
    snapshots: dict[str, dict[str, Any]] = {}
    for name, attr_name in SERVICE_SPECS:
        service = getattr(bot, attr_name, None) if bot is not None else None
        snapshots[name] = public_service_snapshot(name, service)
    return snapshots


def runtime_service_lines(bot: Any) -> tuple[str, ...]:
    lines: list[str] = []
    for name, snapshot in bot_service_snapshots(bot).items():
        if not snapshot["attached"]:
            lines.append(f"{name.replace('_', ' ').title()}: Missing")
            continue
        backend = snapshot["configured_backend"] or snapshot["active_backend"] or "unknown"
        status = "Ready" if snapshot["storage_ready"] else "Unavailable"
        line = f"{name.replace('_', ' ').title()}: {status} ({backend})"
        if snapshot["storage_error"] and not snapshot["storage_ready"]:
            line += f"\nError: {snapshot['storage_error']}"
        lines.append(line)
    return tuple(lines)


def format_service_startup_failure(label: str, service: Any) -> str:
    snapshot = service_snapshot(service)
    backend = snapshot["configured_backend"] or snapshot["active_backend"] or "unknown"
    detail = snapshot["storage_error"] or "startup did not complete safely."
    return f"{label} startup failed for configured backend `{backend}`: {detail}"


async def bind_started_service(bot: Any, *, attr_name: str, service: Any, label: str) -> bool:
    setattr(bot, attr_name, service)
    started = await service.start()
    if not started and service_requires_fail_fast(service):
        raise RuntimeError(format_service_startup_failure(label, service))
    return started
