from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any


def _decode_postgres_json(value: Any, *, label: str, default: Any, expected_kind: str) -> Any:
    if value is None:
        return deepcopy(default)
    parsed = value
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            print(f"Postgres JSON decode fallback ({label}): invalid JSON; using default.")
            return deepcopy(default)
    if expected_kind == "object":
        if isinstance(parsed, Mapping):
            return deepcopy(dict(parsed))
        print(f"Postgres JSON decode fallback ({label}): expected object; using default.")
        return deepcopy(default)
    if isinstance(parsed, (list, tuple)):
        return deepcopy(list(parsed))
    print(f"Postgres JSON decode fallback ({label}): expected array; using default.")
    return deepcopy(default)


def decode_postgres_json_object(value: Any, *, label: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    return _decode_postgres_json(value, label=label, default=default or {}, expected_kind="object")


def decode_postgres_json_array(value: Any, *, label: str, default: list[Any] | None = None) -> list[Any]:
    return _decode_postgres_json(value, label=label, default=default or [], expected_kind="array")
