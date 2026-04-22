from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from copy import deepcopy
from typing import Any


LOGGER = logging.getLogger(__name__)


def _decode_postgres_json(value: Any, *, label: str, default: Any, expected_kind: str) -> Any:
    if value is None:
        return deepcopy(default)
    parsed = value
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            LOGGER.warning(
                "Postgres JSON decode fallback (%s): invalid JSON; using default.",
                label,
            )
            return deepcopy(default)
    if expected_kind == "object":
        if isinstance(parsed, Mapping):
            return deepcopy(dict(parsed))
        LOGGER.warning(
            "Postgres JSON decode fallback (%s): expected object; using default.",
            label,
        )
        return deepcopy(default)
    if isinstance(parsed, (list, tuple)):
        return deepcopy(list(parsed))
    LOGGER.warning(
        "Postgres JSON decode fallback (%s): expected array; using default.",
        label,
    )
    return deepcopy(default)


def decode_postgres_json_object(value: Any, *, label: str, default: dict[str, Any] | None = None) -> dict[str, Any]:
    return _decode_postgres_json(value, label=label, default=default or {}, expected_kind="object")


def decode_postgres_json_array(value: Any, *, label: str, default: list[Any] | None = None) -> list[Any]:
    return _decode_postgres_json(value, label=label, default=default or [], expected_kind="array")
