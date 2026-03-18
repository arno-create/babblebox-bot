from __future__ import annotations

import asyncio
import json
import shutil
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def default_utility_state() -> dict[str, Any]:
    return {
        "version": 1,
        "watch": {},
        "later": {},
        "reminders": {},
        "brb": {},
    }


class UtilityStateStore:
    def __init__(self, path: Path | None = None):
        self.path = path or (Path(__file__).resolve().parent.parent / ".cache" / "utility_state.json")
        self.state: dict[str, Any] = default_utility_state()
        self._io_lock = asyncio.Lock()

    async def load(self) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        try:
            raw = await asyncio.to_thread(self.path.read_text, encoding="utf-8")
        except FileNotFoundError:
            self.state = default_utility_state()
            return self.state
        except Exception as exc:
            print(f"Utility store load failed: {exc}")
            self.state = default_utility_state()
            return self.state

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            print(f"Utility store is corrupt, using defaults: {exc}")
            await self._backup_corrupt_store()
            self.state = default_utility_state()
            return self.state

        self.state = self._normalize_state(payload)
        return self.state

    async def flush(self) -> bool:
        snapshot = deepcopy(self.state)
        async with self._io_lock:
            try:
                await asyncio.to_thread(self._write_snapshot, snapshot)
            except Exception as exc:
                print(f"Utility store flush failed: {exc}")
                return False
        return True

    def _normalize_state(self, payload: Any) -> dict[str, Any]:
        normalized = default_utility_state()
        if not isinstance(payload, dict):
            return normalized

        version = payload.get("version")
        normalized["version"] = version if isinstance(version, int) and version > 0 else 1

        for section in ("watch", "later", "reminders", "brb"):
            value = payload.get(section)
            if isinstance(value, dict):
                normalized[section] = value

        return normalized

    async def _backup_corrupt_store(self):
        if not self.path.exists():
            return

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        backup_path = self.path.with_suffix(f".corrupt-{timestamp}.json")

        try:
            await asyncio.to_thread(shutil.copy2, self.path, backup_path)
        except Exception as exc:
            print(f"Failed to back up corrupt utility store: {exc}")

    def _write_snapshot(self, snapshot: dict[str, Any]):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        payload = json.dumps(snapshot, indent=2, ensure_ascii=True, sort_keys=True)
        temp_path.write_text(payload, encoding="utf-8")
        temp_path.replace(self.path)
