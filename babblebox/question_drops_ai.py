from __future__ import annotations

import asyncio
import json
import os
from typing import Any

import aiohttp

from babblebox.text_safety import normalize_plain_text


DEFAULT_QUESTION_DROPS_AI_MODEL = "gpt-5-mini"
DEFAULT_QUESTION_DROPS_AI_TIMEOUT_SECONDS = 4.0
DEFAULT_QUESTION_DROPS_AI_CONCURRENCY = 1
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"

_SYSTEM_PROMPT = (
    "You write rare Babblebox Question Drops celebration lines. "
    "Keep them compact, stylish, warm, and non-cringe. "
    "Do not sound like a fantasy RPG narrator. "
    "Do not mention AI. "
    "Stay under 180 characters. "
    "Return strict JSON with one key: message."
)


def _read_float_env(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return min(maximum, max(minimum, value))


def sanitize_question_drop_ai_payload(payload: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, str):
            cleaned[key] = normalize_plain_text(value)[:160]
        elif isinstance(value, (int, float, bool)) or value is None:
            cleaned[key] = value
        elif isinstance(value, list):
            cleaned[key] = [normalize_plain_text(str(item))[:80] for item in value[:6]]
    return cleaned


class QuestionDropAIProvider:
    def diagnostics(self) -> dict[str, Any]:
        return {
            "provider": "disabled",
            "available": False,
            "configured": False,
            "model": None,
            "status": "AI celebrations are unavailable because no provider is configured.",
        }

    async def highlight(self, payload: dict[str, Any]) -> str | None:
        return None

    async def close(self):
        return None


class OpenAIQuestionDropAIProvider(QuestionDropAIProvider):
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY", "").strip()
        self.model = os.getenv("QUESTION_DROPS_AI_MODEL", "").strip() or DEFAULT_QUESTION_DROPS_AI_MODEL
        self.timeout_seconds = _read_float_env(
            "QUESTION_DROPS_AI_TIMEOUT_SECONDS",
            DEFAULT_QUESTION_DROPS_AI_TIMEOUT_SECONDS,
            minimum=1.0,
            maximum=15.0,
        )
        self._session: aiohttp.ClientSession | None = None
        self._semaphore = asyncio.Semaphore(DEFAULT_QUESTION_DROPS_AI_CONCURRENCY)

    def diagnostics(self) -> dict[str, Any]:
        available = bool(self.api_key)
        return {
            "provider": "openai",
            "available": available,
            "configured": available,
            "model": self.model,
            "status": "Ready." if available else "OpenAI API key is not configured.",
        }

    async def close(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def highlight(self, payload: dict[str, Any]) -> str | None:
        if not self.api_key:
            return None
        cleaned_payload = sanitize_question_drop_ai_payload(payload)
        acquired = False
        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=0.05)
            acquired = True
        except asyncio.TimeoutError:
            return None
        try:
            session = await self._get_session()
            async with session.post(
                OPENAI_CHAT_COMPLETIONS_URL,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "temperature": 0.8,
                    "max_tokens": 120,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {"role": "system", "content": _SYSTEM_PROMPT},
                        {"role": "user", "content": json.dumps(cleaned_payload, ensure_ascii=True)},
                    ],
                },
            ) as response:
                if response.status >= 400:
                    return None
                body = await response.json(content_type=None)
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError):
            return None
        finally:
            if acquired:
                self._semaphore.release()
        try:
            content = body["choices"][0]["message"]["content"]
            if isinstance(content, list):
                content = "\n".join(
                    str(part.get("text", "")).strip()
                    for part in content
                    if isinstance(part, dict) and str(part.get("type", "")).strip() in {"text", "output_text"}
                )
            parsed = json.loads(str(content))
            message = normalize_plain_text(str(parsed.get("message", "")))
            return message[:180] if message else None
        except (KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError):
            return None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
            connector = aiohttp.TCPConnector(limit=DEFAULT_QUESTION_DROPS_AI_CONCURRENCY + 1, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session


def build_question_drop_ai_provider() -> QuestionDropAIProvider:
    return OpenAIQuestionDropAIProvider()
