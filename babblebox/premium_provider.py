from __future__ import annotations

from typing import Protocol

from babblebox.premium_models import PatreonIdentity


class PremiumProviderError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        safe_message: str | None = None,
        provider_code: str | None = None,
        status_code: int | None = None,
        retryable: bool = False,
        hard_failure: bool = False,
    ):
        super().__init__(message)
        self.safe_message = str(safe_message or message)
        self.provider_code = str(provider_code or "").strip() or None
        self.status_code = int(status_code) if status_code is not None else None
        self.retryable = bool(retryable)
        self.hard_failure = bool(hard_failure)


class OAuthExchangeError(PremiumProviderError):
    pass


class WebhookVerificationError(PremiumProviderError):
    pass


class PremiumProviderAdapter(Protocol):
    provider_name: str

    def configured(self) -> bool:
        raise NotImplementedError

    def automation_ready(self) -> bool:
        raise NotImplementedError

    def build_authorize_url(self, *, state_token: str) -> str:
        raise NotImplementedError

    async def exchange_code(self, *, code: str) -> dict:
        raise NotImplementedError

    async def refresh_access_token(self, *, refresh_token: str) -> dict:
        raise NotImplementedError

    async def fetch_identity(self, *, access_token: str) -> PatreonIdentity:
        raise NotImplementedError

    def verify_webhook(self, *, body: bytes, signature: str, secret: str) -> None:
        raise NotImplementedError
