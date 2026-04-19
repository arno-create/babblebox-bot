from __future__ import annotations

from typing import Protocol

from babblebox.premium_models import PatreonIdentity


class PremiumProviderError(RuntimeError):
    pass


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

