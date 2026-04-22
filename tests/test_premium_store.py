import types
import unittest
from datetime import datetime, timezone

from babblebox.premium_store import _PostgresPremiumStore


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


class _SchemaConnection:
    def __init__(self):
        self.executed: list[str] = []

    async def execute(self, statement: str, *args):
        self.executed.append(statement)
        return "OK"


class _ConflictConnection:
    def __init__(self, *, conflict_prefix: str, conflict_error: type[Exception]):
        self.conflict_prefix = conflict_prefix
        self.conflict_error = conflict_error
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self):
        return _FakeTransaction()

    async def fetchrow(self, statement: str, *args):
        self.calls.append((statement, args))
        if statement.startswith(self.conflict_prefix):
            raise self.conflict_error("duplicate active premium source")
        return None


class PostgresPremiumStoreSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_ensure_schema_adds_unique_active_claim_source_index(self):
        store = _PostgresPremiumStore("postgresql://premium-user:secret@db.example.com/app")
        connection = _SchemaConnection()
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        self.assertIn(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_premium_guild_claims_source_active ON premium_guild_claims (source_kind, source_id) WHERE status = 'active'",
            connection.executed,
        )


class PostgresPremiumStoreConflictTests(unittest.IsolatedAsyncioTestCase):
    async def test_claim_guild_returns_none_on_active_source_conflict(self):
        class _FakeUniqueViolationError(Exception):
            pass

        store = _PostgresPremiumStore("postgresql://premium-user:secret@db.example.com/app")
        connection = _ConflictConnection(
            conflict_prefix="INSERT INTO premium_guild_claims",
            conflict_error=_FakeUniqueViolationError,
        )
        store._pool = _FakePool(connection)
        store._asyncpg = types.SimpleNamespace(exceptions=types.SimpleNamespace(UniqueViolationError=_FakeUniqueViolationError))

        claimed = await store.claim_guild(
            {
                "claim_id": "claim-1",
                "guild_id": 901,
                "plan_code": "guild_pro",
                "owner_user_id": 42,
                "source_kind": "entitlement",
                "source_id": "patreon:member-42:guild_pro",
                "status": "active",
                "claimed_at": _utcnow().isoformat(),
                "updated_at": _utcnow().isoformat(),
                "entitlement_id": "patreon:member-42:guild_pro",
                "note": None,
            }
        )

        self.assertIsNone(claimed)

    async def test_reassign_guild_claim_source_returns_none_on_active_source_conflict(self):
        class _FakeUniqueViolationError(Exception):
            pass

        store = _PostgresPremiumStore("postgresql://premium-user:secret@db.example.com/app")
        connection = _ConflictConnection(
            conflict_prefix="UPDATE premium_guild_claims SET source_kind",
            conflict_error=_FakeUniqueViolationError,
        )
        store._pool = _FakePool(connection)
        store._asyncpg = types.SimpleNamespace(exceptions=types.SimpleNamespace(UniqueViolationError=_FakeUniqueViolationError))

        rebound = await store.reassign_guild_claim_source(
            902,
            owner_user_id=42,
            source_kind="grant",
            source_id="override-1",
            entitlement_id=None,
            updated_at=_utcnow(),
            note="Auto-rebound after premium source check: entitlement_inactive",
        )

        self.assertIsNone(rebound)
