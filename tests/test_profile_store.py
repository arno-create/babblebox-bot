import unittest

from babblebox.profile_store import _PostgresProfileStore


class _FakeTransaction:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        self.connection.transaction_entries += 1
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


class _FakeConnection:
    def __init__(self, *, legacy_missing_question_drop_points: bool):
        self.executed: list[str] = []
        self.transaction_entries = 0
        self.profile_table_exists = legacy_missing_question_drop_points
        self.question_drop_points_exists = False

    def transaction(self):
        return _FakeTransaction(self)

    async def execute(self, statement: str):
        self.executed.append(statement)
        if "CREATE TABLE IF NOT EXISTS bb_user_profiles" in statement:
            if not self.profile_table_exists:
                self.profile_table_exists = True
                self.question_drop_points_exists = True
            return
        if "ADD COLUMN IF NOT EXISTS question_drop_points" in statement:
            self.question_drop_points_exists = True
            return
        if "ix_bb_profiles_question_drop_points" in statement and not self.question_drop_points_exists:
            raise AssertionError("question_drop_points index executed before the column existed")


class _FakePool:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


class PostgresProfileStoreSchemaTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_profile_table_adds_question_drop_points_before_index(self):
        connection = _FakeConnection(legacy_missing_question_drop_points=True)
        store = _PostgresProfileStore("postgresql://example")
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        alter_index = next(
            index for index, statement in enumerate(connection.executed) if "ADD COLUMN IF NOT EXISTS question_drop_points" in statement
        )
        create_index = next(
            index for index, statement in enumerate(connection.executed) if "ix_bb_profiles_question_drop_points" in statement
        )
        self.assertLess(alter_index, create_index)
        self.assertTrue(connection.question_drop_points_exists)
        self.assertEqual(connection.transaction_entries, 1)

    async def test_schema_bootstrap_is_idempotent_after_legacy_upgrade(self):
        connection = _FakeConnection(legacy_missing_question_drop_points=True)
        store = _PostgresProfileStore("postgresql://example")
        store._pool = _FakePool(connection)

        await store._ensure_schema()
        await store._ensure_schema()

        self.assertTrue(connection.question_drop_points_exists)
        self.assertEqual(connection.transaction_entries, 2)
        self.assertEqual(
            sum(1 for statement in connection.executed if "ix_bb_profiles_question_drop_points" in statement),
            2,
        )

    async def test_fresh_schema_creation_still_builds_question_drop_points_index(self):
        connection = _FakeConnection(legacy_missing_question_drop_points=False)
        store = _PostgresProfileStore("postgresql://example")
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        self.assertTrue(connection.profile_table_exists)
        self.assertTrue(connection.question_drop_points_exists)
        self.assertTrue(any("ix_bb_profiles_question_drop_points" in statement for statement in connection.executed))
        self.assertEqual(connection.transaction_entries, 1)

    async def test_schema_bootstrap_creates_question_drop_role_opt_out_table(self):
        connection = _FakeConnection(legacy_missing_question_drop_points=False)
        store = _PostgresProfileStore("postgresql://example")
        store._pool = _FakePool(connection)

        await store._ensure_schema()

        self.assertTrue(any("CREATE TABLE IF NOT EXISTS bb_question_drop_role_opt_outs" in statement for statement in connection.executed))
