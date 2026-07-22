"""Correctness guards for the AsyncSQLite write/read pool.

Targets the invariants that make the pool safe rather than smoke coverage:
lastrowid contract, transaction atomicity, cancellation safety (rollback +
write-lock release), cross-connection visibility, the writer-preferring
"writes finish before reads begin" ordering, and non-interleaving of
concurrent transactions on the single writer.
"""

import asyncio
import os
import shutil
import tempfile
import unittest
import uuid

from pubsub.server._asqlite import AsyncSQLite


class AsyncSQLiteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self._dir, ignore_errors=True)

    async def _db(self, *, readers: int = 4) -> AsyncSQLite:
        path = os.path.join(self._dir, f"{uuid.uuid4().hex}.db")
        db = await AsyncSQLite.connect(path, readers=readers)
        self.addAsyncCleanup(db.close)
        await db.executescript(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);"
        )
        return db

    async def _values(self, db: AsyncSQLite) -> list[str]:
        return [row["v"] for row in await db.fetchall("SELECT v FROM t ORDER BY id")]

    async def test_execute_returns_lastrowid_for_insert_else_none(self) -> None:
        # Callers depend on the broker-created rowid; a non-INSERT must not
        # report a bogus id.
        db = await self._db()
        first = await db.execute("INSERT INTO t (v) VALUES (?)", ("a",))
        self.assertEqual(first, 1)
        select = await db.execute("SELECT 1")
        self.assertIsNone(select)

    async def test_transaction_rolls_back_on_error(self) -> None:
        # Atomicity: an exception mid-transaction must leave no partial write.
        db = await self._db()
        await db.execute("INSERT INTO t (v) VALUES (?)", ("keep",))
        with self.assertRaises(ValueError):
            async with db.transaction():
                await db.execute("INSERT INTO t (v) VALUES (?)", ("drop",))
                raise ValueError("boom")
        self.assertEqual(await self._values(db), ["keep"])

    async def test_cancellation_rolls_back_and_frees_writer(self) -> None:
        # Cancelling a task inside a transaction must roll back AND release the
        # write lock, or every later write would deadlock.
        db = await self._db()

        async def cancelme() -> None:
            async with db.transaction():
                await db.execute("INSERT INTO t (v) VALUES (?)", ("gone",))
                await asyncio.sleep(10)

        task = asyncio.ensure_future(cancelme())
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        # Lock must be free: this write completes and the rolled-back row is absent.
        await asyncio.wait_for(
            db.execute("INSERT INTO t (v) VALUES (?)", ("after",)), timeout=1.0
        )
        self.assertEqual(await self._values(db), ["after"])

    async def test_reader_connection_sees_committed_write(self) -> None:
        # Reads run on separate connections; a committed write must be visible
        # through the read pool, else replay would miss just-stored messages.
        db = await self._db(readers=2)
        await db.execute("INSERT INTO t (v) VALUES (?)", ("visible",))
        rows = await db.fetchall("SELECT v FROM t")
        self.assertEqual([r["v"] for r in rows], ["visible"])

    async def test_writer_preference_orders_writes_before_reads(self) -> None:
        # The core invariant: a read issued while a write transaction holds the
        # lock waits for the commit, then observes the write.
        db = await self._db(readers=2)
        order: list[object] = []
        write_holds_lock = asyncio.Event()

        async def slow_write() -> None:
            async with db.transaction():
                order.append("w-begin")
                await db.execute("INSERT INTO t (v) VALUES (?)", ("z",))
                write_holds_lock.set()
                await asyncio.sleep(0.05)
                order.append("w-commit")

        async def gated_read() -> None:
            await write_holds_lock.wait()  # ensure the write already holds the lock
            count = (await db.fetchall("SELECT COUNT(*) AS n FROM t WHERE v='z'"))[0]["n"]
            order.append(("r", count))

        await asyncio.gather(slow_write(), gated_read())
        self.assertEqual(order, ["w-begin", "w-commit", ("r", 1)])

    async def test_concurrent_transactions_do_not_interleave(self) -> None:
        # Two transactions must serialise on the single writer — interleaved
        # BEGINs on one connection would raise, not silently corrupt.
        db = await self._db()

        async def txn(value: str) -> None:
            async with db.transaction():
                await db.execute("INSERT INTO t (v) VALUES (?)", (value,))
                await asyncio.sleep(0.01)

        await asyncio.gather(txn("a"), txn("b"))
        self.assertEqual(sorted(await self._values(db)), ["a", "b"])

    async def test_readers_zero_uses_write_connection(self) -> None:
        # Degrade path: with no read connections, reads run on the writer and
        # must still return correct data.
        db = await self._db(readers=0)
        await db.execute("INSERT INTO t (v) VALUES (?)", ("solo",))
        self.assertEqual(await self._values(db), ["solo"])

    async def test_continuous_writes_do_not_starve_reads(self) -> None:
        # Phase fairness: a relentless writer stream must not lock reads out.
        # Under the old unconditional writer preference this read never returns.
        db = await self._db(readers=2)
        stop = asyncio.Event()

        async def write_forever() -> None:
            while not stop.is_set():
                await db.execute("INSERT INTO t (v) VALUES (?)", ("x",))

        writer = asyncio.ensure_future(write_forever())
        try:
            await asyncio.sleep(0.02)  # let the write stream saturate the lock
            rows = await asyncio.wait_for(
                db.fetchall("SELECT COUNT(*) AS n FROM t"), timeout=1.0
            )
            self.assertGreaterEqual(rows[0]["n"], 0)
        finally:
            stop.set()
            await writer


if __name__ == "__main__":
    unittest.main()
