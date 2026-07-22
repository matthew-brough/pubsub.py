"""Minimal async SQLite wrapper (private to the server layer).

Reimplements the small ``asqlite`` surface the project needs instead of adding
the dependency (stdlib + msgpack only).

Concurrency model
-----------------
A single ``sqlite3.Connection`` may only be touched by one thread, so each
connection gets a dedicated ``ThreadPoolExecutor(max_workers=1)``. To lift the
single-worker throughput ceiling for reads, the wrapper holds:

- one **write** connection + worker — the sole writer (SQLite serialises writers
  anyway, so this is free), and
- ``readers`` **read** connections + workers, checked out from a pool, so
  replay/bulk scans run concurrently and off the hot write path.

File databases open in WAL mode so readers can run alongside the writer.

Read-your-writes ordering is enforced explicitly by a writer-preferring
async read/write lock (``_RWLock``): while a write is active or waiting, new
reads block until writes drain and commit. This deliberately trades some of
WAL's read-concurrent-with-write for deterministic "writes finish before reads
begin"; it can be relaxed later if a use case wants stale-snapshot reads.

Transactions are caller-owned (``isolation_level=None`` → no implicit
BEGIN/COMMIT). A ``_Transaction`` pins the write connection and holds the write
lock for its whole span; ``execute`` calls made by the owning task inside a
transaction skip re-locking (task-scoped reentrancy) so they do not self-block.

Consumed only by ``durability/sqlite.py``.
"""

import asyncio
import sqlite3
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import Self

type _Params = Sequence[object] | Mapping[str, object]

DEFAULT_READERS = 4
_BUSY_TIMEOUT_MS = 5000


class _RWLock:
    """Phase-fair async read/write lock (writer-preferring, reader-starvation-free).

    Many readers may hold it concurrently; a writer holds it exclusively. While a
    writer is active or waiting, new readers block — this enforces "writes finish
    before reads begin". To keep a *continuous* writer stream (e.g. group-commit
    publish batches) from starving reads (replay/ack) forever, a completed write
    grants the readers already waiting at its release one turn ahead of the next
    writer. Reads issued after a write still observe it, so read-your-writes holds.
    """

    def __init__(self) -> None:
        self._readers = 0
        self._writer_active = False
        self._waiting_writers = 0
        self._waiting_readers = 0
        # Set when a write releases with readers queued: those readers run before
        # the next writer. Cleared once the reader phase drains.
        self._reader_turn = False
        self._cond = asyncio.Condition()

    async def acquire_read(self) -> None:
        async with self._cond:
            self._waiting_readers += 1
            try:
                # Yield to writers unless it is the readers' granted turn.
                while self._writer_active or (
                    self._waiting_writers > 0 and not self._reader_turn
                ):
                    await self._cond.wait()
            finally:
                self._waiting_readers -= 1
            self._readers += 1

    async def release_read(self) -> None:
        async with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._reader_turn = False  # reader phase over; writers may run
                self._cond.notify_all()

    async def acquire_write(self) -> None:
        async with self._cond:
            self._waiting_writers += 1
            try:
                # Stand down during a granted reader turn so the phase completes.
                while (
                    self._writer_active
                    or self._readers > 0
                    or (self._reader_turn and self._waiting_readers > 0)
                ):
                    await self._cond.wait()
            finally:
                self._waiting_writers -= 1
            self._writer_active = True

    async def release_write(self) -> None:
        async with self._cond:
            self._writer_active = False
            # Hand the next turn to readers already waiting, so back-to-back
            # writes cannot lock them out indefinitely.
            self._reader_turn = self._waiting_readers > 0
            self._cond.notify_all()


class _Conn:
    """A single sqlite3 connection bound to its own single-worker executor."""

    __slots__ = ("_conn", "_executor")

    def __init__(self, connection: sqlite3.Connection, executor: ThreadPoolExecutor) -> None:
        self._conn = connection
        self._executor = executor

    @classmethod
    async def open(cls, factory: Callable[[], sqlite3.Connection]) -> "_Conn":
        executor = ThreadPoolExecutor(max_workers=1)
        loop = asyncio.get_running_loop()
        connection = await loop.run_in_executor(executor, factory)
        return cls(connection, executor)

    async def run[T](self, func: Callable[[sqlite3.Connection], T]) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, func, self._conn)

    async def close(self) -> None:
        await self.run(lambda conn: conn.close())
        await asyncio.to_thread(self._executor.shutdown, True)


class AsyncSQLite:
    def __init__(self, writer: _Conn, readers: list[_Conn]) -> None:
        self._writer = writer
        self._readers = readers
        self._read_pool: asyncio.Queue[_Conn] = asyncio.Queue()
        for reader in readers:
            self._read_pool.put_nowait(reader)
        self._rw = _RWLock()
        self._write_owner: asyncio.Task[object] | None = None
        self._closed = False

    @classmethod
    async def connect(cls, path: str, *, readers: int = DEFAULT_READERS) -> Self:
        """Open a write connection plus ``readers`` read connections.

        ``readers=0`` degrades to single-connection mode (reads run on the write
        connection, still gated by the read/write lock).
        """
        if readers < 0:
            raise ValueError("readers must be >= 0")

        if path == ":memory:":
            # Per-instance shared-cache name so the pool's connections share one
            # in-memory DB while staying isolated from other AsyncSQLite instances.
            name = f"asqlite_mem_{uuid.uuid4().hex}"
            target = f"file:{name}?mode=memory&cache=shared"
            uri = True
        else:
            target = path
            uri = False

        def _open(*, wal: bool) -> sqlite3.Connection:
            connection = sqlite3.connect(
                target, isolation_level=None, check_same_thread=False, uri=uri
            )
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}").close()
            if wal:
                # WAL is a database-level, persistent setting; the writer sets it
                # before any reader opens. No-op ('memory') for :memory: DBs.
                connection.execute("PRAGMA journal_mode=WAL").close()
            return connection

        writer = await _Conn.open(lambda: _open(wal=True))
        read_conns: list[_Conn] = []
        for _ in range(readers):
            read_conns.append(await _Conn.open(lambda: _open(wal=False)))
        return cls(writer, read_conns)

    # -- write side ---------------------------------------------------------

    def _owns_write(self) -> bool:
        return self._write_owner is not None and self._write_owner is asyncio.current_task()

    async def _acquire_write(self) -> None:
        await self._rw.acquire_write()
        self._write_owner = asyncio.current_task()

    async def _release_write(self) -> None:
        self._write_owner = None
        await self._rw.release_write()

    async def _write[T](self, func: Callable[[sqlite3.Connection], T]) -> T:
        if self._owns_write():  # inside a transaction owned by this task
            return await self._writer.run(func)
        await self._rw.acquire_write()
        try:
            return await self._writer.run(func)
        finally:
            await self._rw.release_write()

    async def execute(self, sql: str, params: _Params = ()) -> int | None:
        """Run one write statement. Returns ``lastrowid`` for INSERT, else ``None``.

        ``cursor.lastrowid`` retains the connection's last insert rowid across
        later non-insert statements, so the verb is checked explicitly rather
        than trusting ``lastrowid`` to reset.
        """
        verb = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""

        def _op(conn: sqlite3.Connection) -> int | None:
            cursor = conn.execute(sql, params)
            try:
                return cursor.lastrowid if verb in ("INSERT", "REPLACE") else None
            finally:
                cursor.close()

        return await self._write(_op)

    async def executemany(self, sql: str, params: Iterable[_Params]) -> None:
        """Batch write (DLQ entries, log records)."""

        def _op(conn: sqlite3.Connection) -> None:
            conn.executemany(sql, params).close()

        await self._write(_op)

    async def executescript(self, sql: str) -> None:
        """DDL / schema init only. sqlite3 issues an implicit COMMIT first."""

        def _op(conn: sqlite3.Connection) -> None:
            conn.executescript(sql).close()

        await self._write(_op)

    async def commit(self) -> None:
        await self._write(lambda conn: conn.commit())

    async def rollback(self) -> None:
        await self._write(lambda conn: conn.rollback())

    def transaction(self) -> "_Transaction":
        """Return an explicit transaction context manager (not a generator)."""
        return _Transaction(self)

    # -- read side ----------------------------------------------------------

    async def _read[T](self, func: Callable[[sqlite3.Connection], T]) -> T:
        await self._rw.acquire_read()
        try:
            if not self._readers:
                # Single-connection mode: read on the writer connection.
                return await self._writer.run(func)
            conn = await self._read_pool.get()
            try:
                return await conn.run(func)
            finally:
                self._read_pool.put_nowait(conn)
        finally:
            await self._rw.release_read()

    async def fetchall(self, sql: str, params: _Params = ()) -> list[sqlite3.Row]:
        def _op(conn: sqlite3.Connection) -> list[sqlite3.Row]:
            cursor = conn.execute(sql, params)
            try:
                return cursor.fetchall()
            finally:
                cursor.close()

        return await self._read(_op)

    async def fetchone(self, sql: str, params: _Params = ()) -> sqlite3.Row | None:
        def _op(conn: sqlite3.Connection) -> sqlite3.Row | None:
            cursor = conn.execute(sql, params)
            try:
                return cursor.fetchone()
            finally:
                cursor.close()

        return await self._read(_op)

    # -- lifecycle ----------------------------------------------------------

    async def close(self) -> None:
        """Drain and close every connection (writer + readers)."""
        if self._closed:
            return
        self._closed = True
        await self._writer.close()
        for reader in self._readers:
            await reader.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()


class _Transaction:
    """Explicit async transaction: pins the writer and holds the write lock.

    Hand-written rather than ``@asynccontextmanager`` to avoid generator
    overhead. ``__aexit__`` rolls back and reraises on any ``BaseException`` —
    including ``asyncio.CancelledError`` — so a cancelled task leaves no
    half-open transaction and always releases the write lock.
    """

    __slots__ = ("_db",)

    def __init__(self, db: AsyncSQLite) -> None:
        self._db = db

    async def __aenter__(self) -> AsyncSQLite:
        await self._db._acquire_write()
        try:
            await self._db._writer.run(lambda conn: conn.execute("BEGIN").close())
        except BaseException:
            await self._db._release_write()
            raise
        return self._db

    async def __aexit__(
        self, exc_type: type[BaseException] | None, *_: object
    ) -> bool:
        try:
            if exc_type is None:
                await self._db._writer.run(lambda conn: conn.commit())
            else:
                await self._db._writer.run(lambda conn: conn.rollback())
        finally:
            await self._db._release_write()
        return False  # never suppress; reraise including CancelledError
