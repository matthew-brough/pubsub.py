"""Durable logging handler contract.

``DurableHandler`` extends the stdlib ``logging.Handler`` with persistence and
query. DLQ entries are logged at ERROR level through a durable handler with
structured ``extra`` fields. Concrete backends (in-memory, SQLite) are left as
stubs and co-located with message storage.

``DurableHandler`` stores ``LogRecord`` objects; that is a separate concern from
the durability backend which stores ``Message`` objects.
"""

import logging
import sqlite3
from abc import ABC, abstractmethod


class DurableHandler(logging.Handler, ABC):
    @abstractmethod
    def flush_to_storage(self) -> None:
        """Force-write buffered records to the persistent store."""
        ...

    @abstractmethod
    def retrieve(
        self, level: int | None = None, since: float | None = None
    ) -> list[logging.LogRecord]:
        """Query persisted records by level and/or epoch timestamp."""
        ...


def _matches(record: logging.LogRecord, level: int | None, since: float | None) -> bool:
    return (level is None or record.levelno >= level) and (
        since is None or record.created >= since
    )


class InMemoryDurableHandler(DurableHandler):
    """Non-persistent handler. ``emit`` buffers; ``flush_to_storage`` commits.

    ``retrieve`` reads the committed store only — buffered-but-unflushed records
    are invisible, mirroring the persistence boundary the SQLite handler has.
    """

    def __init__(self, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self._buffer: list[logging.LogRecord] = []
        self._store: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        with self.lock:  # type: ignore[union-attr]
            self._buffer.append(record)

    def flush_to_storage(self) -> None:
        with self.lock:  # type: ignore[union-attr]
            self._store.extend(self._buffer)
            self._buffer.clear()

    def retrieve(
        self, level: int | None = None, since: float | None = None
    ) -> list[logging.LogRecord]:
        with self.lock:  # type: ignore[union-attr]
            return [r for r in self._store if _matches(r, level, since)]


class SQLiteDurableHandler(DurableHandler):
    """Persists log records to SQLite. Buffers until ``flush_to_storage``.

    ``logging.Handler.emit`` is synchronous, so this uses a plain blocking
    ``sqlite3`` connection (not ``AsyncSQLite``) guarded by the handler lock.
    """

    _DDL = (
        "CREATE TABLE IF NOT EXISTS logs ("
        "seq INTEGER PRIMARY KEY AUTOINCREMENT, created REAL NOT NULL, "
        "levelno INTEGER NOT NULL, name TEXT NOT NULL, message TEXT NOT NULL)"
    )

    def __init__(self, path: str, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(self._DDL)
        self._conn.commit()
        self._buffer: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        with self.lock:  # type: ignore[union-attr]
            # Render the message now, while args are still attached.
            record.message = record.getMessage()
            self._buffer.append(record)

    def flush_to_storage(self) -> None:
        # The single sqlite3 connection is not safe for concurrent use, so every
        # touch of it is serialized under the handler lock (the same lock emit
        # holds). self.lock is a re-entrant RLock, so close() may nest.
        with self.lock:  # type: ignore[union-attr]
            rows = [
                (r.created, r.levelno, r.name, r.message)
                for r in self._buffer
            ]
            self._buffer.clear()
            if not rows:
                return
            self._conn.executemany(
                "INSERT INTO logs (created, levelno, name, message) VALUES (?, ?, ?, ?)",
                rows,
            )
            self._conn.commit()

    def retrieve(
        self, level: int | None = None, since: float | None = None
    ) -> list[logging.LogRecord]:
        query = "SELECT created, levelno, name, message FROM logs WHERE 1=1"
        params: list[object] = []
        if level is not None:
            query += " AND levelno>=?"
            params.append(level)
        if since is not None:
            query += " AND created>=?"
            params.append(since)
        query += " ORDER BY created ASC, seq ASC"
        with self.lock:  # type: ignore[union-attr]
            rows = self._conn.execute(query, params).fetchall()
        records: list[logging.LogRecord] = []
        for created, levelno, name, message in rows:
            record = logging.LogRecord(
                name, levelno, "(sqlite)", 0, message, None, None
            )
            record.created = created
            records.append(record)
        return records

    def close(self) -> None:
        try:
            with self.lock:  # type: ignore[union-attr]
                self.flush_to_storage()
                self._conn.close()
        finally:
            super().close()
