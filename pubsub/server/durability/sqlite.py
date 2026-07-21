"""SQLite durability backend.

Persists messages, acks, and DLQ entries through ``AsyncSQLite`` so all blocking
``sqlite3`` work stays off the event-loop thread. Payloads and extras are
MessagePack-encoded blobs (stdlib + msgpack only). The ``replayable`` flag is
denormalised onto each stored message so ``read_from`` filters without a join
and stays correct across reconnects.
"""

import sqlite3
from typing import Self, cast

import msgpack

from pubsub.server._asqlite import DEFAULT_READERS, AsyncSQLite
from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.types import DLQEntry, Message, MessagePackValue

_SCHEMA = """
CREATE TABLE IF NOT EXISTS topics (
    topic TEXT PRIMARY KEY,
    replayable INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    payload BLOB NOT NULL,
    extras BLOB NOT NULL,
    created_at REAL NOT NULL,
    replayable INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_replay
    ON messages (replayable, created_at, seq);
CREATE TABLE IF NOT EXISTS acks (
    subscription_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dlq (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    message BLOB NOT NULL
);
"""


def _pack(value: object) -> bytes:
    return cast(bytes, msgpack.packb(value, use_bin_type=True))


def _unpack(blob: bytes) -> MessagePackValue:
    return cast(MessagePackValue, msgpack.unpackb(blob, raw=False))


def _row_to_message(row: sqlite3.Row) -> Message[MessagePackValue]:
    return Message(
        message_id=row["message_id"],
        topic=row["topic"],
        payload=_unpack(row["payload"]),
        extras=cast("dict[str, MessagePackValue]", _unpack(row["extras"])),
        created_at=row["created_at"],
    )


class SQLiteDurability(DurabilityBackend):
    """Durable backend over ``AsyncSQLite``. Construct via ``connect``."""

    def __init__(self, db: AsyncSQLite) -> None:
        self._db = db

    @classmethod
    async def connect(cls, path: str, *, readers: int = DEFAULT_READERS) -> Self:
        db = await AsyncSQLite.connect(path, readers=readers)
        await db.executescript(_SCHEMA)
        return cls(db)

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        await self._db.execute(
            "INSERT INTO topics (topic, replayable) VALUES (?, ?) "
            "ON CONFLICT(topic) DO UPDATE SET replayable=excluded.replayable",
            (topic, int(replayable)),
        )

    async def append(self, message: Message[MessagePackValue]) -> None:
        # Resolve the replayable flag inside the write itself: a single
        # statement is atomic, so there is no read-then-write window in which a
        # concurrent register_topic could denormalise a stale flag. Unregistered
        # topics default to not-replayable.
        await self._db.execute(
            "INSERT INTO messages "
            "(message_id, topic, payload, extras, created_at, replayable) "
            "VALUES (?, ?, ?, ?, ?, "
            "COALESCE((SELECT replayable FROM topics WHERE topic=?), 0))",
            (
                message.message_id,
                message.topic,
                _pack(message.payload),
                _pack(dict(message.extras)),
                message.created_at,
                message.topic,
            ),
        )

    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        rows = await self._db.fetchall(
            "SELECT message_id, topic, payload, extras, created_at FROM messages "
            "WHERE replayable=1 AND created_at>=? ORDER BY created_at ASC, seq ASC",
            (timestamp,),
        )
        return [_row_to_message(row) for row in rows]

    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        await self._db.execute(
            "INSERT INTO acks (subscription_id, message_id) VALUES (?, ?) "
            "ON CONFLICT(subscription_id) DO UPDATE SET message_id=excluded.message_id",
            (subscription_id, message_id),
        )

    async def last_acked(self, subscription_id: str) -> str | None:
        row = await self._db.fetchone(
            "SELECT message_id FROM acks WHERE subscription_id=?", (subscription_id,)
        )
        return row["message_id"] if row is not None else None

    async def to_dlq(self, entry: DLQEntry) -> None:
        await self._db.execute(
            "INSERT INTO dlq (subscription_id, attempts, message) VALUES (?, ?, ?)",
            (
                entry.subscription_id,
                entry.attempts,
                _pack(
                    {
                        "message_id": entry.message.message_id,
                        "topic": entry.message.topic,
                        "payload": entry.message.payload,
                        "extras": dict(entry.message.extras),
                        "created_at": entry.message.created_at,
                    }
                ),
            ),
        )

    async def read_dlq(self) -> list[DLQEntry]:
        rows = await self._db.fetchall(
            "SELECT subscription_id, attempts, message FROM dlq ORDER BY seq ASC"
        )
        entries: list[DLQEntry] = []
        for row in rows:
            data = cast("dict[str, MessagePackValue]", _unpack(row["message"]))
            message: Message[MessagePackValue] = Message(
                message_id=cast(str, data["message_id"]),
                topic=cast(str, data["topic"]),
                payload=data["payload"],
                extras=cast("dict[str, MessagePackValue]", data["extras"]),
                created_at=cast(float, data["created_at"]),
            )
            entries.append(
                DLQEntry(
                    message=message,
                    subscription_id=row["subscription_id"],
                    attempts=row["attempts"],
                )
            )
        return entries

    async def close(self) -> None:
        await self._db.close()
