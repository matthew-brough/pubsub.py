"""In-memory durability backend.

Non-persistent reference implementation of ``DurabilityBackend``. History is
kept only for topics registered ``replayable=True``; live-only (and unregistered)
topics are accepted by ``append`` — so publish still succeeds — but never
surface from ``read_from``.
"""

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.types import DLQEntry, Message, MessagePackValue


class InMemoryDurability(DurabilityBackend):
    """In-process durability. State lives for the object's lifetime only."""

    def __init__(self) -> None:
        self._replayable: dict[str, bool] = {}
        self._history: list[Message[MessagePackValue]] = []
        self._acks: dict[str, str] = {}
        self._dlq: list[DLQEntry] = []

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        self._replayable[topic] = replayable

    async def append(self, message: Message[MessagePackValue]) -> None:
        # Retain for replay only when the topic opted in; others are accepted
        # (publish succeeds) but dropped from history.
        if self._replayable.get(message.topic, False):
            self._history.append(message)

    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        # Inclusive lower bound, oldest first. Stable sort keeps insertion order
        # for equal timestamps.
        return sorted(
            (m for m in self._history if m.created_at >= timestamp),
            key=lambda m: m.created_at,
        )

    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        self._acks[subscription_id] = message_id

    async def last_acked(self, subscription_id: str) -> str | None:
        return self._acks.get(subscription_id)

    async def to_dlq(self, entry: DLQEntry) -> None:
        self._dlq.append(entry)

    async def read_dlq(self) -> list[DLQEntry]:
        return list(self._dlq)

    async def close(self) -> None:
        return None
