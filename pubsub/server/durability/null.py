"""Null durability backend — accept-and-drop, retains nothing.

``NullDurability`` satisfies the ``DurabilityBackend`` contract with pure no-ops:
``append`` drops the message, ``read_from`` / ``read_dlq`` are always empty, and
no ack or dead-letter state is kept. ``append`` still returns, so publish succeeds
and live fanout is unaffected — this backend simply keeps *nothing*.

Use it for at-most-once, non-durable workloads where replay is not needed and the
unbounded in-process history of ``InMemoryDurability`` is an unwanted liability:
no retention means no memory growth on a long-lived broker. Replay subscriptions
(``FromUnixTimestamp``) get an empty history and ``last_acked`` is always ``None``,
so a reconnecting subscriber resumes from live.
"""

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.types import DLQEntry, Message, MessagePackValue


class NullDurability(DurabilityBackend):
    """No-op durability: accepts publishes, retains nothing."""

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        return None

    async def append(self, message: Message[MessagePackValue]) -> None:
        return None

    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        return []

    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        return None

    async def last_acked(self, subscription_id: str) -> str | None:
        return None

    async def to_dlq(self, entry: DLQEntry) -> None:
        return None

    async def read_dlq(self) -> list[DLQEntry]:
        return []

    async def close(self) -> None:
        return None
