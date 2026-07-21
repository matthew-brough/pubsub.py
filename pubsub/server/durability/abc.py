"""Durability backend contract.

Concrete backends (in-memory, SQLite) implement this ABC and are selected at
runtime. The DLQ is part of this interface, not a separate concept.

The method surface below is inferred from the delivery-semantics notes rather
than fully specified there; it is the minimal set the broker needs. Custom
backends subclass this ABC.
"""

from abc import ABC, abstractmethod

from pubsub.shared.types import DLQEntry, Message, MessagePackValue


class DurabilityBackend(ABC):
    @abstractmethod
    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        """Record a topic's retention mode. Live-only topics keep no history."""
        ...

    @abstractmethod
    async def append(self, message: Message[MessagePackValue]) -> None:
        """Durably store a message. Publish succeeds only after this returns.

        Messages for live-only topics may be stored transiently or dropped from
        history at the backend's discretion; replay reads must not return them.
        """
        ...

    @abstractmethod
    async def read_from(self, timestamp: float) -> list[Message[MessagePackValue]]:
        """Return retained messages with ``created_at >= timestamp``, oldest first.

        Inclusive. A timestamp before retention start returns available data,
        not an error. Topic/pattern filtering is the router's responsibility.
        """
        ...

    @abstractmethod
    async def record_ack(self, subscription_id: str, message_id: str) -> None:
        """Persist the last-acked message for a subscription (reconnect replay)."""
        ...

    @abstractmethod
    async def last_acked(self, subscription_id: str) -> str | None:
        """Return the last-acked message id for a subscription, or ``None``."""
        ...

    @abstractmethod
    async def to_dlq(self, entry: DLQEntry) -> None:
        """Route an exhausted delivery to the dead-letter store."""
        ...

    @abstractmethod
    async def read_dlq(self) -> list[DLQEntry]:
        """Return dead-lettered entries."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release backend resources (connections, threads)."""
        ...
