"""Subscriber client boundary (typed, async, stream-first).

Callback wrappers are a library-consumer responsibility. The stream yields
``Delivery`` objects; ack/nack go back through the broker by delivery identity.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from pubsub.shared.types import Delivery


class Subscriber[PayloadT](ABC):
    """Async stream of deliveries plus acknowledgement controls."""

    @property
    @abstractmethod
    def subscription_id(self) -> str:
        """Broker-created id for this subscription; stable across reconnect."""
        ...

    def __aiter__(self) -> AsyncIterator[Delivery[PayloadT]]:
        return self

    @abstractmethod
    async def __anext__(self) -> Delivery[PayloadT]:
        """Return the next delivery; raise ``StopAsyncIteration`` when closed."""
        ...

    @abstractmethod
    async def ack(self, delivery: Delivery[PayloadT]) -> None:
        """Acknowledge successful handling; stops redelivery."""
        ...

    @abstractmethod
    async def nack(self, delivery: Delivery[PayloadT]) -> None:
        """Negative-acknowledge; schedules retry with backoff."""
        ...

    @abstractmethod
    async def unsubscribe(self) -> None:
        """Hard unsubscribe: pending acks are dropped immediately."""
        ...
