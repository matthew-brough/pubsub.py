"""Publisher convenience: bind a topic over any broker backend.

Not a competing contract. The canonical publish shape is
``publish(topic, payload)`` on ``Broker`` (embedded) and ``BrokerClient``
(networked). ``Publisher`` merely binds a topic so a caller that always targets
one subject supplies just the payload; it delegates to whichever backend it
wraps, so the same convenience works in-process or over the wire.
"""

from collections.abc import Mapping
from typing import Protocol

from pubsub.shared.types import MessagePackValue, PublishResult


class PublishBackend(Protocol):
    """Anything with the canonical publish shape (``Broker``, ``BrokerClient``)."""

    async def publish(
        self,
        topic: str,
        payload: MessagePackValue,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult: ...


class Publisher[PayloadT: MessagePackValue]:
    """Binds a topic to a backend; callers supply only the payload."""

    def __init__(self, backend: PublishBackend, topic: str) -> None:
        self._backend = backend
        self._topic = topic

    @property
    def topic(self) -> str:
        return self._topic

    async def publish(
        self,
        payload: PayloadT,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult:
        return await self._backend.publish(self._topic, payload, extras=extras)
