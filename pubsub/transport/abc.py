"""Typed transport interfaces.

Per the dependency posture, transport is a swappable layer: the core broker
never imports sockets, and any concrete transport (the TCP impl here, or a
future one) implements these contracts. The server side adapts a ``Broker`` to
a stream; the client side re-exposes the broker surface over that stream.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping

from pubsub.client.subscriber import Subscriber
from pubsub.shared.types import MessagePackValue, PublishResult, ReplayPolicy


class ServerTransport(ABC):
    """Serves a broker over a stream. ``start`` binds and begins accepting."""

    @abstractmethod
    async def start(self) -> None:
        """Bind and begin accepting connections; returns once bound."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Stop accepting and tear down live connections."""
        ...


class ClientTransport(ABC):
    """Remote broker surface: mirrors ``Broker.register_topic``/``publish``/``subscribe``."""

    @abstractmethod
    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        """Declare a topic's retention mode (replay-capable or live-only)."""
        ...

    @abstractmethod
    async def publish(
        self,
        topic: str,
        payload: MessagePackValue,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult: ...

    @abstractmethod
    async def subscribe(
        self, selector: str, replay_policy: ReplayPolicy | None = None
    ) -> Subscriber[MessagePackValue]: ...

    @abstractmethod
    async def close(self) -> None: ...
