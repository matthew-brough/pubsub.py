"""Publisher client boundary (typed, async, in-process).

CLI and network clients are future adapters that call the same contract.
"""

from abc import ABC, abstractmethod
from collections.abc import Mapping

from pubsub.shared.types import MessagePackValue, PublishResult


class Publisher[PayloadT](ABC):
    """Typed interface for publishing payloads to a bound topic.

    The concrete implementation binds a topic; callers supply only the payload
    and optional user ``extras`` metadata.
    """

    @abstractmethod
    async def publish(
        self,
        payload: PayloadT,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult:
        """Wrap payload in an envelope, durably store, return broker result."""
        ...
