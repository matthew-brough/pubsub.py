"""Subject matching and subscriber fanout registry.

The router owns the mapping from active subscriptions to their bounded delivery
queues and answers "which subscriptions match this published subject". It holds
no delivery state or retry logic — that lives in the broker/retry engine.
"""

import asyncio
from collections.abc import Iterator

from pubsub.shared import topic as _topic
from pubsub.shared.types import Delivery, MessagePackValue, Subscription


class Registration:
    """A live subscription and its bounded delivery queue.

    ``selector_tokens`` is the pre-validated split of the selector so the hot
    fanout path never re-validates the pattern per publish. ``replaying`` /
    ``buffer`` stage live deliveries that arrive while ``FromUnixTimestamp``
    history is being read, so the handoff stays age-ordered (oldest first).
    """

    __slots__ = ("subscription", "queue", "selector_tokens", "replaying", "buffer")

    def __init__(
        self,
        subscription: Subscription,
        queue: "asyncio.Queue[Delivery[MessagePackValue]]",
        selector_tokens: list[str] | None = None,
    ) -> None:
        self.subscription = subscription
        self.queue = queue
        self.selector_tokens = selector_tokens or subscription.selector.split(".")
        self.replaying = False
        self.buffer: list[Delivery[MessagePackValue]] = []


class Router:
    def __init__(self) -> None:
        self._registrations: dict[str, Registration] = {}

    def register(self, registration: Registration) -> None:
        self._registrations[registration.subscription.subscription_id] = registration

    def unregister(self, subscription_id: str) -> Registration | None:
        return self._registrations.pop(subscription_id, None)

    def get(self, subscription_id: str) -> Registration | None:
        return self._registrations.get(subscription_id)

    def match(self, subject: str) -> Iterator[Registration]:
        """Yield registrations whose selector matches a concrete ``subject``."""
        for registration in self._registrations.values():
            if _topic.matches_tokens(registration.selector_tokens, subject):
                yield registration

    def active_count(self) -> int:
        return len(self._registrations)
