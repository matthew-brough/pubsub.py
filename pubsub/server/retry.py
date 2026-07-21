"""Retry backoff policy and redelivery engine.

Backoff is full-jitter exponential: a uniform draw over ``[0, base * 2**exp]``
where ``exp = min(attempt, cap_exponent)``. Defaults: budget 10 attempts, base
1 second, exponent cap 10. The pure ``full_jitter_backoff`` function is kept
separate so its bounds and cap can be tested without any async machinery.
"""

import asyncio
import logging
import random
import uuid
from collections.abc import Callable

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.shared.clock import Clock
from pubsub.shared.types import DLQEntry, Delivery, MessagePackValue

_log = logging.getLogger("pubsub.server.retry")

DEFAULT_MAX_ATTEMPTS = 10
DEFAULT_BASE_SECONDS = 1.0
DEFAULT_CAP_EXPONENT = 10


def full_jitter_backoff(
    attempt: int,
    *,
    base: float = DEFAULT_BASE_SECONDS,
    cap_exponent: int = DEFAULT_CAP_EXPONENT,
    rng: Callable[[], float] = random.random,
) -> float:
    """Return a full-jitter backoff delay in seconds for a 0-based ``attempt``.

    The exponent is capped at ``cap_exponent`` so late attempts do not produce
    unbounded (or overflowing) delays.
    """
    exponent = min(max(attempt, 0), cap_exponent)
    return rng() * (base * (2**exponent))


class RetryPolicy:
    """Retry budget and backoff schedule."""

    def __init__(
        self,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base: float = DEFAULT_BASE_SECONDS,
        cap_exponent: int = DEFAULT_CAP_EXPONENT,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.max_attempts = max_attempts
        self.base = base
        self.cap_exponent = cap_exponent
        self._rng = rng

    def exhausted(self, attempt: int) -> bool:
        """True once ``attempt`` reaches the configured budget."""
        return attempt >= self.max_attempts

    def backoff(self, attempt: int) -> float:
        return full_jitter_backoff(
            attempt, base=self.base, cap_exponent=self.cap_exponent, rng=self._rng
        )


class RetryEngine:
    """Schedules redelivery of nacked/undeliverable messages with backoff.

    Retries are driven per-delivery by ``asyncio.sleep``; the broker calls
    ``schedule`` and the engine re-enqueues onto the subscriber queue or routes
    to the DLQ once the budget is exhausted. No backoff state is reset on
    exhaustion — the DLQ is the terminal state.
    """

    def __init__(
        self,
        durability: DurabilityBackend,
        policy: RetryPolicy,
        clock: Clock,
    ) -> None:
        self._durability = durability
        self._policy = policy
        self._clock = clock
        self._tasks: set[asyncio.Task[None]] = set()

    async def schedule(
        self,
        queue: "asyncio.Queue[Delivery[MessagePackValue]]",
        delivery: Delivery[MessagePackValue],
        record: Callable[[Delivery[MessagePackValue]], None],
        on_exhausted: Callable[[str], None] | None = None,
    ) -> None:
        """Route ``delivery`` for redelivery or dead-lettering.

        The terminal DLQ decision carries no backoff, so it is resolved inline
        (the caller can rely on it having happened on return). A non-exhausted
        retry owns a backoff *sleep*; only that part is spawned as a background
        task so a slow subscriber never blocks fresh-event fanout or the nacking
        consumer. ``on_exhausted`` fires with the subscription id once the budget
        is spent (slow-subscriber disconnect).
        """
        next_attempt = delivery.attempt + 1
        if self._policy.exhausted(next_attempt):
            await self._to_dlq(delivery, attempts=next_attempt)
            if on_exhausted is not None:
                on_exhausted(delivery.subscription_id)
            return

        task = asyncio.ensure_future(
            self._delayed(queue, delivery, record, on_exhausted, next_attempt)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _delayed(
        self,
        queue: "asyncio.Queue[Delivery[MessagePackValue]]",
        delivery: Delivery[MessagePackValue],
        record: Callable[[Delivery[MessagePackValue]], None],
        on_exhausted: Callable[[str], None] | None,
        next_attempt: int,
    ) -> None:
        """Off-path backoff wait, then re-enqueue.

        ``record`` re-registers the retried delivery as in-flight; it is invoked
        only after a successful enqueue (with no ``await`` in between) so an ack
        cannot race ahead of the in-flight record.
        """
        await asyncio.sleep(self._policy.backoff(next_attempt))
        retry = Delivery(
            delivery_id=uuid.uuid4().hex,
            subscription_id=delivery.subscription_id,
            message=delivery.message,
            attempt=next_attempt,
        )
        try:
            queue.put_nowait(retry)
        except asyncio.QueueFull:
            # Subscriber still saturated; fold back into the retry loop.
            await self.schedule(queue, retry, record, on_exhausted)
            return
        record(retry)

    async def aclose(self) -> None:
        """Cancel and drain any in-flight backoff tasks."""
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _to_dlq(self, delivery: Delivery[MessagePackValue], *, attempts: int) -> None:
        entry = DLQEntry(
            message=delivery.message,
            subscription_id=delivery.subscription_id,
            attempts=attempts,
        )
        await self._durability.to_dlq(entry)
        _log.error(
            "delivery exhausted; routed to DLQ",
            extra={
                "message_id": delivery.message.message_id,
                "topic": delivery.message.topic,
                "attempt_count": attempts,
            },
        )
