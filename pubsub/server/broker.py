"""Concrete broker: routing, durable publish, subscribe/replay, ack/nack, retry.

This is the first internal boundary before any network protocol exists. It is
built against the durability ABC; a concrete backend is injected at construction
(no concrete backend ships yet — those are deliberately left as stubs).

Deferred / not fully specified by the notes, marked here rather than invented:
- Bounded-queue sizing: the notes say the default bound "scales with number of
  active subscribers". The exact formula is deferred; a fixed bound is used.
- Replay→live ordering is registered-then-replayed and is best-effort: a live
  publish racing an in-progress replay may interleave. Global best-effort order
  matches the stated semantics.
"""

import asyncio
import logging
import uuid
from collections.abc import Mapping

from pubsub.client.subscriber import Subscriber
from pubsub.server.durability.abc import DurabilityBackend
from pubsub.server.retry import RetryEngine, RetryPolicy
from pubsub.server.router import Registration, Router
from pubsub.shared import topic as _topic
from pubsub.shared.clock import Clock, SystemClock
from pubsub.shared.types import (
    Delivery,
    FromUnixTimestamp,
    FutureOnly,
    Message,
    MessagePackValue,
    PublishError,
    PublishResult,
    ReplayPolicy,
    Subscription,
)

# DEFER: notes call for a bound that scales with active subscriber count; the
# scaling formula is unspecified. Fixed bound used until that decision is made.
DEFAULT_QUEUE_BOUND = 128


class Broker:
    def __init__(
        self,
        durability: DurabilityBackend,
        *,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> None:
        self._durability = durability
        self._clock: Clock = clock or SystemClock()
        self._router = Router()
        self._policy = retry_policy or RetryPolicy()
        self._retry = RetryEngine(durability, self._policy, self._clock)
        self._inflight: dict[str, Delivery[MessagePackValue]] = {}
        self._streams: dict[str, "_SubscriptionStream"] = {}
        self._log = logging.getLogger("pubsub.server.broker")

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        await self._durability.register_topic(topic, replayable=replayable)

    async def publish(
        self,
        topic: str,
        payload: MessagePackValue,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult:
        try:
            _topic.validate_subject(topic)
        except _topic.TopicError as exc:
            return PublishResult.rejected(PublishError("invalid_topic", str(exc)))

        message: Message[MessagePackValue] = Message(
            message_id=uuid.uuid4().hex,
            topic=topic,
            payload=payload,
            extras=dict(extras or {}),
            created_at=self._clock.now(),
        )
        # Publish succeeds only after durable storage returns.
        await self._durability.append(message)

        # Fanout is non-blocking: enqueue per subscriber, hand slow ones to the
        # retry engine (which defers the backoff sleep to a background task). A
        # saturated subscriber never stalls the others or the publisher.
        for registration in list(self._router.match(topic)):
            await self._deliver(registration, message, attempt=1)

        return PublishResult.ok(message.message_id)

    async def subscribe(
        self, selector: str, replay_policy: ReplayPolicy | None = None
    ) -> Subscriber[MessagePackValue]:
        tokens = _topic.validate_pattern(selector)  # raises TopicError on bad pattern
        policy: ReplayPolicy = replay_policy or FutureOnly()
        subscription = Subscription(
            subscription_id=uuid.uuid4().hex,
            selector=selector,
            replay_policy=policy,
        )
        queue: asyncio.Queue[Delivery[MessagePackValue]] = asyncio.Queue(
            maxsize=DEFAULT_QUEUE_BOUND
        )
        registration = Registration(subscription, queue, tokens)
        self._router.register(registration)
        stream = _SubscriptionStream(self, subscription, queue)
        self._streams[subscription.subscription_id] = stream

        if isinstance(policy, FromUnixTimestamp):
            try:
                await self._replay(registration, policy.timestamp)
            except BaseException:
                # Don't leak a registration/stream with no live consumer.
                self._router.unregister(subscription.subscription_id)
                self._streams.pop(subscription.subscription_id, None)
                raise

        return stream

    async def _replay(self, registration: Registration, timestamp: float) -> None:
        """Deliver durable history (oldest first), then flush live events that
        arrived during the read so the replay→live handoff stays age-ordered."""
        registration.replaying = True
        try:
            history = await self._durability.read_from(timestamp)
        except BaseException:
            registration.replaying = False
            raise
        # From here on there is no ``await``: history delivery, the switch out
        # of replay mode, and the buffer flush are one atomic step, so no live
        # event can slip into the queue ahead of older replayed history.
        tokens = registration.selector_tokens
        for message in history:
            if _topic.matches_tokens(tokens, message.topic):
                await self._enqueue(
                    registration, self._make_delivery(registration, message)
                )
        registration.replaying = False
        for delivery in sorted(
            registration.buffer, key=lambda d: d.message.created_at
        ):
            await self._enqueue(registration, delivery)
        registration.buffer.clear()

    def _make_delivery(
        self, registration: Registration, message: Message[MessagePackValue]
    ) -> Delivery[MessagePackValue]:
        return Delivery(
            delivery_id=uuid.uuid4().hex,
            subscription_id=registration.subscription.subscription_id,
            message=message,
            attempt=1,
        )

    async def _deliver(
        self,
        registration: Registration,
        message: Message[MessagePackValue],
        *,
        attempt: int,
    ) -> None:
        delivery = Delivery(
            delivery_id=uuid.uuid4().hex,
            subscription_id=registration.subscription.subscription_id,
            message=message,
            attempt=attempt,
        )
        if registration.replaying:
            # Stage live events until history has drained (age-order handoff).
            registration.buffer.append(delivery)
            return
        await self._enqueue(registration, delivery)

    async def _enqueue(
        self,
        registration: Registration,
        delivery: Delivery[MessagePackValue],
    ) -> None:
        try:
            registration.queue.put_nowait(delivery)
        except asyncio.QueueFull:
            # Slow subscriber: hand off to the retry engine. schedule() only
            # spawns the backoff sleep as a task — this await does not block on
            # the backoff itself.
            await self._retry.schedule(
                registration.queue, delivery, self._record_inflight, self._on_exhausted
            )
            return
        self._record_inflight(delivery)

    def _on_exhausted(self, subscription_id: str) -> None:
        # Retry budget spent: disconnect the slow subscriber; its id survives.
        self._disconnect(subscription_id)

    def _record_inflight(self, delivery: Delivery[MessagePackValue]) -> None:
        self._inflight[delivery.delivery_id] = delivery

    def _drop_inflight(self, subscription_id: str) -> None:
        stale = [
            delivery_id
            for delivery_id, delivery in self._inflight.items()
            if delivery.subscription_id == subscription_id
        ]
        for delivery_id in stale:
            self._inflight.pop(delivery_id, None)

    def _disconnect(self, subscription_id: str) -> None:
        """Tear down a subscription (slow-subscriber eviction). Sync so it is
        safe to call from a retry background task with no interleaving await."""
        self._router.unregister(subscription_id)
        self._drop_inflight(subscription_id)
        stream = self._streams.pop(subscription_id, None)
        if stream is not None:
            stream._mark_closed()

    async def _ack(self, delivery: Delivery[MessagePackValue]) -> None:
        self._inflight.pop(delivery.delivery_id, None)
        await self._durability.record_ack(
            delivery.subscription_id, delivery.message.message_id
        )

    async def _nack(self, delivery: Delivery[MessagePackValue]) -> None:
        self._inflight.pop(delivery.delivery_id, None)
        registration = self._router.get(delivery.subscription_id)
        if registration is None:
            return  # subscription gone; nothing to redeliver to
        # Backoff runs off-path (schedule spawns it); nack returns promptly.
        await self._retry.schedule(
            registration.queue, delivery, self._record_inflight, self._on_exhausted
        )

    async def _unsubscribe(self, subscription_id: str) -> None:
        self._router.unregister(subscription_id)
        self._streams.pop(subscription_id, None)
        # Hard unsubscribe: pending acks dropped immediately.
        self._drop_inflight(subscription_id)

    async def close(self) -> None:
        await self._retry.aclose()
        await self._durability.close()


class _SubscriptionStream(Subscriber[MessagePackValue]):
    """Concrete async delivery stream returned by ``Broker.subscribe``."""

    def __init__(
        self,
        broker: Broker,
        subscription: Subscription,
        queue: "asyncio.Queue[Delivery[MessagePackValue]]",
    ) -> None:
        self._broker = broker
        self._subscription = subscription
        self._queue = queue
        self._closed = asyncio.Event()

    @property
    def subscription_id(self) -> str:
        return self._subscription.subscription_id

    async def __anext__(self) -> Delivery[MessagePackValue]:
        if self._closed.is_set() and self._queue.empty():
            raise StopAsyncIteration

        get_task = asyncio.ensure_future(self._queue.get())
        closed_task = asyncio.ensure_future(self._closed.wait())
        try:
            done, pending = await asyncio.wait(
                {get_task, closed_task}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            # Consumer/pump cancelled mid-wait: cancel the helper tasks so a
            # pending ``queue.get()`` does not leak as a destroyed-pending task.
            get_task.cancel()
            closed_task.cancel()
            raise
        # Prefer a ready delivery over the close signal so nothing is dropped.
        if get_task in done:
            closed_task.cancel()
            return get_task.result()

        for task in pending:
            task.cancel()
        raise StopAsyncIteration

    async def ack(self, delivery: Delivery[MessagePackValue]) -> None:
        await self._broker._ack(delivery)

    async def nack(self, delivery: Delivery[MessagePackValue]) -> None:
        await self._broker._nack(delivery)

    async def unsubscribe(self) -> None:
        await self._broker._unsubscribe(self._subscription.subscription_id)
        self._closed.set()

    def _mark_closed(self) -> None:
        """Broker-driven close (slow-subscriber disconnect). Drains queued
        deliveries, then ends the stream."""
        self._closed.set()
