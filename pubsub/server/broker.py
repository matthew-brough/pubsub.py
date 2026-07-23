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
from collections.abc import Mapping

from pubsub.client.subscriber import Subscriber
from pubsub.observability import Observer
from pubsub.server.durability.abc import DurabilityBackend
from pubsub.server.retry import RetryEngine, RetryPolicy
from pubsub.server.router import Registration, Router
from pubsub.shared import topic as _topic
from pubsub.shared.clock import Clock, SystemClock
from pubsub.shared.ids import new_id
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

# Backpressure high-watermark, expressed *per active subscriber*. In-memory /
# null durability accept publishes with no natural pacing (unlike sqlite, whose
# per-append fsync throttles a producer for free), so an unbraked fast producer
# floods subscriber queues into the retry/DLQ eviction spiral. The effective gate
# is this value times the live subscriber count (see ``Broker._watermark``).
#
# Scaling with subscribers is what a fixed total got wrong: a fixed 1024 sat 8x
# above a single consumer's queue bound, so under a producer-heavy flood a lone
# consumer's deliveries spilled to retry and exhausted its budget — evicting it —
# *before* the global watermark ever engaged, after which fanout emptied and
# accept ran away (PERF_MATRIX §3 collapse). Tied to the per-subscriber queue
# bound, the gate instead trips as a consumer's queue fills, pacing producers to
# drain rather than evicting.
DEFAULT_MAX_INFLIGHT_PER_SUBSCRIBER = DEFAULT_QUEUE_BOUND


class Broker:
    def __init__(
        self,
        durability: DurabilityBackend,
        *,
        clock: Clock | None = None,
        retry_policy: RetryPolicy | None = None,
        observer: Observer | None = None,
        max_inflight_per_subscriber: int | None = DEFAULT_MAX_INFLIGHT_PER_SUBSCRIBER,
    ) -> None:
        self._durability = durability
        self._clock: Clock = clock or SystemClock()
        self._router = Router()
        self._policy = retry_policy or RetryPolicy()
        self._retry = RetryEngine(durability, self._policy, self._clock)
        self._obs = observer or Observer()
        self._inflight: dict[str, Delivery[MessagePackValue]] = {}
        # Deliveries handed to the retry engine (in backoff, or bouncing on a
        # full queue) are not yet in ``_inflight`` but are still unacked backlog.
        # Counted per-subscriber so eviction can clear a dead sub's share, with a
        # running total for an O(1) gate check.
        self._retry_pending: dict[str, int] = {}
        self._retry_pending_total = 0
        self._streams: dict[str, "_SubscriptionStream"] = {}
        self._log = logging.getLogger("pubsub.server.broker")
        # Admission control: gate opens while the unacked-delivery backlog is
        # below the effective watermark (``per_subscriber × live subscribers``;
        # ``None`` disables). Acks, evictions, and subscriber joins reopen it.
        self._max_inflight_per_sub = max_inflight_per_subscriber
        self._admit = asyncio.Event()
        self._admit.set()

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
            self._obs.on_publish(topic, accepted=False, reason="invalid_topic")
            return PublishResult.rejected(PublishError("invalid_topic", str(exc)))

        message: Message[MessagePackValue] = Message(
            message_id=new_id(),
            topic=topic,
            payload=payload,
            extras=dict(extras or {}),
            created_at=self._clock.now(),
        )
        # Backpressure: pace intake to the unacked backlog before taking on more
        # durable work, so a fast producer cannot flood queues into eviction.
        await self._gate()
        # Publish succeeds only after durable storage returns.
        await self._durability.append(message)

        # Fanout is non-blocking: the common path (observe + put_nowait + record)
        # is fully synchronous, so a wide fanout mints no per-delivery coroutine.
        # Saturated subscribers spill to a tail that hands them to the retry
        # engine off the match loop, so eviction can't mutate it mid-iteration.
        spills: list[tuple[Registration, Delivery[MessagePackValue]]] = []
        for registration in list(self._router.match(topic)):
            delivery = Delivery(
                delivery_id=new_id(),
                subscription_id=registration.subscription.subscription_id,
                message=message,
                attempt=1,
            )
            if registration.replaying:
                registration.buffer.append(delivery)
            elif not self._try_enqueue(registration, delivery):
                spills.append((registration, delivery))
        for registration, delivery in spills:
            self._enter_retry(registration.subscription.subscription_id)
            await self._retry.schedule(
                registration.queue, delivery, self._record_retried, self._on_exhausted
            )

        self._obs.on_publish(topic, accepted=True)
        return PublishResult.ok(message.message_id)

    async def subscribe(
        self, selector: str, replay_policy: ReplayPolicy | None = None
    ) -> Subscriber[MessagePackValue]:
        tokens = _topic.validate_pattern(selector)  # raises TopicError on bad pattern
        policy: ReplayPolicy = replay_policy or FutureOnly()
        subscription = Subscription(
            subscription_id=new_id(),
            selector=selector,
            replay_policy=policy,
        )
        queue: asyncio.Queue[Delivery[MessagePackValue]] = asyncio.Queue(
            maxsize=DEFAULT_QUEUE_BOUND
        )
        registration = Registration(subscription, queue, tokens)
        self._router.register(registration)
        # A new subscriber raises the effective watermark; release any producer
        # already blocked on admission.
        self._refresh_admission()
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
            delivery_id=new_id(),
            subscription_id=registration.subscription.subscription_id,
            message=message,
            attempt=1,
        )

    def _try_enqueue(
        self,
        registration: Registration,
        delivery: Delivery[MessagePackValue],
    ) -> bool:
        """Synchronous live-fanout enqueue. Returns False when the subscriber
        queue is full so the caller spills the delivery to the retry engine."""
        self._obs.on_deliver(
            delivery.message.topic,
            delivery.subscription_id,
            attempt=delivery.attempt,
        )
        try:
            registration.queue.put_nowait(delivery)
        except asyncio.QueueFull:
            return False
        self._record_inflight(delivery)
        return True

    async def _enqueue(
        self,
        registration: Registration,
        delivery: Delivery[MessagePackValue],
    ) -> None:
        self._obs.on_deliver(
            delivery.message.topic,
            delivery.subscription_id,
            attempt=delivery.attempt,
        )
        try:
            registration.queue.put_nowait(delivery)
        except asyncio.QueueFull:
            # Slow subscriber: hand off to the retry engine. schedule() only
            # spawns the backoff sleep as a task — this await does not block on
            # the backoff itself. The delivery is now retry-limbo backlog and
            # counts against admission until it is re-enqueued or the sub evicted.
            self._enter_retry(registration.subscription.subscription_id)
            await self._retry.schedule(
                registration.queue, delivery, self._record_retried, self._on_exhausted
            )
            return
        self._record_inflight(delivery)

    def _on_exhausted(self, subscription_id: str) -> None:
        # Retry budget spent: disconnect the slow subscriber; its id survives.
        self._log.warning(
            "retry budget exhausted; evicting slow subscriber %s", subscription_id
        )
        self._obs.on_retry_exhausted(subscription_id)
        self._disconnect(subscription_id)

    async def _gate(self) -> None:
        """Admission control: block a new publish while the unacked-delivery
        backlog is at/above the effective watermark. Acks and evictions lower the
        backlog and reopen the gate. Waiting yields the loop, so ack/retry tasks
        keep draining — the gate always eventually reopens (dead subscribers are
        evicted, releasing their backlog). Soft bound: transient overshoot up to
        the in-flight publish concurrency is tolerated."""
        if self._max_inflight_per_sub is None:
            return
        while self._backlog() >= self._watermark():
            self._admit.clear()
            await self._admit.wait()

    def _backlog(self) -> int:
        """Total unacked backlog: enqueued-and-inflight plus retry-limbo. Gating
        on the sum (not just ``_inflight``) is what keeps a flood whose deliveries
        all spill to retry from reading as empty and defeating the gate."""
        return len(self._inflight) + self._retry_pending_total

    def _watermark(self) -> int:
        """Effective backlog bound: the per-subscriber budget times the live
        subscriber count, floored at one subscriber's worth so a broker with no
        subscribers — where no delivery backlog can form — never self-blocks.
        Recomputed on each check so it tracks subscribers joining and leaving."""
        assert self._max_inflight_per_sub is not None
        return self._max_inflight_per_sub * max(1, self._router.active_count())

    def _refresh_admission(self) -> None:
        """Reopen the admission gate once the backlog drops below the watermark.
        Sync (no await) so it is safe from retry background tasks and eviction.
        Also called when a subscriber joins — the higher watermark can release a
        producer already parked on the gate."""
        if self._max_inflight_per_sub is None or self._backlog() < self._watermark():
            self._admit.set()

    def _enter_retry(self, subscription_id: str) -> None:
        """Account a delivery leaving its queue for retry-limbo (backoff or a
        full-queue bounce). Paired with ``_leave_retry`` (re-enqueued) or the
        bulk clear in ``_drop_inflight`` (evicted/unsubscribed)."""
        self._retry_pending[subscription_id] = (
            self._retry_pending.get(subscription_id, 0) + 1
        )
        self._retry_pending_total += 1

    def _leave_retry(self, subscription_id: str) -> None:
        remaining = self._retry_pending.get(subscription_id)
        if not remaining:
            return  # subscriber already evicted; its count was cleared in bulk
        if remaining == 1:
            del self._retry_pending[subscription_id]
        else:
            self._retry_pending[subscription_id] = remaining - 1
        self._retry_pending_total -= 1

    def _record_retried(self, delivery: Delivery[MessagePackValue]) -> None:
        """Retry engine re-enqueued a delivery: move it from retry-limbo to
        inflight. Net backlog is unchanged, so the gate state does not shift."""
        self._leave_retry(delivery.subscription_id)
        self._record_inflight(delivery)

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
        # Drop this sub's retry-limbo share too, else evicting a subscriber that
        # was spilling to retry would leak backlog and wedge the gate shut.
        self._retry_pending_total -= self._retry_pending.pop(subscription_id, 0)
        self._refresh_admission()

    def _disconnect(self, subscription_id: str) -> None:
        """Tear down a subscription (slow-subscriber eviction). Sync so it is
        safe to call from a retry background task with no interleaving await."""
        self._router.unregister(subscription_id)
        self._retry.cancel_for(subscription_id)
        self._drop_inflight(subscription_id)
        stream = self._streams.pop(subscription_id, None)
        if stream is not None:
            stream._mark_closed()

    async def _ack(self, delivery: Delivery[MessagePackValue]) -> None:
        self._inflight.pop(delivery.delivery_id, None)
        self._refresh_admission()
        self._obs.on_ack(delivery.subscription_id)
        await self._durability.record_ack(
            delivery.subscription_id, delivery.message.message_id
        )

    async def _nack(self, delivery: Delivery[MessagePackValue]) -> None:
        self._inflight.pop(delivery.delivery_id, None)
        self._obs.on_nack(delivery.subscription_id, attempt=delivery.attempt)
        registration = self._router.get(delivery.subscription_id)
        if registration is None:
            # Subscription gone: the slot is genuinely freed, reopen the gate.
            self._refresh_admission()
            return
        # Move inflight -> retry-limbo (net backlog unchanged, so the gate does
        # not shift). Backoff runs off-path (schedule spawns it); nack returns
        # promptly.
        self._enter_retry(delivery.subscription_id)
        await self._retry.schedule(
            registration.queue, delivery, self._record_retried, self._on_exhausted
        )

    async def _unsubscribe(self, subscription_id: str) -> None:
        self._router.unregister(subscription_id)
        self._streams.pop(subscription_id, None)
        # Hard unsubscribe: pending acks and backoff redeliveries dropped now.
        self._retry.cancel_for(subscription_id)
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
