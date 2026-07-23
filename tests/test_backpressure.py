"""Admission control: the broker paces publish intake to the unacked-delivery
backlog so a fast producer cannot outrun consumer drain.

Backpressure is a *reliability* property (it stops the memory eviction/DLQ
spiral), not a throughput gain: once the backlog hits ``max_inflight`` a new
publish blocks until an ack (or eviction) drains a slot.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import DEFAULT_QUEUE_BOUND, Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


class BackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            max_inflight_per_subscriber=2,
        )
        self.addAsyncCleanup(self.broker.close)

    async def test_publish_blocks_at_watermark_until_ack_drains(self) -> None:
        sub = await self.broker.subscribe("t.*")

        # Two publishes fill the unacked backlog to the watermark.
        self.assertTrue((await self.broker.publish("t.x", 1)).accepted)
        self.assertTrue((await self.broker.publish("t.x", 2)).accepted)

        # The third must block on admission — nothing has been acked yet.
        third = asyncio.ensure_future(self.broker.publish("t.x", 3))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(third), timeout=0.05)

        # Drain one delivery + ack -> backlog drops below the watermark -> gate
        # reopens -> the blocked publish completes.
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.ack(delivery)
        result = await asyncio.wait_for(third, timeout=1.0)
        self.assertTrue(result.accepted)

    async def test_unsubscribe_reopens_the_gate(self) -> None:
        # Dropping the subscription releases its unacked backlog, so a producer
        # stalled on admission resumes. (At the wire layer heartbeat teardown
        # does the same via _drop_inflight.)
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        await self.broker.publish("t.x", 2)  # backlog now at the watermark (2)

        third = asyncio.ensure_future(self.broker.publish("t.x", 3))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(third), timeout=0.05)

        await sub.unsubscribe()  # drops inflight -> gate reopens
        self.assertTrue((await asyncio.wait_for(third, timeout=1.0)).accepted)

    async def test_retry_limbo_backlog_keeps_the_gate_closed(self) -> None:
        # Regression (PERF_RERUN §7 accept-flood): a delivery that spills to the
        # retry engine (queue full) is not in _inflight yet, but is still unacked
        # backlog. If the gate ignored it, a flood whose deliveries all bounce
        # into retry would read as empty backlog and the gate would never engage
        # — exactly the observed 8500/s accept with ack_ratio 0.22. It must count.
        broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            # Huge backoff so retried deliveries park in limbo (they never
            # exhaust and evict the sub during the test).
            retry_policy=RetryPolicy(base=1e6, rng=lambda: 1.0, max_attempts=100),
            max_inflight_per_subscriber=DEFAULT_QUEUE_BOUND + 2,
        )
        self.addAsyncCleanup(broker.close)
        sub = await broker.subscribe("t.*")  # never consumed → its queue fills

        # Fill the queue to its bound: these all enqueue as inflight.
        for n in range(DEFAULT_QUEUE_BOUND):
            self.assertTrue((await broker.publish("t.x", n)).accepted)
        # The next two overflow the queue → retry-limbo. Backlog is now
        # bound (inflight) + 2 (limbo) = the watermark.
        self.assertTrue((await broker.publish("t.x", "a")).accepted)
        self.assertTrue((await broker.publish("t.x", "b")).accepted)

        # At the watermark. The next publish must block on admission even though
        # only `bound` deliveries are truly enqueued — the old leaky accounting
        # (len(_inflight) only) would wave it straight through.
        blocked = asyncio.ensure_future(broker.publish("t.x", "c"))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(blocked), timeout=0.05)

        # Drain + ack one enqueued delivery → inflight drops below the watermark
        # → gate reopens → the blocked publish proceeds.
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.ack(delivery)
        self.assertTrue((await asyncio.wait_for(blocked, timeout=1.0)).accepted)

    async def test_max_inflight_none_never_blocks(self) -> None:
        broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            max_inflight_per_subscriber=None,
        )
        self.addAsyncCleanup(broker.close)
        await broker.subscribe("t.*")
        for n in range(10):  # no acks, yet none of these block
            self.assertTrue((await broker.publish("t.x", n)).accepted)

    async def test_watermark_scales_with_subscriber_count(self) -> None:
        # The gate is per-subscriber: the effective bound is the per-subscriber
        # budget times the live subscriber count. A producer blocked at one
        # subscriber's watermark is released when a second subscriber joins and
        # raises it (PERF_MATRIX §3 fix — the gate paces to drain instead of the
        # flood evicting a lone consumer before a fixed global bound engages).
        broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            max_inflight_per_subscriber=1,
        )
        self.addAsyncCleanup(broker.close)

        await broker.subscribe("a.*")  # 1 subscriber → watermark 1
        # Fills the backlog to the watermark (delivered, unacked).
        self.assertTrue((await broker.publish("a.x", 1)).accepted)

        # Second publish is over the one-subscriber watermark → blocks.
        blocked = asyncio.ensure_future(broker.publish("a.y", 2))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(blocked), timeout=0.05)

        # A second (independent) subscriber lifts the watermark to 2 → the gate
        # reopens and the parked publish completes, no ack required.
        await broker.subscribe("b.*")
        self.assertTrue((await asyncio.wait_for(blocked, timeout=1.0)).accepted)

    async def test_eviction_cancels_pending_backoff_no_backlog_leak(self) -> None:
        # A delivery parked in retry-limbo must be cancelled when its sub is
        # dropped. If the backoff task instead wakes after the drop it re-adds an
        # inflight row for a dead subscription — unremovable backlog that leaks
        # and can wedge the admission gate. It must also not spill to the DLQ.
        durability = InMemoryDurability()
        broker = Broker(
            durability,
            clock=FakeClock(),
            # ~20ms backoff: long enough to unsubscribe before the task wakes,
            # short enough that the sleep below outlasts it (a leaked wake fires).
            retry_policy=RetryPolicy(base=0.02, cap_exponent=0, rng=lambda: 1.0, max_attempts=100),
            max_inflight_per_subscriber=DEFAULT_QUEUE_BOUND,
        )
        self.addAsyncCleanup(broker.close)

        sub = await broker.subscribe("t.*")
        self.assertTrue((await broker.publish("t.x", 1)).accepted)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.nack(delivery)  # inflight -> retry-limbo, task now in backoff
        self.assertEqual(broker._backlog(), 1)
        self.assertIn(sub.subscription_id, broker._retry._by_sub)

        await sub.unsubscribe()  # cancel_for + drop -> terminal
        await asyncio.sleep(0.05)  # past when the backoff would have re-fired

        self.assertEqual(broker._backlog(), 0)
        self.assertEqual(len(broker._inflight), 0)
        self.assertNotIn(sub.subscription_id, broker._retry._by_sub)
        self.assertEqual(await durability.read_dlq(), [])


if __name__ == "__main__":
    unittest.main()
