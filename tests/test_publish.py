"""Broker publish path: validation and durable-before-return.

Uses the in-memory backend so the broker is exercised end-to-end.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import DEFAULT_QUEUE_BOUND, Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


class PublishTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.durability = InMemoryDurability()
        self.clock = FakeClock(start=100.0)
        self.broker = Broker(self.durability, clock=self.clock)
        self.addAsyncCleanup(self.broker.close)

    async def test_rejects_wildcard_subject(self) -> None:
        # A published subject with wildcards is a client error, not a topic.
        result = await self.broker.publish("a.*", 1)
        self.assertFalse(result.accepted)
        self.assertIsNone(result.message_id)
        assert result.error is not None
        self.assertEqual(result.error.code, "invalid_topic")

    async def test_rejects_empty_token(self) -> None:
        result = await self.broker.publish("a..b", 1)
        self.assertFalse(result.accepted)
        assert result.error is not None
        self.assertEqual(result.error.code, "invalid_topic")

    async def test_accepted_publish_is_durable_before_return(self) -> None:
        # Publish success implies the message is already in replayable history.
        await self.broker.register_topic("a.b", replayable=True)
        result = await self.broker.publish("a.b", {"v": 1})
        self.assertTrue(result.accepted)
        self.assertIsNotNone(result.message_id)

        stored = await self.durability.read_from(0.0)
        self.assertEqual([m.message_id for m in stored], [result.message_id])
        self.assertEqual(stored[0].created_at, 100.0)  # broker stamped via clock

    async def test_fanout_delivers_to_all_live_subscribers(self) -> None:
        subs = [await self.broker.subscribe("a.*") for _ in range(5)]
        self.assertTrue((await self.broker.publish("a.b", "hi")).accepted)
        for sub in subs:
            delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
            self.assertEqual(delivery.message.payload, "hi")

    async def test_saturated_subscriber_spills_without_blocking_fast_ones(self) -> None:
        # The synchronous fast path must not stall the whole fanout on one full
        # queue: fast subs get the message inline; the full one spills to retry.
        broker = Broker(
            self.durability,
            clock=self.clock,
            retry_policy=RetryPolicy(base=1e6, rng=lambda: 1.0, max_attempts=100),
        )
        self.addAsyncCleanup(broker.close)
        fast = await broker.subscribe("a.*")
        slow = await broker.subscribe("a.*")  # never drained → its queue fills
        for _ in range(DEFAULT_QUEUE_BOUND):
            self.assertTrue((await broker.publish("a.b", 1)).accepted)

        # No retry tasks yet: every delivery enqueued on the fast path.
        self.assertEqual(len(broker._retry._tasks), 0)

        # One more overflows slow's queue → it spills to retry, fast still gets it.
        self.assertTrue((await broker.publish("a.b", "last")).accepted)
        self.assertEqual(broker._retry_pending.get(slow.subscription_id), 1)
        # Drain fast up to the newest and confirm the tail message arrived.
        last = None
        for _ in range(DEFAULT_QUEUE_BOUND + 1):
            last = await asyncio.wait_for(fast.__anext__(), timeout=1.0)
        assert last is not None
        self.assertEqual(last.message.payload, "last")


if __name__ == "__main__":
    unittest.main()
