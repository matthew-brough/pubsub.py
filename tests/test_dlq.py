"""Broker dead-letter path: an exhausted delivery lands in the DLQ.

With ``max_attempts=2`` the first nack (which targets attempt 2) exhausts the
budget, so redelivery routes straight to the DLQ with no backoff reset.
"""

import asyncio
import logging
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


class DLQTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.durability = InMemoryDurability()
        policy = RetryPolicy(max_attempts=2, base=0.0, rng=lambda: 0.0)
        self.broker = Broker(self.durability, clock=FakeClock(), retry_policy=policy)
        self.addAsyncCleanup(self.broker.close)
        # DLQ exhaustion logs at ERROR; silence it so test output stays clean.
        logging.getLogger("pubsub.server.retry").setLevel(logging.CRITICAL)

    async def test_exhausted_delivery_is_dead_lettered(self) -> None:
        sub = await self.broker.subscribe("t.*")
        result = await self.broker.publish("t.x", {"n": 1})
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

        await sub.nack(delivery)  # attempt 2 == budget -> DLQ

        entries = await self.durability.read_dlq()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry.message.message_id, result.message_id)
        self.assertEqual(entry.subscription_id, delivery.subscription_id)
        self.assertEqual(entry.attempts, 2)
        self.assertEqual(entry.message.payload, {"n": 1})

        # Terminal: exhausting the budget disconnects the slow subscriber, so
        # its stream ends (StopAsyncIteration) rather than dangling open.
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(sub.__anext__(), timeout=1.0)

    async def test_inflight_cleared_after_dead_letter(self) -> None:
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.nack(delivery)
        self.assertEqual(self.broker._inflight, {})


if __name__ == "__main__":
    unittest.main()
