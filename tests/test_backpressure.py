"""Admission control: the broker paces publish intake to the unacked-delivery
backlog so a fast producer cannot outrun consumer drain.

Backpressure is a *reliability* property (it stops the memory eviction/DLQ
spiral), not a throughput gain: once the backlog hits ``max_inflight`` a new
publish blocks until an ack (or eviction) drains a slot.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


class BackpressureTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            max_inflight=2,
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

    async def test_max_inflight_none_never_blocks(self) -> None:
        broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            max_inflight=None,
        )
        self.addAsyncCleanup(broker.close)
        await broker.subscribe("t.*")
        for n in range(10):  # no acks, yet none of these block
            self.assertTrue((await broker.publish("t.x", n)).accepted)


if __name__ == "__main__":
    unittest.main()
