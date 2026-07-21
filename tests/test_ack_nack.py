"""Broker acknowledgement path: ack clears in-flight + persists; nack redelivers.

Backoff is forced to zero (``rng`` -> 0) so redelivery is deterministic and the
tests never wait on real time.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


def _no_backoff(**kw: object) -> RetryPolicy:
    return RetryPolicy(base=0.0, rng=lambda: 0.0, **kw)  # type: ignore[arg-type]


class AckNackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.durability = InMemoryDurability()
        self.broker = Broker(
            self.durability, clock=FakeClock(), retry_policy=_no_backoff()
        )
        self.addAsyncCleanup(self.broker.close)

    async def test_ack_records_and_clears_inflight(self) -> None:
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

        await sub.ack(delivery)
        self.assertEqual(
            await self.durability.last_acked(delivery.subscription_id),
            delivery.message.message_id,
        )
        self.assertEqual(self.broker._inflight, {})

    async def test_nack_redelivers_with_incremented_attempt(self) -> None:
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        first = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(first.attempt, 1)

        await sub.nack(first)
        second = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(second.attempt, 2)
        self.assertEqual(second.message.message_id, first.message.message_id)
        # New delivery id per attempt.
        self.assertNotEqual(second.delivery_id, first.delivery_id)

    async def test_ack_of_unknown_delivery_is_noop(self) -> None:
        # Acking twice (or a stale id) must not raise.
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.ack(delivery)
        await sub.ack(delivery)  # second ack: no inflight entry, no error


if __name__ == "__main__":
    unittest.main()
