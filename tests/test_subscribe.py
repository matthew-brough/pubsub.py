"""Broker subscribe/fanout: wildcard routing delivers only matching subjects."""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability


class SubscribeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = Broker(InMemoryDurability(), clock=FakeClock())
        self.addAsyncCleanup(self.broker.close)

    async def test_subscribe_rejects_bad_pattern(self) -> None:
        from pubsub.shared.topic import TopicError

        with self.assertRaises(TopicError):
            await self.broker.subscribe("a.>.b")  # '>' not final

    async def test_matching_subject_is_delivered(self) -> None:
        sub = await self.broker.subscribe("orders.*")
        result = await self.broker.publish("orders.created", {"id": 1})
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(delivery.message.message_id, result.message_id)
        self.assertEqual(delivery.message.payload, {"id": 1})
        self.assertEqual(delivery.attempt, 1)

    async def test_non_matching_subject_is_not_delivered(self) -> None:
        sub = await self.broker.subscribe("orders.*")
        await self.broker.publish("billing.created", {"id": 2})
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.__anext__(), timeout=0.1)

    async def test_fanout_to_multiple_subscribers(self) -> None:
        # One publish reaches every matching subscription independently.
        a = await self.broker.subscribe("news.>")
        b = await self.broker.subscribe("news.tech")
        await self.broker.publish("news.tech", "hello")
        da = await asyncio.wait_for(a.__anext__(), timeout=1.0)
        db = await asyncio.wait_for(b.__anext__(), timeout=1.0)
        self.assertEqual(da.message.payload, "hello")
        self.assertEqual(db.message.payload, "hello")

    async def test_unsubscribe_stops_stream(self) -> None:
        sub = await self.broker.subscribe("x.>")
        await sub.unsubscribe()
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(sub.__anext__(), timeout=1.0)


if __name__ == "__main__":
    unittest.main()
