"""Broker publish path: validation and durable-before-return.

Uses the in-memory backend so the broker is exercised end-to-end.
"""

import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability


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


if __name__ == "__main__":
    unittest.main()
