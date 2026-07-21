"""Broker replay: FromUnixTimestamp streams durable history, then live.

FutureOnly (the default) must skip pre-subscribe history entirely.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.shared.types import FromUnixTimestamp


class ReplayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = FakeClock(start=1.0)
        self.broker = Broker(InMemoryDurability(), clock=self.clock)
        self.addAsyncCleanup(self.broker.close)
        await self.broker.register_topic("evt.a", replayable=True)

    async def test_future_only_skips_history(self) -> None:
        # Default policy: messages published before subscribe are not replayed.
        await self.broker.publish("evt.a", "old")
        sub = await self.broker.subscribe("evt.>")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.__anext__(), timeout=0.1)

    async def test_replay_from_timestamp_then_live(self) -> None:
        # History replays oldest-first, inclusive of the timestamp, then a
        # post-subscribe publish is delivered live on the same stream.
        self.clock._t = 10.0
        await self.broker.publish("evt.a", "m1")
        self.clock._t = 20.0
        await self.broker.publish("evt.a", "m2")

        sub = await self.broker.subscribe("evt.>", FromUnixTimestamp(10.0))
        first = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        second = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual([first.message.payload, second.message.payload], ["m1", "m2"])

        self.clock._t = 30.0
        await self.broker.publish("evt.a", "m3")
        live = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(live.message.payload, "m3")

    async def test_replay_respects_inclusive_lower_bound(self) -> None:
        self.clock._t = 5.0
        await self.broker.publish("evt.a", "before")
        self.clock._t = 15.0
        await self.broker.publish("evt.a", "after")

        sub = await self.broker.subscribe("evt.>", FromUnixTimestamp(15.0))
        got = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(got.message.payload, "after")
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(sub.__anext__(), timeout=0.1)


if __name__ == "__main__":
    unittest.main()
