"""Observability seam: the broker fires ``Observer`` hooks at its boundaries.

A recording ``Observer`` subclass captures calls; assertions check publish
accept/reject, deliver, ack, and nack hooks. Backoff is forced to zero so the
nack redelivery path is deterministic.
"""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.observability import Observer
from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy


class _RecordingObserver(Observer):
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def on_publish(self, topic: str, *, accepted: bool, reason: str | None = None) -> None:
        self.calls.append(("publish", (topic, accepted, reason)))

    def on_deliver(self, topic: str, subscription_id: str, *, attempt: int) -> None:
        self.calls.append(("deliver", (topic, subscription_id, attempt)))

    def on_ack(self, subscription_id: str) -> None:
        self.calls.append(("ack", (subscription_id,)))

    def on_nack(self, subscription_id: str, *, attempt: int) -> None:
        self.calls.append(("nack", (subscription_id, attempt)))

    def on_retry_exhausted(self, subscription_id: str) -> None:
        self.calls.append(("retry_exhausted", (subscription_id,)))

    def kinds(self) -> list[str]:
        return [kind for kind, _ in self.calls]


class ObserverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.obs = _RecordingObserver()
        self.broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(base=0.0, rng=lambda: 0.0),
            observer=self.obs,
        )
        self.addAsyncCleanup(self.broker.close)

    async def test_default_observer_is_noop(self) -> None:
        # No observer passed -> base Observer, publish/subscribe must not raise.
        broker = Broker(InMemoryDurability(), clock=FakeClock())
        self.addAsyncCleanup(broker.close)
        result = await broker.publish("t.x", 1)
        self.assertTrue(result.accepted)

    async def test_rejected_publish_hook(self) -> None:
        await self.broker.publish("a..b", 1)
        self.assertIn(("publish", ("a..b", False, "invalid_topic")), self.obs.calls)

    async def test_accepted_publish_and_deliver_hooks(self) -> None:
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

        self.assertIn(("publish", ("t.x", True, None)), self.obs.calls)
        self.assertIn(("deliver", ("t.x", delivery.subscription_id, 1)), self.obs.calls)

    async def test_ack_and_nack_hooks(self) -> None:
        sub = await self.broker.subscribe("t.*")
        await self.broker.publish("t.x", 1)
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)

        await sub.nack(delivery)
        self.assertIn(("nack", (delivery.subscription_id, 1)), self.obs.calls)

        redelivered = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        await sub.ack(redelivered)
        self.assertIn(("ack", (delivery.subscription_id,)), self.obs.calls)


if __name__ == "__main__":
    unittest.main()
