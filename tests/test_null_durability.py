"""NullDurability: accept-and-drop backend keeps nothing but never breaks fanout."""

import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.null import NullDurability
from pubsub.shared.types import DLQEntry, FromUnixTimestamp, Message


class NullDurabilityBackendTests(unittest.IsolatedAsyncioTestCase):
    async def test_retains_nothing(self) -> None:
        backend = NullDurability()
        await backend.register_topic("t.x", replayable=True)  # even replayable
        await backend.append(
            Message(
                message_id="m1",
                topic="t.x",
                payload=1,
                extras={},
                created_at=0.0,
            )
        )
        self.assertEqual(await backend.read_from(0.0), [])

    async def test_no_ack_or_dlq_state(self) -> None:
        backend = NullDurability()
        await backend.record_ack("sub", "m1")
        self.assertIsNone(await backend.last_acked("sub"))
        await backend.to_dlq(
            DLQEntry(
                message=Message(
                    message_id="m1",
                    topic="t.x",
                    payload=1,
                    extras={},
                    created_at=0.0,
                ),
                subscription_id="sub",
                attempts=5,
            )
        )
        self.assertEqual(await backend.read_dlq(), [])
        await backend.close()


class NullDurabilityBrokerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = Broker(NullDurability(), clock=FakeClock())
        self.addAsyncCleanup(self.broker.close)

    async def test_live_fanout_still_works(self) -> None:
        sub = await self.broker.subscribe("t.*")
        self.assertTrue((await self.broker.publish("t.x", "hi")).accepted)
        delivery = await sub.__anext__()
        self.assertEqual(delivery.message.payload, "hi")
        await sub.ack(delivery)  # no-op ack path must not raise

    async def test_replay_is_empty(self) -> None:
        await self.broker.register_topic("t.x", replayable=True)
        self.assertTrue((await self.broker.publish("t.x", "gone")).accepted)
        # A replay subscription gets nothing back — history is not retained.
        sub = await self.broker.subscribe("t.*", FromUnixTimestamp(0.0))
        self.assertTrue((await self.broker.publish("t.x", "live")).accepted)
        delivery = await sub.__anext__()
        self.assertEqual(delivery.message.payload, "live")


if __name__ == "__main__":
    unittest.main()
