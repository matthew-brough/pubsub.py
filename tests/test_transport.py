"""Network transport: end-to-end publish/subscribe/ack over the TCP wire."""

import asyncio
import unittest

from tests.conftest import FakeClock

from pubsub.server.broker import Broker
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.retry import RetryPolicy
from pubsub.shared.types import FromUnixTimestamp, PublishResult
from pubsub.transport.client import BrokerClient
from pubsub.transport.server import BrokerServer
from pubsub.transport.wire import (
    Op,
    ProtocolError,
    encode_frame,
    read_frame,
)


async def _aclose(writer: asyncio.StreamWriter) -> None:
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, OSError):
        pass


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.clock = FakeClock()
        self.broker = Broker(InMemoryDurability(), clock=self.clock)
        self.server = BrokerServer(self.broker, host="127.0.0.1", port=0)
        await self.server.start()
        self.addAsyncCleanup(self.server.close)
        self.addAsyncCleanup(self.broker.close)

    async def _client(self) -> BrokerClient:
        client = await BrokerClient.connect("127.0.0.1", self.server.port)
        self.addAsyncCleanup(client.close)
        return client

    async def test_publish_returns_ok(self) -> None:
        client = await self._client()
        result = await client.publish("orders.created", {"id": 1})
        self.assertTrue(result.accepted)
        self.assertIsNotNone(result.message_id)

    async def test_publish_rejects_bad_topic(self) -> None:
        client = await self._client()
        result = await client.publish("orders..created", {"id": 1})
        self.assertFalse(result.accepted)
        assert result.error is not None
        self.assertEqual(result.error.code, "invalid_topic")

    async def test_subscribe_receives_delivery_and_acks(self) -> None:
        client = await self._client()
        sub = await client.subscribe("orders.*")
        published = await client.publish("orders.created", {"id": 7})
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(delivery.message.message_id, published.message_id)
        self.assertEqual(delivery.message.payload, {"id": 7})
        self.assertEqual(delivery.attempt, 1)
        await sub.ack(delivery)

    async def test_nack_redelivers_with_incremented_attempt(self) -> None:
        # Fast retry so the redelivery lands within the test timeout.
        broker = Broker(
            InMemoryDurability(),
            clock=FakeClock(),
            retry_policy=RetryPolicy(max_attempts=5, base=0.0, rng=lambda: 0.0),
        )
        server = BrokerServer(broker, host="127.0.0.1", port=0)
        await server.start()
        self.addAsyncCleanup(server.close)
        self.addAsyncCleanup(broker.close)
        client = await BrokerClient.connect("127.0.0.1", server.port)
        self.addAsyncCleanup(client.close)

        sub = await client.subscribe("t.*")
        await client.publish("t.a", "v")
        first = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(first.attempt, 1)
        await sub.nack(first)
        second = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(second.attempt, 2)
        self.assertEqual(second.message.message_id, first.message.message_id)
        await sub.ack(second)

    async def test_replay_from_timestamp_delivers_history(self) -> None:
        # History published before the subscription exists is replayed.
        await self.broker.register_topic("h.a", replayable=True)
        self.clock._t = 5.0
        pub = await self.broker.publish("h.a", "old")
        client = await self._client()
        sub = await client.subscribe("h.>", FromUnixTimestamp(0.0))
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(delivery.message.message_id, pub.message_id)
        self.assertEqual(delivery.message.payload, "old")

    async def test_register_topic_over_wire_enables_replay(self) -> None:
        # Remote provisioning: a client declares retention, then history
        # published before any subscription is replayable over the wire.
        client = await self._client()
        await client.register_topic("prov.a", replayable=True)
        self.clock._t = 3.0
        pub = await client.publish("prov.a", "hist")
        sub = await client.subscribe("prov.>", FromUnixTimestamp(0.0))
        delivery = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(delivery.message.message_id, pub.message_id)

    async def test_publisher_convenience_binds_topic(self) -> None:
        from pubsub.client.publisher import Publisher

        client = await self._client()
        publisher: Publisher = Publisher(client, "orders.created")
        self.assertEqual(publisher.topic, "orders.created")
        result = await publisher.publish({"id": 99})
        self.assertTrue(result.accepted)

    async def test_unsubscribe_stops_stream(self) -> None:
        client = await self._client()
        sub = await client.subscribe("x.>")
        await sub.unsubscribe()
        with self.assertRaises(StopAsyncIteration):
            await asyncio.wait_for(sub.__anext__(), timeout=1.0)

    async def test_version_mismatch_is_rejected(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.addAsyncCleanup(_aclose, writer)
        writer.write(encode_frame({"op": Op.HELLO, "version": 999}))
        await writer.drain()
        reply = await asyncio.wait_for(read_frame(reader), timeout=1.0)
        self.assertEqual(reply["op"], Op.ERROR)

    async def test_non_hello_first_frame_is_rejected(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.addAsyncCleanup(_aclose, writer)
        writer.write(encode_frame({"op": Op.PUBLISH, "rid": 1, "topic": "a.b", "payload": 1}))
        await writer.drain()
        reply = await asyncio.wait_for(read_frame(reader), timeout=1.0)
        self.assertEqual(reply["op"], Op.ERROR)

    async def test_oversize_declared_frame_raises(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.server.port)
        self.addAsyncCleanup(_aclose, writer)
        # Length prefix far above the cap; server must not try to read the body.
        writer.write((64 * 1024 * 1024).to_bytes(4, "big"))
        await writer.drain()
        with self.assertRaises((asyncio.IncompleteReadError, ProtocolError, ConnectionError)):
            await asyncio.wait_for(read_frame(reader), timeout=1.0)


class ResumeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # Shared broker/durability so a restarted server keeps history + topics.
        self.clock = FakeClock()
        self.broker = Broker(InMemoryDurability(), clock=self.clock)
        await self.broker.register_topic("evt.a", replayable=True)
        self.addAsyncCleanup(self.broker.close)
        self.server = BrokerServer(self.broker, host="127.0.0.1", port=0)
        await self.server.start()
        self.port = self.server.port

    async def test_replay_subscription_resumes_after_reconnect(self) -> None:
        client = await BrokerClient.connect(
            "127.0.0.1", self.port, reconnect_base=0.02, heartbeat_interval=100.0
        )
        self.addAsyncCleanup(client.close)

        sub = await client.subscribe("evt.>", FromUnixTimestamp(0.0))
        self.clock._t = 1.0
        m1 = await client.publish("evt.a", "first")
        d1 = await asyncio.wait_for(sub.__anext__(), timeout=1.0)
        self.assertEqual(d1.message.message_id, m1.message_id)
        await sub.ack(d1)

        # Drop the server; the client's read loop sees EOF and starts reconnecting.
        await self.server.close()
        # Bring a new server up on the same port, sharing the broker.
        server2 = BrokerServer(self.broker, host="127.0.0.1", port=self.port)
        await server2.start()
        self.addAsyncCleanup(server2.close)

        # Published only after the outage; must arrive on the resumed handle.
        self.clock._t = 2.0
        m2 = await _publish_when_connected(client, "evt.a", "second")

        seen: set[str] = set()
        while m2.message_id not in seen:
            d = await asyncio.wait_for(sub.__anext__(), timeout=2.0)
            seen.add(d.message.message_id)
            await sub.ack(d)
        self.assertIn(m2.message_id, seen)


async def _publish_when_connected(
    client: BrokerClient, topic: str, payload: str
) -> PublishResult:
    """Retry publish until the client has reconnected."""
    for _ in range(200):
        try:
            return await client.publish(topic, payload)
        except (ConnectionError, ProtocolError):
            await asyncio.sleep(0.02)
    raise AssertionError("client never reconnected")


class PublicApiTests(unittest.TestCase):
    def test_wire_surface_is_exported(self) -> None:
        import pubsub

        for name in ("Broker", "BrokerServer", "BrokerClient", "ProtocolError"):
            self.assertTrue(hasattr(pubsub, name), name)
        self.assertTrue(callable(pubsub.connect))


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_memory_durability_is_default(self) -> None:
        from pubsub.server.durability.memory import InMemoryDurability
        from pubsub.transport.runner import _build_durability

        backend = await _build_durability("memory", "")
        self.assertIsInstance(backend, InMemoryDurability)


if __name__ == "__main__":
    unittest.main()
