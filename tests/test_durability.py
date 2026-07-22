"""Durability backend contract guards, run against every concrete backend.

The mixin fixes the invariants the broker relies on: replayable-only retention,
inclusive/ordered ``read_from``, ack persistence, and DLQ round-trip. Backend
subclasses add only their construction. SQLite additionally checks msgpack
payload fidelity and cross-reconnect persistence.
"""

import os
import shutil
import tempfile
import unittest
from typing import cast

from tests.conftest import make_message

from pubsub.server.durability.abc import DurabilityBackend
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.durability.sqlite import SQLiteDurability
from pubsub.shared.types import DLQEntry, MessagePackValue


class DurabilityContract:
    """Shared invariants. Subclasses implement ``_make`` (and optional teardown)."""

    async def _make(self) -> DurabilityBackend:
        raise NotImplementedError

    async def _open(self) -> DurabilityBackend:
        backend = await self._make()
        self.addAsyncCleanup(backend.close)  # type: ignore[attr-defined]
        return backend

    async def test_replayable_topic_is_retained_and_ordered(self) -> None:
        # read_from returns replayable history oldest-first, inclusive lower bound.
        b = await self._open()
        await b.register_topic("a.b", replayable=True)
        await b.append(make_message("a.b", 1, created_at=10.0, message_id="m1"))
        await b.append(make_message("a.b", 2, created_at=20.0, message_id="m2"))
        await b.append(make_message("a.b", 3, created_at=30.0, message_id="m3"))

        got = await b.read_from(20.0)
        self.assertEqual([m.message_id for m in got], ["m2", "m3"])  # type: ignore[attr-defined]

    async def test_read_from_before_retention_returns_all(self) -> None:
        # A timestamp older than any stored message yields everything, not an error.
        b = await self._open()
        await b.register_topic("a.b", replayable=True)
        await b.append(make_message("a.b", created_at=5.0, message_id="only"))
        got = await b.read_from(0.0)
        self.assertEqual([m.message_id for m in got], ["only"])  # type: ignore[attr-defined]

    async def test_live_only_topic_is_not_replayed(self) -> None:
        # append must succeed (publish depends on it) but leave no replay history.
        b = await self._open()
        await b.register_topic("live", replayable=False)
        await b.append(make_message("live", created_at=1.0))
        self.assertEqual(await b.read_from(0.0), [])  # type: ignore[attr-defined]

    async def test_unregistered_topic_defaults_to_not_replayable(self) -> None:
        b = await self._open()
        await b.append(make_message("never.registered", created_at=1.0))
        self.assertEqual(await b.read_from(0.0), [])  # type: ignore[attr-defined]

    async def test_last_acked_roundtrip_and_missing(self) -> None:
        b = await self._open()
        self.assertIsNone(await b.last_acked("sub"))  # type: ignore[attr-defined]
        await b.record_ack("sub", "m1")
        await b.record_ack("sub", "m2")  # latest wins
        self.assertEqual(await b.last_acked("sub"), "m2")  # type: ignore[attr-defined]

    async def test_dlq_roundtrip_preserves_fields(self) -> None:
        b = await self._open()
        entry = DLQEntry(
            message=make_message("x.y", {"k": "v"}, message_id="dead"),
            subscription_id="sub-9",
            attempts=7,
        )
        await b.to_dlq(entry)
        (stored,) = await b.read_dlq()
        self.assertEqual(stored.message.message_id, "dead")  # type: ignore[attr-defined]
        self.assertEqual(stored.subscription_id, "sub-9")  # type: ignore[attr-defined]
        self.assertEqual(stored.attempts, 7)  # type: ignore[attr-defined]
        self.assertEqual(stored.message.payload, {"k": "v"})  # type: ignore[attr-defined]


class InMemoryDurabilityTests(DurabilityContract, unittest.IsolatedAsyncioTestCase):
    async def _make(self) -> DurabilityBackend:
        return InMemoryDurability()


class SQLiteDurabilityTests(DurabilityContract, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._dir = tempfile.mkdtemp()

    def tearDown(self) -> None:
        shutil.rmtree(self._dir, ignore_errors=True)

    def _path(self) -> str:
        return os.path.join(self._dir, "dur.db")

    async def _make(self) -> DurabilityBackend:
        return await SQLiteDurability.connect(self._path(), readers=2)

    async def test_msgpack_payload_and_extras_roundtrip(self) -> None:
        # Blobs must survive pack/unpack, including bytes vs str and nesting.
        b = await self._open()
        await b.register_topic("a.b", replayable=True)
        payload = cast(
            MessagePackValue,
            {"n": 1, "s": "text", "raw": b"\x00\x01", "list": [1, 2, 3]},
        )
        await b.append(
            make_message("a.b", payload, created_at=1.0, extras={"trace": "id-1"})
        )
        (got,) = await b.read_from(0.0)
        self.assertEqual(got.payload, payload)
        self.assertEqual(got.extras, {"trace": "id-1"})

    async def test_group_commit_persists_all_concurrent_appends_in_order(self) -> None:
        # Group commit must not drop or reorder rows: fire many appends
        # concurrently (so they coalesce into shared transactions) and assert
        # every one is durable, oldest-first by created_at.
        b = await self._open()
        await b.register_topic("a.b", replayable=True)
        import asyncio

        await asyncio.gather(
            *(
                b.append(make_message("a.b", n, created_at=float(n), message_id=f"m{n}"))
                for n in range(50)
            )
        )
        got = await b.read_from(0.0)
        self.assertEqual([m.message_id for m in got], [f"m{n}" for n in range(50)])

    async def test_history_persists_across_reconnect(self) -> None:
        # Durability: data stored by one connection is visible after reopen,
        # even though the in-process replayable cache is empty on the new handle.
        first = await SQLiteDurability.connect(self._path())
        await first.register_topic("a.b", replayable=True)
        await first.append(make_message("a.b", created_at=1.0, message_id="persist"))
        await first.record_ack("sub", "persist")
        await first.close()

        second = await SQLiteDurability.connect(self._path())
        self.addAsyncCleanup(second.close)
        got = await second.read_from(0.0)
        self.assertEqual([m.message_id for m in got], ["persist"])
        self.assertEqual(await second.last_acked("sub"), "persist")


if __name__ == "__main__":
    unittest.main()
