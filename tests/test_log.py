"""DurableHandler concretes: buffer/flush boundary and level/time filtering.

The persistence boundary is the invariant that matters — ``emit`` buffers, and
records only become queryable via ``retrieve`` after ``flush_to_storage``.
"""

import logging
import os
import shutil
import tempfile
import unittest

from pubsub.server.log import InMemoryDurableHandler, SQLiteDurableHandler
from pubsub.shared.log import configure_logging


def _record(name: str, level: int, msg: str, created: float) -> logging.LogRecord:
    record = logging.LogRecord(name, level, __file__, 0, msg, None, None)
    record.created = created
    return record


class InMemoryHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.h = InMemoryDurableHandler()

    def test_records_invisible_until_flush(self) -> None:
        self.h.emit(_record("x", logging.INFO, "hi", 1.0))
        self.assertEqual(self.h.retrieve(), [])  # buffered, not persisted
        self.h.flush_to_storage()
        self.assertEqual([r.getMessage() for r in self.h.retrieve()], ["hi"])

    def test_level_filter(self) -> None:
        self.h.emit(_record("x", logging.INFO, "lo", 1.0))
        self.h.emit(_record("x", logging.ERROR, "hi", 2.0))
        self.h.flush_to_storage()
        got = [r.getMessage() for r in self.h.retrieve(level=logging.ERROR)]
        self.assertEqual(got, ["hi"])

    def test_since_filter_is_inclusive(self) -> None:
        self.h.emit(_record("x", logging.INFO, "old", 10.0))
        self.h.emit(_record("x", logging.INFO, "new", 20.0))
        self.h.flush_to_storage()
        got = [r.getMessage() for r in self.h.retrieve(since=20.0)]
        self.assertEqual(got, ["new"])


class ConfigureLoggingTests(unittest.TestCase):
    """configure_logging attaches handlers to the ``pubsub`` root logger so
    records emitted anywhere in the package reach the durable handler."""

    def tearDown(self) -> None:
        root = logging.getLogger("pubsub")
        for handler in list(root.handlers):
            root.removeHandler(handler)
        root.propagate = True

    def test_wires_durable_handler_and_disables_propagation(self) -> None:
        durable = InMemoryDurableHandler()
        configure_logging(level=logging.INFO, durable=durable)
        root = logging.getLogger("pubsub")
        self.assertIn(durable, root.handlers)
        self.assertFalse(root.propagate)

        logging.getLogger("pubsub.audit").error("boom")
        durable.flush_to_storage()
        got = [r.getMessage() for r in durable.retrieve(level=logging.ERROR)]
        self.assertEqual(got, ["boom"])

    def test_idempotent_clears_prior_handlers(self) -> None:
        configure_logging(durable=InMemoryDurableHandler())
        configure_logging(durable=InMemoryDurableHandler())
        root = logging.getLogger("pubsub")
        # One console + one durable, not doubled up across the two calls.
        self.assertEqual(len(root.handlers), 2)


class SQLiteHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._dir = tempfile.mkdtemp()
        self.path = os.path.join(self._dir, "logs.db")

    def tearDown(self) -> None:
        shutil.rmtree(self._dir, ignore_errors=True)

    def test_records_invisible_until_flush(self) -> None:
        h = SQLiteDurableHandler(self.path)
        self.addCleanup(h.close)
        h.emit(_record("x", logging.WARNING, "buffered", 1.0))
        self.assertEqual(h.retrieve(), [])
        h.flush_to_storage()
        self.assertEqual([r.getMessage() for r in h.retrieve()], ["buffered"])

    def test_persists_across_handler_instances(self) -> None:
        first = SQLiteDurableHandler(self.path)
        first.emit(_record("x", logging.ERROR, "kept", 5.0))
        first.close()  # close flushes

        second = SQLiteDurableHandler(self.path)
        self.addCleanup(second.close)
        got = [r.getMessage() for r in second.retrieve()]
        self.assertEqual(got, ["kept"])

    def test_level_and_since_filters(self) -> None:
        h = SQLiteDurableHandler(self.path)
        self.addCleanup(h.close)
        h.emit(_record("x", logging.INFO, "a", 1.0))
        h.emit(_record("x", logging.ERROR, "b", 2.0))
        h.emit(_record("x", logging.ERROR, "c", 3.0))
        h.flush_to_storage()
        got = [r.getMessage() for r in h.retrieve(level=logging.ERROR, since=3.0)]
        self.assertEqual(got, ["c"])


if __name__ == "__main__":
    unittest.main()
