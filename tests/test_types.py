"""Immutability guard for domain envelopes.

"Immutable envelope" is a stated contract. The frozen behavior is custom
(``_Frozen``); if an envelope were later changed to a plain mutable class this
would regress silently. This test fails on that regression.
"""

import unittest

from pubsub.shared.types import Message


def _message() -> Message[int]:
    return Message(message_id="m1", topic="a.b", payload=1, extras={}, created_at=0.0)


class Immutability(unittest.TestCase):
    def test_reassigning_field_raises(self) -> None:
        message = _message()
        with self.assertRaises(AttributeError):
            message.payload = 2  # type: ignore[misc]

    def test_deleting_field_raises(self) -> None:
        message = _message()
        with self.assertRaises(AttributeError):
            del message.topic  # type: ignore[misc]


if __name__ == "__main__":
    unittest.main()
