"""Shared test helpers (plain module; tests use stdlib unittest, not pytest)."""

import uuid

from pubsub.shared.types import Message, MessagePackValue


class FakeClock:
    """Manual clock: ``now()`` returns the set value; ``tick`` advances it."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def now(self) -> float:
        return self._t

    def tick(self, delta: float = 1.0) -> float:
        self._t += delta
        return self._t


def make_message(
    topic: str = "a.b",
    payload: MessagePackValue = None,
    *,
    created_at: float = 0.0,
    message_id: str | None = None,
    extras: dict[str, MessagePackValue] | None = None,
) -> Message[MessagePackValue]:
    return Message(
        message_id=message_id or uuid.uuid4().hex,
        topic=topic,
        payload=payload,
        extras=extras or {},
        created_at=created_at,
    )
