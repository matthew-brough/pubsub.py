"""Shared domain objects used by both client and server layers.

Immutable envelopes are implemented as plain ``__slots__`` classes with a
frozen ``__setattr__`` guard (hand-rolled per project preference; no
dataclasses). Construction goes through ``object.__setattr__`` via ``_set``.
"""

from collections.abc import Mapping

type MessagePackValue = (
    str
    | int
    | float
    | bool
    | None
    | bytes
    | list[MessagePackValue]
    | dict[MessagePackValue, MessagePackValue]
)


class _Frozen:
    """Base for immutable envelopes: blocks attribute (re)assignment."""

    __slots__ = ()

    def __setattr__(self, name: str, _: object) -> None:
        raise AttributeError(f"cannot set {name!r}: {type(self).__name__} is immutable")

    def __delattr__(self, name: str) -> None:
        raise AttributeError(f"cannot delete {name!r}: {type(self).__name__} is immutable")


def _set(obj: object, name: str, value: object) -> None:
    object.__setattr__(obj, name, value)


class ReplayPolicy(_Frozen):
    """Subscribe-time replay selection. See ``FutureOnly`` / ``FromUnixTimestamp``."""

    __slots__ = ()


class FutureOnly(ReplayPolicy):
    """Deliver only messages published after the subscription registers."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "FutureOnly()"


class FromUnixTimestamp(ReplayPolicy):
    """Replay durable history from ``timestamp`` (inclusive), then continue live.

    A timestamp before retention start yields available retained data, not an
    error (per delivery-semantics notes).
    """

    __slots__ = ("timestamp",)

    timestamp: float

    def __init__(self, timestamp: float) -> None:
        _set(self, "timestamp", float(timestamp))

    def __repr__(self) -> str:
        return f"FromUnixTimestamp({self.timestamp!r})"


class TopicConfig(_Frozen):
    """Producer-time retention config: replay-capable or live-only."""

    __slots__ = ("topic", "replayable")

    topic: str
    replayable: bool

    def __init__(self, topic: str, *, replayable: bool) -> None:
        _set(self, "topic", topic)
        _set(self, "replayable", replayable)


class Message[PayloadT](_Frozen):
    """Immutable message envelope.

    ``message_id`` is a broker-created UUID string. ``created_at`` is Unix epoch
    seconds (external representation; broker converts from its internal clock).
    ``extras`` is the user metadata namespace; broker metadata is not mixed in
    here so publisher input cannot forge protected fields.
    """

    __slots__ = ("message_id", "topic", "payload", "extras", "created_at")

    message_id: str
    topic: str
    payload: PayloadT
    extras: Mapping[str, MessagePackValue]
    created_at: float

    def __init__(
        self,
        *,
        message_id: str,
        topic: str,
        payload: PayloadT,
        extras: Mapping[str, MessagePackValue],
        created_at: float,
    ) -> None:
        _set(self, "message_id", message_id)
        _set(self, "topic", topic)
        _set(self, "payload", payload)
        _set(self, "extras", extras)
        _set(self, "created_at", created_at)

    def __repr__(self) -> str:
        return (
            f"Message(message_id={self.message_id!r}, topic={self.topic!r}, "
            f"created_at={self.created_at!r})"
        )


class Delivery[PayloadT](_Frozen):
    """A single delivery attempt of a message to one subscription.

    ``attempt`` starts at 1 for the first-time delivery and increments per
    redelivery. ``delivery_id`` is unique per attempt.
    """

    __slots__ = ("delivery_id", "subscription_id", "message", "attempt")

    delivery_id: str
    subscription_id: str
    message: "Message[PayloadT]"
    attempt: int

    def __init__(
        self,
        *,
        delivery_id: str,
        subscription_id: str,
        message: "Message[PayloadT]",
        attempt: int,
    ) -> None:
        _set(self, "delivery_id", delivery_id)
        _set(self, "subscription_id", subscription_id)
        _set(self, "message", message)
        _set(self, "attempt", attempt)

    def __repr__(self) -> str:
        return (
            f"Delivery(delivery_id={self.delivery_id!r}, "
            f"subscription_id={self.subscription_id!r}, attempt={self.attempt!r})"
        )


class Subscription(_Frozen):
    """Broker-owned subscription registration record."""

    __slots__ = ("subscription_id", "selector", "replay_policy")

    subscription_id: str
    selector: str
    replay_policy: ReplayPolicy

    def __init__(
        self, *, subscription_id: str, selector: str, replay_policy: ReplayPolicy
    ) -> None:
        _set(self, "subscription_id", subscription_id)
        _set(self, "selector", selector)
        _set(self, "replay_policy", replay_policy)

    def __repr__(self) -> str:
        return (
            f"Subscription(subscription_id={self.subscription_id!r}, "
            f"selector={self.selector!r})"
        )


class PublishError(_Frozen):
    """Structured rejection reason carried by a rejected ``PublishResult``."""

    __slots__ = ("code", "detail")

    code: str
    detail: str

    def __init__(self, code: str, detail: str) -> None:
        _set(self, "code", code)
        _set(self, "detail", detail)

    def __repr__(self) -> str:
        return f"PublishError(code={self.code!r}, detail={self.detail!r})"


class PublishResult(_Frozen):
    """Accepted/rejected outcome of a publish.

    Accepted carries the broker-created ``message_id``; rejected carries a
    structured ``error`` and ``message_id is None``.
    """

    __slots__ = ("accepted", "message_id", "error")

    accepted: bool
    message_id: str | None
    error: PublishError | None

    def __init__(
        self,
        *,
        accepted: bool,
        message_id: str | None = None,
        error: PublishError | None = None,
    ) -> None:
        _set(self, "accepted", accepted)
        _set(self, "message_id", message_id)
        _set(self, "error", error)

    @classmethod
    def ok(cls, message_id: str) -> "PublishResult":
        return cls(accepted=True, message_id=message_id)

    @classmethod
    def rejected(cls, error: PublishError) -> "PublishResult":
        return cls(accepted=False, error=error)

    def __repr__(self) -> str:
        if self.accepted:
            return f"PublishResult(accepted=True, message_id={self.message_id!r})"
        return f"PublishResult(accepted=False, error={self.error!r})"


class DLQEntry(_Frozen):
    """Dead-letter record: a message whose retry budget was exhausted."""

    __slots__ = ("message", "subscription_id", "attempts")

    message: "Message[MessagePackValue]"
    subscription_id: str
    attempts: int

    def __init__(
        self,
        *,
        message: "Message[MessagePackValue]",
        subscription_id: str,
        attempts: int,
    ) -> None:
        _set(self, "message", message)
        _set(self, "subscription_id", subscription_id)
        _set(self, "attempts", attempts)
