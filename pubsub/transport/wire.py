"""Wire protocol shared by the network client and server.

Framing: every frame is a 4-byte big-endian unsigned length prefix followed by
a MessagePack body (``packb(use_bin_type=True)`` / ``unpackb(raw=False)`` — the
same codec the durability layer already uses on disk). Bodies are always
MessagePack maps tagged with an ``"op"`` key; the ``OP_*`` constants below are
the whole vocabulary.

The protocol is deliberately framework-free: it speaks to
``asyncio.StreamReader``/``StreamWriter`` only, so any stream transport can
carry it. Domain <-> wire mapping lives here so client and server can never
disagree on the shape of a frame.
"""

import asyncio
from collections.abc import Mapping
from enum import StrEnum
from typing import cast

import msgpack

from pubsub.shared.types import (
    Delivery,
    FromUnixTimestamp,
    FutureOnly,
    Message,
    MessagePackValue,
    PublishError,
    PublishResult,
    ReplayPolicy,
)

PROTOCOL_VERSION = 1
# Hard cap on a single frame. An oversize declared length is rejected before a
# single body byte is read, so a hostile peer cannot force a huge allocation.
MAX_FRAME_BYTES = 8 * 1024 * 1024
_LENGTH_BYTES = 4

# A frame is a MessagePack map tagged with "op". Alias for readability.
type Frame = dict[str, MessagePackValue]

class Op(StrEnum):
    """Frame ``"op"`` vocabulary. ``StrEnum`` members are ``str``, so they pack
    as plain MessagePack strings and compare equal to the decoded values."""

    # client -> server
    HELLO = "hello"
    REGISTER = "register"
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"
    ACK = "ack"
    NACK = "nack"
    UNSUBSCRIBE = "unsubscribe"
    # server -> client
    WELCOME = "welcome"
    REGISTER_OK = "register_ok"
    PUBLISH_RESULT = "publish_result"
    SUBSCRIBE_OK = "subscribe_ok"
    DELIVERY = "delivery"
    SUB_CLOSED = "sub_closed"
    ERROR = "error"
    # both directions
    HEARTBEAT = "heartbeat"


HEARTBEAT_FRAME: Frame = {"op": Op.HEARTBEAT}


class ProtocolError(Exception):
    """Malformed frame, oversize frame, or protocol-version mismatch."""


# --- framing -------------------------------------------------------------
def encode_frame(frame: Frame) -> bytes:
    body = cast(bytes, msgpack.packb(frame, use_bin_type=True))
    if len(body) > MAX_FRAME_BYTES:
        raise ProtocolError(f"frame too large to send: {len(body)} > {MAX_FRAME_BYTES}")
    return len(body).to_bytes(_LENGTH_BYTES, "big") + body


async def read_frame(reader: asyncio.StreamReader) -> Frame:
    """Read one length-prefixed frame.

    Propagates ``asyncio.IncompleteReadError`` on a clean EOF (caller treats it
    as a closed connection). Raises ``ProtocolError`` on an oversize or
    non-op-tagged frame.
    """
    header = await reader.readexactly(_LENGTH_BYTES)
    size = int.from_bytes(header, "big")
    if size > MAX_FRAME_BYTES:
        raise ProtocolError(f"declared frame size {size} exceeds cap {MAX_FRAME_BYTES}")
    body = await reader.readexactly(size)
    obj = msgpack.unpackb(body, raw=False)
    if not isinstance(obj, dict) or "op" not in obj:
        raise ProtocolError("frame is not an op-tagged map")
    return cast(Frame, obj)


# --- domain <-> wire -----------------------------------------------------
def message_to_wire(message: Message[MessagePackValue]) -> dict[str, MessagePackValue]:
    # ``dict`` is invariant in its key type, so a ``dict[str, ...]`` value does
    # not statically satisfy the recursive ``MessagePackValue`` alias (whose map
    # is keyed by ``MessagePackValue``). The runtime shape is correct; cast.
    return cast(
        "dict[str, MessagePackValue]",
        {
            "message_id": message.message_id,
            "topic": message.topic,
            "payload": message.payload,
            "extras": dict(message.extras),
            "created_at": message.created_at,
        },
    )


def message_from_wire(
    data: Mapping[str, MessagePackValue],
) -> Message[MessagePackValue]:
    return Message(
        message_id=cast(str, data["message_id"]),
        topic=cast(str, data["topic"]),
        payload=data["payload"],
        extras=cast("dict[str, MessagePackValue]", data["extras"]),
        created_at=cast(float, data["created_at"]),
    )


def delivery_to_wire(delivery: Delivery[MessagePackValue]) -> Frame:
    return cast(
        Frame,
        {
            "op": Op.DELIVERY,
            "subscription_id": delivery.subscription_id,
            "delivery_id": delivery.delivery_id,
            "attempt": delivery.attempt,
            "message": message_to_wire(delivery.message),
        },
    )


def delivery_from_wire(frame: Mapping[str, MessagePackValue]) -> Delivery[MessagePackValue]:
    return Delivery(
        delivery_id=cast(str, frame["delivery_id"]),
        subscription_id=cast(str, frame["subscription_id"]),
        message=message_from_wire(cast("Mapping[str, MessagePackValue]", frame["message"])),
        attempt=cast(int, frame["attempt"]),
    )


def replay_to_wire(policy: ReplayPolicy) -> dict[str, MessagePackValue]:
    if isinstance(policy, FromUnixTimestamp):
        return {"mode": "from", "timestamp": policy.timestamp}
    return {"mode": "future"}


def replay_from_wire(data: Mapping[str, MessagePackValue]) -> ReplayPolicy:
    mode = data.get("mode")
    if mode == "from":
        return FromUnixTimestamp(cast(float, data["timestamp"]))
    if mode == "future":
        return FutureOnly()
    raise ProtocolError(f"unknown replay mode: {mode!r}")


def publish_result_to_wire(rid: int, result: PublishResult) -> Frame:
    frame: Frame = {"op": Op.PUBLISH_RESULT, "rid": rid, "accepted": result.accepted}
    if result.accepted:
        frame["message_id"] = result.message_id
    elif result.error is not None:
        frame["error"] = cast(
            MessagePackValue,
            {"code": result.error.code, "detail": result.error.detail},
        )
    return frame


def publish_result_from_wire(frame: Mapping[str, MessagePackValue]) -> PublishResult:
    if frame.get("accepted"):
        return PublishResult.ok(cast(str, frame["message_id"]))
    error = cast("Mapping[str, MessagePackValue]", frame.get("error") or {})
    return PublishResult.rejected(
        PublishError(
            cast(str, error.get("code", "error")),
            cast(str, error.get("detail", "")),
        )
    )
