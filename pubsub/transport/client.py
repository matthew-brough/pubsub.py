"""TCP client: the remote-broker surface, with transparent resume.

``BrokerClient.connect`` opens a connection, performs the hello/welcome
handshake, and starts a single background read loop. That loop is the only
reader of the socket: it resolves request futures (publish/subscribe replies by
``rid``) and fans delivery frames out to the right subscription queue. Keeping
all reads in one place preserves frame order and avoids a race between a
subscribe reply and the deliveries that follow it.

Resume: a subscription is a stable, client-owned handle. When the connection
drops, the client reconnects with exponential backoff and re-subscribes every
live handle, so ``async for delivery in sub`` keeps working across the outage.
The server mints a fresh subscription id on each (re)subscribe; the client
remaps it under the hood. Replay-capable subscriptions resume from the last
acked message (at-least-once: the boundary message may be re-delivered once);
live-only subscriptions resume future-only and do not recover the gap — matching
the delivery-semantics decision.

In-flight ``publish``/``subscribe`` calls are *not* resumed: they fail with
``ConnectionError`` on disconnect so the caller can retry. Only established
subscriptions auto-resume.
"""

import asyncio
import itertools
import logging
from collections.abc import Mapping
from typing import cast

from pubsub.client.subscriber import Subscriber
from pubsub.shared.types import (
    Delivery,
    FromUnixTimestamp,
    FutureOnly,
    MessagePackValue,
    PublishResult,
    ReplayPolicy,
)
from pubsub.transport import wire
from pubsub.transport.abc import ClientTransport
from pubsub.transport.wire import Frame, Op, ProtocolError

DEFAULT_HEARTBEAT_INTERVAL = 15.0
DEFAULT_RECONNECT_BASE = 0.5
DEFAULT_RECONNECT_CAP = 30.0


class BrokerClient(ClientTransport):
    def __init__(
        self,
        host: str,
        port: int,
        *,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        reconnect: bool = True,
        reconnect_base: float = DEFAULT_RECONNECT_BASE,
        reconnect_cap: float = DEFAULT_RECONNECT_CAP,
        max_reconnect_attempts: int | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._interval = heartbeat_interval
        self._reconnect = reconnect
        self._reconnect_base = reconnect_base
        self._reconnect_cap = reconnect_cap
        self._max_reconnect = max_reconnect_attempts
        self._loop = asyncio.get_running_loop()
        self._write_lock = asyncio.Lock()
        self._rids = itertools.count(1)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        # rid -> future resolved by the read loop with the reply frame
        self._pending: dict[int, asyncio.Future[Frame]] = {}
        # local_id -> handle (stable across reconnect)
        self._subs: dict[str, _Subscription] = {}
        # current server subscription id -> handle (rebuilt on each resume)
        self._by_server: dict[str, _Subscription] = {}
        self._local_ids = itertools.count(1)
        self._read_task: asyncio.Task[None] | None = None
        self._hb_task: asyncio.Task[None] | None = None
        self._reconnect_task: asyncio.Task[None] | None = None
        self._connected = asyncio.Event()
        self._closed = False
        self._log = logging.getLogger("pubsub.transport.client")

    @classmethod
    async def connect(
        cls,
        host: str = "127.0.0.1",
        port: int = 8765,
        *,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        reconnect: bool = True,
        reconnect_base: float = DEFAULT_RECONNECT_BASE,
        reconnect_cap: float = DEFAULT_RECONNECT_CAP,
        max_reconnect_attempts: int | None = None,
    ) -> "BrokerClient":
        self = cls(
            host,
            port,
            heartbeat_interval=heartbeat_interval,
            reconnect=reconnect,
            reconnect_base=reconnect_base,
            reconnect_cap=reconnect_cap,
            max_reconnect_attempts=max_reconnect_attempts,
        )
        await self._open()
        return self

    async def register_topic(self, topic: str, *, replayable: bool) -> None:
        frame = await self._request(
            cast(Frame, {"op": Op.REGISTER, "topic": topic, "replayable": replayable})
        )
        if frame.get("op") == Op.ERROR:
            raise ProtocolError(str(frame.get("detail")))

    async def publish(
        self,
        topic: str,
        payload: MessagePackValue,
        *,
        extras: Mapping[str, MessagePackValue] | None = None,
    ) -> PublishResult:
        frame = await self._request(
            cast(
                Frame,
                {"op": Op.PUBLISH, "topic": topic, "payload": payload, "extras": dict(extras or {})},
            )
        )
        if frame.get("op") == Op.ERROR:
            raise ProtocolError(str(frame.get("detail")))
        return wire.publish_result_from_wire(frame)

    async def subscribe(
        self, selector: str, replay_policy: ReplayPolicy | None = None
    ) -> Subscriber[MessagePackValue]:
        policy: ReplayPolicy = replay_policy or FutureOnly()
        sub = _Subscription(self, str(next(self._local_ids)), selector, policy)
        self._subs[sub._local_id] = sub
        await self._activate(sub, policy)
        return sub

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for task in (self._reconnect_task, self._hb_task, self._read_task):
            if task is not None:
                task.cancel()
        for sub in list(self._subs.values()):
            sub._close()
        self._subs.clear()
        self._by_server.clear()
        self._fail_pending(ConnectionError("client closed"))
        await self._close_writer()

    # --- connection lifecycle -------------------------------------------
    async def _open(self) -> None:
        reader, writer = await asyncio.open_connection(self._host, self._port)
        await self._handshake(reader, writer)
        self._reader, self._writer = reader, writer
        self._read_task = asyncio.create_task(self._read_loop(reader))
        self._hb_task = asyncio.create_task(self._heartbeat_loop())
        self._connected.set()

    async def _handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        writer.write(wire.encode_frame({"op": Op.HELLO, "version": wire.PROTOCOL_VERSION}))
        await writer.drain()
        welcome = await wire.read_frame(reader)
        if welcome.get("op") != Op.WELCOME:
            writer.close()
            raise ProtocolError(f"expected welcome, got {welcome.get('op')!r}")
        if welcome.get("version") != wire.PROTOCOL_VERSION:
            writer.close()
            raise ProtocolError(f"server version {welcome.get('version')!r} unsupported")

    async def _activate(self, sub: "_Subscription", policy: ReplayPolicy) -> None:
        """(Re)register a subscription on the current connection."""
        frame = await self._request(
            cast(
                Frame,
                {
                    "op": Op.SUBSCRIBE,
                    "selector": sub._selector,
                    "replay": wire.replay_to_wire(policy),
                },
            )
        )
        if frame.get("op") == Op.ERROR:
            raise ProtocolError(str(frame.get("detail")))
        server_sid = str(frame["subscription_id"])
        sub._server_sid = server_sid
        self._by_server[server_sid] = sub

    async def _read_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                try:
                    frame = await wire.read_frame(reader)
                except (asyncio.IncompleteReadError, ConnectionError, ProtocolError):
                    return
                self._on_frame(frame)
        finally:
            self._on_disconnect()

    def _on_frame(self, frame: Frame) -> None:
        op = frame.get("op")
        if op in (Op.REGISTER_OK, Op.PUBLISH_RESULT, Op.SUBSCRIBE_OK, Op.ERROR):
            self._resolve(frame)
        elif op == Op.DELIVERY:
            self._route_delivery(frame)
        elif op == Op.SUB_CLOSED:
            sub = self._by_server.pop(str(frame.get("subscription_id")), None)
            if sub is not None:
                # Broker closed it server-side (e.g. slow-subscriber eviction).
                # Don't resume; drop the handle too.
                self._subs.pop(sub._local_id, None)
                sub._close()
        elif op == Op.HEARTBEAT:
            pass

    def _resolve(self, frame: Frame) -> None:
        rid = frame.get("rid")
        if not isinstance(rid, int):
            return
        fut = self._pending.get(rid)
        if fut is not None and not fut.done():
            fut.set_result(frame)

    def _route_delivery(self, frame: Frame) -> None:
        delivery = wire.delivery_from_wire(frame)
        sub = self._by_server.get(delivery.subscription_id)
        if sub is not None:
            sub._push(delivery)

    def _on_disconnect(self) -> None:
        self._connected.clear()
        self._by_server.clear()
        # Close the dead socket and stop its heartbeat before a reconnect
        # replaces them, so the old writer does not leak.
        if self._hb_task is not None:
            self._hb_task.cancel()
        if self._writer is not None:
            self._writer.close()
        # In-flight requests can't survive a reconnect; fail them.
        self._fail_pending(ConnectionError("connection lost"))
        if self._closed or not self._reconnect:
            for sub in list(self._subs.values()):
                sub._close()
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _reconnect_loop(self) -> None:
        attempt = 0
        while not self._closed:
            if self._max_reconnect is not None and attempt >= self._max_reconnect:
                break
            delay = min(self._reconnect_base * (2**attempt), self._reconnect_cap)
            await asyncio.sleep(delay)
            attempt += 1
            try:
                await self._open()
            except (ConnectionError, OSError, ProtocolError, asyncio.IncompleteReadError):
                continue
            try:
                await self._resume_all()
            except (ConnectionError, ProtocolError):
                continue  # lost it again mid-resume; _open's read loop will retrigger
            return
        # Reconnect gave up: surface closure to consumers.
        for sub in list(self._subs.values()):
            sub._close()
        self._subs.clear()

    async def _resume_all(self) -> None:
        for sub in list(self._subs.values()):
            await self._activate(sub, sub._resume_policy())

    # --- requests / writes ----------------------------------------------
    async def _request(self, frame: Frame) -> Frame:
        if not self._connected.is_set():
            raise ConnectionError("not connected")
        rid = next(self._rids)
        frame["rid"] = rid
        fut: asyncio.Future[Frame] = self._loop.create_future()
        self._pending[rid] = fut
        try:
            await self._raw_send(frame)
            return await fut
        finally:
            self._pending.pop(rid, None)

    def _fail_pending(self, exc: BaseException) -> None:
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(exc)
        self._pending.clear()

    async def _heartbeat_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._interval)
                await self._raw_send(wire.HEARTBEAT_FRAME)
        except (asyncio.CancelledError, ConnectionError, RuntimeError):
            return

    async def _raw_send(self, frame: Frame) -> None:
        writer = self._writer
        if writer is None:
            raise ConnectionError("not connected")
        async with self._write_lock:
            writer.write(wire.encode_frame(frame))
            await writer.drain()

    async def _close_writer(self) -> None:
        if self._writer is None:
            return
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except (ConnectionError, RuntimeError, OSError):
            pass

    async def _send_ack(self, delivery_id: str) -> None:
        await self._raw_send({"op": Op.ACK, "delivery_id": delivery_id})

    async def _send_nack(self, delivery_id: str) -> None:
        await self._raw_send({"op": Op.NACK, "delivery_id": delivery_id})

    async def _send_unsubscribe(self, subscription_id: str) -> None:
        await self._raw_send({"op": Op.UNSUBSCRIBE, "subscription_id": subscription_id})


class _Subscription(Subscriber[MessagePackValue]):
    """Client-side, reconnect-stable delivery stream.

    The consumer holds this object for the life of the subscription; its
    identity does not change across reconnects even though the server-assigned
    ``subscription_id`` does.
    """

    def __init__(
        self,
        client: BrokerClient,
        local_id: str,
        selector: str,
        replay_policy: ReplayPolicy,
    ) -> None:
        self._client = client
        self._local_id = local_id
        self._selector = selector
        self._replay_policy = replay_policy
        self._server_sid: str | None = None
        self._last_acked_ts: float | None = None
        self._queue: asyncio.Queue[Delivery[MessagePackValue]] = asyncio.Queue()
        self._closed = asyncio.Event()

    @property
    def subscription_id(self) -> str:
        if self._server_sid is None:
            raise RuntimeError("subscription not active")
        return self._server_sid

    def _resume_policy(self) -> ReplayPolicy:
        """Where to resume after a reconnect. Replay-capable subscriptions pick
        up just after the last acked message; live-only ones resume future-only
        and do not recover the disconnect gap."""
        if isinstance(self._replay_policy, FromUnixTimestamp):
            if self._last_acked_ts is not None:
                return FromUnixTimestamp(self._last_acked_ts)
            return self._replay_policy
        return FutureOnly()

    async def __anext__(self) -> Delivery[MessagePackValue]:
        if self._closed.is_set() and self._queue.empty():
            raise StopAsyncIteration
        get_task = asyncio.ensure_future(self._queue.get())
        closed_task = asyncio.ensure_future(self._closed.wait())
        try:
            done, pending = await asyncio.wait(
                {get_task, closed_task}, return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.CancelledError:
            get_task.cancel()
            closed_task.cancel()
            raise
        if get_task in done:
            closed_task.cancel()
            return get_task.result()
        for task in pending:
            task.cancel()
        raise StopAsyncIteration

    async def ack(self, delivery: Delivery[MessagePackValue]) -> None:
        ts = delivery.message.created_at
        if self._last_acked_ts is None or ts > self._last_acked_ts:
            self._last_acked_ts = ts
        await self._client._send_ack(delivery.delivery_id)

    async def nack(self, delivery: Delivery[MessagePackValue]) -> None:
        await self._client._send_nack(delivery.delivery_id)

    async def unsubscribe(self) -> None:
        self._client._subs.pop(self._local_id, None)
        if self._server_sid is not None:
            self._client._by_server.pop(self._server_sid, None)
            try:
                await self._client._send_unsubscribe(self._server_sid)
            except ConnectionError:
                pass  # already disconnected; nothing to tear down server-side
        self._closed.set()

    def _push(self, delivery: Delivery[MessagePackValue]) -> None:
        self._queue.put_nowait(delivery)

    def _close(self) -> None:
        self._closed.set()
