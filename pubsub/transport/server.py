"""TCP server: adapts a ``Broker`` to the wire protocol.

One ``_Connection`` per accepted socket. A connection reads client frames in a
loop and dispatches them onto the shared broker; each subscription runs a pump
task that drains its ``Subscriber`` stream and pushes ``delivery`` frames back.
All writes on a connection are serialised through a lock so the heartbeat task,
the pump tasks, and request replies never interleave on the stream.

Heartbeat: the server sends a ``heartbeat`` frame every ``heartbeat_interval``
seconds and drops the connection if no client frame arrives within
``heartbeat_timeout`` — app-level liveness that catches stalls TCP keepalive
would miss. A dropped connection's subscriptions are torn down, consistent with
slow-subscriber eviction (the subscription id itself is broker-owned and could
be resumed on reconnect).
"""

import asyncio
import logging
from typing import cast

from pubsub.client.subscriber import Subscriber
from pubsub.server.broker import Broker
from pubsub.shared.types import Delivery, MessagePackValue
from pubsub.transport import wire
from pubsub.transport.abc import ServerTransport
from pubsub.transport.wire import Frame, Op, ProtocolError

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_HEARTBEAT_INTERVAL = 15.0
DEFAULT_HEARTBEAT_TIMEOUT = 45.0


class BrokerServer(ServerTransport):
    def __init__(
        self,
        broker: Broker,
        *,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        heartbeat_interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        heartbeat_timeout: float = DEFAULT_HEARTBEAT_TIMEOUT,
    ) -> None:
        self._broker = broker
        self._host = host
        self._port = port
        self._interval = heartbeat_interval
        self._timeout = heartbeat_timeout
        self._server: asyncio.Server | None = None
        self._conns: set["_Connection"] = set()
        self._log = logging.getLogger("pubsub.transport.server")

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_connect, self._host, self._port
        )

    @property
    def port(self) -> int:
        """The bound port (useful when constructed with ``port=0``)."""
        if self._server is None:
            raise RuntimeError("server not started")
        return self._server.sockets[0].getsockname()[1]

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        # Stop accepting, then actively drop live connections; otherwise
        # ``wait_closed`` blocks on still-connected clients.
        for conn in list(self._conns):
            conn.abort()
        await self._server.wait_closed()

    async def _on_connect(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        conn = _Connection(
            self._broker, reader, writer, self._interval, self._timeout, self._log
        )
        self._conns.add(conn)
        try:
            await conn.run()
        finally:
            self._conns.discard(conn)


class _Connection:
    def __init__(
        self,
        broker: Broker,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        interval: float,
        timeout: float,
        log: logging.Logger,
    ) -> None:
        self._broker = broker
        self._reader = reader
        self._writer = writer
        self._interval = interval
        self._timeout = timeout
        self._log = log
        self._loop = asyncio.get_running_loop()
        self._write_lock = asyncio.Lock()
        self._last_recv = self._loop.time()
        # subscription_id -> (stream, pump task)
        self._subs: dict[str, tuple[Subscriber[MessagePackValue], asyncio.Task[None]]] = {}
        # delivery_id -> live Delivery, so ack/nack can be routed by id alone.
        self._inflight: dict[str, Delivery[MessagePackValue]] = {}
        self._reader_task: asyncio.Task[None] | None = None
        self._hb_task: asyncio.Task[None] | None = None

    async def run(self) -> None:
        try:
            if not await self._handshake():
                await self._aclose_writer()  # rejected: close our side too
                return
        except (asyncio.IncompleteReadError, ConnectionError, ProtocolError):
            await self._aclose_writer()
            return

        self._reader_task = asyncio.create_task(self._reader_loop())
        self._hb_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._reader_task
        except asyncio.CancelledError:
            pass  # heartbeat timeout cancelled the read loop
        finally:
            await self._teardown()

    def abort(self) -> None:
        """Force this connection down (server shutdown). Cancelling the read
        loop unwinds ``run`` into teardown; if the handshake hasn't finished
        yet, close the socket directly."""
        if self._reader_task is not None:
            self._reader_task.cancel()
        else:
            self._writer.close()

    async def _handshake(self) -> bool:
        frame = await wire.read_frame(self._reader)
        if frame.get("op") != Op.HELLO:
            await self._send_error("expected hello")
            return False
        if frame.get("version") != wire.PROTOCOL_VERSION:
            await self._send_error(
                f"unsupported protocol version {frame.get('version')!r}"
            )
            return False
        await self._send({"op": Op.WELCOME, "version": wire.PROTOCOL_VERSION})
        return True

    async def _reader_loop(self) -> None:
        while True:
            try:
                frame = await wire.read_frame(self._reader)
            except (asyncio.IncompleteReadError, ConnectionError):
                return  # client hung up
            except ProtocolError as exc:
                await self._send_error(str(exc))
                return
            self._last_recv = self._loop.time()
            await self._dispatch(frame)

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            if self._loop.time() - self._last_recv > self._timeout:
                # Client silent past the deadline: drop it. Cancelling the read
                # loop unwinds run() into teardown.
                if self._reader_task is not None:
                    self._reader_task.cancel()
                return
            try:
                await self._send(wire.HEARTBEAT_FRAME)
            except (ConnectionError, RuntimeError):
                return

    async def _dispatch(self, frame: Frame) -> None:
        op = frame.get("op")
        if op == Op.REGISTER:
            await self._on_register(frame)
        elif op == Op.PUBLISH:
            await self._on_publish(frame)
        elif op == Op.SUBSCRIBE:
            await self._on_subscribe(frame)
        elif op == Op.ACK:
            await self._on_ack(frame)
        elif op == Op.NACK:
            await self._on_nack(frame)
        elif op == Op.UNSUBSCRIBE:
            await self._on_unsubscribe(frame)
        elif op == Op.HEARTBEAT:
            pass  # liveness already recorded via _last_recv
        else:
            await self._send_error(f"unknown op {op!r}")

    async def _on_register(self, frame: Frame) -> None:
        rid = cast(int, frame.get("rid"))
        try:
            await self._broker.register_topic(
                cast(str, frame["topic"]), replayable=bool(frame.get("replayable"))
            )
        except Exception as exc:  # noqa: BLE001 - surface as a wire error
            await self._send_error(f"register failed: {exc}", rid=rid)
            return
        await self._send({"op": Op.REGISTER_OK, "rid": rid})

    async def _on_publish(self, frame: Frame) -> None:
        rid = cast(int, frame.get("rid"))
        extras = cast("dict[str, MessagePackValue]", frame.get("extras") or {})
        result = await self._broker.publish(
            cast(str, frame["topic"]), frame.get("payload"), extras=extras
        )
        await self._send(wire.publish_result_to_wire(rid, result))

    async def _on_subscribe(self, frame: Frame) -> None:
        rid = cast(int, frame.get("rid"))
        selector = cast(str, frame["selector"])
        replay = wire.replay_from_wire(
            cast("dict[str, MessagePackValue]", frame.get("replay") or {"mode": "future"})
        )
        try:
            stream = await self._broker.subscribe(selector, replay)
        except Exception as exc:  # bad selector, etc.
            await self._send_error(f"subscribe failed: {exc}", rid=rid)
            return
        sid = stream.subscription_id
        pump = asyncio.create_task(self._pump(sid, stream))
        self._subs[sid] = (stream, pump)
        await self._send({"op": Op.SUBSCRIBE_OK, "rid": rid, "subscription_id": sid})

    async def _pump(self, sid: str, stream: Subscriber[MessagePackValue]) -> None:
        try:
            async for delivery in stream:
                self._inflight[delivery.delivery_id] = delivery
                await self._send(wire.delivery_to_wire(delivery))
        except (ConnectionError, RuntimeError):
            return  # connection died; teardown handles cleanup
        # Reached only on a natural stream end (broker-side close, e.g.
        # slow-subscriber eviction). A client-driven unsubscribe cancels this
        # task instead, so it never sends a spurious sub_closed.
        self._subs.pop(sid, None)
        await self._send({"op": Op.SUB_CLOSED, "subscription_id": sid, "reason": "closed"})

    async def _on_ack(self, frame: Frame) -> None:
        delivery = self._inflight.pop(cast(str, frame.get("delivery_id")), None)
        if delivery is None:
            return
        entry = self._subs.get(delivery.subscription_id)
        if entry is not None:
            await entry[0].ack(delivery)

    async def _on_nack(self, frame: Frame) -> None:
        delivery = self._inflight.pop(cast(str, frame.get("delivery_id")), None)
        if delivery is None:
            return
        entry = self._subs.get(delivery.subscription_id)
        if entry is not None:
            await entry[0].nack(delivery)

    async def _on_unsubscribe(self, frame: Frame) -> None:
        sid = cast(str, frame.get("subscription_id"))
        entry = self._subs.pop(sid, None)
        if entry is None:
            return
        stream, pump = entry
        pump.cancel()
        await stream.unsubscribe()

    # --- writes ----------------------------------------------------------
    async def _send(self, frame: Frame) -> None:
        async with self._write_lock:
            self._writer.write(wire.encode_frame(frame))
            await self._writer.drain()

    async def _send_error(self, detail: str, *, rid: int | None = None) -> None:
        frame: Frame = {"op": Op.ERROR, "detail": detail}
        if rid is not None:
            frame["rid"] = rid
        try:
            await self._send(frame)
        except (ConnectionError, RuntimeError):
            pass

    async def _teardown(self) -> None:
        if self._hb_task is not None:
            self._hb_task.cancel()
            await _drain(self._hb_task)
        for sid, (stream, pump) in list(self._subs.items()):
            pump.cancel()
            await _drain(pump)
            try:
                await stream.unsubscribe()
            except Exception:  # noqa: BLE001 - best-effort teardown
                self._log.debug("unsubscribe failed during teardown for %s", sid)
        self._subs.clear()
        self._inflight.clear()
        await self._aclose_writer()

    async def _aclose_writer(self) -> None:
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except (ConnectionError, RuntimeError):
            pass


async def _drain(task: asyncio.Task[None]) -> None:
    """Await a just-cancelled task, swallowing the cancellation, so no
    'Task was destroyed but it is pending' warning leaks."""
    try:
        await task
    except asyncio.CancelledError:
        pass
