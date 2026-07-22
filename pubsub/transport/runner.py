"""Runnable broker entrypoint — the ``pubsub-server`` console script.

Builds a durability backend, wraps it in a ``Broker``, and serves it over TCP.
This is the turnkey path: ``pubsub-server --port 8765`` stands up a central
broker without hand-writing the ``asyncio.run`` boilerplate.
"""

import argparse
import asyncio
import logging
from collections.abc import Callable, Sequence

from pubsub.shared.log import configure_logging

from pubsub.server.broker import Broker
from pubsub.server.durability.abc import DurabilityBackend
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.durability.null import NullDurability
from pubsub.server._asqlite import DEFAULT_SYNCHRONOUS, _SYNCHRONOUS_MODES
from pubsub.server.durability.sqlite import SQLiteDurability
from pubsub.server.log import (
    DurableHandler,
    InMemoryDurableHandler,
    SQLiteDurableHandler,
)
from pubsub.transport.server import DEFAULT_HOST, DEFAULT_PORT, BrokerServer

_log = logging.getLogger("pubsub.transport.runner")

# Buffered durable-log records are force-written this often so a crash loses at
# most one interval of audit history rather than everything since startup.
_LOG_FLUSH_INTERVAL = 5.0


async def _build_durability(kind: str, db_path: str, synchronous: str = DEFAULT_SYNCHRONOUS) -> DurabilityBackend:
    if kind == "sqlite":
        return await SQLiteDurability.connect(db_path, synchronous=synchronous)
    if kind in ("none", "null", "off"):
        return NullDurability()
    return InMemoryDurability()


def _build_log_handler(kind: str, db_path: str) -> DurableHandler:
    # SQLite deployments persist the audit log alongside messages (own ``logs``
    # table); memory deployments keep it in-process.
    if kind == "sqlite":
        return SQLiteDurableHandler(db_path)
    return InMemoryDurableHandler()


def _loop_factory() -> tuple[str, Callable[[], asyncio.AbstractEventLoop] | None]:
    """Pick the event loop for ``asyncio.run``. uvloop if the ``fast`` extra is
    installed (the single broker loop is the throughput wall, so a faster loop is
    a free win), else stdlib asyncio via ``None``. Returned as a ``loop_factory``
    rather than a global policy — the policy API is deprecated in 3.14. Fallback
    keeps the core portable (uvloop has no Windows wheels)."""
    try:
        import uvloop
    except ImportError:
        return "asyncio", None
    return "uvloop", uvloop.new_event_loop


async def _flush_loop(handler: DurableHandler) -> None:
    while True:
        await asyncio.sleep(_LOG_FLUSH_INTERVAL)
        handler.flush_to_storage()


async def serve(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    durability: str = "memory",
    db_path: str = "pubsub.db",
    synchronous: str = DEFAULT_SYNCHRONOUS,
    log_handler: DurableHandler | None = None,
) -> None:
    """Run a broker server until cancelled. Tears down broker + server on exit."""
    backend = await _build_durability(durability, db_path, synchronous)
    broker = Broker(backend)
    server = BrokerServer(broker, host=host, port=port)
    await server.start()
    flusher = asyncio.create_task(_flush_loop(log_handler)) if log_handler is not None else None
    _log.info("pubsub broker serving on %s:%d (durability=%s)", host, server.port, durability)
    try:
        await server.serve_forever()
    finally:
        if flusher is not None:
            flusher.cancel()
        await server.close()
        await broker.close()
        if log_handler is not None:
            # Final flush captures records buffered since the last tick.
            log_handler.flush_to_storage()
            log_handler.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="pubsub-server", description="Run a pubsub broker over TCP.")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--durability",
        choices=("memory", "sqlite", "none"),
        default="memory",
        help="none = accept-and-drop (at-most-once, no replay, no history growth)",
    )
    parser.add_argument("--db", default="pubsub.db", help="SQLite path (used when --durability sqlite)")
    parser.add_argument(
        "--sqlite-sync",
        choices=_SYNCHRONOUS_MODES,
        default=DEFAULT_SYNCHRONOUS,
        help="SQLite WAL synchronous mode: NORMAL (fast, crash-safe) or FULL "
        "(strict last-transaction durability). Default NORMAL.",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    log_handler = _build_log_handler(args.durability, args.db)
    configure_logging(level=logging.DEBUG if args.verbose else logging.INFO, durable=log_handler)
    loop_name, loop_factory = _loop_factory()
    _log.info("event loop: %s", loop_name)
    try:
        asyncio.run(
            serve(
                host=args.host,
                port=args.port,
                durability=args.durability,
                db_path=args.db,
                synchronous=args.sqlite_sync,
                log_handler=log_handler,
            ),
            loop_factory=loop_factory,
        )
    except KeyboardInterrupt:
        _log.info("shutting down")


if __name__ == "__main__":
    main()
