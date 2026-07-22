"""Runnable broker entrypoint — the ``pubsub-server`` console script.

Builds a durability backend, wraps it in a ``Broker``, and serves it over TCP.
This is the turnkey path: ``pubsub-server --port 8765`` stands up a central
broker without hand-writing the ``asyncio.run`` boilerplate.
"""

import argparse
import asyncio
import logging
from collections.abc import Sequence

from pubsub.server.broker import Broker
from pubsub.server.durability.abc import DurabilityBackend
from pubsub.server.durability.memory import InMemoryDurability
from pubsub.server.durability.sqlite import SQLiteDurability
from pubsub.transport.server import DEFAULT_HOST, DEFAULT_PORT, BrokerServer

_log = logging.getLogger("pubsub.transport.runner")


async def _build_durability(kind: str, db_path: str) -> DurabilityBackend:
    if kind == "sqlite":
        return await SQLiteDurability.connect(db_path)
    return InMemoryDurability()


async def serve(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    durability: str = "memory",
    db_path: str = "pubsub.db",
) -> None:
    """Run a broker server until cancelled. Tears down broker + server on exit."""
    backend = await _build_durability(durability, db_path)
    broker = Broker(backend)
    server = BrokerServer(broker, host=host, port=port)
    await server.start()
    _log.info(
        "pubsub broker serving on %s:%d (durability=%s)", host, server.port, durability
    )
    try:
        await server.serve_forever()
    finally:
        await server.close()
        await broker.close()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pubsub-server", description="Run a pubsub broker over TCP."
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--durability", choices=("memory", "sqlite"), default="memory"
    )
    parser.add_argument(
        "--db", default="pubsub.db", help="SQLite path (used when --durability sqlite)"
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)
    try:
        asyncio.run(
            serve(
                host=args.host,
                port=args.port,
                durability=args.durability,
                db_path=args.db,
            )
        )
    except KeyboardInterrupt:
        _log.info("shutting down")


if __name__ == "__main__":
    main()
