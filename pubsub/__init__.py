"""pubsub.py — a small, strongly-typed async network pub/sub library.

Wire-first: run a central broker with ``BrokerServer`` and pub/sub against it
from another process with ``BrokerClient`` (or ``pubsub.connect``). The same
``Broker`` engine can also be used embedded, in-process, for single-process
use. Deeper internals (router, retry, ``_asqlite``) stay in their subpackages.
"""

from pubsub.client import Publisher, Subscriber
from pubsub.server import Broker
from pubsub.server.durability import (
    DurabilityBackend,
    InMemoryDurability,
    SQLiteDurability,
)
from pubsub.transport import (
    BrokerClient,
    BrokerServer,
    ClientTransport,
    ProtocolError,
    ServerTransport,
)
from pubsub.shared.types import (
    Delivery,
    DLQEntry,
    FromUnixTimestamp,
    FutureOnly,
    Message,
    MessagePackValue,
    PublishError,
    PublishResult,
    ReplayPolicy,
    Subscription,
    TopicConfig,
)

__version__ = "0.1.0"

# Convenience: ``await pubsub.connect(host, port)`` -> a connected BrokerClient.
connect = BrokerClient.connect

__all__ = [
    "__version__",
    "connect",
    "Broker",
    "BrokerServer",
    "BrokerClient",
    "ClientTransport",
    "ServerTransport",
    "ProtocolError",
    "Publisher",
    "Subscriber",
    "DurabilityBackend",
    "InMemoryDurability",
    "SQLiteDurability",
    "Delivery",
    "DLQEntry",
    "FromUnixTimestamp",
    "FutureOnly",
    "Message",
    "MessagePackValue",
    "PublishError",
    "PublishResult",
    "ReplayPolicy",
    "Subscription",
    "TopicConfig",
]
