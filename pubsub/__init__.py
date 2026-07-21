"""pubsub.py — a small, strongly-typed async pub/sub system.

Top-level re-exports the common public surface: the ``Broker`` engine, client
``Publisher``/``Subscriber`` contracts, and the shared domain types. Deeper
internals (router, retry, ``_asqlite``) stay in their subpackages.
"""

from pubsub.client import Publisher, Subscriber
from pubsub.server import Broker
from pubsub.server.durability import (
    DurabilityBackend,
    InMemoryDurability,
    SQLiteDurability,
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

__all__ = [
    "__version__",
    "Broker",
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
