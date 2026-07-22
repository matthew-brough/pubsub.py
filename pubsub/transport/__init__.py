"""Network transport: TCP wire protocol adapting the broker to a stream.

Transport is a swappable layer (see ``abc``): the core broker never imports
sockets. ``BrokerServer`` serves a ``Broker`` over TCP; ``BrokerClient`` re-
exposes the broker surface to a remote process. Wire framing and the frame
vocabulary live in ``wire``.
"""

from pubsub.transport.abc import ClientTransport, ServerTransport
from pubsub.transport.client import BrokerClient
from pubsub.transport.server import BrokerServer
from pubsub.transport.wire import Op, ProtocolError

__all__ = [
    "BrokerClient",
    "BrokerServer",
    "ClientTransport",
    "ServerTransport",
    "Op",
    "ProtocolError",
]
