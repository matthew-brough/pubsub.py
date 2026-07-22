"""Shared domain layer: types, clock, and subject rules used by both sides."""

from pubsub.shared.clock import Clock, SystemClock
from pubsub.shared.log import configure_logging, get_logger, make_formatter
from pubsub.shared.topic import (
    TAIL_WILDCARD,
    TOKEN_WILDCARD,
    TopicError,
    matches,
    validate_pattern,
    validate_subject,
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

__all__ = [
    "Clock",
    "SystemClock",
    "configure_logging",
    "get_logger",
    "make_formatter",
    "TopicError",
    "TOKEN_WILDCARD",
    "TAIL_WILDCARD",
    "matches",
    "validate_pattern",
    "validate_subject",
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
