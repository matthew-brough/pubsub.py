"""Observability seam: a no-op ``Observer`` the broker calls at its boundaries.

The default ``Observer`` does nothing (zero cost). To emit metrics or traces,
subclass it, override only the hooks you care about, and pass an instance to
``Broker(..., observer=my_observer)``. Nothing in the core imports a telemetry
library — the same stdlib-only-runtime posture as transport/durability.

For an OpenTelemetry-backed implementation see ``pubsub.observability.otel``
(needs the optional ``otel`` extra: ``pip install pubsub-py[otel]``).

Hooks fire synchronously on the broker's event loop, so keep implementations
cheap and non-blocking (counter increments, span events — not I/O).
"""

from __future__ import annotations

__all__ = ["Observer"]


class Observer:
    """Broker lifecycle hooks. Every method is a no-op by default; override the
    subset you want. Arguments are primitives only, so the seam stays stable as
    internal types evolve."""

    def on_publish(
        self, topic: str, *, accepted: bool, reason: str | None = None
    ) -> None:
        """A publish was accepted (durably stored) or rejected. ``reason`` is the
        rejection code when ``accepted`` is False, else ``None``."""

    def on_deliver(self, topic: str, subscription_id: str, *, attempt: int) -> None:
        """A delivery was dispatched to a subscriber queue (attempt >= 1)."""

    def on_ack(self, subscription_id: str) -> None:
        """A delivery was acknowledged."""

    def on_nack(self, subscription_id: str, *, attempt: int) -> None:
        """A delivery was negatively acknowledged (redelivery scheduled)."""

    def on_retry_exhausted(self, subscription_id: str) -> None:
        """Retry budget spent: delivery dead-lettered, subscription evicted."""
