"""OpenTelemetry adapter for the broker observability seam.

Requires the optional ``otel`` extra::

    pip install pubsub-py[otel]

Maps each broker hook onto an OpenTelemetry metric counter. Only the metrics
*API* is imported; wiring an SDK/exporter is the application's job, exactly as
OTel intends. Import this module only when the extra is installed — the core
never imports it.

Usage::

    from pubsub import Broker
    from pubsub.observability.otel import OTelObserver

    broker = Broker(durability, observer=OTelObserver())
"""

from __future__ import annotations

try:
    from opentelemetry import metrics
except ImportError as exc:  # pragma: no cover - exercised only without the extra
    raise ImportError(
        "OTelObserver requires the 'otel' extra: pip install pubsub-py[otel]"
    ) from exc

from pubsub.observability import Observer


class OTelObserver(Observer):
    """Records broker events as OpenTelemetry counters.

    Pass a specific ``Meter`` to control instrumentation scope, or let it fall
    back to a meter named ``pubsub``.
    """

    def __init__(self, meter: metrics.Meter | None = None) -> None:
        m = meter or metrics.get_meter("pubsub")
        self._publishes = m.create_counter(
            "pubsub.publishes", unit="1", description="Publishes by outcome"
        )
        self._deliveries = m.create_counter(
            "pubsub.deliveries", unit="1", description="Deliveries dispatched"
        )
        self._acks = m.create_counter(
            "pubsub.acks", unit="1", description="Deliveries acknowledged"
        )
        self._nacks = m.create_counter(
            "pubsub.nacks", unit="1", description="Deliveries negatively acknowledged"
        )
        self._exhausted = m.create_counter(
            "pubsub.retry_exhausted",
            unit="1",
            description="Deliveries dead-lettered after retry exhaustion",
        )

    def on_publish(
        self, topic: str, *, accepted: bool, reason: str | None = None
    ) -> None:
        attrs: dict[str, str | bool] = {"topic": topic, "accepted": accepted}
        if reason is not None:
            attrs["reason"] = reason
        self._publishes.add(1, attrs)

    def on_deliver(self, topic: str, subscription_id: str, *, attempt: int) -> None:
        self._deliveries.add(
            1, {"topic": topic, "subscription_id": subscription_id, "attempt": attempt}
        )

    def on_ack(self, subscription_id: str) -> None:
        self._acks.add(1, {"subscription_id": subscription_id})

    def on_nack(self, subscription_id: str, *, attempt: int) -> None:
        self._nacks.add(1, {"subscription_id": subscription_id, "attempt": attempt})

    def on_retry_exhausted(self, subscription_id: str) -> None:
        self._exhausted.add(1, {"subscription_id": subscription_id})
