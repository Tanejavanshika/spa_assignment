"""Kafka client configuration helpers."""

from __future__ import annotations

import os
from typing import Any

from confluent_kafka import Consumer, Producer

from config.constants import DEFAULT_PRODUCER_TUNING

DEFAULT_BOOTSTRAP: str = "localhost:19092,localhost:29092,localhost:39092"


def get_bootstrap_servers() -> str:
    """Return Kafka bootstrap servers from environment or default."""
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP)


def build_producer(client_id: str, **overrides: Any) -> Producer:
    """Create a Kafka producer with UrbanPulse delivery guarantees."""
    tuning = DEFAULT_PRODUCER_TUNING
    conf: dict[str, Any] = {
        "bootstrap.servers": get_bootstrap_servers(),
        "client.id": client_id,
        "acks": tuning.acks,
        "enable.idempotence": tuning.enable_idempotence,
        "retries": tuning.retries,
        "retry.backoff.ms": tuning.retry_backoff_ms,
        "delivery.timeout.ms": tuning.delivery_timeout_ms,
        "linger.ms": tuning.linger_ms,
        "batch.size": tuning.batch_size,
        "compression.type": tuning.compression_type,
        "max.in.flight.requests.per.connection": tuning.max_in_flight_requests_per_connection,
    }
    conf.update(overrides)
    return Producer(conf)


def build_consumer(
    group_id: str,
    *,
    client_id: str | None = None,
    auto_offset_reset: str = "latest",
    enable_auto_commit: bool = True,
    **overrides: Any,
) -> Consumer:
    """Create a Kafka consumer with standard UrbanPulse settings."""
    conf: dict[str, Any] = {
        "bootstrap.servers": get_bootstrap_servers(),
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": enable_auto_commit,
    }
    if client_id:
        conf["client.id"] = client_id
    conf.update(overrides)
    return Consumer(conf)
