"""Shared producer utilities for UrbanPulse telemetry simulators."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Producer

from config.kafka_config import build_producer

logger = logging.getLogger(__name__)


def delivery_report(err: Any, msg: Any) -> None:
    """Log Kafka delivery failures."""
    if err is not None:
        logger.error("Message delivery failed: %s", err)


def create_producer(client_id: str, **overrides: Any) -> Producer:
    """Build a producer with UrbanPulse defaults and optional overrides."""
    return build_producer(client_id, **overrides)
