"""Simulated smart meter telemetry producer with DLQ validation."""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import DLQ_TOPIC, SMART_METERS_TOPIC
from config.logging_config import setup_logging
from producers.base import create_producer, delivery_report
from producers.validation import validate_and_build_dlq

logger = setup_logging("urbanpulse.meter_producer")


def main() -> None:
    """Publish simulated smart meter events to Kafka."""
    producer = create_producer("meter-producer", **{"batch.size": 65536, "linger.ms": 10})
    wards = [f"Ward-{i}" for i in range(1, 7)]
    meters = [
        {
            "meter_id": f"MET-{1000 + i}",
            "ward_id": random.choice(wards),
            "kwh_reading": random.uniform(100.0, 1000.0),
            "voltage": 230.0,
            "power_factor": 0.95,
        }
        for i in range(1, 31)
    ]

    logger.info("Starting Smart Meter producer. Press Ctrl+C to stop.")
    try:
        while True:
            for meter in meters:
                meter["kwh_reading"] += random.uniform(0.01, 0.15)
                voltage = random.uniform(210.0, 245.0)
                power_factor = random.uniform(0.85, 0.99)
                inject_error = random.random() < 0.02

                payload: dict[str, Any] = {
                    "meter_id": meter["meter_id"],
                    "ward_id": meter["ward_id"],
                    "kwh_reading": meter["kwh_reading"] if not inject_error else -10.0,
                    "voltage": voltage if not inject_error else 300.0,
                    "power_factor": power_factor,
                    "timestamp": int(time.time() * 1000),
                }

                dlq_payload = validate_and_build_dlq(payload, SMART_METERS_TOPIC, "meter")
                if dlq_payload is not None:
                    producer.produce(
                        DLQ_TOPIC,
                        key=payload["meter_id"].encode("utf-8"),
                        value=json.dumps(dlq_payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                    logger.warning(
                        "[DLQ] Meter %s failed validation: %s",
                        payload["meter_id"],
                        dlq_payload["error_reason"],
                    )
                else:
                    producer.produce(
                        SMART_METERS_TOPIC,
                        key=payload["meter_id"].encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                producer.poll(0)
            producer.flush()
            time.sleep(2.0)
    except KeyboardInterrupt:
        logger.info("Stopping Meter producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
