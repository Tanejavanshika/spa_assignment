"""Simulated air quality telemetry producer with DLQ validation."""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import AIR_QUALITY_TOPIC, DLQ_TOPIC
from config.logging_config import setup_logging
from producers.base import create_producer, delivery_report
from producers.validation import validate_and_build_dlq

logger = setup_logging("urbanpulse.aq_producer")


def main() -> None:
    """Publish simulated air quality events to Kafka."""
    producer = create_producer("air-quality-producer")
    zones = [f"Zone-{c}" for c in "ABCDEF"]
    sensors = [
        {
            "sensor_id": f"AQ-SEN-{100 + i}",
            "zone": random.choice(zones),
            "pm25_base": random.uniform(20, 150),
            "pm10_base": random.uniform(30, 200),
            "no2_base": random.uniform(10, 80),
        }
        for i in range(1, 11)
    ]

    logger.info("Starting Air Quality producer. Press Ctrl+C to stop.")
    try:
        while True:
            for sensor in sensors:
                pm25 = max(5, sensor["pm25_base"] + random.uniform(-10, 10))
                pm10 = max(10, sensor["pm10_base"] + random.uniform(-15, 15))
                no2 = max(5, sensor["no2_base"] + random.uniform(-5, 5))
                aqi = int(max(pm25 * 2.1, pm10 * 1.2, no2 * 1.5))
                is_failed = random.random() < 0.05

                payload: dict[str, Any] = {
                    "sensor_id": sensor["sensor_id"],
                    "zone": sensor["zone"],
                    "pm25": round(pm25, 2),
                    "pm10": round(pm10, 2),
                    "no2": round(no2, 2),
                    "aqi": aqi if not is_failed else None,
                    "timestamp": int(time.time() * 1000),
                }

                dlq_payload = validate_and_build_dlq(payload, AIR_QUALITY_TOPIC, "aqi")
                if dlq_payload is not None:
                    producer.produce(
                        DLQ_TOPIC,
                        key=payload["sensor_id"].encode("utf-8"),
                        value=json.dumps(dlq_payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                    logger.warning(
                        "[DLQ] Sensor %s failed validation: %s",
                        payload["sensor_id"],
                        dlq_payload["error_reason"],
                    )
                else:
                    producer.produce(
                        AIR_QUALITY_TOPIC,
                        key=payload["sensor_id"].encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                producer.poll(0)
            producer.flush()
            time.sleep(3.0)
    except KeyboardInterrupt:
        logger.info("Stopping Air Quality producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
