"""Simulated bus GPS telemetry producer with DLQ validation."""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import BUS_GPS_TOPIC, DLQ_TOPIC
from config.logging_config import setup_logging
from producers.base import create_producer, delivery_report
from producers.validation import validate_and_build_dlq

logger = setup_logging("urbanpulse.bus_gps_producer")


def main() -> None:
    """Publish simulated bus GPS events to Kafka."""
    producer = create_producer("bus-gps-producer", **{"batch.size": 65536, "linger.ms": 10})
    routes = [f"R{101 + i}" for i in range(8)]
    buses = [
        {
            "bus_id": f"BUS-{1000 + i:04d}",
            "route_id": random.choice(routes),
            "lat": random.uniform(18.9, 19.2),
            "lon": random.uniform(72.8, 73.0),
            "speed_kmh": random.uniform(10, 60),
            "occupancy_pct": random.randint(10, 95),
        }
        for i in range(1, 51)
    ]

    logger.info("Starting Bus GPS producer. Press Ctrl+C to stop.")
    try:
        while True:
            for bus in buses:
                bus["lat"] += random.uniform(-0.001, 0.001)
                bus["lon"] += random.uniform(-0.001, 0.001)
                bus["speed_kmh"] = max(0, min(100, bus["speed_kmh"] + random.uniform(-5, 5)))
                bus["occupancy_pct"] = max(0, min(100, bus["occupancy_pct"] + random.randint(-5, 5)))

                inject_error = random.random() < 0.02
                payload: dict[str, Any] = {
                    "bus_id": bus["bus_id"],
                    "route_id": bus["route_id"],
                    "lat": bus["lat"] if not inject_error else 95.0,
                    "lon": bus["lon"] if not inject_error else 185.0,
                    "speed_kmh": bus["speed_kmh"] if not inject_error else 150.0,
                    "occupancy_pct": bus["occupancy_pct"],
                    "timestamp": int(time.time() * 1000),
                }

                dlq_payload = validate_and_build_dlq(payload, BUS_GPS_TOPIC, "bus")
                if dlq_payload is not None:
                    producer.produce(
                        DLQ_TOPIC,
                        key=payload["bus_id"].encode("utf-8"),
                        value=json.dumps(dlq_payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                    logger.warning(
                        "[DLQ] Bus %s validation failed: %s",
                        payload["bus_id"],
                        dlq_payload["error_reason"],
                    )
                else:
                    producer.produce(
                        BUS_GPS_TOPIC,
                        key=payload["route_id"].encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                producer.poll(0)
            producer.flush()
            time.sleep(2.0)
    except KeyboardInterrupt:
        logger.info("Stopping Bus GPS producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
