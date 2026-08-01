"""Simulated traffic signal telemetry producer with DLQ validation."""

from __future__ import annotations

import json
import random
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import DLQ_TOPIC, TRAFFIC_SIGNALS_TOPIC
from config.logging_config import setup_logging
from producers.base import create_producer, delivery_report
from producers.validation import validate_and_build_dlq

logger = setup_logging("urbanpulse.traffic_producer")


def main() -> None:
    """Publish simulated traffic signal events to Kafka."""
    producer = create_producer("traffic-producer", **{"batch.size": 65536, "linger.ms": 10})
    zones = [f"Zone-{c}" for c in "ABCDEF"]
    phases = ["RED", "GREEN", "YELLOW"]
    junctions = [
        {"junction_id": f"JNC-{100 + i}", "zone": random.choice(zones), "signal_phase": "RED"}
        for i in range(1, 13)
    ]

    logger.info("Starting Traffic Signal producer. Press Ctrl+C to stop.")
    try:
        while True:
            for junction in junctions:
                if random.random() < 0.3:
                    junction["signal_phase"] = random.choice(phases)

                payload: dict[str, Any] = {
                    "junction_id": junction["junction_id"],
                    "zone": junction["zone"],
                    "vehicle_count": random.randint(5, 120),
                    "avg_wait_sec": random.randint(10, 240),
                    "signal_phase": junction["signal_phase"],
                    "timestamp": int(time.time() * 1000),
                }

                dlq_payload = validate_and_build_dlq(payload, TRAFFIC_SIGNALS_TOPIC, "traffic")
                if dlq_payload is not None:
                    producer.produce(
                        DLQ_TOPIC,
                        key=payload["junction_id"].encode("utf-8"),
                        value=json.dumps(dlq_payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                    logger.warning(
                        "[DLQ] Junction %s failed validation: %s",
                        payload["junction_id"],
                        dlq_payload["error_reason"],
                    )
                else:
                    producer.produce(
                        TRAFFIC_SIGNALS_TOPIC,
                        key=payload["junction_id"].encode("utf-8"),
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report,
                    )
                producer.poll(0)
            producer.flush()
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Stopping Traffic producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
