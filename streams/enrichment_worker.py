"""Stream-table join enrichment worker for bus GPS telemetry."""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from typing import Any

from confluent_kafka import KafkaError

from config.constants import (
    BUS_GPS_ENRICHED_TOPIC,
    BUS_GPS_TOPIC,
    ENRICHMENT_GROUP_ID,
    ROUTE_SCHEDULE_CSV,
)
from config.kafka_config import build_consumer, build_producer
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.enrichment_worker")


def load_route_schedule(csv_path: Path) -> dict[str, dict[str, str]]:
    """Load route schedule CSV as an in-memory KTable for stream-table joins."""
    schedule: dict[str, dict[str, str]] = {}
    if not csv_path.exists():
        logger.error("Static schedule CSV file not found at %s", csv_path)
        sys.exit(1)

    with csv_path.open(mode="r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            schedule[row["route_id"].strip()] = {
                "route_name": row["route_name"].strip(),
                "terminal": row["terminal"].strip(),
                "scheduled_arrival_time": row["scheduled_arrival_time"].strip(),
            }
    logger.info("Loaded %s routes into memory KTable.", len(schedule))
    return schedule


def delivery_report(err: Any, msg: Any) -> None:
    """Log enriched message delivery failures."""
    if err is not None:
        logger.error("Enriched message delivery failed: %s", err)


def main() -> None:
    """Run the GPS enrichment stream-table join."""
    route_ktable = load_route_schedule(ROUTE_SCHEDULE_CSV)
    consumer = build_consumer(ENRICHMENT_GROUP_ID)
    producer = build_producer("gps-enrichment-producer", batch_size=65536, linger_ms=10)

    consumer.subscribe([BUS_GPS_TOPIC])
    logger.info(
        "Subscribed to %s. Processing stream-table join... Press Ctrl+C to stop.",
        BUS_GPS_TOPIC,
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            try:
                gps_data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error("Error parsing GPS message JSON: %s", exc)
                continue

            route_id = gps_data.get("route_id")
            route_meta = route_ktable.get(route_id)
            if route_meta:
                gps_data["route_name"] = route_meta["route_name"]
                gps_data["terminal"] = route_meta["terminal"]
                gps_data["scheduled_arrival_time"] = route_meta["scheduled_arrival_time"]
                gps_data["enrichment_status"] = "SUCCESS"
            else:
                gps_data["route_name"] = "UNKNOWN_ROUTE"
                gps_data["terminal"] = "UNKNOWN"
                gps_data["scheduled_arrival_time"] = "00:00"
                gps_data["enrichment_status"] = "UNMAPPED_ROUTE"
                logger.warning("Route ID '%s' was not found in the KTable; emitting UNKNOWN_ROUTE", route_id)

            producer.produce(
                BUS_GPS_ENRICHED_TOPIC,
                key=msg.key(),
                value=json.dumps(gps_data).encode("utf-8"),
                callback=delivery_report,
            )
            producer.poll(0)
    except KeyboardInterrupt:
        logger.info("Stopping enrichment worker...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
