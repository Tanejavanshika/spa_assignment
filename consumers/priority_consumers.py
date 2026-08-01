"""Priority consumer groups demonstrating real-time vs analytics workloads."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from confluent_kafka import KafkaError, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import (
    HIGH_PRIORITY_GROUP_ID,
    STANDARD_PRIORITY_GROUP_ID,
    TRAFFIC_SIGNALS_TOPIC,
)
from config.kafka_config import build_consumer
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.priority_consumers")


def main() -> None:
    """Run high-priority or standard-priority traffic signal consumers."""
    parser = argparse.ArgumentParser(description="UrbanPulse Priority Consumers")
    parser.add_argument(
        "--mode",
        choices=["high", "standard"],
        required=True,
        help="Consumer mode: 'high' (real-time signal control) or 'standard' (slow analytics)",
    )
    parser.add_argument("--id", type=int, default=1, help="Consumer ID for standard priority instances")
    args = parser.parse_args()

    if args.mode == "high":
        group_id = HIGH_PRIORITY_GROUP_ID
        client_id = "high-priority-consumer"
        delay_sec = 0.0
        logger.info("[HIGH_PRIORITY] Starting real-time signal control consumer...")
    else:
        group_id = STANDARD_PRIORITY_GROUP_ID
        client_id = f"standard-priority-consumer-{args.id}"
        delay_sec = 0.5
        logger.info("[STANDARD_PRIORITY-%s] Starting analytics consumer...", args.id)

    consumer = build_consumer(group_id, client_id=client_id, auto_commit_interval_ms=1000)
    consumer.subscribe([TRAFFIC_SIGNALS_TOPIC])

    msg_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("[%s] Error: %s", group_id, msg.error())
                continue

            if delay_sec > 0:
                time.sleep(delay_sec)

            msg_count += 1
            should_print = (msg_count % 100 == 0) if args.mode == "high" else (msg_count % 5 == 0)
            if should_print:
                tp = TopicPartition(msg.topic(), msg.partition())
                try:
                    _, high_watermark = consumer.get_watermark_offsets(tp, timeout=0.5)
                    lag = max(0, high_watermark - msg.offset() - 1)
                except Exception:
                    lag = "Unknown"
                logger.info(
                    "[%s] Instance %s | Partition %s | Msg Offset: %s | Lag: %s",
                    group_id,
                    client_id,
                    msg.partition(),
                    msg.offset(),
                    lag,
                )
    except KeyboardInterrupt:
        logger.info("[%s] Stopping consumer...", group_id)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
