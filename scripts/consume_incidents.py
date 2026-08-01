#!/usr/bin/env python3
"""Consume a small sample from urbanpulse.incidents and save as JSONL for evidence."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, KafkaError

from config.constants import INCIDENTS_TOPIC, REPORTS_DIR
from config.kafka_config import get_bootstrap_servers


def run(bootstrap: str, topic: str, count: int, timeout: int, outpath: Path) -> None:
    """Collect incident samples and write them to a JSONL file."""
    outpath.parent.mkdir(parents=True, exist_ok=True)
    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": "consume-incidents-sample",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])

    samples: list[Any] = []
    start = time.time()
    try:
        while len(samples) < count and (time.time() - start) < timeout:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("Consumer error:", msg.error())
                continue
            try:
                payload = msg.value().decode("utf-8")
                obj = json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError):
                obj = {"raw": msg.value().decode("utf-8", errors="replace")}
            samples.append(obj)
    finally:
        consumer.close()

    if samples:
        with outpath.open("w", encoding="utf-8") as handle:
            for sample in samples:
                handle.write(json.dumps(sample))
                handle.write("\n")
        print(f"Wrote {len(samples)} incident samples to {outpath}")
    else:
        print("No incident messages received in the allotted time.")


def cli() -> None:
    """Parse CLI arguments and run the incident sampler."""
    parser = argparse.ArgumentParser(description="Sample Flink incident alerts from Kafka.")
    parser.add_argument("--bootstrap", default=get_bootstrap_servers(), help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=INCIDENTS_TOPIC)
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=20, help="seconds to wait")
    parser.add_argument("--out", default=str(REPORTS_DIR / "incident_samples.jsonl"))
    args = parser.parse_args()
    run(args.bootstrap, args.topic, args.count, args.timeout, Path(args.out))


if __name__ == "__main__":
    cli()
