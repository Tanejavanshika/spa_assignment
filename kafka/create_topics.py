"""Create and configure UrbanPulse Kafka topics."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka.admin import AdminClient, NewTopic

from config.constants import (
    DEFAULT_MIN_INSYNC_REPLICAS,
    DEFAULT_REPLICATION_FACTOR,
    TOPIC_PARTITIONS,
    TOPIC_RETENTION_MS,
)
from config.kafka_config import get_bootstrap_servers
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.create_topics")

TOPIC_REASONS: dict[str, str] = {
    "urbanpulse.bus_gps": "24-hour replay window for incident investigations and bus operations.",
    "urbanpulse.traffic_signals": "7-day operational window for signal control analysis.",
    "urbanpulse.air_quality": "90-day environmental trend analysis.",
    "urbanpulse.smart_meters": "365-day regulatory and billing retention.",
    "urbanpulse.incidents": "30-day incident archive for emergency review.",
    "urbanpulse.health_advisories": "14-day advisory retention for public health operations.",
    "urbanpulse.ward_energy_summary": "30-day ward-level analytics retention.",
    "urbanpulse.dlq": "14-day troubleshooting window for failed messages.",
    "urbanpulse.bus_gps_enriched": "24-hour enrichment output for downstream routing and ETA workflows.",
}


def create_urbanpulse_topics() -> None:
    """Create all UrbanPulse topics with assignment-specified retention and partitions."""
    admin_client = AdminClient({"bootstrap.servers": get_bootstrap_servers()})
    new_topics: list[NewTopic] = []

    logger.info("Preparing topic creation requests...")
    for topic, partitions in TOPIC_PARTITIONS.items():
        retention_ms = TOPIC_RETENTION_MS[topic]
        reason = TOPIC_REASONS.get(topic, "UrbanPulse operational topic.")
        logger.info(
            " - %s: Partitions=%s, Retention=%s ms (%s)",
            topic,
            partitions,
            retention_ms,
            reason,
        )
        new_topics.append(
            NewTopic(
                topic=topic,
                num_partitions=partitions,
                replication_factor=DEFAULT_REPLICATION_FACTOR,
                config={
                    "retention.ms": str(retention_ms),
                    "min.insync.replicas": str(DEFAULT_MIN_INSYNC_REPLICAS),
                },
            )
        )

    futures = admin_client.create_topics(new_topics)
    success = True
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Topic '%s' created successfully.", topic)
        except Exception as exc:
            if "TopicAlreadyExistsException" in str(exc) or "already exists" in str(exc).lower():
                logger.info("Topic '%s' already exists.", topic)
            else:
                logger.error("Failed to create topic '%s': %s", topic, exc)
                success = False

    if success:
        logger.info("All topics verified and configured successfully!")
    else:
        logger.error("Some topic configurations failed. Please inspect logs.")
        sys.exit(1)


if __name__ == "__main__":
    create_urbanpulse_topics()
