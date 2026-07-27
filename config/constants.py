"""UrbanPulse shared constants.

This module centralizes canonical names used across producers, consumers,
stream processors, and tests so runtime behavior remains consistent.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final


# --------- Project paths ---------
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
DATA_DIR: Final[Path] = PROJECT_ROOT / "data"
SAMPLE_DATA_DIR: Final[Path] = DATA_DIR / "sample_data"
LOG_DIR: Final[Path] = PROJECT_ROOT / "logs"
REPORTS_DIR: Final[Path] = PROJECT_ROOT / "reports"
CHECKPOINTS_DIR: Final[Path] = PROJECT_ROOT / "checkpoints"
SCREENSHOTS_DIR: Final[Path] = PROJECT_ROOT / "screenshots"

ROUTE_SCHEDULE_CSV: Final[Path] = DATA_DIR / "route_schedule.csv"
ZONE_PROFILE_CSV: Final[Path] = DATA_DIR / "zone_profile.csv"
WARD_PARQUET_OUTPUT_DIR: Final[Path] = DATA_DIR / "ward_energy_summary"
DLQ_SUMMARY_CSV: Final[Path] = REPORTS_DIR / "dlq_summary.csv"


# --------- Kafka topics ---------
BUS_GPS_TOPIC: Final[str] = "urbanpulse.bus_gps"
AIR_QUALITY_TOPIC: Final[str] = "urbanpulse.air_quality"
TRAFFIC_SIGNALS_TOPIC: Final[str] = "urbanpulse.traffic_signals"
SMART_METERS_TOPIC: Final[str] = "urbanpulse.smart_meters"
INCIDENTS_TOPIC: Final[str] = "urbanpulse.incidents"
HEALTH_ADVISORIES_TOPIC: Final[str] = "urbanpulse.health_advisories"
WARD_ENERGY_SUMMARY_TOPIC: Final[str] = "urbanpulse.ward_energy_summary"
DLQ_TOPIC: Final[str] = "urbanpulse.dlq"
BUS_GPS_ENRICHED_TOPIC: Final[str] = "urbanpulse.bus_gps_enriched"

ALL_TOPICS: Final[tuple[str, ...]] = (
    BUS_GPS_TOPIC,
    AIR_QUALITY_TOPIC,
    TRAFFIC_SIGNALS_TOPIC,
    SMART_METERS_TOPIC,
    INCIDENTS_TOPIC,
    HEALTH_ADVISORIES_TOPIC,
    WARD_ENERGY_SUMMARY_TOPIC,
    DLQ_TOPIC,
    BUS_GPS_ENRICHED_TOPIC,
)


# --------- Topic and cluster defaults ---------
DEFAULT_PARTITION_COUNT: Final[int] = 3
DEFAULT_REPLICATION_FACTOR: Final[int] = 3
DEFAULT_MIN_INSYNC_REPLICAS: Final[int] = 2

TOPIC_RETENTION_MS: Final[dict[str, int]] = {
    BUS_GPS_TOPIC: 24 * 60 * 60 * 1000,
    BUS_GPS_ENRICHED_TOPIC: 24 * 60 * 60 * 1000,
    TRAFFIC_SIGNALS_TOPIC: 7 * 24 * 60 * 60 * 1000,
    AIR_QUALITY_TOPIC: 90 * 24 * 60 * 60 * 1000,
    SMART_METERS_TOPIC: 365 * 24 * 60 * 60 * 1000,
    INCIDENTS_TOPIC: 30 * 24 * 60 * 60 * 1000,
    HEALTH_ADVISORIES_TOPIC: 14 * 24 * 60 * 60 * 1000,
    WARD_ENERGY_SUMMARY_TOPIC: 30 * 24 * 60 * 60 * 1000,
    DLQ_TOPIC: 14 * 24 * 60 * 60 * 1000,
}

TOPIC_PARTITIONS: Final[dict[str, int]] = {
    BUS_GPS_TOPIC: 6,
    TRAFFIC_SIGNALS_TOPIC: 3,
    AIR_QUALITY_TOPIC: 2,
    SMART_METERS_TOPIC: 4,
    INCIDENTS_TOPIC: 2,
    HEALTH_ADVISORIES_TOPIC: 2,
    WARD_ENERGY_SUMMARY_TOPIC: 3,
    DLQ_TOPIC: 2,
    BUS_GPS_ENRICHED_TOPIC: 2,
}


# --------- Validation rules ---------
MIN_AQI: Final[int] = 0
MAX_AQI: Final[int] = 500
MIN_LATITUDE: Final[float] = -90.0
MAX_LATITUDE: Final[float] = 90.0
MIN_LONGITUDE: Final[float] = -180.0
MAX_LONGITUDE: Final[float] = 180.0
MAX_FUTURE_EVENT_SKEW_MS: Final[int] = 5 * 60 * 1000
MAX_PAST_EVENT_AGE_MS: Final[int] = 365 * 24 * 60 * 60 * 1000


# --------- Flink/Spark thresholds ---------
AQI_EMERGENCY_THRESHOLD: Final[int] = 300
GRIDLOCK_WAIT_THRESHOLD_SEC: Final[int] = 180
GRIDLOCK_CONSECUTIVE_EVENTS: Final[int] = 3
BUS_BUNCHING_DISTANCE_METERS: Final[float] = 200.0
BUS_BUNCHING_DURATION_MS: Final[int] = 5 * 60 * 1000

SPARK_WINDOW_DURATION: Final[str] = "15 minutes"
SPARK_WATERMARK_DURATION: Final[str] = "45 minutes"
SQL_ROLLING_WINDOW_DURATION: Final[str] = "10 minutes"
SQL_ROLLING_SLIDE_DURATION: Final[str] = "1 minute"
AQI_ADVISORY_THRESHOLD: Final[float] = 150.0


# --------- Consumer groups ---------
HIGH_PRIORITY_GROUP_ID: Final[str] = "urbanpulse.high_priority"
STANDARD_PRIORITY_GROUP_ID: Final[str] = "urbanpulse.standard_priority"
ENRICHMENT_GROUP_ID: Final[str] = "urbanpulse.enrichment"
DLQ_GROUP_ID: Final[str] = "urbanpulse.dlq"


@dataclass(frozen=True)
class ProducerTuning:
    """Reusable Kafka producer delivery settings."""

    acks: str = "all"
    enable_idempotence: bool = True
    retries: int = 10
    retry_backoff_ms: int = 100
    delivery_timeout_ms: int = 120_000
    linger_ms: int = 20
    batch_size: int = 16_384
    compression_type: str = "lz4"
    max_in_flight_requests_per_connection: int = 5


DEFAULT_PRODUCER_TUNING: Final[ProducerTuning] = ProducerTuning()
