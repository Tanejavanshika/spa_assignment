"""Unit tests for UrbanPulse validation and configuration."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import ALL_TOPICS, BUS_GPS_TOPIC, DLQ_TOPIC
from producers.validation import build_dlq_payload, validate_and_build_dlq, validate_bus_payload


class TestValidation:
    """Tests for producer payload validation."""

    def test_valid_bus_payload_returns_none(self) -> None:
        payload = {
            "bus_id": "BUS-1001",
            "route_id": "R101",
            "lat": 19.0,
            "lon": 72.9,
            "speed_kmh": 40.0,
            "timestamp": 1_700_000_000_000,
        }
        assert validate_and_build_dlq(payload, BUS_GPS_TOPIC, "bus") is None

    def test_invalid_latitude_builds_dlq(self) -> None:
        payload = {
            "bus_id": "BUS-1001",
            "route_id": "R101",
            "lat": 95.0,
            "lon": 72.9,
            "speed_kmh": 40.0,
            "timestamp": 1_700_000_000_000,
        }
        dlq = validate_and_build_dlq(payload, BUS_GPS_TOPIC, "bus")
        assert dlq is not None
        assert "Latitude" in dlq["error_reason"]
        assert dlq["source_topic"] == BUS_GPS_TOPIC

    def test_build_dlq_payload_shape(self) -> None:
        original = {"sensor_id": "AQ-1", "aqi": None}
        dlq = build_dlq_payload(original, "Missing aqi", "urbanpulse.air_quality", 12345)
        assert dlq["original_message"] == original
        assert dlq["error_reason"] == "Missing aqi"
        assert dlq["timestamp_failed"] == 12345

    def test_missing_bus_id(self) -> None:
        errors = validate_bus_payload({"route_id": "R101"}, BUS_GPS_TOPIC)
        assert "Missing bus_id" in errors


class TestConstants:
    """Tests for shared configuration constants."""

    def test_all_topics_include_core_streams(self) -> None:
        assert BUS_GPS_TOPIC in ALL_TOPICS
        assert DLQ_TOPIC in ALL_TOPICS

    def test_topic_count(self) -> None:
        assert len(ALL_TOPICS) == 9
