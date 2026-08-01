"""Payload validation and DLQ helpers for UrbanPulse producers."""

from __future__ import annotations

import time
from typing import Any

from config.constants import (
    MAX_AQI,
    MAX_LATITUDE,
    MAX_LONGITUDE,
    MIN_AQI,
    MIN_LATITUDE,
    MIN_LONGITUDE,
)


def build_dlq_payload(
    original_message: dict[str, Any],
    reason: str,
    source_topic: str,
    timestamp_ms: int | None = None,
) -> dict[str, Any]:
    """Create a DLQ payload matching the assignment rubric."""
    return {
        "original_message": original_message,
        "error_reason": reason,
        "timestamp_failed": int(timestamp_ms or time.time() * 1000),
        "source_topic": source_topic,
    }


def validate_aqi_payload(payload: dict[str, Any], source_topic: str) -> list[str]:
    """Validate air quality sensor payloads."""
    errors: list[str] = []
    if payload.get("sensor_id") in (None, ""):
        errors.append("Missing sensor_id")
    if payload.get("zone") in (None, ""):
        errors.append("Missing zone")
    if payload.get("timestamp") is None:
        errors.append("Missing timestamp")
    if payload.get("aqi") is None:
        errors.append("AQI reading is null/missing due to sensor timeout")
    elif not (MIN_AQI <= int(payload["aqi"]) <= MAX_AQI):
        errors.append(f"AQI reading {payload['aqi']} is out of range [{MIN_AQI}, {MAX_AQI}]")
    return errors


def validate_bus_payload(payload: dict[str, Any], source_topic: str) -> list[str]:
    """Validate bus GPS payloads."""
    errors: list[str] = []
    if payload.get("bus_id") in (None, ""):
        errors.append("Missing bus_id")
    if payload.get("route_id") in (None, ""):
        errors.append("Missing route_id")
    if payload.get("timestamp") is None:
        errors.append("Missing timestamp")
    lat = payload.get("lat")
    lon = payload.get("lon")
    speed = payload.get("speed_kmh")
    if not isinstance(lat, (int, float)) or not (MIN_LATITUDE <= float(lat) <= MAX_LATITUDE):
        errors.append(f"Latitude {lat} out of bounds [{MIN_LATITUDE}, {MAX_LATITUDE}]")
    if not isinstance(lon, (int, float)) or not (MIN_LONGITUDE <= float(lon) <= MAX_LONGITUDE):
        errors.append(f"Longitude {lon} out of bounds [{MIN_LONGITUDE}, {MAX_LONGITUDE}]")
    if not isinstance(speed, (int, float)) or not (0.0 <= float(speed) <= 120.0):
        errors.append(f"Speed {speed} km/h is impossible")
    return errors


def validate_meter_payload(payload: dict[str, Any], source_topic: str) -> list[str]:
    """Validate smart meter payloads."""
    errors: list[str] = []
    if payload.get("meter_id") in (None, ""):
        errors.append("Missing meter_id")
    if payload.get("ward_id") in (None, ""):
        errors.append("Missing ward_id")
    if payload.get("timestamp") is None:
        errors.append("Missing timestamp")
    if payload.get("kwh_reading") is None:
        errors.append("kWh reading is missing")
    elif float(payload["kwh_reading"]) < 0:
        errors.append(f"kWh reading {payload['kwh_reading']} cannot be negative")
    voltage = payload.get("voltage")
    if not isinstance(voltage, (int, float)) or not (180.0 <= float(voltage) <= 260.0):
        errors.append(f"Voltage {voltage} is out of bounds [180, 260]")
    power_factor = payload.get("power_factor")
    if not isinstance(power_factor, (int, float)) or not (0.0 <= float(power_factor) <= 1.0):
        errors.append(f"Power factor {power_factor} is out of bounds [0, 1]")
    return errors


def validate_traffic_payload(payload: dict[str, Any], source_topic: str) -> list[str]:
    """Validate traffic signal payloads."""
    errors: list[str] = []
    if payload.get("junction_id") in (None, ""):
        errors.append("Missing junction_id")
    if payload.get("zone") in (None, ""):
        errors.append("Missing zone")
    if payload.get("timestamp") is None:
        errors.append("Missing timestamp")
    if payload.get("avg_wait_sec") is None:
        errors.append("Missing avg_wait_sec")
    elif float(payload["avg_wait_sec"]) < 0:
        errors.append("avg_wait_sec cannot be negative")
    return errors


def validate_and_build_dlq(
    payload: dict[str, Any],
    source_topic: str,
    payload_type: str,
) -> dict[str, Any] | None:
    """Validate a payload and return a DLQ record when validation fails."""
    validators = {
        "aqi": validate_aqi_payload,
        "bus": validate_bus_payload,
        "meter": validate_meter_payload,
        "traffic": validate_traffic_payload,
    }
    validator = validators.get(payload_type)
    if validator is None:
        return None
    errors = validator(payload, source_topic)
    if errors:
        return build_dlq_payload(payload, "; ".join(errors), source_topic)
    return None
