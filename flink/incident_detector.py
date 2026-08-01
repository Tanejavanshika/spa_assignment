"""Flink real-time incident detection for UrbanPulse."""

from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    from pyflink.common import Duration, SimpleStringSchema, Types, WatermarkStrategy
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink, KafkaSource
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
except ImportError:
    import subprocess
    import sys
    import os
    print("Warning: 'pyflink' module not found on host. Transparently delegating execution to Docker container 'streaming-runner'...\n", file=sys.stderr)
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docker_dir = os.path.join(root_dir, "docker")
    cmd = ["docker", "compose", "exec"]
    if not sys.stdin.isatty():
        cmd.append("-T")
    cmd += ["streaming-runner", "python3", "flink/incident_detector.py"] + sys.argv[1:]
    try:
        res = subprocess.run(cmd, cwd=docker_dir)
        sys.exit(res.returncode)
    except Exception as e:
        print(f"Error: Failed to delegate to Docker: {e}", file=sys.stderr)
        print("Please ensure Docker is running and run: cd docker && docker compose up -d --force-recreate streaming-runner", file=sys.stderr)
        sys.exit(1)

from config.constants import (
    AIR_QUALITY_TOPIC,
    AQI_EMERGENCY_THRESHOLD,
    BUS_BUNCHING_DISTANCE_METERS,
    BUS_BUNCHING_DURATION_MS,
    BUS_GPS_TOPIC,
    GRIDLOCK_CONSECUTIVE_EVENTS,
    GRIDLOCK_WAIT_THRESHOLD_SEC,
    INCIDENTS_TOPIC,
    TRAFFIC_SIGNALS_TOPIC,
)
from config.kafka_config import get_bootstrap_servers
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.flink.incident_detector")


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate the distance between two points in meters using the Haversine formula."""
    radius = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return radius * c


class TrafficGridlockDetector(KeyedProcessFunction):
    """Detect gridlock after consecutive high-wait cycles."""

    def __init__(self) -> None:
        self.consecutive_count_state = None

    def open(self, context: Any) -> None:
        state_desc = ValueStateDescriptor("consecutive_count", Types.INT())
        self.consecutive_count_state = context.get_state(state_desc)

    def process_element(self, value_json_str: str, ctx: Any) -> Iterator[str]:
        try:
            event = json.loads(value_json_str)
            avg_wait_sec = event["avg_wait_sec"]
            current_count = self.consecutive_count_state.value() or 0
            current_count = current_count + 1 if avg_wait_sec > GRIDLOCK_WAIT_THRESHOLD_SEC else 0
            self.consecutive_count_state.update(current_count)

            if current_count >= GRIDLOCK_CONSECUTIVE_EVENTS:
                alert = {
                    "alert_type": "TRAFFIC_GRIDLOCK",
                    "severity": "CRITICAL",
                    "timestamp": event["timestamp"],
                    "location": f"Junction {event['junction_id']} ({event['zone']})",
                    "message": (
                        f"Junction {event['junction_id']} has exceeded "
                        f"{GRIDLOCK_WAIT_THRESHOLD_SEC} seconds average wait for {current_count} consecutive cycles."
                    ),
                }
                yield json.dumps(alert)
                self.consecutive_count_state.update(0)
        except (KeyError, json.JSONDecodeError, TypeError) as exc:
            logger.error("Error in TrafficGridlockDetector: %s", exc)


class BusBunchingDetector(KeyedProcessFunction):
    """Detect bus bunching using Haversine distance and keyed state."""

    def __init__(self) -> None:
        self.bus_positions_state = None
        self.bunching_start_state = None

    def open(self, context: Any) -> None:
        pos_desc = MapStateDescriptor(
            "bus_positions",
            Types.STRING(),
            Types.TUPLE([Types.DOUBLE(), Types.DOUBLE(), Types.LONG()]),
        )
        self.bus_positions_state = context.get_map_state(pos_desc)
        bunch_desc = MapStateDescriptor("bunching_start", Types.STRING(), Types.LONG())
        self.bunching_start_state = context.get_map_state(bunch_desc)

    def process_element(self, value_json_str: str, ctx: Any) -> Iterator[str]:
        try:
            event = json.loads(value_json_str)
            bus_id = event["bus_id"]
            route_id = event["route_id"]
            lat = event["lat"]
            lon = event["lon"]
            timestamp = event["timestamp"]

            self.bus_positions_state.put(bus_id, (lat, lon, timestamp))
            for other_id in list(self.bus_positions_state.keys()):
                if other_id == bus_id:
                    continue
                other_pos = self.bus_positions_state.get(other_id)
                if other_pos is None:
                    continue
                other_lat, other_lon, other_ts = other_pos
                if abs(timestamp - other_ts) > 600000:
                    continue
                dist = haversine_distance(lat, lon, other_lat, other_lon)
                pair_key = "_".join(sorted([bus_id, other_id]))
                if dist < BUS_BUNCHING_DISTANCE_METERS:
                    start_ts = self.bunching_start_state.get(pair_key)
                    if start_ts is None:
                        self.bunching_start_state.put(pair_key, timestamp)
                    elif timestamp - start_ts >= BUS_BUNCHING_DURATION_MS:
                        alert = {
                            "alert_type": "BUS_BUNCHING",
                            "severity": "WARNING",
                            "timestamp": timestamp,
                            "location": f"Route {route_id}",
                            "message": (
                                f"Bus bunching detected on route {route_id} between {bus_id} and {other_id} "
                                f"with no more than {dist:.1f}m separation."
                            ),
                        }
                        yield json.dumps(alert)
                        self.bunching_start_state.put(pair_key, timestamp)
                else:
                    self.bunching_start_state.remove(pair_key)
        except (KeyError, json.JSONDecodeError, TypeError) as exc:
            logger.error("Error in BusBunchingDetector: %s", exc)


class AQIAlertDetector(KeyedProcessFunction):
    """Emit an AQI emergency alert within two minutes of the event using event-time timers."""

    def __init__(self) -> None:
        self.pending_state = None
        self.last_event_state = None
        self.alert_time_state = None

    def open(self, context: Any) -> None:
        self.pending_state = context.get_state(ValueStateDescriptor("pending", Types.BOOLEAN()))
        self.last_event_state = context.get_state(ValueStateDescriptor("last_event", Types.STRING()))
        self.alert_time_state = context.get_state(ValueStateDescriptor("alert_time", Types.LONG()))

    def process_element(self, value_json_str: str, ctx: Any) -> None:
        try:
            event = json.loads(value_json_str)
            if event.get("aqi") is not None and event["aqi"] > AQI_EMERGENCY_THRESHOLD:
                self.pending_state.update(True)
                self.last_event_state.update(value_json_str)
                self.alert_time_state.update(event["timestamp"])
                ctx.timer_service().register_event_time_timer(event["timestamp"] + 120000)
        except (KeyError, json.JSONDecodeError, TypeError) as exc:
            logger.error("Error in AQIAlertDetector: %s", exc)

    def on_timer(self, timestamp: int, ctx: Any) -> Iterator[str]:
        if self.pending_state.value() is True:
            event = json.loads(self.last_event_state.value())
            alert = {
                "alert_type": "AQI_EMERGENCY",
                "severity": "CRITICAL",
                "timestamp": event["timestamp"],
                "location": f"{event['zone']} / {event['sensor_id']}",
                "message": f"Air quality emergency detected in {event['zone']}: AQI {event['aqi']}.",
            }
            self.pending_state.clear()
            self.last_event_state.clear()
            self.alert_time_state.clear()
            yield json.dumps(alert)


def extract_timestamp(json_str: str) -> int:
    """Extract event timestamp from a JSON Kafka message."""
    try:
        return json.loads(json_str)["timestamp"]
    except (KeyError, json.JSONDecodeError, TypeError):
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def main() -> None:
    """Build and execute the Flink incident detection job."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    flink_jar = os.getenv(
        "FLINK_KAFKA_CONNECTOR_JAR",
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.18.jar",
    )
    env.add_jars(flink_jar)

    bootstrap_servers = get_bootstrap_servers()
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        .with_timestamp_assigner(lambda event, timestamp: extract_timestamp(event))
    )

    aq_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(AIR_QUALITY_TOPIC)
        .set_group_id("flink-aq-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    traffic_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(TRAFFIC_SIGNALS_TOPIC)
        .set_group_id("flink-traffic-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    bus_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(BUS_GPS_TOPIC)
        .set_group_id("flink-bus-group")
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    alert_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(INCIDENTS_TOPIC)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    aq_stream = env.from_source(aq_source, watermark_strategy, "AQI Source")
    traffic_stream = env.from_source(traffic_source, watermark_strategy, "Traffic Source")
    bus_stream = env.from_source(bus_source, watermark_strategy, "Bus GPS Source")

    aqi_alerts = aq_stream.key_by(lambda value: json.loads(value)["sensor_id"]).process(
        AQIAlertDetector(), output_type=Types.STRING()
    )
    gridlock_alerts = traffic_stream.key_by(lambda value: json.loads(value)["junction_id"]).process(
        TrafficGridlockDetector(), output_type=Types.STRING()
    )
    bunching_alerts = bus_stream.key_by(lambda value: json.loads(value)["route_id"]).process(
        BusBunchingDetector(), output_type=Types.STRING()
    )

    union_alerts = aqi_alerts.union(gridlock_alerts).union(bunching_alerts)
    union_alerts.sink_to(alert_sink)
    union_alerts.print()

    logger.info("Flink Incident Detector Stream Graph created. Starting execution...")
    env.execute("UrbanPulse Real-Time Incident Detector")


if __name__ == "__main__":
    main()
