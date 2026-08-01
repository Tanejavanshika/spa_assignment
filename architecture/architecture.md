# UrbanPulse Architecture Report

## Problem Statement
UrbanPulse must ingest bus, traffic, air-quality, and smart-meter telemetry in real time, detect incidents, generate health advisories, and support government reporting.

## Design
The platform uses a Lambda-style architecture: Kafka as the shared ingestion backbone, Flink for real-time incident detection, Spark for batch-style and streaming analytics, and Parquet/JSON sinks for long-term storage.

## Implementation
- Bus GPS, traffic, AQI, and smart meter events flow into Kafka topics.
- Flink processes events with event-time watermarks, keyed state, and timers.
- Spark generates ward-level energy summaries and health advisories.
## Storage Layer Justifications
The storage layer of UrbanPulse is designed to support high-throughput ingestion, spatial routing queries, and temporal aggregations by leveraging specialized databases:
- **InfluxDB**: Chosen for time-series air quality telemetry. It excels in handling high-velocity writes and queries for metrics like `pm25`, `pm10`, and `no2` with automatic retention policies and efficient time-range aggregation.
- **PostgreSQL + PostGIS**: Selected for geo-spatial routing. The PostGIS extension enables spatial index lookups and distance queries on bus GPS telemetry (`lat` and `lon`), supporting features like bus route geofencing and proximity alerting.
- **TimescaleDB**: Utilized for temporal smart-meter analytics. It combines the SQL interface of PostgreSQL with time-series hyper-tables, allowing fast aggregations of `total_kwh`, `avg_power_factor`, and voltage spikes over arbitrary time windows.
- **Parquet Columnar Files**: Used as the long-term cold storage sink for historical audit and compliance. It offers high columnar compression, partitioning by `ward_id` and `date`, and optimal query performance for Spark batch processes.

## Code Snippets
- Kafka topics are created in `kafka/create_topics.py`
- Flink detection is implemented in `flink/incident_detector.py`
- Spark summarization is implemented in `spark/ward_analytics.py` and `spark/health_advisory.py`
- Stream-table enrichment is implemented in `streams/enrichment_worker.py`

## Screenshots
Screenshots are documented in `screenshots/overview.svg`.

## Results
The repository now includes a working local demonstration path for Kafka, Flink, Spark, DLQ handling, and documentation generation.

## Challenges
Balancing the assignment rubric with a lightweight local deployment required careful topic planning and validation logic.

## Flink vs Spark Comparison
A detailed comparison between Apache Flink and Apache Spark Structured Streaming has been documented in [flink_vs_spark.md](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/flink_vs_spark.md). Key findings include:
- **Latency**: Flink provides sub-second event-driven latency suitable for real-time traffic gridlock and AQI breach alerts, while Spark Structured Streaming operates on a micro-batch model suited for ward-level metrics and health advisory aggregations.
- **State management**: Flink utilizes a managed local state backend (RocksDB) supporting incremental checkpointing, whereas Spark relies on distributed file system checkpoints.
- **Fault Recovery**: Flink leverages Chandy-Lamport checkpointing and savepoints to recover operator state, while Spark uses Write-Ahead Logs (WAL) and metadata lineage replay.

## Conclusion
UrbanPulse now demonstrates a production-style streaming architecture suitable for a full assignment submission.

