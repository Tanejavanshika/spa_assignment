# UrbanPulse Streaming Platform Assignment Evaluation

This evaluation report compares two repositories in the workspace:
1. **Repository 1**: Located at `/Users/rudreshkumarlabh/Vanshika-Study/spa_assignment`
2. **Repository 2**: Located at `/Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment`

The evaluation is conducted strictly according to the **BITS Pilani WILP Stream Processing and Analytics (DSE ZG556)** assignment criteria.

---

## 1. Executive Summary

A comprehensive, side-by-side evaluation of Repository 1 and Repository 2 shows a stark contrast in rubric fulfillment. **Repository 1** is a high-quality academic submission containing complete architectural documentation, Lambda matrices, operational readiness checklists, correct stateful stream processing logic, and PEP8 compliant logging. However, it suffers from a minor configuration key bug that causes three of its ingestion producers to crash on startup when run outside Docker.

**Repository 2**, on the other hand, is a stripped-down implementation. It completely lacks the entire architectural documentation folder, readiness checklists, and Lambda vs Kappa matrices. Furthermore, it contains significant architectural shortcuts in Flink, implementing the AQI alert using a simple stateless filter/map instead of the required event-time state and timers. It also outputs incorrect field schemas for incident reports, has incomplete topic configurations, lacks DLQ charts, uses stdout prints instead of logging, and has buggy host-side test scripts.

* **Repository 1 Score**: **62.5 / 75**
* **Repository 2 Score**: **38.0 / 75**

---

## 2. Repository Winner

### **Winner: Repository 1**
Repository 1 is the clear and definitive winner. It scores **24.5 marks higher** than Repository 2. The primary reasons for Repository 1's victory are:
1. **Task A Documents**: Repository 1 has complete and formatted reports for Task A (Lambda vs Kappa Matrix and Readiness Checklist), whereas Repository 2 has absolutely nothing.
2. **Streaming Integrity**: Repository 1 correctly implements the AQI alert in Flink using keyed states and event-time timers, whereas Repository 2 implements it as a stateless pipeline.
3. **Schema Compliance**: Repository 1 adheres to the schema fields required by the assignment (`alert_type`, `severity`, `timestamp`, `location`, `message`) for incident alerts. Repository 2 uses incorrect field keys, causing integration mismatches.
4. **Validation and DLQ**: Repository 1 enforces data validation across all 4 telemetry streams, including traffic signals, and outputs detailed DLQ analysis with charts and CSV. Repository 2 lacks traffic signal validation and omits charts/CSV.

---

## 3. Requirement-by-Requirement Comparison

| Requirement | Rubric Marks | Repository 1 Status & Evidence | Repository 2 Status & Evidence |
| :--- | :---: | :--- | :--- |
| **Task A: Architecture Diagram** | **8** | **Partial**: Architecture reports present in [architecture.md](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/architecture.md) and [architecture.pdf](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/architecture.pdf). However, the diagram [overview.svg](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/screenshots/overview.svg) is a generic placeholder and lacks detailed storage/serving justifications. | **Missing**: No architecture directory, PDFs, or diagram assets exist. |
| **Task A: Lambda vs Kappa Matrix** | **6** | **Complete**: [lambda_vs_kappa.md](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/lambda_vs_kappa.md) compares both architectures across all 6 rows with specific references to UrbanPulse. Concludes Lambda is required. | **Missing**: Completely missing. |
| **Task A: Readiness Checklist** | **6** | **Complete**: [readiness_checklist.md](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/readiness_checklist.md) contains exactly 12 items addressing all criteria (DR, HA, Security, etc.). | **Missing**: Completely missing. |
| **Task B: Kafka 3-Broker Config** | **2** | **Complete**: Configured in [docker-compose.yml](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/docker/docker-compose.yml) with replication factor 3 and min ISR 2. Documented in reports. | **Complete**: Configured in [docker-compose.yml](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/docker/docker-compose.yml) and commented in topic scripts. |
| **Task B: Topic Design** | **2** | **Complete**: Creates all 8 required topics with `urbanpulse.` prefix in [create_topics.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/kafka/create_topics.py). | **Partial**: Missing creation of `urbanpulse.health_advisories` and `urbanpulse.ward_energy_summary` in [create_topics.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/create_topics.py). Spark writes to `ward_energy_summary` (no prefix). |
| **Task B: Partition & Retention Sizing** | **2** | **Complete**: Custom sizes configured in [constants.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/config/constants.py) and justified via dictionary. | **Complete**: Custom partitions and retention times justified via comments in [create_topics.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/create_topics.py). |
| **Task B: Producer Tuning** | **3** | **Partial**: Idempotency, `acks=all`, retries, and compression configured in [kafka_config.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/config/kafka_config.py). But configuration naming errors crash producers on host. | **Partial**: Idempotency and retries configured on AQ producer, but missing on GPS producer in [bus_gps_producer.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/producers/bus_gps_producer.py). No compression or timeouts set. |
| **Task B: AQI 5% Null Simulation** | **2** | **Complete**: 5% null values simulated in [aq_producer.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/producers/aq_producer.py). Graceful logger preserves runtime loop. | **Complete**: 5% null values simulated in [aq_producer.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/producers/aq_producer.py). Uses stdout print statements. |
| **Task B: Payload Validation & DLQ** | **3** | **Complete**: Detailed schemas in [validation.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/producers/validation.py) reject bad inputs and route them to DLQ for all streams, including traffic. | **Partial**: Missing validation for traffic signals in [traffic_producer.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/producers/traffic_producer.py). Bounding box check is localized; missing generic coordinates check and ID validations. |
| **Task B: DLQ Report & Charts** | **2** | **Complete**: [dlq_logger.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/consumers/dlq_logger.py) writes md, CSV, and generates matplotlib bar/pie charts in `reports/`. Stores `source_topic`. | **Partial**: [dlq_logger.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/consumers/dlq_logger.py) writes md only. Lacks CSV, bar chart, and pie chart deliverables. Payload lacks `source_topic` field. |
| **Task B: Consumer Lag Demo** | **2** | **Complete**: Priority consumer logic in [priority_consumers.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/consumers/priority_consumers.py) calculates offset lag and adds artificial sleep. | **Complete**: Lag simulation logic exists in [priority_consumers.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/consumers/priority_consumers.py). (Verification script crashes on host due to environment path issues). |
| **Task B: KTable Stream-Table Join** | **2** | **Complete**: Stream join worker in [enrichment_worker.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/streams/enrichment_worker.py) joins bus GPS with schedule. | **Complete**: Stream join worker in [enrichment_worker.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/consumers/enrichment_worker.py) enriches correctly. |
| **Task C: Flink Event Time/Watermark** | **3** | **Complete**: Configured with 10s out-of-orderness and event time extractors in [incident_detector.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/flink/incident_detector.py). | **Complete**: Configured with 10s out-of-orderness and event time extractors in [incident_detector.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/flink/incident_detector.py). |
| **Task C: Flink AQI State & Timers** | **4** | **Complete**: Keyed process function `AQIAlertDetector` registers event-time timers for 2 minutes and uses ValueState. | **Missing**: Shortcut implemented. Emits alerts immediately using stateless filter and map operations. No state, timers, or keys are used. |
| **Task C: Flink Gridlock Alert** | **4** | **Complete**: Keyed process function `TrafficGridlockDetector` stores consecutive cycles count in ValueState. | **Complete**: Keyed process function `TrafficGridlockDetector` tracks cycles in ValueState correctly. |
| **Task C: Flink Bus Bunching** | **4** | **Complete**: Stateful `BusBunchingDetector` tracks distances and uses the geodesic `haversine_distance` formula. | **Complete**: Stateful `BusBunchingDetector` implements geodesic Haversine distance correctly. |
| **Task C: Flink Output Schema** | **1** (Ded.) | **Complete**: Emits fields matching assignment (`alert_type`, `severity`, `timestamp`, `location`, `message`). | **Incorrect**: Schema mismatch. Uses `incident_type`, `key_id`, `details`, `timestamp`, `severity`. |
| **Task C: Spark Window & Watermark** | **3** | **Complete**: 15-minute tumbling windows and 45-minute watermarks configured in [ward_analytics.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/spark/ward_analytics.py). | **Complete**: 15-minute windows and 45-minute watermarks configured in [ward_analytics.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/spark/ward_analytics.py). |
| **Task C: Spark Metrics Aggregation** | **3** | **Complete**: Aggregates `total_kwh`, `avg_power_factor`, and `peak_voltage` in Spark DataFrame. | **Partial**: Computes fields, but uses `total_kwh_consumed` instead of required name `total_kwh`. |
| **Task C: Spark Dual Sinks** | **3** | **Complete**: Outputs to `urbanpulse.ward_energy_summary` Kafka topic and Parquet partitioned by `ward_id` and `date`. | **Partial**: Outputs to Parquet and Kafka, but Kafka topic is incorrectly named `ward_energy_summary` (no prefix). |
| **Task C: Spark Streaming SQL** | **6** | **Complete**: [health_advisory.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/spark/health_advisory.py) registers static CSV, does sliding window, joins, filters AQI > 150, uses Update mode. | **Complete**: Sliding join query, static CSV load, filtering, and Update mode implemented in [health_advisory.py](file:///Users/rudreshkumarlabh/Vanshika-Study/spa-assignment2/spa_assignment/spark/health_advisory.py). |
| **Task C: Flink vs Spark Report** | **5** | **Missing**: No Flink vs Spark comparison report exists. | **Missing**: No Flink vs Spark comparison report exists. |
| **Task D: Screenshots / Videos** | **N/A** | **Missing**: No real screenshots or demonstration video files are present (only placeholder overview graphic). | **Missing**: No screenshots or demonstration videos exist. |

---

## 4. Runtime Comparison

### **Repository 1**
* **Verification Outcome**: **Highly Functional inside Docker; Configuration Errors on Host.**
* **Unit Tests**: Passes 100% of unit tests successfully (`pytest tests/ -v` returns 6 passed).
* **Flink Engine**: Starts successfully inside the container and processes incoming telemetry, detecting `AQI_EMERGENCY` and `TRAFFIC_GRIDLOCK` events in event-time.
* **Spark Analytics**: Starts successfully inside the streaming container, downloads Spark-Kafka jars, and reads Kafka telemetry.
* **Ingestion Producers (Crashes)**: Running `bus_gps_producer.py`, `traffic_producer.py`, or `meter_producer.py` on the host system results in an immediate crash:
  `cimpl.KafkaException: KafkaError{code=_INVALID_ARG,val=-186,str="No such configuration property: "batch_size""}`
  This is caused by passing configuration overrides with underscores (`batch_size` and `linger_ms`) instead of dots (`batch.size` and `linger.ms`) to the confluent-kafka-python driver.

### **Repository 2**
* **Verification Outcome**: **Functional inside Docker; Major Host-Side Python Environment Failures.**
* **Integration Tests (Crashes)**:
  1. **Subprocess Interpreter Error**: Running `test_priority_lag.py` or `run_enrichment_and_dlq.py` on the host results in a crash:
     `ModuleNotFoundError: No module named 'confluent_kafka'`
     This happens because the scripts spawn background processes using hardcoded `python3` instead of the virtual environment interpreter (`sys.executable`).
  2. **Schema Mismatch Error**: Re-running Flink/Spark jobs and running `test_task_c_triggers.py` results in a crash:
     `KeyError: 'incident_type'`
     This is caused by a schema conflict. The script reads the earliest offset of `urbanpulse.incidents` (which contains alerts generated under Repository 1's schema with `alert_type` as the key) and crashes on lookup.
* **Flink Engine**: Runs inside Docker but operates on an incorrect stateless filter/map shortcut for AQI alerts.
* **Spark Analytics**: Runs inside Docker and writes Parquet partitions correctly to `/app/data/ward_energy_summary/` but uses incorrect topic name strings without prefixes.

---

## 5. Missing Features in Repository 1

1. **Detailed Architecture Diagram**: The diagram file [overview.svg](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/screenshots/overview.svg) is a placeholder block-flow diagram. It lacks detailed database shapes (InfluxDB, PostgreSQL+PostGIS, TimescaleDB) and serving layer shapes (Grafana, REST API, Signal Control API, Dashboard).
2. **Storage Justifications**: The report `architecture/architecture.md` does not list or justify the specific storage choices required by the assignment (InfluxDB, PostgreSQL+PostGIS, TimescaleDB).
3. **Flink vs Spark Comparison Report**: The report required to compare Latency, State Size, Recovery, and Operational Complexity using concrete UrbanPulse examples is completely missing.
4. **Verification Trigger Scripts**: Repo 1 lacks any integration test scripts to programmatically inject events and verify Flink/Spark triggers (unlike Repo 2).
5. **System Screenshots**: Missing real console/dashboard screenshots showing the platform running.

---

## 6. Missing Features in Repository 2

1. **Task A Deliverables**: The entire `architecture/` directory is missing. There is no Architecture Report, Lambda vs Kappa Matrix, or Operational Readiness Checklist.
2. **Kafka Topic Pre-configurations**: The topic creation script does not create `urbanpulse.health_advisories` and `urbanpulse.ward_energy_summary` (which Spark attempts to write to).
3. **Traffic Signal Validation**: The traffic producer does not validate payloads and contains no DLQ routing logic.
4. **Idempotence & Retries on GPS**: The Bus GPS producer does not configure idempotence, retries, compression, or timeouts.
5. **DLQ Payload Context**: The DLQ messages do not record the `source_topic` field.
6. **DLQ Chart & CSV Output**: The DLQ logger does not write a CSV and does not use matplotlib to render bar/pie charts.
7. **PEP8 Compliance**: Standard `logging` is omitted in favor of simple stdout prints.
8. **Flink State & Timers (AQI Alert)**: The AQI breach alert is implemented statelessly without key-by routing, value state tracking, or event-time timers.
9. **Flink vs Spark Comparison Report**: Missing.
10. **README File**: Missing documentation file (only a setup guide is present).

---

## 7. Incorrect Implementations

### **Repository 1**
* **Underscored Config Overrides**: In `producers/bus_gps_producer.py`, `traffic_producer.py`, and `meter_producer.py`, properties are passed as `batch_size` and `linger_ms` which are rejected by librdkafka. They must be corrected to `batch.size` and `linger.ms`.

### **Repository 2**
* **Stateless AQI Alert**: Implemented AQI alert in Flink using a simple filter and map, completely bypassing state and timer mechanics.
* **Flink Output Schema Mismatch**: Emits incident records with fields `incident_type`, `key_id`, and `details` instead of the required assignment schema (`alert_type`, `location`, `message`).
* **Spark Aggregate Key Mismatch**: Computes energy summaries using `total_kwh_consumed` instead of `total_kwh`.
* **Spark Kafka Sink Topic**: Writes to topic `ward_energy_summary` which violates the unified `urbanpulse.` prefix system.
* **Subprocess Hardcoding**: Verification scripts invoke subprocesses via `python3` which fails in virtual environment installations.

---

## 8. Extra Features Not Required

### **Repository 2**
* **Integration Trigger Scripts**: The test scripts (`test_task_c_triggers.py`, `test_spark_triggers.py`, `test_spark_sql_triggers.py`) are extra files created to inject and verify streaming states. While useful, they contain coding environment bugs.

---

## 9. Marks Breakdown

| Evaluation Item | Max Marks | Repository 1 Marks | Repository 2 Marks | Reason for Deductions |
| :--- | :---: | :---: | :---: | :--- |
| **Task A: Architecture Diagram** | 8 | **2.0** | **0.0** | **Repo 1**: Placeholder diagram, missing storage/serving layers, missing justifications. <br>**Repo 2**: Completely missing. |
| **Task A: Lambda Matrix** | 6 | **6.0** | **0.0** | **Repo 2**: Completely missing. |
| **Task A: Readiness Checklist** | 6 | **6.0** | **0.0** | **Repo 2**: Completely missing. |
| **Task B: Kafka Broker Config** | 2 | **2.0** | **2.0** | None. |
| **Task B: Topic Design** | 2 | **2.0** | **1.0** | **Repo 2**: Creation script fails to define 2 of 8 required topics. Spark writes to incorrect prefix. |
| **Task B: Partition & Retention** | 2 | **2.0** | **2.0** | None. |
| **Task B: Producer Tuning** | 3 | **1.5** | **1.5** | **Repo 1**: Config errors cause startup crash.<br>**Repo 2**: GPS producer lacks idempotence, retries, compression, timeouts. |
| **Task B: AQ Producer** | 2 | **2.0** | **1.8** | **Repo 2**: Uses prints instead of PEP8 logging. |
| **Task B: Validation & DLQ** | 3 | **3.0** | **1.5** | **Repo 2**: Missing validation on traffic signals, missing ID/range checks on others. |
| **Task B: DLQ Report** | 2 | **2.0** | **0.5** | **Repo 2**: Missing source topic key, missing CSV, bar, and pie charts. |
| **Task B: Consumer Lag Demo** | 2 | **2.0** | **1.3** | **Repo 2**: Subprocess scripts crash due to environment paths. |
| **Task B: Kafka Streams Join** | 2 | **2.0** | **2.0** | None. |
| **Task C: Flink Event Time** | 3 | **3.0** | **3.0** | None. |
| **Task C: Flink AQI State/Timers** | 4 | **4.0** | **0.0** | **Repo 2**: AQI alerts implemented statelessly. No state, timers, or keys used. |
| **Task C: Flink Gridlock Alert** | 4 | **4.0** | **4.0** | None. |
| **Task C: Flink Bus Bunching** | 4 | **4.0** | **4.0** | None. |
| **Task C: Flink Output Schema** | 1 (Ded.) | **0.0** | **-1.0** | **Repo 2**: Schema keys mismatch (`incident_type` vs `alert_type`, etc.). |
| **Task C: Spark Windowing** | 3 | **3.0** | **3.0** | None. |
| **Task C: Spark Metrics** | 3 | **3.0** | **2.7** | **Repo 2**: Field name mismatch (`total_kwh_consumed` instead of `total_kwh`). |
| **Task C: Spark Sinks** | 3 | **3.0** | **2.7** | **Repo 2**: Writes to topic `ward_energy_summary` (no prefix). |
| **Task C: Spark SQL** | 6 | **6.0** | **6.0** | None. |
| **Task C: Flink vs Spark Report** | 5 | **0.0** | **0.0** | **Both**: Report comparing Latency, State, Recovery, and Complexity is missing. |
| **TOTAL SCORE** | **75** | **62.5** | **38.0** | **Winner: Repository 1** |

---

## 10. Final Recommendation

### **Submit Repository 1**
Repository 1 is the superior submission and must be chosen. It scores a **62.5 / 75** compared to Repository 2's **38.0 / 75**.

#### **Critical Action to Perform before Submitting Repository 1**
To ensure that Repository 1 runs flawlessly on the evaluator's system without throwing startup crashes:
1. **Fix Ingestion Producers Configuration**: Open the following three producer files:
   - `producers/bus_gps_producer.py`
   - `producers/traffic_producer.py`
   - `producers/meter_producer.py`
   Locate the instantiation call `create_producer(..., batch_size=65536, linger_ms=10)` and change the argument keys to use dot notation:
   ```python
   # Change from:
   producer = create_producer("bus-gps-producer", batch_size=65536, linger_ms=10)
   # To:
   producer = create_producer("bus-gps-producer", **{"batch.size": 65536, "linger.ms": 10})
   ```
2. **Create the Flink vs Spark Comparison Report**: Add a subsection at the end of [architecture.md](file:///Users/rudreshkumarlabh/Vanshika-Study/spa_assignment/architecture/architecture.md) comparing Flink and Spark across Latency, State Size, Recovery, and Operational Complexity.

---

## 11. Ranked List of Missing Features by Marks Lost

This checklist ranks all incomplete or missing features across both repositories to enable a developer or AI to achieve a perfect 75/75.

### **Priority 1: Task A Architecture Documentation (Up to 14 Marks Lost)**
* **Missing in Repo 2 (Loss of 14 Marks)** / **Partial in Repo 1 (Loss of 6 Marks)**:
  - Generate the `architecture/` folder containing `architecture.md`, `lambda_vs_kappa.md`, and `readiness_checklist.md`.
  - Draw a detailed SVG/Mermaid diagram showing:
    - Inputs: Bus GPS, Traffic Signals, Air Quality, Smart Meters.
    - Streaming layer: Kafka Cluster -> Apache Flink & Apache Spark.
    - Sinks: InfluxDB, (PostgreSQL + PostGIS), Parquet, TimescaleDB. Include written justifications explaining why each DB was selected (e.g. InfluxDB/TimescaleDB for time-series metrics, PostgreSQL/PostGIS for geospatial routing queries, Parquet for long-term audit storage).
    - Serving layer: Grafana dashboard, REST API, Signal Control API, Dashboard, and Government Reporting Portal.

### **Priority 2: Flink State and Timers for AQI Alerts (4 Marks Lost)**
* **Missing in Repo 2 (Loss of 4 Marks)**:
  - Refactor `flink/incident_detector.py` to route AQI breaches using `KeyedProcessFunction` keyed on `sensor_id`.
  - Store the latest event state in a `ValueState`.
  - Register an event-time timer using `ctx.timer_service().register_event_time_timer(event["timestamp"] + 120000)`.
  - Emit the AQI alert from `on_timer()` and clean state. Do not use stateless filter/map shortcuts.

### **Priority 3: Flink vs Spark Comparison Report (5 Marks Lost)**
* **Missing in Both Repositories (Loss of 5 Marks)**:
  - Document a detailed comparison report comparing Latency (sub-second Flink vs micro-batch Spark), State Size (managed RocksDB state in Flink vs checkpointed structure in Spark), Recovery (Flink savepoints/offsets vs Spark Write-Ahead-Logs), and Operational Complexity. 
  - Ground the report with UrbanPulse-specific scenarios (e.g. Flink recovering from a gridlock state vs Spark recovering from a tumbling energy summary window).

### **Priority 4: Ingestion Producer Tuning Fixes (Up to 3 Marks Lost)**
* **Partial in Both Repositories (Loss of 1.5 Marks)**:
  - **Repo 1**: Change config overrides in GPS, traffic, and meter producers from `batch_size`/`linger_ms` to `batch.size`/`linger.ms` to avoid startup configuration property crashes.
  - **Repo 2**: Add idempotency (`enable.idempotence=True`), retries (`retries=10`, `retry.backoff.ms=100`), compression (`compression.type=lz4`), and delivery timeouts (`delivery.timeout.ms=120000`) configurations to the Bus GPS producer client.

### **Priority 5: Payload Validation and DLQ (Up to 3 Marks Lost)**
* **Partial in Repo 2 (Loss of 1.5 Marks)**:
  - Implement full telemetry checks inside `traffic_producer.py` and route non-compliant events to the DLQ topic.
  - Add missing ID checks, null checks, and coordinate range checks to all producers.

### **Priority 6: DLQ Deliverables and Metrics (Up to 2 Marks Lost)**
* **Partial in Repo 2 (Loss of 1.5 Marks)**:
  - Add the `source_topic` field to DLQ payloads.
  - Update `dlq_logger.py` to output a CSV list of anomalies, and import `matplotlib` to render a bar chart and pie chart of validation errors.

### **Priority 7: Topic Configuration Mismatch (Up to 2 Marks Lost)**
* **Partial in Repo 2 (Loss of 1 Mark)**:
  - Update `create_topics.py` to create `urbanpulse.health_advisories` and `urbanpulse.ward_energy_summary`.
  - Update `spark/ward_analytics.py` to write to the correct prefixed topic `urbanpulse.ward_energy_summary`.

### **Priority 8: Spark Metrics Naming (0.3 Marks Lost)**
* **Partial in Repo 2 (Loss of 0.3 Marks)**:
  - Rename the aggregated Spark output column from `total_kwh_consumed` to `total_kwh`.
