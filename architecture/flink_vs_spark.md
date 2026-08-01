# UrbanPulse: Flink vs Spark Streaming Comparison Report

This report evaluates and compares **Apache Flink** and **Apache Spark Structured Streaming** as the two processing engines utilized in the UrbanPulse platform. It compares their streaming paradigms, state management, fault recovery, and operational complexity, using concrete scenarios from the UrbanPulse telemetry streams.

---

## 1. Core Comparison Matrix

| Evaluation Dimension | Apache Flink | Apache Spark Structured Streaming |
| :--- | :--- | :--- |
| **Streaming Paradigm** | **Continuous Processing (Event-driven)**: Processes each incoming record individually as soon as it arrives. | **Micro-batching (Interval-driven)**: Discretizes streams into continuous small batches (e.g., every 15 minutes). |
| **Latency** | **Sub-second / Millisecond-level**: Highly optimized for real-time alerting and immediate actions. | **Second to Minute-level**: Optimized for high throughput, aggregation, and window-based summaries. |
| **State Management** | **Managed Local State (RocksDB)**: Incremental checkpointing, fine-grained state tracking, out-of-core storage. | **Checkpointing in Distributed FS**: In-memory state backed by HDFS/S3, structured using DataFrame stateful APIs. |
| **Fault Recovery** | **Chandy-Lamport Checkpointing / Savepoints**: Restores precise operator state without full replay; uses Kafka offsets. | **Write-Ahead Logs (WAL) & Lineage**: Recomputes lost micro-batches using deterministic lineage and checkpoint metadata. |
| **Operational Complexity** | **High**: Requires configuring Cluster Manager, task managers, RocksDB memory tuning, and watermark alignment. | **Moderate**: Integrates cleanly into standard Hadoop/Spark ecosystems; leverages common Spark execution models. |

---

## 2. Deep-Dive Evaluations

### A. Latency & Processing Paradigms
* **Apache Flink (Continuous Stream)**: Flink uses a continuous event-driven pipeline where elements are streamed through operators. In UrbanPulse, this is critical for the `incident_detector.py` flow. When air quality data (`AQ-SEN-100`) breaches critical levels, Flink processes it instantly, registering a 2-minute event-time timer. There is no batching delay.
* **Apache Spark (Micro-batch)**: Spark discretizes the stream into micro-batches. In UrbanPulse, the smart meter analytics (`ward_analytics.py`) are computed over 15-minute tumbling windows. Since the metrics represent hourly or daily consumption patterns, the latency of a few seconds/minutes to process a micro-batch is perfectly acceptable and allows high-throughput bulk aggregations.

### B. State Management
* **Apache Flink (Managed RocksDB)**: Flink manages states natively via keyed state structures (e.g., `ValueState`, `MapState`). It can handle state sizes that exceed available RAM by spilling to RocksDB on local disk. In UrbanPulse, the gridlock detector (`TrafficGridlockDetector`) maintains a ValueState representing consecutive vehicle count cycles, which is durable and highly scalable.
* **Apache Spark (DFS Checkpoints)**: Spark stores state memory structures inside the executor heap. For stateful operations (like sliding joins or windowed aggregations), it writes the state to a distributed file system (DFS) checkpoint directory. While less optimal for millions of keys with complex timers, it is highly efficient for localized sliding joins (e.g., matching AQI readings with static risk CSVs in `health_advisory.py`).

### C. Fault Recovery & Consistency
* **Apache Flink (Savepoints & Chandy-Lamport)**: Flink uses a distributed snapshot algorithm (Chandy-Lamport) to draw consistent checkpoints without stopping the stream. When a Flink job fails (e.g., during a gridlock event verification), it can be restarted from a savepoint. Flink coordinates offset commits with transaction boundaries, ensuring **exactly-once** end-to-end semantics.
* **Apache Spark (WAL and Lineage)**: Spark relies on metadata checkpoints and Write-Ahead Logs (WAL) to record input sources. If a node fails while aggregating ward energy metrics, Spark reads the WAL, retrieves the source Kafka offsets, and replays the lineage to rebuild the micro-batch state.

### D. Operational Complexity
* **Apache Flink**: Requires active management of task slots, memory fraction allocations (managed memory for RocksDB vs. JVM heap), and watermark propagation across parallel partitions. Event-time skew can stall state processing if not carefully monitored.
* **Apache Spark**: Operates on a standard Spark cluster topology (Driver and Executors). Memory tuning is simpler as it follows the standard Spark Executor memory model (Storage vs. Execution memory).

---

## 3. Concrete UrbanPulse Scenarios

### Scenario 1: Flink Recovering from a Gridlock Alert State
* **Context**: The `TrafficGridlockDetector` is tracking vehicle counts across 12 junctions (`JNC-101` to `JNC-112`) to detect 3 consecutive cycles of traffic congestion.
* **Failure & Recovery**: If a Flink TaskManager crashes during the second cycle of congestion, Flink uses its checkpoint data. It restarts the task, retrieves the value state from the latest incremental RocksDB checkpoint, pulls the exact source offsets from Kafka, and resumes counting without losing the fact that Junction 102 had already experienced 2 cycles of congestion. This avoids missed gridlock alerts.

### Scenario 2: Spark Recovering from a Tumbling Energy Summary Window
* **Context**: `ward_analytics.py` is aggregating smart meter readings into a 15-minute tumbling window with a 45-minute watermark to compute `total_kwh` and `avg_power_factor`.
* **Failure & Recovery**: If a Spark Executor crashes midway through a 15-minute window, the Driver spawns a new Executor. The new Executor reads the checkpoint directory on the DFS, restores the state of the active tumbling window, recovers the WAL to fetch missing messages from Kafka topics, and rebuilds the aggregation. Since the operation is a tumbling window, the state is bounded by the window size and does not require complex timer state recovery like Flink's event-time alerts.
