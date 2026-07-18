# UrbanPulse: Local Service Respin & Verification Guide

Follow these steps to start and verify the entire MetroConnect UrbanPulse platform (Ingestion, Processing, and Analytics layers) from a clean state.

---

## Prerequisites
1. **Docker Desktop**: Ensure Docker Desktop is running locally.
2. **Python environment**: Ensure Python 3.10+ is installed and install the Kafka dependency:
   ```bash
   pip install -r requirements.txt
   ```

---

## Step 1: Start the Kafka Infrastructure
Launch the 3-broker KRaft Kafka cluster and the Kafka UI management console:
```bash
cd docker
docker-compose up -d
```
* **Verify**: Open `http://localhost:8080` in your web browser to check the health of brokers `kafka1`, `kafka2`, and `kafka3`.

---

## Step 2: Initialize Kafka Topics
Run the topic initialization script to configure the custom partition counts and retention policies (24 hours for GPS, 90 days for air quality, and 365 days for smart meters):
```bash
# From the project root directory
python3 create_topics.py
```

---

## Step 3: Run the Streaming Engines (Task C)

Launch the Flink and Spark stream processing jobs in the background inside the Docker containers.

1. **Start Flink Incident Detection Pipeline** (detects gridlock, AQI breaches, and bus bunching):
   ```bash
   cd docker
   docker-compose -f docker-compose.yml run -d --name flink-detector streaming-runner python3 flink/incident_detector.py
   ```

2. **Start Spark Ward Energy Analytics** (calculates 15-minute tumbling aggregations):
   ```bash
   docker-compose -f docker-compose.yml run -d --name spark-ward-analytics streaming-runner python3 spark/ward_analytics.py
   ```

3. **Start Spark Streaming SQL Health Advisory** (performs sliding joins with static CSV):
   ```bash
   docker-compose -f docker-compose.yml run -d --name spark-health-advisory streaming-runner python3 spark/health_advisory.py
   ```

* **Verify**: Check container status using `docker ps`. You can monitor log outputs using:
  ```bash
  docker logs flink-detector
  docker logs spark-ward-analytics
  docker logs spark-health-advisory
  ```

---

## Step 4: Run the Ingestion Layer Producers (Task B)
Start the Python telemetry simulation producers on the host. We recommend running them with python's `-u` (unbuffered) flag to ensure prints are flushed instantly:

```bash
# Run in separate terminal windows or in the background:
python3 -u producers/bus_gps_producer.py
python3 -u producers/aq_producer.py
python3 -u producers/meter_producer.py
python3 -u producers/traffic_producer.py
```

---

## Step 5: Run Ingestion Processing & Validation

1. **Verify Priority Consumers** (bottleneck load testing):
   ```bash
   # Run High Priority in one shell:
   python3 consumers/priority_consumers.py --mode high
   
   # Run Standard Priority instance in another:
   python3 consumers/priority_consumers.py --mode standard --id 1
   ```
2. **Verify GPS timetable enrichment**:
   ```bash
   python3 consumers/enrichment_worker.py
   ```
3. **Execute Task C automated validation triggers**:
   ```bash
   # Tests Flink alerting
   python3 test_task_c_triggers.py
   
   # Tests Spark SQL sliding aggregations and static join
   python3 test_spark_sql_triggers.py
   ```

---

## How to Stop All Services
To completely stop and remove all active containers, run:
```bash
# 1. Stop streaming engines
docker rm -f flink-detector spark-ward-analytics spark-health-advisory

# 2. Tear down Kafka brokers and UI
cd docker
docker-compose down
```
* **Storage reset**: If you want to wipe Spark checkpoint and Parquet output states to perform a clean start in the future, run:
  ```bash
  rm -rf checkpoints/ data/ward_energy_summary/
  ```
