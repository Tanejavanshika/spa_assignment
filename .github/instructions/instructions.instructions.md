# INSTRUCTIONS.md
# UrbanPulse - Stream Processing & Analytics Assignment
# Goal: Score Full Marks (75/75)

---

# IMPORTANT

This repository is being developed for the BITS Pilani WILP course:

DSE ZG556 – Stream Processing and Analytics

The objective is NOT to build a toy Kafka project.

The objective is to satisfy EVERY rubric item mentioned in the assignment.

Whenever implementing anything, always prioritize:

1. Assignment requirements
2. Correct streaming concepts
3. Clean architecture
4. Production-quality code
5. Demonstrable outputs
6. Marks maximization

If a requirement is missing, implement it even if it adds extra files.

Never simplify requirements.

---

# Coding Standards

Use

- Python 3.11+
- Kafka 3.x
- Apache Flink
- Spark Structured Streaming
- Docker Compose
- Java 17 where required
- clean architecture
- modular packages
- logging instead of print()
- configuration through .env or config.py
- dataclasses/pydantic where appropriate
- type hints
- docstrings

Follow PEP8.

No hardcoded paths.

No duplicated code.

---

# Repository Structure

Use this structure.

project/

    architecture/
        architecture.pdf
        lambda_vs_kappa.md
        readiness_checklist.md

    docker/
        docker-compose.yml

    config/

    data/
        route_schedule.csv
        sample_data/

    kafka/
        topics.py
        producer/
        consumer/
        streams/
        dlq/

    flink/
        incidents.py

    spark/
        ward_stream.py
        streaming_sql.py

    reports/
        task_a.pdf
        task_b.pdf
        task_c.pdf
        dlq_report.pdf

    screenshots/

    logs/

    tests/

    README.md

---

# Assignment First Rule

Whenever writing code ask:

"Which marking criterion does this satisfy?"

If none,

don't implement it.

---

# TASK A

## Architecture Diagram

Diagram MUST include

✔ Bus GPS

✔ Traffic Signals

✔ Air Quality

✔ Smart Meters

↓

Kafka Cluster

↓

Real Time Layer

Apache Flink

↓

Batch Layer

Apache Spark

↓

Storage

InfluxDB
(PostgreSQL + PostGIS)
Parquet
TimescaleDB

↓

Serving Layer

Grafana

REST API

Signal Control API

Dashboard

Government Reporting Portal

Every storage technology must have justification.

---

## Lambda vs Kappa Matrix

Rows

Latency

Fault Tolerance

Operational Complexity

Reprocessing

Cost

Government Reporting

Columns

Lambda

Kappa

Every cell must mention UrbanPulse specifically.

Never use generic textbook answers.

---

## Architecture Choice

Conclude

Lambda Architecture

because

real-time alerts

AND

monthly government reports

must coexist.

Explain why Kappa alone is insufficient.

---

## Readiness Checklist

Minimum

12 items.

Must include

✔ Disaster Recovery

✔ RPO

✔ RTO

✔ Data Sovereignty

✔ Open Source

✔ Accessibility

✔ Security

✔ Monitoring

✔ Audit Logging

✔ Backup

✔ High Availability

✔ Role Based Access

---

# TASK B

## Kafka Cluster

Exactly

3 Brokers

Replication Factor

3

Min ISR

2

Document

why.

---

## Topics

Create

urbanpulse.bus_gps

urbanpulse.traffic_signals

urbanpulse.air_quality

urbanpulse.smart_meters

urbanpulse.incidents

urbanpulse.health_advisories

urbanpulse.ward_energy_summary

urbanpulse.dlq

---

## Partition Count

Justify every partition count.

Example

Bus GPS

many partitions because highest throughput

Air Quality

few partitions because low throughput

Never choose arbitrary numbers.

---

## Retention

Bus GPS

24 Hours

Air Quality

90 Days

Smart Meter

365 Days

Justify each.

---

## Producer Requirements

Bus GPS

route_id

MUST be Kafka key.

Guarantee ordering.

Enable

acks=all

enable.idempotence=true

linger.ms

batch.size

compression=lz4

Retries

Retry Backoff

Delivery Timeout

Use logging.

---

## Air Quality Producer

Implement

At Least Once

Retry

5% null AQI

Graceful logging

No crash

Continue publishing

---

## Validation

Reject

Null values

AQI > 500

AQI < 0

Latitude outside

-90

90

Longitude outside

-180

180

Invalid timestamps

Missing IDs

Route to DLQ.

Never silently discard.

---

## DLQ

Store

Original message

Reason

Timestamp

Source topic

Report

Error Distribution

Bar chart

Pie chart

CSV

---

## Consumer Groups

HIGH_PRIORITY

1 Consumer

Reads all partitions

Near zero lag

STANDARD_PRIORITY

3 Consumers

Artificial slowdown

time.sleep()

Demonstrate lag increase.

Take screenshots.

---

## Kafka Streams

Join

bus_gps

+

route_schedule KTable

Output

scheduled_arrival_time

route_name

terminal

Verify output.

---

# TASK C

## Flink

Must use

Event Time

Watermarks

Keyed State

Timers

Never Process Time.

---

## AQI Alert

AQI >300

Alert within

2 minutes

Output

Kafka

---

## Gridlock

Key

junction_id

Condition

3 consecutive cycles

avg_wait>180

Maintain state.

---

## Bus Bunching

Key

route_id

Distance

200m

Time

5 minutes

Need geodesic calculation.

Do NOT compare latitude differences.

Use Haversine Formula.

---

## Kafka Output

urbanpulse.incidents

Include

alert_type

severity

timestamp

location

message

---

## Spark Streaming

Input

urbanpulse.smart_meters

Window

15 Minutes

Watermark

45 Minutes

Output

Ward Energy Summary

Metrics

total_kwh

avg_power_factor

peak_voltage

---

## Output

Kafka

AND

Partitioned Parquet

Partition

ward_id

date

---

## Streaming SQL

Compute

10-minute rolling AQI

Join

zone_profile

Filter

AQI >150

Update Mode

Kafka Output

urbanpulse.health_advisories

---

## Flink vs Spark Report

Compare

Latency

State Size

Recovery

Operational Complexity

Use UrbanPulse examples.

Not textbook.

---

# Screenshots Required

Always capture

Kafka Topics

Kafka UI

Producer Running

Consumer Running

Consumer Lag

Kafka Streams Output

DLQ Topic

Spark Output

Parquet Files

Grafana

Docker

Flink Dashboard

Spark UI

Terminal Logs

---

# README

Must include

Architecture

Setup

Docker

Commands

Screenshots

Outputs

Folder Structure

How to Run

Results

Future Improvements

---

# Video Demonstration

Show

Start Docker

Create Topics

Run Producers

Run Consumers

Generate Data

DLQ

Kafka Streams

Flink Alerts

Spark Streaming

SQL

Parquet

Dashboards

Final Folder Structure

---

# Documentation

Every report should include

Problem Statement

Design

Implementation

Code Snippets

Screenshots

Results

Challenges

Future Work

Conclusion

---

# Evaluation Checklist

Before considering the assignment complete, verify

☐ Every rubric item implemented

☐ Every topic exists

☐ Every producer works

☐ Every consumer works

☐ Retry logic demonstrated

☐ Idempotence enabled

☐ DLQ populated

☐ Validation implemented

☐ Kafka Streams join works

☐ Flink event-time processing works

☐ Watermarks implemented

☐ Keyed state used

☐ Timers used

☐ Spark watermark is 45 minutes

☐ 15-minute tumbling windows

☐ Kafka output verified

☐ Parquet output verified

☐ SQL update mode works

☐ Architecture report complete

☐ Lambda vs Kappa matrix complete

☐ Government checklist complete

☐ Screenshots captured

☐ Reports generated

☐ README complete

☐ Code formatted

☐ No TODO comments

☐ No placeholder code

☐ No hardcoded paths

☐ Repository builds successfully

☐ End-to-end demo works without manual fixes

---

# Final Objective

Produce a repository that resembles a production-grade Smart City Streaming Platform rather than a classroom assignment.

Every implementation should be directly traceable to a marking criterion so that an evaluator can award full marks with minimal interpretation.