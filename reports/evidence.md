**Evidence Summary**

**What:** Collected runtime evidence from the local streaming run (Kafka, producers, consumers, Spark, Flink).

**When:** 27 July 2026

**Artifacts (files in this repo):**

- **Incident samples:** [incident_samples.jsonl](incident_samples.jsonl)
- **DLQ report:** [dlq_report.md](dlq_report.md)
- **Assignment PDFs:** [task_a.pdf](task_a.pdf), [task_b.pdf](task_b.pdf), [task_c.pdf](task_c.pdf), [dlq_report.pdf](dlq_report.pdf)

**Commands used to collect evidence:**

```bash
# Start infrastructure
./scripts/start_kafka.sh
python3 kafka/create_topics.py

# Run producers and consumers, then collect DLQ report
python3 consumers/dlq_logger.py --duration 120

# Sample incidents (inside docker streaming-runner or host)
python3 scripts/consume_incidents.py --bootstrap kafka1:9092 --count 5 --timeout 30
```

**Notes:**

- Spark Structured Streaming writes Parquet to `data/ward_energy_summary/` and checkpoints to `checkpoints/`.
- Flink incident detector emits alerts to `urbanpulse.incidents`.
- DLQ artifacts are written directly to `reports/` by `consumers/dlq_logger.py`.
