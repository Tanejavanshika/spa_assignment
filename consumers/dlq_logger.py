"""DLQ consumer and reporting engine for UrbanPulse validation failures."""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from confluent_kafka import KafkaError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import DLQ_TOPIC, REPORTS_DIR
from config.kafka_config import build_consumer
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.dlq_logger")


def categorize_error(reason: str) -> str:
    """Map a validation reason string to a report category."""
    lowered = reason.lower()
    if "null" in lowered or "missing" in lowered:
        return "AQI Value Null/Sensor Timeout"
    if "latitude" in lowered or "longitude" in lowered:
        return "Geospatial Boundary Violation (GPS)"
    if "speed" in lowered:
        return "Impossible Speed Telemetry (GPS)"
    if "kwh" in lowered or "negative" in lowered:
        return "Negative Power Reading (Smart Meter)"
    if "voltage" in lowered:
        return "Voltage Spike/Out of Bounds (Smart Meter)"
    return "Other Validation Error"


def write_reports(
    error_counts: Counter[str],
    total_errors: int,
    duration: int,
    report_dir: Path,
) -> None:
    """Write DLQ markdown, CSV, and chart artifacts."""
    report_dir.mkdir(parents=True, exist_ok=True)
    sorted_counts = sorted(error_counts.items(), key=lambda item: item[1], reverse=True)

    report_md_path = report_dir / "dlq_report.md"
    report_csv_path = report_dir / "dlq_report.csv"
    report_bar_path = report_dir / "dlq_report_bar.png"
    report_pie_path = report_dir / "dlq_report_pie.png"

    with report_csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Error Category", "Count", "Percentage"])
        for err_type, count in sorted_counts:
            writer.writerow([err_type, count, f"{(count / total_errors) * 100:.2f}"])

    labels = [item[0] for item in sorted_counts]
    counts = [item[1] for item in sorted_counts]
    percentages = [count / total_errors * 100 for count in counts]

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(labels, counts, color="#1f77b4")
    ax.set_title("UrbanPulse DLQ Error Distribution")
    ax.set_ylabel("Count")
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right")
    fig.tight_layout()
    fig.savefig(report_bar_path)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(percentages, labels=labels, autopct="%1.1f%%", startangle=90)
    ax.set_title("UrbanPulse DLQ Error Mix")
    fig.tight_layout()
    fig.savefig(report_pie_path)
    plt.close(fig)

    with report_md_path.open("w", encoding="utf-8") as handle:
        handle.write("# UrbanPulse Dead-Letter Queue (DLQ) Analysis Report\n\n")
        handle.write(f"- **Collection Duration:** {duration} seconds ({duration / 60:.2f} minutes)\n")
        handle.write(f"- **Total Validation Failures:** {total_errors}\n\n")
        handle.write("## Error Distribution Table\n\n")
        handle.write("| Error Category | Count | Percentage |\n")
        handle.write("| :--- | :---: | :---: |\n")
        for err_type, count in sorted_counts:
            handle.write(f"| {err_type} | {count} | {(count / total_errors) * 100:.2f}% |\n")
        handle.write("\n## Generated Artifacts\n")
        handle.write(f"- Bar chart: [{report_bar_path.name}]({report_bar_path.name})\n")
        handle.write(f"- Pie chart: [{report_pie_path.name}]({report_pie_path.name})\n")
        handle.write(f"- CSV: [{report_csv_path.name}]({report_csv_path.name})\n")

    logger.info("DLQ report saved to %s", report_md_path)


def main() -> None:
    """Collect DLQ messages and generate assignment deliverables."""
    parser = argparse.ArgumentParser(description="UrbanPulse DLQ Logger & Reporting Engine")
    parser.add_argument("--duration", type=int, default=300, help="Duration to run and gather stats in seconds")
    args = parser.parse_args()

    consumer = build_consumer("dlq-reporting-group")
    consumer.subscribe([DLQ_TOPIC])

    logger.info("DLQ Logger started. Subscribed to %s.", DLQ_TOPIC)
    logger.info(
        "Collecting validation errors for %s seconds (%s minutes)...",
        args.duration,
        args.duration / 60,
    )

    error_counts: Counter[str] = Counter()
    total_errors = 0
    end_time = time.time() + args.duration

    try:
        while time.time() < end_time:
            remaining_time = max(0.1, end_time - time.time())
            msg = consumer.poll(timeout=min(remaining_time, 1.0))
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("DLQ Consumer Error: %s", msg.error())
                continue

            try:
                dlq_record = json.loads(msg.value().decode("utf-8"))
                reason = dlq_record.get("error_reason", "Unknown validation failure")
                original_msg = dlq_record.get("original_message", {})
                device_id = (
                    original_msg.get("bus_id")
                    or original_msg.get("sensor_id")
                    or original_msg.get("meter_id")
                    or "Unknown"
                )
                error_type = categorize_error(reason)
                error_counts[error_type] += 1
                total_errors += 1
                logger.info("[DLQ LOGGED] Type: %s | Device: %s | Reason: %s", error_type, device_id, reason)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error("Failed to process DLQ payload: %s", exc)
    except KeyboardInterrupt:
        logger.info("Stopping DLQ logger early...")
    finally:
        consumer.close()

    logger.info("=" * 70)
    logger.info("URBANPULSE DEAD-LETTER QUEUE REPORT")
    logger.info("Report Duration: %s seconds (%s minutes)", args.duration, args.duration / 60)
    logger.info("Total Logged Validation Failures: %s", total_errors)
    logger.info("=" * 70)

    if total_errors == 0:
        logger.info("No validation failures detected during this period.")
        return

    sorted_counts = sorted(error_counts.items(), key=lambda item: item[1], reverse=True)
    logger.info("%-45s | %-8s | %-10s", "Error Category", "Count", "Percentage")
    logger.info("-" * 70)
    for err_type, count in sorted_counts:
        logger.info("%-45s | %-8s | %-10.2f", err_type, count, (count / total_errors) * 100)

    write_reports(error_counts, total_errors, args.duration, REPORTS_DIR)


if __name__ == "__main__":
    main()
