"""Spark Structured Streaming ward-level energy analytics."""

from __future__ import annotations

import sys
from pathlib import Path

import pyspark
from packaging.version import Version
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, from_json, max as spark_max, struct, sum as spark_sum, to_json, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import (
    CHECKPOINTS_DIR,
    SMART_METERS_TOPIC,
    SPARK_WATERMARK_DURATION,
    SPARK_WINDOW_DURATION,
    WARD_ENERGY_SUMMARY_TOPIC,
    WARD_PARQUET_OUTPUT_DIR,
)
from config.kafka_config import get_bootstrap_servers
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.spark.ward_analytics")

METER_SCHEMA = StructType(
    [
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", LongType(), True),
    ]
)


def build_spark_session() -> SparkSession:
    """Create a Spark session configured for Kafka streaming."""
    spark_version = pyspark.__version__
    if Version(spark_version) < Version("4.0.0"):
        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version}"
    else:
        kafka_package = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
    spark = (
        SparkSession.builder.appName("UrbanPulse-Ward-Energy-Analytics")
        .config("spark.jars.packages", kafka_package)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_to_dual_sinks(batch_df: DataFrame, batch_id: int, bootstrap_servers: str, output_path: Path) -> None:
    """Write ward energy aggregates to Kafka and partitioned Parquet."""
    batch_df.persist()
    kafka_payload = batch_df.select(
        col("ward_id").alias("key"),
        to_json(
            struct(
                col("ward_id"),
                col("window_start"),
                col("window_end"),
                col("total_kwh"),
                col("avg_power_factor"),
                col("peak_voltage"),
            )
        ).alias("value"),
    )
    kafka_payload.write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option(
        "topic", WARD_ENERGY_SUMMARY_TOPIC
    ).save()

    parquet_payload = batch_df.withColumn("date", col("window_start").cast("date"))
    parquet_payload.write.mode("append").partitionBy("ward_id", "date").parquet(str(output_path))
    logger.info("Batch %s processed: written to Kafka and partitioned Parquet.", batch_id)
    batch_df.unpersist()


def main() -> None:
    """Run ward-level smart meter aggregation with dual sinks."""
    spark = build_spark_session()
    bootstrap_servers = get_bootstrap_servers()
    checkpoint_path = CHECKPOINTS_DIR / "ward_energy"
    WARD_PARQUET_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", SMART_METERS_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), METER_SCHEMA).alias("data")).select("data.*")
    time_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))
    aggregated_df = (
        time_df.withWatermark("event_time", SPARK_WATERMARK_DURATION)
        .groupBy(col("ward_id"), window(col("event_time"), SPARK_WINDOW_DURATION))
        .agg(
            spark_sum("kwh_reading").alias("total_kwh"),
            avg("power_factor").alias("avg_power_factor"),
            spark_max("voltage").alias("peak_voltage"),
        )
    )
    flattened_df = aggregated_df.select(
        col("ward_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_kwh"),
        col("avg_power_factor"),
        col("peak_voltage"),
    )

    query = (
        flattened_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: write_to_dual_sinks(batch_df, batch_id, bootstrap_servers, WARD_PARQUET_OUTPUT_DIR)
        )
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )

    logger.info("Spark Structured Streaming query started. Writing to dual sinks...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
