"""Spark Streaming SQL health advisory generator."""

from __future__ import annotations

import sys
from pathlib import Path

import pyspark
from packaging.version import Version
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, to_json
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.constants import (
    AIR_QUALITY_TOPIC,
    AQI_ADVISORY_THRESHOLD,
    CHECKPOINTS_DIR,
    HEALTH_ADVISORIES_TOPIC,
    SQL_ROLLING_SLIDE_DURATION,
    SQL_ROLLING_WINDOW_DURATION,
    ZONE_PROFILE_CSV,
)
from config.kafka_config import get_bootstrap_servers
from config.logging_config import setup_logging

logger = setup_logging("urbanpulse.spark.health_advisory")

AQ_SCHEMA = StructType(
    [
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

ZONE_SCHEMA = StructType(
    [
        StructField("zone", StringType(), True),
        StructField("zone_name", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("number_schools", IntegerType(), True),
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
        SparkSession.builder.appName("UrbanPulse-Health-Advisories")
        .config("spark.jars.packages", kafka_package)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    """Run rolling AQI advisory generation with stream-static join."""
    if not ZONE_PROFILE_CSV.exists():
        logger.error("Static zone profile CSV not found at %s", ZONE_PROFILE_CSV)
        sys.exit(1)

    spark = build_spark_session()
    bootstrap_servers = get_bootstrap_servers()
    checkpoint_path = CHECKPOINTS_DIR / "health_advisories"

    static_zone_df = spark.read.schema(ZONE_SCHEMA).option("header", "true").csv(str(ZONE_PROFILE_CSV))
    static_zone_df.createOrReplaceTempView("zone_profile_static")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", AIR_QUALITY_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), AQ_SCHEMA).alias("data")).select("data.*")
    time_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))
    time_df.createOrReplaceTempView("air_quality_stream")

    sql_query = f"""
        SELECT
            s.zone,
            z.zone_name,
            z.population,
            z.number_schools,
            ROUND(s.rolling_avg_aqi, 2) AS rolling_avg_aqi,
            CAST(s.window_start AS STRING) AS window_start,
            CAST(s.window_end AS STRING) AS window_end
        FROM (
            SELECT
                zone,
                window.start AS window_start,
                window.end AS window_end,
                AVG(aqi) AS rolling_avg_aqi
            FROM air_quality_stream
            GROUP BY zone, window(event_time, '{SQL_ROLLING_WINDOW_DURATION}', '{SQL_ROLLING_SLIDE_DURATION}')
        ) s
        INNER JOIN zone_profile_static z ON s.zone = z.zone
        WHERE s.rolling_avg_aqi > {AQI_ADVISORY_THRESHOLD}
    """

    advisory_stream_df = spark.sql(sql_query)
    kafka_output_df = advisory_stream_df.select(
        col("zone").alias("key"),
        to_json(
            struct(
                col("zone"),
                col("zone_name"),
                col("population"),
                col("number_schools"),
                col("rolling_avg_aqi"),
                col("window_start"),
                col("window_end"),
            )
        ).alias("value"),
    )

    query = (
        kafka_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", HEALTH_ADVISORIES_TOPIC)
        .option("checkpointLocation", str(checkpoint_path))
        .outputMode("update")
        .start()
    )

    logger.info("Spark Streaming SQL advisory job started. Writing unhealthy warnings to Kafka...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
