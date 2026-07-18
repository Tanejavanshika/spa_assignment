import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, avg, max as _max, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

def main():
    # Initialize Spark Session with Kafka package configuration
    spark = SparkSession.builder \
        .appName("UrbanPulse-Ward-Energy-Analytics") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092,localhost:29092,localhost:39092')

    
    # 1. Define schema for smart_meters stream
    schema = StructType([
        StructField("meter_id", StringType(), True),
        StructField("ward_id", StringType(), True),
        StructField("kwh_reading", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
        StructField("power_factor", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # 2. Read streaming from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "urbanpulse.smart_meters") \
        .option("startingOffsets", "latest") \
        .load()
        
    # Convert value bytes to string and parse JSON
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")
        
    # Convert epoch millisecond timestamp to TimestampType for windowing
    time_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))
    
    # 3. Apply 45-minute watermark and 15-minute tumbling window
    # Group by ward_id and window, and compute aggregations
    aggregated_df = time_df \
        .withWatermark("event_time", "45 minutes") \
        .groupBy(
            col("ward_id"),
            window(col("event_time"), "15 minutes")
        ) \
        .agg(
            _sum("kwh_reading").alias("total_kwh_consumed"),
            avg("power_factor").alias("avg_power_factor"),
            _max("voltage").alias("peak_voltage")
        )
        
    # Flatten window for easier downstream representation
    flattened_df = aggregated_df.select(
        col("ward_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage")
    )

    # 4. Define foreachBatch writer to publish to Kafka and save to partitioned Parquet
    def write_to_dual_sinks(batch_df, batch_id):
        # Cache batch to avoid computing it twice
        batch_df.persist()
        
        # Sink A: Write to Kafka topic 'ward_energy_summary'
        # Convert row to JSON and set as message value
        kafka_payload = batch_df.select(
            col("ward_id").alias("key"),
            to_json(struct(
                col("ward_id"),
                col("window_start").cast(StringType()).alias("window_start"),
                col("window_end").cast(StringType()).alias("window_end"),
                col("total_kwh_consumed"),
                col("avg_power_factor"),
                col("peak_voltage")
            )).alias("value")
        )
        
        kafka_payload.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("topic", "ward_energy_summary") \
            .save()
            
        # Sink B: Write to partitioned Parquet
        # Partitioned by ward_id and date (derived from window_start)
        parquet_payload = batch_df \
            .withColumn("date", col("window_start").cast("date"))
            
        output_path = "/Users/kanak/.gemini/antigravity/scratch/urbanpulse/data/ward_energy_summary/"
        
        parquet_payload.write \
            .mode("append") \
            .partitionBy("ward_id", "date") \
            .parquet(output_path)
            
        print(f"Batch {batch_id} processed: Written to Kafka and saved partitioned Parquet.")
        batch_df.unpersist()

    # 5. Start the streaming query
    query = flattened_df.writeStream \
        .foreachBatch(write_to_dual_sinks) \
        .option("checkpointLocation", "/Users/kanak/.gemini/antigravity/scratch/urbanpulse/checkpoints/ward_energy/") \
        .start()
        
    print("Spark Structured Streaming query started. Writing to dual sinks... Press Ctrl+C to terminate.")
    query.awaitTermination()

if __name__ == '__main__':
    main()
