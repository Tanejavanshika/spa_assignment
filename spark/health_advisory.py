import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType, IntegerType

def main():
    # Initialize Spark Session with Kafka package configuration
    spark = SparkSession.builder \
        .appName("UrbanPulse-Health-Advisories") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:19092,localhost:29092,localhost:39092')
    
    script_dir = os.path.dirname(os.path.realpath(__file__))
    csv_path = os.path.join(script_dir, '../data/zone_profile.csv')
    
    # 1. Load static zone profiles CSV
    if not os.path.exists(csv_path):
        print(f"Error: Static zone profile CSV not found at {csv_path}")
        sys.exit(1)
        
    static_zone_schema = StructType([
        StructField("zone", StringType(), True),
        StructField("zone_name", StringType(), True),
        StructField("population", IntegerType(), True),
        StructField("number_schools", IntegerType(), True)
    ])
    
    static_zone_df = spark.read \
        .schema(static_zone_schema) \
        .option("header", "true") \
        .csv(csv_path)
        
    static_zone_df.createOrReplaceTempView("zone_profile_static")
    print("Static zone profiles loaded and registered as view 'zone_profile_static'.")

    # 2. Define schema for air_quality stream
    aq_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("zone", StringType(), True),
        StructField("pm25", DoubleType(), True),
        StructField("pm10", DoubleType(), True),
        StructField("no2", DoubleType(), True),
        StructField("aqi", IntegerType(), True),
        StructField("timestamp", LongType(), True)
    ])
    
    # 3. Read streaming from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "urbanpulse.air_quality") \
        .option("startingOffsets", "latest") \
        .load()
        
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), aq_schema).alias("data")) \
        .select("data.*")
        
    # Convert epoch timestamp to TimestampType
    time_df = parsed_df.withColumn("event_time", (col("timestamp") / 1000).cast(TimestampType()))
    
    # Register streaming table view
    time_df.createOrReplaceTempView("air_quality_stream")
    
    # 4. Write Streaming SQL Query
    # (a) computes a 10-minute rolling average AQI per zone (sliding window: 10 mins duration, 1 min slide)
    # (b) joins with static zone_profile table to produce enriched advisory
    # (c) filters for rolling_avg_aqi > 150 (Unhealthy)
    sql_query = """
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
            /* 10-minute window sliding every 1 minute to represent rolling average */
            GROUP BY zone, window(event_time, '10 minutes', '1 minute')
        ) s
        INNER JOIN zone_profile_static z ON s.zone = z.zone
        WHERE s.rolling_avg_aqi > 150.0
    """
    
    advisory_stream_df = spark.sql(sql_query)
    
    # 5. Format payload for Kafka Sink (key and value)
    kafka_output_df = advisory_stream_df.select(
        col("zone").alias("key"),
        to_json(struct(
            col("zone"),
            col("zone_name"),
            col("population"),
            col("number_schools"),
            col("rolling_avg_aqi"),
            col("window_start"),
            col("window_end")
        )).alias("value")
    )
    
    # 6. Write stream in Update mode to 'urbanpulse.health_advisories' Kafka topic
    query = kafka_output_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", "urbanpulse.health_advisories") \
        .option("checkpointLocation", "/app/checkpoints/health_advisories/") \
        .outputMode("update") \
        .start()

        
    print("Spark Streaming SQL advisory job started. Writing unhealthy warnings to Kafka in Update mode...")
    query.awaitTermination()

if __name__ == '__main__':
    main()
