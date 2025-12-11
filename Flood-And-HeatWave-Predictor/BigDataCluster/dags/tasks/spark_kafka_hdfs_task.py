"""
Spark Streaming Task for Kafka to HDFS
Consumes weather data from Kafka in batches of 100 and saves to HDFS via Spark
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')
HDFS_BASE_PATH = os.environ.get('HDFS_BASE_PATH', 'hdfs://namenode:9000/user/airflow/weather_data')
BATCH_SIZE = int(os.environ.get('KAFKA_BATCH_SIZE', '100'))
CHECKPOINT_PATH = os.environ.get('CHECKPOINT_PATH', 'hdfs://namenode:9000/user/airflow/checkpoints/kafka_streaming')

# Weather data schema matching Kafka producer format
WEATHER_SCHEMA = StructType([
    StructField("District", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Latitude", DoubleType(), True),
    StructField("Longitude", DoubleType(), True),
    StructField("MaxTemp_2m", DoubleType(), True),
    StructField("MinTemp_2m", DoubleType(), True),
    StructField("TempRange_2m", DoubleType(), True),
    StructField("Precip", DoubleType(), True),
    StructField("RH_2m", DoubleType(), True),
    StructField("WindSpeed_10m", DoubleType(), True),
    StructField("WindSpeed_50m", DoubleType(), True),
    StructField("SolarRad", DoubleType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])


def create_spark_session():
    """Create Spark session with Kafka and HDFS support."""
    return SparkSession.builder \
        .appName("KafkaToHDFSStreaming") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.streaming.kafka.maxRatePerPartition", str(BATCH_SIZE)) \
        .getOrCreate()


def process_kafka_batch_to_hdfs():
    """
    Consume Kafka messages in batches of BATCH_SIZE and write to HDFS.
    Uses Spark Structured Streaming with trigger.processingTime for batch processing.
    """
    print(f"🚀 Starting Kafka to HDFS batch processor")
    print(f"   Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Batch Size: {BATCH_SIZE}")
    print(f"   HDFS Path: {HDFS_BASE_PATH}/streaming")
    
    spark = create_spark_session()
    
    try:
        # Read from Kafka
        kafka_df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", str(BATCH_SIZE)) \
            .load()
        
        # Parse JSON from Kafka value
        parsed_df = kafka_df \
            .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
            .select(
                col("key").alias("kafka_key"),
                from_json(col("value"), WEATHER_SCHEMA).alias("data")
            ) \
            .select(
                "kafka_key",
                "data.*"
            ) \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
        # Write to HDFS in parquet format, partitioned by date
        hdfs_output_path = f"{HDFS_BASE_PATH}/streaming"
        
        query = parsed_df \
            .writeStream \
            .format("parquet") \
            .option("path", hdfs_output_path) \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .partitionBy("Date", "District") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds") \
            .start()
        
        print(f"✅ Streaming query started. Writing batches to {hdfs_output_path}")
        
        # Run for a limited time in Airflow task context, or indefinitely
        query.awaitTermination()
        
    except Exception as e:
        print(f"❌ Error in Kafka to HDFS streaming: {e}")
        raise
    finally:
        spark.stop()


def run_kafka_batch_once():
    """
    Run a single batch read from Kafka and write to HDFS.
    This is useful for Airflow scheduled runs instead of continuous streaming.
    """
    print(f"🚀 Running single Kafka batch to HDFS")
    print(f"   Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"   Kafka Topic: {KAFKA_TOPIC}")
    print(f"   Batch Size: {BATCH_SIZE}")
    
    spark = create_spark_session()
    
    try:
        # Read latest messages from Kafka (batch mode)
        kafka_df = spark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("endingOffsets", "latest") \
            .load()
        
        if kafka_df.count() == 0:
            print("ℹ️  No new messages in Kafka topic")
            spark.stop()
            return
        
        # Parse JSON from Kafka value
        parsed_df = kafka_df \
            .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value") \
            .select(
                col("key").alias("kafka_key"),
                from_json(col("value"), WEATHER_SCHEMA).alias("data")
            ) \
            .select(
                "kafka_key",
                "data.*"
            ) \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
        record_count = parsed_df.count()
        print(f"📦 Processing {record_count} records from Kafka")
        
        # Write to HDFS
        hdfs_output_path = f"{HDFS_BASE_PATH}/streaming"
        parsed_df.write \
            .mode("append") \
            .partitionBy("Date", "District") \
            .parquet(hdfs_output_path)
        
        print(f"✅ Batch written to HDFS: {hdfs_output_path}")
        print(f"   Records: {record_count}")
        
    except Exception as e:
        print(f"❌ Error in Kafka batch processing: {e}")
        raise
    finally:
        spark.stop()


def merge_streaming_with_historical():
    """
    Merge streaming data from Kafka with historical processed data.
    This prepares the combined dataset for feature engineering.
    """
    print("🔄 Merging streaming data with historical data")
    
    spark = create_spark_session()
    
    try:
        historical_path = f"{HDFS_BASE_PATH}/processed"
        streaming_path = f"{HDFS_BASE_PATH}/streaming"
        merged_path = f"{HDFS_BASE_PATH}/merged"
        
        # Read historical data
        historical_df = spark.read.parquet(historical_path)
        print(f"   Historical records: {historical_df.count()}")
        
        # Read streaming data
        try:
            streaming_df = spark.read.parquet(streaming_path)
            streaming_count = streaming_df.count()
            print(f"   Streaming records: {streaming_count}")
            
            # Select common columns for union
            common_cols = list(set(historical_df.columns) & set(streaming_df.columns))
            
            # Union datasets
            merged_df = historical_df.select(common_cols).union(streaming_df.select(common_cols))
            
            # Deduplicate based on District and Date
            merged_df = merged_df.dropDuplicates(["District", "Date"])
            
        except Exception as e:
            print(f"ℹ️  No streaming data yet: {e}")
            merged_df = historical_df
        
        # Write merged data
        merged_df.write.mode("overwrite").parquet(merged_path)
        print(f"✅ Merged data written to: {merged_path}")
        print(f"   Total records: {merged_df.count()}")
        
    except Exception as e:
        print(f"❌ Error merging data: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "stream":
            process_kafka_batch_to_hdfs()
        elif mode == "batch":
            run_kafka_batch_once()
        elif mode == "merge":
            merge_streaming_with_historical()
        else:
            print(f"Unknown mode: {mode}. Use 'stream', 'batch', or 'merge'")
    else:
        # Default: run single batch
        run_kafka_batch_once()
