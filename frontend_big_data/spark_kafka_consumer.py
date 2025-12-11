"""
Spark Streaming Kafka Consumer for Frontend
Consumes weather data from Kafka in batches of 100 and saves to HDFS via Spark.
Integrates with the Flood-And-HeatWave-Predictor backend.

Usage:
    python spark_kafka_consumer.py [--mode stream|batch] [--batch-size 100]

Architecture:
    API → Kafka Producer → Kafka Topic → Spark Streaming → HDFS
                              ↓
                      'weather-data' topic
                              ↓
                      Batch of 100 messages
                              ↓
                           HDFS
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime
from typing import Optional
import threading
import warnings
warnings.filterwarnings('ignore')

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'weather-data')
HDFS_NAMENODE = os.environ.get('HDFS_NAMENODE', 'hdfs://namenode:9000')
HDFS_BASE_PATH = os.environ.get('HDFS_BASE_PATH', f'{HDFS_NAMENODE}/user/airflow/weather_data')
BATCH_SIZE = int(os.environ.get('KAFKA_BATCH_SIZE', '100'))
SPARK_MASTER = os.environ.get('SPARK_MASTER', 'spark://spark-spark-1:7077')

# Try importing PySpark
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json, current_timestamp, lit, window
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    print("⚠️  PySpark not installed. Run: pip install pyspark")

# Try importing Kafka for fallback
try:
    from kafka import KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python not installed. Run: pip install kafka-python")


# Weather data schema
WEATHER_SCHEMA = None
if SPARK_AVAILABLE:
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


class SparkKafkaHDFSConsumer:
    """
    Spark Streaming consumer that batches Kafka messages and writes to HDFS.
    """
    
    def __init__(self, 
                 kafka_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 kafka_topic: str = KAFKA_TOPIC,
                 hdfs_path: str = HDFS_BASE_PATH,
                 batch_size: int = BATCH_SIZE,
                 spark_master: str = SPARK_MASTER):
        
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.hdfs_path = hdfs_path
        self.batch_size = batch_size
        self.spark_master = spark_master
        self.spark = None
        self.streaming_query = None
        self.stats = {
            'batches_processed': 0,
            'records_written': 0,
            'errors': 0,
            'last_batch_time': None
        }
    
    def create_spark_session(self):
        """Create Spark session with Kafka and HDFS support."""
        if not SPARK_AVAILABLE:
            raise RuntimeError("PySpark not available")
        
        # Check if we're running locally or in cluster
        is_local = 'localhost' in self.kafka_servers or '127.0.0.1' in self.kafka_servers
        master = "local[*]" if is_local else self.spark_master
        
        builder = SparkSession.builder \
            .appName("KafkaToHDFSBatchConsumer") \
            .master(master) \
            .config("spark.jars.packages", 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", 
                    f"{self.hdfs_path}/checkpoints/spark_streaming")
        
        if not is_local:
            builder = builder.config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE)
        
        self.spark = builder.getOrCreate()
        print(f"✅ Spark session created")
        print(f"   Master: {master}")
        print(f"   Kafka: {self.kafka_servers}")
        print(f"   HDFS: {self.hdfs_path}")
        
        return self.spark
    
    def run_streaming(self, trigger_interval: str = "10 seconds"):
        """
        Run Spark Structured Streaming from Kafka to HDFS.
        Batches are automatically created based on trigger_interval.
        """
        if self.spark is None:
            self.create_spark_session()
        
        print(f"🚀 Starting Spark Streaming")
        print(f"   Topic: {self.kafka_topic}")
        print(f"   Batch Size: {self.batch_size}")
        print(f"   Trigger: {trigger_interval}")
        
        # Read from Kafka
        kafka_df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", str(self.batch_size)) \
            .load()
        
        # Parse JSON from Kafka value
        parsed_df = kafka_df \
            .selectExpr("CAST(key AS STRING) as kafka_key", 
                       "CAST(value AS STRING) as value",
                       "partition as kafka_partition",
                       "offset as kafka_offset",
                       "timestamp as kafka_timestamp") \
            .select(
                "kafka_key",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                from_json(col("value"), WEATHER_SCHEMA).alias("data")
            ) \
            .select(
                "kafka_key",
                "kafka_partition", 
                "kafka_offset",
                "kafka_timestamp",
                "data.*"
            ) \
            .withColumn("ingestion_time", current_timestamp()) \
            .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
        
        # Write to HDFS as parquet
        output_path = f"{self.hdfs_path}/streaming"
        checkpoint_path = f"{self.hdfs_path}/checkpoints/spark_streaming"
        
        self.streaming_query = parsed_df \
            .writeStream \
            .format("parquet") \
            .option("path", output_path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("Date", "District") \
            .outputMode("append") \
            .trigger(processingTime=trigger_interval) \
            .start()
        
        print(f"✅ Streaming query started")
        print(f"   Output: {output_path}")
        print(f"   Checkpoints: {checkpoint_path}")
        
        return self.streaming_query
    
    def run_batch_once(self) -> int:
        """
        Run a single batch read from Kafka and write to HDFS.
        Returns the number of records processed.
        """
        if self.spark is None:
            self.create_spark_session()
        
        print(f"📦 Running batch read from Kafka")
        
        try:
            # Read batch from Kafka
            kafka_df = self.spark \
                .read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
            
            record_count = kafka_df.count()
            
            if record_count == 0:
                print("ℹ️  No messages in Kafka topic")
                return 0
            
            # Parse JSON
            parsed_df = kafka_df \
                .selectExpr("CAST(key AS STRING) as kafka_key",
                           "CAST(value AS STRING) as value") \
                .select(
                    "kafka_key",
                    from_json(col("value"), WEATHER_SCHEMA).alias("data")
                ) \
                .select("kafka_key", "data.*") \
                .withColumn("ingestion_time", current_timestamp()) \
                .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            
            # Write to HDFS
            output_path = f"{self.hdfs_path}/streaming"
            parsed_df.write \
                .mode("append") \
                .partitionBy("Date", "District") \
                .parquet(output_path)
            
            self.stats['batches_processed'] += 1
            self.stats['records_written'] += record_count
            self.stats['last_batch_time'] = datetime.now().isoformat()
            
            print(f"✅ Batch written to HDFS")
            print(f"   Records: {record_count}")
            print(f"   Output: {output_path}")
            
            return record_count
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"❌ Batch processing error: {e}")
            raise
    
    def run_micro_batching(self, 
                           batch_size: int = 100,
                           interval_seconds: int = 10,
                           duration_seconds: Optional[int] = None):
        """
        Run micro-batching loop that collects batch_size messages 
        and writes to HDFS every interval_seconds.
        """
        if not KAFKA_AVAILABLE:
            raise RuntimeError("kafka-python required for micro-batching")
        
        print(f"🚀 Starting micro-batch consumer")
        print(f"   Batch Size: {batch_size}")
        print(f"   Interval: {interval_seconds}s")
        print(f"   Duration: {duration_seconds}s" if duration_seconds else "   Duration: Indefinite")
        
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_servers.split(','),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='spark-hdfs-consumer'
        )
        
        batch = []
        start_time = time.time()
        last_write_time = time.time()
        
        try:
            for message in consumer:
                batch.append(message.value)
                
                # Check if batch is full or interval elapsed
                should_write = (len(batch) >= batch_size or 
                               (time.time() - last_write_time) >= interval_seconds)
                
                if should_write and batch:
                    self._write_batch_to_hdfs(batch)
                    batch = []
                    last_write_time = time.time()
                
                # Check duration
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
            
            # Write remaining batch
            if batch:
                self._write_batch_to_hdfs(batch)
                
        except KeyboardInterrupt:
            print("\n⚠️  Consumer interrupted")
        finally:
            consumer.close()
            if self.spark:
                self.spark.stop()
        
        print(f"✅ Micro-batch consumer stopped")
        print(f"   Total batches: {self.stats['batches_processed']}")
        print(f"   Total records: {self.stats['records_written']}")
    
    def _write_batch_to_hdfs(self, batch: list):
        """Write a batch of messages to HDFS."""
        if self.spark is None:
            self.create_spark_session()
        
        if not batch:
            return
        
        try:
            # Create DataFrame from batch
            df = self.spark.createDataFrame(batch, schema=WEATHER_SCHEMA)
            df = df.withColumn("ingestion_time", current_timestamp()) \
                   .withColumn("batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
            
            # Write to HDFS
            output_path = f"{self.hdfs_path}/streaming"
            df.write \
                .mode("append") \
                .partitionBy("Date", "District") \
                .parquet(output_path)
            
            self.stats['batches_processed'] += 1
            self.stats['records_written'] += len(batch)
            self.stats['last_batch_time'] = datetime.now().isoformat()
            
            print(f"📦 Batch {self.stats['batches_processed']}: {len(batch)} records → HDFS")
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"❌ Error writing batch: {e}")
    
    def get_stats(self) -> dict:
        """Get consumer statistics."""
        return self.stats.copy()
    
    def stop(self):
        """Stop streaming and Spark session."""
        if self.streaming_query:
            self.streaming_query.stop()
        if self.spark:
            self.spark.stop()
        print("👋 Consumer stopped")


class FallbackKafkaConsumer:
    """
    Fallback Kafka consumer that doesn't require Spark.
    Collects batches and saves to local files (can be synced to HDFS later).
    """
    
    def __init__(self,
                 kafka_servers: str = KAFKA_BOOTSTRAP_SERVERS,
                 kafka_topic: str = KAFKA_TOPIC,
                 output_dir: str = "./data/kafka_batches",
                 batch_size: int = BATCH_SIZE):
        
        self.kafka_servers = kafka_servers
        self.kafka_topic = kafka_topic
        self.output_dir = output_dir
        self.batch_size = batch_size
        self.running = False
        self.stats = {
            'batches_saved': 0,
            'records_processed': 0,
            'errors': 0
        }
        
        os.makedirs(output_dir, exist_ok=True)
    
    def run(self, duration_seconds: Optional[int] = None):
        """Run the consumer."""
        if not KAFKA_AVAILABLE:
            print("❌ kafka-python not available")
            return
        
        print(f"🚀 Starting fallback Kafka consumer")
        print(f"   Servers: {self.kafka_servers}")
        print(f"   Topic: {self.kafka_topic}")
        print(f"   Batch Size: {self.batch_size}")
        print(f"   Output: {self.output_dir}")
        
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_servers.split(','),
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='fallback-consumer'
        )
        
        self.running = True
        batch = []
        start_time = time.time()
        
        try:
            for message in consumer:
                if not self.running:
                    break
                
                batch.append(message.value)
                self.stats['records_processed'] += 1
                
                if len(batch) >= self.batch_size:
                    self._save_batch(batch)
                    batch = []
                
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    break
            
            # Save remaining
            if batch:
                self._save_batch(batch)
                
        except KeyboardInterrupt:
            print("\n⚠️  Consumer interrupted")
        finally:
            consumer.close()
        
        print(f"✅ Consumer stopped. Batches: {self.stats['batches_saved']}")
    
    def _save_batch(self, batch: list):
        """Save batch to local JSON file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        filename = f"{self.output_dir}/batch_{timestamp}.json"
        
        try:
            with open(filename, 'w') as f:
                json.dump(batch, f, indent=2)
            
            self.stats['batches_saved'] += 1
            print(f"📦 Batch {self.stats['batches_saved']}: {len(batch)} records → {filename}")
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"❌ Error saving batch: {e}")
    
    def stop(self):
        """Stop the consumer."""
        self.running = False


def main():
    parser = argparse.ArgumentParser(description='Spark Kafka to HDFS Consumer')
    parser.add_argument('--mode', choices=['stream', 'batch', 'micro', 'fallback'],
                       default='micro', help='Consumer mode')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                       help='Batch size for micro-batching')
    parser.add_argument('--interval', type=int, default=10,
                       help='Interval in seconds for micro-batching')
    parser.add_argument('--duration', type=int, default=None,
                       help='Duration in seconds (None for indefinite)')
    parser.add_argument('--kafka-servers', default=KAFKA_BOOTSTRAP_SERVERS,
                       help='Kafka bootstrap servers')
    parser.add_argument('--kafka-topic', default=KAFKA_TOPIC,
                       help='Kafka topic')
    parser.add_argument('--hdfs-path', default=HDFS_BASE_PATH,
                       help='HDFS output path')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Spark Kafka to HDFS Consumer")
    print("=" * 60)
    
    if args.mode == 'fallback' or not SPARK_AVAILABLE:
        # Use fallback consumer
        consumer = FallbackKafkaConsumer(
            kafka_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            batch_size=args.batch_size
        )
        consumer.run(duration_seconds=args.duration)
    else:
        # Use Spark consumer
        consumer = SparkKafkaHDFSConsumer(
            kafka_servers=args.kafka_servers,
            kafka_topic=args.kafka_topic,
            hdfs_path=args.hdfs_path,
            batch_size=args.batch_size
        )
        
        try:
            if args.mode == 'stream':
                query = consumer.run_streaming(
                    trigger_interval=f"{args.interval} seconds"
                )
                query.awaitTermination()
            elif args.mode == 'batch':
                consumer.run_batch_once()
            else:  # micro
                consumer.run_micro_batching(
                    batch_size=args.batch_size,
                    interval_seconds=args.interval,
                    duration_seconds=args.duration
                )
        except KeyboardInterrupt:
            print("\n⚠️  Interrupted")
        finally:
            consumer.stop()


if __name__ == "__main__":
    main()
