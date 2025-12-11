"""
Data Ingestion Task for Airflow
Loads weather CSV and saves to HDFS as Parquet
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def ingest_weather_data():
    """Ingest weather data from CSV to HDFS"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("WeatherDataIngest") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Read CSV directly from HDFS (we already uploaded it)
    csv_path = "hdfs://namenode:9000/user/airflow/weather_data/raw/terai_districts_weather.csv"
    df = spark.read.csv(csv_path, header=True, inferSchema=True)
    
    # Clean column names
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip())
    
    # Parse dates
    df = df.withColumn("Date", to_date(col("Date")))
    
    # Write to HDFS as parquet (processed data)
    hdfs_path = "hdfs://namenode:9000/user/airflow/weather_data/processed"
    df.write.mode("overwrite").parquet(hdfs_path)
    
    print(f"✓ Data ingested to HDFS: {hdfs_path}")
    print(f"  Records: {df.count()}")
    print(f"  Columns: {len(df.columns)}")
    
    spark.stop()

if __name__ == "__main__":
    ingest_weather_data()
