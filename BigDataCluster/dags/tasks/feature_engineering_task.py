"""
Feature Engineering Task for Airflow
Creates time-based, rolling, and anomaly features using Spark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofyear, sin, cos, year, month, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def feature_engineering():
    """Apply feature engineering to weather data"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("FeatureEngineering") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Read processed data from HDFS (output from ingest_data task)
    hdfs_input = "hdfs://namenode:9000/user/airflow/weather_data/processed"
    df = spark.read.parquet(hdfs_input)
    
    # Time features
    df = df.withColumn("doy", dayofyear(col("Date")))
    df = df.withColumn("doy_sin", sin(2 * 3.14159 * col("doy") / 365.25))
    df = df.withColumn("doy_cos", cos(2 * 3.14159 * col("doy") / 365.25))
    df = df.withColumn("year", year(col("Date")))
    df = df.withColumn("month", month(col("Date")))
    
    # Window for rolling calculations
    window_3d = Window.partitionBy("District").orderBy("Date").rowsBetween(-2, 0)
    window_7d = Window.partitionBy("District").orderBy("Date").rowsBetween(-6, 0)
    
    # Rolling features
    df = df.withColumn("precip_3d", F.sum("Precip").over(window_3d))
    df = df.withColumn("precip_7d", F.sum("Precip").over(window_7d))
    df = df.withColumn("maxT_3d_mean", F.avg("MaxTemp_2m").over(window_3d))
    df = df.withColumn("temp_range_3d_max", F.max("TempRange_2m").over(window_3d))
    
    # Lag features
    window_lag = Window.partitionBy("District").orderBy("Date")
    for lag_val in [1, 2, 3, 7, 14]:
        df = df.withColumn(f"precip_lag_{lag_val}", F.lag("Precip", lag_val).over(window_lag))
        df = df.withColumn(f"maxT_lag_{lag_val}", F.lag("MaxTemp_2m", lag_val).over(window_lag))
    
    # Climatology (simplified - using overall median as proxy)
    clim_df = df.groupBy("District", "doy").agg(
        F.median("MaxTemp_2m").alias("clim_maxT"),
        F.median("Precip").alias("clim_precip")
    )
    
    df = df.join(clim_df, on=["District", "doy"], how="left")
    df = df.withColumn("anom_maxT", col("MaxTemp_2m") - col("clim_maxT"))
    df = df.withColumn("anom_precip", col("Precip") - col("clim_precip"))
    
    # Write to HDFS
    hdfs_output = "hdfs://namenode:9000/user/airflow/weather_data/features"
    df.write.mode("overwrite").parquet(hdfs_output)
    
    print(f"✓ Features created and saved to HDFS: {hdfs_output}")
    print(f"  Records: {df.count()}")
    print(f"  Columns: {len(df.columns)}")
    
    spark.stop()

if __name__ == "__main__":
    feature_engineering()
