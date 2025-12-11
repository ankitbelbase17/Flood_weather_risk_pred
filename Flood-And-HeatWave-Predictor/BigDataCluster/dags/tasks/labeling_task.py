"""
Labeling Task for Airflow
Creates heatwave and flood labels using Spark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def create_labels():
    """Create heatwave and flood labels"""
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("Labeling") \
        .master("spark://spark-spark-1:7077") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
        .getOrCreate()
    
    # Read features from HDFS
    hdfs_input = "hdfs://namenode:9000/user/airflow/weather_data/features"
    df = spark.read.parquet(hdfs_input)
    
    # Heatwave labeling
    # Calculate 95th percentile per district
    percentile_df = df.groupBy("District").agg(
        F.expr("percentile_approx(MaxTemp_2m, 0.95)").alias("temp_p95")
    )
    
    df = df.join(percentile_df, on="District", how="left")
    df = df.withColumn("heat_exceed", col("MaxTemp_2m") > col("temp_p95"))
    
    # Simplified heatwave logic (consecutive days check would need UDF)
    df = df.withColumn("heatwave", col("heat_exceed").cast("int"))
    
    # Flood labeling
    window_3d = Window.partitionBy("District").orderBy("Date").rowsBetween(-2, 0)
    df = df.withColumn("precip_1d", F.coalesce(col("Precip"), lit(0)))
    df = df.withColumn("precip_3d_sum", F.sum("precip_1d").over(window_3d))
    
    # Calculate percentiles for flood
    flood_percentile_df = df.groupBy("District").agg(
        F.expr("percentile_approx(precip_1d, 0.99)").alias("precip_p99"),
        F.expr("percentile_approx(precip_3d_sum, 0.98)").alias("precip3_p98"),
        F.expr("percentile_approx(RH_2m, 0.90)").alias("rh_p90")
    )
    
    df = df.join(flood_percentile_df, on="District", how="left")
    
    df = df.withColumn("wetness_flag", col("RH_2m") > col("rh_p90"))
    df = df.withColumn("flood_proxy", 
        when((col("precip_1d") > col("precip_p99")) | 
             (col("precip_3d_sum") > col("precip3_p98")), True)
        .otherwise(False) & col("wetness_flag")
    )
    df = df.withColumn("flood_proxy", col("flood_proxy").cast("int"))
    
    # Write to HDFS
    hdfs_output = "hdfs://namenode:9000/user/airflow/weather_data/labeled"
    df.write.mode("overwrite").parquet(hdfs_output)
    
    print(f"✓ Labels created and saved to HDFS: {hdfs_output}")
    print(f"  Records: {df.count()}")
    
    # Show label statistics
    heatwave_count = df.filter(col("heatwave") == 1).count()
    flood_count = df.filter(col("flood_proxy") == 1).count()
    total = df.count()
    
    print(f"  Heatwave events: {heatwave_count} ({heatwave_count/total*100:.2f}%)")
    print(f"  Flood events: {flood_count} ({flood_count/total*100:.2f}%)")
    
    spark.stop()

if __name__ == "__main__":
    create_labels()
