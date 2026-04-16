import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum, count, avg

def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    
    builder = SparkSession.builder \
        .appName("GoldAggregations") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

    if run_mode == "cloud":
        print(f"Running in CLOUD mode, targeting S3 Bucket: {s3_bucket}")
    else:
        print("Running in LOCAL mode, targeting MinIO")
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123")

    return builder.getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    silver_path = f"s3a://{s3_bucket}/silver/trips"
    gold_fact_path = f"s3a://{s3_bucket}/gold/fact_trips"
    gold_daily_path = f"s3a://{s3_bucket}/gold/agg_daily_revenue"

    print(f"Reading silver data from {silver_path}...")
    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as e:
        print(f"Failed to read silver table: {e}")
        return

    # 1. Fact Table (Flattened for BI)
    # We select key columns and ensure types are correct for analytics
    print(f"Creating Gold Fact Table at {gold_fact_path}...")
    df_fact = df_silver.select(
        "trip_id", "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "tip_amount", "total_amount",
        "trip_duration_minutes", "fare_per_mile", "pickup_h3", "dropoff_h3"
    )

    df_fact.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(gold_fact_path)

    # 2. Daily Revenue Aggregation
    print(f"Creating Gold Aggregation (Daily Revenue) at {gold_daily_path}...")
    df_daily = df_silver.withColumn("trip_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd")) \
        .groupBy("trip_date", "VendorID") \
        .agg(
            count("trip_id").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance")
        ).orderBy("trip_date")

    df_daily.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(gold_daily_path)

    print("Gold processing complete.")

if __name__ == "__main__":
    main()
