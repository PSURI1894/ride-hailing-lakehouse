import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, date_format, sum, when


def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")

    builder = (
        SparkSession.builder.appName("GoldAggregations")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )

    if run_mode == "cloud":
        print(f"Running in CLOUD mode, targeting S3 Bucket: {s3_bucket}")
    else:
        print("Running in LOCAL mode, targeting MinIO")
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "admin")
            .config("spark.hadoop.fs.s3a.secret.key", "password123")
        )

    return builder.getOrCreate()


def write_delta(df, path, mode="overwrite"):
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    silver_path = f"s3a://{s3_bucket}/silver/trips"
    gold_fact_path = f"s3a://{s3_bucket}/gold/fact_trips"
    gold_daily_path = f"s3a://{s3_bucket}/gold/agg_daily_revenue"
    gold_hourly_path = f"s3a://{s3_bucket}/gold/agg_hourly_demand"
    gold_payment_dim_path = f"s3a://{s3_bucket}/gold/dim_payment"
    gold_zone_dim_path = f"s3a://{s3_bucket}/gold/dim_zone"

    print(f"Reading silver data from {silver_path}...")
    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as exc:
        print(f"Failed to read silver table: {exc}")
        return

    print(f"Creating Gold Fact Table at {gold_fact_path}...")
    df_fact = df_silver.select(
        "trip_id",
        "vendor_id",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "PULocationID",
        "DOLocationID",
        "pickup_zone",
        "dropoff_zone",
        "pickup_borough",
        "dropoff_borough",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_duration_minutes",
        "fare_per_mile",
        "pickup_h3",
        "dropoff_h3",
        "_ingest_ts",
    )
    write_delta(df_fact, gold_fact_path)

    print(f"Creating Gold Aggregation (Daily Revenue) at {gold_daily_path}...")
    df_daily = (
        df_silver.withColumn("trip_date", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
        .groupBy("trip_date", "vendor_id")
        .agg(
            count("trip_id").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance"),
        )
        .orderBy("trip_date")
    )
    write_delta(df_daily, gold_daily_path)

    print(f"Creating Gold Aggregation (Hourly Demand) at {gold_hourly_path}...")
    df_hourly = (
        df_silver.withColumn("pickup_hour", date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:00:00"))
        .groupBy("pickup_hour", "pickup_h3", "pickup_zone")
        .agg(
            count("trip_id").alias("trip_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
        )
        .orderBy("pickup_hour")
    )
    write_delta(df_hourly, gold_hourly_path)

    print(f"Creating Gold Dimension (Payment) at {gold_payment_dim_path}...")
    df_payment = df_silver.select("payment_type").distinct().withColumn(
        "payment_description",
        when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Other"),
    )
    write_delta(df_payment, gold_payment_dim_path)

    print(f"Creating Gold Dimension (Zone) at {gold_zone_dim_path}...")
    df_zone = (
        df_silver.select(
            col("PULocationID").alias("location_id"),
            col("pickup_zone").alias("zone_name"),
            col("pickup_borough").alias("borough"),
            col("pickup_h3").alias("zone_h3"),
        )
        .unionByName(
            df_silver.select(
                col("DOLocationID").alias("location_id"),
                col("dropoff_zone").alias("zone_name"),
                col("dropoff_borough").alias("borough"),
                col("dropoff_h3").alias("zone_h3"),
            )
        )
        .dropDuplicates(["location_id"])
        .orderBy("location_id")
    )
    write_delta(df_zone, gold_zone_dim_path)

    print("Gold processing complete.")


if __name__ == "__main__":
    main()
