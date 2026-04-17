import os

import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, to_timestamp, when
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")

    builder = (
        SparkSession.builder.appName("SilverProcessing")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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


def get_h3_index(lat, lon, resolution=9):
    try:
        lat = float(lat)
        lon = float(lon)
        if lat != 0.0 and lon != 0.0:
            return h3.latlng_to_cell(lat, lon, resolution)
    except Exception:
        return None
    return None


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    bronze_path = f"s3a://{s3_bucket}/bronze/trips"
    silver_path = f"s3a://{s3_bucket}/silver/trips"

    print(f"Reading bronze data from {bronze_path}...")
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as exc:
        print(f"Failed to read bronze table (maybe empty or not created yet): {exc}")
        return

    centroids_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "taxi_zone_lookup_coordinates.csv"
    )
    df_centroids = (
        spark.read.option("header", "true")
        .csv(centroids_path)
        .withColumn("LocationID", col("LocationID").cast("integer"))
        .withColumn("lat", col("lat").cast("double"))
        .withColumn("lon", col("lon").cast("double"))
        .withColumnRenamed("Zone", "zone_name")
        .withColumnRenamed("Borough", "borough")
    )

    from pyspark.sql.functions import udf

    h3_udf = udf(lambda lat, lon: get_h3_index(lat, lon, 9), StringType())

    df_typed = (
        df_bronze.withColumn("vendor_id", col("vendor_id").cast("int"))
        .withColumn("RatecodeID", col("RatecodeID").cast("int"))
        .withColumn("PULocationID", col("PULocationID").cast("int"))
        .withColumn("DOLocationID", col("DOLocationID").cast("int"))
        .withColumn("payment_type", col("payment_type").cast("int"))
        .withColumn("fare_amount", col("fare_amount").cast("double"))
        .withColumn("tip_amount", col("tip_amount").cast("double"))
        .withColumn("total_amount", col("total_amount").cast("double"))
        .withColumn("trip_distance", col("trip_distance").cast("double"))
        .withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime"))
        .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime"))
    )

    df_with_pu = df_typed.join(
        df_centroids.alias("pu"), col("PULocationID") == col("pu.LocationID"), "left"
    ).select(
        df_typed["*"],
        col("pu.zone_name").alias("pickup_zone"),
        col("pu.borough").alias("pickup_borough"),
        col("pu.lat").alias("pu_lat"),
        col("pu.lon").alias("pu_lon"),
    )

    df_with_geo = df_with_pu.join(
        df_centroids.alias("do"), col("DOLocationID") == col("do.LocationID"), "left"
    ).select(
        df_with_pu["*"],
        col("do.zone_name").alias("dropoff_zone"),
        col("do.borough").alias("dropoff_borough"),
        col("do.lat").alias("do_lat"),
        col("do.lon").alias("do_lon"),
    )

    df_enriched = (
        df_with_geo.withColumn("pickup_h3", h3_udf(col("pu_lat"), col("pu_lon")))
        .withColumn("dropoff_h3", h3_udf(col("do_lat"), col("do_lon")))
        .withColumn(
            "trip_duration_minutes",
            (
                col("tpep_dropoff_datetime").cast("long")
                - col("tpep_pickup_datetime").cast("long")
            )
            / 60.0,
        )
        .withColumn(
            "fare_per_mile",
            when(
                col("trip_distance") > 0, col("fare_amount") / col("trip_distance")
            ).otherwise(None),
        )
        .filter(col("fare_amount") >= 0)
        .filter(col("trip_duration_minutes") >= 0)
    )

    # Batch-friendly approximation of the requested watermark dedupe:
    # keep the latest ingested row for each (trip_id, pickup timestamp) pair.
    dedupe_window = Window.partitionBy("trip_id", "tpep_pickup_datetime").orderBy(
        col("_ingest_ts").desc_nulls_last()
    )
    df_silver = (
        df_enriched.withColumn("_row_num", row_number().over(dedupe_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    print(f"Writing silver data to {silver_path}...")
    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    print("Silver processing complete.")


if __name__ == "__main__":
    main()
