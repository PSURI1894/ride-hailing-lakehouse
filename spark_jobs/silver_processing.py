import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
import h3

def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    
    builder = SparkSession.builder \
        .appName("SilverProcessing") \
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

# UDF to get H3 hexagon index
def get_h3_index(lat, lon, resolution=9):
    try:
        lat = float(lat)
        lon = float(lon)
        if lat != 0.0 and lon != 0.0:
            return h3.geo_to_h3(lat, lon, resolution)
    except:
        pass
    return None

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # In a real scenario for batch we'd use spark.read, for streaming spark.readStream.
    # The gold layer mentions "Airflow orchestrates batch DAGs + table maintenance" for hourly_silver.
    # So silver processing is an hourly batch job reading from Bronze.

    bronze_path = f"s3a://{s3_bucket}/bronze/trips"
    silver_path = f"s3a://{s3_bucket}/silver/trips"

    print(f"Reading bronze data from {bronze_path}...")
    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        print(f"Failed to read bronze table (maybe empty or not created yet): {e}")
        return

    # Load taxi zone centroids for H3 indexing
    centroids_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'taxi_zone_lookup_coordinates.csv')
    df_centroids = spark.read.option("header", "true").csv(centroids_path) \
        .withColumn("LocationID", col("LocationID").cast("integer")) \
        .withColumn("lat", col("lat").cast("double")) \
        .withColumn("lon", col("lon").cast("double"))

    # Join centroids for Pickup
    df_with_pu = df_bronze.join(df_centroids.alias("pu"), col("PULocationID") == col("pu.LocationID"), "left") \
        .select("*", col("pu.lat").alias("pu_lat"), col("pu.lon").alias("pu_lon")) \
        .drop("LocationID", "lat", "lon")

    # Join centroids for Dropoff
    df_with_geo = df_with_pu.join(df_centroids.alias("do"), col("DOLocationID") == col("do.LocationID"), "left") \
        .select("*", col("do.lat").alias("do_lat"), col("do.lon").alias("do_lon")) \
        .drop("LocationID", "lat", "lon")

    # Register H3 UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    h3_udf = udf(lambda lat, lon: get_h3_index(lat, lon, 9), StringType())

    # Add H3 Hexagons
    df_silver = df_with_geo.withColumn("pickup_h3", h3_udf(col("pu_lat"), col("pu_lon"))) \
        .withColumn("dropoff_h3", h3_udf(col("do_lat"), col("do_lon"))) \
        .filter(col("fare_amount") >= 0) \
        .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
        .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp")) \
        .withColumn("trip_duration_minutes", 
                    (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60.0) \
        .withColumn("fare_per_mile", 
                    when(col("trip_distance") > 0, col("fare_amount") / col("trip_distance")).otherwise(0.0))

    # Deduplication - simple batch dropDuplicates since it's a batch job
    # If it was streaming, we'd use watermarks. The requirements said watermark '10 min' but also Airflow hourly_silver.
    # Let's do batch dedup on trip_id
    df_dedup = df_silver.dropDuplicates(["trip_id"])

    # NOTE: To add H3, we typically need lat/lon, but TLC data provides PULocationID and DOLocationID instead of LAT/LON.
    # If we need H3 resolution 9 for pickup/dropoff, we must join with TLC Taxi Zone lookup which has geometry.
    # But for this pipeline, we will just register it as Silver properly cleaned.

    print(f"Writing silver data to {silver_path}...")
    df_dedup.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(silver_path)

    print("Silver processing complete.")

if __name__ == "__main__":
    main()
