import os
import sys
from pyspark.sql import SparkSession
import great_expectations as ge
from great_expectations.dataset import SparkDFDataset

def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    
    builder = SparkSession.builder \
        .appName("DataQualityChecks") \
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

    print(f"Reading silver data for DQ checks from {silver_path}...")
    try:
        df_silver = spark.read.format("delta").load(silver_path)
    except Exception as e:
        print(f"Failed to read silver table: {e}")
        sys.exit(1)

    # Convert to GE Spark Dataset
    gdf = SparkDFDataset(df_silver)

    print("Running Expectations...")
    
    # 1. Column existence
    gdf.expect_column_to_exist("trip_id")
    gdf.expect_column_to_exist("vendor_id")
    
    # 2. Null checks
    gdf.expect_column_values_to_not_be_null("tpep_pickup_datetime")
    
    # 3. Value ranges
    gdf.expect_column_values_to_be_between("passenger_count", min_value=0, max_value=10)
    gdf.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=1000)
    
    # 4. H3 format check (must be a valid hex string of length 15)
    gdf.expect_column_values_to_match_regex("pickup_h3", r"^[0-9a-f]{15}$")

    # Final validation results
    results = gdf.validate()
    
    if results["success"]:
        print("✅ Data Quality Checks PASSED!")
    else:
        print("❌ Data Quality Checks FAILED!")
        # In a real pipeline, we might sys.exit(1) here to stop the flow
        # For this demo, we'll just log the failure
        # sys.exit(1)

if __name__ == "__main__":
    main()
