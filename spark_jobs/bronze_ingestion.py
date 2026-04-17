import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_avro, lit


schema_path = os.path.join(os.path.dirname(__file__), "..", "producer", "schemas", "trip.avsc")
with open(schema_path, "r", encoding="utf-8") as schema_file:
    avro_schema = schema_file.read()


def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")

    builder = (
        SparkSession.builder.appName("BronzeIngestion")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
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


def write_historical_seed(spark, bronze_path):
    historical_source_path = os.getenv("HISTORICAL_SOURCE_PATH")
    if not historical_source_path:
        print("No HISTORICAL_SOURCE_PATH provided. Skipping parquet bootstrap.")
        return

    print(f"Bootstrapping historical parquet from {historical_source_path} into {bronze_path}...")
    df_historical = (
        spark.read.parquet(historical_source_path)
        .withColumn("trip_id", col("trip_id").cast("string"))
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("record_source", lit("historical_parquet"))
    )
    (
        df_historical.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_broker = os.getenv("KAFKA_BROKER", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "trips.live")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    bronze_path = f"s3a://{s3_bucket}/bronze/trips"
    checkpoint_path = f"s3a://{s3_bucket}/checkpoints/bronze_trips"

    write_historical_seed(spark, bronze_path)

    print(f"Connecting to Kafka at {kafka_broker}, topic {topic}...")
    df_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_stripped = df_kafka.withColumn("fixed_val", col("value").substr(6, 1000000))

    df_parsed = df_stripped.select(
        from_avro(col("fixed_val"), avro_schema).alias("data"),
        col("timestamp").alias("kafka_ts"),
    ).select("data.*", "kafka_ts")

    df_bronze = (
        df_parsed.withColumn("_ingest_ts", current_timestamp())
        .withColumn("record_source", lit("kafka_live"))
    )

    print(f"Starting stream to {bronze_path}...")
    query = (
        df_bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(bronze_path)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
