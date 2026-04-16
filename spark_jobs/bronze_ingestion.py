import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_avro, current_timestamp
from pyspark.sql.types import StringType

# Read Avro schema to use with from_avro
schema_path = os.path.join(os.path.dirname(__file__), '..', 'producer', 'schemas', 'trip.avsc')
with open(schema_path, 'r') as f:
    avro_schema = f.read()

def create_spark_session():
    run_mode = os.getenv("RUN_MODE", "local")
    s3_bucket = os.getenv("S3_BUCKET", "raw-taxi-data")
    
    builder = SparkSession.builder \
        .appName("BronzeIngestion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

    if run_mode == "cloud":
        # In the cloud we use default AWS S3 endpoint and IAM role
        print(f"Running in CLOUD mode, targeting S3 Bucket: {s3_bucket}")
    else:
        # Locally we use MinIO
        print("Running in LOCAL mode, targeting MinIO")
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "password123")

    return builder.getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_broker = os.getenv("KAFKA_BROKER", "redpanda:9092")
    topic = os.getenv("KAFKA_TOPIC", "trips.live")
    
    # Wait for Kafka to be ready logic could be handled outside, but let's just connect
    print(f"Connecting to Kafka at {kafka_broker}, topic {topic}...")

    # Read from Kafka streaming
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # The value is avro, but since we used Schema Registry, the first 5 bytes are Confluent magic byte + schema ID.
    # To decode it purely without Abris in PySpark, we skip the first 5 bytes.
    # A cleaner approach in open source is substring the binary.
    
    # Strip magic bytes (1 byte magic, 4 bytes id)
    df_stripped = df_kafka.withColumn("fixed_val", col("value").substr(6, 1000000))
    
    df_parsed = df_stripped.select(
        from_avro(col("fixed_val"), avro_schema).alias("data"),
        col("timestamp").alias("kafka_ts")
    ).select("data.*", "kafka_ts")

    # Add ingestion timestamp
    df_bronze = df_parsed.withColumn("_ingest_ts", current_timestamp())

    # Write to Delta Bronze layer
    bronze_path = f"s3a://{s3_bucket}/bronze/trips"
    checkpoint_path = f"s3a://{s3_bucket}/checkpoints/bronze_trips"

    print(f"Starting stream to {bronze_path}...")
    
    query = df_bronze.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(bronze_path)

    query.awaitTermination()

if __name__ == "__main__":
    main()
