import os
import time
import json
import pandas as pd
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:19092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = os.getenv("KAFKA_TOPIC", "trips.live")

# Download a sample of TLC trip data (yellow taxi, Jan 2024 for example)
# Since large files could be slow, we fetch a few rows or you can use local parquet.
SAMPLE_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed for trip record {msg.key()}: {err}")
    else:
        logger.debug(f"Trip record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def load_schema(schema_path):
    with open(schema_path, "r") as f:
        schema_str = f.read()
    return schema_str

def main():
    logger.info("Initializing Schema Registry Client...")
    schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    schema_path = os.path.join(os.path.dirname(__file__), '..', 'schemas', 'trip.avsc')
    value_schema_str = load_schema(schema_path)

    # For keys, we'll just use a String serializer
    from confluent_kafka.serialization import StringSerializer
    key_serializer = StringSerializer('utf_8')
    
    logger.info("Registering Avro Schema...")
    avro_serializer = AvroSerializer(
        schema_registry_client,
        value_schema_str
    )

    producer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'key.serializer': key_serializer,
        'value.serializer': avro_serializer,
        'linger.ms': 10,
        'batch.size': 32768,
        'compression.type': 'lz4'
    }

    producer = SerializingProducer(producer_conf)

    logger.info(f"Downloading sample NYC taxi dataset from {SAMPLE_DATA_URL}...")
    try:
        # Load just top 10k rows to save memory
        df = pd.read_parquet(SAMPLE_DATA_URL).head(10000)
        logger.info(f"Loaded {len(df)} rows.")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return

    # Clean up column names to match schema if necessary and handle nulls
    # Rename columns if needed and cast
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].astype(str)
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].astype(str)
    df = df.where(pd.notnull(df), None)

    logger.info(f"Streaming data to Kafka topic '{TOPIC}'...")

    for index, row in df.iterrows():
        # Create dictionary explicitly matching Avro schema
        record = {
            "trip_id": f"trip_{index}_{int(time.time())}",
            "vendor_id": int(row.get('VendorID') or 0),
            "tpep_pickup_datetime": str(row.get('tpep_pickup_datetime')),
            "tpep_dropoff_datetime": str(row.get('tpep_dropoff_datetime')),
            "passenger_count": int(row.get('passenger_count')) if pd.notna(row.get('passenger_count')) else None,
            "trip_distance": float(row.get('trip_distance') or 0.0),
            "RatecodeID": int(row.get('RatecodeID')) if pd.notna(row.get('RatecodeID')) else None,
            "store_and_fwd_flag": str(row.get('store_and_fwd_flag')) if pd.notna(row.get('store_and_fwd_flag')) else None,
            "PULocationID": int(row.get('PULocationID') or 0),
            "DOLocationID": int(row.get('DOLocationID') or 0),
            "payment_type": int(row.get('payment_type') or 0),
            "fare_amount": float(row.get('fare_amount') or 0.0),
            "extra": float(row.get('extra') or 0.0),
            "mta_tax": float(row.get('mta_tax') or 0.0),
            "tip_amount": float(row.get('tip_amount') or 0.0),
            "tolls_amount": float(row.get('tolls_amount') or 0.0),
            "improvement_surcharge": float(row.get('improvement_surcharge') or 0.0),
            "total_amount": float(row.get('total_amount') or 0.0),
            "congestion_surcharge": float(row.get('congestion_surcharge')) if pd.notna(row.get('congestion_surcharge')) else None,
            "airport_fee": float(row.get('Airport_fee')) if pd.notna(row.get('Airport_fee')) else None
        }

        try:
            producer.produce(
                topic=TOPIC,
                key=str(record['trip_id']),
                value=record,
                on_delivery=delivery_report
            )
            
            # Flush periodically to not build up too much in memory
            if index % 1000 == 0:
                producer.poll(0)
                logger.info(f"Produced {index} records...")
                
            # Sleep small amount to simulate streaming
            time.sleep(0.01)

        except Exception as e:
            logger.error(f"Error producing record: {e}")
            break

    logger.info("Flushing final records...")
    producer.flush()
    logger.info("Done streaming!")

if __name__ == "__main__":
    main()
