# Architecture - Real-Time Ride-Hailing Lakehouse

This project is intentionally designed as a local-first, S3-compatible medallion lakehouse so it can be demonstrated at zero dollars while still matching the shape of a production Data Engineering platform.

## System Diagram

```mermaid
graph LR
    subgraph "Ingestion"
        Producer["Python Producer"] --> Kafka["Redpanda / Kafka"]
        Historic["Historic TLC Parquet"] --> Bronze
    end

    subgraph "Lakehouse Storage"
        Kafka --> Bronze["Bronze Delta"]
        Bronze --> Silver["Silver Delta"]
        Silver --> DQ["Great Expectations"]
        DQ --> Gold["Gold Delta / dbt Models"]
    end

    subgraph "Orchestration"
        Airflow["Airflow DAGs"] --> Silver
        Airflow --> Gold
        Airflow --> Maint["Weekly Maintenance"]
    end

    subgraph "Serving"
        Gold --> Trino["Trino"]
        Trino --> Superset["Superset Dashboards"]
    end

    subgraph "Deployment Modes"
        MinIO["MinIO (default)"] -. S3-compatible .- Bronze
        S3["AWS S3 (optional)"] -. S3-compatible .- Bronze
        EC2["Ephemeral EC2 (optional)"] --> Airflow
    end
```

## Why This Architecture Fits a $0 Goal
- Object storage is the system of record.
- Compute is disposable.
- Development happens locally with MinIO instead of paid S3.
- The code paths remain S3-compatible, so the same jobs can target AWS only when needed for a demo.

## Data Layers

### Bronze
- Inputs: Kafka live stream plus optional historic parquet bootstrap
- Format: Delta Lake
- Key fields: source payload, `kafka_ts`, `_ingest_ts`, `record_source`
- Goal: immutable raw landing zone

### Silver
- Deduplicates by `(trip_id, tpep_pickup_datetime)` using latest ingestion timestamp
- Rejects negative fares and negative trip durations
- Computes `trip_duration_minutes` and `fare_per_mile`
- Enriches pickup and dropoff zones with H3 resolution 9

### Gold
- `fact_trips`
- `agg_daily_revenue`
- `agg_hourly_demand`
- `dim_payment`
- `dim_zone`

## Orchestration
- `hourly_silver_processing`: run Silver transforms and DQ checks
- `daily_gold_modeling`: build serving tables
- `weekly_compaction`: lightweight placeholder for maintenance strategy documentation

## Data Quality Boundaries
- Bronze to Silver: schema expectations and null checks
- Silver to Gold: uniqueness on business keys, fare and duration ranges, H3 format checks
- Airflow is expected to fail fast when DQ breaks

## Data Model ERD

```mermaid
erDiagram
    FACT_TRIPS {
        string trip_id PK
        int vendor_id
        timestamp tpep_pickup_datetime
        timestamp tpep_dropoff_datetime
        int pickup_location_id
        int dropoff_location_id
        int payment_type
        double total_amount
        double fare_amount
        string pickup_h3
        string dropoff_h3
    }

    DIM_PAYMENT {
        int payment_type PK
        string payment_description
    }

    DIM_ZONE {
        int location_id PK
        string zone_name
        string borough
        string zone_h3
    }

    FACT_TRIPS }o--|| DIM_PAYMENT : "payment_type"
    FACT_TRIPS }o--|| DIM_ZONE : "pickup_location_id"
    FACT_TRIPS }o--|| DIM_ZONE : "dropoff_location_id"
```

## Interview Talking Points
- Exactly-once semantics come from Structured Streaming checkpoints plus Delta transactions.
- H3 is used instead of geohash because hexagons reduce directional bias and work well for urban demand heatmaps.
- Delta is a better fit for lightweight Bronze and Silver write-heavy paths.
- Iceberg remains relevant for multi-engine portability, but this repo stays local-first and keeps the Gold layer simple enough to run cheaply.
