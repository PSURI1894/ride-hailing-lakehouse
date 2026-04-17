# RUNBOOK.md

## Purpose
Operational guide for running, validating, and explaining the ride-hailing lakehouse in a zero-dollar local-first mode.

## Prerequisites
- Docker Desktop
- 8GB RAM minimum, 16GB preferred
- Make

## Happy Path Demo

### 1. Start core services
```bash
make core-up
```

Expected outcome:
- Redpanda is reachable
- MinIO buckets are created
- The trip producer begins publishing
- The Bronze Spark job begins writing Delta files

### 2. Confirm Bronze landing
- Open MinIO at `http://localhost:9001`
- Check bucket `raw-taxi-data`
- Inspect `bronze/trips` and `checkpoints/bronze_trips`

### 3. Run orchestration tier
```bash
docker-compose --profile orchestration up -d --build
```

Expected outcome:
- Airflow webserver available at `http://localhost:8080`
- DAGs visible:
  - `hourly_silver_processing`
  - `daily_gold_modeling`
  - `weekly_compaction`

### 4. Trigger DAGs
Run in this order:
1. `hourly_silver_processing`
2. `daily_gold_modeling`

Expected outcome:
- Silver Delta data appears under `silver/trips`
- Gold outputs appear under `gold/fact_trips`, `gold/agg_daily_revenue`, `gold/agg_hourly_demand`, `gold/dim_payment`, and `gold/dim_zone`

### 5. Open serving tier
```bash
docker-compose --profile serving up -d --build
```

Expected outcome:
- Trino UI/API available on `http://localhost:8090`
- Superset available on `http://localhost:8088`

## Troubleshooting

### Bronze has no data
- Check producer logs
- Check Schema Registry availability on port `8081`
- Confirm Redpanda is healthy

### Silver fails
- Check whether Bronze Delta files exist first
- Confirm the taxi zone CSV is mounted and readable
- Review `spark_jobs/silver_processing.py`

### DQ fails
- Inspect null pickup timestamps
- Check for malformed H3 values
- Review duplicate `(trip_id, tpep_pickup_datetime)` pairs

### Gold is empty
- Confirm Silver ran successfully
- Review `spark_jobs/gold_aggregations.py`

## Manual Validation Checklist
- Bronze records contain `_ingest_ts`
- Silver records contain `pickup_h3`, `dropoff_h3`, `trip_duration_minutes`, `fare_per_mile`
- DQ step fails on broken Silver data
- Gold contains fact and dimension-style outputs
- Project can be explained as local-first and S3-compatible

## Optional AWS Demo Mode
- Keep S3 durable
- Start EC2 only for the demo run
- Upload only minimal output
- Terminate EC2 immediately after the run
- Never run the full serving stack on AWS for this portfolio project
