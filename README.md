# Real-Time Ride-Hailing Lakehouse

Local-first, zero-dollar portfolio implementation of a medallion lakehouse for NYC taxi trips. The stack is built to show senior-level Data Engineering concepts without requiring a permanent AWS bill.

## What This Project Demonstrates
- Streaming ingestion with a Python producer and Redpanda/Kafka
- Bronze/Silver/Gold lakehouse modeling on S3-compatible object storage
- Spark Structured Streaming plus batch transformations
- H3 spatial enrichment for heatmaps and demand analysis
- Data quality gates with Great Expectations
- Airflow DAGs for hourly, daily, and weekly orchestration
- dbt serving models for BI-friendly dimensions and facts
- Trino and Superset integration for analytics
- Terraform for optional ephemeral cloud compute

## Zero-Dollar Design
- Day-to-day development runs locally with Docker Compose and MinIO.
- The implementation is S3-compatible, so MinIO stands in for S3 during development.
- AWS is optional and intentionally ephemeral: short-lived EC2, durable S3, no always-on services.
- Terraform is hardened so lakehouse storage is not destroyed by accident.

## Repo Layout
- [producer](C:\Users\Hp\Desktop\ride-hailing-lakehouse\producer): Avro producer that replays NYC trips into Kafka
- [spark_jobs](C:\Users\Hp\Desktop\ride-hailing-lakehouse\spark_jobs): Bronze, Silver, Gold, and DQ jobs
- [airflow/dags](C:\Users\Hp\Desktop\ride-hailing-lakehouse\airflow\dags): Orchestration layer
- [dbt_project](C:\Users\Hp\Desktop\ride-hailing-lakehouse\dbt_project): BI-facing models
- [infrastructure](C:\Users\Hp\Desktop\ride-hailing-lakehouse\infrastructure): Terraform and Trino config
- [data_quality](C:\Users\Hp\Desktop\ride-hailing-lakehouse\data_quality): Soda-style reference checks

## Quick Start

### 1. Start the core stack
```bash
make core-up
```

This launches:
- MinIO for S3-compatible lakehouse storage
- Redpanda plus Schema Registry
- Kafka producer
- Spark bronze ingestion job

### 2. Run Silver and Gold manually
```bash
docker-compose --profile orchestration up -d --build
```

Then use Airflow at `http://localhost:8080`:
- `hourly_silver_processing`
- `daily_gold_modeling`
- `weekly_compaction`

### 3. Explore the serving layer
```bash
docker-compose --profile serving up -d --build
```

Then open:
- MinIO: `http://localhost:9001`
- Trino: `http://localhost:8080`
- Superset: `http://localhost:8088`

## Current Deliverables
- Architecture deep-dive: [ARCHITECTURE.md](C:\Users\Hp\Desktop\ride-hailing-lakehouse\ARCHITECTURE.md)
- Cost strategy: [COST.md](C:\Users\Hp\Desktop\ride-hailing-lakehouse\COST.md)
- Runbook: [RUNBOOK.md](C:\Users\Hp\Desktop\ride-hailing-lakehouse\RUNBOOK.md)
- Delta vs Iceberg benchmark: [BENCHMARK.md](C:\Users\Hp\Desktop\ride-hailing-lakehouse\BENCHMARK.md)

## Important Constraints
- The source dataset does not contain a real driver grain, so the gold layer focuses on `fact_trips`, `dim_payment`, and `dim_zone`.
- Weekly compaction is documented but intentionally not automated into a heavy compute path, because this repository is optimized for a $0 local-first workflow.
- For a cloud demo, keep EC2 temporary and treat S3 as the only persistent layer.
