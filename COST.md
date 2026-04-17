# COST.md

## Goal
Keep the project demonstrable at effectively $0 by making local compute the default and treating AWS as optional, short-lived showcase infrastructure.

## Cost Strategy
- Default runtime: local Docker Compose on a laptop
- Object storage: MinIO for daily work, S3 only for optional demos
- Cloud compute: ephemeral EC2 only, never permanent
- BI and orchestration: local only
- Storage hygiene: keep raw, checkpoints, and benchmark artifacts small

## What Must Never Run 24/7 in AWS
- Redpanda / Kafka
- Airflow
- Spark streaming jobs
- Trino
- Superset

## Safe AWS Usage Pattern
1. Create or reuse an S3 bucket.
2. Start a short-lived EC2 instance only for a demo run.
3. Execute ingestion and processing.
4. Terminate EC2.
5. Keep only the curated output needed for the demo.

## Why Previous Terraform Was Dangerous
- `force_destroy = true` allowed bucket deletion even when data existed.
- No versioning meant deleted objects were not recoverable.
- No `prevent_destroy` meant the bucket could be destroyed as part of a full teardown.

## Current Protections
- S3 versioning enabled
- `prevent_destroy = true` on the bucket
- Bucket deletion requires explicit opt-in via `allow_bucket_destroy`
- GitHub workflow now destroys compute resources only, not storage

## Recruiter-Friendly Story
This project is intentionally designed to prove architecture judgment under constraints:
- same medallion concepts as a paid cloud platform
- implemented on S3-compatible local infrastructure
- optional cloud execution path for demos
- no dependency on a large budget
