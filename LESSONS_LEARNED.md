# Lessons Learned

## Building a Medallion Lakehouse Without a Cloud Budget

The most important lesson from this project is that architecture skill is not the same thing as cloud spend. A strong Data Engineering portfolio project should prove design judgment, reliability thinking, and data modeling depth even when the runtime is intentionally cheap.

## What Worked Well
- MinIO let the project stay S3-compatible without paying for persistent AWS storage.
- Separating Bronze, Silver, and Gold layers made the pipeline easier to debug and explain.
- H3 enrichment added a genuinely interesting analytics angle instead of a generic taxi pipeline.
- Airflow plus Great Expectations created a stronger production story than a single Spark script.

## What I Changed After Early Iterations
- Removed the assumption that AWS should host the full stack all the time.
- Hardened Terraform so storage is not destroyed during compute teardown.
- Shifted the project to local-first operation with AWS as an optional demo path.
- Tightened Silver and Gold transformations to better match interview expectations.

## Trade-Offs
- The dataset does not expose a reliable driver grain, so a true `dim_driver SCD2` model is not realistic without inventing data.
- Weekly compaction is documented rather than fully automated because heavy maintenance jobs do not fit the $0 operating model.
- The benchmark is intentionally lightweight and should be described honestly as a portfolio-scale comparison, not a production benchmark suite.

## How I Would Extend This With Budget
- Replace MinIO with durable S3 plus lifecycle policies
- Add a metastore that cleanly supports both Delta and Iceberg catalogs
- Run scheduled jobs on serverless or spot-friendly compute
- Add Slack alerting and richer observability
