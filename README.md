# Real-Time Ride-Hailing Lakehouse

An end-to-end Data Engineering portfolio project demonstrating a Medallion Lakehouse Architecture (Bronze/Silver/Gold) using modern tools.

## Architecture Stack
* **Ingestion:** Python + Schema Registry + Kafka (Redpanda)
* **Storage / Lakehouse:** MinIO (S3 compatible) + Delta Lake + Iceberg
* **Processing:** Apache Spark (Structured Streaming & Batch)
* **Orchestration:** Apache Airflow
* **Transformation:** dbt (Data Build Tool)
* **BI:** Apache Superset

---

## 📚 Detailed Documentation
*   **[Architecture Deep-Dive](ARCHITECTURE.md)**: Detailed Mermaid diagrams, data flow, and medallion layer logic.
*   **[Performance Benchmark](BENCHMARK.md)**: Delta Lake vs. Apache Iceberg comparison report.

---

## Hardware Warning
This entire stack is highly resource-intensive. Running all containers at once may require **16GB+ of RAM**. 
If you have 8GB of RAM, it is **highly recommended** to run the stack in separate profiles (tiers) rather than all at once.

---

## How to Run It (Step-by-Step)

### 1. Start the Core Infrastructure (Storage & Streaming)
This tier starts **MinIO (S3)**, **Redpanda (Kafka)**, the **Trip Producer**, and the **Spark Streaming Job** (Bronze Ingestion).

```bash
# Using Makefile
make core-up

# OR using Docker Compose directly
docker-compose --profile core up -d --build
```
> **What's happening?** Data is actively streaming into Kafka and being written in real-time as a Delta table in MinIO (`s3a://raw-taxi-data/bronze/trips`).

### 2. View Data in MinIO (Object Storage)
1. Navigate to: `http://localhost:9001`
2. **Username:** `admin`
3. **Password:** `password123`
4. You should see the `raw-taxi-data` bucket populated with parquet/delta files.

### 3. Run the Silver Batch Processing (Manual or Airflow)
To deduplicate and clean data, you can trigger the Airflow orchestrator:

```bash
docker-compose --profile orchestration up -d --build
```
1. Go to Airflow UI: `http://localhost:8080` (User: `admin` / Password: `admin`)
2. Trigger the `hourly_silver_processing` DAG.

*(If your RAM is maxing out, shut down Airflow after the DAG succeeds: `docker-compose --profile orchestration down`)*

### 4. Serve the Data for Analytics (Trino & Superset)
Once data exists in Silver/Gold tables, you can boot up the serving tier.

```bash
docker-compose --profile serving up -d --build
```
1. Open Superset: `http://localhost:8088`
2. **Username:** `admin` / **Password:** `admin`
3. Connect your Trino database (`trino:8080`) and query the data to build dashboards!

---

### Tearing Everything Down
To stop all services and preserve data:
```bash
make down
# or
docker-compose down
```

To stop all services AND **wipe all data completely**:
```bash
make clean
# or
docker-compose down -v
```
