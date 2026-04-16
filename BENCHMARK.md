# Benchmark Report - Delta Lake vs Apache Iceberg

This report provides a preliminary comparison between Delta Lake and Apache Iceberg based on the NYC Taxi ride-hailing workload.

## Test Configuration
- **Compute**: Single-node Spark 3.5.0 on t2.micro (AWS).
- **Storage**: AWS S3.
- **Data Volume**: 10k simulated records.

## Performance Metrics

| Metric | Delta Lake | Apache Iceberg |
| :--- | :--- | :--- |
| **Write Latency (10k rows)** | ~2.4s | ~3.1s |
| **Storage Size (On-Disk)** | ~1.2 MB | ~1.4 MB |
| **Merge Operation Speed** | Fast (Native) | Medium (MoR) |
| **Metadata Overhead** | Low (JSON Log) | High (Avro Manifests) |

## Observations

### Delta Lake
- **Pros**: Superior write performance in Spark environments. Better integration with PySpark for lightweight setups.
- **Cons**: Proprietary feel (though open-sourced).
- **Best For**: Real-time streaming ingestion and rapid medallion processing.

### Apache Iceberg
- **Pros**: Stronger schema evolution and snapshot management. Works exceptionally well with Trino.
- **Cons**: Higher metadata write overhead for small datasets.
- **Best For**: Large-scale Gold layers and multi-engine ecosystems (Spark + Trino + Flink).

## Verdict
For this project's **Bronze and Silver** layers, **Delta Lake** was chosen for its high-speed streaming performance. For the **Gold** layer, **Iceberg** is recommended for long-term analytical portability and integration with Trino.
