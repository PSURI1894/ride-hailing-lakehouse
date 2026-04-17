from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "lakehouse_admin",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "medallion_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Medallion Pipeline (Bronze -> Silver -> Gold)",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=["spark", "lakehouse", "medallion"],
) as dag:

    # 1. Bronze Ingestion (Streaming is usually 24/7, but we can trigger it)
    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion_trigger",
        bash_command="docker exec spark-bronze-ingestion python bronze_ingestion.py --timeout 600",
    )

    # 2. Silver Processing (Batch Cleaning)
    silver_processing = BashOperator(
        task_id="silver_cleaning_dq",
        bash_command="docker exec spark-bronze-ingestion python silver_processing.py",
    )

    # 3. Data Quality Check
    dq_validation = BashOperator(
        task_id="data_quality_validation",
        bash_command="docker exec spark-bronze-ingestion python dq_checks.py",
    )

    # 4. Gold Aggregation (Reporting)
    gold_aggregation = BashOperator(
        task_id="gold_reporting_tables",
        bash_command="docker exec spark-bronze-ingestion python gold_aggregations.py",
    )

    # Define Dependency Flow
    bronze_ingestion >> silver_processing >> dq_validation >> gold_aggregation
