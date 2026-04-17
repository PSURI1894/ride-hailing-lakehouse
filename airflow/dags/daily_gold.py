import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

project_mount = Mount(
    source=os.environ.get("HOST_PROJECT_PATH", "/tmp"),
    target="/app",
    type="bind",
)

common_env = {
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password123",
    "S3_BUCKET": os.environ.get("S3_BUCKET", "raw-taxi-data"),
}


with DAG(
    "daily_gold_modeling",
    default_args=default_args,
    description="Build Gold serving tables once per day",
    schedule="@daily",
    catchup=False,
    tags=["gold", "serving"],
) as dag:
    run_gold = DockerOperator(
        task_id="run_gold_aggregations",
        image="jupyter/pyspark-notebook:spark-3.5.0",
        api_version="auto",
        auto_remove="force",
        command="python /app/spark_jobs/gold_aggregations.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="ride-hailing-lakehouse_default",
        mounts=[project_mount],
        environment=common_env,
    )

    run_gold
