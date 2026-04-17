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
    "retry_delay": timedelta(minutes=5),
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
    "hourly_silver_processing",
    default_args=default_args,
    description="Run Silver cleaning and DQ every hour",
    schedule="@hourly",
    catchup=False,
    tags=["silver", "dq"],
) as dag:
    run_silver = DockerOperator(
        task_id="run_silver_spark_job",
        image="jupyter/pyspark-notebook:spark-3.5.0",
        api_version="auto",
        auto_remove="force",
        command="python /app/spark_jobs/silver_processing.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="ride-hailing-lakehouse_default",
        mounts=[project_mount],
        environment=common_env,
    )

    run_dq = DockerOperator(
        task_id="validate_silver_quality",
        image="jupyter/pyspark-notebook:spark-3.5.0",
        api_version="auto",
        auto_remove="force",
        command="python /app/spark_jobs/dq_checks.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="ride-hailing-lakehouse_default",
        mounts=[project_mount],
        environment=common_env,
    )

    run_silver >> run_dq
