from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

ENV = {
    "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    "MINIO_ACCESS_KEY": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
    "MINIO_SECRET_KEY": os.environ.get("MINIO_SECRET_KEY", "minioadminpassword"),
    "SPARK_LOCAL_IP": "127.0.0.1",
}

SPARK_APPS_DIR = "/opt/airflow/spark_apps"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flight_delays_medallion_pipeline",
    default_args=default_args,
    description="Medallion Pipeline: Bronze -> Silver -> Gold cho phân tích chậm chuyến bay",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["medallion", "flight-delays", "etl"],
) as dag:

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command=f"cd {SPARK_APPS_DIR} && python ingest_to_bronze.py",
        env=ENV,
    )

    transform_to_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command=f"cd {SPARK_APPS_DIR} && python transform_to_silver.py",
        env=ENV,
    )

    transform_to_gold = BashOperator(
        task_id="transform_to_gold",
        bash_command=f"cd {SPARK_APPS_DIR} && python transform_to_gold.py",
        env=ENV,
    )

    register_tables = BashOperator(
        task_id="register_trino_tables",
        bash_command=f"cd {SPARK_APPS_DIR} && python register_trino_tables.py",
        env={**ENV, "TRINO_HOST": "trino", "TRINO_PORT": "8080"},
    )

    ingest_to_bronze >> transform_to_silver >> transform_to_gold >> register_tables
