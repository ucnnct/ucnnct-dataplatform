"""
DAG kpi - Chargement curated -> staging.events PostgreSQL.

Declenche par dag_transform.
  load_postgres  -> charge staging.events depuis curated Parquet (SparkSubmitOperator)
Pipeline : dag_collect -> dag_transform -> dag_kpi
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
PG_HOST = os.getenv("POSTGRES_HOST", "172.31.253.25")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "uconnect")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

JARS = (
    "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,"
    "/opt/spark/jars/postgresql-42.7.3.jar"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="chargement-staging",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["uconnect"],
) as dag:

    SparkSubmitOperator(
        task_id="load_postgres",
        application="/opt/spark/jobs/load_postgres.py",
        conn_id="spark_default",
        jars=JARS,
        verbose=True,
        env_vars={
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "POSTGRES_HOST": PG_HOST,
            "POSTGRES_PORT": PG_PORT,
            "POSTGRES_DB": PG_DB,
            "POSTGRES_USER": PG_USER,
            "POSTGRES_PASSWORD": PG_PASSWORD,
        },
    )
