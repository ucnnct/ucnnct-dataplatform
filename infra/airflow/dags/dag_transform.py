"""
DAG transform - Normalisation raw -> curated (5 sources en parallele).

Declenche par dag_collect.
Soumet les jobs Spark via SparkSubmitOperator (spark://spark-master:7077).
Pipeline : dag_collect -> dag_transform -> dag_kpi
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",     "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",    "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
SOURCES        = ["bluesky", "nostr", "hackernews", "rss", "stackoverflow"]

JARS = (
    "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar"
)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="normalisation",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["uconnect", "spark"],
) as dag:

    normalize_tasks = [
        SparkSubmitOperator(
            task_id=f"normalize_{source}",
            application=f"/opt/spark/jobs/normalization/{source}.py",
            conn_id="spark_default",
            jars=JARS,
            verbose=True,
            env_vars={
                "RUN_DATE": "{{ ds.replace('-', '/') }}",
                "MINIO_ENDPOINT": MINIO_ENDPOINT,
                "MINIO_ROOT_USER": MINIO_USER,
                "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            },
        )
        for source in SOURCES
    ]

    trigger_kpi = TriggerDagRunOperator(
        task_id="trigger_kpi",
        trigger_dag_id="chargement-staging",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    normalize_tasks >> trigger_kpi
