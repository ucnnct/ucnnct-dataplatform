"""
DAG collect - Verification disponibilite donnees brutes MinIO.
Déclenché par horaire (ex: toutes les heures).
Vérifie que les données brutes du jour sont présentes dans MinIO.
Si oui, déclenche dag_transform.
Pipeline : dag_collect -> dag_transform --> chargement en Staging -> dag_kpi
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
SOURCES = ["bluesky", "nostr", "hackernews", "rss", "stackoverflow"]


def check_raw_data(source: str, **ctx):
    import boto3

    run_date = ctx["ds"].replace("-", "/")
    prefix = f"raw/{source}/{run_date}/"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )
    resp = s3.list_objects_v2(Bucket="datalake", Prefix=prefix, MaxKeys=1)
    if resp.get("KeyCount", 0) == 0:
        raise AirflowException(f"Pas de donnees brutes {source}/{run_date}")


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="verification-sources",
    schedule_interval="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["uconnect"],
) as dag:

    checks = [
        PythonOperator(
            task_id=f"check_{source}",
            python_callable=check_raw_data,
            op_kwargs={"source": source},
        )
        for source in SOURCES
    ]

    trigger = TriggerDagRunOperator(
        task_id="trigger_transform",
        trigger_dag_id="normalisation",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    checks >> trigger
