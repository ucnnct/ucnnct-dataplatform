"""
DAG chargement_staging - curated Parquet (MinIO) -> ClickHouse staging.

Déclenché par dag_transform.
Déclenche calcul_kpi à la fin.

Pipeline : dag_collect -> normalisation -> chargement_staging -> calcul_kpi
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "uconnect")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="chargement_staging",
    dag_display_name="Chargement en Staging",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["uconnect"],
    params={
        "date_debut": Param(
            None,
            type=["null", "string"],
            description="Borne inférieure event_ts (YYYY-MM-DD HH:MM:SS). Vide = watermark automatique.",
        ),
        "date_fin": Param(
            None,
            type=["null", "string"],
            description="Borne supérieure event_ts (YYYY-MM-DD HH:MM:SS). Vide = pas de borne.",
        ),
        "sources": Param(
            None,
            type=["null", "string"],
            description="Sources à charger séparées par virgule (ex: bluesky,nostr). Vide = toutes.",
        ),
    },
) as dag:

    curated_to_staging = BashOperator(
        task_id="curated_to_staging",
        task_display_name="Curated → ClickHouse",
        bash_command="python3 /opt/airflow/jobs/curated_to_staging.py",
        env={
            "CLICKHOUSE_HOST": CH_HOST,
            "CLICKHOUSE_PORT": CH_PORT,
            "CLICKHOUSE_USER": CH_USER,
            "CLICKHOUSE_PASSWORD": CH_PASSWORD,
            "CLICKHOUSE_DB": CH_DB,
            "MINIO_ENDPOINT": MINIO_ENDPOINT,
            "MINIO_ROOT_USER": MINIO_USER,
            "MINIO_ROOT_PASSWORD": MINIO_PASSWORD,
            "MINIO_BUCKET": "datalake",
            "LOAD_DATE_DEBUT": "{{ params.date_debut or '' }}",
            "LOAD_DATE_FIN": "{{ params.date_fin or '' }}",
            "LOAD_SOURCES": "{{ params.sources or '' }}",
            "LOG_FORMAT": "json",
            "LOG_LEVEL": "INFO",
        },
    )

    trigger_calcul_kpi = TriggerDagRunOperator(
        task_id="trigger_calcul_kpi",
        task_display_name="Déclencher Calcul KPI",
        trigger_dag_id="calcul_kpi",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    curated_to_staging >> trigger_calcul_kpi
