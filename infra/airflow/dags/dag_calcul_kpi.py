"""
DAG calcul_kpi - Calcul des KPIs (ClickHouse -> PostgreSQL datamart).

Déclenché par chargement_staging.

Pipeline : Verifier que nos donnees de raw son toujours present -> 
Nettoyage et Detournement  -> chargement en staging -> Calcul des KPIs
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT",     "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",    "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")

CH_HOST     = os.getenv("CLICKHOUSE_HOST",     "localhost")
CH_PORT     = os.getenv("CLICKHOUSE_PORT",     "8123")
CH_USER     = os.getenv("CLICKHOUSE_USER",     "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB       = os.getenv("CLICKHOUSE_DB",       "uconnect")

PG_HOST     = os.getenv("POSTGRES_HOST",     "172.31.253.25")
PG_PORT     = os.getenv("POSTGRES_PORT",     "5432")
PG_DB       = os.getenv("POSTGRES_DB",       "uconnect")
PG_USER     = os.getenv("POSTGRES_USER",     "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

KPI_ENV = {
    "CLICKHOUSE_HOST":     CH_HOST,
    "CLICKHOUSE_PORT":     CH_PORT,
    "CLICKHOUSE_USER":     CH_USER,
    "CLICKHOUSE_PASSWORD": CH_PASSWORD,
    "CLICKHOUSE_DB":       CH_DB,
    "POSTGRES_HOST":       PG_HOST,
    "POSTGRES_PORT":       PG_PORT,
    "POSTGRES_DB":         PG_DB,
    "POSTGRES_USER":       PG_USER,
    "POSTGRES_PASSWORD":   PG_PASSWORD,
    "POSTGRES_SCHEMA":     "datamart",
    "MINIO_ENDPOINT":      MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY":    MINIO_USER,
    "MINIO_SECRET_KEY":    MINIO_PASSWORD,
    "MINIO_BUCKET":        "datalake",
    "LOG_FORMAT":          "json",
    "LOG_LEVEL":           "INFO",
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="calcul_kpi",
    dag_display_name="Calcul des KPI",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["uconnect"],
) as dag:

    kpi1 = BashOperator(
        task_id="kpi1_utilisation",
        task_display_name="KPI 1 — Utilisation",
        bash_command="python3 /opt/airflow/jobs/kpi/kpi1_utilisation.py",
        env=KPI_ENV,
    )

    kpi2 = BashOperator(
        task_id="kpi2_collaboration",
        task_display_name="KPI 2 — Collaboration",
        bash_command="python3 /opt/airflow/jobs/kpi/kpi2_collaboration.py",
        env=KPI_ENV,
    )

    stats = BashOperator(
        task_id="stats_globales",
        task_display_name="Stats Globales",
        bash_command="python3 /opt/airflow/jobs/kpi/stats_globales.py",
        env=KPI_ENV,
    )

    [kpi1, kpi2, stats]
