"""
Stats globales de la plateforme.

Calcule par source un snapshot global couvrant les 3 couches du lac :

MÉTRIQUES D'ACTIVITÉ (ClickHouse staging)
──────────────────────────────────────────
total_events          Nombre total d'événements
total_acteurs         Nombre d'acteurs distincts
dau_moyen             Daily Active Users moyen
avg_events_per_user   Événements moyens par acteur
reply_rate            Part des événements avec parent_id
thread_rate           Part des événements avec thread_id
help_request_rate     Part des demandes d'aide
resolution_rate       Part des demandes d'aide résolues
first_event_ts        Premier événement collecté
last_event_ts         Dernier événement collecté

VOLUMÉTRIE DU LAC (MinIO)
──────────────────────────
raw_size_bytes / raw_file_count       Taille et fichiers dans raw/<source>/
curated_size_bytes / curated_file_count  Taille et fichiers dans curated/<source>/
staging_rows / staging_size_bytes     Lignes et taille dans ClickHouse

Lecture  : ClickHouse staging (table events) + MinIO (boto3)
Écriture : PostgreSQL {POSTGRES_SCHEMA}.stats_globales — une ligne par source (snapshot du jour)

Variables d'environnement :
    CLICKHOUSE_HOST     défaut: localhost
    CLICKHOUSE_PORT     défaut: 8123
    CLICKHOUSE_USER     défaut: default
    CLICKHOUSE_PASSWORD défaut: sirius2025
    CLICKHOUSE_DB       défaut: uconnect
    POSTGRES_HOST       défaut: localhost
    POSTGRES_PORT       défaut: 5432
    POSTGRES_DB         défaut: uconnect
    POSTGRES_USER       défaut: ucnnct
    POSTGRES_PASSWORD   défaut: ucnnct_pg_2024
    POSTGRES_SCHEMA     défaut: datamart  (gold pour les tests)
    MINIO_ENDPOINT      défaut: 172.31.250.57:9000
    MINIO_ACCESS_KEY    défaut: ucnnct
    MINIO_SECRET_KEY    défaut: sirius2025
    MINIO_BUCKET        défaut: datalake
    LOAD_SOURCES        ex: bluesky,nostr  (défaut: toutes les sources)
    LOG_LEVEL           défaut: INFO
    LOG_FORMAT          json | console  (défaut: console)
"""

import os
import sys
from datetime import date

import boto3
import clickhouse_connect
import psycopg2
from botocore.client import Config
from psycopg2.extras import execute_values
from config import CH_DB, CH_HOST, CH_PASSWORD, CH_PORT, CH_USER
from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_SCHEMA, PG_USER
from config import MINIO_ACCESS_KEY, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_SECRET_KEY
from log_setup import setup

log = setup("stats-globales")

# ── Sources ───────────────────────────────────────────────────────────────────
ALL_SOURCES = ["bluesky", "nostr", "hackernews", "rss", "stackoverflow"]
_env_src = os.getenv("LOAD_SOURCES", "").strip()
SOURCES = [s.strip() for s in _env_src.split(",") if s.strip()] or ALL_SOURCES


def get_minio_prefix_stats(s3, prefix: str) -> tuple[int, int]:
    paginator = s3.get_paginator("list_objects_v2")
    total_size = 0
    total_count = 0
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            total_size += obj["Size"]
            total_count += 1
    return total_size, total_count


def fetch_stats(ch, source: str) -> dict | None:
    row = ch.query(f"""
        SELECT
            count()                                                             AS total_events,
            uniq(actor_id)                                                      AS total_acteurs,
            avg(daily_dau)                                                      AS dau_moyen,
            countIf(isNotNull(parent_id))                                       AS nb_replies,
            countIf(isNotNull(thread_id))                                       AS nb_in_thread,
            countIf(is_help_request = 1)                                        AS nb_help,
            countIf(is_help_request = 1 AND is_resolved = 1)                   AS nb_resolved,
            min(event_ts)                                                       AS first_event_ts,
            max(event_ts)                                                       AS last_event_ts
        FROM (
            SELECT
                actor_id, event_ts, parent_id, thread_id,
                is_help_request, is_resolved,
                uniq(actor_id) OVER (PARTITION BY toDate(event_ts))            AS daily_dau
            FROM events
            WHERE source = '{source}'
        )
    """).first_row

    if not row or row[0] == 0:
        return None

    (
        total_events,
        total_acteurs,
        dau_moyen,
        nb_replies,
        nb_in_thread,
        nb_help,
        nb_resolved,
        first_event_ts,
        last_event_ts,
    ) = row

    total_events = int(total_events or 0)
    total_acteurs = int(total_acteurs or 0)
    nb_replies = int(nb_replies or 0)
    nb_in_thread = int(nb_in_thread or 0)
    nb_help = int(nb_help or 0)
    nb_resolved = int(nb_resolved or 0)

    return {
        "total_events": total_events,
        "total_acteurs": total_acteurs,
        "dau_moyen": round(float(dau_moyen or 0), 2),
        "avg_events_per_user": (
            round(total_events / total_acteurs, 2) if total_acteurs > 0 else 0.0
        ),
        "reply_rate": round(nb_replies / total_events, 4) if total_events > 0 else 0.0,
        "thread_rate": (
            round(nb_in_thread / total_events, 4) if total_events > 0 else 0.0
        ),
        "help_request_rate": (
            round(nb_help / total_events, 4) if total_events > 0 else 0.0
        ),
        "resolution_rate": round(nb_resolved / nb_help, 4) if nb_help > 0 else 0.0,
        "first_event_ts": first_event_ts,
        "last_event_ts": last_event_ts,
    }


def fetch_staging_stats(ch, source: str) -> tuple[int, int]:
    count_row = ch.query(
        f"SELECT count() FROM events WHERE source = '{source}'"
    ).first_row
    staging_rows = int(count_row[0] or 0) if count_row else 0

    total_row = ch.query(
        "SELECT sum(rows), sum(data_compressed_bytes) FROM system.parts "
        f"WHERE database = '{CH_DB}' AND table = 'events' AND active = 1"
    ).first_row

    if total_row and total_row[0] and int(total_row[0]) > 0:
        total_rows = int(total_row[0])
        table_size = int(total_row[1] or 0)
        staging_size = int(table_size * staging_rows / total_rows)
    else:
        staging_size = 0

    return staging_rows, staging_size


def main() -> None:
    log.info("démarrage stats-globales", sources=SOURCES)

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )

    try:
        ch = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB,
        )
        log.info("connexion ClickHouse OK")
    except Exception:
        log.critical("échec connexion ClickHouse", exc_info=True)
        sys.exit(1)

    try:
        pg = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASSWORD,
        )
        log.info("connexion PostgreSQL OK")
    except Exception:
        log.critical("échec connexion PostgreSQL", exc_info=True)
        sys.exit(1)

    today = date.today().isoformat()

    try:
        rows = []
        for source in SOURCES:
            try:
                raw_size, raw_count = get_minio_prefix_stats(s3, f"raw/{source}/")
                curated_size, curated_count = get_minio_prefix_stats(
                    s3, f"curated/{source}/"
                )
                staging_rows, staging_size = fetch_staging_stats(ch, source)

                log.info(
                    "volumétrie",
                    source=source,
                    raw_gb=round(raw_size / 1024**3, 2),
                    raw_files=raw_count,
                    curated_gb=round(curated_size / 1024**3, 2),
                    curated_files=curated_count,
                    staging_rows=staging_rows,
                )

                stats = fetch_stats(ch, source)
                if stats is None:
                    log.warning("aucune donnée", source=source)
                    continue

                rows.append(
                    (
                        today,
                        today,
                        source,
                        stats["total_events"],
                        stats["total_acteurs"],
                        stats["dau_moyen"],
                        stats["avg_events_per_user"],
                        stats["reply_rate"],
                        stats["thread_rate"],
                        stats["help_request_rate"],
                        stats["resolution_rate"],
                        stats["first_event_ts"],
                        stats["last_event_ts"],
                        raw_size,
                        raw_count,
                        curated_size,
                        curated_count,
                        staging_size,
                    )
                )

                log.info(
                    "stats collectées",
                    source=source,
                    total_events=stats["total_events"],
                    total_acteurs=stats["total_acteurs"],
                )

            except Exception:
                log.error("erreur source", source=source, exc_info=True)
                continue

        with pg.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {PG_SCHEMA}.stats_globales
                    (period_start, period_end, source,
                     total_events, total_acteurs, dau_moyen, avg_events_per_user,
                     reply_rate, thread_rate, help_request_rate, resolution_rate,
                     first_event_ts, last_event_ts,
                     raw_size_bytes, raw_file_count,
                     curated_size_bytes, curated_file_count,
                     staging_size_bytes)
                VALUES %s
                ON CONFLICT (period_end, source) DO UPDATE SET
                    total_events        = EXCLUDED.total_events,
                    total_acteurs       = EXCLUDED.total_acteurs,
                    dau_moyen           = EXCLUDED.dau_moyen,
                    avg_events_per_user = EXCLUDED.avg_events_per_user,
                    reply_rate          = EXCLUDED.reply_rate,
                    thread_rate         = EXCLUDED.thread_rate,
                    help_request_rate   = EXCLUDED.help_request_rate,
                    resolution_rate     = EXCLUDED.resolution_rate,
                    first_event_ts      = EXCLUDED.first_event_ts,
                    last_event_ts       = EXCLUDED.last_event_ts,
                    raw_size_bytes      = EXCLUDED.raw_size_bytes,
                    raw_file_count      = EXCLUDED.raw_file_count,
                    curated_size_bytes  = EXCLUDED.curated_size_bytes,
                    curated_file_count  = EXCLUDED.curated_file_count,
                    staging_size_bytes  = EXCLUDED.staging_size_bytes,
                    computed_at         = NOW()
            """,
                rows,
            )

        pg.commit()
        log.info("terminé", sources=len(rows))

    except Exception:
        log.critical("erreur fatale", exc_info=True)
        pg.rollback()
        sys.exit(1)

    finally:
        pg.close()


if __name__ == "__main__":
    main()
