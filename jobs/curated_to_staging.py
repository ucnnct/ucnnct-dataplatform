"""
curated_to_staging — Ingestion incrémentale curated MinIO → ClickHouse staging.

Lit les fichiers Parquet depuis MinIO (curated/<source>/<year>/<month>/<day>/)
et les insère dans la table ClickHouse `events` (ReplacingMergeTree).

Pipeline : dag_collect → dag_transform → curated_to_staging

Watermark automatique : MAX(event_ts) depuis ClickHouse.
Optimisation S3       : pattern year= calculé depuis les dates pour éviter
                        de scanner tout le bucket.

Variables d'environnement :
    CLICKHOUSE_HOST       défaut: localhost
    CLICKHOUSE_PORT       défaut: 8123
    CLICKHOUSE_USER       défaut: default
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_DB         défaut: uconnect
    MINIO_ENDPOINT        défaut: 172.31.250.57:9000
    MINIO_ROOT_USER       défaut: ucnnct
    MINIO_ROOT_PASSWORD
    MINIO_BUCKET          défaut: datalake
    LOAD_SOURCES          ex: bluesky,nostr  (défaut: toutes les sources)
    LOAD_DATE_DEBUT       ex: 2026-01-01 00:00:00  (écrase le watermark auto)
    LOAD_DATE_FIN         ex: 2026-01-31 23:59:59  (défaut: pas de borne)
    LOAD_DATE_DEFAULT     date de départ si aucun watermark en base
                          défaut: 2026-01-01 00:00:00
    LOG_LEVEL             défaut: INFO
    LOG_FORMAT            json | console  (défaut: console)
"""

import os
import sys
import time
from datetime import datetime

import logging

import clickhouse_connect
import structlog

_log_format = os.getenv("LOG_FORMAT", "console").lower()
_log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, _log_level, logging.INFO))

_shared_processors = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]

if _log_format == "json":
    _renderer = structlog.processors.JSONRenderer()
else:
    _renderer = structlog.dev.ConsoleRenderer()

structlog.configure(
    processors=_shared_processors + [_renderer],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger("curated-to-staging")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sirius2025")
CH_DB = os.getenv("CLICKHOUSE_DB", "uconnect")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "ucnnct")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "sirius2025")
BUCKET = os.getenv("MINIO_BUCKET", "datalake")

SOURCES_ENV = os.getenv("LOAD_SOURCES", "").strip()
DATE_DEBUT_ENV = os.getenv("LOAD_DATE_DEBUT", "").strip()
DATE_FIN_ENV = os.getenv("LOAD_DATE_FIN", "").strip()
DATE_DEFAULT = os.getenv("LOAD_DATE_DEFAULT", "2026-01-01 00:00:00")


def get_year_pattern(start_date_str: str, end_date_str: str) -> str:
    """Génère un pattern de dossier ClickHouse basé sur les années couvertes.

    Évite de scanner tout le bucket en ciblant uniquement les partitions
    year=YYYY concernées par la fenêtre temporelle.

    Exemples :
        2026-01-01 → 2026-12-31  →  year=2026
        2025-11-01 → 2026-03-31  →  year={2025,2026}

    Args:
        start_date_str: date de début (ISO ou datetime string).
        end_date_str:   date de fin   (ISO ou datetime string). Vide = année courante.

    Returns:
        Pattern ClickHouse ex: "year=2026" ou "year={2025,2026}".
    """
    try:
        start_year = datetime.strptime(start_date_str[:10], "%Y-%m-%d").year
        end_year = (
            datetime.strptime(end_date_str[:10], "%Y-%m-%d").year
            if end_date_str
            else datetime.now().year
        )
        if start_year == end_year:
            return f"year={start_year}"
        return f"year={{{','.join(map(str, range(start_year, end_year + 1)))}}}"
    except Exception:
        # Repli sécurisé si le format de date est inattendu
        log.warning(
            "pattern year impossible à calculer, repli sur *",
            start=start_date_str,
            end=end_date_str,
        )
        return "year=*"


def main() -> None:
    """Orchestre l'ingestion incrémentale curated → ClickHouse staging."""
    log.info(
        "démarrage",
        sources=SOURCES_ENV or "toutes",
        date_debut=DATE_DEBUT_ENV or "watermark auto",
        date_fin=DATE_FIN_ENV or "aucune borne",
    )

    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DB,
        )
        log.info("connexion ClickHouse OK", host=CH_HOST, port=CH_PORT, db=CH_DB)
    except Exception:
        log.critical("échec connexion ClickHouse", exc_info=True)
        sys.exit(1)

    # ReplacingMergeTree(ingested_at) : en cas de doublon sur (source, event_ts, event_id)
    # ClickHouse garde la ligne avec le ingested_at le plus récent après merge.
    client.command("""
        CREATE TABLE IF NOT EXISTS events (
            source          String,
            actor_id        String,
            event_id        String,
            event_ts        DateTime64(3, 'UTC'),
            event_type      String,
            thread_id       Nullable(String),
            parent_id       Nullable(String),
            tags            Array(String),
            text            String,
            is_help_request UInt8,
            is_resolved     UInt8,
            ingested_at     DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(ingested_at)
        ORDER BY (source, event_ts, event_id)
    """)
    log.info("table events prête")

    if DATE_DEBUT_ENV and DATE_DEBUT_ENV.lower() != "none":
        watermark = DATE_DEBUT_ENV
        log.info("watermark manuel", watermark=watermark)
    else:
        try:
            res = client.command(
                "SELECT formatDateTime(max(event_ts), '%Y-%m-%d %H:%i:%s') FROM events"
            )
            watermark = (
                res if res and str(res) != "0000-00-00 00:00:00" else DATE_DEFAULT
            )
            log.info("watermark automatique", watermark=watermark)
        except Exception:
            watermark = DATE_DEFAULT
            log.warning("watermark auto échoué, repli sur défaut", watermark=watermark)

    year_pattern = get_year_pattern(watermark, DATE_FIN_ENV)
    source_pattern = (
        f"{{{SOURCES_ENV}}}" if "," in SOURCES_ENV else (SOURCES_ENV or "*")
    )

    s3_url = (
        f"http://{MINIO_ENDPOINT}/{BUCKET}/curated/"
        f"{source_pattern}/{year_pattern}/*/*/*.parquet"
    )
    log.info("chemin S3 ciblé", url=s3_url)

    where_clauses = [f"event_ts > '{watermark}'"]
    if DATE_FIN_ENV and DATE_FIN_ENV.lower() != "none":
        where_clauses.append(f"event_ts <= '{DATE_FIN_ENV}'")

    import_sql = f"""
        INSERT INTO events
            (source, actor_id, event_id, event_ts, event_type,
             thread_id, parent_id, tags, text, is_help_request, is_resolved)
        SELECT
            source, actor_id, event_id, event_ts, event_type,
            thread_id, parent_id, tags, text,
            toUInt8(is_help_request), toUInt8(is_resolved)
        FROM s3('{s3_url}', '{MINIO_USER}', '{MINIO_PASSWORD}', 'Parquet')
        WHERE {" AND ".join(where_clauses)}
    """

    try:
        log.info("insertion en cours...")
        start_ts = time.time()

        client.command(
            import_sql,
            settings={
                "max_insert_block_size": 1_000_000,
                "s3_truncate_on_insert": 0,
            },
        )

        duration = time.time() - start_ts
        final_count = client.command("SELECT count() FROM events")

        log.info(
            "insertion terminée",
            durée_s=round(duration, 2),
            total_lignes=final_count,
            débit_lignes_s=round(final_count / duration) if duration > 0 else 0,
        )

    except Exception:
        log.error(
            "échec insertion", exc_info=True, sql=import_sql[:300] + "..."
        )  # tronqué pour éviter les logs géants
        sys.exit(1)


if __name__ == "__main__":
    main()
