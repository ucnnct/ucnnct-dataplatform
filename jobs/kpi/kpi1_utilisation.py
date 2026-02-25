"""
KPI 1 — Pourcentage d'utilisation du réseau social.

Mesure la proportion d'utilisateurs actifs qui utilisent réellement
les fonctionnalités du réseau social.

DIMENSIONS
──────────
taux_actifs
    |{actor_id | event_ts ∈ jour}| / |{actor_id}|

taux_participation
    |{actor_id | thread_id ≠ NULL ∧ event_ts ∈ jour}| / |{actor_id | event_ts ∈ jour}|

taux_fonctionnel
    |{actor_id | event_type ≠ NULL ∧ event_ts ∈ jour}| / |{actor_id | event_ts ∈ jour}|

freq_moyenne
    |{(actor_id, DATE(event_ts))}| / |{actor_id}|

Lecture  : ClickHouse staging (table events)
Écriture : PostgreSQL {POSTGRES_SCHEMA}.kpi1_utilisation — une ligne par (jour, source)

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
    LOG_LEVEL           défaut: INFO
    LOG_FORMAT          json | console  (défaut: console)
"""

import logging
import os
import sys

import clickhouse_connect
import psycopg2
import structlog
from psycopg2.extras import execute_values

_log_format = os.getenv("LOG_FORMAT", "console").lower()
_log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(level=getattr(logging, _log_level, logging.INFO))

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        (
            structlog.processors.JSONRenderer()
            if _log_format == "json"
            else structlog.dev.ConsoleRenderer()
        ),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger("kpi1-utilisation")

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "sirius2025")
CH_DB = os.getenv("CLICKHOUSE_DB", "uconnect")

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "uconnect")
PG_USER = os.getenv("POSTGRES_USER", "ucnnct")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "ucnnct_pg_2024")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "datamart")

# event_type considérés comme "actifs" (création de contenu, pas passif comme like/follow/reaction)
ACTIVE_TYPES = (
    "'app.bsky.feed.post', '1', '6', 'story', 'article', 'question', 'answer'"
)

# Une ligne par (jour, source)
KPI_SQL_BY_SOURCE = f"""
WITH base_total AS (
    SELECT source, uniq(actor_id) AS nb_inscrits_total
    FROM events
    GROUP BY source
)
SELECT
    toDate(event_ts)                                                                    AS jour,
    source,
    uniq(actor_id) / nb_inscrits_total                                                  AS taux_actifs,
    uniqIf(actor_id, isNotNull(thread_id)) / nullIf(uniq(actor_id), 0)                 AS taux_participation,
    uniqIf(actor_id, event_type IN ({ACTIVE_TYPES})) / nullIf(uniq(actor_id), 0)       AS taux_fonctionnel,
    uniq(actor_id, toDate(event_ts)) / nullIf(nb_inscrits_total, 0)                    AS freq_moyenne
FROM events
JOIN base_total USING (source)
GROUP BY jour, source, nb_inscrits_total
"""

# Une ligne par jour toutes sources confondues (source = 'all')
KPI_SQL_ALL = f"""
WITH base_total AS (
    SELECT uniq(actor_id) AS nb_inscrits_total
    FROM events
)
SELECT
    toDate(event_ts)                                                                    AS jour,
    uniq(actor_id) / nb_inscrits_total                                                  AS taux_actifs,
    uniqIf(actor_id, isNotNull(thread_id)) / nullIf(uniq(actor_id), 0)                 AS taux_participation,
    uniqIf(actor_id, event_type IN ({ACTIVE_TYPES})) / nullIf(uniq(actor_id), 0)       AS taux_fonctionnel,
    uniq(actor_id, toDate(event_ts)) / nullIf(nb_inscrits_total, 0)                    AS freq_moyenne
FROM events
CROSS JOIN base_total
GROUP BY jour, nb_inscrits_total
"""


def main() -> None:
    log.info("démarrage kpi1")

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

    try:
        rows_src = ch.query(KPI_SQL_BY_SOURCE).result_rows
        rows_all = ch.query(KPI_SQL_ALL).result_rows
        log.info("calcul terminé", par_source=len(rows_src), all=len(rows_all))

        pg_data = [(r[0], r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows_src]
        pg_data += [(r[0], r[0], "all", r[1], r[2], r[3], r[4]) for r in rows_all]

        with pg.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {PG_SCHEMA}.kpi1_utilisation
                    (period_start, period_end, source,
                     taux_actifs, taux_participation, taux_fonctionnel, freq_moyenne)
                VALUES %s
                ON CONFLICT (period_end, source) DO UPDATE SET
                    taux_actifs        = EXCLUDED.taux_actifs,
                    taux_participation = EXCLUDED.taux_participation,
                    taux_fonctionnel   = EXCLUDED.taux_fonctionnel,
                    freq_moyenne       = EXCLUDED.freq_moyenne,
                    computed_at        = NOW()
            """,
                pg_data,
            )

        pg.commit()
        log.info("kpi1 écrit", nb_lignes=len(pg_data))

    except Exception:
        log.critical("erreur fatale", exc_info=True)
        pg.rollback()
        sys.exit(1)

    finally:
        pg.close()


if __name__ == "__main__":
    main()
