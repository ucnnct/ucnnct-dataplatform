"""
KPI 2 — Collaboration Étudiante.

Mesure le niveau global d'utilisation et d'engagement dans la messagerie
à travers le volume total de messages envoyés et l'intensité d'échange
entre les utilisateurs.

DIMENSIONS
──────────
taux_participation_groupe
    |{actor_id | thread_id ≠ NULL ∧ event_ts ∈ jour}| / |{actor_id}|

taux_resolution
    Σ1(is_help_request=1 ∧ is_resolved=1) / Σ1(is_help_request=1)

interaction_moyenne
    Σ1(event_type ∈ M) / |{actor_id | event_type ∈ M ∧ event_ts ∈ jour}|
    M = {post, reply, comment, answer, app.bsky.feed.post, question, 1}

nb_messages_total
    |{e | parent_id ≠ NULL}| + |{e | thread_id ≠ NULL}|   (addition intentionnelle)

Lecture  : ClickHouse staging (table events)
Écriture : PostgreSQL {POSTGRES_SCHEMA}.kpi2_collaboration — une ligne par (jour, source)

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

import sys

import clickhouse_connect
import psycopg2
from psycopg2.extras import execute_values
from config import CH_DB, CH_HOST, CH_PASSWORD, CH_PORT, CH_USER
from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_SCHEMA, PG_USER
from log_setup import setup

log = setup("kpi2-collaboration")

M = "'post', 'reply', 'comment', 'answer', 'app.bsky.feed.post', 'question', '1'"

# Une ligne par (jour, source)
KPI_SQL_BY_SOURCE = f"""
WITH base_total AS (
    SELECT source, uniq(actor_id) AS nb_inscrits_total
    FROM events
    GROUP BY source
)
SELECT
    toDate(event_ts)                                                                        AS jour,
    source,
    uniqIf(actor_id, isNotNull(thread_id)) / nullIf(nb_inscrits_total, 0)                  AS taux_participation_groupe,
    countIf(is_help_request = 1 AND is_resolved = 1)
        / nullIf(countIf(is_help_request = 1), 0)                                          AS taux_resolution,
    countIf(event_type IN ({M}))
        / nullIf(uniqIf(actor_id, event_type IN ({M})), 0)                                 AS interaction_moyenne,
    countIf(isNotNull(parent_id)) + countIf(isNotNull(thread_id))                          AS nb_messages_total
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
    toDate(event_ts)                                                                        AS jour,
    uniqIf(actor_id, isNotNull(thread_id)) / nullIf(nb_inscrits_total, 0)                  AS taux_participation_groupe,
    countIf(is_help_request = 1 AND is_resolved = 1)
        / nullIf(countIf(is_help_request = 1), 0)                                          AS taux_resolution,
    countIf(event_type IN ({M}))
        / nullIf(uniqIf(actor_id, event_type IN ({M})), 0)                                 AS interaction_moyenne,
    countIf(isNotNull(parent_id)) + countIf(isNotNull(thread_id))                          AS nb_messages_total
FROM events
CROSS JOIN base_total
GROUP BY jour, nb_inscrits_total
"""


def main() -> None:
    log.info("démarrage kpi2")

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
                INSERT INTO {PG_SCHEMA}.kpi2_collaboration
                    (period_start, period_end, source,
                     taux_participation_groupe, taux_resolution,
                     interaction_moyenne, nb_messages_total)
                VALUES %s
                ON CONFLICT (period_end, source) DO UPDATE SET
                    taux_participation_groupe = EXCLUDED.taux_participation_groupe,
                    taux_resolution           = EXCLUDED.taux_resolution,
                    interaction_moyenne       = EXCLUDED.interaction_moyenne,
                    nb_messages_total         = EXCLUDED.nb_messages_total,
                    computed_at               = NOW()
            """,
                pg_data,
            )

        pg.commit()
        log.info("kpi2 écrit", nb_lignes=len(pg_data))

    except Exception:
        log.critical("erreur fatale", exc_info=True)
        pg.rollback()
        sys.exit(1)

    finally:
        pg.close()


if __name__ == "__main__":
    main()
