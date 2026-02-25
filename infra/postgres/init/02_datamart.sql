CREATE SCHEMA IF NOT EXISTS datamart;

CREATE TABLE IF NOT EXISTS datamart.kpi1_utilisation (
    period_start        DATE        NOT NULL,
    period_end          DATE        NOT NULL,
    source              TEXT        NOT NULL,
    taux_actifs         NUMERIC(5,4),
    taux_participation  NUMERIC(5,4),
    taux_fonctionnel    NUMERIC(5,4),
    freq_moyenne        NUMERIC(10,2),
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (period_end, source)
);

CREATE TABLE IF NOT EXISTS datamart.kpi2_collaboration (
    period_start              DATE        NOT NULL,
    period_end                DATE        NOT NULL,
    source                    TEXT        NOT NULL,
    taux_participation_groupe NUMERIC(5,4),
    taux_resolution           NUMERIC(5,4),
    interaction_moyenne       NUMERIC(10,2),
    nb_messages_total         BIGINT,
    computed_at               TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (period_end, source)
);

CREATE TABLE IF NOT EXISTS datamart.stats_globales (
    period_start        DATE        NOT NULL,
    period_end          DATE        NOT NULL,
    source              TEXT        NOT NULL,
    total_events        BIGINT,
    total_acteurs       BIGINT,
    dau_moyen           NUMERIC(10,2),
    avg_events_per_user NUMERIC(10,2),
    reply_rate          NUMERIC(5,4),
    thread_rate         NUMERIC(5,4),
    help_request_rate   NUMERIC(5,4),
    resolution_rate     NUMERIC(5,4),
    first_event_ts      TIMESTAMPTZ,
    last_event_ts       TIMESTAMPTZ,
    raw_size_bytes      BIGINT,
    raw_file_count      BIGINT,
    curated_size_bytes  BIGINT,
    curated_file_count  BIGINT,
    staging_size_bytes  BIGINT,
    computed_at         TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (period_end, source)
);
