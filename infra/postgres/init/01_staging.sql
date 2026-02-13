-- ─────────────────────────────────────────
-- SCHEMA : staging
-- Données brutes normalisées avant datamart
-- ─────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.events;
CREATE TABLE staging.events (
    source           TEXT        NOT NULL,
    actor_id         TEXT        NOT NULL,
    event_id         TEXT        NOT NULL,
    event_ts         TIMESTAMPTZ NOT NULL,
    event_type       TEXT,
    thread_id        TEXT,
    parent_id        TEXT,
    tags             TEXT,
    text             TEXT,
    is_help_request  BOOLEAN     DEFAULT FALSE,
    is_resolved      BOOLEAN     DEFAULT FALSE,
    ingested_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (source, event_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_actor_id  ON staging.events (actor_id);
CREATE INDEX IF NOT EXISTS idx_staging_event_ts  ON staging.events (event_ts);
CREATE INDEX IF NOT EXISTS idx_staging_source    ON staging.events (source);
