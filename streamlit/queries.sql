-- ============================================================
-- UConnect Analytics — Requêtes ClickHouse (staging.events)
-- Base : uconnect | Table : events
-- ============================================================


-- ── Types d'événements fonctionnels (KPI 1 — taux_fonctionnel) ──────────────
-- Bluesky: app.bsky.feed.post | Nostr: 1, 6 | HN/SO/RSS: story, article, question, answer

-- ── Types de messages (KPI 2 — interaction_moyenne) ─────────────────────────
-- post, reply, comment, answer, app.bsky.feed.post, question, 1


-- ============================================================
-- 1. STATS GLOBALES (snapshot toutes périodes confondues)
-- ============================================================
SELECT
    count()                                                             AS total_events,
    uniq(actor_id)                                                      AS total_acteurs,
    (SELECT avg(dau) FROM (
        SELECT toDate(event_ts) AS jour, uniq(actor_id) AS dau
        FROM events GROUP BY jour
    ))                                                                  AS dau_moyen,
    count() / nullIf(uniq(actor_id), 0)                                 AS avg_events_per_user,
    countIf(is_help_request = 1 AND is_resolved = 1)
        / nullIf(countIf(is_help_request = 1), 0)                       AS resolution_rate,
    min(event_ts)                                                       AS first_event_ts,
    max(event_ts)                                                       AS last_event_ts
FROM events;


-- ============================================================
-- 2. KPI 1 — Utilisation (par jour)
-- ============================================================
SELECT
    toDate(event_ts)                                                    AS period_start,

    -- Taux d'utilisateurs actifs : actifs du jour / total inscrits
    uniq(actor_id) / (SELECT uniq(actor_id) FROM events)                AS taux_actifs,

    -- Taux de participation : actifs dans un thread / actifs du jour
    uniqIf(actor_id, isNotNull(thread_id))
        / nullIf(uniq(actor_id), 0)                                     AS taux_participation,

    -- Taux fonctionnel : actifs ayant utilisé un event_type fonctionnel / actifs du jour
    uniqIf(actor_id, event_type IN (
        'app.bsky.feed.post','1','6','story','article','question','answer'
    )) / nullIf(uniq(actor_id), 0)                                      AS taux_fonctionnel,

    -- Fréquence moyenne : événements / actifs du jour
    count() / nullIf(uniq(actor_id), 0)                                 AS freq_moyenne

FROM events
GROUP BY period_start
ORDER BY period_start DESC;


-- ============================================================
-- 3. KPI 2 — Collaboration (par jour)
-- ============================================================
SELECT
    toDate(event_ts)                                                    AS period_start,

    -- Taux participation groupe : actifs dans un thread / total acteurs inscrits
    uniqIf(actor_id, isNotNull(thread_id))
        / nullIf((SELECT uniq(actor_id) FROM events), 0)                AS taux_participation_groupe,

    -- Taux résolution : demandes d'aide résolues / total demandes d'aide
    countIf(is_help_request = 1 AND is_resolved = 1)
        / nullIf(countIf(is_help_request = 1), 0)                       AS taux_resolution,

    -- Interaction moyenne : messages / acteurs actifs en messagerie
    countIf(event_type IN (
        'post','reply','comment','answer','app.bsky.feed.post','question','1'
    )) / nullIf(uniqIf(actor_id, event_type IN (
        'post','reply','comment','answer','app.bsky.feed.post','question','1'
    )), 0)                                                              AS interaction_moyenne,

    -- Nb messages total : events avec parent_id + events avec thread_id
    countIf(isNotNull(parent_id)) + countIf(isNotNull(thread_id))       AS nb_messages_total

FROM events
GROUP BY period_start
ORDER BY period_start DESC;


-- ============================================================
-- 4. ÉVOLUTION TEMPORELLE (volumes par jour)
-- ============================================================
SELECT
    toDate(event_ts)                                                    AS period_start,
    count()                                                             AS total_events,
    uniq(actor_id)                                                      AS total_acteurs,
    count() / nullIf(uniq(actor_id), 0)                                 AS avg_events_per_user
FROM events
GROUP BY period_start
ORDER BY period_start;
