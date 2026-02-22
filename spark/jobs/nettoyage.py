"""
Nettoyage des colonnes texte pour la couche curated.

Transformations appliquées :
  clean_text       : \x00 + ctrl, trim, espaces multiples        (toutes sources)
  clean_html_text  : clean_text + balises HTML + entités HTML
                     (RSS, StackOverflow, HackerNews)
  clean_id         : trim                            (actor_id, event_id, ...)
  clean_tags       : trim + lowercase + suppression vides        (tags array<string>)
  quality_filter   : event_id/actor_id non nuls, text non vide,
                     event_ts >= 2020, déduplication event_id    (toutes sources)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

TEXT_MAX_LEN = 50_000
MIN_YEAR = 2020


def clean_text(col):
    """Nettoyage de base : octets nuls, ctrl, trim, espaces multiples."""
    return F.trim(
        F.regexp_replace(
            F.regexp_replace(col, "[\\x00-\\x1f]", " "),
            "\\s+",
            " ",
        )
    )


def clean_html_text(col):
    """Nettoyage HTML : balises + entités + nettoyage de base.
    Pour RSS, StackOverflow, HackerNews."""
    stripped = F.regexp_replace(col, "<[^>]+>", " ")
    entities = F.regexp_replace(stripped, "&amp;", "&")
    entities = F.regexp_replace(entities, "&lt;", "<")
    entities = F.regexp_replace(entities, "&gt;", ">")
    entities = F.regexp_replace(entities, "&nbsp;", " ")
    entities = F.regexp_replace(entities, "&#?[a-zA-Z0-9]+;", " ")
    cleaned = clean_text(entities)
    return F.substring(cleaned, 1, TEXT_MAX_LEN)


def clean_id(col):
    """Trim simple sur les colonnes identifiant."""
    return F.trim(col)


def clean_tags(col):
    """Trim + lowercase sur chaque élément + suppression des éléments vides ou nuls."""
    trimmed = F.transform(col, lambda t: F.lower(F.trim(t)))
    return F.filter(trimmed, lambda t: t.isNotNull() & (F.length(t) > 0))


def quality_filter(df: DataFrame) -> DataFrame:
    """
    Supprime les lignes invalides :
      - event_id ou actor_id nuls/vides
      - text nul ou vide après nettoyage
      - event_ts avant MIN_YEAR
    Remplace les dates futures par la date d'ingestion (current_timestamp).
    Puis déduplique sur (source, event_id).
    """
    now = F.current_timestamp()
    return (
        df.filter(
            F.col("event_id").isNotNull()
            & (F.length(F.col("event_id")) > 0)
            & F.col("actor_id").isNotNull()
            & (F.length(F.col("actor_id")) > 0)
            & F.col("text").isNotNull()
            & (F.length(F.col("text")) > 0)
            & F.col("event_ts").isNotNull()
            & (F.year(F.col("event_ts")) >= MIN_YEAR)
        )
        .withColumn(
            "event_ts",
            F.when(F.col("event_ts") > now, now).otherwise(F.col("event_ts")),
        )
        .dropDuplicates(["source", "event_id"])
    )
