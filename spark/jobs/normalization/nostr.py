"""
Normalisation Nostr : raw JSONL -> curated Parquet

Détournement selon docs/Détournement_Expliqué.pdf :
  pubkey      -> actor_id
  id          -> event_id
  created_at  -> event_ts     (Unix secondes -> timestamp)
  kind        -> event_type
  thread_ref  -> thread_id    (tag "e" marqueur "root", sinon premier tag "e")
  parent_ref  -> parent_id    (tag "e" marqueur "reply", sinon dernier tag "e")
  tags["p"]   -> tags         (pubkeys mentionnées)
  content     -> text
  is_help_request / is_resolved : False

Format tags Nostr (NIP-01) :
  [["e", <event_id>, <relay>, "root"],
   ["e", <event_id>, <relay>, "reply"],
   ["p", <pubkey>]]
"""

import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bookmark import get_s3_client, list_new_files, read_bookmark, write_bookmark
from nettoyage import clean_id, clean_tags, clean_text, quality_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-nostr")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
MAX_DATE = os.getenv("MAX_DATE")  # ex: "2026/02/20/20260220_1200" — None = pas de borne
BUCKET = "datalake"
SOURCE = "nostr"


def build_spark():
    return (
        SparkSession.builder.appName(f"normalize-{SOURCE}")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def main():
    s3 = get_s3_client(MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD)
    last_key = read_bookmark(s3, BUCKET, SOURCE)
    end_key = f"raw/{SOURCE}/{MAX_DATE}" if MAX_DATE else None
    new_keys = list_new_files(s3, BUCKET, f"raw/{SOURCE}/", last_key, end_key)

    if not new_keys:
        logger.info("Aucun nouveau fichier pour %s, arrêt.", SOURCE)
        sys.exit(0)

    t0 = time.time()
    logger.info("démarrage  fichiers=%d  bookmark=%s", len(new_keys), last_key or "début")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Higher-order functions JVM (pas de Python UDF)
    thread_ref = F.coalesce(
        F.element_at(
            F.transform(
                F.filter(
                    F.col("tags"),
                    lambda t: (F.size(t) >= 4) & (t[0] == "e") & (t[3] == "root"),
                ),
                lambda t: t[1],
            ),
            1,
        ),
        F.element_at(
            F.transform(
                F.filter(F.col("tags"), lambda t: (F.size(t) >= 2) & (t[0] == "e")),
                lambda t: t[1],
            ),
            1,
        ),
    )
    parent_ref = F.coalesce(
        F.element_at(
            F.transform(
                F.filter(
                    F.col("tags"),
                    lambda t: (F.size(t) >= 4) & (t[0] == "e") & (t[3] == "reply"),
                ),
                lambda t: t[1],
            ),
            1,
        ),
        F.element_at(
            F.transform(
                F.filter(F.col("tags"), lambda t: (F.size(t) >= 2) & (t[0] == "e")),
                lambda t: t[1],
            ),
            -1,
        ),
    )
    p_tags = F.transform(
        F.filter(F.col("tags"), lambda t: (F.size(t) >= 2) & (t[0] == "p")),
        lambda t: t[1],
    )

    s3a_paths = [f"s3a://{BUCKET}/{k}" for k in new_keys]
    t_read = time.time()

    try:
        df = spark.read.json(s3a_paths)
    except Exception as e:
        logger.warning("Lecture impossible : %s", e)
        spark.stop()
        sys.exit(0)

    count_before = df.count()
    if count_before == 0:
        logger.warning("Aucune donnée, arrêt.")
        spark.stop()
        sys.exit(0)

    logger.info("lecture     lignes=%s  durée=%.1fs", f"{count_before:,}", time.time() - t_read)

    normalized = df.select(
        F.lit(SOURCE).alias("source"),
        F.col("pubkey").alias("actor_id"),
        F.col("id").alias("event_id"),
        F.to_timestamp(F.col("created_at")).alias("event_ts"),
        F.col("kind").cast("string").alias("event_type"),
        thread_ref.alias("thread_id"),
        parent_ref.alias("parent_id"),
        p_tags.alias("tags"),
        F.col("content").alias("text"),
        F.lit(False).cast("boolean").alias("is_help_request"),
        F.lit(False).cast("boolean").alias("is_resolved"),
    )
    normalized = (
        normalized.withColumn("actor_id", clean_id(F.col("actor_id")))
        .withColumn("event_id", clean_id(F.col("event_id")))
        .withColumn("thread_id", clean_id(F.col("thread_id")))
        .withColumn("parent_id", clean_id(F.col("parent_id")))
        .withColumn("tags", clean_tags(F.col("tags")))
        .withColumn("text", clean_text(F.col("text")))
    )
    normalized = quality_filter(normalized)
    normalized = (
        normalized.withColumn("year", F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day", F.dayofmonth("event_ts"))
    )

    out_path = f"s3a://{BUCKET}/curated/{SOURCE}"
    normalized = normalized.cache()
    t_norm = time.time()
    count_after = normalized.count()
    rejected = count_before - count_after
    pct = (rejected / count_before * 100) if count_before > 0 else 0
    logger.info(
        "normalisation  conservés=%s  rejetés=%s (%.1f%%)  durée=%.1fs",
        f"{count_after:,}",
        f"{rejected:,}",
        pct,
        time.time() - t_norm,
    )
    t_write = time.time()
    normalized = normalized.dropDuplicates(["source", "event_id"])
    normalized.repartition(1, "year", "month", "day").write.mode("append").partitionBy(
        "year", "month", "day"
    ).parquet(out_path)
    normalized.unpersist()
    write_bookmark(s3, BUCKET, SOURCE, new_keys[-1])
    t_end = time.time()
    debit = int(count_after / (t_end - t_write)) if (t_end - t_write) > 0 else 0
    logger.info("écriture    durée=%.1fs  débit=%s lig/s", t_end - t_write, f"{debit:,}")
    logger.info("terminé     total=%.1fs", t_end - t0)
    spark.stop()


if __name__ == "__main__":
    main()
