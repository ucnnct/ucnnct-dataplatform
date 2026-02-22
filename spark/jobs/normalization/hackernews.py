"""
Normalisation HackerNews : raw JSONL -> curated Parquet

Détournement selon docs/Détournement_Expliqué.pdf :
  by          -> actor_id
  id          -> event_id
  time        -> event_ts   (Unix secondes -> timestamp)
  type        -> event_type
  thread_id   -> thread_id  (id si type=story, null sinon)
  parent      -> parent_id
  tags        -> tags       (null — non fourni par l'API HN)
  text/title  -> text       (coalesce : text pour comments, title pour stories)
  is_help_request : type == "ask"
  is_resolved     : type == "ask" AND descendants > 0
"""
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bookmark import get_s3_client, list_new_files, read_bookmark, write_bookmark
from nettoyage import clean_id, clean_html_text, quality_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-hackernews")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
MAX_DATE = os.getenv("MAX_DATE")  # ex: "2026/02/20/20260220_1200" — None = pas de borne
BUCKET = "datalake"
SOURCE = "hackernews"


def build_spark():
    return (
        SparkSession.builder
        .appName(f"normalize-{SOURCE}")
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

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    s3a_paths = [f"s3a://{BUCKET}/{k}" for k in new_keys]
    logger.info("Lecture de %d fichiers", len(s3a_paths))

    try:
        df = spark.read.json(s3a_paths)
    except Exception as e:
        logger.warning("Lecture impossible : %s", e)
        spark.stop()
        sys.exit(0)

    if df.limit(1).count() == 0:
        logger.warning("Aucune donnée, arrêt.")
        spark.stop()
        sys.exit(0)

    is_ask = F.col("type") == "ask"
    parent_col = F.col("parent").cast("string") if "parent" in df.columns else F.lit(None).cast("string")

    normalized = df.select(
        F.lit(SOURCE).alias("source"),
        F.col("by").alias("actor_id"),
        F.col("id").cast("string").alias("event_id"),
        F.to_timestamp(F.col("time")).alias("event_ts"),
        F.col("type").alias("event_type"),
        F.when(F.col("type") == "story", F.col("id").cast("string")).alias("thread_id"),
        parent_col.alias("parent_id"),
        F.lit(None).cast("array<string>").alias("tags"),
        F.coalesce(F.col("text"), F.col("title")).alias("text"),
        is_ask.alias("is_help_request"),
        (is_ask & (F.coalesce(F.col("descendants"), F.lit(0)) > 0)).alias("is_resolved"),
    )
    normalized = (
        normalized
        .withColumn("actor_id",  clean_id(F.col("actor_id")))
        .withColumn("event_id",  clean_id(F.col("event_id")))
        .withColumn("thread_id", clean_id(F.col("thread_id")))
        .withColumn("parent_id", clean_id(F.col("parent_id")))
        .withColumn("text",      clean_html_text(F.col("text")))
    )
    normalized = quality_filter(normalized)
    normalized = (
        normalized
        .withColumn("year",  F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day",   F.dayofmonth("event_ts"))
    )

    out_path = f"s3a://{BUCKET}/curated/{SOURCE}"
    normalized.write.mode("append").partitionBy("year", "month", "day").parquet(out_path)
    write_bookmark(s3, BUCKET, SOURCE, new_keys[-1])
    logger.info("OK | fichiers=%d sortie=%s", len(new_keys), out_path)
    spark.stop()


if __name__ == "__main__":
    main()
