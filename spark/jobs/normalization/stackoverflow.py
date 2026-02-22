"""
Normalisation StackOverflow : raw JSONL -> curated Parquet

Détournement selon docs/Détournement_Expliqué.pdf :
  owner_user_id   -> actor_id     (owner.user_id)
  post_id         -> event_id     (question_id si question, answer_id si réponse)
  creation_date   -> event_ts     (Unix secondes -> timestamp)
  post_type       -> event_type   ("question" | "answer")
  thread_id       -> thread_id    (question_id dans les deux cas)
  parent_id       -> parent_id    (question_id pour les réponses, null pour les questions)
  tags            -> tags         (array<string>, présent uniquement sur les questions)
  body            -> text
  is_help_request : post_type == "question"
  is_resolved     : answer_count > 0 (questions) | is_accepted == True (réponses)
"""
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bookmark import get_s3_client, list_new_files, read_bookmark, write_bookmark
from nettoyage import clean_id, clean_tags, clean_html_text, quality_filter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-stackoverflow")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
MAX_DATE = os.getenv("MAX_DATE")  # ex: "2026/02/20/20260220_1200" — None = pas de borne
BUCKET = "datalake"
SOURCE = "stackoverflow"


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

    count_before = df.count()
    if count_before == 0:
        logger.warning("Aucune donnée, arrêt.")
        spark.stop()
        sys.exit(0)

    is_question = F.col("post_type") == "question"

    event_id = F.when(is_question, F.col("question_id")).otherwise(F.col("answer_id")).cast("string")
    thread_id = F.col("question_id").cast("string")
    parent_id = F.when(~is_question, F.col("question_id").cast("string")).otherwise(F.lit(None))

    is_resolved = F.when(
        is_question,
        F.coalesce(F.col("answer_count"), F.lit(0)) > 0,
    ).otherwise(
        F.coalesce(F.col("is_accepted"), F.lit(False))
    )

    normalized = df.select(
        F.lit(SOURCE).alias("source"),
        F.col("owner.user_id").cast("string").alias("actor_id"),
        event_id.alias("event_id"),
        F.to_timestamp(F.col("creation_date")).alias("event_ts"),
        F.col("post_type").alias("event_type"),
        thread_id.alias("thread_id"),
        parent_id.alias("parent_id"),
        F.col("tags").alias("tags"),
        F.col("body").alias("text"),
        is_question.alias("is_help_request"),
        is_resolved.alias("is_resolved"),
    )
    normalized = (
        normalized
        .withColumn("actor_id",  clean_id(F.col("actor_id")))
        .withColumn("event_id",  clean_id(F.col("event_id")))
        .withColumn("thread_id", clean_id(F.col("thread_id")))
        .withColumn("parent_id", clean_id(F.col("parent_id")))
        .withColumn("tags",      clean_tags(F.col("tags")))
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
    normalized = normalized.cache()
    count_after = normalized.count()
    normalized.repartition(1, "year", "month", "day").write.mode("append").partitionBy("year", "month", "day").parquet(out_path)
    normalized.unpersist()
    write_bookmark(s3, BUCKET, SOURCE, new_keys[-1])
    rejected = count_before - count_after
    pct = (rejected / count_before * 100) if count_before > 0 else 0
    logger.info(
        "OK | fichiers=%d reçus=%d traités=%d rejetés=%d (%.1f%%) sortie=%s",
        len(new_keys), count_before, count_after, rejected, pct, out_path,
    )
    spark.stop()


if __name__ == "__main__":
    main()
