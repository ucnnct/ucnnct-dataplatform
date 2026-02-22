"""
Normalisation Bluesky : raw JSONL -> curated Parquet

Détournement selon docs/Détournement_Expliqué.pdf :
  did                             -> actor_id
  commit.cid                      -> event_id
  time_us                         -> event_ts  (microsecondes -> timestamp)
  commit.collection               -> event_type
  commit.rkey                     -> thread_id
  commit.record.reply.parent.uri  -> parent_id
  commit.record.langs             -> tags
  commit.record.text              -> text
  is_help_request / is_resolved   -> False
"""
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from bookmark import get_s3_client, list_new_files, read_bookmark, write_bookmark
from nettoyage import clean_id, clean_tags, clean_text


BLUESKY_SCHEMA = StructType([
    StructField("did",     StringType(), True),
    StructField("time_us", LongType(),   True),
    StructField("kind",    StringType(), True),
    StructField("commit", StructType([
        StructField("cid",        StringType(), True),
        StructField("operation",  StringType(), True),
        StructField("collection", StringType(), True),
        StructField("rkey",       StringType(), True),
        StructField("record", StructType([
            StructField("text",  StringType(), True),
            StructField("langs", ArrayType(StringType()), True),
            StructField("reply", StructType([
                StructField("parent", StructType([
                    StructField("uri", StringType(), True),
                ]), True),
                StructField("root", StructType([
                    StructField("uri", StringType(), True),
                ]), True),
            ]), True),
        ]), True),
    ]), True),
])

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-bluesky")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
MAX_DATE       = os.getenv("MAX_DATE")  # ex: "2026/02/20/20260220_1200" — None = pas de borne
BUCKET         = "datalake"
SOURCE         = "bluesky"


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
    s3       = get_s3_client(MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD)
    last_key = read_bookmark(s3, BUCKET, SOURCE)
    end_key  = f"raw/{SOURCE}/{MAX_DATE}" if MAX_DATE else None
    new_keys = list_new_files(s3, BUCKET, f"raw/{SOURCE}/", last_key, end_key)

    if not new_keys:
        logger.info("Aucun nouveau fichier pour %s, arrêt.", SOURCE)
        sys.exit(0)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    s3a_paths = [f"s3a://{BUCKET}/{k}" for k in new_keys]
    logger.info("Lecture de %d fichiers", len(s3a_paths))

    try:
        df = spark.read.schema(BLUESKY_SCHEMA).json(s3a_paths)
    except Exception as e:
        logger.warning("Lecture impossible : %s", e)
        spark.stop()
        sys.exit(0)

    if df.limit(1).count() == 0:
        logger.warning("Aucune donnée, arrêt.")
        spark.stop()
        sys.exit(0)

    normalized = df.select(
        F.lit(SOURCE).alias("source"),
        F.col("did").alias("actor_id"),
        F.col("commit.cid").alias("event_id"),
        F.to_timestamp(F.col("time_us") / 1_000_000).alias("event_ts"),
        F.col("commit.collection").alias("event_type"),
        F.col("commit.rkey").alias("thread_id"),
        F.col("commit.record.reply.parent.uri").alias("parent_id"),
        F.col("commit.record.langs").alias("tags"),
        F.col("commit.record.text").alias("text"),
        F.lit(False).cast("boolean").alias("is_help_request"),
        F.lit(False).cast("boolean").alias("is_resolved"),
    )
    normalized = (
        normalized
        .withColumn("actor_id",  clean_id(F.col("actor_id")))
        .withColumn("event_id",  clean_id(F.col("event_id")))
        .withColumn("thread_id", clean_id(F.col("thread_id")))
        .withColumn("parent_id", clean_id(F.col("parent_id")))
        .withColumn("tags",      clean_tags(F.col("tags")))
        .withColumn("text",      clean_text(F.col("text")))
    )
    # Pour les posts : text requis. Pour likes/follows : text peut être null.
    is_post = F.col("event_type") == "app.bsky.feed.post"
    now = F.current_timestamp()
    normalized = (
        normalized
        .filter(
            F.col("event_id").isNotNull() & (F.length(F.col("event_id")) > 0) &
            F.col("actor_id").isNotNull() & (F.length(F.col("actor_id")) > 0) &
            F.col("event_ts").isNotNull() & (F.year(F.col("event_ts")) >= 2020) &
            (~is_post | (F.col("text").isNotNull() & (F.length(F.col("text")) > 0)))
        )
        .withColumn("event_ts", F.when(F.col("event_ts") > now, now).otherwise(F.col("event_ts")))
        .dropDuplicates(["source", "event_id"])
    )
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
