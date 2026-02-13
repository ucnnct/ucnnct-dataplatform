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
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-bluesky")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
RUN_DATE       = os.getenv("RUN_DATE", date.today().strftime("%Y/%m/%d"))
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
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"s3a://{BUCKET}/raw/{SOURCE}/{RUN_DATE}/*.jsonl"
    logger.info("Lecture : %s", input_path)

    try:
        df = spark.read.json(input_path)
    except Exception as e:
        logger.warning("Aucun fichier pour %s : %s", RUN_DATE, e)
        spark.stop()
        sys.exit(0)

    if df.limit(1).count() == 0:
        logger.warning("Aucune donnée pour %s, arrêt.", RUN_DATE)
        spark.stop()
        sys.exit(0)

    normalized = (
        df.select(
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
        .withColumn("year",  F.year("event_ts"))
        .withColumn("month", F.month("event_ts"))
        .withColumn("day",   F.dayofmonth("event_ts"))
        .filter(F.col("event_ts").isNotNull() & (F.col("event_ts") <= F.current_timestamp()))
    )

    out_path = f"s3a://{BUCKET}/curated/{SOURCE}"
    normalized.write.mode("overwrite").partitionBy("year", "month", "day").parquet(out_path)
    logger.info("OK | date=%s sortie=%s", RUN_DATE, out_path)
    spark.stop()


if __name__ == "__main__":
    main()
