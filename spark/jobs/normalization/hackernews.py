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
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-hackernews")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
RUN_DATE       = os.getenv("RUN_DATE", date.today().strftime("%Y/%m/%d"))
BUCKET         = "datalake"
SOURCE         = "hackernews"


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

    is_ask = F.col("type") == "ask"
    parent_col = F.col("parent").cast("string") if "parent" in df.columns else F.lit(None).cast("string")

    normalized = (
        df.select(
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
