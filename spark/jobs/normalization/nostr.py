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
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("normalize-nostr")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
RUN_DATE       = os.getenv("RUN_DATE", date.today().strftime("%Y/%m/%d"))
BUCKET         = "datalake"
SOURCE         = "nostr"


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


def _extract_thread_ref(tags):
    if not tags:
        return None
    for tag in tags:
        if len(tag) >= 4 and tag[0] == "e" and tag[3] == "root":
            return tag[1]
    for tag in tags:
        if len(tag) >= 2 and tag[0] == "e":
            return tag[1]
    return None


def _extract_parent_ref(tags):
    if not tags:
        return None
    for tag in tags:
        if len(tag) >= 4 and tag[0] == "e" and tag[3] == "reply":
            return tag[1]
    e_tags = [t for t in tags if len(t) >= 2 and t[0] == "e"]
    return e_tags[-1][1] if e_tags else None


def _extract_p_tags(tags):
    if not tags:
        return []
    return [t[1] for t in tags if len(t) >= 2 and t[0] == "p"]


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    udf_thread_ref = F.udf(_extract_thread_ref, StringType())
    udf_parent_ref = F.udf(_extract_parent_ref, StringType())
    udf_p_tags     = F.udf(_extract_p_tags, ArrayType(StringType()))

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
            F.col("pubkey").alias("actor_id"),
            F.col("id").alias("event_id"),
            F.to_timestamp(F.col("created_at")).alias("event_ts"),
            F.col("kind").cast("string").alias("event_type"),
            udf_thread_ref(F.col("tags")).alias("thread_id"),
            udf_parent_ref(F.col("tags")).alias("parent_id"),
            udf_p_tags(F.col("tags")).alias("tags"),
            F.col("content").alias("text"),
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
