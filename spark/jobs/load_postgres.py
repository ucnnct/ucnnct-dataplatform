"""
Chargement curated -> staging.events PostgreSQL (incrémental).

Pour chaque source, lit le MAX(event_ts) dans staging.events (watermark),
puis charge uniquement les événements plus récents depuis curated Parquet.
Déduplication (source, event_id) avant écriture JDBC.
"""
import logging
import os
import sys
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("load-postgres")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
PG_HOST        = os.getenv("POSTGRES_HOST", "172.31.250.57")
PG_PORT        = os.getenv("POSTGRES_PORT", "5432")
PG_DB          = os.getenv("POSTGRES_DB", "uconnect")
PG_USER        = os.getenv("POSTGRES_USER", "")
PG_PASSWORD    = os.getenv("POSTGRES_PASSWORD", "")
BUCKET         = "datalake"
SOURCES        = ["bluesky", "nostr", "hackernews", "rss", "stackoverflow"]


def build_spark():
    return (
        SparkSession.builder
        .appName("load-postgres")
        .config("spark.hadoop.fs.s3a.endpoint",          f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def get_watermark(spark, jdbc_url, props, source):
    """Retourne le MAX(event_ts) pour une source, ou None si la table est vide."""
    query = (
        f"(SELECT COALESCE(MAX(event_ts), TIMESTAMP '1970-01-01 00:00:00') AS wm "
        f"FROM staging.events WHERE source = '{source}') AS t"
    )
    try:
        wm = spark.read.jdbc(jdbc_url, query, properties=props).collect()[0]["wm"]
        logger.info("Watermark %s : %s", source, wm)
        return wm
    except Exception as e:
        logger.warning("Watermark %s inaccessible : %s — chargement intégral", source, e)
        return None


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url   = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    jdbc_props = {
        "user":     PG_USER,
        "password": PG_PASSWORD,
        "driver":   "org.postgresql.Driver",
    }

    frames = []
    for source in SOURCES:
        path = f"s3a://{BUCKET}/curated/{source}"
        wm   = get_watermark(spark, jdbc_url, jdbc_props, source)
        try:
            df = spark.read.parquet(path)
            if wm is not None:
                df = df.filter(F.col("event_ts") > F.lit(wm))
            count = df.count()
            logger.info("Source %s : %d nouveaux événements", source, count)
            if count > 0:
                frames.append(df)
        except Exception as e:
            logger.warning("Pas de données %s : %s", source, e)

    if not frames:
        logger.warning("Aucune donnée à charger, arrêt.")
        spark.stop()
        sys.exit(0)

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    combined = (
        combined
        .drop("year", "month", "day")
        .withColumn("tags", F.array_join(F.col("tags"), ","))
        .dropDuplicates(["source", "event_id"])
    )

    total = combined.count()
    combined.coalesce(1).write.jdbc(
        url=jdbc_url,
        table="staging.events",
        mode="append",
        properties=jdbc_props,
    )
    logger.info("OK | lignes=%d", total)
    spark.stop()


if __name__ == "__main__":
    main()
