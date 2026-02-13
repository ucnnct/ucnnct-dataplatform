"""
P4 - Chargement curated -> staging.events PostgreSQL (premise)

Lit les Parquet curated (toutes sources) pour RUN_DATE et insere dans staging.events.
tags array<string> -> TEXT comma-joined (JDBC ne supporte pas les tableaux PostgreSQL natifs).
Idempotent : DELETE du jour avant INSERT.

TODO : premise — a completer avec calcul des KPIs directement en SQL sur staging.events.
"""
import logging
import os
import sys
from datetime import date
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
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
RUN_DATE       = os.getenv("RUN_DATE", date.today().strftime("%Y/%m/%d"))
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


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    year, month, day = RUN_DATE.split("/")
    date_str = f"{year}-{int(month):02d}-{int(day):02d}"

    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
    jdbc_props = {
        "user":       PG_USER,
        "password":   PG_PASSWORD,
        "driver":     "org.postgresql.Driver",
        "preActions": f"DELETE FROM staging.events WHERE event_ts::date = '{date_str}'",
    }

    frames = []
    for source in SOURCES:
        path = (
            f"s3a://{BUCKET}/curated/{source}"
            f"/year={year}/month={int(month)}/day={int(day)}"
        )
        try:
            df = spark.read.parquet(path)
            frames.append(df)
            logger.info("Source %s : %d lignes", source, df.count())
        except Exception as e:
            logger.warning("Pas de donnees %s/%s : %s", source, RUN_DATE, e)

    if not frames:
        logger.warning("Aucune donnee a charger pour %s, arret.", RUN_DATE)
        spark.stop()
        sys.exit(0)

    combined = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
    combined = (
        combined
        .drop("year", "month", "day")
        .withColumn("tags", F.array_join(F.col("tags"), ","))
    )

    total = combined.count()
    combined.coalesce(1).write.jdbc(
        url=jdbc_url,
        table="staging.events",
        mode="append",
        properties=jdbc_props,
    )
    logger.info("OK | date=%s lignes=%d", RUN_DATE, total)
    spark.stop()


if __name__ == "__main__":
    main()
