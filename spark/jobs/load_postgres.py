"""
Chargement curated -> staging.events PostgreSQL (incrémental).

Pour chaque source, lit le MAX(event_ts) dans staging.events (watermark),
puis charge uniquement les événements plus récents depuis curated Parquet.
Déduplication (source, event_id) avant écriture JDBC.
"""

import logging
import os
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
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
PG_HOST = os.getenv("POSTGRES_HOST", "172.31.250.57")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "uconnect")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
BUCKET = "datalake"
ALL_SOURCES = ["bluesky", "nostr", "hackernews", "rss", "stackoverflow"]

_sources_env = os.getenv("LOAD_SOURCES", "").strip()
_sources_list = [
    s.strip()
    for s in _sources_env.split(",")
    if s.strip() and s.strip().lower() != "none"
]
SOURCES = _sources_list if _sources_list else ALL_SOURCES
DATE_DEBUT = os.getenv("LOAD_DATE_DEBUT", "").strip() or None
if DATE_DEBUT and DATE_DEBUT.lower() == "none":
    DATE_DEBUT = None
DATE_FIN = os.getenv("LOAD_DATE_FIN", "").strip() or None
if DATE_FIN and DATE_FIN.lower() == "none":
    DATE_FIN = None


def build_spark():
    """Construit la session Spark avec les configs S3/MinIO."""
    return (
        SparkSession.builder.appName("load-postgres")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )


def get_watermark(spark, jdbc_url, props, source):
    """Retourne le MAX(event_ts) pour une source."""
    query = (
        f"(SELECT COALESCE(MAX(event_ts), TIMESTAMP '1970-01-01 00:00:00') "
        f"AS wm FROM staging.events WHERE source = '{source}') AS t"
    )
    try:
        wm = spark.read.jdbc(jdbc_url, query, properties=props).collect()[0][
            "wm"
        ]
        logger.info("Watermark %s : %s", source, wm)
        return wm
    except Exception:
        logger.warning(
            "Watermark %s inaccessible — chargement intégral",
            source,
            exc_info=True,
        )
        return None


def execute_sql_returning_int(spark, jdbc_url, props, sql):
    """Exécute une requête SQL brute et retourne un entier."""
    user = props.get("user")
    pwd = props.get("password")
    driver = props.get("driver", "org.postgresql.Driver")
    jvm = spark._jvm
    jvm.java.lang.Class.forName(driver)
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, pwd)
    try:
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        result = 0
        if rs.next():
            result = rs.getInt(1)
        rs.close()
        stmt.close()
        return result
    finally:
        conn.close()


def execute_sql(spark, jdbc_url, props, sql):
    """Exécute une commande SQL brute sans retour."""
    user = props.get("user")
    pwd = props.get("password")
    driver = props.get("driver", "org.postgresql.Driver")
    jvm = spark._jvm
    jvm.java.lang.Class.forName(driver)
    conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, user, pwd)
    try:
        stmt = conn.createStatement()
        stmt.execute(sql)
        stmt.close()
    finally:
        conn.close()


def main():
    """Job principal de chargement vers PostgreSQL."""
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    jdbc_url = (
        f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        "?reWriteBatchedInserts=true"
    )
    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
        "isolationLevel": "READ_UNCOMMITTED",
    }

    try:
        logger.info(
            "Sources : %s | date_debut : %s | date_fin : %s",
            SOURCES,
            DATE_DEBUT or "watermark auto",
            DATE_FIN or "aucune",
        )
        frames = []
        for source in SOURCES:
            path = f"s3a://{BUCKET}/curated/{source}"
            try:
                df = spark.read.parquet(path)
                if DATE_DEBUT:
                    df = df.filter(F.col("event_ts") >= F.lit(DATE_DEBUT))
                else:
                    wm = get_watermark(spark, jdbc_url, jdbc_props, source)
                    if wm is not None:
                        df = df.filter(F.col("event_ts") > F.lit(wm))
                if DATE_FIN:
                    df = df.filter(F.col("event_ts") <= F.lit(DATE_FIN))
                frames.append(df)
            except Exception:
                logger.warning("Pas de données %s", source, exc_info=True)

        if not frames:
            logger.warning("Aucune donnée à charger, arrêt.")
            return

        combined = reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True), frames
        )
        combined = (
            combined.drop("year", "month", "day")
            .withColumn("tags", F.array_join(F.col("tags"), ","))
            .dropDuplicates(["source", "event_id"])
            .repartition(20)
        )

        target_table = "staging.events"
        load_table = "staging.events_load"

        logger.info("Préparation de la table de staging %s...", load_table)
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS {load_table} "
            f"(LIKE {target_table} INCLUDING DEFAULTS EXCLUDING INDEXES)"
        )
        execute_sql(spark, jdbc_url, jdbc_props, create_sql)
        execute_sql(
            spark, jdbc_url, jdbc_props, f"TRUNCATE TABLE {load_table}"
        )

        logger.info("Écriture du batch dans la table de staging...")
        combined.write.option("batchsize", "20000").option(
            "numPartitions", "8"
        ).jdbc(
            url=jdbc_url,
            table=load_table,
            mode="append",
            properties=jdbc_props,
        )

        batch_q = f"(SELECT COUNT(*) AS cnt FROM {load_table}) AS t"
        batch_count = spark.read.jdbc(
            jdbc_url, batch_q, properties=jdbc_props
        ).collect()[0]["cnt"]

        logger.info(
            "Fusion finale (INSERT ... ON CONFLICT DO NOTHING)..."
        )
        upsert_query = f"""
            WITH inserted AS (
                INSERT INTO {target_table}
                SELECT * FROM {load_table}
                ON CONFLICT (source, event_id) DO NOTHING
                RETURNING 1
            )
            SELECT COUNT(*) FROM inserted
        """
        inserted_count = execute_sql_returning_int(
            spark, jdbc_url, jdbc_props, upsert_query
        )
        duplicate_count = batch_count - inserted_count

        logger.info(
            "Fusion terminée : %d lignes insérées | %d doublons ignorés",
            inserted_count,
            duplicate_count,
        )

        execute_sql(
            spark, jdbc_url, jdbc_props, f"TRUNCATE TABLE {load_table}"
        )

        total_q = f"(SELECT COUNT(*) AS cnt FROM {target_table}) AS t"
        total = spark.read.jdbc(
            jdbc_url, total_q, properties=jdbc_props
        ).collect()[0]["cnt"]
        logger.info(
            "OK | Chargement terminé | Total final en base : %d", total
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
