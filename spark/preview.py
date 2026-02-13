"""
Aperçu du contenu curated dans MinIO local.
Usage :
  spark-submit preview.py [source]
  SOURCE=bluesky spark-submit preview.py
"""
import os
import sys

from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "test-minio:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
SOURCE         = sys.argv[1] if len(sys.argv) > 1 else os.getenv("SOURCE", "bluesky")
BUCKET         = "datalake"

spark = (
    SparkSession.builder
    .appName(f"preview-{SOURCE}")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

path = f"s3a://{BUCKET}/curated/{SOURCE}/"
print(f"\n{'='*50}")
print(f" Source : {SOURCE}")
print(f" Path   : {path}")
print(f"{'='*50}")

df = spark.read.parquet(path)

print("\nSchema :")
df.printSchema()

print(f"\nNombre de lignes : {df.count():,}")

print("\nAperçu (10 lignes) :")
df.show(10, truncate=80, vertical=True)

print("\nRépartition event_type :")
df.groupBy("event_type").count().orderBy("count", ascending=False).show()

print("\nNulls par colonne :")
from pyspark.sql import functions as F
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show()

spark.stop()
