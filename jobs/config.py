import os

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CH_DB = os.getenv("CLICKHOUSE_DB", "uconnect")

PG_HOST = os.getenv("POSTGRES_HOST", "172.31.253.25")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
PG_DB = os.getenv("POSTGRES_DB", "uconnect")
PG_USER = os.getenv("POSTGRES_USER", "")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
PG_SCHEMA = os.getenv("POSTGRES_SCHEMA", "datamart")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "ucnnct")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", MINIO_USER)
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", MINIO_PASSWORD)
