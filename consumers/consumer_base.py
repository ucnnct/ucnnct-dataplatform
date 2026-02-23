import os
import json
import time
import uuid
import logging
from datetime import datetime, timezone
from io import BytesIO
from confluent_kafka import Consumer, KafkaError
import redis
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
BUCKET = "datalake"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "60"))
DEDUP_TTL = int(os.getenv("DEDUP_TTL", "86400"))


def run(source: str, topic: str, group: str) -> None:
    if not MINIO_USER or not MINIO_PASSWORD:
        raise RuntimeError("MINIO_ROOT_USER et MINIO_ROOT_PASSWORD sont requis")

    logger = logging.getLogger(f"consumer.{source}")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "session.timeout.ms": "300000",
            "max.poll.interval.ms": "600000",
            "heartbeat.interval.ms": "10000",
            "fetch.min.bytes": "1048576",
            "fetch.wait.max.ms": "500",
            "max.partition.fetch.bytes": "10485760",
        }
    )
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    minio_client = Minio(
        MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=False
    )

    def flush(buffer):
        now = datetime.now(timezone.utc)
        path = (
            f"raw/{source}/{now.strftime('%Y/%m/%d')}/"
            f"{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.jsonl"
        )
        content = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
        try:
            minio_client.put_object(
                BUCKET,
                path,
                data=BytesIO(content),
                length=len(content),
                content_type="application/x-ndjson",
            )
            logger.info("Flush MinIO | path=%s records=%d", path, len(buffer))
        except Exception as exc:
            logger.error("Échec flush MinIO | path=%s erreur=%s", path, exc)
            raise

    consumer.subscribe([topic])
    buffer = []
    last_flush = time.time()
    logger.info(
        "Démarrage | source=%s topic=%s group=%s batch=%d interval=%ds",
        source,
        topic,
        group,
        BATCH_SIZE,
        FLUSH_INTERVAL,
    )

    try:
        while True:
            msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
            for msg in msgs:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Erreur Kafka | %s", msg.error())
                    continue

                key = msg.key().decode("utf-8") if msg.key() else ""
                dedup_key = f"dedup:{source}:{key}"

                # SET NX atomique — évite la race condition exists+setex
                if key and not redis_client.set(dedup_key, "1", nx=True, ex=DEDUP_TTL):
                    logger.debug("Doublon ignoré | key=%s", key)
                    continue

                if msg.value() is None:
                    logger.warning("Message vide ignoré | key=%s", key)
                    continue
                try:
                    buffer.append(json.loads(msg.value()))
                except json.JSONDecodeError:
                    logger.warning("Message JSON invalide | key=%s", key)

            if len(buffer) >= BATCH_SIZE or (
                buffer and time.time() - last_flush >= FLUSH_INTERVAL
            ):
                flush(buffer)
                consumer.commit(asynchronous=False)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        if buffer:
            try:
                flush(buffer)
                consumer.commit(asynchronous=False)
            except Exception as exc:
                logger.error("Échec flush final | erreur=%s", exc)
        consumer.close()
        logger.info("Consumer arrêté | source=%s", source)
