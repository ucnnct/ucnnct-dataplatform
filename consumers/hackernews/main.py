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
logger = logging.getLogger("consumer.hackernews")

SOURCE         = "hackernews"
TOPIC          = "uconnect.datalake.raw.hackernews"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_GROUP    = "uconnect-consumer-hackernews"
REDIS_HOST     = os.getenv("REDIS_HOST", "redis")
REDIS_PORT     = int(os.getenv("REDIS_PORT", "6379"))
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "172.31.250.57:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "")
BUCKET         = "datalake"
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_INTERVAL", "60"))
DEDUP_TTL      = int(os.getenv("DEDUP_TTL", "86400"))

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_USER, secret_key=MINIO_PASSWORD, secure=False)


def flush(buffer):
    now = datetime.now(timezone.utc)
    path = f"raw/{SOURCE}/{now.strftime('%Y/%m/%d')}/{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.jsonl"
    content = "\n".join(json.dumps(r) for r in buffer).encode("utf-8")
    minio_client.put_object(BUCKET, path, data=BytesIO(content), length=len(content),
                            content_type="application/x-ndjson")
    logger.info("Flush MinIO | path=%s records=%d", path, len(buffer))


def consume():
    consumer.subscribe([TOPIC])
    buffer = []
    last_flush = time.time()
    logger.info("Démarrage | source=%s topic=%s group=%s batch=%d interval=%ds",
                SOURCE, TOPIC, KAFKA_GROUP, BATCH_SIZE, FLUSH_INTERVAL)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Erreur Kafka | %s", msg.error())
            else:
                key = msg.key().decode("utf-8") if msg.key() else ""
                dedup_key = f"dedup:{SOURCE}:{key}"
                if key and redis_client.exists(dedup_key):
                    logger.debug("Doublon ignoré | key=%s", key)
                else:
                    if key:
                        redis_client.setex(dedup_key, DEDUP_TTL, "1")
                    try:
                        buffer.append(json.loads(msg.value()))
                    except json.JSONDecodeError:
                        logger.warning("Message JSON invalide | key=%s", key)
                consumer.commit(asynchronous=False)

            if len(buffer) >= BATCH_SIZE or (buffer and time.time() - last_flush >= FLUSH_INTERVAL):
                flush(buffer)
                buffer = []
                last_flush = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        if buffer:
            flush(buffer)
        consumer.close()
        logger.info("Consumer arrêté | source=%s", SOURCE)


if __name__ == "__main__":
    logger.info("Démarrage du consumer | source=%s topic=%s version=1.0.0", SOURCE, TOPIC)
    consume()
