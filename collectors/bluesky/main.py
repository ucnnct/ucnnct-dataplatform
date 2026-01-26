import os
import json
import asyncio
import logging
import websockets
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.bluesky")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "uconnect.datalake.raw.bluesky"
JETSTREAM_URL = os.getenv(
    "JETSTREAM_URL",
    "wss://jetstream2.us-east.bsky.network/subscribe"
    "?wantedCollections=app.bsky.feed.post"
    "&wantedCollections=app.bsky.feed.like"
    "&wantedCollections=app.bsky.graph.follow",
)
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "5"))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        logger.error("Échec de livraison vers Kafka | topic=%s partition=%s erreur=%s",
                     msg.topic(), msg.partition(), err)


def build_payload(data):
    data["source"] = "bluesky"
    return data


async def collect():
    while True:
        try:
            async with websockets.connect(
                JETSTREAM_URL,
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                logger.info("Connexion établie avec Bluesky Jetstream | url=%s", JETSTREAM_URL)
                count = 0
                async for message in ws:
                    data = json.loads(message)
                    if data.get("kind") != "commit":
                        continue
                    payload = build_payload(data)
                    key = payload.get("commit", {}).get("cid") or payload.get("did", "unknown")
                    producer.produce(
                        TOPIC,
                        key=key,
                        value=json.dumps(payload),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    count += 1
                    if count % 100 == 0:
                        producer.flush()
                        logger.info("Progression | messages_produits=%d topic=%s", count, TOPIC)
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning("Connexion Jetstream fermée | code=%s raison=%s | nouvelle tentative dans %ds",
                           e.code, e.reason, RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            logger.error("Erreur inattendue | type=%s message=%s | nouvelle tentative dans %ds",
                         type(e).__name__, e, RECONNECT_DELAY)
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    logger.info("Démarrage du collecteur | source=bluesky topic=%s", TOPIC)
    asyncio.run(collect())
