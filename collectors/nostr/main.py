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
logger = logging.getLogger("collector.nostr")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "uconnect.datalake.raw.nostr"
NOSTR_RELAYS = os.getenv(
    "NOSTR_RELAYS",
    ",".join(
        [
            "wss://purplepag.es",
            "wss://nostr.mom",
            "wss://relay.nostr.com.au",
            "wss://nostr.oxtr.dev",
            "wss://relay.nsec.app",
            "wss://eden.nostr.land",
            "wss://nos.lol",
            "wss://relay.damus.io",
            "wss://bitcoiner.social",
        ]
    ),
).split(",")
RECONNECT_DELAY = int(os.getenv("RECONNECT_DELAY", "10"))

SUBSCRIPTION = json.dumps(["REQ", "ucnnct-sub", {"kinds": [1, 3, 6, 7], "limit": 100}])

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        logger.error(
            "Échec de livraison vers Kafka | topic=%s partition=%s erreur=%s",
            msg.topic(),
            msg.partition(),
            err,
        )


def build_payload(event):
    event["source"] = "nostr"
    return event


async def collect_relay(relay_url):
    while True:
        try:
            async with websockets.connect(
                relay_url,
                ping_interval=30,
                ping_timeout=60,
            ) as ws:
                await ws.send(SUBSCRIPTION)
                logger.info("Abonnement actif | relay=%s kinds=[1,3,6,7]", relay_url)
                count = 0
                async for message in ws:
                    data = json.loads(message)
                    if not data or data[0] != "EVENT":
                        continue
                    event = data[2]
                    payload = build_payload(event)
                    producer.produce(
                        TOPIC,
                        key=event.get("id", "unknown"),
                        value=json.dumps(payload),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    count += 1
                    if count % 50 == 0:
                        producer.flush()
                        logger.info(
                            "Progression | relay=%s events_produits=%d topic=%s",
                            relay_url,
                            count,
                            TOPIC,
                        )
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(
                "Relay déconnecté | relay=%s code=%s | nouvelle tentative dans %ds",
                relay_url,
                e.code,
                RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)
        except Exception as e:
            logger.error(
                "Erreur relay | relay=%s type=%s message=%s"
                " | nouvelle tentative dans %ds",
                relay_url,
                type(e).__name__,
                e,
                RECONNECT_DELAY,
            )
            await asyncio.sleep(RECONNECT_DELAY)


async def collect():
    logger.info(
        "Connexion aux relays | total=%d relays=%s", len(NOSTR_RELAYS), NOSTR_RELAYS
    )
    tasks = [
        asyncio.create_task(collect_relay(relay.strip())) for relay in NOSTR_RELAYS
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    logger.info("Démarrage du collecteur | source=nostr topic=%s version=1.1.0", TOPIC)
    asyncio.run(collect())
