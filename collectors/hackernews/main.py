import os
import json
import time
import logging
import requests
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.hackernews")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "uconnect.datalake.raw.hackernews"
HN_API = "https://hacker-news.firebaseio.com/v0"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        logger.error("Échec de livraison vers Kafka | topic=%s partition=%s erreur=%s",
                     msg.topic(), msg.partition(), err)


def fetch_item(item_id):
    r = requests.get(f"{HN_API}/item/{item_id}.json", timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_new_stories():
    r = requests.get(f"{HN_API}/newstories.json", timeout=10)
    r.raise_for_status()
    return r.json()[:BATCH_SIZE]


def build_payload(item):
    item["source"] = "hackernews"
    return item


def collect():
    seen = set()
    logger.info("Début de la collecte | api=%s poll_interval=%ds batch_size=%d",
                HN_API, POLL_INTERVAL, BATCH_SIZE)
    while True:
        try:
            story_ids = fetch_new_stories()
            count = 0
            for sid in story_ids:
                if sid in seen:
                    continue
                seen.add(sid)
                item = fetch_item(sid)
                if not item:
                    logger.debug("Item vide ignoré | id=%s", sid)
                    continue
                payload = build_payload(item)
                producer.produce(
                    TOPIC,
                    key=str(payload.get("id", "")),
                    value=json.dumps(payload),
                    callback=delivery_report,
                )
                producer.poll(0)
                count += 1
            producer.flush()
            logger.info("Batch traité | nouveaux_items=%d total_vus=%d topic=%s prochaine_collecte=%ds",
                        count, len(seen), TOPIC, POLL_INTERVAL)
        except requests.exceptions.Timeout:
            logger.warning("Timeout API HackerNews | prochaine tentative dans %ds", POLL_INTERVAL)
        except requests.exceptions.RequestException as e:
            logger.error("Erreur réseau HackerNews | type=%s message=%s", type(e).__name__, e)
        except Exception as e:
            logger.error("Erreur inattendue | type=%s message=%s", type(e).__name__, e)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    logger.info("Démarrage du collecteur | source=hackernews topic=%s", TOPIC)
    collect()
