import os
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.stackoverflow")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC            = "uconnect.datalake.raw.stackoverflow"
SO_API_KEY       = os.getenv("SO_API_KEY", "rl_op3u9xnruk9ZXrLAf3MKMChQg")
SO_API           = "https://api.stackexchange.com/2.3"
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "100"))

SITES = os.getenv("SO_SITES", ",".join([
    "stackoverflow", "superuser", "serverfault",
    "askubuntu", "datascience", "ai", "softwareengineering",
])).split(",")

REQUESTS_PER_POLL = len(SITES) * 2        # questions + answers par poll
DAILY_QUOTA       = 10_000
QUOTA_RESERVE     = 50                    # seuil d'arrêt d'urgence
MIN_INTERVAL      = 60                    # intervalle minimum en secondes
MAX_INTERVAL      = 3600                  # intervalle maximum en secondes

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        logger.error("Échec de livraison vers Kafka | topic=%s partition=%s erreur=%s",
                     msg.topic(), msg.partition(), err)


def seconds_until_reset():
    now = datetime.utcnow()
    midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return (midnight - now).total_seconds()


def compute_interval(quota_remaining):
    seconds_left  = seconds_until_reset()
    safe_polls    = (quota_remaining - QUOTA_RESERVE) / REQUESTS_PER_POLL
    if safe_polls <= 0:
        return None
    interval = seconds_left / safe_polls
    return max(MIN_INTERVAL, min(interval, MAX_INTERVAL))


def fetch_items(endpoint, site, fromdate):
    params = {
        "order": "desc", "sort": "creation",
        "site": site, "pagesize": BATCH_SIZE,
        "filter": "withbody", "fromdate": fromdate,
        "key": SO_API_KEY,
    }
    r = requests.get(f"{SO_API}/{endpoint}", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("error_id"):
        raise Exception(f"Erreur API StackExchange {data.get('error_id')}: {data.get('error_message')}")
    return data.get("items", []), data.get("quota_remaining")


def collect():
    last_ts        = int(time.time()) - 3600
    quota_remaining = DAILY_QUOTA
    poll_interval  = compute_interval(quota_remaining)

    logger.info("Démarrage du collecteur | source=stackoverflow topic=%s sites=%d quota_journalier=%d",
                TOPIC, len(SITES), DAILY_QUOTA)
    logger.info("Intervalle initial calculé | interval=%ds requetes_par_poll=%d",
                int(poll_interval), REQUESTS_PER_POLL)

    while True:
        if quota_remaining <= QUOTA_RESERVE:
            wait = seconds_until_reset()
            logger.warning("Quota épuisé | quota_restant=%d pause_jusqu_au_reset=%.0fs (%.1fh)",
                           quota_remaining, wait, wait / 3600)
            time.sleep(wait + 10)
            quota_remaining = DAILY_QUOTA
            poll_interval   = compute_interval(quota_remaining)
            logger.info("Quota réinitialisé | nouvel_intervalle=%ds", int(poll_interval))
            continue

        total = 0
        for site in SITES:
            try:
                questions, quota_remaining = fetch_items("questions", site, last_ts)
                for item in questions:
                    item["source"]    = "stackoverflow"
                    item["post_type"] = "question"
                    item["site"]      = site
                    producer.produce(TOPIC,
                                     key=f"{site}_q_{item.get('question_id', '')}",
                                     value=json.dumps(item),
                                     callback=delivery_report)
                    producer.poll(0)
                total += len(questions)

                answers, quota_remaining = fetch_items("answers", site, last_ts)
                for item in answers:
                    item["source"]    = "stackoverflow"
                    item["post_type"] = "answer"
                    item["site"]      = site
                    producer.produce(TOPIC,
                                     key=f"{site}_a_{item.get('answer_id', '')}",
                                     value=json.dumps(item),
                                     callback=delivery_report)
                    producer.poll(0)
                total += len(answers)

                logger.debug("Site traité | site=%s questions=%d réponses=%d quota_restant=%d",
                             site, len(questions), len(answers), quota_remaining)

            except requests.exceptions.Timeout:
                logger.warning("Timeout API | site=%s", site)
            except Exception as e:
                logger.error("Erreur collecte | site=%s type=%s message=%s",
                             site, type(e).__name__, e)

        producer.flush()
        last_ts       = int(time.time()) - 120
        poll_interval = compute_interval(quota_remaining)

        if poll_interval is None:
            logger.warning("Quota critique | quota_restant=%d arrêt préventif", quota_remaining)
            continue

        logger.info("Batch terminé | items=%d quota_restant=%d intervalle_adaptatif=%ds reset_dans=%.1fh",
                    total, quota_remaining, int(poll_interval), seconds_until_reset() / 3600)

        time.sleep(poll_interval)


if __name__ == "__main__":
    logger.info("Démarrage du collecteur | source=stackoverflow topic=%s version=1.1.0", TOPIC)
    collect()
