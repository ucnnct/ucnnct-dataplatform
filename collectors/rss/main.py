import os
import json
import time
import logging
import feedparser
from email.utils import parsedate_to_datetime
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("collector.rss")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "uconnect.datalake.raw.rss"
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))
RSS_FEEDS = os.getenv(
    "RSS_FEEDS",
    ",".join(
        [
            "https://dev.to/feed",
            "https://hnrss.org/frontpage",
            "https://lobste.rs/rss",
            "https://www.reddit.com/r/machinelearning/.rss",
            "https://www.reddit.com/r/datascience/.rss",
            "https://aws.amazon.com/blogs/aws/feed/",
            "https://github.blog/feed/",
            "https://techcrunch.com/feed/",
            "https://stackoverflow.blog/feed/",
            "https://www.theverge.com/rss/index.xml",
            "https://www.wired.com/feed/rss",
            "https://feeds.bbci.co.uk/news/world/rss.xml",
            "http://feeds.reuters.com/reuters/topNews",
        ]
    ),
).split(",")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; uconnect-rss-collector/1.0)"}

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def delivery_report(err, msg):
    if err:
        logger.error(
            "Échec de livraison vers Kafka | topic=%s partition=%s erreur=%s",
            msg.topic(),
            msg.partition(),
            err,
        )


def parse_date(entry):
    for attr in ("published", "updated"):
        val = entry.get(attr)
        if val:
            try:
                return parsedate_to_datetime(val).isoformat()
            except Exception:
                return val
    return None


def build_payload(entry, feed_url, feed_title):
    return {
        "source": "rss",
        "feed_url": feed_url,
        "feed_title": feed_title,
        "actor_id": feed_title or feed_url,
        "guid": entry.get("id") or entry.get("link", ""),
        "pubDate": entry.get("published") or entry.get("updated"),
        "title": entry.get("title"),
        "summary": entry.get("summary"),
        "link": entry.get("link"),
        "author": entry.get("author"),
        "tags": [t.get("term", "") for t in entry.get("tags", [])],
        "item": entry.get("enclosures"),
    }


def collect():
    seen = set()
    logger.info(
        "Début de la collecte | feeds=%d poll_interval=%ds",
        len(RSS_FEEDS),
        POLL_INTERVAL,
    )
    while True:
        t_cycle = time.time()
        cycle_total = 0
        for feed_url in RSS_FEEDS:
            try:
                feed = feedparser.parse(feed_url.strip(), request_headers=HEADERS)
                feed_title = feed.feed.get("title", feed_url)

                if feed.bozo:
                    logger.warning(
                        "Feed malformé ignoré | feed=%s erreur=%s",
                        feed_title,
                        feed.bozo_exception,
                    )
                    continue

                count = 0
                for entry in feed.entries:
                    guid = entry.get("id") or entry.get("link", "")
                    if guid in seen:
                        continue
                    seen.add(guid)
                    payload = build_payload(entry, feed_url, feed_title)
                    producer.produce(
                        TOPIC,
                        key=guid,
                        value=json.dumps(payload),
                        callback=delivery_report,
                    )
                    producer.poll(0)
                    count += 1

                producer.flush()
                if count > 0:
                    logger.info("Feed traité | feed=%s nouveaux_items=%d", feed_title, count)
                else:
                    logger.debug("Aucun nouvel item | feed=%s", feed_title)
                cycle_total += count

            except Exception as e:
                logger.error(
                    "Erreur lors de la lecture du feed | feed=%s type=%s message=%s",
                    feed_url,
                    type(e).__name__,
                    e,
                )

        logger.info(
            "Cycle terminé | total_nouveaux=%d total_vus=%d durée=%.1fs prochaine_collecte=%ds",
            cycle_total,
            len(seen),
            time.time() - t_cycle,
            POLL_INTERVAL,
        )
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    logger.info(
        "Démarrage du collecteur | source=rss topic=%s feeds=%d version=1.1.0",
        TOPIC,
        len(RSS_FEEDS),
    )
    collect()
