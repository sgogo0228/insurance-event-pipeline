import json
import logging
import os
import signal

import psycopg
from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.environ.get("TOPIC", "insurance_events")
GROUP_ID = os.environ.get("GROUP_ID", "notification-service")
APP_DB_DSN = os.environ.get("APP_DB_DSN", "postgresql://app:app@localhost:5433/insurance_app")

INSERT_NOTIFICATION = """
    insert into notifications (claim_id, policy_id, claim_amount, event_id)
    values (%s, %s, %s, %s)
    on conflict (claim_id) do nothing
"""

log = logging.getLogger("notification-service")
running = True


def stop(*_):
    global running
    running = False


def send_notification(payload: dict) -> None:
    log.info("notify customer: claim %s received for policy %s (amount %s)",
             payload["claim_id"], payload["policy_id"], payload["claim_amount"])


def handle_claim(conn: psycopg.Connection, event: dict) -> None:
    payload = event["payload"]
    # The unique claim_id is checked in the same transaction as the send:
    # a duplicate event is rejected before anything reaches the customer.
    with conn.transaction():
        cur = conn.execute(
            INSERT_NOTIFICATION,
            (payload["claim_id"], payload["policy_id"], payload["claim_amount"], event["event_id"]),
        )
        if cur.rowcount == 0:
            log.info("skip claim %s: customer already notified", payload["claim_id"])
            return
        send_notification(payload)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    # A separate consumer group gets its own full copy of the topic, independent of the warehouse loader.
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        while running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            try:
                event = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.warning("skip malformed message at offset %s", msg.offset())
                event = None
            if event and event.get("event_type") == "claim_filed":
                handle_claim(conn, event)
            consumer.commit(message=msg, asynchronous=False)

    consumer.close()


if __name__ == "__main__":
    main()
