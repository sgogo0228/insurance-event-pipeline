import json
import logging
import os
import signal
import time
from typing import Callable

import psycopg
from confluent_kafka import Producer
from psycopg.rows import dict_row

from events import claim_event, payment_event

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.environ.get("TOPIC", "insurance_events")
APP_DB_DSN = os.environ.get("APP_DB_DSN", "postgresql://app:app@localhost:5433/insurance_app")
POLL_INTERVAL_SECONDS = float(os.environ.get("POLL_INTERVAL_SECONDS", "1"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
CRASH_AFTER_PUBLISH = os.environ.get("CRASH_AFTER_PUBLISH", "false").lower() == "true"

# Source table -> (query for rows after the checkpoint, row-to-event mapper).
# Polling by id works because these tables are append-only; mutable data (policies) goes through log-based CDC.
SOURCES: dict[str, tuple[str, Callable[[dict], dict]]] = {
    "payments": (
        "select id, event_id, payment_id, policy_id, amount, payment_method, paid_at"
        " from payments where id > %s order by id limit %s",
        payment_event,
    ),
    "claims": (
        "select id, event_id, claim_id, policy_id, claim_amount, claim_detail, filed_at"
        " from claims where id > %s order by id limit %s",
        claim_event,
    ),
}

log = logging.getLogger("producer")
running = True


def stop(*_):
    global running
    running = False


def load_checkpoint(conn: psycopg.Connection, source: str) -> int:
    return conn.execute("select last_id from publisher_checkpoints where source_table = %s", (source,)).fetchone()["last_id"]


def save_checkpoint(conn: psycopg.Connection, source: str, last_id: int) -> None:
    conn.execute(
        "update publisher_checkpoints set last_id = %s, updated_at = now() where source_table = %s",
        (last_id, source),
    )


def publish_all(producer: Producer, events: list[dict]) -> bool:
    """Publish and wait until Kafka acknowledged every event; False if any delivery failed."""
    failures = []

    def on_delivery(err, _msg):
        if err is not None:
            failures.append(err)

    for event in events:
        producer.produce(
            TOPIC,
            # Same policy -> same partition, so the events of one policy stay in order.
            key=event["payload"]["policy_id"],
            value=json.dumps(event),
            on_delivery=on_delivery,
        )
        log.info("%-12s %s event_id=%s", event["event_type"], event["payload"]["policy_id"], event["event_id"])
    remaining = producer.flush(30)
    if failures or remaining:
        log.error("publish failed: %d errors, %d not delivered (%s)", len(failures), remaining, failures[:1])
        return False
    return True


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})

    with psycopg.connect(APP_DB_DSN, autocommit=True, row_factory=dict_row) as conn:
        while running:
            new_rows = {}
            for source, (query, _) in SOURCES.items():
                rows = conn.execute(query, (load_checkpoint(conn, source), BATCH_SIZE)).fetchall()
                if rows:
                    new_rows[source] = rows
            if not new_rows:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            events = [SOURCES[source][1](row) for source, rows in new_rows.items() for row in rows]
            # Publish first, then move the checkpoints: a crash in between re-publishes rows (at-least-once).
            if not publish_all(producer, events):
                time.sleep(POLL_INTERVAL_SECONDS)
                continue
            if CRASH_AFTER_PUBLISH:
                log.warning("CRASH_AFTER_PUBLISH: exiting before saving the checkpoints")
                os._exit(1)
            for source, rows in new_rows.items():
                save_checkpoint(conn, source, rows[-1]["id"])

    log.info("producer stopped")


if __name__ == "__main__":
    main()
