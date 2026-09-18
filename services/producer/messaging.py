import json
import logging
import os
import time

import psycopg
from confluent_kafka import Producer
from psycopg.rows import dict_row

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = os.environ.get("TOPIC", "insurance_events")
APP_DB_DSN = os.environ.get("APP_DB_DSN", "postgresql://app:app@localhost:5433/insurance_app")

log = logging.getLogger("producer")


def create_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery failed: %s", err)


def publish(producer: Producer, event: dict, tag: str = "") -> None:
    producer.produce(
        TOPIC,
        # Same policy -> same partition, so the events of one policy stay in order.
        key=event["payload"]["policy_id"],
        value=json.dumps(event),
        on_delivery=_on_delivery,
    )
    log.info("%-12s %s event_id=%s%s", event["event_type"], event["payload"]["policy_id"], event["event_id"], tag)


class ActivePolicies:
    """Reads active policies from the policy admin DB, cached to avoid querying on every event."""

    def __init__(self, dsn: str = APP_DB_DSN, refresh_seconds: float = 10):
        self.dsn = dsn
        self.refresh_seconds = refresh_seconds
        self._policies: list[dict] = []
        self._loaded_at = float("-inf")

    def get(self) -> list[dict]:
        if time.monotonic() - self._loaded_at >= self.refresh_seconds:
            with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
                self._policies = conn.execute(
                    "select policy_id, product_code, sum_insured, annual_premium from policies where status = 'active'"
                ).fetchall()
            self._loaded_at = time.monotonic()
        return self._policies
