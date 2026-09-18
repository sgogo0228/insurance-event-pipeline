import logging
import os
import signal
import time
from datetime import datetime, timezone

import clickhouse_connect
from confluent_kafka import Consumer, Message

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPICS = os.environ.get("TOPICS", "insurance_events,cdc.public.policies").split(",")
GROUP_ID = os.environ.get("GROUP_ID", "warehouse-loader")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "analytics")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "analytics")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
BATCH_TIMEOUT_SECONDS = float(os.environ.get("BATCH_TIMEOUT_SECONDS", "5"))
CRASH_AFTER_WRITE = os.environ.get("CRASH_AFTER_WRITE", "false").lower() == "true"

TABLE = "raw.kafka_messages"
COLUMNS = ["topic", "kafka_partition", "kafka_offset", "message_key", "message_value", "kafka_timestamp"]

log = logging.getLogger("warehouse-loader")
running = True


def stop(*_):
    global running
    running = False


def to_row(msg: Message) -> list | None:
    """Keep the message untouched; parsing belongs to dbt staging (schema-on-read)."""
    if msg.value() is None:
        return None
    key = msg.key()
    _, timestamp_ms = msg.timestamp()
    return [
        msg.topic(),
        msg.partition(),
        msg.offset(),
        key.decode("utf-8", errors="replace") if key is not None else None,
        msg.value().decode("utf-8", errors="replace"),
        datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
    ]


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    clickhouse = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            # Offsets are committed only after the batch is stored -> at-least-once delivery.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(TOPICS)

    batch: list[list] = []
    consumed = 0
    batch_started = time.monotonic()

    def flush() -> None:
        nonlocal batch, consumed, batch_started
        if batch:
            # ClickHouse prefers fewer, larger inserts: each insert creates a new data part on disk.
            clickhouse.insert(TABLE, batch, column_names=COLUMNS)
            log.info("stored %d messages", len(batch))
            if CRASH_AFTER_WRITE:
                log.warning("CRASH_AFTER_WRITE: exiting before committing offsets")
                os._exit(1)
        consumer.commit(asynchronous=False)
        batch, consumed, batch_started = [], 0, time.monotonic()

    while running:
        msg = consumer.poll(1.0)
        if msg is not None:
            if msg.error():
                log.error("kafka error: %s", msg.error())
                continue
            consumed += 1
            row = to_row(msg)
            if row is not None:
                batch.append(row)

        if consumed and (len(batch) >= BATCH_SIZE or time.monotonic() - batch_started >= BATCH_TIMEOUT_SECONDS):
            flush()

    if consumed:
        flush()
    consumer.close()
    log.info("warehouse loader stopped")


if __name__ == "__main__":
    main()
