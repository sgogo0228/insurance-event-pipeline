import logging
import os
import random
import signal
import time

from events import claim_filed, premium_paid
from messaging import ActivePolicies, create_producer, publish

EVENTS_PER_SECOND = float(os.environ.get("EVENTS_PER_SECOND", "2"))
CLAIM_RATE = float(os.environ.get("CLAIM_RATE", "0.1"))
DUPLICATE_RATE = float(os.environ.get("DUPLICATE_RATE", "0.05"))

log = logging.getLogger("producer")
running = True


def stop(*_):
    global running
    running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    producer = create_producer()
    active_policies = ActivePolicies()
    rng = random.Random()

    while running:
        policies = active_policies.get()
        if policies:
            policy = rng.choice(policies)
            event = claim_filed(rng, policy) if rng.random() < CLAIM_RATE else premium_paid(rng, policy)
            publish(producer, event)
            if rng.random() < DUPLICATE_RATE:
                # Same event sent twice, e.g. an upstream system retrying after a timeout.
                publish(producer, event, " [DUPLICATE]")
        else:
            log.info("no active policies yet, waiting")

        producer.poll(0)
        time.sleep(1 / EVENTS_PER_SECOND)

    producer.flush(10)
    log.info("producer stopped")


if __name__ == "__main__":
    main()
