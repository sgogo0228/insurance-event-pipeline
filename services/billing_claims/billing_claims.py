import logging
import os
import random
import signal
import time

import psycopg

from records import APP_DB_DSN, ActivePolicies, insert_claim, insert_payment, new_claim, new_payment

RECORDS_PER_SECOND = float(os.environ.get("RECORDS_PER_SECOND", "2"))
CLAIM_RATE = float(os.environ.get("CLAIM_RATE", "0.1"))

log = logging.getLogger("billing-claims")
running = True


def stop(*_):
    global running
    running = False


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    rng = random.Random()
    active_policies = ActivePolicies()

    # The system only writes to its own database; it knows nothing about Kafka.
    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        while running:
            policies = active_policies.get()
            if not policies:
                log.info("no active policies yet, waiting")
            elif rng.random() < CLAIM_RATE:
                claim = new_claim(rng, rng.choice(policies))
                insert_claim(conn, claim)
                log.info("claim %s filed for policy %s", claim["claim_id"], claim["policy_id"])
            else:
                payment = new_payment(rng, rng.choice(policies))
                insert_payment(conn, payment)
                log.info("payment %s received for policy %s", payment["payment_id"], payment["policy_id"])
            time.sleep(1 / RECORDS_PER_SECOND)

    log.info("billing & claims system stopped")


if __name__ == "__main__":
    main()
