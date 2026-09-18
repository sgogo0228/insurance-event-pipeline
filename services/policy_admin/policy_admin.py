import logging
import os
import random
import signal
import time

import psycopg

APP_DB_DSN = os.environ.get("APP_DB_DSN", "postgresql://app:app@localhost:5433/insurance_app")
INTERVAL_SECONDS = float(os.environ.get("INTERVAL_SECONDS", "3"))
TERMINATE_RATE = float(os.environ.get("TERMINATE_RATE", "0.1"))

# product_code -> (min sum insured, max sum insured, annual premium rate)
PRODUCTS = {
    "TERM_LIFE": (1_000_000, 5_000_000, 0.002),
    "MEDICAL": (100_000, 1_000_000, 0.03),
    "ACCIDENT": (500_000, 3_000_000, 0.004),
}

INSERT_POLICY = """
    insert into policies (customer_id, product_code, sum_insured, annual_premium)
    values (%s, %s, %s, %s)
    returning policy_id
"""

TERMINATE_POLICY = """
    update policies
    set status = %s, updated_at = now()
    where policy_id = (select policy_id from policies where status = 'active' order by random() limit 1)
    returning policy_id
"""

log = logging.getLogger("policy-admin")
running = True


def stop(*_):
    global running
    running = False


def new_policy_values(rng: random.Random) -> tuple:
    product_code = rng.choice(list(PRODUCTS))
    low, high, rate = PRODUCTS[product_code]
    sum_insured = rng.randrange(low, high + 1, 100_000)
    return f"C{rng.randint(1, 500):05d}", product_code, sum_insured, round(sum_insured * rate)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    rng = random.Random()

    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        while running:
            if rng.random() < TERMINATE_RATE:
                status = rng.choice(["lapsed", "surrendered"])
                row = conn.execute(TERMINATE_POLICY, (status,)).fetchone()
                if row:
                    log.info("policy %s -> %s", row[0], status)
            else:
                values = new_policy_values(rng)
                row = conn.execute(INSERT_POLICY, values).fetchone()
                log.info("policy %s issued (%s)", row[0], values[1])
            time.sleep(INTERVAL_SECONDS)

    log.info("policy admin stopped")


if __name__ == "__main__":
    main()
