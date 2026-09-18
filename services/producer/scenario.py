"""Deterministic demo scenarios, run with: docker compose run --rm producer python scenario.py <name>"""

import argparse
import logging
import random
import time
import uuid

import psycopg

from events import claim_filed, premium_paid
from messaging import APP_DB_DSN, ActivePolicies, create_producer, publish


def duplicate(rng: random.Random) -> None:
    policy = rng.choice(ActivePolicies().get())
    event = claim_filed(rng, policy, claim_id_prefix="CLM-DUP")
    producer = create_producer()
    publish(producer, event)
    publish(producer, event, " [DUPLICATE]")
    producer.flush(10)
    print(f"\nSent the same claim twice: claim_id={event['payload']['claim_id']} event_id={event['event_id']}")


def late(rng: random.Random, delay_seconds: int) -> None:
    policy = {
        "policy_id": f"PLATE-{uuid.uuid4().hex[:6]}",
        "customer_id": "C00001",
        "product_code": "MEDICAL",
        "sum_insured": 500_000,
        "annual_premium": 15_000,
    }
    producer = create_producer()
    publish(producer, premium_paid(rng, policy), " [ARRIVES BEFORE ITS POLICY]")
    producer.flush(10)
    print(f"\nPayment for {policy['policy_id']} sent. Policy will be created in {delay_seconds}s "
          "- run dbt now to see product_code = 'UNKNOWN'.")

    time.sleep(delay_seconds)
    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        conn.execute(
            "insert into policies (policy_id, customer_id, product_code, sum_insured, annual_premium)"
            " values (%(policy_id)s, %(customer_id)s, %(product_code)s, %(sum_insured)s, %(annual_premium)s)",
            policy,
        )
    print(f"Policy {policy['policy_id']} created - CDC will deliver it; the next dbt run resolves the payment.")


def bad_claim(rng: random.Random) -> None:
    policy = rng.choice(ActivePolicies().get())
    event = claim_filed(rng, policy, claim_amount=-1000, claim_id_prefix="CLM-BAD")
    producer = create_producer()
    publish(producer, event, " [INVALID AMOUNT]")
    producer.flush(10)
    print(f"\nSent invalid claim {event['payload']['claim_id']} with claim_amount=-1000")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="scenario", required=True)
    sub.add_parser("duplicate", help="send the same claim event twice")
    late_parser = sub.add_parser("late", help="send a payment before its policy exists")
    late_parser.add_argument("--delay", type=int, default=120, help="seconds before the policy is created")
    sub.add_parser("bad-claim", help="send a claim with a negative amount")
    args = parser.parse_args()

    rng = random.Random()
    if args.scenario == "duplicate":
        duplicate(rng)
    elif args.scenario == "late":
        late(rng, args.delay)
    else:
        bad_claim(rng)


if __name__ == "__main__":
    main()
