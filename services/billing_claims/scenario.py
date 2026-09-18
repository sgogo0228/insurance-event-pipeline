"""Deterministic demo scenarios, run with: docker compose run --rm billing-claims python scenario.py <name>"""

import argparse
import random
import time
import uuid

import psycopg

from records import APP_DB_DSN, ActivePolicies, insert_claim, insert_payment, new_claim, new_payment


def late(rng: random.Random, delay_seconds: int) -> None:
    policy = {
        "policy_id": f"PLATE-{uuid.uuid4().hex[:6]}",
        "customer_id": "C00001",
        "product_code": "MEDICAL",
        "sum_insured": 500_000,
        "annual_premium": 15_000,
    }
    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        insert_payment(conn, new_payment(rng, policy))
        print(f"Payment for {policy['policy_id']} recorded. Policy will be created in {delay_seconds}s "
              "- run dbt now to see product_code = 'UNKNOWN'.")

        time.sleep(delay_seconds)
        conn.execute(
            "insert into policies (policy_id, customer_id, product_code, sum_insured, annual_premium)"
            " values (%(policy_id)s, %(customer_id)s, %(product_code)s, %(sum_insured)s, %(annual_premium)s)",
            policy,
        )
    print(f"Policy {policy['policy_id']} created - CDC will deliver it; the next dbt run resolves the payment.")


def bad_claim(rng: random.Random) -> None:
    claim = new_claim(rng, rng.choice(ActivePolicies().get()), claim_amount=-1000, claim_id_prefix="CLM-BAD")
    with psycopg.connect(APP_DB_DSN, autocommit=True) as conn:
        insert_claim(conn, claim)
    print(f"Recorded invalid claim {claim['claim_id']} with claim_amount=-1000")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="scenario", required=True)
    late_parser = sub.add_parser("late", help="record a payment before its policy exists")
    late_parser.add_argument("--delay", type=int, default=120, help="seconds before the policy is created")
    sub.add_parser("bad-claim", help="record a claim with a negative amount")
    args = parser.parse_args()

    rng = random.Random()
    if args.scenario == "late":
        late(rng, args.delay)
    else:
        bad_claim(rng)


if __name__ == "__main__":
    main()
