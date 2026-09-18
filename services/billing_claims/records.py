import os
import random
import time
import uuid

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

APP_DB_DSN = os.environ.get("APP_DB_DSN", "postgresql://app:app@localhost:5433/insurance_app")

CLAIM_CATEGORIES = {
    "TERM_LIFE": ["death"],
    "MEDICAL": ["hospitalization", "surgery", "outpatient"],
    "ACCIDENT": ["injury", "disability"],
}

CLAIM_DOCUMENTS = ["claim_form", "receipt", "diagnosis_certificate", "police_report", "id_copy"]

PAYMENT_METHODS = ["credit_card", "bank_transfer", "auto_debit"]

INSERT_PAYMENT = """
    insert into payments (payment_id, policy_id, amount, payment_method)
    values (%(payment_id)s, %(policy_id)s, %(amount)s, %(payment_method)s)
"""

INSERT_CLAIM = """
    insert into claims (claim_id, policy_id, claim_amount, claim_detail)
    values (%(claim_id)s, %(policy_id)s, %(claim_amount)s, %(claim_detail)s)
"""


def new_payment(rng: random.Random, policy: dict) -> dict:
    return {
        "payment_id": f"PAY-{uuid.uuid4().hex[:10]}",
        "policy_id": policy["policy_id"],
        "amount": round(policy["annual_premium"] / 12),
        "payment_method": rng.choice(PAYMENT_METHODS),
    }


def new_claim(rng: random.Random, policy: dict, claim_amount: int | None = None, claim_id_prefix: str = "CLM") -> dict:
    category = rng.choice(CLAIM_CATEGORIES[policy["product_code"]])
    if claim_amount is None:
        claim_amount = round(policy["sum_insured"] * rng.uniform(0.001, 0.02))
    return {
        "claim_id": f"{claim_id_prefix}-{uuid.uuid4().hex[:10]}",
        "policy_id": policy["policy_id"],
        "claim_amount": claim_amount,
        # Semi-structured part (nested object + array), parsed later in dbt staging.
        "claim_detail": {
            "category": category,
            "description": f"Customer reported {category} case",
            "documents": rng.sample(CLAIM_DOCUMENTS, k=rng.randint(1, 3)),
        },
    }


def insert_payment(conn: psycopg.Connection, payment: dict) -> None:
    conn.execute(INSERT_PAYMENT, payment)


def insert_claim(conn: psycopg.Connection, claim: dict) -> None:
    conn.execute(INSERT_CLAIM, {**claim, "claim_detail": Jsonb(claim["claim_detail"])})


class ActivePolicies:
    """Active policies from the policy admin DB, cached to avoid querying on every record."""

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
