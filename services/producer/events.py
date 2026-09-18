import random
import uuid
from datetime import datetime, timezone

SCHEMA_VERSION = 1

CLAIM_CATEGORIES = {
    "TERM_LIFE": ["death"],
    "MEDICAL": ["hospitalization", "surgery", "outpatient"],
    "ACCIDENT": ["injury", "disability"],
}

CLAIM_DOCUMENTS = ["claim_form", "receipt", "diagnosis_certificate", "police_report", "id_copy"]

PAYMENT_METHODS = ["credit_card", "bank_transfer", "auto_debit"]


def make_event(event_type: str, payload: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_ts": datetime.now(timezone.utc).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
    }


def premium_paid(rng: random.Random, policy: dict) -> dict:
    return make_event(
        "premium_paid",
        {
            "payment_id": f"PAY-{uuid.uuid4().hex[:10]}",
            "policy_id": policy["policy_id"],
            "amount": round(policy["annual_premium"] / 12),
            "payment_method": rng.choice(PAYMENT_METHODS),
        },
    )


def claim_filed(rng: random.Random, policy: dict, claim_amount: int | None = None, claim_id_prefix: str = "CLM") -> dict:
    category = rng.choice(CLAIM_CATEGORIES[policy["product_code"]])
    if claim_amount is None:
        claim_amount = round(policy["sum_insured"] * rng.uniform(0.001, 0.02))
    return make_event(
        "claim_filed",
        {
            "claim_id": f"{claim_id_prefix}-{uuid.uuid4().hex[:10]}",
            "policy_id": policy["policy_id"],
            "claim_amount": claim_amount,
            # Semi-structured part (nested object + array), parsed later in dbt staging.
            "claim_detail": {
                "category": category,
                "description": f"Customer reported {category} case",
                "documents": rng.sample(CLAIM_DOCUMENTS, k=rng.randint(1, 3)),
            },
        },
    )
