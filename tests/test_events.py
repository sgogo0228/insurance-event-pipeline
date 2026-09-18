import random

from events import claim_filed, premium_paid

POLICY = {"policy_id": "P000001", "product_code": "MEDICAL", "sum_insured": 500_000, "annual_premium": 15_000}


def test_premium_paid_is_monthly_share_of_annual_premium():
    event = premium_paid(random.Random(1), POLICY)

    assert event["event_type"] == "premium_paid"
    assert event["payload"]["policy_id"] == "P000001"
    assert event["payload"]["amount"] == 1_250


def test_claim_detail_is_nested_and_matches_product():
    event = claim_filed(random.Random(1), POLICY)
    detail = event["payload"]["claim_detail"]

    assert detail["category"] in {"hospitalization", "surgery", "outpatient"}
    assert 1 <= len(detail["documents"]) <= 3
    assert 0 < event["payload"]["claim_amount"] <= POLICY["sum_insured"]


def test_claim_amount_can_be_overridden_for_scenarios():
    event = claim_filed(random.Random(1), POLICY, claim_amount=-1000, claim_id_prefix="CLM-BAD")

    assert event["payload"]["claim_amount"] == -1000
    assert event["payload"]["claim_id"].startswith("CLM-BAD-")


def test_every_event_gets_a_unique_id():
    rng = random.Random(1)

    assert len({premium_paid(rng, POLICY)["event_id"] for _ in range(100)}) == 100
