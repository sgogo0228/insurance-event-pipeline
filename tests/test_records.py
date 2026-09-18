import random

from records import new_claim, new_payment

POLICY = {"policy_id": "P000001", "product_code": "MEDICAL", "sum_insured": 500_000, "annual_premium": 15_000}


def test_payment_is_monthly_share_of_annual_premium():
    payment = new_payment(random.Random(1), POLICY)

    assert payment["policy_id"] == "P000001"
    assert payment["amount"] == 1_250


def test_claim_detail_is_nested_and_matches_product():
    claim = new_claim(random.Random(1), POLICY)

    assert claim["claim_detail"]["category"] in {"hospitalization", "surgery", "outpatient"}
    assert 1 <= len(claim["claim_detail"]["documents"]) <= 3
    assert 0 < claim["claim_amount"] <= POLICY["sum_insured"]


def test_claim_amount_can_be_overridden_for_scenarios():
    claim = new_claim(random.Random(1), POLICY, claim_amount=-1000, claim_id_prefix="CLM-BAD")

    assert claim["claim_amount"] == -1000
    assert claim["claim_id"].startswith("CLM-BAD-")
