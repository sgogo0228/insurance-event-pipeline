import uuid
from datetime import datetime, timezone

from events import claim_event, payment_event

PAID_AT = datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc)
EVENT_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")


def payment_row() -> dict:
    return {
        "id": 7,
        "event_id": EVENT_ID,
        "payment_id": "PAY-1",
        "policy_id": "P000001",
        "amount": 1_250,
        "payment_method": "credit_card",
        "paid_at": PAID_AT,
    }


def test_payment_event_uses_row_identity_and_business_time():
    event = payment_event(payment_row())

    assert event["event_id"] == str(EVENT_ID)
    assert event["event_type"] == "premium_paid"
    assert event["event_ts"] == "2026-09-01T08:30:00+00:00"
    assert event["payload"] == {"payment_id": "PAY-1", "policy_id": "P000001", "amount": 1_250, "payment_method": "credit_card"}


def test_republishing_the_same_row_gives_the_same_event():
    assert payment_event(payment_row()) == payment_event(payment_row())


def test_claim_event_keeps_nested_detail():
    detail = {"category": "surgery", "description": "Customer reported surgery case", "documents": ["receipt"]}
    row = {
        "id": 3,
        "event_id": EVENT_ID,
        "claim_id": "CLM-1",
        "policy_id": "P000001",
        "claim_amount": 5_000,
        "claim_detail": detail,
        "filed_at": PAID_AT,
    }

    event = claim_event(row)

    assert event["event_type"] == "claim_filed"
    assert event["payload"]["claim_detail"] == detail
