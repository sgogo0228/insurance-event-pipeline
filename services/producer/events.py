SCHEMA_VERSION = 1


def make_event(event_type: str, row: dict, event_ts_column: str, payload: dict) -> dict:
    return {
        # Taken from the source row, so re-publishing the same row yields the same event_id.
        "event_id": str(row["event_id"]),
        "event_type": event_type,
        # Business time of the record, not the time it was published.
        "event_ts": row[event_ts_column].isoformat(),
        "schema_version": SCHEMA_VERSION,
        "payload": payload,
    }


def payment_event(row: dict) -> dict:
    return make_event(
        "premium_paid",
        row,
        "paid_at",
        {
            "payment_id": row["payment_id"],
            "policy_id": row["policy_id"],
            "amount": row["amount"],
            "payment_method": row["payment_method"],
        },
    )


def claim_event(row: dict) -> dict:
    return make_event(
        "claim_filed",
        row,
        "filed_at",
        {
            "claim_id": row["claim_id"],
            "policy_id": row["policy_id"],
            "claim_amount": row["claim_amount"],
            "claim_detail": row["claim_detail"],
        },
    )
