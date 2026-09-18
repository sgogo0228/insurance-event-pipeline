from datetime import datetime, timezone

from warehouse_loader import to_row


class FakeMessage:
    def __init__(self, value: bytes | None, key: bytes | None = b"P000001"):
        self._value = value
        self._key = key

    def topic(self):
        return "insurance_events"

    def partition(self):
        return 2

    def offset(self):
        return 42

    def key(self):
        return self._key

    def value(self):
        return self._value

    def timestamp(self):
        return 1, 1_700_000_000_000


def test_row_keeps_message_untouched_with_kafka_coordinates():
    row = to_row(FakeMessage(b'{"event_type": "premium_paid"}'))

    assert row == [
        "insurance_events",
        2,
        42,
        "P000001",
        '{"event_type": "premium_paid"}',
        datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc),
    ]


def test_malformed_payload_is_still_stored_for_dbt_to_filter():
    assert to_row(FakeMessage(b"not json"))[4] == "not json"


def test_message_without_key_is_allowed():
    assert to_row(FakeMessage(b"{}", key=None))[3] is None


def test_tombstone_is_skipped():
    assert to_row(FakeMessage(None)) is None
