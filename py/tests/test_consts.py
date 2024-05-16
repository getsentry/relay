from sentry_relay.consts import (
    DataCategory,
    SPAN_STATUS_CODE_TO_NAME,
    SPAN_STATUS_NAME_TO_CODE,
)


def test_parse_data_category():
    assert DataCategory.parse("default") == DataCategory.DEFAULT
    assert DataCategory.parse("transaction") == DataCategory.TRANSACTION
    assert DataCategory.parse("") is None
    assert DataCategory.parse(None) is None
    assert DataCategory.parse("something completely different") is None


def test_data_category_from_event_type():
    assert DataCategory.from_event_type("transaction") == DataCategory.TRANSACTION
    # Special case!
    assert DataCategory.from_event_type("default") == DataCategory.ERROR
    # Anything unknown is coerced to "default", which is ERROR
    assert DataCategory.from_event_type("") == DataCategory.ERROR
    assert DataCategory.from_event_type(None) == DataCategory.ERROR


def test_data_category_api_name():
    assert DataCategory.ERROR.api_name() == "error"


def test_data_category_compatibility():
    assert 1 == DataCategory.ERROR
    assert 1 in DataCategory.event_categories()
    assert DataCategory.ERROR in (0, 1, 2)


def test_span_mapping():
    # This is a pure regression test to protect against accidental renames.
    assert SPAN_STATUS_CODE_TO_NAME == {
        0: "ok",
        1: "cancelled",
        2: "unknown",
        3: "invalid_argument",
        4: "deadline_exceeded",
        5: "not_found",
        6: "already_exists",
        7: "permission_denied",
        8: "resource_exhausted",
        9: "failed_precondition",
        10: "aborted",
        11: "out_of_range",
        12: "unimplemented",
        13: "internal_error",
        14: "unavailable",
        15: "data_loss",
        16: "unauthenticated",
    }

    assert SPAN_STATUS_NAME_TO_CODE == {
        "aborted": 10,
        "already_exists": 6,
        "cancelled": 1,
        "data_loss": 15,
        "deadline_exceeded": 4,
        "failed_precondition": 9,
        "internal_error": 13,
        "invalid_argument": 3,
        "not_found": 5,
        "ok": 0,
        "out_of_range": 11,
        "permission_denied": 7,
        "resource_exhausted": 8,
        "unauthenticated": 16,
        "unavailable": 14,
        "unimplemented": 12,
        "unknown": 2,
    }
