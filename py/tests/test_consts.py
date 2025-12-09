from sentry_relay.consts import CategoryUnit, DataCategory, SPAN_STATUS_CODE_TO_NAME


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


def test_parse_category_unit():
    assert CategoryUnit.parse("count") == CategoryUnit.COUNT
    assert CategoryUnit.parse("bytes") == CategoryUnit.BYTES
    assert CategoryUnit.parse("milliseconds") == CategoryUnit.MILLISECONDS
    # Invalid/unknown names return None
    assert CategoryUnit.parse("") is None
    assert CategoryUnit.parse(None) is None
    assert CategoryUnit.parse("unknown") is None
    assert CategoryUnit.parse("something completely different") is None


def test_category_unit_api_name():
    assert CategoryUnit.COUNT.api_name() == "count"
    assert CategoryUnit.BYTES.api_name() == "bytes"
    assert CategoryUnit.MILLISECONDS.api_name() == "milliseconds"


def test_category_unit_from_category():
    # Count categories (most categories)
    assert CategoryUnit.from_category(DataCategory.ERROR) == CategoryUnit.COUNT
    assert CategoryUnit.from_category(DataCategory.TRANSACTION) == CategoryUnit.COUNT
    assert CategoryUnit.from_category(DataCategory.SPAN) == CategoryUnit.COUNT
    assert CategoryUnit.from_category(DataCategory.PROFILE_CHUNK) == CategoryUnit.COUNT

    # Bytes categories
    assert CategoryUnit.from_category(DataCategory.ATTACHMENT) == CategoryUnit.BYTES
    assert CategoryUnit.from_category(DataCategory.LOG_BYTE) == CategoryUnit.BYTES

    # Milliseconds categories
    assert (
        CategoryUnit.from_category(DataCategory.PROFILE_DURATION)
        == CategoryUnit.MILLISECONDS
    )
    assert (
        CategoryUnit.from_category(DataCategory.PROFILE_DURATION_UI)
        == CategoryUnit.MILLISECONDS
    )

    # Unknown category returns None (not an UNKNOWN enum member)
    assert CategoryUnit.from_category(DataCategory.UNKNOWN) is None


def test_category_unit_from_category_with_int():
    """Test that from_category works with integer values too."""
    assert CategoryUnit.from_category(1) == CategoryUnit.COUNT  # ERROR = 1
    assert CategoryUnit.from_category(4) == CategoryUnit.BYTES  # ATTACHMENT = 4
    # PROFILE_DURATION = 17
    assert CategoryUnit.from_category(17) == CategoryUnit.MILLISECONDS
    assert CategoryUnit.from_category(-1) is None  # UNKNOWN = -1


def test_category_unit_compatibility():
    """Test that CategoryUnit works as integer in comparisons."""
    assert 0 == CategoryUnit.COUNT
    assert 1 == CategoryUnit.BYTES
    assert 2 == CategoryUnit.MILLISECONDS
    assert CategoryUnit.BYTES in (0, 1, 2)


def test_category_unit_no_unknown_member():
    """Verify there is no UNKNOWN member in CategoryUnit."""
    assert not hasattr(CategoryUnit, "UNKNOWN")
    assert len(CategoryUnit) == 3  # Only COUNT, BYTES, MILLISECONDS


def test_category_unit_mapping():
    """
    Regression test to protect against accidental changes to the mapping.
    This ensures the unit for each category remains consistent.
    """
    # All count-based categories
    count_categories = [
        DataCategory.DEFAULT,
        DataCategory.ERROR,
        DataCategory.TRANSACTION,
        DataCategory.SECURITY,
        DataCategory.SESSION,
        DataCategory.PROFILE,
        DataCategory.REPLAY,
        DataCategory.TRANSACTION_PROCESSED,
        DataCategory.TRANSACTION_INDEXED,
        DataCategory.MONITOR,
        DataCategory.PROFILE_INDEXED,
        DataCategory.SPAN,
        DataCategory.MONITOR_SEAT,
        DataCategory.USER_REPORT_V2,
        DataCategory.METRIC_BUCKET,
        DataCategory.SPAN_INDEXED,
        DataCategory.PROFILE_CHUNK,
        DataCategory.METRIC_SECOND,
        DataCategory.DO_NOT_USE_REPLAY_VIDEO,
        DataCategory.UPTIME,
        DataCategory.ATTACHMENT_ITEM,
        DataCategory.LOG_ITEM,
        DataCategory.PROFILE_CHUNK_UI,
        DataCategory.SEER_AUTOFIX,
        DataCategory.SEER_SCANNER,
        DataCategory.PREVENT_USER,
        DataCategory.PREVENT_REVIEW,
        DataCategory.SIZE_ANALYSIS,
        DataCategory.INSTALLABLE_BUILD,
        DataCategory.TRACE_METRIC,
        DataCategory.SEER_USER,
    ]
    for category in count_categories:
        assert (
            CategoryUnit.from_category(category) == CategoryUnit.COUNT
        ), f"{category} should be COUNT"

    # Bytes-based categories
    bytes_categories = [DataCategory.ATTACHMENT, DataCategory.LOG_BYTE]
    for category in bytes_categories:
        assert (
            CategoryUnit.from_category(category) == CategoryUnit.BYTES
        ), f"{category} should be BYTES"

    # Milliseconds-based categories
    ms_categories = [DataCategory.PROFILE_DURATION, DataCategory.PROFILE_DURATION_UI]
    for category in ms_categories:
        assert (
            CategoryUnit.from_category(category) == CategoryUnit.MILLISECONDS
        ), f"{category} should be MILLISECONDS"

    # Unknown returns None
    assert CategoryUnit.from_category(DataCategory.UNKNOWN) is None
