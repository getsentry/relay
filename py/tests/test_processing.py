# coding: utf-8
import sentry_relay

import pytest

from sentry_relay._compat import PY2

REMARKS = [["myrule", "s", 7, 17]]
META = {"": {"rem": REMARKS}}
TEXT = "Hello, [redacted]!"
CHUNKS = [
    {"type": "text", "text": "Hello, "},
    {"type": "redaction", "text": "[redacted]", "rule_id": "myrule", "remark": "s"},
    {"type": "text", "text": "!"},
]
META_WITH_CHUNKS = {"": {"rem": REMARKS, "chunks": CHUNKS}}

PII_VARS = {
    "foo": "bar",
    "password": "hello",
    "the_secret": "hello",
    "a_password_here": "hello",
    "api_key": "secret_key",
    "apiKey": "secret_key",
}


def test_valid_platforms():
    assert len(sentry_relay.VALID_PLATFORMS) > 0
    assert "native" in sentry_relay.VALID_PLATFORMS


def test_split_chunks():
    chunks = sentry_relay.split_chunks(TEXT, REMARKS)
    assert chunks == CHUNKS


def test_meta_with_chunks():
    meta = sentry_relay.meta_with_chunks(TEXT, META)
    assert meta == META_WITH_CHUNKS


def test_meta_with_chunks_none():
    meta = sentry_relay.meta_with_chunks(TEXT, None)
    assert meta is None


def test_meta_with_chunks_empty():
    meta = sentry_relay.meta_with_chunks(TEXT, {})
    assert meta == {}


def test_meta_with_chunks_empty_remarks():
    meta = sentry_relay.meta_with_chunks(TEXT, {"rem": []})
    assert meta == {"rem": []}


def test_meta_with_chunks_dict():
    meta = sentry_relay.meta_with_chunks({"test": TEXT, "other": 1}, {"test": META})
    assert meta == {"test": META_WITH_CHUNKS}


def test_meta_with_chunks_list():
    meta = sentry_relay.meta_with_chunks(["other", TEXT], {"1": META})
    assert meta == {"1": META_WITH_CHUNKS}


def test_meta_with_chunks_missing_value():
    meta = sentry_relay.meta_with_chunks(None, META)
    assert meta == META


def test_meta_with_chunks_missing_non_string():
    meta = sentry_relay.meta_with_chunks(True, META)
    assert meta == META


def test_basic_store_normalization():
    normalizer = sentry_relay.StoreNormalizer(project_id=1)
    event = normalizer.normalize_event({"tags": []})
    assert event["project"] == 1
    assert event["type"] == "default"
    assert event["platform"] == "other"
    assert "tags" not in event
    assert "received" in event


def test_legacy_json():
    normalizer = sentry_relay.StoreNormalizer(project_id=1)
    event = normalizer.normalize_event(raw_event='{"extra":{"x":NaN}}')
    assert event["extra"] == {"x": 0.0}


def test_broken_json():
    normalizer = sentry_relay.StoreNormalizer(project_id=1)
    bad_str = u"Hello\ud83dWorld🇦🇹!"
    event = normalizer.normalize_event({"message": bad_str})
    assert "Hello" in event["logentry"]["formatted"]
    assert "World" in event["logentry"]["formatted"]
    if not PY2:
        assert event["logentry"]["formatted"] != bad_str


def test_validate_pii_config():
    sentry_relay.validate_pii_config("{}")
    sentry_relay.validate_pii_config('{"applications": {}}')

    with pytest.raises(ValueError):
        sentry_relay.validate_pii_config('{"applications": []}')

    with pytest.raises(ValueError):
        sentry_relay.validate_pii_config('{"applications": true}')


def test_convert_datascrubbing_config():
    cfg = sentry_relay.convert_datascrubbing_config(
        {
            "scrubData": True,
            "excludeFields": [],
            "scrubIpAddresses": True,
            "sensitiveFields": [],
            "scrubDefaults": True,
        }
    )

    assert cfg["applications"]

    assert (
        sentry_relay.convert_datascrubbing_config(
            {
                "scrubData": False,
                "excludeFields": [],
                "scrubIpAddresses": False,
                "sensitiveFields": [],
                "scrubDefaults": False,
            }
        )
        == {}
    )


def test_pii_strip_event():
    event = {"logentry": {"message": "hi"}}
    assert sentry_relay.pii_strip_event({}, event) == event


def test_pii_selector_suggestions_from_event():
    event = {"logentry": {"formatted": "hi"}}
    assert sentry_relay.pii_selector_suggestions_from_event(event) == [
        {"path": "$string", "value": "hi"},
        {"path": "$message", "value": "hi"},
    ]


def test_parse_release():
    parsed = sentry_relay.parse_release("org.example.FooApp@1.0rc1+20200101100")
    assert parsed == {
        "build_hash": None,
        "description": "1.0-rc1 (20200101100)",
        "package": "org.example.FooApp",
        "version_parsed": {
            "build_code": "20200101100",
            "components": 2,
            "major": 1,
            "minor": 0,
            "patch": 0,
            "pre": "rc1",
        },
        "version_raw": "1.0rc1+20200101100",
    }


def test_parse_release_error():
    with pytest.raises(sentry_relay.InvalidReleaseErrorBadCharacters):
        sentry_relay.parse_release("/var/foo/foo")


@pytest.mark.parametrize(
    "condition",
    [
        '{"op": "eq", "name": "field_1", "value": ["UPPER", "lower"], "options":{ "ignoreCase": true}}',
        '{"op": "eq", "name": "field_2", "value": ["UPPER", "lower"]}',
        '{"op": "glob", "name": "field_3", "value": ["1.2.*", "2.*"]}',
        '{"op": "not", "inner": {"op": "glob", "name": "field_4", "value": ["1.*"]}}',
        '{"op": "and", "inner": [{"op": "glob", "name": "field_5", "value": ["2.*"]}]}',
        '{"op": "or", "inner": [{"op": "glob", "name": "field_6", "value": ["3.*"]}]}',
        '{"op": "custom", "name": "event:legacy_browser", "value": ["ie9", "ie10", "ie11"]}',
    ],
    ids=("eq with case", "eq", "glob", "not", "and", "or", "custom")
)
def test_validate_dynamic_rule_condition(condition):
    """
    Validates various condition types
    """
    # Should not throw
    sentry_relay.validate_dynamic_rule_condition(condition)


@pytest.mark.parametrize(
    "condition",
    [
        '{"op2": "eq", "name": "field_1", "value": ["UPPER", "lower"], "ignoreCase": true}',
        '{"op": "eq", "value": ["UPPER", "lower"]}',
        '{"op": "legacyBrowser", "value": [1,2,3]}',
        '{"op": "csp", "value": ["v1, "v2"]}',
    ],
    ids=("bad operator","missing field", "bad value", "bad json")
)
def test_invalid_dynamic_rule_condition(condition):
    """
    Tests that invalid conditions are caught
    """
    # Should not throw
    with pytest.raises(ValueError):
        sentry_relay.validate_dynamic_rule_condition(condition)
