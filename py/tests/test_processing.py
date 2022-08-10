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


@pytest.mark.parametrize(
    "must_normalize", [None, False, True],
)
def test_normalize_user_agent(must_normalize):
    normalizer = sentry_relay.StoreNormalizer(
        project_id=1, normalize_user_agent=must_normalize
    )
    event = normalizer.normalize_event(
        {
            "request": {
                "headers": [
                    [
                        "User-Agent",
                        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0.1",
                    ],
                ],
            },
        }
    )

    if must_normalize:
        assert event["contexts"] == {
            "browser": {"name": "Firefox", "version": "15.0.1", "type": "browser"},
            "client_os": {"name": "Ubuntu", "type": "os"},
        }
    else:
        assert "contexts" not in event


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
        "description": "1.0rc1 (20200101100)",
        "package": "org.example.FooApp",
        "version_parsed": {
            "build_code": "20200101100",
            "components": 2,
            "major": 1,
            "minor": 0,
            "patch": 0,
            "pre": "rc1",
            "raw_quad": ["1", "0", None, None],
            "raw_short": "1.0rc1",
            "revision": 0,
        },
        "version_raw": "1.0rc1+20200101100",
    }


def test_parse_release_error():
    with pytest.raises(sentry_relay.InvalidReleaseErrorBadCharacters):
        sentry_relay.parse_release("/var/foo/foo")


def compare_versions():
    assert sentry_relay.compare_versions("1.0.0", "0.1.1") == 1
    assert sentry_relay.compare_versions("0.0.0", "0.1.1") == -1
    assert sentry_relay.compare_versions("1.0.0", "1.0") == -1


def test_validate_sampling_condition():
    """
    Test that a valid condition passes
    """
    # Should not throw
    condition = '{"op": "eq", "name": "field_2", "value": ["UPPER", "lower"]}'
    sentry_relay.validate_sampling_condition(condition)


def test_invalid_sampling_condition():
    """
    Tests that invalid conditions are caught
    """
    # Should throw
    condition = '{"op": "legacyBrowser", "value": [1,2,3]}'
    with pytest.raises(ValueError):

        sentry_relay.validate_sampling_condition(condition)


def test_validate_sampling_configuration():
    """
    Tests that a valid sampling rule configuration passes
    """
    config = """{
        "rules": [
            {
                "type": "trace",
                "sampleRate": 0.7,
                "condition": {
                    "op": "custom",
                    "name": "event.legacy_browser",
                    "value":["ie10"]
                },
                "id":1
            },
            {
                "type": "trace",
                "sampleRate": 0.9,
                "condition": {
                    "op": "eq",
                    "name": "event.release",
                    "value":["1.1.*"],
                    "options": {"ignoreCase": true}
                },
                "id":2
            }
        ]
    }"""
    # Should NOT throw
    sentry_relay.validate_sampling_configuration(config)
