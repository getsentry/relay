# coding: utf-8
import sentry_relay

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


def test_data_scrubbing_missing_config():
    event = {"extra": PII_VARS}
    config = None

    scrubbed = sentry_relay.scrub_event(config, event)
    assert event == scrubbed


def test_data_scrubbing_empty_config():
    event = {"extra": PII_VARS}
    config = {}

    scrubbed = sentry_relay.scrub_event(config, event)
    assert event == scrubbed


def test_data_scrubbing_disabled_config():
    event = {"extra": PII_VARS}
    config = {
        "scrubData": False,
        "excludeFields": [],
        "scrubIpAddresses": False,
        "sensitiveFields": [],
        "scrubDefaults": True,
    }

    scrubbed = sentry_relay.scrub_event(config, event)
    assert event == scrubbed


def test_data_scrubbing_default_config():
    event = {"extra": PII_VARS}
    config = {
        "scrubData": True,
        "excludeFields": [],
        "scrubIpAddresses": True,
        "sensitiveFields": [],
        "scrubDefaults": True,
    }

    scrubbed = sentry_relay.scrub_event(config, event)
    assert scrubbed.pop("_meta", None)
    assert scrubbed == {
        "extra": {
            "foo": "bar",
            "password": "[Filtered]",
            "the_secret": "[Filtered]",
            "a_password_here": "[Filtered]",
            "api_key": "[Filtered]",
            "apiKey": "[Filtered]",
        }
    }
