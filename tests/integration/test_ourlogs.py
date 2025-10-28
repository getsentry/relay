import json

from datetime import datetime, timezone, timedelta
from unittest import mock

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within, matches

import pytest


TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
        "batch_size": 1,
        "batch_interval": 1,
        "aggregator": {
            "bucket_interval": 1,
            "flush_interval": 1,
        },
    },
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    },
}


def envelope_with_sentry_logs(*payloads: dict) -> Envelope:
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="log",
            payload=PayloadRef(json={"items": payloads}),
            content_type="application/vnd.sentry.items.log+json",
            headers={"item_count": len(payloads)},
        )
    )
    return envelope


def timestamps(ts: datetime):
    return {
        "sentry.observed_timestamp_nanos": {
            "stringValue": time_within(ts, expect_resolution="ns")
        },
        "sentry._internal.observed_timestamp_nanos": {
            "stringValue": time_within(ts, expect_resolution="ns")
        },
        "sentry.timestamp_nanos": {
            "stringValue": time_within_delta(
                ts, delta=timedelta(seconds=0), expect_resolution="ns", precision="us"
            )
        },
        "sentry.timestamp_precise": {
            "intValue": time_within_delta(
                ts, delta=timedelta(seconds=0), expect_resolution="ns", precision="us"
            )
        },
    }


def test_ourlog_multiple_containers_not_allowed(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    outcomes_consumer,
):
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    relay = relay(relay_with_processing(options=TEST_CONFIG), options=TEST_CONFIG)
    start = datetime.now(timezone.utc)
    envelope = Envelope()

    for _ in range(2):
        payload = {
            "timestamp": start.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "oops, not again",
        }
        envelope.add_item(
            Item(
                type="log",
                payload=PayloadRef(json={"items": [payload]}),
                content_type="application/vnd.sentry.items.log+json",
                headers={"item_count": 1},
            )
        )

    relay.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 2,
            "reason": "duplicate_item",
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": matches(lambda x: 300 < x < 400),
            "reason": "duplicate_item",
        },
    ]


@pytest.mark.parametrize(
    "external_mode,expected_byte_size",
    [
        # 296 here is a billing relevant metric, do not arbitrarily change it,
        # this value is supposed to be static and purely based on data received,
        # independent of any normalization.
        (None, 296),
        # Same applies as above, a proxy Relay does not need to run normalization.
        ("proxy", 296),
        # If an external Relay/Client makes modifications, sizes can change,
        # this is fuzzy due to slight changes in sizes due to added timestamps
        # and may need to be adjusted when changing normalization.
        ("managed", 641),
    ],
)
def test_ourlog_extraction_with_sentry_logs(
    mini_sentry,
    relay,
    relay_with_processing,
    relay_credentials,
    items_consumer,
    outcomes_consumer,
    external_mode,
    expected_byte_size,
):
    relay_fn = relay

    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    credentials = relay_credentials()
    relay = relay_fn(
        relay_with_processing(options=TEST_CONFIG, static_credentials=credentials),
        credentials=credentials,
        options=TEST_CONFIG,
    )
    if external_mode is not None:
        relay = relay_fn(
            relay, options={"relay": {"mode": external_mode}, **TEST_CONFIG}
        )

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Example log record",
            "attributes": {
                "boolean.attribute": {"value": True, "type": "boolean"},
                "integer.attribute": {"value": 42, "type": "integer"},
                "double.attribute": {"value": 1.23, "type": "double"},
                "string.attribute": {"value": "some string", "type": "string"},
                "pii": {"value": "4242 4242 4242 4242", "type": "string"},
                "sentry.severity_text": {"value": "info", "type": "string"},
                "http.response_content_length": {"value": 17, "type": "integer"},
                "unknown_type": {"value": "info", "type": "unknown"},
                "broken_type": {"value": "info", "type": "not_a_real_type"},
                "mismatched_type": {"value": "some string", "type": "boolean"},
                "valid_string_with_other": {
                    "value": "test",
                    "type": "string",
                    "some_other_field": "some_other_value",
                },
            },
        },
    )

    relay.send_envelope(project_id, envelope)

    assert items_consumer.get_items(n=2) == [
        {
            "attributes": {
                "sentry.body": {"stringValue": "This is really bad"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.severity_text": {"stringValue": "error"},
                "sentry.payload_size_bytes": {"intValue": mock.ANY},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
                **timestamps(ts),
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                ts, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
        {
            "attributes": {
                "sentry._meta.fields.attributes.broken_type": {
                    "stringValue": '{"meta":{"":{"err":["invalid_data"],"val":{"type":"not_a_real_type","value":"info"}}}}'
                },
                "sentry._meta.fields.attributes.mismatched_type": {
                    "stringValue": '{"meta":{"":{"err":["invalid_data"],"val":{"type":"boolean","value":"some '
                    'string"}}}}'
                },
                "sentry._meta.fields.attributes.unknown_type": {
                    "stringValue": '{"meta":{"":{"err":["invalid_data"],"val":{"type":"unknown","value":"info"}}}}'
                },
                "boolean.attribute": {"boolValue": True},
                "double.attribute": {"doubleValue": 1.23},
                "integer.attribute": {"intValue": "42"},
                "pii": {"stringValue": "[creditcard]"},
                "sentry._meta.fields.attributes.pii": {
                    "stringValue": '{"meta":{"value":{"":{"rem":[["@creditcard","s",0,12]],"len":19}}}}'
                },
                "sentry.body": {"stringValue": "Example log record"},
                "sentry.browser.name": {"stringValue": "Python Requests"},
                "sentry.browser.version": {"stringValue": "2.32"},
                "sentry.severity_text": {"stringValue": "info"},
                "sentry.payload_size_bytes": {"intValue": mock.ANY},
                "http.response_content_length": {"intValue": "17"},
                "http.response.body.size": {"intValue": "17"},
                "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
                "string.attribute": {"stringValue": "some string"},
                "valid_string_with_other": {"stringValue": "test"},
                **timestamps(ts),
            },
            "clientSampleRate": 1.0,
            "itemId": mock.ANY,
            "itemType": "TRACE_ITEM_TYPE_LOG",
            "organizationId": "1",
            "projectId": "42",
            "received": time_within_delta(),
            "retentionDays": 30,
            "downsampledRetentionDays": 390,
            "serverSampleRate": 1.0,
            "timestamp": time_within_delta(
                ts, delta=timedelta(seconds=1), expect_resolution="ns"
            ),
            "traceId": "5b8efff798038103d269b633813fc60c",
        },
    ]

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=2)
    assert outcomes == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": expected_byte_size,
        },
    ]


def test_ourlog_extraction_with_string_pii_scrubbing(
    mini_sentry,
    relay,
    scrubbing_rule,
):
    rule_type, test_value, expected_scrubbed = scrubbing_rule
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    project_config["config"]["piiConfig"]["applications"] = {"$string": [rule_type]}

    relay_instance = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log",
            "attributes": {
                "test_pii": {"value": test_value, "type": "string"},
            },
        }
    )

    relay_instance.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=5)
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    item = item_payload["items"][0]

    assert item == {
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "span_id": "eee19b7ec3c1b174",
        "attributes": {
            "test_pii": {"type": "string", "value": expected_scrubbed},
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry._internal.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
        },
        "__header": {"byte_size": mock.ANY},
        "_meta": {
            "attributes": {
                "test_pii": {
                    "value": {
                        "": {
                            "len": mock.ANY,
                            "rem": [[rule_type, mock.ANY, mock.ANY, mock.ANY]],
                        }
                    }
                }
            },
        },
        "body": "Test log",
        "level": "info",
        "timestamp": time_within(ts),
    }


def test_ourlog_extraction_default_pii_scrubbing_attributes(
    mini_sentry,
    relay,
    secret_attribute,
):
    attribute_key, attribute_value, expected_value, rule_type = secret_attribute
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    project_config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
            "scrubIpAddresses": True,
        },
    )

    relay_instance = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log",
            "attributes": {
                attribute_key: {"value": attribute_value, "type": "string"},
            },
        }
    )

    relay_instance.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=5)
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    item = item_payload["items"][0]
    attributes = item["attributes"]

    assert attribute_key in attributes
    assert attributes[attribute_key]["value"] == expected_value
    assert "_meta" in item
    meta = item["_meta"]["attributes"][attribute_key]["value"][""]
    assert "rem" in meta
    rem_info = meta["rem"]
    assert len(rem_info) == 1
    assert rem_info[0][0] == rule_type


def test_ourlog_default_pii_body(
    mini_sentry,
    relay,
    non_destructive,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    non_destructive.install(project_config)

    relay_instance = relay(mini_sentry, options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": non_destructive.input_message,
        }
    )

    relay_instance.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=5)
    item_payload = json.loads(envelope.items[0].payload.bytes.decode())
    log = item_payload["items"][0]

    assert log == {
        **(
            {"_meta": {"body": {"": {"len": mock.ANY, "rem": mock.ANY}}}}
            if non_destructive.scrubs()
            else {}
        ),
        "attributes": mock.ANY,
        "body": non_destructive.expected_output,
        "level": "info",
        "span_id": "eee19b7ec3c1b174",
        "timestamp": time_within(ts),
        "trace_id": "5b8efff798038103d269b633813fc60c",
        "__header": mock.ANY,
    }

    if non_destructive.additional_checks:
        assert non_destructive.additional_checks(log["body"])


def test_ourlog_extraction_default_pii_scrubbing_does_not_scrub_default_attributes(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
            "scrubIpAddresses": True,
        },
    )

    # Testing the 'anything' filter as it's the most egregious with deep wildcards
    project_config["config"]["piiConfig"] = {
        "rules": {
            "remove_custom_field": {
                "type": "anything",
                "redaction": {"method": "replace", "text": "[REDACTED]"},
            }
        },
        "applications": {"**": ["remove_custom_field"]},
    }

    relay = relay_with_processing(options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log",
            "attributes": {
                "custom_field": {"value": "custom_value", "type": "string"},
            },
        }
    )

    relay.send_envelope(project_id, envelope)

    item = items_consumer.get_item()
    assert item == {
        "attributes": {
            "sentry._meta.fields.attributes.custom_field": {
                "stringValue": '{"meta":{"value":{"":{"rem":[["remove_custom_field","s",0,10]],"len":12}}}}'
            },
            "sentry.browser.version": {"stringValue": "2.32"},
            "custom_field": {"stringValue": "[REDACTED]"},
            "sentry.body": {"stringValue": "Test log"},
            "sentry.severity_text": {"stringValue": "info"},
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b174"},
            "sentry.payload_size_bytes": mock.ANY,
            "sentry.browser.name": {"stringValue": "Python Requests"},
            **timestamps(ts),
        },
        "clientSampleRate": 1.0,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "organizationId": "1",
        "projectId": "42",
        "received": time_within_delta(),
        "retentionDays": 30,
        "downsampledRetentionDays": 390,
        "serverSampleRate": 1.0,
        "timestamp": time_within_delta(
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


def test_ourlog_extraction_with_sentry_logs_with_missing_fields(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
            "scrubIpAddresses": True,
        },
    )
    relay = relay_with_processing(options=TEST_CONFIG)
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "Example log record 2",
        }
    )

    relay.send_envelope(project_id, envelope)

    assert items_consumer.get_item() == {
        "attributes": {
            "sentry.body": {"stringValue": "Example log record 2"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "sentry.severity_text": {"stringValue": "warn"},
            "sentry.payload_size_bytes": {"intValue": mock.ANY},
            **timestamps(ts),
        },
        "clientSampleRate": 1.0,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "organizationId": "1",
        "projectId": "42",
        "received": time_within_delta(),
        "retentionDays": 30,
        "downsampledRetentionDays": 390,
        "serverSampleRate": 1.0,
        "timestamp": time_within_delta(
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


def test_ourlog_extraction_is_disabled_without_feature(
    mini_sentry,
    relay_with_processing,
    items_consumer,
):
    items_consumer = items_consumer()
    relay = relay_with_processing(options=TEST_CONFIG)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }

    project_config["config"]["features"] = []

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": datetime.now(timezone.utc).timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "level": "warn",
            "body": "Example log",
        }
    )
    relay.send_envelope(project_id, envelope)

    items_consumer.assert_empty()


@pytest.mark.parametrize(
    "user_agent,expected_browser_name,expected_browser_version",
    [
        # Chrome desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Chrome",
            "131.0.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Chrome",
            "120.0.0",
        ),
        # Firefox desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0",
            "Firefox",
            "121.0",
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
            "Firefox",
            "120.0",
        ),
        # Safari desktop
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Safari",
            "17.1",
        ),
        # Edge desktop
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Edge",
            "120.0.0",
        ),
        # Chrome mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/119.0.6045.169 Mobile/15E148 Safari/604.1",
            "Chrome Mobile iOS",
            "119.0.6045",
        ),
        (
            "Mozilla/5.0 (Linux; Android 10; SM-G973F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",
            "Chrome Mobile",
            "119.0.0",
        ),
        # Safari mobile
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1",
            "Mobile Safari",
            "17.1",
        ),
    ],
)
def test_browser_name_version_extraction(
    mini_sentry,
    relay,
    relay_with_processing,
    items_consumer,
    user_agent,
    expected_browser_name,
    expected_browser_version,
):
    items_consumer = items_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }
    relay = relay(relay_with_processing(options=TEST_CONFIG))
    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "This is really bad",
        },
    )

    relay.send_envelope(project_id, envelope, headers={"User-Agent": user_agent})

    assert items_consumer.get_item() == {
        "attributes": {
            "sentry.body": {"stringValue": "This is really bad"},
            "sentry.browser.name": {"stringValue": expected_browser_name},
            "sentry.browser.version": {"stringValue": expected_browser_version},
            "sentry.severity_text": {"stringValue": "error"},
            "sentry.payload_size_bytes": {"intValue": mock.ANY},
            "sentry.span_id": {"stringValue": "eee19b7ec3c1b175"},
            **timestamps(ts),
        },
        "clientSampleRate": 1.0,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "organizationId": "1",
        "projectId": "42",
        "received": time_within_delta(),
        "retentionDays": 30,
        "downsampledRetentionDays": 390,
        "serverSampleRate": 1.0,
        "timestamp": time_within_delta(
            ts, delta=timedelta(seconds=1), expect_resolution="ns"
        ),
        "traceId": "5b8efff798038103d269b633813fc60c",
    }


@pytest.mark.parametrize(
    "filter_name,filter_config,args",
    [
        pytest.param(
            "release-version",
            {"releases": {"releases": ["foobar@1.0"]}},
            {},
            id="release",
        ),
        pytest.param(
            "legacy-browsers",
            {"legacyBrowsers": {"isEnabled": True, "options": ["ie9"]}},
            {
                "user-agent": "Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)"
            },
            id="legacy-browsers",
        ),
        pytest.param(
            "web-crawlers",
            {"webCrawlers": {"isEnabled": True}},
            {
                "user-agent": "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; PerplexityBot/1.0; +https://perplexity.ai/perplexitybot)"
            },
            id="web-crawlers",
        ),
        pytest.param(
            "gen_body",
            {
                "op": "glob",
                "name": "log.body",
                "value": ["fo*"],
            },
            {},
            id="gen_body",
        ),
        pytest.param(
            "gen_attr",
            {
                "op": "gte",
                "name": "log.attributes.some_integer.value",
                "value": 123,
            },
            {},
            id="gen_attr",
        ),
    ],
)
def test_filters_are_applied_to_logs(
    mini_sentry,
    relay,
    filter_name,
    filter_config,
    args,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["retentions"] = {
        "log": {"standard": 30, "downsampled": 13 * 30},
    }
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]

    if filter_name.startswith("gen_"):
        filter_config = {
            "generic": {
                "version": 1,
                "filters": [
                    {
                        "id": filter_name,
                        "isEnabled": True,
                        "condition": filter_config,
                    }
                ],
            }
        }

    project_config["config"]["filterSettings"] = filter_config

    relay = relay(mini_sentry, options=TEST_CONFIG)

    ts = datetime.now(timezone.utc)

    envelope = envelope_with_sentry_logs(
        {
            "timestamp": ts.timestamp(),
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b175",
            "level": "error",
            "body": "foo",
            "attributes": {
                "some_integer": {"value": 123, "type": "integer"},
                "sentry.release": {"value": "foobar@1.0", "type": "string"},
            },
        },
    )

    headers = None
    if user_agent := args.get("user-agent"):
        headers = {"User-Agent": user_agent}

    relay.send_envelope(project_id, envelope, headers=headers)

    assert mini_sentry.get_outcomes(2) == [
        {
            "category": DataCategory.LOG_ITEM.value,
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,  # Filtered
            "reason": filter_name,
            "quantity": 1,
            "timestamp": time_within_delta(ts),
        },
        {
            "category": DataCategory.LOG_BYTE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": mock.ANY,
            "reason": filter_name,
            "timestamp": time_within_delta(ts),
        },
    ]

    assert mini_sentry.captured_events.empty()
