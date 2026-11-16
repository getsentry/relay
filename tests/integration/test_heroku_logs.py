import json
from datetime import datetime, timezone
from urllib.parse import urlsplit

import pytest
from requests.exceptions import HTTPError

from .asserts import matches, matches_any, only_items, time_within_delta
from .consts import Outcome

from sentry_relay.consts import DataCategory

LOGPLEX_HEADERS = {
    "Logplex-Msg-Count": "3",
    "Logplex-Frame-Id": "09C557EAFCFB6CF2740EE62F62971098",
    "Logplex-Drain-Token": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a",
    "User-Agent": "Logplex/v72",
}


def logplex_payload(*messages):
    """Frames syslog messages like Logplex, using octet counting."""
    payload = b""
    for message in messages:
        message = message.encode() + b"\n"
        payload += str(len(message)).encode() + b" " + message
    return payload


def heroku_messages():
    now = datetime.now(timezone.utc).isoformat()
    # Message formats from https://devcenter.heroku.com/articles/error-codes and
    # https://devcenter.heroku.com/articles/http-routing#heroku-router-log-format
    return [
        f"<190>1 {now} host app web.1 - Completed 200 OK in 12ms",
        f'<158>1 {now} host heroku router - at=error code=H12 desc="Request timeout" method=GET path="/forecast?x=1" host=example.herokuapp.com:443 request_id=8601b555-6a83-4c12-8269-97c8e32cdb22 fwd="198.51.100.8, 203.0.113.7" dyno=web.1 connect=1.25ms service=30001.5ms status=503 bytes=0 protocol=http1.1 tls=true tls_version=tls1.3',
        f"<45>1 {now} host heroku web.1 - Error R14 (Memory quota exceeded)",
    ]


def expected_item(body, attributes, trace_id=None):
    item = {
        "organizationId": "1",
        "projectId": "42",
        "traceId": matches_any(),
        "itemId": matches_any(),
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": time_within_delta(),
        "attributes": {
            "cloud.provider": {"stringValue": "heroku"},
            "sentry.origin": {"stringValue": "auto.log_drain.heroku"},
            "sentry.body": {"stringValue": body},
            "sentry.observed_timestamp_nanos": {
                "stringValue": time_within_delta(expect_resolution="ns")
            },
            "sentry.timestamp_precise": {
                "intValue": time_within_delta(expect_resolution="ns")
            },
            "sentry.payload_size_bytes": {
                "intValue": matches(lambda value: int(value) > 0)
            },
            "sentry._meta.fields.trace_id": {
                "stringValue": '{"meta":{"":{"rem":[["trace_id.missing","s"]]}}}'
            },
            "heroku.logplex.drain_token": {
                "stringValue": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            **attributes,
        },
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "retentionDays": 90,
        "received": time_within_delta(),
        "downsampledRetentionDays": 90,
        "outcomes": {
            "categoryCount": [
                {"dataCategory": DataCategory.LOG_ITEM, "quantity": "1"},
                {
                    "dataCategory": DataCategory.LOG_BYTE,
                    "quantity": matches(lambda value: int(value) > 0),
                },
            ],
            "keyId": "123",
        },
    }
    if trace_id is not None:
        item["traceId"] = trace_id
        del item["attributes"]["sentry._meta.fields.trace_id"]
    return item


def expected_items():
    # Built at test time, `time_within_delta` is relative to when it is created.
    return [
        expected_item(
            "Completed 200 OK in 12ms",
            {
                "sentry.severity_text": {"stringValue": "info"},
                "heroku.dyno.name": {"stringValue": "web.1"},
            },
        ),
        expected_item(
            'at=error code=H12 desc="Request timeout" method=GET path="/forecast?x=1" host=example.herokuapp.com:443 request_id=8601b555-6a83-4c12-8269-97c8e32cdb22 fwd="198.51.100.8, 203.0.113.7" dyno=web.1 connect=1.25ms service=30001.5ms status=503 bytes=0 protocol=http1.1 tls=true tls_version=tls1.3',
            {
                "sentry.severity_text": {"stringValue": "error"},
                "heroku.dyno.name": {"stringValue": "web.1"},
                "heroku.error.code": {"stringValue": "H12"},
                "heroku.error.description": {"stringValue": "Request timeout"},
                "http.request.method": {"stringValue": "GET"},
                "url.path": {"stringValue": "/forecast"},
                "url.query": {"stringValue": "x=1"},
                "server.address": {"stringValue": "example.herokuapp.com"},
                "server.port": {"intValue": "443"},
                "heroku.router.request_id": {
                    "stringValue": "8601b555-6a83-4c12-8269-97c8e32cdb22"
                },
                "client.address": {"stringValue": "203.0.113.7"},
                "heroku.router.connect": {"doubleValue": 1.25},
                "heroku.router.service": {"doubleValue": 30001.5},
                "http.response.status_code": {"intValue": "503"},
                "network.protocol.name": {"stringValue": "http"},
                "network.protocol.version": {"stringValue": "1.1"},
                "url.scheme": {"stringValue": "https"},
                "tls.protocol.name": {"stringValue": "tls"},
                "tls.protocol.version": {"stringValue": "1.3"},
            },
            trace_id="8601b5556a834c12826997c8e32cdb22",
        ),
        expected_item(
            "Error R14 (Memory quota exceeded)",
            {
                "sentry.severity_text": {"stringValue": "error"},
                "heroku.dyno.name": {"stringValue": "web.1"},
                "heroku.error.code": {"stringValue": "R14"},
                "heroku.error.description": {"stringValue": "Memory quota exceeded"},
            },
        ),
    ]


def heroku_project(mini_sentry, features=True):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    if features:
        project_config["config"]["features"].append(
            "organizations:relay-heroku-log-drain-endpoint"
        )
    return project_id


def test_heroku_logs(mini_sentry, relay, relay_with_processing, items_consumer):
    """Test Heroku Logplex log drain ingestion."""
    items_consumer = items_consumer()
    project_id = heroku_project(mini_sentry)

    relay = relay(relay_with_processing())

    response = relay.send_heroku_logs(
        project_id,
        data=logplex_payload(*heroku_messages()),
        headers=LOGPLEX_HEADERS,
    )
    assert response.status_code == 202
    assert response.content == b""

    items = items_consumer.get_items(n=3)

    def by_body(item):
        return item["attributes"]["sentry.body"]["stringValue"]

    assert sorted(items, key=by_body) == sorted(expected_items(), key=by_body)


def test_heroku_logs_requires_feature(mini_sentry, relay):
    """Without the feature flag, no logs are ingested."""
    project_id = heroku_project(mini_sentry, features=False)
    payload = logplex_payload(*heroku_messages())

    relay = relay(mini_sentry)
    relay.send_heroku_logs(
        project_id,
        data=payload,
        headers=LOGPLEX_HEADERS,
    )

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.LOG_ITEM,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "quantity": 1,
        },
        {
            "category": DataCategory.LOG_BYTE,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "quantity": len(payload),
        },
    ]
    assert mini_sentry.captured_envelopes.empty()


def test_heroku_logs_unsupported_content_type(mini_sentry, relay):
    project_id = heroku_project(mini_sentry)

    relay = relay(mini_sentry)
    with pytest.raises(HTTPError) as exc:
        relay.send_heroku_logs(
            project_id,
            data=logplex_payload(*heroku_messages()),
            headers={**LOGPLEX_HEADERS, "Content-Type": "application/json"},
        )

    assert exc.value.response.status_code == 415
    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize("key", ["unknown", "other_project"])
def test_heroku_logs_wrong_dsn_key(mini_sentry, relay, key):
    """Logs sent with a key that does not belong to the project are dropped."""
    project_id = heroku_project(mini_sentry)
    other_project = mini_sentry.add_full_project_config(43)
    dsn_key = {
        "unknown": "0" * 32,
        "other_project": other_project["publicKeys"][0]["publicKey"],
    }[key]

    relay = relay(mini_sentry)
    try:
        relay.send_heroku_logs(
            project_id,
            data=logplex_payload(*heroku_messages()),
            headers=LOGPLEX_HEADERS,
            dsn_key=dsn_key,
        )
    except HTTPError:
        pass

    # Send valid logs afterwards, only those may be ingested.
    relay.send_heroku_logs(
        project_id,
        data=logplex_payload(*heroku_messages()),
        headers=LOGPLEX_HEADERS,
    )

    envelope = mini_sentry.get_captured_envelope()
    dsn = urlsplit(envelope.headers["dsn"])
    assert dsn.path == f"/{project_id}"
    assert dsn.username == relay.get_dsn_public_key(project_id)
    assert envelope == only_items("log")
    bodies = [
        log["body"]
        for item in envelope.items
        for log in json.loads(item.payload.bytes)["items"]
    ]
    assert sorted(bodies) == sorted(
        item["attributes"]["sentry.body"]["stringValue"] for item in expected_items()
    )
    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b'{"message": "not logplex"}', id="json"),
        pytest.param(b"999999999 <190>1 - - - - truncated", id="truncated"),
        pytest.param(b"5 hello", id="not_syslog"),
    ],
)
def test_heroku_logs_invalid_payload(mini_sentry, relay, payload):
    """Malformed payloads produce no logs and an invalid outcome."""
    project_id = heroku_project(mini_sentry)

    relay = relay(mini_sentry)
    try:
        relay.send_heroku_logs(project_id, data=payload, headers=LOGPLEX_HEADERS)
    except HTTPError as e:
        # Empty bodies may be rejected by the endpoint directly.
        assert payload == b"", e

    if payload:
        assert mini_sentry.get_aggregated_outcomes(n=2) == [
            {
                "category": DataCategory.LOG_ITEM,
                "outcome": Outcome.INVALID,
                "reason": "invalid_log",
                "quantity": 1,
            },
            {
                "category": DataCategory.LOG_BYTE,
                "outcome": Outcome.INVALID,
                "reason": "invalid_log",
                "quantity": len(payload),
            },
        ]
    assert mini_sentry.captured_envelopes.empty()
