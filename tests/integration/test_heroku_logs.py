from datetime import datetime, timezone

import pytest
from requests.exceptions import HTTPError

from .asserts import matches_any, time_within_delta
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
        f'<158>1 {now} host heroku router - at=error code=H12 desc="Request timeout" method=GET path="/forecast" host=example.herokuapp.com request_id=8601b555-6a83-4c12-8269-97c8e32cdb22 fwd="203.0.113.7" dyno=web.1 connect=1ms service=30001ms status=503 bytes=0 protocol=http1.1',
        f"<45>1 {now} host heroku web.1 - Error R14 (Memory quota exceeded)",
    ]


def expected_item(body, attributes):
    return {
        "organizationId": "1",
        "projectId": "42",
        "traceId": matches_any(),
        "itemId": matches_any(),
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": matches_any(),
        "attributes": {
            "sentry.origin": {"stringValue": "auto.log_drain.heroku"},
            "sentry.body": {"stringValue": body},
            "sentry.relay.ingress": {"stringValue": "integration"},
            "sentry.observed_timestamp_nanos": {"stringValue": matches_any()},
            "sentry.timestamp_precise": {
                "intValue": time_within_delta(expect_resolution="ns")
            },
            "sentry.payload_size_bytes": {"intValue": matches_any()},
            "sentry._meta.fields.trace_id": {
                "stringValue": '{"meta":{"":{"rem":[["trace_id.missing","s"]]}}}'
            },
            "heroku.logplex.frame_id": {
                "stringValue": "09C557EAFCFB6CF2740EE62F62971098"
            },
            "heroku.logplex.drain_token": {
                "stringValue": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            "heroku.logplex.user_agent": {"stringValue": "Logplex/v72"},
            **attributes,
        },
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "retentionDays": 90,
        "received": matches_any(),
        "downsampledRetentionDays": 90,
        "outcomes": {
            "categoryCount": [
                {"dataCategory": DataCategory.LOG_ITEM, "quantity": "1"},
                {"dataCategory": DataCategory.LOG_BYTE, "quantity": matches_any()},
            ],
            "keyId": "123",
        },
    }


def expected_items():
    # Built at test time, `time_within_delta` is relative to when it is created.
    return [
        expected_item(
            "Completed 200 OK in 12ms",
            {
                "sentry.severity_text": {"stringValue": "info"},
                "heroku.source": {"stringValue": "app"},
                "heroku.dyno": {"stringValue": "web.1"},
            },
        ),
        expected_item(
            'at=error code=H12 desc="Request timeout" method=GET path="/forecast" host=example.herokuapp.com request_id=8601b555-6a83-4c12-8269-97c8e32cdb22 fwd="203.0.113.7" dyno=web.1 connect=1ms service=30001ms status=503 bytes=0 protocol=http1.1',
            {
                "sentry.severity_text": {"stringValue": "error"},
                "heroku.source": {"stringValue": "heroku"},
                "heroku.dyno": {"stringValue": "router"},
                "heroku.error.code": {"stringValue": "H12"},
                "heroku.error.description": {"stringValue": "Request timeout"},
                "heroku.router.at": {"stringValue": "error"},
                "heroku.router.method": {"stringValue": "GET"},
                "heroku.router.path": {"stringValue": "/forecast"},
                "heroku.router.host": {"stringValue": "example.herokuapp.com"},
                "heroku.router.request_id": {
                    "stringValue": "8601b555-6a83-4c12-8269-97c8e32cdb22"
                },
                "heroku.router.fwd": {"stringValue": "203.0.113.7"},
                "heroku.router.dyno": {"stringValue": "web.1"},
                "heroku.router.connect": {"intValue": "1"},
                "heroku.router.service": {"intValue": "30001"},
                "heroku.router.status": {"intValue": "503"},
                "heroku.router.bytes": {"intValue": "0"},
                "heroku.router.protocol": {"stringValue": "http1.1"},
            },
        ),
        expected_item(
            "Error R14 (Memory quota exceeded)",
            {
                "sentry.severity_text": {"stringValue": "error"},
                "heroku.source": {"stringValue": "heroku"},
                "heroku.dyno": {"stringValue": "web.1"},
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


def test_heroku_logs_requires_feature(
    mini_sentry, relay, relay_with_processing, items_consumer
):
    """Without the feature flag, no logs are ingested."""
    items_consumer = items_consumer()
    project_id = heroku_project(mini_sentry, features=False)

    relay = relay(relay_with_processing())
    relay.send_heroku_logs(
        project_id,
        data=logplex_payload(*heroku_messages()),
        headers=LOGPLEX_HEADERS,
    )

    items_consumer.assert_empty()


def test_heroku_logs_unsupported_content_type(
    mini_sentry, relay, relay_with_processing, items_consumer
):
    items_consumer = items_consumer()
    project_id = heroku_project(mini_sentry)

    relay = relay(relay_with_processing())
    with pytest.raises(HTTPError) as exc:
        relay.send_heroku_logs(
            project_id,
            data=logplex_payload(*heroku_messages()),
            headers={**LOGPLEX_HEADERS, "Content-Type": "application/json"},
        )

    assert exc.value.response.status_code == 415
    items_consumer.assert_empty()


@pytest.mark.parametrize("key", ["unknown", "other_project"])
def test_heroku_logs_wrong_dsn_key(
    mini_sentry, relay, relay_with_processing, items_consumer, key
):
    """Logs sent with a key that does not belong to the project are dropped."""
    items_consumer = items_consumer()
    project_id = heroku_project(mini_sentry)
    other_project = mini_sentry.add_full_project_config(43)
    dsn_key = {
        "unknown": "0" * 32,
        "other_project": other_project["publicKeys"][0]["publicKey"],
    }[key]

    relay = relay(relay_with_processing())
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

    items = items_consumer.get_items(n=3)
    assert all(item["projectId"] == "42" for item in items)
    assert all(item["outcomes"]["keyId"] == "123" for item in items)
    items_consumer.assert_empty()


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b'{"message": "not logplex"}', id="json"),
        pytest.param(b"999999999 <190>1 - - - - truncated", id="truncated"),
        pytest.param(b"5 hello", id="not_syslog"),
    ],
)
def test_heroku_logs_invalid_payload(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    items_consumer,
    payload,
):
    """Malformed payloads produce no logs and an invalid outcome."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = heroku_project(mini_sentry)

    config = {"http": {"global_metrics": True}}
    relay = relay(relay_with_processing(options=config), options=config)
    try:
        relay.send_heroku_logs(project_id, data=payload, headers=LOGPLEX_HEADERS)
    except HTTPError as e:
        # Empty bodies may be rejected by the endpoint directly.
        assert payload == b"", e

    items_consumer.assert_empty()
    if payload:
        outcomes = outcomes_consumer.get_outcomes(n=2)
        assert {o["outcome"] for o in outcomes} == {Outcome.INVALID}
        assert {o["reason"] for o in outcomes} == {"invalid_log"}
        assert {o["category"] for o in outcomes} == {
            DataCategory.LOG_ITEM,
            DataCategory.LOG_BYTE,
        }
