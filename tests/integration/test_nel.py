import json
from datetime import datetime, timedelta, timezone
from unittest import mock

from .asserts import time_within_delta


def test_nel_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)

    relay.send_nel_event(project_id)

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the NEL report
    expected_ts = datetime.now(tz=timezone.utc) - timedelta(milliseconds=1200000)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "items": [
            {
                "__header": mock.ANY,
                "attributes": {
                    "sentry.origin": {
                        "type": "string",
                        "value": "auto.http.browser_report.nel",
                    },
                    "browser.report.type": {"type": "string", "value": "network-error"},
                    "url.domain": {"type": "string", "value": "example.com"},
                    "url.full": {
                        "type": "string",
                        "value": "https://example.com/index.html",
                    },
                    "http.request.duration": {"type": "integer", "value": 37},
                    "http.request.method": {"type": "string", "value": "GET"},
                    "http.response.status_code": {"type": "integer", "value": 500},
                    "network.protocol.name": {"type": "string", "value": "http"},
                    "network.protocol.version": {"type": "string", "value": "1.1"},
                    "server.address": {"type": "string", "value": "123.123.123.123"},
                    "http.request.header.referer": {
                        "type": "string",
                        "value": "https://example.com/nel/",
                    },
                    "nel.referrer": {
                        "type": "string",
                        "value": "https://example.com/nel/",
                    },
                    "nel.phase": {
                        "type": "string",
                        "value": "application",
                    },
                    "nel.sampling_fraction": {
                        "type": "double",
                        "value": 1.0,
                    },
                    "nel.type": {
                        "type": "string",
                        "value": "http.error",
                    },
                    "sentry.browser.name": {
                        "type": "string",
                        "value": "Python Requests",
                    },
                    "sentry.browser.version": {"type": "string", "value": "2.32"},
                    "sentry.observed_timestamp_nanos": {
                        "type": "string",
                        "value": time_within_delta(expect_resolution="ns"),
                    },
                },
                "body": "The user agent successfully received a response, but it had a 500 status code",
                "level": "warn",
                "timestamp": time_within_delta(expected_ts),
                "trace_id": mock.ANY,
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()
