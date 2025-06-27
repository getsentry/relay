import json
from datetime import datetime, timedelta, timezone
from unittest import mock

from .asserts import time_within_delta


def test_nel_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)
    payload = relay.send_nel_event(project_id)

    envelope = mini_sentry.captured_events.get(timeout=1)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "items": [
            {
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
                    "browser.nel.referrer": {
                        "type": "string",
                        "value": "https://example.com/nel/",
                    },
                    "browser.nel.phase": {
                        "type": "string",
                        "value": "application",
                    },
                    "browser.nel.sampling_fraction": {
                        "type": "double",
                        "value": 1.0,
                    },
                    "browser.nel.type": {
                        "type": "string",
                        "value": "http.error",
                    },
                },
                "body": "The user agent successfully received a response, but it had a 500 status code",
                "level": "info",
                # Time is corrected by the `age` specified in the NEL report
                "timestamp": time_within_delta(
                    datetime.now(tz=timezone.utc)
                    - timedelta(milliseconds=payload[0]["age"])
                ),
                "trace_id": mock.ANY,
            }
        ],
    }
    assert mini_sentry.captured_events.empty()
