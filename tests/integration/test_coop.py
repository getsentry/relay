import json
from datetime import datetime, timezone
from unittest import mock

from .asserts import time_within_delta


def test_coop_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)

    relay.send_coop_event(project_id)

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the COOP report (age=0 in this case)
    expected_ts = datetime.now(tz=timezone.utc)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "items": [
            {
                "__header": mock.ANY,
                "attributes": {
                    "sentry.origin": {
                        "type": "string",
                        "value": "auto.http.browser_report.coop",
                    },
                    "browser.report.type": {"type": "string", "value": "coop"},
                    "url.full": {
                        "type": "string",
                        "value": "https://example.com/",
                    },
                    "coop.disposition": {"type": "string", "value": "enforce"},
                    "coop.effective_policy": {"type": "string", "value": "same-origin"},
                    "coop.violation": {
                        "type": "string",
                        "value": "navigate-to-document",
                    },
                    "coop.previous_response_url": {
                        "type": "string",
                        "value": "https://example.com/previous",
                    },
                    "http.request.header.referer": {
                        "type": "string",
                        "value": "https://referrer.com/",
                    },
                    "sentry.browser.name": {
                        "type": "string",
                        "value": "Chrome",
                    },
                    "sentry.browser.version": {"type": "string", "value": "116.0"},
                    "sentry.observed_timestamp_nanos": {
                        "type": "string",
                        "value": time_within_delta(expect_resolution="ns"),
                    },
                },
                "body": "Navigating to a document with a different COOP policy caused a browsing context group switch",
                "level": "error",
                "timestamp": time_within_delta(expected_ts),
                "trace_id": mock.ANY,
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()


def test_coop_access_violation_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)

    payload = [
        {
            "age": 123,
            "body": {
                "disposition": "reporting",
                "effectivePolicy": "same-origin-allow-popups",
                "openerURL": "https://opener.com/",
                "referrer": "",
                "violation": "access-from-coop-page-to-opener",
                "property": "postMessage",
                "sourceFile": "https://example.com/script.js",
                "lineNumber": 42,
                "columnNumber": 15,
            },
            "type": "coop",
            "url": "https://example.com/page",
            "user_agent": "Mozilla/5.0",
        }
    ]

    relay.send_coop_event(project_id, payload=payload)

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the COOP report (age=123 in this case)
    expected_ts = datetime.now(tz=timezone.utc)

    assert [item.type for item in envelope.items] == ["log"]
    log_data = json.loads(envelope.items[0].payload.bytes)

    assert log_data["items"][0]["body"] == "Blocked access from COOP page to its opener window"
    assert log_data["items"][0]["level"] == "warn"
    assert log_data["items"][0]["attributes"]["coop.disposition"]["value"] == "reporting"
    assert log_data["items"][0]["attributes"]["coop.property"]["value"] == "postMessage"
    assert log_data["items"][0]["attributes"]["coop.source_file"]["value"] == "https://example.com/script.js"
    assert log_data["items"][0]["attributes"]["coop.line_number"]["value"] == 42
    assert log_data["items"][0]["attributes"]["coop.column_number"]["value"] == 15
    assert log_data["items"][0]["attributes"]["coop.opener_url"]["value"] == "https://opener.com/"

    assert mini_sentry.captured_envelopes.empty()
