import json
from datetime import datetime, timedelta, timezone

from .asserts import matches_any, time_within_delta


def test_nel_browser_report_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:browser-report-ingestion",
    ]
    relay = relay(mini_sentry)

    relay.send_browser_report(
        project_id,
        payload=[
            {
                "age": 1200000,
                "body": {
                    "elapsed_time": 37,
                    "method": "GET",
                    "phase": "application",
                    "protocol": "http/1.1",
                    "referrer": "https://example.com/nel/",
                    "sampling_fraction": 1,
                    "server_ip": "123.123.123.123",
                    "status_code": 500,
                    "type": "http.error",
                },
                "type": "network-error",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            }
        ],
    )

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the NEL report
    expected_ts = datetime.now(tz=timezone.utc) - timedelta(milliseconds=1200000)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "version": 2,
        "items": [
            {
                "__header": matches_any(),
                "attributes": {
                    "sentry.origin": {
                        "type": "string",
                        "value": "auto.http.browser_report",
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
                    "browser.name": {
                        "type": "string",
                        "value": "Chrome",
                    },
                    "browser.version": {"type": "string", "value": "111.0.0"},
                    "user_agent.original": {
                        "type": "string",
                        "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                    },
                    "sentry.observed_timestamp_nanos": {
                        "type": "string",
                        "value": time_within_delta(expect_resolution="ns"),
                    },
                },
                "body": "The user agent successfully received a response, but it had a 500 status code",
                "level": "warn",
                "timestamp": time_within_delta(expected_ts),
                "trace_id": matches_any(),
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()


def test_integrity_browser_report_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:browser-report-ingestion",
    ]
    relay = relay(mini_sentry)

    relay.send_browser_report(
        project_id,
        payload=[
            {
                "age": 5000,
                "type": "integrity-violation",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                "body": {
                    "documentUrl": "https://example.com/index.html",
                    "blockedUrl": "https://cdn.example.com/app.js",
                    "destination": "script",
                    "reportOnly": True,
                },
            }
        ],
    )

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the NEL report
    expected_ts = datetime.now(tz=timezone.utc) - timedelta(milliseconds=5000)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "version": 2,
        "items": [
            {
                "__header": matches_any(),
                "timestamp": time_within_delta(expected_ts),
                "trace_id": matches_any(),
                "level": "warn",
                "body": "Integrity policy would block script from 'https://cdn.example.com/app.js', but does not due to report only configuration",
                "attributes": {
                    "browser.name": {
                        "type": "string",
                        "value": "Chrome",
                    },
                    "browser.report.type": {
                        "type": "string",
                        "value": "integrity-violation",
                    },
                    "browser.version": {"type": "string", "value": "111.0.0"},
                    "integrity.blocked_url": {
                        "type": "string",
                        "value": "https://cdn.example.com/app.js",
                    },
                    "integrity.destination": {"type": "string", "value": "script"},
                    "integrity.document_url": {
                        "type": "string",
                        "value": "https://example.com/index.html",
                    },
                    "integrity.report_only": {"type": "boolean", "value": True},
                    "sentry.observed_timestamp_nanos": {
                        "type": "string",
                        "value": time_within_delta(expect_resolution="ns"),
                    },
                    "sentry.origin": {
                        "type": "string",
                        "value": "auto.http.browser_report",
                    },
                    "url.domain": {"type": "string", "value": "example.com"},
                    "url.full": {
                        "type": "string",
                        "value": "https://example.com/index.html",
                    },
                    "user_agent.original": {
                        "type": "string",
                        "value": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
                    },
                },
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()
