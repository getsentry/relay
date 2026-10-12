import json
from datetime import datetime, timedelta, timezone

from .asserts import matches_any, time_within_delta


def common_attributes():
    return {
        "sentry.origin": {
            "type": "string",
            "value": "auto.http.browser_report.csp",
        },
        "browser.report.type": {"type": "string", "value": "csp-violation"},
        "browser.name": {"type": "string", "value": "Firefox"},
        "browser.version": {"type": "string", "value": "42.0"},
        "user_agent.original": {
            "type": "string",
            "value": "RelayIntegrationTests/1.0.0 Firefox/42.0",
        },
        "sentry.observed_timestamp_nanos": {
            "type": "string",
            "value": time_within_delta(expect_resolution="ns"),
        },
        "sentry.relay.ingress": {"type": "string", "value": "integration"},
    }


def test_csp_legacy_report_converted_to_log(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)

    relay.send_csp_logs(
        project_id,
        {
            "csp-report": {
                "document-uri": "https://example.com/foo/bar",
                "referrer": "https://www.google.com/",
                "violated-directive": "script-src 'self'",
                "effective-directive": "script-src",
                "blocked-uri": "https://evil.com/script.js",
                "status-code": 200,
                "disposition": "enforce",
            }
        },
        content_type="application/csp-report",
    )

    envelope = mini_sentry.get_captured_envelope()

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "version": 2,
        "items": [
            {
                "__header": matches_any(),
                "attributes": {
                    **common_attributes(),
                    "url.domain": {"type": "string", "value": "example.com"},
                    "url.full": {
                        "type": "string",
                        "value": "https://example.com/foo/bar",
                    },
                    "http.request.header.referer": {
                        "type": "string",
                        "value": "https://www.google.com/",
                    },
                    "http.response.status_code": {"type": "integer", "value": 200},
                    "csp.effective_directive": {
                        "type": "string",
                        "value": "script-src",
                    },
                    "csp.violated_directive": {
                        "type": "string",
                        "value": "script-src 'self'",
                    },
                    "csp.blocked_uri": {
                        "type": "string",
                        "value": "https://evil.com/script.js",
                    },
                    "csp.blocked_host": {"type": "string", "value": "evil.com"},
                    "csp.disposition": {"type": "string", "value": "enforce"},
                },
                "body": "Blocked 'script' from 'evil.com'",
                "level": "warn",
                "timestamp": time_within_delta(),
                "trace_id": matches_any(),
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()


def test_csp_reporting_api_reports_converted_to_logs(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    relay = relay(mini_sentry)

    relay.send_csp_logs(
        project_id,
        [
            {
                "age": 60000,
                "body": {
                    "blockedURL": "https://evil.com/image.png",
                    "disposition": "report",
                    "documentURL": "https://example.com/page",
                    "effectiveDirective": "img-src",
                },
                "type": "csp-violation",
                "url": "https://example.com/page",
            },
            # Other report types sent to the same endpoint are ignored.
            {
                "age": 10,
                "body": {
                    "blockedURL": "https://example.com/script.js",
                    "destination": "script",
                    "documentURL": "https://example.com/page",
                    "reportOnly": False,
                },
                "type": "integrity-violation",
                "url": "https://example.com/page",
            },
        ],
    )

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the report.
    expected_ts = datetime.now(tz=timezone.utc) - timedelta(milliseconds=60000)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "version": 2,
        "items": [
            {
                "__header": matches_any(),
                "attributes": {
                    **common_attributes(),
                    "url.domain": {"type": "string", "value": "example.com"},
                    "url.full": {
                        "type": "string",
                        "value": "https://example.com/page",
                    },
                    "csp.effective_directive": {
                        "type": "string",
                        "value": "img-src",
                    },
                    "csp.blocked_uri": {
                        "type": "string",
                        "value": "https://evil.com/image.png",
                    },
                    "csp.blocked_host": {"type": "string", "value": "evil.com"},
                    "csp.disposition": {"type": "string", "value": "report"},
                },
                "body": "Blocked 'image' from 'evil.com'",
                "level": "warn",
                "timestamp": time_within_delta(expected_ts),
                "trace_id": matches_any(),
            }
        ],
    }
    assert mini_sentry.captured_envelopes.empty()
