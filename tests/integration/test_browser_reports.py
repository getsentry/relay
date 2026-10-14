import json
from datetime import datetime, timedelta, timezone

from .asserts import matches_any, time_within_delta


def common_attributes(report_type, origin):
    return {
        "sentry.origin": {"type": "string", "value": origin},
        "browser.report.type": {"type": "string", "value": report_type},
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


def setup_relay(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = ["organizations:ourlogs-ingestion"]
    return project_id, relay(mini_sentry)


def captured_logs(mini_sentry):
    envelope = mini_sentry.get_captured_envelope()
    assert [item.type for item in envelope.items] == ["log"]
    payload = json.loads(envelope.items[0].payload.bytes)
    assert payload["version"] == 2
    assert mini_sentry.captured_envelopes.empty()
    return payload["items"]


def test_browser_reports_legacy_csp_report(mini_sentry, relay):
    project_id, relay = setup_relay(mini_sentry, relay)

    relay.send_browser_reports(
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

    assert captured_logs(mini_sentry) == [
        {
            "__header": matches_any(),
            "attributes": {
                **common_attributes("csp-violation", "auto.http.browser_report.csp"),
                "url.domain": {"type": "string", "value": "example.com"},
                "url.full": {"type": "string", "value": "https://example.com/foo/bar"},
                "http.request.header.referer": {
                    "type": "string",
                    "value": "https://www.google.com/",
                },
                "http.response.status_code": {"type": "integer", "value": 200},
                "csp.effective_directive": {"type": "string", "value": "script-src"},
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
    ]


def test_browser_reports_reporting_api_batch(mini_sentry, relay):
    project_id, relay = setup_relay(mini_sentry, relay)

    relay.send_browser_reports(
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
            {
                "age": 1000,
                "body": {
                    "elapsed_time": 37,
                    "method": "GET",
                    "phase": "application",
                    "protocol": "http/1.1",
                    "sampling_fraction": 1,
                    "server_ip": "123.123.123.123",
                    "status_code": 500,
                    "type": "http.error",
                },
                "type": "network-error",
                "url": "https://example.com/index.html",
            },
            {
                "age": 2000,
                "body": {
                    "id": "websql",
                    "lineNumber": 12,
                    "message": "WebSQL is deprecated",
                    "sourceFile": "https://example.com/index.js",
                },
                "type": "deprecation",
                "url": "https://example.com/page",
            },
            {
                "age": 3000,
                "body": {
                    "reason": "oom",
                    "is_top_level": True,
                },
                "type": "crash",
                "url": "https://example.com/page",
            },
            {
                "age": 4000,
                "body": {
                    "blockedURL": "https://example.com/script.js",
                    "destination": "script",
                    "documentURL": "https://example.com/page",
                    "reportOnly": False,
                },
                "type": "integrity-violation",
                "url": "https://example.com/page",
            },
            # Reports without a type are skipped.
            {"body": {}},
        ],
    )

    now = datetime.now(tz=timezone.utc)
    logs = captured_logs(mini_sentry)

    assert [
        (log["attributes"]["browser.report.type"]["value"], log["body"], log["level"])
        for log in logs
    ] == [
        ("csp-violation", "Blocked 'image' from 'evil.com'", "warn"),
        (
            "network-error",
            "The user agent successfully received a response, but it had a 500 status code",
            "warn",
        ),
        ("deprecation", "WebSQL is deprecated", "info"),
        ("crash", "Browser crash: oom", "error"),
        ("integrity-violation", "Browser report: integrity-violation", "warn"),
    ]

    # Time is corrected by the `age` specified in each report.
    for log, age in zip(logs, [60000, 1000, 2000, 3000, 4000]):
        assert log["timestamp"] == time_within_delta(now - timedelta(milliseconds=age))

    deprecation = logs[2]
    assert deprecation["attributes"] == {
        **common_attributes("deprecation", "auto.http.browser_report.deprecation"),
        "url.domain": {"type": "string", "value": "example.com"},
        "url.full": {"type": "string", "value": "https://example.com/page"},
        "browser.report.body.id": {"type": "string", "value": "websql"},
        "browser.report.body.line_number": {"type": "integer", "value": 12},
        "browser.report.body.message": {
            "type": "string",
            "value": "WebSQL is deprecated",
        },
        "browser.report.body.source_file": {
            "type": "string",
            "value": "https://example.com/index.js",
        },
    }

    crash = logs[3]
    assert crash["attributes"] == {
        **common_attributes("crash", "auto.http.browser_report.crash"),
        "url.domain": {"type": "string", "value": "example.com"},
        "url.full": {"type": "string", "value": "https://example.com/page"},
        "browser.report.body.reason": {"type": "string", "value": "oom"},
        "browser.report.body.is_top_level": {"type": "boolean", "value": True},
    }
