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
                    "request_headers": {"Accept-Language": ["de-DE"]},
                    "response_headers": {"X-Served-By": ["cache-fra-1", "cache-fra-2"]},
                },
                "type": "network-error",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            },
            # This report has an invalid type. The Browser Report
            # integration should ignore it.
            {
                "age": 1700000,
                "body": {
                    "frobnicated": True,
                    "mana_used": 123,
                },
                "type": "future-magic",
                "url": "https://example.com/index.html",
                "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36",
            },
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
                        "type": "array",
                        "value": ["https://example.com/nel/"],
                    },
                    "http.request.header.accept-language": {
                        "type": "array",
                        "value": ["de-DE"],
                    },
                    "http.response.header.x-served-by": {
                        "type": "array",
                        "value": ["cache-fra-1", "cache-fra-2"],
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
                    "documentURL": "https://example.com/index.html",
                    "blockedURL": "https://cdn.example.com/app.js",
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


def test_csp_browser_report_converted_to_logs(mini_sentry, relay):
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
                "age": 10,
                "body": {
                    "blockedURL": "https://example.net/assets/media/7_del.png",
                    "disposition": "enforce",
                    "documentURL": "https://example.net/assets/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    "effectiveDirective": "img-src",
                    "lineNumber": 9,
                    "originalPolicy": "default-src 'none'; report-to endpoint-csp;",
                    "referrer": "https://example.net/test229/",
                    "sourceFile": "https://example.net/assets/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    "statusCode": 0,
                },
                "type": "csp-violation",
                "url": "https://example.com/assets/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                "user_agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            },
            {
                "age": 0,
                "body": {
                    "blockedURL": "https://example.com/tst/media/7_del.png",
                    "disposition": "report",
                    "documentURL": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    "effectiveDirective": "img-src",
                    "lineNumber": 9,
                    "originalPolicy": "default-src 'none'; report-to endpoint-csp;",
                    "referrer": "https://example.com/test229/",
                    "sourceFile": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    "statusCode": 0,
                },
                "type": "csp-violation",
                "url": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                "user_agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
            },
        ],
    )

    envelope = mini_sentry.get_captured_envelope()

    # Time is corrected by the `age` specified in the NEL report
    expected_ts_1 = datetime.now(tz=timezone.utc) - timedelta(milliseconds=10)
    expected_ts_2 = datetime.now(tz=timezone.utc)

    assert [item.type for item in envelope.items] == ["log"]
    assert json.loads(envelope.items[0].payload.bytes) == {
        "version": 2,
        "items": [
            {
                "__header": matches_any(),
                "timestamp": time_within_delta(expected_ts_1),
                "trace_id": matches_any(),
                "level": "warn",
                "body": "Blocked 'image' from 'example.net'",
                "attributes": {
                    "browser.name": {
                        "type": "string",
                        "value": "Chrome",
                    },
                    "browser.report.type": {"type": "string", "value": "csp-violation"},
                    "browser.version": {"type": "string", "value": "79.0.3945"},
                    "csp.blocked_url": {
                        "type": "string",
                        "value": "https://example.net/assets/media/7_del.png",
                    },
                    "csp.document_url": {
                        "type": "string",
                        "value": "https://example.net/assets/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    },
                    "csp.effective_directive": {
                        "type": "string",
                        "value": "img-src",
                    },
                    "csp.original_policy": {
                        "type": "string",
                        "value": "default-src 'none'; report-to endpoint-csp;",
                    },
                    "csp.report_only": {
                        "type": "boolean",
                        "value": False,
                    },
                    "http.request.header.referer": {
                        "type": "string",
                        "value": "https://example.net/test229/",
                    },
                    "http.response.status_code": {"type": "integer", "value": 0},
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
                        "value": "https://example.com/assets/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    },
                    "user_agent.original": {
                        "type": "string",
                        "value": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
                    },
                },
            },
            {
                "__header": matches_any(),
                "timestamp": time_within_delta(expected_ts_2),
                "trace_id": matches_any(),
                "level": "warn",
                "body": "Blocked 'image' from 'example.com'",
                "attributes": {
                    "browser.name": {
                        "type": "string",
                        "value": "Chrome",
                    },
                    "browser.report.type": {"type": "string", "value": "csp-violation"},
                    "browser.version": {"type": "string", "value": "79.0.3945"},
                    "csp.blocked_url": {
                        "type": "string",
                        "value": "https://example.com/tst/media/7_del.png",
                    },
                    "csp.document_url": {
                        "type": "string",
                        "value": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    },
                    "csp.effective_directive": {
                        "type": "string",
                        "value": "img-src",
                    },
                    "csp.original_policy": {
                        "type": "string",
                        "value": "default-src 'none'; report-to endpoint-csp;",
                    },
                    "csp.report_only": {
                        "type": "boolean",
                        "value": True,
                    },
                    "http.request.header.referer": {
                        "type": "string",
                        "value": "https://example.com/test229/",
                    },
                    "http.response.status_code": {"type": "integer", "value": 0},
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
                        "value": "https://example.com/tst/test_frame.php?ID=229&hash=da964209653e467d337313e51876e27d",
                    },
                    "user_agent.original": {
                        "type": "string",
                        "value": "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36",
                    },
                },
            },
        ],
    }
    assert mini_sentry.captured_envelopes.empty()
