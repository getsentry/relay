from unittest import mock
import json

from sentry_relay.consts import DataCategory

# From Vercel Log Drain Docs: https://vercel.com/docs/drains/reference/logs#format
VERCEL_LOG_1 = {
    "id": "1573817187330377061717300000",
    "deploymentId": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD",
    "source": "build",
    "host": "my-app-abc123.vercel.app",
    "timestamp": 1573817187330,
    "projectId": "gdufoJxB6b9b1fEqr1jUtFkyavUU",
    "level": "info",
    "message": "Build completed successfully",
    "buildId": "bld_cotnkcr76",
    "type": "stdout",
    "projectName": "my-app",
}

# From Vercel Log Drain Docs: https://vercel.com/docs/drains/reference/logs#format
VERCEL_LOG_2 = {
    "id": "1573817250283254651097202070",
    "deploymentId": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD",
    "source": "lambda",
    "host": "my-app-abc123.vercel.app",
    "timestamp": 1573817250283,
    "projectId": "gdufoJxB6b9b1fEqr1jUtFkyavUU",
    "level": "info",
    "message": "API request processed",
    "entrypoint": "api/index.js",
    "requestId": "643af4e3-975a-4cc7-9e7a-1eda11539d90",
    "statusCode": 200,
    "path": "/api/users",
    "executionRegion": "sfo1",
    "environment": "production",
    "traceId": "1b02cd14bb8642fd092bc23f54c7ffcd",
    "spanId": "f24e8631bd11faa7",
    "trace.id": "1b02cd14bb8642fd092bc23f54c7ffcd",
    "span.id": "f24e8631bd11faa7",
    "proxy": {
        "timestamp": 1573817250172,
        "method": "GET",
        "host": "my-app.vercel.app",
        "path": "/api/users?page=1",
        "userAgent": ["Mozilla/5.0..."],
        "referer": "https://my-app.vercel.app",
        "region": "sfo1",
        "statusCode": 200,
        "clientIp": "120.75.16.101",
        "scheme": "https",
        "vercelCache": "MISS",
    },
}

EXPECTED_ITEMS = [
    {
        "organizationId": "1",
        "projectId": "42",
        "traceId": mock.ANY,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": mock.ANY,
        "attributes": {
            "vercel.id": {"stringValue": "1573817187330377061717300000"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "sentry.timestamp_nanos": {"stringValue": "1573817187330000000"},
            "sentry.origin": {"stringValue": "auto.log_drain.vercel"},
            "server.address": {"stringValue": "my-app-abc123.vercel.app"},
            "vercel.source": {"stringValue": "build"},
            "vercel.deployment_id": {"stringValue": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD"},
            "vercel.log_type": {"stringValue": "stdout"},
            "sentry.body": {"stringValue": "Build completed successfully"},
            "vercel.project_name": {"stringValue": "my-app"},
            "sentry.severity_text": {"stringValue": "info"},
            "sentry.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry._internal.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry.timestamp_precise": {"intValue": "1573817187330000000"},
            "vercel.build_id": {"stringValue": "bld_cotnkcr76"},
            "sentry.payload_size_bytes": {"intValue": "496"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "vercel.project_id": {"stringValue": "gdufoJxB6b9b1fEqr1jUtFkyavUU"},
            "sentry._meta.fields.trace_id": {
                "stringValue": '{"meta":{"":{"rem":[["trace_id.missing","s"]]}}}'
            },
        },
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "retentionDays": 90,
        "received": mock.ANY,
        "downsampledRetentionDays": 90,
    },
    {
        "organizationId": "1",
        "projectId": "42",
        "traceId": "1b02cd14bb8642fd092bc23f54c7ffcd",
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": mock.ANY,
        "attributes": {
            "vercel.path": {"stringValue": "/api/users"},
            "sentry.browser.version": {"stringValue": "2.32"},
            "vercel.proxy.scheme": {"stringValue": "https"},
            "vercel.entrypoint": {"stringValue": "api/index.js"},
            "vercel.proxy.user_agent": {"stringValue": '["Mozilla/5.0..."]'},
            "vercel.proxy.client_ip": {"stringValue": "120.75.16.101"},
            "server.address": {"stringValue": "my-app-abc123.vercel.app"},
            "vercel.proxy.path": {"stringValue": "/api/users?page=1"},
            "vercel.status_code": {"intValue": "200"},
            "vercel.deployment_id": {"stringValue": "dpl_233NRGRjVZX1caZrXWtz5g1TAksD"},
            "vercel.proxy.method": {"stringValue": "GET"},
            "vercel.execution_region": {"stringValue": "sfo1"},
            "sentry.severity_text": {"stringValue": "info"},
            "sentry.span_id": {"stringValue": "f24e8631bd11faa7"},
            "sentry.browser.name": {"stringValue": "Python Requests"},
            "vercel.project_id": {"stringValue": "gdufoJxB6b9b1fEqr1jUtFkyavUU"},
            "vercel.request_id": {
                "stringValue": "643af4e3-975a-4cc7-9e7a-1eda11539d90"
            },
            "vercel.proxy.host": {"stringValue": "my-app.vercel.app"},
            "vercel.proxy.referer": {"stringValue": "https://my-app.vercel.app"},
            "vercel.id": {"stringValue": "1573817250283254651097202070"},
            "sentry.environment": {"stringValue": "production"},
            "vercel.proxy.vercel_cache": {"stringValue": "MISS"},
            "sentry.timestamp_nanos": {"stringValue": "1573817250283000000"},
            "sentry.origin": {"stringValue": "auto.log_drain.vercel"},
            "vercel.source": {"stringValue": "lambda"},
            "vercel.proxy.timestamp": {"intValue": "1573817250172"},
            "sentry.body": {"stringValue": "API request processed"},
            "vercel.proxy.status_code": {"intValue": "200"},
            "sentry.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry._internal.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry.timestamp_precise": {"intValue": "1573817250283000000"},
            "sentry.payload_size_bytes": {"intValue": "949"},
            "vercel.proxy.region": {"stringValue": "sfo1"},
        },
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "retentionDays": 90,
        "received": mock.ANY,
        "downsampledRetentionDays": 90,
    },
]


def test_vercel_logs_json_array(
    mini_sentry, relay, relay_with_processing, outcomes_consumer, items_consumer
):
    """Test Vercel logs ingestion with JSON array format."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-vercel-log-drain-endpoint",
    ]

    relay = relay(relay_with_processing())

    vercel_logs_payload = [
        VERCEL_LOG_1,
        VERCEL_LOG_2,
    ]

    relay.send_vercel_logs(
        project_id,
        data=json.dumps(vercel_logs_payload),
        headers={"Content-Type": "application/json"},
    )

    items = items_consumer.get_items(n=2)
    assert items == EXPECTED_ITEMS

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=4)
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
            "quantity": 1445,
        },
    ]


def test_vercel_logs_ndjson(
    mini_sentry, relay, relay_with_processing, outcomes_consumer, items_consumer
):
    """Test Vercel logs ingestion with NDJSON format."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-vercel-log-drain-endpoint",
    ]

    relay = relay(relay_with_processing())

    # Format as NDJSON
    ndjson_payload = json.dumps(VERCEL_LOG_1) + "\n" + json.dumps(VERCEL_LOG_2) + "\n"

    relay.send_vercel_logs(
        project_id,
        data=ndjson_payload,
        headers={"Content-Type": "application/x-ndjson"},
    )

    items = items_consumer.get_items(n=2)
    assert items == EXPECTED_ITEMS

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=4)
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
            "quantity": 1445,
        },
    ]
