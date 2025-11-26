from unittest import mock

from sentry_relay.consts import DataCategory

# Example Logplex syslog messages from Heroku documentation
# https://devcenter.heroku.com/articles/log-drains#https-drains
HEROKU_LOGS_PAYLOAD = """\
83 <40>1 2012-11-30T06:45:29+00:00 host app web.3 - State changed from starting to up
119 <40>1 2012-11-30T06:45:26+00:00 host app web.3 - Starting process with command `bundle exec rackup config.ru -p 24405`"""


EXPECTED_ITEMS = [
    {
        "organizationId": "1",
        "projectId": "42",
        "traceId": mock.ANY,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": mock.ANY,
        "attributes": {
            "sentry.origin": {"stringValue": "auto.log_drain.heroku"},
            "syslog.facility": {"intValue": "5"},
            "syslog.version": {"intValue": "1"},
            "syslog.procid": {"stringValue": "web.3"},
            "resource.host.name": {"stringValue": "host"},
            "resource.service.name": {"stringValue": "app"},
            "sentry.body": {"stringValue": "State changed from starting to up"},
            "sentry.severity_text": {"stringValue": "fatal"},
            "sentry.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry.timestamp_precise": {"intValue": "1354257929000000000"},
            "sentry.payload_size_bytes": {"intValue": mock.ANY},
            "sentry._meta.fields.trace_id": {
                "stringValue": '{"meta":{"":{"rem":[["trace_id.missing","s"]]}}}'
            },
            "heroku.logplex.frame_id": {"stringValue": "abc123"},
            "heroku.logplex.drain_token": {
                "stringValue": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            "heroku.logplex.user_agent": {"stringValue": "Logplex/v72"},
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
        "traceId": mock.ANY,
        "itemId": mock.ANY,
        "itemType": "TRACE_ITEM_TYPE_LOG",
        "timestamp": mock.ANY,
        "attributes": {
            "sentry.origin": {"stringValue": "auto.log_drain.heroku"},
            "syslog.facility": {"intValue": "5"},
            "syslog.version": {"intValue": "1"},
            "syslog.procid": {"stringValue": "web.3"},
            "resource.host.name": {"stringValue": "host"},
            "resource.service.name": {"stringValue": "app"},
            "sentry.body": {
                "stringValue": "Starting process with command `bundle exec rackup config.ru -p 24405`"
            },
            "sentry.severity_text": {"stringValue": "fatal"},
            "sentry.observed_timestamp_nanos": {"stringValue": mock.ANY},
            "sentry.timestamp_precise": {"intValue": "1354257926000000000"},
            "sentry.payload_size_bytes": {"intValue": mock.ANY},
            "sentry._meta.fields.trace_id": {
                "stringValue": '{"meta":{"":{"rem":[["trace_id.missing","s"]]}}}'
            },
            "heroku.logplex.frame_id": {"stringValue": "abc123"},
            "heroku.logplex.drain_token": {
                "stringValue": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a"
            },
            "heroku.logplex.user_agent": {"stringValue": "Logplex/v72"},
        },
        "clientSampleRate": 1.0,
        "serverSampleRate": 1.0,
        "retentionDays": 90,
        "received": mock.ANY,
        "downsampledRetentionDays": 90,
    },
]


def test_heroku_logs(
    mini_sentry, relay, relay_with_processing, outcomes_consumer, items_consumer
):
    """Test Heroku Logplex log drain ingestion."""
    items_consumer = items_consumer()
    outcomes_consumer = outcomes_consumer()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
        "organizations:relay-heroku-log-drain-endpoint",
    ]

    relay = relay(relay_with_processing())

    relay.send_heroku_logs(
        project_id,
        data=HEROKU_LOGS_PAYLOAD,
        headers={
            "Logplex-Msg-Count": "2",
            "Logplex-Frame-Id": "abc123",
            "Logplex-Drain-Token": "d.73ea7440-270a-435a-a0ea-adf50b4e5f5a",
            "User-Agent": "Logplex/v72",
        },
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
            "quantity": mock.ANY,
        },
    ]
