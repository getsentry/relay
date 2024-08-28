from datetime import timedelta, datetime, timezone

from sentry_sdk.envelope import Envelope


def mock_transaction(timestamp):
    return {
        "type": "transaction",
        "timestamp": format_date(timestamp),
        "start_timestamp": format_date(timestamp - timedelta(seconds=2)),
        "spans": [],
        "contexts": {
            "trace": {
                "op": "http",
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
            }
        },
        "transaction": "/hello",
    }


def format_date(date):
    return date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def test_clock_drift_applied_when_timestamp_is_too_old(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    now = datetime.now(tz=timezone.utc)
    one_month_ago = now - timedelta(days=30)

    envelope = Envelope(headers={"sent_at": format_date(one_month_ago)})
    envelope.add_transaction(mock_transaction(one_month_ago))

    relay.send_envelope(project_id, envelope, headers={"Accept-Encoding": "gzip"})

    transaction_event = mini_sentry.captured_events.get(
        timeout=1
    ).get_transaction_event()

    error_name, error_metadata = transaction_event["_meta"]["timestamp"][""]["err"][0]
    assert error_name == "clock_drift"
    assert (
        one_month_ago.timestamp()
        < transaction_event["timestamp"]
        < datetime.now(tz=timezone.utc).timestamp()
    )


def test_clock_drift_not_applied_when_timestamp_is_recent(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    now = datetime.now(tz=timezone.utc)
    five_minutes_ago = now - timedelta(minutes=5)

    envelope = Envelope(headers={"sent_at": format_date(five_minutes_ago)})
    envelope.add_transaction(mock_transaction(five_minutes_ago))

    relay.send_envelope(project_id, envelope, headers={"Accept-Encoding": "gzip"})

    transaction_event = mini_sentry.captured_events.get(
        timeout=1
    ).get_transaction_event()
    assert "_meta" not in transaction_event
    assert transaction_event["timestamp"] == five_minutes_ago.timestamp()


def test_clock_drift_not_applied_when_sent_at_is_not_supplied(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["config"]["eventRetention"] = 30

    now = datetime.now(tz=timezone.utc)
    one_month_ago = now - timedelta(days=30)

    envelope = Envelope()
    envelope.add_transaction(mock_transaction(one_month_ago))

    relay.send_envelope(project_id, envelope, headers={"Accept-Encoding": "gzip"})

    transaction_event = mini_sentry.captured_events.get(
        timeout=1
    ).get_transaction_event()
    error_name, error_metadata = transaction_event["_meta"]["timestamp"][""]["err"][0]
    # In case clock drift is not run, we expect timestamps normalization to go into effect to mark timestamps as
    # past or future if they surpass a certain threshold.
    assert error_name == "past_timestamp"
    assert (
        one_month_ago.timestamp()
        < transaction_event["timestamp"]
        < datetime.now(tz=timezone.utc).timestamp()
    )
