from datetime import timedelta, datetime

from sentry_sdk.envelope import Envelope


def mock_transaction(timestamp):
    return {
        "type": "transaction",
        "timestamp": timestamp.isoformat(),
        "start_timestamp": (timestamp - timedelta(seconds=2)).isoformat(),
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


def test_clock_drift_applied_when_timestamp_is_too_old(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    now = datetime.utcnow()
    one_month_ago = now - timedelta(days=30)

    envelope = Envelope(
        headers={"sent_at": one_month_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
    )
    envelope.add_transaction(mock_transaction(one_month_ago))

    relay.send_envelope(project_id, envelope, headers={"Accept-Encoding": "gzip"})

    transaction_event = mini_sentry.captured_events.get(
        timeout=1
    ).get_transaction_event()
    error_name, error_metadata = transaction_event["_meta"]["timestamp"][""]["err"][0]
    assert error_name == "clock_drift"


def test_clock_drift_not_applied_when_timestamp_is_recent(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    now = datetime.utcnow()
    five_minutes_ago = now - timedelta(minutes=5)

    envelope = Envelope(
        headers={"sent_at": five_minutes_ago.strftime("%Y-%m-%dT%H:%M:%S.%fZ")}
    )
    envelope.add_transaction(mock_transaction(five_minutes_ago))

    relay.send_envelope(project_id, envelope, headers={"Accept-Encoding": "gzip"})

    transaction_event = mini_sentry.captured_events.get(
        timeout=1
    ).get_transaction_event()
    assert "_meta" not in transaction_event


def test_clock_drift_not_applied_when_sent_at_is_not_supplied(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    now = datetime.utcnow()
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
