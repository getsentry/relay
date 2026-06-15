import pytest
from queue import Empty
from datetime import datetime, timezone, timedelta


def test_client_reports(relay, mini_sentry):
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "source": "my-layer",
        }
    }

    relay = relay(mini_sentry, config)

    project_id = 42
    timestamp = datetime.now(tz=timezone.utc).replace(microsecond=123)

    report_payload = {
        "timestamp": timestamp.isoformat(),
        "discarded_events": [
            {"reason": "queue_overflow", "category": "error", "quantity": 42},
            {"reason": "queue_overflow", "category": "transaction", "quantity": 1231},
        ],
    }

    mini_sentry.add_full_project_config(project_id)

    # Send outcomes twice to see if they are aggregated
    relay.send_client_report(project_id, report_payload)
    report_payload["timestamp"] = (timestamp + timedelta(milliseconds=100)).isoformat()
    relay.send_client_report(project_id, report_payload)

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "outcome": 5,
            "reason": "queue_overflow",
            "source": "my-layer",
            "category": 1,
            "quantity": 84,
        },
        {
            "outcome": 5,
            "reason": "queue_overflow",
            "source": "my-layer",
            "category": 2,
            "quantity": 2462,
        },
    ]
    assert mini_sentry.captured_outcomes.empty()
    assert mini_sentry.captured_envelopes.empty()


def test_client_reports_bad_timestamps(relay, mini_sentry):
    config = {
        "outcomes": {
            "emit_outcomes": True,
        },
    }

    relay = relay(mini_sentry, config)

    project_id = 42
    timestamp = datetime.now(tz=timezone.utc) + timedelta(days=300)

    report_payload = {
        # too far into the future
        "timestamp": timestamp.isoformat(),
        "discarded_events": [
            {"reason": "queue_overflow", "category": "error", "quantity": 42},
            {"reason": "queue_overflow", "category": "transaction", "quantity": 1231},
        ],
    }

    mini_sentry.add_full_project_config(project_id)
    relay.send_client_report(project_id, report_payload)

    # we should not have received any outcomes because they are too far into the future
    with pytest.raises(Empty):
        mini_sentry.captured_outcomes.get(timeout=1.5)

    with pytest.raises(Empty):
        mini_sentry.captured_envelopes.get(timeout=1.5)
