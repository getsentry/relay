import pytest
from queue import Empty
from datetime import datetime, timezone, timedelta


def test_client_reports(relay, mini_sentry):
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "my-layer",
            "bucket_interval": 1,
            "flush_delay": 0,
        }
    }

    relay = relay(mini_sentry, config)

    project_id = 42
    timestamp = datetime.now(tz=timezone.utc)

    report_payload = {
        "timestamp": timestamp.isoformat(),
        "discarded_events": [
            {"reason": "queue_overflow", "category": "error", "quantity": 42},
            {"reason": "queue_overflow", "category": "transaction", "quantity": 1231},
        ],
    }

    mini_sentry.add_full_project_config(project_id)
    relay.send_client_report(project_id, report_payload)

    outcomes = []
    for _ in range(2):
        outcomes.extend(mini_sentry.captured_outcomes.get(timeout=1.2)["outcomes"])

    outcomes.sort(key=lambda x: x["category"])

    timestamp_formatted = timestamp.isoformat().split(".")[0] + ".000000Z"
    assert outcomes == [
        {
            "timestamp": timestamp_formatted,
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 5,
            "reason": "queue_overflow",
            "remote_addr": "127.0.0.1",
            "source": "my-layer",
            "category": 1,
            "quantity": 42,
        },
        {
            "timestamp": timestamp_formatted,
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 5,
            "reason": "queue_overflow",
            "remote_addr": "127.0.0.1",
            "source": "my-layer",
            "category": 2,
            "quantity": 1231,
        },
    ]


def test_client_reports_bad_timestamps(relay, mini_sentry):
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "my-layer",
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
        mini_sentry.captured_outcomes.get(timeout=1.5)["outcomes"]
