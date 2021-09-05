from datetime import datetime, timezone


def test_client_reports(relay, mini_sentry):
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "my-layer",
        }
    }

    relay = relay(mini_sentry, config)

    project_id = 42
    timestamp = datetime.now(tz=timezone.utc)

    report_payload = {
        "timestamp": timestamp.isoformat(),
        "discarded_events": [
            ["queue_overflow", "error", 42],
            ["queue_overflow", "transaction", 1231],
        ],
    }

    mini_sentry.add_full_project_config(project_id)
    relay.send_client_report(project_id, report_payload)

    outcomes = []
    for _ in range(2):
        outcomes.extend(mini_sentry.captured_outcomes.get(timeout=0.2)["outcomes"])

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
