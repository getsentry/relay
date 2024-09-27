import json
import time
import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from datetime import datetime, timezone, time as dttime


def test_multi_write_redis_client_with_metric_meta(
    mini_sentry, redis_client, secondary_redis_client, relay_with_processing
):
    project_id = 42
    now = datetime.now(tz=timezone.utc)
    start_of_day = datetime.combine(now, dttime.min, tzinfo=timezone.utc)

    # Key magic number is the fnv32 hash from the MRI.
    redis_key = "mm:l:{1}:42:2718098263:" + str(int(start_of_day.timestamp()))
    # Clear just so there is no overlap with previous tests.
    redis_client.delete(redis_key)

    relay = relay_with_processing(
        {
            "processing": {
                "redis": {
                    "configs": [
                        {"server": "redis://127.0.0.1:6379"},
                        # We want to test with multiple nesting levels to make sure the multi-write
                        # works nonetheless.
                        {"configs": ["redis://127.0.0.1:6380"]},
                    ]
                }
            }
        }
    )

    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).append("organizations:custom-metrics")

    location = {
        "type": "location",
        "function": "_scan_for_suspect_projects",
        "module": "sentry.tasks.low_priority_symbolication",
        "filename": "sentry/tasks/low_priority_symbolication.py",
        "abs_path": "/usr/src/sentry/src/sentry/tasks/low_priority_symbolication.py",
        "lineno": 45,
    }

    envelope = Envelope()
    payload = {
        "timestamp": now.isoformat(),
        "mapping": {
            "d:custom/sentry.process_profile.track_outcome@second": [
                location,
            ]
        },
    }
    envelope.add_item(Item(PayloadRef(json=payload), type="metric_meta"))
    relay.send_envelope(project_id, envelope)

    time.sleep(1)

    for client in [redis_client, secondary_redis_client]:
        stored = sorted(
            (json.loads(v) for v in client.smembers(redis_key)),
            key=lambda x: x["lineno"],
        )
        assert len(stored) == 1


def test_multi_write_redis_client_with_rate_limiting(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    outcomes_consumer,
):
    project_id = 42
    event_id = uuid.uuid1().hex
    relay = relay_with_processing(
        {
            "processing": {
                "redis": {
                    "configs": [
                        {"server": "redis://127.0.0.1:6379"},
                        {"server": "redis://127.0.0.1:6380"},
                    ]
                }
            }
        }
    )
    outcomes_consumer = outcomes_consumer(timeout=10)
    events_consumer = events_consumer()

    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{event_id}",
            "categories": ["error"],
            "window": 3600,
            "limit": 1,
            "reasonCode": "drop_all",
        }
    ]

    error_payload = {
        "event_id": event_id,
        "message": "test",
        "extra": {"msg_text": "test"},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope()
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    # We send the first envelope.
    relay.send_envelope(project_id, envelope)

    # Because of the quotas, we should drop error, and since the user_report is in the same envelope, we should also drop it.
    events_consumer.assert_empty()

    # We must have 1 outcome with provided reason.
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["reason"] == "drop_all"
