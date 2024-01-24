import json
import time

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from datetime import datetime, timezone, time as dttime


def assert_location(expected, actual):
    # Compare without the 'type' which is not persisted into storage.
    assert {k: expected[k] for k in expected.keys() - {"type"}} == actual


def test_metric_meta(mini_sentry, redis_client, relay_with_processing):
    project_id = 42
    now = datetime.now(tz=timezone.utc)
    start_of_day = datetime.combine(now, dttime.min, tzinfo=timezone.utc)

    # Key magic number is the fnv32 hash from the MRI.
    redis_key = "mm:l:{1}:42:2718098263:" + str(int(start_of_day.timestamp()))
    # Clear just so there is no overlap with previous tests.
    redis_client.delete(redis_key)

    relay = relay_with_processing()

    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).append("organizations:metric-meta")

    location1 = {
        "type": "location",
        "function": "_scan_for_suspect_projects",
        "module": "sentry.tasks.low_priority_symbolication",
        "filename": "sentry/tasks/low_priority_symbolication.py",
        "abs_path": "/usr/src/sentry/src/sentry/tasks/low_priority_symbolication.py",
        "lineno": 45,
    }
    location2 = {
        "type": "location",
        "function": "_scan_for_suspect_projects_the_second",
        "module": "sentry.tasks.low_priority_symbolication",
        "filename": "sentry/tasks/low_priority_symbolication.py",
        "abs_path": "/usr/src/sentry/src/sentry/tasks/low_priority_symbolication.py",
        "lineno": 120,
    }
    location3 = {
        "type": "location",
        "function": "_scan_for_suspect_projects_the_second",
        "module": "sentry.tasks.low_priority_symbolication",
        "filename": "sentry/tasks/low_priority_symbolication.py",
        "abs_path": "/usr/src/sentry/src/sentry/tasks/low_priority_symbolication.py",
        "lineno": 123,
    }

    envelope = Envelope()
    payload = {
        "timestamp": now.isoformat(),
        "mapping": {
            "d:custom/sentry.process_profile.track_outcome@second": [
                location1,
                location2,
            ]
        },
    }
    envelope.add_item(Item(PayloadRef(json=payload), type="metric_meta"))
    relay.send_envelope(project_id, envelope)

    time.sleep(1)

    stored = sorted(
        (json.loads(v) for v in redis_client.smembers(redis_key)),
        key=lambda x: x["lineno"],
    )
    assert len(stored) == 2
    assert_location(location1, stored[0])
    assert_location(location2, stored[1])

    # Resend one location and add a new location.
    envelope = Envelope()
    payload = {
        "timestamp": now.isoformat(),
        "mapping": {
            "d:custom/sentry.process_profile.track_outcome@second": [
                location1,
                location3,
            ]
        },
    }
    envelope.add_item(Item(PayloadRef(json=payload), type="metric_meta"))
    relay.send_envelope(project_id, envelope)

    time.sleep(1)

    stored = sorted(
        (json.loads(v) for v in redis_client.smembers(redis_key)),
        key=lambda x: x["lineno"],
    )
    assert len(stored) == 3
    assert_location(location1, stored[0])
    assert_location(location2, stored[1])
    assert_location(location3, stored[2])
