import json
import time

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from datetime import datetime, timezone, time as dttime


def test_multi_write_redis_client(
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
                "redis": {"nodes": ["redis://127.0.0.1:6379", "redis://127.0.0.1:6380"]}
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
