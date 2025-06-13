from datetime import datetime, timedelta, timezone
import json
from unittest import mock

from .asserts import time_within_delta


def test_nel_converted_to_logs(
    mini_sentry,
    relay,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:ourlogs-ingestion",
    ]
    relay = relay(mini_sentry)
    relay.send_nel_event(project_id)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert [item.type for item in envelope.items] == ["log"]
    print(envelope)

    assert json.loads(envelope.items[0].payload.bytes) == {
        "items": [
            {
                "attributes": {
                    "todo.nel.elapsed_time": {"type": "string", "value": 37},
                    "todo.nel.error_type": {"type": "string", "value": "http.error"},
                    "todo.nel.phase": {"type": "string", "value": "application"},
                    "todo.nel.referrer": {
                        "type": "string",
                        "value": "https://example.com/nel/",
                    },
                    "todo.nel.sampling_fraction": {"type": "string", "value": 1.0},
                    "todo.nel.server_ip": {
                        "type": "string",
                        "value": "123.123.123.123",
                    },
                },
                "body": "application / http.error",
                "level": "info",
                # Time is corrected by the `age` specified in the NEL report
                "timestamp": time_within_delta(
                    datetime.now(tz=timezone.utc) - timedelta(milliseconds=1200000)
                ),
                "trace_id": mock.ANY,
            }
        ],
    }
    assert mini_sentry.captured_events.empty()
