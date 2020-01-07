import os
import pytest


def _load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    return dmp_file


@pytest.mark.parametrize("dump_file_name", ["unreal_crash", "unreal_crash_apple"])
def test_unreal_crash(mini_sentry, relay, dump_file_name):
    project_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[project_id] = mini_sentry.full_project_config()
    unreal_content = _load_dump_file(dump_file_name)

    response = relay.send_unreal_request(project_id, unreal_content)

    event_id = response.text.replace("-", "")
    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    assert event_id == envelope.headers.get("event_id")
    items = envelope.items

    assert len(items) == 1
    unreal_item = items[0]
    assert unreal_item.headers
    assert unreal_item.headers.get("type") == "unreal_report"
    assert unreal_item.headers.get("content_type") == "application/octet-stream"
    assert unreal_item.payload is not None
