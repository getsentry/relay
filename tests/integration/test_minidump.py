import pytest


def _get_item_file_name(item):
    if item is not None and getattr(item, "headers"):
        headers = item.headers
        return headers.get("filename")
    return None


def _get_item_by_file_name(items, filename):
    filtered_items = [item for item in items if _get_item_file_name(item) == filename]
    if len(filtered_items) >= 1:
        return filtered_items[0]
    return None


def test_minidump_attachments_are_added_to_the_envelope(mini_sentry, relay):
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    relay.send_minidump(
        project_id=proj_id,
        params=(("sentry[a][b]", "val1"), ("sentry[x][y]", "val2"),),
        files=(
            ("minidump", "minidump.txt", "minidump content"),
            ("attachment1", "attach1.txt", "attachment 1 content"),
        ),
    )
    items = mini_sentry.captured_events.get(timeout=1).items

    mini_dump = _get_item_by_file_name(items, "minidump.txt")
    assert mini_dump is not None
    assert mini_dump.headers.get("name") == "minidump"
    payload = mini_dump.payload.get_bytes()
    assert payload is not None
    assert payload.decode("utf-8") == "minidump content"
    attachment = _get_item_by_file_name(items, "attach1.txt")
    assert attachment is not None
    assert attachment.headers.get("name") == "attachment1"
