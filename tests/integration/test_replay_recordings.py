import pytest
import time
import uuid

from requests.exceptions import HTTPError


def test_payload(
    mini_sentry, relay_with_processing, replay_recordings_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    relay = relay_with_processing()
    mini_sentry.add_full_project_config(project_id)
    replay_recordings_consumer = replay_recordings_consumer()
    outcomes_consumer = outcomes_consumer()

    replay_recordings = [
        ("sentry_replay_recording", "sentry_replay_recording", b"test"),
    ]
    relay.send_attachments(project_id, event_id, replay_recordings)

    replay_recording_contents = {}
    replay_recording_ids = []
    replay_recording_num_chunks = {}

    while set(replay_recording_contents.values()) != {b"test"}:
        chunk, v = replay_recordings_consumer.get_replay_chunk()
        replay_recording_contents[v["id"]] = (
            replay_recording_contents.get(v["id"], b"") + chunk
        )
        if v["id"] not in replay_recording_ids:
            replay_recording_ids.append(v["id"])
        num_chunks = 1 + replay_recording_num_chunks.get(v["id"], 0)
        assert v["chunk_index"] == num_chunks - 1
        replay_recording_num_chunks[v["id"]] = num_chunks

    id1 = replay_recording_ids[0]

    assert replay_recording_contents[id1] == b"test"

    replay_recording = replay_recordings_consumer.get_individual_replay()

    assert replay_recording == {
        "type": "replay_recording",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": replay_recording_num_chunks[id1],
            "content_type": "application/octet-stream",
            "id": id1,
            "name": "sentry_replay_recording",
            "size": len(replay_recording_contents[id1]),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()
