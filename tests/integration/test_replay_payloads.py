import pytest
import time
import uuid

from requests.exceptions import HTTPError


def test_payload(
    mini_sentry, relay_with_processing, replay_payloads_consumer, outcomes_consumer
):
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"

    relay = relay_with_processing()
    mini_sentry.add_full_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )
    replay_payloads_consumer = replay_payloads_consumer()
    outcomes_consumer = outcomes_consumer()

    replay_payloads = [
        ("sentry_replay_payload", "sentry_replay_payload", b"test"),
    ]
    relay.send_attachments(project_id, event_id, replay_payloads)

    replay_payload_contents = {}
    replay_payload_ids = []
    replay_payload_num_chunks = {}

    while set(replay_payload_contents.values()) != {b"test"}:
        chunk, v = replay_payloads_consumer.get_replay_chunk()
        replay_payload_contents[v["id"]] = (
            replay_payload_contents.get(v["id"], b"") + chunk
        )
        if v["id"] not in replay_payload_ids:
            replay_payload_ids.append(v["id"])
        num_chunks = 1 + replay_payload_num_chunks.get(v["id"], 0)
        assert v["chunk_index"] == num_chunks - 1
        replay_payload_num_chunks[v["id"]] = num_chunks

    id1 = replay_payload_ids[0]

    assert replay_payload_contents[id1] == b"test"

    replay_payload = replay_payloads_consumer.get_individual_replay()

    assert replay_payload == {
        "type": "replay_payload",
        "attachment": {
            "attachment_type": "event.attachment",
            "chunks": replay_payload_num_chunks[id1],
            "content_type": "application/octet-stream",
            "id": id1,
            "name": "sentry_replay_payload",
            "size": len(replay_payload_contents[id1]),
            "rate_limited": False,
        },
        "event_id": event_id,
        "project_id": project_id,
    }

    outcomes_consumer.assert_empty()
