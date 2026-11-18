import json
import uuid
import zlib

from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .asserts import time_within_delta
from .consts import Outcome
from .test_replay_events import generate_replay_sdk_event


def test_replay_recordings(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="latest")

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_id = "515539018c9b4260a6f999572f1661ee"

    envelope = Envelope(headers=[["event_id", replay_id]])
    envelope.add_item(
        Item(payload=PayloadRef(bytes=b"{}\n[]"), type="replay_recording")
    )
    envelope.add_item(
        Item(payload=PayloadRef(json=generate_replay_sdk_event()), type="replay_event")
    )

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    assert len(envelope.items) == 2
    assert {item.type for item in envelope.items} == {
        "replay_recording",
        "replay_event",
    }

    recording_item = next(
        item for item in envelope.items if item.type == "replay_recording"
    )
    replay_recording = recording_item.get_bytes()
    assert replay_recording.startswith(b"{}\n")  # The body is compressed


def test_nonchunked_replay_recordings_processing(
    mini_sentry,
    relay_with_processing,
    replay_recordings_consumer,
    outcomes_consumer,
):
    project_id = 42
    org_id = 0
    replay_id = "515539018c9b4260a6f999572f1661ee"

    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )
    relay = relay_with_processing()
    replay_recordings_consumer = replay_recordings_consumer()
    outcomes_consumer = outcomes_consumer()

    envelope = Envelope(
        headers=[
            [
                "event_id",
                replay_id,
            ],
            ["attachment_type", "replay_recording"],
        ]
    )
    payload = recording_payload(b"[]")
    envelope.add_item(Item(payload=PayloadRef(bytes=payload), type="replay_recording"))
    json_payload = generate_replay_sdk_event()
    envelope.add_item(Item(payload=PayloadRef(json=json_payload), type="replay_event"))

    relay.send_envelope(project_id, envelope)

    # Get the non-chunked replay-recording message from the kafka queue.
    replay_recording = replay_recordings_consumer.get_not_chunked_replay(timeout=10)
    assert replay_recording["replay_id"] == replay_id
    assert replay_recording["project_id"] == project_id
    assert replay_recording["key_id"] == 123
    assert replay_recording["org_id"] == org_id
    assert isinstance(replay_recording["received"], int)
    assert replay_recording["retention_days"] == 90
    assert replay_recording["payload"] == payload
    assert replay_recording["type"] == "replay_recording_not_chunked"
    assert replay_recording["relay_snuba_publish_disabled"]

    outcomes_consumer.assert_empty()


def test_replay_recording_max_uncompressed_size(
    mini_sentry,
    relay_with_processing,
    replay_recordings_consumer,
    outcomes_consumer,
):
    """
    Processing Relays reject recordings that decompress past `max_replay_uncompressed_size`
    whether or not recording scrubbing runs, and accept recordings exactly at the limit.
    """
    project_configs = {
        # Scrubbing disabled.
        42: {"features": ["organizations:session-replay"]},
        # Scrubbing enabled without a PII config, so the scrubber does not run.
        43: {
            "features": [
                "organizations:session-replay",
                "organizations:session-replay-recording-scrubbing",
            ],
            "piiConfig": None,
        },
        # Scrubbing enabled with the non-empty PII config from the basic project config.
        44: {
            "features": [
                "organizations:session-replay",
                "organizations:session-replay-recording-scrubbing",
            ],
        },
    }
    for project_id, config in project_configs.items():
        mini_sentry.add_basic_project_config(project_id, extra={"config": config})

    def recording_body(href_length):
        return json.dumps([{"type": 4, "data": {"href": "a" * href_length}}]).encode()

    at_limit = recording_body(1000)
    over_limit = recording_body(1001)
    assert len(over_limit) == len(at_limit) + 1

    relay = relay_with_processing(
        {"limits": {"max_replay_uncompressed_size": len(at_limit)}}
    )
    replay_recordings_consumer = replay_recordings_consumer()
    outcomes_consumer = outcomes_consumer()

    for project_id in project_configs:
        for body in (at_limit, over_limit):
            envelope = Envelope(headers=[["event_id", uuid.uuid4().hex]])
            envelope.add_item(
                Item(
                    payload=PayloadRef(bytes=recording_payload(body)),
                    type="replay_recording",
                )
            )
            relay.send_envelope(project_id, envelope)

    accepted = {}
    for _ in project_configs:
        replay_recording = replay_recordings_consumer.get_not_chunked_replay(timeout=10)
        header, compressed = replay_recording["payload"].split(b"\n", 1)
        assert header == b'{"segment_id": 0}'
        # Scrubbing recompresses the body, so compare the decompressed recording.
        accepted[replay_recording["project_id"]] = json.loads(
            zlib.decompress(compressed)
        )
    assert accepted == {
        project_id: json.loads(at_limit) for project_id in project_configs
    }
    replay_recordings_consumer.assert_empty()

    outcomes = outcomes_consumer.get_outcomes(n=len(project_configs))
    outcomes.sort(key=lambda outcome: outcome["project_id"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "project_id": project_id,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "too_large:replay_recording",
            "category": DataCategory.REPLAY,
            "quantity": 1,
        }
        for project_id in project_configs
    ]
    outcomes_consumer.assert_empty()


def recording_payload(bits: bytes):
    compressed_payload = zlib.compress(bits)
    return b'{"segment_id": 0}\n' + compressed_payload
