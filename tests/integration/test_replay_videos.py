from .test_replay_events import generate_replay_sdk_event
from .test_replay_recordings import recording_payload
from sentry_sdk.envelope import Envelope, Item, PayloadRef

import msgpack
import json


def test_replay_recording_with_video(
    mini_sentry,
    relay_with_processing,
    replay_recordings_consumer,
    outcomes_consumer,
):
    project_id = 42
    org_id = 0
    replay_id = "515539018c9b4260a6f999572f1661ee"
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )
    replay_recordings_consumer = replay_recordings_consumer()
    outcomes_consumer = outcomes_consumer()

    _recording_payload = recording_payload(b"[]")
    payload = msgpack.packb(
        {
            "replay_event": json.dumps(generate_replay_sdk_event(replay_id)).encode(),
            "replay_recording": _recording_payload,
            "replay_video": b"hello, world!",
        }
    )

    envelope = Envelope(
        headers=[
            [
                "event_id",
                replay_id,
            ],
            ["attachment_type", "replay_video"],
        ]
    )
    envelope.add_item(Item(payload=PayloadRef(bytes=payload), type="replay_video"))

    relay.send_envelope(project_id, envelope)

    # Get the non-chunked replay-recording message from the kafka queue.
    replay_recording = replay_recordings_consumer.get_not_chunked_replay(timeout=10)

    # Assert the recording payload appears normally.
    assert replay_recording["replay_id"] == replay_id
    assert replay_recording["project_id"] == project_id
    assert replay_recording["key_id"] == 123
    assert replay_recording["org_id"] == org_id
    assert type(replay_recording["received"]) == int
    assert replay_recording["retention_days"] == 90
    assert replay_recording["payload"] == _recording_payload
    assert replay_recording["type"] == "replay_recording_not_chunked"
    assert replay_recording["replay_event"] is not None

    # Assert the replay-video bytes were published to the consumer.
    assert replay_recording["replay_video"] == b"hello, world!"

    # Assert the replay-event bytes were published to the consumer.
    replay_event = json.loads(replay_recording["replay_event"])
    assert replay_event["type"] == "replay_event"
    assert replay_event["replay_id"] == "515539018c9b4260a6f999572f1661ee"

    outcomes_consumer.assert_empty()
