from sentry_sdk.envelope import Envelope, Item, PayloadRef
from .test_replay_events import generate_replay_sdk_event
import json


def test_replay_combined_with_processing(
    mini_sentry, relay_with_processing, replay_recordings_consumer
):
    replay_recording_bytes = b"{}\n[]"
    relay = relay_with_processing()
    replay_recordings_consumer = replay_recordings_consumer()

    mini_sentry.add_basic_project_config(
        42,
        extra={
            "config": {
                "features": [
                    "organizations:session-replay",
                    "organizations:session-replay-combined-envelope-items",
                ]
            }
        },
    )

    replay_id = "d2132d31b39445f1938d7e21b6bf0ec4"

    replay_event = generate_replay_sdk_event()

    envelope = Envelope(headers=[["event_id", replay_id]])
    envelope.add_item(
        Item(payload=PayloadRef(bytes=replay_recording_bytes), type="replay_recording")
    )
    envelope.add_item(Item(payload=PayloadRef(json=replay_event), type="replay_event"))

    relay.send_envelope(42, envelope)

    # the not-combined message will be produced first
    replay_recordings_consumer.get_not_chunked_replay()
    combined_replay_message = replay_recordings_consumer.get_not_chunked_replay()

    assert combined_replay_message["type"] == "replay_recording_not_chunked"
    assert combined_replay_message["replay_id"] == "d2132d31b39445f1938d7e21b6bf0ec4"

    assert combined_replay_message["payload"] == replay_recording_bytes

    replay_event = json.loads(combined_replay_message["replay_event"])

    assert replay_event["replay_id"] == "d2132d31b39445f1938d7e21b6bf0ec4"
