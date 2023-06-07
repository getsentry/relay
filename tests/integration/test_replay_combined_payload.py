# import pytest
# import zlib

from sentry_sdk.envelope import Envelope, Item, PayloadRef
import msgpack
from .test_replay_events import generate_replay_sdk_event
import json

# def test_replay_recordings(mini_sentry, relay_chain):
#     relay = relay_chain(min_relay_version="latest")

#     project_id = 42
#     mini_sentry.add_basic_project_config(
#         project_id, extra={"config": {"features": ["organizations:session-replay"]}}
#     )

#     replay_id = "515539018c9b4260a6f999572f1661ee"

#     replay_event = generate_replay_sdk_event(replay_id=replay_id)

#     envelope = Envelope(headers=[["event_id", replay_id]])
#     envelope.add_item(
#         Item(payload=PayloadRef(bytes=b"{}\n[]"), type="replay_recording")
#     )
#     envelope.add_item(Item(payload=PayloadRef(json=replay_event), type="replay_event"))

#     relay.send_envelope(project_id, envelope)

#     envelope = mini_sentry.captured_events.get(timeout=1)
#     assert len(envelope.items) == 1

#     replay_combined_item = envelope.items[0]
#     assert session_item.type == "replay_recording"

#     replay_recording = session_item.get_bytes()
#     assert replay_recording.startswith(b"{}\n")  # The body is compressed


def test_replay_combined_with_processing(
    mini_sentry, relay_with_processing, replay_recordings_consumer
):
    replay_recording_bytes = b"{}\n[]"
    relay = relay_with_processing()
    replay_recordings_consumer = replay_recordings_consumer(timeout=10)

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

    replay_id = "515539018c9b4260a6f999572f1661ee"

    replay_event = generate_replay_sdk_event(replay_id=replay_id)

    envelope = Envelope(headers=[["event_id", replay_id]])
    envelope.add_item(
        Item(payload=PayloadRef(bytes=replay_recording_bytes), type="replay_recording")
    )
    envelope.add_item(Item(payload=PayloadRef(json=replay_event), type="replay_event"))

    relay.send_envelope(42, envelope)

    combined_replay_message = replay_recordings_consumer.get_not_chunked_replay()
    assert combined_replay_message["type"] == "replay_recording_not_chunked"
    assert combined_replay_message["replay_id"] == "515539018c9b4260a6f999572f1661ee"
    assert combined_replay_message["version"] == 1

    payload = msgpack.unpackb(combined_replay_message["payload"])

    replay_event = json.loads(payload["replay_event"])
    assert replay_event["replay_id"] == "515539018c9b4260a6f999572f1661ee"

    assert payload["replay_recording"] == replay_recording_bytes
    # breakpoint()


# def test_replay_events_without_processing(mini_sentry, relay_chain):
#     relay = relay_chain(min_relay_version="latest")

#     project_id = 42
#     mini_sentry.add_basic_project_config(
#         project_id, extra={"config": {"features": ["organizations:session-replay"]}}
#     )

#     replay_item = generate_replay_sdk_event()

#     relay.send_replay_event(42, replay_item)

#     envelope = mini_sentry.captured_events.get(timeout=20)
#     assert len(envelope.items) == 1

#     replay_event = envelope.items[0]
#     assert replay_event.type == "replay_event"
