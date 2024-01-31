from sentry_sdk.envelope import Envelope, Item, PayloadRef
import msgpack
from .test_replay_events import generate_replay_sdk_event
import json


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

    replay_event = json.loads(bytes(payload["replay_event"]))

    assert replay_event["replay_id"] == "515539018c9b4260a6f999572f1661ee"

    assert bytes(payload["replay_recording"]) == replay_recording_bytes


# TODO: figure out behavior for this test
# def test_replay_combined_without_processing(
#     mini_sentry, relay_chain, replay_recordings_consumer
# ):
#     relay = relay_chain(min_relay_version="latest")
#     replay_recordings_consumer = replay_recordings_consumer(timeout=10)
#     replay_recording_bytes = b"{}\n[]"

#     mini_sentry.add_basic_project_config(
#         42,
#         extra={
#             "config": {
#                 "features": [
#                     "organizations:session-replay",
#                     "organizations:session-replay-combined-envelope-items",
#                 ]
#             }
#         },
#     )

#     replay_id = "515539018c9b4260a6f999572f1661ee"

#     replay_event = generate_replay_sdk_event(replay_id=replay_id)

#     envelope = Envelope(headers=[["event_id", replay_id]])
#     envelope.add_item(
#         Item(payload=PayloadRef(bytes=replay_recording_bytes), type="replay_recording")
#     )
#     envelope.add_item(Item(payload=PayloadRef(json=replay_event), type="replay_event"))

#     relay.send_envelope(42, envelope)

#     envelope = mini_sentry.captured_events.get(timeout=20)
#     assert len(envelope.items) == 1

#     replay_event = envelope.items[0]
#     assert replay_event.type == "replay_recording_not_chunked"
