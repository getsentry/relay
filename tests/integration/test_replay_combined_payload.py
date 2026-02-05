from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .test_replay_recordings import recording_payload
from .test_replay_events import generate_replay_sdk_event

import json
import pytest


def test_replay_combined_with_processing(
    mini_sentry,
    relay_with_processing,
    replay_recordings_consumer,
    replay_events_consumer,
):
    project_id = 42
    replay_id = "515539018c9b4260a6f999572f1661ee"
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(
        project_id,
        extra={"config": {"features": ["organizations:session-replay"]}},
    )
    replay_recordings_consumer = replay_recordings_consumer()
    replay_events_consumer = replay_events_consumer(timeout=10)

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

    replay_event = generate_replay_sdk_event(replay_id=replay_id)
    envelope.add_item(Item(payload=PayloadRef(json=replay_event), type="replay_event"))

    relay.send_envelope(project_id, envelope)

    combined_replay_message = replay_recordings_consumer.get_not_chunked_replay(
        timeout=10
    )

    assert combined_replay_message["type"] == "replay_recording_not_chunked"
    assert combined_replay_message["replay_id"] == replay_id

    assert combined_replay_message["payload"] == payload

    replay_event = json.loads(combined_replay_message["replay_event"])

    assert replay_event["replay_id"] == replay_id

    replay_event, replay_event_message = replay_events_consumer.get_replay_event()
    assert replay_event["type"] == "replay_event"
    assert replay_event["replay_id"] == replay_id
    assert replay_event_message["retention_days"] == 90


@pytest.mark.parametrize(
    "send_event,send_recording",
    [(True, False), (False, True)],
    ids=["standalone_event", "standalone_recording"],
)
@pytest.mark.parametrize(
    "use_new_processing",
    [False, True],
    ids=["old_processing_path", "new_processing_path"],
)
def test_standalone_replay_item_from_faulty_sdk(
    mini_sentry,
    relay_with_processing,
    replay_events_consumer,
    replay_recordings_consumer,
    send_event,
    send_recording,
    use_new_processing,
):
    project_id = 42
    replay_id = "515539018c9b4260a6f999572f1661ee"
    relay = relay_with_processing()

    features = ["organizations:session-replay"]
    if use_new_processing:
        features.append("organizations:new-replay-processing")

    mini_sentry.add_basic_project_config(
        project_id,
        extra={"config": {"features": features}},
    )

    replay_events_consumer = replay_events_consumer(timeout=10)
    replay_recordings_consumer = replay_recordings_consumer()

    envelope = Envelope(headers=[["event_id", replay_id]])
    payload = recording_payload(b"[]")

    if send_event:
        replay_event = generate_replay_sdk_event(replay_id=replay_id)
        envelope.add_item(
            Item(payload=PayloadRef(json=replay_event), type="replay_event")
        )

    if send_recording:
        envelope.add_item(
            Item(payload=PayloadRef(bytes=payload), type="replay_recording")
        )

    relay.send_envelope(project_id, envelope)

    if send_event:
        replay_event, replay_event_message = replay_events_consumer.get_replay_event()
        assert replay_event["type"] == "replay_event"
        assert replay_event["replay_id"] == replay_id
        assert replay_event_message["retention_days"] == 90

    if send_recording:
        replay_recording = replay_recordings_consumer.get_not_chunked_replay(timeout=10)
        assert replay_recording["replay_id"] == replay_id
        assert replay_recording["payload"] == payload
        assert replay_recording["replay_event"] is None
