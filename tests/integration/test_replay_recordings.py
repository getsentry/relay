import zlib

from sentry_sdk.envelope import Envelope, Item, PayloadRef
import pytest

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

    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.get_captured_envelope()
    assert len(envelope.items) == 1

    session_item = envelope.items[0]
    assert session_item.type == "replay_recording"

    replay_recording = session_item.get_bytes()
    assert replay_recording.startswith(b"{}\n")  # The body is compressed


@pytest.mark.parametrize("value,expected", [(1.0, True), (None, False), (0.0, False)])
def test_nonchunked_replay_recordings_processing(
    mini_sentry,
    relay_with_processing,
    replay_events_consumer,
    replay_recordings_consumer,
    outcomes_consumer,
    value,
    expected,
):
    project_id = 42
    org_id = 0
    replay_id = "515539018c9b4260a6f999572f1661ee"

    if value is not None:
        mini_sentry.global_config["options"][
            "replay.relay-snuba-publishing-disabled.sample-rate"
        ] = value
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )
    relay = relay_with_processing()
    replay_events_consumer = replay_events_consumer(timeout=10)
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
    assert replay_recording["relay_snuba_publish_disabled"] is expected

    if expected is True:
        # Nothing produced.
        with pytest.raises(AssertionError):
            replay_events_consumer.get_replay_event()
    else:
        assert replay_events_consumer.get_replay_event() is not None

    outcomes_consumer.assert_empty()


def recording_payload(bits: bytes):
    compressed_payload = zlib.compress(bits)
    return b'{"segment_id": 0}\n' + compressed_payload
