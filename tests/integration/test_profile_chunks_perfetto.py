import json
from pathlib import Path

from sentry_sdk.envelope import Envelope

RELAY_ROOT = Path(__file__).parent.parent.parent

TEST_CONFIG = {
    "outcomes": {
        "emit_outcomes": True,
        "batch_size": 1,
        "batch_interval": 1,
        "aggregator": {
            "bucket_interval": 1,
            "flush_interval": 1,
        },
    },
    "aggregator": {
        "bucket_interval": 1,
        "initial_delay": 0,
    },
}

PERFETTO_ENVELOPE_FIXTURE = (
    RELAY_ROOT
    / "relay-profiling/tests/fixtures/android/perfetto/profile_chunk.envelope"
)


def test_perfetto_profile_chunk_end_to_end(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
):
    """
    Ingests a real Perfetto `profile_chunk` envelope end-to-end and verifies
    that Relay decodes the binary Perfetto trace into a Sample v2 profile
    that is forwarded to the profiles consumer.

    The fixture envelope was captured from the Android SDK and contains a
    single `profile_chunk` item whose payload is `[JSON metadata][perfetto
    binary]` concatenated, delimited by the `meta_length` item header.
    """
    profiles_consumer = profiles_consumer()
    outcomes_consumer = outcomes_consumer(timeout=2)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).extend(
        [
            "organizations:continuous-profiling",
            "organizations:continuous-profiling-perfetto",
        ]
    )

    upstream = relay_with_processing(TEST_CONFIG)

    with open(PERFETTO_ENVELOPE_FIXTURE, "rb") as f:
        envelope = Envelope.deserialize_from(f)

    upstream.send_envelope(project_id, envelope)

    # Successful ingestion emits no outcomes from Relay (profile_duration is
    # emitted later in Sentry itself).
    outcomes_consumer.assert_empty()

    profile, headers = profiles_consumer.get_profile()
    assert headers == [("project_id", b"42")]

    payload = json.loads(profile["payload"])

    assert {
        k: payload[k] for k in ("version", "platform", "chunk_id", "profiler_id")
    } == {
        "version": "2",
        "platform": "android",
        "chunk_id": "c3b09c0608844f558eaf6e65df6b9cdf",
        "profiler_id": "814b081c638b4ad982ae351547bfe499",
    }
    assert payload["client_sdk"]["name"] == "sentry.java.android"

    profile_data = payload["profile"]
    assert len(profile_data["samples"]) == 398
    assert len(profile_data["stacks"]) == 52
    assert len(profile_data["frames"]) == 358
    assert len(payload["debug_meta"]["images"]) == 17

    samples = profile_data["samples"]
    timestamps = [s["timestamp"] for s in samples]
    assert timestamps == sorted(timestamps)
    assert abs((timestamps[-1] - timestamps[0]) - 1.96) < 0.01

    num_stacks = len(profile_data["stacks"])
    num_frames = len(profile_data["frames"])
    for sample in samples:
        assert 0 <= sample["stack_id"] < num_stacks
        assert isinstance(sample["thread_id"], str)

    for stack in profile_data["stacks"]:
        for frame_id in stack:
            assert 0 <= frame_id < num_frames

    frames = profile_data["frames"]
    assert sum(1 for f in frames if f.get("function")) >= 350
    assert any(f.get("function", "").startswith("io.sentry.") for f in frames)

    sample_thread_ids = {s["thread_id"] for s in samples}
    assert len(sample_thread_ids) == 6
    thread_metadata = profile_data["thread_metadata"]
    assert any(meta.get("name") == "main" for meta in thread_metadata.values())
    for tid, meta in thread_metadata.items():
        assert isinstance(tid, str)
        assert "name" in meta and isinstance(meta["name"], str)
