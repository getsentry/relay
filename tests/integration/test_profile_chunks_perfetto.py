import json
from pathlib import Path

from sentry_sdk.envelope import Envelope

from .asserts import matches, matches_any, time_within_delta

RELAY_ROOT = Path(__file__).parent.parent.parent


PERFETTO_ENVELOPE_FIXTURE = (
    RELAY_ROOT
    / "relay-profiling/tests/fixtures/android/perfetto/profile_chunk.envelope"
)


def _expected_profile_chunk(project_id):
    return {
        "type": "profile_chunk",
        "organization_id": 1,
        "project_id": project_id,
        "received": time_within_delta(),
        "retention_days": 90,
        "payload": matches_any(),
        "attachments": [
            {
                "name": "profile.perfetto",
                "content_type": "application/x-perfetto-trace",
                "stored_id": matches(bool),
            }
        ],
    }


def test_perfetto_profile_chunk_end_to_end(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    json_fixture_provider,
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
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).extend(
        [
            "organizations:continuous-profiling",
            "organizations:continuous-profiling-perfetto",
        ]
    )

    upstream = relay_with_processing()

    with open(PERFETTO_ENVELOPE_FIXTURE, "rb") as f:
        envelope = Envelope.deserialize_from(f)

    upstream.send_envelope(project_id, envelope)

    # Successful ingestion emits no outcomes from Relay (profile_duration is
    # emitted later in Sentry itself).
    outcomes_consumer.assert_empty()

    profile, headers = profiles_consumer.get_profile()
    assert headers == [("project_id", b"42")]

    assert profile == _expected_profile_chunk(project_id)
    assert json.loads(profile["payload"]) == json_fixture_provider(__file__).load(
        "profile_chunk", ".output"
    )


def test_perfetto_profile_chunk_objectstore_content_type(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    objectstore,
    json_fixture_provider,
):
    """
    Verifies that the raw Perfetto trace is uploaded to objectstore with its
    content type preserved on the stored object, so a subsequent GET returns
    the correct `Content-Type`.
    """
    profiles_consumer = profiles_consumer()
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).extend(
        [
            "organizations:continuous-profiling",
            "organizations:continuous-profiling-perfetto",
        ]
    )

    upstream = relay_with_processing()

    with open(PERFETTO_ENVELOPE_FIXTURE, "rb") as f:
        envelope = Envelope.deserialize_from(f)

    upstream.send_envelope(project_id, envelope)

    outcomes_consumer.assert_empty()

    profile, _ = profiles_consumer.get_profile()

    assert profile == _expected_profile_chunk(project_id)
    assert json.loads(profile["payload"]) == json_fixture_provider(__file__).load(
        "profile_chunk", ".output"
    )

    attachments = profile["attachments"]
    object_store_key = attachments[0]["stored_id"]

    stored = objectstore("profile_attachments", project_id).get(object_store_key)
    assert stored.metadata.content_type == "application/x-perfetto-trace"
    assert len(stored.payload.read()) == 97252
