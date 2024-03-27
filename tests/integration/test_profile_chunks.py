from copy import deepcopy
from pathlib import Path

import pytest
import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


RELAY_ROOT = Path(__file__).parent.parent.parent


@pytest.mark.parametrize("num_intermediate_relays", [0, 1, 2])
def test_profile_chunk_outcomes(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    num_intermediate_relays,
):
    """
    Tests that Relay reports correct outcomes for profile chunks.

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by the first relay
    are properly forwarded up to sentry.
    """
    outcomes_consumer = outcomes_consumer(timeout=5)
    profiles_consumer = profiles_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append(
        "organizations:continuous-profiling"
    )
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "processing-relay",
        },
        "aggregator": {"bucket_interval": 1, "initial_delay": 0, "debounce_delay": 0},
    }

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(config)

    # build a chain of relays
    for i in range(num_intermediate_relays):
        config = deepcopy(config)
        if i == 0:
            # Emulate a PoP Relay
            config["outcomes"]["source"] = "pop-relay"
        if i == 1:
            # Emulate a customer Relay
            config["outcomes"]["source"] = "external-relay"
            config["outcomes"]["emit_outcomes"] = "as_client_reports"
        upstream = relay(upstream, config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v2/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    envelope = Envelope()
    envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile_chunk"))

    upstream.send_envelope(project_id, envelope)

    # no outcome since it's a successful ingestion
    outcomes_consumer.assert_empty()
    assert profiles_consumer.get_profile()


def test_profile_chunk_outcomes_invalid(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
):
    """
    Tests that Relay reports correct outcomes for invalid profiles as `ProfileChunk`.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    profiles_consumer = profiles_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append(
        "organizations:continuous-profiling"
    )

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "pop-relay",
        },
        "aggregator": {"bucket_interval": 1, "initial_delay": 0, "debounce_delay": 0},
    }

    upstream = relay_with_processing(config)

    envelope = Envelope()
    envelope.add_item(Item(payload=PayloadRef(bytes=b""), type="profile_chunk"))

    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_outcomes = [
        {
            "category": 18,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_invalid_json",
            "source": "pop-relay",
        },
    ]
    for outcome in outcomes:
        outcome.pop("timestamp")

    assert outcomes == expected_outcomes, outcomes
    profiles_consumer.assert_empty()


def test_profile_chunk_outcomes_rate_limited(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
):
    outcomes_consumer = outcomes_consumer(timeout=2)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": ["profile_chunk"],
            "limit": 0,
            "reasonCode": "profile_chunks_exceeded",
        }
    ]

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 0,
            },
        },
        "aggregator": {"bucket_interval": 1, "initial_delay": 0, "debounce_delay": 0},
    }

    upstream = relay_with_processing(config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v2/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    envelope = Envelope()
    envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile_chunk"))
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_outcomes = [
        {
            "category": 18,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,  # RateLimited
            "project_id": 42,
            "quantity": 1,
            "reason": "profile_chunks_exceeded",
        },
    ]
    for outcome in outcomes:
        outcome.pop("timestamp")
        outcome.pop("event_id", None)

    assert outcomes == expected_outcomes, outcomes
