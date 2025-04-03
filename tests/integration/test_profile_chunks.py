import uuid
from copy import deepcopy
from pathlib import Path

import pytest
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory
from .asserts import time_within_delta

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

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(TEST_CONFIG)

    # build a chain of relays
    for i in range(num_intermediate_relays):
        config = deepcopy(TEST_CONFIG)
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

    # No outcome is emitted in Relay since it's a successful ingestion.
    # However, we will emit the outcome for PROFILE_DURATION in sentry.
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

    upstream = relay_with_processing(TEST_CONFIG)

    envelope = Envelope()
    payload = {
        "chunk_id": "11111111111111111111111111111111",
        "platform": "thisisnotvalid",
    }
    envelope.add_item(Item(payload=PayloadRef(json=payload), type="profile_chunk"))

    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.PROFILE_CHUNK.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_platform_not_supported",
        },
    ]

    profiles_consumer.assert_empty()


@pytest.mark.parametrize("item_header_platform", [None, "cocoa"])
def test_profile_chunk_outcomes_rate_limited(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    item_header_platform,
):
    """
    Tests that Relay reports correct outcomes when profile chunks are rate limited.

    This test verifies that when a profile chunk hits a rate limit:
    1. The profile chunk is dropped and not forwarded to the profiles consumer
    2. A rate limited outcome is emitted with the correct category and reason code
    3. The rate limit is enforced at the project level
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    profiles_consumer = profiles_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    # Enable profiling feature flag
    project_config.setdefault("features", []).append(
        "organizations:continuous-profiling"
    )

    # Configure rate limiting quota that blocks all profile chunks
    project_config["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": ["profile_chunk_ui"],  # Target profile chunks specifically
            "limit": 0,  # Block all profile chunks
            "reasonCode": "profile_chunks_exceeded",
        }
    ]

    upstream = relay_with_processing(TEST_CONFIG)

    # Load a valid profile chunk from test fixtures
    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v2/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    # Create and send envelope containing the profile chunk
    envelope = Envelope()
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=profile),
            type="profile_chunk",
            headers={"platform": item_header_platform},
        )
    )
    upstream.send_envelope(project_id, envelope)

    # Verify the rate limited outcome was emitted with correct properties
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.PROFILE_CHUNK_UI.value,
            "timestamp": time_within_delta(),
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,  # RateLimited
            "project_id": 42,
            "quantity": 1,
            "reason": "profile_chunks_exceeded",
        },
    ]

    # Verify no profiles were forwarded to the consumer
    profiles_consumer.assert_empty()


@pytest.mark.parametrize(
    "platform, category",
    [
        ("cocoa", "profile_chunk_ui"),
        ("node", "profile_chunk"),
        (None, "profile_chunk"),  # Special case, currently this will forward
    ],
)
def test_profile_chunk_outcomes_rate_limited_fast(
    mini_sentry,
    relay,
    platform,
    category,
):
    """
    Tests that Relay reports correct outcomes when profile chunks are rate limited already in the
    fast-path, using the item header.

    The test is parameterized to also *not* send the necessary item header, in which case this currently
    asserts the chunk is let through. Once Relay's behaviour is changed to reject or profile chunks
    without the necessary headers or the profile type is defaulted this test needs to be adjusted accordingly.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append(
        "organizations:continuous-profiling"
    )

    project_config["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": [category],
            "limit": 0,
            "reasonCode": "profile_chunks_exceeded",
        }
    ]

    upstream = relay(mini_sentry)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v2/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    envelope = Envelope()
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=profile),
            type="profile_chunk",
            headers={"platform": platform},
        )
    )
    upstream.send_envelope(project_id, envelope)

    if platform is None:
        envelope = mini_sentry.captured_events.get(timeout=1)
        assert [item.type for item in envelope.items] == ["profile_chunk"]
    else:
        outcome = mini_sentry.get_client_report()
        assert outcome["rate_limited_events"] == [
            {"category": category, "quantity": 1, "reason": "profile_chunks_exceeded"}
        ]
        assert mini_sentry.captured_events.empty()
