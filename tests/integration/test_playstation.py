import pytest
import os
import requests

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


# Turns out these are not data_dog metrics but sentry_metrics hence the entire thing is not working
def test_playstation_no_feature_flag(
    mini_sentry, relay_with_processing, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_processing()

    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    # Get these outcomes since the feature flag is not enabled:
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2
    assert outcomes[0]["reason"] == "feature_disabled"
    assert outcomes[1]["reason"] == "feature_disabled"


def test_playstation_wrong_file(mini_sentry, relay_with_processing, outcomes_consumer):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("unreal_crash")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_processing()

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)

    response = exc_info.value.response
    assert response.status_code == 400, "Expected a 400 status code"
    assert response.json()["detail"] == "invalid prosperodump"


def test_playstation_too_large(mini_sentry, relay_with_processing, outcomes_consumer):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_processing(
        {
            "limits": {
                "max_attachments_size": len(playstation_dump) - 1,
            }
        }
    )

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)

    response = exc_info.value.response
    assert response.status_code == 400, "Expected a 400 status code"


@pytest.mark.parametrize("num_intermediate_relays", [0, 1, 2])
def test_playstation_with_feature_flag(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    num_intermediate_relays,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["projects:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing()
    # Build chain of relays
    for _ in range(num_intermediate_relays):
        upstream = relay(upstream)

    response = upstream.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2
    assert outcomes[0]["reason"] == "no_event_payload"
    assert outcomes[1]["reason"] == "no_event_payload"


def test_playstation_attachment(
    mini_sentry, relay_with_processing, outcomes_consumer, attachments_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["projects:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_with_processing()

    bogus_error = {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "type": "error",
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
    }
    envelope = Envelope()
    envelope.add_event(bogus_error)

    # Add the PlayStation dump as an attachment
    envelope.add_item(
        Item(
            type="attachment",
            payload=PayloadRef(bytes=playstation_dump),
            headers={
                "attachment_type": "playstation.prosperodump",
                "filename": "playstation.prosperodmp",
                "content_type": "application/octet-stream",
            },
        )
    )
    relay.send_envelope(PROJECT_ID, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 0
    # TODO: Add stronger assert later to check we make it into the expand function and add test
    # to check that we don't make it into if the feature is not enabled.
