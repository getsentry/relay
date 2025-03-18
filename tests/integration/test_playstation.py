import pytest
import os
import requests


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    return dmp_file


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


# Turns out these are not data_dog metrics but sentry_metrics hence the entire thing is not working
def test_playstation_with_feature_flag(
    mini_sentry, relay_with_processing, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["projects:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_processing()

    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    # Get these outcomes since the feature flag is not enabled:
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2
    assert outcomes[0]["reason"] == "no_event_payload"
    assert outcomes[1]["reason"] == "no_event_payload"
