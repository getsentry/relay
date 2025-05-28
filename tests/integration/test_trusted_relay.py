import json
import os
from time import sleep

import pytest
from requests import HTTPError


def test_trusted_relay_chain(mini_sentry, relay, relay_credentials):
    """
    Tests the happy path where we have a static relay that is registered in the
    managed relay and sends the proper signature using the correct key pair.
    """
    project_id = 42
    credentials = relay_credentials()
    static_relays = {
        credentials["id"]: {
            "public_key": credentials["public_key"],
            "internal": False,
        },
    }

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay = relay(mini_sentry, static_relays=static_relays)

    relay = relay(managed_relay, credentials=credentials)

    relay.send_event(project_id, {"message": "trusted event"})
    sleep(1)
    relay.send_event(project_id, {"message": "trusted event"})

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["logentry"]["formatted"] == "trusted event"


def test_missing_version(mini_sentry, relay, relay_credentials):
    """
    Tests a request with the correct signature but no version is provided.
    """
    project_id = 42
    credentials = relay_credentials()

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True
    config["config"]["trustedRelays"] = ["Br9f5pRIXDJ6J8_RDyxy-T-JKCpbil2w7FKfc2kigH4"]

    relay = relay(mini_sentry)

    headers = {
        "x-sentry-relay-signature": "uqFY5JuNcRvi1vUDv2A2xRjKH-U-jchmW61owNBA8QaZ5Cf9A2HQclN6bSDXq-8Cj72GEysHA44reOgWjix2AA.eyJ0IjoiMjAyNS0wNS0yOFQwODo0Nzo0Ny45MzcwNjBaIn0",
        "x-sentry-relay-id": credentials["id"],
        "x-sentry-signature-headers": "x-sentry-relay-signature-datetime",
        "x-sentry-relay-signature-datetime": "2025-05-28 08:47:47.937053 UTC",
    }

    relay.send_event(project_id, {"message": "trusted event"}, headers=headers)
    sleep(1)
    relay.send_event(project_id, {"message": "trusted event"}, headers=headers)

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["logentry"]["formatted"] == "trusted event"


def test_internal_relays(mini_sentry, relay, relay_credentials):
    """
    Tests that even though the received signature is invalid, it will not check
    it because the relay-id we submit is considered internal.
    """
    project_id = 42
    credentials = relay_credentials()
    static_relays = {
        credentials["id"]: {
            "public_key": credentials["public_key"],
            "internal": True,
        },
    }

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    # managed_relay = relay(mini_sentry, static_relays=static_relays)
    managed_relay = relay(mini_sentry, static_relays=static_relays)

    headers = {
        "x-sentry-relay-signature": "this-is-a-cool-signature",
        "x-sentry-relay-signature-version": "v1",
        "x-sentry-relay-id": credentials["id"],
        "x-sentry-signature-headers": "x-sentry-relay-signature-datetime",
        "x-sentry-relay-signature-datetime": "202505211045",
    }

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )

    sleep(1)

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["logentry"]["formatted"] == "trusted event"


def test_invalid_signature(
    mini_sentry,
    relay_with_processing,
    relay_credentials,
    events_consumer,
    outcomes_consumer,
):
    """
    Tests that a signature is rejected if the signature is invalid and the relay is not configured
    as an internal relay.
    """
    project_id = 42
    credentials = relay_credentials()
    relay = relay_with_processing(credentials=credentials)
    outcomes_consumer = outcomes_consumer(timeout=1)
    events_consumer = events_consumer(timeout=1)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    headers = {
        "x-sentry-relay-signature": "this-is-a-cool-signature",
        "x-sentry-relay-signature-version": "v1",
        "x-sentry-relay-id": credentials["id"],
        "x-sentry-signature-headers": "x-sentry-relay-signature-datetime",
        "x-sentry-relay-signature-datetime": "202505211045",
    }

    relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )

    # Wait a bit for the project config fetch
    sleep(1)

    with pytest.raises(HTTPError) as error:
        relay.send_event(
            project_id,
            {"message": "trusted event"},
            headers=headers,
        )

    assert error.value.response.status_code == 403

    events_consumer.assert_empty()
    outcomes = outcomes_consumer.get_outcomes(timeout=1)
    for outcome in outcomes:
        assert outcome["reason"] == "invalid_signature"


def test_not_trusted_relay(
    mini_sentry,
    relay_with_processing,
    relay_credentials,
    events_consumer,
    outcomes_consumer,
):
    """
    Tests that events are rejected if we directly send an event to relay
    without any signature.
    """
    project_id = 42
    credentials = relay_credentials()
    relay = relay_with_processing(credentials=credentials)

    events_consumer = events_consumer(timeout=1)
    outcomes_consumer = outcomes_consumer(timeout=1)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    relay.send_event(project_id)
    sleep(1)
    with pytest.raises(HTTPError) as error:
        relay.send_event(project_id)
    assert error.value.response.status_code == 403

    events_consumer.assert_empty()
    outcomes = outcomes_consumer.get_outcomes(timeout=1)
    for outcome in outcomes:
        assert outcome["reason"] == "invalid_signature"


def test_reject_without_signature(
    mini_sentry, relay_with_processing, events_consumer, outcomes_consumer
):
    """
    Simple case where a client sends an event without a signature.
    """
    project_id = 42
    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    outcomes_consumer = outcomes_consumer(timeout=1)
    events_consumer = events_consumer(timeout=1)

    relay.send_event(project_id)
    sleep(1)
    with pytest.raises(HTTPError) as error:
        relay.send_event(project_id)
    assert error.value.response.status_code == 403

    events_consumer.assert_empty()
    outcomes = outcomes_consumer.get_outcomes(timeout=1)
    for outcome in outcomes:
        assert outcome["reason"] == "invalid_signature"


def test_static_relay(mini_sentry, relay, relay_with_processing, outcomes_consumer):
    """
    A static relay without credentials will automatically fail the signature check.
    """
    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    processing_relay = relay_with_processing()

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join(f"{project_id}.json").write(json.dumps(config))

    relay_options = {"relay": {"mode": "static"}}
    relay = relay(
        processing_relay, options=relay_options, prepare=configure_static_project
    )

    # Fetches project config
    relay.send_event(project_id)
    with pytest.raises(HTTPError) as excinfo:
        relay.send_event(project_id)

    # rejected with 403 because project config was fetched
    assert excinfo.value.response.status_code == 403


def test_drop_envelope(
    mini_sentry, relay_with_processing, events_consumer, outcomes_consumer
):
    """
    Tests that an envelope is buffered because there is no project config and drops it once it fetches the config
    """
    outcomes_consumer = outcomes_consumer(timeout=1)
    events_consumer = events_consumer(timeout=1)

    project_id = 42
    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    relay.send_event(project_id)

    # at this point we have no project config so it's buffered
    outcomes_consumer.assert_empty()

    # after sending another event there is a projectconfig so it will get
    # rejected in the fast path
    with pytest.raises(HTTPError) as excinfo:
        relay.send_event(project_id)

    assert excinfo.value.response.status_code == 403

    # both events are rejected
    outcomes = outcomes_consumer.get_outcomes(timeout=1)
    assert len(outcomes) == 2
    for outcome in outcomes:
        assert outcome["reason"] == "invalid_signature"

    # make sure that no event got through
    events_consumer.assert_empty()
