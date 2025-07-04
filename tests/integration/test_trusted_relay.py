import json
import os
from copy import deepcopy

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
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay = relay(mini_sentry, static_relays=static_relays)

    relay = relay(managed_relay, credentials=credentials)

    relay.send_event(project_id, {"message": "trusted event"})
    relay.send_event(project_id, {"message": "trusted event"})

    for _ in range(2):
        envelope = mini_sentry.captured_events.get(timeout=1)
        event = envelope.get_event()
        assert event["logentry"]["formatted"] == "trusted event"


def test_send_directly(mini_sentry, relay, relay_credentials):
    """
    Tests that sending through the configured static relay works and is accepted but sending
    directly to the managed relay and circumventing the static relay will be rejected.
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
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay = relay(mini_sentry, static_relays=static_relays)

    relay = relay(managed_relay, credentials=credentials)

    # sending to the static relay works
    relay.send_event(project_id, {"message": "trusted event"})

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["logentry"]["formatted"] == "trusted event"

    # sending directly to the managed relay will be rejected
    with pytest.raises(HTTPError, match="403 Client Error"):
        managed_relay.send_event(project_id, {"message": "trusted event"})

    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "missing_signature", "category": "error", "quantity": 1}
    ]


def test_expired_signature(mini_sentry, relay):
    """
    Tests a direct request with a signature that is generated by the added trusted relay public key.
    """
    project_id = 42

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = ["EpWXCzYBxsX3jIp9P5YpVffwW3AIJJbB7BtWeaWM7Mk"]

    relay = relay(mini_sentry)

    # Signature created around 2025-06-04 15:21
    headers = {
        "x-sentry-relay-signature": "Jho91xVt8SEc_yvwKaPtIOCeCr-6zdnrIFa-KVfKoKcpAXInVe_QGE5JGEoZxAa4cP9Imbl5vb8nyNQ54vRvBQ.eyJ0IjoiMjAyNS0wNi0wNFQxMzoyMToyNi41NzIxNzVaIn0",
    }

    relay.send_event(project_id, {"message": "expired signature"}, headers=headers)

    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "invalid_signature", "category": "error", "quantity": 1}
    ]

    with pytest.raises(HTTPError, match="403 Client Error"):
        relay.send_event(
            project_id,
            {"message": "expired signature"},
            headers=headers,
        )


def test_internal_relay(mini_sentry, relay, relay_credentials):
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
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay = relay(mini_sentry, static_relays=static_relays)

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers={"x-sentry-relay-id": credentials["id"]},
    )

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers={"x-sentry-relay-id": credentials["id"]},
    )

    for _ in range(2):
        envelope = mini_sentry.captured_events.get(timeout=1)
        event = envelope.get_event()
        assert event["logentry"]["formatted"] == "trusted event"


def test_internal_relays_invalid_signature(mini_sentry, relay, relay_credentials):
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
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay = relay(mini_sentry, static_relays=static_relays)

    headers = {
        "x-sentry-relay-signature": "this-is-a-cool-signature",
        "x-sentry-relay-id": credentials["id"],
    }

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )

    managed_relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )
    for _ in range(2):
        envelope = mini_sentry.captured_events.get(timeout=1)
        event = envelope.get_event()
        assert event["logentry"]["formatted"] == "trusted event"


def test_invalid_signature(mini_sentry, relay, relay_credentials):
    """
    Tests that a signature is rejected if the signature is invalid and the relay is not configured
    as an internal relay.
    """
    project_id = 42
    credentials = relay_credentials()
    relay = relay(mini_sentry, credentials=credentials)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    headers = {
        "x-sentry-relay-signature": "this-is-a-cool-signature",
        "x-sentry-relay-id": credentials["id"],
    }

    relay.send_event(
        project_id,
        {"message": "trusted event"},
        headers=headers,
    )

    # Wait a bit for the project config fetch
    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "invalid_signature", "category": "error", "quantity": 1}
    ]

    with pytest.raises(HTTPError, match="403 Client Error"):
        relay.send_event(
            project_id,
            {"message": "trusted event"},
            headers=headers,
        )


def test_not_trusted_relay(mini_sentry, relay, relay_credentials):
    """
    Tests that events are rejected if we directly send an event to relay
    without any signature.
    """
    project_id = 42
    credentials = relay_credentials()
    relay = relay(mini_sentry, credentials=credentials)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"

    relay.send_event(project_id)

    # once the project config is fetched we can check the outcome
    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "missing_signature", "category": "error", "quantity": 1}
    ]

    # will reject in the fast path because project config is fetched at this point
    with pytest.raises(HTTPError, match="403 Client Error"):
        relay.send_event(project_id)


def test_static_relay(mini_sentry, relay):
    """
    A static relay without credentials will automatically fail the signature check
    because it does not have credentials.
    """
    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    # config for the static relay does not have the verifySignatureSetting
    static_config = deepcopy(config)

    config["config"]["trustedRelaySettings"]["verifySignature"] = "enabled"

    managed_relay = relay(mini_sentry)

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join(f"{project_id}.json").write(json.dumps(static_config))

    relay_options = {"relay": {"mode": "static"}}
    static_relay = relay(
        managed_relay, options=relay_options, prepare=configure_static_project
    )

    # sending to static relay is fine because it does not verify signature
    static_relay.send_event(project_id)

    # wait for project config and make sure that the event was discarded
    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "missing_signature", "category": "error", "quantity": 1}
    ]

    # sending to managed relay directly is not because it needs the signature
    # it will fail in the fast path because the project config is fetched at this point
    with pytest.raises(HTTPError, match="403 Client Error"):
        managed_relay.send_event(project_id)

    outcome = mini_sentry.get_client_report(timeout=1)
    assert outcome["discarded_events"] == [
        {"reason": "missing_signature", "category": "error", "quantity": 1}
    ]


def test_invalid_header_value(mini_sentry, relay):
    """
    Tests that getting a sequence in a header that cannot be represented as a UTF-8 string will
    produce a 400 error independent of the verifySignature setting.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(mini_sentry)

    with pytest.raises(HTTPError, match="400 Client Error"):
        relay.send_event(
            project_id,
            headers={
                "x-sentry-relay-signature": b"\xFF\xFF\xFF\xFF\xFF",
            },
        )
