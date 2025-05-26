import _queue
import json
import os


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

    # managed_relay = relay(mini_sentry, static_relays=static_relays)
    managed_relay = relay(mini_sentry, static_relays=static_relays)

    relay = relay(managed_relay, credentials=credentials)

    relay.send_event(project_id, {"platform": "javascript"})

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["platform"] == "javascript"


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

    managed_relay.send_event(
        project_id,
        {"platform": "javascript"},
        headers={
            "x-sentry-relay-signature": "this-is-a-cool-signature",
            "x-signature-version": "v1",
            "x-sentry-relay-id": credentials["id"],
            "x-signature-headers": "x-signature-datetime",
            "x-signature-datetime": "202505211045",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["platform"] == "javascript"


def test_invalid_signature(mini_sentry, relay, relay_credentials):
    """
    Tests that a signature is rejected if the signature is invalid and the relay is not configured
    as an internal relay.
    """
    project_id = 42
    credentials = relay_credentials()
    managed_relay = relay(mini_sentry, credentials=credentials)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True
    config["config"]["trustedRelays"] = [credentials["public_key"]]

    managed_relay.send_event(
        project_id,
        {"platform": "javascript"},
        headers={
            "x-sentry-relay-signature": "this-is-a-cool-signature",
            "x-signature-version": "v1",
            "x-sentry-relay-id": credentials["id"],
            "x-signature-headers": "x-signature-datetime",
            "x-signature-datetime": "202505211045",
        },
    )

    try:
        mini_sentry.captured_events.get(timeout=1)
        assert False
    except _queue.Empty:
        assert True


def test_not_trusted_relay(mini_sentry, relay, relay_credentials):
    """
    Tests that events are rejected if we directly send an event to relay
    without any signature.
    """
    project_id = 42
    credentials = relay_credentials()
    managed_relay = relay(mini_sentry, credentials=credentials)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    managed_relay.send_event(project_id, {"platform": "javascript"})

    try:
        mini_sentry.captured_events.get(timeout=1)
        assert False
    except _queue.Empty:
        assert True


def test_reject_without_signature(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    relay.send_event(project_id, {"platform": "javascript"})

    try:
        mini_sentry.captured_events.get(timeout=1)
        assert False
    except _queue.Empty:
        assert True


def test_static_relay(mini_sentry, relay):
    """
    A static relay does (or might?) not have credentials, so it cannot sign any outgoing requests.
    """

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["trustedRelaySettings"]["verifySignature"] = True

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join(f"{project_id}.json").write(json.dumps(config))

    relay_options = {"relay": {"mode": "static"}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_static_project)

    relay.send_event(project_id, {"platform": "javascript"})

    try:
        mini_sentry.captured_events.get(timeout=1)
        assert False
    except _queue.Empty:
        assert True
