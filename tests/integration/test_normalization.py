import json

import pytest
from .test_unreal import load_dump_file
import os
from sentry_sdk.envelope import Envelope, Item, PayloadRef


def get_test_data(name):
    input_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-input.json"
    )
    with open(input_path, "r") as f:
        input = json.loads(f.read())
    input.pop("timestamp", None)

    output_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-output.json"
    )
    with open(output_path, "r") as f:
        output = json.loads(f.read())

    return input, output


def drop_props(payload):
    props = ["timestamp", "received", "ingest_path", "_metrics"]
    for prop in props:
        payload.pop(prop, None)
    return payload


@pytest.mark.parametrize("config_full_normalization", (False, True))
def test_relay_with_full_normalization(mini_sentry, relay, config_full_normalization):
    input, expected = get_test_data("extended-event")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        upstream=mini_sentry,
        options=(
            {"normalization": {"level": "full"}} if config_full_normalization else {}
        ),
    )

    relay.send_event(project_id, input)
    envelope = mini_sentry.captured_events.get(timeout=10)

    assert "fully_normalized" in envelope.items[0].headers
    assert drop_props(expected) == drop_props(envelope.get_event())


@pytest.mark.parametrize("config_full_normalization", (False, True))
@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
def test_processing_with_full_normalization(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay_credentials,
    config_full_normalization,
    request_from_internal,
    fully_normalized,
):
    input, expected = get_test_data("extended-event")

    events_consumer = events_consumer()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    relay_config = {}
    if request_from_internal:
        relay_config["auth"] = {
            "static_relays": {
                credentials["id"]: {
                    "public_key": credentials["public_key"],
                    "internal": True,
                },
            }
        }
    if config_full_normalization:
        relay_config["normalization"] = {"level": "full"}
    processing = relay_with_processing(
        options=relay_config,
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    envelope.add_item(
        item=Item(
            type="event",
            payload=PayloadRef(json=input),
            headers={"fully_normalized": True} if fully_normalized else {},
        ),
    )

    processing.send_envelope(
        project_id,
        envelope,
        headers=(
            {"X-Sentry-Relay-Id": credentials["id"]} if request_from_internal else {}
        ),
    )

    ingested, _ = events_consumer.get_event(timeout=10)
    if not config_full_normalization and request_from_internal and fully_normalized:
        assert drop_props(expected) != drop_props(ingested)
    else:
        assert drop_props(expected) == drop_props(ingested)


@pytest.mark.parametrize(
    "relay_static_config_normalization",
    [
        False,
        True,
    ],
)
def test_relay_chain_normalizes_events(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay,
    relay_credentials,
    relay_static_config_normalization,
):
    input, expected = get_test_data("extended-event")

    events_consumer = events_consumer()
    project_id = 42

    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    processing = relay_with_processing(
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            }
        }
    )
    relay = relay(
        upstream=processing,
        options=(
            {"normalization": {"level": "full"}}
            if relay_static_config_normalization
            else {}
        ),
        credentials=credentials,
    )

    relay.send_event(project_id, input)

    ingested, _ = events_consumer.get_event(timeout=15)

    # Running full normalization twice on the same envelope adds the errors
    # twice, one per run. The rest is the same.
    if relay_static_config_normalization:
        assert ingested["errors"] == [
            {"name": "location", "type": "invalid_attribute"},
            {"name": "location", "type": "invalid_attribute"},
        ]
        ingested["errors"].pop(0)

    assert drop_props(expected) == drop_props(ingested)


@pytest.mark.parametrize("config_full_normalization", (False, True))
@pytest.mark.parametrize("dump_file_name", ("unreal_crash", "unreal_crash_apple"))
def test_relay_doesnt_normalize_unextracted_unreal_event(
    mini_sentry,
    relay,
    dump_file_name,
    config_full_normalization,
):
    """
    Independently of the configuration, relays forward minidumps, apple crash
    reports and unreal events without marking them as normalized.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        mini_sentry,
        options=(
            {"normalization": {"level": "full"}} if config_full_normalization else {}
        ),
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    unreal_content = load_dump_file(dump_file_name)
    envelope.add_item(
        item=Item(
            type="unreal_report",
            payload=PayloadRef(unreal_content),
            # fully_normalized == False
        ),
    )
    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=10)
    assert len(envelope.items) == 1
    assert "fully_normalized" not in envelope.items[0].headers


@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
@pytest.mark.parametrize("dump_file_name", ("unreal_crash", "unreal_crash_apple"))
def test_processing_normalizes_unreal_event(
    mini_sentry,
    attachments_consumer,
    relay_credentials,
    relay_with_processing,
    request_from_internal,
    fully_normalized,
    dump_file_name,
):
    attachments_consumer = attachments_consumer()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    relay_config = {"processing": {"attachment_chunk_size": "1.23 GB"}}
    if request_from_internal:
        relay_config["auth"] = {
            "static_relays": {
                credentials["id"]: {
                    "public_key": credentials["public_key"],
                    "internal": True,
                },
            }
        }
    processing = relay_with_processing(options=relay_config)

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    unreal_content = load_dump_file(dump_file_name)
    envelope.add_item(
        item=Item(
            type="unreal_report",
            payload=PayloadRef(unreal_content),
            headers={"fully_normalized": True} if fully_normalized else {},
        ),
    )
    processing.send_envelope(
        project_id, envelope, headers={"X-Sentry-Relay-Id": credentials["id"]}
    )

    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"


@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
def test_processing_normalizes_minidump_events(
    mini_sentry,
    attachments_consumer,
    relay_with_processing,
    relay_credentials,
    request_from_internal,
    fully_normalized,
):
    attachments_consumer = attachments_consumer()
    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    credentials = relay_credentials()
    if request_from_internal:
        static_config = {
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        }
    else:
        static_config = {}
    processing = relay_with_processing(static_relays=static_config)

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})

    minidump = load_dump_file("minidump.dmp")
    item_headers = {"attachment_type": "event.minidump"}
    if fully_normalized:
        item_headers["fully_normalized"] = True
    envelope.add_item(
        item=Item(
            type="attachment",
            payload=PayloadRef(minidump),
            content_type="application/x-dmp",
            headers=item_headers,
        ),
    )
    processing.send_envelope(
        project_id, envelope, headers={"X-Sentry-Relay-Id": credentials["id"]}
    )
    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"


@pytest.mark.parametrize(
    "relay_static_config_normalization",
    [
        False,
        True,
    ],
)
def test_relay_chain_normalizes_minidump_events(
    mini_sentry,
    attachments_consumer,
    relay_with_processing,
    relay,
    relay_credentials,
    relay_static_config_normalization,
    processing_skip_normalization,
):

    attachments_consumer = attachments_consumer()
    project_id = 42

    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    processing = relay_with_processing(
        options={"processing": {"attachment_chunk_size": "1.23 GB"}},
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            }
        },
    )
    relay = relay(
        upstream=processing,
        options=(
            {"normalization": {"level": "full"}}
            if relay_static_config_normalization
            else {}
        ),
        credentials=credentials,
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    minidump = load_dump_file("minidump.dmp")
    item_headers = {"attachment_type": "event.minidump"}
    envelope.add_item(
        item=Item(
            type="attachment",
            payload=PayloadRef(minidump),
            content_type="application/x-dmp",
            headers=item_headers,
        ),
    )

    relay.send_envelope(project_id, envelope)
    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"
