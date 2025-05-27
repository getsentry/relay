import pytest
import os
import json
import requests

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


def test_playstation_no_feature_flag(
    mini_sentry, relay_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_playstation()

    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    # Get these outcomes since the feature flag is not enabled:
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2
    assert outcomes[0]["reason"] == "feature_disabled"
    assert outcomes[1]["reason"] == "feature_disabled"


def test_playstation_wrong_file(mini_sentry, relay_with_playstation, outcomes_consumer):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("unreal_crash")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_playstation()

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)

    response = exc_info.value.response
    assert response.status_code == 400, "Expected a 400 status code"
    assert response.json()["detail"] == "invalid prosperodump"


def test_playstation_too_large(mini_sentry, relay_with_playstation, outcomes_consumer):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_playstation(
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
    relay_with_playstation,
    outcomes_consumer,
    attachments_consumer,
    num_intermediate_relays,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_playstation()
    # Build chain of relays
    for _ in range(num_intermediate_relays):
        upstream = relay(upstream)

    response = upstream.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 0

    while True:
        _, message = attachments_consumer.get_message()
        if message is None or message["type"] != "attachment_chunk":
            event = message
            break

    assert event
    assert event["type"] == "event"

    event_data = json.loads(event["payload"])
    assert event_data["type"] == "error"
    assert event_data["level"] == "fatal"

    assert "contexts" in event_data
    assert "device" in event_data["contexts"]
    assert "os" in event_data["contexts"]
    assert "runtime" in event_data["contexts"]

    assert event_data["contexts"]["device"]["model"] == "PS5"
    assert event_data["contexts"]["device"]["manufacturer"] == "Sony"
    assert event_data["contexts"]["device"]["arch"] == "x86_64"

    assert event_data["contexts"]["os"]["name"] == "PlayStation"
    assert event_data["contexts"]["runtime"]["name"] == "PS5"

    tags_dict = dict(event_data["tags"])
    assert tags_dict["cpu_vendor"] == "Sony"
    assert tags_dict["os.name"] == "PlayStation"
    assert tags_dict["cpu_brand"] == "PS5 CPU"
    assert "titleId" in tags_dict

    assert "exception" in event_data
    assert len(event_data["exception"]["values"]) == 1
    assert event_data["exception"]["values"][0]["type"] == "Minidump"
    assert event_data["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    assert "_metrics" in event_data
    assert event_data["_metrics"]["bytes.ingested.event.minidump"] > 0
    assert event_data["_metrics"]["bytes.ingested.event.attachment"] > 0

    assert len(event["attachments"]) == 3
    attachment = event["attachments"][0]
    assert attachment["attachment_type"] == "playstation.prosperodump"


def test_playstation_attachment(
    mini_sentry,
    relay_with_playstation,
    outcomes_consumer,
    attachments_consumer,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_with_playstation()

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

    while True:
        _, message = attachments_consumer.get_message()
        if message is None or message["type"] != "attachment_chunk":
            event = message
            break

    assert event
    event_data = json.loads(event["payload"])
    assert event_data["type"] == "error"
    assert event_data["level"] == "fatal"

    assert "contexts" in event_data
    assert "device" in event_data["contexts"]
    assert "os" in event_data["contexts"]
    assert "runtime" in event_data["contexts"]

    assert event_data["contexts"]["device"]["model"] == "PS5"
    assert event_data["contexts"]["device"]["manufacturer"] == "Sony"
    assert event_data["contexts"]["device"]["arch"] == "x86_64"

    assert event_data["contexts"]["os"]["name"] == "PlayStation"
    assert event_data["contexts"]["runtime"]["name"] == "PS5"

    tags_dict = dict(event_data["tags"])
    assert tags_dict["cpu_vendor"] == "Sony"
    assert tags_dict["os.name"] == "PlayStation"
    assert tags_dict["cpu_brand"] == "PS5 CPU"
    assert "titleId" in tags_dict

    assert "exception" in event_data
    assert len(event_data["exception"]["values"]) == 1
    assert event_data["exception"]["values"][0]["type"] == "Minidump"
    assert event_data["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    assert "_metrics" in event_data
    assert event_data["_metrics"]["bytes.ingested.event.minidump"] > 0
    assert event_data["_metrics"]["bytes.ingested.event.attachment"] > 0

    assert len(event["attachments"]) == 3
    attachment = event["attachments"][0]
    assert attachment["attachment_type"] == "playstation.prosperodump"


def test_playstation_attachment_no_feature_flag(
    mini_sentry,
    relay_with_playstation,
    outcomes_consumer,
    attachments_consumer,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_with_playstation()

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

    while True:
        _, message = attachments_consumer.get_message()
        if message is None or message["type"] != "attachment_chunk":
            event = message
            break

    assert event

    assert len(event["attachments"]) == 1
    attachment = event["attachments"][0]
    assert attachment["attachment_type"] == "playstation.prosperodump"

    event_data = json.loads(event["payload"])
    assert event_data["type"] == "error"
    assert "exception" in event_data
    assert event_data["exception"]["values"][0]["type"] == "ValueError"
    assert event_data["exception"]["values"][0]["value"] == "Should not happen"
