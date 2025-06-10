import pytest
import os
import json
import requests

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from .asserts import time_within_delta


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


def event_json(response):
    return {
        "event_id": response.text.replace("-", ""),
        "timestamp": 1748373553.0,
        "received": time_within_delta(),
        "level": "error",
        "version": "7",
        "type": "error",
        "logger": "",
        "platform": "native",
        "environment": "production",
        "contexts": {
            "app": {"app_version": "", "type": "app"},
            "device": {
                "name": "",
                "model": "PS5",
                "model_id": "5be3652dd663dbdcd044da0f2144b17f",
                "arch": "x86_64",
                "manufacturer": "Sony",
                "type": "device",
            },
            "os": {"name": "Prospero", "type": "os"},
            "runtime": {
                "runtime": "PS5 11.20.00.05-00.00.00.0.1",
                "name": "PS5",
                "version": "11.20.00.05-00.00.00.0.1",
                "type": "runtime",
            },
            "trace": {
                "trace_id": "a4c6cc5ab0d949d23f6d42e518ba49b4",
                "span_id": "75470378528743c2",
                "status": "unknown",
                "type": "trace",
            },
        },
        "breadcrumbs": {
            "values": [
                {
                    "timestamp": 1748373552.703305,
                    "type": "default",
                    "level": "info",
                    "message": "crumb",
                }
            ]
        },
        "exception": {
            "values": [
                {
                    "type": "Minidump",
                    "value": "Invalid Minidump",
                    "mechanism": {
                        "type": "minidump",
                        "synthetic": True,
                        "handled": False,
                    },
                }
            ]
        },
        "tags": [
            ["tag-name", "tag value"],
            ["server_name", "5be3652dd663dbdcd044da0f2144b17f"],
        ],
        "extra": {"extra-name": "extra value"},
        "sdk": {
            "name": "sentry.native.playstation",
            "version": "0.8.5",
            "packages": [
                {"name": "github:getsentry/sentry-native", "version": "0.8.5"}
            ],
        },
        "key_id": "123",
        "project": 42,
    }


def test_playstation_no_feature_flag(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation()

    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    # Get these outcomes since the feature flag is not enabled:
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2
    assert outcomes[0]["reason"] == "feature_disabled"
    assert outcomes[1]["reason"] == "feature_disabled"


def test_playstation_wrong_file(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("unreal_crash")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation()

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)

    response = exc_info.value.response
    assert response.status_code == 400, "Expected a 400 status code"
    assert response.json()["detail"] == "invalid prosperodump"


def test_playstation_too_large(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID)
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation(
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
    relay_processing_with_playstation,
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
    upstream = relay_processing_with_playstation()
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
    assert "playstation.prosperodmp" in [
        attachment["name"] for attachment in event["attachments"]
    ]

    assert event_data["sdk"]["name"] == "sentry.playstation.devkit"


def test_playstation_user_data_extraction(
    mini_sentry,
    relay,
    relay_processing_with_playstation,
    outcomes_consumer,
    attachments_consumer,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("user_data.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_processing_with_playstation()
    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
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

    for key in ["_metrics", "grouping_config"]:
        del event_data[key]

    assert event_data == event_json(response)
    assert len(event["attachments"]) == 3


def test_playstation_ignore_large_fields(
    mini_sentry,
    relay_with_playstation,
    outcomes_consumer,
    attachments_consumer,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("user_data.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()

    # Make a dummy video that is larger than the dump
    video_content = "1" * (len(playstation_dump) + 100)
    relay = relay_with_playstation(
        mini_sentry,
        {
            "limits": {
                "max_attachment_size": len(video_content) - 1,
            }
        },
    )

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, video_content
    )
    assert response.ok
    assert len(mini_sentry.captured_events.get().items) == 1


def test_playstation_attachment(
    mini_sentry,
    relay_processing_with_playstation,
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
    relay = relay_processing_with_playstation()

    bogus_error = {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "type": "error",
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
        "sdk": {
            "name": "sentry.native.playstation",
            "version": "0.1.0",
        },
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
    assert "playstation.prosperodmp" in [
        attachment["name"] for attachment in event["attachments"]
    ]

    assert event_data["sdk"]["name"] == "sentry.native.playstation"


def test_playstation_attachment_no_feature_flag(
    mini_sentry,
    relay_processing_with_playstation,
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
    relay = relay_processing_with_playstation()

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
