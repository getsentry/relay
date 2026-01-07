from unittest import mock
import pytest
import os
import requests

from sentry_sdk.envelope import Envelope, Item, PayloadRef
from .asserts import time_within_delta


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


def user_data_event_json(response):
    return {
        "event_id": response.text.replace("-", ""),
        "timestamp": mock.ANY,
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
            "os": {"name": "Prospero", "os": "Prospero", "type": "os"},
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
                    "timestamp": mock.ANY,
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
        "_metrics": mock.ANY,
        "grouping_config": mock.ANY,
        "_meta": mock.ANY,
        "errors": mock.ANY,
    }


def playstation_event_json(sdk=mock.ANY):
    return {
        "event_id": mock.ANY,
        "level": "fatal",
        "version": mock.ANY,
        "type": "error",
        "logger": "",
        "platform": "native",
        "timestamp": mock.ANY,
        "received": time_within_delta(),
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
            "os": {
                "os": "PlayStation 9.20.00.05-00.00.00.0.1",
                "name": "PlayStation",
                "version": "9.20.00.05-00.00.00.0.1",
                "type": "os",
            },
            "runtime": {
                "runtime": "PS5 9.20.00.05-00.00.00.0.1",
                "name": "PS5",
                "version": "9.20.00.05-00.00.00.0.1",
                "type": "runtime",
            },
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
            ["cpu_vendor", "Sony"],
            ["os.name", "PlayStation"],
            ["cpu_brand", "PS5 CPU"],
            ["runtime.name", "PS5"],
            ["os", "PlayStation 9.20.00.05-00.00.00.0.1"],
            ["runtime", "9.20.00.05-00.00.00.0.1"],
            ["runtime.version", "9.20.00.05-00.00.00.0.1"],
            ["titleId", "NPXS29997"],
            ["server_name", "5be3652dd663dbdcd044da0f2144b17f"],
        ],
        "sdk": sdk,
        "errors": [
            {
                "type": "past_timestamp",
                "name": "timestamp",
                "sdk_time": "2025-02-20T10:23:01+00:00",
                "server_time": mock.ANY,
            }
        ],
        "key_id": "123",
        "project": 42,
        "grouping_config": mock.ANY,
        "_metrics": mock.ANY,
        "_meta": mock.ANY,
    }


def attachments(
    log_size=mock.ANY, generated_dump_size=mock.ANY, playstation_dump_size=mock.ANY
):
    return [
        {
            "id": mock.ANY,
            "name": "console.log",
            "rate_limited": False,
            "content_type": "text/plain",
            "attachment_type": "event.attachment",
            "size": log_size,
            "chunks": 1,
        },
        {
            "id": mock.ANY,
            "name": "generated_minidump.dmp",
            "rate_limited": False,
            "content_type": "application/x-dmp",
            "attachment_type": "event.minidump",
            "size": generated_dump_size,
            "chunks": 1,
        },
        {
            "id": mock.ANY,
            "name": "playstation.prosperodmp",
            "rate_limited": False,
            "content_type": "application/octet-stream",
            "attachment_type": "playstation.prosperodump",
            "size": playstation_dump_size,
            "chunks": 1,
        },
    ]


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
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "feature_disabled",
            "category": 1,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "feature_disabled",
            "category": 4,
            "quantity": 209385,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 3,
            "reason": "feature_disabled",
            "category": 22,
            "quantity": 1,
        },
    ]


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

    event, payload = attachments_consumer.get_event_only()

    assert payload == playstation_event_json(
        {"name": "sentry.playstation.devkit", "version": "0.0.1"},
    )
    assert sorted(event["attachments"], key=lambda x: x["name"]) == attachments(
        155829, 78050, 209385
    )


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

    event, payload = attachments_consumer.get_event_only()
    assert payload == user_data_event_json(response)
    assert len(event["attachments"]) == 3


def test_playstation_ignore_large_fields(
    mini_sentry,
    relay_with_playstation,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("user_data.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )

    # Make a dummy video that is larger than the dump
    video_content = "1" * (len(playstation_dump) + 100)
    relay = relay_with_playstation(
        mini_sentry,
        {
            "limits": {
                "max_attachment_size": len(video_content) - 1,
            },
            "outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1},
        },
    )

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, video_content
    )
    assert response.ok
    assert (mini_sentry.captured_outcomes.get(timeout=5)["outcomes"]) == [
        {
            "timestamp": mock.ANY,
            "project_id": 42,
            "outcome": 3,
            "reason": "too_large:attachment:attachment",
            "category": 4,
            "quantity": len(video_content),
        }
    ]
    assert [
        item.headers["filename"] for item in mini_sentry.get_captured_envelope().items
    ] == ["playstation.prosperodmp"]


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

    event, payload = attachments_consumer.get_event_only()

    assert payload == playstation_event_json(
        {"name": "sentry.native.playstation", "version": "0.1.0"}
    )
    assert sorted(event["attachments"], key=lambda x: x["name"]) == attachments(
        155829, 78050, 209385
    )


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

    event, payload = attachments_consumer.get_event_only()

    assert payload == {
        "event_id": mock.ANY,
        "level": "error",
        "version": "5",
        "type": "error",
        "logger": "",
        "platform": "other",
        "timestamp": mock.ANY,
        "received": time_within_delta(),
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
        "sdk": {"name": "raven-node", "version": "2.6.3"},
        "key_id": "123",
        "project": 42,
        "grouping_config": {
            "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
            "id": "legacy:2019-03-12",
        },
        "_metrics": {"bytes.ingested.event": 137},
    }

    assert event["attachments"] == (
        {
            "id": mock.ANY,
            "name": "playstation.prosperodmp",
            "rate_limited": False,
            "content_type": "application/octet-stream",
            "attachment_type": "playstation.prosperodump",
            "size": 209385,
            "chunks": 1,
        },
    )


def test_data_request(mini_sentry, relay_processing_with_playstation):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    relay = relay_processing_with_playstation()
    response = relay.send_playstation_data_request(PROJECT_ID)

    expected_response = {
        "parts": {
            "upload": [
                "corefile",
                "screenshot",
            ],
        }
    }
    assert response.status_code == 200
    assert response.json() == expected_response


def test_event_merging(
    mini_sentry,
    relay_processing_with_playstation,
    outcomes_consumer,
    attachments_consumer,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("native_user_data.prosperodmp")
    mini_sentry.add_full_project_config(
        PROJECT_ID,
        extra={"config": {"features": ["organizations:relay-playstation-ingestion"]}},
    )
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    relay = relay_processing_with_playstation()

    tempest_event = {
        "event_id": "123-123-123",
        "level": "fatal",
        "sdk": {
            "name": "sentry.playstation.crs",
            "version": "0.0.1",
            "packages": [],
            "integrations": [],
        },
        "contexts": {
            "CRS": {
                "crash_id": "123",
                "crash_url": "https://link_to_the_crash/123",
            }
        },
        "tags": {"CRS CrashID": "123"},
    }

    envelope = Envelope()
    envelope.add_event(tempest_event)

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

    event, payload = attachments_consumer.get_event_only()
    assert payload == {
        "event_id": mock.ANY,
        "level": "fatal",
        "version": "5",
        "type": "error",
        "logger": "",
        "platform": "native",
        "timestamp": mock.ANY,
        "received": time_within_delta(),
        "release": "test-app@1.0.0",
        "environment": "integration-test",
        "contexts": {
            "CRS": {
                "crash_id": "123",
                "crash_url": "https://link_to_the_crash/123",
                "type": "CRS",
            },
            "app": {"app_version": "", "type": "app"},
            "device": {
                "name": "PS5",
                "arch": "x86_64",
                "manufacturer": "Sony",
                "type": "device",
            },
            "os": {
                "os": "PlayStation 12.00.00.43-00.00.00.0.1",
                "name": "PlayStation",
                "version": "12.00.00.43-00.00.00.0.1",
                "type": "os",
            },
            "runtime": {
                "runtime": "PS5 12.00.00.43-00.00.00.0.1",
                "name": "PS5",
                "version": "12.00.00.43-00.00.00.0.1",
                "type": "runtime",
            },
            "trace": {
                "trace_id": "327245d5fdaa4b7e4689f44dc0bfd10d",
                "span_id": "dd5205ce07f34128",
                "status": "unknown",
                "sample_rand": 0.2063985201360333,
                "type": "trace",
            },
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
            ["CRS-CrashID", "123"],
            ["test.crash_id", "30b929e6-add4-4fce-e457-cb3187a0db7a"],
            ["test.suite", "integration"],
            ["test.type", "crash-capture"],
            ["server_name", "5be3652dd663dbdcd044da0f2144b17f"],
        ],
        "sdk": {
            "name": "sentry.native.playstation",
            "version": "0.10.1+20250903",
            "packages": [
                {"name": "github:getsentry/sentry-native", "version": "0.10.1+20250903"}
            ],
        },
        "key_id": "123",
        "project": 42,
        "grouping_config": {
            "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
            "id": "legacy:2019-03-12",
        },
        "_metrics": {
            "bytes.ingested.event": 725,
            "bytes.ingested.event.minidump": 60446,
            "bytes.ingested.event.attachment": 158008,
        },
        "_meta": mock.ANY,
        "errors": mock.ANY,
    }

    assert sorted(event["attachments"], key=lambda x: x["name"]) == attachments(
        158008, 60446, 210174
    )
