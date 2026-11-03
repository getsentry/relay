from datetime import datetime
from time import sleep
import pytest
import json
import os
import requests
from functools import cache

from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from urllib3 import encode_multipart_formdata
from .asserts import matches_any, time_within_delta
from .consts import DUMMY_UPLOAD_FINAL_LOCATION
from .consts import Outcome


@cache
def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        return f.read()


def playstation_project_config():
    """Project config extras for PlayStation tests with timestamp validation effectively disabled."""
    return {
        "config": {
            # Set to 100 years to prevent normalization from overwriting the timestamp
            "eventRetention": 36500,
            "features": [
                "organizations:relay-playstation-ingestion",
                "organizations:relay-new-error-processing",
                "projects:relay-playstation-uploads",
            ],
        }
    }


def user_data_event_json(response):
    return {
        "event_id": response.text.replace("-", ""),
        "timestamp": matches_any(),
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
                    "timestamp": matches_any(),
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
            ["titleId", "NPXS29997"],
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
        "_metrics": matches_any(),
        "grouping_config": matches_any(),
    }


def playstation_event_json(sdk=matches_any()):
    return {
        "event_id": matches_any(),
        "level": "fatal",
        "version": matches_any(),
        "type": "error",
        "logger": "",
        "platform": "native",
        "timestamp": matches_any(),
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
            "trace": matches_any(),
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
        "key_id": "123",
        "project": 42,
        "grouping_config": matches_any(),
        "_metrics": matches_any(),
    }


def attachments(
    log_size=matches_any(),
    generated_dump_size=matches_any(),
    playstation_dump_size=matches_any(),
):
    return [
        {
            "id": matches_any(),
            "name": "console.log",
            "rate_limited": False,
            "content_type": "text/plain",
            "attachment_type": "event.attachment",
            "size": log_size,
            "retention_days": 36500,
            "stored_id": matches_any(),
        },
        {
            "id": matches_any(),
            "name": "generated_minidump.dmp",
            "rate_limited": False,
            "content_type": "application/x-dmp",
            "attachment_type": "event.minidump",
            "size": generated_dump_size,
            "retention_days": 36500,
            "stored_id": matches_any(),
        },
        {
            "id": matches_any(),
            "name": "playstation.prosperodmp",
            "rate_limited": False,
            "content_type": "application/octet-stream",
            "attachment_type": "playstation.prosperodump",
            "size": playstation_dump_size,
            "retention_days": 36500,
            "stored_id": matches_any(),
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

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    response = exc_info.value.response

    assert response.status_code == 403

    # Get these outcomes since the feature flag is not enabled:
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda outcome: outcome["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "category": DataCategory.ERROR,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "category": DataCategory.ATTACHMENT,
            "quantity": 209385,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "feature_disabled",
            "category": DataCategory.ATTACHMENT_ITEM,
            "quantity": 1,
        },
    ]


def test_playstation_invalid_prosperodump(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("unreal_crash")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation()

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, raise_for_status=False
    )

    assert response.status_code == 400, "Expected a 400 status code"
    assert response.json()["detail"] == "invalid prosperodump"
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda outcome: outcome["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "invalid_prosperodump",
            "category": DataCategory.ERROR,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "invalid_prosperodump",
            "category": DataCategory.ATTACHMENT,
            "quantity": len(playstation_dump),
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "invalid_prosperodump",
            "category": DataCategory.ATTACHMENT_ITEM,
            "quantity": 1,
        },
    ]


def test_playstation_missing_prosperodump(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    video_content = b"yo"
    prosperodump = None
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation()

    response = relay.send_playstation_request(
        PROJECT_ID, prosperodump, video_content, raise_for_status=False
    )

    assert response.status_code == 400, "Expected a 400 status code"
    assert response.json()["detail"] == "missing prosperodump"
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda outcome: outcome["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "missing_prosperodump_upload",
            "category": DataCategory.ERROR,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "missing_prosperodump_upload",
            "category": DataCategory.ATTACHMENT,
            "quantity": len(video_content),
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "missing_prosperodump_upload",
            "category": DataCategory.ATTACHMENT_ITEM,
            "quantity": 1,
        },
    ]


def test_playstation_max_attachments_size_exceeded(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation(
        {
            "limits": {
                "max_attachment_size": 1000 * 1024 * 1024,
                "max_attachments_size": len(playstation_dump) - 1,
            }
        }
    )

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, raise_for_status=False
    )

    assert response.status_code == 413, response.json()
    assert response.json() == {"detail": "request content exceeded size limits"}
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda outcome: outcome["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "request_too_large",
            "category": DataCategory.ERROR,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "too_large:attachment:attachment",
            "category": DataCategory.ATTACHMENT,
            "quantity": len(playstation_dump),
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "too_large:attachment:attachment",
            "category": DataCategory.ATTACHMENT_ITEM,
            "quantity": 1,
        },
    ]


def test_playstation_max_attachment_size_exceeded(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation(
        {
            "limits": {
                "max_attachment_size": len(playstation_dump) - 1,
                "max_attachments_size": 1000 * 1024 * 1024,
            }
        }
    )

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, raise_for_status=False
    )

    assert response.status_code == 400, "Expected a 400 status code"
    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda outcome: outcome["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "missing_prosperodump_upload",
            "category": DataCategory.ERROR,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "too_large:attachment:prosperodump",
            "category": DataCategory.ATTACHMENT,
            "quantity": len(playstation_dump),
        },
        {
            "timestamp": time_within_delta(),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": Outcome.INVALID,
            "reason": "too_large:attachment:prosperodump",
            "category": DataCategory.ATTACHMENT_ITEM,
            "quantity": 1,
        },
    ]


def test_playstation_max_stream_size_exceeded(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    stream_size_limit = len(playstation_dump) - 100
    relay = relay_processing_with_playstation(
        {
            "limits": {
                "max_upload_size": int(stream_size_limit / 2),
                "max_attachments_size": int(stream_size_limit / 2),
            }
        }
    )
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()

    with pytest.raises(requests.exceptions.HTTPError) as exc_info:
        _ = relay.send_playstation_request(PROJECT_ID, playstation_dump)

    response = exc_info.value.response
    assert response.status_code == 413, "Expected a 413 status code"
    assert response.content.decode("utf-8") == "length limit exceeded"
    assert len(outcomes_consumer.get_outcomes()) == 0


def test_playstation_max_stream_size_exceeded_chunked(
    mini_sentry, relay_with_playstation
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    stream_size_limit = len(playstation_dump) - 100
    relay = relay_with_playstation(
        mini_sentry,
        {
            "limits": {
                "max_upload_size": int(stream_size_limit / 2),
                "max_attachments_size": int(stream_size_limit / 2),
            }
        },
    )
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())

    fields = [
        (
            "upload_file_minidump",
            ("playstation.prosperodmp", playstation_dump, "application/octet-stream"),
        ),
    ]
    body, content_type = encode_multipart_formdata(fields)

    # Passing a generator to `data` makes requests send Transfer-Encoding: chunked
    # instead of a fixed Content-Length.
    response = relay.request(
        "post",
        "/api/{}/playstation/?sentry_key={}".format(
            PROJECT_ID, mini_sentry.get_dsn_public_key(PROJECT_ID)
        ),
        headers={"Content-Type": content_type},
        data=iter([body]),
    )
    assert response.status_code == 413, "Expected a 413 status code"
    assert response.json() == {"detail": "request content exceeded size limits"}


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
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
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
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
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


def session_relay_options():
    # The session in the dump has a fixed `started` time, which must not be rejected as too old.
    forever = 100 * 365 * 24 * 60 * 60
    return {
        "processing": {"max_session_secs_in_past": forever},
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
            "max_secs_in_past": forever,
        },
    }


def test_playstation_with_session(
    mini_sentry,
    relay_processing_with_playstation,
    outcomes_consumer,
    attachments_consumer,
    metrics_consumer,
):
    PROJECT_ID = 42
    # Crash captured by sentry-playstation, which embeds the crashed session in the userdata.
    playstation_dump = load_dump_file("session.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_processing_with_playstation(options=session_relay_options())

    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    event, payload = attachments_consumer.get_event_only()
    assert payload["level"] == "fatal"
    assert payload["release"] == "test-app@1.0.0"

    metrics = metrics_consumer.get_metrics(with_headers=False)

    started = int(
        datetime.fromisoformat("2026-10-21T08:11:28.881605+00:00").timestamp()
    )
    common_tags = {"environment": "integration-test", "release": "test-app@1.0.0"}
    assert [
        (m["name"], m["tags"].get("session.status"), m["value"]) for m in metrics
    ] == [
        ("c:sessions/session@none", "crashed", 1.0),
        ("c:sessions/session@none", "init", 1.0),
        ("s:sessions/error@none", None, [matches_any()]),
        ("s:sessions/user@none", "crashed", [matches_any()]),
        ("s:sessions/user@none", "errored", [matches_any()]),
    ]
    for metric in metrics:
        assert metric["project_id"] == PROJECT_ID
        assert metric["timestamp"] == started
        assert {k: v for k, v in metric["tags"].items() if k in common_tags} == (
            common_tags
        )

    outcomes_consumer.assert_empty()


def test_playstation_without_session(
    mini_sentry,
    relay_processing_with_playstation,
    attachments_consumer,
    metrics_consumer,
):
    PROJECT_ID = 42
    # Crash captured before the SDK embedded the session.
    playstation_dump = load_dump_file("user_data.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    attachments_consumer = attachments_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_processing_with_playstation(options=session_relay_options())
    response = relay.send_playstation_request(PROJECT_ID, playstation_dump)
    assert response.ok

    attachments_consumer.get_event_only()
    metrics_consumer.assert_empty()


@pytest.mark.parametrize("use_pop_relay", [True, False])
def test_playstation_upload_attachments(
    mini_sentry,
    relay_with_playstation,
    relay_processing_with_playstation,
    relay_credentials,
    objectstore,
    outcomes_consumer,
    attachments_consumer,
    use_pop_relay,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    outcomes_consumer = outcomes_consumer()
    attachments_consumer = attachments_consumer()
    credentials = relay_credentials()
    relay = relay_processing_with_playstation(static_credentials=credentials)
    if use_pop_relay:
        relay = relay_with_playstation(relay, credentials=credentials)

    video_content = "video content"
    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, video_content
    )

    assert response.ok
    assert len(outcomes_consumer.get_outcomes()) == 0

    _, event = attachments_consumer.get_event()
    objectstore_session = objectstore("attachments", PROJECT_ID)

    video_attachment = [
        a for a in event["attachments"] if a["name"] == "crash-video.webm"
    ][0]
    video_key = video_attachment["stored_id"]
    assert objectstore_session.get(video_key).payload.read() == video_content.encode()

    dump_attachment = [
        a for a in event["attachments"] if a["name"] == "playstation.prosperodmp"
    ][0]
    dump_key = dump_attachment["stored_id"]
    assert objectstore_session.get(dump_key).payload.read() == playstation_dump


def test_playstation_ignore_large_attachments_when_uploading_disabled(
    mini_sentry,
    relay_with_playstation,
):
    PROJECT_ID = 42
    config = playstation_project_config()
    config["config"]["features"].remove("projects:relay-playstation-uploads")
    mini_sentry.add_full_project_config(PROJECT_ID, extra=config)
    playstation_dump = load_dump_file("user_data.prosperodmp")
    relay = relay_with_playstation(
        mini_sentry,
        {
            "limits": {
                "max_attachment_size": len(playstation_dump),
            },
        },
    )
    # Make a dummy video that exceeds max_attachment_size
    video_content = "1" * 1024 * 1024

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, video_content
    )

    assert response.ok
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
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
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

    expected = playstation_event_json(
        {"name": "sentry.native.playstation", "version": "0.1.0"}
    )
    # Exceptions sent by the client are retained after the minidump placeholder.
    expected["exception"]["values"].append(
        {"type": "ValueError", "value": "Should not happen"}
    )
    assert payload == expected
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
    mini_sentry.add_full_project_config(PROJECT_ID)
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
        "event_id": matches_any(),
        "level": "error",
        "version": "5",
        "type": "error",
        "logger": "",
        "platform": "other",
        "timestamp": matches_any(),
        "received": time_within_delta(),
        "exception": {"values": [{"type": "ValueError", "value": "Should not happen"}]},
        "sdk": {"name": "raven-node", "version": "2.6.3"},
        "key_id": "123",
        "project": 42,
        "contexts": {
            "trace": matches_any(),
        },
        "grouping_config": {
            "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
            "id": "legacy:2019-03-12",
        },
        "_metrics": {
            "bytes.ingested.event": 137,
        },
    }

    assert event["attachments"] == (
        {
            "id": matches_any(),
            "name": "playstation.prosperodmp",
            "rate_limited": False,
            "content_type": "application/octet-stream",
            "attachment_type": "playstation.prosperodump",
            "size": 209385,
            "retention_days": 90,
            "stored_id": matches_any(),
        },
    )


def test_data_request(mini_sentry, relay_processing_with_playstation):
    PROJECT_ID = 42
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
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
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
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
        "event_id": matches_any(),
        "level": "fatal",
        "version": "5",
        "type": "error",
        "logger": "",
        "platform": "native",
        "timestamp": 1759841673.0,
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
            ["titleId", "NPXS29997"],
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
            "bytes.ingested.event": 836,
            "bytes.ingested.event.minidump": 60446,
            "bytes.ingested.event.attachment": 158008,
        },
    }

    assert sorted(event["attachments"], key=lambda x: x["name"]) == attachments(
        158008, 60446, 210174
    )


@pytest.mark.parametrize("rate_limits", [[], ["error"], ["error", "attachment"]])
@pytest.mark.parametrize("use_pop_relay", [True, False])
def test_playstation_rate_limited(
    mini_sentry,
    relay_with_playstation,
    relay_processing_with_playstation,
    relay_credentials,
    use_pop_relay,
    rate_limits,
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    config = playstation_project_config()
    config["config"]["quotas"] = [
        {"categories": rate_limits, "limit": 0, "reasonCode": "static_disabled_quota"}
    ]
    mini_sentry.add_full_project_config(PROJECT_ID, extra=config)
    credentials = relay_credentials()
    relay = relay_processing_with_playstation(static_credentials=credentials)
    if use_pop_relay:
        relay = relay_with_playstation(relay, credentials=credentials)

    # Playstation never returns 429
    relay.send_playstation_request(PROJECT_ID, playstation_dump)
    sleep(1)
    relay.send_playstation_request(PROJECT_ID, playstation_dump)


def test_playstation_unknown_project(
    relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    playstation_dump = load_dump_file("playstation.prosperodmp")
    # Deliberately do NOT register the project, so it resolves as Disabled upstream.
    # mini_sentry.add_full_project_config(project_id)
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation()

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, raise_for_status=False
    )

    assert response.status_code == 403
    assert outcomes_consumer.get_aggregated_outcomes(timeout=0.5) == []


def test_playstation_project_unavailable(
    mini_sentry, relay_processing_with_playstation, outcomes_consumer
):
    PROJECT_ID = 42
    # Force the upstream to keep returning the project as pending so `ready()` times out.
    mini_sentry.project_config_simulate_pending = True
    mini_sentry.add_full_project_config(PROJECT_ID, extra=playstation_project_config())
    mini_sentry.global_config["options"]["relay.endpoint-fetch-config.enabled"] = True
    outcomes_consumer = outcomes_consumer()
    relay = relay_processing_with_playstation(
        {"limits": {"query_timeout": 1}, "cache": {"batch_interval": 500}}
    )
    playstation_dump = load_dump_file("playstation.prosperodmp")

    response = relay.send_playstation_request(
        PROJECT_ID, playstation_dump, raise_for_status=False
    )

    assert response.status_code == 503
    assert outcomes_consumer.get_aggregated_outcomes(timeout=0.5) == []


def test_playstation_attachment_inline_limit(
    mini_sentry,
    dummy_upload,
    relay_with_playstation,
):
    project_id = 42
    prosperodump = load_dump_file("playstation.prosperodmp")

    mini_sentry.add_full_project_config(project_id, extra=playstation_project_config())
    mini_sentry.global_config["options"]["relay.endpoint-fetch-config.enabled"] = True
    mini_sentry.global_config["options"]["relay.attachment-inline.limit"] = 16

    relay = relay_with_playstation(mini_sentry)

    # Video smaller than the limit should be inlined.
    small_video = b"small video"
    response = relay.send_playstation_request(project_id, prosperodump, small_video)
    assert response.ok

    envelope = mini_sentry.get_captured_envelope()
    by_name = {i.headers.get("filename"): i for i in envelope.items}
    video = by_name["crash-video.webm"]
    assert (
        video.headers.get("content_type")
        != "application/vnd.sentry.attachment-ref+json"
    )
    assert video.payload.bytes == small_video

    # Prosperodump inlined regardless of the size.
    dump = by_name["playstation.prosperodmp"]
    assert (
        dump.headers.get("content_type") != "application/vnd.sentry.attachment-ref+json"
    )

    # Large video uploaded to objectstore.
    large_video = b"X" * 100
    response = relay.send_playstation_request(project_id, prosperodump, large_video)
    assert response.ok

    envelope = mini_sentry.get_captured_envelope()
    by_name = {i.headers.get("filename"): i for i in envelope.items}
    video = by_name["crash-video.webm"]
    assert video.headers["content_type"] == "application/vnd.sentry.attachment-ref+json"
    assert json.loads(video.payload.bytes) == {
        "location": DUMMY_UPLOAD_FINAL_LOCATION,
        "content_type": "video/webm",
    }
