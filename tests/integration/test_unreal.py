import os
import pytest
import json


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    return dmp_file


@pytest.mark.parametrize("dump_file_name", ["unreal_crash", "unreal_crash_apple"])
@pytest.mark.parametrize("rollout_rate", [0.0, 1.0])
def test_unreal_crash(mini_sentry, relay, dump_file_name, rollout_rate):
    """
    Asserts that non-processing Relays forward the Unreal report either as a
    single unreal_report item or as expanded attachment items depending on
    the endpoint expansion rollout rate.
    """
    project_id = 42
    mini_sentry.global_config["options"][
        "relay.unreal-report-expansion.rollout-rate"
    ] = rollout_rate
    relay = relay(mini_sentry)
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "categories": ["attachment"],
            "limit": 0,
            "window": 3600,
            "id": "attachment_limit",
            "reasonCode": "static_disabled_quota",
        }
    ]

    unreal_content = load_dump_file(dump_file_name)

    response = relay.send_unreal_request(project_id, unreal_content)

    event_id = response.text.replace("-", "")
    envelope = mini_sentry.get_captured_envelope()
    assert envelope
    assert event_id == envelope.headers.get("event_id")
    items = envelope.items

    if rollout_rate == 0.0:
        assert len(items) == 1
        unreal_item = items[0]
        assert unreal_item.headers
        assert unreal_item.headers.get("type") == "unreal_report"
        assert unreal_item.headers.get("content_type") == "application/octet-stream"
        assert unreal_item.payload is not None
    else:
        assert len(items) == (4 if dump_file_name == "unreal_crash" else 6)
        for item in items:
            assert item.headers.get("type") == "attachment"
            assert item.headers.get("unreal_expanded") is True


@pytest.mark.parametrize("dump_file_name", ["unreal_crash", "unreal_crash_apple"])
@pytest.mark.parametrize("has_quota", [False, True])
def test_unreal_crash_ratelimiting(
    mini_sentry,
    relay,
    relay_with_processing,
    relay_credentials,
    attachments_consumer,
    outcomes_consumer,
    dump_file_name,
    has_quota,
):
    """
    Even if items expanded from an unreal report are not rate-limited in pops (see test_unreal_crash)
    they should still be rat-limited in processing after all the important information has been
    extracted.
    """
    project_id = 42
    mini_sentry.global_config["options"][
        "relay.unreal-report-expansion.rollout-rate"
    ] = 1.0

    credentials = relay_credentials()
    processing = relay_with_processing(static_credentials=credentials)
    pop = relay(processing, credentials=credentials)

    project_config = mini_sentry.add_full_project_config(project_id)
    if has_quota:
        project_config["config"]["quotas"] = [
            {
                "categories": ["attachment"],
                "limit": 0,
                "window": 3600,
                "id": "attachment_limit",
                "reasonCode": "static_disabled_quota",
            }
        ]

    attachments_consumer = attachments_consumer()
    outcomes_consumer = outcomes_consumer()

    unreal_content = load_dump_file(dump_file_name)
    pop.send_unreal_request(project_id, unreal_content)

    event, payload = attachments_consumer.get_event_only()

    assert event is not None
    assert event["type"] == "event"
    assert "unreal" in payload.get("contexts", {})

    if has_quota:
        outcomes_consumer.assert_rate_limited(
            "static_disabled_quota",
            categories=["attachment", "attachment_item"],
        )
    else:
        assert len(event["attachments"]) == (
            4 if dump_file_name == "unreal_crash" else 6
        )


def test_unreal_minidump_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    project_id = 42
    options = {"processing": {"attachment_chunk_size": "1.23 GB"}}
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash")

    relay.send_unreal_request(project_id, unreal_content)

    attachments = {}

    while True:
        raw_message, message = attachments_consumer.get_message()
        if message is None or message["type"] != "attachment_chunk":
            event = message
            break
        attachments[message["id"]] = message

    assert event
    assert event["type"] == "event"

    project_id = event["project_id"]
    event_id = event["event_id"]

    assert len(event["attachments"]) == 4
    assert len(attachments) == 4

    logs_file_found = False
    mini_dump_found = False
    crash_report_ini_found = False
    unreal_context_found = False

    for attachment_entry in event["attachments"]:
        # check that the attachment is registered in the event
        attachment_id = attachment_entry["id"]
        # check that we didn't get the messages chunked
        assert attachment_entry["chunks"] == 1

        entry_name = attachment_entry["name"]

        if entry_name == "UE4Minidump.dmp":
            mini_dump_found = True
        elif entry_name == "YetAnother.log":
            logs_file_found = True
        elif entry_name == "CrashContext.runtime-xml":
            unreal_context_found = True
        elif entry_name == "CrashReportClient.ini":
            crash_report_ini_found = True

        attachment = attachments.get(attachment_id)
        assert attachment is not None
        assert attachment["event_id"] == event_id
        assert attachment["project_id"] == project_id

    assert mini_dump_found
    assert logs_file_found
    assert unreal_context_found
    assert crash_report_ini_found

    # check the created event
    event_data = json.loads(event["payload"])

    assert event_data["event_id"] == event_id

    exception = event_data.get("exception")
    assert exception is not None
    values = exception["values"]
    assert values is not None

    mini_dump_process_marker_found = False

    for value in values:
        if value == {
            "type": "Minidump",
            "value": "Invalid Minidump",
            "mechanism": {"type": "minidump", "synthetic": True, "handled": False},
        }:
            mini_dump_process_marker_found = True

    assert mini_dump_process_marker_found
    assert "errors" not in event


def test_unreal_apple_crash_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    project_id = 42
    options = {"processing": {"attachment_chunk_size": "1.23 GB"}}
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash_apple")

    relay.send_unreal_request(project_id, unreal_content)

    attachments = {}

    user_report = None
    event = None
    while True:
        raw_message, message = attachments_consumer.get_message()
        if message is None:
            pytest.fail("could not get messages from attachment consumer")
        if message["type"] == "attachment_chunk":
            attachments[message["id"]] = message
        elif message["type"] == "user_report":
            user_report = message
        elif message["type"] == "event":
            event = message
            break

    assert event is not None
    assert user_report is not None

    project_id = event["project_id"]
    event_id = event["event_id"]

    assert len(event["attachments"]) == 6
    assert len(attachments) == 6

    mini_dump_found = False
    crash_report_ini_found = False
    logs_file_found = False
    crash_context_found = False
    info_file_found = False
    diagnostics_file_found = False

    for attachment_entry in event["attachments"]:
        # check that the attachment is registered in the event
        attachment_id = attachment_entry["id"]
        # check that we didn't get the messages chunked
        assert attachment_entry["chunks"] == 1

        entry_name = attachment_entry["name"]

        if entry_name == "minidump.dmp":
            mini_dump_found = True
        elif entry_name == "CrashReportClient.ini":
            crash_report_ini_found = True
        elif entry_name == "info.txt":
            info_file_found = True
        elif entry_name == "YetAnotherMac.log":
            logs_file_found = True
        elif entry_name == "CrashContext.runtime-xml":
            crash_context_found = True
        elif entry_name == "Diagnostics.txt":
            diagnostics_file_found = True

        attachment = attachments.get(attachment_id)
        assert attachment is not None
        assert attachment["event_id"] == event_id
        assert attachment["project_id"] == project_id

    assert mini_dump_found
    assert logs_file_found
    assert crash_context_found
    assert crash_report_ini_found
    assert info_file_found
    assert diagnostics_file_found

    # check the created event
    event_data = json.loads(event["payload"])

    assert event_data["event_id"] == event_id

    exception = event_data.get("exception")
    assert exception is not None
    values = exception["values"]
    assert values is not None

    apple_crash_report_marker_found = False

    for value in values:
        if value == {
            "type": "AppleCrashReport",
            "value": "Invalid Apple Crash Report",
            "mechanism": {
                "type": "applecrashreport",
                "synthetic": True,
                "handled": False,
            },
        }:
            apple_crash_report_marker_found = True

    assert apple_crash_report_marker_found
    assert "errors" not in event


def test_unreal_minidump_with_config_and_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    project_id = 42
    options = {"processing": {"attachment_chunk_size": "1.23 GB"}}
    relay = relay_with_processing(options)
    attachments_consumer = attachments_consumer()

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash_with_config")

    relay.send_unreal_request(project_id, unreal_content)

    attachments = {}

    while True:
        raw_message, message = attachments_consumer.get_message()
        if message is None or message["type"] != "attachment_chunk":
            event = message
            break
        attachments[message["id"]] = message

    assert event
    assert event["type"] == "event"

    # Test dump sets `IsAssert` to true but also supplies a custom `__sentry`
    # value setting the error level to warning. Relay should not overwrite
    # the user supplied level in this case.
    assert json.loads(event["payload"])["level"] == "warning"

    project_id = event["project_id"]
    event_id = event["event_id"]

    assert len(event["attachments"]) == 4
    assert len(attachments) == 4

    logs_file_found = False
    mini_dump_found = False
    crash_report_ini_found = False
    unreal_context_found = False

    for attachment_entry in event["attachments"]:
        # check that the attachment is registered in the event
        attachment_id = attachment_entry["id"]
        # check that we didn't get the messages chunked
        assert attachment_entry["chunks"] == 1

        entry_name = attachment_entry["name"]

        if entry_name == "UE4Minidump.dmp":
            mini_dump_found = True
        elif entry_name == "MyProject.log":
            logs_file_found = True
        elif entry_name == "CrashContext.runtime-xml":
            unreal_context_found = True
        elif entry_name == "CrashReportClient.ini":
            crash_report_ini_found = True

        attachment = attachments.get(attachment_id)
        assert attachment is not None
        assert attachment["event_id"] == event_id
        assert attachment["project_id"] == project_id

    assert mini_dump_found
    assert logs_file_found
    assert unreal_context_found
    assert crash_report_ini_found

    # check the created event
    event_data = json.loads(event["payload"])
    assert event_data["release"] == "foo-bar@1.0.0"

    assert event_data["event_id"] == event_id

    exception = event_data.get("exception")
    assert exception is not None
    values = exception["values"]
    assert values is not None

    mini_dump_process_marker_found = False

    for value in values:
        if value == {
            "type": "Minidump",
            "value": "Invalid Minidump",
            "mechanism": {"type": "minidump", "synthetic": True, "handled": False},
        }:
            mini_dump_process_marker_found = True

    assert mini_dump_process_marker_found
    assert "errors" not in event
