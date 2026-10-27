import os
import pytest


def load_dump_file(base_file_name: str):
    dmp_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "native", base_file_name
    )

    with open(dmp_path, "rb") as f:
        dmp_file = f.read()

    return dmp_file


@pytest.mark.parametrize(
    "dump_file_name, expected",
    [
        (
            "unreal_crash",
            [
                "event",
                "UE4Minidump.dmp",
                "YetAnother.log",
                "CrashReportClient.ini",
                "CrashContext.runtime-xml",
            ],
        ),
        (
            "unreal_crash_apple",
            [
                "event",
                "minidump.dmp",
                "Diagnostics.txt",
                "CrashReportClient.ini",
                "info.txt",
                "YetAnotherMac.log",
                "CrashContext.runtime-xml",
                "user_report",
            ],
        ),
    ],
)
def test_unreal_crash(mini_sentry, relay, dump_file_name, expected):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    unreal_content = load_dump_file(dump_file_name)

    response = relay.send_unreal_request(project_id, unreal_content)

    event_id = response.text.replace("-", "")
    envelope = mini_sentry.get_captured_envelope()
    assert event_id == envelope.headers.get("event_id")

    def fmt_item(item):
        ty = item.headers.get("type")
        return ty if not ty == "attachment" else item.headers.get("filename")

    assert [fmt_item(item) for item in envelope.items] == expected


def test_unreal_minidump_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer, objectstore
):
    project_id = 42
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    objectstore = objectstore("attachments", project_id)

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash")

    relay.send_unreal_request(project_id, unreal_content)

    event_data, event = attachments_consumer.get_event()
    assert event["project_id"] == project_id
    event_id = event["event_id"]

    assert len(event["attachments"]) == 4

    logs_file_found = False
    mini_dump_found = False
    crash_report_ini_found = False
    unreal_context_found = False

    for attachment_entry in event["attachments"]:
        entry_name = attachment_entry["name"]

        if entry_name == "UE4Minidump.dmp":
            mini_dump_found = True
        elif entry_name == "YetAnother.log":
            logs_file_found = True
        elif entry_name == "CrashContext.runtime-xml":
            unreal_context_found = True
        elif entry_name == "CrashReportClient.ini":
            crash_report_ini_found = True

        attachment = objectstore.get(attachment_entry["stored_id"])
        assert attachment.metadata.filename == entry_name
        assert len(attachment.payload.read()) == attachment_entry["size"]

    assert mini_dump_found
    assert logs_file_found
    assert unreal_context_found
    assert crash_report_ini_found

    # check the created event
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
    mini_sentry, relay_with_processing, attachments_consumer, objectstore
):
    project_id = 42
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    objectstore = objectstore("attachments", project_id)

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash_apple")

    relay.send_unreal_request(project_id, unreal_content)

    attachments_consumer.get_user_report()
    event_data, event = attachments_consumer.get_event()
    assert event["project_id"] == project_id
    event_id = event["event_id"]

    assert len(event["attachments"]) == 6

    mini_dump_found = False
    crash_report_ini_found = False
    logs_file_found = False
    crash_context_found = False
    info_file_found = False
    diagnostics_file_found = False

    for attachment_entry in event["attachments"]:
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

        attachment = objectstore.get(attachment_entry["stored_id"])
        assert attachment.metadata.filename == entry_name
        assert len(attachment.payload.read()) == attachment_entry["size"]

    assert mini_dump_found
    assert logs_file_found
    assert crash_context_found
    assert crash_report_ini_found
    assert info_file_found
    assert diagnostics_file_found

    # check the created event
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
    mini_sentry, relay_with_processing, attachments_consumer, objectstore
):
    project_id = 42
    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()
    objectstore = objectstore("attachments", project_id)

    mini_sentry.add_full_project_config(project_id)
    unreal_content = load_dump_file("unreal_crash_with_config")

    relay.send_unreal_request(project_id, unreal_content)

    event_data, event = attachments_consumer.get_event()

    # Test dump sets `IsAssert` to true but also supplies a custom `__sentry`
    # value setting the error level to warning. Relay should not overwrite
    # the user supplied level in this case.
    assert event_data["level"] == "warning"

    assert event["project_id"] == project_id
    event_id = event["event_id"]

    assert len(event["attachments"]) == 4

    logs_file_found = False
    mini_dump_found = False
    crash_report_ini_found = False
    unreal_context_found = False

    for attachment_entry in event["attachments"]:
        entry_name = attachment_entry["name"]

        if entry_name == "UE4Minidump.dmp":
            mini_dump_found = True
        elif entry_name == "MyProject.log":
            logs_file_found = True
        elif entry_name == "CrashContext.runtime-xml":
            unreal_context_found = True
        elif entry_name == "CrashReportClient.ini":
            crash_report_ini_found = True

        attachment = objectstore.get(attachment_entry["stored_id"])
        assert attachment.metadata.filename == entry_name
        assert len(attachment.payload.read()) == attachment_entry["size"]

    assert mini_dump_found
    assert logs_file_found
    assert unreal_context_found
    assert crash_report_ini_found

    # check the created event
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


def test_unreal_proxy_mode(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry, options={"relay": {"mode": "proxy"}})

    response = relay.send_unreal_request(project_id, load_dump_file("unreal_crash"))
    assert response.ok

    envelope = mini_sentry.get_captured_envelope()
    assert envelope
    assert len(envelope.items) == 1
    item = envelope.items[0]
    assert item.headers.get("type") == "unreal_report"
    assert item.headers.get("content_type") == "application/octet-stream"
