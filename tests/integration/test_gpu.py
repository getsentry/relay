"""Integration tests for the GPU crash split.

Relay splits a minidump upload that carries an NVIDIA Aftermath GPU crash dump
(`.nv-gpudmp`) into two events: the CPU crash (minidump) keeps the original event,
and the GPU crash becomes a second, trace-connected event that carries a copy of the
scope plus the `.nv-gpudmp` / `.nvdbg` attachments. Gated on the
`organizations:gpu-crash-symbolication` feature.
"""

from uuid import UUID

import msgpack

MINIDUMP_ATTACHMENT_NAME = "upload_file_minidump"
EVENT_ATTACHMENT_NAME = "__sentry-event"
GPU_FEATURE = "organizations:gpu-crash-symbolication"

# Relay infers attachment types from the file name; the bytes are opaque to Relay, so
# dummy content is enough.
MINIDUMP = (MINIDUMP_ATTACHMENT_NAME, "minidump.dmp", "MDMP content")
GPU_DUMP = ("gpudump", "crash.nv-gpudmp", b"NVGPU dummy dump")
SHADER_DBG = ("shaderdbg", "shader-abc.nvdbg", b"nvdbg dummy")

SCOPE_EVENT_ID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
SCOPE_TRACE_ID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


def scope_attachment():
    """The `__sentry-event` scope (msgpack) the SDK ships alongside the crash."""
    event = {
        "event_id": SCOPE_EVENT_ID,
        "release": "game@1.0.0",
        "environment": "prod",
        "tags": {"custom_tag": "custom_value"},
        "contexts": {
            "trace": {"trace_id": SCOPE_TRACE_ID, "span_id": "cccccccccccccccc"}
        },
    }
    return (EVENT_ATTACHMENT_NAME, EVENT_ATTACHMENT_NAME, msgpack.packb(event))


def attachment_types(envelope):
    return {
        item.headers.get("attachment_type")
        for item in envelope.items
        if item.headers.get("type") == "attachment"
    }


def test_gpu_crash_splits_into_two_events(mini_sentry, relay):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    config["config"]["features"] = [GPU_FEATURE]
    relay = relay(mini_sentry)

    response = relay.send_minidump(
        project_id=project_id,
        files=[MINIDUMP, scope_attachment(), GPU_DUMP, SHADER_DBG],
    )
    cpu_event_id = UUID(response.text.strip())

    # Both envelopes are forwarded; classify them by their attachments.
    envelopes = [mini_sentry.get_captured_envelope() for _ in range(2)]
    assert mini_sentry.captured_envelopes.empty()
    cpu, gpu = None, None
    for envelope in envelopes:
        if "event.nv_gpudmp" in attachment_types(envelope):
            gpu = envelope
        else:
            cpu = envelope
    assert cpu is not None and gpu is not None

    # The CPU event keeps the minidump; the GPU attachments moved off it.
    cpu_types = attachment_types(cpu)
    assert "event.minidump" in cpu_types
    assert "event.nv_gpudmp" not in cpu_types
    assert "event.nv_shader_debug" not in cpu_types

    # The GPU event carries the dump and shader debug, but not the minidump.
    gpu_types = attachment_types(gpu)
    assert {"event.nv_gpudmp", "event.nv_shader_debug"} <= gpu_types
    assert "event.minidump" not in gpu_types

    # The GPU event is its own (billed) event with a distinct id.
    assert UUID(cpu.headers["event_id"]) == cpu_event_id
    assert cpu.headers["event_id"] == SCOPE_EVENT_ID
    assert cpu.headers["event_id"] != gpu.headers["event_id"]

    # The GPU event is a copy of the scope, so it shares the CPU event's trace,
    # release and tags.
    cpu_event = cpu.get_event()
    gpu_event = gpu.get_event()
    assert gpu_event["release"] == "game@1.0.0"
    assert gpu_event["environment"] == "prod"
    assert gpu_event["contexts"]["trace"]["trace_id"] == SCOPE_TRACE_ID
    assert gpu_event["tags"] == cpu_event["tags"]
    assert cpu_event["contexts"]["trace"]["trace_id"] == SCOPE_TRACE_ID


def test_gpu_crash_not_split_without_feature(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    relay.send_minidump(
        project_id=project_id,
        files=[MINIDUMP, scope_attachment(), GPU_DUMP, SHADER_DBG],
    )

    # No split: the GPU dump rides along on the single CPU event.
    envelope = mini_sentry.get_captured_envelope()
    assert {"event.minidump", "event.nv_gpudmp"} <= attachment_types(envelope)
    assert mini_sentry.captured_envelopes.empty()


def test_gpu_crash_not_split_without_scope(mini_sentry, relay):
    # Without a scope (`__sentry-event`) there is nothing to copy the GPU event from,
    # so the crash stays on the CPU event rather than being dropped.
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    config["config"]["features"] = [GPU_FEATURE]
    relay = relay(mini_sentry)

    relay.send_minidump(project_id=project_id, files=[MINIDUMP, GPU_DUMP, SHADER_DBG])

    envelope = mini_sentry.get_captured_envelope()
    assert {"event.minidump", "event.nv_gpudmp"} <= attachment_types(envelope)
    assert mini_sentry.captured_envelopes.empty()


def test_gpu_crash_not_split_without_gpu_dump(mini_sentry, relay):
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    config["config"]["features"] = [GPU_FEATURE]
    relay = relay(mini_sentry)

    relay.send_minidump(project_id=project_id, files=[MINIDUMP, scope_attachment()])

    envelope = mini_sentry.get_captured_envelope()
    assert "event.nv_gpudmp" not in attachment_types(envelope)
    assert mini_sentry.captured_envelopes.empty()


def test_gpu_crash_split_with_processing(
    mini_sentry, relay_with_processing, attachments_consumer
):
    """The split survives full processing: both events reach the store."""
    project_id = 42
    config = mini_sentry.add_full_project_config(project_id)
    config["config"]["features"] = [GPU_FEATURE]

    relay = relay_with_processing()
    attachments_consumer = attachments_consumer()

    relay.send_minidump(
        project_id=project_id,
        files=[MINIDUMP, scope_attachment(), GPU_DUMP, SHADER_DBG],
    )

    # Both events reach processing; `get_event_only` skips the attachment chunks.
    events = {}
    for _ in range(2):
        message, event = attachments_consumer.get_event_only()
        names = {att["name"] for att in message.get("attachments", [])}
        kind = "gpu" if "crash.nv-gpudmp" in names else "cpu"
        events[kind] = (event, message)
    assert set(events) == {"cpu", "gpu"}

    cpu_event, _ = events["cpu"]
    gpu_event, gpu_message = events["gpu"]

    # The CPU event is the minidump crash.
    assert cpu_event["exception"]["values"][0]["mechanism"]["type"] == "minidump"

    # The GPU event is the copied scope, carrying the GPU crash attachments.
    assert gpu_event["event_id"] != cpu_event["event_id"]
    assert gpu_event["release"] == "game@1.0.0"
    gpu_names = {att["name"] for att in gpu_message.get("attachments", [])}
    assert {"crash.nv-gpudmp", "shader-abc.nvdbg"} <= gpu_names
    assert "minidump.dmp" not in gpu_names
