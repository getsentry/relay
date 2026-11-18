import json
import os
import signal
import socket
import time
import tempfile

import pytest

from sentry_relay.consts import DataCategory

from .consts import Outcome


@pytest.mark.parametrize("storage", ["memory", "ephemeral", "permanent"])
def test_graceful_shutdown(mini_sentry, relay, storage):
    """
    On SIGTERM, relay completes any in-flight HTTP requests before shutting down.
    New connections are rejected once the TCP listener closes.

    We wait for a 100 Continue response to confirm Relay has accepted the request,
    then send an incomplete body, SIGTERM, and the rest of the body. Relay must
    process and respond to the request before it exits.
    """
    relay_factory = relay
    is_ephemeral = storage != "permanent"
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    with tempfile.TemporaryDirectory() as db_dir:
        db_file_path = (
            None if storage == "memory" else os.path.join(db_dir, "database.db")
        )

        options = {
            "spool": {"envelopes": {"path": db_file_path, "ephemeral": is_ephemeral}},
        }
        relay = relay_factory(mini_sentry, options=options)

        # Send an event and wait for it--to make sure that things are stood up on the Relay side,
        # like the EnvelopeProcessingBuffer.
        relay.send_event(project_id)
        event = mini_sentry.get_captured_envelope().get_event()
        assert event["logentry"] == {"formatted": "Hello, World!"}

        # At this point, we're ready to send an incomplete request.
        host, port = relay.server_address
        dsn_key = relay.get_dsn_public_key(project_id)

        body = json.dumps({"message": "in-flight during shutdown"}).encode()
        headers = (
            f"POST /api/{project_id}/store/ HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Content-Type: application/json\r\n"
            f"X-Sentry-Auth: Sentry sentry_version=7, sentry_key={dsn_key}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"Expect: 100-continue\r\n"
            f"\r\n"
        ).encode()

        # Confirm Relay is reading the request before signaling shutdown. Merely
        # sending bytes does not guarantee the connection has been accepted yet.
        sock = socket.create_connection((host, port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(10)
        sock.sendall(headers)
        interim_response = b""
        while b"\r\n\r\n" not in interim_response:
            chunk = sock.recv(4096)
            assert chunk, "connection closed before request acceptance"
            interim_response += chunk
        assert interim_response == b"HTTP/1.1 100 Continue\r\n\r\n"

        sock.sendall(body[:-1])

        # Trigger graceful shutdown while the request is in-flight.
        relay.process.send_signal(signal.SIGTERM)
        time.sleep(0.05)

        # Complete the request after shutdown has begun.
        sock.sendall(body[-1:])

        response = b""
        while chunk := sock.recv(4096):
            response += chunk
        sock.close()

        # The HTTP server retains the buffer until accepted requests finish,
        # including requests whose envelopes may be persisted during shutdown.
        assert b"HTTP/1.1 200" in response
        assert relay.wait_for_exit(timeout=10) == 0

        if not is_ephemeral:
            # A permanent buffer may have forwarded the event or persisted it.
            # Drain the reopened spool so duplicate deliveries cannot be hidden
            # by persisting them again at the next shutdown.
            options["spool"]["envelopes"]["ephemeral"] = True
            relay = relay_factory(mini_sentry, options=options)

        envelope = mini_sentry.get_captured_envelope()
        assert (
            envelope.get_event()["logentry"]["formatted"] == "in-flight during shutdown"
        )

        if not is_ephemeral:
            relay.send_signal(signal.SIGTERM)
            assert relay.wait_for_exit(timeout=10) == 0
        assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize(
    "emit_outcomes,global_metrics",
    [
        pytest.param(False, False, id="disabled"),
        pytest.param(True, False, id="outcomes"),
        pytest.param("as_client_reports", False, id="client-reports"),
        pytest.param(True, True, id="global-metrics"),
    ],
)
def test_graceful_shutdown_outcomes(mini_sentry, relay, emit_outcomes, global_metrics):
    """Shutdown drains the outcome pipeline, including its pending aggregates."""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["releases"] = {
        "releases": ["shutdown-test"]
    }
    relay = relay(
        mini_sentry,
        options={
            "outcomes": {"emit_outcomes": emit_outcomes},
            "http": {"global_metrics": global_metrics},
            # Outcomes must be flushed by shutdown, not by the periodic timer.
            "aggregator": {"initial_delay": 3600},
        },
    )

    relay.send_event(project_id, {"message": "filtered", "release": "shutdown-test"})
    relay.send_signal(signal.SIGTERM)

    if emit_outcomes == "as_client_reports":
        report = mini_sentry.get_client_report()
        assert report["filtered_events"] == [
            {"reason": "release-version", "category": "error", "quantity": 1}
        ]
    elif emit_outcomes:
        outcomes = mini_sentry.get_aggregated_outcomes(n=1)
        if global_metrics:
            assert outcomes[0].pop("public_key") == relay.get_dsn_public_key(project_id)
        assert outcomes == [
            {
                "outcome": Outcome.FILTERED,
                "reason": "release-version",
                "category": DataCategory.ERROR,
                "quantity": 1,
            }
        ]

    assert relay.wait_for_exit(timeout=10) == 0
    assert mini_sentry.captured_outcomes.empty()
    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize("emit_outcomes", [False, True, "as_client_reports"])
def test_graceful_shutdown_proxy(mini_sentry, relay, emit_outcomes):
    """The proxy processor and its outcome producer must both stop."""
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(
        mini_sentry,
        options={
            "relay": {"mode": "proxy"},
            "outcomes": {"emit_outcomes": emit_outcomes},
        },
    )

    event = {"event_id": "515539018c9b4260a6f999572f1661ee", "message": "shutdown"}
    relay.send_event(project_id, event)
    relay.send_signal(signal.SIGTERM)
    assert mini_sentry.get_captured_envelope().get_event() == event
    assert relay.wait_for_exit(timeout=10) == 0
    assert mini_sentry.captured_envelopes.empty()


def test_graceful_shutdown_internal_server(mini_sentry, relay, random_port):
    """Both HTTP listeners must release their services during shutdown."""
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(
        mini_sentry,
        options={"relay": {"internal_port": random_port()}},
    )

    relay.send_event(project_id)
    relay.send_signal(signal.SIGTERM)
    event = mini_sentry.get_captured_envelope().get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
    assert relay.wait_for_exit(timeout=10) == 0


@pytest.mark.parametrize("use_objectstore", [False, True])
def test_graceful_shutdown_processing(
    mini_sentry,
    relay_with_processing,
    attachments_consumer,
    objectstore,
    use_objectstore,
):
    """Drain accepted attachments through objectstore or Kafka before exiting."""
    project_id = 42
    event_id = "515539018c9b4260a6f999572f1661ee"
    mini_sentry.add_full_project_config(project_id)

    options = {"processing": {"attachment_chunk_size": "100KB"}}
    if not use_objectstore:
        options["processing"]["objectstore"] = {"objectstore_url": None}

    attachments = attachments_consumer()
    relay = relay_with_processing(options)
    # Exceed the chunk size so the Kafka fallback cannot inline the attachment.
    contents = b"pending attachment" * 20_000
    relay.send_attachments(
        project_id, event_id, [("attachment", "shutdown.txt", contents)]
    )
    relay.send_signal(signal.SIGTERM)

    chunk_ids = []
    if not use_objectstore:
        received = bytearray()
        while len(received) < len(contents):
            chunk, message = attachments.get_attachment_chunk()
            assert message["chunk_index"] == len(chunk_ids)
            chunk_ids.append(message["id"])
            received.extend(chunk)
        assert received == contents
        assert len(chunk_ids) > 1

    message = attachments.get_individual_attachment()
    attachment = message["attachment"]
    attachment_id = attachment.pop("id")
    assert attachment_id
    if use_objectstore:
        stored = objectstore("attachments", project_id).get(attachment.pop("stored_id"))
        assert stored.payload.read() == contents
        assert stored.metadata.filename == "shutdown.txt"
        assert stored.metadata.content_type == "text/plain"
        assert attachment.pop("content_type") == "text/plain"
    else:
        assert set(chunk_ids) == {attachment_id}
        assert attachment.pop("chunks") == len(chunk_ids)

    assert message == {
        "type": "attachment",
        "attachment": {
            "name": "shutdown.txt",
            "rate_limited": False,
            "attachment_type": "event.attachment",
            "size": len(contents),
            "retention_days": 90,
        },
        "event_id": event_id,
        "project_id": project_id,
    }
    assert relay.wait_for_exit(timeout=10) == 0
