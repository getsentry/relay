import json
import os
import signal
import socket
import time
import tempfile

import pytest


@pytest.mark.parametrize("storage", ["ephemeral", "permanent"])
def test_graceful_shutdown(mini_sentry, relay, storage):
    """
    On SIGTERM, relay completes any in-flight HTTP requests before shutting down.
    New connections are rejected once the TCP listener closes.

    We test this by holding a request in-flight at the TCP level: send the HTTP
    headers and the first byte of the body, then send SIGTERM, then send the rest
    of the body.  Relay must process and respond to the request before it exits.
    """
    is_ephemeral = storage == "ephemeral"
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    with tempfile.TemporaryDirectory() as db_dir:
        db_file_path = os.path.join(db_dir, "database.db")

        relay = relay(
            mini_sentry,
            options={
                "limits": {"shutdown_timeout": 5},
                "spool": {
                    "envelopes": {"path": db_file_path, "ephemeral": is_ephemeral}
                },
            },
        )

        host, port = relay.server_address
        dsn_key = relay.get_dsn_public_key(project_id)

        body = json.dumps({"message": "in-flight during shutdown"}).encode()
        request = (
            f"POST /api/{project_id}/store/ HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Content-Type: application/json\r\n"
            f"X-Sentry-Auth: Sentry sentry_version=7, sentry_key={dsn_key}\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode() + body

        # Open a raw TCP connection and send everything up to the last byte.
        # Relay is now holding the connection open waiting for the body to complete —
        # the request is in-flight.
        sock = socket.create_connection((host, port))
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.sendall(request[:-1])

        # Trigger graceful shutdown while the request is in-flight.
        relay.process.send_signal(signal.SIGTERM)

        # Wait until relay has actually entered shutdown by trying to open new
        # TCP connections.  The first thing graceful shutdown does is close the
        # TCP listener, so `create_connection` will raise "connection refused"
        # once shutdown has started — no arbitrary sleep needed.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                probe = socket.create_connection((host, port), timeout=1)
                probe.close()
            except OSError:
                break  # Connection refused — listener is closed, shutdown started
            time.sleep(0.01)  # throttle probes to avoid hot-spinning on CI

        # Complete the request — relay will respond with Service Unavailable.
        sock.sendall(request[-1:])

        sock.settimeout(10)
        response = b""
        while chunk := sock.recv(4096):
            response += chunk
        sock.close()

        expected_status_code = b"HTTP/1.1 200" if is_ephemeral else b"HTTP/1.1 503"
        assert expected_status_code in response

        if is_ephemeral:
            envelope = mini_sentry.get_captured_envelope()
            assert (
                envelope.get_event()["logentry"]["formatted"]
                == "in-flight during shutdown"
            )
        else:
            assert mini_sentry.captured_envelopes.empty()
