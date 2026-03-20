import json
import os
import signal
import socket
import time
import tempfile


def test_graceful_shutdown(mini_sentry, relay):
    """
    On SIGTERM, relay completes any in-flight HTTP requests before shutting down.
    New connections are rejected once the TCP listener closes.

    We test this by holding a request in-flight at the TCP level: send the HTTP
    headers and the first byte of the body, then send SIGTERM, then send the rest
    of the body.  Relay must process and respond to the request before it exits.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    with tempfile.TemporaryDirectory() as db_dir:
        db_file_path = os.path.join(db_dir, "database.db")

        relay = relay(
            mini_sentry,
            options={
                "limits": {"shutdown_timeout": 5},
                "spool": {"envelopes": {"path": db_file_path}},
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
        time.sleep(0.05)

        # Complete the request — relay will respond with Service Unavailable.
        sock.sendall(request[-1:])

        sock.settimeout(10)
        response = b""
        while chunk := sock.recv(4096):
            response += chunk
        sock.close()

        assert b"HTTP/1.1 503" in response

        # After relay exits, new connections are refused.
        relay.wait_for_exit(timeout=10)
        try:
            socket.create_connection((host, port))
            assert False, "Expected connection to be refused after relay exited"
        except ConnectionRefusedError:
            pass
