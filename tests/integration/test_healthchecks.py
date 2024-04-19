"""
Test the health check endpoints
"""

import time
import tempfile
import os


def failing_check_challenge(*args, **kwargs):
    return "fail", 400


def wait_get(server, path):
    """Waits until the server listens to requests and returns the response."""
    backoff = 0.1
    while True:
        try:
            return server.get(path)
        except Exception:
            time.sleep(backoff)
            if backoff > 2:
                raise
            backoff *= 2


def test_live(mini_sentry, relay):
    """Internal endpoint used by kubernetes"""
    relay = relay(mini_sentry)
    response = relay.get("/api/relay/healthcheck/live/")
    assert response.status_code == 200


def test_external_live(mini_sentry, relay):
    """Endpoint called by a downstream to see if it has network connection to the upstream."""
    relay = relay(mini_sentry)
    response = relay.get("/api/0/relays/live/")
    assert response.status_code == 200


def test_readiness(mini_sentry, relay):
    """Internal endpoint used by kubernetes"""
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(mini_sentry, wait_health_check=False)
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 503

        mini_sentry.app.view_functions["check_challenge"] = original_check_challenge

        relay.wait_relay_health_check()
        response = relay.get("/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_flag(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(
            mini_sentry, {"auth": {"ready": "always"}}, wait_health_check=False
        )
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_proxy(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(
            mini_sentry, {"relay": {"mode": "proxy"}}, wait_health_check=False
        )
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_not_enough_memory_bytes(mini_sentry, relay):
    try:
        relay = relay(
            mini_sentry,
            {"health": {"max_memory_bytes": 42}},
            wait_health_check=False,
        )

        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        time.sleep(0.3)  # Wait for error
        error = str(mini_sentry.test_failures.pop(0))
        assert "Not enough memory" in error and ">= 42" in error
        error = str(mini_sentry.test_failures.pop(0))
        assert "Health check probe 'system memory'" in error
        assert response.status_code == 503
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_not_enough_memory_percent(mini_sentry, relay):
    try:
        relay = relay(
            mini_sentry,
            {"health": {"max_memory_percent": 0.01}},
            wait_health_check=False,
        )
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        time.sleep(0.3)  # Wait for error
        error = str(mini_sentry.test_failures.pop(0))
        assert "Not enough memory" in error and ">= 1.00%" in error
        error = str(mini_sentry.test_failures.pop(0))
        assert "Health check probe 'system memory'" in error
        assert response.status_code == 503
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_depends_on_aggregator_being_full(mini_sentry, relay):
    try:
        relay = relay(
            mini_sentry,
            {"aggregator": {"max_total_bucket_bytes": 0}},
            wait_health_check=False,
        )

        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        time.sleep(0.3)  # Wait for error
        error = str(mini_sentry.test_failures.pop())
        assert "Health check probe 'aggregator'" in error
        assert response.status_code == 503
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_disk_spool(mini_sentry, relay):
    @mini_sentry.app.endpoint("store_internal_error_event")
    def store_internal_error_event():
        return {}

    try:
        temp = tempfile.mkdtemp()
        dbfile = os.path.join(temp, "buffer.db")

        project_key = 42
        mini_sentry.add_full_project_config(project_key)
        # Set the broken config, so we won't be able to dequeue the envelopes.
        config = mini_sentry.project_configs[project_key]["config"]
        config["quotas"] = None

        relay_config = {
            "spool": {
                # if the config contains max_disk_size and max_memory_size set both to 0, Relay will never passes readiness check
                "envelopes": {
                    "path": dbfile,
                    "max_memory_size": 0,
                    "max_disk_size": "24577",  # one more than the initial size
                }
            },
        }

        relay = relay(
            mini_sentry,
            relay_config,
            wait_health_check=True,
        )

        # Second sent event can trigger error on the relay size, since the spool is full now.
        # Wrapping this into the try block, to make sure we ignore those errors and just check the health at the end.
        try:
            # These events will consume all the disk space and we will report not ready.
            for i in range(20):
                # It takes ~10 events to make SQLlite use more pages.
                relay.send_event(project_key)
        finally:
            # Authentication failures would fail the test
            mini_sentry.test_failures.clear()

        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 503
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()
