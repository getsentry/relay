"""
Test the health check endpoints
"""

import time
import tempfile
import os

import pytest
from requests import HTTPError


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
    finally:
        mini_sentry.clear_test_failures()

    response = relay.get("/api/relay/healthcheck/ready/")
    assert response.status_code == 200


def test_readiness_flag(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(
            mini_sentry, {"auth": {"ready": "always"}}, wait_health_check=False
        )
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        mini_sentry.clear_test_failures()


def test_readiness_proxy(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    relay = relay(mini_sentry, {"relay": {"mode": "proxy"}}, wait_health_check=False)
    response = wait_get(relay, "/api/relay/healthcheck/ready/")
    assert response.status_code == 200


def assert_not_enough_memory(relay, mini_sentry):
    response = wait_get(relay, "/api/relay/healthcheck/ready/")
    assert response.status_code == 503
    errors = ""
    for _ in range(10):
        errors += f"{mini_sentry.test_failures.get(timeout=1)}\n"
        if (
            "Not enough memory" in errors
            and ">= 42" in errors
            and "Health check probe 'system memory'" in errors
            and "Health check probe 'spool health'" in errors
        ):
            return

    assert False, f"Not all errors represented: {errors}"


@pytest.mark.parametrize("i", range(10))  # remove before merge
def test_readiness_not_enough_memory_bytes(mini_sentry, relay, i):
    relay = relay(
        mini_sentry,
        {"relay": {"mode": "proxy"}, "health": {"max_memory_bytes": 42}},
        wait_health_check=False,
    )

    assert_not_enough_memory(relay, mini_sentry)


@pytest.mark.parametrize("i", range(10))  # remove before merge
def test_readiness_not_enough_memory_percent(mini_sentry, relay, i):
    relay = relay(
        mini_sentry,
        {"relay": {"mode": "proxy"}, "health": {"max_memory_percent": 0.01}},
        wait_health_check=False,
    )

    assert_not_enough_memory(relay, mini_sentry)


def test_readiness_depends_on_aggregator_being_full(mini_sentry, relay):
    relay = relay(
        mini_sentry,
        {"relay": {"mode": "proxy"}, "aggregator": {"max_total_bucket_bytes": 0}},
        wait_health_check=False,
    )

    response = wait_get(relay, "/api/relay/healthcheck/ready/")

    error = str(mini_sentry.test_failures.get(timeout=1))
    assert "Health check probe 'aggregator'" in error
    assert response.status_code == 503


def test_readiness_depends_on_aggregator_being_full_after_metrics(mini_sentry, relay):
    relay = relay(
        mini_sentry,
        {"aggregator": {"max_total_bucket_bytes": 1}},
    )

    metrics_payload = "transactions/foo:42|c\ntransactions/bar:17|c"
    relay.send_metrics(42, metrics_payload)

    for _ in range(100):
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        if response.status_code == 503:
            error = str(mini_sentry.test_failures.get(timeout=1))
            assert "aggregator limit exceeded" in error
            error = str(mini_sentry.test_failures.get(timeout=1))
            assert "Health check probe 'aggregator'" in error
            return
        time.sleep(0.1)

    assert False, "health check never failed"


def test_readiness_disk_spool(mini_sentry, relay):
    mini_sentry.fail_on_relay_error = False
    temp = tempfile.mkdtemp()
    dbfile = os.path.join(temp, "buffer.db")

    project_key = 42
    mini_sentry.add_full_project_config(project_key)
    # Set the broken config, so we won't be able to dequeue the envelopes.
    config = mini_sentry.project_configs[project_key]["config"]
    config["quotas"] = None

    relay_config = {
        "health": {
            "refresh_interval_ms": 100,
        },
        "spool": {
            # if the config contains max_disk_size and max_memory_size set both to 0, Relay will never passes readiness check
            "envelopes": {
                "version": "experimental",
                "path": dbfile,
                "max_disk_size": 24577,  # one more than the initial size
                "disk_batch_size": 1,
                "max_batches": 1,
            }
        },
    }

    relay = relay(mini_sentry, relay_config)

    # Second sent event can trigger error on the relay size, since the spool is full now.
    for _ in range(20):
        # It takes ~10 events to make SQLlite use more pages.
        try:
            relay.send_event(project_key)
        except HTTPError as e:
            # Might already reject responses
            assert e.response.status_code == 503

    time.sleep(2.0)

    response = wait_get(relay, "/api/relay/healthcheck/ready/")
    assert response.status_code == 503
