"""
Test the health check endpoints
"""

import time


def failing_check_challenge(*args, **kwargs):
    return 'fail', 400


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
    """Internal endpoint used by kubernetes """
    relay = relay(mini_sentry)
    response = relay.get("/api/relay/healthcheck/live/")
    assert response.status_code == 200


def test_external_live(mini_sentry, relay):
    """Endpoint called by a downstream to see if it has network connection to the upstream. """
    relay = relay(mini_sentry)
    response = relay.get("/api/0/relays/live/")
    assert response.status_code == 200


def test_readiness(mini_sentry, relay):
    """Internal endpoint used by kubernetes """
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(mini_sentry, wait_healthcheck=False)
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 503

        mini_sentry.app.view_functions["check_challenge"] = original_check_challenge

        relay.wait_relay_healthcheck()
        response = relay.get("/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()


def test_readiness_flag(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(mini_sentry, {"relay": {"ready": "always"}}, wait_healthcheck=False)
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()

def test_readiness_proxy(mini_sentry, relay):
    mini_sentry.app.view_functions["check_challenge"] = failing_check_challenge

    try:
        relay = relay(mini_sentry, {"relay": {"mode": "proxy"}}, wait_healthcheck=False)
        response = wait_get(relay, "/api/relay/healthcheck/ready/")
        assert response.status_code == 200
    finally:
        # Authentication failures would fail the test
        mini_sentry.test_failures.clear()
