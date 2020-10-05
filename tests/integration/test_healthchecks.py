"""
Test the health check endpoints
"""


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


def test_is_healthy(mini_sentry, relay):
    """Internal endpoint used by kubernetes """
    relay = relay(mini_sentry)
    # NOTE this is redundant but palced here to clearly show the exposed endpoint
    # (internally the relay fixture waits for the ready health check anyway)
    response = relay.get("/api/relay/healthcheck/ready/")
    assert response.status_code == 200
