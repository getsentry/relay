"""
Tests the keda endpoint.
"""


def test_basic_keda(mini_sentry, relay):
    relay = relay(mini_sentry)
    response = relay.get("/api/relay/autoscaling/")
    assert response.status_code == 200
    assert "up 1" in response.text
