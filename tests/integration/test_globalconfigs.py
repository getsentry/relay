"""
Tests the global_config endpoint (/api/0/relays/projectconfigs/)
"""


def test_global_config_deserialization(mini_sentry, relay):
    """Verify Relay can deserialize the global config Sentry returns."""
    mini_sentry.add_basic_global_config()
    assert mini_sentry.global_config["measurements"] is not None
    # Relay requests global config at start-up
    relay = relay(mini_sentry, wait_health_check=True)
