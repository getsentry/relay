import queue

import pytest


def test_store(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.wait_relay_healthcheck()

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1)

    assert event["message"] == "Hello, World!"


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    payload = b"eJytVctu2zAQ/BWDFzuAJYt6WVIfaAsE6KFBi6K3IjAoiXIYSyRLUm7cwP/eJaXEcZr0Bd/E5e7OzJIc3aKOak3WFBXoXCmhislOTDqiNmiO6E1FpWGCo+LrLTI7eZ8Fm1vS9nZ9SNeGVBujSAXhW9QoAq1dZcNaymEF2aUQRkOOXHFRU/9aQ13LOOUCFSkO56gSrf2O5qjpeTWAI963rf+ScMF3nej1ayhifEWkREVDWk3nqBN13/4KgPbzv4bHOb6Hx+kRPihTppf/DTukPVKbRwe44AjuYkhXPb8gjP8Gdfz4C7Q4Xz4z2xFs1QpSnwQqCZKDsPAIy6jdAPfhZGDpASwKnxJ2Ml1p+qcDW9EbQ7mGmPaH2hOgJg8exdOolegkNPlnuIVUbEsMXZhOLuy19TRfMF7Tm0d3555AGB8R+Fhe08o88zCN6h9ScH1hWyoKhLmBUYE3gIuoyWeypXzyaqLot54pOpsqG5ievYB0t+dDQcPWs+mVMVIXi0WSZDQgASF108Q4xqSMaUmDKkuzrEzD5E29Vgx8jSpvWQZ5sizxMgqbKCMJDYPEp73P10psfCYWGE/PfMbhibftzGGiSyvYUVzZGQD7kQaRplf0/M4WZ5x+nzg/nE1HG5yeuRZSaPNA5uX+cr+HrmAQXJO78bmRTIiZPDnHHtiDj+6hiqz18AXdFLHm6kymQNvMx9iP4GBRqSipK9V3pc0d3Fk76Dmyg6XaDD2GE3FJbs7QJvRTaGJFiw2zfQM/8jEEDOto7YkeSlHsBy7mXN4bbR4yIRpYuj2rYR3B2i67OnGNQ1dTqZ00Y3Zo11dEUV49iDDtlX3TWMkI+9hPrSaYwJaq1Xhd35Mfb70LUr0Dlt4nJTycwOOuSGv/VCDErByDNE/iZZLXQY3zOAnDvElpjJcJTXCUZSEZZYGMTlqKAc68IPPC5RccwQUvgsDdUmGPxJKx/GVLTCNUZ39Fzt5/AgZYWKw="  # noqa
    relay.send_event(42, payload)

    event = mini_sentry.captured_events.get(timeout=1)

    assert event["message"] == "Error: yo mark"


def test_store_pii_stripping(mini_sentry, relay):
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42, {"message": "test@mail.org"})

    event = mini_sentry.captured_events.get(timeout=2)

    # Email should be stripped:
    assert event["message"] == "[email]"


def test_event_timeout(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1.5)  # Causes the first event to drop, but not the second one
        return get_project_config_original()

    relay = relay(mini_sentry, {"cache": {"event_expiry": 1}})
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42, {"message": "invalid"})
    sleep(1)  # Sleep so that the second event also has to wait but succeeds
    relay.send_event(42, {"message": "correct"})

    assert mini_sentry.captured_events.get(timeout=1)["message"] == "correct"
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))
