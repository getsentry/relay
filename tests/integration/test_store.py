import json
import os
import queue

import pytest


def test_store(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.wait_relay_healthcheck()

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1)

    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    payload = b"eJytVctu2zAQ/BWDFzuAJYt6WVIfaAsE6KFBi6K3IjAoiXIYSyRLUm7cwP/eJaXEcZr0Bd/E5e7OzJIc3aKOak3WFBXoXCmhislOTDqiNmiO6E1FpWGCo+LrLTI7eZ8Fm1vS9nZ9SNeGVBujSAXhW9QoAq1dZcNaymEF2aUQRkOOXHFRU/9aQ13LOOUCFSkO56gSrf2O5qjpeTWAI963rf+ScMF3nej1ayhifEWkREVDWk3nqBN13/4KgPbzv4bHOb6Hx+kRPihTppf/DTukPVKbRwe44AjuYkhXPb8gjP8Gdfz4C7Q4Xz4z2xFs1QpSnwQqCZKDsPAIy6jdAPfhZGDpASwKnxJ2Ml1p+qcDW9EbQ7mGmPaH2hOgJg8exdOolegkNPlnuIVUbEsMXZhOLuy19TRfMF7Tm0d3555AGB8R+Fhe08o88zCN6h9ScH1hWyoKhLmBUYE3gIuoyWeypXzyaqLot54pOpsqG5ievYB0t+dDQcPWs+mVMVIXi0WSZDQgASF108Q4xqSMaUmDKkuzrEzD5E29Vgx8jSpvWQZ5sizxMgqbKCMJDYPEp73P10psfCYWGE/PfMbhibftzGGiSyvYUVzZGQD7kQaRplf0/M4WZ5x+nzg/nE1HG5yeuRZSaPNA5uX+cr+HrmAQXJO78bmRTIiZPDnHHtiDj+6hiqz18AXdFLHm6kymQNvMx9iP4GBRqSipK9V3pc0d3Fk76Dmyg6XaDD2GE3FJbs7QJvRTaGJFiw2zfQM/8jEEDOto7YkeSlHsBy7mXN4bbR4yIRpYuj2rYR3B2i67OnGNQ1dTqZ00Y3Zo11dEUV49iDDtlX3TWMkI+9hPrSaYwJaq1Xhd35Mfb70LUr0Dlt4nJTycwOOuSGv/VCDErByDNE/iZZLXQY3zOAnDvElpjJcJTXCUZSEZZYGMTlqKAc68IPPC5RccwQUvgsDdUmGPxJKx/GVLTCNUZ39Fzt5/AgZYWKw="  # noqa
    relay.send_event(42, payload)

    event = mini_sentry.captured_events.get(timeout=1)

    assert event["logentry"] == {"formatted": "Error: yo mark"}


def test_store_pii_stripping(mini_sentry, relay):
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42, {"message": "test@mail.org"})

    event = mini_sentry.captured_events.get(timeout=2)

    # Email should be stripped:
    assert event["logentry"] == {"formatted": "[email]"}


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

    assert mini_sentry.captured_events.get(timeout=1)["logentry"] == {
        "formatted": "correct"
    }
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


def test_rate_limit(mini_sentry, relay):
    from time import sleep

    store_event_original = mini_sentry.app.view_functions["store_event"]

    rate_limit_sent = False

    @mini_sentry.app.endpoint("store_event")
    def store_event():
        # Only send a rate limit header for the first request. If relay sends a
        # second request to mini_sentry, we want to see it so we can log an error.
        nonlocal rate_limit_sent
        if rate_limit_sent:
            store_event_original()
        else:
            rate_limit_sent = True
            return "", 429, {"retry-after": "2"}

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[42] = relay.basic_project_config()

    # This message should return the initial 429 and start rate limiting
    relay.send_event(42, {"message": "rate limit"})

    # This event should get dropped by relay. We expect 429 here
    sleep(1)
    relay.send_event(42, {"message": "invalid"})

    # This event should arrive
    sleep(2)
    relay.send_event(42, {"message": "correct"})

    assert mini_sentry.captured_events.get(timeout=1)["logentry"] == {
        "formatted": "correct"
    }


def test_static_config(mini_sentry, relay):
    from time import sleep

    project_config = mini_sentry.basic_project_config()

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join("42.json").write(json.dumps(project_config))

    relay = relay(mini_sentry, prepare=configure_static_project)
    mini_sentry.project_configs[42] = project_config
    sleep(1)  # There is no upstream auth, so just wait for relay to initialize

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1)
    assert event["logentry"] == {"formatted": "Hello, World!"}

    sleep(1)  # Regression test: Relay tried to issue a request for 0 states
    if mini_sentry.test_failures:
        raise AssertionError(
            f"Exceptions happened in mini_sentry: {mini_sentry.test_failures}"
        )


def test_event_buffer_size(mini_sentry, relay):
    relay = relay(mini_sentry, {"cache": {"event_buffer_size": 0}})
    relay.wait_relay_healthcheck()
    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42, {"message": "pls ignore"})
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=5))


def test_max_concurrent_requests(mini_sentry, relay):
    from time import sleep
    from threading import Semaphore

    processing_store = False
    store_count = Semaphore()

    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()

    @mini_sentry.app.endpoint("store_event")
    def store_event():
        nonlocal processing_store
        nonlocal store_event
        assert not processing_store

        processing_store = True
        # sleep long, but less than event_buffer_expiry
        sleep(0.5)
        store_count.release()
        sleep(0.5)
        processing_store = False

        return "ok"

    relay = relay(
        mini_sentry,
        {
            "limits": {"max_concurrent_requests": 1},
            "cache": {"event_buffer_expiry": 2},
        },
    )
    relay.wait_relay_healthcheck()

    relay.send_event(42)
    relay.send_event(42)

    store_count.acquire(timeout=4)
    store_count.acquire(timeout=4)
