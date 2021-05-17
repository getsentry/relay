import json
import os
import queue
import datetime
import uuid
import six
import socket
import threading
import pytest
import time

from requests.exceptions import HTTPError
from flask import abort, Response, jsonify, request as flask_request


def test_store(mini_sentry, relay_chain):
    relay = relay_chain()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}


@pytest.mark.parametrize("allowed", [True, False])
def test_store_external_relay(mini_sentry, relay, allowed):
    # Use 3 Relays to force the middle one to fetch public keys
    relay = relay(relay(relay(mini_sentry)), external=True)

    project_config = mini_sentry.add_basic_project_config(42)

    if allowed:
        # manually  add all public keys form the relays to the configuration
        project_config["config"]["trustedRelays"] = list(relay.iter_public_keys())

    # Send the event, which always succeeds. The project state is fetched asynchronously and Relay
    # drops the event internally if it does not have permissions.
    relay.send_event(42)

    if allowed:
        event = mini_sentry.captured_events.get(timeout=1).get_event()
        assert event["logentry"] == {"formatted": "Hello, World!"}
    else:
        pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


def test_legacy_store(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.add_basic_project_config(42)

    relay.send_event(42, legacy=True)
    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}

    relay.send_event(42, legacy=True)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


@pytest.mark.parametrize(
    "filter_config, should_filter",
    [
        ({"errorMessages": {"patterns": ["Panic: originalCreateNotification"]}}, True),
        ({"errorMessages": {"patterns": ["Warning"]}}, False),
    ],
    ids=["error messages filtered", "error messages not filtered",],
)
def test_filters_are_applied(
    mini_sentry, relay_with_processing, events_consumer, filter_config, should_filter,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """
    events_consumer = events_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    for key in filter_config.keys():
        filter_settings[key] = filter_config[key]

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.utcnow()
    message_text = "some message {}".format(now.isoformat())

    event = {
        "message": message_text,
        "exception": {
            "values": [{"type": "Panic", "value": "originalCreateNotification"}]
        },
    }

    relay.send_event(project_id, event)

    if should_filter:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()


@pytest.mark.parametrize(
    "is_enabled, should_filter",
    [(True, True), (False, False),],
    ids=["web crawlers filtered", "web crawlers not filtered",],
)
def test_web_crawlers_filter_are_applied(
    mini_sentry, relay_with_processing, events_consumer, is_enabled, should_filter,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """
    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["webCrawlers"] = {"isEnabled": is_enabled}

    # UA parsing introduces higher latency in debug mode
    events_consumer = events_consumer(timeout=5)

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.utcnow()
    message_text = "some message {}".format(now.isoformat())

    event = {
        "message": message_text,
        "request": {"headers": {"User-Agent": "BingBot",}},
    }

    relay.send_event(project_id, event)

    if should_filter:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()


@pytest.mark.parametrize("method_to_test", [("GET", False), ("POST", True)])
def test_options_response(mini_sentry, relay, method_to_test):
    method, should_succeed = method_to_test
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    headers = {
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": "X-Sentry-Auth",
    }

    result = relay.send_options(project_id, headers)

    assert result.ok == should_succeed


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    payload = (
        b"eJytVctu2zAQ/BWDFzuAJYt6WVIfaAsE6KFBi6K3IjAoiXIYSyRLUm7cwP/eJaXEcZr0Bd"
        b"/E5e7OzJIc3aKOak3WFBXoXCmhislOTDqiNmiO6E1FpWGCo"
        b"+LrLTI7eZ8Fm1vS9nZ9SNeGVBujSAXhW9QoAq1dZcNaymEF2aUQRkOOXHFRU/9aQ13LOOUCFSkO56gSrf2O5qjpeTWAI963rf"
        b"+ScMF3nej1ayhifEWkREVDWk3nqBN13/4KgPbzv4bHOb6Hx+kRPihTppf"
        b"/DTukPVKbRwe44AjuYkhXPb8gjP8Gdfz4C7Q4Xz4z2xFs1QpSnwQqCZKDsPAIy6jdAPfhZGDpASwKnxJ2Ml1p"
        b"+qcDW9EbQ7mGmPaH2hOgJg8exdOolegkNPlnuIVUbEsMXZhOLuy19TRfMF7Tm0d3555AGB8R"
        b"+Fhe08o88zCN6h9ScH1hWyoKhLmBUYE3gIuoyWeypXzyaqLot54pOpsqG5ievYB0t+dDQcPWs"
        b"+mVMVIXi0WSZDQgASF108Q4xqSMaUmDKkuzrEzD5E29Vgx8jSpvWQZ5sizxMgqbKCMJDYPEp73P10psfCYWGE"
        b"/PfMbhibftzGGiSyvYUVzZGQD7kQaRplf0/M4WZ5x+nzg/nE1HG5yeuRZSaPNA5uX+cr+HrmAQXJO78bmRTIiZPDnHHtiDj"
        b"+6hiqz18AXdFLHm6kymQNvMx9iP4GBRqSipK9V3pc0d3Fk76Dmyg6XaDD2GE3FJbs7QJvRTaGJFiw2zfQM"
        b"/8jEEDOto7YkeSlHsBy7mXN4bbR4yIRpYuj2rYR3B2i67OnGNQ1dTqZ00Y3Zo11dEUV49iDDtlX3TWMkI"
        b"+9hPrSaYwJaq1Xhd35Mfb70LUr0Dlt4nJTycwOOuSGv/VCDErByDNE"
        b"/iZZLXQY3zOAnDvElpjJcJTXCUZSEZZYGMTlqKAc68IPPC5RccwQUvgsDdUmGPxJKx/GVLTCNUZ39Fzt5/AgZYWKw="
    )  # noqa
    relay.send_event(project_id, payload)

    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Error: yo mark"}


def test_store_pii_stripping(mini_sentry, relay):
    relay = relay(mini_sentry)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay.send_event(project_id, {"message": "hi", "extra": {"foo": "test@mail.org"}})

    event = mini_sentry.captured_events.get(timeout=2).get_event()

    # Email should be stripped:
    assert event["extra"]["foo"] == "[email]"


def test_store_timeout(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1.5)  # Causes the first event to drop, but not the second one
        return get_project_config_original()

    relay = relay(mini_sentry, {"cache": {"event_expiry": 1}})

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    try:
        relay.send_event(project_id, {"message": "invalid"})
        sleep(1)  # Sleep so that the second event also has to wait but succeeds
        relay.send_event(project_id, {"message": "correct"})

        event = mini_sentry.captured_events.get(timeout=1).get_event()
        assert event["logentry"] == {"formatted": "correct"}
        pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))
        ((route, error),) = mini_sentry.test_failures
        assert route == "/api/666/envelope/"
        assert "configured lifetime" in str(error)
    finally:
        mini_sentry.test_failures.clear()


def test_store_rate_limit(mini_sentry, relay):
    from time import sleep

    store_event_original = mini_sentry.app.view_functions["store_event"]

    rate_limit_sent = False

    @mini_sentry.app.endpoint("store_event")
    def store_event():
        # Only send a rate limit header for the first request. If relay sends a
        # second request to mini_sentry, we want to see it so we can log an error.
        nonlocal rate_limit_sent
        if rate_limit_sent:
            return store_event_original()
        else:
            rate_limit_sent = True
            return "", 429, {"retry-after": "2"}

    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    # This message should return the initial 429 and start rate limiting
    relay.send_event(project_id, {"message": "rate limit"})

    # This event should get dropped by relay. We expect 429 here
    sleep(1)
    with pytest.raises(HTTPError):
        relay.send_event(project_id, {"message": "invalid"})

    # This event should arrive
    sleep(2)
    relay.send_event(project_id, {"message": "correct"})

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "correct"}


def test_store_static_config(mini_sentry, relay):
    from time import sleep

    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join("{}.json".format(project_id)).write(
            json.dumps(project_config)
        )

    relay_options = {"relay": {"mode": "static"}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_static_project)

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}

    sleep(1)  # Regression test: Relay tried to issue a request for 0 states
    if mini_sentry.test_failures:
        raise AssertionError(
            f"Exceptions happened in mini_sentry: {mini_sentry.format_failures()}"
        )


def test_store_proxy_config(mini_sentry, relay):
    from time import sleep

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    def configure_proxy(dir):
        os.remove(dir.join("credentials.json"))

    relay_options = {"relay": {"mode": "proxy"}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_proxy)
    sleep(1)  # There is no upstream auth, so just wait for relay to initialize

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_store_buffer_size(mini_sentry, relay):
    relay = relay(mini_sentry, {"cache": {"event_buffer_size": 0}})
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    try:
        with pytest.raises(HTTPError):
            relay.send_event(project_id, {"message": "pls ignore"})
        pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))

        for (_, error) in mini_sentry.test_failures:
            assert isinstance(error, AssertionError)
            assert "Too many envelopes" in str(error)
    finally:
        mini_sentry.test_failures.clear()


def test_store_max_concurrent_requests(mini_sentry, relay):
    from time import sleep
    from threading import Semaphore

    processing_store = False
    store_count = Semaphore()

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    @mini_sentry.app.endpoint("store_event")
    def store_event():
        nonlocal processing_store
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
        {"limits": {"max_concurrent_requests": 1}, "cache": {"event_buffer_expiry": 2}},
    )

    relay.send_event(project_id)
    relay.send_event(project_id)

    store_count.acquire(timeout=2)
    store_count.acquire(timeout=2)


def test_store_not_normalized(mini_sentry, relay):
    """
    Tests that relay does not normalize when processing is disabled
    """
    relay = relay(mini_sentry, {"processing": {"enabled": False}})
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay.send_event(project_id, {"message": "some_message"})
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event.get("key_id") is None
    assert event.get("project") is None
    assert event.get("version") is None


def make_transaction(event):
    now = datetime.datetime.utcnow()
    event.update(
        {
            "type": "transaction",
            "timestamp": now.isoformat(),
            "start_timestamp": (now - datetime.timedelta(seconds=2)).isoformat(),
            "spans": [],
            "contexts": {
                "trace": {
                    "op": "hi",
                    "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                    "span_id": "968cff94913ebb07",
                }
            },
            "transaction": "hi",
        }
    )
    return event


def make_error(event):
    event.update(
        {
            "type": "error",
            "exception": {
                "values": [{"type": "ValueError", "value": "Should not happen"}]
            },
        }
    )
    return event


@pytest.mark.parametrize("event_type", ["default", "transaction"])
def test_processing(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    transactions_consumer,
    event_type,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """

    if event_type == "default":
        events_consumer = events_consumer()
    else:
        events_consumer = transactions_consumer()

    relay = relay_with_processing()
    project_id = 42
    mini_sentry.add_full_project_config(42)

    # create a unique message so we can make sure we don't test with stale data
    message_text = "some message {}".format(uuid.uuid4())
    event = {
        "message": message_text,
        "extra": {"msg_text": message_text},
    }

    if event_type == "transaction":
        make_transaction(event)

    relay.send_event(project_id, event)

    event, v = events_consumer.get_event()

    start_time = v.get("start_time")
    assert start_time is not None  # we have some start time field
    event_id = v.get("event_id")
    assert event_id is not None
    project_id = v.get("project_id")
    assert project_id is not None
    remote_addr = v.get("remote_addr")
    assert remote_addr is not None

    # check that we are actually retrieving the message that we sent
    assert event.get("extra") is not None
    assert event.get("extra").get("msg_text") is not None
    assert event["extra"]["msg_text"] == message_text

    # check that normalization ran
    assert event.get("key_id") is not None
    assert event.get("project") is not None
    assert event.get("version") is not None


# TODO: This parameterization should be unit-tested, instead
@pytest.mark.parametrize(
    "window,max_rate_limit", [(86400, 2 * 86400), (2 * 86400, 86400)]
)
@pytest.mark.parametrize("event_type", ["default", "error", "transaction"])
def test_processing_quotas(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    events_consumer,
    transactions_consumer,
    event_type,
    window,
    max_rate_limit,
):
    from time import sleep

    # At the moment (2020-10-12) transactions do not generate outcomes.
    # When this changes this test must be fixed, (remove generate_outcomes check).
    if event_type == "transaction":
        events_consumer = transactions_consumer()
        outcomes_consumer = None
    else:
        events_consumer = events_consumer()
        outcomes_consumer = outcomes_consumer()

    relay = relay_with_processing({"processing": {"max_rate_limit": max_rate_limit}})

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    # add another dsn key (we want 2 keys so we can set limits per key)
    mini_sentry.add_dsn_key_to_project(project_id)

    # we should have 2 keys (one created with the config and one added above)
    public_keys = mini_sentry.get_dsn_public_key_configs(project_id)

    key_id = public_keys[0]["numericId"]

    # Default events are also mapped to "error" by Relay.
    category = "error" if event_type == "default" else event_type

    projectconfig["config"]["quotas"] = [
        {
            "id": "test_rate_limiting_{}".format(uuid.uuid4().hex),
            "scope": "key",
            "scopeId": six.text_type(key_id),
            "categories": [category],
            "limit": 5,
            "window": window,
            "reasonCode": "get_lost",
        }
    ]

    if event_type == "transaction":
        transform = make_transaction
    elif event_type == "error":
        transform = make_error
    else:
        transform = lambda e: e

    for i in range(5):
        # send using the first dsn
        relay.send_event(
            project_id, transform({"message": f"regular{i}"}), dsn_key_idx=0
        )

        event, _ = events_consumer.get_event()
        assert event["logentry"]["formatted"] == f"regular{i}"

    # this one will not get a 429 but still get rate limited (silently) because
    # of our caching
    relay.send_event(project_id, transform({"message": "some_message"}), dsn_key_idx=0)

    if outcomes_consumer is not None:
        outcomes_consumer.assert_rate_limited(
            "get_lost", key_id=key_id, categories=[category]
        )
    else:
        # since we don't wait for the outcome, wait a little for the event to go through
        sleep(0.1)

    for _ in range(5):
        with pytest.raises(HTTPError) as excinfo:
            # Failed: DID NOT RAISE <class 'requests.exceptions.HTTPError'>
            relay.send_event(project_id, transform({"message": "rate_limited"}))
        headers = excinfo.value.response.headers

        retry_after = headers["retry-after"]
        assert int(retry_after) <= window
        assert int(retry_after) <= max_rate_limit
        retry_after2, rest = headers["x-sentry-rate-limits"].split(":", 1)
        assert int(retry_after2) == int(retry_after)
        assert rest == "%s:key:get_lost" % category
        if outcomes_consumer is not None:
            outcomes_consumer.assert_rate_limited(
                "get_lost", key_id=key_id, categories=[category]
            )

    for i in range(10):
        # now send using the second key
        relay.send_event(
            project_id, transform({"message": f"otherkey{i}"}), dsn_key_idx=1
        )
        event, _ = events_consumer.get_event()

        assert event["logentry"]["formatted"] == f"otherkey{i}"


def test_events_buffered_before_auth(relay, mini_sentry):
    evt = threading.Event()

    def server_error(*args, **kwargs):
        # simulate a bug in sentry
        evt.set()
        abort(500, "sentry is down")

    old_handler = mini_sentry.app.view_functions["get_challenge"]
    # make the register endpoint fail with a network error
    mini_sentry.app.view_functions["get_challenge"] = server_error

    # keep max backoff as short as the configuration allows (1 sec)
    relay_options = {"http": {"max_retry_interval": 1}}
    relay = relay(mini_sentry, relay_options, wait_healthcheck=False)
    assert evt.wait(1)  # wait for relay to start authenticating

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    try:
        relay.send_event(project_id)
        # resume normal function
        mini_sentry.app.view_functions["get_challenge"] = old_handler

        # now test that we still get the message sent at some point in time (the event is retried)
        event = mini_sentry.captured_events.get(timeout=3).get_event()
        assert event["logentry"] == {"formatted": "Hello, World!"}
    finally:
        # Relay reports authentication errors, which is fine.
        mini_sentry.test_failures.clear()


def test_events_are_retried(relay, mini_sentry):
    # keep max backoff as short as the configuration allows (1 sec)
    relay_options = {"http": {"max_retry_interval": 1}}
    relay = relay(mini_sentry, relay_options)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    evt = threading.Event()

    def network_error_endpoint(*args, **kwargs):
        # simulate a network error
        evt.set()
        raise socket.timeout()

    old_handler = mini_sentry.app.view_functions["store_event"]
    # make the store endpoint fail with a network error
    mini_sentry.app.view_functions["store_event"] = network_error_endpoint

    relay.send_event(project_id)
    # test that the network fail handler is called at least once
    assert evt.wait(1)
    # resume normal function
    mini_sentry.app.view_functions["store_event"] = old_handler

    # now test that we still get the message sent at some point in time (the event is retried)
    event = mini_sentry.captured_events.get(timeout=3).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_failed_network_requests_trigger_health_check(relay, mini_sentry):
    """
    Tests that consistently failing network requests will trigger relay to enter outage mode
    and call on the liveliness endpoint
    """

    def network_error_endpoint(*args, **kwargs):
        # simulate a network error
        raise socket.timeout()

    # make the store endpoint fail with a network error
    mini_sentry.app.view_functions["store_event"] = network_error_endpoint
    original_is_live = mini_sentry.app.view_functions["is_live"]
    evt = threading.Event()

    def is_live():
        evt.set()  # mark is_live was called
        return original_is_live()

    mini_sentry.app.view_functions["is_live"] = is_live

    # keep max backoff and the outage grace period as short as the configuration allows

    relay_options = {
        "http": {
            "max_retry_interval": 1,
            "auth_interval": 1000,
            "outage_grace_period": 1,
        }
    }
    relay = relay(mini_sentry, relay_options)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    # send an event, the event should fail and trigger a liveliness check (after a second)
    relay.send_event(project_id)

    # it did try to reestablish connection
    assert evt.wait(5)


@pytest.mark.parametrize("mode", ["static", "proxy"])
def test_no_auth(relay, mini_sentry, mode):
    """
    Tests that relays that run in proxy and static mode do NOT authenticate
    """
    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)

    old_handler = mini_sentry.app.view_functions["get_challenge"]
    has_registered = [False]

    # remember if somebody has tried to register
    def register_challenge(*args, **kwargs):
        has_registered[0] = True
        return old_handler(*args, **kwargs)

    mini_sentry.app.view_functions["get_challenge"] = register_challenge

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join("{}.json".format(project_id)).write(
            json.dumps(project_config)
        )

    relay_options = {"relay": {"mode": mode}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_static_project)

    relay.send_event(project_id, {"message": "123"})

    # sanity test that we got the event we sent
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "123"}
    # verify that no registration took place (the register flag is not set)
    assert not has_registered[0]


def test_processing_no_re_auth(relay_with_processing, mini_sentry):
    """
    Test that processing relays only authenticate once.

    That is processing relays do NOT reauthenticate.
    """
    from time import sleep

    relay_options = {"http": {"auth_interval": 1}}

    # count the number of times relay registers
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    counter = [0]

    def counted_check_challenge(*args, **kwargs):
        counter[0] += 1
        return original_check_challenge(*args, **kwargs)

    mini_sentry.app.view_functions["check_challenge"] = counted_check_challenge

    # creates a relay (we don't need to call it explicitly it should register by itself)
    relay_with_processing(options=relay_options)

    sleep(2)
    # check that the registration happened only once (although it should have happened every 0.1 secs)
    assert counter[0] == 1


def test_re_auth(relay, mini_sentry):
    """
    Tests that managed non-processing relays re-authenticate periodically.
    """
    from time import sleep

    relay_options = {"http": {"auth_interval": 1}}

    # count the number of times relay registers
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    counter = [0]

    def counted_check_challenge(*args, **kwargs):
        counter[0] += 1
        return original_check_challenge(*args, **kwargs)

    mini_sentry.app.view_functions["check_challenge"] = counted_check_challenge

    # creates a relay (we don't need to call it explicitly it should register by itself)
    relay(mini_sentry, options=relay_options)

    sleep(2)
    # check that the registration happened repeatedly
    assert counter[0] > 1


def test_re_auth_failure(relay, mini_sentry):
    """
    Test that after a re-authentication failure, relay stops sending messages until is reauthenticated.

    That is re-authentication failure puts relay in Error state that blocks any
    further message passing until authentication is re established.
    """
    relay_options = {"http": {"auth_interval": 1}}

    # count the number of times relay registers
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    counter = [0]
    registration_should_succeed = True
    evt = threading.Event()

    def counted_check_challenge(*args, **kwargs):
        counter[0] += 1
        evt.set()
        if registration_should_succeed:
            return original_check_challenge(*args, **kwargs)
        else:
            return Response("failed", status=500)

    mini_sentry.app.view_functions["check_challenge"] = counted_check_challenge

    # creates a relay (we don't need to call it explicitly it should register by itself)
    relay = relay(mini_sentry, options=relay_options)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    # we have authenticated successfully
    assert evt.wait(2)
    auth_count_1 = counter[0]
    # now fail re-authentication
    registration_should_succeed = False
    # wait for re-authentication try (should fail)
    evt.clear()
    assert evt.wait(2)
    # check that we have had some authentications attempts (that failed)
    auth_count_2 = counter[0]
    assert auth_count_1 < auth_count_2

    # send a message, it should not come through while the authentication has failed
    relay.send_event(project_id, {"message": "123"})
    # sentry should have received nothing
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))

    # set back authentication to ok
    registration_should_succeed = True
    # and wait for authentication to be called
    evt.clear()
    assert evt.wait(2)
    # clear authentication errors accumulated until now
    mini_sentry.test_failures.clear()
    # check that we have had some auth that succeeded
    auth_count_3 = counter[0]
    assert auth_count_2 < auth_count_3

    # now we should be re-authenticated and we should have the event

    # sanity test that we got the event we sent
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "123"}


def test_permanent_rejection(relay, mini_sentry):
    """
    Tests that after a permanent rejection stops authentication attempts.

    That is once an authentication message detects a permanent rejection
    it will not re-try to authenticate.
    """

    relay_options = {"http": {"auth_interval": 1}}

    # count the number of times relay registers
    original_check_challenge = mini_sentry.app.view_functions["check_challenge"]
    counter = [0, 0]
    registration_should_succeed = True
    evt = threading.Event()

    def counted_check_challenge(*args, **kwargs):
        counter[0] += 1
        evt.set()
        if registration_should_succeed:
            return original_check_challenge(*args, **kwargs)
        else:
            counter[1] += 1
            response = Response(
                json.dumps({"detail": "bad dog", "relay": "stop"}),
                status=403,
                content_type="application/json",
            )
            return response

    mini_sentry.app.view_functions["check_challenge"] = counted_check_challenge

    relay(mini_sentry, options=relay_options)

    # we have authenticated successfully
    assert evt.wait(2)
    auth_count_1 = counter[0]
    # now fail re-authentication with client error
    registration_should_succeed = False
    # wait for re-authentication try (should fail)
    evt.clear()
    assert evt.wait(2)
    # check that we have had some authentications attempts (that failed)
    auth_count_2 = counter[0]
    assert auth_count_1 < auth_count_2

    # once we issue a client error we are never called back again
    # and wait for authentication to be called
    evt.clear()
    # check that we were not called
    assert evt.wait(2) is False
    # to be sure verify that we have only been called once (after failing)
    assert counter[1] == 1
    # clear authentication errors accumulated until now
    mini_sentry.test_failures.clear()


def test_buffer_events_during_outage(relay, mini_sentry):
    """
    Tests that events are buffered during network outages and then sent.
    """

    original_store_event = mini_sentry.app.view_functions["store_event"]
    is_network_error = True

    def network_error_endpoint(*args, **kwargs):
        if is_network_error:
            # simulate a network error
            raise socket.timeout()
        else:
            # normal processing
            original_store_event(*args, **kwargs)

    # make the store endpoint fail with a network error
    is_network_error = True
    mini_sentry.app.view_functions["store_event"] = network_error_endpoint
    original_is_live = mini_sentry.app.view_functions["is_live"]
    evt = threading.Event()

    def is_live():
        evt.set()  # mark is_live was called
        return original_is_live()

    mini_sentry.app.view_functions["is_live"] = is_live

    # keep max backoff and the outage grace period as short as the configuration allows
    relay_options = {
        "http": {
            "max_retry_interval": 1,
            "auth_interval": 1000,
            "outage_grace_period": 1,
        }
    }
    relay = relay(mini_sentry, relay_options)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    # send an event, the event should fail and trigger a liveliness check (after a second)
    relay.send_event(project_id, {"message": "123"})

    # it did try to reestablish connection
    assert evt.wait(5)

    # now stop network errors (let the events pass)
    is_network_error = False

    # sanity test that we got the event we sent
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "123"}


@pytest.mark.parametrize("retry", [True, False])
def test_missing_in_response(mini_sentry, relay, retry):
    """
    Test that Relay still lets other events through if the projectconfig
    response is missing data, and that retryConfigs param causes Relay to retry
    fetching projectconfig.
    """

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    dsn_key = config["publicKeys"][0]["publicKey"]

    broken_project_id = 43
    broken_dsn_key = "59bcc3ad6775562f845953cf01624225"

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    requests = []

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        original_keys = flask_request.json["publicKeys"]
        if not requests:
            flask_request.json["publicKeys"] = [
                x for x in flask_request.json["publicKeys"] if x != broken_dsn_key
            ]
            data = json.loads(get_project_config_original().data)
            assert len(data["configs"]) == 1

            if retry:
                data["retryConfigs"] = [broken_dsn_key]
            rv = jsonify(**data)
        else:
            rv = get_project_config_original()

        requests.append(set(original_keys))
        return rv

    relay = relay(mini_sentry, options={"cache": {"batch_interval": 500}})

    relay.send_event(broken_project_id, {"message": "123"}, dsn_key=broken_dsn_key)
    relay.send_event(project_id, {"message": "123"})

    envelope = mini_sentry.captured_events.get(timeout=2)
    assert envelope.headers["dsn"].endswith("/42")
    event = envelope.get_event()
    assert event["logentry"] == {"formatted": "123"}

    assert not mini_sentry.captured_events.qsize()

    assert requests == [{dsn_key, broken_dsn_key}]

    time.sleep(0.5)

    if retry:
        assert requests == [
            {dsn_key, broken_dsn_key},
            {broken_dsn_key},
        ]
    else:
        assert requests == [
            {dsn_key, broken_dsn_key},
        ]
