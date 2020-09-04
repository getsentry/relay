import json
import os
import queue
import datetime
import uuid
import six

import pytest

from requests.exceptions import HTTPError


def test_store(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}


@pytest.mark.parametrize("allowed", [True, False])
def test_store_external_relay(mini_sentry, relay, allowed):
    # Use 3 Relays to force the middle one to fetch public keys
    relay = relay(relay(relay(mini_sentry)), external=True)

    if allowed:
        project_config = relay.basic_project_config()
    else:
        # Use `mini_sentry` to create the project config, which does not allow the Relay in the
        # project config.
        project_config = mini_sentry.basic_project_config()
    mini_sentry.project_configs[42] = project_config

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
    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42, legacy=True)
    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}

    relay.send_event(42, legacy=True)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}

    # Second request should have the project id cached
    assert mini_sentry.get_hits("/api/0/relays/projectids/") == 1


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
    relay = relay_with_processing()
    project_config = relay.full_project_config()
    filter_settings = project_config["config"]["filterSettings"]
    for key in filter_config.keys():
        filter_settings[key] = filter_config[key]
    mini_sentry.project_configs[42] = project_config

    events_consumer = events_consumer()

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.utcnow()
    message_text = "some message {}".format(now.isoformat())

    user_agent = (
        "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; "
        "Trident/6.0; en-IN)"
    )

    event = {
        "message": message_text,
        "release": "1.2.3",
        "request": {"headers": {"User-Agent": user_agent,}},
        "exception": {
            "values": [{"type": "Panic", "value": "originalCreateNotification"}]
        },
        "user": {"ip_address": "127.0.0.1"},
    }

    relay.send_event(42, event)

    event, _ = events_consumer.try_get_event()

    if should_filter:
        assert event is None
    else:
        assert event is not None


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
    project_config = relay.full_project_config()
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["webCrawlers"] = {"isEnabled": is_enabled}
    mini_sentry.project_configs[42] = project_config

    events_consumer = events_consumer()

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.utcnow()
    message_text = "some message {}".format(now.isoformat())

    event = {
        "message": message_text,
        "request": {"headers": {"User-Agent": "BingBot",}},
    }

    relay.send_event(42, event)

    event, _ = events_consumer.try_get_event()

    if should_filter:
        assert event is None
    else:
        assert event is not None


@pytest.mark.parametrize("method_to_test", [("GET", False), ("POST", True)])
def test_options_response(mini_sentry, relay, method_to_test):
    method, should_succeed = method_to_test
    relay = relay(mini_sentry)
    mini_sentry.project_configs[42] = relay.basic_project_config()

    headers = {
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": "X-Sentry-Auth",
    }

    result = relay.send_options(42, headers)

    assert result.ok == should_succeed

    print(result)


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()

    mini_sentry.project_configs[42] = relay.basic_project_config()
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
    relay.send_event(42, payload)

    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Error: yo mark"}


def test_store_pii_stripping(mini_sentry, relay):
    relay = relay(mini_sentry)

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42, {"message": "hi", "extra": {"foo": "test@mail.org"}})

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

    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42, {"message": "invalid"})
    sleep(1)  # Sleep so that the second event also has to wait but succeeds
    relay.send_event(42, {"message": "correct"})

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "correct"}
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))
    ((route, error),) = mini_sentry.test_failures
    assert route == "/api/666/store/"
    assert "configured lifetime" in str(error)
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
    mini_sentry.project_configs[42] = relay.basic_project_config()

    # This message should return the initial 429 and start rate limiting
    relay.send_event(42, {"message": "rate limit"})

    # This event should get dropped by relay. We expect 429 here
    sleep(1)
    with pytest.raises(HTTPError):
        relay.send_event(42, {"message": "invalid"})

    # This event should arrive
    sleep(2)
    relay.send_event(42, {"message": "correct"})

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "correct"}


def test_store_static_config(mini_sentry, relay):
    from time import sleep

    project_config = mini_sentry.basic_project_config()

    def configure_static_project(dir):
        os.remove(dir.join("credentials.json"))
        os.makedirs(dir.join("projects"))
        dir.join("projects").join("42.json").write(json.dumps(project_config))

    relay_options = {"relay": {"mode": "static"}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_static_project)
    mini_sentry.project_configs[42] = project_config

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}

    sleep(1)  # Regression test: Relay tried to issue a request for 0 states
    if mini_sentry.test_failures:
        raise AssertionError(
            f"Exceptions happened in mini_sentry: {mini_sentry.format_failures()}"
        )


def test_store_proxy_config(mini_sentry, relay):
    from time import sleep

    project_config = mini_sentry.basic_project_config()

    def configure_proxy(dir):
        os.remove(dir.join("credentials.json"))

    relay_options = {"relay": {"mode": "proxy"}}
    relay = relay(mini_sentry, options=relay_options, prepare=configure_proxy)
    mini_sentry.project_configs[42] = project_config
    sleep(1)  # There is no upstream auth, so just wait for relay to initialize

    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_store_buffer_size(mini_sentry, relay):
    relay = relay(mini_sentry, {"cache": {"event_buffer_size": 0}})
    mini_sentry.project_configs[42] = relay.basic_project_config()

    with pytest.raises(HTTPError):
        relay.send_event(42, {"message": "pls ignore"})
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))

    for (_, error) in mini_sentry.test_failures:
        assert isinstance(error, AssertionError)
        assert "Too many events (event_buffer_size reached)" in str(error)
    mini_sentry.test_failures.clear()


def test_store_max_concurrent_requests(mini_sentry, relay):
    from time import sleep
    from threading import Semaphore

    processing_store = False
    store_count = Semaphore()

    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()

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

    relay.send_event(42)
    relay.send_event(42)

    store_count.acquire(timeout=2)
    store_count.acquire(timeout=2)


def test_store_not_normalized(mini_sentry, relay):
    """
    Tests that relay does not normalize when processing is disabled
    """
    relay = relay(mini_sentry, {"processing": {"enabled": False}})
    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    relay.send_event(42, {"message": "some_message"})
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
    relay = relay_with_processing()
    mini_sentry.project_configs[42] = mini_sentry.full_project_config()

    if event_type == "default":
        events_consumer = events_consumer()
    else:
        events_consumer = transactions_consumer()

    # create a unique message so we can make sure we don't test with stale data
    message_text = "some message {}".format(uuid.uuid4())
    event = {
        "message": message_text,
        "extra": {"msg_text": message_text},
    }

    if event_type == "transaction":
        make_transaction(event)

    relay.send_event(42, event)

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


@pytest.mark.parametrize("event_type", ["default", "error", "transaction"])
def test_processing_quotas(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    events_consumer,
    transactions_consumer,
    event_type,
):
    relay = relay_with_processing({"processing": {"max_rate_limit": 120}})

    mini_sentry.project_configs[42] = projectconfig = mini_sentry.full_project_config()
    public_keys = projectconfig["publicKeys"]
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
            "window": 3600,
            "reasonCode": "get_lost",
        }
    ]

    second_key = {
        "publicKey": "31a5a894b4524f74a9a8d0e27e21ba92",
        "isEnabled": True,
        "numericId": 1234,
    }
    public_keys.append(second_key)

    if event_type == "transaction":
        events_consumer = transactions_consumer()
    else:
        events_consumer = events_consumer()
    outcomes_consumer = outcomes_consumer()

    if event_type == "transaction":
        transform = make_transaction
    elif event_type == "error":
        transform = make_error
    else:
        transform = lambda e: e

    for i in range(5):
        relay.send_event(42, transform({"message": f"regular{i}"}))

        event, _ = events_consumer.get_event()
        assert event["logentry"]["formatted"] == f"regular{i}"

    # this one will not get a 429 but still get rate limited (silently) because
    # of our caching
    relay.send_event(42, transform({"message": "some_message"}))

    outcomes_consumer.assert_rate_limited("get_lost", key_id=key_id)

    for _ in range(5):
        with pytest.raises(HTTPError) as excinfo:
            relay.send_event(42, transform({"message": "rate_limited"}))
        headers = excinfo.value.response.headers

        # The rate limit is actually for 1 hour, but we cap at 120s with the
        # max_rate_limit parameter
        retry_after = headers["retry-after"]
        assert int(retry_after) <= 120
        retry_after2, rest = headers["x-sentry-rate-limits"].split(":", 1)
        assert int(retry_after2) == int(retry_after)
        assert rest == "%s:key" % category
        outcomes_consumer.assert_rate_limited("get_lost", key_id=key_id)

    relay.dsn_public_key = second_key["publicKey"]

    for i in range(10):
        relay.send_event(42, transform({"message": f"otherkey{i}"}))
        event, _ = events_consumer.get_event()

        assert event["logentry"]["formatted"] == f"otherkey{i}"
