import json
import os
import queue
import socket
import threading
import uuid
from datetime import datetime, timedelta, timezone
from time import sleep

import pytest
from flask import Response, abort
from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef


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


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_options_response(mini_sentry, relay, method):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    headers = {
        "Access-Control-Request-Method": method,
        "Access-Control-Request-Headers": "X-Sentry-Auth",
    }

    result = relay.send_options(project_id, headers)
    assert result.ok, result
    # GET is never allowed for XHR
    assert result.headers["access-control-allow-methods"] == "POST"
    # Contents tested by test_security_report_preflight
    assert "access-control-allow-headers" in result.headers


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

    # Disable outcomes so client report envelopes do not interfere with the events we are looking for
    config = {"outcomes": {"emit_outcomes": "as_client_reports"}}
    relay = relay(mini_sentry, config)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    # This message should return the initial 429 and start rate limiting
    relay.send_event(project_id, {"message": "rate limit"})

    # This event should get dropped by relay. We expect 429 here
    sleep(1)
    with pytest.raises(HTTPError):
        relay.send_event(project_id, {"message": "invalid"})

    # Generated outcome has reason code 'generic':
    outcome_envelope = mini_sentry.captured_events.get(timeout=1)
    outcome = json.loads(outcome_envelope.items[0].payload.bytes)
    assert outcome["rate_limited_events"] == [
        {"reason": "generic", "category": "error", "quantity": 1}
    ]

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
        dir.join("projects").join(f"{project_id}.json").write(
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

        for _, error in mini_sentry.test_failures:
            assert isinstance(error, AssertionError)
            assert "buffer capacity exceeded" in str(error)
    finally:
        mini_sentry.test_failures.clear()


def test_store_max_concurrent_requests(mini_sentry, relay):
    from threading import Semaphore
    from time import sleep

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
    now = datetime.utcnow()
    event.update(
        {
            "type": "transaction",
            "timestamp": now.isoformat(),
            "start_timestamp": (now - timedelta(seconds=2)).isoformat(),
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
    message_text = f"some message {uuid.uuid4()}"
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

    if event_type == "transaction":
        assert event["transaction_info"]["source"] == "unknown"  # the default
    else:
        # Should not be serialized
        assert "transaction_info" not in event


# TODO: This parameterization should be unit-tested, instead
@pytest.mark.parametrize(
    "window,max_rate_limit", [(86400, 2 * 86400), (2 * 86400, 86400)]
)
@pytest.mark.parametrize("event_type", ["default", "error", "transaction", "nel"])
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
    category = "error" if event_type == "default" or event_type == "nel" else event_type

    projectconfig["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "key",
            "scopeId": str(key_id),
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

        def transform(e):
            return e

    for i in range(5):
        if event_type == "nel":
            relay.send_nel_event(project_id, dsn_key_idx=0)
        else:
            # send using the first dsn
            relay.send_event(
                project_id, transform({"message": f"regular{i}"}), dsn_key_idx=0
            )

        event, _ = events_consumer.get_event(timeout=10)
        if event_type == "nel":
            assert event["logentry"]["formatted"] == "application / http.error"
        else:
            assert event["logentry"]["formatted"] == f"regular{i}"

    # this one will not get a 429 but still get rate limited (silently) because
    # of our caching
    if event_type == "nel":
        relay.send_nel_event(project_id, dsn_key_idx=0)
    else:
        relay.send_event(
            project_id, transform({"message": "some_message"}), dsn_key_idx=0
        )

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
        if event_type == "nel":
            relay.send_nel_event(project_id, dsn_key_idx=1)
        else:
            relay.send_event(
                project_id, transform({"message": f"otherkey{i}"}), dsn_key_idx=1
            )
        event, _ = events_consumer.get_event()

        if event_type == "nel":
            assert event["logentry"]["formatted"] == "application / http.error"
        else:
            assert event["logentry"]["formatted"] == f"otherkey{i}"


def test_rate_limit_metric_bucket(
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
):
    metrics_consumer = metrics_consumer()

    bucket_interval = 1  # second
    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 2 * 86400},
            "aggregator": {
                "bucket_interval": bucket_interval,
                "initial_delay": 0,
                "debounce_delay": 0,
            },
        }
    )

    metric_bucket_limit = 5
    buckets_sent = 10

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    mini_sentry.add_dsn_key_to_project(project_id)

    public_keys = mini_sentry.get_dsn_public_key_configs(project_id)
    key_id = public_keys[0]["numericId"]
    projectconfig["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "key",
            "scopeId": str(key_id),
            "categories": ["metric_bucket"],
            "limit": metric_bucket_limit,
            "window": 86400,
            "reasonCode": "throughput rate limiting",
        }
    ]

    def generate_ticks():
        # Generate a new timestamp for every bucket, so they do not get merged by the aggregator
        tick = int(datetime.utcnow().timestamp() // bucket_interval * bucket_interval)
        while True:
            yield tick
            tick += bucket_interval

    tick = generate_ticks()

    def make_bucket(name, type_, values):
        return {
            "org_id": 1,
            "project_id": project_id,
            "timestamp": next(tick),
            "name": name,
            "type": type_,
            "value": values,
            "width": bucket_interval,
        }

    def send_buckets(buckets):
        relay.send_metrics_buckets(project_id, buckets)
        sleep(0.2)

    for _ in range(buckets_sent):
        bucket = make_bucket("d:transactions/measurements.lcp@millisecond", "d", [1.0])
        send_buckets(
            [bucket],
        )
    produced_buckets = [m for m, _ in metrics_consumer.get_metrics()]

    assert metric_bucket_limit < buckets_sent
    assert len(produced_buckets) == metric_bucket_limit


@pytest.mark.parametrize("violating_bucket", [[4.0, 5.0], [4.0, 5.0, 6.0]])
def test_rate_limit_metrics_buckets(
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
    outcomes_consumer,
    violating_bucket,
):
    """
    param violating_bucket is parametrized so we cover both cases:
        1. the quota is matched exactly
        2. quota is exceeded by one
    """
    bucket_interval = 1  # second
    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 2 * 86400},
            "aggregator": {
                "bucket_interval": bucket_interval,
                "initial_delay": 0,
                "debounce_delay": 0,
            },
        }
    )
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    # add another dsn key (we want 2 keys so we can set limits per key)
    mini_sentry.add_dsn_key_to_project(project_id)

    public_keys = mini_sentry.get_dsn_public_key_configs(project_id)
    key_id = public_keys[0]["numericId"]

    reason_code = uuid.uuid4().hex

    projectconfig["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "key",
            "scopeId": str(key_id),
            "categories": ["transaction"],
            "limit": 5,
            "window": 86400,
            "reasonCode": reason_code,
        }
    ]

    def generate_ticks():
        # Generate a new timestamp for every bucket, so they do not get merged by the aggregator
        tick = int(datetime.utcnow().timestamp() // bucket_interval * bucket_interval)
        while True:
            yield tick
            tick += bucket_interval

    tick = generate_ticks()

    def make_bucket(name, type_, values):
        return {
            "org_id": 1,
            "project_id": project_id,
            "timestamp": next(tick),
            "name": name,
            "type": type_,
            "value": values,
            "width": bucket_interval,
        }

    def send_buckets(buckets):
        relay.send_metrics_buckets(project_id, buckets)
        sleep(0.2)

    # NOTE: Sending these buckets in multiple envelopes because the order of flushing
    # and also the order of rate limiting is not deterministic.
    send_buckets(
        [
            # Send a few non-duration buckets, they will not deplete the quota
            make_bucket("d:transactions/measurements.lcp@millisecond", "d", 10 * [1.0]),
            # Session metrics are accepted
            make_bucket("d:sessions/session@none", "c", 1),
            make_bucket("d:sessions/duration@second", "d", 9 * [1]),
        ]
    )
    send_buckets(
        [
            # Duration metric, subtract 3 from quota
            make_bucket("d:transactions/duration@millisecond", "d", [1, 2, 3]),
        ],
    )
    send_buckets(
        [
            # Can still send unlimited non-duration metrics
            make_bucket("d:transactions/measurements.lcp@millisecond", "d", 10 * [2.0]),
        ],
    )
    send_buckets(
        [
            # Duration metric, subtract from quota. This bucket is still accepted, but the rest
            # will be exceeded.
            make_bucket("d:transactions/duration@millisecond", "d", violating_bucket),
        ],
    )
    send_buckets(
        [
            # FCP buckets won't make it into kakfa
            make_bucket("d:transactions/measurements.fcp@millisecond", "d", 10 * [7.0]),
        ],
    )
    send_buckets(
        [
            # Another three for duration, won't make it into kafka.
            make_bucket("d:transactions/duration@millisecond", "d", [7, 8, 9]),
            # Session metrics are still accepted.
            make_bucket("d:sessions/session@user", "s", [1254]),
        ],
    )
    metrics = [m for m, _ in metrics_consumer.get_metrics(timeout=4)]
    produced_buckets = metrics

    # Sort buckets to prevent ordering flakiness:
    produced_buckets.sort(key=lambda b: (b["name"], b["value"]))
    for bucket in produced_buckets:
        del bucket["timestamp"]

    assert produced_buckets == [
        {
            "name": "d:sessions/duration@second",
            "org_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "tags": {},
            "type": "d",
            "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        },
        {
            "name": "d:sessions/session@none",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "d:sessions/session@user",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "s",
            "value": [1254],
        },
        {
            "name": "d:transactions/duration@millisecond",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "d",
            "value": [1.0, 2.0, 3.0],
        },
        {
            "name": "d:transactions/duration@millisecond",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "d",
            "value": violating_bucket,
        },
        {
            "name": "d:transactions/measurements.lcp@millisecond",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "d",
            "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
        },
        {
            "name": "d:transactions/measurements.lcp@millisecond",
            "org_id": 1,
            "retention_days": 90,
            "project_id": 42,
            "tags": {},
            "type": "d",
            "value": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
        },
    ]

    outcomes_consumer.assert_rate_limited(
        reason_code,
        key_id=key_id,
        categories=["transaction"],
        quantity=3,
    )


@pytest.mark.parametrize("extraction_version", [1, 3])
def test_processing_quota_transaction_indexing(
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
    transactions_consumer,
    extraction_version,
):
    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 100},
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "debounce_delay": 0,
            },
        }
    )

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    key_id = mini_sentry.get_dsn_public_key_configs(project_id)[0]["numericId"]
    projectconfig["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "key",
            "scopeId": str(key_id),
            "categories": ["transaction_indexed"],
            "limit": 1,
            "window": 86400,
            "reasonCode": "get_lost",
        },
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "key",
            "scopeId": str(key_id),
            "categories": ["transaction"],
            "limit": 2,
            "window": 86400,
            "reasonCode": "get_lost",
        },
    ]
    projectconfig["config"]["transactionMetrics"] = {
        "version": extraction_version,
    }

    relay.send_event(project_id, make_transaction({"message": "1st tx"}))
    event, _ = tx_consumer.get_event()
    assert event["logentry"]["formatted"] == "1st tx"
    buckets = list(metrics_consumer.get_metrics())
    assert len(buckets) > 0

    relay.send_event(project_id, make_transaction({"message": "2nd tx"}))
    tx_consumer.assert_empty()
    buckets = list(metrics_consumer.get_metrics())
    assert len(buckets) > 0

    with pytest.raises(HTTPError) as exc_info:
        relay.send_event(project_id, make_transaction({"message": "2nd tx"}))

    assert exc_info.value.response.status_code == 429, "Expected a 429 status code"


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
    relay = relay(mini_sentry, relay_options, wait_health_check=False)

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    assert evt.wait(2)  # wait for relay to start authenticating

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
    relay_options = {
        "http": {
            "max_retry_interval": 1,
            "retry_delay": 0,
        }
    }
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
            "retry_delay": 0,
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
        dir.join("projects").join(f"{project_id}.json").write(
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

    # Give Relay some time to process the auth response and mark itself as not ready
    sleep(0.1)

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

    # Give Relay some time to process the auth response and mark itself as ready
    sleep(0.1)

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
            "retry_delay": 0,
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


def test_store_invalid_gzip(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="21.6.0")
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    headers = {
        "Content-Encoding": "gzip",
        "X-Sentry-Auth": relay.get_auth_header(project_id),
    }

    url = "/api/%s/store/" % project_id

    response = relay.post(url, headers=headers)
    assert response.status_code == 400


def test_store_project_move(mini_sentry, relay):
    """
    Tests that relay redirects events for moved projects based on the project key if
    `override_project_ids` is enabled. The expected behavior is:

     1. Resolve project config based on the public key alone
     2. Skip validation of the project ID
     3. Update the project ID
    """

    relay = relay(mini_sentry, {"relay": {"override_project_ids": True}})
    mini_sentry.add_basic_project_config(42)

    headers = {
        # send_event uses the project_id to create the auth header. We need to compute the correct
        # one so we can change to an invalid ID.
        "X-Sentry-Auth": relay.get_auth_header(42),
    }

    relay.send_event(99, headers=headers)
    envelope = mini_sentry.captured_events.get(timeout=1)

    # NB: mini_sentry errors on all other URLs than /api/42/store/. All that's left is checking the
    # DSN that is transmitted as part of the Envelope headers.
    assert envelope.headers["dsn"].endswith("/42")


def test_invalid_project_id(mini_sentry, relay):
    """
    Tests that Relay drops events for project ID mismatches. See `test_store_project_move` for an
    override of this behavior.
    """
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)

    headers = {
        # send_event uses the project_id to create the auth header. We need to compute the correct
        # one so we can change to an invalid ID.
        "X-Sentry-Auth": relay.get_auth_header(42),
    }

    relay.send_event(99, headers=headers)
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


def test_span_extraction(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
):
    spans_consumer = spans_consumer()

    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "projects:span-metrics-extraction",
        "projects:span-metrics-extraction-all-modules",
    ]

    event = make_transaction({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    end = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(seconds=1)
    start = end - timedelta(milliseconds=500)
    event["spans"] = [
        {
            "description": "GET /api/0/organizations/?member=1",
            "op": "http",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.isoformat(),
            "timestamp": end.isoformat(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    ]

    relay.send_event(project_id, event)

    child_span = spans_consumer.get_span()
    del child_span["start_time"]
    del child_span["span"]["received"]
    assert child_span == {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "project_id": 42,
        "organization_id": 1,
        "retention_days": 90,
        "span": {
            "description": "GET /api/0/organizations/?member=1",
            "exclusive_time": 500.0,
            "is_segment": False,
            "op": "http",
            "parent_span_id": "aaaaaaaaaaaaaaaa",
            "segment_id": "968cff94913ebb07",
            "sentry_tags": {
                "category": "http",
                "description": "GET *",
                "group": "37e3d9fab1ae9162",
                "op": "http",
                "transaction": "hi",
                "transaction.op": "hi",
            },
            "span_id": "bbbbbbbbbbbbbbbb",
            "start_timestamp": start.timestamp(),
            "timestamp": end.timestamp(),
            "trace_id": "ff62a8b040f340bda5d830223def1d81",
        },
    }

    transaction_span = spans_consumer.get_span()
    del transaction_span["start_time"]
    del transaction_span["span"]["received"]
    assert transaction_span == {
        "event_id": "cbf6960622e14a45abc1f03b2055b186",
        "project_id": 42,
        "organization_id": 1,
        "retention_days": 90,
        "span": {
            "description": "hi",
            "exclusive_time": 2000.0,
            "is_segment": True,
            "op": "hi",
            "segment_id": "968cff94913ebb07",
            "sentry_tags": {"transaction": "hi", "transaction.op": "hi"},
            "span_id": "968cff94913ebb07",
            "start_timestamp": datetime.fromisoformat(event["start_timestamp"])
            .replace(tzinfo=timezone.utc)
            .timestamp(),
            "status": "unknown",
            "timestamp": datetime.fromisoformat(event["timestamp"])
            .replace(tzinfo=timezone.utc)
            .timestamp(),
            "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
        },
    }

    spans_consumer.assert_empty()


def test_span_ingestion(
    mini_sentry,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay_with_processing(
        options={
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "debounce_delay": 0,
                "max_secs_in_past": 2**64 - 1,
            }
        }
    )
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "projects:span-metrics-extraction",
        "projects:span-metrics-extraction-all-modules",
    ]

    duration = timedelta(milliseconds=500)
    end = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(seconds=1)
    start = end - duration

    # 1 - Send OTel span and sentry span via envelope
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="otel_span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "traceId": "89143b0763095bd9c9955e8175d1fb23",
                        "spanId": "e342abb1214ca181",
                        "name": "my 1st OTel span",
                        "startTimeUnixNano": int(start.timestamp() * 1e9),
                        "endTimeUnixNano": int(end.timestamp() * 1e9),
                        "attributes": [
                            {
                                "key": "sentry.exclusive_time_ns",
                                "value": {
                                    "intValue": int(duration.total_seconds() * 1e9),
                                },
                            },
                        ],
                    },
                ).encode()
            ),
        )
    )
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": "https://example.com/p/blah.js",
                        "op": "resource.script",
                        "span_id": "bd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(project_id, envelope)

    # 2 - Send OTel span via endpoint
    relay.send_otel_span(
        project_id,
        {
            "resourceSpans": [
                {
                    "scopeSpans": [
                        {
                            "spans": [
                                {
                                    "traceId": "89143b0763095bd9c9955e8175d1fb24",
                                    "spanId": "e342abb1214ca182",
                                    "name": "my 2nd OTel span",
                                    "startTimeUnixNano": int(start.timestamp() * 1e9)
                                    + 2,
                                    "endTimeUnixNano": int(end.timestamp() * 1e9) + 3,
                                    "attributes": [
                                        {
                                            "key": "sentry.exclusive_time_ns",
                                            "value": {
                                                "intValue": int(
                                                    (duration.total_seconds() + 1) * 1e9
                                                ),
                                            },
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        },
    )

    spans = list(spans_consumer.get_spans())
    for span in spans:
        del span["start_time"]
        span["span"].pop("received", None)

    spans.sort(
        key=lambda msg: msg["span"].get("description", "")
    )  # endpoint might overtake envelope

    assert spans == [
        {
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "span": {
                "description": "https://example.com/p/blah.js",
                "is_segment": True,
                "op": "resource.script",
                "segment_id": "968cff94913ebb07",
                "sentry_tags": {
                    "category": "resource",
                    "description": "https://example.com/*/blah.js",
                    "domain": "example.com",
                    "file_extension": "js",
                    "group": "8a97a9e43588e2bd",
                    "op": "resource.script",
                },
                "span_id": "bd429c44b67a3eb1",
                "start_timestamp": start.timestamp(),
                "timestamp": end.timestamp() + 1,
                "exclusive_time": 345.0,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
            },
        },
        {
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "span": {
                "data": {},
                "description": "my 1st OTel span",
                "exclusive_time": 500.0,
                "is_segment": True,
                "op": "default",
                "parent_span_id": "",
                "segment_id": "e342abb1214ca181",
                "sentry_tags": {"op": "default"},
                "span_id": "e342abb1214ca181",
                "start_timestamp": start.timestamp(),
                "status": "ok",
                "timestamp": end.timestamp(),
                "trace_id": "89143b0763095bd9c9955e8175d1fb23",
            },
        },
        {
            "organization_id": 1,
            "project_id": 42,
            "retention_days": 90,
            "span": {
                "data": {},
                "description": "my 2nd OTel span",
                "exclusive_time": 1500.0,
                "is_segment": True,
                "op": "default",
                "parent_span_id": "",
                "segment_id": "e342abb1214ca182",
                "sentry_tags": {"op": "default"},
                "span_id": "e342abb1214ca182",
                "start_timestamp": start.timestamp(),
                "status": "ok",
                "timestamp": end.timestamp(),
                "trace_id": "89143b0763095bd9c9955e8175d1fb24",
            },
        },
    ]

    metrics = [metric for (metric, _headers) in metrics_consumer.get_metrics()]
    metrics.sort(key=lambda m: (m["name"], sorted(m["tags"].items())))
    for metric in metrics:
        try:
            metric["value"].sort()
        except AttributeError:
            pass

    expected_timestamp = int(end.timestamp())

    assert metrics == [
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/count_per_op@none",
            "type": "c",
            "value": 1.0,
            "timestamp": expected_timestamp + 1,
            "tags": {"span.category": "resource", "span.op": "resource.script"},
            "retention_days": 90,
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "c:spans/count_per_op@none",
            "type": "c",
            "value": 2.0,
            "timestamp": expected_timestamp,
            "tags": {"span.op": "default"},
            "retention_days": 90,
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "type": "d",
            "value": [345.0],
            "timestamp": expected_timestamp + 1,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "retention_days": 90,
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time@millisecond",
            "type": "d",
            "value": [500.0, 1500],
            "timestamp": expected_timestamp,
            "tags": {"span.status": "ok", "span.op": "default"},
            "retention_days": 90,
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time_light@millisecond",
            "type": "d",
            "value": [345.0],
            "timestamp": expected_timestamp + 1,
            "tags": {
                "file_extension": "js",
                "span.category": "resource",
                "span.description": "https://example.com/*/blah.js",
                "span.domain": "example.com",
                "span.group": "8a97a9e43588e2bd",
                "span.op": "resource.script",
            },
            "retention_days": 90,
        },
        {
            "org_id": 1,
            "project_id": 42,
            "name": "d:spans/exclusive_time_light@millisecond",
            "type": "d",
            "value": [500.0, 1500],
            "timestamp": expected_timestamp,
            "tags": {
                "span.op": "default",
                "span.status": "ok",
            },
            "retention_days": 90,
        },
    ]
