import json
import os
import queue
import socket
import threading
import uuid
from datetime import UTC, datetime, timedelta, timezone
from time import sleep

from sentry_relay.consts import DataCategory

from .asserts import time_within_delta
from .consts import (
    TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
)

import pytest
from flask import Response, abort
from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope


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
    if not mini_sentry.test_failures.empty():
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


def test_store_with_low_memory(mini_sentry, relay):
    relay = relay(
        mini_sentry, {"health": {"max_memory_percent": 0.0}}, wait_health_check=False
    )
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    try:
        with pytest.raises(HTTPError):
            relay.send_event(project_id, {"message": "pls ignore"})
        pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))

        found_queue_error = False
        for _, error in mini_sentry.current_test_failures():
            assert isinstance(error, AssertionError)
            if "failed to queue envelope" in str(error):
                found_queue_error = True
                break

        assert found_queue_error
    finally:
        mini_sentry.clear_test_failures()


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


def make_transaction(event):
    now = datetime.now(UTC)
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
        # send using the first dsn
        relay.send_event(
            project_id, transform({"message": f"regular{i}"}), dsn_key_idx=0
        )

        event, _ = events_consumer.get_event(timeout=10)
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
            sleep(0.2)
        headers = excinfo.value.response.headers

        retry_after = headers["retry-after"]
        assert int(retry_after) <= window
        assert int(retry_after) <= max_rate_limit
        retry_after2, rest = headers["x-sentry-rate-limits"].split(":", 1)
        assert int(retry_after2) == int(retry_after)
        if event_type == "transaction":
            # Transaction limits also apply to spans.
            assert rest == "transaction;span:key:get_lost"
        else:
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
        event, _ = events_consumer.get_event(timeout=10)
        assert event["logentry"]["formatted"] == f"otherkey{i}"


@pytest.mark.parametrize("namespace", ["transactions", "custom"])
def test_sends_metric_bucket_outcome(
    mini_sentry, relay_with_processing, outcomes_consumer, namespace
):
    """
    Checks that with a zero-quota without categories specified we send metric bucket outcomes.
    """
    outcomes_consumer = outcomes_consumer()
    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 2 * 86400},
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "flush_interval": 0,
            },
        }
    )

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    mini_sentry.add_dsn_key_to_project(project_id)

    projectconfig["config"]["features"] = ["organizations:custom-metrics"]
    projectconfig["config"]["quotas"] = [
        {
            "scope": "organization",
            "namespace": namespace,
            "limit": 0,
        }
    ]

    timestamp = int(datetime.now(tz=timezone.utc).timestamp())
    metrics_payload = f"transactions/foo:42|c\nbar@second:17|c|T{timestamp}"
    relay.send_metrics(project_id, metrics_payload)

    outcome = outcomes_consumer.get_outcome(timeout=3)

    assert outcome["category"] == 15  # metric_bucket
    assert outcome["quantity"] == 1

    outcomes_consumer.assert_empty()


def test_enforce_bucket_rate_limits(
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
):
    metrics_consumer = metrics_consumer()

    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 2 * 86400},
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
                "shift_key": "project",
            },
        }
    )

    metric_bucket_limit = 5

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    mini_sentry.add_dsn_key_to_project(project_id)

    projectconfig["config"]["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "scope": "organization",
            "categories": ["metric_bucket"],
            "limit": metric_bucket_limit,
            "window": 86400,
            "reasonCode": "throughput rate limiting",
        }
    ]

    def make_bucket(name, type_, values):
        return {
            "org_id": 1,
            "project_id": project_id,
            "timestamp": int(datetime.now(UTC).timestamp()),
            "name": name,
            "type": type_,
            "value": values,
            "width": 1,
        }

    buckets = [
        make_bucket(f"d:transactions/measurements.lcp{i}@millisecond", "d", [1.0])
        for i in range(metric_bucket_limit)
    ]

    # Send as many metrics as the quota allows.
    relay.send_metrics_buckets(project_id, buckets)
    metrics_consumer.get_metrics(n=metric_bucket_limit)

    # Send metrics again, at this point the quota is exhausted.
    relay.send_metrics_buckets(project_id, buckets)
    metrics_consumer.assert_empty()


@pytest.mark.parametrize("violating_bucket", [2.0, 3.0])
def test_enforce_transaction_rate_limit_on_metric_buckets(
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

    now = datetime.now(tz=timezone.utc).timestamp()

    def generate_ticks():
        # Generate a new timestamp for every bucket, so they do not get merged by the aggregator
        tick = int(now // bucket_interval * bucket_interval)
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

    def assert_metrics(expected_metrics):
        produced_metrics = [
            m for m, _ in metrics_consumer.get_metrics(n=len(expected_metrics))
        ]
        produced_metrics.sort(key=lambda b: (b["name"], b["value"]))
        assert produced_metrics == expected_metrics

    # NOTE: Sending these buckets in multiple envelopes because the order of flushing
    # and also the order of rate limiting is not deterministic.
    relay.send_metrics_buckets(
        project_id,
        [
            # Send a few non-usage buckets, they will not deplete the quota
            make_bucket("d:transactions/measurements.lcp@millisecond", "d", 10 * [1.0]),
            # Session metrics are accepted
            make_bucket("d:sessions/session@none", "c", 1),
            make_bucket("d:sessions/duration@second", "d", 9 * [1]),
        ],
    )
    assert_metrics(
        [
            {
                "name": "d:sessions/duration@second",
                "org_id": 1,
                "project_id": 42,
                "retention_days": 90,
                "tags": {},
                "type": "d",
                "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            },
            {
                "name": "d:sessions/session@none",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "c",
                "value": 1.0,
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            },
            {
                "name": "d:transactions/measurements.lcp@millisecond",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "d",
                "value": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            },
        ]
    )

    relay.send_metrics_buckets(
        project_id,
        [
            # Usage metric, subtract 3 from quota
            make_bucket("c:transactions/usage@none", "c", 3),
        ],
    )
    assert_metrics(
        [
            {
                "name": "c:transactions/usage@none",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "c",
                "value": 3.0,
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            },
        ]
    )

    relay.send_metrics_buckets(
        project_id,
        [
            # Can still send unlimited non-usage metrics
            make_bucket("d:transactions/measurements.lcp@millisecond", "d", 10 * [2.0]),
        ],
    )
    assert_metrics(
        [
            {
                "name": "d:transactions/measurements.lcp@millisecond",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "d",
                "value": [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0],
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            }
        ]
    )

    relay.send_metrics_buckets(
        project_id,
        [
            # Usage metric, subtract from quota. This bucket is still accepted (see over_accept_once),
            # but the rest will be rejected.
            make_bucket("c:transactions/usage@none", "c", violating_bucket),
        ],
    )
    assert_metrics(
        [
            {
                "name": "c:transactions/usage@none",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "c",
                "value": violating_bucket,
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            },
        ]
    )

    relay.send_metrics_buckets(
        project_id,
        [
            # FCP buckets won't make it into Kafka, since usage was now counted in Redis and it's >= 5.
            make_bucket("d:transactions/measurements.fcp@millisecond", "d", 10 * [7.0]),
        ],
    )

    relay.send_metrics_buckets(
        project_id,
        [
            # Another three for usage, won't make it into kafka because quota is zero.
            make_bucket("c:transactions/usage@none", "c", 3),
            # Session metrics are still accepted.
            make_bucket("d:sessions/session@user", "s", [1254]),
        ],
    )
    assert_metrics(
        [
            {
                "name": "d:sessions/session@user",
                "org_id": 1,
                "retention_days": 90,
                "project_id": 42,
                "tags": {},
                "type": "s",
                "value": [1254],
                "timestamp": time_within_delta(now),
                "received_at": time_within_delta(now),
            }
        ]
    )

    outcomes_consumer.assert_rate_limited(
        reason_code,
        key_id=key_id,
        categories=["transaction", "metric_bucket"],
        quantity=5,
    )


@pytest.mark.parametrize(
    "extraction_version",
    [
        TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
        TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
    ],
)
def test_processing_quota_transaction_indexing(
    mini_sentry,
    relay_with_processing,
    metrics_consumer,
    transactions_consumer,
    outcomes_consumer,
    extraction_version,
):
    relay = relay_with_processing(
        {
            "processing": {"max_rate_limit": 100},
            # make sure that sent envelopes will be processed sequentially.
            "limits": {"max_thread_count": 1},
            "aggregator": {
                "bucket_interval": 1,
                "initial_delay": 0,
            },
        }
    )

    metrics_consumer = metrics_consumer()
    tx_consumer = transactions_consumer()
    outcomes_consumer = outcomes_consumer()

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
    assert len(list(metrics_consumer.get_metrics())) > 0

    relay.send_event(project_id, make_transaction({"message": "2nd tx"}))
    assert len(list(metrics_consumer.get_metrics())) > 0
    outcomes_consumer.assert_rate_limited(
        "get_lost", categories=[DataCategory.TRANSACTION_INDEXED], ignore_other=True
    )

    relay.send_event(project_id, make_transaction({"message": "3rd tx"}))
    outcomes_consumer.assert_rate_limited(
        "get_lost",
        categories=[DataCategory.TRANSACTION, DataCategory.TRANSACTION_INDEXED],
        ignore_other=True,
        timeout=3,
    )

    with pytest.raises(HTTPError) as exc_info:
        relay.send_event(project_id, make_transaction({"message": "4nd tx"}))
    assert exc_info.value.response.status_code == 429, "Expected a 429 status code"
    outcomes_consumer.assert_rate_limited(
        "get_lost",
        categories=[DataCategory.TRANSACTION, DataCategory.TRANSACTION_INDEXED],
        ignore_other=True,
        timeout=3,
    )


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
        mini_sentry.clear_test_failures()


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
    mini_sentry.clear_test_failures()
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
    mini_sentry.clear_test_failures()


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


def test_store_content_encodings(mini_sentry, relay):
    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    url = "/api/%s/store/" % project_id

    # All of the supported content encodings should generate a 400 error.
    # An unknown/unsupported encoding generates a 415 (not supported).
    for content_encoding in ["gzip", "br", "zstd", "deflate"]:
        headers = {
            "Content-Encoding": content_encoding,
            "X-Sentry-Auth": relay.get_auth_header(project_id),
        }
        response = relay.post(url, headers=headers)
        assert response.status_code == 400, content_encoding

    headers = {
        "Content-Encoding": "this-does-not-exist",
        "X-Sentry-Auth": relay.get_auth_header(project_id),
    }
    response = relay.post(url, headers=headers)
    assert response.status_code == 415


@pytest.mark.parametrize(
    "encoding", ["identity", "zstd", "br", "gzip", "deflate", None]
)
def test_store_content_encodings_chained(mini_sentry, relay, encoding):
    relay = relay(
        relay(mini_sentry), options={"http": {"encoding": encoding} if encoding else {}}
    )
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}


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


def test_kafka_ssl(relay_with_processing):
    relay_with_processing(
        options={"kafka_config": [{"name": "ssl.key.password", "value": "foo"}]}
    )


def test_error_with_type_transaction_fixed_by_inference(
    mini_sentry, events_consumer, relay_with_processing, relay, relay_credentials
):
    """
    Ensure Relay sets the correct type for bogus payloads of errors with
    `type=transaction` some clients send.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    events_consumer = events_consumer()

    credentials = relay_credentials()
    processing = relay_with_processing(
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        },
        # normalization.level == 'default'
    )
    relay = relay(
        processing,
        credentials=credentials,
        options={
            "normalization": {
                "level": "full",
            }
        },
    )

    bogus_error = make_error({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    bogus_error["type"] = "transaction"
    envelope = Envelope()
    envelope.add_event(bogus_error)

    relay.send_envelope(project_id, envelope)

    ingested, _ = events_consumer.get_event(timeout=7)
    assert ingested["type"] == "error"
    events_consumer.assert_empty()


def test_error_with_type_transaction_fixed_by_inference_even_if_only_feature_flags(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay,
    relay_credentials,
):
    """
    Ensure Relay sets the correct type for bogus payloads of errors with
    `type=transaction` some clients send, even if configured by feature flags
    and not static config.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    events_consumer = events_consumer()

    credentials = relay_credentials()
    processing = relay_with_processing(
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        },
        # normalization.level == 'default'
    )
    relay = relay(
        processing,
        credentials=credentials,
        # normalization.level == 'default'
    )

    bogus_error = make_error({"event_id": "cbf6960622e14a45abc1f03b2055b186"})
    bogus_error["type"] = "transaction"
    envelope = Envelope()
    envelope.add_event(bogus_error)

    relay.send_envelope(project_id, envelope)

    ingested, _ = events_consumer.get_event(timeout=7)
    assert ingested["type"] == "error"
    events_consumer.assert_empty()
