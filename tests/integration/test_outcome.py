import contextlib
import json
import signal
import time
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from queue import Empty

from pytest_localserver.http import itertools
from .consts import (
    TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
)

import pytest
import requests
from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory
from .asserts import time_within_delta

from .test_metrics import metrics_by_name

RELAY_ROOT = Path(__file__).parent.parent.parent

HOUR_MILLISEC = 1000 * 3600


def test_outcomes_processing(relay_with_processing, mini_sentry, outcomes_consumer):
    """
    Tests outcomes are sent to the kafka outcome topic

    Send one event to a processing Relay and verify that the event is placed on the
    kafka outcomes topic and the event has the proper information.
    """
    relay = relay_with_processing()

    outcomes_consumer = outcomes_consumer()

    message_text = f"some message {datetime.now()}"
    event_id = "11122233344455566677788899900011"
    start = datetime.now(UTC).replace(
        microsecond=0
    )  # Outcome aggregator rounds down to seconds

    relay.send_event(
        42,
        {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
        },
    )

    outcome = outcomes_consumer.get_outcome()
    assert outcome["project_id"] == 42
    assert outcome.get("event_id") is None
    assert outcome.get("org_id") is None
    assert outcome.get("key_id") is None
    assert outcome["outcome"] == 3
    assert outcome["reason"] == "project_id"
    assert outcome.get("remote_addr") is None

    # deal with the timestamp separately (we can't control it exactly)
    event_emission = datetime.fromisoformat(outcome.get("timestamp"))
    end = datetime.now(UTC)
    assert start <= event_emission <= end


def test_outcomes_custom_topic(
    mini_sentry, outcomes_consumer, processing_config, relay, get_topic_name
):
    """
    Tests outcomes are sent to the kafka outcome topic, but this
    time use secondary_kafka_configs to set up the outcomes topic.
    Since we are unlikely to be able to run multiple kafka clusters,
    we set the primary/default kafka config to some nonsense that for
    sure won't work, so asserting that an outcome comes through
    effectively tests that the secondary config is used.
    """
    options = processing_config(None)
    kafka_config = options["processing"]["kafka_config"]

    # This kafka config becomes invalid, rdkafka warns on stdout that it will drop everything on this client
    options["processing"]["kafka_config"] = []

    options["processing"]["secondary_kafka_configs"] = {}
    options["processing"]["secondary_kafka_configs"]["foo"] = kafka_config

    # ...however, we use a custom topic config to make it work again
    options["processing"]["topics"]["outcomes"] = {
        "name": get_topic_name("outcomes"),
        "config": "foo",
    }

    relay = relay(mini_sentry, options=options)

    outcomes_consumer = outcomes_consumer()

    message_text = f"some message {datetime.now()}"
    event_id = "11122233344455566677788899900011"
    start = datetime.now(UTC).replace(
        microsecond=0
    )  # Outcome aggregator rounds down to seconds

    relay.send_event(
        42,
        {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
        },
    )

    outcome = outcomes_consumer.get_outcome()
    assert outcome["project_id"] == 42
    assert outcome.get("event_id") is None
    assert outcome.get("org_id") is None
    assert outcome.get("key_id") is None
    assert outcome["outcome"] == 3
    assert outcome["reason"] == "project_id"
    assert outcome.get("remote_addr") is None

    # deal with the timestamp separately (we can't control it exactly)
    event_emission = datetime.fromisoformat(outcome.get("timestamp"))
    end = datetime.now(UTC)
    assert start <= event_emission <= end


def test_outcomes_two_configs(
    get_topic_name, processing_config, relay, mini_sentry, outcomes_consumer
):
    """
    Tests routing outcomes to the billing and the default topic based on the outcome ID.
    """
    project_config = mini_sentry.add_basic_project_config(44)
    project_config["config"]["quotas"] = [
        {
            "categories": ["error"],
            "limit": 0,
            "reasonCode": "static_disabled_quota",
        }
    ]

    # Change from default, which would inherit the outcomes topic
    options = processing_config(None)
    # Create an additional config for outcomes_billing topic
    default_config = options["processing"]["kafka_config"]
    options["processing"]["secondary_kafka_configs"] = {"bar": default_config}
    options["processing"]["topics"]["outcomes_billing"] = {
        "name": get_topic_name("outbilling"),
        "config": "bar",
    }

    relay = relay(mini_sentry, options=options)
    billing_consumer = outcomes_consumer(topic="outbilling")
    outcomes_consumer = outcomes_consumer()

    relay.send_event(44, {"message": "this is rate limited"})
    relay.send_event(99, {"message": "wrong project"})

    rate_limited = billing_consumer.get_outcome()
    assert rate_limited["project_id"] == 44
    assert rate_limited["outcome"] == 2

    invalid = outcomes_consumer.get_outcome()
    assert invalid["project_id"] == 99
    assert invalid["outcome"] == 3

    billing_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def test_outcomes_two_topics(
    get_topic_name, processing_config, relay, mini_sentry, outcomes_consumer
):
    """
    Tests routing outcomes to the billing and the default topic based on the outcome ID.
    """
    project_config = mini_sentry.add_basic_project_config(42)
    project_config["config"]["quotas"] = [
        {
            "categories": ["error"],
            "limit": 0,
            "reasonCode": "static_disabled_quota",
        }
    ]

    # Change from default, which would inherit the outcomes topic
    options = processing_config(None)
    options["processing"]["topics"]["outcomes_billing"] = get_topic_name("billing")

    relay = relay(mini_sentry, options=options)
    billing_consumer = outcomes_consumer(topic="billing")
    outcomes_consumer = outcomes_consumer()

    relay.send_event(42, {"message": "this is rate limited"})
    relay.send_event(99, {"message": "wrong project"})

    rate_limited = billing_consumer.get_outcome()
    assert rate_limited["project_id"] == 42
    assert rate_limited["outcome"] == 2

    invalid = outcomes_consumer.get_outcome()
    assert invalid["project_id"] == 99
    assert invalid["outcome"] == 3

    billing_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def _send_event(relay, project_id=42, event_type="error", event_id=None, trace_id=None):
    """
    Send an event to the given project.

    If the project doesn't exist, relay should generate INVALID outcome with reason "project_id".
    """
    trace_id = trace_id or uuid.uuid4().hex
    event_id = event_id or uuid.uuid1().hex
    message_text = f"some message {datetime.now()}"
    if event_type == "error":
        event_body = {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
            "type": event_type,
            "environment": "production",
            "release": "foo@1.2.3",
        }
    elif event_type == "transaction":
        event_body = {
            "event_id": event_id,
            "type": "transaction",
            "transaction": "tr1",
            "start_timestamp": 1597976392.6542819,
            "timestamp": 1597976400.6189718,
            "contexts": {
                "trace": {
                    "trace_id": trace_id,
                    "span_id": "FA90FDEAD5F74052",
                    "type": "trace",
                }
            },
            "spans": [],
            "extra": {"id": event_id},
            "environment": "production",
            "release": "foo@1.2.3",
        }

    try:
        relay.send_event(project_id=project_id, payload=event_body)
    except Exception:
        pass
    return event_id


@pytest.mark.parametrize("event_type", ["error", "transaction"])
def test_outcomes_non_processing(relay, mini_sentry, event_type):
    """
    Test basic outcome functionality.

    Send one event that generates an outcome and verify that we get an outcomes batch
    with all necessary information set.
    """
    config = {"outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1}}

    relay = relay(mini_sentry, config)

    _send_event(relay, event_type=event_type)

    expected_categories = (
        [
            DataCategory.TRANSACTION,
            DataCategory.TRANSACTION_INDEXED,
        ]
        if event_type == "transaction"
        else [DataCategory.ERROR]
    )

    outcomes = []
    for _ in expected_categories:
        outcomes.extend(mini_sentry.captured_outcomes.get(timeout=3).get("outcomes"))
    assert len(outcomes) == len(expected_categories)
    outcomes.sort(key=lambda x: x["category"])

    expected_outcomes = [
        {
            "project_id": 42,
            "outcome": 3,  # invalid
            "reason": "project_id",  # missing project id
            "category": category,
            "quantity": 1,
            "timestamp": time_within_delta(),
        }
        for category in expected_categories
    ]

    assert outcomes == expected_outcomes

    # no events received since all have been for an invalid project id
    assert mini_sentry.captured_events.empty()
    assert mini_sentry.captured_outcomes.empty()


def test_outcomes_not_sent_when_disabled(relay, mini_sentry):
    """
    Test that no outcomes are sent when outcomes are disabled.

    Set batching to a very short interval and verify that we don't receive any outcome
    when we disable outcomes.
    """
    config = {
        "outcomes": {"emit_outcomes": False, "batch_size": 1, "batch_interval": 1}
    }

    relay = relay(mini_sentry, config)

    try:
        mini_sentry.captured_outcomes.get(timeout=0.2)
        # we should not be here ( previous call should have failed)
        assert False
    except Empty:
        pass  # we do expect not to get anything since we have outcomes disabled


def test_outcomes_non_processing_max_batch_time(relay, mini_sentry):
    """
    Test that outcomes are not batched more than max specified time.
    Send events at an  interval longer than max_batch_time and expect
    not to have them batched although we have a very large batch size.
    """
    events_to_send = 3
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1000,  # a huge batch size
            "batch_interval": 1,  # very short batch time
        }
    }
    relay = relay(mini_sentry, config)

    event_ids = set()
    # send one less events than the batch size (and check we don't send anything)
    for _ in range(events_to_send):
        event_id = _send_event(relay)
        event_ids.add(event_id)
        time.sleep(0.12)  # sleep more than the batch time

    # we should get one batch per event sent
    batches = []
    for _ in range(events_to_send):
        batch = mini_sentry.captured_outcomes.get(timeout=1)
        batches.append(batch)

    # verify that the batches contain one outcome each and the event_ids are ok
    for batch in batches:
        outcomes = batch.get("outcomes")
        assert len(outcomes) == 1  # one outcome per batch


def test_outcomes_non_processing_batching(relay, mini_sentry):
    """
    Test that outcomes are batched according to max size.

    Send max_outcome_batch_size events with a very large max_batch_time and expect all
    to come in one batch.
    """
    batch_size = 3
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": batch_size,
            "batch_interval": HOUR_MILLISEC,  # batch every hour
        }
    }

    relay = relay(mini_sentry, config)

    event_ids = set()
    # send one less events than the batch size (and check we don't send anything)
    for _ in range(batch_size - 1):
        event_id = _send_event(relay)
        event_ids.add(event_id)

    # nothing should be sent at this time
    try:
        mini_sentry.captured_outcomes.get(timeout=0.2)
        assert False  # the request should timeout, there is no outcome coming
    except Empty:
        pass  # yes we expect to timout since there should not be any outcome sent yet

    event_id = _send_event(relay)
    event_ids.add(event_id)

    # now we should be getting a batch
    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
    # we should have received only one outcomes batch (check nothing left)
    assert mini_sentry.captured_outcomes.qsize() == 0

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == batch_size

    # no events received since all have been for an invalid project id
    assert mini_sentry.captured_events.empty()


def test_outcome_source(relay, mini_sentry):
    """
    Test that the source is picked from configuration and passed in outcomes
    """
    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "my-layer",
        }
    }

    relay = relay(mini_sentry, config)

    _send_event(relay)

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    assert outcome.get("source") == "my-layer"


@pytest.mark.parametrize("num_intermediate_relays", [1, 3])
@pytest.mark.parametrize("event_type", ["error", "transaction"])
def test_outcome_forwarding(
    relay, relay_with_processing, outcomes_consumer, num_intermediate_relays, event_type
):
    """
    Tests that Relay forwards outcomes from a chain of relays

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by the first (downstream relay)
    are properly forwarded up to sentry.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)

    processing_config = {
        "outcomes": {
            "emit_outcomes": False,  # The default, overridden by processing.enabled: true
            "batch_size": 1,
            "batch_interval": 1,
            "source": "processing-layer",
        }
    }

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(processing_config)

    intermediate_config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "intermediate-layer",
        }
    }

    # build a chain of identical relays
    for _ in range(num_intermediate_relays):
        upstream = relay(upstream, intermediate_config)

    # mark the downstream relay so we can identify outcomes originating from it
    config_downstream = deepcopy(intermediate_config)
    config_downstream["outcomes"]["source"] = "downstream-layer"

    downstream_relay = relay(upstream, config_downstream)

    _send_event(downstream_relay, event_type=event_type)

    expected_categories = [1] if event_type == "error" else [2, 9]
    outcomes = outcomes_consumer.get_outcomes(n=len(expected_categories))
    outcomes.sort(key=lambda x: x["category"])

    assert outcomes == [
        {
            "project_id": 42,
            "outcome": 3,  # Invalid
            "source": "downstream-layer",
            "reason": "project_id",
            "category": category,
            "quantity": 1,
            "timestamp": time_within_delta(),
        }
        for category in expected_categories
    ]


def test_outcomes_forwarding_rate_limited(
    mini_sentry, relay, relay_with_processing, outcomes_consumer
):
    """
    Tests that external relays do not emit duplicate outcomes for forwarded messages.

    External relays should not produce outcomes for messages already forwarded to the upstream.
    In this test, we send two events that should be rate-limited. The first one is dropped in the
    upstream (processing) relay, and the second -- in the downstream, because it should cache the
    rate-limited response from the upstream.
    In total, two outcomes have to be emitted:
    - The first one from the upstream relay
    - The second one is emittedy by the downstream, and then sent to the upstream that writes it
      to Kafka.
    """
    outcomes_consumer = outcomes_consumer()

    processing_config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "processing-layer",
        }
    }
    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(processing_config)

    config_downstream = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "source": "downstream-layer",
        }
    }
    downstream_relay = relay(upstream, config_downstream)

    # Create project config
    project_id = 42
    category = "error"
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": "drop-everything",
            "categories": [category],
            "limit": 0,
            "window": 1600,
            "reasonCode": "rate_limited",
        }
    ]

    # Send an event, it should be dropped in the upstream (processing) relay
    downstream_relay.send_event(project_id, _get_event_payload(category))

    outcome = outcomes_consumer.get_outcome()
    outcome.pop("timestamp")
    expected_outcome = {
        "reason": "rate_limited",
        "org_id": 1,
        "key_id": 123,
        "outcome": 2,
        "project_id": 42,
        "source": "processing-layer",
        "category": 1,
        "quantity": 1,
    }
    assert outcome == expected_outcome

    # Send another event, now the downstream should drop it because it'll cache the 429
    # response from the previous event, but the outcome should be emitted
    with pytest.raises(requests.exceptions.HTTPError, match="429 Client Error"):
        downstream_relay.send_event(project_id, _get_event_payload(category))

    expected_outcome_from_downstream = deepcopy(expected_outcome)
    expected_outcome_from_downstream["source"] = "downstream-layer"

    outcome = outcomes_consumer.get_outcome()
    outcome.pop("timestamp")

    assert outcome == expected_outcome_from_downstream

    outcomes_consumer.assert_empty()


def _get_event_payload(data_category):
    if data_category == "error":
        return {"message": "hello"}
    elif data_category == "transaction":
        now = datetime.now(UTC)
        return {
            "type": "transaction",
            "timestamp": now.isoformat(),
            "start_timestamp": (now - timedelta(seconds=2)).isoformat(),
            "spans": [
                {
                    "op": "default",
                    "span_id": "968cff94913ebb07",
                    "segment_id": "968cff94913ebb07",
                    "start_timestamp": now.timestamp(),
                    "timestamp": now.timestamp() + 1,
                    "exclusive_time": 1000.0,
                    "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                },
            ],
            "contexts": {
                "trace": {
                    "op": "hi",
                    "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                    "span_id": "968cff94913ebb07",
                }
            },
            "transaction": "hi",
        }
    elif data_category == "user_report_v2":
        return {
            "type": "userreportv2",
            "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
            "timestamp": 1597977777.6189718,
            "dist": "1.12",
            "platform": "javascript",
            "environment": "production",
            "release": 42,
            "tags": {"transaction": "/organizations/:orgId/performance/:eventSlug/"},
            "sdk": {"name": "name", "version": "veresion"},
            "user": {
                "id": "123",
                "username": "user",
                "email": "user@site.com",
                "ip_address": "192.168.11.12",
            },
            "request": {
                "url": None,
                "headers": {
                    "user-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15"
                },
            },
            "contexts": {
                "feedback": {
                    "message": "test message",
                    "contact_email": "test@example.com",
                    "type": "feedback",
                },
                "trace": {
                    "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                    "span_id": "FA90FDEAD5F74052",
                    "type": "trace",
                },
                "replay": {
                    "replay_id": "e2d42047b1c5431c8cba85ee2a8ab25d",
                },
            },
        }
    else:
        raise Exception("Invalid event type")


def _get_profile_payload(metadata_only=True):
    profile = {
        "event_id": "41fed0925670468bb0457f61a74688ec",
        "version": "1",
        "os": {"name": "iOS", "version": "16.0", "build_number": "19H253"},
        "device": {
            "architecture": "arm64e",
            "is_emulator": False,
            "locale": "en_US",
            "manufacturer": "Apple",
            "model": "iPhone14,3",
        },
        "timestamp": "2022-09-01T09:45:00.000Z",
        "release": "0.1 (199)",
        "platform": "cocoa",
        "debug_meta": {
            "images": [
                {
                    "debug_id": "32420279-25E2-34E6-8BC7-8A006A8F2425",
                    "image_addr": "0x000000010258c000",
                    "code_file": "/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies",
                    "type": "macho",
                    "image_size": 1720320,
                    "image_vmaddr": "0x0000000100000000",
                }
            ]
        },
        "transactions": [
            {
                "name": "example_ios_movies_sources.MoviesViewController",
                "trace_id": "4b25bc58f14243d8b208d1e22a054164",
                "id": "30976f2ddbe04ac9b6bffe6e35d4710c",
                "active_thread_id": "259",
                "relative_start_ns": "500500",
                "relative_end_ns": "50500500",
            }
        ],
    }
    if metadata_only:
        return profile
    profile["profile"] = {
        "samples": [
            {
                "stack_id": 0,
                "thread_id": "1",
                "queue_address": "0x0000000102adc700",
                "elapsed_since_start_ns": "10500500",
            },
            {
                "stack_id": 1,
                "thread_id": "1",
                "queue_address": "0x0000000102adc700",
                "elapsed_since_start_ns": "20500500",
            },
            {
                "stack_id": 0,
                "thread_id": "1",
                "queue_address": "0x0000000102adc700",
                "elapsed_since_start_ns": "30500500",
            },
            {
                "stack_id": 1,
                "thread_id": "1",
                "queue_address": "0x0000000102adc700",
                "elapsed_since_start_ns": "40500500",
            },
        ],
        "stacks": [[0], [1]],
        "frames": [
            {"instruction_addr": "0xa722447ffffffffc"},
            {"instruction_addr": "0x442e4b81f5031e58"},
        ],
        "thread_metadata": {"1": {"priority": 31}, "2": {}},
        "queue_metadata": {
            "0x0000000102adc700": {"label": "com.apple.main-thread"},
            "0x000000016d8fb180": {"label": "com.apple.network.connections"},
        },
    }
    return profile


def _get_span_payload():
    now = datetime.now(UTC)
    return {
        "op": "default",
        "span_id": "968cff94913ebb07",
        "segment_id": "968cff94913ebb07",
        "start_timestamp": now.timestamp(),
        "timestamp": now.timestamp() + 1,
        "exclusive_time": 1000.0,
        "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
    }


@pytest.mark.parametrize(
    "category,outcome_categories",
    [
        ("session", []),
        ("transaction", ["transaction", "transaction_indexed"]),
        ("user_report_v2", ["user_report_v2"]),
    ],
)
def test_outcomes_rate_limit(
    relay_with_processing, mini_sentry, outcomes_consumer, category, outcome_categories
):
    """
    Tests that outcomes are emitted or not, depending on the type of message.

    Pass a transaction that is rate limited and check whether a rate limit outcome is emitted.
    """

    config = {"outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1}}
    relay = relay_with_processing(config)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    reason_code = "transactions are banned"
    project_config["config"]["quotas"] = [
        {
            "id": "transaction category",
            "categories": [category],
            "limit": 0,
            "window": 1600,
            "reasonCode": reason_code,
        }
    ]

    project_config["config"]["features"] = ["organizations:user-feedback-ingest"]

    outcomes_consumer = outcomes_consumer()

    if category == "session":
        timestamp = datetime.now(tz=timezone.utc)
        started = timestamp - timedelta(hours=1)
        payload = {
            "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
            "did": "foobarbaz",
            "seq": 42,
            "init": True,
            "timestamp": timestamp.isoformat(),
            "started": started.isoformat(),
            "duration": 1947.49,
            "status": "exited",
            "errors": 0,
            "attrs": {"release": "sentry-test@1.0.0", "environment": "production"},
        }
        relay.send_session(project_id, payload)
    else:
        relay.send_event(project_id, _get_event_payload(category))

    if outcome_categories:
        outcomes_consumer.assert_rate_limited(
            reason_code, categories=outcome_categories
        )
    else:
        outcomes_consumer.assert_empty()


def test_outcome_to_client_report(relay, mini_sentry):
    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }
    project_config["config"]["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ],
    }

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
                "emit_client_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
            }
        },
    )

    downstream = relay(
        upstream,
        {
            "outcomes": {
                "emit_outcomes": "as_client_reports",
                "source": "downstream-layer",
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    _send_event(downstream, event_type="transaction")

    outcomes_batches = [
        mini_sentry.captured_outcomes.get(timeout=3.2),
        mini_sentry.captured_outcomes.get(timeout=3.2),
    ]
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch
    assert mini_sentry.captured_events.qsize() == 0

    outcomes = list(
        itertools.chain.from_iterable(o.get("outcomes") for o in outcomes_batches)
    )
    outcomes.sort(key=lambda o: o["category"])

    assert outcomes == [
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 1,
        },
    ]


def test_filtered_event_outcome_client_reports(relay, mini_sentry):
    """Make sure that an event filtered by non-processing relay will create client reports"""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["releases"] = {"releases": ["foo@1.2.3"]}

    relay = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": "as_client_reports",
                "source": "downstream-layer",
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    _send_event(relay, event_type="error")

    report = mini_sentry.get_client_report(timeout=10)
    del report["timestamp"]
    assert report == {
        "discarded_events": [],
        "filtered_events": [
            {"reason": "release-version", "category": "error", "quantity": 1}
        ],
    }


def test_filtered_event_outcome_kafka(relay, mini_sentry):
    """Make sure that an event filtered by non-processing relay will create outcomes in kafka"""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["releases"] = {"releases": ["foo@1.2.3"]}

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    downstream = relay(
        upstream,
        {
            "outcomes": {
                "emit_outcomes": "as_client_reports",
                "source": "downstream-layer",
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    _send_event(downstream, event_type="error")

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=3.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch
    assert mini_sentry.captured_events.qsize() == 0

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    del outcome["timestamp"]

    expected_outcome = {
        "org_id": 1,
        "project_id": 42,
        "key_id": 123,
        # no event ID because it was a client report
        "outcome": 1,
        "reason": "release-version",
        "category": 1,
        "quantity": 1,
        # no remote_addr because it was a client report
    }
    assert outcome == expected_outcome


def test_outcomes_aggregate_dynamic_sampling(relay, mini_sentry):
    """Dynamic sampling is aggregated"""
    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ],
    }

    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    _send_event(upstream, event_type="transaction")
    _send_event(upstream, event_type="transaction")

    outcomes_batches = [
        mini_sentry.captured_outcomes.get(timeout=1.2),
        mini_sentry.captured_outcomes.get(timeout=1.2),
    ]
    assert mini_sentry.captured_outcomes.qsize() == 0
    assert mini_sentry.captured_events.qsize() == 0

    outcomes = list(
        itertools.chain.from_iterable(o.get("outcomes") for o in outcomes_batches)
    )
    outcomes.sort(key=lambda o: o["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "quantity": 2,
        },
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 2,
        },
    ]


def test_outcomes_aggregate_inbound_filters(
    relay, relay_with_processing, mini_sentry, outcomes_consumer
):
    """Make sure that inbound filters outcomes are aggregated"""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["releases"] = {"releases": ["foo@1.2.3"]}

    relay = relay_with_processing(
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {
                    "flush_interval": 1,
                },
            }
        },
    )

    outcomes_consumer = outcomes_consumer(timeout=1.2)

    # Send empty body twice
    _send_event(relay)
    _send_event(relay)

    outcomes = outcomes_consumer.get_outcomes(timeout=5)
    assert len(outcomes) == 1, outcomes

    for outcome in outcomes:
        del outcome["timestamp"]

    # Results in a single aggregated outcome:
    assert outcomes == [
        {
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "release-version",
            "category": 1,
            "quantity": 2,
        }
    ]


def test_graceful_shutdown(relay, mini_sentry):
    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION
    }
    project_config["config"]["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ],
    }

    relay = relay(
        mini_sentry,
        options={
            "limits": {"shutdown_timeout": 1},
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {
                    "flush_interval": 10,
                },
            },
        },
    )

    _send_event(relay, event_type="transaction")

    # Give relay some time to handle event
    time.sleep(0.1)

    # Shutdown relay
    relay.shutdown(sig=signal.SIGTERM)

    # We should have outcomes almost immediately through force flush:
    outcomes_batches = [
        mini_sentry.captured_outcomes.get(timeout=1.2),
        mini_sentry.captured_outcomes.get(timeout=1.2),
    ]
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = list(
        itertools.chain.from_iterable(o.get("outcomes") for o in outcomes_batches)
    )
    outcomes.sort(key=lambda o: o["category"])
    assert outcomes == [
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "quantity": 1,
        },
        {
            "timestamp": time_within_delta(delta=timedelta(minutes=2)),
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.SPAN_INDEXED.value,
            "quantity": 1,
        },
    ]


@pytest.mark.parametrize("num_intermediate_relays", [0, 1, 2])
def test_profile_outcomes(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    num_intermediate_relays,
    metrics_consumer,
):
    """
    Tests that Relay reports correct outcomes for profiles.

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by the first relay
    are properly forwarded up to sentry.
    """
    outcomes_consumer = outcomes_consumer(timeout=5)
    profiles_consumer = profiles_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
    }
    project_config["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {
                    "op": "eq",
                    "name": "event.transaction",
                    "value": "hi",
                },
            }
        ],
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "processing-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(config)

    # build a chain of relays
    for i in range(num_intermediate_relays):
        config = deepcopy(config)
        if i == 0:
            # Emulate a PoP Relay
            config["outcomes"]["source"] = "pop-relay"
        if i == 1:
            # Emulate a customer Relay
            config["outcomes"]["source"] = "external-relay"
            config["outcomes"]["emit_outcomes"] = "as_client_reports"
        upstream = relay(upstream, config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v1/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    def make_envelope(transaction_name):
        payload = _get_event_payload("transaction")
        payload["transaction"] = transaction_name
        envelope = Envelope()
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile"))
        envelope.add_item(Item(payload=PayloadRef(bytes=b"foobar"), type="attachment"))
        return envelope

    upstream.send_envelope(
        project_id, make_envelope("hi")
    )  # should get dropped by dynamic sampling
    upstream.send_envelope(
        project_id, make_envelope("ho")
    )  # should be kept by dynamic sampling

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_source = {
        0: "processing-relay",
        1: "pop-relay",
        # outcomes from client reports do not have a correct source (known issue)
        2: "pop-relay",
    }[num_intermediate_relays]
    expected_outcomes = [
        {
            "category": DataCategory.ATTACHMENT.value,  # attachment
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": 6,  # len(b"foobar")
            "reason": "Sampled:3000",
            "source": expected_source,
        },
        {
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,  # Filtered
            "project_id": 42,
            "quantity": 1,
            "reason": "Sampled:3000",
            "source": expected_source,
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,  # Filtered
            "project_id": 42,
            "quantity": 2,
            "reason": "Sampled:3000",
            "source": expected_source,
        },
        {
            "category": DataCategory.ATTACHMENT_ITEM.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,
            "project_id": 42,
            "quantity": 1,  # number of attachments
            "reason": "Sampled:3000",
            "source": expected_source,
        },
    ]
    for outcome in outcomes:
        outcome.pop("timestamp")

    metrics = [
        m
        for m, _ in metrics_consumer.get_metrics()
        if m["name"] == "c:transactions/usage@none"
    ]
    assert sum(metric["value"] for metric in metrics) == 2

    assert outcomes == expected_outcomes, outcomes

    assert profiles_consumer.get_profile()
    assert profiles_consumer.get_profile()


@pytest.mark.parametrize(
    "profile_payload,expected_outcome",
    [
        # Completely invalid header
        (b"foobar", "profiling_invalid_json"),
        # Invalid platform -> invalid profile, but a valid profile header
        (
            b"""{"profile_id":"11111111111111111111111111111111","version":"2","platform":"<this does not exist>"}""",
            "profiling_platform_not_supported",
        ),
    ],
)
def test_profile_outcomes_invalid(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    metrics_consumer,
    profile_payload,
    expected_outcome,
):
    """
    Tests that Relay reports correct outcomes for invalid profiles as `Profile`.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "pop-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    upstream = relay_with_processing(config)

    # Create an envelope with an invalid profile:
    def make_envelope():
        payload = _get_event_payload("transaction")
        envelope = Envelope()
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        envelope.add_item(
            Item(payload=PayloadRef(bytes=profile_payload), type="profile")
        )

        return envelope

    envelope = make_envelope()
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": expected_outcome,
            "source": "pop-relay",
            "timestamp": time_within_delta(),
        }
        for category in [6, 11]  # Profile, ProfileIndexed
    ]

    # Make sure the profile will not be counted as accepted:
    metrics = metrics_by_name(metrics_consumer, 4)
    assert "has_profile" not in metrics["d:transactions/duration@millisecond"]["tags"]
    assert "has_profile" not in metrics["c:transactions/usage@none"]["tags"]


def test_profile_outcomes_too_many(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
):
    """
    Tests that Relay reports duplicate profiles as invalid
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    profiles_consumer = profiles_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MAX_SUPPORTED_VERSION,
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "pop-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    upstream = relay_with_processing(config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v1/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    # Create an envelope with an invalid profile:
    def make_envelope():
        payload = _get_event_payload("transaction")
        envelope = Envelope()
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile"))
        envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile"))

        return envelope

    envelope = make_envelope()
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_too_many_profiles",
            "source": "pop-relay",
            "timestamp": time_within_delta(),
        }
        for category in [6, 11]  # Profile, ProfileIndexed
    ]

    # One profile was accepted
    assert profiles_consumer.get_profile()


def test_profile_outcomes_data_invalid(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    metrics_consumer,
):
    """
    Tests that Relay reports correct outcomes for invalid profiles as `Profile`.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "processing-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    upstream = relay_with_processing(config)

    # Create an envelope with an invalid profile:
    def make_envelope():
        payload = _get_event_payload("transaction")
        envelope = Envelope()
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(_get_profile_payload()).encode()),
                type="profile",
            )
        )
        return envelope

    envelope = make_envelope()
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes(n=2)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_invalid_json",
            "source": "processing-relay",
            "timestamp": time_within_delta(),
        }
        for category in [6, 11]  # Profile, ProfileIndexed
    ]

    metrics = metrics_by_name(metrics_consumer, 4)
    assert "has_profile" not in metrics["d:transactions/duration@millisecond"]["tags"]
    assert "has_profile" not in metrics["c:transactions/usage@none"]["tags"]


@pytest.mark.parametrize("quota_category", ["transaction", "profile"])
def test_profile_outcomes_rate_limited(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    quota_category,
):
    """
    Profiles that are rate limited before metrics extraction should count towards `Profile`.
    Profiles that are rate limited after metrics extraction should count towards `ProfileIndexed`.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": [quota_category],
            "limit": 0,
            "reasonCode": "profiles_exceeded",
        }
    ]

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 0,
            },
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    upstream = relay_with_processing(config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v1/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    # Create an envelope with an invalid profile:
    payload = _get_event_payload("transaction")
    envelope = Envelope()
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=json.dumps(payload).encode()),
            type="transaction",
        )
    )
    envelope.add_item(Item(payload=PayloadRef(bytes=profile), type="profile"))
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_categories = [
        DataCategory.PROFILE,
        DataCategory.PROFILE_INDEXED,
    ]  # Profile, ProfileIndexed
    if quota_category == "transaction":
        # Transaction got rate limited as well:
        expected_categories += [
            DataCategory.TRANSACTION,
            DataCategory.TRANSACTION_INDEXED,
        ]  # Transaction, TransactionIndexed
    expected_categories.sort()

    expected_outcomes = [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,  # RateLimited
            "project_id": 42,
            "quantity": 1,
            "reason": "profiles_exceeded",
            "timestamp": time_within_delta(),
        }
        for category in expected_categories
    ]

    assert outcomes == expected_outcomes, outcomes


@pytest.mark.parametrize("quota_category", ["transaction", "transaction_indexed"])
def test_profile_outcomes_rate_limited_when_dynamic_sampling_drops(
    mini_sentry,
    relay,
    quota_category,
):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).append("organizations:profiling")
    project_config["quotas"] = [
        {
            "id": f"test_rate_limiting_{uuid.uuid4().hex}",
            "categories": [quota_category],
            "limit": 0,
            "reasonCode": "profiles_exceeded",
        }
    ]

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 0,
            },
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    relay = relay(mini_sentry, options=config)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v1/valid.json",
        "rb",
    ) as f:
        profile = f.read()

    # Create an envelope with an invalid profile:
    envelope = Envelope()
    profile_item = Item(payload=PayloadRef(bytes=profile), type="profile")
    profile_item.headers["sampled"] = False
    envelope.add_item(profile_item)
    relay.send_envelope(project_id, envelope)

    if quota_category == "transaction":
        (outcome1,) = mini_sentry.captured_outcomes.get(timeout=10)["outcomes"]
        (outcome2,) = mini_sentry.captured_outcomes.get(timeout=1)["outcomes"]
        outcome1, outcome2 = sorted([outcome1, outcome2], key=lambda o: o["category"])
        assert outcome1["outcome"] == 2  # rate limited
        assert outcome1["category"] == DataCategory.PROFILE
        assert outcome1["quantity"] == 1
        assert outcome2["outcome"] == 2  # rate limited
        assert outcome2["category"] == DataCategory.PROFILE_INDEXED
        assert outcome2["quantity"] == 1

        assert mini_sentry.captured_events.empty()
    else:
        # Do not rate limit if there is only a transaction_indexed quota.
        envelope = mini_sentry.captured_events.get(timeout=10)
        assert envelope.items[0].headers["type"] == "profile"

        assert mini_sentry.captured_outcomes.empty()


def test_global_rate_limit(
    mini_sentry, relay_with_processing, metrics_consumer, outcomes_consumer
):
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

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

    metric_bucket_limit = 9

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    mini_sentry.add_dsn_key_to_project(project_id)

    now = datetime.now(UTC).timestamp()

    projectconfig["config"]["quotas"] = [
        {
            "id": "test_rate_limiting" + str(uuid.uuid4()),
            "scope": "global",
            "categories": ["metric_bucket"],
            "limit": metric_bucket_limit,
            # Ensures we begin at the start of a slot so we don't go to next slot in the middle of the test
            "window": int(now),
            "reasonCode": "global rate limit hit",
        }
    ]

    def send_buckets(n):
        buckets = [
            {
                "org_id": 1,
                "project_id": project_id,
                "timestamp": now,
                "name": "d:transactions/measurements.lcp@millisecond",
                "type": "d",
                "value": [1.0],
                "width": bucket_interval,
                "tags": {"foo": str(i)},
            }
            for i in range(n)
        ]

        relay.send_metrics_buckets(project_id, buckets)
        time.sleep(5)

    def assert_metrics_outcomes(n_metrics, n_outcomes):
        produced_buckets = [m for m, _ in metrics_consumer.get_metrics()]
        outcomes = outcomes_consumer.get_outcomes()

        assert len(produced_buckets) == n_metrics
        assert len(outcomes) == n_outcomes

        for outcome in outcomes:
            assert outcome["reason"] == "global rate limit hit"

    # Send the exact amount allowed
    send_buckets(metric_bucket_limit)
    assert_metrics_outcomes(metric_bucket_limit, 0)

    # Send more once the limit is hit and make sure they are rejected.
    send_buckets(1)
    assert_metrics_outcomes(0, 1)

    # Subsequent requests should expose the rate limit via 429
    with pytest.raises(HTTPError, match="429 Client Error"):
        send_buckets(1)
    assert_metrics_outcomes(0, 1)


@pytest.mark.parametrize("num_intermediate_relays", [0, 1, 2])
def test_span_outcomes(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    num_intermediate_relays,
):
    """
    Tests that Relay reports correct outcomes for spans.

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by the first relay
    are properly forwarded up to sentry.
    """
    outcomes_consumer = outcomes_consumer(timeout=5)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).extend(
        [
            "organizations:indexed-spans-extraction",
        ]
    )
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }
    project_config["sampling"] = {
        "version": 2,
        "rules": [
            {
                "id": 3001,
                "samplingValue": {"type": "sampleRate", "value": 0.0},
                "type": "transaction",
                "condition": {
                    "op": "eq",
                    "name": "event.transaction",
                    "value": "hi",
                },
            }
        ],
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "processing-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(config)

    # build a chain of relays
    for i in range(num_intermediate_relays):
        config = deepcopy(config)
        if i == 0:
            # Emulate a PoP Relay
            config["outcomes"]["source"] = "pop-relay"
        if i == 1:
            # Emulate a customer Relay
            config["outcomes"]["source"] = "external-relay"
            config["outcomes"]["emit_outcomes"] = "as_client_reports"
        upstream = relay(upstream, config)

    def make_envelope(transaction_name):
        payload = _get_event_payload("transaction")
        payload["transaction"] = transaction_name
        envelope = Envelope()
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        return envelope

    upstream.send_envelope(
        project_id, make_envelope("hi")
    )  # should get dropped by dynamic sampling
    upstream.send_envelope(
        project_id, make_envelope("ho")
    )  # should be kept by dynamic sampling

    outcomes = outcomes_consumer.get_outcomes(timeout=10.0)
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_source = {
        0: "processing-relay",
        1: "pop-relay",
        2: "pop-relay",
    }[num_intermediate_relays]

    assert outcomes == [
        {
            "timestamp": time_within_delta(),
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,  # Filtered
            "project_id": 42,
            "quantity": 1,
            "reason": "Sampled:3000",
            "source": expected_source,
        },
        {
            "timestamp": time_within_delta(),
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,  # Filtered
            "project_id": 42,
            "quantity": 2,
            "reason": "Sampled:3000",
            "source": expected_source,
        },
    ]


def test_span_outcomes_invalid(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
):
    """
    Tests that Relay reports correct outcomes for invalid spans as `Span` or `Transaction`.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).extend(
        [
            "organizations:standalone-span-ingestion",
        ]
    )
    project_config["transactionMetrics"] = {
        "version": TRANSACTION_EXTRACT_MIN_SUPPORTED_VERSION,
    }

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "pop-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }
    upstream = relay_with_processing(config)

    # Create an envelope with an invalid profile:
    def make_envelope():
        envelope = Envelope()
        payload = _get_event_payload("transaction")
        payload["spans"][0].pop("span_id", None)
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="transaction",
            )
        )
        payload = _get_span_payload()
        payload.pop("span_id", None)
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(payload).encode()),
                type="span",
            )
        )
        return envelope

    envelope = make_envelope()
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes(timeout=10.0, n=4)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": reason,
            "source": "pop-relay",
            "timestamp": time_within_delta(),
        }
        for (category, reason) in [
            (DataCategory.TRANSACTION, "invalid_transaction"),
            (DataCategory.TRANSACTION_INDEXED, "invalid_transaction"),
            (DataCategory.SPAN, "invalid_span"),
            (DataCategory.SPAN_INDEXED, "invalid_span"),
        ]
    ]


def test_global_rate_limit_by_namespace(
    mini_sentry, relay_with_processing, outcomes_consumer, metrics_consumer
):
    """
    Checks that we can hit a namespace quota first, and then have more quota left for the global limit.
    """
    outcomes_consumer = outcomes_consumer()
    metrics_consumer = metrics_consumer()

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

    metric_bucket_limit = 9
    transaction_limit = 5

    project_id = 42
    projectconfig = mini_sentry.add_full_project_config(project_id)
    mini_sentry.add_dsn_key_to_project(project_id)

    global_reason_code = "global rate limit hit"
    transaction_reason_code = "global rate limit for transactions hit"
    expect_429 = False

    unique_id = str(uuid.uuid4())
    projectconfig["config"]["quotas"] = [
        {
            "id": "testlimit" + unique_id,
            "scope": "global",
            "categories": ["metric_bucket"],
            "limit": metric_bucket_limit,
            "window": int(datetime.now(UTC).timestamp()) - 1,
            "reasonCode": global_reason_code,
        },
        {
            "id": "testlimit" + unique_id,
            "scope": "global",
            "categories": ["metric_bucket"],
            "limit": transaction_limit,
            "namespace": "transactions",
            "window": int(datetime.now(UTC).timestamp()) - 1,
            "reasonCode": transaction_reason_code,
        },
    ]

    # Truncate the timestamp and add a slight offset to never be on the border of the rate limiting window.
    ts = datetime.now(UTC).timestamp()

    def send_buckets(n, name, value, ty):
        for i in range(n):
            bucket = [
                {
                    "org_id": 1,
                    "project_id": project_id,
                    "timestamp": ts,
                    "name": name,
                    "type": ty,
                    "value": value,
                    "width": bucket_interval,
                    "tags": {"foo": str(i)},
                }
            ]

            envelope = Envelope()
            envelope.add_item(
                Item(payload=PayloadRef(json=bucket), type="metric_buckets")
            )

            maybe_raises = (
                pytest.raises(HTTPError, match="429 Client Error")
                if expect_429
                else contextlib.nullcontext()
            )
            with maybe_raises:
                relay.send_envelope(project_id, envelope)

        time.sleep(3)

    transaction_name = "d:transactions/measurements.lcp@millisecond"
    transaction_value = [1.0]

    session_name = "s:sessions/user@none"
    session_value = [12345423]

    # Send as many transactions as we can.
    send_buckets(transaction_limit, transaction_name, transaction_value, "d")

    metrics = metrics_consumer.get_metrics(timeout=10, n=5)
    assert len(metrics) == 5

    # The next request will trigger a rate limit, AFTER this request we should get 429s
    send_buckets(1, transaction_name, transaction_value, "d")
    expect_429 = True

    # assert we hit the transaction throughput limit configured.
    outcomes = outcomes_consumer.get_outcomes(timeout=10, n=1)
    assert len(outcomes) == 1
    assert outcomes[0]["reason"] == transaction_reason_code
    metrics_consumer.assert_empty()

    # Fill up the global limit
    global_quota_remaining = metric_bucket_limit - transaction_limit
    send_buckets(global_quota_remaining, session_name, session_value, "s")

    # Assert we didn't get ratelimited
    metrics = metrics_consumer.get_metrics(timeout=10, n=4)
    assert len(metrics) == 4
    outcomes_consumer.assert_empty()

    # Send more than we have of global quota.
    send_buckets(1, session_name, session_value, "s")

    # Assert we hit the global limit
    outcomes = outcomes_consumer.get_outcomes(timeout=10, n=1)
    assert len(outcomes) == 1
    assert outcomes[0]["reason"] == global_reason_code


def test_replay_outcomes_item_failed(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    metrics_consumer,
):
    """
    Assert Relay records a single outcome even though both envelope items fail.
    """
    outcomes_consumer = outcomes_consumer(timeout=2)
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "batch_size": 1,
            "batch_interval": 1,
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 1,
            },
            "source": "pop-relay",
        },
        "aggregator": {
            "bucket_interval": 1,
            "initial_delay": 0,
        },
    }

    upstream = relay_with_processing(config)

    def make_envelope():
        envelope = Envelope(headers=[["event_id", "515539018c9b4260a6f999572f1661ee"]])
        envelope.add_item(
            Item(payload=PayloadRef(bytes=b"not valid"), type="replay_event")
        )
        envelope.add_item(
            Item(payload=PayloadRef(bytes=b"still not valid"), type="replay_recording")
        )

        return envelope

    envelope = make_envelope()
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes(n=1)
    assert len(outcomes) == 1

    expected = {
        "category": 7,
        "key_id": 123,
        "outcome": 3,
        "project_id": 42,
        "quantity": 2,
        "reason": "invalid_replay",
        "source": "pop-relay",
    }
    expected["timestamp"] = outcomes[0]["timestamp"]
    assert outcomes[0] == expected
