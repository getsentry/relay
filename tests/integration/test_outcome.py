import json
import signal
import time
import uuid
from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
import requests
from sentry_sdk.envelope import Envelope, Item, PayloadRef
from sentry_relay.consts import DataCategory
from .asserts import time_within_delta

RELAY_ROOT = Path(__file__).parent.parent.parent

HOUR_MILLISEC = 1000 * 3600


def _disable_quota(project_config, event_type="error", reason="rate_limited"):
    project_config["config"]["quotas"] = [
        {
            "id": "drop-everything",
            "categories": [event_type],
            "limit": 0,
            "reasonCode": reason,
        }
    ]


def _send_event(relay, project_id=42, event_type="error", event_id=None, trace_id=None):
    """
    Send an event to the given project.
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


def test_outcomes_processing(relay_with_processing, mini_sentry, outcomes_consumer):
    """
    Tests outcomes are sent to the Kafka outcome topic

    Send one event to a processing Relay and verify that the event is placed on the
    Kafka outcomes topic and the event has the proper information.
    """
    outcomes_consumer = outcomes_consumer()

    _disable_quota(mini_sentry.add_full_project_config(42))

    relay = relay_with_processing()

    event = {
        "event_id": "11122233344455566677788899900011",
        "message": "hello world",
    }
    relay.send_event(42, event)

    assert outcomes_consumer.get_outcome() == {
        "category": DataCategory.ERROR,
        "key_id": 123,
        "org_id": 1,
        "outcome": 2,
        "project_id": 42,
        "quantity": 1,
        "reason": "rate_limited",
        "timestamp": time_within_delta(),
    }


def test_outcomes_custom_topic(
    mini_sentry,
    outcomes_consumer,
    processing_config,
    relay_with_processing,
    get_topic_name,
):
    """
    Tests outcomes are sent to the Kafka outcome topic, but this
    time use secondary_kafka_configs to set up the outcomes topic.
    Since we are unlikely to be able to run multiple Kafka clusters,
    we set the primary/default Kafka config to some nonsense that for
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

    outcomes_consumer = outcomes_consumer()

    project_config = mini_sentry.add_full_project_config(42)
    project_config["config"]["filterSettings"] = {"errorMessages": {"patterns": ["*"]}}

    relay = relay_with_processing(options=options)

    event = {
        "event_id": "11122233344455566677788899900011",
        "message": "hello world",
    }
    relay.send_event(42, event)

    assert outcomes_consumer.get_outcome() == {
        "category": DataCategory.ERROR,
        "key_id": 123,
        "org_id": 1,
        "outcome": 1,
        "project_id": 42,
        "quantity": 1,
        "reason": "error-message",
        "timestamp": time_within_delta(),
    }


@pytest.mark.parametrize("event_type", ["error", "transaction"])
def test_outcomes_non_processing(relay, mini_sentry, event_type):
    """
    Test basic outcome functionality.

    Send one event that generates an outcome and verify that we get an outcomes batch
    with all necessary information set.
    """
    config = {"outcomes": {"emit_outcomes": True, "source": "my-layer"}}

    _disable_quota(mini_sentry.add_full_project_config(42), event_type=event_type)

    relay = relay(mini_sentry, config)

    _send_event(relay, event_type=event_type)

    expected_categories = (
        [
            DataCategory.TRANSACTION,
            DataCategory.TRANSACTION_INDEXED,
            DataCategory.SPAN,
            DataCategory.SPAN_INDEXED,
        ]
        if event_type == "transaction"
        else [DataCategory.ERROR]
    )

    assert mini_sentry.get_aggregated_outcomes() == [
        {
            "outcome": 2,  # rate limited
            "reason": "rate_limited",
            "source": "my-layer",
            "category": category,
            "quantity": 1,
        }
        for category in expected_categories
    ]

    # no events received since all have been for an invalid project id
    assert mini_sentry.captured_envelopes.empty()


def test_outcomes_not_sent_when_disabled(relay, mini_sentry):
    """
    Test that no outcomes are sent when outcomes are disabled.

    Set batching to a very short interval and verify that we don't receive any outcome
    when we disable outcomes.
    """
    config = {"outcomes": {"emit_outcomes": False}}

    _disable_quota(mini_sentry.add_full_project_config(42))

    relay = relay(mini_sentry, config)

    _send_event(relay)

    assert mini_sentry.get_aggregated_outcomes(timeout=0.2) == []
    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize("num_intermediate_relays", [1, 3])
@pytest.mark.parametrize("event_type", ["error", "transaction"])
def test_outcome_forwarding(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    num_intermediate_relays,
    event_type,
):
    """
    Tests that Relay forwards outcomes from a chain of relays

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by the first (downstream relay)
    are properly forwarded up to sentry.
    """
    outcomes_consumer = outcomes_consumer()

    processing_config = {
        "outcomes": {
            "emit_outcomes": False,  # The default, overridden by processing.enabled: true
            "source": "processing-layer",
        }
    }

    project_config = mini_sentry.add_full_project_config(42)
    project_config["config"]["filterSettings"] = {
        "releases": {"releases": ["foo@1.2.3"]}
    }

    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(processing_config)

    intermediate_config = {
        "outcomes": {
            "emit_outcomes": True,
            "source": "intermediate-layer",
        },
        "http": {
            "global_metrics": True,
        },
    }

    # build a chain of identical relays
    for _ in range(num_intermediate_relays):
        upstream = relay(upstream, intermediate_config)

    # mark the downstream relay so we can identify outcomes originating from it
    config_downstream = deepcopy(intermediate_config)
    config_downstream["outcomes"]["source"] = "downstream-layer"

    downstream_relay = relay(upstream, config_downstream)

    _send_event(downstream_relay, event_type=event_type)

    expected_categories = [1] if event_type == "error" else [2, 9, 12, 16]

    outcomes = outcomes_consumer.get_outcomes(n=len(expected_categories))
    outcomes.sort(key=lambda x: x["category"])

    assert outcomes == [
        {
            "project_id": 42,
            "key_id": 123,
            "org_id": 1,
            "outcome": 1,  # filtered
            "source": "downstream-layer",
            "reason": "release-version",
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
            "source": "processing-layer",
        }
    }
    # The innermost Relay needs to be in processing mode
    upstream = relay_with_processing(processing_config)

    config_downstream = {
        "outcomes": {
            "emit_outcomes": True,
            "source": "downstream-layer",
        },
        "http": {
            "global_metrics": True,
        },
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
        ("transaction", ["transaction", "transaction_indexed", "span", "span_indexed"]),
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
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    relay = relay_with_processing()

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

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
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

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED,
            "quantity": 1,
        },
        {
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.SPAN_INDEXED,
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
            }
        },
    )

    _send_event(relay, event_type="error")

    report = mini_sentry.get_client_report()
    del report["timestamp"]
    assert report == {
        "discarded_events": [],
        "filtered_events": [
            {"reason": "release-version", "category": "error", "quantity": 1}
        ],
    }


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

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
            }
        },
    )

    _send_event(upstream, event_type="transaction")
    _send_event(upstream, event_type="transaction")

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "quantity": 2,
        },
        {
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
            }
        },
    )

    outcomes_consumer = outcomes_consumer()

    # Send empty body twice
    _send_event(relay)
    _send_event(relay)

    assert outcomes_consumer.get_aggregated_outcomes(n=1) == [
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
            },
        },
    )

    _send_event(relay, event_type="transaction")

    # Give relay some time to handle event
    time.sleep(0.1)

    # Shutdown relay
    relay.shutdown(sig=signal.SIGTERM)

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "outcome": 1,
            "reason": "Sampled:3000",
            "category": DataCategory.TRANSACTION_INDEXED.value,
            "quantity": 1,
        },
        {
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
    outcomes_consumer = outcomes_consumer()
    profiles_consumer = profiles_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).extend(
        ["organizations:profiling", "organizations:relay-generate-billing-outcome"]
    )
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
            "source": "processing-relay",
        },
        "http": {
            "global_metrics": True,
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

    expected_source = {
        0: "processing-relay",
        1: "pop-relay",
        # outcomes from client reports do not have a correct source (known issue)
        2: "pop-relay",
    }[num_intermediate_relays]
    expected_outcomes = [
        {
            "category": DataCategory.TRANSACTION.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "source": "processing-relay",
        },
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
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 4,
            "source": "processing-relay",
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "source": "processing-relay",
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
    outcomes = outcomes_consumer.get_aggregated_outcomes(n=12)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == expected_outcomes, outcomes

    metrics = [
        m
        for m, _ in metrics_consumer.get_metrics()
        if m["name"] == "c:spans/usage@none" and m["tags"].get("is_segment") == "true"
    ]
    assert sum(metric["value"] for metric in metrics) == 2

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
    profile_payload,
    expected_outcome,
):
    """
    Tests that Relay reports correct outcomes for invalid profiles as `Profile`.
    """
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).extend(
        ["organizations:profiling", "organizations:relay-generate-billing-outcome"]
    )

    config = {
        "outcomes": {
            "emit_outcomes": True,
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
            "category": DataCategory.TRANSACTION.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.PROFILE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": expected_outcome,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.PROFILE_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": expected_outcome,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "timestamp": time_within_delta(),
        },
    ]


def test_profile_outcomes_too_many(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
):
    """
    Tests that Relay reports duplicate profiles as invalid
    """
    outcomes_consumer = outcomes_consumer()
    profiles_consumer = profiles_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).extend(
        ["organizations:profiling", "organizations:relay-generate-billing-outcome"]
    )

    config = {
        "outcomes": {
            "emit_outcomes": True,
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

    outcomes = outcomes_consumer.get_outcomes(n=5)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": DataCategory.TRANSACTION.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.PROFILE.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_too_many_profiles",
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.PROFILE_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": 1,
            "reason": "profiling_too_many_profiles",
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "timestamp": time_within_delta(),
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "timestamp": time_within_delta(),
        },
    ]

    # One profile was accepted
    assert profiles_consumer.get_profile()


@pytest.mark.parametrize(
    "quota_category",
    ["transaction", "profile", "profile_ui"],
)
@pytest.mark.parametrize("with_platform_header", [True, False])
def test_profile_outcomes_rate_limited(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    quota_category,
    with_platform_header,
):
    """
    Profiles that are rate limited before metrics extraction should count towards `Profile`.
    Profiles that are rate limited after metrics extraction should count towards `ProfileIndexed`.
    """
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]

    project_config.setdefault("features", []).extend(
        ["organizations:profiling", "organizations:relay-generate-billing-outcome"]
    )
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
        }
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
    envelope.add_item(
        Item(
            payload=PayloadRef(bytes=profile),
            type="profile",
            headers=dict(platform="cocoa") if with_platform_header else dict(),
        )
    )
    upstream.send_envelope(project_id, envelope)

    outcomes = outcomes_consumer.get_outcomes()

    expected_categories = [
        (DataCategory.PROFILE.value, 1),
        (DataCategory.PROFILE_INDEXED.value, 1),
    ]
    # If the platform header is set, the outcome can be emitted in the fast path, for all limits,
    # if the header is missing, it can only be enforced with consistent rate limiting, which only
    # happens for the `profile_ui` category (as the rate limit can't be enforced in the fast path).
    if with_platform_header or quota_category == "profile_ui":
        expected_categories.append((DataCategory.PROFILE_UI.value, 1))

    if quota_category == "transaction":
        # Transaction got rate limited as well:
        expected_categories += [
            (DataCategory.TRANSACTION.value, 1),
            (DataCategory.TRANSACTION_INDEXED.value, 1),
            (DataCategory.SPAN.value, 2),
            (DataCategory.SPAN_INDEXED.value, 2),
        ]

    expected_outcomes = [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,  # RateLimited
            "project_id": 42,
            "quantity": quantity,
            "reason": "profiles_exceeded",
        }
        for (category, quantity) in expected_categories
    ]

    if quota_category != "transaction":
        expected_outcomes.append(
            {
                "category": DataCategory.TRANSACTION.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 0,
                "project_id": 42,
                "quantity": 1,
            }
        )

        expected_outcomes.append(
            {
                "category": DataCategory.SPAN.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 0,
                "project_id": 42,
                "quantity": 2,
            }
        )

        expected_outcomes.append(
            {
                "category": DataCategory.SPAN_INDEXED.value,
                "key_id": 123,
                "org_id": 1,
                "outcome": 0,
                "project_id": 42,
                "quantity": 2,
            }
        )

    for outcome in outcomes:
        outcome.pop("timestamp")

    outcomes.sort(key=lambda o: sorted(o.items()))
    expected_outcomes.sort(key=lambda o: sorted(o.items()))

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
        assert mini_sentry.get_aggregated_outcomes() == [
            {
                "category": DataCategory.PROFILE,
                "outcome": 2,
                "quantity": 1,
                "reason": "profiles_exceeded",
            },
            {
                "category": DataCategory.PROFILE_INDEXED,
                "outcome": 2,
                "quantity": 1,
                "reason": "profiles_exceeded",
            },
        ]
    else:
        # Do not rate limit if there is only a transaction_indexed quota.
        envelope = mini_sentry.get_captured_envelope()
        assert envelope.items[0].headers["type"] == "profile"

        assert mini_sentry.captured_outcomes.empty()
        assert mini_sentry.captured_envelopes.empty()


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
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)["config"]
    project_config.setdefault("features", []).extend(
        ["organizations:profiling", "organizations:relay-generate-billing-outcome"]
    )
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
            "source": "processing-relay",
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
            config["http"] = {"global_metrics": True}
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

    expected_source = {
        0: "processing-relay",
        1: "pop-relay",
        2: "pop-relay",
    }[num_intermediate_relays]

    outcomes = outcomes_consumer.get_aggregated_outcomes(n=10)
    outcomes.sort(key=lambda o: sorted(o.items()))

    expected_outcomes = [
        {
            "category": DataCategory.TRANSACTION.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 2,
            "source": "processing-relay",
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
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 4,
            "source": "processing-relay",
        },
        {
            "category": DataCategory.SPAN_INDEXED.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,  # Accepted
            "project_id": 42,
            "quantity": 2,
            "source": "processing-relay",
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
    ]

    assert outcomes == expected_outcomes


def test_span_outcomes_invalid(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
):
    """
    Tests that Relay reports correct outcomes for invalid spans as `Span` or `Transaction`.
    """
    outcomes_consumer = outcomes_consumer()

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "source": "pop-relay",
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

    outcomes = outcomes_consumer.get_outcomes(n=6)
    outcomes.sort(key=lambda o: sorted(o.items()))

    assert outcomes == [
        {
            "category": category,
            "key_id": 123,
            "org_id": 1,
            "outcome": 3,  # Invalid
            "project_id": 42,
            "quantity": quantity,
            "reason": reason,
            "source": "pop-relay",
            "timestamp": time_within_delta(),
        }
        for (category, quantity, reason) in [
            (DataCategory.TRANSACTION, 1, "invalid_transaction"),
            (DataCategory.TRANSACTION_INDEXED, 1, "invalid_transaction"),
            (DataCategory.SPAN, 1, "invalid_span"),
            (DataCategory.SPAN, 2, "invalid_transaction"),
            (DataCategory.SPAN_INDEXED, 1, "invalid_span"),
            (DataCategory.SPAN_INDEXED, 2, "invalid_transaction"),
        ]
    ]


def test_replay_outcomes_item_failed(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    metrics_consumer,
):
    """
    Assert Relay records a single outcome even though both envelope items fail.
    """
    outcomes_consumer = outcomes_consumer()
    metrics_consumer = metrics_consumer()

    project_id = 42
    mini_sentry.add_basic_project_config(
        project_id, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    config = {
        "outcomes": {
            "emit_outcomes": True,
            "source": "pop-relay",
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


def test_outcomes_as_metrics_forwarded_as_metrics(relay, mini_sentry):
    """
    Test forwarding of outcomes as metrics to the next upstream.
    """
    config = {"outcomes": {"emit_outcomes": True}}

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    relay = relay(mini_sentry, config)

    relay.send_client_report(
        project_id,
        {
            "timestamp": datetime.now(tz=timezone.utc).timestamp(),
            "discarded_events": [
                {"reason": "queue_overflow", "category": "error", "quantity": 42},
            ],
        },
    )

    assert mini_sentry.get_outcomes(n=1) == [
        {
            "category": DataCategory.ERROR,
            "outcome": 5,
            "quantity": 42,
            "reason": "queue_overflow",
            "timestamp": time_within_delta(),
        }
    ]

    assert mini_sentry.captured_outcomes.empty()
    assert mini_sentry.captured_envelopes.empty()


@pytest.mark.parametrize("to_billing", ["two_configs", "two_topics", False])
def test_outcomes_as_metrics_forwarded_to_kafka_billing(
    mini_sentry,
    relay,
    relay_with_processing,
    relay_credentials,
    outcomes_consumer,
    processing_config,
    get_topic_name,
    to_billing,
):
    """
    Test forwarding of outcomes as metrics and production to Kafka.

    Outcomes need to be produced to the billing topic if configured.
    """
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "categories": ["error"],
            "limit": 0,
            "reasonCode": "static_disabled_quota",
        },
        # This quota should not affect the metric outcomes.
        {
            "categories": ["metric_bucket"],
            "limit": 0,
            "reasonCode": "metric_bucket_quota",
        },
    ]

    billing_consumer = outcomes_consumer(topic="outbilling")
    outcomes_consumer = outcomes_consumer()

    consumer_with_outcome, consumer_empty = (
        (billing_consumer, outcomes_consumer)
        if to_billing
        else (outcomes_consumer, billing_consumer)
    )

    credentials = relay_credentials()
    static_relays = {
        credentials["id"]: {
            "public_key": credentials["public_key"],
            "internal": True,
        },
    }
    # Change from default, which would inherit the outcomes topic
    processing = processing_config(None)
    if to_billing == "two_configs":
        # Create an additional processing for outcomes_billing topic
        processing["processing"]["secondary_kafka_config"] = {
            "bar": processing["processing"]["kafka_config"]
        }
        processing["processing"]["topics"]["outcomes_billing"] = {
            "name": get_topic_name("outbilling"),
            "processing": "bar",
        }
    elif to_billing == "two_topics":
        processing["processing"]["topics"]["outcomes_billing"] = get_topic_name(
            "outbilling"
        )

    relay = relay(
        relay_with_processing(options=processing, static_relays=static_relays),
        options={"outcomes": {"emit_outcomes": True, "source": "aaa"}},
        credentials=credentials,
    )

    # First event does not have a cached rate limit in the first Relay.
    relay.send_event(42, {"message": "this is rate limited"})
    assert consumer_with_outcome.get_aggregated_outcomes(n=1) == [
        {
            "category": 1,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,
            "project_id": 42,
            "quantity": 1,
            "reason": "static_disabled_quota",
        },
    ]
    consumer_empty.assert_empty()

    # Second event now will actually be sent as a metric, we can verify that with the source.
    with pytest.raises(requests.HTTPError, match="429 Client Error"):
        relay.send_event(42, {"message": "this is rate limited"})
    assert consumer_with_outcome.get_aggregated_outcomes(n=1) == [
        {
            "category": 1,
            "key_id": 123,
            "org_id": 1,
            "outcome": 2,
            "project_id": 42,
            "quantity": 1,
            "reason": "static_disabled_quota",
            "source": "aaa",
        },
    ]
    consumer_empty.assert_empty()


def test_outcomes_as_metrics_forwarded_non_internal(
    mini_sentry, relay, relay_with_processing, outcomes_consumer
):
    """
    Test making sure Relay does not accept outcome metrics from non-internal Relays.
    """
    config = {"outcomes": {"emit_outcomes": True}}

    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    outcomes_consumer = outcomes_consumer()

    relay = relay(relay_with_processing(), options=config)

    relay.send_client_report(
        project_id,
        {
            "timestamp": datetime.now(tz=timezone.utc).timestamp(),
            "discarded_events": [
                {"reason": "queue_overflow", "category": "error", "quantity": 42},
            ],
        },
    )

    outcomes_consumer.assert_empty()
