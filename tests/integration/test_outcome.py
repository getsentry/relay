import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from queue import Empty
import signal

import requests
import pytest
import time

HOUR_MILLISEC = 1000 * 3600


def test_outcomes_processing(relay_with_processing, mini_sentry, outcomes_consumer):
    """
    Tests outcomes are sent to the kafka outcome topic

    Send one event to a processing Relay and verify that the event is placed on the
    kafka outcomes topic and the event has the proper information.
    """
    relay = relay_with_processing()

    outcomes_consumer = outcomes_consumer()

    message_text = "some message {}".format(datetime.now())
    event_id = "11122233344455566677788899900011"
    start = datetime.utcnow().replace(
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
    timestamp = outcome.get("timestamp")
    event_emission = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.utcnow()
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

    message_text = "some message {}".format(datetime.now())
    event_id = "11122233344455566677788899900011"
    start = datetime.utcnow().replace(
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
    timestamp = outcome.get("timestamp")
    event_emission = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.utcnow()
    assert start <= event_emission <= end


def test_outcomes_two_topics(
    get_topic_name, processing_config, relay, mini_sentry, outcomes_consumer
):
    """
    Tests routing outcomes to the billing and the default topic based on the outcome ID.
    """
    mini_sentry.add_basic_project_config(42)

    # Change from default, which would inherit the outcomes topic
    options = processing_config(None)
    options["processing"]["topics"]["outcomes_billing"] = get_topic_name("billing")

    relay = relay(mini_sentry, options=options)
    billing_consumer = outcomes_consumer(topic="billing")
    outcomes_consumer = outcomes_consumer()

    relay.send_event(42, {"message": "this is valid"})
    relay.send_event(99, {"message": "wrong project"})

    accepted = billing_consumer.get_outcome()
    assert accepted["project_id"] == 42
    assert accepted["outcome"] == 0

    invalid = outcomes_consumer.get_outcome()
    assert invalid["project_id"] == 99
    assert invalid["outcome"] == 3

    billing_consumer.assert_empty()
    outcomes_consumer.assert_empty()


def _send_event(relay, project_id=42, event_type="error", event_id=None):
    """
    Send an event to the given project.

    If the project doesn't exist, relay should generate INVALID outcome with reason "project_id".
    """
    event_id = event_id or uuid.uuid1().hex
    message_text = "some message {}".format(datetime.now())
    event_body = {
        "event_id": event_id,
        "message": message_text,
        "extra": {"msg_text": message_text},
        "type": event_type,
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

    event_id = _send_event(relay, event_type=event_type)

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    del outcome["timestamp"]  # 'timestamp': '2020-06-03T16:18:59.259447Z'

    expected_outcome = {
        "project_id": 42,
        "outcome": 3,  # invalid
        "reason": "project_id",  # missing project id
        "category": 2 if event_type == "transaction" else 1,
        "quantity": 1,
    }
    assert outcome == expected_outcome

    # no events received since all have been for an invalid project id
    assert mini_sentry.captured_events.empty()


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
        assert False  # we should not be here ( previous call should have failed)
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
    and verify that the outcomes sent by  the first (downstream relay)
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

    event_id = _send_event(downstream_relay, event_type=event_type)

    outcome = outcomes_consumer.get_outcome()

    expected_outcome = {
        "project_id": 42,
        "outcome": 3,
        "source": "downstream-layer",
        "reason": "project_id",
        "category": 2 if event_type == "transaction" else 1,
        "quantity": 1,
    }
    outcome.pop("timestamp")

    assert outcome == expected_outcome


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


def _get_event_payload(event_type):
    if event_type == "error":
        return {"message": "hello"}
    elif event_type == "transaction":
        now = datetime.utcnow()
        return {
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
    else:
        raise Exception("Invalid event type")


@pytest.mark.parametrize(
    "category,is_outcome_expected", [("session", False), ("transaction", True)]
)
def test_outcomes_rate_limit(
    relay_with_processing, mini_sentry, outcomes_consumer, category, is_outcome_expected
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

    # give relay some to handle the request (and send any outcomes it needs to send)
    time.sleep(1)

    if is_outcome_expected:
        outcomes_consumer.assert_rate_limited(reason_code, categories=[category])
    else:
        outcomes_consumer.assert_empty()


def test_outcome_to_client_report(relay, mini_sentry):

    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["dynamicSampling"] = {
        "rules": [
            {
                "id": 1,
                "sampleRate": 0.0,
                "type": "error",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ]
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
                "aggregator": {"flush_interval": 1,},
            }
        },
    )

    _send_event(downstream, event_type="error")

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=3.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    del outcome["timestamp"]

    expected_outcome = {
        "org_id": 1,
        "project_id": 42,
        "key_id": 123,
        "outcome": 1,
        "reason": "Sampled:1",
        "category": 1,
        "quantity": 1,
    }
    assert outcome == expected_outcome


def test_outcomes_aggregate_dynamic_sampling(relay, mini_sentry):
    """Dynamic sampling is aggregated"""
    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["dynamicSampling"] = {
        "rules": [
            {
                "id": 1,
                "sampleRate": 0.0,
                "type": "error",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ]
    }

    upstream = relay(
        mini_sentry,
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {"flush_interval": 1,},
            }
        },
    )

    _send_event(upstream, event_type="error")
    _send_event(upstream, event_type="error")

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=1.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    del outcome["timestamp"]

    expected_outcome = {
        "org_id": 1,
        "project_id": 42,
        "key_id": 123,
        "outcome": 1,
        "reason": "Sampled:1",
        "category": 1,
        "quantity": 2,
    }
    assert outcome == expected_outcome


def test_outcomes_do_not_aggregate(
    relay, relay_with_processing, mini_sentry, outcomes_consumer
):
    """Make sure that certain types are not aggregated"""
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["releases"] = {"releases": ["foo@1.2.3"]}

    relay = relay_with_processing(
        {
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {"flush_interval": 1,},
            }
        },
    )

    outcomes_consumer = outcomes_consumer(timeout=1.2)

    # Send empty body twice
    event_id1 = _send_event(relay)
    event_id2 = _send_event(relay)

    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 2, outcomes

    for outcome in outcomes:
        del outcome["timestamp"]

    # Results in two outcomes, nothing aggregated:
    expected_outcomes = {
        event_id1: {
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "event_id": event_id1,
            "remote_addr": "127.0.0.1",
            "reason": "release-version",
            "category": 1,
            "quantity": 1,
        },
        event_id2: {
            "org_id": 1,
            "project_id": 42,
            "key_id": 123,
            "outcome": 1,
            "event_id": event_id2,
            "remote_addr": "127.0.0.1",
            "reason": "release-version",
            "category": 1,
            "quantity": 1,
        },
    }
    # Convert to dict to ignore sort order:
    assert {x["event_id"]: x for x in outcomes} == expected_outcomes


def test_graceful_shutdown(relay, mini_sentry):
    # Create project config
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["dynamicSampling"] = {
        "rules": [
            {
                "id": 1,
                "sampleRate": 0.0,
                "type": "error",
                "condition": {
                    "op": "eq",
                    "name": "event.environment",
                    "value": "production",
                },
            }
        ]
    }

    relay = relay(
        mini_sentry,
        options={
            "limits": {"shutdown_timeout": 1},
            "outcomes": {
                "emit_outcomes": True,
                "batch_size": 1,
                "batch_interval": 1,
                "aggregator": {"flush_interval": 10,},
            },
        },
    )

    _send_event(relay, event_type="error")

    # Give relay some time to handle event
    time.sleep(0.1)

    # Shutdown relay
    relay.shutdown(sig=signal.SIGTERM)

    # We should have outcomes almost immediately through force flush:
    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    del outcome["timestamp"]

    expected_outcome = {
        "org_id": 1,
        "project_id": 42,
        "key_id": 123,
        "outcome": 1,
        "reason": "Sampled:1",
        "category": 1,
        "quantity": 1,
    }
    assert outcome == expected_outcome
