import random
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from queue import Empty

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
    start = datetime.utcnow()

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
    assert outcome["event_id"] == event_id
    assert outcome.get("org_id") is None
    assert outcome.get("key_id") is None
    assert outcome["outcome"] == 3
    assert outcome["reason"] == "project_id"
    assert outcome["remote_addr"] == "127.0.0.1"

    # deal with the timestamp separately (we can't control it exactly)
    timestamp = outcome.get("timestamp")
    event_emission = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    end = datetime.utcnow()
    assert start <= event_emission <= end


def _send_event(relay, project_id=42):
    """
    Send an event to the given project.

    If the project doesn't exist, relay should generate INVALID outcome with reason "project_id".
    """
    event_id = uuid.uuid1().hex
    message_text = "some message {}".format(datetime.now())
    event_body = {
        "event_id": event_id,
        "message": message_text,
        "extra": {"msg_text": message_text},
    }

    try:
        relay.send_event(project_id=project_id, payload=event_body)
    except Exception:
        pass
    return event_id


def test_outcomes_non_processing(relay, mini_sentry):
    """
    Test basic outcome functionality.

    Send one event that generates an outcome and verify that we get an outcomes batch
    with all necessary information set.
    """
    config = {"outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1}}

    relay = relay(mini_sentry, config)

    event_id = _send_event(relay)

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
        "event_id": event_id,
        "remote_addr": "127.0.0.1",
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
        assert outcomes[0].get("event_id") in event_ids  # a known event id


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

    received_event_ids = [outcome.get("event_id") for outcome in outcomes]

    for event_id in received_event_ids:
        assert event_id in event_ids  # the outcome is one of those we sent

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
def test_outcome_forwarding(
    mini_sentry,
    relay,
    relay_with_processing,
    outcomes_consumer,
    num_intermediate_relays,
):
    """
    Tests that Relay forwards outcomes from a chain of relays

    Have a chain of many relays that eventually connect to Sentry
    and verify that the outcomes sent by  the first (downstream relay)
    are properly forwarded up to sentry.
    """

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

    event_id = _send_event(downstream_relay)

    outcomes_consumer = outcomes_consumer()
    outcome = outcomes_consumer.get_outcome()

    expected_outcome = {
        "project_id": 42,
        "outcome": 3,
        "source": "downstream-layer",
        "reason": "project_id",
        "event_id": event_id,
        "remote_addr": "127.0.0.1",
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

    outcomes_consumer_instance = outcomes_consumer()

    # Send an event, it should be dropped in the upstream (processing) relay
    result = downstream_relay.send_event(project_id, _get_message(category))
    event_id = result["id"]

    outcome = outcomes_consumer_instance.get_outcome()
    outcome.pop("timestamp")
    expected_outcome = {
        "reason": "rate_limited",
        "org_id": 1,
        "key_id": 123,
        "outcome": 2,
        "project_id": 42,
        "remote_addr": "127.0.0.1",
        "event_id": event_id,
        "source": "processing-layer",
    }
    assert outcome == expected_outcome

    # Send another event, now the downstream should drop it because it'll cache the 429
    # response from the previous event, but the outcome should be emitted
    with pytest.raises(requests.exceptions.HTTPError, match="429 Client Error"):
        downstream_relay.send_event(project_id, _get_message(category))

    expected_outcome_from_downstream = deepcopy(expected_outcome)
    expected_outcome_from_downstream["source"] = "downstream-layer"
    expected_outcome_from_downstream.pop("event_id")

    outcome = outcomes_consumer_instance.get_outcome()
    outcome.pop("timestamp")
    outcome.pop("event_id")

    assert outcome == expected_outcome_from_downstream

    _assert_outcomes_topic_empty(outcomes_consumer_instance)


def _assert_outcomes_topic_empty(outcomes_consumer_instance):
    """
    To prove there's nothing in the topic, we send a randomized message and then
    immediately consume it.
    """
    event_id = "".join(random.choice("0123456789abcdef") for _ in range(32))
    print(event_id)
    dummy_outcome = {
        "org_id": 0,
        "project_id": 0,
        "reason": "fake",
        "event_id": event_id,
    }
    outcomes_consumer_instance.produce_test_message(dummy_outcome)
    outcome = outcomes_consumer_instance.get_outcome()
    assert outcome == dummy_outcome


def _get_message(message_type):
    if message_type == "error":
        return {"message": "hello"}
    elif message_type == "transaction":
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
    elif message_type == "session":
        timestamp = datetime.now(tz=timezone.utc)
        started = timestamp - timedelta(hours=1)
        return {
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
    else:
        raise Exception("Invalid message_type")


@pytest.mark.parametrize("category_type", ["session", "transaction"])
def test_no_outcomes_rate_limit(
    relay_with_processing, mini_sentry, outcomes_consumer, category_type
):
    """
    Tests that outcomes are not emitted for certain type of messages

    Pass a transaction that is rate limited and check that an outcome is not emitted (although
    the transaction does NOT go through).

    NOTE: This test should start failing once transactions outcomes become supported.
    Once that happens change test to verify that a transaction outcome IS sent
    """

    config = {"outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1}}
    relay = relay_with_processing(config)
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": "transaction category",
            "categories": [category_type],
            "limit": 0,
            "window": 1600,
            "reasonCode": "transactions are banned",
        }
    ]
    outcomes_consumer = outcomes_consumer()

    message = _get_message(category_type)
    relay.send_event(project_id, message)

    # give relay some to handle the message (and send any outcomes it needs to send)
    time.sleep(1)

    # we should not have anything on the outcome topic,
    _assert_outcomes_topic_empty(outcomes_consumer)
