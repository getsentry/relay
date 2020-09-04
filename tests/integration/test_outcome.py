from copy import deepcopy
from datetime import datetime
import uuid
from queue import Empty

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
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

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


def _send_event(relay):
    event_id = uuid.uuid1().hex
    message_text = "some message {}".format(datetime.now())
    event_body = {
        "event_id": event_id,
        "message": message_text,
        "extra": {"msg_text": message_text},
    }

    try:
        relay.send_event(42, event_body)
    except:
        pass
    return event_id


def test_outcomes_non_processing(relay, mini_sentry):
    """
    Test basic outcome functionality.

    Send one event that generates an outcome and verify that we get an outcomes batch
    with all necessary information set.
    """
    config = {
        "outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1,}
    }

    relay = relay(mini_sentry, config)
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_id = _send_event(relay)

    outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
    assert mini_sentry.captured_outcomes.qsize() == 0  # we had only one batch

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 1

    outcome = outcomes[0]

    timestamp = outcome.get("timestamp")
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
        "outcomes": {"emit_outcomes": False, "batch_size": 1, "batch_interval": 1,}
    }

    relay = relay(mini_sentry, config)
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

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
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

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
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

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


def _send_event(relay):
    event_id = uuid.uuid1().hex
    message_text = "some message {}".format(datetime.now())
    event_body = {
        "event_id": event_id,
        "message": message_text,
        "extra": {"msg_text": message_text},
    }

    try:
        relay.send_event(42, event_body)
    except:
        pass
    return event_id


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
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

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

    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_id = _send_event(downstream_relay)

    outcomes_consumer = outcomes_consumer()
    outcome = outcomes_consumer.get_outcome()
    assert outcome.get("source") == "downstream-layer"
    assert outcome.get("event_id") == event_id
