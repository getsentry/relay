from datetime import datetime
import json
import uuid
from queue import Empty

import time

HOUR_MILLISEC = 1000 * 3600


def test_outcomes_processing(relay_with_processing, kafka_consumer, mini_sentry):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    outcomes = kafka_consumer("outcomes")
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    message_text = "some message {}".format(datetime.now())
    event_id = "11122233344455566677788899900011"
    relay.send_event(
        42,
        {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
        },
    )
    start = datetime.utcnow()
    # polling first message can take a few good seconds
    outcome = outcomes.poll(timeout=20)
    end = datetime.utcnow()

    assert outcome is not None
    outcome = outcome.value()
    outcome = json.loads(outcome)
    # set defaults to allow for results that elide empty fields
    default = {
        "org_id": None,
        "key_id": None,
        "reason": None,
        "event_id": None,
        "remote_addr": None,
    }
    outcome = {**default, **outcome}
    # deal with the timestamp separately ( we can't control it exactly)
    timestamp = outcome.get("timestamp")
    del outcome["timestamp"]
    assert timestamp is not None
    event_emission = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    assert start <= event_emission <= end
    # reconstruct the expected message without timestamp
    expected = {
        "project_id": 42,
        "event_id": event_id,
        "org_id": None,
        "key_id": None,
        "outcome": 3,
        "reason": "project_id",
        "remote_addr": "127.0.0.1",
    }
    assert outcome == expected


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


def test_outcomes_non_processing(relay, relay_with_processing, mini_sentry):
    """
    Test basic outcome functionality.
    Send one event that generates an outcome and verify that we get an outcomes batch
    with all necessary information set.
    """
    config = {
        "outcomes": {"emit_outcomes": True, "batch_size": 1, "batch_interval": 1,}
    }

    relay = relay(mini_sentry, config)
    relay.wait_relay_healthcheck()
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
    Set batching to a very short interval and verify that we don't receive any outcome
    when we disable outcomes.
    """
    config = {
        "outcomes": {"emit_outcomes": False, "batch_size": 1, "batch_interval": 1,}
    }

    relay = relay(mini_sentry, config)
    relay.wait_relay_healthcheck()
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    try:
        outcomes_batch = mini_sentry.captured_outcomes.get(timeout=0.2)
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
    relay.wait_relay_healthcheck()
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_ids = set()
    # send one less events than the batch size (and check we don't send anything)
    for i in range(events_to_send):
        event_id = _send_event(relay)
        event_ids.add(event_id)
        time.sleep(0.005)  # sleep more than the batch time

    # we should get one batch per event sent
    batches = []
    for batch_id in range(events_to_send):
        batch = mini_sentry.captured_outcomes.get(timeout=0.2)
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
    relay.wait_relay_healthcheck()
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_ids = set()
    # send one less events than the batch size (and check we don't send anything)
    for i in range(batch_size - 1):
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
