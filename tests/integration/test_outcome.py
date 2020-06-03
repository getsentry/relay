import datetime
import json
import uuid

import time


def test_outcomes_processing(relay_with_processing, kafka_consumer, mini_sentry):
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    outcomes = kafka_consumer("outcomes")
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    message_text = "some message {}".format(datetime.datetime.now())
    event_id = "11122233344455566677788899900011"
    relay.send_event(
        42,
        {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
        },
    )
    start = datetime.datetime.utcnow()
    # polling first message can take a few good seconds
    outcome = outcomes.poll(timeout=20)
    end = datetime.datetime.utcnow()

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
    event_emission = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
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
    message_text = "some message {}".format(datetime.datetime.now())
    relay.send_event(
        42,
        {
            "event_id": event_id,
            "message": message_text,
            "extra": {"msg_text": message_text},
        },
    )
    return event_id


def test_outcomes_non_processing(relay, relay_with_processing, mini_sentry):
    config = {
        "relay": {
            "emit_outcomes": True,
            "max_outcome_batch_size": 1,
            "max_outcome_interval_millsec": 1,
        }
    }

    relay = relay(mini_sentry, config)
    relay.wait_relay_healthcheck()
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_ids = []
    for i in range(1):
        event_id = _send_event(relay)
        event_ids.append(event_id)

    events = mini_sentry.captured_events

    assert events.empty()  # no events received since all have been for an invalid project id
    outcomes = mini_sentry.captured_outcomes
    assert outcomes.qsize() == 1


def test_outcomes_non_processing_batching(relay, mini_sentry):
    config = {
        "relay": {
            "emit_outcomes": True,
            "max_outcome_batch_size": 3,
            "max_outcome_interval_millsec": 3600 * 1000,  # an hour
        }
    }

    relay = relay(config)
    relay.wait_relay_healthcheck()
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None

    event_ids = []
    for i in range(2):
        event_id = _send_event(relay)
        event_ids.append(event_id)

    # nothing should be sent at this time
    outcomes = mini_sentry.captured_outcomes
    assert outcomes.qsize() == 0

    event_id = _send_event(relay)
    event_ids.append(event_id)

    # now we should have received a batch with all the events
    outcomes = mini_sentry.captured_outcomes
    assert outcomes.qsize() == 1

    outcomes_batch = outcomes.get(block=False)

    outcomes = outcomes_batch.get("outcomes")
    assert len(outcomes) == 3
    actual_event_ids = [ outcome.get("event_id") for outcome in outcomes]
    assert event_ids == actual_event_ids
