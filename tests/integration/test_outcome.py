import datetime
import json

def test_outcomes(relay_with_kafka, kafka_consumer, mini_sentry, kafka_admin):
    relay = relay_with_kafka()
    relay.wait_relay_healthcheck()
    outcomes = kafka_consumer("outcomes")
    events = kafka_consumer("events")
    # hack mini_sentry configures project 42 (remove the configuration so that we get an error for project 42)
    mini_sentry.project_configs[42] = None
    admin_client = kafka_admin()
    admin_client.delete_events_topic()
    admin_client.delete_outcomes_topic()

    message_text = "some message {}".format(datetime.datetime.now())
    event_id = "11122233344455566677788899900011"
    relay.send_event(42, {"event_id": event_id, "message": message_text, "extra": {"msg_text": message_text}})
    start = datetime.datetime.utcnow()
    # polling first message can take a few good seconds
    outcome = outcomes.poll(timeout=20)
    end = datetime.datetime.utcnow()

    assert outcome is not None
    outcome = outcome.value()
    outcome = json.loads(outcome)
    # deal with the timestamp separately ( we can't control it exactly)
    timestamp = outcome.get("timestamp")
    del outcome['timestamp']
    assert timestamp is not None
    event_emission = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    assert start <= event_emission
    assert end >= event_emission
    # reconstruct the expected message without timestamp
    expected = {"org_id": None, "project_id": 42, "key_id": None, "outcome": 3, "reason": "project_id",
                "event_id": "11122233344455566677788899900011"}
    assert outcome == expected
