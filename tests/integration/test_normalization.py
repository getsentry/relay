import json
import os


def get_test_data(name):
    input_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-input.json"
    )
    with open(input_path, "r") as f:
        input = json.loads(f.read())
    input.pop("timestamp", None)

    output_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-output.json"
    )
    with open(output_path, "r") as f:
        output = json.loads(f.read())

    return input, output


def assert_ingested_event(expected, ingested):
    drop_props = ["timestamp", "received", "ingest_path", "_metrics"]
    for prop in drop_props:
        expected.pop(prop, None)
        ingested.pop(prop, None)

    assert expected == ingested, "Ingested event does not match the expected snapshot"


def test_relay_with_full_normalization(mini_sentry, relay):
    input, expected = get_test_data("extended-event")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(
        upstream=mini_sentry,
        options={"normalization": {"level": "full"}},
    )

    relay.send_event(project_id, input)
    ingested = mini_sentry.captured_events.get(timeout=10).get_event()
    assert_ingested_event(expected, ingested)


def test_processing(mini_sentry, events_consumer, relay_with_processing):
    input, expected = get_test_data("extended-event")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    events_consumer = events_consumer()
    processing = relay_with_processing()

    processing.send_event(project_id, input)
    ingested, _ = events_consumer.get_event(timeout=10)
    assert_ingested_event(expected, ingested)


def test_relay_chain(
    mini_sentry, events_consumer, relay_with_processing, relay, relay_credentials
):
    input, expected = get_test_data("extended-event")

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
        options={"normalization": {"level": "disabled"}},
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

    relay.send_event(project_id, input)
    ingested, _ = events_consumer.get_event(timeout=10)
    assert_ingested_event(expected, ingested)
