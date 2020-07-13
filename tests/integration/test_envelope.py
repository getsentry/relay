from sentry_sdk.envelope import Envelope


def test_envelope(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.project_configs[42] = relay.basic_project_config()

    envelope = Envelope()
    envelope.add_event({"message": "Hello, World!"})
    relay.send_envelope(42, envelope)

    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}
