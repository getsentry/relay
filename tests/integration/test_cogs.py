from sentry_sdk.envelope import Envelope

TEST_CONFIG = {
    "cogs": {
        "enabled": True,
        "granularity_secs": 0,
    }
}


def test_cogs_simple(mini_sentry, relay_with_processing, cogs_consumer):
    relay = relay_with_processing(TEST_CONFIG)
    cogs_consumer = cogs_consumer()

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    envelope = Envelope()
    envelope.add_event({"message": "Hello, World!"})
    relay.send_envelope(project_id, envelope)

    m = cogs_consumer.get_measurement()
    assert m["shared_resource_id"] == "relay_service"
    assert m["app_feature"] == "errors"
    assert m["usage_unit"] == "milliseconds"
    assert (
        m["amount"] < 1000
    )  # If processing this error takes a second, we have a problem ...
