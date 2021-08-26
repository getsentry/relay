def test_ingest_path(mini_sentry, relay, relay_with_processing, latest_relay_version):
    internal_relay = relay_with_processing()
    internal_keys = list(internal_relay.iter_public_keys())
    relay = relay(relay(relay(mini_sentry)))
    project_id = 42
    project_config = mini_sentry.add_basic_project_config(project_id)
    external_keys = [
        key for key in relay.iter_public_keys() if key not in internal_keys
    ]
    project_config["config"]["trustedRelays"] = list(external_keys)

    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["ingest_path"] == [
        {"version": latest_relay_version, "public_key": key} for key in external_keys
    ]
