def test_advertised_upstream_envelope(mini_sentry, mini_proxy, relay):
    project_id = 42

    proxy = mini_proxy(mini_sentry)

    project_config = mini_sentry.add_basic_project_config(project_id)
    project_config["upstream"] = proxy.url

    relay = relay(mini_sentry)

    relay.send_event(42)

    event = mini_sentry.get_captured_envelope().get_event()
    assert event is not None

    assert proxy.get_captured_request().path == "/api/42/envelope/"
    assert proxy.captured_requests.empty()
