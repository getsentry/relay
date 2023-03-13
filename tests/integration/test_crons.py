def generate_check_in():
    return {
        "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_slug": "my-monitor",
        "status": "in_progress",
        "duration": 21.0,
    }


def test_monitors_with_processing(
    mini_sentry, relay_with_processing, monitors_consumer
):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)
    monitors_consumer = monitors_consumer()

    check_in = generate_check_in()
    relay.send_check_in(42, check_in)

    check_in, message = monitors_consumer.get_check_in()
    assert message["start_time"] is not None
    assert message["project_id"] == 42
    assert check_in == {
        "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_slug": "my-monitor",
        "status": "in_progress",
        "duration": 21.0,
    }
