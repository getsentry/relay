def generate_checkin():
    return {
        "checkin_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_id": "4dc8556e039245c7bd569f8cf513ea42",
        "status": "in_progress",
        "duration": 21.0,
    }


def test_crons_with_processing(mini_sentry, relay_with_processing, crons_consumer):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)
    crons_consumer = crons_consumer()

    checkin = generate_checkin()
    relay.send_cron_checkin(42, checkin)

    checkin, message = crons_consumer.get_checkin()
    assert message["start_time"] is not None
    assert message["project_id"] == 42
    assert checkin == {
        "checkin_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_id": "4dc8556e039245c7bd569f8cf513ea42",
        "status": "in_progress",
        "duration": 21.0,
    }
