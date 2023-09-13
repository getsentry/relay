import base64
import json


def generate_check_in(slug):
    return {
        "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_slug": slug,
        "status": "in_progress",
        "duration": 21.0,
    }


def test_monitor_ingest(mini_sentry, relay):
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)

    check_in = generate_check_in("my-monitor")
    relay.send_check_in(42, check_in)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert len(envelope.items) == 1
    item = envelope.items[0]
    assert item.headers["type"] == "check_in"

    check_in = json.loads(item.get_bytes().decode())
    assert check_in == {
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

    check_in = generate_check_in("my-monitor")
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


def test_monitor_endpoint_get_with_processing(
    mini_sentry, relay_with_processing, monitors_consumer
):
    project_id = 42
    options = {"processing": {}}
    relay = relay_with_processing(options)
    monitors_consumer = monitors_consumer()

    mini_sentry.add_full_project_config(project_id)

    monitor_slug = "my-monitor"
    public_key = relay.get_dsn_public_key(project_id)
    response = relay.get(
        f"/api/{project_id}/cron/{monitor_slug}/{public_key}?status=ok"
    )

    assert response.status_code == 202

    check_in, message = monitors_consumer.get_check_in()
    assert message["start_time"] is not None
    assert message["project_id"] == project_id
    assert check_in == {
        "check_in_id": "00000000000000000000000000000000",
        "monitor_slug": "my-monitor",
        "status": "ok",
    }


def test_monitor_endpoint_post_auth_basic_with_processing(
    mini_sentry, relay_with_processing, monitors_consumer
):
    project_id = 42
    options = {"processing": {}}
    relay = relay_with_processing(options)
    monitors_consumer = monitors_consumer()

    mini_sentry.add_full_project_config(project_id)

    monitor_slug = "my-monitor"
    public_key = relay.get_dsn_public_key(project_id)
    basic_auth = base64.b64encode((public_key + ":").encode("utf-8")).decode("utf-8")
    response = relay.post(
        f"/api/{project_id}/cron/{monitor_slug}?status=ok",
        headers={"Authorization": "Basic " + basic_auth},
    )

    assert response.status_code == 202

    check_in, message = monitors_consumer.get_check_in()
    assert message["start_time"] is not None
    assert message["project_id"] == project_id
    assert check_in == {
        "check_in_id": "00000000000000000000000000000000",
        "monitor_slug": "my-monitor",
        "status": "ok",
    }


def test_monitor_endpoint_embedded_auth_with_processing(
    mini_sentry, relay_with_processing, monitors_consumer
):
    project_id = 42
    options = {"processing": {}}
    relay = relay_with_processing(options)
    monitors_consumer = monitors_consumer()

    mini_sentry.add_full_project_config(project_id)

    monitor_slug = "my-monitor"
    public_key = relay.get_dsn_public_key(project_id)
    response = relay.post(
        f"/api/{project_id}/cron/{monitor_slug}/{public_key}?status=ok",
    )

    assert response.status_code == 202

    check_in, message = monitors_consumer.get_check_in()
    assert message["start_time"] is not None
    assert message["project_id"] == project_id
    assert check_in == {
        "check_in_id": "00000000000000000000000000000000",
        "monitor_slug": "my-monitor",
        "status": "ok",
    }


def test_monitor_post_json_body(mini_sentry, relay):
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)

    check_in = {
        "status": "in_progress",
        "duration": 21,
        "monitor_config": {
            "schedule": {"type": "crontab", "value": "0 * * * *"},
            "checkin_margin": 5,
            "max_runtime": 10,
            "timezone": "America/Los_Angles",
        },
    }

    public_key = relay.get_dsn_public_key(42)
    response = relay.post(f"/api/42/cron/my-monitor/{public_key}", json=check_in)
    assert response.status_code == 202, response.text

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert len(envelope.items) == 1
    item = envelope.items[0]
    assert item.headers["type"] == "check_in"

    check_in = json.loads(item.get_bytes().decode())
    assert check_in == {
        "check_in_id": "00000000000000000000000000000000",
        "monitor_slug": "my-monitor",
        "status": "in_progress",
        "duration": 21.0,
        "monitor_config": {
            "schedule": {"type": "crontab", "value": "0 * * * *"},
            "checkin_margin": 5,
            "max_runtime": 10,
            "timezone": "America/Los_Angles",
        },
    }
