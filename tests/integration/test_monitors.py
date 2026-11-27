import base64
import json

import pytest
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .consts import Outcome


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

    envelope = mini_sentry.get_captured_envelope()
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
    assert message["message_type"] == "check_in"
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
    assert message["message_type"] == "check_in"
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
    assert message["message_type"] == "check_in"
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
    assert message["message_type"] == "check_in"
    assert message["start_time"] is not None
    assert message["project_id"] == project_id
    assert message["sdk"] == "relay-http"
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

    envelope = mini_sentry.get_captured_envelope()
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


def test_monitor_forwards_dimensions_header(mini_sentry, relay):
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)

    check_in = generate_check_in("My Monitor")
    check_in["environment"] = "production"
    relay.send_check_in(42, check_in)

    envelope = mini_sentry.get_captured_envelope()
    assert len(envelope.items) == 1
    item = envelope.items[0]
    assert item.headers["type"] == "check_in"
    # The forwarding Relay attaches the normalized dimensions, so that the next Relay can
    # enforce dimensioned rate limits without parsing the payload.
    assert item.headers["dimensions"] == {
        "checkInEnvironment": "production",
        "checkInSlug": "my-monitor",
    }


@pytest.mark.parametrize("with_header", [True, False])
def test_monitor_fast_path_uses_dimensions_header(mini_sentry, relay, with_header):
    """
    The fast path must enforce dimensioned quotas from the item's `dimensions` header alone.

    The payload is invalid JSON, so the two paths are distinguishable by outcome: the fast path
    never parses the payload and rate limits the check-in, while the processor rejects it as
    invalid. Without the header, the fast path cannot match the grouped quota and the processor
    has to handle it.
    """
    relay = relay(mini_sentry, options={"outcomes": {"emit_outcomes": True}})
    project_config = mini_sentry.add_basic_project_config(42)
    project_config["config"]["quotas"] = [
        {
            "categories": ["monitor"],
            "limit": 0,
            "reasonCode": "slug_quota",
            "groupBy": {"maxCardinality": 100, "dimensions": ["checkInSlug"]},
        }
    ]

    headers = {"type": "check_in"}
    if with_header:
        headers["dimensions"] = {"checkInEnvironment": "", "checkInSlug": "limited"}

    envelope = Envelope()
    envelope.add_item(Item(payload=PayloadRef(bytes=b"not json"), headers=headers))
    relay.send_envelope(42, envelope)

    (outcome,) = mini_sentry.get_outcomes(n=1)
    assert outcome["category"] == DataCategory.MONITOR
    assert outcome["quantity"] == 1
    if with_header:
        assert outcome["outcome"] == Outcome.RATE_LIMITED
        assert outcome["reason"] == "slug_quota"
    else:
        assert outcome["outcome"] == Outcome.INVALID
        assert outcome["reason"] == "invalid_json"

    assert mini_sentry.captured_envelopes.empty()
