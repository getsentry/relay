"""
Tests the autoscaling endpoint.
"""

import os
import signal
import tempfile
from time import sleep

import pytest


def parse_prometheus(input_string):
    result = {}
    for line in input_string.splitlines():
        parts = line.rsplit(" ", 1)
        result[parts[0]] = parts[1]
    return result


def test_basic_autoscaling_endpoint(mini_sentry, relay):
    relay = relay(mini_sentry)
    response = relay.get("/api/relay/autoscaling/")
    parsed = parse_prometheus(response.text)
    assert response.status_code == 200
    assert int(parsed["relay_up"]) == 1


def test_sqlite_spooling_metrics(mini_sentry, relay):
    # Create a temporary directory for the sqlite db
    db_file_path = os.path.join(tempfile.mkdtemp(), "database.db")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        mini_sentry,
        {
            "spool": {"envelopes": {"path": db_file_path, "batch_size_bytes": 1}},
        },
    )

    # Send SIGUSR1 to disable unspooling
    relay.send_signal(signal.SIGUSR1)
    sleep(1)  # Give time for the signal to be processed

    # Send more events while unspooling is disabled
    for i in range(200):
        relay.send_event(project_id)

    response = relay.get("/api/relay/autoscaling/")
    assert response.status_code == 200
    body = parse_prometheus(response.text)
    # In some test runs, the last event is not counted making this test flaky.
    # Since this metric does not need to be very precise, we can consider it successful
    # if one item is not counted.
    assert 199 <= int(body["relay_spool_item_count"]) <= 200
    assert int(body["relay_up"]) == 1
    assert int(body["relay_spool_total_size"]) > 30000


def test_memory_spooling_metrics(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(mini_sentry)

    relay.send_signal(signal.SIGUSR1)
    sleep(0.5)

    for i in range(200):
        relay.send_event(project_id)

    response = relay.get("/api/relay/autoscaling/")
    assert response.status_code == 200
    body = parse_prometheus(response.text)
    assert int(body["relay_spool_item_count"]) == 200
    assert int(body["relay_up"]) == 1
    assert int(body["relay_spool_total_size"]) == 0


@pytest.mark.parametrize(
    "metric_name",
    (
        'relay_service_utilization{relay_service="AggregatorService", instance_id="0"}',
        "relay_worker_pool_utilization",
        "relay_runtime_utilization",
    ),
)
def test_service_utilization_metrics(mini_sentry, relay, metric_name):
    relay = relay(mini_sentry)

    response = relay.get("/api/relay/autoscaling/")
    parsed = parse_prometheus(response.text)
    assert response.status_code == 200

    assert 0 <= int(parsed[metric_name]) <= 100
