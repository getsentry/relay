"""
Tests the autoscaling endpoint.
"""

import os
import signal
import tempfile
from time import sleep


def parse_prometheus(input_string):
    result = {}
    for line in input_string.splitlines():
        parts = line.split(" ")
        result[parts[0]] = parts[1]
    return result


def test_basic_autoscaling_endpoint(mini_sentry, relay):
    relay = relay(mini_sentry)
    response = relay.get("/api/relay/autoscaling/")
    parsed = parse_prometheus(response.text)
    assert response.status_code == 200
    assert int(parsed['up{service="autoscaling"}']) == 1


def test_sqlite_spooling_metrics(mini_sentry, relay):
    # Create a temporary directory for the sqlite db
    db_file_path = os.path.join(tempfile.mkdtemp(), "database.db")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        mini_sentry,
        {
            "spool": {"batch_size_bytes": 0, "envelopes": {"path": db_file_path}},
        },
    )

    # Send SIGUSR1 to disable unspooling
    relay.send_signal(signal.SIGUSR1)
    sleep(0.5)  # Give time for the signal to be processed

    # Send more events while unspooling is disabled
    for i in range(200):
        relay.send_event(project_id)

    response = relay.get("/api/relay/autoscaling/")
    assert response.status_code == 200
    body = parse_prometheus(response.text)
    print(body)
    assert int(body['item_count{service="autoscaling"}']) == 200
    assert int(body['up{service="autoscaling"}']) == 1
    assert int(body['total_size{service="autoscaling"}']) > 30000


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
    assert int(body['item_count{service="autoscaling"}']) == 200
    assert int(body['up{service="autoscaling"}']) == 1
    assert int(body['total_size{service="autoscaling"}']) == 0
