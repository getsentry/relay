import json
import queue
import redis
import socket
import time
import threading

import pytest

from flask import jsonify

from requests.exceptions import HTTPError


def test_local_project_config(mini_sentry, relay):
    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    # TODO RaduW see how to fix this hack
    # hack we only need a config (we don't need it added to sentry)
    del mini_sentry.project_configs[42]
    relay_config = {
        "cache": {"file_interval": 1, "project_expiry": 0, "project_grace_period": 0}
    }
    relay = relay(mini_sentry, relay_config, wait_healthcheck=False)
    relay.config_dir.mkdir("projects").join("42.json").write(
        json.dumps(
            {
                # remove defaults to assert they work
                "publicKeys": config["publicKeys"],
                "config": {
                    "allowedDomains": ["*"],
                    "trustedRelays": [],
                    "piiConfig": {},
                },
            }
        )
    )

    relay.wait_relay_healthcheck()
    relay.send_event(project_id)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}

    relay.config_dir.join("projects").join("42.json").write(
        json.dumps({"disabled": True})
    )
    time.sleep(2)

    try:
        # This may or may not respond with 403, depending on how quickly the future to fetch project
        # states executes.
        relay.send_event(project_id)
    except HTTPError:
        pass

    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


@pytest.mark.parametrize("grace_period", [0, 5])
def test_project_grace_period(mini_sentry, relay, grace_period):
    config = mini_sentry.add_basic_project_config(42)
    config["disabled"] = True
    fetched_project_config = threading.Event()

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        fetched_project_config.set()
        return get_project_config_original()

    relay = relay(
        mini_sentry,
        {
            "cache": {
                "miss_expiry": 1,
                "project_expiry": 1,
                "project_grace_period": grace_period,
            }
        },
    )

    assert not fetched_project_config.is_set()

    # The first event sent should always return a 200 because we have no
    # project config
    relay.send_event(42)

    assert fetched_project_config.wait(timeout=1)
    time.sleep(2)
    fetched_project_config.clear()

    if grace_period == 0:
        # With a grace period of 0, sending a second event should return a 200
        # because the project config has expired again and needs to be refetched
        relay.send_event(42)
    else:
        assert not fetched_project_config.is_set()

        # With a non-zero grace period the request should fail during the grace period
        with pytest.raises(HTTPError) as excinfo:
            relay.send_event(42)

        assert excinfo.value.response.status_code == 403
        assert fetched_project_config.wait(timeout=1)

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize("failure_type", ["timeout", "socketerror"])
def test_query_retry(failure_type, mini_sentry, relay):
    retry_count = 0

    mini_sentry.add_basic_project_config(42)
    original_endpoint = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        nonlocal retry_count
        retry_count += 1
        print("RETRY", retry_count)

        if retry_count < 2:
            if failure_type == "timeout":
                time.sleep(50)  # ensure timeout
            elif failure_type == "socketerror":
                raise socket.error()
            else:
                assert False

            return "ok"  # never read by client
        else:
            return original_endpoint()

    relay = relay(mini_sentry)
    relay.send_event(42)

    event = mini_sentry.captured_events.get(timeout=8).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
    assert retry_count == 2

    if mini_sentry.test_failures:
        for (_, error) in mini_sentry.test_failures:
            assert isinstance(error, (socket.error, AssertionError))
        mini_sentry.test_failures.clear()


def test_query_retry_maxed_out(
    mini_sentry, relay_with_processing, outcomes_consumer, events_consumer
):
    """
    Assert that a query is not retried an infinite amount of times.

    This is not specific to processing or store, but here we have the outcomes
    consumer which we can use to assert that an event has been dropped.
    """

    request_count = 0

    outcomes_consumer = outcomes_consumer()
    events_consumer = events_consumer()

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        nonlocal request_count
        request_count += 1
        time.sleep(1)
        print("RETRY", request_count)
        return "no", 500

    relay = relay_with_processing({"limits": {"query_timeout": 10}})
    relay.send_event(42)
    time.sleep(10)  # Wait for 4 retries with backoff

    outcomes_consumer.assert_dropped_internal()
    assert request_count == 4

    for (_, error) in mini_sentry.test_failures[:-1]:
        assert isinstance(error, AssertionError)
        assert "error fetching project states" in str(error)

    _, last_error = mini_sentry.test_failures[-1]
    assert "failed to resolve project information" in str(last_error)

    mini_sentry.test_failures.clear()


@pytest.mark.parametrize("disabled", (True, False))
def test_processing_redis_query(
    mini_sentry, relay_with_processing, events_consumer, outcomes_consumer, disabled
):
    outcomes_consumer = outcomes_consumer()
    events_consumer = events_consumer()

    relay = relay_with_processing({"limits": {"query_timeout": 10}})
    project_id = 42
    cfg = mini_sentry.add_full_project_config(project_id)
    cfg["disabled"] = disabled

    key = mini_sentry.get_dsn_public_key(project_id)
    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    projectconfig_cache_prefix = relay.options["processing"][
        "projectconfig_cache_prefix"
    ]
    redis_client.setex(f"{projectconfig_cache_prefix}:{key}", 3600, json.dumps(cfg))

    relay.send_event(project_id)

    if disabled:
        outcomes_consumer.assert_dropped_unknown_project()
    else:
        event, v = events_consumer.get_event()
        assert event["logentry"] == {"formatted": "Hello, World!"}
