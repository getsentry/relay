import socket
import time
import queue
import signal

import pytest

from flask import jsonify


def test_graceful_shutdown(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1)  # Causes the process to wait for one second before shutting down
        return get_project_config_original()

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42)

    relay.shutdown(sig=signal.SIGTERM)
    assert mini_sentry.captured_events.get(timeout=0)["message"] == "Hello, World!"


def test_forced_shutdown(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1)  # Causes the process to wait for one second before shutting down
        return get_project_config_original()

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42)

    relay.shutdown(sig=signal.SIGINT)
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


@pytest.mark.parametrize("failure_type", ["timeout", "socketerror"])
def test_query_retry(failure_type, mini_sentry, relay):
    retry_count = 0

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
            return jsonify(configs={"42": relay.basic_project_config()})

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    relay.send_event(42)

    # relay's http timeout is 2 seconds, and retry interval 1s * 1.5^n
    event = mini_sentry.captured_events.get(timeout=4)
    assert event["message"] == "Hello, World!"
    assert retry_count == 2

    if mini_sentry.test_failures:
        (_, error), = mini_sentry.test_failures
        assert isinstance(error, socket.error)
        mini_sentry.test_failures.clear()
