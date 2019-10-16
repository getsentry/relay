import json
import queue
import signal
import socket
import time

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
    assert mini_sentry.captured_events.get(timeout=0)["logentry"] == {
        "formatted": "Hello, World!"
    }


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
    assert event["logentry"] == {"formatted": "Hello, World!"}
    assert retry_count == 2

    if mini_sentry.test_failures:
        for (_, error) in mini_sentry.test_failures:
            assert isinstance(error, (socket.error, AssertionError))
        mini_sentry.test_failures.clear()


def test_local_project_config(mini_sentry, relay):
    config = mini_sentry.basic_project_config()
    relay = relay(mini_sentry, {"cache": {"file_interval": 1}})
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
    relay.send_event(42)
    event = mini_sentry.captured_events.get(timeout=1)
    assert event["logentry"] == {"formatted": "Hello, World!"}

    relay.config_dir.join("projects").join("42.json").write(
        json.dumps({"disabled": True})
    )
    time.sleep(5)

    relay.send_event(42)
    pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))


@pytest.mark.parametrize("trailing_slash", [True, False])
@pytest.mark.parametrize("input", [
    '{"message": "im in ur query params"}',
    "eF6rVspNLS5OTE9VslJQysxVyMxTKC1SKCxNLapUKEgsSswtVqoFAOKyDI4="
])
def test_store_pixel_gif(mini_sentry, relay, input, trailing_slash):
    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    relay = relay(mini_sentry)

    relay.wait_relay_healthcheck()

    response = relay.post(
        "/api/42/store%s?sentry_data=%s"
        "&sentry_key=%s" % ("/" if trailing_slash else "", input, relay.dsn_public_key,)
    )
    response.raise_for_status()
    assert response.headers['content-type'] == 'image/gif'

    event = mini_sentry.captured_events.get(timeout=1)

    assert event['logentry']['formatted'] == 'im in ur query params'


@pytest.mark.parametrize("trailing_slash", [True, False])
def test_store_post_trailing_slash(mini_sentry, relay, trailing_slash):
    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    relay = relay(mini_sentry)

    relay.wait_relay_healthcheck()

    response = relay.post(
        "/api/42/store%s"
        "?sentry_key=%s" % ("/" if trailing_slash else "", relay.dsn_public_key,),
        json={"message": "hi"}
    )
    response.raise_for_status()

    event = mini_sentry.captured_events.get(timeout=1)

    assert event['logentry']['formatted'] == 'hi'
