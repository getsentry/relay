import json
import queue
import signal
import socket
import time
import threading

import pytest

from flask import jsonify

from requests.exceptions import HTTPError


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
    event = mini_sentry.captured_events.get(timeout=0).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


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

    ((route, error),) = mini_sentry.test_failures
    assert route == "/api/666/store/"
    assert "Dropped unfinished future" in str(error)
    mini_sentry.test_failures.clear()


@pytest.mark.parametrize("trailing_slash", [True, False])
@pytest.mark.parametrize(
    "input",
    [
        '{"message": "im in ur query params"}',
        "eF6rVspNLS5OTE9VslJQysxVyMxTKC1SKCxNLapUKEgsSswtVqoFAOKyDI4=",
    ],
)
def test_store_pixel_gif(mini_sentry, relay, input, trailing_slash):
    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    relay = relay(mini_sentry)

    relay.wait_relay_healthcheck()

    response = relay.get(
        "/api/42/store%s?sentry_data=%s"
        "&sentry_key=%s" % ("/" if trailing_slash else "", input, relay.dsn_public_key,)
    )
    response.raise_for_status()
    assert response.headers["content-type"] == "image/gif"

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"]["formatted"] == "im in ur query params"


@pytest.mark.parametrize("trailing_slashes", list(range(4)))
def test_store_post_trailing_slash(mini_sentry, relay, trailing_slashes):
    mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    relay = relay(mini_sentry)

    relay.wait_relay_healthcheck()

    response = relay.post(
        "/api/42/store%s"
        "?sentry_key=%s" % ("/" * trailing_slashes, relay.dsn_public_key,),
        json={"message": "hi"},
    )
    response.raise_for_status()

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"]["formatted"] == "hi"


def id_fun1(param):
    allowed, should_be_allowed = param
    should_it = "" if should_be_allowed else "not"
    return f"{str(allowed)} should {should_it} be allowed"


@pytest.mark.parametrize(
    "allowed_origins",
    [
        (["*"], True),
        (["http://valid.com"], True),
        (["http://*"], True),
        (["valid.com"], True),
        ([], False),
        (["invalid.com"], False),
    ],
    ids=id_fun1,
)
def test_store_allowed_origins_passes(mini_sentry, relay, allowed_origins):
    allowed_domains, should_be_allowed = allowed_origins
    config = mini_sentry.project_configs[42] = mini_sentry.basic_project_config()
    config["config"]["allowedDomains"] = allowed_domains

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    response = relay.post(
        "/api/42/store/?sentry_key=%s" % (relay.dsn_public_key,),
        headers={"Origin": "http://valid.com"},
        json={"message": "hi"},
    )

    if should_be_allowed:
        mini_sentry.captured_events.get(timeout=1).get_event()
    assert mini_sentry.captured_events.empty()
