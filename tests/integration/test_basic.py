import queue
import os
import gzip
import pytest
import signal
import zlib
import subprocess

def test_config_file(mini_sentry, relay):
    config_file = "config.yaml"
    relay = relay(mini_sentry, config_file=config_file)

    assert os.path.exists(relay.config_dir.join(config_file))


def test_graceful_shutdown(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1)  # Causes the process to wait for one second before shutting down
        return get_project_config_original()

    relay = relay(mini_sentry, {"limits": {"shutdown_timeout": 2}})
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay.send_event(project_id)

    relay.shutdown(sig=signal.SIGTERM)
    event = mini_sentry.captured_events.get(timeout=0).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


@pytest.mark.skip("Flaky test")
def test_forced_shutdown(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(2)  # Ensures the event is stuck in the queue when we send SIGINT
        return get_project_config_original()

    relay = relay(mini_sentry)
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    try:
        relay.send_event(project_id)
        sleep(0.5)  # Give the event time to get stuck

        relay.shutdown(sig=signal.SIGINT)
        pytest.raises(queue.Empty, lambda: mini_sentry.captured_events.get(timeout=1))

        failures = mini_sentry.test_failures
        assert failures

        # we are expecting at least a dropped unfinished future error
        dropped_unfinished_error_found = False
        for (route, error) in failures:
            assert route == "/api/666/envelope/"
            if "Dropped unfinished future" in str(error):
                dropped_unfinished_error_found = True
        assert dropped_unfinished_error_found
    finally:
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
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.get(
        "/api/%d/store/?sentry_data=%s&sentry_key=%s"
        % (project_id, input, mini_sentry.get_dsn_public_key(project_id),)
    )
    response.raise_for_status()
    assert response.headers["content-type"] == "image/gif"

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"]["formatted"] == "im in ur query params"


@pytest.mark.parametrize("route", ["/api/42/store/", "/api/42/store//"])
def test_store_post_trailing_slash(mini_sentry, relay, route):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        "%s?sentry_key=%s" % (route, mini_sentry.get_dsn_public_key(project_id),),
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
    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["allowedDomains"] = allowed_domains

    relay = relay(mini_sentry)

    relay.post(
        "/api/%d/store/?sentry_key=%s"
        % (project_id, mini_sentry.get_dsn_public_key(project_id),),
        headers={"Origin": "http://valid.com"},
        json={"message": "hi"},
    )

    if should_be_allowed:
        mini_sentry.captured_events.get(timeout=1).get_event()
    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "route,status_code",
    [
        ("/api/42/store/", 413),
        ("/api/42/envelope/", 413),
        ("/api/42/attachment/", 413),
        # Minidumps attempt to read the first line (using dedicated limits) and parse it
        ("/api/42/minidump/", 400),
    ],
)
def test_zipbomb_content_encoding(mini_sentry, relay, route, status_code):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(
        mini_sentry,
        options={
            "limits": {
                "max_event_size": "20MB",
                "max_attachment_size": "20MB",
                "max_envelope_size": "20MB",
                "max_api_payload_size": "20MB",
            },
        },
    )
    max_size = 20_000_000

    path = f"{os.path.dirname(__file__)}/fixtures/10GB.gz"
    size = os.path.getsize(path)
    assert size < max_size

    with open(path, "rb") as f:
        response = relay.post(
            "%s?sentry_key=%s" % (route, mini_sentry.get_dsn_public_key(project_id),),
            headers={"content-encoding": "gzip", "content-length": str(size)},
            data=f,
        )

    assert response.status_code == status_code


@pytest.mark.parametrize("content_encoding", ["gzip", "deflate"])
def test_compression(mini_sentry, relay, content_encoding):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/42/store/?sentry_key=%s" % mini_sentry.get_dsn_public_key(project_id),
        headers={"content-encoding": content_encoding},
        data={"deflate": zlib.compress, "gzip": gzip.compress,}[content_encoding](
            b'{"message": "hello world"}'
        ),
    )
    response.raise_for_status()
