import datetime
import queue
import os
import gzip
import pytest
import signal
import zlib


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
        for route, error in failures:
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
        % (
            project_id,
            input,
            mini_sentry.get_dsn_public_key(project_id),
        )
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
        "%s?sentry_key=%s"
        % (
            route,
            mini_sentry.get_dsn_public_key(project_id),
        ),
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
        % (
            project_id,
            mini_sentry.get_dsn_public_key(project_id),
        ),
        headers={"Origin": "http://valid.com"},
        json={"message": "hi"},
    )

    if should_be_allowed:
        mini_sentry.captured_events.get(timeout=1).get_event()
    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "route",
    [
        "/api/42/store/",
        "/api/42/envelope/",
        "/api/42/attachment/",
        "/api/42/minidump/",
    ],
)
def test_zipbomb_content_encoding(mini_sentry, relay, route):
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
            "%s?sentry_key=%s"
            % (
                route,
                mini_sentry.get_dsn_public_key(project_id),
            ),
            headers={
                "content-encoding": "gzip",
                "content-length": str(size),
                "content-type": "application/octet-stream",
            },
            data=f,
        )

    assert response.status_code == 413


@pytest.mark.parametrize("content_encoding", ["gzip", "deflate", "identity", ""])
def test_compression(mini_sentry, relay, content_encoding):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(mini_sentry)

    encodings = {
        "deflate": zlib.compress,
        "gzip": gzip.compress,
        "identity": lambda x: x,
        "": lambda x: x,
    }

    response = relay.post(
        "/api/42/store/?sentry_key=%s" % mini_sentry.get_dsn_public_key(project_id),
        headers={"content-encoding": content_encoding},
        data=encodings[content_encoding](b'{"message": "hello world"}'),
    )
    response.raise_for_status()


@pytest.mark.parametrize(
    "cross_origin_resource_policy",
    [
        "cross-origin",
    ],
)
def test_corp_response_header(mini_sentry, relay, cross_origin_resource_policy):
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        f"/api/42/store/?sentry_key={mini_sentry.get_dsn_public_key(project_id)}",
    )

    assert (
        response.headers["cross-origin-resource-policy"] == cross_origin_resource_policy
    )


def send_transaction_with_dsc(mini_sentry, relay, project_id, sampling_project_key):
    relay = relay(mini_sentry)

    now = datetime.datetime.now(datetime.UTC)
    start_timestamp = (now - datetime.timedelta(minutes=1)).timestamp()
    timestamp = now.timestamp()

    relay.send_transaction(
        project_id,
        payload={
            "type": "transaction",
            "transaction": "foo",
            "start_timestamp": start_timestamp,
            "timestamp": timestamp,
            "contexts": {
                "trace": {
                    "trace_id": "1234F60C11214EB38604F4AE0781BFB2",
                    "span_id": "ABCDFDEAD5F74052",
                    "type": "trace",
                }
            },
        },
        trace_info={
            "public_key": sampling_project_key,
            "trace_id": "1234F60C11214EB38604F4AE0781BFB2",
        },
    )

    return mini_sentry.captured_events.get(timeout=1).get_transaction_event()


def test_root_project_disabled(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    disabled_dsn = "00000000000000000000000000000000"
    txn = send_transaction_with_dsc(mini_sentry, relay, project_id, disabled_dsn)
    assert txn


def test_root_project_same(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    same_dsn = mini_sentry.get_dsn_public_key(project_id)
    txn = send_transaction_with_dsc(mini_sentry, relay, project_id, same_dsn)
    assert txn
