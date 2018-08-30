import errno
import gzip
import socket
import time

from datetime import datetime

import pytest
import requests
import sentry_sdk

from flask import Response, request, jsonify


@pytest.mark.parametrize("compress_request", (True, False))
@pytest.mark.parametrize("compress_response", (True, False))
def test_forwarding_content_encoding(
    compress_request, compress_response, mini_sentry, relay_chain
):
    data = b"foobar"

    @mini_sentry.app.route("/api/test/reflect", methods=["POST"])
    def test():
        _data = request.data
        if request.headers.get("Content-Encoding", "") == "gzip":
            _data = gzip.decompress(_data)

        assert _data == data

        headers = {}

        if compress_response:
            _data = gzip.compress(_data)
            headers["Content-Encoding"] = "gzip"

        return Response(_data, headers=headers)

    relay = relay_chain()
    relay.wait_relay_healthcheck()

    headers = {"Content-Type": "application/octet-stream"}

    if compress_request:
        payload = gzip.compress(data)
        headers["Content-Encoding"] = "gzip"
    else:
        payload = data

    response = requests.post(
        relay.url + "/api/test/reflect", data=payload, headers=headers
    )
    response.raise_for_status()
    assert response.content == data


def test_forwarding_routes(mini_sentry, relay):
    r = relay(mini_sentry)

    @mini_sentry.app.route("/")
    @mini_sentry.app.route("/<path:x>")
    def hi(x=None):
        return "ok"

    r.wait_relay_healthcheck()

    assert requests.get(r.url + "/").status_code == 404
    assert requests.get(r.url + "/foo").status_code == 404
    assert requests.get(r.url + "/foo/bar").status_code == 404
    assert requests.get(r.url + "/api/").status_code == 404
    assert requests.get(r.url + "/api/foo").status_code == 200
    assert requests.get(r.url + "/api/foo/bar").status_code == 200


def test_store(mini_sentry, relay_chain):
    relay = relay_chain()
    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.wait_relay_healthcheck()

    client = sentry_sdk.Client(relay.dsn, default_integrations=False)
    hub = sentry_sdk.Hub(client)
    hub.add_breadcrumb(level="info", message="i like bread", timestamp=datetime.now())
    hub.capture_message("hü")
    client.drain_events()

    event = mini_sentry.captured_events.get(timeout=30)
    assert mini_sentry.captured_events.empty()

    crumbs = event["breadcrumbs"]["values"]
    assert any(crumb["message"] == "i like bread" for crumb in crumbs)
    assert event["message"] == "hü"


def test_limits(mini_sentry, relay):
    @mini_sentry.app.route(
        "/api/0/projects/<org>/<project>/releases/<release>/files/", methods=["POST"]
    )
    def dummy_upload(**opts):
        return Response(request.data, content_type="application/octet-stream")

    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    response = requests.post(
        relay.url + "/api/0/projects/a/b/releases/1.0/files/",
        data="Hello",
        headers={"Content-Type": "text/plain"},
    )
    assert response.content == b"Hello"

    try:
        response = requests.post(
            relay.url + "/api/0/projects/a/b/releases/1.0/files/",
            data=b"x" * (1024 * 1024 * 2),
            headers={"Content-Type": "text/plain"},
        )
    except requests.exceptions.ConnectionError as e:
        if e.errno not in (errno.ECONNRESET, errno.EPIPE):
            # XXX: Aborting response during chunked upload sometimes goes
            # wrong.
            raise
    else:
        assert response.status_code == 413


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    payload = b"eJytVctu2zAQ/BWDFzuAJYt6WVIfaAsE6KFBi6K3IjAoiXIYSyRLUm7cwP/eJaXEcZr0Bd/E5e7OzJIc3aKOak3WFBXoXCmhislOTDqiNmiO6E1FpWGCo+LrLTI7eZ8Fm1vS9nZ9SNeGVBujSAXhW9QoAq1dZcNaymEF2aUQRkOOXHFRU/9aQ13LOOUCFSkO56gSrf2O5qjpeTWAI963rf+ScMF3nej1ayhifEWkREVDWk3nqBN13/4KgPbzv4bHOb6Hx+kRPihTppf/DTukPVKbRwe44AjuYkhXPb8gjP8Gdfz4C7Q4Xz4z2xFs1QpSnwQqCZKDsPAIy6jdAPfhZGDpASwKnxJ2Ml1p+qcDW9EbQ7mGmPaH2hOgJg8exdOolegkNPlnuIVUbEsMXZhOLuy19TRfMF7Tm0d3555AGB8R+Fhe08o88zCN6h9ScH1hWyoKhLmBUYE3gIuoyWeypXzyaqLot54pOpsqG5ievYB0t+dDQcPWs+mVMVIXi0WSZDQgASF108Q4xqSMaUmDKkuzrEzD5E29Vgx8jSpvWQZ5sizxMgqbKCMJDYPEp73P10psfCYWGE/PfMbhibftzGGiSyvYUVzZGQD7kQaRplf0/M4WZ5x+nzg/nE1HG5yeuRZSaPNA5uX+cr+HrmAQXJO78bmRTIiZPDnHHtiDj+6hiqz18AXdFLHm6kymQNvMx9iP4GBRqSipK9V3pc0d3Fk76Dmyg6XaDD2GE3FJbs7QJvRTaGJFiw2zfQM/8jEEDOto7YkeSlHsBy7mXN4bbR4yIRpYuj2rYR3B2i67OnGNQ1dTqZ00Y3Zo11dEUV49iDDtlX3TWMkI+9hPrSaYwJaq1Xhd35Mfb70LUr0Dlt4nJTycwOOuSGv/VCDErByDNE/iZZLXQY3zOAnDvElpjJcJTXCUZSEZZYGMTlqKAc68IPPC5RccwQUvgsDdUmGPxJKx/GVLTCNUZ39Fzt5/AgZYWKw="  # noqa
    response = relay.send_event(42, payload)
    response.raise_for_status()

    event = mini_sentry.captured_events.get(timeout=10)
    assert mini_sentry.captured_events.empty()

    assert event["message"] == "Error: yo mark"


def test_store_pii_stripping(mini_sentry, relay):
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()
    relay.send_event(42, {"message": "test@mail.org"}).raise_for_status()

    event = mini_sentry.captured_events.get(timeout=10)
    assert mini_sentry.captured_events.empty()

    # Email should be stripped:
    assert event["message"] == "[email]"


def test_event_timeout(mini_sentry, relay):
    from time import sleep

    get_project_config_original = mini_sentry.app.view_functions["get_project_config"]

    @mini_sentry.app.endpoint("get_project_config")
    def get_project_config():
        sleep(1.5)
        return get_project_config_original()

    relay = relay(mini_sentry, {'cache': {'event_expiry': 1}})
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = relay.basic_project_config()

    relay.send_event(42, {"message": "invalid"}).raise_for_status()
    sleep(1)
    relay.send_event(42, {"message": "correct"}).raise_for_status()

    assert mini_sentry.captured_events.get(timeout=1)["message"] == "correct"
    assert mini_sentry.captured_events.get(timeout=1) is None


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

    client = sentry_sdk.Client(relay.dsn, default_integrations=False)
    hub = sentry_sdk.Hub(client)
    hub.capture_message("hü")
    client.drain_events()

    event = mini_sentry.captured_events.get(timeout=3600)
    assert mini_sentry.captured_events.empty()

    assert event["message"] == "hü"

    assert retry_count == 2

    if mini_sentry.test_failures:
        (_, error), = mini_sentry.test_failures
        assert isinstance(error, socket.error)
        mini_sentry.test_failures.clear()
