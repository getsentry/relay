import pytest
import gzip
from datetime import datetime

import requests

import sentry_sdk

from flask import request, Response


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
    trusted_relays = list(relay.iter_public_keys())

    mini_sentry.project_configs[42] = {
        "publicKeys": {relay.dsn_public_key: True},
        "rev": "5ceaea8c919811e8ae7daae9fe877901",
        "disabled": False,
        "lastFetch": "2018-08-24T17:29:04.426Z",
        "lastChange": "2018-07-27T12:27:01.481Z",
        "config": {
            "allowedDomains": ["*"],
            "trustedRelays": trusted_relays,
            "piiConfig": {
                "rules": {},
                "applications": {
                    "freeform": ["@email", "@mac", "@creditcard", "@userpath"],
                    "username": ["@userpath"],
                    "ip": [],
                    "databag": [
                        "@email",
                        "@mac",
                        "@creditcard",
                        "@userpath",
                        "@password",
                    ],
                    "email": ["@email"],
                },
            },
        },
        "slug": "python",
    }

    relay.wait_relay_healthcheck()

    client = sentry_sdk.Client(relay.dsn, default_integrations=False)
    hub = sentry_sdk.Hub(client)
    hub.add_breadcrumb(level="info", message="i like bread", timestamp=datetime.now())
    hub.capture_message("hü")
    client.drain_events()

    event = mini_sentry.captured_events.get(timeout=30)
    assert mini_sentry.captured_events.empty()

    if isinstance(event["breadcrumbs"], dict):
        crumbs = event["breadcrumbs"]["values"]
    else:
        crumbs = event["breadcrumbs"]

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

    response = requests.post(
        relay.url + "/api/0/projects/a/b/releases/1.0/files/",
        data=b"x" * (1024 * 1024 * 2),
        headers={"Content-Type": "text/plain"},
    )
    assert response.status_code == 413


def test_store_node_base64(mini_sentry, relay_chain):
    relay = relay_chain()
    relay.wait_relay_healthcheck()

    mini_sentry.project_configs[42] = {
        "publicKeys": {relay.dsn_public_key: True},
        "rev": "5ceaea8c919811e8ae7daae9fe877901",
        "disabled": False,
        "lastFetch": "2018-08-24T17:29:04.426Z",
        "lastChange": "2018-07-27T12:27:01.481Z",
        "config": {
            "allowedDomains": ["*"],
            "trustedRelays": list(relay.iter_public_keys()),
            "piiConfig": {
                "rules": {},
                "applications": {
                    "freeform": ["@email", "@mac", "@creditcard", "@userpath"],
                    "username": ["@userpath"],
                    "ip": [],
                    "databag": [
                        "@email",
                        "@mac",
                        "@creditcard",
                        "@userpath",
                        "@password",
                    ],
                    "email": ["@email"],
                },
            },
        },
        "slug": "python",
    }

    payload = b"eJytVctu2zAQ/BWDFzuAJYt6WVIfaAsE6KFBi6K3IjAoiXIYSyRLUm7cwP/eJaXEcZr0Bd/E5e7OzJIc3aKOak3WFBXoXCmhislOTDqiNmiO6E1FpWGCo+LrLTI7eZ8Fm1vS9nZ9SNeGVBujSAXhW9QoAq1dZcNaymEF2aUQRkOOXHFRU/9aQ13LOOUCFSkO56gSrf2O5qjpeTWAI963rf+ScMF3nej1ayhifEWkREVDWk3nqBN13/4KgPbzv4bHOb6Hx+kRPihTppf/DTukPVKbRwe44AjuYkhXPb8gjP8Gdfz4C7Q4Xz4z2xFs1QpSnwQqCZKDsPAIy6jdAPfhZGDpASwKnxJ2Ml1p+qcDW9EbQ7mGmPaH2hOgJg8exdOolegkNPlnuIVUbEsMXZhOLuy19TRfMF7Tm0d3555AGB8R+Fhe08o88zCN6h9ScH1hWyoKhLmBUYE3gIuoyWeypXzyaqLot54pOpsqG5ievYB0t+dDQcPWs+mVMVIXi0WSZDQgASF108Q4xqSMaUmDKkuzrEzD5E29Vgx8jSpvWQZ5sizxMgqbKCMJDYPEp73P10psfCYWGE/PfMbhibftzGGiSyvYUVzZGQD7kQaRplf0/M4WZ5x+nzg/nE1HG5yeuRZSaPNA5uX+cr+HrmAQXJO78bmRTIiZPDnHHtiDj+6hiqz18AXdFLHm6kymQNvMx9iP4GBRqSipK9V3pc0d3Fk76Dmyg6XaDD2GE3FJbs7QJvRTaGJFiw2zfQM/8jEEDOto7YkeSlHsBy7mXN4bbR4yIRpYuj2rYR3B2i67OnGNQ1dTqZ00Y3Zo11dEUV49iDDtlX3TWMkI+9hPrSaYwJaq1Xhd35Mfb70LUr0Dlt4nJTycwOOuSGv/VCDErByDNE/iZZLXQY3zOAnDvElpjJcJTXCUZSEZZYGMTlqKAc68IPPC5RccwQUvgsDdUmGPxJKx/GVLTCNUZ39Fzt5/AgZYWKw="  # noqa

    response = requests.post(
        relay.url + "/api/42/store/",
        data=payload,
        headers={
            "Content-Type": "application/octet-stream",
            "X-Sentry-Auth": (
                "Sentry sentry_version=5, sentry_timestamp=1535376240291, "
                "sentry_client=raven-node/2.6.3, "
                "sentry_key={}".format(relay.dsn_public_key)
            ),
        },
    )
    response.raise_for_status()

    event = mini_sentry.captured_events.get(timeout=10)
    assert mini_sentry.captured_events.empty()

    assert event["message"] == "Error: yo mark"
