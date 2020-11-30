import errno
import gzip
import time

import pytest
import requests

from flask import Response, request


@pytest.mark.parametrize(
    "compress_request", (True, False), ids=("compress_request", "no_compress_request")
)
@pytest.mark.parametrize(
    "compress_response",
    (True, False),
    ids=("compress_response", "no_compress_response"),
)
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

        if request.headers.get("Accept-Encoding", "") == "gzip":
            _data = gzip.compress(_data)
            headers["Content-Encoding"] = "gzip"

        return Response(_data, headers=headers)

    relay = relay_chain()

    headers = {"Content-Type": "application/octet-stream"}

    if compress_request:
        payload = gzip.compress(data)
        headers["Content-Encoding"] = "gzip"
    else:
        payload = data

    if compress_response:
        headers["Accept-Encoding"] = "gzip"

    response = relay.post(
        "/api/test/reflect", data=payload, headers=headers, stream=True
    )
    response.raise_for_status()
    assert response.content == data


def test_forwarding_routes(mini_sentry, relay):
    @mini_sentry.app.route("/")
    @mini_sentry.app.route("/<path:x>")
    def hi(x=None):
        return "ok"

    r = relay(mini_sentry)

    assert r.get("/").status_code == 404
    assert r.get("/foo").status_code == 404
    assert r.get("/foo/bar").status_code == 404
    assert r.get("/api/").status_code == 404
    assert r.get("/api/foo").status_code == 200
    assert r.get("/api/foo/bar").status_code == 200


def test_limits(mini_sentry, relay):
    @mini_sentry.app.route(
        "/api/0/projects/<org>/<project>/releases/<release>/files/", methods=["POST"]
    )
    def dummy_upload(**opts):
        return Response(request.data, content_type="application/octet-stream")

    relay = relay(mini_sentry)

    response = relay.post(
        "/api/0/projects/a/b/releases/1.0/files/",
        data="Hello",
        headers={"Content-Type": "text/plain"},
    )
    assert response.content == b"Hello"

    try:
        response = relay.post(
            "/api/0/projects/a/b/releases/1.0/files/",
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


def test_timeouts(mini_sentry, relay):
    @mini_sentry.app.route("/api/test/timeout")
    def hi():
        time.sleep(60)

    relay = relay(mini_sentry)

    response = relay.get("/api/test/timeout")
    assert response.status_code == 504
