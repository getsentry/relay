from hypothesis import given, settings
from hypothesis import strategies as st
import gzip
import requests

from flask import request, Response


def test_forwarding(relay, mini_sentry, gobetween):
    should_compress_response = False
    assert_data = None

    @mini_sentry.app.route("/test/reflect", methods=["POST"])
    def test():
        data = request.data
        if request.headers.get("Content-Encoding", "") == "gzip":
            data = gzip.decompress(data)

        assert data == assert_data

        headers = {}

        if should_compress_response:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"

        return Response(data, headers=headers)

    r1 = relay(gobetween(relay(mini_sentry)))
    r1.wait_relay_healthcheck()

    @settings(max_examples=50)
    @given(
        data=st.text(), compress_response=st.booleans(), compress_request=st.booleans()
    )
    def test_fuzzing(data, compress_request, compress_response):
        data = data.encode("utf-8")
        headers = {"Content-Type": "application/octet-stream"}
        nonlocal should_compress_response
        should_compress_response = compress_response

        nonlocal assert_data
        assert_data = data

        if compress_request:
            payload = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        else:
            payload = data

        sentry_response = requests.post(
            mini_sentry.url + "/test/reflect", data=payload, headers=headers
        )
        sentry_response.raise_for_status()
        assert sentry_response.content == data

        response = requests.post(
            r1.url + "/test/reflect", data=payload, headers=headers
        )
        response.raise_for_status()
        assert response.content == data

    test_fuzzing()
