from hypothesis import given, settings
from hypothesis import strategies as st
import gzip
import requests

import sentry_sdk

from flask import request, Response


def test_forwarding(mini_sentry, relay_chain_strategy):
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

    @settings(max_examples=50, deadline=5000)
    @given(
        relay=relay_chain_strategy,
        data=st.text(),
        compress_response=st.booleans(),
        compress_request=st.booleans(),
    )
    def test_fuzzing(relay, data, compress_request, compress_response):
        relay.wait_relay_healthcheck()

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

        response = requests.post(
            relay.url + "/test/reflect", data=payload, headers=headers
        )
        response.raise_for_status()
        assert response.content == data

    test_fuzzing()


def test_store(mini_sentry, relay_chain_strategy):
    @given(relay=relay_chain_strategy)
    def test_store(relay):
        print("TEST", relay)
        relay.wait_relay_healthcheck()

        while not mini_sentry.captured_events.empty():
            mini_sentry.captured_events.get()

        client = sentry_sdk.Client(relay.dsn)
        hub = sentry_sdk.Hub(client)
        hub.add_breadcrumb(message="i like bread")
        hub.capture_message("hü")
        client.drain_events()

        event = mini_sentry.captured_events.get()
        assert mini_sentry.captured_events.empty()

        assert event["breadcrumbs"] == [{"message": "i like bread"}]
        assert event["message"] == "hü"

    test_store()
