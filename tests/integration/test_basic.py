from hypothesis import given
from hypothesis import strategies as st
import requests

from flask import request


def test_forwarding(relay, mini_sentry):
    @mini_sentry.app.route("/test/reflect", methods=["POST", "GET"])
    def test():
        return request.data

    r1 = relay(mini_sentry)
    r2 = relay(r1)
    r2.wait_authenticated()

    @given(data=st.text())
    def test_fuzzing(data):
        data = data.encode('utf-8')
        response = requests.post(r2.url + '/test/reflect', data=data)
        response.raise_for_status()
        assert response.content == data

    test_fuzzing()
