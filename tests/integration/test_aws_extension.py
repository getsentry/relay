import pytest
from time import sleep


def test_register(aws_lambda_runtime, relay, mini_sentry):
    netloc = aws_lambda_runtime.api_netloc()

    relay_options = {"aws": {"runtime_api": netloc}, "limits": {"shutdown_timeout": 2}}

    relay(mini_sentry, options=relay_options, wait_healthcheck=False)
    sleep(2)  # wait for clean shutdown, trigerred by 5th next_event request

    requests = aws_lambda_runtime.requests
    assert len(requests) == 6

    register = requests[0]
    assert register.url == aws_lambda_runtime.register_url()
    assert register.headers["Lambda-Extension-Name"] == "sentry-lambda-extension"
    assert register.get_json() == {"events": ["INVOKE", "SHUTDOWN"]}

    next_event = requests[1]
    assert next_event.url == aws_lambda_runtime.next_event_url()
    assert next_event.headers["Lambda-Extension-Identifier"] == "mockedExtensionID"

    shutdown = requests[-1]
    assert shutdown.url == aws_lambda_runtime.next_event_url()
    assert shutdown.headers["Lambda-Extension-Identifier"] == "mockedExtensionID"
