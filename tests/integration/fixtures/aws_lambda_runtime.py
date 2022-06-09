import pytest
import logging
from time import sleep
from urllib.parse import urlparse
from flask import Flask, request as flask_request
from pytest_localserver.http import WSGIServer


BASE_URL = "/2020-01-01/extension"
LAMBDA_EXTENSION_ID = "mockedExtensionID"
HEADERS = {"Lambda-Extension-Identifier": LAMBDA_EXTENSION_ID}
SHUTDOWN_MAX = 5


class AwsLambdaRuntime:
    def __init__(self, server, app):
        self.server = server
        self.app = app
        self.requests = []

    def api_netloc(self):
        return urlparse(self.server.url).netloc

    def register_url(self):
        return f"http://{self.api_netloc()}{BASE_URL}/register"

    def next_event_url(self):
        return f"http://{self.api_netloc()}{BASE_URL}/event/next"

    def capture_request(self, request):
        self.requests.append(request)


@pytest.fixture
def aws_lambda_runtime(request):
    app = Flask(__name__)
    runtime = None

    @app.before_request
    def capture_request():
        runtime.capture_request(flask_request._get_current_object())

    @app.route(f"{BASE_URL}/register", methods=["POST"])
    def register():
        request_headers = flask_request.headers
        request_body = flask_request.get_json()
        print(f"Register request headers: {request_headers}")
        print(f"Register request body: {request_body}")

        data = {
            "functionName": "helloWorld",
            "functionVersion": "X.X.X",
            "handler": "lambda_function.lambda_handler",
        }

        return (data, HEADERS)

    @app.route(f"{BASE_URL}/event/next")
    def next_event():
        request_headers = flask_request.headers
        print(f"Next request headers: {request_headers}")

        if len(runtime.requests) > SHUTDOWN_MAX:
            data = {
                "eventType": "SHUTDOWN",
                "deadlineMs": 1581512138111,
                "shutdownReason": "OOPSIES",
            }
        else:
            data = {
                "eventType": "INVOKE",
                "deadlineMs": 1581512138111,
                "requestId": "aws-request-ID",
                "invokedFunctionArn": "invoked-function-arn",
                "tracing": {
                    "type": "X-Amzn-Trace-Id",
                    "value": "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
                },
            }
        return (data, HEADERS)

    server = WSGIServer(application=app, threaded=True)
    server.start()
    request.addfinalizer(server.stop)
    runtime = AwsLambdaRuntime(server, app)
    return runtime
