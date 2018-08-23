import os
import uuid
import pytest
import json
import socket
import subprocess
import time
import requests

from pytest_localserver.http import WSGIServer

from flask import Flask, request as flask_request, jsonify

SEMAPHORE_BIN = [os.environ.get("SEMAPHORE_BIN") or "target/debug/semaphore"]

if os.environ.get("SEMAPHORE_AS_CARGO", "false") == "true":
    SEMAPHORE_BIN = ["cargo", "run", "--"]


@pytest.fixture
def mini_sentry(request):
    app = Flask(__name__)
    app.debug = True
    sentry = None

    @app.route("/api/0/relays/register/challenge/", methods=["POST"])
    def get_challenge():
        return jsonify(
            {"token": "123", "relay_id": flask_request.headers["x-sentry-relay-id"]}
        )

    @app.route("/api/0/relays/register/response/", methods=["POST"])
    def check_challenge():
        return jsonify({"relay_id": flask_request.headers["x-sentry-relay-id"]})

    @app.route("/api/<project>/store/", methods=["POST"])
    def store_event(project):
        sentry.captured_events.append(flask_request.json)
        return jsonify({"event_id": uuid.uuid4().hex})

    server = WSGIServer(application=app)
    server.start()
    request.addfinalizer(server.stop)
    sentry = Sentry(server.server_address, app)
    return sentry


class SentryLike(object):
    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)


class Sentry(SentryLike):
    def __init__(self, server_address, app):
        self.server_address = server_address
        self.app = app
        self.trusted_relays = []
        self.captured_events = []


class Relay(SentryLike):
    def __init__(self, server_address, process):
        self.server_address = server_address
        self.process = process

    def _wait(self, url):
        backoff = 0.1
        while True:
            try:
                requests.get(url).raise_for_status()
                break
            except Exception as e:
                time.sleep(backoff)
                if backoff > 0.5:
                    raise
                backoff *= 2

    def wait_authenticated(self):
        self._wait(self.url + "/api/relay/healthcheck/")


@pytest.fixture
def relay(tmpdir, mini_sentry, request):
    i = 0

    def inner(upstream):
        nonlocal i
        i += 1
        name = "relay-config-{}".format(i)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()

        server_address = ("127.0.0.1", port)

        config_dir = tmpdir.mkdir(name)
        config_dir.join("config.yml").write(
            json.dumps(
                {
                    "relay": {
                        "upstream": "http://{}:{}/".format(*upstream.server_address),
                        "host": server_address[0],
                        "port": server_address[1],
                        "tls_port": None,
                        "tls_private_key": None,
                        "tls_cert": None,
                    },
                    "sentry": {
                        "dsn": (
                            # bogus, we never check the DSN
                            "http://31a5a894b4524f74a9a8d0e27e21ba91@{}:{}/42".format(
                                *mini_sentry.server_address
                            )
                        )
                    },
                }
            )
        )

        subprocess.check_call(
            SEMAPHORE_BIN + ["-c", str(config_dir), "credentials", "generate"]
        )
        process = subprocess.Popen(SEMAPHORE_BIN + ["-c", str(config_dir), "run"])
        request.addfinalizer(process.kill)

        return Relay(server_address, process)

    return inner
