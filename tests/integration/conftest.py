import os
import uuid
import pytest
import json
import socket
import subprocess
import time
import requests
import functools

from hypothesis import given
from hypothesis import strategies as st

from pytest_localserver.http import WSGIServer

from flask import Flask, request as flask_request, jsonify

SEMAPHORE_BIN = [os.environ.get("SEMAPHORE_BIN") or "target/debug/semaphore"]

if os.environ.get("SEMAPHORE_AS_CARGO", "false") == "true":
    SEMAPHORE_BIN = ["cargo", "run", "--"]

GOBETWEEN_BIN = [os.environ.get("GOBETWEEN_BIN") or "gobetween"]


@pytest.fixture
def random_port():
    def inner():
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
        s.close()
        return port

    return inner


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

    @app.route("/api/relay/healthcheck/")
    def healthcheck():
        return "ok"

    server = WSGIServer(application=app)
    server.start()
    request.addfinalizer(server.stop)
    sentry = Sentry(server.server_address, app)
    return sentry


class SentryLike(object):
    _healthcheck_passed = False

    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)

    def _wait(self, url):
        backoff = 0.1
        while True:
            try:
                requests.get(url).raise_for_status()
                break
            except Exception as e:
                time.sleep(backoff)
                if backoff > 3:
                    raise
                backoff *= 2

    def wait_relay_healthcheck(self):
        if self._healthcheck_passed:
            return

        self._wait(self.url + "/api/relay/healthcheck/")
        self._healthcheck_passed = True

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, repr(self.upstream))


class Sentry(SentryLike):
    def __init__(self, server_address, app):
        self.server_address = server_address
        self.app = app
        self.trusted_relays = []
        self.captured_events = []
        self.upstream = None


class Relay(SentryLike):
    def __init__(self, server_address, process, upstream):
        self.server_address = server_address
        self.process = process
        self.upstream = upstream


@pytest.fixture
def background_process(request):
    def inner(*args, **kwargs):
        p = subprocess.Popen(*args, **kwargs)
        request.addfinalizer(p.kill)
        return p

    return inner


@pytest.fixture
def config_dir(tmpdir):
    counters = {}

    def inner(name):
        counters.setdefault(name, 0)
        counters[name] += 1
        return tmpdir.mkdir("{}-{}".format(name, counters[name]))

    return inner


@pytest.fixture
def relay(tmpdir, mini_sentry, request, random_port, background_process, config_dir):
    def inner(upstream):
        host = "127.0.0.1"
        port = random_port()

        dir = config_dir("relay")
        dir.join("config.yml").write(
            json.dumps(
                {
                    "relay": {
                        "upstream": upstream.url,
                        "host": host,
                        "port": port,
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
            SEMAPHORE_BIN + ["-c", str(dir), "credentials", "generate"]
        )
        process = background_process(SEMAPHORE_BIN + ["-c", str(dir), "run"])

        return Relay((host, port), process, upstream)

    return inner


class Gobetween(SentryLike):
    def __init__(self, server_address, process, upstream):
        self.server_address = server_address
        self.process = process
        self.upstream = upstream


@pytest.fixture
def gobetween(background_process, random_port, config_dir):
    def inner(*upstreams):
        host = "127.0.0.1"
        port = random_port()

        config = config_dir("gobetween").join("config.json")
        config.write(
            json.dumps(
                {
                    "logging": {"level": "debug", "output": "stdout"},
                    "api": {
                        "enabled": True,
                        "bind": f"{host}:{random_port()}",
                        "cors": False,
                    },
                    "defaults": {
                        "max_connections": 0,
                        "client_idle_timeout": "0",
                        "backend_idle_timeout": "0",
                        "backend_connection_timeout": "0",
                    },
                    "servers": {
                        "sample": {
                            "protocol": "tcp",
                            "bind": f"{host}:{port}",
                            "discovery": {
                                "kind": "static",
                                "static_list": [
                                    f"{u.server_address[0]}:{u.server_address[1]}"
                                    for u in upstreams
                                ],
                            },
                        }
                    },
                }
            )
        )

        process = background_process(
            GOBETWEEN_BIN + ["from-file", "-fjson", str(config)]
        )

        return Gobetween((host, port), process, upstreams)

    return inner


@pytest.fixture
def relay_chain_strategy(relay, mini_sentry, gobetween):
    chain_cache = {}

    def spawn_chain(chain):
        if not chain:
            return mini_sentry
        chain = tuple(chain)
        if chain in chain_cache:
            return chain_cache[chain]

        f, *rest = chain
        rv = chain_cache[chain] = f(spawn_chain(rest))
        return rv

    return st.lists(st.one_of(st.just(relay), st.just(gobetween)), max_size=10).map(
        spawn_chain
    )
