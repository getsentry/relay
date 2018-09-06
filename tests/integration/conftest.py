import os
import uuid
import pytest
import json
import signal
import socket
import subprocess
import time
import requests
import gzip
import shutil

from queue import Queue

from pytest_localserver.http import WSGIServer
from flask import Flask, request as flask_request, jsonify

SEMAPHORE_BIN = [os.environ.get("SEMAPHORE_BIN") or "target/debug/semaphore"]

if os.environ.get("SEMAPHORE_AS_CARGO", "false") == "true":
    SEMAPHORE_BIN = ["cargo", "run", "--"]

GOBETWEEN_BIN = [os.environ.get("GOBETWEEN_BIN") or "gobetween"]
HAPROXY_BIN = [os.environ.get("HAPROXY_BIN") or "haproxy"]


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

    authenticated_relays = {}

    @app.route("/api/0/relays/register/challenge/", methods=["POST"])
    def get_challenge():
        relay_id = flask_request.json["relay_id"]
        public_key = flask_request.json["public_key"]
        authenticated_relays[relay_id] = public_key

        assert relay_id == flask_request.headers["x-sentry-relay-id"]
        return jsonify({"token": "123", "relay_id": relay_id})

    @app.route("/api/0/relays/register/response/", methods=["POST"])
    def check_challenge():
        relay_id = flask_request.json["relay_id"]
        assert relay_id == flask_request.headers["x-sentry-relay-id"]
        assert relay_id in authenticated_relays
        return jsonify({"relay_id": relay_id})

    @app.route("/api/666/store/", methods=["POST"])
    def store_internal_error_event():
        sentry.test_failures.append(AssertionError("Relay sent us event"))
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/42/store/", methods=["POST"])
    def store_event():
        if flask_request.headers.get("Content-Encoding", "") == "gzip":
            data = gzip.decompress(flask_request.data)
        else:
            data = flask_request.data

        sentry.captured_events.put(json.loads(data))
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/<project>/store/", methods=["POST"])
    def store_event_catchall(project):
        raise AssertionError(f"Unknown project: {project}")

    @app.route("/api/0/relays/projectconfigs/", methods=["POST"])
    def get_project_config():
        rv = {}
        for project_id in flask_request.json["projects"]:
            rv[project_id] = sentry.project_configs[int(project_id)]

        return jsonify(configs=rv)

    @app.route("/api/0/relays/publickeys/", methods=["POST"])
    def public_keys():
        ids = flask_request.json["relay_ids"]
        rv = {}
        for id in ids:
            rv[id] = authenticated_relays[id]

        return jsonify(public_keys=rv)

    @app.errorhandler(Exception)
    def fail(e):
        sentry.test_failures.append((flask_request.url, e))
        raise e

    @request.addfinalizer
    def reraise_test_failures():
        if sentry.test_failures:
            raise AssertionError(f"Exceptions happened in mini_sentry: {test_failures}")

    server = WSGIServer(application=app, threaded=True)
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
                if backoff > 10:
                    raise
                backoff *= 2

    def wait_relay_healthcheck(self):
        if self._healthcheck_passed:
            return

        self._wait(self.url + "/api/relay/healthcheck/")
        self._healthcheck_passed = True

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, repr(self.upstream))

    @property
    def dsn_public_key(self):
        return "31a5a894b4524f74a9a8d0e27e21ba91"

    @property
    def dsn(self):
        """DSN for which you will find the events in self.captured_events"""
        # bogus, we never check the DSN
        return "http://{}@{}:{}/42".format(self.dsn_public_key, *self.server_address)

    def iter_public_keys(self):
        try:
            yield self.public_key
        except AttributeError:
            pass

        if self.upstream is not None:
            if isinstance(self.upstream, tuple):
                for upstream in self.upstream:
                    yield from upstream.iter_public_keys()
            else:
                yield from self.upstream.iter_public_keys()

    def basic_project_config(self):
        return {
            "publicKeys": {self.dsn_public_key: True},
            "rev": "5ceaea8c919811e8ae7daae9fe877901",
            "disabled": False,
            "lastFetch": "2018-08-24T17:29:04.426Z",
            "lastChange": "2018-07-27T12:27:01.481Z",
            "config": {
                "allowedDomains": ["*"],
                "trustedRelays": list(self.iter_public_keys()),
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

    def send_event(self, project_id, payload=None):
        content_type = None
        if payload is None:
            payload = {"message": "Hello, World!"}

        if isinstance(payload, bytes):
            content_type = 'application/octet-stream'

        if isinstance(payload, dict):
            payload = json.dumps(payload)
            content_type = 'application/json'

        return requests.post(
            self.url + "/api/%s/store/" % project_id,
            data=payload,
            headers={
                "Content-Type": content_type,
                "X-Sentry-Auth": (
                    "Sentry sentry_version=5, sentry_timestamp=1535376240291, "
                    "sentry_client=raven-node/2.6.3, "
                    "sentry_key={}".format(self.dsn_public_key)
                ),
            },
        )



class Sentry(SentryLike):
    def __init__(self, server_address, app):
        self.server_address = server_address
        self.app = app
        self.project_configs = {}
        self.captured_events = Queue()
        self.test_failures = []
        self.upstream = None

    @property
    def internal_error_dsn(self):
        """DSN whose events make the test fail."""
        return "http://{}@{}:{}/666".format(self.dsn_public_key, *self.server_address)


class Relay(SentryLike):
    def __init__(self, server_address, process, upstream, public_key, relay_id):
        self.server_address = server_address
        self.process = process
        self.upstream = upstream
        self.public_key = public_key
        self.relay_id = relay_id

    def shutdown(self, graceful=True):
        sig = signal.SIGTERM if graceful else signal.SIGINT
        self.process.send_signal(sig)

        try:
            self.process.wait(12)
        except subprocess.TimeoutExpired:
            self.process.kill()
            raise

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
    def inner(upstream, options=None):
        host = "127.0.0.1"
        port = random_port()

        default_opts = {
            "relay": {
                "upstream": upstream.url,
                "host": host,
                "port": port,
                "tls_port": None,
                "tls_private_key": None,
                "tls_cert": None,
            },
            "sentry": {"dsn": mini_sentry.internal_error_dsn},
            "limits": {"max_api_file_upload_size": "1MiB"},
            "cache": {"batch_interval": 0},
            "logging": {"level": "trace"},
            "http": {"timeout": 2},
        }

        if options is not None:
            for key in options:
                default_opts.setdefault(key, {}).update(options[key])

        dir = config_dir("relay")
        dir.join("config.yml").write(json.dumps(default_opts))

        output = subprocess.check_output(
            SEMAPHORE_BIN + ["-c", str(dir), "credentials", "generate"]
        )

        process = background_process(SEMAPHORE_BIN + ["-c", str(dir), "run"])

        public_key = None
        relay_id = None

        for line in output.splitlines():
            if b"public key" in line:
                public_key = line.split()[-1].decode("ascii")
            if b"relay id" in line:
                relay_id = line.split()[-1].decode("ascii")

        assert public_key
        assert relay_id

        return Relay((host, port), process, upstream, public_key, relay_id)

    return inner


class Gobetween(SentryLike):
    def __init__(self, server_address, process, upstream):
        self.server_address = server_address
        self.process = process
        self.upstream = upstream


@pytest.fixture
def gobetween(background_process, random_port, config_dir):
    def inner(*upstreams):
        if shutil.which(GOBETWEEN_BIN[0]) is None:
            pytest.skip("Gobetween not installed")

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


class HAProxy(SentryLike):
    def __init__(self, server_address, process, upstream):
        self.server_address = server_address
        self.process = process
        self.upstream = upstream


@pytest.fixture
def haproxy(background_process, random_port, config_dir):
    def inner(*upstreams):
        if shutil.which(HAPROXY_BIN[0]) is None:
            pytest.skip("HAProxy not installed")

        host = "127.0.0.1"
        port = random_port()

        config = config_dir("haproxy").join("config")

        config_lines = [
            f"defaults",
            f"    mode http",
            f"    timeout connect 25000ms",
            f"    timeout client 25000ms",
            f"    timeout server 25000ms",
            f"    timeout queue 25000ms",
            f"    timeout http-request 25000ms",
            f"    timeout http-keep-alive 25000ms",
            f"    option forwardfor",
            f"    option redispatch",
            f"frontend defaultFront",
            f"    bind {host}:{port}",
            f"    default_backend defaultBack",
            f"backend defaultBack",
            f"    balance roundrobin",
        ]

        for i, upstream in enumerate(upstreams):
            upstream_host, upstream_port = upstream.server_address
            config_lines.append(
                f"    server sentryUpstream{i} {upstream_host}:{upstream_port} no-check"
            )

        config.write("\n".join(config_lines))

        process = background_process(HAPROXY_BIN + ["-f", str(config)])

        return HAProxy((host, port), process, upstreams)

    return inner


@pytest.fixture(
    params=[
        lambda s, r, g, h: r(s),
        lambda s, r, g, h: r(r(s)),
        lambda s, r, g, h: r(h(r(g(s)))),
        lambda s, r, g, h: r(g(r(h(s)))),
    ]
)
def relay_chain(request, mini_sentry, relay, gobetween, haproxy):
    return lambda: request.param(mini_sentry, relay, gobetween, haproxy)
