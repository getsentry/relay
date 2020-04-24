import gzip
import json
import uuid
import types

from queue import Queue

import pytest

from flask import Flask, request as flask_request, jsonify
from pytest_localserver.http import WSGIServer

from . import SentryLike, Envelope


class Sentry(SentryLike):
    def __init__(self, server_address, app):
        self.server_address = server_address
        self.app = app
        self.project_configs = {}
        self.captured_events = Queue()
        self.test_failures = []
        self.upstream = None
        self.hits = {}

    @property
    def internal_error_dsn(self):
        """DSN whose events make the test fail."""
        return "http://{}@{}:{}/666".format(self.dsn_public_key, *self.server_address)

    def get_hits(self, path):
        return self.hits.get(path) or 0

    def hit(self, path):
        self.hits.setdefault(path, 0)
        self.hits[path] += 1


def _get_project_id(public_key, project_configs):
    for project_id, project_config in project_configs.items():
        for key_config in project_config["publicKeys"]:
            if key_config["publicKey"] == public_key:
                return project_id


@pytest.fixture
def mini_sentry(request):
    app = Flask(__name__)
    app.debug = True
    sentry = None

    authenticated_relays = {}

    @app.before_request
    def count_hits():
        if flask_request.url_rule:
            sentry.hit(flask_request.url_rule.rule)

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
        sentry.test_failures.append(
            (
                "/api/666/store/",
                AssertionError("Relay sent us event: {}".format(flask_request.data)),
            )
        )
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/42/store/", methods=["POST"])
    def store_event():
        if flask_request.headers.get("Content-Encoding", "") == "gzip":
            data = gzip.decompress(flask_request.data)
        else:
            data = flask_request.data

        assert flask_request.headers.get("Content-Type") == "application/x-sentry-envelope", "Relay sent us non-envelope data to store"

        envelope = Envelope.deserialize(data)

        sentry.captured_events.put(envelope)
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/<project>/store/", methods=["POST"])
    def store_event_catchall(project):
        raise AssertionError(f"Unknown project: {project}")

    @app.route("/api/0/relays/projectids/", methods=["POST"])
    def get_project_ids():
        project_ids = {}
        for public_key in flask_request.json["publicKeys"]:
            project_ids[public_key] = _get_project_id(
                public_key, sentry.project_configs
            )
        return jsonify(projectIds=project_ids)

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

    @app.errorhandler(500)
    def fail(e):
        sentry.test_failures.append((flask_request.url, e))
        raise e

    @request.addfinalizer
    def reraise_test_failures():
        if sentry.test_failures:
            raise AssertionError(
                f"Exceptions happened in mini_sentry: {sentry.test_failures}"
            )

    server = WSGIServer(application=app, threaded=True)
    server.start()
    request.addfinalizer(server.stop)
    sentry = Sentry(server.server_address, app)
    return sentry
