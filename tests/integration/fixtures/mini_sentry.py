import datetime
import gzip
import json
import os
import re
import uuid
from copy import deepcopy
from queue import Queue

import pytest

from flask import abort, Flask, request as flask_request, jsonify
from werkzeug.serving import WSGIRequestHandler
from pytest_localserver.http import WSGIServer
from sentry_sdk.envelope import Envelope

from . import SentryLike

_version_re = re.compile(r'(?m)^version\s*=\s*"(.*?)"\s*$')
with open(os.path.join(os.path.dirname(__file__), "../../../relay/Cargo.toml")) as f:
    match = _version_re.search(f.read())
    assert match is not None
    CURRENT_VERSION = match[1]


def _parse_version(version):
    if version == "latest":
        return (float("inf"),)

    return tuple(map(int, version.split(".")))


class Sentry(SentryLike):
    def __init__(self, server_address, app):
        super().__init__(server_address)

        self.app = app
        self.project_configs = {}
        self.captured_events = Queue()
        self.captured_outcomes = Queue()
        self.captured_metrics = Queue()
        self.test_failures = []
        self.hits = {}
        self.known_relays = {}
        self.fail_on_relay_error = True
        self.request_log = []
        self.project_config_simulate_pending = False

    @property
    def internal_error_dsn(self):
        """DSN whose events make the test fail."""
        return "http://{}@{}:{}/666".format(
            self.default_dsn_public_key, *self.server_address
        )

    def get_hits(self, path):
        return self.hits.get(path) or 0

    def hit(self, path):
        self.hits.setdefault(path, 0)
        self.hits[path] += 1

    def format_failures(self):
        s = ""
        for route, error in self.test_failures:
            s += f"> {route}: {error}\n"
        return s

    def add_dsn_key_to_project(self, project_id, dsn_public_key=None, numeric_id=None):
        if project_id not in self.project_configs:
            raise Exception("trying to add dsn public key to nonexisting project")

        if dsn_public_key is None:
            dsn_public_key = uuid.uuid4().hex

        public_keys = self.project_configs[project_id]["publicKeys"]

        # generate some unique numeric id ( 1 + max of any other numeric id)
        if numeric_id is None:
            numeric_id = 0
            for public_key_config in public_keys:
                if public_key_config["publicKey"] == dsn_public_key:
                    # we already have this key, just return
                    return dsn_public_key
                numeric_id = max(numeric_id, public_key_config["numericId"])
            numeric_id += 1

        key_entry = {
            "publicKey": dsn_public_key,
            "numericId": numeric_id,
            # For backwards compat, not required by newer relays
            "isEnabled": True,
        }
        public_keys.append(key_entry)

        return key_entry

    def basic_project_config(
        self,
        project_id,
        dsn_public_key=None,
    ):
        if dsn_public_key is None:
            dsn_public_key = {
                "publicKey": uuid.uuid4().hex,
                # For backwards compat, newer relays do not need it
                "isEnabled": True,
                "numericId": 123,
            }

        return {
            "projectId": project_id,
            "slug": "python",
            "publicKeys": [dsn_public_key],
            "rev": "5ceaea8c919811e8ae7daae9fe877901",
            "disabled": False,
            "lastFetch": datetime.datetime.utcnow().isoformat() + "Z",
            "lastChange": datetime.datetime.utcnow().isoformat() + "Z",
            "config": {
                "allowedDomains": ["*"],
                "trustedRelays": list(self.iter_public_keys()),
                "piiConfig": {
                    "rules": {},
                    "applications": {
                        "$string": ["@email", "@mac", "@creditcard", "@userpath"],
                        "$object": ["@password"],
                    },
                },
            },
        }

    def add_basic_project_config(self, project_id, dsn_public_key=None, extra=None):
        ret_val = self.basic_project_config(project_id, dsn_public_key)

        if extra:
            extra_config = extra.get("config", {})
            ret_val = {
                **ret_val,
                **extra,
                "config": {**ret_val["config"], **extra_config},
            }

        self.project_configs[project_id] = ret_val
        return ret_val

    def full_project_config(self, project_id, dsn_public_key=None, extra=None):
        basic = self.basic_project_config(project_id, dsn_public_key)
        full = {
            "organizationId": 1,
            "config": {
                "excludeFields": [],
                "filterSettings": {},
                "scrubIpAddresses": False,
                "sensitiveFields": [],
                "scrubDefaults": True,
                "scrubData": True,
                "groupingConfig": {
                    "id": "legacy:2019-03-12",
                    "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
                },
                "blacklistedIps": ["127.43.33.22"],
                "trustedRelays": [],
            },
        }

        extra = extra or {}
        extra_config = extra.get("config", {})

        ret_val = {
            **basic,
            **full,
            **extra,
            "config": {**basic["config"], **full["config"], **extra_config},
        }

        return ret_val

    def add_full_project_config(self, project_id, dsn_public_key=None, extra=None):
        ret_val = self.full_project_config(project_id, dsn_public_key, extra)
        self.project_configs[project_id] = ret_val
        return ret_val


def _get_project_id(public_key, project_configs):
    for project_id, project_config in project_configs.items():
        for key_config in project_config["publicKeys"]:
            if key_config["publicKey"] == public_key:
                return project_id


@pytest.fixture
def mini_sentry(request):  # noqa
    app = Flask(__name__)
    app.debug = True
    sentry = None

    authenticated_relays = {}

    def is_trusted(relay_id, project_config):
        relay_info = authenticated_relays[relay_id]
        if relay_info.get("internal", False):
            return True
        if not project_config:
            return False
        return relay_info["publicKey"] in project_config["config"]["trustedRelays"]

    def get_error_message(data):
        exceptions = data.get("exception", {}).get("values", [])
        exc_msg = ": ".join(e.get("value", "") for e in reversed(exceptions))
        message = data.get("message", {})
        message = message if type(message) == str else message.get("formatted")
        return exc_msg or message or "unknown error"

    @app.before_request
    def count_hits():
        # Consume POST body even if we don't like this request
        # to no clobber the socket and buffers
        _ = flask_request.data

        if flask_request.url_rule:
            sentry.hit(flask_request.url_rule.rule)

        # Store endpoints theoretically support chunked transfer encoding,
        # but for now, we're conservative and don't allow that anywhere.
        if flask_request.headers.get("transfer-encoding"):
            abort(400, "transfer encoding not supported")

        sentry.request_log.append((flask_request.headers, flask_request.url))

    @app.route("/api/0/relays/register/challenge/", methods=["POST"])
    def get_challenge():
        relay_id = flask_request.json["relay_id"]
        assert relay_id == flask_request.headers["x-sentry-relay-id"]

        if relay_id not in sentry.known_relays:
            abort(403, "unknown relay")

        relay_info = sentry.known_relays[relay_id]

        version = flask_request.json.get("version")
        registered_version = relay_info["version"]

        if version is None:
            if _parse_version(registered_version) >= (20, 8):
                abort(400, "missing version")

        elif version != registered_version and not (
            registered_version == "latest" and version == CURRENT_VERSION
        ):
            abort(403, "outdated version")

        authenticated_relays[relay_id] = relay_info
        return jsonify({"token": "123", "relay_id": relay_id})

    @app.route("/api/0/relays/register/response/", methods=["POST"])
    def check_challenge():
        relay_id = flask_request.json["relay_id"]
        assert relay_id == flask_request.headers["x-sentry-relay-id"]
        assert relay_id in authenticated_relays
        return jsonify({"relay_id": relay_id})

    @app.route("/api/0/relays/live/", methods=["GET"])
    def is_live():
        return jsonify({"is_healthy": True})

    @app.route("/api/666/envelope/", methods=["POST"])
    def store_internal_error_event():
        envelope = Envelope.deserialize(flask_request.data)
        event = envelope.get_event()

        if event is not None and sentry.fail_on_relay_error:
            error = AssertionError("Relay sent us event: " + get_error_message(event))
            sentry.test_failures.append(("/api/666/envelope/", error))

        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/42/envelope/", methods=["POST"])
    def store_event():
        assert (
            flask_request.headers.get("Content-Encoding", "") == "gzip"
        ), "Relay should always compress store requests"
        data = gzip.decompress(flask_request.data)

        assert (
            flask_request.headers.get("Content-Type") == "application/x-sentry-envelope"
        ), "Relay sent us non-envelope data to store"

        envelope = Envelope.deserialize(data)

        sentry.captured_events.put(envelope)
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/<project>/store/", methods=["POST"])
    @app.route("/api/<project>/envelope/", methods=["POST"])
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
        relay_id = flask_request.headers["x-sentry-relay-id"]
        if relay_id not in authenticated_relays:
            abort(403, "relay not registered")

        response = {}
        configs = {}
        pending = []
        global_ = None

        version = flask_request.args.get("version")

        if version == "3" and flask_request.json.get("global"):
            global_ = GLOBAL_CONFIG

        if version in [None, "1"]:
            for project_id in flask_request.json["projects"]:
                project_config = sentry.project_configs[int(project_id)]
                if is_trusted(relay_id, project_config):
                    configs[project_id] = project_config

        elif version in ["2", "3", "4"]:
            for public_key in flask_request.json["publicKeys"]:
                # We store projects by id, but need to return by key
                for project_config in sentry.project_configs.values():
                    for key in project_config["publicKeys"]:
                        if not is_trusted(relay_id, project_config):
                            continue

                        if key["publicKey"] == public_key:
                            if (
                                version == "3"
                                and sentry.project_config_simulate_pending
                            ):
                                pending.append(public_key)
                            else:
                                # TODO 11 Nov 2020 (RaduW) horrible hack
                                #  For some reason returning multiple public keys breaks Relay
                                # Relay seems to work only with the first key
                                # Need to figure out why that is.
                                configs[public_key] = deepcopy(project_config)
                                configs[public_key]["publicKeys"] = [key]

        else:
            abort(500, f"unsupported version: {version}")

        response["configs"] = configs
        response["pending"] = pending
        if global_ is not None:
            response["global"] = global_

        return jsonify(response)

    @app.route("/api/0/relays/publickeys/", methods=["POST"])
    def public_keys():
        relay_id = flask_request.headers["x-sentry-relay-id"]
        if relay_id not in authenticated_relays:
            abort(403, "relay not registered")

        ids = flask_request.json["relay_ids"]
        keys = {}
        relays = {}
        for id in ids:
            relay = authenticated_relays[id]
            if relay:
                keys[id] = relay["publicKey"]
                relays[id] = relay

        return jsonify(public_keys=keys, relays=relays)

    @app.route("/api/0/relays/outcomes/", methods=["POST"])
    def outcomes():
        """
        Mock endpoint for outcomes. SENTRY DOES NOT IMPLEMENT THIS ENDPOINT! This is just used to
        verify Relay's batching behavior.
        """
        relay_id = flask_request.headers["x-sentry-relay-id"]
        if relay_id not in authenticated_relays:
            abort(403, "relay not registered")

        outcomes_batch = flask_request.json
        sentry.captured_outcomes.put(outcomes_batch)
        return jsonify({})

    @app.route("/api/0/relays/metrics/", methods=["POST"])
    def global_metrics():
        """
        Mock endpoint for global batched metrics. SENTRY DOES NOT IMPLEMENT THIS ENDPOINT! This is
        just used to verify Relay's batching behavior.
        """
        relay_id = flask_request.headers["x-sentry-relay-id"]
        if relay_id not in authenticated_relays:
            abort(403, "relay not registered")

        encoding = flask_request.headers.get("Content-Encoding", "")
        assert encoding == "gzip", "Relay should always compress store requests"
        data = gzip.decompress(flask_request.data)

        metrics_batch = json.loads(data)["buckets"]
        sentry.captured_metrics.put(metrics_batch)
        return jsonify({})

    @app.errorhandler(500)
    def fail(e):
        sentry.test_failures.append((flask_request.url, e))
        raise e

    def reraise_test_failures():
        if sentry.test_failures:
            pytest.fail(
                "{n} exceptions happened in mini_sentry:\n\n{failures}".format(
                    n=len(sentry.test_failures), failures=sentry.format_failures()
                )
            )

    # This marker is used by pytest_runtest_call in our conftest.py
    mark = pytest.mark.extra_failure_checks(checks=[reraise_test_failures])
    request.node.add_marker(mark)

    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    server = WSGIServer(application=app, threaded=True)
    server.start()
    request.addfinalizer(server.stop)
    sentry = Sentry(server.server_address, app)
    return sentry


GLOBAL_CONFIG = {
    "measurements": {
        "builtinMeasurements": [
            {"name": "app_start_cold", "unit": "millisecond"},
            {"name": "app_start_warm", "unit": "millisecond"},
            {"name": "cls", "unit": "none"},
            {"name": "fcp", "unit": "millisecond"},
            {"name": "fid", "unit": "millisecond"},
            {"name": "fp", "unit": "millisecond"},
            {"name": "frames_frozen_rate", "unit": "ratio"},
            {"name": "frames_frozen", "unit": "none"},
            {"name": "frames_slow_rate", "unit": "ratio"},
            {"name": "frames_slow", "unit": "none"},
            {"name": "frames_total", "unit": "none"},
            {"name": "inp", "unit": "millisecond"},
            {"name": "lcp", "unit": "millisecond"},
            {"name": "stall_count", "unit": "none"},
            {"name": "stall_longest_time", "unit": "millisecond"},
            {"name": "stall_percentage", "unit": "ratio"},
            {"name": "stall_total_time", "unit": "millisecond"},
            {"name": "ttfb.requesttime", "unit": "millisecond"},
            {"name": "ttfb", "unit": "millisecond"},
            {"name": "time_to_full_display", "unit": "millisecond"},
            {"name": "time_to_initial_display", "unit": "millisecond"},
        ],
        "maxCustomMeasurements": 10,
    }
}
