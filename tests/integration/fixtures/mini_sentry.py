from collections import Counter
import datetime
import copy
import zstandard
import json
import os
import re
import uuid
from copy import deepcopy
from queue import Empty, Queue

import pytest

from flask import abort, Flask, request as flask_request, jsonify
from werkzeug.serving import WSGIRequestHandler
from pytest_localserver.http import WSGIServer
from sentry_sdk.envelope import Envelope, PayloadRef

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


_METRIC_TO_OUTCOME = {
    "c:outcomes/accepted@none": 0,
    "c:outcomes/filtered@none": 1,
    "c:outcomes/rate_limited@none": 2,
    "c:outcomes/invalid@none": 3,
    "c:outcomes/abuse@none": 4,
    "c:outcomes/client_discard@none": 5,
    "c:outcomes/cardinality_limited@none": 6,
}


class Sentry(SentryLike):
    def __init__(self, server_address, app):
        super().__init__(server_address)

        self.app = app
        self.project_configs = {}
        self.global_config = copy.deepcopy(GLOBAL_CONFIG)
        self.captured_envelopes = Queue()
        self.captured_metrics = Queue()
        self.captured_outcomes = Queue()
        self.test_failures = Queue()
        self.hits = {}
        self.known_relays = {}
        self.fail_on_relay_error = True
        self.request_log = []
        self.project_config_simulate_pending = False
        self.project_config_ignore_revision = False
        self.allow_chunked = False

        self.timeout = 10

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

    def current_test_failures(self):
        """Return current list of test failures without waiting for additional failures."""
        try:
            while failure := self.test_failures.get_nowait():
                yield failure
        except Empty:
            return

    def clear_test_failures(self):
        """Reset test failures to an empty queue."""
        self.test_failures = Queue()

    def format_failures(self):
        s = ""
        for route, error in self.current_test_failures():
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
            "lastFetch": datetime.datetime.now(datetime.UTC).isoformat(),
            "lastChange": datetime.datetime.now(datetime.UTC).isoformat(),
            "config": {
                "allowedDomains": ["*"],
                "trustedRelays": list(self.iter_public_keys()),
                "trustedRelaySettings": {},
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

    def set_global_config_option(self, option_name, value):
        # must be called before initializing relay fixture
        self.global_config["options"][option_name] = value

    def get_captured_envelope(self, *, timeout=None):
        return self.captured_envelopes.get(timeout=timeout or self.timeout)

    def get_client_report(self, timeout=None):
        envelope = self.get_captured_envelope(timeout=timeout)
        items = envelope.items
        assert len(items) == 1
        item = items[0]
        assert item.headers["type"] == "client_report"

        return json.loads(item.payload.bytes)

    def get_metrics(self, timeout=None):
        envelope = self.get_captured_envelope(timeout=timeout)
        items = envelope.items
        assert len(items) == 1
        item = items[0]
        assert item.headers["type"] == "metric_buckets"

        return sorted(
            json.loads(item.payload.get_bytes()),
            key=lambda m: (
                m["name"],
                sorted(m.get("tags", {}).items()),
                m["timestamp"],
            ),
        )

    def get_global_metrics(self, *, timeout=None):
        return self.captured_metrics.get(timeout=timeout or self.timeout)

    def get_outcomes(self, *, n=None, timeout=None):
        outcomes = []

        try:
            while n is None or len(outcomes) < n:
                outcome = self.captured_outcomes.get(timeout=timeout or self.timeout)
                if n is None:
                    # When not waiting for a specific amount don't wait the full timeout after the first item
                    # as usually outcomes arrive all at once.
                    timeout = 0.1
                outcomes.append(outcome)
        except Empty:
            pass

        if n is not None:
            assert n == len(outcomes), f"expected {n} outcomes, got {len(outcomes)}"

        outcomes.sort(key=lambda outcome: outcome["category"])
        return outcomes

    def get_aggregated_outcomes(self, *, n=None, timeout=None):
        aggregated = Counter()

        for outcome in self.get_outcomes(n=n, timeout=timeout):
            del outcome["timestamp"]
            quantity = int(outcome.pop("quantity"))
            aggregated[tuple(sorted(outcome.items()))] += quantity

        outcomes = [
            {**{k: v for (k, v) in fields}, "quantity": quantity}
            for (fields, quantity) in aggregated.items()
        ]
        outcomes.sort(key=lambda o: sorted(o.items()))
        return outcomes


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
        message = message if isinstance(message, str) else message.get("formatted")
        if message and exc_msg:
            return ": ".join((message, exc_msg))
        return exc_msg or message or "unknown error"

    @app.before_request
    def count_hits():
        # Consume POST body even if we don't like this request
        # to no clobber the socket and buffers
        try:
            _ = flask_request.data
        except Exception:
            # stream might be invalid
            pass

        if flask_request.url_rule:
            sentry.hit(flask_request.url_rule.rule)

        # Store endpoints theoretically support chunked transfer encoding,
        # but for now, we're conservative and don't allow that anywhere.
        if not sentry.allow_chunked and flask_request.headers.get("transfer-encoding"):
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

        if (
            event is not None
            and sentry.fail_on_relay_error
            and event.get("level") not in ("info", "warning")
        ):
            error = AssertionError("Relay sent us event: " + get_error_message(event))
            sentry.test_failures.put(("/api/666/envelope/", error))

        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/42/envelope/", methods=["POST"])
    def store_event():
        assert (
            flask_request.headers.get("Content-Encoding", "") == "zstd"
        ), "Relay should always compress store requests"

        with zstandard.ZstdDecompressor().stream_reader(
            flask_request.data
        ) as decompressor:
            data = decompressor.read()

        assert (
            flask_request.headers.get("Content-Type") == "application/x-sentry-envelope"
        ), "Relay sent us non-envelope data to store"

        envelope = Envelope.deserialize(data)
        for outcome in _extract_outcomes_from_envelope(envelope):
            sentry.captured_outcomes.put(outcome)
        if envelope.items:
            sentry.captured_envelopes.put(envelope)
        return jsonify({"event_id": uuid.uuid4().hex})

    @app.route("/api/<project>/store/", methods=["POST"])
    @app.route("/api/<project>/envelope/", methods=["POST"])
    def store_event_catchall(project):
        raise AssertionError(f"Unknown project: {project}")

    @app.route("/api/0/relays/projectconfigs/", methods=["POST"])
    def get_project_config():
        relay_id = flask_request.headers["x-sentry-relay-id"]
        if relay_id not in authenticated_relays:
            abort(403, "relay not registered")

        response = {}
        configs = {}
        pending = []
        unchanged = []
        global_ = None

        version = flask_request.args.get("version")

        if version == "3" and flask_request.json.get("global"):
            global_ = sentry.global_config

        if version in [None, "1"]:
            for project_id in flask_request.json["projects"]:
                project_config = sentry.project_configs[int(project_id)]
                if is_trusted(relay_id, project_config):
                    configs[project_id] = project_config

        elif version in ["2", "3", "4"]:
            for i, public_key in enumerate(flask_request.json["publicKeys"]):
                try:
                    revision = flask_request.json.get("revisions")[i]
                except IndexError:
                    revision = None

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
                            elif (
                                version == "3"
                                and not sentry.project_config_ignore_revision
                                and revision is not None
                                and project_config["rev"] == revision
                            ):
                                unchanged.append(public_key)
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
        response["unchanged"] = unchanged
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
        assert encoding == "zstd", "Relay should always compress store requests"
        with zstandard.ZstdDecompressor().stream_reader(
            flask_request.data
        ) as decompressor:
            data = decompressor.read()

        metrics_batch = json.loads(data)["buckets"]
        for outcome in _extract_outcomes_from_global_metrics(metrics_batch):
            sentry.captured_outcomes.put(outcome)
        if any(metrics_batch.values()):
            sentry.captured_metrics.put(metrics_batch)
        return jsonify({})

    @app.errorhandler(500)
    def fail(e):
        sentry.test_failures.put((flask_request.url, e))
        raise e

    def reraise_test_failures():
        if not sentry.test_failures.empty():
            pytest.fail(
                "{n} exceptions happened in mini_sentry:\n\n{failures}".format(
                    n=sentry.test_failures.qsize(), failures=sentry.format_failures()
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
    },
    "filters": {"version": 1, "filters": []},
    "options": {
        "relay.span-usage-metric": True,
        "relay.session.processing.rollout": 1.0,
        "relay.endpoint-fetch-config.enabled": True,
    },
}


def _extract_outcomes_from_global_metrics(batch):
    outcomes = []

    for public_key, buckets in batch.items():
        other = list()

        for bucket in buckets:
            if outcome := _metric_bucket_to_outcome(bucket):
                outcomes.append({"public_key": public_key, **outcome})
            else:
                other.append(bucket)

        batch[public_key] = other

    return outcomes


def _extract_outcomes_from_envelope(envelope):
    # Implementation detail of Relay, metrics are always sent in an envelope with exactly one item
    if not envelope.items:
        return []

    if envelope.items[0].headers["type"] != "metric_buckets":
        return []

    payload = json.loads(envelope.items[0].payload.get_bytes())
    if not isinstance(payload, list):
        return []

    buckets = list()
    outcomes = list()
    for bucket in payload:
        if not isinstance(bucket, dict):
            return []
        if outcome := _metric_bucket_to_outcome(bucket):
            outcomes.append(outcome)
        else:
            buckets.append(bucket)

    # If there were any outcomes in the batch, re-serialize the buckets
    if len(outcomes) > 0:
        if buckets:
            envelope.items[0].payload = PayloadRef(
                bytes=json.dumps(buckets).encode("utf-8")
            )
        else:
            envelope.items.clear()

    return outcomes


def _metric_bucket_to_outcome(bucket):
    name = bucket["name"]
    if name not in _METRIC_TO_OUTCOME:
        return None

    return {
        "timestamp": bucket["timestamp"],
        "outcome": _METRIC_TO_OUTCOME[name],
        "category": int(bucket["tags"].pop("category")),
        "quantity": int(bucket["value"]),
        **bucket["tags"],
    }
