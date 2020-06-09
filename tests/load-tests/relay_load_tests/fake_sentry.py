from queue import Queue

from flask import Flask, request as flask_request, jsonify, abort
import threading
import datetime
import time
import os

from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader


class Sentry(object):
    _healthcheck_passed = False

    def __init__(self, server_address, dns_public_key, app):
        self.server_address = server_address
        self.app = app
        self.project_configs = {}
        self.captured_events = Queue()
        self.test_failures = []
        self.upstream = None
        self.dsn_public_key = dns_public_key

    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)

    def _wait(self, path):
        backoff = 0.1
        while True:
            try:
                self.get(path).raise_for_status()
                break
            except Exception:
                time.sleep(backoff)
                if backoff > 10:
                    raise
                backoff *= 2

    def wait_relay_healthcheck(self):
        if self._healthcheck_passed:
            return

        self._wait("/api/relay/healthcheck/")
        self._healthcheck_passed = True

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, repr(self.upstream))

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
            "publicKeys": [
                {"publicKey": self.dsn_public_key, "isEnabled": True, "numericId": 123}
            ],
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
            "slug": "python",
        }

    def full_project_config(self):
        basic = self.basic_project_config()
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

        return {
            **basic,
            **full,
            "config": {**basic["config"], **full["config"]},
        }

    @property
    def internal_error_dsn(self):
        """DSN whose events make the test fail."""
        return "http://{}@{}:{}/666".format(self.dsn_public_key, *self.server_address)


def run_fake_sentry_async(config):
    """
    Creates a fake sentry server in a new thread (and starts it) and returns the thread
    :param config: the cli configuration
    :return: the thread in which the fake
    """
    t = threading.Thread(target=_fake_sentry_thread, args=(config,))
    t.daemon = True
    t.start()
    return t


def run_blocking_fake_sentry(config):
    """
    Runs a fake sentry server on the current thread (this is a blocking call)
    :param config: the cli configuration
    """
    _fake_sentry_thread(config)


def _fake_sentry_thread(config):
    app = Flask(__name__)
    # sentry = None
    host = config.get("host")
    port = config.get("port")
    dns_public_key = config.get("key")
    sentry = Sentry((host, port), dns_public_key, app)

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

    @app.route("/api/0/relays/projectconfigs/", methods=["POST"])
    def get_project_config():
        request_body = flask_request.json
        print("get_project_config called for:{}".format(request_body))
        rv = {}
        for project_id in request_body["projects"]:
            rv[project_id] = sentry.full_project_config()
        return jsonify(configs=rv)

    @app.route("/api/0/relays/publickeys/", methods=["POST"])
    def public_keys():
        ids = flask_request.json["relay_ids"]
        rv = {}
        for id in ids:
            rv[id] = authenticated_relays[id]

        return jsonify(public_keys=rv)

    @app.route("/api/<project_id>/store", methods=["POST", "GET"])
    def store_all(project_id):
        print("FAKE sentry store called for project:{}".format(repr(project_id)))
        return ""

    @app.route("/<path:u_path>", methods=["POST", "GET"])
    def catch_all(u_path):
        print("fake sentry called on:'{}'".format(repr(u_path)))
        return (
            "<h1>Fake Sentry</h1>"
            + "<div>You have called fake-sentry on: <nbsp/>"
            + "<span style='font-family:monospace; background-color:#e8e8e8;'>{}</span></div>".format(
                u_path
            )
            + "<h3><b>Note:</b> This is probably the wrong url to call !!!<h3/>"
        )

    @app.route("/", methods=["GET"])
    def root():
        print("Root url called.")
        return "<h1>Fake Sentry</h1><div>This is the root url</div>"

    @app.errorhandler(Exception)
    def fail(e):
        print("Fake sentry error generated error:\n{}".format(e))
        abort(400)

    app.run(host=host, port=port)


def _get_config():
    """
    Returns the program settings located in the main directory (just above this file's directory)
    with the name config.yml
    """
    file_name = os.path.realpath(
        os.path.join(__file__, "../..", "fake_sentry.config.yml")
    )
    try:
        with open(file_name, "r") as file:
            return load(file, Loader=FullLoader)
    except Exception as err:
        print("Error while getting the configuration file:\n {}".format(err))
        raise ValueError("Invalid configuration")


if __name__ == "__main__":
    config = _get_config()
    run_blocking_fake_sentry(config)
