import gzip
import json
import os
import re
import uuid
import datetime
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
    CURRENT_VERSION = _version_re.search(f.read()).group(1)


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

    def basic_project_config(self, project_id, dsn_public_key=None):
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
