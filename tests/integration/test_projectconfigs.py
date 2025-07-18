"""
Tests the project_configs endpoint (/api/0/relays/projectconfigs/)
"""

import uuid
import pytest
import time
from collections import namedtuple

from sentry_relay.auth import PublicKey, SecretKey, generate_key_pair

RelayInfo = namedtuple("RelayInfo", ["id", "public_key", "secret_key", "internal"])


def request_config(
    relay, packed, signature, *, version="3", relay_id=None, headers=None
):
    return relay.post(
        f"/api/0/relays/projectconfigs/?version={version}",
        data=packed,
        headers={
            "X-Sentry-Relay-Id": relay_id or relay.relay_id,
            "X-Sentry-Relay-Signature": signature,
        }
        | (headers or {}),
    )


def get_response(relay, packed, signature, *, version="3", relay_id=None, headers=None):
    data = None
    deadline = time.monotonic() + 15
    while time.monotonic() <= deadline:
        response = request_config(
            relay,
            packed,
            signature,
            version=version,
            relay_id=relay_id,
            headers=headers,
        )
        assert response.ok
        data = response.json()
        print(data)
        if data["configs"] or data.get("global") or data.get("unchanged"):
            return data, response
        time.sleep(0.01)

    assert False, "Relay did still not receive a project config from minisentry"


@pytest.mark.parametrize(
    "caller, projects",
    [
        ("r2", ["p2"]),
        ("sr1", ["p1", "p2", "p3", "p4"]),
        ("sr2", ["p4"]),
    ],
    ids=[
        "dyn external relay fetches proj info",
        "static internal relay fetches proj info",
        "static external relay fetches proj info",
    ],
)
def test_dynamic_relays(mini_sentry, relay, caller, projects):
    sk1, pk1 = generate_key_pair()
    id1 = str(uuid.uuid4())
    sk2, pk2 = generate_key_pair()
    id2 = str(uuid.uuid4())

    # create configuration containing the static relays
    relays_conf = {
        id1: {"public_key": str(pk1), "internal": True},
        id2: {"public_key": str(pk2), "internal": False},
    }

    relay1 = relay(mini_sentry, static_relays=relays_conf)
    relay2 = relay(mini_sentry, external=True, static_relays=relays_conf)

    # create info for our test parameters
    r1 = RelayInfo(
        id=relay1.relay_id,
        public_key=PublicKey.parse(relay1.public_key),
        secret_key=SecretKey.parse(relay1.secret_key),
        internal=True,
    )
    r2 = RelayInfo(
        id=relay2.relay_id,
        public_key=PublicKey.parse(relay2.public_key),
        secret_key=SecretKey.parse(relay2.secret_key),
        internal=True,
    )
    sr1 = RelayInfo(id=id1, public_key=pk1, secret_key=sk1, internal=True)
    sr2 = RelayInfo(id=id2, public_key=pk2, secret_key=sk2, internal=False)

    # add a project config for each relay key
    p1 = mini_sentry.add_basic_project_config(1)
    p2 = mini_sentry.add_basic_project_config(2)
    # add r2 to p2 since it is external
    p2["config"]["trustedRelays"].append(str(r2.public_key))
    p3 = mini_sentry.add_basic_project_config(3)
    p4 = mini_sentry.add_basic_project_config(4)
    # add sr2 to p4 (since it is external)
    p4["config"]["trustedRelays"].append(str(sr2.public_key))

    test_info = {"r1": r1, "r2": r2, "sr1": sr1, "sr2": sr2}

    proj_info = {
        "p1": p1,
        "p2": p2,
        "p3": p3,
        "p4": p4,
    }

    caller = test_info[caller]
    projects = [proj_info[proj] for proj in projects]

    public_keys = [str(p["publicKeys"][0]["publicKey"]) for p in projects]

    request = {"publicKeys": public_keys, "fullConfig": True}

    packed, signature = caller.secret_key.pack(request)

    data, _ = get_response(relay1, packed, signature, relay_id=caller.id)

    # test that it returns valid data
    for p in public_keys:
        assert data["configs"][p] is not None


def test_invalid_json(mini_sentry, relay):
    relay = relay(mini_sentry)

    body = {}  # missing the required `publicKeys` field
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    response = request_config(relay, packed, signature)

    assert response.status_code == 400  # Bad Request
    assert "JSON" in response.text


def test_invalid_signature(mini_sentry, relay):
    relay = relay(mini_sentry)

    response = relay.post(
        "/api/0/relays/projectconfigs/?version=3",
        data='{"publicKeys":[]}',
        headers={
            "X-Sentry-Relay-Id": relay.relay_id,
            "X-Sentry-Relay-Signature": "broken",
        },
    )

    assert response.status_code == 401  # Unauthorized
    assert "signature" in response.text


def test_broken_projectkey(mini_sentry, relay):
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)
    public_key = mini_sentry.get_dsn_public_key(42)

    body = {
        "publicKeys": [
            public_key,  # valid
            "deadbeef",  # wrong length
            42,  # wrong type
            "/?$äß000000000000000000000000000",  # invalid characters
        ]
    }
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    data, _ = get_response(relay, packed, signature)

    assert public_key in data["configs"]


def test_pending_projects(mini_sentry, relay):
    # V3 requests will never return a projectconfig on the first request, only some
    # subsequent request will contain the response.  However if the machine executing this
    # test is very slow this could still be a flaky test.
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)
    public_key = mini_sentry.get_dsn_public_key(42)

    body = {"publicKeys": [public_key]}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    response = request_config(relay, packed, signature)
    assert response.ok
    data = response.json()
    assert public_key in data["pending"]
    assert public_key not in data["configs"]

    data, _ = get_response(relay, packed, signature)
    assert public_key in data["configs"]
    assert data.get("pending") is None


def test_unparsable_project_config(mini_sentry, relay):
    project_key = 42
    relay_config = {
        "cache": {
            "project_expiry": 2,
            "project_grace_period": 20,
            "miss_expiry": 2,
        },
        "http": {
            "max_retry_interval": 1,
            "retry_delay": 0,
        },
    }

    relay = relay(mini_sentry, relay_config)
    mini_sentry.add_full_project_config(project_key)
    public_key = mini_sentry.get_dsn_public_key(project_key)

    # Config is broken and will produce the invalid project state.
    config = mini_sentry.project_configs[project_key]
    config["slug"] = 99  # ERROR: Number not allowed

    body = {"publicKeys": [public_key]}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    # This request should return invalid project state and also send the error to Sentry.
    data = request_config(relay, packed, signature, version="3").json()
    assert data == {"configs": {}, "pending": [public_key]}

    def assert_clear_test_failures():
        try:
            assert {str(e) for _, e in mini_sentry.current_test_failures()} == {
                f"Relay sent us event: error fetching project state {public_key}: invalid type: integer `99`, expected a string",
            }
        finally:
            mini_sentry.clear_test_failures()

    # Event is not propagated, relay logs an error:
    relay.send_event(project_key)
    assert mini_sentry.captured_events.empty()

    # Wait a bit before fixing the project config to make sure Relay had time to fetch the broken one.
    time.sleep(1)

    # Fix the config.
    config = mini_sentry.project_configs[project_key]
    config["slug"] = "some-slug"

    relay.send_event(project_key)
    relay.send_event(project_key)
    relay.send_event(project_key)

    # Wait for caches to expire. And we will get into the grace period.
    time.sleep(3)

    assert_clear_test_failures()

    # The state should be fixed and updated by now, since we keep re-trying to fetch new one all the time.
    data, _ = get_response(relay, packed, signature)
    assert data["configs"][public_key]["projectId"] == project_key
    assert not data["configs"][public_key]["disabled"]
    time.sleep(1)

    # We should have the previous events through now.
    for _ in range(4):
        event = mini_sentry.captured_events.get(timeout=1).get_event()
        assert event["logentry"] == {"formatted": "Hello, World!"}

    # This must succeed, since we will re-request the project state update at this point.
    relay.send_event(project_key)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
    assert mini_sentry.captured_events.empty()


def test_cached_project_config(mini_sentry, relay):
    mini_sentry.project_config_ignore_revision = True

    project_key = 42
    relay_config = {
        "cache": {"project_expiry": 2, "project_grace_period": 5, "miss_expiry": 2}
    }
    relay = relay(mini_sentry, relay_config)
    mini_sentry.add_full_project_config(project_key)
    public_key = mini_sentry.get_dsn_public_key(project_key)

    # Once the event is sent the project state is requested and cached.
    relay.send_event(project_key)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
    # send a second event
    relay.send_event(project_key)
    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}

    body = {"publicKeys": [public_key]}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)
    data, _ = get_response(relay, packed, signature)
    assert data["configs"][public_key]["projectId"] == project_key
    assert not data["configs"][public_key]["disabled"]

    # Introduce unparsable config.
    config = mini_sentry.project_configs[project_key]
    config["slug"] = 99  # ERROR: Number not allowed

    # Give it a bit time for update to go through.
    time.sleep(1)
    data, _ = get_response(relay, packed, signature)

    assert data["configs"][public_key]["projectId"] == project_key
    assert not data["configs"][public_key]["disabled"]

    # Wait till grace period expires as well and we should start buffering the events now.
    time.sleep(5)
    try:
        relay.send_event(project_key)
        time.sleep(0.5)
        assert {str(e) for _, e in mini_sentry.current_test_failures()} == {
            f"Relay sent us event: error fetching project state {public_key}: invalid type: integer `99`, expected a string",
        }
    finally:
        mini_sentry.clear_test_failures()


def test_get_global_config(mini_sentry, relay):
    project_key = 42
    relay_config = {
        "cache": {"project_expiry": 2, "project_grace_period": 5, "miss_expiry": 2}
    }

    mini_sentry.global_config = {
        "measurements": {
            "builtinMeasurements": [
                {"name": "app_start_cold", "unit": "millisecond"},
                {"name": "app_start_warm", "unit": "millisecond"},
            ],
            "maxCustomMeasurements": 10,
        }
    }
    relay = relay(mini_sentry, relay_config)
    mini_sentry.add_full_project_config(project_key)

    body = {"publicKeys": [], "global": True}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)
    data, _ = get_response(relay, packed, signature, version="3")

    global_extraction_config = data["global"].pop("metricExtraction")
    assert "span_metrics_tx" in global_extraction_config["groups"]

    assert data["global"] == mini_sentry.global_config


def test_compression(mini_sentry, relay):
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(42)
    public_key = mini_sentry.get_dsn_public_key(42)

    body = {"publicKeys": [public_key]}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    data, response = get_response(
        relay, packed, signature, headers={"Accept-Encoding": "gzip"}
    )
    assert response.headers["content-encoding"] == "gzip"
    assert public_key in data["configs"]


def test_unchanged_projects(mini_sentry, relay):
    relay = relay(mini_sentry)
    cfg = mini_sentry.add_basic_project_config(42)
    cfg["rev"] = "123"
    public_key = mini_sentry.get_dsn_public_key(42)

    body = {"publicKeys": [public_key], "revisions": ["123"]}
    packed, signature = SecretKey.parse(relay.secret_key).pack(body)

    response = request_config(relay, packed, signature)

    assert response.ok
    data = response.json()
    assert public_key in data["pending"]
    assert public_key not in data.get("unchanged", [])

    data, response = get_response(relay, packed, signature)

    assert public_key in data["unchanged"]
    assert public_key not in data["configs"]
    assert data.get("pending") is None
