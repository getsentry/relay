"""
Tests the project_configs endpoint (/api/0/relays/projectconfigs/)
"""

import uuid
import pytest
from collections import namedtuple

from sentry_relay import PublicKey, SecretKey, generate_key_pair

RelayInfo = namedtuple("RelayInfo", ["id", "public_key", "secret_key", "internal"])


@pytest.mark.parametrize(
    "caller, projects",
    [("r2", ["p2"]), ("sr1", ["p1", "p2", "p3", "p4"]), ("sr2", ["p4"]),],
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
            id1: {"publicKey": str(pk1), "internal": True},
            id2: {"publicKey": str(pk2), "internal": False},
    }

    relay1 = relay(mini_sentry, wait_healthcheck=True, static_relays=relays_conf)
    relay2 = relay(
        mini_sentry, wait_healthcheck=True, external=True, static_relays=relays_conf,
    )

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

    resp = relay1.post(
        "/api/0/relays/projectconfigs/?version=2",
        data=packed,
        headers={"X-Sentry-Relay-Id": caller.id, "X-Sentry-Relay-Signature": signature},
    )

    assert resp.ok

    # test that it returns valid data
    data = resp.json()
    for p in public_keys:
        assert data["configs"][p] is not None
