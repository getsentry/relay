"""
Tests the publickeys endpoint (/api/0/relays/publickeys/)
"""
import uuid
import pytest
from collections import namedtuple

from sentry_relay import PublicKey, SecretKey, generate_key_pair

RelayInfo = namedtuple("RelayInfo", ["id", "public_key", "secret_key", "internal"])


@pytest.mark.parametrize(
    "caller, relays_to_fetch",
    [
        ("r2", ["r1", "r2"]),
        ("r2", ["sr1", "sr2"]),
        ("sr1", ["r1", "r2"]),
        ("sr1", ["sr1", "sr2"]),
        ("sr1", ["r1", "r2", "sr1", "sr2"]),
        ("sr2", ["r1", "r2", "sr1", "sr2"]),
        ("r2", ["r1", "r2", "sr1", "sr2"]),
    ],
    ids=[
        "dyn relay fetches dyn relay info",
        "dyn relay fetches static relay info",
        "static relay fetches dyn relay info",
        "static relay fetches static relay info",
        "static relay fetches mixed relay info",
        "static external relay fetches mixed relay info",
        "dyn relay fetches mixed relay info",
    ],
)
def test_public_keys(mini_sentry, relay, caller, relays_to_fetch):
    """
    Tests the public key endpoint with dynamic and statically configured relays

    Create 2 normal relays r1 & r2 ( that will register normally to upstream)
    Create the configuration for 2 static relays sr1,sr2 (and add this config to r1)

    Send to r1 various requests for r2,sr1,sr2 from various relays (r2,sr1,sr2)
    """

    # create info for 2 statically configured relay
    sk1, pk1 = generate_key_pair()
    id1 = str(uuid.uuid4())
    sk2, pk2 = generate_key_pair()
    id2 = str(uuid.uuid4())

    # create configuration containing the static relays
    relays_conf = {
        "relays": {
            id1: {"publicKey": str(pk1), "internal": True},
            id2: {"publicKey": str(pk2), "internal": False},
        }
    }

    # create 2 normal relays
    relay1 = relay(mini_sentry, wait_healthcheck=True, static_relays_config=relays_conf)
    relay2 = relay(mini_sentry, wait_healthcheck=True, static_relays_config=relays_conf)

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

    test_info = {"r1": r1, "r2": r2, "sr1": sr1, "sr2": sr2}

    caller = test_info[caller]

    relays_to_fetch = [test_info[r] for r in relays_to_fetch]

    request = {"relay_ids": [relay_info.id for relay_info in relays_to_fetch]}

    packed, signature = caller.secret_key.pack(request)

    resp = relay1.post(
        "/api/0/relays/publickeys/",
        data=packed,
        headers={"X-Sentry-Relay-Id": caller.id, "X-Sentry-Relay-Signature": signature},
    )
    assert resp.ok

    expected = {
        "relays": {
            ri.id: {"publicKey": str(ri.public_key), "internal": ri.internal}
            for ri in relays_to_fetch
        }
    }
    assert resp.json() == expected
