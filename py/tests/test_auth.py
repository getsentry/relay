import uuid
import sentry_relay
import pytest


UPSTREAM_SECRET = "secret"

RELAY_ID = b"6b7d15b8-cee2-4354-9fee-dae7ef43e434"
RELAY_KEY = b"kMpGbydHZSvohzeMlghcWwHd8MkreKGzl_ncdkZSOMg"
REQUEST = b'{"relay_id":"6b7d15b8-cee2-4354-9fee-dae7ef43e434","public_key":"kMpGbydHZSvohzeMlghcWwHd8MkreKGzl_ncdkZSOMg","version":"20.8.0"}'
REQUEST_SIG = "JIwzIb3kuOaVwgq_DRuPpquGVIIu0plfpOSvz_ixzfw_RmdyHr35cJrna7Jg_uXqNHQbSP1Yj0-4X5Omk9jcBA.eyJ0IjoiMjAyMC0wOS0wMVQxMzozNzoxNC40Nzk0NjVaIn0"
TOKEN = "eyJ0aW1lc3RhbXAiOjE1OTg5Njc0MzQsInJlbGF5X2lkIjoiNmI3ZDE1YjgtY2VlMi00MzU0LTlmZWUtZGFlN2VmNDNlNDM0IiwicHVibGljX2tleSI6ImtNcEdieWRIWlN2b2h6ZU1sZ2hjV3dIZDhNa3JlS0d6bF9uY2RrWlNPTWciLCJyYW5kIjoiLUViNG9Hal80dUZYOUNRRzFBVmdqTjRmdGxaNU9DSFlNOFl2d1podmlyVXhUY0tFSWYtQzhHaldsZmgwQTNlMzYxWE01dVh0RHhvN00tbWhZeXpWUWcifQ:KJUDXlwvibKNQmex-_Cu1U0FArlmoDkyqP7bYIDGrLXudfjGfCjH-UjNsUHWVDnbM28YdQ-R2MBSyF51aRLQcw"
RESPONSE = b'{"relay_id":"6b7d15b8-cee2-4354-9fee-dae7ef43e434","token":"eyJ0aW1lc3RhbXAiOjE1OTg5Njc0MzQsInJlbGF5X2lkIjoiNmI3ZDE1YjgtY2VlMi00MzU0LTlmZWUtZGFlN2VmNDNlNDM0IiwicHVibGljX2tleSI6ImtNcEdieWRIWlN2b2h6ZU1sZ2hjV3dIZDhNa3JlS0d6bF9uY2RrWlNPTWciLCJyYW5kIjoiLUViNG9Hal80dUZYOUNRRzFBVmdqTjRmdGxaNU9DSFlNOFl2d1podmlyVXhUY0tFSWYtQzhHaldsZmgwQTNlMzYxWE01dVh0RHhvN00tbWhZeXpWUWcifQ:KJUDXlwvibKNQmex-_Cu1U0FArlmoDkyqP7bYIDGrLXudfjGfCjH-UjNsUHWVDnbM28YdQ-R2MBSyF51aRLQcw"}'
RESPONSE_SIG = "HUp3eybT_5AmRJ_QzutfvStKTeE-cgD_reLPjIf4OpoOJT_Hln8ThrFqGyT_C6P8qF1LHbFLcrYFvQy4iNaqAQ.eyJ0IjoiMjAyMC0wOS0wMVQxMzozNzoxNC40ODEwNTNaIn0"


def test_basic_key_functions():
    sk, pk = sentry_relay.generate_key_pair()
    signature = sk.sign(b"some secret data")
    assert pk.verify(b"some secret data", signature)
    assert not pk.verify(b"some other data", signature)

    packed, signature = sk.pack({"foo": "bar"})
    pk.unpack(packed, signature)
    with pytest.raises(sentry_relay.UnpackErrorBadSignature):
        pk.unpack(b"haha", signature)


def test_challenge_response():
    resp = sentry_relay.create_register_challenge(
        REQUEST, REQUEST_SIG, UPSTREAM_SECRET, max_age=0,
    )
    assert resp["relay_id"] == uuid.UUID(RELAY_ID.decode("utf8"))
    assert len(resp["token"]) > 40
    assert ":" in resp["token"]


def test_challenge_response_validation_errors():
    with pytest.raises(sentry_relay.UnpackErrorSignatureExpired):
        sentry_relay.create_register_challenge(
            REQUEST, REQUEST_SIG, UPSTREAM_SECRET, max_age=1,
        )
    with pytest.raises(sentry_relay.UnpackErrorBadPayload):
        sentry_relay.create_register_challenge(
            REQUEST + b"__broken", REQUEST_SIG, UPSTREAM_SECRET, max_age=0,
        )
    with pytest.raises(sentry_relay.UnpackErrorBadSignature):
        sentry_relay.create_register_challenge(
            REQUEST, REQUEST_SIG + "__broken", UPSTREAM_SECRET, max_age=0,
        )


def test_register_response():
    resp = sentry_relay.validate_register_response(
        RESPONSE, RESPONSE_SIG, UPSTREAM_SECRET, max_age=0,
    )
    assert resp["token"] == TOKEN
    assert resp["relay_id"] == uuid.UUID(RELAY_ID.decode("utf8"))


def test_is_version_supported():
    assert sentry_relay.is_version_supported("99.99.99")

    # These can be updated when deprecating legacy versions:
    assert sentry_relay.is_version_supported("")
    assert sentry_relay.is_version_supported(None)
