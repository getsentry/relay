import uuid
import sentry_relay
import pytest


UPSTREAM_SECRET = "secret"

# NOTE in order to regenerate the test data (in case of changes) run the rust test:
# test_generate_strings_for_test_auth_py and copy its output below
RELAY_ID = b"29308cac-9783-40e9-98ac-b5503dffe3a4"
RELAY_KEY = b"dXq9IiKDLgma0J8dLVITOdkpaU8mPZPJj18t4HCKTfs"
REQUEST = b'{"relay_id":"29308cac-9783-40e9-98ac-b5503dffe3a4","public_key":"dXq9IiKDLgma0J8dLVITOdkpaU8mPZPJj18t4HCKTfs","version":"20.8.0"}'
REQUEST_SIG = "VgFn-7B5JmbSPiM5bikxkn7DjImV8LkfW3UVQcXnK8nLumvLaS7ML0KTY7a7LlU_3grGtSNZlEUbBudOp__RDA.eyJ0IjoiMjAyMC0wOS0wOFQxMzozMzozNS45OTM5MDRaIn0"
TOKEN = "eyJ0aW1lc3RhbXAiOjE1OTk1NzIwMTUsInJlbGF5X2lkIjoiMjkzMDhjYWMtOTc4My00MGU5LTk4YWMtYjU1MDNkZmZlM2E0IiwicHVibGljX2tleSI6ImRYcTlJaUtETGdtYTBKOGRMVklUT2RrcGFVOG1QWlBKajE4dDRIQ0tUZnMiLCJyYW5kIjoiQURNNG9yLVZNZ0Y1eTRLQUo2cHkyQnB5T3lmUmV1NGRjZTJCdmd5UHlSdnczRXFaUmc4SkE0NHdxVWdBVlBQMGhIeHR0am81YTdBYW93UEFVaUR2NUEifQ:XXYCNWscmAiBWzI84ToZWAGgmIrupWQufYcSBhIEcxiDxyBp_BRO0d_LN9wnc0tjtFcT9JViLoGCgfOt6vDS7A"
RESPONSE = b'{"relay_id":"29308cac-9783-40e9-98ac-b5503dffe3a4","token":"eyJ0aW1lc3RhbXAiOjE1OTk1NzIwMTUsInJlbGF5X2lkIjoiMjkzMDhjYWMtOTc4My00MGU5LTk4YWMtYjU1MDNkZmZlM2E0IiwicHVibGljX2tleSI6ImRYcTlJaUtETGdtYTBKOGRMVklUT2RrcGFVOG1QWlBKajE4dDRIQ0tUZnMiLCJyYW5kIjoiQURNNG9yLVZNZ0Y1eTRLQUo2cHkyQnB5T3lmUmV1NGRjZTJCdmd5UHlSdnczRXFaUmc4SkE0NHdxVWdBVlBQMGhIeHR0am81YTdBYW93UEFVaUR2NUEifQ:XXYCNWscmAiBWzI84ToZWAGgmIrupWQufYcSBhIEcxiDxyBp_BRO0d_LN9wnc0tjtFcT9JViLoGCgfOt6vDS7A","version":"20.8.0"}'
RESPONSE_SIG = "iPFV5KcSXDrhjY_99X8r_pMB1NQdw-YWF7hjvdrYpXmsaSier-mp1-3viWsEPIcTNbA76B4t51sjbSYFZPzXBg.eyJ0IjoiMjAyMC0wOS0wOFQxMzozMzozNS45OTU2ODJaIn0"
RELAY_VERSION = "20.8.0"


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
    assert resp["version"] == RELAY_VERSION


def test_is_version_supported():
    assert sentry_relay.is_version_supported("99.99.99")

    # These can be updated when deprecating legacy versions:
    assert sentry_relay.is_version_supported("")
    assert sentry_relay.is_version_supported(None)
