"""PII scrubbing integration tests.

Note on selectors: normalization runs *before* scrubbing and derives fields from user input --
`user.sentry_user` is built from `user.email`/`user.id`, for example. A selector that names only the
source field (`$user.email`) leaves the derived copy untouched. That is pre-existing behaviour for
any redaction method, not something specific to `encrypt`, but it is worth knowing when configuring
rules: prefer `user.**` over `$user.email` if the goal is that a value appears nowhere.
"""

import base64
import json

import pytest


def test_scrub_span_sentry_tags_advanced_rules(mini_sentry, relay):
    project_id = 42
    relay = relay(
        mini_sentry,
        options={"geoip": {"path": "tests/fixtures/GeoIP2-Enterprise-Test.mmdb"}},
    )
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"]["applications"][
        "$span.sentry_tags.'user.geo.country_code'"
    ] = ["@anything:mask"]
    config["config"]["piiConfig"]["applications"][
        "$span.sentry_tags.'user.geo.subregion'"
    ] = ["@anything:mask"]

    relay.send_event(
        project_id,
        {
            "user": {"ip_address": "2.125.160.216"},
            "spans": [
                {
                    "timestamp": 1746007551,
                    "start_timestamp": 1746007545,
                    "span_id": "aaaaaaaa00000000",
                    "trace_id": "aaaaaaaaaaaaaaaaaaaa000000000000",
                    "sentry_tags": {
                        "user.geo.country_code": "AT",
                        "user.geo.subregion": "12",
                    },
                }
            ],
        },
    )

    event = mini_sentry.get_captured_envelope().get_event()
    assert event["spans"][0]["sentry_tags"]["user.geo.country_code"] == "**"
    assert event["spans"][0]["sentry_tags"]["user.geo.subregion"] == "***"


@pytest.mark.parametrize(
    "field,pii",
    [
        # SpanData field, pii: true in conventions
        ("url.full", "true"),
        # SpanData field, pii: maybe in conventions
        ("gen_ai.input.messages", "maybe"),
        # SpanData field, pii: false in conventions
        ("sentry.release", "false"),
        # SpanData field, missing in conventions, explicitly set on SpanData
        ("profile_id", "false"),
        # Not a SpanData field, pii: true in conventions
        ("ai.warnings", "true"),
        # Not a SpanData field, pii: maybe in conventions
        ("process.runtime.name", "maybe"),
        # Not a SpanData field, pii: false in conventions
        ("sentry.cancellation_reason", "false"),
        # Not a SpanData field and not defined in conventions
        ("madeup.field", "true"),
    ],
)
def test_spandata_conventions(mini_sentry, relay, field, pii):
    project_id = 42
    relay = relay(
        mini_sentry,
    )
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"].setdefault(
        "datascrubbingSettings",
        {
            "scrubData": True,
            "scrubDefaults": True,
        },
    )

    config["config"]["piiConfig"] = {
        "rules": {
            "custom_secret": {
                "type": "pattern",
                "pattern": ".*",
                "redaction": {"method": "replace", "text": "[REDACTED]"},
            },
        },
        "applications": {
            "$string": ["@anything:mask"],
            f"'{field}'": ["custom_secret"],
        },
    }

    relay.send_event(
        project_id,
        {
            "spans": [
                {
                    "timestamp": 1778131375.142457,
                    "start_timestamp": 1778131374.296492,
                    "exclusive_time": 845.965,
                    "data": {field: "secret value"},
                }
            ],
        },
    )

    event = mini_sentry.get_captured_envelope().get_event()

    value = event["spans"][0]["data"][field]

    # pii: true fields should get masked
    if pii == "true":
        assert value == "************"
    # pii: maybe fields should get redacted
    elif pii == "maybe":
        assert value == "[REDACTED]"
    # pii: false fields should be left alone
    else:
        assert value == "secret value"


@pytest.mark.parametrize(
    ["input_message", "expected_output"],
    [
        pytest.param(
            "User john.doe@company.com failed authentication",
            "User [email] failed authentication",
            id="email_scrubbing",
        ),
        pytest.param(
            "Payment failed for card 4111-1111-1111-1111",
            "Payment failed for card [creditcard]",
            id="credit_card_scrubbing",
        ),
        pytest.param(
            "User alice@test.com with used card 4111-1111-1111-1111",
            "User [email] with used card [creditcard]",
            id="mixed_pii_scrubbing",
        ),
        pytest.param(
            "Database connection failed to prod-db-01 at 10:30",
            "Database connection failed to prod-db-01 at 10:30",
            id="no_pii",
        ),
    ],
)
def test_logentry_formatted_smart_scrubbing(
    mini_sentry, relay, input_message, expected_output
):
    """Test various smart scrubbing scenarios in logentry.formatted"""
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": input_message},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    event = mini_sentry.get_captured_envelope().get_event()
    assert event["logentry"]["formatted"] == expected_output


def test_logentry_formatted_user_rules(mini_sentry, relay):
    """Test that user-configured PII rules apply to logentry.formatted"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"] = {
        "rules": {
            "custom_secret": {
                "type": "pattern",
                "pattern": r"SECRET_\w+",
                "redaction": {"method": "replace", "text": "[secret]"},
            }
        },
        "applications": {"$logentry.formatted": ["custom_secret"]},
    }

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": "Auth failed with SECRET_KEY_12345"},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    event = mini_sentry.get_captured_envelope().get_event()
    assert event["logentry"]["formatted"] == "Auth failed with [secret]"


def test_logentry_formatted_data_scrubbing_settings(
    mini_sentry,
    relay,
    non_destructive,
):
    """Test logentry.formatted scrubbing with various data scrubbing settings"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    non_destructive.install(config)

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": non_destructive.input_message},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    event = mini_sentry.get_captured_envelope().get_event()
    formatted_value = event["logentry"]["formatted"]
    assert formatted_value == non_destructive.expected_output

    if non_destructive.additional_checks:
        assert non_destructive.additional_checks(formatted_value)


def test_encrypt_pii_roundtrip(mini_sentry, relay):
    """Values matched by an `encrypt` rule are scrubbed from the event but recoverable
    with the private key, which only the org holds."""
    nacl_public = pytest.importorskip("nacl.public")

    secret = nacl_public.PrivateKey.generate()
    public_key = base64.b64encode(bytes(secret.public_key)).decode()

    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(42)
    config["config"]["piiConfig"] = {
        "vars": {"publicKey": public_key},
        "rules": {"recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}},
        "applications": {"user.**": ["recoverable"]},
    }

    relay.send_event(42, {"user": {"id": "42", "email": "bruno@example.com"}})

    event = mini_sentry.get_captured_envelope().get_event()

    # Scrubbed in the event body, and the untouched field is left alone.
    assert event["user"]["email"] == "[Encrypted]"
    assert "bruno@example.com" not in json.dumps(event)

    # Recoverable by whoever holds the private key.
    sealed = base64.b64decode(event["_encrypted_pii"])
    recovered = json.loads(nacl_public.SealedBox(secret).decrypt(sealed))
    assert recovered["user.email"] == "bruno@example.com"
    # `user.**` also covers the fields normalization derives before scrubbing runs, such as
    # `sentry_user`. Narrower selectors leave those behind -- see the `sentry_user` note in the
    # module docstring.
    assert recovered["user.id"] == "42"


def test_encrypt_pii_nondeterministic(mini_sentry, relay):
    """Two identical events must produce different ciphertext, so nothing downstream can
    tell that the same value occurred twice."""
    pytest.importorskip("nacl.public")
    import nacl.public

    secret = nacl.public.PrivateKey.generate()
    public_key = base64.b64encode(bytes(secret.public_key)).decode()

    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(42)
    config["config"]["piiConfig"] = {
        "vars": {"publicKey": public_key},
        "rules": {"recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}},
        "applications": {"user.**": ["recoverable"]},
    }

    payload = {"user": {"email": "bruno@example.com"}}
    relay.send_event(42, dict(payload))
    first = mini_sentry.get_captured_envelope().get_event()
    relay.send_event(42, dict(payload))
    second = mini_sentry.get_captured_envelope().get_event()

    assert first["_encrypted_pii"] != second["_encrypted_pii"]


def test_encrypt_pii_without_key_still_scrubs(mini_sentry, relay):
    """With no public key configured the value must still be destroyed, and no payload
    attached. Failing open here would be the worst outcome."""
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(42)
    config["config"]["piiConfig"] = {
        "rules": {"recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}},
        "applications": {"user.**": ["recoverable"]},
    }

    relay.send_event(42, {"user": {"email": "bruno@example.com"}})

    event = mini_sentry.get_captured_envelope().get_event()
    assert event["user"]["email"] == "[Encrypted]"
    assert "bruno@example.com" not in json.dumps(event)
    assert "_encrypted_pii" not in event
