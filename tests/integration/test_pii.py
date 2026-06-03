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
