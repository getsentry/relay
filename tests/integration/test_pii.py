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

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["spans"][0]["sentry_tags"]["user.geo.country_code"] == "**"
    assert event["spans"][0]["sentry_tags"]["user.geo.subregion"] == "**"


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

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

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

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    assert event["logentry"]["formatted"] == "Auth failed with [secret]"


@pytest.mark.parametrize(
    [
        "pii_config",
        "scrub_data",
        "scrub_defaults",
        "input_message",
        "expected_output",
        "additional_checks",
    ],
    [
        pytest.param(
            None,
            True,
            True,
            "API request failed with Bearer ABC123XYZ789TOKEN and returned 401",
            "API request failed with Bearer [token] and returned 401",
            lambda formatted_value: (
                "Bearer [token]" in formatted_value
                and "ABC123XYZ789TOKEN" not in formatted_value
                and "API request failed" in formatted_value
                and "returned 401" in formatted_value
            ),
            id="bearer_token_scrubbing",
        ),
        pytest.param(
            {},
            False,
            True,
            "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            None,
            id="no_scrubbing_when_disabled",
        ),
        pytest.param(
            {},
            True,
            True,
            "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111",
            "API failed with Bearer [token] for [email] using card [creditcard]",
            None,
            id="all_scrubbing_when_enabled",
        ),
        pytest.param(
            None,
            True,
            True,
            "User's password is 12345",
            "User's password is 12345",
            None,
            id="password_not_scrubbed",
        ),
    ],
)
def test_logentry_formatted_data_scrubbing_settings(
    mini_sentry,
    relay,
    pii_config,
    scrub_data,
    scrub_defaults,
    input_message,
    expected_output,
    additional_checks,
):
    """Test logentry.formatted scrubbing with various data scrubbing settings"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"] = pii_config
    config["config"]["datascrubbingSettings"] = {
        "scrubData": scrub_data,
        "scrubDefaults": scrub_defaults,
    }

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": input_message},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    formatted_value = event["logentry"]["formatted"]

    assert formatted_value == expected_output

    if additional_checks:
        assert additional_checks(formatted_value)
