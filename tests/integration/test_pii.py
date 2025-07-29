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


def test_logentry_formatted_smart_scrubbing_email(mini_sentry, relay):
    """Test that logentry.formatted gets smart email scrubbing"""
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "User john.doe@company.com failed authentication"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    # Should scrub entire email for security
    assert event["logentry"]["formatted"] == "User [email] failed authentication"


def test_logentry_formatted_smart_scrubbing_credit_card(mini_sentry, relay):
    """Test credit card scrubbing in logentry.formatted"""
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": "Payment failed for card 4111-1111-1111-1111"},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    assert event["logentry"]["formatted"] == "Payment failed for card [creditcard]"


def test_logentry_formatted_mixed_pii(mini_sentry, relay):
    """Test multiple PII types in same message"""
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "User alice@test.com with used card 4111-1111-1111-1111"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    # Smart scrubbing applies to all patterns: email, SSN, and credit card
    expected = "User [email] with used card [creditcard]"
    assert event["logentry"]["formatted"] == expected


def test_logentry_formatted_no_pii(mini_sentry, relay):
    """Test that messages without PII are unchanged"""
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "Database connection failed to prod-db-01 at 10:30"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    # Should remain unchanged
    assert (
        event["logentry"]["formatted"]
        == "Database connection failed to prod-db-01 at 10:30"
    )


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


def test_logentry_formatted_bearer_token_scrubbing(mini_sentry, relay):
    """Test that Bearer tokens are properly scrubbed in logentry.formatted"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"] = None
    config["config"]["datascrubbingSettings"] = {
        "scrubData": True,
        "scrubDefaults": True,
    }

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "API request failed with Bearer ABC123XYZ789TOKEN and returned 401"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()

    # Should scrub Bearer token but preserve "Bearer" prefix and context
    formatted_value = event["logentry"]["formatted"]
    assert "Bearer [token]" in formatted_value
    assert "ABC123XYZ789TOKEN" not in formatted_value

    # Should preserve context
    assert "API request failed" in formatted_value
    assert "returned 401" in formatted_value


def test_logentry_formatted_no_scrubbing_when_disabled(mini_sentry, relay):
    """Test that logentry.formatted is not scrubbed when data scrubbing is disabled"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"] = {}
    config["config"]["datascrubbingSettings"] = {
        "scrubData": False,
        "scrubDefaults": True,
    }

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    # Should remain unchanged when data scrubbing is disabled.
    formatted_value = event["logentry"]["formatted"]
    assert (
        formatted_value
        == "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111"
    )


def test_logentry_formatted_scrubbing_enabled(mini_sentry, relay):
    """Test that logentry.formatted is scrubbed when data scrubbing is enabled"""
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"] = {}

    # Enable default and scrubing, we should scrub the logentry as well.
    config["config"]["datascrubbingSettings"] = {
        "scrubData": True,
        "scrubDefaults": True,
    }

    relay.send_event(
        project_id,
        {
            "logentry": {
                "formatted": "API failed with Bearer ABC123TOKEN for user@example.com using card 4111-1111-1111-1111"
            },
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    formatted_value = event["logentry"]["formatted"]
    assert (
        formatted_value
        == "API failed with Bearer [token] for [email] using card [creditcard]"
    )


def test_logentry_formatted_password_not_scrubbed(mini_sentry, relay):
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["datascrubbingSettings"] = {
        "scrubData": True,
        "scrubDefaults": True,
    }

    relay.send_event(
        project_id,
        {
            "logentry": {"formatted": "User's password is 12345"},
            "timestamp": "2024-01-01T00:00:00Z",
        },
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    formatted_value = event["logentry"]["formatted"]
    assert formatted_value == "User's password is 12345"
