import pytest

CSP_IGNORED_FIELDS = (
    "event_id",
    "timestamp",
    "received",
    "_relay_processed",
)
EXPECT_CT_IGNORED_FIELDS = ("event_id",)
EXPECT_STAPLE_IGNORED_FIELDS = ("event_id",)
HPKP_IGNORED_FIELDS = ("event_id",)


@pytest.mark.parametrize("test_case", [("csp", CSP_IGNORED_FIELDS),], ids=("csp",))
def test_security_report_with_processing(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    test_case,
    json_fixture_provider,
):
    fixture_provider = json_fixture_provider(__file__)
    test_name, ignored_properties = test_case
    proj_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    report = fixture_provider.load(test_name, ".input")
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()
    events_consumer = events_consumer()

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json",
        payload=report,
        release="01d5c3165d9fbc5c8bdcf9550a1d6793a80fc02b",
        environment="production",
    )

    event, _ = events_consumer.get_event()
    for x in ignored_properties:
        event.pop(x, None)

    ext = ".normalized.output"
    expected_evt = fixture_provider.load(test_name, ext)

    event.pop("_metrics", None)
    assert event == expected_evt


@pytest.mark.parametrize(
    "test_case",
    [
        ("csp", CSP_IGNORED_FIELDS),
        ("csp_chrome", CSP_IGNORED_FIELDS),
        ("csp_chrome_blocked_asset", CSP_IGNORED_FIELDS),
        ("csp_firefox_blocked_asset", CSP_IGNORED_FIELDS),
        ("expect_ct", EXPECT_CT_IGNORED_FIELDS),
        ("expect_staple", EXPECT_STAPLE_IGNORED_FIELDS),
        ("hpkp", HPKP_IGNORED_FIELDS),
    ],
    ids=(
        "csp",
        "csp_chrome",
        "csp_chrome_blocked_asset",
        "csp_firefox_blocked_asset",
        "expect_ct",
        "expect_staple",
        "hpkp",
    ),
)
def test_security_report(mini_sentry, relay, test_case, json_fixture_provider):
    fixture_provider = json_fixture_provider(__file__)
    test_name, ignored_properties = test_case
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    report = fixture_provider.load(test_name, ".input")
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json",
        payload=report,
        release="01d5c3165d9fbc5c8bdcf9550a1d6793a80fc02b",
        environment="production",
    )

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    for x in ignored_properties:
        event.pop(x, None)

    ext = ".no_processing.output"
    expected_evt = fixture_provider.load(test_name, ext)

    assert event == expected_evt
