import json
import pytest

CSP_IGNORED_FIELDS = (
    "event_id",
    "timestamp",
    "received",
)
EXPECT_CT_IGNORED_FIELDS = ("event_id",)
EXPECT_STAPLE_IGNORED_FIELDS = ("event_id",)
HPKP_IGNORED_FIELDS = ("event_id",)


def get_security_report(envelope):
    if envelope is not None:
        for item in envelope.items:
            if item.headers.get("type") == "security":
                return json.loads(item.get_bytes())


def id_fun1(origins):
    allowed, should_be_allowed = origins
    should_it = "" if should_be_allowed else "not"
    return f"{str(allowed)} should {should_it} be allowed "


@pytest.mark.parametrize(
    "allowed_origins", [(["valid.com"], True), (["invalid.com"], False)], ids=id_fun1,
)
def test_uses_origins(mini_sentry, relay, json_fixture_provider, allowed_origins):
    allowed_domains, should_be_allowed = allowed_origins
    fixture_provider = json_fixture_provider(__file__)
    proj_id = 42
    relay = relay(mini_sentry)
    report = fixture_provider.load("csp", ".input")
    mini_sentry.add_full_project_config(proj_id)

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json; charset=utf-8",
        payload=report,
        release="01d5c3165d9fbc5c8bdcf9550a1d6793a80fc02b",
        environment="production",
        origin="http://valid.com",
    )

    if should_be_allowed:
        mini_sentry.captured_events.get(timeout=1).get_event()
    assert mini_sentry.captured_events.empty()


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
    report = fixture_provider.load(test_name, ".input")
    mini_sentry.add_full_project_config(proj_id)
    events_consumer = events_consumer()

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json; charset=utf-8",
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
    report = fixture_provider.load(test_name, ".input")
    mini_sentry.add_full_project_config(proj_id)

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json",
        payload=report,
        release="01d5c3165d9fbc5c8bdcf9550a1d6793a80fc02b",
        environment="production",
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = get_security_report(envelope)
    for x in ignored_properties:
        event.pop(x, None)

    ext = ".no_processing.output"
    expected_evt = fixture_provider.load(test_name, ext)

    assert event == expected_evt
