import pytest
from os import path
import json


def load_json_fixture(file_name):
    file_path = path.abspath(
        path.join(__file__, "..", "fixtures/security_report", file_name)
    )

    if not path.isfile(file_path):
        return None

    with open(file_path, "rb") as f:
        return json.load(f)


def save_test_fixture(obj, file_name):
    file_path = path.abspath(
        path.join(__file__, "..", "fixtures/security_report", file_name)
    )

    if path.isfile(file_path):
        print(
            "trying to override existing fixture.\n If fixture is out of date delete manually.",
            file_path,
        )
        raise ValueError("Will not override ", file_path)

    with open(file_path, "w") as f:
        print(obj)
        return json.dump(obj, f)


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
def test_security_reports_with_processing(
    mini_sentry, relay_with_processing, events_consumer, test_case
):
    test_name, ignored_properties = test_case
    proj_id = 42
    relay = relay_with_processing()
    relay.wait_relay_healthcheck()
    report = load_json_fixture(test_name + ".input.json")
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

    fixture_name = test_name + ".normalized.output.json"
    expected_evt = load_json_fixture(fixture_name)

    if expected_evt is not None:
        # compare with fixture
        assert event == expected_evt
    else:
        # first run create fixture
        save_test_fixture(event, fixture_name)


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
def test_security_reports_no_processing(mini_sentry, relay, test_case):
    test_name, ignored_properties = test_case
    proj_id = 42
    relay = relay(mini_sentry)
    relay.wait_relay_healthcheck()
    report = load_json_fixture(test_name + ".input.json")
    mini_sentry.project_configs[proj_id] = mini_sentry.full_project_config()

    relay.send_security_report(
        project_id=proj_id,
        content_type="application/json",
        payload=report,
        release="01d5c3165d9fbc5c8bdcf9550a1d6793a80fc02b",
        environment="production",
    )

    event = mini_sentry.captured_events.get(timeout=1)
    for x in ignored_properties:
        event.pop(x, None)

    fixture_name = test_name + ".no_processing.output.json"
    expected_evt = load_json_fixture(fixture_name)

    if expected_evt is not None:
        # compare with fixture
        assert event == expected_evt
    else:
        # first run create fixture
        save_test_fixture(event, fixture_name)
