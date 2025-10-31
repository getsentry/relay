import datetime
import json
from pathlib import Path
from time import sleep

import pytest

from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef


RELAY_ROOT = Path(__file__).parent.parent.parent


@pytest.mark.parametrize(
    "is_processing_relay", (False, True), ids=["non_processing", "processing"]
)
@pytest.mark.parametrize(
    "filter_config, should_filter",
    [
        ({"errorMessages": {"patterns": ["Panic: originalCreateNotification"]}}, True),
        ({"errorMessages": {"patterns": ["Warning"]}}, False),
    ],
    ids=[
        "error messages filtered",
        "error messages not filtered",
    ],
)
def test_filters_are_applied(
    mini_sentry,
    relay_with_processing,
    relay,
    events_consumer,
    is_processing_relay,
    filter_config,
    should_filter,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """
    events_consumer = events_consumer()

    upstream = relay_with_processing()
    if is_processing_relay:
        relay = upstream
    else:
        relay = relay(upstream)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    for key in filter_config.keys():
        filter_settings[key] = filter_config[key]

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.now(datetime.UTC)
    message_text = f"some message {now.isoformat()}"

    event = {
        "message": message_text,
        "exception": {
            "values": [{"type": "Panic", "value": "originalCreateNotification"}]
        },
    }

    relay.send_event(project_id, event)

    if should_filter:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()


@pytest.mark.parametrize(
    "is_processing_relay", (False, True), ids=["external relay", "processing relay"]
)
@pytest.mark.parametrize(
    "event, must_filter",
    (
        ({"logentry": {"message": "filter:"}}, False),
        ({"logentry": {"message": "filter:yes"}}, True),
        ({"logentry": {"message": "filter:%s", "params": ["no"]}}, False),
        ({"logentry": {"message": "filter:%s", "params": ["yes"]}}, True),
    ),
    ids=[
        "plain non-matching message",
        "plain matching message",
        "formatting non-matching message",
        "formatting matching message",
    ],
)
def test_error_message_filters_are_applied(
    mini_sentry,
    events_consumer,
    relay,
    relay_with_processing,
    is_processing_relay,
    event,
    must_filter,
):
    events_consumer = events_consumer()
    processing = relay_with_processing()
    if is_processing_relay:
        relay = processing
    else:
        relay = relay(processing)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["errorMessages"] = {
        "patterns": ["*yes*"]
    }

    relay.send_event(project_id, event)
    sleep(1)

    if must_filter:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()
        events_consumer.assert_empty()


@pytest.mark.parametrize(
    "is_processing_relay", (False, True), ids=["external relay", "processing relay"]
)
@pytest.mark.parametrize(
    "enable_filters",
    (False, True),
    ids=["events from extensions not filtered", "events from extensions filtered"],
)
def test_browser_extension_filters_are_applied(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay,
    is_processing_relay,
    enable_filters,
):
    """Test if all processing relays apply browser extension filters when enabled."""
    events_consumer = events_consumer()
    processing = relay_with_processing()
    if is_processing_relay:
        relay = processing
    else:
        relay = relay(processing)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["filterSettings"]["browserExtensions"] = {
        "isEnabled": enable_filters
    }

    event = {
        "exception": {
            "values": [
                {
                    "stacktrace": {
                        "frames": [
                            {"filename": "a/different.file"},
                            {"filename": "chrome-extension://blablabla"},
                        ]
                    }
                }
            ]
        }
    }
    relay.send_event(project_id, event)

    if enable_filters:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()


@pytest.mark.parametrize(
    "is_enabled, should_filter",
    [
        (True, True),
        (False, False),
    ],
    ids=[
        "web crawlers filtered",
        "web crawlers not filtered",
    ],
)
def test_web_crawlers_filter_are_applied(
    mini_sentry,
    relay_with_processing,
    events_consumer,
    is_enabled,
    should_filter,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """
    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["webCrawlers"] = {"isEnabled": is_enabled}

    # UA parsing introduces higher latency in debug mode
    events_consumer = events_consumer(timeout=10)

    # create a unique message so we can make sure we don't test with stale data
    now = datetime.datetime.now(datetime.UTC)
    message_text = f"some message {now.isoformat()}"

    event = {
        "message": message_text,
        "request": {
            "headers": {
                "User-Agent": "BingBot",
            }
        },
    }

    relay.send_event(project_id, event)

    if should_filter:
        events_consumer.assert_empty()
    else:
        events_consumer.get_event()


@pytest.mark.parametrize(
    "is_enabled, transaction_name, should_filter",
    [
        (True, "health-check-1 ", True),
        (False, "health-check-2", False),
        (True, "some-transaction-3", False),
    ],
    ids=[
        "when enabled ignore transactions are filtered",
        "when disabled ignore transactions are NOT filtered",
        "when enabled leaves alone transactions that are not enabled",
    ],
)
def test_ignore_transactions_filters_are_applied(
    mini_sentry,
    relay_with_processing,
    transactions_consumer,
    is_enabled,
    transaction_name,
    should_filter,
):
    """
    Tests the ignore transactions filter
    """
    relay = relay_with_processing()
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    if is_enabled:
        filter_settings["ignoreTransactions"] = {
            "patterns": ["health*"],
            "isEnabled": is_enabled,
        }
    else:
        filter_settings["ignoreTransactions"] = {
            "patterns": [],
            "isEnabled": is_enabled,
        }

    transactions_consumer = transactions_consumer(timeout=10)

    now = datetime.datetime.now(datetime.UTC)
    start_timestamp = (now - datetime.timedelta(minutes=1)).timestamp()
    timestamp = now.timestamp()

    transaction = {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": transaction_name,
        "start_timestamp": start_timestamp,
        "timestamp": timestamp,
        "contexts": {
            "trace": {
                "trace_id": "1234F60C11214EB38604F4AE0781BFB2",
                "span_id": "ABCDFDEAD5F74052",
                "type": "trace",
            }
        },
    }

    relay.send_transaction(project_id, transaction)

    if should_filter:
        transactions_consumer.assert_empty()
    else:
        event, _ = transactions_consumer.get_event()
        assert event["transaction"] == transaction_name


def test_client_ip_filters_are_applied(
    mini_sentry,
    relay,
):
    """
    Test that relay normalizes messages when processing is enabled and sends them via Kafka queues
    """
    relay = relay(mini_sentry)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["clientIps"] = {"blacklistedIps": ["1.2.3.0/24"]}

    event = {"message": "foo"}
    relay.send_event(project_id, event, headers={"X-Forwarded-For": "1.2.3.4"})

    report = mini_sentry.get_client_report()
    assert report["filtered_events"] == [
        {"reason": "ip-address", "category": "error", "quantity": 1}
    ]

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "localhost:3000"},
        {"Host": "127.0.0.1:3000"},
        {"Host": "localhost"},
        {"X-Forwarded-Host": "localhost:3000"},
        {"X-Forwarded-Host": "127.0.0.1:3000"},
        {"X-Forwarded-Host": "localhost"},
    ],
)
def test_localhost_filter_with_headers(mini_sentry, relay, headers):
    """
    Tests filtering against headers if the user does not exist.
    """
    relay = relay(mini_sentry)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)

    # We need to enable this otherwise the user will always have a localhost address inferred in tests
    project_config["config"].setdefault("datascrubbingSettings", {})[
        "scrubIpAddresses"
    ] = True
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["localhost"] = {"isEnabled": True}

    event = {"user": None, "request": {"headers": headers}}
    relay.send_event(project_id, event)

    report = mini_sentry.get_client_report()
    assert report["filtered_events"] == [
        {"reason": "localhost", "category": "error", "quantity": 1}
    ]

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "localhost:3000"},
        {"Host": "127.0.0.1:3000"},
        {"Host": "localhost"},
        {"X-Forwarded-Host": "localhost:3000"},
        {"X-Forwarded-Host": "127.0.0.1:3000"},
        {"X-Forwarded-Host": "localhost"},
    ],
)
def test_localhost_filter_user_ip_resolved(mini_sentry, relay, headers):
    relay = relay(mini_sentry)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["localhost"] = {"isEnabled": True}

    event = {"user": "{{auto}}", "request": {"headers": headers}}
    relay.send_event(project_id, event, headers={"X-Forwarded-For": "81.41.165.209"})

    report = mini_sentry.get_client_report()
    assert report["filtered_events"] == [
        {"reason": "localhost", "category": "error", "quantity": 1}
    ]

    assert mini_sentry.captured_events.empty()


@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "example.com"},
        {"Host": "localhost.example.com:3000"},
        {"X-Forwarded-Host": "localhost.example.com:3000"},
    ],
)
def test_localhost_filter_no_local_header_ips(mini_sentry, relay, headers):
    """
    Tests against header values that are not local values and the user does not exist.
    """
    relay = relay(mini_sentry)

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    # We need to enable this otherwise the user will always have a localhost address inferred in tests
    project_config["config"].setdefault("datascrubbingSettings", {})[
        "scrubIpAddresses"
    ] = True

    filter_settings = project_config["config"]["filterSettings"]
    filter_settings["localhost"] = {"isEnabled": True}

    event = {"user": {"ip_address": None}, "request": {"headers": headers}}
    relay.send_event(project_id, event)

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event is not None


def test_global_filters_drop_events(
    mini_sentry, relay_with_processing, events_consumer, outcomes_consumer
):
    events_consumer = events_consumer()
    outcomes_consumer = outcomes_consumer()

    mini_sentry.global_config["filters"] = {
        "version": 1,
        "filters": [
            {
                "id": "premature-releases",
                "isEnabled": True,
                "condition": {
                    "op": "eq",
                    "name": "event.release",
                    "value": "0.0.0",
                },
            }
        ],
    }

    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay_with_processing()

    event = {"release": "0.0.0"}
    relay.send_event(project_id, event)

    events_consumer.assert_empty()
    outcomes = outcomes_consumer.get_outcomes()
    assert len(outcomes) == 1
    assert outcomes[0]["reason"] == "premature-releases"


def profile_transaction_item():
    now = datetime.datetime.now(datetime.UTC)
    transaction = {
        "type": "transaction",
        "timestamp": now.isoformat(),
        "start_timestamp": (now - datetime.timedelta(seconds=2)).isoformat(),
        "spans": [
            {
                "op": "default",
                "span_id": "968cff94913ebb07",
                "segment_id": "968cff94913ebb07",
                "start_timestamp": now.timestamp(),
                "timestamp": now.timestamp() + 1,
                "exclusive_time": 1000.0,
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
            },
        ],
        "contexts": {
            "trace": {
                "op": "hi",
                "trace_id": "a0fa8803753e40fd8124b21eeb2986b5",
                "span_id": "968cff94913ebb07",
            }
        },
        "transaction": "hi",
    }
    transaction = json.dumps(transaction).encode()
    return Item(payload=PayloadRef(bytes=transaction), type="transaction")


def sample_profile_v1_envelope(release):
    envelope = Envelope()

    transaction_item = profile_transaction_item()
    envelope.add_item(transaction_item)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v1/valid.json", "r"
    ) as f:
        profile = json.loads(f.read())
        profile["release"] = release
    profile_item = Item(
        payload=PayloadRef(bytes=json.dumps(profile).encode()), type="profile"
    )
    envelope.add_item(profile_item)

    return envelope


def sample_profile_v2_envelope(release):
    envelope = Envelope()

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/sample/v2/valid.json", "r"
    ) as f:
        profile = json.loads(f.read())
        profile["release"] = release
    item = Item(
        payload=PayloadRef(bytes=json.dumps(profile).encode()), type="profile_chunk"
    )
    envelope.add_item(item)

    return envelope


def android_profile_legacy_envelope(release):
    envelope = Envelope()

    transaction_item = profile_transaction_item()
    envelope.add_item(transaction_item)

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/android/legacy/valid.json", "r"
    ) as f:
        profile = json.loads(f.read())
        profile["release"] = release
    item = Item(payload=PayloadRef(bytes=json.dumps(profile).encode()), type="profile")
    envelope.add_item(item)

    return envelope


def android_profile_chunk_envelope(release):
    envelope = Envelope()

    with open(
        RELAY_ROOT / "relay-profiling/tests/fixtures/android/chunk/valid.json", "r"
    ) as f:
        profile = json.loads(f.read())
        profile["release"] = release
    profile_item = Item(
        payload=PayloadRef(bytes=json.dumps(profile).encode()), type="profile_chunk"
    )
    envelope.add_item(profile_item)

    return envelope


@pytest.mark.parametrize(
    ["envelope", "data_category"],
    [
        pytest.param(sample_profile_v1_envelope, DataCategory.PROFILE, id="profile v1"),
        pytest.param(
            sample_profile_v2_envelope, DataCategory.PROFILE_CHUNK_UI, id="profile v2"
        ),
        pytest.param(
            android_profile_legacy_envelope, DataCategory.PROFILE, id="android legacy"
        ),
        pytest.param(
            android_profile_chunk_envelope,
            DataCategory.PROFILE_CHUNK_UI,
            id="android chunk",
        ),
    ],
)
@pytest.mark.parametrize(
    ["filter_config", "should_filter"],
    [
        pytest.param({}, False, id="profile accepted"),
        pytest.param(
            {"releases": {"releases": ["foobar@1.0"]}}, True, id="profile filtered"
        ),
    ],
)
def test_filters_are_applied_to_profiles(
    mini_sentry,
    relay_with_processing,
    outcomes_consumer,
    profiles_consumer,
    filter_config,
    should_filter,
    envelope,
    data_category,
):
    outcomes_consumer = outcomes_consumer()
    profiles_consumer = profiles_consumer()

    relay = relay_with_processing()

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("features", []).extend(
        [
            "organizations:profiling",
            "organizations:continuous-profiling",
        ]
    )
    filter_settings = project_config["config"]["filterSettings"]
    for key in filter_config.keys():
        filter_settings[key] = filter_config[key]

    envelope = envelope("foobar@1.0")
    relay.send_envelope(project_id, envelope)

    if should_filter:
        outcomes = []
        for outcome in outcomes_consumer.get_outcomes():
            if outcome["category"] == data_category:
                outcome.pop("timestamp")
                outcomes.append(outcome)

        assert outcomes == [
            {
                "category": data_category,
                "org_id": 1,
                "project_id": 42,
                "key_id": 123,
                "outcome": 1,  # Filtered
                "reason": "release-version",
                "quantity": 1,
            },
        ]
        profiles_consumer.assert_empty()
    else:
        profile, _ = profiles_consumer.get_profile()
        profile = json.loads(profile["payload"])
        assert profile["release"] == "foobar@1.0"
        assert [
            outcome
            for outcome in outcomes_consumer.get_outcomes()
            if outcome["category"] == data_category
        ] == []
