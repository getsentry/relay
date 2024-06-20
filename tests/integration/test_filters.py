import datetime
from time import sleep
import pytest


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
