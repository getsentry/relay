import pytest
import queue

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_envelope(mini_sentry, relay_chain):
    relay = relay_chain()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    envelope = Envelope()
    envelope.add_event({"message": "Hello, World!"})
    relay.send_envelope(project_id, envelope)

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_envelope_empty(mini_sentry, relay):
    relay = relay(mini_sentry)
    PROJECT_ID = 42
    mini_sentry.add_basic_project_config(PROJECT_ID)

    envelope = Envelope()
    relay.send_envelope(PROJECT_ID, envelope)

    # there is nothing sent to the upstream
    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)


def test_envelope_without_header(mini_sentry, relay):
    relay = relay(mini_sentry)
    PROJECT_ID = 42
    mini_sentry.add_basic_project_config(PROJECT_ID)

    envelope = Envelope(headers={"dsn": relay.get_dsn(PROJECT_ID)})
    envelope.add_event({"message": "Hello, World!"})
    relay.send_envelope(
        PROJECT_ID,
        envelope,
        headers={"X-Sentry-Auth": ""},  # Empty auth header is ignored by Relay
    )

    event = mini_sentry.captured_events.get(timeout=1).get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}


def test_unknown_item(mini_sentry, relay):
    relay = relay(mini_sentry)
    PROJECT_ID = 42
    mini_sentry.add_basic_project_config(PROJECT_ID)

    envelope = Envelope()
    envelope.add_item(
        Item(payload=PayloadRef(bytes=b"something"), type="invalid_unknown")
    )
    relay.send_envelope(PROJECT_ID, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert len(envelope.items) == 1
    assert envelope.items[0].type == "invalid_unknown"


def test_drop_unknown_item(mini_sentry, relay):
    relay = relay(mini_sentry, {"routing": {"accept_unknown_items": False}})
    PROJECT_ID = 42
    mini_sentry.add_basic_project_config(PROJECT_ID)

    envelope = Envelope()
    envelope.add_item(
        Item(payload=PayloadRef(bytes=b"something"), type="invalid_unknown")
    )
    relay.send_envelope(PROJECT_ID, envelope)

    # there is nothing sent to the upstream
    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)


def generate_transaction_item():
    return {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
        "transaction_info": {"source": "route"},
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [
            {
                "description": "<OrganizationContext>",
                "op": "react.mount",
                "parent_span_id": "8f5a2b8768cafb4e",
                "span_id": "bd429c44b67a3eb4",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
            }
        ],
    }


def test_normalize_measurement_interface(
    mini_sentry, relay_with_processing, transactions_consumer
):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    events_consumer = transactions_consumer()

    transaction_item = generate_transaction_item()
    transaction_item.update(
        {
            "measurements": {
                "LCP": {"value": 420.69},
                "   lcp_final.element-Size123  ": {"value": 1},
                "fid": {"value": 2020},
                "inp": {"value": 100.14},
                "cls": {"value": None},
                "fp": {"value": "im a first paint"},
                "Total Blocking Time": {"value": 3.14159},
                "missing_value": "string",
            }
        }
    )

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "measurements" in event, event
    assert event["measurements"] == {
        "cls": {"unit": "none", "value": None},
        "fid": {"unit": "millisecond", "value": 2020.0},
        "inp": {"unit": "millisecond", "value": 100.14},
        "fp": {"unit": "millisecond", "value": None},
        "lcp": {"unit": "millisecond", "value": 420.69},
        "lcp_final.element-size123": {"unit": "none", "value": 1.0},
        "missing_value": None,
    }


def test_empty_measurement_interface(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="20.10.0")
    mini_sentry.add_basic_project_config(42)

    transaction_item = generate_transaction_item()
    transaction_item.update({"measurements": {}})

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_transaction_event()

    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "measurements" not in event, event


def test_strip_measurement_interface(
    mini_sentry, relay_with_processing, events_consumer
):
    events_consumer = events_consumer()

    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    envelope = Envelope()
    envelope.add_event(
        {
            "message": "Hello, World!",
            "measurements": {
                "LCP": {"value": 420.69},
                "fid": {"value": 2020},
                "inp": {"value": 100.14},
                "cls": {"value": None},
            },
        }
    )
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["logentry"] == {"formatted": "Hello, World!"}
    # expect measurements interface object to be stripped out since it's attached to a non-transaction event
    assert "measurements" not in event, event


def test_ops_breakdowns(mini_sentry, relay_with_processing, transactions_consumer):
    events_consumer = transactions_consumer()

    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(42)

    config["config"].setdefault(
        "breakdownsV2",
        {
            "span_ops": {
                "type": "span_operations",
                "matches": ["http", "db", "browser", "resource"],
            },
            "span_ops_2": {
                "type": "spanOperations",
                "matches": ["http", "db", "browser", "resource"],
            },
            "span_ops_3": {
                "type": "whatever",
                "matches": ["http", "db", "browser", "resource"],
            },
        },
    )

    config["config"].setdefault(
        # old name of span operation breakdown key. we expect these to not generate anything at all
        "breakdowns",
        {
            "span_ops_4": {
                "type": "span_operations",
                "matches": ["http", "db", "browser", "resource"],
            },
            "span_ops_5": {
                "type": "spanOperations",
                "matches": ["http", "db", "browser", "resource"],
            },
            "span_ops_6": {
                "type": "whatever",
                "matches": ["http", "db", "browser", "resource"],
            },
        },
    )

    transaction_item = generate_transaction_item()
    transaction_item.update(
        {
            "spans": [
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb4",
                    "start_timestamp": 1000.5,
                    "timestamp": 2000.5,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb5",
                    "start_timestamp": 1500.5,
                    "timestamp": 2500.5,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http.secure",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb6",
                    "start_timestamp": 3500,
                    "timestamp": 4000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "sentry-cdn.com/dist.js",
                    "op": "resource.link",
                    "parent_span_id": "8f5a2b8768cafb4f",
                    "span_id": "bd429c44b67a3eb7",
                    "start_timestamp": 500.001,
                    "timestamp": 600.002003,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "sentry.middleware.env.SentryEnvMiddleware.process_request",
                    "op": "django.middleware",
                    "parent_span_id": "8f5a2b8768cafb4e",
                    "span_id": "bd429c44b67a3eb8",
                    "start_timestamp": 100,
                    "timestamp": 200,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
            ],
        }
    )

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "breakdowns" in event, event
    assert event["breakdowns"] == {
        "span_ops": {
            "ops.http": {"value": 2000000.0, "unit": "millisecond"},
            "ops.resource": {"value": 100001.003, "unit": "millisecond"},
            "total.time": {"value": 2200001.003, "unit": "millisecond"},
        },
        "span_ops_2": {
            "ops.http": {"value": 2000000.0, "unit": "millisecond"},
            "ops.resource": {"value": 100001.003, "unit": "millisecond"},
            "total.time": {"value": 2200001.003, "unit": "millisecond"},
        },
    }


def test_no_span_attributes(mini_sentry, relay_with_processing, transactions_consumer):
    events_consumer = transactions_consumer()

    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(42)

    if "spanAttributes" in config["config"]:
        del config["config"]["spanAttributes"]

    transaction_item = generate_transaction_item()
    transaction_item.update(
        {
            "spans": [
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "aaaaaaaaaaaaaaaa",
                    "span_id": "bbbbbbbbbbbbbbbb",
                    "start_timestamp": 1000,
                    "timestamp": 3000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "bbbbbbbbbbbbbbbb",
                    "span_id": "cccccccccccccccc",
                    "start_timestamp": 1400,
                    "timestamp": 2600,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "cccccccccccccccc",
                    "span_id": "dddddddddddddddd",
                    "start_timestamp": 1700,
                    "timestamp": 2300,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
            ],
        }
    )

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "exclusive_time" not in event["contexts"]["trace"]
    for span in event["spans"]:
        assert "exclusive_time" not in span


def test_empty_span_attributes(
    mini_sentry, relay_with_processing, transactions_consumer
):
    events_consumer = transactions_consumer()

    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(42)

    config["config"].setdefault("spanAttributes", [])

    transaction_item = generate_transaction_item()
    transaction_item.update(
        {
            "spans": [
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "aaaaaaaaaaaaaaaa",
                    "span_id": "bbbbbbbbbbbbbbbb",
                    "start_timestamp": 1000,
                    "timestamp": 3000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "bbbbbbbbbbbbbbbb",
                    "span_id": "cccccccccccccccc",
                    "start_timestamp": 1400,
                    "timestamp": 2600,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "cccccccccccccccc",
                    "span_id": "dddddddddddddddd",
                    "start_timestamp": 1700,
                    "timestamp": 2300,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
            ],
        }
    )

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "exclusive_time" not in event["contexts"]["trace"]
    for span in event["spans"]:
        assert "exclusive_time" not in span


def test_span_attributes_exclusive_time(
    mini_sentry, relay_with_processing, transactions_consumer
):
    events_consumer = transactions_consumer()

    relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(42)

    config["config"].setdefault("spanAttributes", ["exclusive-time"])

    transaction_item = generate_transaction_item()
    transaction_item.update(
        {
            "start_timestamp": 0,
            "timestamp": 4000,
            "spans": [
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "aaaaaaaaaaaaaaaa",
                    "span_id": "bbbbbbbbbbbbbbbb",
                    "start_timestamp": 1000,
                    "timestamp": 3000,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "bbbbbbbbbbbbbbbb",
                    "span_id": "cccccccccccccccc",
                    "start_timestamp": 1400,
                    "timestamp": 2600,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
                {
                    "description": "GET /api/0/organizations/?member=1",
                    "op": "http",
                    "parent_span_id": "cccccccccccccccc",
                    "span_id": "dddddddddddddddd",
                    "start_timestamp": 1700,
                    "timestamp": 2300,
                    "trace_id": "ff62a8b040f340bda5d830223def1d81",
                },
            ],
        }
    )
    transaction_item["contexts"]["trace"].update({"span_id": "aaaaaaaaaaaaaaaa"})

    envelope = Envelope()
    envelope.add_transaction(transaction_item)
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert event["contexts"]["trace"]["exclusive_time"] == 2000000
    assert [span["exclusive_time"] for span in event["spans"]] == [
        800000,
        600000,
        600000,
    ]


def test_sample_rates(mini_sentry, relay_chain):
    relay = relay_chain(min_relay_version="21.1.0")
    mini_sentry.add_basic_project_config(42)

    sample_rates = [
        {"id": "client_sampler", "rate": 0.01},
        {"id": "dyanmic_user", "rate": 0.5},
    ]

    envelope = Envelope()
    envelope.add_event({"message": "hello, world!"})
    envelope.items[0].headers["sample_rates"] = sample_rates
    relay.send_envelope(42, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope.items[0].headers["sample_rates"] == sample_rates


def test_sample_rates_metrics(mini_sentry, relay_with_processing, events_consumer):
    events_consumer = events_consumer()

    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    sample_rates = [
        {"id": "client_sampler", "rate": 0.01},
        {"id": "dyanmic_user", "rate": 0.5},
    ]

    envelope = Envelope()
    envelope.add_event({"message": "hello, world!"})
    envelope.items[0].headers["sample_rates"] = sample_rates
    relay.send_envelope(42, envelope)

    event, _ = events_consumer.get_event()
    assert event["_metrics"]["sample_rates"] == sample_rates
