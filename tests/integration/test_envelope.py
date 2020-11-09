import json

from datetime import datetime

from sentry_sdk.envelope import Envelope, PayloadRef, Item
from sentry_sdk.utils import format_timestamp


def test_envelope(mini_sentry, relay_chain):
    relay = relay_chain()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    envelope = Envelope()
    envelope.add_event({"message": "Hello, World!"})
    relay.send_envelope(project_id, envelope)

    event = mini_sentry.captured_events.get(timeout=1).get_event()

    assert event["logentry"] == {"formatted": "Hello, World!"}


def generate_transaction_item():
    return {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
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

    # set up relay

    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    events_consumer = transactions_consumer()

    # construct envelope

    transaction_item = generate_transaction_item()

    transaction_item.update(
        {
            "measurements": {
                "LCP": {"value": 420.69},
                "   lcp_final.element-Size123  ": {"value": 1},
                "fid": {"value": 2020},
                "cls": {"value": None},
                "fp": {"value": "im a first paint"},
                "Total Blocking Time": {"value": 3.14159},
                "missing_value": "string",
            }
        }
    )

    envelope = Envelope()
    envelope.add_transaction(transaction_item)

    # ingest envelope

    relay.send_envelope(42, envelope)

    event, _ = events_consumer.try_get_event()

    # test actual output

    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "measurements" in event, event
    assert event["measurements"] == {
        "lcp": {"value": 420.69},
        "lcp_final.element-size123": {"value": 1},
        "fid": {"value": 2020},
        "cls": {"value": None},
        "fp": {"value": None},
        "missing_value": None,
    }


def test_empty_measurement_interface(mini_sentry, relay_chain):

    # set up relay

    relay = relay_chain()
    mini_sentry.add_basic_project_config(42)

    # construct envelope

    transaction_item = generate_transaction_item()

    transaction_item.update({"measurements": {}})

    envelope = Envelope()
    envelope.add_transaction(transaction_item)

    # ingest envelope

    relay.send_envelope(42, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)

    event = envelope.get_transaction_event()

    # test actual output

    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "measurements" not in event, event


def test_strip_measurement_interface(
    mini_sentry, relay_with_processing, events_consumer
):

    # set up relay

    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    events_consumer = events_consumer()

    # construct envelope

    envelope = Envelope()
    envelope.add_event(
        {
            "message": "Hello, World!",
            "measurements": {
                "LCP": {"value": 420.69},
                "fid": {"value": 2020},
                "cls": {"value": None},
            },
        }
    )

    # ingest envelope

    relay.send_envelope(42, envelope)

    event, _ = events_consumer.try_get_event()

    # test actual output

    assert event["logentry"] == {"formatted": "Hello, World!"}

    # expect measurements interface object to be stripped out since it's attached to a non-transaction event
    assert "measurements" not in event, event
