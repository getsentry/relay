import uuid

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def test_event_with_span_link_in_transaction(relay, mini_sentry):
    """Assert that span links in the trace context and child spans are parsed and have the expected output."""

    mini_sentry.add_full_project_config(42)
    relay = relay(mini_sentry)

    event_id = uuid.uuid1().hex
    error_payload = {
        "event_id": event_id,
        "transaction": "/users",
        "contexts": {
            "trace": {
                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                "span_id": "fa90fdead5f74052",
                "op": "navigation",
                "links": [
                    {
                        "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "ea90fdead5f74052",
                        "sampled": True,
                        "attributes": {"sentry.link.type": "previous_trace"},
                    }
                ],
            }
        },
        "spans": [
            {
                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                "span_id": "ea90fdead5f74052",
                "parent_span_id": "fa90fdead5f74052",
                "op": "http.client",
                "description": "GET /api/users",
                "status": "ok",
                "start_timestamp": 1624366926.0,
                "timestamp": 1624366927.0,
                "links": [
                    {
                        "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "ea90fdead5f74052",
                        "sampled": False,
                        "attributes": {
                            "stringVal": "bar",
                            "numberVal": 42,
                            "boolVal": True,
                        },
                    },
                    {
                        "trace_id": "2c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "da90fdead5f74052",
                    },
                ],
            },
        ],
        "type": "transaction",
        "environment": "production",
        "release": "foo@1.2.3",
    }

    envelope = Envelope(headers={"event_id": event_id})
    envelope.add_item(Item(PayloadRef(json=error_payload), type="event"))

    relay.send_envelope(42, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope
    assert envelope.items[0].payload.json["contexts"] == {
        "trace": {
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "fa90fdead5f74052",
            "op": "navigation",
            "links": [
                {
                    "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "ea90fdead5f74052",
                    "sampled": True,
                    "attributes": {"sentry.link.type": "previous_trace"},
                }
            ],
            "status": "unknown",
            "type": "trace",
        }
    }

    assert len(envelope.items[0].payload.json["spans"]) == 1
    assert envelope.items[0].payload.json["spans"] == [
        {
            "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
            "span_id": "ea90fdead5f74052",
            "parent_span_id": "fa90fdead5f74052",
            "op": "http.client",
            "description": "GET /api/users",
            "status": "ok",
            "start_timestamp": 1624366926.0,
            "timestamp": 1624366927.0,
            "links": [
                {
                    "trace_id": "3c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "ea90fdead5f74052",
                    "sampled": False,
                    "attributes": {
                        "stringVal": "bar",
                        "numberVal": 42,
                        "boolVal": True,
                    },
                },
                {
                    "trace_id": "2c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "da90fdead5f74052",
                },
            ],
        },
    ]

    assert mini_sentry.captured_events.empty()
