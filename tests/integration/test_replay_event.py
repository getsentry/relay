import pytest

from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope


def generate_replay_event():
    return {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "replay_event",
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
        "breadcrumbs": [
            {
                "timestamp": 1597976410.6189718,
                "category": "console",
                "data": {
                    "arguments": ["%c first log", "more logs etc etc.", "test test",],
                    "logger": "console",
                },
                "level": "log",
                "message": "%c prev state color: #9E9E9E; font-weight: bold [object Object]",
            }
        ],
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
        "measurements": {
            "LCP": {"value": 420.69},
            "   lcp_final.element-Size123  ": {"value": 1},
            "fid": {"value": 2020},
            "cls": {"value": None},
            "fp": {"value": "im a first paint"},
            "Total Blocking Time": {"value": 3.14159},
            "missing_value": "string",
        },
    }


def test_replay_event(mini_sentry, relay_with_processing, replay_events_consumer):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(42)

    replay_events_consumer = replay_events_consumer()

    replay_item = generate_replay_event()

    relay.send_replay_event(42, replay_item)

    event, _ = replay_events_consumer.get_replay_event()
    assert event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    assert "trace" in event["contexts"]
    assert "measurements" in event, event
    assert "spans" in event, event
    assert event["measurements"] == {
        "lcp": {"value": 420.69},
        "lcp_final.element-size123": {"value": 1},
        "fid": {"value": 2020},
        "cls": {"value": None},
        "fp": {"value": None},
        "missing_value": None,
    }
    assert "breadcrumbs" in event
