import pytest

from requests.exceptions import HTTPError
from sentry_sdk.envelope import Envelope


def generate_replay_event():
    return {
        "replay_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "seq_id": 0,
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
    }


def test_replay_event(mini_sentry, relay_with_processing, replay_events_consumer):
    relay = relay_with_processing()
    mini_sentry.add_basic_project_config(
        42, extra={"config": {"features": ["organizations:session-replay"]}}
    )

    replay_events_consumer = replay_events_consumer()

    replay_item = generate_replay_event()

    relay.send_replay_event(42, replay_item)

    replay_event, _ = replay_events_consumer.get_replay_event()
    assert (
        replay_event["transaction"] == "/organizations/:orgId/performance/:eventSlug/"
    )
    assert "trace" in replay_event["contexts"]
    assert replay_event["seq_id"] == 0
