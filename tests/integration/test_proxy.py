""" Tests for proxy and static mode. """

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from sentry_sdk.envelope import Envelope, Item, PayloadRef

from .test_dynamic_sampling import get_profile_payload


def test_span_allowed(mini_sentry, relay):
    relay = relay(mini_sentry, options={"relay": {"mode": "proxy"}})

    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(
                bytes=json.dumps(
                    {
                        "description": r"test \" with \" escaped \" chars",
                        "op": "default",
                        "span_id": "cd429c44b67a3eb1",
                        "segment_id": "968cff94913ebb07",
                        "start_timestamp": start.timestamp(),
                        "timestamp": end.timestamp() + 1,
                        "exclusive_time": 345.0,  # The SDK knows that this span has a lower exclusive time
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                    },
                ).encode()
            ),
        )
    )
    relay.send_envelope(42, envelope)

    # Does not raise queue.Empty
    envelope = mini_sentry.captured_events.get(timeout=10)


def test_profile_allowed(mini_sentry, relay):
    relay = relay(mini_sentry, options={"relay": {"mode": "proxy"}})

    end = datetime.now(timezone.utc) - timedelta(seconds=1)
    duration = timedelta(milliseconds=500)
    start = end - duration
    envelope = Envelope()
    transaction = {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "foo",
        "start_timestamp": start.timestamp(),
        "timestamp": end.timestamp(),
        "contexts": {
            "trace": {
                "trace_id": "1234F60C11214EB38604F4AE0781BFB2",
                "span_id": "ABCDFDEAD5F74052",
                "type": "trace",
            }
        },
    }
    envelope.add_item(
        Item(
            payload=PayloadRef(json.dumps(transaction).encode()),
            type="transaction",
        )
    )
    envelope.add_item(
        Item(
            payload=PayloadRef(json.dumps(get_profile_payload(transaction)).encode()),
            type="profile",
        )
    )
    relay.send_envelope(42, envelope)

    # Does not raise queue.Empty
    envelope = mini_sentry.captured_events.get(timeout=10)
    assert {item.type for item in envelope.items} == {"transaction", "profile"}


def test_replay_allowed(mini_sentry, relay):
    relay = relay(mini_sentry, options={"relay": {"mode": "proxy"}})

    envelope = Envelope()
    envelope.add_item(
        Item(
            type="replay_event",
            payload=PayloadRef(
                bytes=open(
                    Path(__file__).parent.parent / "fixtures" / "replay.json", "rb"
                ).read(),
            ),
        )
    )
    relay.send_envelope(42, envelope)

    # Does not raise queue.Empty
    envelope = mini_sentry.captured_events.get(timeout=10)
