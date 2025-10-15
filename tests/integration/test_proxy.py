"""Tests for proxy and static mode."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from time import sleep

from sentry_sdk.envelope import Envelope, Item, PayloadRef

import pytest
from .test_dynamic_sampling import get_profile_payload
import queue
from requests.exceptions import HTTPError

TRANSACTION = {
    "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
    "type": "transaction",
    "transaction": "foo",
    "start_timestamp": 0,
    "timestamp": 0,
    "contexts": {
        "trace": {
            "trace_id": "1234F60C11214EB38604F4AE0781BFB2",
            "span_id": "ABCDFDEAD5F74052",
            "type": "trace",
        }
    },
}

PAYLOADS = {
    "event": {
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "message": "test",
        "extra": {"msg_text": "test"},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    },
    "transaction": TRANSACTION,
    "attachment": b"file content",
    "user_report": {
        "name": "Josh",
        "email": "",
        "comments": "I'm having fun",
        "event_id": "4cec9f3e1f214073b816e0f4de5f59b1",
    },
    "session": {
        "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
        "did": "foobarbaz",
        "seq": 42,
        "init": True,
        "timestamp": 0,
        "started": datetime.now(timezone.utc).isoformat(),
        "duration": 1947.49,
        "status": "exited",
        "errors": 0,
        "attrs": {
            "release": "sentry-test@1.0.0",
            "environment": "production",
        },
    },
    "sessions": {
        "aggregates": [
            {
                "started": datetime.now(timezone.utc).isoformat(),
                "did": "foobarbaz",
                "exited": 2,
                "errored": 3,
            },
        ],
        "attrs": {
            "release": "sentry-test@1.0.0",
            "environment": "production",
        },
    },
    "statsd": "transactions/foo:42|c",
    "client_report": {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "discarded_events": [],
        "rate_limited_events": [
            {"reason": "generic", "category": "error", "quantity": 1}
        ],
    },
    "profile": get_profile_payload(TRANSACTION),
    "replay_event": open(
        Path(__file__).parent.parent / "fixtures" / "replay.json", "rb"
    ).read(),
    "replay_recording": b"{}\n[]",
    "replay_video": b"hello, world!",
    "check_in": {
        "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
        "monitor_slug": "my-monitor",
        "status": "in_progress",
        "duration": 21.0,
    },
    "log": {
        "items": [
            {
                "message": "test log",
                "level": "info",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ]
    },
    "span": {
        "description": r"test \" with \" escaped \" chars",
        "op": "default",
        "span_id": "cd429c44b67a3eb1",
        "segment_id": "968cff94913ebb07",
        "start_timestamp": datetime.now(timezone.utc).isoformat(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "exclusive_time": 345.0,
        "trace_id": "ff62a8b040f340bda5d830223def1d81",
    },
    "otel_span": {
        "traceId": "89143b0763095bd9c9955e8175d1fb23",
        "spanId": "a342abb1214ca181",
        "name": "my 1st OTel span",
        "startTimeUnixNano": "1000000000",
        "endTimeUnixNano": "2000000000",
        "attributes": [
            {"key": "sentry.category", "value": {"stringValue": "db"}},
        ],
    },
    "profile_chunk": open(
        Path(__file__).parent.parent.parent
        / "relay-profiling/tests/fixtures/sample/v2/valid.json",
        "rb",
    ).read(),
}

BINARY_ITEMS = {
    "replay_event",
    "replay_recording",
    "replay_video",
    "attachment",
    "profile_chunk",
}


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


@pytest.mark.parametrize("item_type", PAYLOADS.keys())
@pytest.mark.parametrize("rate_limited", [True, False])
def test_passthrough(relay, mini_sentry, item_type, rate_limited):
    if rate_limited:
        # Logic taken from: test_store.py::test_store_rate_limit
        store_event_original = mini_sentry.app.view_functions["store_event"]
        rate_limit_sent = False

        @mini_sentry.app.endpoint("store_event")
        def store_event():
            nonlocal rate_limit_sent
            if rate_limit_sent:
                return store_event_original()
            else:
                rate_limit_sent = True
                return "", 429, {"retry-after": "20"}

    config = {
        "outcomes": {"emit_outcomes": "as_client_reports"},
        "relay": {"mode": "proxy"},
    }
    relay = relay(mini_sentry, config)

    payload = PAYLOADS[item_type]
    envelope = Envelope()
    envelope.add_item(
        Item(
            type=item_type,
            payload=PayloadRef(
                payload if item_type in BINARY_ITEMS else json.dumps(payload).encode()
            ),
        )
    )
    relay.send_envelope(42, envelope)

    if rate_limited:
        with pytest.raises(queue.Empty):
            mini_sentry.captured_events.get(timeout=1)

        sleep(1)
        # Not entirely sure why they are special
        if item_type not in {"client_report", "profile_chunk"}:
            with pytest.raises(HTTPError):
                relay.send_envelope(42, envelope)

            if item_type not in {"session", "sessions", "statsd"}:
                outcome_envelope = mini_sentry.captured_events.get(timeout=2)
                outcome = json.loads(outcome_envelope.items[0].payload.bytes)
                assert len(outcome["rate_limited_events"]) == 1

    else:
        captured_envelope = mini_sentry.captured_events.get(timeout=10)
        assert len(captured_envelope.items) == 1

        if item_type in BINARY_ITEMS:
            assert payload == captured_envelope.items[0].payload.get_bytes()
        else:
            assert payload == json.loads(captured_envelope.items[0].payload.get_bytes())
