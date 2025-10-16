"""Tests for proxy and static mode."""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any

from sentry_sdk.envelope import Envelope, Item, PayloadRef

import pytest
from .test_dynamic_sampling import get_profile_payload
import queue
from requests.exceptions import HTTPError
from dataclasses import dataclass
from random import randbytes


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


# Need to monkey patch this here since the original does json parsing which interferes with us
# sending random binary data.
@pytest.fixture(autouse=True, scope="module")
def patch_sentry_envelope_for_proxy_tests():
    original = Item.deserialize_from

    @classmethod
    def patched(cls, f):
        line = f.readline().rstrip()
        if not line:
            return None
        headers = json.loads(line)
        length = headers.get("length")
        if length is not None:
            payload = f.read(length)
            f.readline()
        else:
            payload = f.readline().rstrip(b"\n")

        # Store all payloads as raw bytes without JSON parsing
        return cls(headers=headers, payload=PayloadRef(bytes=payload))

    Item.deserialize_from = patched
    yield
    Item.deserialize_from = original  # Restore after module tests complete


@dataclass
class RateLimitBehavior:
    item_type: str
    # None if sending an envelope with the item does not get a 429
    expected_outcomes: list[dict[str, Any]] | None
    # True if the envelope should get a 429 but also should still be forwarded
    expected_forward: bool = False


ITEM_TYPE_RATE_LIMIT_BEHAVIORS = [
    RateLimitBehavior("bogus_type", None),
    RateLimitBehavior("client_report", None),
    RateLimitBehavior("form_data", None),
    RateLimitBehavior("nel", None),
    RateLimitBehavior("profile_chunk", None),
    RateLimitBehavior("statsd", [], True),
    RateLimitBehavior("metric_buckets", [], True),
    RateLimitBehavior("sessions", [], True),
    RateLimitBehavior("session", []),
    RateLimitBehavior(
        "event", [{"reason": "generic", "category": "error", "quantity": 1}]
    ),
    RateLimitBehavior(
        "transaction",
        [
            {"reason": "generic", "category": "transaction_indexed", "quantity": 1},
            {"reason": "generic", "category": "transaction", "quantity": 1},
        ],
    ),
    RateLimitBehavior(
        "security",
        [{"reason": "generic", "category": "security", "quantity": 1}],
    ),
    RateLimitBehavior(
        "attachment",
        [{"category": "attachment", "quantity": 100, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "raw_security",
        [{"category": "security", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "unreal_report",
        [{"category": "error", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "user_report",
        [{"category": "user_report_v2", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "feedback",
        [{"category": "user_report_v2", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "profile",
        [
            {"category": "profile_indexed", "quantity": 1, "reason": "generic"},
            {"category": "profile", "quantity": 1, "reason": "generic"},
        ],
    ),
    RateLimitBehavior(
        "replay_event",
        [{"category": "replay", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "replay_recording",
        [{"category": "replay", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "replay_video",
        [{"category": "replay", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "check_in",
        [{"category": "monitor", "quantity": 1, "reason": "generic"}],
    ),
    RateLimitBehavior(
        "log",
        [
            {"category": "log_byte", "quantity": 100, "reason": "generic"},
            {"category": "log_item", "quantity": 1, "reason": "generic"},
        ],
    ),
    RateLimitBehavior(
        "span",
        [
            {"category": "span_indexed", "quantity": 1, "reason": "generic"},
            {"category": "span", "quantity": 1, "reason": "generic"},
        ],
    ),
    RateLimitBehavior(
        "otel_span",
        [
            {"category": "span_indexed", "quantity": 1, "reason": "generic"},
            {"category": "span", "quantity": 1, "reason": "generic"},
        ],
    ),
    RateLimitBehavior(
        "otel_traces_data",
        [
            {"category": "span", "quantity": 1, "reason": "generic"},
            {"category": "span_indexed", "quantity": 1, "reason": "generic"},
        ],
    ),
    RateLimitBehavior(
        "otel_logs_data",
        [
            {"category": "log_item", "quantity": 1, "reason": "generic"},
            {"category": "log_byte", "quantity": 100, "reason": "generic"},
        ],
    ),
]


@pytest.mark.parametrize(
    "behavior", ITEM_TYPE_RATE_LIMIT_BEHAVIORS, ids=lambda b: b.item_type
)
def test_proxy_rate_limit_passthrough(relay, mini_sentry, behavior):
    store_event_original = mini_sentry.app.view_functions["store_event"]

    @mini_sentry.app.endpoint("store_event")
    def store_event():
        store_event_original()
        return "", 429, {"retry-after": "5"}

    config = {
        "outcomes": {"emit_outcomes": "as_client_reports"},
        "relay": {"mode": "proxy"},
    }
    relay = relay(mini_sentry, config)
    project_id = 42

    payload = randbytes(100)
    envelope = Envelope()
    envelope.add_item(Item(type=behavior.item_type, payload=PayloadRef(payload)))

    relay.send_envelope(project_id, envelope)

    captured = mini_sentry.captured_events.get(timeout=1)
    (item,) = captured.items
    assert item.payload.get_bytes() == payload

    if behavior.expected_outcomes is not None:
        with pytest.raises(HTTPError):
            relay.send_envelope(project_id, envelope)

        for _ in range(0, len(behavior.expected_outcomes)):
            client_report = mini_sentry.get_client_report(timeout=1)
            rate_limited_events = client_report["rate_limited_events"]
            assert len(rate_limited_events) == 1
            assert rate_limited_events[0] in behavior.expected_outcomes

        if behavior.expected_forward:
            captured = mini_sentry.captured_events.get(timeout=1)
            (item,) = captured.items
            assert item.payload.get_bytes() == payload
    else:
        # If there is no outcome than they should just be forwarded
        relay.send_envelope(project_id, envelope)
        captured = mini_sentry.captured_events.get(timeout=1)
        (item,) = captured.items
        assert item.payload.get_bytes() == payload

    with pytest.raises(queue.Empty):
        mini_sentry.get_client_report(timeout=1)
    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)
