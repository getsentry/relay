from datetime import datetime, timezone
from typing import Literal
import uuid
import json

import pytest
from sentry_relay.consts import DataCategory
from sentry_sdk.envelope import Envelope, Item, PayloadRef
import queue
from .asserts import time_within_delta


def _create_transaction_item(trace_id=None, event_id=None, transaction=None, **kwargs):
    """
    Creates a transaction item that can be added to an envelope

    :return: a tuple (transaction_item, trace_id)
    """
    trace_id = trace_id or uuid.uuid4().hex
    event_id = event_id or uuid.uuid4().hex
    item = {
        "event_id": event_id,
        "type": "transaction",
        "transaction": transaction or "tr1",
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "contexts": {
            "trace": {
                "trace_id": trace_id,
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [],
        "extra": {"id": event_id},
        **kwargs,
    }
    return item, trace_id, event_id


def _create_event_item(
    environment=None, release=None, trace_id=None, event_id=None, transaction=None
):
    """
    Creates an event with the specified environment and release
    :return: a tuple (event_item, event_id)
    """
    event_id = event_id or uuid.uuid4().hex
    trace_id = trace_id or uuid.uuid4().hex
    item = {
        "event_id": event_id,
        "message": "Hello, World!",
        "contexts": {
            "trace": {
                "trace_id": trace_id,
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "extra": {"id": event_id},
    }
    if transaction is not None:
        item["transaction"] = transaction
    if environment is not None:
        item["environment"] = environment
    if release is not None:
        item["release"] = release
    return item, trace_id, event_id


def _outcomes_enabled_config():
    """
    Returns a configuration for Relay that enables outcome generation
    """
    return {
        "outcomes": {
            "emit_outcomes": True,
            "source": "relay",
        },
        "http": {
            "global_metrics": True,
        },
    }


def add_sampling_config(
    config,
    sample_rate,
    rule_type,
    releases=None,
    user_segments=None,
    environments=None,
):
    """
    Adds a sampling configuration rule to a project configuration
    """
    ds = config["config"].setdefault("sampling", {})
    ds.setdefault("version", 2)
    # We set the rules as empty array, and we add rules to it.
    rules = ds.setdefault("rules", [])

    if rule_type is None:
        rule_type = "trace"
    conditions = []
    field_prefix = "trace." if rule_type == "trace" else "event."
    if releases is not None:
        conditions.append(
            {
                "op": "glob",
                "name": field_prefix + "release",
                "value": releases,
            }
        )
    if user_segments is not None:
        conditions.append(
            {
                "op": "eq",
                "name": field_prefix + "user",
                "value": user_segments,
                "options": {
                    "ignoreCase": True,
                },
            }
        )
    if environments is not None:
        conditions.append(
            {
                "op": "eq",
                "name": field_prefix + "environment",
                "value": environments,
                "options": {
                    "ignoreCase": True,
                },
            }
        )

    rule = {
        "samplingValue": {"type": "sampleRate", "value": sample_rate},
        "type": rule_type,
        "condition": {"op": "and", "inner": conditions},
        "id": len(rules) + 1,
    }
    rules.append(rule)
    return rules


def _add_trace_info(
    envelope,
    trace_id,
    public_key,
    release=None,
    user_segment=None,
    client_sample_rate=None,
    transaction=None,
    sampled=None,
):
    """
    Adds trace information to an envelope (to the envelope headers)
    """
    if envelope.headers is None:
        envelope.headers = {}

    trace_info = {"trace_id": trace_id, "public_key": public_key}
    envelope.headers["trace"] = trace_info

    if release is not None:
        trace_info["release"] = release

    if user_segment is not None:
        trace_info["user"] = user_segment

    if client_sample_rate is not None:
        # yes, sdks ought to send this as string and so do we
        trace_info["sample_rate"] = json.dumps(client_sample_rate)

    if transaction is not None:
        trace_info["transaction"] = transaction

    if sampled is not None:
        trace_info["sampled"] = sampled


def _create_transaction_envelope(
    public_key,
    client_sample_rate=None,
    trace_id=None,
    event_id=None,
    transaction=None,
    **kwargs,
):
    envelope = Envelope()
    if event_id:
        envelope.headers["event_id"] = event_id
    transaction_event, trace_id, event_id = _create_transaction_item(
        trace_id=trace_id, event_id=event_id, transaction=transaction, **kwargs
    )
    envelope.add_transaction(transaction_event)
    _add_trace_info(
        envelope,
        trace_id=trace_id,
        public_key=public_key,
        client_sample_rate=client_sample_rate,
        transaction=transaction,
    )
    return envelope, trace_id, event_id


def _create_error_envelope(public_key):
    envelope = Envelope()
    event_id = "abbcea72-abc7-4ac6-93d2-2b8f366e58d7"
    trace_id = "f26fdb98-68f7-4e10-b9bc-2d2dd9256e53"
    error_event = {
        "event_id": event_id,
        "message": "This is an error.",
        "extra": {"msg_text": "This is an error", "id": event_id},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    }
    envelope.add_event(error_event)
    _add_trace_info(
        envelope,
        trace_id=trace_id,
        public_key=public_key,
        client_sample_rate=1.0,
        transaction="/transaction",
        release="1.0",
        sampled="true",
    )
    return envelope, event_id


def test_it_removes_events(mini_sentry, relay):
    """
    Tests that when sampling is set to 0% for the trace context project the events are removed
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)

    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that removes all transactions (sample_rate=0)
    add_sampling_config(config, sample_rate=0, rule_type="transaction")

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, trace_id, event_id = _create_transaction_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.TRANSACTION_INDEXED,
            "outcome": 1,
            "public_key": public_key,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
        {
            "category": DataCategory.SPAN_INDEXED,
            "outcome": 1,
            "public_key": public_key,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
    ]
    assert mini_sentry.captured_envelopes.empty()


def test_it_does_not_sample_error(mini_sentry, relay):
    """
    Tests that we keep an event if it is of type error.
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that removes all traces of release "1.0"
    add_sampling_config(config, sample_rate=0, rule_type="trace", releases=["1.0"])

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, event_id = _create_error_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # test that error is kept by Relay
    envelope = mini_sentry.get_captured_envelope()
    assert envelope is not None
    # double check that we get back our object
    # we put the id in extra since Relay overrides the initial event_id
    items = [item for item in envelope]
    assert len(items) == 1
    evt = items[0].payload.json
    evt_id = evt.setdefault("extra", {}).get("id")
    assert evt_id == event_id


@pytest.mark.parametrize(
    "expected_sampled, sample_rate",
    [
        (True, 1.0),
        (False, 0.0),
    ],
)
def test_it_tags_error(
    mini_sentry, relay_with_processing, events_consumer, expected_sampled, sample_rate
):
    """
    Tests that it tags an incoming error if the trace connected to it its sampled or not.
    """
    events_consumer = events_consumer()

    project_id = 42
    relay = relay_with_processing(_outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that keeps all events (sample_rate=1)
    add_sampling_config(
        config, sample_rate=sample_rate, rule_type="trace", releases=["1.0"]
    )

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, event_id = _create_error_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)

    # The event must always be kept, independent of the sampling decision
    event, _ = events_consumer.get_event()
    assert event["contexts"]["trace"]["sampled"] == expected_sampled
    assert event.setdefault("extra", {}).get("id") == event_id


def test_sample_on_parametrized_root_transaction(mini_sentry, relay):
    """
    Tests that dynamic sampling on traces operate on
    the parametrized version of the root transaction.
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # The pattern used to parametrize the transaction
    pattern = "/auth/login/*/**"
    # How the original transaction is in the DSC.
    original_transaction = "/auth/login/test/"
    # What the transaction is transformed into, which the dynamic sampling rules should respect.
    parametrized_transaction = "/auth/login/*/"

    project_config = mini_sentry.add_basic_project_config(project_id)
    public_key = project_config["publicKeys"][0]["publicKey"]

    sampling_config = mini_sentry.add_basic_project_config(43)
    sampling_public_key = sampling_config["publicKeys"][0]["publicKey"]
    sampling_config["config"]["txNameRules"] = [
        {
            "pattern": pattern,
            "expiry": "3022-11-30T00:00:00.000000Z",
            "redaction": {"method": "replace", "substitution": "*"},
        }
    ]

    ds = sampling_config["config"].setdefault("sampling", {})
    ds.setdefault("version", 2)

    sampling_rule = {
        "samplingValue": {"type": "sampleRate", "value": 0.0},
        "type": "trace",
        "condition": {
            "op": "and",
            "inner": [
                {
                    "op": "eq",
                    "name": "trace.transaction",
                    "value": parametrized_transaction,
                    "options": {
                        "ignoreCase": True,
                    },
                }
            ],
        },
        "id": 1,
    }

    rules = ds.setdefault("rules", [sampling_rule])
    rules.append(sampling_rule)

    envelope = Envelope()
    transaction_event, trace_id, _ = _create_transaction_item()
    envelope.add_transaction(transaction_event)
    _add_trace_info(
        envelope, trace_id, sampling_public_key, transaction=original_transaction
    )

    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.TRANSACTION_INDEXED,
            "outcome": 1,
            "public_key": public_key,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
        {
            "category": DataCategory.SPAN_INDEXED,
            "outcome": 1,
            "public_key": public_key,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
    ]


def test_it_keeps_events(mini_sentry, relay):
    """
    Tests that when sampling is set to 100% for the trace context project the events are kept
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that keeps all events (sample_rate=1)
    add_sampling_config(config, sample_rate=1, rule_type="transaction")

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, trace_id, event_id = _create_transaction_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # the event should be left alone by Relay sampling
    envelope = mini_sentry.get_captured_envelope()
    assert envelope is not None
    # double check that we get back our object
    # we put the id in extra since Relay overrides the initial event_id
    items = [item for item in envelope]
    assert len(items) == 1
    evt = items[0].payload.json
    evt_id = evt.setdefault("extra", {}).get("id")
    assert evt_id == event_id

    # no outcome should be generated since we forward the event to upstream
    with pytest.raises(queue.Empty):
        mini_sentry.captured_outcomes.get(timeout=2)


def test_uses_trace_public_key(mini_sentry, relay):
    """
    Tests that the public_key from the trace context is used

    The project configuration corresponding to the project pointed to
    by the context public_key DSN is used (not the dsn of the request)

    Create a trace context for projectA and send an event from projectB
    using projectA's trace.

    Configure project1 to sample out all events (sample_rate=0)
    Configure project2 to sample in all events (sample_rate=1)
    First:
        Send event to project2 with trace from project1
        It should be removed (sampled out)
    Second:
        Send event to project1 with trace from project2
        It should pass through

    """
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create basic project configs
    project_id1 = 42
    config1 = mini_sentry.add_basic_project_config(project_id1)

    public_key1 = config1["publicKeys"][0]["publicKey"]
    add_sampling_config(config1, sample_rate=0, rule_type="trace")

    project_id2 = 43
    config2 = mini_sentry.add_basic_project_config(project_id2)
    public_key2 = config2["publicKeys"][0]["publicKey"]
    add_sampling_config(config2, sample_rate=1, rule_type="trace")

    # First
    # send trace with project_id1 context (should be removed)
    envelope = Envelope()
    transaction, trace_id, event_id = _create_transaction_item()
    envelope.add_transaction(transaction)
    _add_trace_info(envelope, trace_id=trace_id, public_key=public_key1)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id2, envelope)
    # Dynamic sampling metrics
    metrics_batch1 = mini_sentry.get_global_metrics()
    print(metrics_batch1)
    metrics_batch2 = mini_sentry.get_global_metrics()
    print(metrics_batch2)
    assert metrics_batch1 is not None
    assert metrics_batch2 is not None
    assert mini_sentry.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.TRANSACTION_INDEXED,
            "outcome": 1,
            "public_key": public_key2,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
        {
            "category": DataCategory.SPAN_INDEXED,
            "outcome": 1,
            "public_key": public_key2,
            "quantity": 1,
            "reason": "Sampled:0",
            "source": "relay",
        },
    ]
    assert mini_sentry.captured_envelopes.empty()
    assert mini_sentry.captured_metrics.empty()

    # Second
    # send trace with project_id2 context (should go through)
    envelope = Envelope()
    transaction, trace_id, event_id = _create_transaction_item()
    envelope.add_transaction(transaction)
    _add_trace_info(envelope, trace_id=trace_id, public_key=public_key2)

    # send the event.
    relay.send_envelope(project_id1, envelope)

    # the event should be passed along to upstream (with the transaction unchanged)
    evt = mini_sentry.get_captured_envelope().get_transaction_event()
    assert evt is not None

    # no outcome should be generated (since the event is passed along to the upstream)
    assert mini_sentry.get_aggregated_outcomes(timeout=2) == []


@pytest.mark.parametrize(
    "rule_type, event_factory",
    [
        ("transaction", _create_transaction_envelope),
        ("trace", _create_transaction_envelope),
    ],
)
def test_multi_item_envelope(mini_sentry, relay, rule_type, event_factory):
    """
    Associated items are removed together with event item.

    The event is sent twice to account for both fast and slow paths.

    When sampling decides to remove a transaction it should also remove all
    dependent items (attachments).
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)
    # add a sampling rule to project config that removes all transactions (sample_rate=0)
    public_key = config["publicKeys"][0]["publicKey"]
    # add a sampling rule to project config that drops all events (sample_rate=0), it should be ignored
    # because there is an invalid rule in the configuration
    add_sampling_config(config, sample_rate=0, rule_type=rule_type)

    for i in range(2):
        # create an envelope with a trace context that is initiated by this project (for simplicity)
        envelope = Envelope()
        # create an envelope with a trace context that is initiated by this project (for simplicity)
        envelope, trace_id, event_id = event_factory(public_key)
        envelope.add_item(
            Item(payload=PayloadRef(json={"x": "some attachment"}), type="attachment")
        )
        envelope.add_item(
            Item(
                payload=PayloadRef(json={"y": "some other attachment"}),
                type="attachment",
            )
        )

        # send the event, the transaction should be removed.
        relay.send_envelope(project_id, envelope)
        # the event should be removed by Relay sampling
        assert mini_sentry.captured_envelopes.empty()

        assert mini_sentry.get_aggregated_outcomes(n=4) == [
            {
                "category": DataCategory.ATTACHMENT,
                "outcome": 1,
                "public_key": public_key,
                "quantity": 52,
                "reason": "Sampled:0",
                "source": "relay",
            },
            {
                "category": DataCategory.TRANSACTION_INDEXED,
                "outcome": 1,
                "public_key": public_key,
                "quantity": 1,
                "reason": "Sampled:0",
                "source": "relay",
            },
            {
                "category": DataCategory.SPAN_INDEXED,
                "outcome": 1,
                "public_key": public_key,
                "quantity": 1,
                "reason": "Sampled:0",
                "source": "relay",
            },
            {
                "category": DataCategory.ATTACHMENT_ITEM,
                "outcome": 1,
                "public_key": public_key,
                "quantity": 2,
                "reason": "Sampled:0",
                "source": "relay",
            },
        ]


@pytest.mark.parametrize(
    "rule_type, event_factory",
    [
        ("transaction", _create_transaction_envelope),
        ("trace", _create_transaction_envelope),
    ],
)
def test_client_sample_rate_adjusted(mini_sentry, relay, rule_type, event_factory):
    """
    Tests that the client sample rate is honored when applying server-side
    sampling.

    When client_sample_rate=SAMPLE_RATE, the server should adjust to let most
    events through (since client already sampled). When client_sample_rate=1.0,
    the server should apply the full sampling rate, dropping most of the events.
    """

    def collect_events_batch(expected_count, timeout_per_event=0.1):
        events_count = 0

        for _ in range(expected_count * 3):  # Allow for some extra attempts
            try:
                received_envelope = mini_sentry.get_captured_envelope(
                    timeout=timeout_per_event
                )
                if (
                    received_envelope.get_transaction_event() is not None
                    or received_envelope.get_event() is not None
                ):
                    events_count += 1
            except queue.Empty:
                break

        return events_count

    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # the closer to 0, the less flaky the test is
    # still needs to be distinguishable from 0 in a f32 in rust
    SAMPLE_RATE = 0.001
    add_sampling_config(config, sample_rate=SAMPLE_RATE, rule_type=rule_type)

    NUM_EVENTS = 200

    # Test 1: Send events with client_sample_rate=SAMPLE_RATE
    # The server should adjust sampling to let most of these through
    # since the client already did the sampling
    for i in range(NUM_EVENTS):
        envelope, trace_id, event_id = event_factory(
            public_key, client_sample_rate=SAMPLE_RATE
        )
        relay.send_envelope(project_id, envelope)

    accepted_with_client_sampling = collect_events_batch(NUM_EVENTS)

    # Test 2: Send events with client_sample_rate=1.0
    # The server should apply full sampling rate, dropping most of the event.
    for i in range(NUM_EVENTS):
        envelope, trace_id, event_id = event_factory(public_key, client_sample_rate=1.0)
        relay.send_envelope(project_id, envelope)

    accepted_without_client_sampling = collect_events_batch(NUM_EVENTS)

    # Both should drop most events due to the 0.001 sample rate
    total_events = NUM_EVENTS * 2
    total_accepted = accepted_with_client_sampling + accepted_without_client_sampling

    # Expect to drop most of the events (allowing some variance)
    # We expect at least 99% of the events dropped and accept that we can still receive up to 1% of sent events.
    assert (
        total_accepted <= total_events * 0.01
    ), f"Expected ~99% drop rate overall, got {total_accepted}/{total_events} accepted"


@pytest.mark.parametrize(
    "rule_type, event_factory",
    [
        ("transaction", _create_transaction_envelope),
        ("trace", _create_transaction_envelope),
    ],
)
def test_relay_chain(
    mini_sentry,
    relay,
    rule_type,
    event_factory,
):
    """
    Tests that nested relays do not end up double-sampling. This is guaranteed
    by the fact that we never actually use an RNG, but hash either the event-
    or trace-id.
    """

    project_id = 42
    relay = relay(relay(mini_sentry))
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]
    SAMPLE_RATE = 0.001
    add_sampling_config(config, sample_rate=SAMPLE_RATE, rule_type=rule_type)

    # A trace ID that gets hashed to a value lower than 0.001
    magic_uuid = "414e119d37694a32869f9d81b76a0b70"

    envelope, trace_id, event_id = event_factory(
        public_key,
        trace_id=None if rule_type != "trace" else magic_uuid,
        event_id=None if rule_type == "trace" else magic_uuid,
    )
    relay.send_envelope(project_id, envelope)

    assert mini_sentry.get_captured_envelope().get_transaction_event() is not None


@pytest.mark.parametrize("mode", ["default", "chain"])
def test_relay_chain_keep_unsampled_profile(
    mini_sentry, relay, relay_with_processing, profiles_consumer, mode
):
    profiles_consumer = profiles_consumer()

    # Create an envelope with a profile:
    def make_envelope(public_key):
        trace_uuid = "414e119d37694a32869f9d81b76a0bbb"
        transaction_uuid = "414e119d37694a32869f9d81b76a0baa"

        envelope, trace_id, event_id = _create_transaction_envelope(
            public_key,
            trace_id=trace_uuid,
            event_id=transaction_uuid,
            transaction="my_first_transaction",
            platform="python",
        )
        transaction = envelope.get_transaction_event()
        profile_payload = get_profile_payload(transaction)
        envelope.add_item(
            Item(
                payload=PayloadRef(bytes=json.dumps(profile_payload).encode()),
                type="profile",
            )
        )
        return envelope

    project_id = 42
    if mode == "chain":
        relay = relay(relay_with_processing())
    else:
        relay = relay_with_processing()
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["features"] = [
        "organizations:profiling",
    ]

    public_key = config["publicKeys"][0]["publicKey"]
    add_sampling_config(config, sample_rate=0.0, rule_type="transaction")

    envelope = make_envelope(public_key)

    relay.send_envelope(project_id, envelope)

    profile, headers = profiles_consumer.get_profile()

    assert headers == [("project_id", b"42"), ("sampled", b"false")]
    profile_payload = json.loads(profile["payload"])
    assert (
        profile_payload["transaction_metadata"]["transaction"] == "my_first_transaction"
    )


def get_profile_payload(transaction):
    return {
        "debug_meta": {"images": []},
        "device": {
            "architecture": "x86_64",
            "classification": "",
            "locale": "",
            "manufacturer": "",
            "model": "",
        },
        "event_id": "429c1ffa194f41f5b6a6650929744177",
        "os": {"build_number": "", "name": "Linux", "version": "5.15.107+"},
        "organization_id": 1,
        "platform": "python",
        "project_id": 1,
        "retention_days": 90,
        "runtime": {"name": "CPython", "version": "3.8.18"},
        "timestamp": datetime.fromtimestamp(transaction["start_timestamp"]).isoformat()
        + "Z",
        "profile": {
            "frames": [
                {
                    "data": {},
                    "filename": "concurrent/futures/thread.py",
                    "function": "_worker",
                    "in_app": False,
                    "lineno": 78,
                    "module": "concurrent.futures.thread",
                    "abs_path": "/usr/local/lib/python3.8/concurrent/futures/thread.py",
                },
                {
                    "data": {},
                    "filename": "threading.py",
                    "function": "Thread.run",
                    "in_app": False,
                    "lineno": 870,
                    "module": "threading",
                    "abs_path": "/usr/local/lib/python3.8/threading.py",
                },
                {
                    "data": {},
                    "filename": "sentry_sdk/integrations/threading.py",
                    "function": "run",
                    "in_app": False,
                    "lineno": 70,
                    "module": "sentry_sdk.integrations.threading",
                    "abs_path": "/usr/local/lib/python3.8/site-packages/sentry_sdk/integrations/threading.py",
                },
                {
                    "data": {},
                    "filename": "threading.py",
                    "function": "Thread._bootstrap_inner",
                    "in_app": False,
                    "lineno": 932,
                    "module": "threading",
                    "abs_path": "/usr/local/lib/python3.8/threading.py",
                },
                {
                    "data": {},
                    "filename": "threading.py",
                    "function": "Thread._bootstrap",
                    "in_app": False,
                    "lineno": 890,
                    "module": "threading",
                    "abs_path": "/usr/local/lib/python3.8/threading.py",
                },
            ],
            "queue_metadata": None,
            "samples": [
                {
                    "elapsed_since_start_ns": 2668948,
                    "stack_id": 0,
                    "thread_id": 140510151571200,
                },
                {
                    "elapsed_since_start_ns": 2668948,
                    "stack_id": 0,
                    "thread_id": 140510673217280,
                },
                {
                    "elapsed_since_start_ns": 2668948,
                    "stack_id": 0,
                    "thread_id": 140510673217280,
                },
            ],
            "stacks": [[0, 1, 2, 3, 4]],
            "thread_metadata": {
                "140510151571200": {"name": "ThreadPoolExecutor-1_4"},
                "140510673217280": {"name": "ThreadPoolExecutor-1_3"},
                "140510710716160": {"name": "Thread-19"},
                "140510719108864": {"name": "Thread-18"},
                "140510727501568": {"name": "ThreadPoolExecutor-1_2"},
                "140511074039552": {"name": "ThreadPoolExecutor-1_1"},
                "140511082432256": {"name": "ThreadPoolExecutor-1_0"},
                "140511090824960": {"name": "Thread-17"},
                "140511099217664": {"name": "raven-sentry.BackgroundWorker"},
                "140511117047552": {"name": "raven-sentry.BackgroundWorker"},
                "140511574738688": {"name": "sentry.profiler.ThreadScheduler"},
                "140511583131392": {"name": "sentry.monitor"},
                "140512539440896": {"name": "uWSGIWorker6Core1"},
                "140512620431104": {"name": "Thread-1"},
                "140514768926528": {"name": "uWSGIWorker6Core0"},
            },
        },
        "transaction": {
            "active_thread_id": 140512539440896,
            "id": transaction["event_id"],
            "name": "/api/0/organizations/{organization_slug}/broadcasts/",
            "trace_id": transaction["contexts"]["trace"]["trace_id"],
        },
        "version": "1",
    }


def test_invalid_global_generic_filters_skip_dynamic_sampling(mini_sentry, relay):
    mini_sentry.global_config["filters"] = {
        "version": 65535,  # u16::MAX
        "filters": [],
    }

    relay = relay(mini_sentry, _outcomes_enabled_config())

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # Reject all transactions with dynamic sampling
    add_sampling_config(config, sample_rate=0, rule_type="transaction")

    envelope, _, _ = _create_transaction_envelope(public_key, client_sample_rate=0)

    relay.send_envelope(project_id, envelope)
    assert mini_sentry.get_captured_envelope()


def test_invalid_metric_extraction_config_skips_dynamic_sampling(mini_sentry, relay):
    relay = relay(mini_sentry, _outcomes_enabled_config())

    project_id = 42
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # Unsupported metric extraction config, so no metrics can be extracted
    config["config"]["metricExtraction"] = {
        "version": 666,  # this version is too new for this relay
        "metrics": [],
    }

    # Reject all transactions with dynamic sampling
    add_sampling_config(config, sample_rate=0, rule_type="transaction")

    envelope, _, _ = _create_transaction_envelope(public_key)

    relay.send_envelope(project_id, envelope)
    assert mini_sentry.get_captured_envelope()


def get_transaction_envelope(
    trace_id: str,
    segment_id: str,
    child_id_1: str,
    child_id_2: str,
    dsc: Literal["dsc_with_tx", "dsc_no_tx", "no_dsc"],
    sampling_project_config: dict,
):
    ts = datetime.now(timezone.utc)

    # Create transaction
    event = {
        "type": "transaction",
        "timestamp": ts.timestamp(),
        "start_timestamp": ts.timestamp(),
        "spans": [],
        "contexts": {
            "trace": {
                "op": "/trace/",
                "trace_id": trace_id,
                "span_id": segment_id,
            }
        },
        "transaction": "/event/",
    }

    # Add child spans
    event["spans"] = [
        {
            "trace_id": trace_id,
            "span_id": child_id_1,
            "parent_span_id": segment_id,
            "start_timestamp": ts.timestamp(),
            "timestamp": ts.timestamp() + 0.3,
        },
        {
            "trace_id": trace_id,
            "span_id": child_id_2,
            "parent_span_id": segment_id,
            "start_timestamp": ts.timestamp(),
            "timestamp": ts.timestamp() + 0.3,
            "data": {
                "sentry.dsc.trace_id": trace_id,
                "sentry.dsc.transaction": "/spandata/",
                "sentry.dsc.project_id": "41",
            },
        },
    ]

    # Add transaction to envelope
    envelope = Envelope()
    envelope.add_item(Item(payload=PayloadRef(json=event), type="transaction"))

    # Add DSC to envelope
    if dsc != "no_dsc":
        envelope.headers["trace"] = {
            "trace_id": trace_id,
            "public_key": sampling_project_config["publicKeys"][0]["publicKey"],
            "sample_rate": "1",
            "sample_rand": "0.9",
            "sampled": "true",
            "release": "some_release",
            "environment": "some_environment",
            **({"transaction": "/dsc/"} if dsc == "dsc_with_tx" else {}),
            "org_id": sampling_project_config["organizationId"],
        }

    return envelope


def get_v2_envelope(
    trace_id: str,
    segment_id: str,
    child_id_1: str,
    child_id_2: str,
    dsc: Literal["dsc_with_tx", "dsc_no_tx", "no_dsc"],
    sampling_project_config: dict,
):
    ts = datetime.now(timezone.utc)

    spans = [
        # Segment span.
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.5,
            "trace_id": trace_id,
            "span_id": segment_id,
            "is_segment": True,
            "name": "root",
            "status": "ok",
            "attributes": {
                "sentry.segment.name": {"type": "string", "value": "/segment/"},
            },
        },
        # Child span.
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.3,
            "trace_id": trace_id,
            "span_id": child_id_1,
            "parent_span_id": segment_id,
            "is_segment": False,
            "name": "child1",
            "status": "ok",
            "attributes": {
                "sentry.segment.name": {"type": "string", "value": "/segment/"},
            },
        },
        # Child span which already has `sentry.dsc.*` attributes set.
        {
            "start_timestamp": ts.timestamp(),
            "end_timestamp": ts.timestamp() + 0.3,
            "trace_id": trace_id,
            "span_id": child_id_2,
            "parent_span_id": segment_id,
            "is_segment": False,
            "name": "child2",
            "status": "ok",
            "attributes": {
                "sentry.dsc.trace_id": {"type": "string", "value": trace_id},
                "sentry.dsc.transaction": {"type": "string", "value": "/spandata/"},
                "sentry.dsc.project_id": {"type": "string", "value": "41"},
                "sentry.segment.name": {"type": "string", "value": "/segment/"},
            },
        },
    ]

    # Add spans to envelope
    envelope = Envelope()
    envelope.add_item(
        Item(
            type="span",
            payload=PayloadRef(json={"items": spans}),
            content_type="application/vnd.sentry.items.span.v2+json",
            headers={"item_count": len(spans)},
        )
    )

    # Add DSC to envelope
    trace_info = (
        {
            "trace_id": trace_id,
            "public_key": sampling_project_config["publicKeys"][0]["publicKey"],
            "sample_rate": "1",
            "sampled": "true",
            "release": "some_release",
            "environment": "some_environment",
            **({"transaction": "/dsc/"} if dsc == "dsc_with_tx" else {}),
        }
        if dsc != "no_dsc"
        else None
    )
    envelope.headers["trace"] = trace_info

    return envelope


@pytest.mark.parametrize("span_type", ["tx", "v2"])
@pytest.mark.parametrize("org", ["same_org", "diff_org"])
@pytest.mark.parametrize("dsc", ["dsc_with_tx", "dsc_no_tx", "no_dsc"])
def test_dsc_normalization(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    dsc,
    org,
    span_type,
):
    segment_id = "a" * 16
    child_id_1 = "b" * 16
    child_id_2 = "c" * 16
    trace_id = "a0fa8803753e40fd8124b21eeb2986b5"
    project_id = 42
    sampling_project_id = 43
    org_id = 1
    sampling_org_id = 1 if org == "same_org" else 2
    mini_sentry.add_full_project_config(project_id, extra={"organizationId": org_id})
    sampling_project_config = mini_sentry.add_full_project_config(
        sampling_project_id, extra={"organizationId": sampling_org_id}
    )
    relay = relay(relay_with_processing())
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    # Expected results based on the parameters
    expected_tx, expected_project_id, expected_root_org_id = {
        # DSC with tx + same org
        ("dsc_with_tx", "same_org", "tx"): ("/dsc/", sampling_project_id, org_id),
        ("dsc_with_tx", "same_org", "v2"): ("/dsc/", sampling_project_id, org_id),
        # ----------------------------------------------------------------------
        # DSC without tx + same org
        ("dsc_no_tx", "same_org", "tx"): (None, sampling_project_id, org_id),
        ("dsc_no_tx", "same_org", "v2"): (None, sampling_project_id, org_id),
        # ----------------------------------------------------------------------
        # DSC with tx + different org
        ("dsc_with_tx", "diff_org", "tx"): ("/event/", project_id, org_id),
        ("dsc_with_tx", "diff_org", "v2"): (None, project_id, org_id),
        # ----------------------------------------------------------------------
        # DSC without tx + different org
        ("dsc_no_tx", "diff_org", "tx"): ("/event/", project_id, org_id),
        ("dsc_no_tx", "diff_org", "v2"): (None, project_id, org_id),
        # ----------------------------------------------------------------------
        # No DSC
        ("no_dsc", "same_org", "tx"): ("/event/", project_id, org_id),
        ("no_dsc", "diff_org", "tx"): ("/event/", project_id, org_id),
        ("no_dsc", "same_org", "v2"): (None, None, None),  # rejected, see early return
        ("no_dsc", "diff_org", "v2"): (None, None, None),  # rejected, see early return
        # ----------------------------------------------------------------------
    }[dsc, org, span_type]

    envelope = (
        get_transaction_envelope(
            trace_id=trace_id,
            segment_id=segment_id,
            child_id_1=child_id_1,
            child_id_2=child_id_2,
            dsc=dsc,
            sampling_project_config=sampling_project_config,
        )
        if span_type == "tx"
        else get_v2_envelope(
            trace_id=trace_id,
            segment_id=segment_id,
            child_id_1=child_id_1,
            child_id_2=child_id_2,
            dsc=dsc,
            sampling_project_config=sampling_project_config,
        )
    )

    relay.send_envelope(project_id, envelope)
    metrics = metrics_consumer.get_metrics(with_headers=False)
    metrics = [m for m in metrics if "count_per_root_project" in m["name"]]
    spans = {s["span_id"]: s for s in spans_consumer.get_spans()}

    if dsc == "no_dsc" and span_type == "v2":
        assert len(spans) == 0
        assert len(metrics) == 0
        return

    def get_dsc_attr(attr: str, span_id: str):
        return spans[span_id]["attributes"].get(f"sentry.dsc.{attr}", {}).get("value")

    # Segment span
    assert spans[segment_id]["is_segment"] is True
    assert get_dsc_attr("transaction", segment_id) == expected_tx
    assert get_dsc_attr("project_id", segment_id) == str(expected_project_id)
    assert get_dsc_attr("trace_id", segment_id) == trace_id

    # Child span
    assert spans[child_id_1]["is_segment"] is False
    assert get_dsc_attr("transaction", child_id_1) == expected_tx
    assert get_dsc_attr("project_id", child_id_1) == str(expected_project_id)
    assert get_dsc_attr("trace_id", child_id_1) == trace_id

    # Child span with sentry.dsc.* attributes already set in its span data
    assert spans[child_id_2]["is_segment"] is False
    assert get_dsc_attr("transaction", child_id_2) == "/spandata/"
    assert get_dsc_attr("project_id", child_id_2) == "41"
    assert get_dsc_attr("trace_id", child_id_2) == trace_id

    assert metrics == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": expected_root_org_id,
            "project_id": expected_project_id,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "false",
                "target_project_id": str(project_id),
                **({"transaction": expected_tx} if expected_tx else {}),
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 2.0,
        },
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": expected_root_org_id,
            "project_id": expected_project_id,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "true",
                "target_project_id": str(project_id),
                **({"transaction": expected_tx} if expected_tx else {}),
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]
