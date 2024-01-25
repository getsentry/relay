from datetime import datetime
import uuid
import json
from unittest import mock

import pytest
from sentry_sdk.envelope import Envelope, Item, PayloadRef
import queue

from .fixtures.mini_sentry import GLOBAL_CONFIG


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
            "batch_size": 1,
            "batch_interval": 1,
            "source": "relay",
        }
    }


def _add_sampling_config(
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


def _create_event_envelope(
    public_key, client_sample_rate=None, trace_id=None, event_id=None, transaction=None
):
    envelope = Envelope()
    event, trace_id, event_id = _create_event_item(
        trace_id=trace_id, event_id=event_id, transaction=transaction
    )
    envelope.add_event(event)
    _add_trace_info(
        envelope,
        trace_id=trace_id,
        public_key=public_key,
        client_sample_rate=client_sample_rate,
        transaction=transaction,
    )

    return envelope, trace_id, event_id


def _create_transaction_envelope(
    public_key,
    client_sample_rate=None,
    trace_id=None,
    event_id=None,
    transaction=None,
    **kwargs,
):
    envelope = Envelope()
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
    config["config"]["transactionMetrics"] = {"version": 1}

    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that removes all transactions (sample_rate=0)
    rules = _add_sampling_config(config, sample_rate=0, rule_type="transaction")

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, trace_id, event_id = _create_transaction_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # the event should be removed by Relay sampling
    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)

    outcomes = mini_sentry.captured_outcomes.get(timeout=2)
    assert outcomes is not None
    outcome = outcomes["outcomes"][0]
    assert outcome.get("outcome") == 1
    assert outcome.get("reason") == f"Sampled:{rules[0]['id']}"


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
    _add_sampling_config(config, sample_rate=0, rule_type="trace", releases=["1.0"])

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, event_id = _create_error_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # test that error is kept by Relay
    envelope = mini_sentry.captured_events.get(timeout=1)
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
def test_it_tags_error(mini_sentry, relay, expected_sampled, sample_rate):
    """
    Tests that it tags an incoming error if the trace connected to it its sampled or not.
    """
    project_id = 42
    relay = relay(mini_sentry, _outcomes_enabled_config())

    # create a basic project config
    config = mini_sentry.add_basic_project_config(project_id)
    public_key = config["publicKeys"][0]["publicKey"]

    # add a sampling rule to project config that keeps all events (sample_rate=1)
    _add_sampling_config(
        config, sample_rate=sample_rate, rule_type="trace", releases=["1.0"]
    )

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, event_id = _create_error_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # test that error is kept by Relay
    envelope = mini_sentry.captured_events.get(timeout=1)
    assert envelope is not None
    # double check that we get back our object
    # we put the id in extra since Relay overrides the initial event_id
    items = [item for item in envelope]
    assert len(items) == 1
    evt = items[0].payload.json
    # we check if it is marked as sampled
    assert evt["contexts"]["trace"]["sampled"] == expected_sampled
    evt_id = evt.setdefault("extra", {}).get("id")
    assert evt_id == event_id


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
    _add_sampling_config(config, sample_rate=1, rule_type="transaction")

    # create an envelope with a trace context that is initiated by this project (for simplicity)
    envelope, trace_id, event_id = _create_transaction_envelope(public_key)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id, envelope)
    # the event should be left alone by Relay sampling
    envelope = mini_sentry.captured_events.get(timeout=1)
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
    config1["config"]["transactionMetrics"] = {"version": 1}

    public_key1 = config1["publicKeys"][0]["publicKey"]
    _add_sampling_config(config1, sample_rate=0, rule_type="trace")

    project_id2 = 43
    config2 = mini_sentry.add_basic_project_config(project_id2)
    config2["config"]["transactionMetrics"] = {"version": 1}
    public_key2 = config2["publicKeys"][0]["publicKey"]
    _add_sampling_config(config2, sample_rate=1, rule_type="trace")

    # First
    # send trace with project_id1 context (should be removed)
    envelope = Envelope()
    transaction, trace_id, event_id = _create_transaction_item()
    envelope.add_transaction(transaction)
    _add_trace_info(envelope, trace_id=trace_id, public_key=public_key1)

    # send the event, the transaction should be removed.
    relay.send_envelope(project_id2, envelope)
    # the event should be removed by Relay sampling
    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)

    # and it should create an outcome
    outcomes = mini_sentry.captured_outcomes.get(timeout=2)
    assert outcomes is not None
    with pytest.raises(queue.Empty):
        mini_sentry.captured_outcomes.get(timeout=1)

    # Second
    # send trace with project_id2 context (should go through)
    envelope = Envelope()
    transaction, trace_id, event_id = _create_transaction_item()
    envelope.add_transaction(transaction)
    _add_trace_info(envelope, trace_id=trace_id, public_key=public_key2)

    # send the event.
    relay.send_envelope(project_id1, envelope)

    # the event should be passed along to upstream (with the transaction unchanged)
    evt = mini_sentry.captured_events.get(timeout=1).get_transaction_event()
    assert evt is not None

    # no outcome should be generated (since the event is passed along to the upstream)
    with pytest.raises(queue.Empty):
        mini_sentry.captured_outcomes.get(timeout=2)


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
    config["config"]["transactionMetrics"] = {"version": 1}
    # add a sampling rule to project config that removes all transactions (sample_rate=0)
    public_key = config["publicKeys"][0]["publicKey"]
    # add a sampling rule to project config that drops all events (sample_rate=0), it should be ignored
    # because there is an invalid rule in the configuration
    _add_sampling_config(config, sample_rate=0, rule_type=rule_type)

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
        with pytest.raises(queue.Empty):
            mini_sentry.captured_events.get(timeout=1)

        outcomes = mini_sentry.captured_outcomes.get(timeout=2)
        assert outcomes is not None


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
    sampling. Do so by sending an envelope with a very low reported client sample rate
    and a sampling rule with the same sample rate. The server should adjust
    itself to 1.0. The chances of this test passing without the adjustment in
    place are very low (but not 0).
    """
    project_id = 42
    relay = relay(mini_sentry)
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["transactionMetrics"] = {"version": 1}
    public_key = config["publicKeys"][0]["publicKey"]

    # the closer to 0, the less flaky the test is
    # still needs to be distinguishable from 0 in a f32 in rust
    SAMPLE_RATE = 0.001
    _add_sampling_config(config, sample_rate=SAMPLE_RATE, rule_type=rule_type)

    envelope, trace_id, event_id = event_factory(
        public_key, client_sample_rate=SAMPLE_RATE
    )

    relay.send_envelope(project_id, envelope)

    received_envelope = mini_sentry.captured_events.get(timeout=1)
    received_envelope.get_transaction_event()

    envelope, trace_id, event_id = event_factory(public_key, client_sample_rate=1.0)

    relay.send_envelope(project_id, envelope)

    # Relay is sending a client report, skip over it
    received_envelope = mini_sentry.captured_events.get(timeout=1)
    assert received_envelope.get_transaction_event() is None
    assert received_envelope.get_event() is None

    with pytest.raises(queue.Empty):
        mini_sentry.captured_events.get(timeout=1)


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
    _add_sampling_config(config, sample_rate=SAMPLE_RATE, rule_type=rule_type)

    # A trace ID that gets hashed to a value lower than 0.001
    magic_uuid = "414e119d37694a32869f9d81b76a0b70"

    envelope, trace_id, event_id = event_factory(
        public_key,
        trace_id=None if rule_type != "trace" else magic_uuid,
        event_id=None if rule_type == "trace" else magic_uuid,
    )
    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=1)
    envelope.get_transaction_event()


@mock.patch.dict(
    GLOBAL_CONFIG,
    {
        "options": {
            "profiling.profile_metrics.unsampled_profiles.platforms": ["python"],
            "profiling.profile_metrics.unsampled_profiles.sample_rate": 1.0,
            "profiling.profile_metrics.unsampled_profiles.enabled": True,
        }
    },
)
def test_relay_chain_keep_unsampled_profile(
    mini_sentry,
    relay,
):
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
    relay = relay(relay(mini_sentry))
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["transactionMetrics"] = {"version": 1}
    config["config"]["features"] = ["projects:profiling-ingest-unsampled-profiles"]

    public_key = config["publicKeys"][0]["publicKey"]
    _add_sampling_config(config, sample_rate=0.0, rule_type="transaction")

    envelope = make_envelope(public_key)

    relay.send_envelope(project_id, envelope)
    envelope_items = []
    envelope = mini_sentry.captured_events.get(timeout=1)
    envelope_items.extend(envelope.items)
    envelope = mini_sentry.captured_events.get(timeout=1)
    envelope_items.extend(envelope.items)

    profiles = [item for item in envelope_items if item.data_category == "profile"]
    (profile_item,) = profiles

    assert profile_item.headers.get("sampled") is False

    profile = json.loads(profile_item.payload.bytes)

    # Assert that transaction fields have been set:
    assert profile["transaction_metadata"]["transaction"] == "my_first_transaction"


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
