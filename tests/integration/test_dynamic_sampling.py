from datetime import datetime
import uuid
import json

import pytest
from sentry_sdk.envelope import Envelope, Item, PayloadRef


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


def test_relay_chain_keep_unsampled_profile(
    mini_sentry, relay, relay_with_processing, profiles_consumer
):
    mini_sentry.global_config["options"] = {
        "profiling.profile_metrics.unsampled_profiles.platforms": ["python"],
        "profiling.profile_metrics.unsampled_profiles.sample_rate": 1.0,
        "profiling.profile_metrics.unsampled_profiles.enabled": True,
    }

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
    relay = relay(relay_with_processing())
    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["transactionMetrics"] = {"version": 1}
    config["config"]["features"] = ["projects:profiling-ingest-unsampled-profiles"]

    public_key = config["publicKeys"][0]["publicKey"]
    _add_sampling_config(config, sample_rate=0.0, rule_type="transaction")

    envelope = make_envelope(public_key)

    relay.send_envelope(project_id, envelope)

    profile, headers = profiles_consumer.get_profile()

    assert headers == [("sampled", b"false")]
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
