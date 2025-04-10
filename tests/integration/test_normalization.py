import json

import pytest

from .test_unreal import load_dump_file
import os
from sentry_sdk.envelope import Envelope, Item, PayloadRef


def get_test_data(name):
    input_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-input.json"
    )
    with open(input_path, "r") as f:
        input = json.loads(f.read())
    input.pop("timestamp", None)

    output_path = os.path.join(
        os.path.dirname(__file__), "..", "fixtures", f"{name}-output.json"
    )
    with open(output_path, "r") as f:
        output = json.loads(f.read())

    return input, output


def drop_props(payload):
    props = ["timestamp", "received", "ingest_path", "_metrics"]
    for prop in props:
        payload.pop(prop, None)
    return payload


@pytest.mark.parametrize("config_full_normalization", (False, True))
def test_relay_with_full_normalization(mini_sentry, relay, config_full_normalization):
    input, expected = get_test_data("extended-event")

    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        upstream=mini_sentry,
        options=(
            {"normalization": {"level": "full"}} if config_full_normalization else {}
        ),
    )

    relay.send_event(project_id, input)
    envelope = mini_sentry.captured_events.get(timeout=10)

    if config_full_normalization:
        assert "fully_normalized" in envelope.items[0].headers
        assert drop_props(expected) == drop_props(envelope.get_event())
    else:
        assert "fully_normalized" not in envelope.items[0].headers
        assert drop_props(expected) != drop_props(envelope.get_event())


@pytest.mark.parametrize("config_full_normalization", (False, True))
@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
def test_processing_with_full_normalization(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay_credentials,
    config_full_normalization,
    request_from_internal,
    fully_normalized,
):
    input, expected = get_test_data("extended-event")

    events_consumer = events_consumer()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    relay_config = {}
    if request_from_internal:
        relay_config["auth"] = {
            "static_relays": {
                credentials["id"]: {
                    "public_key": credentials["public_key"],
                    "internal": True,
                },
            }
        }
    if config_full_normalization:
        relay_config["normalization"] = {"level": "full"}
    processing = relay_with_processing(
        options=relay_config,
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    envelope.add_item(
        item=Item(
            type="event",
            payload=PayloadRef(json=input),
            headers={"fully_normalized": True} if fully_normalized else {},
        ),
    )

    processing.send_envelope(
        project_id,
        envelope,
        headers=(
            {"X-Sentry-Relay-Id": credentials["id"]} if request_from_internal else {}
        ),
    )

    ingested, _ = events_consumer.get_event(timeout=10)
    if not config_full_normalization and request_from_internal and fully_normalized:
        assert drop_props(expected) != drop_props(ingested)
    else:
        assert drop_props(expected) == drop_props(ingested)


@pytest.mark.parametrize(
    "relay_static_config_normalization",
    [
        False,
        True,
    ],
)
def test_relay_chain_normalizes_events(
    mini_sentry,
    events_consumer,
    relay_with_processing,
    relay,
    relay_credentials,
    relay_static_config_normalization,
):
    input, expected = get_test_data("extended-event")

    events_consumer = events_consumer()
    project_id = 42

    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    processing = relay_with_processing(
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            }
        }
    )
    relay = relay(
        upstream=processing,
        options=(
            {"normalization": {"level": "full"}}
            if relay_static_config_normalization
            else {}
        ),
        credentials=credentials,
    )

    relay.send_event(project_id, input)

    ingested, _ = events_consumer.get_event(timeout=15)

    if relay_static_config_normalization:
        assert ingested["errors"] == [
            {"name": "location", "type": "invalid_attribute"},
        ]

    assert drop_props(expected) == drop_props(ingested)


@pytest.mark.parametrize("config_full_normalization", (False, True))
@pytest.mark.parametrize("dump_file_name", ("unreal_crash", "unreal_crash_apple"))
def test_relay_doesnt_normalize_unextracted_unreal_event(
    mini_sentry,
    relay,
    dump_file_name,
    config_full_normalization,
):
    """
    Independently of the configuration, relays forward minidumps, apple crash
    reports and unreal events without marking them as normalized.
    """
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    relay = relay(
        mini_sentry,
        options=(
            {"normalization": {"level": "full"}} if config_full_normalization else {}
        ),
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    unreal_content = load_dump_file(dump_file_name)
    envelope.add_item(
        item=Item(
            type="unreal_report",
            payload=PayloadRef(unreal_content),
            # fully_normalized == False
        ),
    )
    relay.send_envelope(project_id, envelope)

    envelope = mini_sentry.captured_events.get(timeout=10)
    assert len(envelope.items) == 1
    assert "fully_normalized" not in envelope.items[0].headers


@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
@pytest.mark.parametrize("dump_file_name", ("unreal_crash", "unreal_crash_apple"))
def test_processing_normalizes_unreal_event(
    mini_sentry,
    attachments_consumer,
    relay_credentials,
    relay_with_processing,
    request_from_internal,
    fully_normalized,
    dump_file_name,
):
    attachments_consumer = attachments_consumer()
    project_id = 42
    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    relay_config = {"processing": {"attachment_chunk_size": "1.23 GB"}}
    if request_from_internal:
        relay_config["auth"] = {
            "static_relays": {
                credentials["id"]: {
                    "public_key": credentials["public_key"],
                    "internal": True,
                },
            }
        }
    processing = relay_with_processing(options=relay_config)

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    unreal_content = load_dump_file(dump_file_name)
    envelope.add_item(
        item=Item(
            type="unreal_report",
            payload=PayloadRef(unreal_content),
            headers={"fully_normalized": True} if fully_normalized else {},
        ),
    )
    processing.send_envelope(
        project_id, envelope, headers={"X-Sentry-Relay-Id": credentials["id"]}
    )

    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"


@pytest.mark.parametrize("request_from_internal", (False, True))
@pytest.mark.parametrize("fully_normalized", (False, True))
def test_processing_normalizes_minidump_events(
    mini_sentry,
    attachments_consumer,
    relay_with_processing,
    relay_credentials,
    request_from_internal,
    fully_normalized,
):
    attachments_consumer = attachments_consumer()
    project_id = 42
    mini_sentry.add_full_project_config(project_id)

    credentials = relay_credentials()
    if request_from_internal:
        static_config = {
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            },
        }
    else:
        static_config = {}
    processing = relay_with_processing(static_relays=static_config)

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})

    minidump = load_dump_file("minidump.dmp")
    item_headers = {"attachment_type": "event.minidump"}
    if fully_normalized:
        item_headers["fully_normalized"] = True
    envelope.add_item(
        item=Item(
            type="attachment",
            payload=PayloadRef(minidump),
            content_type="application/x-dmp",
            headers=item_headers,
        ),
    )
    processing.send_envelope(
        project_id, envelope, headers={"X-Sentry-Relay-Id": credentials["id"]}
    )
    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"


@pytest.mark.parametrize(
    "relay_static_config_normalization",
    [
        False,
        True,
    ],
)
def test_relay_chain_normalizes_minidump_events(
    mini_sentry,
    attachments_consumer,
    relay_with_processing,
    relay,
    relay_credentials,
    relay_static_config_normalization,
):

    attachments_consumer = attachments_consumer()
    project_id = 42

    mini_sentry.add_basic_project_config(project_id)

    credentials = relay_credentials()
    processing = relay_with_processing(
        options={"processing": {"attachment_chunk_size": "1.23 GB"}},
        static_relays={
            credentials["id"]: {
                "public_key": credentials["public_key"],
                "internal": True,
            }
        },
    )
    relay = relay(
        upstream=processing,
        options=(
            {"normalization": {"level": "full"}}
            if relay_static_config_normalization
            else {}
        ),
        credentials=credentials,
    )

    envelope = Envelope(headers={"event_id": "69241adef5744ef19bde5bbd06fe8177"})
    minidump = load_dump_file("minidump.dmp")
    item_headers = {"attachment_type": "event.minidump"}
    envelope.add_item(
        item=Item(
            type="attachment",
            payload=PayloadRef(minidump),
            content_type="application/x-dmp",
            headers=item_headers,
        ),
    )

    relay.send_envelope(project_id, envelope)
    while True:
        _, message = attachments_consumer.get_message()
        # Skip attachment-related messages
        if message.get("type") == "event":
            event = json.loads(message["payload"])
            break

    assert event["exception"]["values"] is not None
    assert event["type"] == "error"


@pytest.mark.parametrize("relay_chain", ["relay->relay->sentry"], indirect=True)
def test_ip_normalization_with_remove_remark(mini_sentry, relay_chain):
    project_id = 42
    relay = relay_chain(min_relay_version="25.01.0")

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"]["piiConfig"]["applications"]["$user.ip_address"] = ["@ip:hash"]

    relay.send_event(project_id, {"platform": "javascript"})

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event["user"]["ip_address"] is None
    assert event["user"]["id"] == "AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741"


@pytest.mark.parametrize(
    "scrub_ip_addresses, user_id",
    [(True, None), (False, "[ip]")],
)
def test_ip_not_extracted_with_setting(mini_sentry, relay, scrub_ip_addresses, user_id):
    project_id = 42
    relay = relay(mini_sentry)

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"].setdefault("datascrubbingSettings", {})[
        "scrubIpAddresses"
    ] = scrub_ip_addresses
    config["config"]["piiConfig"]["applications"]["$user.ip_address"] = ["@ip"]

    relay.send_event(project_id, {"user": {"ip_address": "{{auto}}"}})

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) is None
    assert event.get("user", {}).get("id", None) == user_id


@pytest.mark.parametrize(
    "scrub_ip_addresses, expected_ip", [(True, None), (False, "2.125.160.216")]
)
def test_geo_inferred_without_user_ip(
    mini_sentry, relay, scrub_ip_addresses, expected_ip
):
    project_id = 42
    relay = relay(
        mini_sentry,
        options={"geoip": {"path": "tests/fixtures/GeoIP2-Enterprise-Test.mmdb"}},
    )

    config = mini_sentry.add_basic_project_config(project_id)
    config["config"].setdefault("datascrubbingSettings", {})[
        "scrubIpAddresses"
    ] = scrub_ip_addresses

    relay.send_event(
        project_id,
        {"user": {"ip_address": "{{auto}}"}},
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected_ip
    # Geo is always present
    assert event["user"]["geo"] is not None


@pytest.mark.parametrize(
    "user_ip_address, infer_ip, expected",
    [
        ("{{auto}}", "never", None),
        (None, "never", None),
        ("123.123.123.123", "never", "123.123.123.123"),
        ("{{auto}}", "auto", "2.125.160.216"),
        (None, "auto", "2.125.160.216"),
        ("123.123.123.123", "auto", "123.123.123.123"),
        ("{{auto}}", None, "2.125.160.216"),
        (None, None, "2.125.160.216"),
        ("123.123.123.123", None, "123.123.123.123"),
    ],
)
def test_infer_ip_setting_javascript(
    mini_sentry, relay, user_ip_address, infer_ip, expected
):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "platform": "javascript",
            "user": {"ip_address": user_ip_address},
            "sdk": {"settings": {"infer_ip": infer_ip}},
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


@pytest.mark.parametrize(
    "user_ip_address, infer_ip, expected",
    [
        ("{{auto}}", "never", None),
        (None, "never", None),
        ("123.123.123.123", "never", "123.123.123.123"),
        ("{{auto}}", "auto", "2.125.160.216"),
        (None, "auto", "2.125.160.216"),
        ("123.123.123.123", "auto", "123.123.123.123"),
        ("{{auto}}", None, "2.125.160.216"),
        (None, None, "2.125.160.216"),
        ("123.123.123.123", None, "123.123.123.123"),
    ],
)
def test_auto_infer_setting_cocoa(
    mini_sentry, relay, user_ip_address, infer_ip, expected
):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "platform": "cocoa",
            "user": {"ip_address": user_ip_address},
            "sdk": {"settings": {"infer_ip": infer_ip}},
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


@pytest.mark.parametrize(
    "user_ip_address, infer_ip, expected",
    [
        ("{{auto}}", "never", None),
        (None, "never", None),
        ("123.123.123.123", "never", "123.123.123.123"),
        ("{{auto}}", "auto", "2.125.160.216"),
        (None, "auto", "2.125.160.216"),
        ("123.123.123.123", "auto", "123.123.123.123"),
        ("{{auto}}", None, "2.125.160.216"),
        (None, None, None),
        ("123.123.123.123", None, "123.123.123.123"),
    ],
)
def test_auto_infer_settings(mini_sentry, relay, user_ip_address, infer_ip, expected):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "user": {"ip_address": user_ip_address},
            "sdk": {"settings": {"infer_ip": infer_ip}},
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


@pytest.mark.parametrize(
    "user_ip_address, infer_ip, expected",
    [
        ("{{auto}}", "never", None),
        (None, "never", None),
        ("123.123.123.123", "never", "123.123.123.123"),
        ("{{auto}}", "auto", "2.125.160.216"),
        (None, "auto", "2.125.160.216"),
        ("123.123.123.123", "auto", "123.123.123.123"),
        ("{{auto}}", None, "2.125.160.216"),
        (None, None, "2.125.160.216"),
        ("123.123.123.123", None, "123.123.123.123"),
    ],
)
def test_auto_infer_setting_objective_c(
    mini_sentry, relay, user_ip_address, infer_ip, expected
):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "platform": "objc",
            "user": {"ip_address": user_ip_address},
            "sdk": {"settings": {"infer_ip": infer_ip}},
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


@pytest.mark.parametrize(
    "platform, infer_ip, expected",
    [
        ("javascript", "never", None),
        ("javascript", "auto", "2.125.160.216"),
        ("javascript", None, None),
        ("cocoa", "never", None),
        ("cocoa", "auto", "2.125.160.216"),
        ("cocoa", None, None),
        ("objc", "never", None),
        ("objc", "auto", "2.125.160.216"),
        ("objc", None, None),
        ("platform", "never", None),
        ("platform", "auto", "2.125.160.216"),
        ("platform", None, None),
    ],
)
def test_auto_infer_without_user(mini_sentry, relay, platform, infer_ip, expected):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "sdk": {"settings": {"infer_ip": infer_ip}},
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


@pytest.mark.parametrize(
    "infer_ip, expected",
    [("auto", "111.222.111.222"), ("never", None), (None, "111.222.111.222")],
)
def test_auto_infer_remote_addr_env(mini_sentry, relay, infer_ip, expected):
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "sdk": {"settings": {"infer_ip": infer_ip}},
            "request": {
                "env": {
                    "REMOTE_ADDR": "111.222.111.222",
                }
            },
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) == expected


def test_auto_infer_invalid_setting(mini_sentry, relay):
    """
    Tests that sending an invalid value for `infer_ip` is treated the same way as sending `never`.
    The behaviour between not sending the value and sending an invalid is different so this needs specific testing.
    """
    project_id = 42
    relay = relay(mini_sentry)
    mini_sentry.add_basic_project_config(project_id)

    relay.send_event(
        project_id,
        {
            "sdk": {
                "settings": {
                    "infer_ip": "invalid",
                }
            },
            "request": {
                "env": {
                    "REMOTE_ADDR": "111.222.111.222",
                }
            },
        },
        headers={"X-Forwarded-For": "2.125.160.216"},
    )

    envelope = mini_sentry.captured_events.get(timeout=1)
    event = envelope.get_event()
    assert event.get("user", {}).get("ip_address", None) is None
