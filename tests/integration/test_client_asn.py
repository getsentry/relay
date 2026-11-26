import hmac
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

from .test_spansv2 import envelope_with_spans
from .test_store import make_error, make_transaction

DB = {
    "geoip": {
        "asn_path": str(
            Path(__file__).resolve().parents[1] / "fixtures/GeoLite2-ASN-Test.mmdb"
        )
    }
}
NUMBER = "client.as.number"
ORG = "client.as.organization"
ASN_TYPES = {NUMBER: "integer", ORG: "string"}
IP = "1.0.0.1"
IP_HASH = hmac.digest(b"", IP.encode(), "sha1").hex().upper()
ASN_FEATURE = "projects:relay-client-asn-lookup"
HASH_USER_IP = {"$user.ip_address": ["@ip:hash"]}
HASH_AND_REMOVE_REMOTE = {
    **HASH_USER_IP,
    "$request.env.REMOTE_ADDR": ["@anything:remove"],
}


def transaction(ip, supplied):
    tx = make_transaction({"user": {"ip_address": ip}})
    trace = tx["contexts"]["trace"]
    trace["data"] = supplied
    tx["spans"] = [
        {
            "span_id": "bbbbbbbbbbbbbbbb",
            "trace_id": trace["trace_id"],
            "parent_span_id": trace["span_id"],
            "start_timestamp": tx["start_timestamp"],
            "timestamp": tx["timestamp"],
            "op": "http.server",
            "data": {"client.address": ip, **supplied},
        }
    ]
    return tx


def standalone_span(project, ip, supplied):
    trace_id = uuid4().hex
    now = datetime.now(UTC).timestamp()
    attrs = {"client.address": {"type": "string", "value": ip}}
    for k, v in supplied.items():
        attrs[k] = {"type": ASN_TYPES[k], "value": v}
    return envelope_with_spans(
        {
            "trace_id": trace_id,
            "span_id": "eeeeeeeeeeeeeeee",
            "is_segment": True,
            "name": "request",
            "status": "ok",
            "start_timestamp": now - 1,
            "end_timestamp": now,
            "attributes": attrs,
        },
        trace_info={
            "trace_id": trace_id,
            "public_key": project["publicKeys"][0]["publicKey"],
        },
    )


@dataclass(frozen=True, kw_only=True)
class EventCase:
    id: str
    supplied: dict[str, str | int] = field(default_factory=dict)
    expected: dict[str, str | int] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class ProjectCase:
    id: str
    events: tuple[EventCase, ...]
    config: dict = field(
        default_factory=lambda: {
            "features": [ASN_FEATURE],
            "datascrubbingSettings": {"scrubIpAddresses": False},
        }
    )
    applications: dict[str, list[str]] = field(default_factory=dict)
    omit_config: tuple[str, ...] = ()


LOOKUP = EventCase(id="lookup", expected={NUMBER: 15169, ORG: "Google Inc."})
NO_ASN = EventCase(id="no-asn")
SDK_NUMBER = EventCase(id="sdk-number", supplied={NUMBER: 42}, expected={NUMBER: 42})


PROCESSING_PROJECTS = (
    ProjectCase(
        id="enabled",
        events=(
            LOOKUP,  # Fill both ASN fields from the IP.
            SDK_NUMBER,  # Keep the SDK number and leave the organization absent.
            EventCase(
                id="sdk-organization",
                supplied={ORG: "SDK value"},
                expected={ORG: "SDK value"},
            ),  # Keep the SDK organization and leave the number absent.
        ),
    ),  # Fill missing ASN data and preserve SDK-supplied fields.
    ProjectCase(
        id="without-pii",
        config={
            "features": [ASN_FEATURE],
            "datascrubbingSettings": {
                "scrubIpAddresses": False,
                "scrubData": False,
            },
        },
        omit_config=("piiConfig",),
        events=(LOOKUP,),
    ),  # Lookup also works without scrubbing rules.
    ProjectCase(
        id="flag-off",
        config={"datascrubbingSettings": {"scrubIpAddresses": False}},
        events=(NO_ASN,),
    ),  # Disabling lookup leaves both fields absent.
    ProjectCase(
        id="scrub-ips",
        config={
            "features": [ASN_FEATURE],
            "datascrubbingSettings": {"scrubIpAddresses": True},
        },
        events=(LOOKUP, SDK_NUMBER),
    ),  # Disabling IP storage preserves both derived and supplied ASN data.
    ProjectCase(
        id="remove-source",
        applications={
            "$user.ip_address": ["@anything:remove"],
            "$span.data.'client.address'": ["@anything:remove"],
            "$span.attributes.'client.address'.value": ["@anything:remove"],
        },
        events=(LOOKUP,),
    ),  # Removing the source IP preserves the derived ASN.
    ProjectCase(
        id="remove-destination",
        applications={
            "tags.'client.as.organization'": ["@anything:remove"],
            "$span.attributes.'client.as.organization'.value": ["@anything:remove"],
        },
        events=(EventCase(id="number-only", expected={NUMBER: 15169}),),
    ),  # Scrubbing the organization preserves the ASN number.
)


@pytest.mark.parametrize(
    "processing_options, forwarders, projects",
    [
        pytest.param(
            DB, (), PROCESSING_PROJECTS, id="processing"
        ),  # The processor owns the database.
        pytest.param(
            {},
            (DB,),
            (ProjectCase(id="forwarder-only", events=(LOOKUP,)),),
            id="forwarder-only",
        ),  # The forwarder's ASN survives processing without a database.
    ],
)
def test_client_asn(
    mini_sentry,
    relay,
    relay_with_processing,
    events_consumer,
    spans_consumer,
    processing_options,
    forwarders,
    projects,
):
    events = events_consumer()
    spans = spans_consumer()
    ingress = relay_with_processing(options=processing_options)
    for options in forwarders:
        ingress = relay(ingress, options=options)

    for project_id, case in enumerate(projects, start=42):
        project = mini_sentry.add_full_project_config(
            project_id, extra={"config": deepcopy(case.config)}
        )
        config = project["config"]
        config["piiConfig"]["applications"].update(case.applications)
        for key in case.omit_config:
            del config[key]
        for event in case.events:
            tx = transaction(IP, event.supplied)
            ingress.send_event(
                project_id,
                make_error(
                    {
                        "user": {"ip_address": IP},
                        "tags": event.supplied,
                        "contexts": {
                            "trace": {**tx["contexts"]["trace"], "data": {NUMBER: 123}}
                        },
                    }
                ),
            )
            ingress.send_event(project_id, tx)
            ingress.send_envelope(
                project_id, standalone_span(project, IP, event.supplied)
            )

            error, _ = events.get_event()
            assert error["contexts"]["trace"]["data"][NUMBER] == 123
            tags = dict(error.get("tags", []))
            expected_tags = {k: str(v) for k, v in event.expected.items()}
            for key in (NUMBER, ORG):
                assert tags.get(key) == expected_tags.get(key), (case.id, event.id, key)

            seen = set()
            for _ in range(3):
                span = spans.get_span()
                seen.add(span["span_id"])
                for key in (NUMBER, ORG):
                    attr = span["attributes"].get(key) or {}
                    assert attr.get("value") == event.expected.get(key), (
                        case.id,
                        event.id,
                        span["span_id"],
                        key,
                    )
            assert seen == {
                tx["contexts"]["trace"]["span_id"],
                "bbbbbbbbbbbbbbbb",
                "eeeeeeeeeeeeeeee",
            }, (case.id, event.id)


@pytest.mark.parametrize(
    "keys, case",
    [
        pytest.param(
            (
                "$user.ip_address",
                "$span.data.'client.address'",
                "$span.attributes.'client.address'.value",
            ),
            LOOKUP,
            id="source",
        ),  # The ASN survives removal of its source on the first hop.
        pytest.param(
            (
                "tags.'client.as.number'",
                "$span.data.'client.as.number'",
                "contexts.trace.data.'client.as.number'",
                "$span.attributes.'client.as.number'.value",
            ),
            EventCase(id="removed-sdk-number", supplied={NUMBER: 42}, expected={}),
            id="destination",
        ),  # The second hop leaves the removed SDK number absent.
    ],
)
def test_client_asn_preserves_upstream_removal(
    mini_sentry,
    relay,
    relay_with_processing,
    events_consumer,
    spans_consumer,
    keys,
    case,
):
    project = mini_sentry.add_full_project_config(42)
    project["config"].setdefault("features", []).append(ASN_FEATURE)
    rules = project["config"]["piiConfig"]["applications"]
    rules.update({key: ["@anything:remove"] for key in keys})
    supplied = case.supplied
    events = events_consumer()
    spans = spans_consumer()
    forwarder = relay(mini_sentry, options=DB)

    forwarder.send_event(42, make_error({"user": {"ip_address": IP}, "tags": supplied}))
    error = mini_sentry.get_captured_envelope()
    forwarder.send_event(42, transaction(IP, supplied))
    tx = mini_sentry.get_captured_envelope()
    forwarder.send_envelope(42, standalone_span(project, IP, supplied))
    standalone = mini_sentry.get_captured_envelope()

    # The final Relay sees the actual serialized first-hop payload with the
    # removal remarks, but has no access to the first hop's redaction rules.
    for key in keys:
        del rules[key]
    processing = relay_with_processing(options=DB)
    for envelope in (
        error,  # Check redaction across hops for error tags.
        tx,  # Check the transaction segment and its child span.
        standalone,  # Check native span attributes across hops.
    ):
        processing.send_envelope(42, envelope, headers={"X-Forwarded-For": IP})

    error, _ = events.get_event()
    tags = dict(error.get("tags", []))
    expected = case.expected
    expected_tags = {k: str(v) for k, v in expected.items()}
    for key in (NUMBER, ORG):
        assert tags.get(key) == expected_tags.get(key)
    for _ in range(3):
        span = spans.get_span()
        for key in (NUMBER, ORG):
            assert (span["attributes"].get(key) or {}).get("value") == expected.get(
                key
            ), span["span_id"]


def test_client_asn_address_selection(
    mini_sentry, relay_with_processing, events_consumer, spans_consumer
):
    project = mini_sentry.add_full_project_config(42)
    project["config"].setdefault("features", []).append(ASN_FEATURE)
    # This legacy selector applies only to the transaction segment, even though
    # standalone spans use the same convention names for the generated fields.
    project["config"]["piiConfig"]["applications"][
        "contexts.trace.data.'client.as.organization'"
    ] = ["@anything:remove"]
    events = events_consumer()
    spans = spans_consumer()
    processing = relay_with_processing(options=DB)
    headers = {"X-Forwarded-For": IP}

    for addr, expected in [
        (
            "{{auto}}",
            {NUMBER: 15169, ORG: "Google Inc."},
        ),  # Resolve the marker from the transport IP.
        (
            "2600::1",
            {NUMBER: 237, ORG: "Merit Network Inc."},
        ),  # Prefer the supplied IPv6 address over the transport IP.
        ("192.0.2.1", {}),  # A valid IP missing from the database yields no ASN.
        ("example.invalid", {}),  # A hostname cannot supply an IP for lookup.
    ]:
        processing.send_envelope(
            42, standalone_span(project, addr, {}), headers=headers
        )
        attrs = spans.get_span()["attributes"]
        assert {
            key: attrs[key]["value"] for key in attrs.keys() & {NUMBER, ORG}
        } == expected

    # A transaction's user IP belongs to its segment. Don't fan it out to
    # every child from the copy of user.ip made during tag extraction.
    tx = transaction("{{auto}}", {})
    tx["spans"][0]["data"] = {}
    tx["spans"].append(
        {
            **tx["spans"][0],
            "span_id": "cccccccccccccccc",
            "tags": {"client.address": "2600::1"},
        }
    )
    processing.send_event(42, tx, headers=headers)
    results = {
        s["span_id"]: s["attributes"]
        for s in (spans.get_span(), spans.get_span(), spans.get_span())
    }
    assert results[tx["contexts"]["trace"]["span_id"]][NUMBER]["value"] == 15169
    assert (results[tx["contexts"]["trace"]["span_id"]].get(ORG) or {}).get(
        "value"
    ) is None
    assert results["cccccccccccccccc"][NUMBER]["value"] == 237
    assert NUMBER not in results["bbbbbbbbbbbbbbbb"]
    assert ORG not in results["bbbbbbbbbbbbbbbb"]

    # The segment and child explicitly request the connection IP, distinct from the user IP.
    tx = transaction("2600::1", {})
    tx["contexts"]["trace"]["data"]["client.address"] = "{{auto}}"
    tx["spans"][0]["data"] = {}
    tx["spans"][0]["tags"] = {"user.ip_address": "{{auto}}"}
    processing.send_event(42, tx, headers=headers)
    for _ in range(2):
        span = spans.get_span()
        key = "client.address" if span["is_segment"] else "user.ip_address"
        assert span["attributes"][key]["value"] == IP
        assert span["attributes"][NUMBER]["value"] == 15169

    processing.send_event(
        42, make_error({"user": {"ip_address": "{{auto}}"}}), headers=headers
    )
    error, _ = events.get_event()
    assert dict(error["tags"])[NUMBER] == "15169"

    # The marker resolves on the request while the explicit user IP supplies the ASN.
    event = make_error(
        {
            "request": {"env": {"REMOTE_ADDR": "{{auto}}"}},
            "user": {"ip_address": "2600::1"},
        }
    )
    processing.send_event(42, event, headers=headers)
    error, _ = events.get_event()
    assert error["request"]["env"]["REMOTE_ADDR"] == IP
    assert error["user"]["ip_address"] == "2600::1"
    assert dict(error["tags"])[NUMBER] == "237"


def test_client_asn_scrubbed_ip_and_indexed_destination(
    mini_sentry, relay_with_processing, events_consumer, spans_consumer
):
    project = mini_sentry.add_full_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append(ASN_FEATURE)
    pii = config["piiConfig"]
    pii["rules"]["replace-ip"] = {
        "type": "pattern",
        "pattern": r"^1\.0\.0\.1$",
        "redaction": {"method": "replace", "text": "2600::1"},
    }
    pii["applications"].update(
        {
            "$request.env.REMOTE_ADDR": ["@anything:remove"],
            "$user.ip_address": ["replace-ip"],
            "$span.attributes.'client.address'.value": ["replace-ip"],
            "spans.1.data.'client.as.organization'": ["@anything:remove"],
        }
    )
    events = events_consumer()
    spans = spans_consumer()
    processing = relay_with_processing(options=DB)

    # Both IPs match the fixture. Lookup uses the input, before substitution.
    processing.send_event(42, make_error({"user": {"ip_address": IP}}))
    error, _ = events.get_event()
    assert error["user"]["ip_address"] == "2600::1"
    assert dict(error["tags"])[NUMBER] == "15169"

    # The ASN survives while both the source and its raw copy are removed.
    processing.send_event(
        42, make_error({"request": {"env": {"REMOTE_ADDR": "2600::1"}}})
    )
    error, _ = events.get_event()
    assert error["request"]["env"]["REMOTE_ADDR"] is None
    assert (error.get("user") or {}).get("ip_address") is None
    assert dict(error["tags"])[NUMBER] == "237"

    tx = transaction(IP, {})
    first = tx["spans"][0]
    for span_id in (
        "cccccccccccccccc",  # The rule removes the organization from spans[1].
        "dddddddddddddddd",  # The same address keeps its organization on spans[2].
    ):
        tx["spans"].append(
            {**first, "span_id": span_id, "data": {"client.address": "2600::1"}}
        )
    processing.send_event(42, tx)
    processing.send_envelope(42, standalone_span(project, IP, {}))
    by_id = {}
    for _ in range(5):
        span = spans.get_span()
        attrs = span["attributes"]
        by_id[span["span_id"]] = tuple(
            (attrs.get(k) or {}).get("value") for k in (NUMBER, ORG)
        )
    assert by_id[tx["contexts"]["trace"]["span_id"]] == (15169, "Google Inc.")
    assert by_id[first["span_id"]] == (15169, "Google Inc.")
    assert by_id["eeeeeeeeeeeeeeee"] == (15169, "Google Inc.")
    assert by_id["cccccccccccccccc"] == (237, None)
    assert by_id["dddddddddddddddd"] == (237, "Merit Network Inc.")


@dataclass(frozen=True, kw_only=True)
class HashCase:
    id: str
    event: dict
    expected_id: str | None
    expected_remote: str | None


@pytest.mark.parametrize(
    "applications, settings, cases",
    [
        pytest.param(
            HASH_AND_REMOVE_REMOTE,
            {"scrubIpAddresses": True},
            (
                HashCase(
                    id="explicit",
                    event={"user": {"ip_address": IP}},
                    expected_id=IP_HASH,
                    expected_remote=None,
                ),  # The explicit IP supplies the hash.
                HashCase(
                    id="removed-remote",
                    event={"request": {"env": {"REMOTE_ADDR": IP}}},
                    expected_id=None,
                    expected_remote=None,
                ),  # The removed address supplies only ASN.
                HashCase(
                    id="auto",
                    event={"user": {"ip_address": "{{auto}}"}},
                    expected_id=IP_HASH,
                    expected_remote=None,
                ),  # The marker hashes the transport IP.
            ),
            id="remove-remote",
        ),  # Remove the request IP and disable IP storage.
        pytest.param(
            HASH_USER_IP,
            {"scrubIpAddresses": False},
            (
                HashCase(
                    id="retained-remote",
                    event={"request": {"env": {"REMOTE_ADDR": IP}}},
                    expected_id=IP_HASH,
                    expected_remote=IP,
                ),  # The retained address supplies the hash.
            ),
            id="keep-remote",
        ),  # Keep the request IP and hash the user IP.
    ],
)
def test_client_ip_hash_survives_relay_hops(
    mini_sentry, relay, applications, settings, cases
):
    project = mini_sentry.add_basic_project_config(
        42,
        extra={
            "config": {"features": [ASN_FEATURE], "datascrubbingSettings": settings}
        },
    )
    project["config"]["piiConfig"]["applications"].update(applications)

    options = {**DB, "normalization": {"level": "full"}}
    first = relay(mini_sentry, options=options)
    second = relay(mini_sentry, options=options)

    def check(payload, case):
        assert payload.get("user", {}).get("id") == case.expected_id, case.id
        assert payload.get("user", {}).get("ip_address") is None, case.id
        assert (payload.get("request", {}).get("env") or {}).get(
            "REMOTE_ADDR"
        ) == case.expected_remote, case.id
        assert dict(payload["tags"])[NUMBER] == "15169", case.id

    for case in cases:
        first.send_event(42, case.event, headers={"X-Forwarded-For": IP})
        envelope = mini_sentry.get_captured_envelope()
        check(envelope.get_event(), case)
        second.send_envelope(42, envelope, headers={"X-Forwarded-For": "2600::1"})
        check(mini_sentry.get_captured_envelope().get_event(), case)


def test_transaction_user_skips_placeholders(mini_sentry, relay):
    project = mini_sentry.add_basic_project_config(42)
    rules = project["config"]["piiConfig"]["applications"]
    rules.update({"$user.ip_address": ["@ip"], "$user.email": ["@email"]})
    ingress = relay(mini_sentry, options={"normalization": {"level": "full"}})
    for fields, expected_tag in [
        ({}, None),
        ({"email": "alice@example.org"}, None),
        ({"email": "alice@example.org", "id": "alice"}, "id:alice"),
    ]:
        tx = transaction(IP, {})
        tx["user"].update(fields)
        ingress.send_event(42, tx)
        event = mini_sentry.get_captured_envelope().get_transaction_event()
        assert (event.get("user") or {}).get("id") == fields.get("id")
        if "email" in fields:
            assert event["user"]["email"] == "[email]"
        tags = event["spans"][0].get("sentry_tags") or {}
        assert tags.get("user") == expected_tag


def test_mask_ip_hash_at_user_id(mini_sentry, relay):
    project = mini_sentry.add_basic_project_config(42)
    project["config"]["piiConfig"]["applications"].update(
        {**HASH_USER_IP, "$user.id": ["@anything:mask"]}
    )
    ingress = relay(mini_sentry, options={"normalization": {"level": "full"}})
    for supplied, expected in [({}, IP_HASH), ({"id": IP_HASH}, "*" * len(IP_HASH))]:
        tx = transaction(IP, {})
        tx["user"].update(supplied)
        ingress.send_event(42, tx)
        event = mini_sentry.get_captured_envelope().get_transaction_event()
        assert event["user"]["id"] == expected
        assert event["spans"][0]["sentry_tags"]["user"] == f"id:{expected}"


def test_client_ip_backfill_and_span_tags(mini_sentry, relay):
    project = mini_sentry.add_basic_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append(ASN_FEATURE)
    config["piiConfig"]["applications"].update(
        {
            "$request.env.REMOTE_ADDR": ["@anything:remove"],
            "$user.ip_address": ["@ip:hash"],
        }
    )
    first = relay(mini_sentry, options={**DB, "normalization": {"level": "full"}})
    second = relay(mini_sentry, options={**DB, "normalization": {"level": "full"}})

    def check(envelope, expected_id):
        event = next(
            item.payload.json for item in envelope.items if item.type == "transaction"
        )
        assert (event.get("request", {}).get("env") or {}).get("REMOTE_ADDR") is None
        user = event.get("user") or {}
        assert user.get("ip_address") is None
        assert user.get("id") == expected_id
        assert event["contexts"]["trace"]["data"][NUMBER] == 15169
        tags = event["spans"][0].get("sentry_tags") or {}
        assert tags.get("user.ip") is None
        assert tags.get("user") == (f"id:{expected_id}" if expected_id else None)

    for address, expected_id in (
        (
            "{{auto}}",
            None,
        ),  # Scrubbing REMOTE_ADDR leaves the marker without an ID source.
        (IP, IP_HASH),  # The explicit user IP supplies the hash.
    ):
        tx = transaction(address, {})
        tx["request"] = {"env": {"REMOTE_ADDR": IP}}
        tx["spans"][0]["data"] = {}
        first.send_event(42, tx)
        envelope = mini_sentry.get_captured_envelope()
        check(envelope, expected_id)
        second.send_envelope(42, envelope, headers={"X-Forwarded-For": "2600::1"})
        check(mini_sentry.get_captured_envelope(), expected_id)


def test_remote_addr_identity_and_lookup(mini_sentry, relay):
    # Both fixture databases cover this address. Use a different transport IP so
    # a lookup against the envelope cannot accidentally satisfy the assertions.
    ip = "89.160.20.112"
    project = mini_sentry.add_basic_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append(ASN_FEATURE)
    config["piiConfig"]["applications"] = {
        "$request.env.REMOTE_ADDR": ["@anything:remove"],
        "$user.ip_address": ["@ip:hash"],
    }
    config.setdefault("datascrubbingSettings", {})["scrubData"] = False
    options = {
        "normalization": {"level": "full"},
        "geoip": {
            **DB["geoip"],
            "city_path": str(
                Path(__file__).resolve().parents[1]
                / "fixtures/GeoIP2-Enterprise-Test.mmdb"
            ),
        },
    }
    first = relay(mini_sentry, options=options)
    second = relay(mini_sentry, options=options)
    hashed = hmac.digest(b"", ip.encode(), "sha1").hex().upper()

    def check(event, expected_city, expected_identity):
        user = event["user"]
        assert user["geo"]["city"] == expected_city
        assert dict(event["tags"])[NUMBER] == "29518"
        assert (event["request"].get("env") or {}).get("REMOTE_ADDR") is None
        assert user.get("ip_address") != ip
        assert {k: user.get(k) for k in expected_identity} == expected_identity

    for fields, expected_city, expected_identity in [
        ({}, "Linköping", {"id": None}),  # A City hit creates the missing user.
        (
            {"user": {}},
            "Linköping",
            {"id": None},
        ),  # An existing empty user receives the city.
        (
            {"user": {"geo": {"city": "SDK city"}}},
            "SDK city",
            {"id": None},
        ),  # Preserve the SDK city while deriving ASN from REMOTE_ADDR.
        (
            {"user": {"ip_address": ip}},
            "Linköping",
            {"id": hashed},
        ),  # The explicit IP supplies the hash.
        (
            {"user": {"ip_address": ip, "id": "sdk-id"}},
            "Linköping",
            {"id": "sdk-id"},
        ),  # SDK ID wins.
        (
            {"user": {"ip_address": ip, "email": "client@example.org"}},
            "Linköping",
            {"id": None, "email": "client@example.org"},
        ),  # Email takes priority.
        (
            {"user": {"ip_address": ip, "username": "sdk-user"}},
            "Linköping",
            {"id": None, "username": "sdk-user"},
        ),  # Username takes priority.
    ]:
        event = {"request": {"env": {"REMOTE_ADDR": ip}}, **fields}
        first.send_event(42, event, headers={"X-Forwarded-For": "1.0.0.1"})
        envelope = mini_sentry.get_captured_envelope()
        check(envelope.get_event(), expected_city, expected_identity)
        second.send_envelope(42, envelope, headers={"X-Forwarded-For": "2600::1"})
        check(
            mini_sentry.get_captured_envelope().get_event(),
            expected_city,
            expected_identity,
        )
