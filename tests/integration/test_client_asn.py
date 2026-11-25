import hmac
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
IP = "1.0.0.1"


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
        attrs[k] = {"type": "integer" if k == NUMBER else "string", "value": v}
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
    enabled: bool = True
    forwarder_only_db: bool = False
    scrub_ips: bool = False
    applications: dict[str, list[str]] | None = field(default_factory=dict)


LOOKUP = EventCase(id="lookup", expected={NUMBER: 15169, ORG: "Google Inc."})
NO_ASN = EventCase(id="no-asn")
SDK_NUMBER = EventCase(id="sdk-number", supplied={NUMBER: 42}, expected={NUMBER: 42})


@pytest.mark.parametrize(
    "case",
    [
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
            applications=None,
            events=(LOOKUP,),
        ),  # Lookup also works without scrubbing rules.
        ProjectCase(
            id="flag-off",
            enabled=False,
            events=(NO_ASN,),
        ),  # Disabling lookup leaves both fields absent.
        ProjectCase(
            id="forwarder-only",
            forwarder_only_db=True,
            events=(LOOKUP,),
        ),  # The forwarder's ASN survives processing without a database.
        ProjectCase(
            id="scrub-ips",
            scrub_ips=True,
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
    ],
    ids=lambda case: case.id,
)
def test_client_asn(
    mini_sentry,
    relay,
    relay_with_processing,
    events_consumer,
    spans_consumer,
    case: ProjectCase,
):
    project = mini_sentry.add_full_project_config(42)
    config = project["config"]
    if case.enabled:
        config.setdefault("features", []).append("projects:relay-client-asn-lookup")
    config.setdefault("datascrubbingSettings", {})["scrubIpAddresses"] = case.scrub_ips
    if case.applications is None:
        del config["piiConfig"]
        config["datascrubbingSettings"]["scrubData"] = False
    else:
        config["piiConfig"]["applications"].update(case.applications)
    events = events_consumer()
    spans = spans_consumer()
    ingress = (
        relay(relay_with_processing(), options=DB)
        if case.forwarder_only_db
        else relay_with_processing(options=DB)
    )

    for event in case.events:
        tx = transaction(IP, event.supplied)
        ingress.send_event(
            42, make_error({"user": {"ip_address": IP}, "tags": event.supplied})
        )
        ingress.send_event(42, tx)
        ingress.send_envelope(42, standalone_span(project, IP, event.supplied))

        error, _ = events.get_event()
        tags = dict(error.get("tags", []))
        expected_tags = {k: str(v) for k, v in event.expected.items()}
        for key in (NUMBER, ORG):
            assert tags.get(key) == expected_tags.get(key), (event.id, key)

        seen = set()
        for _ in range(3):
            span = spans.get_span()
            seen.add(span["span_id"])
            for key in (NUMBER, ORG):
                attr = span["attributes"].get(key) or {}
                assert attr.get("value") == event.expected.get(key), (
                    event.id,
                    span["span_id"],
                    key,
                )
        assert seen == {
            tx["contexts"]["trace"]["span_id"],
            "bbbbbbbbbbbbbbbb",
            "eeeeeeeeeeeeeeee",
        }, event.id


@pytest.mark.parametrize(
    "remove",
    [
        "source",  # The ASN survives removal of its source on the first hop.
        "destination",  # The second hop leaves the removed SDK number absent.
    ],
)
def test_client_asn_preserves_upstream_removal(
    mini_sentry,
    relay,
    relay_with_processing,
    events_consumer,
    spans_consumer,
    remove,
):
    project = mini_sentry.add_full_project_config(42)
    project["config"].setdefault("features", []).append(
        "projects:relay-client-asn-lookup"
    )
    rules = project["config"]["piiConfig"]["applications"]
    keys = {
        "source": [
            "$user.ip_address",
            "$span.data.'client.address'",
            "$span.attributes.'client.address'.value",
        ],
        "destination": [
            "tags.'client.as.number'",
            "$span.data.'client.as.number'",
            "contexts.trace.data.'client.as.number'",
            "$span.attributes.'client.as.number'.value",
        ],
    }[remove]
    rules.update({key: ["@anything:remove"] for key in keys})
    supplied = {NUMBER: 42} if remove == "destination" else {}
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
    expected = LOOKUP.expected if remove == "source" else {}
    for key in (NUMBER, ORG):
        assert tags.get(key) == (str(expected[key]) if key in expected else None)
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
    project["config"].setdefault("features", []).append(
        "projects:relay-client-asn-lookup"
    )
    # This legacy selector applies only to the transaction segment, even though
    # standalone spans use the same convention names for the generated fields.
    project["config"]["piiConfig"]["applications"][
        "contexts.trace.data.'client.as.organization'"
    ] = ["@anything:remove"]
    events = events_consumer()
    spans = spans_consumer()
    processing = relay_with_processing(options=DB)
    headers = {"X-Forwarded-For": IP}

    for addr, number in [
        ("{{auto}}", 15169),  # Resolve the marker from the transport IP.
        ("2600::1", 237),  # Prefer the supplied IPv6 address over the transport IP.
        ("192.0.2.1", None),  # A valid IP missing from the database yields no ASN.
        ("example.invalid", None),  # A hostname cannot supply an IP for lookup.
    ]:
        processing.send_envelope(
            42, standalone_span(project, addr, {}), headers=headers
        )
        attrs = spans.get_span()["attributes"]
        assert (attrs.get(NUMBER) or {}).get("value") == number
        if number is None:
            assert ORG not in attrs
        else:
            assert attrs[ORG]["value"]

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

    processing.send_event(
        42, make_error({"user": {"ip_address": "{{auto}}"}}), headers=headers
    )
    error, _ = events.get_event()
    assert dict(error["tags"])[NUMBER] == "15169"


def test_client_asn_scrubbed_ip_and_indexed_destination(
    mini_sentry, relay_with_processing, events_consumer, spans_consumer
):
    project = mini_sentry.add_full_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append("projects:relay-client-asn-lookup")
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
    number, organization = by_id["dddddddddddddddd"]
    assert number == 237 and organization


@pytest.mark.parametrize(
    "source, remove_remote",
    [
        ("user", True),  # An explicit user IP still supplies a stable hash.
        ("remote", False),  # A retained REMOTE_ADDR supplies the hash on the first hop.
        ("remote", True),  # A removed REMOTE_ADDR supplies ASN but no user ID.
        ("auto", True),  # The marker uses the transport IP for a stable hash.
    ],
)
def test_client_ip_hash_survives_relay_hops(mini_sentry, relay, source, remove_remote):
    project = mini_sentry.add_basic_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append("projects:relay-client-asn-lookup")
    config.setdefault("datascrubbingSettings", {})["scrubIpAddresses"] = remove_remote
    rules = config["piiConfig"]["applications"]
    rules["$user.ip_address"] = ["@ip:hash"]
    if remove_remote:
        rules["$request.env.REMOTE_ADDR"] = ["@anything:remove"]
    event = {
        "user": {"ip_address": IP if source == "user" else "{{auto}}"},
    }
    if source == "remote":
        event = {"request": {"env": {"REMOTE_ADDR": IP}}}

    options = {**DB, "normalization": {"level": "full"}}
    first = relay(mini_sentry, options=options)
    first.send_event(42, event, headers={"X-Forwarded-For": IP})
    envelope = mini_sentry.get_captured_envelope()
    expected_id = hmac.digest(b"", IP.encode(), "sha1").hex().upper()

    def check(payload):
        expected = None if source == "remote" and remove_remote else expected_id
        assert payload.get("user", {}).get("id") == expected
        assert payload.get("user", {}).get("ip_address") is None
        assert (payload.get("request", {}).get("env") or {}).get("REMOTE_ADDR") == (
            IP if source == "remote" and not remove_remote else None
        )
        assert dict(payload["tags"])[NUMBER] == "15169"

    check(envelope.get_event())
    second = relay(mini_sentry, options=options)
    second.send_envelope(42, envelope, headers={"X-Forwarded-For": "2600::1"})
    check(mini_sentry.get_captured_envelope().get_event())


def test_client_ip_backfill_and_span_tags(mini_sentry, relay):
    project = mini_sentry.add_basic_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append("projects:relay-client-asn-lookup")
    config["piiConfig"]["applications"].update(
        {
            "$request.env.REMOTE_ADDR": ["@anything:remove"],
            "$user.ip_address": ["@ip:hash"],
        }
    )
    first = relay(mini_sentry, options={**DB, "normalization": {"level": "full"}})
    second = relay(mini_sentry, options={**DB, "normalization": {"level": "full"}})
    for explicit in (
        False,  # Removing REMOTE_ADDR leaves the marker without an ID source.
        True,  # The explicit user IP still supplies the ID hash.
    ):
        tx = transaction(IP, {})
        tx["request"] = {"env": {"REMOTE_ADDR": IP}}
        tx["spans"][0]["data"] = {}
        tx["user"] = {"ip_address": IP if explicit else "{{auto}}"}
        first.send_event(42, tx)
        envelope = mini_sentry.get_captured_envelope()
        for hop in (
            1,  # Check identity and span tags after initial scrubbing.
            2,  # A different transport IP must preserve the first hop's result.
        ):
            event = next(
                item.payload.json
                for item in envelope.items
                if item.type == "transaction"
            )
            assert (event.get("request", {}).get("env") or {}).get(
                "REMOTE_ADDR"
            ) is None
            user = event.get("user") or {}
            assert user.get("ip_address") is None
            expected_id = (
                hmac.digest(b"", IP.encode(), "sha1").hex().upper()
                if explicit
                else None
            )
            assert user.get("id") == expected_id
            assert event["contexts"]["trace"]["data"][NUMBER] == 15169
            tags = event["spans"][0].get("sentry_tags") or {}
            assert tags.get("user.ip") is None
            assert tags.get("user") != f"ip:{IP}"
            if hop == 1:
                second.send_envelope(
                    42, envelope, headers={"X-Forwarded-For": "2600::1"}
                )
                envelope = mini_sentry.get_captured_envelope()


def test_remote_addr_identity_and_lookup(mini_sentry, relay):
    # Both fixture databases cover this address. Use a different transport IP so
    # a lookup against the envelope cannot accidentally satisfy the assertions.
    ip = "89.160.20.112"
    project = mini_sentry.add_basic_project_config(42)
    config = project["config"]
    config.setdefault("features", []).append("projects:relay-client-asn-lookup")
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
    for supplied, identity, expected_id in [
        (False, None, None),  # A City hit creates the missing user.
        (False, {}, None),  # An existing empty user receives the city.
        (True, {}, hashed),  # The explicit user IP supplies the fallback ID.
        (True, {"id": "sdk-id"}, "sdk-id"),  # Keep the SDK's ID over the IP hash.
        (True, {"email": "client@example.org"}, None),  # Email takes priority.
        (True, {"username": "sdk-user"}, None),  # Username takes priority.
    ]:
        event = {"request": {"env": {"REMOTE_ADDR": ip}}}
        if identity is not None:
            event["user"] = identity.copy()
            if supplied:
                event["user"]["ip_address"] = ip
        first.send_event(42, event, headers={"X-Forwarded-For": "1.0.0.1"})

        def check(event):
            assert event["user"]["geo"]["city"] == "Linköping"
            assert dict(event["tags"])[NUMBER] == "29518"
            assert (event["request"].get("env") or {}).get("REMOTE_ADDR") is None
            assert event["user"].get("ip_address") != ip
            assert event["user"].get("id") == expected_id
            for key, value in (identity or {}).items():
                assert event["user"][key] == value

        envelope = mini_sentry.get_captured_envelope()
        check(envelope.get_event())
        second.send_envelope(42, envelope, headers={"X-Forwarded-For": "2600::1"})
        check(mini_sentry.get_captured_envelope().get_event())
