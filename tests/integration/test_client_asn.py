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
                LOOKUP,
                SDK_NUMBER,
                EventCase(
                    id="sdk-organization",
                    supplied={ORG: "SDK value"},
                    expected={ORG: "SDK value"},
                ),
            ),
        ),
        ProjectCase(id="without-pii", applications=None, events=(LOOKUP,)),
        ProjectCase(id="flag-off", enabled=False, events=(NO_ASN,)),
        ProjectCase(id="forwarder-only", forwarder_only_db=True, events=(NO_ASN,)),
        ProjectCase(id="scrub-ips", scrub_ips=True, events=(NO_ASN, SDK_NUMBER)),
        ProjectCase(
            id="remove-source",
            applications={
                "$user.ip_address": ["@anything:remove"],
                "$span.data.'client.address'": ["@anything:remove"],
                "$span.attributes.'client.address'.value": ["@anything:remove"],
            },
            events=(NO_ASN,),
        ),
        ProjectCase(
            id="remove-destination",
            applications={
                "tags.'client.as.organization'": ["@anything:remove"],
                "$span.attributes.'client.as.organization'.value": ["@anything:remove"],
            },
            events=(EventCase(id="number-only", expected={NUMBER: 15169}),),
        ),
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


@pytest.mark.parametrize("remove", ["source", "destination"])
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
    for envelope in (error, tx, standalone):
        processing.send_envelope(42, envelope, headers={"X-Forwarded-For": IP})

    error, _ = events.get_event()
    tags = dict(error.get("tags", []))
    assert tags.get(NUMBER) is None
    assert tags.get(ORG) is None
    for _ in range(3):
        span = spans.get_span()
        for key in (NUMBER, ORG):
            assert (span["attributes"].get(key) or {}).get("value") is None, span[
                "span_id"
            ]


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
        ("{{auto}}", 15169),
        ("2600::1", 237),
        ("192.0.2.1", None),
        ("example.invalid", None),
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
