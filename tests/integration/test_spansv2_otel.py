from datetime import datetime, timezone

import pytest
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue,
    InstrumentationScope,
    KeyValue,
)
from opentelemetry.proto.resource.v1.resource_pb2 import Resource
from opentelemetry.proto.trace.v1.trace_pb2 import (
    ResourceSpans,
    ScopeSpans,
    Span,
    SpanFlags,
    TracesData,
)

from .asserts import time_within_delta, time_within


@pytest.mark.parametrize(
    "span_v2_feature",
    [
        "projects:span-v2-experimental-processing",
        "organizations:span-v2-otlp-processing",
    ],
)
def test_span_ingestion(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    span_v2_feature,
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()

    relay = relay(relay_with_processing())

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["features"] = [
        "organizations:standalone-span-ingestion",
        "organizations:relay-otlp-traces-endpoint",
        span_v2_feature,
    ]

    ts = datetime.now(timezone.utc)

    span = Span(
        trace_id=bytes.fromhex("89143b0763095bd9c9955e8175d1fb24"),
        span_id=bytes.fromhex("f0b809703e783d00"),
        parent_span_id=bytes.fromhex("f0f0f0abcdef1234"),
        name="A Proto Span",
        start_time_unix_nano=int((ts.timestamp() - 1.0) * 1e9),
        end_time_unix_nano=int((ts.timestamp() - 0.5) * 1e9),
        flags=SpanFlags.SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK
        | SpanFlags.SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK,
        kind=Span.SPAN_KIND_SERVER,
        attributes=[
            KeyValue(
                key="ui.component_name",
                value=AnyValue(string_value="MyComponent"),
            ),
        ],
        links=[
            Span.Link(
                trace_id=bytes.fromhex("89143b0763095bd9c9955e8175d1fb24"),
                span_id=bytes.fromhex("e0b809703e783d01"),
                attributes=[
                    KeyValue(
                        key="link_str_key",
                        value=AnyValue(string_value="link_str_value"),
                    )
                ],
            )
        ],
    )
    scope_spans = ScopeSpans(
        spans=[span], scope=InstrumentationScope(name="my_scope_name", version="13.37")
    )
    resource_spans = ResourceSpans(
        scope_spans=[scope_spans],
        resource=Resource(
            attributes=[
                KeyValue(
                    key="company",
                    value=AnyValue(string_value="Relay Corp"),
                ),
            ]
        ),
    )

    relay.send_otel_span(
        project_id,
        bytes=TracesData(resource_spans=[resource_spans]).SerializeToString(),
        headers={"Content-Type": "application/x-protobuf"},
    )

    assert spans_consumer.get_span() == {
        "attributes": {
            "instrumentation.name": {"type": "string", "value": "my_scope_name"},
            "instrumentation.version": {"type": "string", "value": "13.37"},
            "resource.company": {"type": "string", "value": "Relay Corp"},
            "sentry.browser.name": {"type": "string", "value": "Python Requests"},
            "sentry.browser.version": {"type": "string", "value": "2.32"},
            "sentry.is_remote": {"type": "boolean", "value": True},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
            "sentry.origin": {"type": "string", "value": "auto.otlp.spans"},
            "sentry.kind": {"type": "string", "value": "server"},
            "ui.component_name": {"type": "string", "value": "MyComponent"},
        },
        "downsampled_retention_days": 90,
        "end_timestamp": time_within(ts.timestamp() - 0.5),
        "is_segment": True,
        "key_id": 123,
        "links": [
            {
                "attributes": {
                    "link_str_key": {"type": "string", "value": "link_str_value"}
                },
                "sampled": False,
                "span_id": "e0b809703e783d01",
                "trace_id": "89143b0763095bd9c9955e8175d1fb24",
            }
        ],
        "name": "A Proto Span",
        "organization_id": 1,
        "parent_span_id": "f0f0f0abcdef1234",
        "project_id": 42,
        "received": time_within(ts),
        "retention_days": 90,
        "span_id": "f0b809703e783d00",
        "start_timestamp": time_within(ts.timestamp() - 1.0),
        "status": "ok",
        "trace_id": "89143b0763095bd9c9955e8175d1fb24",
    }

    assert metrics_consumer.get_metrics(n=2, with_headers=False) == [
        {
            "name": "c:spans/count_per_root_project@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {
                "decision": "keep",
                "is_segment": "true",
                "target_project_id": "42",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
        {
            "name": "c:spans/usage@none",
            "org_id": 1,
            "project_id": 42,
            "received_at": time_within_delta(),
            "retention_days": 90,
            "tags": {"is_segment": "true", "was_transaction": "false"},
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]
