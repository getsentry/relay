from datetime import datetime, timezone

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
)
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
from sentry_relay.consts import DataCategory

from .asserts import time_within_delta, time_within

GRPC_INVALID_ARGUMENT = 3
GRPC_RESOURCE_EXHAUSTED = 8
GRPC_UNAUTHENTICATED = 16


def parse_google_rpc_status(data):
    file_descriptor = descriptor_pb2.FileDescriptorProto(
        name="google/rpc/status.proto",
        package="google.rpc",
        message_type=[
            descriptor_pb2.DescriptorProto(
                name="Status",
                field=[
                    descriptor_pb2.FieldDescriptorProto(
                        name="code",
                        number=1,
                        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
                        type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
                    ),
                    descriptor_pb2.FieldDescriptorProto(
                        name="message",
                        number=2,
                        label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL,
                        type=descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
                    ),
                ],
            )
        ],
    )
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_descriptor)
    status_cls = message_factory.GetMessageClass(
        pool.FindMessageTypeByName("google.rpc.Status")
    )
    status = status_cls()
    status.ParseFromString(data)
    return status


def warm_project_cache(relay, mini_sentry, project_id):
    relay.send_event(project_id, {"message": "warm project cache"})

    while True:
        event = mini_sentry.get_captured_envelope().get_event()
        if event.get("logentry", {}).get("formatted") == "warm project cache":
            break


def test_otlp_traces_protobuf_success_response(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.send_otel_span(
        project_id,
        headers={"Content-Type": "application/x-protobuf"},
        bytes=TracesData().SerializeToString(),
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-protobuf"
    assert ExportTraceServiceResponse.FromString(response.content) == (
        ExportTraceServiceResponse()
    )


def test_otlp_traces_unsupported_media_type_returns_status(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.send_otel_span(
        project_id,
        headers={"Content-Type": "text/plain"},
        bytes=b"",
        raise_for_status=False,
    )

    assert response.status_code == 415
    assert response.headers["content-type"] == "application/json"
    assert response.json()["code"] == GRPC_INVALID_ARGUMENT


def test_otlp_traces_missing_auth_returns_protobuf_status(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry)

    response = relay.post(
        f"/api/{project_id}/integration/otlp/v1/traces",
        headers={"Content-Type": "application/x-protobuf"},
        data=TracesData().SerializeToString(),
    )

    assert response.status_code == 401
    assert response.headers["content-type"] == "application/x-protobuf"
    status = parse_google_rpc_status(response.content)
    assert status.code == GRPC_UNAUTHENTICATED
    assert status.message == "missing authorization information"


def test_otlp_traces_rate_limit_returns_status_and_retry_headers(mini_sentry, relay):
    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"]["quotas"] = [
        {
            "id": "test_otlp_traces_rate_limit",
            "categories": ["span"],
            "limit": 0,
            "window": 60,
            "reasonCode": "otlp_rate_limited",
        }
    ]
    relay = relay(mini_sentry)

    # Make sure project cache is loaded:
    relay.send_event(project_id, {"message": "warm project cache"})
    _ = mini_sentry.get_captured_envelope().get_event()

    response = relay.send_otel_span(
        project_id,
        headers={"Content-Type": "application/json"},
        json={"resourceSpans": []},
        raise_for_status=False,
    )

    assert response.status_code == 429
    assert response.headers["retry-after"]
    assert response.headers["x-sentry-rate-limits"]
    assert response.headers["content-type"] == "application/json"
    assert response.json()["code"] == GRPC_RESOURCE_EXHAUSTED


def test_otlp_traces_oversized_body_returns_status(mini_sentry, relay):
    project_id = 42
    mini_sentry.add_full_project_config(project_id)
    relay = relay(mini_sentry, options={"limits": {"max_container_size": 1}})

    response = relay.send_otel_span(
        project_id,
        headers={"Content-Type": "application/json"},
        bytes=b"{}",
        raise_for_status=False,
    )

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/json"
    assert response.json()["code"] == GRPC_RESOURCE_EXHAUSTED


def test_span_ingestion(
    mini_sentry,
    relay,
    relay_with_processing,
    spans_consumer,
    metrics_consumer,
    outcomes_consumer,
):
    spans_consumer = spans_consumer()
    metrics_consumer = metrics_consumer()
    outcomes_consumer = outcomes_consumer()

    relay = relay(relay_with_processing())

    project_id = 42
    project_config = mini_sentry.add_full_project_config(project_id)
    project_config["config"].setdefault("features", []).extend(
        ["organizations:relay-generate-billing-outcome"]
    )

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
            "sentry.category": {"type": "string", "value": "ui"},
            "sentry.is_remote": {"type": "boolean", "value": True},
            "sentry.observed_timestamp_nanos": {
                "type": "string",
                "value": time_within(ts, expect_resolution="ns"),
            },
            "sentry.op": {"type": "string", "value": "default"},
            "sentry.trace.status": {"type": "string", "value": "ok"},
            "sentry.origin": {"type": "string", "value": "auto.otlp.spans"},
            "sentry.segment.id": {"type": "string", "value": "f0b809703e783d00"},
            "sentry.segment.name": {"type": "string", "value": "A Proto Span"},
            "sentry.transaction": {"type": "string", "value": "A Proto Span"},
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
            "tags": {
                "is_segment": "true",
                "was_transaction": "false",
                "billing_outcome_emitted": "true",
            },
            "timestamp": time_within_delta(),
            "type": "c",
            "value": 1.0,
        },
    ]

    assert outcomes_consumer.get_aggregated_outcomes(n=2) == [
        {
            "category": DataCategory.TRANSACTION.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
        },
        {
            "category": DataCategory.SPAN.value,
            "key_id": 123,
            "org_id": 1,
            "outcome": 0,
            "project_id": 42,
            "quantity": 1,
        },
    ]
