//! Tests for the generated bounded-decoding descriptors.
//!
//! The scanner itself is tested in `relay-serialization`; these tests make sure the generated
//! OTel descriptors wire up the right nested messages, so that every part of a real payload is
//! counted against the budget.

use opentelemetry_proto::tonic::common::v1::any_value::Value;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, LogsData, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status, TracesData, span,
};
use prost::Message;
use relay_otel::descriptors::{Decodable, LOGS_DATA, TRACES_DATA};
use relay_serialization::prost::{MessageDesc, scan};

/// Ops spent by a single string-valued [`KeyValue`] *inside* its parent:
/// the parent's field key (1) + `KeyValue.key` (1) + `KeyValue.value` (1) + `AnyValue.string_value` (1).
const KV_OPS: usize = 4;

fn ops(msg: &impl Message, desc: &MessageDesc) -> usize {
    scan(&msg.encode_to_vec(), desc, usize::MAX).unwrap()
}

fn string(s: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(s.to_owned())),
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(string(value)),
        ..Default::default()
    }
}

fn logs_with(record: LogRecord) -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![record],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A log payload which exercises every nested message reachable from `LogsData`.
fn full_logs() -> LogsData {
    LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "test-service")],
                dropped_attributes_count: 1,
                entity_refs: vec![EntityRef {
                    r#type: "service".to_owned(),
                    id_keys: vec!["service.name".to_owned()],
                    ..Default::default()
                }],
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "test-library".to_owned(),
                    version: "1.0".to_owned(),
                    attributes: vec![kv("scope.attr", "x")],
                    dropped_attributes_count: 0,
                }),
                log_records: vec![LogRecord {
                    time_unix_nano: 123,
                    observed_time_unix_nano: 456,
                    severity_number: 9,
                    severity_text: "INFO".to_owned(),
                    body: Some(AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "list".to_owned(),
                                value: Some(AnyValue {
                                    value: Some(Value::ArrayValue(ArrayValue {
                                        values: vec![string("a"), string("b")],
                                    })),
                                }),
                                ..Default::default()
                            }],
                        })),
                    }),
                    attributes: vec![kv("attr", "value")],
                    trace_id: vec![0xAB; 16],
                    span_id: vec![0xCD; 8],
                    event_name: "event".to_owned(),
                    ..Default::default()
                }],
                schema_url: "http://example.com/scope".to_owned(),
            }],
            schema_url: "http://example.com".to_owned(),
        }],
    }
}

#[test]
fn test_logs_roundtrip() {
    let logs = full_logs();
    let decoded = LogsData::decode_bounded(&logs.encode_to_vec(), 1_000).unwrap();
    assert_eq!(decoded, logs);
}

#[test]
fn test_logs_exact_op_count() {
    // LogsData: resource_logs                                  1
    // ResourceLogs: resource, scope_logs, schema_url           3
    //   Resource: attributes(kv), dropped, entity_refs          KV_OPS + 1 + 1
    //     EntityRef: type, id_keys                              2
    //   ScopeLogs: scope, log_records, schema_url               3
    //     Scope: name, version, attributes(kv)                  2 + KV_OPS
    //     LogRecord: time, observed, sev_num, sev_text,
    //                body, attributes(kv), trace_id, span_id,
    //                event_name                                 8 + KV_OPS
    //       body AnyValue: kvlist                               1
    //         KeyValueList: values                              1
    //           KeyValue: key, value                            2
    //             AnyValue: array                               1
    //               ArrayValue: values x2                       2
    //                 AnyValue: string x2                       2
    let expected =
        1 + 3 + (KV_OPS + 2) + 2 + 3 + (2 + KV_OPS) + (8 + KV_OPS) + 1 + 1 + 2 + 1 + 2 + 2;
    assert_eq!(ops(&full_logs(), &LOGS_DATA), expected);
}

#[test]
fn test_logs_budget_boundary() {
    let buf = full_logs().encode_to_vec();
    let spent = scan(&buf, &LOGS_DATA, usize::MAX).unwrap();

    assert!(LogsData::decode_bounded(&buf, spent).is_ok());
    assert!(
        LogsData::decode_bounded(&buf, spent - 1)
            .unwrap_err()
            .is_limit_exceeded()
    );
}

/// Adding an attribute at each location must cost the full [`KV_OPS`]. If a descriptor were
/// missing a nested edge, the scanner would treat the attribute as opaque bytes and only charge 1.
#[test]
fn test_logs_attributes_counted_at_every_level() {
    let base = full_logs();
    let base_ops = ops(&base, &LOGS_DATA);

    let mut resource = base.clone();
    resource.resource_logs[0]
        .resource
        .as_mut()
        .unwrap()
        .attributes
        .push(kv("k", "v"));
    assert_eq!(ops(&resource, &LOGS_DATA), base_ops + KV_OPS);

    let mut scope = base.clone();
    scope.resource_logs[0].scope_logs[0]
        .scope
        .as_mut()
        .unwrap()
        .attributes
        .push(kv("k", "v"));
    assert_eq!(ops(&scope, &LOGS_DATA), base_ops + KV_OPS);

    let mut record = base.clone();
    record.resource_logs[0].scope_logs[0].log_records[0]
        .attributes
        .push(kv("k", "v"));
    assert_eq!(ops(&record, &LOGS_DATA), base_ops + KV_OPS);
}

#[test]
fn test_logs_body_nesting_counted() {
    // Each element of an array body costs 2: `ArrayValue.values` + `AnyValue.string_value`.
    let array_body = |n: usize| {
        logs_with(LogRecord {
            body: Some(AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: (0..n).map(|i| string(&i.to_string())).collect(),
                })),
            }),
            ..Default::default()
        })
    };
    assert_eq!(
        ops(&array_body(100), &LOGS_DATA),
        ops(&array_body(0), &LOGS_DATA) + 200
    );

    // Each entry of a kvlist body costs a full KeyValue.
    let kvlist_body = |n: usize| {
        logs_with(LogRecord {
            body: Some(AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: (0..n).map(|i| kv(&i.to_string(), "v")).collect(),
                })),
            }),
            ..Default::default()
        })
    };
    assert_eq!(
        ops(&kvlist_body(100), &LOGS_DATA),
        ops(&kvlist_body(0), &LOGS_DATA) + 100 * KV_OPS
    );
}

#[test]
fn test_logs_many_records_exceed_budget() {
    let logs = LogsData {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord::default(); 1_001],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let buf = logs.encode_to_vec();

    // resource_logs + scope_logs + one per (empty) record.
    assert_eq!(scan(&buf, &LOGS_DATA, usize::MAX).unwrap(), 1_003);
    assert!(
        LogsData::decode_bounded(&buf, 1_000)
            .unwrap_err()
            .is_limit_exceeded()
    );
    assert_eq!(LogsData::decode_bounded(&buf, 1_003).unwrap(), logs);
}

#[test]
fn test_logs_deeply_nested_body_rejected() {
    let mut body = string("leaf");
    for _ in 0..200 {
        body = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue { values: vec![body] })),
        };
    }
    let logs = logs_with(LogRecord {
        body: Some(body),
        ..Default::default()
    });

    let err = LogsData::decode_bounded(&logs.encode_to_vec(), usize::MAX).unwrap_err();
    assert!(!err.is_limit_exceeded(), "{err}");
}

#[test]
fn test_logs_invalid_payload_rejected() {
    let buf = full_logs().encode_to_vec();
    // Truncating the payload leaves a length-delimited field pointing past the end.
    let err = LogsData::decode_bounded(&buf[..buf.len() - 1], usize::MAX).unwrap_err();
    assert!(!err.is_limit_exceeded(), "{err}");
}

fn full_traces() -> TracesData {
    TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv("service.name", "test-service")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "test-library".to_owned(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: vec![0xAB; 16],
                    span_id: vec![0xCD; 8],
                    name: "span".to_owned(),
                    attributes: vec![kv("attr", "value")],
                    events: vec![span::Event {
                        name: "event".to_owned(),
                        attributes: vec![kv("event.attr", "value")],
                        ..Default::default()
                    }],
                    links: vec![span::Link {
                        trace_id: vec![0xEF; 16],
                        attributes: vec![kv("link.attr", "value")],
                        ..Default::default()
                    }],
                    status: Some(Status {
                        message: "ok".to_owned(),
                        code: 1,
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[test]
fn test_traces_roundtrip() {
    let traces = full_traces();
    let decoded = TracesData::decode_bounded(&traces.encode_to_vec(), 1_000).unwrap();
    assert_eq!(decoded, traces);
}

#[test]
fn test_traces_exact_op_count() {
    // TracesData: resource_spans                                1
    // ResourceSpans: resource, scope_spans                      2
    //   Resource: attributes(kv)                                KV_OPS
    //   ScopeSpans: scope, spans                                2
    //     Scope: name                                           1
    //     Span: trace_id, span_id, name, attributes(kv),
    //           events, links, status                           6 + KV_OPS
    //       Event: name, attributes(kv)                         1 + KV_OPS
    //       Link: trace_id, attributes(kv)                      1 + KV_OPS
    //       Status: message, code                               2
    let expected = 1 + 2 + KV_OPS + 2 + 1 + (6 + KV_OPS) + (1 + KV_OPS) + (1 + KV_OPS) + 2;
    assert_eq!(ops(&full_traces(), &TRACES_DATA), expected);
}
