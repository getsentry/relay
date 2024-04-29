mod convert;

#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, Getter, IntoValue, Object, Val, Value};

use crate::processor::ProcessValue;
use crate::protocol::{
    EventId, JsonLenientString, LenientString, Measurements, MetricsSummary, OperationType,
    OriginType, SpanId, SpanStatus, Timestamp, TraceId,
};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    /// Timestamp when the span was ended.
    #[metastructure(required = "true")]
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the span started.
    #[metastructure(required = "true")]
    pub start_timestamp: Annotated<Timestamp>,

    /// The amount of time in milliseconds spent in this span,
    /// excluding its immediate child spans.
    pub exclusive_time: Annotated<f64>,

    /// Human readable description of a span (e.g. method URL).
    #[metastructure(pii = "maybe")]
    pub description: Annotated<String>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = 128)]
    pub op: Annotated<OperationType>,

    /// The Span id.
    #[metastructure(required = "true")]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    #[metastructure(required = "true")]
    pub trace_id: Annotated<TraceId>,

    /// A unique identifier for a segment within a trace (8 byte hexadecimal string).
    ///
    /// For spans embedded in transactions, the `segment_id` is the `span_id` of the containing
    /// transaction.
    pub segment_id: Annotated<SpanId>,

    /// Whether or not the current span is the root of the segment.
    pub is_segment: Annotated<bool>,

    /// The status of a span.
    pub status: Annotated<SpanStatus>,

    /// Arbitrary tags on a span, like on the top-level event.
    #[metastructure(pii = "maybe")]
    pub tags: Annotated<Object<JsonLenientString>>,

    /// The origin of the span indicates what created the span (see [OriginType] docs).
    #[metastructure(max_chars = 128, allow_chars = "a-zA-Z0-9_.")]
    pub origin: Annotated<OriginType>,

    /// ID of a profile that can be associated with the span.
    pub profile_id: Annotated<EventId>,

    /// Arbitrary additional data on a span.
    ///
    /// Besides arbitrary user data, this object also contains SDK-provided fields used by the
    /// product (see <https://develop.sentry.dev/sdk/performance/span-data-conventions/>).
    #[metastructure(pii = "true")]
    pub data: Annotated<SpanData>,

    /// Tags generated by Relay. These tags are a superset of the tags set on span metrics.
    pub sentry_tags: Annotated<Object<String>>,

    /// Timestamp when the span has been received by Sentry.
    pub received: Annotated<Timestamp>,

    /// Measurements which holds observed values such as web vitals.
    #[metastructure(skip_serialization = "empty")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub measurements: Annotated<Measurements>,

    /// Temporary protocol support for metric summaries.
    ///
    /// This shall move to a stable location once we have stabilized the
    /// interface.  This is intentionally not typed today.
    #[metastructure(skip_serialization = "empty")]
    pub _metrics_summary: Annotated<MetricsSummary>,

    /// Platform identifier.
    ///
    /// See [`Event::platform`](`crate::protocol::Event::platform`).
    #[metastructure(skip_serialization = "empty")]
    pub platform: Annotated<String>,

    /// Whether the span is a segment span that was converted from a transaction.
    #[metastructure(skip_serialization = "empty")]
    pub was_transaction: Annotated<bool>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl Getter for Span {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        Some(match path.strip_prefix("span.")? {
            "exclusive_time" => self.exclusive_time.value()?.into(),
            "description" => self.description.as_str()?.into(),
            "op" => self.op.as_str()?.into(),
            "span_id" => self.span_id.as_str()?.into(),
            "parent_span_id" => self.parent_span_id.as_str()?.into(),
            "trace_id" => self.trace_id.as_str()?.into(),
            "status" => self.status.as_str()?.into(),
            "origin" => self.origin.as_str()?.into(),
            "duration" => {
                let start_timestamp = *self.start_timestamp.value()?;
                let timestamp = *self.timestamp.value()?;
                relay_common::time::chrono_to_positive_millis(timestamp - start_timestamp).into()
            }
            "was_transaction" => self.was_transaction.value().unwrap_or(&false).into(),
            path => {
                if let Some(key) = path.strip_prefix("tags.") {
                    self.tags.value()?.get(key)?.as_str()?.into()
                } else if let Some(key) = path.strip_prefix("data.") {
                    self.data.value()?.get_value(key)?
                } else if let Some(key) = path.strip_prefix("sentry_tags.") {
                    self.sentry_tags.value()?.get(key)?.as_str()?.into()
                } else if let Some(rest) = path.strip_prefix("measurements.") {
                    let name = rest.strip_suffix(".value")?;
                    self.measurements
                        .value()?
                        .get(name)?
                        .value()?
                        .value
                        .value()?
                        .into()
                } else {
                    return None;
                }
            }
        })
    }
}

/// Arbitrary additional data on a span.
///
/// Besides arbitrary user data, this type also contains SDK-provided fields used by the
/// product (see <https://develop.sentry.dev/sdk/performance/span-data-conventions/>).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct SpanData {
    /// Mobile app start variant.
    ///
    /// Can be either "cold" or "warm".
    #[metastructure(field = "app_start_type")] // TODO: no dot?
    pub app_start_type: Annotated<Value>,

    /// The client's browser name.
    #[metastructure(field = "browser.name")]
    pub browser_name: Annotated<String>,

    /// The source code file name that identifies the code unit as uniquely as possible.
    #[metastructure(field = "code.filepath", pii = "maybe")]
    pub code_filepath: Annotated<Value>,
    /// The line number in `code.filepath` best representing the operation.
    #[metastructure(field = "code.lineno", pii = "maybe")]
    pub code_lineno: Annotated<Value>,
    /// The method or function name, or equivalent.
    ///
    /// Usually rightmost part of the code unit's name.
    #[metastructure(field = "code.function", pii = "maybe")]
    pub code_function: Annotated<Value>,
    /// The "namespace" within which `code.function` is defined.
    ///
    /// Usually the qualified class or module name, such that
    /// `code.namespace + some separator + code.function`
    /// form a unique identifier for the code unit.
    #[metastructure(field = "code.namespace", pii = "maybe")]
    pub code_namespace: Annotated<Value>,

    /// The name of the operation being executed.
    ///
    /// E.g. the MongoDB command name such as findAndModify, or the SQL keyword.
    /// Based on [OpenTelemetry's call level db attributes](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md#call-level-attributes).
    #[metastructure(field = "db.operation")]
    pub db_operation: Annotated<Value>,

    /// An identifier for the database management system (DBMS) product being used.
    ///
    /// See [OpenTelemetry docs for a list of well-known identifiers](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/database.md#notes-and-well-known-identifiers-for-dbsystem).
    #[metastructure(field = "db.system")]
    pub db_system: Annotated<Value>,

    /// The sentry environment.
    #[metastructure(field = "sentry.environment", legacy_alias = "environment")]
    pub environment: Annotated<String>,

    /// The release version of the project.
    #[metastructure(field = "sentry.release", legacy_alias = "release")]
    pub release: Annotated<LenientString>,

    /// The decoded body size of the response (in bytes).
    #[metastructure(field = "http.decoded_response_content_length")]
    pub http_decoded_response_content_length: Annotated<Value>,

    /// The HTTP method used.
    #[metastructure(
        field = "http.request_method",
        legacy_alias = "http.method",
        legacy_alias = "method"
    )]
    pub http_request_method: Annotated<Value>,

    /// The encoded body size of the response (in bytes).
    #[metastructure(field = "http.response_content_length")]
    pub http_response_content_length: Annotated<Value>,

    /// The transfer size of the response (in bytes).
    #[metastructure(field = "http.response_transfer_size")]
    pub http_response_transfer_size: Annotated<Value>,

    /// The render blocking status of the resource.
    #[metastructure(field = "resource.render_blocking_status")]
    pub resource_render_blocking_status: Annotated<Value>,

    /// Name of the web server host.
    #[metastructure(field = "server.address")]
    pub server_address: Annotated<Value>,

    /// Whether cache was hit or miss on a read operation.
    #[metastructure(field = "cache.hit")]
    pub cache_hit: Annotated<Value>,

    /// The size of the cache item.
    #[metastructure(field = "cache.item_size")]
    pub cache_item_size: Annotated<Value>,

    /// The status HTTP response.
    #[metastructure(field = "http.response.status_code", legacy_alias = "status_code")]
    pub http_response_status_code: Annotated<Value>,

    /// The 'name' field of the ancestor span with op ai.pipeline.*
    #[metastructure(field = "ai.pipeline.name")]
    pub ai_pipeline_name: Annotated<Value>,

    /// The input messages to an AI model call
    #[metastructure(field = "ai.input_messages")]
    pub ai_input_messages: Annotated<Value>,

    /// The number of tokens used to generate the response to an AI call
    #[metastructure(field = "ai.completion_tokens.used", pii = "false")]
    pub ai_completion_tokens_used: Annotated<Value>,

    /// The number of tokens used to process a request for an AI call
    #[metastructure(field = "ai.prompt_tokens.used", pii = "false")]
    pub ai_prompt_tokens_used: Annotated<Value>,

    /// The total number of tokens used to for an AI call
    #[metastructure(field = "ai.total_tokens.used", pii = "false")]
    pub ai_total_tokens_used: Annotated<Value>,

    /// The responses to an AI model call
    #[metastructure(field = "ai.responses")]
    pub ai_responses: Annotated<Value>,

    /// Label identifying a thread from where the span originated.
    #[metastructure(field = "thread.name")]
    pub thread_name: Annotated<Value>,

    /// Name of the segment that this span belongs to (see `segment_id`).
    ///
    /// This corresponds to the transaction name in the transaction-based model.
    ///
    /// For INP spans, this is the route name where the interaction occurred.
    #[metastructure(field = "sentry.segment.name", legacy_alias = "transaction")]
    pub segment_name: Annotated<String>,

    /// Name of the UI component (e.g. React).
    #[metastructure(field = "ui.component_name")]
    pub ui_component_name: Annotated<Value>,

    /// The URL scheme, e.g. `"https"`.
    #[metastructure(field = "url.scheme")]
    pub url_scheme: Annotated<Value>,

    /// User Display
    #[metastructure(field = "user")]
    pub user: Annotated<Value>,

    /// Replay ID
    #[metastructure(field = "sentry.replay.id", legacy_alias = "replay_id")]
    pub replay_id: Annotated<Value>,

    /// The sentry SDK (see [`crate::protocol::ClientSdkInfo`]).
    #[metastructure(field = "sentry.sdk.name")]
    pub sdk_name: Annotated<String>,

    /// Slow Frames
    #[metastructure(field = "sentry.frames.slow", legacy_alias = "frames.slow")]
    pub frames_slow: Annotated<Value>,

    /// Frozen Frames
    #[metastructure(field = "sentry.frames.frozen", legacy_alias = "frames.frozen")]
    pub frames_frozen: Annotated<Value>,

    /// Total Frames
    #[metastructure(field = "sentry.frames.total", legacy_alias = "frames.total")]
    pub frames_total: Annotated<Value>,

    // Frames Delay (in seconds)
    #[metastructure(field = "frames.delay")]
    pub frames_delay: Annotated<Value>,

    // Messaging Destination Name
    #[metastructure(field = "messaging.destination.name")]
    pub messaging_destination_name: Annotated<Value>,

    /// Other fields in `span.data`.
    #[metastructure(additional_properties, pii = "true", retain = "true")]
    other: Object<Value>,
}

impl Getter for SpanData {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        Some(match path {
            "app_start_type" => self.app_start_type.value()?.into(),
            "browser\\.name" => self.browser_name.as_str()?.into(),
            "code\\.filepath" => self.code_filepath.value()?.into(),
            "code\\.function" => self.code_function.value()?.into(),
            "code\\.lineno" => self.code_lineno.value()?.into(),
            "code\\.namespace" => self.code_namespace.value()?.into(),
            "db.operation" => self.db_operation.value()?.into(),
            "db\\.system" => self.db_system.value()?.into(),
            "environment" => self.environment.as_str()?.into(),
            "http\\.decoded_response_content_length" => {
                self.http_decoded_response_content_length.value()?.into()
            }
            "http\\.request_method" | "http\\.method" | "method" => {
                self.http_request_method.value()?.into()
            }
            "http\\.response_content_length" => self.http_response_content_length.value()?.into(),
            "http\\.response_transfer_size" => self.http_response_transfer_size.value()?.into(),
            "http\\.response.status_code" | "status_code" => {
                self.http_response_status_code.value()?.into()
            }
            "resource\\.render_blocking_status" => {
                self.resource_render_blocking_status.value()?.into()
            }
            "server\\.address" => self.server_address.value()?.into(),
            "thread\\.name" => self.thread_name.value()?.into(),
            "ui\\.component_name" => self.ui_component_name.value()?.into(),
            "url\\.scheme" => self.url_scheme.value()?.into(),
            "transaction" => self.segment_name.as_str()?.into(),
            _ => {
                let escaped = path.replace("\\.", "\0");
                let mut path = escaped.split('.').map(|s| s.replace('\0', "."));
                let root = path.next()?;

                let mut val = self.other.get(&root)?.value()?;
                for part in path {
                    // While there is path segments left, `val` has to be an Object.
                    let relay_protocol::Value::Object(map) = val else {
                        return None;
                    };
                    val = map.get(&part)?.value()?;
                }
                val.into()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::Measurement;
    use chrono::{TimeZone, Utc};
    use relay_base_schema::metrics::{InformationUnit, MetricUnit};
    use relay_protocol::RuleCondition;
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_span_serialization() {
        let json = r#"{
  "timestamp": 0.0,
  "start_timestamp": -63158400.0,
  "exclusive_time": 1.23,
  "description": "desc",
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "status": "ok",
  "origin": "auto.http",
  "measurements": {
    "memory": {
      "value": 9001.0,
      "unit": "byte"
    }
  }
}"#;
        let mut measurements = Object::new();
        measurements.insert(
            "memory".into(),
            Annotated::new(Measurement {
                value: Annotated::new(9001.0),
                unit: Annotated::new(MetricUnit::Information(InformationUnit::Byte)),
            }),
        );
        let span = Annotated::new(Span {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1968, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            exclusive_time: Annotated::new(1.23),
            description: Annotated::new("desc".to_owned()),
            op: Annotated::new("operation".to_owned()),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
            status: Annotated::new(SpanStatus::Ok),
            origin: Annotated::new("auto.http".to_owned()),
            measurements: Annotated::new(Measurements(measurements)),
            ..Default::default()
        });
        assert_eq!(json, span.to_json_pretty().unwrap());

        let span_from_string = Annotated::from_json(json).unwrap();
        assert_eq!(span, span_from_string);
    }

    #[test]
    fn test_getter_span_data() {
        let span = Annotated::<Span>::from_json(
            r#"{
                "data": {
                    "foo": {"bar": 1},
                    "foo.bar": 2
                },
                "measurements": {
                    "some": {"value": 100.0}
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_eq!(span.get_value("span.data.foo.bar"), Some(Val::I64(1)));
        assert_eq!(span.get_value(r"span.data.foo\.bar"), Some(Val::I64(2)));

        assert_eq!(span.get_value("span.data"), None);
        assert_eq!(span.get_value("span.data."), None);
        assert_eq!(span.get_value("span.data.x"), None);

        assert_eq!(
            span.get_value("span.measurements.some.value"),
            Some(Val::F64(100.0))
        );
    }

    #[test]
    fn test_getter_was_transaction() {
        let mut span = Span::default();
        assert_eq!(
            span.get_value("span.was_transaction"),
            Some(Val::Bool(false))
        );
        assert!(RuleCondition::eq("span.was_transaction", false).matches(&span));
        assert!(!RuleCondition::eq("span.was_transaction", true).matches(&span));

        span.was_transaction.set_value(Some(false));
        assert_eq!(
            span.get_value("span.was_transaction"),
            Some(Val::Bool(false))
        );
        assert!(RuleCondition::eq("span.was_transaction", false).matches(&span));
        assert!(!RuleCondition::eq("span.was_transaction", true).matches(&span));

        span.was_transaction.set_value(Some(true));
        assert_eq!(
            span.get_value("span.was_transaction"),
            Some(Val::Bool(true))
        );
        assert!(RuleCondition::eq("span.was_transaction", true).matches(&span));
        assert!(!RuleCondition::eq("span.was_transaction", false).matches(&span));
    }

    #[test]
    fn test_span_duration() {
        let span = Annotated::<Span>::from_json(
            r#"{
                "start_timestamp": 1694732407.8367,
                "timestamp": 1694732408.3145
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_eq!(span.get_value("span.duration"), Some(Val::F64(477.800131)));
    }

    #[test]
    fn test_span_data() {
        let data = r#"{
        "foo": 2,
        "bar": "3",
        "db.system": "mysql",
        "code.filepath": "task.py",
        "code.lineno": 123,
        "code.function": "fn()",
        "code.namespace": "ns",
        "frames.slow": 1,
        "frames.frozen": 2,
        "frames.total": 9,
        "frames.delay": 100
    }"#;
        let data = Annotated::<SpanData>::from_json(data)
            .unwrap()
            .into_value()
            .unwrap();
        insta::assert_debug_snapshot!(data, @r###"
        SpanData {
            app_start_type: ~,
            browser_name: ~,
            code_filepath: String(
                "task.py",
            ),
            code_lineno: I64(
                123,
            ),
            code_function: String(
                "fn()",
            ),
            code_namespace: String(
                "ns",
            ),
            db_operation: ~,
            db_system: String(
                "mysql",
            ),
            environment: ~,
            release: ~,
            http_decoded_response_content_length: ~,
            http_request_method: ~,
            http_response_content_length: ~,
            http_response_transfer_size: ~,
            resource_render_blocking_status: ~,
            server_address: ~,
            cache_hit: ~,
            cache_item_size: ~,
            http_response_status_code: ~,
            ai_pipeline_name: ~,
            ai_input_messages: ~,
            ai_completion_tokens_used: ~,
            ai_prompt_tokens_used: ~,
            ai_total_tokens_used: ~,
            ai_responses: ~,
            thread_name: ~,
            segment_name: ~,
            ui_component_name: ~,
            url_scheme: ~,
            user: ~,
            replay_id: ~,
            sdk_name: ~,
            frames_slow: I64(
                1,
            ),
            frames_frozen: I64(
                2,
            ),
            frames_total: I64(
                9,
            ),
            frames_delay: I64(
                100,
            ),
            other: {
                "bar": String(
                    "3",
                ),
                "foo": I64(
                    2,
                ),
            },
        }
        "###);

        assert_eq!(data.get_value("foo"), Some(Val::U64(2)));
        assert_eq!(data.get_value("bar"), Some(Val::String("3")));
        assert_eq!(data.get_value("db\\.system"), Some(Val::String("mysql")));
        assert_eq!(data.get_value("code\\.lineno"), Some(Val::U64(123)));
        assert_eq!(data.get_value("code\\.function"), Some(Val::String("fn()")));
        assert_eq!(data.get_value("code\\.namespace"), Some(Val::String("ns")));
        assert_eq!(data.get_value("unknown"), None);
    }
}
