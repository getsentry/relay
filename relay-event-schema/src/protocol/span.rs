mod convert;

use relay_protocol::{
    Annotated, Array, Empty, Error, FromValue, Getter, IntoValue, Object, Val, Value,
};

use crate::processor::ProcessValue;
use crate::protocol::{
    EventId, IpAddr, JsonLenientString, LenientString, Measurements, MetricsSummary, OperationType,
    OriginType, SpanId, SpanStatus, ThreadId, Timestamp, TraceId,
};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[metastructure(process_func = "process_span", value_type = "Span")]
pub struct Span {
    /// Timestamp when the span was ended.
    #[metastructure(required = "true", trim = "false")]
    pub timestamp: Annotated<Timestamp>,

    /// Timestamp when the span started.
    #[metastructure(required = "true", trim = "false")]
    pub start_timestamp: Annotated<Timestamp>,

    /// The amount of time in milliseconds spent in this span,
    /// excluding its immediate child spans.
    #[metastructure(trim = "false")]
    pub exclusive_time: Annotated<f64>,

    /// Span type (see `OperationType` docs).
    #[metastructure(max_chars = 128, trim = "false")]
    pub op: Annotated<OperationType>,

    /// The Span id.
    #[metastructure(required = "true", trim = "false")]
    pub span_id: Annotated<SpanId>,

    /// The ID of the span enclosing this span.
    #[metastructure(trim = "false")]
    pub parent_span_id: Annotated<SpanId>,

    /// The ID of the trace the span belongs to.
    #[metastructure(required = "true", trim = "false")]
    pub trace_id: Annotated<TraceId>,

    /// A unique identifier for a segment within a trace (8 byte hexadecimal string).
    ///
    /// For spans embedded in transactions, the `segment_id` is the `span_id` of the containing
    /// transaction.
    #[metastructure(trim = "false")]
    pub segment_id: Annotated<SpanId>,

    /// Whether or not the current span is the root of the segment.
    #[metastructure(trim = "false")]
    pub is_segment: Annotated<bool>,

    /// The status of a span.
    #[metastructure(trim = "false")]
    pub status: Annotated<SpanStatus>,

    /// Human readable description of a span (e.g. method URL).
    #[metastructure(pii = "maybe", trim = "false")]
    pub description: Annotated<String>,

    /// Arbitrary tags on a span, like on the top-level event.
    #[metastructure(pii = "maybe", trim = "false")]
    pub tags: Annotated<Object<JsonLenientString>>,

    /// The origin of the span indicates what created the span (see [OriginType] docs).
    #[metastructure(max_chars = 128, allow_chars = "a-zA-Z0-9_.", trim = "false")]
    pub origin: Annotated<OriginType>,

    /// ID of a profile that can be associated with the span.
    #[metastructure(trim = "false")]
    pub profile_id: Annotated<EventId>,

    /// Arbitrary additional data on a span.
    ///
    /// Besides arbitrary user data, this object also contains SDK-provided fields used by the
    /// product (see <https://develop.sentry.dev/sdk/performance/span-data-conventions/>).
    #[metastructure(pii = "true", trim = "false")]
    pub data: Annotated<SpanData>,

    /// Tags generated by Relay. These tags are a superset of the tags set on span metrics.
    #[metastructure(trim = "false")]
    pub sentry_tags: Annotated<Object<String>>,

    /// Timestamp when the span has been received by Sentry.
    #[metastructure(trim = "false")]
    pub received: Annotated<Timestamp>,

    /// Measurements which holds observed values such as web vitals.
    #[metastructure(skip_serialization = "empty", trim = "false")]
    #[metastructure(omit_from_schema)] // we only document error events for now
    pub measurements: Annotated<Measurements>,

    /// Temporary protocol support for metric summaries.
    ///
    /// This shall move to a stable location once we have stabilized the
    /// interface.  This is intentionally not typed today.
    #[metastructure(skip_serialization = "empty", trim = "false")]
    pub _metrics_summary: Annotated<MetricsSummary>,

    /// Platform identifier.
    ///
    /// See [`Event::platform`](`crate::protocol::Event::platform`).
    #[metastructure(skip_serialization = "empty", trim = "false")]
    pub platform: Annotated<String>,

    /// Whether the span is a segment span that was converted from a transaction.
    #[metastructure(skip_serialization = "empty", trim = "false")]
    pub was_transaction: Annotated<bool>,

    // TODO remove retain when the api stabilizes
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe", trim = "false")]
    pub other: Object<Value>,
}

impl Span {
    /// Returns the value of an attribute on the span.
    ///
    /// This primarily looks up the attribute in the `data` object, but falls back to the `tags`
    /// object if the attribute is not found.
    fn attribute(&self, key: &str) -> Option<Val<'_>> {
        Some(match self.data.value()?.get_value(key) {
            Some(value) => value,
            None => self.tags.value()?.get(key)?.as_str()?.into(),
        })
    }
}

impl Getter for Span {
    fn get_value(&self, path: &str) -> Option<Val<'_>> {
        let span_prefix = path.strip_prefix("span.");
        if let Some(span_prefix) = span_prefix {
            return Some(match span_prefix {
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
                    relay_common::time::chrono_to_positive_millis(timestamp - start_timestamp)
                        .into()
                }
                "was_transaction" => self.was_transaction.value().unwrap_or(&false).into(),
                path => {
                    if let Some(key) = path.strip_prefix("tags.") {
                        self.tags.value()?.get(key)?.as_str()?.into()
                    } else if let Some(key) = path.strip_prefix("data.") {
                        self.attribute(key)?
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
            });
        }

        // For backward compatibility with event-based rules, we try to support `event.` fields also
        // for a span.
        let event_prefix = path.strip_prefix("event.")?;
        Some(match event_prefix {
            "release" => self.data.value()?.release.as_str()?.into(),
            "environment" => self.data.value()?.environment.as_str()?.into(),
            "transaction" => self.data.value()?.segment_name.as_str()?.into(),
            "contexts.browser.name" => self.data.value()?.browser_name.as_str()?.into(),
            // TODO: we might want to add additional fields once they are added to the span.
            _ => return None,
        })
    }
}

/// Arbitrary additional data on a span.
///
/// Besides arbitrary user data, this type also contains SDK-provided fields used by the
/// product (see <https://develop.sentry.dev/sdk/performance/span-data-conventions/>).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct SpanData {
    /// Mobile app start variant.
    ///
    /// Can be either "cold" or "warm".
    #[metastructure(field = "app_start_type")] // TODO: no dot?
    pub app_start_type: Annotated<Value>,

    /// The total tokens that were used by an LLM call
    #[metastructure(field = "ai.total_tokens.used")]
    pub ai_total_tokens_used: Annotated<Value>,

    /// The input tokens used by an LLM call (usually cheaper than output tokens)
    #[metastructure(field = "ai.prompt_tokens.used")]
    pub ai_prompt_tokens_used: Annotated<Value>,

    /// The output tokens used by an LLM call (the ones the LLM actually generated)
    #[metastructure(field = "ai.completion_tokens.used")]
    pub ai_completion_tokens_used: Annotated<Value>,

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

    /// The name of a collection (table, container) within the database.
    ///
    /// See [OpenTelemetry's database span semantic conventions](https://opentelemetry.io/docs/specs/semconv/database/database-spans/#common-attributes).
    #[metastructure(
        field = "db.collection.name",
        legacy_alias = "db.cassandra.table",
        legacy_alias = "db.cosmosdb.container",
        legacy_alias = "db.mongodb.collection",
        legacy_alias = "db.sql.table"
    )]
    pub db_collection_name: Annotated<Value>,

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

    /// The name of the cache key.
    #[metastructure(field = "cache.key")]
    pub cache_key: Annotated<Value>,

    /// The size of the cache item.
    #[metastructure(field = "cache.item_size")]
    pub cache_item_size: Annotated<Value>,

    /// The status HTTP response.
    #[metastructure(field = "http.response.status_code", legacy_alias = "status_code")]
    pub http_response_status_code: Annotated<Value>,

    /// The 'name' field of the ancestor span with op ai.pipeline.*
    #[metastructure(field = "ai.pipeline.name")]
    pub ai_pipeline_name: Annotated<Value>,

    /// The Model ID of an AI pipeline, e.g., gpt-4
    #[metastructure(field = "ai.model_id")]
    pub ai_model_id: Annotated<Value>,

    /// The input messages to an AI model call
    #[metastructure(field = "ai.input_messages")]
    pub ai_input_messages: Annotated<Value>,

    /// The responses to an AI model call
    #[metastructure(field = "ai.responses")]
    pub ai_responses: Annotated<Value>,

    /// Label identifying a thread from where the span originated.
    #[metastructure(field = "thread.name")]
    pub thread_name: Annotated<String>,

    /// ID of thread from where the span originated.
    #[metastructure(field = "thread.id")]
    pub thread_id: Annotated<ThreadId>,

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

    /// User email address.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.email")]
    pub user_email: Annotated<String>,

    /// User’s full name.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.full_name")]
    pub user_full_name: Annotated<String>,

    /// Two-letter country code (ISO 3166-1 alpha-2).
    ///
    /// This is not an OTel convention (yet).
    #[metastructure(field = "user.geo.country_code")]
    pub user_geo_country_code: Annotated<String>,

    /// Human readable city name.
    ///
    /// This is not an OTel convention (yet).
    #[metastructure(field = "user.geo.city")]
    pub user_geo_city: Annotated<String>,

    /// Human readable subdivision name.
    ///
    /// This is not an OTel convention (yet).
    #[metastructure(field = "user.geo.subdivision")]
    pub user_geo_subdivision: Annotated<String>,

    /// Human readable region name or code.
    ///
    /// This is not an OTel convention (yet).
    #[metastructure(field = "user.geo.region")]
    pub user_geo_region: Annotated<String>,

    /// Unique user hash to correlate information for a user in anonymized form.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.hash")]
    pub user_hash: Annotated<String>,

    /// Unique identifier of the user.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.id")]
    pub user_id: Annotated<String>,

    /// Short name or login/username of the user.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.name")]
    pub user_name: Annotated<String>,

    /// Array of user roles at the time of the event.
    ///
    /// <https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/>
    #[metastructure(field = "user.roles")]
    pub user_roles: Annotated<Array<String>>,

    /// Replay ID
    #[metastructure(field = "sentry.replay.id", legacy_alias = "replay_id")]
    pub replay_id: Annotated<Value>,

    /// The sentry SDK (see [`crate::protocol::ClientSdkInfo`]).
    #[metastructure(field = "sentry.sdk.name")]
    pub sdk_name: Annotated<String>,

    /// The sentry SDK version (see [`crate::protocol::ClientSdkInfo`]).
    #[metastructure(field = "sentry.sdk.version")]
    pub sdk_version: Annotated<String>,

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
    pub messaging_destination_name: Annotated<String>,

    /// Message Retry Count
    #[metastructure(field = "messaging.message.retry.count")]
    pub messaging_message_retry_count: Annotated<Value>,

    /// Message Receive Latency
    #[metastructure(field = "messaging.message.receive.latency")]
    pub messaging_message_receive_latency: Annotated<Value>,

    /// Message Body Size
    #[metastructure(field = "messaging.message.body.size")]
    pub messaging_message_body_size: Annotated<Value>,

    /// Message ID
    #[metastructure(field = "messaging.message.id")]
    pub messaging_message_id: Annotated<String>,

    /// Value of the HTTP User-Agent header sent by the client.
    #[metastructure(field = "user_agent.original")]
    pub user_agent_original: Annotated<String>,

    /// Absolute URL of a network resource.
    #[metastructure(field = "url.full")]
    pub url_full: Annotated<String>,

    /// The client's IP address.
    #[metastructure(field = "client.address")]
    pub client_address: Annotated<IpAddr>,

    /// The current route in the application.
    ///
    /// Set by React Native SDK.
    #[metastructure(pii = "maybe", skip_serialization = "empty")]
    pub route: Annotated<Route>,
    /// The previous route in the application
    ///
    /// Set by React Native SDK.
    #[metastructure(field = "previousRoute", pii = "maybe", skip_serialization = "empty")]
    pub previous_route: Annotated<Route>,

    // The dom element responsible for the largest contentful paint
    #[metastructure(field = "lcp.element")]
    pub lcp_element: Annotated<String>,

    // The size of the largest contentful paint element
    #[metastructure(field = "lcp.size")]
    pub lcp_size: Annotated<Value>,

    // The id of the largest contentful paint element
    #[metastructure(field = "lcp.id")]
    pub lcp_id: Annotated<String>,

    // The url of the largest contentful paint element
    #[metastructure(field = "lcp.url")]
    pub lcp_url: Annotated<String>,

    /// Other fields in `span.data`.
    #[metastructure(
        additional_properties,
        pii = "true",
        retain = "true",
        skip_serialization = "empty"
    )]
    pub other: Object<Value>,
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
            "thread\\.name" => self.thread_name.as_str()?.into(),
            "ui\\.component_name" => self.ui_component_name.value()?.into(),
            "url\\.scheme" => self.url_scheme.value()?.into(),
            "user" => self.user.value()?.into(),
            "user\\.email" => self.user_email.as_str()?.into(),
            "user\\.full_name" => self.user_full_name.as_str()?.into(),
            "user\\.geo\\.city" => self.user_geo_city.as_str()?.into(),
            "user\\.geo\\.country_code" => self.user_geo_country_code.as_str()?.into(),
            "user\\.geo\\.region" => self.user_geo_region.as_str()?.into(),
            "user\\.geo\\.subdivision" => self.user_geo_subdivision.as_str()?.into(),
            "user\\.hash" => self.user_hash.as_str()?.into(),
            "user\\.id" => self.user_id.as_str()?.into(),
            "user\\.name" => self.user_name.as_str()?.into(),
            "transaction" => self.segment_name.as_str()?.into(),
            "release" => self.release.as_str()?.into(),
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

/// The route in the application, set by React Native SDK.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Route {
    /// The name of the route.
    #[metastructure(pii = "maybe", skip_serialization = "empty")]
    pub name: Annotated<String>,

    /// Parameters assigned to this route.
    #[metastructure(
        pii = "true",
        skip_serialization = "empty",
        max_depth = 5,
        max_bytes = 2048
    )]
    pub params: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(
        additional_properties,
        retain = "true",
        pii = "maybe",
        skip_serialization = "empty"
    )]
    pub other: Object<Value>,
}

impl FromValue for Route {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        match value {
            Annotated(Some(Value::String(name)), meta) => Annotated(
                Some(Route {
                    name: Annotated::new(name),
                    ..Default::default()
                }),
                meta,
            ),
            Annotated(Some(Value::Object(mut values)), meta) => {
                let mut route: Route = Default::default();
                if let Some(Annotated(Some(Value::String(name)), _)) = values.remove("name") {
                    route.name = Annotated::new(name);
                }
                if let Some(Annotated(Some(Value::Object(params)), _)) = values.remove("params") {
                    route.params = Annotated::new(params);
                }

                if !values.is_empty() {
                    route.other = values;
                }

                Annotated(Some(route), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("route expected to be an object"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
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
  "op": "operation",
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
  "status": "ok",
  "description": "desc",
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
    fn test_span_fields_as_event() {
        let span = Annotated::<Span>::from_json(
            r#"{
                "data": {
                    "release": "1.0",
                    "environment": "prod",
                    "sentry.segment.name": "/api/endpoint"
                }
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();

        assert_eq!(span.get_value("event.release"), Some(Val::String("1.0")));
        assert_eq!(
            span.get_value("event.environment"),
            Some(Val::String("prod"))
        );
        assert_eq!(
            span.get_value("event.transaction"),
            Some(Val::String("/api/endpoint"))
        );
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
        "frames.delay": 100,
        "messaging.destination.name": "default",
        "messaging.message.retry.count": 3,
        "messaging.message.receive.latency": 40,
        "messaging.message.body.size": 100,
        "messaging.message.id": "abc123",
        "user_agent.original": "Chrome",
        "url.full": "my_url.com",
        "client.address": "192.168.0.1"
    }"#;
        let data = Annotated::<SpanData>::from_json(data)
            .unwrap()
            .into_value()
            .unwrap();
        insta::assert_debug_snapshot!(data, @r###"
        SpanData {
            app_start_type: ~,
            ai_total_tokens_used: ~,
            ai_prompt_tokens_used: ~,
            ai_completion_tokens_used: ~,
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
            db_collection_name: ~,
            environment: ~,
            release: ~,
            http_decoded_response_content_length: ~,
            http_request_method: ~,
            http_response_content_length: ~,
            http_response_transfer_size: ~,
            resource_render_blocking_status: ~,
            server_address: ~,
            cache_hit: ~,
            cache_key: ~,
            cache_item_size: ~,
            http_response_status_code: ~,
            ai_pipeline_name: ~,
            ai_model_id: ~,
            ai_input_messages: ~,
            ai_responses: ~,
            thread_name: ~,
            thread_id: ~,
            segment_name: ~,
            ui_component_name: ~,
            url_scheme: ~,
            user: ~,
            user_email: ~,
            user_full_name: ~,
            user_geo_country_code: ~,
            user_geo_city: ~,
            user_geo_subdivision: ~,
            user_geo_region: ~,
            user_hash: ~,
            user_id: ~,
            user_name: ~,
            user_roles: ~,
            replay_id: ~,
            sdk_name: ~,
            sdk_version: ~,
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
            messaging_destination_name: "default",
            messaging_message_retry_count: I64(
                3,
            ),
            messaging_message_receive_latency: I64(
                40,
            ),
            messaging_message_body_size: I64(
                100,
            ),
            messaging_message_id: "abc123",
            user_agent_original: "Chrome",
            url_full: "my_url.com",
            client_address: IpAddr(
                "192.168.0.1",
            ),
            route: ~,
            previous_route: ~,
            lcp_element: ~,
            lcp_size: ~,
            lcp_id: ~,
            lcp_url: ~,
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
