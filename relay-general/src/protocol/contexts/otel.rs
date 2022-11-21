use crate::types::{Annotated, Object, Value};

/// OpenTelemetry Context
///
/// If an event has this context, it was generated from an OpenTelemetry signal (trace, metric, log).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct OtelContext {
    /// Attributes of the OpenTelemetry span that maps to a Sentry event.
    ///
    /// <https://github.com/open-telemetry/opentelemetry-proto/blob/724e427879e3d2bae2edc0218fff06e37b9eb46e/opentelemetry/proto/trace/v1/trace.proto#L174-L186>
    #[metastructure(pii = "maybe", bag_size = "large")]
    attributes: Annotated<Object<Value>>,

    /// Information about an OpenTelemetry resource.
    ///
    /// <https://github.com/open-telemetry/opentelemetry-proto/blob/724e427879e3d2bae2edc0218fff06e37b9eb46e/opentelemetry/proto/resource/v1/resource.proto>
    #[metastructure(pii = "maybe", bag_size = "large")]
    resource: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl OtelContext {
    /// The key under which a runtime context is generally stored (in `Contexts`).
    pub fn default_key() -> &'static str {
        "otel"
    }
}
