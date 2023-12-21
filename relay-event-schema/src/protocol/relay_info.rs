#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// The Relay Interface describes a Sentry Relay and its configuration used to process an event during ingest.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct RelayInfo {
    /// Version of the Relay. Required.
    #[metastructure(required = "true", max_chars = 256)]
    pub version: Annotated<String>,

    /// Public key of the Relay.
    #[metastructure(required = "false", max_chars = 256)]
    pub public_key: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}
