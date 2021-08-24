use crate::types::{Annotated, Object, Value};

/// The Relay Interface describes a Sentry Relay and its configuration used to process an event during ingest.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct RelayInfo {
    /// The version of the Relay. Required.
    /// TODO
    #[metastructure(required = "true", max_chars = "symbol")]
    pub version: Annotated<String>,

    /// TODO
    #[metastructure()]
    pub public_key: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}
