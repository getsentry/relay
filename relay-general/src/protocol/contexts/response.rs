use crate::protocol::{Cookies, Headers};
use crate::types::{Annotated, Object, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct ResponseContext {
    /// Type `response`.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,

    /// The cookie values.
    ///
    /// Can be given unparsed as string, as dictionary, or as a list of tuples.
    #[metastructure(pii = "true", bag_size = "medium")]
    #[metastructure(skip_serialization = "empty")]
    pub cookies: Annotated<Cookies>,

    /// A dictionary of submitted headers.
    ///
    /// If a header appears multiple times it, needs to be merged according to the HTTP standard
    /// for header merging. Header names are treated case-insensitively by Sentry.
    #[metastructure(pii = "true", bag_size = "large")]
    #[metastructure(skip_serialization = "empty")]
    pub headers: Annotated<Headers>,

    /// HTTP status code.
    pub status_code: Annotated<u64>,

    /// HTTP response body size.
    pub body_size: Annotated<u64>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl ResponseContext {
    /// The key under which a runtime context is generally stored (in `Contexts`).
    pub fn default_key() -> &'static str {
        "response"
    }
}
