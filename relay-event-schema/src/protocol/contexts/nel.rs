#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Contains the unique to NEL reports information.
///
/// NEL is a browser feature that allows reporting of failed network requests from the client side.
/// W3C Editor's Draft: <https://w3c.github.io/network-error-logging/>
/// MDN: <https://developer.mozilla.org/en-US/docs/Web/HTTP/Network_Error_Logging>
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct NelContext {
    /// The age of the report since it got collected and before it got sent.
    pub age: Annotated<i64>,
    /// The type of the report.
    #[metastructure(field = "type")]
    pub ty: Annotated<String>,
    /// For forward compatibility.
    #[metastructure(additional_properties, pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for NelContext {
    fn default_key() -> &'static str {
        "nel"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Nel(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Nel(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Nel(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Nel(Box::new(self))
    }
}
