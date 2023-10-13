#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

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
