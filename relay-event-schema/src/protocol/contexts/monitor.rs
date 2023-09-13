#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Monitor information.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct MonitorContext(#[metastructure(pii = "maybe")] pub Object<Value>);

impl From<Object<Value>> for MonitorContext {
    fn from(object: Object<Value>) -> Self {
        Self(object)
    }
}

impl std::ops::Deref for MonitorContext {
    type Target = Object<Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for MonitorContext {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl super::DefaultContext for MonitorContext {
    fn default_key() -> &'static str {
        "monitor"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Monitor(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Monitor(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Monitor(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Monitor(Box::new(self))
    }
}
