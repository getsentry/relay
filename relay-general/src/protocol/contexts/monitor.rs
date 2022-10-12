use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

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

impl MonitorContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "monitor"
    }
}
