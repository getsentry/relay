use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, SkipSerialization, Value};
use std::fmt;

use crate::processor::ProcessValue;

#[derive(Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct Attribute {
    #[metastructure(flatten)]
    pub value: AttributeValue,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl Attribute {
    pub fn new(attribute_type: AttributeType, value: Value) -> Self {
        Self {
            value: AttributeValue {
                ty: Annotated::new(attribute_type),
                value: Annotated::new(value),
            },
            other: Object::new(),
        }
    }
}

impl fmt::Debug for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attribute")
            .field("value", &self.value.value)
            .field("type", &self.value.ty)
            .field("other", &self.other)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct AttributeValue {
    #[metastructure(field = "type", required = true, trim = false)]
    pub ty: Annotated<AttributeType>,
    #[metastructure(required = true, pii = "true")]
    pub value: Annotated<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttributeType {
    Boolean,
    Integer,
    Double,
    String,
    Unknown(String),
}

impl ProcessValue for AttributeType {}

impl AttributeType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Double => "double",
            Self::String => "string",
            Self::Unknown(value) => value,
        }
    }

    pub fn unknown_string() -> String {
        "unknown".to_string()
    }
}

impl fmt::Display for AttributeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<String> for AttributeType {
    fn from(value: String) -> Self {
        match value.as_str() {
            "boolean" => Self::Boolean,
            "integer" => Self::Integer,
            "double" => Self::Double,
            "string" => Self::String,
            _ => Self::Unknown(value),
        }
    }
}

impl Empty for AttributeType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for AttributeType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), meta) => Annotated(Some(value.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for AttributeType {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(match self {
            Self::Unknown(s) => s,
            s => s.to_string(),
        })
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::ser::Serialize::serialize(self.as_str(), s)
    }
}
