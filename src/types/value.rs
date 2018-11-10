use std::collections::BTreeMap;
use std::fmt;

use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use serde_derive::Deserialize;

use crate::types::{Annotated, Meta};

/// Alias for typed arrays.
pub type Array<T> = Vec<Annotated<T>>;

/// Alias for maps.
pub type Map<K, T> = BTreeMap<K, Annotated<T>>;

/// Alias for typed objects.
pub type Object<T> = Map<String, T>;

/// Represents a boxed value.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Array<Value>),
    Object(Object<Value>),
}

/// Helper type that renders out a description of the value.
pub struct ValueDescription<'a>(&'a Value);

impl<'a> fmt::Display for ValueDescription<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self.0 {
            Value::Null => f.pad("null"),
            Value::Bool(true) => f.pad("true"),
            Value::Bool(false) => f.pad("false"),
            Value::I64(val) => write!(f, "integer {}", val),
            Value::U64(val) => write!(f, "integer {}", val),
            Value::F64(val) => write!(f, "float {}", val),
            Value::String(ref val) => f.pad(val),
            Value::Array(_) => f.pad("an array"),
            Value::Object(_) => f.pad("an object"),
        }
    }
}

impl Value {
    /// Returns a formattable that gives a helper description of the value.
    pub fn describe(&self) -> ValueDescription {
        ValueDescription(self)
    }

    /// Returns the string if this value is a string, otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(string) => Some(string.as_str()),
            _ => None,
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(val) => serializer.serialize_bool(val),
            Value::I64(val) => serializer.serialize_i64(val),
            Value::U64(val) => serializer.serialize_u64(val),
            Value::F64(val) => serializer.serialize_f64(val),
            Value::String(ref val) => serializer.serialize_str(val),
            Value::Array(ref items) => {
                let mut seq_ser = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    match item {
                        Annotated(Some(val), _) => seq_ser.serialize_element(val)?,
                        Annotated(None, _) => seq_ser.serialize_element(&())?,
                    }
                }
                seq_ser.end()
            }
            Value::Object(ref items) => {
                let mut map_ser = serializer.serialize_map(Some(items.len()))?;
                for (key, value) in items {
                    map_ser.serialize_key(key)?;
                    match value {
                        Annotated(Some(val), _) => map_ser.serialize_value(val)?,
                        Annotated(None, _) => map_ser.serialize_value(&())?,
                    }
                }
                map_ser.end()
            }
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Value {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(value) => Value::Bool(value),
            serde_json::Value::Number(num) => {
                if let Some(val) = num.as_i64() {
                    Value::I64(val)
                } else if let Some(val) = num.as_u64() {
                    Value::U64(val)
                } else if let Some(val) = num.as_f64() {
                    Value::F64(val)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(val) => Value::String(val),
            serde_json::Value::Array(items) => {
                Value::Array(items.into_iter().map(From::from).collect())
            }
            serde_json::Value::Object(items) => {
                Value::Object(items.into_iter().map(|(k, v)| (k, From::from(v))).collect())
            }
        }
    }
}

impl From<serde_json::Value> for Annotated<Value> {
    fn from(value: serde_json::Value) -> Annotated<Value> {
        let value: Value = value.into();
        match value {
            Value::Null => Annotated(None, Meta::default()),
            other => Annotated(Some(other), Meta::default()),
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(value: Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Bool(value) => serde_json::Value::Bool(value),
            Value::I64(value) => serde_json::Value::Number(value.into()),
            Value::U64(value) => serde_json::Value::Number(value.into()),
            Value::F64(value) => serde_json::Number::from_f64(value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(val) => serde_json::Value::String(val),
            Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(From::from).collect())
            }
            Value::Object(items) => serde_json::Value::Object(
                items.into_iter().map(|(k, v)| (k, From::from(v))).collect(),
            ),
        }
    }
}

impl From<Annotated<Value>> for serde_json::Value {
    fn from(value: Annotated<Value>) -> serde_json::Value {
        value.0.map(From::from).unwrap_or(serde_json::Value::Null)
    }
}
