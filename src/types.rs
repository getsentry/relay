//! Common types of the protocol.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::ser::{SerializeMap, Serializer};

use meta::{Annotated, Meta, MetaTree, Value};
use processor::{
    FieldAttrs, MetaStructure, ProcessingState, Processor, SerializeMetaStructurePayload,
};

/// Alias for typed arrays.
pub type Array<T> = Vec<Annotated<T>>;
/// Alias for typed objects.
pub type Object<T> = BTreeMap<String, Annotated<T>>;

/// A array like wrapper used in various places.
#[derive(Clone, Debug, PartialEq)]
pub struct Values<T> {
    /// The values of the collection.
    pub values: Annotated<Array<T>>,
    /// Additional arbitrary fields for forwards compatibility.
    pub other: Object<Value>,
}

impl<T> Values<T> {
    /// Constructs a new value array from a given array
    pub fn new(values: Array<T>) -> Values<T> {
        Values {
            values: Annotated::new(values),
            other: Object::default(),
        }
    }
}

impl<T: MetaStructure> MetaStructure for Values<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Values {
                    values: Annotated(
                        Some(items.into_iter().map(MetaStructure::from_value).collect()),
                        meta,
                    ),
                    other: Default::default(),
                }),
                Meta::default(),
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(Value::Object(mut obj)), meta) => {
                if let Some(values) = obj.remove("values") {
                    Annotated(
                        Some(Values {
                            values: MetaStructure::from_value(values),
                            other: obj,
                        }),
                        meta,
                    )
                } else {
                    Annotated(
                        Some(Values {
                            values: Annotated(
                                Some(vec![MetaStructure::from_value(Annotated(
                                    Some(Value::Object(obj)),
                                    meta,
                                ))]),
                                Default::default(),
                            ),
                            other: Default::default(),
                        }),
                        Meta::default(),
                    )
                }
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected array or values".to_string());
                Annotated(None, meta)
            }
        }
    }

    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => {
                let mut rv = value.other;
                rv.insert("values".to_string(), MetaStructure::to_value(value.values));
                Annotated(Some(Value::Object(rv)), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match value {
            &Annotated(Some(ref values), _) => {
                let mut map_ser = s.serialize_map(None)?;
                map_ser.serialize_key("values")?;
                map_ser.serialize_value(&SerializeMetaStructurePayload(&values.values))?;
                for (key, value) in values.other.iter() {
                    map_ser.serialize_key(key)?;
                    map_ser.serialize_value(&SerializeMetaStructurePayload(value))?;
                }
                map_ser.end()
            }
            &Annotated(None, _) => s.serialize_unit(),
        }
    }

    fn extract_meta_tree(value: &Annotated<Self>) -> MetaTree
    where
        Self: Sized,
    {
        let mut meta_tree = MetaTree {
            meta: value.1.clone(),
            children: Default::default(),
        };
        if let Some(ref value) = value.0 {
            let tree = MetaStructure::extract_meta_tree(&value.values);
            if !tree.is_empty() {
                meta_tree.children.insert("values".to_string(), tree);
            }
            for (key, value) in value.other.iter() {
                let tree = MetaStructure::extract_meta_tree(value);
                if !tree.is_empty() {
                    meta_tree.children.insert(key.to_string(), tree);
                }
            }
        }
        meta_tree
    }

    fn process<P: Processor>(
        value: Annotated<Self>,
        processor: &P,
        state: ProcessingState,
    ) -> Annotated<Self> {
        let Annotated(value, meta) = value;
        if let Some(mut value) = value {
            const FIELD_ATTRS: FieldAttrs = FieldAttrs {
                name: Some("values"),
                required: false,
                cap_size: None,
                pii_kind: None,
            };
            value.values = MetaStructure::process(
                value.values,
                processor,
                state.enter_static("values", Some(Cow::Borrowed(&FIELD_ATTRS))),
            );
            value.other = value
                .other
                .into_iter()
                .map(|(key, value)| {
                    let value = MetaStructure::process(
                        value,
                        processor,
                        state.enter_borrowed(key.as_str(), None),
                    );
                    (key, value)
                }).collect();
            Annotated(Some(value), meta)
        } else {
            Annotated(None, meta)
        }
    }
}

/// A register value.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct RegVal(pub u64);

/// An address
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
pub struct Addr(pub u64);

/// Raised if a register value can't be parsed.
#[derive(Fail, Debug)]
#[fail(display = "invalid register value")]
pub struct InvalidRegVal;

macro_rules! hex_metrastructure {
    ($type:ident, $expectation:expr) => {
        impl FromStr for $type {
            type Err = ::std::num::ParseIntError;

            fn from_str(s: &str) -> Result<$type, Self::Err> {
                if s.starts_with("0x") || s.starts_with("0X") {
                    u64::from_str_radix(&s[2..], 16).map($type)
                } else {
                    u64::from_str_radix(&s, 10).map($type)
                }
            }
        }

        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{:#x}", self.0)
            }
        }

        impl MetaStructure for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(err.to_string());
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(Some(Value::U64(value)), meta) => Annotated(Some($type(value)), meta),
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some($type(value as u64)), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(_, mut meta) => {
                        meta.add_error(format!("expected {}", $expectation));
                        Annotated(None, meta)
                    }
                }
            }
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: ::serde::ser::Serializer,
            {
                let &Annotated(ref value, _) = value;
                if let &Some(ref value) = value {
                    ::serde::ser::Serialize::serialize(&value.to_string(), s)
                } else {
                    ::serde::ser::Serialize::serialize(&(), s)
                }
            }
        }
    };
}

hex_metrastructure!(Addr, "address");
hex_metrastructure!(RegVal, "register value");

/// An error used when parsing `Level`.
#[derive(Debug, Fail)]
#[fail(display = "invalid level")]
pub struct ParseLevelError;

/// Severity level of an event or breadcrumb.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// Indicates very spammy debug information.
    Debug,
    /// Informational messages.
    Info,
    /// A warning.
    Warning,
    /// An error.
    Error,
    /// Similar to error but indicates a critical event that usually causes a shutdown.
    Fatal,
}

impl Default for Level {
    fn default() -> Self {
        Level::Info
    }
}

impl Level {
    fn from_python_level(value: u64) -> Option<Level> {
        Some(match value {
            10 => Level::Debug,
            20 => Level::Info,
            30 => Level::Warning,
            40 => Level::Error,
            50 => Level::Fatal,
            _ => return None,
        })
    }
}

impl FromStr for Level {
    type Err = ParseLevelError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "debug" => Level::Debug,
            "info" | "log" => Level::Info,
            "warning" => Level::Warning,
            "error" => Level::Error,
            "fatal" => Level::Fatal,
            _ => return Err(ParseLevelError),
        })
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Level::Debug => write!(f, "debug"),
            Level::Info => write!(f, "info"),
            Level::Warning => write!(f, "warning"),
            Level::Error => write!(f, "error"),
            Level::Fatal => write!(f, "fatal"),
        }
    }
}

impl MetaStructure for Level {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(value) => Annotated(Some(value), meta),
                Err(err) => {
                    meta.add_error(err.to_string());
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::U64(val)), mut meta) => match Level::from_python_level(val) {
                Some(value) => Annotated(Some(value), meta),
                None => {
                    meta.add_error("unknown numeric level");
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(val)), mut meta) => {
                match Level::from_python_level(val as u64) {
                    Some(value) => Annotated(Some(value), meta),
                    None => {
                        meta.add_error("unknown numeric level");
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected level");
                Annotated(None, meta)
            }
        }
    }

    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(Some(Value::String(value.to_string())), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: ::serde::ser::Serializer,
    {
        let &Annotated(ref value, _) = value;
        if let &Some(ref value) = value {
            ::serde::ser::Serialize::serialize(&value.to_string(), s)
        } else {
            ::serde::ser::Serialize::serialize(&(), s)
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(untagged)]
pub enum ThreadId {
    /// Integer representation of the thread id.
    Int(u64),
    /// String representation of the thread id.
    String(String),
}

impl MetaStructure for ThreadId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), meta) => Annotated(Some(ThreadId::String(value)), meta),
            Annotated(Some(Value::U64(value)), meta) => Annotated(Some(ThreadId::Int(value)), meta),
            Annotated(Some(Value::I64(value)), meta) => Annotated(Some(ThreadId::Int(value as u64)), meta),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(_, mut meta) => {
                meta.add_error("expected thread id");
                Annotated(None, meta)
            }
        }
    }

    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(ThreadId::String(value)), meta) => Annotated(Some(Value::String(value)), meta),
            Annotated(Some(ThreadId::Int(value)), meta) => Annotated(Some(Value::U64(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(value: &Annotated<Self>, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: ::serde::ser::Serializer,
    {
        let &Annotated(ref value, _) = value;
        if let &Some(ref value) = value {
            match value {
                ThreadId::String(ref value) => ::serde::ser::Serialize::serialize(value, s),
                ThreadId::Int(value) => ::serde::ser::Serialize::serialize(&value, s),
            }
        } else {
            ::serde::ser::Serialize::serialize(&(), s)
        }
    }
}

#[test]
fn test_values_serialization() {
    let value = Annotated::new(Values {
        values: Annotated::new(vec![
            Annotated::new(0u64),
            Annotated::new(1u64),
            Annotated::new(2u64),
        ]),
        other: Object::default(),
    });
    assert_eq!(value.to_json().unwrap(), "{\"values\":[0,1,2]}");
}

#[test]
fn test_values_deserialization() {
    #[derive(Debug, Clone, MetaStructure, PartialEq)]
    struct Exception {
        #[metastructure(field = "type")]
        ty: Annotated<String>,
        value: Annotated<String>,
    }
    let value = Annotated::<Values<Exception>>::from_json(
        r#"{"values": [{"type": "Test", "value": "aha!"}]}"#,
    ).unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );

    let value = Annotated::<Values<Exception>>::from_json(r#"[{"type": "Test", "value": "aha!"}]"#)
        .unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );

    let value =
        Annotated::<Values<Exception>>::from_json(r#"{"type": "Test", "value": "aha!"}"#).unwrap();
    assert_eq!(
        value,
        Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("Test".to_string()),
            value: Annotated::new("aha!".to_string()),
        })]))
    );
}

#[test]
fn test_hex_to_string() {
    assert_eq_str!("0x0", &Addr(0).to_string());
    assert_eq_str!("0x2a", &Addr(42).to_string());
}

#[test]
fn test_hex_from_string() {
    assert_eq_dbg!(Addr(0), "0".parse().unwrap());
    assert_eq_dbg!(Addr(42), "42".parse().unwrap());
    assert_eq_dbg!(Addr(42), "0x2a".parse().unwrap());
    assert_eq_dbg!(Addr(42), "0X2A".parse().unwrap());
}

#[test]
fn test_hex_serialization() {
    let value = Value::String("0x2a".to_string());
    let addr: Annotated<Addr> = MetaStructure::from_value(Annotated::new(value));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let value = Value::U64(42);
    let addr: Annotated<Addr> = MetaStructure::from_value(Annotated::new(value));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}

#[test]
fn test_hex_deserialization() {
    let addr = Annotated::<Addr>::from_json("\"0x2a\"").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let addr = Annotated::<Addr>::from_json("42").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}

#[test]
fn test_level() {
    assert_eq_dbg!(
        Level::Info,
        Annotated::<Level>::from_json("\"log\"").unwrap().0.unwrap()
    );
    assert_eq_dbg!(
        Level::Warning,
        Annotated::<Level>::from_json("30").unwrap().0.unwrap()
    );
}

#[test]
fn test_thread_id() {
    assert_eq_dbg!(
        ThreadId::String("testing".into()),
        Annotated::<ThreadId>::from_json("\"testing\"").unwrap().0.unwrap()
    );
    assert_eq_dbg!(
        ThreadId::String("42".into()),
        Annotated::<ThreadId>::from_json("\"42\"").unwrap().0.unwrap()
    );
    assert_eq_dbg!(
        ThreadId::Int(42),
        Annotated::<ThreadId>::from_json("42").unwrap().0.unwrap()
    );
}
