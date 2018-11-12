//! Common types of the protocol.
use std::fmt;
use std::net;
use std::str::FromStr;

use failure::Fail;
use serde::ser::{Serialize, SerializeSeq, Serializer};
use serde_derive::{Deserialize, Serialize};

use crate::processor::{
    FromValue, ProcessValue, ProcessingState, Processor, SerializePayload, ToValue,
};
use crate::types::{Annotated, Array, Meta, MetaMap, Object, Value};

/// A array like wrapper used in various places.
#[derive(Clone, Debug, PartialEq, ToValue, ProcessValue)]
pub struct Values<T> {
    /// The values of the collection.
    pub values: Annotated<Array<T>>,
    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl<T> Default for Values<T> {
    fn default() -> Values<T> {
        // Default implemented manually even if <T> does not impl Default.
        Values::new(Vec::new())
    }
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

impl<T: FromValue> FromValue for Values<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(Values {
                    values: Annotated(
                        Some(items.into_iter().map(FromValue::from_value).collect()),
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
                            values: FromValue::from_value(values),
                            other: obj,
                        }),
                        meta,
                    )
                } else {
                    Annotated(
                        Some(Values {
                            values: Annotated(
                                Some(vec![FromValue::from_value(Annotated(
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
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("array or values", value);
                Annotated(None, meta)
            }
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
            type Err = std::num::ParseIntError;

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

        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(err.to_string(), Some(Value::String(value.to_string())));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(Some(Value::U64(value)), meta) => Annotated(Some($type(value)), meta),
                    Annotated(Some(Value::I64(value)), meta) => {
                        Annotated(Some($type(value as u64)), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_unexpected_value_error($expectation, value);
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl ToValue for $type {
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                Serialize::serialize(&self.to_string(), s)
            }
        }

        impl ProcessValue for $type {}
    };
}

hex_metrastructure!(Addr, "address");
hex_metrastructure!(RegVal, "register value");

/// An ip address.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, ToValue, ProcessValue)]
pub struct IpAddr(pub String);

impl IpAddr {
    /// Returns the auto marker ip address.
    pub fn auto() -> IpAddr {
        IpAddr("{{auto}}".into())
    }

    /// Checks if the ip address is set to the auto marker.
    pub fn is_auto(&self) -> bool {
        self.0 == "{{auto}}"
    }

    /// Returns the string value of this ip address.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromValue for IpAddr {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                if value == "{{auto}}" || net::IpAddr::from_str(&value).is_ok() {
                    return Annotated(Some(IpAddr(value)), meta);
                }
                meta.add_unexpected_value_error("an ip address", Value::String(value));
                Annotated(None, meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("an ip address", value);
                Annotated(None, meta)
            }
        }
    }
}

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

impl FromValue for Level {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(value) => Annotated(Some(value), meta),
                Err(err) => {
                    meta.add_error(err.to_string(), Some(Value::String(value.to_string())));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::U64(val)), mut meta) => match Level::from_python_level(val) {
                Some(value) => Annotated(Some(value), meta),
                None => {
                    meta.add_error("unknown numeric level", Some(Value::U64(val)));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(val)), mut meta) => {
                match Level::from_python_level(val as u64) {
                    Some(value) => Annotated(Some(value), meta),
                    None => {
                        meta.add_error("unknown numeric level", Some(Value::I64(val)));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("level", value);
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for Level {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(Some(Value::String(value.to_string())), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for Level {}

/// A "into-string" type of value. Emulates an invocation of `str(x)` in Python
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, ToValue, ProcessValue)]
pub struct LenientString(pub String);

impl LenientString {
    /// Returns the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::convert::AsRef<str> for LenientString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for LenientString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromValue for LenientString {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(string)), meta) => Annotated(Some(string), meta),
            // XXX: True/False instead of true/false because of old python code
            Annotated(Some(Value::Bool(true)), meta) => Annotated(Some("True".to_string()), meta),
            Annotated(Some(Value::Bool(false)), meta) => Annotated(Some("False".to_string()), meta),
            Annotated(Some(Value::U64(num)), meta) => Annotated(Some(num.to_string()), meta),
            Annotated(Some(Value::I64(num)), meta) => Annotated(Some(num.to_string()), meta),
            Annotated(Some(Value::F64(num)), mut meta) => {
                if num.abs() < (1i64 << 53) as f64 {
                    Annotated(Some(num.trunc().to_string()), meta)
                } else {
                    meta.add_error("non integer value", Some(Value::F64(num)));
                    Annotated(None, meta)
                }
            }
            Annotated(None, meta) | Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("primitive", value);
                Annotated(None, meta)
            }
        }.map_value(LenientString)
    }
}

/// A "into-string" type of value. All non-string values are serialized as JSON.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, ToValue, ProcessValue)]
pub struct JsonLenientString(pub String);

impl JsonLenientString {
    /// Returns the string value.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::convert::AsRef<str> for JsonLenientString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for JsonLenientString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromValue for JsonLenientString {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(string)), meta) => Annotated(Some(string.into()), meta),
            Annotated(None, meta) => Annotated(None, meta),
            x => Annotated(Some(x.payload_to_json().unwrap().into()), x.1),
        }
    }
}

impl From<String> for JsonLenientString {
    fn from(value: String) -> JsonLenientString {
        JsonLenientString(value)
    }
}

/// Represents a thread id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[serde(untagged)]
pub enum ThreadId {
    /// Integer representation of the thread id.
    Int(u64),
    /// String representation of the thread id.
    String(String),
}

impl FromValue for ThreadId {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), meta) => {
                Annotated(Some(ThreadId::String(value)), meta)
            }
            Annotated(Some(Value::U64(value)), meta) => Annotated(Some(ThreadId::Int(value)), meta),
            Annotated(Some(Value::I64(value)), meta) => {
                Annotated(Some(ThreadId::Int(value as u64)), meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("thread id", value);
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for ThreadId {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(ThreadId::String(value)), meta) => {
                Annotated(Some(Value::String(value)), meta)
            }
            Annotated(Some(ThreadId::Int(value)), meta) => Annotated(Some(Value::U64(value)), meta),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        match *self {
            ThreadId::String(ref value) => Serialize::serialize(value, s),
            ThreadId::Int(value) => Serialize::serialize(&value, s),
        }
    }
}

impl ProcessValue for ThreadId {}

macro_rules! value_impl_for_tuple {
    () => ();
    ($($name:ident,)+) => (
        impl<$($name: FromValue),*> FromValue for ($(Annotated<$name>,)*) {
            #[allow(non_snake_case, unused_variables)]
            fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
                let mut n = 0;
                $(let $name = (); n += 1;)*
                match annotated {
                    Annotated(Some(Value::Array(items)), mut meta) => {
                        if items.len() != n {
                            meta.add_unexpected_value_error("tuple", Value::Array(items));
                            return Annotated(None, meta);
                        }

                        let mut iter = items.into_iter();
                        Annotated(Some(($({
                            let $name = ();
                            FromValue::from_value(iter.next().unwrap())
                        },)*)), meta)
                    }
                    Annotated(Some(value), mut meta) => {
                        meta.add_unexpected_value_error("tuple", value);
                        Annotated(None, meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta)
                }
            }
        }

        impl<$($name: ToValue),*> ToValue for ($(Annotated<$name>,)*) {
            #[allow(non_snake_case, unused_variables)]
            fn to_value(annotated: Annotated<Self>) -> Annotated<Value> {
                match annotated {
                    Annotated(Some(($($name,)*)), meta) => {
                        Annotated(Some(Value::Array(vec![
                            $(ToValue::to_value($name),)*
                        ])), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta)
                }
            }

            #[allow(non_snake_case, unused_variables)]
            fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let mut s = s.serialize_seq(None)?;
                let ($(ref $name,)*) = self;
                $(s.serialize_element(&SerializePayload($name))?;)*;
                s.end()
            }

            #[allow(non_snake_case, unused_variables, unused_assignments)]
            fn extract_child_meta(&self) -> MetaMap
            where
                Self: Sized,
            {
                let mut children = MetaMap::new();
                let ($(ref $name,)*) = self;
                let mut idx = 0;
                $({
                    let tree = ToValue::extract_meta_tree($name);
                    if !tree.is_empty() {
                        children.insert(idx.to_string(), tree);
                    }
                    idx += 1;
                })*;
                children
            }
        }

        #[allow(non_snake_case, unused_variables, unused_assignments)]
        impl<$($name: ProcessValue),*> ProcessValue for ($(Annotated<$name>,)*) {
            fn process_value<P: Processor>(
                value: Annotated<Self>,
                processor: &mut P,
                state: ProcessingState,
            ) -> Annotated<Self> {
                value.map_value(|value| {
                    let ($($name,)*) = value;
                    let mut idx = 0;
                    ($(#[cfg_attr(feature = "cargo-clippy", allow(eval_order_dependence))]{
                        let rv = ProcessValue::process_value($name, processor, state.enter_index(idx, None));
                        idx += 1;
                        rv
                    },)*)
                })
            }
        }
        value_impl_for_tuple_peel!($($name,)*);
    )
}

macro_rules! value_impl_for_tuple_peel {
    ($name:ident, $($other:ident,)*) => (value_impl_for_tuple!($($other,)*);)
}

value_impl_for_tuple! { T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, }

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
    #[derive(Debug, Clone, FromValue, ToValue, PartialEq)]
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
    let addr: Annotated<Addr> = FromValue::from_value(Annotated::new(value));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let value = Value::U64(42);
    let addr: Annotated<Addr> = FromValue::from_value(Annotated::new(value));
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
        Annotated::<ThreadId>::from_json("\"testing\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        ThreadId::String("42".into()),
        Annotated::<ThreadId>::from_json("\"42\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        ThreadId::Int(42),
        Annotated::<ThreadId>::from_json("42").unwrap().0.unwrap()
    );
}

#[test]
fn test_ip_addr() {
    assert_eq_dbg!(
        IpAddr("{{auto}}".into()),
        Annotated::<IpAddr>::from_json("\"{{auto}}\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        IpAddr("127.0.0.1".into()),
        Annotated::<IpAddr>::from_json("\"127.0.0.1\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        IpAddr("::1".into()),
        Annotated::<IpAddr>::from_json("\"::1\"")
            .unwrap()
            .0
            .unwrap()
    );
    assert_eq_dbg!(
        Annotated::from_error(
            "expected an ip address",
            Some(Value::String("clearly invalid value".into()))
        ),
        Annotated::<IpAddr>::from_json("\"clearly invalid value\"").unwrap()
    );
}
