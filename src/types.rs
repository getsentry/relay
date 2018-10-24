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
#[derive(Clone, Debug)]
pub struct Values<T> {
    /// The values of the collection.
    pub values: Annotated<Array<T>>,
    /// Additional arbitrary fields for forwards compatibility.
    pub other: Object<Value>,
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
            Annotated(Some(Value::Object(mut obj)), mut meta) => {
                if let Some(values) = obj.remove("values") {
                    Annotated(
                        Some(Values {
                            values: MetaStructure::from_value(values),
                            other: obj,
                        }),
                        meta,
                    )
                } else {
                    meta.add_error("expected array or values".to_string());
                    Annotated(None, meta)
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
            #[inline(always)]
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
            #[inline(always)]
            fn to_value(value: Annotated<Self>) -> Annotated<Value> {
                match value {
                    Annotated(Some(value), meta) => {
                        Annotated(Some(Value::String(value.to_string())), meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta),
                }
            }
            #[inline(always)]
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
    let addr: Annotated<Addr> = MetaStructure::from_value(Annotated(Some(value), Meta::default()));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let value = Value::U64(42);
    let addr: Annotated<Addr> = MetaStructure::from_value(Annotated(Some(value), Meta::default()));
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}

#[test]
fn test_hex_deserialization() {
    let addr = Annotated::<Addr>::from_json("\"0x2a\"").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
    let addr = Annotated::<Addr>::from_json("42").unwrap();
    assert_eq!(addr.payload_to_json().unwrap(), "\"0x2a\"");
}
