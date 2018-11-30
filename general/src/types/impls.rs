use std::collections::BTreeMap;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use uuid::Uuid;

use crate::types::{Annotated, Array, FromValue, MetaMap, MetaTree, Object, ToValue, Value};

// This needs to be public because the derive crate emits it
#[doc(hidden)]
pub struct SerializePayload<'a, T: 'a>(pub &'a Annotated<T>);

impl<'a, T: ToValue> Serialize for SerializePayload<'a, T> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match *self.0 {
            Annotated(Some(ref value), _) => ToValue::serialize_payload(value, s),
            Annotated(None, _) => s.serialize_unit(),
        }
    }
}

primitive_meta_structure!(String, String, "a string");
primitive_meta_structure!(bool, Bool, "a boolean");
numeric_meta_structure!(u64, U64, "an unsigned integer");
numeric_meta_structure!(i64, I64, "a signed integer");
numeric_meta_structure!(f64, F64, "a floating point value");
primitive_meta_structure_through_string!(Uuid, "a uuid");

impl<T: FromValue> FromValue for Array<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(items.into_iter().map(FromValue::from_value).collect()),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("array", value);
                Annotated(None, meta)
            }
        }
    }
}

impl<T: ToValue> ToValue for Array<T> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Array(
                    value.into_iter().map(ToValue::to_value).collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let mut seq_ser = s.serialize_seq(Some(self.len()))?;
        for item in self {
            seq_ser.serialize_element(&SerializePayload(item))?;
        }
        seq_ser.end()
    }
    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (idx, item) in self.iter().enumerate() {
            let tree = ToValue::extract_meta_tree(item);
            if !tree.is_empty() {
                children.insert(idx.to_string(), tree);
            }
        }
        children
    }

    fn skip_serialization(&self) -> bool {
        for item in self.iter() {
            if !item.skip_serialization() {
                return false;
            }
        }
        true
    }
}

impl<T: FromValue> FromValue for Object<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), meta) => Annotated(
                Some(
                    items
                        .into_iter()
                        .map(|(k, v)| (k, FromValue::from_value(v)))
                        .collect(),
                ),
                meta,
            ),
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("object", value);
                Annotated(None, meta)
            }
        }
    }
}

impl<T: ToValue> ToValue for Object<T> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => Annotated(
                Some(Value::Object(
                    value
                        .into_iter()
                        .map(|(k, v)| (k, ToValue::to_value(v)))
                        .collect(),
                )),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let mut map_ser = s.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            if !value.skip_serialization() {
                map_ser.serialize_key(&key)?;
                map_ser.serialize_value(&SerializePayload(value))?;
            }
        }
        map_ser.end()
    }

    fn extract_child_meta(&self) -> BTreeMap<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (key, value) in self.iter() {
            let tree = ToValue::extract_meta_tree(value);
            if !tree.is_empty() {
                children.insert(key.to_string(), tree);
            }
        }
        children
    }

    fn skip_serialization(&self) -> bool {
        for (_, value) in self.iter() {
            if !value.skip_serialization() {
                return false;
            }
        }
        true
    }
}

impl FromValue for Value {
    fn from_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }
}

impl ToValue for Value {
    fn to_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self, s)
    }

    fn extract_child_meta(&self) -> BTreeMap<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        match *self {
            Value::Object(ref items) => {
                for (key, value) in items.iter() {
                    let tree = ToValue::extract_meta_tree(value);
                    if !tree.is_empty() {
                        children.insert(key.to_string(), tree);
                    }
                }
            }
            Value::Array(ref items) => {
                for (idx, item) in items.iter().enumerate() {
                    let tree = ToValue::extract_meta_tree(item);
                    if !tree.is_empty() {
                        children.insert(idx.to_string(), tree);
                    }
                }
            }
            _ => {}
        }
        children
    }
}

fn datetime_to_timestamp(dt: DateTime<Utc>) -> f64 {
    let micros = f64::from(dt.timestamp_subsec_micros()) / 1_000_000f64;
    dt.timestamp() as f64 + micros
}

impl FromValue for DateTime<Utc> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => {
                let parsed = match value.parse::<NaiveDateTime>() {
                    Ok(dt) => Ok(DateTime::from_utc(dt, Utc)),
                    Err(_) => value.parse(),
                };
                match parsed {
                    Ok(value) => Annotated(Some(value), meta),
                    Err(err) => {
                        meta.add_error(err.to_string(), Some(Value::String(value.to_string())));
                        Annotated(None, meta)
                    }
                }
            }
            Annotated(Some(Value::U64(ts)), meta) => {
                Annotated(Some(Utc.timestamp_opt(ts as i64, 0).unwrap()), meta)
            }
            Annotated(Some(Value::I64(ts)), meta) => {
                Annotated(Some(Utc.timestamp_opt(ts, 0).unwrap()), meta)
            }
            Annotated(Some(Value::F64(ts)), meta) => {
                let secs = ts as i64;
                let micros = (ts.fract() * 1_000_000f64) as u32;
                Annotated(Some(Utc.timestamp_opt(secs, micros * 1000).unwrap()), meta)
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("timestamp", value);
                Annotated(None, meta)
            }
        }
    }
}

impl ToValue for DateTime<Utc> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value> {
        match value {
            Annotated(Some(value), meta) => {
                Annotated(Some(Value::F64(datetime_to_timestamp(value))), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(&datetime_to_timestamp(*self), s)
    }
}

impl<T: FromValue> FromValue for Box<T> {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        let annotated: Annotated<T> = FromValue::from_value(value);
        Annotated(annotated.0.map(Box::new), annotated.1)
    }
}

impl<T: ToValue> ToValue for Box<T> {
    fn to_value(value: Annotated<Self>) -> Annotated<Value>
    where
        Self: Sized,
    {
        ToValue::to_value(Annotated(value.0.map(|x| *x), value.1))
    }

    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        ToValue::extract_child_meta(&**self)
    }

    fn serialize_payload<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        ToValue::serialize_payload(&**self, s)
    }
}

#[test]
fn test_unsigned_integers() {
    assert_eq!(
        Annotated::<u64>::from_json("-1").unwrap(),
        Annotated::from_error("expected an unsigned integer", Some(Value::I64(-1)))
    );
}

#[test]
fn test_empty_containers_skipped() {
    #[derive(Debug, ToValue)]
    struct Helper {
        items: Annotated<Array<String>>,
    }

    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![]),
    });

    assert_eq_str!(helper.to_json().unwrap(), "{}");
}

#[test]
fn test_empty_containers_not_skipped_if_configured() {
    #[derive(Debug, ToValue)]
    #[metastructure(skip_serialization = "never")]
    struct NeverSkip(Array<String>);

    #[derive(Debug, ToValue)]
    struct NeverSkipHelper {
        items: Annotated<NeverSkip>,
    }

    let helper = Annotated::new(NeverSkipHelper {
        items: Annotated::new(NeverSkip(vec![])),
    });
    assert_eq_str!(helper.to_json().unwrap(), r#"{"items":[]}"#);
}

#[test]
fn test_wrapper_structs_and_skip_serialization() {
    #[derive(Debug, ToValue)]
    struct BasicWrapper(Array<String>);

    #[derive(Debug, ToValue)]
    struct BasicHelper {
        items: Annotated<BasicWrapper>,
    }

    let helper = Annotated::new(BasicHelper {
        items: Annotated::new(BasicWrapper(vec![])),
    });
    assert_eq_str!(helper.to_json().unwrap(), "{}");
}

#[test]
fn test_skip_serialization_on_regular_structs() {
    #[derive(Debug, Default, ToValue)]
    #[metastructure(skip_serialization = "never")]
    struct Wrapper {
        foo: Annotated<u64>,
    }

    #[derive(Debug, Default, ToValue)]
    struct Helper {
        foo: Annotated<Wrapper>,
    }

    let helper = Annotated::new(Helper {
        foo: Annotated::new(Wrapper::default()),
    });

    assert_eq_str!(helper.to_json().unwrap(), r#"{"foo":{}}"#);
}
