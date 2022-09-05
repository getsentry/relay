use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use uuid::Uuid;

use crate::macros::derive_string_meta_structure;
use crate::types::{
    Annotated, Array, Empty, Error, FromValue, IntoValue, Map, Meta, MetaMap, MetaTree, Object,
    SkipSerialization, Value,
};

// This needs to be public because the derive crate emits it
#[doc(hidden)]
pub struct SerializePayload<'a, T>(pub &'a Annotated<T>, pub SkipSerialization);

impl<'a, T: IntoValue> Serialize for SerializePayload<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0.value() {
            Some(value) => value.serialize_payload(serializer, self.1),
            None => serializer.serialize_unit(),
        }
    }
}

macro_rules! derive_from_value {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
        }
    };
}

macro_rules! derive_to_value {
    ($type:ident, $meta_type:ident) => {
        impl IntoValue for $type {
            fn into_value(self) -> Value {
                Value::$meta_type(self)
            }

            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: Serializer,
            {
                self.serialize(s)
            }
        }
    };
}

macro_rules! derive_numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                value.and_then(|value| {
                    let number = match value {
                        Value::U64(x) => num_traits::cast(x),
                        Value::I64(x) => num_traits::cast(x),
                        Value::F64(x) => num_traits::cast(x),
                        _ => None,
                    };

                    match number {
                        Some(x) => Annotated::new(x),
                        None => {
                            let mut meta = Meta::default();
                            meta.add_error(Error::expected($expectation));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    }
                })
            }
        }

        derive_to_value!($type, $meta_type);
    };
}

impl Empty for String {
    #[inline]
    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

derive_from_value!(String, String, "a string");
derive_to_value!(String, String);

impl Empty for bool {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

derive_from_value!(bool, Bool, "a boolean");
derive_to_value!(bool, Bool);

derive_numeric_meta_structure!(u64, U64, "an unsigned integer");
derive_numeric_meta_structure!(i64, I64, "a signed integer");
derive_numeric_meta_structure!(f64, F64, "a floating point number");

impl Empty for u64 {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl Empty for i64 {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl Empty for f64 {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

derive_string_meta_structure!(Uuid, "a uuid");

impl<T> Empty for &'_ T
where
    T: Empty,
{
    #[inline]
    fn is_empty(&self) -> bool {
        (*self).is_empty()
    }

    #[inline]
    fn is_deep_empty(&self) -> bool {
        (*self).is_deep_empty()
    }
}

impl<T> Empty for Option<T>
where
    T: Empty,
{
    #[inline]
    fn is_empty(&self) -> bool {
        self.as_ref().map_or(true, Empty::is_empty)
    }

    #[inline]
    fn is_deep_empty(&self) -> bool {
        self.as_ref().map_or(true, Empty::is_deep_empty)
    }
}

impl<T> Empty for Array<T>
where
    T: Empty,
{
    #[inline]
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_deep_empty(&self) -> bool {
        self.iter().all(Empty::is_deep_empty)
    }
}

impl<T> FromValue for Array<T>
where
    T: FromValue,
{
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Array(items)), meta) => Annotated(
                Some(items.into_iter().map(FromValue::from_value).collect()),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("an array"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl<T> IntoValue for Array<T>
where
    T: IntoValue,
{
    fn into_value(self) -> Value {
        Value::Array(
            self.into_iter()
                .map(|x| Annotated::map_value(x, IntoValue::into_value))
                .collect(),
        )
    }

    fn serialize_payload<S>(&self, s: S, behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let behavior = behavior.descend();
        let mut seq_ser = s.serialize_seq(Some(self.len()))?;
        for item in self {
            if !item.skip_serialization(behavior) {
                seq_ser.serialize_element(&SerializePayload(item, behavior))?;
            }
        }
        seq_ser.end()
    }

    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (idx, item) in self.iter().enumerate() {
            let tree = IntoValue::extract_meta_tree(item);
            if !tree.is_empty() {
                children.insert(idx.to_string(), tree);
            }
        }
        children
    }
}

impl<T> Empty for Object<T>
where
    T: Empty,
{
    #[inline]
    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_deep_empty(&self) -> bool {
        self.values().all(Empty::is_deep_empty)
    }
}

impl<T> FromValue for Object<T>
where
    T: FromValue,
{
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
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("an object"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl<T> IntoValue for Object<T>
where
    T: IntoValue,
{
    fn into_value(self) -> Value {
        Value::Object(
            self.into_iter()
                .map(|(k, v)| (k, Annotated::map_value(v, IntoValue::into_value)))
                .collect(),
        )
    }

    fn serialize_payload<S>(&self, s: S, behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        let behavior = behavior.descend();
        let mut map_ser = s.serialize_map(Some(self.len()))?;
        for (key, value) in self {
            if !value.skip_serialization(behavior) {
                map_ser.serialize_key(&key)?;
                map_ser.serialize_value(&SerializePayload(value, behavior))?;
            }
        }
        map_ser.end()
    }

    fn extract_child_meta(&self) -> Map<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        for (key, value) in self.iter() {
            let tree = IntoValue::extract_meta_tree(value);
            if !tree.is_empty() {
                children.insert(key.to_string(), tree);
            }
        }
        children
    }
}

impl Empty for Value {
    fn is_empty(&self) -> bool {
        match self {
            Value::Bool(_) => false,
            Value::I64(_) => false,
            Value::U64(_) => false,
            Value::F64(_) => false,
            Value::String(v) => v.is_empty(),
            Value::Array(v) => v.is_empty(),
            Value::Object(v) => v.is_empty(),
        }
    }

    fn is_deep_empty(&self) -> bool {
        match self {
            Value::Bool(_) => false,
            Value::I64(_) => false,
            Value::U64(_) => false,
            Value::F64(_) => false,
            Value::String(v) => v.is_deep_empty(),
            Value::Array(v) => v.is_deep_empty(),
            Value::Object(v) => v.is_deep_empty(),
        }
    }
}

impl FromValue for Value {
    #[inline]
    fn from_value(value: Annotated<Value>) -> Annotated<Value> {
        value
    }
}

impl IntoValue for Value {
    fn into_value(self) -> Value {
        self
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        Serialize::serialize(self, s)
    }

    fn extract_child_meta(&self) -> Map<String, MetaTree>
    where
        Self: Sized,
    {
        let mut children = MetaMap::new();
        match self {
            Value::Object(items) => {
                for (key, value) in items.iter() {
                    let tree = IntoValue::extract_meta_tree(value);
                    if !tree.is_empty() {
                        children.insert(key.to_string(), tree);
                    }
                }
            }
            Value::Array(items) => {
                for (idx, item) in items.iter().enumerate() {
                    let tree = IntoValue::extract_meta_tree(item);
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

impl<T> FromValue for Box<T>
where
    T: FromValue,
{
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        let annotated: Annotated<T> = FromValue::from_value(value);
        Annotated(annotated.0.map(Box::new), annotated.1)
    }
}

impl<T> IntoValue for Box<T>
where
    T: IntoValue,
{
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        IntoValue::into_value(*self)
    }

    fn extract_child_meta(&self) -> MetaMap
    where
        Self: Sized,
    {
        IntoValue::extract_child_meta(&**self)
    }

    fn serialize_payload<S>(&self, s: S, behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: Serializer,
    {
        IntoValue::serialize_payload(&**self, s, behavior)
    }
}

impl<T> Empty for Box<T>
where
    T: Empty,
{
    #[inline]
    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    #[inline]
    fn is_deep_empty(&self) -> bool {
        self.as_ref().is_deep_empty()
    }
}

/// Checks whether annotated data contains either a value or meta data.
impl<T> Empty for Annotated<T>
where
    T: Empty,
{
    /// Returns if this object contains data to be serialized.
    ///
    /// **Caution:** This has different behavior than `annotated.value().is_empty()`! Annotated is
    /// **not** empty if there is meta data that needs to be serialized. This is in line with the
    /// derived implementation of `Empty` on structs, which calls `Annotated::is_empty` on every
    /// child.
    ///
    /// To check if a value is missing or empty, use `Option::is_empty` on the value instead.
    fn is_empty(&self) -> bool {
        self.skip_serialization(SkipSerialization::Empty(false))
    }

    /// Returns if this object contains nested data to be serialized.
    fn is_deep_empty(&self) -> bool {
        self.skip_serialization(SkipSerialization::Empty(true))
    }
}

macro_rules! tuple_meta_structure {
    ($count: literal, $($name: ident),+) => {
        impl< $( $name: FromValue ),* > FromValue for ( $( Annotated<$name>, )* ) {
            #[allow(non_snake_case, unused_variables)]
            fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
                let expectation = match $count {
                    1 => "a single element",
                    2 => "a tuple",
                    _ => concat!("a ", $count, "-tuple"),
                };

                let mut n = 0;
                $(let $name = (); n += 1;)*
                match annotated {
                    Annotated(Some(Value::Array(items)), mut meta) => {
                        if items.len() != n {
                            meta.add_error(Error::expected(expectation));
                            meta.set_original_value(Some(items));
                            return Annotated(None, meta);
                        }

                        let mut iter = items.into_iter();
                        Annotated(Some((
                            $({
                                let $name = ();
                                FromValue::from_value(iter.next().unwrap())
                            },)*
                        )), meta)
                    }
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(Error::expected(expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                    Annotated(None, meta) => Annotated(None, meta)
                }
            }
        }

        impl< $( $name: IntoValue ),* > IntoValue for ( $( Annotated<$name>, )* ) {
            #[allow(non_snake_case, unused_variables)]
            fn into_value(self) -> Value {
                let ($($name,)*) = self;
                Value::Array(vec![$(Annotated::map_value($name, IntoValue::into_value),)*])
            }

            #[allow(non_snake_case, unused_variables)]
            fn serialize_payload<S>(&self, s: S, behavior: crate::types::SkipSerialization) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let behavior = behavior.descend();
                let mut s = s.serialize_seq(None)?;
                let ($($name,)*) = self;
                $(s.serialize_element(&SerializePayload($name, behavior))?;)*
                s.end()
            }

            #[allow(non_snake_case, unused_variables, unused_assignments)]
            fn extract_child_meta(&self) -> MetaMap
            where
                Self: Sized,
            {
                let mut children = MetaMap::new();
                let ($($name,)*) = self;
                let mut idx = 0;
                $({
                    let tree = IntoValue::extract_meta_tree($name);
                    if !tree.is_empty() {
                        children.insert(idx.to_string(), tree);
                    }
                    idx += 1;
                })*;
                children
            }
        }

        impl< $( $name: Empty ),* > Empty for ( $( Annotated<$name>, )* ) {
            #[inline]
            fn is_empty(&self) -> bool {
                false
            }

            #[inline]
            #[allow(non_snake_case)]
            fn is_deep_empty(&self) -> bool {
                let ($($name,)*) = self;
                true $(&& $name.is_deep_empty())*
            }
        }
    }
}

tuple_meta_structure!(1, T1);
tuple_meta_structure!(2, T1, T2);
tuple_meta_structure!(3, T1, T2, T3);
tuple_meta_structure!(4, T1, T2, T3, T4);
tuple_meta_structure!(5, T1, T2, T3, T4, T5);
tuple_meta_structure!(6, T1, T2, T3, T4, T5, T6);
tuple_meta_structure!(7, T1, T2, T3, T4, T5, T6, T7);
tuple_meta_structure!(8, T1, T2, T3, T4, T5, T6, T7, T8);
tuple_meta_structure!(9, T1, T2, T3, T4, T5, T6, T7, T8, T9);
tuple_meta_structure!(10, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
tuple_meta_structure!(11, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
tuple_meta_structure!(12, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_unsigned_integers() {
        assert_eq!(Annotated::new(1u64), Annotated::from_json("1").unwrap(),);

        assert_eq!(
            Annotated::from_error(Error::expected("an unsigned integer"), Some(Value::I64(-1))),
            Annotated::<u64>::from_json("-1").unwrap(),
        );

        // Floats are rounded implicitly
        assert_eq!(Annotated::new(4u64), Annotated::from_json("4.2").unwrap(),);
    }

    #[test]
    fn test_signed_integers() {
        assert_eq!(Annotated::new(1i64), Annotated::from_json("1").unwrap(),);

        assert_eq!(Annotated::new(-1i64), Annotated::from_json("-1").unwrap(),);

        // Floats are rounded implicitly
        assert_eq!(Annotated::new(4u64), Annotated::from_json("4.2").unwrap(),);
    }

    #[test]
    fn test_floats() {
        assert_eq!(Annotated::new(1f64), Annotated::from_json("1").unwrap(),);

        assert_eq!(Annotated::new(-1f64), Annotated::from_json("-1").unwrap(),);

        assert_eq!(Annotated::new(4.2f64), Annotated::from_json("4.2").unwrap(),);
    }

    #[test]
    fn test_skip_array_never() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "never")]
            items: Annotated<Array<String>>,
        }

        // "never" always serializes
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
    }

    #[test]
    fn test_skip_array_null() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null")]
            items: Annotated<Array<String>>,
        }

        // "null" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null" does not apply to empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[]}"#);
    }

    #[test]
    fn test_skip_array_null_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null_deep")]
            items: Annotated<Array<String>>,
        }

        // "null_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null_deep" applies to nested null
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::default()]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[]}"#);

        // "null_deep" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::new(String::new())]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
    }

    #[test]
    fn test_skip_array_empty() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty")]
            items: Annotated<Array<String>>,
        }

        // "empty" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty" applies to empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty" -> "never" nested
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::default()]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

        // "empty" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::new(String::new())]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
    }

    #[test]
    fn test_skip_array_empty_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty_deep")]
            items: Annotated<Array<String>>,
        }

        // "empty_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" applies to empty nested
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::new(String::new())]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" does not apply to non-empty value
        let helper = Annotated::new(Helper {
            items: Annotated::new(vec![Annotated::new("some".to_string())]),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":["some"]}"#);
    }

    #[test]
    fn test_skip_object_never() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "never")]
            items: Annotated<Object<String>>,
        }

        // "never" always serializes
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
    }

    #[test]
    fn test_skip_object_null() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null")]
            items: Annotated<Object<String>>,
        }

        // "null" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null" does not apply to empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(Object::new()),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{}}"#);
    }

    #[test]
    fn test_skip_object_null_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null_deep")]
            items: Annotated<Object<String>>,
        }

        // "null_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null_deep" applies to nested null
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::default());
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{}}"#);

        // "null_deep" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::new(String::new()));
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":""}}"#);
    }

    #[test]
    fn test_skip_object_empty() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty")]
            items: Annotated<Object<String>>,
        }

        // "empty" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty" applies to empty
        let helper = Annotated::new(Helper {
            items: Annotated::new(Object::new()),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty" -> "never" nested
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::default());
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":null}}"#);

        // "empty" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::new(String::new()));
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":""}}"#);
    }

    #[test]
    fn test_skip_object_empty_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty_deep")]
            items: Annotated<Object<String>>,
        }

        // "empty_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" applies to empty nested
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::new(String::new()));
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" does not apply to non-empty value
        let helper = Annotated::new(Helper {
            items: Annotated::new({
                let mut obj = Object::<String>::new();
                obj.insert("foo".to_string(), Annotated::new("some".to_string()));
                obj
            }),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":"some"}}"#);
    }

    #[test]
    fn test_skip_tuple_never() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "never")]
            items: Annotated<(Annotated<String>,)>,
        }

        // "never" always serializes
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
    }

    #[test]
    fn test_skip_tuple_null() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null")]
            items: Annotated<(Annotated<String>,)>,
        }

        // "null" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null" does not apply to nested
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::default(),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);
    }

    #[test]
    fn test_skip_tuple_null_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null_deep")]
            items: Annotated<(Annotated<String>,)>,
        }

        // "null_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "null_deep" cannot remove tuple items
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::default(),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

        // "null_deep" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::new(String::new()),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
    }

    #[test]
    fn test_skip_tuple_empty() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty")]
            items: Annotated<(Annotated<String>,)>,
        }

        // "empty" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty" -> "null_deep" nested, but cannot remove tuple item
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::default(),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

        // "empty" does not apply to nested empty
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::new(String::new()),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
    }

    #[test]
    fn test_skip_tuple_empty_deep() {
        #[derive(Debug, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "empty_deep")]
            items: Annotated<(Annotated<String>,)>,
        }

        // "empty_deep" applies to null
        let helper = Annotated::new(Helper {
            items: Annotated::default(),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" applies to empty nested
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::new(String::new()),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{}"#);

        // "empty_deep" does not apply to non-empty value
        let helper = Annotated::new(Helper {
            items: Annotated::new((Annotated::new("some".to_string()),)),
        });
        assert_eq!(helper.to_json().unwrap(), r#"{"items":["some"]}"#);
    }

    #[test]
    fn test_wrapper_structs_and_skip_serialization() {
        #[derive(Debug, Empty, IntoValue)]
        struct BasicWrapper(Array<String>);

        #[derive(Debug, Empty, IntoValue)]
        struct BasicHelper {
            #[metastructure(skip_serialization = "empty")]
            items: Annotated<BasicWrapper>,
        }

        let helper = Annotated::new(BasicHelper {
            items: Annotated::new(BasicWrapper(vec![])),
        });
        assert_eq!(helper.to_json().unwrap(), "{}");
    }

    #[test]
    fn test_skip_serialization_on_regular_structs() {
        #[derive(Debug, Default, Empty, IntoValue)]
        struct Wrapper {
            foo: Annotated<u64>,
        }

        #[derive(Debug, Default, Empty, IntoValue)]
        struct Helper {
            #[metastructure(skip_serialization = "null")]
            foo: Annotated<Wrapper>,
        }

        let helper = Annotated::new(Helper {
            foo: Annotated::new(Wrapper::default()),
        });

        assert_eq!(helper.to_json().unwrap(), r#"{"foo":{}}"#);
    }
}
