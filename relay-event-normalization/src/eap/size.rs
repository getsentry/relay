use relay_event_schema::protocol::{Attribute, Attributes};
use relay_protocol::{Annotated, Object, Value};

/// The size of a `bool` value for the purpose of attributes.
pub const BOOL_SIZE: usize = 1;
/// The size of a `u64` value for the purpose of attributes.
pub const U64_SIZE: usize = 8;
/// The size of an `i64` value for the purpose of attributes.
pub const I64_SIZE: usize = 8;
/// The size of an `f64` value for the purpose of attributes.
pub const F64_SIZE: usize = 8;

/// Calculates the canonical size of [`Attributes`].
///
/// Simple data types have a fixed size assigned to them:
///  - Boolean: 1 byte
///  - Integer: 8 byte
///  - Double: 8 Byte
///  - Strings are counted by their byte (UTF-8 encoded) representation.
///
/// Complex types like objects and arrays are counted as the sum of all contained simple data types.
///
/// The size of all attributes is the sum of all attribute values and their keys.
pub fn attributes_size(attributes: &Attributes) -> usize {
    attributes
        .0
        .iter()
        .map(|(k, v)| k.len() + attribute_size(v))
        .sum()
}

/// Calculates the size of a single attribute.
///
/// As described in [`attributes_size`], only the value of the attribute is considered for the size
/// of an attribute.
pub fn attribute_size(v: &Annotated<Attribute>) -> usize {
    v.value().map(|v| &v.value.value).map_or(0, value_size)
}

/// Recursively calculates the size of a [`Value`], using the rules described in [`attributes_size`].
pub fn value_size(v: &Annotated<Value>) -> usize {
    let Some(v) = v.value() else {
        return 0;
    };

    match v {
        Value::Bool(_) => BOOL_SIZE,
        Value::I64(_) => I64_SIZE,
        Value::U64(_) => U64_SIZE,
        Value::F64(_) => F64_SIZE,
        Value::String(v) => string_size(v),
        Value::Array(v) => array_size(v),
        Value::Object(v) => object_size(v),
    }
}

/// The size of a `String` value for the purpose of attributes.
pub fn string_size(value: &str) -> usize {
    value.len()
}

/// The size of an `Array` value  for the purpose of attributes.
pub fn array_size(value: &[Annotated<Value>]) -> usize {
    value.iter().map(value_size).sum()
}

/// The size of an `Object` value  for the purpose of attributes.
pub fn object_size(value: &Object<Value>) -> usize {
    value.iter().map(|(k, v)| k.len() + value_size(v)).sum()
}

#[cfg(test)]
mod tests {
    use relay_protocol::{Error, Object};

    use super::*;

    #[test]
    fn test_value_size_basic() {
        assert_eq!(value_size(&Value::Bool(true).into()), 1);
        assert_eq!(value_size(&Value::Bool(false).into()), 1);
        assert_eq!(value_size(&Value::I64(0).into()), 8);
        assert_eq!(value_size(&Value::I64(i64::MIN).into()), 8);
        assert_eq!(value_size(&Value::I64(i64::MAX).into()), 8);
        assert_eq!(value_size(&Value::U64(0).into()), 8);
        assert_eq!(value_size(&Value::U64(u64::MIN).into()), 8);
        assert_eq!(value_size(&Value::U64(u64::MAX).into()), 8);
        assert_eq!(value_size(&Value::F64(123.42).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::MAX).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::MIN).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::NAN).into()), 8);
        assert_eq!(value_size(&Value::F64(f64::NEG_INFINITY).into()), 8);
        assert_eq!(value_size(&Value::String("foobar".to_owned()).into()), 6);
        assert_eq!(value_size(&Value::String("ඞ".to_owned()).into()), 3);
        assert_eq!(value_size(&Value::String("".to_owned()).into()), 0);
    }

    #[test]
    fn test_value_size_array() {
        let array = Value::Array(vec![
            Annotated::empty(),
            Annotated::new(Value::Bool(true)),
            Annotated::new(Value::Bool(false)),
            Annotated::from_error(Error::invalid("oops"), Some(Value::U64(0))),
            Annotated::new(Value::String("42".to_owned())),
            Annotated::new(Value::Array(vec![])),
            Annotated::new(Value::Array(vec![
                Annotated::new(Value::Array(vec![Annotated::new(Value::Bool(false))])),
                Annotated::new(Value::Object(Object::from([
                    ("ඞ".to_owned(), Annotated::new(Value::I64(3))),
                    ("empty_key".to_owned(), Annotated::empty()),
                ]))),
                Annotated::empty(),
            ])),
        ]);

        assert_eq!(value_size(&array.into()), 25);
    }

    #[test]
    fn test_value_size_object() {
        let obj = Value::Object(Object::from([
            ("".to_owned(), Annotated::new(Value::Bool(false))),
            ("1".to_owned(), Annotated::new(Value::Bool(false))),
            ("ඞ".to_owned(), Annotated::empty()),
            (
                "key".to_owned(),
                Annotated::new(Value::Object(Object::from([
                    (
                        "foo".to_owned(),
                        Annotated::new(Value::Array(vec![
                            Annotated::new(Value::I64(21)),
                            Annotated::new(Value::F64(42.0)),
                        ])),
                    ),
                    ("bar".to_owned(), Annotated::empty()),
                ]))),
            ),
        ]));

        assert_eq!(value_size(&obj.into()), 31);
    }

    macro_rules! assert_calculated_size_of {
        ($expected:expr, $json:expr) => {{
            let json = $json;

            let attrs = Annotated::<Attributes>::from_json(json)
                .unwrap()
                .into_value()
                .unwrap();

            let size = attributes_size(&attrs);
            assert_eq!(size, $expected, "attrs: {json}");
        }};
    }

    #[test]
    fn test_attributes_size_full() {
        assert_calculated_size_of!(
            82,
            r#"{
            "k1": {
                "value": "string value",
                "type": "string"
            },
            "k2": {
                "value": 18446744073709551615,
                "type": "integer"
            },
            "k3": {
                "value": 42.01234567891234567899,
                "type": "double"
            },
            "k4": {
                "value": false,
                "type": "boolean"
            },
            "k5": {
                "value": {
                    "nested": {
                        "array": [1.0, 2, -12, "7 bytes", false]
                    }
                },
                "type": "not yet supported"
            }
        }"#
        );
    }
}
