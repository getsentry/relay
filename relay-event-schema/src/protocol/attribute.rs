use relay_protocol::{
    Annotated, Empty, ErrorKind, FromObjectRef, FromValue, IntoValue, IntoValueObject, Object,
    SkipSerialization, Value,
};
use serde::ser::SerializeMap;
use std::fmt;

#[derive(Debug, Empty, FromValue, IntoValue)]
pub struct Attribute {
    #[metastructure(flatten)]
    pub value: AttributeValue,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

#[derive(Debug)]
enum AttributeType {
    Bool,
    Int,
    Float,
    String,
    Unknown(String),
}

impl AttributeType {
    fn as_str(&self) -> &str {
        match self {
            Self::Bool => "bool",
            Self::Int => "int",
            Self::Float => "float",
            Self::String => "string",
            Self::Unknown(value) => value,
        }
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
            "bool" => Self::Bool,
            "int" => Self::Int,
            "float" => Self::Float,
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

#[derive(Debug)]
pub enum AttributeValue {
    /// A boolean value.
    Bool(bool),
    /// A signed integer value.
    I64(i64),
    /// An unsigned integer value.
    U64(u64),
    /// A floating point value.
    F64(f64),
    /// A string value.
    String(String),
    /// Any other unknown attribute value.
    ///
    /// This exists to ensure other attribute values such as array and object can be added in the future.
    Other(AttributeValueRaw),
}

#[derive(Debug, Empty, FromValue, IntoValue)]
pub struct AttributeValueRaw {
    #[metastructure(field = "type")]
    ty: Annotated<AttributeType>,
    value: Annotated<Value>,
}

impl From<AttributeValueRaw> for AttributeValue {
    fn from(value: AttributeValueRaw) -> Self {
        match (value.ty, value.value) {
            (Annotated(Some(ty), ty_meta), Annotated(Some(value), mut value_meta)) => {
                match (ty, value) {
                    (AttributeType::Bool, Value::Bool(v)) => Self::Bool(v),
                    (AttributeType::Int, Value::I64(v)) => Self::I64(v),
                    (AttributeType::Int, Value::U64(v)) => Self::U64(v),
                    (AttributeType::Float, Value::F64(v)) => Self::F64(v),
                    (AttributeType::Float, Value::I64(v)) => Self::F64(v as f64),
                    (AttributeType::Float, Value::U64(v)) => Self::F64(v as f64),
                    (AttributeType::String, Value::String(v)) => Self::String(v),
                    (ty @ AttributeType::Unknown(_), value) => Self::Other(AttributeValueRaw {
                        ty: Annotated(Some(ty), ty_meta),
                        value: Annotated(Some(value), value_meta),
                    }),
                    (ty, value) => {
                        value_meta.add_error(ErrorKind::InvalidData);
                        value_meta.set_original_value(Some(value));
                        Self::Other(AttributeValueRaw {
                            ty: Annotated(Some(ty), ty_meta),
                            value: Annotated(None, value_meta),
                        })
                    }
                }
            }
            (mut ty, mut value) => {
                if ty.is_empty() {
                    ty.meta_mut().add_error(ErrorKind::MissingAttribute);
                }
                if value.is_empty() {
                    value.meta_mut().add_error(ErrorKind::MissingAttribute);
                }
                Self::Other(AttributeValueRaw { ty, value })
            }
        }
    }
}

impl From<AttributeValue> for AttributeValueRaw {
    fn from(value: AttributeValue) -> Self {
        match value {
            AttributeValue::Bool(v) => Self {
                ty: Annotated::new(AttributeType::Bool),
                value: Annotated::new(v.into()),
            },
            AttributeValue::I64(v) => Self {
                ty: Annotated::new(AttributeType::Int),
                value: Annotated::new(v.into()),
            },
            AttributeValue::U64(v) => Self {
                ty: Annotated::new(AttributeType::Int),
                value: Annotated::new(v.into()),
            },
            AttributeValue::F64(v) => Self {
                ty: Annotated::new(AttributeType::Float),
                value: Annotated::new(v.into()),
            },
            AttributeValue::String(v) => Self {
                ty: Annotated::new(AttributeType::String),
                value: Annotated::new(v.into()),
            },
            AttributeValue::Other(v) => v,
        }
    }
}

impl Empty for AttributeValue {
    fn is_empty(&self) -> bool {
        match self {
            Self::Other(v) => v.is_empty(),
            _ => false,
        }
    }
}

impl FromObjectRef for AttributeValue {
    fn from_object_ref(value: &mut Object<Value>) -> Self
    where
        Self: Sized,
    {
        AttributeValueRaw::from_object_ref(value).into()
    }
}

impl FromValue for AttributeValue {
    fn from_value(value: Annotated<Value>) -> Annotated<Self>
    where
        Self: Sized,
    {
        AttributeValueRaw::from_value(value).map_value(Into::into)
    }
}

impl IntoValue for AttributeValue {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        AttributeValueRaw::from(self).into_value()
    }

    fn serialize_payload<S>(
        &self,
        s: S,
        behavior: relay_protocol::SkipSerialization,
    ) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        macro_rules! ser {
            ($ty:expr, $value:expr) => {{
                let mut map_ser = s.serialize_map(Some(2))?;
                map_ser.serialize_key("type")?;
                map_ser.serialize_value($ty)?;
                map_ser.serialize_key("value")?;
                map_ser.serialize_value($value)?;
                map_ser.end()
            }};
        }

        match self {
            Self::Bool(v) => ser!("bool", v),
            Self::I64(v) => ser!("int", v),
            Self::U64(v) => ser!("int", v),
            Self::F64(v) => ser!("float", v),
            Self::String(v) => ser!("string", v),
            Self::Other(v) => v.serialize_payload(s, behavior),
        }
    }
}

impl IntoValueObject for AttributeValue {
    fn into_object_fields(self) -> Object<Value> {
        AttributeValueRaw::from(self).into_object_fields()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test {
        ($name:ident, $json:literal, @$foo:literal) => {
            test!($name, $json, @$foo, $json);
        };
        ($name:ident, $json:literal, @$foo:literal, $expected:literal) => {
            #[test]
            fn $name() {
                let v = Annotated::<Attribute>::from_json($json).unwrap();
                insta::assert_debug_snapshot!(v, @$foo);

                let expected: serde_json::Value = serde_json::from_str($expected).unwrap();

                let v_json = v.to_json().unwrap();
                let v_json: serde_json::Value = serde_json::from_str(&v_json).unwrap();

                assert_eq!(v_json, expected);
            }
        }
    }

    test!(test_valid_string,
        r#"{
            "type": "string",
            "value": "My String",
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: String(
            "My String",
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###);

    test!(test_valid_i64,
        r#"{
            "type": "int",
            "value": -1,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: I64(
            -1,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###);
    test!(test_valid_u64,
        r#"{
            "type": "int",
            "value": 18446744073709551615,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: U64(
            18446744073709551615,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###);
    test!(test_valid_bool,
        r#"{
            "type": "bool",
            "value": false,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Bool(
            false,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###);
    test!(test_valid_f64,
        r#"{
            "type": "float",
            "value": 1337.0,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: F64(
            1337.0,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###);
    test!(test_valid_f64_from_i64,
        r#"{
            "type": "float",
            "value": -1,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: F64(
            -1.0,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": "float",
            "value": -1.0,
            "other": "does not exist yet"
        }"#
    );
    test!(test_valid_f64_from_u64,
        r#"{
            "type": "float",
            "value": 18446744073709551615,
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: F64(
            1.8446744073709552e19,
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": "float",
            "value": 1.8446744073709552e19,
            "other": "does not exist yet"
        }"#
    );
    test!(test_valid_unknown,
        r#"{
            "type": "unknown",
            "value": "foobar",
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Other(
            AttributeValueRaw {
                ty: Unknown(
                    "unknown",
                ),
                value: String(
                    "foobar",
                ),
            },
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###
    );

    test!(test_invalid_type_mismatch,
        r#"{
            "type": "float",
            "value": "not a float",
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Other(
            AttributeValueRaw {
                ty: Float,
                value: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: InvalidData,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: Some(
                        String(
                            "not a float",
                        ),
                    ),
                },
            },
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": "float",
            "value": null,
            "other": "does not exist yet"
        }"#
    );

    test!(test_invalid_type_missing,
        r#"{
            "value": "a string",
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Other(
            AttributeValueRaw {
                ty: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
                value: String(
                    "a string",
                ),
            },
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": null,
            "value": "a string",
            "other": "does not exist yet"
        }"#
    );
    test!(test_invalid_value_missing,
        r#"{
            "type": "float",
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Other(
            AttributeValueRaw {
                ty: Float,
                value: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
            },
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": "float",
            "value": null,
            "other": "does not exist yet"
        }"#
    );
    test!(test_invalid_type_value_missing,
        r#"{
            "other": "does not exist yet"
        }"#,
        @r###"
    Attribute {
        value: Other(
            AttributeValueRaw {
                ty: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
                value: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
            },
        ),
        other: {
            "other": String(
                "does not exist yet",
            ),
        },
    }
    "###,
        r#"{
            "type": null,
            "value": null,
            "other": "does not exist yet"
        }"#
    );
}
