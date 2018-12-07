macro_rules! primitive_to_value {
    ($type:ident, $meta_type:ident) => {
        impl crate::types::ToValue for $type {
            fn to_value(self) -> Value {
                Value::$meta_type(self)
            }

            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: crate::types::SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: serde::Serializer,
            {
                serde::Serialize::serialize(self, s)
            }
        }
    };
}

macro_rules! numeric_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                value.and_then(|value| {
                    let number = match value {
                        Value::U64(x) => num_traits::cast::cast(x),
                        Value::I64(x) => num_traits::cast::cast(x),
                        Value::F64(x) => num_traits::cast::cast(x),
                        _ => None,
                    };

                    match number {
                        Some(x) => Annotated::new(x),
                        None => {
                            let mut meta = crate::types::Meta::default();
                            meta.add_error(crate::types::Error::expected($expectation));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    }
                })
            }
        }

        primitive_to_value!($type, $meta_type);
    };
}

macro_rules! primitive_meta_structure_through_string {
    ($type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                        Ok(value) => Annotated(Some(value), meta),
                        Err(err) => {
                            meta.add_error(crate::types::Error::invalid(err));
                            meta.set_original_value(Some(value));
                            Annotated(None, meta)
                        }
                    },
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(crate::types::Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
        }

        impl crate::types::ToValue for $type {
            fn to_value(self) -> Value {
                Value::String(self.to_string())
            }
            fn serialize_payload<S>(
                &self,
                s: S,
                _behavior: crate::types::SkipSerialization,
            ) -> Result<S::Ok, S::Error>
            where
                Self: Sized,
                S: serde::ser::Serializer,
            {
                serde::ser::Serialize::serialize(&self.to_string(), s)
            }
        }
    };
}

macro_rules! primitive_from_value {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        impl crate::types::FromValue for $type {
            fn from_value(value: Annotated<Value>) -> Annotated<Self> {
                match value {
                    Annotated(Some(Value::$meta_type(value)), meta) => Annotated(Some(value), meta),
                    Annotated(Some(Value::Null), meta) => Annotated(None, meta),
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(crate::types::Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }
        }
    };
}

macro_rules! primitive_meta_structure {
    ($type:ident, $meta_type:ident, $expectation:expr) => {
        primitive_from_value!($type, $meta_type, $expectation);
        primitive_to_value!($type, $meta_type);
    };
}

/// Helper macro to implement string based serialization.
///
/// If a type implements `Display` then this automatically
/// implements a serializer for that type that dispatches
/// appropriately.
macro_rules! impl_str_ser {
    ($type:ty) => {
        impl ::serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::ser::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }
    };
}

/// Helper macro to implement string based deserialization.
///
/// If a type implements `FromStr` then this automatically
/// implements a deserializer for that type that dispatches
/// appropriately.
#[allow(unused_macros)]
macro_rules! impl_str_de {
    ($type:ty) => {
        impl<'de> ::serde::de::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::de::Deserializer<'de>,
            {
                <::std::borrow::Cow<str>>::deserialize(deserializer)?
                    .parse()
                    .map_err(::serde::de::Error::custom)
            }
        }
    };
}

/// Helper macro to implement string based serialization and deserialization.
///
/// If a type implements `FromStr` and `Display` then this automatically
/// implements a serializer/deserializer for that type that dispatches
/// appropriately.
#[allow(unused_macros)]
macro_rules! impl_str_serde {
    ($type:ty) => {
        impl_str_ser!($type);
        impl_str_de!($type);
    };
}
