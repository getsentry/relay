macro_rules! derive_string_meta_structure {
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
                    Annotated(None, meta) => Annotated(None, meta),
                    Annotated(Some(value), mut meta) => {
                        meta.add_error(crate::types::Error::expected($expectation));
                        meta.set_original_value(Some(value));
                        Annotated(None, meta)
                    }
                }
            }

            fn attach_meta_map(&mut self, _meta_map: crate::types::MetaMap) {}
        }

        impl crate::types::IntoValue for $type {
            fn into_value(self) -> Value {
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

        impl crate::types::Empty for $type {
            fn is_empty(&self) -> bool {
                false
            }
        }
    };
}

/// Implements FromStr and Display on a flat/C-like enum such that strings roundtrip correctly and
/// all variants can be FromStr'd.
///
///
/// Usage:
///
/// ```rust
/// // derive fail for this or whatever you need. The type must be ZST though.
/// struct ValueTypeError;
///
/// enum ValueType {
///     Foo,
///     Bar,
/// }
///
/// derive_fromstr_and_display!(ValueType, ValueTypeError, {
///     ValueType::Foo => "foo" | "foo2",  // fromstr will recognize foo/foo2, display will use foo.
///     ValueType::Bar => "bar",
/// });
/// ```
macro_rules! derive_fromstr_and_display {
    ($type:ty, $error_type:tt, { $($variant:path => $($name:literal)|*),+ $(,)? }) => {
        impl $type {
            pub fn as_str(&self) -> &'static str {
                match *self {
                    $(
                        $variant => ($($name, )*).0
                    ),*
                }
            }
        }

        impl ::std::fmt::Display for $type {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                write!(f, "{}", self.as_str())
            }
        }

        impl ::std::str::FromStr for $type {
            type Err = $error_type;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(match s {
                    $(
                        $($name)|* => $variant,
                    )*
                    _ => return Err($error_type)
                })
            }
        }
    }
}
