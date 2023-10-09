/// Helper macro to implement string based serialization.
///
/// If a type implements `Display` then this automatically
/// implements a serializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_ser {
    ($type:ty) => {
        impl ::serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::ser::Serializer,
            {
                serializer.collect_str(self)
            }
        }
    };
}

/// Helper macro to implement string based deserialization.
///
/// If a type implements `FromStr` then this automatically
/// implements a deserializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_de {
    ($type:ty, $expectation:expr) => {
        impl<'de> ::serde::de::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::de::Deserializer<'de>,
            {
                struct V;

                impl<'de> ::serde::de::Visitor<'de> for V {
                    type Value = $type;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str($expectation)
                    }

                    fn visit_str<E>(self, value: &str) -> Result<$type, E>
                    where
                        E: ::serde::de::Error,
                    {
                        value.parse().map_err(|_| {
                            ::serde::de::Error::invalid_value(
                                ::serde::de::Unexpected::Str(value),
                                &self,
                            )
                        })
                    }
                }

                deserializer.deserialize_str(V)
            }
        }
    };
}

/// Helper macro to implement string based serialization and deserialization.
///
/// If a type implements `FromStr` and `Display` then this automatically
/// implements a serializer/deserializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_serde {
    ($type:ty, $expectation:expr) => {
        $crate::impl_str_ser!($type);
        $crate::impl_str_de!($type, $expectation);
    };
}

/// Implements FromStr and Display on a flat/C-like enum such that strings roundtrip correctly and
/// all variants can be FromStr'd.
///
///
/// Usage:
///
/// ```
/// use relay_common::derive_fromstr_and_display;
///
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
#[macro_export]
macro_rules! derive_fromstr_and_display {
    ($type:ty, $error_type:tt, { $($variant:path => $($name:literal)|*),+ $(,)? }) => {
        impl $type {
            /// Returns the string representation of this enum variant.
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
