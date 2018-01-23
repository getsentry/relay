#[macro_export]
macro_rules! impl_str_serialization {
    ($type:ty, $expectation:expr) => {
        impl ::serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::ser::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> ::serde::de::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::de::Deserializer<'de>,
            {
                struct V;

                impl<'de> ::serde::de::Visitor<'de> for V {
                    type Value = $type;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> fmt::Result {
                        formatter.write_str($expectation)
                    }

                    fn visit_str<E>(self, value: &str) -> Result<$type, E>
                    where
                        E: ::serde::de::Error,
                    {
                        value
                            .parse()
                            .map_err(|_| ::serde::de::Error::invalid_value(
                                ::serde::de::Unexpected::Str(value), &self))
                    }
                }

                deserializer.deserialize_str(V)
            }
        }
    }
}
