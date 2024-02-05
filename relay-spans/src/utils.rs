use std::fmt::Display;
use std::str::FromStr;

use serde::{de, Deserialize};
use serde_json::{Map, Value};

// TODO-neel-protobuf do this shit idk how
#[allow(dead_code)]
pub fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    D: de::Deserializer<'de>,
    T: FromStr + Deserialize<'de>,
    <T as FromStr>::Err: Display,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AnyType<T> {
        Array(Vec<Value>),
        Bool(bool),
        Null,
        Number(T),
        Object(Map<String, Value>),
        String(String),
    }

    match AnyType::<T>::deserialize(deserializer)? {
        AnyType::String(s) => s.parse::<T>().map_err(serde::de::Error::custom),
        AnyType::Number(n) => Ok(n),
        AnyType::Bool(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
        AnyType::Array(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
        AnyType::Object(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
        AnyType::Null => Err(serde::de::Error::custom("unsupported null value")),
    }
}
