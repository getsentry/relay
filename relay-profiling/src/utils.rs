use std::fmt::Display;
use std::str::FromStr;

use serde::{de, Deserialize};
use serde_json::{Map, Value};

pub fn is_zero(n: &u64) -> bool {
    *n == 0
}

pub fn deserialize_number_from_string<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    D: de::Deserializer<'de>,
    T: FromStr + Deserialize<'de>,
    <T as FromStr>::Err: Display,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrNumber<T> {
        String(String),
        Number(T),
        Bool(bool),
        Array(Vec<Value>),
        Object(Map<String, Value>),
    }

    match StringOrNumber::<T>::deserialize(deserializer)? {
        StringOrNumber::String(s) => s.parse::<T>().map_err(serde::de::Error::custom),
        StringOrNumber::Number(n) => Ok(n),
        StringOrNumber::Bool(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
        StringOrNumber::Array(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
        StringOrNumber::Object(v) => Err(serde::de::Error::custom(format!(
            "unsupported value: {:?}",
            v
        ))),
    }
}
