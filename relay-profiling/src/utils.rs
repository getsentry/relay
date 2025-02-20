use std::fmt::Display;
use std::str::FromStr;

use serde::{de, Deserialize};
use serde_json::{Map, Value};

use crate::types::ClientSdk;

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

pub fn string_is_null_or_empty(s: &Option<String>) -> bool {
    s.as_deref().is_none_or(|s| s.is_empty())
}

pub fn default_client_sdk(platform: &str) -> Option<ClientSdk> {
    let sdk_name = match platform {
        "android" => "sentry.java.android",
        "cocoa" => "sentry.cocoa",
        "csharp" => "sentry.dotnet",
        "go" => "sentry.go",
        "javascript" => "sentry.javascript",
        "node" => "sentry.javascript.node",
        "php" => "sentry.php",
        "python" => "sentry.python",
        "ruby" => "sentry.ruby",
        "rust" => "sentry.rust",
        _ => return None,
    };
    Some(ClientSdk {
        name: sdk_name.into(),
        version: "".into(),
    })
}
