use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// A wrapper type that is able to deserialize its inner value from a nested JSON string.
///
/// For example, this struct:
///
/// ```rust
/// use relay_common::as_string;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct TraceContext {
///     #[serde(with = "as_string")]
///     sample_rate: JsonStringifiedValue<f64>,
/// }
/// ```
///
/// ...deserializes from `{"sample_rate": "1.0"}` and `{"sample_rate": 1.0}`, and serializes to
/// `{"sample_rate": "1.0"}`
pub mod as_string {
    use super::*;

    /// Deserialize a value from a nested json string
    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where
        for<'de2> T: Deserialize<'de2>,
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper<T> {
            Verbatim(T),
            // can't change this to borrowed string, otherwise deserialization within an
            // ErrorBoundary silently fails for some reason
            String(String),
        }

        let helper = Helper::<T>::deserialize(deserializer)?;

        match helper {
            Helper::Verbatim(value) => Ok(value),
            Helper::String(value) => {
                serde_json::from_str(&value).map_err(|e| serde::de::Error::custom(e.to_string()))
            }
        }
    }

    /// Serialize a value as JSON string
    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        let string =
            serde_json::to_string(value).map_err(|e| serde::ser::Error::custom(e.to_string()))?;
        string.serialize(serializer)
    }
}

#[test]
fn test_basic() {
    #[derive(Deserialize)]
    struct JsonStringifiedValue(#[serde(with = "as_string")] f32);

    let value = serde_json::from_str::<JsonStringifiedValue>("42.0").unwrap();
    assert_eq!(value.0, 42.0);
    let value = serde_json::from_str::<JsonStringifiedValue>("\"42.0\"").unwrap();
    assert_eq!(value.0, 42.0);
}
