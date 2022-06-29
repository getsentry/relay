use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

/// A wrapper type that is able to deserialize its inner value from a nested JSON string.
///
/// For example, this struct:
///
/// ```rust
/// #[derive(Deserialize)]
/// struct TraceContext {
///     sample_rate: JsonStringifiedValue<f64>,
/// }
/// ```
///
/// ...deserializes from `{"sample_rate": "1.0"}` and `{"sample_rate": 1.0}`, and serializes to
/// `{"sample_rate": "1.0"}`
///
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct JsonStringifiedValue<T>(pub T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for JsonStringifiedValue<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Helper<'a, T> {
            Verbatim(T),
            String(&'a str),
        }

        let helper = Helper::<T>::deserialize(deserializer)?;

        match helper {
            Helper::Verbatim(value) => Ok(JsonStringifiedValue(value)),
            Helper::String(value) => {
                serde_json::from_str(value).map_err(|e| serde::de::Error::custom(e.to_string()))
            }
        }
    }
}

impl<T: Serialize> Serialize for JsonStringifiedValue<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let string =
            serde_json::to_string(&self.0).map_err(|e| serde::ser::Error::custom(e.to_string()))?;
        string.serialize(serializer)
    }
}
