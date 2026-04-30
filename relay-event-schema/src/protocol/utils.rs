use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer};

/// Returns the default value for a type if the provided value is `null`.
pub fn null_to_default<'de, D, V>(deserializer: D) -> Result<V, D::Error>
where
    D: Deserializer<'de>,
    V: Default + DeserializeOwned,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

/// Returns `None` if de-serialization of `T` fails.
pub fn none_on_error<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: serde::de::DeserializeOwned + std::fmt::Debug,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Try<T> {
        T(T),
        Err(serde::de::IgnoredAny),
    }

    Ok(match Try::<T>::deserialize(deserializer)? {
        Try::T(t) => Some(t),
        Try::Err(_) => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_on_error() {
        #[derive(Deserialize)]
        struct Test {
            #[serde(deserialize_with = "none_on_error")]
            a: Option<String>,
        }

        macro_rules! de {
            ($value:literal) => {
                serde_json::from_str::<Test>(concat!("{\"a\":", $value, "}"))
                    .unwrap()
                    .a
            };
        }

        assert_eq!(de!("\"foo\""), Some("foo".to_owned()));
        assert_eq!(de!("{}"), None);
        assert_eq!(de!("[]"), None);
        assert_eq!(de!(1), None);
        assert_eq!(de!(1.23), None);
    }
}
