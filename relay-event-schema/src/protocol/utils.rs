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
