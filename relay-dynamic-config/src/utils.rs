use serde::{Deserialize, Serialize};

/// Normalizes the given value by deserializing it and serializing it back.
pub fn normalize_json<'de, S>(value: &'de str) -> anyhow::Result<String>
where
    S: Serialize + Deserialize<'de>,
{
    let deserialized: S = serde_json::from_str(value)?;
    let serialized = serde_json::to_value(&deserialized)?.to_string();
    Ok(serialized)
}
