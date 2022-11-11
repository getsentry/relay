use serde::{de, Deserialize, Serialize};

use crate::utils::deserialize_number_from_string;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Measurement {
    #[serde(deserialize_with = "deserialize_measurement_unit")]
    unit: String,
    values: Vec<MeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MeasurementValue {
    // nanoseconds elapsed since the start of the profile
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,
    value: f64,
}

fn deserialize_measurement_unit<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;

    match s {
        "nanosecond" | "ns" => Ok("nanosecond".to_string()),
        "hertz" | "hz" => Ok("hertz".to_string()),
        _ => Err(de::Error::custom(format!(
            "invalid measurement unit: {}",
            s
        ))),
    }
}
