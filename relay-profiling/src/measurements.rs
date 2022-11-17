use serde::{Deserialize, Serialize};

use crate::utils::deserialize_number_from_string;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Measurement {
    unit: MeasurementUnit,
    values: Vec<MeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MeasurementValue {
    // nanoseconds elapsed since the start of the profile
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,
    value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MeasurementUnit {
    #[serde(alias = "ns")]
    Nanosecond,
    #[serde(alias = "hz")]
    Hertz,
}
