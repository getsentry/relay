use serde::{Deserialize, Serialize};

use crate::utils::deserialize_number_from_string;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Measurement {
    unit: String,
    values: Vec<MeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MeasurementValue {
    // nanoseconds elapsed since the start of the profile (wall clock)
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,
    value: f64,
}
