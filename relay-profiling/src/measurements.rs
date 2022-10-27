use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Measurement {
    unit: String,
    values: Vec<MeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MeasurementValue {
    // nanoseconds elapsed since the start of the profile (wall clock)
    elapsed_since_start_ns: u64,
    value: f64,
}
