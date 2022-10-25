use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Measurement {
    // nanoseconds elapsed since the start of the profile (wall clock)
    elapsed_since_start_ns: u64,
    unit: String,
    value: f64,
}
