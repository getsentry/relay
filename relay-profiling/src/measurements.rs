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

    // Android 6.8.0 sends a string instead of a float64 so we need to accept both
    #[serde(deserialize_with = "deserialize_number_from_string")]
    value: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MeasurementUnit {
    #[serde(alias = "ns")]
    Nanosecond,
    #[serde(alias = "hz")]
    Hertz,
    Byte,
    Percent,
    #[serde(alias = "nj")]
    Nanojoule,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_as_float() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":1234.56789}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1234.56789);
    }

    #[test]
    fn test_value_as_string() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"1234.56789"}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1234.56789);
    }

    #[test]
    fn test_value_as_string_scientific_notation() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"1e3"}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1e3f64);
    }

    #[test]
    fn test_value_as_string_infinity() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"+Infinity"}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, f64::INFINITY);
    }

    #[test]
    fn test_value_as_float_scientific_notation() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":1e3}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1e3f64);
    }

    #[test]
    fn test_value_as_float_infinity() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":+Infinity}"#;
        let measurement = serde_json::from_str::<MeasurementValue>(measurement_json);
        assert!(measurement.is_err());
    }
}
