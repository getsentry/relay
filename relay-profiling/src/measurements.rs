use serde::{Deserialize, Serialize};

use crate::utils::deserialize_number_from_string;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LegacyMeasurement {
    unit: MeasurementUnit,
    values: Vec<LegacyMeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkMeasurement {
    unit: MeasurementUnit,
    values: Vec<ChunkMeasurementValue>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LegacyMeasurementValue {
    // nanoseconds elapsed since the start of the profile
    #[serde(deserialize_with = "deserialize_number_from_string")]
    elapsed_since_start_ns: u64,

    // Android 6.8.0 sends a string instead of a float64 so we need to accept both
    #[serde(deserialize_with = "deserialize_number_from_string")]
    value: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkMeasurementValue {
    // UNIX timestamp in seconds as a float
    timestamp: f64,

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
    use super::{ChunkMeasurementValue, LegacyMeasurementValue};

    #[test]
    fn test_roundtrip() {
        let raw_value = r#"{"elapsed_since_start_ns":1234567890,"value":1234.56789}"#;
        let parsed_value = serde_json::from_str::<LegacyMeasurementValue>(raw_value);
        assert!(parsed_value.is_ok());
        let value = parsed_value.unwrap();
        let encoded_value = serde_json::to_string(&value).unwrap();
        assert_eq!(encoded_value, raw_value);
    }

    #[test]
    fn test_value_as_float() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":1234.56789}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1234.56789);
    }

    #[test]
    fn test_value_as_string() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"1234.56789"}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1234.56789);
    }

    #[test]
    fn test_value_as_string_scientific_notation() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"1e3"}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1e3f64);
    }

    #[test]
    fn test_value_as_string_infinity() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":"+Infinity"}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, f64::INFINITY);
    }

    #[test]
    fn test_value_as_float_scientific_notation() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":1e3}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 1e3f64);
    }

    #[test]
    fn test_value_as_float_infinity() {
        let measurement_json = r#"{"elapsed_since_start_ns":1234567890,"value":+Infinity}"#;
        let measurement = serde_json::from_str::<LegacyMeasurementValue>(measurement_json);
        assert!(measurement.is_err());
    }

    #[test]
    fn test_with_timestamp_only() {
        let measurement_json = r#"{"timestamp":1717161756.408,"value":10.3}"#;
        let measurement = serde_json::from_str::<ChunkMeasurementValue>(measurement_json);
        assert!(measurement.is_ok());
        assert_eq!(measurement.unwrap().value, 10.3);
    }
}
