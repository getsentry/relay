use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};

use crate::utils::{deserialize_number_from_string, is_zero};

#[derive(Default, Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct TransactionMetadata {
    pub id: EventId,
    pub name: String,
    pub trace_id: EventId,

    #[serde(default, deserialize_with = "deserialize_number_from_string")]
    pub active_thread_id: u64,

    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    pub relative_start_ns: u64,
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    pub relative_end_ns: u64,

    // Android might have a CPU clock for the trace
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    pub relative_cpu_start_ms: u64,
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    pub relative_cpu_end_ms: u64,
}

impl TransactionMetadata {
    pub fn valid(&self) -> bool {
        !self.id.is_nil() && !self.name.is_empty() && !self.trace_id.is_nil()
    }

    pub fn duration_ns(&self) -> u64 {
        self.relative_end_ns.saturating_sub(self.relative_start_ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transaction_metadata() {
        let metadata = TransactionMetadata {
            active_thread_id: 259,
            id: "A2669CD2-C7E0-47ED-8298-4AAF9666A6B6".parse().unwrap(),
            name: "SomeTransaction".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 133,
            relative_start_ns: 1,
            trace_id: "4705BD13-368A-499A-AA48-439DAFD9CFB0".parse().unwrap(),
        };
        assert!(metadata.valid());
    }

    #[test]
    fn test_invalid_transaction_metadata() {
        let metadata = TransactionMetadata {
            active_thread_id: 259,
            id: "A2669CD2-C7E0-47ED-8298-4AAF9666A6B6".parse().unwrap(),
            name: "".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 133,
            relative_start_ns: 1,
            trace_id: "4705BD13-368A-499A-AA48-439DAFD9CFB0".parse().unwrap(),
        };
        assert!(!metadata.valid());
    }

    #[test]
    fn test_valid_transaction_metadata_without_relative_timestamp() {
        let metadata = TransactionMetadata {
            active_thread_id: 259,
            id: "A2669CD2-C7E0-47ED-8298-4AAF9666A6B6".parse().unwrap(),
            name: "valid-transaction-name".to_string(),
            relative_cpu_end_ms: 0,
            relative_cpu_start_ms: 0,
            relative_end_ns: 0,
            relative_start_ns: 0,
            trace_id: "4705BD13-368A-499A-AA48-439DAFD9CFB0".parse().unwrap(),
        };
        assert!(metadata.valid());
    }
}
