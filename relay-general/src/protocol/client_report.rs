use relay_common::{DataCategory, UnixTimestamp};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DiscardedEvent {
    pub reason: String,
    pub category: DataCategory,
    pub quantity: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClientReport {
    /// The timestamp of when the report was created.
    pub timestamp: Option<UnixTimestamp>,
    /// Discard reason counters.
    pub discarded_events: Vec<DiscardedEvent>,
}

impl ClientReport {
    /// Parses a client report update from JSON.
    pub fn parse(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }

    /// Serializes a client report update back into JSON.
    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_report_roundtrip() {
        let json = r#"{
  "timestamp": "2020-02-07T15:17:00Z",
  "discarded_events": [
    {"reason": "foo_reason", "category": "error", "quantity": 42},
    {"reason": "foo_reason", "category": "transaction", "quantity": 23}
  ]
}"#;

        let output = r#"{
  "timestamp": 1581088620,
  "discarded_events": [
    {
      "reason": "foo_reason",
      "category": "error",
      "quantity": 42
    },
    {
      "reason": "foo_reason",
      "category": "transaction",
      "quantity": 23
    }
  ]
}"#;

        let update = ClientReport {
            timestamp: Some("2020-02-07T15:17:00Z".parse().unwrap()),
            discarded_events: vec![
                DiscardedEvent {
                    reason: "foo_reason".into(),
                    category: DataCategory::Error,
                    quantity: 42,
                },
                DiscardedEvent {
                    reason: "foo_reason".into(),
                    category: DataCategory::Transaction,
                    quantity: 23,
                },
            ],
        };

        let parsed = ClientReport::parse(json.as_bytes()).unwrap();
        assert_eq_dbg!(update, parsed);
        assert_eq_str!(output, serde_json::to_string_pretty(&update).unwrap());
    }
}
