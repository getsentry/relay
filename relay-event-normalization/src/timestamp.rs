use relay_event_schema::processor::{
    ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::Event;
use relay_protocol::Meta;

pub struct TimestampProcessor;

impl Processor for TimestampProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        _: &mut Meta,
        _: &ProcessingState,
    ) -> ProcessingResult {
        if let Some(end_timestamp) = event.timestamp.value() {
            if end_timestamp.into_inner().timestamp_millis() < 0 {
                return Err(ProcessingAction::InvalidTransaction(
                    "timestamp is too stale",
                ));
            }
        }
        if let Some(start_timestamp) = event.start_timestamp.value() {
            if start_timestamp.into_inner().timestamp_millis() < 0 {
                return Err(ProcessingAction::InvalidTransaction(
                    "timestamp is too stale",
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::Event;
    use relay_protocol::Annotated;

    use crate::{light_normalize_event, LightNormalizationConfig};

    #[test]
    fn test_accept_recent_errors() {
        let now = chrono::Utc::now().timestamp_millis();

        let json = format!(
            r#"{{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "timestamp": {}
}}"#,
            now
        );
        let mut error = Annotated::<Event>::from_json(&json).unwrap();
        assert!(light_normalize_event(&mut error, LightNormalizationConfig::default()).is_ok());
    }

    #[test]
    fn test_reject_stale_errors() {
        let stale = -chrono::Utc::now().timestamp_millis();

        let json = format!(
            r#"{{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "timestamp": {}
}}"#,
            stale
        );
        let mut error = Annotated::<Event>::from_json(&json).unwrap();
        assert_eq!(
            light_normalize_event(&mut error, LightNormalizationConfig::default())
                .unwrap_err()
                .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    #[test]
    fn test_accept_recent_transactions() {
        let now = chrono::Utc::now().timestamp_millis();

        let json = format!(
            r#"{{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": {},
  "timestamp": {}
}}"#,
            now, now
        );
        let mut error = Annotated::<Event>::from_json(&json).unwrap();
        assert!(light_normalize_event(&mut error, LightNormalizationConfig::default()).is_ok());
    }

    #[test]
    fn test_reject_stale_transactions() {
        let stale = -chrono::Utc::now().timestamp_millis();

        let json = format!(
            r#"{{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": {},
  "timestamp": {}
}}"#,
            stale, stale
        );
        let mut error = Annotated::<Event>::from_json(&json).unwrap();
        assert_eq!(
            light_normalize_event(&mut error, LightNormalizationConfig::default())
                .unwrap_err()
                .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    #[test]
    fn test_reject_long_running_transactions() {
        let now = chrono::Utc::now().timestamp_millis();
        let stale = -now;

        let json = format!(
            r#"{{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": {},
  "timestamp": {}
}}"#,
            stale, now
        );
        let mut error = Annotated::<Event>::from_json(&json).unwrap();
        assert_eq!(
            light_normalize_event(&mut error, LightNormalizationConfig::default())
                .unwrap_err()
                .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }
}
