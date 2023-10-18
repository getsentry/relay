use relay_event_schema::processor::{
    ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::Event;
use relay_protocol::Meta;

/// Ensures an event's timestamps are not stale.
///
/// Stale timestamps are those that happened before January 1, 1970 UTC. The
/// processor validates the start and end timestamps of an event and returns an
/// error if any of these are stale.
///
/// The processor checks the timestamps individually and it's not responsible
/// for decisions that relate timestamps together, including but not limited to:
/// - Ensuring the start timestamp is not later than the end timestamp.
/// - The event finished in the last X days.
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
    use relay_event_schema::processor::{process_value, ProcessingState};
    use relay_event_schema::protocol::Event;
    use relay_protocol::Annotated;

    use crate::timestamp::TimestampProcessor;

    #[test]
    fn test_accept_recent_errors() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "timestamp": 1
}"#;
        let mut error = Annotated::<Event>::from_json(json).unwrap();
        assert!(
            process_value(&mut error, &mut TimestampProcessor, ProcessingState::root()).is_ok()
        );
    }

    #[test]
    fn test_reject_stale_errors() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "timestamp": -1
}"#;
        let mut error = Annotated::<Event>::from_json(json).unwrap();
        assert_eq!(
            process_value(&mut error, &mut TimestampProcessor, ProcessingState::root())
                .unwrap_err()
                .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    #[test]
    fn test_accept_recent_transactions() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": 1,
  "timestamp": 2
}"#;
        let mut transaction = Annotated::<Event>::from_json(json).unwrap();
        assert!(process_value(
            &mut transaction,
            &mut TimestampProcessor,
            ProcessingState::root()
        )
        .is_ok());
    }

    #[test]
    fn test_reject_stale_transactions() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": -2,
  "timestamp": -1
}"#;
        let mut transaction = Annotated::<Event>::from_json(json).unwrap();
        assert_eq!(
            process_value(
                &mut transaction,
                &mut TimestampProcessor,
                ProcessingState::root()
            )
            .unwrap_err()
            .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    #[test]
    fn test_reject_long_running_transactions() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": -1,
  "timestamp": 1
}"#;
        let mut transaction = Annotated::<Event>::from_json(json).unwrap();
        assert_eq!(
            process_value(
                &mut transaction,
                &mut TimestampProcessor,
                ProcessingState::root()
            )
            .unwrap_err()
            .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }
}
