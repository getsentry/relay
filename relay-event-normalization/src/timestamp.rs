use relay_event_schema::processor::{
    ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{Event, Span};
use relay_protocol::{Error, Meta};

/// Ensures an event's timestamps are not stale.
///
/// Stale timestamps are those that happened before January 1, 1970 UTC. The
/// processor validates the start and end timestamps of an event and returns an
/// error if any of these are stale. Additionally, spans with stale timestamps
/// are removed from the event.
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
        state: &ProcessingState,
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

        event.process_child_values(self, state)?;

        Ok(())
    }

    fn process_span(
        &mut self,
        span: &mut Span,
        meta: &mut Meta,
        _: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(start_timestamp) = span.start_timestamp.value() {
            if start_timestamp.into_inner().timestamp_millis() < 0 {
                meta.add_error(Error::invalid(format!(
                    "timestamp is too stale: {}",
                    start_timestamp
                )));
                return Err(ProcessingAction::DeleteValueHard);
            }
        }
        if let Some(end_timestamp) = span.timestamp.value() {
            if end_timestamp.into_inner().timestamp_millis() < 0 {
                meta.add_error(Error::invalid(format!(
                    "timestamp is too stale: {}",
                    end_timestamp
                )));
                return Err(ProcessingAction::DeleteValueHard);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::{process_value, ProcessingState};
    use relay_event_schema::protocol::{Event, Span};
    use relay_protocol::{assert_annotated_snapshot, Annotated};

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

    #[test]
    fn test_accept_recent_span() {
        let json = r#"{
      "span_id": "52df9022835246eeb317dbd739ccd050",
      "start_timestamp": 1,
      "timestamp": 2
    }"#;
        let mut span = Annotated::<Span>::from_json(json).unwrap();
        assert!(process_value(&mut span, &mut TimestampProcessor, ProcessingState::root()).is_ok());
    }

    #[test]
    fn test_reject_stale_span() {
        let json = r#"{
      "span_id": "52df9022835246eeb317dbd739ccd050",
      "start_timestamp": -2,
      "timestamp": -1
    }"#;
        let mut span = Annotated::<Span>::from_json(json).unwrap();
        assert!(process_value(&mut span, &mut TimestampProcessor, ProcessingState::root()).is_ok());
        assert_annotated_snapshot!(&span, @r###"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "invalid_data",
                  {
                    "reason": "timestamp is too stale: 1969-12-31 23:59:58 UTC"
                  }
                ]
              ]
            }
          }
        }
        "###);
    }

    #[test]
    fn test_reject_long_running_span() {
        let json = r#"{
      "span_id": "52df9022835246eeb317dbd739ccd050",
      "start_timestamp": -1,
      "timestamp": 1
    }"#;
        let mut span = Annotated::<Span>::from_json(json).unwrap();
        assert!(process_value(&mut span, &mut TimestampProcessor, ProcessingState::root()).is_ok());
        assert_annotated_snapshot!(&span, @r###"
        {
          "_meta": {
            "": {
              "err": [
                [
                  "invalid_data",
                  {
                    "reason": "timestamp is too stale: 1969-12-31 23:59:59 UTC"
                  }
                ]
              ]
            }
          }
        }
        "###);
    }
}
