use std::ops::Range;

use chrono::{DateTime, Duration, Utc};
use relay_base_schema::events::EventType;
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{
    self, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{Event, Span, Timestamp, TraceContext};
use relay_protocol::{Annotated, ErrorKind, Meta};

use crate::{ClockDriftProcessor, TimestampProcessor};

/// Configuration for [`validate_transaction`].
#[derive(Debug, Default)]
pub struct TransactionValidationConfig {
    /// Timestamp range in which a transaction must end.
    ///
    /// Transactions that finish outside this range are invalid. The check is
    /// skipped if no range is provided.
    pub timestamp_range: Option<Range<UnixTimestamp>>,
}

/// Validates a transaction.
///
/// Validation consists of performing multiple checks on the payload, based on
/// the given configuration. Noop for non-transaction events.
///
/// The returned [`ProcessingResult`] indicates whether the passed transaction
/// is invalid and thus should be dropped.
///
/// Note: this function does not validate a transaction's timestamp values are
/// up-to-date, [`validate_event_timestamps`] should be used for that.
pub fn validate_transaction(
    event: &Annotated<Event>,
    config: &TransactionValidationConfig,
) -> ProcessingResult {
    let Annotated(Some(ref event), ref _meta) = event else {
        return Ok(());
    };
    if event.ty.value() != Some(&EventType::Transaction) {
        return Ok(());
    }

    validate_transaction_timestamps(event, config)?;
    validate_trace_context(event)?;
    // Transactions with spans with invalid timestamps are not expected to be
    // rejected.
    validate_spans(event, None)?;

    Ok(())
}

/// Returns whether the transacion's start and end timestamps are both set and start <= end.
fn validate_transaction_timestamps(
    transaction_event: &Event,
    config: &TransactionValidationConfig,
) -> ProcessingResult {
    match (
        transaction_event.start_timestamp.value(),
        transaction_event.timestamp.value(),
    ) {
        (Some(start), Some(end)) => {
            validate_timestamps(start, end, config.timestamp_range.as_ref())?;
            Ok(())
        }
        (_, None) => Err(ProcessingAction::InvalidTransaction(
            "timestamp hard-required for transaction events",
        )),
        // XXX: Maybe copy timestamp over?
        (None, _) => Err(ProcessingAction::InvalidTransaction(
            "start_timestamp hard-required for transaction events",
        )),
    }
}

/// Validates that start <= end timestamps and that the end timestamp is inside the valid range.
fn validate_timestamps(
    start: &Timestamp,
    end: &Timestamp,
    valid_range: Option<&Range<UnixTimestamp>>,
) -> ProcessingResult {
    if end < start {
        return Err(ProcessingAction::InvalidTransaction(
            "end timestamp is smaller than start timestamp",
        ));
    }

    let Some(range) = valid_range else {
        return Ok(());
    };

    let Some(timestamp) = UnixTimestamp::from_datetime(end.into_inner()) else {
        return Err(ProcessingAction::InvalidTransaction(
            "invalid unix timestamp",
        ));
    };
    if !range.contains(&timestamp) {
        return Err(ProcessingAction::InvalidTransaction(
            "timestamp is out of the valid range for metrics",
        ));
    }

    Ok(())
}

/// Validates the trace context in a transaction.
///
/// A [`ProcessingResult`] error is returned if the context is not valid. The
/// context is valid if the trace context meets the following conditions:
/// - It exists.
/// - It has a trace id.
/// - It has a span id.
fn validate_trace_context(transaction: &Event) -> ProcessingResult {
    let Some(trace_context) = transaction.context::<TraceContext>() else {
        return Err(ProcessingAction::InvalidTransaction(
            "missing valid trace context",
        ));
    };

    if trace_context.trace_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "trace context is missing trace_id",
        ));
    }

    if trace_context.span_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "trace context is missing span_id",
        ));
    }

    Ok(())
}

/// Validates the spans in the transaction.
///
/// A [`ProcessingResult`] error is returned if there's an invalid span. For
/// span validity, see [`validate_span`].
fn validate_spans(
    transaction: &Event,
    timestamp_range: Option<&Range<UnixTimestamp>>,
) -> ProcessingResult {
    let Some(spans) = transaction.spans.value() else {
        return Ok(());
    };

    for span in spans {
        if let Some(span) = span.value() {
            validate_span(span, timestamp_range)?;
        } else {
            return Err(ProcessingAction::InvalidTransaction(
                "spans must be valid in transaction event",
            ));
        }
    }

    Ok(())
}

/// Validates a span.
///
/// A [`ProcessingResult`] error is returned when the span is invalid. A span is
/// valid if all the following conditions are met:
/// - A start timestamp exists.
/// - An end timestamp exists.
/// - Start timestamp must be no later than end timestamp.
/// - A trace id exists.
/// - A span id exists.
pub fn validate_span(
    span: &Span,
    timestamp_range: Option<&Range<UnixTimestamp>>,
) -> ProcessingResult {
    match (span.start_timestamp.value(), span.timestamp.value()) {
        (Some(start), Some(end)) => {
            validate_timestamps(start, end, timestamp_range)?;
        }
        (_, None) => {
            // XXX: Maybe do the same as event.timestamp?
            return Err(ProcessingAction::InvalidTransaction(
                "span is missing timestamp",
            ));
        }
        (None, _) => {
            // XXX: Maybe copy timestamp over?
            return Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp",
            ));
        }
    }

    if span.trace_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "span is missing trace_id",
        ));
    }

    if span.span_id.value().is_none() {
        return Err(ProcessingAction::InvalidTransaction(
            "span is missing span_id",
        ));
    }

    Ok(())
}

/// Configuration for [`validate_event_timestamps`].
#[derive(Debug, Default)]
pub struct EventValidationConfig {
    /// The time at which the event was received in this Relay.
    ///
    /// This timestamp is persisted into the event payload.
    pub received_at: Option<DateTime<Utc>>,

    /// The maximum amount of seconds an event can be dated in the past.
    ///
    /// If the event's timestamp is older, the received timestamp is assumed.
    pub max_secs_in_past: Option<i64>,

    /// The maximum amount of seconds an event can be predated into the future.
    ///
    /// If the event's timestamp lies further into the future, the received timestamp is assumed.
    pub max_secs_in_future: Option<i64>,
}

/// Validates the timestamp values of an event, after performing minimal timestamp normalization.
///
/// A minimal normalization is performed on an event's timestamps before
/// checking for validity, like clock drift correction. This normalization
/// depends on the given configuration.
///
/// Validation is checked individually on timestamps. Use
/// [`validate_transaction`] to perform additional transaction-specific checks.
///
/// The returned [`ProcessingResult`] indicates whether the event is invalid and
/// thus should be dropped.
///
/// Normalization changes should not be performed during validation, unless
/// strictly required. Consider adding placing the normalization in
/// [`crate::event::normalize_event`].
pub fn validate_event_timestamps(
    event: &mut Annotated<Event>,
    config: &EventValidationConfig,
) -> ProcessingResult {
    let Annotated(Some(ref mut event), ref mut meta) = event else {
        return Ok(());
    };

    //  timestamp processor is required in validation, and requires the clockdrift changes
    normalize_timestamps(
        event,
        meta,
        config.received_at,
        config.max_secs_in_past,
        config.max_secs_in_future,
    ); // Timestamps are core in the metrics extraction
    TimestampProcessor.process_event(event, meta, ProcessingState::root())?;

    Ok(())
}

/// Validates the timestamp range and sets a default value.
fn normalize_timestamps(
    event: &mut Event,
    meta: &mut Meta,
    received_at: Option<DateTime<Utc>>,
    max_secs_in_past: Option<i64>,
    max_secs_in_future: Option<i64>,
) {
    let received_at = received_at.unwrap_or_else(Utc::now);

    let mut sent_at = None;
    let mut error_kind = ErrorKind::ClockDrift;

    let _ = processor::apply(&mut event.timestamp, |timestamp, _meta| {
        if let Some(secs) = max_secs_in_future {
            if *timestamp > received_at + Duration::seconds(secs) {
                error_kind = ErrorKind::FutureTimestamp;
                sent_at = Some(*timestamp);
                return Ok(());
            }
        }

        if let Some(secs) = max_secs_in_past {
            if *timestamp < received_at - Duration::seconds(secs) {
                error_kind = ErrorKind::PastTimestamp;
                sent_at = Some(*timestamp);
                return Ok(());
            }
        }

        Ok(())
    });

    let _ = ClockDriftProcessor::new(sent_at.map(|ts| ts.into_inner()), received_at)
        .error_kind(error_kind)
        .process_event(event, meta, ProcessingState::root());

    // Apply this after clock drift correction, otherwise we will malform it.
    event.received = Annotated::new(received_at.into());

    if event.timestamp.value().is_none() {
        event.timestamp.set_value(Some(received_at.into()));
    }

    let _ = processor::apply(&mut event.time_spent, |time_spent, _| {
        validate_bounded_integer_field(*time_spent)
    });
}

/// Validate fields that go into a `sentry.models.BoundedIntegerField`.
fn validate_bounded_integer_field(value: u64) -> ProcessingResult {
    if value < 2_147_483_647 {
        Ok(())
    } else {
        Err(ProcessingAction::DeleteValueHard)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use relay_event_schema::protocol::{Contexts, SpanId, TraceId};
    use relay_protocol::Object;

    use super::*;

    fn new_test_event() -> Annotated<Event> {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            start_timestamp: Annotated::new(start.into()),
            timestamp: Annotated::new(end.into()),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                start_timestamp: Annotated::new(start.into()),
                timestamp: Annotated::new(end.into()),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                op: Annotated::new("db.statement".to_owned()),
                ..Default::default()
            })]),
            ..Default::default()
        })
    }

    #[test]
    fn test_discards_when_missing_timestamp() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_when_timestamp_out_of_range() {
        let event = new_test_event();

        assert!(matches!(
            validate_transaction(
                &event,
                &TransactionValidationConfig {
                    timestamp_range: Some(UnixTimestamp::now()..UnixTimestamp::now()),
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp is out of the valid range for metrics"
            ))
        ));
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "start_timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_contexts_map() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(
                &event,
                &TransactionValidationConfig {
                    timestamp_range: None
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_context() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: Annotated::new(Contexts::new()),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(
                &event,
                &TransactionValidationConfig {
                    timestamp_range: None
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_null_context() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert("trace".to_owned(), Annotated::empty());
                contexts
            })),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_trace_id_in_context() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext::default());
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_span_id_in_context() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing span_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::empty()]),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "spans must be valid in transaction event"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            validate_transaction(&event, &TransactionValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing span_id"
            ))
        );
    }

    #[test]
    fn test_reject_stale_transaction() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "start_timestamp": -2,
  "timestamp": -1
}"#;
        let mut transaction = Annotated::<Event>::from_json(json).unwrap();
        let res = validate_event_timestamps(&mut transaction, &EventValidationConfig::default());
        assert_eq!(
            res.unwrap_err().to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    /// Test that timestamp normalization updates a transaction's timestamps to
    /// be acceptable, when both timestamps are similarly stale.
    #[test]
    fn test_accept_recent_transactions_with_stale_timestamps() {
        let config = EventValidationConfig {
            received_at: Some(Utc::now()),
            max_secs_in_past: Some(2),
            max_secs_in_future: Some(1),
        };

        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "transaction": "I have a stale timestamp, but I'm recent!",
  "start_timestamp": -2,
  "timestamp": -1
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        assert!(validate_event_timestamps(&mut event, &config).is_ok());
    }

    /// Test that transactions are rejected as invalid when timestamp normalization isn't enough.
    ///
    /// When the end timestamp is recent but the start timestamp is stale, timestamp normalization
    /// will fix the timestamps based on the end timestamp. The start timestamp will be more recent,
    /// but not recent enough for the transaction to be accepted. The transaction will be rejected.
    #[test]
    fn test_reject_stale_transactions_after_timestamp_normalization() {
        let now = Utc::now();
        let config = EventValidationConfig {
            received_at: Some(now),
            max_secs_in_past: Some(2),
            max_secs_in_future: Some(1),
        };

        let json = format!(
            r#"{{
          "event_id": "52df9022835246eeb317dbd739ccd059",
          "transaction": "clockdrift is not enough to accept me :(",
          "start_timestamp": -62135811111,
          "timestamp": {}
        }}"#,
            now.timestamp()
        );
        let mut event = Annotated::<Event>::from_json(json.as_str()).unwrap();

        assert_eq!(
            validate_event_timestamps(&mut event, &config)
                .unwrap_err()
                .to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }
}
