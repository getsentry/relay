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

/// Configuration for [`validate_event`].
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

    /// Timestamp range in which a transaction must end.
    ///
    /// Transactions that finish outside this range are invalid. The check is
    /// skipped if no range is provided.
    pub transaction_timestamp_range: Option<Range<UnixTimestamp>>,

    /// Controls whether the event has been validated before, in which case disables validation.
    ///
    /// By default, `is_validated` is disabled and event validation is run.
    ///
    /// Similar to `is_renormalize` for normalization, `sentry_relay` may configure this value.
    pub is_validated: bool,
}

/// Validates an event.
///
/// Validation consists of performing multiple checks on the payload, based on
/// the given configuration.
///
/// The returned [`ProcessingResult`] indicates whether the passed event is
/// invalid and thus should be dropped.
pub fn validate_event(
    event: &mut Annotated<Event>,
    config: &EventValidationConfig,
) -> ProcessingResult {
    if config.is_validated {
        return Ok(());
    }

    let Annotated(Some(event), meta) = event else {
        return Ok(());
    };

    // TimestampProcessor is required in validation, and requires normalizing
    // timestamps before (especially clock drift changes).
    normalize_timestamps(
        event,
        meta,
        config.received_at,
        config.max_secs_in_past,
        config.max_secs_in_future,
    );
    TimestampProcessor.process_event(event, meta, ProcessingState::root())?;

    if event.ty.value() == Some(&EventType::Transaction) {
        validate_transaction_timestamps(event, config)?;
        validate_trace_context(event)?;
        // There are no timestamp range requirements for span timestamps.
        // Transaction will be rejected only if either end or start timestamp is missing.
        end_all_spans(event);
        validate_spans(event, None)?;
    }

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
        if let Some(secs) = max_secs_in_future
            && *timestamp > received_at + Duration::seconds(secs)
        {
            error_kind = ErrorKind::FutureTimestamp;
            sent_at = Some(*timestamp);
            return Ok(());
        }

        if let Some(secs) = max_secs_in_past
            && *timestamp < received_at - Duration::seconds(secs)
        {
            error_kind = ErrorKind::PastTimestamp;
            sent_at = Some(*timestamp);
            return Ok(());
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

/// Returns whether the transacion's start and end timestamps are both set and start <= end.
fn validate_transaction_timestamps(
    transaction_event: &Event,
    config: &EventValidationConfig,
) -> ProcessingResult {
    match (
        transaction_event.start_timestamp.value(),
        transaction_event.timestamp.value(),
    ) {
        (Some(start), Some(end)) => {
            validate_timestamps(start, end, config.transaction_timestamp_range.as_ref())?;
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

/// Copies the event's end timestamp into the spans that don't have one.
fn end_all_spans(event: &mut Event) {
    let spans = event.spans.value_mut().get_or_insert_with(Vec::new);
    for span in spans {
        if let Some(span) = span.value_mut()
            && span.timestamp.value().is_none()
        {
            // event timestamp guaranteed to be `Some` due to validate_transaction call
            span.timestamp.set_value(event.timestamp.value().cloned());
            span.status = Annotated::new(relay_base_schema::spans::SpanStatus::DeadlineExceeded);
        }
    }
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

/// Validate fields that go into a `sentry.models.BoundedIntegerField`.
fn validate_bounded_integer_field(value: u64) -> ProcessingResult {
    if value < 2_147_483_647 {
        Ok(())
    } else {
        Err(ProcessingAction::DeleteValueHard)
    }
}

/// The maximum value for PostgreSQL integer fields (2^31 - 1).
const MAX_POSTGRES_INTEGER: i64 = 2_147_483_647;

/// Validates sample rates to prevent integer overflow in times_seen calculations.
///
/// When client-side sampling is used, the backend calculates `times_seen` as `1 / sample_rate`.
/// If the sample rate is extremely small, this calculation can overflow the PostgreSQL integer
/// field limit of 2,147,483,647, causing database errors.
///
/// This function validates that `1 / sample_rate` would not exceed the maximum integer value.
/// If the sample rate would cause overflow, it returns a capped sample rate that prevents overflow.
pub fn validate_sample_rate_for_times_seen(sample_rate: f64) -> Option<f64> {
    if sample_rate <= 0.0 {
        return None; // Invalid sample rate
    }

    // Calculate what the times_seen value would be
    let times_seen = 1.0 / sample_rate;

    // If the calculated times_seen would exceed the PostgreSQL integer limit,
    // cap the sample rate to prevent overflow
    if times_seen > MAX_POSTGRES_INTEGER as f64 {
        let capped_sample_rate = 1.0 / MAX_POSTGRES_INTEGER as f64;
        relay_log::warn!(
            "Sample rate {} would cause times_seen overflow ({}), capping to {}",
            sample_rate,
            times_seen as i64,
            capped_sample_rate
        );
        Some(capped_sample_rate)
    } else {
        Some(sample_rate)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use relay_base_schema::spans::SpanStatus;

    use super::*;
    use relay_event_schema::protocol::Contexts;
    use relay_protocol::{Object, get_value};

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
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                start_timestamp: Annotated::new(start.into()),
                timestamp: Annotated::new(end.into()),
                trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                op: Annotated::new("db.statement".to_owned()),
                ..Default::default()
            })]),
            ..Default::default()
        })
    }

    #[test]
    fn test_timestamp_added_if_missing() {
        let mut event = Annotated::new(Event::default());
        assert!(get_value!(event.timestamp).is_none());
        assert!(validate_event(&mut event, &EventValidationConfig::default()).is_ok());
        assert!(get_value!(event.timestamp).is_some());
    }

    #[test]
    fn test_discards_when_timestamp_out_of_range() {
        let mut event = new_test_event();

        assert!(matches!(
            validate_event(
                &mut event,
                &EventValidationConfig {
                    transaction_timestamp_range: Some(UnixTimestamp::now()..UnixTimestamp::now()),
                    is_validated: false,
                    ..Default::default()
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp is out of the valid range for metrics"
            ))
        ));
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            ..Default::default()
        });

        assert_eq!(
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "start_timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_contexts_map() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            ..Default::default()
        });

        assert_eq!(
            validate_event(
                &mut event,
                &EventValidationConfig {
                    transaction_timestamp_range: None,
                    is_validated: false,
                    ..Default::default()
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: Annotated::new(Contexts::new()),
            ..Default::default()
        });

        assert_eq!(
            validate_event(
                &mut event,
                &EventValidationConfig {
                    transaction_timestamp_range: None,
                    is_validated: false,
                    ..Default::default()
                }
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_null_context() {
        let mut event = Annotated::new(Event {
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
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_trace_id_in_context() {
        let mut event = Annotated::new(Event {
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
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_span_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        assert_eq!(
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing span_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::empty()]),
            ..Default::default()
        });

        assert_eq!(
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "spans must be valid in transaction event"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
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
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
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
            validate_event(&mut event, &EventValidationConfig::default()),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new("fa90fdead5f74053".parse().unwrap()),
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
                trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            validate_event(&mut event, &EventValidationConfig::default()),
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
        let res = validate_event(&mut transaction, &EventValidationConfig::default());
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
            is_validated: false,
            ..Default::default()
        };

        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "transaction": "I have a stale timestamp, but I'm recent!",
  "start_timestamp": -2,
  "timestamp": -1
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        assert!(validate_event(&mut event, &config).is_ok());
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
            is_validated: false,
            ..Default::default()
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
            validate_event(&mut event, &config).unwrap_err().to_string(),
            "invalid transaction event: timestamp is too stale"
        );
    }

    /// Validates an unfinished span in a transaction, and the transaction is accepted.
    #[test]
    fn test_accept_transactions_with_unfinished_spans() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "type": "transaction",
  "transaction": "I have a stale timestamp, but I'm recent!",
  "start_timestamp": 1,
  "timestamp": 2,
  "contexts": {
    "trace": {
      "trace_id": "ff62a8b040f340bda5d830223def1d81",
      "span_id": "bd429c44b67a3eb4"
    }
  },
  "spans": [
    {
      "span_id": "bd429c44b67a3eb4",
      "start_timestamp": 1,
      "timestamp": null,
      "trace_id": "ff62a8b040f340bda5d830223def1d81"
    }
  ]
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        assert!(validate_event(&mut event, &EventValidationConfig::default()).is_ok());

        let event = get_value!(event!);
        let spans = &event.spans;
        let span = get_value!(spans[0]!);

        assert_eq!(span.timestamp, event.timestamp);
        assert_eq!(span.status.value().unwrap(), &SpanStatus::DeadlineExceeded);
    }

    /// Validates an unfinished span is finished with the normalized transaction's timestamp.
    #[test]
    fn test_finish_spans_with_normalized_transaction_end_timestamp() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "type": "transaction",
  "transaction": "I have a stale timestamp, but I'm recent!",
  "start_timestamp": 946731000,
  "timestamp": 946731555,
  "contexts": {
    "trace": {
      "trace_id": "ff62a8b040f340bda5d830223def1d81",
      "span_id": "bd429c44b67a3eb4"
    }
  },
  "spans": [
    {
      "span_id": "bd429c44b67a3eb4",
      "start_timestamp": 946731000,
      "timestamp": null,
      "trace_id": "ff62a8b040f340bda5d830223def1d81"
    }
  ]
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        validate_event(
            &mut event,
            &EventValidationConfig {
                received_at: Some(Utc::now()),
                max_secs_in_past: Some(2),
                max_secs_in_future: Some(1),
                is_validated: false,
                ..Default::default()
            },
        )
        .unwrap();
        validate_event(&mut event, &EventValidationConfig::default()).unwrap();

        let event = get_value!(event!);
        let spans = &event.spans;
        let span = get_value!(spans[0]!);

        assert_eq!(span.timestamp.value(), event.timestamp.value());
    }

    #[test]
    fn test_skip_transaction_validation_on_renormalization() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "type": "transaction",
  "transaction": "I'm invalid because I don't have any timestamps!"
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        assert!(validate_event(&mut event, &EventValidationConfig::default()).is_err());
        assert!(
            validate_event(
                &mut event,
                &EventValidationConfig {
                    is_validated: true,
                    ..Default::default()
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn test_skip_event_timestamp_validation_on_renormalization() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "transaction": "completely outdated transaction",
  "start_timestamp": -2,
  "timestamp": -1
}"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        assert!(validate_event(&mut event, &EventValidationConfig::default()).is_err());
        assert!(
            validate_event(
                &mut event,
                &EventValidationConfig {
                    is_validated: true,
                    ..Default::default()
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn test_validate_sample_rate_for_times_seen() {
        // Test normal sample rates that should pass through unchanged
        assert_eq!(validate_sample_rate_for_times_seen(0.1), Some(0.1));
        assert_eq!(validate_sample_rate_for_times_seen(0.5), Some(0.5));
        assert_eq!(validate_sample_rate_for_times_seen(1.0), Some(1.0));

        // Test invalid sample rates
        assert_eq!(validate_sample_rate_for_times_seen(0.0), None);
        assert_eq!(validate_sample_rate_for_times_seen(-0.1), None);

        // Test sample rates that would cause overflow
        // If sample_rate = 0.00000045, then 1/sample_rate = 2,222,222 (safe)
        let safe_rate = 0.00000045;
        assert_eq!(validate_sample_rate_for_times_seen(safe_rate), Some(safe_rate));

        // If sample_rate = 0.0000000001, then 1/sample_rate = 10,000,000,000 (overflow)
        let overflow_rate = 0.0000000001;
        let result = validate_sample_rate_for_times_seen(overflow_rate);
        assert!(result.is_some());
        assert!(result.unwrap() > overflow_rate); // Should be capped to a larger value

        // The capped rate should produce exactly MAX_POSTGRES_INTEGER when inverted
        let capped_rate = result.unwrap();
        let calculated_times_seen = (1.0 / capped_rate) as i64;
        assert_eq!(calculated_times_seen, MAX_POSTGRES_INTEGER);

        // Test the exact edge case from the issue
        // sample_rate = 0.00000145 -> 1/sample_rate = 689,655 (safe)
        let edge_case_rate = 0.00000145;
        assert_eq!(validate_sample_rate_for_times_seen(edge_case_rate), Some(edge_case_rate));

        // Test a rate that would produce exactly the max value
        let max_safe_rate = 1.0 / MAX_POSTGRES_INTEGER as f64;
        let result = validate_sample_rate_for_times_seen(max_safe_rate);
        assert!(result.is_some());
        let times_seen = (1.0 / result.unwrap()) as i64;
        assert!(times_seen <= MAX_POSTGRES_INTEGER);
    }

    #[test]
    fn test_sample_rate_edge_cases() {
        // Test very small rates that would cause massive overflow
        let tiny_rate = f64::MIN_POSITIVE;
        let result = validate_sample_rate_for_times_seen(tiny_rate);
        assert!(result.is_some());
        let capped_rate = result.unwrap();
        assert!(capped_rate > tiny_rate);

        // Verify the capped rate doesn't cause overflow
        let times_seen = (1.0 / capped_rate) as i64;
        assert!(times_seen <= MAX_POSTGRES_INTEGER);
    }
}
