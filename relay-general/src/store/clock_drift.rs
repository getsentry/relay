use std::fmt;

use chrono::{DateTime, Duration as SignedDuration, Utc};

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Event, SessionUpdate};
use crate::types::{Error, ErrorKind, Meta, ProcessingResult, Timestamp};

/// The minimum clock drift for correction to apply.
const MINIMUM_CLOCK_DRIFT_SECS: i64 = 55 * 60;

/// A signed correction that contains the sender's timestamp as well as the drift to the receiver.
#[derive(Clone, Copy, Debug)]
struct ClockCorrection {
    sent_at: DateTime<Utc>,
    drift: SignedDuration,
}

impl ClockCorrection {
    fn new(sent_at: DateTime<Utc>, received_at: DateTime<Utc>) -> Self {
        let drift = received_at - sent_at;
        Self { sent_at, drift }
    }

    fn at_least(self, lower_bound: SignedDuration) -> Option<Self> {
        if self.drift.num_seconds().abs() >= lower_bound.num_seconds().abs() {
            Some(self)
        } else {
            None
        }
    }
}

/// Prints a duration with minimum precision.
///
/// Uses days if the duration is at least 1 day, otherwise falls back to hours and then seconds.
/// Also supports negative durations.
#[derive(Clone, Copy, Debug)]
struct HumanDuration(SignedDuration);

impl fmt::Display for HumanDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let days = self.0.num_days();
        if days.abs() == 1 {
            write!(f, "{} day", days)
        } else if days != 0 {
            write!(f, "{} days", days)
        } else if self.0.num_hours() != 0 {
            write!(f, "{}h", self.0.num_hours())
        } else {
            write!(f, "{}s", self.0.num_seconds())
        }
    }
}

/// Corrects clock drift based on the sender's and receivers timestamps.
///
/// Clock drift correction applies to all timestamps in the event protocol. This includes especially
/// the event's timestamp, breadcrumbs and spans.
///
/// There is a minimum clock drift of _55 minutes_ to compensate for network latency and small clock
/// drift on the sender's machine, but allow to detect timezone differences. For differences lower
/// than that, no correction is performed.
///
/// Clock drift is corrected in both ways:
///
/// - The drift is added to timestamps if the received time is after the send time. This indicates
///   that the sender's clock was lagging behind. For instance, if an event was received with
///   yesterday's timestamp, one day is added to all timestamps.
///
/// - The drift is subtracted from timestamps if the received time is before the send time. This
///   indicates that the sender's clock was running ahead. For instance, if an event was received
///   with tomorrow's timestamp, one day is subtracted from all timestamps.
#[derive(Debug)]
pub struct ClockDriftProcessor {
    received_at: DateTime<Utc>,
    correction: Option<ClockCorrection>,
}

impl ClockDriftProcessor {
    /// Creates a new `ClockDriftProcessor`.
    ///
    /// If no `sent_at` timestamp is provided, then clock drift correction is disabled. The drift is
    /// calculated from the signed difference between the receiver's and the sender's timestamp.
    pub fn new(sent_at: Option<DateTime<Utc>>, received_at: DateTime<Utc>) -> Self {
        let correction = sent_at.and_then(|sent_at| {
            ClockCorrection::new(sent_at, received_at)
                .at_least(SignedDuration::seconds(MINIMUM_CLOCK_DRIFT_SECS))
        });

        Self {
            received_at,
            correction,
        }
    }

    /// Returns `true` if the clocks are significantly drifted.
    pub fn is_drifted(&self) -> bool {
        self.correction.is_some()
    }

    /// Processes the given session.
    pub fn process_session(&self, session: &mut SessionUpdate) {
        if let Some(correction) = self.correction {
            session.timestamp = session.timestamp + correction.drift;
            session.started = session.started + correction.drift;
        }
    }
}

impl Processor for ClockDriftProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        event.process_child_values(self, state)?;

        if let Some(correction) = self.correction {
            let timestamp_meta = event.timestamp.meta_mut();
            timestamp_meta.add_error(Error::with(ErrorKind::InvalidData, |e| {
                let reason = format!(
                    "clock drift: all timestamps adjusted by {}",
                    HumanDuration(correction.drift)
                );

                e.insert("reason", reason);
                e.insert("sdk_time", correction.sent_at.to_string());
                e.insert("server_time", self.received_at.to_string());
            }));
        }

        Ok(())
    }

    fn process_timestamp(
        &mut self,
        timestamp: &mut Timestamp,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if let Some(correction) = self.correction {
            // NB: We're not setting the original value here, as this could considerably increase
            // the event's size. Instead, attach an error message to the top-level event.
            *timestamp = *timestamp + correction.drift;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::offset::TimeZone;

    use crate::processor::process_value;
    use crate::protocol::{
        Context, ContextInner, Contexts, EventType, SpanId, TraceContext, TraceId,
    };
    use crate::types::{Annotated, Object};

    fn create_transaction(start: DateTime<Utc>, end: DateTime<Utc>) -> Annotated<Event> {
        Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(end),
            start_timestamp: Annotated::new(start),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                        op: Annotated::new("http.server".to_owned()),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: Annotated::new(vec![]),
            ..Default::default()
        })
    }

    #[test]
    fn test_no_sent_at() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);
        let now = end;

        // No information on delay, do not default to anything.
        let mut processor = ClockDriftProcessor::new(None, now);
        let mut event = create_transaction(start, end);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();
        assert_eq!(*event.timestamp.value().unwrap(), end);
        assert_eq!(*event.start_timestamp.value().unwrap(), start);
    }

    #[test]
    fn test_no_clock_drift() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);

        let now = end;

        // The event was sent instantly without delay
        let mut processor = ClockDriftProcessor::new(Some(end), now);
        let mut event = create_transaction(start, end);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();
        assert_eq!(*event.timestamp.value().unwrap(), end);
        assert_eq!(*event.start_timestamp.value().unwrap(), start);
    }

    #[test]
    fn test_clock_drift_lower_bound() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);

        let drift = SignedDuration::minutes(1);
        let now = end + drift;

        // The event was sent and received with minimal delay, which should not correct
        let mut processor = ClockDriftProcessor::new(Some(end), now);
        let mut event = create_transaction(start, end);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();
        assert_eq!(*event.timestamp.value().unwrap(), end);
        assert_eq!(*event.start_timestamp.value().unwrap(), start);
    }

    #[test]
    fn test_clock_drift_from_past() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);

        let drift = SignedDuration::days(1);
        let now = end + drift;

        // The event was sent and received with delay
        let mut processor = ClockDriftProcessor::new(Some(end), now);
        let mut event = create_transaction(start, end);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();
        assert_eq!(*event.timestamp.value().unwrap(), now);
        assert_eq!(*event.start_timestamp.value().unwrap(), start + drift);
    }

    #[test]
    fn test_clock_drift_from_future() {
        let start = Utc.ymd(2000, 1, 1).and_hms(0, 0, 0);
        let end = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);

        let drift = -SignedDuration::days(1);
        let now = end + drift;

        // The event was sent and received with delay
        let mut processor = ClockDriftProcessor::new(Some(end), now);
        let mut event = create_transaction(start, end);
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();
        assert_eq!(*event.timestamp.value().unwrap(), now);
        assert_eq!(*event.start_timestamp.value().unwrap(), start + drift);
    }
}
