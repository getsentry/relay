use std::time::Duration;

use chrono::{DateTime, Duration as SignedDuration, Utc};
use relay_common::UnixTimestamp;

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{Event, Timestamp};
use crate::types::{Error, ErrorKind, Meta, ProcessingResult};

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

    fn at_least(self, lower_bound: Duration) -> Option<Self> {
        if self.drift.num_seconds().unsigned_abs() >= lower_bound.as_secs() {
            Some(self)
        } else {
            None
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
    kind: ErrorKind,
}

impl ClockDriftProcessor {
    /// Creates a new `ClockDriftProcessor`.
    ///
    /// If no `sent_at` timestamp is provided, then clock drift correction is disabled. The drift is
    /// calculated from the signed difference between the receiver's and the sender's timestamp.
    pub fn new(sent_at: Option<DateTime<Utc>>, received_at: DateTime<Utc>) -> Self {
        let correction = sent_at.map(|sent_at| ClockCorrection::new(sent_at, received_at));

        Self {
            received_at,
            correction,
            kind: ErrorKind::ClockDrift,
        }
    }

    /// Limits clock drift correction to a minimum duration.
    ///
    /// If the detected clock drift is lower than the given duration, no correction is performed and
    /// `is_drifted` returns `false`. By default, there is no lower bound and every drift is
    /// corrected.
    pub fn at_least(mut self, lower_bound: Duration) -> Self {
        self.correction = self.correction.and_then(|c| c.at_least(lower_bound));
        self
    }

    /// Use the given error kind for the attached eventerror instead of the default
    /// `ErrorKind::ClockDrift`.
    pub fn error_kind(mut self, kind: ErrorKind) -> Self {
        self.kind = kind;
        self
    }

    /// Returns `true` if the clocks are significantly drifted.
    pub fn is_drifted(&self) -> bool {
        self.correction.is_some()
    }

    /// Processes the given `UnixTimestamp` by applying clock drift correction.
    pub fn process_timestamp(&self, timestamp: &mut UnixTimestamp) {
        if let Some(correction) = self.correction {
            let secs = correction.drift.num_seconds();
            *timestamp = if secs > 0 {
                UnixTimestamp::from_secs(timestamp.as_secs() + secs as u64)
            } else {
                UnixTimestamp::from_secs(timestamp.as_secs() - secs.saturating_abs() as u64)
            }
        }
    }

    /// Processes the given [`DateTime`].
    pub fn process_datetime(&self, datetime: &mut DateTime<Utc>) {
        if let Some(correction) = self.correction {
            *datetime = *datetime + correction.drift;
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
        if let Some(correction) = self.correction {
            event.process_child_values(self, state)?;

            let timestamp_meta = event.timestamp.meta_mut();
            timestamp_meta.add_error(Error::with(self.kind.clone(), |e| {
                e.insert("sdk_time", correction.sent_at.to_rfc3339());
                e.insert("server_time", self.received_at.to_rfc3339());
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
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
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
        let mut processor =
            ClockDriftProcessor::new(Some(end), now).at_least(Duration::from_secs(3600));
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

        let drift = -SignedDuration::seconds(60);
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
    fn test_clock_drift_unix() {
        let sent_at = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);
        let drift = SignedDuration::days(1);
        let now = sent_at + drift;

        let processor = ClockDriftProcessor::new(Some(sent_at), now);
        let mut timestamp = UnixTimestamp::from_secs(sent_at.timestamp() as u64);
        processor.process_timestamp(&mut timestamp);

        assert_eq!(timestamp.as_secs(), now.timestamp() as u64);
    }

    #[test]
    fn test_process_datetime() {
        let sent_at = Utc.ymd(2000, 1, 2).and_hms(0, 0, 0);
        let drift = SignedDuration::days(1);
        let now = sent_at + drift;

        let processor = ClockDriftProcessor::new(Some(sent_at), now);
        let mut datetime = Utc.ymd(2021, 11, 29).and_hms(0, 0, 0);
        processor.process_datetime(&mut datetime);

        assert_eq!(datetime, Utc.ymd(2021, 11, 30).and_hms(0, 0, 0));
    }
}
