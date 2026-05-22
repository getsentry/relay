//! Time normalization for EAP items.

use chrono::{DateTime, Utc};
use relay_conventions::attributes::SENTRY__TIMESTAMP__SEQUENCE;
use relay_event_schema::{
    processor::{self, ProcessValue, ProcessingState},
    protocol::{Attributes, OurLog, SpanV2, Timestamp, TraceMetric},
};
use relay_protocol::{Annotated, ErrorKind, Remark, RemarkType};
use std::time::Duration;

use crate::ClockDriftProcessor;

/// Configuration parameters for [`normalize`].
#[derive(Debug, Default, Clone, Copy)]
pub struct Config {
    /// Apply a time sequence shift as provided by the SDK on the timestamp.
    ///
    /// This must only run once, to not shift the same timestamp multiple times and therefor should
    /// be limited to processing Relays.
    pub apply_sequence_shift: bool,
    /// Timestamp when the item was received.
    pub received_at: DateTime<Utc>,
    /// Client local timestamp when the SDK sent the item.
    pub sent_at: Option<DateTime<Utc>>,
    /// Maximum amount of time the timestamp is allowed to be in the past.
    pub max_in_past: Option<Duration>,
    /// Maximum amount of time the timestamp is allowed to be in the future.
    pub max_in_future: Option<Duration>,
    /// Limits clock drift correction to a minimum duration.
    pub minimum_clock_drift: Duration,
}

/// Normalizes and validates timestamps.
///
/// Applies a time shift correction to correct for time drift on clients, see [`ClockDriftProcessor`].
/// Also makes sure timestamps are within boundaries defined by [`Config::max_in_past`] and
/// [`Config::max_in_future`].
pub fn normalize<T>(item: &mut Annotated<T>, config: Config)
where
    T: TimeNormalize,
{
    let received_at = config.received_at;
    let mut sent_at = config.sent_at;
    let mut error_kind = ErrorKind::ClockDrift;

    let timestamp = item
        .value_mut()
        .as_mut()
        .map(|t| t.reference_timestamp_mut())
        .and_then(|ts| ts.value().copied());

    if let Some(timestamp) = timestamp {
        if config
            .max_in_past
            .is_some_and(|delta| timestamp < received_at - delta)
        {
            error_kind = ErrorKind::PastTimestamp;
            sent_at = Some(timestamp.into_inner());
        } else if config
            .max_in_future
            .is_some_and(|delta| timestamp > received_at + delta)
        {
            error_kind = ErrorKind::FutureTimestamp;
            sent_at = Some(timestamp.into_inner());
        }
    }

    let mut processor = ClockDriftProcessor::new(sent_at, received_at)
        .at_least(config.minimum_clock_drift)
        .error_kind(error_kind);

    if processor.is_drifted() {
        let _ = processor::process_value(item, &mut processor, ProcessingState::root());
        if let Some(item) = item.value_mut() {
            processor.apply_correction_meta(item.reference_timestamp_mut().meta_mut());
        }
    }

    let sequence = item
        .value()
        .and_then(|t| t.timestamp_sequence())
        .filter(|d| *d > 0);

    let timestamp = item
        .value_mut()
        .as_mut()
        .map(|t| t.reference_timestamp_mut());

    if config.apply_sequence_shift
        && let Some(sequence) = sequence
        && let Some(ts) = timestamp
        && let Some(ts_value) = ts.value_mut()
    {
        // Always unconditionally apply the time-shift, this puts us potentially slightly over `max_in_future`,
        // by up to ~5s, but this is preferable over losing the ordering.
        ts_value.0 += chrono::TimeDelta::nanoseconds(sequence.into());
        ts.meta_mut()
            .add_remark(Remark::new(RemarkType::Substituted, "timestamp.sequence"));
    }
}

/// Items which can be processed by [`normalize`].
pub trait TimeNormalize: ProcessValue {
    /// The base, reference timestamp of the item used for time shifts.
    ///
    /// Represents the timestamp when the item was created.
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp>;

    /// A tie breaker sent from SDKs for timestamps.
    ///
    /// This is usually stored in [`SENTRY__TIMESTAMP__SEQUENCE`] and applied as additional
    /// nanoseconds to the timestamp.
    fn timestamp_sequence(&self) -> Option<u32>;
}

impl TimeNormalize for OurLog {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.timestamp
    }

    fn timestamp_sequence(&self) -> Option<u32> {
        get_timestamp_sequence(&self.attributes)
    }
}

impl TimeNormalize for SpanV2 {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.start_timestamp
    }

    fn timestamp_sequence(&self) -> Option<u32> {
        // Not supported for spans.
        //
        // If this ever becomes necessary to add, extra care must be taken to not create invalid
        // spans where the start timestamp is moved after the end timestamp.
        None
    }
}

impl TimeNormalize for TraceMetric {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.timestamp
    }

    fn timestamp_sequence(&self) -> Option<u32> {
        get_timestamp_sequence(&self.attributes)
    }
}

fn get_timestamp_sequence(attributes: &Annotated<Attributes>) -> Option<u32> {
    attributes
        .value()
        .and_then(|attrs| attrs.get_value(SENTRY__TIMESTAMP__SEQUENCE))
        .and_then(|v| v.as_f64())
        .map(|v| v as _)
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_event_schema::processor::ProcessValue;
    use relay_protocol::{
        Annotated, Empty, FromValue, IntoValue, assert_annotated_snapshot, get_value,
    };

    #[derive(Debug, Clone, FromValue, IntoValue, Empty, ProcessValue)]
    struct TestItem {
        base: Annotated<Timestamp>,
        other: Annotated<Timestamp>,
    }

    impl TimeNormalize for TestItem {
        fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
            &mut self.base
        }

        fn timestamp_sequence(&self) -> Option<u32> {
            Some(123)
        }
    }

    fn ts(secs: i64) -> Timestamp {
        Timestamp(DateTime::from_timestamp_secs(secs).unwrap())
    }

    #[test]
    fn test_normalize_time_no_drift() {
        let mut item = Annotated::new(TestItem {
            base: ts(1_000).into(),
            other: ts(1_010).into(),
        });

        let config = Config {
            received_at: ts(1_100).0,
            ..Default::default()
        };

        normalize(&mut item, config);

        assert_annotated_snapshot!(item, @r#"
        {
          "base": 1000.0,
          "other": 1010.0
        }
        "#);
    }

    #[test]
    fn test_normalize_time_client_drift() {
        let mut item = Annotated::new(TestItem {
            base: ts(50_000).into(),
            other: ts(50_010).into(),
        });

        let config = Config {
            sent_at: Some(ts(0).0),
            received_at: ts(51_000).0,
            ..Default::default()
        };

        normalize(&mut item, config);

        assert_annotated_snapshot!(item, @r#"
        {
          "base": 101000.0,
          "other": 101010.0,
          "_meta": {
            "base": {
              "": {
                "err": [
                  [
                    "clock_drift",
                    {
                      "sdk_time": "1970-01-01T00:00:00+00:00",
                      "server_time": "1970-01-01T14:10:00+00:00"
                    }
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_time_too_far_in_past() {
        let mut item = Annotated::new(TestItem {
            base: ts(90_000).into(),
            other: ts(80_000).into(),
        });

        let config = Config {
            received_at: ts(100_000).0,
            max_in_past: Some(Duration::from_secs(10)),
            ..Default::default()
        };

        normalize(&mut item, config);

        assert_annotated_snapshot!(item, @r#"
        {
          "base": 100000.0,
          "other": 90000.0,
          "_meta": {
            "base": {
              "": {
                "err": [
                  [
                    "past_timestamp",
                    {
                      "sdk_time": "1970-01-02T01:00:00+00:00",
                      "server_time": "1970-01-02T03:46:40+00:00"
                    }
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_time_too_far_in_future() {
        let mut item = Annotated::new(TestItem {
            base: ts(90_000).into(),
            other: ts(80_000).into(),
        });

        let config = Config {
            received_at: ts(10_000).0,
            max_in_future: Some(Duration::from_secs(10)),
            ..Default::default()
        };

        normalize(&mut item, config);

        assert_annotated_snapshot!(item, @r#"
        {
          "base": 10000.0,
          "other": 0.0,
          "_meta": {
            "base": {
              "": {
                "err": [
                  [
                    "future_timestamp",
                    {
                      "sdk_time": "1970-01-02T01:00:00+00:00",
                      "server_time": "1970-01-01T02:46:40+00:00"
                    }
                  ]
                ]
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_time_sequence_shift() {
        let mut item = Annotated::new(TestItem {
            base: ts(90_000).into(),
            other: ts(80_000).into(),
        });

        let config = Config {
            apply_sequence_shift: true,
            ..Default::default()
        };

        normalize(&mut item, config);

        insta::assert_json_snapshot!(IntoValue::extract_meta_tree(&item), @r#"
        {
          "base": {
            "": {
              "rem": [
                [
                  "timestamp.sequence",
                  "s"
                ]
              ]
            }
          }
        }
        "#);

        // Need to assert the raw values instead of a snapshot because the serialization format of
        // `Timestamp` is not precise enough for nanosecond precision.
        assert_eq!(
            get_value!(item.base!).0,
            DateTime::from_timestamp_secs(90_000).unwrap() + chrono::TimeDelta::nanoseconds(123)
        );
        assert_eq!(
            get_value!(item.other!).0,
            DateTime::from_timestamp_secs(80_000).unwrap()
        );
    }

    #[test]
    fn test_normalize_time_sequence_shift_and_correction() {
        let mut item = Annotated::new(TestItem {
            base: ts(90_000).into(),
            other: ts(80_000).into(),
        });

        let config = Config {
            apply_sequence_shift: true,
            received_at: ts(10_000).0,
            max_in_future: Some(Duration::from_secs(10)),
            ..Default::default()
        };

        normalize(&mut item, config);

        insta::assert_json_snapshot!(IntoValue::extract_meta_tree(&item), @r#"
        {
          "base": {
            "": {
              "rem": [
                [
                  "timestamp.sequence",
                  "s"
                ]
              ],
              "err": [
                [
                  "future_timestamp",
                  {
                    "sdk_time": "1970-01-02T01:00:00+00:00",
                    "server_time": "1970-01-01T02:46:40+00:00"
                  }
                ]
              ]
            }
          }
        }
        "#);

        assert_eq!(
            get_value!(item.base!).0,
            DateTime::from_timestamp_secs(10_000).unwrap() + chrono::TimeDelta::nanoseconds(123)
        );
        assert_eq!(
            get_value!(item.other!).0,
            DateTime::from_timestamp_secs(0).unwrap()
        );
    }
}
