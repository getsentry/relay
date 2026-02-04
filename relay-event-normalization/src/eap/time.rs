//! Time normalization for EAP items.

use chrono::{DateTime, Utc};
use relay_event_schema::{
    processor::{self, ProcessValue, ProcessingState},
    protocol::{OurLog, SpanV2, Timestamp, TraceMetric},
};
use relay_protocol::{Annotated, ErrorKind};
use std::time::Duration;

use crate::ClockDriftProcessor;

/// Configuration parameters for [`normalize`].
#[derive(Debug, Default, Clone, Copy)]
pub struct Config {
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
        .and_then(|t| t.reference_timestamp_mut().value().copied());

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
}

/// Items which can be processed by [`normalize`].
pub trait TimeNormalize: ProcessValue {
    /// The base, reference timestamp of the item used for time shifts.
    ///
    /// Represents the timestamp when the item was created.
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp>;
}

impl TimeNormalize for OurLog {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.timestamp
    }
}

impl TimeNormalize for SpanV2 {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.start_timestamp
    }
}

impl TimeNormalize for TraceMetric {
    fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
        &mut self.timestamp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use relay_event_schema::processor::ProcessValue;
    use relay_protocol::{Annotated, Empty, FromValue, IntoValue, assert_annotated_snapshot};

    #[derive(Debug, Clone, FromValue, IntoValue, Empty, ProcessValue)]
    struct TestItem {
        base: Annotated<Timestamp>,
        other: Annotated<Timestamp>,
    }

    impl TimeNormalize for TestItem {
        fn reference_timestamp_mut(&mut self) -> &mut Annotated<Timestamp> {
            &mut self.base
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
}
