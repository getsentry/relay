//! Contains the measurements normalization code
//!
//! This module is responsible for ensuring the measurements interface is only present for transaction
//! events, and generating operation breakdown measurements.
//!

use std::collections::HashMap;

use crate::protocol::{Event, EventType, Measurement, Measurements, Timestamp};
use crate::types::Annotated;

#[derive(Clone, Debug)]
struct TimeWindowSpan {
    start_timestamp: Timestamp,
    end_timestamp: Timestamp,
}

impl TimeWindowSpan {
    fn new(start_timestamp: Timestamp, end_timestamp: Timestamp) -> Self {
        if end_timestamp < start_timestamp {
            return TimeWindowSpan {
                start_timestamp: end_timestamp,
                end_timestamp: start_timestamp,
            };
        }

        TimeWindowSpan {
            start_timestamp,
            end_timestamp,
        }
    }
}

type OperationName = String;

type OperationNameIntervals = HashMap<OperationName, Vec<TimeWindowSpan>>;

fn merge_intervals(mut intervals: Vec<TimeWindowSpan>) -> Vec<TimeWindowSpan> {
    // sort by start_timestamp in ascending order
    intervals.sort_unstable_by(|a, b| a.start_timestamp.partial_cmp(&b.start_timestamp).unwrap());

    intervals.into_iter().fold(
        vec![],
        |mut merged, current_interval| -> Vec<TimeWindowSpan> {
            // merged is a vector of disjoint intervals

            if merged.is_empty() {
                merged.push(current_interval);
                return merged;
            }

            let mut last_interval = merged.last_mut().unwrap();

            if last_interval.end_timestamp < current_interval.start_timestamp {
                // if current_interval does not overlap with last_interval,
                // then add current_interval
                merged.push(current_interval);
                return merged;
            }

            // current_interval and last_interval overlaps; so we merge these intervals

            // invariant: last_interval.start_timestamp <= current_interval.start_timestamp

            last_interval.end_timestamp =
                std::cmp::max(last_interval.end_timestamp, current_interval.end_timestamp);

            merged
        },
    )
}

/// Ensure measurements interface is only present for transaction events, and emit operation breakdown measurements
pub fn normalize_measurements(event: &mut Event, operation_name_breakdown: &Option<Vec<String>>) {
    if event.ty.value() != Some(&EventType::Transaction) {
        // Only transaction events may have a measurements interface
        event.measurements = Annotated::empty();
        return;
    }

    let operation_name_breakdown: Vec<String> = match operation_name_breakdown {
        None => return,
        Some(operation_name_breakdown) => operation_name_breakdown
            .iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect(),
    };

    if operation_name_breakdown.is_empty() {
        return;
    }

    // Generate operation breakdowns
    if let Some(spans) = event.spans.value() {
        let intervals: OperationNameIntervals =
            spans
                .iter()
                .fold(HashMap::new(), |mut intervals, span| match span.value() {
                    None => intervals,
                    Some(span) => {
                        let cover = TimeWindowSpan::new(
                            *span.start_timestamp.value().unwrap(),
                            *span.timestamp.value().unwrap(),
                        );

                        let operation_name = span.op.value().unwrap();

                        let results = operation_name_breakdown
                            .iter()
                            .find(|maybe| operation_name.starts_with(*maybe));

                        let operation_name = match results {
                            None => return intervals,
                            Some(operation_name) => operation_name.clone(),
                        };

                        intervals
                            .entry(operation_name)
                            .or_insert_with(Vec::new)
                            .push(cover);

                        intervals
                    }
                });

        if intervals.is_empty() {
            return;
        }

        let measurements = event
            .measurements
            .value_mut()
            .get_or_insert_with(Measurements::default);

        let mut total_time_spent: f64 = 0.0;

        for (operation_name, intervals) in intervals {
            let op_time_spent: f64 =
                merge_intervals(intervals)
                    .into_iter()
                    .fold(0.0, |sum, interval| {
                        let delta: f64 = (interval.end_timestamp.timestamp_nanos()
                            - interval.start_timestamp.timestamp_nanos())
                            as f64;
                        // convert to milliseconds (1 ms = 1,000,000 nanoseconds)
                        let duration: f64 = (delta / 1_000_000.00).abs();

                        sum + duration
                    });

            total_time_spent += op_time_spent;

            let time_spent_measurement = Measurement {
                value: Annotated::new(op_time_spent),
            };

            let op_breakdown_name = format!("ops.time.{}", operation_name.to_string());

            measurements.insert(op_breakdown_name, Annotated::new(time_spent_measurement));
        }

        let total_time_spent_measurement = Measurement {
            value: Annotated::new(total_time_spent),
        };
        measurements.insert(
            "ops.total.time".to_string(),
            Annotated::new(total_time_spent_measurement),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Span, SpanId, SpanStatus, TraceId};
    use crate::types::{Annotated, Object};
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_skip_no_measurements() {
        let mut event = Event::default();
        normalize_measurements(&mut event, &None);
        assert_eq!(event.measurements.value(), None);
    }

    #[test]
    fn test_remove_measurements_for_non_transaction_events() {
        let measurements = Annotated::new(Measurements({
            let mut measurements = Object::new();
            measurements.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                }),
            );

            measurements
        }));

        let mut event = Event {
            measurements,
            ..Default::default()
        };
        normalize_measurements(&mut event, &None);
        assert_eq!(event.measurements.value(), None);
    }

    #[test]
    fn test_noop_measurements_transaction_events() {
        let measurements = Measurements({
            let mut measurements = Object::new();
            measurements.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                }),
            );

            measurements
        });

        let mut event = Event {
            ty: EventType::Transaction.into(),
            measurements: measurements.clone().into(),
            ..Default::default()
        };
        normalize_measurements(&mut event, &None);
        assert_eq!(event.measurements.into_value().unwrap(), measurements);
    }

    #[test]
    fn test_emit_ops_breakdown_measurements() {
        fn make_span(
            start_timestamp: Annotated<Timestamp>,
            end_timestamp: Annotated<Timestamp>,
            op_name: String,
        ) -> Annotated<Span> {
            return Annotated::new(Span {
                timestamp: end_timestamp,
                start_timestamp,
                description: Annotated::new("desc".to_owned()),
                op: Annotated::new(op_name),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
                status: Annotated::new(SpanStatus::Ok),
                ..Default::default()
            });
        }

        let spans = vec![
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(0, 0, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(1, 0, 0, 0).into()),
                "http".to_string(),
            ),
            // overlapping spans
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(2, 0, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(3, 0, 0, 0).into()),
                "db".to_string(),
            ),
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(2, 30, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(3, 30, 0, 0).into()),
                "db".to_string(),
            ),
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(4, 0, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(4, 30, 0, 0).into()),
                "db".to_string(),
            ),
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(5, 0, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(6, 0, 0, 10_000).into()),
                "browser".to_string(),
            ),
        ];

        let mut event = Event {
            ty: EventType::Transaction.into(),
            spans: spans.into(),
            measurements: Measurements({
                let mut measurements = Object::new();
                measurements.insert(
                    "lcp".to_owned(),
                    Annotated::new(Measurement {
                        value: Annotated::new(420.69),
                    }),
                );

                measurements
            })
            .into(),
            ..Default::default()
        };

        let ops_breakdown_config = vec!["http".to_string(), "db".to_string()];

        normalize_measurements(&mut event, &ops_breakdown_config.into());

        let expected_measurements = Measurements({
            let mut measurements = Object::new();
            measurements.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                }),
            );
            measurements.insert(
                "ops.time.http".to_owned(),
                Annotated::new(Measurement {
                    // 1 hour in milliseconds
                    value: Annotated::new(3_600_000.0),
                }),
            );

            measurements.insert(
                "ops.time.db".to_owned(),
                Annotated::new(Measurement {
                    // 2 hours in milliseconds
                    value: Annotated::new(7_200_000.0),
                }),
            );

            measurements.insert(
                "ops.total.time".to_owned(),
                Annotated::new(Measurement {
                    // 3 hours and 10 microseconds in milliseconds
                    value: Annotated::new(10_800_000.01),
                }),
            );

            measurements
        });

        assert_eq!(
            event.measurements.into_value().unwrap(),
            expected_measurements
        );
    }
}
