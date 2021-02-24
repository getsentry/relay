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
