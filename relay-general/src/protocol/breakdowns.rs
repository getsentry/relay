use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::protocol::{Event, Measurement, Measurements, Timestamp};
use crate::types::{Annotated, Error, FromValue, Object, Value};

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

    fn get_duration(&self) -> f64 {
        let delta: f64 =
            (self.end_timestamp.timestamp_nanos() - self.start_timestamp.timestamp_nanos()) as f64;
        // convert to milliseconds (1 ms = 1,000,000 nanoseconds)
        (delta / 1_000_000.00).abs()
    }
}

#[derive(PartialEq, Eq, Hash)]
enum OperationBreakdown<'op_name> {
    Emit(&'op_name str),
    DoNotEmit(&'op_name str),
}

type OperationNameIntervals<'op_name> = HashMap<OperationBreakdown<'op_name>, Vec<TimeWindowSpan>>;

fn get_op_time_spent(mut intervals: Vec<TimeWindowSpan>) -> Option<f64> {
    if intervals.is_empty() {
        return None;
    }

    // sort by start_timestamp in ascending order
    intervals.sort_unstable_by(|a, b| a.start_timestamp.partial_cmp(&b.start_timestamp).unwrap());

    let mut op_time_spent: f64 = 0.0;
    let mut previous_interval: Option<TimeWindowSpan> = None;

    for current_interval in intervals.into_iter() {
        match previous_interval.as_mut() {
            Some(last_interval) => {
                if last_interval.end_timestamp < current_interval.start_timestamp {
                    // if current_interval does not overlap with last_interval,
                    // then add last_interval to op_time_spent
                    op_time_spent += last_interval.get_duration();
                    previous_interval = Some(current_interval);
                    continue;
                }

                // current_interval and last_interval overlaps; so we merge these intervals

                // invariant: last_interval.start_timestamp <= current_interval.start_timestamp

                last_interval.end_timestamp =
                    std::cmp::max(last_interval.end_timestamp, current_interval.end_timestamp);
            }
            None => {
                previous_interval = Some(current_interval);
                continue;
            }
        };
    }

    if let Some(remaining_interval) = previous_interval {
        op_time_spent += remaining_interval.get_duration();
    }

    Some(op_time_spent)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub struct SpanOperationsConfig {
    pub matches: Vec<String>,
}

impl SpanOperationsConfig {
    pub fn parse_event(&self, event: &Event) -> Option<Measurements> {
        let operation_name_breakdowns = &self.matches;

        if operation_name_breakdowns.is_empty() {
            return None;
        }

        let spans = match event.spans.value() {
            None => return None,
            Some(spans) => spans,
        };

        // Generate span operation breakdowns
        let mut intervals: OperationNameIntervals = HashMap::new();

        for span in spans.iter() {
            let span = match span.value() {
                None => continue,
                Some(span) => span,
            };

            let operation_name = match span.op.value() {
                None => continue,
                Some(span_op) => span_op,
            };

            let start_timestamp = match span.start_timestamp.value() {
                None => continue,
                Some(start_timestamp) => start_timestamp,
            };

            let end_timestamp = match span.timestamp.value() {
                None => continue,
                Some(end_timestamp) => end_timestamp,
            };

            let cover = TimeWindowSpan::new(*start_timestamp, *end_timestamp);

            // Only emit an operation breakdown measurement if the operation name matches any
            // entries in operation_name_breakdown.
            let results = operation_name_breakdowns
                .iter()
                .find(|maybe| operation_name.starts_with(*maybe));

            let operation_name = match results {
                None => OperationBreakdown::DoNotEmit(operation_name),
                Some(operation_name) => OperationBreakdown::Emit(operation_name),
            };

            intervals
                .entry(operation_name)
                .or_insert_with(Vec::new)
                .push(cover);
        }

        if intervals.is_empty() {
            return None;
        }

        let mut breakdown = Measurements::default();

        let mut total_time_spent: f64 = 0.0;

        for (operation_name, intervals) in intervals {
            let op_time_spent: f64 = match get_op_time_spent(intervals) {
                None => continue,
                Some(op_time_spent) => op_time_spent,
            };

            total_time_spent += op_time_spent;

            let operation_name = match operation_name {
                OperationBreakdown::DoNotEmit(_) => continue,
                OperationBreakdown::Emit(operation_name) => operation_name,
            };

            let time_spent_measurement = Measurement {
                value: Annotated::new(op_time_spent),
            };

            let op_breakdown_name = format!("ops.{}", operation_name);

            breakdown.insert(op_breakdown_name, Annotated::new(time_spent_measurement));
        }

        let total_time_spent_measurement = Measurement {
            value: Annotated::new(total_time_spent),
        };
        breakdown.insert(
            "total.time".to_string(),
            Annotated::new(total_time_spent_measurement),
        );

        Some(breakdown)
    }
}

/// Configuration to define breakdown to be generated based on properties and breakdown type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BreakdownConfig {
    SpanOperations(SpanOperationsConfig),
}

impl BreakdownConfig {
    pub fn parse_event(&self, event: &Event) -> Option<Measurements> {
        match self {
            BreakdownConfig::SpanOperations(config) => config.parse_event(event),
        }
    }
}

type BreakdownName = String;

/// Represents the breakdown configuration for a project.
/// Generate a named (key) breakdown (value).
///
/// Breakdowns are product-defined numbers that are indirectly reported by the client, and are materialized
/// during ingestion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BreakdownsConfig(pub HashMap<BreakdownName, BreakdownConfig>);

impl Deref for BreakdownsConfig {
    type Target = HashMap<BreakdownName, BreakdownConfig>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A map of breakdowns.
/// Breakdowns may be available on any event type. A breakdown are product-defined measurement values
/// generated by the client, or materialized during ingestion. For example, for transactions, we may
/// emit span operation breakdowns based on the attached span data.
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Breakdowns(pub Object<Measurements>);

impl Breakdowns {
    pub fn is_valid_breakdown_name(name: &str) -> bool {
        name.chars()
            .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.'))
    }
}

impl FromValue for Breakdowns {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        let mut processing_errors = Vec::new();

        let mut breakdowns = Object::from_value(value).map_value(|breakdowns| {
            let breakdowns = breakdowns
                .into_iter()
                .filter_map(|(name, object)| {
                    let name = name.trim();

                    if Breakdowns::is_valid_breakdown_name(name) {
                        return Some((name.into(), object));
                    } else {
                        processing_errors.push(Error::invalid(format!(
                            "breakdown name '{}' can contain only characters a-z0-9.-_",
                            name
                        )));
                    }

                    None
                })
                .collect();

            Self(breakdowns)
        });

        for error in processing_errors {
            breakdowns.meta_mut().add_error(error);
        }

        breakdowns
    }
}

impl Deref for Breakdowns {
    type Target = Object<Measurements>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Breakdowns {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
