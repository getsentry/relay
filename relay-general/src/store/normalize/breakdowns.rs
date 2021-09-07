//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.

use std::collections::HashMap;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::protocol::{Breakdowns, Event, Measurement, Measurements, Timestamp};
use crate::types::Annotated;

#[derive(Clone, Debug)]
pub struct TimeWindowSpan {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeWindowSpan {
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        if end < start {
            return TimeWindowSpan {
                start: end,
                end: start,
            };
        }

        TimeWindowSpan { start, end }
    }

    pub fn duration(&self) -> f64 {
        let delta: f64 = (self.end.timestamp_nanos() - self.start.timestamp_nanos()) as f64;
        // convert to milliseconds (1 ms = 1,000,000 nanoseconds)
        (delta / 1_000_000.00).abs()
    }
}

#[derive(PartialEq, Eq, Hash)]
enum OperationBreakdown<'a> {
    Emit(&'a str),
    DoNotEmit(&'a str),
}

fn get_op_time_spent(mut intervals: Vec<TimeWindowSpan>) -> Option<f64> {
    if intervals.is_empty() {
        return None;
    }

    // sort by start timestamp in ascending order
    intervals.sort_unstable_by_key(|span| span.start);

    let mut op_time_spent = 0.0;
    let mut previous_interval: Option<TimeWindowSpan> = None;

    for current_interval in intervals.into_iter() {
        match previous_interval.as_mut() {
            Some(last_interval) => {
                if last_interval.end < current_interval.start {
                    // if current_interval does not overlap with last_interval,
                    // then add last_interval to op_time_spent
                    op_time_spent += last_interval.duration();
                    previous_interval = Some(current_interval);
                    continue;
                }

                // current_interval and last_interval overlaps; so we merge these intervals

                // invariant: last_interval.start <= current_interval.start

                last_interval.end = std::cmp::max(last_interval.end, current_interval.end);
            }
            None => {
                previous_interval = Some(current_interval);
            }
        };
    }

    if let Some(remaining_interval) = previous_interval {
        op_time_spent += remaining_interval.duration();
    }

    Some(op_time_spent)
}

/// Emit breakdowns that are derived using information from the given event.
pub trait EmitBreakdowns {
    fn emit_breakdowns(&self, event: &Event) -> Option<Measurements>;
}

/// Configuration to define breakdowns based on span operation name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SpanOperationsConfig {
    /// Operation names are matched against an array of strings. The match is successful if the span
    /// operation name starts with any string in the array. If any string in the array has at least
    /// one match, then a breakdown group is created, and its name will be the matched string.
    pub matches: Vec<String>,
}

impl EmitBreakdowns for SpanOperationsConfig {
    fn emit_breakdowns(&self, event: &Event) -> Option<Measurements> {
        let operation_name_breakdowns = &self.matches;

        if operation_name_breakdowns.is_empty() {
            return None;
        }

        let spans = match event.spans.value() {
            None => return None,
            Some(spans) => spans,
        };

        // Generate span operation breakdowns
        let mut intervals = HashMap::new();

        for span in spans.iter() {
            let span = match span.value() {
                None => continue,
                Some(span) => span,
            };

            let operation_name = match span.op.value() {
                None => continue,
                Some(span_op) => span_op,
            };

            let start = match span.start_timestamp.value() {
                None => continue,
                Some(start) => start,
            };

            let end = match span.timestamp.value() {
                None => continue,
                Some(end) => end,
            };

            let cover = TimeWindowSpan::new(*start, *end);

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

        let mut total_time_spent = 0.0;

        for (operation_name, intervals) in intervals {
            let op_time_spent = match get_op_time_spent(intervals) {
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
    #[serde(alias = "span_operations")]
    SpanOperations(SpanOperationsConfig),
    #[serde(other)]
    Unsupported,
}

impl EmitBreakdowns for BreakdownConfig {
    fn emit_breakdowns(&self, event: &Event) -> Option<Measurements> {
        match self {
            BreakdownConfig::SpanOperations(config) => config.emit_breakdowns(event),
            BreakdownConfig::Unsupported => None,
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

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: &BreakdownsConfig) {
    if breakdowns_config.is_empty() {
        return;
    }

    for (breakdown_name, breakdown_config) in breakdowns_config.iter() {
        // TODO: move this to deserialization in a follow-up.
        if !Breakdowns::is_valid_breakdown_name(breakdown_name) {
            continue;
        }

        let breakdown = match breakdown_config.emit_breakdowns(event) {
            None => continue,
            Some(breakdown) => breakdown,
        };

        if breakdown.is_empty() {
            continue;
        }

        let breakdowns = event
            .breakdowns
            .value_mut()
            .get_or_insert_with(Breakdowns::default);

        let span_ops_breakdown = breakdowns
            .entry(breakdown_name.clone())
            .or_insert_with(|| Annotated::new(Measurements::default()))
            .value_mut()
            .get_or_insert_with(Measurements::default);

        span_ops_breakdown.extend(breakdown.into_inner());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{EventType, Span, SpanId, SpanStatus, TraceId};
    use crate::types::Object;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_skip_with_empty_breakdowns_config() {
        let mut event = Event::default();
        normalize_breakdowns(&mut event, &BreakdownsConfig::default());
        assert_eq!(event.breakdowns.value(), None);
    }

    #[test]
    fn test_noop_breakdowns_with_empty_config() {
        let breakdowns = Breakdowns({
            let mut span_ops_breakdown = Measurements::default();

            span_ops_breakdown.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                }),
            );

            let mut breakdowns = Object::new();
            breakdowns.insert("span_ops".to_owned(), Annotated::new(span_ops_breakdown));

            breakdowns
        });

        let mut event = Event {
            ty: EventType::Transaction.into(),
            breakdowns: breakdowns.clone().into(),
            ..Default::default()
        };
        normalize_breakdowns(&mut event, &BreakdownsConfig::default());
        assert_eq!(event.breakdowns.into_value().unwrap(), breakdowns);
    }

    #[test]
    fn test_emit_ops_breakdown() {
        fn make_span(
            start: Annotated<Timestamp>,
            end: Annotated<Timestamp>,
            op_name: String,
        ) -> Annotated<Span> {
            Annotated::new(Span {
                timestamp: end,
                start_timestamp: start,
                description: Annotated::new("desc".to_owned()),
                op: Annotated::new(op_name),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74052".into())),
                status: Annotated::new(SpanStatus::Ok),
                ..Default::default()
            })
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
                "db.postgres".to_string(),
            ),
            make_span(
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(4, 0, 0, 0).into()),
                Annotated::new(Utc.ymd(2020, 1, 1).and_hms_nano(4, 30, 0, 0).into()),
                "db.mongo".to_string(),
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
            breakdowns: Breakdowns({
                let mut span_ops_breakdown = Measurements::default();

                span_ops_breakdown.insert(
                    "lcp".to_owned(),
                    Annotated::new(Measurement {
                        value: Annotated::new(420.69),
                    }),
                );

                let mut breakdowns = Object::new();
                breakdowns.insert("span_ops".to_owned(), Annotated::new(span_ops_breakdown));

                breakdowns
            })
            .into(),
            ..Default::default()
        };

        let breakdowns_config = BreakdownsConfig({
            let mut config = HashMap::new();

            let span_ops_config = BreakdownConfig::SpanOperations(SpanOperationsConfig {
                matches: vec!["http".to_string(), "db".to_string()],
            });

            config.insert("span_ops".to_string(), span_ops_config.clone());
            config.insert("span_ops_2".to_string(), span_ops_config);

            config
        });

        normalize_breakdowns(&mut event, &breakdowns_config);

        let expected_breakdowns = Breakdowns({
            let mut span_ops_breakdown = Measurements::default();

            span_ops_breakdown.insert(
                "ops.http".to_owned(),
                Annotated::new(Measurement {
                    // 1 hour in milliseconds
                    value: Annotated::new(3_600_000.0),
                }),
            );

            span_ops_breakdown.insert(
                "ops.db".to_owned(),
                Annotated::new(Measurement {
                    // 2 hours in milliseconds
                    value: Annotated::new(7_200_000.0),
                }),
            );

            span_ops_breakdown.insert(
                "total.time".to_owned(),
                Annotated::new(Measurement {
                    // 4 hours and 10 microseconds in milliseconds
                    value: Annotated::new(14_400_000.01),
                }),
            );

            let mut breakdowns = Object::new();
            breakdowns.insert(
                "span_ops_2".to_owned(),
                Annotated::new(span_ops_breakdown.clone()),
            );

            span_ops_breakdown.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                }),
            );
            breakdowns.insert("span_ops".to_owned(), Annotated::new(span_ops_breakdown));

            breakdowns
        });

        assert_eq!(event.breakdowns.into_value().unwrap(), expected_breakdowns);
    }
}
