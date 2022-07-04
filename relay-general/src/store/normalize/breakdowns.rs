//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.

use std::collections::HashMap;
use std::ops::Deref;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use relay_common::{DurationUnit, MetricUnit};

use crate::protocol::{Breakdowns, Event, Measurement, Measurements, Timestamp};
use crate::types::Annotated;

#[derive(Clone, Copy, Debug)]
pub struct TimeWindowSpan {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeWindowSpan {
    pub fn new(mut start: Timestamp, mut end: Timestamp) -> Self {
        if end < start {
            std::mem::swap(&mut start, &mut end);
        }

        TimeWindowSpan { start, end }
    }

    pub fn duration(&self) -> Duration {
        // Cannot fail since durations are ordered in the constructor
        (self.end - self.start).to_std().unwrap_or_default()
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
enum OperationBreakdown<'a> {
    Emit(&'a str),
    DoNotEmit(&'a str),
}

fn get_operation_duration(mut intervals: Vec<TimeWindowSpan>) -> Duration {
    intervals.sort_unstable_by_key(|span| span.start);

    let mut duration = Duration::new(0, 0);
    let mut last_end = None;

    for mut interval in intervals {
        if let Some(cutoff) = last_end {
            // ensure the current interval doesn't overlap with the last one
            interval = TimeWindowSpan::new(interval.start.max(cutoff), interval.end.max(cutoff));
        }

        duration += interval.duration();
        last_end = Some(interval.end);
    }

    duration
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
        if self.matches.is_empty() {
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

            let name = match span.op.as_str() {
                None => continue,
                Some(span_op) => span_op,
            };

            let interval = match (span.start_timestamp.value(), span.timestamp.value()) {
                (Some(start), Some(end)) => TimeWindowSpan::new(*start, *end),
                _ => continue,
            };

            let key = match self.matches.iter().find(|n| name.starts_with(*n)) {
                Some(op_name) => OperationBreakdown::Emit(op_name),
                None => OperationBreakdown::DoNotEmit(name),
            };

            intervals.entry(key).or_insert_with(Vec::new).push(interval);
        }

        if intervals.is_empty() {
            return None;
        }

        let mut breakdown = Measurements::default();
        let mut total_time = Duration::new(0, 0);

        for (key, intervals) in intervals {
            if intervals.is_empty() {
                continue;
            }

            let op_duration = get_operation_duration(intervals);
            total_time += op_duration;

            let operation_name = match key {
                OperationBreakdown::Emit(name) => name,
                OperationBreakdown::DoNotEmit(_) => continue,
            };

            let op_value = Measurement {
                value: Annotated::new(relay_common::duration_to_millis(op_duration)),
                unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
            };

            let op_breakdown_name = format!("ops.{}", operation_name);
            breakdown.insert(op_breakdown_name, Annotated::new(op_value));
        }

        let total_time_value = Annotated::new(Measurement {
            value: Annotated::new(relay_common::duration_to_millis(total_time)),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        });
        breakdown.insert("total.time".to_string(), total_time_value);

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

pub fn get_breakdown_measurements<'a>(
    event: &'a Event,
    breakdowns_config: &'a BreakdownsConfig,
) -> impl Iterator<Item = (&'a str, Measurements)> {
    breakdowns_config
        .iter()
        .filter_map(move |(breakdown_name, breakdown_config)| {
            // TODO: move this to deserialization in a follow-up.
            if !Breakdowns::is_valid_breakdown_name(breakdown_name) {
                return None;
            }

            let measurements = breakdown_config.emit_breakdowns(event)?;

            if measurements.is_empty() {
                return None;
            }

            Some((breakdown_name.as_str(), measurements))
        })
}

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: &BreakdownsConfig) {
    let mut event_breakdowns = Breakdowns::default();

    for (breakdown_name, breakdown) in get_breakdown_measurements(event, breakdowns_config) {
        event_breakdowns
            .entry(breakdown_name.to_owned())
            .or_insert_with(|| Annotated::new(Measurements::default()))
            .value_mut()
            .get_or_insert_with(Measurements::default)
            .extend(breakdown.into_inner());
    }

    // Do not accept SDK-defined breakdowns. This is required for idempotency in multiple layers of
    // Relay, and also such that performance metrics extraction produces the correct data.
    if event_breakdowns.is_empty() {
        event.breakdowns = Annotated::empty();
    } else {
        event.breakdowns = Annotated::new(event_breakdowns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{EventType, Span, SpanId, SpanStatus, TraceId};
    use crate::testutils::assert_eq_dbg;
    use crate::types::Object;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_skip_with_empty_breakdowns_config() {
        let mut event = Event::default();
        normalize_breakdowns(&mut event, &BreakdownsConfig::default());
        assert_eq_dbg!(event.breakdowns.value(), None);
    }

    #[test]
    fn test_noop_breakdowns_with_empty_config() {
        let breakdowns = Breakdowns({
            let mut span_ops_breakdown = Measurements::default();

            span_ops_breakdown.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                    unit: Annotated::empty(),
                }),
            );

            let mut breakdowns = Object::new();
            breakdowns.insert("span_ops".to_owned(), Annotated::new(span_ops_breakdown));

            breakdowns
        });

        let mut event = Event {
            ty: EventType::Transaction.into(),
            breakdowns: breakdowns.into(),
            ..Default::default()
        };
        normalize_breakdowns(&mut event, &BreakdownsConfig::default());
        assert_eq_dbg!(event.breakdowns.into_value(), None);
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
                    unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
                }),
            );

            span_ops_breakdown.insert(
                "ops.db".to_owned(),
                Annotated::new(Measurement {
                    // 2 hours in milliseconds
                    value: Annotated::new(7_200_000.0),
                    unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
                }),
            );

            span_ops_breakdown.insert(
                "total.time".to_owned(),
                Annotated::new(Measurement {
                    // 4 hours and 10 microseconds in milliseconds
                    value: Annotated::new(14_400_000.01),
                    unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
                }),
            );

            let mut breakdowns = Object::new();
            breakdowns.insert(
                "span_ops_2".to_owned(),
                Annotated::new(span_ops_breakdown.clone()),
            );

            breakdowns.insert("span_ops".to_owned(), Annotated::new(span_ops_breakdown));

            breakdowns
        });

        assert_eq_dbg!(event.breakdowns.into_value().unwrap(), expected_breakdowns);
    }
}
