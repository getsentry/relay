//! Contains the breakdowns normalization code
//!
//! This module is responsible for generating breakdowns for events, such as span operation breakdowns.

use crate::protocol::{Breakdowns, BreakdownsConfig, EmitBreakdowns, Event, Measurements};
use crate::types::Annotated;

pub fn normalize_breakdowns(event: &mut Event, breakdowns_config: &BreakdownsConfig) {
    if breakdowns_config.is_empty() {
        return;
    }

    for (breakdown_name, breakdown_config) in breakdowns_config.iter() {
        // TODO: move this to deserialization in a follow-up.
        if !Breakdowns::is_valid_breakdown_name(&breakdown_name) {
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

        span_ops_breakdown.extend(breakdown.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        BreakdownConfig, EventType, Measurement, Span, SpanId, SpanOperationsConfig, SpanStatus,
        Timestamp, TraceId,
    };
    use crate::types::{Annotated, Object};
    use chrono::{TimeZone, Utc};
    use std::collections::HashMap;

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
            start_timestamp: Annotated<Timestamp>,
            end_timestamp: Annotated<Timestamp>,
            op_name: String,
        ) -> Annotated<Span> {
            Annotated::new(Span {
                timestamp: end_timestamp,
                start_timestamp,
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
                    // 1 hour in nanoseconds
                    value: Annotated::new(3_600_000.0),
                }),
            );

            span_ops_breakdown.insert(
                "ops.db".to_owned(),
                Annotated::new(Measurement {
                    // 2 hours in nanoseconds
                    value: Annotated::new(7_200_000.0),
                }),
            );

            span_ops_breakdown.insert(
                "total.time".to_owned(),
                Annotated::new(Measurement {
                    // 4 hours and 10 microseconds in nanoseconds
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
