use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use crate::protocol::{Context, ContextInner, Contexts, Event, Span, SpanId};
use crate::store::normalize::breakdowns::TimeWindowSpan;
use crate::types::{Annotated, SpanAttribute};

/// Computes the exclusive time of the source interval after subtracting the
/// list of intervals.
///
/// Assumes that the input intervals are sorted by start time.
fn interval_exclusive_time(mut parent: TimeWindowSpan, intervals: &[TimeWindowSpan]) -> Duration {
    let mut exclusive_time = Duration::new(0, 0);

    for interval in intervals {
        // Exit early to avoid adding zeros
        if interval.start >= parent.end {
            break;
        }

        // Add time in the parent before the start of the current interval to the exclusive time
        let start = interval.start.min(parent.end);
        if let Ok(start_offset) = (start - parent.start).to_std() {
            exclusive_time += start_offset;
        }

        parent.start = interval.end.clamp(parent.start, parent.end);
    }

    // Add the remaining duration after the last interval ended
    exclusive_time + parent.duration()
}

pub fn normalize_spans(event: &mut Event, attributes: &BTreeSet<SpanAttribute>) {
    for attribute in attributes {
        match attribute {
            SpanAttribute::ExclusiveTime => compute_span_exclusive_time(event),
            SpanAttribute::Unknown => (), // ignored
        }
    }
}

fn set_event_exclusive_time(
    event_interval: TimeWindowSpan,
    contexts: &mut Contexts,
    span_map: &HashMap<SpanId, Vec<TimeWindowSpan>>,
) {
    let trace_context = match contexts.get_mut("trace").map(Annotated::value_mut) {
        Some(Some(ContextInner(Context::Trace(trace_context)))) => trace_context,
        _ => return,
    };

    let span_id = match trace_context.span_id.value() {
        Some(span_id) => span_id,
        _ => return,
    };

    let child_intervals = span_map
        .get(span_id)
        .map(|vec| vec.as_slice())
        .unwrap_or_default();

    let exclusive_time = interval_exclusive_time(event_interval, child_intervals);
    trace_context.exclusive_time = Annotated::new(relay_common::duration_to_millis(exclusive_time));
}

fn set_span_exclusive_time(
    span: &mut Annotated<Span>,
    span_map: &HashMap<SpanId, Vec<TimeWindowSpan>>,
) {
    let mut span = match span.value_mut() {
        None => return,
        Some(span) => span,
    };

    let span_id = match span.span_id.value() {
        None => return,
        Some(span_id) => span_id,
    };

    let span_interval = match (span.start_timestamp.value(), span.timestamp.value()) {
        (Some(start), Some(end)) => TimeWindowSpan::new(*start, *end),
        _ => return,
    };

    let child_intervals = span_map
        .get(span_id)
        .map(|vec| vec.as_slice())
        .unwrap_or_default();

    let exclusive_time = interval_exclusive_time(span_interval, child_intervals);
    span.exclusive_time = Annotated::new(relay_common::duration_to_millis(exclusive_time));
}

fn compute_span_exclusive_time(event: &mut Event) {
    let contexts = match event.contexts.value_mut() {
        Some(contexts) => contexts,
        _ => return,
    };

    let event_interval = match (event.start_timestamp.value(), event.timestamp.value()) {
        (Some(start), Some(end)) => TimeWindowSpan::new(*start, *end),
        _ => return,
    };

    let spans = event.spans.value_mut().get_or_insert_with(|| Vec::new());

    let mut span_map = HashMap::new();
    for span in spans.iter() {
        let span = match span.value() {
            None => continue,
            Some(span) => span,
        };

        let parent_span_id = match span.parent_span_id.value() {
            None => continue,
            Some(parent_span_id) => parent_span_id.clone(),
        };

        let interval = match (span.start_timestamp.value(), span.timestamp.value()) {
            (Some(start), Some(end)) => TimeWindowSpan::new(*start, *end),
            _ => continue,
        };

        span_map
            .entry(parent_span_id)
            .or_insert_with(Vec::new)
            .push(interval)
    }

    // Sort intervals to fulfill precondition of `interval_exclusive_time`
    for intervals in span_map.values_mut() {
        intervals.sort_unstable_by_key(|interval| interval.start);
    }

    set_event_exclusive_time(event_interval, contexts, &span_map);

    for span in spans.iter_mut() {
        set_span_exclusive_time(span, &span_map);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        Context, ContextInner, Contexts, Event, EventType, Span, SpanId, Timestamp, TraceContext,
        TraceId,
    };
    use crate::types::Object;
    use chrono::{TimeZone, Utc};

    fn make_event(
        start: Timestamp,
        end: Timestamp,
        span_id: &str,
        spans: Vec<Annotated<Span>>,
    ) -> Event {
        Event {
            ty: EventType::Transaction.into(),
            start_timestamp: Annotated::new(start),
            timestamp: Annotated::new(end),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert(
                    "trace".to_owned(),
                    Annotated::new(ContextInner(Context::Trace(Box::new(TraceContext {
                        trace_id: Annotated::new(TraceId(
                            "4c79f60c11214eb38604f4ae0781bfb2".into(),
                        )),
                        span_id: Annotated::new(SpanId(span_id.into())),
                        ..Default::default()
                    })))),
                );
                contexts
            })),
            spans: spans.into(),
            ..Default::default()
        }
    }

    fn make_span(
        op: &str,
        description: &str,
        start: Timestamp,
        end: Timestamp,
        span_id: &str,
        parent_span_id: &str,
    ) -> Annotated<Span> {
        Annotated::new(Span {
            op: Annotated::new(op.into()),
            description: Annotated::new(description.into()),
            start_timestamp: Annotated::new(start),
            timestamp: Annotated::new(end),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId(span_id.into())),
            parent_span_id: Annotated::new(SpanId(parent_span_id.into())),
            ..Default::default()
        })
    }

    fn extract_exclusive_time(span: Span) -> (SpanId, f64) {
        (
            span.span_id.into_value().unwrap(),
            span.exclusive_time.into_value().unwrap(),
        )
    }

    fn extract_span_exclusive_times(event: Event) -> HashMap<SpanId, f64> {
        let mut exclusive_times: HashMap<SpanId, f64> = event
            .clone()
            .spans
            .into_value()
            .unwrap()
            .into_iter()
            .map(|span| extract_exclusive_time(span.into_value().unwrap()))
            .collect();

        let context = event
            .contexts
            .into_value()
            .unwrap()
            .get("trace")
            .unwrap()
            .clone();

        if let Context::Trace(trace_context) = &*context.into_value().unwrap() {
            let transaction_span_id: SpanId = trace_context.span_id.value().unwrap().clone();
            let transaction_exclusive_time = *trace_context.exclusive_time.value().unwrap();
            exclusive_times.insert(transaction_span_id, transaction_exclusive_time);
        }

        exclusive_times
    }

    #[test]
    fn test_skip_exclusive_time() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 4, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 3, 500_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 3, 0).into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 4, 877_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "aaaaaaaaaaaaaaaa",
                ),
            ],
        );

        // do not insert `exclusive-time`
        let config = BTreeSet::new();

        normalize_spans(&mut event, &config);

        let context = event
            .contexts
            .into_value()
            .unwrap()
            .get("trace")
            .unwrap()
            .clone();

        if let Context::Trace(trace_context) = &*context.into_value().unwrap() {
            assert!(trace_context.exclusive_time.value().is_none());
        }

        let has_exclusive_times: Vec<bool> = event
            .spans
            .into_value()
            .unwrap()
            .into_iter()
            .map(|span| span.into_value().unwrap().exclusive_time.value().is_none())
            .collect();

        assert_eq!(has_exclusive_times, vec![true, true, true]);
    }

    #[test]
    fn test_childless_spans() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 4, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 3, 500_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 3, 0).into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 4, 877_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "aaaaaaaaaaaaaaaa",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 1123.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 3000.0),
                (SpanId("cccccccccccccccc".to_string()), 2500.0),
                (SpanId("dddddddddddddddd".to_string()), 1877.0)
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_nested_spans() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 200_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 800_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 400_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 600_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "cccccccccccccccc",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 400.0),
                (SpanId("cccccccccccccccc".to_string()), 400.0),
                (SpanId("dddddddddddddddd".to_string()), 200.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_overlapping_child_spans() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 200_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 600_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 400_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 800_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 400.0),
                (SpanId("cccccccccccccccc".to_string()), 400.0),
                (SpanId("dddddddddddddddd".to_string()), 400.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_child_spans_dont_intersect_parent() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 0, 400_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 0, 800_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 2, 200_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 2, 600_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 1000.0),
                (SpanId("cccccccccccccccc".to_string()), 400.0),
                (SpanId("dddddddddddddddd".to_string()), 400.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_child_spans_extend_beyond_parent() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 0, 800_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 400_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 600_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 2, 200_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 200.0),
                (SpanId("cccccccccccccccc".to_string()), 600.0),
                (SpanId("dddddddddddddddd".to_string()), 600.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_child_spans_consumes_all_of_parent() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 0, 800_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 600_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 400_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 2, 200_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 0.0),
                (SpanId("cccccccccccccccc".to_string()), 800.0),
                (SpanId("dddddddddddddddd".to_string()), 800.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }

    #[test]
    fn test_only_immediate_child_spans_affect_calculation() {
        let mut event = make_event(
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 0, 0).into(),
            Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 5, 0).into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 1, 0).into(),
                    Utc.ymd(2021, 1, 1).and_hms_nano(0, 0, 2, 0).into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 600_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 2, 200_000_000)
                        .into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                // this should only affect the calculation for it's immediate parent
                // which is `cccccccccccccccc` and not `bbbbbbbbbbbbbbbb`
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 400_000_000)
                        .into(),
                    Utc.ymd(2021, 1, 1)
                        .and_hms_nano(0, 0, 1, 800_000_000)
                        .into(),
                    "dddddddddddddddd",
                    "cccccccccccccccc",
                ),
            ],
        );

        let mut config = BTreeSet::new();
        config.insert(SpanAttribute::ExclusiveTime);

        normalize_spans(&mut event, &config);

        let exclusive_times = extract_span_exclusive_times(event);

        assert_eq!(
            exclusive_times,
            vec![
                (SpanId("aaaaaaaaaaaaaaaa".to_string()), 4000.0),
                (SpanId("bbbbbbbbbbbbbbbb".to_string()), 600.0),
                (SpanId("cccccccccccccccc".to_string()), 400.0),
                (SpanId("dddddddddddddddd".to_string()), 400.0),
            ]
            .iter()
            .cloned()
            .collect()
        );
    }
}
