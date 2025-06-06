//! Span attribute materialization.

use std::collections::HashMap;
use std::time::Duration;

use relay_event_schema::protocol::{Contexts, Event, Span, SpanId, TraceContext};
use relay_protocol::Annotated;

use crate::normalize::breakdowns::TimeWindowSpan;

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
        if let Ok(start_offset) = (interval.start - parent.start).to_std() {
            exclusive_time += start_offset;
        }

        parent.start = interval.end.clamp(parent.start, parent.end);
    }

    // Add the remaining duration after the last interval ended
    exclusive_time + parent.duration()
}

fn set_event_exclusive_time(
    event_interval: TimeWindowSpan,
    contexts: &mut Contexts,
    span_map: &HashMap<SpanId, Vec<TimeWindowSpan>>,
) {
    let Some(trace_context) = contexts.get_mut::<TraceContext>() else {
        return;
    };

    if trace_context.exclusive_time.value().is_some() {
        // Exclusive time already set, respect it.
        return;
    }

    let Some(span_id) = trace_context.span_id.value() else {
        return;
    };

    let child_intervals = span_map
        .get(span_id)
        .map(|vec| vec.as_slice())
        .unwrap_or_default();

    let exclusive_time = interval_exclusive_time(event_interval, child_intervals);
    trace_context.exclusive_time =
        Annotated::new(relay_common::time::duration_to_millis(exclusive_time));
}

fn set_span_exclusive_time(
    span: &mut Annotated<Span>,
    span_map: &HashMap<SpanId, Vec<TimeWindowSpan>>,
) {
    let span = match span.value_mut() {
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
    span.exclusive_time = Annotated::new(relay_common::time::duration_to_millis(exclusive_time));
}

/// Computes the exclusive time for all spans in the event.
pub fn compute_span_exclusive_time(event: &mut Event) {
    let contexts = match event.contexts.value_mut() {
        Some(contexts) => contexts,
        _ => return,
    };

    let event_interval = match (event.start_timestamp.value(), event.timestamp.value()) {
        (Some(start), Some(end)) => TimeWindowSpan::new(*start, *end),
        _ => return,
    };

    let spans = event.spans.value_mut().get_or_insert_with(Vec::new);

    let mut span_map = HashMap::new();
    for span in spans.iter() {
        let span = match span.value() {
            None => continue,
            Some(span) => span,
        };

        let Some(&parent_span_id) = span.parent_span_id.value() else {
            continue;
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
    use chrono::{TimeZone, Utc};
    use relay_event_schema::protocol::{EventType, Timestamp};
    use similar_asserts::assert_eq;

    use super::*;

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
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
                    span_id: Annotated::new(span_id.parse().unwrap()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
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
            trace_id: Annotated::new("4c79f60c11214eb38604f4ae0781bfb2".parse().unwrap()),
            span_id: Annotated::new(span_id.parse().unwrap()),
            parent_span_id: Annotated::new(parent_span_id.parse().unwrap()),
            ..Default::default()
        })
    }

    fn extract_exclusive_time(span: &Span) -> (&SpanId, f64) {
        (
            span.span_id.value().unwrap(),
            *span.exclusive_time.value().unwrap(),
        )
    }

    fn extract_span_exclusive_times(event: &Event) -> HashMap<&SpanId, f64> {
        let spans = event.spans.value().unwrap();
        let mut exclusive_times: HashMap<_, _> = spans
            .iter()
            .filter_map(Annotated::value)
            .map(extract_exclusive_time)
            .collect();

        let trace_context = event.context::<TraceContext>().unwrap();
        let transaction_span_id = trace_context.span_id.value().unwrap();
        let transaction_exclusive_time = *trace_context.exclusive_time.value().unwrap();
        exclusive_times.insert(transaction_span_id, transaction_exclusive_time);

        exclusive_times
    }

    #[test]
    fn test_childless_spans() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455604, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455603, 500_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455603, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455604, 877_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "aaaaaaaaaaaaaaaa",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 1123.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 3000.0),
                (&"cccccccccccccccc".parse().unwrap(), 2500.0),
                (&"dddddddddddddddd".parse().unwrap(), 1877.0)
            ]),
        );
    }

    #[test]
    fn test_nested_spans() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 200_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 800_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 400_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 600_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "cccccccccccccccc",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 400.0),
                (&"cccccccccccccccc".parse().unwrap(), 400.0),
                (&"dddddddddddddddd".parse().unwrap(), 200.0),
            ])
        );
    }

    #[test]
    fn test_overlapping_child_spans() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 200_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 600_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 400_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 800_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 400.0),
                (&"cccccccccccccccc".parse().unwrap(), 400.0),
                (&"dddddddddddddddd".parse().unwrap(), 400.0),
            ])
        );
    }

    #[test]
    fn test_child_spans_dont_intersect_parent() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455600, 400_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455600, 800_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455602, 200_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 600_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 1000.0),
                (&"cccccccccccccccc".parse().unwrap(), 400.0),
                (&"dddddddddddddddd".parse().unwrap(), 400.0),
            ])
        );
    }

    #[test]
    fn test_child_spans_extend_beyond_parent() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455600, 800_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 400_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 600_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 200_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 200.0),
                (&"cccccccccccccccc".parse().unwrap(), 600.0),
                (&"dddddddddddddddd".parse().unwrap(), 600.0),
            ])
        );
    }

    #[test]
    fn test_child_spans_consumes_all_of_parent() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455600, 800_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 600_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 400_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 200_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "bbbbbbbbbbbbbbbb",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 0.0),
                (&"cccccccccccccccc".parse().unwrap(), 800.0),
                (&"dddddddddddddddd".parse().unwrap(), 800.0),
            ])
        );
    }

    #[test]
    fn test_only_immediate_child_spans_affect_calculation() {
        let mut event = make_event(
            Utc.timestamp_opt(1609455600, 0).unwrap().into(),
            Utc.timestamp_opt(1609455605, 0).unwrap().into(),
            "aaaaaaaaaaaaaaaa",
            vec![
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 0).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 0).unwrap().into(),
                    "bbbbbbbbbbbbbbbb",
                    "aaaaaaaaaaaaaaaa",
                ),
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 600_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455602, 200_000_000).unwrap().into(),
                    "cccccccccccccccc",
                    "bbbbbbbbbbbbbbbb",
                ),
                // this should only affect the calculation for it's immediate parent
                // which is `cccccccccccccccc` and not `bbbbbbbbbbbbbbbb`
                make_span(
                    "db",
                    "SELECT * FROM table;",
                    Utc.timestamp_opt(1609455601, 400_000_000).unwrap().into(),
                    Utc.timestamp_opt(1609455601, 800_000_000).unwrap().into(),
                    "dddddddddddddddd",
                    "cccccccccccccccc",
                ),
            ],
        );

        compute_span_exclusive_time(&mut event);

        assert_eq!(
            extract_span_exclusive_times(&event),
            HashMap::from_iter([
                (&"aaaaaaaaaaaaaaaa".parse().unwrap(), 4000.0),
                (&"bbbbbbbbbbbbbbbb".parse().unwrap(), 600.0),
                (&"cccccccccccccccc".parse().unwrap(), 400.0),
                (&"dddddddddddddddd".parse().unwrap(), 400.0),
            ])
        );
    }
}
