use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::protocol::{Context, Event, Span, SpanId};
use crate::types::Annotated;
use crate::types::SpanAttribute;

use crate::store::normalize::breakdowns::TimeWindowSpan;

/// Merge a list of intervals into a list of non overlapping intervals.
/// Assumes that the input intervals are sorted by start time.
fn merge_non_overlapping_intervals(intervals: &mut [TimeWindowSpan]) -> Vec<TimeWindowSpan> {
    let mut non_overlapping_intervals = Vec::new();

    // Make sure that there is at least 1 interval present.
    if intervals.is_empty() {
        return non_overlapping_intervals;
    }

    let mut previous = intervals[0].clone();

    // The first interval is stored in `previous`, so make sure to skip it.
    for current in intervals.iter().skip(1) {
        if current.end < previous.end {
            // The current interval is completely contained within the
            // previous interval, nothing to be done here.
            continue;
        } else if current.start < previous.end {
            // The head of the current interval overlaps with the tail of
            // the previous interval, merge the two intervals into one.
            previous.end = current.end;
        } else {
            // The current interval does not intersect with the previous
            // interval, finished with the previous interval, and use the
            // current interval as the reference going forwards
            non_overlapping_intervals.push(previous);
            previous = current.clone();
        }
    }

    // Make sure to include the final interval.
    non_overlapping_intervals.push(previous);

    non_overlapping_intervals
}

/// Computes the exclusive time of the source interval after subtracting the
/// list of intervals.
/// Assumes that the input intervals are sorted by start time.
fn interval_exclusive_time(source: &TimeWindowSpan, intervals: &[TimeWindowSpan]) -> f64 {
    let mut exclusive_time = 0.0;

    let mut remaining = source.clone();

    for interval in intervals {
        if interval.end < remaining.start {
            // The interval is entirely to the left of the remaining interval,
            // so nothing to be done here.
            continue;
        } else if interval.start >= remaining.end {
            // The interval is entirely to the right of the remaining interval,
            // so nothing to be done here.
            //
            // Additionally, since intervals are sorted by start time, all
            // intervals afterwards can be skipped.
            break;
        } else {
            // The interval must intersect with the remaining interval in some way.

            if interval.start > remaining.start {
                // The interval begins within the remaining interval, there is a
                // portion to its left that should be added to the results.
                exclusive_time += TimeWindowSpan::new(remaining.start, interval.start).duration();
            }

            if interval.end < remaining.end {
                // The interval ends within the remaining interval, so the
                // tail of the interval interesects with the head of the remaining
                // interval.
                //
                // Subtract the intersection by shifting the start of the remaining
                // interval.
                remaining.start = interval.end;
            } else {
                // The interval ends to the right of the remaining interval, so
                // the interval intersects with the entirety of the remaining
                // interval. So zero out the interval.
                remaining.start = remaining.end;

                // There is nothing remaining to be checked.
                break;
            }
        }
    }

    // make sure to add the remaining interval
    exclusive_time + remaining.duration()
}

fn get_span_interval(span: &Span) -> Option<TimeWindowSpan> {
    let start = *span.start_timestamp.value()?;
    let end = *span.timestamp.value()?;
    Some(TimeWindowSpan::new(start, end))
}

pub fn normalize_spans(event: &mut Event, attributes: &BTreeSet<SpanAttribute>) {
    for attribute in attributes {
        match attribute {
            SpanAttribute::ExclusiveTime => compute_span_exclusive_time(event),
            SpanAttribute::Unknown => (), // ignored
        }
    }
}

fn get_transaction_interval(event: &Event) -> Option<TimeWindowSpan> {
    let start = *event.start_timestamp.value()?;
    let end = *event.timestamp.value()?;
    Some(TimeWindowSpan::new(start, end))
}

fn get_transaction_span_id(event: &Event) -> Option<SpanId> {
    let contexts = event.contexts.value()?;
    let context = contexts.get("trace")?.value()?;
    match &**context {
        Context::Trace(trace_context) => {
            let span_id = trace_context.span_id.value()?;
            Some(span_id.clone())
        }
        _ => None,
    }
}

fn compute_span_exclusive_time(event: &mut Event) {
    let mut span_map = HashMap::new();

    if let Some(spans) = event.spans.value() {
        for span in spans.iter() {
            let span = match span.value() {
                None => continue,
                Some(span) => span,
            };

            let parent_span_id = match span.parent_span_id.value() {
                None => continue,
                Some(parent_span_id) => parent_span_id.clone(),
            };

            let interval = match get_span_interval(span) {
                None => continue,
                Some(interval) => interval,
            };

            span_map
                .entry(parent_span_id)
                .or_insert_with(Vec::new)
                .push(interval)
        }
    }

    if let Some(span_interval) = get_transaction_interval(event) {
        if let Some(span_id) = get_transaction_span_id(event) {
            let child_intervals = match span_map.get_mut(&span_id) {
                Some(intervals) => {
                    // Make sure that the intervals are sorted by start time.
                    intervals.sort_unstable_by_key(|interval| interval.start);
                    merge_non_overlapping_intervals(intervals)
                }
                None => Vec::new(),
            };

            let exclusive_time = interval_exclusive_time(&span_interval, &child_intervals);
            event.exclusive_time = Annotated::new(exclusive_time);
        }
    }

    let spans = event.spans.value_mut().get_or_insert_with(|| Vec::new());

    for span in spans.iter_mut() {
        let mut span = match span.value_mut() {
            None => continue,
            Some(span) => span,
        };

        let span_id = match span.span_id.value() {
            None => continue,
            Some(span_id) => span_id,
        };

        let span_interval = match get_span_interval(span) {
            None => continue,
            Some(interval) => interval,
        };

        let child_intervals = match span_map.get_mut(span_id) {
            Some(intervals) => {
                // Make sure that the intervals are sorted by start time.
                intervals.sort_unstable_by_key(|interval| interval.start);
                merge_non_overlapping_intervals(intervals)
            }
            None => Vec::new(),
        };

        let exclusive_time = interval_exclusive_time(&span_interval, &child_intervals);
        span.exclusive_time = Annotated::new(exclusive_time);
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
            let transaction_exclusive_time = *event.exclusive_time.value().unwrap();
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

        assert!(event.exclusive_time.value().is_none());

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
