use std::os::macos::raw::stat;

use relay_event_schema::protocol::{
    Attribute, Attributes, Span as SpanV1, SpanData, SpanKind as SpanV1Kind, SpanLink,
    SpanStatus as SpanV1Status, SpanV2, SpanV2Kind, SpanV2Link, SpanV2Status,
};
use relay_protocol::{Annotated, FromValue, IntoValue, Value};

pub fn span_v1_to_span_v2(span_v1: SpanV1) -> SpanV2 {
    let SpanV1 {
        timestamp,
        start_timestamp,
        exclusive_time,
        op,
        span_id,
        parent_span_id,
        trace_id,
        segment_id,
        is_segment,
        is_remote,
        status,
        description,
        tags,
        origin,
        profile_id,
        data,
        links,
        sentry_tags,
        received,
        measurements,
        platform,
        was_transaction,
        kind,
        _performance_issues_spans,
        other,
    } = span_v1;

    let mut attributes = attributes_from_data(data);
    // TODO: write other attributes.

    SpanV2 {
        trace_id,
        parent_span_id,
        span_id,
        name: attributes
            .value()
            .and_then(|attrs| attrs.get_value("sentry.name"))
            .and_then(|v| Some(v.as_str()?.to_owned()))
            .into(),
        status: Annotated::map_value(status, span_v1_status_to_span_v2_status),
        is_remote,
        kind: Annotated::map_value(kind, span_v1_kind_to_span_v2_kind),
        start_timestamp,
        end_timestamp: timestamp,
        links: links.map_value(span_v1_links_to_span_v2_links),
        attributes,
        other,
    }
}

fn span_v1_status_to_span_v2_status(status: SpanV1Status) -> SpanV2Status {
    match status {
        SpanV1Status::Ok => SpanV2Status::Ok,
        _ => SpanV2Status::Error,
    }
}

fn span_v1_kind_to_span_v2_kind(kind: SpanV1Kind) -> SpanV2Kind {
    match kind {
        SpanV1Kind::Internal => SpanV2Kind::Internal,
        SpanV1Kind::Server => SpanV2Kind::Server,
        SpanV1Kind::Client => SpanV2Kind::Client,
        SpanV1Kind::Producer => SpanV2Kind::Producer,
        SpanV1Kind::Consumer => SpanV2Kind::Consumer,
        // TODO: implement catchall type so outdated customer relays can still forward the field.
    }
}

fn span_v1_links_to_span_v2_links(links: Vec<Annotated<SpanLink>>) -> Vec<Annotated<SpanV2Link>> {
    links
        .into_iter()
        .map(|link| {
            link.map_value(
                |SpanLink {
                     trace_id,
                     span_id,
                     sampled,
                     attributes,
                     other,
                 }| SpanV2Link {
                    trace_id,
                    span_id,
                    sampled,
                    attributes: attributes.map_value(|attrs| {
                        Attributes::from_iter(
                            attrs
                                .into_iter()
                                .map(|(key, value)| (key, Attribute::from_value(value))),
                        )
                    }),
                    other,
                },
            )
        })
        .collect()
}

fn attributes_from_data(data: Annotated<SpanData>) -> Annotated<Attributes> {
    let Some(data) = data.into_value() else {
        return Annotated::empty();
    };
    let Value::Object(data) = data.into_value() else {
        return Annotated::empty();
    };

    Annotated::new(Attributes::from_iter(
        data.into_iter()
            .map(|(key, value)| (key, Attribute::from_value(value))),
    ))
}
