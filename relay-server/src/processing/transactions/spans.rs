use std::error::Error;

use crate::processing;
use crate::processing::utils::event::event_type;
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_event_normalization::eap::Ingress;
use relay_event_schema::protocol::{Event, Measurement, Measurements, Span, SpanV2, TraceContext};
use relay_metrics::MetricNamespace;
use relay_metrics::{FractionUnit, MetricUnit};
use relay_protocol::{Annotated, Empty};
use relay_sampling::DynamicSamplingContext;

pub fn extract_from_event(
    dsc: Option<&DynamicSamplingContext>,
    event: &Annotated<Event>,
    config: &Config,
    server_sample_rate: Option<f64>,
) -> Vec<Result<Annotated<SpanV2>, ()>> {
    // Only extract spans from transactions (not errors).
    if event_type(event) != Some(EventType::Transaction) {
        return Vec::new();
    };

    let client_sample_rate = dsc.and_then(|ctx| ctx.sample_rate);

    let Some(event) = event.value() else {
        return Vec::new();
    };

    let Some(transaction_span) = processing::transactions::extraction::extract_segment_span(
        event,
        config
            .aggregator_config_for(MetricNamespace::Spans)
            .max_tag_value_length,
        &[],
    ) else {
        return Vec::new();
    };

    let mut results = vec![];

    // Add child spans.
    if let Some(spans) = event.spans.value() {
        let origin = event
            .context::<TraceContext>()
            .map(|trace| trace.origin.clone())
            .unwrap_or_default();

        for span in spans {
            let Some(inner_span) = span.value() else {
                continue;
            };
            // HACK: clone the span to set the segment_id. This should happen
            // as part of normalization once standalone spans reach wider adoption.
            let mut new_span = inner_span.clone();
            new_span.is_segment = Annotated::new(false);
            new_span.is_remote = Annotated::new(false);
            new_span.received = transaction_span.received.clone();
            new_span.segment_id = transaction_span.segment_id.clone();
            new_span.platform = transaction_span.platform.clone();

            if new_span.origin.value().is_none() {
                new_span.origin = origin.clone();
            }

            // If a profile is associated with the transaction, also associate it with its
            // child spans.
            new_span.profile_id = transaction_span.profile_id.clone();

            results.push(make_span_item(
                new_span,
                client_sample_rate,
                server_sample_rate,
            ));
        }
    }

    results.push(make_span_item(
        transaction_span,
        client_sample_rate,
        server_sample_rate,
    ));

    results
}

fn make_span_item(
    mut span: Span,
    client_sample_rate: Option<f64>,
    server_sample_rate: Option<f64>,
) -> Result<Annotated<SpanV2>, ()> {
    add_sample_rate(
        &mut span.measurements,
        "client_sample_rate",
        client_sample_rate,
    );
    add_sample_rate(
        &mut span.measurements,
        "server_sample_rate",
        server_sample_rate,
    );

    let mut span = Annotated::new(span);

    validate(&mut span)
        .inspect_err(|e| {
            relay_log::debug!(
                error = e as &dyn Error,
                span = ?span,
                source = "event",
                "invalid span"
            );
        })
        .map_err(|_| ())?;

    // It's ok to enable `infer_name` here—the span has gone through the transaction pipeline,
    // so PII has been scrubbed.
    Ok(span.map_value(|span| {
        let mut span = relay_spans::span_v1_to_span_v2(span, true);
        span.attributes.get_or_insert_with(Default::default).insert(
            relay_conventions::attributes::SENTRY__RELAY__INGRESS,
            Ingress::Legacy.to_string(),
        );
        span
    }))
}

/// Any violation of the span schema.
#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    #[error("empty span")]
    EmptySpan,
    #[error("span is missing `trace_id`")]
    MissingTraceId,
    #[error("span is missing `span_id`")]
    MissingSpanId,
    #[error("span is missing `timestamp`")]
    MissingTimestamp,
    #[error("span is missing `start_timestamp`")]
    MissingStartTimestamp,
    #[error("span end must be after start")]
    EndBeforeStartTimestamp,
    #[error("span is missing `exclusive_time`")]
    MissingExclusiveTime,
}

/// We do not extract or ingest spans with missing fields if those fields are required on the Kafka topic.
pub fn validate(span: &mut Annotated<Span>) -> Result<(), ValidationError> {
    let inner = span
        .value_mut()
        .as_mut()
        .ok_or(ValidationError::EmptySpan)?;
    let Span {
        exclusive_time,
        tags,
        sentry_tags,
        start_timestamp,
        timestamp,
        span_id,
        trace_id,
        ..
    } = inner;

    trace_id.value().ok_or(ValidationError::MissingTraceId)?;
    span_id.value().ok_or(ValidationError::MissingSpanId)?;

    match (start_timestamp.value(), timestamp.value()) {
        (Some(start), Some(end)) if end < start => Err(ValidationError::EndBeforeStartTimestamp),
        (Some(_), Some(_)) => Ok(()),
        (_, None) => Err(ValidationError::MissingTimestamp),
        (None, _) => Err(ValidationError::MissingStartTimestamp),
    }?;

    exclusive_time
        .value()
        .ok_or(ValidationError::MissingExclusiveTime)?;

    if let Some(sentry_tags) = sentry_tags.value_mut() {
        if sentry_tags
            .group
            .value()
            .is_some_and(|s| s.len() > 16 || s.chars().any(|c| !c.is_ascii_hexdigit()))
        {
            sentry_tags.group.set_value(None);
        }

        if sentry_tags
            .status_code
            .value()
            .is_some_and(|s| s.parse::<u16>().is_err())
        {
            sentry_tags.status_code.set_value(None);
        }
    }
    if let Some(tags) = tags.value_mut() {
        tags.retain(|_, value| !value.value().is_empty())
    }

    Ok(())
}

fn add_sample_rate(measurements: &mut Annotated<Measurements>, name: &str, value: Option<f64>) {
    let value = match value {
        Some(value) if value > 0.0 => value,
        _ => return,
    };

    let measurement = Annotated::new(Measurement {
        value: Annotated::try_from(value),
        unit: MetricUnit::Fraction(FractionUnit::Ratio).into(),
    });

    measurements
        .get_or_insert_with(Measurements::default)
        .insert(name.to_owned(), measurement);
}
