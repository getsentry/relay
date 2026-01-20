use std::error::Error;

use crate::envelope::{ContentType, Item, ItemType};
use crate::processing;
use crate::processing::utils::event::{EventMetricsExtracted, SpansExtracted, event_type};
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_event_schema::protocol::{Event, Measurement, Measurements, Span};
use relay_metrics::MetricNamespace;
use relay_metrics::{FractionUnit, MetricUnit};
use relay_protocol::{Annotated, Empty};
use relay_sampling::DynamicSamplingContext;

#[allow(clippy::too_many_arguments)]
pub fn extract_from_event(
    dsc: Option<&DynamicSamplingContext>,
    event: &Annotated<Event>,
    config: &Config,
    server_sample_rate: Option<f64>,
    event_metrics_extracted: EventMetricsExtracted,
    spans_extracted: SpansExtracted,
) -> Option<Vec<Result<Item, ()>>> {
    // Only extract spans from transactions (not errors).
    if event_type(event) != Some(EventType::Transaction) {
        return None;
    };

    if spans_extracted.0 {
        return None;
    }

    let client_sample_rate = dsc.and_then(|ctx| ctx.sample_rate);

    let event = event.value()?;

    let transaction_span = processing::transactions::extraction::extract_segment_span(
        event,
        config
            .aggregator_config_for(MetricNamespace::Spans)
            .max_tag_value_length,
        &[],
    )?;

    let mut results = vec![];

    // Add child spans.
    if let Some(spans) = event.spans.value() {
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

            // If a profile is associated with the transaction, also associate it with its
            // child spans.
            new_span.profile_id = transaction_span.profile_id.clone();

            results.push(make_span_item(
                new_span,
                config,
                client_sample_rate,
                server_sample_rate,
                event_metrics_extracted.0,
            ));
        }
    }

    results.push(make_span_item(
        transaction_span,
        config,
        client_sample_rate,
        server_sample_rate,
        event_metrics_extracted.0,
    ));

    Some(results)
}

fn make_span_item(
    mut span: Span,
    config: &Config,
    client_sample_rate: Option<f64>,
    server_sample_rate: Option<f64>,
    metrics_extracted: bool,
) -> Result<Item, ()> {
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

    let mut item = create_span_item(span, config)?;

    // If metrics extraction happened for the event, it also happened for its spans:
    item.set_metrics_extracted(metrics_extracted);

    relay_log::trace!("Adding span to envelope");
    Ok(item)
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

/// Serializes the given span into an envelope item.
///
/// In processing relays, creates a Span V2 so it can be published via kafka.
pub fn create_span_item(span: Annotated<Span>, config: &Config) -> Result<Item, ()> {
    let mut new_item = Item::new(ItemType::Span);
    if cfg!(feature = "processing") && config.processing_enabled() {
        let span_v2 = span.map_value(relay_spans::span_v1_to_span_v2);
        let payload = match span_v2.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::error!("failed to serialize span V2: {}", err);
                return Err(());
            }
        };
        if let Some(trace_id) = span_v2.value().and_then(|s| s.trace_id.value()) {
            new_item.set_routing_hint(*trace_id.as_ref());
        }

        new_item.set_payload(ContentType::Json, payload);
    } else {
        let payload = match span.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::error!("failed to serialize span: {}", err);
                return Err(());
            }
        };
        new_item.set_payload(ContentType::Json, payload);
    }

    Ok(new_item)
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
