use opentelemetry_proto::tonic::trace::v1::TracesData;
use relay_config::ByteSize;
use relay_event_schema::protocol::SpanV2;
use relay_otel::descriptors::Decodable;
use relay_spans::calculate_size;

use crate::envelope::WithHeader;
use crate::integrations::OtelFormat;
use crate::processing::spans::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL traces into the [`SpanV2`] format.
pub fn expand(
    format: OtelFormat,
    payload: &[u8],
    max_ops: usize,
    max_spans_bytes: ByteSize,
) -> Result<Vec<WithHeader<SpanV2>>> {
    let mut results = vec![];
    let traces = parse_traces_data(format, payload, max_ops)?;
    let mut total_spans_bytes = 0;

    for resource_spans in traces.resource_spans {
        let resource = resource_spans.resource.as_ref();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in scope_spans.spans {
                let span = relay_spans::otel_to_sentry_span_v2(span, resource, scope);
                total_spans_bytes += calculate_size(&span) as usize;

                if total_spans_bytes > max_spans_bytes.as_bytes() {
                    return Err(Error::Invalid(DiscardReason::RequestTooLarge));
                }

                results.push(WithHeader::new(span.into()));
            }
        }
    }

    Ok(results)
}

fn parse_traces_data(
    format: OtelFormat,
    payload: &[u8],
    max_ops: usize,
) -> Result<TracesData, Error> {
    match format {
        OtelFormat::Json => {
            let mut de = serde_json::Deserializer::from_reader(payload);
            let result = relay_serialization::serde::deserialize(&mut de, max_ops).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse traces data as JSON"
                );

                match e {
                    relay_serialization::serde::Error::LimitExceeded(_) => {
                        Error::Invalid(DiscardReason::RequestTooLarge)
                    }
                    relay_serialization::serde::Error::Serde(_) => {
                        Error::Invalid(DiscardReason::InvalidJson)
                    }
                }
            });
            de.end()
                .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;
            result
        }
        OtelFormat::Protobuf => TracesData::decode_bounded(payload, max_ops).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            match e {
                relay_serialization::prost::Error::ScanError(_) => {
                    Error::Invalid(DiscardReason::InvalidProtobuf)
                }
                relay_serialization::prost::Error::LimitExceeded => {
                    Error::Invalid(DiscardReason::RequestTooLarge)
                }
                relay_serialization::prost::Error::Decode(_) => {
                    Error::Invalid(DiscardReason::InvalidProtobuf)
                }
            }
        }),
    }
}
