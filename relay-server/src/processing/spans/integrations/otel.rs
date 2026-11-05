use opentelemetry_proto::tonic::trace::v1::TracesData;
use relay_event_schema::protocol::SpanV2;
use relay_otel::descriptors::Decodable;

use crate::integrations::OtelFormat;
use crate::processing::spans::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL traces into the [`SpanV2`] format.
pub fn expand<F>(
    format: OtelFormat,
    payload: &[u8],
    mut produce: F,
    max_ops: usize,
    max_spans: usize,
) -> Result<()>
where
    F: FnMut(SpanV2),
{
    let traces = parse_traces_data(format, payload, max_ops)?;
    let mut total_spans = 0;

    for resource_spans in traces.resource_spans {
        let resource = resource_spans.resource.as_ref();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in scope_spans.spans {
                if total_spans >= max_spans {
                    return Err(Error::Invalid(DiscardReason::RequestTooLarge));
                }

                let span = relay_spans::otel_to_sentry_span_v2(span, resource, scope);
                produce(span);

                total_spans += 1;
            }
        }
    }

    Ok(())
}

fn parse_traces_data(
    format: OtelFormat,
    payload: &[u8],
    max_ops: usize,
) -> Result<TracesData, Error> {
    match format {
        OtelFormat::Json => {
            let mut de = serde_json::Deserializer::from_reader(payload);
            relay_serialization::serde::deserialize(&mut de, max_ops).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse traces data as JSON"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        OtelFormat::Protobuf => TracesData::decode_bounded(payload, max_ops).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
