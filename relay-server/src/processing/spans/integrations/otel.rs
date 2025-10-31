use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message as _;
use relay_event_schema::protocol::SpanV2;

use crate::integrations::OtelFormat;
use crate::processing::spans::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL traces into the [`SpanV2`] format.
pub fn expand<F>(format: OtelFormat, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(SpanV2),
{
    let traces = parse_traces_data(format, payload)?;

    for resource_spans in traces.resource_spans {
        let resource = resource_spans.resource.as_ref();
        for scope_spans in resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            for span in scope_spans.spans {
                let span = relay_spans::otel_to_sentry_span_v2(span, resource, scope);
                produce(span);
            }
        }
    }

    Ok(())
}

fn parse_traces_data(format: OtelFormat, payload: &[u8]) -> Result<TracesData, Error> {
    match format {
        OtelFormat::Json => serde_json::from_slice(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as JSON"
            );
            Error::Invalid(DiscardReason::InvalidJson)
        }),
        OtelFormat::Protobuf => TracesData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse traces data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
