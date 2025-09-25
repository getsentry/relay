use opentelemetry_proto::tonic::logs::v1::LogsData;
use prost::Message as _;
use relay_event_schema::protocol::OurLog;

use crate::integrations::OtelFormat;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL logs into the [`OurLog`] format.
pub fn expand<F>(format: OtelFormat, payload: &[u8], mut produce: F) -> Result<()>
where
    F: FnMut(OurLog),
{
    let logs = parse_logs_data(format, payload)?;

    for resource_logs in logs.resource_logs {
        let resource = resource_logs.resource.as_ref();
        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for log_record in scope_logs.log_records {
                let log = relay_ourlogs::otel_to_sentry_log(log_record, resource, scope);
                produce(log);
            }
        }
    }

    Ok(())
}

fn parse_logs_data(format: OtelFormat, payload: &[u8]) -> Result<LogsData, Error> {
    match format {
        OtelFormat::Json => serde_json::from_slice(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as JSON"
            );
            Error::Invalid(DiscardReason::InvalidJson)
        }),
        OtelFormat::Protobuf => LogsData::decode(payload).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
    }
}
