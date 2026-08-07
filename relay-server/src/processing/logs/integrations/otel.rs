use std::ops::Deref;

use opentelemetry_proto::tonic::logs::v1::LogsData;
use prost::Message as _;
use relay_event_schema::protocol::OurLog;

use crate::integrations::OtelFormat;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;

/// Expands OTeL logs into the [`OurLog`] format.
pub fn expand2(format: OtelFormat, payload: &[u8]) -> Result<Box<dyn Iterator<Item = OurLog>>> {
    let logs: LogsData = parse_logs_data(format, payload).unwrap();

    Ok(Box::new(logs.resource_logs.into_iter().flat_map(
        |resource_logs| {
            let resource = std::cell::RefCell::new(resource_logs.resource);
            resource_logs
                .scope_logs
                .into_iter()
                .flat_map(move |scope_logs| {
                    let scope = scope_logs.scope;
                    let r = resource.clone();
                    scope_logs.log_records.into_iter().map(move |log_record| {
                        let b = r.borrow();
                        relay_ourlogs::otel_to_sentry_log(log_record, b.deref(), &scope)
                    })
                })
        },
    )))
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
