use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, WithHeader};
use crate::integrations::{Integration, LogsIntegration};
use crate::managed::RecordKeeper;
use crate::processing::logs::{Result, Settings};

mod nel;
mod otel;
mod vercel;

/// Expands a log [`Integration`] into a list of logs.
///
/// The function expects *only* log item integrations.
pub fn expand(
    item: Item,
    records: &mut RecordKeeper<'_>,
    headers: &EnvelopeHeaders,
    max_expanded_log_count: usize,
) -> Option<(Settings, ContainerItems<OurLog>)> {
    let integration = match item.integration() {
        Some(Integration::Logs(integration)) => integration,
        integration => {
            records.internal_error(InvalidIntegration(integration), item);
            return None;
        }
    };

    let payload = item.payload();

    let log_stream: Result<Box<dyn Iterator<Item = OurLog>>> = match integration {
        LogsIntegration::Nel => nel::expand2(&payload, headers),
        LogsIntegration::OtelV1 { format } => otel::expand2(format, &payload),
        LogsIntegration::VercelDrainLog { format } => vercel::expand2(format, &payload),
    };

    let settings = match integration {
        LogsIntegration::Nel => Settings {
            infer_user_agent: true,
            infer_ip: false,
        },
        LogsIntegration::OtelV1 { format: _ } => Settings::default(),
        LogsIntegration::VercelDrainLog { format: _ } => Settings::default(),
    };

    let (log_stream, settings) = match log_stream {
        Ok(log_stream) => (log_stream, settings),
        Err(err) => {
            let _ = records.reject_err(err, &item);
            return None;
        }
    };

    let logs = log_stream
        .take(max_expanded_log_count)
        .map(|log| {
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.modify_by(DataCategory::LogItem, 1);
            records.modify_by(DataCategory::LogByte, byte_size as isize);

            WithHeader {
                header: Some(OurLogHeader {
                    byte_size: Some(byte_size),
                    other: Default::default(),
                }),
                value: log.into(),
            }
        })
        .collect();

    // Undo all the base item quantities, as they will be completely taken over by the parsed
    // contents, which contains an arbitrary amount of items (even 0).
    for (category, quantity) in item.quantities() {
        records.modify_by(category, -(quantity as isize));
    }

    Some((settings, logs))
}

#[derive(Debug, thiserror::Error)]
#[error("Expected a logs integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);
