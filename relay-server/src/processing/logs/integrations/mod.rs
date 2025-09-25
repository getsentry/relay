use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_quotas::DataCategory;
use zstd::zstd_safe::WriteBuf;

use crate::envelope::{ContainerItems, Item, WithHeader};
use crate::integrations::{Integration, LogsIntegration};
use crate::managed::RecordKeeper;

mod otel;

/// Expands a list of [`Integration`] items into `result`.
///
/// The function expects *only* log item integrations.
pub fn expand_into(
    result: &mut ContainerItems<OurLog>,
    records: &mut RecordKeeper<'_>,
    items: Vec<Item>,
) {
    for item in items {
        let integration = match item.integration() {
            Some(Integration::Logs(integration)) => integration,
            integration => {
                records.internal_error(InvalidIntegration(integration), item);
                continue;
            }
        };

        let produce = |log: OurLog| {
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.modify_by(DataCategory::LogItem, 1);
            records.modify_by(DataCategory::LogByte, byte_size as isize);

            result.push(WithHeader {
                header: Some(OurLogHeader {
                    byte_size: Some(byte_size),
                    other: Default::default(),
                }),
                value: log.into(),
            });
        };

        let payload = item.payload();
        let payload = payload.as_slice();

        let result = match integration {
            LogsIntegration::OtelV1 { format } => otel::expand(format, payload, produce),
        };

        match result {
            Err(err) => drop(records.reject_err(err, item)),
            Ok(()) => {
                // Undo all the base item quantities, as they will be completely taken over by the parsed
                // contents, which contains an arbitrary amount of items (even 0).
                for (category, quantity) in item.quantities() {
                    records.modify_by(category, -(quantity as isize));
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Expected a logs integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);
