use relay_config::ByteSize;
use relay_event_schema::protocol::SpanV2;

use crate::envelope::{ContainerItems, Item};
use crate::integrations::{Integration, SpansIntegration};
use crate::managed::RecordKeeper;
use crate::processing::spans::Settings;

mod otel;

/// Expands a list of [`Integration`] items.
///
/// The function expects *only* span item integrations.
pub fn expand(
    records: &mut RecordKeeper<'_>,
    items: &[Item],
    max_ops: usize,
    max_spans_bytes: ByteSize,
) -> (Settings, ContainerItems<SpanV2>) {
    let mut results = vec![];
    for item in items {
        let integration = match item.integration() {
            Some(Integration::Spans(integration)) => integration,
            integration => {
                records.internal_error(InvalidIntegration(integration), item);
                continue;
            }
        };

        let payload = item.payload();

        let result = match integration {
            SpansIntegration::OtelV1 { format } => {
                otel::expand(format, &payload, max_ops, max_spans_bytes)
            }
        };

        match result {
            Err(err) => {
                drop(records.reject_err(err, item));
            }
            Ok(cur_results) => {
                results.reserve(cur_results.len());
                for result in cur_results {
                    records.add(&result);
                    results.push(result);
                }
                // Undo all the base item quantities, as they will be completely taken over by the parsed
                // contents, which contains an arbitrary amount of items (even 0).
                records.remove(item);
            }
        }
    }

    (Settings::default(), results)
}

#[derive(Debug, thiserror::Error)]
#[error("Expected a spans integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);
