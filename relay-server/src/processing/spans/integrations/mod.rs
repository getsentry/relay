use relay_event_schema::protocol::SpanV2;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, WithHeader};
use crate::integrations::{Integration, SpansIntegration};
use crate::managed::RecordKeeper;

mod otel;

/// Expands a list of [`Integration`] items into `result`.
///
/// The function expects *only* span item integrations.
pub fn expand_into(
    result: &mut ContainerItems<SpanV2>,
    records: &mut RecordKeeper<'_>,
    items: Vec<Item>,
) {
    for item in items {
        let integration = match item.integration() {
            Some(Integration::Spans(integration)) => integration,
            integration => {
                records.internal_error(InvalidIntegration(integration), item);
                continue;
            }
        };

        let produce = |span: SpanV2| {
            records.modify_by(DataCategory::Span, 1);
            records.modify_by(DataCategory::SpanIndexed, 1);
            result.push(WithHeader::new(span.into()));
        };

        let payload = item.payload();

        let result = match integration {
            SpansIntegration::OtelV1 { format } => otel::expand(format, &payload, produce),
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
#[error("Expected a spans integration, got: {0:?}")]
struct InvalidIntegration(Option<Integration>);
