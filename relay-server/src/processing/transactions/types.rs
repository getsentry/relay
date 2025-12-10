use relay_event_schema::protocol::{Event, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{EnvelopeHeaders, Item, Items};
use crate::managed::{Counted, Quantities};
use crate::metrics_extraction::transactions::ExtractedMetrics;

/// Flags extracted from transaction item headers.
///
/// Ideally `metrics_extracted` and `spans_extracted` will not be needed in the future. Unsure
/// about `fully_normalized`.
#[derive(Debug, Default)]
pub struct Flags {
    pub metrics_extracted: bool,
    pub spans_extracted: bool,
    pub fully_normalized: bool,
}

pub struct Payload {
    pub event: Annotated<Event>,
    pub flags: Flags,
    pub attachments: Items,
    pub profile: Option<Item>,
    pub extracted_spans: Vec<Item>,
}

impl Payload {
    fn count_embedded_spans_and_self(&self) -> usize {
        1 + self
            .event
            .value()
            .and_then(|e| e.spans.value())
            .map_or(0, Vec::len)
    }
}

impl Counted for Payload {
    fn quantities(&self) -> Quantities {
        let Self {
            event,
            flags,
            attachments,
            profile,
            extracted_spans,
        } = self;
        let mut quantities = smallvec::smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
        ];

        let span_count = if !extracted_spans.is_empty() {
            extracted_spans.len()
        } else if !flags.spans_extracted {
            self.count_embedded_spans_and_self()
        } else {
            0
        };
        quantities.extend([
            (DataCategory::Span, span_count),
            (DataCategory::SpanIndexed, span_count),
        ]);

        quantities.extend(attachments.quantities());
        quantities.extend(profile.quantities());

        quantities
    }
}

pub enum SampledPayload {
    /// We still have a transaction + child items, and it counts as both indexed + total.
    Keep { payload: Payload },
    /// All we have left is a profile.
    Drop { profile: Option<Item> },
}

pub struct WithMetrics {
    pub headers: EnvelopeHeaders,
    pub payload: SampledPayload,
    pub metrics: ExtractedMetrics,
}

impl Counted for WithMetrics {
    fn quantities(&self) -> Quantities {
        let Self {
            headers: _,
            payload,
            metrics,
        } = self;
        match payload {
            SampledPayload::Keep { payload } => {
                // As long as we have a payload, the metrics do not carry any data category.
                payload.quantities()
            }
            SampledPayload::Drop { profile } => {
                // When the payload is gone, the metrics carry the data category.
                // FIXME: Verify that metrics never carry profile outcomes.
                let mut quantities = profile.quantities();
                quantities.extend(metrics.quantities());
                quantities
            }
        }
    }
}
