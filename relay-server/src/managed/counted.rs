use relay_event_schema::protocol::OurLog;
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use smallvec::SmallVec;

use crate::Envelope;
use crate::envelope::Item;
use crate::utils::EnvelopeSummary;

/// A list of data categories and amounts.
pub type Quantities = SmallVec<[(DataCategory, usize); 1]>;

/// A counted item.
///
/// An item may represent multiple categories with different counts at once.
pub trait Counted {
    /// Returns the contained item quantities.
    ///
    /// Implementation are expected to be pure.
    fn quantities(&self) -> Quantities;
}

impl Counted for () {
    fn quantities(&self) -> Quantities {
        Quantities::new()
    }
}

impl Counted for Item {
    fn quantities(&self) -> Quantities {
        self.quantities()
    }
}

impl Counted for Box<Envelope> {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();

        // This matches the implementation of `ManagedEnvelope::reject`.
        let summary = EnvelopeSummary::compute(self);
        if let Some(category) = summary.event_category {
            quantities.push((category, 1));
            if let Some(category) = category.index_category() {
                quantities.push((category, 1));
            }
        }

        let data = [
            (DataCategory::Attachment, summary.attachment_quantity),
            (DataCategory::Profile, summary.profile_quantity),
            (DataCategory::ProfileIndexed, summary.profile_quantity),
            (DataCategory::Span, summary.span_quantity),
            (DataCategory::SpanIndexed, summary.span_quantity),
            (
                DataCategory::Transaction,
                summary.secondary_transaction_quantity,
            ),
            (DataCategory::Span, summary.secondary_span_quantity),
            (DataCategory::Replay, summary.replay_quantity),
            (DataCategory::ProfileChunk, summary.profile_chunk_quantity),
            (
                DataCategory::ProfileChunkUi,
                summary.profile_chunk_ui_quantity,
            ),
            (DataCategory::LogItem, summary.log_item_quantity),
            (DataCategory::LogByte, summary.log_byte_quantity),
        ];

        for (category, quantity) in data {
            if quantity > 0 {
                quantities.push((category, quantity));
            }
        }

        quantities
    }
}

impl Counted for Annotated<OurLog> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, 1),
            (
                DataCategory::LogByte,
                crate::processing::logs::DUMMY_LOG_SIZE
            )
        ]
    }
}

impl<T> Counted for &T
where
    T: Counted,
{
    fn quantities(&self) -> Quantities {
        (*self).quantities()
    }
}
