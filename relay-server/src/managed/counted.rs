use relay_event_schema::protocol::{OurLog, Span, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use smallvec::SmallVec;

use crate::envelope::{Item, WithHeader};
use crate::utils::EnvelopeSummary;
use crate::{Envelope, processing};

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

impl Counted for WithHeader<OurLog> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![
            (DataCategory::LogItem, 1),
            (
                DataCategory::LogByte,
                processing::logs::get_calculated_byte_size(self)
            )
        ]
    }
}

impl Counted for WithHeader<SpanV2> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)]
    }
}

impl Counted for Annotated<Span> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)]
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
