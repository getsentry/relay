use relay_event_schema::protocol::{OurLog, Span, SpanV2};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use smallvec::SmallVec;

use crate::envelope::{Item, SourceQuantities, WithHeader};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::utils::EnvelopeSummary;
use crate::{Envelope, metrics, processing};

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
        EnvelopeSummary::compute(self).quantities()
    }
}

impl Counted for EnvelopeSummary {
    fn quantities(&self) -> Quantities {
        let mut quantities = Quantities::new();

        if let Some(category) = self.event_category {
            quantities.push((category, 1));
            if let Some(category) = category.index_category() {
                quantities.push((category, 1));
            }
        }

        let data = [
            (DataCategory::Attachment, self.attachment_quantity),
            (DataCategory::Profile, self.profile_quantity),
            (DataCategory::ProfileIndexed, self.profile_quantity),
            (DataCategory::Span, self.span_quantity),
            (DataCategory::SpanIndexed, self.span_quantity),
            (
                DataCategory::Transaction,
                self.secondary_transaction_quantity,
            ),
            (DataCategory::Span, self.secondary_span_quantity),
            (DataCategory::Replay, self.replay_quantity),
            (DataCategory::ProfileChunk, self.profile_chunk_quantity),
            (DataCategory::ProfileChunkUi, self.profile_chunk_ui_quantity),
            (DataCategory::LogItem, self.log_item_quantity),
            (DataCategory::LogByte, self.log_byte_quantity),
            (DataCategory::Monitor, self.monitor_quantity),
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

impl Counted for ExtractedMetrics {
    fn quantities(&self) -> Quantities {
        // We only consider project metrics, sampling project metrics should never carry outcomes,
        // as they would be for a *different* project.
        let SourceQuantities {
            transactions,
            spans,
            profiles,
            buckets,
        } = metrics::extract_quantities(&self.project_metrics);

        [
            (DataCategory::Transaction, transactions),
            (DataCategory::Span, spans),
            (DataCategory::Profile, profiles),
            (DataCategory::MetricBucket, buckets),
        ]
        .into_iter()
        .filter(|(_, q)| *q > 0)
        .collect()
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
