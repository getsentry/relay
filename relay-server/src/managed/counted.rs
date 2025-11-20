use relay_event_schema::protocol::{
    OurLog, SessionAggregateItem, SessionAggregates, SessionUpdate, Span, SpanV2, TraceMetric,
};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;
use smallvec::SmallVec;

use crate::envelope::{Item, SourceQuantities, WithHeader};
use crate::metrics_extraction::transactions::ExtractedMetrics;
use crate::utils::EnvelopeSummary;
use crate::{Envelope, metrics, processing};

/// A list of data categories and amounts.
pub type Quantities = SmallVec<[(DataCategory, usize); 2]>;

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
            (
                DataCategory::AttachmentItem,
                summary.attachment_item_quantity,
            ),
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
            (DataCategory::TraceMetric, summary.trace_metric_quantity),
            (DataCategory::LogItem, summary.log_item_quantity),
            (DataCategory::LogByte, summary.log_byte_quantity),
            (DataCategory::Monitor, summary.monitor_quantity),
            (DataCategory::Session, summary.session_quantity),
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

impl Counted for WithHeader<TraceMetric> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::TraceMetric, 1)]
    }
}

impl Counted for WithHeader<SpanV2> {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)]
    }
}

impl Counted for SpanV2 {
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

impl Counted for SessionUpdate {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Session, 1)]
    }
}

impl Counted for SessionAggregates {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Session, self.aggregates.len())]
    }
}
impl Counted for SessionAggregateItem {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Session, 1)]
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

impl<T> Counted for Box<T>
where
    T: Counted,
{
    fn quantities(&self) -> Quantities {
        self.as_ref().quantities()
    }
}
