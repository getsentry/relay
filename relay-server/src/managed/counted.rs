use std::collections::BTreeMap;

use itertools::Either;
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

impl<T: Counted> Counted for Option<T> {
    fn quantities(&self) -> Quantities {
        match self {
            Some(inner) => inner.quantities(),
            None => Quantities::new(),
        }
    }
}

impl<L, R> Counted for Either<L, R>
where
    L: Counted,
    R: Counted,
{
    fn quantities(&self) -> Quantities {
        match self {
            Either::Left(value) => value.quantities(),
            Either::Right(value) => value.quantities(),
        }
    }
}

impl Counted for (DataCategory, usize) {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![*self]
    }
}

impl<const N: usize> Counted for [(DataCategory, usize); N] {
    fn quantities(&self) -> Quantities {
        smallvec::SmallVec::from_slice(self)
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
            (DataCategory::Attachment, self.attachment_quantities.bytes()),
            (
                DataCategory::AttachmentItem,
                self.attachment_quantities.count(),
            ),
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
            (DataCategory::UserReportV2, self.user_report_quantity),
            (DataCategory::TraceMetric, self.trace_metric_quantity),
            (DataCategory::LogItem, self.log_item_quantity),
            (DataCategory::LogByte, self.log_byte_quantity),
            (DataCategory::Monitor, self.monitor_quantity),
            (DataCategory::Session, self.session_quantity),
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
            buckets,
        } = metrics::extract_quantities(&self.project_metrics);

        [
            (DataCategory::Transaction, transactions),
            (DataCategory::Span, spans),
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

#[cfg(feature = "processing")]
impl Counted for sentry_protos::snuba::v1::Outcomes {
    fn quantities(&self) -> Quantities {
        self.category_count
            .iter()
            .inspect(|cc| {
                debug_assert!(DataCategory::try_from(cc.data_category).is_ok());
                debug_assert!(usize::try_from(cc.quantity).is_ok());
            })
            .filter_map(|cc| {
                Some((
                    DataCategory::try_from(cc.data_category).ok()?,
                    usize::try_from(cc.quantity).ok()?,
                ))
            })
            .collect()
    }
}

#[cfg(feature = "processing")]
impl Counted for sentry_protos::snuba::v1::TraceItem {
    fn quantities(&self) -> Quantities {
        self.outcomes.quantities()
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

impl<T: Counted> Counted for [T] {
    fn quantities(&self) -> Quantities {
        let mut quantities = BTreeMap::new();
        for element in self {
            for (category, size) in element.quantities() {
                *quantities.entry(category).or_default() += size;
            }
        }
        quantities.into_iter().collect()
    }
}

impl<T: Counted> Counted for Vec<T> {
    fn quantities(&self) -> Quantities {
        self.as_slice().quantities()
    }
}

impl<T: Counted, const N: usize> Counted for SmallVec<[T; N]> {
    fn quantities(&self) -> Quantities {
        self.as_slice().quantities()
    }
}
