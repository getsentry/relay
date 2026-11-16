//! TransactionItem APIs and accounting.

use super::{ItemHeaderKey, TransactionItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl TransactionItem {
    /// Returns the declared number of embedded spans, or `None` for an absent or invalid header.
    pub fn span_count(&self) -> Option<u32> {
        self.data().headers.get(ItemHeaderKey::SpanCount)
    }

    /// Counts embedded spans using the header, falling back to scanning the `spans` payload array.
    /// Invalid payloads count as zero.
    pub fn count_embedded_spans(&self) -> usize {
        #[derive(serde::Deserialize)]
        struct PartialEvent {
            spans: crate::utils::SeqCount,
        }

        self.span_count()
            .map(|count| count as usize)
            .unwrap_or_else(|| {
                relay_statsd::metric!(timer(crate::statsd::RelayTimers::CheckNestedSpans), {
                    serde_json::from_slice::<PartialEvent>(&self.data().payload())
                        .map_or(0, |event| event.spans.0)
                })
            })
    }

    /// Sets the number of spans in the transaction payload.
    pub fn set_span_count(&mut self, value: Option<u32>) {
        self.data_mut()
            .headers
            .set_or_remove(ItemHeaderKey::SpanCount, value);
    }
}

impl Counted for TransactionItem {
    fn quantities(&self) -> Quantities {
        let spans = 1 + self.span_count().unwrap_or(0) as usize;
        smallvec![
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, spans),
            (DataCategory::SpanIndexed, spans),
        ]
    }
}

impl RejectionQuantities for TransactionItem {
    // Include the transaction's own span; the logical transaction is recorded separately.
    // Read embedded spans from the payload when the count header is absent or invalid.
    fn rejected_quantities(&self) -> Quantities {
        let spans = 1 + self.count_embedded_spans();
        smallvec![
            (DataCategory::Span, spans),
            (DataCategory::SpanIndexed, spans),
        ]
    }
}
